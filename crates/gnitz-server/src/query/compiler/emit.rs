//! Instruction emission: one `emit_node` arm per operator, into the view's one
//! program.
//!
//! An arm resolves its operands, asks the operator's own constructor for its
//! artifact, and pushes an instruction into a fresh register. The artifacts —
//! output schemas, packers, plans — and the guards a client-supplied
//! circuit clears to get one belong to `gnitz-zset`, beside the kernels that
//! read them.

use super::*;
use crate::query::vm::{Op, ProgramBuilder};
use gnitz_store::relation::Cut;
use gnitz_store::relation::StateLayout;
use gnitz_zset::stream::JoinPlan;

// ---------------------------------------------------------------------------
// EmitCtx — the per-plan build state every emit arm works against
// ---------------------------------------------------------------------------

/// All state of one view's emission, threaded to the emit arms as one `&mut`.
pub(super) struct EmitCtx<'a> {
    loaded: &'a LoadedCircuit,
    /// The view's routing. Where it is self-contained the `WorkerFilter` arm emits
    /// nothing: the trim would drop rows this worker legitimately owns a copy of.
    meta: &'a ViewMeta,
    /// Answers the schemas and the slot.
    registry: &'a RelationRegistry,
    /// Every child the view's operators declare, in one index space.
    pub(super) layout: StateLayout,
    pub(super) prog: ProgramBuilder,
    /// The register of every node emitted so far, by node id.
    pub(super) regs: Vec<DeltaReg>,
    /// Whether each `ExchangeShard` emitted so far hands on per-worker partials,
    /// which its reader combines.
    partials: Vec<bool>,
    /// The integral of each node some join probes.
    pub(super) integrals: Vec<Option<Integral>>,
    /// source table id → the register its delta seeds, and the register its
    /// scans read: the seed's, or the relay's the source's route puts behind it.
    pub(super) sources: FxHashMap<u64, (DeltaReg, DeltaReg)>,
    /// The view, where its store is the integral of its output node's register:
    /// a bounded view's may hold a skeleton row in a row's place.
    store_out: Option<u64>,
}

impl<'a> EmitCtx<'a> {
    /// Declare one child store of the view's operator state, named after `kind`
    /// and the node. The returned [`StateIdx`] is the only way to reach it.
    fn declare_child(&mut self, kind: &str, nid: NodeId, schema: SchemaDescriptor) -> StateIdx {
        self.layout.declare(format!("{kind}_{nid}"), schema)
    }

    pub(super) fn new(
        loaded: &'a LoadedCircuit,
        registry: &'a RelationRegistry,
        meta: &'a ViewMeta,
        store_out: Option<u64>,
    ) -> Self {
        EmitCtx {
            loaded,
            meta,
            registry,
            layout: StateLayout::default(),
            prog: ProgramBuilder::default(),
            regs: Vec::with_capacity(loaded.len()),
            partials: vec![false; loaded.len()],
            integrals: vec![None; loaded.len()],
            sources: FxHashMap::default(),
            store_out,
        }
    }

    /// The output trace of reduce `nid`: the view's store where that
    /// holds the node's rows, else a child declared under `kind`.
    fn out_trace(&mut self, kind: &str, nid: NodeId, schema: SchemaDescriptor) -> Integral {
        match self.store_out {
            Some(view) if self.loaded.out() == nid => Integral::Relation(view, Cut::Now),
            _ => Integral::Own(self.declare_child(kind, nid, schema)),
        }
    }

    fn reads_partials(&self, nid: NodeId) -> bool {
        self.partials[self.loaded.inputs(nid)[0]]
    }

    /// The delta feeding a unary operator.
    fn unary_delta_in(&self, nid: NodeId) -> DeltaReg {
        self.regs[self.loaded.inputs(nid)[0]]
    }

    /// This worker's share of `reg`'s rows under `plan`.
    fn share(&mut self, reg: DeltaReg, plan: Rc<ScatterPlan>) -> DeltaReg {
        self.prog.push(reg, self.prog.schema_of(reg), Op::Share(plan))
    }

    /// Every worker computes the same rows into `shard`: each relation behind it
    /// is replicated and reaches it whole, and nothing on the way trims or moves
    /// its rows.
    fn feeds_replica(&self, shard: NodeId) -> bool {
        use gnitz_wire::OpNode;
        let behind = self.loaded.ancestors_inclusive(self.loaded.inputs(shard)[0]);
        self.loaded.ops().filter(|(n, _)| behind[*n]).all(|(_, op)| match op {
            OpNode::ScanDelta { source, .. } => {
                let replicated = self
                    .registry
                    .relation(*source)
                    .is_some_and(|r| r.placement().is_replicated());
                replicated && self.meta.source_route(*source).is_none()
            }
            OpNode::WorkerFilter | OpNode::ExchangeShard => false,
            _ => true,
        })
    }

    /// The base table whose store is already the integral of `of`, and `of`'s re-key
    /// over that table's rows.
    fn source_rekey(&self, of: NodeId) -> Option<(&'a Relation, MapPlan)> {
        use gnitz_wire::{JoinKind, MapKind, OpNode};
        let OpNode::Map(mk @ MapKind::Reindex { .. }) = self.loaded.op(of) else {
            return None;
        };
        let OpNode::ScanDelta { source, .. } = self.loaded.op(self.loaded.inputs(of)[0]) else {
            return None;
        };
        let registry: &'a RelationRegistry = self.registry;
        let relation = registry.relation(*source)?;
        // A view's store has absorbed, within one drive, a delta its reader is yet to be fed.
        if !relation.kind().is_base_table() {
            return None;
        }
        let rekey = MapPlan::from_wire(&relation.schema(), mk).ok()?;
        // The table's rows then sort by the integral's key first.
        rekey.rekeys_onto_pk_prefix()?;
        // A trace holds the delta where it lands, which is where the table holds it
        // only if no relay moves it.
        let unmoved = relation.placement().is_replicated() || self.meta.source_route(*source).is_none();
        // An equal-key probe alone walks rows keyed wider than its delta.
        let probes_are_equi = self.loaded.readers(of).all(|n| match self.loaded.op(n) {
            OpNode::Join { kind } if self.loaded.inputs(n)[2..].contains(&of) => *kind == JoinKind::Equi,
            _ => true,
        });
        (unmoved && probes_are_equi).then_some((relation, rekey))
    }

    /// The child integrating `of`'s register, declared and scheduled the first time
    /// a join asks.
    fn own_integral(&mut self, of: NodeId, reg: DeltaReg) -> StateIdx {
        if let Some(Integral::Own(trace)) = self.integrals[of] {
            return trace;
        }
        let trace = self.declare_child("int", of, self.prog.schema_of(reg));
        self.prog.integrate(reg, trace);
        trace
    }
}

// ---------------------------------------------------------------------------
// Instruction emission — per-node handler
// ---------------------------------------------------------------------------

/// Emit `nid`'s instructions and return the register its output lands in: its
/// own, or an input's when the node emits nothing.
pub(super) fn emit_node(ctx: &mut EmitCtx, nid: NodeId, op: &gnitz_wire::OpNode) -> Result<DeltaReg, String> {
    match op {
        // `bound` is a backfill-scan hint consumed by the source drive, not by the
        // VM: emission is identical bounded or not.
        gnitz_wire::OpNode::ScanDelta { source: tid, .. } => {
            // A circuit scanning an unknown table is corrupt: the planner
            // registers every source before shipping the circuit.
            let schema = ctx.registry.relation_or_err(*tid)?.schema();
            // One epoch seeds one delta, which every scan of the source reads.
            if let Some(&(_, reg)) = ctx.sources.get(tid) {
                return Ok(reg);
            }
            let seed = ctx.prog.seed(schema);
            let reg = match ctx.meta.source_route(*tid) {
                Some(Relay::Round(plan)) => ctx.prog.round(seed, Rc::clone(plan)),
                Some(Relay::Share(plan)) => ctx.share(seed, Rc::clone(plan)),
                None => seed,
            };
            ctx.sources.insert(*tid, (seed, reg));
            Ok(reg)
        }

        gnitz_wire::OpNode::Filter(blob) => {
            let in_reg = ctx.unary_delta_in(nid);
            let in_schema = ctx.prog.schema_of(in_reg);
            // A present-but-corrupt blob, or a rejected program, is catalog
            // corruption. Falling back to pass-all would silently turn a WHERE
            // into WHERE TRUE; fail the compile instead.
            let pred = LogicalProgram::from_blob(blob)
                .and_then(|p| p.resolve_filter(&in_schema))
                .map_err(|e| format!("filter: invalid predicate program: {e}"))?;
            Ok(ctx.prog.push(in_reg, in_schema, Op::Filter(Box::new(pred))))
        }

        gnitz_wire::OpNode::Map(mk) => emit_map(ctx, nid, mk),

        gnitz_wire::OpNode::Negate => {
            let in_reg = ctx.unary_delta_in(nid);
            Ok(ctx.prog.push(in_reg, ctx.prog.schema_of(in_reg), Op::Negate))
        }

        gnitz_wire::OpNode::Union => {
            let inputs = ctx.loaded.inputs(nid);
            let (in_a, in_b) = (ctx.regs[inputs[0]], ctx.regs[inputs[1]]);
            let out_schema =
                gnitz_zset::algebra::union_nullability_merge(&ctx.prog.schema_of(in_a), &ctx.prog.schema_of(in_b))?;
            Ok(ctx.prog.push(in_a, out_schema, Op::Union { in_b }))
        }

        gnitz_wire::OpNode::WeightClamp(kind) => {
            let in_reg = ctx.unary_delta_in(nid);
            let schema = ctx.prog.schema_of(in_reg);
            let hist = ctx.declare_child("hist", nid, schema);
            Ok(ctx.prog.push(in_reg, schema, Op::WeightClamp { hist, kind: *kind }))
        }

        gnitz_wire::OpNode::Reduce { group_cols, agg } => emit_reduce(ctx, nid, group_cols, agg),

        gnitz_wire::OpNode::TopN { group_cols, order, limit, offset } => {
            let in_reg = ctx.unary_delta_in(nid);
            let in_schema = ctx.prog.schema_of(in_reg);
            let plan = match ctx.reads_partials(nid) {
                true => gnitz_zset::stream::TopNPlan::combine(&in_schema, order, *limit, *offset)?,
                false => gnitz_zset::stream::TopNPlan::from_wire(&in_schema, group_cols, order, *limit, *offset)?,
            };
            Ok(push_topn(ctx, nid, in_reg, plan))
        }

        gnitz_wire::OpNode::Join { kind } => {
            let &[da, db, ia, ib] = ctx.loaded.inputs(nid) else {
                unreachable!("a join is wired on four inputs")
            };
            // Each term joins a delta to the other side as it stood before the
            // epoch, so an epoch seeding both deltas would miss their product.
            if ctx.prog.share_a_seed(ctx.regs[da], ctx.regs[db]) {
                return Err("join: one relation feeds both deltas".into());
            }
            let ab = emit_join_term(ctx, da, ib, *kind, false)?;
            let ba = emit_join_term(ctx, db, ia, *kind, true)?;
            let out_schema =
                gnitz_zset::algebra::union_nullability_merge(&ctx.prog.schema_of(ab), &ctx.prog.schema_of(ba))?;
            Ok(ctx.prog.push(ab, out_schema, Op::Union { in_b: ba }))
        }

        gnitz_wire::OpNode::ExchangeShard => {
            // A ground row is minted by the first epoch, whatever it seeds, and
            // a round runs only in the epochs of the sources that reach it.
            if ctx.loaded.behind_ground_reduce(nid) {
                return Err("an exchange behind a global aggregate".into());
            }
            let reader = ctx.loaded.keyed_reader(nid)?;
            // A worker holding the whole input has nothing to pre-aggregate.
            let partial = match reader.filter(|(_, group)| group.is_empty() && !ctx.meta.self_contained) {
                Some((consumer, _)) => emit_partial(ctx, consumer)?,
                None => None,
            };
            ctx.partials[nid] = partial.is_some();
            let reg = partial.unwrap_or_else(|| ctx.unary_delta_in(nid));
            let schema = ctx.prog.schema_of(reg);
            let scatter = Rc::new(match reader {
                Some((_, group)) => ScatterPlan::group(&schema, group)?,
                None => ScatterPlan::native(Placement::full_pk(&schema)),
            });
            let alone = ctx.loaded.exchange_shards().nth(1).is_none();
            let stays = ctx.meta.self_contained
                || (alone && routing::skips_output_exchange(ctx.loaded, nid, &scatter, ctx.registry));
            Ok(match () {
                _ if stays => reg,
                _ if ctx.feeds_replica(nid) => ctx.share(reg, scatter),
                _ => ctx.prog.round(reg, scatter),
            })
        }

        gnitz_wire::OpNode::WorkerFilter => {
            let in_reg = ctx.unary_delta_in(nid);
            if ctx.meta.self_contained {
                return Ok(in_reg);
            }
            let own = ScatterPlan::native(Placement::full_pk(&ctx.prog.schema_of(in_reg)));
            Ok(ctx.share(in_reg, Rc::new(own)))
        }

        gnitz_wire::OpNode::NullExtend { type_codes, nulls_first } => {
            let in_reg = ctx.unary_delta_in(nid);
            let out_schema =
                gnitz_zset::algebra::null_extend_output_schema(&ctx.prog.schema_of(in_reg), type_codes, *nulls_first)?;
            let op = Op::NullExtend { nulls_first: *nulls_first };
            Ok(ctx.prog.push(in_reg, out_schema, op))
        }
    }
}

// ---------------------------------------------------------------------------
// JOIN emission
// ---------------------------------------------------------------------------

/// One term of a join: `delta ⋈ z⁻¹I(integrand)`, the delta carrying the right
/// SQL side iff `delta_is_right`.
fn emit_join_term(
    ctx: &mut EmitCtx,
    delta: NodeId,
    integrand: NodeId,
    kind: gnitz_wire::JoinKind,
    delta_is_right: bool,
) -> Result<DeltaReg, String> {
    let (delta_reg, integrand_reg) = (ctx.regs[delta], ctx.regs[integrand]);
    let delta_schema = ctx.prog.schema_of(delta_reg);
    // One constructor owns every kind's guards and its output layout.
    let (trace, plan) = match ctx.source_rekey(integrand) {
        Some((relation, rekey)) => (
            Integral::Relation(relation.id(), Cut::Sealed),
            JoinPlan::over_source(delta_is_right, &delta_schema, &relation.schema(), &rekey)?,
        ),
        None => {
            let trace = ctx.own_integral(integrand, integrand_reg);
            let plan = JoinPlan::from_wire(kind, delta_is_right, &delta_schema, ctx.layout.schema_of(trace))?;
            (Integral::Own(trace), plan)
        }
    };
    ctx.integrals[integrand] = Some(trace);
    let out_schema = *plan.out_schema();
    let op = Op::JoinDT { trace, plan: Box::new(plan) };
    Ok(ctx.prog.push(delta_reg, out_schema, op))
}

// ---------------------------------------------------------------------------
// MAP emission
// ---------------------------------------------------------------------------

/// Every `MapKind`'s output schema, program and PK source are one fact, derived
/// by [`MapPlan::from_wire`]; this arm resolves the operand, asks for the plan,
/// and either elides it or allocates a register for it.
fn emit_map(ctx: &mut EmitCtx, nid: NodeId, mk: &gnitz_wire::MapKind) -> Result<DeltaReg, String> {
    let in_reg = ctx.unary_delta_in(nid);
    let plan = MapPlan::from_wire(&ctx.prog.schema_of(in_reg), mk)?;
    // A MAP that reproduces its input row verbatim emits nothing; the node's
    // consumers read the input register instead.
    if plan.is_identity() {
        return Ok(in_reg);
    }
    Ok(ctx.prog.push(in_reg, *plan.out_schema(), Op::Map(Box::new(plan))))
}

// ---------------------------------------------------------------------------
// REDUCE emission
// ---------------------------------------------------------------------------

fn emit_reduce(ctx: &mut EmitCtx, nid: NodeId, group_cols: &[u32], agg: &[AggDescriptor]) -> Result<DeltaReg, String> {
    let in_reg = ctx.unary_delta_in(nid);
    let in_schema = ctx.prog.schema_of(in_reg);
    let slot = ctx.registry.slot();
    // Each worker that holds the whole input seeds the row, else the one worker
    // V₀'s empty-keyed shard routes to.
    let seeds_ground = ctx.loaded.owes_ground_row(nid)
        && (ctx.meta.self_contained || slot.rank as usize == gnitz_zset::algebra::ground_owner(slot.of as usize));
    let plan = match ctx.reads_partials(nid) {
        true => gnitz_zset::stream::ReducePlan::combine(&in_schema, agg, seeds_ground)?,
        false => gnitz_zset::stream::ReducePlan::from_wire(&in_schema, group_cols, agg, seeds_ground)?,
    };
    Ok(push_reduce(ctx, nid, FUNNEL_REDUCE, in_reg, plan))
}

/// The child-store names of a reduce: its output trace, then its index.
type StateKinds = [&'static str; 2];
const FUNNEL_REDUCE: StateKinds = ["reduce", "avidx"];
/// An exchange's per-worker partial of one.
const PARTIAL: StateKinds = ["partial", "partialidx"];

/// Declare `plan`'s children under `kinds`, and push the reduce over `in_reg`.
fn push_reduce(
    ctx: &mut EmitCtx,
    nid: NodeId,
    [trace_kind, index_kind]: StateKinds,
    in_reg: DeltaReg,
    plan: gnitz_zset::stream::ReducePlan,
) -> DeltaReg {
    let out_schema = *plan.output_schema();
    let out_trace = ctx.out_trace(trace_kind, nid, out_schema);
    // One index table serves every MIN/MAX of the reduce.
    let index = plan
        .index_schema()
        .map(|schema| ctx.declare_child(index_kind, nid, *schema));
    let plan = Box::new(plan);
    ctx.prog.push(in_reg, out_schema, Op::Reduce { out_trace, index, plan })
}

/// Declare `plan`'s index, and push the top-N over `in_reg`.
fn push_topn(ctx: &mut EmitCtx, nid: NodeId, in_reg: DeltaReg, plan: gnitz_zset::stream::TopNPlan) -> DeltaReg {
    let out_schema = *plan.output_schema();
    // The ordered index of every input row — the operator's whole history.
    let index = ctx.declare_child("topnidx", nid, *plan.index_schema());
    let plan = Box::new(plan);
    ctx.prog.push(in_reg, out_schema, Op::TopN { index, plan })
}

/// `consumer`'s per-worker partial over its shard's input, or `None` for a reduce
/// whose partials do not combine. Its children are the shard's.
fn emit_partial(ctx: &mut EmitCtx, consumer: NodeId) -> Result<Option<DeltaReg>, String> {
    let shard = ctx.loaded.inputs(consumer)[0];
    let in_reg = ctx.unary_delta_in(shard);
    let in_schema = ctx.prog.schema_of(in_reg);
    Ok(match ctx.loaded.op(consumer) {
        gnitz_wire::OpNode::Reduce { agg, .. } => gnitz_zset::stream::ReducePlan::partial(&in_schema, agg)?
            .map(|plan| push_reduce(ctx, shard, PARTIAL, in_reg, plan)),
        gnitz_wire::OpNode::TopN { order, limit, offset, .. } => {
            let plan = gnitz_zset::stream::TopNPlan::partial(&in_schema, order, *limit, *offset)?;
            Some(push_topn(ctx, shard, in_reg, plan))
        }
        _ => unreachable!("`keyed_reader` names only a Reduce or a TopN"),
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "tests/emit.rs"]
mod tests;
