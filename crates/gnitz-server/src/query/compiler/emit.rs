//! Instruction emission: one `emit_node` arm per operator, and `build_plan` (one
//! plan, pre or post exchange).
//!
//! An arm resolves its operands, asks the operator's own constructor for its
//! artifact, and pushes an instruction into a fresh register. The artifacts —
//! output schemas, probes, packers, plans — and the guards a client-supplied
//! circuit clears to get one belong to `gnitz-zset`, beside the kernels that
//! read them.

use super::*;
use crate::query::vm::{Op, ProgramBuilder};
use gnitz_store::relation::StateLayout;
use gnitz_zset::stream::JoinPlan;

// ---------------------------------------------------------------------------
// EmitCtx — the per-plan build state every emit arm works against
// ---------------------------------------------------------------------------

/// All state of one `build_plan` invocation. Owned by `build_plan` and threaded
/// to the emit arms as one `&mut` instead of a dozen parallel parameters.
pub(super) struct EmitCtx<'a> {
    loaded: &'a LoadedCircuit,
    /// The view's routing. Where it is self-contained the `WorkerFilter` arm emits
    /// nothing: the trim would drop rows this worker legitimately owns a copy of.
    meta: &'a ViewMeta,
    /// The [`Seed::partials`] seeds, whose reader combines.
    partial_seeds: Vec<NodeId>,
    /// Answers the schemas and the slot.
    registry: &'a RelationRegistry,
    /// Every child the view's sub-plans declare, in one index space.
    layout: &'a mut StateLayout,
    prog: ProgramBuilder,
    out_reg_of: Vec<Option<DeltaReg>>,
    /// The integral of each node some join of this plan probes.
    integrals: Vec<Option<Integral>>,
    source_reg_map: FxHashMap<u64, DeltaReg>,
    /// The plan's [`PlanOut::Store`] node.
    store_out: Option<NodeId>,
}

impl<'a> EmitCtx<'a> {
    /// Declare one child store of the view's operator state, named after `kind`
    /// and the node. The returned [`StateIdx`] is the only way to reach it.
    fn declare_child(&mut self, kind: &str, nid: NodeId, schema: SchemaDescriptor) -> StateIdx {
        self.layout.declare(format!("{kind}_{nid}"), schema)
    }

    /// The register `src` produced. The one rejection left after the load held
    /// every node to `OpNode::arity()`: a plan covers a *slice* of the circuit, so
    /// a producer outside this side has no register at all.
    fn reg_of(&self, src: NodeId) -> Result<DeltaReg, String> {
        self.out_reg_of[src].ok_or_else(|| "operand is produced outside this plan".to_string())
    }

    /// The output trace of reduce or top-N `nid`, declared under `kind` — or
    /// `None` where the node is the plan's [`PlanOut::Store`], so that the view's
    /// store holds the same rows.
    fn out_trace(&mut self, kind: &str, nid: NodeId, schema: SchemaDescriptor) -> Option<StateIdx> {
        (self.store_out != Some(nid)).then(|| self.declare_child(kind, nid, schema))
    }

    fn reads_partials(&self, nid: NodeId) -> bool {
        self.partial_seeds.contains(&self.loaded.inputs(nid)[0])
    }

    /// The delta feeding a unary operator.
    fn unary_delta_in(&self, nid: NodeId) -> Result<DeltaReg, String> {
        self.reg_of(self.loaded.inputs(nid)[0])
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
            OpNode::Join { kind, .. } if self.loaded.inputs(n)[1] == of => *kind == JoinKind::Equi,
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
            let reg = ctx.prog.seed(schema);
            // The driver seeds one register per source, so a second scan would
            // silently see nothing.
            if ctx.source_reg_map.insert(*tid, reg).is_some() {
                return Err("scan-delta: a plan scans one source twice".into());
            }
            Ok(reg)
        }

        gnitz_wire::OpNode::Filter(blob) => {
            let in_reg = ctx.unary_delta_in(nid)?;
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
            let in_reg = ctx.unary_delta_in(nid)?;
            Ok(ctx.prog.push(in_reg, ctx.prog.schema_of(in_reg), Op::Negate))
        }

        gnitz_wire::OpNode::Union => {
            let inputs = ctx.loaded.inputs(nid);
            let (in_a, in_b) = (ctx.reg_of(inputs[0])?, ctx.reg_of(inputs[1])?);
            let out_schema =
                gnitz_zset::algebra::union_nullability_merge(&ctx.prog.schema_of(in_a), &ctx.prog.schema_of(in_b))?;
            Ok(ctx.prog.push(in_a, out_schema, Op::Union { in_b }))
        }

        gnitz_wire::OpNode::WeightClamp(kind) => {
            let in_reg = ctx.unary_delta_in(nid)?;
            let schema = ctx.prog.schema_of(in_reg);
            let hist = ctx.declare_child("hist", nid, schema);
            Ok(ctx.prog.push(in_reg, schema, Op::WeightClamp { hist, kind: *kind }))
        }

        gnitz_wire::OpNode::Reduce { group_cols, agg } => emit_reduce(ctx, nid, group_cols, agg),

        gnitz_wire::OpNode::TopN { group_cols, order, limit, offset } => {
            let in_reg = ctx.unary_delta_in(nid)?;
            let in_schema = ctx.prog.schema_of(in_reg);
            let plan = match ctx.reads_partials(nid) {
                true => gnitz_zset::stream::TopNPlan::combine(&in_schema, order, *limit, *offset)?,
                false => gnitz_zset::stream::TopNPlan::from_wire(&in_schema, group_cols, order, *limit, *offset)?,
            };
            Ok(push_topn(ctx, nid, FUNNEL_TOPN, in_reg, plan))
        }

        gnitz_wire::OpNode::Join { kind, delta_is_right } => {
            let &[delta, integrand] = ctx.loaded.inputs(nid) else {
                unreachable!("a join is wired on two inputs")
            };
            let delta_reg = ctx.reg_of(delta)?;
            let integrand_reg = ctx.reg_of(integrand)?;
            let delta_schema = ctx.prog.schema_of(delta_reg);
            // One constructor owns every kind's guards and its output layout.
            let (trace, plan) = match ctx.source_rekey(integrand) {
                Some((relation, rekey)) => (
                    Integral::Source(relation.id()),
                    JoinPlan::over_source(*delta_is_right, &delta_schema, &relation.schema(), &rekey)?,
                ),
                None => {
                    let trace = ctx.own_integral(integrand, integrand_reg);
                    let plan = JoinPlan::from_wire(*kind, *delta_is_right, &delta_schema, ctx.layout.schema_of(trace))?;
                    (Integral::Own(trace), plan)
                }
            };
            ctx.integrals[integrand] = Some(trace);
            Ok(ctx
                .prog
                .push(delta_reg, plan.out_schema, Op::JoinDT { trace, probe: plan.probe }))
        }

        gnitz_wire::OpNode::ExchangeShard => {
            unreachable!("carve keeps every ExchangeShard out of every plan's node list")
        }

        gnitz_wire::OpNode::WorkerFilter => {
            let in_reg = ctx.unary_delta_in(nid)?;
            if ctx.meta.self_contained {
                return Ok(in_reg);
            }
            Ok(ctx.prog.push(in_reg, ctx.prog.schema_of(in_reg), Op::WorkerFilter))
        }

        gnitz_wire::OpNode::NullExtend { type_codes, nulls_first } => {
            let in_reg = ctx.unary_delta_in(nid)?;
            let out_schema =
                gnitz_zset::algebra::null_extend_output_schema(&ctx.prog.schema_of(in_reg), type_codes, *nulls_first)?;
            let op = Op::NullExtend { nulls_first: *nulls_first };
            Ok(ctx.prog.push(in_reg, out_schema, op))
        }
    }
}

// ---------------------------------------------------------------------------
// MAP emission
// ---------------------------------------------------------------------------

/// Every `MapKind`'s output schema, program and PK source are one fact, derived
/// by [`MapPlan::from_wire`]; this arm resolves the operand, asks for the plan,
/// and either elides it or allocates a register for it.
fn emit_map(ctx: &mut EmitCtx, nid: NodeId, mk: &gnitz_wire::MapKind) -> Result<DeltaReg, String> {
    let in_reg = ctx.unary_delta_in(nid)?;
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
    let in_reg = ctx.unary_delta_in(nid)?;
    let in_schema = ctx.prog.schema_of(in_reg);
    let exchanged = matches!(
        ctx.loaded.op(ctx.loaded.inputs(nid)[0]),
        gnitz_wire::OpNode::ExchangeShard
    );
    let slot = ctx.registry.slot();
    // Each worker that holds the whole input seeds the row, else the one worker
    // V₀'s empty-keyed shard routes to.
    let seeds_ground = group_cols.is_empty()
        && exchanged
        && (ctx.meta.self_contained || slot.rank as usize == gnitz_zset::schema::ground_owner(slot.of as usize));
    let plan = match ctx.reads_partials(nid) {
        true => gnitz_zset::stream::ReducePlan::combine(&in_schema, agg, seeds_ground)?,
        false => gnitz_zset::stream::ReducePlan::from_wire(&in_schema, group_cols, agg, seeds_ground)?,
    };
    Ok(push_reduce(ctx, nid, FUNNEL_REDUCE, in_reg, plan))
}

/// The child-store names of a reduce or top-N: its output trace, then its index.
type StateKinds = [&'static str; 2];
const FUNNEL_REDUCE: StateKinds = ["reduce", "avidx"];
const FUNNEL_TOPN: StateKinds = ["topn", "topnidx"];
/// A split side's per-worker partial of either.
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

/// Declare `plan`'s children under `kinds`, and push the top-N over `in_reg`.
fn push_topn(
    ctx: &mut EmitCtx,
    nid: NodeId,
    [trace_kind, index_kind]: StateKinds,
    in_reg: DeltaReg,
    plan: gnitz_zset::stream::TopNPlan,
) -> DeltaReg {
    let out_schema = plan.output_schema;
    let out_trace = ctx.out_trace(trace_kind, nid, out_schema);
    // The ordered index of every input row — the operator's whole history.
    let index = ctx.declare_child(index_kind, nid, plan.index.schema);
    let plan = Box::new(plan);
    ctx.prog.push(in_reg, out_schema, Op::TopN { out_trace, index, plan })
}

/// `consumer`'s per-worker partial over its shard's input, or `None` for a reduce
/// whose partials do not combine. Its children are the shard's.
fn emit_partial(ctx: &mut EmitCtx, consumer: NodeId) -> Result<Option<DeltaReg>, String> {
    let shard = ctx.loaded.inputs(consumer)[0];
    let in_reg = ctx.unary_delta_in(shard)?;
    let in_schema = ctx.prog.schema_of(in_reg);
    Ok(match ctx.loaded.op(consumer) {
        gnitz_wire::OpNode::Reduce { agg, .. } => gnitz_zset::stream::ReducePlan::partial(&in_schema, agg)?
            .map(|plan| push_reduce(ctx, shard, PARTIAL, in_reg, plan)),
        gnitz_wire::OpNode::TopN { order, limit, offset, .. } => {
            let plan = gnitz_zset::stream::TopNPlan::partial(&in_schema, order, *limit, *offset)?;
            Some(push_topn(ctx, shard, PARTIAL, in_reg, plan))
        }
        _ => unreachable!("`keyed_reader` names only a Reduce or a TopN"),
    })
}

// ---------------------------------------------------------------------------
// build_plan — one plan, pre or post exchange
// ---------------------------------------------------------------------------

/// What a sub-plan outputs.
#[derive(Clone, Copy)]
pub(super) enum PlanOut {
    Node(NodeId),
    /// The output node of a view whose store holds every row it is fed — a bounded
    /// one may hold a skeleton row in their place — and is so the integral of that
    /// node's register.
    Store(NodeId),
    /// The per-worker partial of `consumer`, a global reduce or top-N behind an
    /// exchange — or the exchange's input, when it has none.
    Split {
        consumer: NodeId,
    },
}

/// An exchange node a plan reads as a relayed batch.
pub(super) struct Seed {
    pub(super) shard: NodeId,
    pub(super) schema: SchemaDescriptor,
    /// The batch is its reader's per-worker partials.
    pub(super) partials: bool,
}

/// One `build_plan`'s result.
pub(super) struct Built {
    pub(super) plan: SubPlan,
    /// Each node's register in this plan.
    pub(super) regs: Vec<Option<DeltaReg>>,
    /// The integral of each node a join of this plan probes.
    pub(super) integrals: Vec<Option<Integral>>,
    /// The register each seed's relayed batch lands in, in seed order.
    pub(super) seed_regs: Vec<DeltaReg>,
    /// The plan outputs a [`PlanOut::Split`]'s partial.
    pub(super) partial: bool,
}

/// Emit `ordered` into one sub-plan that outputs `out`.
pub(super) fn build_plan(
    loaded: &LoadedCircuit,
    ordered: &[NodeId],
    registry: &RelationRegistry,
    layout: &mut StateLayout,
    meta: &ViewMeta,
    seeds: &[Seed],
    out: PlanOut,
) -> Result<Built, String> {
    let mut ctx = EmitCtx {
        loaded,
        meta,
        partial_seeds: seeds.iter().filter(|s| s.partials).map(|s| s.shard).collect(),
        registry,
        layout,
        prog: ProgramBuilder::default(),
        out_reg_of: vec![None; loaded.len()],
        integrals: vec![None; loaded.len()],
        source_reg_map: FxHashMap::default(),
        store_out: match out {
            PlanOut::Store(nid) => Some(nid),
            PlanOut::Node(_) | PlanOut::Split { .. } => None,
        },
    };
    let seed_regs = seeds
        .iter()
        .map(|seed| {
            let reg = ctx.prog.seed(seed.schema);
            ctx.out_reg_of[seed.shard] = Some(reg);
            reg
        })
        .collect();
    for &nid in ordered {
        let reg = emit_node(&mut ctx, nid, loaded.op(nid))?;
        ctx.out_reg_of[nid] = Some(reg);
    }
    let (out_reg, partial) = match out {
        PlanOut::Node(nid) | PlanOut::Store(nid) => (ctx.reg_of(nid)?, false),
        PlanOut::Split { consumer } => match emit_partial(&mut ctx, consumer)? {
            Some(reg) => (reg, true),
            None => (ctx.unary_delta_in(loaded.inputs(consumer)[0])?, false),
        },
    };
    let EmitCtx {
        prog,
        source_reg_map,
        out_reg_of,
        integrals,
        ..
    } = ctx;
    let plan = SubPlan { vm: prog.finish(out_reg), source_reg_map };
    Ok(Built {
        plan,
        regs: out_reg_of,
        integrals,
        seed_regs,
        partial,
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "tests/emit.rs"]
mod tests;
