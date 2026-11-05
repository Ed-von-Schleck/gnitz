//! Instruction emission: one `emit_node` arm per operator, into the view's one
//! program.
//!
//! An arm resolves its operands, asks the operator's own constructor for its
//! artifact, and pushes an instruction into a fresh register. The artifacts —
//! output schemas, packers, plans — and the guards a client-supplied
//! circuit clears to get one belong to `gnitz-zset`, beside the kernels that
//! read them.

use super::*;
use crate::query::vm::{ClampHistory, HistoryTerm, Op, ProgramBuilder};
use gnitz_zset::algebra::null_extend_output_schema;
use gnitz_zset::stream::{JoinPlan, OrderedPlan, ReducePlan};

// ---------------------------------------------------------------------------
// EmitCtx — the per-plan build state every emit arm works against
// ---------------------------------------------------------------------------

/// All state of one view's emission, threaded to the emit arms as one `&mut`.
pub(super) struct EmitCtx<'a> {
    circuit: &'a Circuit,
    /// The view's routing.
    meta: &'a ViewMeta,
    /// Answers each source's schema and placement.
    registry: &'a RelationRegistry,
    pub(super) prog: ProgramBuilder,
    /// The register of every node emitted so far, by node id.
    pub(super) regs: Vec<DeltaReg>,
    /// Whether each `ExchangeShard` emitted so far hands on per-worker partials,
    /// which its reader combines.
    partials: Vec<bool>,
    /// The integral of each node some join probes, and the register its rows
    /// enter at: the node's own, or, for a base table's store, the scan's its
    /// re-key reads.
    pub(super) integrals: Vec<Option<(Integral, DeltaReg)>>,
    /// Each scanned source's scan, once per source, in first-scan order.
    pub(super) sources: Vec<(u64, Scan)>,
    /// The view, whose store is the integral of its output node's register.
    view: u64,
}

/// One scanned source: the register its delta seeds, the register its scans read — the
/// seed's, or the share the source's route puts behind it — and its backfill bound.
pub(in crate::query) struct Scan {
    pub(in crate::query) seed: DeltaReg,
    reg: DeltaReg,
    pub(in crate::query) bound: ReadBound,
    /// The exchange round the driver relays the source's delta by before it
    /// seeds `seed`, where the route is one.
    pub(in crate::query) relay: Option<Relay>,
}

/// A source's relay round: its plan, and the scatter key the plan is built from
/// over the source's schema, empty for a broadcast. The round reads the
/// source's delta alone, so equal keys over one source gather equal rows,
/// whichever view relays them.
pub(in crate::query) struct Relay {
    pub(in crate::query) plan: Rc<ScatterPlan>,
    pub(in crate::query) key: Box<[gnitz_wire::ReindexSlot]>,
}

impl<'a> EmitCtx<'a> {
    /// Declare one child store of the view's operator state, named after `kind`
    /// and the node. The returned [`StateIdx`] is the only way to reach it.
    fn declare_child(&mut self, kind: &str, nid: NodeId, schema: SchemaDescriptor) -> StateIdx {
        self.prog.declare(format!("{kind}_{nid}"), schema)
    }

    /// `circuit` emitted whole under the routing `meta` derived from it, as the
    /// plan of view `view`.
    pub(super) fn emit(
        circuit: &'a Circuit,
        registry: &'a RelationRegistry,
        meta: &'a ViewMeta,
        view: u64,
    ) -> Result<Self, String> {
        let nodes = circuit.ops().len();
        let mut ctx = EmitCtx {
            circuit,
            meta,
            registry,
            prog: ProgramBuilder::default(),
            regs: Vec::with_capacity(nodes),
            partials: vec![false; nodes],
            integrals: vec![None; nodes],
            sources: Vec::new(),
            view,
        };
        for (nid, op) in circuit.ops() {
            let reg = emit_node(&mut ctx, nid, op)?;
            ctx.regs.push(reg);
        }
        Ok(ctx)
    }

    fn reads_partials(&self, nid: NodeId) -> bool {
        self.partials[self.circuit.inputs(nid)[0]]
    }

    /// The delta feeding a unary operator.
    fn unary_delta_in(&self, nid: NodeId) -> DeltaReg {
        self.regs[self.circuit.inputs(nid)[0]]
    }

    /// This worker's share of `reg`'s rows under `plan`.
    fn share(&mut self, reg: DeltaReg, plan: Rc<ScatterPlan>) -> DeltaReg {
        self.prog.push(reg, self.prog.schema_of(reg), Op::Share(plan))
    }

    /// `reg`'s rows placed by `plan`: this worker's share where every worker
    /// holds them `whole`, an exchange round otherwise.
    fn relay(&mut self, reg: DeltaReg, plan: Rc<ScatterPlan>, whole: bool) -> DeltaReg {
        match whole {
            true => self.share(reg, plan),
            false => self.prog.round(reg, plan),
        }
    }

    /// The source whose scans read `reg`.
    fn scanned_at(&self, reg: DeltaReg) -> Option<u64> {
        self.sources
            .iter()
            .find(|(_, scan)| scan.reg == reg)
            .map(|&(source, _)| source)
    }

    /// `source`'s store where it holds, on this worker, the integral of the delta
    /// its scan reads: a base table's, with no round moving that delta. A view's
    /// store has absorbed, within one drive, a delta its reader is yet to be fed.
    fn integral_store(&self, source: u64) -> Option<&'a Relation> {
        let relation = self.registry.relation(source).filter(|r| r.kind().is_base_table())?;
        // A replicated delta is shared, never rounded.
        let relayed = self.meta.source_route(source).is_some() && !relation.placement().is_replicated();
        (!relayed).then_some(relation)
    }

    /// True when the `ExchangeShard` at `enid` moves nothing: `scatter`, the plan
    /// over the shard's input, hashes the bytes the scan behind it placed its rows by.
    fn exchange_moves_nothing(&self, enid: NodeId, scatter: &ScatterPlan) -> bool {
        scanned_row_locally(self.circuit, self.circuit.inputs(enid)[0])
            // A relayed delta no longer sits where its source's store places it.
            .filter(|&tid| self.meta.source_route(tid).is_none())
            .and_then(|tid| self.registry.relation(tid))
            .is_some_and(|source| scatter.routes_to_native_owner(source.placement()))
    }

    /// The base table whose store is already the integral of `of`, the register
    /// its scan reads, and `of`'s re-key of that register.
    fn source_rekey(&self, of: NodeId) -> Option<(&'a Relation, DeltaReg, &MapPlan)> {
        let (_, Op::Map(rekey), entry) = self.prog.writer(self.regs[of])? else {
            return None;
        };
        let relation = self.integral_store(self.scanned_at(entry)?)?;
        // The table's rows then sort by the integral's key first.
        rekey.rekeys_onto_pk_prefix()?;
        // An equal-key probe alone walks rows keyed wider than its delta.
        let probes_are_equi = self.circuit.readers(of).all(|n| match self.circuit.op(n) {
            OpNode::Join(kind) if self.circuit.inputs(n)[2..].contains(&of) => *kind == JoinKind::Equi,
            _ => true,
        });
        probes_are_equi.then_some((relation, entry, &**rekey))
    }

    /// The terms of a clamp's input `reg` where it is a signed sum of base tables'
    /// rows whose stores hold its history.
    fn source_history(&self, reg: DeltaReg) -> Option<Box<[HistoryTerm]>> {
        let mut terms = Vec::new();
        let mut operands = vec![(reg, false)];
        while let Some((reg, negated)) = operands.pop() {
            match self.prog.writer(reg) {
                Some((_, Op::Union { in_b }, in_a)) => operands.extend([(in_a, negated), (*in_b, negated)]),
                Some((_, Op::Negate, input)) => operands.push((input, !negated)),
                _ => terms.push(self.history_term(reg, negated)?),
            }
        }
        Some(terms.into())
    }

    /// The scan whose rows reach `reg` under their own keys, and the instructions
    /// on the way, where the scanned table's store holds their history.
    fn history_term(&self, mut reg: DeltaReg, negated: bool) -> Option<HistoryTerm> {
        let mut steps = Vec::new();
        let relation = loop {
            if let Some(source) = self.scanned_at(reg) {
                break source;
            }
            let (pc, op, input) = self.prog.writer(reg)?;
            match op {
                Op::Filter(_) => steps.push(pc),
                Op::Map(plan) if plan.inherits_pk() => steps.push(pc),
                // A share keeps or drops whole elements.
                Op::Share(_) => {}
                _ => return None,
            }
            reg = input;
        };
        self.integral_store(relation)?;
        steps.reverse();
        Some(HistoryTerm { relation, steps: steps.into(), negated })
    }

    /// The child integrating `of`'s register, declared and scheduled the first time
    /// a join asks.
    fn own_integral(&mut self, of: NodeId) -> StateIdx {
        if let Some((Integral::Own(trace), _)) = self.integrals[of] {
            return trace;
        }
        let reg = self.regs[of];
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
fn emit_node(ctx: &mut EmitCtx, nid: NodeId, op: &OpNode) -> Result<DeltaReg, String> {
    match op {
        // Every worker computes the view's whole result: nothing moves, and a trim
        // would drop rows this worker legitimately owns a copy of.
        OpNode::ExchangeShard | OpNode::WorkerFilter if ctx.meta.self_contained => Ok(ctx.unary_delta_in(nid)),

        // `bound` is a backfill-scan hint consumed by the source drive, not by the
        // VM: emission is identical bounded or not.
        OpNode::ScanDelta { source: tid, bound } => {
            // A circuit scanning an unknown table is corrupt: the planner
            // registers every source before shipping the circuit.
            let relation = ctx.registry.relation_or_err(*tid)?;
            // One epoch seeds one delta, and one backfill cursor feeds every scan of it.
            if let Some((_, scan)) = ctx.sources.iter_mut().find(|(s, _)| s == tid) {
                scan.bound = ReadBound::None;
                return Ok(scan.reg);
            }
            let seed = ctx.prog.seed(relation.schema());
            let route = ctx
                .meta
                .source_route(*tid)
                .map(|(plan, key)| (Rc::clone(plan), key.into()));
            let (reg, relay) = match route {
                // Every worker already holds a replicated delta whole, and keeps its share.
                Some((plan, _)) if relation.placement().is_replicated() => (ctx.share(seed, plan), None),
                Some((plan, key)) => (seed, Some(Relay { plan, key })),
                None => (seed, None),
            };
            let bound = bound.clone();
            ctx.sources.push((*tid, Scan { seed, reg, bound, relay }));
            Ok(reg)
        }

        OpNode::Filter(blob) => {
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

        OpNode::Map(mk) => emit_map(ctx, nid, mk),

        OpNode::Negate => {
            let in_reg = ctx.unary_delta_in(nid);
            Ok(ctx.prog.push(in_reg, ctx.prog.schema_of(in_reg), Op::Negate))
        }

        OpNode::Union => {
            let inputs = ctx.circuit.inputs(nid);
            ctx.prog.union(ctx.regs[inputs[0]], ctx.regs[inputs[1]])
        }

        OpNode::WeightClamp(kind) => {
            let in_reg = ctx.unary_delta_in(nid);
            let schema = ctx.prog.schema_of(in_reg);
            let hist = match ctx.source_history(in_reg) {
                Some(terms) => ClampHistory::Sources(terms),
                None => ClampHistory::Own(ctx.declare_child("hist", nid, schema)),
            };
            Ok(ctx.prog.push(in_reg, schema, Op::WeightClamp { hist, kind: *kind }))
        }

        OpNode::Reduce(g) => emit_reduce(ctx, nid, &g.group_cols, &g.aggs),

        OpNode::TopN { group_cols, order, limit, offset } => {
            let in_reg = ctx.unary_delta_in(nid);
            let in_schema = ctx.prog.schema_of(in_reg);
            let plan = match ctx.reads_partials(nid) {
                true => OrderedPlan::top_n_combine(&in_schema, order, *limit, *offset)?,
                false => OrderedPlan::top_n(&in_schema, group_cols, order, *limit, *offset)?,
            };
            Ok(push_ordered(ctx, nid, in_reg, "topnidx", plan))
        }

        OpNode::Window { group, order, rank, dense_rank } => {
            let in_reg = ctx.unary_delta_in(nid);
            let in_schema = ctx.prog.schema_of(in_reg);
            let plan = OrderedPlan::window(&in_schema, &group.group_cols, order, &group.aggs, *rank, *dense_rank)?;
            Ok(push_ordered(ctx, nid, in_reg, "windowidx", plan))
        }

        OpNode::Join(kind) => {
            let &[da, db, ia, ib] = ctx.circuit.inputs(nid) else {
                unreachable!("a join is wired on four inputs")
            };
            // Each term joins a delta to the other side as it stood before the
            // epoch, so an epoch seeding both deltas would miss their product.
            if ctx.prog.share_a_seed(ctx.regs[da], ctx.regs[db]) {
                return Err("join: one relation feeds both deltas".into());
            }
            let ab = emit_join_term(ctx, da, ib, *kind, false)?;
            let ba = emit_join_term(ctx, db, ia, *kind, true)?;
            ctx.prog.union(ab, ba)
        }

        OpNode::ExchangeShard => {
            let whole = ctx.meta.held_whole(ctx.circuit.inputs(nid)[0]);
            let reader = keyed_reader(ctx.circuit, nid);
            // A worker holding the whole input has nothing to pre-aggregate.
            let partial = match reader.filter(|(_, group)| group.is_empty() && !whole) {
                Some((consumer, _)) => emit_partial(ctx, nid, consumer)?,
                None => None,
            };
            ctx.partials[nid] = partial.is_some();
            let reg = partial.unwrap_or_else(|| ctx.unary_delta_in(nid));
            let schema = ctx.prog.schema_of(reg);
            let scatter = Rc::new(match reader {
                Some((_, group)) => ScatterPlan::group(&schema, group)?,
                None => ScatterPlan::native(Placement::full_pk(&schema)),
            });
            if ctx.exchange_moves_nothing(nid, &scatter) {
                return Ok(reg);
            }
            Ok(ctx.relay(reg, scatter, whole))
        }

        OpNode::WorkerFilter => {
            let in_reg = ctx.unary_delta_in(nid);
            let own = ScatterPlan::native(Placement::full_pk(&ctx.prog.schema_of(in_reg)));
            Ok(ctx.share(in_reg, Rc::new(own)))
        }

        OpNode::NullExtend { type_codes, nulls_first } => {
            let in_reg = ctx.unary_delta_in(nid);
            let out_schema = null_extend_output_schema(&ctx.prog.schema_of(in_reg), type_codes, *nulls_first)?;
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
    kind: JoinKind,
    delta_is_right: bool,
) -> Result<DeltaReg, String> {
    let delta_reg = ctx.regs[delta];
    let delta_schema = ctx.prog.schema_of(delta_reg);
    // One constructor owns every kind's guards and its output layout.
    let ((trace, entry), plan) = match ctx.source_rekey(integrand) {
        Some((relation, entry, rekey)) => (
            (Integral::Relation(relation.id()), entry),
            JoinPlan::over_source(delta_is_right, &delta_schema, &relation.schema(), rekey)?,
        ),
        None => {
            let reg = ctx.regs[integrand];
            let plan = JoinPlan::from_wire(kind, delta_is_right, &delta_schema, &ctx.prog.schema_of(reg))?;
            ((Integral::Own(ctx.own_integral(integrand)), reg), plan)
        }
    };
    ctx.integrals[integrand] = Some((trace, entry));
    let out_schema = *plan.output_schema();
    let op = Op::JoinDT { trace, plan: Box::new(plan) };
    Ok(ctx.prog.push(delta_reg, out_schema, op))
}

// ---------------------------------------------------------------------------
// MAP emission
// ---------------------------------------------------------------------------

/// Every `MapKind`'s output schema, program and PK source are one fact, derived
/// by [`MapPlan::from_wire`]; this arm resolves the operand, asks for the plan,
/// and either elides it or allocates a register for it.
fn emit_map(ctx: &mut EmitCtx, nid: NodeId, mk: &MapKind) -> Result<DeltaReg, String> {
    let in_reg = ctx.unary_delta_in(nid);
    let plan = MapPlan::from_wire(&ctx.prog.schema_of(in_reg), mk)?;
    // A MAP that reproduces its input row verbatim emits nothing; the node's
    // consumers read the input register instead.
    if plan.is_identity() {
        return Ok(in_reg);
    }
    Ok(ctx.prog.push(in_reg, *plan.output_schema(), Op::Map(Box::new(plan))))
}

// ---------------------------------------------------------------------------
// REDUCE emission
// ---------------------------------------------------------------------------

fn emit_reduce(ctx: &mut EmitCtx, nid: NodeId, group_cols: &[u32], agg: &[AggDescriptor]) -> Result<DeltaReg, String> {
    let in_reg = ctx.unary_delta_in(nid);
    let in_schema = ctx.prog.schema_of(in_reg);
    // Minted by the view's backfill, in an epoch of its own.
    let seeds_ground = ctx.circuit.owes_ground_row(nid);
    let plan = match ctx.reads_partials(nid) {
        true => ReducePlan::combine(&in_schema, agg, seeds_ground)?,
        false => ReducePlan::from_wire(&in_schema, group_cols, agg, seeds_ground)?,
    };
    Ok(push_reduce(ctx, nid, in_reg, plan))
}

/// Declare `plan`'s children under `nid`, and push the reduce over `in_reg`.
fn push_reduce(ctx: &mut EmitCtx, nid: NodeId, in_reg: DeltaReg, plan: ReducePlan) -> DeltaReg {
    let out_schema = *plan.output_schema();
    // The view's store where the node is the output.
    let out_trace = match ctx.circuit.out() == nid {
        true => Integral::Relation(ctx.view),
        false => Integral::Own(ctx.declare_child("reduce", nid, out_schema)),
    };
    // One index table serves every MIN/MAX of the reduce.
    let index = plan
        .index_schema()
        .map(|schema| ctx.declare_child("avidx", nid, *schema));
    let plan = Box::new(plan);
    ctx.prog.push(in_reg, out_schema, Op::Reduce { out_trace, index, plan })
}

/// Declare `plan`'s index — every input row in order, the operator's whole
/// history — as a `kind` child, and push the operator over `in_reg`.
fn push_ordered(ctx: &mut EmitCtx, nid: NodeId, in_reg: DeltaReg, kind: &'static str, plan: OrderedPlan) -> DeltaReg {
    let out_schema = *plan.output_schema();
    let index = ctx.declare_child(kind, nid, *plan.index_schema());
    let plan = Box::new(plan);
    ctx.prog.push(in_reg, out_schema, Op::Ordered { index, plan })
}

/// `consumer`'s per-worker partial over `shard`'s input, or `None` for a reduce
/// whose partials do not combine and for any other reader (a window's every row
/// reads the rows before it): the exchange then carries the raw rows. Its
/// children are the shard's.
fn emit_partial(ctx: &mut EmitCtx, shard: NodeId, consumer: NodeId) -> Result<Option<DeltaReg>, String> {
    let in_reg = ctx.unary_delta_in(shard);
    let in_schema = ctx.prog.schema_of(in_reg);
    Ok(match ctx.circuit.op(consumer) {
        OpNode::Reduce(g) => {
            ReducePlan::partial(&in_schema, &g.aggs)?.map(|plan| push_reduce(ctx, shard, in_reg, plan))
        }
        OpNode::TopN { order, limit, offset, .. } => {
            let plan = OrderedPlan::top_n_partial(&in_schema, order, *limit, *offset)?;
            Some(push_ordered(ctx, shard, in_reg, "topnidx", plan))
        }
        _ => None,
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "tests/emit.rs"]
mod tests;
