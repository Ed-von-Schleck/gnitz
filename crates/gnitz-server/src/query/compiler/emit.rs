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
    /// The view's routing, and its scanned sources.
    meta: &'a ViewMeta<'a>,
    pub(super) prog: ProgramBuilder,
    /// The register of every node emitted so far, by node id.
    pub(super) regs: Vec<DeltaReg>,
    /// The integral of each node some join probes, the register its rows enter
    /// at, and the clamp its stored weights are read through: the node's own,
    /// or, for a base table's store, the scan's its re-key reads. A clamp's
    /// integral is its own history, read through the clamp.
    pub(super) integrals: Vec<Option<(Integral, DeltaReg, Option<ClampKind>)>>,
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
    /// The source's payload columns, as payload bits, a reader of its rows
    /// reads: its backfill drain may write every other one as zeroes.
    pub(in crate::query) read: u64,
}

/// A source's relay round: its plan, and the scatter key the plan is built from
/// over the source's schema, empty for a broadcast. The round reads the
/// source's delta alone, so equal keys over one source gather equal rows,
/// whichever view relays them.
#[derive(Clone)]
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
    pub(super) fn emit(circuit: &'a Circuit, meta: &'a ViewMeta<'a>, view: u64) -> Result<Self, String> {
        let nodes = circuit.ops().len();
        let mut ctx = EmitCtx {
            circuit,
            meta,
            prog: ProgramBuilder::default(),
            regs: Vec::with_capacity(nodes),
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

    /// The delta feeding a unary operator.
    fn unary_delta_in(&self, nid: NodeId) -> DeltaReg {
        self.regs[self.circuit.inputs(nid)[0]]
    }

    /// This worker's share of `reg`'s rows under `plan`.
    fn share(&mut self, reg: DeltaReg, plan: Rc<ScatterPlan>) -> DeltaReg {
        self.prog.push(reg, self.prog.schema_of(reg), Op::Share(plan))
    }

    /// `nid`'s input, in `reg`, placed by `scatter`: left where it stands when
    /// that is where the plan puts it, this worker's share where every worker
    /// holds it whole, an exchange round otherwise.
    fn exchange(&mut self, nid: NodeId, reg: DeltaReg, scatter: ScatterPlan) -> DeltaReg {
        let input = self.circuit.inputs(nid)[0];
        // Every worker computes the view's whole result: nothing moves.
        if self.meta.self_contained || self.moves_nothing(input, &scatter) {
            return reg;
        }
        match self.meta.held_whole(input) {
            true => self.share(reg, Rc::new(scatter)),
            false => self.prog.round(reg, Rc::new(scatter)),
        }
    }

    /// `nid`, which reads its input co-located on no group columns, has each
    /// worker aggregate its slice ahead of the exchange. A worker holding the
    /// whole input has nothing to pre-aggregate.
    fn pre_aggregates(&self, nid: NodeId, group: &[u32]) -> bool {
        group.is_empty() && self.circuit.op(nid).exchanges() && !self.meta.held_whole(self.circuit.inputs(nid)[0])
    }

    /// The source whose scans read `reg`, and its scan.
    fn scanned_at(&self, reg: DeltaReg) -> Option<&(u64, Scan)> {
        self.sources.iter().find(|(_, scan)| scan.reg == reg)
    }

    /// The scanned source's store where it holds, on this worker, every row the
    /// integral of the delta its scan reads does: a base table's, with no round
    /// moving that delta — a share of a replicated one leaves the store a
    /// superset. A view's store has absorbed, within one drive, a delta its
    /// reader is yet to be fed.
    fn integral_store(&self, &(source, ref scan): &(u64, Scan)) -> Option<&'a Relation> {
        let relation = self.meta.source(source);
        (relation.kind().is_base_table() && scan.relay.is_none()).then_some(relation)
    }

    /// True when an exchange of `input`'s rows by `scatter` moves nothing: the
    /// plan hashes the bytes the scan behind `input` placed its rows by.
    fn moves_nothing(&self, input: NodeId, scatter: &ScatterPlan) -> bool {
        scanned_row_locally(self.circuit, input)
            // A relayed delta no longer sits where its source's store places it.
            .filter(|&tid| self.meta.source_route(tid).is_none())
            .is_some_and(|tid| scatter.routes_to_native_owner(self.meta.source(tid).placement()))
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
        let scanned = loop {
            if let Some(scanned) = self.scanned_at(reg) {
                break scanned;
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
        // The store's rows outside a share are no part of the share's history,
        // and a step may map one onto an element inside it.
        self.integral_store(scanned)
            .filter(|_| scanned.1.reg == scanned.1.seed)?;
        steps.reverse();
        Some(HistoryTerm {
            relation: scanned.0,
            steps: steps.into(),
            negated,
            // The steps are readers of the scan, so read no column it does not.
            read: scanned.1.read,
        })
    }

    /// The child integrating `of`'s register, declared and scheduled the first time
    /// a join asks.
    fn own_integral(&mut self, of: NodeId) -> StateIdx {
        if let Some((Integral::Own(trace), ..)) = self.integrals[of] {
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

/// The payload columns of `schema`, the rows node `at` emits, that a reader of
/// those rows can tell from zeroes. A filter or an exchange reads what it tests
/// or routes by and hands its rows on to readers of its own; a map or a reduce
/// emits a function of the columns it names and keeps no copy of its input
/// rows. Every other operator, and the output node, whose rows the view's store
/// integrates, reads every column.
fn payload_read(circuit: &Circuit, at: NodeId, schema: &SchemaDescriptor) -> u64 {
    let all = schema.payload_slots();
    let mut read = 0;
    // `at`, and every node handing its rows on unchanged.
    let mut carriers = vec![at];
    while let Some(at) = carriers.pop() {
        if at == circuit.out() {
            return all;
        }
        for nid in circuit.readers(at) {
            read |= match circuit.op(nid) {
                OpNode::Filter(blob) => {
                    carriers.push(nid);
                    LogicalProgram::payload_read(blob, schema)
                }
                // Routes by the PK.
                OpNode::ExchangeShard => {
                    carriers.push(nid);
                    0
                }
                OpNode::Map(MapKind::Compute(cm)) => LogicalProgram::payload_read(&cm.program, schema),
                OpNode::Map(MapKind::Reindex { keep, key, .. }) => {
                    schema.payload_bits(keep.iter().copied().chain(key.iter().map(|&(c, _)| c)))
                }
                OpNode::Map(MapKind::HashRow { cols }) => schema.payload_bits(cols.iter().map(|&(c, _)| c)),
                OpNode::Reduce { group, .. } => schema.payload_bits(group.cols()),
                _ => all,
            };
        }
    }
    read
}

/// Emit `nid`'s instructions and return the register its output lands in: its
/// own, or an input's when the node emits nothing.
fn emit_node(ctx: &mut EmitCtx, nid: NodeId, op: &OpNode) -> Result<DeltaReg, String> {
    match op {
        // `bound` is a backfill-scan hint consumed by the source drive, not by the
        // VM: emission is identical bounded or not.
        OpNode::ScanDelta { source: tid, bound, .. } => {
            let relation = ctx.meta.source(*tid);
            let schema = relation.schema();
            // One epoch seeds one delta, and one backfill cursor feeds every scan of it.
            let read = payload_read(ctx.circuit, nid, &schema);
            if let Some((_, scan)) = ctx.sources.iter_mut().find(|(s, _)| s == tid) {
                scan.bound = ReadBound::None;
                scan.read |= read;
                return Ok(scan.reg);
            }
            let seed = ctx.prog.seed(schema);
            let route = ctx.meta.source_route(*tid);
            // The share or the relay round routes each row by its key's columns.
            let routed_by = route.map_or(0, |r| schema.payload_bits(r.key.iter().map(|&(c, _)| c)));
            let (reg, relay) = match route {
                // Every worker already holds a replicated delta whole, and keeps its share.
                Some(r) if relation.placement().is_replicated() => (ctx.share(seed, Rc::clone(&r.plan)), None),
                Some(r) => (seed, Some(r.clone())),
                None => (seed, None),
            };
            let bound = bound.clone();
            ctx.sources.push((
                *tid,
                Scan {
                    seed,
                    reg,
                    bound,
                    relay,
                    read: read | routed_by,
                },
            ));
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

        OpNode::Reduce { group, .. } => emit_reduce(ctx, nid, group),

        OpNode::TopN { group_cols, order, limit, offset } => {
            let in_reg = ctx.unary_delta_in(nid);
            let partial = match ctx.pre_aggregates(nid, group_cols) {
                true => {
                    let plan = OrderedPlan::top_n_partial(&ctx.prog.schema_of(in_reg), order, *limit, *offset)?;
                    Some(push_ordered(ctx, nid, in_reg, "topnpart", plan))
                }
                false => None,
            };
            let reg = co_locate(ctx, nid, partial.unwrap_or(in_reg), group_cols)?;
            let schema = ctx.prog.schema_of(reg);
            let plan = match partial {
                Some(_) => OrderedPlan::top_n_combine(&schema, order, *limit, *offset)?,
                None => OrderedPlan::top_n(&schema, group_cols, order, *limit, *offset)?,
            };
            Ok(push_ordered(ctx, nid, reg, "topnidx", plan))
        }

        // Every row reads the rows before it, so no worker's slice stands in for them.
        OpNode::Window { group, order, rank, dense_rank } => {
            let reg = co_locate(ctx, nid, ctx.unary_delta_in(nid), &group.group_cols)?;
            let schema = ctx.prog.schema_of(reg);
            let plan = OrderedPlan::window(&schema, &group.group_cols, order, &group.aggs, *rank, *dense_rank)?;
            Ok(push_ordered(ctx, nid, reg, "windowidx", plan))
        }

        OpNode::Join(kind) => {
            let &[da, db, ia, ib] = ctx.circuit.inputs(nid) else {
                unreachable!("a join is wired on four inputs")
            };
            // Each term joins a delta to an integral as it stood before the
            // epoch, so an epoch moving both would miss their product.
            let shared = |d: NodeId, i: NodeId| ctx.prog.share_a_seed(ctx.regs[d], ctx.regs[i]);
            if shared(da, ib) || shared(db, ia) {
                return Err("join: one relation feeds a delta and the integral it probes".into());
            }
            let ab = emit_join_term(ctx, da, ib, *kind, false)?;
            let ba = emit_join_term(ctx, db, ia, *kind, true)?;
            ctx.prog.union(ab, ba)
        }

        OpNode::ExchangeShard => {
            let reg = ctx.unary_delta_in(nid);
            let own = ScatterPlan::native(Placement::full_pk(&ctx.prog.schema_of(reg)));
            Ok(ctx.exchange(nid, reg, own))
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
    let (trace, entry, clamp, plan) = match ctx.source_rekey(integrand) {
        Some((relation, entry, rekey)) => (
            Integral::Relation(relation.id()),
            entry,
            None,
            JoinPlan::over_source(delta_is_right, &delta_schema, &relation.schema(), rekey)?,
        ),
        None => {
            let reg = ctx.regs[integrand];
            let plan = JoinPlan::from_wire(kind, delta_is_right, &delta_schema, &ctx.prog.schema_of(reg))?;
            match ctx.prog.writer(reg) {
                // A clamp's output integral is its history clamped, both integrated
                // in one pass, so the join reads the history through the clamp.
                Some((
                    _,
                    &Op::WeightClamp {
                        hist: ClampHistory::Own(hist),
                        kind: clamp,
                    },
                    _,
                )) => (Integral::Own(hist), reg, Some(clamp), plan.clamped_trace(clamp)),
                _ => (Integral::Own(ctx.own_integral(integrand)), reg, None, plan),
            }
        }
    };
    ctx.integrals[integrand] = Some((trace, entry, clamp));
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

/// `nid`'s input, in `reg`, co-located by `group` over `reg`'s columns where
/// `nid` reads it so.
fn co_locate(ctx: &mut EmitCtx, nid: NodeId, reg: DeltaReg, group: &[u32]) -> Result<DeltaReg, String> {
    if !ctx.circuit.op(nid).exchanges() {
        return Ok(reg);
    }
    let scatter = ScatterPlan::group(&ctx.prog.schema_of(reg), group)?;
    Ok(ctx.exchange(nid, reg, scatter))
}

fn emit_reduce(ctx: &mut EmitCtx, nid: NodeId, group: &GroupAggs) -> Result<DeltaReg, String> {
    let GroupAggs { group_cols, aggs } = group;
    let in_reg = ctx.unary_delta_in(nid);
    let in_schema = ctx.prog.schema_of(in_reg);
    // Minted by the view's backfill, in an epoch of its own.
    let seeds_ground = ctx.circuit.owes_ground_row(nid);
    // `None` where the partials do not combine: the exchange carries the raw rows.
    let partial = match ctx.pre_aggregates(nid, group_cols) {
        true => ReducePlan::partial(&in_schema, aggs)?.map(|plan| push_reduce(ctx, nid, in_reg, plan, true)),
        false => None,
    };
    let reg = co_locate(ctx, nid, partial.unwrap_or(in_reg), group_cols)?;
    let schema = ctx.prog.schema_of(reg);
    let plan = match partial {
        Some(_) => ReducePlan::combine(&schema, aggs, seeds_ground)?,
        None => ReducePlan::from_wire(&schema, group_cols, aggs, seeds_ground)?,
    };
    Ok(push_reduce(ctx, nid, reg, plan, false))
}

/// Declare `plan`'s children under `nid`, and push the reduce over `in_reg`:
/// `nid`'s own, or the `partial` each worker runs ahead of its exchange.
fn push_reduce(ctx: &mut EmitCtx, nid: NodeId, in_reg: DeltaReg, plan: ReducePlan, partial: bool) -> DeltaReg {
    let out_schema = *plan.output_schema();
    let (trace, avidx) = match partial {
        true => ("partial", "partavidx"),
        false => ("reduce", "avidx"),
    };
    // The view's store where the node's own reduce is the output.
    let out_trace = match ctx.circuit.out() == nid && !partial {
        true => Integral::Relation(ctx.view),
        false => Integral::Own(ctx.declare_child(trace, nid, out_schema)),
    };
    // One index table serves every MIN/MAX of the reduce.
    let index = plan.index_schema().map(|schema| ctx.declare_child(avidx, nid, *schema));
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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "tests/emit.rs"]
mod tests;
