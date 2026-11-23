//! Instruction emission: one `emit_node` arm per operator, into the view's one
//! program, each answering where the node's rows sit across the workers.
//!
//! An arm resolves its operands, asks the operator's own constructor for its
//! artifact, and pushes an instruction into a fresh register. The artifacts —
//! output schemas, packers, plans — and the guards a client-supplied
//! circuit clears to get one belong to `gnitz-zset` and `gnitz-trace`, beside the kernels that
//! read them.

use super::routing::{Dist, shifted};
use super::*;
use crate::query::vm::{ClampHistory, HistoryStep, HistoryTerm, Op, ProgramBuilder};
use gnitz_trace::stream::{JoinPlan, OrderedPlan, ReducePlan};
use gnitz_wire::ReduceKind;
use gnitz_zset::algebra::null_extend_output_schema;

// ---------------------------------------------------------------------------
// EmitCtx — the per-plan build state every emit arm works against
// ---------------------------------------------------------------------------

/// All state of one view's emission, threaded to the emit arms as one `&mut`.
pub(super) struct EmitCtx<'a> {
    pub(super) circuit: &'a Circuit,
    /// The view's routing, and its scanned sources.
    pub(super) meta: &'a ViewMeta<'a>,
    pub(super) prog: ProgramBuilder,
    /// The register of every node emitted so far, by node id.
    pub(super) regs: Vec<DeltaReg>,
    /// The integral of each node some join probes, the register its rows enter
    /// at, and the clamp its stored weights are read through: the node's own,
    /// or, for a base table's store, the scan's its re-key reads. A clamp's
    /// integral is its own history, read through the clamp.
    pub(super) integrals: Vec<Option<(Integral, DeltaReg, Option<ClampKind>)>>,
    /// Each scanned source's scan, in the circuit's node order.
    pub(super) sources: Vec<(u64, Scan)>,
    /// The view, whose store is the integral of its output node's register.
    view: u64,
    /// Where the rows of every node emitted so far sit across the workers, by
    /// node id, whatever the worker count.
    pub(super) dists: Vec<Dist>,
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
            dists: Vec::with_capacity(nodes),
        };
        for (nid, op) in circuit.ops() {
            let (reg, dist) = emit_node(&mut ctx, nid, op)?;
            ctx.regs.push(reg);
            // In a replicated view every worker computes every node from its own
            // full copies: nothing relays or exchanges.
            ctx.dists.push(if meta.replicated { Dist::Whole } else { dist });
        }
        Ok(ctx)
    }

    /// Emit node `nid`'s scan of source `tid`: the seed its delta enters at, and
    /// the share its route keeps of a replicated one. `bound` is a backfill-scan
    /// hint consumed by the source drive, not by the VM: emission is identical
    /// bounded or not.
    fn emit_scan(&mut self, nid: NodeId, tid: u64, bound: &ReadBound) -> (DeltaReg, Dist) {
        let source = &self.meta.sources[&tid];
        let schema = source.relation.schema();
        let read = payload_read(self.circuit, nid, &schema);
        let seed = self.prog.seed(schema);
        let route = self.meta.source_route(tid);
        // The share or the relay round routes each row by its key's columns.
        let routed_by = route.map_or(0, |r| schema.payload_bits(r.key.iter().map(|&(c, _)| c)));
        let (reg, relay) = match route {
            // Every worker already holds a replicated delta whole, and keeps its share.
            Some(r) if source.relation.placement().is_replicated() => (self.prog.share(seed, Rc::clone(&r.plan)), None),
            Some(r) => (seed, Some(r.clone())),
            None => (seed, None),
        };
        let scan = Scan {
            seed,
            reg,
            bound: bound.clone(),
            relay,
            read: read | routed_by,
        };
        self.sources.push((tid, scan));
        (reg, source.dist.clone())
    }

    /// `reg` — an exchanging operator's input of schema `in_schema` sitting by
    /// `in_dist`, or the partial each worker reduced it to — placed by the plan
    /// `scatter` builds over `reg`'s schema: left where it stands when every
    /// worker computes the view's whole result or the input's rows already sit
    /// where the plan puts them, this worker's share where every worker holds
    /// the input whole, an exchange round otherwise. The plan is built, and a
    /// bad key refused, at any worker count.
    fn gather(
        &mut self,
        (in_dist, in_schema): (&Dist, &SchemaDescriptor),
        reg: DeltaReg,
        scatter: impl FnOnce(&SchemaDescriptor) -> Result<ScatterPlan, String>,
    ) -> Result<DeltaReg, String> {
        let scatter = scatter(&self.prog.schema_of(reg))?;
        if self.meta.self_contained || scatter.routes_to_native_owner(in_dist.placement(in_schema)) {
            return Ok(reg);
        }
        let plan = Rc::new(scatter);
        Ok(match *in_dist == Dist::Whole {
            true => self.prog.share(reg, plan),
            false => self.prog.round(reg, plan),
        })
    }

    /// An exchanging operator over `group` of an input sitting by `in_dist` has
    /// each worker aggregate its slice ahead of the exchange: one co-locating no
    /// group columns. A worker holding the whole input has nothing to
    /// pre-aggregate.
    fn pre_aggregates(&self, in_dist: &Dist, group: &[u32]) -> bool {
        group.is_empty() && !self.meta.self_contained && *in_dist != Dist::Whole
    }

    /// `reg`, written by an operator that exchanges, beside where it leaves its
    /// rows: on the owner of the key it scatters by, the PK its operator emits,
    /// wherever its input sat.
    fn exchanged(&self, reg: DeltaReg) -> (DeltaReg, Dist) {
        let out = self.prog.schema_of(reg);
        (reg, Dist::pk(&out, out.pk_cols().len()))
    }

    /// The scanned source whose scans read `reg`, and its scan, where the
    /// source's store holds, on this worker, every row the integral of that
    /// delta does: a base table's, with no round moving the delta — a share of
    /// a replicated one leaves the store a superset. A view's store has
    /// absorbed, within one drive, a delta its reader is yet to be fed.
    fn stored_scan(&self, reg: DeltaReg) -> Option<(&'a Relation, &Scan)> {
        let (source, scan) = self.sources.iter().find(|(_, scan)| scan.reg == reg)?;
        let relation = self.meta.sources[source].relation;
        (relation.kind().is_base_table() && scan.relay.is_none()).then_some((relation, scan))
    }

    /// The base table whose store is already the integral of `of`, the register
    /// its scan reads, and `of`'s re-key of that register.
    fn source_rekey(&self, of: NodeId) -> Option<(&'a Relation, DeltaReg, &MapPlan)> {
        let (Op::Map(rekey), entry) = self.prog.writer(self.regs[of])? else {
            return None;
        };
        let (relation, _) = self.stored_scan(entry)?;
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
    /// rows whose stores hold its history: each the scan whose rows reach the
    /// operand under their own keys, and the instructions on the way.
    fn source_history(&self, reg: DeltaReg) -> Option<Box<[HistoryTerm]>> {
        let mut terms = Vec::new();
        let mut operands = vec![(reg, false)];
        while let Some((reg, negated)) = operands.pop() {
            match self.prog.writer(reg) {
                Some((Op::Union { in_b }, in_a)) => operands.extend([(in_a, negated), (*in_b, negated)]),
                Some((Op::Negate, input)) => operands.push((input, !negated)),
                _ => terms.push(self.history_term(reg, negated)?),
            }
        }
        Some(terms.into())
    }

    /// The scan whose rows reach `reg` under their own keys, and the
    /// instructions on the way, where the scanned table's store holds their
    /// history.
    fn history_term(&self, mut reg: DeltaReg, negated: bool) -> Option<HistoryTerm> {
        let mut steps = Vec::new();
        let (relation, scan) = loop {
            if let Some(stored) = self.stored_scan(reg) {
                break stored;
            }
            let (op, input) = self.prog.writer(reg)?;
            match op {
                Op::Filter(pred) => steps.push(HistoryStep::Filter(pred.clone())),
                Op::Map(plan) if plan.inherits_pk() => steps.push(HistoryStep::Map(plan.clone())),
                // A share keeps or drops whole elements.
                Op::Share(_) => {}
                _ => return None,
            }
            reg = input;
        };
        // The store's rows outside a share are no part of the share's history,
        // and a step may map one onto an element inside it.
        if scan.reg != scan.seed {
            return None;
        }
        steps.reverse();
        Some(HistoryTerm {
            relation: relation.id(),
            steps: steps.into(),
            negated,
            // The steps are readers of the scan, so read no column it does not.
            read: scan.read,
        })
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

/// Emit `nid`'s instructions and return the register its output lands in — its
/// own, or an input's when the node emits nothing — beside where its rows sit,
/// given where its inputs' do. Refuses an operator that would read rows of one
/// element, or of one join key, spread over several workers.
fn emit_node(ctx: &mut EmitCtx, nid: NodeId, op: &OpNode) -> Result<(DeltaReg, Dist), String> {
    if let OpNode::ScanDelta { source, bound, .. } = op {
        return Ok(ctx.emit_scan(nid, *source, bound));
    }
    let inputs = ctx.circuit.inputs(nid);
    // The first input: every operator but a scan reads one.
    let in_reg = ctx.regs[inputs[0]];
    let in_schema = ctx.prog.schema_of(in_reg);
    let in_dist = ctx.dists[inputs[0]].clone();
    Ok(match op {
        OpNode::ScanDelta { .. } => unreachable!("a scan is emitted above"),

        // Every row is handed on, or dropped, on its worker and under its columns.
        OpNode::Filter(blob) => {
            // A present-but-corrupt blob, or a rejected program, is catalog
            // corruption. Falling back to pass-all would silently turn a WHERE
            // into WHERE TRUE; fail the compile instead.
            let pred = LogicalProgram::from_blob(blob)
                .and_then(|p| p.resolve_filter(&in_schema))
                .map_err(|e| format!("filter: invalid predicate program: {e}"))?;
            (ctx.prog.push(in_reg, in_schema, Op::Filter(Box::new(pred))), in_dist)
        }

        OpNode::Negate => (ctx.prog.push(in_reg, in_schema, Op::Negate), in_dist),

        // Every `MapKind`'s output schema, program and PK source are one fact,
        // derived by [`MapPlan::from_wire`].
        OpNode::Map(mk) => {
            let plan = MapPlan::from_wire(&in_schema, mk)?;
            // A MAP that reproduces its input row verbatim emits nothing; the
            // node's consumers read the input register instead.
            if plan.is_identity() {
                return Ok((in_reg, in_dist));
            }
            let dist = in_dist.mapped(|c, ty| plan.holders(&in_schema, c, ty));
            (
                ctx.prog.push(in_reg, *plan.output_schema(), Op::Map(Box::new(plan))),
                dist,
            )
        }

        OpNode::Union => {
            let reg = ctx.prog.union(in_reg, ctx.regs[inputs[1]])?;
            (reg, in_dist.common(&ctx.dists[inputs[1]])?)
        }

        OpNode::WeightClamp(kind) => {
            // A set admits an element once, so one whose weight is split over
            // workers would be admitted once per part. A positive part over rows
            // no key co-locates is how an outer join's null-fill cancels per worker.
            if *kind == ClampKind::Distinct && in_dist == Dist::Anywhere {
                return Err("a distinct over rows no key co-locates".into());
            }
            let hist = match ctx.source_history(in_reg) {
                Some(terms) => ClampHistory::Sources(terms),
                None => ClampHistory::Own(ctx.declare_child("hist", nid, in_schema)),
            };
            (
                ctx.prog.push(in_reg, in_schema, Op::WeightClamp { hist, kind: *kind }),
                in_dist,
            )
        }

        // A local reduce aggregates each worker's slice as it holds it, and owes
        // no ground row.
        OpNode::Reduce {
            group: GroupAggs { group_cols, aggs },
            kind: ReduceKind::Local,
        } => {
            let plan = ReducePlan::from_wire(&in_schema, group_cols, aggs, false)?;
            let reg = push_reduce(ctx, nid, in_reg, plan, false);
            match in_dist {
                Dist::Whole => (reg, Dist::Whole),
                _ => (reg, Dist::Anywhere),
            }
        }

        OpNode::Reduce {
            group: GroupAggs { group_cols, aggs }, ..
        } => {
            // Minted by the view's backfill, in an epoch of its own.
            let seeds_ground = op.owes_ground_row();
            // `None` where the partials do not combine: the exchange carries the raw rows.
            let partial = match ctx.pre_aggregates(&in_dist, group_cols) {
                true => ReducePlan::partial(&in_schema, aggs)?.map(|plan| push_reduce(ctx, nid, in_reg, plan, true)),
                false => None,
            };
            let input = (&in_dist, &in_schema);
            let reg = ctx.gather(input, partial.unwrap_or(in_reg), |s| ScatterPlan::group(s, group_cols))?;
            let schema = ctx.prog.schema_of(reg);
            let plan = match partial {
                Some(_) => ReducePlan::combine(&schema, aggs, seeds_ground)?,
                None => ReducePlan::from_wire(&schema, group_cols, aggs, seeds_ground)?,
            };
            let reg = push_reduce(ctx, nid, reg, plan, false);
            ctx.exchanged(reg)
        }

        OpNode::TopN { group_cols, order, slots } => {
            let partial = match ctx.pre_aggregates(&in_dist, group_cols) {
                true => {
                    let plan = OrderedPlan::top_n_partial(&in_schema, order, *slots)?;
                    Some(push_ordered(ctx, nid, in_reg, "topnpart", plan))
                }
                false => None,
            };
            let input = (&in_dist, &in_schema);
            let reg = ctx.gather(input, partial.unwrap_or(in_reg), |s| ScatterPlan::group(s, group_cols))?;
            let schema = ctx.prog.schema_of(reg);
            let plan = match partial {
                Some(_) => OrderedPlan::top_n_combine(&schema, order, *slots)?,
                None => OrderedPlan::top_n(&schema, group_cols, order, *slots)?,
            };
            let reg = push_ordered(ctx, nid, reg, "topnidx", plan);
            ctx.exchanged(reg)
        }

        // Every row reads the rows before it, so no worker's slice stands in for them.
        OpNode::Window { group, order, rank, dense_rank } => {
            let reg = ctx.gather((&in_dist, &in_schema), in_reg, |s| {
                ScatterPlan::group(s, &group.group_cols)
            })?;
            let schema = ctx.prog.schema_of(reg);
            let plan = OrderedPlan::window(&schema, &group.group_cols, order, &group.aggs, *rank, *dense_rank)?;
            let reg = push_ordered(ctx, nid, reg, "windowidx", plan);
            ctx.exchanged(reg)
        }

        // Where each term leaves its rows, where both agree.
        OpNode::Join(kind) => {
            let &[da, db, ia, ib] = inputs else {
                unreachable!("a join is wired on four inputs")
            };
            // Each term joins a delta to an integral as it stood before the
            // epoch, so an epoch moving both would miss their product.
            let shared = |d: NodeId, i: NodeId| ctx.prog.share_a_seed(ctx.regs[d], ctx.regs[i]);
            if shared(da, ib) || shared(db, ia) {
                return Err("join: one relation feeds a delta and the integral it probes".into());
            }
            let (ab, ab_dist) = emit_join_term(ctx, da, ib, *kind, false)?;
            let (ba, ba_dist) = emit_join_term(ctx, db, ia, *kind, true)?;
            (ctx.prog.union(ab, ba)?, ab_dist.common(&ba_dist)?)
        }

        OpNode::ExchangeShard => {
            let reg = ctx.gather((&in_dist, &in_schema), in_reg, |s| {
                Ok(ScatterPlan::native(Placement::full_pk(s)))
            })?;
            ctx.exchanged(reg)
        }

        OpNode::NullExtend { type_codes, nulls_first } => {
            let out = null_extend_output_schema(&in_schema, type_codes, *nulls_first)?;
            let dist = in_dist.mapped(|c, _| shifted(&in_schema, &out, c, *nulls_first));
            let op = Op::NullExtend { nulls_first: *nulls_first };
            (ctx.prog.push(in_reg, out, op), dist)
        }
    })
}

// ---------------------------------------------------------------------------
// JOIN emission
// ---------------------------------------------------------------------------

/// One term of a join: `delta ⋈ z⁻¹I(integrand)`, the delta carrying the right
/// SQL side iff `delta_is_right`, beside where it leaves its rows. Refuses a
/// term whose two operands are each spread over the workers by other than the
/// key's leading columns the join matches on equal.
fn emit_join_term(
    ctx: &mut EmitCtx,
    delta: NodeId,
    integrand: NodeId,
    kind: JoinKind,
    delta_is_right: bool,
) -> Result<(DeltaReg, Dist), String> {
    let delta_reg = ctx.regs[delta];
    let delta_schema = ctx.prog.schema_of(delta_reg);
    let int_reg = ctx.regs[integrand];
    let int_schema = ctx.prog.schema_of(int_reg);
    // One constructor owns every kind's guards and its output layout.
    let (trace, entry, clamp, plan) = match ctx.source_rekey(integrand) {
        Some((relation, entry, rekey)) => (
            Integral::Relation(relation.id()),
            entry,
            None,
            JoinPlan::over_source(delta_is_right, &delta_schema, &relation.schema(), rekey)?,
        ),
        None => {
            let plan = JoinPlan::from_wire(kind, delta_is_right, &delta_schema, &int_schema)?;
            match ctx.prog.writer(int_reg) {
                // A clamp's output integral is its history clamped, both integrated
                // in one pass, so the join reads the history through the clamp.
                Some((
                    &Op::WeightClamp {
                        hist: ClampHistory::Own(hist),
                        kind: clamp,
                    },
                    _,
                )) => (Integral::Own(hist), int_reg, Some(clamp), plan.clamped_trace(clamp)),
                // The child integrating the integrand's register, declared and
                // scheduled the first time a join asks.
                _ => {
                    let trace = match ctx.integrals[integrand] {
                        Some((Integral::Own(trace), ..)) => trace,
                        _ => {
                            let trace = ctx.declare_child("int", integrand, int_schema);
                            ctx.prog.integrate(int_reg, trace);
                            trace
                        }
                    };
                    (Integral::Own(trace), int_reg, None, plan)
                }
            }
        }
    };
    ctx.integrals[integrand] = Some((trace, entry, clamp));
    let out = *plan.output_schema();
    let reg = ctx
        .prog
        .push(delta_reg, out, Op::JoinDT { trace, plan: Box::new(plan) });

    // The output column holding column `c` of an operand, of the right SQL
    // side iff `right`: the output is `[key, left payload, right payload]`, and a
    // cross join's pair key is the left side's PK region, then the right's.
    let onto = |side: SchemaDescriptor, right: bool| move |c: u32, _| shifted(&side, &out, c, right);
    let (d, i) = (&ctx.dists[delta], &ctx.dists[integrand]);
    let d_out = d.mapped(onto(delta_schema, delta_is_right));
    let i_out = i.mapped(onto(int_schema, !delta_is_right));
    let dist = match (d, i) {
        // Every worker meets the whole of one side with its part of the other.
        (Dist::Whole, _) => i_out,
        (_, Dist::Whole) => d_out,
        _ => {
            let key_cols = delta_schema.pk_cols().len();
            let matched = match kind {
                JoinKind::Equi => key_cols,
                JoinKind::Range { .. } => key_cols - 1,
                JoinKind::Cross => 0,
            };
            match (d.pk_prefix(&delta_schema), i.pk_prefix(&int_schema)) {
                // Matched rows share the key bytes the output leads with.
                (Some(a), Some(b)) if a == b && a <= matched => Dist::pk(&out, a).merged(&d_out).merged(&i_out),
                _ => return Err("join: a term's operands are not co-located by its key".into()),
            }
        }
    };
    Ok((reg, dist))
}

// ---------------------------------------------------------------------------
// Stateful operators' children
// ---------------------------------------------------------------------------

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
