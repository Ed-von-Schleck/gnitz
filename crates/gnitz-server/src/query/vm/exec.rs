//! Epoch execution: the entry points and the opcode dispatch loop.

use gnitz_foundation::gnitz_debug;
use std::borrow::Cow;

use super::*;
use gnitz_store::relation::{Cut, Relation, RelationRegistry};
use gnitz_trace::runs::{PkSetGather, ReadCursor, Run, StorageError, empty_cursor, from_runs};
use gnitz_trace::stream;
use gnitz_wire::PkKeys;
use gnitz_zset::algebra;
use gnitz_zset::repr::Batch;
use smallvec::SmallVec;

/// The stores an epoch reads and writes: the view's own children, and the
/// relations behind its plan's [`Integral::Relation`]s.
pub(in crate::query) struct Stores<'a> {
    pub(in crate::query) own: &'a mut CircuitState,
    pub(in crate::query) registry: &'a RelationRegistry,
    /// The scanned relations this view has been fed no row of: the sources a
    /// backfill has yet to reach. Each reads empty.
    pub(in crate::query) unfed: &'a [u64],
}

/// A store an operator reads its history from, resolved for one dispatch.
#[derive(Clone, Copy)]
enum Trace<'a> {
    /// One of the view's own children.
    Child(StateIdx),
    Relation(&'a Relation),
    /// A source the view has been fed no row of.
    Unfed(&'a Relation),
}

impl Stores<'_> {
    /// The store `integral` names.
    fn trace(&self, integral: Integral) -> Result<Trace<'_>, String> {
        Ok(match integral {
            Integral::Own(idx) => Trace::Child(idx),
            Integral::Relation(id) => {
                let relation = self.registry.relation_or_err(id)?;
                match self.unfed.contains(&id) {
                    true => Trace::Unfed(relation),
                    false => Trace::Relation(relation),
                }
            }
        })
    }

    /// The opener of `store`, as a kernel takes it.
    fn open<'s>(&'s self, store: Trace<'s>) -> impl FnMut(&[u8], &[u8]) -> ReadCursor {
        move |first: &[u8], last: &[u8]| match store {
            Trace::Child(idx) => self.own.cursor_between(idx, first, last),
            Trace::Relation(relation) => relation.cursor_between(first, last, Cut::Sealed),
            Trace::Unfed(relation) => empty_cursor(relation.schema()),
        }
    }

    /// What `terms`' stores hold at `delta`'s keys, `delta` included, one run per
    /// term through its `steps`.
    fn source_state(&self, delta: &Batch, terms: &mut [HistoryTerm]) -> Result<SmallVec<[Run; 2]>, String> {
        // Consolidated, so its keys ascend, a key repeated only beside itself.
        debug_assert!(delta.is_consolidated());
        let mut keys = PkKeys::from_ascending(delta.schema().pk_stride(), delta.pk());
        let mut runs = SmallVec::new();
        for term in terms {
            let mut gather = self.gather(Integral::Relation(term.relation), keys, term.read)?;
            let chunk = gather.drain_chunk(usize::MAX);
            keys = gather.into_keys();
            let Some(mut rows) = chunk else {
                continue;
            };
            for step in term.steps.iter_mut() {
                rows = match step {
                    HistoryStep::Filter(pred) => algebra::op_filter(Cow::Owned(rows), pred),
                    // A map may reorder the payload a run sorts by.
                    HistoryStep::Map(plan) => plan.evaluate_map_batch(&rows.as_mem_batch()).into_consolidated(),
                };
            }
            if term.negated {
                rows = rows.negated();
            }
            runs.push(Run::Mem(Rc::new(rows)));
        }
        Ok(runs)
    }

    /// Every live row of `keys` in `integral`, for a reader of the payload
    /// columns `read` names, as payload bits, of a relation's rows.
    fn gather(&self, integral: Integral, keys: PkKeys, read: u64) -> Result<PkSetGather, String> {
        Ok(match self.trace(integral)? {
            Trace::Child(idx) => self.own.gather(idx, keys),
            Trace::Relation(relation) => relation.gather(keys, Cut::Sealed, read),
            Trace::Unfed(relation) => PkSetGather::over(empty_cursor(relation.schema()), keys),
        })
    }
}

/// The one context template for a `vm:` ingest fault.
#[cold]
fn ingest_err(op: &str, idx: StateIdx, e: StorageError) -> String {
    format!("vm: {op} ingest (state_idx={idx:?}): {e}")
}

/// A tick's epoch standing at an exchange round: the program's registers, and
/// the round they wait at. Dropped there, it frees the rows it holds.
pub(in crate::query) struct Epoch {
    batches: Box<[Batch]>,
    /// The register the epoch seeded.
    reg: DeltaReg,
    /// The round's position in the seed's run.
    at: usize,
    /// The round's operand, and whether the round is its last reader.
    operand: DeltaReg,
    take: bool,
    plan: Rc<ScatterPlan>,
    /// The round's output register folds.
    fold: bool,
}

impl Epoch {
    /// The round the epoch stands at: its operand — lent, or given where the
    /// round is its last reader — the plan it is sent under, and whether what
    /// it gathers is gathered by merge.
    pub(in crate::query) fn round(&mut self) -> (Cow<'_, Batch>, &ScatterPlan, bool) {
        let batch = operand(&mut self.batches[self.operand.0], self.take);
        (batch, &self.plan, self.fold)
    }
}

/// Where a run stopped.
#[expect(clippy::large_enum_variant, reason = "returned once per round, and boxing allocates")]
pub(in crate::query) enum Ran {
    /// At its end, with the output register's batch: empty when the epoch
    /// produced nothing.
    Done(Batch),
    /// At an exchange round. [`resume`] runs the epoch on over what the round
    /// gathered.
    Round(Epoch),
}

/// Run a tick's epoch over `seed`, the delta of the source seeding `reg` —
/// `None` an empty one — to its first exchange round or to its end. Where
/// `take`, the seed is taken and left empty. Else it is lent, and is the
/// caller's again when this returns `Ok`, labelled with its register's schema
/// and folded where its register folds: a copy, where the epoch stands at a
/// round and reads its seed past it.
pub(in crate::query) fn start(
    vm: &mut Vm,
    stores: &mut Stores<'_>,
    reg: DeltaReg,
    mut seed: Option<&mut Batch>,
    take: bool,
) -> Result<Ran, String> {
    let batches = enter(vm, reg, seed.as_deref_mut());
    run(vm, stores, batches, reg, seed.filter(|_| !take), true, None)
}

/// Run `epoch` on from the round it stands at, `gathered` this worker's share
/// of that round, to its next round or to its end.
pub(in crate::query) fn resume(
    vm: &mut Vm,
    stores: &mut Stores<'_>,
    epoch: Epoch,
    gathered: Batch,
) -> Result<Ran, String> {
    let Epoch { batches, reg, at, .. } = epoch;
    run(vm, stores, batches, reg, None, true, Some((at, gathered)))
}

/// Every row of a bounded view at `keys`, recomputed: `replay`'s seed gathered
/// at them and run through the program, chunk by chunk, integrating nothing.
pub(in crate::query) fn replay_keys(
    vm: &mut Vm,
    stores: &mut Stores<'_>,
    replay: &Replay,
    keys: PkKeys,
) -> Result<Batch, String> {
    let mut gather = stores.gather(replay.seed, keys, replay.read)?;
    let mut out = Batch::empty_with_schema(vm.out_schema());
    while let Some(mut seed) = gather.drain_chunk(stores.registry.scan_chunk_rows()) {
        if let Some(kind) = replay.clamp {
            // Clamped against no history: each gathered weight, clamped.
            let schema = *seed.schema();
            let history = stream::History::Before(&mut |_, _| empty_cursor(schema));
            seed = stream::op_weight_clamp(Cow::Owned(seed), history, kind);
        }
        let produced = run_through(vm, stores, replay.reg, false, seed)?;
        out.append_above(produced.into_consolidated());
    }
    Ok(out)
}

/// Mint the row each global aggregate owes over an empty input, each in a run
/// of its own seeding its reduce's output, and answer what they produced.
pub(in crate::query) fn mint_ground_rows(vm: &mut Vm, stores: &mut Stores<'_>) -> Result<Batch, String> {
    let mut minted = Batch::empty_with_schema(vm.out_schema());
    for pc in 0..vm.instructions.len() {
        let instr = &vm.instructions[pc];
        let Op::Reduce { plan, .. } = &instr.op else { continue };
        let Some(row) = plan.ground_row() else {
            continue;
        };
        let reg = instr.out_reg;
        minted.append_batch(&run_through(vm, stores, reg, true, row)?);
    }
    Ok(minted)
}

/// Run `seed`, taken, from `reg` to the program's end, integrating iff `tick`.
/// `reg` may be one an instruction writes. No round stands: a replay's seed
/// reaches no stateful operator, a round included, and a ground-row mint seeds
/// a reduce's output, behind which routing refuses any exchange.
fn run_through(
    vm: &mut Vm,
    stores: &mut Stores<'_>,
    reg: DeltaReg,
    tick: bool,
    mut seed: Batch,
) -> Result<Batch, String> {
    let batches = enter(vm, reg, Some(&mut seed));
    match run(vm, stores, batches, reg, None, tick, None)? {
        Ran::Done(out) => Ok(out),
        Ran::Round(_) => unreachable!("no round stands past the entry"),
    }
}

/// The registers of a run seeding `reg` with `seed`, labelled with the
/// register's schema and folded where the register folds: the caller's batch
/// is left holding what `reg` held, empty. Each register's batch is empty under
/// its own label, so a kernel skipped over empty operands leaves its output as
/// it would have written it.
fn enter(vm: &mut Vm, reg: DeltaReg, seed: Option<&mut Batch>) -> Box<[Batch]> {
    let mut batches = std::mem::take(&mut vm.spare);
    if batches.len() != vm.regs.len() {
        batches = vm.regs.iter().map(|r| Batch::empty_with_schema(&r.schema)).collect();
    }
    if let Some(seed) = seed {
        vm.label(reg, seed);
        std::mem::swap(&mut batches[reg.0], seed);
        if vm.regs[reg.0].fold {
            batches[reg.0].consolidate_in_place();
        }
    }
    batches
}

/// A register's batch as an operand: moved out when `take`, lent otherwise.
#[inline]
fn operand(batch: &mut Batch, take: bool) -> Cow<'_, Batch> {
    match take {
        true => Cow::Owned(batch.take()),
        false => Cow::Borrowed(batch),
    }
}

/// Run `batches`, the registers of an epoch seeded at `seed`, along the seed's
/// run, integrating iff `tick` — from its start, or from the round `resumed`
/// names, over what that round gathered — to the next exchange round or to its
/// end. A `lent` seed is the caller's again wherever the run stops; on an error
/// the registers, the seed among them, are dropped.
fn run(
    vm: &mut Vm,
    stores: &mut Stores<'_>,
    mut batches: Box<[Batch]>,
    seed: DeltaReg,
    lent: Option<&mut Batch>,
    tick: bool,
    resumed: Option<(usize, Batch)>,
) -> Result<Ran, String> {
    let Vm {
        instructions,
        integrates,
        regs,
        spare,
        out_reg,
    } = vm;
    let n = instructions.len();
    let run = &regs[seed.0].run;
    // The integrates are the run's tail, past every instruction, so no cursor
    // an instruction opens observes this tick's integration.
    let end = match tick {
        true => run.len(),
        false => run.partition_point(|&at| at < n),
    };
    let (from, mut gathered) = resumed.map_or((0, None), |(at, rows)| (at, Some(rows)));
    // While lent, the seed's batch is the caller's, and no reader takes it.
    let lent_reg = lent.is_some().then_some(seed);
    // Whether the reader at `at` on the timeline may take `reg`'s batch.
    let takes = |reg: DeltaReg, at: usize| regs[reg.0].last_read == at && lent_reg != Some(reg);

    let stood = 'run: {
        for (i, &pc) in run[..end].iter().enumerate().skip(from) {
            let Some(instr) = instructions.get_mut(pc) else {
                let (reg, trace) = integrates[pc - n];
                // Nothing to add, as for most of a join's integrands in any one epoch.
                if batches[reg.0].is_empty() {
                    continue;
                }
                gnitz_debug!("vm: INTEGRATE in_count={}", batches[reg.0].len());
                let batch = match operand(&mut batches[reg.0], takes(reg, pc)) {
                    Cow::Owned(batch) => batch,
                    Cow::Borrowed(batch) => batch.to_consolidated(),
                };
                let res = stores.own.ingest(trace, batch);
                res.map_err(|e| ingest_err("integrate", trace, e))?;
                continue;
            };
            let (in_reg, out_reg) = (instr.in_reg, instr.out_reg);
            let take = |r: DeltaReg| takes(r, pc);

            let reads = instr.reads();
            let all_empty = reads.into_iter().flatten().all(|r| batches[r.0].is_empty());
            let out = match &mut instr.op {
                // The round's output is what every worker sent this one.
                Op::Round(_) => match gathered.take() {
                    Some(rows) => rows,
                    None => break 'run Some(i),
                },

                // The kernel would return an empty batch, and the out register is
                // already one, under its own label.
                _ if all_empty => continue,

                Op::Filter(pred) => algebra::op_filter(operand(&mut batches[in_reg.0], take(in_reg)), pred),

                Op::Map(plan) => plan.evaluate_map_batch(&batches[in_reg.0].as_mem_batch()),

                Op::Negate => operand(&mut batches[in_reg.0], take(in_reg)).into_owned().negated(),

                Op::Union { in_b } if in_reg == *in_b => {
                    // One register cannot be lent as two operands, and Z + Z
                    // doubles every weight. Saturating: a wrapping double sends
                    // `i64::MIN` to 0, a ghost under the claim `map_weights` keeps.
                    let mut batch = operand(&mut batches[in_reg.0], take(in_reg)).into_owned();
                    batch.map_weights(|w| w.saturating_mul(2));
                    batch
                }

                Op::Union { in_b } => {
                    let [a, b] = batches
                        .get_disjoint_mut([in_reg.0, in_b.0])
                        .expect("the arm above took a union of one register with itself");
                    // The union's own (nullability-merged) schema, not the
                    // left input's — see `union_nullability_merge` for why a
                    // narrower one mis-sorts nulls.
                    algebra::op_union(
                        operand(a, take(in_reg)),
                        operand(b, take(*in_b)),
                        &regs[out_reg.0].schema,
                    )
                }

                Op::WeightClamp { hist: ClampHistory::Own(hist), kind } => stream::op_weight_clamp(
                    operand(&mut batches[in_reg.0], take(in_reg)),
                    stream::History::Before(&mut stores.open(Trace::Child(*hist))),
                    *kind,
                ),

                Op::WeightClamp { hist: ClampHistory::Sources(terms), kind } => {
                    let runs = stores.source_state(&batches[in_reg.0], terms)?;
                    let schema = regs[in_reg.0].schema;
                    let delta = operand(&mut batches[in_reg.0], take(in_reg));
                    let mut open = |_: &[u8], _: &[u8]| from_runs(runs.iter().cloned(), schema);
                    stream::op_weight_clamp(delta, stream::History::Through(&mut open), *kind)
                }

                Op::JoinDT { trace, plan } => {
                    let mut open = stores.open(stores.trace(*trace)?);
                    stream::op_join_delta_trace(&batches[in_reg.0], &mut open, plan)
                }

                Op::Share(plan) => plan.share(&batches[in_reg.0], stores.registry.slot()),

                Op::NullExtend { nulls_first } => {
                    algebra::null_extend(&batches[in_reg.0], &regs[out_reg.0].schema, *nulls_first)
                }

                Op::Reduce { out_trace, index, plan } => {
                    let delta = &batches[in_reg.0];
                    // Ingested before the index is opened, so a prefix seek over
                    // it sees the rows this epoch wrote.
                    if let Some((idx, entries)) = index.zip(plan.index_entries(delta)) {
                        let res = stores.own.ingest(idx, entries);
                        res.map_err(|e| ingest_err("value index", idx, e))?;
                    }

                    let mut open_out = stores.open(stores.trace(*out_trace)?);
                    let mut open_index = index.map(|idx| stores.open(Trace::Child(idx)));
                    let index_at = open_index.as_mut().map(|open| open as stream::OpenAt<'_>);
                    stream::op_reduce(delta, &mut open_out, index_at, plan)
                }

                Op::Ordered { index, plan } => {
                    let delta = &batches[in_reg.0];
                    let epoch = plan.epoch(delta, &mut stores.open(Trace::Child(*index)));
                    let res = stores.own.ingest(*index, epoch.index_entries);
                    res.map_err(|e| ingest_err("order index", *index, e))?;
                    epoch.out
                }
            };
            // Free every operand this instruction was the last reader of, here
            // rather than per arm — so a register's lifetime follows the dataflow
            // instead of whether some kernel happens to take `Batch` by value.
            for r in reads.into_iter().flatten() {
                if take(r) {
                    batches[r.0].release_buffers();
                }
            }
            // After the release, so a fold never runs beside an input this
            // instruction was the last reader of. `out_reg` is none of its reads:
            // `ProgramBuilder::push` gives every instruction a fresh one.
            batches[out_reg.0] = out;
            if regs[out_reg.0].fold {
                batches[out_reg.0].consolidate_in_place();
            }
        }
        None
    };

    let Some(at) = stood else {
        // The output register has no reader to take it but this extraction.
        let out = operand(&mut batches[out_reg.0], takes(*out_reg, usize::MAX)).into_owned();
        debug_assert_eq!(out.schema(), &regs[out_reg.0].schema);
        if let Some(lent) = lent {
            std::mem::swap(&mut batches[seed.0], lent);
        }
        // Whatever a register still holds, the run left nothing to read it.
        for held in batches.iter_mut() {
            held.release_buffers();
        }
        *spare = batches;
        return Ok(Ran::Done(out));
    };
    let pc = run[at];
    if let Some(lent) = lent {
        // The caller reads its delta while the epoch stands, so the epoch keeps
        // the batch only where it reads it again, and hands back a copy.
        let held = &mut batches[seed.0];
        match regs[seed.0].last_read >= pc {
            true => *lent = held.clone(),
            false => std::mem::swap(lent, held),
        }
    }
    let instr = &instructions[pc];
    let Op::Round(plan) = &instr.op else {
        unreachable!("a run stands only at a round")
    };
    let operand = instr.in_reg;
    // Every worker sends its rows, and decodes theirs, under this label.
    debug_assert_eq!(batches[operand.0].schema(), &regs[operand.0].schema);
    Ok(Ran::Round(Epoch {
        reg: seed,
        at,
        operand,
        take: regs[operand.0].last_read == pc,
        plan: Rc::clone(plan),
        fold: regs[instr.out_reg.0].fold,
        batches,
    }))
}

#[cfg(test)]
#[path = "tests/exec.rs"]
mod tests;
