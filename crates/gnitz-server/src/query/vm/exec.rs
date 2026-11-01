//! Epoch execution: the two entry points and the opcode dispatch loop.

use std::borrow::Cow;

use super::*;
use gnitz_store::relation::{Cut, Relation, RelationRegistry};
use gnitz_wire::PkKeys;
use gnitz_zset::repr::{empty_cursor, from_runs, Batch, PkSetGather, ReadCursor, Run, StorageError};
use gnitz_zset::{algebra, stream};

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
    fn open<'s>(&'s self, store: Trace<'s>) -> impl FnMut(&[u8], &[u8]) -> ReadCursor + 's {
        move |first: &[u8], last: &[u8]| match store {
            Trace::Child(idx) => self.own.cursor_between(idx, first, last),
            Trace::Relation(relation) => relation.cursor_between(first, last, Cut::Sealed),
            Trace::Unfed(relation) => empty_cursor(relation.schema()),
        }
    }

    /// What `terms`' stores hold at `delta`'s keys, `delta` included, one run per
    /// term through its `steps`.
    fn source_state(
        &self,
        delta: &Batch,
        terms: &[HistoryTerm],
        instructions: &mut [Instr],
    ) -> Result<Vec<Run>, String> {
        let mut keys = PkKeys::from_vec(delta.schema().pk_stride(), delta.pk().to_vec());
        let mut runs = Vec::with_capacity(terms.len());
        for term in terms {
            let mut gather = self.gather(Integral::Relation(term.relation), keys)?;
            let chunk = gather.drain_chunk(usize::MAX);
            keys = gather.into_keys();
            let Some(mut rows) = chunk else {
                continue;
            };
            for &pc in term.steps.iter() {
                rows = match &mut instructions[pc].op {
                    Op::Filter(pred) => algebra::op_filter(Cow::Owned(rows), pred),
                    // A map may reorder the payload a run sorts by.
                    Op::Map(plan) => plan.evaluate_map_batch(&rows).into_consolidated(),
                    _ => unreachable!("a history step is a filter or a map"),
                };
            }
            if term.negated {
                rows = rows.negated();
            }
            runs.push(Run::Mem(Rc::new(rows)));
        }
        Ok(runs)
    }

    /// Every live row of `keys` in `integral`.
    pub(in crate::query) fn gather(&self, integral: Integral, keys: PkKeys) -> Result<PkSetGather, String> {
        Ok(match self.trace(integral)? {
            Trace::Child(idx) => self.own.gather(idx, keys),
            Trace::Relation(relation) => relation.gather(keys, Cut::Sealed),
            Trace::Unfed(relation) => PkSetGather::over(empty_cursor(relation.schema()), keys),
        })
    }
}

/// The one context template for a `vm:` ingest fault.
#[cold]
fn ingest_err(op: &str, idx: StateIdx, e: StorageError) -> String {
    format!("vm: {op} ingest (state_idx={idx:?}): {e}")
}

/// One run of a program over the delta seeding one register. The seed is lent:
/// it comes back labelled with its register's schema, folded where its
/// register folds, and empty where `take` let its last reader have it.
pub(in crate::query) struct Epoch<'e> {
    reg: DeltaReg,
    /// The one slot a batch leaves the program through: the seed until the
    /// epoch begins, then the batch its register held, swapped for a round's
    /// operand while that round is out.
    seed: &'e mut Batch,
    take: bool,
    /// The next instruction.
    pc: usize,
    /// A tick's epoch, which integrates. A replay does not.
    tick: bool,
}

impl<'e> Epoch<'e> {
    /// A tick's epoch over `seed`, the delta of the source seeding `reg`.
    pub(in crate::query) fn tick(reg: DeltaReg, seed: &'e mut Batch, take: bool) -> Self {
        Epoch { reg, seed, take, pc: 0, tick: true }
    }
}

/// Where a [`run`] stopped.
pub(in crate::query) enum Ran<'f> {
    /// At its end, with the output register's batch: empty when the epoch
    /// produced nothing.
    Done(Batch),
    /// At an exchange round of `batch` under `plan`, gathered by merge iff
    /// `fold`. The next [`run`] is handed what the round gathered.
    Round {
        plan: Rc<ScatterPlan>,
        batch: Cow<'f, Batch>,
        fold: bool,
    },
}

/// Run `epoch` to its next exchange round or to its end. `gathered`: this
/// worker's share of the round the last run stopped at, `None` on the first.
pub(in crate::query) fn run<'f>(
    vm: &mut Vm,
    stores: &mut Stores<'_>,
    epoch: &'f mut Epoch<'_>,
    gathered: Option<Batch>,
) -> Result<Ran<'f>, String> {
    match gathered {
        None => begin(vm, epoch),
        Some(rows) => {
            let Instr { in_reg, out_reg, .. } = vm.instructions[epoch.pc];
            // The round's operand comes back from the epoch's slot.
            std::mem::swap(&mut vm.batches[in_reg.at()], epoch.seed);
            vm.batches[out_reg.at()] = rows;
            fold_written(&mut vm.batches, &vm.regs, out_reg);
            epoch.pc += 1;
        }
    }
    let ran = run_instructions(vm, stores, epoch).and_then(|round| match round {
        None if epoch.tick => run_integrates(vm, stores.own, epoch).map(|()| None),
        round => Ok(round),
    });
    let pc = match ran {
        Ok(Some(pc)) => pc,
        Ok(None) => return Ok(Ran::Done(end(vm, epoch))),
        Err(e) => {
            end(vm, epoch);
            return Err(e);
        }
    };
    let instr = &vm.instructions[pc];
    let Op::Round { plan, .. } = &instr.op else {
        unreachable!("a run stops only at a round")
    };
    let (plan, in_reg) = (Rc::clone(plan), instr.in_reg);
    let fold = vm.regs[instr.out_reg.at()].fold;
    let take = takes(&vm.regs, epoch, in_reg, LastRead::Instr(pc));
    epoch.pc = pc;
    // The operand leaves for the round through the epoch's slot, lent or given.
    std::mem::swap(&mut vm.batches[in_reg.at()], epoch.seed);
    // Every worker sends its rows, and decodes theirs, under this label.
    debug_assert_eq!(epoch.seed.schema(), &vm.regs[in_reg.at()].schema);
    Ok(Ran::Round {
        plan,
        batch: operand(epoch.seed, take),
        fold,
    })
}

/// One chunk of a capacity-bounded view's hydration replay.
pub(in crate::query) fn replay_chunk(
    vm: &mut Vm,
    stores: &mut Stores<'_>,
    entry: ReplayEntry,
    mut seed: Batch,
) -> Result<Batch, String> {
    run_through(vm, stores, entry.reg, entry.pc, false, &mut seed)
}

/// Mint the row each global aggregate owes over an empty input, each in an
/// epoch of its own entered behind its reduce, and answer what they produced.
pub(in crate::query) fn mint_ground_rows(vm: &mut Vm, stores: &mut Stores<'_>) -> Result<Batch, String> {
    let mut minted = Batch::empty_with_schema(vm.out_schema());
    for pc in 0..vm.instructions.len() {
        let instr = &vm.instructions[pc];
        let Op::Reduce { plan, .. } = &instr.op else { continue };
        let Some(mut row) = plan.ground_row().cloned() else {
            continue;
        };
        let reg = instr.out_reg;
        minted.append_batch(&run_through(vm, stores, reg, pc + 1, true, &mut row)?);
    }
    Ok(minted)
}

/// Run `seed`, taken, from `reg` and instruction `pc` to the program's end,
/// integrating iff `tick`. A replay entry passes no round: `replay_entry` refuses
/// a seed that reaches a stateful op, a round included. A ground-row mint enters
/// at a reduce's output, which no round's seeds name, so a round behind it would
/// be skipped; routing refuses any exchange behind a global aggregate.
fn run_through(
    vm: &mut Vm,
    stores: &mut Stores<'_>,
    reg: DeltaReg,
    pc: usize,
    tick: bool,
    seed: &mut Batch,
) -> Result<Batch, String> {
    let mut epoch = Epoch { reg, seed, take: true, pc, tick };
    match run(vm, stores, &mut epoch, None)? {
        Ran::Done(out) => Ok(out),
        Ran::Round { .. } => unreachable!("no round stands past the entry"),
    }
}

/// Stand the seed in its register, until the epoch ends, labelled with the
/// register's schema and folded where the register folds.
fn begin(vm: &mut Vm, epoch: &mut Epoch<'_>) {
    std::mem::swap(&mut vm.batches[epoch.reg.at()], epoch.seed);
    let seed = &mut vm.batches[epoch.reg.at()];
    let want = &vm.regs[epoch.reg.at()].schema;
    // Rows under the wrong layout would scramble every downstream read
    // silently.
    assert!(
        seed.schema().same_region_types(want),
        "VM register {} schema/batch region mismatch",
        epoch.reg.0,
    );
    // A producer's label may be narrower in nullability. Every kernel labels
    // what it writes with its register's schema, so from here on every
    // register's batch carries its own.
    seed.set_schema(want);
    fold_written(&mut vm.batches, &vm.regs, epoch.reg);
}

/// Whether `reader` may take `reg`'s batch: it is the register's last reader,
/// and the batch is the program's own or a seed lent to be taken.
#[inline]
fn takes(regs: &[Reg], epoch: &Epoch<'_>, reg: DeltaReg, reader: LastRead) -> bool {
    regs[reg.at()].last_read == reader && (reg != epoch.reg || epoch.take)
}

/// Fold `reg`'s batch, just written, when its register folds: every reader runs
/// after the write, so each sees the folded form.
#[inline]
fn fold_written(batches: &mut [Batch], regs: &[Reg], reg: DeltaReg) {
    if regs[reg.at()].fold {
        batches[reg.at()].consolidate_in_place();
    }
}

/// A register's batch as an operand: moved out when `take`, lent otherwise.
#[inline]
fn operand(batch: &mut Batch, take: bool) -> Cow<'_, Batch> {
    match take {
        true => Cow::Owned(batch.take()),
        false => Cow::Borrowed(batch),
    }
}

/// Run the instruction stream from `epoch.pc`, to its end or to the round it
/// next stands at.
fn run_instructions(vm: &mut Vm, stores: &mut Stores<'_>, epoch: &Epoch<'_>) -> Result<Option<usize>, String> {
    let Vm { instructions, regs, batches, .. } = vm;

    for pc in epoch.pc..instructions.len() {
        let (before, rest) = instructions.split_at_mut(pc);
        let instr = &mut rest[0];
        let (in_reg, out_reg) = (instr.in_reg, instr.out_reg);
        let takes = |r: DeltaReg| takes(regs, epoch, r, LastRead::Instr(pc));

        let reads = instr.reads();
        let all_empty = reads.into_iter().flatten().all(|r| batches[r.at()].is_empty());
        let out = match &mut instr.op {
            Op::Round { seeds, .. } => match seeds.contains(&epoch.reg) {
                true => return Ok(Some(pc)),
                // The register stays as every run leaves it: empty.
                false => continue,
            },

            // The kernel would return an empty batch, and the out register is
            // already one, under its own label: every run leaves every
            // register empty.
            _ if all_empty => continue,

            Op::Filter(pred) => algebra::op_filter(operand(&mut batches[in_reg.at()], takes(in_reg)), pred),

            Op::Map(plan) => plan.evaluate_map_batch(&batches[in_reg.at()]),

            Op::Negate => operand(&mut batches[in_reg.at()], takes(in_reg)).into_owned().negated(),

            Op::Union { in_b } if in_reg == *in_b => {
                // One register cannot be lent as two operands, and Z + Z
                // doubles every weight. Saturating: a wrapping double sends
                // `i64::MIN` to 0, a ghost under the claim `map_weights` keeps.
                let mut batch = operand(&mut batches[in_reg.at()], takes(in_reg)).into_owned();
                batch.map_weights(|w| w.saturating_mul(2));
                batch
            }

            Op::Union { in_b } => {
                let [a, b] = batches
                    .get_disjoint_mut([in_reg.at(), in_b.at()])
                    .expect("the arm above took a union of one register with itself");
                // The union's own (nullability-merged) schema, not the
                // left input's — see `union_nullability_merge` for why a
                // narrower one mis-sorts nulls.
                algebra::op_union(
                    operand(a, takes(in_reg)),
                    operand(b, takes(*in_b)),
                    &regs[out_reg.at()].schema,
                )
            }

            // Never taken: the integrate of `hist` reads the register after the clamp.
            Op::WeightClamp { hist: ClampHistory::Own(hist), kind } => stream::op_weight_clamp(
                Cow::Borrowed(&batches[in_reg.at()]),
                stream::History::Before(&mut stores.open(Trace::Child(*hist))),
                *kind,
            ),

            Op::WeightClamp { hist: ClampHistory::Sources(terms), kind } => {
                let runs = stores.source_state(&batches[in_reg.at()], terms, before)?;
                let schema = regs[in_reg.at()].schema;
                let delta = operand(&mut batches[in_reg.at()], takes(in_reg));
                let mut open = |_: &[u8], _: &[u8]| from_runs(runs.iter().cloned(), schema, runs.len());
                stream::op_weight_clamp(delta, stream::History::Through(&mut open), *kind)
            }

            Op::JoinDT { trace, plan } => {
                let mut open = stores.open(stores.trace(*trace)?);
                stream::op_join_delta_trace(&batches[in_reg.at()], &mut open, plan)
            }

            Op::Share(plan) => plan.share(&batches[in_reg.at()], stores.registry.slot()),

            Op::NullExtend { nulls_first } => {
                batches[in_reg.at()].widened_with_nulls(&regs[out_reg.at()].schema, *nulls_first)
            }

            Op::Reduce { out_trace, index, plan } => {
                let delta = &batches[in_reg.at()];
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
                let delta = &batches[in_reg.at()];
                let epoch = plan.epoch(delta, &mut stores.open(Trace::Child(*index)));
                let res = stores.own.ingest(*index, epoch.index_entries);
                res.map_err(|e| ingest_err("order index", *index, e))?;
                epoch.out
            }
        };
        batches[out_reg.at()] = out;

        // Free every operand this instruction was the last reader of, here
        // rather than per arm — so a register's lifetime follows the dataflow
        // instead of whether some kernel happens to take `Batch` by value.
        for r in reads.into_iter().flatten() {
            if takes(r) {
                batches[r.at()].release_buffers();
            }
        }

        // After the release, so a fold never runs beside an input this
        // instruction was the last reader of. `out_reg` is none of its reads:
        // `ProgramBuilder::push` gives every instruction a fresh one.
        fold_written(batches, regs, out_reg);
    }

    Ok(None)
}

/// Accumulate each delta into its trace, after the whole instruction range — so
/// no cursor opened in that range can observe this tick's integration.
fn run_integrates(vm: &mut Vm, state: &mut CircuitState, epoch: &Epoch<'_>) -> Result<(), String> {
    let Vm { integrates, regs, batches, .. } = vm;
    for (i, &(reg, trace)) in integrates.iter().enumerate() {
        // Nothing to add, as for most of a join's integrands in any one epoch.
        if batches[reg.at()].is_empty() {
            continue;
        }
        let take = takes(regs, epoch, reg, LastRead::Integrate(i));
        gnitz_debug!("vm: INTEGRATE in_count={}", batches[reg.at()].len());
        let batch = match operand(&mut batches[reg.at()], take) {
            Cow::Owned(batch) => batch,
            Cow::Borrowed(batch) => batch.to_consolidated(),
        };
        state
            .ingest(trace, batch)
            .map_err(|e| ingest_err("integrate", trace, e))?;
    }
    Ok(())
}

/// End a run: extract the output, hand a lent seed back, and free every
/// register — so none holds rows between runs, whether the run completed,
/// failed, or left a register nothing reads.
fn end(vm: &mut Vm, epoch: &mut Epoch<'_>) -> Batch {
    let Vm { regs, batches, out_reg, .. } = vm;
    // The output register has no reader to take it but this extraction.
    let take = takes(regs, epoch, *out_reg, LastRead::Nobody);
    let batch = operand(&mut batches[out_reg.at()], take).into_owned();
    // A seed that was given is not handed back: the caller holds the
    // register's empty batch, and the loop below frees what nothing read.
    if !epoch.take {
        std::mem::swap(&mut batches[epoch.reg.at()], epoch.seed);
    }
    for held in batches.iter_mut() {
        held.release_buffers();
    }
    debug_assert_eq!(batch.schema(), &regs[out_reg.at()].schema);
    batch
}

#[cfg(test)]
#[path = "tests/exec.rs"]
mod tests;
