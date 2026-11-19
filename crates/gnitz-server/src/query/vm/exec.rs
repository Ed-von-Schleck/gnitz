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
    fn source_state(
        &self,
        delta: &Batch,
        terms: &[HistoryTerm],
        instructions: &mut [Instr],
    ) -> Result<SmallVec<[Run; 2]>, String> {
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
            for &pc in term.steps.iter() {
                rows = match &mut instructions[pc].op {
                    Op::Filter(pred) => algebra::op_filter(Cow::Owned(rows), pred),
                    // A map may reorder the payload a run sorts by.
                    Op::Map(plan) => plan.evaluate_map_batch(&rows.as_mem_batch()).into_consolidated(),
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

/// How one run reads the register it seeds.
struct Pass<'s> {
    /// The register the run seeds.
    reg: DeltaReg,
    /// The caller's batch the seed was lent from, until it is handed back:
    /// while lent, no reader takes it.
    lent: Option<&'s mut Batch>,
    /// A tick's run, which integrates. A replay does not.
    tick: bool,
}

/// A tick's epoch standing at an exchange round: the program's registers, and
/// the round they wait at. Dropped there, it frees the rows it holds.
pub(in crate::query) struct Epoch {
    batches: Box<[Batch]>,
    /// The register the epoch seeded.
    reg: DeltaReg,
    /// The round's instruction.
    pc: usize,
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
        let batch = operand(&mut self.batches[self.operand.at()], self.take);
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
/// caller's again when this returns, labelled with its register's schema and
/// folded where its register folds: a copy, where the epoch stands at a round
/// and reads its seed past it.
pub(in crate::query) fn start(
    vm: &mut Vm,
    stores: &mut Stores<'_>,
    reg: DeltaReg,
    mut seed: Option<&mut Batch>,
    take: bool,
) -> Result<Ran, String> {
    enter(vm, reg, seed.as_deref_mut());
    let lent = seed.filter(|_| !take);
    run_on(vm, stores, Pass { reg, lent, tick: true }, 0)
}

/// Run `epoch` on from the round it stands at, `gathered` this worker's share
/// of that round, to its next round or to its end.
pub(in crate::query) fn resume(
    vm: &mut Vm,
    stores: &mut Stores<'_>,
    epoch: Epoch,
    gathered: Batch,
) -> Result<Ran, String> {
    let Epoch { batches, reg, pc, .. } = epoch;
    vm.batches = batches;
    let out_reg = vm.instructions[pc].out_reg;
    vm.batches[out_reg.at()] = gathered;
    fold_written(&mut vm.batches, &vm.regs, out_reg);
    run_on(vm, stores, Pass { reg, lent: None, tick: true }, pc + 1)
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
/// `reg` may be one an instruction writes: every instruction up to its writer
/// reads empty registers, and is skipped. No round stands: a replay's seed
/// reaches no stateful operator, a round included, and a ground-row mint seeds
/// a reduce's output, which is in no round's reach, so a round behind it would
/// be skipped; routing refuses any exchange behind a global aggregate.
fn run_through(
    vm: &mut Vm,
    stores: &mut Stores<'_>,
    reg: DeltaReg,
    tick: bool,
    mut seed: Batch,
) -> Result<Batch, String> {
    enter(vm, reg, Some(&mut seed));
    match run_on(vm, stores, Pass { reg, lent: None, tick }, 0)? {
        Ran::Done(out) => Ok(out),
        Ran::Round(_) => unreachable!("no round stands past the entry"),
    }
}

/// Ready the registers for a run from the first instruction, `seed` standing in
/// `reg`, labelled with the register's schema and folded where the register
/// folds: the caller's batch is left holding what `reg` held, empty.
fn enter(vm: &mut Vm, reg: DeltaReg, seed: Option<&mut Batch>) {
    // An epoch dropped at its round took the registers with it.
    if vm.batches.len() != vm.regs.len() {
        vm.batches = registers(&vm.regs);
    }
    if let Some(seed) = seed {
        vm.label(reg, seed);
        std::mem::swap(&mut vm.batches[reg.at()], seed);
        fold_written(&mut vm.batches, &vm.regs, reg);
    }
}

/// Run from instruction `pc` to the next exchange round or to the end. A lent
/// seed goes back to the caller wherever the run stops.
fn run_on(vm: &mut Vm, stores: &mut Stores<'_>, pass: Pass, pc: usize) -> Result<Ran, String> {
    let ran = run_instructions(vm, stores, &pass, pc).and_then(|round| match round {
        None if pass.tick => run_integrates(vm, stores.own, &pass).map(|()| None),
        round => Ok(round),
    });
    let pc = match ran {
        Ok(Some(pc)) => pc,
        Ok(None) => return Ok(Ran::Done(end(vm, pass))),
        Err(e) => {
            end(vm, pass);
            return Err(e);
        }
    };
    if let Some(seed) = pass.lent {
        // The caller reads its delta while the epoch stands, so the epoch keeps
        // the batch only where it reads it again, and hands back a copy.
        let keeps = vm.read_from(pass.reg, pc);
        let held = &mut vm.batches[pass.reg.at()];
        match keeps {
            true => *seed = held.clone(),
            false => std::mem::swap(seed, held),
        }
    }
    let instr = &vm.instructions[pc];
    let Op::Round(plan) = &instr.op else {
        unreachable!("a run stops only at a round")
    };
    let operand = instr.in_reg;
    // Every worker sends its rows, and decodes theirs, under this label.
    debug_assert_eq!(vm.batches[operand.at()].schema(), &vm.regs[operand.at()].schema);
    Ok(Ran::Round(Epoch {
        reg: pass.reg,
        pc,
        operand,
        take: vm.regs[operand.at()].last_read == LastRead::Instr(pc),
        plan: Rc::clone(plan),
        fold: vm.regs[instr.out_reg.at()].fold,
        batches: std::mem::take(&mut vm.batches),
    }))
}

/// Whether `reader` may take `reg`'s batch: it is the register's last reader,
/// and the batch is the program's own rather than a lent seed.
#[inline]
fn takes(regs: &[Reg], pass: &Pass, reg: DeltaReg, reader: LastRead) -> bool {
    regs[reg.at()].last_read == reader && !(pass.lent.is_some() && reg == pass.reg)
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

/// Run the instruction stream from `from`, to its end or to the round it next
/// stands at.
fn run_instructions(vm: &mut Vm, stores: &mut Stores<'_>, pass: &Pass, from: usize) -> Result<Option<usize>, String> {
    let Vm { instructions, regs, batches, .. } = vm;

    for pc in from..instructions.len() {
        let (before, rest) = instructions.split_at_mut(pc);
        let instr = &mut rest[0];
        let (in_reg, out_reg) = (instr.in_reg, instr.out_reg);
        let takes = |r: DeltaReg| takes(regs, pass, r, LastRead::Instr(pc));

        let reads = instr.reads();
        let all_empty = reads.into_iter().flatten().all(|r| batches[r.at()].is_empty());
        let out = match &mut instr.op {
            Op::Round(_) => match regs[pass.reg.at()].rounds.contains(&pc) {
                true => return Ok(Some(pc)),
                // The register stays as every run leaves it: empty.
                false => continue,
            },

            // The kernel would return an empty batch, and the out register is
            // already one, under its own label: every run leaves every
            // register empty.
            _ if all_empty => continue,

            Op::Filter(pred) => algebra::op_filter(operand(&mut batches[in_reg.at()], takes(in_reg)), pred),

            Op::Map(plan) => plan.evaluate_map_batch(&batches[in_reg.at()].as_mem_batch()),

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
                let mut open = |_: &[u8], _: &[u8]| from_runs(runs.iter().cloned(), schema);
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
fn run_integrates(vm: &mut Vm, state: &mut CircuitState, pass: &Pass) -> Result<(), String> {
    let Vm { integrates, regs, batches, .. } = vm;
    for (i, &(reg, trace)) in integrates.iter().enumerate() {
        // Nothing to add, as for most of a join's integrands in any one epoch.
        if batches[reg.at()].is_empty() {
            continue;
        }
        let take = takes(regs, pass, reg, LastRead::Integrate(i));
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
fn end(vm: &mut Vm, pass: Pass) -> Batch {
    let Vm { regs, batches, out_reg, .. } = vm;
    // The output register has no reader to take it but this extraction.
    let take = takes(regs, &pass, *out_reg, LastRead::Nobody);
    let batch = operand(&mut batches[out_reg.at()], take).into_owned();
    if let Some(seed) = pass.lent {
        std::mem::swap(&mut batches[pass.reg.at()], seed);
    }
    debug_assert_eq!(batch.schema(), &regs[out_reg.at()].schema);
    for held in batches.iter_mut() {
        held.release_buffers();
    }
    batch
}

#[cfg(test)]
#[path = "tests/exec.rs"]
mod tests;
