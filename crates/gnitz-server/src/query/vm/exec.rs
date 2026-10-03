//! Epoch execution: the two entry points and the opcode dispatch loop.

use std::borrow::Cow;

use super::*;
use gnitz_expr::SchemaFacts;
use gnitz_store::relation::{Cut, RelationRegistry};
use gnitz_wire::PkKeys;
use gnitz_zset::repr::{Batch, PkSetGather, ReadCursor, StorageError};
use gnitz_zset::{algebra, stream};

/// The stores an epoch reads and writes: the view's own children, and the
/// relations behind its plan's [`Integral::Source`]s.
pub(in crate::query) struct Stores<'a> {
    pub(in crate::query) own: &'a mut CircuitState,
    pub(in crate::query) registry: &'a RelationRegistry,
    /// The view the plan computes.
    pub(in crate::query) view: u64,
    /// The scanned relations this view has been fed no row of: the sources a
    /// backfill has yet to reach. Each reads empty.
    pub(in crate::query) unfed: &'a [u64],
}

impl Stores<'_> {
    /// A cursor over `integral` for a join probing it with `delta`: at `delta`'s
    /// keys when `keyed`, over every row otherwise. A source's rows are there as
    /// the view last absorbed them.
    fn probe_cursor(&self, integral: Integral, delta: &Batch, keyed: bool) -> Result<ReadCursor, String> {
        let source = match integral {
            Integral::Own(trace) if keyed => return Ok(self.own.cursor_for_keys(trace, delta)),
            Integral::Own(trace) => return Ok(self.own.cursor(trace)),
            Integral::Source(source) => source,
        };
        debug_assert!(keyed, "the compiler bakes a source under an equi probe alone");
        let relation = self.registry.relation_or_err(source)?;
        if self.unfed.contains(&source) {
            return Ok(gnitz_zset::repr::empty_cursor(relation.schema()));
        }
        Ok(relation.cursor_for_keys(delta, Cut::Sealed))
    }

    /// The opener of a reduce's or top-N's output history: its own trace, or the
    /// view's store, which every earlier epoch's output was ingested into.
    fn out_trace(&self, own: Option<StateIdx>) -> Result<impl FnMut(&[u8], &[u8]) -> ReadCursor + '_, String> {
        let view = self.registry.relation_or_err(self.view)?;
        Ok(move |first: &[u8], last: &[u8]| match own {
            Some(trace) => self.own.cursor_between(trace, first, last),
            None => view.cursor_between(first, last, Cut::Now),
        })
    }

    /// The opener of a reduce's or top-N's index.
    fn index(&self, idx: StateIdx) -> impl FnMut(&[u8], &[u8]) -> ReadCursor + '_ {
        move |first: &[u8], last: &[u8]| self.own.cursor_between(idx, first, last)
    }

    /// Every live row of `keys` in `integral`: a source's as the view last
    /// absorbed them.
    pub(in crate::query) fn gather(&self, integral: Integral, keys: PkKeys) -> Result<PkSetGather, String> {
        Ok(match integral {
            Integral::Own(trace) => self.own.gather(trace, keys),
            Integral::Source(source) => self.registry.relation_or_err(source)?.gather(keys, Cut::Sealed),
        })
    }
}

/// The one context template for a `vm:` ingest fault.
#[cold]
fn ingest_err(op: &str, idx: StateIdx, e: StorageError) -> String {
    format!("vm: {op} ingest (state_idx={idx:?}): {e}")
}

/// Execute one epoch over `inputs`, one `(register, batch)` per seeded input —
/// one per exchange side for a post phase, one everywhere else. Returns the
/// output register's batch, empty when the epoch produced nothing.
pub(in crate::query) fn execute_epoch(
    vm: &mut Vm,
    stores: &mut Stores<'_>,
    inputs: impl IntoIterator<Item = (DeltaReg, Batch)>,
) -> Result<Batch, String> {
    let all_empty = seed_inputs(vm, inputs);
    // Spent by any dispatched epoch, not just an empty one: the ground reduce
    // runs either way and leaves V₀ in its output trace.
    let ground_pending = std::mem::take(&mut vm.pending_ground_row);
    if all_empty && !ground_pending {
        return Ok(take_output(vm));
    }
    let ran = run_instructions(vm, stores, 0).and_then(|()| run_integrates(vm, stores.own));
    let out = take_output(vm);
    ran.map(|()| out)
}

/// One chunk of a capacity-bounded view's hydration replay, which runs no
/// integrate.
pub(in crate::query) fn replay_chunk(
    vm: &mut Vm,
    stores: &mut Stores<'_>,
    entry: ReplayEntry,
    seed: Batch,
) -> Result<Batch, String> {
    seed_inputs(vm, std::iter::once((entry.reg, seed)));
    let ran = run_instructions(vm, stores, entry.pc);
    let out = take_output(vm);
    ran.map(|()| out)
}

/// Move each seed into its own register, reporting whether every seed was
/// empty. Every register is empty on entry: [`take_output`] ends each run by
/// freeing them. Split from [`run_instructions`] so only these lines are generic
/// over the seed iterator, not the whole loop below.
fn seed_inputs(vm: &mut Vm, inputs: impl IntoIterator<Item = (DeltaReg, Batch)>) -> bool {
    let mut all_empty = true;
    for (input_reg, input_batch) in inputs {
        all_empty &= input_batch.is_empty();
        // Rows under the wrong layout would scramble every downstream read
        // silently. An empty batch has no rows to misread, so it is exempt.
        if !input_batch.is_empty() {
            assert_eq!(
                input_batch.schema().num_columns(),
                vm.schema_of(input_reg).num_columns(),
                "VM register {} schema/batch column-count mismatch",
                input_reg.0,
            );
        }
        vm.batches[input_reg.at()] = input_batch;
        fold_written(&mut vm.batches, &vm.regs, input_reg);
    }
    all_empty
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

/// Run the instruction stream from `start_pc`.
fn run_instructions(vm: &mut Vm, stores: &mut Stores<'_>, start_pc: usize) -> Result<(), String> {
    let Vm { instructions, regs, batches, out_reg, .. } = vm;

    gnitz_debug!("vm: dispatch out_reg={} instrs={}", out_reg.0, instructions.len());

    for (pc, instr) in instructions.iter_mut().enumerate().skip(start_pc) {
        let (in_reg, out_reg) = (instr.in_reg, instr.out_reg);
        let takes = |r: DeltaReg| regs[r.at()].last_read == LastRead::Instr(pc);

        let all_empty = instr.reads().into_iter().flatten().all(|r| batches[r.at()].is_empty());
        // The kernel would return exactly this, so skip it and its trace cursor.
        let out = if instr.facts.inert_on_empty && all_empty {
            Batch::empty_with_schema(&regs[out_reg.at()].schema)
        } else {
            match &mut instr.op {
                Op::Filter(pred) => algebra::op_filter(&batches[in_reg.at()], pred)
                    .unwrap_or_else(|| operand(&mut batches[in_reg.at()], takes(in_reg)).into_owned()),

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

                Op::WeightClamp { hist, kind } => {
                    let delta = &batches[in_reg.at()];
                    let mut cursor = stores.own.cursor_for_keys(*hist, delta);
                    stream::op_weight_clamp(delta, &mut cursor, *kind)
                }

                Op::JoinDT { trace, probe } => {
                    let delta = &batches[in_reg.at()];
                    let mut cursor = stores.probe_cursor(*trace, delta, probe.probes_delta_keys())?;
                    stream::op_join_delta_trace(delta, &mut cursor, &regs[out_reg.at()].schema, probe)
                }

                Op::WorkerFilter { slot } => algebra::op_worker_filter(&batches[in_reg.at()], *slot),

                Op::NullExtend { nulls_first } => {
                    batches[in_reg.at()].widened_with_nulls(&regs[out_reg.at()].schema, *nulls_first)
                }

                Op::Reduce { out_trace, index, plan } => {
                    let delta = &batches[in_reg.at()];
                    // An empty delta touches no group; only a ground-seeding
                    // reduce reaches here with one.
                    let entries = match delta.is_empty() {
                        true => None,
                        false => index.zip(plan.index_batch(delta)),
                    };
                    // Ingested before the index is opened, so a prefix seek over
                    // it sees the rows this epoch wrote.
                    if let Some((idx, entries)) = entries {
                        let res = stores.own.ingest_owned(idx, entries);
                        res.map_err(|e| ingest_err("avi", idx, e))?;
                    }

                    gnitz_debug!("vm: REDUCE in_count={} avi={}", delta.len(), index.is_some());

                    let mut open_out = stores.out_trace(*out_trace)?;
                    let mut open_index = index.map(|idx| stores.index(idx));
                    let history = open_index.as_mut().map(|open| open as stream::OpenAt<'_>);
                    stream::op_reduce(delta, &mut open_out, history, plan)
                }

                Op::TopN { out_trace, index, plan } => {
                    let delta = &batches[in_reg.at()];
                    // Ingested before the index is opened, as the reduce's is.
                    let res = stores.own.ingest_owned(*index, plan.index_batch(delta));
                    res.map_err(|e| ingest_err("topn index", *index, e))?;
                    let mut open_out = stores.out_trace(*out_trace)?;
                    stream::op_topn(delta, &mut open_out, &mut stores.index(*index), plan)
                }
            }
        };
        batches[out_reg.at()] = out;

        // Free every operand this instruction was the last reader of, here
        // rather than per arm — so a register's lifetime follows the dataflow
        // instead of whether some kernel happens to take `Batch` by value.
        for r in instr.reads().into_iter().flatten() {
            if takes(r) {
                batches[r.at()].release_buffers();
            }
        }

        // After the release, so a fold never runs beside an input this
        // instruction was the last reader of. `out_reg` is none of its reads:
        // `ProgramBuilder::push` gives every instruction a fresh one.
        fold_written(batches, regs, out_reg);
    }

    gnitz_debug!("vm: dispatch done");
    Ok(())
}

/// Accumulate each delta into its trace, after the whole instruction range — so
/// no cursor opened in that range can observe this tick's integration.
fn run_integrates(vm: &mut Vm, state: &mut CircuitState) -> Result<(), String> {
    let Vm { integrates, regs, batches, .. } = vm;
    for (i, &(reg, trace)) in integrates.iter().enumerate() {
        let take = regs[reg.at()].last_read == LastRead::Integrate(i);
        gnitz_debug!("vm: INTEGRATE in_count={}", batches[reg.at()].len());
        let res = match operand(&mut batches[reg.at()], take) {
            Cow::Owned(batch) => state.ingest_owned(trace, batch),
            Cow::Borrowed(batch) => state.ingest_borrowed(trace, batch),
        };
        res.map_err(|e| ingest_err("integrate", trace, e))?;
    }
    Ok(())
}

/// End a run: extract the output, labelled with the output register's own
/// schema, and free every register — so none holds rows between runs, whether
/// the run completed, failed, or left a register nothing reads. A seed arrives
/// under its producer's label and the operators that hand their input through
/// keep it, so the label here may be narrower in nullability than the
/// register's.
fn take_output(vm: &mut Vm) -> Batch {
    let Vm { regs, batches, out_reg, .. } = vm;
    let want = &regs[out_reg.at()].schema;
    let mut batch = batches[out_reg.at()].take();
    for held in batches.iter_mut() {
        held.release_buffers();
    }
    if batch.is_empty() {
        return Batch::empty_with_schema(want);
    }
    // Only a narrower nullability may legitimately arrive here. A different
    // physical layout means the batch was built against another schema
    // entirely, which the stamp would hide from the wire encode.
    debug_assert!(
        batch.schema().same_layout(want),
        "VM output register {}: batch label is not the register's physical layout",
        out_reg.0,
    );
    batch.set_schema(want);
    batch
}

#[cfg(test)]
#[path = "tests/exec.rs"]
mod tests;
