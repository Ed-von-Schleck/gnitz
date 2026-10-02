//! Epoch execution: the two entry points and the opcode dispatch loop.

use super::*;
use gnitz_expr::SchemaFacts;
use gnitz_store::relation::{Cut, RelationRegistry};
use gnitz_wire::PkKeys;
use gnitz_zset::repr::{Batch, PkSetGather, ReadCursor, StorageError};
use gnitz_zset::{algebra, stream};

/// What an epoch reads of the relations its plan scans beyond their deltas: the
/// stores behind its [`Integral::Source`]s.
pub(in crate::query) struct SourceReads<'a> {
    pub(in crate::query) registry: &'a RelationRegistry,
    /// The scanned relations this view has been fed no row of: the sources a
    /// backfill has yet to reach. Each reads empty.
    pub(in crate::query) unfed: &'a [u64],
}

impl SourceReads<'_> {
    /// A cursor over `integral` for probing at `delta`'s keys: a source's rows
    /// there as the view last absorbed them.
    fn cursor_for_keys(&self, state: &CircuitState, integral: Integral, delta: &Batch) -> Result<ReadCursor, String> {
        let source = match integral {
            Integral::Own(trace) => return Ok(state.cursor_for_keys(trace, delta)),
            Integral::Source(source) => source,
        };
        let relation = self.registry.relation_or_err(source)?;
        if self.unfed.contains(&source) {
            return Ok(gnitz_zset::repr::empty_cursor(relation.schema()));
        }
        Ok(relation.cursor_for_keys(delta, Cut::Sealed))
    }

    /// Every live row of `keys` in `integral`: a source's as the view last
    /// absorbed them.
    pub(in crate::query) fn gather(
        &self,
        state: &CircuitState,
        integral: Integral,
        keys: PkKeys,
    ) -> Result<PkSetGather, String> {
        Ok(match integral {
            Integral::Own(trace) => state.gather(trace, keys),
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
/// two for a set-op post phase, one everywhere else. Returns the output
/// register's batch, empty when the epoch produced nothing.
pub(in crate::query) fn execute_epoch_multi(
    vm: &mut Vm,
    state: &mut CircuitState,
    sources: &SourceReads<'_>,
    inputs: impl IntoIterator<Item = (DeltaReg, Batch)>,
) -> Result<Batch, String> {
    let all_empty = seed_inputs(vm, inputs);
    // Spent by any dispatched epoch, not just an empty one: the ground reduce
    // runs either way and leaves V₀ in its output trace.
    let ground_pending = std::mem::take(&mut vm.pending_ground_row);
    if all_empty && !ground_pending {
        return Ok(Batch::empty_with_schema(vm.program.out_schema()));
    }
    run_instructions(vm, state, sources, 0)?;
    run_integrates(vm, state)?;
    Ok(take_output(vm))
}

/// One chunk of a capacity-bounded view's hydration replay, which runs no
/// integrate.
pub(in crate::query) fn replay_chunk(
    vm: &mut Vm,
    state: &mut CircuitState,
    sources: &SourceReads<'_>,
    entry: ReplayEntry,
    seed: Batch,
) -> Result<Batch, String> {
    seed_inputs(vm, std::iter::once((entry.reg, seed)));
    run_instructions(vm, state, sources, entry.pc)?;
    let out = take_output(vm);
    // Frees the seed, which no integrate took, before the next chunk is gathered.
    vm.release();
    Ok(out)
}

/// Clear the registers and move each seed into its own, reporting whether every
/// seed was empty. Split from [`run_instructions`] so only these lines are
/// generic over the seed iterator, not the whole loop below.
fn seed_inputs(vm: &mut Vm, inputs: impl IntoIterator<Item = (DeltaReg, Batch)>) -> bool {
    vm.release();

    let mut all_empty = true;
    for (input_reg, input_batch) in inputs {
        all_empty &= input_batch.is_empty();
        // Rows under the wrong layout would scramble every downstream read
        // silently. An empty batch has no rows to misread, so it is exempt.
        if !input_batch.is_empty() {
            assert_eq!(
                input_batch.schema().num_columns(),
                vm.program.schema_of(input_reg).num_columns(),
                "VM register {} schema/batch column-count mismatch",
                input_reg.0,
            );
        }
        vm.batches[input_reg.at()] = input_batch;
        fold_written(&mut vm.batches, &vm.program.regs, input_reg);
    }
    all_empty
}

/// Fold `reg`'s batch, just written, when its register folds: every reader runs
/// after the write, so each sees the folded form.
#[inline]
fn fold_written(batches: &mut [Batch], regs: &[Reg], reg: DeltaReg) {
    let written = &regs[reg.at()];
    if written.fold {
        batches[reg.at()].consolidate_in_place();
    }
}

/// Register `reg`'s batch, moved out when `take` and copied otherwise.
#[inline]
fn take_or_clone(batches: &mut [Batch], reg: DeltaReg, take: bool) -> Batch {
    let batch = &mut batches[reg.at()];
    match take {
        true => batch.take(),
        false => Batch::clone(batch),
    }
}

/// Run the instruction stream from `start_pc`.
fn run_instructions(
    vm: &mut Vm,
    state: &mut CircuitState,
    sources: &SourceReads<'_>,
    start_pc: usize,
) -> Result<(), String> {
    let Vm { program, batches, .. } = vm;

    gnitz_debug!(
        "vm: dispatch out_reg={} instrs={}",
        program.out_reg.0,
        program.instructions.len()
    );

    let Program { instructions, regs, .. } = program;
    for (pc, instr) in instructions.iter_mut().enumerate().skip(start_pc) {
        let (in_reg, out_reg) = (instr.in_reg, instr.out_reg);
        let takes = |r: DeltaReg| regs[r.at()].last_read == LastRead::Instr(pc);

        let all_empty = instr.reads().into_iter().flatten().all(|r| batches[r.at()].is_empty());
        // The kernel would return exactly this, so skip it and its trace cursor.
        let out = if instr.inert_on_empty && all_empty {
            Batch::empty_with_schema(&regs[out_reg.at()].schema)
        } else {
            match &mut instr.op {
                Op::Filter(pred) => algebra::op_filter(&batches[in_reg.at()], pred)
                    .unwrap_or_else(|| take_or_clone(batches, in_reg, takes(in_reg))),

                Op::Map(plan) => plan.evaluate_map_batch(&batches[in_reg.at()]),

                Op::Negate => take_or_clone(batches, in_reg, takes(in_reg)).negated(),

                Op::Union { in_b } => {
                    if in_reg == *in_b {
                        // Z + Z doubles every weight; delegating would take
                        // operand 0 and add the emptied operand 1 to it.
                        let mut batch = take_or_clone(batches, in_reg, takes(in_reg));
                        batch.map_weights(|w| w.wrapping_mul(2));
                        batch
                    } else if batches[in_reg.at()].is_empty() {
                        // `0 + B = B`, taking B rather than cloning it — which is
                        // why the arm is here and not in `op_union`.
                        take_or_clone(batches, *in_b, takes(*in_b))
                    } else {
                        // The union's own (nullability-merged) schema, not the
                        // left input's — see `union_nullability_merge` for why a
                        // narrower one mis-sorts nulls.
                        algebra::op_union(
                            take_or_clone(batches, in_reg, takes(in_reg)),
                            &batches[in_b.at()],
                            &regs[out_reg.at()].schema,
                        )
                    }
                }

                Op::WeightClamp { hist, kind } => {
                    let delta = &batches[in_reg.at()];
                    let mut cursor = state.cursor_for_keys(*hist, delta);
                    stream::op_weight_clamp(delta, &mut cursor, *kind)
                }

                Op::JoinDT { trace, probe } => {
                    let delta = &batches[in_reg.at()];
                    let mut cursor = match (*trace, probe.probes_delta_keys()) {
                        (Integral::Own(trace), false) => state.cursor(trace),
                        // The compiler bakes a source under an equi probe alone.
                        (integral, _) => sources.cursor_for_keys(state, integral, delta)?,
                    };
                    stream::op_join_delta_trace(delta, &mut cursor, &regs[out_reg.at()].schema, probe)
                }

                Op::WorkerFilter { slot } => algebra::op_worker_filter(&batches[in_reg.at()], *slot),

                Op::NullExtend { nulls_first } => {
                    batches[in_reg.at()].widened_with_nulls(&regs[out_reg.at()].schema, *nulls_first)
                }

                Op::Reduce { out_trace, plan } => {
                    // An empty delta touches no group; only a ground-seeding
                    // reduce reaches here with one.
                    let delta = &batches[in_reg.at()];
                    let mut avi_cursor = match plan.avi_table {
                        Some(idx) if !delta.is_empty() => plan
                            .plan
                            .index_batch(delta)
                            .map(|entries| state.ingest_then_cursor(idx, entries))
                            .transpose()
                            .map_err(|e| ingest_err("avi", idx, e))?,
                        _ => None,
                    };

                    gnitz_debug!(
                        "vm: REDUCE in_count={} avi={}",
                        batches[in_reg.at()].len(),
                        avi_cursor.is_some()
                    );

                    let mut to_cursor = state.cursor(*out_trace);
                    stream::op_reduce(&batches[in_reg.at()], &mut to_cursor, avi_cursor.as_mut(), &plan.plan)
                }

                Op::TopN { out_trace, plan } => {
                    let idx = plan.index_table;
                    let mut history = state
                        .ingest_then_cursor(idx, plan.plan.index_batch(&batches[in_reg.at()]))
                        .map_err(|e| ingest_err("topn index", idx, e))?;
                    let mut to_cursor = state.cursor(*out_trace);
                    stream::op_topn(&batches[in_reg.at()], &mut to_cursor, &mut history, &plan.plan)
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
    let Vm { program, batches, .. } = vm;
    for (i, &(reg, trace)) in program.integrates.iter().enumerate() {
        let take = program.regs[reg.at()].last_read == LastRead::Integrate(i);
        gnitz_debug!("vm: INTEGRATE in_count={}", batches[reg.at()].len());
        // Not `take_or_clone`: for an unconsolidated register its clone arm would cost a
        // clone plus the trace's own consolidate, where moving costs one.
        let res = match take {
            true => state.ingest_owned(trace, batches[reg.at()].take()),
            false => state.ingest_borrowed(trace, &batches[reg.at()]),
        };
        res.map_err(|e| ingest_err("integrate", trace, e))?;
    }
    Ok(())
}

/// Extract the output, labelled with the output register's own schema. An
/// operator's identity path can hand an input batch straight back (see
/// `op_union`), so the label on it may be an operand's; downstream — the
/// exchange mesh, the dag driver — cannot re-derive it.
fn take_output(vm: &mut Vm) -> Batch {
    let Vm { program, batches, .. } = vm;
    let want = program.out_schema();
    let out = &mut batches[program.out_reg.at()];
    if out.is_empty() {
        out.release_buffers();
        return Batch::empty_with_schema(want);
    }
    let mut batch = out.take();
    // Only a narrower nullability may legitimately arrive here. A different
    // physical layout means the batch was built against another schema
    // entirely, which the stamp would hide from the wire encode.
    debug_assert!(
        batch.schema().same_layout(want),
        "VM output register {}: batch label is not the register's physical layout",
        program.out_reg.0,
    );
    batch.set_schema(want);
    batch
}

#[cfg(test)]
#[path = "tests/exec.rs"]
mod tests;
