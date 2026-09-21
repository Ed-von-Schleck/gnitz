//! Epoch execution: the two entry points and the opcode dispatch loop.

use super::*;
use gnitz_store::ops;
use gnitz_store::storage::{Batch, StorageError, StoreError};

/// The one context template for a `vm:` ingest fault.
#[cold]
fn ingest_err(op: &str, idx: StateIdx, e: StorageError) -> StoreError {
    StoreError::storage(format!("vm: {op} ingest (state_idx={idx:?})"), e)
}

/// Execute one epoch over `inputs`, one `(register, batch)` per seeded input —
/// two for a set-op post phase, one everywhere else. Returns the output
/// register's batch, empty when the epoch produced nothing.
pub(in crate::query) fn execute_epoch_multi(
    vm: &mut VmHandle,
    state: &mut CircuitState,
    inputs: impl IntoIterator<Item = (DeltaReg, Batch)>,
) -> Result<Batch, StoreError> {
    let all_empty = seed_inputs(vm, inputs);
    // Spent by any dispatched epoch, not just an empty one: the ground reduce
    // runs either way and leaves V₀ in its output trace.
    let ground_pending = std::mem::take(&mut vm.pending_ground_row);
    if all_empty && !ground_pending {
        return Ok(Batch::empty_with_schema(vm.program.out_schema()));
    }
    run_instructions(vm, state, 0)?;
    run_integrates(vm, state)?;
    Ok(take_output(vm))
}

/// One chunk of a capacity-bounded view's hydration replay: seed a register out
/// of a store and run from `start_pc`, past the prologue that seed replaces.
/// Runs no integrate; that the instructions write no state either is
/// `reject_state_writers`'s to enforce.
pub(in crate::query) fn replay_chunk(
    vm: &mut VmHandle,
    state: &mut CircuitState,
    start_pc: usize,
    seed: (DeltaReg, Batch),
) -> Result<Batch, StoreError> {
    seed_inputs(vm, std::iter::once(seed));
    run_instructions(vm, state, start_pc)?;
    Ok(take_output(vm))
}

/// Clear the registers and move each seed into its own, reporting whether every
/// seed was empty. Split from [`run_instructions`] so only these lines are
/// generic over the seed iterator, not the whole loop below.
fn seed_inputs(vm: &mut VmHandle, inputs: impl IntoIterator<Item = (DeltaReg, Batch)>) -> bool {
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
    }
    all_empty
}

/// Register `reg`'s batch, moved out when `take` and copied otherwise.
///
/// `#[inline]`: it returns a `Batch` by value, so a call would cost an extra
/// sret move at every site.
#[inline]
fn take_or_clone(batches: &mut [Batch], reg: DeltaReg, take: bool) -> Batch {
    let batch = &mut batches[reg.at()];
    match take {
        true => batch.take(),
        false => batch.clone_batch(),
    }
}

/// Run the instruction stream from `start_pc`.
fn run_instructions(vm: &mut VmHandle, state: &mut CircuitState, start_pc: usize) -> Result<(), StoreError> {
    let VmHandle { program, batches, .. } = vm;

    gnitz_debug!(
        "vm: dispatch out_reg={} instrs={}",
        program.out_reg.0,
        program.instructions.len()
    );

    for instr in &program.instructions[start_pc..] {
        let (in_reg, out_reg) = (instr.in_reg, instr.out_reg);

        // Fold in place what this instruction is the first reader of: the raw
        // batch is freed and every later reader sees the folded form, where a
        // kernel's own fold would allocate a copy beside it.
        for (i, r) in instr.operands() {
            if instr.folds[i] {
                batches[r.at()].consolidate_in_place(program.schema_of(r));
            }
        }

        let all_empty = instr.operands().all(|(_, r)| batches[r.at()].is_empty());
        // The kernel would return exactly this, so skip it and its trace cursor.
        let out = if instr.inert_on_empty && all_empty {
            Batch::empty_with_schema(program.schema_of(out_reg))
        } else {
            match &instr.op {
                Op::Filter(pred) => {
                    let schema = program.schema_of(in_reg);
                    match ops::op_filter(&batches[in_reg.at()], pred, schema) {
                        Some(kept) => kept,
                        None => take_or_clone(batches, in_reg, instr.takes[0]),
                    }
                }

                Op::Map(plan) => plan.evaluate_map_batch(&batches[in_reg.at()]),

                Op::Negate => ops::op_negate(take_or_clone(batches, in_reg, instr.takes[0])),

                Op::Union { in_b } => {
                    if in_reg == *in_b {
                        // Z + Z doubles every weight; delegating would take
                        // operand 0 and add the emptied operand 1 to it. Only a
                        // hand-built circuit aliases them.
                        let mut batch = take_or_clone(batches, in_reg, instr.takes[0]);
                        batch.map_weights(|w| w.wrapping_mul(2));
                        batch
                    } else if batches[in_reg.at()].is_empty() {
                        // `0 + B = B`, taking B rather than cloning it — which is
                        // why the arm is here and not in `op_union`.
                        take_or_clone(batches, *in_b, instr.takes[1])
                    } else {
                        // The union's own (nullability-merged) schema, not the
                        // left input's — see `op_union` for why a narrower one
                        // mis-sorts nulls.
                        ops::op_union(
                            take_or_clone(batches, in_reg, instr.takes[0]),
                            &batches[in_b.at()],
                            program.schema_of(out_reg),
                        )
                    }
                }

                Op::WeightClamp { hist, preset } => {
                    let schema = program.schema_of(in_reg);
                    let delta = take_or_clone(batches, in_reg, instr.takes[0]);
                    // Opened before the ingest below: the clamp reads `z⁻¹(I)`.
                    let mut cursor = state.cursor(*hist);
                    let (output, consolidated) = ops::op_weight_clamp(delta, &mut cursor, schema, *preset);
                    drop(cursor);
                    state
                        .ingest_owned(*hist, consolidated)
                        .map_err(|e| ingest_err("weight-clamp history", *hist, e))?;
                    output
                }

                Op::JoinDT { trace, probe } => ops::op_join_delta_trace(
                    &batches[in_reg.at()],
                    &mut state.cursor(*trace),
                    program.schema_of(out_reg),
                    *probe,
                ),

                Op::WorkerFilter { worker_id, num_workers } => {
                    ops::op_worker_filter(&batches[in_reg.at()], *worker_id, *num_workers)
                }

                Op::NullExtend { nulls_first } => {
                    batches[in_reg.at()].widened_with_nulls(program.schema_of(out_reg), *nulls_first)
                }

                Op::Reduce { out_trace, plan } => {
                    // An empty delta touches no group; only a ground-seeding
                    // reduce reaches here with one.
                    let mut avi_cursor = match plan.avi() {
                        Some((idx, bake)) if !batches[in_reg.at()].is_empty() => Some(
                            state
                                .ingest_then_cursor(idx, ops::avi_batch(&batches[in_reg.at()], bake))
                                .map_err(|e| ingest_err("avi", idx, e))?,
                        ),
                        _ => None,
                    };

                    gnitz_debug!(
                        "vm: REDUCE in_count={} avi={}",
                        batches[in_reg.at()].len(),
                        avi_cursor.is_some()
                    );

                    let mut to_cursor = state.cursor(*out_trace);
                    ops::op_reduce(&batches[in_reg.at()], &mut to_cursor, avi_cursor.as_mut(), &plan.plan)
                }

                Op::TopN { out_trace, plan } => {
                    let idx = plan.index_table;
                    let mut history = state
                        .ingest_then_cursor(idx, plan.plan.index.batch(&batches[in_reg.at()]))
                        .map_err(|e| ingest_err("topn index", idx, e))?;
                    let mut to_cursor = state.cursor(*out_trace);
                    ops::op_topn(&batches[in_reg.at()], &mut to_cursor, &mut history, &plan.plan)
                }
            }
        };
        batches[out_reg.at()] = out;

        // Free every operand this instruction was the last reader of, here
        // rather than per arm — so a register's lifetime follows the dataflow
        // instead of whether some kernel happens to take `Batch` by value.
        for (i, r) in instr.operands() {
            if instr.takes[i] {
                batches[r.at()].release_buffers();
            }
        }
    }

    gnitz_debug!("vm: dispatch done");
    Ok(())
}

/// Accumulate each delta into its trace, after the whole instruction range — so
/// no cursor opened in that range can observe this tick's integration.
fn run_integrates(vm: &mut VmHandle, state: &mut CircuitState) -> Result<(), StoreError> {
    let VmHandle { program, batches, .. } = vm;
    for &Integrate { reg, trace, take } in &program.integrates {
        gnitz_debug!("vm: INTEGRATE in_count={}", batches[reg.at()].len());
        // Not `take_or_clone`: for a `Raw` register its clone arm would cost a
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
/// exchange wire, `prepare_relay`, the dag driver — cannot re-derive it.
fn take_output(vm: &mut VmHandle) -> Batch {
    let VmHandle { program, batches, .. } = vm;
    let want = program.out_schema();
    let out = &mut batches[program.out_reg.at()];
    if out.is_empty() {
        return Batch::empty_with_schema(want);
    }
    let mut batch = out.take();
    // Only a narrower nullability may legitimately arrive here. A different
    // physical layout means the batch was built against another schema
    // entirely, which the stamp would hide from the wire encode.
    debug_assert!(
        batch.schema().same_physical_layout(want),
        "VM output register {}: batch label is not the register's physical layout",
        program.out_reg.0,
    );
    batch.set_schema(want);
    batch
}

#[cfg(test)]
#[path = "tests/exec.rs"]
mod tests;
