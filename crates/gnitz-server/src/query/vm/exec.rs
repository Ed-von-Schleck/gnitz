//! Epoch execution: the two entry points and the opcode dispatch loop.

use super::*;
use gnitz_store::ops;
use gnitz_store::storage::{Batch, StorageError};

/// Storage failure while integrating a tick's delta into a child store: the
/// view/history state has diverged from its durable inputs and there is no
/// sound continue (dropping the delta permanently desyncs the integral). The
/// error is returned so the process that owns the recovery decision makes it —
/// for a server, restart + SAL replay. This only logs; every call site applies
/// `?` to what it hands back.
fn tick_ingest_err(op: &str, idx: StateIdx, e: StorageError) -> StorageError {
    gnitz_error!(
        "vm: {} ingest failed (state_idx={:?}): {} — tick state diverged \
         from durable inputs",
        op,
        idx,
        e,
    );
    e
}

/// Execute one epoch over `inputs`, one `(register, batch)` per seeded input —
/// two for a set-op post phase, one everywhere else. Returns the output
/// register's batch, empty when the epoch produced nothing.
pub(in crate::query) fn execute_epoch_multi(
    vm: &mut VmHandle,
    inputs: impl IntoIterator<Item = (DeltaReg, Batch)>,
) -> Result<Batch, StorageError> {
    let all_empty = seed_inputs(vm, inputs);
    // A global-ground reduce mints its V₀ row on one empty epoch; every other
    // opcode is inert on empty input. Cleared before the run, because `trace_out`
    // holds V₀ afterwards either way.
    if all_empty && !std::mem::take(&mut vm.pending_ground_row) {
        return Ok(Batch::empty_with_schema(vm.program.out_schema()));
    }
    vm.state.compact_all();
    run_instructions(vm, 0)?;
    run_integrates(vm)?;
    Ok(take_output(vm))
}

/// One chunk of a capacity-bounded view's hydration replay: seed a register out
/// of a store and run from `start_pc`, past the prologue that seed replaces.
/// Runs no integrate and no compaction; that the instructions write no state
/// either is `reject_state_writers`'s to enforce.
pub(in crate::query) fn replay_chunk(
    vm: &mut VmHandle,
    start_pc: usize,
    seed: (DeltaReg, Batch),
) -> Result<Batch, StorageError> {
    seed_inputs(vm, std::iter::once(seed));
    run_instructions(vm, start_pc)?;
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
        // silently. An empty batch has none, and legitimately carries a foreign
        // schema (a dep_map view with no matching rows), so it is exempt.
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

/// The batch of register `reg`, taken in place when instruction `pc` is its last
/// reader and copied otherwise.
///
/// `#[inline]`: it returns a `Batch` by value, so a call would cost an extra
/// sret move at every site.
#[inline]
fn take_or_clone(batches: &mut [Batch], last_read: &[u32], reg: DeltaReg, pc: usize) -> Batch {
    let batch = &mut batches[reg.at()];
    match last_read[reg.at()] == pc as u32 {
        true => batch.take(),
        false => batch.clone_batch(),
    }
}

/// Run the instruction stream from `start_pc`.
fn run_instructions(vm: &mut VmHandle, start_pc: usize) -> Result<(), StorageError> {
    // Destructured because the three are disjoint fields: that is what lets an
    // operator hold a batch and a cursor (or a table) at once, with no interior
    // mutability and no raw pointer.
    let VmHandle { program, batches, state, .. } = vm;
    let last_read = &program.last_read[..];

    gnitz_debug!(
        "vm: dispatch out_reg={} instrs={}",
        program.out_reg.0,
        program.instructions.len()
    );

    // Indexed, so `pc` is the absolute offset `last_read` is keyed by.
    for pc in start_pc..program.instructions.len() {
        let instr = &program.instructions[pc];
        let (in_reg, out_reg) = (instr.in_reg, instr.out_reg);
        let facts = facts(&instr.op);

        // Fold in place what this instruction is the first reader of: the raw
        // batch is freed and every later reader sees the folded form, where a
        // kernel's own fold would allocate a copy beside it.
        for r in instr.reads().into_iter().flatten() {
            if program.consolidate_at[r.at()] == pc as u32 {
                batches[r.at()].consolidate_in_place(program.schema_of(r));
            }
        }

        // An `inert_on_empty` kernel returns exactly this for an empty input, so
        // skipping it saves opening a cursor over a trace it reads nothing from.
        if facts.inert_on_empty && batches[in_reg.at()].is_empty() {
            batches[out_reg.at()] = Batch::empty_with_schema(program.schema_of(out_reg));
        } else {
            match &instr.op {
                Op::Filter(pred) => {
                    let schema = program.schema_of(in_reg);
                    let result = match ops::op_filter(&batches[in_reg.at()], pred, schema) {
                        Some(kept) => kept,
                        None => take_or_clone(batches, last_read, in_reg, pc),
                    };
                    batches[out_reg.at()] = result;
                }

                Op::Map(plan) => {
                    let result = plan.evaluate_map_batch(&batches[in_reg.at()]);
                    batches[out_reg.at()] = result;
                }

                Op::Negate => {
                    let result = ops::op_negate(take_or_clone(batches, last_read, in_reg, pc));
                    batches[out_reg.at()] = result;
                }

                Op::Union { in_b } => {
                    // The union's own (nullability-merged) schema, not the left
                    // input's — see `op_union` for why a narrower one mis-sorts
                    // nulls.
                    let out_schema = program.schema_of(out_reg);
                    let result = if in_reg == *in_b {
                        // Z + Z doubles every weight. Delegating would take `in_a`
                        // and then add the emptied `in_b` to it, yielding Z.
                        let mut batch = take_or_clone(batches, last_read, in_reg, pc);
                        batch.map_weights(|w| w.wrapping_mul(2));
                        batch
                    } else if batches[in_reg.at()].is_empty() {
                        // `0 + B = B`. Here rather than in `op_union`, which can
                        // only clone B; taking an operand is the VM's decision. The
                        // mirror needs no arm — `op_union` hands `batch_a`, already
                        // taken, straight back.
                        take_or_clone(batches, last_read, *in_b, pc)
                    } else {
                        ops::op_union(
                            take_or_clone(batches, last_read, in_reg, pc),
                            &batches[in_b.at()],
                            out_schema,
                        )
                    };
                    batches[out_reg.at()] = result;
                }

                Op::WeightClamp { hist, preset } => {
                    let schema = program.schema_of(in_reg);
                    let delta = take_or_clone(batches, last_read, in_reg, pc);
                    // Opened before the ingest below: the clamp reads `z⁻¹(I)`.
                    let mut cursor = state.cursor(*hist);
                    let (output, consolidated) = ops::op_weight_clamp(delta, &mut cursor, schema, *preset);
                    drop(cursor);
                    batches[out_reg.at()] = output;
                    state
                        .ingest_owned(*hist, consolidated)
                        .map_err(|e| tick_ingest_err("weight-clamp history", *hist, e))?;
                }

                Op::JoinDT { trace, probe } => {
                    let result = ops::op_join_delta_trace(
                        &batches[in_reg.at()],
                        &mut state.cursor(*trace),
                        program.schema_of(out_reg),
                        *probe,
                    );
                    batches[out_reg.at()] = result;
                }

                Op::WorkerFilter { worker_id, num_workers } => {
                    let result = ops::op_worker_filter(&batches[in_reg.at()], *worker_id, *num_workers);
                    batches[out_reg.at()] = result;
                }

                Op::NullExtend { nulls_first } => {
                    let out_schema = program.schema_of(out_reg);
                    let result = batches[in_reg.at()].widened_with_nulls(out_schema, *nulls_first);
                    batches[out_reg.at()] = result;
                }

                Op::Reduce { out_trace, plan } => {
                    // An empty delta touches no group; only a ground-seeding
                    // reduce reaches here with one.
                    let mut avi_cursor = match plan.avi_table.zip(plan.plan.avi.as_ref()) {
                        Some((idx, bake)) if !batches[in_reg.at()].is_empty() => Some(
                            state
                                .ingest_then_cursor(idx, ops::avi_batch(&batches[in_reg.at()], bake))
                                .map_err(|e| tick_ingest_err("avi", idx, e))?,
                        ),
                        _ => None,
                    };

                    gnitz_debug!(
                        "vm: REDUCE in_count={} avi={} aggs={}",
                        batches[in_reg.at()].len(),
                        avi_cursor.is_some(),
                        plan.plan.shape.acc_template.len()
                    );

                    let mut to_cursor = state.cursor(*out_trace);
                    let raw_out =
                        ops::op_reduce(&batches[in_reg.at()], &mut to_cursor, avi_cursor.as_mut(), &plan.plan);
                    batches[out_reg.at()] = raw_out;
                }

                Op::TopN { out_trace, plan } => {
                    gnitz_debug!("vm: TOPN in_count={}", batches[in_reg.at()].len());
                    let idx = plan.index_table;
                    let mut history = state
                        .ingest_then_cursor(idx, plan.plan.index.batch(&batches[in_reg.at()]))
                        .map_err(|e| tick_ingest_err("topn index", idx, e))?;
                    let mut to_cursor = state.cursor(*out_trace);
                    batches[out_reg.at()] =
                        ops::op_topn(&batches[in_reg.at()], &mut to_cursor, &mut history, &plan.plan);
                }
            }
        }

        // Free every operand this instruction was the last reader of, here
        // rather than per arm — so a register's lifetime follows the dataflow
        // instead of whether some kernel happens to take `Batch` by value.
        // Idempotent: releasing an already-emptied batch returns immediately.
        for r in instr.reads().into_iter().flatten() {
            if last_read[r.at()] == pc as u32 {
                batches[r.at()].release_buffers();
            }
        }
    }

    gnitz_debug!("vm: dispatch done");
    Ok(())
}

/// Accumulate each delta into its trace, after the whole instruction range — so
/// no cursor opened in that range can observe this tick's integration.
fn run_integrates(vm: &mut VmHandle) -> Result<(), StorageError> {
    let VmHandle { program, batches, state, .. } = vm;
    for (i, &(in_reg, trace)) in program.integrates.iter().enumerate() {
        gnitz_debug!("vm: INTEGRATE in_count={}", batches[in_reg.at()].len());
        // Not `take_or_clone`: for a `Raw` register its clone arm would cost a
        // clone plus the trace's own consolidate, where moving costs one.
        let res = match program.last_read[in_reg.at()] == (program.instructions.len() + i) as u32 {
            true => state.ingest_owned(trace, batches[in_reg.at()].take()),
            false => state.ingest_borrowed(trace, &batches[in_reg.at()]),
        };
        res.map_err(|e| tick_ingest_err("integrate", trace, e))?;
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
    // An empty register can still carry a seed's foreign schema, so it is
    // rebuilt rather than taken.
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
