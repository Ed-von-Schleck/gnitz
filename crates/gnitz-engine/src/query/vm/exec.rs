//! Epoch execution: the two entry points and the opcode dispatch loop.

use super::*;
use gnitz_store::ops;
use gnitz_store::storage::{Batch, ReadCursor, StorageError};

// ---------------------------------------------------------------------------
// Execution
// ---------------------------------------------------------------------------

/// Storage failure while integrating a tick's delta into an owned table: the
/// view/history state has diverged from its durable inputs and there is no
/// sound continue (dropping the delta permanently desyncs the integral). The
/// error is returned so the process that owns the recovery decision makes it —
/// for a server, restart + SAL replay. This only logs; every call site applies
/// `?` to what it hands back.
fn log_tick_ingest_err(op: &str, table_idx: TableIdx, r: Result<(), StorageError>) -> Result<(), StorageError> {
    r.inspect_err(|e| {
        gnitz_error!(
            "vm: {} ingest failed (table_idx={}): {} — tick state diverged \
             from durable inputs",
            op,
            table_idx.0,
            e,
        );
    })
}

/// Execute one epoch over `inputs`, one `(register, batch)` per seeded input —
/// two for a set-op post phase, one everywhere else. Returns the output
/// register's batch, or `None` when the epoch produced nothing.
///
/// Seeds, compacts and binds its own cursors, so a caller hands over batches and
/// gets a delta back with no prologue of its own to order.
pub(crate) fn execute_epoch_multi(
    vm: &mut VmHandle,
    inputs: impl IntoIterator<Item = (u16, Batch)>,
) -> Result<Option<Batch>, StorageError> {
    let all_empty = seed_inputs(vm, inputs);
    // A global-ground reduce mints its V₀ row on one empty epoch; every other
    // opcode is inert on empty input. Cleared before the run, because `trace_out`
    // holds V₀ afterwards either way.
    if all_empty && !std::mem::take(&mut vm.pending_ground_row) {
        return Ok(None);
    }
    vm.compact_owned_traces();
    vm.bind_trace_cursors();
    dispatch(vm, 0, IntegrateMode::Write)
}

/// One capacity-bounded view's hydration pass. [`Replay::start`] binds the trace
/// cursors once, so "bound exactly once for the pass" is a property of the type
/// rather than of the caller's statement order.
pub(crate) struct Replay<'a> {
    vm: &'a mut VmHandle,
    start_pc: usize,
}

impl<'a> Replay<'a> {
    /// Cursors only — no `compact_owned_traces`, because a read must not mutate
    /// shard state.
    pub(crate) fn start(vm: &'a mut VmHandle, start_pc: usize) -> Self {
        vm.bind_trace_cursors();
        Replay { vm, start_pc }
    }

    /// Seed one register out of a store and dispatch from `start_pc`, past the
    /// prologue that seed replaces. Read-only-ness is `reject_state_writers`'s to
    /// enforce; the one writer it admits is handled by [`IntegrateMode::Skip`].
    pub(crate) fn chunk(&mut self, seed: (u16, Batch)) -> Result<Option<Batch>, StorageError> {
        seed_inputs(self.vm, std::iter::once(seed));
        dispatch(self.vm, self.start_pc, IntegrateMode::Skip)
    }
}

/// Whether the dispatch runs `Instr::Integrate`. A hydration replay skips it: it
/// seeds a register straight out of a trace, and integrating would write those
/// rows back into the trace they came from.
#[derive(PartialEq, Eq)]
enum IntegrateMode {
    Write,
    Skip,
}

/// Clear the registers and move each seed into its own, reporting whether every
/// seed was empty. Split from [`dispatch`] so only these lines are generic over
/// the seed iterator, not the whole loop below.
fn seed_inputs(vm: &mut VmHandle, inputs: impl IntoIterator<Item = (u16, Batch)>) -> bool {
    vm.regfile.clear();

    let mut all_empty = true;
    for (input_reg, input_batch) in inputs {
        all_empty &= input_batch.count == 0;
        // Rows under the wrong layout would scramble every downstream read
        // silently. An empty batch has none, and legitimately carries a foreign
        // schema (a dep_map view with no matching rows), so it is exempt.
        if input_batch.count > 0 {
            assert_eq!(
                input_batch.schema.num_columns(),
                vm.program.reg_meta[input_reg as usize].schema.num_columns(),
                "VM register {input_reg} schema/batch column-count mismatch",
            );
        }
        vm.regfile.batches[input_reg as usize] = input_batch;
    }
    all_empty
}

/// The batch of register `reg`, taken in place when instruction `pc` is its last
/// reader and copied otherwise.
///
/// `#[inline]`: it returns a 1 KiB `Batch` by value, so a call would cost an
/// extra sret move at every site.
#[inline]
fn take_or_clone(batches: &mut [Batch], last_read: &[u32], reg: u16, pc: usize) -> Batch {
    let batch = &mut batches[reg as usize];
    match last_read[reg as usize] == pc as u32 {
        true => batch.take(),
        false => batch.clone_batch(),
    }
}

/// Run the instruction stream from `start_pc` and extract the output register.
fn dispatch(vm: &mut VmHandle, start_pc: usize, integrate: IntegrateMode) -> Result<Option<Batch>, StorageError> {
    // Destructured because the three are disjoint fields: that is what lets an
    // operator hold a batch and a cursor (or a table) at once, with no interior
    // mutability and no raw pointer.
    let VmHandle {
        program,
        regfile,
        tables,
        ..
    } = vm;
    let RegisterFile { batches, cursors } = regfile;
    let last_read = &program.last_read[..];

    gnitz_debug!(
        "vm: dispatch out_reg={} instrs={}",
        program.out_reg,
        program.instructions.len()
    );

    // Indexed, so `pc` is the absolute offset `last_read` is keyed by.
    for pc in start_pc..program.instructions.len() {
        match &program.instructions[pc] {
            Instr::Filter {
                in_reg,
                out_reg,
                pred_idx,
            } => {
                let pred = &program.predicates[pred_idx.at()];
                let schema = &program.reg_meta[*in_reg as usize].schema;
                let result = ops::op_filter(&batches[*in_reg as usize], pred, schema);
                batches[*out_reg as usize] = result;
            }

            Instr::Map {
                in_reg,
                out_reg,
                map_idx,
            } => {
                let result = program.maps[map_idx.at()].evaluate_map_batch(&batches[*in_reg as usize]);
                batches[*out_reg as usize] = result;
            }

            Instr::Negate { in_reg, out_reg } => {
                let result = ops::op_negate(take_or_clone(batches, last_read, *in_reg, pc));
                batches[*out_reg as usize] = result;
            }

            Instr::Union { in_a, in_b, out_reg } => {
                // The union's own (nullability-merged) schema, not the left
                // input's — see `op_union` for why a narrower one mis-sorts
                // nulls.
                let out_schema = &program.reg_meta[*out_reg as usize].schema;
                let result = if in_a == in_b {
                    // Z + Z doubles every weight. Delegating would take `in_a` and
                    // then add the emptied `in_b` to it, yielding Z.
                    let mut batch = take_or_clone(batches, last_read, *in_a, pc);
                    batch.map_weights(|w| w.wrapping_mul(2));
                    batch
                } else if batches[*in_a as usize].count == 0 {
                    // `0 + B = B`. Here rather than in `op_union`, which can only
                    // clone B; taking an operand is the VM's decision. The mirror
                    // needs no arm — `op_union` hands `batch_a`, already taken,
                    // straight back.
                    take_or_clone(batches, last_read, *in_b, pc)
                } else {
                    ops::op_union(
                        take_or_clone(batches, last_read, *in_a, pc),
                        &batches[*in_b as usize],
                        out_schema,
                    )
                };
                batches[*out_reg as usize] = result;
            }

            Instr::WeightClamp {
                in_reg,
                hist_reg,
                out_reg,
                lo,
                hi,
            } => {
                let cursor = bound_cursor(cursors, *hist_reg);
                let schema = &program.reg_meta[*in_reg as usize].schema;
                let delta = take_or_clone(batches, last_read, *in_reg, pc);
                let (output, consolidated) = ops::op_weight_clamp(delta, cursor, schema, *lo, *hi);
                batches[*out_reg as usize] = output;
                // Ingest consolidated delta into history table
                let hist_table = program.trace_table_idx(*hist_reg);
                let res = tables[hist_table.at()].ingest_owned_batch(consolidated);
                log_tick_ingest_err("weight-clamp history", hist_table, res)?;
            }

            Instr::JoinDT {
                delta_reg,
                trace_reg,
                out_reg,
                probe,
            } => {
                let left_schema = &program.reg_meta[*delta_reg as usize].schema;
                let right_schema = &program.reg_meta[*trace_reg as usize].schema;
                let out_schema = &program.reg_meta[*out_reg as usize].schema;
                let result = ops::op_join_delta_trace(
                    &batches[*delta_reg as usize],
                    bound_cursor(cursors, *trace_reg),
                    left_schema,
                    right_schema,
                    out_schema,
                    *probe,
                );
                batches[*out_reg as usize] = result;
            }

            Instr::WorkerFilter {
                in_reg,
                out_reg,
                worker_id,
                num_workers,
            } => {
                let schema = &program.reg_meta[*in_reg as usize].schema;
                let result = ops::op_worker_filter(&batches[*in_reg as usize], schema, *worker_id, *num_workers);
                batches[*out_reg as usize] = result;
            }

            Instr::NullExtend { in_reg, out_reg } => {
                let in_schema = &program.reg_meta[*in_reg as usize].schema;
                let out_schema = &program.reg_meta[*out_reg as usize].schema;
                let result = batches[*in_reg as usize].widened_with_null_tail(in_schema, out_schema);
                batches[*out_reg as usize] = result;
            }

            Instr::Integrate { in_reg, trace_reg } => {
                if integrate == IntegrateMode::Skip {
                    continue;
                }
                gnitz_debug!("vm: INTEGRATE in_count={}", batches[*in_reg as usize].count);
                let trace_table = program.trace_table_idx(*trace_reg);
                let res = tables[trace_table.at()].ingest_borrowed_batch(&batches[*in_reg as usize]);
                log_tick_ingest_err("integrate", trace_table, res)?;
            }

            Instr::Reduce {
                in_reg,
                trace_out_reg,
                out_reg,
                plan_idx,
            } => {
                let baked = &program.reduce_plans[plan_idx.at()];
                let to_cursor = bound_cursor(cursors, *trace_out_reg);

                // Opened last, so the prefix seek sees this epoch's own entries;
                // compacted first, as `compact_owned_traces` does before any other
                // operator-state read.
                let mut avi_cursor = match baked.avi_table.zip(baked.plan.avi.as_ref()) {
                    Some((idx, bake)) => {
                        let table = &mut tables[idx.at()];
                        let res = ops::op_populate_avi(&batches[*in_reg as usize], table, bake);
                        log_tick_ingest_err("avi", idx, res)?;
                        let _ = table.compact_if_needed();
                        Some(table.open_cursor())
                    }
                    None => None,
                };

                gnitz_debug!(
                    "vm: REDUCE in_count={} avi={} aggs={}",
                    batches[*in_reg as usize].count,
                    avi_cursor.is_some(),
                    baked.plan.acc_template.len()
                );

                let raw_out = ops::op_reduce(&batches[*in_reg as usize], to_cursor, avi_cursor.as_mut(), &baked.plan);
                batches[*out_reg as usize] = raw_out;
            }
        }
    }

    gnitz_debug!("vm: dispatch done");

    // Extract the output, labelled with the output register's own schema. An
    // operator's identity path can hand an input batch straight back (see
    // `op_union`), so the label on it may be an operand's; downstream — the
    // exchange wire, `prepare_relay`, the dag driver's fan-out — cannot re-derive it.
    let out = &mut batches[program.out_reg as usize];
    Ok((out.count > 0).then(|| {
        let mut batch = out.take();
        let want = program.out_schema();
        // Only a narrower nullability may legitimately arrive here. A different
        // physical layout means the batch was built against another schema
        // entirely, which the stamp would hide from the wire encode.
        debug_assert!(
            batch.schema.same_physical_layout(&want),
            "VM output register {}: batch label is not the register's physical layout",
            program.out_reg,
        );
        batch.set_schema(want);
        batch
    }))
}

/// The cursor bound to trace register `reg`. Every path into [`dispatch`] binds
/// first, and `build_plan` rejects a trace port naming a register with no owned
/// table — so a missing cursor is a VM bug, not a state a circuit can reach.
#[inline]
fn bound_cursor(cursors: &mut [Option<Box<ReadCursor>>], reg: u16) -> &mut ReadCursor {
    cursors[reg as usize]
        .as_deref_mut()
        .expect("every dispatch binds its trace cursors first")
}

#[cfg(test)]
#[path = "tests/exec.rs"]
mod tests;
