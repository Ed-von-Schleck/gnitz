//! Epoch execution: `execute_epoch` / `execute_epoch_multi` and the opcode
//! dispatch loop — kept whole: boxing per-opcode handlers or splitting the
//! match arms would break monomorphization of the dispatch loop.

use super::*;
use crate::ops;
use crate::storage::{Batch, ReadCursor};

// ---------------------------------------------------------------------------
// Execution
// ---------------------------------------------------------------------------

/// Storage failure while integrating a tick's delta into an owned table: the
/// view/history state has diverged from its durable inputs and there is no
/// sound continue (dropping the delta permanently desyncs the integral). The
/// error is returned so the process that owns the recovery decision makes it —
/// for a server, restart + SAL replay. This only logs; every call site applies
/// `?` to what it hands back.
fn log_tick_ingest_err(
    op: &str,
    table_idx: TableIdx,
    r: Result<(), crate::storage::StorageError>,
) -> Result<(), crate::storage::StorageError> {
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

/// Execute one epoch of a compiled program with a single input register —
/// a test-only convenience over `execute_epoch_multi` (production seeds
/// through the multi entry point).
///
/// The input_batch is moved into `input_reg`.  After execution, the output
/// batch (if any) is extracted from `output_reg` and returned.
///
/// Returns the output batch, or `None` when the epoch produced nothing.
#[cfg(test)]
pub(crate) fn execute_epoch(
    program: &Program,
    regfile: &mut RegisterFile,
    input_batch: Batch,
    input_reg: u16,
    output_reg: u16,
) -> Result<Option<Batch>, crate::storage::StorageError> {
    execute_epoch_multi(program, regfile, std::iter::once((input_reg, input_batch)), output_reg)
}

/// Execute one epoch, seeding several input registers before the dispatch loop.
///
/// Used by the multi-exchange dispatch: a set-op post phase reads one
/// exchange-relayed batch per side (left, right), so more than one register must
/// be loaded after `clear_deltas` wipes the delta registers. Each
/// `(reg, batch)` is moved into place; later seeds win on a duplicate register.
/// Takes an iterator so the single-input `execute_epoch` (the hot per-epoch
/// entry) seeds via `iter::once` with no heap allocation.
pub(crate) fn execute_epoch_multi(
    program: &Program,
    regfile: &mut RegisterFile,
    inputs: impl IntoIterator<Item = (u16, Batch)>,
    output_reg: u16,
) -> Result<Option<Batch>, crate::storage::StorageError> {
    execute_epoch_from(program, regfile, inputs, output_reg, 0, false)
}

/// [`execute_epoch_multi`] with the two knobs a bounded view's per-key hydration
/// replay needs: `start_pc` skips the prologue whose output the caller seeds
/// directly, and `read_only` suppresses every `Instr::Integrate` so a *read*
/// leaves no operator-state write behind. Both stay runtime arguments — a program
/// executes 1-3 `Integrate`s, so monomorphising the whole dispatch loop over the
/// flag would duplicate it to elide a perfectly-predicted branch.
pub(crate) fn execute_epoch_from(
    program: &Program,
    regfile: &mut RegisterFile,
    inputs: impl IntoIterator<Item = (u16, Batch)>,
    output_reg: u16,
    start_pc: usize,
    read_only: bool,
) -> Result<Option<Batch>, crate::storage::StorageError> {
    gnitz_debug!(
        "vm: execute_epoch output_reg={} instrs={}",
        output_reg,
        program.instructions.len()
    );

    // 1. Clear delta batches. Every trace register is backed by one of the
    // plan's owned tables, and `VmHandle::bind_trace_cursors` has already
    // pointed it at a fresh cursor.
    regfile.clear_deltas(&program.reg_meta);

    // 2. Seed input batches
    for (input_reg, input_batch) in inputs {
        if input_batch.count > 0 {
            // Only assert when the batch is non-empty: an empty batch is a
            // structural no-op (every per-row loop short-circuits) and may
            // legitimately carry a schema unrelated to the target register
            // during DAG dispatch (a dep_map view with no matching rows).
            // A mismatched schema with count>0 means we'd run the program against
            // the wrong column layout, silently scrambling every downstream read —
            // fail loudly instead.
            assert_eq!(
                input_batch.schema.num_columns(),
                program.reg_meta[input_reg as usize].schema.num_columns(),
                "VM register {input_reg} schema/batch column-count mismatch",
            );
        }
        regfile.registers[input_reg as usize].batch = input_batch;
    }

    // Raw pointer to the register array.  All instructions access distinct
    // registers (guaranteed by topological sort), so aliased &/&mut access
    // through different indices is safe.
    let regs = regfile.registers.as_mut_ptr();
    let nregs = regfile.registers.len();

    // Helper macros for safe indexed access via raw pointer.
    macro_rules! reg {
        ($i:expr) => {{
            assert!(
                ($i as usize) < nregs,
                "register index {} out of bounds (nregs={})",
                $i,
                nregs
            );
            unsafe { &*regs.add($i as usize) }
        }};
    }
    macro_rules! reg_mut {
        ($i:expr) => {{
            assert!(
                ($i as usize) < nregs,
                "register index {} out of bounds (nregs={})",
                $i,
                nregs
            );
            unsafe { &mut *regs.add($i as usize) }
        }};
    }
    // Take on the register's last read — and also when it is already empty,
    // where `take` leaves an `empty_like` placeholder a later reader cannot tell
    // apart, and `clone_batch` would pop two pooled arenas to copy nothing.
    macro_rules! take_or_clone {
        ($consume:expr, $i:expr) => {
            if $consume || reg!($i).batch.count == 0 {
                reg_mut!($i).batch.take()
            } else {
                reg!($i).batch.clone_batch()
            }
        };
    }
    // Every trace register names its backing table in `reg_meta`, and
    // `bind_trace_cursors` opens a cursor on each one before dispatch, so a
    // null here is a VM bug rather than a state the circuit can reach.
    macro_rules! cursor_mut {
        ($i:expr) => {{
            let r = reg_mut!($i);
            assert!(
                !r.cursor_ptr.is_null(),
                "register {} has no trace cursor; bind_trace_cursors must run before dispatch",
                $i
            );
            unsafe { &mut *r.cursor_ptr }
        }};
    }

    // 3. Dispatch loop
    for instr in &program.instructions[start_pc..] {
        match instr {
            Instr::Halt => break,

            Instr::Filter {
                in_reg,
                out_reg,
                pred_idx,
            } => {
                debug_assert_ne!(*in_reg, *out_reg, "Filter: in_reg and out_reg must be distinct");
                let pred = &program.predicates[pred_idx.at()];
                let schema = &program.reg_meta[*in_reg as usize].schema;
                let result = ops::op_filter(&reg!(*in_reg).batch, pred, schema);
                reg_mut!(*out_reg).batch = result;
            }

            Instr::Map {
                in_reg,
                out_reg,
                map_idx,
            } => {
                debug_assert_ne!(*in_reg, *out_reg, "Map: in_reg and out_reg must be distinct");
                let result = program.maps[map_idx.at()].evaluate_map_batch(&reg!(*in_reg).batch);
                reg_mut!(*out_reg).batch = result;
            }

            Instr::Negate {
                in_reg,
                out_reg,
                consume,
            } => {
                debug_assert_ne!(*in_reg, *out_reg, "Negate: in_reg and out_reg must be distinct");
                let result = ops::op_negate(take_or_clone!(*consume, *in_reg));
                reg_mut!(*out_reg).batch = result;
            }

            Instr::Union {
                in_a,
                in_b,
                out_reg,
                consume_a,
                consume_b,
            } => {
                // The union's own (nullability-merged) schema, not the left
                // input's — see `op_union` for why a narrower one mis-sorts
                // nulls.
                let out_schema = &program.reg_meta[*out_reg as usize].schema;
                if in_a == in_b {
                    // Self-union: Z + Z doubles every weight in place. `op_union`
                    // would clone 2N rows to reach the same answer, and reading
                    // `in_b` after taking `in_a` would see an empty batch. One
                    // register read once, so `consume_slots` returns no slot.
                    let mut batch = reg_mut!(*in_a).batch.take();
                    batch.map_weights(|w| w.wrapping_mul(2));
                    reg_mut!(*out_reg).batch = batch;
                } else {
                    // Both identity arms live here because taking an operand is the
                    // VM's decision, and single-source-per-epoch leaves one side of
                    // a set operation's union empty every epoch.
                    let (a_empty, b_empty) = (reg!(*in_a).batch.count == 0, reg!(*in_b).batch.count == 0);
                    let result = if b_empty {
                        take_or_clone!(*consume_a, *in_a) // A + 0 = A
                    } else if a_empty {
                        take_or_clone!(*consume_b, *in_b) // 0 + B = B
                    } else {
                        ops::op_union(take_or_clone!(*consume_a, *in_a), &reg!(*in_b).batch, out_schema)
                    };
                    reg_mut!(*out_reg).batch = result;
                }
            }

            Instr::WeightClamp {
                in_reg,
                hist_reg,
                out_reg,
                hist_table_idx,
                lo,
                hi,
                consume,
            } => {
                let cursor = cursor_mut!(*hist_reg);
                let schema = &program.reg_meta[*in_reg as usize].schema;
                let delta = take_or_clone!(*consume, *in_reg);
                let (output, consolidated) = ops::op_weight_clamp(delta, cursor, schema, *lo, *hi);
                reg_mut!(*out_reg).batch = output;
                // Ingest consolidated delta into history table
                let res = program.table_mut(*hist_table_idx).ingest_owned_batch(consolidated);
                log_tick_ingest_err("weight-clamp history", *hist_table_idx, res)?;
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
                let cursor = cursor_mut!(*trace_reg);
                let result = ops::op_join_delta_trace(
                    &reg!(*delta_reg).batch,
                    cursor,
                    left_schema,
                    right_schema,
                    out_schema,
                    *probe,
                );
                reg_mut!(*out_reg).batch = result;
            }

            Instr::WorkerFilter {
                in_reg,
                out_reg,
                worker_id,
                num_workers,
            } => {
                let schema = &program.reg_meta[*in_reg as usize].schema;
                let result = ops::op_worker_filter(&reg!(*in_reg).batch, schema, *worker_id, *num_workers);
                reg_mut!(*out_reg).batch = result;
            }

            Instr::NullExtend { in_reg, out_reg } => {
                let in_schema = &program.reg_meta[*in_reg as usize].schema;
                let out_schema = &program.reg_meta[*out_reg as usize].schema;
                let result = reg!(*in_reg).batch.widened_with_null_tail(in_schema, out_schema);
                reg_mut!(*out_reg).batch = result;
            }

            Instr::Integrate { in_reg, table_idx } => {
                if read_only {
                    continue;
                }
                gnitz_debug!("vm: INTEGRATE in_count={}", reg!(*in_reg).batch.count);
                let res = program
                    .table_mut(*table_idx)
                    .ingest_borrowed_batch(&reg!(*in_reg).batch);
                log_tick_ingest_err("integrate", *table_idx, res)?;
            }

            Instr::Reduce {
                in_reg,
                trace_out_reg,
                out_reg,
                plan_idx,
                avi,
            } => {
                let plan = &program.reduce_plans[plan_idx.at()];
                let to_cursor = cursor_mut!(*trace_out_reg);

                // Opened last, so the prefix seek sees this epoch's own entries;
                // compacted first, as `compact_owned_traces` does before any other
                // operator-state read.
                let mut avi_cursor: Option<Box<ReadCursor>> = match avi {
                    Some(idx) => {
                        let bake = plan
                            .avi
                            .as_ref()
                            .expect("a Reduce naming a value-index table is emitted with the bake that keys it");
                        let table = program.table_mut(*idx);
                        let res = ops::op_populate_avi(&reg!(*in_reg).batch, table, bake);
                        log_tick_ingest_err("avi", *idx, res)?;
                        let _ = table.compact_if_needed();
                        Some(Box::new(table.open_cursor()))
                    }
                    None => None,
                };

                gnitz_debug!(
                    "vm: REDUCE in_count={} avi={} aggs={}",
                    reg!(*in_reg).batch.count,
                    avi_cursor.is_some(),
                    plan.acc_template.len()
                );

                let raw_out = ops::op_reduce(&reg!(*in_reg).batch, to_cursor, avi_cursor.as_deref_mut(), plan);

                // Drop temporary cursor handle (returned to pool)
                drop(avi_cursor);

                reg_mut!(*out_reg).batch = raw_out;
            }
        }
    }

    gnitz_debug!("vm: dispatch done");

    // 4. Extract output, labelled with the output register's own schema. An
    // operator's identity path can hand an input batch straight back (see
    // `op_union`), so the label on it may be an operand's; downstream — the
    // exchange wire, `prepare_relay`, `queue_dependents` — cannot re-derive it.
    let out = &mut regfile.registers[output_reg as usize];
    Ok((out.batch.count > 0).then(|| {
        let mut batch = out.batch.take();
        let want = program.reg_meta[output_reg as usize].schema;
        // Only a narrower nullability may legitimately arrive here. A different
        // physical layout means the batch was built against another schema
        // entirely, which the stamp would hide from the wire encode.
        debug_assert!(
            batch.schema.same_physical_layout(&want),
            "VM output register {output_reg}: batch label is not the register's physical layout",
        );
        batch.set_schema(want);
        batch
    }))
}
