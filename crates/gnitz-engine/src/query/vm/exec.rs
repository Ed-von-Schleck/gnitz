//! Epoch execution: `execute_epoch` / `execute_epoch_multi` and the opcode
//! dispatch loop — kept whole: boxing per-opcode handlers or splitting the
//! match arms would break monomorphization of the dispatch loop.

use super::*;
use crate::ops::{self, AviDesc};
use crate::storage::{Batch, ReadCursor};

// ---------------------------------------------------------------------------
// Execution
// ---------------------------------------------------------------------------

/// Storage failure while integrating a tick's delta into an owned table: the
/// view/history state has diverged from its durable inputs and there is no
/// sound continue (dropping the delta permanently desyncs the integral).
/// Recovery is restart + SAL replay.
fn fatal_on_tick_ingest_err(op: &str, table_idx: i32, r: Result<(), crate::storage::StorageError>) {
    if let Err(e) = r {
        crate::gnitz_fatal_abort!(
            "vm: {} ingest failed (table_idx={}): {} — tick state diverged \
             from durable inputs; aborting for restart+SAL replay",
            op,
            table_idx,
            e,
        );
    }
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
) -> Option<Batch> {
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
) -> Option<Batch> {
    gnitz_debug!(
        "vm: execute_epoch output_reg={} instrs={}",
        output_reg,
        program.instructions.len()
    );

    // 1. Clear delta batches. Every trace register is backed by one of the
    // plan's owned tables, and `VmHandle::refresh_owned_cursors` has already
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
            if let Some(ref s) = input_batch.schema {
                assert_eq!(
                    s.num_columns(),
                    program.reg_meta[input_reg as usize].schema.num_columns(),
                    "VM register {input_reg} schema/batch column-count mismatch",
                );
            }
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
    // Every trace register names its backing table in `reg_meta`, and
    // `refresh_owned_cursors` opens a cursor on each one before dispatch, so a
    // null here is a VM bug rather than a state the circuit can reach.
    macro_rules! cursor_mut {
        ($i:expr) => {{
            let r = reg_mut!($i);
            assert!(
                !r.cursor_ptr.is_null(),
                "register {} has no trace cursor; refresh_owned_cursors must run before dispatch",
                $i
            );
            unsafe { &mut *r.cursor_ptr }
        }};
    }

    // 3. Dispatch loop
    for instr in &program.instructions {
        match instr {
            Instr::Halt => break,

            Instr::Filter {
                in_reg,
                out_reg,
                func_idx,
            } => {
                debug_assert_ne!(*in_reg, *out_reg, "Filter: in_reg and out_reg must be distinct");
                let func_ptr = program.funcs[*func_idx as usize];
                // A predicate-less Filter (no WHERE clause) is elided at emit
                // time by register aliasing; every emitted Filter carries a func.
                debug_assert!(!func_ptr.is_null(), "Filter: null predicate must be elided at emit");
                let func = unsafe { &*func_ptr };
                let schema = &program.reg_meta[*in_reg as usize].schema;
                let result = ops::op_filter(&reg!(*in_reg).batch, func, schema);
                reg_mut!(*out_reg).batch = result;
            }

            Instr::Map {
                in_reg,
                out_reg,
                func_idx,
                reindex,
            } => {
                debug_assert_ne!(*in_reg, *out_reg, "Map: in_reg and out_reg must be distinct");
                let func_ptr = program.funcs[*func_idx as usize];
                // Identity MAPs are elided at emit time by register aliasing;
                // every emitted Map carries a func.
                debug_assert!(!func_ptr.is_null(), "Map: identity map must be elided at emit");
                let func = unsafe { &*func_ptr };
                let in_schema = &program.reg_meta[*in_reg as usize].schema;
                let reindex = match *reindex {
                    ReindexOperand::None => ops::ReindexSpec::None,
                    ReindexOperand::HashRow { branch_id } => ops::ReindexSpec::HashRow { branch_id },
                    ReindexOperand::Pack { off, cnt } => {
                        let (off, cnt) = (off as usize, cnt as usize);
                        ops::ReindexSpec::Pack {
                            cols: &program.reindex_cols[off..off + cnt],
                            target_tcs: &program.reindex_target_tcs[off..off + cnt],
                        }
                    }
                };
                let result = ops::op_map(&reg!(*in_reg).batch, func, in_schema, reindex);
                reg_mut!(*out_reg).batch = result;
            }

            Instr::Negate { in_reg, out_reg } => {
                debug_assert_ne!(*in_reg, *out_reg, "Negate: in_reg and out_reg must be distinct");
                let result = ops::op_negate(&reg!(*in_reg).batch);
                reg_mut!(*out_reg).batch = result;
            }

            Instr::Union { in_a, in_b, out_reg } => {
                let schema = &program.reg_meta[*in_a as usize].schema;
                if in_a == in_b {
                    // Self-union: Z + Z doubles every weight in-place. Reading
                    // batch_b after moving batch_a out would see an empty batch
                    // and produce +1 instead of +2.
                    let mut batch = reg_mut!(*in_a).batch.take();
                    batch.map_weights(|w| w.wrapping_mul(2));
                    reg_mut!(*out_reg).batch = batch;
                } else {
                    let batch_b = &reg!(*in_b).batch;
                    let batch_a = reg_mut!(*in_a).batch.take();
                    reg_mut!(*out_reg).batch = ops::op_union(batch_a, batch_b, schema);
                }
            }

            Instr::WeightClamp {
                in_reg,
                hist_reg,
                out_reg,
                hist_table_idx,
                lo,
                hi,
            } => {
                let cursor = cursor_mut!(*hist_reg);
                let schema = &program.reg_meta[*in_reg as usize].schema;
                let delta = reg_mut!(*in_reg).batch.take();
                let (output, consolidated) = ops::op_weight_clamp(delta, cursor, schema, *lo, *hi);
                reg_mut!(*out_reg).batch = output;
                // Ingest consolidated delta into history table
                let ptr = program.tables[*hist_table_idx as usize];
                let table = unsafe { &mut *ptr };
                let res = table.ingest_owned_batch(consolidated);
                fatal_on_tick_ingest_err("weight-clamp history", *hist_table_idx as i32, res);
            }

            Instr::JoinDT {
                delta_reg,
                trace_reg,
                out_reg,
            } => {
                let left_schema = &program.reg_meta[*delta_reg as usize].schema;
                let right_schema = &program.reg_meta[*trace_reg as usize].schema;
                let out_schema = &program.reg_meta[*out_reg as usize].schema;
                let cursor = cursor_mut!(*trace_reg);
                let result =
                    ops::op_join_delta_trace(&reg!(*delta_reg).batch, cursor, left_schema, right_schema, out_schema);
                reg_mut!(*out_reg).batch = result;
            }

            Instr::JoinDTRange {
                delta_reg,
                trace_reg,
                out_reg,
                n_eq,
                rel,
            } => {
                let left_schema = &program.reg_meta[*delta_reg as usize].schema;
                let right_schema = &program.reg_meta[*trace_reg as usize].schema;
                let out_schema = &program.reg_meta[*out_reg as usize].schema;
                let cursor = cursor_mut!(*trace_reg);
                let result = ops::op_join_delta_trace_range(
                    &reg!(*delta_reg).batch,
                    cursor,
                    left_schema,
                    right_schema,
                    out_schema,
                    *n_eq as usize,
                    *rel,
                );
                reg_mut!(*out_reg).batch = result;
            }

            Instr::PartitionFilter {
                in_reg,
                out_reg,
                worker_id,
                num_workers,
            } => {
                let schema = &program.reg_meta[*in_reg as usize].schema;
                let result = ops::op_partition_filter(&reg!(*in_reg).batch, schema, *worker_id, *num_workers);
                reg_mut!(*out_reg).batch = result;
            }

            Instr::NullExtend { in_reg, out_reg } => {
                let in_schema = &program.reg_meta[*in_reg as usize].schema;
                let out_schema = &program.reg_meta[*out_reg as usize].schema;
                let result = ops::op_null_extend(&reg!(*in_reg).batch, in_schema, out_schema);
                reg_mut!(*out_reg).batch = result;
            }

            Instr::Integrate { in_reg, table_idx, avi } => {
                let target_ptr = if *table_idx >= 0 {
                    program.tables[*table_idx as usize]
                } else {
                    std::ptr::null_mut()
                };
                let target = if !target_ptr.is_null() {
                    Some(unsafe { &mut *target_ptr })
                } else {
                    None
                };

                let avi_desc = avi.as_ref().map(|a| AviDesc {
                    table: program.tables[a.table_idx as usize],
                    bake: &program.avi_bakes[a.bake_idx as usize],
                });

                gnitz_debug!(
                    "vm: INTEGRATE in_count={} target={} avi={}",
                    reg!(*in_reg).batch.count,
                    target.is_some(),
                    avi_desc.is_some()
                );
                let res = ops::op_integrate_with_indexes(&reg!(*in_reg).batch, target, avi_desc.as_ref());
                fatal_on_tick_ingest_err("integrate", *table_idx, res);
            }

            Instr::Reduce {
                in_reg,
                trace_in_reg,
                trace_out_reg,
                out_reg,
                plan_idx,
                avi_table_idx,
            } => {
                let plan = &program.reduce_plans[*plan_idx as usize];

                // trace_in cursor (from register file) — present only for a
                // non-linear aggregate, which needs the input history to
                // recompute an extreme on retraction.
                let ti_opt: Option<&mut ReadCursor> = match trace_in_reg {
                    Some(tr) => Some(cursor_mut!(*tr)),
                    None => None,
                };
                let to_cursor = cursor_mut!(*trace_out_reg);

                // Combined AVI cursor — created fresh from the value-index table
                // (not a register). Must be created AFTER INTEGRATE populates the
                // table, so the prefix seek returns the post-delta extreme.
                // Operator-state read; compact first (see refresh_owned_cursors).
                let mut avi_cursor_handle: Option<Box<ReadCursor>> = if let Some(idx) = avi_table_idx {
                    let avi_ptr = program.tables[*idx as usize];
                    let avi_table = unsafe { &mut *avi_ptr };
                    let _ = avi_table.compact_if_needed();
                    Some(Box::new(avi_table.open_cursor()))
                } else {
                    None
                };

                gnitz_debug!(
                    "vm: REDUCE in_count={} trace_in={} avi={} aggs={}",
                    reg!(*in_reg).batch.count,
                    ti_opt.is_some(),
                    avi_cursor_handle.is_some(),
                    plan.agg_descs.len()
                );

                let avi_opt: Option<&mut ReadCursor> = avi_cursor_handle.as_deref_mut();
                let raw_out = ops::op_reduce(&reg!(*in_reg).batch, ti_opt, to_cursor, avi_opt, plan);

                // Drop temporary cursor handle (returned to pool)
                drop(avi_cursor_handle);

                reg_mut!(*out_reg).batch = raw_out;
            }
        }
    }

    gnitz_debug!("vm: dispatch done");

    // 4. Extract output
    let out = &mut regfile.registers[output_reg as usize];
    (out.batch.count > 0).then(|| out.batch.take())
}

#[cfg(test)]
mod fail_stop_tests {
    use super::fatal_on_tick_ingest_err;
    use crate::storage::StorageError;

    // A storage error while integrating a tick delta must _exit(134).
    #[test]
    fn test_fatal_on_tick_ingest_err_exit_status() {
        crate::test_support::assert_test_aborts_134("fatal_on_tick_ingest_err_internal", &[]);
    }

    // Runs only in the re-exec'd abort child. A returning call would fail the
    // `unreachable!`.
    #[test]
    fn fatal_on_tick_ingest_err_internal() {
        if !crate::test_support::in_abort_child() {
            return;
        }
        fatal_on_tick_ingest_err("integrate", 7, Err(StorageError::Io));
        unreachable!("fatal_on_tick_ingest_err must not return on Err");
    }
}
