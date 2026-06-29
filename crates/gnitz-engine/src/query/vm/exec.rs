//! Epoch execution: `execute_epoch` / `execute_epoch_multi` and the opcode
//! dispatch loop — kept whole (do-not-touch §8 cluster 4).

use super::*;
use crate::ops::{self, AviDesc, GiDesc};
use crate::storage::ReadCursor;
use crate::storage::{Batch, CursorHandle};

// ---------------------------------------------------------------------------
// Execution
// ---------------------------------------------------------------------------

/// Execute one epoch of a compiled program.
///
/// The input_batch is moved into `input_reg`.  After execution, the output
/// batch (if any) is extracted from `output_reg` and returned.
///
/// Returns `Ok(Some(batch))` if output was produced, `Ok(None)` if empty,
/// or `Err(rc)` on error.
pub(crate) fn execute_epoch(
    program: &Program,
    regfile: &mut RegisterFile,
    input_batch: Batch,
    input_reg: u16,
    output_reg: u16,
    cursor_handles: &[*mut libc::c_void],
    owned_trace_reg_ids: &[(u16, usize)],
) -> Result<Option<Batch>, i32> {
    execute_epoch_multi(
        program,
        regfile,
        std::iter::once((input_reg, input_batch)),
        output_reg,
        cursor_handles,
        owned_trace_reg_ids,
    )
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
    cursor_handles: &[*mut libc::c_void],
    owned_trace_reg_ids: &[(u16, usize)],
) -> Result<Option<Batch>, i32> {
    gnitz_debug!(
        "vm: execute_epoch output_reg={} instrs={}",
        output_reg,
        program.instructions.len()
    );

    // 1. Clear delta batches (cursor refresh already done by caller)
    regfile.clear_deltas();

    // 2. Bind cursors and seed input batches
    regfile.bind_cursors(cursor_handles, owned_trace_reg_ids);
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
    macro_rules! cursor_mut {
        ($i:expr) => {{
            let r = reg_mut!($i);
            if r.cursor_ptr.is_null() {
                None
            } else {
                Some(unsafe { &mut *r.cursor_ptr }.cursor_mut())
            }
        }};
    }

    // 3. Dispatch loop
    for instr in &program.instructions {
        match instr {
            Instr::Halt => break,

            Instr::ClearDeltas => {
                regfile.clear_deltas();
            }

            Instr::Delay { src, state_reg, dst } => {
                // reg_mut! hands out aliasing &mut via a raw pointer, so
                // coincident indices would make the swaps below form two &mut to
                // one register — UB. The compiler always assigns distinct
                // registers; assert it in every profile (free, once per epoch)
                // rather than only in debug.
                assert_ne!(*src, *state_reg, "Delay: src and state_reg must be distinct");
                assert_ne!(*src, *dst, "Delay: src and dst must be distinct");
                assert_ne!(*state_reg, *dst, "Delay: state_reg and dst must be distinct");
                // Rotate the three registers in place with zero allocation:
                // dst ← old state_reg, state_reg ← old src, src ← old dst.
                // Correctness of the two-swap rotation relies on `dst` being
                // empty at epoch start; otherwise `src` would not end empty.
                debug_assert_eq!(reg!(*dst).batch.count, 0, "Delay: dst must be empty at epoch start");
                std::mem::swap(&mut reg_mut!(*src).batch, &mut reg_mut!(*state_reg).batch);
                std::mem::swap(&mut reg_mut!(*src).batch, &mut reg_mut!(*dst).batch);
            }

            Instr::ScanTrace {
                trace_reg,
                out_reg,
                chunk_limit,
            } => {
                let schema = reg!(*trace_reg).schema;
                if let Some(cursor) = cursor_mut!(*trace_reg) {
                    let result = ops::op_scan_trace(cursor, &schema, *chunk_limit);
                    reg_mut!(*out_reg).batch = result.into_inner();
                }
            }

            Instr::SeekTrace { trace_reg, key_reg } => {
                let kb = &reg!(*key_reg).batch;
                if kb.count > 0 {
                    // The key batch's PK region is already OPK at rest; seek by
                    // its bytes directly (no native-value round-trip).
                    if let Some(cursor) = cursor_mut!(*trace_reg) {
                        cursor.seek_bytes(kb.get_pk_bytes(0));
                    }
                }
            }

            Instr::Filter {
                in_reg,
                out_reg,
                func_idx,
            } => {
                debug_assert_ne!(*in_reg, *out_reg, "Filter: in_reg and out_reg must be distinct");
                let func_ptr = program.funcs[*func_idx as usize];
                let in_batch = &reg!(*in_reg).batch;
                let schema = reg!(*in_reg).schema;
                let result = if func_ptr.is_null() {
                    // NullPredicate: clone — in_reg may be read by multiple instructions.
                    let mut out = in_batch.clone_batch();
                    out.set_schema(schema);
                    out.sorted = in_batch.sorted;
                    out.consolidated = in_batch.consolidated;
                    out
                } else {
                    let func = unsafe { &*func_ptr };
                    ops::op_filter(in_batch, func, &schema)
                };
                reg_mut!(*out_reg).batch = result;
            }

            Instr::Map {
                in_reg,
                out_reg,
                func_idx,
                out_schema_idx,
                reindex_off,
                reindex_cnt,
                reindex_hash,
                branch_id,
            } => {
                debug_assert_ne!(*in_reg, *out_reg, "Map: in_reg and out_reg must be distinct");
                let func_ptr = program.funcs[*func_idx as usize];
                let in_schema = reg!(*in_reg).schema;
                let out_schema = &program.schemas[*out_schema_idx as usize];
                let result = if func_ptr.is_null() {
                    // Identity MAP: no transformation — copy batch with updated schema.
                    let mut out = reg!(*in_reg).batch.clone_batch();
                    out.set_schema(*out_schema);
                    out
                } else {
                    let func = unsafe { &*func_ptr };
                    let off = *reindex_off as usize;
                    let n = *reindex_cnt as usize;
                    let reindex_cols = &program.reindex_cols[off..off + n];
                    let target_tcs = &program.reindex_target_tcs[off..off + n];
                    ops::op_map(
                        &reg!(*in_reg).batch,
                        func,
                        &in_schema,
                        out_schema,
                        reindex_cols,
                        target_tcs,
                        *reindex_hash,
                        *branch_id,
                    )
                };
                reg_mut!(*out_reg).batch = result;
            }

            Instr::Negate { in_reg, out_reg } => {
                debug_assert_ne!(*in_reg, *out_reg, "Negate: in_reg and out_reg must be distinct");
                let result = ops::op_negate(&reg!(*in_reg).batch);
                reg_mut!(*out_reg).batch = result;
            }

            Instr::Union {
                in_a,
                in_b,
                has_b,
                out_reg,
            } => {
                let schema = reg!(*in_a).schema;
                let npc = reg!(*in_a).batch.num_payload_cols();
                let stride = schema.pk_stride();
                if *has_b && in_a == in_b {
                    // Self-union: Z + Z doubles every weight in-place. Reading
                    // batch_b after moving batch_a out would see an empty batch
                    // and produce +1 instead of +2.
                    let mut batch = std::mem::replace(&mut reg_mut!(*in_a).batch, Batch::empty(npc, stride));
                    for chunk in batch.weight_data_mut().chunks_exact_mut(8) {
                        let w = i64::from_le_bytes(chunk.try_into().unwrap());
                        chunk.copy_from_slice(&w.wrapping_mul(2).to_le_bytes());
                    }
                    reg_mut!(*out_reg).batch = batch;
                } else {
                    let batch_b = if *has_b { Some(&reg!(*in_b).batch) } else { None };
                    let batch_a = std::mem::replace(&mut reg_mut!(*in_a).batch, Batch::empty(npc, stride));
                    reg_mut!(*out_reg).batch = ops::op_union(batch_a, batch_b, &schema);
                }
            }

            Instr::Distinct {
                in_reg,
                hist_reg,
                out_reg,
                hist_table_idx,
            } => {
                if let Some(cursor) = cursor_mut!(*hist_reg) {
                    let schema = reg!(*in_reg).schema;
                    let npc = reg!(*in_reg).batch.num_payload_cols();
                    let stride = schema.pk_stride();
                    let delta = std::mem::replace(&mut reg_mut!(*in_reg).batch, Batch::empty(npc, stride));
                    let (output, consolidated) = ops::op_distinct(delta, cursor, &schema);
                    reg_mut!(*out_reg).batch = output.into_inner();
                    // Ingest consolidated delta into history table
                    if *hist_table_idx >= 0 {
                        let ptr = program.tables[*hist_table_idx as usize];
                        let table = unsafe { &mut *ptr };
                        let _ = table.ingest_owned_batch(consolidated.into_inner());
                    }
                }
                // If cursor is null, in_reg retains its data (not consumed).
            }

            Instr::JoinDT {
                delta_reg,
                trace_reg,
                out_reg,
                right_schema_idx,
            } => {
                let left_schema = reg!(*delta_reg).schema;
                let right_schema = &program.schemas[*right_schema_idx as usize];
                if let Some(cursor) = cursor_mut!(*trace_reg) {
                    let result = ops::op_join_delta_trace(&reg!(*delta_reg).batch, cursor, &left_schema, right_schema);
                    reg_mut!(*out_reg).batch = result;
                }
            }

            Instr::JoinDD {
                a_reg,
                b_reg,
                out_reg,
                right_schema_idx,
            } => {
                let left_schema = reg!(*a_reg).schema;
                let right_schema = &program.schemas[*right_schema_idx as usize];
                let result =
                    ops::op_join_delta_delta(&reg!(*a_reg).batch, &reg!(*b_reg).batch, &left_schema, right_schema);
                reg_mut!(*out_reg).batch = result;
            }

            Instr::JoinDTOuter {
                delta_reg,
                trace_reg,
                out_reg,
                right_schema_idx,
            } => {
                let left_schema = reg!(*delta_reg).schema;
                let right_schema = &program.schemas[*right_schema_idx as usize];
                if let Some(cursor) = cursor_mut!(*trace_reg) {
                    let result =
                        ops::op_join_delta_trace_outer(&reg!(*delta_reg).batch, cursor, &left_schema, right_schema);
                    reg_mut!(*out_reg).batch = result;
                } else {
                    // Absent trace ⟹ every delta row has no right-side match → null-extend.
                    // op_null_extend requires consolidated input.
                    let cs = Batch::consolidate_if_needed(&reg!(*delta_reg).batch, &left_schema);
                    let consolidated = cs.as_deref().unwrap_or(&reg!(*delta_reg).batch);
                    let result = ops::op_null_extend(consolidated, &left_schema, right_schema);
                    reg_mut!(*out_reg).batch = result;
                }
            }

            Instr::JoinDTRange {
                delta_reg,
                trace_reg,
                out_reg,
                right_schema_idx,
                n_eq,
                rel,
            } => {
                let left_schema = reg!(*delta_reg).schema;
                let right_schema = &program.schemas[*right_schema_idx as usize];
                if let Some(cursor) = cursor_mut!(*trace_reg) {
                    let result = ops::op_join_delta_trace_range(
                        &reg!(*delta_reg).batch,
                        cursor,
                        &left_schema,
                        right_schema,
                        *n_eq as usize,
                        *rel,
                    );
                    reg_mut!(*out_reg).batch = result;
                }
                // Absent trace ⟹ empty arrangement on the other side ⟹ no matches;
                // out_reg keeps whatever it held (an empty batch), like JoinDT.
            }

            Instr::PartitionFilter {
                in_reg,
                out_reg,
                worker_id,
                num_workers,
            } => {
                let schema = reg!(*in_reg).schema;
                let result = ops::op_partition_filter(&reg!(*in_reg).batch, &schema, *worker_id, *num_workers);
                reg_mut!(*out_reg).batch = result;
            }

            Instr::AntiJoinDT {
                delta_reg,
                trace_reg,
                out_reg,
            } => {
                let schema = reg!(*delta_reg).schema;
                if let Some(cursor) = cursor_mut!(*trace_reg) {
                    let result = ops::op_anti_join_delta_trace(&reg!(*delta_reg).batch, cursor, &schema);
                    reg_mut!(*out_reg).batch = result.into_inner();
                } else {
                    // Absent trace ⟹ nothing to exclude; pass delta through, but
                    // consolidate so downstream operators receive consolidated input.
                    let npc = reg!(*delta_reg).batch.num_payload_cols();
                    let stride = schema.pk_stride();
                    let batch = std::mem::replace(&mut reg_mut!(*delta_reg).batch, Batch::empty(npc, stride));
                    let cs = Batch::consolidate_if_needed(&batch, &schema);
                    let consolidated = cs.map(|c| c.into_inner()).unwrap_or(batch);
                    reg_mut!(*out_reg).batch = consolidated;
                }
            }

            Instr::AntiJoinDD { a_reg, b_reg, out_reg } => {
                let schema = reg!(*a_reg).schema;
                let result = ops::op_anti_join_delta_delta(&reg!(*a_reg).batch, &reg!(*b_reg).batch, &schema);
                reg_mut!(*out_reg).batch = result.into_inner();
            }

            Instr::SemiJoinDT {
                delta_reg,
                trace_reg,
                out_reg,
            } => {
                let schema = reg!(*delta_reg).schema;
                if let Some(cursor) = cursor_mut!(*trace_reg) {
                    let result = ops::op_semi_join_delta_trace(&reg!(*delta_reg).batch, cursor, &schema);
                    reg_mut!(*out_reg).batch = result.into_inner();
                }
            }

            Instr::SemiJoinDD { a_reg, b_reg, out_reg } => {
                let schema = reg!(*a_reg).schema;
                let result = ops::op_semi_join_delta_delta(&reg!(*a_reg).batch, &reg!(*b_reg).batch, &schema);
                reg_mut!(*out_reg).batch = result.into_inner();
            }

            Instr::NullExtend {
                in_reg,
                out_reg,
                right_schema_idx,
            } => {
                let in_schema = reg!(*in_reg).schema;
                let right_schema = &program.schemas[*right_schema_idx as usize];
                let result = ops::op_null_extend(&reg!(*in_reg).batch, &in_schema, right_schema);
                reg_mut!(*out_reg).batch = result;
            }

            Instr::Integrate {
                in_reg,
                table_idx,
                gi,
                avi,
            } => {
                let schema = reg!(*in_reg).schema;

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

                let gi_desc = gi.as_ref().map(|g| GiDesc {
                    table: program.tables[g.table_idx as usize],
                    col_idx: g.col_idx,
                });

                let avi_desc = avi.as_ref().map(|a| {
                    let gcols = &program.group_cols
                        [a.group_cols_offset as usize..(a.group_cols_offset as usize + a.group_cols_count as usize)];
                    AviDesc {
                        table: program.tables[a.table_idx as usize],
                        for_max: a.for_max,
                        agg_col_type_code: a.agg_col_type_code,
                        group_by_cols: gcols.to_vec(),
                        agg_col_idx: a.agg_col_idx,
                    }
                });

                gnitz_debug!(
                    "vm: INTEGRATE in_count={} target={} gi={} avi={}",
                    reg!(*in_reg).batch.count,
                    target.is_some(),
                    gi_desc.is_some(),
                    avi_desc.is_some()
                );
                let _ = ops::op_integrate_with_indexes(
                    &reg!(*in_reg).batch,
                    target,
                    &schema,
                    gi_desc.as_ref(),
                    avi_desc.as_ref(),
                );
            }

            Instr::Reduce {
                in_reg,
                trace_in_reg,
                trace_out_reg,
                out_reg,
                fin_out_reg,
                agg_descs_offset,
                agg_descs_count,
                group_cols_offset,
                group_cols_count,
                output_schema_idx,
                gi,
                avi,
                finalize_func_idx,
                finalize_schema_idx,
                global_ground,
                i_am_owner,
            } => {
                let in_schema = reg!(*in_reg).schema;
                let out_schema = &program.schemas[*output_schema_idx as usize];
                let aggs = &program.agg_descs
                    [*agg_descs_offset as usize..(*agg_descs_offset as usize + *agg_descs_count as usize)];
                let gcols = &program.group_cols
                    [*group_cols_offset as usize..(*group_cols_offset as usize + *group_cols_count as usize)];

                // trace_in cursor (from register file)
                let ti_cursor_ptr: *mut ReadCursor = if let Some(tr) = trace_in_reg {
                    let ptr = cursor_mut!(*tr)
                        .map(|c| c as *mut ReadCursor)
                        .unwrap_or(std::ptr::null_mut());
                    if ptr.is_null() {
                        return Err(-11);
                    }
                    ptr
                } else {
                    std::ptr::null_mut()
                };

                // trace_out cursor (from register file)
                let to_cursor_ptr: *mut ReadCursor = cursor_mut!(*trace_out_reg)
                    .map(|c| c as *mut ReadCursor)
                    .unwrap_or(std::ptr::null_mut());
                if to_cursor_ptr.is_null() {
                    return Err(-10);
                }

                // AVI cursor — created fresh from the AVI table (not a register).
                // Must be created AFTER INTEGRATE populates the AVI table.
                // Operator-state read; keep compacting (see refresh_owned_cursors).
                #[allow(clippy::disallowed_methods)] // explicit maintenance: REDUCE AVI operator state
                let mut avi_cursor_handle: Option<Box<CursorHandle>> = if let Some(avi) = avi {
                    let avi_ptr = program.tables[avi.table_idx as usize];
                    let avi_table = unsafe { &mut *avi_ptr };
                    avi_table.create_cursor_compacting().ok().map(Box::new)
                } else {
                    None
                };

                // GI cursor — created fresh from the GI table (not a register)
                // Operator-state read; keep compacting (see refresh_owned_cursors).
                #[allow(clippy::disallowed_methods)] // explicit maintenance: REDUCE GI operator state
                let mut gi_cursor_handle: Option<Box<CursorHandle>> = if let Some(gi) = gi {
                    let gi_ptr = program.tables[gi.table_idx as usize];
                    let gi_table = unsafe { &mut *gi_ptr };
                    gi_table.create_cursor_compacting().ok().map(Box::new)
                } else {
                    None
                };

                gnitz_debug!(
                    "vm: REDUCE in_count={} trace_in={} trace_out=ok avi={} gi={} aggs={}",
                    reg!(*in_reg).batch.count,
                    !ti_cursor_ptr.is_null(),
                    avi_cursor_handle.is_some(),
                    gi_cursor_handle.is_some(),
                    aggs.len()
                );

                let fin_prog = finalize_func_idx
                    .as_ref()
                    .map(|idx| unsafe { &*program.expr_progs[*idx as usize] });
                let fin_schema = finalize_schema_idx.as_ref().map(|idx| &program.schemas[*idx as usize]);

                let ti_opt: Option<&mut ReadCursor> = if !ti_cursor_ptr.is_null() {
                    Some(unsafe { &mut *ti_cursor_ptr })
                } else {
                    None
                };
                let avi_opt: Option<&mut ReadCursor> = avi_cursor_handle.as_deref_mut().map(|ch| ch.cursor_mut());
                let gi_opt: Option<&mut ReadCursor> = gi_cursor_handle.as_deref_mut().map(|ch| ch.cursor_mut());

                // AVI type code: real code only when the AVI cursor was actually
                // created (preserves the prior `avi_opt.is_some()` gate); else U64.
                let avi_tc = match avi {
                    Some(a) if avi_opt.is_some() => crate::schema::TypeCode::from_validated_u8(a.agg_col_type_code),
                    _ => crate::schema::TypeCode::U64,
                };
                // None -> false (unused by op_reduce when the AVI cursor is None).
                let avi_for_max = avi.as_ref().is_some_and(|a| a.for_max);
                // None -> 0 (a valid col idx; op_reduce builds the GI extractor
                // unconditionally).
                let gi_col_idx = gi.as_ref().map_or(0, |g| g.col_idx);

                let (raw_out, fin_out) = ops::op_reduce(
                    &reg!(*in_reg).batch,
                    ti_opt,
                    unsafe { &mut *to_cursor_ptr },
                    &in_schema,
                    out_schema,
                    gcols,
                    aggs,
                    avi_opt,
                    avi_for_max,
                    avi_tc,
                    gi_opt,
                    gi_col_idx,
                    fin_prog,
                    fin_schema,
                    *global_ground,
                    *i_am_owner,
                );

                // Drop temporary cursor handles (returned to pool)
                drop(avi_cursor_handle);
                drop(gi_cursor_handle);

                reg_mut!(*out_reg).batch = raw_out;
                if let Some(fin_batch) = fin_out {
                    if let Some(fr) = fin_out_reg {
                        reg_mut!(*fr).batch = fin_batch;
                    }
                }
            }
        }
    }

    gnitz_debug!("vm: dispatch done");

    // 4. Extract output
    let out = &mut regfile.registers[output_reg as usize];
    if out.batch.count > 0 {
        let npc = out.batch.num_payload_cols();
        let stride = out.batch.pk_stride();
        let result = std::mem::replace(&mut out.batch, Batch::empty(npc, stride));
        Ok(Some(result))
    } else {
        Ok(None)
    }
}
