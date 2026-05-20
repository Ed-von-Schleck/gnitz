//! Incremental REDUCE operator: δ_out = Agg(history + δ_in) − Agg(history).

use crate::schema::{SchemaDescriptor, TypeCode, PAYLOAD_MAPPING_PK_SENTINEL};
use crate::storage::{
    Batch, ConsolidatedBatch, DrainGuard, MemBatch, ReadCursor,
    scatter_copy,
};

use super::super::util::{extract_gc_u64, extract_group_key};
use super::agg::{
    Accumulator, AggDescriptor, apply_agg_from_value_index, is_single_col_natural_pk,
};
use super::emit::{emit_finalized_row, emit_reduce_row};
use super::sort::{
    SortDesc, argsort_delta, argsort_pk_canonical, build_sort_descs, compare_by_group_cols,
};

/// `clear()` does not re-zero the data buffer, so the scatter variants used
/// here must be the unconditional-copy ones — a nullable-skip would leak stale
/// bytes through.
fn fill_cleared_batch(
    batch: &mut Batch,
    trace_cursor: Option<&ReadCursor>,
    trace_rows: &[(u32, u32, i64)],
    delta_mb: &MemBatch,
    delta_indices: &[u32],
) {
    let needed = trace_rows.len() + delta_indices.len();
    if needed == 0 {
        return;
    }
    batch.reserve_rows(needed);
    {
        let mut writer = batch.capacity_writer();
        if let Some(cursor) = trace_cursor {
            cursor.scatter_drained_into(trace_rows, &mut writer);
        }
        // Delta rows arrive already filtered to non-zero weights, so the
        // empty `weights` arg routes through `scatter_col_first`.
        scatter_copy(delta_mb, delta_indices, &[], &mut writer);
    }
    batch.count = needed;
}

/// Check if a cursor's current row matches the group columns of an exemplar row.
pub(super) fn cursor_matches_group(
    cursor: &ReadCursor,
    exemplar_mb: &MemBatch,
    exemplar_row: usize,
    descs: &[SortDesc],
) -> bool {
    let cursor_null_word = cursor.current_null_word;
    let exemplar_null_word = exemplar_mb.get_null_word(exemplar_row);

    for desc in descs {
        if desc.pi == PAYLOAD_MAPPING_PK_SENTINEL {
            // Raw byte equality suffices here (no order, no signed/float
            // dispatch): rows with identical bytes at this PK-column window
            // are equal regardless of type.
            let off = desc.pk_off as usize;
            let cs = desc.cs as usize;
            let cursor_bytes = &cursor.current_pk_bytes()[off..off + cs];
            let exemplar_bytes =
                &exemplar_mb.get_pk_bytes(exemplar_row)[off..off + cs];
            if cursor_bytes != exemplar_bytes {
                return false;
            }
            continue;
        }

        let pi = desc.pi as usize;
        let cs = desc.cs as usize;

        let cursor_is_null = (cursor_null_word >> pi) & 1 != 0;
        let exemplar_is_null = (exemplar_null_word >> pi) & 1 != 0;
        if cursor_is_null != exemplar_is_null {
            return false;
        }
        if cursor_is_null {
            continue;
        }

        let cursor_ptr = cursor.col_ptr(desc.c_idx as usize, cs);
        if cursor_ptr.is_null() {
            return false;
        }
        let cursor_bytes = unsafe { std::slice::from_raw_parts(cursor_ptr, cs) };
        let exemplar_bytes = exemplar_mb.get_col_ptr(exemplar_row, pi, cs);

        if desc.tc == TypeCode::String {
            let cursor_blob = cursor.blob_ptr();
            let cmp = crate::schema::compare_german_strings(
                cursor_bytes, if cursor_blob.is_null() { &[] } else { unsafe { std::slice::from_raw_parts(cursor_blob, cursor.blob_len()) } },
                exemplar_bytes, exemplar_mb.blob,
            );
            if cmp != std::cmp::Ordering::Equal {
                return false;
            }
        } else if cursor_bytes != exemplar_bytes {
            return false;
        }
    }
    true
}

/// Incremental DBSP REDUCE: δ_out = Agg(history + δ_in) - Agg(history).
#[allow(clippy::too_many_arguments, clippy::needless_range_loop)]
pub fn op_reduce(
    delta: &Batch,
    trace_in_cursor: Option<&mut ReadCursor>,
    trace_out_cursor: &mut ReadCursor,
    input_schema: &SchemaDescriptor,
    output_schema: &SchemaDescriptor,
    group_by_cols: &[u32],
    agg_descs: &[AggDescriptor],
    avi_cursor: Option<&mut ReadCursor>,
    avi_for_max: bool,
    avi_agg_col_type_code: TypeCode,
    avi_group_by_cols: &[u32],
    avi_input_schema: Option<&SchemaDescriptor>,
    gi_cursor: Option<&mut ReadCursor>,
    gi_col_idx: u32,
    _gi_col_type_code: TypeCode,
    finalize_prog: Option<&crate::expr::ExprProgram>,
    finalize_out_schema: Option<&SchemaDescriptor>,
) -> (Batch, Option<Batch>) {
    let num_aggs = agg_descs.len();
    let num_out_cols = output_schema.num_columns();

    // Linearity check
    let all_linear = agg_descs.iter().all(|d| d.agg_op.is_linear());

    // Consolidate only for non-linear aggregates; linear aggregates work on raw delta.
    // Fast path (linear or already consolidated): borrow delta directly — no allocation.
    let cs = if all_linear { None } else { Batch::consolidate_if_needed(delta, input_schema) };
    let working: &Batch = cs.as_deref().unwrap_or(delta);

    let n = working.count;
    if n == 0 {
        let empty_fin = finalize_out_schema.map(Batch::empty_with_schema);
        return (Batch::empty_with_schema(output_schema), empty_fin);
    }

    // group_set_eq_pk: GROUP BY is a permutation of the source PK columns.
    // group_by_pk additionally requires stride ∈ {8, 16} because the fast
    // path widens PK regions via widen_pk_le on each row; wider compound
    // PKs cannot use the u128 comparator and must take the slow path.
    // The fast path's group-detection loop walks rows in iteration order
    // and breaks on PK mismatch — sound iff iteration order is canonical
    // PK order. When `working.sorted` is set the input is already in that
    // order (consolidation, integrated trace, sorted union); otherwise we
    // must argsort by canonical PK order to avoid splitting one PK into
    // multiple groups. Output's pk_indices is in source pk-list order
    // regardless of group_by_cols permutation.
    let group_set_eq_pk = input_schema.group_cols_eq_pk(group_by_cols);
    let group_by_pk = group_set_eq_pk
        && matches!(input_schema.pk_stride(), 8 | 16);

    // Pre-compute sort descriptors for group comparisons (non-pk path).
    let (sort_descs, sort_descs_len) = if group_by_pk {
        ([SortDesc { pi: 0, cs: 0, tc: TypeCode::U64, c_idx: 0, pk_off: 0 };
          crate::schema::MAX_COLUMNS], 0)
    } else {
        build_sort_descs(input_schema, group_by_cols)
    };
    let group_descs = &sort_descs[..sort_descs_len];

    let mb = working.as_mem_batch();

    // Argsort. group_by_pk keeps `group_key = mb.get_pk(...)` and the
    // sorted/consolidated output mark; only the iteration order changes
    // when the input is unsorted.
    let sorted_indices: Vec<u32> = if group_by_pk && working.sorted {
        (0..n as u32).collect()
    } else if group_by_pk {
        argsort_pk_canonical(&mb, input_schema)
    } else {
        argsort_delta(working, input_schema, group_by_cols)
    };

    // Output mapping: matches build_reduce_output_schema's logic — must
    // stay in sync. Independent of the stride-gated fast-path eligibility
    // above (compound natural-PK at stride > 16 still emits natural PKs).
    let use_natural_pk = group_set_eq_pk
        || is_single_col_natural_pk(input_schema, group_by_cols);

    let mut raw_output = Batch::with_schema(*output_schema, 32);
    let mut fin_output = finalize_out_schema.map(|fs| {
        Batch::with_schema(*fs, 32)
    });
    // One FinalizeContext per finalize program: hoists EvalScratch sizing,
    // out_cols classification, EMIT→register map, and the no_nulls flag out
    // of the (potentially 100k+ iteration) group loop.
    let mut fin_ctx = finalize_prog.map(|p| crate::expr::FinalizeContext::new(p, output_schema));

    let mut accs: Vec<Accumulator> = agg_descs.iter().map(|d| Accumulator::new(d, input_schema)).collect();
    let mut old_vals: Vec<u64> = vec![0u64; num_aggs];

    // We need mutable access to optional cursors. Take ownership via Option::take pattern.
    // The caller passes these as Option<&mut ReadCursor>, but we need to use them
    // multiple times in the loop. They're already &mut so we can use them directly.
    let mut trace_in = trace_in_cursor;
    let mut avi = avi_cursor;
    let mut gi = gi_cursor;

    // Hoist replay batch outside the group loop: reuse the allocation across groups
    // rather than allocating and dropping once per group (can be 100k+ times per epoch).
    let mut replay = (!all_linear && avi.is_none())
        .then(|| Batch::with_schema(*input_schema, 32));

    let mut idx = 0usize;
    let mut num_groups = 0usize;
    while idx < n {
        let group_start_pos = idx;
        let group_start_idx = sorted_indices[group_start_pos] as usize;

        let group_key: u128 = if group_by_pk {
            mb.get_pk(group_start_idx)
        } else {
            extract_group_key(&mb, group_start_idx, input_schema, group_by_cols)
        };

        // Step linear accumulators over delta rows in this group
        for acc in accs.iter_mut() {
            acc.reset();
        }
        while idx < n {
            let curr_idx = sorted_indices[idx] as usize;
            if group_by_pk {
                let curr_pk = mb.get_pk(curr_idx);
                if curr_pk != group_key {
                    break;
                }
            } else {
                if compare_by_group_cols(&mb, curr_idx, group_start_idx, group_descs)
                    != std::cmp::Ordering::Equal
                {
                    break;
                }
            }

            let w = mb.get_weight(curr_idx);
            for acc in accs.iter_mut() {
                if acc.is_linear() {
                    acc.step_from_batch(&mb, curr_idx, w);
                }
            }
            idx += 1;
        }

        // Retraction: read old value from trace_out
        trace_out_cursor.seek(group_key);
        let has_old = trace_out_cursor.valid
            && trace_out_cursor.current_key == group_key;

        if has_old {
            // Read old agg values from trace_out
            for k in 0..num_aggs {
                let agg_col_idx = num_out_cols - num_aggs + k;
                let ptr = trace_out_cursor.col_ptr(agg_col_idx, 8);
                if !ptr.is_null() {
                    let bytes = unsafe { std::slice::from_raw_parts(ptr, 8) };
                    old_vals[k] = u64::from_le_bytes(bytes.try_into().unwrap());
                } else {
                    old_vals[k] = 0;
                }
            }
            // Emit retraction row (weight=-1)
            emit_reduce_row(
                &mut raw_output, &mb, group_start_idx,
                group_key, -1,
                &old_vals, true, // use_old_val=true
                &accs, input_schema, output_schema,
                group_by_cols, use_natural_pk, num_aggs,
            );
            if let (Some(prog), Some(fin_schema), Some(ref mut fin_out), Some(ctx)) =
                (finalize_prog, finalize_out_schema, &mut fin_output, fin_ctx.as_mut())
            {
                emit_finalized_row(
                    fin_out, &raw_output, raw_output.count - 1,
                    group_key, -1,
                    prog, output_schema, fin_schema, ctx,
                );
            }
        }

        // New value calculation
        if all_linear && has_old {
            for k in 0..num_aggs {
                accs[k].merge_accumulated(old_vals[k], 1);
            }
        } else if !all_linear {
            if let Some(ref mut avi_c) = avi {
                // AVI path
                let avi_schema = avi_input_schema.unwrap_or(input_schema);
                let gc_u64 = extract_gc_u64(&mb, group_start_idx, avi_schema, avi_group_by_cols);
                let found = apply_agg_from_value_index(
                    avi_c, gc_u64, avi_for_max, avi_agg_col_type_code,
                    &mut accs[0],
                );
                gnitz_debug!("reduce: AVI lookup gc={:#x} found={}", gc_u64, found);
            } else {
                let replay = replay.as_mut().unwrap();
                replay.clear();

                let delta_indices: &[u32] = &sorted_indices[group_start_pos..idx];

                let mut trace_rows = DrainGuard::new();
                trace_rows.clear();

                if let Some(ti_cursor) = trace_in.as_deref_mut() {
                    if group_by_pk {
                        ti_cursor.seek(group_key);
                        while ti_cursor.valid
                            && ti_cursor.current_key == group_key
                        {
                            ti_cursor.push_current_row(&mut trace_rows);
                            ti_cursor.advance();
                        }
                    } else if let Some(gi_c) = gi.as_deref_mut() {
                        let gi_ci = gi_col_idx as usize;
                        let gc_u64_val = if input_schema.is_pk_col(gi_ci) {
                            mb.get_pk(group_start_idx) as u64
                        } else {
                            let cs = input_schema.columns[gi_ci].size() as usize;
                            let pi = input_schema.payload_idx(gi_ci);
                            let ptr = mb.get_col_ptr(group_start_idx, pi, cs);
                            let mut buf = [0u8; 8];
                            buf[..cs].copy_from_slice(ptr);
                            u64::from_le_bytes(buf)
                        };
                        gi_c.seek(crate::util::make_pk(0, gc_u64_val));
                        while gi_c.valid {
                            let gk_hi = (gi_c.current_key >> 64) as u64;
                            if gk_hi != gc_u64_val {
                                break;
                            }
                            if gi_c.current_weight > 0 {
                                let spk_lo = gi_c.current_key as u64;
                                // spk_hi is in payload col 1 (first payload col = col index 1)
                                let spk_hi_ptr = gi_c.col_ptr(1, 8);
                                let spk_hi = if !spk_hi_ptr.is_null() {
                                    let bytes = unsafe { std::slice::from_raw_parts(spk_hi_ptr, 8) };
                                    u64::from_le_bytes(bytes.try_into().unwrap())
                                } else {
                                    0
                                };
                                let trace_key = crate::util::make_pk(spk_lo, spk_hi);
                                ti_cursor.seek(trace_key);
                                while ti_cursor.valid && ti_cursor.current_key == trace_key {
                                    ti_cursor.push_current_row(&mut trace_rows);
                                    ti_cursor.advance();
                                }
                            }
                            gi_c.advance();
                        }
                    } else {
                        // Fallback: full trace scan, predicate-filtered.
                        ti_cursor.seek(0u128);
                        let ti_mb_exemplar_row = group_start_idx;
                        while ti_cursor.valid {
                            if cursor_matches_group(
                                ti_cursor, &mb, ti_mb_exemplar_row,
                                group_descs,
                            ) {
                                ti_cursor.push_current_row(&mut trace_rows);
                            }
                            ti_cursor.advance();
                        }
                    }
                }

                fill_cleared_batch(
                    replay, trace_in.as_deref(), &trace_rows, &mb, delta_indices,
                );

                // Consolidate replay and step all accumulators (borrow replay; don't consume it)
                let merged_cs = Batch::consolidate_if_needed(replay, input_schema);
                let merged: &Batch = merged_cs.as_deref().unwrap_or(&*replay);
                for acc in accs.iter_mut() {
                    acc.reset();
                }
                let merged_mb = merged.as_mem_batch();
                for m in 0..merged.count {
                    let w = merged_mb.get_weight(m);
                    if w > 0 {
                        for acc in accs.iter_mut() {
                            acc.step_from_batch(&merged_mb, m, w);
                        }
                    }
                }
            }
        }

        // Emission: +1 for new value
        let any_nonzero = accs.iter().any(|a| !a.is_zero());
        if any_nonzero {
            emit_reduce_row(
                &mut raw_output, &mb, group_start_idx,
                group_key, 1,
                &old_vals, false, // use_old_val=false
                &accs, input_schema, output_schema,
                group_by_cols, use_natural_pk, num_aggs,
            );
            if let (Some(prog), Some(fin_schema), Some(ref mut fin_out), Some(ctx)) =
                (finalize_prog, finalize_out_schema, &mut fin_output, fin_ctx.as_mut())
            {
                emit_finalized_row(
                    fin_out, &raw_output, raw_output.count - 1,
                    group_key, 1,
                    prog, output_schema, fin_schema, ctx,
                );
            }
        }

        num_groups += 1;
    }

    gnitz_debug!(
        "op_reduce: in={} groups={} out={} fin={}",
        n, num_groups, raw_output.count,
        fin_output.as_ref().map_or(0, |b| b.count)
    );

    if group_by_pk {
        // PK-grouped output: one row per unique PK, sorted and weight-folded.
        raw_output.sorted = true;
        raw_output.consolidated = true;
        debug_assert!(ConsolidatedBatch::from_batch_ref(&raw_output).is_some());
    }
    (raw_output, fin_output)
}
