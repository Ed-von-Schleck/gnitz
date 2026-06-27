//! Incremental REDUCE operator: δ_out = Agg(history + δ_in) − Agg(history).

use crate::schema::{SchemaDescriptor, TypeCode, PAYLOAD_MAPPING_PK_SENTINEL};
use crate::storage::{pk_bytes_eq, scatter_copy, Batch, ConsolidatedBatch, DrainGuard, MemBatch, ReadCursor};

use super::super::util::{cmp_col_window, extract_group_key, extract_group_key_cursor, GroupKeyExtractor};
use super::agg::{
    apply_agg_from_value_index, fold_old_aggs, is_single_col_natural_pk, Accumulator, AggDescriptor, AggOp,
};
use super::emit::{emit_finalized_row, emit_reduce_row};
use super::sort::{argsort_delta, argsort_pk_canonical, build_sort_descs, compare_by_group_cols, SortDesc};

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
    // `clear()` left the reused batch flagged sorted+consolidated; the rows just
    // written are neither. Without this, `consolidate_if_needed` short-circuits
    // and a retracted (PK,payload) and its insert both survive, corrupting
    // non-linear (MIN/MAX) aggregates.
    batch.sorted = false;
    batch.consolidated = false;
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

    // Hoisted once: the cursor's blob arena backs any German-string group column.
    let cursor_blob = cursor.blob_ptr();
    let cursor_blob_slice: &[u8] = if cursor_blob.is_null() {
        &[]
    } else {
        unsafe { std::slice::from_raw_parts(cursor_blob, cursor.blob_len()) }
    };

    for desc in descs {
        if desc.pi == PAYLOAD_MAPPING_PK_SENTINEL {
            // Raw byte equality suffices here (no order, no signed
            // dispatch): rows with identical bytes at this PK-column window
            // are equal regardless of type.
            let off = desc.pk_off as usize;
            let cs = desc.cs as usize;
            let cursor_bytes = &cursor.current_pk_bytes()[off..off + cs];
            let exemplar_bytes = &exemplar_mb.get_pk_bytes(exemplar_row)[off..off + cs];
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

        // Group membership is an equality test, but byte-equality and typed
        // equality coincide for every fixed-width type (and BLOB/STRING must
        // compare by content), so the shared comparator is exactly right here.
        if cmp_col_window(
            cursor_bytes,
            cursor_blob_slice,
            exemplar_bytes,
            exemplar_mb.blob,
            desc.tc,
        ) != std::cmp::Ordering::Equal
        {
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
    gi_cursor: Option<&mut ReadCursor>,
    gi_col_idx: u32,
    finalize_prog: Option<&crate::expr::ResolvedProgram>,
    finalize_out_schema: Option<&SchemaDescriptor>,
) -> (Batch, Option<Batch>) {
    let num_aggs = agg_descs.len();
    let num_out_cols = output_schema.num_columns();

    // Linearity check
    let all_linear = agg_descs.iter().all(|d| d.agg_op.is_linear());

    // Consolidate only for non-linear aggregates; linear aggregates work on raw delta.
    // Fast path (linear or already consolidated): borrow delta directly — no allocation.
    let cs = if all_linear {
        None
    } else {
        Batch::consolidate_if_needed(delta, input_schema)
    };
    let working: &Batch = cs.as_deref().unwrap_or(delta);

    let n = working.count;
    if n == 0 {
        let empty_fin = finalize_out_schema.map(Batch::empty_with_schema);
        return (Batch::empty_with_schema(output_schema), empty_fin);
    }

    // group_set_eq_pk: GROUP BY is a permutation of the source PK columns.
    // Group membership is tested on the full PK byte window (get_pk_bytes),
    // exact at every width, and argsort_pk_canonical orders every PK width
    // correctly (a width-matched key, or the compare_pk_bytes walk above 32
    // bytes) — so no narrow-stride restriction is needed.
    // The fast path's group-detection loop walks rows in iteration order
    // and breaks on PK mismatch — sound iff iteration order is canonical
    // PK order. When `working.sorted` is set the input is already in that
    // order (consolidation, integrated trace, sorted union); otherwise we
    // must argsort by canonical PK order to avoid splitting one PK into
    // multiple groups. Output's pk_indices is in source pk-list order
    // regardless of group_by_cols permutation.
    let group_set_eq_pk = input_schema.group_cols_eq_pk(group_by_cols);
    let group_by_pk = group_set_eq_pk;

    // Pre-compute sort descriptors for group comparisons (non-pk path).
    let (sort_descs, sort_descs_len) = if group_by_pk {
        (
            [SortDesc {
                pi: 0,
                cs: 0,
                tc: TypeCode::U64,
                c_idx: 0,
                pk_off: 0,
            }; crate::schema::MAX_COLUMNS],
            0,
        )
    } else {
        build_sort_descs(input_schema, group_by_cols)
    };
    let group_descs = &sort_descs[..sort_descs_len];

    let mb = working.as_mem_batch();

    // Argsort. group_by_pk keeps the sorted/consolidated output mark; only the
    // iteration order changes when the input is unsorted.
    let sorted_indices: Vec<u32> = if group_by_pk && working.sorted {
        (0..n as u32).collect()
    } else if group_by_pk {
        argsort_pk_canonical(&mb)
    } else {
        argsort_delta(working, input_schema, group_by_cols)
    };

    // Output mapping: matches build_reduce_output_schema's logic — must
    // stay in sync. Independent of the stride-gated fast-path eligibility
    // above (compound natural-PK at stride > 16 still emits natural PKs).
    let use_natural_pk = group_set_eq_pk || is_single_col_natural_pk(input_schema, group_by_cols);

    let mut raw_output = Batch::with_schema(*output_schema, 32);
    let mut fin_output = finalize_out_schema.map(|fs| Batch::with_schema(*fs, 32));
    // One FinalizeContext per finalize program: hoists EvalScratch sizing,
    // out_cols classification, EMIT→register map, and the no_nulls flag out
    // of the (potentially 100k+ iteration) group loop.
    let mut fin_ctx = finalize_prog.map(|p| crate::expr::FinalizeContext::new(p, output_schema));

    let mut accs: Vec<Accumulator> = agg_descs.iter().map(|d| Accumulator::new(d, input_schema)).collect();
    // Output width of each trailing agg column (loop-invariant across the
    // 100k+-group loop below). MIN/MAX now emit at the source type's width, so a
    // narrow column is < 8 bytes; the trace read-back uses this as both the
    // per-row offset stride and the slice length before `readback_agg_bits`
    // rebuilds the 8-byte accumulator (width-gated, so a float MIN/MAX widened
    // to F64 is read verbatim).
    let agg_col_widths: Vec<usize> = (0..num_aggs)
        .map(|k| output_schema.columns[num_out_cols - num_aggs + k].size() as usize)
        .collect();
    // First aggregate column's logical index / payload slot (the aggregates are
    // the trailing output columns, so these hold at any PK arity). Loop-invariant
    // — hoisted out of the group loop alongside `agg_col_widths`.
    let cbase = num_out_cols - num_aggs;
    let pbase = output_schema.num_payload_cols() - num_aggs;

    // We need mutable access to optional cursors. Take ownership via Option::take pattern.
    // The caller passes these as Option<&mut ReadCursor>, but we need to use them
    // multiple times in the loop. They're already &mut so we can use them directly.
    let mut trace_in = trace_in_cursor;
    let mut avi = avi_cursor;
    let mut gi = gi_cursor;

    // GI group-column extractor: built once so population and read-back agree
    // on a raw, injective gc. `gi_col_idx` defaults to a valid column index
    // (0) when GI is unused, so building unconditionally is safe.
    let gc_extractor = super::super::util::IndexColExtractor::new(input_schema, gi_col_idx as usize);

    // AVI group-key gatherer: built once so the per-group lookup gathers the
    // full byte-form group key (the prefix the AVI cursor seeks on).
    let avi_extractor = avi
        .is_some()
        .then(|| GroupKeyExtractor::new(input_schema, group_by_cols));

    // Hoist replay batch outside the group loop: reuse the allocation across groups
    // rather than allocating and dropping once per group (can be 100k+ times per epoch).
    let mut replay = (!all_linear && avi.is_none()).then(|| Batch::with_schema(*input_schema, 32));

    // Single-scan trace gather for the non-linear, non-PK, no-index fallback.
    // Replaces the per-group full-trace rescan (O(groups × trace)) with one
    // pass (O(trace + delta)). `fallback_state` is `(matched_rows, offsets)`:
    // matched_rows is sorted by group so `offsets[g] = (start, len)` slices the
    // rows belonging to group g.
    type FallbackScan = (Vec<(u32, u32, i64)>, Vec<(u32, u32)>);
    let fallback_state: Option<FallbackScan> = if !all_linear && avi.is_none() && !group_by_pk && gi.is_none() {
        trace_in.as_deref_mut().map(|ti_cursor| {
            // Pass 1: one exemplar row per distinct delta group, in group order.
            let mut group_exemplars = Vec::with_capacity(n);
            let mut tmp_idx = 0usize;
            while tmp_idx < n {
                let exemplar = sorted_indices[tmp_idx] as usize;
                group_exemplars.push(exemplar);
                tmp_idx += 1;
                while tmp_idx < n {
                    let curr = sorted_indices[tmp_idx] as usize;
                    if compare_by_group_cols(&mb, curr, exemplar, group_descs) != std::cmp::Ordering::Equal {
                        break;
                    }
                    tmp_idx += 1;
                }
            }
            let num_g = group_exemplars.len();

            // Hash index only pays off past a handful of groups; below the
            // threshold a direct linear probe per trace row is cheaper than
            // hashing every trace row. Either way the trace is scanned once.
            //
            // Sorted Vec<(hash, group_idx)> rather than HashMap<u128, Vec<usize>>:
            // binary search is O(log num_g) per trace row but avoids per-bucket
            // heap allocations (one malloc per group in the HashMap). For groups
            // that share a hash key (u128 collision, astronomically rare),
            // `cursor_matches_group` still picks the right one via byte compare.
            const HASH_THRESHOLD: usize = 16;
            let use_hash = num_g >= HASH_THRESHOLD;
            let mut hash_groups: Vec<(u128, usize)> = Vec::new(); // (group_key, group_idx)
            if use_hash {
                hash_groups = group_exemplars
                    .iter()
                    .enumerate()
                    .map(|(g, &exemplar)| (extract_group_key(&mb, exemplar, input_schema, group_by_cols), g))
                    .collect();
                hash_groups.sort_unstable_by_key(|&(h, _)| h);
            }

            // Pass 2: one full trace scan, route each row to its group.
            let mut tagged: Vec<(u32, u32, u32, i64)> = Vec::new(); // (g, entry, row, w)
            ti_cursor.rewind();
            let mut scanned = 0usize;
            while ti_cursor.valid {
                scanned += 1;
                if ti_cursor.current_weight != 0 {
                    // Disjoint groups: a trace row belongs to at most one, so
                    // `matched` stops at the first exact match.
                    let mut matched = None;
                    if use_hash {
                        let hash = extract_group_key_cursor(ti_cursor, input_schema, group_by_cols);
                        let pos = hash_groups.partition_point(|&(h, _)| h < hash);
                        let mut p = pos;
                        while p < hash_groups.len() && hash_groups[p].0 == hash {
                            let g = hash_groups[p].1;
                            if cursor_matches_group(ti_cursor, &mb, group_exemplars[g], group_descs) {
                                matched = Some(g);
                                break;
                            }
                            p += 1;
                        }
                    } else {
                        for (g, &exemplar) in group_exemplars.iter().enumerate() {
                            if cursor_matches_group(ti_cursor, &mb, exemplar, group_descs) {
                                matched = Some(g);
                                break;
                            }
                        }
                    }
                    if let Some(g) = matched {
                        let (entry, row, w) = ti_cursor.current_row_loc();
                        tagged.push((g as u32, entry, row, w));
                    }
                }
                ti_cursor.advance();
            }

            gnitz_debug!(
                "op_reduce fallback: 1 trace scan, {} rows, {} groups, {} matched",
                scanned,
                num_g,
                tagged.len()
            );

            // Cluster matches by group in O(matched + groups) via counting
            // sort — group ids are dense `0..num_g`, so a comparison sort is
            // unnecessary. `offsets[g] = (start, len)` slices group g's rows
            // in `matched_rows`; empty groups get `(start, 0)`.
            let mut offsets = vec![(0u32, 0u32); num_g];
            for &(g, _, _, _) in &tagged {
                offsets[g as usize].1 += 1; // pass 1: per-group counts
            }
            let mut acc = 0u32; // pass 2: prefix-sum counts into start offsets
            for off in offsets.iter_mut() {
                off.0 = acc;
                acc += off.1; // off.1 keeps the count, now the slice len
            }
            // Pass 3: scatter each tagged row into its group's slice.
            // Use offsets[g].0 as the advancing write cursor, then restore
            // it to the original start (start = cursor - count = .0 - .1).
            let mut matched_rows = vec![(0u32, 0u32, 0i64); tagged.len()];
            for &(g, entry, row, w) in &tagged {
                let p = offsets[g as usize].0;
                matched_rows[p as usize] = (entry, row, w);
                offsets[g as usize].0 += 1;
            }
            for off in offsets.iter_mut() {
                off.0 -= off.1; // restore start from advanced cursor
            }
            (matched_rows, offsets)
        })
    } else {
        None
    };

    // A group exists iff its net cardinality (row weight) is positive; the unique
    // AggOp::Count accumulator carries that signal. The SQL planner gives every
    // all-linear GROUP BY view exactly one Count — a user COUNT(*) or an appended
    // companion — so SQL views gate on cardinality and shed emptied/all-NULL
    // groups correctly. When no Count is present — a non-linear reduce, or a
    // hand-built circuit from the low-level CircuitBuilder API that omits one —
    // `cardinality_idx` is None and the gate falls back to the any_nonzero
    // touched-ness test below (the prior behavior).
    let cardinality_idx: Option<usize> = all_linear
        .then(|| agg_descs.iter().position(|d| d.agg_op == AggOp::Count))
        .flatten();

    let mut idx = 0usize;
    let mut num_groups = 0usize;
    while idx < n {
        let group_start_pos = idx;
        let group_start_idx = sorted_indices[group_start_pos] as usize;

        // The input row's PK bytes: keys the MIN/MAX history (trace_in) seeks,
        // which read by the *source* PK, and the group-membership compare.
        let group_pk_bytes = mb.get_pk_bytes(group_start_idx);

        // The group's *output* PK bytes, materialised once for both the trace_out
        // retraction seek and the emitted row (so the two can never drift). A
        // compound natural PK's output region mirrors the source PK byte-for-byte
        // → verbatim. Every single-column output PK is the OPK encoding of the
        // group key at the output stride: the source PK value (`get_pk`) for
        // natural-PK grouping, the synthetic `extract_group_key` for payload GROUP
        // BY (which differs from the input row's PK). The single-column branch
        // owns the only width ≤ 16, so `get_pk`/`[16 - stride..]` never overrun.
        let mut out_pk_buf = [0u8; crate::schema::MAX_PK_BYTES];
        let out_pk_bytes: &[u8] = if group_by_pk && output_schema.pk_indices().len() > 1 {
            group_pk_bytes
        } else {
            let stride = output_schema.pk_stride() as usize;
            let key = if group_by_pk {
                mb.get_pk(group_start_idx)
            } else {
                extract_group_key(&mb, group_start_idx, input_schema, group_by_cols)
            };
            out_pk_buf[..stride].copy_from_slice(&key.to_be_bytes()[16 - stride..]);
            &out_pk_buf[..stride]
        };

        // Step linear accumulators over delta rows in this group
        for acc in accs.iter_mut() {
            acc.reset();
        }
        while idx < n {
            let curr_idx = sorted_indices[idx] as usize;
            if group_by_pk {
                // Full PK byte-window compare: exact at every width, unlike the
                // lossy u128 get_pk for pk_stride > 16.
                if !pk_bytes_eq(mb.get_pk_bytes(curr_idx), group_pk_bytes) {
                    break;
                }
            } else if compare_by_group_cols(&mb, curr_idx, group_start_idx, group_descs) != std::cmp::Ordering::Equal {
                break;
            }

            let w = mb.get_weight(curr_idx);
            for acc in accs.iter_mut() {
                if acc.is_linear() {
                    acc.step_from_batch(&mb, curr_idx, w);
                }
            }
            idx += 1;
        }

        // Retraction: read old value from trace_out, keyed by the group's output
        // PK (`out_pk_bytes`). Natural-PK grouping visits groups in ascending
        // output-PK order, so the probe is monotone → galloping `advance_to`
        // seeded at the live position. Payload GROUP BY keys on the synthetic
        // group key, whose order need not match trace_out storage order, so it
        // keeps the absolute `seek_bytes` (correct for a non-monotone probe).
        if group_by_pk {
            trace_out_cursor.advance_to(out_pk_bytes);
        } else {
            trace_out_cursor.seek_bytes(out_pk_bytes);
        }
        let has_old = trace_out_cursor.valid && trace_out_cursor.current_pk_eq(out_pk_bytes);

        if has_old {
            // δ_out's −Agg(history) term IS the stored output row: copy trace_out's
            // current row at weight -1. Byte-identical (PK, payload, null bits,
            // blobs) by construction, so it consolidates against the row it cancels
            // with zero per-column reconstruction and no null plumbing.
            trace_out_cursor.copy_current_row_into(&mut raw_output, -1);
            if let (Some(prog), Some(fin_schema), Some(ref mut fin_out), Some(ctx)) =
                (finalize_prog, finalize_out_schema, &mut fin_output, fin_ctx.as_mut())
            {
                emit_finalized_row(
                    fin_out,
                    &raw_output,
                    raw_output.count - 1,
                    -1,
                    prog,
                    output_schema,
                    fin_schema,
                    ctx,
                );
            }
        }

        // New value calculation
        if all_linear && has_old {
            // new = old + delta: fold the old aggregates straight off the
            // still-positioned trace cursor into the accumulators.
            fold_old_aggs(&mut accs, trace_out_cursor, agg_descs, &agg_col_widths, cbase, pbase);
        } else if !all_linear {
            if let (Some(ref mut avi_c), Some(extractor)) = (&mut avi, &avi_extractor) {
                // AVI path: gather the full group key and prefix-seek the index.
                let mut gk = [0u8; crate::schema::MAX_PK_BYTES];
                extractor.gather(&mb, group_start_idx, &mut gk);
                apply_agg_from_value_index(
                    avi_c,
                    &gk[..extractor.stride],
                    avi_for_max,
                    avi_agg_col_type_code,
                    &mut accs[0],
                );
            } else {
                let replay = replay.as_mut().unwrap();
                replay.clear();

                let delta_indices: &[u32] = &sorted_indices[group_start_pos..idx];

                let mut trace_rows = DrainGuard::new();

                if let Some(ti_cursor) = trace_in.as_deref_mut() {
                    if group_by_pk {
                        // MIN/MAX history read; group_by_pk visits groups in
                        // ascending output-PK order, so the probe is monotone →
                        // galloping `advance_to`.
                        ti_cursor.advance_to(group_pk_bytes);
                        ti_cursor.for_each_pk_group_row(group_pk_bytes, |c| {
                            c.push_current_row(&mut trace_rows);
                        });
                    } else if let Some(gi_c) = gi.as_deref_mut() {
                        // The GI key is `[gc(LE) ‖ src_pk(OPK)]`. gc is stored
                        // and probed in the same LE encoding (the GI is only
                        // point-probed by an exact gc prefix, never numeric
                        // range, so it need not be OPK); src_pk is the source
                        // table's OPK bytes, so it seeks the OPK trace directly.
                        let gc_u64_val = gc_extractor.extract(&mb, group_start_idx);
                        let mut prefix = [0u8; crate::ops::index::GI_GC_BYTES];
                        prefix.copy_from_slice(&gc_u64_val.to_le_bytes());
                        let mut hit = gi_c.seek_first_positive_with_prefix(&prefix);
                        while hit {
                            let k = gi_c.current_pk_bytes();
                            let src_pk_bytes = &k[crate::ops::index::GI_GC_BYTES..];
                            // ti_cursor traversal is non-monotonic across GI
                            // entries; `seek_bytes` is an absolute binary search
                            // so a backward re-seek is O(log N) and correct.
                            ti_cursor.seek_bytes(src_pk_bytes);
                            ti_cursor.for_each_pk_group_row(src_pk_bytes, |c| {
                                c.push_current_row(&mut trace_rows);
                            });
                            gi_c.advance();
                            hit = gi_c.walk_to_positive_with_prefix(&prefix);
                        }
                    } else {
                        // Precomputed by the single-scan pre-pass; `num_groups`
                        // is this group's index (the pre-pass walks identical
                        // group boundaries) and has not yet been incremented.
                        // fallback_state is Some whenever trace_in is Some (the
                        // pre-pass is gated by the same condition as this branch).
                        debug_assert!(
                            fallback_state.is_some(),
                            "fallback_state is None despite trace_in being Some",
                        );
                        let (matched_rows, offsets) = fallback_state.as_ref().unwrap();
                        let (start, len) = offsets[num_groups];
                        for &(entry, row, w) in &matched_rows[start as usize..(start + len) as usize] {
                            trace_rows.push((entry, row, w));
                        }
                    }
                }

                fill_cleared_batch(replay, trace_in.as_deref(), &trace_rows, &mb, delta_indices);

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

        // Emission: a group exists iff its net cardinality is positive. With a
        // COUNT signal (SQL all-linear reduce): read it — correct for both emptied
        // groups (fold saturates has_value) and new all-NULL groups (nothing
        // touches the value accumulators). Without one (non-linear, whose w>0
        // replay already eliminates emptied groups; or a companion-less low-level
        // circuit): fall back to the any_nonzero touched-ness test.
        let should_emit = match cardinality_idx {
            Some(ci) => {
                // Bag-positive reduce inputs ⇒ cardinality ≥ 0; fail loudly if a
                // future operator ever violates that, since the gate is not
                // sign-robust the way the old any_nonzero touched-ness test was.
                debug_assert!(
                    accs[ci].count_value() >= 0,
                    "reduce input must be bag-positive: negative group cardinality",
                );
                accs[ci].count_value() > 0
            }
            None => accs.iter().any(|a| !a.is_zero()),
        };
        if should_emit {
            emit_reduce_row(
                &mut raw_output,
                &mb,
                group_start_idx,
                out_pk_bytes,
                &accs,
                input_schema,
                output_schema,
                group_by_cols,
                use_natural_pk,
                num_aggs,
            );
            if let (Some(prog), Some(fin_schema), Some(ref mut fin_out), Some(ctx)) =
                (finalize_prog, finalize_out_schema, &mut fin_output, fin_ctx.as_mut())
            {
                emit_finalized_row(
                    fin_out,
                    &raw_output,
                    raw_output.count - 1,
                    1,
                    prog,
                    output_schema,
                    fin_schema,
                    ctx,
                );
            }
        }

        num_groups += 1;
    }

    gnitz_debug!(
        "op_reduce: in={} groups={} out={} fin={}",
        n,
        num_groups,
        raw_output.count,
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
