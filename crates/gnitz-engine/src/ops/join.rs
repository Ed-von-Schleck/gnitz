//! Join operators: anti-join, semi-join, inner join, outer join (delta-trace + delta-delta).

use std::cmp::Ordering;

use crate::schema::SchemaDescriptor;
use gnitz_wire::is_german_string;
use crate::storage::{
    write_to_batch, Batch, ConsolidatedBatch, MemBatch, ReadCursor, scatter_copy,
    with_payload_cmp,
};

use super::util::{all_payload_null_mask, merge_null_words, write_string_from_batch};

thread_local! {
    /// Reused emit-index scratch for the delta-trace anti/semi joins. Cleared
    /// per operator call rather than allocating a fresh `Vec<u32>` each tick —
    /// same hold-across-work, no-cap shape as exchange.rs's pools. The borrow is
    /// confined to one operator body; `scatter_copy` does not re-enter.
    static JOIN_EMIT_INDICES: std::cell::RefCell<Vec<u32>> =
        const { std::cell::RefCell::new(Vec::new()) };
}

// ---------------------------------------------------------------------------
// Anti-join delta-trace
// ---------------------------------------------------------------------------

/// Merge-walk a sorted delta's PKs against the trace `cursor` (positioned by an
/// internal `seek` to the first PK), returning the indices of delta rows to
/// emit. `emit_when_present` selects semi-join (emit rows whose key is in the
/// trace) vs anti-join (emit rows whose key is absent). A key is "present" iff
/// some trace entry for it has positive weight — an uncompacted trace may hold a
/// tombstone (weight ≤ 0) before a live row, so every entry for the PK is
/// scanned. Consecutive duplicate PKs reuse the previous presence result instead
/// of re-seeking (which would be O(log N) per duplicate).
fn dt_emit_indices(
    cursor: &mut ReadCursor,
    batch: &Batch,
    emit_when_present: bool,
    emit_indices: &mut Vec<u32>,
) {
    let n = batch.count;
    let mut prev_in_trace = false;
    // Seek/compare by the batch row's OPK bytes directly — no native-value
    // round-trip, correct at every PK width/signedness.
    cursor.seek_bytes(batch.get_pk_bytes(0));

    for i in 0..n {
        let pk = batch.get_pk_bytes(i);

        let in_trace = if i > 0 && pk == batch.get_pk_bytes(i - 1) {
            prev_in_trace
        } else {
            while cursor.valid && cursor.current_pk_cmp_bytes(pk).is_lt() {
                cursor.advance();
            }
            let mut found = false;
            while cursor.valid && cursor.current_pk_cmp_bytes(pk) == Ordering::Equal {
                if cursor.current_weight > 0 { found = true; }
                cursor.advance();
            }
            found
        };

        if in_trace == emit_when_present {
            emit_indices.push(i as u32);
        }

        prev_in_trace = in_trace;
    }
}

/// Emit delta rows whose key has NO positive-weight match in the trace.
pub fn op_anti_join_delta_trace(
    delta: &Batch,
    cursor: &mut ReadCursor,
    schema: &SchemaDescriptor,
) -> ConsolidatedBatch {
    let npc = schema.num_payload_cols();
    let cs = Batch::consolidate_if_needed(delta, schema);
    let consolidated: &Batch = cs.as_deref().unwrap_or(delta);
    let n = consolidated.count;
    if n == 0 {
        return ConsolidatedBatch::new_unchecked(Batch::empty(npc, schema.pk_stride()));
    }

    JOIN_EMIT_INDICES.with(|pool| {
        let mut emit_indices = pool.borrow_mut();
        emit_indices.clear();
        dt_emit_indices(cursor, consolidated, false, &mut emit_indices);

        if emit_indices.is_empty() {
            return ConsolidatedBatch::new_unchecked(Batch::empty(npc, schema.pk_stride()));
        }

        let mb = consolidated.as_mem_batch();
        let blob_cap = mb.blob.len().max(1);
        let mut output =
            write_to_batch(schema, emit_indices.len(), blob_cap, |writer| {
                scatter_copy(&mb, &emit_indices, &[], writer);
            });
        output.sorted = true;
        output.consolidated = true;
        ConsolidatedBatch::new_unchecked(output)
    })
}

// ---------------------------------------------------------------------------
// Semi-join delta-trace
// ---------------------------------------------------------------------------

/// Emit delta rows whose key HAS a positive-weight match in the trace.
pub fn op_semi_join_delta_trace(
    delta: &Batch,
    cursor: &mut ReadCursor,
    schema: &SchemaDescriptor,
) -> ConsolidatedBatch {
    let npc = schema.num_payload_cols();
    let cs = Batch::consolidate_if_needed(delta, schema);
    let consolidated: &Batch = cs.as_deref().unwrap_or(delta);
    let n = consolidated.count;
    if n == 0 {
        return ConsolidatedBatch::new_unchecked(Batch::empty(npc, schema.pk_stride()));
    }

    // Reset cursor to position 0 for the same reason as op_join_delta_trace:
    // the same trace register may be shared across multiple ops in one tick.
    cursor.rewind();

    let trace_len = cursor.estimated_length();

    JOIN_EMIT_INDICES.with(|pool| {
        let mut emit_indices = pool.borrow_mut();
        emit_indices.clear();

        if n > trace_len {
            // Adaptive swap: iterate trace, binary-search delta
            return ConsolidatedBatch::new_unchecked(
                semi_join_dt_swapped(consolidated, cursor, schema, &mut emit_indices));
        }

        // Merge-walk
        dt_emit_indices(cursor, consolidated, true, &mut emit_indices);

        if emit_indices.is_empty() {
            return ConsolidatedBatch::new_unchecked(Batch::empty(npc, schema.pk_stride()));
        }

        let mb = consolidated.as_mem_batch();
        let blob_cap = mb.blob.len().max(1);
        let mut output =
            write_to_batch(schema, emit_indices.len(), blob_cap, |writer| {
                scatter_copy(&mb, &emit_indices, &[], writer);
            });
        output.sorted = true;
        output.consolidated = true;
        ConsolidatedBatch::new_unchecked(output)
    })
}

fn semi_join_dt_swapped(
    consolidated: &Batch,
    cursor: &mut ReadCursor,
    schema: &SchemaDescriptor,
    emit_indices: &mut Vec<u32>,
) -> Batch {
    let npc = schema.num_payload_cols();
    let n = consolidated.count;

    use crate::schema::MAX_PK_BYTES;
    let stride = schema.pk_stride() as usize;
    let mut last_pk_buf = [0u8; MAX_PK_BYTES];
    let mut has_last = false;

    while cursor.valid {
        if cursor.current_weight <= 0 {
            cursor.advance();
            continue;
        }

        // Copy current PK bytes before the dedup compare and before advance().
        let mut t_pk_buf = [0u8; MAX_PK_BYTES];
        t_pk_buf[..stride].copy_from_slice(cursor.current_pk_bytes());
        let t_pk_bytes = &t_pk_buf[..stride];

        // Skip duplicate PKs in trace (different payloads) — delta indices
        // were already pushed for the first occurrence of this PK.
        if has_last && last_pk_buf[..stride] == *t_pk_bytes {
            cursor.advance();
            continue;
        }

        // Binary search in consolidated delta by the OPK bytes (raw memcmp).
        let pos = consolidated.find_lower_bound_bytes(t_pk_bytes);
        let mut j = pos;
        while j < n && consolidated.get_pk_bytes(j) == t_pk_bytes {
            emit_indices.push(j as u32);
            j += 1;
        }
        last_pk_buf[..stride].copy_from_slice(t_pk_bytes);
        has_last = true;
        cursor.advance();
    }

    if emit_indices.is_empty() {
        return Batch::empty(npc, schema.pk_stride());
    }

    let mb = consolidated.as_mem_batch();
    let blob_cap = mb.blob.len().max(1);
    let mut output =
        write_to_batch(schema, emit_indices.len(), blob_cap, |writer| {
            scatter_copy(&mb, emit_indices, &[], writer);
        });
    // Swapped order: output is in trace key order, which is sorted.
    // Rows come from the consolidated delta so (PK, payload) pairs are unique.
    output.sorted = true;
    output.consolidated = true;
    output
}

// ---------------------------------------------------------------------------
// Inner join delta-trace
// ---------------------------------------------------------------------------

/// Join delta rows against trace. Output schema: [left_PK, left_payload..., right_payload...].
pub fn op_join_delta_trace(
    delta: &Batch,
    cursor: &mut ReadCursor,
    left_schema: &SchemaDescriptor,
    right_schema: &SchemaDescriptor,
) -> Batch {
    let left_npc = left_schema.num_payload_cols();
    let right_npc = right_schema.num_payload_cols();
    let out_npc = left_npc + right_npc;

    let cs = Batch::consolidate_if_needed(delta, left_schema);
    let consolidated: &Batch = cs.as_deref().unwrap_or(delta);
    let n = consolidated.count;
    if n == 0 {
        return Batch::empty(out_npc, left_schema.pk_stride());
    }

    // The same trace register may be reused by multiple join ops in one tick
    // (e.g. both join_ba and correction_raw share trace_a in a LEFT JOIN circuit).
    // Reset to position 0 so estimated_length() sees the full trace and
    // join_dt_swapped iterates from the beginning.
    cursor.rewind();

    let trace_len = cursor.estimated_length();
    if n > trace_len {
        return join_dt_swapped(consolidated, cursor, left_schema, right_schema);
    }

    join_dt_merge_walk::<false>(consolidated, cursor, left_schema, right_schema)
}

/// Merge-walk consolidated `delta` against the trace `cursor`, emitting the join
/// product for every matching (delta, trace) pair. With `EMIT_UNMATCHED` (left
/// outer join) a delta row with no positive-weight trace match additionally emits
/// a null-filled right side; without it (inner join) unmatched rows are dropped.
/// Const-generic so the inner path compiles the outer-only bookkeeping away.
fn join_dt_merge_walk<const EMIT_UNMATCHED: bool>(
    delta: &Batch,
    cursor: &mut ReadCursor,
    left_schema: &SchemaDescriptor,
    right_schema: &SchemaDescriptor,
) -> Batch {
    let left_npc = left_schema.num_payload_cols();
    let right_npc = right_schema.num_payload_cols();
    let out_npc = left_npc + right_npc;
    let n = delta.count;
    if n == 0 {
        return Batch::empty(out_npc, left_schema.pk_stride());
    }

    let delta_mb = delta.as_mem_batch();
    let mut output = Batch::empty_joined(left_schema, right_schema);

    cursor.seek_bytes(delta.get_pk_bytes(0));

    for i in 0..n {
        let pk_i = delta.get_pk_bytes(i);
        let w_delta = delta.get_weight(i);

        if i > 0 && pk_i == delta.get_pk_bytes(i - 1) {
            // Multiset delta: same PK, different payload — re-seek trace.
            cursor.seek_bytes(pk_i);
        } else {
            while cursor.valid && cursor.current_pk_cmp_bytes(pk_i).is_lt() {
                cursor.advance();
            }
        }

        // `matched` reflects membership in Distinct(trace): set it on any
        // positive-weight entry, not merely a non-zero join product. An
        // uncompacted trace may present a tombstone (weight ≤ 0) for the PK;
        // a tombstone alone must NOT suppress the outer null-fill.
        let mut matched = false;
        while cursor.valid && cursor.current_pk_cmp_bytes(pk_i) == Ordering::Equal {
            let w_trace = cursor.current_weight;
            if EMIT_UNMATCHED && w_trace > 0 {
                matched = true;
            }
            let w_out = w_delta.wrapping_mul(w_trace);
            if w_out != 0 {
                write_join_row(
                    &mut output, &delta_mb, i, cursor, w_out,
                    left_schema, right_schema,
                );
            }
            cursor.advance();
        }

        if EMIT_UNMATCHED && !matched {
            write_join_row_null_right(
                &mut output, &delta_mb, i, w_delta,
                left_schema, right_schema,
            );
        }
    }

    output.sorted = true;
    output
}

fn join_dt_swapped(
    delta: &Batch,
    cursor: &mut ReadCursor,
    left_schema: &SchemaDescriptor,
    right_schema: &SchemaDescriptor,
) -> Batch {
    use crate::schema::MAX_PK_BYTES;
    let n = delta.count;
    let stride = left_schema.pk_stride() as usize;

    let delta_mb = delta.as_mem_batch();
    let mut output = Batch::empty_joined(left_schema, right_schema);

    while cursor.valid {
        // Copy trace PK bytes to a stack buffer so cursor.advance() does not
        // conflict with a live borrow of cursor in the inner group walk.
        let mut t_pk_buf = [0u8; MAX_PK_BYTES];
        t_pk_buf[..stride].copy_from_slice(cursor.current_pk_bytes());
        let t_pk_bytes = &t_pk_buf[..stride];
        let w_trace = cursor.current_weight;

        let pos = delta.find_lower_bound_bytes(t_pk_bytes);
        let mut j = pos;
        while j < n && delta.get_pk_bytes(j) == t_pk_bytes {
            let w_delta = delta.get_weight(j);
            let w_out = w_delta.wrapping_mul(w_trace);
            if w_out != 0 {
                write_join_row(
                    &mut output, &delta_mb, j, cursor, w_out,
                    left_schema, right_schema,
                );
            }
            j += 1;
        }
        cursor.advance();
    }

    // Trace-major cartesian emission is PK-sorted but NOT (PK, payload)-sorted
    // (left payloads interleave across trace entries). Unlike semi_join_dt_swapped
    // (delta-only rows, deduped trace PKs), this output is genuinely unsorted —
    // do not let into_consolidated/fold_sorted or an N-way merge trust it.
    output.sorted = false;
    output
}

// ---------------------------------------------------------------------------
// Outer join delta-trace
// ---------------------------------------------------------------------------

/// Left outer join: like inner join, but unmatched delta rows emit null-filled
/// right side. Always merge-walks — the swapped (trace-driven) path can't emit
/// the null-fill for delta rows absent from the trace.
pub fn op_join_delta_trace_outer(
    delta: &Batch,
    cursor: &mut ReadCursor,
    left_schema: &SchemaDescriptor,
    right_schema: &SchemaDescriptor,
) -> Batch {
    let cs = Batch::consolidate_if_needed(delta, left_schema);
    let consolidated: &Batch = cs.as_deref().unwrap_or(delta);
    join_dt_merge_walk::<true>(consolidated, cursor, left_schema, right_schema)
}

// ---------------------------------------------------------------------------
// Join row writer helpers
// ---------------------------------------------------------------------------

/// Copy left-side payload columns from a MemBatch into the output.
/// Shared by all join row writers (inner, outer, DD).
fn write_left_payload(
    output: &mut Batch,
    left_batch: &MemBatch,
    left_row: usize,
    left_null: u64,
    left_schema: &SchemaDescriptor,
) {
    for (pi, _ci, col) in left_schema.payload_columns() {
        let cs = col.size() as usize;
        let is_null = (left_null >> pi) & 1 != 0;
        if is_null {
            output.fill_col_zero(pi, cs);
        } else if is_german_string(col.type_code) {
            write_string_from_batch(output, pi, left_batch, left_row, pi);
        } else {
            output.extend_col(pi, left_batch.get_col_ptr(left_row, pi, cs));
        }
    }
}

/// Write one composite join output row: [left_PK, left_payload..., right_payload...].
///
/// Left columns come from the delta MemBatch. Right columns come from the
/// cursor's current row, accessed via `col_ptr()` / `blob_ptr()`.
fn write_join_row(
    output: &mut Batch,
    left_batch: &MemBatch,
    left_row: usize,
    right_cursor: &ReadCursor,
    weight: i64,
    left_schema: &SchemaDescriptor,
    right_schema: &SchemaDescriptor,
) {
    let left_null = left_batch.get_null_word(left_row);
    let right_null = right_cursor.current_null_word;

    let left_npc = left_schema.num_payload_cols();
    let null_word = merge_null_words(left_null, right_null, left_npc);

    output.extend_pk_bytes(left_batch.get_pk_bytes(left_row));
    output.extend_weight(&weight.to_le_bytes());
    output.extend_null_bmp(&null_word.to_le_bytes());

    write_left_payload(output, left_batch, left_row, left_null, left_schema);

    // Right payload columns (from cursor public API)
    let right_blob = right_cursor.blob_slice();  // &[u8], bounds carried
    for (rpi, ci, col) in right_schema.payload_columns() {
        let cs = col.size() as usize;
        let is_null = (right_null >> rpi) & 1 != 0;
        let out_pi = left_npc + rpi;
        if is_null {
            output.fill_col_zero(out_pi, cs);
        } else if is_german_string(col.type_code) {
            let ptr = right_cursor.col_ptr(ci, 16);
            if ptr.is_null() {
                output.fill_col_zero(out_pi, 16);
            } else {
                // SAFETY: ptr is a valid col_ptr (non-null checked above) to a
                // 16-byte German-string struct; relocate_german_string_vec
                // bounds-checks the blob offset against right_blob.
                let src = unsafe { std::slice::from_raw_parts(ptr, 16) };
                let dest = crate::schema::relocate_german_string_vec(
                    src, right_blob, &mut output.blob, None,
                );
                output.extend_col(out_pi, &dest);
            }
        } else {
            let ptr = right_cursor.col_ptr(ci, cs);
            if ptr.is_null() {
                output.fill_col_zero(out_pi, cs);
            } else {
                let src = unsafe { std::slice::from_raw_parts(ptr, cs) };
                output.extend_col(out_pi, src);
            }
        }
    }

    output.count += 1;
}

/// Write one null-filled join output row: left columns from batch, right columns all NULL.
fn write_join_row_null_right(
    output: &mut Batch,
    left_batch: &MemBatch,
    left_row: usize,
    weight: i64,
    left_schema: &SchemaDescriptor,
    right_schema: &SchemaDescriptor,
) {
    let left_null = left_batch.get_null_word(left_row);

    let left_npc = left_schema.num_payload_cols();
    let right_npc = right_schema.num_payload_cols();
    let right_null_bits = all_payload_null_mask(right_npc);
    let null_word = merge_null_words(left_null, right_null_bits, left_npc);

    output.extend_pk_bytes(left_batch.get_pk_bytes(left_row));
    output.extend_weight(&weight.to_le_bytes());
    output.extend_null_bmp(&null_word.to_le_bytes());

    write_left_payload(output, left_batch, left_row, left_null, left_schema);

    // Right payload columns: all zeros (null)
    for (rpi, _ci, col) in right_schema.payload_columns() {
        output.fill_col_zero(left_npc + rpi, col.size() as usize);
    }

    output.count += 1;
}

// ---------------------------------------------------------------------------
// Anti-join / Semi-join delta-delta
// ---------------------------------------------------------------------------

/// Emit batch_a rows whose (PK, payload) has NO positive-weight match in batch_b.
/// For SQL EXCEPT: a row is excluded only if B has a matching (PK, payload), not just PK.
pub fn op_anti_join_delta_delta(
    batch_a: &Batch,
    batch_b: &Batch,
    schema: &SchemaDescriptor,
) -> ConsolidatedBatch {
    ConsolidatedBatch::new_unchecked(filter_join_dd_with_payload(batch_a, batch_b, schema))
}

/// Emit batch_a rows whose PK HAS a positive-weight match in batch_b.
pub fn op_semi_join_delta_delta(
    batch_a: &Batch,
    batch_b: &Batch,
    schema: &SchemaDescriptor,
) -> ConsolidatedBatch {
    ConsolidatedBatch::new_unchecked(filter_join_dd(batch_a, batch_b, schema, true))
}

/// Payload-aware anti-join DD: excludes A rows only when B has a matching
/// (PK, payload) with positive weight. Used for SQL EXCEPT.
fn filter_join_dd_with_payload(
    batch_a: &Batch,
    batch_b: &Batch,
    schema: &SchemaDescriptor,
) -> Batch {
    let cs_a = Batch::consolidate_if_needed(batch_a, schema);
    let cs_b = Batch::consolidate_if_needed(batch_b, schema);
    let ca: &Batch = cs_a.as_deref().unwrap_or(batch_a);
    let cb: &Batch = cs_b.as_deref().unwrap_or(batch_b);

    with_payload_cmp!(schema, filter_join_dd_with_payload_inner, ca, cb, schema)
}

#[inline]
fn filter_join_dd_with_payload_inner<RowCmp>(
    ca: &Batch,
    cb: &Batch,
    schema: &SchemaDescriptor,
    row_cmp: RowCmp,
) -> Batch
where
    RowCmp: Fn(&SchemaDescriptor, &MemBatch, usize, &MemBatch, usize) -> Ordering + Copy,
{
    let npc = schema.num_payload_cols();
    let n_a = ca.count;
    let n_b = cb.count;

    if n_a == 0 {
        return Batch::empty(npc, schema.pk_stride());
    }

    let mb_a = ca.as_mem_batch();
    let mb_b = cb.as_mem_batch();
    let mut idx_a = 0usize;
    let mut idx_b = 0usize;
    let mut output = Batch::empty_with_schema(schema);

    while idx_a < n_a {
        let key_a = ca.get_pk_bytes(idx_a);

        // Advance B past every PK group ordered before key_a.
        while idx_b < n_b && cb.get_pk_bytes(idx_b) < key_a {
            idx_b += 1;
        }

        // Extent of B's PK group matching key_a.
        let b_group_start = idx_b;
        let mut b_group_end = idx_b;
        while b_group_end < n_b && cb.get_pk_bytes(b_group_end) == key_a {
            b_group_end += 1;
        }

        // Extent of A's PK group.
        let a_group_start = idx_a;
        let mut a_group_end = idx_a;
        while a_group_end < n_a && ca.get_pk_bytes(a_group_end) == key_a {
            a_group_end += 1;
        }

        // Both groups are payload-sorted (consolidated), so a single B cursor
        // advances monotonically across all A rows — O(N+M) per group, not
        // O(N×M).
        let mut scan_b = b_group_start;
        let mut unmatched_start: Option<usize> = None;
        for idx_a_row in a_group_start..a_group_end {
            // Reuse the comparison that ends the advance: when the loop breaks
            // with scan_b in range, `ord` already holds the (≥ Equal) result for
            // the current B row, so the match test needs no second row_cmp.
            let mut ord = Ordering::Greater;
            while scan_b < b_group_end {
                ord = row_cmp(schema, &mb_b, scan_b, &mb_a, idx_a_row);
                if ord != Ordering::Less {
                    break;
                }
                scan_b += 1;
            }
            let matched = scan_b < b_group_end
                && ord == Ordering::Equal
                && cb.get_weight(scan_b) > 0;
            if !matched {
                if unmatched_start.is_none() {
                    unmatched_start = Some(idx_a_row);
                }
            } else if let Some(rs) = unmatched_start.take() {
                output.append_batch(ca, rs, idx_a_row);
            }
        }
        if let Some(rs) = unmatched_start.take() {
            output.append_batch(ca, rs, a_group_end);
        }

        idx_a = a_group_end;
    }

    output.sorted = true;
    output.consolidated = true;
    gnitz_debug!("op_anti_join_dd: a={} b={} out={}", n_a, n_b, output.count);
    output
}

/// Shared merge-walk for semi-join DD (PK-only matching).
/// `emit_on_match=false` → anti-join (unused after payload-aware split),
/// `emit_on_match=true` → semi-join.
fn filter_join_dd(
    batch_a: &Batch,
    batch_b: &Batch,
    schema: &SchemaDescriptor,
    emit_on_match: bool,
) -> Batch {
    let npc = schema.num_payload_cols();
    let cs_a = Batch::consolidate_if_needed(batch_a, schema);
    let cs_b = Batch::consolidate_if_needed(batch_b, schema);
    let ca: &Batch = cs_a.as_deref().unwrap_or(batch_a);
    let cb: &Batch = cs_b.as_deref().unwrap_or(batch_b);
    let n_a = ca.count;
    let n_b = cb.count;

    if n_a == 0 {
        return Batch::empty(npc, schema.pk_stride());
    }

    let mut idx_a = 0usize;
    let mut idx_b = 0usize;
    let mut output = Batch::empty_with_schema(schema);

    while idx_a < n_a {
        let key_a = ca.get_pk_bytes(idx_a);

        // Advance B past every PK group ordered before key_a.
        while idx_b < n_b && cb.get_pk_bytes(idx_b) < key_a {
            idx_b += 1;
        }

        let mut has_match = false;
        if idx_b < n_b && cb.get_pk_bytes(idx_b) == key_a {
            let mut scan_b = idx_b;
            while scan_b < n_b && cb.get_pk_bytes(scan_b) == key_a {
                if cb.get_weight(scan_b) > 0 {
                    has_match = true;
                    break;
                }
                scan_b += 1;
            }
        }

        let start_a = idx_a;
        while idx_a < n_a && ca.get_pk_bytes(idx_a) == key_a {
            idx_a += 1;
        }

        if has_match == emit_on_match {
            output.append_batch(ca, start_a, idx_a);
        }
    }

    output.sorted = true;
    output.consolidated = true;
    let tag = if emit_on_match { "semi" } else { "anti" };
    gnitz_debug!("op_{}_join_dd: a={} b={} out={}", tag, n_a, n_b, output.count);
    output
}

// ---------------------------------------------------------------------------
// Inner join delta-delta
// ---------------------------------------------------------------------------

/// Delta-Delta inner join: ΔA ⋈ ΔB. Cartesian product per matching PK group.
/// Output schema: [left_PK, left_payload..., right_payload...].
/// Weight = w_a * w_b (wrapping). Zero-weight pairs are skipped.
pub fn op_join_delta_delta(
    batch_a: &Batch,
    batch_b: &Batch,
    left_schema: &SchemaDescriptor,
    right_schema: &SchemaDescriptor,
) -> Batch {
    let left_npc = left_schema.num_payload_cols();
    let right_npc = right_schema.num_payload_cols();
    let out_npc = left_npc + right_npc;

    let cs_a = Batch::consolidate_if_needed(batch_a, left_schema);
    let cs_b = Batch::consolidate_if_needed(batch_b, right_schema);
    let ca: &Batch = cs_a.as_deref().unwrap_or(batch_a);
    let cb: &Batch = cs_b.as_deref().unwrap_or(batch_b);
    let n_a = ca.count;
    let n_b = cb.count;

    if n_a == 0 || n_b == 0 {
        gnitz_debug!("op_join_dd: a={} b={} out=0", n_a, n_b);
        return Batch::empty(out_npc, left_schema.pk_stride());
    }

    let mb_a = ca.as_mem_batch();
    let mb_b = cb.as_mem_batch();
    let mut output = Batch::empty_joined(left_schema, right_schema);

    let mut idx_a = 0usize;
    let mut idx_b = 0usize;

    while idx_a < n_a && idx_b < n_b {
        let key_a = ca.get_pk_bytes(idx_a);
        let key_b = cb.get_pk_bytes(idx_b);

        match key_a.cmp(key_b) {
            Ordering::Less => idx_a += 1,
            Ordering::Greater => idx_b += 1,
            Ordering::Equal => {
                let match_pk = key_a;

                let start_a = idx_a;
                idx_a += 1;
                while idx_a < n_a && ca.get_pk_bytes(idx_a) == match_pk {
                    idx_a += 1;
                }

                let start_b = idx_b;
                idx_b += 1;
                while idx_b < n_b && cb.get_pk_bytes(idx_b) == match_pk {
                    idx_b += 1;
                }

                for i in start_a..idx_a {
                    let wa = ca.get_weight(i);
                    if wa == 0 {
                        continue;
                    }
                    for j in start_b..idx_b {
                        let wb = cb.get_weight(j);
                        let w_out = wa.wrapping_mul(wb);
                        if w_out != 0 {
                            write_join_row_from_batches(
                                &mut output,
                                &mb_a, i, &mb_b, j,
                                w_out,
                                left_schema, right_schema,
                            );
                        }
                    }
                }
            }
        }
    }

    output.sorted = true;
    gnitz_debug!("op_join_dd: a={} b={} out={}", n_a, n_b, output.count);
    output
}

/// Write one composite join output row where both sides come from MemBatch.
#[allow(clippy::too_many_arguments)]
fn write_join_row_from_batches(
    output: &mut Batch,
    left_batch: &MemBatch,
    left_row: usize,
    right_batch: &MemBatch,
    right_row: usize,
    weight: i64,
    left_schema: &SchemaDescriptor,
    right_schema: &SchemaDescriptor,
) {
    let left_null = left_batch.get_null_word(left_row);
    let right_null = right_batch.get_null_word(right_row);

    let left_npc = left_schema.num_payload_cols();
    let null_word = merge_null_words(left_null, right_null, left_npc);

    output.extend_pk_bytes(left_batch.get_pk_bytes(left_row));
    output.extend_weight(&weight.to_le_bytes());
    output.extend_null_bmp(&null_word.to_le_bytes());

    write_left_payload(output, left_batch, left_row, left_null, left_schema);

    // Right payload columns (from right MemBatch)
    for (rpi, _ci, col) in right_schema.payload_columns() {
        let cs = col.size() as usize;
        let is_null = (right_null >> rpi) & 1 != 0;
        let out_pi = left_npc + rpi;
        if is_null {
            output.fill_col_zero(out_pi, cs);
        } else if is_german_string(col.type_code) {
            write_string_from_batch(output, out_pi, right_batch, right_row, rpi);
        } else {
            output.extend_col(out_pi, right_batch.get_col_ptr(right_row, rpi, cs));
        }
    }

    output.count += 1;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{SchemaColumn, SchemaDescriptor, type_code, SHORT_STRING_THRESHOLD};
    use crate::storage::{Batch, ConsolidatedBatch};

    fn make_schema_u64_i64() -> SchemaDescriptor {
        SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::I64, 0),
            ],
            &[0],
        )
    }

    fn make_schema_u64_string() -> SchemaDescriptor {
        SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::STRING, 0),
            ],
            &[0],
        )
    }

    fn make_batch(
        schema: &SchemaDescriptor,
        rows: &[(u64, i64, i64)],
    ) -> ConsolidatedBatch {
        let n = rows.len();
        let mut b = Batch::with_schema(*schema, n.max(1));

        for &(pk, w, val) in rows {
            b.extend_pk(pk as u128);
            b.extend_weight(&w.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &val.to_le_bytes());
            b.count += 1;
        }
        b.sorted = true;
        b.consolidated = true;
        ConsolidatedBatch::new_unchecked(b)
    }

    fn make_batch_str(
        schema: &SchemaDescriptor,
        rows: &[(u64, i64, &str)],
    ) -> ConsolidatedBatch {
        let n = rows.len();
        let mut b = Batch::with_schema(*schema, n.max(1));

        for &(pk, w, s) in rows {
            b.extend_pk(pk as u128);
            b.extend_weight(&w.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());

            let bytes = s.as_bytes();
            let length = bytes.len() as u32;
            let mut gs = [0u8; 16];
            gs[0..4].copy_from_slice(&length.to_le_bytes());
            if bytes.len() <= SHORT_STRING_THRESHOLD {
                let copy_len = bytes.len().min(12);
                gs[4..4 + copy_len].copy_from_slice(&bytes[..copy_len]);
            } else {
                gs[4..8].copy_from_slice(&bytes[..4]);
                let offset = b.blob.len() as u64;
                gs[8..16].copy_from_slice(&offset.to_le_bytes());
                b.blob.extend_from_slice(bytes);
            }
            b.extend_col(0, &gs);
            b.count += 1;
        }
        b.sorted = true;
        b.consolidated = true;
        ConsolidatedBatch::new_unchecked(b)
    }

    fn read_str_payload(batch: &Batch, col: usize, row: usize) -> String {
        let off = row * 16;
        let gs = &batch.col_data(col)[off..off + 16];
        let length = u32::from_le_bytes(gs[0..4].try_into().unwrap()) as usize;
        if length == 0 {
            return String::new();
        }
        if length <= SHORT_STRING_THRESHOLD {
            String::from_utf8_lossy(&gs[4..4 + length]).to_string()
        } else {
            let blob_offset = u64::from_le_bytes(gs[8..16].try_into().unwrap()) as usize;
            String::from_utf8_lossy(&batch.blob[blob_offset..blob_offset + length]).to_string()
        }
    }

    fn get_payload_i64(b: &Batch, row: usize) -> i64 {
        crate::util::read_i64_le(b.col_data(0), row * 8)
    }

    fn make_schema_compound() -> SchemaDescriptor {
        SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::I64, 0),
            ],
            &[0, 1],
        )
    }

    /// Pack a compound `(U64, U64)` PK into its 16-byte region: col0 then col1,
    /// each little-endian — the layout storage stores and orders by.
    fn compound_pk_bytes(c0: u64, c1: u64) -> [u8; 16] {
        let mut b = [0u8; 16];
        b[0..8].copy_from_slice(&c0.to_le_bytes());
        b[8..16].copy_from_slice(&c1.to_le_bytes());
        b
    }

    fn make_compound_batch(
        schema: &SchemaDescriptor,
        rows: &[(u64, u64, i64, i64)],
    ) -> ConsolidatedBatch {
        let mut b = Batch::with_schema(*schema, rows.len().max(1));
        for &(c0, c1, w, val) in rows {
            b.extend_pk_bytes(&compound_pk_bytes(c0, c1));
            b.extend_weight(&w.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &val.to_le_bytes());
            b.count += 1;
        }
        b.sorted = true;
        b.consolidated = true;
        ConsolidatedBatch::new_unchecked(b)
    }

    fn make_schema_signed() -> SchemaDescriptor {
        SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::I64, 0),
                SchemaColumn::new(type_code::I64, 0),
            ],
            &[0],
        )
    }

    fn make_signed_batch(
        schema: &SchemaDescriptor,
        rows: &[(i64, i64, i64)],
    ) -> ConsolidatedBatch {
        let mut b = Batch::with_schema(*schema, rows.len().max(1));
        for &(pk, w, val) in rows {
            // Signed PK at rest is OPK (big-endian, sign-bit flipped) so the
            // join's compare_pk_bytes merge orders it correctly; the raw
            // `extend_pk` would store non-order-preserving native bytes.
            b.extend_pk_opk(schema, &[(pk as u64) as u128]);
            b.extend_weight(&w.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &val.to_le_bytes());
            b.count += 1;
        }
        b.sorted = true;
        b.consolidated = true;
        ConsolidatedBatch::new_unchecked(b)
    }

    /// Decode a single signed-I64 PK at `row` from its OPK bytes to native.
    fn signed_pk_i64(b: &Batch, row: usize) -> i64 {
        let mut le = [0u8; 8];
        gnitz_wire::decode_pk_column(b.get_pk_bytes(row), type_code::I64, &mut le);
        i64::from_le_bytes(le)
    }

    // -----------------------------------------------------------------------
    // Anti-join DD tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_anti_join_dd_basic() {
        let schema = make_schema_u64_i64();
        let a = make_batch(&schema, &[(1, 1, 10), (2, 1, 20), (3, 1, 30)]);
        // B has PK=2 but different payload (200 vs 20) → payload-aware: no match
        let b = make_batch(&schema, &[(2, 1, 200)]);
        let out = op_anti_join_delta_delta(&a, &b, &schema);
        // All 3 rows survive (different payloads)
        assert_eq!(out.count, 3);
        assert!(out.consolidated);
    }

    #[test]
    fn test_anti_join_dd_basic_matching_payload() {
        let schema = make_schema_u64_i64();
        let a = make_batch(&schema, &[(1, 1, 10), (2, 1, 20), (3, 1, 30)]);
        // B has PK=2 with SAME payload (20) → exact match → excluded
        let b = make_batch(&schema, &[(2, 1, 20)]);
        let out = op_anti_join_delta_delta(&a, &b, &schema);
        assert_eq!(out.count, 2);
        assert_eq!((out.get_pk(0) as u64), 1);
        assert_eq!((out.get_pk(1) as u64), 3);
    }

    #[test]
    fn test_anti_join_dd_empty_right() {
        let schema = make_schema_u64_i64();
        let a = make_batch(&schema, &[(1, 1, 10), (2, 1, 20)]);
        let b = make_batch(&schema, &[]);
        let out = op_anti_join_delta_delta(&a, &b, &schema);
        assert_eq!(out.count, 2);
        assert_eq!((out.get_pk(0) as u64), 1);
        assert_eq!((out.get_pk(1) as u64), 2);
    }

    #[test]
    fn test_anti_join_dd_full_overlap() {
        let schema = make_schema_u64_i64();
        // Same PK AND same payload → full match → all excluded
        let a = make_batch(&schema, &[(1, 1, 10), (2, 1, 20)]);
        let b = make_batch(&schema, &[(1, 1, 10), (2, 1, 20)]);
        let out = op_anti_join_delta_delta(&a, &b, &schema);
        assert_eq!(out.count, 0);
    }

    #[test]
    fn test_anti_join_dd_negative_weight_b() {
        let schema = make_schema_u64_i64();
        let a = make_batch(&schema, &[(1, 1, 10), (2, 1, 20)]);
        // PK=1: B has negative weight → should NOT suppress A rows
        // PK=2: B has matching payload (20) with positive weight → excluded
        let b = make_batch(&schema, &[(1, -1, 10), (2, 1, 20)]);
        let out = op_anti_join_delta_delta(&a, &b, &schema);
        assert_eq!(out.count, 1);
        assert_eq!((out.get_pk(0) as u64), 1);
    }

    // -----------------------------------------------------------------------
    // Semi-join DD tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_semi_join_dd_basic() {
        let schema = make_schema_u64_i64();
        let a = make_batch(&schema, &[(1, 1, 10), (2, 1, 20), (3, 1, 30)]);
        let b = make_batch(&schema, &[(2, 1, 200), (3, 1, 300)]);
        let out = op_semi_join_delta_delta(&a, &b, &schema);
        assert_eq!(out.count, 2);
        assert_eq!((out.get_pk(0) as u64), 2);
        assert_eq!((out.get_pk(1) as u64), 3);
        assert!(out.consolidated);
    }

    #[test]
    fn test_anti_semi_complement_matching_payloads() {
        // When B has matching payloads, anti + semi = total
        let schema = make_schema_u64_i64();
        let a = make_batch(&schema, &[(1, 1, 10), (2, 1, 20), (3, 1, 30), (4, 1, 40)]);
        let b = make_batch(&schema, &[(2, 1, 20), (4, 1, 40)]);
        let total = a.count;
        let anti = op_anti_join_delta_delta(&a, &b, &schema);
        let semi = op_semi_join_delta_delta(&a, &b, &schema);
        assert_eq!(anti.count + semi.count, total);
    }

    // -----------------------------------------------------------------------
    // Join DD tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_join_dd_cartesian() {
        let left_schema = make_schema_u64_i64();
        let right_schema = make_schema_u64_i64();
        // PK=1: 2 rows in a, 3 rows in b → 6 output rows
        let a = make_batch(&left_schema, &[(1, 1, 10), (1, 2, 20)]);
        let b = make_batch(&right_schema, &[(1, 1, 100), (1, 3, 300), (1, -1, 500)]);
        let out = op_join_delta_delta(&a, &b, &left_schema, &right_schema);
        // 2 × 3 = 6, but check for zero-weight elimination:
        // (w=1*1=1), (w=1*3=3), (w=1*-1=-1), (w=2*1=2), (w=2*3=6), (w=2*-1=-2)
        // All non-zero → 6 rows
        assert_eq!(out.count, 6);
        assert!(out.sorted);

        // Check weights: row 0 = (a[0] × b[0]) = 1*1 = 1
        assert_eq!(out.get_weight(0), 1);
        // row 1 = (a[0] × b[1]) = 1*3 = 3
        assert_eq!(out.get_weight(1), 3);
    }

    #[test]
    fn test_join_dd_string_columns() {
        let left_schema = make_schema_u64_string();
        let right_schema = make_schema_u64_string();
        let a = make_batch_str(&left_schema, &[(1, 1, "hello")]);
        let b = make_batch_str(&right_schema, &[(1, 1, "world_longer_than_twelve")]);
        let out = op_join_delta_delta(&a, &b, &left_schema, &right_schema);
        assert_eq!(out.count, 1);
        // Left payload = col 0, right payload = col 1
        let left_str = read_str_payload(&out, 0, 0);
        let right_str = read_str_payload(&out, 1, 0);
        assert_eq!(left_str, "hello");
        assert_eq!(right_str, "world_longer_than_twelve");
    }

    #[test]
    fn test_join_dd_empty_inputs() {
        let ls = make_schema_u64_i64();
        let rs = make_schema_u64_i64();

        let out1 = op_join_delta_delta(&make_batch(&ls, &[]), &make_batch(&rs, &[(1, 1, 100)]), &ls, &rs);
        assert_eq!(out1.count, 0);

        let out2 = op_join_delta_delta(&make_batch(&rs, &[(1, 1, 100)]), &make_batch(&ls, &[]), &ls, &rs);
        assert_eq!(out2.count, 0);

        let out3 = op_join_delta_delta(&make_batch(&ls, &[]), &make_batch(&rs, &[]), &ls, &rs);
        assert_eq!(out3.count, 0);
    }

    #[test]
    fn test_join_dd_zero_weight_skip() {
        let ls = make_schema_u64_i64();
        let rs = make_schema_u64_i64();
        // a has weight=0 after consolidation wouldn't happen, but test the skip
        let a = make_batch(&ls, &[(1, 2, 10)]);
        // b has a pair that nets to zero weight after consolidation
        let mut b = Batch::with_schema(rs, 2);

        for &(pk, w, val) in &[(1u64, 1i64, 100i64), (1u64, -1i64, 100i64)] {
            b.extend_pk(pk as u128);
            b.extend_weight(&w.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &val.to_le_bytes());
            b.count += 1;
        }
        b.sorted = true;
        b.consolidated = false;

        let out = op_join_delta_delta(&a, &b, &ls, &rs);
        // b consolidates to empty (1 + -1 = 0 for same pk+payload) → no match
        assert_eq!(out.count, 0);
    }

    #[test]
    fn test_join_dd_wide_pk_works() {
        // Wide PK: 3× U64 = 24-byte stride. The byte-API port handles all widths
        // without panicking and produces correct Cartesian product output.
        let schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::I64, 0),
            ],
            &[0, 1, 2],
        );
        let mut a = Batch::with_schema(schema, 1);
        a.extend_pk_opk(&schema, &[1, 0, 0]);
        a.extend_weight(&1i64.to_le_bytes());
        a.extend_null_bmp(&0u64.to_le_bytes());
        a.extend_col(0, &7i64.to_le_bytes());
        a.count += 1;
        a.sorted = true;
        a.consolidated = true;
        let a = ConsolidatedBatch::new_unchecked(a);
        // Self-join: (1,0,0) × (1,0,0) → one output row with weight 1*1=1.
        let out = op_join_delta_delta(&a, &a, &schema, &schema);
        assert_eq!(out.count, 1);
        assert_eq!(out.get_weight(0), 1);
    }

    #[test]
    fn test_write_join_row_compound_pk_bytes() {
        // Narrow compound-PK left: 2× U64, pk_stride = 16, no payload.
        let left_schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::U64, 0),
            ],
            &[0, 1],
        );
        let right_schema = make_schema_u64_i64();

        // Known 16-byte PK region: pk0 = 0x1122..., pk1 = 0xAABB...
        let pk0: u64 = 0x1122_3344_5566_7788;
        let pk1: u64 = 0xAABB_CCDD_EEFF_0011;
        let mut pk_bytes = [0u8; 16];
        pk_bytes[0..8].copy_from_slice(&pk0.to_le_bytes());
        pk_bytes[8..16].copy_from_slice(&pk1.to_le_bytes());

        let mut left = Batch::with_schema(left_schema, 1);
        left.extend_pk_bytes(&pk_bytes);
        left.extend_weight(&1i64.to_le_bytes());
        left.extend_null_bmp(&0u64.to_le_bytes());
        left.count += 1;

        let right = make_batch(&right_schema, &[(7, 1, 42)]);

        // Output schema: 2 PK cols + right payload (1 I64) = 3 cols.
        let out_schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::I64, 0),
            ],
            &[0, 1],
        );
        let mut output = Batch::with_schema(out_schema, 1);

        write_join_row_from_batches(
            &mut output,
            &left.as_mem_batch(),
            0,
            &right.as_mem_batch(),
            0,
            1,
            &left_schema,
            &right_schema,
        );

        assert_eq!(output.count, 1);
        assert_eq!(output.get_pk_bytes(0), &pk_bytes);
    }

    // -----------------------------------------------------------------------
    // op_anti_join_delta_trace / op_semi_join_delta_trace
    // -----------------------------------------------------------------------

    #[test]
    fn test_anti_join_dt_basic() {
        use std::rc::Rc;
        use crate::storage::CursorHandle;

        let schema = make_schema_u64_i64();

        // Trace: pk=2 and pk=4 exist
        let trace = Rc::new(make_batch(&schema, &[(2, 1, 200), (4, 1, 400)]).into_inner());
        let mut ch = CursorHandle::from_owned(&[trace], schema);

        // Delta: pk=1,2,3
        let delta = make_batch(&schema, &[(1, 1, 10), (2, 1, 20), (3, 1, 30)]);

        let out = op_anti_join_delta_trace(&delta, ch.cursor_mut(), &schema);
        // pk=2 is in trace, so output should be pk=1 and pk=3
        assert_eq!(out.count, 2);
        assert_eq!((out.get_pk(0) as u64), 1);
        assert_eq!((out.get_pk(1) as u64), 3);
        assert!(out.consolidated);
    }

    #[test]
    fn test_semi_join_dt_basic() {
        use std::rc::Rc;
        use crate::storage::CursorHandle;

        let schema = make_schema_u64_i64();

        // Trace: pk=2 and pk=3 exist with positive weight
        let trace = Rc::new(make_batch(&schema, &[(2, 1, 200), (3, 1, 300)]).into_inner());
        let mut ch = CursorHandle::from_owned(&[trace], schema);

        // Delta: pk=1,2,3,4
        let delta = make_batch(&schema, &[
            (1, 1, 10), (2, 1, 20), (3, 1, 30), (4, 1, 40),
        ]);

        let out = op_semi_join_delta_trace(&delta, ch.cursor_mut(), &schema);
        // Only pk=2 and pk=3 have trace matches
        assert_eq!(out.count, 2);
        assert_eq!((out.get_pk(0) as u64), 2);
        assert_eq!((out.get_pk(1) as u64), 3);
    }

    /// Compound PK: the merge-walk advance loop compares trace PK vs delta PK.
    /// Raw u128 order is last-column-major, so without `current_pk_cmp` the
    /// scan mislocates and matches/anti-matches are wrong.
    #[test]
    fn test_semi_anti_join_dt_compound_pk() {
        use std::rc::Rc;
        use crate::storage::CursorHandle;

        let schema = make_schema_compound();
        // Trace (storage order): (1,5) and (2,3).
        let trace = Rc::new(make_compound_batch(&schema, &[(1, 5, 1, 100), (2, 3, 1, 200)]).into_inner());

        // Delta (storage order): (1,5), (2,3), (3,1).
        let delta = make_compound_batch(&schema, &[(1, 5, 1, 10), (2, 3, 1, 20), (3, 1, 1, 30)]);

        let mut ch = CursorHandle::from_owned(&[Rc::clone(&trace)], schema);
        let semi = op_semi_join_delta_trace(&delta, ch.cursor_mut(), &schema);
        assert_eq!(semi.count, 2, "semi: (1,5) and (2,3) match the trace");
        assert_eq!(semi.get_pk_bytes(0), &compound_pk_bytes(1, 5));
        assert_eq!(semi.get_pk_bytes(1), &compound_pk_bytes(2, 3));

        let mut ch2 = CursorHandle::from_owned(&[trace], schema);
        let anti = op_anti_join_delta_trace(&delta, ch2.cursor_mut(), &schema);
        assert_eq!(anti.count, 1, "anti: only (3,1) is absent from the trace");
        assert_eq!(anti.get_pk_bytes(0), &compound_pk_bytes(3, 1));
    }

    /// Signed single-column PK: negatives sort first in storage but last in raw
    /// u128, so the advance loop must use `current_pk_cmp`.
    #[test]
    fn test_semi_anti_join_dt_signed_pk() {
        use std::rc::Rc;
        use crate::storage::CursorHandle;

        let schema = make_schema_signed();
        // Trace (signed order): -3, -1.
        let trace = Rc::new(make_signed_batch(&schema, &[(-3, 1, 30), (-1, 1, 10)]).into_inner());

        // Delta (signed order): -3, -1, 2.
        let delta = make_signed_batch(&schema, &[(-3, 1, 1), (-1, 1, 2), (2, 1, 3)]);

        let mut ch = CursorHandle::from_owned(&[Rc::clone(&trace)], schema);
        let semi = op_semi_join_delta_trace(&delta, ch.cursor_mut(), &schema);
        assert_eq!(semi.count, 2, "semi: -3 and -1 match the trace");
        assert_eq!(signed_pk_i64(&semi, 0), -3);
        assert_eq!(signed_pk_i64(&semi, 1), -1);

        let mut ch2 = CursorHandle::from_owned(&[trace], schema);
        let anti = op_anti_join_delta_trace(&delta, ch2.cursor_mut(), &schema);
        assert_eq!(anti.count, 1, "anti: only 2 is absent from the trace");
        assert_eq!(signed_pk_i64(&anti, 0), 2);
    }

    #[test]
    fn test_semi_join_dt_nonconsolidated() {
        use std::rc::Rc;
        use crate::storage::CursorHandle;

        let schema = make_schema_u64_i64();

        // Trace: pk=1 exists
        let trace = Rc::new(make_batch(&schema, &[(1, 1, 100)]).into_inner());
        let mut ch = CursorHandle::from_owned(&[trace], schema);

        // Non-consolidated delta: pk=1 appears twice (w=2 and w=3)
        let mut delta = Batch::with_schema(schema, 2);
        for (pk, w, val) in [(1u64, 2i64, 10i64), (1u64, 3i64, 10i64)] {
            delta.extend_pk(pk as u128);
            delta.extend_weight(&w.to_le_bytes());
            delta.extend_null_bmp(&0u64.to_le_bytes());
            delta.extend_col(0, &val.to_le_bytes());
            delta.count += 1;
        }
        delta.sorted = true;
        delta.consolidated = false;

        let out = op_semi_join_delta_trace(&delta, ch.cursor_mut(), &schema);
        // After consolidation of delta: pk=1 w=5 → matches trace → 1 row
        assert_eq!(out.count, 1);
        assert_eq!((out.get_pk(0) as u64), 1);
        assert_eq!(out.get_weight(0), 5);
    }

    // -----------------------------------------------------------------------
    // op_join_delta_trace_outer
    // -----------------------------------------------------------------------

    #[test]
    fn test_outer_join_null_fill() {
        use std::rc::Rc;
        use crate::storage::CursorHandle;

        let left_schema = make_schema_u64_i64();
        let right_schema = make_schema_u64_i64();

        // Right trace: pk=1 val=100
        let trace = Rc::new(make_batch(&right_schema, &[(1, 1, 100)]).into_inner());
        let mut ch = CursorHandle::from_owned(&[trace], right_schema);

        // Delta (left): pk=1 val=10 (matches), pk=2 val=20 (no match → null fill)
        let delta = make_batch(&left_schema, &[(1, 1, 10), (2, 1, 20)]);

        let out = op_join_delta_trace_outer(&delta, ch.cursor_mut(), &left_schema, &right_schema);

        assert_eq!(out.count, 2);
        // pk=1: matched, left payload=10, right payload=100
        assert_eq!((out.get_pk(0) as u64), 1);
        assert_eq!(get_payload_i64(&out, 0), 10); // left val

        // pk=2: no match, left payload=20, right columns null-filled
        assert_eq!((out.get_pk(1) as u64), 2);
        assert_eq!(get_payload_i64(&out, 1), 20); // left val
        // Right column (payload col 1) should be null
        let null_word = u64::from_le_bytes(out.null_bmp_data()[8..16].try_into().unwrap());
        assert!(null_word & 2 != 0, "right payload column (bit 1) should be null for non-matching row");
    }

    fn make_schema_i32() -> SchemaDescriptor {
        SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::I32, 0),
                SchemaColumn::new(type_code::I64, 0),
            ],
            &[0],
        )
    }

    fn make_i32_batch(schema: &SchemaDescriptor, rows: &[(i32, i64, i64)]) -> ConsolidatedBatch {
        let mut b = Batch::with_schema(*schema, rows.len().max(1));
        for &(pk, w, val) in rows {
            b.extend_pk((pk as u32) as u128);
            b.extend_weight(&w.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &val.to_le_bytes());
            b.count += 1;
        }
        b.sorted = true;
        b.consolidated = true;
        ConsolidatedBatch::new_unchecked(b)
    }

    /// Inner join delta×trace on a narrow (I32, 4-byte) signed PK. Every delta
    /// key has a trace match; all must be found, including the smallest key
    /// (regression guard for the byte-seek path at sub-8-byte stride).
    #[test]
    fn test_join_dt_i32_key_all_match() {
        use std::rc::Rc;
        use crate::storage::CursorHandle;

        let left_schema = make_schema_i32();
        let right_schema = make_schema_i32();
        // Trace (right): keys 1 and 2, both present.
        let trace = Rc::new(make_i32_batch(&right_schema, &[(1, 1, 100), (2, 1, 200)]).into_inner());
        let mut ch = CursorHandle::from_owned(&[trace], right_schema);
        // Delta (left): keys 1 and 2.
        let delta = make_i32_batch(&left_schema, &[(1, 1, 10), (2, 1, 20)]);

        let out = op_join_delta_trace(&delta, ch.cursor_mut(), &left_schema, &right_schema);
        assert_eq!(out.count, 2, "both I32 keys must join (smallest key not dropped)");
        assert_eq!(out.get_pk(0) as i32, 1);
        assert_eq!(out.get_pk(1) as i32, 2);
    }

    #[test]
    fn test_join_dt_swapped_marks_output_unsorted() {
        use crate::storage::CursorHandle;
        use std::rc::Rc;
        // Many-to-many join with delta larger than the trace forces join_dt_swapped
        // (n=3 > trace_len=2). The swapped path emits the cartesian product in
        // trace-major order, interleaving left payloads out of (PK, payload) order.
        let schema = make_schema_u64_i64();
        // Trace: PK=1 with two right payloads (100, 200).
        let trace = Rc::new(make_batch(&schema, &[(1, 1, 100), (1, 1, 200)]).into_inner());
        let mut ch = CursorHandle::from_owned(&[trace], schema);
        // Delta: PK=1 with three left payloads.
        let delta = make_batch(&schema, &[(1, 1, 10), (1, 1, 20), (1, 1, 30)]).into_inner();

        let out = op_join_delta_trace(&delta, ch.cursor_mut(), &schema, &schema);
        assert_eq!(out.count, 6, "3 left × 2 right = 6 join outputs");

        // col 0 = left payload, col 1 = right payload (all PKs equal). The sorted
        // flag may be true ONLY if the rows are actually in (left, right) order.
        let pairs: Vec<(i64, i64)> = (0..out.count)
            .map(|r| (
                crate::util::read_i64_le(out.col_data(0), r * 8),
                crate::util::read_i64_le(out.col_data(1), r * 8),
            ))
            .collect();
        let actually_sorted = pairs.windows(2).all(|w| w[0] <= w[1]);
        assert!(
            !out.sorted || actually_sorted,
            "join_dt_swapped marked output sorted but payload order is {pairs:?}",
        );
    }

    // -----------------------------------------------------------------------
    // Anti/Semi-join DT: cursor re-seek for same-PK rows
    // -----------------------------------------------------------------------

    #[test]
    fn test_anti_join_dt_same_pk_different_payload() {
        use std::rc::Rc;
        use crate::storage::CursorHandle;

        let schema = make_schema_u64_i64();

        // Trace: PK=1 exists with positive weight
        let trace = Rc::new(make_batch(&schema, &[(1, 1, 100)]).into_inner());
        let mut ch = CursorHandle::from_owned(&[trace], schema);

        // Delta: two rows with PK=1 but different payloads (both should be matched)
        let delta = make_batch(&schema, &[(1, 1, 100), (1, 1, 200)]);

        let out = op_anti_join_delta_trace(&delta, ch.cursor_mut(), &schema);
        // Both rows have PK=1 which is in the trace → anti-join emits 0 rows
        assert_eq!(out.count, 0, "both same-PK rows should be matched by trace");
    }

    #[test]
    fn test_semi_join_dt_same_pk_different_payload() {
        use std::rc::Rc;
        use crate::storage::CursorHandle;

        let schema = make_schema_u64_i64();

        // Trace: PK=1 exists with positive weight
        let trace = Rc::new(make_batch(&schema, &[(1, 1, 100)]).into_inner());
        let mut ch = CursorHandle::from_owned(&[trace], schema);

        // Delta: two rows with PK=1 but different payloads (both should be matched)
        let delta = make_batch(&schema, &[(1, 1, 100), (1, 1, 200)]);

        let out = op_semi_join_delta_trace(&delta, ch.cursor_mut(), &schema);
        // Both rows have PK=1 which is in the trace → semi-join emits 2 rows
        assert_eq!(out.count, 2, "both same-PK rows should be emitted by semi-join");
    }

    // -----------------------------------------------------------------------
    // Semi-join DT: swapped-path weight inflation + u128::MAX sentinel
    // -----------------------------------------------------------------------

    #[test]
    fn test_semi_join_dt_swapped_max_pk() {
        use std::rc::Rc;
        use crate::storage::CursorHandle;

        let schema = make_schema_u64_i64();

        // Trace: PK=u64::MAX with positive weight — previously the sentinel
        // last_pk = u128::MAX would match this on first occurrence and skip it.
        let max_pk = u64::MAX;
        let trace = Rc::new(make_batch(&schema, &[(max_pk, 1, 0)]).into_inner());
        let mut ch = CursorHandle::from_owned(&[trace], schema);

        // Build 25 delta rows so n > trace_len triggers swapped path
        let mut rows: Vec<(u64, i64, i64)> = (1u64..=24).map(|i| (i, 1, i as i64)).collect();
        rows.push((max_pk, 1, 0));
        rows.sort_by_key(|r| r.0);
        let delta = make_batch(&schema, &rows);

        let out = op_semi_join_delta_trace(&delta, ch.cursor_mut(), &schema);
        // Only PK=u64::MAX from delta matches — must emit exactly 1 row
        assert_eq!(out.count, 1, "PK=u64::MAX must match on swapped path");
        assert_eq!((out.get_pk(0) as u64), max_pk);
    }

    #[test]
    fn test_semi_join_dt_swapped_no_weight_inflation() {
        use std::rc::Rc;
        use crate::storage::CursorHandle;

        let schema = make_schema_u64_i64();

        // Trace: 3 entries for PK=1 with different payloads
        // This triggers the trace-has-many scenario
        let trace = Rc::new(make_batch(&schema, &[
            (1, 1, 100), (1, 1, 200), (1, 1, 300),
        ]).into_inner());
        let mut ch = CursorHandle::from_owned(&[trace], schema);

        // Delta: 20+ rows to trigger n > trace_len swap path
        let mut rows: Vec<(u64, i64, i64)> = (1u64..=25).map(|pk| (pk, 1, pk as i64 * 10)).collect();
        rows.sort_by_key(|r| r.0);
        let delta = make_batch(&schema, &rows);

        let out = op_semi_join_delta_trace(&delta, ch.cursor_mut(), &schema);
        // Only PK=1 from delta matches — should emit exactly 1 row, not 3
        assert_eq!(out.count, 1, "PK=1 should appear once, not inflated by trace duplicates");
        assert_eq!((out.get_pk(0) as u64), 1);
    }

    // -----------------------------------------------------------------------
    // Anti-join DD: payload-aware matching
    // -----------------------------------------------------------------------

    #[test]
    fn test_anti_join_dd_same_pk_different_payload() {
        let schema = make_schema_u64_i64();
        // A: two rows with PK=1, different payloads
        let a = make_batch(&schema, &[(1, 1, 10), (1, 1, 20)]);
        // B: one row with PK=1, payload=20
        let b = make_batch(&schema, &[(1, 1, 20)]);
        let out = op_anti_join_delta_delta(&a, &b, &schema);
        // Only (PK=1, val=10) should survive — (PK=1, val=20) matches B exactly
        assert_eq!(out.count, 1);
        assert_eq!((out.get_pk(0) as u64), 1);
        assert_eq!(get_payload_i64(&out, 0), 10);
    }

    #[test]
    fn test_anti_join_dd_same_pk_same_payload() {
        let schema = make_schema_u64_i64();
        let a = make_batch(&schema, &[(1, 1, 10)]);
        let b = make_batch(&schema, &[(1, 1, 10)]);
        let out = op_anti_join_delta_delta(&a, &b, &schema);
        // Exact payload match → output should be empty
        assert_eq!(out.count, 0);
    }

    #[test]
    fn test_semi_join_dd_unchanged() {
        // Semi-join DD should still use PK-only semantics
        let schema = make_schema_u64_i64();
        let a = make_batch(&schema, &[(1, 1, 10), (1, 1, 20)]);
        let b = make_batch(&schema, &[(1, 1, 999)]); // different payload but same PK
        let out = op_semi_join_delta_delta(&a, &b, &schema);
        // PK-only: both A rows match B's PK → 2 rows
        assert_eq!(out.count, 2);
    }

    // -----------------------------------------------------------------------
    // BLOB payload copy in join writers (item 2)
    // -----------------------------------------------------------------------

    fn make_schema_u64_blob() -> SchemaDescriptor {
        SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::BLOB, 0),
            ],
            &[0],
        )
    }

    fn make_batch_blob(schema: &SchemaDescriptor, rows: &[(u64, i64, &[u8])]) -> ConsolidatedBatch {
        let mut b = Batch::with_schema(*schema, rows.len().max(1));
        for &(pk, w, bytes) in rows {
            b.extend_pk(pk as u128);
            b.extend_weight(&w.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            let length = bytes.len() as u32;
            let mut gs = [0u8; 16];
            gs[0..4].copy_from_slice(&length.to_le_bytes());
            if bytes.len() <= SHORT_STRING_THRESHOLD {
                gs[4..4 + bytes.len()].copy_from_slice(bytes);
            } else {
                gs[4..8].copy_from_slice(&bytes[..4]);
                let offset = b.blob.len() as u64;
                gs[8..16].copy_from_slice(&offset.to_le_bytes());
                b.blob.extend_from_slice(bytes);
            }
            b.extend_col(0, &gs);
            b.count += 1;
        }
        b.sorted = true;
        b.consolidated = true;
        ConsolidatedBatch::new_unchecked(b)
    }

    #[test]
    fn test_join_dd_blob_left_and_right_payload() {
        // BLOB payload columns on both sides of a DD join. write_left_payload
        // (left) and the right-side copy in write_join_row_from_batches must
        // relocate the blob, not copy the 16-byte struct verbatim.
        let left_schema = make_schema_u64_blob();
        let right_schema = make_schema_u64_blob();
        let left_blob: &[u8] = b"left-long-blob-value-beyond-twelve";
        let right_blob: &[u8] = b"right-long-blob-value-beyond-twelve";
        let a = make_batch_blob(&left_schema, &[(1, 1, left_blob)]);
        let b = make_batch_blob(&right_schema, &[(1, 1, right_blob)]);
        let out = op_join_delta_delta(&a, &b, &left_schema, &right_schema);
        assert_eq!(out.count, 1);
        assert!(!out.blob.is_empty(), "join output must carry blob buffer");
        // Left BLOB at out col 0, right BLOB at out col 1.
        let resolve = |col: usize| -> Vec<u8> {
            let gs = &out.col_data(col)[0..16];
            let len = u32::from_le_bytes(gs[0..4].try_into().unwrap()) as usize;
            let off = u64::from_le_bytes(gs[8..16].try_into().unwrap()) as usize;
            crate::schema::long_string_bytes(&out.blob, off, len).to_vec()
        };
        assert_eq!(resolve(0), left_blob, "left BLOB must resolve correctly");
        assert_eq!(resolve(1), right_blob, "right BLOB must resolve correctly");
    }

    // -----------------------------------------------------------------------
    // Stride consistency on early-exit empty join batches (item 3)
    // -----------------------------------------------------------------------

    #[test]
    fn test_join_dd_empty_stride_u64() {
        let ls = make_schema_u64_i64();
        let rs = make_schema_u64_i64();
        let out = op_join_delta_delta(&make_batch(&ls, &[]), &make_batch(&rs, &[(1, 1, 1)]), &ls, &rs);
        assert_eq!(out.count, 0);
        assert_eq!(out.pk_stride(), 8, "empty join output must use schema PK stride 8");
    }

    #[test]
    fn test_anti_join_dd_empty_stride_u64() {
        let schema = make_schema_u64_i64();
        let a = make_batch(&schema, &[]);
        let b = make_batch(&schema, &[(1, 1, 1)]);
        let out = op_anti_join_delta_delta(&a, &b, &schema);
        assert_eq!(out.count, 0);
        assert_eq!(out.pk_stride(), 8);
    }

    // -----------------------------------------------------------------------
    // Shift guards for 64-payload-column left schema (items 4b, 4c, 4d)
    // -----------------------------------------------------------------------

    fn make_wide_left_schema_64() -> SchemaDescriptor {
        let mut cols = vec![SchemaColumn::new(type_code::U64, 0)];
        for _ in 0..64 {
            cols.push(SchemaColumn::new(type_code::I64, 0));
        }
        SchemaDescriptor::new(&cols, &[0])
    }

    fn pk_only_schema() -> SchemaDescriptor {
        SchemaDescriptor::new(&[SchemaColumn::new(type_code::U64, 0)], &[0])
    }

    fn build_wide_left_row(left_schema: &SchemaDescriptor, pk: u64) -> Batch {
        let mut b = Batch::with_schema(*left_schema, 1);
        b.extend_pk(pk as u128);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        for pi in 0..left_schema.num_payload_cols() {
            b.extend_col(pi, &0i64.to_le_bytes());
        }
        b.count += 1;
        b
    }

    #[test]
    fn test_write_join_row_from_batches_shift_guard_64() {
        // left_npc == 64, right_npc == 0: `right_null << 64` panics in debug.
        let left_schema = make_wide_left_schema_64();
        assert_eq!(left_schema.num_payload_cols(), 64);
        let right_schema = pk_only_schema();
        let left = build_wide_left_row(&left_schema, 1);
        let mut right = Batch::with_schema(right_schema, 1);
        right.extend_pk(1u128);
        right.extend_weight(&1i64.to_le_bytes());
        right.extend_null_bmp(&0u64.to_le_bytes());
        right.count += 1;

        let mut output = Batch::with_schema(left_schema, 1); // PK + 64 payload = 65 cols
        write_join_row_from_batches(
            &mut output, &left.as_mem_batch(), 0, &right.as_mem_batch(), 0,
            1, &left_schema, &right_schema,
        );
        assert_eq!(output.count, 1);
    }

    #[test]
    fn test_write_join_row_null_right_shift_guard_64() {
        let left_schema = make_wide_left_schema_64();
        let right_schema = pk_only_schema();
        let left = build_wide_left_row(&left_schema, 1);
        let mut output = Batch::with_schema(left_schema, 1);
        write_join_row_null_right(
            &mut output, &left.as_mem_batch(), 0, 1, &left_schema, &right_schema,
        );
        assert_eq!(output.count, 1);
    }

    #[test]
    fn test_write_join_row_shift_guard_64() {
        use std::rc::Rc;
        use crate::storage::CursorHandle;
        let left_schema = make_wide_left_schema_64();
        let right_schema = pk_only_schema();
        let left = build_wide_left_row(&left_schema, 1);

        let mut trace = Batch::with_schema(right_schema, 1);
        trace.extend_pk(1u128);
        trace.extend_weight(&1i64.to_le_bytes());
        trace.extend_null_bmp(&0u64.to_le_bytes());
        trace.count += 1;
        trace.sorted = true;
        trace.consolidated = true;
        let trace = Rc::new(trace);
        let mut ch = CursorHandle::from_owned(&[trace], right_schema);
        let cursor = ch.cursor_mut();
        cursor.seek_bytes(&1u64.to_be_bytes());

        let mut output = Batch::with_schema(left_schema, 1);
        write_join_row(
            &mut output, &left.as_mem_batch(), 0, cursor, 1, &left_schema, &right_schema,
        );
        assert_eq!(output.count, 1);
    }

    // -----------------------------------------------------------------------
    // Anti-join DT uncompacted multi-entry trace (item 18)
    // -----------------------------------------------------------------------

    #[test]
    fn test_anti_join_dt_uncompacted_tombstone_then_live() {
        use std::rc::Rc;
        use crate::storage::CursorHandle;
        let schema = make_schema_u64_i64();
        // Trace for PK=1 has two entries (sorted by payload): a tombstone
        // (payload 100, w=-1) then a live row (payload 200, w=+1). Net: PK=1
        // is present. Anti-join must NOT emit the delta row for PK=1.
        let trace = Rc::new(make_batch(&schema, &[(1, -1, 100), (1, 1, 200)]).into_inner());
        let mut ch = CursorHandle::from_owned(&[trace], schema);
        let delta = make_batch(&schema, &[(1, 1, 5)]);
        let out = op_anti_join_delta_trace(&delta, ch.cursor_mut(), &schema);
        assert_eq!(out.count, 0, "PK=1 is in trace (net positive via second entry)");
    }

    #[test]
    fn test_semi_join_dt_uncompacted_tombstone_then_live() {
        use std::rc::Rc;
        use crate::storage::CursorHandle;
        let schema = make_schema_u64_i64();
        let trace = Rc::new(make_batch(&schema, &[(1, -1, 100), (1, 1, 200)]).into_inner());
        let mut ch = CursorHandle::from_owned(&[trace], schema);
        let delta = make_batch(&schema, &[(1, 1, 5)]);
        let out = op_semi_join_delta_trace(&delta, ch.cursor_mut(), &schema);
        assert_eq!(out.count, 1, "PK=1 has a positive-weight trace entry → emitted");
    }

    #[test]
    fn test_anti_join_dt_duplicate_pk_cached_no_reseek() {
        use std::rc::Rc;
        use crate::storage::CursorHandle;
        let schema = make_schema_u64_i64();
        // Trace: PK=1 present (positive).
        let trace = Rc::new(make_batch(&schema, &[(1, 1, 100)]).into_inner());
        let mut ch = CursorHandle::from_owned(&[trace], schema);
        // Two consolidated delta rows with same PK=1, distinct payloads.
        let delta = make_batch(&schema, &[(1, 1, 10), (1, 1, 20)]);
        let out = op_anti_join_delta_trace(&delta, ch.cursor_mut(), &schema);
        assert_eq!(out.count, 0, "both PK=1 rows excluded");
    }

    // -----------------------------------------------------------------------
    // Outer join uncompacted trace (item 25)
    // -----------------------------------------------------------------------

    #[test]
    fn test_outer_join_uncompacted_no_positive_entry() {
        use std::rc::Rc;
        use crate::storage::CursorHandle;
        let left_schema = make_schema_u64_i64();
        let right_schema = make_schema_u64_i64();
        // Trace has a single tombstone entry for PK=1 (w=-1). No positive
        // entry → the key is NOT in Distinct(trace) → null-fill must be emitted.
        let trace = Rc::new(make_batch(&right_schema, &[(1, -1, 100)]).into_inner());
        let mut ch = CursorHandle::from_owned(&[trace], right_schema);
        let delta = make_batch(&left_schema, &[(1, 1, 10)]);
        let out = op_join_delta_trace_outer(&delta, ch.cursor_mut(), &left_schema, &right_schema);
        // Expect: one inner row (w = 1 * -1 = -1) AND one null-extended row (w=+1).
        assert_eq!(out.count, 2, "inner row for the tombstone + null-fill (no positive match)");
        let mut weights: Vec<i64> = (0..out.count).map(|i| out.get_weight(i)).collect();
        weights.sort();
        assert_eq!(weights, vec![-1, 1]);
        // The null-filled row must have the right payload column null.
        let null_filled = (0..out.count).find(|&i| {
            let nw = u64::from_le_bytes(out.null_bmp_data()[i * 8..i * 8 + 8].try_into().unwrap());
            nw & 2 != 0
        });
        assert!(null_filled.is_some(), "a null-extended row must be present");
    }

    #[test]
    fn test_outer_join_uncompacted_positive_entry_no_null_fill() {
        use std::rc::Rc;
        use crate::storage::CursorHandle;
        let left_schema = make_schema_u64_i64();
        let right_schema = make_schema_u64_i64();
        // Trace for PK=1: a live entry (w=+1) and a tombstone (w=-1). The key
        // IS in Distinct(trace) → no null-fill.
        let trace = Rc::new(make_batch(&right_schema, &[(1, 1, 100), (1, -1, 200)]).into_inner());
        let mut ch = CursorHandle::from_owned(&[trace], right_schema);
        let delta = make_batch(&left_schema, &[(1, 1, 10)]);
        let out = op_join_delta_trace_outer(&delta, ch.cursor_mut(), &left_schema, &right_schema);
        assert_eq!(out.count, 2, "two inner rows, no null-fill");
        for i in 0..out.count {
            let nw = u64::from_le_bytes(out.null_bmp_data()[i * 8..i * 8 + 8].try_into().unwrap());
            assert_eq!(nw & 2, 0, "no row should have a null right column");
        }
    }

    // -----------------------------------------------------------------------
    // Delta-delta join signed PK ordering (item 39)
    // -----------------------------------------------------------------------

    #[test]
    fn test_join_dd_signed_pk_match_found() {
        // left=[2], right sorted signed [-3, 2]. Shared key = 2. Raw-u128
        // comparison advances the left pointer past 2 (since 2 < (-3 as u128))
        // and misses the match. compare_pk_bytes dispatch must find it.
        let ls = make_schema_signed();
        let rs = make_schema_signed();
        let a = make_signed_batch(&ls, &[(2, 1, 10)]);
        let b = make_signed_batch(&rs, &[(-3, 1, 30), (2, 1, 20)]);
        let out = op_join_delta_delta(&a, &b, &ls, &rs);
        assert_eq!(out.count, 1, "key 2 is shared and must join");
        assert_eq!(signed_pk_i64(&out, 0), 2);
        assert_eq!(get_payload_i64(&out, 0), 10);
    }

    #[test]
    fn test_semi_anti_join_dd_signed_pk() {
        let schema = make_schema_signed();
        // a sorted signed: -3, 2. b sorted signed: -3.
        let a = make_signed_batch(&schema, &[(-3, 1, 1), (2, 1, 2)]);
        let b = make_signed_batch(&schema, &[(-3, 1, 1)]);
        let semi = op_semi_join_delta_delta(&a, &b, &schema);
        assert_eq!(semi.count, 1, "only -3 shares a PK");
        assert_eq!(signed_pk_i64(&semi, 0), -3);
        let anti = op_anti_join_delta_delta(&a, &b, &schema);
        assert_eq!(anti.count, 1, "2 is not in b");
        assert_eq!(signed_pk_i64(&anti, 0), 2);
    }

    #[test]
    fn test_join_dd_compound_pk_match_found() {
        // Compound (c0,c1). left=[(1,0)], right sorted [(0,1),(1,0)]. Shared
        // key (1,0). Raw-u128 packs c1 high, so (1,0)=1 < (0,1)=1<<64; the
        // walk would advance left past the match. Byte dispatch finds it.
        let schema = make_schema_compound();
        let a = make_compound_batch(&schema, &[(1, 0, 1, 10)]);
        let b = make_compound_batch(&schema, &[(0, 1, 1, 30), (1, 0, 1, 20)]);
        let out = op_join_delta_delta(&a, &b, &schema, &schema);
        assert_eq!(out.count, 1, "compound key (1,0) is shared");
        assert_eq!(out.get_pk_bytes(0), &compound_pk_bytes(1, 0));
    }

    // -----------------------------------------------------------------------
    // Wide-PK test helpers
    // -----------------------------------------------------------------------

    /// Wide PK: 3 × U64 = 24-byte stride, exceeds the 16-byte u128.
    fn schema_wide_pk() -> SchemaDescriptor {
        SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::I64, 0),
            ],
            &[0u32, 1u32, 2u32],
        )
    }

    /// Build a wide-PK batch using OPK encoding (required for correct ordering).
    fn make_wide_batch(
        schema: &SchemaDescriptor,
        rows: &[(u64, u64, u64, i64, i64)], // (c0, c1, c2, weight, payload)
    ) -> ConsolidatedBatch {
        let mut b = Batch::with_schema(*schema, rows.len().max(1));
        for &(c0, c1, c2, w, val) in rows {
            b.extend_pk_opk(schema, &[c0 as u128, c1 as u128, c2 as u128]);
            b.extend_weight(&w.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &val.to_le_bytes());
            b.count += 1;
        }
        b.sorted = true;
        b.consolidated = true;
        ConsolidatedBatch::new_unchecked(b)
    }

    fn wide_pk_bytes(schema: &SchemaDescriptor, c0: u64, c1: u64, c2: u64) -> Vec<u8> {
        let mut tmp = Batch::with_schema(*schema, 1);
        tmp.extend_pk_opk(schema, &[c0 as u128, c1 as u128, c2 as u128]);
        tmp.get_pk_bytes(0).to_vec()
    }

    // -----------------------------------------------------------------------
    // Wide-PK join_dt_merge_walk tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_join_dt_merge_walk_wide_pk_multiset_delta() {
        use crate::storage::CursorHandle;
        use std::rc::Rc;
        // Two delta rows sharing the same wide PK but different payloads (multiset
        // delta). One trace row for that PK. Inner join must produce 2 output rows.
        // This exercises the pk_i == delta.get_pk_bytes(i - 1) re-seek branch.
        let schema = schema_wide_pk();
        let trace_batch = Rc::new(make_wide_batch(&schema, &[(1, 0, 0, 1, 100)]).into_inner());
        let mut ch = CursorHandle::from_owned(&[trace_batch], schema);

        let mut delta = Batch::with_schema(schema, 2);
        // Row 0: pk=(1,0,0) payload=10 w=+1
        delta.extend_pk_opk(&schema, &[1, 0, 0]);
        delta.extend_weight(&1i64.to_le_bytes());
        delta.extend_null_bmp(&0u64.to_le_bytes());
        delta.extend_col(0, &10i64.to_le_bytes());
        delta.count += 1;
        // Row 1: pk=(1,0,0) payload=20 w=+1 — same PK, different payload
        delta.extend_pk_opk(&schema, &[1, 0, 0]);
        delta.extend_weight(&1i64.to_le_bytes());
        delta.extend_null_bmp(&0u64.to_le_bytes());
        delta.extend_col(0, &20i64.to_le_bytes());
        delta.count += 1;
        delta.sorted = true;

        let out = op_join_delta_trace(&delta, ch.cursor_mut(), &schema, &schema);
        // 2 delta rows × 1 trace row = 2 output rows
        assert_eq!(out.count, 2, "multiset delta: expected 2 join outputs");
        assert_eq!(out.get_weight(0), 1);
        assert_eq!(out.get_weight(1), 1);
    }

    // -----------------------------------------------------------------------
    // Wide-PK join_dt_swapped prefix-collision test
    // -----------------------------------------------------------------------

    #[test]
    fn test_join_dt_swapped_wide_pk_prefix_collision() {
        use crate::storage::CursorHandle;
        use std::rc::Rc;
        // Two wide-PK rows sharing the same first 16 OPK bytes (columns 0 and 1
        // identical) but differing in column 2. The swapped path must treat them
        // as distinct keys, not conflate via u128 prefix.
        // Row A: pk=(1,1,2), Row B: pk=(1,1,9).
        // Trace: Row A (+1). Delta: Row A (+1) and Row B (+1).
        // Trigger swapped path: delta (2 rows) > trace (1 row).
        let schema = schema_wide_pk();
        let trace_batch = Rc::new(make_wide_batch(&schema, &[(1, 1, 2, 1, 100)]).into_inner());
        let mut ch = CursorHandle::from_owned(&[trace_batch], schema);

        let delta_cb = make_wide_batch(&schema, &[
            (1, 1, 2, 1, 200), // matches trace pk
            (1, 1, 9, 1, 300), // different pk — must NOT match
        ]);
        let delta = delta_cb.into_inner();

        let out = op_join_delta_trace(&delta, ch.cursor_mut(), &schema, &schema);
        // Only Row A matches (delta row 0 × trace row 0). Row B has no trace entry.
        assert_eq!(out.count, 1, "prefix-collision: only matching PK should produce output");
        assert_eq!(out.get_pk_bytes(0), wide_pk_bytes(&schema, 1, 1, 2).as_slice());
        assert_eq!(out.get_weight(0), 1);
    }

    // -----------------------------------------------------------------------
    // Wide-PK outer join multiset delta
    // -----------------------------------------------------------------------

    #[test]
    fn test_outer_join_dt_wide_pk_multiset_delta() {
        use crate::storage::CursorHandle;
        use std::rc::Rc;
        // Same multiset delta setup as merge_walk, but for outer join.
        let schema = schema_wide_pk();
        let trace_batch = Rc::new(make_wide_batch(&schema, &[(1, 0, 0, 1, 100)]).into_inner());
        let mut ch = CursorHandle::from_owned(&[trace_batch], schema);

        let mut delta = Batch::with_schema(schema, 2);
        delta.extend_pk_opk(&schema, &[1, 0, 0]);
        delta.extend_weight(&1i64.to_le_bytes());
        delta.extend_null_bmp(&0u64.to_le_bytes());
        delta.extend_col(0, &10i64.to_le_bytes());
        delta.count += 1;
        delta.extend_pk_opk(&schema, &[1, 0, 0]);
        delta.extend_weight(&1i64.to_le_bytes());
        delta.extend_null_bmp(&0u64.to_le_bytes());
        delta.extend_col(0, &20i64.to_le_bytes());
        delta.count += 1;
        delta.sorted = true;

        let out = op_join_delta_trace_outer(&delta, ch.cursor_mut(), &schema, &schema);
        // Both delta rows match → 2 joined output rows, no null-fills.
        assert_eq!(out.count, 2, "outer multiset: expected 2 matched outputs");
        for i in 0..out.count {
            assert_eq!(out.get_weight(i), 1);
            // PK must be (1,0,0)
            assert_eq!(out.get_pk_bytes(i), wide_pk_bytes(&schema, 1, 0, 0).as_slice());
        }
    }

    // -----------------------------------------------------------------------
    // Wide-PK anti/semi-join delta-trace
    // -----------------------------------------------------------------------

    #[test]
    fn test_anti_join_dt_wide_pk() {
        use crate::storage::CursorHandle;
        use std::rc::Rc;
        // Delta: (1,1,2) and (1,1,9). Trace: (1,1,9) only.
        // Anti-join: emit delta rows whose PK is NOT in trace → only (1,1,2).
        let schema = schema_wide_pk();
        let trace_batch = Rc::new(make_wide_batch(&schema, &[(1, 1, 9, 1, 50)]).into_inner());
        let mut ch = CursorHandle::from_owned(&[trace_batch], schema);

        let delta_cb = make_wide_batch(&schema, &[(1, 1, 2, 1, 10), (1, 1, 9, 1, 20)]);
        let delta = delta_cb.into_inner();
        let out = op_anti_join_delta_trace(&delta, ch.cursor_mut(), &schema);
        assert_eq!(out.count, 1);
        assert_eq!(out.get_pk_bytes(0), wide_pk_bytes(&schema, 1, 1, 2).as_slice());
    }

    #[test]
    fn test_semi_join_dt_wide_pk() {
        use crate::storage::CursorHandle;
        use std::rc::Rc;
        // Delta: (1,1,2) and (1,1,9). Trace: (1,1,9) only.
        // Semi-join: emit delta rows whose PK IS in trace → only (1,1,9).
        let schema = schema_wide_pk();
        let trace_batch = Rc::new(make_wide_batch(&schema, &[(1, 1, 9, 1, 50)]).into_inner());
        let mut ch = CursorHandle::from_owned(&[trace_batch], schema);

        let delta_cb = make_wide_batch(&schema, &[(1, 1, 2, 1, 10), (1, 1, 9, 1, 20)]);
        let delta = delta_cb.into_inner();
        let out = op_semi_join_delta_trace(&delta, ch.cursor_mut(), &schema);
        assert_eq!(out.count, 1);
        assert_eq!(out.get_pk_bytes(0), wide_pk_bytes(&schema, 1, 1, 9).as_slice());
    }

    // -----------------------------------------------------------------------
    // Wide-PK semi_join_dt_swapped — prefix collision and negative weight
    // -----------------------------------------------------------------------

    #[test]
    fn test_semi_join_dt_swapped_wide_pk_prefix_collision() {
        use crate::storage::CursorHandle;
        use std::rc::Rc;
        // Prefix-collision pair: same first 16 OPK bytes (c0=1,c1=1), differ
        // in c2. Trigger swapped path: make delta larger than trace.
        // Trace has (1,1,0) only. Delta has (1,1,0) and (1,1,1<<56).
        // Only (1,1,0) should be semi-joined.
        let schema = schema_wide_pk();
        let trace_batch = Rc::new(make_wide_batch(&schema, &[(1, 1, 0, 1, 99)]).into_inner());
        let mut ch = CursorHandle::from_owned(&[trace_batch], schema);

        // Large c2 differs starting at byte 17 of the OPK key.
        let c2_large = 1u64 << 56;
        let delta_cb = make_wide_batch(&schema, &[
            (1, 1, 0,        1, 10),
            (1, 1, c2_large, 1, 20),
            (2, 0, 0,        1, 30), // extra row to ensure n > trace_len=1
        ]);
        let delta = delta_cb.into_inner();
        let out = op_semi_join_delta_trace(&delta, ch.cursor_mut(), &schema);
        // Only (1,1,0) matches the trace.
        assert_eq!(out.count, 1);
        assert_eq!(out.get_pk_bytes(0), wide_pk_bytes(&schema, 1, 1, 0).as_slice());
    }

    #[test]
    fn test_semi_join_dt_swapped_wide_pk_negative_weight_trace() {
        use crate::storage::CursorHandle;
        use std::rc::Rc;
        // Trace entry for a wide PK with weight <= 0 must be skipped (no emit).
        let schema = schema_wide_pk();
        let trace_batch = Rc::new(make_wide_batch(&schema, &[(5, 5, 5, -1, 99)]).into_inner());
        let mut ch = CursorHandle::from_owned(&[trace_batch], schema);

        // Make delta larger so the swapped path is taken.
        let delta_cb = make_wide_batch(&schema, &[
            (5, 5, 5, 1, 10),
            (6, 6, 6, 1, 20),
        ]);
        let delta = delta_cb.into_inner();
        let out = op_semi_join_delta_trace(&delta, ch.cursor_mut(), &schema);
        // Negative-weight trace row must not produce any output.
        assert_eq!(out.count, 0, "negative-weight trace row must be skipped");
    }

    // -----------------------------------------------------------------------
    // Wide-PK delta-delta family
    // -----------------------------------------------------------------------

    #[test]
    fn test_join_dd_wide_pk_cartesian() {
        // Two wide-PK batches with two matching PKs.
        let schema = schema_wide_pk();
        let a = make_wide_batch(&schema, &[
            (1, 0, 0, 1, 10),
            (2, 0, 0, 1, 20),
        ]);
        let b = make_wide_batch(&schema, &[
            (1, 0, 0, 1, 100),
            (2, 0, 0, 1, 200),
        ]);
        let out = op_join_delta_delta(&a, &b, &schema, &schema);
        // Both PKs match: (1,0,0)×(1,0,0) and (2,0,0)×(2,0,0) → 2 output rows.
        assert_eq!(out.count, 2);
        assert_eq!(out.get_weight(0), 1);
        assert_eq!(out.get_weight(1), 1);
    }

    #[test]
    fn test_semi_join_dd_wide_pk() {
        // Wide-PK semi-join: A has (1,0,0) and (3,0,0); B has (1,0,0) only.
        // Output: only (1,0,0) from A.
        let schema = schema_wide_pk();
        let a = make_wide_batch(&schema, &[(1, 0, 0, 1, 10), (3, 0, 0, 1, 30)]);
        let b = make_wide_batch(&schema, &[(1, 0, 0, 1, 100)]);
        let out = op_semi_join_delta_delta(&a, &b, &schema);
        assert_eq!(out.count, 1);
        assert_eq!(out.get_pk_bytes(0), wide_pk_bytes(&schema, 1, 0, 0).as_slice());
    }

    #[test]
    fn test_anti_join_dd_wide_pk() {
        // Wide-PK payload-aware anti-join (EXCEPT): A row excluded only when B has
        // an exact (PK, payload) match with positive weight.
        // A has (1,0,0,payload=10) and (3,0,0,payload=30); B has (1,0,0,payload=10).
        // Output: only (3,0,0) from A (the (1,0,0) row is excluded by exact match).
        let schema = schema_wide_pk();
        let a = make_wide_batch(&schema, &[(1, 0, 0, 1, 10), (3, 0, 0, 1, 30)]);
        let b = make_wide_batch(&schema, &[(1, 0, 0, 1, 10)]);  // same payload as A's first row
        let out = op_anti_join_delta_delta(&a, &b, &schema);
        assert_eq!(out.count, 1);
        assert_eq!(out.get_pk_bytes(0), wide_pk_bytes(&schema, 3, 0, 0).as_slice());
    }

    // -----------------------------------------------------------------------
    // Signed narrow PK ordering (I64 negative values)
    // -----------------------------------------------------------------------

    #[test]
    fn test_join_dd_signed_narrow_negative_pks() {
        // Single-column I64 PK: -3, -1, 2. Two batches. Assert (-1,-1) matches.
        let schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::I64, 0),
                SchemaColumn::new(type_code::I64, 0),
            ],
            &[0],
        );
        let left = make_signed_batch(&schema, &[(-3, 1, 30), (-1, 1, 10), (2, 1, 20)]);
        let right = make_signed_batch(&schema, &[(-1, 1, 100), (4, 1, 400)]);
        let out = op_join_delta_delta(&left, &right, &schema, &schema);
        // Only -1 matches on both sides.
        assert_eq!(out.count, 1);
        assert_eq!(signed_pk_i64(&out, 0), -1i64);
        assert_eq!(out.get_weight(0), 1);
    }

    // -----------------------------------------------------------------------
    // Anti-join DD monotonic scan_b correctness (item 30)
    // -----------------------------------------------------------------------

    #[test]
    fn test_anti_join_dd_large_same_pk_group() {
        // Single PK group with many payloads. A's payloads 0..50 even; B's
        // payloads 0..50 even with positive weight. Anti-join keeps A rows
        // whose payload is NOT in B. Build A = payloads {0,2,4,...,98},
        // B = payloads {0,4,8,...,96}. Survivors: payloads in A not in B.
        let schema = make_schema_u64_i64();
        let a_rows: Vec<(u64, i64, i64)> = (0..50).map(|k| (1u64, 1i64, (k * 2) as i64)).collect();
        let b_rows: Vec<(u64, i64, i64)> = (0..25).map(|k| (1u64, 1i64, (k * 4) as i64)).collect();
        let a = make_batch(&schema, &a_rows);
        let b = make_batch(&schema, &b_rows);
        let out = op_anti_join_delta_delta(&a, &b, &schema);
        // Reference: A payload p survives iff p not in B (multiples of 4).
        let expected: Vec<i64> = (0..50)
            .map(|k| (k * 2) as i64)
            .filter(|p| p % 4 != 0)
            .collect();
        assert_eq!(out.count, expected.len());
        for (i, &p) in expected.iter().enumerate() {
            assert_eq!(get_payload_i64(&out, i), p, "row {i}");
        }
    }
}
