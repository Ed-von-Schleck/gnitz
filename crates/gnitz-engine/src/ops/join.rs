//! Join operators: anti-join, semi-join, inner join, outer join (delta-trace + delta-delta).

use std::cmp::Ordering;

use crate::schema::SchemaDescriptor;
use crate::schema::type_code::STRING as TYPE_STRING;
use crate::storage::{
    write_to_batch, Batch, ConsolidatedBatch, MemBatch, ReadCursor, scatter_copy,
    compare_rows, compare_rows_int_nonnull, schema_is_int_nonnull,
};

use super::util::{merge_null_words, write_string_from_batch};

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
#[allow(clippy::needless_range_loop)]
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
    assert!(!schema.pk_is_wide(),
        "op_anti_join_delta_trace: wide PK trace is unsupported (narrow u128 seek API)");
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
    assert!(!schema.pk_is_wide(),
        "op_semi_join_delta_trace: wide PK trace is unsupported (narrow u128 seek API)");
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

    let con_pks: Vec<u128> = (0..n).map(|i| consolidated.get_pk(i)).collect();
    let mut last_pk: Option<u128> = None;

    while cursor.valid {
        // `current_key` is the trace PK's native value (narrow) — injective, so
        // it works as the equality/dedup token. The ordered binary search below
        // routes through the OPK bytes, which order correctly at every width.
        let t_pk = cursor.current_key;

        if cursor.current_weight <= 0 {
            cursor.advance();
            continue;
        }

        // Skip duplicate PKs in trace (different payloads) — delta indices
        // were already pushed for the first occurrence of this PK.
        if last_pk == Some(t_pk) {
            cursor.advance();
            continue;
        }

        // Binary search in consolidated delta by the OPK bytes (raw memcmp).
        let pos = consolidated.find_lower_bound_bytes(cursor.current_pk_bytes());
        let mut j = pos;
        while j < n && con_pks[j] == t_pk {
            emit_indices.push(j as u32);
            j += 1;
        }
        last_pk = Some(t_pk);
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
    assert!(!left_schema.pk_is_wide(),
        "op_join_delta_trace: wide PK trace is unsupported (narrow u128 seek API)");
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

    join_dt_merge_walk(consolidated, cursor, left_schema, right_schema)
}

#[allow(clippy::needless_range_loop)]
fn join_dt_merge_walk(
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

    let pks: Vec<u128> = (0..n).map(|i| delta.get_pk(i)).collect();
    let mut prev_pk = pks[0];
    // Seek/compare by the delta row's OPK bytes directly (correct at every width).
    cursor.seek_bytes(delta.get_pk_bytes(0));

    for i in 0..n {
        let d_pk = pks[i];
        let w_delta = delta.get_weight(i);

        if i > 0 && prev_pk == d_pk {
            // Multiset delta: same PK, different payload — re-seek trace.
            cursor.seek_bytes(delta.get_pk_bytes(i));
        } else {
            while cursor.valid && cursor.current_pk_cmp_bytes(delta.get_pk_bytes(i)).is_lt() {
                cursor.advance();
            }
        }

        while cursor.valid && cursor.current_pk_cmp_bytes(delta.get_pk_bytes(i)) == Ordering::Equal {
            let w_trace = cursor.current_weight;
            let w_out = w_delta.wrapping_mul(w_trace);
            if w_out != 0 {
                write_join_row(
                    &mut output, &delta_mb, i, cursor, w_out,
                    left_schema, right_schema,
                );
            }
            cursor.advance();
        }

        prev_pk = d_pk;
    }

    output.sorted = true;
    output
}

#[allow(clippy::needless_range_loop)]
fn join_dt_swapped(
    delta: &Batch,
    cursor: &mut ReadCursor,
    left_schema: &SchemaDescriptor,
    right_schema: &SchemaDescriptor,
) -> Batch {
    let n = delta.count;

    let delta_mb = delta.as_mem_batch();
    let mut output = Batch::empty_joined(left_schema, right_schema);
    let delta_pks: Vec<u128> = (0..n).map(|i| delta.get_pk(i)).collect();

    while cursor.valid {
        // `current_key` is the trace PK's native value (narrow) — injective, a
        // valid equality token. The ordered search below goes by OPK bytes.
        let t_pk = cursor.current_key;
        let w_trace = cursor.current_weight;

        // Binary search in consolidated delta by the OPK bytes (raw memcmp).
        let pos = delta.find_lower_bound_bytes(cursor.current_pk_bytes());
        let mut j = pos;
        while j < n && delta_pks[j] == t_pk {
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

    output.sorted = true;
    output
}

// ---------------------------------------------------------------------------
// Outer join delta-trace
// ---------------------------------------------------------------------------

/// Left outer join: like inner join, but unmatched delta rows emit null-filled right side.
#[allow(clippy::needless_range_loop)]
pub fn op_join_delta_trace_outer(
    delta: &Batch,
    cursor: &mut ReadCursor,
    left_schema: &SchemaDescriptor,
    right_schema: &SchemaDescriptor,
) -> Batch {
    assert!(!left_schema.pk_is_wide(),
        "op_join_delta_trace_outer: wide PK trace is unsupported (narrow u128 seek API)");
    let left_npc = left_schema.num_payload_cols();
    let right_npc = right_schema.num_payload_cols();
    let out_npc = left_npc + right_npc;

    let cs = Batch::consolidate_if_needed(delta, left_schema);
    let consolidated: &Batch = cs.as_deref().unwrap_or(delta);
    let n = consolidated.count;
    if n == 0 {
        return Batch::empty(out_npc, left_schema.pk_stride());
    }

    let delta_mb = consolidated.as_mem_batch();
    let mut output = Batch::empty_joined(left_schema, right_schema);

    let pks: Vec<u128> = (0..n).map(|i| consolidated.get_pk(i)).collect();
    let mut prev_pk = pks[0];
    cursor.seek_bytes(consolidated.get_pk_bytes(0));

    for i in 0..n {
        let d_pk = pks[i];
        let w_delta = consolidated.get_weight(i);

        if i > 0 && prev_pk == d_pk {
            cursor.seek_bytes(consolidated.get_pk_bytes(i));
        } else {
            while cursor.valid && cursor.current_pk_cmp_bytes(consolidated.get_pk_bytes(i)).is_lt() {
                cursor.advance();
            }
        }

        // `matched` reflects membership in Distinct(trace): set it on any
        // positive-weight entry, not merely a non-zero join product. An
        // uncompacted trace may present a tombstone (weight ≤ 0) for the PK;
        // a tombstone alone must NOT suppress the null-fill.
        let mut matched = false;
        while cursor.valid && cursor.current_pk_cmp_bytes(consolidated.get_pk_bytes(i)) == Ordering::Equal {
            let w_trace = cursor.current_weight;
            if w_trace > 0 {
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

        if !matched {
            write_join_row_null_right(
                &mut output, &delta_mb, i, w_delta,
                left_schema, right_schema,
            );
        }

        prev_pk = d_pk;
    }

    output.sorted = true;
    output
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
        } else if col.type_code == TYPE_STRING || col.type_code == crate::schema::type_code::BLOB {
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
        } else if col.type_code == TYPE_STRING || col.type_code == crate::schema::type_code::BLOB {
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
    let right_null_bits = if right_npc < 64 {
        (1u64 << right_npc) - 1
    } else {
        u64::MAX
    };
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

    if schema_is_int_nonnull(schema) {
        filter_join_dd_with_payload_inner(ca, cb, schema,
            |s, a, ai, b, bi| compare_rows_int_nonnull(s, a, ai, b, bi))
    } else {
        filter_join_dd_with_payload_inner(ca, cb, schema,
            |s, a, ai, b, bi| compare_rows(s, a, ai, b, bi))
    }
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

    assert!(!schema.pk_is_wide(),
        "filter_join_dd: wide PK join is unsupported (narrow u128 key path)");
    let mb_a = ca.as_mem_batch();
    let mb_b = cb.as_mem_batch();
    // `get_pk` (widen_pk_be) is order-preserving for any narrow PK — unsigned,
    // signed, and compound alike — so the raw `u128` compare equals the typed
    // lexicographic PK order. Equality is injective for narrow keys.
    let pks_a: Vec<u128> = (0..n_a).map(|i| ca.get_pk(i)).collect();
    let pks_b: Vec<u128> = (0..n_b).map(|i| cb.get_pk(i)).collect();
    let mut idx_a = 0usize;
    let mut idx_b = 0usize;
    let mut output = Batch::empty_with_schema(schema);

    while idx_a < n_a {
        let key_a = pks_a[idx_a];

        // Advance B past every PK group ordered before key_a.
        while idx_b < n_b && pks_b[idx_b] < key_a {
            idx_b += 1;
        }

        // Extent of B's PK group matching key_a.
        let b_group_start = idx_b;
        let mut b_group_end = idx_b;
        while b_group_end < n_b && pks_b[b_group_end] == key_a {
            b_group_end += 1;
        }

        // Extent of A's PK group.
        let a_group_start = idx_a;
        let mut a_group_end = idx_a;
        while a_group_end < n_a && pks_a[a_group_end] == key_a {
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

    assert!(!schema.pk_is_wide(),
        "filter_join_dd: wide PK join is unsupported (narrow u128 key path)");
    // `get_pk` is order-preserving for any narrow PK, so the raw `u128` compare
    // equals canonical PK order; equality is injective for narrow keys.
    let pks_a: Vec<u128> = (0..n_a).map(|i| ca.get_pk(i)).collect();
    let pks_b: Vec<u128> = (0..n_b).map(|i| cb.get_pk(i)).collect();
    let mut idx_a = 0usize;
    let mut idx_b = 0usize;
    let mut output = Batch::empty_with_schema(schema);

    while idx_a < n_a {
        let key_a = pks_a[idx_a];

        // Advance B past every PK group ordered before key_a.
        while idx_b < n_b && pks_b[idx_b] < key_a {
            idx_b += 1;
        }

        let mut has_match = false;
        if idx_b < n_b && pks_b[idx_b] == key_a {
            let mut scan_b = idx_b;
            while scan_b < n_b && pks_b[scan_b] == key_a {
                if cb.get_weight(scan_b) > 0 {
                    has_match = true;
                    break;
                }
                scan_b += 1;
            }
        }

        let start_a = idx_a;
        while idx_a < n_a && pks_a[idx_a] == key_a {
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

    assert!(!left_schema.pk_is_wide(),
        "op_join_delta_delta: wide PK join is unsupported (narrow u128 key path)");
    let mb_a = ca.as_mem_batch();
    let mb_b = cb.as_mem_batch();
    // `get_pk` is order-preserving for any narrow PK (equi-join keys share a
    // width on both sides), so the raw `u128` compare equals canonical PK order;
    // equality is injective for narrow keys.
    let pks_a: Vec<u128> = (0..n_a).map(|i| ca.get_pk(i)).collect();
    let pks_b: Vec<u128> = (0..n_b).map(|i| cb.get_pk(i)).collect();
    let mut output = Batch::empty_joined(left_schema, right_schema);

    let mut idx_a = 0usize;
    let mut idx_b = 0usize;

    while idx_a < n_a && idx_b < n_b {
        let key_a = pks_a[idx_a];
        let key_b = pks_b[idx_b];

        match key_a.cmp(&key_b) {
            Ordering::Less => idx_a += 1,
            Ordering::Greater => idx_b += 1,
            Ordering::Equal => {
                let match_pk = key_a;

                let start_a = idx_a;
                idx_a += 1;
                while idx_a < n_a && pks_a[idx_a] == match_pk {
                    idx_a += 1;
                }

                let start_b = idx_b;
                idx_b += 1;
                while idx_b < n_b && pks_b[idx_b] == match_pk {
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
        } else if col.type_code == TYPE_STRING || col.type_code == crate::schema::type_code::BLOB {
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
    #[should_panic(expected = "wide PK join is unsupported")]
    fn test_join_dd_wide_pk_panics() {
        // Wide PK: 3× U64 compound = stride 24 (> 16). The delta-delta join's
        // `get_pk` precompute can't widen it; the `!pk_is_wide()` guard turns the
        // would-be slice-underflow into a clear diagnostic.
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
        let mut pk = [0u8; 24];
        pk[0..8].copy_from_slice(&1u64.to_le_bytes());
        a.extend_pk_bytes(&pk);
        a.extend_weight(&1i64.to_le_bytes());
        a.extend_null_bmp(&0u64.to_le_bytes());
        a.extend_col(0, &7i64.to_le_bytes());
        a.count += 1;
        a.sorted = true;
        a.consolidated = true;
        let a = ConsolidatedBatch::new_unchecked(a);
        let _ = op_join_delta_delta(&a, &a, &schema, &schema);
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
            assert_eq!(get_payload_i64(&out, i), p, "row {}", i);
        }
    }
}
