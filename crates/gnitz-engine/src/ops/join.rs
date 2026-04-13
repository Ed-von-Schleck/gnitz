//! Join operators: anti-join, semi-join, inner join, outer join (delta-trace + delta-delta).

use crate::schema::SchemaDescriptor;
use crate::schema::type_code::STRING as TYPE_STRING;
use crate::storage::{write_to_batch, Batch, MemBatch, ReadCursor, scatter_copy};

use super::util::{consolidate_owned, write_string_from_batch, write_string_from_raw};

// ---------------------------------------------------------------------------
// Anti-join delta-trace
// ---------------------------------------------------------------------------

/// Emit delta rows whose key has NO positive-weight match in the trace.
pub fn op_anti_join_delta_trace(
    delta: &Batch,
    cursor: &mut ReadCursor,
    schema: &SchemaDescriptor,
) -> Batch {
    let npc = schema.num_columns as usize - 1;
    let consolidated = consolidate_owned(delta, schema);
    let n = consolidated.count;
    if n == 0 {
        return Batch::empty(npc);
    }

    let mut emit_indices: Vec<u32> = Vec::with_capacity(n);
    let pks: Vec<u128> = (0..n).map(|i| consolidated.get_pk(i)).collect();
    let mut prev_pk = pks[0];
    cursor.seek(prev_pk);

    for i in 0..n {
        let d_pk = pks[i];

        if i > 0 && d_pk == prev_pk {
            // Same PK as previous row (different payload) — re-seek cursor.
            cursor.seek(d_pk);
        } else {
            while cursor.valid && crate::util::make_pk(cursor.current_key_lo, cursor.current_key_hi) < d_pk {
                cursor.advance();
            }
        }

        let d_lo = d_pk as u64;
        let d_hi = (d_pk >> 64) as u64;
        let in_trace = cursor.valid
            && cursor.current_key_lo == d_lo
            && cursor.current_key_hi == d_hi
            && cursor.current_weight > 0;

        if !in_trace {
            emit_indices.push(i as u32);
        }

        prev_pk = d_pk;
    }

    if emit_indices.is_empty() {
        return Batch::empty(npc);
    }

    let mb = consolidated.as_mem_batch();
    let blob_cap = mb.blob.len().max(1);
    let mut output =
        write_to_batch(schema, emit_indices.len(), blob_cap, |writer| {
            scatter_copy(&mb, &emit_indices, &[], writer);
        });
    output.sorted = true;
    output.consolidated = true;
    output.schema = Some(*schema);
    output
}

// ---------------------------------------------------------------------------
// Semi-join delta-trace
// ---------------------------------------------------------------------------

/// Emit delta rows whose key HAS a positive-weight match in the trace.
pub fn op_semi_join_delta_trace(
    delta: &Batch,
    cursor: &mut ReadCursor,
    schema: &SchemaDescriptor,
) -> Batch {
    let npc = schema.num_columns as usize - 1;
    let consolidated = consolidate_owned(delta, schema);
    let n = consolidated.count;
    if n == 0 {
        return Batch::empty(npc);
    }

    let trace_len = cursor.estimated_length();
    if n > trace_len {
        // Adaptive swap: iterate trace, binary-search delta
        return semi_join_dt_swapped(&consolidated, cursor, schema);
    }

    // Merge-walk
    let mut emit_indices: Vec<u32> = Vec::with_capacity(n);
    let pks: Vec<u128> = (0..n).map(|i| consolidated.get_pk(i)).collect();
    let mut prev_pk = pks[0];
    cursor.seek(prev_pk);

    for i in 0..n {
        let d_pk = pks[i];

        if i > 0 && d_pk == prev_pk {
            // Same PK as previous row (different payload) — re-seek cursor.
            cursor.seek(d_pk);
        } else {
            while cursor.valid && crate::util::make_pk(cursor.current_key_lo, cursor.current_key_hi) < d_pk {
                cursor.advance();
            }
        }

        let d_lo = d_pk as u64;
        let d_hi = (d_pk >> 64) as u64;
        let in_trace = cursor.valid
            && cursor.current_key_lo == d_lo
            && cursor.current_key_hi == d_hi
            && cursor.current_weight > 0;

        if in_trace {
            emit_indices.push(i as u32);
        }

        prev_pk = d_pk;
    }

    if emit_indices.is_empty() {
        return Batch::empty(npc);
    }

    let mb = consolidated.as_mem_batch();
    let blob_cap = mb.blob.len().max(1);
    let mut output =
        write_to_batch(schema, emit_indices.len(), blob_cap, |writer| {
            scatter_copy(&mb, &emit_indices, &[], writer);
        });
    output.sorted = true;
    output.consolidated = true;
    output.schema = Some(*schema);
    output
}

fn semi_join_dt_swapped(
    consolidated: &Batch,
    cursor: &mut ReadCursor,
    schema: &SchemaDescriptor,
) -> Batch {
    let npc = schema.num_columns as usize - 1;
    let n = consolidated.count;
    let mut emit_indices: Vec<u32> = Vec::with_capacity(n);

    let con_pks: Vec<u128> = (0..n).map(|i| consolidated.get_pk(i)).collect();
    let mut last_pk: u128 = u128::MAX;

    while cursor.valid {
        let t_pk = crate::util::make_pk(cursor.current_key_lo, cursor.current_key_hi);

        if cursor.current_weight <= 0 {
            cursor.advance();
            continue;
        }

        // Skip duplicate PKs in trace (different payloads) — delta indices
        // were already pushed for the first occurrence of this PK.
        if t_pk == last_pk {
            cursor.advance();
            continue;
        }

        // Binary search in consolidated delta
        let pos = consolidated.find_lower_bound(t_pk);
        let mut j = pos;
        while j < n && con_pks[j] == t_pk {
            emit_indices.push(j as u32);
            j += 1;
        }
        last_pk = t_pk;
        cursor.advance();
    }

    if emit_indices.is_empty() {
        return Batch::empty(npc);
    }

    let mb = consolidated.as_mem_batch();
    let blob_cap = mb.blob.len().max(1);
    let mut output =
        write_to_batch(schema, emit_indices.len(), blob_cap, |writer| {
            scatter_copy(&mb, &emit_indices, &[], writer);
        });
    // Swapped order: output is in trace key order, which is sorted
    output.sorted = true;
    output.schema = Some(*schema);
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
    let left_npc = left_schema.num_columns as usize - 1;
    let right_npc = right_schema.num_columns as usize - 1;
    let out_npc = left_npc + right_npc;

    let consolidated = consolidate_owned(delta, left_schema);
    let n = consolidated.count;
    if n == 0 {
        return Batch::empty(out_npc);
    }

    let trace_len = cursor.estimated_length();
    if n > trace_len {
        return join_dt_swapped(&consolidated, cursor, left_schema, right_schema);
    }

    join_dt_merge_walk(&consolidated, cursor, left_schema, right_schema)
}

fn join_dt_merge_walk(
    delta: &Batch,
    cursor: &mut ReadCursor,
    left_schema: &SchemaDescriptor,
    right_schema: &SchemaDescriptor,
) -> Batch {
    let left_npc = left_schema.num_columns as usize - 1;
    let right_npc = right_schema.num_columns as usize - 1;
    let out_npc = left_npc + right_npc;
    let n = delta.count;
    if n == 0 {
        return Batch::empty(out_npc);
    }

    let delta_mb = delta.as_mem_batch();
    let mut output = Batch::empty_joined(left_schema, right_schema);

    let pks: Vec<u128> = (0..n).map(|i| delta.get_pk(i)).collect();
    let mut prev_pk = pks[0];
    cursor.seek(prev_pk);

    for i in 0..n {
        let d_pk = pks[i];
        let d_lo = d_pk as u64;
        let d_hi = (d_pk >> 64) as u64;
        let w_delta = delta.get_weight(i);

        if i > 0 && prev_pk == d_pk {
            // Multiset delta: same PK, different payload — re-seek trace.
            cursor.seek(d_pk);
        } else {
            while cursor.valid && crate::util::make_pk(cursor.current_key_lo, cursor.current_key_hi) < d_pk {
                cursor.advance();
            }
        }

        while cursor.valid && cursor.current_key_lo == d_lo && cursor.current_key_hi == d_hi {
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
        let t_pk = crate::util::make_pk(cursor.current_key_lo, cursor.current_key_hi);
        let w_trace = cursor.current_weight;

        // Binary search in consolidated delta
        let pos = delta.find_lower_bound(t_pk);
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
pub fn op_join_delta_trace_outer(
    delta: &Batch,
    cursor: &mut ReadCursor,
    left_schema: &SchemaDescriptor,
    right_schema: &SchemaDescriptor,
) -> Batch {
    let left_npc = left_schema.num_columns as usize - 1;
    let right_npc = right_schema.num_columns as usize - 1;
    let out_npc = left_npc + right_npc;

    let consolidated = consolidate_owned(delta, left_schema);
    let n = consolidated.count;
    if n == 0 {
        return Batch::empty(out_npc);
    }

    let delta_mb = consolidated.as_mem_batch();
    let mut output = Batch::empty_joined(left_schema, right_schema);

    let pks: Vec<u128> = (0..n).map(|i| consolidated.get_pk(i)).collect();
    let mut prev_pk = pks[0];
    cursor.seek(prev_pk);

    for i in 0..n {
        let d_pk = pks[i];
        let d_lo = d_pk as u64;
        let d_hi = (d_pk >> 64) as u64;
        let w_delta = consolidated.get_weight(i);

        if i > 0 && prev_pk == d_pk {
            cursor.seek(d_pk);
        } else {
            while cursor.valid && crate::util::make_pk(cursor.current_key_lo, cursor.current_key_hi) < d_pk {
                cursor.advance();
            }
        }

        let mut matched = false;
        while cursor.valid && cursor.current_key_lo == d_lo && cursor.current_key_hi == d_hi {
            let w_trace = cursor.current_weight;
            let w_out = w_delta.wrapping_mul(w_trace);
            if w_out != 0 {
                write_join_row(
                    &mut output, &delta_mb, i, cursor, w_out,
                    left_schema, right_schema,
                );
                matched = true;
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
    let left_pk_index = left_schema.pk_index as usize;
    let mut pi = 0;
    for ci in 0..left_schema.num_columns as usize {
        if ci == left_pk_index {
            continue;
        }
        let col = &left_schema.columns[ci];
        let cs = col.size as usize;
        let is_null = (left_null >> pi) & 1 != 0;
        if is_null {
            output.fill_col_zero(pi, cs);
        } else if col.type_code == TYPE_STRING {
            write_string_from_batch(output, pi, left_batch, left_row, pi);
        } else {
            let src = left_batch.get_col_ptr(left_row, pi, cs);
            output.extend_col(pi, src);
        }
        pi += 1;
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
    let pk = left_batch.get_pk(left_row);
    let pk_lo = pk as u64;
    let pk_hi = (pk >> 64) as u64;
    let left_null = left_batch.get_null_word(left_row);
    let right_null = right_cursor.current_null_word;

    let left_npc = left_schema.num_columns as usize - 1;
    let null_word = left_null | (right_null << left_npc);

    output.extend_pk_lo(&pk_lo.to_le_bytes());
    output.extend_pk_hi(&pk_hi.to_le_bytes());
    output.extend_weight(&weight.to_le_bytes());
    output.extend_null_bmp(&null_word.to_le_bytes());

    write_left_payload(output, left_batch, left_row, left_null, left_schema);

    // Right payload columns (from cursor public API)
    let right_pk_index = right_schema.pk_index as usize;
    let right_blob = right_cursor.blob_ptr();
    let mut rpi = 0;
    for ci in 0..right_schema.num_columns as usize {
        if ci == right_pk_index {
            continue;
        }
        let col = &right_schema.columns[ci];
        let cs = col.size as usize;
        let is_null = (right_null >> rpi) & 1 != 0;
        let out_pi = left_npc + rpi;
        if is_null {
            output.fill_col_zero(out_pi, cs);
        } else if col.type_code == TYPE_STRING {
            let ptr = right_cursor.col_ptr(ci, 16);
            if ptr.is_null() {
                output.fill_col_zero(out_pi, 16);
            } else {
                let src = unsafe { std::slice::from_raw_parts(ptr, 16) };
                let mut tmp = Vec::new();
                write_string_from_raw(
                    &mut tmp, &mut output.blob, src, right_blob,
                );
                output.extend_col(out_pi, &tmp);
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
        rpi += 1;
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
    let pk = left_batch.get_pk(left_row);
    let pk_lo = pk as u64;
    let pk_hi = (pk >> 64) as u64;
    let left_null = left_batch.get_null_word(left_row);

    let left_npc = left_schema.num_columns as usize - 1;
    let right_npc = right_schema.num_columns as usize - 1;
    let right_null_bits = if right_npc < 64 {
        (1u64 << right_npc) - 1
    } else {
        u64::MAX
    };
    let null_word = left_null | (right_null_bits << left_npc);

    output.extend_pk_lo(&pk_lo.to_le_bytes());
    output.extend_pk_hi(&pk_hi.to_le_bytes());
    output.extend_weight(&weight.to_le_bytes());
    output.extend_null_bmp(&null_word.to_le_bytes());

    write_left_payload(output, left_batch, left_row, left_null, left_schema);

    // Right payload columns: all zeros (null)
    let right_pk_index = right_schema.pk_index as usize;
    let mut rpi = 0;
    for ci in 0..right_schema.num_columns as usize {
        if ci == right_pk_index {
            continue;
        }
        let col = &right_schema.columns[ci];
        let cs = col.size as usize;
        let out_pi = left_npc + rpi;
        output.fill_col_zero(out_pi, cs);
        rpi += 1;
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
) -> Batch {
    filter_join_dd_with_payload(batch_a, batch_b, schema)
}

/// Emit batch_a rows whose PK HAS a positive-weight match in batch_b.
pub fn op_semi_join_delta_delta(
    batch_a: &Batch,
    batch_b: &Batch,
    schema: &SchemaDescriptor,
) -> Batch {
    filter_join_dd(batch_a, batch_b, schema, true)
}

/// Payload-aware anti-join DD: excludes A rows only when B has a matching
/// (PK, payload) with positive weight. Used for SQL EXCEPT.
fn filter_join_dd_with_payload(
    batch_a: &Batch,
    batch_b: &Batch,
    schema: &SchemaDescriptor,
) -> Batch {
    let npc = schema.num_columns as usize - 1;
    let ca = consolidate_owned(batch_a, schema);
    let cb = consolidate_owned(batch_b, schema);
    let n_a = ca.count;
    let n_b = cb.count;

    if n_a == 0 {
        return Batch::empty(npc);
    }
    if n_b == 0 {
        gnitz_debug!("op_anti_join_dd: a={} b=0 out={}", n_a, n_a);
        return ca;
    }

    let mb_a = ca.as_mem_batch();
    let mb_b = cb.as_mem_batch();
    let pks_a: Vec<u128> = (0..n_a).map(|i| ca.get_pk(i)).collect();
    let pks_b: Vec<u128> = (0..n_b).map(|i| cb.get_pk(i)).collect();
    let mut idx_a = 0usize;
    let mut idx_b = 0usize;
    let mut output = Batch::empty_with_schema(schema);

    while idx_a < n_a {
        let key_a = pks_a[idx_a];

        while idx_b < n_b && pks_b[idx_b] < key_a {
            idx_b += 1;
        }

        // Find extent of B's PK group
        let b_group_start = idx_b;
        let mut b_group_end = idx_b;
        if idx_b < n_b && pks_b[idx_b] == key_a {
            b_group_end = idx_b;
            while b_group_end < n_b && pks_b[b_group_end] == key_a {
                b_group_end += 1;
            }
        }

        // Process each A row individually — check payload match
        while idx_a < n_a && pks_a[idx_a] == key_a {
            let mut matched = false;
            for scan_b in b_group_start..b_group_end {
                if cb.get_weight(scan_b) > 0
                    && crate::storage::compare_rows(schema, &mb_a, idx_a, &mb_b, scan_b)
                        == std::cmp::Ordering::Equal
                {
                    matched = true;
                    break;
                }
            }
            if !matched {
                output.append_batch(&ca, idx_a, idx_a + 1);
            }
            idx_a += 1;
        }
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
    let npc = schema.num_columns as usize - 1;
    let ca = consolidate_owned(batch_a, schema);
    let cb = consolidate_owned(batch_b, schema);
    let n_a = ca.count;
    let n_b = cb.count;

    if n_a == 0 {
        return Batch::empty(npc);
    }
    if n_b == 0 {
        if emit_on_match {
            gnitz_debug!("op_semi_join_dd: a={} b=0 out=0", n_a);
            return Batch::empty(npc);
        } else {
            gnitz_debug!("op_anti_join_dd: a={} b=0 out={}", n_a, n_a);
            return ca;
        }
    }

    let pks_a: Vec<u128> = (0..n_a).map(|i| ca.get_pk(i)).collect();
    let pks_b: Vec<u128> = (0..n_b).map(|i| cb.get_pk(i)).collect();
    let mut idx_a = 0usize;
    let mut idx_b = 0usize;
    let mut output = Batch::empty_with_schema(schema);

    while idx_a < n_a {
        let key_a = pks_a[idx_a];

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
            output.append_batch(&ca, start_a, idx_a);
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
    let left_npc = left_schema.num_columns as usize - 1;
    let right_npc = right_schema.num_columns as usize - 1;
    let out_npc = left_npc + right_npc;

    let ca = consolidate_owned(batch_a, left_schema);
    let cb = consolidate_owned(batch_b, right_schema);
    let n_a = ca.count;
    let n_b = cb.count;

    if n_a == 0 || n_b == 0 {
        gnitz_debug!("op_join_dd: a={} b={} out=0", n_a, n_b);
        return Batch::empty(out_npc);
    }

    let mb_a = ca.as_mem_batch();
    let mb_b = cb.as_mem_batch();
    let pks_a: Vec<u128> = (0..n_a).map(|i| ca.get_pk(i)).collect();
    let pks_b: Vec<u128> = (0..n_b).map(|i| cb.get_pk(i)).collect();
    let mut output = Batch::empty_joined(left_schema, right_schema);

    let mut idx_a = 0usize;
    let mut idx_b = 0usize;

    while idx_a < n_a && idx_b < n_b {
        let key_a = pks_a[idx_a];
        let key_b = pks_b[idx_b];

        if key_a < key_b {
            idx_a += 1;
        } else if key_b < key_a {
            idx_b += 1;
        } else {
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

    output.sorted = true;
    gnitz_debug!("op_join_dd: a={} b={} out={}", n_a, n_b, output.count);
    output
}

/// Write one composite join output row where both sides come from MemBatch.
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
    let pk = left_batch.get_pk(left_row);
    let pk_lo = pk as u64;
    let pk_hi = (pk >> 64) as u64;
    let left_null = left_batch.get_null_word(left_row);
    let right_null = right_batch.get_null_word(right_row);

    let left_npc = left_schema.num_columns as usize - 1;
    let null_word = left_null | (right_null << left_npc);

    output.extend_pk_lo(&pk_lo.to_le_bytes());
    output.extend_pk_hi(&pk_hi.to_le_bytes());
    output.extend_weight(&weight.to_le_bytes());
    output.extend_null_bmp(&null_word.to_le_bytes());

    write_left_payload(output, left_batch, left_row, left_null, left_schema);

    // Right payload columns (from right MemBatch)
    let right_pk_index = right_schema.pk_index as usize;
    let mut rpi = 0;
    for ci in 0..right_schema.num_columns as usize {
        if ci == right_pk_index {
            continue;
        }
        let col = &right_schema.columns[ci];
        let cs = col.size as usize;
        let is_null = (right_null >> rpi) & 1 != 0;
        let out_pi = left_npc + rpi;
        if is_null {
            output.fill_col_zero(out_pi, cs);
        } else if col.type_code == TYPE_STRING {
            write_string_from_batch(
                output, out_pi,
                right_batch, right_row, rpi,
            );
        } else {
            let src = right_batch.get_col_ptr(right_row, rpi, cs);
            output.extend_col(out_pi, src);
        }
        rpi += 1;
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
    use crate::storage::Batch;

    fn make_schema_u64_i64() -> SchemaDescriptor {
        let mut columns = [SchemaColumn {
            type_code: 0, size: 0, nullable: 0, _pad: 0,
        }; 64];
        columns[0] = SchemaColumn {
            type_code: type_code::U64, size: 8, nullable: 0, _pad: 0,
        };
        columns[1] = SchemaColumn {
            type_code: type_code::I64, size: 8, nullable: 0, _pad: 0,
        };
        SchemaDescriptor { num_columns: 2, pk_index: 0, columns }
    }

    fn make_schema_u64_string() -> SchemaDescriptor {
        let mut columns = [SchemaColumn {
            type_code: 0, size: 0, nullable: 0, _pad: 0,
        }; 64];
        columns[0] = SchemaColumn {
            type_code: type_code::U64, size: 8, nullable: 0, _pad: 0,
        };
        columns[1] = SchemaColumn {
            type_code: type_code::STRING, size: 16, nullable: 0, _pad: 0,
        };
        SchemaDescriptor { num_columns: 2, pk_index: 0, columns }
    }

    fn make_batch(
        schema: &SchemaDescriptor,
        rows: &[(u64, i64, i64)],
    ) -> Batch {
        let n = rows.len();
        let mut b = Batch::with_schema(*schema, n.max(1));

        for &(pk, w, val) in rows {
            b.extend_pk_lo(&pk.to_le_bytes());
            b.extend_pk_hi(&0u64.to_le_bytes());
            b.extend_weight(&w.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &val.to_le_bytes());
            b.count += 1;
        }
        b.sorted = true;
        b.consolidated = true;
        b
    }

    fn make_batch_str(
        schema: &SchemaDescriptor,
        rows: &[(u64, i64, &str)],
    ) -> Batch {
        let n = rows.len();
        let mut b = Batch::with_schema(*schema, n.max(1));

        for &(pk, w, s) in rows {
            b.extend_pk_lo(&pk.to_le_bytes());
            b.extend_pk_hi(&0u64.to_le_bytes());
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
        b
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
        let anti = op_anti_join_delta_delta(&a, &b, &schema);
        let semi = op_semi_join_delta_delta(&a, &b, &schema);
        assert_eq!(anti.count + semi.count, a.count);
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
        let empty = make_batch(&ls, &[]);
        let nonempty = make_batch(&rs, &[(1, 1, 100)]);

        let out1 = op_join_delta_delta(&empty, &nonempty, &ls, &rs);
        assert_eq!(out1.count, 0);

        let out2 = op_join_delta_delta(&nonempty, &empty, &ls, &rs);
        assert_eq!(out2.count, 0);

        let out3 = op_join_delta_delta(&empty, &empty, &ls, &rs);
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
            b.extend_pk_lo(&pk.to_le_bytes());
            b.extend_pk_hi(&0u64.to_le_bytes());
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

    // -----------------------------------------------------------------------
    // op_anti_join_delta_trace / op_semi_join_delta_trace
    // -----------------------------------------------------------------------

    #[test]
    fn test_anti_join_dt_basic() {
        use std::sync::Arc;
        use crate::storage::CursorHandle;

        let schema = make_schema_u64_i64();

        // Trace: pk=2 and pk=4 exist
        let trace = Arc::new(make_batch(&schema, &[(2, 1, 200), (4, 1, 400)]));
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
        use std::sync::Arc;
        use crate::storage::CursorHandle;

        let schema = make_schema_u64_i64();

        // Trace: pk=2 and pk=3 exist with positive weight
        let trace = Arc::new(make_batch(&schema, &[(2, 1, 200), (3, 1, 300)]));
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

    #[test]
    fn test_semi_join_dt_nonconsolidated() {
        use std::sync::Arc;
        use crate::storage::CursorHandle;

        let schema = make_schema_u64_i64();

        // Trace: pk=1 exists
        let trace = Arc::new(make_batch(&schema, &[(1, 1, 100)]));
        let mut ch = CursorHandle::from_owned(&[trace], schema);

        // Non-consolidated delta: pk=1 appears twice (w=2 and w=3)
        let mut delta = Batch::with_schema(schema, 2);
        for (pk, w, val) in [(1u64, 2i64, 10i64), (1u64, 3i64, 10i64)] {
            delta.extend_pk_lo(&pk.to_le_bytes());
            delta.extend_pk_hi(&0u64.to_le_bytes());
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
        use std::sync::Arc;
        use crate::storage::CursorHandle;

        let left_schema = make_schema_u64_i64();
        let right_schema = make_schema_u64_i64();

        // Right trace: pk=1 val=100
        let trace = Arc::new(make_batch(&right_schema, &[(1, 1, 100)]));
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
        let null_word = u64::from_le_bytes(out.null_bmp_data()[1 * 8..2 * 8].try_into().unwrap());
        assert!(null_word & 2 != 0, "right payload column (bit 1) should be null for non-matching row");
    }

    // -----------------------------------------------------------------------
    // Fix 3: Anti/Semi-join DT cursor re-seek for same-PK rows
    // -----------------------------------------------------------------------

    #[test]
    fn test_anti_join_dt_same_pk_different_payload() {
        use std::sync::Arc;
        use crate::storage::CursorHandle;

        let schema = make_schema_u64_i64();

        // Trace: PK=1 exists with positive weight
        let trace = Arc::new(make_batch(&schema, &[(1, 1, 100)]));
        let mut ch = CursorHandle::from_owned(&[trace], schema);

        // Delta: two rows with PK=1 but different payloads (both should be matched)
        let delta = make_batch(&schema, &[(1, 1, 100), (1, 1, 200)]);

        let out = op_anti_join_delta_trace(&delta, ch.cursor_mut(), &schema);
        // Both rows have PK=1 which is in the trace → anti-join emits 0 rows
        assert_eq!(out.count, 0, "both same-PK rows should be matched by trace");
    }

    #[test]
    fn test_semi_join_dt_same_pk_different_payload() {
        use std::sync::Arc;
        use crate::storage::CursorHandle;

        let schema = make_schema_u64_i64();

        // Trace: PK=1 exists with positive weight
        let trace = Arc::new(make_batch(&schema, &[(1, 1, 100)]));
        let mut ch = CursorHandle::from_owned(&[trace], schema);

        // Delta: two rows with PK=1 but different payloads (both should be matched)
        let delta = make_batch(&schema, &[(1, 1, 100), (1, 1, 200)]);

        let out = op_semi_join_delta_trace(&delta, ch.cursor_mut(), &schema);
        // Both rows have PK=1 which is in the trace → semi-join emits 2 rows
        assert_eq!(out.count, 2, "both same-PK rows should be emitted by semi-join");
    }

    // -----------------------------------------------------------------------
    // Fix 4: Semi-join DT swapped weight inflation
    // -----------------------------------------------------------------------

    #[test]
    fn test_semi_join_dt_swapped_no_weight_inflation() {
        use std::sync::Arc;
        use crate::storage::CursorHandle;

        let schema = make_schema_u64_i64();

        // Trace: 3 entries for PK=1 with different payloads
        // This triggers the trace-has-many scenario
        let trace = Arc::new(make_batch(&schema, &[
            (1, 1, 100), (1, 1, 200), (1, 1, 300),
        ]));
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
    // Fix 7: Anti-join DD payload-aware matching
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
}
