//! DBSP operators implemented entirely in Rust.
//!
//! Each function accepts OwnedBatch + ReadCursor handles and returns OwnedBatch
//! results — one FFI call per operator invocation, replacing per-row crossings.

use crate::compact::{SchemaDescriptor, SHORT_STRING_THRESHOLD, type_code};
use crate::memtable::{write_to_owned_batch, OwnedBatch};
use crate::merge::{self, MemBatch};
use crate::read_cursor::ReadCursor;

use type_code::STRING as TYPE_STRING;

// ---------------------------------------------------------------------------
// Distinct
// ---------------------------------------------------------------------------

/// DBSP distinct: converts multiset deltas into set-membership deltas.
///
/// Returns `(output_batch, consolidated_delta)`.
/// The consolidated delta is returned so the caller can feed it to `ingest_batch`.
pub fn op_distinct(
    delta: &OwnedBatch,
    cursor: &mut ReadCursor,
    schema: &SchemaDescriptor,
) -> (OwnedBatch, OwnedBatch) {
    let npc = schema.num_columns as usize - 1;

    // 1. Consolidate delta
    let consolidated = consolidate_owned(delta, schema);
    let n = consolidated.count;
    if n == 0 {
        return (OwnedBatch::empty(npc), consolidated);
    }

    // 2. Walk consolidated, collect emitting indices and weights
    let mut emit_indices: Vec<u32> = Vec::with_capacity(n);
    let mut emit_weights: Vec<i64> = Vec::with_capacity(n);

    for i in 0..n {
        let key_lo = consolidated.get_pk_lo(i);
        let key_hi = consolidated.get_pk_hi(i);
        let w_delta = consolidated.get_weight(i);

        cursor.seek(key_lo, key_hi);
        let w_old: i64 = if cursor.valid
            && cursor.current_key_lo == key_lo
            && cursor.current_key_hi == key_hi
        {
            cursor.current_weight
        } else {
            0
        };

        let s_old = signum(w_old);
        let w_new = w_old.wrapping_add(w_delta);
        let s_new = signum(w_new);
        let out_w = s_new - s_old;
        if out_w != 0 {
            emit_indices.push(i as u32);
            emit_weights.push(out_w as i64);
        }
    }

    // 3. Scatter-copy emitting rows
    if emit_indices.is_empty() {
        return (OwnedBatch::empty(npc), consolidated);
    }

    let mb = consolidated.as_mem_batch();
    let blob_cap = mb.blob.len().max(1);
    let mut output =
        write_to_owned_batch(schema, emit_indices.len(), blob_cap, |writer| {
            merge::scatter_copy(&mb, &emit_indices, &emit_weights, writer);
        });
    output.sorted = true;
    output.consolidated = true;
    output.schema = Some(*schema);

    (output, consolidated)
}

// ---------------------------------------------------------------------------
// Anti-join delta-trace
// ---------------------------------------------------------------------------

/// Emit delta rows whose key has NO positive-weight match in the trace.
pub fn op_anti_join_delta_trace(
    delta: &OwnedBatch,
    cursor: &mut ReadCursor,
    schema: &SchemaDescriptor,
) -> OwnedBatch {
    let npc = schema.num_columns as usize - 1;
    let consolidated = consolidate_owned(delta, schema);
    let n = consolidated.count;
    if n == 0 {
        return OwnedBatch::empty(npc);
    }

    let mut emit_indices: Vec<u32> = Vec::with_capacity(n);
    cursor.seek(consolidated.get_pk_lo(0), consolidated.get_pk_hi(0));

    for i in 0..n {
        let d_lo = consolidated.get_pk_lo(i);
        let d_hi = consolidated.get_pk_hi(i);

        while cursor.valid && pk_lt(cursor.current_key_lo, cursor.current_key_hi, d_lo, d_hi) {
            cursor.advance();
        }

        let in_trace = cursor.valid
            && cursor.current_key_lo == d_lo
            && cursor.current_key_hi == d_hi
            && cursor.current_weight > 0;

        if !in_trace {
            emit_indices.push(i as u32);
        }

        // Advance past current key even for negative-weight trace records
        if cursor.valid && cursor.current_key_lo == d_lo && cursor.current_key_hi == d_hi {
            cursor.advance();
        }
    }

    if emit_indices.is_empty() {
        return OwnedBatch::empty(npc);
    }

    let mb = consolidated.as_mem_batch();
    let blob_cap = mb.blob.len().max(1);
    let mut output =
        write_to_owned_batch(schema, emit_indices.len(), blob_cap, |writer| {
            merge::scatter_copy(&mb, &emit_indices, &[], writer);
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
    delta: &OwnedBatch,
    cursor: &mut ReadCursor,
    schema: &SchemaDescriptor,
) -> OwnedBatch {
    let npc = schema.num_columns as usize - 1;
    let consolidated = consolidate_owned(delta, schema);
    let n = consolidated.count;
    if n == 0 {
        return OwnedBatch::empty(npc);
    }

    let trace_len = cursor.estimated_length();
    if n > trace_len {
        // Adaptive swap: iterate trace, binary-search delta
        return semi_join_dt_swapped(&consolidated, cursor, schema);
    }

    // Merge-walk
    let mut emit_indices: Vec<u32> = Vec::with_capacity(n);
    cursor.seek(consolidated.get_pk_lo(0), consolidated.get_pk_hi(0));

    for i in 0..n {
        let d_lo = consolidated.get_pk_lo(i);
        let d_hi = consolidated.get_pk_hi(i);

        while cursor.valid && pk_lt(cursor.current_key_lo, cursor.current_key_hi, d_lo, d_hi) {
            cursor.advance();
        }

        let in_trace = cursor.valid
            && cursor.current_key_lo == d_lo
            && cursor.current_key_hi == d_hi
            && cursor.current_weight > 0;

        if in_trace {
            emit_indices.push(i as u32);
        }

        if cursor.valid && cursor.current_key_lo == d_lo && cursor.current_key_hi == d_hi {
            cursor.advance();
        }
    }

    if emit_indices.is_empty() {
        return OwnedBatch::empty(npc);
    }

    let mb = consolidated.as_mem_batch();
    let blob_cap = mb.blob.len().max(1);
    let mut output =
        write_to_owned_batch(schema, emit_indices.len(), blob_cap, |writer| {
            merge::scatter_copy(&mb, &emit_indices, &[], writer);
        });
    output.sorted = true;
    output.consolidated = true;
    output.schema = Some(*schema);
    output
}

fn semi_join_dt_swapped(
    consolidated: &OwnedBatch,
    cursor: &mut ReadCursor,
    schema: &SchemaDescriptor,
) -> OwnedBatch {
    let npc = schema.num_columns as usize - 1;
    let n = consolidated.count;
    let mut emit_indices: Vec<u32> = Vec::with_capacity(n);

    while cursor.valid {
        let t_lo = cursor.current_key_lo;
        let t_hi = cursor.current_key_hi;

        if cursor.current_weight <= 0 {
            cursor.advance();
            continue;
        }

        // Binary search in consolidated delta
        let pos = consolidated.find_lower_bound(t_lo, t_hi);
        let mut j = pos;
        while j < n && consolidated.get_pk_lo(j) == t_lo && consolidated.get_pk_hi(j) == t_hi {
            emit_indices.push(j as u32);
            j += 1;
        }
        cursor.advance();
    }

    if emit_indices.is_empty() {
        return OwnedBatch::empty(npc);
    }

    let mb = consolidated.as_mem_batch();
    let blob_cap = mb.blob.len().max(1);
    let mut output =
        write_to_owned_batch(schema, emit_indices.len(), blob_cap, |writer| {
            merge::scatter_copy(&mb, &emit_indices, &[], writer);
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
    delta: &OwnedBatch,
    cursor: &mut ReadCursor,
    left_schema: &SchemaDescriptor,
    right_schema: &SchemaDescriptor,
) -> OwnedBatch {
    let left_npc = left_schema.num_columns as usize - 1;
    let right_npc = right_schema.num_columns as usize - 1;
    let out_npc = left_npc + right_npc;

    let consolidated = consolidate_owned(delta, left_schema);
    let n = consolidated.count;
    if n == 0 {
        return OwnedBatch::empty(out_npc);
    }

    let trace_len = cursor.estimated_length();
    if n > trace_len {
        return join_dt_swapped(&consolidated, cursor, left_schema, right_schema);
    }

    join_dt_merge_walk(&consolidated, cursor, left_schema, right_schema)
}

fn join_dt_merge_walk(
    delta: &OwnedBatch,
    cursor: &mut ReadCursor,
    left_schema: &SchemaDescriptor,
    right_schema: &SchemaDescriptor,
) -> OwnedBatch {
    let left_npc = left_schema.num_columns as usize - 1;
    let right_npc = right_schema.num_columns as usize - 1;
    let out_npc = left_npc + right_npc;
    let n = delta.count;
    if n == 0 {
        return OwnedBatch::empty(out_npc);
    }

    let delta_mb = delta.as_mem_batch();
    let mut output = OwnedBatch::empty(out_npc);

    let mut prev_lo = delta.get_pk_lo(0);
    let mut prev_hi = delta.get_pk_hi(0);
    cursor.seek(prev_lo, prev_hi);

    for i in 0..n {
        let d_lo = delta.get_pk_lo(i);
        let d_hi = delta.get_pk_hi(i);
        let w_delta = delta.get_weight(i);

        if i > 0 && prev_lo == d_lo && prev_hi == d_hi {
            // Multiset delta: same PK, different payload — re-seek trace.
            cursor.seek(d_lo, d_hi);
        } else {
            while cursor.valid && pk_lt(cursor.current_key_lo, cursor.current_key_hi, d_lo, d_hi) {
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

        prev_lo = d_lo;
        prev_hi = d_hi;
    }

    output.sorted = true;
    output
}

fn join_dt_swapped(
    delta: &OwnedBatch,
    cursor: &mut ReadCursor,
    left_schema: &SchemaDescriptor,
    right_schema: &SchemaDescriptor,
) -> OwnedBatch {
    let left_npc = left_schema.num_columns as usize - 1;
    let right_npc = right_schema.num_columns as usize - 1;
    let out_npc = left_npc + right_npc;
    let n = delta.count;

    let delta_mb = delta.as_mem_batch();
    let mut output = OwnedBatch::empty(out_npc);

    while cursor.valid {
        let t_lo = cursor.current_key_lo;
        let t_hi = cursor.current_key_hi;
        let w_trace = cursor.current_weight;

        // Binary search in consolidated delta
        let pos = delta.find_lower_bound(t_lo, t_hi);
        let mut j = pos;
        while j < n && delta.get_pk_lo(j) == t_lo && delta.get_pk_hi(j) == t_hi {
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
    delta: &OwnedBatch,
    cursor: &mut ReadCursor,
    left_schema: &SchemaDescriptor,
    right_schema: &SchemaDescriptor,
) -> OwnedBatch {
    let left_npc = left_schema.num_columns as usize - 1;
    let right_npc = right_schema.num_columns as usize - 1;
    let out_npc = left_npc + right_npc;

    let consolidated = consolidate_owned(delta, left_schema);
    let n = consolidated.count;
    if n == 0 {
        return OwnedBatch::empty(out_npc);
    }

    let delta_mb = consolidated.as_mem_batch();
    let mut output = OwnedBatch::empty(out_npc);

    let mut prev_lo = consolidated.get_pk_lo(0);
    let mut prev_hi = consolidated.get_pk_hi(0);
    cursor.seek(prev_lo, prev_hi);

    for i in 0..n {
        let d_lo = consolidated.get_pk_lo(i);
        let d_hi = consolidated.get_pk_hi(i);
        let w_delta = consolidated.get_weight(i);

        if i > 0 && prev_lo == d_lo && prev_hi == d_hi {
            cursor.seek(d_lo, d_hi);
        } else {
            while cursor.valid && pk_lt(cursor.current_key_lo, cursor.current_key_hi, d_lo, d_hi) {
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

        prev_lo = d_lo;
        prev_hi = d_hi;
    }

    output.sorted = true;
    output
}

// ---------------------------------------------------------------------------
// Join row writer helpers
// ---------------------------------------------------------------------------

/// Write one composite join output row: [left_PK, left_payload..., right_payload...].
///
/// Left columns come from the delta MemBatch. Right columns come from the
/// cursor's current row, accessed via `col_ptr()` / `blob_ptr()`.
fn write_join_row(
    output: &mut OwnedBatch,
    left_batch: &MemBatch,
    left_row: usize,
    right_cursor: &ReadCursor,
    weight: i64,
    left_schema: &SchemaDescriptor,
    right_schema: &SchemaDescriptor,
) {
    let pk_lo = left_batch.get_pk_lo(left_row);
    let pk_hi = left_batch.get_pk_hi(left_row);
    let left_null = left_batch.get_null_word(left_row);
    let right_null = right_cursor.current_null_word;

    let left_npc = left_schema.num_columns as usize - 1;
    let null_word = left_null | (right_null << left_npc);

    output.pk_lo.extend_from_slice(&pk_lo.to_le_bytes());
    output.pk_hi.extend_from_slice(&pk_hi.to_le_bytes());
    output.weight.extend_from_slice(&weight.to_le_bytes());
    output.null_bmp.extend_from_slice(&null_word.to_le_bytes());

    let left_pk_index = left_schema.pk_index as usize;
    let right_pk_index = right_schema.pk_index as usize;

    // Left payload columns
    let mut pi = 0;
    for ci in 0..left_schema.num_columns as usize {
        if ci == left_pk_index {
            continue;
        }
        let col = &left_schema.columns[ci];
        let cs = col.size as usize;
        let is_null = (left_null >> pi) & 1 != 0;
        if is_null {
            let new_len = output.col_data[pi].len() + cs;
            output.col_data[pi].resize(new_len, 0);
        } else if col.type_code == TYPE_STRING {
            write_string_from_batch(&mut output.col_data[pi], &mut output.blob, left_batch, left_row, pi);
        } else {
            let src = left_batch.get_col_ptr(left_row, pi, cs);
            output.col_data[pi].extend_from_slice(src);
        }
        pi += 1;
    }

    // Right payload columns (from cursor public API)
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
            let new_len = output.col_data[out_pi].len() + cs;
            output.col_data[out_pi].resize(new_len, 0);
        } else if col.type_code == TYPE_STRING {
            // Read the 16-byte German String struct from cursor
            let ptr = right_cursor.col_ptr(ci, 16);
            if ptr.is_null() {
                let new_len = output.col_data[out_pi].len() + 16;
                output.col_data[out_pi].resize(new_len, 0);
            } else {
                let src = unsafe { std::slice::from_raw_parts(ptr, 16) };
                write_string_from_raw(
                    &mut output.col_data[out_pi], &mut output.blob, src, right_blob,
                );
            }
        } else {
            let ptr = right_cursor.col_ptr(ci, cs);
            if ptr.is_null() {
                let new_len = output.col_data[out_pi].len() + cs;
            output.col_data[out_pi].resize(new_len, 0);
            } else {
                let src = unsafe { std::slice::from_raw_parts(ptr, cs) };
                output.col_data[out_pi].extend_from_slice(src);
            }
        }
        rpi += 1;
    }

    output.count += 1;
}

/// Write one null-filled join output row: left columns from batch, right columns all NULL.
fn write_join_row_null_right(
    output: &mut OwnedBatch,
    left_batch: &MemBatch,
    left_row: usize,
    weight: i64,
    left_schema: &SchemaDescriptor,
    right_schema: &SchemaDescriptor,
) {
    let pk_lo = left_batch.get_pk_lo(left_row);
    let pk_hi = left_batch.get_pk_hi(left_row);
    let left_null = left_batch.get_null_word(left_row);

    let left_npc = left_schema.num_columns as usize - 1;
    let right_npc = right_schema.num_columns as usize - 1;
    // All right payload columns are NULL
    let right_null_bits = if right_npc < 64 {
        (1u64 << right_npc) - 1
    } else {
        u64::MAX
    };
    let null_word = left_null | (right_null_bits << left_npc);

    output.pk_lo.extend_from_slice(&pk_lo.to_le_bytes());
    output.pk_hi.extend_from_slice(&pk_hi.to_le_bytes());
    output.weight.extend_from_slice(&weight.to_le_bytes());
    output.null_bmp.extend_from_slice(&null_word.to_le_bytes());

    let left_pk_index = left_schema.pk_index as usize;
    let right_pk_index = right_schema.pk_index as usize;

    // Left payload columns
    let mut pi = 0;
    for ci in 0..left_schema.num_columns as usize {
        if ci == left_pk_index {
            continue;
        }
        let col = &left_schema.columns[ci];
        let cs = col.size as usize;
        let is_null = (left_null >> pi) & 1 != 0;
        if is_null {
            let new_len = output.col_data[pi].len() + cs;
            output.col_data[pi].resize(new_len, 0);
        } else if col.type_code == TYPE_STRING {
            write_string_from_batch(&mut output.col_data[pi], &mut output.blob, left_batch, left_row, pi);
        } else {
            let src = left_batch.get_col_ptr(left_row, pi, cs);
            output.col_data[pi].extend_from_slice(src);
        }
        pi += 1;
    }

    // Right payload columns: all zeros (null)
    let mut rpi = 0;
    for ci in 0..right_schema.num_columns as usize {
        if ci == right_pk_index {
            continue;
        }
        let col = &right_schema.columns[ci];
        let cs = col.size as usize;
        let out_pi = left_npc + rpi;
        let new_len = output.col_data[out_pi].len() + cs;
        output.col_data[out_pi].resize(new_len, 0);
        rpi += 1;
    }

    output.count += 1;
}

// ---------------------------------------------------------------------------
// String relocation helpers
// ---------------------------------------------------------------------------

/// Copy a German String from a MemBatch into the output OwnedBatch.
fn write_string_from_batch(
    col_buf: &mut Vec<u8>,
    blob: &mut Vec<u8>,
    batch: &MemBatch,
    row: usize,
    payload_col: usize,
) {
    let src = batch.get_col_ptr(row, payload_col, 16);
    write_string_from_raw(col_buf, blob, src, if batch.blob.is_empty() { std::ptr::null() } else { batch.blob.as_ptr() });
}

/// Copy a German String from raw 16-byte struct + blob base ptr into the output.
pub fn write_string_from_raw(
    col_buf: &mut Vec<u8>,
    blob: &mut Vec<u8>,
    src: &[u8],
    src_blob_ptr: *const u8,
) {
    let length = u32::from_le_bytes(src[0..4].try_into().unwrap()) as usize;

    let mut dest = [0u8; 16];
    dest[0..4].copy_from_slice(&src[0..4]); // length
    dest[4..8].copy_from_slice(&src[4..8]); // prefix

    if length <= SHORT_STRING_THRESHOLD {
        let suffix_len = if length > 4 { length - 4 } else { 0 };
        if suffix_len > 0 {
            dest[8..8 + suffix_len].copy_from_slice(&src[8..8 + suffix_len]);
        }
    } else if !src_blob_ptr.is_null() {
        let old_offset = u64::from_le_bytes(src[8..16].try_into().unwrap()) as usize;
        let src_data = unsafe { std::slice::from_raw_parts(src_blob_ptr.add(old_offset), length) };
        let new_offset = blob.len();
        blob.extend_from_slice(src_data);
        dest[8..16].copy_from_slice(&(new_offset as u64).to_le_bytes());
    }

    col_buf.extend_from_slice(&dest);
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn consolidate_owned(batch: &OwnedBatch, schema: &SchemaDescriptor) -> OwnedBatch {
    if batch.consolidated || batch.count == 0 {
        return batch.clone_batch();
    }
    let mb = batch.as_mem_batch();
    let blob_cap = mb.blob.len().max(1);
    let mut consolidated =
        write_to_owned_batch(schema, batch.count, blob_cap, |writer| {
            merge::sort_and_consolidate(&mb, schema, writer);
        });
    consolidated.sorted = true;
    consolidated.consolidated = true;
    consolidated.schema = Some(*schema);
    consolidated
}

#[inline]
fn signum(x: i64) -> i64 {
    if x > 0 { 1 } else if x < 0 { -1 } else { 0 }
}

#[inline]
fn pk_lt(a_lo: u64, a_hi: u64, b_lo: u64, b_hi: u64) -> bool {
    (a_hi, a_lo) < (b_hi, b_lo)
}
