//! DBSP operators implemented entirely in Rust.
//!
//! Each function accepts OwnedBatch + ReadCursor handles and returns OwnedBatch
//! results — one FFI call per operator invocation, replacing per-row crossings.

use crate::compact::{SchemaDescriptor, SHORT_STRING_THRESHOLD, type_code};
use crate::memtable::{write_to_owned_batch, OwnedBatch};
use crate::merge::{self, MemBatch};
use crate::read_cursor::ReadCursor;
use crate::scalar_func::ScalarFuncKind;
use crate::xxh;

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

/// Copy left-side payload columns from a MemBatch into the output.
/// Shared by all join row writers (inner, outer, DD).
fn write_left_payload(
    output: &mut OwnedBatch,
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
}

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
            let new_len = output.col_data[out_pi].len() + cs;
            output.col_data[out_pi].resize(new_len, 0);
        } else if col.type_code == TYPE_STRING {
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

// ---------------------------------------------------------------------------
// Linear operators
// ---------------------------------------------------------------------------

/// Filter: retain rows where predicate returns true.
/// Uses contiguous-range bulk copy for efficiency.
pub fn op_filter(
    batch: &OwnedBatch,
    func: &ScalarFuncKind,
    schema: &SchemaDescriptor,
) -> OwnedBatch {
    let n = batch.count;
    if n == 0 {
        let npc = schema.num_columns as usize - 1;
        return OwnedBatch::empty(npc);
    }

    let mb = batch.as_mem_batch();
    let mut output = OwnedBatch::with_schema(*schema, n);
    output.count = 0;

    let mut range_start: isize = -1;
    for i in 0..n {
        if func.evaluate_predicate(&mb, i, schema) {
            if range_start < 0 {
                range_start = i as isize;
            }
        } else {
            if range_start >= 0 {
                output.append_batch(batch, range_start as usize, i);
                range_start = -1;
            }
        }
    }
    if range_start >= 0 {
        output.append_batch(batch, range_start as usize, n);
    }

    if batch.consolidated {
        output.sorted = true;
        output.consolidated = true;
    } else {
        output.sorted = batch.sorted;
        output.consolidated = false;
    }

    gnitz_debug!("op_filter: in={} out={} func={}", n, output.count, func.kind_name());
    output
}

/// Map: transform batch via scalar function.
/// If reindex_col >= 0, computes new PK from that column value (GROUP BY).
pub fn op_map(
    batch: &OwnedBatch,
    func: &ScalarFuncKind,
    in_schema: &SchemaDescriptor,
    out_schema: &SchemaDescriptor,
    reindex_col: i32,
) -> OwnedBatch {
    if batch.count == 0 {
        let out_npc = out_schema.num_columns as usize - 1;
        return OwnedBatch::empty(out_npc);
    }

    if reindex_col < 0 {
        // Batch path
        let mut result = func.evaluate_map_batch(batch, in_schema, out_schema);
        result.sorted = batch.sorted;
        gnitz_debug!("op_map: in={} out={} reindex=-1 func={}", batch.count, result.count, func.kind_name());
        return result;
    }

    // Per-row path with reindex
    let in_mb = batch.as_mem_batch();
    let ri_col = reindex_col as usize;
    let mut output = OwnedBatch::with_schema(*out_schema, batch.count);
    output.count = 0;

    // First evaluate the map batch (without reindex) to get column data
    let mapped = func.evaluate_map_batch(batch, in_schema, out_schema);

    // Then overwrite PK with promoted values from reindex column
    output.pk_hi = vec![0u8; mapped.count * 8]; // will be overwritten
    output.pk_lo = vec![0u8; mapped.count * 8];
    output.weight = mapped.weight;
    output.null_bmp = mapped.null_bmp;
    output.col_data = mapped.col_data;
    output.blob = mapped.blob;
    output.count = mapped.count;

    for row in 0..batch.count {
        let (pk_lo, pk_hi) = promote_col_to_pk(&in_mb, row, ri_col, in_schema);
        output.pk_lo[row * 8..row * 8 + 8].copy_from_slice(&pk_lo.to_le_bytes());
        output.pk_hi[row * 8..row * 8 + 8].copy_from_slice(&pk_hi.to_le_bytes());
    }

    output.sorted = false;
    output.consolidated = false;
    gnitz_debug!("op_map: in={} out={} reindex={} func={}", batch.count, output.count, reindex_col, func.kind_name());
    output
}

/// Negate: flip the sign of every weight.
pub fn op_negate(batch: &OwnedBatch) -> OwnedBatch {
    if batch.count == 0 {
        return OwnedBatch::empty(batch.col_data.len());
    }

    let mut output = OwnedBatch::empty(batch.col_data.len());
    output.schema = batch.schema;
    output.append_batch_negated(batch, 0, batch.count);

    if batch.consolidated {
        output.sorted = true;
        output.consolidated = true;
    } else {
        output.sorted = batch.sorted;
        output.consolidated = false;
    }
    gnitz_debug!("op_negate: count={}", batch.count);
    output
}

/// Union: algebraic addition of two Z-Set streams.
/// When both inputs are sorted, performs O(N) merge preserving sort order.
pub fn op_union(
    batch_a: &OwnedBatch,
    batch_b: Option<&OwnedBatch>,
    schema: &SchemaDescriptor,
) -> OwnedBatch {
    let b = match batch_b {
        Some(b) if b.count > 0 => b,
        _ => {
            // Identity copy of batch_a
            let mut output = batch_a.clone_batch();
            if batch_a.consolidated {
                output.sorted = true;
                output.consolidated = true;
            } else {
                output.sorted = batch_a.sorted;
            }
            gnitz_debug!("op_union: a={} b=0 identity", batch_a.count);
            return output;
        }
    };

    if batch_a.sorted && b.sorted {
        return op_union_merge(batch_a, b, schema);
    }

    // Unsorted: concatenate
    let mut output = OwnedBatch::with_schema(*schema, batch_a.count + b.count);
    output.count = 0;
    output.append_batch(batch_a, 0, batch_a.count);
    output.append_batch(b, 0, b.count);
    output.sorted = false;
    output.consolidated = false;
    gnitz_debug!("op_union: a={} b={} out={} concat", batch_a.count, b.count, output.count);
    output
}

/// Sorted merge of two sorted batches with contiguous-run batching.
fn op_union_merge(
    batch_a: &OwnedBatch,
    batch_b: &OwnedBatch,
    schema: &SchemaDescriptor,
) -> OwnedBatch {
    let n_a = batch_a.count;
    let n_b = batch_b.count;
    let mut output = OwnedBatch::with_schema(*schema, n_a + n_b);
    output.count = 0;

    let mut i = 0usize;
    let mut j = 0usize;
    // run_src: 0=batch_a, 1=batch_b, -1=no current run
    let mut run_src: i32 = -1;
    let mut run_a_start = 0usize;
    let mut run_b_start = 0usize;

    while i < n_a && j < n_b {
        let a_lo = batch_a.get_pk_lo(i);
        let a_hi = batch_a.get_pk_hi(i);
        let b_lo = batch_b.get_pk_lo(j);
        let b_hi = batch_b.get_pk_hi(j);

        if pk_lt(a_lo, a_hi, b_lo, b_hi) {
            if run_src != 0 {
                if run_src == 1 {
                    output.append_batch(batch_b, run_b_start, j);
                }
                run_src = 0;
                run_a_start = i;
            }
            i += 1;
        } else if pk_lt(b_lo, b_hi, a_lo, a_hi) {
            if run_src != 1 {
                if run_src == 0 {
                    output.append_batch(batch_a, run_a_start, i);
                }
                run_src = 1;
                run_b_start = j;
            }
            j += 1;
        } else {
            // Equal keys: flush current run then emit one from each
            if run_src == 0 {
                output.append_batch(batch_a, run_a_start, i);
            } else if run_src == 1 {
                output.append_batch(batch_b, run_b_start, j);
            }
            run_src = -1;
            output.append_batch(batch_a, i, i + 1);
            output.append_batch(batch_b, j, j + 1);
            i += 1;
            j += 1;
        }
    }

    // Flush final run
    if run_src == 0 {
        output.append_batch(batch_a, run_a_start, i);
    } else if run_src == 1 {
        output.append_batch(batch_b, run_b_start, j);
    }

    // Remaining
    if i < n_a {
        output.append_batch(batch_a, i, n_a);
    }
    if j < n_b {
        output.append_batch(batch_b, j, n_b);
    }

    output.sorted = true;
    output.consolidated = false;
    gnitz_debug!("op_union: a={} b={} out={} sorted_merge", n_a, n_b, output.count);
    output
}

// ---------------------------------------------------------------------------
// PK promotion for reindex (GROUP BY)
// ---------------------------------------------------------------------------

/// Murmur3 64-bit finalizer.
#[inline]
fn mix64(mut v: u64) -> u64 {
    v ^= v >> 33;
    v = v.wrapping_mul(0xFF51AFD7ED558CCD);
    v ^= v >> 33;
    v = v.wrapping_mul(0xC4CEB9FE1A85EC53);
    v ^= v >> 33;
    v
}

/// Promote a column value to a (lo, hi) PK pair for reindexing.
fn promote_col_to_pk(
    batch: &MemBatch,
    row: usize,
    col_idx: usize,
    schema: &SchemaDescriptor,
) -> (u64, u64) {
    let tc = schema.columns[col_idx].type_code;
    let pki = schema.pk_index as usize;

    // If reindexing by the PK column itself, read from pk_lo/pk_hi directly.
    if col_idx == pki {
        let lo = crate::util::read_u64_le(batch.pk_lo, row * 8);
        let hi = crate::util::read_u64_le(batch.pk_hi, row * 8);
        return (lo, hi);
    }

    match tc {
        type_code::U128 => {
            let pi = if col_idx < pki { col_idx } else { col_idx - 1 };
            let ptr = batch.get_col_ptr(row, pi, 16);
            let lo = u64::from_le_bytes(ptr[0..8].try_into().unwrap());
            let hi = u64::from_le_bytes(ptr[8..16].try_into().unwrap());
            (lo, hi)
        }
        type_code::U64 | type_code::I64 => {
            let pi = if col_idx < pki { col_idx } else { col_idx - 1 };
            let ptr = batch.get_col_ptr(row, pi, 8);
            let val = u64::from_le_bytes(ptr.try_into().unwrap());
            (val, 0)
        }
        type_code::STRING => {
            let pi = if col_idx < pki { col_idx } else { col_idx - 1 };
            let struct_bytes = batch.get_col_ptr(row, pi, 16);
            let length = crate::util::read_u32_le(struct_bytes, 0) as usize;
            if length == 0 {
                return (0, 0);
            }
            // Hash the string bytes
            let h = if length <= SHORT_STRING_THRESHOLD {
                // Inline: prefix bytes at struct[4..4+length]
                xxh::checksum(&struct_bytes[4..4 + length])
            } else {
                let heap_offset =
                    u64::from_le_bytes(struct_bytes[8..16].try_into().unwrap()) as usize;
                xxh::checksum(&batch.blob[heap_offset..heap_offset + length])
            };
            let h_hi = mix64(h);
            (h, h_hi)
        }
        type_code::F64 | type_code::F32 => {
            let pi = if col_idx < pki { col_idx } else { col_idx - 1 };
            let ptr = batch.get_col_ptr(row, pi, 8);
            let bits = u64::from_le_bytes(ptr.try_into().unwrap());
            (bits, 0)
        }
        _ => {
            let pi = if col_idx < pki { col_idx } else { col_idx - 1 };
            let ptr = batch.get_col_ptr(row, pi, 8);
            let val = u64::from_le_bytes(ptr.try_into().unwrap());
            (val, 0)
        }
    }
}

// ---------------------------------------------------------------------------
// Anti-join / Semi-join delta-delta
// ---------------------------------------------------------------------------

/// Emit batch_a rows whose PK has NO positive-weight match in batch_b.
pub fn op_anti_join_delta_delta(
    batch_a: &OwnedBatch,
    batch_b: &OwnedBatch,
    schema: &SchemaDescriptor,
) -> OwnedBatch {
    filter_join_dd(batch_a, batch_b, schema, false)
}

/// Emit batch_a rows whose PK HAS a positive-weight match in batch_b.
pub fn op_semi_join_delta_delta(
    batch_a: &OwnedBatch,
    batch_b: &OwnedBatch,
    schema: &SchemaDescriptor,
) -> OwnedBatch {
    filter_join_dd(batch_a, batch_b, schema, true)
}

/// Shared merge-walk for anti-join and semi-join DD.
/// `emit_on_match=false` → anti-join, `emit_on_match=true` → semi-join.
fn filter_join_dd(
    batch_a: &OwnedBatch,
    batch_b: &OwnedBatch,
    schema: &SchemaDescriptor,
    emit_on_match: bool,
) -> OwnedBatch {
    let npc = schema.num_columns as usize - 1;
    let ca = consolidate_owned(batch_a, schema);
    let cb = consolidate_owned(batch_b, schema);
    let n_a = ca.count;
    let n_b = cb.count;

    if n_a == 0 {
        return OwnedBatch::empty(npc);
    }
    if n_b == 0 {
        if emit_on_match {
            gnitz_debug!("op_semi_join_dd: a={} b=0 out=0", n_a);
            return OwnedBatch::empty(npc);
        } else {
            gnitz_debug!("op_anti_join_dd: a={} b=0 out={}", n_a, n_a);
            return ca;
        }
    }

    let mut idx_a = 0usize;
    let mut idx_b = 0usize;
    let mut output = OwnedBatch::empty(npc);

    while idx_a < n_a {
        let key_a_lo = ca.get_pk_lo(idx_a);
        let key_a_hi = ca.get_pk_hi(idx_a);

        while idx_b < n_b && pk_lt(cb.get_pk_lo(idx_b), cb.get_pk_hi(idx_b), key_a_lo, key_a_hi) {
            idx_b += 1;
        }

        let mut has_match = false;
        if idx_b < n_b && cb.get_pk_lo(idx_b) == key_a_lo && cb.get_pk_hi(idx_b) == key_a_hi {
            let mut scan_b = idx_b;
            while scan_b < n_b
                && cb.get_pk_lo(scan_b) == key_a_lo
                && cb.get_pk_hi(scan_b) == key_a_hi
            {
                if cb.get_weight(scan_b) > 0 {
                    has_match = true;
                    break;
                }
                scan_b += 1;
            }
        }

        let start_a = idx_a;
        while idx_a < n_a
            && ca.get_pk_lo(idx_a) == key_a_lo
            && ca.get_pk_hi(idx_a) == key_a_hi
        {
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
    batch_a: &OwnedBatch,
    batch_b: &OwnedBatch,
    left_schema: &SchemaDescriptor,
    right_schema: &SchemaDescriptor,
) -> OwnedBatch {
    let left_npc = left_schema.num_columns as usize - 1;
    let right_npc = right_schema.num_columns as usize - 1;
    let out_npc = left_npc + right_npc;

    let ca = consolidate_owned(batch_a, left_schema);
    let cb = consolidate_owned(batch_b, right_schema);
    let n_a = ca.count;
    let n_b = cb.count;

    if n_a == 0 || n_b == 0 {
        gnitz_debug!("op_join_dd: a={} b={} out=0", n_a, n_b);
        return OwnedBatch::empty(out_npc);
    }

    let mb_a = ca.as_mem_batch();
    let mb_b = cb.as_mem_batch();
    let mut output = OwnedBatch::empty(out_npc);

    let mut idx_a = 0usize;
    let mut idx_b = 0usize;

    while idx_a < n_a && idx_b < n_b {
        let key_a_lo = ca.get_pk_lo(idx_a);
        let key_a_hi = ca.get_pk_hi(idx_a);
        let key_b_lo = cb.get_pk_lo(idx_b);
        let key_b_hi = cb.get_pk_hi(idx_b);

        if pk_lt(key_a_lo, key_a_hi, key_b_lo, key_b_hi) {
            idx_a += 1;
        } else if pk_lt(key_b_lo, key_b_hi, key_a_lo, key_a_hi) {
            idx_b += 1;
        } else {
            let match_lo = key_a_lo;
            let match_hi = key_a_hi;

            let start_a = idx_a;
            idx_a += 1;
            while idx_a < n_a
                && ca.get_pk_lo(idx_a) == match_lo
                && ca.get_pk_hi(idx_a) == match_hi
            {
                idx_a += 1;
            }

            let start_b = idx_b;
            idx_b += 1;
            while idx_b < n_b
                && cb.get_pk_lo(idx_b) == match_lo
                && cb.get_pk_hi(idx_b) == match_hi
            {
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
    output: &mut OwnedBatch,
    left_batch: &MemBatch,
    left_row: usize,
    right_batch: &MemBatch,
    right_row: usize,
    weight: i64,
    left_schema: &SchemaDescriptor,
    right_schema: &SchemaDescriptor,
) {
    let pk_lo = left_batch.get_pk_lo(left_row);
    let pk_hi = left_batch.get_pk_hi(left_row);
    let left_null = left_batch.get_null_word(left_row);
    let right_null = right_batch.get_null_word(right_row);

    let left_npc = left_schema.num_columns as usize - 1;
    let null_word = left_null | (right_null << left_npc);

    output.pk_lo.extend_from_slice(&pk_lo.to_le_bytes());
    output.pk_hi.extend_from_slice(&pk_hi.to_le_bytes());
    output.weight.extend_from_slice(&weight.to_le_bytes());
    output.null_bmp.extend_from_slice(&null_word.to_le_bytes());

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
            let new_len = output.col_data[out_pi].len() + cs;
            output.col_data[out_pi].resize(new_len, 0);
        } else if col.type_code == TYPE_STRING {
            write_string_from_batch(
                &mut output.col_data[out_pi],
                &mut output.blob,
                right_batch,
                right_row,
                rpi,
            );
        } else {
            let src = right_batch.get_col_ptr(right_row, rpi, cs);
            output.col_data[out_pi].extend_from_slice(src);
        }
        rpi += 1;
    }

    output.count += 1;
}

// ---------------------------------------------------------------------------
// Reduce operator: accumulator, group key, argsort, AVI, op_reduce,
// op_gather_reduce
// ---------------------------------------------------------------------------

// Aggregate opcodes (matches gnitz/dbsp/functions.py)
const AGG_COUNT: u8 = 1;
const AGG_SUM: u8 = 2;
const AGG_MIN: u8 = 3;
const AGG_MAX: u8 = 4;
const AGG_COUNT_NON_NULL: u8 = 5;

/// Descriptor for one aggregate function, passed across FFI.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct AggDescriptor {
    pub col_idx: u32,
    pub agg_op: u8,
    pub col_type_code: u8,
    pub _pad: [u8; 2],
}

const _: () = assert!(std::mem::size_of::<AggDescriptor>() == 8);
const _: () = assert!(std::mem::align_of::<AggDescriptor>() == 4);

/// Map logical column index to payload column index (skipping the PK column).
#[inline]
fn payload_idx(col_idx: usize, pk_index: usize) -> usize {
    if col_idx < pk_index { col_idx } else { col_idx - 1 }
}

/// Accumulator: internal state for one aggregate column.
struct Accumulator {
    acc: i64,
    has_value: bool,
    agg_op: u8,
    col_type_code: u8,
    col_idx: u32,
}

impl Accumulator {
    fn new(desc: &AggDescriptor) -> Self {
        Accumulator {
            acc: 0,
            has_value: false,
            agg_op: desc.agg_op,
            col_type_code: desc.col_type_code,
            col_idx: desc.col_idx,
        }
    }

    fn reset(&mut self) {
        self.acc = 0;
        self.has_value = false;
    }

    fn is_linear(&self) -> bool {
        self.agg_op == AGG_COUNT || self.agg_op == AGG_SUM || self.agg_op == AGG_COUNT_NON_NULL
    }

    fn is_zero(&self) -> bool {
        !self.has_value
    }

    fn get_value_bits(&self) -> u64 {
        self.acc as u64
    }

    fn seed_from_raw_bits(&mut self, bits: u64) {
        self.acc = bits as i64;
        self.has_value = true;
    }

    fn is_float(&self) -> bool {
        self.col_type_code == type_code::F64 || self.col_type_code == type_code::F32
    }

    /// Step: incorporate one input row into the accumulator.
    fn step_from_batch(
        &mut self,
        mb: &MemBatch,
        row: usize,
        schema: &SchemaDescriptor,
        weight: i64,
    ) {
        let col_idx = self.col_idx as usize;
        let pk_index = schema.pk_index as usize;

        // COUNT ignores nulls entirely
        if self.agg_op != AGG_COUNT {
            let payload_idx = payload_idx(col_idx, pk_index);
            let null_word = mb.get_null_word(row);
            if (null_word >> payload_idx) & 1 != 0 {
                return; // null value — skip
            }
        }

        let first = !self.has_value;
        self.has_value = true;
        let is_f = self.is_float();

        match self.agg_op {
            AGG_COUNT => {
                self.acc = self.acc.wrapping_add(weight);
            }
            AGG_COUNT_NON_NULL => {
                // Already checked null above
                self.acc = self.acc.wrapping_add(weight);
            }
            AGG_SUM => {
                let val = read_col_value(mb, row, col_idx, pk_index);
                if is_f {
                    let cur_f = f64::from_bits(self.acc as u64);
                    let val_f = f64::from_bits(val as u64);
                    let w_f = weight as f64;
                    self.acc = f64::to_bits(cur_f + val_f * w_f) as i64;
                } else {
                    let val_s = val as i64;
                    self.acc = self.acc.wrapping_add(val_s.wrapping_mul(weight));
                }
            }
            AGG_MIN => {
                let val = read_col_value(mb, row, col_idx, pk_index);
                if is_f {
                    let v = f64::from_bits(val as u64);
                    if first || v < f64::from_bits(self.acc as u64) {
                        self.acc = f64::to_bits(v) as i64;
                    }
                } else {
                    let v = val as i64;
                    if first || v < self.acc {
                        self.acc = v;
                    }
                }
            }
            AGG_MAX => {
                let val = read_col_value(mb, row, col_idx, pk_index);
                if is_f {
                    let v = f64::from_bits(val as u64);
                    if first || v > f64::from_bits(self.acc as u64) {
                        self.acc = f64::to_bits(v) as i64;
                    }
                } else {
                    let v = val as i64;
                    if first || v > self.acc {
                        self.acc = v;
                    }
                }
            }
            _ => {}
        }
    }

    /// Merge an already-accumulated value (from trace_out) into this accumulator.
    /// Only valid for linear aggregates.
    fn merge_accumulated(&mut self, value_bits: u64, weight: i64) {
        let is_f = self.is_float();
        match self.agg_op {
            AGG_COUNT | AGG_COUNT_NON_NULL => {
                let prev = value_bits as i64;
                self.acc = self.acc.wrapping_add(prev.wrapping_mul(weight));
                self.has_value = true;
            }
            AGG_SUM => {
                if is_f {
                    let prev_f = f64::from_bits(value_bits);
                    let w_f = weight as f64;
                    let cur_f = f64::from_bits(self.acc as u64);
                    self.acc = f64::to_bits(cur_f + prev_f * w_f) as i64;
                } else {
                    let prev = value_bits as i64;
                    self.acc = self.acc.wrapping_add(prev.wrapping_mul(weight));
                }
                self.has_value = true;
            }
            _ => {}
        }
    }

    /// Combine a partial aggregate from another shard.
    fn combine(&mut self, other_bits: u64) {
        let is_f = self.is_float();
        match self.agg_op {
            AGG_COUNT | AGG_COUNT_NON_NULL => {
                let prev = other_bits as i64;
                self.acc = self.acc.wrapping_add(prev);
                self.has_value = true;
            }
            AGG_SUM => {
                if is_f {
                    let prev_f = f64::from_bits(other_bits);
                    let cur_f = f64::from_bits(self.acc as u64);
                    self.acc = f64::to_bits(cur_f + prev_f) as i64;
                } else {
                    let prev = other_bits as i64;
                    self.acc = self.acc.wrapping_add(prev);
                }
                self.has_value = true;
            }
            AGG_MIN => {
                let first = !self.has_value;
                self.has_value = true;
                if is_f {
                    let other_f = f64::from_bits(other_bits);
                    if first || other_f < f64::from_bits(self.acc as u64) {
                        self.acc = f64::to_bits(other_f) as i64;
                    }
                } else {
                    let other_v = other_bits as i64;
                    if first || other_v < self.acc {
                        self.acc = other_v;
                    }
                }
            }
            AGG_MAX => {
                let first = !self.has_value;
                self.has_value = true;
                if is_f {
                    let other_f = f64::from_bits(other_bits);
                    if first || other_f > f64::from_bits(self.acc as u64) {
                        self.acc = f64::to_bits(other_f) as i64;
                    }
                } else {
                    let other_v = other_bits as i64;
                    if first || other_v > self.acc {
                        self.acc = other_v;
                    }
                }
            }
            _ => {}
        }
    }
}

/// Read a signed i64 column value from a MemBatch.
#[inline]
fn read_col_value(mb: &MemBatch, row: usize, col_idx: usize, pk_index: usize) -> i64 {
    let payload_idx = payload_idx(col_idx, pk_index);
    let ptr = mb.get_col_ptr(row, payload_idx, 8);
    i64::from_le_bytes(ptr.try_into().unwrap())
}

// ---------------------------------------------------------------------------
// Group key extraction
// ---------------------------------------------------------------------------

/// Extract 128-bit group key from a batch row.
fn extract_group_key(
    mb: &MemBatch,
    row: usize,
    schema: &SchemaDescriptor,
    group_by_cols: &[u32],
) -> (u64, u64) {
    let pki = schema.pk_index as usize;

    if group_by_cols.len() == 1 {
        let c_idx = group_by_cols[0] as usize;
        let tc = schema.columns[c_idx].type_code;
        if tc == type_code::U64 || tc == type_code::I64 {
            let pi = payload_idx(c_idx, pki);
            let ptr = mb.get_col_ptr(row, pi, 8);
            let v = u64::from_le_bytes(ptr.try_into().unwrap());
            return (v, 0);
        }
        if tc == type_code::U128 {
            let pi = payload_idx(c_idx, pki);
            let ptr = mb.get_col_ptr(row, pi, 16);
            let lo = u64::from_le_bytes(ptr[0..8].try_into().unwrap());
            let hi = u64::from_le_bytes(ptr[8..16].try_into().unwrap());
            return (lo, hi);
        }
    }

    let mut h: u64 = 0x9E3779B97F4A7C15; // golden ratio seed
    for (i, &c_idx_u32) in group_by_cols.iter().enumerate() {
        let c_idx = c_idx_u32 as usize;
        let tc = schema.columns[c_idx].type_code;
        let col_hash = if tc == TYPE_STRING {
            let pi = payload_idx(c_idx, pki);
            let struct_bytes = mb.get_col_ptr(row, pi, 16);
            let length = crate::util::read_u32_le(struct_bytes, 0) as usize;
            if length == 0 {
                0u64
            } else if length <= SHORT_STRING_THRESHOLD {
                xxh::checksum(&struct_bytes[4..4 + length])
            } else {
                let heap_offset = u64::from_le_bytes(struct_bytes[8..16].try_into().unwrap()) as usize;
                xxh::checksum(&mb.blob[heap_offset..heap_offset + length])
            }
        } else {
            let pi = payload_idx(c_idx, pki);
            let ptr = mb.get_col_ptr(row, pi, 8);
            let v = u64::from_le_bytes(ptr.try_into().unwrap());
            mix64(v)
        };
        h = mix64(h ^ col_hash ^ (i as u64));
    }
    let h_hi = mix64(h ^ (group_by_cols.len() as u64));
    (h, h_hi)
}

/// Extract 64-bit group key for AVI composite keys.
fn extract_gc_u64(
    mb: &MemBatch,
    row: usize,
    schema: &SchemaDescriptor,
    group_by_cols: &[u32],
) -> u64 {
    let pki = schema.pk_index as usize;
    if group_by_cols.len() == 1 {
        let c_idx = group_by_cols[0] as usize;
        let tc = schema.columns[c_idx].type_code;
        if tc != type_code::U128 && tc != TYPE_STRING
            && tc != type_code::F32 && tc != type_code::F64
        {
            let pi = payload_idx(c_idx, pki);
            let ptr = mb.get_col_ptr(row, pi, 8);
            return u64::from_le_bytes(ptr.try_into().unwrap());
        }
    }
    let (lo, _hi) = extract_group_key(mb, row, schema, group_by_cols);
    lo
}

/// IEEE 754 order-preserving encoding.
fn ieee_order_bits(raw_bits: u64) -> u64 {
    if raw_bits >> 63 != 0 {
        !raw_bits
    } else {
        raw_bits ^ (1u64 << 63)
    }
}

/// Reverse IEEE order-preserving encoding.
fn ieee_order_bits_reverse(encoded: u64) -> u64 {
    if encoded >> 63 != 0 {
        encoded ^ (1u64 << 63)
    } else {
        !encoded
    }
}

// ---------------------------------------------------------------------------
// Argsort
// ---------------------------------------------------------------------------

/// Compare two rows by group columns.
fn compare_by_group_cols(
    mb: &MemBatch,
    row_a: usize,
    row_b: usize,
    schema: &SchemaDescriptor,
    group_by_cols: &[u32],
) -> std::cmp::Ordering {
    let pki = schema.pk_index as usize;
    for &c_idx_u32 in group_by_cols {
        let c_idx = c_idx_u32 as usize;
        let tc = schema.columns[c_idx].type_code;
        let pi = payload_idx(c_idx, pki);

        let ord = if tc == TYPE_STRING {
            let a_bytes = mb.get_col_ptr(row_a, pi, 16);
            let b_bytes = mb.get_col_ptr(row_b, pi, 16);
            use crate::compact::compare_german_strings;
            compare_german_strings(a_bytes, mb.blob, b_bytes, mb.blob)
        } else if tc == type_code::F64 || tc == type_code::F32 {
            let cs = schema.columns[c_idx].size as usize;
            let a_ptr = mb.get_col_ptr(row_a, pi, cs);
            let b_ptr = mb.get_col_ptr(row_b, pi, cs);
            let a_f = f64::from_bits(u64::from_le_bytes(a_ptr[0..8].try_into().unwrap()));
            let b_f = f64::from_bits(u64::from_le_bytes(b_ptr[0..8].try_into().unwrap()));
            a_f.partial_cmp(&b_f).unwrap_or(std::cmp::Ordering::Equal)
        } else if tc == type_code::U128 {
            let a_ptr = mb.get_col_ptr(row_a, pi, 16);
            let b_ptr = mb.get_col_ptr(row_b, pi, 16);
            let a_lo = u64::from_le_bytes(a_ptr[0..8].try_into().unwrap());
            let a_hi = u64::from_le_bytes(a_ptr[8..16].try_into().unwrap());
            let b_lo = u64::from_le_bytes(b_ptr[0..8].try_into().unwrap());
            let b_hi = u64::from_le_bytes(b_ptr[8..16].try_into().unwrap());
            (a_hi, a_lo).cmp(&(b_hi, b_lo))
        } else if tc == type_code::I64 || tc == type_code::I32
            || tc == type_code::I16 || tc == type_code::I8
        {
            let a_ptr = mb.get_col_ptr(row_a, pi, 8);
            let b_ptr = mb.get_col_ptr(row_b, pi, 8);
            let a_v = i64::from_le_bytes(a_ptr.try_into().unwrap());
            let b_v = i64::from_le_bytes(b_ptr.try_into().unwrap());
            a_v.cmp(&b_v)
        } else {
            let a_ptr = mb.get_col_ptr(row_a, pi, 8);
            let b_ptr = mb.get_col_ptr(row_b, pi, 8);
            let a_v = u64::from_le_bytes(a_ptr.try_into().unwrap());
            let b_v = u64::from_le_bytes(b_ptr.try_into().unwrap());
            a_v.cmp(&b_v)
        };
        if ord != std::cmp::Ordering::Equal {
            return ord;
        }
    }
    std::cmp::Ordering::Equal
}

/// Argsort delta batch by group columns.
fn argsort_delta(
    batch: &OwnedBatch,
    schema: &SchemaDescriptor,
    group_by_cols: &[u32],
) -> Vec<u32> {
    let mb = batch.as_mem_batch();
    let n = batch.count;
    let mut indices: Vec<u32> = (0..n as u32).collect();
    if n <= 1 {
        return indices;
    }

    // Fast path: single I64 column
    let pki = schema.pk_index as usize;
    if group_by_cols.len() == 1 {
        let ci = group_by_cols[0] as usize;
        let tc = schema.columns[ci].type_code;
        if tc == type_code::I64 {
            let pi = payload_idx(ci, pki);
            let keys: Vec<i64> = (0..n)
                .map(|i| {
                    let ptr = mb.get_col_ptr(i, pi, 8);
                    i64::from_le_bytes(ptr.try_into().unwrap())
                })
                .collect();
            indices.sort_unstable_by_key(|&i| keys[i as usize]);
            return indices;
        }
    }

    indices.sort_unstable_by(|&a, &b| {
        compare_by_group_cols(&mb, a as usize, b as usize, schema, group_by_cols)
    });
    indices
}

/// Sort batch by (PK, payload) without consolidation.
/// Used by op_gather_reduce where we need to see each partial separately.
fn sort_owned(batch: &OwnedBatch, schema: &SchemaDescriptor) -> OwnedBatch {
    let n = batch.count;
    if n <= 1 || batch.sorted {
        return batch.clone_batch();
    }

    let mb = batch.as_mem_batch();
    let pks: Vec<u128> = (0..n).map(|i| mb.get_pk(i)).collect();
    let mut indices: Vec<u32> = (0..n as u32).collect();
    indices.sort_unstable_by(|&a, &b| {
        let ai = a as usize;
        let bi = b as usize;
        match pks[ai].cmp(&pks[bi]) {
            std::cmp::Ordering::Equal => {
                crate::columnar::compare_rows(schema, &mb, ai, &mb, bi)
            }
            ord => ord,
        }
    });

    // Scatter-copy in sorted order
    let mut output = OwnedBatch::with_schema(*schema, n);
    output.count = 0;
    for &idx in &indices {
        output.append_batch(batch, idx as usize, idx as usize + 1);
    }
    output.sorted = true;
    output
}

// ---------------------------------------------------------------------------
// AVI lookup
// ---------------------------------------------------------------------------

/// Seek AVI cursor to group, apply decoded min/max to accumulator.
fn apply_agg_from_value_index(
    avi_cursor: &mut ReadCursor,
    gc_u64: u64,
    for_max: bool,
    agg_col_type_code: u8,
    acc: &mut Accumulator,
) -> bool {
    avi_cursor.seek(0, gc_u64);
    while avi_cursor.valid {
        let k_hi = avi_cursor.current_key_hi;
        if k_hi != gc_u64 {
            break;
        }
        if avi_cursor.current_weight > 0 {
            let mut encoded = avi_cursor.current_key_lo;
            if for_max {
                encoded = !encoded;
            }
            let tc = agg_col_type_code;
            if tc == type_code::I64 || tc == type_code::I32
                || tc == type_code::I16 || tc == type_code::I8
            {
                encoded = (encoded as i64).wrapping_sub(1i64 << 63) as u64;
            } else if tc == type_code::F64 || tc == type_code::F32 {
                encoded = ieee_order_bits_reverse(encoded);
            }
            acc.seed_from_raw_bits(encoded);
            return true;
        }
        avi_cursor.advance();
    }
    acc.reset();
    false
}

// ---------------------------------------------------------------------------
// op_reduce
// ---------------------------------------------------------------------------

/// Incremental DBSP REDUCE: δ_out = Agg(history + δ_in) - Agg(history).
pub fn op_reduce(
    delta: &OwnedBatch,
    trace_in_cursor: Option<&mut ReadCursor>,
    trace_out_cursor: &mut ReadCursor,
    input_schema: &SchemaDescriptor,
    output_schema: &SchemaDescriptor,
    group_by_cols: &[u32],
    agg_descs: &[AggDescriptor],
    avi_cursor: Option<&mut ReadCursor>,
    avi_for_max: bool,
    avi_agg_col_type_code: u8,
    avi_group_by_cols: &[u32],
    avi_input_schema: Option<&SchemaDescriptor>,
    gi_cursor: Option<&mut ReadCursor>,
    gi_col_idx: u32,
    _gi_col_type_code: u8,
    finalize_prog: Option<&crate::expr::ExprProgram>,
    finalize_out_schema: Option<&SchemaDescriptor>,
) -> (OwnedBatch, Option<OwnedBatch>) {
    let num_aggs = agg_descs.len();
    let num_out_cols = output_schema.num_columns as usize;
    let out_npc = num_out_cols - 1;
    let in_pki = input_schema.pk_index as usize;

    // Linearity check
    let all_linear = agg_descs.iter().all(|d| {
        d.agg_op == AGG_COUNT || d.agg_op == AGG_SUM || d.agg_op == AGG_COUNT_NON_NULL
    });

    // Consolidate only for non-linear aggregates; linear aggregates work on raw delta.
    let consolidated;
    let working: &OwnedBatch = if all_linear {
        delta
    } else {
        consolidated = consolidate_owned(delta, input_schema);
        &consolidated
    };

    let n = working.count;
    if n == 0 {
        let empty_fin = finalize_out_schema.map(|fs| {
            OwnedBatch::empty(fs.num_columns as usize - 1)
        });
        return (OwnedBatch::empty(out_npc), empty_fin);
    }

    // group_by_pk detection
    let group_by_pk = group_by_cols.len() == 1
        && group_by_cols[0] as usize == in_pki;

    // Argsort
    let sorted_indices = if group_by_pk {
        (0..n as u32).collect()
    } else {
        argsort_delta(&working, input_schema, group_by_cols)
    };

    let mb = working.as_mem_batch();

    // Determine output mapping: use_natural_pk
    let use_natural_pk = if group_by_cols.len() == 1 {
        let grp_tc = input_schema.columns[group_by_cols[0] as usize].type_code;
        grp_tc == type_code::U64 || grp_tc == type_code::U128
    } else {
        false
    };

    let mut raw_output = OwnedBatch::empty(out_npc);
    let mut fin_output = finalize_out_schema.map(|fs| {
        OwnedBatch::empty(fs.num_columns as usize - 1)
    });

    let mut accs: Vec<Accumulator> = agg_descs.iter().map(Accumulator::new).collect();
    let mut old_vals: Vec<u64> = vec![0u64; num_aggs];

    // We need mutable access to optional cursors. Take ownership via Option::take pattern.
    // The caller passes these as Option<&mut ReadCursor>, but we need to use them
    // multiple times in the loop. They're already &mut so we can use them directly.
    let mut trace_in = trace_in_cursor;
    let mut avi = avi_cursor;
    let mut gi = gi_cursor;

    let mut idx = 0usize;
    let mut num_groups = 0usize;
    while idx < n {
        let group_start_pos = idx;
        let group_start_idx = sorted_indices[group_start_pos] as usize;

        let (group_key_lo, group_key_hi) = if group_by_pk {
            (mb.get_pk_lo(group_start_idx), mb.get_pk_hi(group_start_idx))
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
                if mb.get_pk_lo(curr_idx) != group_key_lo
                    || mb.get_pk_hi(curr_idx) != group_key_hi
                {
                    break;
                }
            } else {
                if compare_by_group_cols(&mb, curr_idx, group_start_idx, input_schema, group_by_cols)
                    != std::cmp::Ordering::Equal
                {
                    break;
                }
            }

            let w = mb.get_weight(curr_idx);
            for (_k, acc) in accs.iter_mut().enumerate() {
                if acc.is_linear() {
                    acc.step_from_batch(&mb, curr_idx, input_schema, w);
                }
            }
            idx += 1;
        }

        // Retraction: read old value from trace_out
        trace_out_cursor.seek(group_key_lo, group_key_hi);
        let has_old = trace_out_cursor.valid
            && trace_out_cursor.current_key_lo == group_key_lo
            && trace_out_cursor.current_key_hi == group_key_hi;

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
                group_key_lo, group_key_hi, -1,
                &old_vals, true, // use_old_val=true
                &accs, input_schema, output_schema,
                group_by_cols, use_natural_pk, num_aggs,
            );
            if let (Some(prog), Some(fin_schema), Some(ref mut fin_out)) =
                (finalize_prog, finalize_out_schema, &mut fin_output)
            {
                emit_finalized_row(
                    fin_out, &raw_output, raw_output.count - 1,
                    group_key_lo, group_key_hi, -1,
                    prog, output_schema, fin_schema,
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
                apply_agg_from_value_index(
                    avi_c, gc_u64, avi_for_max, avi_agg_col_type_code,
                    &mut accs[0],
                );
            } else {
                // Full replay path: build replay batch from trace_in + delta
                let mut replay = OwnedBatch::with_schema(*input_schema, 32);
                replay.count = 0;
                replay.sorted = false;
                replay.consolidated = false;

                if let Some(ref mut ti_cursor) = trace_in {
                    if group_by_pk {
                        ti_cursor.seek(group_key_lo, group_key_hi);
                        while ti_cursor.valid
                            && ti_cursor.current_key_lo == group_key_lo
                            && ti_cursor.current_key_hi == group_key_hi
                        {
                            append_cursor_row_to_batch(&mut replay, ti_cursor, input_schema);
                            ti_cursor.advance();
                        }
                    } else if let Some(ref mut gi_c) = gi {
                        // GI path
                        let pki = input_schema.pk_index as usize;
                        let gi_ci = gi_col_idx as usize;
                        let pi = if gi_ci < pki { gi_ci } else { gi_ci - 1 };
                        let ptr = mb.get_col_ptr(group_start_idx, pi, 8);
                        let gc_u64_val = u64::from_le_bytes(ptr.try_into().unwrap());
                        gi_c.seek(0, gc_u64_val);
                        while gi_c.valid {
                            let gk_hi = gi_c.current_key_hi;
                            if gk_hi != gc_u64_val {
                                break;
                            }
                            if gi_c.current_weight > 0 {
                                let spk_lo = gi_c.current_key_lo;
                                // spk_hi is in payload col 1 (first payload col = col index 1)
                                let spk_hi_ptr = gi_c.col_ptr(1, 8);
                                let spk_hi = if !spk_hi_ptr.is_null() {
                                    let bytes = unsafe { std::slice::from_raw_parts(spk_hi_ptr, 8) };
                                    u64::from_le_bytes(bytes.try_into().unwrap())
                                } else {
                                    0
                                };
                                if let Some(ref mut ti) = trace_in {
                                    ti.seek(spk_lo, spk_hi);
                                    if ti.valid
                                        && ti.current_key_lo == spk_lo
                                        && ti.current_key_hi == spk_hi
                                    {
                                        append_cursor_row_to_batch(&mut replay, ti, input_schema);
                                    }
                                }
                            }
                            gi_c.advance();
                        }
                    } else {
                        // Fallback: full trace scan
                        ti_cursor.seek(0, 0);
                        let ti_mb_exemplar_row = group_start_idx;
                        while ti_cursor.valid {
                            // Compare group columns between cursor row and exemplar
                            if cursor_matches_group(
                                ti_cursor, &mb, ti_mb_exemplar_row,
                                input_schema, group_by_cols,
                            ) {
                                append_cursor_row_to_batch(&mut replay, ti_cursor, input_schema);
                            }
                            ti_cursor.advance();
                        }
                    }
                }

                // Append delta rows to replay
                for k in group_start_pos..idx {
                    let d_idx = sorted_indices[k] as usize;
                    append_membatch_row_to_batch(&mut replay, &mb, d_idx, input_schema);
                }

                // Consolidate replay and step all accumulators
                let merged = consolidate_owned(&replay, input_schema);
                for acc in accs.iter_mut() {
                    acc.reset();
                }
                let merged_mb = merged.as_mem_batch();
                for m in 0..merged.count {
                    let w = merged_mb.get_weight(m);
                    if w > 0 {
                        for acc in accs.iter_mut() {
                            acc.step_from_batch(&merged_mb, m, input_schema, w);
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
                group_key_lo, group_key_hi, 1,
                &old_vals, false, // use_old_val=false
                &accs, input_schema, output_schema,
                group_by_cols, use_natural_pk, num_aggs,
            );
            if let (Some(prog), Some(fin_schema), Some(ref mut fin_out)) =
                (finalize_prog, finalize_out_schema, &mut fin_output)
            {
                emit_finalized_row(
                    fin_out, &raw_output, raw_output.count - 1,
                    group_key_lo, group_key_hi, 1,
                    prog, output_schema, fin_schema,
                );
            }
        }

        num_groups += 1;
    }

    // Output flags
    raw_output.sorted = group_by_pk;
    if group_by_pk {
        raw_output.consolidated = true;
    }

    gnitz_debug!(
        "op_reduce: in={} groups={} out={} fin={}",
        n, num_groups, raw_output.count,
        fin_output.as_ref().map_or(0, |b| b.count)
    );

    (raw_output, fin_output)
}

/// Emit one reduce output row.
fn emit_reduce_row(
    output: &mut OwnedBatch,
    input_mb: &MemBatch,
    exemplar_row: usize,
    group_key_lo: u64,
    group_key_hi: u64,
    weight: i64,
    old_vals: &[u64],
    use_old_val: bool,
    accs: &[Accumulator],
    input_schema: &SchemaDescriptor,
    output_schema: &SchemaDescriptor,
    group_by_cols: &[u32],
    use_natural_pk: bool,
    num_aggs: usize,
) {
    let out_pki = output_schema.pk_index as usize;
    let in_pki = input_schema.pk_index as usize;
    let num_out_cols = output_schema.num_columns as usize;

    output.pk_lo.extend_from_slice(&group_key_lo.to_le_bytes());
    output.pk_hi.extend_from_slice(&group_key_hi.to_le_bytes());
    output.weight.extend_from_slice(&weight.to_le_bytes());

    // Build null word and payload columns
    let mut null_word: u64 = 0;
    let mut out_pi = 0usize;

    for ci in 0..num_out_cols {
        if ci == out_pki {
            continue;
        }
        let col = &output_schema.columns[ci];
        let cs = col.size as usize;

        // Determine if this is an agg column or a group exemplar column
        let agg_base = num_out_cols - num_aggs;
        if ci >= agg_base {
            // Aggregate column
            let agg_idx = ci - agg_base;
            let bits = if use_old_val {
                old_vals[agg_idx]
            } else {
                accs[agg_idx].get_value_bits()
            };
            // Null if accumulator is zero (no value) and not using old val
            if !use_old_val && accs[agg_idx].is_zero() {
                null_word |= 1u64 << out_pi;
                { let cur = output.col_data[out_pi].len(); output.col_data[out_pi].resize(cur + cs, 0); }
            } else {
                output.col_data[out_pi].extend_from_slice(&bits.to_le_bytes()[..cs]);
            }
        } else if use_natural_pk {
            // use_natural_pk: no group exemplar columns in output (PK IS the group)
            // This shouldn't happen — with use_natural_pk, non-agg non-PK cols don't exist
            { let cur = output.col_data[out_pi].len(); output.col_data[out_pi].resize(cur + cs, 0); }
        } else {
            // Group exemplar column: ci=1..N maps to group_by_cols[ci-1]
            let grp_idx = ci - 1; // skip PK at 0
            if grp_idx < group_by_cols.len() {
                let src_ci = group_by_cols[grp_idx] as usize;
                let src_pi = payload_idx(src_ci, in_pki);
                // Check null from input
                let in_null = input_mb.get_null_word(exemplar_row);
                if (in_null >> src_pi) & 1 != 0 {
                    null_word |= 1u64 << out_pi;
                    { let cur = output.col_data[out_pi].len(); output.col_data[out_pi].resize(cur + cs, 0); }
                } else if col.type_code == TYPE_STRING {
                    write_string_from_batch(
                        &mut output.col_data[out_pi],
                        &mut output.blob,
                        input_mb, exemplar_row, src_pi,
                    );
                } else {
                    let src = input_mb.get_col_ptr(exemplar_row, src_pi, cs);
                    output.col_data[out_pi].extend_from_slice(src);
                }
            } else {
                { let cur = output.col_data[out_pi].len(); output.col_data[out_pi].resize(cur + cs, 0); }
            }
        }
        out_pi += 1;
    }

    output.null_bmp.extend_from_slice(&null_word.to_le_bytes());
    output.count += 1;
}

/// Emit a finalized row by evaluating the finalize ExprProgram on the raw output.
///
/// Handles COPY_COL (copy column from raw→finalized), EMIT (computed value),
/// and EMIT_NULL (null column) instructions by pre-scanning the bytecode.
fn emit_finalized_row(
    fin_output: &mut OwnedBatch,
    raw_output: &OwnedBatch,
    raw_row: usize,
    group_key_lo: u64,
    group_key_hi: u64,
    weight: i64,
    prog: &crate::expr::ExprProgram,
    raw_schema: &SchemaDescriptor,
    fin_schema: &SchemaDescriptor,
) {
    use crate::expr::{EXPR_COPY_COL, EXPR_EMIT, EXPR_EMIT_NULL};

    let raw_mb = raw_output.as_mem_batch();
    let raw_pki = raw_schema.pk_index as usize;
    let fin_pki = fin_schema.pk_index as usize;
    let null_word = raw_mb.get_null_word(raw_row);

    // Pre-scan bytecode: classify each output column as COPY_COL, EMIT, or EMIT_NULL
    #[derive(Clone, Copy)]
    enum OutCol {
        CopyCol(usize), // source logical col in raw schema
        Emit(usize),     // index in the eval emit sequence
        EmitNull,
    }

    let mut out_cols: Vec<OutCol> = Vec::new();
    let mut emit_count = 0usize;
    for i in 0..prog.num_instrs as usize {
        let base = i * 4;
        let op = prog.code[base];
        if op == EXPR_COPY_COL {
            let src_col = prog.code[base + 2] as usize;
            out_cols.push(OutCol::CopyCol(src_col));
        } else if op == EXPR_EMIT {
            out_cols.push(OutCol::Emit(emit_count));
            emit_count += 1;
        } else if op == EXPR_EMIT_NULL {
            out_cols.push(OutCol::EmitNull);
        }
    }

    // Create emit targets only for EMIT columns
    let buf_rows = raw_row + 1;
    let mut emit_bufs: Vec<Vec<u8>> = Vec::with_capacity(emit_count);
    for _ in 0..emit_count {
        emit_bufs.push(vec![0u8; buf_rows * 8]); // 8 bytes max per column
    }

    let emit_targets: Vec<crate::expr::EmitTarget> = emit_bufs
        .iter_mut()
        .enumerate()
        .map(|(i, buf)| crate::expr::EmitTarget {
            base: buf.as_mut_ptr(),
            stride: 8,
            payload_col: i,
        })
        .collect();

    // Evaluate (only EMIT instructions write to targets)
    let (_result, _is_null, eval_emit_mask) = crate::expr::eval_with_emit(
        prog, &raw_mb, raw_row, raw_schema.pk_index, null_word, &emit_targets,
    );

    // Build the finalized output row
    fin_output.pk_lo.extend_from_slice(&group_key_lo.to_le_bytes());
    fin_output.pk_hi.extend_from_slice(&group_key_hi.to_le_bytes());
    fin_output.weight.extend_from_slice(&weight.to_le_bytes());

    let mut fin_null_mask: u64 = 0;
    let mut fpi = 0usize;
    let mut out_col_idx = 0usize;

    for ci in 0..fin_schema.num_columns as usize {
        if ci == fin_pki {
            continue;
        }
        let cs = fin_schema.columns[ci].size as usize;

        if out_col_idx < out_cols.len() {
            match out_cols[out_col_idx] {
                OutCol::CopyCol(src_col) => {
                    if src_col == raw_pki {
                        // Source is the PK column — read from pk_lo
                        let pk_lo = raw_mb.get_pk_lo(raw_row);
                        fin_output.col_data[fpi].extend_from_slice(&pk_lo.to_le_bytes()[..cs]);
                    } else {
                        let src_pi = payload_idx(src_col, raw_pki);
                        if (null_word >> src_pi) & 1 != 0 {
                            fin_null_mask |= 1u64 << fpi;
                            { let cur = fin_output.col_data[fpi].len(); fin_output.col_data[fpi].resize(cur + cs, 0); }
                        } else if raw_schema.columns[src_col].type_code == TYPE_STRING {
                            write_string_from_batch(
                                &mut fin_output.col_data[fpi],
                                &mut fin_output.blob,
                                &raw_mb, raw_row, src_pi,
                            );
                        } else {
                            let src = raw_mb.get_col_ptr(raw_row, src_pi, cs);
                            fin_output.col_data[fpi].extend_from_slice(src);
                        }
                    }
                }
                OutCol::Emit(eidx) => {
                    if (eval_emit_mask >> eidx) & 1 != 0 {
                        fin_null_mask |= 1u64 << fpi;
                        { let cur = fin_output.col_data[fpi].len(); fin_output.col_data[fpi].resize(cur + cs, 0); }
                    } else {
                        let off = raw_row * 8;
                        fin_output.col_data[fpi].extend_from_slice(
                            &emit_bufs[eidx][off..off + cs],
                        );
                    }
                }
                OutCol::EmitNull => {
                    fin_null_mask |= 1u64 << fpi;
                    { let cur = fin_output.col_data[fpi].len(); fin_output.col_data[fpi].resize(cur + cs, 0); }
                }
            }
        } else {
            { let cur = fin_output.col_data[fpi].len(); fin_output.col_data[fpi].resize(cur + cs, 0); }
        }

        fpi += 1;
        out_col_idx += 1;
    }

    fin_output.null_bmp.extend_from_slice(&fin_null_mask.to_le_bytes());
    fin_output.count += 1;
}

/// Append a row from a ReadCursor to an OwnedBatch.
fn append_cursor_row_to_batch(
    output: &mut OwnedBatch,
    cursor: &ReadCursor,
    schema: &SchemaDescriptor,
) {
    let pki = schema.pk_index as usize;
    output.pk_lo.extend_from_slice(&cursor.current_key_lo.to_le_bytes());
    output.pk_hi.extend_from_slice(&cursor.current_key_hi.to_le_bytes());
    output.weight.extend_from_slice(&cursor.current_weight.to_le_bytes());
    output.null_bmp.extend_from_slice(&cursor.current_null_word.to_le_bytes());

    let blob_base = cursor.blob_ptr();
    let mut pi = 0;
    for ci in 0..schema.num_columns as usize {
        if ci == pki {
            continue;
        }
        let col = &schema.columns[ci];
        let cs = col.size as usize;
        let is_null = (cursor.current_null_word >> pi) & 1 != 0;
        if is_null {
            let new_len = output.col_data[pi].len() + cs;
            output.col_data[pi].resize(new_len, 0);
        } else if col.type_code == TYPE_STRING {
            let ptr = cursor.col_ptr(ci, 16);
            if ptr.is_null() {
                let new_len = output.col_data[pi].len() + 16;
                output.col_data[pi].resize(new_len, 0);
            } else {
                let src = unsafe { std::slice::from_raw_parts(ptr, 16) };
                write_string_from_raw(
                    &mut output.col_data[pi],
                    &mut output.blob,
                    src, blob_base,
                );
            }
        } else {
            let ptr = cursor.col_ptr(ci, cs);
            if ptr.is_null() {
                let new_len = output.col_data[pi].len() + cs;
                output.col_data[pi].resize(new_len, 0);
            } else {
                let src = unsafe { std::slice::from_raw_parts(ptr, cs) };
                output.col_data[pi].extend_from_slice(src);
            }
        }
        pi += 1;
    }
    output.count += 1;
}

/// Append a row from a MemBatch to an OwnedBatch.
fn append_membatch_row_to_batch(
    output: &mut OwnedBatch,
    mb: &MemBatch,
    row: usize,
    schema: &SchemaDescriptor,
) {
    let pki = schema.pk_index as usize;
    output.pk_lo.extend_from_slice(&mb.get_pk_lo(row).to_le_bytes());
    output.pk_hi.extend_from_slice(&mb.get_pk_hi(row).to_le_bytes());
    output.weight.extend_from_slice(&mb.get_weight(row).to_le_bytes());
    let null_word = mb.get_null_word(row);
    output.null_bmp.extend_from_slice(&null_word.to_le_bytes());

    let mut pi = 0;
    for ci in 0..schema.num_columns as usize {
        if ci == pki {
            continue;
        }
        let col = &schema.columns[ci];
        let cs = col.size as usize;
        let is_null = (null_word >> pi) & 1 != 0;
        if is_null {
            let new_len = output.col_data[pi].len() + cs;
            output.col_data[pi].resize(new_len, 0);
        } else if col.type_code == TYPE_STRING {
            write_string_from_batch(
                &mut output.col_data[pi],
                &mut output.blob,
                mb, row, pi,
            );
        } else {
            let src = mb.get_col_ptr(row, pi, cs);
            output.col_data[pi].extend_from_slice(src);
        }
        pi += 1;
    }
    output.count += 1;
}

/// Check if a cursor's current row matches the group columns of an exemplar row.
fn cursor_matches_group(
    cursor: &ReadCursor,
    exemplar_mb: &MemBatch,
    exemplar_row: usize,
    schema: &SchemaDescriptor,
    group_by_cols: &[u32],
) -> bool {
    let pki = schema.pk_index as usize;
    for &c_idx_u32 in group_by_cols {
        let c_idx = c_idx_u32 as usize;
        let col = &schema.columns[c_idx];
        let cs = col.size as usize;
        let pi = payload_idx(c_idx, pki);

        let cursor_ptr = cursor.col_ptr(c_idx, cs);
        if cursor_ptr.is_null() {
            return false;
        }
        let cursor_bytes = unsafe { std::slice::from_raw_parts(cursor_ptr, cs) };
        let exemplar_bytes = exemplar_mb.get_col_ptr(exemplar_row, pi, cs);

        if col.type_code == TYPE_STRING {
            let cursor_blob = cursor.blob_ptr();
            let cmp = crate::compact::compare_german_strings(
                cursor_bytes, if cursor_blob.is_null() { &[] } else { unsafe { std::slice::from_raw_parts(cursor_blob, cursor.blob_len()) } },
                exemplar_bytes, exemplar_mb.blob,
            );
            if cmp != std::cmp::Ordering::Equal {
                return false;
            }
        } else {
            if cursor_bytes != exemplar_bytes {
                return false;
            }
        }
    }
    true
}

// ---------------------------------------------------------------------------
// op_gather_reduce
// ---------------------------------------------------------------------------

/// Gather-reduce: merge partial aggregate deltas from workers.
pub fn op_gather_reduce(
    partial_batch: &OwnedBatch,
    trace_out_cursor: &mut ReadCursor,
    partial_schema: &SchemaDescriptor,
    agg_descs: &[AggDescriptor],
) -> OwnedBatch {
    let num_aggs = agg_descs.len();
    let num_out_cols = partial_schema.num_columns as usize;
    let out_npc = num_out_cols - 1;

    // Sort without consolidation
    let sorted = sort_owned(partial_batch, partial_schema);
    let n = sorted.count;
    if n == 0 {
        return OwnedBatch::empty(out_npc);
    }

    let smb = sorted.as_mem_batch();

    // Derive group_indices layout
    let num_group_cols = num_out_cols - 1 - num_aggs; // -1 for PK
    let _use_natural_pk_gather = num_group_cols == 0;

    let mut output = OwnedBatch::empty(out_npc);
    let mut accs: Vec<Accumulator> = agg_descs.iter().map(Accumulator::new).collect();
    let mut old_vals: Vec<u64> = vec![0u64; num_aggs];

    let mut idx = 0usize;
    while idx < n {
        let group_key_lo = smb.get_pk_lo(idx);
        let group_key_hi = smb.get_pk_hi(idx);
        let exemplar_row = idx;

        for acc in accs.iter_mut() {
            acc.reset();
        }

        // Accumulate all partial deltas for this group
        while idx < n
            && smb.get_pk_lo(idx) == group_key_lo
            && smb.get_pk_hi(idx) == group_key_hi
        {
            let w = smb.get_weight(idx);
            for k in 0..num_aggs {
                let agg_col_idx = num_out_cols - num_aggs + k;
                let pi = agg_col_idx - 1; // -1 for PK (pk_index=0 always in output schema)
                let ptr = smb.get_col_ptr(idx, pi, 8);
                let bits = u64::from_le_bytes(ptr.try_into().unwrap());
                if w > 0 {
                    accs[k].combine(bits);
                } else if w < 0 {
                    accs[k].merge_accumulated(bits, -1);
                }
            }
            idx += 1;
        }

        // Read old global from trace_out
        trace_out_cursor.seek(group_key_lo, group_key_hi);
        let has_old = trace_out_cursor.valid
            && trace_out_cursor.current_key_lo == group_key_lo
            && trace_out_cursor.current_key_hi == group_key_hi;

        if has_old {
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

            // Emit retraction
            emit_gather_row(
                &mut output, &smb, exemplar_row,
                group_key_lo, group_key_hi, -1,
                &old_vals, true,
                &accs, partial_schema, num_aggs,
            );

            // Fold old global into accumulators
            for k in 0..num_aggs {
                accs[k].merge_accumulated(old_vals[k], 1);
            }
        }

        // Emit new global if non-zero
        let any_nonzero = accs.iter().any(|a| !a.is_zero());
        if any_nonzero {
            emit_gather_row(
                &mut output, &smb, exemplar_row,
                group_key_lo, group_key_hi, 1,
                &old_vals, false,
                &accs, partial_schema, num_aggs,
            );
        }
    }

    output.sorted = true;
    gnitz_debug!("op_gather_reduce: in={} out={}", n, output.count);
    output
}

/// Emit one gather-reduce output row.
fn emit_gather_row(
    output: &mut OwnedBatch,
    input_mb: &MemBatch,
    exemplar_row: usize,
    group_key_lo: u64,
    group_key_hi: u64,
    weight: i64,
    old_vals: &[u64],
    use_old_val: bool,
    accs: &[Accumulator],
    schema: &SchemaDescriptor,
    num_aggs: usize,
) {
    let pki = schema.pk_index as usize;
    let num_cols = schema.num_columns as usize;

    output.pk_lo.extend_from_slice(&group_key_lo.to_le_bytes());
    output.pk_hi.extend_from_slice(&group_key_hi.to_le_bytes());
    output.weight.extend_from_slice(&weight.to_le_bytes());

    let mut null_word: u64 = 0;
    let in_null = input_mb.get_null_word(exemplar_row);
    let mut out_pi = 0usize;

    for ci in 0..num_cols {
        if ci == pki {
            continue;
        }
        let col = &schema.columns[ci];
        let cs = col.size as usize;

        let agg_base = num_cols - num_aggs;
        if ci >= agg_base {
            let agg_idx = ci - agg_base;
            let bits = if use_old_val {
                old_vals[agg_idx]
            } else {
                accs[agg_idx].get_value_bits()
            };
            if !use_old_val && accs[agg_idx].is_zero() {
                null_word |= 1u64 << out_pi;
                { let cur = output.col_data[out_pi].len(); output.col_data[out_pi].resize(cur + cs, 0); }
            } else {
                output.col_data[out_pi].extend_from_slice(&bits.to_le_bytes()[..cs]);
            }
        } else {
            // Group exemplar column: copy from input
            let src_pi = out_pi; // same position
            if (in_null >> src_pi) & 1 != 0 {
                null_word |= 1u64 << out_pi;
                { let cur = output.col_data[out_pi].len(); output.col_data[out_pi].resize(cur + cs, 0); }
            } else if col.type_code == TYPE_STRING {
                write_string_from_batch(
                    &mut output.col_data[out_pi],
                    &mut output.blob,
                    input_mb, exemplar_row, src_pi,
                );
            } else {
                let src = input_mb.get_col_ptr(exemplar_row, src_pi, cs);
                output.col_data[out_pi].extend_from_slice(src);
            }
        }
        out_pi += 1;
    }

    output.null_bmp.extend_from_slice(&null_word.to_le_bytes());
    output.count += 1;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compact::{SchemaColumn, SchemaDescriptor, type_code};

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

    /// Build a sorted OwnedBatch from (pk, weight, payload) triples.
    fn make_batch(
        schema: &SchemaDescriptor,
        rows: &[(u64, i64, i64)],
    ) -> OwnedBatch {
        let n = rows.len();
        let mut b = OwnedBatch::with_schema(*schema, n.max(1));
        b.count = 0;
        for &(pk, w, val) in rows {
            b.pk_lo.extend_from_slice(&pk.to_le_bytes());
            b.pk_hi.extend_from_slice(&0u64.to_le_bytes());
            b.weight.extend_from_slice(&w.to_le_bytes());
            b.null_bmp.extend_from_slice(&0u64.to_le_bytes());
            b.col_data[0].extend_from_slice(&val.to_le_bytes());
            b.count += 1;
        }
        b.sorted = true;
        b.consolidated = true;
        b
    }

    /// Build a sorted OwnedBatch with STRING payload from (pk, weight, string) triples.
    fn make_batch_str(
        schema: &SchemaDescriptor,
        rows: &[(u64, i64, &str)],
    ) -> OwnedBatch {
        let n = rows.len();
        let mut b = OwnedBatch::with_schema(*schema, n.max(1));
        b.count = 0;
        for &(pk, w, s) in rows {
            b.pk_lo.extend_from_slice(&pk.to_le_bytes());
            b.pk_hi.extend_from_slice(&0u64.to_le_bytes());
            b.weight.extend_from_slice(&w.to_le_bytes());
            b.null_bmp.extend_from_slice(&0u64.to_le_bytes());

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
            b.col_data[0].extend_from_slice(&gs);
            b.count += 1;
        }
        b.sorted = true;
        b.consolidated = true;
        b
    }

    fn read_str_payload(batch: &OwnedBatch, col: usize, row: usize) -> String {
        let off = row * 16;
        let gs = &batch.col_data[col][off..off + 16];
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

    // -----------------------------------------------------------------------
    // Anti-join DD tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_anti_join_dd_basic() {
        let schema = make_schema_u64_i64();
        let a = make_batch(&schema, &[(1, 1, 10), (2, 1, 20), (3, 1, 30)]);
        let b = make_batch(&schema, &[(2, 1, 200)]);
        let out = op_anti_join_delta_delta(&a, &b, &schema);
        assert_eq!(out.count, 2);
        assert_eq!(out.get_pk_lo(0), 1);
        assert_eq!(out.get_pk_lo(1), 3);
        assert!(out.consolidated);
    }

    #[test]
    fn test_anti_join_dd_empty_right() {
        let schema = make_schema_u64_i64();
        let a = make_batch(&schema, &[(1, 1, 10), (2, 1, 20)]);
        let b = make_batch(&schema, &[]);
        let out = op_anti_join_delta_delta(&a, &b, &schema);
        assert_eq!(out.count, 2);
        assert_eq!(out.get_pk_lo(0), 1);
        assert_eq!(out.get_pk_lo(1), 2);
    }

    #[test]
    fn test_anti_join_dd_full_overlap() {
        let schema = make_schema_u64_i64();
        let a = make_batch(&schema, &[(1, 1, 10), (2, 1, 20)]);
        let b = make_batch(&schema, &[(1, 1, 100), (2, 1, 200)]);
        let out = op_anti_join_delta_delta(&a, &b, &schema);
        assert_eq!(out.count, 0);
    }

    #[test]
    fn test_anti_join_dd_negative_weight_b() {
        let schema = make_schema_u64_i64();
        let a = make_batch(&schema, &[(1, 1, 10), (2, 1, 20)]);
        // PK=1 has negative weight in b — should NOT suppress a rows
        let b = make_batch(&schema, &[(1, -1, 100), (2, 1, 200)]);
        let out = op_anti_join_delta_delta(&a, &b, &schema);
        assert_eq!(out.count, 1);
        assert_eq!(out.get_pk_lo(0), 1);
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
        assert_eq!(out.get_pk_lo(0), 2);
        assert_eq!(out.get_pk_lo(1), 3);
        assert!(out.consolidated);
    }

    #[test]
    fn test_anti_semi_complement() {
        let schema = make_schema_u64_i64();
        let a = make_batch(&schema, &[(1, 1, 10), (2, 1, 20), (3, 1, 30), (4, 1, 40)]);
        let b = make_batch(&schema, &[(2, 1, 200), (4, 1, 400)]);
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
        let mut b = OwnedBatch::with_schema(rs, 2);
        b.count = 0;
        for &(pk, w, val) in &[(1u64, 1i64, 100i64), (1u64, -1i64, 100i64)] {
            b.pk_lo.extend_from_slice(&pk.to_le_bytes());
            b.pk_hi.extend_from_slice(&0u64.to_le_bytes());
            b.weight.extend_from_slice(&w.to_le_bytes());
            b.null_bmp.extend_from_slice(&0u64.to_le_bytes());
            b.col_data[0].extend_from_slice(&val.to_le_bytes());
            b.count += 1;
        }
        b.sorted = true;
        b.consolidated = false;

        let out = op_join_delta_delta(&a, &b, &ls, &rs);
        // b consolidates to empty (1 + -1 = 0 for same pk+payload) → no match
        assert_eq!(out.count, 0);
    }
}
