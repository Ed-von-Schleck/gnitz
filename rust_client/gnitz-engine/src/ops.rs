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
