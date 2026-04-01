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

    let consolidated_mb = consolidated.as_mem_batch();
    let mut prev_lo = u64::MAX;
    let mut prev_hi = u64::MAX;

    for i in 0..n {
        let key_lo = consolidated.get_pk_lo(i);
        let key_hi = consolidated.get_pk_hi(i);
        let w_delta = consolidated.get_weight(i);

        if key_lo != prev_lo || key_hi != prev_hi {
            cursor.seek(key_lo, key_hi);
        }
        prev_lo = key_lo;
        prev_hi = key_hi;

        let w_old: i64 = loop {
            if !cursor.valid
                || cursor.current_key_lo != key_lo
                || cursor.current_key_hi != key_hi
            {
                break 0;
            }
            match compare_cursor_payload_to_batch_row(cursor, &consolidated_mb, i, schema) {
                std::cmp::Ordering::Less => {
                    cursor.advance();
                }
                std::cmp::Ordering::Equal => {
                    let w = cursor.current_weight;
                    cursor.advance();
                    break w;
                }
                std::cmp::Ordering::Greater => break 0,
            }
        };

        let s_old = signum(w_old);
        let w_new = w_old.wrapping_add(w_delta);
        let s_new = signum(w_new);
        let out_w = s_new - s_old;
        if out_w != 0 {
            emit_indices.push(i as u32);
            emit_weights.push(out_w);
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
    } else {
        panic!(
            "write_string_from_raw: string length {} exceeds SHORT_STRING_THRESHOLD but src_blob_ptr is null",
            length
        );
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
// Scan trace (source operator)
// ---------------------------------------------------------------------------

/// Scan rows from a ReadCursor into an OwnedBatch.
///
/// Faithfully ports source.py:14-45.  Skips zero-weight rows (defense-in-depth:
/// individual shard entries may carry non-zero weights that cancel at merge level).
/// The cursor is left at the next unscanned position.
///
/// `chunk_limit <= 0` means scan everything until cursor exhaustion.
pub fn op_scan_trace(
    cursor: &mut ReadCursor,
    schema: &SchemaDescriptor,
    chunk_limit: i32,
) -> OwnedBatch {
    let cap = if chunk_limit > 0 { chunk_limit as usize } else { 64 };
    let mut output = OwnedBatch::with_schema(*schema, cap);
    output.sorted = true;       // cursor produces sorted order
    output.consolidated = true; // cursor merges → consolidated

    let mut scanned: i32 = 0;
    while cursor.valid {
        if chunk_limit > 0 && scanned >= chunk_limit {
            break;
        }
        // Skip zero-weight rows (source.py:37)
        if cursor.current_weight != 0 {
            append_cursor_row_to_batch(&mut output, cursor, schema);
            scanned += 1;
        }
        cursor.advance();
    }

    output
}

// ---------------------------------------------------------------------------
// Integrate with secondary indexes
// ---------------------------------------------------------------------------

/// Promote a column value to u64 for GroupIndex composite keys.
/// Ports group_index.py:26-42.
fn promote_col_to_u64(mb: &MemBatch, row: usize, col_idx: usize, pk_index: usize, type_code: u8) -> u64 {
    let pi = payload_idx(col_idx, pk_index);
    let cs = match type_code {
        type_code::U8 | type_code::I8 => 1,
        type_code::U16 | type_code::I16 => 2,
        type_code::U32 | type_code::I32 | type_code::F32 => 4,
        _ => 8, // U64, I64, F64; U128 excluded by caller
    };
    let ptr = mb.get_col_ptr(row, pi, cs);
    let mut buf = [0u8; 8];
    buf[..cs].copy_from_slice(ptr);
    u64::from_le_bytes(buf)
}

/// Promote an aggregate column value to an order-preserving u64 for AVI keys.
/// Ports group_index.py:169-192.
fn promote_agg_col_to_u64_ordered(
    mb: &MemBatch,
    row: usize,
    col_idx: usize,
    pk_index: usize,
    col_type_code: u8,
    for_max: bool,
) -> u64 {
    let pi = payload_idx(col_idx, pk_index);

    if col_type_code == type_code::F32 {
        let ptr = mb.get_col_ptr(row, pi, 4);
        let raw32 = u32::from_le_bytes(ptr.try_into().unwrap());
        let val = ieee_order_bits_f32(raw32);
        return if for_max { !val } else { val };
    }

    let ptr = mb.get_col_ptr(row, pi, 8);
    let raw = u64::from_le_bytes(ptr.try_into().unwrap());

    let val = match col_type_code {
        type_code::U8 | type_code::U16 | type_code::U32 | type_code::U64 => raw,
        type_code::I8 | type_code::I16 | type_code::I32 | type_code::I64 => {
            // Offset-binary: reinterpret as signed then add 2^63
            let signed = raw as i64;
            (signed as u64).wrapping_add(1u64 << 63)
        }
        type_code::F64 => ieee_order_bits(raw),
        _ => raw,
    };

    if for_max { !val } else { val }
}

/// GI/AVI descriptor for integrate_with_indexes.
pub struct GiDesc {
    pub table: *mut crate::table::Table,
    pub col_idx: u32,
    pub col_type_code: u8,
}

pub struct AviDesc {
    pub table: *mut crate::table::Table,
    pub for_max: bool,
    pub agg_col_type_code: u8,
    pub group_by_cols: Vec<u32>,
    pub input_schema: SchemaDescriptor,
    pub agg_col_idx: u32,
}

/// GI schema: U128 PK + I64 payload (spk_hi).
fn make_gi_schema() -> SchemaDescriptor {
    let mut s = SchemaDescriptor {
        num_columns: 2,
        pk_index: 0,
        columns: [crate::compact::SchemaColumn { type_code: 0, size: 0, nullable: 0, _pad: 0 }; 64],
    };
    s.columns[0] = crate::compact::SchemaColumn {
        type_code: type_code::U128, size: 16, nullable: 0, _pad: 0,
    };
    s.columns[1] = crate::compact::SchemaColumn {
        type_code: type_code::I64, size: 8, nullable: 0, _pad: 0,
    };
    s
}

/// AVI schema: U128 PK only, no payload.
fn make_avi_schema() -> SchemaDescriptor {
    let mut s = SchemaDescriptor {
        num_columns: 1,
        pk_index: 0,
        columns: [crate::compact::SchemaColumn { type_code: 0, size: 0, nullable: 0, _pad: 0 }; 64],
    };
    s.columns[0] = crate::compact::SchemaColumn {
        type_code: type_code::U128, size: 16, nullable: 0, _pad: 0,
    };
    s
}

/// Integrate a delta batch into a target table, optionally populating
/// GroupIndex and AggValueIndex secondary indexes.
///
/// Ports interpreter.py:171-221.  The Rust Table handles memtable capacity
/// internally (flush-on-overflow), so no explicit MemTableFullError retry.
pub fn op_integrate_with_indexes(
    batch: &OwnedBatch,
    target_table: Option<&mut crate::table::Table>,
    input_schema: &SchemaDescriptor,
    gi: Option<&GiDesc>,
    avi: Option<&AviDesc>,
) -> i32 {
    if batch.count == 0 {
        return 0;
    }

    // Phase 1: ingest into target table
    if let Some(table) = target_table {
        let regions = batch.regions();
        let ptrs: Vec<*const u8> = regions.iter().map(|&(p, _)| p).collect();
        let sizes: Vec<u32> = regions.iter().map(|&(_, s)| s as u32).collect();
        let npc = input_schema.num_columns as usize - 1;
        if let Err(rc) = table.ingest_batch_memonly_from_regions(&ptrs, &sizes, batch.count as u32, npc) {
            return rc;
        }
    }

    let mb = batch.as_mem_batch();
    let pki = input_schema.pk_index as usize;

    // Phase 2: GroupIndex population (interpreter.py:175-199)
    if let Some(gi_desc) = gi {
        let gi_schema = make_gi_schema();
        let mut gi_batch = OwnedBatch::with_schema(gi_schema, batch.count);
        gi_batch.sorted = false;
        gi_batch.consolidated = false;

        let gi_col = gi_desc.col_idx as usize;
        let gi_pi = payload_idx(gi_col, pki);

        for row in 0..batch.count {
            // Skip null group column (interpreter.py:184)
            let null_word = mb.get_null_word(row);
            if (null_word >> gi_pi) & 1 != 0 {
                continue;
            }

            let gc_u64 = promote_col_to_u64(&mb, row, gi_col, pki, gi_desc.col_type_code);
            let source_pk_lo = mb.get_pk_lo(row);
            let source_pk_hi = mb.get_pk_hi(row);
            let weight = mb.get_weight(row);

            // Composite key: ck_lo = source_pk_lo, ck_hi = gc_u64
            gi_batch.pk_lo.extend_from_slice(&source_pk_lo.to_le_bytes());
            gi_batch.pk_hi.extend_from_slice(&gc_u64.to_le_bytes());
            gi_batch.weight.extend_from_slice(&weight.to_le_bytes());
            gi_batch.null_bmp.extend_from_slice(&0u64.to_le_bytes());
            // Payload: spk_hi (source pk high 64 bits) as I64
            gi_batch.col_data[0].extend_from_slice(&(source_pk_hi as i64).to_le_bytes());
            gi_batch.count += 1;
        }

        if gi_batch.count > 0 {
            let gi_table = unsafe { &mut *gi_desc.table };
            let gi_schema = gi_table.schema();
            let regions = gi_batch.regions();
            let ptrs: Vec<*const u8> = regions.iter().map(|&(p, _)| p).collect();
            let sizes: Vec<u32> = regions.iter().map(|&(_, s)| s as u32).collect();
            let _ = gi_table.ingest_batch_memonly_from_regions(
                &ptrs, &sizes, gi_batch.count as u32,
                gi_schema.num_columns as usize - 1,
            );
        }
    }

    // Phase 3: AggValueIndex population (interpreter.py:200-221)
    if let Some(avi_desc) = avi {
        let avi_schema = make_avi_schema();
        let mut avi_batch = OwnedBatch::with_schema(avi_schema, batch.count);
        avi_batch.sorted = false;
        avi_batch.consolidated = false;

        let avi_col = avi_desc.agg_col_idx as usize;
        let avi_pi = payload_idx(avi_col, pki);

        for row in 0..batch.count {
            // Skip null agg column (interpreter.py:208)
            let null_word = mb.get_null_word(row);
            if (null_word >> avi_pi) & 1 != 0 {
                continue;
            }

            let gc_u64 = extract_gc_u64(&mb, row, &avi_desc.input_schema, &avi_desc.group_by_cols);
            let av_u64 = promote_agg_col_to_u64_ordered(
                &mb, row, avi_col, pki,
                avi_desc.agg_col_type_code, avi_desc.for_max,
            );
            let weight = mb.get_weight(row);

            // Composite key: ck_lo = av_u64, ck_hi = gc_u64
            avi_batch.pk_lo.extend_from_slice(&av_u64.to_le_bytes());
            avi_batch.pk_hi.extend_from_slice(&gc_u64.to_le_bytes());
            avi_batch.weight.extend_from_slice(&weight.to_le_bytes());
            avi_batch.null_bmp.extend_from_slice(&0u64.to_le_bytes());
            // No payload columns (AVI schema is U128 PK only)
            avi_batch.count += 1;
        }

        if avi_batch.count > 0 {
            gnitz_debug!("integrate_avi: ingesting {} rows, for_max={}, agg_col_idx={}, agg_type={}",
                avi_batch.count, avi_desc.for_max, avi_desc.agg_col_idx, avi_desc.agg_col_type_code);
            for i in 0..avi_batch.count {
                let lo = u64::from_le_bytes(avi_batch.pk_lo[i*8..(i+1)*8].try_into().unwrap());
                let hi = u64::from_le_bytes(avi_batch.pk_hi[i*8..(i+1)*8].try_into().unwrap());
                let w = i64::from_le_bytes(avi_batch.weight[i*8..(i+1)*8].try_into().unwrap());
                gnitz_debug!("  avi[{}]: pk_lo={:#x} pk_hi={:#x} w={}", i, lo, hi, w);
            }
            let avi_table = unsafe { &mut *avi_desc.table };
            let avi_schema = avi_table.schema();
            let regions = avi_batch.regions();
            let ptrs: Vec<*const u8> = regions.iter().map(|&(p, _)| p).collect();
            let sizes: Vec<u32> = regions.iter().map(|&(_, s)| s as u32).collect();
            let _ = avi_table.ingest_batch_memonly_from_regions(
                &ptrs, &sizes, avi_batch.count as u32,
                avi_schema.num_columns as usize - 1,
            );
        }
    }

    0
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

    let mb_a = batch_a.as_mem_batch();
    let mb_b = batch_b.as_mem_batch();

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
            // Equal PKs: flush pending run, then merge-sort the equal-PK
            // sub-ranges from both batches by payload to preserve (PK, payload)
            // sort order.
            if run_src == 0 {
                output.append_batch(batch_a, run_a_start, i);
            } else if run_src == 1 {
                output.append_batch(batch_b, run_b_start, j);
            }
            run_src = -1;

            // Find the end of the equal-PK run in each batch.
            let mut i_end = i + 1;
            while i_end < n_a
                && batch_a.get_pk_lo(i_end) == a_lo
                && batch_a.get_pk_hi(i_end) == a_hi
            {
                i_end += 1;
            }
            let mut j_end = j + 1;
            while j_end < n_b
                && batch_b.get_pk_lo(j_end) == b_lo
                && batch_b.get_pk_hi(j_end) == b_hi
            {
                j_end += 1;
            }

            // Merge the two sub-ranges by payload order.
            let (mut ia, mut jb) = (i, j);
            while ia < i_end && jb < j_end {
                if crate::columnar::compare_rows(schema, &mb_a, ia, &mb_b, jb)
                    != std::cmp::Ordering::Greater
                {
                    output.append_batch(batch_a, ia, ia + 1);
                    ia += 1;
                } else {
                    output.append_batch(batch_b, jb, jb + 1);
                    jb += 1;
                }
            }
            if ia < i_end {
                output.append_batch(batch_a, ia, i_end);
            }
            if jb < j_end {
                output.append_batch(batch_b, jb, j_end);
            }

            i = i_end;
            j = j_end;
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
        type_code::F64 => {
            let pi = if col_idx < pki { col_idx } else { col_idx - 1 };
            let ptr = batch.get_col_ptr(row, pi, 8);
            let bits = u64::from_le_bytes(ptr.try_into().unwrap());
            (bits, 0)
        }
        type_code::F32 => {
            let pi = if col_idx < pki { col_idx } else { col_idx - 1 };
            let ptr = batch.get_col_ptr(row, pi, 4);
            let bits = u32::from_le_bytes(ptr.try_into().unwrap()) as u64;
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
            let cs = schema.columns[c_idx].size as usize;
            let ptr = mb.get_col_ptr(row, pi, cs);
            let mut buf = [0u8; 8];
            buf[..cs].copy_from_slice(ptr);
            mix64(u64::from_le_bytes(buf))
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

/// IEEE 754 order-preserving encoding for 32-bit floats, returning u64.
/// Checks the F32 sign bit (bit 31), not bit 63.
fn ieee_order_bits_f32(raw_bits: u32) -> u64 {
    (if raw_bits >> 31 != 0 { !raw_bits } else { raw_bits ^ (1u32 << 31) }) as u64
}

/// Reverse of ieee_order_bits_f32.
fn ieee_order_bits_f32_reverse(encoded: u64) -> u32 {
    let e = encoded as u32;
    if e >> 31 != 0 { e ^ (1u32 << 31) } else { !e }
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
        } else if tc == type_code::F64 {
            let a_ptr = mb.get_col_ptr(row_a, pi, 8);
            let b_ptr = mb.get_col_ptr(row_b, pi, 8);
            let a_f = f64::from_bits(crate::util::read_u64_le(a_ptr, 0));
            let b_f = f64::from_bits(crate::util::read_u64_le(b_ptr, 0));
            a_f.partial_cmp(&b_f).unwrap_or(std::cmp::Ordering::Equal)
        } else if tc == type_code::F32 {
            let a_ptr = mb.get_col_ptr(row_a, pi, 4);
            let b_ptr = mb.get_col_ptr(row_b, pi, 4);
            let a_f = f32::from_bits(crate::util::read_u32_le(a_ptr, 0));
            let b_f = f32::from_bits(crate::util::read_u32_le(b_ptr, 0));
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
    gnitz_debug!("avi_lookup: seek(0, {:#x}) valid={}", gc_u64, avi_cursor.valid);
    while avi_cursor.valid {
        let k_hi = avi_cursor.current_key_hi;
        let k_lo = avi_cursor.current_key_lo;
        let w = avi_cursor.current_weight;
        gnitz_debug!("avi_lookup: at pk_lo={:#x} pk_hi={:#x} w={}", k_lo, k_hi, w);
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
            } else if tc == type_code::F64 {
                encoded = ieee_order_bits_reverse(encoded);
            } else if tc == type_code::F32 {
                encoded = ieee_order_bits_f32_reverse(encoded) as u64;
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
                let found = apply_agg_from_value_index(
                    avi_c, gc_u64, avi_for_max, avi_agg_col_type_code,
                    &mut accs[0],
                );
                gnitz_debug!("reduce: AVI lookup gc={:#x} found={}", gc_u64, found);
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
                                    while ti.valid
                                        && ti.current_key_lo == spk_lo
                                        && ti.current_key_hi == spk_hi
                                    {
                                        append_cursor_row_to_batch(&mut replay, ti, input_schema);
                                        ti.advance();
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

/// Compare a cursor's current payload to a batch row's payload, returning their ordering.
///
/// Iterates payload columns in schema order (skipping pk_index). Null ordering:
/// null < non-null, null == null. Type dispatch follows `compare_by_group_cols`.
fn compare_cursor_payload_to_batch_row(
    cursor: &ReadCursor,
    batch: &MemBatch,
    row: usize,
    schema: &SchemaDescriptor,
) -> std::cmp::Ordering {
    use std::cmp::Ordering;

    let pki = schema.pk_index as usize;
    let cursor_blob = cursor.blob_ptr();
    let cursor_blob_slice: &[u8] = if cursor_blob.is_null() {
        &[]
    } else {
        unsafe { std::slice::from_raw_parts(cursor_blob, cursor.blob_len()) }
    };

    let mut pi = 0usize;
    for ci in 0..schema.num_columns as usize {
        if ci == pki {
            continue;
        }
        let col = &schema.columns[ci];
        let cs = col.size as usize;

        let cursor_null = (cursor.current_null_word >> pi) & 1 != 0;
        let batch_null = (batch.get_null_word(row) >> pi) & 1 != 0;

        let ord = match (cursor_null, batch_null) {
            (true, true) => Ordering::Equal,
            (true, false) => Ordering::Less,
            (false, true) => Ordering::Greater,
            (false, false) => {
                let c_ptr = cursor.col_ptr(ci, cs);
                let c_bytes = unsafe { std::slice::from_raw_parts(c_ptr, cs) };
                let b_bytes = batch.get_col_ptr(row, pi, cs);

                if col.type_code == TYPE_STRING {
                    crate::compact::compare_german_strings(
                        c_bytes,
                        cursor_blob_slice,
                        b_bytes,
                        batch.blob,
                    )
                } else if col.type_code == type_code::U128 {
                    let c_lo = u64::from_le_bytes(c_bytes[0..8].try_into().unwrap());
                    let c_hi = u64::from_le_bytes(c_bytes[8..16].try_into().unwrap());
                    let b_lo = u64::from_le_bytes(b_bytes[0..8].try_into().unwrap());
                    let b_hi = u64::from_le_bytes(b_bytes[8..16].try_into().unwrap());
                    (c_hi, c_lo).cmp(&(b_hi, b_lo))
                } else if col.type_code == type_code::F64 {
                    let c_f = f64::from_bits(u64::from_le_bytes(c_bytes[0..8].try_into().unwrap()));
                    let b_f = f64::from_bits(u64::from_le_bytes(b_bytes[0..8].try_into().unwrap()));
                    c_f.partial_cmp(&b_f).unwrap_or(Ordering::Equal)
                } else if col.type_code == type_code::F32 {
                    let c_f = f32::from_bits(u32::from_le_bytes(c_bytes[0..4].try_into().unwrap()));
                    let b_f = f32::from_bits(u32::from_le_bytes(b_bytes[0..4].try_into().unwrap()));
                    c_f.partial_cmp(&b_f).unwrap_or(Ordering::Equal)
                } else if col.type_code == type_code::I64
                    || col.type_code == type_code::I32
                    || col.type_code == type_code::I16
                    || col.type_code == type_code::I8
                {
                    let c_v = i64::from_le_bytes(c_bytes.try_into().unwrap());
                    let b_v = i64::from_le_bytes(b_bytes.try_into().unwrap());
                    c_v.cmp(&b_v)
                } else {
                    let c_v = u64::from_le_bytes(c_bytes.try_into().unwrap());
                    let b_v = u64::from_le_bytes(b_bytes.try_into().unwrap());
                    c_v.cmp(&b_v)
                }
            }
        };

        if ord != Ordering::Equal {
            return ord;
        }
        pi += 1;
    }
    Ordering::Equal
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
// Exchange repartitioning
// ---------------------------------------------------------------------------

#[inline]
fn worker_for_partition(partition: usize, num_workers: usize) -> usize {
    let chunk = 256 / num_workers;
    (partition / chunk).min(num_workers - 1)
}

fn hash_row_for_partition(
    mb: &MemBatch,
    row: usize,
    col_indices: &[u32],
    schema: &SchemaDescriptor,
) -> usize {
    let pki = schema.pk_index as usize;
    if col_indices.len() == 1 && col_indices[0] as usize == pki {
        return crate::partitioned_table::partition_for_key(mb.get_pk_lo(row), mb.get_pk_hi(row));
    }
    let (lo, hi) = extract_group_key(mb, row, schema, col_indices);
    crate::partitioned_table::partition_for_key(lo, hi)
}

pub fn op_repartition_batch(
    batch: &OwnedBatch,
    col_indices: &[u32],
    schema: &SchemaDescriptor,
    num_workers: usize,
) -> Vec<OwnedBatch> {
    gnitz_debug!("op_repartition_batch: count={} num_workers={}", batch.count, num_workers);
    let npc = schema.num_columns as usize - 1;
    let n = batch.count;
    let mb = batch.as_mem_batch();

    let mut worker_indices: Vec<Vec<u32>> = (0..num_workers).map(|_| Vec::new()).collect();
    for i in 0..n {
        let partition = hash_row_for_partition(&mb, i, col_indices, schema);
        let w = worker_for_partition(partition, num_workers);
        worker_indices[w].push(i as u32);
    }

    let mut result: Vec<Option<OwnedBatch>> = (0..num_workers).map(|_| None).collect();
    for w in 0..num_workers {
        if !worker_indices[w].is_empty() {
            result[w] = Some(OwnedBatch::from_indexed_rows(&mb, &worker_indices[w], schema));
        }
    }
    result
        .into_iter()
        .map(|opt| opt.unwrap_or_else(|| OwnedBatch::empty(npc)))
        .collect()
}

pub fn op_repartition_batches(
    sources: &[Option<&OwnedBatch>],
    col_indices: &[u32],
    schema: &SchemaDescriptor,
    num_workers: usize,
) -> Vec<OwnedBatch> {
    gnitz_debug!("op_repartition_batches: sources={} num_workers={}", sources.len(), num_workers);
    let npc = schema.num_columns as usize - 1;
    let mut dest: Vec<Option<OwnedBatch>> = (0..num_workers).map(|_| None).collect();

    for src_opt in sources {
        let src = match src_opt {
            Some(s) if s.count > 0 => *s,
            _ => continue,
        };
        let mb = src.as_mem_batch();
        let mut worker_indices: Vec<Vec<u32>> = (0..num_workers).map(|_| Vec::new()).collect();
        for i in 0..src.count {
            let partition = hash_row_for_partition(&mb, i, col_indices, schema);
            let w = worker_for_partition(partition, num_workers);
            worker_indices[w].push(i as u32);
        }
        for w in 0..num_workers {
            if !worker_indices[w].is_empty() {
                let d = dest[w].get_or_insert_with(|| {
                    OwnedBatch::with_schema(*schema, worker_indices[w].len())
                });
                d.append_indexed_rows(&mb, &worker_indices[w], schema);
            }
        }
    }

    dest.into_iter()
        .map(|opt| opt.unwrap_or_else(|| OwnedBatch::empty(npc)))
        .collect()
}

pub fn op_repartition_batches_merged(
    sources: &[Option<&OwnedBatch>],
    col_indices: &[u32],
    schema: &SchemaDescriptor,
    num_workers: usize,
) -> Vec<OwnedBatch> {
    gnitz_debug!(
        "op_repartition_batches_merged: sources={} num_workers={}",
        sources.len(),
        num_workers
    );
    let npc = schema.num_columns as usize - 1;
    let n = sources.len();
    let mut cursors: Vec<usize> = vec![0; n];
    let mut dest: Vec<Option<OwnedBatch>> = (0..num_workers).map(|_| None).collect();

    // Pre-compute MemBatch views to avoid per-row allocation in as_mem_batch()
    let mem_batches: Vec<Option<MemBatch>> = sources
        .iter()
        .map(|src_opt| match src_opt {
            Some(s) if s.count > 0 => Some(s.as_mem_batch()),
            _ => None,
        })
        .collect();

    loop {
        let mut min_w: Option<usize> = None;
        for w in 0..n {
            if mem_batches[w].is_none() {
                continue;
            }
            let src = sources[w].as_ref().unwrap();
            let idx = cursors[w];
            if idx >= src.count {
                continue;
            }
            match min_w {
                None => {
                    min_w = Some(w);
                }
                Some(mw) => {
                    let min_src = sources[mw].as_ref().unwrap();
                    let min_idx = cursors[mw];
                    let cur_hi = src.get_pk_hi(idx);
                    let min_hi = min_src.get_pk_hi(min_idx);
                    if cur_hi < min_hi
                        || (cur_hi == min_hi
                            && src.get_pk_lo(idx) < min_src.get_pk_lo(min_idx))
                    {
                        min_w = Some(w);
                    }
                }
            }
        }
        let min_w = match min_w {
            None => break,
            Some(w) => w,
        };

        let src = sources[min_w].as_ref().unwrap();
        let src_idx = cursors[min_w];
        let mb = mem_batches[min_w].as_ref().unwrap();
        let partition = hash_row_for_partition(mb, src_idx, col_indices, schema);
        let dw = worker_for_partition(partition, num_workers);

        let d = dest[dw]
            .get_or_insert_with(|| OwnedBatch::with_schema(*schema, 16));
        d.append_batch(src, src_idx, src_idx + 1);
        cursors[min_w] += 1;
    }

    dest.into_iter()
        .map(|opt| match opt {
            Some(mut d) => {
                d.sorted = true;
                d.consolidated = true;
                d.schema = Some(*schema);
                d
            }
            None => OwnedBatch::empty(npc),
        })
        .collect()
}

pub fn op_relay_scatter(
    sources: &[Option<&OwnedBatch>],
    col_indices: &[u32],
    schema: &SchemaDescriptor,
    num_workers: usize,
) -> Vec<OwnedBatch> {
    let all_consolidated = sources.iter().all(|src_opt| match src_opt {
        Some(s) if s.count > 0 => s.consolidated,
        _ => true,
    });
    let has_any = sources.iter().any(|src_opt| matches!(src_opt, Some(s) if s.count > 0));
    gnitz_debug!(
        "op_relay_scatter: sources={} merged={}",
        sources.iter().filter(|s| matches!(s, Some(sb) if sb.count > 0)).count(),
        all_consolidated
    );
    if !has_any {
        let npc = schema.num_columns as usize - 1;
        return (0..num_workers).map(|_| OwnedBatch::empty(npc)).collect();
    }
    if all_consolidated {
        op_repartition_batches_merged(sources, col_indices, schema, num_workers)
    } else {
        op_repartition_batches(sources, col_indices, schema, num_workers)
    }
}

pub fn op_multi_scatter(
    batch: &OwnedBatch,
    col_specs: &[&[u32]],
    schema: &SchemaDescriptor,
    num_workers: usize,
) -> Vec<Vec<OwnedBatch>> {
    gnitz_debug!(
        "op_multi_scatter: count={} specs={} num_workers={}",
        batch.count,
        col_specs.len(),
        num_workers
    );
    let n = batch.count;
    let n_specs = col_specs.len();
    let npc = schema.num_columns as usize - 1;
    let pki = schema.pk_index as usize;
    let mb = batch.as_mem_batch();

    let mut flat_indices: Vec<Vec<u32>> = (0..n_specs * num_workers).map(|_| Vec::new()).collect();

    for i in 0..n {
        for si in 0..n_specs {
            let partition = hash_row_for_partition(&mb, i, col_specs[si], schema);
            let w = worker_for_partition(partition, num_workers);
            flat_indices[si * num_workers + w].push(i as u32);
        }
    }

    let mut results: Vec<Vec<OwnedBatch>> = (0..n_specs)
        .map(|_| (0..num_workers).map(|_| OwnedBatch::empty(npc)).collect())
        .collect();

    for si in 0..n_specs {
        for w in 0..num_workers {
            let indices = &flat_indices[si * num_workers + w];
            if !indices.is_empty() {
                results[si][w] = OwnedBatch::from_indexed_rows(&mb, indices, schema);
            }
        }
    }

    // Flag propagation: PK-spec sub-batches inherit sorted/consolidated from source
    for si in 0..n_specs {
        let spec = col_specs[si];
        if spec.len() == 1 && spec[0] as usize == pki && (batch.sorted || batch.consolidated) {
            for w in 0..num_workers {
                if results[si][w].count > 0 {
                    if batch.sorted {
                        results[si][w].sorted = true;
                    }
                    if batch.consolidated {
                        results[si][w].consolidated = true;
                    }
                }
            }
        }
    }

    results
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

    // -----------------------------------------------------------------------
    // Union merge sort-invariant tests
    // -----------------------------------------------------------------------

    fn get_payload_i64(b: &OwnedBatch, row: usize) -> i64 {
        crate::util::read_i64_le(&b.col_data[0], row * 8)
    }

    #[test]
    fn test_union_merge_same_pk_payload_order() {
        // batch_a has val=20, batch_b has val=10 — output must be [10, 20]
        let schema = make_schema_u64_i64();
        let a = make_batch(&schema, &[(1, 1, 20)]);
        let b = make_batch(&schema, &[(1, 1, 10)]);
        let out = op_union(&a, Some(&b), &schema);
        assert_eq!(out.count, 2);
        assert!(out.sorted);
        assert_eq!(get_payload_i64(&out, 0), 10);
        assert_eq!(get_payload_i64(&out, 1), 20);
    }

    #[test]
    fn test_union_merge_same_pk_multiple_entries() {
        // a: [(1,1,20),(1,1,30)], b: [(1,1,10),(1,1,25)] → payloads [10,20,25,30]
        let schema = make_schema_u64_i64();
        let a = make_batch(&schema, &[(1, 1, 20), (1, 1, 30)]);
        let b = make_batch(&schema, &[(1, 1, 10), (1, 1, 25)]);
        let out = op_union(&a, Some(&b), &schema);
        assert_eq!(out.count, 4);
        assert!(out.sorted);
        assert_eq!(get_payload_i64(&out, 0), 10);
        assert_eq!(get_payload_i64(&out, 1), 20);
        assert_eq!(get_payload_i64(&out, 2), 25);
        assert_eq!(get_payload_i64(&out, 3), 30);
    }

    #[test]
    fn test_union_merge_mixed_same_diff_pk() {
        // a: [(1,1,20),(3,1,300)], b: [(1,1,10),(2,1,200)]
        // output PKs [1,1,2,3], vals [10,20,200,300]
        let schema = make_schema_u64_i64();
        let a = make_batch(&schema, &[(1, 1, 20), (3, 1, 300)]);
        let b = make_batch(&schema, &[(1, 1, 10), (2, 1, 200)]);
        let out = op_union(&a, Some(&b), &schema);
        assert_eq!(out.count, 4);
        assert!(out.sorted);
        assert_eq!(out.get_pk_lo(0), 1);
        assert_eq!(get_payload_i64(&out, 0), 10);
        assert_eq!(out.get_pk_lo(1), 1);
        assert_eq!(get_payload_i64(&out, 1), 20);
        assert_eq!(out.get_pk_lo(2), 2);
        assert_eq!(get_payload_i64(&out, 2), 200);
        assert_eq!(out.get_pk_lo(3), 3);
        assert_eq!(get_payload_i64(&out, 3), 300);
    }

    #[test]
    fn test_union_merge_same_pk_equal_payload() {
        // Same (PK, payload), opposite weights — must be adjacent for consolidation
        let schema = make_schema_u64_i64();
        let a = make_batch(&schema, &[(1, 1, 10)]);
        let b = make_batch(&schema, &[(1, -1, 10)]);
        let out = op_union(&a, Some(&b), &schema);
        assert_eq!(out.count, 2);
        assert!(out.sorted);
        assert_eq!(get_payload_i64(&out, 0), 10);
        assert_eq!(get_payload_i64(&out, 1), 10);
    }

    // -----------------------------------------------------------------------
    // Reduce GI-path regression: same PK, multiple payloads
    // -----------------------------------------------------------------------

    /// 3-column source schema: U64 pk (pk_index=0), I64 grp, STRING val (nullable).
    fn make_schema_3col_grp_str() -> SchemaDescriptor {
        let mut s = SchemaDescriptor {
            num_columns: 3,
            pk_index: 0,
            columns: [SchemaColumn { type_code: 0, size: 0, nullable: 0, _pad: 0 }; 64],
        };
        s.columns[0] = SchemaColumn { type_code: type_code::U64, size: 8, nullable: 0, _pad: 0 };
        s.columns[1] = SchemaColumn { type_code: type_code::I64, size: 8, nullable: 0, _pad: 0 };
        s.columns[2] = SchemaColumn {
            type_code: type_code::STRING, size: 16, nullable: 1, _pad: 0,
        };
        s
    }

    /// 3-column reduce output schema: U128 pk (pk_index=0), I64 grp, I64 agg (nullable).
    ///
    /// The accumulator stores only the first 8 bytes of the German string as i64
    /// (UniversalAccumulator.output_column_type() returns TYPE_I64 for STRING MAX).
    fn make_reduce_str_out_schema() -> SchemaDescriptor {
        let mut s = SchemaDescriptor {
            num_columns: 3,
            pk_index: 0,
            columns: [SchemaColumn { type_code: 0, size: 0, nullable: 0, _pad: 0 }; 64],
        };
        s.columns[0] = SchemaColumn { type_code: type_code::U128, size: 16, nullable: 0, _pad: 0 };
        s.columns[1] = SchemaColumn { type_code: type_code::I64, size: 8, nullable: 0, _pad: 0 };
        s.columns[2] = SchemaColumn { type_code: type_code::I64, size: 8, nullable: 1, _pad: 0 };
        s
    }

    /// Build a 3-column OwnedBatch (U64 pk, I64 grp, STRING val) from tuples.
    /// All strings must be <= 12 bytes (inline, no blob needed).
    fn make_batch_3col_grp_str(
        schema: &SchemaDescriptor,
        rows: &[(u64, i64, i64, &str)],
    ) -> OwnedBatch {
        let n = rows.len();
        let mut b = OwnedBatch::with_schema(*schema, n.max(1));
        b.count = 0;
        for &(pk, w, grp, val) in rows {
            b.pk_lo.extend_from_slice(&pk.to_le_bytes());
            b.pk_hi.extend_from_slice(&0u64.to_le_bytes());
            b.weight.extend_from_slice(&w.to_le_bytes());
            b.null_bmp.extend_from_slice(&0u64.to_le_bytes());
            b.col_data[0].extend_from_slice(&grp.to_le_bytes());
            let bytes = val.as_bytes();
            assert!(bytes.len() <= SHORT_STRING_THRESHOLD, "use inline strings only");
            let mut gs = [0u8; 16];
            gs[0..4].copy_from_slice(&(bytes.len() as u32).to_le_bytes());
            gs[4..4 + bytes.len()].copy_from_slice(bytes);
            b.col_data[1].extend_from_slice(&gs);
            b.count += 1;
        }
        b.sorted = true;
        b.consolidated = true;
        b
    }

    /// Build a GI OwnedBatch (U128 pk: ck_lo=source_pk_lo, ck_hi=gc_u64; I64 payload: spk_hi).
    fn make_gi_batch(rows: &[(u64, u64, i64)]) -> OwnedBatch {
        let gi_schema = make_gi_schema();
        let n = rows.len();
        let mut b = OwnedBatch::with_schema(gi_schema, n.max(1));
        b.count = 0;
        for &(ck_lo, gc_u64, spk_hi) in rows {
            b.pk_lo.extend_from_slice(&ck_lo.to_le_bytes());
            b.pk_hi.extend_from_slice(&gc_u64.to_le_bytes());
            b.weight.extend_from_slice(&1i64.to_le_bytes());
            b.null_bmp.extend_from_slice(&0u64.to_le_bytes());
            b.col_data[0].extend_from_slice(&spk_hi.to_le_bytes());
            b.count += 1;
        }
        b.sorted = true;
        b.consolidated = true;
        b
    }

    /// GI path bug: same PK, two different string payloads — the `if` must be `while`.
    ///
    /// Scenario (simulates tick 2 of a reduce):
    ///   trace_in = {(pk=1,grp=1,val="apple",w=+1), (pk=1,grp=1,val="zebra",w=+1)}
    ///   trace_out = empty (no prior aggregate)
    ///   delta     = {(pk=1,grp=1,val="apple",w=-1)}
    ///   GI        = {(ck_lo=1, ck_hi=group=1)}
    ///
    /// With the fix, the GI loop reads both apple and zebra from trace_in.
    /// The replay is {apple+1, zebra+1, apple−1} → consolidated {zebra+1}.
    /// MAX = zebra's 8-byte compare key (first 8 bytes of the German string).
    ///
    /// With the bug (if instead of while), only apple is read from trace_in.
    /// Replay = {apple+1, apple−1} → consolidated {} → empty → no output row.
    #[test]
    fn test_reduce_gi_same_pk_multiple_payloads() {
        use std::sync::Arc;
        use crate::read_cursor::create_cursor_from_snapshots;

        let input_schema = make_schema_3col_grp_str();
        let output_schema = make_reduce_str_out_schema();
        let gi_schema = make_gi_schema();

        // trace_in: apple and zebra both at PK=1 (apple sorts first by payload)
        let ti_batch = Arc::new(make_batch_3col_grp_str(
            &input_schema,
            &[(1, 1, 1, "apple"), (1, 1, 1, "zebra")],
        ));

        // GI: only PK=1 → group gc_u64=1
        let gi_batch = Arc::new(make_gi_batch(&[(1, 1, 0)]));

        // trace_out: empty (no previous aggregate, no retraction emitted)
        let to_batch = Arc::new(OwnedBatch::empty(output_schema.num_columns as usize - 1));

        // delta: retract apple at PK=1
        let delta = make_batch_3col_grp_str(&input_schema, &[(1, -1, 1, "apple")]);

        let mut ti_handle =
            unsafe { create_cursor_from_snapshots(&[ti_batch], &[], input_schema) };
        let mut gi_handle =
            unsafe { create_cursor_from_snapshots(&[gi_batch], &[], gi_schema) };
        let mut to_handle =
            unsafe { create_cursor_from_snapshots(&[to_batch], &[], output_schema) };

        // MAX on STRING agg col (col_idx=2, type=STRING); no AVI
        let agg_desc = AggDescriptor {
            col_idx: 2,
            agg_op: AGG_MAX,
            col_type_code: type_code::STRING,
            _pad: [0; 2],
        };

        let (out, _fin) = op_reduce(
            &delta,
            Some(&mut ti_handle.cursor),
            &mut to_handle.cursor,
            &input_schema,
            &output_schema,
            &[1u32],            // group_by_cols: col 1 (grp)
            &[agg_desc],
            None,               // avi_cursor
            false,              // avi_for_max
            type_code::STRING,  // avi_agg_col_type_code (unused; no AVI)
            &[1u32],            // avi_group_by_cols (unused)
            None,               // avi_input_schema
            Some(&mut gi_handle.cursor), // gi_cursor
            1u32,               // gi_col_idx: grp column
            type_code::I64,     // gi_col_type_code
            None,               // finalize_prog
            None,               // finalize_out_schema
        );

        // The accumulator stores the first 8 bytes of the German string as i64.
        // "zebra" first 8 bytes: [len=5, 'z'=122, 'e'=101, 'b'=98, 'r'=114]
        // "apple" first 8 bytes: [len=5, 'a'=97,  'p'=112, 'p'=112, 'l'=108]
        let zebra_ck = i64::from_le_bytes([5, 0, 0, 0, 122, 101, 98, 114]);
        let apple_ck = i64::from_le_bytes([5, 0, 0, 0, 97, 112, 112, 108]);
        assert!(zebra_ck > apple_ck, "test invariant: zebra_ck > apple_ck");

        // With fix: replay = {apple+1, zebra+1, apple−1} → {zebra+1}; one output row.
        // With bug: replay = {apple+1, apple−1} → {}; no output row.
        assert_eq!(out.count, 1,
            "GI loop must be `while` to include zebra after apple is retracted; \
             `if` leaves replay empty → no output");

        // Output payload layout: col_data[0]=grp(I64), col_data[1]=agg(I64)
        let agg = crate::util::read_i64_le(&out.col_data[1], 0);
        assert_eq!(agg, zebra_ck,
            "MAX of {{zebra+1}} must be zebra_ck; got {agg} (apple_ck={apple_ck})");
    }

    // -----------------------------------------------------------------------
    // Exchange repartition tests
    // -----------------------------------------------------------------------

    fn make_schema_u128_i64() -> SchemaDescriptor {
        let mut columns = [SchemaColumn { type_code: 0, size: 0, nullable: 0, _pad: 0 }; 64];
        columns[0] = SchemaColumn { type_code: type_code::U128, size: 16, nullable: 0, _pad: 0 };
        columns[1] = SchemaColumn { type_code: type_code::I64, size: 8, nullable: 0, _pad: 0 };
        SchemaDescriptor { num_columns: 2, pk_index: 0, columns }
    }

    fn total_rows(batches: &[OwnedBatch]) -> usize {
        batches.iter().map(|b| b.count).sum()
    }

    #[test]
    fn test_repartition_batch_pk_routing() {
        let schema = make_schema_u64_i64();
        let num_workers = 4;
        let pk_vals: &[u64] = &[1, 7, 42, 100, 255, 1024, 65537, 999983];

        let mut b = OwnedBatch::with_schema(schema, pk_vals.len());
        b.count = 0;
        for &pk in pk_vals {
            b.pk_lo.extend_from_slice(&pk.to_le_bytes());
            b.pk_hi.extend_from_slice(&0u64.to_le_bytes());
            b.weight.extend_from_slice(&1i64.to_le_bytes());
            b.null_bmp.extend_from_slice(&0u64.to_le_bytes());
            b.col_data[0].extend_from_slice(&0i64.to_le_bytes());
            b.count += 1;
        }

        let sub_batches = op_repartition_batch(&b, &[0u32], &schema, num_workers);
        assert_eq!(total_rows(&sub_batches), pk_vals.len());

        for &pk in pk_vals {
            let expected = worker_for_partition(
                crate::partitioned_table::partition_for_key(pk, 0),
                num_workers,
            );
            let found = (0..sub_batches[expected].count)
                .any(|r| sub_batches[expected].get_pk_lo(r) == pk);
            assert!(found, "pk={pk} not found in worker {expected}");
        }
    }

    #[test]
    fn test_repartition_batch_u128_pk() {
        let schema = make_schema_u128_i64();
        let num_workers = 4;
        let pk_pairs: &[(u64, u64)] = &[
            (0, 1),
            (0xDEAD_BEEF, 0xCAFE_BABE),
            (u64::MAX, u64::MAX),
            (42, 7),
        ];

        let n = pk_pairs.len();
        let mut b = OwnedBatch::with_schema(schema, n);
        b.count = 0;
        for &(lo, hi) in pk_pairs {
            b.pk_lo.extend_from_slice(&lo.to_le_bytes());
            b.pk_hi.extend_from_slice(&hi.to_le_bytes());
            b.weight.extend_from_slice(&1i64.to_le_bytes());
            b.null_bmp.extend_from_slice(&0u64.to_le_bytes());
            b.col_data[0].extend_from_slice(&0i64.to_le_bytes());
            b.count += 1;
        }

        let sub_batches = op_repartition_batch(&b, &[0u32], &schema, num_workers);
        assert_eq!(total_rows(&sub_batches), n);

        for &(lo, hi) in pk_pairs {
            let expected = worker_for_partition(
                crate::partitioned_table::partition_for_key(lo, hi),
                num_workers,
            );
            let found = (0..sub_batches[expected].count).any(|r| {
                sub_batches[expected].get_pk_lo(r) == lo
                    && sub_batches[expected].get_pk_hi(r) == hi
            });
            assert!(found, "pk=({lo},{hi}) not in worker {expected}");
        }
    }

    #[test]
    fn test_repartition_batch_group_col() {
        let schema = make_schema_u64_i64();
        let num_workers = 4;
        let same_val: i64 = 42;

        let mut b = OwnedBatch::with_schema(schema, 4);
        b.count = 0;
        for pk in [1u64, 2, 3, 4] {
            b.pk_lo.extend_from_slice(&pk.to_le_bytes());
            b.pk_hi.extend_from_slice(&0u64.to_le_bytes());
            b.weight.extend_from_slice(&1i64.to_le_bytes());
            b.null_bmp.extend_from_slice(&0u64.to_le_bytes());
            b.col_data[0].extend_from_slice(&same_val.to_le_bytes());
            b.count += 1;
        }

        let sub_batches = op_repartition_batch(&b, &[1u32], &schema, num_workers);
        assert_eq!(total_rows(&sub_batches), 4);
        let non_empty = sub_batches.iter().filter(|sb| sb.count > 0).count();
        assert_eq!(non_empty, 1, "all rows with same group key must go to one worker");
    }

    #[test]
    fn test_repartition_batch_string_col() {
        let schema = make_schema_u64_string();
        let num_workers = 4;

        // Short string "hello" (≤ 12 bytes): two rows must go to same worker
        let b = make_batch_str(&schema, &[(1, 1, "hello"), (2, 1, "hello"), (3, 1, "world")]);
        let sub_batches = op_repartition_batch(&b, &[1u32], &schema, num_workers);
        assert_eq!(total_rows(&sub_batches), 3);

        let mut worker_of_1 = None;
        for w in 0..num_workers {
            for r in 0..sub_batches[w].count {
                if sub_batches[w].get_pk_lo(r) == 1 {
                    worker_of_1 = Some(w);
                }
            }
        }
        let w1 = worker_of_1.expect("pk=1 must be in some worker");
        let pk2_same = (0..sub_batches[w1].count).any(|r| sub_batches[w1].get_pk_lo(r) == 2);
        assert!(pk2_same, "same short string 'hello' must route to same worker");

        // Long string (> 12 bytes): two rows must go to same worker
        let long_str = "this is a longer string for heap";
        let b2 = make_batch_str(&schema, &[(10, 1, long_str), (11, 1, long_str)]);
        let sub2 = op_repartition_batch(&b2, &[1u32], &schema, num_workers);
        assert_eq!(total_rows(&sub2), 2);

        let mut worker_of_10 = None;
        for w in 0..num_workers {
            for r in 0..sub2[w].count {
                if sub2[w].get_pk_lo(r) == 10 {
                    worker_of_10 = Some(w);
                }
            }
        }
        let w10 = worker_of_10.expect("pk=10 must be in some worker");
        let pk11_same = (0..sub2[w10].count).any(|r| sub2[w10].get_pk_lo(r) == 11);
        assert!(pk11_same, "same long string must route to same worker");
    }

    #[test]
    fn test_repartition_batch_no_consolidation() {
        let schema = make_schema_u64_i64();
        let b = make_batch(&schema, &[(1, 1, 10), (2, 1, 20), (3, 1, 30)]);
        assert!(b.sorted && b.consolidated);

        let sub_batches = op_repartition_batch(&b, &[0u32], &schema, 4);
        for sb in &sub_batches {
            if sb.count > 0 {
                assert!(!sb.consolidated, "repartition must not propagate consolidated");
                assert!(!sb.sorted, "repartition must not propagate sorted");
            }
        }
    }

    #[test]
    fn test_relay_scatter_consolidated_path() {
        let schema = make_schema_u64_i64();
        let num_workers = 4;
        let b0 = make_batch(&schema, &[(1, 1, 10), (5, 1, 50), (9, 1, 90)]);
        let b1 = make_batch(&schema, &[(2, 1, 20), (6, 1, 60), (10, 1, 100)]);
        assert!(b0.consolidated && b1.consolidated);

        let sources: Vec<Option<&OwnedBatch>> = vec![Some(&b0), Some(&b1)];
        let result = op_relay_scatter(&sources, &[0u32], &schema, num_workers);

        assert_eq!(total_rows(&result), 6);
        for sb in &result {
            if sb.count > 0 {
                assert!(sb.consolidated, "merged path output must be consolidated");
                assert!(sb.sorted, "merged path output must be sorted");
            }
        }
    }

    #[test]
    fn test_relay_scatter_fallback_path() {
        let schema = make_schema_u64_i64();
        let num_workers = 4;
        let b0 = make_batch(&schema, &[(1, 1, 10)]);
        let mut b1 = make_batch(&schema, &[(2, 1, 20)]);
        b1.consolidated = false;

        let sources: Vec<Option<&OwnedBatch>> = vec![Some(&b0), Some(&b1)];
        let result = op_relay_scatter(&sources, &[0u32], &schema, num_workers);

        assert_eq!(total_rows(&result), 2);
        for sb in &result {
            if sb.count > 0 {
                assert!(!sb.consolidated, "fallback path output must not be consolidated");
            }
        }
    }

    #[test]
    fn test_multi_scatter_pk_flag_propagation() {
        let schema = make_schema_u64_i64();
        let num_workers = 4;
        let b = make_batch(&schema, &[(1, 1, 10), (2, 1, 20)]);
        assert!(b.sorted && b.consolidated);

        let specs: Vec<&[u32]> = vec![&[0u32], &[1u32]];
        let result = op_multi_scatter(&b, &specs, &schema, num_workers);

        // PK spec (specs[0]): sub-batches must inherit sorted + consolidated
        for sb in &result[0] {
            if sb.count > 0 {
                assert!(sb.sorted, "PK spec sub-batch must inherit sorted");
                assert!(sb.consolidated, "PK spec sub-batch must inherit consolidated");
            }
        }
        // Non-PK spec (specs[1]): must NOT inherit
        for sb in &result[1] {
            if sb.count > 0 {
                assert!(!sb.sorted, "non-PK spec sub-batch must not inherit sorted");
                assert!(!sb.consolidated, "non-PK spec sub-batch must not inherit consolidated");
            }
        }
    }

    #[test]
    fn test_repartition_row_count() {
        let schema = make_schema_u64_i64();
        let rows: Vec<(u64, i64, i64)> = (1u64..=100).map(|i| (i, 1, i as i64 * 10)).collect();
        let b = make_batch(&schema, &rows);

        let sub_batches = op_repartition_batch(&b, &[0u32], &schema, 4);
        assert_eq!(total_rows(&sub_batches), 100, "total rows must equal input count");
    }

    // -----------------------------------------------------------------------
    // op_distinct: same-PK update regression
    // -----------------------------------------------------------------------

    #[test]
    fn test_distinct_update_same_pk() {
        use std::sync::Arc;
        use crate::read_cursor::create_cursor_from_snapshots;

        let schema = make_schema_u64_i64();

        // Trace: (PK=1, val=100, w=+1) — a row inserted in a previous tick.
        let trace_batch = Arc::new(make_batch(&schema, &[(1, 1, 100)]));
        let mut cursor_handle =
            unsafe { create_cursor_from_snapshots(&[trace_batch], &[], schema) };

        // Delta: UPDATE PK=1 sets val=100 → 200.
        // _enforce_unique_pk emits (PK=1, val=100, w=-1) and (PK=1, val=200, w=+1).
        // Both rows have the same PK but different payloads; sorted by payload ascending.
        let delta = make_batch(&schema, &[(1, -1, 100), (1, 1, 200)]);

        let (out, _consolidated) = op_distinct(&delta, &mut cursor_handle.cursor, &schema);

        assert_eq!(
            out.count, 2,
            "expected 2 output rows after same-PK update, got {}",
            out.count
        );

        assert_eq!(out.get_pk_lo(0), 1);
        assert_eq!(get_payload_i64(&out, 0), 100);
        assert_eq!(out.get_weight(0), -1);

        assert_eq!(out.get_pk_lo(1), 1);
        assert_eq!(get_payload_i64(&out, 1), 200);
        assert_eq!(out.get_weight(1), 1);
    }

    #[test]
    fn test_repartition_routing_contract() {
        let schema = make_schema_u64_i64();
        let num_workers = 4;
        let vals: Vec<i64> = (0..64i64).map(|i| i * 997 + 1).collect();

        let mut b = OwnedBatch::with_schema(schema, vals.len());
        b.count = 0;
        for (i, &v) in vals.iter().enumerate() {
            b.pk_lo.extend_from_slice(&((i + 1) as u64).to_le_bytes());
            b.pk_hi.extend_from_slice(&0u64.to_le_bytes());
            b.weight.extend_from_slice(&1i64.to_le_bytes());
            b.null_bmp.extend_from_slice(&0u64.to_le_bytes());
            b.col_data[0].extend_from_slice(&v.to_le_bytes());
            b.count += 1;
        }

        let sub_batches = op_repartition_batch(&b, &[1u32], &schema, num_workers);
        assert_eq!(total_rows(&sub_batches), vals.len());

        for &v in &vals {
            let expected_partition =
                crate::partitioned_table::partition_for_key(v as u64, 0);
            let expected_worker = worker_for_partition(expected_partition, num_workers);
            let found = (0..sub_batches[expected_worker].count).any(|r| {
                i64::from_le_bytes(
                    sub_batches[expected_worker].col_data[0][r * 8..r * 8 + 8]
                        .try_into()
                        .unwrap(),
                ) == v
            });
            assert!(found, "val={v} should be in worker {expected_worker}");
        }
    }

    // -----------------------------------------------------------------------
    // op_filter tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_op_filter_basic() {
        use crate::scalar_func::ScalarFuncKind;
        use crate::expr::ExprProgram;

        let schema = make_schema_u64_i64();
        // 3 rows: pk=1 val=5, pk=2 val=15, pk=3 val=25
        let batch = make_batch(&schema, &[(1, 1, 5), (2, 1, 15), (3, 1, 25)]);

        // Predicate: col1 > 10
        let code = vec![
            1i64, 0, 1, 0,  // LOAD_COL_INT r0 = col[1]
            3, 1, 10, 0,    // LOAD_CONST r1 = 10
            17, 2, 0, 1,    // CMP_GT r2 = (r0 > r1)
        ];
        let prog = ExprProgram::new(code, 3, 2, vec![]);
        let func = ScalarFuncKind::ExprPredicate(prog);

        let out = op_filter(&batch, &func, &schema);
        assert_eq!(out.count, 2, "only pk=2 and pk=3 pass val>10");
        assert_eq!(out.get_pk_lo(0), 2);
        assert_eq!(out.get_pk_lo(1), 3);
    }

    #[test]
    fn test_op_filter_consolidated_flag() {
        use crate::scalar_func::ScalarFuncKind;

        // Null func passes everything
        let schema = make_schema_u64_i64();
        let mut batch = make_batch(&schema, &[(1, 1, 10), (2, 1, 20)]);
        batch.consolidated = true;

        let func = ScalarFuncKind::Null;
        let out = op_filter(&batch, &func, &schema);
        assert_eq!(out.count, 2);
        assert!(out.consolidated, "consolidated input + pass-all → consolidated output");
        assert!(out.sorted);
    }

    // -----------------------------------------------------------------------
    // op_negate tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_op_negate_weights() {
        let schema = make_schema_u64_i64();
        let batch = make_batch(&schema, &[(1, 3, 10), (2, -1, 20)]);
        let out = op_negate(&batch);
        assert_eq!(out.count, 2);
        assert_eq!(out.get_weight(0), -3);
        assert_eq!(out.get_weight(1), 1);
        // Payload preserved
        assert_eq!(get_payload_i64(&out, 0), 10);
        assert_eq!(get_payload_i64(&out, 1), 20);
        assert!(out.consolidated);
    }

    // -----------------------------------------------------------------------
    // op_map tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_op_map_empty_batch() {
        use crate::scalar_func::ScalarFuncKind;

        let schema = make_schema_u64_i64();
        let batch = OwnedBatch::with_schema(schema, 1);
        // count=0 by default after with_schema
        let empty_batch = OwnedBatch::empty(1);

        let func = ScalarFuncKind::Null;
        let out = op_map(&empty_batch, &func, &schema, &schema, -1);
        assert_eq!(out.count, 0);
    }

    // -----------------------------------------------------------------------
    // op_distinct tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_op_distinct_boundary() {
        use std::sync::Arc;
        use crate::read_cursor::create_cursor_from_snapshots;

        let schema = make_schema_u64_i64();

        // Empty trace → all positive deltas emit +1
        let empty = Arc::new(OwnedBatch::empty(1));
        let mut ch = unsafe { create_cursor_from_snapshots(&[empty.clone()], &[], schema) };

        // Delta: pk=1 w=+3, pk=2 w=+1
        let delta = make_batch(&schema, &[(1, 3, 10), (2, 1, 20)]);
        let (out, _) = op_distinct(&delta, &mut ch.cursor, &schema);
        // 0→positive: both emit +1
        assert_eq!(out.count, 2);
        assert_eq!(out.get_weight(0), 1);
        assert_eq!(out.get_weight(1), 1);

        // Now trace has pk=1 w=3 and pk=2 w=1
        // Delta: pk=1 w=-2 (3→1, still positive, no output), pk=2 w=-1 (1→0, emit -1)
        let trace_batch = Arc::new(make_batch(&schema, &[(1, 3, 10), (2, 1, 20)]));
        let mut ch2 = unsafe { create_cursor_from_snapshots(&[trace_batch], &[], schema) };
        let delta2 = make_batch(&schema, &[(1, -2, 10), (2, -1, 20)]);
        let (out2, _) = op_distinct(&delta2, &mut ch2.cursor, &schema);
        // pk=1: 3→1, positive→positive, no change
        // pk=2: 1→0, positive→non-positive, emit -1
        assert_eq!(out2.count, 1);
        assert_eq!(out2.get_pk_lo(0), 2);
        assert_eq!(out2.get_weight(0), -1);
    }

    #[test]
    fn test_op_distinct_consolidated_flag() {
        use std::sync::Arc;
        use crate::read_cursor::create_cursor_from_snapshots;

        let schema = make_schema_u64_i64();
        let empty = Arc::new(OwnedBatch::empty(1));
        let mut ch = unsafe { create_cursor_from_snapshots(&[empty], &[], schema) };

        let delta = make_batch(&schema, &[(1, 1, 10)]);
        let (out, consolidated) = op_distinct(&delta, &mut ch.cursor, &schema);
        assert!(out.consolidated, "distinct output must be consolidated");
        assert!(out.sorted, "distinct output must be sorted");
        assert!(consolidated.consolidated, "consolidated output must be consolidated");
    }

    // -----------------------------------------------------------------------
    // op_scan_trace tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_op_scan_trace_chunked() {
        use std::sync::Arc;
        use crate::read_cursor::create_cursor_from_snapshots;

        let schema = make_schema_u64_i64();
        // 5 rows in trace
        let trace = Arc::new(make_batch(&schema, &[
            (1, 1, 10), (2, 1, 20), (3, 1, 30), (4, 1, 40), (5, 1, 50),
        ]));
        let mut ch = unsafe { create_cursor_from_snapshots(&[trace], &[], schema) };

        // First scan: chunk_limit=3 → get 3 rows
        let out1 = op_scan_trace(&mut ch.cursor, &schema, 3);
        assert_eq!(out1.count, 3);
        assert_eq!(out1.get_pk_lo(0), 1);
        assert_eq!(out1.get_pk_lo(2), 3);
        assert!(out1.sorted);
        assert!(out1.consolidated);

        // Second scan: remaining 2 rows
        let out2 = op_scan_trace(&mut ch.cursor, &schema, 3);
        assert_eq!(out2.count, 2);
        assert_eq!(out2.get_pk_lo(0), 4);
        assert_eq!(out2.get_pk_lo(1), 5);
    }

    #[test]
    fn test_op_scan_trace_empty() {
        use std::sync::Arc;
        use crate::read_cursor::create_cursor_from_snapshots;

        let schema = make_schema_u64_i64();
        let empty = Arc::new(OwnedBatch::empty(1));
        let mut ch = unsafe { create_cursor_from_snapshots(&[empty], &[], schema) };

        let out = op_scan_trace(&mut ch.cursor, &schema, 10);
        assert_eq!(out.count, 0);
    }

    // -----------------------------------------------------------------------
    // op_reduce tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_reduce_sum_retraction() {
        use std::sync::Arc;
        use crate::read_cursor::create_cursor_from_snapshots;
        use crate::compact::type_code;

        // Input: pk(U64), grp(I64), val(I64)
        let mut in_schema = make_schema_u64_i64();
        in_schema.num_columns = 3;
        in_schema.columns[2] = crate::compact::SchemaColumn {
            type_code: type_code::I64, size: 8, nullable: 0, _pad: 0,
        };

        // Output: pk(U128), grp(I64), sum(I64)
        let mut out_schema = crate::compact::SchemaDescriptor {
            num_columns: 3,
            pk_index: 0,
            columns: [crate::compact::SchemaColumn { type_code: 0, size: 0, nullable: 0, _pad: 0 }; 64],
        };
        out_schema.columns[0] = crate::compact::SchemaColumn {
            type_code: type_code::U128, size: 16, nullable: 0, _pad: 0,
        };
        out_schema.columns[1] = crate::compact::SchemaColumn {
            type_code: type_code::I64, size: 8, nullable: 0, _pad: 0,
        };
        out_schema.columns[2] = crate::compact::SchemaColumn {
            type_code: type_code::I64, size: 8, nullable: 1, _pad: 0,
        };

        // Empty trace_out
        let empty_out = Arc::new(OwnedBatch::empty(2));
        let mut to_ch = unsafe { create_cursor_from_snapshots(&[empty_out], &[], out_schema) };

        // Tick 1: insert 3 rows in group 10: val=100, val=200, val=300
        let mut delta1 = OwnedBatch::with_schema(in_schema, 3);
        delta1.count = 0;
        for (pk, val) in [(1u64, 100i64), (2, 200), (3, 300)] {
            delta1.pk_lo.extend_from_slice(&pk.to_le_bytes());
            delta1.pk_hi.extend_from_slice(&0u64.to_le_bytes());
            delta1.weight.extend_from_slice(&1i64.to_le_bytes());
            delta1.null_bmp.extend_from_slice(&0u64.to_le_bytes());
            delta1.col_data[0].extend_from_slice(&10i64.to_le_bytes()); // grp=10
            delta1.col_data[1].extend_from_slice(&val.to_le_bytes());
            delta1.count += 1;
        }
        delta1.sorted = true;
        delta1.consolidated = true;

        let agg = AggDescriptor {
            col_idx: 2, agg_op: AGG_SUM, col_type_code: type_code::I64, _pad: [0; 2],
        };

        let (out1, _) = op_reduce(
            &delta1, None, &mut to_ch.cursor,
            &in_schema, &out_schema, &[1u32], &[agg],
            None, false, 0, &[], None, None, 0, 0, None, None,
        );
        // SUM of (100+200+300) = 600
        assert_eq!(out1.count, 1);
        let sum1 = crate::util::read_i64_le(&out1.col_data[1], 0);
        assert_eq!(sum1, 600);

        // Tick 2: retract pk=2 (val=200) → SUM should go from 600 to 400
        // Need trace_out with previous aggregate
        let prev_out = Arc::new(out1);
        let mut to_ch2 = unsafe { create_cursor_from_snapshots(&[prev_out], &[], out_schema) };

        let mut delta2 = OwnedBatch::with_schema(in_schema, 1);
        delta2.count = 0;
        delta2.pk_lo.extend_from_slice(&2u64.to_le_bytes());
        delta2.pk_hi.extend_from_slice(&0u64.to_le_bytes());
        delta2.weight.extend_from_slice(&(-1i64).to_le_bytes());
        delta2.null_bmp.extend_from_slice(&0u64.to_le_bytes());
        delta2.col_data[0].extend_from_slice(&10i64.to_le_bytes());
        delta2.col_data[1].extend_from_slice(&200i64.to_le_bytes());
        delta2.count += 1;
        delta2.sorted = true;
        delta2.consolidated = true;

        let (out2, _) = op_reduce(
            &delta2, None, &mut to_ch2.cursor,
            &in_schema, &out_schema, &[1u32], &[agg],
            None, false, 0, &[], None, None, 0, 0, None, None,
        );
        // Output: retract old sum (600, w=-1) + insert new sum (400, w=+1) = 2 rows
        assert_eq!(out2.count, 2);
    }

    #[test]
    fn test_reduce_count() {
        use std::sync::Arc;
        use crate::read_cursor::create_cursor_from_snapshots;
        use crate::compact::type_code;

        // Input: pk(U64), val(I64)
        let in_schema = make_schema_u64_i64();

        // Output: pk(U128), count(I64)
        let mut out_schema = crate::compact::SchemaDescriptor {
            num_columns: 2,
            pk_index: 0,
            columns: [crate::compact::SchemaColumn { type_code: 0, size: 0, nullable: 0, _pad: 0 }; 64],
        };
        out_schema.columns[0] = crate::compact::SchemaColumn {
            type_code: type_code::U128, size: 16, nullable: 0, _pad: 0,
        };
        out_schema.columns[1] = crate::compact::SchemaColumn {
            type_code: type_code::I64, size: 8, nullable: 1, _pad: 0,
        };

        let empty_out = Arc::new(OwnedBatch::empty(1));
        let mut to_ch = unsafe { create_cursor_from_snapshots(&[empty_out], &[], out_schema) };

        // 3 rows: pk=1,2,3 all GROUP BY pk (single group using pk as group)
        let delta = make_batch(&in_schema, &[(1, 1, 10), (2, 1, 20), (3, 1, 30)]);

        let agg = AggDescriptor {
            col_idx: 0, agg_op: AGG_COUNT, col_type_code: type_code::I64, _pad: [0; 2],
        };

        // GROUP BY pk → each row is its own group
        let (out, _) = op_reduce(
            &delta, None, &mut to_ch.cursor,
            &in_schema, &out_schema, &[0u32], &[agg],
            None, false, 0, &[], None, None, 0, 0, None, None,
        );
        // Each pk forms its own group, COUNT=1 for each
        assert_eq!(out.count, 3);
        for i in 0..3 {
            let count = crate::util::read_i64_le(&out.col_data[0], i * 8);
            assert_eq!(count, 1, "each single-row group has count=1");
        }
    }

    // -----------------------------------------------------------------------
    // op_anti_join_delta_trace / op_semi_join_delta_trace
    // -----------------------------------------------------------------------

    #[test]
    fn test_anti_join_dt_basic() {
        use std::sync::Arc;
        use crate::read_cursor::create_cursor_from_snapshots;

        let schema = make_schema_u64_i64();

        // Trace: pk=2 and pk=4 exist
        let trace = Arc::new(make_batch(&schema, &[(2, 1, 200), (4, 1, 400)]));
        let mut ch = unsafe { create_cursor_from_snapshots(&[trace], &[], schema) };

        // Delta: pk=1,2,3
        let delta = make_batch(&schema, &[(1, 1, 10), (2, 1, 20), (3, 1, 30)]);

        let out = op_anti_join_delta_trace(&delta, &mut ch.cursor, &schema);
        // pk=2 is in trace, so output should be pk=1 and pk=3
        assert_eq!(out.count, 2);
        assert_eq!(out.get_pk_lo(0), 1);
        assert_eq!(out.get_pk_lo(1), 3);
        assert!(out.consolidated);
    }

    #[test]
    fn test_semi_join_dt_basic() {
        use std::sync::Arc;
        use crate::read_cursor::create_cursor_from_snapshots;

        let schema = make_schema_u64_i64();

        // Trace: pk=2 and pk=3 exist with positive weight
        let trace = Arc::new(make_batch(&schema, &[(2, 1, 200), (3, 1, 300)]));
        let mut ch = unsafe { create_cursor_from_snapshots(&[trace], &[], schema) };

        // Delta: pk=1,2,3,4
        let delta = make_batch(&schema, &[
            (1, 1, 10), (2, 1, 20), (3, 1, 30), (4, 1, 40),
        ]);

        let out = op_semi_join_delta_trace(&delta, &mut ch.cursor, &schema);
        // Only pk=2 and pk=3 have trace matches
        assert_eq!(out.count, 2);
        assert_eq!(out.get_pk_lo(0), 2);
        assert_eq!(out.get_pk_lo(1), 3);
    }

    #[test]
    fn test_semi_join_dt_nonconsolidated() {
        use std::sync::Arc;
        use crate::read_cursor::create_cursor_from_snapshots;

        let schema = make_schema_u64_i64();

        // Trace: pk=1 exists
        let trace = Arc::new(make_batch(&schema, &[(1, 1, 100)]));
        let mut ch = unsafe { create_cursor_from_snapshots(&[trace], &[], schema) };

        // Non-consolidated delta: pk=1 appears twice (w=2 and w=3)
        let mut delta = OwnedBatch::with_schema(schema, 2);
        delta.count = 0;
        for (pk, w, val) in [(1u64, 2i64, 10i64), (1u64, 3i64, 10i64)] {
            delta.pk_lo.extend_from_slice(&pk.to_le_bytes());
            delta.pk_hi.extend_from_slice(&0u64.to_le_bytes());
            delta.weight.extend_from_slice(&w.to_le_bytes());
            delta.null_bmp.extend_from_slice(&0u64.to_le_bytes());
            delta.col_data[0].extend_from_slice(&val.to_le_bytes());
            delta.count += 1;
        }
        delta.sorted = true;
        delta.consolidated = false;

        let out = op_semi_join_delta_trace(&delta, &mut ch.cursor, &schema);
        // After consolidation of delta: pk=1 w=5 → matches trace → 1 row
        assert_eq!(out.count, 1);
        assert_eq!(out.get_pk_lo(0), 1);
        assert_eq!(out.get_weight(0), 5);
    }

    // -----------------------------------------------------------------------
    // op_join_delta_trace_outer
    // -----------------------------------------------------------------------

    #[test]
    fn test_outer_join_null_fill() {
        use std::sync::Arc;
        use crate::read_cursor::create_cursor_from_snapshots;

        let left_schema = make_schema_u64_i64();
        let right_schema = make_schema_u64_i64();

        // Right trace: pk=1 val=100
        let trace = Arc::new(make_batch(&right_schema, &[(1, 1, 100)]));
        let mut ch = unsafe { create_cursor_from_snapshots(&[trace], &[], right_schema) };

        // Delta (left): pk=1 val=10 (matches), pk=2 val=20 (no match → null fill)
        let delta = make_batch(&left_schema, &[(1, 1, 10), (2, 1, 20)]);

        let out = op_join_delta_trace_outer(&delta, &mut ch.cursor, &left_schema, &right_schema);

        assert_eq!(out.count, 2);
        // pk=1: matched, left payload=10, right payload=100
        assert_eq!(out.get_pk_lo(0), 1);
        assert_eq!(get_payload_i64(&out, 0), 10); // left val

        // pk=2: no match, left payload=20, right columns null-filled
        assert_eq!(out.get_pk_lo(1), 2);
        assert_eq!(get_payload_i64(&out, 1), 20); // left val
        // Right column (payload col 1) should be null
        let null_word = u64::from_le_bytes(out.null_bmp[1 * 8..2 * 8].try_into().unwrap());
        assert!(null_word & 2 != 0, "right payload column (bit 1) should be null for non-matching row");
    }

    // -----------------------------------------------------------------------
    // op_gather_reduce
    // -----------------------------------------------------------------------

    #[test]
    fn test_gather_reduce_retraction() {
        use std::sync::Arc;
        use crate::read_cursor::create_cursor_from_snapshots;
        use crate::compact::type_code;

        // Schema: pk(U128), count(I64) — same as partial/output schema
        let mut schema = crate::compact::SchemaDescriptor {
            num_columns: 2,
            pk_index: 0,
            columns: [crate::compact::SchemaColumn { type_code: 0, size: 0, nullable: 0, _pad: 0 }; 64],
        };
        schema.columns[0] = crate::compact::SchemaColumn {
            type_code: type_code::U128, size: 16, nullable: 0, _pad: 0,
        };
        schema.columns[1] = crate::compact::SchemaColumn {
            type_code: type_code::I64, size: 8, nullable: 1, _pad: 0,
        };

        // Tick 1: two partial COUNT=2 from different workers → global COUNT=4
        let empty_out = Arc::new(OwnedBatch::empty(1));
        let mut to_ch = unsafe { create_cursor_from_snapshots(&[empty_out], &[], schema) };

        let mut partial1 = OwnedBatch::with_schema(schema, 2);
        partial1.count = 0;
        // Two entries for same group key (pk=1), count=2 each
        for count in [2i64, 2] {
            partial1.pk_lo.extend_from_slice(&1u64.to_le_bytes());
            partial1.pk_hi.extend_from_slice(&0u64.to_le_bytes());
            partial1.weight.extend_from_slice(&1i64.to_le_bytes());
            partial1.null_bmp.extend_from_slice(&0u64.to_le_bytes());
            partial1.col_data[0].extend_from_slice(&count.to_le_bytes());
            partial1.count += 1;
        }

        let agg = AggDescriptor {
            col_idx: 1, agg_op: AGG_SUM, col_type_code: type_code::I64, _pad: [0; 2],
        };

        let out1 = op_gather_reduce(&partial1, &mut to_ch.cursor, &schema, &[agg]);
        assert_eq!(out1.count, 1);
        let global_count = crate::util::read_i64_le(&out1.col_data[0], 0);
        assert_eq!(global_count, 4);

        // Tick 2: retract 1 from each worker → partial counts are -1 each → global delta = -2
        let prev_out = Arc::new(out1);
        let mut to_ch2 = unsafe { create_cursor_from_snapshots(&[prev_out], &[], schema) };

        let mut partial2 = OwnedBatch::with_schema(schema, 2);
        partial2.count = 0;
        for count in [-1i64, -1] {
            partial2.pk_lo.extend_from_slice(&1u64.to_le_bytes());
            partial2.pk_hi.extend_from_slice(&0u64.to_le_bytes());
            partial2.weight.extend_from_slice(&1i64.to_le_bytes());
            partial2.null_bmp.extend_from_slice(&0u64.to_le_bytes());
            partial2.col_data[0].extend_from_slice(&count.to_le_bytes());
            partial2.count += 1;
        }

        let out2 = op_gather_reduce(&partial2, &mut to_ch2.cursor, &schema, &[agg]);
        // Should have 2 rows: retract old (4, w=-1) + insert new (2, w=+1)
        assert_eq!(out2.count, 2);
    }

    fn make_schema_u64_f32() -> SchemaDescriptor {
        let mut columns = [SchemaColumn {
            type_code: 0, size: 0, nullable: 0, _pad: 0,
        }; 64];
        columns[0] = SchemaColumn {
            type_code: type_code::U64, size: 8, nullable: 0, _pad: 0,
        };
        columns[1] = SchemaColumn {
            type_code: type_code::F32, size: 4, nullable: 0, _pad: 0,
        };
        SchemaDescriptor { num_columns: 2, pk_index: 0, columns }
    }

    fn make_batch_f32(schema: &SchemaDescriptor, rows: &[(u64, i64, f32)]) -> OwnedBatch {
        let n = rows.len();
        let mut b = OwnedBatch::with_schema(*schema, n.max(1));
        b.count = 0;
        for &(pk, w, val) in rows {
            b.pk_lo.extend_from_slice(&pk.to_le_bytes());
            b.pk_hi.extend_from_slice(&0u64.to_le_bytes());
            b.weight.extend_from_slice(&w.to_le_bytes());
            b.null_bmp.extend_from_slice(&0u64.to_le_bytes());
            b.col_data[0].extend_from_slice(&val.to_bits().to_le_bytes());
            b.count += 1;
        }
        b.sorted = true;
        b.consolidated = true;
        b
    }

    #[test]
    fn test_argsort_delta_f32_group() {
        let schema = make_schema_u64_f32();
        let batch = make_batch_f32(&schema, &[
            (1, 1, 2.0f32),
            (2, 1, -1.0f32),
            (3, 1, 0.5f32),
        ]);
        let indices = argsort_delta(&batch, &schema, &[1]);
        // Sorted order by F32: -1.0 < 0.5 < 2.0
        assert_eq!(indices.len(), 3);
        let mb = batch.as_mem_batch();
        let vals: Vec<f32> = indices.iter().map(|&i| {
            let ptr = mb.get_col_ptr(i as usize, 0, 4);
            f32::from_bits(u32::from_le_bytes(ptr.try_into().unwrap()))
        }).collect();
        assert_eq!(vals, vec![-1.0f32, 0.5f32, 2.0f32]);
    }

    #[test]
    fn test_compare_by_group_cols_f32_negative() {
        let schema = make_schema_u64_f32();
        let batch = make_batch_f32(&schema, &[
            (1, 1, -5.0f32),
            (2, 1, 3.0f32),
        ]);
        let mb = batch.as_mem_batch();
        let ord = compare_by_group_cols(&mb, 0, 1, &schema, &[1]);
        assert_eq!(ord, std::cmp::Ordering::Less);
        let ord2 = compare_by_group_cols(&mb, 1, 0, &schema, &[1]);
        assert_eq!(ord2, std::cmp::Ordering::Greater);
    }

    #[test]
    fn test_promote_agg_col_f32_ordering() {
        let schema = make_schema_u64_f32();
        let vals = [-2.0f32, -1.0f32, 0.0f32, 1.0f32, 2.0f32];
        let batch = make_batch_f32(
            &schema,
            &vals.iter().enumerate().map(|(i, &v)| (i as u64 + 1, 1, v)).collect::<Vec<_>>(),
        );
        let mb = batch.as_mem_batch();
        let encoded: Vec<u64> = (0..vals.len()).map(|row| {
            promote_agg_col_to_u64_ordered(&mb, row, 1, 0, type_code::F32, false)
        }).collect();
        // Encoded values must be strictly ascending (order-preserving)
        for w in encoded.windows(2) {
            assert!(w[0] < w[1], "order-preserving invariant violated: {} >= {}", w[0], w[1]);
        }
        // Round-trip invariant
        for &v in &vals {
            let bits = v.to_bits();
            assert_eq!(
                ieee_order_bits_f32_reverse(ieee_order_bits_f32(bits)),
                bits,
                "round-trip failed for {:?}",
                v
            );
        }
    }

    #[test]
    fn test_extract_group_key_f32() {
        let schema = make_schema_u64_f32();
        let batch = make_batch_f32(&schema, &[
            (1, 1, 1.5f32),
            (2, 1, 2.5f32),
        ]);
        let mb = batch.as_mem_batch();
        let key0 = extract_group_key(&mb, 0, &schema, &[1]);
        let key1 = extract_group_key(&mb, 1, &schema, &[1]);
        assert_ne!(key0, key1, "different F32 values must produce different group keys");
    }
}
