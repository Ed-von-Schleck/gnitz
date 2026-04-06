//! Shared helpers used by ≥2 sub-modules.

use crate::schema::{SchemaDescriptor, SHORT_STRING_THRESHOLD, type_code};
use crate::schema::type_code::STRING as TYPE_STRING;
use crate::storage::{write_to_owned_batch, OwnedBatch, MemBatch, ReadCursor, sort_and_consolidate};
use crate::xxh;

// ---------------------------------------------------------------------------
// String relocation helpers
// ---------------------------------------------------------------------------

/// Copy a German String from a MemBatch into the output OwnedBatch.
pub(super) fn write_string_from_batch(
    col_buf: &mut Vec<u8>,
    blob: &mut Vec<u8>,
    batch: &MemBatch,
    row: usize,
    payload_col: usize,
) {
    let src = batch.get_col_ptr(row, payload_col, 16);
    let blob_ptr = if batch.blob.is_empty() { std::ptr::null() } else { batch.blob.as_ptr() };
    write_string_from_raw(col_buf, blob, src, blob_ptr);
}

/// Copy a German String from raw 16-byte struct + blob base ptr into the output.
pub fn write_string_from_raw(
    col_buf: &mut Vec<u8>,
    blob: &mut Vec<u8>,
    src: &[u8],
    src_blob_ptr: *const u8,
) {
    let (mut dest, is_long) = crate::schema::prep_german_string_copy(src);
    if is_long {
        let length = u32::from_le_bytes(src[0..4].try_into().unwrap()) as usize;
        assert!(!src_blob_ptr.is_null(), "write_string_from_raw: long string but src_blob_ptr is null");
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

pub(super) fn consolidate_owned(batch: &OwnedBatch, schema: &SchemaDescriptor) -> OwnedBatch {
    if batch.consolidated || batch.count == 0 {
        return batch.clone_batch();
    }
    let mb = batch.as_mem_batch();
    let blob_cap = mb.blob.len().max(1);
    let mut consolidated =
        write_to_owned_batch(schema, batch.count, blob_cap, |writer| {
            sort_and_consolidate(&mb, schema, writer);
        });
    consolidated.sorted = true;
    consolidated.consolidated = true;
    consolidated.schema = Some(*schema);
    consolidated
}

#[inline]
pub(super) fn signum(x: i64) -> i64 {
    if x > 0 { 1 } else if x < 0 { -1 } else { 0 }
}

#[inline]
pub(super) fn pk_lt(a_lo: u64, a_hi: u64, b_lo: u64, b_hi: u64) -> bool {
    (a_hi, a_lo) < (b_hi, b_lo)
}

/// Append a row from a ReadCursor to an OwnedBatch.
pub(super) fn append_cursor_row_to_batch(
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

/// Compare a cursor's current payload to a batch row's payload, returning their ordering.
///
/// Iterates payload columns in schema order (skipping pk_index). Null ordering:
/// null < non-null, null == null. Type dispatch follows `compare_by_group_cols`.
pub(super) fn compare_cursor_payload_to_batch_row(
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
                    crate::schema::compare_german_strings(
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
// Payload index and group key helpers (shared by reduce, exchange)
// ---------------------------------------------------------------------------

/// Map logical column index to payload column index (skipping the PK column).
#[inline]
pub(super) fn payload_idx(col_idx: usize, pk_index: usize) -> usize {
    if col_idx < pk_index { col_idx } else { col_idx - 1 }
}

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

/// Extract 128-bit group key from a batch row.
pub(super) fn extract_group_key(
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
                let heap_offset =
                    u64::from_le_bytes(struct_bytes[8..16].try_into().unwrap()) as usize;
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
