//! Shared helpers used by ≥2 sub-modules.

use crate::schema::{SchemaDescriptor, SHORT_STRING_THRESHOLD, type_code};
use crate::schema::type_code::STRING as TYPE_STRING;
use crate::storage::{Batch, MemBatch, ReadCursor};
use crate::xxh;

// ---------------------------------------------------------------------------
// String relocation helpers
// ---------------------------------------------------------------------------

/// Copy a German String from a MemBatch into the output Batch at
/// payload column `out_pi` (current row position).
pub(super) fn write_string_from_batch(
    output: &mut Batch,
    out_pi: usize,
    batch: &MemBatch,
    row: usize,
    payload_col: usize,
) {
    let src = batch.get_col_ptr(row, payload_col, 16);
    let cell = crate::schema::relocate_german_string_vec(src, &batch.blob, &mut output.blob, None);
    output.extend_col(out_pi, &cell);
}

/// Copy a German String from raw 16-byte struct + blob base ptr into the output.
/// Returns the relocated 16-byte German string struct.
///
/// # Safety
/// See [`crate::schema::write_string_from_raw`].
pub unsafe fn write_string_from_raw(
    blob: &mut Vec<u8>,
    src: &[u8],
    src_blob_ptr: *const u8,
) -> [u8; 16] {
    // SAFETY: caller upholds the same pointer validity contract.
    unsafe { crate::schema::write_string_from_raw(blob, src, src_blob_ptr) }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

#[inline]
pub(super) fn signum(x: i64) -> i64 {
    if x > 0 { 1 } else if x < 0 { -1 } else { 0 }
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

    let cursor_blob = cursor.blob_ptr();
    let cursor_blob_slice: &[u8] = if cursor_blob.is_null() {
        &[]
    } else {
        unsafe { std::slice::from_raw_parts(cursor_blob, cursor.blob_len()) }
    };

    for (pi, ci, col) in schema.payload_columns() {
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
                } else if col.type_code == type_code::U128 || col.type_code == type_code::UUID {
                    let c_lo = u64::from_le_bytes(c_bytes[0..8].try_into().unwrap());
                    let c_hi = u64::from_le_bytes(c_bytes[8..16].try_into().unwrap());
                    let b_lo = u64::from_le_bytes(b_bytes[0..8].try_into().unwrap());
                    let b_hi = u64::from_le_bytes(b_bytes[8..16].try_into().unwrap());
                    (c_hi, c_lo).cmp(&(b_hi, b_lo))
                } else if col.type_code == type_code::F64 {
                    let c_f = f64::from_bits(u64::from_le_bytes(c_bytes[0..8].try_into().unwrap()));
                    let b_f = f64::from_bits(u64::from_le_bytes(b_bytes[0..8].try_into().unwrap()));
                    c_f.total_cmp(&b_f)
                } else if col.type_code == type_code::F32 {
                    let c_f = f32::from_bits(u32::from_le_bytes(c_bytes[0..4].try_into().unwrap()));
                    let b_f = f32::from_bits(u32::from_le_bytes(b_bytes[0..4].try_into().unwrap()));
                    c_f.total_cmp(&b_f)
                } else if col.type_code == type_code::I64
                    || col.type_code == type_code::I32
                    || col.type_code == type_code::I16
                    || col.type_code == type_code::I8
                {
                    let c_v = crate::schema::read_signed(c_bytes, cs);
                    let b_v = crate::schema::read_signed(b_bytes, cs);
                    c_v.cmp(&b_v)
                } else {
                    // Unsigned: zero-extend to u64 for any column width (1/2/4/8 bytes).
                    let mut c_buf = [0u8; 8];
                    let mut b_buf = [0u8; 8];
                    c_buf[..cs].copy_from_slice(c_bytes);
                    b_buf[..cs].copy_from_slice(b_bytes);
                    u64::from_le_bytes(c_buf).cmp(&u64::from_le_bytes(b_buf))
                }
            }
        };

        if ord != Ordering::Equal {
            return ord;
        }
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
) -> u128 {
    let pki = schema.pk_index as usize;

    if group_by_cols.len() == 1 {
        let c_idx = group_by_cols[0] as usize;
        let tc = schema.columns[c_idx].type_code;
        if tc == type_code::U64 || tc == type_code::I64 {
            let pi = payload_idx(c_idx, pki);
            let ptr = mb.get_col_ptr(row, pi, 8);
            let v = u64::from_le_bytes(ptr.try_into().unwrap());
            return v as u128;
        }
        if tc == type_code::U128 || tc == type_code::UUID {
            let pi = payload_idx(c_idx, pki);
            let ptr = mb.get_col_ptr(row, pi, 16);
            return u128::from_le_bytes(ptr[0..16].try_into().unwrap());
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
    ((h_hi as u128) << 64) | (h as u128)
}
