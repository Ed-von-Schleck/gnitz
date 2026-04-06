//! SQL type constants, schema descriptor types, and row-format utilities.
//!
//! These are shared across the storage, IPC, and query layers.

use std::collections::HashMap;

use crate::util::{read_u32_le, read_u64_le};
use crate::xxh;

// Type codes — all defined for completeness;
// the compare_rows dispatch only uses F32/F64/STRING/U128 explicitly.
#[allow(dead_code)]
pub(crate) mod type_code {
    pub const U8: u8 = 1;
    pub const I8: u8 = 2;
    pub const U16: u8 = 3;
    pub const I16: u8 = 4;
    pub const U32: u8 = 5;
    pub const I32: u8 = 6;
    pub const F32: u8 = 7;
    pub const U64: u8 = 8;
    pub const I64: u8 = 9;
    pub const F64: u8 = 10;
    pub const STRING: u8 = 11;
    pub const U128: u8 = 12;
}

pub(crate) const SHORT_STRING_THRESHOLD: usize = 12;

// ---------------------------------------------------------------------------
// Schema descriptor
// ---------------------------------------------------------------------------

#[repr(C)]
#[derive(Clone, Copy)]
pub struct SchemaColumn {
    pub type_code: u8,
    pub size: u8,
    pub nullable: u8,
    pub _pad: u8,
}

impl SchemaColumn {
    pub const fn new(type_code: u8, nullable: u8) -> Self {
        SchemaColumn { type_code, size: type_size(type_code), nullable, _pad: 0 }
    }
}

#[derive(Clone, Copy)]
#[repr(C)]
pub struct SchemaDescriptor {
    pub num_columns: u32,
    pub pk_index: u32,
    pub columns: [SchemaColumn; 64],
}

impl SchemaDescriptor {
    pub fn minimal_u64() -> Self {
        let mut columns = [SchemaColumn::new(0, 0); 64];
        columns[0] = SchemaColumn::new(type_code::U64, 0);
        SchemaDescriptor { num_columns: 1, pk_index: 0, columns }
    }
}

pub(crate) const fn type_size(tc: u8) -> u8 {
    match tc {
        1 | 2 => 1,       // U8, I8
        3 | 4 => 2,       // U16, I16
        5 | 6 | 7 => 4,   // U32, I32, F32
        8 | 9 | 10 => 8,  // U64, I64, F64
        11 | 12 => 16,    // STRING, U128
        _ => 8,
    }
}

// ---------------------------------------------------------------------------
// Row-format utilities
// ---------------------------------------------------------------------------

#[inline]
pub(crate) fn read_signed(bytes: &[u8], size: usize) -> i64 {
    match size {
        1 => bytes[0] as i8 as i64,
        2 => i16::from_le_bytes(bytes[..2].try_into().unwrap()) as i64,
        4 => i32::from_le_bytes(bytes[..4].try_into().unwrap()) as i64,
        8 => i64::from_le_bytes(bytes[..8].try_into().unwrap()),
        _ => 0,
    }
}

/// Promote raw column data at `offset` to (key_lo, key_hi) for index lookups.
#[inline]
pub(crate) fn promote_to_index_key(
    col_data: &[u8],
    offset: usize,
    col_size: usize,
    type_code_val: u8,
) -> (u64, u64) {
    match type_code_val {
        type_code::U128 => {
            let lo = u64::from_le_bytes(col_data[offset..offset + 8].try_into().unwrap());
            let hi = u64::from_le_bytes(col_data[offset + 8..offset + 16].try_into().unwrap());
            (lo, hi)
        }
        type_code::I8 | type_code::I16 | type_code::I32 => {
            (read_signed(&col_data[offset..], col_size) as u64, 0)
        }
        _ => {
            let mut bytes = [0u8; 8];
            let copy_len = col_size.min(8);
            bytes[..copy_len].copy_from_slice(&col_data[offset..offset + copy_len]);
            (u64::from_le_bytes(bytes), 0)
        }
    }
}

/// Prepare the 16-byte German string output struct for a copy operation.
/// Fills length, prefix, and (for short strings) inline suffix.
/// For long strings dest[8..16] is left as zero — the caller must resolve
/// the blob data and write the new offset in.
/// Returns (dest_struct, is_long_string).
#[inline]
pub(crate) fn prep_german_string_copy(src: &[u8]) -> ([u8; 16], bool) {
    let length = u32::from_le_bytes(src[0..4].try_into().unwrap()) as usize;
    let mut dest = [0u8; 16];
    dest[0..4].copy_from_slice(&src[0..4]);
    dest[4..8].copy_from_slice(&src[4..8]);
    if length <= SHORT_STRING_THRESHOLD {
        let suffix_len = if length > 4 { length - 4 } else { 0 };
        if suffix_len > 0 {
            dest[8..8 + suffix_len].copy_from_slice(&src[8..8 + suffix_len]);
        }
        (dest, false)
    } else {
        (dest, true)
    }
}

/// Check the blob dedup cache. Returns `Some(offset)` if `data` is already
/// present in `existing_blob`. Otherwise inserts `new_offset` into the cache
/// and returns `None` — the caller must actually append `data` at `new_offset`.
pub(crate) fn blob_cache_lookup(
    data: &[u8],
    cache: &mut HashMap<(u64, usize), usize>,
    existing_blob: &[u8],
    new_offset: usize,
) -> Option<usize> {
    if data.is_empty() { return Some(0); }
    let h = xxh::checksum(data);
    let key = (h, data.len());
    if let Some(&off) = cache.get(&key) {
        if &existing_blob[off..off + data.len()] == data {
            return Some(off);
        }
    }
    cache.insert(key, new_offset);
    None
}

/// Encode a German string into a 16-byte struct + optional blob append.
pub(crate) fn encode_german_string(s: &[u8], blob: &mut Vec<u8>) -> [u8; 16] {
    let len = s.len();
    let mut st = [0u8; 16];
    st[0..4].copy_from_slice(&(len as u32).to_le_bytes());
    let pfx = len.min(4);
    st[4..4 + pfx].copy_from_slice(&s[..pfx]);
    if len <= SHORT_STRING_THRESHOLD {
        if len > 4 {
            st[8..8 + (len - 4)].copy_from_slice(&s[4..len]);
        }
    } else {
        let off = blob.len();
        blob.extend_from_slice(s);
        st[8..16].copy_from_slice(&(off as u64).to_le_bytes());
    }
    st
}

/// Decode a German string from a 16-byte struct + blob.
pub(crate) fn decode_german_string(st: &[u8; 16], blob: &[u8]) -> Vec<u8> {
    let len = u32::from_le_bytes(st[0..4].try_into().unwrap()) as usize;
    if len == 0 {
        return Vec::new();
    }
    if len <= SHORT_STRING_THRESHOLD {
        let mut out = Vec::with_capacity(len);
        let pfx = len.min(4);
        out.extend_from_slice(&st[4..4 + pfx]);
        if len > 4 {
            out.extend_from_slice(&st[8..8 + (len - 4)]);
        }
        out
    } else {
        let off = u64::from_le_bytes(st[8..16].try_into().unwrap()) as usize;
        blob[off..off + len].to_vec()
    }
}

/// Returns bytes [4..end] of a German string as a contiguous slice.
/// Short strings (len ≤ SHORT_STRING_THRESHOLD): inline at struct[8..4+end].
/// Long strings: full string is in blob at heap_offset; skip first 4 bytes
/// (they duplicate the prefix, already compared by the caller).
#[inline]
pub(crate) fn german_string_tail<'a>(
    s: &'a [u8], blob: &'a [u8], length: usize, end: usize,
) -> &'a [u8] {
    if length <= SHORT_STRING_THRESHOLD {
        &s[8..4 + end]
    } else {
        let heap_offset = read_u64_le(s, 8) as usize;
        &blob[heap_offset + 4..heap_offset + end]
    }
}

#[inline]
pub(crate) fn compare_german_strings(
    a: &[u8], blob_a: &[u8],
    b: &[u8], blob_b: &[u8],
) -> std::cmp::Ordering {
    let len_a = read_u32_le(a, 0) as usize;
    let len_b = read_u32_le(b, 0) as usize;
    let min_len = len_a.min(len_b);
    let prefix_cmp = min_len.min(4);

    // Bulk prefix comparison — single 32-bit compare for the common case.
    let ord = a[4..4 + prefix_cmp].cmp(&b[4..4 + prefix_cmp]);
    if ord != std::cmp::Ordering::Equal {
        return ord;
    }
    if min_len <= 4 {
        return len_a.cmp(&len_b);
    }

    // Bulk suffix comparison — vectorised memcmp via [u8]::cmp.
    let tail_a = german_string_tail(a, blob_a, len_a, min_len);
    let tail_b = german_string_tail(b, blob_b, len_b, min_len);
    match tail_a.cmp(tail_b) {
        std::cmp::Ordering::Equal => len_a.cmp(&len_b),
        ord => ord,
    }
}

/// Copy a German String from raw 16-byte struct + blob base ptr into the output.
pub fn write_string_from_raw(
    col_buf: &mut Vec<u8>,
    blob: &mut Vec<u8>,
    src: &[u8],
    src_blob_ptr: *const u8,
) {
    let (mut dest, is_long) = prep_german_string_copy(src);
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
