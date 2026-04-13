//! SQL type constants, schema descriptor types, and row-format utilities.
//!
//! These are shared across the storage, IPC, and query layers.

use std::collections::HashMap;

use crate::util::{read_u32_le, read_u64_le};
use crate::xxh;

pub(crate) use gnitz_wire::type_code;
pub(crate) use gnitz_wire::SHORT_STRING_THRESHOLD;

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
    pub const fn minimal_u64() -> Self {
        let mut columns = [SchemaColumn::new(0, 0); 64];
        columns[0] = SchemaColumn::new(type_code::U64, 0);
        SchemaDescriptor { num_columns: 1, pk_index: 0, columns }
    }
}

pub(crate) const fn type_size(tc: u8) -> u8 {
    gnitz_wire::wire_stride(tc) as u8
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

/// Copy a 16-byte German string cell and (for long strings) migrate the
/// out-of-line payload from `src_blob` into `dst_blob`.
///
/// The returned 16-byte cell is ready to write into the output column buffer.
/// Short strings (≤ SHORT_STRING_THRESHOLD) are copied inline — `src_blob` is
/// unused for them.
///
/// When `cache` is `Some`, the appended blob data is deduplicated via
/// `blob_cache_lookup`; the returned offset may point to an existing copy.
///
/// **Malformed-input fallback:** if the long-string header declares a region
/// `[old_offset, old_offset + length)` that overruns `src_blob.len()`, the
/// cell is rewritten to an empty string (length = 0) rather than triggering
/// an out-of-bounds read. This keeps trusted in-memory callers panic-free
/// even when fed corrupted data that slipped past validation.
pub(crate) fn relocate_german_string_vec(
    src_cell: &[u8],
    src_blob: &[u8],
    dst_blob: &mut Vec<u8>,
    cache: Option<&mut HashMap<(u64, usize), usize>>,
) -> [u8; 16] {
    let (mut dest, is_long) = prep_german_string_copy(src_cell);
    if !is_long {
        return dest;
    }
    let length = u32::from_le_bytes(src_cell[0..4].try_into().unwrap()) as usize;
    let old_offset = u64::from_le_bytes(src_cell[8..16].try_into().unwrap()) as usize;
    if old_offset.saturating_add(length) > src_blob.len() {
        // Malformed: emit an empty string. prep_german_string_copy already
        // zero-initialized dest and left dest[8..16] == 0 for long strings;
        // overwriting the length to 0 makes this a valid short empty string.
        dest[0..4].copy_from_slice(&0u32.to_le_bytes());
        return dest;
    }
    let src_data = &src_blob[old_offset..old_offset + length];
    let new_offset = dst_blob.len();
    let off = match cache {
        Some(cache) => blob_cache_lookup(src_data, cache, dst_blob, new_offset)
            .unwrap_or_else(|| { dst_blob.extend_from_slice(src_data); new_offset }),
        None => { dst_blob.extend_from_slice(src_data); new_offset },
    };
    dest[8..16].copy_from_slice(&(off as u64).to_le_bytes());
    dest
}

pub(crate) use gnitz_wire::encode_german_string;
pub(crate) use gnitz_wire::decode_german_string;

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
