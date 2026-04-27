//! SQL type constants, schema descriptor types, and row-format utilities.
//!
//! These are shared across the storage, IPC, and query layers.

use std::collections::HashMap;

use crate::util::{read_u32_le, read_u64_le};
use crate::xxh;

pub(crate) use gnitz_wire::type_code;
pub(crate) use gnitz_wire::SHORT_STRING_THRESHOLD;
pub use gnitz_wire::MAX_COLUMNS;

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
    pub columns: [SchemaColumn; MAX_COLUMNS],
}

impl SchemaDescriptor {
    pub const fn minimal_u64() -> Self {
        let mut columns = [SchemaColumn::new(0, 0); MAX_COLUMNS];
        columns[0] = SchemaColumn::new(type_code::U64, 0);
        SchemaDescriptor { num_columns: 1, pk_index: 0, columns }
    }

    /// Iterate over the non-PK ("payload") columns.
    ///
    /// Yields `(payload_idx, col_idx, &SchemaColumn)` where:
    /// - `payload_idx` is the dense 0-based index used for batch payload
    ///   regions and null-bitmap bits;
    /// - `col_idx` is the original logical column index in `self.columns`.
    ///
    /// Replaces the manual `for ci in 0..n { if ci == pk { continue; } ... pi += 1 }`
    /// pattern; keeps `pi` and `ci` in lockstep so they cannot desync.
    #[inline]
    pub fn payload_columns(&self) -> impl Iterator<Item = (usize, usize, &SchemaColumn)> {
        let pk = self.pk_index as usize;
        let n = self.num_columns as usize;
        (0..n)
            .filter(move |ci| *ci != pk)
            .enumerate()
            .map(move |(pi, ci)| (pi, ci, &self.columns[ci]))
    }

    /// Map a logical column index to its dense payload index (batch payload
    /// region + null-bitmap bit position). Caller must ensure `col_idx != pk_index`.
    #[inline]
    pub fn payload_idx(&self, col_idx: usize) -> usize {
        debug_assert!(col_idx != self.pk_index as usize, "payload_idx: col_idx must not be the pk_index");
        let pk = self.pk_index as usize;
        if col_idx < pk { col_idx } else { col_idx - 1 }
    }
}

impl Default for SchemaDescriptor {
    fn default() -> Self {
        SchemaDescriptor {
            num_columns: 0,
            pk_index: 0,
            columns: [SchemaColumn::new(0, 0); MAX_COLUMNS],
        }
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
    debug_assert!(bytes.len() >= size, "read_signed: buffer too short ({} < {})", bytes.len(), size);
    match size {
        1 => bytes[0] as i8 as i64,
        2 => i16::from_le_bytes(bytes[..2].try_into().unwrap()) as i64,
        4 => i32::from_le_bytes(bytes[..4].try_into().unwrap()) as i64,
        8 => i64::from_le_bytes(bytes[..8].try_into().unwrap()),
        _ => unreachable!("read_signed: unexpected size {size}"),
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
    debug_assert!(
        col_data.len() >= offset + col_size,
        "promote_to_index_key: buffer too short ({} < {})",
        col_data.len(), offset + col_size,
    );
    match type_code_val {
        type_code::U128 | type_code::UUID => {
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
    debug_assert!(src.len() >= 16, "prep_german_string_copy: src must be a 16-byte German string struct");
    let length = u32::from_le_bytes(src[0..4].try_into().unwrap()) as usize;
    let mut dest = [0u8; 16];
    dest[0..4].copy_from_slice(&src[0..4]);
    dest[4..8].copy_from_slice(&src[4..8]);
    if length <= SHORT_STRING_THRESHOLD {
        let suffix_len = length.saturating_sub(4);
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
        if existing_blob.get(off..off + data.len()) == Some(data) {
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
        // Malformed: emit an empty string. Zero the full inline header so no
        // stale prefix bytes remain. dest[8..16] is already 0 (prep_german_string_copy
        // zero-initialized the struct and left the long-string offset as 0).
        dest[0..8].copy_from_slice(&0u64.to_le_bytes());
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
        debug_assert!(s.len() >= 4 + end, "german_string_tail: short string struct too small");
        &s[8..4 + end]
    } else {
        let heap_offset = read_u64_le(s, 8) as usize;
        debug_assert!(
            heap_offset + end <= blob.len(),
            "german_string_tail: long string [{}..{}) overruns blob (len={})",
            heap_offset + 4, heap_offset + end, blob.len(),
        );
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
/// Returns the relocated 16-byte German string struct; the caller must append it
/// to its column buffer.
///
/// # Safety
/// `src_blob_ptr`, when non-null, must point to valid memory for at least
/// `old_offset + length` bytes, where `length` and `old_offset` are read from
/// the first 16 bytes of `src`.
pub unsafe fn write_string_from_raw(
    blob: &mut Vec<u8>,
    src: &[u8],
    src_blob_ptr: *const u8,
) -> [u8; 16] {
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
    dest
}

#[cfg(test)]
mod tests {
    use super::*;

    // Claim 6: the malformed-long-string fallback must zero both the length
    // field (dest[0..4]) AND the prefix field (dest[4..8]).  Before the fix,
    // dest[4..8] was left containing the garbage bytes copied from the corrupt
    // source cell.
    #[test]
    fn test_malformed_long_string_fallback_clean_header() {
        let mut src_cell = [0u8; 16];
        // length = 20  (> SHORT_STRING_THRESHOLD = 12)
        src_cell[0..4].copy_from_slice(&20u32.to_le_bytes());
        // prefix = 0xDEADBEEF  (stale garbage that must be zeroed on fallback)
        src_cell[4..8].copy_from_slice(&0xDEAD_BEEF_u32.to_le_bytes());
        // heap_offset = 999  (well beyond src_blob)
        src_cell[8..16].copy_from_slice(&999u64.to_le_bytes());

        let src_blob = vec![0u8; 4]; // far too small
        let mut dst_blob: Vec<u8> = Vec::new();

        let result = relocate_german_string_vec(&src_cell, &src_blob, &mut dst_blob, None);

        assert_eq!(
            u32::from_le_bytes(result[0..4].try_into().unwrap()), 0,
            "fallback must emit length=0",
        );
        assert_eq!(&result[4..8], &[0u8; 4], "fallback must zero the prefix field");
        assert_eq!(&result[8..16], &[0u8; 8], "fallback must leave blob offset zero");
        assert!(dst_blob.is_empty(), "fallback must not extend dst_blob");
    }

    #[test]
    fn test_blob_cache_lookup_hit_and_miss() {
        let data = b"hello world test data";
        let mut cache: HashMap<(u64, usize), usize> = HashMap::new();
        // First lookup on empty blob: miss, populates cache at offset 0.
        assert_eq!(blob_cache_lookup(data, &mut cache, &[], 0), None);
        // Second lookup with the data in the blob: hit.
        let blob: Vec<u8> = data.to_vec();
        assert_eq!(blob_cache_lookup(data, &mut cache, &blob, 99), Some(0));
    }

    #[test]
    fn test_blob_cache_lookup_empty_always_zero() {
        let mut cache: HashMap<(u64, usize), usize> = HashMap::new();
        assert_eq!(blob_cache_lookup(b"", &mut cache, &[], 42), Some(0));
        assert!(cache.is_empty(), "empty string must not pollute the cache");
    }

    #[test]
    fn test_payload_idx_around_pk() {
        // pk_index = 1: col 0 maps to payload 0, col 2 maps to payload 1.
        let mut s = SchemaDescriptor::minimal_u64();
        s.num_columns = 3;
        s.pk_index = 1;
        assert_eq!(s.payload_idx(0), 0);
        assert_eq!(s.payload_idx(2), 1);
    }
}
