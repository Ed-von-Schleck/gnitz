//! SQL type constants, schema descriptor types, and row-format utilities.
//!
//! These are shared across the storage, IPC, and query layers.

use rustc_hash::FxHashMap;

use crate::util::{read_u32_le, read_u64_le};

pub(crate) use gnitz_wire::type_code;
pub(crate) use gnitz_wire::TypeCode;
pub(crate) use gnitz_wire::SHORT_STRING_THRESHOLD;
pub use gnitz_wire::MAX_COLUMNS;

// ---------------------------------------------------------------------------
// Schema descriptor
// ---------------------------------------------------------------------------

#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct SchemaColumn {
    pub type_code: u8,
    size: u8,
    pub nullable: u8,
    _pad: u8,
}

impl SchemaColumn {
    pub const fn new(type_code: u8, nullable: u8) -> Self {
        SchemaColumn { type_code, size: type_size(type_code), nullable, _pad: 0 }
    }

    /// On-disk byte width of one cell of this column. Derived from `type_code`
    /// via `SchemaColumn::new` and never written independently.
    #[inline]
    pub const fn size(&self) -> u8 {
        self.size
    }
}

/// Sizing cap for the compound-PK column list carried in `SchemaDescriptor`.
/// Engine-internal: callers materialise `pk_indices` as a stack-resident
/// fixed array, but only the first `pk_count` entries are valid. Today
/// every schema has `pk_count == 1`; this constant exists so the
/// representation is ready for `pk_count > 1` without a re-layout.
pub const MAX_PK_COLUMNS: usize = 4;

/// Sentinel for any dense payload-index slot (e.g. `SortDesc::pi`,
/// `SchemaDescriptor::payload_mapping[ci]`) that needs to express
/// "this slot refers to a PK column, not a payload column". Using
/// `u8::MAX` (not 0) keeps the sentinel unambiguous against a real
/// payload index of 0.
pub const PAYLOAD_MAPPING_PK_SENTINEL: u8 = u8::MAX;

#[derive(Clone, Copy)]
#[repr(C)]
pub struct SchemaDescriptor {
    num_columns: u32,
    pk_count: u32,
    pk_indices: [u32; MAX_PK_COLUMNS],
    /// payload_mapping[ci] = dense payload index, or PAYLOAD_MAPPING_PK_SENTINEL.
    /// Same encoding as `SortDesc::pi` in `ops/reduce.rs`: PK columns hold the
    /// sentinel, payload columns hold their dense payload slot. Lets call
    /// sites that need this byte read it directly via
    /// `payload_mapping_byte(ci)` instead of re-deriving it from a branch on
    /// `is_pk_col` + `payload_idx`.
    payload_mapping: [u8; MAX_COLUMNS],
    /// payload_to_ci[pi] = logical column index for dense payload slot `pi`.
    /// Inverse of `payload_mapping` over the non-PK columns; the trailing
    /// `num_columns - pk_count`..MAX_COLUMNS slots hold the sentinel.
    /// Lets `payload_columns()` walk a contiguous `0..num_payload` range
    /// with one byte load per element, no per-row predicate.
    payload_to_ci: [u8; MAX_COLUMNS],
    pub columns: [SchemaColumn; MAX_COLUMNS],
}

const fn compute_mappings(
    num_columns: usize,
    pk_indices: &[u32],
) -> ([u8; MAX_COLUMNS], [u8; MAX_COLUMNS]) {
    let mut payload_mapping = [PAYLOAD_MAPPING_PK_SENTINEL; MAX_COLUMNS];
    let mut payload_to_ci = [PAYLOAD_MAPPING_PK_SENTINEL; MAX_COLUMNS];
    let mut pi: u8 = 0;
    let mut ci: usize = 0;
    while ci < num_columns {
        let mut is_pk = false;
        let mut k = 0;
        while k < pk_indices.len() {
            if pk_indices[k] as usize == ci {
                is_pk = true;
            }
            k += 1;
        }
        if !is_pk {
            payload_mapping[ci] = pi;
            payload_to_ci[pi as usize] = ci as u8;
            pi += 1;
        }
        ci += 1;
    }
    (payload_mapping, payload_to_ci)
}

impl SchemaDescriptor {
    /// Construct a SchemaDescriptor from a column list and PK index list.
    /// Today only single-PK schemas are supported in practice (`pk_indices.len() == 1`);
    /// the empty case `pk_indices = &[]` is reserved for the placeholder
    /// produced by `Default::default()` and is structurally invalid for real
    /// use. The representation itself accepts up to `MAX_PK_COLUMNS` entries
    /// so the field shape is ready for compound PKs without a re-layout.
    pub const fn new(cols: &[SchemaColumn], pk_indices: &[u32]) -> Self {
        debug_assert!(cols.len() <= MAX_COLUMNS, "new: too many columns");
        debug_assert!(
            pk_indices.len() <= MAX_PK_COLUMNS,
            "new: pk_indices.len() exceeds MAX_PK_COLUMNS",
        );
        let mut columns = [SchemaColumn::new(0, 0); MAX_COLUMNS];
        let mut i = 0;
        while i < cols.len() {
            columns[i] = cols[i];
            i += 1;
        }
        let mut pk_arr = [0u32; MAX_PK_COLUMNS];
        let mut k = 0;
        while k < pk_indices.len() {
            debug_assert!(
                (pk_indices[k] as usize) < cols.len(),
                "new: pk index out of range",
            );
            pk_arr[k] = pk_indices[k];
            k += 1;
        }
        let (payload_mapping, payload_to_ci) =
            compute_mappings(cols.len(), pk_indices);
        SchemaDescriptor {
            num_columns: cols.len() as u32,
            pk_count: pk_indices.len() as u32,
            pk_indices: pk_arr,
            payload_mapping,
            payload_to_ci,
            columns,
        }
    }

    pub const fn minimal_u64() -> Self {
        Self::new(&[SchemaColumn::new(type_code::U64, 0)], &[0])
    }

    /// Number of logical columns in this schema (PK + payload).
    #[inline]
    pub const fn num_columns(&self) -> usize {
        self.num_columns as usize
    }

    /// All PK column indices, in compound-key order. Today: always length 1.
    #[inline]
    pub fn pk_indices(&self) -> &[u32] {
        &self.pk_indices[..self.pk_count as usize]
    }

    /// The single PK column index. Use only at boundaries that have not yet
    /// been generalized (format encoders, catalog serialization, SQL parser
    /// path, wire/client BatchAppender). Asserts pk_count == 1.
    #[inline]
    pub const fn pk_index_single(&self) -> u32 {
        debug_assert!(self.pk_count == 1, "compound PK not yet supported here");
        self.pk_indices[0]
    }

    /// Number of non-PK ("payload") columns.
    #[inline]
    pub const fn num_payload_cols(&self) -> usize {
        self.num_columns as usize - self.pk_count as usize
    }

    /// Iterate over the non-PK ("payload") columns.
    ///
    /// Yields `(payload_idx, col_idx, &SchemaColumn)` where:
    /// - `payload_idx` is the dense 0-based index used for batch payload
    ///   regions and null-bitmap bits;
    /// - `col_idx` is the original logical column index in `self.columns`.
    ///
    /// Walks a contiguous `0..num_payload` range with one byte load per
    /// element via `payload_to_ci` — no per-row predicate.
    #[inline]
    pub fn payload_columns(&self) -> impl Iterator<Item = (usize, usize, &SchemaColumn)> {
        let num_payload = self.num_payload_cols();
        (0..num_payload).map(move |pi| {
            let ci = self.payload_to_ci[pi] as usize;
            (pi, ci, &self.columns[ci])
        })
    }

    /// Map a logical column index to its dense payload index (batch payload
    /// region + null-bitmap bit position). Caller must ensure `col_idx` is
    /// not a PK column.
    #[inline]
    pub fn payload_idx(&self, col_idx: usize) -> usize {
        debug_assert!(!self.is_pk_col(col_idx), "payload_idx: col_idx must not be a pk column");
        self.payload_mapping[col_idx] as usize
    }

    /// True iff column `ci` is a PK column.
    #[inline]
    pub fn is_pk_col(&self, ci: usize) -> bool {
        self.payload_mapping[ci] == PAYLOAD_MAPPING_PK_SENTINEL
    }

    /// Raw `payload_mapping[ci]` byte — equals `PAYLOAD_MAPPING_PK_SENTINEL`
    /// for PK columns, the dense payload index otherwise. Lets call sites
    /// that already encode "pk → sentinel else payload_idx as u8" in their
    /// own slot (e.g. `SortDesc::pi`) read the precomputed byte directly
    /// without a branch.
    #[inline]
    pub fn payload_mapping_byte(&self, ci: usize) -> u8 {
        self.payload_mapping[ci]
    }

    /// Inverse of `payload_idx`: dense payload slot → logical column index.
    /// Caller must ensure `pi < num_payload_cols()`.
    #[inline]
    pub fn payload_col_idx(&self, pi: usize) -> usize {
        debug_assert!(pi < self.num_payload_cols(), "payload_col_idx: pi out of range");
        self.payload_to_ci[pi] as usize
    }
}

impl PartialEq for SchemaDescriptor {
    fn eq(&self, other: &Self) -> bool {
        if self.num_columns() != other.num_columns() || self.pk_indices() != other.pk_indices() {
            return false;
        }
        // Compare only the active columns; _pad is always 0 (SchemaColumn::new enforces it).
        self.columns[..self.num_columns()] == other.columns[..other.num_columns()]
    }
}

impl Eq for SchemaDescriptor {}

impl Default for SchemaDescriptor {
    fn default() -> Self {
        Self::new(&[], &[])
    }
}

pub(crate) const fn type_size(tc: u8) -> u8 {
    gnitz_wire::wire_stride(tc) as u8
}

// ---------------------------------------------------------------------------
// Row-format utilities
// ---------------------------------------------------------------------------

#[inline(always)]
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

/// Promote raw column data at `offset` to a u128 key for index lookups.
#[inline]
pub(crate) fn promote_to_index_key(
    col_data: &[u8],
    offset: usize,
    col_size: usize,
    type_code_val: u8,
) -> u128 {
    debug_assert!(
        col_data.len() >= offset + col_size,
        "promote_to_index_key: buffer too short ({} < {})",
        col_data.len(), offset + col_size,
    );
    match TypeCode::from_validated_u8(type_code_val) {
        TypeCode::U128 | TypeCode::UUID => {
            u128::from_le_bytes(col_data[offset..offset + 16].try_into().unwrap())
        }
        TypeCode::U8 | TypeCode::I8 | TypeCode::U16 | TypeCode::I16 |
        TypeCode::U32 | TypeCode::I32 | TypeCode::F32 |
        TypeCode::U64 | TypeCode::I64 | TypeCode::F64 |
        TypeCode::String | TypeCode::Blob => {
            let mut bytes = [0u8; 8];
            let copy_len = col_size.min(8);
            bytes[..copy_len].copy_from_slice(&col_data[offset..offset + copy_len]);
            u64::from_le_bytes(bytes) as u128
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

/// Identity-keyed dedup cache for `relocate_german_string_vec`.
///
/// Key: `(src_blob.as_ptr() as usize, old_offset, length)`. The same source
/// span is copied at most once per merge — the cached value is the offset
/// inside the destination blob where the bytes were appended.
pub(crate) type BlobCache = FxHashMap<(usize, usize, usize), usize>;

/// Copy a 16-byte German string cell and (for long strings) migrate the
/// out-of-line payload from `src_blob` into `dst_blob`.
///
/// The returned 16-byte cell is ready to write into the output column buffer.
/// Short strings (≤ SHORT_STRING_THRESHOLD) are copied inline — `src_blob` is
/// unused for them.
///
/// When `cache` is `Some`, the appended blob data is deduplicated by
/// `(src_blob.as_ptr(), old_offset, length)` — i.e. the same source span is
/// only copied once per merge.
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
    cache: Option<&mut BlobCache>,
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
    let new_offset = dst_blob.len();
    let off = match cache {
        Some(cache) => {
            let key = (src_blob.as_ptr() as usize, old_offset, length);
            *cache.entry(key).or_insert_with(|| {
                dst_blob.extend_from_slice(&src_blob[old_offset..old_offset + length]);
                new_offset
            })
        }
        None => {
            dst_blob.extend_from_slice(&src_blob[old_offset..old_offset + length]);
            new_offset
        }
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
    fn test_relocate_german_string_vec_cache_hit_dedups() {
        // Long-string source: length=20, payload at offset 0 in src_blob.
        let payload: &[u8] = b"hello world test dat";
        assert_eq!(payload.len(), 20);
        let src_blob: Vec<u8> = payload.to_vec();

        let mut src_cell = [0u8; 16];
        src_cell[0..4].copy_from_slice(&20u32.to_le_bytes());
        src_cell[4..8].copy_from_slice(&payload[0..4]);
        src_cell[8..16].copy_from_slice(&0u64.to_le_bytes());

        let mut dst_blob: Vec<u8> = Vec::new();
        let mut cache: BlobCache = BlobCache::default();

        let r1 = relocate_german_string_vec(&src_cell, &src_blob, &mut dst_blob, Some(&mut cache));
        let after_first = dst_blob.len();
        assert_eq!(after_first, 20, "first call must append payload exactly once");

        let r2 = relocate_german_string_vec(&src_cell, &src_blob, &mut dst_blob, Some(&mut cache));
        assert_eq!(dst_blob.len(), 20, "cache hit must not append a second copy");
        assert_eq!(&r1[8..16], &r2[8..16], "both calls must return the same offset");
    }

    #[test]
    fn test_new_constructs_schema() {
        let cols = [
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 0),
            SchemaColumn::new(type_code::STRING, 1),
        ];
        let s = SchemaDescriptor::new(&cols, &[0]);
        assert_eq!(s.num_columns(), 3);
        assert_eq!(s.pk_index_single(), 0);
        assert_eq!(s.columns[0].type_code, type_code::U64);
        assert_eq!(s.columns[1].type_code, type_code::I64);
        assert_eq!(s.columns[2].type_code, type_code::STRING);

        // Trailing slot fill: SchemaColumn::new(0, 0) resolves via wire_stride(0)
        // → 8 (default arm). Locked in so future regressions in the trailing
        // representation are caught.
        assert_eq!(s.columns[3].type_code, 0);
        assert_eq!(s.columns[3].size(), 8);
        assert_eq!(s.columns[MAX_COLUMNS - 1].size(), 8);

        // payload_columns() walks non-PK indices in logical order.
        let payload: Vec<usize> = s.payload_columns().map(|(_, ci, _)| ci).collect();
        assert_eq!(payload, vec![1, 2]);
        assert_eq!(s.payload_idx(1), 0);
        assert_eq!(s.payload_idx(2), 1);

        // Non-zero pk_index round-trips.
        let s2 = SchemaDescriptor::new(&cols, &[2]);
        assert_eq!(s2.pk_index_single(), 2);

        // Empty placeholder (Default-style).
        let empty = SchemaDescriptor::new(&[], &[]);
        assert_eq!(empty.num_columns(), 0);
    }

    #[test]
    fn test_payload_idx_around_pk() {
        // pk_index = 1: col 0 maps to payload 0, col 2 maps to payload 1.
        let s = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::U64, 0),
            ],
            &[1],
        );
        assert_eq!(s.payload_idx(0), 0);
        assert_eq!(s.payload_idx(2), 1);
    }

    #[test]
    fn test_num_payload_cols() {
        // 2-column schema → 1 payload column.
        let s = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::U64, 0),
            ],
            &[0],
        );
        assert_eq!(s.num_payload_cols(), 1);

        // pk_index not at column 0 → same answer (num_columns - pk_indices().len()).
        let s = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::U64, 0),
            ],
            &[2],
        );
        assert_eq!(s.num_payload_cols(), 3);
    }

    #[test]
    fn test_num_columns() {
        let s = SchemaDescriptor::new(&[SchemaColumn::new(type_code::U64, 0)], &[0]);
        assert_eq!(s.num_columns(), 1);

        let s = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::U64, 0),
            ],
            &[0],
        );
        assert_eq!(s.num_columns(), 3);

        let s = SchemaDescriptor::new(
            &[SchemaColumn::new(type_code::U64, 0); 8],
            &[0],
        );
        assert_eq!(s.num_columns(), 8);
    }
}
