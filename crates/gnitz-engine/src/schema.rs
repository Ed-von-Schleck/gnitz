//! SQL type constants, schema descriptor types, and row-format utilities.
//!
//! These are shared across the storage, IPC, and query layers.

use rustc_hash::FxHashMap;

use crate::util::{read_u32_le, read_u64_le};

pub(crate) use gnitz_wire::type_code;
pub(crate) use gnitz_wire::TypeCode;
pub(crate) use gnitz_wire::SHORT_STRING_THRESHOLD;
pub use gnitz_wire::MAX_COLUMNS;
pub use gnitz_wire::{MAX_PK_BYTES, MAX_PK_COLUMNS};

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

/// Widest PK region that still fits in a packed `u128` word. At or below
/// this width the order-agnostic byte paths keep the PK in a `u128`
/// (`get_pk`/`partition_for_key`); above it the region is "wide" and must
/// be handled via the raw-bytes accessors and `compare_pk_bytes`.
pub const NARROW_PK_MAX_BYTES: usize = 16;

/// Reassemble a wide PK's full byte image from its wire split: the low
/// `NARROW_PK_MAX_BYTES` carried as a `u128` plus the `extra` tail (PK bytes
/// `16..stride`). This is the inverse of `PkTuple::split_wire` and the single
/// home for the seek-path join contract shared by the master partition router
/// and the worker SEEK handler. The client transmits seek keys as native LE
/// column bytes (the seek frame bypasses `append_pk_region`, so unlike the DML
/// path the bytes are not yet OPK), so this reassembles the native image and
/// then OPK-encodes it — the returned `..stride` is the OPK key that
/// `seek_bytes`/`partition_for_pk_bytes` compare against OPK storage. Errors if
/// `extra` is shorter than the wide suffix `stride - 16`. Callers must only
/// invoke this for wide PKs (`pk_is_wide()`, `stride > 16`).
pub fn assemble_wide_pk(
    schema: &SchemaDescriptor,
    low: u128,
    extra: &[u8],
    stride: usize,
) -> Result<[u8; MAX_PK_BYTES], String> {
    debug_assert!(stride > NARROW_PK_MAX_BYTES, "assemble_wide_pk: narrow stride {stride}");
    let needed = stride - NARROW_PK_MAX_BYTES;
    if extra.len() < needed {
        return Err(format!(
            "wide PK stride {stride} requires {needed} extra bytes, got {}",
            extra.len()
        ));
    }
    let mut le = [0u8; MAX_PK_BYTES];
    le[..NARROW_PK_MAX_BYTES].copy_from_slice(&low.to_le_bytes());
    le[NARROW_PK_MAX_BYTES..stride].copy_from_slice(&extra[..needed]);
    let mut opk = [0u8; MAX_PK_BYTES];
    crate::storage::encode_order_preserving_pk(schema, &le[..stride], &mut opk[..stride]);
    Ok(opk)
}

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
    /// Total bytes per row of the PK region — sum of
    /// `columns[pk_indices[k]].size()` for k in 0..pk_count. Precomputed
    /// once in `new()` so per-row hot loops never re-run the sum. `u8`
    /// matches the existing `storage::batch::pk_stride()` helper and the
    /// cached `pk_stride: u8` fields on `MappedShard`/`DirectWriter`/
    /// `MemBatch`; today's worst case is 5 × 16 = 80, well under 255.
    pk_stride: u8,
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
    /// The empty case `pk_indices = &[]` is reserved for the placeholder
    /// produced by `Default::default()` and is structurally invalid for real
    /// use. Accepts up to `MAX_PK_COLUMNS` entries.
    #[track_caller]
    pub const fn new(cols: &[SchemaColumn], pk_indices: &[u32]) -> Self {
        assert!(cols.len() <= MAX_COLUMNS, "new: too many columns");
        assert!(
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
        let mut stride_acc: u16 = 0;
        let mut k = 0;
        while k < pk_indices.len() {
            assert!(
                (pk_indices[k] as usize) < cols.len(),
                "new: pk index out of range",
            );
            // Duplicate-PK guard: a duplicate would desynchronise
            // num_payload_cols (counts the dup twice), pk_columns
            // (yields duplicates), and pk_stride (double-counts).
            // O(pk_count²) over pk_count ≤ MAX_PK_COLUMNS — negligible.
            let mut j = 0;
            while j < k {
                assert!(
                    pk_indices[j] != pk_indices[k],
                    "new: duplicate PK column index",
                );
                j += 1;
            }
            // STRING and BLOB store a German-string struct whose heap_offset
            // points into the batch blob. The PK region is bulk-copied without
            // blob relocation, so these types would produce dangling pointers.
            assert!(
                cols[pk_indices[k] as usize].type_code != type_code::STRING
                    && cols[pk_indices[k] as usize].type_code != type_code::BLOB,
                "new: STRING and BLOB columns cannot be PK columns \
                 (PK region is bulk-copied without blob relocation)",
            );
            // `compare_pk_bytes` reads PK bytes with no null-bit handling; a
            // nullable PK would silently corrupt the merge comparison.
            assert!(
                cols[pk_indices[k] as usize].nullable == 0,
                "new: PK columns must be non-nullable",
            );
            // `compare_pk_bytes` and the order-preserving sort-key encoder have
            // no float arm (IEEE-754 breaks the byte-equal key contract), so
            // both panic on a float PK. Float PKs are already rejected at the
            // catalog DDL and wire layers; close the invariant here too so the
            // descriptor constructor is the single authority on PK eligibility.
            assert!(
                cols[pk_indices[k] as usize].type_code != type_code::F32
                    && cols[pk_indices[k] as usize].type_code != type_code::F64,
                "new: F32 and F64 columns cannot be PK columns \
                 (compare_pk_bytes has no float ordering)",
            );
            pk_arr[k] = pk_indices[k];
            stride_acc += cols[pk_indices[k] as usize].size() as u16;
            k += 1;
        }
        assert!(
            stride_acc <= u8::MAX as u16,
            "new: pk_stride exceeds u8 width",
        );
        // `assemble_wide_pk` and other wide-path routines allocate
        // `[0u8; MAX_PK_BYTES]` and index up to `stride`; a stride in
        // (MAX_PK_BYTES, 255] would construct here but panic at runtime.
        assert!(
            stride_acc as usize <= MAX_PK_BYTES,
            "new: pk_stride exceeds MAX_PK_BYTES",
        );
        let pk_stride = stride_acc as u8;
        let (payload_mapping, payload_to_ci) =
            compute_mappings(cols.len(), pk_indices);
        SchemaDescriptor {
            num_columns: cols.len() as u32,
            pk_count: pk_indices.len() as u32,
            pk_indices: pk_arr,
            pk_stride,
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

    /// Iterate over PK columns in pk-list order. Mirror of
    /// `payload_columns()`. Yields `(pk_ord, col_idx, &SchemaColumn)`.
    /// No non-test caller today; introduced preemptively so future
    /// compound-PK migration sites can drop in `for (ord, ci, col) in
    /// schema.pk_columns()` without redefining the iterator shape.
    #[inline]
    pub fn pk_columns(&self) -> impl Iterator<Item = (usize, usize, &SchemaColumn)> {
        self.pk_indices().iter().copied().enumerate().map(move |(ord, ci)| {
            (ord, ci as usize, &self.columns[ci as usize])
        })
    }

    /// Total bytes per row of the PK region. Precomputed in `new()`;
    /// O(1) field load. Returns `u8` to mirror `SchemaColumn::size()`
    /// and the storage-layer `pk_stride` caches; callers that need
    /// `usize` for buffer arithmetic cast at the use site.
    #[inline]
    pub const fn pk_stride(&self) -> u8 {
        self.pk_stride
    }

    /// True iff the PK region is too wide to pack into a `u128` word
    /// (`pk_stride > NARROW_PK_MAX_BYTES`). Wide PKs cannot use the
    /// `get_pk`/`partition_for_key` fast paths and must go through the
    /// raw-bytes accessors and `compare_pk_bytes`.
    #[inline]
    pub const fn pk_is_wide(&self) -> bool {
        self.pk_stride as usize > NARROW_PK_MAX_BYTES
    }

    /// True iff the PK is a single signed column (I8/I16/I32/I64). Its OPK
    /// encoding flips the sign bit of the leading byte, so the `extend_pk` /
    /// `set_pk_at` u128 fast paths — which write right-aligned big-endian bytes
    /// with no sign flip — are wrong for it. Such callers must use
    /// `extend_pk_bytes`. Only the `#[cfg(test)]` `append_row` guard needs this
    /// today, so it is gated to test builds to keep the production API minimal.
    #[cfg(test)]
    #[inline]
    pub const fn pk_is_signed_single_col(&self) -> bool {
        if self.pk_count != 1 {
            return false;
        }
        let tc = self.columns[self.pk_indices[0] as usize].type_code;
        matches!(tc, type_code::I8 | type_code::I16 | type_code::I32 | type_code::I64)
    }

    /// The single PK column index. Use only at boundaries that have not yet
    /// been generalized (format encoders, catalog serialization, SQL parser
    /// path, wire/client BatchAppender). Asserts pk_count == 1.
    ///
    /// Hard `assert!` (not `debug_assert!`): this is the release-active
    /// canary for the day a compound PK reaches a boundary that has not
    /// yet been generalised — a `debug_assert!` would compile out in
    /// release and let the silent truncation to the first PK column ship
    /// to production.
    #[inline]
    #[track_caller]
    pub const fn pk_index_single(&self) -> u32 {
        assert!(self.pk_count == 1, "compound PK not yet supported here");
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

    /// True iff `cols` is a permutation of `pk_indices()` (same set,
    /// any order). Used by reduce to detect `GROUP BY pk` even when the
    /// SQL lists PK columns in an order that differs from the schema's
    /// pk-list order.
    pub fn group_cols_eq_pk(&self, cols: &[u32]) -> bool {
        let pk = self.pk_indices();
        cols.len() == pk.len() && pk.iter().all(|p| cols.contains(p))
    }

    /// Byte offset of `col_idx` within the row's PK region. Walks
    /// `pk_columns()` in pk-list order; caller must ensure `col_idx` is
    /// a PK column.
    pub fn pk_byte_offset(&self, col_idx: usize) -> u8 {
        debug_assert!(self.is_pk_col(col_idx), "pk_byte_offset: col_idx must be a pk column");
        let mut off: u16 = 0;
        for (_, pk_ci, c) in self.pk_columns() {
            if pk_ci == col_idx { return off as u8; }
            off += c.size() as u16;
        }
        unreachable!("pk_byte_offset: col_idx is a pk column but not found in pk_columns()");
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

#[inline(always)]
pub(crate) fn read_unsigned(bytes: &[u8], size: usize) -> u64 {
    debug_assert!(bytes.len() >= size, "read_unsigned: buffer too short ({} < {})", bytes.len(), size);
    match size {
        1 => bytes[0] as u64,
        2 => u16::from_le_bytes(bytes[..2].try_into().unwrap()) as u64,
        4 => u32::from_le_bytes(bytes[..4].try_into().unwrap()) as u64,
        8 => u64::from_le_bytes(bytes[..8].try_into().unwrap()),
        _ => unreachable!("read_unsigned: unexpected size {size}"),
    }
}

// Two distinct key spaces derive a `u128` from a column. They coincide for
// unsigned types and differ for signed:
//
// * ROUTING (`*_route_key`): the canonical `widen_pk_be(OPK)` value — sign-
//   flipped for signed. Used by exchange/`extract_group_key`, matching
//   `partition_for_pk_bytes`, which is schema-less and *cannot* decode, so it
//   must hash the OPK bytes' widened value. Both sides of a distributed join
//   agree only in this space.
// * INDEX (`*_native_key`): the native value (signed integers keep their
//   two's-complement bits, zero-extended). Used by FK validation, unique-index
//   maintenance, `has_pk`, and `seek_by_index`, which all re-encode native →
//   OPK at the storage boundary (`Table::opk_key`, `batch_project_index`), so
//   they need the native value back, not the sign-flipped one.

/// ROUTING key for one PK column's OPK bytes (canonical / sign-flipped).
/// `col_size` is the addressed column's width (≤ 16); `offset` its byte offset
/// within the PK region (0 for a lone PK).
#[inline]
pub(crate) fn pk_route_key(pk_bytes: &[u8], offset: usize, col_size: usize) -> u128 {
    debug_assert!(
        pk_bytes.len() >= offset + col_size,
        "pk_route_key: buffer too short ({} < {})",
        pk_bytes.len(), offset + col_size,
    );
    gnitz_wire::widen_pk_be(&pk_bytes[offset..offset + col_size], col_size)
}

/// ROUTING key for one native little-endian payload column (canonical). Integer
/// columns are OPK-encoded (signed sign-flip) then widened, so a payload FK
/// column routes to the same partition as the same value stored as a PK column.
/// U128/UUID are unsigned (OPK == native). Float/String/Blob have no PK
/// counterpart; they keep a zero-extended low-8-byte key.
#[inline]
pub(crate) fn payload_route_key(
    col_data: &[u8],
    offset: usize,
    col_size: usize,
    type_code_val: u8,
) -> u128 {
    debug_assert!(
        col_data.len() >= offset + col_size,
        "payload_route_key: buffer too short ({} < {})",
        col_data.len(), offset + col_size,
    );
    let src = &col_data[offset..offset + col_size];
    match TypeCode::from_validated_u8(type_code_val) {
        TypeCode::U128 | TypeCode::UUID => u128::from_le_bytes(src.try_into().unwrap()),
        TypeCode::U8 | TypeCode::I8 | TypeCode::U16 | TypeCode::I16
        | TypeCode::U32 | TypeCode::I32 | TypeCode::U64 | TypeCode::I64 => {
            let mut opk = [0u8; 16];
            gnitz_wire::encode_pk_column(src, type_code_val, &mut opk[..col_size]);
            gnitz_wire::widen_pk_be(&opk[..col_size], col_size)
        }
        TypeCode::F32 | TypeCode::F64 | TypeCode::String | TypeCode::Blob => {
            let mut bytes = [0u8; 8];
            let copy_len = col_size.min(8);
            bytes[..copy_len].copy_from_slice(&src[..copy_len]);
            u64::from_le_bytes(bytes) as u128
        }
    }
}

/// INDEX key for one PK column's OPK bytes: decode back to the native value
/// (signed bits preserved), zero-extended to `u128`. Feeds `has_pk` /
/// `seek_by_index`, which re-encode native → OPK to hit the OPK-stored index.
/// `offset + col_size` must lie within the OPK PK region (`pk_bytes`); a
/// mismatched `col_size` slices past the column and panics.
#[inline]
pub(crate) fn pk_native_key(
    pk_bytes: &[u8],
    offset: usize,
    col_size: usize,
    type_code_val: u8,
) -> u128 {
    debug_assert!(
        pk_bytes.len() >= offset + col_size,
        "pk_native_key: buffer too short ({} < {})",
        pk_bytes.len(), offset + col_size,
    );
    let mut le = [0u8; 16];
    gnitz_wire::decode_pk_column(&pk_bytes[offset..offset + col_size], type_code_val, &mut le[..col_size]);
    u128::from_le_bytes(le)
}

/// INDEX key for one native little-endian payload column: the native value,
/// zero-extended. U128/UUID read all 16 bytes; narrower types zero-extend the
/// low ≤8 bytes. Float/String/Blob keep the same zero-extended low-8-byte key.
#[inline]
pub(crate) fn payload_native_key(
    col_data: &[u8],
    offset: usize,
    col_size: usize,
    type_code_val: u8,
) -> u128 {
    debug_assert!(
        col_data.len() >= offset + col_size,
        "payload_native_key: buffer too short ({} < {})",
        col_data.len(), offset + col_size,
    );
    match TypeCode::from_validated_u8(type_code_val) {
        TypeCode::U128 | TypeCode::UUID => {
            u128::from_le_bytes(col_data[offset..offset + 16].try_into().unwrap())
        }
        _ => {
            let mut bytes = [0u8; 8];
            let copy_len = col_size.min(8);
            bytes[..copy_len].copy_from_slice(&col_data[offset..offset + copy_len]);
            u64::from_le_bytes(bytes) as u128
        }
    }
}

/// OPK-encode a native index-key value into the index's leading-column bytes,
/// for a prefix seek or a check-batch composite PK. The leading index column is
/// the promoted key type (`idx_key_type`, width `idx_key_size`); the index PK
/// region is OPK-at-rest, so the prefix must be order-preserving to match the
/// entries `batch_project_index` writes. Bytes beyond `idx_key_size` stay zero
/// (the source-PK suffix is not part of the leading-column prefix).
#[inline]
pub(crate) fn index_opk_prefix(
    native: u128,
    idx_key_type: u8,
    idx_key_size: usize,
) -> [u8; MAX_PK_BYTES] {
    let mut opk = [0u8; MAX_PK_BYTES];
    gnitz_wire::encode_pk_column(&native.to_le_bytes()[..idx_key_size], idx_key_type, &mut opk[..idx_key_size]);
    opk
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
pub(crate) use gnitz_wire::try_decode_german_string;

/// Bytes `[heap_offset, heap_offset + length)` of a long German string's blob
/// payload, or `&[]` if the header overruns `blob`. Mirrors
/// `relocate_german_string_vec`'s overrun guard: corrupt data that slipped past
/// validation degrades to an empty payload rather than slicing OOB in release.
/// A corrupt long string then hashes identically (`checksum(&[])`) at every
/// call site, so routing stays deterministic.
#[inline]
pub(crate) fn long_string_bytes(blob: &[u8], heap_offset: usize, length: usize) -> &[u8] {
    if heap_offset.saturating_add(length) > blob.len() {
        &[]
    } else {
        &blob[heap_offset..heap_offset + length]
    }
}

/// Full logical content bytes of a German string struct `s` (16-byte layout:
/// `[0..4]` = length, then inline-or-heap content). Short strings
/// (len ≤ SHORT_STRING_THRESHOLD) store content inline at `[4..4+length]`;
/// long strings live in `blob` at the heap offset. Unlike `german_string_tail`,
/// this returns the complete content with no prefix skip — for hashing or
/// whole-value equality. A zero-length string yields `&[]`.
#[inline]
pub(crate) fn german_string_content<'a>(s: &'a [u8], blob: &'a [u8]) -> &'a [u8] {
    let length = read_u32_le(s, 0) as usize;
    if length == 0 {
        &[]
    } else if length <= SHORT_STRING_THRESHOLD {
        &s[4..4 + length]
    } else {
        let heap_offset = read_u64_le(s, 8) as usize;
        long_string_bytes(blob, heap_offset, length)
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
        debug_assert!(s.len() >= 4 + end, "german_string_tail: short string struct too small");
        &s[8..4 + end]
    } else {
        let heap_offset = read_u64_le(s, 8) as usize;
        let start = heap_offset.saturating_add(4);
        let limit = heap_offset.saturating_add(end);
        debug_assert!(
            limit <= blob.len(),
            "german_string_tail: long string [{start}..{limit}) overruns blob (len={})",
            blob.len(),
        );
        // `end ≥ min_len > 4` at the one call site (compare_german_strings),
        // so `start < limit`; if `limit ≤ blob.len()` the slice is in bounds.
        if limit > blob.len() { &[] } else { &blob[start..limit] }
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
    fn test_long_string_bytes_overrun_returns_empty() {
        let blob = vec![1u8, 2, 3, 4, 5, 6, 7, 8];
        // In-bounds window returns the exact slice.
        assert_eq!(long_string_bytes(&blob, 2, 3), &[3u8, 4, 5]);
        // heap_offset + length past the end degrades to empty, no panic.
        assert_eq!(long_string_bytes(&blob, 6, 10), &[] as &[u8]);
        // heap_offset itself past the end.
        assert_eq!(long_string_bytes(&blob, 99, 1), &[] as &[u8]);
        // Saturating add: length near usize::MAX must not wrap to in-bounds.
        assert_eq!(long_string_bytes(&blob, 4, usize::MAX), &[] as &[u8]);
        // Exact fit is in bounds.
        assert_eq!(long_string_bytes(&blob, 0, 8), &blob[..]);
    }

    // The german_string_tail / compare_german_strings overrun fallback is
    // release-only: in debug the `debug_assert!` fires loudly (intended), so
    // these tests run only when debug assertions are off.
    #[cfg(not(debug_assertions))]
    #[test]
    fn test_german_string_tail_overrun_returns_empty() {
        // Long-string struct: length=20 (> SHORT_STRING_THRESHOLD), heap_offset
        // far beyond a tiny blob. Release path must return &[] not slice OOB.
        let mut s = [0u8; 16];
        s[0..4].copy_from_slice(&20u32.to_le_bytes());
        s[8..16].copy_from_slice(&999u64.to_le_bytes());
        let blob = vec![0u8; 4];
        // `end` = full length (the compare_german_strings call shape).
        assert_eq!(german_string_tail(&s, &blob, 20, 20), &[] as &[u8]);
    }

    #[cfg(not(debug_assertions))]
    #[test]
    fn test_compare_german_strings_corrupt_long_cell_no_panic() {
        // Two long-string cells with out-of-range heap offsets. Both tails
        // degrade to empty, so the comparison resolves on length/prefix
        // instead of panicking on an OOB blob slice.
        let mut a = [0u8; 16];
        a[0..4].copy_from_slice(&20u32.to_le_bytes());
        a[4..8].copy_from_slice(b"abcd");
        a[8..16].copy_from_slice(&500u64.to_le_bytes());
        let mut b = a;
        b[8..16].copy_from_slice(&900u64.to_le_bytes());
        let blob = vec![0u8; 4];
        // Equal length + equal prefix + empty tails ⇒ Equal, and crucially no panic.
        assert_eq!(compare_german_strings(&a, &blob, &b, &blob), std::cmp::Ordering::Equal);
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

        // Non-zero pk_index round-trips (use I64 col at index 1, not STRING).
        let s2 = SchemaDescriptor::new(&cols, &[1]);
        assert_eq!(s2.pk_index_single(), 1);

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

    #[test]
    fn test_pk_columns_single_pk() {
        let cols = [
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 0),
            SchemaColumn::new(type_code::U128, 0),
        ];
        let s = SchemaDescriptor::new(&cols, &[1]);
        let v: Vec<(usize, usize, u8)> = s
            .pk_columns()
            .map(|(ord, ci, c)| (ord, ci, c.type_code))
            .collect();
        assert_eq!(v, vec![(0, 1, type_code::I64)]);
        assert_eq!(s.pk_indices()[0], 1);
    }

    #[test]
    fn test_pk_stride_matches_single_pk_size() {
        // Floats are excluded: F32/F64 are not PK-eligible (rejected in `new`).
        for tc in [
            type_code::U8, type_code::I8, type_code::U16, type_code::I16,
            type_code::U32, type_code::I32,
            type_code::U64, type_code::I64,
            type_code::U128, type_code::UUID,
        ] {
            let cols = [SchemaColumn::new(tc, 0)];
            let s = SchemaDescriptor::new(&cols, &[0]);
            assert_eq!(
                s.pk_stride(),
                s.columns[s.pk_indices()[0] as usize].size(),
                "pk_stride mismatch for type_code {}", tc,
            );
        }
    }

    #[test]
    #[should_panic(expected = "STRING and BLOB columns cannot be PK columns")]
    fn test_string_pk_rejected() {
        let cols = [SchemaColumn::new(type_code::STRING, 0)];
        SchemaDescriptor::new(&cols, &[0]);
    }

    #[test]
    #[should_panic(expected = "STRING and BLOB columns cannot be PK columns")]
    fn test_blob_pk_rejected() {
        let cols = [SchemaColumn::new(type_code::BLOB, 0)];
        SchemaDescriptor::new(&cols, &[0]);
    }

    #[test]
    #[should_panic(expected = "F32 and F64 columns cannot be PK columns")]
    fn test_f32_pk_rejected() {
        let cols = [SchemaColumn::new(type_code::F32, 0)];
        SchemaDescriptor::new(&cols, &[0]);
    }

    #[test]
    #[should_panic(expected = "F32 and F64 columns cannot be PK columns")]
    fn test_f64_pk_rejected() {
        let cols = [SchemaColumn::new(type_code::F64, 0)];
        SchemaDescriptor::new(&cols, &[0]);
    }

    #[test]
    fn test_pk_stride_compound() {
        // Synthetic compound schema: [U64, U32] with both columns as PK.
        // Sum = 8 + 4 = 12.
        let cols = [
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::U32, 0),
        ];
        let s = SchemaDescriptor::new(&cols, &[0, 1]);
        assert_eq!(s.pk_stride(), 12);
        let v: Vec<(usize, usize, u8)> = s
            .pk_columns()
            .map(|(ord, ci, c)| (ord, ci, c.type_code))
            .collect();
        assert_eq!(v, vec![(0, 0, type_code::U64), (1, 1, type_code::U32)]);
    }

    #[test]
    fn test_max_pk_columns_boundary() {
        // Construct exactly MAX_PK_COLUMNS PK columns so a future bump
        // of the constant keeps exercising the boundary case.
        let cols = [SchemaColumn::new(type_code::U64, 0); MAX_PK_COLUMNS];
        let pks: Vec<u32> = (0..MAX_PK_COLUMNS as u32).collect();
        let s = SchemaDescriptor::new(&cols, &pks);
        assert_eq!(s.pk_indices().len(), MAX_PK_COLUMNS);
        let collected: Vec<(usize, usize)> = s
            .pk_columns()
            .map(|(ord, ci, _)| (ord, ci))
            .collect();
        let expected: Vec<(usize, usize)> =
            (0..MAX_PK_COLUMNS).map(|k| (k, k)).collect();
        assert_eq!(collected, expected);
        assert_eq!(s.pk_stride() as usize, MAX_PK_COLUMNS * 8);
    }

    #[test]
    fn test_default_empty_schema() {
        let s = SchemaDescriptor::default();
        assert_eq!(s.pk_columns().count(), 0);
        assert_eq!(s.pk_stride(), 0);
    }

    #[test]
    #[should_panic(expected = "duplicate PK column index")]
    fn test_duplicate_pk_guard_panics_in_release() {
        // No cfg(debug_assertions) gate — guard is a hard assert! and
        // must fire in release too.
        let cols = [
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::U64, 0),
        ];
        let _ = SchemaDescriptor::new(&cols, &[0, 0]);
    }

    #[test]
    #[should_panic(expected = "compound PK not yet supported here")]
    fn test_pk_index_single_panics_on_compound() {
        // Release-active canary the migration policy depends on.
        let cols = [
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::U32, 0),
        ];
        let s = SchemaDescriptor::new(&cols, &[0, 1]);
        let _ = s.pk_index_single();
    }

    #[test]
    fn read_unsigned_zero_extends() {
        // size 1: high-bit-set vs small — must match u8.cmp.
        assert_eq!(read_unsigned(&[0xFF], 1), 0xFF);
        assert_eq!(read_unsigned(&[0x01], 1), 0x01);
        assert!(read_unsigned(&[0xFF], 1) > read_unsigned(&[0x01], 1));

        // size 2: 0xFFFE > 0x0001 as unsigned (sign-extension would invert).
        assert_eq!(read_unsigned(&0xFFFEu16.to_le_bytes(), 2), 0xFFFE);
        assert_eq!(read_unsigned(&0x0001u16.to_le_bytes(), 2), 0x0001);
        assert!(
            read_unsigned(&0xFFFEu16.to_le_bytes(), 2)
                > read_unsigned(&0x0001u16.to_le_bytes(), 2),
        );

        // size 4.
        let big: u32 = 0xFFFF_FFFE;
        let small: u32 = 0x0000_0001;
        assert_eq!(read_unsigned(&big.to_le_bytes(), 4), big as u64);
        assert!(
            read_unsigned(&big.to_le_bytes(), 4)
                > read_unsigned(&small.to_le_bytes(), 4),
        );

        // size 8: full u64 round-trip.
        let v: u64 = 0xDEAD_BEEF_CAFE_BABE;
        assert_eq!(read_unsigned(&v.to_le_bytes(), 8), v);
    }
}
