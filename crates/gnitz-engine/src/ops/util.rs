//! Shared helpers used by ≥2 sub-modules.

use xxhash_rust::xxh3::Xxh3Default;

use crate::schema::{SchemaDescriptor, TypeCode, type_code};
use crate::storage::{Batch, MemBatch, ReadCursor};

/// Compare two equal-length LE byte windows of a fixed-width column under
/// the given `TypeCode`. Unifies the per-TypeCode dispatch used by reduce's
/// PK-sentinel and payload arms and by `compare_cursor_payload_to_batch_row`.
/// String and Blob are not handled here — callers requiring them must
/// dispatch on String before delegating.
#[inline]
pub(super) fn cmp_typed_le(a: &[u8], b: &[u8], tc: TypeCode) -> std::cmp::Ordering {
    debug_assert_eq!(a.len(), b.len(), "cmp_typed_le: windows must be equal length");
    match tc {
        TypeCode::U128 | TypeCode::UUID => u128::from_le_bytes(a.try_into().unwrap())
            .cmp(&u128::from_le_bytes(b.try_into().unwrap())),
        // I128 payload (a cross-sign `_join_pk` surfaced into a payload slot) is
        // stored native-LE and compares as a signed two's-complement value.
        TypeCode::I128 => i128::from_le_bytes(a.try_into().unwrap())
            .cmp(&i128::from_le_bytes(b.try_into().unwrap())),
        TypeCode::F64 => f64::from_le_bytes(a.try_into().unwrap())
            .total_cmp(&f64::from_le_bytes(b.try_into().unwrap())),
        TypeCode::F32 => f32::from_le_bytes(a.try_into().unwrap())
            .total_cmp(&f32::from_le_bytes(b.try_into().unwrap())),
        TypeCode::I8 | TypeCode::I16 | TypeCode::I32 | TypeCode::I64 => {
            crate::schema::read_signed(a, a.len())
                .cmp(&crate::schema::read_signed(b, b.len()))
        }
        TypeCode::U8 | TypeCode::U16 | TypeCode::U32 | TypeCode::U64 => {
            crate::schema::read_unsigned(a, a.len())
                .cmp(&crate::schema::read_unsigned(b, b.len()))
        }
        TypeCode::String | TypeCode::Blob => {
            unreachable!("cmp_typed_le: caller must dispatch String/Blob separately")
        }
    }
}

/// Compare two equal-width column windows of type `tc`, dispatching German
/// strings (STRING/BLOB) to content comparison through their backing blob
/// arenas and every fixed-width type to [`cmp_typed_le`]. The blob slices back
/// each side's German-string heap tail (ignored for non-string columns).
///
/// This is the single home for the "STRING and BLOB share the 16-byte layout,
/// so `cmp_typed_le` panics on them and they must be compared by content" rule;
/// the group-by and payload comparators all route through here rather than
/// re-spelling the dispatch (a missed site panics the engine on a BLOB key).
#[inline]
pub(super) fn cmp_col_window(
    a: &[u8], a_blob: &[u8],
    b: &[u8], b_blob: &[u8],
    tc: TypeCode,
) -> std::cmp::Ordering {
    if tc.is_german_string() {
        crate::schema::compare_german_strings(a, a_blob, b, b_blob)
    } else {
        cmp_typed_le(a, b, tc)
    }
}

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
    let cell = crate::schema::relocate_german_string_vec(src, batch.blob, &mut output.blob, None);
    output.extend_col(out_pi, &cell);
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

#[inline]
pub(super) fn signum(x: i64) -> i64 {
    if x > 0 { 1 } else if x < 0 { -1 } else { 0 }
}

/// Merge a right-side null bitmap into a left-side bitmap for a composite output
/// row laid out as `[left payload..., right payload...]`. The right bits shift
/// up by `left_npc` (the left payload-column count). `right << 64` panics in
/// debug builds; `left_npc` reaches 64 only when the right side has no payload
/// columns, in which case `right` is 0 and the shift is a no-op.
#[inline]
pub(super) fn merge_null_words(left: u64, right: u64, left_npc: usize) -> u64 {
    if left_npc < 64 {
        left | (right << left_npc)
    } else {
        left
    }
}

/// Null-bitmap word with the low `npc` payload bits set — i.e. "all `npc`
/// payload columns are null". `npc` reaches the row-major cap of 64 only when a
/// schema has exactly 64 payload columns; `1u64 << 64` is UB / debug-panics, so
/// that boundary returns all-ones directly.
#[inline]
pub(crate) fn all_payload_null_mask(npc: usize) -> u64 {
    if npc < 64 { (1u64 << npc) - 1 } else { u64::MAX }
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
        let cs = col.size() as usize;

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
                cmp_col_window(
                    c_bytes, cursor_blob_slice,
                    b_bytes, batch.blob,
                    TypeCode::from_validated_u8(col.type_code),
                )
            }
        };

        if ord != Ordering::Equal {
            return ord;
        }
    }
    Ordering::Equal
}

// ---------------------------------------------------------------------------
// Fixed-width column location + raw index/group column extractors
// ---------------------------------------------------------------------------

/// Physical location of one fixed-width column within a `MemBatch` row: either
/// a byte offset into the PK region or a dense payload slot. Resolved once from
/// the schema so `bytes` is a pure slice with no per-row schema walk.
struct ColLoc {
    is_pk: bool,
    /// PK byte offset when `is_pk`, else the dense payload slot index.
    off_or_pi: usize,
    size: usize,
}

impl ColLoc {
    fn resolve(schema: &SchemaDescriptor, col_idx: usize) -> Self {
        let size = schema.columns[col_idx].size() as usize;
        let is_pk = schema.is_pk_col(col_idx);
        let off_or_pi = if is_pk {
            schema.pk_byte_offset(col_idx) as usize
        } else {
            schema.payload_idx(col_idx)
        };
        Self { is_pk, off_or_pi, size }
    }

    #[inline]
    fn bytes<'a>(&self, mb: &'a MemBatch, row: usize) -> &'a [u8] {
        if self.is_pk {
            &mb.get_pk_bytes(row)[self.off_or_pi..self.off_or_pi + self.size]
        } else {
            mb.get_col_ptr(row, self.off_or_pi, self.size)
        }
    }
}

/// Precomputed reader for one raw, injective index/group column. Built once
/// before a population or read-back loop so the per-row path is a slice plus a
/// `read_unsigned`, with no schema walk.
///
/// Raw (not hashed): GroupIndex replay groups source rows by this exact value
/// and does not re-filter by group membership, so the encoding must stay
/// injective within the column's domain — a hashed (colliding) value would
/// merge distinct groups. GI's gc column is always U8..I64 (size ∈ {1,2,4,8}),
/// so `read_unsigned` never sees an out-of-range width.
pub(super) struct IndexColExtractor {
    loc: ColLoc,
}

impl IndexColExtractor {
    pub(super) fn new(schema: &SchemaDescriptor, col_idx: usize) -> Self {
        Self { loc: ColLoc::resolve(schema, col_idx) }
    }

    #[inline]
    pub(super) fn extract(&self, mb: &MemBatch, row: usize) -> u64 {
        let bytes = self.loc.bytes(mb, row);
        if self.loc.is_pk {
            // PK columns hold OPK (big-endian) bytes at rest; widen to the
            // canonical value (native for unsigned, sign-flipped for signed).
            gnitz_wire::widen_pk_be(bytes, self.loc.size) as u64
        } else {
            crate::schema::read_unsigned(bytes, self.loc.size)
        }
    }
}

/// Precomputed gatherer for the byte-form AVI composite group key. Resolves
/// each group column's layout once; `gather` then concatenates raw bytes per
/// row. Callers must have passed `compiler::avi_group_key_eligible`
/// (fixed-width, non-nullable columns), so it never sees STRING/BLOB or a NULL.
pub(super) struct GroupKeyExtractor {
    cols: Vec<ColLoc>,
    /// Total group-key width in bytes (the group stride).
    pub(super) stride: usize,
}

impl GroupKeyExtractor {
    pub(super) fn new(schema: &SchemaDescriptor, group_by_cols: &[u32]) -> Self {
        let mut cols = Vec::with_capacity(group_by_cols.len());
        let mut stride = 0;
        for &c in group_by_cols {
            let ci = c as usize;
            // gather() reads each column's raw bytes unconditionally; a nullable
            // column would let a NULL row's stale slot bytes form a phantom key.
            // avi_group_key_eligible already excludes nullable/STRING/BLOB/float
            // columns — assert it so a future gate change can't silently regress.
            debug_assert_eq!(
                schema.columns[ci].nullable, 0,
                "GroupKeyExtractor: group columns must be non-nullable",
            );
            let loc = ColLoc::resolve(schema, ci);
            stride += loc.size;
            cols.push(loc);
        }
        Self { cols, stride }
    }

    /// Concatenate the group columns' raw little-endian bytes into `out` (which
    /// must be at least `self.stride` long), in `group_by_cols` order.
    #[inline]
    pub(super) fn gather(&self, mb: &MemBatch, row: usize, out: &mut [u8]) {
        let mut off = 0;
        for col in &self.cols {
            out[off..off + col.size].copy_from_slice(col.bytes(mb, row));
            off += col.size;
        }
    }
}

// ---------------------------------------------------------------------------
// Order-preserving aggregate-value codec (AVI keys)
// ---------------------------------------------------------------------------
//
// `encode_ordered` (index population) and `decode_ordered` (AVI scan in
// `agg::apply_agg_from_value_index`) are the two halves of one codec: encode
// maps a value to a u64 whose unsigned ordering matches the value's natural
// ordering, decode recovers the original bits. They must stay mutual inverses.

/// Width in bytes of the order-encoded aggregate value appended to every AVI
/// composite key (`group_key_bytes ++ av_encoded`). The codec always emits a
/// u64, so this is the trailing-segment length the index schema, the population
/// loop, and the lookup all agree on.
pub(crate) const AVI_AV_BYTES: usize = 8;

#[inline]
fn ieee_order_bits(raw_bits: u64) -> u64 {
    if raw_bits >> 63 != 0 {
        !raw_bits
    } else {
        raw_bits ^ (1u64 << 63)
    }
}

#[inline]
fn ieee_order_bits_reverse(encoded: u64) -> u64 {
    if encoded >> 63 != 0 {
        encoded ^ (1u64 << 63)
    } else {
        !encoded
    }
}

/// IEEE 754 order-preserving encoding for 32-bit floats, returning u64.
/// Checks the F32 sign bit (bit 31), not bit 63.
#[inline]
pub(super) fn ieee_order_bits_f32(raw_bits: u32) -> u64 {
    (if raw_bits >> 31 != 0 { !raw_bits } else { raw_bits ^ (1u32 << 31) }) as u64
}

/// Reverse of [`ieee_order_bits_f32`].
#[inline]
pub(super) fn ieee_order_bits_f32_reverse(encoded: u64) -> u32 {
    let e = encoded as u32;
    if e >> 31 != 0 { e ^ (1u32 << 31) } else { !e }
}

/// Order-preserving u64 encoding of a fixed-width aggregate value held in
/// `bytes` (native little-endian). Shared by the payload and PK-column AVI
/// paths so a PK aggregate column encodes identically to the same value in a
/// payload column. `for_max` inverts the order so the cursor's ascending walk
/// yields the maximum first. Width is ≤ 8 (F32 is 4); U128/UUID/String/Blob
/// are excluded upstream by `agg_value_idx_eligible`, so the fallback arm is
/// unreachable.
pub(super) fn encode_ordered(bytes: &[u8], col_type_code: u8, for_max: bool) -> u64 {
    let val = match col_type_code {
        type_code::F32 => ieee_order_bits_f32(u32::from_le_bytes(bytes[..4].try_into().unwrap())),
        type_code::F64 => ieee_order_bits(u64::from_le_bytes(bytes[..8].try_into().unwrap())),
        type_code::U8 | type_code::U16 | type_code::U32 | type_code::U64 => {
            crate::schema::read_unsigned(bytes, bytes.len())
        }
        type_code::I8 | type_code::I16 | type_code::I32 | type_code::I64 => {
            (crate::schema::read_signed(bytes, bytes.len()) as u64).wrapping_add(1u64 << 63)
        }
        other => unreachable!("AVI agg type {other} is not order-encodable (gated by agg_value_idx_eligible)"),
    };
    if for_max { !val } else { val }
}

/// Inverse of [`encode_ordered`]: recover the original value's raw little-endian
/// bits (IEEE bits for floats, two's-complement for signed, the value itself for
/// unsigned) from its order-preserving u64 key.
pub(super) fn decode_ordered(encoded: u64, col_type_code: TypeCode, for_max: bool) -> u64 {
    let e = if for_max { !encoded } else { encoded };
    match col_type_code {
        TypeCode::I8 | TypeCode::I16 | TypeCode::I32 | TypeCode::I64 => {
            (e as i64).wrapping_sub(1i64 << 63) as u64
        }
        TypeCode::F64 => ieee_order_bits_reverse(e),
        TypeCode::F32 => {
            // The accumulator and the reduce output column are F64; promote the
            // recovered F32 to F64 bits so the AVI seed matches the
            // step_from_batch path.
            let f32_bits = ieee_order_bits_f32_reverse(e);
            f64::to_bits(f32::from_bits(f32_bits) as f64)
        }
        TypeCode::U8 | TypeCode::U16 | TypeCode::U32 | TypeCode::U64
        | TypeCode::U128 | TypeCode::UUID | TypeCode::String => e,
        TypeCode::Blob => unreachable!("BLOB columns are not valid aggregate inputs"),
        TypeCode::I128 => unreachable!(
            "I128 is excluded from the AVI by agg_value_idx_eligible (16-byte value \
             cannot order-encode into the 8-byte slot)"),
    }
}

// ---------------------------------------------------------------------------
// Group key helpers (shared by reduce, exchange)
// ---------------------------------------------------------------------------

/// Feed a German-string column's content into `hasher` as a length-prefixed
/// byte run: a 4-byte LE length, then the content (following the heap pointer
/// for long strings). The length prefix keeps "ab"+"c" from aliasing "a"+"bc"
/// across adjacent columns. Shared by the group-key fold below and the set-op
/// row-identity hash (`reindex_hash_row`); the two MUST agree byte-for-byte so a
/// string key routes to — and dedups against — the partition it belongs to.
#[inline]
pub(super) fn hash_german_string_content(hasher: &mut Xxh3Default, struct_bytes: &[u8], blob: &[u8]) {
    let content = crate::schema::german_string_content(struct_bytes, blob);
    hasher.update(&(content.len() as u32).to_le_bytes());
    hasher.update(content);
}

/// Minimal row accessor shared by `extract_group_key` over a `MemBatch` row and
/// over a `ReadCursor`'s current row, so the group-key hash has ONE
/// implementation. The two must hash byte-identically — a divergence would
/// silently merge or split groups in the non-linear REDUCE fallback (wrong
/// MIN/MAX). Pinned by `extract_group_key_cursor_matches_batch`.
///
/// `c_idx` is a LOGICAL column index. The `MemBatch` impl maps it to a payload
/// slot via the passed schema; the `ReadCursor` impl maps it via the cursor's
/// own (identical) schema — the trace-in cursor is built with the input schema,
/// so both resolve the same physical layout.
trait GroupKeyRow {
    /// OPK (big-endian) PK byte window for the row.
    fn pk_bytes(&self) -> &[u8];
    /// Null bitmap word for the row.
    fn null_word(&self) -> u64;
    /// Raw little-endian bytes of non-PK logical column `c_idx` (`cs` wide), or
    /// `None` if the backing pointer is null (treated as an absent value).
    fn payload_bytes(&self, schema: &SchemaDescriptor, c_idx: usize, cs: usize) -> Option<&[u8]>;
    /// Blob arena backing this row's German strings.
    fn blob(&self) -> &[u8];
}

struct BatchRow<'a, 'b> {
    mb: &'b MemBatch<'a>,
    row: usize,
}

impl GroupKeyRow for BatchRow<'_, '_> {
    #[inline]
    fn pk_bytes(&self) -> &[u8] { self.mb.get_pk_bytes(self.row) }
    #[inline]
    fn null_word(&self) -> u64 { self.mb.get_null_word(self.row) }
    #[inline]
    fn payload_bytes(&self, schema: &SchemaDescriptor, c_idx: usize, cs: usize) -> Option<&[u8]> {
        // MemBatch get_col_ptr is infallible (panics OOB) — always Some, so the
        // batch path keeps its original non-null assumption and behaviour.
        Some(self.mb.get_col_ptr(self.row, schema.payload_idx(c_idx), cs))
    }
    #[inline]
    fn blob(&self) -> &[u8] { self.mb.blob }
}

impl GroupKeyRow for ReadCursor {
    #[inline]
    fn pk_bytes(&self) -> &[u8] { self.current_pk_bytes() }
    #[inline]
    fn null_word(&self) -> u64 { self.current_null_word }
    #[inline]
    fn payload_bytes(&self, _schema: &SchemaDescriptor, c_idx: usize, cs: usize) -> Option<&[u8]> {
        // col_ptr maps logical→payload internally and returns null for PK /
        // unpositioned cursors. The caller has already excluded PK columns and
        // scans only under `valid`, so null is an invariant violation — treat it
        // as an absent value rather than dereferencing it (UB).
        let p = self.col_ptr(c_idx, cs);
        if p.is_null() { None } else { Some(unsafe { std::slice::from_raw_parts(p, cs) }) }
    }
    #[inline]
    fn blob(&self) -> &[u8] { self.blob_slice() }
}

/// Extract 128-bit group key from a batch row.
#[inline]
pub(super) fn extract_group_key(
    mb: &MemBatch,
    row: usize,
    schema: &SchemaDescriptor,
    group_by_cols: &[u32],
) -> u128 {
    extract_group_key_row(&BatchRow { mb, row }, schema, group_by_cols)
}

/// Extract 128-bit group key from a `ReadCursor`'s current row. Hashes
/// byte-identically to [`extract_group_key`] for the same logical row, so a
/// trace row routes to the delta group it belongs to.
#[inline]
pub(super) fn extract_group_key_cursor(
    cursor: &ReadCursor,
    schema: &SchemaDescriptor,
    group_by_cols: &[u32],
) -> u128 {
    extract_group_key_row(cursor, schema, group_by_cols)
}

#[inline]
fn extract_group_key_row<R: GroupKeyRow>(
    row: &R,
    schema: &SchemaDescriptor,
    group_by_cols: &[u32],
) -> u128 {
    // Single PK group column: the addressed column's canonical key (native for
    // unsigned, sign-flipped for signed) via `pk_route_key` — identical to
    // `partition_for_pk_bytes` and `mb.get_pk(row)`, so a join key routes the
    // same whether it is a (sub-)PK column here or a single PK / payload FK on
    // the join's other side. For a single-PK schema this equals `mb.get_pk(row)`.
    if group_by_cols.len() == 1 && schema.is_pk_col(group_by_cols[0] as usize) {
        let c_idx = group_by_cols[0] as usize;
        let off = schema.pk_byte_offset(c_idx) as usize;
        let cs = schema.columns[c_idx].size() as usize;
        return crate::schema::pk_route_key(row.pk_bytes(), off, cs);
    }

    if group_by_cols.len() == 1 {
        let c_idx = group_by_cols[0] as usize;
        let col = &schema.columns[c_idx];
        let tc = col.type_code;
        // Skip fast path for nullable — the hash loop handles NULL distinctly.
        // A value routes to the same partition whether it is the PK on one side
        // of a join (OPK bytes, widened above) or a payload FK column on the
        // other: `payload_route_key` OPK-encodes+widens to the canonical value
        // (sign-flipped for signed), matching the PK side. STRING/BLOB/F32/F64
        // fall through to the hash loop — a zero-extended content prefix is not
        // a valid routing key for them.
        if col.nullable == 0 && crate::schema::is_routable_int(tc) {
            let cs = col.size() as usize;
            if let Some(b) = row.payload_bytes(schema, c_idx, cs) {
                return crate::schema::payload_route_key(b, 0, cs, tc);
            }
        }
    }

    // Fold path: multi-column GROUP BY, a single STRING/BLOB column, or a single
    // nullable int/U128/UUID column. Stream the same canonical per-column
    // material into a true 128-bit Xxh3 digest, gaining full 128-bit entropy
    // (matching reindex_hash_row). Order-sensitivity comes from the stream
    // itself: every column emits exactly one null marker before its content, so
    // (a, b) ≠ (b, a) and (NULL, v) ≠ (v, NULL) without an explicit index byte,
    // and STRING content is length-prefixed so "ab"+"c" can't alias "a"+"bc".
    let null_word = row.null_word();
    // Function-local streaming hasher: `Xxh3Default::new()` only copies the
    // initial accumulator (the 256-byte buffer is `MaybeUninit`), so it is as
    // cheap as `reset()` — no thread-local or per-row reuse needed.
    let mut hasher = Xxh3Default::new();
    for &c_idx_u32 in group_by_cols {
        let c_idx = c_idx_u32 as usize;
        let tc = schema.columns[c_idx].type_code;
        if schema.is_pk_col(c_idx) {
            // PK columns are non-nullable; canonical OPK-derived route key, so
            // a PK sub-column hashes like the same value as a payload FK on a
            // join's other side.
            let off = schema.pk_byte_offset(c_idx) as usize;
            let cs = schema.columns[c_idx].size() as usize;
            let key = crate::schema::pk_route_key(row.pk_bytes(), off, cs);
            hasher.update(&[1u8]); // non-null marker
            hasher.update(&key.to_le_bytes());
            continue;
        }
        let pi = schema.payload_idx(c_idx);
        let is_null = schema.columns[c_idx].nullable != 0 && (null_word >> pi) & 1 != 0;
        // `size()` is already 16 for STRING/BLOB/U128/UUID (all share the wide
        // 16-byte layout), so no per-type width fixup is needed here.
        let cs = schema.columns[c_idx].size() as usize;
        // A null payload bit OR a missing backing slot (cursor invariant
        // violation, never on the batch path) both hash as a bare NULL marker
        // — no deref.
        let bytes = if is_null { None } else { row.payload_bytes(schema, c_idx, cs) };
        let Some(b) = bytes else { hasher.update(&[0u8]); continue }; // null marker
        hasher.update(&[1u8]); // non-null marker
        if gnitz_wire::is_german_string(tc) {
            // STRING and BLOB both hash length-prefixed content via the shared
            // helper (matching reindex_hash_row); load-bearing for BLOB grouping
            // keys (Fix C), not only STRING.
            hash_german_string_content(&mut hasher, b, row.blob());
        } else if tc == type_code::U128 || tc == type_code::UUID {
            hasher.update(&b[..16]);
        } else {
            // Canonical (sign-flipped/widened) value: a payload FK hashes like
            // the same value stored as a PK column (matches the is_pk_col arm).
            let key = crate::schema::payload_route_key(b, 0, cs, tc);
            hasher.update(&key.to_le_bytes());
        }
    }
    hasher.digest128()
}

#[cfg(test)]
mod index_col_extractor_tests {
    use super::IndexColExtractor;
    use crate::schema::{SchemaColumn, SchemaDescriptor, type_code};
    use crate::storage::Batch;

    // Compound narrow PK (U32, U32), stride 8: the addressed column must be
    // sliced from the packed slot, not read as the whole slot.
    #[test]
    fn compound_pk_slices_addressed_column() {
        let schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U32, 0),
                SchemaColumn::new(type_code::U32, 0),
            ],
            &[0, 1],
        );
        assert_eq!(schema.pk_byte_offset(0), 0);
        assert_eq!(schema.pk_byte_offset(1), 4);

        let (c0, c1): (u32, u32) = (0x1111_2222, 0xAAAA_BBBB);
        let mut b = Batch::with_schema(schema, 1);
        // OPK slot: col0 OPK in bytes [0..4], col1 OPK in bytes [4..8].
        b.extend_pk_opk(&schema, &[c0 as u128, c1 as u128]);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.count += 1;
        let mb = b.as_mem_batch();

        let ex0 = IndexColExtractor::new(&schema, 0);
        let ex1 = IndexColExtractor::new(&schema, 1);
        assert_eq!(ex0.extract(&mb, 0), c0 as u64, "col 0 must be the low U32 only");
        assert_eq!(ex1.extract(&mb, 0), c1 as u64, "col 1 must be the high U32, not the whole slot");
    }

    // Single U64 PK spanning the whole slot: extractor reduces to the old
    // `source_pk as u64`, preserving bit-identical behaviour.
    #[test]
    fn single_u64_pk_is_bit_identical() {
        let schema = SchemaDescriptor::new(&[SchemaColumn::new(type_code::U64, 0)], &[0]);
        let v: u64 = 0xDEAD_BEEF_CAFE_F00D;
        let mut b = Batch::with_schema(schema, 1);
        b.extend_pk(v as u128);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.count += 1;
        let mb = b.as_mem_batch();

        let ex = IndexColExtractor::new(&schema, 0);
        assert_eq!(ex.extract(&mb, 0), v);
        assert_eq!(ex.extract(&mb, 0), mb.get_pk(0) as u64);
    }

    // Payload column path: the addressed payload slot is zero-extended to u64.
    #[test]
    fn payload_column_zero_extends() {
        let schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::U16, 0),
            ],
            &[0],
        );
        let pi = schema.payload_idx(1);
        let mut b = Batch::with_schema(schema, 1);
        b.extend_pk(7u128);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(pi, &0xBEEFu16.to_le_bytes());
        b.count += 1;
        let mb = b.as_mem_batch();

        let ex = IndexColExtractor::new(&schema, 1);
        assert_eq!(ex.extract(&mb, 0), 0xBEEFu64);
    }

    // Co-partition invariant (bug #2 regression): a single narrow-int
    // routing/group column must yield the canonical OPK key — the value an
    // OPK PK column produces via `widen_pk_be` (native for unsigned, sign-
    // flipped for signed). Pre-OPK both PK and payload sides used the raw
    // native value; after the flip the PK side is sign-flipped, so a signed
    // payload FK must match it or a distributed join silently drops rows.
    #[test]
    fn extract_group_key_single_narrow_int_canonical_widen() {
        use super::extract_group_key;
        use crate::storage::Batch as B;

        // Group key for `le` (native LE bytes) stored as a payload column at
        // idx 1 (U64 PK + tested column), routed by col 1.
        let key_as_payload = |tc: u8, le: &[u8]| -> u128 {
            let schema = SchemaDescriptor::new(
                &[SchemaColumn::new(type_code::U64, 0), SchemaColumn::new(tc, 0)],
                &[0],
            );
            let pi = schema.payload_idx(1);
            let mut b = B::with_schema(schema, 1);
            b.extend_pk(0u128);
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(pi, le);
            b.count += 1;
            extract_group_key(&b.as_mem_batch(), 0, &schema, &[1])
        };

        // Signed I32: canonical key is the sign-flipped value (top bit toggled
        // on the native bits), NOT the old zero-extended native value.
        for (v, expected) in [
            (1i32, 0x8000_0001u128),
            (-1, 0x7FFF_FFFF),
            (2, 0x8000_0002),
            (100, 0x8000_0064),
            (i32::MIN, 0x0000_0000),
            (i32::MAX, 0xFFFF_FFFF),
        ] {
            assert_eq!(
                key_as_payload(type_code::I32, &v.to_le_bytes()),
                expected,
                "I32 v={v}: canonical sign-flipped key",
            );
        }

        // Unsigned U16: canonical key equals the native value (OPK == native).
        for v in [0u16, 1, 0xBEEF, u16::MAX] {
            assert_eq!(key_as_payload(type_code::U16, &v.to_le_bytes()), v as u128);
        }
    }
}
