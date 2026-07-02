//! Shared helpers used by ≥2 sub-modules.

use xxhash_rust::xxh3::Xxh3Default;

use crate::schema::{type_code, ColumnLocator, SchemaDescriptor, TypeCode};
use crate::storage::{Batch, MemBatch, ReadCursor};

/// Compare two equal-width column windows of type `tc` — the `ops`-side adapter over
/// storage's single-homed [`crate::storage::cmp_col_window`], which dispatches German
/// strings (STRING/BLOB) to content comparison and every fixed-width type to its
/// little-endian comparator. `tc as u8` is the raw type code (`TypeCode` is
/// `#[repr(u8)]`); the group-by and payload comparators route through here so the
/// `ops` side keeps thinking in `TypeCode`.
#[inline]
pub(super) fn cmp_col_window(a: &[u8], a_blob: &[u8], b: &[u8], b_blob: &[u8], tc: TypeCode) -> std::cmp::Ordering {
    crate::storage::cmp_col_window(a, a_blob, b, b_blob, tc as u8)
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
    if npc < 64 {
        (1u64 << npc) - 1
    } else {
        u64::MAX
    }
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
                    c_bytes,
                    cursor_blob_slice,
                    b_bytes,
                    batch.blob,
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
// Raw index/group column extractors
// ---------------------------------------------------------------------------

/// Precomputed gatherer for the byte-form AVI composite group key. Resolves
/// each group column's layout once; `gather` then concatenates raw bytes per
/// row. Callers must have passed `query::compiler::avi_group_key_eligible`
/// (fixed-width, non-nullable columns), so it never sees STRING/BLOB or a NULL.
pub(super) struct GroupKeyExtractor {
    cols: Vec<ColumnLocator>,
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
            // columns, but the constructor runs once per operator (not per row),
            // so a hard assert here is free insurance against a future gate
            // change silently corrupting group keys.
            assert_eq!(
                schema.columns[ci].nullable, 0,
                "GroupKeyExtractor: group columns must be non-nullable",
            );
            let loc = schema.locate(ci);
            stride += loc.size();
            cols.push(loc);
        }
        Self { cols, stride }
    }

    /// Concatenate the group columns' raw little-endian bytes into `out` (which
    /// must be at least `self.stride` long), in `group_by_cols` order.
    #[inline]
    pub(super) fn gather(&self, mb: &MemBatch, row: usize, out: &mut [u8]) {
        let mut off = 0;
        for loc in &self.cols {
            let size = loc.size();
            out[off..off + size].copy_from_slice(loc.bytes(mb, row));
            off += size;
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
    (if raw_bits >> 31 != 0 {
        !raw_bits
    } else {
        raw_bits ^ (1u32 << 31)
    }) as u64
}

/// Reverse of [`ieee_order_bits_f32`].
#[inline]
pub(super) fn ieee_order_bits_f32_reverse(encoded: u64) -> u32 {
    let e = encoded as u32;
    if e >> 31 != 0 {
        e ^ (1u32 << 31)
    } else {
        !e
    }
}

/// Order-preserving u64 encoding of a fixed-width aggregate value held in
/// `bytes` (native little-endian). A PK aggregate column's at-rest bytes are
/// the OPK window (big-endian, sign-flipped) — callers read them through
/// `ColumnLocator::native_le_bytes`, so a PK aggregate column encodes
/// identically to the same value in a payload column. `for_max` inverts
/// the order so the cursor's ascending walk yields the maximum first. Width is
/// ≤ 8 (F32 is 4); U128/UUID/String/Blob are excluded upstream by
/// `agg_value_idx_eligible`, so the fallback arm is unreachable.
#[inline]
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
    if for_max {
        !val
    } else {
        val
    }
}

/// Inverse of [`encode_ordered`]: recover the original value's raw little-endian
/// bits (IEEE bits for floats, two's-complement for signed, the value itself for
/// unsigned) from its order-preserving u64 key.
#[inline]
pub(super) fn decode_ordered(encoded: u64, col_type_code: TypeCode, for_max: bool) -> u64 {
    let e = if for_max { !encoded } else { encoded };
    match col_type_code {
        TypeCode::I8 | TypeCode::I16 | TypeCode::I32 | TypeCode::I64 => (e as i64).wrapping_sub(1i64 << 63) as u64,
        TypeCode::F64 => ieee_order_bits_reverse(e),
        TypeCode::F32 => {
            // The accumulator and the reduce output column are F64; promote the
            // recovered F32 to F64 bits so the AVI seed matches the
            // step_from_batch path.
            let f32_bits = ieee_order_bits_f32_reverse(e);
            f64::to_bits(f32::from_bits(f32_bits) as f64)
        }
        TypeCode::U8
        | TypeCode::U16
        | TypeCode::U32
        | TypeCode::U64
        | TypeCode::U128
        | TypeCode::UUID
        | TypeCode::String => e,
        TypeCode::Blob => unreachable!("BLOB columns are not valid aggregate inputs"),
        TypeCode::I128 => unreachable!(
            "I128 is excluded from the AVI by agg_value_idx_eligible (16-byte value \
             cannot order-encode into the 8-byte slot)"
        ),
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
    fn pk_bytes(&self) -> &[u8] {
        self.mb.get_pk_bytes(self.row)
    }
    #[inline]
    fn null_word(&self) -> u64 {
        self.mb.get_null_word(self.row)
    }
    #[inline]
    fn payload_bytes(&self, schema: &SchemaDescriptor, c_idx: usize, cs: usize) -> Option<&[u8]> {
        // MemBatch get_col_ptr is infallible (panics OOB) — always Some, so the
        // batch path keeps its original non-null assumption and behaviour.
        let pi = schema
            .try_payload_idx(c_idx)
            .expect("group-key column is non-PK by construction");
        Some(self.mb.get_col_ptr(self.row, pi, cs))
    }
    #[inline]
    fn blob(&self) -> &[u8] {
        self.mb.blob
    }
}

impl GroupKeyRow for ReadCursor {
    #[inline]
    fn pk_bytes(&self) -> &[u8] {
        self.current_pk_bytes()
    }
    #[inline]
    fn null_word(&self) -> u64 {
        self.current_null_word
    }
    #[inline]
    fn payload_bytes(&self, _schema: &SchemaDescriptor, c_idx: usize, cs: usize) -> Option<&[u8]> {
        // col_ptr maps logical→payload internally and returns null for PK /
        // unpositioned cursors. The caller has already excluded PK columns and
        // scans only under `valid`, so null is an invariant violation — treat it
        // as an absent value rather than dereferencing it (UB).
        let p = self.col_ptr(c_idx, cs);
        if p.is_null() {
            None
        } else {
            Some(unsafe { std::slice::from_raw_parts(p, cs) })
        }
    }
    #[inline]
    fn blob(&self) -> &[u8] {
        self.blob_slice()
    }
}

/// Extract 128-bit group key from a batch row.
#[inline]
pub(super) fn extract_group_key(mb: &MemBatch, row: usize, schema: &SchemaDescriptor, group_by_cols: &[u32]) -> u128 {
    extract_group_key_row(&BatchRow { mb, row }, schema, group_by_cols)
}

/// The 128-bit group key of the **empty** group-column set — the constant `V₀` a
/// global (ungrouped) aggregate folds to. Batch-free: identical to the XXH3 empty
/// digest `extract_group_key(.., &[])` produces over any populated row (the
/// zero-column fold reads no row content), but without touching a batch, so it is
/// callable at emit time (no batch exists) and over an empty delta — where
/// `extract_group_key`'s unconditional `null_word()` read would index an empty
/// slice and panic (a safe slice index, so it panics in release too). The single
/// seam where `V₀` is defined: the owner-bake's `partition_for_key`, the seed's
/// `emit_global_ground` PK and `trace_out` probe all route through this, while the
/// runtime router and PK-stamp use the equal `extract_group_key` over a real row —
/// so every site agrees byte-for-byte with no embedded literal.
#[inline]
pub(crate) fn global_group_key() -> u128 {
    Xxh3Default::new().digest128()
}

/// Extract 128-bit group key from a `ReadCursor`'s current row. Hashes
/// byte-identically to [`extract_group_key`] for the same logical row, so a
/// trace row routes to the delta group it belongs to.
#[inline]
pub(super) fn extract_group_key_cursor(cursor: &ReadCursor, schema: &SchemaDescriptor, group_by_cols: &[u32]) -> u128 {
    extract_group_key_row(cursor, schema, group_by_cols)
}

/// Which canonical (order-preserving) fast-path arm [`extract_group_key_row`]
/// emits the group key of `group_by_cols` through, or `None` for the XXH3
/// fold (multi-column, nullable, or non-routable type). `op_reduce` upgrades
/// its `trace_out` retraction probe to the monotone `advance_to` exactly when
/// this is `Some`: a canonical key ascends in group-visit order because the
/// group comparator, the route-key encoding, and the OPK truncation to the
/// output stride agree on one byte order — the agreement `op_reduce`'s debug
/// tripwire and the monotone-probe tests pin.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(super) enum CanonicalKeyArm {
    /// Single PK (sub-)column: key via `pk_route_key` over the OPK window.
    Pk,
    /// Single non-nullable routable-int payload column: key via
    /// `payload_route_key`.
    Payload,
}

#[inline]
pub(super) fn single_col_canonical_group_key(
    schema: &SchemaDescriptor,
    group_by_cols: &[u32],
) -> Option<CanonicalKeyArm> {
    if group_by_cols.len() != 1 {
        return None;
    }
    let c = group_by_cols[0] as usize;
    if schema.is_pk_col(c) {
        return Some(CanonicalKeyArm::Pk);
    }
    let col = &schema.columns[c];
    (col.nullable == 0 && crate::schema::is_routable_int(col.type_code)).then_some(CanonicalKeyArm::Payload)
}

#[inline]
fn extract_group_key_row<R: GroupKeyRow>(row: &R, schema: &SchemaDescriptor, group_by_cols: &[u32]) -> u128 {
    // Canonical single-column fast path, dispatched by the shared
    // `single_col_canonical_group_key` classifier (see its doc).
    //
    // Pk arm: the addressed column's canonical key (native for unsigned,
    // sign-flipped for signed) via `pk_route_key` — identical to
    // `partition_for_pk_bytes` and `mb.get_pk(row)`, so a join key routes the
    // same whether it is a (sub-)PK column here or a single PK / payload FK on
    // the join's other side. For a single-PK schema this equals
    // `mb.get_pk(row)`.
    //
    // Payload arm: a value routes to the same partition whether it is the PK on
    // one side of a join (OPK bytes, widened) or a payload FK column on the
    // other: `payload_route_key` OPK-encodes+widens to the canonical value
    // (sign-flipped for signed), matching the PK side. Nullable columns (the
    // hash loop handles NULL distinctly) and STRING/BLOB/F32/F64 fall through
    // to the hash loop — a zero-extended content prefix is not a valid routing
    // key for them.
    if let Some(arm) = single_col_canonical_group_key(schema, group_by_cols) {
        let c_idx = group_by_cols[0] as usize;
        let col = &schema.columns[c_idx];
        let cs = col.size() as usize;
        match arm {
            CanonicalKeyArm::Pk => {
                let off = schema.pk_byte_offset(c_idx) as usize;
                return crate::schema::pk_route_key(row.pk_bytes(), off, cs);
            }
            CanonicalKeyArm::Payload => {
                // `payload_bytes == None` (a cursor-impl invariant violation
                // only, never on the `MemBatch` path) falls through to the
                // fold's NULL-marker hash.
                if let Some(b) = row.payload_bytes(schema, c_idx, cs) {
                    return crate::schema::payload_route_key(b, 0, cs, col.type_code);
                }
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
        let pi = schema
            .try_payload_idx(c_idx)
            .expect("non-PK: PK columns handled in the branch above");
        let is_null = schema.columns[c_idx].nullable != 0 && (null_word >> pi) & 1 != 0;
        // `size()` is already 16 for STRING/BLOB/U128/UUID (all share the wide
        // 16-byte layout), so no per-type width fixup is needed here.
        let cs = schema.columns[c_idx].size() as usize;
        // A null payload bit OR a missing backing slot (cursor invariant
        // violation, never on the batch path) both hash as a bare NULL marker
        // — no deref.
        let bytes = if is_null {
            None
        } else {
            row.payload_bytes(schema, c_idx, cs)
        };
        let Some(b) = bytes else {
            hasher.update(&[0u8]);
            continue;
        }; // null marker
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
mod group_key_tests {
    use crate::schema::{type_code, SchemaColumn, SchemaDescriptor};
    use crate::storage::Batch;

    // The batch-free `global_group_key()` (used at emit time and over an empty
    // delta) must equal the runtime `extract_group_key(.., &[])` over a real,
    // arbitrarily-populated row — they are the SAME V₀, so routing, PK-stamp,
    // owner-bake and seed all agree byte-for-byte. Row content must not perturb
    // it (the empty-column fold reads nothing).
    #[test]
    fn global_group_key_matches_runtime_empty_cols() {
        use super::{extract_group_key, global_group_key};
        let schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::I64, 1),
            ],
            &[0],
        );
        let v0 = global_group_key();
        for (pk, payload, null) in [(1u128, 42i64, false), (999, -7, false), (0, 0, true)] {
            let mut b = Batch::with_schema(schema, 1);
            b.extend_pk(pk);
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&(null as u64).to_le_bytes());
            b.extend_col(0, &payload.to_le_bytes());
            b.count += 1;
            let mb = b.as_mem_batch();
            assert_eq!(
                extract_group_key(&mb, 0, &schema, &[]),
                v0,
                "extract_group_key over empty group cols must equal global_group_key regardless of row content",
            );
        }
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
            let schema = SchemaDescriptor::new(&[SchemaColumn::new(type_code::U64, 0), SchemaColumn::new(tc, 0)], &[0]);
            let pi = schema.try_payload_idx(1).unwrap();
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
