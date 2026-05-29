//! Shared helpers used by ≥2 sub-modules.

use crate::schema::{SchemaDescriptor, TypeCode, type_code};
use crate::schema::type_code::STRING as TYPE_STRING;
use crate::storage::{Batch, MemBatch, ReadCursor};
use crate::xxh;

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

                if col.type_code == TYPE_STRING {
                    crate::schema::compare_german_strings(
                        c_bytes,
                        cursor_blob_slice,
                        b_bytes,
                        batch.blob,
                    )
                } else {
                    cmp_typed_le(
                        c_bytes, b_bytes,
                        TypeCode::from_validated_u8(col.type_code),
                    )
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
    }
}

// ---------------------------------------------------------------------------
// Group key helpers (shared by reduce, exchange)
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

/// Extract 128-bit group key from a batch row.
pub(super) fn extract_group_key(
    mb: &MemBatch,
    row: usize,
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
        return crate::schema::pk_route_key(mb.get_pk_bytes(row), off, cs);
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
        if col.nullable == 0
            && matches!(tc,
                type_code::U8 | type_code::U16 | type_code::U32 | type_code::U64
                    | type_code::I8 | type_code::I16 | type_code::I32 | type_code::I64
                    | type_code::U128 | type_code::UUID)
        {
            let cs = col.size() as usize;
            let pi = schema.payload_idx(c_idx);
            let ptr = mb.get_col_ptr(row, pi, cs);
            return crate::schema::payload_route_key(ptr, 0, cs, tc);
        }
    }

    let null_word = mb.get_null_word(row);
    let mut h: u64 = 0x9E3779B97F4A7C15; // golden ratio seed
    for (i, &c_idx_u32) in group_by_cols.iter().enumerate() {
        let c_idx = c_idx_u32 as usize;
        let tc = schema.columns[c_idx].type_code;
        let col_hash = if schema.is_pk_col(c_idx) {
            // Canonical per-column key from the addressed PK column's OPK bytes,
            // so it hashes identically whether stored as a PK column here or a
            // native-LE payload FK on the join's other side (the payload arm
            // below applies the matching encode+widen).
            let off = schema.pk_byte_offset(c_idx) as usize;
            let cs = schema.columns[c_idx].size() as usize;
            let key = crate::schema::pk_route_key(mb.get_pk_bytes(row), off, cs);
            if tc == type_code::U128 || tc == type_code::UUID {
                let lo = key as u64;
                let hi = (key >> 64) as u64;
                mix64(lo ^ mix64(hi))
            } else {
                mix64(key as u64)
            }
        } else {
            let pi = schema.payload_idx(c_idx);
            if schema.columns[c_idx].nullable != 0 && (null_word >> pi) & 1 != 0 {
                // NULL — position-dependent constant keeps (NULL, v) distinct from (v, NULL).
                h = mix64(h ^ 0xDEAD_BEEF_DEAD_BEEF ^ (i as u64));
                continue;
            }
            if tc == TYPE_STRING {
                let struct_bytes = mb.get_col_ptr(row, pi, 16);
                let content = crate::schema::german_string_content(struct_bytes, mb.blob);
                if content.is_empty() { 0u64 } else { xxh::checksum(content) }
            } else if tc == type_code::U128 || tc == type_code::UUID {
                let ptr = mb.get_col_ptr(row, pi, 16);
                let lo = u64::from_le_bytes(ptr[0..8].try_into().unwrap());
                let hi = u64::from_le_bytes(ptr[8..16].try_into().unwrap());
                mix64(lo ^ mix64(hi))
            } else {
                // Native-LE payload int: encode+widen (payload_route_key) so a
                // payload FK column hashes like the same value stored as a PK
                // column (sign-flipped for signed), matching the PK arm above.
                let cs = schema.columns[c_idx].size() as usize;
                let ptr = mb.get_col_ptr(row, pi, cs);
                mix64(crate::schema::payload_route_key(ptr, 0, cs, tc) as u64)
            }
        };
        h = mix64(h ^ col_hash ^ (i as u64));
    }
    let h_hi = mix64(h ^ (group_by_cols.len() as u64));
    ((h_hi as u128) << 64) | (h as u128)
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
