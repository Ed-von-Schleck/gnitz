//! Shared helpers used by ≥2 sub-modules.

use crate::schema::{SchemaDescriptor, SHORT_STRING_THRESHOLD, TypeCode, type_code};
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
        crate::schema::read_unsigned(self.loc.bytes(mb, row), self.loc.size)
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
        TypeCode::F32 => ieee_order_bits_f32_reverse(e) as u64,
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
    // Single PK group column: isolate the addressed column's bytes from
    // neighbouring PK columns. For single-PK schemas this equals
    // `mb.get_pk(row)` bit-for-bit (off == 0, cs == pk_stride).
    if group_by_cols.len() == 1 && schema.is_pk_col(group_by_cols[0] as usize) {
        let c_idx = group_by_cols[0] as usize;
        let off = schema.pk_byte_offset(c_idx) as usize;
        let col = &schema.columns[c_idx];
        return crate::schema::promote_to_index_key(
            mb.get_pk_bytes(row), off, col.size() as usize, col.type_code,
        );
    }

    if group_by_cols.len() == 1 {
        let c_idx = group_by_cols[0] as usize;
        let tc = schema.columns[c_idx].type_code;
        // Skip fast path for nullable — hash loop handles NULL distinctly.
        if schema.columns[c_idx].nullable == 0 {
            if tc == type_code::U64 || tc == type_code::I64 {
                let pi = schema.payload_idx(c_idx);
                let ptr = mb.get_col_ptr(row, pi, 8);
                let v = u64::from_le_bytes(ptr.try_into().unwrap());
                return v as u128;
            }
            if tc == type_code::U128 || tc == type_code::UUID {
                let pi = schema.payload_idx(c_idx);
                let ptr = mb.get_col_ptr(row, pi, 16);
                return u128::from_le_bytes(ptr[0..16].try_into().unwrap());
            }
        }
    }

    let null_word = mb.get_null_word(row);
    let mut h: u64 = 0x9E3779B97F4A7C15; // golden ratio seed
    for (i, &c_idx_u32) in group_by_cols.iter().enumerate() {
        let c_idx = c_idx_u32 as usize;
        let tc = schema.columns[c_idx].type_code;
        let col_hash = if schema.is_pk_col(c_idx) {
            // Hash only the addressed PK column's bytes. The previous
            // `mb.get_pk(row)` widen mixed the whole PK region, colliding
            // distinct compound PKs whose addressed column shared bytes.
            let off = schema.pk_byte_offset(c_idx) as usize;
            let cs = schema.columns[c_idx].size() as usize;
            let pk_bytes = mb.get_pk_bytes(row);
            let col_bytes = &pk_bytes[off..off + cs];
            if tc == type_code::U128 || tc == type_code::UUID {
                let lo = u64::from_le_bytes(col_bytes[0..8].try_into().unwrap());
                let hi = u64::from_le_bytes(col_bytes[8..16].try_into().unwrap());
                mix64(lo ^ mix64(hi))
            } else {
                let mut buf = [0u8; 8];
                buf[..cs].copy_from_slice(col_bytes);
                mix64(u64::from_le_bytes(buf))
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
            } else if tc == type_code::U128 || tc == type_code::UUID {
                let ptr = mb.get_col_ptr(row, pi, 16);
                let lo = u64::from_le_bytes(ptr[0..8].try_into().unwrap());
                let hi = u64::from_le_bytes(ptr[8..16].try_into().unwrap());
                mix64(lo ^ mix64(hi))
            } else {
                let cs = schema.columns[c_idx].size() as usize;
                let ptr = mb.get_col_ptr(row, pi, cs);
                let mut buf = [0u8; 8];
                buf[..cs].copy_from_slice(ptr);
                mix64(u64::from_le_bytes(buf))
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
        // Packed LE slot: col0 in bytes [0..4], col1 in bytes [4..8].
        b.extend_pk(((c1 as u128) << 32) | c0 as u128);
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
}
