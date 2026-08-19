//! Shared helpers used by ≥2 sub-modules.

use crate::foundation::xxh::RowHasher;

use crate::schema::{ColumnLocator, SchemaDescriptor, TypeCode};
use gnitz_expr::RowSource;

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
pub(super) fn ieee_order_bits(raw_bits: u64) -> u64 {
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
/// `agg_value_idx_eligible`.
#[inline]
pub(super) fn encode_ordered(bytes: &[u8], col_type_code: TypeCode, for_max: bool) -> u64 {
    let val = match col_type_code {
        TypeCode::F32 => ieee_order_bits_f32(u32::from_le_bytes(bytes[..4].try_into().unwrap())),
        TypeCode::F64 => ieee_order_bits(u64::from_le_bytes(bytes[..8].try_into().unwrap())),
        TypeCode::U8 | TypeCode::U16 | TypeCode::U32 | TypeCode::U64 => gnitz_wire::read_unsigned_exact(bytes),
        TypeCode::I8 | TypeCode::I16 | TypeCode::I32 | TypeCode::I64 => {
            (gnitz_wire::read_signed_exact(bytes) as u64).wrapping_add(1u64 << 63)
        }
        TypeCode::String | TypeCode::Blob | TypeCode::U128 | TypeCode::UUID | TypeCode::I128 => unreachable!(
            "AVI agg type {col_type_code:?} is not order-encodable (compile-rejected by \
             agg_value_idx_eligible)"
        ),
    };
    if for_max {
        !val
    } else {
        val
    }
}

/// Inverse of [`encode_ordered`] at `for_max == false`: recover the original
/// value's raw little-endian bits (IEEE bits for floats, two's-complement for
/// signed, the value itself for unsigned). A `for_max` key is un-inverted by its
/// reader before it gets here.
#[inline]
pub(super) fn decode_ordered(e: u64, col_type_code: TypeCode) -> u64 {
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
        TypeCode::U8 | TypeCode::U16 | TypeCode::U32 | TypeCode::U64 => e,
        TypeCode::String | TypeCode::Blob | TypeCode::U128 | TypeCode::UUID | TypeCode::I128 => unreachable!(
            "AVI agg type {col_type_code:?} is not order-encodable (compile-rejected by \
             agg_value_idx_eligible)"
        ),
    }
}

// ---------------------------------------------------------------------------
// Group key helpers (shared by reduce, exchange)
// ---------------------------------------------------------------------------

/// Feed a German-string column's content into `hasher` as a length-prefixed
/// byte run: a 4-byte LE length, then the content (following the heap pointer
/// for long strings). The length prefix keeps "ab"+"c" from aliasing "a"+"bc"
/// across adjacent columns.
///
/// Shared by the group-key fold below and the set-op row-identity hash
/// (`reindex_hash_row`) so a string column contributes the same bytes to both.
/// The two *digests* still differ by construction and are meant to: the group
/// fold streams each column's canonical `route_key` under a `1`/`0` null marker,
/// the row hash streams raw native cell bytes under the inverted marker and a
/// leading branch discriminator. Only this per-column body is shared.
#[inline]
pub(super) fn hash_german_string_content(hasher: &mut RowHasher, struct_bytes: &[u8], blob: &[u8]) {
    let content = gnitz_wire::german_string_content(struct_bytes, blob);
    hasher.update(&(content.len() as u32).to_le_bytes());
    hasher.update(content);
}

/// Whether the group key of `group_by_cols` can be emitted through the
/// canonical (order-preserving) fast path — `ColumnLocator::route_key` on the
/// single group column — rather than the XXH3 fold (multi-column, nullable, or
/// non-routable type). Two shapes qualify: a single PK (sub-)column, whose OPK
/// window widens directly; and a single non-nullable routable-int payload
/// column, which OPK-encodes then widens to the same image — so a value routes
/// identically whether it is the PK on one side of a join or a payload FK on
/// the other. `route_key` dispatches on the locator, so the two need no
/// separate arm here.
///
/// `op_reduce` upgrades its `trace_out` retraction probe to the monotone
/// `advance_to` exactly when this is true: a canonical key ascends in
/// group-visit order because the group comparator, the route-key encoding, and
/// the OPK truncation to the output stride agree on one byte order — the
/// agreement `op_reduce`'s debug tripwire and the monotone-probe tests pin.
#[inline]
pub(super) fn single_col_canonical_group_key(schema: &SchemaDescriptor, group_by_cols: &[u32]) -> bool {
    if group_by_cols.len() != 1 {
        return false;
    }
    let c = group_by_cols[0] as usize;
    if schema.is_pk_col(c) {
        return true;
    }
    let col = &schema.columns[c];
    col.nullable == 0 && gnitz_wire::is_pk_eligible(col.type_code)
}

/// Hash one group column into the fold-path digest. The single per-column
/// body shared by the schema-walking [`extract_group_key`] and the baked
/// [`GroupKeyCols::key_row`] — a divergence would silently merge or split
/// groups (a wrong output PK, and a wrong AVI bucket).
///
/// Reads the null bit unconditionally, like the sibling `compare_by_group_cols`:
/// a NOT NULL column never carries one, so masking it off would cost a per-row
/// AND to change nothing.
#[inline]
pub(super) fn hash_group_col<R: RowSource>(
    hasher: &mut RowHasher,
    src: &R,
    row: usize,
    null_word: u64,
    loc: ColumnLocator,
) {
    match loc {
        ColumnLocator::Pk { .. } => {
            // PK columns are non-nullable; canonical OPK-derived route key, so
            // a PK sub-column hashes like the same value as a payload FK on a
            // join's other side.
            hasher.update(&[1u8]); // non-null marker
            hasher.update(&loc.route_key(src, row).to_le_bytes());
        }
        ColumnLocator::Payload { slot, size, type_code } => {
            if gnitz_wire::null_word_get(null_word, slot as usize) {
                hasher.update(&[0u8]); // null marker
                return;
            }
            hasher.update(&[1u8]); // non-null marker
            if gnitz_wire::is_german_string(type_code) {
                // STRING and BLOB both hash length-prefixed content via the shared
                // helper (matching reindex_hash_row). BLOB takes this path too: it
                // shares the 16-byte German-string struct, so hashing the struct
                // instead of the content would key on a heap pointer. `size` is
                // already 16 for both.
                hash_german_string_content(hasher, src.get_col_ptr(row, slot as usize, size as usize), src.blob());
            } else {
                // Canonical (sign-flipped/widened) value: a payload FK hashes like
                // the same value stored as a PK column — the same `route_key` the
                // Pk arm takes, which is exactly why the two agree. This covers
                // U128/UUID too: `payload_route_key`'s arm for them is
                // `u128::from_le_bytes(cell)`, so its `to_le_bytes()` *is* the
                // 16-byte cell.
                hasher.update(&loc.route_key(src, row).to_le_bytes());
            }
        }
    }
}

/// The 128-bit group key of a row: the canonical single-column route key where
/// the group set has one, else an XXH3 fold of the per-column canonical
/// material. Per-column locators are resolved once at bake time, so the per-row
/// body is the fold alone.
///
/// The one implementation. It used to have a schema-walking twin whose only job
/// was to be byte-identical to this one, policed by a test; the twin is gone.
pub(super) struct GroupKeyCols {
    canonical: bool,
    cols: Vec<ColumnLocator>,
}

impl GroupKeyCols {
    pub(super) fn new(schema: &SchemaDescriptor, group_by_cols: &[u32]) -> Self {
        GroupKeyCols {
            canonical: single_col_canonical_group_key(schema, group_by_cols),
            cols: group_by_cols.iter().map(|&c| schema.locate(c as usize)).collect(),
        }
    }

    /// The 128-bit group key of `row`.
    #[inline]
    pub(super) fn key_row<R: RowSource>(&self, src: &R, row: usize) -> u128 {
        if self.canonical {
            return self.cols[0].route_key(src, row);
        }
        let null_word = src.get_null_word(row);
        let mut hasher = RowHasher::new();
        for &loc in &self.cols {
            hash_group_col(&mut hasher, src, row, null_word, loc);
        }
        hasher.digest128()
    }
}

#[cfg(test)]
mod group_key_tests {
    use crate::schema::{type_code, SchemaColumn, SchemaDescriptor};

    // Co-partition invariant (bug #2 regression): a single narrow-int
    // routing/group column must yield the canonical OPK key — the value an
    // OPK PK column produces via `widen_pk_be` (native for unsigned, sign-
    // flipped for signed). Pre-OPK both PK and payload sides used the raw
    // native value; after the flip the PK side is sign-flipped, so a signed
    // payload FK must match it or a distributed join silently drops rows.
    #[test]
    fn group_key_single_narrow_int_canonical_widen() {
        use super::GroupKeyCols;
        use crate::storage::Batch as B;

        // Group key for `le` (native LE bytes) stored as a payload column at
        // idx 1 (U64 PK + tested column), routed by col 1.
        let key_as_payload = |tc: u8, le: &[u8]| -> u128 {
            let schema = SchemaDescriptor::new(&[SchemaColumn::new(type_code::U64, 0), SchemaColumn::new(tc, 0)], &[0]);
            let pi = schema.try_payload_idx(1).unwrap();
            let mut b = B::with_capacity(schema, 1);
            b.extend_pk(0u128);
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(pi, le);
            b.count += 1;
            GroupKeyCols::new(&schema, &[1]).key_row(&b.as_mem_batch(), 0)
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
