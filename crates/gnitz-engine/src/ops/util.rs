//! Shared helpers used by ≥2 sub-modules.

use crate::schema::key::{
    hash_fold, ieee_order_bits, ieee_order_bits_f32, ieee_order_bits_f32_reverse, ieee_order_bits_reverse,
};
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

/// The exact value set [`encode_ordered`] can encode: a narrow (<=8B) fixed int
/// or float. Written as the canonical predicates rather than a negative variant
/// list, so a future `TypeCode` is ineligible by default until classified.
///
/// This is what keeps the `unreachable!` arms below unreachable — the compiler
/// rejects a MIN/MAX over anything it excludes. Change its *form* freely; never
/// widen its accepted set without giving those arms an encoding.
pub(super) fn agg_value_idx_eligible(tc: TypeCode) -> bool {
    gnitz_wire::is_fixed_int(tc as u8) || tc.is_float()
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
/// A canonical key is **injective** on the group value, which is what lets a
/// key-equality test stand in for a value comparison (the ad-hoc fold's
/// per-row confirmation), and it is order-preserving, so sorting by it visits
/// groups in ascending output-PK order.
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

/// The 128-bit group key of a row: the canonical single-column route key where
/// the group set has one, else an XXH3 fold of the per-column canonical
/// material. Per-column locators are resolved once at bake time, so the per-row
/// body is the fold alone.
///
/// The one implementation. It used to have a schema-walking twin whose only job
/// was to be byte-identical to this one, policed by a test; the twin is gone.
pub(super) struct GroupKeyCols {
    /// See [`single_col_canonical_group_key`]. Private, and read only through
    /// [`GroupKeyCols::canonical_col`], so the flag and the single column it
    /// promises can never be consulted apart.
    canonical: bool,
    /// The group columns' resolved locators, in group-set order. Empty for a
    /// global (ungrouped) aggregate, whose key is the constant
    /// `gnitz_wire::global_group_key()` — which is what `key_row` folds to over
    /// zero columns.
    pub(super) cols: Vec<ColumnLocator>,
}

impl GroupKeyCols {
    pub(super) fn new(schema: &SchemaDescriptor, group_by_cols: &[u32]) -> Self {
        GroupKeyCols {
            canonical: single_col_canonical_group_key(schema, group_by_cols),
            cols: group_by_cols.iter().map(|&c| schema.locate(c as usize)).collect(),
        }
    }

    /// The single column the group key is the canonical `route_key` of, or
    /// `None` when the key is the hash fold. `Some` is exactly "the key is
    /// injective and order-preserving on the group value".
    #[inline]
    pub(super) fn canonical_col(&self) -> Option<ColumnLocator> {
        self.canonical.then(|| self.cols[0])
    }

    /// The 128-bit group key of `row`. Over an empty group set this is the fold
    /// of nothing — `gnitz_wire::global_group_key()`, the V₀ every global
    /// aggregate keys its one row by.
    #[inline]
    pub(super) fn key_row<R: RowSource>(&self, src: &R, row: usize) -> u128 {
        if let Some(col) = self.canonical_col() {
            return col.route_key(src, row);
        }
        hash_fold(&self.cols, src, row, src.get_null_word(row))
    }
}

#[cfg(test)]
#[path = "tests/util.rs"]
mod tests;
