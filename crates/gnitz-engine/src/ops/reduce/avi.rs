//! The combined aggregate-value index (AVI): the secondary index a reduce's
//! MIN/MAX aggregates read their history out of, and its one owner.
//!
//! It is not free: every (input row × value-indexed aggregate) becomes one index
//! entry, so a reduce with a non-linear aggregate pays a whole extra table
//! ingest — that batch's own sort-and-consolidate plus a memtable push — on top
//! of the reduce's own group sort. `MIN(a), MAX(a), MIN(b)` triples it.

use crate::schema::key::{
    ieee_order_bits, ieee_order_bits_f32, ieee_order_bits_f32_reverse, ieee_order_bits_reverse, ReindexPacker,
};
use crate::schema::{type_code, ColumnLocator, SchemaColumn, SchemaDescriptor, TypeCode, MAX_PK_BYTES};
use crate::storage::{Batch, Table};
use gnitz_expr::RowSource;
use gnitz_wire::AggFunc;

use super::agg::AggDescriptor;

// ---------------------------------------------------------------------------
// Order-preserving aggregate-value codec
// ---------------------------------------------------------------------------
//
// `encode_ordered` and `decode_ordered` are mutual inverses; `tests/avi.rs`
// round-trips every encodable type against `gnitz_wire::cmp_typed_le`.

/// The value set [`encode_ordered`] has an arm for: a narrow (<=8B) fixed int or
/// float. Canonical predicates rather than a negative variant list, so a future
/// `TypeCode` is ineligible by default until classified — and every producer of
/// an order-encoded value asserts it, so the `unreachable!` arms below stay so.
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
/// [`agg_value_idx_eligible`].
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
// Key layout
// ---------------------------------------------------------------------------

/// Which non-linear aggregate an entry belongs to. Sits **between** the group key
/// and the value, so the byte-ordered key sorts by `(group, ordinal, av)`:
/// `MIN(a)` and `MAX(a)` coexist with no collision, and within an ordinal the
/// `for_max` encoding sorts the extreme first.
const ORDINAL_COL: SchemaColumn = SchemaColumn::new(type_code::U8, 0);
/// The order-encoded aggregate value: [`encode_ordered`] always emits a `u64`.
const VALUE_COL: SchemaColumn = SchemaColumn::new(type_code::U64, 0);
/// What the AVI appends behind a group key. The schema pushes exactly this, and
/// it is also the reservation `new_group_key` packs the group key inside — so
/// the suffix's width and column count have one definition, not two.
const SUFFIX: [SchemaColumn; 2] = [ORDINAL_COL, VALUE_COL];
const ORDINAL_BYTES: usize = ORDINAL_COL.size() as usize;
const VALUE_BYTES: usize = VALUE_COL.size() as usize;
// The writers below store the ordinal as one bare byte and the value through
// `u64::to_be_bytes`, which is each column's OPK image only at these two widths.
const _: () = assert!(ORDINAL_BYTES == 1 && VALUE_BYTES == 8);

// ---------------------------------------------------------------------------
// The compile-time bake
// ---------------------------------------------------------------------------

/// One value-indexed aggregate. Its position in [`AviBake::aggs`] is the ordinal
/// written into the key, so the write loop and `op_reduce`'s probe walk index one
/// list instead of each rebuilding the ordinal from a predicate.
///
/// `loc`, `tc` and `for_max` are write-side hoists — measured: LLVM lifts neither
/// `TypeCode::from_validated_u8` nor the `AggFunc` compare out of the
/// (row × aggregate) loop. They are reachable only from this file; the read side
/// gets [`AviBake::acc_indices`] and reads direction and type off the accumulator.
struct AviAgg {
    /// The reduce's own aggregate position — the accumulator this ordinal serves.
    acc_idx: u8,
    loc: ColumnLocator,
    tc: TypeCode,
    for_max: bool,
}

/// The AVI resources a reduce's `ReducePlan` carries, baked once at compile time.
pub(crate) struct AviBake {
    /// Packs a row's group columns into the key's leading OPK prefix. The
    /// *group*-key packer, which folds rather than rejects on overflow, so every
    /// group set has a key and no reduce is left rescanning its trace.
    key_packer: ReindexPacker,
    pub(crate) schema: SchemaDescriptor,
    /// The value-indexed aggregates in descriptor order; entry `j` is ordinal `j`.
    aggs: Vec<AviAgg>,
}

impl AviBake {
    /// Takes the reduce's *whole* descriptor list and applies the value-index
    /// selection itself, so the ordinal order has one spelling. The index schema
    /// is the key packer's own key columns then [`SUFFIX`], all PK, no payload.
    pub(super) fn new(src: &SchemaDescriptor, group_by_cols: &[u32], aggs: &[AggDescriptor]) -> Self {
        let key_packer = ReindexPacker::new_group_key(src, group_by_cols, &SUFFIX);
        let mut b = crate::schema::DerivedSchema::new();
        for c in key_packer.key_columns().chain(SUFFIX) {
            b.push_pk(c)
                .expect("a group key packed inside the SUFFIX reservation, plus SUFFIX, is non-null PK-eligible");
        }
        AviBake {
            schema: b.finish(),
            key_packer,
            aggs: aggs
                .iter()
                .enumerate()
                .filter(|(_, d)| d.agg_op.uses_value_index())
                .map(|(k, d)| {
                    let loc = src.locate(d.col_idx as usize);
                    let tc = TypeCode::from_validated_u8(loc.type_code());
                    // Bound to this construction rather than to the caller having
                    // built the accumulators first: an ineligible type would reach
                    // `encode_ordered`'s unreachable arms one row into the tick.
                    assert!(agg_value_idx_eligible(tc), "MIN/MAX over a non-order-encodable column");
                    AviAgg {
                        acc_idx: k as u8,
                        loc,
                        tc,
                        for_max: d.agg_op == AggFunc::Max,
                    }
                })
                .collect(),
        }
    }

    /// Whether any value-indexed aggregate reads an integer column — the only
    /// kind the probe-skip path can pre-step, since a float extreme always
    /// probes.
    pub(super) fn has_integer_extreme(&self) -> bool {
        self.aggs.iter().any(|a| !a.tc.is_float())
    }

    /// The accumulator each ordinal serves, in ordinal order.
    #[inline]
    pub(super) fn acc_indices(&self) -> impl Iterator<Item = usize> + '_ {
        self.aggs.iter().map(|a| a.acc_idx as usize)
    }

    /// Pack `row`'s group columns into the leading bytes of `buf`. Split from
    /// [`Self::prefix`] / [`Self::entry`] so a caller packs the group once per
    /// row or group and then only rewrites the per-aggregate tail.
    #[inline]
    pub(super) fn pack_group<R: RowSource>(&self, buf: &mut [u8], src: &R, row: usize) {
        let n = self.key_packer.out_stride;
        self.key_packer.pack_into(&mut buf[..n], src, row);
    }

    /// `group ‖ ordinal` over a buffer [`Self::pack_group`] already filled — the
    /// prefix `seek_first_positive_with_prefix` matches.
    #[inline]
    pub(super) fn prefix<'a>(&self, buf: &'a mut [u8], ord: u8) -> &'a [u8] {
        buf[self.key_packer.out_stride] = ord;
        &buf[..self.key_packer.out_stride + ORDINAL_BYTES]
    }

    /// `group ‖ ordinal ‖ av_encoded` over the same buffer: the prefix, then the
    /// value big-endian so the index's raw lexicographic byte order *is* the
    /// encoded value's order. Written through [`Self::prefix`], so the ordinal's
    /// position has one definition; the whole constant-length window is
    /// overwritten, so no stale bytes leak across the ordinal loop.
    #[inline]
    pub(super) fn entry<'a>(&self, buf: &'a mut [u8], ord: u8, av: u64) -> &'a [u8] {
        let n = self.prefix(buf, ord).len();
        buf[n..n + VALUE_BYTES].copy_from_slice(&av.to_be_bytes());
        &buf[..n + VALUE_BYTES]
    }

    /// The encoded value out of a full entry PK, given the prefix length it was
    /// sought by — the read-back half of [`Self::entry`].
    #[inline]
    pub(super) fn av_of(pk: &[u8], prefix_len: usize) -> u64 {
        debug_assert_eq!(
            pk.len(),
            prefix_len + VALUE_BYTES,
            "AVI key = seek prefix (group ‖ ordinal) ‖ value",
        );
        u64::from_be_bytes(pk[prefix_len..prefix_len + VALUE_BYTES].try_into().unwrap())
    }
}

// ---------------------------------------------------------------------------
// Population
// ---------------------------------------------------------------------------

/// The index entries `delta` contributes: one per (row × value-indexed
/// aggregate), keyed by [`AviBake::entry`], carrying the row's own weight.
///
/// Left `Raw`: the entry's trailing bytes are the aggregate *value* in row
/// order, so any group with ≥2 rows breaks ascension whatever order the delta
/// arrives in — the ingest's consolidation is what sorts it.
pub(super) fn avi_batch(delta: &Batch, bake: &AviBake) -> Batch {
    let mb = delta.as_mem_batch();
    let mut out = Batch::with_capacity(bake.schema, (delta.count * bake.aggs.len()).max(1));

    let mut key = [0u8; MAX_PK_BYTES];
    let mut scratch = [0u8; 16];
    for row in 0..delta.count {
        let weight = mb.get_weight(row);
        // A weight-0 row contributes nothing: consolidation drops the entry it
        // would write and `seek_first_positive_with_prefix` skips it, so writing
        // one is waste, not corruption. The sibling per-row projections drop it
        // too.
        if weight == 0 {
            continue;
        }
        bake.pack_group(&mut key, &mb, row);
        for (j, a) in bake.aggs.iter().enumerate() {
            // The value column is a non-nullable PK, so a NULL has no encoding:
            // skip the ordinal and let the seek miss (→ MIN/MAX renders NULL).
            // Writing one anyway would key a zeroed value and corrupt the extreme.
            if a.loc.is_null(&mb, row) {
                continue;
            }
            // `native_le_bytes` OPK-decodes a PK-source aggregate, so it encodes
            // identically to the same value in a payload column (and to the
            // batch-walk accumulator's `step_from_batch`, which reads through the
            // same accessor).
            let av = encode_ordered(a.loc.native_le_bytes(&mb, row, &mut scratch), a.tc, a.for_max);
            out.push_key_row(bake.entry(&mut key, j as u8, av), weight);
        }
    }
    out
}

/// Accumulate `delta`'s index entries into the reduce's value-index table. Runs
/// before the reduce reads the table, so a prefix seek returns the post-delta
/// extreme.
pub(crate) fn op_populate_avi(
    delta: &Batch,
    table: &mut Table,
    bake: &AviBake,
) -> Result<(), crate::storage::StorageError> {
    table.ingest_owned_batch(avi_batch(delta, bake))
}

#[cfg(test)]
#[path = "tests/avi.rs"]
mod tests;
