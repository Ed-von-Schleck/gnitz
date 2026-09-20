//! The combined aggregate-value index (AVI): the secondary index a reduce's
//! MIN/MAX aggregates read their history out of, and its one owner.
//!
//! It is not free: every (input row × value-indexed aggregate) becomes one index
//! entry, so a reduce with a non-linear aggregate pays a whole extra table
//! ingest — that batch's own sort-and-consolidate plus a memtable push — on top
//! of the reduce's own group sort. `MIN(a), MAX(a), MIN(b)` triples it.

use crate::schema::key::ReindexPacker;
use crate::schema::{type_code, SchemaColumn, SchemaDescriptor, MAX_PK_BYTES};
use crate::storage::{Batch, ReadCursor};
use gnitz_expr::payload_bytes;
use gnitz_expr::RowSource;

use super::super::order_image::{
    append_wide_image, scalar_image, wide_native, wide_native_of_image, write_image_slot, ImageKind,
};
use super::agg::{Accumulator, ExtremeSpec};

// ---------------------------------------------------------------------------
// Key layout
// ---------------------------------------------------------------------------

/// Which non-linear aggregate an entry belongs to. Sits **between** the group key
/// and the value, so the byte-ordered key sorts by `(group, ordinal, av)`:
/// `MIN(a)` and `MAX(a)` coexist with no collision, and within an ordinal the
/// `for_max` encoding sorts the extreme first.
const ORDINAL_COL: SchemaColumn = SchemaColumn::new(type_code::U8, 0);
/// The ordinal's image, spreading one ordinal's entries over many PKs so the
/// linear equal-PK walks stay short.
const VALUE_COL: SchemaColumn = SchemaColumn::new(type_code::U64, 0);
/// [`VALUE_COL`] for a bake with a wide ordinal.
const WIDE_VALUE_COL: SchemaColumn = SchemaColumn::new(type_code::U128, 0);
/// A wide ordinal's whole image, and the order the seek follows: a BLOB payload
/// sorts by content. Pushed only when a wide ordinal exists.
const WIDE_COL: SchemaColumn = SchemaColumn::new(type_code::BLOB, 0);
/// What the AVI appends behind a group key — one of the two, by whether the bake
/// has a wide ordinal. The schema pushes exactly this, and it is also the
/// reservation `new_group_key` packs the group key inside.
const SUFFIX: [SchemaColumn; 2] = [ORDINAL_COL, VALUE_COL];
const WIDE_SUFFIX: [SchemaColumn; 2] = [ORDINAL_COL, WIDE_VALUE_COL];
const ORDINAL_BYTES: usize = ORDINAL_COL.size() as usize;
// [`AviBake::prefix`] stores the ordinal as one bare byte.
const _: () = assert!(ORDINAL_BYTES == 1);

// ---------------------------------------------------------------------------
// The compile-time bake
// ---------------------------------------------------------------------------

/// One value-indexed aggregate. Its position in [`AviBake::aggs`] is the ordinal
/// written into the key, so the write loop and `op_reduce`'s probe walk index one
/// list instead of each rebuilding the ordinal from a predicate.
struct AviAgg {
    /// The reduce's own aggregate position — the accumulator this ordinal serves.
    acc_idx: u8,
    /// Where and how this ordinal reads its value. A write-side hoist, measured:
    /// LLVM lifts neither the type resolve nor the direction test out of the
    /// (row × aggregate) loop.
    spec: ExtremeSpec,
}

/// The AVI resources a reduce's `ReducePlan` carries, baked once at compile time.
pub struct AviBake {
    /// Packs a row's group columns into the key's leading OPK prefix. The
    /// *group*-key packer, which folds rather than rejects on overflow, so every
    /// group set has a key and no reduce is left rescanning its trace.
    key_packer: ReindexPacker,
    pub schema: SchemaDescriptor,
    /// The value-indexed aggregates in accumulator order; entry `j` is ordinal `j`.
    aggs: Vec<AviAgg>,
    /// Width of the value slot this bake's suffix reserved; every ordinal of one
    /// bake shares it, wide or scalar.
    value_bytes: usize,
}

impl AviBake {
    /// Applies the value-index selection to the reduce's *whole* accumulator set
    /// itself, so the ordinal order has one spelling. The index schema is the key
    /// packer's own key columns then [`SUFFIX`], all PK, plus [`WIDE_COL`] when
    /// a wide ordinal needs it.
    ///
    /// The index's key bytes are the group key's, so it inherits
    /// `new_group_key`'s rejections verbatim.
    pub(crate) fn new(
        src: &SchemaDescriptor,
        group_by_cols: &[u32],
        accs: &[Accumulator],
    ) -> Result<Self, crate::schema::OpBuildErr> {
        let aggs: Vec<AviAgg> = accs
            .iter()
            .enumerate()
            .filter_map(|(k, acc)| {
                let spec = acc.extreme_index_spec()?;
                Some(AviAgg { acc_idx: k as u8, spec })
            })
            .collect();
        let has_wide = aggs.iter().any(|a| matches!(a.spec.kind, ImageKind::Wide(_)));
        let suffix = if has_wide { &WIDE_SUFFIX } else { &SUFFIX };
        let key_packer = ReindexPacker::new_group_key(src, group_by_cols, suffix)?;
        let mut b = crate::schema::DerivedSchema::new();
        super::super::group_key::push_group_index_key(&mut b, &key_packer, suffix);
        if has_wide {
            b.push(WIDE_COL)
                .expect("one payload column fits behind a PK-only schema");
        }
        Ok(AviBake {
            schema: b.finish(),
            key_packer,
            aggs,
            value_bytes: suffix[1].size() as usize,
        })
    }

    /// Each ordinal's accumulator index, in ordinal order.
    #[inline]
    pub(super) fn acc_indices(&self) -> impl Iterator<Item = usize> + '_ {
        self.aggs.iter().map(|a| a.acc_idx as usize)
    }

    /// Pack `row`'s group columns into the leading bytes of `buf`. Split from
    /// [`Self::prefix`] / [`Self::entry`] so a caller packs the group once per
    /// row or group and then only rewrites the per-aggregate tail.
    #[inline]
    pub(super) fn pack_group<R: RowSource>(&self, buf: &mut [u8], src: &R, row: usize) {
        self.key_packer.pack_prefix(buf, src, row);
    }

    /// `group ‖ ordinal` over a buffer [`Self::pack_group`] already filled — the
    /// prefix `seek_first_positive_with_prefix` matches.
    #[inline]
    pub(super) fn prefix<'a>(&self, buf: &'a mut [u8], ord: u8) -> &'a [u8] {
        buf[self.key_packer.out_stride] = ord;
        &buf[..self.key_packer.out_stride + ORDINAL_BYTES]
    }

    /// `group ‖ ordinal ‖ image` over the same buffer, the image big-endian so
    /// the index's raw lexicographic byte order *is* the encoded value's order.
    #[inline]
    pub(super) fn entry<'a>(&self, buf: &'a mut [u8], ord: u8, image: &[u8]) -> &'a [u8] {
        let n = self.prefix(buf, ord).len();
        write_image_slot(&mut buf[n..n + self.value_bytes], image);
        &buf[..n + self.value_bytes]
    }

    /// Seek ordinal `ord`'s group and seed `acc` with its extreme, or reset `acc`
    /// on a miss. `key` is a buffer [`Self::pack_group`] has already filled.
    pub(super) fn seed_extreme(&self, cur: &mut ReadCursor, key: &mut [u8], ord: usize, acc: &mut Accumulator) {
        let spec = &self.aggs[ord].spec;
        let prefix = self.prefix(key, ord as u8);
        if !cur.seek_first_positive_with_prefix(prefix) {
            acc.reset();
            return;
        }
        // Entries sort by (group, ordinal, av, image) with the MAX complement
        // putting this ordinal's extreme first, so the first positive entry
        // under the prefix is it — undo the complement to get what `acc` holds.
        match spec.kind {
            ImageKind::Scalar(_) => {
                let av = self.av_of(cur.current_pk_bytes(), prefix.len());
                acc.seed_encoded_extreme(if spec.for_max { !av } else { av });
            }
            ImageKind::Wide(kind) => {
                // Every key column is PK, so [`WIDE_COL`] is payload slot 0.
                let (src, row) = cur.current_row_source();
                let image = payload_bytes(src, row, 0);
                acc.seed_wide(&wide_native_of_image(kind, spec.for_max, image));
            }
        }
    }

    /// A scalar ordinal's encoded value out of a full entry PK, given the prefix
    /// length it was sought by — the read-back half of [`Self::entry`].
    #[inline]
    fn av_of(&self, pk: &[u8], prefix_len: usize) -> u64 {
        debug_assert_eq!(
            pk.len(),
            prefix_len + self.value_bytes,
            "AVI key = seek prefix (group ‖ ordinal) ‖ value",
        );
        u64::from_be_bytes(pk[prefix_len..prefix_len + 8].try_into().unwrap())
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
pub fn avi_batch(delta: &Batch, bake: &AviBake) -> Batch {
    let mb = delta.as_mem_batch();
    let mut out = Batch::with_capacity(&bake.schema, (delta.count * bake.aggs.len()).max(1));

    let mut key = [0u8; MAX_PK_BYTES];
    let (mut image, mut scratch) = (Vec::new(), [0u8; 16]);
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
            if a.spec.loc.is_null(&mb, row) {
                continue;
            }
            let ExtremeSpec { loc, kind, for_max } = a.spec;
            match kind {
                ImageKind::Scalar(kind) => {
                    let av = scalar_image(&loc, kind, for_max, &mb, row).to_be_bytes();
                    out.push_zero_filled_row(bake.entry(&mut key, j as u8, &av), weight, 0);
                }
                ImageKind::Wide(kind) => {
                    image.clear();
                    append_wide_image(
                        kind,
                        for_max,
                        wide_native(&loc, kind, &mb, row, &mut scratch),
                        &mut image,
                    );
                    out.begin_row(bake.entry(&mut key, j as u8, &image), weight);
                    out.extend_col_blob(0, &image);
                    out.commit_row(0);
                }
            }
        }
    }
    out
}

#[cfg(test)]
#[path = "tests/avi.rs"]
mod tests;
