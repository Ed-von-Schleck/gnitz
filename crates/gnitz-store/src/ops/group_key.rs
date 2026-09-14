//! The group key shared by the reduce and the exchange scatter: whether a group
//! set has a canonical (order-preserving, injective) single-column key, and the
//! 128-bit key of a row either way.

use crate::schema::key::{FoldCols, NarrowPkOpk, ReindexPacker};
use crate::schema::{type_code, ColumnLocator, DerivedSchema, ReduceOutKey, SchemaColumn, SchemaDescriptor};
use crate::storage::MemBatch;
use gnitz_expr::RowSource;

/// Whether the group key of `group_by_cols` can be emitted through the
/// canonical (order-preserving) fast path — `ColumnLocator::opk_image` on the
/// single group column — rather than the XXH3 fold (multi-column, nullable, or
/// non-routable type). Two shapes qualify: a single PK (sub-)column, whose OPK
/// window widens directly; and a single non-nullable routable-int payload
/// column, which OPK-encodes then widens to the same image — so a value routes
/// identically whether it is the PK on one side of a join or a payload FK on
/// the other. `opk_image` dispatches on the locator, so the two need no
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
    // `try_payload_idx` is the totality gate: `Some` proves `c` names a real
    // payload column, so the read below cannot land on a padding slot.
    schema
        .try_payload_idx(c)
        .is_some_and(|_| schema.columns[c].nullable == 0 && gnitz_wire::is_pk_eligible(schema.columns[c].type_code))
}

/// The 128-bit group key of a row: the single group column's OPK image where
/// the group set is canonical, else an XXH3 fold of the per-column canonical
/// material. Per-column locators are resolved once at bake time, so the per-row
/// body is the fold alone.
///
/// The one implementation: the scatter's routing key and `op_reduce`'s output
/// PK both come off it, so they cannot drift.
pub(super) struct GroupKeyCols {
    /// See [`single_col_canonical_group_key`]. Private, and read only through
    /// [`GroupKeyCols::canonical_col`], so the flag and the single column it
    /// promises can never be consulted apart.
    canonical: bool,
    /// The group columns in group-set order. Empty for a global (ungrouped)
    /// aggregate, whose key is `gnitz_wire::global_group_key()` — the fold of
    /// zero columns.
    pub(super) cols: FoldCols,
}

impl GroupKeyCols {
    pub(crate) fn new(schema: &SchemaDescriptor, group_by_cols: &[u32]) -> Self {
        // Resolve first: `locate` carries the release-active in-range assert, so
        // every index is proven before anything else reads `schema.columns`.
        let cols = group_by_cols.iter().map(|&c| schema.locate(c as usize)).collect();
        GroupKeyCols {
            canonical: single_col_canonical_group_key(schema, group_by_cols),
            cols: FoldCols::new(cols),
        }
    }

    /// The single column the group key is the canonical `opk_image` of, or
    /// `None` when the key is the hash fold. `Some` is exactly "the key is
    /// injective and order-preserving on the group value".
    #[inline]
    pub(super) fn canonical_col(&self) -> Option<ColumnLocator> {
        self.canonical.then(|| self.cols.locs()[0])
    }

    /// The 128-bit group key of `row`. Over an empty group set this is the fold
    /// of nothing — `gnitz_wire::global_group_key()`, the V₀ every global
    /// aggregate keys its one row by.
    #[inline]
    pub(super) fn key_row<R: RowSource>(&self, src: &R, row: usize) -> u128 {
        if let Some(col) = self.canonical_col() {
            return col.opk_image(src, row);
        }
        self.cols.key_row(src, row, src.get_null_word(row))
    }
}

/// The synthetic `_group_pk` key — the whole PK region of an output whose group
/// set has no natural key. One definition, so every operator keyed like a reduce
/// keys its output at the same width.
pub(super) const GROUP_PK_COL: SchemaColumn = SchemaColumn::new(type_code::U128, 0);

/// Push a group-keyed secondary index's PK region — the packed group key, then
/// the `suffix` columns the packer reserved room for — onto `b`. Infallible by
/// construction: `ReindexPacker::new_group_key` accepted this exact suffix, so
/// every column it hands back is non-null and PK-eligible and the whole region
/// fits.
pub(super) fn push_group_index_key(b: &mut DerivedSchema, packer: &ReindexPacker, suffix: &[SchemaColumn]) {
    for c in packer.key_columns().chain(suffix.iter().copied()) {
        b.push_pk(c)
            .expect("a group key packed inside the suffix reservation, plus the suffix, is non-null PK-eligible");
    }
}

/// One row's group output PK: borrowed out of the batch, or held inline. A
/// returned value rather than a caller's scratch, so the borrowed arm copies
/// nothing.
pub(super) enum OutPk<'a> {
    Borrowed(&'a [u8]),
    Narrow(NarrowPkOpk),
}

impl OutPk<'_> {
    /// The `pk_stride` OPK bytes of the key.
    #[inline]
    pub(super) fn bytes(&self) -> &[u8] {
        match self {
            OutPk::Borrowed(b) => b,
            OutPk::Narrow(k) => k.bytes(),
        }
    }
}

/// The output PK of the group `row` of `mb` belongs to, under `out_key` — the
/// one derivation for every operator keyed like a reduce (the reduce itself and
/// the top-N). `PkPermutation` borrows the input PK region; every other kind
/// keys by a value it does not carry, at the output's `out_stride`.
#[inline]
pub(super) fn group_out_pk<'a>(
    out_key: ReduceOutKey,
    group_key: &GroupKeyCols,
    out_stride: usize,
    mb: &'a MemBatch,
    row: usize,
) -> OutPk<'a> {
    if out_key == ReduceOutKey::PkPermutation {
        OutPk::Borrowed(mb.get_pk_bytes(row))
    } else {
        OutPk::Narrow(NarrowPkOpk::new(group_key.key_row(mb, row), out_stride))
    }
}

#[cfg(test)]
#[path = "tests/group_key.rs"]
mod tests;
