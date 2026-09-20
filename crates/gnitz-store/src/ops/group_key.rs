//! The group key shared by the reduce, the top-N and the exchange scatter:
//! whether a group set has a canonical (order-preserving, injective)
//! single-column key, the 128-bit key of a row either way, and the one grouping
//! mechanism built on it — a group is its output PK.

use std::ops::Range;

use crate::schema::key::{locate_key_col, pk_width_dispatch, FoldCols, NarrowPkOpk, PkSortKey, ReindexPacker};
use crate::schema::{
    type_code, ColumnLocator, DerivedSchema, OpBuildErr, ReduceOutKey, SchemaColumn, SchemaDescriptor,
};
use crate::storage::{Batch, MemBatch};
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
/// A canonical key is **injective** on the group value and order-preserving, so
/// sorting by it visits groups in ascending output-PK order.
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
    pub(crate) fn new(schema: &SchemaDescriptor, group_by_cols: &[u32]) -> Result<Self, OpBuildErr> {
        let cols = group_by_cols
            .iter()
            .map(|&c| locate_key_col(schema, c, "group key"))
            .collect::<Result<_, _>>()?;
        Ok(GroupKeyCols {
            canonical: single_col_canonical_group_key(schema, group_by_cols),
            cols: FoldCols::new(cols),
        })
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

/// How an operator keyed like a reduce (the reduce itself, the ad-hoc fold, the
/// top-N) groups its input and keys its output. A group is its output PK.
pub(super) struct GroupOutKey {
    pub(super) kind: ReduceOutKey,
    pub(super) group: GroupKeyCols,
    out_stride: usize,
}

impl GroupOutKey {
    /// `output` is the schema built for `kind`, whose PK region the key fills.
    pub(super) fn new(
        input: &SchemaDescriptor,
        group_cols: &[u32],
        kind: ReduceOutKey,
        output: &SchemaDescriptor,
    ) -> Result<Self, OpBuildErr> {
        Ok(GroupOutKey {
            kind,
            group: GroupKeyCols::new(input, group_cols)?,
            out_stride: output.pk_stride(),
        })
    }

    /// The output PK of `row`'s group.
    #[inline]
    pub(super) fn out_pk<'a>(&self, mb: &'a MemBatch, row: usize) -> OutPk<'a> {
        if self.kind == ReduceOutKey::PkPermutation {
            OutPk::Borrowed(mb.get_pk_bytes(row))
        } else {
            OutPk::Narrow(NarrowPkOpk::new(self.group.key_row(mb, row), self.out_stride))
        }
    }

    /// `V₀`, the output PK of the empty group set's one group.
    #[inline]
    pub(super) fn ground_pk(&self) -> NarrowPkOpk {
        NarrowPkOpk::new(gnitz_wire::global_group_key(), self.out_stride)
    }

    /// The group columns the output carries as its leading payload. Only a
    /// synthetic key has any: a natural key is the group value itself.
    #[inline(always)]
    pub(super) fn exemplar_locs(&self) -> &[ColumnLocator] {
        if self.kind == ReduceOutKey::SyntheticFold {
            self.group.cols.locs()
        } else {
            &[]
        }
    }

    /// `batch`'s groups as runs in ascending output-PK order.
    pub(super) fn runs(&self, batch: &Batch) -> GroupRuns {
        let mb = &batch.as_mem_batch();
        let n = mb.count;
        // One run, with no key to compute.
        if n <= 1 || self.group.cols.is_empty() {
            return GroupRuns::in_place(n, |_| ());
        }
        let pk_keyed = self.kind == ReduceOutKey::PkPermutation;
        // A leading-PK-column key is that column's OPK bytes, widened.
        let leading_pk_col = matches!(self.group.canonical_col(), Some(ColumnLocator::Pk { byte_off: 0, .. }));
        if (pk_keyed || leading_pk_col) && batch.consolidated_verified(batch.schema()) {
            return match pk_keyed {
                true => GroupRuns::in_place(n, |i| mb.get_pk_bytes(i)),
                false => GroupRuns::in_place(n, |i| self.group.key_row(mb, i)),
            };
        }
        if pk_keyed {
            return pk_width_dispatch!(mb.pk_stride as usize, |K| GroupRuns::sorted(n, |i| K::from_opk(
                mb.get_pk_bytes(i)
            )));
        }
        // Exact, not a truncation: a canonical key over a ≤8-byte column fits 64
        // bits, and halves the sorted payload for `GROUP BY <BIGINT>`.
        if self.group.canonical_col().is_some_and(|c| c.size() <= 8) {
            GroupRuns::sorted(n, |i| self.group.key_row(mb, i) as u64)
        } else {
            GroupRuns::sorted(n, |i| self.group.key_row(mb, i))
        }
    }
}

/// A batch's groups as contiguous runs of positions, in ascending output-PK
/// order. A position maps to a batch row through [`Self::row`].
pub(super) struct GroupRuns {
    /// Position → row, or `None` when the batch is already in group order.
    order: Option<Vec<u32>>,
    /// Each run's exclusive end position, ascending; the last is the row count.
    ends: Vec<u32>,
}

impl GroupRuns {
    /// Runs over rows already in group order.
    fn in_place<K: PartialEq>(n: usize, key: impl Fn(usize) -> K) -> Self {
        GroupRuns { order: None, ends: run_ends(n, key) }
    }

    /// Runs over `0..n` sorted by `key`, computed once per row. The row index
    /// breaks ties, so a group's rows keep source order — which a float SUM's low
    /// bits depend on.
    pub(super) fn sorted<K: Ord>(n: usize, key: impl Fn(usize) -> K) -> Self {
        let mut pairs: Vec<(K, u32)> = (0..n).map(|i| (key(i), i as u32)).collect();
        pairs.sort_unstable();
        GroupRuns {
            ends: run_ends(n, |p| &pairs[p].0),
            order: Some(pairs.into_iter().map(|(_, i)| i).collect()),
        }
    }

    /// The batch row at visit position `pos`.
    #[inline(always)]
    pub(super) fn row(&self, pos: usize) -> usize {
        self.order.as_ref().map_or(pos, |o| o[pos] as usize)
    }

    /// The group count.
    #[inline]
    pub(super) fn len(&self) -> usize {
        self.ends.len()
    }

    /// Each group's position range, in visit order.
    pub(super) fn iter(&self) -> impl Iterator<Item = Range<usize>> + '_ {
        let starts = std::iter::once(0).chain(self.ends.iter().map(|&e| e as usize));
        starts.zip(self.ends.iter().map(|&e| e as usize)).map(|(s, e)| s..e)
    }
}

/// The exclusive end of each run of equal adjacent keys over `0..n`.
fn run_ends<K: PartialEq>(n: usize, key: impl Fn(usize) -> K) -> Vec<u32> {
    let mut ends = Vec::new();
    if n == 0 {
        return ends;
    }
    let mut prev = key(0);
    for i in 1..n {
        let k = key(i);
        if k != prev {
            ends.push(i as u32);
        }
        prev = k;
    }
    ends.push(n as u32);
    ends
}

#[cfg(test)]
#[path = "tests/group_key.rs"]
mod tests;
