//! The group key shared by the reduce, the top-N and the exchange scatter:
//! whether a group set has a canonical (order-preserving, injective)
//! single-column key, the 128-bit key of a row either way, and the one grouping
//! mechanism built on it — a group is its output PK.

use std::ops::Range;

use crate::schema::key::{locate_key_col, pk_width_dispatch, FoldCols, NarrowPkOpk, PkSortKey};
use crate::schema::{
    ColumnLocator, DerivedSchema, OpBuildErr, ReduceOutKey, SchemaBound, SchemaColumn, SchemaDescriptor, SchemaFacts,
    TypeCode,
};
use crate::storage::{Batch, MemBatch};
use gnitz_expr::RowSource;
use gnitz_wire::ReduceOutSlot;

/// Whether the group key of `group_by_cols` is the single column's
/// `ColumnLocator::opk_image` rather than the XXH3 fold: injective and
/// order-preserving, so sorting by it visits groups in ascending output-PK order.
#[inline]
pub(super) fn single_col_canonical_group_key(schema: &SchemaDescriptor, group_by_cols: &[u32]) -> bool {
    matches!(*group_by_cols, [c] if schema.column(c as usize)
        .is_some_and(|col| !col.nullable && col.type_code.is_pk_eligible()))
}

/// The 128-bit group key of a row: the single group column's OPK image where
/// the group set is canonical, else an XXH3 fold of the per-column canonical
/// material.
pub(super) struct GroupKeyCols {
    /// The single column the group key is the canonical `opk_image` of, or
    /// `None` when the key is the hash fold. `Some` is exactly "the key is
    /// injective and order-preserving on the group value". See
    /// [`single_col_canonical_group_key`].
    pub(super) canonical: Option<ColumnLocator>,
    /// The group columns in group-set order. Empty for a global (ungrouped)
    /// aggregate, whose key is `gnitz_wire::global_group_key()` — the fold of
    /// zero columns.
    pub(super) cols: FoldCols,
}

impl GroupKeyCols {
    pub(crate) fn new(schema: &SchemaDescriptor, group_by_cols: &[u32]) -> Result<Self, OpBuildErr> {
        let cols: Vec<ColumnLocator> = group_by_cols
            .iter()
            .map(|&c| locate_key_col(schema, c, "group key"))
            .collect::<Result<_, _>>()?;
        Ok(GroupKeyCols {
            canonical: single_col_canonical_group_key(schema, group_by_cols).then(|| cols[0]),
            cols: FoldCols::new(cols),
        })
    }

    /// The 128-bit group key of `row`. Over an empty group set this is the fold
    /// of nothing — `gnitz_wire::global_group_key()`, the V₀ every global
    /// aggregate keys its one row by.
    #[inline]
    pub(super) fn key_row<R: RowSource>(&self, src: &R, row: usize) -> u128 {
        if let Some(col) = self.canonical {
            return col.opk_image(src, row);
        }
        self.cols.key_row(src, row, src.get_null_word(row))
    }
}

/// The synthetic `_group_pk` key — the whole PK region of an output whose group
/// set has no natural key.
const GROUP_PK_COL: SchemaColumn = SchemaColumn::new(TypeCode::U128, false);

/// One row's group output PK: borrowed out of the batch, or held inline.
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

/// Where a group's output PK comes from.
#[derive(Clone, Copy)]
enum KeyRegion {
    /// The source row's own PK: the group set is the PK list.
    SourcePk,
    /// The 128-bit group key, narrowed to this output PK width.
    Narrow(usize),
}

/// How an operator keyed like a reduce (the reduce itself, the ad-hoc fold, the
/// top-N) groups its input and keys its output. A group is its output PK.
pub(super) struct GroupOutKey {
    pub(super) group: GroupKeyCols,
    region: KeyRegion,
    /// The input columns the output carries after its key, as the input locates them.
    carried: Vec<ColumnLocator>,
}

impl GroupOutKey {
    /// `group_cols` over `input`, keyed as [`ReduceOutKey::for_group_cols`] picks, and
    /// the output's leading columns: the key region, then each `row` column the key
    /// region does not spell.
    pub(super) fn for_group_cols(
        input: &SchemaDescriptor,
        group_cols: &[u32],
        row: impl IntoIterator<Item = u32>,
    ) -> Result<(Self, DerivedSchema), OpBuildErr> {
        Self::build(input, group_cols, row, || input.reduce_out_key(group_cols))
    }

    /// [`Self::for_group_cols`] under the synthetic `_group_pk` whatever the group set.
    pub(super) fn synthetic(
        input: &SchemaDescriptor,
        group_cols: &[u32],
        row: impl IntoIterator<Item = u32>,
    ) -> Result<(Self, DerivedSchema), OpBuildErr> {
        Self::build(input, group_cols, row, || ReduceOutKey::SyntheticFold)
    }

    /// `kind` runs over group columns [`GroupKeyCols::new`] has bounded.
    fn build(
        input: &SchemaDescriptor,
        group_cols: &[u32],
        row: impl IntoIterator<Item = u32>,
        kind: impl FnOnce() -> ReduceOutKey,
    ) -> Result<(Self, DerivedSchema), OpBuildErr> {
        let group = GroupKeyCols::new(input, group_cols)?;
        let kind = kind();
        let over = |e: SchemaBound| OpBuildErr::shape(format!("group key: output {e}"));
        let mut b = DerivedSchema::new();
        let mut carried = Vec::new();
        for slot in kind.output_layout(group_cols, row) {
            match slot {
                ReduceOutSlot::SyntheticKey => b.push_pk(GROUP_PK_COL),
                ReduceOutSlot::Key(c) => b.push_pk(input.columns[c as usize]),
                ReduceOutSlot::Carried(c) => {
                    carried.push(input.locate(c as usize));
                    b.push(input.columns[c as usize])
                }
            }
            .map_err(over)?;
        }
        let region = match kind {
            ReduceOutKey::SourcePk => KeyRegion::SourcePk,
            _ => KeyRegion::Narrow(b.pk_bytes()),
        };
        Ok((GroupOutKey { group, region, carried }, b))
    }

    /// Grouped by the empty set: one group, V₀.
    #[inline]
    pub(super) fn is_global(&self) -> bool {
        self.group.cols.is_empty()
    }

    /// The output PK of `row`'s group.
    #[inline]
    pub(super) fn out_pk<'a>(&self, mb: &'a MemBatch, row: usize) -> OutPk<'a> {
        match self.region {
            KeyRegion::SourcePk => OutPk::Borrowed(mb.get_pk_bytes(row)),
            KeyRegion::Narrow(stride) => OutPk::Narrow(NarrowPkOpk::new(self.group.key_row(mb, row), stride)),
        }
    }

    /// The output PK of the group keyed `key`.
    #[inline]
    pub(super) fn narrow_pk(&self, key: u128) -> NarrowPkOpk {
        let KeyRegion::Narrow(stride) = self.region else {
            unreachable!("a PK-keyed group's output PK is the source's own")
        };
        NarrowPkOpk::new(key, stride)
    }

    /// `V₀`, the output PK of the empty group set's one group.
    #[inline]
    pub(super) fn ground_pk(&self) -> NarrowPkOpk {
        self.narrow_pk(gnitz_wire::global_group_key())
    }

    /// The input columns the output carries after its key.
    #[inline(always)]
    pub(super) fn carried(&self) -> &[ColumnLocator] {
        &self.carried
    }

    /// `batch`'s groups as runs in ascending output-PK order.
    pub(super) fn runs(&self, batch: &Batch) -> GroupRuns {
        let mb = &batch.as_mem_batch();
        let n = mb.count;
        // One run, with no key to compute.
        if n <= 1 || self.is_global() {
            return GroupRuns::in_place(n, |_| ());
        }
        let pk_keyed = matches!(self.region, KeyRegion::SourcePk);
        // A leading-PK-column key is that column's OPK bytes, widened.
        let leading_pk_col = matches!(self.group.canonical, Some(ColumnLocator::Pk { byte_off: 0, .. }));
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
        if self.group.canonical.is_some_and(|c| c.size() <= 8) {
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

    /// Whether positions are rows.
    #[inline]
    pub(super) fn in_row_order(&self) -> bool {
        self.order.is_none()
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
