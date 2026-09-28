//! The group key shared by the reduce, the top-N, the ad-hoc fold and the
//! exchange scatter: [`GroupKey`] resolves a group set to the output PK a reduce
//! over it stamps, and the one grouping mechanism built on it — a group is its
//! output PK.

use std::ops::Range;

use crate::schema::key::{locate_key_col, pk_width_dispatch, FoldCols, NarrowPkOpk, PkSortKey};
use crate::schema::{
    ColumnLocator, DerivedSchema, OpBuildErr, ReduceOutKey, SchemaBound, SchemaColumn, SchemaDescriptor, SchemaFacts,
    TypeCode,
};
use crate::storage::{Batch, MemBatch};
use gnitz_wire::{ReduceOutSlot, NARROW_PK_MAX_BYTES};

/// A row's group key, as the output PK a reduce over the group set stamps.
pub(super) enum GroupKey {
    /// The leading `n` PK bytes: the whole PK, or a single leading PK column.
    PkPrefix(usize),
    /// One column's OPK image, at that column's width.
    Image(ColumnLocator),
    /// The NULL-distinct XXH3 fold of the group columns, as the 16-byte
    /// `_group_pk`. Over no columns it is V₀.
    Fold(FoldCols),
}

impl GroupKey {
    /// Refused when a group column is out of range or has no key image.
    pub(super) fn new(schema: &SchemaDescriptor, group_cols: &[u32]) -> Result<Self, OpBuildErr> {
        let locs: Vec<ColumnLocator> = group_cols
            .iter()
            .map(|&c| locate_key_col(schema, c, "group key"))
            .collect::<Result<_, _>>()?;
        Ok(match (schema.reduce_out_key(group_cols), &locs[..]) {
            (ReduceOutKey::SyntheticFold, _) => GroupKey::Fold(FoldCols::new(locs)),
            (ReduceOutKey::Natural, &[ColumnLocator::Pk { byte_off: 0, size, .. }]) => {
                GroupKey::PkPrefix(size as usize)
            }
            (ReduceOutKey::Natural, &[loc]) => GroupKey::Image(loc),
            (ReduceOutKey::Natural, _) => GroupKey::PkPrefix(schema.pk_stride()),
        })
    }
}

/// The synthetic `_group_pk` key — the whole PK region of an output whose group
/// set has no natural key.
const GROUP_PK_COL: SchemaColumn = SchemaColumn::new(TypeCode::U128, false);
const GROUP_PK_BYTES: usize = GROUP_PK_COL.size() as usize;

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

/// A row loop keyed by group identity; see [`GroupOutKey::with_identity`].
pub(super) trait IdentityLoop {
    type Out;
    fn run(self, identity: impl Fn(usize) -> u128) -> Self::Out;
}

/// V₀, the output PK of the empty group set's one group.
#[inline]
pub(super) fn ground_pk() -> NarrowPkOpk {
    NarrowPkOpk::new(gnitz_wire::global_group_key(), GROUP_PK_BYTES)
}

/// How an operator keyed like a reduce (the reduce itself, the ad-hoc fold, the
/// top-N) groups its input and keys its output. A group is its output PK.
pub(super) struct GroupOutKey {
    key: GroupKey,
    /// The input columns the output carries after its key, as the input locates them.
    carried: Vec<ColumnLocator>,
}

impl GroupOutKey {
    /// `group_cols` over `input`, and the output's leading columns: the key
    /// region, then each `row` column the key region does not spell.
    pub(super) fn new(
        input: &SchemaDescriptor,
        group_cols: &[u32],
        row: impl IntoIterator<Item = u32>,
    ) -> Result<(Self, DerivedSchema), OpBuildErr> {
        let key = GroupKey::new(input, group_cols)?;
        let kind = match key {
            GroupKey::Fold(_) => ReduceOutKey::SyntheticFold,
            _ => ReduceOutKey::Natural,
        };
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
        Ok((GroupOutKey { key, carried }, b))
    }

    /// Grouped by the empty set: one group, V₀.
    #[inline]
    pub(super) fn is_global(&self) -> bool {
        matches!(&self.key, GroupKey::Fold(f) if f.is_empty())
    }

    /// The output PK of `row`'s group.
    #[inline]
    pub(super) fn out_pk<'a>(&self, mb: &'a MemBatch, row: usize) -> OutPk<'a> {
        match &self.key {
            &GroupKey::PkPrefix(w) => OutPk::Borrowed(mb.get_pk_prefix(row, w)),
            &GroupKey::Image(loc) => OutPk::Narrow(NarrowPkOpk::new(loc.opk_image(mb, row), loc.size())),
            GroupKey::Fold(f) => OutPk::Narrow(NarrowPkOpk::new(
                f.key_row(mb, row, mb.get_null_word(row)),
                GROUP_PK_BYTES,
            )),
        }
    }

    /// `body` over `mb`, handed each row's identity: the output PK up to 16 bytes,
    /// widened, else its XXH3-128. The key's form is matched once per call.
    #[inline]
    pub(super) fn with_identity<L: IdentityLoop>(&self, mb: &MemBatch, body: L) -> L::Out {
        match &self.key {
            &GroupKey::Image(loc) => body.run(|r| loc.opk_image(mb, r)),
            GroupKey::Fold(f) => body.run(|r| f.key_row(mb, r, mb.get_null_word(r))),
            &GroupKey::PkPrefix(w) if w <= NARROW_PK_MAX_BYTES => {
                body.run(|r| gnitz_wire::widen_pk_be(mb.get_pk_prefix(r, w)))
            }
            &GroupKey::PkPrefix(w) => body.run(|r| gnitz_wire::checksum_128(mb.get_pk_prefix(r, w))),
        }
    }

    /// `row`'s identity, as [`Self::with_identity`] hands it to a loop.
    #[cfg(test)]
    pub(super) fn identity(&self, mb: &MemBatch, row: usize) -> u128 {
        struct At(usize);
        impl IdentityLoop for At {
            type Out = u128;
            fn run(self, identity: impl Fn(usize) -> u128) -> u128 {
                identity(self.0)
            }
        }
        self.with_identity(mb, At(row))
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
        match &self.key {
            // One run, with no key to compute.
            _ if n <= 1 || self.is_global() => GroupRuns::in_place(n, |_| ()),
            // A consolidated batch is in PK order, so already grouped by any PK prefix.
            &GroupKey::PkPrefix(w) => pk_width_dispatch!(w, |K| {
                let key = |i| K::from_opk(mb.get_pk_prefix(i, w));
                match batch.consolidated_verified(batch.schema()) {
                    true => GroupRuns::in_place(n, key),
                    false => GroupRuns::sorted(n, key),
                }
            }),
            &GroupKey::Image(loc) if loc.size() <= 8 => GroupRuns::sorted(n, |i| loc.opk_image(mb, i) as u64),
            &GroupKey::Image(loc) => GroupRuns::sorted(n, |i| loc.opk_image(mb, i)),
            GroupKey::Fold(f) => GroupRuns::sorted(n, |i| f.key_row(mb, i, mb.get_null_word(i))),
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
    fn sorted<K: Ord>(n: usize, key: impl Fn(usize) -> K) -> Self {
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
