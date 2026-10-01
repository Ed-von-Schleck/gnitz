//! The group key shared by the reduce, the top-N, the ad-hoc fold and the
//! exchange scatter: [`GroupKey`] resolves a group set to the output PK a reduce
//! over it stamps, and the one grouping mechanism built on it — a group is its
//! output PK.

use std::ops::Range;

use super::reindex::{locate_key_col, FoldCols};
use crate::repr::{Batch, MemBatch};
use crate::schema::key::{pk_width_dispatch, NarrowPkOpk, PkSortKey};
use crate::schema::{
    ColumnLocator, DerivedSchema, ReduceOutKey, SchemaColumn, SchemaDescriptor, SchemaFacts, TypeCode,
};
use gnitz_wire::{ReduceOutSlot, NARROW_PK_MAX_BYTES};
use rustc_hash::FxHashMap;
use std::collections::hash_map::Entry;

/// A row's group key, as the output PK a reduce over the group set stamps.
pub(crate) enum GroupKey {
    /// The leading `n` PK bytes.
    PkPrefix(usize),
    /// One column's OPK image, at that column's width.
    Image(ColumnLocator),
    /// The NULL-distinct XXH3 fold of the group columns, as the 16-byte
    /// `_group_pk`. Over no columns it is V₀.
    Fold(FoldCols),
}

impl GroupKey {
    /// Refused when a group column is out of range or has no key image.
    pub(crate) fn new(schema: &SchemaDescriptor, group_cols: &[u32]) -> Result<Self, String> {
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
pub(crate) enum OutPk<'a> {
    Borrowed(&'a [u8]),
    Narrow(NarrowPkOpk),
}

impl OutPk<'_> {
    /// The `pk_stride` OPK bytes of the key.
    #[inline]
    pub(crate) fn bytes(&self) -> &[u8] {
        match self {
            OutPk::Borrowed(b) => b,
            OutPk::Narrow(k) => k.bytes(),
        }
    }
}

/// A row loop keyed by group identity; see [`GroupOutKey::with_identity`].
pub(crate) trait IdentityLoop {
    type Out;
    fn run(self, identity: impl Fn(usize) -> u128) -> Self::Out;
}

/// V₀, the output PK of the empty group set's one group.
#[inline]
pub(crate) fn ground_pk() -> NarrowPkOpk {
    NarrowPkOpk::new(gnitz_wire::global_group_key(), GROUP_PK_BYTES)
}

/// How an operator keyed like a reduce (the reduce itself, the ad-hoc fold, the
/// top-N) groups its input and keys its output. A group is its output PK.
pub(crate) struct GroupOutKey {
    key: GroupKey,
    /// The input columns the output carries after its key, as the input locates them.
    carried: Vec<ColumnLocator>,
}

impl GroupOutKey {
    /// `group_cols` over `input`, and the output's leading columns: the key
    /// region, then each `row` column the key region does not spell.
    pub(crate) fn new(
        input: &SchemaDescriptor,
        group_cols: &[u32],
        row: impl IntoIterator<Item = u32>,
    ) -> Result<(Self, DerivedSchema), String> {
        let key = GroupKey::new(input, group_cols)?;
        let kind = match key {
            GroupKey::Fold(_) => ReduceOutKey::SyntheticFold,
            _ => ReduceOutKey::Natural,
        };
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
        }
        Ok((GroupOutKey { key, carried }, b))
    }

    /// Grouped by the empty set: one group, V₀.
    #[inline]
    pub(crate) fn is_global(&self) -> bool {
        matches!(&self.key, GroupKey::Fold(f) if f.is_empty())
    }

    /// The output PK of `row`'s group.
    #[inline]
    pub(crate) fn out_pk<'a>(&self, mb: &'a MemBatch, row: usize) -> OutPk<'a> {
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
    pub(crate) fn with_identity<L: IdentityLoop>(&self, mb: &MemBatch, body: L) -> L::Out {
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
    pub(crate) fn identity(&self, mb: &MemBatch, row: usize) -> u128 {
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
    pub(crate) fn carried(&self) -> &[ColumnLocator] {
        &self.carried
    }

    /// `batch`'s groups as row runs, where its rows already stand in ascending
    /// output-PK order; `None` where they need not.
    pub(crate) fn runs(&self, batch: &Batch) -> Option<GroupRuns> {
        let mb = &batch.as_mem_batch();
        let n = mb.count;
        match &self.key {
            // One run, with no key to compute.
            _ if n <= 1 || self.is_global() => Some(GroupRuns::of(n, |_| ())),
            // A consolidated batch is in PK order, so already grouped by any PK prefix.
            &GroupKey::PkPrefix(w) if batch.consolidated_verified() => Some(pk_width_dispatch!(w, |K| {
                GroupRuns::of(n, |i| K::from_opk(mb.get_pk_prefix(i, w)))
            })),
            _ => None,
        }
    }

    /// `batch`'s groups as one ordinal per row.
    pub(crate) fn ordinals(&self, batch: &Batch) -> GroupOrdinals {
        match self.runs(batch) {
            Some(runs) => GroupOrdinals::of_runs(&runs),
            None => self.numbered(batch),
        }
    }

    /// [`Self::ordinals`] of a batch [`Self::runs`] does not answer for: its
    /// rows hashed into groups while those are few for the rows, else sorted.
    pub(crate) fn numbered(&self, batch: &Batch) -> GroupOrdinals {
        let mb = &batch.as_mem_batch();
        let n = mb.count;
        // A wide prefix's identity is a digest, and a sort compares the bytes.
        let hashes = !matches!(self.key, GroupKey::PkPrefix(w) if w > NARROW_PK_MAX_BYTES);
        if let Some(groups) = hashes.then(|| self.with_identity(mb, Hashed { n })).flatten() {
            return groups;
        }
        match &self.key {
            &GroupKey::PkPrefix(w) => {
                pk_width_dispatch!(w, |K| GroupOrdinals::sorted(n, |i| K::from_opk(mb.get_pk_prefix(i, w))))
            }
            &GroupKey::Image(loc) if loc.size() <= 8 => GroupOrdinals::sorted(n, |i| loc.opk_image(mb, i) as u64),
            &GroupKey::Image(loc) => GroupOrdinals::sorted(n, |i| loc.opk_image(mb, i)),
            GroupKey::Fold(f) => GroupOrdinals::sorted(n, |i| f.key_row(mb, i, mb.get_null_word(i))),
        }
    }
}

/// A batch's groups as contiguous row runs, in ascending output-PK order.
pub(crate) struct GroupRuns {
    /// Each run's exclusive end row, ascending; the last is the row count.
    ends: Vec<u32>,
}

impl GroupRuns {
    /// The runs of equal adjacent keys over rows `0..n`.
    fn of<K: PartialEq>(n: usize, key: impl Fn(usize) -> K) -> Self {
        let mut ends = Vec::new();
        if n == 0 {
            return GroupRuns { ends };
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
        GroupRuns { ends }
    }

    /// The group count.
    #[inline]
    pub(crate) fn len(&self) -> usize {
        self.ends.len()
    }

    /// Each group's row range, ascending.
    pub(crate) fn iter(&self) -> impl Iterator<Item = Range<usize>> + '_ {
        let starts = std::iter::once(0).chain(self.ends.iter().map(|&e| e as usize));
        starts.zip(self.ends.iter().map(|&e| e as usize)).map(|(s, e)| s..e)
    }
}

/// Fewest rows per group at which hashing a batch's rows into groups costs
/// less than sorting them.
const HASH_MIN_ROWS_PER_GROUP: usize = 4;

/// A batch's groups as one ordinal per row: no row moves, and a fold over the
/// groups is one pass per column.
pub(crate) struct GroupOrdinals {
    /// Row → group ordinal.
    pub(crate) ord: Vec<u32>,
    /// Group ordinal → its first row.
    pub(crate) first: Vec<u32>,
    /// The group ordinals in ascending output-PK order.
    pub(crate) by_pk: Vec<u32>,
}

impl GroupOrdinals {
    /// The group count.
    #[inline]
    pub(crate) fn len(&self) -> usize {
        self.first.len()
    }

    /// Run `g` is group `g`.
    pub(crate) fn of_runs(runs: &GroupRuns) -> Self {
        let mut ord = Vec::with_capacity(runs.ends.last().map_or(0, |&e| e as usize));
        let mut first = Vec::with_capacity(runs.len());
        for (g, run) in runs.iter().enumerate() {
            first.push(run.start as u32);
            ord.extend(std::iter::repeat_n(g as u32, run.len()));
        }
        GroupOrdinals {
            ord,
            by_pk: (0..first.len() as u32).collect(),
            first,
        }
    }

    /// Groups numbered in ascending `key` order, found by sorting the rows.
    fn sorted<K: Ord>(n: usize, key: impl Fn(usize) -> K) -> Self {
        // The row index breaks ties, so a group's first row is its first in the batch.
        let mut pairs: Vec<(K, u32)> = (0..n).map(|i| (key(i), i as u32)).collect();
        pairs.sort_unstable();
        let mut ord = vec![0u32; n];
        let mut first: Vec<u32> = Vec::new();
        for (p, (k, row)) in pairs.iter().enumerate() {
            if p == 0 || *k != pairs[p - 1].0 {
                first.push(*row);
            }
            ord[*row as usize] = (first.len() - 1) as u32;
        }
        GroupOrdinals {
            ord,
            by_pk: (0..first.len() as u32).collect(),
            first,
        }
    }
}

/// Group identities numbered in the order first met.
#[derive(Default)]
pub(crate) struct GroupNumbers {
    by_identity: FxHashMap<u128, u32>,
    /// The previous row's `(identity, ordinal)`, so a run of one group skips the map.
    last: Option<(u128, u32)>,
}

impl GroupNumbers {
    /// `identity`'s ordinal, which `new` mints where it is first met.
    #[inline(always)]
    pub(crate) fn ordinal<E>(&mut self, identity: u128, new: impl FnOnce() -> Result<u32, E>) -> Result<u32, E> {
        if let Some((_, g)) = self.last.filter(|&(k, _)| k == identity) {
            return Ok(g);
        }
        let g = match self.by_identity.entry(identity) {
            Entry::Occupied(e) => *e.get(),
            Entry::Vacant(e) => *e.insert(new()?),
        };
        self.last = Some((identity, g));
        Ok(g)
    }
}

/// Groups numbered in discovery order, found by hashing each row's identity.
struct Hashed {
    n: usize,
}

impl IdentityLoop for Hashed {
    /// `None` once the batch holds more groups than
    /// [`HASH_MIN_ROWS_PER_GROUP`] admits for its rows.
    type Out = Option<GroupOrdinals>;

    fn run(self, identity: impl Fn(usize) -> u128) -> Self::Out {
        let limit = self.n / HASH_MIN_ROWS_PER_GROUP;
        let mut numbers = GroupNumbers::default();
        let mut ord: Vec<u32> = Vec::with_capacity(self.n);
        let mut first: Vec<u32> = Vec::new();
        for row in 0..self.n {
            let new = || {
                let g = first.len();
                first.push(row as u32);
                (g < limit).then_some(g as u32).ok_or(())
            };
            ord.push(numbers.ordinal(identity(row), new).ok()?);
        }
        // Every hashed identity is its output PK read as an integer.
        // Taken in discovery order, which is often PK order already.
        let mut by_pk: Vec<(u128, u32)> = first
            .iter()
            .zip(0..)
            .map(|(&row, g)| (identity(row as usize), g))
            .collect();
        by_pk.sort_unstable();
        Some(GroupOrdinals {
            ord,
            first,
            by_pk: by_pk.into_iter().map(|(_, g)| g).collect(),
        })
    }
}

#[cfg(test)]
#[path = "tests/group_key.rs"]
mod tests;
