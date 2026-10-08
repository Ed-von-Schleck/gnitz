//! The group key shared by the reduce, the top-N, the ad-hoc fold and the
//! exchange scatter: [`GroupKey`] resolves a group set to the output PK a reduce
//! over it stamps, and the one grouping mechanism built on it — a group is its
//! output PK.

use super::reindex::{locate_key_col, FoldCols, ReindexPacker};
use crate::repr::{Batch, MemBatch};
use crate::schema::key::{pk_width_dispatch, PkSortKey};
use crate::schema::{ColumnLocator, DerivedSchema, ReduceOutKey, SchemaColumn, SchemaDescriptor, TypeCode};
use gnitz_wire::{ReduceOutSlot, NARROW_PK_MAX_BYTES};
use rustc_hash::FxHashMap;
use std::collections::hash_map::Entry;

/// A row's key as the PK bytes its consumer gives it: a group as the output PK a
/// reduce over the group set stamps, a join key as its `_join_pk`.
pub(crate) enum GroupKey {
    /// The `n` PK bytes at `at`: consecutive PK columns' own images.
    PkRange { at: usize, n: usize },
    /// One payload column's OPK image, at that column's width.
    Image(ColumnLocator),
    /// The group's packed key, NULLs marked: at most 16 bytes, held by the
    /// `_group_pk` — also a join key packed off the PK region, at any width.
    Packed(ReindexPacker),
    /// The NULL-distinct XXH3 fold of the group columns: the `_group_pk` of a
    /// group whose own images do not fit one. Over no columns it is V₀.
    Fold(FoldCols),
}

impl GroupKey {
    /// Refused when a group column is out of range or has no key image.
    pub(crate) fn new(schema: &SchemaDescriptor, group_cols: &[u32]) -> Result<Self, String> {
        let locs: Vec<ColumnLocator> = group_cols
            .iter()
            .map(|&c| locate_key_col(schema, c, "group key").map(|(_, loc)| loc))
            .collect::<Result<_, _>>()?;
        Ok(match (schema.reduce_out_key(group_cols), &locs[..]) {
            (ReduceOutKey::Natural, &[loc]) => Self::column(loc),
            (ReduceOutKey::Natural, _) => GroupKey::PkRange { at: 0, n: schema.pk_stride() },
            // A `_group_pk` holds the group itself where its packed key fits one.
            (ReduceOutKey::SyntheticFold, _) => match ReindexPacker::new_group_key(schema, group_cols, &[])?.0 {
                p if p.packs_whole() && (1..=GROUP_PK_BYTES).contains(&p.out_stride) => Self::packed(p),
                _ => GroupKey::Fold(FoldCols::new(locs)),
            },
        })
    }

    /// One column as its own key.
    fn column(loc: ColumnLocator) -> Self {
        match loc {
            ColumnLocator::Pk { byte_off, size, .. } => GroupKey::PkRange { at: byte_off as usize, n: size as usize },
            ColumnLocator::Payload { .. } => GroupKey::Image(loc),
        }
    }

    /// The key `packer` packs, read in place where the batch already spells it.
    pub(crate) fn packed(packer: ReindexPacker) -> Self {
        match (packer.pk_range(), packer.identity_columns().as_deref()) {
            (Some((at, n)), _) => GroupKey::PkRange { at, n },
            (None, Some(&[loc])) => Self::column(loc),
            _ => GroupKey::Packed(packer),
        }
    }

    /// This key as one cell per row of `mb`, where it is 1 to 16 bytes of one region.
    pub(crate) fn cells<'a>(&self, mb: &MemBatch<'a>) -> Option<KeyCells<'a>> {
        let pk = |off: usize, width: usize| KeyCells {
            region: mb.pk(),
            stride: mb.pk_stride(),
            off,
            width,
            opk: true,
            signed: false,
        };
        match *self {
            GroupKey::PkRange { at, n } if (1..=NARROW_PK_MAX_BYTES).contains(&n) => Some(pk(at, n)),
            GroupKey::Image(ColumnLocator::Payload { slot, size, type_code }) => Some(KeyCells {
                region: mb.col_data(slot as usize, size as usize),
                stride: size as usize,
                off: 0,
                width: size as usize,
                opk: false,
                signed: type_code.is_signed_int(),
            }),
            GroupKey::Image(ColumnLocator::Pk { .. }) => unreachable!("a PK column keys as a PK range"),
            GroupKey::PkRange { .. } | GroupKey::Packed(_) | GroupKey::Fold(_) => None,
        }
    }
}

/// A group key of at most 16 bytes, as the cell it is in every row of `region`.
pub(crate) struct KeyCells<'a> {
    pub(crate) region: &'a [u8],
    pub(crate) stride: usize,
    pub(crate) off: usize,
    pub(crate) width: usize,
    /// A PK cell, stored as its image; else a native payload cell, signed or not.
    pub(crate) opk: bool,
    pub(crate) signed: bool,
}

impl KeyCells<'_> {
    /// Row `r`'s cell; `W` is `self.width`.
    #[inline(always)]
    fn cell<const W: usize>(&self, r: usize) -> &[u8; W] {
        let at = r * self.stride + self.off;
        self.region[at..at + W].try_into().unwrap()
    }
}

/// One `W`-byte cell's key image: a PK cell's bytes as they are, a payload cell's
/// native value with its sign bit flipped.
#[inline(always)]
pub(crate) fn cell_image<const W: usize, const OPK: bool>(cell: &[u8; W], signed: bool) -> u128 {
    if OPK {
        return gnitz_wire::widen_pk_be(cell);
    }
    let mut le = [0u8; 16];
    le[..W].copy_from_slice(cell);
    u128::from_le_bytes(le) ^ ((signed as u128) << (8 * W - 1))
}

/// Run `$body` with `$w` the constant `$width`, a [`KeyCells`] width.
macro_rules! for_cell_width {
    ($width:expr, |$w:ident| $body:expr) => {
        $crate::algebra::group_key::for_cell_width!(@arms $width, $w, $body, 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16)
    };
    (@arms $width:expr, $w:ident, $body:expr, $($n:literal)*) => {
        match $width {
            $($n => {
                const $w: usize = $n;
                $body
            })*
            _ => unreachable!("a key cell is 1..=16 bytes"),
        }
    };
}
pub(crate) use for_cell_width;

/// The synthetic `_group_pk` key — the whole PK region of an output whose group
/// set has no natural key.
const GROUP_PK_COL: SchemaColumn = SchemaColumn::new(TypeCode::U128, false);
const GROUP_PK_BYTES: usize = GROUP_PK_COL.size();

/// The `stride`-byte OPK region of a narrow key, from its `u128` image: the
/// inverse of `gnitz_wire::widen_pk_be`.
pub(crate) struct NarrowPkOpk {
    be: [u8; 16],
    stride: usize,
}

impl NarrowPkOpk {
    #[inline(always)]
    pub(crate) fn new(image: u128, stride: usize) -> Self {
        // Static message: `#[inline(always)]` puts this in every per-row caller,
        // and an `Arguments` value costs a stack slot even on a cold panic path.
        assert!(
            stride <= NARROW_PK_MAX_BYTES,
            "NarrowPkOpk::new: stride exceeds NARROW_PK_MAX_BYTES"
        );
        debug_assert!(
            stride == 16 || (image >> (stride * 8)) == 0,
            "narrow PK image {image} does not fit {stride} bytes",
        );
        NarrowPkOpk { be: image.to_be_bytes(), stride }
    }

    /// The `stride` order-preserving bytes — a full PK region for one row.
    #[inline(always)]
    pub(crate) fn bytes(&self) -> &[u8] {
        &self.be[16 - self.stride..]
    }
}

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
    /// The output PK is a hidden `_group_pk`, which the key is widened to.
    synthetic: bool,
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
        let kind = input.reduce_out_key(group_cols);
        let mut b = DerivedSchema::new();
        let mut carried = Vec::new();
        for slot in kind.output_layout(group_cols, row) {
            match slot {
                ReduceOutSlot::SyntheticKey => b.push_pk(GROUP_PK_COL),
                ReduceOutSlot::Key(c) => b.push_pk(input.columns()[c as usize]),
                ReduceOutSlot::Carried(c) => {
                    carried.push(input.locate(c as usize));
                    b.push(input.columns()[c as usize])
                }
            }
        }
        let synthetic = kind == ReduceOutKey::SyntheticFold;
        Ok((GroupOutKey { key, synthetic, carried }, b))
    }

    /// Grouped by the empty set: one group, V₀.
    #[inline]
    pub(crate) fn is_global(&self) -> bool {
        matches!(&self.key, GroupKey::Fold(f) if f.is_empty())
    }

    /// The output PK of `row`'s group.
    #[inline]
    pub(crate) fn out_pk<'a>(&self, mb: &'a MemBatch, row: usize) -> OutPk<'a> {
        let group_pk = |image| OutPk::Narrow(NarrowPkOpk::new(image, GROUP_PK_BYTES));
        match &self.key {
            &GroupKey::PkRange { at, n } if self.synthetic => {
                group_pk(gnitz_wire::widen_pk_be(mb.get_pk_range(row, at, n)))
            }
            &GroupKey::PkRange { at, n } => OutPk::Borrowed(mb.get_pk_range(row, at, n)),
            &GroupKey::Image(loc) => OutPk::Narrow(NarrowPkOpk::new(loc.opk_image(mb, row), loc.size())),
            GroupKey::Packed(p) => group_pk(p.narrow_image(mb, row)),
            GroupKey::Fold(f) => group_pk(f.key_row(mb, row, mb.get_null_word(row))),
        }
    }

    /// `body` over `mb`, handed each row's identity: the output PK up to 16 bytes,
    /// widened, else its XXH3-128. The key's form and width are matched once per call.
    #[inline]
    pub(crate) fn with_identity<L: IdentityLoop>(&self, mb: &MemBatch, body: L) -> L::Out {
        if let Some(cells) = self.key.cells(mb) {
            return for_cell_width!(cells.width, |W| match cells.opk {
                true => body.run(|r| cell_image::<W, true>(cells.cell::<W>(r), cells.signed)),
                false => body.run(|r| cell_image::<W, false>(cells.cell::<W>(r), cells.signed)),
            });
        }
        match &self.key {
            GroupKey::Fold(f) => body.run(|r| f.key_row(mb, r, mb.get_null_word(r))),
            GroupKey::Packed(p) => packed_identity(p, mb, body),
            &GroupKey::PkRange { at, n } => body.run(|r| gnitz_wire::checksum_128(mb.get_pk_range(r, at, n))),
            GroupKey::Image(_) => unreachable!("an image key is one cell"),
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
    pub(crate) fn runs(&self, batch: &Batch) -> Option<Vec<(usize, usize)>> {
        let mb = &batch.as_mem_batch();
        let n = mb.count;
        match &self.key {
            // One run, with no key to compute.
            _ if n <= 1 || self.is_global() => Some(equal_runs(n, |_| ())),
            // A consolidated batch is in PK order, so already grouped by any PK prefix.
            &GroupKey::PkRange { at: 0, n: w } if batch.is_consolidated() => Some(pk_width_dispatch!(w, |K| {
                equal_runs(n, |i| K::from_opk(mb.get_pk_range(i, 0, w)))
            })),
            _ => None,
        }
    }

    /// The groups of a batch [`Self::runs`] does not answer for, as one ordinal
    /// per row: its rows hashed into groups while those are few for the rows,
    /// else sorted.
    pub(crate) fn numbered(&self, batch: &Batch) -> GroupOrdinals {
        let mb = &batch.as_mem_batch();
        let n = mb.count;
        // A wide key's identity is a digest, and a sort compares the bytes.
        // Nor do too few rows pay for a hash table.
        let hashes =
            n >= HASH_MIN_ROWS && !matches!(self.key, GroupKey::PkRange { n: w, .. } if w > NARROW_PK_MAX_BYTES);
        if let Some(groups) = hashes.then(|| self.with_identity(mb, Hashed { n })).flatten() {
            return groups;
        }
        match &self.key {
            &GroupKey::PkRange { at, n: w } => {
                pk_width_dispatch!(w, |K| GroupOrdinals::sorted(n, |i| K::from_opk(
                    mb.get_pk_range(i, at, w)
                )))
            }
            &GroupKey::Image(loc) if loc.size() <= 8 => GroupOrdinals::sorted(n, |i| loc.opk_image(mb, i) as u64),
            &GroupKey::Image(loc) => GroupOrdinals::sorted(n, |i| loc.opk_image(mb, i)),
            GroupKey::Packed(p) => {
                let (keys, w) = (p.keys(mb), p.out_stride);
                GroupOrdinals::sorted(n, |i| gnitz_wire::widen_pk_be(&keys[i * w..(i + 1) * w]))
            }
            GroupKey::Fold(f) => GroupOrdinals::sorted(n, |i| f.key_row(mb, i, mb.get_null_word(i))),
        }
    }
}

/// [`GroupOutKey::with_identity`] over a packed `_group_pk`: the keys packed a
/// column at a time, once for the whole loop. Out of line, so the loops that
/// read their key in place compile as they do without it.
#[inline(never)]
fn packed_identity<L: IdentityLoop>(p: &ReindexPacker, mb: &MemBatch, body: L) -> L::Out {
    let (keys, w) = (p.keys(mb), p.out_stride);
    body.run(|r| gnitz_wire::widen_pk_be(&keys[r * w..(r + 1) * w]))
}

/// The runs of equal adjacent keys over rows `0..n`, each as `[start, end)`.
fn equal_runs<K: PartialEq>(n: usize, key: impl Fn(usize) -> K) -> Vec<(usize, usize)> {
    let mut runs = Vec::new();
    if n == 0 {
        return runs;
    }
    let (mut start, mut prev) = (0, key(0));
    for i in 1..n {
        let k = key(i);
        if k != prev {
            runs.push((start, i));
            start = i;
        }
        prev = k;
    }
    runs.push((start, n));
    runs
}

/// Fewest rows per group at which hashing a batch's rows into groups costs
/// less than sorting them.
const HASH_MIN_ROWS_PER_GROUP: usize = 4;

/// Fewest rows at which hashing them costs less than sorting them, however few
/// their groups.
const HASH_MIN_ROWS: usize = 16;

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
    pub(crate) fn of_runs(runs: &[(usize, usize)]) -> Self {
        let mut ord = Vec::with_capacity(runs.last().map_or(0, |&(_, end)| end));
        let mut first = Vec::with_capacity(runs.len());
        for (g, &(start, end)) in runs.iter().enumerate() {
            first.push(start as u32);
            ord.extend(std::iter::repeat_n(g as u32, end - start));
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

#[cfg(test)]
#[path = "benches/group_key.rs"]
mod bench;
