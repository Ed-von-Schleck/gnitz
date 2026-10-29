//! The key a row is grouped or routed by. [`GroupKey`] resolves a group set, a
//! join key or a placement prefix to the PK bytes its consumer keys by: the
//! exchange scatter routes by it, and [`GroupOutKey`] groups and keys the rows of
//! an operator keyed like a reduce (the reduce, the ad-hoc fold, the top-N and
//! the window) by it — a group is its output PK.

use super::key_pack::{locate_key_col, pk_run, FoldCols, KeyPacker};
use crate::repr::{Batch, MemBatch};
use crate::schema::key::{pk_width_dispatch, PkSortKey};
use crate::schema::{DerivedSchema, SchemaColumn, SchemaDescriptor};
use gnitz_expr::{ColumnLocator, ReduceOutKey, ReduceOutSlot};
use gnitz_wire::{TypeCode, NARROW_PK_MAX_BYTES};
use rustc_hash::FxHashMap;
use std::collections::hash_map::Entry;

/// A row's key as the PK bytes its consumer gives it: a group as the output PK a
/// reduce over the group set stamps, a join key as its `_join_pk`.
pub(crate) enum GroupKey {
    /// The `n` PK bytes at `at`: consecutive PK columns' own images.
    PkRange { at: usize, n: usize },
    /// The OPK image of the payload column in `slot`, at that column's width.
    Image { slot: u8, size: u8, type_code: TypeCode },
    /// The group's packed key, NULLs marked: at most 16 bytes, held by the
    /// `_group_pk` — also a join key packed off the PK region, at any width.
    Packed(KeyPacker),
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
        Ok(match schema.reduce_out_key(group_cols) {
            // The group's own columns: one PK run, or one payload column.
            ReduceOutKey::Natural => Self::in_place(&locs).expect("a natural group key reads in place"),
            // A `_group_pk` holds the group itself where its packed key fits one.
            ReduceOutKey::SyntheticFold => match KeyPacker::new_group_key(schema, group_cols, &[])?.0 {
                // A folded column alone fills the 16 bytes, so a packer that fits folds none.
                p if (1..=GROUP_PK_BYTES).contains(&p.out_stride) => Self::packed(p),
                _ => GroupKey::Fold(FoldCols::new(locs)),
            },
        })
    }

    /// The key `packer` packs, read in place where the batch already spells it.
    pub(crate) fn packed(packer: KeyPacker) -> Self {
        match packer.identity_columns().and_then(|locs| Self::in_place(&locs)) {
            Some(key) => key,
            None => GroupKey::Packed(packer),
        }
    }

    /// `locs`' key read where a batch already spells it: consecutive PK columns,
    /// or one payload column (a PK column is a run of its own).
    fn in_place(locs: &[ColumnLocator]) -> Option<Self> {
        match (pk_run(locs), locs) {
            (Some((at, n)), _) => Some(GroupKey::PkRange { at, n }),
            (None, &[ColumnLocator::Payload { slot, size, type_code }]) => {
                Some(GroupKey::Image { slot, size, type_code })
            }
            _ => None,
        }
    }

    /// This key resolved against the rows of `mb`.
    #[inline(always)]
    pub(crate) fn form<'k, 'a>(&'k self, mb: &MemBatch<'a>) -> KeyForm<'k, 'a> {
        match *self {
            GroupKey::PkRange { at, n } if n <= NARROW_PK_MAX_BYTES => KeyForm::Cells(KeyCells {
                region: mb.pk(),
                stride: mb.pk_stride(),
                off: at,
                width: n,
                opk: true,
                signed: false,
            }),
            GroupKey::PkRange { at, n } => KeyForm::WidePk { at, n },
            GroupKey::Image { slot, size, type_code } => KeyForm::Cells(KeyCells {
                region: mb.col_data(slot as usize),
                stride: size as usize,
                off: 0,
                width: size as usize,
                opk: false,
                signed: type_code.is_signed_int(),
            }),
            GroupKey::Packed(ref p) => KeyForm::Packed(p),
            GroupKey::Fold(ref f) => KeyForm::Fold(f),
        }
    }
}

/// A [`GroupKey`] as one batch's rows hold it.
pub(crate) enum KeyForm<'k, 'a> {
    /// One cell of 1 to 16 bytes per row.
    Cells(KeyCells<'a>),
    /// A PK range wider than a cell.
    WidePk {
        at: usize,
        n: usize,
    },
    Packed(&'k KeyPacker),
    Fold(&'k FoldCols),
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
    match OPK {
        true => gnitz_wire::widen_pk_be(cell),
        false => gnitz_wire::native_key_image(cell, signed),
    }
}

/// Run `$body` with `$w` the constant width of [`KeyCells`] `$cells` and `$opk`
/// its constant `opk`.
macro_rules! for_key_cells {
    ($cells:ident, |$w:ident, $opk:ident| $body:expr) => {
        $crate::algebra::group_key::for_key_cells!(@arms $cells, $w, $opk, $body, 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16)
    };
    (@arms $cells:ident, $w:ident, $opk:ident, $body:expr, $($n:literal)*) => {
        match $cells.width {
            $($n => {
                const $w: usize = $n;
                match $cells.opk {
                    true => {
                        const $opk: bool = true;
                        $body
                    }
                    false => {
                        const $opk: bool = false;
                        $body
                    }
                }
            })*
            _ => unreachable!("a key cell is 1..=16 bytes"),
        }
    };
}
pub(crate) use for_key_cells;

/// The synthetic `_group_pk` key — the whole PK region of an output whose group
/// set has no natural key.
const GROUP_PK_COL: SchemaColumn = SchemaColumn::new(TypeCode::U128, false);
const GROUP_PK_BYTES: usize = GROUP_PK_COL.size();

/// One row's group output PK: borrowed out of the batch, or held inline.
pub(crate) enum OutPk<'a> {
    Borrowed(&'a [u8]),
    /// A narrow key's big-endian image, its OPK bytes from `at`.
    Inline {
        be: [u8; 16],
        at: usize,
    },
}

impl OutPk<'_> {
    /// The `width`-byte OPK region of a narrow key, from its `u128` image: the
    /// inverse of `gnitz_wire::widen_pk_be`.
    #[inline(always)]
    fn inline(image: u128, width: usize) -> Self {
        debug_assert!(
            width == 16 || (image >> (width * 8)) == 0,
            "narrow PK image {image} does not fit {width} bytes",
        );
        OutPk::Inline { be: image.to_be_bytes(), at: 16 - width }
    }

    /// The `pk_stride` OPK bytes of the key.
    #[inline]
    pub(crate) fn bytes(&self) -> &[u8] {
        match self {
            OutPk::Borrowed(b) => b,
            OutPk::Inline { be, at } => &be[*at..],
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
pub(crate) fn ground_pk() -> [u8; GROUP_PK_BYTES] {
    gnitz_wire::global_group_key().to_be_bytes()
}

/// How an operator keyed like a reduce (the reduce itself, the ad-hoc fold, the
/// top-N, the window) groups its input and keys its output. A group is its output PK.
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
        let mut b = DerivedSchema::new();
        let mut carried = Vec::new();
        for slot in input.reduce_out_layout(group_cols, row) {
            match slot {
                ReduceOutSlot::SyntheticKey => b.push_pk(GROUP_PK_COL),
                ReduceOutSlot::Key(c) => b.push_pk(input.columns()[c as usize]),
                ReduceOutSlot::Carried(c) => {
                    carried.push(input.locate(c as usize));
                    b.push(input.columns()[c as usize])
                }
            }
        }
        let synthetic = input.reduce_out_key(group_cols) == ReduceOutKey::SyntheticFold;
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
        let group_pk = |image| OutPk::inline(image, GROUP_PK_BYTES);
        match &self.key {
            &GroupKey::PkRange { at, n } if self.synthetic => {
                group_pk(gnitz_wire::widen_pk_be(mb.get_pk_range(row, at, n)))
            }
            &GroupKey::PkRange { at, n } => OutPk::Borrowed(mb.get_pk_range(row, at, n)),
            &GroupKey::Image { slot, size, type_code } => OutPk::inline(
                ColumnLocator::Payload { slot, size, type_code }.opk_image(mb, row),
                size as usize,
            ),
            GroupKey::Packed(p) => group_pk(p.narrow_image(mb, row)),
            GroupKey::Fold(f) => group_pk(f.key_row(mb, row)),
        }
    }

    /// `body` over `mb`, handed the identity of each row of `rows`: the output PK
    /// up to 16 bytes, widened, else its XXH3-128. The key's form and width are
    /// matched once per call.
    #[inline]
    pub(crate) fn with_identity<L: IdentityLoop>(&self, mb: &MemBatch, rows: &[(usize, usize)], body: L) -> L::Out {
        match self.key.form(mb) {
            KeyForm::Cells(cells) => {
                for_key_cells!(cells, |W, OPK| body
                    .run(|r| cell_image::<W, OPK>(cells.cell::<W>(r), cells.signed)))
            }
            KeyForm::Fold(f) => body.run(|r| f.key_row(mb, r)),
            KeyForm::Packed(p) => packed_identity(p, mb, rows, body),
            KeyForm::WidePk { at, n } => body.run(|r| gnitz_wire::checksum_128(mb.get_pk_range(r, at, n))),
        }
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
        let n = mb.len();
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
        let n = mb.len();
        // Too few rows do not pay for a hash table. A wide PK's identity is a
        // digest, which does not sort as the PK.
        let hashable = n >= HASH_MIN_ROWS && !matches!(self.key.form(mb), KeyForm::WidePk { .. });
        if let Some(groups) = hashable
            .then(|| self.with_identity(mb, &[(0, n)], Hashed { n }))
            .flatten()
        {
            return groups;
        }
        match &self.key {
            &GroupKey::PkRange { at, n: w } => {
                pk_width_dispatch!(w, |K| GroupOrdinals::sorted(n, |i| K::from_opk(
                    mb.get_pk_range(i, at, w)
                )))
            }
            &GroupKey::Image { slot, size, type_code } => {
                let loc = ColumnLocator::Payload { slot, size, type_code };
                match size <= 8 {
                    true => GroupOrdinals::sorted(n, |i| loc.opk_image(mb, i) as u64),
                    false => GroupOrdinals::sorted(n, |i| loc.opk_image(mb, i)),
                }
            }
            GroupKey::Packed(p) => {
                let (keys, w) = (p.keys(mb), p.out_stride);
                GroupOrdinals::sorted(n, |i| gnitz_wire::widen_pk_be(&keys[i * w..(i + 1) * w]))
            }
            GroupKey::Fold(f) => GroupOrdinals::sorted(n, |i| f.key_row(mb, i)),
        }
    }
}

/// [`GroupOutKey::with_identity`] over a packed `_group_pk`: every row packed once
/// for the whole loop where `rows` are all of them, or where the key packs a
/// column at a time and `rows` hold more than one row in
/// `PACK_ROWS_MIN_SPARSITY`; else each of `rows` packs its own key as it is read.
/// Out of line, so the loops that read their key in place compile as they do
/// without it.
#[inline(never)]
fn packed_identity<L: IdentityLoop>(p: &KeyPacker, mb: &MemBatch, rows: &[(usize, usize)], body: L) -> L::Out {
    let few = |k: usize| k * PACK_ROWS_MIN_SPARSITY <= mb.len();
    let every = rows == [(0, mb.len())];
    if every || p.packs_by_column() && !(few(rows.len()) && few(crate::repr::range_rows(rows))) {
        let (keys, w) = (p.keys(mb), p.out_stride);
        return body.run(|r| gnitz_wire::widen_pk_be(&keys[r * w..(r + 1) * w]));
    }
    body.run(|r| p.narrow_image(mb, r))
}

/// Fewest rows per row read at which [`packed_identity`] packs a column-packed
/// key a row at a time, rather than every row.
const PACK_ROWS_MIN_SPARSITY: usize = 16;

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
