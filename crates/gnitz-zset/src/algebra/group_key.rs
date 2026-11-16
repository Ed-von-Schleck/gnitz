//! The key a row is grouped or routed by. [`GroupKey`] resolves a group set, a
//! join key or a placement prefix to the PK bytes its consumer keys by: the
//! exchange scatter routes by it, and [`GroupOutKey`] groups and keys the rows of
//! an operator keyed like a reduce (the reduce, the ad-hoc fold, the top-N and
//! the window) by it — a group is its output PK. Because the scatter and the
//! reduce resolve a group set through the one [`GroupKey::new`], a reduce's
//! input is co-partitioned by its output PK.
//!
//! It also numbers a batch's groups for the reduce and the ad-hoc fold:
//! [`GroupOutKey::runs_into`] where a consolidated batch already stands in key
//! order, else [`GroupOutKey::numbered_into`] — one ordinal per row, hashed
//! while the groups are few for the rows, else sorted.

use super::key_pack::{FOLD_CHUNK_MIN_ROWS, KeyPacker, locate_key_col, pk_run};
use super::zeroed;
use crate::repr::{Batch, MemBatch, vec_bytes};
use crate::schema::{DerivedSchema, PkSortKey, SchemaColumn, SchemaDescriptor, pk_width_dispatch};
use gnitz_expr::{ColumnLocator, ReduceOutKey, ReduceOutSlot};
use gnitz_wire::{NARROW_PK_MAX_BYTES, TypeCode};
use rustc_hash::FxHashMap;
use smallvec::SmallVec;
use std::collections::hash_map::Entry;

/// A row's key as the PK bytes its consumer gives it: a group as the output PK a
/// reduce over the group set stamps, a join key as its `_join_pk`.
pub(crate) enum GroupKey {
    /// The `n` PK bytes at `at`: consecutive PK columns' own images.
    PkRange { at: usize, n: usize },
    /// The OPK image of the payload column in `slot`, at that column's width.
    Image { slot: u8, size: u8, type_code: TypeCode },
    /// The group's packed key, NULLs marked: at most 16 bytes, held by the
    /// `_group_pk` — also a join key packed off the PK region, at any width. A
    /// group whose own images do not fit one packs to the NULL-distinct XXH3
    /// fold of its columns alone.
    Packed(KeyPacker),
    /// The empty group set's one group, V₀: no column is read.
    Global,
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
            ReduceOutKey::SyntheticFold if locs.is_empty() => GroupKey::Global,
            ReduceOutKey::SyntheticFold => match KeyPacker::new_group_key(schema, group_cols, &[])?.0 {
                // A folded column alone fills the 16 bytes, so a packer that fits folds none.
                p if (1..=GROUP_PK_BYTES).contains(&p.out_stride) => Self::packed(p),
                _ => GroupKey::Packed(KeyPacker::fold_only(locs)),
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
            GroupKey::Global => KeyForm::Global,
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
    /// The one group, V₀.
    Global,
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
/// its constant `opk`: a PK cell at any width, a payload cell at a column's.
macro_rules! for_key_cells {
    ($cells:ident, |$w:ident, $opk:ident| $body:expr) => {
        $crate::algebra::group_key::for_key_cells!(
            @arms $cells, $w, $opk, $body, 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16
        )
    };
    (@arms $cells:ident, $w:ident, $opk:ident, $body:expr, $($n:literal)*) => {
        if $cells.opk {
            const $opk: bool = true;
            match $cells.width {
                $($n => {
                    const $w: usize = $n;
                    $body
                })*
                _ => unreachable!("a PK key cell is 1..=16 bytes"),
            }
        } else {
            const $opk: bool = false;
            gnitz_wire::by_cell_width!($cells.width, |T| {
                const $w: usize = size_of::<T>();
                $body
            })
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
        matches!(self.key, GroupKey::Global)
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
            GroupKey::Global => group_pk(gnitz_wire::global_group_key()),
        }
    }

    /// [`Self::out_pk`], built from `id` — the group's [`Self::with_identity`]
    /// identity — where one is given and the key's output PK is that identity;
    /// a PK range is borrowed or widened from `row` whatever `id` holds.
    #[inline]
    pub(crate) fn out_pk_with<'a>(&self, mb: &'a MemBatch, row: usize, id: Option<u128>) -> OutPk<'a> {
        match (&self.key, id) {
            (GroupKey::Packed(_) | GroupKey::Global, Some(id)) => OutPk::inline(id, GROUP_PK_BYTES),
            (&GroupKey::Image { size, .. }, Some(id)) => OutPk::inline(id, size as usize),
            _ => self.out_pk(mb, row),
        }
    }

    /// `body` over `mb`, handed the identity of each row of `rows`: the output PK
    /// up to 16 bytes, widened, else its XXH3-128 — so for every key but a PK
    /// range wider than 16 bytes, [`Self::out_pk_with`] rebuilds the output PK
    /// from it. The key's form and width are matched once per call.
    #[inline]
    pub(super) fn with_identity<L: IdentityLoop>(&self, mb: &MemBatch, rows: &[(usize, usize)], body: L) -> L::Out {
        match self.key.form(mb) {
            KeyForm::Cells(cells) => {
                for_key_cells!(cells, |W, OPK| body
                    .run(|r| cell_image::<W, OPK>(cells.cell::<W>(r), cells.signed)))
            }
            KeyForm::Global => body.run(|_| gnitz_wire::global_group_key()),
            KeyForm::Packed(p) => packed_identity(p, mb, rows, body),
            KeyForm::WidePk { at, n } => body.run(|r| gnitz_wire::checksum_128(mb.get_pk_range(r, at, n))),
        }
    }

    /// The input columns the output carries after its key.
    #[inline(always)]
    pub(crate) fn carried(&self) -> &[ColumnLocator] {
        &self.carried
    }

    /// `batch`'s groups as row runs into `out`, where its rows already stand in
    /// ascending output-PK order; false, `out` unspecified, where they need not.
    pub(crate) fn runs_into(&self, batch: &Batch, out: &mut Vec<(usize, usize)>) -> bool {
        let mb = &batch.as_mem_batch();
        let n = mb.len();
        match &self.key {
            // One run, with no key to compute.
            _ if n <= 1 || self.is_global() => equal_runs(n, |_| (), out),
            // A consolidated batch is in PK order, so already grouped by any PK prefix.
            &GroupKey::PkRange { at: 0, n: w } if batch.is_consolidated() => {
                pk_width_dispatch!(w, |K| equal_runs(n, |i| K::from_opk(mb.get_pk_range(i, 0, w)), out))
            }
            _ => return false,
        }
        true
    }

    /// The groups of a batch [`Self::runs_into`] does not answer for, as one
    /// ordinal per row, into `s.groups`, whatever `s` held: its rows hashed into
    /// groups while those are few for the rows, else sorted.
    pub(crate) fn numbered_into(&self, batch: &Batch, s: &mut GroupScratch) {
        let mb = &batch.as_mem_batch();
        let n = mb.len();
        // Too few rows do not pay for a hash table. A wide PK's identity is a
        // digest, which does not sort as the PK.
        let hashable = n >= HASH_MIN_ROWS && !matches!(self.key.form(mb), KeyForm::WidePk { .. });
        let GroupScratch { groups, numbers, identities } = s;
        match &self.key {
            GroupKey::Packed(p) => {
                // Every row packed once, for the hash and for the sort it may fall back to.
                identities.clear();
                identities.resize(n, 0);
                p.identities_over(mb, &[(0, n)], identities);
                let key = |i: usize| identities[i];
                if !(hashable && Hashed { n, groups: &mut *groups, numbers }.run(key)) {
                    groups.fill_sorted(n, key, Some);
                }
            }
            GroupKey::Global => unreachable!("runs_into answers for the one group"),
            _ if hashable && self.with_identity(mb, &[(0, n)], Hashed { n, groups: &mut *groups, numbers }) => {}
            &GroupKey::PkRange { at, n: w } => {
                pk_width_dispatch!(w, |K| groups.fill_sorted(
                    n,
                    |i| K::from_opk(mb.get_pk_range(i, at, w)),
                    |_| None
                ))
            }
            &GroupKey::Image { slot, size, type_code } => {
                // No identities kept: reading an image again per group costs a
                // batch too small to hash less than allocating them does.
                let loc = ColumnLocator::Payload { slot, size, type_code };
                match size <= 8 {
                    true => groups.fill_sorted(n, |i| loc.opk_image(mb, i) as u64, |_| None),
                    false => groups.fill_sorted(n, |i| loc.opk_image(mb, i), |_| None),
                }
            }
        }
    }
}

/// The buffers [`GroupOutKey::numbered_into`] numbers a batch's groups with,
/// kept by a caller that numbers batch after batch.
#[derive(Default)]
pub(crate) struct GroupScratch {
    /// The last numbering.
    pub(crate) groups: GroupOrdinals,
    numbers: GroupNumbers,
    /// Every row's packed key as its identity, for a packed group key.
    identities: Vec<u128>,
}

impl GroupScratch {
    /// The bytes this holds allocated.
    pub(crate) fn held_bytes(&self) -> usize {
        let GroupOrdinals { ord, first, by_pk, ids } = &self.groups;
        let map = self.numbers.by_identity.capacity() * size_of::<(u128, u32)>();
        vec_bytes(ord) + vec_bytes(first) + vec_bytes(by_pk) + vec_bytes(ids) + vec_bytes(&self.identities) + map
    }
}

/// [`GroupOutKey::with_identity`] over a packed `_group_pk`: every row packed once
/// for the whole loop where `rows` are all of them, or where no slot of the key
/// is hashed and `rows` hold more than one row in `PACK_ROWS_MIN_SPARSITY`; the
/// rows of `rows` packed a run at a time where a slot is hashed and the runs are
/// long enough to hash a chunk of rows at a time; else each of `rows` packs its
/// own key as it is read. Out of line, so the loops that read their key in place
/// compile as they do without it.
#[inline(never)]
fn packed_identity<L: IdentityLoop>(p: &KeyPacker, mb: &MemBatch, rows: &[(usize, usize)], body: L) -> L::Out {
    let (n, read) = (mb.len(), crate::repr::range_rows(rows));
    let few = |k: usize| k * PACK_ROWS_MIN_SPARSITY <= n;
    let all = [(0, n)];
    let packed: &[(usize, usize)] = match p.packs_without_hashing() {
        _ if rows == all => rows,
        // Packing every row costs less than packing the rows of `rows` alone.
        true if !(few(rows.len()) && few(read)) => &all,
        false if read >= FOLD_CHUNK_MIN_ROWS * rows.len() => rows,
        _ => return body.run(|r| p.narrow_image(mb, r)),
    };
    // Indexed by row; a row left unpacked keeps its zero, which nothing reads.
    let mut identities = vec![0u128; n];
    p.identities_over(mb, packed, &mut identities);
    body.run(|r| identities[r])
}

/// Fewest rows per row read at which [`packed_identity`] packs a key that hashes
/// no slot a row at a time, rather than every row.
const PACK_ROWS_MIN_SPARSITY: usize = 16;

/// The runs of equal adjacent keys over rows `0..n`, each as `[start, end)`,
/// into `runs`.
fn equal_runs<K: PartialEq>(n: usize, key: impl Fn(usize) -> K, runs: &mut Vec<(usize, usize)>) {
    runs.clear();
    if n == 0 {
        return;
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
}

/// Fewest rows per group at which hashing a batch's rows into groups costs
/// less than sorting them.
const HASH_MIN_ROWS_PER_GROUP: usize = 4;

/// Fewest rows at which hashing them costs less than sorting them, however few
/// their groups.
const HASH_MIN_ROWS: usize = 16;

/// A batch's groups as one ordinal per row: no row moves, and a fold over the
/// groups is one pass per column.
#[derive(Default)]
pub(crate) struct GroupOrdinals {
    /// Row → group ordinal; empty after [`Self::fill_heads`].
    pub(crate) ord: Vec<u32>,
    /// Group ordinal → its first row.
    pub(crate) first: Vec<u32>,
    /// The group ordinals in ascending output-PK order.
    pub(crate) by_pk: Vec<u32>,
    /// Group ordinal → its [`GroupOutKey::with_identity`] identity, where the
    /// numbering computed one; else empty.
    pub(crate) ids: Vec<u128>,
}

impl GroupOrdinals {
    #[inline]
    pub(crate) fn len(&self) -> usize {
        self.first.len()
    }

    /// The buffers, emptied, taken out of `self`. A fill destructures them into
    /// locals rather than writing `self`'s fields: with the four in one struct,
    /// a call handed one of them makes the loop reload the others' lengths.
    fn take_cleared(&mut self) -> Self {
        let mut taken = std::mem::take(self);
        taken.ord.clear();
        taken.first.clear();
        taken.by_pk.clear();
        taken.ids.clear();
        taken
    }

    /// Run `g` is group `g`, its rows not numbered: `ord` is left empty, for a
    /// fold over the runs themselves.
    pub(crate) fn fill_heads(&mut self, runs: &[(usize, usize)]) {
        let GroupOrdinals { ord, mut first, mut by_pk, ids } = self.take_cleared();
        first.extend(runs.iter().map(|&(start, _)| start as u32));
        by_pk.extend(0..runs.len() as u32);
        *self = GroupOrdinals { ord, first, by_pk, ids };
    }

    /// Run `g` is group `g`.
    pub(crate) fn fill_runs(&mut self, runs: &[(usize, usize)]) {
        self.fill_heads(runs);
        let ord = &mut self.ord;
        ord.reserve(runs.last().map_or(0, |&(_, end)| end));
        for (g, &(start, end)) in runs.iter().enumerate() {
            ord.extend(std::iter::repeat_n(g as u32, end - start));
        }
    }

    /// Groups numbered in ascending `key` order, found by sorting the rows; `id`
    /// maps a group's key to the identity kept for it, `None` for every key
    /// where none is kept.
    fn fill_sorted<'a, K: PkSortKey<'a> + Default>(
        &mut self,
        n: usize,
        key: impl Fn(usize) -> K,
        id: impl Fn(K) -> Option<u128>,
    ) {
        let GroupOrdinals { mut ord, mut first, mut by_pk, mut ids } = self.take_cleared();
        // A batch too small to hash sorts on the stack.
        let mut pairs: SmallVec<[(K, u32); HASH_MIN_ROWS]> = (0..n).map(|i| (key(i), i as u32)).collect();
        K::sort_by_key(&mut pairs, |p| p.0);
        zeroed(&mut ord, n);
        for (p, &(k, row)) in pairs.iter().enumerate() {
            if p == 0 || k != pairs[p - 1].0 {
                first.push(row);
                ids.extend(id(k));
            }
            // The sort leaves a group's rows in any order: its first row is its least.
            let g = first.len() - 1;
            first[g] = first[g].min(row);
            ord[row as usize] = g as u32;
        }
        by_pk.extend(0..first.len() as u32);
        *self = GroupOrdinals { ord, first, by_pk, ids };
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

    /// Forget every identity, the previous row's included.
    fn clear(&mut self) {
        self.by_identity.clear();
        self.last = None;
    }
}

/// Groups numbered in discovery order, found by hashing each row's identity,
/// into `groups`.
struct Hashed<'s> {
    n: usize,
    groups: &'s mut GroupOrdinals,
    numbers: &'s mut GroupNumbers,
}

impl IdentityLoop for Hashed<'_> {
    /// False, `groups` unspecified, once the batch holds more groups than
    /// [`HASH_MIN_ROWS_PER_GROUP`] admits for its rows.
    type Out = bool;

    fn run(self, identity: impl Fn(usize) -> u128) -> Self::Out {
        let Hashed { n, groups: out, numbers: kept } = self;
        let limit = n / HASH_MIN_ROWS_PER_GROUP;
        let GroupOrdinals { mut ord, mut first, mut by_pk, mut ids } = out.take_cleared();
        let mut numbers = std::mem::take(kept);
        numbers.clear();
        // Room for every group the hash admits, and the one it gives up at.
        ord.reserve(n);
        first.reserve(limit + 1);
        ids.reserve(limit + 1);
        numbers.by_identity.reserve(limit + 1);
        let mut fits = true;
        for row in 0..n {
            let id = identity(row);
            let new = || {
                let g = first.len();
                first.push(row as u32);
                ids.push(id);
                (g < limit).then_some(g as u32).ok_or(())
            };
            match numbers.ordinal(id, new) {
                Ok(g) => ord.push(g),
                Err(()) => {
                    fits = false;
                    break;
                }
            }
        }
        if fits {
            // The identities are distinct and sort as the output PK. Taken in
            // discovery order, which is often PK order already.
            by_pk.extend(0..first.len() as u32);
            by_pk.sort_unstable_by_key(|&g| ids[g as usize]);
        }
        (*out, *kept) = (GroupOrdinals { ord, first, by_pk, ids }, numbers);
        fits
    }
}

#[cfg(test)]
#[path = "tests/group_key.rs"]
mod tests;

#[cfg(test)]
#[path = "benches/group_key.rs"]
mod bench;
