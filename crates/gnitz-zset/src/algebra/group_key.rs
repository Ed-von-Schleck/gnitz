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
//!
//! An index of an operator's input rows grouped like its output — a reduce's
//! value index, an order index — leads each entry's PK with the group's
//! [`GroupPrefix`]: the group's columns packed, or the key's identity.

use super::key_pack::{FOLD_CHUNK, FOLD_CHUNK_MIN_ROWS, FoldCols, KeyPacker, locate_key_col, pk_run};
use super::zeroed;
use crate::repr::{Batch, MemBatch, vec_bytes};
use crate::schema::{
    DerivedSchema, PkSortKey, ReduceOutSlot, SchemaColumn, SchemaDescriptor, natural_key, pk_width_dispatch,
    radix_sort_by_key, reduce_out_layout,
};
use gnitz_expr::{BatchView, ColumnLocator};
use gnitz_wire::{MAX_PK_COLUMNS, NARROW_PK_MAX_BYTES, TypeCode};
use rustc_hash::FxHashMap;
use smallvec::SmallVec;
use std::collections::hash_map::Entry;

/// A row's key as the PK bytes its consumer gives it: a group as the output PK a
/// reduce over the group set stamps, a join key as its `_join_pk`.
pub(crate) enum GroupKey {
    /// The `n` PK bytes at `at`: consecutive PK columns' own images.
    PkRange { at: usize, n: usize },
    /// The OPK image of the payload column in `slot`, at that column's width: a
    /// natural key only, since a lone column packs as itself only when it is
    /// non-nullable and keyed by its own type, which makes it PK-eligible.
    Image { slot: u8, size: u8, type_code: TypeCode },
    /// The group's packed key, NULLs marked, held by the `_group_pk` — also a
    /// join key packed off the PK region: at most 16 bytes.
    Packed(KeyPacker),
    /// The NULL-distinct XXH3 fold of the group's columns, held by the
    /// `_group_pk` of a group whose packed key does not fit one.
    Fold(FoldCols),
    /// The empty group set's one group, V₀: no column is read.
    Global,
}

/// `group_cols` over `schema`, each column and where a row holds it; refused
/// when one is out of range or has no key image.
fn locate_group(schema: &SchemaDescriptor, group_cols: &[u32]) -> Result<Vec<(SchemaColumn, ColumnLocator)>, String> {
    group_cols
        .iter()
        .map(|&c| locate_key_col(schema, c, "group key"))
        .collect()
}

impl GroupKey {
    /// Refused when a group column is out of range or has no key image.
    pub(crate) fn new(schema: &SchemaDescriptor, group_cols: &[u32]) -> Result<Self, String> {
        Ok(Self::of_group(
            &locate_group(schema, group_cols)?,
            natural_key(schema, group_cols),
        ))
    }

    /// The key of the located `group`, `natural` where its columns are the
    /// output PK's own.
    fn of_group(group: &[(SchemaColumn, ColumnLocator)], natural: bool) -> Self {
        let locs = || group.iter().map(|&(_, loc)| loc).collect::<Vec<_>>();
        if natural {
            // The group's own columns: one PK run, or one payload column.
            return Self::in_place(&locs()).expect("a natural group key reads in place");
        }
        if group.is_empty() {
            return GroupKey::Global;
        }
        // A `_group_pk` holds the group itself where its packed key fits one.
        let fits = |p: &KeyPacker| p.key_columns().count() <= MAX_PK_COLUMNS && p.out_stride() <= GROUP_PK_BYTES;
        match KeyPacker::group(group).filter(fits) {
            Some(p) => Self::packed(p).ok().expect("a packed key of at most 16 bytes"),
            None => GroupKey::Fold(FoldCols::new(locs())),
        }
    }

    /// The key `packer` packs, read in place where the batch already spells it;
    /// else the packer back where it packs more than 16 bytes.
    pub(crate) fn packed(packer: KeyPacker) -> Result<Self, KeyPacker> {
        match packer.identity_columns().and_then(|locs| Self::in_place(&locs)) {
            Some(key) => Ok(key),
            None if packer.out_stride() <= NARROW_PK_MAX_BYTES => Ok(GroupKey::Packed(packer)),
            None => Err(packer),
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
    pub(crate) fn form<'a>(&self, mb: &MemBatch<'a>) -> KeyForm<'a> {
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
            GroupKey::Packed(_) | GroupKey::Fold(_) => KeyForm::Chunked,
            GroupKey::Image { slot, size, type_code } => KeyForm::Cells(KeyCells {
                region: mb.col_data(slot as usize),
                stride: size as usize,
                off: 0,
                width: size as usize,
                opk: false,
                signed: type_code.is_signed_int(),
            }),
            GroupKey::Global => KeyForm::Global,
        }
    }

    /// Row `row`'s identity: the key up to 16 bytes, widened as
    /// `gnitz_wire::widen_pk_be` does, else its XXH3-128.
    #[inline(always)]
    fn identity(&self, mb: &MemBatch, row: usize) -> u128 {
        match *self {
            GroupKey::PkRange { at, n } if n <= NARROW_PK_MAX_BYTES => {
                gnitz_wire::widen_pk_be(mb.get_pk_range(row, at, n))
            }
            GroupKey::PkRange { at, n } => gnitz_wire::checksum_128(mb.get_pk_range(row, at, n)),
            GroupKey::Image { slot, size, type_code } => {
                ColumnLocator::Payload { slot, size, type_code }.opk_image(mb, row)
            }
            GroupKey::Packed(ref p) => p.narrow_image(mb, row),
            GroupKey::Fold(ref f) => f.key_row(mb, row),
            GroupKey::Global => gnitz_wire::global_group_key(),
        }
    }

    /// The identities of rows `start..start + out.len()` into `out`, of a narrow
    /// packed key or a fold.
    #[inline]
    pub(crate) fn identities<B: BatchView>(&self, batch: &B, start: usize, out: &mut [u128]) {
        match self {
            GroupKey::Packed(p) => p.identities(batch, start, out),
            GroupKey::Fold(f) => f.identities(batch, start, out),
            _ => unreachable!("only a packed key or a fold computes its identities ahead"),
        }
    }

    /// Whether computing an identity hashes: a fold, a packed key with a string
    /// column, or a PK range wider than 16 bytes.
    fn hashes(&self) -> bool {
        match *self {
            GroupKey::Packed(ref p) => !p.packs_without_hashing(),
            GroupKey::Fold(_) => true,
            GroupKey::PkRange { n, .. } => n > NARROW_PK_MAX_BYTES,
            GroupKey::Image { .. } | GroupKey::Global => false,
        }
    }
}

/// A [`GroupKey`] as one batch's rows hold it.
pub(crate) enum KeyForm<'a> {
    /// One cell of 1 to 16 bytes per row.
    Cells(KeyCells<'a>),
    /// A PK range wider than a cell.
    WidePk { at: usize, n: usize },
    /// A packed key or a fold: its identities computed a chunk of rows at a time.
    Chunked,
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

/// The width of the synthetic `_group_pk` key — the whole PK region of an
/// output whose group set has no natural key.
const GROUP_PK_BYTES: usize = ReduceOutSlot::SYNTHETIC_KEY_COL.size();

/// One row's group output PK: borrowed out of the batch, or held inline.
pub enum OutPk<'a> {
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
    pub fn bytes(&self) -> &[u8] {
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
pub fn ground_pk() -> [u8; GROUP_PK_BYTES] {
    gnitz_wire::global_group_key().to_be_bytes()
}

/// How an operator keyed like a reduce (the reduce itself, the ad-hoc fold, the
/// top-N, the window) groups its input and keys its output. A group is its output PK.
pub struct GroupOutKey {
    key: GroupKey,
    /// The output PK is a hidden `_group_pk`, which the key is widened to.
    synthetic: bool,
    /// Packs the group's columns as an index's [`GroupPrefix`] leads with them;
    /// `None` where they pass the PK's column limit.
    index_packer: Option<KeyPacker>,
    /// The input columns the output carries after its key, as the input locates them.
    carried: Vec<ColumnLocator>,
}

impl GroupOutKey {
    /// `group_cols` over `input`, and the output's leading columns: the key
    /// region, then each `row` column the key region does not spell.
    pub fn new(
        input: &SchemaDescriptor,
        group_cols: &[u32],
        row: impl IntoIterator<Item = u32>,
    ) -> Result<(Self, DerivedSchema), String> {
        let group = locate_group(input, group_cols)?;
        let natural = natural_key(input, group_cols);
        let key = GroupKey::of_group(&group, natural);
        // The output PK's own columns where the key is natural; else the group as
        // a packed `_group_pk` packs it, also where that is wider than one.
        let index_packer = match natural {
            true => Some(KeyPacker::of_span(group.iter().map(|&(_, loc)| loc))),
            false => KeyPacker::group(&group),
        };
        let mut b = DerivedSchema::new();
        let mut carried = Vec::new();
        for slot in reduce_out_layout(input, group_cols, row) {
            match slot {
                ReduceOutSlot::SyntheticKey => b.push_pk(ReduceOutSlot::SYNTHETIC_KEY_COL),
                ReduceOutSlot::Key(c) => b.push_pk(input.columns()[c as usize]),
                ReduceOutSlot::Carried(c) => {
                    carried.push(input.locate(c as usize));
                    b.push(input.columns()[c as usize])
                }
            }
        }
        Ok((
            GroupOutKey {
                key,
                synthetic: !natural,
                index_packer,
                carried,
            },
            b,
        ))
    }

    /// Grouped by the empty set: one group, V₀.
    #[inline]
    pub(crate) fn is_global(&self) -> bool {
        matches!(self.key, GroupKey::Global)
    }

    /// The output PK of `row`'s group.
    #[inline]
    pub fn out_pk<'a>(&self, mb: &'a MemBatch, row: usize) -> OutPk<'a> {
        self.out_pk_with(mb, row, None)
    }

    /// [`Self::out_pk`], built from `id` — the group's [`Self::with_identity`]
    /// identity — where one is given: a `_group_pk` is the identity, a natural
    /// payload key its image, and a natural PK range is borrowed from `row`.
    #[inline]
    pub fn out_pk_with<'a>(&self, mb: &'a MemBatch, row: usize, id: Option<u128>) -> OutPk<'a> {
        let width = match self.key {
            GroupKey::PkRange { at, n } if !self.synthetic => return OutPk::Borrowed(mb.get_pk_range(row, at, n)),
            GroupKey::Image { size, .. } => size as usize,
            _ => GROUP_PK_BYTES,
        };
        OutPk::inline(id.unwrap_or_else(|| self.key.identity(mb, row)), width)
    }

    /// The prefix an index's entries lead with — see [`GroupPrefix`] — and the
    /// index's PK region: the prefix's columns, then `suffix`.
    pub fn index_prefix(&self, suffix: &[SchemaColumn]) -> (GroupPrefix, DerivedSchema) {
        let packer = (self.index_packer.clone()).filter(|p| p.key_columns().count() + suffix.len() <= MAX_PK_COLUMNS);
        let mut b = DerivedSchema::new();
        match &packer {
            Some(p) => p.key_columns().for_each(|c| b.push_pk(c)),
            None => b.push_pk(ReduceOutSlot::SYNTHETIC_KEY_COL),
        }
        suffix.iter().for_each(|&c| b.push_pk(c));
        (GroupPrefix { packer }, b)
    }

    /// The prefix of `row`'s group into the front of `buf`, returning it; built
    /// from `id`, where one is given, where the prefix is the key's identity.
    #[inline]
    pub fn prefix_of<'b>(
        &self,
        prefix: &GroupPrefix,
        mb: &MemBatch,
        row: usize,
        id: Option<u128>,
        buf: &'b mut [u8],
    ) -> &'b [u8] {
        match &prefix.packer {
            Some(p) => p.pack_prefix(buf, mb, row),
            None => {
                let dst = &mut buf[..GROUP_PK_BYTES];
                dst.copy_from_slice(&id.unwrap_or_else(|| self.key.identity(mb, row)).to_be_bytes());
                dst
            }
        }
    }

    /// The prefix of each row of `runs` — ascending `[start, end)` — in order,
    /// into the fronts of `dst`'s keys, `stride` apart.
    #[inline(always)]
    pub fn write_prefixes(
        &self,
        prefix: &GroupPrefix,
        mb: &MemBatch,
        runs: &[(usize, usize)],
        dst: &mut [u8],
        stride: usize,
    ) {
        match (&prefix.packer, &self.key) {
            (Some(p), _) => p.pack_rows(dst, stride, mb, runs),
            // A chunk of identities at a time on the stack, rather than every row's.
            (None, GroupKey::Packed(_) | GroupKey::Fold(_)) => {
                let mut buf = [0u128; FOLD_CHUNK];
                let mut keys = dst.chunks_exact_mut(stride);
                for &(s, e) in runs {
                    for start in (s..e).step_by(FOLD_CHUNK) {
                        let ids = &mut buf[..(e - start).min(FOLD_CHUNK)];
                        self.key.identities(mb, start, ids);
                        for (id, key) in ids.iter().zip(keys.by_ref()) {
                            key[..GROUP_PK_BYTES].copy_from_slice(&id.to_be_bytes());
                        }
                    }
                }
            }
            (None, _) => self.with_identity(mb, runs, WriteIdentities { dst, stride, runs }),
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
            KeyForm::Chunked => packed_identity(&self.key, mb, rows, body),
            KeyForm::WidePk { at, n } => body.run(|r| gnitz_wire::checksum_128(mb.get_pk_range(r, at, n))),
        }
    }

    /// The input columns the output carries after its key.
    #[inline(always)]
    pub fn carried(&self) -> &[ColumnLocator] {
        &self.carried
    }

    /// `batch`'s groups as row runs into `out`, where its rows already stand in
    /// ascending output-PK order; false, `out` unspecified, where they need not.
    pub fn runs_into(&self, batch: &Batch, out: &mut Vec<(usize, usize)>) -> bool {
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
    pub fn numbered_into(&self, batch: &Batch, s: &mut GroupScratch) {
        let mb = &batch.as_mem_batch();
        let n = mb.len();
        // Too few rows do not pay for a hash table. A wide PK's identity is a
        // digest, which does not sort as the PK.
        let hashable = n >= HASH_MIN_ROWS && !matches!(self.key.form(mb), KeyForm::WidePk { .. });
        let GroupScratch { groups, numbers, identities } = s;
        match &self.key {
            GroupKey::Packed(_) | GroupKey::Fold(_) => {
                // Every row packed once, for the hash and for the sort it may fall back to.
                identities.clear();
                identities.resize(n, 0);
                self.key.identities(mb, 0, identities);
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

/// How a group leads the PK of an index of its operator's input rows: the
/// group's columns packed — the output PK's own columns, the bytes a packed
/// `_group_pk` holds, or a fold's columns packed as such a `_group_pk` packs
/// them, at any width — where those and the index's suffix fit the PK's column
/// limit, else the key's 16-byte identity. Equal prefixes are one group, so a
/// group's entries are one run of the index.
pub struct GroupPrefix {
    /// Packs the key's own bytes; `None` for the identity.
    packer: Option<KeyPacker>,
}

impl GroupPrefix {
    /// The bytes of one prefix.
    #[inline]
    pub fn width(&self) -> usize {
        self.packer.as_ref().map_or(GROUP_PK_BYTES, KeyPacker::out_stride)
    }

    /// Whether computing every row's prefix hashes, so it costs about what
    /// computing a few rows' costs.
    pub fn hashes(&self, key: &GroupOutKey) -> bool {
        match &self.packer {
            Some(p) => !p.packs_without_hashing(),
            None => key.key.hashes(),
        }
    }
}

/// The buffers [`GroupOutKey::numbered_into`] numbers a batch's groups with,
/// kept by a caller that numbers batch after batch.
#[derive(Default)]
pub struct GroupScratch {
    /// The last numbering.
    pub groups: GroupOrdinals,
    numbers: GroupNumbers,
    /// Every row's packed key as its identity, for a packed group key.
    identities: Vec<u128>,
}

impl GroupScratch {
    /// The bytes this holds allocated.
    pub fn held_bytes(&self) -> usize {
        let GroupOrdinals { ord, first, by_pk, ids } = &self.groups;
        let map = self.numbers.by_identity.capacity() * size_of::<(u128, u32)>();
        vec_bytes(ord) + vec_bytes(first) + vec_bytes(by_pk) + vec_bytes(ids) + vec_bytes(&self.identities) + map
    }
}

/// [`GroupOutKey::with_identity`] over a packed `_group_pk` or a fold: every row
/// computed once for the whole loop where `rows` are all of them, or where the
/// key hashes nothing and `rows` hold more than one row in
/// `PACK_ROWS_MIN_SPARSITY`; the rows of `rows` a run at a time where the key
/// hashes and the runs are long enough to hash a chunk of rows at a time; else
/// each of `rows` computes its own identity as it is read. Out of line, so the
/// loops that read their key in place compile as they do without it.
#[inline(never)]
fn packed_identity<L: IdentityLoop>(key: &GroupKey, mb: &MemBatch, rows: &[(usize, usize)], body: L) -> L::Out {
    let (n, read) = (mb.len(), crate::repr::range_rows(rows));
    let few = |k: usize| k * PACK_ROWS_MIN_SPARSITY <= n;
    let all = [(0, n)];
    let packed: &[(usize, usize)] = match key.hashes() {
        _ if rows == all => rows,
        // Packing every row costs less than packing the rows of `rows` alone.
        false if !(few(rows.len()) && few(read)) => &all,
        true if read >= FOLD_CHUNK_MIN_ROWS * rows.len() => rows,
        _ => return body.run(|r| key.identity(mb, r)),
    };
    // Indexed by row; a row left uncomputed keeps its zero, which nothing reads.
    let mut identities = vec![0u128; n];
    for &(s, e) in packed {
        key.identities(mb, s, &mut identities[s..e]);
    }
    body.run(|r| identities[r])
}

/// [`GroupOutKey::write_prefixes`] of a prefix that is the identity: each row's,
/// big-endian, into the front of its key.
struct WriteIdentities<'a> {
    dst: &'a mut [u8],
    stride: usize,
    runs: &'a [(usize, usize)],
}

impl IdentityLoop for WriteIdentities<'_> {
    type Out = ();

    fn run(self, identity: impl Fn(usize) -> u128) {
        let mut keys = self.dst.chunks_exact_mut(self.stride);
        for &(s, e) in self.runs {
            for (row, key) in (s..e).zip(keys.by_ref()) {
                key[..GROUP_PK_BYTES].copy_from_slice(&identity(row).to_be_bytes());
            }
        }
    }
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
pub struct GroupOrdinals {
    /// Row → group ordinal; empty after [`Self::fill_heads`].
    pub ord: Vec<u32>,
    /// Group ordinal → its first row.
    pub first: Vec<u32>,
    /// The group ordinals in ascending output-PK order.
    pub by_pk: Vec<u32>,
    /// Group ordinal → its [`GroupOutKey::with_identity`] identity, where the
    /// numbering computed one; else empty.
    pub ids: Vec<u128>,
}

impl GroupOrdinals {
    #[inline]
    pub fn len(&self) -> usize {
        self.first.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.first.is_empty()
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
    pub fn fill_heads(&mut self, runs: &[(usize, usize)]) {
        let GroupOrdinals { ord, mut first, mut by_pk, ids } = self.take_cleared();
        first.extend(runs.iter().map(|&(start, _)| start as u32));
        by_pk.extend(0..runs.len() as u32);
        *self = GroupOrdinals { ord, first, by_pk, ids };
    }

    /// Run `g` is group `g`.
    pub fn fill_runs(&mut self, runs: &[(usize, usize)]) {
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
    fn fill_sorted<'a, K: PkSortKey<'a>>(
        &mut self,
        n: usize,
        key: impl Fn(usize) -> K,
        id: impl Fn(K) -> Option<u128>,
    ) {
        let GroupOrdinals { mut ord, mut first, mut by_pk, mut ids } = self.take_cleared();
        // A batch too small to hash sorts on the stack.
        let mut pairs: SmallVec<[(K, u32); HASH_MIN_ROWS]> = (0..n).map(|i| (key(i), i as u32)).collect();
        radix_sort_by_key(&mut pairs, |p| p.0);
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
            radix_sort_by_key(&mut by_pk, |&g| ids[g as usize]);
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
