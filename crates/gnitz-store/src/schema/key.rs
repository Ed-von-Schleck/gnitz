//! Order-preserving primary-key (OPK) primitives.
//!
//! These pure layout/key operations sit *below* both `schema` and `storage`:
//! they encode a PK region — a whole one or an index's leading-column span —
//! to its order-preserving big-endian image, compare two such images with a
//! raw `memcmp`, pack a narrow region into a sort key, carry a width-tagged PK
//! byte buffer, and derive the half-open key range a `KeyRange`'s cut pair
//! denotes — and compose a multi-column key span ([`KeySpec`]). None of them
//! reaches up into storage — the dependency runs `storage → schema::key`, the
//! legitimate downward direction. This module is the one import path: every
//! caller, storage included, names `crate::schema::key::X`.
//!
//! The per-column codec and tuple encoders are `gnitz_wire::pk`'s, shared with the
//! client; this module composes schema-typed and row-sourced keys from them.

use crate::schema::ColumnTable;
use std::cmp::Ordering;

use gnitz_expr::RowSource;
use gnitz_wire::{KeyRange, NARROW_PK_MAX_BYTES};

use crate::schema::{
    oob_col, ColumnLocator, DerivedSchema, SchemaBound, SchemaColumn, SchemaDescriptor, SchemaFacts, TypeCode,
    MAX_PK_COLUMNS,
};

// ---------------------------------------------------------------------------
// Column-aware PK byte-region comparator
// ---------------------------------------------------------------------------

/// Raw byte comparator for PK regions.
///
/// Unsigned byte order over OPK regions, which is the typed PK order at any width.
///
/// This and [`compare_pk_ordering`] return the same `Ordering` for equal-width
/// inputs. The rule between them: this one is **total** — it accepts operands of
/// differing width and orders them lexicographically — while
/// `compare_pk_ordering` requires equal widths and settles everything up to 16
/// bytes on the packed `u128` image instead. Reach for that one wherever the two
/// operands are one relation's rows (a merge, a group fold, a probe); reach for
/// this one where a width may differ or the operand is untrusted.
#[inline(always)]
pub(crate) fn compare_pk_bytes(a: &[u8], b: &[u8]) -> Ordering {
    a.cmp(b)
}

/// Sort `idx` into the order of `flat`'s `stride`-byte records, which stay in
/// place.
pub fn sort_indices(flat: &[u8], stride: usize, idx: &mut Vec<u32>) {
    let n = flat.len() / stride;
    assert!(n <= u32::MAX as usize, "record count exceeds u32");
    idx.clear();
    idx.extend(0..n as u32);
    idx.sort_unstable_by(|&a, &b| {
        let a = a as usize * stride;
        let b = b as usize * stride;
        compare_pk_bytes(&flat[a..a + stride], &flat[b..b + stride])
    });
}

/// Typed lexicographic OPK ordering of two **equal-length** PK regions — the
/// comparator the N-way merge and the read-cursor loser tree read through their
/// sources. Returns the same `Ordering` as [`compare_pk_bytes`] at every width,
/// settling the common case on the leading-16 `pack_pk_be` image. For `len ≤ 16`
/// that image is the *whole* PK and is injective, so a `pack_pk_be` tie is
/// already a byte-equal PK — the byte
/// compare is skipped (it would be a guaranteed-`Equal` `memcmp`). Only `len > 16`
/// can tie on the 16-byte prefix while differing later, so the full-byte
/// `compare_pk_bytes` tiebreak runs only there. No stride / width-class dispatch.
#[inline(always)]
pub(crate) fn compare_pk_ordering(a: &[u8], b: &[u8]) -> Ordering {
    debug_assert_eq!(a.len(), b.len(), "compare_pk_ordering on unequal PK widths");
    match pack_pk_be(a).cmp(&pack_pk_be(b)) {
        Ordering::Equal if a.len() > NARROW_PK_MAX_BYTES => compare_pk_bytes(a, b),
        ord => ord,
    }
}

/// OPK byte-equality of two **equal-length** PK regions (a full PK, or a shared
/// equi-prefix sliced to the same width on both sides): [`compare_pk_ordering`]'s
/// `Equal`, so it inherits that function's equal-width contract, its width
/// reasoning and its `pk_bytes_eq(a, b) == (a == b)` guarantee. Use at every
/// merge/group fold or probe that tests "same PK".
#[inline(always)]
pub(crate) fn pk_bytes_eq(a: &[u8], b: &[u8]) -> bool {
    compare_pk_ordering(a, b) == Ordering::Equal
}

/// `min <= key <= max` over OPK bytes. The exact-match probes gate on this
/// before searching, so a key outside a block's bounds costs two `memcmp`s
/// instead of a `log n` walk over cold cache lines. The cursor's lower-bound
/// seeks do not: they need a landing position on a miss, not a verdict.
#[inline]
pub(crate) fn pk_in_range(min: &[u8], max: &[u8], key: &[u8]) -> bool {
    compare_pk_bytes(min, key) != Ordering::Greater && compare_pk_bytes(key, max) != Ordering::Greater
}

/// The **inclusive** OPK ranges `[min, max]` and `[lo, hi]` intersect — whether a
/// run or shard holding `[min, max]` can answer about a key the caller's bound
/// admits. A half-open upper bound passed as `hi` over-approximates, which is
/// the safe direction.
#[inline]
pub(crate) fn pk_ranges_overlap(min: &[u8], max: &[u8], lo: &[u8], hi: &[u8]) -> bool {
    compare_pk_bytes(max, lo) != Ordering::Less && compare_pk_bytes(min, hi) != Ordering::Greater
}

// ---------------------------------------------------------------------------
// Narrow-region PK key packing
// ---------------------------------------------------------------------------

/// BE sort-key packer over an OPK region. Left-aligns the bytes at the MSB end
/// of a `u128` and reads big-endian, so `pack_pk_be(a).cmp(&pack_pk_be(b))`
/// equals the lexicographic byte order of the OPK regions — exactly
/// `compare_pk_bytes`. Narrow (`len ≤ 16`) = the exact key; wide (`len > 16`) =
/// the order-preserving leading-16 prefix (authoritative whenever two prefixes
/// differ; a prefix collision needs a `compare_pk_bytes` tiebreak).
///
/// The `{2, 4, 8, ≥16}` arms load the dominant scalar and `U128`/wide-prefix
/// widths straight into a register, value-equal to the pad-and-copy (a narrow
/// value occupies the high bits, the low bits zero; `≥16` reads the
/// order-preserving leading 16 bytes) — pinned by
/// `pack_pk_be_specialization_matches_naive`, which covers every width. The
/// remaining arms compose the same fixed-width loads: none copies through a
/// runtime length, which would be an out-of-line `memcpy` per key.
///
/// NOT a value accessor — for a U64 OPK value 1 (`[0,…,0,1]` at `[..8]`) this
/// packs as `1·2^64`, not 1. Opposite alignment from `gnitz_wire::widen_pk_be`
/// (right-aligned value recovery); never conflate them.
#[inline(always)]
pub(crate) fn pack_pk_be(pk_bytes: &[u8]) -> u128 {
    match pk_bytes.len() {
        8 => (u64::from_be_bytes(pk_bytes[..8].try_into().unwrap()) as u128) << 64,
        len if len >= 16 => u128::from_be_bytes(pk_bytes[..16].try_into().unwrap()),
        4 => (u32::from_be_bytes(pk_bytes[..4].try_into().unwrap()) as u128) << 96,
        2 => (u16::from_be_bytes(pk_bytes[..2].try_into().unwrap()) as u128) << 112,
        // 9..=15: two overlapping big-endian loads instead of a runtime-length
        // `copy_from_slice`, which lowers to an out-of-line `memcpy` per key. With
        // `m = len - 8`, the low `m` bytes of the tail load are exactly
        // `pk_bytes[8..len]`, since `len - m == 8`. Measured ~3x on this band —
        // the merge and AVI paths sit here (`GROUP BY <INT>` is 13,
        // `PRIMARY KEY (BIGINT, INT)` is 12).
        len @ 9..=15 => {
            let m = len - 8;
            let hi = u64::from_be_bytes(pk_bytes[..8].try_into().unwrap()) as u128;
            let tail = u64::from_be_bytes(pk_bytes[len - 8..len].try_into().unwrap());
            let low = (tail & ((1u64 << (8 * m)) - 1)) as u128;
            (hi << 64) | (low << (64 - 8 * m))
        }
        1 => (pk_bytes[0] as u128) << 120,
        3 => {
            let hi = u16::from_be_bytes(pk_bytes[..2].try_into().unwrap()) as u128;
            (hi << 112) | ((pk_bytes[2] as u128) << 104)
        }
        5 => {
            let hi = u32::from_be_bytes(pk_bytes[..4].try_into().unwrap()) as u128;
            (hi << 96) | ((pk_bytes[4] as u128) << 88)
        }
        6 => {
            let hi = u32::from_be_bytes(pk_bytes[..4].try_into().unwrap()) as u128;
            let lo = u16::from_be_bytes(pk_bytes[4..6].try_into().unwrap()) as u128;
            (hi << 96) | (lo << 80)
        }
        7 => {
            let hi = u32::from_be_bytes(pk_bytes[..4].try_into().unwrap()) as u128;
            let mid = u16::from_be_bytes(pk_bytes[4..6].try_into().unwrap()) as u128;
            (hi << 96) | (mid << 80) | ((pk_bytes[6] as u128) << 72)
        }
        // Width 0: an empty OPK region packs to zero.
        _ => 0,
    }
}

/// The leading eight OPK bytes as a `u64`, right-zero-padded for a narrower key.
/// A value accessor, where [`pack_pk_be`] is deliberately not one.
#[inline(always)]
pub(crate) fn leading_u64(pk_bytes: &[u8]) -> u64 {
    if pk_bytes.len() >= 8 {
        u64::from_be_bytes(pk_bytes[..8].try_into().unwrap())
    } else {
        // `pack_pk_be` left-aligns a narrow key with register loads at 2 and 4.
        (pack_pk_be(pk_bytes) >> 64) as u64
    }
}

/// The `stride` OPK bytes of a narrow PK value that is **already in OPK/route
/// space** — the widened image `widen_pk_be` produces, sign-flipped for a signed
/// key. Right-aligning it big-endian reproduces the key's OPK region at any
/// width. A *native* value must be encoded through [`SchemaFacts::opk_key`]
/// instead, which applies the per-column sign flip this does not.
///
/// The home for "right-align a `u128` into an OPK of width `stride`" wherever
/// the stride is a runtime value — the batch PK setters and the reduce
/// group-key emitters build one, so the width checks below cannot be skipped by
/// hand-rolling `&pk.to_be_bytes()[16 - stride..]`, which silently truncates a
/// value that overflows the stride. (A writer whose slot width is fixed by the
/// schema — the packer's string arm, `AviBake::entry` — is
/// width-total already and copies its `to_be_bytes` directly.) Zero-cost: a
/// stack value, no allocation.
pub(crate) struct NarrowPkOpk {
    be: [u8; 16],
    stride: usize,
}

impl NarrowPkOpk {
    #[inline(always)]
    pub(crate) fn new(pk: u128, stride: usize) -> Self {
        // Static message: `#[inline(always)]` puts this in every per-row caller,
        // and an `Arguments` value costs a stack slot even on a cold panic path.
        assert!(
            stride <= NARROW_PK_MAX_BYTES,
            "NarrowPkOpk::new: stride exceeds NARROW_PK_MAX_BYTES; use the raw-OPK-bytes setter"
        );
        debug_assert!(
            stride == 16 || (pk >> (stride * 8)) == 0,
            "narrow PK {pk} does not fit {stride} bytes",
        );
        NarrowPkOpk { be: pk.to_be_bytes(), stride }
    }

    /// The `stride` order-preserving bytes — a full PK region for one row.
    #[inline(always)]
    pub(crate) fn bytes(&self) -> &[u8] {
        &self.be[16 - self.stride..]
    }
}

/// A whole-PK sort key ordered exactly as `compare_pk_bytes` orders the bytes it
/// was built from, so a key sort needs no PK-byte tiebreak. Keys compare only
/// against keys built from the same number of bytes.
pub(crate) trait PkSortKey<'a>: Ord + Copy {
    fn from_opk(opk: &'a [u8]) -> Self;
}

/// Run `$body` with `$k` bound to the [`PkSortKey`] matching `$stride` — the one
/// statement of the width taxonomy the impls below define.
macro_rules! pk_width_dispatch {
    ($stride:expr, |$k:ident| $body:expr $(,)?) => {
        match $stride {
            0..=8 => {
                type $k<'k> = u64;
                $body
            }
            9..=16 => {
                type $k<'k> = u128;
                $body
            }
            17..=32 => {
                type $k<'k> = [u128; 2];
                $body
            }
            _ => {
                type $k<'k> = &'k [u8];
                $body
            }
        }
    };
}
pub(crate) use pk_width_dispatch;

impl PkSortKey<'_> for u64 {
    #[inline(always)]
    fn from_opk(opk: &[u8]) -> u64 {
        leading_u64(opk)
    }
}

impl PkSortKey<'_> for u128 {
    #[inline(always)]
    fn from_opk(opk: &[u8]) -> u128 {
        // Identical left-align to `u64`, one width up; `pack_pk_be` is the canonical
        // left-align-to-`u128`, reused here. Dispatched only for strides ≤ 16.
        pack_pk_be(opk)
    }
}

impl<'a> PkSortKey<'a> for &'a [u8] {
    #[inline(always)]
    fn from_opk(opk: &'a [u8]) -> Self {
        opk
    }
}

impl PkSortKey<'_> for [u128; 2] {
    #[inline(always)]
    fn from_opk(opk: &[u8]) -> [u128; 2] {
        // Dispatched only for 17..=32-byte strides: hi = the full leading 16 bytes
        // (always a register load), lo = the trailing 1..=16 left-aligned. Array
        // `Ord` is lexicographic, so the low limb settles a leading-16-byte tie a
        // bare `u128` prefix would tie on.
        let hi = u128::from_be_bytes(opk[..16].try_into().unwrap());
        if opk.len() == 32 {
            [hi, u128::from_be_bytes(opk[16..32].try_into().unwrap())]
        } else {
            // `pack_pk_be` is left-align-into-a-`u128`, so its width arms give the
            // partial limb register loads where a runtime-length copy lowered to a
            // `memcpy` call. Measured per key build, against that copy: -48% at
            // stride 24, -37% at 20, -3% at 31, +2% at 17.
            [hi, pack_pk_be(&opk[16..])]
        }
    }
}

// ---------------------------------------------------------------------------
// OPK span fingerprint
// ---------------------------------------------------------------------------

/// The 64-bit fingerprint of an OPK byte span — a row's whole PK region, or an
/// index's leading-key span. Every approximate-membership structure over OPK
/// keys derives its key here, so a probe key always equals the key inserted.
#[inline]
pub fn probe_key(opk: &[u8]) -> u64 {
    gnitz_wire::checksum(opk)
}

// ---------------------------------------------------------------------------
// Width-tagged PK byte buffer
// ---------------------------------------------------------------------------

/// The OPK byte container, homed in `gnitz-wire` beside `MAX_PK_BYTES` and the
/// per-column codec whose output it carries. Re-exported because this module is
/// the one import path for the OPK vocabulary.
pub use gnitz_wire::PkBuf;

/// One key column: where its source value lives, and the key column it packs
/// into.
#[derive(Clone, Copy)]
pub(crate) struct KeyCol {
    pub(crate) loc: ColumnLocator,
    pub(crate) out: SchemaColumn,
}

impl KeyCol {
    /// Padding for the fixed array's unused slots; nothing reads past `n`.
    const EMPTY: KeyCol = KeyCol {
        loc: ColumnLocator::Pk {
            byte_off: 0,
            size: 0,
            type_code: TypeCode::U8,
        },
        out: SchemaColumn::EMPTY,
    };
}

/// A key span: columns at their key types, packed tightly in order. Built once
/// per circuit, `Copy`, so the row paths allocate nothing.
#[derive(Clone, Copy)]
pub struct KeySpec {
    n: u8,
    /// Sum of the promoted column widths, so the row path re-sums nothing.
    key_size: u8,
    /// Sized by `MAX_PK_COLUMNS`, not the wire's `PK_LIST_MAX_COLS`: `for_pk`
    /// may be handed an index schema, whose PK arity reaches the engine limit.
    cols: [KeyCol; MAX_PK_COLUMNS],
}

impl KeySpec {
    /// The span of `cols`, each at its key type.
    pub(crate) fn of(cols: impl IntoIterator<Item = (ColumnLocator, TypeCode)>) -> Self {
        let mut spec = KeySpec {
            n: 0,
            key_size: 0,
            cols: [KeyCol::EMPTY; MAX_PK_COLUMNS],
        };
        for (loc, tc) in cols {
            let out = SchemaColumn::new(tc, false);
            spec.cols[spec.n as usize] = KeyCol { loc, out };
            spec.n += 1;
            spec.key_size += out.size();
        }
        spec
    }

    /// The span's columns, in order.
    pub(crate) fn columns(&self) -> &[KeyCol] {
        &self.cols[..self.n as usize]
    }

    /// The span of a secondary index on `cols` of `owner`.
    ///
    /// `Err` on an arity outside `1..=MAX_PK_COLUMNS`, an out-of-range column, a
    /// type no index key carries, or a record over the PK arity limit — the last
    /// two through [`gnitz_wire::index_key_types`], shared with the
    /// SQL planner's CREATE INDEX pre-check.
    pub fn new(cols: &[u32], owner: &SchemaDescriptor) -> Result<Self, String> {
        // `index_key_types` has no lower bound, and a zero-column spec would
        // project the same empty span for every row. The upper bound is the fixed
        // array's, rejected not asserted: an untrusted circuit reaches here.
        if cols.is_empty() || cols.len() > MAX_PK_COLUMNS {
            return Err(format!(
                "Index: key arity {} is outside 1..={MAX_PK_COLUMNS}",
                cols.len(),
            ));
        }
        let mut col_types: Vec<TypeCode> = Vec::with_capacity(cols.len());
        for &c in cols {
            if c as usize >= owner.num_columns() {
                return Err(oob_col("Index: column", c, owner));
            }
            col_types.push(owner.columns[c as usize].type_code);
        }
        let promoted = gnitz_wire::index_key_types(&col_types, owner.pk_cols().len()).map_err(|r| r.to_string())?;
        Ok(Self::of(cols.iter().map(|&c| owner.locate(c as usize)).zip(promoted)))
    }

    /// The index schema this spec's entries land in: the key columns, then
    /// `source`'s PK columns, all in the PK. `Err` only on the limits
    /// [`Self::new`] has already checked.
    pub(in crate::schema) fn output_schema(&self, source: &SchemaDescriptor) -> Result<SchemaDescriptor, SchemaBound> {
        let mut b = DerivedSchema::new();
        for c in &self.cols[..self.n as usize] {
            b.push_pk(c.out)?;
        }
        b.push_pk_of(source)?;
        Ok(b.finish())
    }

    /// A base table's own PK as the degenerate span, each column at its own type, so a
    /// PK range walk reads through [`Self::range_keys`] as an index walk does.
    pub(crate) fn for_pk(schema: &SchemaDescriptor) -> Self {
        Self::of(schema.pk_columns().map(|(ci, c)| (schema.locate(ci), c.type_code)))
    }

    /// Span width in bytes (`idx_key_size`) — the sum of the promoted widths.
    #[inline]
    pub fn key_size(&self) -> usize {
        self.key_size as usize
    }

    /// Write one row's leading-key span into `dst[..key_size()]` — the bytes
    /// [`Self::seek_prefix`] produces for the same values.
    /// `false` when any indexed column is NULL: the row is unindexed, `dst` partly written.
    pub fn write_span(&self, mb: &impl RowSource, row: usize, dst: &mut [u8]) -> bool {
        debug_assert!(dst.len() >= self.key_size(), "write_span: dst shorter than the span");
        let mut off = 0;
        for c in &self.cols[..self.n as usize] {
            if c.loc.is_null(mb, row) {
                return false;
            }
            let w = c.out.size() as usize;
            c.loc
                .encode_opk_promoted(mb, row, c.out.type_code, &mut dst[off..off + w]);
            off += w;
        }
        true
    }

    /// One row's index entry `[span ‖ src_pk]` in `dst[..key_size() + pk_stride]`,
    /// or `false` where [`Self::write_span`] returns `false`.
    pub fn write_entry(&self, mb: &impl RowSource, row: usize, dst: &mut [u8]) -> bool {
        if !self.write_span(mb, row, dst) {
            return false;
        }
        let pk = mb.get_pk_bytes(row);
        dst[self.key_size()..self.key_size() + pk.len()].copy_from_slice(pk);
        true
    }

    /// Split a stored index entry back into `(span, source PK)` — the read-side
    /// inverse of [`Self::write_entry`], which put the PK at `key_size()`. The
    /// split is exact by layout (an index schema is the promoted indexed columns
    /// followed by the source PK columns, so its stride is `key_size() +
    /// src_pk_stride`), so nothing is decoded.
    pub fn split_entry<'a>(&self, entry: &'a [u8]) -> (&'a [u8], &'a [u8]) {
        debug_assert!(entry.len() > self.key_size(), "index entry shorter than its span");
        entry.split_at(self.key_size())
    }

    /// [`Self::write_span`] into a caller-reused `PkBuf` — no stack buffer, no
    /// `from_bytes` re-copy, since this runs in the backfill scan and on every
    /// insert. `out` comes back `key_size()` wide with a zero tail, so a caller
    /// may take `out.padded(stride)` as the span zero-padded to any wider one.
    pub fn key_bytes(&self, mb: &impl RowSource, row: usize, out: &mut PkBuf) -> bool {
        out.write(self.key_size(), |dst| self.write_span(mb, row, dst))
    }

    /// The leading-key span of one key image per leading spec column: what
    /// [`Self::write_span`] writes for those values.
    pub fn seek_prefix(&self, images: &[u128]) -> PkBuf {
        let k = images.len();
        debug_assert!(
            k >= 1 && k <= self.n as usize,
            "seek_prefix: one image per leading spec column"
        );
        gnitz_wire::encode_pk_images(
            self.cols[..k]
                .iter()
                .zip(images)
                .map(|(c, &v)| (c.loc.type_code(), c.out.type_code, v)),
        )
    }

    /// The half-open OPK key range `[start, end)` for `range` over this key
    /// space, each key exactly `stride` bytes (the leading span plus, for a
    /// secondary index, the source-PK suffix).
    ///
    /// `None` = provably empty.
    pub(crate) fn range_keys(&self, stride: usize, range: &KeyRange) -> Option<(PkBuf, Option<PkBuf>)> {
        debug_assert!(
            range.cols().as_slice().len() <= self.n as usize,
            "range_keys: the range lists more columns than the key space"
        );
        let eq = range.eq_vals();
        let n_eq = eq.len();
        let mut images = [0u128; MAX_PK_COLUMNS];
        images[..n_eq].copy_from_slice(eq);
        images[n_eq] = range.start.image;
        let start = self.seek_prefix(&images[..=n_eq]);
        images[n_eq] = range.end.image;
        let end = self.seek_prefix(&images[..=n_eq]);
        key_range_between_cuts(
            KeyCut::new(start.pk_bytes(), range.start.after),
            KeyCut::new(end.pk_bytes(), range.end.after),
            stride,
        )
    }
}

// ---------------------------------------------------------------------------
// Byte successor / predecessor and cut → key-range derivation
// ---------------------------------------------------------------------------

/// Fixed-width byte-string successor: `p + 1` with carry, in place. Returns
/// `false` when `p` is all-`0xFF` (or empty) — no successor exists at this width
/// (carry-out), which a caller reads as `+∞` (scan to the table end, or a
/// provably-empty start).
fn increment_key_in_place(p: &mut [u8]) -> bool {
    for b in p.iter_mut().rev() {
        *b = b.wrapping_add(1);
        if *b != 0 {
            return true;
        }
    }
    false
}

/// Fixed-width byte-string predecessor: `p - 1` with borrow, in place — the
/// mirror of [`increment_key_in_place`]. An all-zero `p` borrows out and wraps to
/// all-`0xFF`; the sole caller never passes one.
fn decrement_key_in_place(p: &mut [u8]) {
    for b in p.iter_mut().rev() {
        *b = b.wrapping_sub(1);
        if *b != 0xFF {
            return;
        }
    }
}

/// A cut in the OPK key space, named by an OPK group prefix: either that
/// group's own minimum key or the first key above every member of it. Zero-
/// padding the prefix to the key width is what makes it the group's minimum.
#[derive(Clone, Copy)]
pub(crate) struct KeyCut<'a> {
    group: &'a [u8],
    above: bool,
}

impl<'a> KeyCut<'a> {
    /// [`Self::above`] when `above`, else [`Self::min_of`].
    pub(crate) fn new(group: &'a [u8], above: bool) -> Self {
        KeyCut { group, above }
    }

    /// The group's own minimum key — below every member of it.
    pub(crate) fn min_of(group: &'a [u8]) -> Self {
        KeyCut::new(group, false)
    }

    /// The first key above every member of the group. A saturated group — and
    /// the zero-width group, which is the whole key space — has none.
    pub(crate) fn above(group: &'a [u8]) -> Self {
        KeyCut::new(group, true)
    }

    /// This cut as a `stride`-wide key; `None` when it lies above the whole key
    /// space. The successor's carry ripples into the equality prefix, landing
    /// exactly on the first key of the next equality group.
    fn key(&self, stride: usize) -> Option<PkBuf> {
        let mut k = PkBuf::from_bytes(self.group);
        let exists = !self.above || k.write(k.width(), increment_key_in_place);
        exists.then(|| k.widened(stride))
    }
}

/// Half-open `[start, end)` OPK key range between two cuts over a `stride`-byte
/// key space.
///
/// `None` = provably empty: `start` lies above the whole key space, or
/// `start >= end`. `end == None` inside `Some` means the range runs to the table
/// end.
pub(crate) fn key_range_between_cuts(start: KeyCut, end: KeyCut, stride: usize) -> Option<(PkBuf, Option<PkBuf>)> {
    let start = start.key(stride)?;
    let end = end.key(stride);
    if end.as_ref().is_some_and(|e| start.pk_bytes() >= e.pk_bytes()) {
        return None;
    }
    Some((start, end))
}

/// Whether every key of a non-empty band `[start, end)` shares its leading `prefix`
/// bytes — the bytes a worker owner hashes. OPK order is byte order, so the first and
/// last keys decide; a band with no `end` runs to the all-`0xFF` key.
fn range_shares_prefix(start: &PkBuf, end: Option<&PkBuf>, prefix: usize) -> bool {
    let last = match end {
        Some(e) => {
            let mut l = *e;
            l.write(l.width(), decrement_key_in_place);
            l
        }
        None => PkBuf::max(start.width()),
    };
    start.pk_bytes()[..prefix] == last.pk_bytes()[..prefix]
}

impl SchemaDescriptor {
    /// The one worker that can answer `range`, when one provably can. Any worker
    /// answers an empty range, so worker 0 does.
    pub fn confined_worker(&self, range: &KeyRange, num_workers: usize) -> Option<usize> {
        if !range.walks_pk(self.pk_cols()) {
            return None;
        }
        let Some((start, end)) = self.pk_range_keys(range) else {
            return Some(0);
        };
        if !self.placement().is_key_routed() {
            return None;
        }
        range_shares_prefix(&start, end.as_ref(), self.dist_stride())
            .then(|| self.worker_for_pk(start.pk_bytes(), num_workers))
    }

    /// The OPK key band `r` names over this schema's whole PK list; `None` when it
    /// names no key.
    pub fn pk_range_keys(&self, r: &KeyRange) -> Option<(PkBuf, Option<PkBuf>)> {
        KeySpec::for_pk(self).range_keys(self.pk_stride(), r)
    }
}

#[cfg(test)]
#[path = "tests/key.rs"]
mod tests;
