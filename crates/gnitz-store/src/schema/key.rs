//! Order-preserving primary-key (OPK) primitives.
//!
//! These pure layout/key operations sit *below* both `schema` and `storage`:
//! they encode a PK region — a whole one, a wire seek key, or an index's
//! leading-column span — to its order-preserving big-endian
//! image, compare two such images with a raw `memcmp`, pack a narrow region
//! into a sort key, carry a width-tagged PK byte buffer, and derive the
//! half-open key range a `KeyRange`'s cut pair denotes — and compose the
//! two multi-column OPK keys: a secondary index's leading span
//! ([`IndexKeySpec`]) and a reindex's synthetic PK
//! ([`ReindexPacker`](crate::schema::key::ReindexPacker)). None of them reaches
//! up into storage — the dependency runs `storage → schema::key`,
//! the legitimate downward direction. This module is the one import path: every
//! caller, storage included, names `crate::schema::key::X`.
//!
//! The per-column codec and tuple encoders are `gnitz_wire::pk`'s, shared with the
//! client; this module composes schema-typed and row-sourced keys from them.

use std::cell::Cell;
use std::cmp::Ordering;

use gnitz_expr::RowSource;
use gnitz_wire::{KeyRange, NARROW_PK_MAX_BYTES};

use crate::schema::{
    ColumnLocator, DerivedSchema, OpBuildErr, SchemaBound, SchemaColumn, SchemaDescriptor, SchemaFacts, TypeCode,
    MAX_PK_BYTES, MAX_PK_COLUMNS,
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
// Order-preserving PK encoder
// ---------------------------------------------------------------------------

/// A wire seek key (packed native-LE PK columns) as the OPK a PK region holds. Only
/// the PK's own stride is read, so a scalar key may arrive as a 16-byte word.
pub fn seek_opk_bytes(schema: &SchemaDescriptor, key: &[u8]) -> Result<PkBuf, String> {
    let stride = schema.pk_stride();
    let key = key
        .get(..stride)
        .ok_or_else(|| format!("key of {} bytes is shorter than the {stride}-byte PK", key.len()))?;
    Ok(schema.opk_key(key))
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
/// schema — the packer's string and float arms, `AviBake::entry` — is
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

/// One indexed column: where the owner-side value lives (`locate` — a PK source
/// is sliced from the OPK region, a payload column read from its dense slot),
/// and the promoted key column it encodes at, whose `size()` is the slot width.
#[derive(Clone, Copy)]
struct IndexKeyCol {
    loc: ColumnLocator,
    out: SchemaColumn,
}

impl IndexKeyCol {
    /// Unused slots of the fixed array: the schema layer's designated padding
    /// column and locator. Nothing reads past `n`.
    const EMPTY: IndexKeyCol = IndexKeyCol {
        loc: ColumnLocator::EMPTY,
        out: SchemaColumn::EMPTY,
    };

    fn new(loc: ColumnLocator, out: SchemaColumn) -> Self {
        IndexKeyCol { loc, out }
    }
}

/// The single definition of "what key do these columns map to": byte-equal ⟺
/// value-equal at any width, byte-lexicographic order is the seek/merge order.
/// Built once per circuit, `Copy`, so the row paths allocate nothing.
/// [`Self::new`] is a secondary index's span, `Self::for_pk` a base table's
/// own PK under the identity promotion.
#[derive(Clone, Copy)]
pub struct IndexKeySpec {
    n: u8,
    /// Sum of the promoted column widths, so the row path re-sums nothing.
    key_size: u8,
    /// Sized by `MAX_PK_COLUMNS`, not the wire's `PK_LIST_MAX_COLS`: `for_pk`
    /// may be handed an index schema, whose PK arity reaches the engine limit.
    cols: [IndexKeyCol; MAX_PK_COLUMNS],
}

impl IndexKeySpec {
    /// The span of a secondary index on `cols` of `owner`, and the **one**
    /// promotion of an indexed column's type — `Self::output_schema` reads the
    /// promoted columns back off this spec, so the entry bytes and the schema
    /// they land in are one derivation.
    ///
    /// `Err` on an arity outside `1..=MAX_PK_COLUMNS`, an out-of-range column, a
    /// type no index key carries, or a record over the PK arity/stride limits —
    /// the last three through [`gnitz_wire::index_key_types`], shared with the
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
                return Err(format!(
                    "Index: column index {c} out of bounds (columns={})",
                    owner.num_columns(),
                ));
            }
            col_types.push(owner.columns[c as usize].type_code);
        }
        let promoted = gnitz_wire::index_key_types(&col_types, owner.pk_indices().len(), owner.pk_stride())
            .map_err(|r| r.to_string())?;
        let mut spec = IndexKeySpec {
            n: cols.len() as u8,
            key_size: 0,
            cols: [IndexKeyCol::EMPTY; MAX_PK_COLUMNS],
        };
        for (i, (&c, &t)) in cols.iter().zip(&promoted).enumerate() {
            let out = SchemaColumn::new(t, false);
            spec.cols[i] = IndexKeyCol::new(owner.locate(c as usize), out);
            spec.key_size += out.size();
        }
        Ok(spec)
    }

    /// The index schema this spec's entries land in: the promoted key columns in
    /// declared order, then `source`'s PK columns, all in the PK with zero
    /// payload. Built from the promoters [`Self::write_span`] encodes through, so
    /// the schema's leading-key width *is* [`Self::key_size`]. `Err` only on the
    /// limits [`Self::new`] has already checked.
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
        let mut spec = IndexKeySpec {
            n: schema.pk_indices().len() as u8,
            key_size: schema.pk_stride() as u8,
            cols: [IndexKeyCol::EMPTY; MAX_PK_COLUMNS],
        };
        for (i, (ci, col)) in schema.pk_columns().enumerate() {
            spec.cols[i] = IndexKeyCol::new(schema.locate(ci), *col);
        }
        spec
    }

    /// Span width in bytes (`idx_key_size`) — the sum of the promoted widths.
    #[inline]
    pub fn key_size(&self) -> usize {
        self.key_size as usize
    }

    /// Write one row's leading-key span into `dst[..key_size()]` — the bytes
    /// [`Self::seek_prefix`] and `ReindexPacker::pack_into` produce for the same values.
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
        if !range.walks_pk(self.pk_indices()) {
            return None;
        }
        let Some((start, end)) = IndexKeySpec::for_pk(self).range_keys(self.pk_stride(), range) else {
            return Some(0);
        };
        if !self.placement().is_key_routed() {
            return None;
        }
        range_shares_prefix(&start, end.as_ref(), self.dist_stride())
            .then(|| self.worker_for_pk(start.pk_bytes(), num_workers))
    }
}

// ---------------------------------------------------------------------------
// Row-content key material — the hashed key bytes, for the slots no scalar OPK
// encode can produce: a string column's content and the group key's fold slot.
// ---------------------------------------------------------------------------

/// Column `c` of `schema` as a key column, `what` naming the key in a refusal.
/// A float has none: `+0.0` and `-0.0` differ byte-wise but compare equal.
pub(crate) fn locate_key_col(schema: &SchemaDescriptor, c: u32, what: &str) -> Result<ColumnLocator, OpBuildErr> {
    let loc = schema
        .try_locate(c as usize)
        .ok_or_else(|| OpBuildErr::oob_col(&format!("{what}: column"), c, schema))?;
    if loc.type_code().is_float() {
        return Err(OpBuildErr::shape(format!(
            "{what}: column {c} is a float, which has no order-preserving key image"
        )));
    }
    Ok(loc)
}

/// Append one column's key bytes to `buf`: a null marker, then a German string's
/// content behind its 4-byte LE length — which keeps "ab"+"c" from aliasing
/// "a"+"bc" — or the value's `opk_image`, so a payload FK keys like the same
/// value stored as a PK column.
#[inline]
fn push_col_key<R: RowSource>(buf: &mut Vec<u8>, src: &R, row: usize, null_word: u64, loc: ColumnLocator) {
    if loc.is_null_word(null_word) {
        buf.push(0);
        return;
    }
    buf.push(1);
    match loc {
        ColumnLocator::Payload { slot, size, type_code } if type_code.is_german_string() => {
            let content =
                gnitz_wire::german_string_content(src.get_col_ptr(row, slot as usize, size as usize), src.blob());
            buf.extend_from_slice(&(content.len() as u32).to_le_bytes());
            buf.extend_from_slice(content);
        }
        _ => buf.extend_from_slice(&loc.opk_image(src, row).to_le_bytes()),
    }
}

thread_local! {
    /// `key_row`'s buffer for a fold its stack buffer cannot hold.
    static FOLD_SCRATCH: Cell<Vec<u8>> = const { Cell::new(Vec::new()) };
}

/// Columns a fold assembles on the stack: the PK arity, which every real group
/// set fits.
const FOLD_INLINE_COLS: usize = MAX_PK_COLUMNS;

/// The columns one 128-bit row digest folds, and whether their key bytes fit
/// the stack buffer.
pub(crate) struct FoldCols {
    locs: Vec<ColumnLocator>,
    /// Every column is fixed-width, and there are at most `FOLD_INLINE_COLS`.
    inline: bool,
}

impl FoldCols {
    pub(crate) fn new(locs: Vec<ColumnLocator>) -> Self {
        let inline = locs.len() <= FOLD_INLINE_COLS && !locs.iter().any(|l| l.type_code().is_german_string());
        FoldCols { locs, inline }
    }

    #[inline]
    pub(crate) fn is_empty(&self) -> bool {
        self.locs.is_empty()
    }

    /// The 128-bit XXH3 digest of these columns' key bytes over `row`.
    #[inline]
    pub(crate) fn key_row<R: RowSource>(&self, src: &R, row: usize, null_word: u64) -> u128 {
        if self.inline {
            // `push_col_key`'s bytes, written to the stack.
            let mut buf = [0u8; 17 * FOLD_INLINE_COLS];
            let mut n = 0usize;
            for &loc in &self.locs {
                if loc.is_null_word(null_word) {
                    buf[n] = 0;
                    n += 1;
                    continue;
                }
                buf[n] = 1;
                buf[n + 1..n + 17].copy_from_slice(&loc.opk_image(src, row).to_le_bytes());
                n += 17;
            }
            return gnitz_wire::checksum_128(&buf[..n]);
        }
        self.key_row_scratch(src, row, null_word)
    }

    /// [`Self::key_row`] through the thread-local scratch, out of line so the
    /// stack arm stays small enough to inline into its callers.
    #[inline(never)]
    fn key_row_scratch<R: RowSource>(&self, src: &R, row: usize, null_word: u64) -> u128 {
        FOLD_SCRATCH.with(|cell| {
            let mut buf = cell.take();
            buf.clear();
            for &loc in &self.locs {
                push_col_key(&mut buf, src, row, null_word, loc);
            }
            let key = gnitz_wire::checksum_128(&buf);
            cell.set(buf);
            key
        })
    }
}

// ---------------------------------------------------------------------------
// ReindexPacker — the synthetic-key composer
// ---------------------------------------------------------------------------

/// Synthetic-PK / routing key for a German-string column's content. Both the
/// reindex Map (setting a row's `_join_pk`) and the exchange scatter (routing the
/// raw delta) reach it through the packer's `String` arm, so a string join key
/// scatters to the worker that owns its own `_join_pk` partition. Empty content —
/// including a NULL string, a zeroed German-string struct — hashes to 0.
#[inline]
pub(crate) fn german_string_promote_key(struct_bytes: &[u8], blob: &[u8]) -> u128 {
    let content = gnitz_wire::german_string_content(struct_bytes, blob);
    if content.is_empty() {
        return 0; // NULL / empty-string sentinel
    }
    // A true 128-bit content hash. A 64-bit hash widened to 128 bits would carry
    // only 2^64 of entropy — a ~2^32-row birthday bound past which two distinct
    // strings collide to one `_join_pk` and the join's OPK byte-compare silently
    // equijoins them.
    gnitz_wire::checksum_128(content)
}

// ---------------------------------------------------------------------------
// Packed group key
// ---------------------------------------------------------------------------

/// The two slots a packed group key carries besides its columns: a leading
/// presence bitmap and a trailing overflow fold. Every width below is read back
/// off these, so the schema, the stride and the pack offsets cannot disagree.
const BITMAP_COL: SchemaColumn = SchemaColumn::new(TypeCode::U8, false);
const FOLD_COL: SchemaColumn = SchemaColumn::new(TypeCode::U128, false);
const BITMAP_BYTES: usize = BITMAP_COL.size() as usize;
const FOLD_BYTES: usize = FOLD_COL.size() as usize;
// `pack_into` writes the bitmap as one bare byte and the fold as a `u128`'s
// big-endian image, which are those columns' OPK images only at these widths.
const _: () = assert!(BITMAP_BYTES == 1 && FOLD_BYTES == 16);

/// How one source column's bytes become key bytes. The group key's bitmap and
/// fold are facts of the whole [`ReindexPacker`], not columns, and live there.
#[derive(Clone, Copy)]
enum PromoteKind {
    /// Any scalar source column, at either width and either sign: the locator
    /// says which region holds the bytes, and the encode differs only by that.
    /// Never a float — both constructors reject one, because a reindex key is
    /// compared as raw OPK bytes and no IEEE-754 image survives that.
    Col(ColumnLocator),
    /// STRING/BLOB payload: sign-agnostic XXH3 content-hash key. The only source
    /// that is not a scalar cell the OPK encoders can consume.
    String(ColumnLocator),
}

/// Per-column classifier for "read a source column, project it to OPK PK
/// bytes". Runs once per column at construction; the resulting `PromoteKind` is
/// stored on the `ColPromoter`, and the per-row work is the read + OPK encode
/// `ReindexPacker::pack_into` performs.
fn classify_promote(loc: ColumnLocator) -> PromoteKind {
    match loc {
        // BLOB shares the 16-byte German-string struct layout with STRING, so it
        // takes the same hash path rather than a raw cell encode. Neither can be
        // a PK column, so only the payload arm needs the test.
        ColumnLocator::Payload { type_code, .. } if type_code.is_german_string() => PromoteKind::String(loc),
        _ => PromoteKind::Col(loc),
    }
}

/// One key column of a `ReindexPacker`: the output PK column it packs into —
/// resolved once at construction, and its `size()` is the slot width, so the
/// packed bytes and [`ReindexPacker::output_schema`] read one value rather than
/// two — plus the `PromoteKind` that says where the source bytes come from.
#[derive(Clone, Copy)]
struct ColPromoter {
    out_col: SchemaColumn,
    /// Group-key mode: the **source** column is nullable, so a NULL zeroes the
    /// slot and the presence bitmap carries the NULL-ness instead. A stale or
    /// arbitrary cell under a NULL would otherwise split one group in two.
    /// Never set on a join key — those are NULL-gated upstream.
    nullable: bool,
    kind: PromoteKind,
}

impl ColPromoter {
    /// Unused slots of the fixed `cols` array, matching what `IndexKeySpec::new`
    /// fills its own with: the schema layer's designated padding column and a
    /// zeroed PK locator.
    const PLACEHOLDER: ColPromoter = ColPromoter {
        out_col: SchemaColumn::EMPTY,
        nullable: false,
        kind: PromoteKind::Col(ColumnLocator::EMPTY),
    };

    /// A slot packing into a `out_tc` output PK column. The one place the output
    /// column is spelled, because it is never nullable while `nullable` — the
    /// *source* column's — routinely is.
    pub(crate) fn new(out_tc: TypeCode, nullable: bool, kind: PromoteKind) -> Self {
        ColPromoter {
            out_col: SchemaColumn::new(out_tc, false),
            nullable,
            kind,
        }
    }
}

/// Packs a reindex column list into a contiguous OPK PK region. The same packer
/// drives both the reindex map (which writes the synthetic `_join_pk` at
/// emission — `expr::MapPlan`'s `PkSource::Pack`) and the exchange scatter
/// (which routes the raw delta by the same key), so the reindexed trace side and
/// the delta scatter side co-partition byte-for-byte at every key arity and
/// width.
pub struct ReindexPacker {
    cols: [ColPromoter; MAX_PK_COLUMNS], // first `num_cols` valid
    num_cols: usize,
    pub(crate) out_stride: usize,
    /// Group-key only: a leading `U8` slot, bit *i* set iff packed column *i* is
    /// NULL. Without it a NULL group and a `0` group collide on one output PK.
    has_bitmap: bool,
    /// Group columns past the packed prefix, hashed into a trailing 16-byte
    /// fold slot. Empty for a join key and for a group key with no fold.
    fold: FoldCols,
}

impl ReindexPacker {
    /// Build per-column promoters from the reindex column list (key order),
    /// tightly packed with no inter-column padding. The **only** derivation of
    /// that layout: [`Self::output_schema`] reads these same promoters, so the
    /// two cannot disagree per slot while still agreeing on the total stride —
    /// which would silently stop equal keys co-partitioning.
    ///
    /// The whole trust boundary for a key list off the wire: a forged circuit is
    /// rejected, never panicked on.
    ///
    /// A carried target's domain is `join_key_common_type`'s codomain — the
    /// *key* domain, whose collapse to U128 is what a `_join_pk` slot does to a
    /// UUID pair, not the value domain `TypeCode::int_domain_fits` answers.
    pub(crate) fn new(schema: &SchemaDescriptor, key: &[gnitz_wire::ReindexSlot]) -> Result<Self, OpBuildErr> {
        if key.len() > MAX_PK_COLUMNS {
            return Err(OpBuildErr::shape(format!(
                "reindex key: {} columns exceeds the {MAX_PK_COLUMNS}-column PK limit",
                key.len()
            )));
        }
        let mut cols = [ColPromoter::PLACEHOLDER; MAX_PK_COLUMNS];
        let mut stride = 0usize;
        for (i, &(c, carried)) in key.iter().enumerate() {
            let loc = locate_key_col(schema, c, "reindex key")?;
            if carried.is_some_and(|t| loc.type_code().join_key_common_type(t) != Some(t)) {
                return Err(OpBuildErr::shape(format!(
                    "reindex key: column {c} does not promote to the carried target"
                )));
            }
            let kind = classify_promote(loc);
            // Carried promotion target (`None` = self-derive); the slot type and
            // width follow `resolve_reindex_type` so the scatter packer and the
            // trace-side reindex Map derive identical widths.
            let cp = ColPromoter::new(gnitz_wire::resolve_reindex_type(loc.type_code(), carried), false, kind);
            // A payload slot right-aligns its source, so one narrower than the
            // source would truncate it. `resolve_reindex_type` never derives that;
            // debug-only because it is a type-system invariant, not input.
            debug_assert!(
                cp.out_col.size() as usize >= loc.type_code().wire_stride()
                    || !matches!(kind, PromoteKind::Col(ColumnLocator::Payload { .. }))
            );
            stride += cp.out_col.size() as usize;
            cols[i] = cp;
        }
        if stride > MAX_PK_BYTES {
            return Err(OpBuildErr::shape(format!(
                "reindex key: {stride} PK bytes exceeds the {MAX_PK_BYTES}-byte limit"
            )));
        }
        Ok(ReindexPacker {
            cols,
            num_cols: key.len(),
            out_stride: stride,
            has_bitmap: false,
            fold: FoldCols::new(Vec::new()),
        })
    }

    /// The output PK columns this packer's bytes fill, in key order. The
    /// promoters are the only derivation of that layout, so a schema built from
    /// these describes what `pack_into` writes by construction.
    pub(crate) fn key_columns(&self) -> impl Iterator<Item = SchemaColumn> + '_ {
        let bitmap = self.has_bitmap.then_some(BITMAP_COL);
        let fold = (!self.fold.is_empty()).then_some(FOLD_COL);
        bitmap
            .into_iter()
            .chain(self.cols[..self.num_cols].iter().map(|cp| cp.out_col))
            .chain(fold)
    }

    /// The reindex Map's output schema: the packer's own [`Self::key_columns`]
    /// — so this schema's stride and `out_stride` are the same sum — then
    /// `in_schema.columns[payload_cols[i]]`. `payload_cols` is what the reindex
    /// program copies, so a join side skipping a dead column stops persisting it.
    ///
    /// The PK-side bounds are `new`'s; `payload_cols` is bounded here.
    pub(crate) fn output_schema(
        &self,
        in_schema: &SchemaDescriptor,
        payload_cols: &[u32],
    ) -> Result<SchemaDescriptor, OpBuildErr> {
        let over = |e| OpBuildErr::shape(format!("reindex map: output {e}"));
        let mut b = DerivedSchema::new();
        for c in self.key_columns() {
            b.push_pk(c).map_err(over)?;
        }
        for &c in payload_cols {
            let col = in_schema
                .column(c as usize)
                .ok_or_else(|| OpBuildErr::oob_col("reindex map: payload column", c, in_schema))?;
            b.push(col).map_err(over)?;
        }
        Ok(b.finish())
    }

    /// Build the packer for a **group** key over `group_cols`, and the PK region
    /// of an index keyed by it: [`Self::key_columns`], then `suffix`.
    ///
    /// Greedy: pack leading columns while the budget still leaves room for the
    /// fold slot the rest would need. Unlike a join key this is total over
    /// *arity* — the overflow folds into one hash slot — so no group set is
    /// refused for being wide; it refuses what [`locate_key_col`] refuses.
    pub(crate) fn new_group_key(
        schema: &SchemaDescriptor,
        group_cols: &[u32],
        suffix: &[SchemaColumn],
    ) -> Result<(Self, DerivedSchema), OpBuildErr> {
        let max_cols = MAX_PK_COLUMNS - suffix.len();
        let max_bytes = MAX_PK_BYTES - suffix.iter().map(|c| c.size() as usize).sum::<usize>();
        assert!(
            max_cols >= 2 && max_bytes >= BITMAP_BYTES + FOLD_BYTES,
            "a group-key suffix must leave room for a bitmap byte and a fold slot",
        );
        assert!(max_cols <= 9, "the one bitmap byte addresses at most 8 packed columns");
        let group: Vec<(SchemaColumn, ColumnLocator)> = group_cols
            .iter()
            .map(|&c| {
                let loc = locate_key_col(schema, c, "group key")?;
                Ok((schema.columns[c as usize], loc))
            })
            .collect::<Result<_, OpBuildErr>>()?;
        let has_bitmap = group.iter().any(|(col, _)| col.nullable);

        // The bitmap occupies one leading slot, so both budgets start spent by it.
        let lead = usize::from(has_bitmap);
        let mut cols = [ColPromoter::PLACEHOLDER; MAX_PK_COLUMNS];
        let mut stride = lead * BITMAP_BYTES;
        let mut n_packed = 0usize;
        for (i, &(col, loc)) in group.iter().enumerate() {
            // A group column takes the same slot a join key's would; the float
            // arm the two policies would differ on returned above.
            let out_tc = col.type_code.reindex_output_type();
            let w = out_tc.wire_stride();
            // Room this column needs, plus the fold slot the columns behind it
            // would still require. Reserving it here is what keeps the greedy
            // walk from packing a column it would have to give back.
            let tail_cols = usize::from(i + 1 < group.len());
            if lead + n_packed + 1 + tail_cols > max_cols || stride + w + tail_cols * FOLD_BYTES > max_bytes {
                break;
            }
            cols[n_packed] = ColPromoter::new(out_tc, col.nullable, classify_promote(loc));
            stride += w;
            n_packed += 1;
        }
        let fold = FoldCols::new(group[n_packed..].iter().map(|&(_, loc)| loc).collect());
        stride += if fold.is_empty() { 0 } else { FOLD_BYTES };

        let packer = ReindexPacker {
            cols,
            num_cols: n_packed,
            out_stride: stride,
            has_bitmap,
            fold,
        };
        let mut b = DerivedSchema::new();
        for c in packer.key_columns().chain(suffix.iter().copied()) {
            b.push_pk(c)
                .expect("a group key packed inside the suffix's budget, plus the suffix, is non-null PK-eligible");
        }
        Ok((packer, b))
    }

    /// Pack the full reindex key (`out_stride` OPK bytes) for `row` into `dst`.
    ///
    /// One pass over the source columns, between the two key-level slots a
    /// group key carries: the leading presence bitmap, whose bits are the NULL
    /// tests the packed slots already perform, and the trailing fold.
    /// [`Self::pack_into`] over the leading `out_stride` bytes of `buf`,
    /// returning them — the prefix a group-keyed secondary index seeks by, so the
    /// key's width is read off the packer rather than re-sliced per index.
    #[inline]
    pub(crate) fn pack_prefix<'a, R: RowSource>(&self, buf: &'a mut [u8], batch: &R, row: usize) -> &'a [u8] {
        let n = self.out_stride;
        self.pack_into(&mut buf[..n], batch, row);
        &buf[..n]
    }

    #[inline]
    pub(crate) fn pack_into<R: RowSource>(&self, dst: &mut [u8], batch: &R, row: usize) {
        let null_word = batch.get_null_word(row);
        let mut off = usize::from(self.has_bitmap) * BITMAP_BYTES;
        let mut null_bits = 0u8;
        for (i, cp) in self.cols[..self.num_cols].iter().enumerate() {
            let w = cp.out_col.size() as usize;
            let slot = &mut dst[off..off + w];
            off += w;
            match cp.kind {
                // A NULL packed column: zeroed slot, and its bit in the bitmap.
                PromoteKind::Col(loc) | PromoteKind::String(loc) if cp.nullable && loc.is_null_word(null_word) => {
                    null_bits |= 1 << i;
                    slot.fill(0);
                }
                PromoteKind::Col(loc) => loc.encode_opk_promoted(batch, row, cp.out_col.type_code, slot),
                PromoteKind::String(loc) => {
                    let h = german_string_promote_key(loc.bytes(batch, row), batch.blob());
                    slot.copy_from_slice(&h.to_be_bytes());
                }
            }
        }
        // Both unconditional per row for the keys that have them, so every slot
        // of `dst` is fully overwritten — which is what lets a caller reuse one
        // destination across rows with no inter-row clear.
        if self.has_bitmap {
            dst[0] = null_bits;
        }
        if !self.fold.is_empty() {
            let h = self.fold.key_row(batch, row, null_word);
            dst[off..off + FOLD_BYTES].copy_from_slice(&h.to_be_bytes());
        }
    }
}

#[cfg(test)]
#[path = "tests/key.rs"]
mod tests;
