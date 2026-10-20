//! Order-preserving primary-key (OPK) primitives.
//!
//! These pure layout/key operations compare two OPK regions in byte order,
//! pack a region into a fixed-width sort key, derive the half-open key band a
//! `KeyRange` walks over a store's PK, and compose a multi-column key span
//! ([`KeySpec`]). The per-column codec is `gnitz_wire::pk`'s, shared with the
//! client.
use std::cmp::Ordering;

use gnitz_wire::RowSource;
use gnitz_wire::{KeyRange, PkBuf, PkListRole, NARROW_PK_MAX_BYTES};

use crate::schema::{ColumnLocator, DerivedSchema, SchemaColumn, SchemaDescriptor, TypeCode, MAX_PK_COLUMNS};

/// Typed lexicographic OPK ordering of two **equal-length** PK regions — the
/// comparator of seeks, group walks and probes. Slice order
/// at every width, settled on the leading-16 `pack_pk_be` image where that
/// decides: for `len ≤ 16` the image is the *whole* PK and is injective, so a
/// tie is already a byte-equal PK; only `len > 16` can tie on the prefix while
/// differing later, and the slice compare runs only there.
///
/// Equal-width operands use this or [`pk_bytes_eq`]; anything else is slice order.
#[inline(always)]
pub(crate) fn compare_pk_ordering(a: &[u8], b: &[u8]) -> Ordering {
    debug_assert_eq!(a.len(), b.len(), "compare_pk_ordering on unequal PK widths");
    match pack_pk_be(a).cmp(&pack_pk_be(b)) {
        Ordering::Equal if a.len() > NARROW_PK_MAX_BYTES => a.cmp(b),
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

/// The **inclusive** OPK ranges `[min, max]` and `[lo, hi]` intersect — whether a
/// run or shard holding `[min, max]` can answer about a key the caller's bound
/// admits. A half-open upper bound passed as `hi` over-approximates, which is
/// the safe direction.
#[inline]
pub fn pk_ranges_overlap(min: &[u8], max: &[u8], lo: &[u8], hi: &[u8]) -> bool {
    max >= lo && min <= hi
}

// ---------------------------------------------------------------------------
// Narrow-region PK key packing
// ---------------------------------------------------------------------------

/// BE sort-key packer over an OPK region. Left-aligns the bytes at the MSB end
/// of a `u128` and reads big-endian, so `pack_pk_be(a).cmp(&pack_pk_be(b))`
/// equals the lexicographic byte order of the OPK regions. Narrow (`len ≤ 16`) =
/// the exact key; wide (`len > 16`) = the order-preserving leading-16 prefix
/// (authoritative whenever two prefixes differ; a prefix collision needs a
/// slice-order tiebreak).
///
/// The `{2, 4, 8, ≥16}` arms load the dominant scalar and `U128`/wide-prefix
/// widths straight into a register, value-equal to the pad-and-copy (a narrow
/// value occupies the high bits, the low bits zero; `≥16` reads the
/// order-preserving leading 16 bytes). The remaining arms compose the same
/// fixed-width loads: none copies through a runtime length, which would be an
/// out-of-line `memcpy` per key. Kept apart from `gnitz_wire::widen_pk_be`'s
/// right-aligned image, which retires more instructions per compare at 8, 12
/// and 16 bytes.
///
/// NOT a value accessor — for a U64 OPK value 1 (`[0,…,0,1]` at `[..8]`) this
/// packs as `1·2^64`, not 1.
#[inline(always)]
pub(crate) fn pack_pk_be(pk_bytes: &[u8]) -> u128 {
    match pk_bytes.len() {
        8 => (u64::from_be_bytes(pk_bytes[..8].try_into().unwrap()) as u128) << 64,
        len if len >= 16 => u128::from_be_bytes(pk_bytes[..16].try_into().unwrap()),
        // Two overlapping loads: the shift drops the tail load's bytes below 8.
        len @ 9..=15 => {
            let hi = u64::from_be_bytes(pk_bytes[..8].try_into().unwrap()) as u128;
            let tail = u64::from_be_bytes(pk_bytes[len - 8..len].try_into().unwrap());
            (hi << 64) | (tail << (8 * (16 - len))) as u128
        }
        4 => (u32::from_be_bytes(pk_bytes[..4].try_into().unwrap()) as u128) << 96,
        2 => (u16::from_be_bytes(pk_bytes[..2].try_into().unwrap()) as u128) << 112,
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

/// A whole-PK sort key in the slice order of the bytes it was built from, so a
/// key sort needs no PK-byte tiebreak. Keys compare only
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
        if opk.len() >= 8 {
            u64::from_be_bytes(opk[..8].try_into().unwrap())
        } else {
            // `pack_pk_be` left-aligns a narrow key with register loads at 2 and 4.
            (pack_pk_be(opk) >> 64) as u64
        }
    }
}

impl PkSortKey<'_> for u128 {
    #[inline(always)]
    fn from_opk(opk: &[u8]) -> u128 {
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
        // The leading 16 bytes, then the trailing 1..=16 left-aligned: array `Ord`
        // is lexicographic, as byte order is.
        let hi = u128::from_be_bytes(opk[..16].try_into().unwrap());
        if opk.len() == 32 {
            [hi, u128::from_be_bytes(opk[16..32].try_into().unwrap())]
        } else {
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

/// A key span: columns at their own types, packed tightly in order. `Copy`, so
/// the row paths allocate nothing.
#[derive(Clone, Copy)]
pub struct KeySpec {
    n: u8,
    /// Sum of the column widths, so the row path re-sums nothing.
    key_size: u8,
    locs: [ColumnLocator; MAX_PK_COLUMNS],
}

impl KeySpec {
    /// The span's columns, in order.
    pub(crate) fn locators(&self) -> &[ColumnLocator] {
        &self.locs[..self.n as usize]
    }

    /// The span of a secondary index on `cols` of `owner`.
    ///
    /// `Err` where `cols` breaks `gnitz_wire::validate_index_key`.
    pub fn new(cols: &[u32], owner: &SchemaDescriptor) -> Result<Self, String> {
        gnitz_wire::validate_index_key(cols, owner.num_columns(), owner.pk_cols().len(), |c| {
            owner.columns()[c as usize].type_code
        })
        .map_err(|rule| format!("Index: {}", rule.for_role(PkListRole::ColumnList)))?;
        // Padding for the fixed array's unused slots; nothing reads past `n`.
        const EMPTY: ColumnLocator = ColumnLocator::Pk {
            byte_off: 0,
            size: 0,
            type_code: TypeCode::U8,
        };
        let mut spec = KeySpec {
            n: 0,
            key_size: 0,
            locs: [EMPTY; MAX_PK_COLUMNS],
        };
        for &c in cols {
            let loc = owner.locate(c as usize);
            spec.locs[spec.n as usize] = loc;
            spec.n += 1;
            spec.key_size += loc.size() as u8;
        }
        Ok(spec)
    }

    /// The index schema this spec's entries land in: the key columns, then
    /// `source`'s PK columns, all in the PK.
    pub fn index_schema(&self, source: &SchemaDescriptor) -> SchemaDescriptor {
        let mut b = self.key_columns();
        b.push_pk_of(source);
        b.finish().expect("KeySpec::new bounds the index schema")
    }

    /// The key columns alone, all in the PK: one row's PK region is one span.
    pub fn span_schema(&self) -> SchemaDescriptor {
        self.key_columns()
            .finish()
            .expect("KeySpec::new admits its key columns")
    }

    fn key_columns(&self) -> DerivedSchema {
        let mut b = DerivedSchema::new();
        self.locators()
            .iter()
            .for_each(|loc| b.push_pk(SchemaColumn::new(loc.type_code(), false)));
        b
    }

    /// Span width in bytes — the sum of the column widths.
    #[inline]
    pub fn key_size(&self) -> usize {
        self.key_size as usize
    }

    /// One row's leading-key span, `key_size()` bytes; `None` when any indexed
    /// column is NULL: the row is unindexed.
    pub fn span(&self, mb: &impl RowSource, row: usize) -> Option<PkBuf> {
        let mut key = PkBuf::zeroed(0);
        for loc in self.locators() {
            if loc.is_null(mb, row) {
                return None;
            }
            key.push(loc.size(), loc.opk_image(mb, row), false);
        }
        Some(key)
    }

    /// Split a stored index entry back into `(span, source PK)`: an entry is the
    /// span followed by the source PK, which therefore sits at `key_size()`. The
    /// split is exact by layout (an index schema is the indexed columns followed
    /// by the source PK columns, so its stride is `key_size() + src_pk_stride`),
    /// so nothing is decoded.
    pub fn split_entry<'a>(&self, entry: &'a [u8]) -> (&'a [u8], &'a [u8]) {
        debug_assert!(entry.len() > self.key_size(), "index entry shorter than its span");
        entry.split_at(self.key_size())
    }
}

/// The half-open OPK key band `[start, end)` `range` walks over a store keyed by
/// `schema`, whose leading PK columns are `range`'s columns at their own widths:
/// every key from the group of `eq ‖ first` through the group of `eq ‖ last`.
///
/// `None` = provably empty.
pub fn range_band(schema: &SchemaDescriptor, range: &KeyRange) -> Option<(PkBuf, Option<PkBuf>)> {
    let mut widths = schema.pk_columns().map(|(ci, _)| schema.locate(ci).size());
    let mut first = PkBuf::zeroed(0);
    for (&image, w) in range.eq_vals().iter().zip(&mut widths) {
        first.push(w, image, false);
    }
    let w = widths
        .next()
        .expect("range_band: the range lists more columns than the PK");
    let mut last = first;
    first.push(w, range.first, false);
    last.push(w, range.last, false);
    key_range_between_cuts(
        KeyCut::min_of(first.pk_bytes()),
        KeyCut::above(last.pk_bytes()),
        schema.pk_stride(),
    )
}

// ---------------------------------------------------------------------------
// Byte successor and cut → key-range derivation
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

/// A cut in the OPK key space, named by an OPK group prefix: either that
/// group's own minimum key or the first key above every member of it. Zero-
/// padding the prefix to the key width is what makes it the group's minimum.
#[derive(Clone, Copy)]
pub struct KeyCut<'a> {
    group: &'a [u8],
    above: bool,
}

impl<'a> KeyCut<'a> {
    /// [`Self::above`] when `above`, else [`Self::min_of`].
    pub(crate) fn new(group: &'a [u8], above: bool) -> Self {
        KeyCut { group, above }
    }

    /// The group's own minimum key — below every member of it.
    pub fn min_of(group: &'a [u8]) -> Self {
        KeyCut::new(group, false)
    }

    /// The first key above every member of the group. A saturated group — and
    /// the zero-width group, which is the whole key space — has none.
    pub fn above(group: &'a [u8]) -> Self {
        KeyCut::new(group, true)
    }

    /// This cut as a `stride`-wide key; `None` when it lies above the whole key
    /// space. The successor's carry ripples into the equality prefix, landing
    /// exactly on the first key of the next equality group.
    pub fn key(&self, stride: usize) -> Option<PkBuf> {
        let mut k = PkBuf::from_bytes(self.group);
        let exists = !self.above || increment_key_in_place(k.pk_bytes_mut());
        exists.then(|| k.widened(stride))
    }
}

/// Half-open `[start, end)` OPK key range between two cuts over a `stride`-byte
/// key space.
///
/// `None` = provably empty: `start` lies above the whole key space, or
/// `start >= end`. `end == None` inside `Some` means the range runs to the table
/// end.
pub fn key_range_between_cuts(start: KeyCut, end: KeyCut, stride: usize) -> Option<(PkBuf, Option<PkBuf>)> {
    let start = start.key(stride)?;
    let end = end.key(stride);
    if end.as_ref().is_some_and(|e| start.pk_bytes() >= e.pk_bytes()) {
        return None;
    }
    Some((start, end))
}

/// Whether every key of a non-empty band `[start, end)` shares its leading `prefix`
/// bytes — the bytes a worker owner hashes. OPK order is byte order, so the band
/// stays in `start`'s prefix group iff it ends at or below the first key above
/// that group; a group with no key above it reaches the top of the key space.
pub(crate) fn range_shares_prefix(start: &PkBuf, end: Option<&PkBuf>, prefix: usize) -> bool {
    let start = start.pk_bytes();
    KeyCut::above(&start[..prefix])
        .key(start.len())
        .is_none_or(|above| end.is_some_and(|e| e.pk_bytes() <= above.pk_bytes()))
}

#[cfg(test)]
#[path = "tests/key.rs"]
mod tests;

#[cfg(test)]
#[path = "benches/key.rs"]
mod bench;
