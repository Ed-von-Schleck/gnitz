//! Order-preserving primary-key (OPK) primitives.
//!
//! These pure layout/key operations compare two OPK regions in byte order,
//! pack a region into a fixed-width sort key, fingerprint an OPK span, and
//! derive the half-open key band a `KeyRange` walks over a store's PK. The
//! per-column codec is `gnitz_wire::pk`'s, shared with the client.
use std::cmp::Ordering;

use gnitz_wire::{KeyRange, NARROW_PK_MAX_BYTES, PkBuf, radix_sort_by_key};

use crate::schema::SchemaDescriptor;

/// Typed lexicographic OPK ordering of two **equal-length** PK regions — the
/// comparator of seeks, group walks and probes. Slice order
/// at every width, settled on the leading-16 `pack_pk_be` image where that
/// decides: for `len ≤ 16` the image is the *whole* PK and is injective, so a
/// tie is already a byte-equal PK; only `len > 16` can tie on the prefix while
/// differing later, and the slice compare runs only there.
#[inline(always)]
pub(crate) fn compare_pk_ordering(a: &[u8], b: &[u8]) -> Ordering {
    debug_assert_eq!(a.len(), b.len(), "compare_pk_ordering on unequal PK widths");
    match pack_pk_be(a).cmp(&pack_pk_be(b)) {
        Ordering::Equal if a.len() > NARROW_PK_MAX_BYTES => a.cmp(b),
        ord => ord,
    }
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
    /// Sort `v` by `key`, unstably: a radix sort where the key is an integer.
    fn sort_by_key<T: Copy + Default>(v: &mut [T], key: impl Fn(&T) -> Self);
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
            // `pack_pk_be` left-aligns a narrow key with fixed-width register loads.
            (pack_pk_be(opk) >> 64) as u64
        }
    }
    #[inline]
    fn sort_by_key<T: Copy + Default>(v: &mut [T], key: impl Fn(&T) -> Self) {
        radix_sort_by_key(v, key)
    }
}

impl PkSortKey<'_> for u128 {
    /// The leading 8 bytes, then the trailing 8 shifted past the bytes they
    /// share with them below 16 bytes: the key left-aligned, with no length
    /// dispatch, so a search's comparison closure stays small enough to inline.
    /// A byte stands in the key once, so a radix sort passes over it once.
    #[inline(always)]
    fn from_opk(opk: &[u8]) -> u128 {
        let len = opk.len();
        let hi = u64::from_be_bytes(opk[..8].try_into().unwrap()) as u128;
        let tail = u64::from_be_bytes(opk[len - 8..].try_into().unwrap()) << (8 * (16 - len));
        (hi << 64) | tail as u128
    }
    #[inline]
    fn sort_by_key<T: Copy + Default>(v: &mut [T], key: impl Fn(&T) -> Self) {
        radix_sort_by_key(v, key)
    }
}

impl<'a> PkSortKey<'a> for &'a [u8] {
    #[inline(always)]
    fn from_opk(opk: &'a [u8]) -> Self {
        opk
    }
    #[inline]
    fn sort_by_key<T: Copy + Default>(v: &mut [T], key: impl Fn(&T) -> Self) {
        v.sort_unstable_by_key(key)
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
    #[inline]
    fn sort_by_key<T: Copy + Default>(v: &mut [T], key: impl Fn(&T) -> Self) {
        radix_sort_by_key(v, key)
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

/// The widest span whose [`probe_key`] is injective over spans of its width.
/// XXH3-64 packs an input of 1 to 8 bytes losslessly into one word and mixes
/// that word only by steps that are each a bijection — a constant XOR, odd
/// multiplies, xorshifts and an invertible rotate-XOR — so two such inputs of
/// one length never share a fingerprint.
pub const PROBE_KEY_INJECTIVE_BYTES: usize = 8;

/// The half-open OPK key band `[start, end)` `range` walks over a store keyed by
/// `schema`, whose leading PK columns are `range`'s columns at their own widths:
/// every key from the group of `eq ‖ first` through the group of `eq ‖ last`.
///
/// `None` = provably empty.
pub fn range_band(schema: &SchemaDescriptor, range: &KeyRange) -> Option<(PkBuf, Option<PkBuf>)> {
    let mut widths = schema.pk_columns().map(|c| c.size());
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
    group_band(first.pk_bytes(), last.pk_bytes(), schema.pk_stride())
}

/// The half-open band `[start, end)` of every `stride`-wide key beginning with a
/// group in `[first, last]`; `end == None` runs to the top of the key space.
/// `None` = provably empty.
pub fn group_band(first: &[u8], last: &[u8], stride: usize) -> Option<(PkBuf, Option<PkBuf>)> {
    band(group_floor(first, stride), key_above(last, stride))
}

/// The least `stride`-wide key beginning with `group`: the group zero-padded.
#[inline(always)]
pub(crate) fn group_floor(group: &[u8], stride: usize) -> PkBuf {
    PkBuf::from_bytes(group).widened(stride)
}

/// The least `stride`-wide key above every key beginning with `group`: the group
/// plus one, the carry running leftward through its bytes, zero-padded. `None`
/// for an all-`0xFF` or empty group, which no key lies above.
pub fn key_above(group: &[u8], stride: usize) -> Option<PkBuf> {
    let i = group.iter().rposition(|&b| b != 0xFF)?;
    let mut k = PkBuf::from_bytes(&group[..=i]);
    k.pk_bytes_mut()[i] += 1;
    Some(k.widened(stride))
}

/// The half-open key range `[start, end)`, `end == None` running to the top of
/// the key space; `None` when `start >= end`.
pub(crate) fn band(start: PkBuf, end: Option<PkBuf>) -> Option<(PkBuf, Option<PkBuf>)> {
    end.as_ref().is_none_or(|e| start < *e).then_some((start, end))
}

#[cfg(test)]
#[path = "tests/key.rs"]
mod tests;

#[cfg(test)]
#[path = "benches/key.rs"]
mod bench;
