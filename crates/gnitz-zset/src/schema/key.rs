//! Order-preserving primary-key (OPK) primitives.
//!
//! These pure layout/key operations compare two OPK regions in byte order,
//! fingerprint an OPK span, and derive the half-open key band a `KeyRange`
//! walks over a store's PK. The per-column codec is `gnitz_wire::pk`'s, shared
//! with the client; the fixed-width sort keys are `sort_key`'s.
use std::cmp::Ordering;

use gnitz_wire::{KeyRange, NARROW_PK_MAX_BYTES, PkBuf};

use crate::schema::{SchemaDescriptor, pack_pk_be};

/// Typed lexicographic OPK ordering of two **equal-length** PK regions — the
/// comparator of seeks, group walks and probes. Slice order
/// at every width, settled on the leading-16 `pack_pk_be` image where that
/// decides: for `len ≤ 16` the image is the *whole* PK and is injective, so a
/// tie is already a byte-equal PK; only `len > 16` can tie on the prefix while
/// differing later, and the slice compare runs only there.
#[inline(always)]
pub fn compare_pk_ordering(a: &[u8], b: &[u8]) -> Ordering {
    debug_assert_eq!(a.len(), b.len(), "compare_pk_ordering on unequal PK widths");
    match pack_pk_be(a).cmp(&pack_pk_be(b)) {
        Ordering::Equal if a.len() > NARROW_PK_MAX_BYTES => a.cmp(b),
        ord => ord,
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
        first.push(w, image);
    }
    let w = widths
        .next()
        .expect("range_band: the range lists more columns than the PK");
    let mut last = first;
    first.push(w, range.first);
    last.push(w, range.last);
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
pub fn group_floor(group: &[u8], stride: usize) -> PkBuf {
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
pub fn band(start: PkBuf, end: Option<PkBuf>) -> Option<(PkBuf, Option<PkBuf>)> {
    end.as_ref().is_none_or(|e| start < *e).then_some((start, end))
}

#[cfg(test)]
#[path = "tests/key.rs"]
mod tests;

#[cfg(test)]
#[path = "benches/key.rs"]
mod bench;
