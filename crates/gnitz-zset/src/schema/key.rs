//! Order-preserving primary-key (OPK) primitives.
//!
//! These pure layout/key operations fingerprint an OPK span and derive the
//! half-open key band a `KeyRange` walks over a store's PK. The per-column
//! codec is `gnitz_wire::pk`'s, shared with the client; the comparator and the
//! fixed-width sort keys are `sort_key`'s.
use gnitz_wire::{KeyRange, PkBuf};

use crate::schema::SchemaDescriptor;

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
/// every key from the group of `eq ‖ first` through the group of `eq ‖ last`,
/// `first` and `last` the ends of its first and last interval — the hull of the
/// intervals, exact for one.
///
/// `None` = provably empty.
pub fn range_band(schema: &SchemaDescriptor, range: &KeyRange) -> Option<(PkBuf, Option<PkBuf>)> {
    let (&(first, _), &(_, last)) = (range.spans().first()?, range.spans().last()?);
    span_band(schema, range, (first, last))
}

/// [`range_band`] of the interval `[first, last]` on `range`'s range column.
pub fn span_band(
    schema: &SchemaDescriptor,
    range: &KeyRange,
    (first, last): (u128, u128),
) -> Option<(PkBuf, Option<PkBuf>)> {
    let mut widths = schema.pk_columns().map(|c| c.size());
    let mut lo = PkBuf::zeroed(0);
    for (&image, w) in range.eq_vals().iter().zip(&mut widths) {
        lo.push(w, image);
    }
    let w = widths
        .next()
        .expect("range_band: the range lists more columns than the PK");
    let mut hi = lo;
    lo.push(w, first);
    hi.push(w, last);
    group_band(lo.pk_bytes(), hi.pk_bytes(), schema.pk_stride())
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
