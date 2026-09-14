//! XXH3 hashing — the one owner in the workspace.
//!
//! Both ends compute some of these (the WAL body checksum, the global group
//! key) and the engine alone computes the rest (shard and manifest header
//! digests, row and group identity). They live together because a second XXH3
//! definition elsewhere could drift from this one with nothing to catch it:
//! `global_group_key` and `FoldCols::key_row` have to agree bit for bit across
//! the wire, and they only do so by calling the same function.

use xxhash_rust::xxh3::{xxh3_128, xxh3_64, Xxh3Default};

/// XXH3-64 over `b` — the WAL body checksum, the wide-PK routing hash, and the
/// engine's shard-region and filter checksums.
#[inline]
pub fn checksum(b: &[u8]) -> u64 {
    xxh3_64(b)
}

/// XXH3-128 over arbitrary bytes (no seed). Full 128-bit image — use where a
/// 64-bit birthday bound is too low, e.g. a string content join key (a 64-bit
/// hash widened to 128 bits has a ~2^32-row collision window that would silently
/// equijoin distinct strings).
#[inline]
pub fn checksum_128(data: &[u8]) -> u128 {
    xxh3_128(data)
}

/// `V₀` — the group key of the ungrouped (global) aggregate: the digest over no
/// group columns at all. Every row keyed at V₀ — a global reduce's row and the
/// FROM-less SELECT's constant row — carries this key, with no literal embedded
/// on either end.
#[inline]
pub fn global_group_key() -> u128 {
    checksum_128(b"")
}

/// XXH3-64 over `seed` followed by `buf` with the eight bytes at `hole`
/// excluded — the shape a self-describing file header needs, since the field
/// holding the digest cannot be part of it. `seed` binds the digest to
/// something outside the buffer; pass `&[]` where there is nothing to bind to.
///
/// Writer and reader call this identically, so neither can restate the hashed
/// span differently from the other.
#[inline]
pub fn digest_with_hole(seed: &[u8], buf: &[u8], hole: usize) -> u64 {
    let mut h = Xxh3Default::default();
    h.update(seed);
    h.update(&buf[..hole]);
    h.update(&buf[hole + 8..]);
    h.digest()
}

/// Streaming XXH3-128 hasher for row and group identity, built column by
/// column — 128 bits for the same reason [`checksum_128`] is.
pub use xxhash_rust::xxh3::Xxh3Default as RowHasher;

#[cfg(test)]
#[path = "tests/xxh.rs"]
mod tests;
