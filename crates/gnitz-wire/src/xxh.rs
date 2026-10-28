//! XXH3 hashing, the workspace's one owner: a digest computed in two places —
//! the global group key on client and engine, a persisted digest read back by a
//! later build — agrees bit for bit only because both call these functions.

use xxhash_rust::xxh3::{xxh3_128, xxh3_64};

/// XXH3-64 over `b`.
#[inline]
pub fn checksum(b: &[u8]) -> u64 {
    xxh3_64(b)
}

/// XXH3-128 over `data`: for a content-derived key where a 64-bit birthday
/// bound is too low, such as a group or join key.
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

/// Streaming XXH3-64: `.digest()` over a sequence of `update`s equals
/// [`checksum`] over their concatenation.
pub use xxhash_rust::xxh3::Xxh3Default as ChecksumStream;

#[cfg(test)]
#[path = "tests/xxh.rs"]
mod tests;
