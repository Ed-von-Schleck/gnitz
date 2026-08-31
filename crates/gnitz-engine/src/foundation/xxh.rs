//! XXH3 hashing: the WAL/shard body checksum, the header digest that must skip
//! its own field, and the 128-bit hashes used where a 64-bit birthday bound
//! would silently coalesce distinct rows.

use xxhash_rust::xxh3::xxh3_128;

/// XXH3-64 body checksum. Owned by `gnitz_wire` (the crate that defines the WAL
/// format and computes its checksum); re-exported here so the engine's many
/// non-WAL callers keep `xxh::checksum` without a second identical wrapper.
pub(crate) use gnitz_wire::checksum;

/// XXH3-64 over `seed` followed by `buf` with the eight bytes at `hole`
/// excluded — the shape a self-describing file header needs, since the field
/// holding the digest cannot be part of it. `seed` binds the digest to
/// something outside the buffer; pass `&[]` where there is nothing to bind to.
///
/// Writer and reader call this identically, so neither can restate the hashed
/// span differently from the other.
#[inline]
pub fn digest_with_hole(seed: &[u8], buf: &[u8], hole: usize) -> u64 {
    let mut h = xxhash_rust::xxh3::Xxh3Default::default();
    h.update(seed);
    h.update(&buf[..hole]);
    h.update(&buf[hole + 8..]);
    h.digest()
}

/// Streaming XXH3-128 hasher for row and group identity, built column by
/// column. Row and group identity hash to 128 bits for the same reason
/// [`checksum_128`] does: a 64-bit birthday bound would silently coalesce
/// distinct rows.
pub(crate) use xxhash_rust::xxh3::Xxh3Default as RowHasher;

/// XXH3-128 over arbitrary bytes (no seed). Full 128-bit image — use where a
/// 64-bit birthday bound is too low, e.g. a string content join key (a 64-bit
/// hash widened to 128 bits has a ~2^32-row collision window that would silently
/// equijoin distinct strings).
#[inline]
pub(crate) fn checksum_128(data: &[u8]) -> u128 {
    xxh3_128(data)
}

#[cfg(test)]
#[path = "tests/xxh.rs"]
mod tests;
