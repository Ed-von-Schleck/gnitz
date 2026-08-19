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
pub(crate) fn digest_with_hole(seed: &[u8], buf: &[u8], hole: usize) -> u64 {
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
mod tests {
    use super::*;

    #[test]
    fn checksum_128_full_image_deterministic_and_distinct() {
        // Deterministic, and the full 128-bit image is populated (not a 64-bit
        // hash widened to 128 bits) — the high half is non-zero for typical input.
        let a = checksum_128(b"hello world");
        assert_eq!(a, checksum_128(b"hello world"));
        assert_ne!(a, checksum_128(b"hello worle"));
        assert_ne!(a >> 64, 0, "128-bit hash must populate the high half");
        // Distinct content → distinct 128-bit keys (no truncation collision).
        assert_ne!(checksum_128(b"abc"), checksum_128(b"abd"));
    }

    #[test]
    fn digest_with_hole_equals_checksum_over_the_spliced_bytes() {
        // The streaming spans must reassemble into exactly seed ++ buf-without-
        // the-hole. Swept across the 240-byte input size where xxh3 switches
        // algorithms, and with the hole at both ends of the buffer.
        let buf: Vec<u8> = (0..600u32).map(|i| (i * 31 + 7) as u8).collect();
        for &len in &[8usize, 64, 239, 240, 241, 600] {
            // Every hole must fit: `digest_with_hole` requires `hole + 8 <= len`.
            for &hole in [0usize, 24, len - 8].iter().filter(|&&h| h + 8 <= len) {
                for seed in [b"".as_slice(), b"shard_7_1.db".as_slice()] {
                    let mut spliced = seed.to_vec();
                    spliced.extend_from_slice(&buf[..hole]);
                    spliced.extend_from_slice(&buf[hole + 8..len]);
                    assert_eq!(
                        digest_with_hole(seed, &buf[..len], hole),
                        checksum(&spliced),
                        "len={len} hole={hole} seed={}",
                        seed.len()
                    );
                }
            }
        }
    }

    #[test]
    fn digest_with_hole_ignores_the_hole_and_separates_seeds() {
        let mut buf: Vec<u8> = (0..128u8).collect();
        let base = digest_with_hole(b"a.db", &buf, 24);
        buf[24..32].copy_from_slice(&u64::MAX.to_le_bytes());
        assert_eq!(digest_with_hole(b"a.db", &buf, 24), base, "the hole is excluded");
        assert_ne!(digest_with_hole(b"b.db", &buf, 24), base, "the seed is included");
    }
}
