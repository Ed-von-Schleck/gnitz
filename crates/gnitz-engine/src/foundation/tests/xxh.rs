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
