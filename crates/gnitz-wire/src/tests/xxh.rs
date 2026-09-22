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

/// `checksum` must agree byte-for-byte with the C/Python `XXH3_64bits` the
/// other end of the wire runs — the interop contract this crate defines.
#[test]
fn checksum_matches_c_xxh3_64bits() {
    let body_hex = "9800000008000000a000000008000000a800000008000000b000000008000000b800000008000000c000000008000000c800000008000000d000000008000000d800000008000000e000000008000000e800000008000000f00000001000000000010000000000000000000000000000000000000000000001000000000000008000000000000000000000000000000001000000000000000300000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000";
    let body: Vec<u8> = (0..body_hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&body_hex[i..i + 2], 16).unwrap())
        .collect();
    assert_eq!(body.len(), 208);
    let computed = checksum(&body);
    assert_eq!(
        computed, 0x741C9E0BA1D8A9FD_u64,
        "xxhash-rust and C XXH3_64bits disagree: got 0x{computed:016X}"
    );
}
