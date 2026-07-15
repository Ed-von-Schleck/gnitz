use xxhash_rust::xxh3::{xxh3_128, xxh3_64};

/// XXH3-64 body checksum. Owned by `gnitz_wire` (the crate that defines the WAL
/// format and computes its checksum); re-exported here so the engine's many
/// non-WAL callers keep `xxh::checksum` without a second identical wrapper.
pub use gnitz_wire::checksum;

/// Hash a 128-bit key to a 64-bit hash via XXH3-64.
#[inline]
pub fn hash_u128(pk: u128) -> u64 {
    xxh3_64(&pk.to_le_bytes())
}

/// XXH3-128 over arbitrary bytes (no seed). Full 128-bit image — use where a
/// 64-bit birthday bound is too low, e.g. a string content join key (a 64-bit
/// hash widened to 128 bits has a ~2^32-row collision window that would silently
/// equijoin distinct strings).
#[inline]
pub fn checksum_128(data: &[u8]) -> u128 {
    xxh3_128(data)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pk(lo: u64, hi: u64) -> u128 {
        ((hi as u128) << 64) | (lo as u128)
    }

    #[test]
    fn hash_u128_zero_extension_invariant() {
        assert_eq!(hash_u128(42u64 as u128), hash_u128(42u128));
    }

    #[test]
    fn deterministic() {
        assert_eq!(hash_u128(pk(42, 99)), hash_u128(pk(42, 99)));
    }

    #[test]
    fn different_keys_differ() {
        assert_ne!(hash_u128(pk(1, 2)), hash_u128(pk(2, 1)));
        assert_ne!(hash_u128(pk(0, 0)), hash_u128(pk(0, 1)));
    }

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
    fn checksum_matches_c_xxh3_64bits() {
        // Same 208-byte test vector from gnitz-core's test_xxh3_matches_python_server.
        // Python/C computes checksum 0x741C9E0BA1D8A9FD using XXH3_64bits.
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
}
