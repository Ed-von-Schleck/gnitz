use xxhash_rust::xxh3::xxh3_64;

/// Hash a 128-bit key to a 64-bit hash via XXH3-64.
#[inline]
pub fn hash_u128(pk: u128) -> u64 {
    let mut buf = [0u8; 16];
    buf[..8].copy_from_slice(&(pk as u64).to_le_bytes());
    buf[8..].copy_from_slice(&((pk >> 64) as u64).to_le_bytes());
    xxh3_64(&buf)
}

/// Compute XXH3-64 checksum over arbitrary bytes (no seed).
#[inline]
pub fn checksum(data: &[u8]) -> u64 {
    xxh3_64(data)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pk(lo: u64, hi: u64) -> u128 {
        ((hi as u128) << 64) | (lo as u128)
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
    fn checksum_matches_c_xxh3_64bits() {
        // Same 208-byte test vector from gnitz-protocol's test_xxh3_matches_python_server.
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
            "xxhash-rust and C XXH3_64bits disagree: got 0x{:016X}",
            computed
        );
    }
}
