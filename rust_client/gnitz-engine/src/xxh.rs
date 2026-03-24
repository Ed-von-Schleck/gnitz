use xxhash_rust::xxh3::xxh3_64;

/// Hash a 128-bit key (lo, hi) to a 64-bit hash.
/// Lays out [lo:u64 LE, hi:u64 LE] as 16 bytes and hashes with XXH3-64.
#[inline]
pub fn hash_u128(lo: u64, hi: u64) -> u64 {
    let mut buf = [0u8; 16];
    buf[..8].copy_from_slice(&lo.to_le_bytes());
    buf[8..].copy_from_slice(&hi.to_le_bytes());
    xxh3_64(&buf)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deterministic() {
        assert_eq!(hash_u128(42, 99), hash_u128(42, 99));
    }

    #[test]
    fn different_keys_differ() {
        assert_ne!(hash_u128(1, 2), hash_u128(2, 1));
        assert_ne!(hash_u128(0, 0), hash_u128(0, 1));
    }

    #[test]
    fn zero_key() {
        // Should produce a valid hash, not zero
        let h = hash_u128(0, 0);
        // Just check it doesn't panic; exact value depends on xxh3
        let _ = h;
    }
}
