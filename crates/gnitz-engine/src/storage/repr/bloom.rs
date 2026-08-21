const BITS_PER_KEY: usize = 10;
const NUM_PROBES: usize = 7;

/// The bit positions `key` probes. `key` is already a well-mixed 64-bit
/// fingerprint (`probe_key`), so no further hashing happens here; the two
/// derived hashes are the standard double-hashing pair.
#[inline]
fn probes(key: u64, num_bits: u64) -> impl Iterator<Item = u64> {
    let h2 = (key >> 32) | 1;
    let mask = num_bits - 1;
    (0..NUM_PROBES as u64).map(move |i| key.wrapping_add(i.wrapping_mul(h2)) & mask)
}

pub(crate) struct BloomFilter {
    bits: Vec<u8>,
    num_bits: u64,
}

impl BloomFilter {
    pub(crate) fn new(expected_n: u32) -> Self {
        let n = (expected_n as usize).max(1);
        let m = n * BITS_PER_KEY;
        let num_bytes_raw = ((m + 7) >> 3).max(8);
        // Round up to a power-of-two byte count so num_bits is also a power of
        // two.  add/may_contain can then use bitwise AND instead of hardware
        // division (7 probes per PK check, called on every INSERT/DELETE/UPDATE).
        let num_bytes = num_bytes_raw.next_power_of_two();
        BloomFilter {
            bits: vec![0u8; num_bytes],
            num_bits: (num_bytes * 8) as u64,
        }
    }

    #[inline]
    pub(crate) fn add(&mut self, key: u64) {
        for pos in probes(key, self.num_bits) {
            self.bits[(pos >> 3) as usize] |= 1u8 << (pos & 7);
        }
    }

    #[inline]
    pub(crate) fn may_contain(&self, key: u64) -> bool {
        probes(key, self.num_bits).all(|pos| self.bits[(pos >> 3) as usize] & (1u8 << (pos & 7)) != 0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::key::probe_key;

    /// Keys are derived exactly as production derives them — from a PK's OPK
    /// bytes — so these exercise the real fingerprint, not raw counters.
    fn key(i: u64) -> u64 {
        probe_key(&i.to_be_bytes())
    }

    #[test]
    fn no_false_negatives() {
        let mut bf = BloomFilter::new(100);
        for i in 0u64..100 {
            bf.add(key(i));
        }
        for i in 0u64..100 {
            assert!(bf.may_contain(key(i)), "false negative for key {i}");
        }
    }

    #[test]
    fn false_positive_rate() {
        let mut bf = BloomFilter::new(1000);
        for i in 0u64..1000 {
            bf.add(key(i));
        }
        let mut fp = 0u32;
        for i in 10_000u64..11_000 {
            if bf.may_contain(key(i)) {
                fp += 1;
            }
        }
        // 10 bits/key, 7 probes → theoretical ~0.8%. Allow up to 5%.
        assert!(fp < 50, "FPR too high: {fp}/1000");
    }

    #[test]
    fn empty_filter() {
        let bf = BloomFilter::new(100);
        let mut fp = 0u32;
        for i in 0u64..100 {
            if bf.may_contain(key(i)) {
                fp += 1;
            }
        }
        assert_eq!(fp, 0);
    }

    /// Wide PKs (`> 16` OPK bytes) that share a 16-byte prefix must still be
    /// distinct keys, and each must be found.
    #[test]
    fn wide_pks_sharing_a_prefix_are_distinct_keys() {
        let wide = |tail: u64| {
            let mut k = [0u8; 24];
            k[..16].copy_from_slice(&0xABCD_1234_5678_9ABCu64.to_be_bytes().repeat(2));
            k[16..].copy_from_slice(&tail.to_be_bytes());
            k
        };
        let mut bf = BloomFilter::new(100);
        for i in 0u64..100 {
            bf.add(probe_key(&wide(i)));
        }
        for i in 0u64..100 {
            assert!(bf.may_contain(probe_key(&wide(i))), "false negative for wide key {i}");
        }
        let distinct: std::collections::HashSet<u64> = (0u64..100).map(|i| probe_key(&wide(i))).collect();
        assert_eq!(
            distinct.len(),
            100,
            "a shared 16-byte prefix must not collapse the keys"
        );
    }

    /// PKs differing only in their high bytes must not collide with PKs that
    /// differ only in their low bytes.
    #[test]
    fn high_byte_keys_distinct_from_low_byte_keys() {
        let mut bf = BloomFilter::new(200);
        for i in 0u64..100 {
            bf.add(key(i << 40));
        }
        let mut fp = 0u32;
        for i in 0u64..100 {
            if bf.may_contain(key(i)) {
                fp += 1;
            }
        }
        assert!(fp < 10, "too many false positives for low-byte keys: {fp}/100");
    }
}
