use crate::xxh;

const BITS_PER_KEY: usize = 10;
const NUM_PROBES: usize = 7;

pub struct BloomFilter {
    bits: Vec<u8>,
    num_bits: u64,
}

impl BloomFilter {
    pub fn new(expected_n: u32) -> Self {
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
    pub fn add(&mut self, key: u128) {
        let h = xxh::hash_u128(key);
        let h1 = h;
        let h2 = (h >> 32) | 1;
        let mask = self.num_bits - 1;
        for i in 0..NUM_PROBES as u64 {
            let pos = h1.wrapping_add(i.wrapping_mul(h2)) & mask;
            self.bits[(pos >> 3) as usize] |= 1u8 << (pos & 7);
        }
    }

    #[inline]
    pub fn may_contain(&self, key: u128) -> bool {
        let h = xxh::hash_u128(key);
        let h1 = h;
        let h2 = (h >> 32) | 1;
        let mask = self.num_bits - 1;
        for i in 0..NUM_PROBES as u64 {
            let pos = h1.wrapping_add(i.wrapping_mul(h2)) & mask;
            if self.bits[(pos >> 3) as usize] & (1u8 << (pos & 7)) == 0 {
                return false;
            }
        }
        true
    }

    pub fn reset(&mut self) {
        self.bits.fill(0);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_false_negatives() {
        let mut bf = BloomFilter::new(100);
        for i in 0u64..100 {
            bf.add(i as u128);
        }
        for i in 0u64..100 {
            assert!(bf.may_contain(i as u128), "false negative for key {}", i);
        }
    }

    #[test]
    fn false_positive_rate() {
        let mut bf = BloomFilter::new(1000);
        for i in 0u64..1000 {
            bf.add(i as u128);
        }
        let mut fp = 0u32;
        for i in 10_000u64..11_000 {
            if bf.may_contain(i as u128) {
                fp += 1;
            }
        }
        // 10 bits/key, 7 probes → theoretical ~0.8%. Allow up to 5%.
        assert!(fp < 50, "FPR too high: {}/1000", fp);
    }

    #[test]
    fn reset_clears() {
        let mut bf = BloomFilter::new(10);
        for i in 0u64..10 {
            bf.add(i as u128);
        }
        bf.reset();
        let mut found = 0u32;
        for i in 0u64..10 {
            if bf.may_contain(i as u128) {
                found += 1;
            }
        }
        assert_eq!(found, 0, "reset didn't clear all bits");
    }

    #[test]
    fn empty_filter() {
        let bf = BloomFilter::new(100);
        let mut fp = 0u32;
        for i in 0u64..100 {
            if bf.may_contain(i as u128) {
                fp += 1;
            }
        }
        assert_eq!(fp, 0);
    }

    // All existing tests use `i as u128` with small i, so upper 64 bits are
    // always zero.  Verify the filter works for keys with significant upper bits
    // — hash_u128 encodes both halves, so this exercises a different hash path.
    #[test]
    fn no_false_negatives_high_bits() {
        let base: u128 = 0xDEAD_BEEF_0000_0000_0000_0000u128;
        let mut bf = BloomFilter::new(100);
        for i in 0u64..100 {
            bf.add(base | i as u128);
        }
        for i in 0u64..100 {
            assert!(
                bf.may_contain(base | i as u128),
                "false negative for high-bit key {}",
                i
            );
        }
    }

    // Keys differing only in upper bits must not collide with lower-bit-only keys.
    #[test]
    fn high_bit_keys_distinct_from_low_bit_keys() {
        let mut bf = BloomFilter::new(200);
        let hi_base: u128 = 0x0102_0304_0000_0000_0000_0000u128;
        for i in 0u64..100 {
            bf.add(hi_base | i as u128);
        }
        // Keys with only low bits set were never added; FPR should be low.
        let mut fp = 0u32;
        for i in 0u64..100 {
            if bf.may_contain(i as u128) {
                fp += 1;
            }
        }
        assert!(fp < 10, "too many false positives for low-bit keys: {}/100", fp);
    }

    #[test]
    fn reset_then_readd() {
        let mut bf = BloomFilter::new(10);
        for i in 0u64..10 {
            bf.add(i as u128);
        }
        bf.reset();
        // Re-add a different set after reset.
        for i in 100u64..110 {
            bf.add(i as u128);
        }
        for i in 100u64..110 {
            assert!(bf.may_contain(i as u128), "false negative after reset+readd for key {}", i);
        }
        // Original keys must not reliably appear (zero bits, near-zero FPR expected).
        let mut fp = 0u32;
        for i in 0u64..10 {
            if bf.may_contain(i as u128) {
                fp += 1;
            }
        }
        assert!(fp < 5, "old keys still present after reset: {}/10", fp);
    }
}
