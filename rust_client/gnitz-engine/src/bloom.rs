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
        let num_bytes = ((m + 7) >> 3).max(8);
        BloomFilter {
            bits: vec![0u8; num_bytes],
            num_bits: (num_bytes * 8) as u64,
        }
    }

    pub fn add(&mut self, key_lo: u64, key_hi: u64) {
        let h = xxh::hash_u128(key_lo, key_hi);
        let h1 = h;
        let h2 = (h >> 32) | 1;
        for i in 0..NUM_PROBES as u64 {
            let pos = (h1.wrapping_add(i.wrapping_mul(h2))) % self.num_bits;
            let byte_idx = (pos >> 3) as usize;
            let bit_mask = 1u8 << (pos & 7);
            self.bits[byte_idx] |= bit_mask;
        }
    }

    pub fn may_contain(&self, key_lo: u64, key_hi: u64) -> bool {
        let h = xxh::hash_u128(key_lo, key_hi);
        let h1 = h;
        let h2 = (h >> 32) | 1;
        for i in 0..NUM_PROBES as u64 {
            let pos = (h1.wrapping_add(i.wrapping_mul(h2))) % self.num_bits;
            let byte_idx = (pos >> 3) as usize;
            let bit_mask = 1u8 << (pos & 7);
            if self.bits[byte_idx] & bit_mask == 0 {
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
            bf.add(i, 0);
        }
        for i in 0u64..100 {
            assert!(bf.may_contain(i, 0), "false negative for key {}", i);
        }
    }

    #[test]
    fn false_positive_rate() {
        let mut bf = BloomFilter::new(1000);
        for i in 0u64..1000 {
            bf.add(i, 0);
        }
        let mut fp = 0u32;
        for i in 10_000u64..11_000 {
            if bf.may_contain(i, 0) {
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
            bf.add(i, 0);
        }
        bf.reset();
        // After reset, all queries should return false (with overwhelming probability)
        let mut found = 0u32;
        for i in 0u64..10 {
            if bf.may_contain(i, 0) {
                found += 1;
            }
        }
        assert_eq!(found, 0, "reset didn't clear all bits");
    }

    #[test]
    fn empty_filter() {
        let bf = BloomFilter::new(100);
        // Empty filter should almost certainly return false
        let mut fp = 0u32;
        for i in 0u64..100 {
            if bf.may_contain(i, 0) {
                fp += 1;
            }
        }
        assert_eq!(fp, 0);
    }
}
