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
        for (byte_idx, bit_mask) in self.probe_positions(key_lo, key_hi) {
            self.bits[byte_idx] |= bit_mask;
        }
    }

    pub fn may_contain(&self, key_lo: u64, key_hi: u64) -> bool {
        for (byte_idx, bit_mask) in self.probe_positions(key_lo, key_hi) {
            if self.bits[byte_idx] & bit_mask == 0 {
                return false;
            }
        }
        true
    }

    fn probe_positions(&self, key_lo: u64, key_hi: u64) -> ProbeIter {
        let h = xxh::hash_u128(key_lo, key_hi);
        ProbeIter { h1: h, h2: (h >> 32) | 1, num_bits: self.num_bits, i: 0 }
    }

    pub fn reset(&mut self) {
        self.bits.fill(0);
    }
}

struct ProbeIter {
    h1: u64,
    h2: u64,
    num_bits: u64,
    i: u64,
}

impl Iterator for ProbeIter {
    type Item = (usize, u8);

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.i >= NUM_PROBES as u64 {
            return None;
        }
        let pos = (self.h1.wrapping_add(self.i.wrapping_mul(self.h2))) % self.num_bits;
        self.i += 1;
        Some(((pos >> 3) as usize, 1u8 << (pos & 7)))
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
