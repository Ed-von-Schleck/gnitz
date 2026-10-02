const BITS_PER_KEY: usize = 10;

/// One odd multiplier per word of a block: each picks that word's bit.
const SALT: [u32; 8] = [
    0x47b6137b, 0x44974d91, 0x8824ad5b, 0xa2b7289d, 0x705495c7, 0x2df1424b, 0x9efc4947, 0x5c6bfb31,
];

/// A split-block filter: a key sets one bit in each of the eight words of one
/// 256-bit block.
///
/// A key is already a well-mixed 64-bit fingerprint (`probe_key`): its high
/// half picks the block, its low half the bits.
pub(crate) struct BloomFilter {
    words: Vec<u32>,
    /// The block count less one; the count is a power of two.
    block_mask: usize,
    /// Keys added, one per row, repeats counted.
    added: usize,
}

impl BloomFilter {
    pub(crate) fn new(expected_n: usize) -> Self {
        let blocks = (expected_n.max(1) * BITS_PER_KEY).div_ceil(256).next_power_of_two();
        BloomFilter {
            words: vec![0; blocks * 8],
            block_mask: blocks - 1,
            added: 0,
        }
    }

    /// The first word of `key`'s block.
    #[inline(always)]
    fn block(&self, key: u64) -> usize {
        ((key >> 32) as usize & self.block_mask) * 8
    }

    #[inline]
    pub(crate) fn add(&mut self, key: u64) {
        self.added += 1;
        let at = self.block(key);
        let h = key as u32;
        for (word, salt) in self.words[at..at + 8].iter_mut().zip(SALT) {
            *word |= 1 << (h.wrapping_mul(salt) >> 27);
        }
    }

    /// More keys went in than the bits are sized for, and no more than half of
    /// them are among the `live` rows the filter still answers for.
    pub(crate) fn stale(&self, live: usize) -> bool {
        self.added * BITS_PER_KEY > self.words.len() * 32 && self.added >= 2 * live
    }

    #[inline]
    pub(crate) fn may_contain(&self, key: u64) -> bool {
        let at = self.block(key);
        let h = key as u32;
        let mut missing = 0u32;
        for (word, salt) in self.words[at..at + 8].iter().zip(SALT) {
            missing |= !word & (1 << (h.wrapping_mul(salt) >> 27));
        }
        missing == 0
    }
}

#[cfg(test)]
#[path = "tests/bloom.rs"]
mod tests;
