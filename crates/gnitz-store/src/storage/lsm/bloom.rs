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
#[path = "tests/bloom.rs"]
mod tests;
