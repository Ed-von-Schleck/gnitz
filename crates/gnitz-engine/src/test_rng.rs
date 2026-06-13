//! Shared test-only deterministic PRNG.
//!
//! Tiny xorshift64* — no `rand` crate dependency, deterministic across hosts. One
//! copy for every property / differential test in the crate (k-way merge, range
//! join, …) instead of a per-module re-implementation.

pub(crate) struct Rng(u64);

impl Rng {
    pub(crate) fn new(seed: u64) -> Self {
        Self(seed | 1)
    }

    pub(crate) fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545F4914F6CDD1D)
    }

    pub(crate) fn gen_u128(&mut self) -> u128 {
        ((self.next_u64() as u128) << 64) | (self.next_u64() as u128)
    }

    pub(crate) fn gen_range(&mut self, max: u64) -> u64 {
        self.next_u64() % max
    }
}
