//! The test-only deterministic PRNG (xorshift64*, deterministic across hosts)
//! and the `u64` scrambler `mix`, compiled here and as part of
//! `gnitz-zset-testkit`.

/// A bijection of `u64` that scatters consecutive values.
pub fn mix(i: u64) -> u64 {
    i.wrapping_mul(0x9E37_79B9_7F4A_7C15)
}

pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Self {
        Self(seed | 1)
    }

    pub fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545F4914F6CDD1D)
    }

    pub fn gen_u128(&mut self) -> u128 {
        ((self.next_u64() as u128) << 64) | (self.next_u64() as u128)
    }

    pub fn gen_range(&mut self, max: u64) -> u64 {
        self.next_u64() % max
    }

    /// A uniformly drawn element of `items`.
    pub fn pick<T: Clone>(&mut self, items: &[T]) -> T {
        items[self.gen_range(items.len() as u64) as usize].clone()
    }

    /// Fisher–Yates, in place.
    pub fn shuffle<T>(&mut self, items: &mut [T]) {
        for i in (1..items.len()).rev() {
            items.swap(i, self.gen_range(i as u64 + 1) as usize);
        }
    }
}
