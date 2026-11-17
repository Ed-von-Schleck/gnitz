//! The deterministic PRNG of the workspace's tests and benchmarks
//! (xorshift64*, the same stream on every host), and the `u64` bijections
//! `mix` and `splitmix`.

/// A bijection of `u64` that scatters consecutive values across its high bits.
/// Its low `k` bits depend only on, and permute, the low `k` bits of `i`, so
/// `mix(i) & (n - 1)` over `i in 0..n` visits every value below a power of two
/// `n` once.
pub fn mix(i: u64) -> u64 {
    i.wrapping_mul(0x9E37_79B9_7F4A_7C15)
}

/// splitmix64's finalizer: a bijection of `u64` whose every output bit
/// depends on every input bit.
pub fn splitmix(i: u64) -> u64 {
    let mut z = i.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

pub struct Rng(u64);

impl Rng {
    /// The stream `seed` names. Distinct seeds name distinct streams, but for
    /// the one seed [`splitmix`] maps to 0: xorshift has no zero state, so it
    /// shares the stream of the seed mapped to 1.
    pub fn new(seed: u64) -> Self {
        Self(splitmix(seed).max(1))
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

#[cfg(test)]
#[path = "tests/rng.rs"]
mod tests;
