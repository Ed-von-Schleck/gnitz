//! The radix sort of fixed-width integer keys. In `gnitz-wire` because
//! [`PkKeys`](crate::PkKeys) sorts with it, and this is the lowest crate the
//! engine shares with it.

use smallvec::SmallVec;

/// A key [`radix_sort_by_key`] orders a byte at a time: `BYTES` bytes read as
/// one unsigned integer, which orders as `Ord` does. A key may hold bytes below
/// them that `Ord` also reads; the radix passes leave their ties in any order.
pub trait RadixKey: Copy + Ord + Default {
    /// How many bytes the key has.
    const BYTES: usize;
    /// Byte `i` of `0..BYTES`, counted from the least significant.
    fn byte(self, i: usize) -> u8;
    /// `self` with every bit set where `a` and `b` differ.
    fn or_diff(self, a: Self, b: Self) -> Self;
}

impl RadixKey for u64 {
    const BYTES: usize = 8;
    #[inline(always)]
    fn byte(self, i: usize) -> u8 {
        (self >> (8 * i)) as u8
    }
    #[inline(always)]
    fn or_diff(self, a: Self, b: Self) -> Self {
        self | (a ^ b)
    }
}

impl RadixKey for u128 {
    const BYTES: usize = 16;
    /// The half first: a variable shift of the whole `u128` costs a funnel shift.
    #[inline(always)]
    fn byte(self, i: usize) -> u8 {
        let half = if i < 8 { self as u64 } else { (self >> 64) as u64 };
        half.byte(i & 7)
    }
    #[inline(always)]
    fn or_diff(self, a: Self, b: Self) -> Self {
        self | (a ^ b)
    }
}

/// The high word, then the low.
impl RadixKey for [u128; 2] {
    const BYTES: usize = 32;
    #[inline(always)]
    fn byte(self, i: usize) -> u8 {
        if i < 16 { self[1].byte(i) } else { self[0].byte(i - 16) }
    }
    #[inline(always)]
    fn or_diff(self, a: Self, b: Self) -> Self {
        [self[0].or_diff(a[0], b[0]), self[1].or_diff(a[1], b[1])]
    }
}

/// Sort `v` by `key`, unstably: least-significant-digit radix over the key bytes
/// that vary among its elements, or a comparison sort on a list too short to
/// repay one or whose keys vary in too many bytes.
pub fn radix_sort_by_key<T: Copy + Default, K: RadixKey>(v: &mut [T], key: impl Fn(&T) -> K) {
    const RADIX_MIN: usize = 256;
    if v.len() < RADIX_MIN {
        v.sort_unstable_by_key(&key);
        return;
    }
    if v.is_sorted_by_key(&key) {
        return;
    }
    if v.is_sorted_by(|a, b| key(a) >= key(b)) {
        v.reverse();
        return;
    }
    // A pass costs about what four levels of a comparison sort do.
    let max_digits = v.len().ilog2() as usize / 4;
    let varying = |diff: K| (0..K::BYTES).filter(move |&b| diff.byte(b) != 0);
    let first = key(&v[0]);
    let mut diff = K::default();
    for chunk in v.chunks(RADIX_MIN) {
        diff = chunk.iter().fold(diff, |d, x| d.or_diff(key(x), first));
        if varying(diff).count() > max_digits {
            v.sort_unstable_by_key(&key);
            return;
        }
    }
    // Neither ascending nor descending, so some byte varies.
    let digits: SmallVec<[usize; 32]> = varying(diff).collect();
    let mut buf = vec![T::default(); v.len()];
    let (mut src, mut dst) = (&mut *v, &mut buf[..]);
    // A pass reading one key byte, rather than also counting the next digit
    // while it scatters, lets the compiler hoist the byte's word out of the loop.
    for &b in &digits {
        let mut count = [0usize; 256];
        for x in src.iter() {
            count[key(x).byte(b) as usize] += 1;
        }
        let mut at = 0;
        for c in count.iter_mut() {
            (*c, at) = (at, at + *c);
        }
        for &x in src.iter() {
            let slot = &mut count[key(&x).byte(b) as usize];
            dst[*slot] = x;
            *slot += 1;
        }
        std::mem::swap(&mut src, &mut dst);
    }
    if digits.len() % 2 == 1 {
        v.copy_from_slice(&buf);
    }
}

#[cfg(test)]
#[path = "tests/radix.rs"]
mod tests;
