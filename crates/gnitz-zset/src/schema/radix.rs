//! Radix sorts by a key read a byte at a time: an integer, or a byte string of
//! one width among the keys of a sort.

/// A key [`radix_sort_by_key`] orders a byte at a time, as `Ord` orders it. A
/// key may hold bytes below the ones it reports that `Ord` also reads; the radix
/// passes leave their ties in any order.
pub trait RadixKey: Copy + Ord {
    /// Byte `i` of the key, counted from the least significant.
    fn byte(self, i: usize) -> u8;
    /// A mask with bit `i` set where byte `i` of some key of `keys` differs
    /// from `first`'s.
    fn varying(first: Self, keys: impl Iterator<Item = Self>) -> u128;
}

/// A mask with bit `i` set where byte `i` of `diff`, from the least
/// significant, is not zero.
#[inline]
fn nonzero_bytes(diff: u128) -> u128 {
    (0..16).filter(|&i| diff.byte(i) != 0).fold(0, |m, i| m | 1 << i)
}

impl RadixKey for u64 {
    #[inline(always)]
    fn byte(self, i: usize) -> u8 {
        (self >> (8 * i)) as u8
    }
    #[inline]
    fn varying(first: Self, keys: impl Iterator<Item = Self>) -> u128 {
        nonzero_bytes(keys.fold(0, |d, k| d | (k ^ first)) as u128)
    }
}

impl RadixKey for u128 {
    /// The half first: a variable shift of the whole `u128` costs a funnel shift.
    #[inline(always)]
    fn byte(self, i: usize) -> u8 {
        let half = if i < 8 { self as u64 } else { (self >> 64) as u64 };
        half.byte(i & 7)
    }
    #[inline]
    fn varying(first: Self, keys: impl Iterator<Item = Self>) -> u128 {
        nonzero_bytes(keys.fold(0, |d, k| d | (k ^ first)))
    }
}

/// The high word, then the low.
impl RadixKey for [u128; 2] {
    #[inline(always)]
    fn byte(self, i: usize) -> u8 {
        if i < 16 { self[1].byte(i) } else { self[0].byte(i - 16) }
    }
    #[inline]
    fn varying(first: Self, keys: impl Iterator<Item = Self>) -> u128 {
        let [hi, lo] = keys.fold([0, 0], |[hi, lo], k| [hi | (k[0] ^ first[0]), lo | (k[1] ^ first[1])]);
        nonzero_bytes(hi) << 16 | nonzero_bytes(lo)
    }
}

/// A byte string, at most 128 bytes, its first byte the most significant.
impl RadixKey for &[u8] {
    #[inline(always)]
    fn byte(self, i: usize) -> u8 {
        self[self.len() - 1 - i]
    }
    fn varying(first: Self, keys: impl Iterator<Item = Self>) -> u128 {
        let mut buf = [0u8; 128];
        let diff = &mut buf[..first.len()];
        for k in keys {
            for ((d, a), b) in diff.iter_mut().zip(k).zip(first) {
                *d |= a ^ b;
            }
        }
        let at = |i| (diff[diff.len() - 1 - i] != 0) as u128;
        (0..diff.len()).fold(0, |m, i| m | at(i) << i)
    }
}

/// Sort `v` by `key`, unstably: a radix sort over the key bytes that vary among
/// its elements — least significant digit first where few vary, most significant
/// first where many do — or a comparison sort on a list too short to repay one.
#[inline]
pub fn radix_sort_by_key<T: Copy + Default, K: RadixKey>(v: &mut [T], key: impl Fn(&T) -> K) {
    radix_sort::<false, T, K>(v, key)
}

/// [`radix_sort_by_key`], stably: elements of one key keep their order.
#[inline]
pub(crate) fn stable_radix_sort_by_key<T: Copy + Default, K: RadixKey>(v: &mut [T], key: impl Fn(&T) -> K) {
    radix_sort::<true, T, K>(v, key)
}

#[inline]
fn radix_sort<const STABLE: bool, T: Copy + Default, K: RadixKey>(v: &mut [T], key: impl Fn(&T) -> K) {
    const RADIX_MIN: usize = 256;
    match v.len() < RADIX_MIN {
        true => comparison_sort::<STABLE, T, K>(v, &key),
        false => radix_sort_long::<STABLE, T, K>(v, key),
    }
}

#[inline(always)]
fn comparison_sort<const STABLE: bool, T, K: Ord>(v: &mut [T], key: &impl Fn(&T) -> K) {
    match STABLE {
        true => v.sort_by_key(key),
        false => v.sort_unstable_by_key(key),
    }
}

/// [`radix_sort`] of a list long enough for a radix sort. Out of line, so a
/// caller whose lists are mostly short keeps a small body.
#[inline(never)]
fn radix_sort_long<const STABLE: bool, T: Copy + Default, K: RadixKey>(v: &mut [T], key: impl Fn(&T) -> K) {
    if v.is_sorted_by_key(&key) {
        return;
    }
    // The stable sort reverses only a strictly descending list: reversed, a run
    // of equal keys would change its order.
    if v.is_sorted_by(|a, b| if STABLE { key(a) > key(b) } else { key(a) >= key(b) }) {
        v.reverse();
        return;
    }
    // LSD makes a pass per varying byte; MSD stops once its buckets are leaves,
    // about log256(n) passes over uniform keys, but pays a count table per
    // bucket, which outweighs the passes it saves where few bytes vary.
    let max_digits = v.len().ilog2() / 4;
    // Neither ascending nor descending, so some byte varies.
    let digits = K::varying(key(&v[0]), v.iter().map(&key));
    // Zeroed: the allocator hands out fresh pages with no pass over them.
    let mut buf = vec![T::default(); v.len()];
    if digits.count_ones() > max_digits {
        return msd::<STABLE, T, K>(v, &mut buf, digits, &key, true);
    }
    let (mut from, mut to) = (&mut *v, &mut buf[..]);
    // A pass reading one key byte, rather than also counting the next digit
    // while it scatters, lets the compiler hoist the byte's word out of the loop.
    for b in (0..128).filter(|&b| digits >> b & 1 != 0) {
        let mut count = [0usize; 256];
        for x in from.iter() {
            count[key(x).byte(b) as usize] += 1;
        }
        let mut at = 0;
        for c in count.iter_mut() {
            (*c, at) = (at, at + *c);
        }
        for &x in from.iter() {
            let slot = &mut count[key(&x).byte(b) as usize];
            to[*slot] = x;
            *slot += 1;
        }
        std::mem::swap(&mut from, &mut to);
    }
    if digits.count_ones() % 2 == 1 {
        v.copy_from_slice(&buf);
    }
}

/// Most-significant-digit radix over the bytes set in `digits`, highest first:
/// `data`, longer than `MSD_LEAF`, scattered into `other` by the first digit
/// that splits it, each bucket then sorted the same way by the digits below
/// with the two roles swapped, and a bucket of at most `MSD_LEAF` elements
/// sorted by comparison. `data_is_v` says which of the two is the caller's
/// slice, which ends up holding every bucket. The scatter keeps the order of
/// elements of one digit, so the sort is stable where the leaves' sort is.
fn msd<const STABLE: bool, T: Copy, K: RadixKey>(
    data: &mut [T],
    other: &mut [T],
    mut digits: u128,
    key: &impl Fn(&T) -> K,
    data_is_v: bool,
) {
    const MSD_LEAF: usize = 128;
    let n = data.len();
    let mut count = [0usize; 256];
    // The first digit that splits `data`; one every element shares is passed over
    // here rather than by a call of its own.
    let b = loop {
        if digits == 0 {
            // Every key in `data` is equal.
            if !data_is_v {
                other.copy_from_slice(data);
            }
            return;
        }
        let b = 127 - digits.leading_zeros() as usize;
        digits &= !(1 << b);
        count.fill(0);
        for x in data.iter() {
            count[key(x).byte(b) as usize] += 1;
        }
        if count[key(&data[0]).byte(b) as usize] < n {
            break b;
        }
        // Only the bytes that vary within `data`: the digits none of its
        // elements differ in, all of them where its keys are equal, cost no
        // count pass each.
        digits &= K::varying(key(&data[0]), data.iter().map(key));
    };
    let mut start = [0usize; 257];
    for d in 0..256 {
        start[d + 1] = start[d] + count[d];
    }
    let mut slot = start;
    for &x in data.iter() {
        let s = &mut slot[key(&x).byte(b) as usize];
        other[*s] = x;
        *s += 1;
    }
    for d in 0..256 {
        let (lo, hi) = (start[d], start[d + 1]);
        if lo == hi {
            continue;
        }
        let (bucket, home) = (&mut other[lo..hi], &mut data[lo..hi]);
        if hi - lo > MSD_LEAF {
            msd::<STABLE, T, K>(bucket, home, digits, key, !data_is_v);
        } else {
            // A leaf, sorted where it ends up: no call of its own.
            let leaf = match data_is_v {
                true => {
                    home.copy_from_slice(bucket);
                    home
                }
                false => bucket,
            };
            if leaf.len() > 1 && digits != 0 {
                comparison_sort::<STABLE, T, K>(leaf, key);
            }
        }
    }
}

#[cfg(test)]
#[path = "tests/radix.rs"]
mod tests;
