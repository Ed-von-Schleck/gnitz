//! The fixed-width sort keys of an OPK region: the key a merge, a group walk and
//! a seek order rows by, and the sort that builds a [`PkKeys`] list.

use gnitz_wire::{MAX_PK_BYTES, NARROW_PK_MAX_BYTES, PkKeys};
use smallvec::SmallVec;

use super::radix::{RadixKey, radix_sort_by_key};

// ---------------------------------------------------------------------------
// Narrow-region PK key packing
// ---------------------------------------------------------------------------

/// BE sort-key packer over an OPK region. Left-aligns the bytes at the MSB end
/// of a `u128` and reads big-endian, so `pack_pk_be(a).cmp(&pack_pk_be(b))`
/// equals the lexicographic byte order of the OPK regions. Narrow (`len ≤ 16`) =
/// the exact key; wide (`len > 16`) = the order-preserving leading-16 prefix
/// (authoritative whenever two prefixes differ; a prefix collision needs a
/// slice-order tiebreak).
///
/// The `{2, 4, 8, ≥16}` arms load the dominant scalar and `U128`/wide-prefix
/// widths straight into a register, value-equal to the pad-and-copy (a narrow
/// value occupies the high bits, the low bits zero; `≥16` reads the
/// order-preserving leading 16 bytes). The remaining arms compose the same
/// fixed-width loads: none copies through a runtime length, which would be an
/// out-of-line `memcpy` per key. Kept apart from [`gnitz_wire::widen_pk_be`]'s
/// right-aligned image, which retires more instructions per compare at 8, 12
/// and 16 bytes.
///
/// NOT a value accessor — for a U64 OPK value 1 (`[0,…,0,1]` at `[..8]`) this
/// packs as `1·2^64`, not 1.
#[inline(always)]
pub(crate) fn pack_pk_be(pk_bytes: &[u8]) -> u128 {
    match pk_bytes.len() {
        8 => (u64::from_be_bytes(pk_bytes[..8].try_into().unwrap()) as u128) << 64,
        len if len >= 16 => u128::from_be_bytes(pk_bytes[..16].try_into().unwrap()),
        // Two overlapping loads: the shift drops the tail load's bytes below 8.
        len @ 9..=15 => {
            let hi = u64::from_be_bytes(pk_bytes[..8].try_into().unwrap()) as u128;
            let tail = u64::from_be_bytes(pk_bytes[len - 8..len].try_into().unwrap());
            (hi << 64) | (tail << (8 * (16 - len))) as u128
        }
        4 => (u32::from_be_bytes(pk_bytes[..4].try_into().unwrap()) as u128) << 96,
        2 => (u16::from_be_bytes(pk_bytes[..2].try_into().unwrap()) as u128) << 112,
        1 => (pk_bytes[0] as u128) << 120,
        3 => {
            let hi = u16::from_be_bytes(pk_bytes[..2].try_into().unwrap()) as u128;
            (hi << 112) | ((pk_bytes[2] as u128) << 104)
        }
        5 => {
            let hi = u32::from_be_bytes(pk_bytes[..4].try_into().unwrap()) as u128;
            (hi << 96) | ((pk_bytes[4] as u128) << 88)
        }
        6 => {
            let hi = u32::from_be_bytes(pk_bytes[..4].try_into().unwrap()) as u128;
            let lo = u16::from_be_bytes(pk_bytes[4..6].try_into().unwrap()) as u128;
            (hi << 96) | (lo << 80)
        }
        7 => {
            let hi = u32::from_be_bytes(pk_bytes[..4].try_into().unwrap()) as u128;
            let mid = u16::from_be_bytes(pk_bytes[4..6].try_into().unwrap()) as u128;
            (hi << 96) | (mid << 80) | ((pk_bytes[6] as u128) << 72)
        }
        // Width 0: an empty OPK region packs to zero.
        _ => 0,
    }
}

/// A whole-PK sort key in the slice order of the bytes it was built from, so a
/// key sort needs no PK-byte tiebreak. Keys compare only
/// against keys built from the same number of bytes.
pub(crate) trait PkSortKey<'a>: RadixKey + Default {
    fn from_opk(opk: &'a [u8]) -> Self;
    /// The bytes [`Self::put`] writes for a key built from `stride` bytes.
    fn put_width(stride: usize) -> usize;
    /// The key's bytes big-endian into `out`, [`Self::put_width`] long: the
    /// `stride` bytes it was built from, then zeros.
    fn put(self, out: &mut [u8]);
}

/// Run `$body` with `$k` bound to the [`PkSortKey`] matching `$stride` — the one
/// statement of the width taxonomy the impls below define.
macro_rules! pk_width_dispatch {
    ($stride:expr, |$k:ident| $body:expr $(,)?) => {
        match $stride {
            0..=8 => {
                type $k<'k> = u64;
                $body
            }
            9..=16 => {
                type $k<'k> = u128;
                $body
            }
            17..=32 => {
                type $k<'k> = [u128; 2];
                $body
            }
            _ => {
                type $k<'k> = &'k [u8];
                $body
            }
        }
    };
}
pub(crate) use pk_width_dispatch;

impl PkSortKey<'_> for u64 {
    #[inline(always)]
    fn from_opk(opk: &[u8]) -> u64 {
        if opk.len() >= 8 {
            u64::from_be_bytes(opk[..8].try_into().unwrap())
        } else {
            // `pack_pk_be` left-aligns a narrow key with fixed-width register loads.
            (pack_pk_be(opk) >> 64) as u64
        }
    }
    #[inline(always)]
    fn put_width(_: usize) -> usize {
        size_of::<Self>()
    }
    #[inline(always)]
    fn put(self, out: &mut [u8]) {
        out.copy_from_slice(&self.to_be_bytes());
    }
}

impl PkSortKey<'_> for u128 {
    /// The leading 8 bytes, then the trailing 8 shifted past the bytes they
    /// share with them below 16 bytes: the key left-aligned, with no length
    /// dispatch, so a search's comparison closure stays small enough to inline.
    /// A byte stands in the key once, so a radix sort passes over it once.
    #[inline(always)]
    fn from_opk(opk: &[u8]) -> u128 {
        let len = opk.len();
        let hi = u64::from_be_bytes(opk[..8].try_into().unwrap()) as u128;
        let tail = u64::from_be_bytes(opk[len - 8..].try_into().unwrap()) << (8 * (16 - len));
        (hi << 64) | tail as u128
    }
    #[inline(always)]
    fn put_width(_: usize) -> usize {
        size_of::<Self>()
    }
    #[inline(always)]
    fn put(self, out: &mut [u8]) {
        out.copy_from_slice(&self.to_be_bytes());
    }
}

impl<'a> PkSortKey<'a> for &'a [u8] {
    #[inline(always)]
    fn from_opk(opk: &'a [u8]) -> Self {
        opk
    }
    #[inline(always)]
    fn put_width(stride: usize) -> usize {
        stride
    }
    #[inline(always)]
    fn put(self, out: &mut [u8]) {
        out.copy_from_slice(self);
    }
}

impl PkSortKey<'_> for [u128; 2] {
    #[inline(always)]
    fn from_opk(opk: &[u8]) -> [u128; 2] {
        // The leading 16 bytes, then the trailing 1..=16 left-aligned: array `Ord`
        // is lexicographic, as byte order is.
        let hi = u128::from_be_bytes(opk[..16].try_into().unwrap());
        if opk.len() == 32 {
            [hi, u128::from_be_bytes(opk[16..32].try_into().unwrap())]
        } else {
            [hi, pack_pk_be(&opk[16..])]
        }
    }
    #[inline(always)]
    fn put_width(_: usize) -> usize {
        32
    }
    #[inline(always)]
    fn put(self, out: &mut [u8]) {
        let (high, low) = out.split_at_mut(16);
        self[0].put(high);
        self[1].put(low);
    }
}

/// Sort and dedup `keys`, each exactly `stride` bytes.
pub fn pk_keys<'a>(stride: usize, keys: impl IntoIterator<Item = &'a [u8]>) -> PkKeys {
    assert!((1..=MAX_PK_BYTES).contains(&stride), "PkKeys: stride {stride}");
    let keys = keys
        .into_iter()
        .inspect(|k| assert_eq!(k.len(), stride, "PkKeys: key width"));
    match stride {
        // The width a PK most often has, read without `from_opk`'s width test.
        8 => sorted(8, keys.map(|k| u64::from_be_bytes(k.try_into().unwrap()))),
        _ => pk_width_dispatch!(stride, |K| sorted(stride, keys.map(K::from_opk))),
    }
}

/// Sort and dedup key images, each stored as its low `stride` bytes
/// big-endian. An image orders as those bytes do.
pub fn pk_keys_of_images(stride: usize, images: impl IntoIterator<Item = u128>) -> PkKeys {
    assert!((1..=NARROW_PK_MAX_BYTES).contains(&stride), "PkKeys: stride {stride}");
    let images = images.into_iter();
    // Left-aligned, as the sort key of those bytes is.
    match stride <= 8 {
        true => sorted(stride, images.map(|v| (v as u64) << (64 - 8 * stride))),
        false => sorted(stride, images.map(|v| v << (128 - 8 * stride))),
    }
}

/// `keys` sorted, then deduped while written: each a store of its whole
/// sort key, whose zero spill past `stride` the next key overwrites. A list
/// of at most two keys, and its bytes up to `MAX_PK_BYTES`, end up inline.
fn sorted<'a, K: PkSortKey<'a>>(stride: usize, keys: impl Iterator<Item = K>) -> PkKeys {
    let mut keys: SmallVec<[K; 2]> = keys.collect();
    radix_sort_by_key(&mut keys, |&k| k);
    let width = K::put_width(stride);
    let mut bytes = SmallVec::<[u8; MAX_PK_BYTES]>::from_elem(0, keys.len() * stride + width - stride);
    let mut at = 0;
    if let Some((&first, rest)) = keys.split_first() {
        first.put(&mut bytes[..width]);
        at = stride;
        let mut prev = first;
        for &k in rest {
            k.put(&mut bytes[at..at + width]);
            at += stride * (k != prev) as usize;
            prev = k;
        }
    }
    bytes.truncate(at);
    // Repeats or the spill may leave a list that fits inline on the heap.
    bytes.shrink_to_fit();
    PkKeys::from_sorted(stride, bytes)
}

#[cfg(test)]
#[path = "tests/sort_key.rs"]
mod tests;

#[cfg(test)]
#[path = "benches/sort_key.rs"]
mod bench;
