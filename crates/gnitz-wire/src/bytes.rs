//! Little-endian field I/O, typed views of LE regions, and bit-mask helpers.
//! `*_le(buf, off)` accesses a fixed-width field at `off`; `*_exact(cell)` reads
//! a cell whose width is the slice's length.

#[inline(always)]
pub fn read_u32_le(buf: &[u8], off: usize) -> u32 {
    u32::from_le_bytes(buf[off..off + 4].try_into().unwrap())
}

#[inline(always)]
pub fn read_u64_le(buf: &[u8], off: usize) -> u64 {
    u64::from_le_bytes(buf[off..off + 8].try_into().unwrap())
}

#[inline(always)]
pub fn read_i64_le(buf: &[u8], off: usize) -> i64 {
    i64::from_le_bytes(buf[off..off + 8].try_into().unwrap())
}

#[inline(always)]
pub fn write_u32_le(buf: &mut [u8], off: usize, val: u32) {
    buf[off..off + 4].copy_from_slice(&val.to_le_bytes());
}

#[inline(always)]
pub fn write_u64_le(buf: &mut [u8], off: usize, val: u64) {
    buf[off..off + 8].copy_from_slice(&val.to_le_bytes());
}

mod sealed {
    pub trait Sealed {}
}

/// Padding-free little-endian scalars whose every byte pattern is a valid
/// value — the types a region may be reinterpreted as. Sealed, so no other type
/// can claim that.
pub trait LeScalar: Copy + sealed::Sealed {}
macro_rules! le_scalar {
    ($($t:ty),*) => { $(impl sealed::Sealed for $t {} impl LeScalar for $t {})* };
}
le_scalar!(u32, u64, i64, u128);

/// A `&[T]` of LE scalars as the region bytes it already is, without a copy.
#[inline(always)]
pub fn as_le_bytes<T: LeScalar>(v: &[T]) -> &[u8] {
    // SAFETY: `size_of_val(v)` initialized bytes borrowed from `v`, consumed as
    // opaque bytes and never as typed values.
    unsafe { std::slice::from_raw_parts(v.as_ptr().cast::<u8>(), std::mem::size_of_val(v)) }
}

/// [`as_le_bytes`] for writing: every byte pattern is a valid `LeScalar`.
#[inline(always)]
pub fn as_le_bytes_mut<T: LeScalar>(v: &mut [T]) -> &mut [u8] {
    // SAFETY: `size_of_val(v)` initialized bytes exclusively borrowed from `v`; `LeScalar` admits
    // only padding-free integers, so any written value is a valid `T`.
    unsafe { std::slice::from_raw_parts_mut(v.as_mut_ptr().cast::<u8>(), std::mem::size_of_val(v)) }
}

/// The little-endian `T`s `src` holds, refused unless it is a whole number of
/// them.
pub fn le_scalars<T: LeScalar>(src: &[u8]) -> Result<Vec<T>, String> {
    let size = std::mem::size_of::<T>();
    if !src.len().is_multiple_of(size) {
        return Err(format!(
            "{} bytes are no whole number of {size}-byte scalars",
            src.len()
        ));
    }
    let mut v = Vec::new();
    extend_from_le_bytes(&mut v, src);
    Ok(v)
}

/// Append `src`, a whole number of little-endian `T`s, to `dst` in one copy.
pub fn extend_from_le_bytes<T: LeScalar>(dst: &mut Vec<T>, src: &[u8]) {
    let size = std::mem::size_of::<T>();
    assert!(
        src.len().is_multiple_of(size),
        "extend_from_le_bytes: {} bytes is not a whole number of {size}-byte scalars",
        src.len()
    );
    let (base, n) = (dst.len(), src.len() / size);
    dst.reserve(n);
    // SAFETY: `reserve` leaves room for `n` more `T`s, i.e. `src.len()` bytes past
    // `base`; the regions do not overlap; the copy initializes every element
    // `set_len` publishes, and `LeScalar` makes every byte pattern a valid `T`.
    unsafe {
        std::ptr::copy_nonoverlapping(src.as_ptr(), dst.as_mut_ptr().add(base).cast::<u8>(), src.len());
        dst.set_len(base + n);
    }
}

/// A `u64` with its low `n` bits set; all ones for `n >= 64`.
#[inline(always)]
pub const fn low_bits_mask(n: usize) -> u64 {
    if n < 64 {
        (1u64 << n) - 1
    } else {
        u64::MAX
    }
}

/// Yields the set bit positions of a mask, lowest first.
pub struct BitIter(pub u64);

impl Iterator for BitIter {
    type Item = usize;

    #[inline(always)]
    fn next(&mut self) -> Option<usize> {
        if self.0 == 0 {
            return None;
        }
        let i = self.0.trailing_zeros() as usize;
        self.0 &= self.0 - 1;
        Some(i)
    }
}

/// Append every maximal run of set bits below `n` to `out` as `[start, end)`,
/// one step per run. `bits` holds `n.div_ceil(64)` words; the bits of the last
/// one at or past `n` are ignored.
pub fn set_bit_runs(bits: &[u64], n: usize, out: &mut Vec<(usize, usize)>) {
    debug_assert_eq!(bits.len(), n.div_ceil(64), "set_bit_runs: one bit per row");
    set_bit_runs_iter(bits.iter().copied(), n, out);
}

/// [`set_bit_runs`] over words produced one at a time, so a caller that computes
/// them need not collect them first. `words` yields `n.div_ceil(64)` words; the
/// bits of the last one at or past `n` are ignored.
pub fn set_bit_runs_iter(words: impl IntoIterator<Item = u64>, n: usize, out: &mut Vec<(usize, usize)>) {
    let mut open: Option<usize> = None;
    let mut base = 0;
    for word in words {
        debug_assert!(base < n, "set_bit_runs_iter: one bit per row");
        // Only the last word has bits at or past `n`; every other mask is all ones.
        scan_bit_runs(&mut open, base, word & low_bits_mask(n - base), out);
        base += 64;
    }
    debug_assert!(base >= n, "set_bit_runs_iter: one bit per row");
    // A run still open reached the top of the last word, which is `n`.
    if let Some(s) = open {
        out.push((s, n));
    }
}

/// Append the runs of `word`, the 64 positions from `base`, that end inside it.
/// `open` is the start of a run that reached the top of the previous word: it
/// continues into this one only while the low bit is still set, and is left set
/// when this word's run reaches its top.
#[inline(always)]
fn scan_bit_runs(open: &mut Option<usize>, base: usize, word: u64, out: &mut Vec<(usize, usize)>) {
    if word & 1 == 0 {
        if let Some(s) = open.take() {
            out.push((s, base));
        }
    }
    // Unscanned bits stay in place, so no shift below reaches 64.
    let mut rest = word;
    while rest != 0 {
        let start_bit = rest.trailing_zeros() as usize;
        // Filling in below the run's start is what makes `trailing_ones`
        // measure the run rather than the gap under it.
        let end_bit = (rest | ((1u64 << start_bit) - 1)).trailing_ones() as usize;
        let start = open.take().unwrap_or(base + start_bit);
        if end_bit == 64 {
            *open = Some(start); // reaches the top; the next word may continue it
            break;
        }
        out.push((start, base + end_bit));
        rest &= !((1u64 << end_bit) - 1);
    }
}

#[cfg(test)]
#[path = "tests/bytes.rs"]
mod tests;
