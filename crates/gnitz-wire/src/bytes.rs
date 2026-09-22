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
le_scalar!(u32, u64, i64);

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

/// A 1/2/4/8-byte little-endian signed cell, sign-extended to `i64`.
#[inline(always)]
pub fn read_signed_exact(bytes: &[u8]) -> i64 {
    match bytes.len() {
        1 => bytes[0] as i8 as i64,
        2 => i16::from_le_bytes(bytes.try_into().unwrap()) as i64,
        4 => i32::from_le_bytes(bytes.try_into().unwrap()) as i64,
        8 => i64::from_le_bytes(bytes.try_into().unwrap()),
        _ => unreachable!("read_signed_exact: unexpected column width"),
    }
}

/// A 1/2/4/8-byte little-endian unsigned cell, zero-extended to `u64`.
#[inline(always)]
pub fn read_unsigned_exact(bytes: &[u8]) -> u64 {
    match bytes.len() {
        1 => bytes[0] as u64,
        2 => u16::from_le_bytes(bytes.try_into().unwrap()) as u64,
        4 => u32::from_le_bytes(bytes.try_into().unwrap()) as u64,
        8 => u64::from_le_bytes(bytes.try_into().unwrap()),
        _ => unreachable!("read_unsigned_exact: unexpected column width"),
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

#[inline(always)]
pub fn write_u32_le(buf: &mut [u8], off: usize, val: u32) {
    buf[off..off + 4].copy_from_slice(&val.to_le_bytes());
}

#[inline(always)]
pub fn write_u64_le(buf: &mut [u8], off: usize, val: u64) {
    buf[off..off + 8].copy_from_slice(&val.to_le_bytes());
}

#[cfg(test)]
#[path = "tests/bytes.rs"]
mod tests;
