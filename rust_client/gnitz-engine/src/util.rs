//! Shared little-endian read/write helpers for binary format handling.

#[inline]
pub fn read_u32_le(buf: &[u8], off: usize) -> u32 {
    u32::from_le_bytes(buf[off..off + 4].try_into().unwrap())
}

#[inline]
pub fn read_u64_le(buf: &[u8], off: usize) -> u64 {
    u64::from_le_bytes(buf[off..off + 8].try_into().unwrap())
}

#[inline]
pub fn read_i64_le(buf: &[u8], off: usize) -> i64 {
    i64::from_le_bytes(buf[off..off + 8].try_into().unwrap())
}

/// Read a little-endian u64 from a raw pointer + byte offset.
#[inline]
pub unsafe fn read_u64_at(base: *const u8, off: usize) -> u64 {
    u64::from_le_bytes(std::slice::from_raw_parts(base.add(off), 8).try_into().unwrap())
}

/// Read a little-endian i64 from a raw pointer + byte offset.
#[inline]
pub unsafe fn read_i64_at(base: *const u8, off: usize) -> i64 {
    i64::from_le_bytes(std::slice::from_raw_parts(base.add(off), 8).try_into().unwrap())
}

#[inline]
pub fn write_u32_le(buf: &mut [u8], off: usize, val: u32) {
    buf[off..off + 4].copy_from_slice(&val.to_le_bytes());
}

#[inline]
pub fn write_u64_le(buf: &mut [u8], off: usize, val: u64) {
    buf[off..off + 8].copy_from_slice(&val.to_le_bytes());
}
