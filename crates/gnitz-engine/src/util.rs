//! Shared little-endian read/write helpers and POSIX I/O primitives.

use libc::c_int;

/// Write all bytes to fd, handling partial writes and EINTR.
/// Returns 0 on success, -3 on error.
pub unsafe fn write_all_fd(fd: c_int, data: &[u8]) -> i32 {
    let mut written: usize = 0;
    let total = data.len();
    while written < total {
        let ret = libc::write(
            fd,
            data[written..].as_ptr() as *const libc::c_void,
            total - written,
        );
        if ret < 0 {
            let e = *libc::__errno_location();
            if e == libc::EINTR {
                continue;
            }
            return -3;
        }
        written += ret as usize;
    }
    0
}

/// Read up to `buf.len()` bytes from fd. Returns bytes read, or -3 on error.
pub unsafe fn read_all_fd(fd: c_int, buf: &mut [u8]) -> i64 {
    let total = buf.len();
    let mut offset: usize = 0;
    while offset < total {
        let ret = libc::read(
            fd,
            buf[offset..].as_mut_ptr() as *mut libc::c_void,
            total - offset,
        );
        if ret < 0 {
            let e = *libc::__errno_location();
            if e == libc::EINTR {
                continue;
            }
            return -3;
        }
        if ret == 0 {
            break; // EOF
        }
        offset += ret as usize;
    }
    offset as i64
}

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

#[inline]
pub fn write_u32_le(buf: &mut [u8], off: usize, val: u32) {
    buf[off..off + 4].copy_from_slice(&val.to_le_bytes());
}

#[inline]
pub fn write_u64_le(buf: &mut [u8], off: usize, val: u64) {
    buf[off..off + 8].copy_from_slice(&val.to_le_bytes());
}

#[inline]
pub fn make_pk(lo: u64, hi: u64) -> u128 {
    ((hi as u128) << 64) | (lo as u128)
}

#[inline]
pub fn split_pk(pk: u128) -> (u64, u64) {
    (pk as u64, (pk >> 64) as u64)
}

pub fn cstr_from_buf(buf: &[u8]) -> &str {
    let end = buf.iter().position(|&b| b == 0).unwrap_or(buf.len());
    std::str::from_utf8(&buf[..end]).unwrap_or("")
}

#[inline]
pub fn align8(val: usize) -> usize {
    (val + 7) & !7
}
