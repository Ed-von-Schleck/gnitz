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
        if ret == 0 {
            // POSIX guarantees regular files never return 0 for a non-zero
            // count, but some device types can — without this guard the loop
            // would spin forever at 100% CPU. Treat it as an error.
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

/// `fdatasync` with EINTR retry. Returns the OS error on any other failure.
pub(crate) fn fdatasync_eintr(fd: c_int) -> std::io::Result<()> {
    loop {
        let rc = unsafe { libc::fdatasync(fd) };
        if rc >= 0 { return Ok(()); }
        let err = std::io::Error::last_os_error();
        if err.kind() != std::io::ErrorKind::Interrupted { return Err(err); }
    }
}

/// `fsync` with EINTR retry. Returns the OS error on any other failure.
pub(crate) fn fsync_eintr(fd: c_int) -> std::io::Result<()> {
    loop {
        let rc = unsafe { libc::fsync(fd) };
        if rc >= 0 { return Ok(()); }
        let err = std::io::Error::last_os_error();
        if err.kind() != std::io::ErrorKind::Interrupted { return Err(err); }
    }
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

pub fn cstr_from_buf(buf: &[u8]) -> &str {
    let end = buf.iter().position(|&b| b == 0).unwrap_or(buf.len());
    std::str::from_utf8(&buf[..end]).unwrap_or("")
}

pub use gnitz_wire::align8;

/// Unaligned write of a `u64` at `base + offset` bytes. Used by the
/// SAL and W2M mmap paths where offsets are computed from a `*mut u8`
/// base pointer and don't satisfy the alignment requirement of a
/// `*mut u64` dereference.
///
/// # Safety
/// `base + offset + 8` must lie inside a live, writable allocation.
#[inline]
pub(crate) unsafe fn write_u64_raw(base: *mut u8, offset: usize, val: u64) {
    (base.add(offset) as *mut u64).write_unaligned(val);
}

/// Unaligned read of a `u64` at `base + offset`. Companion to
/// `write_u64_raw`.
///
/// # Safety
/// `base + offset + 8` must lie inside a live, readable allocation.
#[inline]
pub(crate) unsafe fn read_u64_raw(base: *const u8, offset: usize) -> u64 {
    (base.add(offset) as *const u64).read_unaligned()
}

/// Unaligned write of a `u32` at `base + offset`. Companion to
/// `write_u64_raw`.
///
/// # Safety
/// `base + offset + 4` must lie inside a live, writable allocation.
#[inline]
pub(crate) unsafe fn write_u32_raw(base: *mut u8, offset: usize, val: u32) {
    (base.add(offset) as *mut u32).write_unaligned(val);
}

/// Unaligned read of a `u32` at `base + offset`. Companion to
/// `write_u64_raw`.
///
/// # Safety
/// `base + offset + 4` must lie inside a live, readable allocation.
#[inline]
pub(crate) unsafe fn read_u32_raw(base: *const u8, offset: usize) -> u32 {
    (base.add(offset) as *const u32).read_unaligned()
}

/// Run `f` under `catch_unwind`. On panic, returns
/// `Err("internal server error (panic in <op>)")`. Otherwise the closure's
/// `Result` is returned unchanged. Used in async handlers and the committer
/// task where a panic must not propagate (per async-invariants V.4 / V.7).
pub fn guard_panic<T, F>(op: &'static str, f: F) -> Result<T, String>
where F: FnOnce() -> Result<T, String>,
{
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)) {
        Ok(r) => r,
        Err(_) => Err(format!("internal server error (panic in {op})")),
    }
}

/// Raise RLIMIT_NOFILE soft limit to the hard limit.
/// Called once per process via `std::sync::Once`; safe to invoke from any test.
#[cfg(test)]
pub fn raise_fd_limit_for_tests() {
    use std::sync::Once;
    static INIT: Once = Once::new();
    INIT.call_once(|| unsafe {
        let mut rlim: libc::rlimit = std::mem::zeroed();
        if libc::getrlimit(libc::RLIMIT_NOFILE, &mut rlim) == 0 && rlim.rlim_cur < rlim.rlim_max {
            rlim.rlim_cur = rlim.rlim_max;
            libc::setrlimit(libc::RLIMIT_NOFILE, &rlim);
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Seek, SeekFrom};
    use std::os::unix::io::AsRawFd;

    #[test]
    fn test_write_all_fd_roundtrip() {
        // Guards the happy path of the partial-write loop (and that the new
        // ret==0 guard does not break a normal full write).
        let mut f = tempfile::tempfile().unwrap();
        let data = b"hello write_all_fd partial-write loop";
        let rc = unsafe { write_all_fd(f.as_raw_fd(), data) };
        assert_eq!(rc, 0);
        f.seek(SeekFrom::Start(0)).unwrap();
        let mut buf = Vec::new();
        f.read_to_end(&mut buf).unwrap();
        assert_eq!(&buf, data);
    }
}
