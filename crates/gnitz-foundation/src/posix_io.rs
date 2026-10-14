//! Syscall idioms `std` does not offer. Each returns `io::Result`, the errno
//! captured at the syscall.

use std::ffi::CStr;
use std::fs::File;
use std::io;
use std::os::fd::{AsRawFd, BorrowedFd, FromRawFd};

use libc::c_int;

/// Retry a raw syscall until it succeeds (`>= 0`) or fails with an error other
/// than EINTR, handing back what it returned. Not for a `write`: a partial
/// write returns a non-negative count, which this reports as success.
pub fn retry_eintr(mut f: impl FnMut() -> c_int) -> std::io::Result<c_int> {
    loop {
        let ret = f();
        if ret >= 0 {
            return Ok(ret);
        }
        let err = std::io::Error::last_os_error();
        if err.raw_os_error() != Some(libc::EINTR) {
            return Err(err);
        }
    }
}

/// `setsockopt(level, opt)` with a `c_int` value.
pub fn set_sockopt_int(fd: c_int, level: c_int, opt: c_int, val: c_int) -> std::io::Result<()> {
    // SAFETY: `val` lives across the call and the length passed is its size.
    let rc = unsafe {
        libc::setsockopt(
            fd,
            level,
            opt,
            (&raw const val).cast(),
            size_of::<c_int>() as libc::socklen_t,
        )
    };
    if rc < 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

/// A read-write mapping shared with every `fork()`ed child: `len` bytes of `fd`
/// from offset 0, or zeroed anonymous memory for `None`. `MAP_NORESERVE`, which
/// only the anonymous form is subject to: its pages are charged as they are
/// touched, so a mapping sized far above its occupancy is not refused for its size.
pub fn map_shared(fd: Option<BorrowedFd<'_>>, len: usize) -> io::Result<*mut u8> {
    let (flags, raw) = match fd {
        Some(fd) => (libc::MAP_SHARED | libc::MAP_NORESERVE, fd.as_raw_fd()),
        None => (libc::MAP_SHARED | libc::MAP_ANONYMOUS | libc::MAP_NORESERVE, -1),
    };
    // SAFETY: a fresh mapping at an address the kernel picks.
    let ptr = unsafe {
        libc::mmap(
            std::ptr::null_mut(),
            len,
            libc::PROT_READ | libc::PROT_WRITE,
            flags,
            raw,
            0,
        )
    };
    if ptr == libc::MAP_FAILED {
        return Err(io::Error::last_os_error());
    }
    Ok(ptr.cast())
}

/// A close-on-exec memfd named `name`.
pub fn memfd(name: &CStr) -> io::Result<File> {
    // SAFETY: `name` is NUL-terminated.
    let fd = unsafe { libc::memfd_create(name.as_ptr(), libc::MFD_CLOEXEC) };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: a descriptor nothing else owns.
    Ok(unsafe { File::from_raw_fd(fd) })
}

/// Make the entries of directory `dir` durable.
pub fn fsync_dir(dir: &str) -> io::Result<()> {
    File::open(dir)?.sync_all()
}

#[cfg(test)]
#[path = "tests/posix_io.rs"]
mod tests;
