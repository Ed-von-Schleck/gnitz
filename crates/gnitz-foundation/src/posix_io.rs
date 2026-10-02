//! Syscall idioms `std` does not offer. Each returns `io::Result`, the errno
//! captured at the syscall.

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

#[cfg(test)]
#[path = "tests/posix_io.rs"]
mod tests;
