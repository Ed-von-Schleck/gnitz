//! Syscall idioms `std` does not offer. Each returns `io::Result`, the errno
//! captured at the syscall.

use std::fs::File;
use std::io;
use std::os::fd::{AsRawFd, BorrowedFd};

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
#[cfg(target_os = "linux")]
pub fn memfd(name: &std::ffi::CStr) -> io::Result<File> {
    use std::os::fd::FromRawFd;
    // SAFETY: `name` is NUL-terminated.
    let fd = unsafe { libc::memfd_create(name.as_ptr(), libc::MFD_CLOEXEC) };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: a descriptor nothing else owns.
    Ok(unsafe { File::from_raw_fd(fd) })
}

/// Make the entries of directory `dir` durable.
pub fn fsync_dir(dir: impl AsRef<std::path::Path>) -> io::Result<()> {
    File::open(dir)?.sync_all()
}

/// Make `path`'s own entry durable: sync its parent, the working directory for
/// a bare name.
pub fn fsync_parent(path: impl AsRef<std::path::Path>) -> io::Result<()> {
    match path.as_ref().parent() {
        Some(p) if !p.as_os_str().is_empty() => fsync_dir(p),
        _ => fsync_dir("."),
    }
}

/// Create `dir` and its missing ancestors; once this returns, `dir`'s entry and
/// every entry it created are durable. `dir`'s is synced even where it already
/// exists, since a creator killed before its sync left it unsynced.
pub fn create_dir_all_durable(dir: impl AsRef<std::path::Path>) -> io::Result<()> {
    let dir = dir.as_ref();
    let missing: Vec<&std::path::Path> = dir
        .ancestors()
        .skip(1)
        .take_while(|d| !d.as_os_str().is_empty() && !d.is_dir())
        .collect();
    std::fs::create_dir_all(dir)?;
    std::iter::once(dir).chain(missing).try_for_each(fsync_parent)
}

/// Rename `from` to `to`, refused with `AlreadyExists` where `to` exists:
/// `std::fs::rename` replaces a file, or an empty directory, in its place.
#[cfg(target_os = "linux")]
pub fn rename_noreplace(from: &str, to: &str) -> io::Result<()> {
    let (from, to) = (std::ffi::CString::new(from)?, std::ffi::CString::new(to)?);
    // SAFETY: both paths are NUL-terminated and outlive the call.
    let r = unsafe {
        libc::renameat2(
            libc::AT_FDCWD,
            from.as_ptr(),
            libc::AT_FDCWD,
            to.as_ptr(),
            libc::RENAME_NOREPLACE,
        )
    };
    if r != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

#[cfg(test)]
#[path = "tests/posix_io.rs"]
mod tests;
