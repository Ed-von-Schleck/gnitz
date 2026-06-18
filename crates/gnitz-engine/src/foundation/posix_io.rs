//! POSIX I/O primitives and non-IPC Linux syscall wrappers: fd read/write with
//! EINTR/partial-write handling, fdatasync/fsync, fallocate, ftruncate, NOCOW,
//! madvise, server socket, fd-limit, plus the `catch_unwind` panic guard.

use libc::c_int;

/// Write all bytes to fd, handling partial writes and EINTR.
/// Returns 0 on success, -3 on error.
pub unsafe fn write_all_fd(fd: c_int, data: &[u8]) -> i32 {
    let mut written: usize = 0;
    let total = data.len();
    while written < total {
        let ret = libc::write(fd, data[written..].as_ptr() as *const libc::c_void, total - written);
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
        let ret = libc::read(fd, buf[offset..].as_mut_ptr() as *mut libc::c_void, total - offset);
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
        if rc >= 0 {
            return Ok(());
        }
        let err = std::io::Error::last_os_error();
        if err.kind() != std::io::ErrorKind::Interrupted {
            return Err(err);
        }
    }
}

/// `fsync` with EINTR retry. Returns the OS error on any other failure.
pub(crate) fn fsync_eintr(fd: c_int) -> std::io::Result<()> {
    loop {
        let rc = unsafe { libc::fsync(fd) };
        if rc >= 0 {
            return Ok(());
        }
        let err = std::io::Error::last_os_error();
        if err.kind() != std::io::ErrorKind::Interrupted {
            return Err(err);
        }
    }
}

/// Pre-allocate blocks for fd. Returns 0 on success, -1 on error.
pub fn fallocate(fd: i32, length: i64) -> i32 {
    unsafe { libc::fallocate(fd, 0, 0, length as libc::off_t) }
}

/// Set file size via ftruncate. Returns 0 on success, -1 on error.
pub fn ftruncate(fd: i32, size: i64) -> i32 {
    unsafe { libc::ftruncate(fd, size as libc::off_t) }
}

/// Set FS_NOCOW_FL on fd (btrfs in-place overwrites).
/// Silently ignored on non-btrfs filesystems.
/// Returns 0 on success, -1 on error (non-fatal).
pub fn try_set_nocow(fd: i32) -> i32 {
    // FS_IOC_GETFLAGS = 0x80086601, FS_IOC_SETFLAGS = 0x40086602
    // FS_NOCOW_FL = 0x00800000
    const FS_IOC_GETFLAGS: libc::c_ulong = 0x80086601;
    const FS_IOC_SETFLAGS: libc::c_ulong = 0x40086602;
    const FS_NOCOW_FL: libc::c_int = 0x00800000;

    let mut flags: libc::c_int = 0;
    unsafe {
        if libc::ioctl(fd, FS_IOC_GETFLAGS, &mut flags) < 0 {
            return -1;
        }
        flags |= FS_NOCOW_FL;
        libc::ioctl(fd, FS_IOC_SETFLAGS, &flags)
    }
}

/// Hint the kernel to back [ptr, ptr+size) with transparent hugepages.
/// Best-effort: ignores errors and is a no-op for null ptr or size 0.
/// For anonymous private memory: requires `enabled` = `madvise` or `always`.
/// For memfd/shmem: requires `shmem_enabled` = `advise` or `within_size`.
/// For writable file-backed mmap: silently ignored by the kernel.
pub fn madvise_hugepage(ptr: *mut u8, size: usize) {
    if ptr.is_null() || size == 0 {
        return;
    }
    unsafe {
        libc::madvise(ptr as *mut libc::c_void, size, libc::MADV_HUGEPAGE);
    }
}

/// Pre-fault writable page-table entries for [ptr, ptr+size).
/// Uses MADV_POPULATE_WRITE (Linux 5.14+): installs writable PTEs and
/// triggers the filesystem page_mkwrite callback, so later writes don't
/// fault.  Unlike memset, this does not dirty page contents or pollute
/// CPU caches.  Returns 0 on success, -1 on error.
pub fn madvise_populate_write(ptr: *mut u8, size: usize) -> i32 {
    if ptr.is_null() || size == 0 {
        return 0;
    }
    unsafe { libc::madvise(ptr as *mut libc::c_void, size, libc::MADV_POPULATE_WRITE) }
}

/// Hint the kernel to read-ahead [ptr, ptr+size) into page cache.
/// Best-effort: ignores errors and is a no-op for null ptr or size 0.
pub fn madvise_willneed(ptr: *mut u8, size: usize) {
    if ptr.is_null() || size == 0 {
        return;
    }
    unsafe {
        libc::madvise(ptr as *mut libc::c_void, size, libc::MADV_WILLNEED);
    }
}

/// Hint the kernel to read-ahead [ptr, ptr+size) sequentially.
/// Best-effort: ignores errors and is a no-op for null ptr or size 0.
pub fn madvise_sequential(ptr: *mut u8, size: usize) {
    if ptr.is_null() || size == 0 {
        return;
    }
    unsafe {
        libc::madvise(ptr as *mut libc::c_void, size, libc::MADV_SEQUENTIAL);
    }
}

/// Create a Unix domain SOCK_STREAM server socket: socket + bind + listen.
/// Sets the listen socket to non-blocking.
/// Unlinks any existing socket at `path` before binding.
/// Returns the server fd, or a negative value on error.
#[allow(clippy::needless_range_loop)]
pub fn server_create(path: &str) -> i32 {
    unsafe {
        let fd = libc::socket(libc::AF_UNIX, libc::SOCK_STREAM, 0);
        if fd < 0 {
            return -1;
        }

        let mut addr: libc::sockaddr_un = std::mem::zeroed();
        addr.sun_family = libc::AF_UNIX as libc::sa_family_t;
        let path_bytes = path.as_bytes();
        // Reject paths that would not fit with the null terminator.
        // Consistent with the client-side check in gnitz-core/src/protocol/transport.rs.
        if path_bytes.len() >= addr.sun_path.len() {
            libc::close(fd);
            return -1;
        }
        for i in 0..path_bytes.len() {
            addr.sun_path[i] = path_bytes[i] as libc::c_char;
        }

        // addr is zeroed so sun_path is already null-terminated after the copy above.
        libc::unlink(addr.sun_path.as_ptr());

        if libc::bind(
            fd,
            &addr as *const libc::sockaddr_un as *const libc::sockaddr,
            std::mem::size_of::<libc::sockaddr_un>() as libc::socklen_t,
        ) < 0
        {
            libc::close(fd);
            return -2;
        }
        if libc::listen(fd, 1024) < 0 {
            libc::close(fd);
            return -3;
        }

        // Set non-blocking for the listen socket.
        let flags = libc::fcntl(fd, libc::F_GETFL, 0);
        if flags < 0 || libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) < 0 {
            libc::close(fd);
            return -4;
        }

        fd
    }
}

/// Raise RLIMIT_NOFILE soft limit to `target` (capped by hard limit).
/// Returns the new soft limit, or -1 on failure.
pub fn raise_fd_limit(target: u64) -> i64 {
    unsafe {
        let mut rl: libc::rlimit = std::mem::zeroed();
        if libc::getrlimit(libc::RLIMIT_NOFILE, &mut rl) != 0 {
            return -1;
        }
        if rl.rlim_cur >= target as libc::rlim_t {
            return rl.rlim_cur as i64;
        }
        rl.rlim_cur = target as libc::rlim_t;
        if rl.rlim_cur > rl.rlim_max {
            rl.rlim_cur = rl.rlim_max;
        }
        if libc::setrlimit(libc::RLIMIT_NOFILE, &rl) != 0 {
            return -1;
        }
        rl.rlim_cur as i64
    }
}

/// Run `f` under `catch_unwind`. On panic, returns
/// `Err("internal server error (panic in <op>)")`. Otherwise the closure's
/// `Result` is returned unchanged. Used in async handlers and the committer
/// task where a panic must not propagate (per async-invariants V.4 / V.7).
pub fn guard_panic<T, F>(op: &'static str, f: F) -> Result<T, String>
where
    F: FnOnce() -> Result<T, String>,
{
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)) {
        Ok(r) => r,
        Err(_) => Err(format!("internal server error (panic in {op})")),
    }
}

/// Raise RLIMIT_NOFILE soft limit to the hard limit.
/// Called once per process via `std::sync::Once`; safe to invoke from any test.
#[cfg(test)]
pub(crate) fn raise_fd_limit_for_tests() {
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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

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

    #[test]
    fn test_fallocate() {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let raw_fd = tmp.as_file().as_raw_fd();
        let r = fallocate(raw_fd, 1048576);
        assert_eq!(r, 0, "fallocate failed");
        let mut stat: libc::stat = unsafe { std::mem::zeroed() };
        unsafe {
            libc::fstat(raw_fd, &mut stat);
        }
        assert_eq!(stat.st_size, 1048576);
    }

    #[test]
    fn test_try_set_nocow() {
        // On non-btrfs this returns -1, but must not crash
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let raw_fd = tmp.as_file().as_raw_fd();
        let _ = try_set_nocow(raw_fd); // just verify no crash/panic
    }

    #[test]
    fn test_raise_fd_limit() {
        // Should succeed (may be no-op if already ≥ 1024)
        let r = raise_fd_limit(1024);
        assert!(r >= 1024, "expected ≥1024, got {r}");
    }

    #[test]
    fn test_server_create() {
        let path = "/tmp/gnitz_test_sys_server_create.sock";
        let fd = server_create(path);
        assert!(fd >= 0, "server_create failed: {fd}");
        unsafe {
            libc::close(fd);
            libc::unlink(format!("{path}\0").as_ptr() as *const libc::c_char);
        }
    }

    #[test]
    fn test_server_create_path_too_long() {
        // sun_path is 108 bytes; a path of exactly 108 bytes has no room for the null terminator
        let long_path = "/tmp/".to_string() + &"a".repeat(110);
        assert!(long_path.len() >= 108);
        let fd = server_create(&long_path);
        assert!(fd < 0, "expected error for overlong path, got fd={fd}");
    }

    #[test]
    fn test_server_create_is_nonblocking() {
        let path = "/tmp/gnitz_test_sys_server_nonblocking.sock";
        let fd = server_create(path);
        assert!(fd >= 0, "server_create failed: {fd}");
        let flags = unsafe { libc::fcntl(fd, libc::F_GETFL, 0) };
        unsafe {
            libc::close(fd);
            libc::unlink(format!("{path}\0").as_ptr() as *const libc::c_char);
        }
        assert!(flags >= 0, "F_GETFL failed: {flags}");
        assert!(
            flags & libc::O_NONBLOCK != 0,
            "socket is not non-blocking, flags={flags:#o}"
        );
    }
}
