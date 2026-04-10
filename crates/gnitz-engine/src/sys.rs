//! Non-IPC Linux syscall wrappers: fallocate, fdatasync, NOCOW, madvise,
//! server socket, fd-limit.
//!
//! Split from ipc_sys.rs — these are pure OS helpers with no IPC dependency.

/// Pre-allocate blocks for fd. Returns 0 on success, -1 on error.
pub fn fallocate(fd: i32, length: i64) -> i32 {
    unsafe { libc::fallocate(fd, 0, 0, length as libc::off_t) }
}

/// Pre-allocate blocks without changing file size (FALLOC_FL_KEEP_SIZE).
/// Useful for WAL files opened with O_APPEND — allocates extents so future
/// writes avoid extent-tree allocation overhead on btrfs/XFS.
/// Returns 0 on success, -1 on error (non-fatal).
pub fn fallocate_keep_size(fd: i32, length: i64) -> i32 {
    unsafe { libc::fallocate(fd, libc::FALLOC_FL_KEEP_SIZE, 0, length as libc::off_t) }
}

/// fdatasync a file descriptor. Returns 0 on success, -1 on error.
pub fn fdatasync(fd: i32) -> i32 {
    unsafe { libc::fdatasync(fd) }
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
    if ptr.is_null() || size == 0 { return; }
    unsafe { libc::madvise(ptr as *mut libc::c_void, size, libc::MADV_HUGEPAGE); }
}

/// Pre-fault writable page-table entries for [ptr, ptr+size).
/// Uses MADV_POPULATE_WRITE (Linux 5.14+): installs writable PTEs and
/// triggers the filesystem page_mkwrite callback, so later writes don't
/// fault.  Unlike memset, this does not dirty page contents or pollute
/// CPU caches.  Returns 0 on success, -1 on error.
pub fn madvise_populate_write(ptr: *mut u8, size: usize) -> i32 {
    if ptr.is_null() || size == 0 { return 0; }
    unsafe { libc::madvise(ptr as *mut libc::c_void, size, libc::MADV_POPULATE_WRITE) }
}

/// Hint the kernel to read-ahead [ptr, ptr+size) into page cache.
/// Best-effort: ignores errors and is a no-op for null ptr or size 0.
pub fn madvise_willneed(ptr: *mut u8, size: usize) {
    if ptr.is_null() || size == 0 { return; }
    unsafe { libc::madvise(ptr as *mut libc::c_void, size, libc::MADV_WILLNEED); }
}

/// Hint the kernel to read-ahead [ptr, ptr+size) sequentially.
/// Best-effort: ignores errors and is a no-op for null ptr or size 0.
pub fn madvise_sequential(ptr: *mut u8, size: usize) {
    if ptr.is_null() || size == 0 { return; }
    unsafe { libc::madvise(ptr as *mut libc::c_void, size, libc::MADV_SEQUENTIAL); }
}

/// Create a Unix domain SOCK_STREAM server socket: socket + bind + listen.
/// Sets the listen socket to non-blocking.
/// Unlinks any existing socket at `path` before binding.
/// Returns the server fd, or a negative value on error.
pub fn server_create(path: &str) -> i32 {
    unsafe {
        let fd = libc::socket(libc::AF_UNIX, libc::SOCK_STREAM, 0);
        if fd < 0 {
            return -1;
        }

        let mut addr: libc::sockaddr_un = std::mem::zeroed();
        addr.sun_family = libc::AF_UNIX as libc::sa_family_t;
        let path_bytes = path.as_bytes();
        let max_len = addr.sun_path.len() - 1;
        let copy_len = path_bytes.len().min(max_len);
        for i in 0..copy_len {
            addr.sun_path[i] = path_bytes[i] as libc::c_char;
        }

        // Remove existing socket file
        let mut path_buf = [0u8; 108];
        let plen = path_bytes.len().min(107);
        path_buf[..plen].copy_from_slice(&path_bytes[..plen]);
        path_buf[plen] = 0;
        libc::unlink(path_buf.as_ptr() as *const libc::c_char);

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

        // Set non-blocking for the listen socket
        let flags = libc::fcntl(fd, libc::F_GETFL, 0);
        libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK);

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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fallocate() {
        use std::os::unix::io::AsRawFd;
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let raw_fd = tmp.as_file().as_raw_fd();
        let r = fallocate(raw_fd, 1048576);
        assert_eq!(r, 0, "fallocate failed");
        let mut stat: libc::stat = unsafe { std::mem::zeroed() };
        unsafe { libc::fstat(raw_fd, &mut stat); }
        assert_eq!(stat.st_size, 1048576);
    }

    #[test]
    fn test_fallocate_keep_size() {
        use std::os::unix::io::AsRawFd;
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let raw_fd = tmp.as_file().as_raw_fd();
        let r = fallocate_keep_size(raw_fd, 1048576);
        // May return -1 on filesystems that don't support KEEP_SIZE (non-fatal)
        if r == 0 {
            let mut stat: libc::stat = unsafe { std::mem::zeroed() };
            unsafe { libc::fstat(raw_fd, &mut stat); }
            assert_eq!(stat.st_size, 0, "KEEP_SIZE must not change st_size");
            assert!(stat.st_blocks > 0, "blocks should be allocated");
        }
    }

    #[test]
    fn test_try_set_nocow() {
        // On non-btrfs this returns -1, but must not crash
        let tmp = tempfile::NamedTempFile::new().unwrap();
        use std::os::unix::io::AsRawFd;
        let raw_fd = tmp.as_file().as_raw_fd();
        let _ = try_set_nocow(raw_fd); // just verify no crash/panic
    }

    #[test]
    fn test_fdatasync() {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        use std::os::unix::io::AsRawFd;
        let raw_fd = tmp.as_file().as_raw_fd();
        // Write something first
        unsafe {
            let data = b"hello";
            libc::write(raw_fd, data.as_ptr() as *const libc::c_void, 5);
        }
        let r = fdatasync(raw_fd);
        assert_eq!(r, 0);
    }

    #[test]
    fn test_raise_fd_limit() {
        // Should succeed (may be no-op if already ≥ 1024)
        let r = raise_fd_limit(1024);
        assert!(r >= 1024, "expected ≥1024, got {}", r);
    }

    #[test]
    fn test_server_create() {
        let path = "/tmp/gnitz_test_sys_server_create.sock";
        let fd = server_create(path);
        assert!(fd >= 0, "server_create failed: {}", fd);
        unsafe {
            libc::close(fd);
            libc::unlink(format!("{}\0", path).as_ptr() as *const libc::c_char);
        }
    }
}
