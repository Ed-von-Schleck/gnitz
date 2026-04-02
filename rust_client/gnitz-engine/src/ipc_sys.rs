//! Linux syscall wrappers for IPC: eventfd, fallocate, NOCOW, fdatasync.
//!
//! Replaces gnitz/server/eventfd_ffi.py and gnitz/server/sal_ffi.py.

/// Create a non-blocking, close-on-exec eventfd. Returns fd or -1 on error.
pub fn eventfd_create() -> i32 {
    unsafe { libc::eventfd(0, libc::EFD_NONBLOCK | libc::EFD_CLOEXEC) }
}

/// Signal an eventfd (increment counter by 1). Returns 0 on success, -1 on error.
pub fn eventfd_signal(efd: i32) -> i32 {
    let v: u64 = 1;
    loop {
        let n = unsafe {
            libc::write(efd, &v as *const u64 as *const libc::c_void, 8)
        };
        if n == 8 {
            return 0;
        }
        if n < 0 {
            let e = unsafe { *libc::__errno_location() };
            if e == libc::EINTR {
                continue;
            }
        }
        return -1;
    }
}

/// Wait for an eventfd to become readable.
/// Returns >0 if ready (counter drained), 0 on timeout, <0 on error.
pub fn eventfd_wait(efd: i32, timeout_ms: i32) -> i32 {
    let mut pfd = libc::pollfd {
        fd: efd,
        events: libc::POLLIN,
        revents: 0,
    };
    let r = loop {
        let r = unsafe { libc::poll(&mut pfd, 1, timeout_ms) };
        if r < 0 {
            let e = unsafe { *libc::__errno_location() };
            if e == libc::EINTR {
                continue;
            }
        }
        break r;
    };
    if r > 0 {
        let mut v: u64 = 0;
        unsafe {
            libc::read(efd, &mut v as *mut u64 as *mut libc::c_void, 8);
        }
    }
    r
}

/// Wait for any of the eventfds to become readable. All ready fds are drained.
/// Returns >0 if any ready, 0 on timeout, <0 on error.
///
/// Max 64 fds (matches C implementation).
pub fn eventfd_wait_any(efds: &[i32], timeout_ms: i32) -> i32 {
    let n = efds.len().min(64);
    if n == 0 {
        return 0;
    }
    let mut pfds = [libc::pollfd {
        fd: 0,
        events: 0,
        revents: 0,
    }; 64];
    for i in 0..n {
        pfds[i].fd = efds[i];
        pfds[i].events = libc::POLLIN;
    }
    let r = loop {
        let r = unsafe { libc::poll(pfds.as_mut_ptr(), n as libc::nfds_t, timeout_ms) };
        if r < 0 {
            let e = unsafe { *libc::__errno_location() };
            if e == libc::EINTR {
                continue;
            }
        }
        break r;
    };
    if r > 0 {
        let mut v: u64 = 0;
        for i in 0..n {
            if pfds[i].revents & libc::POLLIN != 0 {
                unsafe {
                    libc::read(pfds[i].fd, &mut v as *mut u64 as *mut libc::c_void, 8);
                }
            }
        }
    }
    r
}

/// Pre-allocate blocks for fd. Returns 0 on success, -1 on error.
pub fn fallocate(fd: i32, length: i64) -> i32 {
    unsafe { libc::fallocate(fd, 0, 0, length as libc::off_t) }
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

/// fdatasync a file descriptor. Returns 0 on success, -1 on error.
pub fn fdatasync(fd: i32) -> i32 {
    unsafe { libc::fdatasync(fd) }
}

// ---------------------------------------------------------------------------
// FFI exports
// ---------------------------------------------------------------------------

#[no_mangle]
pub extern "C" fn gnitz_eventfd_create() -> i32 {
    eventfd_create()
}

#[no_mangle]
pub extern "C" fn gnitz_eventfd_signal(efd: i32) -> i32 {
    eventfd_signal(efd)
}

#[no_mangle]
pub extern "C" fn gnitz_eventfd_wait(efd: i32, timeout_ms: i32) -> i32 {
    eventfd_wait(efd, timeout_ms)
}

#[no_mangle]
pub extern "C" fn gnitz_eventfd_wait_any(
    efds: *const i32,
    n: i32,
    timeout_ms: i32,
) -> i32 {
    if efds.is_null() || n <= 0 {
        return 0;
    }
    let slice = unsafe { std::slice::from_raw_parts(efds, n as usize) };
    eventfd_wait_any(slice, timeout_ms)
}

#[no_mangle]
pub extern "C" fn gnitz_fallocate(fd: i32, length: i64) -> i32 {
    fallocate(fd, length)
}

#[no_mangle]
pub extern "C" fn gnitz_try_set_nocow(fd: i32) -> i32 {
    try_set_nocow(fd)
}

#[no_mangle]
pub extern "C" fn gnitz_fdatasync(fd: i32) -> i32 {
    fdatasync(fd)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_eventfd_create_close() {
        let fd = eventfd_create();
        assert!(fd >= 0, "eventfd_create failed: {}", fd);
        unsafe { libc::close(fd); }
    }

    #[test]
    fn test_eventfd_signal_wait() {
        let fd = eventfd_create();
        assert!(fd >= 0);
        assert_eq!(eventfd_signal(fd), 0);
        let r = eventfd_wait(fd, 1000);
        assert!(r > 0, "expected >0, got {}", r);
        unsafe { libc::close(fd); }
    }

    #[test]
    fn test_eventfd_wait_timeout() {
        let fd = eventfd_create();
        assert!(fd >= 0);
        let r = eventfd_wait(fd, 10);
        assert_eq!(r, 0, "expected 0 (timeout), got {}", r);
        unsafe { libc::close(fd); }
    }

    #[test]
    fn test_eventfd_wait_any() {
        let fd0 = eventfd_create();
        let fd1 = eventfd_create();
        let fd2 = eventfd_create();
        assert!(fd0 >= 0 && fd1 >= 0 && fd2 >= 0);

        // Signal only fd1
        eventfd_signal(fd1);
        let efds = [fd0, fd1, fd2];
        let r = eventfd_wait_any(&efds, 1000);
        assert!(r > 0, "expected >0, got {}", r);

        // All drained — no more ready
        let r2 = eventfd_wait_any(&efds, 10);
        assert_eq!(r2, 0);

        unsafe {
            libc::close(fd0);
            libc::close(fd1);
            libc::close(fd2);
        }
    }

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
    fn test_cross_process_atomic() {
        use std::sync::atomic::{AtomicU64, Ordering};

        // Create shared mmap region
        let fd = unsafe {
            libc::memfd_create(b"test\0".as_ptr() as *const libc::c_char, 0)
        };
        assert!(fd >= 0);
        unsafe { libc::ftruncate(fd, 4096); }
        let ptr = unsafe {
            libc::mmap(
                std::ptr::null_mut(), 4096,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED, fd, 0,
            )
        };
        assert_ne!(ptr, libc::MAP_FAILED);

        let efd = eventfd_create();
        assert!(efd >= 0);

        let pid = unsafe { libc::fork() };
        if pid == 0 {
            // Child: write atomic value, signal parent
            let atomic = unsafe { &*(ptr as *const AtomicU64) };
            atomic.store(0xDEADBEEF, Ordering::Release);
            eventfd_signal(efd);
            unsafe { libc::_exit(0); }
        }

        // Parent: wait for child signal, read atomic
        let r = eventfd_wait(efd, 5000);
        assert!(r > 0, "eventfd_wait timed out");
        let atomic = unsafe { &*(ptr as *const AtomicU64) };
        let val = atomic.load(Ordering::Acquire);
        assert_eq!(val, 0xDEADBEEF);

        // Cleanup
        let mut status: i32 = 0;
        unsafe {
            libc::waitpid(pid, &mut status, 0);
            libc::munmap(ptr, 4096);
            libc::close(fd);
            libc::close(efd);
        }
    }
}
