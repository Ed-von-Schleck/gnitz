//! Linux syscall wrappers for IPC: eventfd, memfd, mmap_shared.

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

/// Create an anonymous memory-backed fd with MFD_CLOEXEC.
/// Returns fd on success, -1 on error.
pub fn memfd_create(name: &[u8]) -> i32 {
    // name must be null-terminated for the syscall
    let mut buf = [0u8; 64];
    let len = name.len().min(62);
    buf[..len].copy_from_slice(&name[..len]);
    buf[len] = 0;
    unsafe { libc::memfd_create(buf.as_ptr() as *const libc::c_char, libc::MFD_CLOEXEC) }
}

/// mmap a shared, read-write region of `size` bytes backed by `fd`.
/// Returns the mapped pointer, or null on error.
pub fn mmap_shared(fd: i32, size: usize) -> *mut u8 {
    let ptr = unsafe {
        libc::mmap(
            std::ptr::null_mut(),
            size,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_SHARED,
            fd,
            0,
        )
    };
    if ptr == libc::MAP_FAILED {
        std::ptr::null_mut()
    } else {
        ptr as *mut u8
    }
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

    #[test]
    fn test_memfd_create_and_ftruncate() {
        let fd = memfd_create(b"test_memfd");
        assert!(fd >= 0, "memfd_create failed: {}", fd);
        assert_eq!(crate::sys::ftruncate(fd, 4096), 0);
        // Verify size
        let mut stat: libc::stat = unsafe { std::mem::zeroed() };
        unsafe { libc::fstat(fd, &mut stat); }
        assert_eq!(stat.st_size, 4096);
        unsafe { libc::close(fd); }
    }

    #[test]
    fn test_mmap_shared() {
        let fd = memfd_create(b"test_mmap");
        assert!(fd >= 0);
        assert_eq!(crate::sys::ftruncate(fd, 8192), 0);
        let ptr = mmap_shared(fd, 8192);
        assert!(!ptr.is_null(), "mmap_shared returned null");
        // Write and read back
        unsafe {
            *ptr = 42;
            assert_eq!(*ptr, 42);
            libc::munmap(ptr as *mut libc::c_void, 8192);
            libc::close(fd);
        }
    }
}
