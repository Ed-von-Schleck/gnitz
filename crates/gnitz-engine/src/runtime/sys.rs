//! Linux syscall wrappers for IPC: eventfd, memfd, mmap_shared, futex.

use std::sync::atomic::AtomicU32;

/// `futex2(2)` flags byte for a 32-bit atomic. Matches the kernel constant
/// `FUTEX2_SIZE_U32` (=2). No `FUTEX2_PRIVATE` bit — W2M is `MAP_SHARED`
/// across `fork()`.
pub const FUTEX2_SIZE_U32: u32 = 2;

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

/// Block on a futex at `ptr` until its value differs from `expected` or
/// a wake arrives. Uses the `v1` futex(2) syscall (opcode `FUTEX_WAIT`)
/// — NOT the `FUTEX_PRIVATE_FLAG` variant, since W2M regions are
/// `MAP_SHARED` across `fork()`. `timeout_ms < 0` means "block forever".
///
/// Returns the syscall return value: 0 on successful wake,
/// -1 on error (inspect `errno` — EAGAIN means value already differed,
/// ETIMEDOUT means the timespec elapsed).
pub fn futex_wait_u32(ptr: *const AtomicU32, expected: u32, timeout_ms: i32) -> i32 {
    let ts = libc::timespec {
        tv_sec: (timeout_ms as i64) / 1000,
        tv_nsec: ((timeout_ms as i64) % 1000) * 1_000_000,
    };
    let ts_ptr: *const libc::timespec = if timeout_ms < 0 {
        std::ptr::null()
    } else {
        &ts
    };
    unsafe {
        libc::syscall(
            libc::SYS_futex,
            ptr as *const libc::c_void,
            libc::FUTEX_WAIT,
            expected as libc::c_int,
            ts_ptr,
            std::ptr::null::<u32>(),
            0u32,
        ) as i32
    }
}

/// Wake up at most `n_waiters` futex waiters parked on `ptr` via v1
/// `FUTEX_WAKE` (no `FUTEX_PRIVATE_FLAG` — W2M is shared). Returns
/// the number of waiters woken, or -1 on error.
pub fn futex_wake_u32(ptr: *const AtomicU32, n_waiters: u32) -> i32 {
    unsafe {
        libc::syscall(
            libc::SYS_futex,
            ptr as *const libc::c_void,
            libc::FUTEX_WAKE,
            n_waiters as libc::c_int,
            std::ptr::null::<libc::timespec>(),
            std::ptr::null::<u32>(),
            0u32,
        ) as i32
    }
}

/// Return the errno of the most recent failed syscall.
#[inline]
pub fn errno() -> i32 {
    unsafe { *libc::__errno_location() }
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

    /// A parent `futex_wait_u32` against a `MAP_SHARED` memfd unblocks when
    /// a forked child stores a new value and issues `futex_wake_u32`. Proves
    /// the cross-process shared-futex contract the W2M migration depends on.
    #[test]
    fn test_cross_process_futex_on_mapshared() {
        use std::sync::atomic::Ordering;

        let fd = memfd_create(b"test_futex_shared");
        assert!(fd >= 0);
        assert_eq!(crate::sys::ftruncate(fd, 4096), 0);
        let ptr = mmap_shared(fd, 4096);
        assert!(!ptr.is_null());

        let atomic_ptr = ptr as *mut AtomicU32;
        unsafe { (*atomic_ptr).store(7, Ordering::Release); }

        let pid = unsafe { libc::fork() };
        if pid == 0 {
            // Child: bump the atomic, then wake the parent.
            unsafe { (*atomic_ptr).store(8, Ordering::Release); }
            let _ = futex_wake_u32(atomic_ptr as *const AtomicU32, 1);
            unsafe { libc::_exit(0); }
        }

        // Parent: wait on atomic=7 with a 5-second timeout. Return <=0 via
        // timeout = failure. Value-already-differed (EAGAIN) is also a
        // successful proof — the child got ahead of us.
        let rc = futex_wait_u32(atomic_ptr as *const AtomicU32, 7, 5000);
        let errno = unsafe { *libc::__errno_location() };
        // 0 = woke normally; -1 w/ EAGAIN = value was already != 7 by the
        // time we issued the syscall (equally acceptable proof).
        assert!(
            rc == 0 || (rc == -1 && errno == libc::EAGAIN),
            "futex_wait_u32 returned rc={} errno={}", rc, errno,
        );
        let final_val = unsafe { (*atomic_ptr).load(Ordering::Acquire) };
        assert_eq!(final_val, 8);

        let mut status: i32 = 0;
        unsafe {
            libc::waitpid(pid, &mut status, 0);
            libc::munmap(ptr as *mut libc::c_void, 4096);
            libc::close(fd);
        }
    }

    /// Submit a `FutexWaitV` SQE on a local io_uring, fork a child that
    /// updates the atomic and wakes it via raw FUTEX_WAKE, drain the CQE
    /// in the parent. Smoke test for the reactor's new FUTEX_WAITV wiring.
    #[test]
    fn test_futex_waitv_via_io_uring() {
        use std::sync::atomic::Ordering;
        use io_uring::{opcode, types, IoUring};

        let fd = memfd_create(b"test_futex_waitv");
        assert!(fd >= 0);
        assert_eq!(crate::sys::ftruncate(fd, 4096), 0);
        let ptr = mmap_shared(fd, 4096);
        assert!(!ptr.is_null());

        let atomic_ptr = ptr as *mut AtomicU32;
        unsafe { (*atomic_ptr).store(0, Ordering::Release); }

        let mut ring = match IoUring::new(8) {
            Ok(r) => r,
            Err(e) => panic!("IoUring::new: {}", e),
        };

        // One-entry FutexWaitV array (heap-stable), waiting for value 0.
        let futexv: Box<[types::FutexWaitV; 1]> = Box::new([
            types::FutexWaitV::new()
                .val(0)
                .uaddr(atomic_ptr as u64)
                .flags(FUTEX2_SIZE_U32),
        ]);

        let entry = opcode::FutexWaitV::new(futexv.as_ptr() as *const types::FutexWaitV, 1)
            .build()
            .user_data(0xAABBCCDD);
        unsafe {
            ring.submission().push(&entry).expect("sqe push");
        }
        // Flush the SQE to the kernel without blocking for a CQE.
        ring.submit().expect("submit");

        let pid = unsafe { libc::fork() };
        if pid == 0 {
            // Child: brief delay so the parent has time to park, then wake.
            unsafe { libc::usleep(50_000); }
            unsafe { (*atomic_ptr).store(1, Ordering::Release); }
            let _ = futex_wake_u32(atomic_ptr as *const AtomicU32, 1);
            unsafe { libc::_exit(0); }
        }

        // Parent: block until at least one CQE arrives.
        ring.submitter().submit_and_wait(1).expect("submit_and_wait");
        let cqe = ring.completion().next().expect("cqe");
        assert_eq!(
            cqe.user_data(), 0xAABBCCDD,
            "user_data must round-trip; got 0x{:X}", cqe.user_data(),
        );
        // Unsupported opcode → -EINVAL or -ENOSYS. Success → 0 or >0.
        // Val mismatch → -EAGAIN (would happen if the child raced ahead).
        let res = cqe.result();
        assert!(
            res == 0 || res == -libc::EAGAIN,
            "unexpected FutexWaitV result: {} (ENOSYS={} EINVAL={}) — kernel may not support FUTEX_WAITV (Linux 6.7+ required)",
            res, -libc::ENOSYS, -libc::EINVAL,
        );

        let mut status: i32 = 0;
        unsafe {
            libc::waitpid(pid, &mut status, 0);
            libc::munmap(ptr as *mut libc::c_void, 4096);
            libc::close(fd);
        }
        drop(futexv);
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
