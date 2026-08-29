//! M2W: the master→worker wake channel — one non-blocking eventfd per worker.
//! The counter carries no information; a worker sees a SAL entry through the
//! mapping's Acquire size prefix either way, so a signal only ends a park.
//!
//! The reverse direction is [`super::w2m`], which parks on a futex in the ring
//! itself and needs no descriptor.

use gnitz_engine::foundation::posix_io;

/// Create a non-blocking, close-on-exec eventfd. Raw rather than an `OwnedFd`
/// because the master makes one per worker before the fork and each child then
/// closes every rank but its own.
pub(crate) fn eventfd_create() -> std::io::Result<i32> {
    let fd = unsafe { libc::eventfd(0, libc::EFD_NONBLOCK | libc::EFD_CLOEXEC) };
    if fd < 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(fd)
}

/// Signal an eventfd (increment its counter by 1). Nothing is returned because
/// nothing can fail: an `EFD_NONBLOCK` eventfd refuses a write only at
/// `u64::MAX - 1`, i.e. after 2^64 unconsumed wakes.
pub(crate) fn eventfd_signal(efd: i32) {
    let _ = posix_io::write_all_fd(efd, &1u64.to_ne_bytes());
}

/// How a park on the wake channel ended.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[must_use]
pub(crate) enum Wake {
    /// The master signalled; the counter is drained.
    Signalled,
    /// Nothing arrived before the timeout. The one outcome that leaves the
    /// master's liveness unknown — nothing signals a dead master's eventfd — so
    /// it is the only one worth spending a `getppid` on.
    Idle,
    /// The `poll` itself failed. The caller re-parks.
    Failed,
}

/// Park until the master signals, or `timeout_ms` elapses, draining the counter.
pub(crate) fn eventfd_wait(efd: i32, timeout_ms: i32) -> Wake {
    let mut pfd = libc::pollfd {
        fd: efd,
        events: libc::POLLIN,
        revents: 0,
    };
    let r = loop {
        let r = unsafe { libc::poll(&mut pfd, 1, timeout_ms) };
        if r < 0 && std::io::Error::last_os_error().raw_os_error() == Some(libc::EINTR) {
            continue;
        }
        break r;
    };
    match r {
        0 => Wake::Idle,
        r if r < 0 => Wake::Failed,
        _ => {
            let mut v: u64 = 0;
            unsafe {
                libc::read(efd, &mut v as *mut u64 as *mut libc::c_void, 8);
            }
            Wake::Signalled
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_eventfd_signal_wait() {
        let fd = eventfd_create().unwrap();
        eventfd_signal(fd);
        assert_eq!(eventfd_wait(fd, 1000), Wake::Signalled);
        unsafe {
            libc::close(fd);
        }
    }

    #[test]
    fn test_eventfd_wait_timeout() {
        let fd = eventfd_create().unwrap();
        assert_eq!(eventfd_wait(fd, 10), Wake::Idle);
        unsafe {
            libc::close(fd);
        }
    }
}
