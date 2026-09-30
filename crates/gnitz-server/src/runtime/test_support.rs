//! Helpers shared by `runtime`'s test suites, across `orchestration`,
//! `protocol` and `suites`.
//!
//! Here rather than in the engine's testkit because nothing below this crate
//! uses them: a ring is a `runtime` shape, and a helper that crosses no crate
//! boundary should not sit on one's published surface.

impl crate::runtime::wire::WireMsg<'_> {
    /// Encode into a fresh `Vec` sized by `WireMsg::size`.
    pub(crate) fn encode_to_vec(&self) -> Vec<u8> {
        let mut buf = vec![0u8; self.size()];
        self.encode(&mut buf);
        buf
    }
}

/// Run `body` in a child process that shares this process's fd table, and return
/// its pid. The child exits 0, or 1 if `body` panics.
///
/// # Safety
/// `body` must not allocate, and must open or close no fd.
pub(crate) unsafe fn fork_child(body: impl FnOnce()) -> libc::pid_t {
    // SAFETY: a null stack runs the child on a copy-on-write copy of this one.
    let pid = unsafe { libc::syscall(libc::SYS_clone, libc::CLONE_FILES | libc::SIGCHLD, 0, 0, 0, 0) };
    assert!(pid >= 0, "clone failed: {}", std::io::Error::last_os_error());
    if pid == 0 {
        let ok = std::panic::catch_unwind(std::panic::AssertUnwindSafe(body)).is_ok();
        unsafe { libc::_exit(if ok { 0 } else { 1 }) };
    }
    pid as libc::pid_t
}

/// Reap `pid` and require a clean exit: a panic in a [`fork_child`] body is seen
/// only here.
pub(crate) unsafe fn assert_child_exited_ok(pid: libc::pid_t) {
    let mut status = 0i32;
    gnitz_foundation::posix_io::retry_eintr(|| libc::waitpid(pid, &mut status, 0))
        .unwrap_or_else(|e| panic!("waitpid failed on child {pid}: {e}"));
    assert!(
        libc::WIFEXITED(status) && libc::WEXITSTATUS(status) == 0,
        "child {pid} did not exit cleanly (status {status:#x})"
    );
}

/// Run `f` on its own thread and fail if it has not returned within `limit`, so
/// a tick that sleeps when it must not fails the test instead of wedging the
/// run. A timed-out thread is left blocked; the harness exits past it.
pub(crate) fn within(limit: std::time::Duration, f: impl FnOnce() + Send + 'static) {
    let (tx, rx) = std::sync::mpsc::channel();
    let handle = std::thread::spawn(move || {
        f();
        let _ = tx.send(());
    });
    match rx.recv_timeout(limit) {
        Ok(()) => handle.join().expect("test thread panicked"),
        // The sender dropped without sending: `f` panicked.
        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
            if let Err(panic) = handle.join() {
                std::panic::resume_unwind(panic);
            }
        }
        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => panic!("did not finish within {limit:?}"),
    }
}

/// Poll a future exactly once with a noop waker; `None` if it is still pending.
/// For the tests that assert what a *single* poll does and then discard the
/// future — one that is re-polled needs its own pinned handle instead.
pub(crate) fn try_poll_once<T>(fut: impl std::future::Future<Output = T>) -> Option<T> {
    use std::task::{Context, Poll, Waker};
    let mut cx = Context::from_waker(Waker::noop());
    let mut fut = std::pin::pin!(fut);
    match fut.as_mut().poll(&mut cx) {
        Poll::Ready(r) => Some(r),
        Poll::Pending => None,
    }
}

/// A `memfd` mapped `MAP_SHARED` for IPC-shaped tests — a real fd, so a writer
/// can `fdatasync` it; unmapped and closed on drop. A child that `_exit`s never
/// runs drops, so only the parent does.
pub(crate) struct SharedRegion {
    fd: i32,
    ptr: *mut u8,
    size: usize,
}

impl SharedRegion {
    pub(crate) fn new(size: usize) -> Self {
        let fd = unsafe { libc::memfd_create(c"shared_region".as_ptr(), libc::MFD_CLOEXEC) };
        assert!(fd >= 0, "memfd_create failed: {}", std::io::Error::last_os_error());
        let ptr = gnitz_foundation::posix_io::map_file_reserved(fd, size).expect("SharedRegion mmap failed");
        SharedRegion { fd, ptr, size }
    }

    pub(crate) fn ptr(&self) -> *mut u8 {
        self.ptr
    }

    pub(crate) fn fd(&self) -> i32 {
        self.fd
    }

    pub(crate) fn size(&self) -> usize {
        self.size
    }

    /// The region's base pointer, mapped for the rest of the process. For the
    /// fixtures that hand a raw pointer to something outliving no scope in
    /// particular — a `W2mReceiver` or a `SalWriter`, neither of which owns what
    /// it points at — so there is no unmap for a stale pointer to outlive.
    pub(crate) fn leak(self) -> *mut u8 {
        let ptr = self.ptr;
        std::mem::forget(self);
        ptr
    }
}

impl Drop for SharedRegion {
    fn drop(&mut self) {
        unsafe {
            libc::munmap(self.ptr as *mut libc::c_void, self.size);
            libc::close(self.fd);
        }
    }
}
