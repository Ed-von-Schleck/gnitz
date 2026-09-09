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
        self.encode(&mut buf, 0);
        buf
    }
}

/// Reap `pid` and require a clean exit. A forked child that panics unwinds into
/// a copy of the test harness whose main thread no longer exists, so without
/// this the parent's own assertions are the only thing standing between a
/// broken child and a green test.
pub(crate) unsafe fn assert_child_exited_ok(pid: libc::pid_t) {
    let mut status = 0i32;
    gnitz_foundation::posix_io::retry_eintr(|| libc::waitpid(pid, &mut status, 0))
        .unwrap_or_else(|e| panic!("waitpid failed on child {pid}: {e}"));
    assert!(
        libc::WIFEXITED(status) && libc::WEXITSTATUS(status) == 0,
        "child {pid} did not exit cleanly (status {status:#x})"
    );
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

/// Anonymous `MAP_SHARED` region for IPC-shaped tests; unmapped on drop. A
/// child that `_exit`s never runs drops, so only the parent unmaps.
pub(crate) struct SharedRegion {
    ptr: *mut u8,
    size: usize,
}

impl SharedRegion {
    pub(crate) fn new(size: usize) -> Self {
        let ptr = gnitz_foundation::posix_io::map_anon_shared(size).expect("SharedRegion mmap failed");
        SharedRegion { ptr, size }
    }

    pub(crate) fn ptr(&self) -> *mut u8 {
        self.ptr
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
        }
    }
}
