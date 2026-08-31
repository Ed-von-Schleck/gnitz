//! Helpers shared by `runtime`'s test suites, across `orchestration`,
//! `protocol` and `suites`.
//!
//! Here rather than in the engine's testkit because nothing below this crate
//! uses them: a ring is a `runtime` shape, and a helper that crosses no crate
//! boundary should not sit on one's published surface.

use crate::runtime::sal::{EpochGate, SalLog, SalMessage, SalStep};

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

/// The group published at `base`, walked at the log's current epoch.
pub(crate) fn group_at(log: SalLog, base: u64) -> SalMessage {
    match log.read_at(base, EpochGate::Walk(log.walk_epoch())) {
        SalStep::Group(msg, _) => msg,
        _ => panic!("a group is published at offset {base}"),
    }
}

/// Anonymous `MAP_SHARED` region for IPC-shaped tests; unmapped on drop.
/// `MAP_SHARED` so a `fork()`ed child sees the same pages (a child that
/// `_exit`s never runs drops, so only the parent unmaps). Pages are
/// kernel-zeroed and lazily populated, and `MAP_NORESERVE` keeps even a
/// ring-sized region off `Committed_AS`.
pub(crate) struct SharedRegion {
    ptr: *mut u8,
    size: usize,
}

impl SharedRegion {
    pub(crate) fn new(size: usize) -> Self {
        let ptr = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                size,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_ANONYMOUS | libc::MAP_SHARED | libc::MAP_NORESERVE,
                -1,
                0,
            )
        };
        assert_ne!(ptr, libc::MAP_FAILED, "SharedRegion mmap failed");
        SharedRegion {
            ptr: ptr as *mut u8,
            size,
        }
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
