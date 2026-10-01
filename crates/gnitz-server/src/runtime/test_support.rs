//! Helpers shared by `runtime`'s tests, across `orchestration`, `protocol` and
//! `reactor`.
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

/// Run `f` on its own thread and fail if it has not returned within 30s, so a
/// tick that sleeps when it must not fails the test instead of wedging the run.
/// A timed-out thread is left blocked; the harness exits past it.
pub(crate) fn within(f: impl FnOnce() + Send + 'static) {
    let limit = std::time::Duration::from_secs(30);
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

/// The worker's sorted-span producer over `stride`-byte `keys`, sorted in RAM
/// — the budget is never reached, so the spill dir is never touched.
pub(crate) fn key_producer(stride: usize, keys: &[impl AsRef<[u8]>]) -> gnitz_zset::repr::KeyProducer {
    let mut sort = gnitz_zset::repr::SpillSort::new("", stride, usize::MAX);
    for k in keys {
        sort.push(k.as_ref()).unwrap();
    }
    sort.finish().unwrap()
}
