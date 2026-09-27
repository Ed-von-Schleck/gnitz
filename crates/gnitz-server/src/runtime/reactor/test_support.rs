//! Helpers shared by the reactor's test suites; the `pub(crate)` ones also serve
//! suites elsewhere that drive a reactor over test rings.

use std::os::fd::{AsRawFd, OwnedFd};

pub(super) use super::runloop::make_waker;
use super::runloop::RUN_QUEUE;
use super::*;
pub(super) use crate::runtime::test_support::{make_reactor, make_reactor_over, make_reactor_with, within};
use crate::runtime::w2m::W2mWriter;

/// The drivers every reactor suite runs on. An inherent impl here rather than in
/// `runloop`/`mod`, so the production files carry no test-only method.
impl Reactor {
    /// Bounded: the tests that use this are the ones guarding against lost
    /// wakes and lock deadlocks, and an unbounded loop would turn each of those
    /// regressions into a wedged test run rather than a failure. Ticks
    /// non-blocking, so a task waiting only on another task still progresses.
    pub(super) fn block_until_idle(&self) {
        const MAX_TICKS: usize = 10_000;
        for _ in 0..MAX_TICKS {
            if self.inner.tasks.borrow().is_empty() {
                return;
            }
            self.tick(false);
        }
        panic!("reactor: {MAX_TICKS} ticks without reaching idle — lost wake or deadlock");
    }

    /// Route every frame the W2M rings hold, as a tick's drain does — for a
    /// fixture that publishes frames and never ticks.
    pub(crate) fn route_w2m_for_test(&self) {
        self.drain_all_w2m();
    }

    /// Worker `w`'s W2M release cursor.
    pub(crate) fn release_cursor_for_test(&self, w: usize) -> u64 {
        self.inner.w2m.release_cursor(w)
    }
}

/// A reactor over `n` fresh W2M rings, with the writer of each. The rings
/// are leaked: a `W2mSlot` a failing assert leaves routed writes through its
/// ring on drop, so never unmapping makes teardown order irrelevant.
pub(crate) fn reactor_with_rings(n: usize) -> (Reactor, Vec<W2mWriter>) {
    let ptrs: Vec<*mut u8> = (0..n)
        .map(|_| unsafe { crate::runtime::w2m::fixtures::test_ring(64 * 1024) }.leak())
        .collect();
    let writers = ptrs.iter().map(|&p| W2mWriter::new(p)).collect();
    let r = make_reactor_over(W2mReceiver::new(ptrs));
    (r, writers)
}

/// Task `key` is in this thread's run queue.
pub(super) fn is_queued(key: usize) -> bool {
    RUN_QUEUE.with(|q| q.borrow().is_queued(key))
}

/// How many tasks this thread's run queue holds.
pub(super) fn run_queue_len() -> usize {
    RUN_QUEUE.with(|q| q.borrow().len())
}

/// An op installed with no SQE behind it, so its CQE is whatever a test feeds
/// through [`cqe`]: its id and its awaiter.
pub(super) fn bare_op(r: &Reactor, carry: Option<conn::Outbound>) -> (u64, oneshot::Receiver<OpResult>) {
    let (u, rx) = r.install_op(carry);
    (udata_id(u), rx)
}

/// Drive `dispatch_cqe` with a synthetic completion tagged `kind`/`id`,
/// carrying `rc` — the ring completions a test cannot make the kernel produce.
pub(super) fn cqe(r: &Reactor, kind: u64, id: u64, rc: i32) {
    r.dispatch_cqe(udata(kind, id), rc, 0);
}

/// Future that returns Pending exactly once, then Ready.
pub(super) struct YieldOnce {
    yielded: bool,
}

impl YieldOnce {
    pub(super) fn new() -> Self {
        YieldOnce { yielded: false }
    }
}

impl Future for YieldOnce {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.yielded {
            Poll::Ready(())
        } else {
            self.yielded = true;
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }
}

/// A reactor built with `limits` and a socketpair `(sender, receiver)`, nothing
/// registered. `sndbuf` shrinks both socket buffers.
pub(super) fn egress_pair(limits: Limits, sndbuf: Option<i32>) -> (Rc<Reactor>, OwnedFd, OwnedFd) {
    let (sender, receiver) = std::os::unix::net::UnixStream::pair().expect("socketpair");
    let (sender, receiver) = (OwnedFd::from(sender), OwnedFd::from(receiver));
    if let Some(bytes) = sndbuf {
        for (fd, opt) in [(&sender, libc::SO_SNDBUF), (&receiver, libc::SO_RCVBUF)] {
            gnitz_foundation::posix_io::set_sockopt_int(fd.as_raw_fd(), libc::SOL_SOCKET, opt, bytes);
        }
    }
    (Rc::new(make_reactor_with(limits)), sender, receiver)
}

/// Read `expect` bytes off `fd` and close it, so the send under test never
/// stalls on a full socket buffer. Returns what it actually saw.
pub(super) fn spawn_drain(fd: OwnedFd, expect: usize) -> std::thread::JoinHandle<usize> {
    std::thread::spawn(move || {
        let mut seen = 0usize;
        let mut scratch = vec![0u8; 64 * 1024];
        while seen < expect {
            // EINTR is not EOF: breaking on it would close `fd` early and fail
            // the sender under test with EPIPE.
            let n = gnitz_foundation::posix_io::retry_eintr(|| unsafe {
                libc::read(fd.as_raw_fd(), scratch.as_mut_ptr() as *mut libc::c_void, scratch.len()) as libc::c_int
            });
            match n {
                Ok(n) if n > 0 => seen += n as usize,
                _ => break,
            }
        }
        drop(fd);
        seen
    })
}

/// Build a length-prefixed wire frame: LE payload length + payload.
pub(super) fn framed(payload: &[u8]) -> Vec<u8> {
    let mut v = Vec::with_capacity(gnitz_wire::FRAME_LEN_PREFIX_BYTES + payload.len());
    v.extend_from_slice(&gnitz_wire::frame_len_prefix(payload.len()));
    v.extend_from_slice(payload);
    v
}

/// Drive the reactor up to `max` non-blocking ticks, returning `true` as
/// soon as `cond` holds after a tick (and `false` if it never does).
pub(super) fn poll_until(r: &Reactor, max: usize, mut cond: impl FnMut() -> bool) -> bool {
    (0..max).any(|_| {
        r.tick(false);
        cond()
    })
}

/// A listening socket no peer connects to, so an accept submitted on it stays
/// pending until the reactor drops.
pub(super) fn fake_listener() -> OwnedFd {
    OwnedFd::from(std::net::TcpListener::bind("127.0.0.1:0").expect("bind"))
}
