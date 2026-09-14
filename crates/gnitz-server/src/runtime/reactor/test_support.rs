//! Helpers shared by the reactor's test suites.
//!
//! Private to `reactor` and visible to every module under it, so a suite
//! attached to `conn`, `io` or `futures` reaches the same fixtures without any
//! of them becoming crate API.

use std::os::fd::{AsRawFd, OwnedFd};

pub(super) use super::runloop::make_waker;
use super::*;
pub(super) use crate::runtime::test_support::make_reactor;
use crate::runtime::w2m::W2mWriter;

/// The drivers every reactor suite runs on. An inherent impl here rather than in
/// `runloop`/`mod`, so the production files carry no test-only method.
impl Reactor {
    /// Drive `fut` to completion. Single-threaded, blocking. Spawns the
    /// future as a task internally and returns its output via a shared cell.
    pub(super) fn block_on<F, T>(&self, fut: F) -> T
    where
        F: Future<Output = T> + 'static,
        T: 'static,
    {
        let out: Rc<RefCell<Option<T>>> = Rc::new(RefCell::new(None));
        let out_capture = Rc::clone(&out);
        let root_key = self.spawn(async move {
            let v = fut.await;
            *out_capture.borrow_mut() = Some(v);
        });

        // Drive the reactor until the root task completes. The `tasks`
        // map removes the entry on completion, so `contains_key(root_key)`
        // returning false is the termination signal.
        loop {
            self.tick(true);
            if !self.inner.tasks.borrow().contains_key(&root_key) {
                break;
            }
        }

        // SAFETY: spawn ran the future to completion, so Some.
        let v = out
            .borrow_mut()
            .take()
            .expect("block_on root task did not produce output");
        v
    }

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
}

/// A reactor over `n` fresh W2M rings, with the writer of each. The rings
/// are leaked: a `W2mSlot` a failing assert leaves routed writes through its
/// ring on drop, so never unmapping makes teardown order irrelevant.
pub(super) fn reactor_with_rings(n: usize) -> (Reactor, Vec<W2mWriter>) {
    let ptrs: Vec<*mut u8> = (0..n)
        .map(|_| unsafe { crate::runtime::w2m::fixtures::test_ring(64 * 1024) }.leak())
        .collect();
    let writers = ptrs.iter().map(|&p| W2mWriter::new(p)).collect();
    let r = Reactor::new(16, Limits::TEST, Rc::new(W2mReceiver::new(ptrs))).expect("reactor");
    (r, writers)
}

/// Run `f` on its own thread and fail if it has not returned within `limit`, so
/// a tick that sleeps when it must not fails the test instead of wedging the
/// run. A timed-out thread is left blocked; the harness exits past it.
pub(super) fn within(limit: std::time::Duration, f: impl FnOnce() + Send + 'static) {
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

/// Drive `dispatch_cqe` with a synthetic completion tagged `kind`/`id`,
/// carrying `rc` — the ring completions a test cannot make the kernel produce.
pub(super) fn cqe(r: &Reactor, kind: u64, id: u64, rc: i32) {
    r.dispatch_cqe(super::uring::Cqe {
        user_data: udata(kind, id),
        res: rc,
        flags: 0,
    });
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
            unsafe {
                libc::setsockopt(
                    fd.as_raw_fd(),
                    libc::SOL_SOCKET,
                    opt,
                    &bytes as *const _ as *const libc::c_void,
                    std::mem::size_of::<i32>() as u32,
                );
            }
        }
    }
    let r = Rc::new(Reactor::new(16, limits, Rc::new(W2mReceiver::new(vec![]))).expect("reactor"));
    (r, sender, receiver)
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
    v.extend_from_slice(&(payload.len() as u32).to_le_bytes());
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

/// A real, owned fd to stand in for a listener. These tests only prep accept
/// SQEs (never submitting them), so nothing but the caller touches the fd — it
/// must merely be a number no parallel test owns.
pub(super) fn fake_listener() -> i32 {
    let fd = unsafe { libc::socket(libc::AF_UNIX, libc::SOCK_STREAM, 0) };
    assert!(fd >= 0, "socket");
    fd
}

/// Build a W2M ring carrying `n` scan frames, all tagged with
/// `internal_req_id` but with distinct wire request_ids (100, 101, …) so the
/// caller can verify arrival order. The returned `W2mReceiver` owns the
/// `InFlightState` the slots reference; caller reads frames via
/// `receiver.try_read_slot(0)`; the returned `SharedRegion` unmaps the ring
/// on drop, after both receiver and all slots drop.
pub(super) unsafe fn make_scan_ring(
    internal_req_id: u32,
    n: usize,
) -> (
    crate::runtime::w2m::W2mReceiver,
    crate::runtime::test_support::SharedRegion,
) {
    use crate::runtime::wire as ipc;

    let region = crate::runtime::w2m::fixtures::test_ring(64 * 1024);
    let ptr = region.ptr();

    let writer = W2mWriter::new(ptr);
    let receiver = W2mReceiver::new(vec![ptr]);
    for i in 0..n {
        let wire_req = 100u64 + i as u64;
        let msg = ipc::WireMsg {
            request_id: wire_req,
            ..Default::default()
        };
        writer.send_msg(internal_req_id as u64, &msg);
    }
    (receiver, region)
}
