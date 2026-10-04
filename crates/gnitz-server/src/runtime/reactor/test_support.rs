//! Helpers shared by the reactor's test suites; the `pub(crate)` ones also serve
//! suites elsewhere that drive a reactor over test rings.

use std::io::Read;
use std::os::fd::{AsFd, AsRawFd, OwnedFd};
use std::os::unix::net::UnixStream;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use super::*;
pub(super) use crate::runtime::test_support::within;
use crate::runtime::w2m::fixtures::test_rings;
use crate::runtime::w2m::W2mWriter;

/// A reactor with no W2M rings, for the tests that route no worker traffic.
pub(super) fn make_reactor() -> Reactor {
    make_reactor_with(Limits::TEST)
}

/// [`make_reactor`] built with `limits`.
pub(crate) fn make_reactor_with(limits: Limits) -> Reactor {
    Reactor::new(16, limits, test_rings(&[]).1).expect("reactor")
}

/// A test reactor reading the rings `w2m` covers.
pub(crate) fn make_reactor_over(w2m: W2mReceiver) -> Reactor {
    Reactor::new(16, Limits::TEST, w2m).expect("reactor")
}

/// The drivers every reactor suite runs on. An inherent impl here rather than in
/// `runloop`/`mod`, so the production files carry no test-only method.
impl Reactor {
    /// Tick until no task is left. Bounded as [`poll_until`] is: the tests that
    /// use this guard against lost wakes and lock deadlocks, which an unbounded
    /// loop would turn into a wedged test run rather than a failure.
    pub(super) fn block_until_idle(&self) {
        let idle = poll_until(self, || self.tasks.borrow().is_empty());
        assert!(idle, "reactor never reached idle: lost wake or deadlock");
    }

    /// One pass of the loop `block_on` runs; with `block`, it sleeps whenever
    /// nothing is runnable.
    pub(super) fn tick(&self, block: bool) {
        self.run_ready();
        self.submit_or_sleep(block);
    }
}

/// A reactor over `n` fresh W2M rings, with the writer of each.
pub(crate) fn reactor_with_rings(n: usize) -> (Reactor, Vec<W2mWriter>) {
    let (writers, receiver) = test_rings(&vec![64 * 1024; n]);
    (make_reactor_over(receiver), writers)
}

/// A fresh ring holding one frame whose error text is `pad` bytes, read back as a
/// slot.
pub(crate) fn ring_slot(pad: usize) -> (W2mReceiver, W2mSlot) {
    let (mut writers, receiver) = test_rings(&[256 * 1024]);
    let text = vec![0x42u8; pad];
    let msg = crate::runtime::wire::WireMsg { blob: &text, ..Default::default() };
    writers[0].send_msg(1, &msg);
    let slot = receiver.try_read_slot(0).expect("a frame");
    (receiver, slot)
}

/// A waker that records being woken, for a test that polls a future by hand.
#[derive(Default)]
pub(super) struct WakeFlag(AtomicBool);

impl std::task::Wake for WakeFlag {
    fn wake(self: Arc<Self>) {
        self.0.store(true, Ordering::Relaxed);
    }
}

impl WakeFlag {
    pub(super) fn new() -> (Arc<WakeFlag>, Waker) {
        let flag = Arc::new(WakeFlag::default());
        (Arc::clone(&flag), Waker::from(flag))
    }

    pub(super) fn woken(&self) -> bool {
        self.0.load(Ordering::Relaxed)
    }
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

/// A connection over one end of a fresh socketpair, nothing armed on it, and the
/// other end.
pub(crate) fn client_pair(r: &Reactor) -> (Rc<ClientConn>, UnixStream) {
    let (local, partner) = UnixStream::pair().expect("socketpair");
    (r.client_conn(OwnedFd::from(local)).expect("under the cap"), partner)
}

/// [`client_pair`] with a recv armed on the connection.
pub(super) fn registered(r: &Reactor) -> (Rc<ClientConn>, UnixStream) {
    let (conn, partner) = client_pair(r);
    r.register_conn(&conn, Box::new(Plain::new()));
    (conn, partner)
}

/// A reactor built with `limits`, and a [`client_pair`] on it. `sndbuf` shrinks
/// both socket buffers.
pub(crate) fn egress_pair(limits: Limits, sndbuf: Option<i32>) -> (Reactor, Rc<ClientConn>, UnixStream) {
    let r = make_reactor_with(limits);
    let (conn, partner) = client_pair(&r);
    if let Some(bytes) = sndbuf {
        for (fd, opt) in [(conn.fd(), libc::SO_SNDBUF), (partner.as_raw_fd(), libc::SO_RCVBUF)] {
            gnitz_foundation::posix_io::set_sockopt_int(fd, libc::SOL_SOCKET, opt, bytes).unwrap();
        }
    }
    (r, conn, partner)
}

/// Read `expect` bytes off `s` and close it, so the send under test never
/// stalls on a full socket buffer. Returns how many it saw before `expect` or EOF.
pub(crate) fn spawn_drain(s: UnixStream, expect: usize) -> std::thread::JoinHandle<u64> {
    std::thread::spawn(move || std::io::copy(&mut (&s).take(expect as u64), &mut std::io::sink()).expect("drain"))
}

/// One non-blocking read of up to `cap` bytes off `s`: empty at EOF, `None` when
/// nothing is readable yet.
pub(crate) fn read_nonblocking(s: &UnixStream, cap: usize) -> Option<Vec<u8>> {
    s.set_nonblocking(true).expect("nonblocking");
    let mut buf = vec![0u8; cap];
    match (&*s).read(&mut buf) {
        Ok(n) => {
            buf.truncate(n);
            Some(buf)
        }
        Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => None,
        Err(e) => panic!("read: {e}"),
    }
}

/// Build a length-prefixed wire frame: LE payload length + payload.
pub(crate) fn framed(payload: &[u8]) -> Vec<u8> {
    [&gnitz_wire::frame_len_prefix(payload.len())[..], payload].concat()
}

/// Drive the reactor in non-blocking ticks until `cond` holds after one, for at
/// most ten seconds; `false` if it never does.
pub(crate) fn poll_until(r: &Reactor, mut cond: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + std::time::Duration::from_secs(10);
    while Instant::now() < deadline {
        r.tick(false);
        if cond() {
            return true;
        }
    }
    false
}

/// `fd`, open for the rest of the process.
pub(super) fn leaked(fd: OwnedFd) -> BorrowedFd<'static> {
    let fd: &'static OwnedFd = Box::leak(Box::new(fd));
    fd.as_fd()
}
