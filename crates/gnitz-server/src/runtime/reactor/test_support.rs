//! Helpers shared by the reactor's test suites; the `pub(crate)` ones also serve
//! suites elsewhere that drive a reactor over test rings.

use std::io::Read;
use std::os::fd::{AsFd, AsRawFd, OwnedFd};
use std::os::unix::net::UnixStream;

use super::*;
pub(super) use crate::runtime::test_support::{try_poll_once, within};
use crate::runtime::w2m::fixtures::test_rings;
use crate::runtime::w2m::W2mWriter;

/// A reactor with no W2M rings, for the tests that route no worker traffic.
pub(super) fn make_reactor() -> Reactor {
    make_reactor_with(Limits::TEST)
}

/// [`make_reactor`] built with `limits`.
pub(super) fn make_reactor_with(limits: Limits) -> Reactor {
    Reactor::new(16, limits, test_rings(&[]).1).expect("reactor")
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
    (Reactor::new(16, Limits::TEST, receiver).expect("reactor"), writers)
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

/// Future that returns Pending exactly once, waking itself, then Ready.
pub(super) fn yield_once() -> impl Future<Output = ()> {
    let mut yielded = false;
    std::future::poll_fn(move |cx| {
        if std::mem::replace(&mut yielded, true) {
            return Poll::Ready(());
        }
        cx.waker().wake_by_ref();
        Poll::Pending
    })
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

/// [`registered`], past its handshake: admitting frames up to the payload ceiling.
pub(super) fn admitted(r: &Reactor) -> (Rc<ClientConn>, UnixStream) {
    let (conn, partner) = registered(r);
    conn.admit_full_frames();
    (conn, partner)
}

/// A reactor, and a [`client_pair`] on it.
pub(crate) fn egress_pair() -> (Reactor, Rc<ClientConn>, UnixStream) {
    let r = make_reactor();
    let (conn, partner) = client_pair(&r);
    (r, conn, partner)
}

/// Read `expect` bytes off `s` and close it, so the send under test never
/// stalls on a full socket buffer. Returns how many it saw before `expect` or EOF.
pub(crate) fn spawn_drain(s: UnixStream, expect: usize) -> std::thread::JoinHandle<u64> {
    std::thread::spawn(move || std::io::copy(&mut (&s).take(expect as u64), &mut std::io::sink()).expect("drain"))
}

/// One non-blocking read of up to `cap` bytes off `s`: empty at EOF, `None` when
/// nothing is readable yet. `s` stays blocking for its other readers.
pub(crate) fn read_nonblocking(s: &UnixStream, cap: usize) -> Option<Vec<u8>> {
    let mut buf = vec![0u8; cap];
    // SAFETY: `buf` is `cap` writable bytes.
    let n = unsafe { libc::recv(s.as_raw_fd(), buf.as_mut_ptr().cast(), cap, libc::MSG_DONTWAIT) };
    if n < 0 {
        let e = std::io::Error::last_os_error();
        assert_eq!(e.kind(), std::io::ErrorKind::WouldBlock, "recv: {e}");
        return None;
    }
    buf.truncate(n as usize);
    Some(buf)
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
