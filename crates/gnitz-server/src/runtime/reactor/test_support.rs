//! Helpers shared by the reactor's test suites; the `pub(crate)` ones also serve
//! `runtime`'s other suites that drive a reactor or a connection.

use std::os::fd::{AsFd, OwnedFd};
use std::os::unix::net::UnixStream;

use super::*;
use crate::runtime::park::WorkerSet;
pub(super) use crate::runtime::sync::{oneshot, select2, Either};
pub(super) use crate::runtime::test_support::{read_nonblocking, spawn_drain, try_poll_once, within, yield_once};
pub(super) use crate::runtime::w2m::fixtures::ring_slot;
use crate::runtime::w2m::fixtures::test_rings;
use crate::runtime::w2m::W2mWriter;

impl Limits {
    /// Every send carries `client_send_timeout`, so it is generous here: a drain
    /// thread descheduled on a loaded box must not evict the sender of a socket
    /// test. A test that waits a deadline out sets its own.
    pub(super) const TEST: Limits = Limits {
        inbound_cap: usize::MAX,
        conn_inbound_share: usize::MAX,
        client_send_timeout: Duration::from_secs(30),
        accept_rearm_backoff: Duration::from_millis(2),
        max_conns: usize::MAX,
    };
}

/// A reactor over one W2M ring, as production runs over at least one worker's.
pub(crate) fn make_reactor() -> Reactor {
    make_reactor_with(Limits::TEST)
}

/// [`make_reactor`] built with `limits`.
pub(super) fn make_reactor_with(limits: Limits) -> Reactor {
    Reactor::new(limits, test_rings(1, 64 * 1024).1).expect("reactor").0
}

/// The drivers every reactor suite runs on. An inherent impl here rather than in
/// `runloop`/`mod`, so the production files carry no test-only method.
impl Reactor {
    /// Tick until no task is left. Bounded as [`poll_until`] is: the tests that
    /// use this guard against lost wakes and lock deadlocks, which an unbounded
    /// loop would turn into a wedged test run rather than a failure.
    pub(crate) fn block_until_idle(&self) {
        let idle = poll_until(self, || self.tasks.borrow().is_empty());
        assert!(idle, "reactor never reached idle: lost wake or deadlock");
    }

    /// One pass of the loop `block_on` runs, never sleeping.
    pub(crate) fn tick(&self) {
        self.run_ready();
        self.submit_or_sleep(false);
    }
}

/// A reactor over `n` fresh W2M rings, with the writer of each.
pub(crate) fn reactor_with_rings(n: usize) -> (Reactor, Vec<W2mWriter>) {
    let (reactor, _ready, writers) = booting_reactor(n);
    (reactor, writers)
}

/// A train lease of `kind` over every one of `n` fresh rings, with each ring's writer.
pub(crate) fn train_lease(n: usize, kind: &'static str) -> (Reactor, TrainLease, Vec<W2mWriter>) {
    let (reactor, writers) = reactor_with_rings(n);
    let lease = reactor.lease_train(WorkerSet::ALL, kind);
    (reactor, lease, writers)
}

/// [`reactor_with_rings`], with the future that resolves on every worker's ready ACK.
pub(super) fn booting_reactor(n: usize) -> (Reactor, impl Future<Output = ReadyAcked>, Vec<W2mWriter>) {
    let (writers, receiver) = test_rings(n, 64 * 1024);
    let (reactor, ready) = Reactor::new(Limits::TEST, receiver).expect("reactor");
    (reactor, ready, writers)
}

/// A one-shot op the kernel completes at its submit.
pub(super) fn nop(r: &Reactor) -> Op<()> {
    // SAFETY: the SQE names no memory.
    unsafe { r.push_op(opcode::Nop::new().build(), ()) }
}

/// One end of a fresh socketpair admitted under the connection cap, and the
/// other end.
pub(crate) fn accepted_pair(r: &Reactor) -> (Accepted, UnixStream) {
    let (local, partner) = UnixStream::pair().expect("socketpair");
    (r.admit(OwnedFd::from(local)).expect("under the cap"), partner)
}

/// A connection over an [`accepted_pair`], its recv armed and no egress owner,
/// and the other end.
pub(super) fn registered(r: &Reactor) -> (Rc<ClientConn>, UnixStream) {
    let (accepted, partner) = accepted_pair(r);
    (r.connect(accepted, None, Plain::new()), partner)
}

/// [`registered`], past its handshake: admitting frames up to the payload ceiling.
pub(super) fn admitted(r: &Reactor) -> (Rc<ClientConn>, UnixStream) {
    let (conn, partner) = registered(r);
    conn.admit_full_frames();
    (conn, partner)
}

/// A frame sink with a `cap`-byte budget of its own, past the handshake when
/// `admitted`.
pub(crate) fn frame_sink(cap: usize, admitted: bool) -> FrameSink {
    let mut q = FrameSink::new(conn::Budget::new(cap));
    if admitted {
        q.admit_full_frames();
    }
    q
}

/// Drive the reactor in non-blocking ticks until `cond` holds after one, for at
/// most ten seconds; `false` if it never does.
pub(crate) fn poll_until(r: &Reactor, mut cond: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + std::time::Duration::from_secs(10);
    while Instant::now() < deadline {
        r.tick();
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
