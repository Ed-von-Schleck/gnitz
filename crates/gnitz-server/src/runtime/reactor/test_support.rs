//! Helpers shared by the reactor's test suites; the `pub(crate)` ones also serve
//! `runtime`'s other suites that drive a reactor or a connection.

use std::os::fd::{AsFd, OwnedFd};
use std::os::unix::net::UnixStream;

use super::conn::Accepted;
use super::op::Op;
use super::*;
use crate::runtime::park::WorkerSet;
pub(super) use crate::runtime::sync::{Either, oneshot, select2};
pub(super) use crate::runtime::test_support::{read_nonblocking, spawn_drain, try_poll_once, within, yield_once};
use crate::runtime::w2m::W2mWriter;
pub(super) use crate::runtime::w2m::fixtures::ring_slot;
use crate::runtime::w2m::fixtures::test_rings;

impl Limits {
    /// Every send carries `client_send_timeout`, so it is generous here: a drain
    /// thread descheduled on a loaded box must not evict the sender of a socket
    /// test. A test that waits a deadline out sets its own.
    pub(super) const TEST: Limits = Limits {
        inbound_cap: usize::MAX,
        conn_inbound_share: usize::MAX,
        client_send_timeout: Duration::from_secs(30),
        max_conns: usize::MAX,
        write_span_cap: usize::MAX,
    };
}

/// A reactor over one W2M ring, as production runs over at least one worker's.
pub(crate) fn make_reactor() -> Reactor {
    make_reactor_with(Limits::TEST)
}

/// [`make_reactor`] built with `limits`.
pub(super) fn make_reactor_with(limits: Limits) -> Reactor {
    Reactor::new(limits, test_rings(1, 64 * 1024).1).expect("reactor")
}

/// The drivers every reactor suite runs on. An inherent impl here rather than in
/// `runloop`, so production code carries no test-only method.
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
    let (writers, receiver) = test_rings(n, 64 * 1024);
    let r = Reactor::new(Limits::TEST, receiver).expect("reactor");
    if n == 0 {
        // A drain raises the level only when a worker moves its position, which
        // over no worker never happens: there every position is applied.
        r.applied.raise(u64::MAX);
    }
    (r, writers)
}

/// A train lease of one read of `kind`, written to every one of `n` fresh
/// rings, with each ring's writer.
pub(crate) fn train_lease(n: usize, kind: &'static str) -> (Reactor, TrainLease, Vec<W2mWriter>) {
    let (reactor, writers) = reactor_with_rings(n);
    let mut lease = reactor.lease_train();
    lease.add(WorkerSet::ALL, kind);
    (reactor, lease, writers)
}

/// The next read's train of `lease`, every read before it having ended.
pub(crate) fn train_of(lease: &mut TrainLease) -> Train<'_> {
    try_poll_once(lease.next_train()).expect("every earlier read has ended")
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

impl Peer {
    /// A plain connection over one end of an [`accepted_pair`], its recv armed,
    /// and the other end.
    pub(crate) fn over_socketpair(r: &Reactor) -> (Peer, UnixStream) {
        let (accepted, partner) = accepted_pair(r);
        (r.connect(accepted, None), partner)
    }
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
