//! Single-threaded io_uring reactor: the master process's one event loop, driven
//! by [`Reactor::block_on`] alone. A task's waker is its key (see `waker_wake`);
//! a one-CQE awaiter polls its op's slot in `ops`, next-of-many awaiters park in
//! a [`WakeQueue`](crate::runtime::sync::WakeQueue), an ACK round parks on its
//! lease's route until every worker of its set has answered, and every deadline
//! sits in one deadline map.
//!
//! This file holds the reactor's state, the CQE `user_data` encoding, one-shot
//! ops and `fsync`. The rest is split by concept, each owning its part of the
//! state: `runloop` (scheduling, the pass and its sleep on the W2M park, CQE
//! dispatch), `lease` (W2M ingest and the leases it routes to), `conn` (client
//! connections and their limits), `timer` (deadlines) and `uring` (the ring).
//!
//! The reactor reads the W2M rings (`w2m`, `park`) and shares the task
//! primitives of `sync`; it names nothing that schedules work on it (`sal`,
//! `master`, `executor`, `tls`).

use std::cell::{Cell, RefCell};
use std::future::Future;
use std::os::fd::{AsRawFd, BorrowedFd};
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};
use std::time::{Duration, Instant};

use io_uring::{opcode, types};
use slab::Slab;

use self::uring::IoUringRing;

use crate::runtime::w2m::W2mReceiver;

mod conn;
mod lease;
mod runloop;
#[cfg(test)]
pub(super) mod test_support;
mod timer;
mod uring;

pub(crate) use conn::{
    Accepted, Charge, ClientConn, FrameSink, Limits, PeerGone, Plain, RecvBuf, RecvEnd, RecvFilter, SendBody,
};
pub(crate) use lease::{AckLease, GroupTargets, ReadyAcked, ReadyLease, TrainFrame, TrainLease};
pub(crate) use uring::check_host;

// ---------------------------------------------------------------------------
// CQE user_data encoding (high 8 bits = kind, low 56 bits = id)
// ---------------------------------------------------------------------------

/// A one-shot op whose CQE lands in `ops`.
const KIND_OP: u64 = 1;
const KIND_PARK: u64 = 2;
const KIND_RECV: u64 = 3;
/// An `ASYNC_CANCEL`, whose CQE nothing awaits: the cancelled op's own says
/// how that op ended.
const KIND_CANCEL: u64 = 4;

const KIND_SHIFT: u64 = 56;
const ID_MASK: u64 = 0x00FF_FFFF_FFFF_FFFF;

#[inline]
const fn udata(kind: u64, id: u64) -> u64 {
    (kind << KIND_SHIFT) | (id & ID_MASK)
}

/// The low bits of a one-shot op's id, its slot in `ops`; the bits above count
/// the ops pushed, so no later op that reuses the slot has the same id, and a
/// cancel names its op alone.
const OP_SLOT_MASK: u64 = 0xFFFF_FFFF;

// ---------------------------------------------------------------------------
// Reactor
// ---------------------------------------------------------------------------

type Task = Pin<Box<dyn Future<Output = ()>>>;

/// The state behind every [`Reactor`] handle. Field order is drop order, and
/// its head is fixed; see `ring`.
pub(crate) struct ReactorShared {
    /// First, so it drops before the memory its SQEs point into.
    ring: RefCell<IoUringRing>,
    /// Live tasks; a task's waker key is its slot plus one. A slot holds `None`
    /// while its task is polled, and is freed for reuse when it completes.
    tasks: RefCell<Slab<Option<Task>>>,
    /// One-shot ops, each in a slot freed by the later of its CQE and its
    /// future's end (taking the result, or being dropped).
    ops: RefCell<Slab<PendingOp>>,
    /// One-shot ops pushed: the high bits of each op's id.
    ops_pushed: Cell<u64>,
    /// A `FUTEX_WAIT` SQE on the W2M park is outstanding. Cleared by its CQE.
    parked: Cell<bool>,
    timers: timer::Timers,
    routes: lease::Routes,
    conns: conn::Conns,
    w2m: W2mReceiver,
}

/// A submitted op: where its CQE `res` meets its awaiter.
struct PendingOp {
    state: OpState,
    /// What the SQE names, held until the CQE, and handed to the awaiter with
    /// the result.
    carry: Option<conn::Outbound>,
}

enum OpState {
    /// No CQE yet; the waker of the awaiter's last poll.
    Waiting(Option<Waker>),
    /// The CQE's `res`, not yet taken by the awaiter.
    Done(i32),
    /// The awaiter was dropped before the CQE, which frees the slot.
    Abandoned,
}

/// The awaiter of a one-shot op: yields its CQE `res` and the op's carry.
/// Dropped before the CQE, it leaves the slot, carry included, to the CQE.
struct NamedOp {
    reactor: Reactor,
    user_data: u64,
    /// The result was taken, and the slot freed.
    finished: bool,
}

impl NamedOp {
    /// The op's `user_data`, which [`Reactor::cancel_op`] takes.
    fn user_data(&self) -> u64 {
        self.user_data
    }

    fn slot(&self) -> usize {
        (self.user_data & OP_SLOT_MASK) as usize
    }

    /// `f` over the carry while the op is in flight. `f` runs under a shared
    /// borrow of `ops`, so it must not reach the reactor.
    fn with_carry<R>(&self, f: impl FnOnce(&conn::Outbound) -> R) -> R {
        debug_assert!(!self.finished, "the carry left with the result");
        let ops = self.reactor.ops.borrow();
        f(ops[self.slot()].carry.as_ref().expect("an op that carries"))
    }
}

impl Future for NamedOp {
    type Output = (i32, Option<conn::Outbound>);

    // Inlined into each caller: outlined, the result moves through extra
    // stack copies.
    #[inline(always)]
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        assert!(!self.finished, "an op polled after its result");
        let slot = self.slot();
        let mut ops = self.reactor.ops.borrow_mut();
        let entry = &mut ops[slot];
        let res = match &mut entry.state {
            OpState::Done(res) => *res,
            OpState::Waiting(waker) => {
                match waker {
                    Some(w) if w.will_wake(cx.waker()) => {}
                    _ => *waker = Some(cx.waker().clone()),
                }
                return Poll::Pending;
            }
            OpState::Abandoned => unreachable!("a live op's slot marked abandoned"),
        };
        let PendingOp { state, carry } = ops.remove(slot);
        drop(ops);
        drop(state);
        self.finished = true;
        Poll::Ready((res, carry))
    }
}

impl Drop for NamedOp {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        let slot = self.slot();
        let mut ops = self.reactor.ops.borrow_mut();
        let entry = &mut ops[slot];
        if let OpState::Done(_) = entry.state {
            drop(ops);
            self.reactor.free_op(slot);
        } else {
            entry.state = OpState::Abandoned;
        }
    }
}

impl ReactorShared {
    /// Free op `slot`, dropping its carry outside the `ops` borrow: the carry
    /// may hold the last handle on a connection.
    fn free_op(&self, slot: usize) {
        let op = self.ops.borrow_mut().remove(slot);
        drop(op);
    }
}

/// The awaiter of a one-shot op that carries nothing: yields its CQE `res`.
struct OpFuture(NamedOp);

impl Future for OpFuture {
    type Output = i32;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<i32> {
        Pin::new(&mut self.0).poll(cx).map(|(res, _)| res)
    }
}

/// A handle on the master's one event loop; clones share it.
#[derive(Clone)]
pub(crate) struct Reactor(Rc<ReactorShared>);

impl std::ops::Deref for Reactor {
    type Target = ReactorShared;
    fn deref(&self) -> &ReactorShared {
        &self.0
    }
}

impl Reactor {
    /// The reactor, and the lease every worker's ready ACK answers.
    pub(crate) fn new(limits: Limits, w2m: W2mReceiver) -> std::io::Result<(Self, ReadyLease)> {
        let ring = IoUringRing::new()?;
        // Before the shared state exists: a refused claim then drops none of it,
        // and so does not end the live reactor's claim.
        runloop::claim_thread();
        let reactor = Reactor(Rc::new(ReactorShared {
            ring: RefCell::new(ring),
            tasks: RefCell::new(Slab::new()),
            ops: RefCell::new(Slab::new()),
            ops_pushed: Cell::new(0),
            parked: Cell::new(false),
            timers: timer::Timers::new(),
            routes: lease::Routes::new(),
            conns: conn::Conns::new(limits),
            w2m,
        }));
        let ready = reactor.ready_lease();
        Ok((reactor, ready))
    }

    /// [`Self::push_named_op`] of an op that holds nothing.
    ///
    /// # Safety
    /// As [`IoUringRing::push`].
    unsafe fn push_op(&self, sqe: io_uring::squeue::Entry) -> OpFuture {
        // SAFETY: forwarded to the caller.
        OpFuture(unsafe { self.push_named_op(sqe, None) })
    }

    /// Queue `sqe` as a one-shot op holding `carry` until its CQE, and return
    /// its awaiter, which yields the CQE `res` with `carry` and names the op's
    /// `user_data` for [`Self::cancel_op`]. Not submitted. The op's slot is
    /// freed by the later of its one CQE and its awaiter's end, and then
    /// reused, so `sqe` must be an opcode that posts exactly one CQE.
    ///
    /// # Safety
    /// As [`IoUringRing::push`]; what `carry` holds counts as valid until the CQE.
    unsafe fn push_named_op(&self, sqe: io_uring::squeue::Entry, carry: Option<conn::Outbound>) -> NamedOp {
        let slot = self
            .ops
            .borrow_mut()
            .insert(PendingOp { state: OpState::Waiting(None), carry }) as u64;
        debug_assert!(slot <= OP_SLOT_MASK, "{slot} ops in flight");
        let pushed = self.ops_pushed.get() + 1;
        self.ops_pushed.set(pushed);
        let user_data = udata(KIND_OP, pushed << OP_SLOT_MASK.count_ones() | slot);
        // SAFETY: forwarded to the caller.
        unsafe { self.ring.borrow_mut().push(sqe, user_data) };
        NamedOp {
            reactor: self.clone(),
            user_data,
            finished: false,
        }
    }

    /// Cancel the op [`Self::push_named_op`] pushed as `user_data`, which its
    /// awaiter has not seen complete: the op's CQE comes early, `-ECANCELED`
    /// unless it was completing anyway. Submitted now, so the cancel reaches
    /// the kernel while the op's slot is still its own.
    fn cancel_op(&self, user_data: u64) {
        let sqe = opcode::AsyncCancel::new(user_data).build();
        let mut ring = self.ring.borrow_mut();
        // SAFETY: the SQE names no memory.
        unsafe { ring.push(sqe, udata(KIND_CANCEL, 0)) };
        ring.submit();
    }

    /// Queue an fdatasync of `fd` and submit it now, so it runs while the caller
    /// works on. The future yields the CQE `res`: 0, or a negative errno.
    pub(crate) fn fsync(&self, fd: BorrowedFd<'static>) -> impl Future<Output = i32> {
        let sqe = opcode::Fsync::new(types::Fd(fd.as_raw_fd()))
            .flags(types::FsyncFlags::DATASYNC)
            .build();
        // SAFETY: `fd` is open for the process's life; the SQE names no memory.
        let op = unsafe { self.push_op(sqe) };
        self.ring.borrow_mut().submit();
        op
    }
}

#[cfg(test)]
#[path = "tests/reactor.rs"]
mod tests;
