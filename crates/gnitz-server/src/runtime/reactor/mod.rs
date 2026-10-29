//! Single-threaded io_uring reactor: the master process's one event loop, driven
//! by [`Reactor::block_on`] alone. A task's waker is its key (see `waker_wake`);
//! one-CQE awaiters hold a [`oneshot`], next-of-many awaiters park in a
//! [`WakeQueue`](crate::runtime::sync::WakeQueue), an ACK round parks on its
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

use crate::runtime::sync::oneshot;
use crate::runtime::w2m::W2mReceiver;

mod conn;
mod lease;
mod runloop;
#[cfg(test)]
pub(super) mod test_support;
mod timer;
mod uring;

pub(crate) use conn::{
    Accepted, ClientConn, Limits, PeerGone, Plain, RecvBuf, RecvEnd, RecvFilter, RecvQueue, SendBody,
};
pub(crate) use lease::{AckLease, TrainFrame, TrainLease};
pub(crate) use uring::check_host;

// ---------------------------------------------------------------------------
// CQE user_data encoding (high 8 bits = kind, low 56 bits = id)
// ---------------------------------------------------------------------------

/// A one-shot op whose CQE lands in `ops`.
const KIND_OP: u64 = 1;
const KIND_PARK: u64 = 2;
const KIND_RECV: u64 = 3;

const KIND_SHIFT: u64 = 56;
const ID_MASK: u64 = 0x00FF_FFFF_FFFF_FFFF;

#[inline]
const fn udata(kind: u64, id: u64) -> u64 {
    (kind << KIND_SHIFT) | (id & ID_MASK)
}

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
    /// In-flight one-shot ops; an op's id is its slot, freed at its CQE.
    ops: RefCell<Slab<PendingOp>>,
    /// A `FUTEX_WAIT` SQE on the W2M park is outstanding. Cleared by its CQE.
    parked: Cell<bool>,
    timers: timer::Timers,
    routes: lease::Routes,
    conns: conn::Conns,
    w2m: W2mReceiver,
}

/// A submitted op awaiting its CQE `res`.
struct PendingOp {
    done: oneshot::Sender<i32>,
    /// What the SQE names, held until the CQE.
    _carry: Option<Rc<conn::Outbound>>,
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
    pub(crate) fn new(limits: Limits, w2m: W2mReceiver) -> std::io::Result<Self> {
        let ring = IoUringRing::new()?;
        // Before the shared state exists: a refused claim then drops none of it,
        // and so does not end the live reactor's claim.
        runloop::claim_thread();
        Ok(Reactor(Rc::new(ReactorShared {
            ring: RefCell::new(ring),
            tasks: RefCell::new(Slab::new()),
            ops: RefCell::new(Slab::new()),
            parked: Cell::new(false),
            timers: timer::Timers::new(),
            routes: lease::Routes::new(),
            conns: conn::Conns::new(limits),
            w2m,
        })))
    }

    /// Queue `sqe` as a one-shot op holding `carry` until its CQE, whose `res` the
    /// receiver yields. Not submitted. The op's slot, and with it `carry`, is
    /// freed at its one CQE and then reused, so `sqe` must be an opcode that
    /// posts exactly one CQE.
    ///
    /// # Safety
    /// As [`IoUringRing::push`]; what `carry` holds counts as valid until the CQE.
    unsafe fn push_op(
        &self,
        sqe: io_uring::squeue::Entry,
        carry: Option<Rc<conn::Outbound>>,
    ) -> oneshot::Receiver<i32> {
        let (done, rx) = oneshot::channel();
        let id = self.ops.borrow_mut().insert(PendingOp { done, _carry: carry }) as u64;
        // SAFETY: forwarded to the caller.
        unsafe { self.ring.borrow_mut().push(sqe, udata(KIND_OP, id)) };
        rx
    }

    /// Queue an fdatasync of `fd` and submit it now, so it runs while the caller
    /// works on. The future yields the CQE `res`: 0, or a negative errno.
    pub(crate) fn fsync(&self, fd: BorrowedFd<'static>) -> impl Future<Output = i32> {
        let sqe = opcode::Fsync::new(types::Fd(fd.as_raw_fd()))
            .flags(types::FsyncFlags::DATASYNC)
            .build();
        // SAFETY: `fd` is open for the process's life; the SQE names no memory.
        let op = unsafe { self.push_op(sqe, None) };
        self.ring.borrow_mut().submit();
        op
    }
}

#[cfg(test)]
#[path = "tests/reactor.rs"]
mod tests;
