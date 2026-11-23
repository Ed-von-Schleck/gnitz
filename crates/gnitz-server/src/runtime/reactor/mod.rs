//! Single-threaded io_uring reactor: the master process's one event loop, driven
//! by [`Reactor::block_on`] alone. A task's waker is its key (see `waker_wake`);
//! a one-CQE awaiter polls its op's slot, next-of-many awaiters park in
//! a [`WakeQueue`](crate::runtime::sync::WakeQueue), an ACK round parks on the
//! applied [`Level`] until every launched worker has applied the log through
//! its group, and every deadline sits in one deadline map.
//!
//! This file holds the reactor's state and the CQE `user_data` encoding. The
//! rest is split by concept: `runloop` (scheduling, the pass and its sleep on
//! the W2M park, CQE dispatch), `op` (one-shot ops), `lease` (W2M ingest, the
//! applied level it raises and the leases it routes to), `conn` (client
//! connections, their limits, and the `Peer` a connection's task sends and
//! receives through), `timer` (deadlines) and `uring` (the ring and its syscalls).
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

use io_uring::{IoUring, opcode, types};
use slab::Slab;

use crate::runtime::sync::Level;
use crate::runtime::w2m::W2mReceiver;

mod conn;
mod lease;
mod op;
mod runloop;
#[cfg(test)]
pub(super) mod test_support;
mod timer;
mod uring;

pub(crate) use conn::{Budget, Charge, Limits, Peer, PeerGone, RecvBuf, RecvEnd, SendBody, Session};
pub(crate) use lease::{AckWait, Train, TrainLease};
pub(crate) use uring::check_host;

// ---------------------------------------------------------------------------
// CQE user_data encoding (high 8 bits = kind, low 56 bits = id)
// ---------------------------------------------------------------------------

/// A one-shot op, whose CQE lands in its slot.
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

// ---------------------------------------------------------------------------
// Reactor
// ---------------------------------------------------------------------------

type Task = Pin<Box<dyn Future<Output = ()>>>;

/// The state behind every [`Reactor`] handle.
pub(crate) struct ReactorShared {
    ring: RefCell<IoUring>,
    /// Live tasks; a task's waker key is its slot plus one. A slot holds `None`
    /// while its task is polled, and is freed for reuse when it completes.
    tasks: RefCell<Slab<Option<Task>>>,
    /// A `FUTEX_WAIT` SQE on the W2M park is outstanding. Cleared by its CQE.
    parked: Cell<bool>,
    ops: op::Ops,
    timers: timer::Timers,
    routes: RefCell<lease::Routes>,
    /// How far every launched worker has applied the log: the lowest position
    /// any of them has published.
    applied: Level,
    conns: conn::Conns,
    w2m: W2mReceiver,
    _thread: runloop::ThreadClaim,
}

/// The ring stops before the memory its SQEs name is freed: this runs ahead of
/// every field's own drop.
impl Drop for ReactorShared {
    fn drop(&mut self) {
        self.cancel_all();
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
    pub(crate) fn new(limits: Limits, w2m: W2mReceiver) -> std::io::Result<Self> {
        Ok(Reactor(Rc::new(ReactorShared {
            ring: RefCell::new(IoUring::new(uring::SQ_ENTRIES)?),
            tasks: RefCell::new(Slab::new()),
            parked: Cell::new(false),
            ops: op::Ops::default(),
            timers: timer::Timers::new(),
            routes: RefCell::new(lease::Routes::new()),
            applied: Level::default(),
            conns: conn::Conns::new(limits),
            w2m,
            _thread: runloop::ThreadClaim::take(),
        })))
    }
}
