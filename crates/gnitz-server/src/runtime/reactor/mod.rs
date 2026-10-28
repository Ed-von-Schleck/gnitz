//! Single-threaded io_uring reactor: the master process's one event loop, driven
//! by [`Reactor::block_on`] alone. A task's waker is its key (see `waker_wake`);
//! one-CQE awaiters hold a [`oneshot`], next-of-many awaiters park in a
//! [`wake_queue::WakeQueue`], an all-workers round parks on its ACK route, and
//! every deadline sits in one deadline map.

use std::cell::{Cell, RefCell};
use std::collections::BTreeMap;
use std::future::Future;
use std::os::fd::{AsRawFd, BorrowedFd};
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};
use std::time::{Duration, Instant};

use io_uring::{opcode, types};
use rustc_hash::FxHashMap;
use slab::Slab;

use self::uring::IoUringRing;

use crate::runtime::w2m::{W2mReceiver, W2mSlot};

mod conn;
mod futures;
mod io;
mod runloop;
mod sync;
#[cfg(test)]
mod test_support;
#[cfg(test)]
pub(crate) use test_support::{
    admitted_recv_queue, client_pair, egress_pair, poll_until, reactor_with_rings, read_nonblocking, ring_slot,
    spawn_drain,
};
mod uring;
mod wake_queue;

pub(crate) use conn::{PeerGone, SendBody};
pub(crate) use uring::check_host;

use futures::Route;
pub(crate) use futures::{AckLease, TrainFrame, TrainLease, BOOT_READY_REQUEST_ID};
use runloop::run_queue_is_empty;
use wake_queue::WakeQueue;

pub(crate) use io::{ClientConn, Plain, RecvBuf, RecvEnd, RecvFilter, RecvQueue};
pub use sync::{chan, oneshot, select2, AsyncRwLock, Either, ReadGuard, WriteGuard};

/// The ceilings and deadlines a reactor is built with, fixed for its life.
/// Startup resolves them from the environment; a test that has to wait one out
/// picks its own instead, which is why they are arguments rather than globals.
#[derive(Clone, Copy)]
pub(crate) struct Limits {
    /// Ceiling on in-flight inbound frame payload bytes, across every
    /// connection.
    pub inbound_cap: usize,
    /// One kernel send making no progress for this long evicts the client, whose
    /// stalled sends would otherwise pin W2M ring space and block a worker
    /// (`GNITZ_CLIENT_SEND_TIMEOUT_MS`).
    pub client_send_timeout: Duration,
    /// A failed accept is retried after this long, so closing connections get a
    /// window to free fds.
    pub accept_rearm_backoff: Duration,
    /// Ceiling on open client connections, across every listener and
    /// transport (`GNITZ_MAX_CONNS`). A connection accepted past it is closed
    /// at once.
    pub max_conns: usize,
}

impl Limits {
    /// The server's, resolved from the environment.
    pub(crate) fn from_env() -> Limits {
        Limits {
            inbound_cap: io::resolve_inbound_cap(),
            client_send_timeout: Duration::from_millis(gnitz_foundation::env::env_num(
                "GNITZ_CLIENT_SEND_TIMEOUT_MS",
                30_000,
            )),
            accept_rearm_backoff: Duration::from_millis(50),
            max_conns: gnitz_foundation::env::env_num("GNITZ_MAX_CONNS", 256),
        }
    }
}

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
pub struct ReactorShared {
    /// First, so it drops before the memory its SQEs point into.
    ring: RefCell<IoUringRing>,
    /// Live tasks; a task's waker key is its slot plus one. A slot holds `None`
    /// while its task is polled, and is freed for reuse when it completes.
    tasks: RefCell<Slab<Option<Task>>>,
    /// Every live lease's id and what its workers have answered on it.
    routes: RefCell<FxHashMap<u32, Route>>,
    /// The next request id a lease starts from.
    next_request_id: Cell<u32>,
    /// In-flight one-shot ops; an op's id is its slot, freed at its CQE.
    ops: RefCell<Slab<PendingOp>>,
    /// A `FUTEX_WAIT` SQE on the W2M park is outstanding. Cleared by its CQE.
    parked: Cell<bool>,
    /// Every pending deadline, keyed by `(deadline, timer id)` and valued by the
    /// waker to fire. A sleep never outlasts the first.
    deadlines: RefCell<BTreeMap<(Instant, u64), Waker>>,
    /// The next timer id, which orders deadlines sharing an instant.
    next_timer_id: Cell<u64>,
    /// Every connection with a recv armed on it, by fd. See [`conn::Armed`].
    conns: RefCell<FxHashMap<i32, conn::Armed>>,
    /// The OOM guard shared by every connection, charged per inbound frame.
    inbound: Rc<io::Budget>,
    /// Open client connections under `Limits::max_conns`; each [`ClientConn`]
    /// holds one charge of it.
    conn_slots: Rc<io::Budget>,
    /// The deadlines and ceilings this reactor was built with.
    limits: Limits,
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
pub struct Reactor(Rc<ReactorShared>);

impl std::ops::Deref for Reactor {
    type Target = ReactorShared;
    fn deref(&self) -> &ReactorShared {
        &self.0
    }
}

/// The ring's SQ entries. This sets submit batching, not depth: a full SQ is
/// flushed.
const SQ_ENTRIES: u32 = 256;

impl Reactor {
    pub fn new(limits: Limits, w2m: W2mReceiver) -> std::io::Result<Self> {
        let ring = IoUringRing::new(SQ_ENTRIES)?;
        // Before the shared state exists: a refused claim then drops none of it,
        // and so does not end the live reactor's claim.
        runloop::claim_thread();
        Ok(Reactor(Rc::new(ReactorShared {
            ring: RefCell::new(ring),
            tasks: RefCell::new(Slab::new()),
            routes: RefCell::new(FxHashMap::default()),
            next_request_id: Cell::new(BOOT_READY_REQUEST_ID + 1),
            ops: RefCell::new(Slab::new()),
            parked: Cell::new(false),
            deadlines: RefCell::new(BTreeMap::new()),
            next_timer_id: Cell::new(0),
            conns: RefCell::new(FxHashMap::default()),
            inbound: io::Budget::new(limits.inbound_cap),
            conn_slots: io::Budget::new(limits.max_conns),
            limits,
            w2m,
        })))
    }

    /// The next timer id; the 64-bit counter does not wrap within a process's
    /// life.
    fn alloc_timer_id(&self) -> u64 {
        let id = self.next_timer_id.get();
        self.next_timer_id.set(id + 1);
        id
    }

    /// Route every unread slot of every worker's ring.
    fn drain_all_w2m(&self) {
        for w in 0..self.w2m.num_workers() {
            while let Some(slot) = self.w2m.try_read_slot(w) {
                self.route(slot);
            }
        }
    }

    /// Arm the W2M park for a sleep. False, arming nothing, when the drain run
    /// first woke a task.
    fn arm_park(&self) -> bool {
        loop {
            self.drain_all_w2m();
            if !run_queue_is_empty() {
                return false;
            }
            if let Some((word, at)) = self.w2m.arm() {
                // A 32-bit word with no `FUTEX2_PRIVATE`, since the park is
                // `MAP_SHARED` across `fork()`; the mask matches any waker.
                const FUTEX2_SIZE_U32: u32 = 2;
                let mask = libc::FUTEX_BITSET_MATCH_ANY as u32 as u64;
                let sqe = opcode::FutexWait::new(word.cast(), at as u64, mask, FUTEX2_SIZE_U32).build();
                // SAFETY: the word is in a never-unmapped shared mapping, and the
                // SQE names no other memory.
                unsafe { self.ring.borrow_mut().push(sqe, udata(KIND_PARK, 0)) };
                self.parked.set(true);
                return true;
            }
        }
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
