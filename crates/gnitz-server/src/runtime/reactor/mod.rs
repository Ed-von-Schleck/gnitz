//! Single-threaded io_uring reactor: the master process's one event loop. A task's
//! waker is its key (see `waker_wake`); one-CQE awaiters hold a [`oneshot`],
//! next-of-many awaiters park in a [`wake_queue::WakeQueue`], and every deadline
//! sits in one deadline map.

use std::cell::{Cell, RefCell};
use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};
use std::time::Instant;

use io_uring::types::FutexWaitV;
use rustc_hash::{FxHashMap, FxHashSet};

use self::uring::{Cqe, IoUringRing};

use crate::runtime::w2m::{W2mReceiver, W2mSlot, BOOT_READY_REQUEST_ID, W2M_EXCHANGE_RING_ID};
use crate::runtime::wire::DecodedWire;

mod conn;
mod futures;
mod io;
mod runloop;
mod sync;
#[cfg(test)]
mod test_support;
#[cfg(test)]
pub(crate) use test_support::reactor_with_rings;
mod uring;
mod wake_queue;

pub(crate) use conn::{PeerGone, SendBody};

pub(crate) use futures::{AckLease, TrainFrame, TrainLease};
use futures::{AckRoute, TimerFuture, TrainRoute};
use runloop::run_queue_is_empty;
use wake_queue::WakeQueue;

pub(crate) use io::{Budget, Charge, ClientConn, Plain, RecvBuf, RecvEnd, RecvFilter, RecvQueue};
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
    /// stalled sends would otherwise pin W2M ring space and block a worker.
    pub client_send_timeout: std::time::Duration,
    /// How long a listener whose multishot accept was cancelled on fd
    /// exhaustion waits before re-arming, so closing connections get a window
    /// to free fds.
    pub accept_rearm_backoff: std::time::Duration,
}

impl Limits {
    /// The server's, resolved from the environment.
    pub(crate) fn from_env() -> Limits {
        Limits {
            inbound_cap: io::resolve_inbound_cap(),
            client_send_timeout: conn::resolve_client_send_timeout(),
            accept_rearm_backoff: std::time::Duration::from_millis(50),
        }
    }

    /// Every send carries `client_send_timeout`, so it is generous here: a drain
    /// thread descheduled on a loaded box must not evict the sender of a socket
    /// test. A test that waits a deadline out sets its own.
    #[cfg(test)]
    pub(crate) const TEST: Limits = Limits {
        inbound_cap: usize::MAX,
        client_send_timeout: std::time::Duration::from_secs(30),
        accept_rearm_backoff: std::time::Duration::from_millis(2),
    };
}

// ---------------------------------------------------------------------------
// CQE user_data encoding (high 8 bits = kind, low 56 bits = id)
// ---------------------------------------------------------------------------

/// A one-shot op whose CQE lands in `ops`.
const KIND_OP: u64 = 3;
const KIND_FUTEX_WAITV: u64 = 4;
const KIND_ACCEPT: u64 = 5;
const KIND_RECV: u64 = 6;

const KIND_SHIFT: u64 = 56;
const ID_MASK: u64 = 0x00FF_FFFF_FFFF_FFFF;

#[inline]
const fn udata(kind: u64, id: u64) -> u64 {
    (kind << KIND_SHIFT) | (id & ID_MASK)
}

#[inline]
const fn udata_kind(u: u64) -> u64 {
    u >> KIND_SHIFT
}

#[inline]
const fn udata_id(u: u64) -> u64 {
    u & ID_MASK
}

/// Leave `waker` in `slot` for the next wake, reusing the waker already there.
fn park_waker(slot: &mut Option<Waker>, waker: &Waker) {
    match slot {
        Some(w) => w.clone_from(waker),
        None => *slot = Some(waker.clone()),
    }
}

// ---------------------------------------------------------------------------
// Reactor
// ---------------------------------------------------------------------------

type Task = Pin<Box<dyn Future<Output = ()>>>;

/// Field order is drop order, and both ends of it are fixed; see `ring` and
/// `w2m`.
struct ReactorShared {
    /// First, so it closes before the memory its SQEs point into is freed. Closing
    /// only queues their cancellation: drop no reactor whose recv a peer can still feed.
    ring: RefCell<IoUringRing>,
    /// Live tasks keyed by a monotonically-increasing id. A task's future may
    /// spawn during its own poll, so `poll_task` takes the future out and puts
    /// it back at the same key rather than holding a borrow across the poll.
    tasks: RefCell<FxHashMap<usize, Task>>,
    next_task_key: Cell<usize>,
    /// Every live [`AckLease`] id and what its workers have answered.
    acks: RefCell<FxHashMap<u32, AckRoute>>,
    /// Every live [`TrainLease`] id and its workers' frame queues.
    trains: RefCell<FxHashMap<u32, TrainRoute>>,
    /// The next request id a lease starts from.
    next_request_id: Cell<u32>,
    /// In-flight one-shot ops, by op id.
    ops: RefCell<FxHashMap<u64, PendingOp>>,
    /// The array the `FUTEX_WAITV` SQE is prepped over, one entry per worker
    /// ring. The kernel copies it at submit, so it need not outlive the SQE.
    futex_waitv: RefCell<Box<[FutexWaitV]>>,
    /// A `FUTEX_WAITV` SQE is outstanding. Cleared by its CQE.
    futex_waitv_armed: Cell<bool>,
    /// Every pending deadline, keyed by `(deadline, op id)` and valued by the
    /// waker to fire. A sleep never outlasts the first.
    deadlines: RefCell<BTreeMap<(Instant, u64), Waker>>,
    /// One counter for every local op, so an id is unique in every map keyed on
    /// it.
    next_op_id: Cell<u64>,
    /// Every connection with a recv armed on it, by fd. See [`conn::Armed`].
    conns: RefCell<FxHashMap<i32, conn::Armed>>,
    /// The OOM guard shared by every connection, charged per inbound frame.
    inbound: Rc<io::Budget>,
    /// The deadlines and ceilings this reactor was built with.
    limits: Limits,
    /// Every attached listener, indexed by the id its accept SQEs carry. See
    /// [`conn::Listener`].
    listeners: RefCell<Vec<conn::Listener>>,
    /// Shutdown flag. `block_until_shutdown` polls until this is set.
    shutdown: Cell<bool>,
    /// Exchange frames, as `(worker, frame)`, awaiting their round's driver. The
    /// queue exists from `Reactor::new`, so a frame published before the driver
    /// awaits is delivered rather than dropped.
    exchanges: RefCell<WakeQueue<(usize, DecodedWire)>>,
    /// Last: every `W2mSlot` the reactor holds, in a field or in a task, releases
    /// through it on drop.
    w2m: W2mReceiver,
}

/// A one-shot op's CQE `res` and what it carried.
type OpResult = (i32, Option<SendBody>);

/// A submitted op awaiting its CQE.
struct PendingOp {
    done: oneshot::Sender<OpResult>,
    /// Memory the kernel may read until the CQE.
    carry: Option<SendBody>,
}

impl ReactorShared {
    /// Next local op id, lossless when packed into a CQE's `user_data`: the
    /// 56-bit counter does not wrap within a process's life.
    fn alloc_op_id(&self) -> u64 {
        let id = self.next_op_id.get();
        self.next_op_id.set(id + 1);
        id
    }
}

/// The reactor. The futures it creates capture an `Rc<ReactorShared>`, since
/// they must be `'static`.
pub struct Reactor {
    inner: Rc<ReactorShared>,
}

impl Reactor {
    pub fn new(ring_capacity: u32, limits: Limits, w2m: W2mReceiver) -> std::io::Result<Self> {
        let ring = IoUringRing::new(ring_capacity)?;
        let futex_waitv = (0..w2m.num_workers()).map(|_| FutexWaitV::new()).collect();
        let inner = Rc::new(ReactorShared {
            ring: RefCell::new(ring),
            tasks: RefCell::new(FxHashMap::default()),
            next_task_key: Cell::new(0),
            acks: RefCell::new(FxHashMap::default()),
            trains: RefCell::new(FxHashMap::default()),
            next_request_id: Cell::new(BOOT_READY_REQUEST_ID),
            ops: RefCell::new(FxHashMap::default()),
            futex_waitv: RefCell::new(futex_waitv),
            futex_waitv_armed: Cell::new(false),
            deadlines: RefCell::new(BTreeMap::new()),
            next_op_id: Cell::new(1),
            conns: RefCell::new(FxHashMap::default()),
            inbound: io::Budget::new(limits.inbound_cap),
            limits,
            listeners: RefCell::new(Vec::new()),
            shutdown: Cell::new(false),
            exchanges: RefCell::new(WakeQueue::default()),
            w2m,
        });
        runloop::claim_thread();
        Ok(Reactor { inner })
    }

    /// Stop the reactor: the current tick does not sleep, and `block_until_shutdown`
    /// returns after it. Client sends still in flight are abandoned.
    pub fn request_shutdown(&self) {
        self.inner.shutdown.set(true);
    }

    /// Future that completes at `deadline`.
    pub fn timer(&self, deadline: Instant) -> impl Future<Output = ()> {
        TimerFuture::new(deadline, Rc::clone(&self.inner))
    }

    /// The next exchange frame a worker published, as `(worker, frame)`. Rounds
    /// are orchestration policy, assembled by the consumer. One awaiter at a time.
    pub async fn next_exchange(&self) -> (usize, DecodedWire) {
        struct Unpark<'a>(&'a RefCell<WakeQueue<(usize, DecodedWire)>>);
        impl Drop for Unpark<'_> {
            fn drop(&mut self) {
                self.0.borrow_mut().unpark();
            }
        }
        let _unpark = Unpark(&self.inner.exchanges);
        std::future::poll_fn(|cx| {
            let mut q = self.inner.exchanges.borrow_mut();
            debug_assert!(!q.parked_elsewhere(cx.waker()), "two tasks await the exchange queue");
            q.poll(cx)
        })
        .await
    }

    /// Route every unread slot of every worker's ring.
    fn drain_all_w2m(&self) {
        for w in 0..self.inner.w2m.num_workers() {
            while let Some(slot) = self.inner.w2m.try_read_slot(w) {
                self.inner.route(w, slot);
            }
        }
    }

    /// Arm the W2M park for a sleep. False, arming nothing, when the drain run
    /// first woke a task.
    fn arm_futex_waitv(&self) -> bool {
        let w2m = &self.inner.w2m;
        if w2m.num_workers() == 0 {
            return true; // nothing to watch, and a zero-length FUTEX_WAITV is -EINVAL
        }
        let mut waitv = self.inner.futex_waitv.borrow_mut();
        loop {
            self.drain_all_w2m();
            if !run_queue_is_empty() {
                return false;
            }
            if let Some(armed) = w2m.arm_waitv(&mut waitv) {
                // SAFETY: the kernel copies the array at submit, and it lives in
                // `futex_waitv` for the reactor's life regardless; every `uaddr`
                // is a word of a W2M mapping that is never unmapped.
                unsafe {
                    self.inner.ring.borrow_mut().prep_futex_waitv(
                        armed.as_ptr(),
                        armed.len() as u32,
                        udata(KIND_FUTEX_WAITV, 0),
                    );
                }
                self.inner.futex_waitv_armed.set(true);
                return true;
            }
        }
    }

    /// Queue one op SQE, prepped by `prep` under its `user_data`. `carry` lives
    /// until the CQE, which resolves the receiver with it.
    fn submit_op(
        &self,
        prep: impl FnOnce(&mut IoUringRing, u64),
        carry: Option<SendBody>,
    ) -> oneshot::Receiver<OpResult> {
        let id = self.inner.alloc_op_id();
        prep(&mut self.inner.ring.borrow_mut(), udata(KIND_OP, id));
        let (done, rx) = oneshot::channel();
        self.inner.ops.borrow_mut().insert(id, PendingOp { done, carry });
        rx
    }

    /// Submit an fdatasync and flush it to the kernel now, so it runs while the
    /// caller works on. The future yields the CQE `res`: 0, or a negative errno.
    pub fn fsync(&self, fd: i32) -> impl Future<Output = i32> {
        let op = self.submit_op(|ring, u| ring.prep_fsync(fd, u), None);
        if let Err(e) = self.inner.ring.borrow_mut().submit() {
            gnitz_error!(
                "reactor: fsync SQE submit failed (errno={}); it goes out with the next tick",
                e
            );
        }
        async move { op.await.0 }
    }

    /// Drive the reactor forever; returns when `request_shutdown` is
    /// called. Used by the executor's main loop.
    pub fn block_until_shutdown(&self) {
        while !self.inner.shutdown.get() {
            self.tick(true);
        }
    }
}

#[cfg(test)]
#[path = "tests/reactor.rs"]
mod tests;
