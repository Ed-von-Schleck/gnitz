//! Single-threaded io_uring reactor: the master process's one event loop. A task's
//! waker is its key (see `waker_wake`); one-CQE awaiters park in a
//! [`park::ParkMap`], next-of-many awaiters in a [`wake_queue::WakeQueue`], and
//! every deadline sits in one deadline map.

use std::cell::{Cell, RefCell};
use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::ptr;
use std::rc::Rc;
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};
use std::time::Instant;

use io_uring::types::FutexWaitV;
use rustc_hash::{FxHashMap, FxHashSet};

use self::uring::{Cqe, IoUringRing, CQE_F_MORE};

use crate::runtime::w2m::{W2mReceiver, W2mSlot, W2M_EXCHANGE_RING_ID};
use crate::runtime::wire::DecodedWire;

mod conn;
mod futures;
mod io;
mod park;
mod runloop;
mod sync;
#[cfg(test)]
mod test_support;
mod uring;
mod wake_queue;

pub(crate) use conn::{shutdown, SendBody};

pub(crate) use futures::{FsyncFuture, Lease};
use futures::{Route, TimerFuture};
use park::ParkMap;
use runloop::{RunQueue, REACTOR_RUN_QUEUE};
use wake_queue::WakeQueue;

#[cfg(test)]
pub(crate) use io::InboundBudget;
pub(crate) use io::{ClientConn, RecvBuf, RecvFilter, RecvQueue};
pub use sync::{chan, oneshot, select2, AsyncMutex, AsyncRwLock, Either, ReadGuard, WriteGuard};

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

const KIND_FSYNC: u64 = 3;
const KIND_FUTEX_WAITV: u64 = 4;
const KIND_ACCEPT: u64 = 5;
const KIND_RECV: u64 = 6;
const KIND_SEND: u64 = 7;
/// The CQE of a recv's `AsyncCancel`, whose effect lands on the recv's own CQE.
/// The dispatch arm is a no-op sink.
const KIND_CANCEL_SINK: u64 = 9;

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

// ---------------------------------------------------------------------------
// Reactor
// ---------------------------------------------------------------------------

/// One spawned task. An alias, not a newtype: nothing is added to the boxed
/// future, and a newtype only adds a construction and a field access.
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
    /// Tasks whose wakers fired; the reactor's main loop polls them on
    /// the next tick. The waker vtable reaches this through a
    /// thread-local raw pointer (`REACTOR_RUN_QUEUE`) — see `waker_wake`.
    run_queue: RefCell<RunQueue>,
    /// Scratch buffer `tick` swaps the run queue into, so wakes issued during a
    /// poll schedule for the next tick instead of re-entering this one. Kept
    /// here (rather than rebuilt per tick) to reuse the one allocation.
    tick_scratch: Cell<Vec<usize>>,
    /// Every leased W2M request id and where its frames go. An entry lives
    /// exactly as long as the [`Lease`] that created it.
    routes: RefCell<FxHashMap<u32, Route>>,
    /// The next request id a lease starts from. Never 0 (`AllUnaddressed`) and
    /// never [`W2M_EXCHANGE_RING_ID`]; see [`Reactor::lease`].
    next_request_id: Cell<u32>,
    /// In-flight fdatasyncs; the CQE result is the fdatasync return code.
    fsyncs: ParkMap<i32>,
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
    /// The OOM guard shared by every connection. See [`io::InboundBudget`].
    inbound: Rc<io::InboundBudget>,
    /// The deadlines and ceilings this reactor was built with.
    limits: Limits,
    /// Accepted `(conn_fd, listener_fd)` pairs not yet claimed.
    accepts: RefCell<WakeQueue<(i32, i32)>>,
    /// In-flight client sends, each carrying its body until the CQE.
    sends: ParkMap<i32, SendBody>,
    /// Shutdown flag. `block_until_shutdown` polls until this is set.
    shutdown: Cell<bool>,
    /// Exchange frames, as `(worker, frame)`, awaiting the relay task. The queue
    /// exists from `Reactor::new`, so a frame published before that task is
    /// spawned is delivered rather than dropped.
    exchanges: RefCell<WakeQueue<(usize, DecodedWire)>>,
    /// Last: every `W2mSlot` an earlier field holds releases through it on drop.
    w2m: Rc<W2mReceiver>,
}

impl ReactorShared {
    /// Next local op id, lossless when packed into a CQE's `user_data`.
    fn alloc_op_id(&self) -> u64 {
        let id = self.next_op_id.get();
        self.next_op_id.set(if id >= ID_MASK { 1 } else { id + 1 });
        id
    }

    /// Wake `waker` at `at` (replacing any waker already registered under the key).
    fn wake_at(&self, at: Instant, id: u64, waker: &Waker) {
        match self.deadlines.borrow_mut().entry((at, id)) {
            Entry::Occupied(mut e) => e.get_mut().clone_from(waker),
            Entry::Vacant(e) => {
                e.insert(waker.clone());
            }
        }
    }

    fn cancel_wake(&self, at: Instant, id: u64) {
        self.deadlines.borrow_mut().remove(&(at, id));
    }
}

/// Shared, clonable handle to the reactor. All futures created by the
/// reactor capture an `Rc<ReactorShared>` so they can submit ops and
/// register wakers without borrowing the `Reactor` mutably.
pub struct Reactor {
    inner: Rc<ReactorShared>,
}

impl Reactor {
    pub fn new(ring_capacity: u32, limits: Limits, w2m: Rc<W2mReceiver>) -> std::io::Result<Self> {
        let ring = IoUringRing::new(ring_capacity)?;
        let futex_waitv = (0..w2m.num_workers()).map(|_| FutexWaitV::new()).collect();
        let inner = Rc::new(ReactorShared {
            ring: RefCell::new(ring),
            tasks: RefCell::new(FxHashMap::default()),
            next_task_key: Cell::new(0),
            run_queue: RefCell::new(RunQueue::new()),
            tick_scratch: Cell::new(Vec::with_capacity(16)),
            routes: RefCell::new(FxHashMap::default()),
            next_request_id: Cell::new(1),
            fsyncs: ParkMap::default(),
            futex_waitv: RefCell::new(futex_waitv),
            futex_waitv_armed: Cell::new(false),
            deadlines: RefCell::new(BTreeMap::new()),
            next_op_id: Cell::new(1),
            conns: RefCell::new(FxHashMap::default()),
            inbound: Rc::new(io::InboundBudget::new(limits.inbound_cap)),
            limits,
            accepts: RefCell::new(WakeQueue::default()),
            sends: ParkMap::default(),
            shutdown: Cell::new(false),
            exchanges: RefCell::new(WakeQueue::default()),
            w2m,
        });
        // Publish the run-queue pointer for the waker vtable. `ReactorShared`
        // lives behind `Rc`, so the address is stable for the reactor's life;
        // `Drop for Reactor` clears it.
        REACTOR_RUN_QUEUE.with(|p| {
            assert!(
                p.get().is_null(),
                "a second reactor on this thread would orphan the first's wakes"
            );
            p.set(&inner.run_queue as *const RefCell<RunQueue>);
        });
        Ok(Reactor { inner })
    }

    /// Stop the reactor: the current tick does not sleep, and `block_until_shutdown`
    /// returns after it. Client sends still in flight are abandoned.
    pub fn request_shutdown(&self) {
        self.inner.shutdown.set(true);
    }

    /// `n` consecutive request ids, each answered by one ACK. See [`Lease`].
    pub(crate) fn lease_acks(&self, n: usize) -> Lease {
        self.lease(n, || Route::Ack { ack: None, waker: None })
    }

    /// `n` consecutive request ids, each answered by a train of frames. See
    /// [`Lease`].
    pub(crate) fn lease_train(&self, n: usize) -> Lease {
        self.lease(n, || Route::Train(WakeQueue::default()))
    }

    fn lease(&self, n: usize, route: fn() -> Route) -> Lease {
        let n = n as u32;
        let mut routes = self.inner.routes.borrow_mut();
        let mut base = self.inner.next_request_id.get();
        loop {
            // 0 is `GroupTargets::AllUnaddressed`'s id. The ids are `base..base + n`,
            // exclusive, and `base + n` fits a `u32`, so `u32::MAX` — the exchange id —
            // is never handed out.
            if base == 0 || base.checked_add(n).is_none() {
                base = 1;
            }
            // Never re-lease an id a live lease still routes: after a wrap a long-held
            // scan would otherwise have its route overwritten.
            match (base..base + n).find(|id| routes.contains_key(id)) {
                Some(taken) => base = taken + 1,
                None => break,
            }
        }
        routes.extend((base..base + n).map(|id| (id, route())));
        self.inner.next_request_id.set(base + n);
        Lease::new(Rc::clone(&self.inner), base, n)
    }

    /// Future that completes at `deadline`.
    pub fn timer(&self, deadline: Instant) -> impl Future<Output = ()> {
        TimerFuture::new(deadline, Rc::clone(&self.inner))
    }

    /// The next exchange frame a worker published, as `(worker, frame)`. Rounds
    /// are orchestration policy, assembled by the consumer.
    pub async fn next_exchange(&self) -> (usize, DecodedWire) {
        std::future::poll_fn(|cx| {
            self.inner
                .exchanges
                .borrow_mut()
                .poll(cx)
                .map(|frame| frame.expect("the exchange queue is never closed"))
        })
        .await
    }

    /// Drain every worker's ring.
    fn drain_all_w2m(&self) {
        for w in 0..self.inner.w2m.num_workers() {
            self.drain_w2m_for_worker(w);
        }
    }

    /// Arm the W2M park for a tick about to sleep. False, arming nothing, when the
    /// drain it runs first woke a task.
    fn arm_futex_waitv(&self) -> bool {
        let w2m = &self.inner.w2m;
        if w2m.num_workers() == 0 {
            return true; // nothing to watch, and a zero-length FUTEX_WAITV is -EINVAL
        }
        let mut waitv = self.inner.futex_waitv.borrow_mut();
        let mut refused = false;
        loop {
            self.drain_all_w2m();
            if !self.inner.run_queue.borrow().is_empty() {
                if refused {
                    w2m.clear_waitv(); // a refused arm left some rings' flags set
                }
                return false;
            }
            match w2m.arm_waitv(&mut waitv) {
                Some(armed) => {
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
                None => refused = true,
            }
        }
    }

    /// Submit an fdatasync and await its completion. Returns the CQE `res`
    /// (0 on success, negative errno on failure). The SQE is flushed to the
    /// kernel immediately so the fsync can overlap with subsequent CPU work —
    /// `commit_pushes` awaits its worker ACKs and fires the tick between the
    /// submit and the CQE, and depends on it.
    pub fn fsync(&self, fd: i32) -> FsyncFuture {
        let id = self.inner.alloc_op_id();
        {
            let mut ring = self.inner.ring.borrow_mut();
            ring.prep_fsync(fd, udata(KIND_FSYNC, id));
            ring.flush_sqes("fsync");
        }
        self.inner.fsyncs.open(id, ());
        FsyncFuture { id, inner: Rc::clone(&self.inner) }
    }

    /// Drive the reactor forever; returns when `request_shutdown` is
    /// called. Used by the executor's main loop.
    pub fn block_until_shutdown(&self) {
        while !self.inner.shutdown.get() {
            self.tick(true);
        }
    }

    /// Route every unread slot of worker `w` on its ring id, before anything decodes it.
    fn drain_w2m_for_worker(&self, w: usize) {
        while let Some(slot) = self.inner.w2m.try_read_slot(w) {
            let id = slot.internal_req_id;
            if id == W2M_EXCHANGE_RING_ID {
                let frame = slot.decode(w);
                drop(slot); // free the ring space before the relay task runs
                self.inner.exchanges.borrow_mut().push((w, frame));
                continue;
            }
            match self.inner.routes.borrow_mut().get_mut(&id) {
                // No live lease: the request or scan was abandoned. Dropping the
                // slot undecoded releases its ring space.
                None => {}
                Some(Route::Train(q)) => q.push(slot),
                Some(Route::Ack { ack, waker }) => {
                    // A worker answers each request id once.
                    debug_assert!(ack.is_none(), "worker {w} answered request id {id} twice");
                    *ack = Some((w, slot.control(w)));
                    drop(slot);
                    if let Some(waker) = waker.take() {
                        waker.wake();
                    }
                }
            }
        }
    }
}

#[cfg(test)]
#[path = "tests/reactor.rs"]
mod tests;
