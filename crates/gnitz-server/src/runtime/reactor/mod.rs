//! Single-threaded io_uring reactor: the master process's one event loop.
//!
//! Production drives it through `spawn` and `block_until_shutdown`; `timer` and
//! `fsync` for kernel ops; the client-connection family in [`conn`]; and, for
//! worker traffic, `alloc_replies` + `await_reply`, `await_scan_slot` and
//! `next_exchange`. It owns its own `IoUringRing`, separate from the executor's
//! transport ring.
//!
//! Design notes:
//!
//! - A task's waker *is* its key: the key is cast to the vtable's `*const ()`
//!   data pointer and waking reaches the run queue through a thread-local raw
//!   pointer set in `Reactor::new`, so nothing is allocated or reference-counted
//!   per wake. See `waker_wake` in [`runloop`].
//! - Everything that awaits a single completion — timer, W2M reply, fsync,
//!   send, raw recv — parks in a [`park::ParkMap`]. An entry exists exactly
//!   while its op is outstanding, so a late CQE can tell "deliver this" from
//!   "the awaiter is gone" without a per-family tombstone set.
//! - Everything that awaits the *next* of many — an accepted connection, a
//!   scan's continuation frames, an exchange frame, a completed inbound client
//!   frame — queues in a [`wake_queue::WakeQueue`].
//! - Those two are the shared mechanisms; [`sync`]'s primitives park their own
//!   wakers besides — a set per mode for the locks, an inline slot for
//!   `oneshot`.
//! - CQE `user_data` packs an 8-bit kind tag in the high byte and a
//!   56-bit id in the low bits, where id is a request/op id (not an fd,
//!   except for accept and recv, which route on the fd itself). Safe from
//!   collisions because the reactor owns its own ring.
//!
//! Unit tests live in `tests/<module>.rs`, attached with `#[path]` to the module
//! they cover, so each stays that module's own `tests` child and reaches its
//! private items.

use std::cell::{Cell, OnceCell, RefCell, RefMut};
use std::future::Future;
use std::pin::Pin;
use std::ptr;
use std::rc::Rc;
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};
use std::time::Instant;

use io_uring::types::{FutexWaitV, Timespec};
use rustc_hash::{FxHashMap, FxHashSet};

use self::uring::{Cqe, IoUringRing, CQE_F_MORE};

use crate::runtime::w2m::{futex_waitv_entry, W2mReceiver, W2mSlot};
use crate::runtime::wire::DecodedWire;
use gnitz_wire::{FLAG_EXCHANGE, MAX_WORKERS};

/// High bit of `internal_req_id` (u32), set on scan-allocated request ids.
/// Both spaces draw from the one `next_request_id` counter; nothing keys on
/// both at once (replies on the `u64` in `replies`, scans on the `u32` in
/// `scans`), so the flag alone separates them.
///
/// It earns its place by classifying a frame *before anything decodes it*, so
/// an abandoned scan's continuation frames are dropped at the ring boundary
/// rather than each allocating a `Batch` on the way to being discarded.
const SCAN_REQ_ID_FLAG: u32 = 1 << 31;
const MAX_REGULAR_REQ_ID: u64 = (SCAN_REQ_ID_FLAG - 1) as u64; // 0x7FFFFFFF

mod conn;
mod futures;
pub mod io;
mod park;
mod runloop;
mod sync;
#[cfg(test)]
mod test_support;
mod uring;
mod wake_queue;

pub(crate) use conn::{guard_egress_deadline, shutdown, SendPayload};

use futures::{ExchangeFuture, ScanRoute, ScanSlotFuture, SendCarry, TimerFuture};
pub(crate) use futures::{FsyncFuture, PeerToken, ReplyFuture, ScanLease};
use park::ParkMap;
use runloop::{RunQueue, REACTOR_RUN_QUEUE};
use wake_queue::WakeQueue;

pub use io::RecvBuf;
pub use sync::{
    chan, join_all_unpin, join_into, oneshot, select2, AsyncMutex, AsyncRwLock, Either, ReadGuard, WriteGuard,
};

/// The ceilings and deadlines a reactor is built with, fixed for its life.
/// Startup resolves them from the environment; a test that has to wait one out
/// picks its own instead, which is why they are arguments rather than globals.
#[derive(Clone, Copy)]
pub(crate) struct Limits {
    /// Ceiling on in-flight inbound frame payload bytes, across every
    /// connection.
    pub inbound_cap: usize,
    /// Per-frame client-egress deadline; a send making no progress for this
    /// long evicts the client.
    pub client_send_timeout: std::time::Duration,
    /// How long a listener whose multishot accept was cancelled on fd
    /// exhaustion waits before re-arming, giving `reap_closing_conns` a window
    /// to free some.
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

    /// Prompt deadlines, so the tests that wait one out cost milliseconds.
    #[cfg(test)]
    pub(crate) const TEST: Limits = Limits {
        inbound_cap: usize::MAX,
        client_send_timeout: std::time::Duration::from_millis(10),
        accept_rearm_backoff: std::time::Duration::from_millis(2),
    };
}

// ---------------------------------------------------------------------------
// CQE user_data encoding (high 8 bits = kind, low 56 bits = id)
// ---------------------------------------------------------------------------

const KIND_TIMEOUT: u64 = 2;
const KIND_FSYNC: u64 = 3;
const KIND_FUTEX_WAITV: u64 = 4;
const KIND_ACCEPT: u64 = 5;
const KIND_RECV: u64 = 6;
const KIND_SEND: u64 = 7;
/// CQE tag for an `AsyncCancel` submitted against another SQE, so the
/// cancelled op's kernel state is reclaimed promptly. The dispatch arm is a
/// pure no-op sink — the cancellation's *effect* is the target op's own
/// `-ECANCELED` CQE under its own kind.
const KIND_CANCEL_SINK: u64 = 9;
/// One-shot raw recv into a caller-owned buffer (`Reactor::recv_raw`) — the
/// TLS read pump's undeframed byte source. Deliberately NOT `KIND_RECV`:
/// that handler silently drops CQEs for fds absent from `conns`, and TLS
/// fds never call `register_conn`.
const KIND_RAW_RECV: u64 = 10;

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

/// Take the next id from `cell`, wrapping back to `base` past `max`.
fn bump_id(cell: &Cell<u64>, base: u64, max: u64) -> u64 {
    let id = cell.get();
    cell.set(if id >= max { base } else { id + 1 });
    id
}

// ---------------------------------------------------------------------------
// Reactor
// ---------------------------------------------------------------------------

/// One spawned task. An alias, not a newtype: nothing is added to the boxed
/// future, and a newtype only adds a construction and a field access.
type Task = Pin<Box<dyn Future<Output = ()>>>;

struct ReactorShared {
    /// `Option` only so [`Drop for ReactorShared`] can close the io_uring fd
    /// before any buffer an in-flight SQE points at is freed; it is `Some` for
    /// the whole of the reactor's life, and [`Self::ring`] is how every caller
    /// reaches it.
    ring: RefCell<Option<IoUringRing>>,
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
    /// W2M replies keyed by request_id. A slot is opened by [`ReplyLease`], at
    /// id-allocation time, and delivered by `route_reply`.
    replies: ParkMap<DecodedWire>,
    /// In-flight fdatasyncs; the CQE result is the fdatasync return code.
    fsyncs: ParkMap<i32>,
    /// Pointer-stable storage for the reactor's persistent
    /// `FUTEX_WAITV` SQE. The kernel dereferences this array
    /// asynchronously, so it must outlive the SQE. A single SQE covers
    /// every worker's `write_cursor` word; we own it on the heap, and
    /// tear it down only after cancelling the SQE in
    /// `request_shutdown` and awaiting the `-ECANCELED` CQE.
    futex_waitv_storage: RefCell<Option<Box<[FutexWaitV]>>>,
    /// True while an outstanding `FUTEX_WAITV` SQE exists whose
    /// `FutexWaitV` array lives in `futex_waitv_storage`. Cleared when that
    /// SQE's CQE is drained, which is also when the kernel releases its
    /// reference to the array — so this false is what makes freeing the
    /// storage safe.
    futex_waitv_armed: Cell<bool>,
    /// In-flight io_uring Timeout ops, each carrying the `Timespec` its SQE
    /// points at — the kernel reads that address until the CQE lands, which is
    /// exactly the lifetime a slot's carry has.
    timers: ParkMap<(), Box<Timespec>>,
    /// Recycled `Timespec` boxes: a timer is armed per client egress frame, so
    /// the alloc/free per frame is worth avoiding.
    #[allow(clippy::vec_box)]
    spec_pool: RefCell<Vec<Box<Timespec>>>,
    /// W2M request ids the workers echo back, for both the regular and the scan
    /// space. Kept separate from `next_op_id` because the protocol constrains
    /// their range (see `SCAN_REQ_ID_FLAG`).
    next_request_id: Cell<u64>,
    /// Ids for purely local kernel ops (timer / fsync / send / raw recv). They
    /// share a counter because each family has its own park map and its own
    /// `KIND_*` tag, so equal ids never collide.
    next_op_id: Cell<u64>,
    /// Per-fd connection state: recv decoder, delivery queue and send
    /// accounting. Boxed so the inline hdr buffer address survives HashMap
    /// resizes — io_uring SQEs capture the pointer.
    conns: RefCell<FxHashMap<i32, Box<io::Conn>>>,
    /// The OOM guard shared by every connection. See [`io::InboundBudget`].
    inbound: Rc<io::InboundBudget>,
    /// The deadlines and ceilings this reactor was built with.
    limits: Limits,
    /// Accepted `(conn_fd, listener_fd)` pairs the kernel has delivered but the
    /// accept loop has not claimed. The listener fd rides the multishot-accept
    /// SQE's udata `id` field, so the accept loop can route AF_UNIX vs TLS
    /// connections without reactor state.
    accepts: RefCell<WakeQueue<(i32, i32)>>,
    /// In-flight client sends. The slot carries the buffer keep-alive and the
    /// target fd, so the CQE handler settles a send whose awaiter has already
    /// been dropped without any side table.
    sends: ParkMap<i32, SendCarry>,
    /// In-flight raw recvs (`Reactor::recv_raw`). The slot carries the caller's
    /// buffer, so a late kernel write always lands in live memory.
    raw_recvs: ParkMap<i32, Vec<u8>>,
    /// Shutdown flag. `block_until_shutdown` polls until this is set.
    shutdown: Cell<bool>,
    /// `FLAG_EXCHANGE` frames, as `(worker, frame)`, awaiting the relay task.
    /// The queue exists from `Reactor::new`, so a frame published before that
    /// task is spawned is delivered rather than dropped.
    exchanges: RefCell<WakeQueue<(usize, DecodedWire)>>,
    /// Scan routing state keyed by `internal_req_id`, one entry per live
    /// `ScanLease`-held id (see [`ScanRoute`]).
    scans: RefCell<FxHashMap<u32, ScanRoute>>,
    /// Fds that have been marked closing via `close_fd`. `reap_closing_conns`
    /// iterates only this set (O(closing)) rather than all connections (O(all)).
    closing_fds: RefCell<FxHashSet<i32>>,
    /// A `W2mSlot` holds a raw `*mut InFlightState` into this `W2mReceiver` and
    /// calls `release()` through it on drop. Slots outlive their stack frame in
    /// `scans` and in a `sends` slot's keep-alive, so the receiver must outlive
    /// both — which the `MasterDispatcher`'s own `Rc`, outliving the reactor,
    /// guarantees.
    w2m: OnceCell<Rc<W2mReceiver>>,
}

impl Drop for ReactorShared {
    fn drop(&mut self) {
        // Closing the io_uring fd cancels the in-flight SQEs and drops the
        // kernel's references to the buffers they point at. Only then may those
        // buffers — the `sends` / `raw_recvs` / `timers` park slots and `conns`
        // — be freed, which the field drops after this do.
        self.ring.borrow_mut().take();
    }
}

impl ReactorShared {
    /// The io_uring ring. Panics only after [`Drop for ReactorShared`] has
    /// closed it, which is past the last point anything can submit.
    #[inline]
    fn ring(&self) -> RefMut<'_, IoUringRing> {
        RefMut::map(self.ring.borrow_mut(), |r| r.as_mut().expect("reactor ring closed"))
    }

    /// Next id for a local kernel op (timer / fsync / send / raw recv). Wraps
    /// within `ID_MASK` so packing it into a CQE's `user_data` is lossless.
    fn alloc_op_id(&self) -> u64 {
        bump_id(&self.next_op_id, 1, ID_MASK)
    }
}

/// Shared, clonable handle to the reactor. All futures created by the
/// reactor capture an `Rc<ReactorShared>` so they can submit ops and
/// register wakers without borrowing the `Reactor` mutably.
pub struct Reactor {
    inner: Rc<ReactorShared>,
}

/// One dispatch's W2M reply ids, with their park slots open for the lease's
/// whole lifetime. Because a slot exists before the request is emitted, a reply
/// that beats its awaiter is parked rather than dropped — so a caller may
/// `.await` anything at all between emitting and collecting. Derefs to `[u64]`.
pub(crate) struct ReplyLease {
    inner: Rc<ReactorShared>,
    ids: Vec<u64>,
}

impl std::ops::Deref for ReplyLease {
    type Target = [u64];
    fn deref(&self) -> &[u64] {
        &self.ids
    }
}

impl Drop for ReplyLease {
    fn drop(&mut self) {
        // Closed, not abandoned: a worker that dies mid-request never sends the
        // reply that would retire an abandoned slot.
        for &id in &self.ids {
            self.inner.replies.close(id);
        }
    }
}

/// Submit a `FUTEX_WAITV` SQE with a deliberately mismatched expected
/// value (returns -EAGAIN immediately when the opcode is supported).
/// Aborts if the kernel returns -EINVAL or -ENOSYS — those signal
/// that the opcode isn't available, and the W2M master-wait path
/// would silently fail to deliver CQEs. Linux 6.7+ required.
///
/// Memoised: whether the running kernel has the opcode cannot change while the
/// process lives, so the probe's own io_uring cycle runs once.
fn probe_futex_waitv_support() {
    use std::sync::atomic::AtomicU32;
    use std::sync::Once;

    static PROBED: Once = Once::new();
    PROBED.call_once(|| {
        let atomic = Box::new(AtomicU32::new(42));
        let futexv: Box<[FutexWaitV; 1]> = Box::new([futex_waitv_entry(&*atomic as *const AtomicU32, 0)]);

        let mut ring = match IoUringRing::new(8) {
            Ok(r) => r,
            Err(e) => gnitz_fatal_abort!("reactor: probe io_uring init failed: {}", e,),
        };
        // SAFETY: `futexv` and `atomic` outlive the CQE drained below.
        unsafe { ring.prep_futex_waitv(futexv.as_ptr(), 1, 0xFEED) };
        if let Err(e) = ring.submit_and_wait_timeout(1, -1) {
            gnitz_fatal_abort!("reactor: probe submit_and_wait failed (errno={})", e);
        }
        let mut out = [Cqe::default(); 1];
        if ring.drain_cqes(&mut out) != 1 {
            gnitz_fatal_abort!("reactor: probe produced no CQE");
        }
        if out[0].res == -libc::ENOSYS || out[0].res == -libc::EINVAL {
            gnitz_fatal_abort!(
                "reactor: io_uring IORING_OP_FUTEX_WAITV not supported (res={}); \
                 Linux 6.7+ required for the W2M tail-chasing-ring transport.",
                out[0].res,
            );
        }
    });
}

impl Reactor {
    pub fn new(ring_capacity: u32, limits: Limits) -> std::io::Result<Self> {
        probe_futex_waitv_support();
        let ring = IoUringRing::new(ring_capacity)?;
        let inner = Rc::new(ReactorShared {
            ring: RefCell::new(Some(ring)),
            tasks: RefCell::new(FxHashMap::default()),
            next_task_key: Cell::new(0),
            run_queue: RefCell::new(RunQueue::new()),
            tick_scratch: Cell::new(Vec::with_capacity(16)),
            replies: ParkMap::default(),
            fsyncs: ParkMap::default(),
            w2m: OnceCell::new(),
            futex_waitv_storage: RefCell::new(None),
            futex_waitv_armed: Cell::new(false),
            timers: ParkMap::default(),
            spec_pool: RefCell::new(Vec::new()),
            next_request_id: Cell::new(1),
            next_op_id: Cell::new(1),
            conns: RefCell::new(FxHashMap::default()),
            inbound: Rc::new(io::InboundBudget::new(limits.inbound_cap)),
            limits,
            accepts: RefCell::new(WakeQueue::default()),
            sends: ParkMap::default(),
            raw_recvs: ParkMap::default(),
            shutdown: Cell::new(false),
            exchanges: RefCell::new(WakeQueue::default()),
            closing_fds: RefCell::new(FxHashSet::default()),
            scans: RefCell::new(FxHashMap::default()),
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

    /// Request reactor shutdown — the next `block_until_shutdown` tick
    /// exits cleanly. Also cancels the outstanding `FUTEX_WAITV` SQE
    /// (if any) so its `FutexWaitV` array storage can be dropped
    /// safely; waits for the `-ECANCELED` CQE before returning.
    ///
    /// In-flight client SENDs are abandoned, not drained: a reply whose CQE has
    /// not landed is lost, so a client can see its connection close instead of
    /// its last response. Durability is unaffected — `watchdog` runs the full
    /// checkpoint sequence to completion before it calls this.
    pub fn request_shutdown(&self) {
        self.inner.shutdown.set(true);
        self.cancel_futex_waitv_and_wait();
        // Unblock a sleeping reactor by scheduling any live task directly.
        // We're on the reactor thread, so a direct push is sound — no
        // waker round-trip needed.
        if let Some(&key) = self.inner.tasks.borrow().keys().next() {
            self.inner.run_queue.borrow_mut().push(key);
        }
    }

    /// If a `FUTEX_WAITV` SQE is armed, submit an `AsyncCancel` against it and
    /// drive the ring until its CQE is drained — which is when the kernel
    /// releases its reference to the `FutexWaitV` array, and so when freeing
    /// that array becomes safe. The `AsyncCancel`'s own CQE points at nothing
    /// and lands on the no-op `KIND_CANCEL_SINK`.
    ///
    /// If the CQE does not arrive within 2 s we abort rather than free storage
    /// the kernel still points at. A 2 s wait here is already pathological.
    ///
    /// `armed` is set at prep time, not submit time, so the cancel and its target
    /// can leave in the same `io_uring_enter`. That is still ordered: io_uring
    /// consumes SQEs in submission order within one enter, and neither is
    /// `IOSQE_ASYNC`.
    fn cancel_futex_waitv_and_wait(&self) {
        if !self.inner.futex_waitv_armed.get() {
            return;
        }
        {
            let mut ring = self.inner.ring();
            ring.prep_async_cancel(udata(KIND_FUTEX_WAITV, 0), udata(KIND_CANCEL_SINK, 0));
            let _ = ring.submit_and_wait_timeout(0, 0);
        }
        let deadline = Instant::now() + std::time::Duration::from_millis(2000);
        while self.inner.futex_waitv_armed.get() && Instant::now() < deadline {
            self.drain_cqes_into_wakers();
            if !self.inner.futex_waitv_armed.get() {
                break;
            }
            let _ = self.inner.ring().submit_and_wait_timeout(1, 100);
        }
        if self.inner.futex_waitv_armed.get() {
            gnitz_fatal_abort!(
                "reactor: FUTEX_WAITV cancel did not complete within 2s — \
                 freeing storage now would be a UAF"
            );
        }
        // Safe to drop now: no in-flight SQE references the storage.
        *self.inner.futex_waitv_storage.borrow_mut() = None;
    }

    /// Allocate `n` regular (non-scan) request ids and open their reply slots.
    /// Hold the returned [`ReplyLease`] until every reply has been collected —
    /// dropping it closes the slots.
    pub(crate) fn alloc_replies(&self, n: usize) -> ReplyLease {
        let ids: Vec<u64> = (0..n)
            .map(|_| bump_id(&self.inner.next_request_id, 1, MAX_REGULAR_REQ_ID))
            .collect();
        for &id in &ids {
            self.inner.replies.open(id, None);
        }
        ReplyLease { inner: Rc::clone(&self.inner), ids }
    }

    /// Allocate a scan request_id: the same counter as [`Self::alloc_replies`]
    /// with `SCAN_REQ_ID_FLAG` set, so `drain_w2m_for_worker` can classify the
    /// frame before decoding it.
    pub fn alloc_scan_request_id(&self) -> u64 {
        bump_id(&self.inner.next_request_id, 1, MAX_REGULAR_REQ_ID) | (SCAN_REQ_ID_FLAG as u64)
    }

    /// Future that completes at `deadline`, backed by an io_uring Timeout
    /// SQE whose CQE wakes the task.
    pub fn timer(&self, deadline: Instant) -> impl Future<Output = ()> {
        TimerFuture::new(deadline, Rc::clone(&self.inner))
    }

    /// Future for the decoded W2M reply on `req_id`, an id of a live
    /// [`ReplyLease`]. A pure poller, so it may be built and dropped freely —
    /// a `select2` that loses does not lose the reply.
    ///
    /// Concrete, not `impl Future`, so callers can hold a `Vec<ReplyFuture>`
    /// scratch buffer across reactor calls.
    pub fn await_reply(&self, req_id: u64) -> ReplyFuture {
        ReplyFuture { req_id, inner: Rc::clone(&self.inner) }
    }

    /// Return a future that resolves to the raw `W2mSlot` routed to
    /// `internal_req_id`. The scan-intercept path in `drain_w2m_for_worker`
    /// fires it ahead of flag-based routing so scan response frames are
    /// never decoded into `Batch` on the master side.
    ///
    /// Concrete, not `impl Future`, so `join_all_unpin`'s `F: Unpin` binds to
    /// the definition: behind `impl Future` a new `!Unpin` field would error at
    /// the fan-out call site instead of here.
    pub fn await_scan_slot(&self, req_id: u32) -> ScanSlotFuture {
        ScanSlotFuture { req_id, inner: Rc::clone(&self.inner) }
    }

    /// The next `FLAG_EXCHANGE` frame a worker published, as `(worker, frame)`.
    /// The reactor's job on such a reply ends at queueing it: turning a
    /// worker's frames into completed rounds is orchestration policy and lives
    /// with the relay task that consumes them.
    pub fn next_exchange(&self) -> impl Future<Output = (usize, DecodedWire)> {
        ExchangeFuture { inner: Rc::clone(&self.inner) }
    }

    /// Create a `ScanLease` that registers `ids` as active scans for its
    /// lifetime. The scan's routing state must span the whole scan
    /// operation regardless of which `.await` a cancellation lands on, so the
    /// lease must be bound to a named local held to end of scope. On drop the
    /// lease deregisters its ids and purges any waker / queued frames they
    /// left, so `route_scan_slot` discards an abandoned scan's later frames.
    pub(crate) fn scan_lease(&self, ids: &[u32]) -> ScanLease {
        ScanLease::new(Rc::clone(&self.inner), ids)
    }

    /// Attach the `W2mReceiver`. Replies published before the attach are already
    /// in the rings, so drain them here; the `FUTEX_WAITV` SQE that watches
    /// every worker's `write_cursor` is armed by the first `tick` that sleeps.
    pub fn attach_w2m(&self, w2m: Rc<W2mReceiver>) {
        let nw = w2m.num_workers();
        let futexv: Vec<FutexWaitV> = (0..nw).map(|_| FutexWaitV::new()).collect();
        if self.inner.w2m.set(w2m).is_err() {
            panic!("attach_w2m called twice");
        }
        *self.inner.futex_waitv_storage.borrow_mut() = Some(futexv.into_boxed_slice());
        self.drain_all_w2m();
    }

    /// Drain every worker's ring. A no-op before `attach_w2m`, so callers on
    /// the tick path need no guard of their own.
    fn drain_all_w2m(&self) {
        let Some(w2m) = self.inner.w2m.get() else {
            return;
        };
        for w in 0..w2m.num_workers() {
            self.drain_w2m_for_worker(w2m, w);
        }
    }

    /// Arm the persistent `FUTEX_WAITV` SQE, drained rings first.
    ///
    /// A ring with unread data cannot be armed — the SQE would wait for a wake
    /// that already happened — so this loops until `arm_waitv` reports every
    /// ring quiet. The SQE is left in the SQ for `tick`'s own
    /// `submit_and_wait_timeout` rather than flushed here; that call follows
    /// immediately, and a second `io_uring_enter` per wake buys nothing.
    pub(super) fn arm_futex_waitv(&self) {
        let Some(w2m) = self.inner.w2m.get() else {
            return;
        };
        let (ptr, n) = loop {
            self.drain_all_w2m();
            let mut storage = self.inner.futex_waitv_storage.borrow_mut();
            let Some(boxed) = storage.as_mut() else {
                return;
            };
            if let Some(armed) = w2m.arm_waitv(boxed) {
                break (armed.as_ptr(), armed.len() as u32);
            }
        };
        // SAFETY: the array lives in `futex_waitv_storage`, which is freed only
        // after `cancel_futex_waitv_and_wait` sees the SQE's CQE.
        unsafe {
            self.inner.ring().prep_futex_waitv(ptr, n, udata(KIND_FUTEX_WAITV, 0));
        }
        self.inner.futex_waitv_armed.set(true);
    }

    /// Submit an fdatasync and await its completion. Returns the CQE `res`
    /// (0 on success, negative errno on failure). The SQE is flushed to the
    /// kernel immediately so the fsync can overlap with subsequent CPU work —
    /// `commit_pushes` awaits its worker ACKs and fires the tick between the
    /// submit and the CQE, and depends on it.
    pub fn fsync(&self, fd: i32) -> FsyncFuture {
        let id = self.inner.alloc_op_id();
        {
            let mut ring = self.inner.ring();
            ring.prep_fsync(fd, udata(KIND_FSYNC, id));
            ring.flush_sqes("fsync");
        }
        self.inner.fsyncs.open(id, None);
        FsyncFuture { id, inner: Rc::clone(&self.inner) }
    }

    /// Drive the reactor forever; returns when `request_shutdown` is
    /// called. Used by the executor's main loop.
    pub fn block_until_shutdown(&self) {
        while !self.inner.shutdown.get() {
            self.tick(true);
        }
    }

    /// Drive the reactor until the task slab is empty.
    ///
    /// Drain every unread W2M slot for worker `w` and route each reply:
    /// scan frames are intercepted raw; everything else — data, exchange,
    /// control-only — funnels through one owned decode into `route_reply`,
    /// which peels FLAG_EXCHANGE off into the exchange queue.
    ///
    /// The RAII `W2mSlot` advances `release_cursor` on drop so the worker
    /// can reuse the ring space immediately after decoding completes.
    fn drain_w2m_for_worker(&self, w2m: &W2mReceiver, w: usize) {
        while let Some(slot) = w2m.try_read_slot(w) {
            // Scan-slot intercept on the raw ring prefix — see
            // `SCAN_REQ_ID_FLAG`: no decode, no `Batch`, no hash probe.
            if slot.internal_req_id & SCAN_REQ_ID_FLAG != 0 {
                self.route_scan_slot(slot);
                continue;
            }
            let prefix = slot.internal_req_id;
            let decoded = slot.decode(w);
            // RAII: advance release_cursor before waking the awaiter.
            drop(slot);
            self.route_reply(w, prefix, decoded);
        }
    }

    pub(crate) fn route_scan_slot(&self, slot: W2mSlot) {
        let mut scans = self.inner.scans.borrow_mut();
        let Some(route) = scans.get_mut(&slot.internal_req_id) else {
            // Abandoned scan (no live ScanLease): dropping `slot` advances
            // release_cursor so the still-streaming worker never wedges on
            // a full ring.
            return; // slot dropped here
        };
        route.push(slot);
    }

    /// Park `decoded` for its awaiter.
    ///
    /// A `FLAG_EXCHANGE` frame rides an in-flight tick id, so it is peeled off
    /// into the exchange queue before that id's awaiter can complete. What a
    /// round is belongs to the task draining that queue.
    ///
    /// Routes on `prefix` — the ring slot's `internal_req_id` — before anything
    /// decodes the frame, which is what drops an abandoned scan's continuations
    /// at the ring boundary. The payload's `request_id` is the same identity at
    /// full width; the train producers omit it and go to `route_scan_slot`.
    ///
    /// Unrouted replies are logged and dropped.
    fn route_reply(&self, w: usize, prefix: u32, decoded: DecodedWire) {
        if decoded.control.flags & FLAG_EXCHANGE != 0 {
            self.inner.exchanges.borrow_mut().push((w, decoded));
            return;
        }

        debug_assert_eq!(
            prefix as u64, decoded.control.request_id,
            "W2M reply prefix disagrees with its payload request_id"
        );
        let req_id = prefix as u64;
        if !self.inner.replies.complete(req_id, decoded) {
            gnitz_warn!("reactor: unrouted W2M reply worker={} req_id={}", w, req_id,);
        }
    }

    /// The global inbound-memory budget, shared with every `RecvQueue` and
    /// every `RecvBuf` it charges.
    pub(crate) fn inbound(&self) -> &Rc<io::InboundBudget> {
        &self.inner.inbound
    }
}

#[cfg(test)]
#[path = "tests/reactor.rs"]
mod tests;
