//! Single-threaded io_uring reactor.
//!
//! Provides `block_on`, `spawn`, `timer`, and a reply-routing API
//! (`await_reply`). The reactor owns its own `IoUringRing`, separate
//! from the executor's transport ring.
//!
//! Design notes:
//!
//! - Run queue is a thread-local-reachable `RefCell<RunQueue>`. The
//!   `Waker` vtable requires `Send + Sync`, but the reactor is strictly
//!   single-threaded, so the waker stores only the task key as its
//!   `*const ()` data pointer and reaches the queue via a thread-local
//!   raw pointer set in `Reactor::new`. This eliminates the per-wake
//!   `Arc::fetch_add/sub` and `Mutex::lock` round-trips that an
//!   `Arc<Mutex<VecDeque>>` design would impose.
//! - Everything that awaits a single completion — timer, W2M reply, fsync,
//!   send, raw recv — parks in a [`park::ParkMap`]. An entry exists exactly
//!   while its op is outstanding, so a late CQE can tell "deliver this" from
//!   "the awaiter is gone" without a per-family tombstone set.
//! - CQE `user_data` packs an 8-bit kind tag in the high byte and a
//!   56-bit id in the low bits, where id is a request/op id (not an fd,
//!   except for accept and recv, which route on the fd itself). Safe from
//!   collisions because the reactor owns its own ring.

use std::cell::{Cell, OnceCell, RefCell};
use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::ptr;
use std::rc::Rc;
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};
use std::time::Instant;

use rustc_hash::{FxHashMap, FxHashSet};

use self::uring::{Cqe, IoUringRing, CQE_F_MORE};

use crate::runtime::posix::FUTEX2_SIZE_U32;
use crate::runtime::sal::MAX_WORKERS;
use crate::runtime::w2m::{W2mReceiver, W2mSlot};
use crate::runtime::wire::{self, DecodedWire, FLAG_EXCHANGE};

/// High bit of `internal_req_id` (u32) marks scan-allocated request IDs.
/// Regular IDs stay in [1, MAX_REGULAR_REQ_ID] (bit 31 clear).
/// Scan IDs stay in [SCAN_REQ_ID_BASE, 0xFFFFFFFE] (bit 31 set).
/// Detection in `drain_w2m_for_worker` is a single bitwise AND, so a scan
/// frame is intercepted before it is decoded.
const SCAN_REQ_ID_FLAG: u32 = 1 << 31;
const MAX_REGULAR_REQ_ID: u64 = (SCAN_REQ_ID_FLAG - 1) as u64; // 0x7FFFFFFF
const SCAN_REQ_ID_BASE: u32 = SCAN_REQ_ID_FLAG | 1; // 0x80000001

use io_uring::types::FutexWaitV;

mod conn;
mod exchange;
mod futures;
pub mod io;
mod park;
mod runloop;
pub mod sync;
mod uring;

#[cfg(test)]
use conn::client_send_timeout;
pub(crate) use conn::guard_client_egress;

pub(crate) use futures::{FsyncFuture, PeerToken, ReplyFuture, ScanLease};
use futures::{ScanRoute, ScanSlotFuture, SendCarry, TimerFuture};
use park::ParkMap;

pub use exchange::{
    ExchangeAccumulator, PendingRelay, BACKFILL_DECISION_CHECKPOINT, BACKFILL_DECISION_CONTINUE,
    BACKFILL_DECISION_STOP, BACKFILL_PAD_BIT,
};
pub use io::RecvBuf;
pub use sync::{
    join_all_unpin, join_into, mpsc, oneshot, select2, AsyncMutex, AsyncRwLock, Either, ReadGuard, WriteGuard,
};

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

/// Backing store for one async task.
struct Task {
    future: Pin<Box<dyn Future<Output = ()>>>,
}

struct ReactorShared {
    /// SAFETY INVARIANT: `ring` MUST be the first declared field. Rust drops
    /// fields in declaration order; dropping `ring` closes the io_uring fd,
    /// whose kernel-side teardown cancels all in-flight SQEs and drops the
    /// kernel's references to userspace buffers. Only then is it safe to free
    /// the buffers those SQEs pointed at — the `sends` / `raw_recvs` park
    /// slots and `conns`. Moving `ring` below any of them is a use-after-free
    /// at shutdown. `raw_recvs` must additionally sit BELOW `tasks`: a
    /// `RawRecvFuture::Drop` running during `tasks`'s own field-drop touches
    /// it, so it must still be alive then.
    ring: RefCell<IoUringRing>,
    /// Live tasks keyed by a monotonically-increasing id. HashMap (not
    /// `slab::Slab`) because same-key reinsertion is load-bearing: a
    /// task's future may spawn new tasks during its poll, and we must
    /// reinsert the running task at its original key so the waker hits
    /// the right entry on the next wake.
    tasks: RefCell<FxHashMap<usize, Task>>,
    next_task_key: Cell<usize>,
    /// Tasks whose wakers fired; the reactor's main loop polls them on
    /// the next tick. The waker vtable reaches this through a
    /// thread-local raw pointer (`REACTOR_RUN_QUEUE`) — see `waker_wake`.
    /// Dedup in `RunQueue::push` collapses N wakes for the same task
    /// into a single poll per tick.
    run_queue: RefCell<RunQueue>,
    /// Scratch buffer `tick` swaps the run queue into, so wakes issued during a
    /// poll schedule for the next tick instead of re-entering this one. Kept
    /// here (rather than rebuilt per tick) to reuse the one allocation.
    tick_scratch: Cell<Vec<usize>>,
    /// W2M replies keyed by request_id; opened by `await_reply`, delivered by
    /// `route_reply`.
    replies: ParkMap<DecodedWire>,
    /// In-flight fdatasyncs; the CQE result is the fdatasync return code.
    fsyncs: ParkMap<i32>,
    /// Pointer-stable storage for the reactor's persistent
    /// `FUTEX_WAITV` SQE. The kernel dereferences this array
    /// asynchronously, so it must outlive the SQE. A single SQE covers
    /// every worker's `reader_seq` word; we own it on the heap, and
    /// tear it down only after cancelling the SQE in
    /// `request_shutdown` and awaiting the `-ECANCELED` CQE.
    futex_waitv_storage: RefCell<Option<Box<[FutexWaitV]>>>,
    /// True while an outstanding `FUTEX_WAITV` SQE exists whose
    /// `FutexWaitV` array lives in `futex_waitv_storage`. Cleared when that
    /// SQE's CQE is drained, which is also when the kernel releases its
    /// reference to the array — so this false is what makes freeing the
    /// storage safe.
    futex_waitv_armed: Cell<bool>,
    /// In-flight io_uring Timeout ops. A `TimerFuture` dropped before the CQE
    /// abandons its slot, so the late CQE has nothing to wake.
    timers: ParkMap<()>,
    /// W2M request ids the workers echo back. Kept separate from `next_op_id`
    /// because the protocol constrains their range (see `alloc_request_id`).
    next_request_id: Cell<u64>,
    next_scan_req_id: Cell<u32>,
    /// Ids for purely local kernel ops (timer / fsync / send / raw recv). They
    /// share a counter because each family has its own park map and its own
    /// `KIND_*` tag, so equal ids never collide.
    next_op_id: Cell<u64>,
    /// Per-fd connection state: recv decoder, delivery queue and send
    /// accounting. Boxed so the inline hdr buffer address survives HashMap
    /// resizes — io_uring SQEs capture the pointer.
    conns: RefCell<FxHashMap<i32, Box<io::Conn>>>,
    /// The OOM guard shared by every connection; its ceiling is resolved once
    /// at startup by `resolve_inbound_cap`. See [`io::InboundBudget`].
    inbound: Rc<io::InboundBudget>,
    /// Accept queue: `(conn_fd, listener_fd)` pairs delivered by the kernel
    /// but not yet claimed by an `accept().await` caller. The listener fd
    /// rides the multishot-accept SQE's udata `id` field, so the accept
    /// loop can route AF_UNIX vs TLS connections without reactor state.
    accept_queue: RefCell<VecDeque<(i32, i32)>>,
    accept_waker: RefCell<Option<Waker>>,
    /// In-flight client sends. The slot carries the buffer keep-alive and the
    /// target fd, so the CQE handler settles a send whose awaiter has already
    /// been dropped without any side table.
    sends: ParkMap<i32, SendCarry>,
    /// In-flight raw recvs (`Reactor::recv_raw`). The slot carries the caller's
    /// buffer, so a late kernel write always lands in live memory. Declared
    /// below `ring` AND below `tasks` (see the drop-order SAFETY INVARIANT on
    /// `ring`): a `RawRecvFuture::Drop` firing during `tasks`'s field-drop must
    /// still find it alive.
    raw_recvs: ParkMap<i32, Vec<u8>>,
    /// Shutdown flag. `block_until_shutdown` polls until this is set.
    shutdown: Cell<bool>,
    /// FLAG_EXCHANGE accumulator: when route_reply sees an exchange wire,
    /// it feeds it here. Once the accumulator has heard from every
    /// worker for a view_id, it produces a PendingRelay and the reactor
    /// dispatches it to the relay task via `relay_tx`.
    exchange_acc: RefCell<ExchangeAccumulator>,
    relay_tx: RefCell<Option<mpsc::Sender<PendingRelay>>>,
    /// Scan routing state keyed by `internal_req_id`, one entry per live
    /// `ScanLease`-held id (see [`ScanRoute`]).
    scans: RefCell<FxHashMap<u32, ScanRoute>>,
    /// Fds that have been marked closing via `close_fd`. `reap_closing_conns`
    /// iterates only this set (O(closing)) rather than all connections (O(all)).
    closing_fds: RefCell<FxHashSet<i32>>,
    /// Listeners whose multishot accept cancelled on fd exhaustion and await
    /// the backoff re-arm. A set, not a single flag: fd exhaustion is
    /// global, so BOTH listeners' accepts can cancel in the same window —
    /// with one flag the second CQE would be dropped and that listener
    /// would stay permanently deaf. One 50 ms backoff task runs while the
    /// set is non-empty (spawned on the 0→1 transition) and drains it,
    /// re-arming every listed listener.
    accept_rearm_pending: RefCell<FxHashSet<i32>>,
    /// A `W2mSlot` holds a raw `*mut InFlightState` into this `W2mReceiver` and
    /// calls `release()` through it on drop. Slots outlive their originating
    /// stack frame in `scans` (queued continuation frames) and in a `sends`
    /// slot's keep-alive, so the receiver must outlive both. The
    /// `MasterDispatcher` holds the other `Rc` and outlives the reactor, which
    /// keeps the allocation alive here regardless of field order.
    w2m: OnceCell<Rc<W2mReceiver>>,
}

impl ReactorShared {
    /// Next id for a local kernel op (timer / fsync / send / raw recv). Wraps
    /// within `ID_MASK` so packing it into a CQE's `user_data` is lossless.
    fn alloc_op_id(&self) -> u64 {
        bump_id(&self.next_op_id, 1, ID_MASK)
    }

    fn num_workers(&self) -> usize {
        self.w2m.get().expect("w2m not attached").num_workers()
    }
}

/// Shared, clonable handle to the reactor. All futures created by the
/// reactor capture an `Rc<ReactorShared>` so they can submit ops and
/// register wakers without borrowing the `Reactor` mutably.
pub struct Reactor {
    inner: Rc<ReactorShared>,
}

/// Submit a `FUTEX_WAITV` SQE with a deliberately mismatched expected
/// value (returns -EAGAIN immediately when the opcode is supported).
/// Aborts if the kernel returns -EINVAL or -ENOSYS — those signal
/// that the opcode isn't available, and the W2M master-wait path
/// would silently fail to deliver CQEs. Linux 6.7+ required.
///
/// Memoised: the probe result is process-lifetime invariant (the
/// kernel does not grow or lose io_uring opcodes mid-run), so the
/// full probe io_uring cycle only runs on the first `Reactor::new`.
/// This matters for the test suite, which creates many reactors.
fn probe_futex_waitv_support() {
    use std::sync::atomic::AtomicU32;
    use std::sync::Once;

    static PROBED: Once = Once::new();
    PROBED.call_once(|| {
        let atomic = Box::new(AtomicU32::new(42));
        let futexv: Box<[FutexWaitV; 1]> = Box::new([FutexWaitV::new()
            .val(0)
            .uaddr(&*atomic as *const AtomicU32 as u64)
            .flags(FUTEX2_SIZE_U32)]);

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

/// Resolve the global inbound-memory ceiling once at startup.
///
/// `GNITZ_INBOUND_MEM_BYTES` is an operator override (floored so it can never
/// bar a single max-size frame). Otherwise the default is a quarter of the
/// process memory budget — cgroup `memory.max` if set, else physical RAM —
/// clamped to `[INBOUND_CAP_FLOOR, INBOUND_CAP_CEIL]`. A fraction of the
/// *actual* budget scales with the deployment; a flat constant would OOM a
/// mid-size box yet never trip inside a small container.
fn resolve_inbound_cap() -> usize {
    let default =
        (crate::runtime::posix::available_memory_bytes() / 4).clamp(io::INBOUND_CAP_FLOOR, io::INBOUND_CAP_CEIL);
    // The operator override wins, floored so it can never bar a single
    // max-size frame (the default is already within the floor).
    gnitz_engine::foundation::env::env_num("GNITZ_INBOUND_MEM_BYTES", default).max(io::INBOUND_CAP_FLOOR)
}

impl Reactor {
    pub fn new(ring_capacity: u32) -> std::io::Result<Self> {
        probe_futex_waitv_support();
        let ring = IoUringRing::new(ring_capacity)?;
        let inner = Rc::new(ReactorShared {
            ring: RefCell::new(ring),
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
            next_request_id: Cell::new(1),
            next_scan_req_id: Cell::new(SCAN_REQ_ID_BASE),
            next_op_id: Cell::new(1),
            conns: RefCell::new(FxHashMap::default()),
            inbound: Rc::new(io::InboundBudget::new(resolve_inbound_cap())),
            accept_queue: RefCell::new(VecDeque::new()),
            accept_waker: RefCell::new(None),
            sends: ParkMap::default(),
            raw_recvs: ParkMap::default(),
            shutdown: Cell::new(false),
            exchange_acc: RefCell::new(ExchangeAccumulator::new(0)),
            relay_tx: RefCell::new(None),
            closing_fds: RefCell::new(FxHashSet::default()),
            scans: RefCell::new(FxHashMap::default()),
            accept_rearm_pending: RefCell::new(FxHashSet::default()),
        });
        // Publish the run-queue pointer for the waker vtable. ReactorShared
        // lives behind Rc with a stable address, so the pointer is valid
        // for the reactor's lifetime; `Drop for Reactor` clears it.
        REACTOR_RUN_QUEUE.with(|p| {
            p.set(&inner.run_queue as *const RefCell<RunQueue>);
        });
        Ok(Reactor { inner })
    }

    /// Wire the relay channel sender into the reactor.  Called from the
    /// executor before spawning the tick + relay tasks.  When
    /// `route_reply` sees the accumulator complete a view, it sends the
    /// `PendingRelay` here for the relay task to write back to SAL.
    pub fn attach_relay_tx(&self, tx: mpsc::Sender<PendingRelay>) {
        *self.inner.relay_tx.borrow_mut() = Some(tx);
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
    fn cancel_futex_waitv_and_wait(&self) {
        if !self.inner.futex_waitv_armed.get() {
            return;
        }
        {
            let mut ring = self.inner.ring.borrow_mut();
            ring.prep_async_cancel(udata(KIND_FUTEX_WAITV, 0), udata(KIND_CANCEL_SINK, 0));
            let _ = ring.submit_and_wait_timeout(0, 0);
        }
        let deadline = Instant::now() + std::time::Duration::from_millis(2000);
        while self.inner.futex_waitv_armed.get() && Instant::now() < deadline {
            self.drain_cqes_into_wakers();
            if !self.inner.futex_waitv_armed.get() {
                break;
            }
            let _ = self.inner.ring.borrow_mut().submit_and_wait_timeout(1, 100);
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

    /// Allocate a regular (non-scan) request_id. Bit 31 is always clear,
    /// so `drain_w2m_for_worker` can distinguish these from scan IDs by
    /// a single bitwise AND without a HashMap lookup.
    pub fn alloc_request_id(&self) -> u64 {
        bump_id(&self.inner.next_request_id, 1, MAX_REGULAR_REQ_ID)
    }

    /// Allocate a scan request_id (bit 31 set). The hot-path check in
    /// `drain_w2m_for_worker` is `internal_req_id & SCAN_REQ_ID_FLAG != 0`
    /// — a single AND with no HashMap borrow on every push/seek ack.
    pub fn alloc_scan_request_id(&self) -> u64 {
        let id = self.inner.next_scan_req_id.get();
        let next = match id.checked_add(1) {
            Some(n) if n != 0 => n, // wraps only at u32 overflow
            _ => SCAN_REQ_ID_BASE,
        };
        self.inner.next_scan_req_id.set(next);
        id as u64
    }

    /// Future that completes at `deadline`, backed by an io_uring Timeout
    /// SQE whose CQE wakes the task.
    pub fn timer(&self, deadline: Instant) -> impl Future<Output = ()> {
        TimerFuture::new(deadline, Rc::clone(&self.inner))
    }

    /// Future that resolves to the decoded W2M reply for `req_id`.
    /// Returns the concrete `ReplyFuture` so callers can declare a
    /// `Vec<ReplyFuture>` scratch buffer that lives across reactor calls.
    ///
    /// The reply becomes routable here, not at `alloc_request_id`: a reply that
    /// arrives before this call has nowhere to land and is dropped with a
    /// warning, so callers must build their futures before awaiting anything.
    pub fn await_reply(&self, req_id: u64) -> ReplyFuture {
        self.inner.replies.open(req_id, None);
        ReplyFuture {
            req_id,
            inner: Rc::clone(&self.inner),
        }
    }

    /// Return a future that resolves to the raw `W2mSlot` routed to
    /// `internal_req_id`. The scan-intercept path in `drain_w2m_for_worker`
    /// fires it ahead of flag-based routing so scan response frames are
    /// never decoded into `Batch` on the master side.
    pub fn await_scan_slot(&self, req_id: u32) -> impl Future<Output = W2mSlot> {
        ScanSlotFuture {
            req_id,
            inner: Rc::clone(&self.inner),
        }
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

    /// Attach the `W2mReceiver` and arm a persistent `FUTEX_WAITV` SQE
    /// that watches every worker's `reader_seq` word. On each CQE, the
    /// reactor drains all rings (the wake index is not authoritative
    /// for FutexWaitV), rebuilds the expected-values array, and
    /// re-arms.
    pub fn attach_w2m(&self, w2m: Rc<W2mReceiver>) {
        let nw = w2m.num_workers();
        *self.inner.exchange_acc.borrow_mut() = ExchangeAccumulator::new(nw);
        // Allocate a zeroed boxed slice — `refresh_futex_waitv_vals`
        // fills the entries after MASTER_PARKED is published.
        let futexv: Vec<FutexWaitV> = (0..nw).map(|_| FutexWaitV::new()).collect();
        let boxed: Box<[FutexWaitV]> = futexv.into_boxed_slice();
        if self.inner.w2m.set(w2m).is_err() {
            panic!("attach_w2m called twice");
        }
        *self.inner.futex_waitv_storage.borrow_mut() = Some(boxed);
        self.drain_refresh_and_arm();
    }

    /// Drain every worker's ring.
    fn drain_all_w2m(&self) {
        for w in 0..self.inner.num_workers() {
            self.drain_w2m_for_worker(w);
        }
    }

    /// The lost-wake protocol in one place: drain every ring, refresh the
    /// expected `reader_seq` values (with MASTER_PARKED published first so
    /// the wake window stays closed), loop until no ring has unread data,
    /// then re-arm the FUTEX_WAITV SQE. Arming with a stale expected value
    /// would block on a wake that already happened — the classic lost-wake
    /// race. Used by `attach_w2m` (catches messages published between init
    /// and attach) and by the KIND_FUTEX_WAITV CQE handler on every wake.
    fn drain_refresh_and_arm(&self) {
        loop {
            self.drain_all_w2m();
            if !self.refresh_futex_waitv_vals() {
                break;
            }
        }
        // (Re-)submit the SQE against the heap-owned `FutexWaitV` array.
        let storage = self.inner.futex_waitv_storage.borrow();
        let Some(boxed) = storage.as_ref() else {
            return;
        };
        {
            let mut ring = self.inner.ring.borrow_mut();
            unsafe {
                ring.prep_futex_waitv(boxed.as_ptr(), boxed.len() as u32, udata(KIND_FUTEX_WAITV, 0));
            }
            ring.flush_sqes("FUTEX_WAITV");
        }
        self.inner.futex_waitv_armed.set(true);
    }

    /// Arm the master-park protocol on every ring and snapshot each
    /// `reader_seq` into its `FutexWaitV` entry. Returns `true` if any ring has
    /// unread data, meaning the caller must drain before arming — otherwise the
    /// SQE would block waiting for a wake that already happened.
    ///
    /// `W2mRingHeader::arm_master_park` owns the store ordering this depends on.
    fn refresh_futex_waitv_vals(&self) -> bool {
        let mut storage = self.inner.futex_waitv_storage.borrow_mut();
        let Some(boxed) = storage.as_mut() else {
            return false;
        };
        let Some(w2m) = self.inner.w2m.get() else {
            return false;
        };
        let mut pending = false;
        for (w, entry) in boxed.iter_mut().enumerate() {
            let hdr = unsafe { w2m.header(w) };
            let (expected, has_unread) = hdr.arm_master_park();
            *entry = FutexWaitV::new()
                .val(expected as u64)
                .uaddr(hdr.reader_seq() as *const std::sync::atomic::AtomicU32 as u64)
                .flags(FUTEX2_SIZE_U32);
            pending |= has_unread;
        }
        pending
    }

    /// Submit an fdatasync and await its completion. Returns the CQE `res`
    /// (0 on success, negative errno on failure). The SQE is flushed to the
    /// kernel immediately so the fsync can overlap with subsequent CPU work —
    /// the `pre_write_pushes` Phase-A / tick-evaluation overlap depends on it.
    pub fn fsync(&self, fd: i32) -> FsyncFuture {
        let id = self.inner.alloc_op_id();
        {
            let mut ring = self.inner.ring.borrow_mut();
            ring.prep_fsync(fd, udata(KIND_FSYNC, id));
            ring.flush_sqes("fsync");
        }
        self.inner.fsyncs.open(id, None);
        FsyncFuture {
            id,
            inner: Rc::clone(&self.inner),
        }
    }

    /// Drive the reactor forever; returns when `request_shutdown` is
    /// called. Used by the executor's main loop.
    pub fn block_until_shutdown(&self) {
        while !self.inner.shutdown.get() {
            self.tick(true);
        }
    }

    /// Drive the reactor until the task slab is empty. Blocks.
    #[cfg(test)]
    fn block_until_idle(&self) {
        while !self.inner.tasks.borrow().is_empty() {
            self.tick(true);
        }
    }

    /// True while at least one task is alive in the slab.
    #[cfg(test)]
    fn has_pending_tasks(&self) -> bool {
        !self.inner.tasks.borrow().is_empty()
    }

    fn dispatch_cqe(&self, cqe: Cqe) {
        let kind = udata_kind(cqe.user_data);
        let id = udata_id(cqe.user_data);
        match kind {
            KIND_TIMEOUT => {
                // The kernel is done with the Timeout's Timespec once its CQE
                // has been drained — recycle it. A cancelled timer's slot is
                // already abandoned, so the CQE is a no-op wake.
                self.inner.ring.borrow_mut().release_timer_spec(cqe.user_data);
                self.inner.timers.complete(id, ());
            }
            KIND_FUTEX_WAITV => {
                // Wake index is not authoritative for FUTEX_WAITV: the
                // kernel may wake us for any of the watched words, so
                // `drain_refresh_and_arm` drains every worker's ring
                // before re-arming.
                self.inner.futex_waitv_armed.set(false);
                if !self.inner.shutdown.get() {
                    self.drain_refresh_and_arm();
                }
            }
            KIND_FSYNC => {
                self.inner.fsyncs.complete(id, cqe.res);
            }
            KIND_ACCEPT => self.handle_accept_cqe(id as i32, cqe.res, cqe.flags),
            KIND_RECV => self.handle_recv_cqe(id as i32, cqe.res),
            KIND_SEND => {
                // The kernel is done with the buffer regardless of whether the
                // awaiter is still alive, so the fd's in-flight count drops
                // either way; `complete` then frees or parks the slot.
                self.inner.sends.with_carry(id, |&(fd, _)| {
                    if let Some(conn) = self.inner.conns.borrow_mut().get_mut(&fd) {
                        conn.send_inflight = conn.send_inflight.saturating_sub(1);
                    }
                });
                self.inner.sends.complete(id, cqe.res);
            }
            KIND_RAW_RECV => {
                self.inner.raw_recvs.complete(id, cqe.res);
            }
            KIND_CANCEL_SINK => {
                // An AsyncCancel's own CQE. The cancellation's *effect* arrives
                // separately as the target op's -ECANCELED under its own kind.
            }
            _ => {}
        }
    }

    /// Drain every unread W2M slot for worker `w` and route each reply:
    /// scan frames are intercepted raw; everything else — data, exchange,
    /// control-only — funnels through one zero-copy decode into
    /// `route_reply`, which demuxes FLAG_EXCHANGE into the accumulator.
    ///
    /// The RAII `W2mSlot` advances `consume_cursor` on drop so the worker
    /// can reuse the ring space immediately after decoding completes.
    fn drain_w2m_for_worker(&self, w: usize) {
        let w2m = self
            .inner
            .w2m
            .get()
            .expect("drain_w2m_for_worker called before attach_w2m");
        while let Some(slot) = w2m.try_read_slot(w) {
            // Scan-slot intercept: bit 31 of internal_req_id is set for
            // scan-allocated IDs (alloc_scan_request_id). Single AND, no
            // HashMap borrow — keeps the push/seek hot path overhead zero.
            if slot.internal_req_id & SCAN_REQ_ID_FLAG != 0 {
                self.route_scan_slot(slot);
                continue;
            }
            let ctrl = wire::peek_control_block_ipc(slot.bytes()).expect("W2M control block corrupt — ring corrupt");
            let prefix = slot.internal_req_id;
            let decoded = self.decode_slot_owned(slot, ctrl);
            self.route_reply(w, prefix, decoded);
        }
    }

    fn route_scan_slot(&self, slot: W2mSlot) {
        let waker = {
            let mut scans = self.inner.scans.borrow_mut();
            let Some(route) = scans.get_mut(&slot.internal_req_id) else {
                // Abandoned scan (no live ScanLease): dropping `slot` advances
                // consume_cursor so the still-streaming worker never wedges on
                // a full ring.
                return; // slot dropped here
            };
            route.queue.push_back(slot);
            route.waker.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    /// Decode one W2M slot into an owned `DecodedWire`, tolerating
    /// control-only frames (`data_batch: None`). Zero-copy: borrows ring
    /// bytes via `MemBatch`, allocates exactly one owned `Batch` when the
    /// frame carries data (ring → run, single `copy_from_slice`), then
    /// drops the slot so `consume_cursor` advances before any awaiter wakes.
    fn decode_slot_owned(&self, slot: W2mSlot, ctrl: wire::DecodedControl) -> DecodedWire {
        let bytes = slot.bytes();
        let mut offsets = [0usize; gnitz_engine::storage::MAX_BATCH_REGIONS];
        let zc = wire::decode_wire_ipc_zero_copy_with_ctrl(bytes, ctrl, None, &mut offsets)
            .expect("W2M zero-copy decode failed — ring corrupt");
        let flags = zc.control.flags;
        let control = zc.control;
        let schema = zc.schema;
        let data_batch = zc.data_batch.map(|mb| {
            let sch = schema.as_ref().expect("FLAG_HAS_DATA set but no schema — ring corrupt");
            let mut owned = gnitz_engine::storage::Batch::with_capacity(*sch, mb.count);
            owned.append_mem_batch(&mb);
            // The wire flags are ground truth. `append_mem_batch` leaves
            // `owned` `Raw`; raise to the decoded claim, debug-verifying the data.
            owned.certify_layout(wire::layout_from_wire_flags(flags), sch);
            owned
        });
        drop(slot); // RAII: advance consume_cursor before waking awaiter
        DecodedWire {
            control,
            schema,
            data_batch,
        }
    }

    /// Park `decoded` for its awaiter. FLAG_EXCHANGE replies are
    /// demuxed into `exchange_acc` so the tick req_id's waker stays
    /// parked until the final (non-FLAG_EXCHANGE) ACK lands. When an
    /// exchange round completes, the resulting `PendingRelay` is dispatched on
    /// `relay_tx` — the write itself happens in `relay_loop`, which can take the
    /// catalog read lock and `sal_writer_excl`; this handler cannot.
    ///
    /// Routes on `prefix` — the ring slot's `internal_req_id` — which is the
    /// key `send_msg` documents as the reply's identity. The payload's
    /// `request_id` agrees with it for every reply that reaches here, but a
    /// producer is only obliged to set the prefix: the chunked-train frames
    /// leave the payload field 0, and they stay correct here if a future reply
    /// helper is built from one of them.
    ///
    /// Unrouted replies are logged and dropped.
    fn route_reply(&self, w: usize, prefix: u32, decoded: DecodedWire) {
        if decoded.control.flags & FLAG_EXCHANGE != 0 {
            let pending = self.inner.exchange_acc.borrow_mut().process(w, decoded);
            if let Some(relay) = pending {
                if let Some(tx) = self.inner.relay_tx.borrow().as_ref() {
                    tx.send(relay);
                } else {
                    gnitz_warn!(
                        "reactor: FLAG_EXCHANGE relay produced before relay_tx attached (view_id={})",
                        relay.view_id,
                    );
                }
            }
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

    /// Test-only: dispatch a synthetic CQE tagged with `kind` and `id`, with
    /// `rc` as the CQE `res`.
    #[cfg(test)]
    fn inject_cqe(&self, kind: u64, id: u64, rc: i32) {
        self.dispatch_cqe(Cqe {
            user_data: udata(kind, id),
            res: rc,
            flags: 0,
        });
    }

    /// Test-only: size the exchange accumulator so `route_reply` can
    /// be driven directly.
    #[cfg(test)]
    fn test_init_state(&self, num_workers: usize) {
        *self.inner.exchange_acc.borrow_mut() = ExchangeAccumulator::new(num_workers);
    }

    /// Test-only: drive `route_reply` with a synthetic decoded wire, standing in
    /// for the ring prefix the way every real producer sets it — equal to the
    /// payload's `request_id`.
    #[cfg(test)]
    fn test_route_reply(&self, w: usize, decoded: DecodedWire) {
        let prefix = decoded.control.request_id as u32;
        self.route_reply(w, prefix, decoded)
    }

    /// Test-only: drive `route_scan_slot` with a real slot.
    #[cfg(test)]
    pub(super) fn test_route_scan_slot(&self, slot: W2mSlot) {
        self.route_scan_slot(slot);
    }

    #[cfg(test)]
    fn task_count(&self) -> usize {
        self.inner.tasks.borrow().len()
    }

    /// The global inbound-memory budget, shared with every `RecvQueue` and
    /// every `RecvBuf` it charges.
    pub(crate) fn inbound(&self) -> &Rc<io::InboundBudget> {
        &self.inner.inbound
    }
}

// ---------------------------------------------------------------------------
// Run queue + waker vtable
// ---------------------------------------------------------------------------

/// Single-threaded run queue with in-flight deduplication.
///
/// `push` is O(scan) over the current contents; in practice the queue
/// holds ≤ 16 entries (peak in-flight wake count for normal workloads),
/// so the scan fits in one or two cache lines and auto-vectorizes — much
/// cheaper than maintaining a parallel `FxHashSet`.
///
/// `Vec` (not `VecDeque`) so `swap_into` can exchange backing storage in
/// O(1) (three pointer-width words) instead of copying N elements.
struct RunQueue {
    queue: Vec<usize>,
}

impl RunQueue {
    fn new() -> Self {
        Self {
            queue: Vec::with_capacity(16),
        }
    }

    /// Enqueue `key` unless it is already pending. Idempotent: N wakes
    /// for the same task before the next tick collapse to one poll.
    fn push(&mut self, key: usize) {
        if !self.queue.contains(&key) {
            self.queue.push(key);
        }
    }

    fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    /// O(1) swap of the backing allocation into `out`.
    fn swap_into(&mut self, out: &mut Vec<usize>) {
        std::mem::swap(out, &mut self.queue);
    }
}

thread_local! {
    /// Points to the `RefCell<RunQueue>` owned by the reactor running on
    /// this thread. Set in `Reactor::new`, cleared in `Reactor::drop`.
    /// The waker vtable reads this to schedule wakes without per-wake
    /// `Arc` traffic. Invariant: at most one reactor exists per thread
    /// at any time (so this pointer is never overwritten while live).
    static REACTOR_RUN_QUEUE: Cell<*const RefCell<RunQueue>> =
        const { Cell::new(ptr::null()) };
}

impl Drop for Reactor {
    fn drop(&mut self) {
        // Clear before `ReactorShared` (and any pending tasks/futures) is
        // freed: drop chains in `sync.rs` may call `Waker::wake`, and
        // those calls must observe a null pointer rather than a dangling
        // one. Wakes after this point are silent no-ops.
        REACTOR_RUN_QUEUE.with(|p| p.set(ptr::null()));
    }
}

unsafe fn waker_clone(data: *const ()) -> RawWaker {
    RawWaker::new(data, &WAKER_VTABLE)
}

unsafe fn waker_wake(data: *const ()) {
    let key = data as usize;
    REACTOR_RUN_QUEUE.with(|p| {
        let ptr = p.get();
        if ptr.is_null() {
            // Reactor torn down; the wake has nowhere to land. This is
            // reached when `sync.rs` Drop chains (oneshot / mpsc /
            // AsyncMutexGuard / WriteGuard) fire `waker.wake()` while
            // the reactor is being dropped. Silent no-op.
            return;
        }
        // SAFETY: invariant: the reactor that published this pointer is
        // still alive (cleared in `Drop for Reactor`). RefCell enforces
        // borrow rules, so a double-borrow panics rather than UB.
        unsafe {
            (*ptr).borrow_mut().push(key);
        }
    });
}

unsafe fn waker_wake_by_ref(data: *const ()) {
    unsafe {
        waker_wake(data);
    }
}

unsafe fn waker_drop(_data: *const ()) {}

const WAKER_VTABLE: RawWakerVTable = RawWakerVTable::new(waker_clone, waker_wake, waker_wake_by_ref, waker_drop);

fn make_waker(key: usize) -> Waker {
    let raw = RawWaker::new(key as *const (), &WAKER_VTABLE);
    unsafe { Waker::from_raw(raw) }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::futures::{SendAlive, SendFuture};
    use super::*;
    use crate::runtime::w2m_ring::park_order;
    use gnitz_engine::schema::SchemaDescriptor;
    use std::cell::Cell as StdCell;
    use std::time::Duration;

    fn make_reactor() -> Reactor {
        Reactor::new(16).expect("reactor")
    }

    /// `block_on` returns a value from a trivial async fn.
    #[test]
    fn block_on_trivial() {
        let r = make_reactor();
        let v = r.block_on(async { 42u32 });
        assert_eq!(v, 42);
    }

    /// `block_on` with a future that yields once via `pending_then_ready`.
    #[test]
    fn block_on_yields_then_completes() {
        let r = make_reactor();
        let v = r.block_on(async {
            // Two-poll await: yield once, then complete.
            YieldOnce::new().await;
            7u32
        });
        assert_eq!(v, 7);
    }

    /// Spawned task drives a counter to 1.
    #[test]
    fn spawn_runs_to_completion() {
        let r = make_reactor();
        let counter: Rc<StdCell<u32>> = Rc::new(StdCell::new(0));
        let c2 = Rc::clone(&counter);
        r.spawn(async move {
            c2.set(c2.get() + 1);
        });
        r.block_until_idle();
        assert_eq!(counter.get(), 1);
        assert_eq!(r.task_count(), 0, "a finished task must leave the slab");
    }

    /// Timer fires after a short deadline.
    #[test]
    fn timer_fires() {
        let r = make_reactor();
        let inner = Rc::clone(&r.inner);
        let start = Instant::now();
        r.block_on(async move {
            TimerFuture::new(Instant::now() + Duration::from_millis(50), inner).await;
        });
        let elapsed = start.elapsed();
        assert!(
            elapsed >= Duration::from_millis(50),
            "timer fired too early: {elapsed:?}"
        );
        assert!(
            elapsed < Duration::from_millis(500),
            "timer fired too late: {elapsed:?}"
        );
    }

    /// Earlier timer must resolve before a later timer.
    #[test]
    fn timer_ordering() {
        let r = make_reactor();
        let order: Rc<RefCell<Vec<u32>>> = Rc::new(RefCell::new(Vec::new()));

        let inner1 = Rc::clone(&r.inner);
        let order1 = Rc::clone(&order);
        let inner2 = Rc::clone(&r.inner);
        let order2 = Rc::clone(&order);

        r.spawn(async move {
            TimerFuture::new(Instant::now() + Duration::from_millis(100), inner2).await;
            order2.borrow_mut().push(2);
        });

        r.block_on(async move {
            TimerFuture::new(Instant::now() + Duration::from_millis(20), inner1).await;
            order1.borrow_mut().push(1);
        });

        // The block_on completes when its root task does, but the spawned
        // 100ms timer may still be pending. Drive a few more ticks to let
        // it complete.
        let deadline = Instant::now() + Duration::from_millis(500);
        while r.has_pending_tasks() && Instant::now() < deadline {
            r.tick(true);
        }
        assert_eq!(order.borrow().as_slice(), &[1, 2]);
    }

    /// A parked DecodedWire must be returned to the awaiter on resume.
    #[test]
    fn reply_waker_dispatch() {
        let r = make_reactor();
        let got: Rc<StdCell<u64>> = Rc::new(StdCell::new(0));
        let got2 = Rc::clone(&got);
        let reply_fut = r.await_reply(7);
        r.test_route_reply(0, synthetic_decoded_wire(7));
        r.block_on(async move {
            got2.set(reply_fut.await.control.request_id);
        });
        assert_eq!(got.get(), 7);
    }

    /// A reply for a different req_id must not wake an unrelated awaiter:
    /// the guard timer must win the race.
    #[test]
    fn reply_waker_no_spurious() {
        let r = make_reactor();
        // A reply nobody awaits is dropped; the point is that it must not
        // resolve the req_id=7 awaiter below.
        r.test_route_reply(0, synthetic_decoded_wire(8));
        let resolved: Rc<StdCell<bool>> = Rc::new(StdCell::new(false));
        let r2 = Rc::clone(&resolved);
        let timer_inner = Rc::clone(&r.inner);
        let reply_fut = r.await_reply(7);
        r.block_on(async move {
            let timer = TimerFuture::new(Instant::now() + Duration::from_millis(50), timer_inner);
            select_reply_or_timer(timer, reply_fut, &r2).await;
        });
        assert!(!resolved.get(), "reply for req_id=8 must not wake req_id=7 awaiter");
    }

    /// `alloc_request_id` returns strictly increasing values in
    /// `[1, MAX_REGULAR_REQ_ID]`, so bit 31 (the scan marker) stays clear.
    #[test]
    fn alloc_request_id_monotonic() {
        let r = make_reactor();
        let mut last = 0u64;
        for _ in 0..1000 {
            let id = r.alloc_request_id();
            assert!(id > last);
            assert!((1..=MAX_REGULAR_REQ_ID).contains(&id));
            last = id;
        }
    }

    /// `block_on` must cope with a future that wakes itself synchronously
    /// during poll (the wake schedules another poll on the next tick, but
    /// must not double-poll within the current tick).
    #[test]
    fn waker_wake_then_wake_no_double_poll() {
        let r = make_reactor();
        let polls: Rc<StdCell<u32>> = Rc::new(StdCell::new(0));
        let polls2 = polls.clone();
        r.block_on(DoublyWaking {
            polls: polls2,
            polled: 0,
        });
        // Three Pending polls (each waking twice) plus the Ready one. Without
        // RunQueue::push's dedup the double wake would poll twice per tick and
        // the count would be higher.
        assert_eq!(polls.get(), 4, "N wakes before a tick must collapse to one poll");
    }

    /// Spawned task that panics during poll must propagate — no silent
    /// swallow. The panic unwinds through `tick` (and up to the caller
    /// in real use); tests observe it via `#[should_panic]`.
    #[test]
    #[should_panic(expected = "boom")]
    fn panic_in_spawned_task_propagates_not_swallowed() {
        let r = make_reactor();
        r.spawn(async {
            panic!("boom");
        });
        // First tick polls the task and unwinds out of `poll_task` → `tick`.
        r.tick(false);
    }

    /// A timer in the past resolves on the very first poll instead of
    /// hanging the reactor.
    #[test]
    fn timer_in_the_past_resolves_immediately() {
        let r = make_reactor();
        let inner = Rc::clone(&r.inner);
        let start = Instant::now();
        r.block_on(async move {
            TimerFuture::new(Instant::now() - Duration::from_secs(1), inner).await;
        });
        assert!(
            start.elapsed() < Duration::from_millis(100),
            "past-deadline timer must not block"
        );
    }

    /// Cloning a waker, dropping the original, then waking the clone
    /// must still schedule the task. With the key-as-pointer waker
    /// design, clone is a bitwise copy and drop is a no-op — but the
    /// behaviour must still be observable.
    #[test]
    fn waker_clone_outlives_original() {
        let r = make_reactor();
        let original = make_waker(123);
        let cloned = original.clone();
        drop(original);
        cloned.wake();
        let q: Vec<usize> = r.inner.run_queue.borrow().queue.clone();
        assert!(q.contains(&123));
    }

    /// A non-blocking tick returns promptly with no work to do — no syscall
    /// other than the no-op submit. Bound: under 100ms (very generous;
    /// failure indicates accidental blocking in the no-work path).
    #[test]
    fn nonblocking_tick_returns_promptly() {
        let r = make_reactor();
        let start = Instant::now();
        r.tick(false);
        assert!(start.elapsed() < Duration::from_millis(100));
    }

    // ------------------------------------------------------------------
    // Primitives: oneshot / mpsc / AsyncMutex / AsyncRwLock
    // ------------------------------------------------------------------

    #[test]
    fn oneshot_deliver_value() {
        let r = make_reactor();
        let got: Rc<StdCell<i32>> = Rc::new(StdCell::new(0));
        let got2 = Rc::clone(&got);
        let (tx, rx) = oneshot::channel::<i32>();
        r.spawn(async move {
            let v = rx.await.unwrap();
            got2.set(v);
        });
        // Drive one tick so the receiver registers its waker, then send.
        r.tick(false);
        let _ = tx.send(42);
        r.block_until_idle();
        assert_eq!(got.get(), 42);
    }

    #[test]
    fn oneshot_sender_drop_cancels() {
        let r = make_reactor();
        let err: Rc<StdCell<bool>> = Rc::new(StdCell::new(false));
        let err2 = Rc::clone(&err);
        let (tx, rx) = oneshot::channel::<i32>();
        r.spawn(async move {
            let res = rx.await;
            err2.set(res.is_err());
        });
        r.tick(false);
        drop(tx);
        r.block_until_idle();
        assert!(err.get(), "dropping sender must produce Cancelled");
    }

    #[test]
    fn oneshot_receiver_drop_before_send_returns_err() {
        let (tx, rx) = oneshot::channel::<i32>();
        drop(rx);
        assert!(tx.send(7).is_err(), "send to dropped receiver must fail");
    }

    #[test]
    fn mpsc_send_then_recv() {
        let r = make_reactor();
        let got: Rc<RefCell<Vec<i32>>> = Rc::new(RefCell::new(Vec::new()));
        let got2 = Rc::clone(&got);
        let (tx, mut rx) = mpsc::unbounded::<i32>();
        tx.send(1);
        tx.send(2);
        drop(tx);
        r.block_on(async move {
            while let Some(v) = rx.recv().await {
                got2.borrow_mut().push(v);
            }
        });
        assert_eq!(*got.borrow(), vec![1, 2]);
    }

    #[test]
    fn mpsc_multi_senders() {
        let r = make_reactor();
        let got: Rc<RefCell<Vec<i32>>> = Rc::new(RefCell::new(Vec::new()));
        let got2 = Rc::clone(&got);
        let (tx, mut rx) = mpsc::unbounded::<i32>();
        let tx2 = tx.clone();
        tx.send(10);
        tx2.send(20);
        drop(tx);
        drop(tx2);
        r.block_on(async move {
            while let Some(v) = rx.recv().await {
                got2.borrow_mut().push(v);
            }
        });
        let mut g = got.borrow().clone();
        g.sort();
        assert_eq!(g, vec![10, 20]);
    }

    #[test]
    fn async_mutex_serializes_access() {
        let r = make_reactor();
        let order: Rc<RefCell<Vec<u32>>> = Rc::new(RefCell::new(Vec::new()));
        let mutex: Rc<AsyncMutex> = Rc::new(AsyncMutex::new());
        for i in 0u32..3 {
            let m = Rc::clone(&mutex);
            let ord = Rc::clone(&order);
            r.spawn(async move {
                let g = m.lock().await;
                let len = ord.borrow().len();
                ord.borrow_mut().push(i);
                // Fail loudly if another task entered the section while this
                // one held the lock.
                assert_eq!(ord.borrow().len(), len + 1);
                drop(g);
            });
        }
        r.block_until_idle();
        assert_eq!(*order.borrow(), vec![0, 1, 2], "tasks must serialize, in lock order");
    }

    /// Structural regression: a task that acquires the SAL writer mutex,
    /// writes, drops the guard, then awaits must release the mutex
    /// before that await — so a concurrent relay/tick task can acquire
    /// it while the first task's await is outstanding (committer pattern:
    /// emit under lock, `.await` outside).
    #[test]
    fn sal_writer_excl_not_held_across_commit_await() {
        let r = make_reactor();
        let mutex: Rc<AsyncMutex> = Rc::new(AsyncMutex::new());
        // One-shot channel used as a stand-in for "fsync CQE / worker
        // ACK": the fake committer awaits `ack_rx`; the other task
        // sends on `ack_tx` AFTER acquiring the mutex. If the committer
        // was still holding the mutex, it would deadlock because
        // neither would make progress.
        let (ack_tx, ack_rx) = oneshot::channel::<()>();
        let commit_done: Rc<StdCell<bool>> = Rc::new(StdCell::new(false));
        let relay_done: Rc<StdCell<bool>> = Rc::new(StdCell::new(false));

        let m1 = Rc::clone(&mutex);
        let cd = Rc::clone(&commit_done);
        r.spawn(async move {
            // Emit under lock: identical pattern to the new committer.
            {
                let _guard = m1.lock().await;
                // ...SAL writes would go here...
            }
            // Lock dropped. Now wait for "fsync + ACK".
            let _ = ack_rx.await;
            cd.set(true);
        });

        let m2 = Rc::clone(&mutex);
        let rd = Rc::clone(&relay_done);
        r.spawn(async move {
            // This future MUST make progress while the committer is
            // awaiting ack_rx — proving the mutex was released.
            let _guard = m2.lock().await;
            rd.set(true);
            // Unblock the committer by sending its ACK.
            let _ = ack_tx.send(());
        });

        r.block_until_idle();
        assert!(relay_done.get(), "concurrent task must have acquired the mutex");
        assert!(commit_done.get(), "committer must complete after its ACK is delivered");
    }

    /// Structural regression for the SERIAL range allocation: it acquires the
    /// catalog write lock AND the SAL-writer lock, emits synchronously, drops
    /// BOTH, then awaits the fdatasync CQE with no locks held. A concurrent
    /// catalog READER (SEEK / SEEK_BY_INDEX* / tick emission) must make progress
    /// during that await — proving the write lock is not held across the fsync.
    /// If it were, the writer-preferring rwlock would block the reader and both
    /// would deadlock (the reader never sends the fake fsync completion).
    #[test]
    fn catalog_write_lock_not_held_across_serial_fsync() {
        let r = make_reactor();
        let rwlock: Rc<AsyncRwLock> = Rc::new(AsyncRwLock::new());
        let sal: Rc<AsyncMutex> = Rc::new(AsyncMutex::new());
        // Stand-in for the fsync CQE: the SERIAL task awaits it AFTER dropping
        // both locks; the reader sends it after acquiring the read lock.
        let (fsync_tx, fsync_rx) = oneshot::channel::<()>();
        let serial_done: Rc<StdCell<bool>> = Rc::new(StdCell::new(false));
        let reader_done: Rc<StdCell<bool>> = Rc::new(StdCell::new(false));

        let rw1 = Rc::clone(&rwlock);
        let sal1 = Rc::clone(&sal);
        let sd = Rc::clone(&serial_done);
        r.spawn(async move {
            // Reserve + mutate + emit under both locks, release both, THEN fsync.
            {
                let _w = rw1.write().await;
                let _s = sal1.lock().await;
                // ...synchronous SAL emission would go here...
            }
            // Both locks dropped. Park on the fsync with no locks held.
            let _ = fsync_rx.await;
            sd.set(true);
        });

        let rw2 = Rc::clone(&rwlock);
        let rd = Rc::clone(&reader_done);
        r.spawn(async move {
            // A catalog reader MUST acquire the read lock while the SERIAL task is
            // parked on its fsync — impossible if the write lock were held across
            // that await.
            let _rg = rw2.read().await;
            rd.set(true);
            // Unblock the SERIAL task's fsync.
            let _ = fsync_tx.send(());
        });

        r.block_until_idle();
        assert!(
            reader_done.get(),
            "catalog reader must acquire the read lock during the SERIAL fsync"
        );
        assert!(
            serial_done.get(),
            "SERIAL task must complete after its fsync CQE arrives"
        );
    }

    /// Structural regression: the relay loop acquires `sal_writer_excl` for a
    /// synchronous SAL write, then releases it at scope exit before awaiting
    /// the next item from its channel.  If the guard leaked across that await,
    /// a concurrent committer could never acquire the mutex and would deadlock.
    #[test]
    fn sal_writer_excl_not_held_across_relay_recv() {
        let r = make_reactor();
        let mutex: Rc<AsyncMutex> = Rc::new(AsyncMutex::new());
        // `next_rx` stands in for the relay's `rx.recv()` — the await that
        // follows the SAL write scope.  `commit_tx` stands in for a concurrent
        // committer that must be able to acquire the SAL lock while the relay
        // task is parked on `next_rx.await`.
        let (next_tx, next_rx) = oneshot::channel::<()>();
        let (commit_tx, commit_rx) = oneshot::channel::<()>();
        let relay_done: Rc<StdCell<bool>> = Rc::new(StdCell::new(false));
        let commit_done: Rc<StdCell<bool>> = Rc::new(StdCell::new(false));

        let m1 = Rc::clone(&mutex);
        let rd = Rc::clone(&relay_done);
        r.spawn(async move {
            // Phase 2 of relay_loop: acquire lock, sync write, release.
            {
                let _sal = m1.lock().await;
                // ...emit_relay would go here...
            }
            // Lock dropped. Now await the next relay item (rx.recv()).
            let _ = next_rx.await;
            rd.set(true);
        });

        let m2 = Rc::clone(&mutex);
        let cd = Rc::clone(&commit_done);
        r.spawn(async move {
            // Committer: must be able to acquire the SAL lock while the relay
            // task is parked waiting for its next item.  Once it can, it
            // unblocks the relay by sending on `next_tx`.
            let _sal = m2.lock().await;
            cd.set(true);
            let _ = commit_tx.send(());
            let _ = next_tx.send(());
        });

        // commit_rx is unused — its role is to confirm the committer ran.
        drop(commit_rx);

        r.block_until_idle();
        assert!(
            commit_done.get(),
            "committer must have acquired the mutex while relay was parked"
        );
        assert!(relay_done.get(), "relay must complete after being unblocked");
    }

    #[test]
    fn async_rwlock_multiple_readers() {
        let r = make_reactor();
        let active: Rc<StdCell<u32>> = Rc::new(StdCell::new(0));
        let max: Rc<StdCell<u32>> = Rc::new(StdCell::new(0));
        let lock: Rc<AsyncRwLock> = Rc::new(AsyncRwLock::new());
        for _ in 0..4 {
            let l = Rc::clone(&lock);
            let a = Rc::clone(&active);
            let m = Rc::clone(&max);
            r.spawn(async move {
                let _g = l.read().await;
                a.set(a.get() + 1);
                if a.get() > m.get() {
                    m.set(a.get());
                }
                // Yield once to let other tasks acquire too.
                YieldOnce::new().await;
                a.set(a.get() - 1);
            });
        }
        r.block_until_idle();
        assert!(max.get() >= 2, "readers must overlap, got max={}", max.get());
    }

    #[test]
    fn async_rwlock_writer_waits_for_readers() {
        let r = make_reactor();
        let order: Rc<RefCell<Vec<&'static str>>> = Rc::new(RefCell::new(Vec::new()));
        let lock: Rc<AsyncRwLock> = Rc::new(AsyncRwLock::new());
        let l1 = Rc::clone(&lock);
        let o1 = Rc::clone(&order);
        r.spawn(async move {
            let _g = l1.read().await;
            o1.borrow_mut().push("R_start");
            YieldOnce::new().await;
            o1.borrow_mut().push("R_end");
        });
        let l2 = Rc::clone(&lock);
        let o2 = Rc::clone(&order);
        r.spawn(async move {
            let _g = l2.write().await;
            o2.borrow_mut().push("W_start");
        });
        r.block_until_idle();
        let o = order.borrow().clone();
        // R_start before W_start, R_end also before W_start (writer waits).
        let r_end_pos = o.iter().position(|&s| s == "R_end").unwrap();
        let w_start_pos = o.iter().position(|&s| s == "W_start").unwrap();
        assert!(r_end_pos < w_start_pos, "writer must run after reader finishes: {o:?}");
    }

    /// A `LockFuture` dropped while parked (e.g. via `select2`) leaves a
    /// stale waker in `AsyncMutex::waiters`.  `release()` must not pop
    /// exactly one waker — doing so risks consuming the stale entry and
    /// leaving all live waiters permanently blocked.
    #[test]
    fn async_mutex_cancelled_waiter_does_not_block_remaining() {
        let r = make_reactor();
        let mutex: Rc<AsyncMutex> = Rc::new(AsyncMutex::new());
        let done: Rc<StdCell<bool>> = Rc::new(StdCell::new(false));

        // Task A: holds the mutex, yields once (letting B and C park), then releases.
        let m_a = Rc::clone(&mutex);
        r.spawn(async move {
            let _g = m_a.lock().await;
            YieldOnce::new().await;
        });

        // Task B: races lock acquisition against an immediately-ready future.
        // `select2` polls the LockFuture first (it parks its waker inside
        // `waiters`), then `ready()` resolves. The LockFuture is dropped,
        // but its stale waker remains in the queue.
        let m_b = Rc::clone(&mutex);
        r.spawn(async move {
            let _ = select2(m_b.lock(), std::future::ready(())).await;
        });

        // Task C: must acquire the mutex once A releases — must not be
        // blocked by B's stale waker absorbing the single-pop release signal.
        let m_c = Rc::clone(&mutex);
        let d = Rc::clone(&done);
        r.spawn(async move {
            let _g = m_c.lock().await;
            d.set(true);
        });

        for _ in 0..20 {
            r.tick(false);
        }
        assert!(
            done.get(),
            "task C must acquire the mutex after task B's cancelled waiter"
        );
    }

    /// A `WriteFuture` dropped while parked leaves a stale waker in
    /// `AsyncRwLock::write_waiters`.  `release_write()` popping exactly
    /// one waker risks consuming the stale entry and leaving all remaining
    /// live write waiters permanently blocked.
    #[test]
    fn async_rwlock_cancelled_write_waiter_does_not_block_remaining() {
        let r = make_reactor();
        let lock: Rc<AsyncRwLock> = Rc::new(AsyncRwLock::new());
        let done: Rc<StdCell<bool>> = Rc::new(StdCell::new(false));

        // Task A: holds the write lock, yields once, then releases.
        let l_a = Rc::clone(&lock);
        r.spawn(async move {
            let _g = l_a.write().await;
            YieldOnce::new().await;
        });

        // Task B: races write acquisition against an immediately-ready future.
        // Its WriteFuture parks (parked=true, writers_waiting bumped) then
        // is dropped by select2 with its stale waker still in write_waiters.
        let l_b = Rc::clone(&lock);
        r.spawn(async move {
            let _ = select2(l_b.write(), std::future::ready(())).await;
        });

        // Task C: must acquire the write lock after A releases — must not be
        // blocked by B's stale waker absorbing the single-pop release signal.
        let l_c = Rc::clone(&lock);
        let d = Rc::clone(&done);
        r.spawn(async move {
            let _g = l_c.write().await;
            d.set(true);
        });

        for _ in 0..20 {
            r.tick(false);
        }
        assert!(
            done.get(),
            "task C must acquire the write lock after task B's cancelled waiter"
        );
    }

    #[test]
    fn fsync_future_roundtrip() {
        let r = make_reactor();
        let fd = crate::runtime::posix::memfd_create(b"reactor_fsync_future");
        let rc: Rc<StdCell<i32>> = Rc::new(StdCell::new(1));
        let rc2 = Rc::clone(&rc);
        let fsync = r.fsync(fd);
        r.block_on(async move {
            rc2.set(fsync.await);
        });
        unsafe {
            libc::close(fd);
        }
        assert_eq!(rc.get(), 0);
    }

    // -- helper futures used by the tests above --

    /// Future that returns Pending exactly once, then Ready.
    struct YieldOnce {
        yielded: bool,
    }
    impl YieldOnce {
        fn new() -> Self {
            YieldOnce { yielded: false }
        }
    }
    impl Future for YieldOnce {
        type Output = ();
        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
            if self.yielded {
                Poll::Ready(())
            } else {
                self.yielded = true;
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        }
    }

    /// Wakes itself a few times, then completes. Mirrors a tight async
    /// loop that would burn the reactor if double-polled per wake.
    struct DoublyWaking {
        polls: Rc<StdCell<u32>>,
        polled: u32,
    }
    impl Future for DoublyWaking {
        type Output = ();
        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
            let n = self.polled;
            self.polls.set(self.polls.get() + 1);
            if n >= 3 {
                return Poll::Ready(());
            }
            self.polled = n + 1;
            // Wake twice before returning Pending: must not get polled
            // twice in the same tick, only once on the next tick.
            cx.waker().wake_by_ref();
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }

    /// Race a timer and a reply future. Sets `flag` to true if the reply
    /// resolved first, leaves it false if the timer won. Polls both each
    /// tick; the first to return Ready wins.
    async fn select_reply_or_timer<'a, T, R>(timer: T, reply: R, flag: &'a Rc<StdCell<bool>>)
    where
        T: Future<Output = ()> + 'a,
        R: Future<Output = DecodedWire> + 'a,
    {
        let mut timer = Box::pin(timer);
        let mut reply = Box::pin(reply);
        std::future::poll_fn(move |cx| {
            if timer.as_mut().poll(cx).is_ready() {
                return Poll::Ready(());
            }
            if reply.as_mut().poll(cx).is_ready() {
                flag.set(true);
                return Poll::Ready(());
            }
            Poll::Pending
        })
        .await
    }

    /// Build a minimal `DecodedWire` for tests — only `request_id` matters.
    fn synthetic_decoded_wire(req_id: u64) -> DecodedWire {
        use crate::runtime::wire::DecodedControl;
        DecodedWire {
            control: DecodedControl {
                request_id: req_id,
                ..Default::default()
            },
            schema: None,
            data_batch: None,
        }
    }

    /// `block_until_idle` must drive spawned tasks to completion.
    #[test]
    fn block_until_idle_completes() {
        let r = make_reactor();
        let done: Rc<StdCell<bool>> = Rc::new(StdCell::new(false));
        let done2 = Rc::clone(&done);
        let reply = r.await_reply(99);
        r.spawn(async move {
            let _ = reply.await;
            done2.set(true);
        });
        r.test_route_reply(0, synthetic_decoded_wire(99));
        r.block_until_idle();
        assert!(done.get());
        assert_eq!(r.task_count(), 0);
    }

    /// Routed reply: wakes the parked awaiter and resolves it.
    /// No `in_flight` accounting — the tail-chasing ring self-maintains.
    #[test]
    fn route_reply_routes_to_registered_waker() {
        let r = make_reactor();
        r.test_init_state(2);

        let mut fut = std::pin::pin!(r.await_reply(42));
        let waker = make_waker(0);
        let mut cx = Context::from_waker(&waker);
        assert!(fut.as_mut().poll(&mut cx).is_pending());

        r.test_route_reply(1, synthetic_decoded_wire(42));

        assert!(
            r.inner.run_queue.borrow().queue.contains(&0),
            "a routed reply must wake its awaiter"
        );
        assert!(fut.as_mut().poll(&mut cx).is_ready());
    }

    /// Unrouted reply (nobody awaiting): logged and dropped. Must not leave a
    /// slot behind — stale entries would keep non-dead memory alive.
    #[test]
    fn route_reply_unrouted_is_logged_and_dropped() {
        let r = make_reactor();
        r.test_init_state(1);
        // Nobody called await_reply(7).

        r.test_route_reply(0, synthetic_decoded_wire(7));

        assert_eq!(r.inner.replies.len(), 0, "unrouted replies must not leak a park slot");
    }

    /// Dispatching a KIND_FSYNC CQE parks the CQE `res` verbatim under
    /// the request id — both success (rc=0) and failure (rc<0), since
    /// the caller's `rc < 0` fatal-abort branch depends on it.
    #[test]
    fn fsync_dispatch_parks_rc_verbatim() {
        let r = make_reactor();
        for rc in [0, -5] {
            r.inner.fsyncs.open(42, None);
            r.inject_cqe(KIND_FSYNC, 42, rc);
            assert_eq!(r.inner.fsyncs.take_result(42), Some(rc));
        }
    }

    /// End-to-end with a real io_uring: submit fdatasync on a memfd,
    /// block until complete, expect rc=0.  Also asserts
    /// the fsync park map is drained afterwards (catches leaks).
    #[test]
    fn fsync_real_memfd_roundtrip() {
        let r = make_reactor();
        let fd = crate::runtime::posix::memfd_create(b"reactor_fsync_ok");
        let rc = r.block_on(r.fsync(fd));
        unsafe {
            libc::close(fd);
        }
        assert_eq!(rc, 0, "fdatasync on a fresh memfd should succeed");
        assert_eq!(r.inner.fsyncs.len(), 0, "a resolved fsync must retire its slot");
    }

    /// Submitting fdatasync on an fd that is not in the process's fd
    /// table returns a negative rc (typically -EBADF) from the kernel.
    /// Direct replacement for the deleted fork-based ipc test.
    ///
    /// Uses `i32::MAX` rather than `close(real_fd); submit(real_fd)` so
    /// the test is race-free under the parallel test runner — a freshly
    /// closed fd number can be reallocated by another thread before our
    /// SQE reaches the kernel, masking the expected EBADF.
    #[test]
    fn fsync_real_bad_fd_returns_negative() {
        let r = make_reactor();
        let rc = r.block_on(r.fsync(i32::MAX));
        assert!(rc < 0, "fdatasync on a bogus fd must return rc<0, got {rc}");
    }

    /// `fsync` flushes the SQE to the kernel before returning.
    /// Without the eager submit the CQE would only arrive on the next
    /// `tick`, defeating the Phase-A / tick-evaluation overlap.
    #[test]
    fn fsync_submit_flushes_sqe_before_returning() {
        let r = make_reactor();
        let fd = crate::runtime::posix::memfd_create(b"reactor_fsync_flush");
        let fut = r.fsync(fd);
        let id = fut.id;

        // Spin briefly (no further ticks driven from outside) until the
        // CQE either arrives in the ring or we time out.  The kernel
        // completes fdatasync on a memfd in microseconds, so ~100 ms
        // gives generous headroom for scheduler jitter without flakiness.
        let deadline = Instant::now() + Duration::from_millis(100);
        let mut got: Option<i32> = None;
        while Instant::now() < deadline {
            r.drain_cqes_into_wakers();
            if let Some(rc) = r.inner.fsyncs.take_result(id) {
                got = Some(rc);
                break;
            }
        }
        unsafe {
            libc::close(fd);
        }
        assert_eq!(
            got,
            Some(0),
            "fsync CQE must be available without driving another tick — \
             Reactor::fsync should flush the SQE eagerly"
        );
    }

    // ─────────────────────────────────────────────────────────────────
    // KIND_SEND CQE dispatch + SendFuture lifecycle.
    //
    // Regression guards for:
    //   (a) partial-send handling in `send_buffer` (OP_SEND on a stream
    //       socket can return rc < len); treating one CQE as "done"
    //       truncated the scan response at ~208 KB and hung the client;
    //   (b) SendFuture::Drop parking its buffer Rc so the kernel's
    //       in-flight pointer stays valid after cancellation.
    // ─────────────────────────────────────────────────────────────────

    #[test]
    fn send_cqe_parks_rc_and_wakes_waker() {
        let r = make_reactor();
        r.inner.sends.open(55, None);
        let mut fut = std::pin::pin!(SendFuture {
            send_id: 55,
            inner: Rc::clone(&r.inner),
        });
        let waker = make_waker(11);
        let mut cx = Context::from_waker(&waker);
        assert!(fut.as_mut().poll(&mut cx).is_pending());

        r.inject_cqe(KIND_SEND, 55, 1234);

        assert!(
            r.inner.run_queue.borrow().queue.contains(&11),
            "KIND_SEND must wake the send future"
        );
        assert_eq!(
            fut.as_mut().poll(&mut cx),
            Poll::Ready(1234),
            "KIND_SEND must deliver the CQE rc verbatim"
        );
        assert_eq!(r.inner.sends.len(), 0, "a resolved send must retire its slot");
    }

    #[test]
    fn send_cqe_decrements_conn_inflight_and_releases_the_buffer() {
        let r = make_reactor();
        let alive: SendAlive = Rc::new(gnitz_engine::storage::batch_pool::PooledSendBuf(vec![0u8; 16]));
        r.inner.sends.open(77, Some((42, Rc::clone(&alive))));
        r.inner
            .conns
            .borrow_mut()
            .insert(42, Box::new(io::Conn::new(Rc::clone(&r.inner.inbound))));
        r.inner.conns.borrow_mut().get_mut(&42).unwrap().send_inflight = 1;

        r.inject_cqe(KIND_SEND, 77, 16);

        let inflight = r.inner.conns.borrow().get(&42).unwrap().send_inflight;
        assert_eq!(
            inflight, 0,
            "KIND_SEND must decrement conn.send_inflight (gates close_fd)"
        );
        // The keep-alive rides the slot until the awaiter collects the result;
        // once it does, the last reference goes with it.
        assert_eq!(Rc::strong_count(&alive), 2, "slot still holds the buffer");
        let waker = make_waker(0);
        let mut fut = std::pin::pin!(SendFuture {
            send_id: 77,
            inner: Rc::clone(&r.inner),
        });
        assert_eq!(fut.as_mut().poll(&mut Context::from_waker(&waker)), Poll::Ready(16));
        assert_eq!(Rc::strong_count(&alive), 1, "collecting the result frees the buffer");
    }

    /// A dropped SendFuture with an in-flight SQE must leave the buffer alive
    /// for the kernel — the park slot holds it — and let the late CQE free it.
    /// This is `II.2 io_uring SQE buffer lifetime` made concrete.
    #[test]
    fn dropped_send_future_keeps_buffer_alive_until_its_cqe() {
        let r = make_reactor();
        let alive: SendAlive = Rc::new(gnitz_engine::storage::batch_pool::PooledSendBuf(vec![0xAB_u8; 64]));
        r.inner.sends.open(88, Some((42, Rc::clone(&alive))));
        drop(SendFuture {
            send_id: 88,
            inner: Rc::clone(&r.inner),
        });

        assert!(r.inner.sends.is_abandoned(88), "drop must abandon the slot");
        assert_eq!(
            Rc::strong_count(&alive),
            2,
            "the kernel may still read the buffer — it must outlive the future"
        );

        r.inject_cqe(KIND_SEND, 88, 64);
        assert_eq!(r.inner.sends.len(), 0, "the late CQE must retire the abandoned slot");
        assert_eq!(Rc::strong_count(&alive), 1, "and free the buffer");
    }

    // ─────────────────────────────────────────────────────────────────
    // Per-fd state lifecycle. Every bit of it lives in the `Conn`, so
    // retiring a connection retires all of it at once.
    //
    // Regression guard: a kernel-reused fd number carrying the previous
    // incarnation's closed flag made the new connection see immediate EOF.
    // ─────────────────────────────────────────────────────────────────

    /// A reaped connection leaves nothing behind for the next incarnation of
    /// the same fd number: registering again starts open, with no backlog.
    /// Otherwise `recv().await` on the new connection returns `None` at once
    /// and the connection is dead on arrival.
    #[test]
    fn reaped_conn_leaves_no_state_for_the_next_connection() {
        let r = make_reactor();
        let (read_end, write_end) = unsafe { pipe_pair() };
        r.register_conn(read_end);
        // Peer EOF: closes the connection and queues it for reaping.
        r.handle_recv_cqe(read_end, 0);
        assert!(r.inner.conns.borrow().get(&read_end).unwrap().q.recv_closed());
        r.reap_closing_conns();
        assert!(
            r.inner.conns.borrow().is_empty(),
            "reaping must retire the whole Conn, closing its fd"
        );

        // The kernel is now free to hand that number back out.
        let (next_read, next_write) = unsafe { pipe_pair() };
        r.register_conn(next_read);
        let conns = r.inner.conns.borrow();
        let conn = conns.get(&next_read).expect("registered");
        assert!(!conn.q.recv_closed(), "a fresh connection must not inherit phantom EOF");
        assert!(conn.q.pending().is_empty(), "nor a stale delivery backlog");
        drop(conns);
        unsafe {
            libc::close(write_end);
            libc::close(next_read);
            libc::close(next_write);
        }
    }

    /// The egress deadline is process-wide, so both socketpair send tests seed
    /// it through here with one value: short enough that the eviction test
    /// waits it out in a couple of seconds, and far above what the partial-send
    /// test needs to push 200 KB through a draining reader.
    fn short_client_send_timeout() -> Duration {
        conn::force_client_send_timeout(Duration::from_secs(2));
        client_send_timeout()
    }

    /// A socketpair set up for an egress test: the reactor, the sender fd, and
    /// the receiver end. The sender is registered in `conns` so `send_inflight`
    /// accounting has something to touch, as in real flow. `sndbuf` shrinks both
    /// socket buffers, so a payload larger than it is guaranteed to split across
    /// several OP_SEND CQEs. The caller closes both fds.
    unsafe fn egress_pair(sndbuf: Option<i32>) -> (Rc<Reactor>, i32, i32) {
        let mut fds = [0i32; 2];
        assert_eq!(
            libc::socketpair(libc::AF_UNIX, libc::SOCK_STREAM, 0, fds.as_mut_ptr()),
            0,
            "socketpair"
        );
        let (sender, receiver) = (fds[0], fds[1]);
        if let Some(bytes) = sndbuf {
            for (fd, opt) in [(sender, libc::SO_SNDBUF), (receiver, libc::SO_RCVBUF)] {
                libc::setsockopt(
                    fd,
                    libc::SOL_SOCKET,
                    opt,
                    &bytes as *const _ as *const libc::c_void,
                    std::mem::size_of::<i32>() as u32,
                );
            }
        }
        let r: Rc<Reactor> = Rc::new(make_reactor());
        r.inner
            .conns
            .borrow_mut()
            .insert(sender, Box::new(io::Conn::new(Rc::clone(&r.inner.inbound))));
        (r, sender, receiver)
    }

    /// Read `expect` bytes off `fd` and close it, so the send under test never
    /// stalls on a full socket buffer. Returns what it actually saw.
    fn spawn_drain(fd: i32, expect: usize) -> std::thread::JoinHandle<usize> {
        std::thread::spawn(move || unsafe {
            let mut seen = 0usize;
            let mut scratch = vec![0u8; 64 * 1024];
            while seen < expect {
                let n = libc::read(fd, scratch.as_mut_ptr() as *mut libc::c_void, scratch.len());
                if n <= 0 {
                    break;
                }
                seen += n as usize;
            }
            libc::close(fd);
            seen
        })
    }

    /// Concrete test of the partial-send contract: io_uring's OP_SEND on a
    /// stream socket can return `rc < len`. A 200 KB payload over ~8 KB socket
    /// buffers forces `send_buffer`'s loop to resubmit the remaining slice until
    /// the full buffer drains.
    #[test]
    fn send_buffer_loops_until_full_payload_sent_over_socketpair() {
        short_client_send_timeout();
        unsafe {
            let (r, sender, receiver) = egress_pair(Some(8 * 1024));
            let payload = vec![0x5Au8; 200 * 1024];
            let payload_len = payload.len();
            let drain_t = spawn_drain(receiver, payload_len);

            let r2 = Rc::clone(&r);
            let sent = r.block_on(async move {
                r2.send_buffer(sender, gnitz_engine::storage::batch_pool::PooledSendBuf(payload))
                    .await
            });
            let received = drain_t.join().expect("drain thread");

            libc::close(sender);
            assert_eq!(
                sent as usize, payload_len,
                "send_buffer must loop on partial CQEs until the full \
                 payload is sent (got rc={sent}, expected {payload_len})"
            );
            assert_eq!(
                received, payload_len,
                "receiver must observe every byte — a truncated send_buffer \
                 would leave the client blocked waiting for bytes that never \
                 arrive"
            );
        }
    }

    /// `send_buffer` carries the client-egress deadline: a peer that never reads
    /// must be evicted, not park the connection task forever on a
    /// master-authored frame. No reader thread here, so once both socket buffers
    /// fill the send makes zero progress and only the timer can end it — by
    /// shutting the fd down and surfacing a negative rc.
    #[test]
    fn send_buffer_evicts_a_client_that_never_drains() {
        let timeout = short_client_send_timeout();
        unsafe {
            let (r, sender, receiver) = egress_pair(Some(4 * 1024));

            // Far larger than both buffers, and nothing ever reads the other
            // end — the send stalls partway and only the deadline can end it.
            let payload = vec![0x7Au8; 1024 * 1024];
            let r2 = Rc::clone(&r);
            let start = Instant::now();
            let rc = r.block_on(async move {
                r2.send_buffer(sender, gnitz_engine::storage::batch_pool::PooledSendBuf(payload))
                    .await
            });
            let elapsed = start.elapsed();

            assert!(rc < 0, "a client that never drains must be evicted, got rc={rc}");
            assert!(
                elapsed >= timeout,
                "eviction must wait out the full deadline ({timeout:?}), took {elapsed:?}"
            );
            // The eviction path shuts the socket down, so nothing more can be
            // written to it — that is what releases the send's held resources.
            let probe = libc::send(sender, [0u8; 1].as_ptr() as *const libc::c_void, 1, libc::MSG_NOSIGNAL);
            assert_eq!(probe, -1, "evicted fd must be shut down for send");

            libc::close(sender);
            libc::close(receiver);
        }
    }

    /// Where coalescing stops paying, which is what `COALESCE_MAX_BYTES` is set
    /// from: W head frames leaving as W guarded sends versus one guarded send of
    /// their concatenation. Each send costs an `OP_SEND` + `OP_TIMEOUT` +
    /// `OP_ASYNC_CANCEL` triple across two `io_uring_enter` calls, so the win is
    /// a fixed cost per elided frame and the loss is the concatenation copy —
    /// the grid brackets the crossover on both axes.
    ///
    /// Only the RATIO is meaningful: absolute per-batch times swing up to 2.5x
    /// with machine load. Both arms send through `send_buffer`, so this isolates
    /// the per-frame kernel cost; the real per-worker drain uses `send_slot`,
    /// which additionally pins a ring slot. Arm order alternates per sample
    /// because the second send of a pair is systematically the cheaper one.
    ///
    /// `cd crates && cargo test -p gnitz-server --release fanout_coalesced_egress_bench -- --ignored --nocapture --test-threads=1`
    #[test]
    #[ignore]
    fn fanout_coalesced_egress_bench() {
        use gnitz_engine::storage::batch_pool::{acquire_buf, PooledSendBuf};
        use std::hint::black_box;

        const ITERS: usize = 3000;

        for w in [2usize, 4, 8] {
            for total in [4 * 1024usize, 32 * 1024, 64 * 1024, 128 * 1024] {
                let per_frame = total / w;
                unsafe {
                    let (r, sender, receiver) = egress_pair(None);
                    // Both arms push `total` bytes per sample; the reader keeps
                    // the socket buffers from ever stalling a send, so the timed
                    // region is kernel-op cost, not backpressure.
                    let expect = 2 * ITERS * total;
                    let drain_t = spawn_drain(receiver, expect);

                    let frame = vec![0xA5u8; per_frame];
                    let r2 = Rc::clone(&r);
                    let (per_frame_dur, coalesced_dur) = r.block_on(async move {
                        let (mut a, mut b) = (Duration::ZERO, Duration::ZERO);
                        for i in 0..ITERS {
                            // Source buffers are filled outside the timed region
                            // on both arms except the concatenation itself,
                            // which is the copy under test.
                            let mut bufs = Vec::with_capacity(w);
                            for _ in 0..w {
                                let mut buf = acquire_buf();
                                buf.extend_from_slice(&frame);
                                bufs.push(PooledSendBuf(buf));
                            }
                            let run_per_frame = async |bufs: Vec<PooledSendBuf>| {
                                let t = Instant::now();
                                for buf in bufs {
                                    black_box(r2.send_buffer(sender, buf).await);
                                }
                                t.elapsed()
                            };
                            let run_coalesced = async || {
                                let t = Instant::now();
                                let mut buf = acquire_buf();
                                buf.reserve(total);
                                for _ in 0..w {
                                    buf.extend_from_slice(&frame);
                                }
                                black_box(r2.send_buffer(sender, PooledSendBuf(buf)).await);
                                t.elapsed()
                            };
                            if i % 2 == 0 {
                                a += run_per_frame(bufs).await;
                                b += run_coalesced().await;
                            } else {
                                b += run_coalesced().await;
                                a += run_per_frame(bufs).await;
                            }
                        }
                        (a, b)
                    });

                    let seen = drain_t.join().expect("drain thread");
                    assert_eq!(seen, expect, "reader must observe every byte both arms sent");
                    libc::close(sender);

                    let delta = coalesced_dur.as_secs_f64() / per_frame_dur.as_secs_f64() - 1.0;
                    println!(
                        "coalesced egress W={w} total={total}B: coalesced vs per-frame {:+.1}% \
                         (per-frame {:?}/batch, coalesced {:?}/batch)",
                        delta * 100.0,
                        per_frame_dur / ITERS as u32,
                        coalesced_dur / ITERS as u32,
                    );
                }
            }
        }
    }

    // ─────────────────────────────────────────────────────────────────
    // Inbound-memory hard cap (fd-path OOM guard).
    // ─────────────────────────────────────────────────────────────────

    /// Build a length-prefixed wire frame: 4-byte LE payload length + payload.
    fn framed(payload: &[u8]) -> Vec<u8> {
        let mut v = Vec::with_capacity(4 + payload.len());
        v.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        v.extend_from_slice(payload);
        v
    }

    /// AF_UNIX SOCK_STREAM pair. Returns `(server_read_fd, client_write_fd)`:
    /// the reactor `register_conn`s the first and recvs from it; the test
    /// `write_all`s framed bytes into the second.
    unsafe fn stream_pair() -> (i32, i32) {
        let mut fds = [0i32; 2];
        let rc = libc::socketpair(libc::AF_UNIX, libc::SOCK_STREAM, 0, fds.as_mut_ptr());
        assert_eq!(rc, 0, "socketpair");
        (fds[1], fds[0])
    }

    /// `(read_end, write_end)` of a fresh pipe. Tests that need a real,
    /// owned fd number — `reap_closing_conns` calls `libc::close` on what it
    /// reaps, so a magic number would race a parallel test that owns it.
    unsafe fn pipe_pair() -> (i32, i32) {
        let mut fds = [0i32; 2];
        assert_eq!(libc::pipe(fds.as_mut_ptr()), 0, "pipe");
        (fds[0], fds[1])
    }

    /// Poll a fresh `recv(fd)` future exactly once, returning its result:
    /// `Some(_)` if it resolved (a frame, or `None` when closed), `None` if
    /// still pending.
    fn poll_recv_once(r: &Reactor, fd: i32) -> Option<Option<io::RecvBuf>> {
        let mut fut = Box::pin(r.recv(fd));
        let waker = make_waker(usize::MAX);
        let mut cx = Context::from_waker(&waker);
        match fut.as_mut().poll(&mut cx) {
            Poll::Ready(v) => Some(v),
            Poll::Pending => None,
        }
    }

    /// Drive the reactor up to `max` non-blocking ticks, returning `true` as
    /// soon as `cond` holds after a tick (and `false` if it never does).
    fn poll_until(r: &Reactor, max: usize, mut cond: impl FnMut() -> bool) -> bool {
        (0..max).any(|_| {
            r.tick(false);
            cond()
        })
    }

    /// Cap trips: unconsumed frames whose cumulative weight passes the ceiling
    /// close the connection at the breaching header (before malloc), and reap
    /// returns the global counter to 0.
    #[test]
    fn inbound_cap_trips_and_reap_reconciles() {
        unsafe {
            let (read_fd, write_fd) = stream_pair();
            let r = make_reactor();
            r.register_conn(read_fd);
            r.set_max_payload_len(read_fd, 1 << 20);
            // frame_weight(100) = 100. Two frames = 200 held; the 3rd frame's
            // header pushes 200 + 100 = 300 > 250 and is refused before malloc.
            r.inbound().set_cap(250);
            let payload = vec![0xABu8; 100];
            let mut wire = Vec::new();
            for _ in 0..3 {
                wire.extend_from_slice(&framed(&payload));
            }
            gnitz_engine::foundation::posix_io::write_all_fd(write_fd, &wire).expect("write");

            let reaped = poll_until(&r, 10_000, || !r.inner.conns.borrow().contains_key(&read_fd));
            assert!(reaped, "cap-trip connection was never reaped");
            assert_eq!(
                r.inbound().held(),
                0,
                "reap must subtract the reaped connection's undrained share"
            );
            assert!(
                poll_recv_once(&r, read_fd).unwrap().is_none(),
                "recv after a cap-trip close must yield None"
            );

            libc::close(write_fd); // read_fd was closed by reap
        }
    }

    /// In-flight (partial, un-completed) payloads are accounted, and a second
    /// connection whose first frame would breach the full cap is refused — the
    /// many-connection uncounted-in-flight OOM vector.
    #[test]
    fn inbound_cap_counts_in_flight_and_refuses_new_conn() {
        unsafe {
            let (read_fd, write_fd) = stream_pair();
            let r = make_reactor();
            r.register_conn(read_fd);
            r.set_max_payload_len(read_fd, 1 << 20);
            // Exactly one 10_000-byte in-flight buffer fits.
            r.inbound().set_cap(10_000);

            // Header claims 10_000 bytes but only 100 are delivered: the buffer
            // is malloc'd and counted, yet no frame completes (no MessageDone).
            let mut hdr_and_part = Vec::new();
            hdr_and_part.extend_from_slice(&10_000u32.to_le_bytes());
            hdr_and_part.extend_from_slice(&[0x11u8; 100]);
            gnitz_engine::foundation::posix_io::write_all_fd(write_fd, &hdr_and_part).expect("write");

            let counted = poll_until(&r, 10_000, || r.inbound().held() == 10_000);
            assert!(counted, "in-flight buffer was not accounted");
            assert!(
                r.inner
                    .conns
                    .borrow()
                    .get(&read_fd)
                    .is_none_or(|c| c.q.pending().is_empty()),
                "no frame should have completed from a partial payload"
            );

            // Second connection whose first frame would breach the now-full cap.
            let (read_fd2, write_fd2) = stream_pair();
            r.register_conn(read_fd2);
            r.set_max_payload_len(read_fd2, 1 << 20);
            gnitz_engine::foundation::posix_io::write_all_fd(write_fd2, &framed(&[0x22u8; 100])).expect("write");

            let refused = poll_until(&r, 10_000, || !r.inner.conns.borrow().contains_key(&read_fd2));
            assert!(refused, "over-cap second connection was not closed");
            // Refused connection allocated nothing; the first buffer is intact.
            assert_eq!(r.inbound().held(), 10_000);

            libc::close(write_fd);
            libc::close(write_fd2); // read_fd2 was closed by reap
            libc::close(read_fd); // conn1 never reaped (in-flight)
        }
    }

    /// Accounting balances: consumption decrements the counter, so total traffic
    /// far above the cap never trips as long as the consumer keeps pace, and the
    /// counter returns to 0 once the queue fully drains.
    #[test]
    fn inbound_cap_accounting_balances_on_consume() {
        let (read_fd, write_fd) = unsafe { stream_pair() };
        let r = Rc::new(make_reactor());
        r.register_conn(read_fd);
        r.set_max_payload_len(read_fd, 1 << 20);
        // Cap admits several frames; the pipeline holds ~1 at a time because
        // each frame is popped in the same tick it lands, so 10 frames/round of
        // 1_000-weight traffic (10_000 > cap) never trips.
        r.inbound().set_cap(5_000);

        let payload = vec![0x7Eu8; 1_000]; // frame_weight = 1_000
        for _round in 0..2 {
            let mut wire = Vec::new();
            for _ in 0..10 {
                wire.extend_from_slice(&framed(&payload));
            }
            gnitz_engine::foundation::posix_io::write_all_fd(write_fd, &wire).expect("write");
            let r2 = Rc::clone(&r);
            r.block_on(async move {
                for _ in 0..10 {
                    let buf = r2.recv(read_fd).await.expect("frame");
                    assert_eq!(buf.as_slice().len(), 1_000);
                }
            });
        }
        assert_eq!(
            r.inbound().held(),
            0,
            "counter must return to 0 once every frame is consumed"
        );

        unsafe {
            libc::close(read_fd);
            libc::close(write_fd);
        }
    }

    /// Tiny-frame floor: a flood of 1-byte payloads trips the cap after
    /// ~CAP/64 frames (each weighs the 64-byte floor), not CAP — without the
    /// floor, 65 one-byte frames weigh 65 B and would never trip.
    #[test]
    fn inbound_cap_tiny_frame_floor_trips_early() {
        unsafe {
            let (read_fd, write_fd) = stream_pair();
            let r = make_reactor();
            r.register_conn(read_fd);
            // 1-byte payloads ≤ HELLO_PRE_HANDSHAKE_LEN (8), so no
            // set_max_payload_len is needed.
            r.inbound().set_cap(4096);
            // 64 frames = 64 × 64 = 4096 held; the 65th frame's header would
            // make 4160 > 4096 and is refused.
            let mut wire = Vec::new();
            for _ in 0..65 {
                wire.extend_from_slice(&framed(&[0xCD]));
            }
            gnitz_engine::foundation::posix_io::write_all_fd(write_fd, &wire).expect("write");

            let reaped = poll_until(&r, 20_000, || !r.inner.conns.borrow().contains_key(&read_fd));
            assert!(reaped, "tiny-frame flood must trip the cap via the frame_weight floor");
            assert_eq!(
                r.inbound().held(),
                0,
                "reap must reconcile the counter after a tiny-frame trip"
            );

            libc::close(write_fd); // read_fd was closed by reap
        }
    }

    // ─────────────────────────────────────────────────────────────────
    // Per-connection recv policy on the fd path: the pre-HELLO frame
    // ceiling and the zero-length close sentinel. Both live in
    // `RecvQueue::deliver`, shared with the TLS transport.
    // ─────────────────────────────────────────────────────────────────

    /// A first frame larger than the pre-handshake ceiling is refused at its
    /// header — before any payload byte is allocated — and the connection dies.
    #[test]
    fn oversize_first_frame_is_refused_at_the_header() {
        unsafe {
            let (read_fd, write_fd) = stream_pair();
            let r = make_reactor();
            r.register_conn(read_fd);
            // No set_max_payload_len: the ceiling is still the 8-byte HELLO
            // payload size, so a 9-byte frame must be refused.
            let wire = framed(&[0u8; io::HELLO_PRE_HANDSHAKE_LEN + 1]);
            gnitz_engine::foundation::posix_io::write_all_fd(write_fd, &wire).expect("write");

            let reaped = poll_until(&r, 10_000, || !r.inner.conns.borrow().contains_key(&read_fd));
            assert!(
                reaped,
                "a frame over the per-connection ceiling must close the connection"
            );
            assert_eq!(r.inbound().held(), 0, "a refused frame must never have been allocated");

            libc::close(write_fd); // read_fd was closed by reap
        }
    }

    /// A zero-length frame is the close sentinel, not a frame: it closes the
    /// connection instead of completing a message.
    #[test]
    fn zero_length_frame_closes_the_connection() {
        unsafe {
            let (read_fd, write_fd) = stream_pair();
            let r = make_reactor();
            r.register_conn(read_fd);
            gnitz_engine::foundation::posix_io::write_all_fd(write_fd, &0u32.to_le_bytes()).expect("write");

            let reaped = poll_until(&r, 10_000, || !r.inner.conns.borrow().contains_key(&read_fd));
            assert!(reaped, "the zero-length sentinel must close the connection");

            libc::close(write_fd); // read_fd was closed by reap
        }
    }

    /// A live `PeerToken` — what `Peer::unix` holds for its whole life — must
    /// defer the reap. Without it the fd is closed while its `Peer` can still
    /// name that number, and the kernel may hand it to a freshly-accepted
    /// client, so the old peer's next send goes into the new client's socket.
    #[test]
    fn peer_token_defers_reap_until_it_drops() {
        unsafe {
            let (read_fd, write_fd) = stream_pair();
            let r = make_reactor();
            r.register_conn(read_fd);
            let token = PeerToken::new(&r, read_fd);

            // Peer FIN while the token lives: the recv completes with 0, so
            // nothing is outstanding but the token.
            libc::close(write_fd);
            let closing = poll_until(&r, 10_000, || r.inner.closing_fds.borrow().contains(&read_fd));
            assert!(closing, "peer FIN must mark the connection closing");
            r.tick(false);
            assert!(
                r.inner.conns.borrow().contains_key(&read_fd),
                "a live PeerToken must defer the reap"
            );

            drop(token);
            let reaped = poll_until(&r, 10, || !r.inner.conns.borrow().contains_key(&read_fd));
            assert!(reaped, "the first reap after the token drops must retire the fd");
        }
    }

    /// `close_fd` on a connection whose recv is armed must cancel it, so a
    /// rejected client that then goes silent does not pin its fd forever.
    /// The recv SQE is queued but unflushed when `close_fd` runs, which is the
    /// same-tick ordering the reachable path produces (the HELLO rejection
    /// closes right after `handle_recv_cqe` re-armed).
    #[test]
    fn close_fd_cancels_an_armed_recv_so_a_silent_peer_is_reaped() {
        unsafe {
            let (read_fd, write_fd) = stream_pair();
            let r = make_reactor();
            r.register_conn(read_fd);
            assert!(
                r.inner.conns.borrow().get(&read_fd).unwrap().recv_armed,
                "register_conn arms the recv"
            );
            r.close_fd(read_fd);

            // The peer neither writes nor closes: only the cancellation can
            // complete the recv.
            let reaped = poll_until(&r, 10_000, || !r.inner.conns.borrow().contains_key(&read_fd));
            assert!(
                reaped,
                "close_fd must cancel the armed recv; without it the fd leaks until the peer acts"
            );

            libc::close(write_fd); // read_fd was closed by reap
        }
    }

    // ─────────────────────────────────────────────────────────────────
    // FLAG_EXCHANGE demux + W2M reset gating + select2 +
    // sal_writer_excl + TimerFuture::Drop.
    // ─────────────────────────────────────────────────────────────────

    /// Inject a synthetic FLAG_EXCHANGE wire for `view_id` on worker `w`.
    /// `req_id` is echoed back from the worker side; the accumulator
    /// keys by `(view_id, source_id)`, not req_id.
    fn synthetic_exchange_wire(view_id: i64, req_id: u64) -> DecodedWire {
        synthetic_exchange_wire_src(view_id, 0, req_id)
    }

    fn synthetic_exchange_wire_src(view_id: i64, source_id: i64, req_id: u64) -> DecodedWire {
        use crate::runtime::wire::DecodedControl;
        DecodedWire {
            control: DecodedControl {
                target_id: view_id as u64,
                flags: FLAG_EXCHANGE,
                seek_pk: source_id as u128,
                request_id: req_id,
                ..Default::default()
            },
            schema: Some(SchemaDescriptor::minimal_u64()),
            data_batch: None,
        }
    }

    /// FLAG_EXCHANGE replies must NOT consume the registered tick
    /// waker. The tick worker is still mid-DAG and the final ACK will
    /// arrive separately.
    #[test]
    fn route_reply_flag_exchange_does_not_wake() {
        let r = make_reactor();
        r.test_init_state(2);
        let mut fut = std::pin::pin!(r.await_reply(42));
        let waker = make_waker(0);
        let mut cx = Context::from_waker(&waker);
        assert!(fut.as_mut().poll(&mut cx).is_pending());

        let exch = synthetic_exchange_wire(/*view_id*/ 100, /*req_id*/ 42);
        r.test_route_reply(0, exch);

        assert!(
            fut.as_mut().poll(&mut cx).is_pending(),
            "FLAG_EXCHANGE must NOT resolve the tick's await_reply"
        );
        assert!(
            r.inner.replies.has_waker(42),
            "FLAG_EXCHANGE must leave the tick waker parked"
        );
    }

    /// Once every worker has reported FLAG_EXCHANGE for the same
    /// view_id, the accumulator produces a PendingRelay and dispatches
    /// it to the registered relay_tx.
    #[test]
    fn route_reply_flag_exchange_completes_view_dispatches_relay() {
        let r = make_reactor();
        let (tx, mut rx) = mpsc::unbounded::<PendingRelay>();
        r.attach_relay_tx(tx);
        r.test_init_state(2);

        let ack10 = r.await_reply(10);
        let ack11 = r.await_reply(11);

        r.test_route_reply(0, synthetic_exchange_wire(99, 10));
        assert!(
            rx.try_recv().is_none(),
            "single-worker FLAG_EXCHANGE must not produce a relay"
        );
        r.test_route_reply(1, synthetic_exchange_wire(99, 11));
        let relay = rx.try_recv().expect("complete view must produce a relay");
        assert_eq!(relay.view_id, 99);
        assert_eq!(relay.payloads.len(), 2);
        // Final ACKs (no FLAG_EXCHANGE) for the same req_ids resolve the
        // awaiters the exchange frames deliberately left parked.
        r.test_route_reply(0, synthetic_decoded_wire(10));
        r.test_route_reply(1, synthetic_decoded_wire(11));
        let waker = make_waker(0);
        let mut cx = Context::from_waker(&waker);
        assert!(std::pin::pin!(ack10).as_mut().poll(&mut cx).is_ready());
        assert!(std::pin::pin!(ack11).as_mut().poll(&mut cx).is_ready());
    }

    /// A view with multiple input sources (e.g. join of two tables)
    /// drives two distinct exchange rounds per tick — same view_id,
    /// different source_ids. Keying the accumulator by (view_id,
    /// source_id) keeps their payloads in disjoint map entries; the
    /// relays come out one per round, each tagged with its own
    /// source_id. Before the tuple key, round B's payloads
    /// last-write-wins-overwrote round A's and the relay scattered with
    /// source B's shard_cols over source A's data.
    #[test]
    fn accumulator_distinguishes_source_ids() {
        let r = make_reactor();
        let (tx, mut rx) = mpsc::unbounded::<PendingRelay>();
        r.attach_relay_tx(tx);
        r.test_init_state(4);
        *r.inner.exchange_acc.borrow_mut() = ExchangeAccumulator::new(4);

        // One open reply slot per worker, so the final ACKs have somewhere
        // to land — route_reply drops a reply nobody awaits.
        let _acks: Vec<ReplyFuture> = (0..4).map(|w| r.await_reply(100 + w as u64)).collect();

        // Interleave two rounds for the same view_id=100:
        //   round A: (view_id=100, source_id=10), workers 0..4
        //   round B: (view_id=100, source_id=20), workers 0..4
        // Arrival order deliberately mixed; neither round is strictly
        // before the other in wall-clock order.
        r.test_route_reply(0, synthetic_exchange_wire_src(100, 10, 100));
        r.test_route_reply(0, synthetic_exchange_wire_src(100, 20, 100));
        r.test_route_reply(1, synthetic_exchange_wire_src(100, 20, 101));
        r.test_route_reply(1, synthetic_exchange_wire_src(100, 10, 101));
        r.test_route_reply(2, synthetic_exchange_wire_src(100, 10, 102));
        r.test_route_reply(3, synthetic_exchange_wire_src(100, 10, 103));
        // Round A must now be complete (4 workers reported).
        let ra = rx.try_recv().expect("round A should produce a relay");
        assert_eq!(ra.view_id, 100);
        assert_eq!(ra.source_id, 10);
        assert_eq!(ra.payloads.len(), 4);
        // Round B still incomplete — worker 2+3 haven't reported for src=20.
        assert!(rx.try_recv().is_none(), "round B must not be ready yet");

        r.test_route_reply(2, synthetic_exchange_wire_src(100, 20, 102));
        r.test_route_reply(3, synthetic_exchange_wire_src(100, 20, 103));
        let rb = rx.try_recv().expect("round B should now produce a relay");
        assert_eq!(rb.view_id, 100);
        assert_eq!(rb.source_id, 20);
        assert_eq!(rb.payloads.len(), 4);
    }

    /// `select2` returns whichever future completes first; the loser is
    /// dropped. With a pre-resolved future and an unresolved one, the
    /// pre-resolved must win.
    #[test]
    fn select2_pre_resolved_wins() {
        let r = make_reactor();
        let inner = Rc::clone(&r.inner);
        let v: Rc<StdCell<u32>> = Rc::new(StdCell::new(0));
        let v2 = Rc::clone(&v);
        r.block_on(async move {
            let ready = async { 7u32 };
            let never = TimerFuture::new(Instant::now() + Duration::from_secs(60), inner);
            match select2(ready, never).await {
                Either::A(x) => v2.set(x),
                Either::B(()) => panic!("timer should not have fired"),
            }
        });
        assert_eq!(v.get(), 7);
    }

    /// A dropped TimerFuture must set the cancellation flag so the
    /// KIND_TIMEOUT CQE handler discards the wake. Verified by registering
    /// a timer, dropping the future, then running ticks and asserting the
    /// CQE is consumed without firing the waker (key 999 must not appear
    /// in the run queue).
    #[test]
    fn timer_future_drop_cancels_heap_entry() {
        let r = make_reactor();
        let inner = Rc::clone(&r.inner);
        let waker = make_waker(999);
        // Build a TimerFuture, poll once to register, then drop it.
        let mut tf = TimerFuture::new(Instant::now() + Duration::from_millis(10), inner);
        let mut cx = Context::from_waker(&waker);
        let pinned = Pin::new(&mut tf);
        let _ = pinned.poll(&mut cx);
        assert_eq!(r.inner.timers.len(), 1);
        drop(tf);
        // Drive ticks past the deadline; the cancelled CQE must be
        // discarded without waking key=999.
        std::thread::sleep(Duration::from_millis(20));
        for _ in 0..4 {
            r.tick(false);
        }
        let q: Vec<usize> = r.inner.run_queue.borrow().queue.to_vec();
        assert!(!q.contains(&999), "cancelled timer must not wake its original waker");
    }

    /// Cross-process stress: a forked child publishes `n_messages` into a
    /// shared W2M ring while the parent drains them through the reactor's
    /// `FUTEX_WAITV` + `W2mReceiver` pipeline.
    ///
    /// Regression guard for two distinct hazards:
    /// 1. The lost-wake race in `refresh_futex_waitv_vals`: at this scale the
    ///    master takes many `refresh → arm` cycles, each a potential lost-wake
    ///    window. A missed wake hangs the test (caught by the timer guard).
    /// 2. The writer-crosses-reader data-loss bug in the SKIP-wrap path:
    ///    capacity is small enough (64 KiB) and the message count high enough
    ///    (500 × ~280 B ≈ 140 KiB) to force multiple SKIP-wraps. Truncated or
    ///    out-of-order delivery fails the `ids` assertion.
    ///
    /// Waker-install ordering matters: the reply futures must register their
    /// wakers BEFORE `attach_w2m` runs its initial drain, or replies drained
    /// during attach are logged as "unrouted" and dropped. That is what the
    /// `tick(false)` between `spawn` and `attach_w2m` is for.
    fn w2m_cross_process_stress(n_messages: u64, timeout_secs: u64) {
        use crate::runtime::reactor::{join_all_unpin, select2, Either};
        use crate::runtime::w2m::{W2mReceiver, W2mWriter};
        use crate::runtime::w2m_ring;
        use crate::runtime::wire::STATUS_OK;
        use std::time::Duration;

        const CAPACITY: usize = 64 * 1024;

        let region = gnitz_engine_testkit::SharedRegion::new(CAPACITY);
        let ptr = region.ptr();
        unsafe {
            w2m_ring::init_region_for_tests(ptr, CAPACITY as u64);
        }

        let pid = unsafe { libc::fork() };
        assert!(pid >= 0, "fork failed");
        if pid == 0 {
            // Child: publish a monotonic req_id stream as fast as
            // possible. The parent's wake protocol must not drop
            // any of them under the resulting drain-refresh-arm
            // race pressure.
            let writer = W2mWriter::new(ptr);
            for req_id in 1..=n_messages {
                writer.send_status(0, req_id, STATUS_OK, &[]);
            }
            unsafe {
                libc::_exit(0);
            }
        }

        // Parent: drain via the reactor's FUTEX_WAITV + W2mReceiver path.
        let reactor = Reactor::new(16).expect("reactor");

        let received: Rc<RefCell<Vec<u64>>> = Rc::new(RefCell::new(Vec::new()));
        let reply_futs: Vec<_> = (1..=n_messages).map(|i| reactor.await_reply(i)).collect();
        {
            let received = Rc::clone(&received);
            reactor.spawn(async move {
                let replies = join_all_unpin(reply_futs).await;
                *received.borrow_mut() = replies.into_iter().map(|r| r.control.request_id).collect();
            });
        }
        // One tick polls the spawned task, which walks join_all_unpin and
        // registers every ReplyFuture's waker before attach drains.
        reactor.tick(false);

        reactor.attach_w2m(Rc::new(W2mReceiver::new(vec![ptr])));

        let inner = Rc::clone(&reactor.inner);
        let received_check = Rc::clone(&received);
        let outcome = reactor.block_on(async move {
            let timeout = TimerFuture::new(Instant::now() + Duration::from_secs(timeout_secs), inner);
            let watch = async move {
                while received_check.borrow().is_empty() {
                    YieldOnce::new().await;
                }
            };
            select2(watch, timeout).await
        });
        if let Either::B(()) = outcome {
            unsafe {
                libc::kill(pid, libc::SIGKILL);
            }
            panic!(
                "reactor stalled with {} replies received after {}s — \
                 lost-wake symptom",
                received.borrow().len(),
                timeout_secs,
            );
        }

        let mut ids = received.borrow().clone();
        assert_eq!(ids.len(), n_messages as usize);
        ids.sort();
        let expected: Vec<u64> = (1..=n_messages).collect();
        assert_eq!(ids, expected, "every published req_id must round-trip");

        let mut status: i32 = 0;
        unsafe {
            libc::waitpid(pid, &mut status, 0);
        }

        // AsyncCancel the in-flight FUTEX_WAITV before the storage drops.
        reactor.request_shutdown();
    }

    #[test]
    fn w2m_cross_process_stress_drains_all_messages_via_reactor() {
        w2m_cross_process_stress(500, 30);
    }

    /// The same run at 10x the volume: 5 000 messages through a 64 KiB ring
    /// forces dozens of SKIP-wraps and many writer-park/wake cycles, catching
    /// wake-protocol or virtual-cursor flakes that only appear at scale.
    #[test]
    fn w2m_cross_process_stress_high_volume() {
        w2m_cross_process_stress(5_000, 60);
    }

    /// This pin ISOLATES the refresh path: it drives
    /// `refresh_futex_waitv_vals` directly, with NO `tick()` and NO safety-net
    /// drain, so the store ordering is not masked, and asserts via a
    /// `#[cfg(test)]` order-witness probe that the flag publish was stamped
    /// strictly before the `reader_seq` snapshot.
    ///
    /// Single-threaded on purpose: the property is a store order inside one
    /// call, and a helper thread racing the refresh only made the *other* half
    /// of this (the unread-data check) nondeterministic. That half is
    /// [`refresh_reports_pending_when_data_is_unread`], where the publish
    /// simply precedes the refresh.
    ///
    /// Teeth: reordering the two operations (snapshot before `fetch_or`) flips
    /// the order stamps and fails the assert.
    #[test]
    fn refresh_publishes_flag_before_snapshotting_reader_seq() {
        use crate::runtime::w2m::W2mReceiver;
        use crate::runtime::w2m_ring;

        const CAPACITY: usize = 64 * 1024;

        let region = gnitz_engine_testkit::SharedRegion::new(CAPACITY);
        let ptr = region.ptr();
        unsafe {
            w2m_ring::init_region_for_tests(ptr, CAPACITY as u64);
        }

        // Only the state refresh needs — deliberately NOT `attach_w2m`, so no
        // drain loop, no SQE arm, no tick().
        let reactor = Reactor::new(16).expect("reactor");
        reactor
            .inner
            .w2m
            .set(Rc::new(W2mReceiver::new(vec![ptr])))
            .ok()
            .expect("w2m set");
        *reactor.inner.futex_waitv_storage.borrow_mut() = Some(vec![FutexWaitV::new()].into_boxed_slice());

        let _ = reactor.refresh_futex_waitv_vals();

        assert_eq!(
            park_order::step(),
            2,
            "FLAG_MASTER_PARKED must be published BEFORE reader_seq is \
             snapshotted; reordering them opens the lost-wake window",
        );
    }

    /// The other half of the park protocol: an unread publish must make
    /// `refresh_futex_waitv_vals` report pending, so the caller drains instead
    /// of arming a `FUTEX_WAITV` for a wake that already happened.
    ///
    /// The publish lands before the refresh, so there is no race to lose: the
    /// `write_cursor != read_cursor` check either runs or the code is wrong.
    #[test]
    fn refresh_reports_pending_when_data_is_unread() {
        use crate::runtime::w2m::{W2mReceiver, W2mWriter};
        use crate::runtime::w2m_ring;
        use crate::runtime::wire::STATUS_OK;

        const CAPACITY: usize = 64 * 1024;

        let region = gnitz_engine_testkit::SharedRegion::new(CAPACITY);
        let ptr = region.ptr();
        unsafe {
            w2m_ring::init_region_for_tests(ptr, CAPACITY as u64);
        }
        W2mWriter::new(ptr).send_status(0, 1u64, STATUS_OK, &[]);

        let reactor = Reactor::new(16).expect("reactor");
        reactor
            .inner
            .w2m
            .set(Rc::new(W2mReceiver::new(vec![ptr])))
            .ok()
            .expect("w2m set");
        *reactor.inner.futex_waitv_storage.borrow_mut() = Some(vec![FutexWaitV::new()].into_boxed_slice());

        assert!(
            reactor.refresh_futex_waitv_vals(),
            "an unread publish must be reported as pending — the caller would \
             otherwise arm a doomed wait (lost wake)",
        );
    }

    // ─────────────────────────────────────────────────────────────────
    // `closing_fds` sync: every path that sets conn.closing=true must
    // also insert into closing_fds so reap_closing_conns finds the fd.
    // ─────────────────────────────────────────────────────────────────

    #[test]
    fn handle_recv_cqe_error_populates_closing_fds() {
        let r = make_reactor();
        let conn = Box::new(io::Conn::new(Rc::clone(&r.inner.inbound)));
        r.inner.conns.borrow_mut().insert(55, conn);

        r.inject_cqe(KIND_RECV, 55, -1);

        assert!(
            r.inner.closing_fds.borrow().contains(&55),
            "res<=0 recv CQE must insert fd into closing_fds"
        );
        assert!(
            r.inner.conns.borrow().get(&55).unwrap().q.recv_closed(),
            "res<=0 recv CQE must mark the connection closed"
        );
    }

    #[test]
    fn reap_closing_conns_removes_idle_closing_fd() {
        let r = make_reactor();
        unsafe {
            let (read_end, write_end) = pipe_pair();

            let mut conn = Box::new(io::Conn::new(Rc::clone(&r.inner.inbound)));
            conn.closing = true;
            r.inner.conns.borrow_mut().insert(read_end, conn);
            r.inner.closing_fds.borrow_mut().insert(read_end);

            r.reap_closing_conns();

            assert!(
                !r.inner.conns.borrow().contains_key(&read_end),
                "idle closing conn must be removed from conns"
            );
            assert!(
                !r.inner.closing_fds.borrow().contains(&read_end),
                "reaped fd must be removed from closing_fds"
            );

            libc::close(write_end);
        }
    }

    #[test]
    fn reap_closing_conns_defers_conn_with_outstanding_send() {
        let r = make_reactor();
        let mut conn = Box::new(io::Conn::new(Rc::clone(&r.inner.inbound)));
        conn.closing = true;
        conn.send_inflight = 1; // outstanding send SQE
        r.inner.conns.borrow_mut().insert(77, conn);
        r.inner.closing_fds.borrow_mut().insert(77);

        r.reap_closing_conns();

        assert!(
            r.inner.conns.borrow().contains_key(&77),
            "conn with outstanding send must NOT be reaped yet"
        );
        assert!(
            r.inner.closing_fds.borrow().contains(&77),
            "conn deferred by outstanding send must stay in closing_fds"
        );
    }

    // ─────────────────────────────────────────────────────────────────
    // RecvState state-machine unit tests: its four transitions (NeedMore,
    // HeaderDone, MessageDone, Disconnect) driven directly, with no io_uring.
    // The policy `RecvQueue` layers on top is covered by the connection tests
    // above.
    // ─────────────────────────────────────────────────────────────────

    #[test]
    fn recv_state_partial_header_accumulates() {
        let mut rs = io::RecvState::new();
        // Feed 2 of 4 header bytes.
        assert!(matches!(rs.advance(2), io::RecvAdvance::NeedMore));
        let (_, rem) = rs.remaining();
        assert_eq!(rem, 2, "remaining must reflect the 2 consumed header bytes");
    }

    #[test]
    fn recv_state_zero_payload_len_disconnects() {
        let mut rs = io::RecvState::new();
        // hdr_buf is all-zeros → payload_len = 0 → protocol violation.
        assert!(matches!(rs.advance(4), io::RecvAdvance::Disconnect));
    }

    #[test]
    fn recv_state_payload_accumulates_then_message_done() {
        let mut rs = io::RecvState::new();
        rs.seed_header(8);
        assert!(matches!(rs.advance(4), io::RecvAdvance::HeaderDone));

        let buf = unsafe { libc::malloc(8) as *mut u8 };
        rs.start_payload(io::RecvBuf::new(buf, 8, Rc::new(io::InboundBudget::new(usize::MAX))));

        // Partial payload.
        assert!(matches!(rs.advance(5), io::RecvAdvance::NeedMore));
        // Remaining 3 bytes complete the message.
        assert!(matches!(rs.advance(3), io::RecvAdvance::MessageDone));

        // `take_message` yields the owning `RecvBuf` (frees on drop).
        let ret = rs.take_message();
        assert_eq!(ret.ptr, buf);
        assert_eq!(ret.len, 8);

        // After take_message the state must be back in header phase.
        let (_, rem) = rs.remaining();
        assert_eq!(rem, 4, "take_message must reset to header phase");
    }

    // ─────────────────────────────────────────────────────────────────
    // AsyncRwLock writer-preference: new readers blocked by a parked
    // writer.
    // ─────────────────────────────────────────────────────────────────

    /// When a write waiter is queued (writers_waiting > 0), ReadFuture
    /// must block. The writer acquires the lock before the new reader.
    #[test]
    fn async_rwlock_new_readers_blocked_by_waiting_writer() {
        let r = make_reactor();
        let lock: Rc<AsyncRwLock> = Rc::new(AsyncRwLock::new());
        let order: Rc<RefCell<Vec<&'static str>>> = Rc::new(RefCell::new(Vec::new()));

        // Task A: holds read lock, yields once.
        let l_a = Rc::clone(&lock);
        let o_a = Rc::clone(&order);
        r.spawn(async move {
            let _g = l_a.read().await;
            o_a.borrow_mut().push("R1");
            YieldOnce::new().await;
        });

        // Task B: writer — parks while A holds the read lock.
        let l_b = Rc::clone(&lock);
        let o_b = Rc::clone(&order);
        r.spawn(async move {
            let _g = l_b.write().await;
            o_b.borrow_mut().push("W");
        });

        // Task C: new reader — must be blocked by the waiting writer
        // (writer-preference) and only enter after B releases.
        let l_c = Rc::clone(&lock);
        let o_c = Rc::clone(&order);
        r.spawn(async move {
            let _g = l_c.read().await;
            o_c.borrow_mut().push("R2");
        });

        r.block_until_idle();
        let o = order.borrow().clone();
        let w_pos = o.iter().position(|&s| s == "W").expect("W not seen");
        let r2_pos = o.iter().position(|&s| s == "R2").expect("R2 not seen");
        assert!(
            w_pos < r2_pos,
            "writer-preference violated: W must precede R2, got {o:?}"
        );
    }

    /// WriteFuture::Drop path 3: readers hold the lock, the dropped
    /// WriteFuture was the LAST live write waiter. Pending readers
    /// blocked by `writers_waiting > 0` must be unblocked.
    #[test]
    fn async_rwlock_last_write_waiter_cancelled_unblocks_pending_readers() {
        let r = make_reactor();
        let lock: Rc<AsyncRwLock> = Rc::new(AsyncRwLock::new());
        let done: Rc<StdCell<bool>> = Rc::new(StdCell::new(false));

        // cancel channel: dropping the sender unblocks select2 in Task B.
        let (cancel_tx, cancel_rx) = oneshot::channel::<()>();

        // Task A: holds read lock for many ticks so B stays parked.
        let l_a = Rc::clone(&lock);
        r.spawn(async move {
            let _g = l_a.read().await;
            for _ in 0..10 {
                YieldOnce::new().await;
            }
        });

        // Task B: races write acquisition vs cancel_rx. WriteFuture parks
        // (writers_waiting=1); cancel_rx stays Pending until we drop cancel_tx.
        let l_b = Rc::clone(&lock);
        r.spawn(async move {
            let _ = select2(l_b.write(), cancel_rx).await;
        });

        // Task C: new reader; parks in read_waiters while B is alive.
        let l_c = Rc::clone(&lock);
        let d = Rc::clone(&done);
        r.spawn(async move {
            let _g = l_c.read().await;
            d.set(true);
        });

        // Let A, B, C all park (A acquires read, B parks write, C parks read).
        for _ in 0..5 {
            r.tick(false);
        }
        assert!(!done.get(), "C must be blocked while write waiter B is alive");

        // Cancel B: WriteFuture::Drop path 3 must wake C.
        drop(cancel_tx);
        for _ in 0..5 {
            r.tick(false);
        }
        assert!(done.get(), "C must unblock when the last write waiter (B) is cancelled");
    }

    // ─────────────────────────────────────────────────────────────────
    // KIND_ACCEPT CQE dispatch.
    // ─────────────────────────────────────────────────────────────────

    /// A real fd to stand in for a listener, so the `CQE_F_MORE == 0` re-arm
    /// these tests trigger targets something the kernel will accept.
    fn fake_listener() -> i32 {
        unsafe { pipe_pair().0 }
    }

    #[test]
    fn dispatch_accept_queues_the_connection_and_its_listener() {
        let r = make_reactor();
        // The listener fd rides the udata id and must round-trip into the
        // queued pair — it is the accept loop's unix-vs-tls routing key.
        let listener = fake_listener();
        r.inject_cqe(KIND_ACCEPT, listener as u64, 9);
        let q: Vec<(i32, i32)> = r.inner.accept_queue.borrow().iter().copied().collect();
        assert_eq!(
            q,
            vec![(9, listener)],
            "KIND_ACCEPT res>=0 must queue (conn_fd, listener_fd)"
        );
        unsafe { libc::close(listener) };
    }

    #[test]
    fn dispatch_accept_wakes_waiter_when_present() {
        let r = make_reactor();
        let listener = fake_listener();
        let waker = make_waker(42);
        *r.inner.accept_waker.borrow_mut() = Some(waker);

        r.inject_cqe(KIND_ACCEPT, listener as u64, 5);

        let q: Vec<usize> = r.inner.run_queue.borrow().queue.clone();
        assert!(q.contains(&42), "KIND_ACCEPT must wake the registered accept_waker");
        assert!(
            r.inner.accept_waker.borrow().is_none(),
            "KIND_ACCEPT must consume (take) the accept_waker"
        );
        unsafe { libc::close(listener) };
    }

    #[test]
    fn dispatch_accept_ignores_error_result() {
        let r = make_reactor();
        let listener = fake_listener();
        r.inject_cqe(KIND_ACCEPT, listener as u64, -libc::ECONNABORTED);
        assert!(
            r.inner.accept_queue.borrow().is_empty(),
            "KIND_ACCEPT with res<0 must not push to accept_queue"
        );
        unsafe { libc::close(listener) };
    }

    // ─────────────────────────────────────────────────────────────────
    // join_all_unpin edge cases.
    // ─────────────────────────────────────────────────────────────────

    #[test]
    fn join_all_unpin_empty_returns_empty_vec() {
        let r = make_reactor();
        let result = r.block_on(async { join_all_unpin(std::iter::empty::<std::future::Ready<i32>>()).await });
        assert!(
            result.is_empty(),
            "join_all_unpin on empty iterator must return empty vec"
        );
    }

    #[test]
    fn join_all_unpin_single_future_completes() {
        let r = make_reactor();
        let result = r.block_on(async { join_all_unpin(std::iter::once(std::future::ready(99u32))).await });
        assert_eq!(result, vec![99u32]);
    }

    // ─────────────────────────────────────────────────────────────────
    // mpsc::try_recv: non-blocking drain used by the committer.
    // ─────────────────────────────────────────────────────────────────

    #[test]
    fn mpsc_try_recv_drains_queue_without_blocking() {
        let (tx, mut rx) = mpsc::unbounded::<i32>();
        tx.send(10);
        tx.send(20);
        tx.send(30);
        assert_eq!(rx.try_recv(), Some(10));
        assert_eq!(rx.try_recv(), Some(20));
        assert_eq!(rx.try_recv(), Some(30));
        assert_eq!(rx.try_recv(), None, "queue must be empty after full drain");
    }

    // ─────────────────────────────────────────────────────────────────
    // alloc_request_id: bit 31 always clear; wraps at MAX_REGULAR_REQ_ID.
    // alloc_scan_request_id: bit 31 always set; wraps at u32::MAX.
    // ─────────────────────────────────────────────────────────────────

    #[test]
    fn alloc_request_id_wraps_within_regular_range() {
        let r = make_reactor();
        r.inner.next_request_id.set(MAX_REGULAR_REQ_ID - 1);
        let id1 = r.alloc_request_id();
        assert_eq!(id1, MAX_REGULAR_REQ_ID - 1);
        let id2 = r.alloc_request_id();
        assert_eq!(id2, MAX_REGULAR_REQ_ID);
        let id3 = r.alloc_request_id();
        assert_eq!(id3, 1, "must wrap to 1 after MAX_REGULAR_REQ_ID");
        assert!(id3 & SCAN_REQ_ID_FLAG as u64 == 0, "bit 31 must be clear");
    }

    #[test]
    fn alloc_scan_request_id_bit31_always_set() {
        let r = make_reactor();
        for _ in 0..1000 {
            let id = r.alloc_scan_request_id();
            assert!(id & SCAN_REQ_ID_FLAG as u64 != 0, "bit 31 must be set");
        }
    }

    #[test]
    fn regular_and_scan_ids_never_collide() {
        let r = make_reactor();
        let regular_ids: std::collections::HashSet<u64> = (0..10000).map(|_| r.alloc_request_id()).collect();
        let scan_ids: std::collections::HashSet<u64> = (0..10000).map(|_| r.alloc_scan_request_id()).collect();
        assert!(
            regular_ids.is_disjoint(&scan_ids),
            "regular and scan IDs must not overlap"
        );
    }

    // ─────────────────────────────────────────────────────────────────
    // route_scan_slot: park-before-wake ordering.
    //
    // The bug: the old impl woke the waker before parking the slot.
    // If a slot arrived before the future was first polled (no waker
    // yet registered), the slot was silently dropped and join_all
    // would hang forever.  The fix parks unconditionally, then wakes
    // if a waker is present — matching the behaviour of route_reply.
    // ─────────────────────────────────────────────────────────────────

    /// Build a minimal W2M ring and write one scan slot with `req_id`.
    /// The returned `W2mReceiver` owns the `InFlightState` that the slot's Drop
    /// references — it must be kept alive until after the slot is dropped.
    /// The returned `SharedRegion` unmaps the ring on drop.
    unsafe fn make_scan_slot(
        req_id: u32,
    ) -> (
        crate::runtime::w2m::W2mSlot,
        crate::runtime::w2m::W2mReceiver,
        gnitz_engine_testkit::SharedRegion,
    ) {
        let (receiver, region) = make_scan_ring(req_id, 1);
        let slot = receiver.try_read_slot(0).expect("scan slot");
        (slot, receiver, region)
    }

    #[test]
    fn route_scan_slot_parks_when_no_waker_registered() {
        // Slot arrives before any future is polled, but the scan IS active (a
        // ScanLease is held): the frame must be queued, not dropped or
        // overwritten.
        let r = make_reactor();
        let req_id = r.alloc_scan_request_id() as u32;
        let (slot, _recv, _region) = unsafe { make_scan_slot(req_id) };

        let _lease = r.scan_lease(&[req_id]);
        r.test_route_scan_slot(slot);

        assert!(
            r.inner.scans.borrow().contains_key(&req_id),
            "slot must be queued when no waker is registered for an active scan"
        );
        assert_eq!(
            r.inner.scans.borrow().get(&req_id).map(|s| s.queue.len()),
            Some(1),
            "exactly one frame queued"
        );
        assert!(
            r.inner.scans.borrow().values().all(|s| s.waker.is_none()),
            "no waker should have been inserted"
        );

        r.inner
            .scans
            .borrow_mut()
            .get_mut(&req_id)
            .map(|s| std::mem::take(&mut s.queue)); // drop slot, advance consume_cursor
    }

    #[test]
    fn await_scan_slot_resolves_immediately_when_slot_preloaded() {
        // The bug scenario: slot arrives and is parked BEFORE the future is
        // first polled. The first poll must return Poll::Ready, not Poll::Pending.
        let r = make_reactor();
        let req_id = r.alloc_scan_request_id() as u32;
        let (slot, _recv, _region) = unsafe { make_scan_slot(req_id) };

        let _lease = r.scan_lease(&[req_id]);
        r.test_route_scan_slot(slot); // park before any poll

        let fut = r.await_scan_slot(req_id);
        let result = r.block_on(fut);
        assert_eq!(
            result.internal_req_id, req_id,
            "first poll must return the pre-parked slot"
        );

        drop(result); // advance consume_cursor before the region unmaps
    }

    #[test]
    fn await_scan_slot_resolves_after_route_fires_waker() {
        // Normal path: future polled first (registers waker), then slot arrives.
        use std::cell::Cell;
        let r = make_reactor();
        let req_id = r.alloc_scan_request_id() as u32;
        let (slot, _recv, _region) = unsafe { make_scan_slot(req_id) };

        let _lease = r.scan_lease(&[req_id]);
        let delivered: Rc<Cell<bool>> = Rc::new(Cell::new(false));
        let delivered2 = Rc::clone(&delivered);
        let fut = r.await_scan_slot(req_id);
        r.spawn(async move {
            let s = fut.await;
            assert_eq!(s.internal_req_id, req_id);
            delivered2.set(true);
        });

        r.tick(false); // poll task → Poll::Pending, waker registered
        assert!(
            r.inner.scans.borrow()[&req_id].waker.is_some(),
            "waker must be registered after first poll"
        );
        assert!(!delivered.get(), "must not be delivered yet");

        r.test_route_scan_slot(slot); // park + wake
        r.tick(false); // task woken → Poll::Ready

        assert!(delivered.get(), "slot must be delivered after route_scan_slot");
    }

    // ─────────────────────────────────────────────────────────────────
    // Cancel-safe + lossless scan path (Fix B): per-req_id frame queue,
    // active-scan gate, and ScanLease lifecycle.
    // ─────────────────────────────────────────────────────────────────

    /// Build a W2M ring carrying `n` scan frames, all tagged with
    /// `internal_req_id` but with distinct wire request_ids (100, 101, …) so the
    /// caller can verify arrival order. The returned `W2mReceiver` owns the
    /// `InFlightState` the slots reference; caller reads frames via
    /// `receiver.try_read_slot(0)`; the returned `SharedRegion` unmaps the ring
    /// on drop, after both receiver and all slots drop.
    unsafe fn make_scan_ring(
        internal_req_id: u32,
        n: usize,
    ) -> (crate::runtime::w2m::W2mReceiver, gnitz_engine_testkit::SharedRegion) {
        use crate::runtime::w2m::{W2mReceiver, W2mWriter};
        use crate::runtime::w2m_ring;
        use crate::runtime::wire as ipc;

        const CAPACITY: usize = 64 * 1024;
        let region = gnitz_engine_testkit::SharedRegion::new(CAPACITY);
        let ptr = region.ptr();
        w2m_ring::init_region_for_tests(ptr, CAPACITY as u64);

        let writer = W2mWriter::new(ptr);
        let receiver = W2mReceiver::new(vec![ptr]);
        for i in 0..n {
            let wire_req = 100u64 + i as u64;
            let msg = ipc::WireMsg {
                request_id: wire_req,
                ..Default::default()
            };
            writer.send_encoded(msg.size(), internal_req_id, |buf| {
                msg.encode_ipc(buf, 0);
            });
        }
        (receiver, region)
    }

    /// Failure mode 4: concurrently-streamed continuation frames for one
    /// req_id must be queued in arrival order, not overwritten (the old
    /// a single-value slot dropped all but the last).
    #[test]
    fn scan_queue_retains_continuation_frames_in_order() {
        let r = make_reactor();
        let req_id = r.alloc_scan_request_id() as u32;
        let (recv, _region) = unsafe { make_scan_ring(req_id, 2) };
        let _lease = r.scan_lease(&[req_id]);

        // Route two frames for the same req_id while no awaiter is registered.
        let s0 = recv.try_read_slot(0).expect("frame 0");
        let s1 = recv.try_read_slot(0).expect("frame 1");
        r.test_route_scan_slot(s0);
        r.test_route_scan_slot(s1);
        assert_eq!(
            r.inner.scans.borrow().get(&req_id).map(|s| s.queue.len()),
            Some(2),
            "both frames must be queued, not overwritten"
        );

        // await twice → frames returned in arrival order (100 then 101).
        let f0 = r.block_on(r.await_scan_slot(req_id));
        let rid0 = wire::peek_control_block_ipc(f0.bytes()).unwrap().request_id;
        drop(f0);
        let f1 = r.block_on(r.await_scan_slot(req_id));
        let rid1 = wire::peek_control_block_ipc(f1.bytes()).unwrap().request_id;
        drop(f1);
        assert_eq!(
            (rid0, rid1),
            (100, 101),
            "queued continuation frames must be returned in arrival order"
        );
        assert!(
            r.inner.scans.borrow().values().all(|s| s.queue.is_empty()),
            "queue emptied after both frames consumed"
        );

        drop(_lease);
        drop(recv);
    }

    /// The per-ring in-flight tracker is dynamic: routing more continuation
    /// frames for one req_id than the old fixed 64-slot ceiling must queue them
    /// all and deliver them in order — the fixed `InFlightState` aborted on the
    /// 65th, and `route_scan_slot`'s old `debug_assert` fired at 64.
    #[test]
    fn scan_queue_exceeds_legacy_64_slot_ceiling() {
        let r = make_reactor();
        let req_id = r.alloc_scan_request_id() as u32;
        const N: usize = 100; // > 64
        let (recv, _region) = unsafe { make_scan_ring(req_id, N) };
        let _lease = r.scan_lease(&[req_id]);

        for _ in 0..N {
            let s = recv.try_read_slot(0).expect("frame");
            r.test_route_scan_slot(s);
        }
        assert_eq!(
            r.inner.scans.borrow().get(&req_id).map(|s| s.queue.len()),
            Some(N),
            "all {N} frames queued past the legacy 64 ceiling — none dropped or aborted"
        );

        for i in 0..N {
            let f = r.block_on(r.await_scan_slot(req_id));
            let rid = wire::peek_control_block_ipc(f.bytes()).unwrap().request_id;
            assert_eq!(rid, 100 + i as u64, "frame {i} delivered in arrival order");
            drop(f);
        }
        assert!(
            r.inner.scans.borrow().values().all(|s| s.queue.is_empty()),
            "queue emptied after drain"
        );

        drop(_lease);
        drop(recv);
    }

    /// Dropping the `ScanLease` purges the parked queue (dropping each queued
    /// `W2mSlot` advances `consume_cursor`) and deregisters the active scan.
    #[test]
    fn scan_lease_drop_frees_queued_slots() {
        use std::sync::atomic::Ordering;
        let r = make_reactor();
        let req_id = r.alloc_scan_request_id() as u32;
        let (recv, _region) = unsafe { make_scan_ring(req_id, 1) };
        let lease = r.scan_lease(&[req_id]);

        let s0 = recv.try_read_slot(0).expect("frame 0");
        r.test_route_scan_slot(s0); // queued (active)
        assert!(r.inner.scans.borrow().contains_key(&req_id));

        let cc_before = unsafe { recv.header(0) }.consume_cursor().load(Ordering::Acquire);
        drop(lease); // purge parked queue → drop queued slot → advance consume_cursor
        assert!(
            r.inner.scans.borrow().values().all(|s| s.queue.is_empty()),
            "lease drop purges parked queue"
        );
        assert!(
            r.inner.scans.borrow().values().all(|s| s.waker.is_none()),
            "lease drop purges wakers"
        );
        assert!(r.inner.scans.borrow().is_empty(), "lease drop deregisters active scan");
        let cc_after = unsafe { recv.header(0) }.consume_cursor().load(Ordering::Acquire);
        assert!(cc_after > cc_before, "dropped queued slot must advance consume_cursor");

        drop(recv);
    }

    /// Failure mode 1: a frame whose scan has no live lease is discarded
    /// (freeing ring space), not parked — so the still-streaming worker never
    /// wedges on a full ring.
    #[test]
    fn abandoned_scan_frame_is_discarded_not_parked() {
        use std::sync::atomic::Ordering;
        let r = make_reactor();
        let req_id = r.alloc_scan_request_id() as u32;
        let (recv, _region) = unsafe { make_scan_ring(req_id, 1) };

        // No lease held: route_scan_slot must drop the slot, not park it.
        let s0 = recv.try_read_slot(0).expect("frame 0");
        let cc_before = unsafe { recv.header(0) }.consume_cursor().load(Ordering::Acquire);
        r.test_route_scan_slot(s0); // dropped here (inactive)
        assert!(
            r.inner.scans.borrow().values().all(|s| s.queue.is_empty()),
            "abandoned-scan frame must be discarded, not parked"
        );
        let cc_after = unsafe { recv.header(0) }.consume_cursor().load(Ordering::Acquire);
        assert!(
            cc_after > cc_before,
            "discarded slot must advance consume_cursor (ring freed)"
        );

        drop(recv);
    }

    /// End-to-end Failure mode 1 guard: a worker streams more frames than the
    /// ring holds; the master takes a lease, queues 2 (filling the ring and
    /// parking the writer), then drops the lease mid-train. The freed slots +
    /// the gate discarding every later frame must let the writer finish all its
    /// writes instead of wedging in `send_encoded`.
    #[test]
    fn dropped_scan_lease_unblocks_streaming_writer() {
        use crate::runtime::w2m::{W2mReceiver, W2mWriter};
        use crate::runtime::w2m_ring::{self, W2M_HEADER_SIZE};
        use crate::runtime::wire as ipc;
        use gnitz_wire::align8;
        use std::sync::atomic::Ordering;
        use std::time::{Duration, Instant};

        const TOTAL: usize = 8;

        // Ring sized for exactly 2 small frames.
        let msg_total = 8 + align8(ipc::WireMsg::default().size()) as u64;
        let capacity = W2M_HEADER_SIZE as u64 + 2 * msg_total + 8;
        let region = gnitz_engine_testkit::SharedRegion::new(capacity as usize);
        let ptr = region.ptr();
        unsafe { w2m_ring::init_region_for_tests(ptr, capacity) };

        let r = make_reactor();
        let req_id = r.alloc_scan_request_id() as u32;
        let writer = W2mWriter::new(ptr);
        let receiver = W2mReceiver::new(vec![ptr]);

        let (done_tx, done_rx) = std::sync::mpsc::channel::<()>();
        let handle = std::thread::spawn(move || {
            for _ in 0..TOTAL {
                let msg = ipc::WireMsg::default();
                writer.send_encoded(msg.size(), req_id, |buf| {
                    msg.encode_ipc(buf, 0);
                });
            }
            let _ = done_tx.send(());
        });

        let lease = r.scan_lease(&[req_id]);

        // Read+queue 2 frames (active) — fills the ring, parking the writer.
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut queued = 0;
        while queued < 2 {
            if let Some(slot) = receiver.try_read_slot(0) {
                r.test_route_scan_slot(slot);
                queued += 1;
            } else if Instant::now() > deadline {
                panic!("writer never produced the first 2 frames");
            } else {
                std::thread::sleep(Duration::from_millis(1));
            }
        }

        // Drop the lease mid-train: queued slots free, later frames are gated.
        drop(lease);

        let mut read = queued;
        while read < TOTAL {
            if let Some(slot) = receiver.try_read_slot(0) {
                r.test_route_scan_slot(slot); // discarded (lease gone)
                read += 1;
            } else if Instant::now() > deadline {
                panic!("scan lease drop failed to free the ring — writer wedged at {read}/{TOTAL}");
            } else {
                std::thread::sleep(Duration::from_millis(1));
            }
        }

        done_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("writer thread must finish — never wedge on a full ring");
        handle.join().expect("writer thread panicked");

        let hdr = unsafe { receiver.header(0) };
        assert_eq!(
            hdr.consume_cursor().load(Ordering::Acquire),
            hdr.write_cursor().load(Ordering::Acquire),
            "every emitted slot must be freed (consume_cursor reaches write_cursor)",
        );

        drop(receiver);
    }

    // ─────────────────────────────────────────────────────────────────
    // Cancellation. Abandoning a park slot is the one mechanism behind
    // all of these: the slot's presence is the op's liveness, so a late
    // completion can always tell "deliver" from "the awaiter is gone".
    // ─────────────────────────────────────────────────────────────────

    /// A dropped `ReplyFuture` abandons its slot, so a late reply hits
    /// route_reply's unrouted arm (logged + dropped) instead of parking behind
    /// a dead waker forever.
    #[test]
    fn dropped_reply_future_discards_a_late_reply() {
        let r = make_reactor();
        let req_id = 4242u64;
        {
            let mut fut = Box::pin(r.await_reply(req_id));
            let w = make_waker(1);
            let mut cx = Context::from_waker(&w);
            assert!(fut.as_mut().poll(&mut cx).is_pending(), "no reply parked yet");
            assert!(r.inner.replies.has_waker(req_id), "poll must register the waker");
        } // fut dropped → slot abandoned
        assert!(!r.inner.replies.is_open(req_id), "drop must retire the slot");

        r.test_route_reply(0, synthetic_decoded_wire(req_id));
        assert_eq!(
            r.inner.replies.len(),
            0,
            "a late reply for a dropped future must not be parked"
        );
    }

    /// A dropped `AcceptFuture` must clear `accept_waker`.
    #[test]
    fn dropped_accept_future_leaves_no_stale_waker() {
        let r = make_reactor();
        {
            let mut fut = Box::pin(r.accept());
            let w = make_waker(1);
            let mut cx = Context::from_waker(&w);
            assert!(fut.as_mut().poll(&mut cx).is_pending());
            assert!(r.inner.accept_waker.borrow().is_some(), "poll registers accept_waker");
        }
        assert!(r.inner.accept_waker.borrow().is_none(), "drop must clear accept_waker");
    }

    /// A `FsyncFuture` dropped while pending leaves its slot abandoned, so the
    /// late KIND_FSYNC retires it instead of leaking a result nobody collects.
    #[test]
    fn dropped_fsync_future_discards_a_late_cqe() {
        let r = make_reactor();
        let id = 7001u64;
        r.inner.fsyncs.open(id, None);
        {
            let mut fut = Box::pin(FsyncFuture {
                id,
                inner: Rc::clone(&r.inner),
            });
            let w = make_waker(1);
            let mut cx = Context::from_waker(&w);
            assert!(fut.as_mut().poll(&mut cx).is_pending(), "no result yet");
            assert!(r.inner.fsyncs.has_waker(id));
        } // drop while pending → abandoned
        assert!(r.inner.fsyncs.is_abandoned(id), "drop while pending must abandon");
        assert!(!r.inner.fsyncs.has_waker(id), "drop must withdraw the waker");

        r.inject_cqe(KIND_FSYNC, id, 0);
        assert_eq!(r.inner.fsyncs.len(), 0, "the late CQE must retire the slot");
    }

    /// A CQE arriving before the drop is reclaimed by `Drop`, leaving nothing
    /// behind — the case that made a hand-rolled tombstone set grow one entry
    /// per durable commit.
    #[test]
    fn dropped_fsync_future_reclaims_an_already_parked_result() {
        let r = make_reactor();
        let id = 7002u64;
        r.inner.fsyncs.open(id, None);
        let mut fut = Box::pin(FsyncFuture {
            id,
            inner: Rc::clone(&r.inner),
        });
        let w = make_waker(1);
        let mut cx = Context::from_waker(&w);
        assert!(fut.as_mut().poll(&mut cx).is_pending());

        r.inject_cqe(KIND_FSYNC, id, 0); // result parked before the drop

        drop(fut);
        assert_eq!(r.inner.fsyncs.len(), 0, "drop must reclaim the orphaned result");
    }

    /// The success path leaves nothing behind either: `poll` retires the slot,
    /// so `Drop` finds nothing to abandon.
    #[test]
    fn resolved_fsync_future_leaves_no_slot() {
        let r = make_reactor();
        let id = 7003u64;
        r.inner.fsyncs.open(id, None);
        let mut fut = Box::pin(FsyncFuture {
            id,
            inner: Rc::clone(&r.inner),
        });
        let w = make_waker(1);
        let mut cx = Context::from_waker(&w);

        r.inject_cqe(KIND_FSYNC, id, 0); // park result, then resolve from it
        assert_eq!(fut.as_mut().poll(&mut cx), Poll::Ready(0));
        drop(fut);
        assert_eq!(r.inner.fsyncs.len(), 0);
    }
}
