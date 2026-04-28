//! Single-threaded io_uring reactor.
//!
//! Provides `block_on`, `spawn`, `timer`, and a reply-routing API
//! (`await_reply` / `register_reply_waker`). The reactor owns its own
//! `IoUringRing`, separate from the executor's transport ring.
//!
//! Design notes:
//!
//! - Run queue is `Arc<Mutex<VecDeque<usize>>>`. The `Waker` vtable
//!   requires `Send + Sync`; the master process is single-threaded so
//!   the mutex never contends, but its presence is API-forced.
//! - Timers are `io_uring Timeout` SQEs. `TimerFuture::poll` submits a
//!   `prep_timeout` SQE on first poll; the resulting CQE wakes the
//!   registered waker. A cancelled timer's CQE is silently discarded by
//!   `dispatch_cqe`. `IoUringRing::timer_specs` keys each `Box<Timespec>`
//!   by the SQE's `user_data` and drops it when the matching CQE is
//!   drained, so the map size is bounded by in-flight timers.
//! - CQE `user_data` packs an 8-bit kind tag in the high byte and a
//!   56-bit id in the low bits, where id is a request/timer/send id (not
//!   an fd). Safe from collisions because the reactor owns its own ring.

use std::cell::{Cell, OnceCell, RefCell};
use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};
use std::time::Instant;

use self::ring::{Cqe, Ring, CQE_F_MORE};
use self::uring::IoUringRing;

use crate::runtime::wire::DecodedWire;
use crate::runtime::sal::FLAG_EXCHANGE;
use crate::runtime::w2m::W2mReceiver;
use crate::runtime::sys::FUTEX2_SIZE_U32;
use crate::runtime::w2m_ring::FLAG_MASTER_PARKED;

use io_uring::types::FutexWaitV;


pub mod io;
mod ring;
mod uring;
mod exchange;
pub mod sync;

pub use exchange::{ExchangeAccumulator, PendingRelay};
pub use sync::{AsyncMutex, AsyncRwLock, Either, join_all, oneshot, mpsc, select2};
#[cfg(test)]
pub use sync::join2;

// ---------------------------------------------------------------------------
// CQE user_data encoding (high 8 bits = kind, low 56 bits = id)
// ---------------------------------------------------------------------------

pub const KIND_REPLY:        u64 = 1;
pub const KIND_TIMEOUT:      u64 = 2;
pub const KIND_FSYNC:        u64 = 3;
pub const KIND_FUTEX_WAITV:  u64 = 4;
pub const KIND_ACCEPT:       u64 = 5;
pub const KIND_RECV:         u64 = 6;
pub const KIND_SEND:         u64 = 7;
pub const KIND_FUTEX_CANCEL: u64 = 8;

const KIND_SHIFT: u64 = 56;
const ID_MASK:    u64 = 0x00FF_FFFF_FFFF_FFFF;

#[inline]
pub const fn udata(kind: u64, id: u64) -> u64 {
    (kind << KIND_SHIFT) | (id & ID_MASK)
}

#[inline]
pub const fn udata_kind(u: u64) -> u64 { u >> KIND_SHIFT }

#[inline]
pub const fn udata_id(u: u64) -> u64 { u & ID_MASK }

// ---------------------------------------------------------------------------
// Reactor
// ---------------------------------------------------------------------------

/// Backing store for one async task.
struct Task {
    future: Pin<Box<dyn Future<Output = ()>>>,
}

struct ReactorShared {
    ring: RefCell<IoUringRing>,
    /// Live tasks keyed by a monotonically-increasing id. HashMap (not
    /// `slab::Slab`) because same-key reinsertion is load-bearing: a
    /// task's future may spawn new tasks during its poll, and we must
    /// reinsert the running task at its original key so the waker hits
    /// the right entry on the next wake.
    tasks: RefCell<HashMap<usize, Task>>,
    next_task_key: Cell<usize>,
    /// Tasks whose wakers fired; the reactor's main loop polls them on
    /// the next tick. Wrapped in `Arc<Mutex<…>>` because the waker vtable
    /// requires `Send + Sync` even though we never cross threads.
    run_queue: Arc<Mutex<VecDeque<usize>>>,
    /// Per-task waker; stored so spawning code does not need to rebuild
    /// it on each poll. Indexed by task key.
    task_wakers: RefCell<HashMap<usize, Waker>>,
    /// Reply wakers keyed by request_id; populated by `await_reply`.
    reply_wakers: RefCell<HashMap<u64, Waker>>,
    parked_replies: RefCell<HashMap<u64, DecodedWire>>,
    /// Fsync wakers + parked results. An async fsync registers its waker
    /// here; when the CQE arrives the result is parked and the waker
    /// fires.
    fsync_wakers: RefCell<HashMap<u64, Waker>>,
    parked_fsync_results: RefCell<HashMap<u64, i32>>,
    w2m: OnceCell<W2mReceiver>,
    /// Pointer-stable storage for the reactor's persistent
    /// `FUTEX_WAITV` SQE. The kernel dereferences this array
    /// asynchronously, so it must outlive the SQE. A single SQE covers
    /// every worker's `reader_seq` word; we own it on the heap, and
    /// tear it down only after cancelling the SQE in
    /// `request_shutdown` and awaiting the `-ECANCELED` CQE.
    futex_waitv_storage: RefCell<Option<Box<[FutexWaitV]>>>,
    /// True while an outstanding `FUTEX_WAITV` SQE exists whose
    /// `FutexWaitV` array lives in `futex_waitv_storage`. Dropping the
    /// storage without clearing this flag is a use-after-free hazard.
    futex_waitv_armed: Cell<bool>,
    /// Signal from `request_shutdown` → `dispatch_cqe` that the
    /// cancellation CQE for the FUTEX_WAITV SQE has arrived and the
    /// storage can be dropped.
    futex_waitv_cancelled: Cell<bool>,
    /// Per-timer wakers keyed by timer_id. Each entry holds the waker to
    /// fire when the io_uring Timeout CQE arrives and a cancellation flag
    /// shared with the owning TimerFuture. If the future is dropped before
    /// the CQE, the flag is set and dispatch_cqe silently discards it.
    #[allow(clippy::type_complexity)]
    timer_wakers: RefCell<HashMap<u64, (Waker, Rc<Cell<bool>>)>>,
    next_timer_id: Cell<u64>,
    #[cfg(test)]
    injected_cqes: RefCell<VecDeque<Cqe>>,
    next_request_id: Cell<u64>,
    next_send_id: Cell<u64>,
    /// Per-fd connection state (recv decoder + send queue). Boxed so the
    /// inline hdr buffer address survives HashMap resizes — io_uring SQEs
    /// capture the pointer.
    conns: RefCell<HashMap<i32, Box<io::Conn>>>,
    /// Listen socket fd; set by `attach_server_fd`. The reactor submits a
    /// single `AcceptMulti` SQE on attach and re-arms on cancellation.
    server_fd: Cell<i32>,
    /// Accept queue: fds delivered by the kernel but not yet claimed by
    /// an `accept().await` caller. The accept loop task drains this.
    accept_queue: RefCell<VecDeque<i32>>,
    accept_waker: RefCell<Option<Waker>>,
    /// Recv waiters per fd: set when a task has called `recv(fd)` and is
    /// awaiting a complete message. Only one waiter per fd — per-connection
    /// FIFO is a correctness requirement (I2).
    recv_waiters: RefCell<HashMap<i32, Waker>>,
    /// Completed recv messages awaiting pickup by a `recv().await` call.
    /// A VecDeque per fd — multiple messages can queue if the handler
    /// is slow. Load-bearing for TCP flow control: with a single slot,
    /// pipelined clients deadlock once the kernel socket buffer fills
    /// (we can only drain messages one-at-a-time from the handler,
    /// while the kernel blocks the client's send).
    pending_recv: RefCell<HashMap<i32, std::collections::VecDeque<(*mut u8, usize)>>>,
    /// Closed-fd sentinel: set to true when a recv CQE arrived with
    /// res<=0 or the connection was forcibly closed. A subsequent
    /// `recv()` returns `None`.
    recv_closed: RefCell<HashMap<i32, bool>>,
    /// Send wakers keyed by send_id. `(send_id → fd)` lives separately
    /// so the CQE handler can decrement `Conn::send_inflight` even when
    /// no waker has been installed yet.
    send_wakers: RefCell<HashMap<u64, Waker>>,
    send_fd_for_id: RefCell<HashMap<u64, i32>>,
    /// Buffers stashed here outlive their owning `SendFuture` when it is
    /// dropped before the CQE arrives. The CQE handler removes the entry.
    send_buffers_in_flight: RefCell<HashMap<u64, Rc<Vec<u8>>>>,
    /// Send results parked before their waker was installed (same pattern
    /// as parked_replies / parked_fsync_results).
    parked_send_results: RefCell<HashMap<u64, i32>>,
    /// Shutdown flag. `block_until_shutdown` polls until this is set.
    shutdown: Cell<bool>,
    /// FLAG_EXCHANGE accumulator: when route_reply sees an exchange wire,
    /// it feeds it here. Once the accumulator has heard from every
    /// worker for a view_id, it produces a PendingRelay and the reactor
    /// dispatches it to the relay task via `relay_tx`.
    exchange_acc: RefCell<ExchangeAccumulator>,
    relay_tx: RefCell<Option<mpsc::Sender<PendingRelay>>>,
    /// Fds that have been marked closing via `close_fd`. `reap_closing_conns`
    /// iterates only this set (O(closing)) rather than all connections (O(all)).
    closing_fds: RefCell<std::collections::HashSet<i32>>,
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
    use std::sync::Once;
    static PROBED: Once = Once::new();
    PROBED.call_once(probe_futex_waitv_support_inner);
}

fn probe_futex_waitv_support_inner() {
    use std::sync::atomic::AtomicU32;
    use io_uring::{opcode, IoUring};

    let atomic = Box::new(AtomicU32::new(42));
    let futexv: Box<[FutexWaitV; 1]> = Box::new([
        FutexWaitV::new()
            .val(0)
            .uaddr(&*atomic as *const AtomicU32 as u64)
            .flags(FUTEX2_SIZE_U32),
    ]);

    let mut ring = match IoUring::new(8) {
        Ok(r) => r,
        Err(e) => crate::gnitz_fatal_abort!(
            "reactor: probe io_uring init failed: {}", e,
        ),
    };
    let entry = opcode::FutexWaitV::new(futexv.as_ptr(), 1)
        .build()
        .user_data(0xFEEDu64);
    if unsafe { ring.submission().push(&entry) }.is_err() {
        crate::gnitz_fatal_abort!("reactor: probe SQE push failed");
    }
    if let Err(e) = ring.submitter().submit_and_wait(1) {
        crate::gnitz_fatal_abort!("reactor: probe submit_and_wait failed: {}", e);
    }
    let cqe = match ring.completion().next() {
        Some(c) => c,
        None => crate::gnitz_fatal_abort!("reactor: probe produced no CQE"),
    };
    let res = cqe.result();
    if res == -libc::ENOSYS || res == -libc::EINVAL {
        crate::gnitz_fatal_abort!(
            "reactor: io_uring IORING_OP_FUTEX_WAITV not supported (res={}); \
             Linux 6.7+ required for the W2M tail-chasing-ring transport.",
            res,
        );
    }
    drop(futexv);
    drop(atomic);
}

impl Reactor {
    pub fn new(ring_capacity: u32) -> std::io::Result<Self> {
        probe_futex_waitv_support();
        let ring = IoUringRing::new(ring_capacity)?;
        let inner = Rc::new(ReactorShared {
            ring: RefCell::new(ring),
            tasks: RefCell::new(HashMap::new()),
            next_task_key: Cell::new(0),
            run_queue: Arc::new(Mutex::new(VecDeque::new())),
            task_wakers: RefCell::new(HashMap::new()),
            reply_wakers: RefCell::new(HashMap::new()),
            parked_replies: RefCell::new(HashMap::new()),
            fsync_wakers: RefCell::new(HashMap::new()),
            parked_fsync_results: RefCell::new(HashMap::new()),
            w2m: OnceCell::new(),
            futex_waitv_storage: RefCell::new(None),
            futex_waitv_armed: Cell::new(false),
            futex_waitv_cancelled: Cell::new(false),
            timer_wakers: RefCell::new(HashMap::new()),
            next_timer_id: Cell::new(0),
            #[cfg(test)]
            injected_cqes: RefCell::new(VecDeque::new()),
            next_request_id: Cell::new(1),
            next_send_id: Cell::new(1),
            conns: RefCell::new(HashMap::new()),
            server_fd: Cell::new(-1),
            accept_queue: RefCell::new(VecDeque::new()),
            accept_waker: RefCell::new(None),
            recv_waiters: RefCell::new(HashMap::new()),
            pending_recv: RefCell::new(HashMap::new()),
            recv_closed: RefCell::new(HashMap::new()),
            send_wakers: RefCell::new(HashMap::new()),
            send_fd_for_id: RefCell::new(HashMap::new()),
            send_buffers_in_flight: RefCell::new(HashMap::new()),
            parked_send_results: RefCell::new(HashMap::new()),
            shutdown: Cell::new(false),
            exchange_acc: RefCell::new(ExchangeAccumulator::new(0)),
            relay_tx: RefCell::new(None),
            closing_fds: RefCell::new(std::collections::HashSet::new()),
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
    pub fn request_shutdown(&self) {
        self.inner.shutdown.set(true);
        self.cancel_futex_waitv_and_wait();
        if let Some(w) = self.inner.task_wakers.borrow().values().next().cloned() {
            w.wake();
        }
    }

    /// If a `FUTEX_WAITV` SQE is armed, submit an `AsyncCancel` against
    /// it and drive the ring until the `futex_waitv_cancelled` flag
    /// flips. Without this, dropping the reactor's `FutexWaitV` array
    /// while the SQE still holds a pointer into it is a UAF in the
    /// kernel.
    ///
    /// If the cancel CQE does not arrive within 2 s we abort instead of
    /// freeing the storage: the kernel still holds a pointer into it
    /// and freeing now would be a use-after-free. A 2 s wait for an
    /// `AsyncCancel` + `FutexWaitV` pair is already pathological, so
    /// aborting is strictly safer than the previous behavior of
    /// silently dropping armed storage.
    fn cancel_futex_waitv_and_wait(&self) {
        if !self.inner.futex_waitv_armed.get() {
            return;
        }
        let target = udata(KIND_FUTEX_WAITV, 0);
        let cancel_udata = udata(KIND_FUTEX_CANCEL, 0);
        {
            let mut ring = self.inner.ring.borrow_mut();
            ring.prep_async_cancel(target, cancel_udata);
            let _ = ring.submit_and_wait_timeout(0, 0);
        }
        // Pump the ring until both the cancelled FUTEX_WAITV CQE (which
        // clears `futex_waitv_armed`) and the cancel-CQE (which sets
        // `futex_waitv_cancelled`) have been drained.
        let deadline = Instant::now() + std::time::Duration::from_millis(2000);
        while (self.inner.futex_waitv_armed.get() || !self.inner.futex_waitv_cancelled.get())
            && Instant::now() < deadline
        {
            self.drain_cqes_into_wakers();
            // Re-check before blocking: drain may have received both CQEs in
            // one pass, making the 100 ms wait unnecessary.
            if !self.inner.futex_waitv_armed.get() && self.inner.futex_waitv_cancelled.get() {
                break;
            }
            let _ = self.inner.ring.borrow_mut()
                .submit_and_wait_timeout(1, 100);
        }
        if self.inner.futex_waitv_armed.get() || !self.inner.futex_waitv_cancelled.get() {
            crate::gnitz_fatal_abort!(
                "reactor: FUTEX_WAITV cancel did not complete within 2s \
                 (armed={}, cancelled={}) — freeing storage now would be a UAF",
                self.inner.futex_waitv_armed.get(),
                self.inner.futex_waitv_cancelled.get(),
            );
        }
        // Safe to drop now: no in-flight SQE references the storage.
        *self.inner.futex_waitv_storage.borrow_mut() = None;
    }

    /// Allocate a new master-side request_id. Strictly monotonic; values
    /// 0 and `u64::MAX` are reserved (untagged, broadcast).
    pub fn alloc_request_id(&self) -> u64 {
        let id = self.inner.next_request_id.get();
        // Skip the reserved sentinels on overflow / wrap.
        let next = match id.checked_add(1) {
            Some(n) if n != u64::MAX => n,
            _ => 1,
        };
        self.inner.next_request_id.set(next);
        id
    }

    /// Spawn a task that runs detached. Returns the task key (useful for
    /// tests that want to assert task lifecycle).
    pub fn spawn(&self, fut: impl Future<Output = ()> + 'static) -> usize {
        let key = self.inner.next_task_key.get();
        self.inner.next_task_key.set(key + 1);
        let task = Task { future: Box::pin(fut) };
        self.inner.tasks.borrow_mut().insert(key, task);
        let waker = make_waker(key, Arc::clone(&self.inner.run_queue));
        self.inner.task_wakers.borrow_mut().insert(key, waker);
        // Schedule immediate first poll.
        self.inner.run_queue.lock().unwrap().push_back(key);
        key
    }

    /// Drive `fut` to completion. Single-threaded, blocking. Spawns the
    /// future as a task internally and returns its output via a shared cell.
    #[cfg(test)]
    pub fn block_on<F, T>(&self, fut: F) -> T
    where
        F: Future<Output = T> + 'static,
        T: 'static,
    {
        let out: Rc<RefCell<Option<T>>> = Rc::new(RefCell::new(None));
        let out_capture = Rc::clone(&out);
        let root_key = self.spawn(async move {
            let v = fut.await;
            *out_capture.borrow_mut() = Some(v);
        });

        // Drive the reactor until the root task completes. The `tasks`
        // map removes the entry on completion, so `contains_key(root_key)`
        // returning false is the termination signal.
        loop {
            self.tick(true);
            if !self.inner.tasks.borrow().contains_key(&root_key) {
                break;
            }
        }

        // SAFETY: spawn ran the future to completion, so Some.
        let v = out.borrow_mut().take()
            .expect("block_on root task did not produce output");
        v
    }

    /// Future that completes at `deadline`. The reactor wakes the task
    /// when its main loop sees that the soonest deadline has passed.
    pub fn timer(&self, deadline: Instant) -> impl Future<Output = ()> {
        TimerFuture::new(deadline, Rc::clone(&self.inner))
    }

    /// Future that resolves to the decoded W2M reply for `req_id`.
    pub fn await_reply(&self, req_id: u64) -> impl Future<Output = DecodedWire> {
        ReplyFuture {
            req_id,
            inner: Rc::clone(&self.inner),
        }
    }

    /// Attach the `W2mReceiver` and arm a persistent `FUTEX_WAITV` SQE
    /// that watches every worker's `reader_seq` word. On each CQE, the
    /// reactor drains all rings (the wake index is not authoritative
    /// for FutexWaitV), rebuilds the expected-values array, and
    /// re-arms.
    pub fn attach_w2m(&self, w2m: W2mReceiver) {
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
        // Drain-then-refresh loop: the same protocol used by the
        // CQE dispatch path. Drain catches any messages published
        // between init and attach; refresh snapshots expected values
        // with MASTER_PARKED already set.
        loop {
            for w in 0..nw {
                self.drain_w2m_for_worker(w);
            }
            if !self.refresh_futex_waitv_vals() {
                break;
            }
        }
        self.arm_futex_waitv();
    }

    /// (Re-)submit a `FUTEX_WAITV` SQE referencing the heap-owned
    /// `FutexWaitV` array in `futex_waitv_storage`. Called by
    /// `attach_w2m` on startup and by the `KIND_FUTEX_WAITV` dispatch
    /// handler on every CQE.
    fn arm_futex_waitv(&self) {
        let storage = self.inner.futex_waitv_storage.borrow();
        let Some(boxed) = storage.as_ref() else {
            return;
        };
        let nr = boxed.len() as u32;
        let ptr = boxed.as_ptr();
        let udata_val = udata(KIND_FUTEX_WAITV, 0);
        {
            let mut ring = self.inner.ring.borrow_mut();
            unsafe { ring.prep_futex_waitv(ptr, nr, udata_val); }
            if let Err(e) = ring.submit_and_wait_timeout(0, 0) {
                crate::gnitz_error!(
                    "reactor: FUTEX_WAITV SQE flush failed (errno={}); \
                     SQE queued in SQ ring — will submit on next tick",
                    e,
                );
            }
        }
        self.inner.futex_waitv_armed.set(true);
    }

    /// Publish `MASTER_PARKED` on every ring, then snapshot each
    /// `reader_seq` for the `FutexWaitV` entry. Returns `true` if any
    /// ring has unread data (`read_cursor != write_cursor`), meaning
    /// the caller must drain before arming — otherwise the expected
    /// values would already match current reader_seq and the SQE
    /// would block waiting for a wake that already happened (classic
    /// lost-wake race).
    ///
    /// Store order is load-bearing: setting `FLAG_MASTER_PARKED`
    /// BEFORE loading `reader_seq` ensures that any worker that
    /// advances `reader_seq` AFTER our fetch_or is guaranteed to
    /// observe the flag and issue `FUTEX_WAKE`. Workers that advanced
    /// BEFORE our fetch_or are caught by the post-snapshot
    /// `write_cursor != read_cursor` check.
    fn refresh_futex_waitv_vals(&self) -> bool {
        let mut storage = self.inner.futex_waitv_storage.borrow_mut();
        let Some(boxed) = storage.as_mut() else { return false; };
        let Some(w2m) = self.inner.w2m.get() else { return false; };
        let mut pending = false;
        for (w, entry) in boxed.iter_mut().enumerate() {
            let hdr = unsafe { w2m.header(w) };
            // Publish park intent FIRST (AcqRel — the flag must be
            // globally visible before we read reader_seq). Once the
            // reactor has attached, no path clears `FLAG_MASTER_PARKED`
            // (`W2mReceiver::wait_for` does, but it is bootstrap-only
            // and runs before `attach_w2m`), so after the first
            // iteration the bit is already set: load first and skip the
            // RMW when it's a no-op.
            let flags_now = hdr.waiter_flags()
                .load(std::sync::atomic::Ordering::Acquire);
            if flags_now & FLAG_MASTER_PARKED == 0 {
                hdr.waiter_flags().fetch_or(
                    FLAG_MASTER_PARKED,
                    std::sync::atomic::Ordering::AcqRel,
                );
            }
            // Now snapshot expected reader_seq. Any worker store that
            // happens-before this load MUST have been preceded by a
            // write_cursor Release store, so the post-loop
            // `write_cursor != read_cursor` check below catches it.
            let expected = hdr.reader_seq()
                .load(std::sync::atomic::Ordering::Acquire);
            let uaddr = hdr.reader_seq() as *const std::sync::atomic::AtomicU32 as u64;
            *entry = FutexWaitV::new()
                .val(expected as u64)
                .uaddr(uaddr)
                .flags(FUTEX2_SIZE_U32);
            // Unread-data check: if write_cursor has advanced past
            // read_cursor, a publish is pending that we haven't
            // drained. Signal the caller to drain before arming.
            let wc = hdr.write_cursor()
                .load(std::sync::atomic::Ordering::Acquire);
            let rc = hdr.read_cursor()
                .load(std::sync::atomic::Ordering::Acquire);
            if wc != rc {
                pending = true;
            }
        }
        pending
    }

    /// Submit an fdatasync on `fd`. The SQE is immediately flushed to the
    /// kernel so the fsync can overlap with subsequent CPU work — this is
    /// load-bearing for the `pre_write_pushes` Phase-A / tick-evaluation
    /// overlap.
    fn submit_fsync(&self, fd: i32) -> u64 {
        // Fsync IDs share the reply counter; the KIND_FSYNC tag in udata
        // prevents collisions with KIND_REPLY ids.
        let id = self.alloc_request_id();
        {
            let mut ring = self.inner.ring.borrow_mut();
            ring.prep_fsync(fd, udata(KIND_FSYNC, id));
            if let Err(e) = ring.submit_and_wait_timeout(0, 0) {
                crate::gnitz_error!(
                    "reactor: fsync SQE flush failed (errno={}); \
                     SQE queued — will submit on next tick",
                    e,
                );
            }
        }
        id
    }

    /// Submit an fdatasync and await its completion. Returns the CQE `res`
    /// (0 on success, negative errno on failure).
    pub fn fsync(&self, fd: i32) -> FsyncFuture {
        let id = self.submit_fsync(fd);
        FsyncFuture { id, inner: Rc::clone(&self.inner) }
    }

    #[cfg(test)]
    pub fn block_on_fsync(&self, id: u64) -> i32 {
        loop {
            if let Some(rc) = self.inner.parked_fsync_results.borrow_mut().remove(&id) {
                return rc;
            }
            self.tick(true);
        }
    }

    /// Allocate a per-send id distinct from reply / fsync id space.
    fn alloc_send_id(&self) -> u64 {
        let id = self.inner.next_send_id.get();
        let next = match id.checked_add(1) {
            Some(n) if n != u64::MAX => n,
            _ => 1,
        };
        self.inner.next_send_id.set(next);
        id
    }

    /// Attach the listen socket fd and arm a multishot-accept SQE.
    pub fn attach_server_fd(&self, server_fd: i32) {
        self.inner.server_fd.set(server_fd);
        {
            let mut ring = self.inner.ring.borrow_mut();
            ring.prep_accept(server_fd, udata(KIND_ACCEPT, 0));
            if let Err(e) = ring.submit_and_wait_timeout(0, 0) {
                crate::gnitz_fatal_abort!(
                    "reactor: accept SQE flush failed (errno={}) — no connections can be accepted",
                    e,
                );
            }
        }
    }

    /// Future resolving to the next newly-accepted fd. Called by the
    /// accept-loop task.
    pub fn accept(&self) -> AcceptFuture {
        AcceptFuture { inner: Rc::clone(&self.inner) }
    }

    /// Initial arm of a recv SQE on a new connection fd.
    pub fn register_conn(&self, fd: i32) {
        // Clear any stale close-sentinel from a prior incarnation of
        // this fd number (kernel may reuse fds after close).
        self.inner.recv_closed.borrow_mut().remove(&fd);
        // Stray pending_recv entries at this point mean the previous
        // incarnation of this fd closed without `reap_closing_conns`
        // freeing its payload buffers — a bug. Assert in debug; in
        // release, free the buffers so we don't leak.
        let stale = self.inner.pending_recv.borrow_mut().remove(&fd);
        if let Some(stale) = stale {
            if !stale.is_empty() {
                crate::gnitz_fatal_abort!(
                    "reactor: register_conn: {} stale pending_recv entries for fd={} — \
                     reap_closing_conns did not run before fd was reused; \
                     freeing would be a use-after-free if any SQEs are still in flight",
                    stale.len(), fd,
                );
            }
        }
        let mut conns = self.inner.conns.borrow_mut();
        conns.insert(fd, Box::new(io::Conn::new()));
        let conn = conns.get_mut(&fd).unwrap();
        let hdr_ptr = conn.recv_state.hdr_buf_ptr();
        {
            let mut ring = self.inner.ring.borrow_mut();
            ring.prep_recv(fd, hdr_ptr, 4, udata(KIND_RECV, fd as u32 as u64));
            if let Err(e) = ring.submit_and_wait_timeout(0, 0) {
                crate::gnitz_error!(
                    "reactor: recv SQE flush failed for fd={} (errno={}); \
                     SQE queued — will submit on next tick",
                    fd, e,
                );
            }
        }
        conn.recv_armed = true;
    }

    /// Future resolving to the next complete message on `fd`, or `None`
    /// when the peer has disconnected.  Each call drains at most one
    /// message; the caller is responsible for `libc::free`-ing the
    /// returned ptr.
    pub fn recv(&self, fd: i32) -> RecvFuture {
        RecvFuture { fd, inner: Rc::clone(&self.inner) }
    }

    /// Returns total bytes sent (>= 0) or negative errno. The loop is
    /// load-bearing: OP_SEND on a stream socket may return rc < len
    /// when the kernel's socket buffer fills up.
    pub async fn send_buffer(&self, fd: i32, buf: Vec<u8>) -> i32 {
        let len = buf.len();
        // Rc so a dropped outer future hands the allocation to the
        // in-flight SendFuture, which parks it until the CQE fires
        // (the kernel still dereferences the pointer).
        let buf_rc: Rc<Vec<u8>> = Rc::new(buf);
        let mut sent: usize = 0;
        let mut final_rc: i32 = 0;
        while sent < len {
            let send_id = self.alloc_send_id();
            if let Some(conn) = self.inner.conns.borrow_mut().get_mut(&fd) {
                conn.send_inflight += 1;
            }
            self.inner.send_fd_for_id.borrow_mut().insert(send_id, fd);
            let ptr = unsafe { buf_rc.as_ptr().add(sent) };
            let remaining = (len - sent) as u32;
            {
                let mut ring = self.inner.ring.borrow_mut();
                ring.prep_send(fd, ptr, remaining, udata(KIND_SEND, send_id));
                if let Err(e) = ring.submit_and_wait_timeout(0, 0) {
                    crate::gnitz_error!(
                        "reactor: send SQE flush failed for send_id={} (errno={}); \
                         SQE queued — will submit on next tick",
                        send_id, e,
                    );
                }
            }
            let rc = SendFuture {
                send_id,
                _buf: Some(Rc::clone(&buf_rc)),
                inner: Rc::clone(&self.inner),
            }.await;
            if rc < 0 { final_rc = rc; break; }
            if rc == 0 { break; }  // connection closed / EOF
            sent += rc as usize;
        }
        if final_rc < 0 { final_rc } else { sent as i32 }
    }

    /// Request the reactor close `fd` once all outstanding SQEs
    /// complete.  Marks the connection as closing; `reap_closing` in
    /// the tick loop frees the fd when its recv/send slots go quiet.
    pub fn close_fd(&self, fd: i32) {
        if let Some(conn) = self.inner.conns.borrow_mut().get_mut(&fd) {
            conn.closing = true;
            self.inner.closing_fds.borrow_mut().insert(fd);
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
    pub fn block_until_idle(&self) {
        while !self.inner.tasks.borrow().is_empty() {
            self.tick(true);
        }
    }

    /// Register `w` to fire when a CQE with `(KIND_REPLY, req_id)` arrives.
    /// Replaces any waker previously registered for the same id.
    #[allow(dead_code)]
    pub(crate) fn register_reply_waker(&self, req_id: u64, w: Waker) {
        self.inner.reply_wakers.borrow_mut().insert(req_id, w);
    }

    /// True while at least one task is alive in the slab.
    #[cfg(test)]
    pub fn has_pending_tasks(&self) -> bool {
        !self.inner.tasks.borrow().is_empty()
    }

    /// Drive ready tasks and process CQEs without blocking.
    #[cfg(test)]
    pub fn poll_nonblocking(&self) {
        self.tick(false);
    }

    /// Single iteration of the event loop:
    ///   0. proactive W2M drain (lost-wake safety net)
    ///   1. drain CQEs (waking reply / timeout / fsync wakers)
    ///   2. poll all tasks in the run queue (each polled at most once)
    ///   3. submit pending SQEs; if `block` and the run queue is now
    ///      empty, sleep until the next CQE.
    fn tick(&self, block: bool) {
        // 0. Proactive W2M drain — safety net for lost FUTEX_WAITV wakes.
        //
        // A narrow race exists between refresh_futex_waitv_vals (which sets
        // FLAG_MASTER_PARKED and snapshots reader_seq) and arm_futex_waitv
        // (which registers the FUTEX_WAITV SQE with the kernel): a worker
        // that publishes in that window calls FUTEX_WAKE against a waiter
        // that does not exist yet. The kernel's mismatch-detection (it
        // immediately completes a FUTEX_WAITV whose expected value is stale)
        // covers the case where the arm precedes the publish; this drain
        // covers the opposite case (publish beats the arm) in both the
        // spinning path (no blocking) and the blocking path (before we sleep).
        //
        // Cost: one try_consume call per worker per tick — just two Acquire
        // loads that return None when the ring is empty.
        if self.inner.futex_waitv_armed.get() {
            let nw = self.inner.w2m.get()
                .expect("futex_waitv_armed=true but w2m not attached")
                .num_workers();
            for w in 0..nw {
                self.drain_w2m_for_worker(w);
            }
        }

        // 1. CQEs (no syscall — reads memory-mapped CQ).
        self.drain_cqes_into_wakers();
        #[cfg(test)]
        self.drain_injected_cqes();
        self.reap_closing_conns();

        // 2. Drain the run queue. Snapshot first so wakes during poll
        // schedule for the *next* tick rather than re-entering this one.
        let ready: Vec<usize> = {
            let mut q = self.inner.run_queue.lock().unwrap();
            q.drain(..).collect()
        };
        for key in ready {
            self.poll_task(key);
        }

        // 3. Submit pending SQEs and optionally block until the next event.
        // Skip blocking when there is nothing to drive (slab empty) or when
        // a wake fired during this tick (run_queue non-empty); otherwise we
        // would sleep past the natural completion of the loop.
        let should_block = block
            && !self.inner.tasks.borrow().is_empty()
            && self.inner.run_queue.lock().unwrap().is_empty();
        if should_block {
            // Block indefinitely — outstanding timer SQEs guarantee a CQE
            // will arrive when the soonest timer fires.
            if let Err(e) = self.inner.ring.borrow_mut().submit_and_wait_timeout(1, -1) {
                crate::gnitz_error!("reactor: tick blocking submit failed (errno={})", e);
            }
        } else if let Err(e) = self.inner.ring.borrow_mut().submit_and_wait_timeout(0, 0) {
            crate::gnitz_error!("reactor: tick non-blocking submit failed (errno={})", e);
        }
    }

    /// Poll a single task. The future is removed from the map for the
    /// duration of the poll (so the poll can re-enter the reactor to
    /// spawn new tasks), then reinserted at the SAME key on Pending.
    /// HashMap removal + same-key reinsertion is stable regardless of
    /// what other keys are added/removed during poll.
    fn poll_task(&self, key: usize) {
        let waker = match self.inner.task_wakers.borrow().get(&key).cloned() {
            Some(w) => w,
            None => return,
        };
        let mut task = match self.inner.tasks.borrow_mut().remove(&key) {
            Some(t) => t,
            None => return,
        };
        let mut cx = Context::from_waker(&waker);
        match task.future.as_mut().poll(&mut cx) {
            Poll::Ready(()) => { self.inner.task_wakers.borrow_mut().remove(&key); }
            Poll::Pending   => { self.inner.tasks.borrow_mut().insert(key, task); }
        }
    }

    /// Look at all CQEs pending in the ring. For each:
    ///   - KIND_REPLY: wake the registered reply waker (if any).
    ///   - KIND_TIMEOUT: wake the registered timer waker (if not cancelled).
    ///   - KIND_FSYNC: wake the fsync waker and park `res`.
    ///   - KIND_POLL_EVENTFD: drain the associated W2M ring and re-arm.
    fn drain_cqes_into_wakers(&self) {
        let mut buf = [Cqe::default(); 64];
        loop {
            let n = self.inner.ring.borrow_mut().drain_cqes(&mut buf);
            if n == 0 { break; }
            for cqe in &buf[..n] {
                self.dispatch_cqe(*cqe);
            }
        }
    }

    fn dispatch_cqe(&self, cqe: Cqe) {
        let kind = udata_kind(cqe.user_data);
        let id = udata_id(cqe.user_data);
        match kind {
            KIND_REPLY => {
                if let Some(w) = self.inner.reply_wakers.borrow_mut().remove(&id) {
                    w.wake();
                }
            }
            KIND_TIMEOUT => {
                if let Some((w, cancelled)) = self.inner.timer_wakers.borrow_mut().remove(&id) {
                    if !cancelled.get() { w.wake(); }
                }
            }
            KIND_FUTEX_WAITV => {
                // Wake index is not authoritative for FUTEX_WAITV:
                // the kernel may wake us for any of the watched
                // words. Drain every worker's ring, refresh expected
                // values (with MASTER_PARKED published first so the
                // wake window stays closed), and re-arm. The
                // `refresh → drain` loop runs until no ring has
                // unread data, guaranteeing we never arm with an
                // already-stale expected value.
                self.inner.futex_waitv_armed.set(false);
                if !self.inner.shutdown.get() {
                    let nw = self.inner.w2m.get()
                        .expect("KIND_FUTEX_WAITV fired but w2m not attached")
                        .num_workers();
                    loop {
                        for w in 0..nw {
                            self.drain_w2m_for_worker(w);
                        }
                        if !self.refresh_futex_waitv_vals() {
                            break;
                        }
                    }
                    self.arm_futex_waitv();
                }
            }
            KIND_FUTEX_CANCEL => {
                // The AsyncCancel CQE itself arrives here. The
                // cancellation of the FUTEX_WAITV SQE delivers a
                // separate CQE under KIND_FUTEX_WAITV with
                // `res = -ECANCELED`, which we detect via the
                // `futex_waitv_armed` flag flipping false above.
                // Record completion so shutdown can drop storage.
                self.inner.futex_waitv_cancelled.set(true);
            }
            KIND_FSYNC => {
                self.inner.parked_fsync_results.borrow_mut().insert(id, cqe.res);
                if let Some(w) = self.inner.fsync_wakers.borrow_mut().remove(&id) {
                    w.wake();
                }
            }
            KIND_ACCEPT => {
                if cqe.res >= 0 {
                    self.inner.accept_queue.borrow_mut().push_back(cqe.res);
                    if let Some(w) = self.inner.accept_waker.borrow_mut().take() {
                        w.wake();
                    }
                }
                if cqe.flags & CQE_F_MORE == 0 {
                    // Multishot cancelled — re-arm.
                    let sfd = self.inner.server_fd.get();
                    if sfd >= 0 {
                        self.inner.ring.borrow_mut().prep_accept(sfd, udata(KIND_ACCEPT, 0));
                    }
                }
            }
            KIND_RECV => {
                let fd = id as i32;
                self.handle_recv_cqe(fd, cqe.res);
            }
            KIND_SEND => {
                self.inner.parked_send_results.borrow_mut().insert(id, cqe.res);
                let fd = self.inner.send_fd_for_id.borrow_mut().remove(&id);
                if let Some(fd) = fd {
                    if let Some(conn) = self.inner.conns.borrow_mut().get_mut(&fd) {
                        conn.send_inflight = conn.send_inflight.saturating_sub(1);
                    }
                }
                self.inner.send_buffers_in_flight.borrow_mut().remove(&id);
                if let Some(w) = self.inner.send_wakers.borrow_mut().remove(&id) {
                    w.wake();
                }
            }
            _ => {}
        }
    }

    fn handle_recv_cqe(&self, fd: i32, res: i32) {
        let mut conns = self.inner.conns.borrow_mut();
        let conn = match conns.get_mut(&fd) {
            Some(c) => c,
            None => return,
        };
        conn.recv_armed = false;

        if res <= 0 || conn.closing {
            conn.closing = true;
            self.inner.closing_fds.borrow_mut().insert(fd);
            self.inner.recv_closed.borrow_mut().insert(fd, true);
            if let Some(w) = self.inner.recv_waiters.borrow_mut().remove(&fd) {
                w.wake();
            }
            return;
        }

        match conn.recv_state.advance(res as usize) {
            io::RecvAdvance::NeedMore => {
                let (buf, len) = conn.recv_state.remaining();
                self.inner.ring.borrow_mut().prep_recv(
                    fd, buf, len, udata(KIND_RECV, fd as u32 as u64),
                );
                conn.recv_armed = true;
            }
            io::RecvAdvance::HeaderDone => {
                let plen = conn.recv_state.payload_len();
                if plen > io::MAX_PAYLOAD_LEN {
                    conn.closing = true;
                    self.inner.closing_fds.borrow_mut().insert(fd);
                    self.inner.recv_closed.borrow_mut().insert(fd, true);
                    if let Some(w) = self.inner.recv_waiters.borrow_mut().remove(&fd) {
                        w.wake();
                    }
                    return;
                }
                let pbuf = unsafe { libc::malloc(plen) as *mut u8 };
                if pbuf.is_null() {
                    conn.closing = true;
                    self.inner.closing_fds.borrow_mut().insert(fd);
                    self.inner.recv_closed.borrow_mut().insert(fd, true);
                    if let Some(w) = self.inner.recv_waiters.borrow_mut().remove(&fd) {
                        w.wake();
                    }
                    return;
                }
                conn.recv_state.start_payload(pbuf, plen);
                self.inner.ring.borrow_mut().prep_recv(
                    fd, pbuf, plen as u32, udata(KIND_RECV, fd as u32 as u64),
                );
                conn.recv_armed = true;
            }
            io::RecvAdvance::MessageDone => {
                let (ptr, len) = conn.recv_state.take_message();
                self.inner.pending_recv.borrow_mut()
                    .entry(fd).or_default().push_back((ptr, len));
                // Arm next header recv immediately so the kernel can keep
                // draining the client's send buffer. Per-session FIFO is
                // preserved by the `VecDeque` order — the handler still
                // consumes messages in arrival order.
                let hdr = conn.recv_state.hdr_buf_ptr();
                self.inner.ring.borrow_mut().prep_recv(
                    fd, hdr, 4, udata(KIND_RECV, fd as u32 as u64),
                );
                conn.recv_armed = true;
                if let Some(w) = self.inner.recv_waiters.borrow_mut().remove(&fd) {
                    w.wake();
                }
            }
            io::RecvAdvance::Disconnect => {
                conn.closing = true;
                self.inner.closing_fds.borrow_mut().insert(fd);
                self.inner.recv_closed.borrow_mut().insert(fd, true);
                if let Some(w) = self.inner.recv_waiters.borrow_mut().remove(&fd) {
                    w.wake();
                }
            }
        }
    }

    /// Reap connections that are closing and have no outstanding SQEs.
    /// Called once per tick. Iterates only `closing_fds` (O(closing)),
    /// not all connections.
    fn reap_closing_conns(&self) {
        let closing: Vec<i32> = self.inner.closing_fds.borrow().iter().copied().collect();
        if closing.is_empty() { return; }
        for fd in closing {
            let ready = {
                let conns = self.inner.conns.borrow();
                match conns.get(&fd) {
                    Some(conn) => !conn.has_outstanding(),
                    None => true,
                }
            };
            if !ready { continue; }
            self.inner.closing_fds.borrow_mut().remove(&fd);
            self.inner.conns.borrow_mut().remove(&fd);
            self.inner.recv_waiters.borrow_mut().remove(&fd);
            if let Some(queue) = self.inner.pending_recv.borrow_mut().remove(&fd) {
                for (ptr, _) in queue {
                    if !ptr.is_null() {
                        unsafe { libc::free(ptr as *mut libc::c_void); }
                    }
                }
            }
            unsafe { libc::close(fd); }
        }
    }

    /// Decode every unread W2M message for worker `w` and route each
    /// through `route_reply`. The tail-chasing ring self-maintains —
    /// no reset gate, no `in_flight` accounting. Each successful
    /// `try_read` advances the ring's shared `read_cursor` and may
    /// wake a parked producer.
    fn drain_w2m_for_worker(&self, w: usize) {
        let w2m = self.inner.w2m.get()
            .expect("drain_w2m_for_worker called before attach_w2m");
        while let Some(decoded) = w2m.try_read(w) {
            self.route_reply(w, decoded);
        }
    }

    /// Park `decoded` for its awaiter. FLAG_EXCHANGE replies are
    /// demuxed into `exchange_acc` so the tick req_id's waker stays
    /// parked until the final (non-FLAG_EXCHANGE) ACK lands. When an
    /// exchange round completes, the resulting `PendingRelay` is
    /// dispatched on `relay_tx`. See async-invariants.md §III.3b.
    ///
    /// Unrouted replies are logged and dropped.
    fn route_reply(&self, w: usize, decoded: DecodedWire) {
        let flags = decoded.control.flags as u32;
        if flags & FLAG_EXCHANGE != 0 {
            let pending = self.inner.exchange_acc.borrow_mut().process(w, decoded);
            if let Some(relay) = pending {
                if let Some(tx) = self.inner.relay_tx.borrow().as_ref() {
                    tx.send(relay);
                } else {
                    crate::gnitz_warn!(
                        "reactor: FLAG_EXCHANGE relay produced before relay_tx attached (view_id={})",
                        relay.view_id,
                    );
                }
            }
            return;
        }

        let req_id = decoded.control.request_id;
        let waker = self.inner.reply_wakers.borrow_mut().remove(&req_id);
        match waker {
            Some(waker) => {
                self.inner.parked_replies.borrow_mut().insert(req_id, decoded);
                waker.wake();
            }
            None => {
                crate::gnitz_warn!(
                    "reactor: unrouted W2M reply worker={} req_id={}",
                    w, req_id,
                );
            }
        }
    }

    #[cfg(test)]
    fn drain_injected_cqes(&self) {
        loop {
            let cqe = self.inner.injected_cqes.borrow_mut().pop_front();
            match cqe {
                Some(c) => self.dispatch_cqe(c),
                None => break,
            }
        }
    }

    /// Test-only: inject a synthetic CQE tagged with `kind` and `id`,
    /// with `rc` as the CQE `res`. Dispatched on the next `tick` (or via
    /// `drain_injected_cqes`).
    #[cfg(test)]
    pub fn inject_cqe(&self, kind: u64, id: u64, rc: i32) {
        self.inner.injected_cqes.borrow_mut().push_back(Cqe {
            user_data: udata(kind, id),
            res: rc,
            flags: 0,
        });
    }

    /// Test-only: park a synthetic `DecodedWire` for `req_id` and wake
    /// any matching awaiter.
    #[cfg(test)]
    pub fn inject_parked_reply(&self, req_id: u64, decoded: DecodedWire) {
        self.inner.parked_replies.borrow_mut().insert(req_id, decoded);
        if let Some(waker) = self.inner.reply_wakers.borrow_mut().remove(&req_id) {
            waker.wake();
        }
    }

    /// Test-only: size the exchange accumulator so `route_reply` can
    /// be driven directly.
    #[cfg(test)]
    pub fn test_init_state(&self, num_workers: usize) {
        *self.inner.exchange_acc.borrow_mut() = ExchangeAccumulator::new(num_workers);
    }

    /// Test-only: drive `route_reply` with a synthetic decoded wire.
    #[cfg(test)]
    pub fn test_route_reply(&self, w: usize, decoded: DecodedWire) {
        self.route_reply(w, decoded)
    }

    #[cfg(test)]
    pub fn task_count(&self) -> usize {
        self.inner.tasks.borrow().len()
    }
}

// ---------------------------------------------------------------------------
// Waker vtable
// ---------------------------------------------------------------------------

/// Heap-allocated state captured by the waker. The vtable requires
/// `Send + Sync` so we use `Arc`; in practice the reactor never crosses
/// threads.
struct WakerInner {
    key: usize,
    queue: Arc<Mutex<VecDeque<usize>>>,
}

unsafe fn waker_clone(data: *const ()) -> RawWaker {
    let arc = unsafe { Arc::from_raw(data as *const WakerInner) };
    let cloned = Arc::clone(&arc);
    std::mem::forget(arc);
    RawWaker::new(Arc::into_raw(cloned) as *const (), &WAKER_VTABLE)
}

unsafe fn waker_wake(data: *const ()) {
    let arc = unsafe { Arc::from_raw(data as *const WakerInner) };
    arc.queue.lock().unwrap().push_back(arc.key);
}

unsafe fn waker_wake_by_ref(data: *const ()) {
    let arc = unsafe { Arc::from_raw(data as *const WakerInner) };
    arc.queue.lock().unwrap().push_back(arc.key);
    std::mem::forget(arc);
}

unsafe fn waker_drop(data: *const ()) {
    let _ = unsafe { Arc::from_raw(data as *const WakerInner) };
}

const WAKER_VTABLE: RawWakerVTable = RawWakerVTable::new(
    waker_clone, waker_wake, waker_wake_by_ref, waker_drop,
);

fn make_waker(key: usize, queue: Arc<Mutex<VecDeque<usize>>>) -> Waker {
    let inner = Arc::new(WakerInner { key, queue });
    let raw = RawWaker::new(Arc::into_raw(inner) as *const (), &WAKER_VTABLE);
    unsafe { Waker::from_raw(raw) }
}

// ---------------------------------------------------------------------------
// TimerFuture / ReplyFuture
// ---------------------------------------------------------------------------

struct TimerFuture {
    deadline: Instant,
    /// Set to Some(id) on first poll, when the io_uring Timeout SQE is
    /// submitted. None means the future was never polled (no SQE in
    /// flight).
    timer_id: Option<u64>,
    /// Shared with the timer_wakers entry; flipped to true on Drop so
    /// dispatch_cqe silently discards the CQE without waking the task.
    cancelled: Rc<Cell<bool>>,
    inner: Rc<ReactorShared>,
}

impl TimerFuture {
    fn new(deadline: Instant, inner: Rc<ReactorShared>) -> Self {
        TimerFuture {
            deadline,
            timer_id: None,
            cancelled: Rc::new(Cell::new(false)),
            inner,
        }
    }
}

impl Future for TimerFuture {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let now = Instant::now();
        if now >= self.deadline {
            return Poll::Ready(());
        }
        if let Some(id) = self.timer_id {
            // Re-poll: update the waker in case it changed.
            if let Some(entry) = self.inner.timer_wakers.borrow_mut().get_mut(&id) {
                entry.0 = cx.waker().clone();
            }
        } else {
            // First poll: submit the io_uring Timeout SQE and register the
            // waker. The SQE is flushed to the kernel by tick's
            // submit_and_wait_timeout call at the end of the same tick.
            let id = self.inner.next_timer_id.get();
            self.inner.next_timer_id.set(id.wrapping_add(1));
            let ns = self.deadline.duration_since(now).as_nanos() as u64;
            self.inner.ring.borrow_mut().prep_timeout(ns, udata(KIND_TIMEOUT, id));
            self.inner.timer_wakers.borrow_mut()
                .insert(id, (cx.waker().clone(), Rc::clone(&self.cancelled)));
            self.timer_id = Some(id);
        }
        Poll::Pending
    }
}

impl Drop for TimerFuture {
    fn drop(&mut self) {
        if self.timer_id.is_some() {
            // Signal dispatch_cqe to discard the CQE without waking.
            // No SQE cancellation is needed: the CQE will arrive and be
            // silently dropped by the KIND_TIMEOUT handler.
            self.cancelled.set(true);
        }
    }
}

struct ReplyFuture {
    req_id: u64,
    inner: Rc<ReactorShared>,
}

impl Future for ReplyFuture {
    type Output = DecodedWire;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<DecodedWire> {
        if let Some(decoded) = self.inner.parked_replies.borrow_mut().remove(&self.req_id) {
            return Poll::Ready(decoded);
        }
        self.inner.reply_wakers.borrow_mut()
            .insert(self.req_id, cx.waker().clone());
        Poll::Pending
    }
}

// ---------------------------------------------------------------------------
// FsyncFuture / AcceptFuture / RecvFuture / SendFuture
// ---------------------------------------------------------------------------

pub struct FsyncFuture {
    id: u64,
    inner: Rc<ReactorShared>,
}

impl Future for FsyncFuture {
    type Output = i32;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<i32> {
        if let Some(rc) = self.inner.parked_fsync_results.borrow_mut().remove(&self.id) {
            return Poll::Ready(rc);
        }
        self.inner.fsync_wakers.borrow_mut()
            .insert(self.id, cx.waker().clone());
        Poll::Pending
    }
}

pub struct AcceptFuture {
    inner: Rc<ReactorShared>,
}

impl Future for AcceptFuture {
    type Output = i32;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<i32> {
        if let Some(fd) = self.inner.accept_queue.borrow_mut().pop_front() {
            return Poll::Ready(fd);
        }
        *self.inner.accept_waker.borrow_mut() = Some(cx.waker().clone());
        Poll::Pending
    }
}

pub struct RecvFuture {
    fd: i32,
    inner: Rc<ReactorShared>,
}

impl Future for RecvFuture {
    type Output = Option<(*mut u8, usize)>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let taken = {
            let mut map = self.inner.pending_recv.borrow_mut();
            map.get_mut(&self.fd).and_then(|q| q.pop_front())
        };
        if let Some((ptr, len)) = taken {
            return Poll::Ready(Some((ptr, len)));
        }
        if self.inner.recv_closed.borrow().get(&self.fd).copied().unwrap_or(false) {
            return Poll::Ready(None);
        }
        self.inner.recv_waiters.borrow_mut().insert(self.fd, cx.waker().clone());
        Poll::Pending
    }
}

pub struct SendFuture {
    send_id: u64,
    _buf: Option<Rc<Vec<u8>>>,
    inner: Rc<ReactorShared>,
}

impl Future for SendFuture {
    type Output = i32;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<i32> {
        let parked = self.inner.parked_send_results.borrow_mut().remove(&self.send_id);
        if let Some(rc) = parked {
            self._buf.take();
            return Poll::Ready(rc);
        }
        let send_id = self.send_id;
        self.inner.send_wakers.borrow_mut()
            .insert(send_id, cx.waker().clone());
        Poll::Pending
    }
}

impl Drop for SendFuture {
    fn drop(&mut self) {
        // CQE hasn't arrived — park the buffer in the reactor so the
        // kernel's pointer stays valid until KIND_SEND fires.
        if let Some(buf) = self._buf.take() {
            self.inner.send_buffers_in_flight.borrow_mut()
                .insert(self.send_id, buf);
        }
    }
}

// (oneshot, mpsc, AsyncMutex, AsyncRwLock, join2, join_all, select2 live in sync.rs)

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell as StdCell;
    use std::time::Duration;
    use crate::schema::SchemaDescriptor;

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
        r.block_on(async move {
            c2.set(c2.get() + 1);
        });
        assert_eq!(counter.get(), 1);
    }

    /// Timer fires after a short deadline.
    #[test]
    fn timer_fires() {
        let r = make_reactor();
        let inner = Rc::clone(&r.inner);
        let start = Instant::now();
        r.block_on(async move {
            TimerFuture::new(
                Instant::now() + Duration::from_millis(50),
                inner,
            ).await;
        });
        let elapsed = start.elapsed();
        assert!(elapsed >= Duration::from_millis(50),
            "timer fired too early: {:?}", elapsed);
        assert!(elapsed < Duration::from_millis(500),
            "timer fired too late: {:?}", elapsed);
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
            TimerFuture::new(
                Instant::now() + Duration::from_millis(100),
                inner2,
            ).await;
            order2.borrow_mut().push(2);
        });

        r.block_on(async move {
            TimerFuture::new(
                Instant::now() + Duration::from_millis(20),
                inner1,
            ).await;
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
        r.inject_parked_reply(7, synthetic_decoded_wire(7));
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
        r.inject_parked_reply(8, synthetic_decoded_wire(8));
        let resolved: Rc<StdCell<bool>> = Rc::new(StdCell::new(false));
        let r2 = Rc::clone(&resolved);
        let timer_inner = Rc::clone(&r.inner);
        let reply_fut = r.await_reply(7);
        r.block_on(async move {
            let timer = TimerFuture::new(
                Instant::now() + Duration::from_millis(50),
                timer_inner,
            );
            select_reply_or_timer(timer, reply_fut, &r2).await;
        });
        assert!(!resolved.get(), "reply for req_id=8 must not wake req_id=7 awaiter");
    }

    /// `alloc_request_id` returns strictly increasing values, skipping
    /// the reserved sentinels (0 and u64::MAX).
    #[test]
    fn alloc_request_id_monotonic() {
        let r = make_reactor();
        let mut last = 0u64;
        for _ in 0..1000 {
            let id = r.alloc_request_id();
            assert!(id > last);
            assert_ne!(id, 0);
            assert_ne!(id, u64::MAX);
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
        r.block_on(async move {
            DoublyWaking { polls: polls2, polled: 0 }.await
        });
        // Doubly waking polls itself N times before completing; the
        // exact value isn't load-bearing, just that we eventually finish.
        assert!(polls.get() >= 1);
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
            TimerFuture::new(
                Instant::now() - Duration::from_secs(1),
                inner,
            ).await;
        });
        assert!(start.elapsed() < Duration::from_millis(100),
            "past-deadline timer must not block");
    }

    /// Cloning a waker, dropping the original, then waking the clone
    /// must still schedule the task. Exercises the Arc refcount in the
    /// waker vtable.
    #[test]
    fn waker_clone_outlives_original() {
        let r = make_reactor();
        let queue = Arc::clone(&r.inner.run_queue);
        let original = make_waker(123, queue);
        let cloned = original.clone();
        drop(original);
        cloned.wake();
        let q: Vec<usize> = r.inner.run_queue.lock().unwrap().iter().copied().collect();
        assert!(q.contains(&123));
    }

    /// `poll_nonblocking` returns promptly with no work to do — no syscall
    /// other than the no-op submit. Bound: under 100ms (very generous;
    /// failure indicates accidental blocking in the no-work path).
    #[test]
    fn poll_nonblocking_returns_promptly() {
        let r = make_reactor();
        let start = Instant::now();
        r.poll_nonblocking();
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
        let mutex: Rc<AsyncMutex<u32>> = Rc::new(AsyncMutex::new(0));
        for i in 0u32..3 {
            let m = Rc::clone(&mutex);
            let ord = Rc::clone(&order);
            r.spawn(async move {
                let g = m.lock().await;
                ord.borrow_mut().push(i);
                drop(g);
            });
        }
        r.block_until_idle();
        assert_eq!(*order.borrow(), vec![0, 1, 2], "tasks must serialize");
    }

    /// Structural regression: a task that acquires the SAL writer mutex,
    /// writes, drops the guard, then awaits must release the mutex
    /// before that await — so a concurrent relay/tick task can acquire
    /// it while the first task's await is outstanding (committer pattern:
    /// emit under lock, `.await` outside).
    #[test]
    fn sal_writer_excl_not_held_across_commit_await() {
        let r = make_reactor();
        let mutex: Rc<AsyncMutex<()>> = Rc::new(AsyncMutex::new(()));
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
        assert!(relay_done.get(),
            "concurrent task must have acquired the mutex");
        assert!(commit_done.get(),
            "committer must complete after its ACK is delivered");
    }

    /// Structural regression: the relay loop acquires `sal_writer_excl` for a
    /// synchronous SAL write, then releases it at scope exit before awaiting
    /// the next item from its channel.  If the guard leaked across that await,
    /// a concurrent committer could never acquire the mutex and would deadlock.
    #[test]
    fn sal_writer_excl_not_held_across_relay_recv() {
        let r = make_reactor();
        let mutex: Rc<AsyncMutex<()>> = Rc::new(AsyncMutex::new(()));
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
        assert!(commit_done.get(),
            "committer must have acquired the mutex while relay was parked");
        assert!(relay_done.get(),
            "relay must complete after being unblocked");
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
                if a.get() > m.get() { m.set(a.get()); }
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
        assert!(r_end_pos < w_start_pos, "writer must run after reader finishes: {:?}", o);
    }

    /// A `LockFuture` dropped while parked (e.g. via `select2`) leaves a
    /// stale waker in `AsyncMutex::waiters`.  `release()` must not pop
    /// exactly one waker — doing so risks consuming the stale entry and
    /// leaving all live waiters permanently blocked.
    #[test]
    fn async_mutex_cancelled_waiter_does_not_block_remaining() {
        let r = make_reactor();
        let mutex: Rc<AsyncMutex<()>> = Rc::new(AsyncMutex::new(()));
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

        for _ in 0..20 { r.tick(false); }
        assert!(done.get(),
            "task C must acquire the mutex after task B's cancelled waiter");
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

        for _ in 0..20 { r.tick(false); }
        assert!(done.get(),
            "task C must acquire the write lock after task B's cancelled waiter");
    }

    #[test]
    fn join2_waits_for_both() {
        let r = make_reactor();
        let out: Rc<StdCell<(u32, u32)>> = Rc::new(StdCell::new((0, 0)));
        let out2 = Rc::clone(&out);
        r.block_on(async move {
            let a = async { 3u32 };
            let b = async { 4u32 };
            let (x, y) = join2(a, b).await;
            out2.set((x, y));
        });
        assert_eq!(out.get(), (3, 4));
    }

    #[test]
    fn fsync_future_roundtrip() {
        let r = make_reactor();
        let fd = crate::runtime::sys::memfd_create(b"reactor_fsync_future");
        let rc: Rc<StdCell<i32>> = Rc::new(StdCell::new(1));
        let rc2 = Rc::clone(&rc);
        let fsync = r.fsync(fd);
        r.block_on(async move {
            rc2.set(fsync.await);
        });
        unsafe { libc::close(fd); }
        assert_eq!(rc.get(), 0);
    }

    /// `register_reply_waker` overrides any earlier registration for the
    /// same id (the more recent caller wins), so a stale waker does not
    /// fire by mistake.
    #[test]
    fn register_reply_waker_replaces() {
        let r = make_reactor();
        let waker_q = Arc::clone(&r.inner.run_queue);
        let w1 = make_waker(99, Arc::clone(&waker_q));
        let w2 = make_waker(100, Arc::clone(&waker_q));
        r.register_reply_waker(42, w1);
        r.register_reply_waker(42, w2);
        r.inject_cqe(KIND_REPLY, 42, 0);
        r.drain_injected_cqes();
        let q: Vec<usize> = r.inner.run_queue.lock().unwrap().iter().copied().collect();
        assert!(q.contains(&100), "second waker (key=100) must win");
        assert!(!q.contains(&99), "first waker (key=99) must have been replaced");
    }

    // -- helper futures used by the tests above --

    /// Future that returns Pending exactly once, then Ready.
    struct YieldOnce { yielded: bool }
    impl YieldOnce { fn new() -> Self { YieldOnce { yielded: false } } }
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
    struct DoublyWaking { polls: Rc<StdCell<u32>>, polled: u32 }
    impl Future for DoublyWaking {
        type Output = ();
        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
            let n = self.polled;
            self.polls.set(self.polls.get() + 1);
            if n >= 3 { return Poll::Ready(()); }
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
    fn select_reply_or_timer<'a, T, R>(
        timer: T, reply: R, flag: &'a Rc<StdCell<bool>>,
    ) -> impl Future<Output = ()> + 'a
    where
        T: Future<Output = ()> + 'a,
        R: Future<Output = DecodedWire> + 'a,
    {
        async move {
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
            }).await
        }
    }

    /// Build a minimal `DecodedWire` for tests — only `request_id` is
    /// load-bearing; every other field is zeroed / empty.
    fn synthetic_decoded_wire(req_id: u64) -> DecodedWire {
        use crate::runtime::wire::DecodedControl;
        DecodedWire {
            control: DecodedControl {
                status: 0,
                client_id: 0,
                target_id: 0,
                flags: 0,
                seek_pk: 0,
                seek_col_idx: 0,
                request_id: req_id,
                error_msg: Vec::new(),
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
        r.inject_parked_reply(99, synthetic_decoded_wire(99));
        r.block_until_idle();
        assert!(done.get());
        assert_eq!(r.task_count(), 0);
    }

    /// Routed reply: pops the waker, parks the wire for the awaiter.
    /// No `in_flight` accounting — the tail-chasing ring self-maintains.
    #[test]
    fn route_reply_routes_to_registered_waker() {
        let r = make_reactor();
        r.test_init_state(2);

        let waker = make_waker(0, Arc::clone(&r.inner.run_queue));
        r.register_reply_waker(42, waker);

        r.test_route_reply(1, synthetic_decoded_wire(42));

        assert!(r.inner.parked_replies.borrow().contains_key(&42));
        assert!(!r.inner.reply_wakers.borrow().contains_key(&42));
    }

    /// Unrouted reply (no waker): logged and dropped. Must not
    /// contaminate `parked_replies` — stale entries would keep
    /// non-dead memory alive indefinitely.
    #[test]
    fn route_reply_unrouted_is_logged_and_dropped() {
        let r = make_reactor();
        r.test_init_state(1);
        // No waker registered for req_id=7.

        r.test_route_reply(0, synthetic_decoded_wire(7));

        assert!(r.inner.parked_replies.borrow().is_empty(),
            "unrouted replies must not leak into parked_replies");
    }

    /// Dispatching a KIND_FSYNC CQE parks the CQE `res` verbatim under
    /// the request id — both success (rc=0) and failure (rc<0), since
    /// the caller's `rc < 0` fatal-abort branch depends on it.
    #[test]
    fn fsync_dispatch_parks_rc_verbatim() {
        let r = make_reactor();
        for rc in [0, -5] {
            r.inject_cqe(KIND_FSYNC, 42, rc);
            r.drain_injected_cqes();
            let parked = r.inner.parked_fsync_results.borrow_mut().remove(&42);
            assert_eq!(parked, Some(rc));
        }
    }

    /// End-to-end with a real io_uring: submit fdatasync on a memfd,
    /// block until complete, expect rc=0.  Also asserts
    /// `parked_fsync_results` is drained afterwards (catches leaks).
    #[test]
    fn fsync_real_memfd_roundtrip() {
        let r = make_reactor();
        let fd = crate::runtime::sys::memfd_create(b"reactor_fsync_ok");
        let id = r.submit_fsync(fd);
        let rc = r.block_on_fsync(id);
        unsafe { libc::close(fd); }
        assert_eq!(rc, 0, "fdatasync on a fresh memfd should succeed");
        assert!(r.inner.parked_fsync_results.borrow().is_empty(),
            "block_on_fsync must remove the parked result");
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
        let id = r.submit_fsync(i32::MAX);
        let rc = r.block_on_fsync(id);
        assert!(rc < 0, "fdatasync on a bogus fd must return rc<0, got {}", rc);
    }

    /// `submit_fsync` flushes the SQE to the kernel before returning.
    /// Without the eager submit the CQE would only arrive on the next
    /// `tick`, defeating the Phase-A / tick-evaluation overlap.
    #[test]
    fn fsync_submit_flushes_sqe_before_returning() {
        let r = make_reactor();
        let fd = crate::runtime::sys::memfd_create(b"reactor_fsync_flush");
        let id = r.submit_fsync(fd);

        // Spin briefly (no further ticks driven from outside) until the
        // CQE either arrives in the ring or we time out.  The kernel
        // completes fdatasync on a memfd in microseconds, so ~100 ms
        // gives generous headroom for scheduler jitter without flakiness.
        let deadline = Instant::now() + Duration::from_millis(100);
        let mut got: Option<i32> = None;
        while Instant::now() < deadline {
            r.drain_cqes_into_wakers();
            if let Some(rc) = r.inner.parked_fsync_results.borrow_mut().remove(&id) {
                got = Some(rc);
                break;
            }
        }
        unsafe { libc::close(fd); }
        assert_eq!(got, Some(0),
            "fsync CQE must be available without driving another tick — \
             submit_fsync should flush the SQE eagerly");
    }

    /// The KIND_REPLY / KIND_FSYNC tags in `user_data` are what keep
    /// shared-id spaces from colliding.  With matching ids (42) but
    /// different kinds, each dispatch path must land in its own map.
    #[test]
    fn fsync_and_reply_udata_kinds_do_not_collide() {
        let r = make_reactor();
        // Register a reply waker so KIND_REPLY's dispatch path has
        // something to pop.
        let waker = make_waker(7, Arc::clone(&r.inner.run_queue));
        r.register_reply_waker(42, waker);

        r.inject_cqe(KIND_REPLY, 42, 0);
        r.inject_cqe(KIND_FSYNC, 42, -11);
        r.drain_injected_cqes();

        let q: Vec<usize> = r.inner.run_queue.lock().unwrap()
            .iter().copied().collect();
        assert!(q.contains(&7),
            "KIND_REPLY dispatch must wake the reply waker for id=42");
        assert!(!r.inner.reply_wakers.borrow().contains_key(&42),
            "reply waker must be consumed by KIND_REPLY dispatch");
        let fsync_rc = r.inner.parked_fsync_results.borrow_mut().remove(&42);
        assert_eq!(fsync_rc, Some(-11),
            "KIND_FSYNC dispatch must park its own rc under the same id");
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
        let waker = make_waker(11, Arc::clone(&r.inner.run_queue));
        r.inner.send_wakers.borrow_mut().insert(55, waker);

        r.inject_cqe(KIND_SEND, 55, 1234);
        r.drain_injected_cqes();

        let parked = r.inner.parked_send_results.borrow_mut().remove(&55);
        assert_eq!(parked, Some(1234),
            "KIND_SEND must park the CQE rc verbatim so SendFuture sees it");
        assert!(!r.inner.send_wakers.borrow().contains_key(&55),
            "KIND_SEND must consume the waker entry");
        let q: Vec<usize> = r.inner.run_queue.lock().unwrap()
            .iter().copied().collect();
        assert!(q.contains(&11), "KIND_SEND must wake the send future");
    }

    #[test]
    fn send_cqe_removes_parked_buffer_and_decrements_conn_inflight() {
        let r = make_reactor();
        // Pre-populate an in-flight buffer and a conn with send_inflight=1.
        r.inner.send_buffers_in_flight.borrow_mut()
            .insert(77, Rc::new(vec![0u8; 16]));
        r.inner.send_fd_for_id.borrow_mut().insert(77, 42);
        r.inner.conns.borrow_mut().insert(42, Box::new(io::Conn::new()));
        if let Some(c) = r.inner.conns.borrow_mut().get_mut(&42) {
            c.send_inflight = 1;
        }

        r.inject_cqe(KIND_SEND, 77, 16);
        r.drain_injected_cqes();

        assert!(!r.inner.send_buffers_in_flight.borrow().contains_key(&77),
            "KIND_SEND must drop the parked buffer — leaks linearly with \
             the number of cancelled SendFutures otherwise");
        assert!(!r.inner.send_fd_for_id.borrow().contains_key(&77),
            "KIND_SEND must clear send_fd_for_id so the fd entry does not \
             grow unboundedly");
        let inflight = r.inner.conns.borrow().get(&42).unwrap().send_inflight;
        assert_eq!(inflight, 0,
            "KIND_SEND must decrement conn.send_inflight (gates close_fd)");
    }

    /// A dropped SendFuture with an in-flight SQE MUST hand the buffer
    /// off to the reactor so the kernel doesn't dereference freed
    /// memory when the CQE eventually arrives.  This is
    /// `II.2 io_uring SQE buffer lifetime` made concrete.
    #[test]
    fn send_future_drop_parks_buffer_rc() {
        let r = make_reactor();
        let buf: Rc<Vec<u8>> = Rc::new(vec![0xAB; 64]);
        {
            // Build and drop a SendFuture without it ever becoming Ready.
            let _fut = SendFuture {
                send_id: 88,
                _buf: Some(Rc::clone(&buf)),
                inner: Rc::clone(&r.inner),
            };
        }
        let parked = r.inner.send_buffers_in_flight.borrow();
        let stored = parked.get(&88).expect("drop must park buffer");
        assert_eq!(stored.as_slice(), buf.as_slice());
    }

    // ─────────────────────────────────────────────────────────────────
    // Per-fd state lifecycle (`register_conn` / `recv_closed` /
    // `pending_recv`).  III.4 in async-invariants.md.
    //
    // Regression guard: a kernel-reused fd number carrying the
    // previous incarnation's `recv_closed = true` flag caused the new
    // connection to see immediate EOF on its first recv.
    // ─────────────────────────────────────────────────────────────────

    /// Simulate fd reuse: mark fd=99 as closed, then register a new
    /// connection on the same fd.  The stale `recv_closed` sentinel
    /// MUST be cleared — otherwise `recv(99).await` on the new conn
    /// returns `None` immediately and the connection is dead on arrival.
    #[test]
    fn register_conn_clears_stale_recv_closed_sentinel() {
        let r = make_reactor();
        // Pretend fd=99's previous incarnation closed.
        r.inner.recv_closed.borrow_mut().insert(99, true);
        // Work around: register_conn submits a RECV SQE on the fd, which
        // would fail with EBADF if fd=99 isn't open. We skirt that by
        // opening a pipe and using the read end (a valid fd that won't
        // spuriously produce data during the test).
        unsafe {
            let mut fds = [0i32; 2];
            assert_eq!(libc::pipe(fds.as_mut_ptr()), 0);
            let (read_end, write_end) = (fds[0], fds[1]);
            // Prime recv_closed under the pipe fd number.
            r.inner.recv_closed.borrow_mut().insert(read_end, true);

            r.register_conn(read_end);

            assert!(!r.inner.recv_closed.borrow().contains_key(&read_end),
                "register_conn must clear stale recv_closed so a new \
                 connection on a reused fd doesn't see phantom EOF");
            assert!(r.inner.conns.borrow().contains_key(&read_end));

            libc::close(read_end);
            libc::close(write_end);
        }
    }

    /// `register_conn` installs a fresh, empty `pending_recv` queue.
    /// If stale entries survive the reset, messages from the old
    /// incarnation would be delivered to the new handler.
    #[test]
    fn register_conn_clears_stale_pending_recv() {
        let r = make_reactor();
        unsafe {
            let mut fds = [0i32; 2];
            assert_eq!(libc::pipe(fds.as_mut_ptr()), 0);
            let (read_end, write_end) = (fds[0], fds[1]);
            // Seed pending_recv with an empty queue (the hot path — a
            // full queue with non-empty buffers would also trip
            // debug_assert, which we can't catch cleanly in a test).
            r.inner.pending_recv.borrow_mut()
                .insert(read_end, std::collections::VecDeque::new());

            r.register_conn(read_end);

            let pr = r.inner.pending_recv.borrow();
            assert!(pr.get(&read_end).map(|q| q.is_empty()).unwrap_or(true),
                "register_conn must leave pending_recv empty for the \
                 new incarnation, even if an entry existed");

            libc::close(read_end);
            libc::close(write_end);
        }
    }

    /// Concrete test of the partial-send contract: io_uring's OP_SEND on
    /// a stream socket can return `rc < len`.  Uses a real AF_UNIX
    /// socketpair with a deliberately-small send buffer so the kernel
    /// returns partial counts, forcing `send_buffer`'s loop to resubmit
    /// the remaining slice until the full buffer drains.
    #[test]
    fn send_buffer_loops_until_full_payload_sent_over_socketpair() {
        unsafe {
            let mut fds = [0i32; 2];
            let rc = libc::socketpair(libc::AF_UNIX, libc::SOCK_STREAM,
                0, fds.as_mut_ptr());
            assert_eq!(rc, 0, "socketpair");
            let (sender, receiver) = (fds[0], fds[1]);
            // Shrink both buffers to ~8 KB so a 200 KB payload is
            // guaranteed to split across multiple OP_SEND CQEs.
            let bufsz: i32 = 8 * 1024;
            libc::setsockopt(sender, libc::SOL_SOCKET, libc::SO_SNDBUF,
                &bufsz as *const _ as *const libc::c_void,
                std::mem::size_of::<i32>() as u32);
            libc::setsockopt(receiver, libc::SOL_SOCKET, libc::SO_RCVBUF,
                &bufsz as *const _ as *const libc::c_void,
                std::mem::size_of::<i32>() as u32);

            let r: Rc<Reactor> = Rc::new(make_reactor());
            // Register the sender in conns so send_inflight accounting
            // has something to touch (matches real-world flow).
            r.inner.conns.borrow_mut().insert(sender, Box::new(io::Conn::new()));

            // 200 KB > both SNDBUF and RCVBUF → guaranteed partial sends.
            let payload = vec![0x5Au8; 200 * 1024];
            let payload_len = payload.len();
            let sender_fd = sender;

            // Drain the receiver in a background thread so the kernel
            // socket-buffer drains and subsequent OP_SEND can make
            // progress.  Without the drain, send_buffer blocks
            // indefinitely at ~8 KB.
            let drain_t = std::thread::spawn(move || {
                let mut total = 0usize;
                let mut scratch = vec![0u8; 32 * 1024];
                while total < payload_len {
                    let n = libc::read(receiver,
                        scratch.as_mut_ptr() as *mut libc::c_void,
                        scratch.len());
                    if n <= 0 { break; }
                    total += n as usize;
                }
                libc::close(receiver);
                total
            });

            let r2 = Rc::clone(&r);
            let sent = r.block_on(async move {
                r2.send_buffer(sender_fd, payload).await
            });
            let received = drain_t.join().expect("drain thread");

            libc::close(sender);
            assert_eq!(sent as usize, payload_len,
                "send_buffer must loop on partial CQEs until the full \
                 payload is sent (got rc={}, expected {})", sent, payload_len);
            assert_eq!(received, payload_len,
                "receiver must observe every byte — a truncated send_buffer \
                 would leave the client blocked waiting for bytes that never \
                 arrive");
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
        use crate::runtime::sal::FLAG_EXCHANGE;
        DecodedWire {
            control: DecodedControl {
                status: 0,
                client_id: 0,
                target_id: view_id as u64,
                flags: FLAG_EXCHANGE as u64,
                seek_pk: source_id as u128,
                seek_col_idx: 0,
                request_id: req_id,
                error_msg: Vec::new(),
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
        let waker = make_waker(0, Arc::clone(&r.inner.run_queue));
        r.register_reply_waker(42, waker);

        let exch = synthetic_exchange_wire(/*view_id*/ 100, /*req_id*/ 42);
        r.test_route_reply(0, exch);

        assert!(r.inner.reply_wakers.borrow().contains_key(&42),
            "FLAG_EXCHANGE must NOT consume the tick waker");
        assert!(r.inner.parked_replies.borrow().is_empty(),
            "FLAG_EXCHANGE must NOT park the wire for await_reply");
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

        let w0 = make_waker(0, Arc::clone(&r.inner.run_queue));
        let w1 = make_waker(1, Arc::clone(&r.inner.run_queue));
        r.register_reply_waker(10, w0);
        r.register_reply_waker(11, w1);

        r.test_route_reply(0, synthetic_exchange_wire(99, 10));
        assert!(rx.try_recv().is_none(),
            "single-worker FLAG_EXCHANGE must not produce a relay");
        r.test_route_reply(1, synthetic_exchange_wire(99, 11));
        let relay = rx.try_recv().expect("complete view must produce a relay");
        assert_eq!(relay.view_id, 99);
        assert_eq!(relay.payloads.len(), 2);
        // Final ACKs (no FLAG_EXCHANGE) for the same req_ids wake
        // their tick wakers and park the wires.
        r.test_route_reply(0, synthetic_decoded_wire(10));
        r.test_route_reply(1, synthetic_decoded_wire(11));
        assert!(r.inner.parked_replies.borrow().contains_key(&10));
        assert!(r.inner.parked_replies.borrow().contains_key(&11));
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

        // Register one reply waker per worker to satisfy route_reply's
        // parked-waker pre-flight check.
        for w in 0..4 {
            let waker = make_waker(w, Arc::clone(&r.inner.run_queue));
            r.register_reply_waker(100 + w as u64, waker);
        }

        // Interleave two rounds for the same view_id=100:
        //   round A: (view_id=100, source_id=10), workers 0..4
        //   round B: (view_id=100, source_id=20), workers 0..4
        // Arrival order deliberately mixed; neither round is strictly
        // before the other in wall-clock order.
        let _ = r.test_route_reply(0, synthetic_exchange_wire_src(100, 10, 100));
        let _ = r.test_route_reply(0, synthetic_exchange_wire_src(100, 20, 100));
        let _ = r.test_route_reply(1, synthetic_exchange_wire_src(100, 20, 101));
        let _ = r.test_route_reply(1, synthetic_exchange_wire_src(100, 10, 101));
        let _ = r.test_route_reply(2, synthetic_exchange_wire_src(100, 10, 102));
        let _ = r.test_route_reply(3, synthetic_exchange_wire_src(100, 10, 103));
        // Round A must now be complete (4 workers reported).
        let ra = rx.try_recv().expect("round A should produce a relay");
        assert_eq!(ra.view_id, 100);
        assert_eq!(ra.source_id, 10);
        assert_eq!(ra.payloads.len(), 4);
        // Round B still incomplete — worker 2+3 haven't reported for src=20.
        assert!(rx.try_recv().is_none(), "round B must not be ready yet");

        let _ = r.test_route_reply(2, synthetic_exchange_wire_src(100, 20, 102));
        let _ = r.test_route_reply(3, synthetic_exchange_wire_src(100, 20, 103));
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
            let never = TimerFuture::new(
                Instant::now() + Duration::from_secs(60),
                inner,
            );
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
        let waker = make_waker(999, Arc::clone(&r.inner.run_queue));
        // Build a TimerFuture, poll once to register, then drop it.
        let mut tf = TimerFuture::new(
            Instant::now() + Duration::from_millis(10),
            inner,
        );
        let mut cx = Context::from_waker(&waker);
        let pinned = Pin::new(&mut tf);
        let _ = pinned.poll(&mut cx);
        assert_eq!(r.inner.timer_wakers.borrow().len(), 1);
        drop(tf);
        // Drive ticks past the deadline; the cancelled CQE must be
        // discarded without waking key=999.
        std::thread::sleep(Duration::from_millis(20));
        for _ in 0..4 { r.tick(false); }
        let q: Vec<usize> = r.inner.run_queue.lock().unwrap()
            .iter().copied().collect();
        assert!(!q.contains(&999),
            "cancelled timer must not wake its original waker");
    }

    /// AsyncMutex serialises tasks: even when several tasks race on
    /// `lock().await` only one runs the critical section at a time.
    /// Mirrors sal_writer_excl's role for III.3b.
    #[test]
    fn sal_writer_excl_serializes_tasks() {
        let r = make_reactor();
        let order: Rc<RefCell<Vec<u32>>> = Rc::new(RefCell::new(Vec::new()));
        let mutex: Rc<AsyncMutex<()>> = Rc::new(AsyncMutex::new(()));
        for i in 0u32..3 {
            let m = Rc::clone(&mutex);
            let ord = Rc::clone(&order);
            r.spawn(async move {
                let g = m.lock().await;
                let len = ord.borrow().len();
                ord.borrow_mut().push(i);
                // Fail loudly if any other task entered the section while
                // this one held the lock.
                assert_eq!(ord.borrow().len(), len + 1);
                drop(g);
            });
        }
        r.block_until_idle();
        assert_eq!(order.borrow().len(), 3);
    }

    /// Cross-process stress: a forked child publishes thousands of
    /// messages into a shared W2M ring while the parent drains them
    /// through the reactor's `FUTEX_WAITV` + `W2mReceiver` pipeline.
    ///
    /// Regression guard for two distinct hazards:
    /// 1. The lost-wake race in `refresh_futex_waitv_vals`: at this
    ///    scale the master takes many `refresh → arm` cycles, each a
    ///    potential lost-wake window. A missed wake hangs the test
    ///    (caught by the reactor-timer guard).
    /// 2. The writer-crosses-reader data-loss bug in the SKIP-wrap
    ///    path: capacity is small enough (64 KiB) and message count
    ///    high enough (500 × ~280 B ≈ 140 KiB) to force multiple
    ///    SKIP-wraps. Truncated or out-of-order delivery fails the
    ///    `ids` assertion.
    ///
    /// Waker-install ordering is load-bearing: the reply futures
    /// must register their wakers BEFORE `attach_w2m` runs its
    /// initial drain, otherwise replies drained during attach are
    /// logged as "unrouted" and dropped. The `poll_nonblocking`
    /// between `spawn` and `attach_w2m` exists for this reason.
    #[test]
    fn w2m_cross_process_stress_drains_all_messages_via_reactor() {
        use std::time::Duration;
        use crate::runtime::wire as ipc;
        use crate::runtime::wire::STATUS_OK;
        use crate::runtime::w2m::{W2mReceiver, W2mWriter};
        use crate::runtime::w2m_ring;
        use crate::runtime::reactor::{join_all, select2, Either};

        const CAPACITY: usize = 64 * 1024;
        const N_MESSAGES: u64 = 500;
        const TIMEOUT_SECS: u64 = 30;

        let fd = crate::runtime::sys::memfd_create(b"w2m_stress");
        assert!(fd >= 0, "memfd_create failed");
        assert_eq!(crate::sys::ftruncate(fd, CAPACITY as i64), 0);
        let ptr = crate::runtime::sys::mmap_shared(fd, CAPACITY);
        assert!(!ptr.is_null(), "mmap_shared failed");
        unsafe { w2m_ring::init_region_for_tests(ptr, CAPACITY as u64); }

        let pid = unsafe { libc::fork() };
        assert!(pid >= 0, "fork failed");
        if pid == 0 {
            // Child: publish a monotonic req_id stream as fast as
            // possible. The parent's wake protocol must not drop
            // any of them under the resulting drain-refresh-arm
            // race pressure.
            let writer = W2mWriter::new(ptr, CAPACITY as u64);
            let sz = ipc::wire_size(STATUS_OK, &[], None, None, None, None);
            for req_id in 1..=N_MESSAGES {
                writer.send_encoded(sz, |buf| {
                    ipc::encode_wire_into(
                        buf, 0, 0, 0, 0,
                         0u128, 0, req_id, STATUS_OK, &[], None, None, None, None,
                    );
                });
            }
            unsafe { libc::_exit(0); }
        }

        // Parent: drain via the reactor's FUTEX_WAITV + W2mReceiver path.
        let reactor = Reactor::new(16).expect("reactor");

        let received: Rc<RefCell<Vec<u64>>> = Rc::new(RefCell::new(Vec::new()));
        let reply_futs: Vec<_> = (1..=N_MESSAGES)
            .map(|i| reactor.await_reply(i))
            .collect();
        {
            let received = Rc::clone(&received);
            reactor.spawn(async move {
                let replies = join_all(reply_futs).await;
                *received.borrow_mut() = replies.into_iter()
                    .map(|r| r.control.request_id)
                    .collect();
            });
        }
        // One tick polls the spawned task, which walks join_all and
        // registers every ReplyFuture's waker before attach drains.
        reactor.poll_nonblocking();

        reactor.attach_w2m(W2mReceiver::new(vec![ptr]));

        let inner = Rc::clone(&reactor.inner);
        let received_check = Rc::clone(&received);
        let outcome = reactor.block_on(async move {
            let timeout = TimerFuture::new(
                Instant::now() + Duration::from_secs(TIMEOUT_SECS),
                inner,
            );
            let watch = async move {
                while received_check.borrow().is_empty() {
                    YieldOnce::new().await;
                }
            };
            select2(watch, timeout).await
        });
        if let Either::B(()) = outcome {
            unsafe { libc::kill(pid, libc::SIGKILL); }
            panic!(
                "reactor stalled with {} replies received after {}s — \
                 lost-wake symptom",
                received.borrow().len(),
                TIMEOUT_SECS,
            );
        }

        let mut ids = received.borrow().clone();
        assert_eq!(ids.len(), N_MESSAGES as usize);
        ids.sort();
        let expected: Vec<u64> = (1..=N_MESSAGES).collect();
        assert_eq!(ids, expected, "every published req_id must round-trip");

        let mut status: i32 = 0;
        unsafe { libc::waitpid(pid, &mut status, 0); }

        // AsyncCancel the in-flight FUTEX_WAITV before the storage drops.
        reactor.request_shutdown();

        unsafe {
            libc::munmap(ptr as *mut libc::c_void, CAPACITY);
            libc::close(fd);
        }
    }

    /// High-volume variant of the W2M cross-process stress: 5 000
    /// messages through a 64 KiB ring forces dozens of SKIP-wraps and
    /// many writer-park/wake cycles. Catches any flake in the wake
    /// protocol or virtual-cursor accounting that only manifests at
    /// scale.
    #[test]
    fn w2m_cross_process_stress_high_volume() {
        use std::time::Duration;
        use crate::runtime::wire as ipc;
        use crate::runtime::wire::STATUS_OK;
        use crate::runtime::w2m::{W2mReceiver, W2mWriter};
        use crate::runtime::w2m_ring;
        use crate::runtime::reactor::{join_all, select2, Either};

        const CAPACITY: usize = 64 * 1024;
        const N_MESSAGES: u64 = 5_000;
        const TIMEOUT_SECS: u64 = 60;

        let fd = crate::runtime::sys::memfd_create(b"w2m_stress_hv");
        assert!(fd >= 0);
        assert_eq!(crate::sys::ftruncate(fd, CAPACITY as i64), 0);
        let ptr = crate::runtime::sys::mmap_shared(fd, CAPACITY);
        assert!(!ptr.is_null());
        unsafe { w2m_ring::init_region_for_tests(ptr, CAPACITY as u64); }

        let pid = unsafe { libc::fork() };
        assert!(pid >= 0);
        if pid == 0 {
            let writer = W2mWriter::new(ptr, CAPACITY as u64);
            let sz = ipc::wire_size(STATUS_OK, &[], None, None, None, None);
            for req_id in 1..=N_MESSAGES {
                writer.send_encoded(sz, |buf| {
                    ipc::encode_wire_into(
                        buf, 0, 0, 0, 0,
                         0u128, 0, req_id, STATUS_OK, &[], None, None, None, None,
                    );
                });
            }
            unsafe { libc::_exit(0); }
        }

        let reactor = Reactor::new(16).expect("reactor");
        let received: Rc<RefCell<Vec<u64>>> = Rc::new(RefCell::new(Vec::new()));
        let reply_futs: Vec<_> = (1..=N_MESSAGES)
            .map(|i| reactor.await_reply(i))
            .collect();
        {
            let received = Rc::clone(&received);
            reactor.spawn(async move {
                let replies = join_all(reply_futs).await;
                *received.borrow_mut() = replies.into_iter()
                    .map(|r| r.control.request_id)
                    .collect();
            });
        }
        reactor.poll_nonblocking();

        reactor.attach_w2m(W2mReceiver::new(vec![ptr]));

        let inner = Rc::clone(&reactor.inner);
        let received_check = Rc::clone(&received);
        let outcome = reactor.block_on(async move {
            let timeout = TimerFuture::new(
                Instant::now() + Duration::from_secs(TIMEOUT_SECS),
                inner,
            );
            let watch = async move {
                while received_check.borrow().is_empty() {
                    YieldOnce::new().await;
                }
            };
            select2(watch, timeout).await
        });
        if let Either::B(()) = outcome {
            unsafe { libc::kill(pid, libc::SIGKILL); }
            panic!(
                "reactor stalled with {} replies after {}s",
                received.borrow().len(), TIMEOUT_SECS,
            );
        }

        let mut ids = received.borrow().clone();
        assert_eq!(ids.len(), N_MESSAGES as usize);
        ids.sort();
        let expected: Vec<u64> = (1..=N_MESSAGES).collect();
        assert_eq!(ids, expected);

        let mut status: i32 = 0;
        unsafe { libc::waitpid(pid, &mut status, 0); }

        reactor.request_shutdown();
        unsafe {
            libc::munmap(ptr as *mut libc::c_void, CAPACITY);
            libc::close(fd);
        }
    }

    // ─────────────────────────────────────────────────────────────────
    // `closing_fds` sync: every path that sets conn.closing=true must
    // also insert into closing_fds so reap_closing_conns finds the fd.
    // ─────────────────────────────────────────────────────────────────

    #[test]
    fn handle_recv_cqe_error_populates_closing_fds() {
        let r = make_reactor();
        r.inner.conns.borrow_mut().insert(55, Box::new(io::Conn::new()));

        r.inject_cqe(KIND_RECV, 55, -1);
        r.drain_injected_cqes();

        assert!(r.inner.closing_fds.borrow().contains(&55),
            "res<=0 recv CQE must insert fd into closing_fds");
        assert!(r.inner.recv_closed.borrow().get(&55).copied().unwrap_or(false),
            "res<=0 recv CQE must set recv_closed sentinel");
    }

    #[test]
    fn reap_closing_conns_removes_idle_closing_fd() {
        let r = make_reactor();
        // Insert a conn with no in-flight SQEs and mark it closing manually,
        // as if handle_recv_cqe had already run.
        let mut conn = Box::new(io::Conn::new());
        conn.closing = true;
        r.inner.conns.borrow_mut().insert(88, conn);
        r.inner.closing_fds.borrow_mut().insert(88);

        r.reap_closing_conns();

        assert!(!r.inner.conns.borrow().contains_key(&88),
            "idle closing conn must be removed from conns");
        assert!(!r.inner.closing_fds.borrow().contains(&88),
            "reaped fd must be removed from closing_fds");
    }

    #[test]
    fn reap_closing_conns_defers_conn_with_outstanding_send() {
        let r = make_reactor();
        let mut conn = Box::new(io::Conn::new());
        conn.closing = true;
        conn.send_inflight = 1; // outstanding send SQE
        r.inner.conns.borrow_mut().insert(77, conn);
        r.inner.closing_fds.borrow_mut().insert(77);

        r.reap_closing_conns();

        assert!(r.inner.conns.borrow().contains_key(&77),
            "conn with outstanding send must NOT be reaped yet");
        assert!(r.inner.closing_fds.borrow().contains(&77),
            "conn deferred by outstanding send must stay in closing_fds");
    }

    // ─────────────────────────────────────────────────────────────────
    // RecvState state-machine unit tests.
    //
    // io.rs has zero unit tests even though RecvState has four distinct
    // transitions (NeedMore, HeaderDone, MessageDone, Disconnect).
    // These tests drive the state machine directly — no io_uring needed.
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
        rs.hdr_buf = 8u32.to_le_bytes();
        assert!(matches!(rs.advance(4), io::RecvAdvance::HeaderDone));

        let buf = unsafe { libc::malloc(8) as *mut u8 };
        rs.start_payload(buf, 8);

        // Partial payload.
        assert!(matches!(rs.advance(5), io::RecvAdvance::NeedMore));
        // Remaining 3 bytes complete the message.
        assert!(matches!(rs.advance(3), io::RecvAdvance::MessageDone));

        let (ret_ptr, ret_len) = rs.take_message();
        assert_eq!(ret_ptr, buf);
        assert_eq!(ret_len, 8);

        // After take_message the state must be back in header phase.
        let (_, rem) = rs.remaining();
        assert_eq!(rem, 4, "take_message must reset to header phase");

        unsafe { libc::free(buf as *mut libc::c_void); }
    }

    #[test]
    fn recv_state_free_payload_resets_to_header() {
        let mut rs = io::RecvState::new();
        let buf = unsafe { libc::malloc(4) as *mut u8 };
        rs.start_payload(buf, 4);
        // free_payload must release the allocation and reset the phase.
        rs.free_payload();
        let (_, rem) = rs.remaining();
        assert_eq!(rem, 4, "free_payload must reset to Header{{pos:0}}");
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
        let w_pos  = o.iter().position(|&s| s == "W").expect("W not seen");
        let r2_pos = o.iter().position(|&s| s == "R2").expect("R2 not seen");
        assert!(w_pos < r2_pos,
            "writer-preference violated: W must precede R2, got {:?}", o);
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
            for _ in 0..10 { YieldOnce::new().await; }
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
        for _ in 0..5 { r.tick(false); }
        assert!(!done.get(),
            "C must be blocked while write waiter B is alive");

        // Cancel B: WriteFuture::Drop path 3 must wake C.
        drop(cancel_tx);
        for _ in 0..5 { r.tick(false); }
        assert!(done.get(),
            "C must unblock when the last write waiter (B) is cancelled");
    }

    // ─────────────────────────────────────────────────────────────────
    // KIND_ACCEPT CQE dispatch.
    // ─────────────────────────────────────────────────────────────────

    #[test]
    fn dispatch_accept_queues_fd_when_res_non_negative() {
        let r = make_reactor();
        // flags=0 → CQE_F_MORE not set → re-arm attempted; server_fd=-1 guards it.
        r.inject_cqe(KIND_ACCEPT, 0, 7);
        r.drain_injected_cqes();
        let q: Vec<i32> = r.inner.accept_queue.borrow().iter().copied().collect();
        assert_eq!(q, vec![7], "KIND_ACCEPT res>=0 must push the fd to accept_queue");
    }

    #[test]
    fn dispatch_accept_wakes_waiter_when_present() {
        let r = make_reactor();
        let waker = make_waker(42, Arc::clone(&r.inner.run_queue));
        *r.inner.accept_waker.borrow_mut() = Some(waker);

        r.inject_cqe(KIND_ACCEPT, 0, 5);
        r.drain_injected_cqes();

        let q: Vec<usize> = r.inner.run_queue.lock().unwrap().iter().copied().collect();
        assert!(q.contains(&42), "KIND_ACCEPT must wake the registered accept_waker");
        assert!(r.inner.accept_waker.borrow().is_none(),
            "KIND_ACCEPT must consume (take) the accept_waker");
    }

    #[test]
    fn dispatch_accept_ignores_error_result() {
        let r = make_reactor();
        r.inject_cqe(KIND_ACCEPT, 0, -libc::ECONNABORTED);
        r.drain_injected_cqes();
        assert!(r.inner.accept_queue.borrow().is_empty(),
            "KIND_ACCEPT with res<0 must not push to accept_queue");
    }

    // ─────────────────────────────────────────────────────────────────
    // join_all edge cases.
    // ─────────────────────────────────────────────────────────────────

    #[test]
    fn join_all_empty_returns_empty_vec() {
        let r = make_reactor();
        let result = r.block_on(async {
            join_all(std::iter::empty::<std::future::Ready<i32>>()).await
        });
        assert!(result.is_empty(), "join_all on empty iterator must return empty vec");
    }

    #[test]
    fn join_all_single_future_completes() {
        let r = make_reactor();
        let result = r.block_on(async {
            join_all(std::iter::once(async { 99u32 })).await
        });
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
    // alloc_request_id wrap: u64::MAX is reserved; the counter must
    // skip it and wrap to 1.
    // ─────────────────────────────────────────────────────────────────

    #[test]
    fn alloc_request_id_skips_max_and_wraps_to_one() {
        let r = make_reactor();
        // Position counter so the next allocation returns u64::MAX - 1
        // and the one after would naturally hit u64::MAX (reserved).
        r.inner.next_request_id.set(u64::MAX - 1);
        let id1 = r.alloc_request_id();
        assert_eq!(id1, u64::MAX - 1);
        let id2 = r.alloc_request_id();
        assert_eq!(id2, 1, "counter must skip u64::MAX and wrap to 1");
        assert_ne!(id2, u64::MAX, "u64::MAX is reserved and must never be returned");
    }
}
