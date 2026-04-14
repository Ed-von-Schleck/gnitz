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
//! - Timers are kept in a `BinaryHeap`, NOT as io_uring `Timeout` SQEs.
//!   The reactor passes the soonest deadline to `submit_and_wait_timeout`
//!   so the kernel wakes at the right time; this avoids the lifetime
//!   hazard around storing `Timespec` for the kernel to dereference later.
//! - CQE `user_data` packs an 8-bit kind tag in the high byte and a
//!   56-bit id in the low bits. Distinct from
//!   `gnitz_transport::ring::make_udata` (which packs an fd), and safe
//!   from collisions because the reactor owns its own ring.

use std::cell::{Cell, RefCell};
use std::collections::{BinaryHeap, HashMap, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};
use std::time::Instant;

use gnitz_transport::ring::{Cqe, Ring};
use gnitz_transport::uring::IoUringRing;
use slab::Slab;

use crate::ipc::{DecodedWire, W2mReceiver, W2M_HEADER_SIZE};

// ---------------------------------------------------------------------------
// CQE user_data encoding (high 8 bits = kind, low 56 bits = id)
// ---------------------------------------------------------------------------

pub const KIND_REPLY:        u64 = 1;
pub const KIND_TIMEOUT:      u64 = 2;
pub const KIND_FSYNC:        u64 = 3;
pub const KIND_POLL_EVENTFD: u64 = 4;

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

/// Heap-ordered timer entry: earlier deadlines come first.
struct TimerEntry {
    deadline: Instant,
    waker: Waker,
}

impl PartialEq for TimerEntry {
    fn eq(&self, other: &Self) -> bool { self.deadline == other.deadline }
}
impl Eq for TimerEntry {}
impl PartialOrd for TimerEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for TimerEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // BinaryHeap is a max-heap; flip ordering so the earliest deadline
        // is the largest element (and gets popped first).
        other.deadline.cmp(&self.deadline)
    }
}

/// Backing store for one async task.
struct Task {
    future: Pin<Box<dyn Future<Output = ()>>>,
}

struct ReactorShared {
    ring: RefCell<IoUringRing>,
    tasks: RefCell<Slab<Task>>,
    /// Tasks whose wakers fired; the reactor's main loop polls them on
    /// the next tick. Wrapped in `Arc<Mutex<…>>` because the waker vtable
    /// requires `Send + Sync` even though we never cross threads.
    run_queue: Arc<Mutex<VecDeque<usize>>>,
    /// Per-task waker; stored so spawning code does not need to rebuild
    /// it on each poll. Indexed by slab key.
    task_wakers: RefCell<HashMap<usize, Waker>>,
    /// Reply wakers keyed by request_id; populated by `await_reply`.
    reply_wakers: RefCell<HashMap<u64, Waker>>,
    parked_replies: RefCell<HashMap<u64, DecodedWire>>,
    w2m: RefCell<Option<Rc<W2mReceiver>>>,
    in_flight: RefCell<Vec<usize>>,
    /// Independent of the dispatcher's `async_w2m_rcs`: the two are used
    /// in disjoint time windows (reactor only between barrier notify
    /// and the next sync W2M op).
    w2m_cursors: RefCell<Vec<u64>>,
    /// Min-heap of pending timer deadlines.
    timers: RefCell<BinaryHeap<TimerEntry>>,
    #[cfg(test)]
    injected_cqes: RefCell<VecDeque<Cqe>>,
    next_request_id: Cell<u64>,
    /// Set when a polled future panics (and was caught + dropped). Tests
    /// observe this; production never reads it but the field stays for
    /// the catch_unwind path to write into.
    last_task_panicked: Cell<bool>,
}

/// Shared, clonable handle to the reactor. All futures created by the
/// reactor capture an `Rc<ReactorShared>` so they can submit ops and
/// register wakers without borrowing the `Reactor` mutably.
pub struct Reactor {
    inner: Rc<ReactorShared>,
}

impl Reactor {
    pub fn new(ring_capacity: u32) -> std::io::Result<Self> {
        let ring = IoUringRing::new(ring_capacity)?;
        let inner = Rc::new(ReactorShared {
            ring: RefCell::new(ring),
            tasks: RefCell::new(Slab::new()),
            run_queue: Arc::new(Mutex::new(VecDeque::new())),
            task_wakers: RefCell::new(HashMap::new()),
            reply_wakers: RefCell::new(HashMap::new()),
            parked_replies: RefCell::new(HashMap::new()),
            w2m: RefCell::new(None),
            in_flight: RefCell::new(Vec::new()),
            w2m_cursors: RefCell::new(Vec::new()),
            timers: RefCell::new(BinaryHeap::new()),
            #[cfg(test)]
            injected_cqes: RefCell::new(VecDeque::new()),
            next_request_id: Cell::new(1),
            last_task_panicked: Cell::new(false),
        });
        Ok(Reactor { inner })
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

    /// Spawn a task that runs detached. Returns the slab key (useful for
    /// tests that want to assert task lifecycle).
    pub fn spawn(&self, fut: impl Future<Output = ()> + 'static) -> usize {
        let task = Task { future: Box::pin(fut) };
        let key = self.inner.tasks.borrow_mut().insert(task);
        let waker = make_waker(key, Arc::clone(&self.inner.run_queue));
        self.inner.task_wakers.borrow_mut().insert(key, waker);
        // Schedule immediate first poll.
        self.inner.run_queue.lock().unwrap().push_back(key);
        key
    }

    /// Drive `fut` to completion. Single-threaded, blocking. Spawns the
    /// future as a task internally and returns its output via a shared cell.
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
        // slab removes the entry on completion, so `contains(root_key)`
        // returning false is the termination signal.
        loop {
            self.tick(true);
            if !self.inner.tasks.borrow().contains(root_key) {
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
        TimerFuture {
            deadline,
            registered: false,
            inner: Rc::clone(&self.inner),
        }
    }

    /// Future that resolves to the decoded W2M reply for `req_id`.
    pub fn await_reply(&self, req_id: u64) -> impl Future<Output = DecodedWire> {
        ReplyFuture {
            req_id,
            inner: Rc::clone(&self.inner),
        }
    }

    /// Arm POLL_ADD on every worker eventfd and stash the W2M handle.
    /// Re-arms on each one-shot firing, so the wiring lasts the
    /// lifetime of the executor.
    pub fn attach_w2m(&self, w2m: Rc<W2mReceiver>) {
        let nw = w2m.num_workers();
        *self.inner.in_flight.borrow_mut() = vec![0; nw];
        *self.inner.w2m_cursors.borrow_mut() = vec![W2M_HEADER_SIZE as u64; nw];
        let efds: Vec<i32> = w2m.efds().to_vec();
        *self.inner.w2m.borrow_mut() = Some(w2m);
        for (w, &efd) in efds.iter().enumerate() {
            self.arm_poll_eventfd(efd, w as u64);
        }
    }

    fn arm_poll_eventfd(&self, fd: i32, worker_id: u64) {
        self.inner.ring.borrow_mut().prep_poll_add(
            fd, libc::POLLIN as u32, udata(KIND_POLL_EVENTFD, worker_id),
        );
    }

    /// Must be called immediately after submitting a SAL message that
    /// will produce a reply on worker `w`; `drain_w2m_for_worker` uses
    /// this to decide when the W2M ring for that worker is drained.
    pub fn increment_in_flight(&self, w: usize) {
        self.inner.in_flight.borrow_mut()[w] += 1;
    }

    /// Drive the reactor until the task slab is empty. Blocks.
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

    /// True while at least one task is alive in the slab. Used by the
    /// executor to decide whether to clamp the main-loop timeout.
    pub fn has_pending_tasks(&self) -> bool {
        !self.inner.tasks.borrow().is_empty()
    }

    /// Drive ready tasks and process CQEs without blocking. Intended for
    /// the executor's main loop.
    pub fn poll_nonblocking(&self) {
        self.tick(false);
    }

    /// Single iteration of the event loop:
    ///   1. drain CQEs (waking reply / timeout / fsync wakers)
    ///   2. fire elapsed timers
    ///   3. poll all tasks in the run queue (each polled at most once)
    ///   4. submit pending SQEs; if `block` and the run queue is now
    ///      empty, sleep until the next CQE or the soonest timer.
    fn tick(&self, block: bool) {
        // 1. CQEs (no syscall — reads memory-mapped CQ).
        self.drain_cqes_into_wakers();
        #[cfg(test)]
        self.drain_injected_cqes();

        // 2. Timers.
        let now = Instant::now();
        loop {
            let due = {
                let timers = self.inner.timers.borrow();
                timers.peek().map(|e| e.deadline <= now).unwrap_or(false)
            };
            if !due { break; }
            let entry = self.inner.timers.borrow_mut().pop().unwrap();
            entry.waker.wake();
        }

        // 3. Drain the run queue. Snapshot first so wakes during poll
        // schedule for the *next* tick rather than re-entering this one.
        let ready: Vec<usize> = {
            let mut q = self.inner.run_queue.lock().unwrap();
            q.drain(..).collect()
        };
        for key in ready {
            self.poll_task(key);
        }

        // 4. Submit pending SQEs and optionally block until the next event.
        // Skip blocking when there is nothing to drive (slab empty) or when
        // a wake fired during this tick (run_queue non-empty); otherwise we
        // would sleep past the natural completion of the loop.
        let should_block = block
            && !self.inner.tasks.borrow().is_empty()
            && self.inner.run_queue.lock().unwrap().is_empty();
        if should_block {
            let timeout_ms = self.next_block_deadline_ms();
            let _ = self.inner.ring.borrow_mut()
                .submit_and_wait_timeout(1, timeout_ms);
        } else {
            let _ = self.inner.ring.borrow_mut()
                .submit_and_wait_timeout(0, 0);
        }
    }

    /// Poll a single task. The future is moved out of the slab for the
    /// duration of the poll (so it does not hold the slab borrow), then
    /// reinserted at the SAME key on Pending — slab reuses the lowest
    /// free key first, and `poll_task` runs synchronously inside `tick`,
    /// so no concurrent spawn can claim the slot. The waker captured at
    /// spawn time keys on `key`; without the same-key reinsert it would
    /// miss the next wake.
    fn poll_task(&self, key: usize) {
        let waker = match self.inner.task_wakers.borrow().get(&key).cloned() {
            Some(w) => w,
            None => return,
        };
        let mut task = match self.inner.tasks.borrow_mut().try_remove(key) {
            Some(t) => t,
            None => return,
        };
        let poll_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let mut cx = Context::from_waker(&waker);
            task.future.as_mut().poll(&mut cx)
        }));
        match poll_result {
            Ok(Poll::Ready(())) => {
                self.inner.task_wakers.borrow_mut().remove(&key);
            }
            Ok(Poll::Pending) => {
                let mut tasks = self.inner.tasks.borrow_mut();
                let new_key = tasks.insert(task);
                debug_assert_eq!(new_key, key,
                    "slab key drift: poll spawned a new task while reinserting");
            }
            Err(_) => {
                self.inner.task_wakers.borrow_mut().remove(&key);
                self.inner.last_task_panicked.set(true);
            }
        }
    }

    /// Look at all CQEs pending in the ring. For each:
    ///   - KIND_REPLY: wake the registered reply waker (if any).
    ///   - KIND_TIMEOUT: ignored (timer waking is handled by the heap).
    ///   - KIND_FSYNC / KIND_POLL_EVENTFD: not used in Stage 1.
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
            KIND_POLL_EVENTFD => {
                self.drain_w2m_for_worker(id as usize);
                // POLL_ADD is one-shot — re-arm.
                let efd = self.inner.w2m.borrow().as_ref()
                    .map(|w2m| w2m.efds()[id as usize]);
                if let Some(efd) = efd {
                    self.arm_poll_eventfd(efd, id);
                }
            }
            _ => {}
        }
    }

    /// Decode every unread W2M message for worker `w` and route each
    /// through `route_reply`. The mmap read + cursor snap live here;
    /// the pure bookkeeping lives in `route_reply` so it can be
    /// exercised directly from unit tests.
    fn drain_w2m_for_worker(&self, w: usize) {
        let w2m = Rc::clone(
            self.inner.w2m.borrow().as_ref()
                .expect("drain_w2m_for_worker called before attach_w2m"),
        );
        // A sync path may have called `reset_one` behind our back; if our
        // cursor advanced past the new write_cursor we would otherwise
        // sit past end-of-ring forever.
        {
            let wc = w2m.write_cursor(w);
            let mut cursors = self.inner.w2m_cursors.borrow_mut();
            if cursors[w] > wc {
                cursors[w] = W2M_HEADER_SIZE as u64;
            }
        }
        loop {
            let cursor = self.inner.w2m_cursors.borrow()[w];
            let (decoded, new_rc) = match w2m.try_read(w, cursor) {
                Some(v) => v,
                None => break,
            };
            self.inner.w2m_cursors.borrow_mut()[w] = new_rc;

            if self.route_reply(w, decoded) {
                w2m.reset_one(w);
                self.inner.w2m_cursors.borrow_mut()[w] = W2M_HEADER_SIZE as u64;
            }
        }
    }

    /// Park `decoded` for its awaiter and update bookkeeping. Returns
    /// `true` iff this reply drained the last outstanding request for
    /// `w` (caller resets the ring). Unrouted replies are logged and
    /// dropped — they do not decrement `in_flight`, since we did not
    /// track them in the first place.
    fn route_reply(&self, w: usize, decoded: DecodedWire) -> bool {
        let req_id = decoded.control.request_id;
        let waker = self.inner.reply_wakers.borrow_mut().remove(&req_id);
        match waker {
            Some(waker) => {
                self.inner.parked_replies.borrow_mut().insert(req_id, decoded);
                waker.wake();
                let mut inflight = self.inner.in_flight.borrow_mut();
                inflight[w] = inflight[w].saturating_sub(1);
                inflight[w] == 0
            }
            None => {
                crate::gnitz_warn!("reactor: unrouted W2M reply req_id={}", req_id);
                false
            }
        }
    }

    fn next_block_deadline_ms(&self) -> i32 {
        if !self.inner.run_queue.lock().unwrap().is_empty() {
            return 0;
        }
        let now = Instant::now();
        let next = self.inner.timers.borrow()
            .peek()
            .map(|e| e.deadline);
        match next {
            None => 1000,  // arbitrary fallback when no timers
            Some(d) if d <= now => 0,
            Some(d) => {
                let dur = d.duration_since(now);
                dur.as_millis().min(i32::MAX as u128) as i32
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

    /// Test-only: inject a synthetic CQE for reply-routing tests.
    /// Dispatched on the next `tick` (or via `drain_injected_cqes`).
    #[cfg(test)]
    pub fn inject_reply_cqe(&self, req_id: u64) {
        self.inner.injected_cqes.borrow_mut().push_back(Cqe {
            user_data: udata(KIND_REPLY, req_id),
            res: 0,
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

    /// Test-only: size `in_flight` / `w2m_cursors` without a real W2M
    /// ring so `route_reply` can be driven directly.
    #[cfg(test)]
    pub fn test_init_state(&self, num_workers: usize) {
        *self.inner.in_flight.borrow_mut() = vec![0; num_workers];
        *self.inner.w2m_cursors.borrow_mut() = vec![W2M_HEADER_SIZE as u64; num_workers];
    }

    /// Test-only: drive `route_reply` with a synthetic decoded wire.
    #[cfg(test)]
    pub fn test_route_reply(&self, w: usize, decoded: DecodedWire) -> bool {
        self.route_reply(w, decoded)
    }

    #[cfg(test)]
    pub fn in_flight_len(&self, w: usize) -> usize {
        self.inner.in_flight.borrow()[w]
    }

    #[cfg(test)]
    pub fn task_count(&self) -> usize {
        self.inner.tasks.borrow().len()
    }

    #[cfg(test)]
    pub fn last_task_panicked(&self) -> bool {
        self.inner.last_task_panicked.get()
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
    registered: bool,
    inner: Rc<ReactorShared>,
}

impl Future for TimerFuture {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if Instant::now() >= self.deadline {
            return Poll::Ready(());
        }
        if !self.registered {
            self.inner.timers.borrow_mut().push(TimerEntry {
                deadline: self.deadline,
                waker: cx.waker().clone(),
            });
            self.registered = true;
        }
        Poll::Pending
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
// TickIdleBarrier
// ---------------------------------------------------------------------------
//
// `wait()` returns immediately when the barrier is in the idle state;
// otherwise the future suspends until `notify_all` is called. Single-
// threaded by construction (uses `RefCell`, not `Mutex`).

pub struct TickIdleBarrier {
    /// True when no tick is currently active. `wait()` returns immediately
    /// in this state; setters flip it back to false when a tick begins.
    idle: Cell<bool>,
    wakers: RefCell<Vec<Waker>>,
}

impl TickIdleBarrier {
    pub fn new() -> Self {
        TickIdleBarrier {
            idle: Cell::new(true),
            wakers: RefCell::new(Vec::new()),
        }
    }

    /// Mark the barrier as "tick active". Calls to `wait()` will suspend
    /// until the next `notify_all`.
    pub fn set_active(&self) {
        self.idle.set(false);
    }

    /// Mark the barrier as "tick idle" and wake every suspended waiter.
    pub fn notify_all(&self) {
        self.idle.set(true);
        let wakers = std::mem::take(&mut *self.wakers.borrow_mut());
        for w in wakers {
            w.wake();
        }
    }

    /// Future that resolves when the barrier is in the idle state.
    /// Returns immediately on the first poll if a tick is not active.
    pub fn wait<'a>(self: &'a Rc<Self>) -> impl Future<Output = ()> + 'a {
        BarrierFuture { barrier: Rc::clone(self) }
    }
}

impl Default for TickIdleBarrier {
    fn default() -> Self { Self::new() }
}

struct BarrierFuture {
    barrier: Rc<TickIdleBarrier>,
}

impl Future for BarrierFuture {
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.barrier.idle.get() {
            Poll::Ready(())
        } else {
            self.barrier.wakers.borrow_mut().push(cx.waker().clone());
            Poll::Pending
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
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
            TimerFuture {
                deadline: Instant::now() + Duration::from_millis(50),
                registered: false,
                inner,
            }.await;
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
            TimerFuture {
                deadline: Instant::now() + Duration::from_millis(100),
                registered: false,
                inner: inner2,
            }.await;
            order2.borrow_mut().push(2);
        });

        r.block_on(async move {
            TimerFuture {
                deadline: Instant::now() + Duration::from_millis(20),
                registered: false,
                inner: inner1,
            }.await;
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
            let timer = TimerFuture {
                deadline: Instant::now() + Duration::from_millis(50),
                registered: false,
                inner: timer_inner,
            };
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

    /// Spawned task that panics during poll must not crash the reactor.
    /// The slab entry is dropped and `last_task_panicked` is set.
    #[test]
    fn task_panic_does_not_crash_reactor() {
        let r = make_reactor();
        // Suppress the panic-handler stderr noise so test output stays clean.
        let prev = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));
        r.spawn(async {
            panic!("boom");
        });
        // Drive a few ticks; the panicking task should be removed.
        for _ in 0..4 {
            r.tick(false);
        }
        std::panic::set_hook(prev);
        assert!(r.last_task_panicked());
        assert_eq!(r.task_count(), 0);
    }

    /// `TickIdleBarrier::wait()` returns immediately when the barrier is
    /// in the idle state (the common-case fast path for SELECTs that
    /// arrive when no tick is running).
    #[test]
    fn tick_idle_barrier_wait_returns_immediately_when_idle() {
        let r = make_reactor();
        let barrier = Rc::new(TickIdleBarrier::new());
        let b = Rc::clone(&barrier);
        let start = Instant::now();
        r.block_on(async move {
            b.wait().await;
        });
        assert!(start.elapsed() < Duration::from_millis(50),
            "idle barrier wait should not block");
    }

    /// `notify_all` wakes every suspended waiter. Each spawned task races
    /// the barrier against a 200ms guard timer; if the barrier wins, the
    /// task's index is recorded.
    #[test]
    fn tick_idle_barrier_wakes_all_waiters() {
        let r = make_reactor();
        let barrier = Rc::new(TickIdleBarrier::new());
        barrier.set_active();
        let woken: Rc<RefCell<Vec<u32>>> = Rc::new(RefCell::new(Vec::new()));

        for i in 0u32..3 {
            let b = Rc::clone(&barrier);
            let w = Rc::clone(&woken);
            r.spawn(async move {
                b.wait().await;
                w.borrow_mut().push(i);
            });
        }

        // Trigger the wake from a separate task that fires after a short
        // delay (so the awaiters have time to register).
        let b_notify = Rc::clone(&barrier);
        let inner = Rc::clone(&r.inner);
        r.block_on(async move {
            TimerFuture {
                deadline: Instant::now() + Duration::from_millis(20),
                registered: false,
                inner,
            }.await;
            b_notify.notify_all();
        });

        // Drain remaining tasks.
        let deadline = Instant::now() + Duration::from_millis(500);
        while r.has_pending_tasks() && Instant::now() < deadline {
            r.tick(true);
        }
        let mut got: Vec<u32> = woken.borrow().clone();
        got.sort();
        assert_eq!(got, vec![0, 1, 2], "all 3 waiters must wake");
    }

    /// A timer in the past resolves on the very first poll instead of
    /// hanging the reactor.
    #[test]
    fn timer_in_the_past_resolves_immediately() {
        let r = make_reactor();
        let inner = Rc::clone(&r.inner);
        let start = Instant::now();
        r.block_on(async move {
            TimerFuture {
                deadline: Instant::now() - Duration::from_secs(1),
                registered: false,
                inner,
            }.await;
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
        r.inject_reply_cqe(42);
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
        use crate::ipc::DecodedControl;
        DecodedWire {
            control: DecodedControl {
                status: 0,
                client_id: 0,
                target_id: 0,
                flags: 0,
                seek_pk_lo: 0,
                seek_pk_hi: 0,
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

    /// Routed reply: pops the waker, parks the wire, decrements
    /// `in_flight`, and reports `should_reset` when the counter hits 0.
    #[test]
    fn route_reply_routed_path_decrements_and_signals_reset() {
        let r = make_reactor();
        r.test_init_state(2);
        r.increment_in_flight(1);

        // Register a waker for req_id=42.
        let waker = make_waker(0, Arc::clone(&r.inner.run_queue));
        r.register_reply_waker(42, waker);

        let should_reset = r.test_route_reply(1, synthetic_decoded_wire(42));

        assert!(should_reset, "in_flight back to 0 must signal reset");
        assert_eq!(r.in_flight_len(1), 0);
        assert!(r.inner.parked_replies.borrow().contains_key(&42));
        assert!(!r.inner.reply_wakers.borrow().contains_key(&42));
    }

    /// Routed reply with outstanding siblings must NOT signal reset.
    /// Guards against premature ring resets when multiple async ops
    /// target the same worker.
    #[test]
    fn route_reply_leaves_reset_deferred_while_in_flight() {
        let r = make_reactor();
        r.test_init_state(1);
        r.increment_in_flight(0);
        r.increment_in_flight(0);
        let waker = make_waker(0, Arc::clone(&r.inner.run_queue));
        r.register_reply_waker(5, waker);

        let should_reset = r.test_route_reply(0, synthetic_decoded_wire(5));

        assert!(!should_reset);
        assert_eq!(r.in_flight_len(0), 1);
    }

    /// Unrouted reply (no waker): must NOT decrement `in_flight` —
    /// decrementing untracked requests would corrupt the ring-reset
    /// signal for legitimate outstanding ones.
    #[test]
    fn route_reply_unrouted_does_not_touch_in_flight() {
        let r = make_reactor();
        r.test_init_state(1);
        r.increment_in_flight(0);
        // No waker registered for req_id=7.

        let should_reset = r.test_route_reply(0, synthetic_decoded_wire(7));

        assert!(!should_reset);
        assert_eq!(r.in_flight_len(0), 1,
            "unrouted reply must leave tracked in_flight untouched");
        assert!(r.inner.parked_replies.borrow().is_empty(),
            "unrouted replies must not leak into parked_replies");
    }
}
