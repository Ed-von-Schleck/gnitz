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

use gnitz_transport::ring::{Cqe, Ring, CQE_F_MORE};
use gnitz_transport::uring::IoUringRing;

use crate::ipc::{DecodedWire, W2mReceiver, W2M_HEADER_SIZE};

pub mod io;

// ---------------------------------------------------------------------------
// CQE user_data encoding (high 8 bits = kind, low 56 bits = id)
// ---------------------------------------------------------------------------

pub const KIND_REPLY:        u64 = 1;
pub const KIND_TIMEOUT:      u64 = 2;
pub const KIND_FSYNC:        u64 = 3;
pub const KIND_POLL_EVENTFD: u64 = 4;
pub const KIND_ACCEPT:       u64 = 5;
pub const KIND_RECV:         u64 = 6;
pub const KIND_SEND:         u64 = 7;

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
    /// Set when a polled future panics (and was caught + dropped).
    /// Test-only observability — production never reads it.
    #[cfg(test)]
    last_task_panicked: Cell<bool>,
    /// Shutdown flag. `block_until_shutdown` polls until this is set.
    shutdown: Cell<bool>,
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
            tasks: RefCell::new(HashMap::new()),
            next_task_key: Cell::new(0),
            run_queue: Arc::new(Mutex::new(VecDeque::new())),
            task_wakers: RefCell::new(HashMap::new()),
            reply_wakers: RefCell::new(HashMap::new()),
            parked_replies: RefCell::new(HashMap::new()),
            fsync_wakers: RefCell::new(HashMap::new()),
            parked_fsync_results: RefCell::new(HashMap::new()),
            w2m: RefCell::new(None),
            in_flight: RefCell::new(Vec::new()),
            w2m_cursors: RefCell::new(Vec::new()),
            timers: RefCell::new(BinaryHeap::new()),
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
            #[cfg(test)]
            last_task_panicked: Cell::new(false),
            shutdown: Cell::new(false),
        });
        Ok(Reactor { inner })
    }

    /// Reset the reactor's W2M reader cursors. Call alongside
    /// `SalWriter::reset_w2m_cursors` so the reactor's auto-drain reads
    /// from the start of the ring on the next CQE.
    pub fn reset_w2m_cursors(&self) {
        let mut cursors = self.inner.w2m_cursors.borrow_mut();
        for c in cursors.iter_mut() {
            *c = W2M_HEADER_SIZE as u64;
        }
        // Also clear the in_flight counter: sync paths that bypass
        // `increment_in_flight` can leave it nonzero after a ring reset,
        // which would otherwise prevent future `route_reply` from
        // signalling a reset.
        let mut inflight = self.inner.in_flight.borrow_mut();
        for i in inflight.iter_mut() { *i = 0; }
    }

    /// Request reactor shutdown — the next `block_until_shutdown` tick
    /// exits cleanly.
    pub fn request_shutdown(&self) {
        self.inner.shutdown.set(true);
        // Wake an idle tick so the check fires on the next loop iteration.
        if let Some(w) = self.inner.task_wakers.borrow().values().next().cloned() {
            w.wake();
        }
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
            let _ = ring.submit_and_wait_timeout(0, 0);
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
            let _ = ring.submit_and_wait_timeout(0, 0);
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
            debug_assert!(stale.is_empty(),
                "register_conn: stale pending_recv queue ({} entries) for fd={}; \
                 reap_closing_conns did not free the previous incarnation's buffers",
                stale.len(), fd);
            for (ptr, _) in stale {
                if !ptr.is_null() {
                    unsafe { libc::free(ptr as *mut libc::c_void); }
                }
            }
        }
        let mut conns = self.inner.conns.borrow_mut();
        conns.insert(fd, Box::new(io::Conn::new()));
        let conn = conns.get_mut(&fd).unwrap();
        let hdr_ptr = conn.recv_state.hdr_buf_ptr();
        {
            let mut ring = self.inner.ring.borrow_mut();
            ring.prep_recv(fd, hdr_ptr, 4, udata(KIND_RECV, fd as u32 as u64));
            let _ = ring.submit_and_wait_timeout(0, 0);
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
                let _ = ring.submit_and_wait_timeout(0, 0);
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
        self.reap_closing_conns();

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
        let poll_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let mut cx = Context::from_waker(&waker);
            task.future.as_mut().poll(&mut cx)
        }));
        match poll_result {
            Ok(Poll::Ready(())) => {
                self.inner.task_wakers.borrow_mut().remove(&key);
            }
            Ok(Poll::Pending) => {
                self.inner.tasks.borrow_mut().insert(key, task);
            }
            Err(_) => {
                self.inner.task_wakers.borrow_mut().remove(&key);
                #[cfg(test)]
                self.inner.last_task_panicked.set(true);
            }
        }
    }

    /// Look at all CQEs pending in the ring. For each:
    ///   - KIND_REPLY: wake the registered reply waker (if any).
    ///   - KIND_TIMEOUT: ignored (timer waking is handled by the heap).
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
            KIND_POLL_EVENTFD => {
                self.drain_w2m_for_worker(id as usize);
                // POLL_ADD is one-shot — re-arm.
                let efd = self.inner.w2m.borrow().as_ref()
                    .map(|w2m| w2m.efds()[id as usize]);
                if let Some(efd) = efd {
                    self.arm_poll_eventfd(efd, id);
                }
            }
            KIND_FSYNC => {
                self.inner.parked_fsync_results.borrow_mut().insert(id, cqe.res);
                if let Some(w) = self.inner.fsync_wakers.borrow_mut().remove(&id) {
                    w.wake();
                }
            }
            KIND_ACCEPT => {
                if cqe.res >= 0 {
                    self.inner.accept_queue.borrow_mut().push_back(cqe.res as i32);
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
                    self.inner.recv_closed.borrow_mut().insert(fd, true);
                    if let Some(w) = self.inner.recv_waiters.borrow_mut().remove(&fd) {
                        w.wake();
                    }
                    return;
                }
                let pbuf = unsafe { libc::malloc(plen) as *mut u8 };
                if pbuf.is_null() {
                    conn.closing = true;
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
                self.inner.recv_closed.borrow_mut().insert(fd, true);
                if let Some(w) = self.inner.recv_waiters.borrow_mut().remove(&fd) {
                    w.wake();
                }
            }
        }
    }

    /// Reap connections that are closing and have no outstanding SQEs.
    /// Called once per tick.
    fn reap_closing_conns(&self) {
        let mut to_remove: Vec<i32> = Vec::new();
        {
            let conns = self.inner.conns.borrow();
            for (&fd, conn) in conns.iter() {
                if conn.closing && !conn.has_outstanding() {
                    to_remove.push(fd);
                }
            }
        }
        for fd in to_remove {
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
    /// through `route_reply`. The mmap read + cursor snap live here;
    /// the pure bookkeeping lives in `route_reply` so it can be
    /// exercised directly from unit tests.
    fn drain_w2m_for_worker(&self, w: usize) {
        let w2m = Rc::clone(
            self.inner.w2m.borrow().as_ref()
                .expect("drain_w2m_for_worker called before attach_w2m"),
        );
        // Consume the eventfd signal: the one-shot PollAdd re-arms on
        // the next tick, and without draining the counter Linux would
        // deliver a CQE immediately (tight loop).
        {
            let efd = w2m.efds()[w];
            let mut v: u64 = 0;
            unsafe {
                libc::read(efd, &mut v as *mut u64 as *mut libc::c_void, 8);
            }
        }
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
            // Ring reset is tick_loop's job — see async-invariants §III.1.
            self.route_reply(w, decoded);
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

    /// True when no `await_reply`-style request is outstanding on any
    /// worker. The tick task gates its W2M reset on this to avoid
    /// resetting `write_cursor` while a scan / validate / push ACK is
    /// mid-flight — a reset between the worker's atomic_store (of the
    /// write_cursor) and the reactor's drain would strand the data past
    /// the new write_cursor, masking it from subsequent `try_read`s.
    pub fn all_in_flight_zero(&self) -> bool {
        self.inner.in_flight.borrow().iter().all(|&n| n == 0)
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

// ---------------------------------------------------------------------------
// oneshot
// ---------------------------------------------------------------------------
//
// Single-threaded, cancellable. Used by the committer to send per-commit
// results back to the handler that produced the push.

pub mod oneshot {
    use super::*;

    #[derive(Debug, PartialEq, Eq)]
    pub struct Cancelled;

    struct State<T> {
        value: Option<T>,
        waker: Option<Waker>,
        sender_alive: bool,
        receiver_alive: bool,
    }

    pub struct Sender<T> { inner: Rc<RefCell<State<T>>> }
    pub struct Receiver<T> { inner: Rc<RefCell<State<T>>> }

    pub fn channel<T>() -> (Sender<T>, Receiver<T>) {
        let s = Rc::new(RefCell::new(State {
            value: None, waker: None, sender_alive: true, receiver_alive: true,
        }));
        (Sender { inner: Rc::clone(&s) }, Receiver { inner: s })
    }

    impl<T> Sender<T> {
        /// Attempt to send. Returns `Err(v)` if the receiver has been
        /// dropped (cancelled).
        pub fn send(self, v: T) -> Result<(), T> {
            let mut s = self.inner.borrow_mut();
            if !s.receiver_alive {
                return Err(v);
            }
            s.value = Some(v);
            if let Some(w) = s.waker.take() { w.wake(); }
            Ok(())
        }
    }

    impl<T> Drop for Sender<T> {
        fn drop(&mut self) {
            let mut s = self.inner.borrow_mut();
            s.sender_alive = false;
            if let Some(w) = s.waker.take() { w.wake(); }
        }
    }

    impl<T> Future for Receiver<T> {
        type Output = Result<T, Cancelled>;
        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            let mut s = self.inner.borrow_mut();
            if let Some(v) = s.value.take() {
                return Poll::Ready(Ok(v));
            }
            if !s.sender_alive {
                return Poll::Ready(Err(Cancelled));
            }
            s.waker = Some(cx.waker().clone());
            Poll::Pending
        }
    }

    impl<T> Drop for Receiver<T> {
        fn drop(&mut self) {
            self.inner.borrow_mut().receiver_alive = false;
        }
    }
}

// ---------------------------------------------------------------------------
// mpsc (unbounded)
// ---------------------------------------------------------------------------
//
// Used as the committer's request channel. Senders are cloneable; Drop
// of the last sender returns `None` from the receiver.

pub mod mpsc {
    use super::*;

    struct State<T> {
        queue: VecDeque<T>,
        waker: Option<Waker>,
        senders: usize,
    }

    pub struct Sender<T> { inner: Rc<RefCell<State<T>>> }
    pub struct Receiver<T> { inner: Rc<RefCell<State<T>>> }

    pub fn unbounded<T>() -> (Sender<T>, Receiver<T>) {
        let s = Rc::new(RefCell::new(State {
            queue: VecDeque::new(), waker: None, senders: 1,
        }));
        (Sender { inner: Rc::clone(&s) }, Receiver { inner: s })
    }

    impl<T> Clone for Sender<T> {
        fn clone(&self) -> Self {
            self.inner.borrow_mut().senders += 1;
            Sender { inner: Rc::clone(&self.inner) }
        }
    }

    impl<T> Sender<T> {
        pub fn send(&self, v: T) {
            let mut s = self.inner.borrow_mut();
            s.queue.push_back(v);
            if let Some(w) = s.waker.take() { w.wake(); }
        }
    }

    impl<T> Drop for Sender<T> {
        fn drop(&mut self) {
            let mut s = self.inner.borrow_mut();
            s.senders -= 1;
            if s.senders == 0 {
                if let Some(w) = s.waker.take() { w.wake(); }
            }
        }
    }

    impl<T> Receiver<T> {
        pub fn recv(&mut self) -> RecvOne<'_, T> {
            RecvOne { inner: &self.inner }
        }

        /// Non-blocking receive: returns `Some(T)` if the queue has an
        /// item, `None` otherwise. Never awaits. Used by the committer
        /// to drain pipelined requests without paying the 1ms debounce
        /// timer when nothing more is available.
        pub fn try_recv(&mut self) -> Option<T> {
            self.inner.borrow_mut().queue.pop_front()
        }
    }

    pub struct RecvOne<'a, T> {
        inner: &'a Rc<RefCell<State<T>>>,
    }

    impl<'a, T> Future for RecvOne<'a, T> {
        type Output = Option<T>;
        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<T>> {
            let mut s = self.inner.borrow_mut();
            if let Some(v) = s.queue.pop_front() {
                return Poll::Ready(Some(v));
            }
            if s.senders == 0 {
                return Poll::Ready(None);
            }
            s.waker = Some(cx.waker().clone());
            Poll::Pending
        }
    }
}

// ---------------------------------------------------------------------------
// AsyncMutex
// ---------------------------------------------------------------------------

pub struct AsyncMutex<T> {
    locked: Cell<bool>,
    value: RefCell<T>,
    waiters: RefCell<VecDeque<Waker>>,
}

impl<T> AsyncMutex<T> {
    pub fn new(v: T) -> Self {
        AsyncMutex {
            locked: Cell::new(false),
            value: RefCell::new(v),
            waiters: RefCell::new(VecDeque::new()),
        }
    }

    pub fn lock<'a>(self: &'a Rc<Self>) -> LockFuture<'a, T> {
        LockFuture { mutex: Rc::clone(self), _p: std::marker::PhantomData }
    }

    fn release(&self) {
        self.locked.set(false);
        if let Some(w) = self.waiters.borrow_mut().pop_front() { w.wake(); }
    }
}

pub struct LockFuture<'a, T> {
    mutex: Rc<AsyncMutex<T>>,
    _p: std::marker::PhantomData<&'a ()>,
}

impl<'a, T: 'a> Future for LockFuture<'a, T> {
    type Output = LockGuard<T>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<LockGuard<T>> {
        if !self.mutex.locked.get() {
            self.mutex.locked.set(true);
            return Poll::Ready(LockGuard { mutex: Rc::clone(&self.mutex) });
        }
        self.mutex.waiters.borrow_mut().push_back(cx.waker().clone());
        Poll::Pending
    }
}

pub struct LockGuard<T> {
    mutex: Rc<AsyncMutex<T>>,
}

impl<T> std::ops::Deref for LockGuard<T> {
    type Target = RefCell<T>;
    fn deref(&self) -> &Self::Target { &self.mutex.value }
}

impl<T> Drop for LockGuard<T> {
    fn drop(&mut self) { self.mutex.release(); }
}

// ---------------------------------------------------------------------------
// AsyncRwLock (writer-preference)
// ---------------------------------------------------------------------------
//
// `()`-valued: used as a catalog-wide barrier. Readers hold a read guard
// during INSERT; the DDL path holds a write guard during catalog
// mutation. Writer-preference blocks new readers as soon as a writer
// parks, guaranteeing DDL doesn't starve.

struct RwLockInner {
    readers: usize,
    has_writer: bool,
    writers_waiting: usize,
    read_waiters: VecDeque<Waker>,
    write_waiters: VecDeque<Waker>,
}

pub struct AsyncRwLock {
    inner: RefCell<RwLockInner>,
}

impl AsyncRwLock {
    pub fn new() -> Self {
        AsyncRwLock {
            inner: RefCell::new(RwLockInner {
                readers: 0, has_writer: false, writers_waiting: 0,
                read_waiters: VecDeque::new(),
                write_waiters: VecDeque::new(),
            }),
        }
    }

    pub fn read<'a>(self: &'a Rc<Self>) -> ReadFuture<'a> {
        ReadFuture { lock: Rc::clone(self), _p: std::marker::PhantomData }
    }

    pub fn write<'a>(self: &'a Rc<Self>) -> WriteFuture<'a> {
        WriteFuture { lock: Rc::clone(self), parked: false, _p: std::marker::PhantomData }
    }

    fn release_read(&self) {
        let mut s = self.inner.borrow_mut();
        s.readers -= 1;
        if s.readers == 0 && s.writers_waiting > 0 {
            if let Some(w) = s.write_waiters.pop_front() {
                w.wake();
            }
        }
    }

    fn release_write(&self) {
        let mut s = self.inner.borrow_mut();
        s.has_writer = false;
        // Writer-preference: wake the next writer before any readers.
        if let Some(w) = s.write_waiters.pop_front() {
            w.wake();
            return;
        }
        // No queued writer — wake all parked readers.
        let readers = std::mem::take(&mut s.read_waiters);
        drop(s);
        for w in readers { w.wake(); }
    }
}

impl Default for AsyncRwLock {
    fn default() -> Self { Self::new() }
}

pub struct ReadFuture<'a> {
    lock: Rc<AsyncRwLock>,
    _p: std::marker::PhantomData<&'a ()>,
}

impl<'a> Future for ReadFuture<'a> {
    type Output = ReadGuard;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<ReadGuard> {
        let mut s = self.lock.inner.borrow_mut();
        if !s.has_writer && s.writers_waiting == 0 {
            s.readers += 1;
            return Poll::Ready(ReadGuard { lock: Rc::clone(&self.lock) });
        }
        s.read_waiters.push_back(cx.waker().clone());
        Poll::Pending
    }
}

pub struct ReadGuard {
    lock: Rc<AsyncRwLock>,
}

impl Drop for ReadGuard {
    fn drop(&mut self) { self.lock.release_read(); }
}

pub struct WriteFuture<'a> {
    lock: Rc<AsyncRwLock>,
    parked: bool,
    _p: std::marker::PhantomData<&'a ()>,
}

impl<'a> Future for WriteFuture<'a> {
    type Output = WriteGuard;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<WriteGuard> {
        let was_parked = self.parked;
        let lock = Rc::clone(&self.lock);
        let mut s = lock.inner.borrow_mut();
        if !s.has_writer && s.readers == 0 {
            s.has_writer = true;
            if was_parked {
                s.writers_waiting -= 1;
                drop(s);
                self.parked = false;
            }
            return Poll::Ready(WriteGuard { lock: Rc::clone(&self.lock) });
        }
        if !was_parked {
            s.writers_waiting += 1;
            drop(s);
            self.parked = true;
            lock.inner.borrow_mut().write_waiters.push_back(cx.waker().clone());
        } else {
            s.write_waiters.push_back(cx.waker().clone());
        }
        Poll::Pending
    }
}

impl<'a> Drop for WriteFuture<'a> {
    fn drop(&mut self) {
        if self.parked {
            let mut s = self.lock.inner.borrow_mut();
            s.writers_waiting -= 1;
            // Cancelled: if no writer holds the lock and no other writer
            // is waiting, wake any parked readers.
            if !s.has_writer && s.writers_waiting == 0 {
                let readers = std::mem::take(&mut s.read_waiters);
                drop(s);
                for w in readers { w.wake(); }
            }
        }
    }
}

pub struct WriteGuard {
    lock: Rc<AsyncRwLock>,
}

impl Drop for WriteGuard {
    fn drop(&mut self) { self.lock.release_write(); }
}

// ---------------------------------------------------------------------------
// join2 / join_all
// ---------------------------------------------------------------------------

pub async fn join2<A, B>(a: A, b: B) -> (A::Output, B::Output)
where
    A: Future,
    B: Future,
{
    let mut a = Box::pin(a);
    let mut b = Box::pin(b);
    let mut a_out: Option<A::Output> = None;
    let mut b_out: Option<B::Output> = None;
    std::future::poll_fn(move |cx| {
        if a_out.is_none() {
            if let Poll::Ready(v) = a.as_mut().poll(cx) { a_out = Some(v); }
        }
        if b_out.is_none() {
            if let Poll::Ready(v) = b.as_mut().poll(cx) { b_out = Some(v); }
        }
        if a_out.is_some() && b_out.is_some() {
            Poll::Ready((a_out.take().unwrap(), b_out.take().unwrap()))
        } else {
            Poll::Pending
        }
    }).await
}

/// Drive every future in `futs` to completion, return values in input order.
pub async fn join_all<F, T, I>(futs: I) -> Vec<T>
where
    I: IntoIterator<Item = F>,
    F: Future<Output = T>,
{
    let mut futs: Vec<Pin<Box<F>>> = futs.into_iter().map(Box::pin).collect();
    let mut out: Vec<Option<T>> = (0..futs.len()).map(|_| None).collect();
    std::future::poll_fn(move |cx| {
        let mut all_done = true;
        for i in 0..futs.len() {
            if out[i].is_some() { continue; }
            match futs[i].as_mut().poll(cx) {
                Poll::Ready(v) => out[i] = Some(v),
                Poll::Pending => all_done = false,
            }
        }
        if all_done {
            let result: Vec<T> = out.iter_mut().map(|o| o.take().unwrap()).collect();
            Poll::Ready(result)
        } else {
            Poll::Pending
        }
    }).await
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
        let fd = crate::ipc_sys::memfd_create(b"reactor_fsync_future");
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
        let fd = crate::ipc_sys::memfd_create(b"reactor_fsync_ok");
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
        let fd = crate::ipc_sys::memfd_create(b"reactor_fsync_flush");
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
    // `all_in_flight_zero` — gate used by `tick_loop` before W2M reset.
    //
    // Regression guard: the tick task must not reset W2M while the
    // reactor still has a scan/push response routing for any worker.
    // The reset zeroes `write_cursor`; data written past that offset
    // becomes invisible to `try_read`, which returns `None` when
    // `write_cursor <= read_cursor`. Symptom: a concurrent scan's
    // response got stranded at offset ~128 and the client hung.
    // ─────────────────────────────────────────────────────────────────

    #[test]
    fn all_in_flight_zero_true_on_fresh_reactor() {
        let r = make_reactor();
        r.test_init_state(4);
        assert!(r.all_in_flight_zero(),
            "fresh reactor with zeroed counters must report all-zero");
    }

    #[test]
    fn all_in_flight_zero_false_when_any_worker_pending() {
        let r = make_reactor();
        r.test_init_state(4);
        r.increment_in_flight(2);
        assert!(!r.all_in_flight_zero(),
            "any nonzero in_flight slot must block the all-zero gate");
    }

    #[test]
    fn all_in_flight_zero_tracks_route_reply_decrement() {
        // Walk the full lifecycle: submit N requests, route replies one
        // by one, assert the gate flips only when the LAST reply routes.
        let r = make_reactor();
        r.test_init_state(2);

        // Two requests on worker 0, one on worker 1 — N = 3 total pending.
        let waker0 = make_waker(0, Arc::clone(&r.inner.run_queue));
        let waker1 = make_waker(1, Arc::clone(&r.inner.run_queue));
        let waker2 = make_waker(2, Arc::clone(&r.inner.run_queue));
        r.register_reply_waker(100, waker0);
        r.register_reply_waker(101, waker1);
        r.register_reply_waker(102, waker2);
        r.increment_in_flight(0);
        r.increment_in_flight(0);
        r.increment_in_flight(1);

        assert!(!r.all_in_flight_zero(), "3 pending");
        r.test_route_reply(0, synthetic_decoded_wire(100));
        assert!(!r.all_in_flight_zero(), "2 pending");
        r.test_route_reply(1, synthetic_decoded_wire(102));
        assert!(!r.all_in_flight_zero(), "1 pending on worker 0");
        r.test_route_reply(0, synthetic_decoded_wire(101));
        assert!(r.all_in_flight_zero(), "gate flips on last reply");
    }

    /// Unrouted replies (req_id=0, e.g. FLAG_TICK broadcast ACKs) MUST
    /// NOT touch the gate.  Otherwise a tick ACK draining through the
    /// reactor would flip `all_in_flight_zero` → true while a scan's
    /// routed reply is still pending, and `tick_loop` would reset W2M
    /// prematurely — the exact race that hung
    /// `test_no_rows_lost_with_view_during_checkpoints`.
    #[test]
    fn all_in_flight_zero_ignores_unrouted_tick_acks() {
        let r = make_reactor();
        r.test_init_state(2);
        let waker = make_waker(0, Arc::clone(&r.inner.run_queue));
        r.register_reply_waker(145, waker);
        r.increment_in_flight(0);

        // Worker 1 sends 4 unrouted FLAG_TICK ACKs (req_id=0, no waker).
        for _ in 0..4 {
            r.test_route_reply(1, synthetic_decoded_wire(0));
        }
        assert!(!r.all_in_flight_zero(),
            "unrouted tick ACKs must not flip the gate — worker 0's \
             routed reply is still pending");

        // Now route the real reply; gate flips.
        r.test_route_reply(0, synthetic_decoded_wire(145));
        assert!(r.all_in_flight_zero());
    }

    /// `reactor.reset_w2m_cursors` zeros the in_flight counters.  The
    /// tick task only calls this after `all_in_flight_zero` returns
    /// true, but the reset itself must leave the gate in the true
    /// state (invariant for the next cycle).
    #[test]
    fn reset_w2m_cursors_clears_in_flight() {
        let r = make_reactor();
        r.test_init_state(4);
        r.increment_in_flight(0);
        r.increment_in_flight(2);
        assert!(!r.all_in_flight_zero());
        r.reset_w2m_cursors();
        assert!(r.all_in_flight_zero(),
            "reset_w2m_cursors must zero in_flight so the next cycle \
             starts with the gate open");
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
}
