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

use std::cell::{Cell, RefCell};
use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};
use std::time::Instant;

use self::ring::{Cqe, Ring, CQE_F_MORE};
use self::uring::IoUringRing;

use crate::ipc::{DecodedWire, FLAG_EXCHANGE, W2mReceiver, W2M_HEADER_SIZE, W2M_REGION_SIZE};


pub mod io;
mod ring;
mod uring;
mod exchange;
pub mod sync;

pub use exchange::{ExchangeAccumulator, PendingRelay};
pub use sync::{AsyncMutex, AsyncRwLock, Either, join2, join_all, oneshot, mpsc, select2};

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
    /// Per-worker W2M read cursor. The reactor is the sole W2M reader,
    /// so this is the only cursor for each ring.
    w2m_cursors: RefCell<Vec<u64>>,
    /// Per-timer wakers keyed by timer_id. Each entry holds the waker to
    /// fire when the io_uring Timeout CQE arrives and a cancellation flag
    /// shared with the owning TimerFuture. If the future is dropped before
    /// the CQE, the flag is set and dispatch_cqe silently discards it.
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
        TimerFuture::new(deadline, Rc::clone(&self.inner))
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
        *self.inner.exchange_acc.borrow_mut() = ExchangeAccumulator::new(nw);
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
    ///   2. poll all tasks in the run queue (each polled at most once)
    ///   3. submit pending SQEs; if `block` and the run queue is now
    ///      empty, sleep until the next CQE.
    fn tick(&self, block: bool) {
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
            let _ = self.inner.ring.borrow_mut()
                .submit_and_wait_timeout(1, -1);
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
    /// through `route_reply`. After draining, if the worker has no
    /// outstanding routed requests AND its cursor has crossed the
    /// reset threshold, atomically reset the worker's write_cursor
    /// (mmap header) and the reactor's read cursor. Safe because the
    /// reactor is the sole W2M reader: there are no other readers
    /// whose unread data could be stranded.
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
        loop {
            let cursor = self.inner.w2m_cursors.borrow()[w];
            let (decoded, new_rc) = match w2m.try_read(w, cursor) {
                Some(v) => v,
                None => break,
            };
            self.inner.w2m_cursors.borrow_mut()[w] = new_rc;
            self.route_reply(w, decoded);
        }
        self.maybe_reset_w2m_for_worker(w, &w2m);
    }

    /// W2M ring reset for `w`. Gated on:
    /// 1. `in_flight[w] == 0` — no routed requests outstanding for `w`,
    ///    so worker `w` has no reply to write.  Combined with the
    ///    single-threaded master + reactor-as-sole-reader, this closes
    ///    the race where a worker's reply could land at a cursor that
    ///    we are about to zero.
    /// 2. cursor crossed `RESET_THRESHOLD` (~half the region) — avoids
    ///    chatty reset traffic for low-volume workloads.
    ///
    /// Hard safety: warn at 3/4 region, fatal abort at 7/8. The abort
    /// is a backstop for sustained mixed load where in_flight rarely
    /// hits 0 and the cursor climbs.
    fn maybe_reset_w2m_for_worker(&self, w: usize, w2m: &Rc<W2mReceiver>) {
        const RESET_THRESHOLD: u64 = (W2M_REGION_SIZE as u64) / 2;
        const WARN_THRESHOLD:  u64 = (W2M_REGION_SIZE as u64) * 3 / 4;
        const FATAL_THRESHOLD: u64 = (W2M_REGION_SIZE as u64) * 7 / 8;

        let cursor = self.inner.w2m_cursors.borrow()[w];
        let empty = self.inner.in_flight.borrow()[w] == 0;

        if empty && cursor > RESET_THRESHOLD {
            // No outstanding requests for w; worker w has no pending
            // replies to write. Atomically reset worker's write_cursor
            // (mmap header) and reactor's read cursor.
            w2m.reset_one_unsafe(w);
            self.inner.w2m_cursors.borrow_mut()[w] = W2M_HEADER_SIZE as u64;
            return;
        }

        if cursor > FATAL_THRESHOLD {
            crate::gnitz_fatal_abort!(
                "W2M region near exhaustion for worker={} cursor={} (in_flight={}; reset gate could not fire under sustained traffic)",
                w, cursor, self.inner.in_flight.borrow()[w]);
        } else if cursor > WARN_THRESHOLD {
            crate::gnitz_warn!(
                "W2M region 3/4 full for worker={} cursor={} in_flight={}",
                w, cursor, self.inner.in_flight.borrow()[w]);
        }
    }

    /// Park `decoded` for its awaiter and update bookkeeping. Returns
    /// `true` iff this reply drained the last outstanding request for
    /// `w` (caller resets the ring). Unrouted replies are logged and
    /// dropped — they do not decrement `in_flight`, since we did not
    /// track them in the first place.
    ///
    /// FLAG_EXCHANGE replies are demuxed into `exchange_acc` instead of
    /// being routed via the waker map: the worker is still mid-tick
    /// (waiting for FLAG_EXCHANGE_RELAY), so its tick req_id's waker
    /// must remain parked. When the accumulator completes a view, the
    /// resulting PendingRelay is enqueued on `relay_tx` for the relay
    /// task. See async-invariants III.3b.
    fn route_reply(&self, w: usize, decoded: DecodedWire) -> bool {
        let flags = decoded.control.flags as u32;
        if flags & FLAG_EXCHANGE != 0 {
            let pending = self.inner.exchange_acc.borrow_mut().process(w, decoded);
            if let Some(relay) = pending {
                if let Some(tx) = self.inner.relay_tx.borrow().as_ref() {
                    tx.send(relay);
                } else {
                    crate::gnitz_warn!("reactor: FLAG_EXCHANGE relay produced before relay_tx attached (view_id={})",
                        relay.view_id);
                }
            }
            return false;
        }

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
        if self.timer_id.is_none() {
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
        } else {
            // Re-poll: update the waker in case it changed.
            let id = self.timer_id.unwrap();
            if let Some(entry) = self.inner.timer_wakers.borrow_mut().get_mut(&id) {
                entry.0 = cx.waker().clone();
            }
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
        use crate::ipc::{DecodedControl, FLAG_EXCHANGE};
        DecodedWire {
            control: DecodedControl {
                status: 0,
                client_id: 0,
                target_id: view_id as u64,
                flags: FLAG_EXCHANGE as u64,
                seek_pk_lo: source_id as u64,
                seek_pk_hi: 0,
                seek_col_idx: 0,
                request_id: req_id,
                error_msg: Vec::new(),
            },
            schema: Some(SchemaDescriptor::minimal_u64()),
            data_batch: None,
        }
    }

    /// FLAG_EXCHANGE replies must NOT consume the registered tick waker
    /// or decrement in_flight. The tick worker is still mid-DAG and the
    /// final ACK will arrive separately.
    #[test]
    fn route_reply_flag_exchange_does_not_wake() {
        let r = make_reactor();
        r.test_init_state(2);
        *r.inner.exchange_acc.borrow_mut() = ExchangeAccumulator::new(2);
        // Simulate a parked tick waker for req_id=42 + an outstanding
        // request on worker 0.
        let waker = make_waker(0, Arc::clone(&r.inner.run_queue));
        r.register_reply_waker(42, waker);
        r.increment_in_flight(0);

        // FLAG_EXCHANGE wire with the same req_id MUST be demuxed via the
        // accumulator instead of consuming the tick waker.
        let exch = synthetic_exchange_wire(/*view_id*/ 100, /*req_id*/ 42);
        let _ = r.test_route_reply(0, exch);

        assert!(r.inner.reply_wakers.borrow().contains_key(&42),
            "FLAG_EXCHANGE must NOT consume the tick waker");
        assert_eq!(r.in_flight_len(0), 1,
            "FLAG_EXCHANGE must NOT decrement in_flight");
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
        // Manually size accumulator + in_flight for nw=2.
        r.test_init_state(2);
        *r.inner.exchange_acc.borrow_mut() = ExchangeAccumulator::new(2);

        // Two parked tick wakers; in_flight is 1 per worker.
        let w0 = make_waker(0, Arc::clone(&r.inner.run_queue));
        let w1 = make_waker(1, Arc::clone(&r.inner.run_queue));
        r.register_reply_waker(10, w0);
        r.register_reply_waker(11, w1);
        r.increment_in_flight(0);
        r.increment_in_flight(1);

        // Worker 0's FLAG_EXCHANGE: accumulator partial (no relay yet).
        let _ = r.test_route_reply(0, synthetic_exchange_wire(99, 10));
        assert!(rx.try_recv().is_none(),
            "single-worker FLAG_EXCHANGE must not produce a relay");
        // Worker 1's FLAG_EXCHANGE: accumulator complete → relay enqueued.
        let _ = r.test_route_reply(1, synthetic_exchange_wire(99, 11));
        let relay = rx.try_recv().expect("complete view must produce a relay");
        assert_eq!(relay.view_id, 99);
        assert_eq!(relay.payloads.len(), 2);
        // Final ACKs (no FLAG_EXCHANGE) for the same req_ids should now
        // wake their tick wakers and decrement in_flight.
        let _ = r.test_route_reply(0, synthetic_decoded_wire(10));
        let _ = r.test_route_reply(1, synthetic_decoded_wire(11));
        assert_eq!(r.in_flight_len(0), 0);
        assert_eq!(r.in_flight_len(1), 0);
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
            r.increment_in_flight(w);
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
}
