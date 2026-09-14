//! The reactor's loop: `spawn` task scheduling, the `tick` drain-then-poll body,
//! the exclusive driver beside it, the CQE dispatch table both run, and the run
//! queue and waker vtable the wakes land in.

use super::*;

impl Reactor {
    /// Spawn a task that runs detached.
    pub fn spawn(&self, fut: impl Future<Output = ()> + 'static) {
        let key = self.inner.next_task_key.get();
        self.inner.next_task_key.set(key + 1);
        self.inner.tasks.borrow_mut().insert(key, Box::pin(fut));
        // Schedule immediate first poll.
        self.inner.run_queue.borrow_mut().push(key);
    }

    /// Single iteration of the event loop:
    ///   1. drain CQEs and every W2M ring, then fire every passed deadline
    ///   2. poll all tasks in the run queue (each polled at most once)
    ///   3. submit pending SQEs; if `block` and the run queue is now empty, arm
    ///      the W2M park and sleep until the next CQE or the earliest deadline.
    pub(super) fn tick(&self, block: bool) {
        // 1. CQEs, every W2M ring, then passed deadlines — ahead of the poll, so what
        // landed since the last tick is served in this one.
        self.drain_cqes_into_wakers();
        self.drain_all_w2m();
        self.fire_deadlines();

        // 2. Drain the run queue. Swap into the scratch buffer so wakes during
        // poll schedule for the *next* tick rather than re-entering this one.
        // `Cell::take` releases any borrow before poll_task runs so waker_wake
        // can push freely.
        let mut buf = self.inner.tick_scratch.take();
        self.inner.run_queue.borrow_mut().swap_into(&mut buf);
        for key in buf.drain(..) {
            self.poll_task(key);
        }
        self.inner.tick_scratch.set(buf);

        // 3. Sleep only when nothing is runnable. The W2M park is armed only on a
        //    tick that sleeps: a set park flag costs each worker publish a
        //    cross-process `FUTEX_WAKE`.
        let may_sleep = block
            && !self.inner.shutdown.get()
            && !self.inner.tasks.borrow().is_empty()
            && self.inner.run_queue.borrow().is_empty();
        self.submit_or_sleep(may_sleep, |_| !self.inner.run_queue.borrow().is_empty());
    }

    /// Drive `fut` to completion without polling any task: CQEs are dispatched, W2M
    /// frames routed and deadlines fired as in `tick`, so their wakes land for the
    /// tasks to run afterwards. `fut` may await only leases, the exchange queue and
    /// timers. No train lease may be live: a routed train frame no task consumes pins
    /// its ring's release, and a worker parked for ring space never answers.
    ///
    /// Callable from inside a task's poll: the reactor holds no borrow of its own
    /// state across a task poll, and a CQE handler only wakes or spawns.
    pub(crate) fn block_on_exclusive<T>(&self, fut: impl Future<Output = T>) -> T {
        debug_assert!(!self
            .inner
            .routes
            .borrow()
            .values()
            .any(|r| matches!(r, Route::Train(_))));
        let mut fut = std::pin::pin!(fut);
        let mut cx = Context::from_waker(Waker::noop());
        loop {
            self.drain_cqes_into_wakers();
            self.drain_all_w2m();
            self.fire_deadlines();
            if let Poll::Ready(v) = fut.as_mut().poll(&mut cx) {
                return v;
            }
            self.submit_or_sleep(true, |routed| routed);
        }
    }

    /// Submit queued SQEs. When `may_sleep`, wait for the next CQE, W2M publish or
    /// earliest deadline instead — unless a deadline has passed or `found(routed)`
    /// reports work turned up by the drain run before arming the W2M park.
    fn submit_or_sleep(&self, may_sleep: bool, found: impl Fn(bool) -> bool) {
        let until = self.inner.deadlines.borrow().first_key_value().map(|(&(at, _), _)| at);
        let sleep = may_sleep
            && until.is_none_or(|at| at > Instant::now())
            && (self.inner.futex_waitv_armed.get() || self.arm_futex_waitv(&found));
        let rc = if sleep {
            // Measured after the arm, whose drain takes time of its own.
            let timeout = until.map(|at| at.saturating_duration_since(Instant::now()));
            self.inner.ring.borrow_mut().wait(timeout)
        } else {
            self.inner.ring.borrow_mut().submit()
        };
        if let Err(e) = rc {
            gnitz_error!("reactor: submit failed (errno={})", e);
        }
    }

    /// Wake every deadline that has passed.
    fn fire_deadlines(&self) {
        let now = Instant::now();
        loop {
            // One entry per borrow, so no borrow spans a wake.
            let waker = {
                let mut deadlines = self.inner.deadlines.borrow_mut();
                match deadlines.first_entry() {
                    Some(e) if e.key().0 <= now => e.remove(),
                    _ => break,
                }
            };
            waker.wake();
        }
    }

    /// Poll a single task. The future is taken out of the map for the duration
    /// of the poll and reinserted at the same key on Pending: the poll may
    /// re-enter the reactor and `spawn`, which can reallocate the map, so no
    /// borrow of it may span the poll.
    fn poll_task(&self, key: usize) {
        let mut task = match self.inner.tasks.borrow_mut().remove(&key) {
            Some(t) => t,
            None => return,
        };
        let waker = make_waker(key);
        let mut cx = Context::from_waker(&waker);
        match task.as_mut().poll(&mut cx) {
            Poll::Ready(()) => {}
            Poll::Pending => {
                self.inner.tasks.borrow_mut().insert(key, task);
            }
        }
    }

    /// Drain all CQEs pending in the ring and route each through
    /// `dispatch_cqe` (op / futex / accept / recv).
    pub(super) fn drain_cqes_into_wakers(&self) {
        let mut buf = [Cqe::default(); 64];
        loop {
            let n = self.inner.ring.borrow_mut().drain_cqes(&mut buf);
            for cqe in &buf[..n] {
                self.dispatch_cqe(*cqe);
            }
            // A short read proves the ring is empty, so the usual single-batch
            // tick costs one `drain_cqes` rather than two.
            if n < buf.len() {
                break;
            }
        }
    }

    pub(super) fn dispatch_cqe(&self, cqe: Cqe) {
        let kind = udata_kind(cqe.user_data);
        let id = udata_id(cqe.user_data);
        match kind {
            KIND_FUTEX_WAITV => {
                // No drain here: the tick's own drain follows CQE dispatch.
                if cqe.res == -libc::ENOSYS || cqe.res == -libc::EINVAL {
                    gnitz_fatal_abort!(
                        "reactor: io_uring IORING_OP_FUTEX_WAITV unsupported (res={}); Linux 6.7+ required",
                        cqe.res
                    );
                }
                self.inner.futex_waitv_armed.set(false);
                self.inner.w2m.clear_waitv();
            }
            KIND_OP => self.inner.ops.complete(id, cqe.res),
            KIND_ACCEPT => self.handle_accept_cqe(id as i32, cqe.res, cqe.flags),
            KIND_RECV => self.handle_recv_cqe(id as i32, cqe.res),
            _ => gnitz_error!(
                "reactor: CQE with unknown kind={} (user_data={:#x})",
                kind,
                cqe.user_data
            ),
        }
    }
}

// ---------------------------------------------------------------------------
// Run queue + waker vtable
// ---------------------------------------------------------------------------

/// The keys of tasks whose wakers have fired since the last tick, in wake
/// order. `queued` makes the dedup O(1): one drain completes an ACK per worker,
/// all waking the same tick task, and that must cost one poll.
pub(super) struct RunQueue {
    queue: Vec<usize>,
    queued: FxHashSet<usize>,
}

impl RunQueue {
    pub(super) fn new() -> Self {
        Self {
            queue: Vec::with_capacity(16),
            queued: FxHashSet::default(),
        }
    }

    /// Enqueue `key` unless it is already pending.
    pub(super) fn push(&mut self, key: usize) {
        if self.queued.insert(key) {
            self.queue.push(key);
        }
    }

    pub(super) fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    /// O(1) swap of the backing allocation into `out`. Clearing `queued` in the
    /// same borrow is what keeps the semantics exact: a task woken during its
    /// own poll inserts into the emptied set and lands in the fresh queue.
    fn swap_into(&mut self, out: &mut Vec<usize>) {
        std::mem::swap(out, &mut self.queue);
        self.queued.clear();
    }

    #[cfg(test)]
    pub(super) fn is_queued(&self, key: usize) -> bool {
        self.queued.contains(&key)
    }

    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.queue.len()
    }
}

thread_local! {
    /// The run queue of the reactor on this thread, for the waker vtable to
    /// reach without per-wake `Arc` traffic. Set in `Reactor::new` (which
    /// refuses to overwrite a live one), cleared in `Reactor::drop`.
    pub(super) static REACTOR_RUN_QUEUE: Cell<*const RefCell<RunQueue>> =
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
            // reached when `sync.rs` Drop chains (WriteGuard / ReadGuard)
            // fire `waker.wake()` while the reactor is being dropped. Silent
            // no-op.
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

/// The waker for task `key`: the key itself *is* the waker's data pointer, so
/// clone is a bitwise copy and drop is a no-op. Waking reads the thread-local
/// run-queue pointer and pushes the key.
pub(super) fn make_waker(key: usize) -> Waker {
    let raw = RawWaker::new(key as *const (), &WAKER_VTABLE);
    unsafe { Waker::from_raw(raw) }
}

#[cfg(test)]
#[path = "tests/runloop.rs"]
mod tests;
