//! The reactor's loop: `spawn` / `block_on` task scheduling, the `tick`
//! drain-then-poll body, the CQE dispatch table it drives, and the run queue
//! and waker vtable the wakes land in.

use super::*;

impl Reactor {
    /// Spawn a task that runs detached. Returns the task key (useful for
    /// tests that want to assert task lifecycle).
    pub fn spawn(&self, fut: impl Future<Output = ()> + 'static) -> usize {
        let key = self.inner.next_task_key.get();
        self.inner.next_task_key.set(key + 1);
        self.inner.tasks.borrow_mut().insert(key, Box::pin(fut));
        // Schedule immediate first poll.
        self.inner.run_queue.borrow_mut().push(key);
        key
    }

    /// Drive `fut` to completion. Single-threaded, blocking. Spawns the
    /// future as a task internally and returns its output via a shared cell.
    #[cfg(test)]
    pub(super) fn block_on<F, T>(&self, fut: F) -> T
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
        let v = out
            .borrow_mut()
            .take()
            .expect("block_on root task did not produce output");
        v
    }

    /// Single iteration of the event loop:
    ///   1. drain CQEs and every W2M ring (waking reply / timeout / fsync wakers)
    ///   2. poll all tasks in the run queue (each polled at most once)
    ///   3. submit pending SQEs; if `block` and the run queue is now empty, arm
    ///      the W2M park and sleep until the next CQE.
    pub(super) fn tick(&self, block: bool) {
        // 1. CQEs (no syscall — reads the memory-mapped CQ), then every W2M
        // ring. The rings are drained HERE, ahead of the poll below, so a reply
        // published since the last tick is served in this tick rather than
        // waiting a full tick body for the next one.
        self.drain_cqes_into_wakers();
        self.drain_all_w2m();
        self.reap_closing_conns();

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

        // 3. Arm the W2M park only on a tick that will sleep: a set park gate
        // costs the worker one cross-process `FUTEX_WAKE` per published frame.
        let would_block = block && !self.inner.tasks.borrow().is_empty() && self.inner.run_queue.borrow().is_empty();
        if would_block && !self.inner.futex_waitv_armed.get() {
            self.arm_futex_waitv();
        }
        // The arm drains every ring first, so it can itself have woken a task.
        // Re-check; the park then stays armed while the master runs on, which
        // the next tick that does sleep reuses.
        let should_block = would_block && self.inner.run_queue.borrow().is_empty();
        if should_block {
            // Block indefinitely — outstanding timer SQEs guarantee a CQE
            // will arrive when the soonest timer fires.
            if let Err(e) = self.inner.ring().submit_and_wait_timeout(1, -1) {
                gnitz_error!("reactor: tick blocking submit failed (errno={})", e);
            }
        } else if let Err(e) = self.inner.ring().submit_and_wait_timeout(0, 0) {
            gnitz_error!("reactor: tick non-blocking submit failed (errno={})", e);
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
    /// `dispatch_cqe` (timer / fsync / futex / accept / recv / send).
    pub(super) fn drain_cqes_into_wakers(&self) {
        let mut buf = [Cqe::default(); 64];
        loop {
            let n = self.inner.ring().drain_cqes(&mut buf);
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
            KIND_TIMEOUT => {
                // The Timespec rides the park slot's carry, so completing the
                // op is also what returns it to the pool. A cancelled timer's
                // slot is already abandoned, so the CQE is a no-op wake that
                // still retires the entry — and the Timespec with it.
                if let Some(spec) = self.inner.timers.take_carry(id) {
                    self.inner.spec_pool.borrow_mut().push(spec);
                }
                self.inner.timers.complete(id, ());
            }
            KIND_FUTEX_WAITV => {
                // The wake index is not authoritative for FUTEX_WAITV — the
                // kernel may wake us for any watched word — so every ring is
                // drained. The flag drops first, and on the shutdown path too:
                // from here until `tick` re-arms the master is running and a
                // publish should not spend a syscall waking it, and a flag left
                // set at shutdown outlives the park it describes for good.
                self.inner.futex_waitv_armed.set(false);
                if let Some(w2m) = self.inner.w2m.get() {
                    w2m.clear_waitv();
                }
                if !self.inner.shutdown.get() {
                    self.drain_all_w2m();
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
/// order. `queued` makes the dedup O(1): one drain completes a `ReplyFuture`
/// per worker, all waking the same tick task, and that must cost one poll.
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
