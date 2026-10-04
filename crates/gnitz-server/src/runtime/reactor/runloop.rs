//! The reactor's loop: `spawn` task scheduling, the `block_on` driver and the two
//! halves of a pass it runs, the CQE dispatch table, and the run queue and waker
//! vtable the wakes land in.

use std::collections::VecDeque;
use std::mem::MaybeUninit;

use io_uring::cqueue;

use super::*;

impl Reactor {
    /// Spawn a task that runs detached.
    pub fn spawn(&self, fut: impl Future<Output = ()> + 'static) {
        let key = self.next_task_key.get();
        self.next_task_key.set(key + 1);
        self.tasks.borrow_mut().insert(key, Box::pin(fut));
        // Schedule immediate first poll.
        RUN_QUEUE.with(|q| q.borrow_mut().push(key));
    }

    /// Drive `fut` to completion as a task among the others, and return its
    /// output. The one driver of the loop, called outside any task.
    pub(crate) fn block_on<F>(&self, fut: F) -> F::Output
    where
        F: Future + 'static,
        F::Output: 'static,
    {
        let out = Rc::new(Cell::new(None));
        let slot = Rc::clone(&out);
        self.spawn(async move { slot.set(Some(fut.await)) });
        loop {
            self.run_ready();
            let done = out.take();
            // The pass that completed `fut` submits and returns; any other may sleep.
            self.submit_or_sleep(done.is_none());
            if let Some(v) = done {
                return v;
            }
        }
    }

    /// Drain CQEs and every W2M ring, then fire every passed deadline — ahead of
    /// the poll, so what landed since the last pass is served in this one. Then
    /// poll each task queued on entry, once; a task woken or spawned by those
    /// polls runs next pass.
    pub(super) fn run_ready(&self) {
        self.drain_cqes_into_wakers();
        self.drain_all_w2m();
        self.fire_deadlines();

        for _ in 0..RUN_QUEUE.with(|q| q.borrow().len()) {
            let key = RUN_QUEUE.with(|q| q.borrow_mut().pop());
            self.poll_task(key);
        }
    }

    /// Submit queued SQEs. When `block` and nothing is runnable, arm the W2M park
    /// and sleep until the next CQE, W2M publish or earliest deadline instead —
    /// unless a deadline has passed or the drain run before arming woke a task.
    /// The park is armed only on a pass that sleeps: a set park flag costs each
    /// worker publish a cross-process `FUTEX_WAKE`.
    pub(super) fn submit_or_sleep(&self, block: bool) {
        let until = self.deadlines.borrow().first_key_value().map(|(&(at, _), _)| at);
        let sleep = block
            && run_queue_is_empty()
            && until.is_none_or(|at| at > Instant::now())
            && (self.futex_waitv_armed.get() || self.arm_futex_waitv());
        if sleep {
            // Measured after the arm, whose drain takes time of its own.
            let timeout = until.map(|at| at.saturating_duration_since(Instant::now()));
            self.ring.borrow_mut().wait(timeout);
        } else {
            self.ring.borrow_mut().submit();
        }
    }

    /// Wake every deadline that has passed.
    fn fire_deadlines(&self) {
        let now = Instant::now();
        let mut d = self.deadlines.borrow_mut();
        while let Some(e) = d.first_entry() {
            if e.key().0 > now {
                break;
            }
            e.remove().wake();
        }
    }

    /// Poll a single task. The future is taken out of the map for the duration
    /// of the poll and reinserted at the same key on Pending: the poll may
    /// re-enter the reactor and `spawn`, which can reallocate the map, so no
    /// borrow of it may span the poll.
    fn poll_task(&self, key: usize) {
        let mut task = match self.tasks.borrow_mut().remove(&key) {
            Some(t) => t,
            None => return,
        };
        let waker = make_waker(key);
        let mut cx = Context::from_waker(&waker);
        match task.as_mut().poll(&mut cx) {
            Poll::Ready(()) => {}
            Poll::Pending => {
                self.tasks.borrow_mut().insert(key, task);
            }
        }
    }

    /// Drain all CQEs pending in the ring and route each through `dispatch_cqe`.
    pub(super) fn drain_cqes_into_wakers(&self) {
        let mut buf = [const { MaybeUninit::<cqueue::Entry>::uninit() }; 64];
        loop {
            // `fill`'s slice borrows `buf`, not the ring, so dispatch can re-borrow it.
            let cqes = self.ring.borrow_mut().fill(&mut buf);
            for e in cqes {
                self.dispatch_cqe(e.user_data(), e.result());
            }
            // A short read proves the ring is empty.
            if cqes.len() < 64 {
                break;
            }
        }
    }

    pub(super) fn dispatch_cqe(&self, user_data: u64, res: i32) {
        let id = udata_id(user_data);
        match udata_kind(user_data) {
            KIND_FUTEX_WAITV => {
                // No drain here: the pass's own drain follows CQE dispatch.
                if res == -libc::ENOSYS || res == -libc::EINVAL {
                    gnitz_fatal_abort!(
                        "reactor: io_uring IORING_OP_FUTEX_WAITV unsupported (res={}); Linux 6.7+ required",
                        res
                    );
                }
                self.futex_waitv_armed.set(false);
                self.w2m.clear_waitv();
            }
            KIND_OP => {
                let op = self.ops.borrow_mut().remove(&id);
                let PendingOp { done, carry } = op.expect("an op's entry lives until its CQE");
                done.send((res, carry));
            }
            KIND_RECV => self.handle_recv_cqe(id as i32, res),
            kind => unreachable!("CQE kind {kind} (user_data={user_data:#x}) was never issued"),
        }
    }
}

// ---------------------------------------------------------------------------
// Run queue + waker vtable
// ---------------------------------------------------------------------------

/// The keys of tasks whose wakers have fired, in wake order. A task woken several
/// times before its poll is queued once; `queued` makes that test O(1).
pub(super) struct RunQueue {
    queue: VecDeque<usize>,
    queued: FxHashSet<usize>,
}

impl RunQueue {
    const fn new() -> Self {
        Self {
            queue: VecDeque::new(),
            queued: FxHashSet::with_hasher(rustc_hash::FxBuildHasher),
        }
    }

    /// Enqueue `key` unless it is already pending.
    fn push(&mut self, key: usize) {
        if self.queued.insert(key) {
            self.queue.push_back(key);
        }
    }

    /// The oldest queued key. A wake of it from here on queues it again.
    fn pop(&mut self) -> usize {
        let key = self.queue.pop_front().expect("pop on an empty run queue");
        self.queued.remove(&key);
        key
    }

    pub(super) fn len(&self) -> usize {
        self.queue.len()
    }

    fn clear(&mut self) {
        self.queue.clear();
        self.queued.clear();
    }
}

thread_local! {
    /// The run queue of the reactor on this thread, for the waker vtable to reach
    /// without per-wake `Rc` traffic. A wake with no reactor live lands here
    /// harmlessly; `claim_thread` clears what a dropped reactor left.
    static RUN_QUEUE: RefCell<RunQueue> = const { RefCell::new(RunQueue::new()) };
    /// A reactor is live on this thread.
    static REACTOR_LIVE: Cell<bool> = const { Cell::new(false) };
}

pub(super) fn run_queue_is_empty() -> bool {
    RUN_QUEUE.with(|q| q.borrow().len() == 0)
}

/// Claim this thread for a new reactor, starting it on an empty run queue.
pub(super) fn claim_thread() {
    assert!(
        !REACTOR_LIVE.replace(true),
        "a second reactor on this thread would orphan the first's wakes"
    );
    RUN_QUEUE.with(|q| q.borrow_mut().clear());
}

/// The thread's claim ends when the last handle drops.
impl Drop for ReactorShared {
    fn drop(&mut self) {
        REACTOR_LIVE.set(false);
    }
}

unsafe fn waker_clone(data: *const ()) -> RawWaker {
    RawWaker::new(data, &WAKER_VTABLE)
}

unsafe fn waker_wake(data: *const ()) {
    // `try_with`: a wake from a TLS destructor that runs after the queue's own is
    // a no-op.
    let _ = RUN_QUEUE.try_with(|q| q.borrow_mut().push(data as usize));
}

unsafe fn waker_drop(_data: *const ()) {}

const WAKER_VTABLE: RawWakerVTable = RawWakerVTable::new(waker_clone, waker_wake, waker_wake, waker_drop);

/// The waker for task `key`: the key itself *is* the waker's data pointer, so
/// clone is a bitwise copy and drop is a no-op. Waking pushes the key onto the
/// thread-local run queue. Waking only queues the key and never polls, so a wake
/// may be issued under any reactor borrow.
fn make_waker(key: usize) -> Waker {
    let raw = RawWaker::new(key as *const (), &WAKER_VTABLE);
    unsafe { Waker::from_raw(raw) }
}

#[cfg(test)]
#[path = "tests/runloop.rs"]
mod tests;

#[cfg(test)]
#[path = "benches/runloop.rs"]
mod bench;
