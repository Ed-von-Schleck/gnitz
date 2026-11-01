//! The reactor's loop: `spawn` task scheduling, the `block_on` driver and the two
//! halves of a pass it runs, the W2M park the second arms before it sleeps, the
//! CQE dispatch table, and the run queue and waker vtable the wakes land in.

use std::collections::VecDeque;
use std::mem::MaybeUninit;

use io_uring::cqueue;

use super::*;

/// The waker key of `block_on`'s own future. A task's waker key is its slot in
/// `tasks` plus one, so no task gets this key.
const ROOT: usize = 0;

impl Reactor {
    /// Spawn a task that runs detached.
    pub(crate) fn spawn(&self, fut: impl Future<Output = ()> + 'static) {
        let slot = self.tasks.borrow_mut().insert(Some(Box::pin(fut)));
        RUN_QUEUE.with(|q| q.borrow_mut().push(slot + 1));
    }

    /// Drive `fut` to completion beside the spawned tasks, and return its output.
    /// The one driver of the loop, called outside any task. `fut` is polled in
    /// place under the [`ROOT`] key, after the tasks of each pass that woke it.
    pub(crate) fn block_on<F: Future>(&self, fut: F) -> F::Output {
        let out = {
            let mut fut = std::pin::pin!(fut);
            let waker = make_waker(ROOT);
            let mut cx = Context::from_waker(&waker);
            // The first pass polls `fut`.
            RUN_QUEUE.with(|q| q.borrow_mut().push(ROOT));
            loop {
                if self.run_ready() {
                    if let Poll::Ready(v) = fut.as_mut().poll(&mut cx) {
                        break v;
                    }
                }
                self.submit_or_sleep(true);
            }
        };
        // The pass that completed `fut` submits and returns; any other may sleep.
        self.submit_or_sleep(false);
        out
    }

    /// Drain CQEs and every W2M ring, then fire every passed deadline — ahead of
    /// the poll, so what landed since the last pass is served in this one. Then
    /// poll each task queued on entry, once; a task woken by those polls runs next
    /// pass. A task spawned by them runs next pass too, unless a stale wake left
    /// its reused key queued, when it runs later in this one. True when [`ROOT`]
    /// was among those queued.
    pub(super) fn run_ready(&self) -> bool {
        self.dispatch_cqes();
        self.drain_all_w2m();
        self.timers.fire();

        let mut root = false;
        for _ in 0..RUN_QUEUE.with(|q| q.borrow().len()) {
            match RUN_QUEUE.with(|q| q.borrow_mut().pop()) {
                ROOT => root = true,
                key => self.poll_task(key),
            }
        }
        root
    }

    /// Re-arm the recvs this pass's tasks resumed, then submit queued SQEs.
    /// When `block` and nothing is runnable, arm the W2M park and sleep until
    /// the next CQE, W2M publish or earliest deadline instead — unless a
    /// deadline has passed or the drain run before arming woke a task. The park
    /// is armed only on a pass that sleeps: the publish that takes an arm spends
    /// a cross-process `FUTEX_WAKE`.
    pub(super) fn submit_or_sleep(&self, block: bool) {
        self.resume_recvs();
        let until = self.timers.earliest();
        let sleep = block
            && run_queue_is_empty()
            && until.is_none_or(|at| at > Instant::now())
            && (self.parked.get() || self.arm_park());
        if sleep {
            // Measured after the arm, whose drain takes time of its own.
            let timeout = until.map(|at| at.saturating_duration_since(Instant::now()));
            self.ring.borrow_mut().wait(timeout);
        } else {
            self.ring.borrow_mut().submit();
        }
    }

    /// Arm the W2M park for a sleep. False, arming nothing, when the drain run
    /// first woke a task.
    fn arm_park(&self) -> bool {
        loop {
            self.drain_all_w2m();
            if !run_queue_is_empty() {
                return false;
            }
            if let Some((word, at)) = self.w2m.arm() {
                // A 32-bit word with no `FUTEX2_PRIVATE`, since the park is
                // `MAP_SHARED` across `fork()`; the mask matches any waker.
                const FUTEX2_SIZE_U32: u32 = 2;
                let mask = libc::FUTEX_BITSET_MATCH_ANY as u32 as u64;
                let sqe = opcode::FutexWait::new(word.cast(), at as u64, mask, FUTEX2_SIZE_U32).build();
                // SAFETY: the word is in a never-unmapped shared mapping, and the
                // SQE names no other memory.
                unsafe { self.ring.borrow_mut().push(sqe, udata(KIND_PARK, 0)) };
                self.parked.set(true);
                return true;
            }
        }
    }

    /// Poll the task of waker `key`. The future is taken out of its slot for the
    /// duration of the poll, which leaves the slot holding `None`, so a `spawn`
    /// from the poll cannot take its key; no borrow of `tasks` spans the poll. A
    /// completed task frees its slot, and drops at the end of the function, after
    /// the borrow, since its drop may re-enter the reactor.
    fn poll_task(&self, key: usize) {
        let slot = key - 1;
        let Some(mut task) = self.tasks.borrow_mut().get_mut(slot).and_then(Option::take) else {
            return;
        };
        if task
            .as_mut()
            .poll(&mut Context::from_waker(&make_waker(key)))
            .is_pending()
        {
            self.tasks.borrow_mut()[slot] = Some(task);
        } else {
            self.tasks.borrow_mut().remove(slot);
        }
    }

    /// Drain all CQEs pending in the ring and route each through `dispatch_cqe`.
    fn dispatch_cqes(&self) {
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

    fn dispatch_cqe(&self, user_data: u64, res: i32) {
        let id = user_data & ID_MASK;
        match user_data >> KIND_SHIFT {
            KIND_PARK => {
                // No drain here: the pass's own drain follows CQE dispatch.
                // The boot's `check_host` saw the opcode supported, so these are
                // a malformed wait, which no later pass repairs.
                if res == -libc::ENOSYS || res == -libc::EINVAL {
                    gnitz_fatal_abort!(
                        "reactor: IORING_OP_FUTEX_WAIT failed: {}",
                        std::io::Error::from_raw_os_error(-res)
                    );
                }
                // No disarm: a take clears the arm. A CQE with no take behind it,
                // such as a late wake from an earlier take, leaves the arm set,
                // and the next `arm` sets it again and re-tests the rings.
                self.parked.set(false);
            }
            KIND_OP => {
                let slot = (id & OP_SLOT_MASK) as usize;
                let mut ops = self.ops.borrow_mut();
                let entry = ops.get_mut(slot).expect("an op's entry lives until its CQE");
                match std::mem::replace(entry, OpSlot::Done(res)) {
                    OpSlot::Waiting(waker) => {
                        drop(ops);
                        if let Some(w) = waker {
                            w.wake();
                        }
                    }
                    OpSlot::Abandoned(carry) => {
                        ops.remove(slot);
                        drop(ops);
                        // Outside the borrow: the carry may hold the last handle
                        // on a connection.
                        drop(carry);
                    }
                    OpSlot::Done(_) => unreachable!("a second CQE for op {user_data:#x}"),
                }
            }
            KIND_RECV => self.handle_recv_cqe(id as usize, res),
            KIND_CANCEL => {}
            kind => unreachable!("CQE kind {kind} (user_data={user_data:#x}) was never issued"),
        }
    }
}

// ---------------------------------------------------------------------------
// Run queue + waker vtable
// ---------------------------------------------------------------------------

/// The keys of tasks whose wakers have fired, in wake order. A task woken several
/// times before its poll is queued once; `queued[key]` is set while it is.
struct RunQueue {
    queue: VecDeque<usize>,
    queued: Vec<bool>,
}

impl RunQueue {
    const fn new() -> Self {
        Self {
            queue: VecDeque::new(),
            queued: Vec::new(),
        }
    }

    /// Enqueue `key` unless it is already pending. A stale waker's key is queued
    /// too, and costs at most one spurious poll.
    fn push(&mut self, key: usize) {
        if key >= self.queued.len() {
            self.queued.resize(key + 1, false);
        }
        if !std::mem::replace(&mut self.queued[key], true) {
            self.queue.push_back(key);
        }
    }

    /// The oldest queued key. A wake of it from here on queues it again.
    fn pop(&mut self) -> usize {
        let key = self.queue.pop_front().expect("pop on an empty run queue");
        self.queued[key] = false;
        key
    }

    fn len(&self) -> usize {
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

fn run_queue_is_empty() -> bool {
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
/// may be issued under any reactor borrow. A key is reused once its task
/// completes, so a waker that outlives its task costs the key's next task at
/// most one spurious poll per wake.
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
