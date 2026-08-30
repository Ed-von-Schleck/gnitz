//! Task scheduling and the tick body: `spawn` / `block_on`, the run queue's
//! dedup, the waker vtable, and the order in which one tick drains and polls.

use std::cell::Cell as StdCell;
use std::time::Duration;

use super::super::test_support::*;
use super::*;

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
    assert_eq!(r.inner.tasks.borrow().len(), 0, "a finished task must leave the slab");
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
    assert!(r.inner.run_queue.borrow().is_queued(123));
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

/// `block_until_idle` must drive spawned tasks to completion.
#[test]
fn block_until_idle_completes() {
    let r = make_reactor();
    let done: Rc<StdCell<bool>> = Rc::new(StdCell::new(false));
    let done2 = Rc::clone(&done);
    let lease = r.alloc_replies(1);
    let id = lease[0];
    let reply = r.await_reply(id);
    r.spawn(async move {
        let _ = reply.await;
        done2.set(true);
    });
    r.route_reply(0, id as u32, synthetic_decoded_wire(id));
    r.block_until_idle();
    assert!(done.get());
    assert_eq!(r.inner.tasks.borrow().len(), 0);
}

/// `RunQueue::push` dedups by task key, so the N wakes one drain can deliver to
/// a single task — `drain_all_w2m` completes one reply per worker, all waking
/// the same tick task — cost one poll, not N.
#[test]
fn repeated_wakes_for_one_key_collapse_to_a_single_poll() {
    /// Counts its polls and never completes, so every wake is one more poll.
    struct CountPolls(Rc<StdCell<u32>>);
    impl Future for CountPolls {
        type Output = ();
        fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<()> {
            self.0.set(self.0.get() + 1);
            Poll::Pending
        }
    }

    let r = make_reactor();
    let polls: Rc<StdCell<u32>> = Rc::new(StdCell::new(0));
    let key = r.spawn(CountPolls(Rc::clone(&polls)));
    r.tick(false); // the spawn's own first poll
    assert_eq!(polls.get(), 1);

    let waker = make_waker(key);
    for _ in 0..16 {
        waker.wake_by_ref();
    }
    assert_eq!(
        r.inner.run_queue.borrow().len(),
        1,
        "16 wakes for one key must leave one run-queue entry"
    );
    r.tick(false);
    assert_eq!(polls.get(), 2, "and cost exactly one further poll");
}

/// A tick drains the W2M rings at step 1, ahead of the task poll, so a reply
/// that arrived since the last tick is served *in this tick* rather than
/// waiting a whole tick body for the next one.
#[test]
fn a_reply_routed_before_the_poll_is_served_in_the_same_tick() {
    let r = make_reactor();
    let lease = r.alloc_replies(1);
    let id = lease[0];
    let done: Rc<StdCell<bool>> = Rc::new(StdCell::new(false));
    let done2 = Rc::clone(&done);
    let reply = r.await_reply(id);
    r.spawn(async move {
        let _ = reply.await;
        done2.set(true);
    });
    r.tick(false); // the task parks on the reply

    r.route_reply(0, id as u32, synthetic_decoded_wire(id));
    r.tick(false);
    assert!(done.get(), "the wake must be polled in the tick that saw it");
}
