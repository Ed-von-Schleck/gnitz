//! Task scheduling and the tick body: `spawn`, the run queue's dedup, and the
//! waker vtable the wakes land in.

use std::cell::Cell as StdCell;

use super::super::test_support::*;
use super::*;

/// A spawned task runs to completion and leaves the slab.
#[test]
fn spawn_runs_to_completion() {
    let r = make_reactor();
    let counter: Rc<StdCell<u32>> = Rc::new(StdCell::new(0));
    let c2 = Rc::clone(&counter);
    r.spawn(async move {
        YieldOnce::new().await;
        c2.set(c2.get() + 1);
    });
    r.block_until_idle();
    assert_eq!(counter.get(), 1);
    assert_eq!(r.inner.tasks.borrow().len(), 0, "a finished task must leave the slab");
}

/// A panic during poll must propagate rather than be swallowed: it unwinds
/// through `tick` and, in real use, up to the caller.
#[test]
#[should_panic(expected = "boom")]
fn panic_in_spawned_task_propagates_not_swallowed() {
    let r = make_reactor();
    r.spawn(async {
        panic!("boom");
    });
    r.tick(false);
}

/// Cloning a waker, dropping the original, then waking the clone must still
/// schedule the task. With the key-as-pointer design clone is a bitwise copy
/// and drop a no-op, but the behaviour must still be observable.
#[test]
fn waker_clone_outlives_original() {
    let r = make_reactor();
    let original = make_waker(123);
    let cloned = original.clone();
    drop(original);
    cloned.wake();
    assert!(r.inner.run_queue.borrow().is_queued(123));
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
