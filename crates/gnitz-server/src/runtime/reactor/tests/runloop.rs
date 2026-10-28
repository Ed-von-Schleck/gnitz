//! Task scheduling and the pass: `spawn`, the run queue's dedup, the waker
//! vtable the wakes land in, `block_on`, and how long a pass sleeps.

use std::time::Duration;

use super::super::test_support::*;
use super::*;

/// A panic during poll must propagate rather than be swallowed: it unwinds
/// through the pass and, in real use, up to `block_on`'s caller.
#[test]
#[should_panic(expected = "boom")]
fn panic_in_spawned_task_propagates_not_swallowed() {
    let r = make_reactor();
    r.spawn(async {
        panic!("boom");
    });
    r.tick(false);
}

/// A task may spawn during its own poll, and what it spawns runs.
#[test]
fn a_task_spawns_during_its_own_poll() {
    let r = make_reactor();
    let ran = Rc::new(Cell::new(0));
    let (r2, ran2) = (r.clone(), Rc::clone(&ran));
    r.spawn(async move {
        for _ in 0..64 {
            let ran = Rc::clone(&ran2);
            r2.spawn(async move { ran.set(ran.get() + 1) });
        }
    });
    r.block_until_idle();
    assert_eq!(ran.get(), 64);
}

/// `RunQueue::push` dedups by task key, so N wakes before a task's next poll cost
/// one poll, not N — here through a clone that outlived the waker it came from.
#[test]
fn repeated_wakes_for_one_key_collapse_to_a_single_poll() {
    let r = make_reactor();
    let polls = Rc::new(Cell::new(0u32));
    let kept: Rc<RefCell<Option<Waker>>> = Rc::default();
    let (p, k) = (Rc::clone(&polls), Rc::clone(&kept));
    r.spawn(std::future::poll_fn(move |cx| {
        p.set(p.get() + 1);
        k.borrow_mut().get_or_insert_with(|| cx.waker().clone());
        Poll::<()>::Pending
    }));
    r.tick(false); // the spawn's own first poll
    assert_eq!(polls.get(), 1);

    let waker = kept.borrow().clone().expect("the first poll kept its waker");
    for _ in 0..16 {
        waker.wake_by_ref();
    }
    r.tick(false);
    assert_eq!(
        polls.get(),
        2,
        "16 wakes for one key must cost exactly one further poll"
    );
}

/// `block_on` returns in the pass that completes its future, whatever else is
/// parked: nothing would ever end a sleep there.
#[test]
fn block_on_returns_beside_a_parked_task() {
    within(|| {
        let r = make_reactor();
        r.spawn(std::future::pending());
        assert_eq!(r.block_on(async { 7 }), 7);
    });
}

/// `block_on`'s future is woken as a task is: here by a task's send, once the
/// future has parked on the receiver.
#[test]
fn block_on_is_woken_by_a_task() {
    within(|| {
        let r = make_reactor();
        let (tx, rx) = oneshot::channel();
        r.spawn(async move {
            yield_once().await;
            tx.send(9);
        });
        assert_eq!(r.block_on(rx), 9);
    });
}

/// A blocking pass with a timer pending sleeps until that timer's deadline — not
/// forever, not past it, and not for a later one — with no CQE involved.
#[test]
fn a_sleeping_tick_wakes_at_the_earliest_deadline() {
    within(|| {
        let r = make_reactor();
        let start = Instant::now();
        let fired = Rc::new(Cell::new(false));
        let f = Rc::clone(&fired);
        let early = r.sleep(Duration::from_millis(20));
        r.spawn(async move {
            early.await;
            f.set(true);
        });
        r.spawn(r.sleep(Duration::from_secs(3600)));

        let mut ticks = 0;
        loop {
            r.run_ready();
            ticks += 1;
            // Otherwise the pass that polled the early timer sleeps on toward the
            // later deadline.
            if fired.get() {
                break;
            }
            r.submit_or_sleep(true);
        }
        assert!(start.elapsed() >= Duration::from_millis(20), "woke before the deadline");
        assert!(ticks <= 16, "{ticks} ticks: the reactor spun instead of sleeping");
        assert_eq!(r.deadlines.borrow().len(), 1, "only the later deadline is left");
        r.tasks.borrow_mut().clear(); // the pending timer holds the reactor
    });
}

/// A pass polls only the tasks queued on its entry, so a task that wakes itself
/// on every poll runs once a pass and the pass still reaches its deadlines.
#[test]
fn a_self_waking_task_does_not_starve_the_pass() {
    within(|| {
        let r = make_reactor();
        r.spawn(async {
            loop {
                yield_once().await
            }
        });
        r.block_on(r.sleep(Duration::from_millis(5)));
    });
}

/// The caller of `block_on` may block outside the loop once it returns (a test
/// reading a socket, or the process exiting), so the SQEs of the pass that
/// completed its future are already in the kernel.
#[test]
fn block_on_submits_the_pass_that_completed_it() {
    let r = make_reactor();
    let mut op = r.block_on(std::future::poll_fn(|_| Poll::Ready(nop(&r))));
    r.drain_cqes_into_wakers();
    assert_eq!(
        try_poll_once(&mut op),
        Some(0),
        "block_on returned with its last pass's SQE unsubmitted"
    );
}

/// A thread holds one live reactor at a time: its wakes land in the one
/// thread-local run queue.
#[test]
#[should_panic(expected = "second reactor")]
fn a_second_live_reactor_on_one_thread_panics() {
    let _first = make_reactor();
    let _second = make_reactor();
}

/// The thread's claim ends with its reactor, so a later one may take it.
#[test]
fn a_dropped_reactor_frees_its_thread() {
    drop(make_reactor());
    drop(make_reactor());
}

/// More ops than the SQ holds, queued at once: the push that finds the SQ full
/// submits it, the next pass drains those CQEs in several full drain batches, and
/// the pass after drains the rest. No completion is lost. The count stays within
/// the CQ (twice the SQ), which nothing drains while the ops are queued.
#[test]
fn a_pass_past_the_sq_and_the_drain_batch_loses_no_completion() {
    let r = make_reactor();
    let n = SQ_ENTRIES + SQ_ENTRIES / 2;
    let ops: Vec<_> = (0..n).map(|_| nop(&r)).collect();
    r.tick(false);
    r.run_ready();
    assert!(ops.into_iter().all(|op| try_poll_once(op) == Some(0)));
}
