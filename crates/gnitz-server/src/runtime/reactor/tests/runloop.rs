//! Task scheduling and the pass: `spawn`, the run queue's dedup, the waker
//! vtable the wakes land in, `block_on`, how long a pass sleeps, and the W2M
//! park — when a pass arms it, and the publishes that wake it.

use std::time::Duration;

use super::super::test_support::*;
use super::super::uring::SQ_ENTRIES;
use super::*;
use crate::runtime::test_support::{assert_child_exited_ok, fork_child};

/// A panic during poll must propagate rather than be swallowed: it unwinds
/// through the pass and, in real use, up to `block_on`'s caller.
#[test]
#[should_panic(expected = "boom")]
fn panic_in_spawned_task_propagates_not_swallowed() {
    let r = make_reactor();
    r.spawn(async {
        panic!("boom");
    });
    r.tick();
}

/// A task may spawn during its own poll, and what it spawns runs.
#[test]
fn a_task_spawns_during_its_own_poll() {
    let r = make_reactor();
    let ran = Rc::new(Cell::new(0));
    let ran2 = Rc::clone(&ran);
    r.spawn(async move {
        for _ in 0..64 {
            let ran = Rc::clone(&ran2);
            r.spawn(async move { ran.update(|v| v + 1) });
        }
    });
    r.block_until_idle();
    assert_eq!(ran.get(), 64);
}

/// A waker that outlived its task wakes whichever task took its key since, or
/// none; either costs at most one spurious poll, however often it wakes, as
/// the run queue dedups by task key. `send_owned`'s eviction leaves
/// such a waker behind: the abandoned send's slot body keeps its task's waker
/// until the op's CQE, and a give-back of its ring fires it.
#[test]
fn a_stale_wake_costs_at_most_a_spurious_poll() {
    let r = make_reactor();
    let kept: Rc<RefCell<Option<Waker>>> = Rc::default();
    let k = Rc::clone(&kept);
    r.spawn(std::future::poll_fn(move |cx| {
        *k.borrow_mut() = Some(cx.waker().clone());
        Poll::Ready(())
    }));
    r.tick();
    let stale = kept.take().expect("the task kept its waker");
    stale.wake_by_ref();
    r.tick(); // its key is vacant: nothing to poll

    let polls = Rc::new(Cell::new(0u32));
    let p = Rc::clone(&polls);
    r.spawn(std::future::poll_fn(move |_| {
        p.update(|v| v + 1);
        Poll::<()>::Pending
    }));
    r.tick(); // the spawn takes the freed key
    assert_eq!(polls.get(), 1);
    for _ in 0..16 {
        stale.wake_by_ref();
    }
    r.tick();
    r.tick();
    assert_eq!(polls.get(), 2, "the stale wake costs one spurious poll, and no more");
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
        // Read before the reactor: a timer created outside a pass counts from
        // the clock the reactor read when it was built, which is no earlier.
        let start = Instant::now();
        let r = make_reactor();
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
        assert!(
            r.timers.earliest() > Some(start + Duration::from_secs(60)),
            "only the hour-long deadline is left"
        );
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
    let mut op = r.block_on(std::future::poll_fn(|_| Poll::Ready(nop(r))));
    r.dispatch_cqes();
    assert_eq!(
        try_poll_once(&mut op),
        Some((0, ())),
        "block_on returned with its last pass's SQE unsubmitted"
    );
}

/// More ops than the SQ holds, queued at once: the push that finds the SQ full
/// submits it, the next pass drains those CQEs in several full drain batches, and
/// the pass after drains the rest. No completion is lost. The count stays within
/// the CQ (twice the SQ), which nothing drains while the ops are queued.
#[test]
fn a_pass_past_the_sq_and_the_drain_batch_loses_no_completion() {
    let r = make_reactor();
    let n = SQ_ENTRIES + SQ_ENTRIES / 2;
    let ops: Vec<_> = (0..n).map(|_| nop(r)).collect();
    r.tick();
    r.run_ready();
    assert!(ops.into_iter().all(|op| try_poll_once(op) == Some((0, ()))));
}

/// The drain the arm runs first can read a position that wakes a task; that tick
/// must then run on instead of arming and sleeping, where nothing would wake it.
#[test]
fn a_tick_whose_arm_drain_wakes_a_task_does_not_arm() {
    within(|| {
        let (r, mut writers) = reactor_with_rings(1);
        let mut writer = writers.pop().expect("one ring");
        let wait = r.applied_through(1);
        let done = Rc::new(Cell::new(false));
        let d = Rc::clone(&done);
        r.spawn(async move {
            // Published during the poll, after this tick's own drain.
            writer.applied(1);
            wait.await;
            d.set(true);
        });
        r.run_ready();
        r.submit_or_sleep(true);
        r.tick();
        assert!(done.get());
    });
}

/// A pass that does not sleep leaves the park unarmed, so a worker's publish
/// then spends no `FUTEX_WAKE`.
#[test]
fn a_pass_that_does_not_sleep_does_not_arm() {
    let (r, writers) = reactor_with_rings(1);
    r.tick();
    assert!(!writers[0].master_parked());
}

/// A forked child publishes to a reactor parked on the one master park word over
/// two rings: a lost wake runs into the timeout. The child waits until the
/// reactor parks with nothing published, so only its position on ring 1 can
/// wake it; then it raises ring 0's position as fast as it can, so the drain-arm
/// race is under pressure, and the wait resolves only on the last of them.
#[test]
fn w2m_publishes_from_another_process_wake_the_parked_reactor() {
    const N_POSITIONS: u64 = 500;
    const TIMEOUT: Duration = Duration::from_secs(30);

    let (r, mut writers) = reactor_with_rings(2);
    let child = || {
        let deadline = Instant::now() + TIMEOUT;
        while !writers[1].master_parked() {
            assert!(Instant::now() < deadline, "the reactor never parked");
            std::thread::yield_now();
        }
        writers[1].applied(N_POSITIONS);
        for n in 1..=N_POSITIONS {
            writers[0].applied(n);
        }
    };
    let pid = unsafe { fork_child(child) };

    let reached = r.applied_through(N_POSITIONS);
    if let Either::B(()) = r.block_on(select2(reached, r.sleep(TIMEOUT))) {
        unsafe { libc::kill(pid, libc::SIGKILL) };
        panic!("reactor stalled after {TIMEOUT:?}: a lost wake");
    }
    assert_child_exited_ok(pid);
}
