//! Task scheduling and the pass: `spawn`, the run queue's dedup, the waker
//! vtable the wakes land in, `block_on`, how long a pass sleeps, and the W2M
//! park — when a pass arms it, and the publishes that wake it.

use std::time::Duration;

use super::super::test_support::*;
use super::super::uring::SQ_ENTRIES;
use super::*;
use crate::runtime::park::WorkerSet;
use crate::runtime::test_support::{assert_child_exited_ok, fork_child};
use crate::runtime::w2m::fixtures::id;
use crate::runtime::w2m::RequestId;

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
    r.tick(); // the spawn's own first poll
    assert_eq!(polls.get(), 1);

    let waker = kept.borrow().clone().expect("the first poll kept its waker");
    for _ in 0..16 {
        waker.wake_by_ref();
    }
    r.tick();
    assert_eq!(
        polls.get(),
        2,
        "16 wakes for one key must cost exactly one further poll"
    );
}

/// A waker that outlived its task wakes whichever task took its key since, or
/// none; either costs at most one spurious poll. `send_owned`'s eviction leaves
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
        p.set(p.get() + 1);
        Poll::<()>::Pending
    }));
    r.tick(); // the spawn takes the freed key
    assert_eq!(polls.get(), 1);
    stale.wake();
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
    r.dispatch_cqes();
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
    r.tick();
    r.run_ready();
    assert!(ops.into_iter().all(|op| try_poll_once(op) == Some(0)));
}

/// The drain the arm runs first can route a frame that wakes a task; that tick
/// must then run on instead of arming and sleeping, where nothing would wake it.
#[test]
fn a_tick_whose_arm_drain_wakes_a_task_does_not_arm() {
    within(|| {
        let (r, mut writers) = reactor_with_rings(1);
        let mut writer = writers.pop().expect("one ring");
        let lease = r.lease_acks(WorkerSet::ALL);
        let id = lease.id();
        let done = Rc::new(Cell::new(false));
        let d = Rc::clone(&done);
        r.spawn(async move {
            // Published during the poll, after this tick's own drain.
            writer.ack(id);
            lease.acks().await;
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

/// A sleep arms the park, and the publish that ends it takes the arm.
///
/// A publish that lands between the arm and the re-test behind it is drained by
/// the arm loop, which then arms again and sleeps for the next publish. So the
/// publisher answers every arm it sees until the tick returns.
#[test]
fn a_publish_wakes_the_sleep_and_takes_the_arm() {
    within(|| {
        let (r, mut writers) = reactor_with_rings(1);
        let mut writer = writers.pop().expect("one ring");
        let woke = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let publisher = std::thread::spawn({
            let woke = woke.clone();
            move || {
                while !woke.load(std::sync::atomic::Ordering::SeqCst) {
                    if writer.master_parked() {
                        writer.ack(RequestId::BOOT_READY); // no lease holds it
                    }
                    std::thread::yield_now();
                }
                writer
            }
        });
        r.run_ready();
        r.submit_or_sleep(true);
        woke.store(true, std::sync::atomic::Ordering::SeqCst);
        let writer = publisher.join().expect("the publisher");
        assert!(!writer.master_parked(), "the publish took the arm");
    });
}

/// A forked child publishes to a reactor parked on the one master park word over
/// two rings: a lost wake runs into the timeout, a torn frame loses an id. The child
/// waits until the reactor parks with nothing published, so only its answer on
/// ring 1 can wake it; then it floods ring 0 as fast as it can, so the
/// drain-arm race is under pressure: ids no lease routes, which are
/// released unrouted, and the ready ACK last, so the lease resolves only once
/// every one is drained.
#[test]
fn w2m_publishes_from_another_process_wake_the_parked_reactor() {
    const N_MESSAGES: u32 = 500;
    const TIMEOUT: Duration = Duration::from_secs(30);

    let (r, ready, mut writers) = booting_reactor(2);
    let child = || {
        let deadline = Instant::now() + TIMEOUT;
        while !writers[1].master_parked() {
            assert!(Instant::now() < deadline, "the reactor never parked");
            std::thread::yield_now();
        }
        writers[1].ack(RequestId::BOOT_READY);
        for n in 2..=N_MESSAGES {
            writers[0].ack(id(n));
        }
        writers[0].ack(RequestId::BOOT_READY);
    };
    let pid = unsafe { fork_child(child) };

    if let Either::B(()) = r.block_on(select2(ready.all_acked(), r.sleep(TIMEOUT))) {
        unsafe { libc::kill(pid, libc::SIGKILL) };
        panic!("reactor stalled after {TIMEOUT:?}: a lost wake");
    }
    unsafe { assert_child_exited_ok(pid) };
}
