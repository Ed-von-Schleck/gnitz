//! Reactor state: W2M routing by ring id — leases and their id space, the
//! release of an unrouted frame — and when a tick sleeps.

use std::time::Duration;

use super::test_support::*;
use super::*;
use crate::runtime::sal::{SalMessageKind, WorkerSet};
use crate::runtime::test_support::{fork_child, try_poll_once};
use gnitz_wire::{WireFault, WireStatus};

/// A lease routes its id for exactly its own life, and the next lease takes
/// the next id.
#[test]
fn a_lease_routes_its_id_until_dropped() {
    let (r, _writers) = reactor_with_rings(3);
    let acks = r.lease_acks("test");
    assert_eq!(
        r.inner.acks.borrow().keys().copied().collect::<Vec<_>>(),
        vec![acks.id()]
    );

    let train = r.lease_train(WorkerSet::one(1), SalMessageKind::Scan);
    assert_eq!(train.id(), acks.id() + 1, "the next lease takes the next id");
    assert_eq!(train.workers().iter().collect::<Vec<_>>(), vec![1]);
    assert_eq!(
        r.inner.trains.borrow().keys().copied().collect::<Vec<_>>(),
        vec![train.id()]
    );

    drop(acks);
    assert!(r.inner.acks.borrow().is_empty(), "dropping a lease removes its route");
    assert_eq!(r.inner.trains.borrow().len(), 1, "and only its own");
    drop(train);
    assert!(r.inner.trains.borrow().is_empty());
}

/// The window a lease exists to close: an ACK drained after the id is leased
/// but before anything awaits it is kept for the awaiter.
#[test]
fn an_ack_landing_before_its_awaiter_is_kept() {
    let (r, writers) = reactor_with_rings(1);
    let lease = r.lease_acks("test");
    writers[0].send_status(0, lease.id(), WireStatus::Error, b"boom");
    r.drain_all_w2m();

    assert!(
        matches!(try_poll_once(lease.acks()), Some(Err(_))),
        "an ACK drained before its awaiter must be there on the first poll"
    );
}

/// `acks` parks until every worker has answered, then reports the fault by the
/// ring it arrived on.
#[test]
fn acks_resolve_on_the_last_ack_and_name_the_faulting_ring() {
    let (r, writers) = reactor_with_rings(2);
    let lease = r.lease_acks("test");
    let mut fut = std::pin::pin!(lease.acks());
    let waker = make_waker(0);
    let mut cx = Context::from_waker(&waker);
    assert!(fut.as_mut().poll(&mut cx).is_pending());

    writers[1].send_status(0, lease.id(), WireStatus::Error, b"boom");
    r.drain_all_w2m();
    assert!(
        fut.as_mut().poll(&mut cx).is_pending(),
        "one of two ACKs must not resolve"
    );

    writers[0].send_status(0, lease.id(), WireStatus::Ok, &[]);
    r.drain_all_w2m();
    assert!(is_queued(0), "the last ACK wakes the awaiter");
    match fut.as_mut().poll(&mut cx) {
        Poll::Ready(Err(e)) => {
            assert!(e.text.contains("worker 1"), "the fault names its ring: {e}");
            assert!(e.text.contains("boom"), "the fault carries the worker message: {e}");
        }
        _ => panic!("the fault must resolve the wait, named after its ring"),
    }
}

/// A frame for an id no lease routes is released at the ring without creating a
/// route — how an abandoned request's late reply is disposed of.
#[test]
fn an_unrouted_frame_is_released_undecoded() {
    let (r, writers) = reactor_with_rings(1);
    writers[0].send_status(0, 77, WireStatus::Ok, &[]);
    let before = r.inner.w2m.release_cursor(0);
    r.drain_all_w2m();
    assert!(r.inner.w2m.release_cursor(0) > before, "the slot is released");
    assert!(r.inner.acks.borrow().is_empty(), "and no route is created");
}

/// Lease ids wrap past 0, which no lease takes, and a lease never takes an id a
/// live lease still routes.
#[test]
fn lease_ids_wrap_past_zero_and_skip_live_ids() {
    let (r, _writers) = reactor_with_rings(1);
    r.inner.next_request_id.set(u32::MAX);
    let top = r.lease_acks("test");
    assert_eq!(top.id(), u32::MAX);
    let wrapped = r.lease_acks("test");
    assert_eq!(wrapped.id(), 1, "the id after u32::MAX is 1, not 0");

    r.inner.next_request_id.set(1);
    let skipped = r.lease_train(WorkerSet::ALL, SalMessageKind::Scan);
    assert_eq!(skipped.id(), 2, "a live lease's id is skipped");
}

/// The drain the arm runs first can route a frame that wakes a task; that tick
/// must then run on instead of arming and sleeping.
#[test]
fn a_tick_whose_arm_drain_wakes_a_task_does_not_arm() {
    within(Duration::from_secs(30), || {
        let (r, mut writers) = reactor_with_rings(1);
        let writer = writers.pop().expect("one ring");
        let lease = r.lease_acks("test");
        let id = lease.id();
        let done = Rc::new(Cell::new(false));
        let d = Rc::clone(&done);
        r.spawn(async move {
            // Published during the poll, after this tick's own drain.
            writer.send_status(0, id, WireStatus::Ok, &[]);
            lease.acks().await.expect("an OK ACK");
            d.set(true);
        });
        r.tick(true);
        assert!(!r.inner.futex_waitv_armed.get(), "the woken tick must not arm");
        assert!(run_queue_len() > 0, "the drain woke the task");
        r.tick(false);
        assert!(done.get());
    });
}

/// `request_shutdown` from inside a task keeps that very tick from sleeping.
#[test]
fn request_shutdown_keeps_the_current_tick_from_sleeping() {
    within(Duration::from_secs(30), || {
        let r = Rc::new(make_reactor());
        let r2 = Rc::clone(&r);
        r.spawn(async move {
            r2.request_shutdown();
            std::future::pending::<()>().await
        });
        r.tick(true);
        assert!(r.inner.shutdown.get());
        r.inner.tasks.borrow_mut().clear(); // the task holds the reactor
    });
}

/// A blocking tick with a timer pending sleeps until that timer's deadline — not
/// forever, not past it, and not for a later one — with no CQE involved.
#[test]
fn a_sleeping_tick_wakes_at_the_earliest_deadline() {
    within(Duration::from_secs(30), || {
        let r = Rc::new(make_reactor());
        let start = Instant::now();
        let fired = Rc::new(Cell::new(false));
        let (f, r2) = (Rc::clone(&fired), Rc::clone(&r));
        let early = r.timer(start + Duration::from_millis(20));
        r.spawn(async move {
            early.await;
            f.set(true);
            // Otherwise the tick that polled this sleeps on toward the later deadline.
            r2.request_shutdown();
        });
        r.spawn(r.timer(start + Duration::from_secs(3600)));

        let mut ticks = 0;
        while !fired.get() {
            r.tick(true);
            ticks += 1;
        }
        assert!(start.elapsed() >= Duration::from_millis(20), "woke before the deadline");
        assert!(ticks <= 16, "{ticks} ticks: the reactor spun instead of sleeping");
        r.inner.tasks.borrow_mut().clear(); // the pending timer holds the reactor state
    });
}

/// A forked child floods a shared W2M ring while the parent drains through the
/// `FUTEX_WAITV` park: a lost wake runs into the timeout, a torn wrap loses an id.
#[test]
fn w2m_cross_process_stress_drains_all_messages_via_reactor() {
    use crate::runtime::w2m::W2mWriter;

    const N_MESSAGES: u64 = 500;
    const TIMEOUT: Duration = Duration::from_secs(30);

    let region = unsafe { crate::runtime::w2m::fixtures::test_ring(64 * 1024) };
    let ptr = region.ptr();

    let child = || {
        // Publish a monotonic req_id stream as fast as possible. The
        // parent's wake protocol must not drop any of them under the resulting
        // drain-refresh-arm race pressure.
        let writer = W2mWriter::new(ptr);
        for req_id in 1..=N_MESSAGES {
            writer.send_status(req_id, req_id as u32, WireStatus::Ok, &[]);
        }
    };

    let pid = unsafe { fork_child(child) };

    let reactor = make_reactor_over(W2mReceiver::new(vec![ptr]));
    // Only the last of the child's ids is leased: the rest are released unrouted,
    // and the lease resolves only once the reactor has drained every one.
    reactor.inner.next_request_id.set(N_MESSAGES as u32);
    let lease = Rc::new(reactor.lease_acks("stress"));
    let outcome: Rc<RefCell<Option<Result<(), WireFault>>>> = Rc::new(RefCell::new(None));
    // A oneshot rather than a polling watcher: a watcher that yields re-arms
    // its own waker every poll, so the run queue never empties and the
    // `FUTEX_WAITV` arm this test exists to stress never happens.
    let (done_tx, done_rx) = oneshot::channel::<()>();
    {
        let (lease, outcome) = (Rc::clone(&lease), Rc::clone(&outcome));
        reactor.spawn(async move {
            *outcome.borrow_mut() = Some(lease.acks().await);
            done_tx.send(());
        });
    }

    let timeout = reactor.timer(Instant::now() + TIMEOUT);
    if let Either::B(()) = reactor.block_on(select2(done_rx, timeout)) {
        unsafe { libc::kill(pid, libc::SIGKILL) };
        panic!("reactor stalled after {TIMEOUT:?} — lost-wake symptom");
    }

    assert!(matches!(*outcome.borrow(), Some(Ok(()))), "every ACK is OK");

    unsafe { crate::runtime::test_support::assert_child_exited_ok(pid) };
}

/// An unread publish must make the arm refuse, so the reactor drains instead of
/// arming a `FUTEX_WAITV` for a wake that already happened. The publish lands
/// before the arm, so there is no race to lose.
#[test]
fn arm_waitv_refuses_while_data_is_unread() {
    use crate::runtime::w2m::W2mWriter;

    let region = unsafe { crate::runtime::w2m::fixtures::test_ring(64 * 1024) };
    let ptr = region.ptr();
    W2mWriter::new(ptr).send_status(0, 1, WireStatus::Ok, &[]);

    let receiver = W2mReceiver::new(vec![ptr]);
    let mut out = [FutexWaitV::new(); 1];
    assert!(
        receiver.arm_waitv(&mut out).is_none(),
        "an unread publish must refuse the arm — arming it would be a lost wake",
    );
}

/// A publish on a ring past the first wakes a reactor parked on all of them: the
/// child answers on ring 0, waits until the reactor parks, then answers on ring 1.
#[test]
fn a_publish_on_a_later_ring_wakes_the_reactor() {
    use crate::runtime::w2m::fixtures::{master_parked, test_ring};
    use crate::runtime::w2m::W2mWriter;

    const TIMEOUT: Duration = Duration::from_secs(30);

    let r0 = unsafe { test_ring(64 * 1024) }.leak();
    let r1 = unsafe { test_ring(64 * 1024) }.leak();

    let child = || {
        W2mWriter::new(r0).send_status(0, 1, WireStatus::Ok, &[]);
        let deadline = Instant::now() + TIMEOUT;
        while !unsafe { master_parked(r1) } {
            assert!(Instant::now() < deadline, "the reactor never parked on ring 1");
            std::thread::yield_now();
        }
        W2mWriter::new(r1).send_status(0, 1, WireStatus::Ok, &[]);
    };
    let pid = unsafe { fork_child(child) };

    let r = Rc::new(make_reactor_over(W2mReceiver::new(vec![r0, r1])));
    // Request id 1 is a fresh reactor's first.
    let lease = r.lease_acks("later ring");
    let r2 = Rc::clone(&r);
    let out = r.block_on(async move { select2(lease.acks(), r2.timer(Instant::now() + TIMEOUT)).await });
    if let Either::B(()) = out {
        unsafe { libc::kill(pid, libc::SIGKILL) };
        panic!("reactor stalled after {TIMEOUT:?}: ring 1's wake never reached it");
    }
    assert!(matches!(out, Either::A(Ok(()))), "both ACKs are OK");
    unsafe { crate::runtime::test_support::assert_child_exited_ok(pid) };
}
