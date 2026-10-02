//! Reactor state: W2M routing by ring id — leases and their id space, the
//! release of an unrouted frame — and when a pass sleeps.

use std::time::Duration;

use super::test_support::*;
use super::*;
use crate::runtime::sal::{SalMessageKind, WorkerSet};
use crate::runtime::test_support::fork_child;
use gnitz_wire::WireStatus;

/// An ACK drained before anything awaits it is kept for the awaiter, and `acks`
/// parks until the last worker has answered OK.
#[test]
fn acks_keep_early_answers_and_resolve_on_the_last_ok() {
    let (r, writers) = reactor_with_rings(3);
    let lease = r.lease_acks("test", WorkerSet::ALL);
    writers[0].send_status(lease.id(), WireStatus::Ok, &[]);
    writers[1].send_status(lease.id(), WireStatus::Ok, &[]);
    r.drain_all_w2m(); // before any awaiter

    let mut fut = std::pin::pin!(lease.acks());
    let (flag, waker) = WakeFlag::new();
    let mut cx = Context::from_waker(&waker);
    assert!(
        fut.as_mut().poll(&mut cx).is_pending(),
        "two of three ACKs must not resolve"
    );

    writers[2].send_status(lease.id(), WireStatus::Ok, &[]);
    r.drain_all_w2m();
    assert!(flag.woken(), "the last ACK wakes the awaiter");
    assert!(matches!(fut.as_mut().poll(&mut cx), Poll::Ready(Ok(()))));
}

/// A fault resolves `acks` without waiting for the other workers, reported as the
/// lowest-numbered failed worker's whatever the arrival order.
#[test]
fn acks_resolve_on_a_fault_naming_the_lowest_failed_worker() {
    let (r, writers) = reactor_with_rings(3);
    let lease = r.lease_acks("test", WorkerSet::ALL);
    let mut fut = std::pin::pin!(lease.acks());
    let (flag, waker) = WakeFlag::new();
    assert!(fut.as_mut().poll(&mut Context::from_waker(&waker)).is_pending());

    writers[2].send_status(lease.id(), WireStatus::Error, b"late");
    r.drain_all_w2m();
    assert!(flag.woken(), "a fault wakes the awaiter with workers still to answer");
    writers[1].send_status(lease.id(), WireStatus::Error, b"boom");
    r.drain_all_w2m();
    match fut.as_mut().poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(Err(e)) => assert_eq!(e.text, "worker 1: test: boom"),
        _ => panic!("the fault must resolve the wait"),
    }
}

/// A frame for an id no lease routes is released at the ring undecoded — here a
/// dropped lease's, as an abandoned request's late reply is — and a new lease
/// does not take that id straight back.
#[test]
fn a_dropped_leases_late_frame_is_released_and_its_id_not_reused() {
    let (r, writers) = reactor_with_rings(3);
    let id = r.lease_acks("test", WorkerSet::ALL).id();
    writers[0].send_status(id, WireStatus::Ok, &[]);
    r.drain_all_w2m();
    assert_eq!(r.w2m.release_cursor(0), r.w2m.write_cursor(0));

    let train = r.lease_train(WorkerSet::ALL, SalMessageKind::ScanSpec);
    assert_ne!(train.id(), id);
    assert_eq!(
        train.workers().iter().collect::<Vec<_>>(),
        [0, 1, 2],
        "clipped to the launched"
    );
}

/// Lease ids wrap past 0, which no lease takes, and a lease never takes an id a
/// live lease still routes.
#[test]
fn lease_ids_wrap_past_zero_and_skip_live_ids() {
    let (r, _writers) = reactor_with_rings(1);
    r.next_request_id.set(u32::MAX);
    let top = r.lease_acks("test", WorkerSet::ALL);
    assert_eq!(top.id(), u32::MAX);
    let wrapped = r.lease_acks("test", WorkerSet::ALL);
    assert_eq!(wrapped.id(), 1, "the id after u32::MAX is 1, not 0");

    r.next_request_id.set(1);
    let skipped = r.lease_train(WorkerSet::ALL, SalMessageKind::ScanSpec);
    assert_eq!(skipped.id(), 2, "a live lease's id is skipped");
}

/// The drain the arm runs first can route a frame that wakes a task; that tick
/// must then run on instead of arming and sleeping.
#[test]
fn a_tick_whose_arm_drain_wakes_a_task_does_not_arm() {
    within(|| {
        let (r, mut writers) = reactor_with_rings(1);
        let writer = writers.pop().expect("one ring");
        let lease = r.lease_acks("test", WorkerSet::ALL);
        let id = lease.id();
        let done = Rc::new(Cell::new(false));
        let d = Rc::clone(&done);
        r.spawn(async move {
            // Published during the poll, after this tick's own drain.
            writer.send_status(id, WireStatus::Ok, &[]);
            lease.acks().await.expect("an OK ACK");
            d.set(true);
        });
        r.tick(true);
        assert!(!r.futex_waitv_armed.get(), "the woken tick must not arm");
        r.tick(false);
        assert!(done.get());
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

/// A forked child floods a shared W2M ring while the parent drains through the
/// `FUTEX_WAITV` park: a lost wake runs into the timeout, a torn wrap loses an id.
#[test]
fn w2m_cross_process_stress_drains_all_messages_via_reactor() {
    const N_MESSAGES: u32 = 500;
    const TIMEOUT: Duration = Duration::from_secs(30);

    let (reactor, writers) = reactor_with_rings(1);
    // Only id 1, a fresh reactor's first lease, is leased, and published last: the
    // rest are released unrouted, and the lease resolves only once the reactor has
    // drained every one. As fast as possible, so the drain-refresh-arm race is
    // under pressure.
    let child = || {
        for req_id in (2..=N_MESSAGES).chain([1]) {
            writers[0].send_status(req_id, WireStatus::Ok, &[]);
        }
    };
    let pid = unsafe { fork_child(child) };

    let lease = reactor.lease_acks("stress", WorkerSet::ALL);
    assert_eq!(lease.id(), 1);
    let timer = reactor.sleep(TIMEOUT);
    let out = reactor.block_on(async move { select2(lease.acks(), timer).await });
    if let Either::B(()) = out {
        unsafe { libc::kill(pid, libc::SIGKILL) };
        panic!("reactor stalled after {TIMEOUT:?} — lost-wake symptom");
    }
    assert!(matches!(out, Either::A(Ok(()))), "every ACK is OK");
    unsafe { crate::runtime::test_support::assert_child_exited_ok(pid) };
}

/// A publish on a ring past the first wakes a reactor parked on all of them: the
/// child answers on ring 0, waits until the reactor parks, then answers on ring 1.
#[test]
fn a_publish_on_a_later_ring_wakes_the_reactor() {
    use crate::runtime::w2m::fixtures::{master_parked, test_ring};
    use crate::runtime::w2m::W2mWriter;

    const TIMEOUT: Duration = Duration::from_secs(30);

    let r0 = test_ring(64 * 1024);
    let r1 = test_ring(64 * 1024);

    let child = || {
        W2mWriter::new(r0).send_status(1, WireStatus::Ok, &[]);
        let deadline = Instant::now() + TIMEOUT;
        while !unsafe { master_parked(r1) } {
            assert!(Instant::now() < deadline, "the reactor never parked on ring 1");
            std::thread::yield_now();
        }
        W2mWriter::new(r1).send_status(1, WireStatus::Ok, &[]);
    };
    let pid = unsafe { fork_child(child) };

    let r = make_reactor_over(W2mReceiver::new(vec![r0, r1]));
    // Request id 1 is a fresh reactor's first.
    let lease = r.lease_acks("later ring", WorkerSet::ALL);
    let timer = r.sleep(TIMEOUT);
    let out = r.block_on(async move { select2(lease.acks(), timer).await });
    if let Either::B(()) = out {
        unsafe { libc::kill(pid, libc::SIGKILL) };
        panic!("reactor stalled after {TIMEOUT:?}: ring 1's wake never reached it");
    }
    assert!(matches!(out, Either::A(Ok(()))), "both ACKs are OK");
    unsafe { crate::runtime::test_support::assert_child_exited_ok(pid) };
}
