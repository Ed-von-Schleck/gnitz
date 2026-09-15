//! Reactor state: W2M routing by ring id — leases and their id space, the
//! exchange queue, the release of an unrouted frame — and when a tick sleeps.

use std::time::Duration;

use super::test_support::*;
use super::*;
use crate::runtime::test_support::try_poll_once;
use gnitz_wire::WireStatus;

/// A lease routes `n` consecutive ids for exactly its own life, and the next
/// lease starts past them.
#[test]
fn a_lease_routes_consecutive_ids_until_dropped() {
    let r = make_reactor();
    let acks = r.lease_acks(4);
    let ids: Vec<u64> = (0..4).map(|i| acks.id(i)).collect();
    assert!(ids.windows(2).all(|p| p[1] == p[0] + 1), "consecutive: {ids:?}");
    assert!(ids.iter().all(|&id| r.inner.routes.borrow().contains_key(&(id as u32))));

    let train = r.lease_train(2);
    assert_eq!(train.id(0), ids[3] + 1, "the next lease starts past the last");
    assert_eq!(r.inner.routes.borrow().len(), 6);

    drop(acks);
    assert_eq!(r.inner.routes.borrow().len(), 2, "dropping a lease removes its routes");
    drop(train);
    assert!(r.inner.routes.borrow().is_empty());
}

/// The window a lease exists to close: an ACK drained after the id is leased
/// but before anything awaits it is kept for the awaiter.
#[test]
fn an_ack_landing_before_its_awaiter_is_kept() {
    let (r, writers) = reactor_with_rings(1);
    let lease = r.lease_acks(1);
    writers[0].send_status(0, lease.id(0), WireStatus::Ok, &[]);
    r.drain_all_w2m();

    assert!(
        matches!(try_poll_once(lease.acks(1, |_, _| Some(()))), Some(Err(()))),
        "an ACK drained before its awaiter must be there on the first poll"
    );
}

/// `acks` parks until every id has answered, then reports the fault by the ring
/// it arrived on: ring 1 answering the lease's first id is worker 1.
#[test]
fn acks_resolve_on_the_last_ack_and_name_the_faulting_ring() {
    let (r, writers) = reactor_with_rings(2);
    let lease = r.lease_acks(2);
    let mut fut = std::pin::pin!(lease.acks(2, |w, c| {
        (c.status != WireStatus::Ok).then(|| (w, String::from_utf8_lossy(&c.error_msg).into_owned()))
    }));
    let waker = make_waker(0);
    let mut cx = Context::from_waker(&waker);
    assert!(fut.as_mut().poll(&mut cx).is_pending());

    writers[1].send_status(0, lease.id(0), WireStatus::Error, b"boom");
    r.drain_all_w2m();
    assert!(
        fut.as_mut().poll(&mut cx).is_pending(),
        "one of two ACKs must not resolve"
    );

    writers[0].send_status(0, lease.id(1), WireStatus::Ok, &[]);
    r.drain_all_w2m();
    assert!(
        r.inner.run_queue.borrow().is_queued(0),
        "the last ACK wakes the awaiter"
    );
    match fut.as_mut().poll(&mut cx) {
        Poll::Ready(Err((w, msg))) => assert_eq!((w, msg.as_str()), (1, "boom")),
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
    assert!(r.inner.routes.borrow().is_empty(), "and no route is created");
}

/// Exchange frames ride the reserved ring id: each is queued for the round driver
/// tagged with the ring it came from, and none touches a leased ACK.
#[test]
fn exchange_frames_queue_by_ring_id_tagged_with_their_worker() {
    let (r, writers) = reactor_with_rings(2);
    let lease = r.lease_acks(2);
    let mut acks = std::pin::pin!(lease.acks(2, |_, _| None::<()>));
    let mut cx = Context::from_waker(Waker::noop());
    assert!(acks.as_mut().poll(&mut cx).is_pending());

    let msg = crate::runtime::wire::WireMsg { target_id: 99, ..Default::default() };
    for w in writers.iter().rev() {
        w.send_msg(W2M_EXCHANGE_RING_ID as u64, &msg);
    }
    r.drain_all_w2m();

    let mut q = r.inner.exchanges.borrow_mut();
    let (w0, f0) = q.pop().expect("worker 0's exchange frame must be queued");
    let (w1, f1) = q.pop().expect("worker 1's exchange frame must be queued");
    drop(q);
    assert_eq!((w0, w1), (0, 1), "each frame carries the worker that sent it");
    assert_eq!((f0.control.target_id, f1.control.target_id), (99, 99));
    assert!(acks.as_mut().poll(&mut cx).is_pending(), "no ACK was delivered");
}

/// `acks` holds a waker on the first unanswered id alone: an ACK landing behind
/// it wakes nothing, and the one that fills the gap moves the park past every
/// id already answered.
#[test]
fn acks_parks_only_on_the_first_unanswered_id() {
    let (r, writers) = reactor_with_rings(1);
    let lease = r.lease_acks(3);
    let mut fut = std::pin::pin!(lease.acks(3, |_, _| None::<()>));
    let waker = make_waker(7);
    let mut cx = Context::from_waker(&waker);
    let parked = |i: usize| match &r.inner.routes.borrow()[&(lease.id(i) as u32)] {
        Route::Ack { waker, .. } => waker.is_some(),
        Route::Train(_) => unreachable!(),
    };

    assert!(fut.as_mut().poll(&mut cx).is_pending());
    assert_eq!((parked(0), parked(1), parked(2)), (true, false, false));

    writers[0].send_status(0, lease.id(1), WireStatus::Ok, &[]);
    r.drain_all_w2m();
    assert!(
        !r.inner.run_queue.borrow().is_queued(7),
        "an ACK behind the first gap wakes nothing"
    );

    writers[0].send_status(0, lease.id(0), WireStatus::Ok, &[]);
    r.drain_all_w2m();
    assert!(
        r.inner.run_queue.borrow().is_queued(7),
        "filling the gap wakes the awaiter"
    );
    assert!(fut.as_mut().poll(&mut cx).is_pending());
    assert!(parked(2), "the park moves past every answered id");

    writers[0].send_status(0, lease.id(2), WireStatus::Ok, &[]);
    r.drain_all_w2m();
    assert!(matches!(fut.as_mut().poll(&mut cx), Poll::Ready(Ok(()))));
}

/// `trains_idle` stays pending while any train lease lives — an ACK lease is not
/// one — and every lease drop wakes it to look again.
#[test]
fn trains_idle_waits_for_the_last_train_lease() {
    let r = make_reactor();
    let (first, second, acks) = (r.lease_train(1), r.lease_train(2), r.lease_acks(1));
    let mut idle = std::pin::pin!(r.trains_idle());
    let waker = make_waker(9);
    let mut cx = Context::from_waker(&waker);
    assert!(idle.as_mut().poll(&mut cx).is_pending());

    drop(acks);
    assert!(r.inner.run_queue.borrow().is_queued(9), "a lease drop wakes the waiter");
    assert!(idle.as_mut().poll(&mut cx).is_pending(), "two trains are still live");

    drop(first);
    assert!(idle.as_mut().poll(&mut cx).is_pending(), "one train is still live");

    drop(second);
    assert!(idle.as_mut().poll(&mut cx).is_ready(), "no train lease is live");
}

/// Lease ids wrap to 1 before they could reach the exchange id, and a lease
/// never takes an id a live lease still routes.
#[test]
fn lease_ids_wrap_below_the_exchange_id_and_skip_live_ids() {
    let r = make_reactor();
    r.inner.next_request_id.set(u32::MAX - 2);
    let top = r.lease_acks(2);
    assert_eq!((top.id(0), top.id(1)), (u32::MAX as u64 - 2, u32::MAX as u64 - 1));

    let wrapped = r.lease_acks(2);
    assert_eq!(
        (wrapped.id(0), wrapped.id(1)),
        (1, 2),
        "the exchange id is never handed out"
    );

    r.inner.next_request_id.set(2);
    let skipped = r.lease_train(2);
    assert_eq!((skipped.id(0), skipped.id(1)), (3, 4), "a live lease's ids are skipped");

    r.inner.next_request_id.set(u32::MAX - 1);
    let straddling = r.lease_acks(3);
    assert_eq!(straddling.id(0), 5, "a lease that would reach the exchange id wraps");
}

/// The drain the arm runs first can route a frame that wakes a task; that tick
/// must then run on instead of arming and sleeping.
#[test]
fn a_tick_whose_arm_drain_wakes_a_task_does_not_arm() {
    within(Duration::from_secs(30), || {
        let (r, mut writers) = reactor_with_rings(1);
        let writer = writers.pop().expect("one ring");
        let lease = r.lease_acks(1);
        let id = lease.id(0);
        let done = Rc::new(Cell::new(false));
        let d = Rc::clone(&done);
        r.spawn(async move {
            // Published during the poll, after this tick's own drain.
            writer.send_status(0, id, WireStatus::Ok, &[]);
            lease.acks(1, |_, _| None::<()>).await.expect("an OK ACK");
            d.set(true);
        });
        r.tick(true);
        assert!(!r.inner.futex_waitv_armed.get(), "the woken tick must not arm");
        assert!(!r.inner.run_queue.borrow().is_empty(), "the drain woke the task");
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

    let pid = unsafe { libc::fork() };
    assert!(pid >= 0, "fork failed");
    if pid == 0 {
        // Child: publish a monotonic req_id stream as fast as possible. The
        // parent's wake protocol must not drop any of them under the resulting
        // drain-refresh-arm race pressure.
        let writer = W2mWriter::new(ptr);
        for req_id in 1..=N_MESSAGES {
            writer.send_status(0, req_id, WireStatus::Ok, &[]);
        }
        unsafe { libc::_exit(0) };
    }

    let reactor = make_reactor_over(Rc::new(W2mReceiver::new(vec![ptr])));
    // A fresh reactor's first lease is 1..=N, which is what the child publishes.
    let lease = Rc::new(reactor.lease_acks(N_MESSAGES as usize));
    let received: Rc<RefCell<Vec<u64>>> = Rc::new(RefCell::new(Vec::new()));
    // A oneshot rather than a polling watcher: a watcher that yields re-arms
    // its own waker every poll, so the run queue never empties and the
    // `FUTEX_WAITV` arm this test exists to stress never happens.
    let (done_tx, done_rx) = oneshot::channel::<()>();
    {
        let (lease, received) = (Rc::clone(&lease), Rc::clone(&received));
        reactor.spawn(async move {
            let mut got = Vec::new();
            let _ = lease
                .acks(N_MESSAGES as usize, |_, c| {
                    got.push(c.request_id);
                    None::<()>
                })
                .await;
            *received.borrow_mut() = got;
            done_tx.send(());
        });
    }

    let timeout = reactor.timer(Instant::now() + TIMEOUT);
    let outcome = reactor.block_on(select2(done_rx, timeout));
    if let Either::B(()) = outcome {
        unsafe { libc::kill(pid, libc::SIGKILL) };
        panic!("reactor stalled after {TIMEOUT:?} — lost-wake symptom");
    }

    assert_eq!(
        *received.borrow(),
        (1..=N_MESSAGES).collect::<Vec<u64>>(),
        "every published req_id must round-trip"
    );

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
    W2mWriter::new(ptr).send_status(0, 1u64, WireStatus::Ok, &[]);

    let receiver = W2mReceiver::new(vec![ptr]);
    let mut out = [FutexWaitV::new(); 1];
    assert!(
        receiver.arm_waitv(&mut out).is_none(),
        "an unread publish must refuse the arm — arming it would be a lost wake",
    );
}
