//! The reactor's futures: timers, fsync, a train lease's frame stream, and the
//! park slot every op future waits in.

use std::time::Duration;

use super::super::test_support::*;
use super::*;
use crate::runtime::test_support::try_poll_once;

/// A timer fires no earlier than its deadline, and leaves no deadline behind.
/// No upper bound: "fired late" is a statement about the machine, not about the
/// reactor.
#[test]
fn a_timer_fires_after_its_deadline() {
    let r = make_reactor();
    let start = Instant::now();
    r.block_on(r.timer(start + Duration::from_millis(5)));
    assert!(start.elapsed() >= Duration::from_millis(5), "timer fired early");
    assert!(r.inner.deadlines.borrow().is_empty());
}

/// A timer in the past resolves on the very first poll instead of
/// hanging the reactor.
#[test]
fn timer_in_the_past_resolves_immediately() {
    let r = make_reactor();
    assert!(
        try_poll_once(r.timer(Instant::now() - Duration::from_secs(1))).is_some(),
        "a past deadline must resolve on the first poll"
    );
    assert!(r.inner.deadlines.borrow().is_empty(), "and register no deadline");
}

/// A dropped timer removes its deadline, so it neither wakes its waker nor
/// bounds a later sleep.
#[test]
fn a_dropped_timer_leaves_no_deadline() {
    let r = make_reactor();
    let waker = make_waker(999);
    let mut tf = Box::pin(r.timer(Instant::now() + Duration::from_millis(1)));
    assert!(tf.as_mut().poll(&mut Context::from_waker(&waker)).is_pending());
    assert_eq!(r.inner.deadlines.borrow().len(), 1, "the poll registers the deadline");
    drop(tf);
    assert!(r.inner.deadlines.borrow().is_empty(), "the drop removes it");

    std::thread::sleep(Duration::from_millis(2));
    r.tick(false);
    assert!(
        !r.inner.run_queue.borrow().is_queued(999),
        "a dropped timer must not wake its original waker"
    );
}

/// Submitting fdatasync on an fd that is not in the process's fd
/// table returns a negative rc (typically -EBADF) from the kernel.
///
/// Uses `i32::MAX` rather than `close(real_fd); submit(real_fd)` so
/// the test is race-free under the parallel test runner — a freshly
/// closed fd number can be reallocated by another thread before our
/// SQE reaches the kernel, masking the expected EBADF.
#[test]
fn fsync_real_bad_fd_returns_negative() {
    let r = make_reactor();
    let rc = r.block_on(r.fsync(i32::MAX));
    assert!(rc < 0, "fdatasync on a bogus fd must return rc<0, got {rc}");
}

/// `fsync` flushes the SQE to the kernel before returning.
/// Without the eager submit the CQE would only arrive on the next
/// `tick`, defeating the Phase-A / tick-evaluation overlap.
#[test]
fn fsync_submit_flushes_sqe_before_returning() {
    let r = make_reactor();
    let fd = unsafe { libc::memfd_create(c"reactor_fsync_flush".as_ptr(), libc::MFD_CLOEXEC) };
    let mut fut = std::pin::pin!(r.fsync(fd));

    // Spin briefly (driving no tick from outside) until the CQE arrives in the
    // ring or we time out. The kernel completes fdatasync on a memfd in
    // microseconds, so 100 ms is headroom for jitter, not a real bound.
    let deadline = Instant::now() + Duration::from_millis(100);
    let mut got: Option<i32> = None;
    while Instant::now() < deadline {
        r.drain_cqes_into_wakers();
        if let Poll::Ready(rc) = fut.as_mut().poll(&mut Context::from_waker(Waker::noop())) {
            got = Some(rc);
            break;
        }
    }
    unsafe {
        libc::close(fd);
    }
    assert_eq!(
        got,
        Some(0),
        "fsync CQE must be available without driving another tick — \
         Reactor::fsync should flush the SQE eagerly"
    );
}

/// A train route is a queue, not a slot: more frames than any worker count all
/// wait in it, and come out in arrival order.
#[test]
fn a_train_route_queues_past_worker_count_in_arrival_order() {
    const N: usize = 100; // > MAX_WORKERS
    let (r, writers) = reactor_with_rings(1);
    let lease = r.lease_train(1);
    for i in 0..N {
        let msg = crate::runtime::wire::WireMsg {
            request_id: 100 + i as u64,
            ..Default::default()
        };
        writers[0].send_msg(lease.id(0), &msg);
    }
    r.drain_all_w2m();

    for i in 0..N {
        let f = try_poll_once(lease.next_frame(0)).expect("a routed frame resolves on the first poll");
        let rid = gnitz_wire::control::peek_control_block(f.bytes(), false)
            .unwrap()
            .request_id;
        assert_eq!(rid, 100 + i as u64, "frame {i} delivered in arrival order");
    }
    assert!(try_poll_once(lease.next_frame(0)).is_none(), "the queue is drained");
}

/// `first_frames` hands out nothing until every id has a frame, so no frame is
/// taken by a poll that then parks.
#[test]
fn first_frames_waits_for_every_id_before_taking_any() {
    let (r, writers) = reactor_with_rings(2);
    let lease = r.lease_train(2);
    let mut fut = std::pin::pin!(lease.first_frames());
    let mut cx = Context::from_waker(Waker::noop());

    writers[0].send_msg(lease.id(0), &Default::default());
    r.drain_all_w2m();
    assert!(fut.as_mut().poll(&mut cx).is_pending());
    assert!(
        matches!(&r.inner.routes.borrow()[&(lease.id(0) as u32)], Route::Train(q) if q.len() == 1),
        "the pending poll left id 0's frame queued"
    );

    writers[1].send_msg(lease.id(1), &Default::default());
    r.drain_all_w2m();
    match fut.as_mut().poll(&mut cx) {
        Poll::Ready(slots) => assert_eq!(slots.len(), 2),
        Poll::Pending => panic!("both ids have a frame"),
    }
}

/// Dropping a train lease releases the frames it still holds, and a frame for
/// its id arriving afterwards is released at the ring.
#[test]
fn a_dropped_lease_releases_held_and_late_frames() {
    let (r, writers) = reactor_with_rings(1);
    let lease = r.lease_train(1);
    let id = lease.id(0);
    writers[0].send_msg(id, &Default::default());
    r.drain_all_w2m();

    let held = r.inner.w2m.release_cursor(0);
    drop(lease);
    assert!(r.inner.routes.borrow().is_empty());
    assert!(r.inner.w2m.release_cursor(0) > held, "the held frame is released");

    writers[0].send_msg(id, &Default::default());
    let late = r.inner.w2m.release_cursor(0);
    r.drain_all_w2m();
    assert!(r.inner.w2m.release_cursor(0) > late, "the late frame is released");
    assert!(r.inner.routes.borrow().is_empty(), "and routes nothing");
}

/// Dropping a lease mid-train releases its held and later frames, so a writer
/// blocked on the full ring finishes.
#[test]
fn a_dropped_lease_unblocks_a_streaming_writer() {
    use crate::runtime::w2m::fixtures::make_ring;
    use crate::runtime::w2m::W2mWriter;
    use crate::runtime::wire as ipc;

    const TOTAL: usize = 8;

    // Ring sized for exactly 2 small frames; declared first so it unmaps last.
    let region = unsafe { make_ring(ipc::WireMsg::default().size(), 2, 8) };
    let ptr = region.ptr();

    let r = make_reactor_over(Rc::new(W2mReceiver::new(vec![ptr])));
    let lease = r.lease_train(1);
    let id = lease.id(0);
    let writer = W2mWriter::new(ptr);

    let (done_tx, done_rx) = std::sync::mpsc::channel::<()>();
    let handle = std::thread::spawn(move || {
        for _ in 0..TOTAL {
            writer.send_msg(id, &ipc::WireMsg::default());
        }
        let _ = done_tx.send(());
    });

    let deadline = Instant::now() + Duration::from_secs(5);
    let queued = |r: &Reactor| match &r.inner.routes.borrow()[&(id as u32)] {
        Route::Train(q) => q.len(),
        Route::Ack { .. } => unreachable!(),
    };
    while queued(&r) < 2 {
        assert!(Instant::now() < deadline, "writer never produced the first 2 frames");
        r.drain_all_w2m();
        if queued(&r) < 2 {
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    // Drop the lease mid-train: held slots free, later frames are released.
    drop(lease);

    loop {
        r.drain_all_w2m();
        if done_rx.try_recv().is_ok() {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the lease drop failed to free the ring — writer wedged"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
    handle.join().expect("writer thread panicked");
    r.drain_all_w2m();

    assert_eq!(
        r.inner.w2m.release_cursor(0),
        r.inner.w2m.write_cursor(0),
        "every emitted slot must be freed (release_cursor reaches write_cursor)",
    );
}

/// The three ways a park slot ends. The slot is opened by hand so the
/// completion's timing relative to the drop is exact; `Reactor::fsync`'s own CQE
/// is not.
#[test]
fn a_park_slot_ends_by_completion_abandonment_or_reclaim() {
    let r = make_reactor();
    let mut cx = Context::from_waker(Waker::noop());
    let op_future = |id| Box::pin(OpFuture { id, inner: Rc::clone(&r.inner) });

    // 1. Dropped while pending: the slot is abandoned, so the late CQE retires
    //    it rather than parking a result nobody will collect.
    r.inner.ops.open(1, None);
    {
        let mut fut = op_future(1);
        assert!(fut.as_mut().poll(&mut cx).is_pending(), "no result yet");
        assert!(r.inner.ops.has_waker(1), "the poll registers a waker");
    }
    assert!(r.inner.ops.is_abandoned(1), "drop while pending must abandon");
    assert!(!r.inner.ops.has_waker(1), "and withdraw the waker");
    cqe(&r, KIND_OP, 1, 0);
    assert_eq!(r.inner.ops.len(), 0, "the late CQE must retire the slot");

    // 2. Completion beats the drop: `Drop` reclaims the orphaned result. This
    //    is the case a tombstone set grew one entry per durable commit for.
    r.inner.ops.open(2, None);
    let mut fut = op_future(2);
    assert!(fut.as_mut().poll(&mut cx).is_pending());
    cqe(&r, KIND_OP, 2, 0);
    drop(fut);
    assert_eq!(r.inner.ops.len(), 0, "drop must reclaim the orphaned result");

    // 3. Collected: `poll` retires the slot, leaving `Drop` nothing to do — and
    //    delivers the CQE `res` verbatim, which the caller's `rc < 0` fatal
    //    branch depends on.
    for rc in [0, -libc::EBADF] {
        r.inner.ops.open(3, None);
        let mut fut = op_future(3);
        cqe(&r, KIND_OP, 3, rc);
        assert!(matches!(fut.as_mut().poll(&mut cx), Poll::Ready((got, None)) if got == rc));
        drop(fut);
        assert_eq!(r.inner.ops.len(), 0, "a resolved slot is already retired");
    }
}
