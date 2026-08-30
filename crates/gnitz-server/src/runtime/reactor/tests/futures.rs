//! The reactor's futures: timers, fsync, the scan-frame stream and its lease,
//! and the drop hygiene each one owes its park slot or queue.

use std::cell::Cell as StdCell;
use std::time::Duration;

use super::super::test_support::*;
use super::super::uring::Cqe;
use super::*;

/// Dispatch a synthetic CQE tagged with `kind` and `id`, carrying `rc`.
fn cqe(r: &Reactor, kind: u64, id: u64, rc: i32) {
    r.dispatch_cqe(Cqe {
        user_data: udata(kind, id),
        res: rc,
        flags: 0,
    });
}

/// Timer fires after a short deadline.
#[test]
fn timer_fires() {
    let r = make_reactor();
    let inner = Rc::clone(&r.inner);
    let start = Instant::now();
    r.block_on(async move {
        TimerFuture::new(Instant::now() + Duration::from_millis(50), inner).await;
    });
    let elapsed = start.elapsed();
    assert!(
        elapsed >= Duration::from_millis(50),
        "timer fired too early: {elapsed:?}"
    );
    assert!(
        elapsed < Duration::from_millis(500),
        "timer fired too late: {elapsed:?}"
    );
}

/// Earlier timer must resolve before a later timer.
#[test]
fn timer_ordering() {
    let r = make_reactor();
    let order: Rc<RefCell<Vec<u32>>> = Rc::new(RefCell::new(Vec::new()));

    let inner1 = Rc::clone(&r.inner);
    let order1 = Rc::clone(&order);
    let inner2 = Rc::clone(&r.inner);
    let order2 = Rc::clone(&order);

    r.spawn(async move {
        TimerFuture::new(Instant::now() + Duration::from_millis(100), inner2).await;
        order2.borrow_mut().push(2);
    });

    r.block_on(async move {
        TimerFuture::new(Instant::now() + Duration::from_millis(20), inner1).await;
        order1.borrow_mut().push(1);
    });

    // The block_on completes when its root task does, but the spawned
    // 100ms timer may still be pending. Drive a few more ticks to let
    // it complete.
    let deadline = Instant::now() + Duration::from_millis(500);
    while r.has_pending_tasks() && Instant::now() < deadline {
        r.tick(true);
    }
    assert_eq!(order.borrow().as_slice(), &[1, 2]);
}

/// A timer in the past resolves on the very first poll instead of
/// hanging the reactor.
#[test]
fn timer_in_the_past_resolves_immediately() {
    let r = make_reactor();
    let inner = Rc::clone(&r.inner);
    let start = Instant::now();
    r.block_on(async move {
        TimerFuture::new(Instant::now() - Duration::from_secs(1), inner).await;
    });
    assert!(
        start.elapsed() < Duration::from_millis(100),
        "past-deadline timer must not block"
    );
}

#[test]
fn fsync_future_roundtrip() {
    let r = make_reactor();
    let fd = unsafe { libc::memfd_create(c"reactor_fsync_future".as_ptr(), libc::MFD_CLOEXEC) };
    let rc: Rc<StdCell<i32>> = Rc::new(StdCell::new(1));
    let rc2 = Rc::clone(&rc);
    let fsync = r.fsync(fd);
    r.block_on(async move {
        rc2.set(fsync.await);
    });
    unsafe {
        libc::close(fd);
    }
    assert_eq!(rc.get(), 0);
}

/// Dispatching a KIND_FSYNC CQE parks the CQE `res` verbatim under
/// the request id — both success (rc=0) and failure (rc<0), since
/// the caller's `rc < 0` fatal-abort branch depends on it.
#[test]
fn fsync_dispatch_parks_rc_verbatim() {
    let r = make_reactor();
    for rc in [0, -5] {
        r.inner.fsyncs.open(42, None);
        cqe(&r, KIND_FSYNC, 42, rc);
        assert_eq!(r.inner.fsyncs.take_result(42), Some(rc));
    }
}

/// End-to-end with a real io_uring: submit fdatasync on a memfd,
/// block until complete, expect rc=0.  Also asserts
/// the fsync park map is drained afterwards (catches leaks).
#[test]
fn fsync_real_memfd_roundtrip() {
    let r = make_reactor();
    let fd = unsafe { libc::memfd_create(c"reactor_fsync_ok".as_ptr(), libc::MFD_CLOEXEC) };
    let rc = r.block_on(r.fsync(fd));
    unsafe {
        libc::close(fd);
    }
    assert_eq!(rc, 0, "fdatasync on a fresh memfd should succeed");
    assert_eq!(r.inner.fsyncs.len(), 0, "a resolved fsync must retire its slot");
}

/// Submitting fdatasync on an fd that is not in the process's fd
/// table returns a negative rc (typically -EBADF) from the kernel.
/// Direct replacement for the deleted fork-based ipc test.
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
    let fut = r.fsync(fd);
    let id = fut.id;

    // Spin briefly (no further ticks driven from outside) until the
    // CQE either arrives in the ring or we time out.  The kernel
    // completes fdatasync on a memfd in microseconds, so ~100 ms
    // gives generous headroom for scheduler jitter without flakiness.
    let deadline = Instant::now() + Duration::from_millis(100);
    let mut got: Option<i32> = None;
    while Instant::now() < deadline {
        r.drain_cqes_into_wakers();
        if let Some(rc) = r.inner.fsyncs.take_result(id) {
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

/// `select2` returns whichever future completes first; the loser is
/// dropped. With a pre-resolved future and an unresolved one, the
/// pre-resolved must win.
#[test]
fn select2_pre_resolved_wins() {
    let r = make_reactor();
    let inner = Rc::clone(&r.inner);
    let v: Rc<StdCell<u32>> = Rc::new(StdCell::new(0));
    let v2 = Rc::clone(&v);
    r.block_on(async move {
        let ready = async { 7u32 };
        let never = TimerFuture::new(Instant::now() + Duration::from_secs(60), inner);
        match select2(ready, never).await {
            Either::A(x) => v2.set(x),
            Either::B(()) => panic!("timer should not have fired"),
        }
    });
    assert_eq!(v.get(), 7);
}

/// A dropped TimerFuture must set the cancellation flag so the
/// KIND_TIMEOUT CQE handler discards the wake. Verified by registering
/// a timer, dropping the future, then running ticks and asserting the
/// CQE is consumed without firing the waker (key 999 must not appear
/// in the run queue).
#[test]
fn timer_future_drop_cancels_heap_entry() {
    let r = make_reactor();
    let inner = Rc::clone(&r.inner);
    let waker = make_waker(999);
    // Build a TimerFuture, poll once to register, then drop it.
    let mut tf = TimerFuture::new(Instant::now() + Duration::from_millis(10), inner);
    let mut cx = Context::from_waker(&waker);
    let pinned = Pin::new(&mut tf);
    let _ = pinned.poll(&mut cx);
    assert_eq!(r.inner.timers.len(), 1);
    drop(tf);
    // Drive ticks past the deadline; the cancelled CQE must be
    // discarded without waking key=999.
    std::thread::sleep(Duration::from_millis(20));
    for _ in 0..4 {
        r.tick(false);
    }
    assert!(
        !r.inner.run_queue.borrow().is_queued(999),
        "cancelled timer must not wake its original waker"
    );
    assert_eq!(
        r.inner.spec_pool.borrow().len(),
        1,
        "the cancelled timer's Timespec must come back to the pool"
    );
}

/// The happy path returns its `Timespec` to the pool too — the carry is
/// retired by the CQE either way, which is what keeps the per-egress-frame
/// timer allocation-free after the first one.
#[test]
fn resolved_timer_returns_its_timespec_to_the_pool() {
    let r = make_reactor();
    let inner = Rc::clone(&r.inner);
    assert_eq!(r.inner.spec_pool.borrow().len(), 0);
    r.block_on(async move {
        TimerFuture::new(Instant::now() + Duration::from_millis(5), inner).await;
    });
    assert_eq!(r.inner.spec_pool.borrow().len(), 1, "the box must be recycled");

    let inner = Rc::clone(&r.inner);
    r.block_on(async move {
        TimerFuture::new(Instant::now() + Duration::from_millis(5), inner).await;
    });
    assert_eq!(r.inner.spec_pool.borrow().len(), 1, "and reused, not re-allocated");
}

#[test]
fn route_scan_slot_parks_when_no_waker_registered() {
    // Slot arrives before any future is polled, but the scan IS active (a
    // ScanLease is held): the frame must be queued, not dropped or
    // overwritten.
    let r = make_reactor();
    let req_id = r.alloc_scan_request_id() as u32;
    let (slot, _recv, _region) = unsafe { make_scan_slot(req_id) };

    let _lease = r.scan_lease(&[req_id]);
    r.route_scan_slot(slot);

    assert!(
        r.inner.scans.borrow().contains_key(&req_id),
        "slot must be queued when no waker is registered for an active scan"
    );
    assert_eq!(
        r.inner.scans.borrow().get(&req_id).map(|s| s.len()),
        Some(1),
        "exactly one frame queued"
    );
    assert!(
        r.inner.scans.borrow().values().all(|s| !s.has_waiter()),
        "no waker should have been inserted"
    );

    // Drain the queue here so the slot drops (advancing release_cursor) before
    // the ring region unmaps.
    while r.inner.scans.borrow_mut().get_mut(&req_id).unwrap().pop().is_some() {}
}

#[test]
fn await_scan_slot_resolves_immediately_when_slot_preloaded() {
    // The bug scenario: slot arrives and is parked BEFORE the future is
    // first polled. The first poll must return Poll::Ready, not Poll::Pending.
    let r = make_reactor();
    let req_id = r.alloc_scan_request_id() as u32;
    let (slot, _recv, _region) = unsafe { make_scan_slot(req_id) };

    let _lease = r.scan_lease(&[req_id]);
    r.route_scan_slot(slot); // park before any poll

    let fut = r.await_scan_slot(req_id);
    let result = r.block_on(fut);
    assert_eq!(
        result.internal_req_id, req_id,
        "first poll must return the pre-parked slot"
    );

    drop(result); // advance release_cursor before the region unmaps
}

#[test]
fn await_scan_slot_resolves_after_route_fires_waker() {
    // Normal path: future polled first (registers waker), then slot arrives.
    use std::cell::Cell;
    let r = make_reactor();
    let req_id = r.alloc_scan_request_id() as u32;
    let (slot, _recv, _region) = unsafe { make_scan_slot(req_id) };

    let _lease = r.scan_lease(&[req_id]);
    let delivered: Rc<Cell<bool>> = Rc::new(Cell::new(false));
    let delivered2 = Rc::clone(&delivered);
    let fut = r.await_scan_slot(req_id);
    r.spawn(async move {
        let s = fut.await;
        assert_eq!(s.internal_req_id, req_id);
        delivered2.set(true);
    });

    r.tick(false); // poll task → Poll::Pending, waker registered
    assert!(
        r.inner.scans.borrow()[&req_id].has_waiter(),
        "waker must be registered after first poll"
    );
    assert!(!delivered.get(), "must not be delivered yet");

    r.route_scan_slot(slot); // park + wake
    r.tick(false); // task woken → Poll::Ready

    assert!(delivered.get(), "slot must be delivered after route_scan_slot");
}

/// Failure mode 4: concurrently-streamed continuation frames for one
/// req_id must be queued in arrival order, not overwritten (the old
/// a single-value slot dropped all but the last).
#[test]
fn scan_queue_retains_continuation_frames_in_order() {
    let r = make_reactor();
    let req_id = r.alloc_scan_request_id() as u32;
    let (recv, _region) = unsafe { make_scan_ring(req_id, 2) };
    let _lease = r.scan_lease(&[req_id]);

    // Route two frames for the same req_id while no awaiter is registered.
    let s0 = recv.try_read_slot(0).expect("frame 0");
    let s1 = recv.try_read_slot(0).expect("frame 1");
    r.route_scan_slot(s0);
    r.route_scan_slot(s1);
    assert_eq!(
        r.inner.scans.borrow().get(&req_id).map(|s| s.len()),
        Some(2),
        "both frames must be queued, not overwritten"
    );

    // await twice → frames returned in arrival order (100 then 101).
    let f0 = r.block_on(r.await_scan_slot(req_id));
    let rid0 = gnitz_wire::control::peek_control_block_ipc(f0.bytes())
        .unwrap()
        .request_id;
    drop(f0);
    let f1 = r.block_on(r.await_scan_slot(req_id));
    let rid1 = gnitz_wire::control::peek_control_block_ipc(f1.bytes())
        .unwrap()
        .request_id;
    drop(f1);
    assert_eq!(
        (rid0, rid1),
        (100, 101),
        "queued continuation frames must be returned in arrival order"
    );
    assert!(
        r.inner.scans.borrow().values().all(|s| s.len() == 0),
        "queue emptied after both frames consumed"
    );

    drop(_lease);
    drop(recv);
}

/// The per-req_id frame queue has no fixed ceiling: a worker streaming far
/// more continuation frames than there are workers must have every one of them
/// queued and delivered in order. Ring capacity, not a slot count, is the
/// bound.
#[test]
fn scan_queue_grows_past_worker_count() {
    let r = make_reactor();
    let req_id = r.alloc_scan_request_id() as u32;
    const N: usize = 100; // > 64
    let (recv, _region) = unsafe { make_scan_ring(req_id, N) };
    let _lease = r.scan_lease(&[req_id]);

    for _ in 0..N {
        let s = recv.try_read_slot(0).expect("frame");
        r.route_scan_slot(s);
    }
    assert_eq!(
        r.inner.scans.borrow().get(&req_id).map(|s| s.len()),
        Some(N),
        "all {N} frames must be queued — none dropped or overwritten"
    );

    for i in 0..N {
        let f = r.block_on(r.await_scan_slot(req_id));
        let rid = gnitz_wire::control::peek_control_block_ipc(f.bytes())
            .unwrap()
            .request_id;
        assert_eq!(rid, 100 + i as u64, "frame {i} delivered in arrival order");
        drop(f);
    }
    assert!(
        r.inner.scans.borrow().values().all(|s| s.len() == 0),
        "queue emptied after drain"
    );

    drop(_lease);
    drop(recv);
}

/// Dropping the `ScanLease` purges the parked queue (dropping each queued
/// `W2mSlot` advances `release_cursor`) and deregisters the active scan.
#[test]
fn scan_lease_drop_frees_queued_slots() {
    let r = make_reactor();
    let req_id = r.alloc_scan_request_id() as u32;
    let (recv, _region) = unsafe { make_scan_ring(req_id, 1) };
    let lease = r.scan_lease(&[req_id]);

    let s0 = recv.try_read_slot(0).expect("frame 0");
    r.route_scan_slot(s0); // queued (active)
    assert!(r.inner.scans.borrow().contains_key(&req_id));

    let cc_before = recv.release_cursor(0);
    drop(lease); // purge parked queue → drop queued slot → advance release_cursor
    assert!(
        r.inner.scans.borrow().values().all(|s| s.len() == 0),
        "lease drop purges parked queue"
    );
    assert!(
        r.inner.scans.borrow().values().all(|s| !s.has_waiter()),
        "lease drop purges wakers"
    );
    assert!(r.inner.scans.borrow().is_empty(), "lease drop deregisters active scan");
    let cc_after = recv.release_cursor(0);
    assert!(cc_after > cc_before, "dropped queued slot must advance release_cursor");

    drop(recv);
}

/// Failure mode 1: a frame whose scan has no live lease is discarded
/// (freeing ring space), not parked — so the still-streaming worker never
/// wedges on a full ring.
#[test]
fn abandoned_scan_frame_is_discarded_not_parked() {
    let r = make_reactor();
    let req_id = r.alloc_scan_request_id() as u32;
    let (recv, _region) = unsafe { make_scan_ring(req_id, 1) };

    // No lease held: route_scan_slot must drop the slot, not park it.
    let s0 = recv.try_read_slot(0).expect("frame 0");
    let cc_before = recv.release_cursor(0);
    r.route_scan_slot(s0); // dropped here (inactive)
    assert!(
        r.inner.scans.borrow().values().all(|s| s.len() == 0),
        "abandoned-scan frame must be discarded, not parked"
    );
    let cc_after = recv.release_cursor(0);
    assert!(
        cc_after > cc_before,
        "discarded slot must advance release_cursor (ring freed)"
    );

    drop(recv);
}

/// End-to-end Failure mode 1 guard: a worker streams more frames than the
/// ring holds; the master takes a lease, queues 2 (filling the ring and
/// parking the writer), then drops the lease mid-train. The freed slots +
/// the gate discarding every later frame must let the writer finish all its
/// writes instead of wedging in `W2mWriter::send_msg`.
#[test]
fn dropped_scan_lease_unblocks_streaming_writer() {
    use crate::runtime::w2m::make_ring;
    use crate::runtime::w2m::{W2mReceiver, W2mWriter};
    use crate::runtime::wire as ipc;
    use std::time::{Duration, Instant};

    const TOTAL: usize = 8;

    // Ring sized for exactly 2 small frames.
    let region = unsafe { make_ring(ipc::WireMsg::default().size(), 2, 8) };
    let ptr = region.ptr();

    let r = make_reactor();
    let req_id = r.alloc_scan_request_id() as u32;
    let writer = W2mWriter::new(ptr);
    let receiver = W2mReceiver::new(vec![ptr]);

    let (done_tx, done_rx) = std::sync::mpsc::channel::<()>();
    let handle = std::thread::spawn(move || {
        for _ in 0..TOTAL {
            let msg = ipc::WireMsg::default();
            writer.send_msg(req_id as u64, &msg);
        }
        let _ = done_tx.send(());
    });

    let lease = r.scan_lease(&[req_id]);

    // Read+queue 2 frames (active) — fills the ring, parking the writer.
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut queued = 0;
    while queued < 2 {
        if let Some(slot) = receiver.try_read_slot(0) {
            r.route_scan_slot(slot);
            queued += 1;
        } else if Instant::now() > deadline {
            panic!("writer never produced the first 2 frames");
        } else {
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    // Drop the lease mid-train: queued slots free, later frames are gated.
    drop(lease);

    let mut read = queued;
    while read < TOTAL {
        if let Some(slot) = receiver.try_read_slot(0) {
            r.route_scan_slot(slot); // discarded (lease gone)
            read += 1;
        } else if Instant::now() > deadline {
            panic!("scan lease drop failed to free the ring — writer wedged at {read}/{TOTAL}");
        } else {
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    done_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("writer thread must finish — never wedge on a full ring");
    handle.join().expect("writer thread panicked");

    assert_eq!(
        receiver.release_cursor(0),
        receiver.write_cursor(0),
        "every emitted slot must be freed (release_cursor reaches write_cursor)",
    );

    drop(receiver);
}

// ─────────────────────────────────────────────────────────────────
// Cancellation. Abandoning a park slot is the one mechanism behind
// all of these: the slot's presence is the op's liveness, so a late
// completion can always tell "deliver" from "the awaiter is gone".
// ─────────────────────────────────────────────────────────────────

/// A dropped `ReplyFuture` withdraws nothing: the slot belongs to the
/// [`ReplyLease`], so a reply landing after a `select2` loser is dropped is
/// still parked and collected by the next `await_reply` for that id. Dropping
/// the lease is what discards it.
#[test]
fn a_dropped_reply_future_leaves_the_leases_slot_open() {
    let r = make_reactor();
    let lease = r.alloc_replies(1);
    let req_id = lease[0];
    {
        let mut fut = Box::pin(r.await_reply(req_id));
        let w = make_waker(1);
        let mut cx = Context::from_waker(&w);
        assert!(fut.as_mut().poll(&mut cx).is_pending(), "no reply parked yet");
        assert!(r.inner.replies.has_waker(req_id), "poll must register the waker");
    } // fut dropped
    assert!(r.inner.replies.is_open(req_id), "the lease still owns the slot");

    r.route_reply(0, req_id as u32, synthetic_decoded_wire(req_id));
    let mut fut = Box::pin(r.await_reply(req_id));
    let w = make_waker(1);
    assert!(
        fut.as_mut().poll(&mut Context::from_waker(&w)).is_ready(),
        "the reply must still be there for the next awaiter"
    );

    drop(lease);
    assert_eq!(r.inner.replies.len(), 0, "the lease closes every slot it opened");
}

/// Waker hygiene across every `WakeQueue` user: a dropped awaiter must leave
/// no waker behind, or the next push wakes a task that is no longer listening.
#[test]
fn a_dropped_awaiter_leaves_no_waker_in_any_wake_queue() {
    let r = make_reactor();
    let w = make_waker(1);
    let mut cx = Context::from_waker(&w);

    {
        let mut fut = Box::pin(r.accept());
        assert!(fut.as_mut().poll(&mut cx).is_pending());
        assert!(r.inner.accepts.borrow().has_waiter(), "poll registers a waiter");
    }
    assert!(!r.inner.accepts.borrow().has_waiter(), "accept queue");

    {
        let mut fut = Box::pin(r.next_exchange());
        assert!(fut.as_mut().poll(&mut cx).is_pending());
        assert!(r.inner.exchanges.borrow().has_waiter(), "poll registers a waiter");
    }
    assert!(!r.inner.exchanges.borrow().has_waiter(), "exchange queue");

    let req_id = r.alloc_scan_request_id() as u32;
    let _lease = r.scan_lease(&[req_id]);
    {
        let mut fut = Box::pin(r.await_scan_slot(req_id));
        assert!(fut.as_mut().poll(&mut cx).is_pending());
        assert!(r.inner.scans.borrow()[&req_id].has_waiter(), "poll registers a waiter");
    }
    assert!(!r.inner.scans.borrow()[&req_id].has_waiter(), "scan route");

    let (read_end, write_end) = unsafe { pipe_pair() };
    r.register_conn(read_end);
    assert!(poll_recv_once(&r, read_end).is_none());
    assert!(
        !r.inner.conns.borrow()[&read_end].q.has_waiter(),
        "recv queue — the future poll_recv_once built is already dropped"
    );
    r.close_fd(read_end);
    unsafe { libc::close(write_end) };
}

/// A `FsyncFuture` dropped while pending leaves its slot abandoned, so the
/// late KIND_FSYNC retires it instead of leaking a result nobody collects.
#[test]
fn dropped_fsync_future_discards_a_late_cqe() {
    let r = make_reactor();
    let id = 7001u64;
    r.inner.fsyncs.open(id, None);
    {
        let mut fut = Box::pin(FsyncFuture {
            id,
            inner: Rc::clone(&r.inner),
        });
        let w = make_waker(1);
        let mut cx = Context::from_waker(&w);
        assert!(fut.as_mut().poll(&mut cx).is_pending(), "no result yet");
        assert!(r.inner.fsyncs.has_waker(id));
    } // drop while pending → abandoned
    assert!(r.inner.fsyncs.is_abandoned(id), "drop while pending must abandon");
    assert!(!r.inner.fsyncs.has_waker(id), "drop must withdraw the waker");

    cqe(&r, KIND_FSYNC, id, 0);
    assert_eq!(r.inner.fsyncs.len(), 0, "the late CQE must retire the slot");
}

/// A CQE arriving before the drop is reclaimed by `Drop`, leaving nothing
/// behind — the case that made a hand-rolled tombstone set grow one entry
/// per durable commit.
#[test]
fn dropped_fsync_future_reclaims_an_already_parked_result() {
    let r = make_reactor();
    let id = 7002u64;
    r.inner.fsyncs.open(id, None);
    let mut fut = Box::pin(FsyncFuture {
        id,
        inner: Rc::clone(&r.inner),
    });
    let w = make_waker(1);
    let mut cx = Context::from_waker(&w);
    assert!(fut.as_mut().poll(&mut cx).is_pending());

    cqe(&r, KIND_FSYNC, id, 0); // result parked before the drop

    drop(fut);
    assert_eq!(r.inner.fsyncs.len(), 0, "drop must reclaim the orphaned result");
}

/// The success path leaves nothing behind either: `poll` retires the slot,
/// so `Drop` finds nothing to abandon.
#[test]
fn resolved_fsync_future_leaves_no_slot() {
    let r = make_reactor();
    let id = 7003u64;
    r.inner.fsyncs.open(id, None);
    let mut fut = Box::pin(FsyncFuture {
        id,
        inner: Rc::clone(&r.inner),
    });
    let w = make_waker(1);
    let mut cx = Context::from_waker(&w);

    cqe(&r, KIND_FSYNC, id, 0); // park result, then resolve from it
    assert_eq!(fut.as_mut().poll(&mut cx), Poll::Ready(0));
    drop(fut);
    assert_eq!(r.inner.fsyncs.len(), 0);
}
