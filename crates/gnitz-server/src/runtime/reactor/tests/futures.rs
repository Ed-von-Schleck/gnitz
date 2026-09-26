//! The reactor's futures: timers, fsync, a train lease's frame stream, and the
//! `ops` entry every one-CQE op waits on.

use std::os::fd::{AsFd, FromRawFd, OwnedFd};
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
    assert!(!is_queued(999), "a dropped timer must not wake its original waker");
}

/// `fd`, open for the rest of the process, as `Reactor::fsync` requires.
fn leaked(fd: OwnedFd) -> BorrowedFd<'static> {
    let fd: &'static OwnedFd = Box::leak(Box::new(fd));
    fd.as_fd()
}

/// A failed fdatasync resolves to the kernel's negative errno: a socket, which
/// fdatasync refuses with `EINVAL`.
#[test]
fn fsync_error_returns_negative() {
    let r = make_reactor();
    let (sock, _) = std::os::unix::net::UnixStream::pair().expect("socketpair");
    let rc = r.block_on(r.fsync(leaked(OwnedFd::from(sock))));
    assert_eq!(rc, -libc::EINVAL, "fdatasync on a socket must fail with EINVAL");
}

/// `fsync` flushes the SQE to the kernel before returning.
/// Without the eager submit the CQE would only arrive on the next
/// `tick`, defeating the Phase-A / tick-evaluation overlap.
#[test]
fn fsync_submit_flushes_sqe_before_returning() {
    let r = make_reactor();
    let fd = unsafe { libc::memfd_create(c"reactor_fsync_flush".as_ptr(), libc::MFD_CLOEXEC) };
    assert!(fd >= 0, "memfd_create failed: {}", std::io::Error::last_os_error());
    // SAFETY: a fresh fd, owned by nothing else.
    let mut fut = std::pin::pin!(r.fsync(leaked(unsafe { OwnedFd::from_raw_fd(fd) })));

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
    let lease = r.lease_train(WorkerSet::ALL, SalMessageKind::Scan);
    for i in 0..N {
        let msg = crate::runtime::wire::WireMsg {
            target_id: 100 + i as u64,
            ..Default::default()
        };
        writers[0].send_msg(lease.id(), &msg);
    }
    r.drain_all_w2m();

    for i in 0..N {
        let f = try_poll_once(lease.next_slot(0)).expect("a routed frame resolves on the first poll");
        let tag = gnitz_wire::control::peek_control_block(f.bytes())
            .unwrap()
            .hdr
            .target_id;
        assert_eq!(tag, 100 + i as u64, "frame {i} delivered in arrival order");
    }
    assert!(try_poll_once(lease.next_slot(0)).is_none(), "the queue is drained");
}

/// Dropping a train lease releases the frames it still holds, and a frame for
/// its id arriving afterwards is released at the ring.
#[test]
fn a_dropped_lease_releases_held_and_late_frames() {
    let (r, writers) = reactor_with_rings(1);
    let lease = r.lease_train(WorkerSet::ALL, SalMessageKind::Scan);
    let id = lease.id();
    writers[0].send_msg(id, &Default::default());
    r.drain_all_w2m();

    let held = r.release_cursor_for_test(0);
    drop(lease);
    assert!(r.inner.trains.borrow().is_empty());
    assert!(r.release_cursor_for_test(0) > held, "the held frame is released");

    writers[0].send_msg(id, &Default::default());
    let late = r.release_cursor_for_test(0);
    r.drain_all_w2m();
    assert!(r.release_cursor_for_test(0) > late, "the late frame is released");
    assert!(r.inner.trains.borrow().is_empty(), "and routes nothing");
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

    let r = make_reactor_over(W2mReceiver::new(vec![ptr]));
    let lease = r.lease_train(WorkerSet::ALL, SalMessageKind::Scan);
    let id = lease.id();
    let writer = W2mWriter::new(ptr);

    let (done_tx, done_rx) = std::sync::mpsc::channel::<()>();
    let handle = std::thread::spawn(move || {
        for _ in 0..TOTAL {
            writer.send_msg(id, &ipc::WireMsg::default());
        }
        let _ = done_tx.send(());
    });

    let deadline = Instant::now() + Duration::from_secs(5);
    let queued = |r: &Reactor| r.inner.trains.borrow()[&id].queues[0].len();
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
        r.release_cursor_for_test(0),
        r.inner.w2m.write_cursor(0),
        "every emitted slot must be freed (release_cursor reaches write_cursor)",
    );
}

/// The three ways an op ends.
#[test]
fn an_op_ends_by_completion_early_drop_or_collection() {
    let r = make_reactor();
    let mut cx = Context::from_waker(Waker::noop());

    // 1. Dropped while pending: the entry stays until the late CQE retires it.
    let (id, mut rx) = bare_op(&r, None);
    assert!(Pin::new(&mut rx).poll(&mut cx).is_pending(), "no result yet");
    drop(rx);
    assert_eq!(r.inner.ops.borrow().len(), 1, "the kernel still owes the CQE");
    cqe(&r, KIND_OP, id, 0);
    assert_eq!(r.inner.ops.borrow().len(), 0, "the late CQE must retire the entry");

    // 2. Completion beats the drop: the CQE alone retires the entry.
    let (id, mut rx) = bare_op(&r, None);
    assert!(Pin::new(&mut rx).poll(&mut cx).is_pending());
    cqe(&r, KIND_OP, id, 0);
    assert_eq!(r.inner.ops.borrow().len(), 0, "a completed op is gone before its poll");
    drop(rx);

    // 3. Collected: the CQE `res` arrives verbatim.
    for rc in [0, -libc::EBADF] {
        let (id, mut rx) = bare_op(&r, None);
        cqe(&r, KIND_OP, id, rc);
        assert!(matches!(Pin::new(&mut rx).poll(&mut cx), Poll::Ready((got, None)) if got == rc));
    }
}

/// Instructions retired per op round trip: submit, pending poll, CQE, ready poll.
/// Run under `perf stat -e instructions:u`.
#[test]
#[ignore]
fn op_park_roundtrip_bench() {
    const N: u64 = 1_000_000;
    let r = make_reactor();
    let mut cx = Context::from_waker(Waker::noop());
    let start = Instant::now();
    for _ in 0..N {
        let (id, mut rx) = bare_op(&r, None);
        assert!(Pin::new(&mut rx).poll(&mut cx).is_pending());
        cqe(&r, KIND_OP, id, 0);
        assert!(std::hint::black_box(Pin::new(&mut rx).poll(&mut cx)).is_ready());
    }
    let ns = start.elapsed().as_nanos() as f64 / N as f64;
    println!("op_park_roundtrip_bench: {N} ops, {ns:.1} ns/op");
}
