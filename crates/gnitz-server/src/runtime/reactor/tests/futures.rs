//! The reactor's futures: timers, fsync, and a train lease's frame stream.

use std::os::fd::OwnedFd;
use std::time::Duration;

use super::super::test_support::*;
use super::*;
use crate::runtime::test_support::try_poll_once;

/// A sleep of nothing resolves on the very first poll instead of hanging the
/// reactor.
#[test]
fn a_zero_sleep_resolves_immediately() {
    let r = make_reactor();
    assert!(
        try_poll_once(r.sleep(Duration::ZERO)).is_some(),
        "a passed deadline must resolve on the first poll"
    );
    assert!(r.deadlines.borrow().is_empty(), "and register no deadline");
}

/// A dropped timer removes its deadline, so it neither wakes its waker nor
/// bounds a later sleep.
#[test]
fn a_dropped_timer_leaves_no_deadline() {
    let r = make_reactor();
    let mut tf = Box::pin(r.sleep(Duration::from_secs(3600)));
    assert!(tf.as_mut().poll(&mut Context::from_waker(Waker::noop())).is_pending());
    assert_eq!(r.deadlines.borrow().len(), 1, "the poll registers the deadline");
    drop(tf);
    assert!(r.deadlines.borrow().is_empty(), "the drop removes it");
}

/// `fsync` submits its SQE before returning, so its CQE arrives with no tick
/// driven — which is what lets it run while the caller works on — and resolves to
/// the CQE `res` verbatim: 0, or a socket's `-EINVAL`.
#[test]
fn fsync_is_submitted_eagerly_and_yields_the_cqe_res() {
    let r = make_reactor();
    let file = OwnedFd::from(tempfile::tempfile().expect("tempfile"));
    let (sock, _) = std::os::unix::net::UnixStream::pair().expect("socketpair");
    for (fd, want) in [(file, 0), (OwnedFd::from(sock), -libc::EINVAL)] {
        let mut fut = std::pin::pin!(r.fsync(leaked(fd)));
        let deadline = Instant::now() + Duration::from_secs(10);
        let got = loop {
            r.drain_cqes_into_wakers();
            if let Poll::Ready(rc) = fut.as_mut().poll(&mut Context::from_waker(Waker::noop())) {
                break rc;
            }
            assert!(
                Instant::now() < deadline,
                "no CQE without a tick: the SQE was not submitted"
            );
        };
        assert_eq!(got, want);
    }
}

/// A train route is a queue, not a slot: more frames than any worker count all
/// wait in it, and come out in arrival order.
#[test]
fn a_train_route_queues_past_worker_count_in_arrival_order() {
    const N: usize = 100; // > MAX_WORKERS
    let (r, writers) = reactor_with_rings(1);
    let lease = r.lease_train(WorkerSet::ALL, SalMessageKind::ScanSpec);
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

/// Dropping a train lease releases the frames it still holds.
#[test]
fn a_dropped_lease_releases_the_frames_it_holds() {
    let (r, writers) = reactor_with_rings(1);
    let lease = r.lease_train(WorkerSet::ALL, SalMessageKind::ScanSpec);
    for _ in 0..3 {
        writers[0].send_msg(lease.id(), &Default::default());
    }
    r.drain_all_w2m();
    assert!(r.w2m.release_cursor(0) < r.w2m.write_cursor(0), "the route holds them");
    drop(lease);
    assert_eq!(r.w2m.release_cursor(0), r.w2m.write_cursor(0));
}

/// `next` yields exactly the frames that carry rows, workers in ascending order,
/// and ends once every worker's terminal frame is read — a row-less one
/// included. Every slot it passes over is released at its ring.
#[test]
fn next_yields_the_row_frames_and_releases_the_rest() {
    let (r, writers) = reactor_with_rings(2);
    let lease = r.lease_train(WorkerSet::ALL, SalMessageKind::ScanSpec);
    let schema = crate::test_support::make_schema_u64_i64();
    let rows = |pk| crate::test_support::make_batch(&schema, &[(pk, 1, 0)]);
    let send = |w: usize, last, batch: Option<&gnitz_zset::repr::Batch>| {
        let msg = crate::runtime::wire::WireMsg {
            flags: gnitz_wire::WireFlags::train_frame(last),
            data: batch.and_then(gnitz_zset::repr::Batch::wire_whole),
            ..Default::default()
        };
        writers[w].send_msg(lease.id(), &msg);
    };
    send(0, false, None);
    send(0, false, Some(&rows(1)));
    send(0, true, None);
    send(1, true, Some(&rows(2)));
    r.drain_all_w2m();

    for (w, pk) in [(0u32, 1u64), (1, 2)] {
        let f = try_poll_once(lease.next())
            .expect("routed")
            .expect("no fault")
            .expect("a row frame");
        assert_eq!(f.slot.worker, w, "frames arrive in worker order");
        assert_eq!(f.rows(&schema).view().get_pk_bytes(0), pk.to_be_bytes());
    }
    assert!(try_poll_once(lease.next())
        .expect("routed")
        .expect("no fault")
        .is_none());
    for w in 0..2 {
        assert_eq!(r.w2m.release_cursor(w), r.w2m.write_cursor(w), "worker {w}");
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
        let (u, mut rx) = r.install_op(None);
        assert!(Pin::new(&mut rx).poll(&mut cx).is_pending());
        r.dispatch_cqe(u, 0);
        assert!(std::hint::black_box(Pin::new(&mut rx).poll(&mut cx)).is_ready());
    }
    let ns = start.elapsed().as_nanos() as f64 / N as f64;
    println!("op_park_roundtrip_bench: {N} ops, {ns:.1} ns/op");
}
