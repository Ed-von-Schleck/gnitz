//! Reactor state: reply routing and the id spaces that key it, the
//! `FLAG_EXCHANGE` peel, and the W2M ring drain.

use std::cell::Cell as StdCell;
use std::time::Duration;

use gnitz_engine::schema::SchemaDescriptor;

use super::test_support::*;
use super::*;

/// Route `decoded` the way `drain_w2m_for_worker` does: the ring prefix every
/// real producer sets, which is the payload's own `request_id`.
fn route(r: &Reactor, w: usize, decoded: DecodedWire) {
    let prefix = decoded.control.request_id as u32;
    r.route_reply(w, prefix, decoded);
}

/// A parked DecodedWire must be returned to the awaiter on resume.
#[test]
fn reply_waker_dispatch() {
    let r = make_reactor();
    let got: Rc<StdCell<u64>> = Rc::new(StdCell::new(0));
    let got2 = Rc::clone(&got);
    let lease = r.alloc_replies(1);
    let id = lease[0];
    let reply_fut = r.await_reply(id);
    route(&r, 0, synthetic_decoded_wire(id));
    r.block_on(async move {
        got2.set(reply_fut.await.control.request_id);
    });
    assert_eq!(got.get(), id);
}

/// A reply for a different req_id must not wake an unrelated awaiter:
/// the guard timer must win the race.
#[test]
fn reply_waker_no_spurious() {
    let r = make_reactor();
    let lease = r.alloc_replies(1);
    let id = lease[0];
    // A reply nobody awaits is dropped; the point is that it must not
    // resolve the awaiter below.
    route(&r, 0, synthetic_decoded_wire(id + 1));
    let resolved: Rc<StdCell<bool>> = Rc::new(StdCell::new(false));
    let r2 = Rc::clone(&resolved);
    let timer_inner = Rc::clone(&r.inner);
    let reply_fut = r.await_reply(id);
    r.block_on(async move {
        let timer = TimerFuture::new(Instant::now() + Duration::from_millis(50), timer_inner);
        select_reply_or_timer(timer, reply_fut, &r2).await;
    });
    assert!(!resolved.get(), "a reply for another req_id must not wake this awaiter");
}

/// `alloc_replies` hands out strictly increasing ids in
/// `[1, MAX_REGULAR_REQ_ID]`, so bit 31 (the scan marker) stays clear, and
/// opens a park slot for every one of them.
#[test]
fn alloc_replies_is_monotonic_and_opens_every_slot() {
    let r = make_reactor();
    let lease = r.alloc_replies(1000);
    let mut last = 0u64;
    for &id in lease.iter() {
        assert!(id > last);
        assert!((1..=MAX_REGULAR_REQ_ID).contains(&id));
        assert!(r.inner.replies.is_open(id), "the lease must open every id's slot");
        last = id;
    }
    assert_eq!(r.inner.replies.len(), 1000);
    drop(lease);
    assert_eq!(r.inner.replies.len(), 0, "dropping the lease must close every slot");
}

/// The window `ReplyLease` exists to close: a reply that lands after the id is
/// allocated but before anything awaits it must still be delivered.
#[test]
fn a_reply_landing_before_its_awaiter_is_delivered() {
    let r = make_reactor();
    let lease = r.alloc_replies(1);
    let id = lease[0];

    route(&r, 0, synthetic_decoded_wire(id));

    let got: Rc<StdCell<u64>> = Rc::new(StdCell::new(0));
    let got2 = Rc::clone(&got);
    let fut = r.await_reply(id);
    r.block_on(async move {
        got2.set(fut.await.control.request_id);
    });
    assert_eq!(got.get(), id, "a reply parked before its awaiter must not be dropped");
}

/// Routed reply: wakes the parked awaiter and resolves it.
/// No `in_flight` accounting — the tail-chasing ring self-maintains.
#[test]
fn route_reply_routes_to_registered_waker() {
    let r = make_reactor();
    let lease = r.alloc_replies(1);
    let id = lease[0];

    let mut fut = std::pin::pin!(r.await_reply(id));
    let waker = make_waker(0);
    let mut cx = Context::from_waker(&waker);
    assert!(fut.as_mut().poll(&mut cx).is_pending());

    route(&r, 1, synthetic_decoded_wire(id));

    assert!(
        r.inner.run_queue.borrow().is_queued(0),
        "a routed reply must wake its awaiter"
    );
    assert!(fut.as_mut().poll(&mut cx).is_ready());
}

/// Unrouted reply (nobody awaiting): logged and dropped. Must not leave a
/// slot behind — stale entries would keep non-dead memory alive.
#[test]
fn route_reply_unrouted_is_logged_and_dropped() {
    let r = make_reactor();
    // Nobody allocated a lease for req_id 7.

    route(&r, 0, synthetic_decoded_wire(7));

    assert_eq!(r.inner.replies.len(), 0, "unrouted replies must not leak a park slot");
}

// ─────────────────────────────────────────────────────────────────
// FLAG_EXCHANGE peel: the reactor queues the frame, and must not let it
// resolve the in-flight tick id the worker stamped on it.
// ─────────────────────────────────────────────────────────────────

/// A synthetic FLAG_EXCHANGE wire for `view_id`. `req_id` is the in-flight
/// tick id the worker echoes back on the frame — the thing the peel exists to
/// protect.
fn synthetic_exchange_wire(view_id: i64, req_id: u64) -> DecodedWire {
    use gnitz_wire::control::DecodedControl;
    DecodedWire {
        control: DecodedControl {
            target_id: view_id as u64,
            flags: FLAG_EXCHANGE,
            request_id: req_id,
            ..Default::default()
        },
        schema: Some(SchemaDescriptor::minimal_u64()),
        data_batch: None,
    }
}

/// FLAG_EXCHANGE replies must NOT consume the registered tick
/// waker. The tick worker is still mid-DAG and the final ACK will
/// arrive separately.
#[test]
fn route_reply_flag_exchange_does_not_wake() {
    let r = make_reactor();
    let lease = r.alloc_replies(1);
    let id = lease[0];
    let mut fut = std::pin::pin!(r.await_reply(id));
    let waker = make_waker(0);
    let mut cx = Context::from_waker(&waker);
    assert!(fut.as_mut().poll(&mut cx).is_pending());

    route(&r, 0, synthetic_exchange_wire(/*view_id*/ 100, id));

    assert!(
        fut.as_mut().poll(&mut cx).is_pending(),
        "FLAG_EXCHANGE must NOT resolve the tick's await_reply"
    );
    assert!(
        r.inner.replies.has_waker(id),
        "FLAG_EXCHANGE must leave the tick waker parked"
    );
}

/// Every worker's FLAG_EXCHANGE frame is queued for the relay task, tagged with
/// the worker that sent it, and none of them resolves the tick id it rides.
/// The frames' *plain* ACKs, arriving later, are what resolve those.
#[test]
fn route_reply_flag_exchange_queues_frames_for_the_relay_task() {
    let r = make_reactor();
    let lease = r.alloc_replies(2);
    let (id0, id1) = (lease[0], lease[1]);

    let ack0 = r.await_reply(id0);
    let ack1 = r.await_reply(id1);

    route(&r, 0, synthetic_exchange_wire(99, id0));
    route(&r, 1, synthetic_exchange_wire(99, id1));

    let mut q = r.inner.exchanges.borrow_mut();
    let (w0, f0) = q.pop().expect("worker 0's exchange frame must be queued");
    let (w1, f1) = q.pop().expect("worker 1's exchange frame must be queued");
    drop(q);
    assert_eq!((w0, w1), (0, 1), "each frame carries the worker that sent it");
    assert_eq!(f0.control.target_id, 99);
    assert_eq!(f1.control.target_id, 99);

    // Final ACKs (no FLAG_EXCHANGE) for the same req_ids resolve the
    // awaiters the exchange frames deliberately left parked.
    route(&r, 0, synthetic_decoded_wire(id0));
    route(&r, 1, synthetic_decoded_wire(id1));
    let waker = make_waker(0);
    let mut cx = Context::from_waker(&waker);
    assert!(std::pin::pin!(ack0).as_mut().poll(&mut cx).is_ready());
    assert!(std::pin::pin!(ack1).as_mut().poll(&mut cx).is_ready());
}

/// Cross-process stress: a forked child publishes `n_messages` into a
/// shared W2M ring while the parent drains them through the reactor's
/// `FUTEX_WAITV` + `W2mReceiver` pipeline.
///
/// Regression guard for two distinct hazards, both of them *loss*:
/// 1. The lost-wake race in the master's park: at this scale the master
///    takes many drain → arm cycles, each a potential lost-wake window. A
///    missed wake hangs the test (caught by the timer guard).
/// 2. The writer-crosses-reader data-loss bug in the SKIP-wrap path:
///    capacity is small enough (64 KiB) and the message count high enough
///    (500 × ~280 B ≈ 140 KiB) to force multiple SKIP-wraps. A truncated
///    delivery fails the `ids` assertion.
///
/// It says nothing about ordering, and cannot: `join_all_unpin` returns its
/// results in *input* order, so `received` is sorted by construction.
/// `w2m_concurrent_small_frames_arrive_in_order` (runtime/tests/w2m.rs) is
/// where ring order is tested.
fn w2m_cross_process_stress(n_messages: u64, timeout_secs: u64) {
    use crate::runtime::reactor::{join_all_unpin, select2, Either};
    use crate::runtime::w2m::{W2mReceiver, W2mWriter};
    use gnitz_wire::STATUS_OK;
    use std::time::Duration;

    const CAPACITY: usize = 64 * 1024;

    let region = unsafe { crate::runtime::w2m::test_ring(CAPACITY) };
    let ptr = region.ptr();

    let pid = unsafe { libc::fork() };
    assert!(pid >= 0, "fork failed");
    if pid == 0 {
        // Child: publish a monotonic req_id stream as fast as
        // possible. The parent's wake protocol must not drop
        // any of them under the resulting drain-refresh-arm
        // race pressure.
        let writer = W2mWriter::new(ptr);
        for req_id in 1..=n_messages {
            writer.send_status(0, req_id, STATUS_OK, &[]);
        }
        unsafe {
            libc::_exit(0);
        }
    }

    // Parent: drain via the reactor's FUTEX_WAITV + W2mReceiver path.
    let reactor = Reactor::new(16).expect("reactor");

    let received: Rc<RefCell<Vec<u64>>> = Rc::new(RefCell::new(Vec::new()));
    // One lease for the whole run, held to the end: the ids are 1..=n, which is
    // what the child publishes.
    let lease = reactor.alloc_replies(n_messages as usize);
    let reply_futs: Vec<_> = lease.iter().map(|&i| reactor.await_reply(i)).collect();
    {
        let received = Rc::clone(&received);
        reactor.spawn(async move {
            let replies = join_all_unpin(reply_futs).await;
            *received.borrow_mut() = replies.into_iter().map(|r| r.control.request_id).collect();
        });
    }
    // One tick polls the spawned task, which walks join_all_unpin and
    // registers every ReplyFuture's waker before attach drains.
    reactor.tick(false);

    reactor.attach_w2m(Rc::new(W2mReceiver::new(vec![ptr])));

    let inner = Rc::clone(&reactor.inner);
    let received_check = Rc::clone(&received);
    let outcome = reactor.block_on(async move {
        let timeout = TimerFuture::new(Instant::now() + Duration::from_secs(timeout_secs), inner);
        let watch = async move {
            while received_check.borrow().is_empty() {
                YieldOnce::new().await;
            }
        };
        select2(watch, timeout).await
    });
    if let Either::B(()) = outcome {
        unsafe {
            libc::kill(pid, libc::SIGKILL);
        }
        panic!(
            "reactor stalled with {} replies received after {}s — \
             lost-wake symptom",
            received.borrow().len(),
            timeout_secs,
        );
    }

    let ids = received.borrow().clone();
    assert_eq!(ids.len(), n_messages as usize);
    let expected: Vec<u64> = lease.to_vec();
    assert_eq!(ids, expected, "every published req_id must round-trip");

    let mut status: i32 = 0;
    unsafe {
        libc::waitpid(pid, &mut status, 0);
    }

    // AsyncCancel the in-flight FUTEX_WAITV before the storage drops.
    reactor.request_shutdown();
    drop(lease);
}

#[test]
fn w2m_cross_process_stress_drains_all_messages_via_reactor() {
    w2m_cross_process_stress(500, 30);
}

/// An unread publish must make the arm refuse, so the reactor drains
/// instead of arming a `FUTEX_WAITV` for a wake that already happened.
///
/// The publish lands before the arm, so there is no race to lose: the
/// unread-data check either runs or the code is wrong.
#[test]
fn arm_waitv_refuses_while_data_is_unread() {
    use crate::runtime::w2m::{W2mReceiver, W2mWriter};
    use gnitz_wire::STATUS_OK;

    let region = unsafe { crate::runtime::w2m::test_ring(64 * 1024) };
    let ptr = region.ptr();
    W2mWriter::new(ptr).send_status(0, 1u64, STATUS_OK, &[]);

    let receiver = W2mReceiver::new(vec![ptr]);
    let mut out = [FutexWaitV::new(); 1];
    assert!(
        receiver.arm_waitv(&mut out).is_none(),
        "an unread publish must refuse the arm — arming it would be a lost wake",
    );
}

// ─────────────────────────────────────────────────────────────────
// The shared id counter: bit 31 clear for a reply id, set for a scan id,
// and both wrapping at MAX_REGULAR_REQ_ID.
// ─────────────────────────────────────────────────────────────────

#[test]
fn reply_ids_wrap_within_the_regular_range() {
    let r = make_reactor();
    r.inner.next_request_id.set(MAX_REGULAR_REQ_ID - 1);
    let lease = r.alloc_replies(3);
    assert_eq!(lease[0], MAX_REGULAR_REQ_ID - 1);
    assert_eq!(lease[1], MAX_REGULAR_REQ_ID);
    assert_eq!(lease[2], 1, "must wrap to 1 after MAX_REGULAR_REQ_ID");
    assert!(lease[2] & SCAN_REQ_ID_FLAG as u64 == 0, "bit 31 must be clear");
}

#[test]
fn alloc_scan_request_id_bit31_always_set() {
    let r = make_reactor();
    for _ in 0..1000 {
        let id = r.alloc_scan_request_id();
        assert!(id & SCAN_REQ_ID_FLAG as u64 != 0, "bit 31 must be set");
        assert!(id & !(SCAN_REQ_ID_FLAG as u64) <= MAX_REGULAR_REQ_ID);
    }
}

/// The two spaces share one counter, so this is the property that makes that
/// safe: the flag alone separates them.
#[test]
fn regular_and_scan_ids_never_collide() {
    let r = make_reactor();
    let regular_ids: std::collections::HashSet<u64> = r.alloc_replies(10000).iter().copied().collect();
    let scan_ids: std::collections::HashSet<u64> = (0..10000).map(|_| r.alloc_scan_request_id()).collect();
    assert!(
        regular_ids.is_disjoint(&scan_ids),
        "regular and scan IDs must not overlap"
    );
}
