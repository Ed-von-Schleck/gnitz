//! Reactor state: reply routing and the id spaces that key it, the
//! `FLAG_EXCHANGE` peel, and the W2M ring drain.

use std::time::Duration;

use gnitz_engine::schema::SchemaDescriptor;

use super::test_support::*;
use super::*;

/// `alloc_replies` hands out strictly increasing ids in
/// `[1, MAX_REGULAR_REQ_ID]` and opens a park slot for every one of them,
/// which dropping the lease closes.
#[test]
fn alloc_replies_is_monotonic_and_opens_every_slot() {
    let r = make_reactor();
    let lease = r.alloc_replies(4);
    let mut last = 0u64;
    for &id in lease.iter() {
        assert!(id > last && (1..=MAX_REGULAR_REQ_ID).contains(&id));
        assert!(r.inner.replies.is_open(id), "the lease must open every id's slot");
        last = id;
    }
    assert_eq!(r.inner.replies.len(), 4);
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

    r.route_reply(0, id as u32, synthetic_decoded_wire(id));

    let fut = r.await_reply(id);
    let got = r.block_on(async move { fut.await.control.request_id });
    assert_eq!(got, id, "a reply parked before its awaiter must not be dropped");
}

/// A routed reply wakes the parked awaiter and resolves it; one for an id
/// nobody leased is dropped without leaving a slot behind.
#[test]
fn route_reply_wakes_its_awaiter_and_drops_the_unrouted() {
    let r = make_reactor();
    let lease = r.alloc_replies(1);
    let id = lease[0];

    let mut fut = std::pin::pin!(r.await_reply(id));
    let waker = make_waker(0);
    let mut cx = Context::from_waker(&waker);
    assert!(fut.as_mut().poll(&mut cx).is_pending());

    // A reply for an unleased id reaches no awaiter and allocates no slot.
    r.route_reply(0, (id + 1) as u32, synthetic_decoded_wire(id + 1));
    assert_eq!(r.inner.replies.len(), 1, "an unrouted reply must not open a slot");
    assert!(fut.as_mut().poll(&mut cx).is_pending(), "nor resolve another id");

    r.route_reply(1, id as u32, synthetic_decoded_wire(id));
    assert!(
        r.inner.run_queue.borrow().is_queued(0),
        "a routed reply must wake its awaiter"
    );
    assert!(fut.as_mut().poll(&mut cx).is_ready());
}

/// A synthetic FLAG_EXCHANGE wire for `view_id`. `req_id` is the in-flight
/// tick id the worker echoes back on the frame — the thing the peel exists to
/// protect.
fn synthetic_exchange_wire(view_id: i64, req_id: u64) -> DecodedWire {
    let mut w = synthetic_decoded_wire(req_id);
    w.control.target_id = view_id as u64;
    w.control.flags = FLAG_EXCHANGE;
    w.schema = Some(SchemaDescriptor::minimal_u64());
    w
}

/// Each worker's FLAG_EXCHANGE frame is queued for the relay task tagged with
/// its sender, and none of them consumes the tick waker riding the frame — the
/// worker is still mid-DAG, and the plain ACK that arrives later is what
/// resolves it.
#[test]
fn flag_exchange_frames_queue_without_resolving_their_tick_id() {
    let r = make_reactor();
    let lease = r.alloc_replies(2);
    let (id0, id1) = (lease[0], lease[1]);

    let mut ack0 = std::pin::pin!(r.await_reply(id0));
    let mut ack1 = std::pin::pin!(r.await_reply(id1));
    let waker = make_waker(0);
    let mut cx = Context::from_waker(&waker);
    assert!(ack0.as_mut().poll(&mut cx).is_pending());
    assert!(ack1.as_mut().poll(&mut cx).is_pending());

    r.route_reply(0, id0 as u32, synthetic_exchange_wire(99, id0));
    r.route_reply(1, id1 as u32, synthetic_exchange_wire(99, id1));

    let mut q = r.inner.exchanges.borrow_mut();
    let (w0, f0) = q.pop().expect("worker 0's exchange frame must be queued");
    let (w1, f1) = q.pop().expect("worker 1's exchange frame must be queued");
    drop(q);
    assert_eq!((w0, w1), (0, 1), "each frame carries the worker that sent it");
    assert_eq!((f0.control.target_id, f1.control.target_id), (99, 99));

    assert!(ack0.as_mut().poll(&mut cx).is_pending(), "the tick must stay parked");
    assert!(r.inner.replies.has_waker(id0), "and keep its waker");

    // The final ACKs, without FLAG_EXCHANGE, resolve what the frames left parked.
    r.route_reply(0, id0 as u32, synthetic_decoded_wire(id0));
    r.route_reply(1, id1 as u32, synthetic_decoded_wire(id1));
    assert!(ack0.as_mut().poll(&mut cx).is_ready());
    assert!(ack1.as_mut().poll(&mut cx).is_ready());
}

/// Cross-process stress: a forked child publishes into a shared W2M ring while
/// the parent drains through the reactor's `FUTEX_WAITV` + `W2mReceiver`
/// pipeline.
///
/// Guards two losses. A missed wake — the master takes many drain → arm cycles
/// here, each a window — hangs, and the timer catches it. A truncated
/// SKIP-wrap — 500 × ~280 B against a 64 KiB ring forces several wraps — fails
/// the id assertion.
///
/// It says nothing about ordering, and cannot: `join_all_unpin` returns results
/// in *input* order, so `received` is sorted by construction. Ring order is
/// tested in `runtime/tests/w2m.rs`.
#[test]
fn w2m_cross_process_stress_drains_all_messages_via_reactor() {
    use crate::runtime::w2m::{W2mReceiver, W2mWriter};
    use gnitz_wire::STATUS_OK;

    const N_MESSAGES: u64 = 500;
    const TIMEOUT: Duration = Duration::from_secs(30);

    let region = unsafe { crate::runtime::w2m::test_ring(64 * 1024) };
    let ptr = region.ptr();

    let pid = unsafe { libc::fork() };
    assert!(pid >= 0, "fork failed");
    if pid == 0 {
        // Child: publish a monotonic req_id stream as fast as possible. The
        // parent's wake protocol must not drop any of them under the resulting
        // drain-refresh-arm race pressure.
        let writer = W2mWriter::new(ptr);
        for req_id in 1..=N_MESSAGES {
            writer.send_status(0, req_id, STATUS_OK, &[]);
        }
        unsafe { libc::_exit(0) };
    }

    let reactor = make_reactor();
    let received: Rc<RefCell<Vec<u64>>> = Rc::new(RefCell::new(Vec::new()));
    // One lease for the whole run, held to the end: the ids are 1..=N, which is
    // what the child publishes.
    let lease = reactor.alloc_replies(N_MESSAGES as usize);
    let reply_futs: Vec<_> = lease.iter().map(|&i| reactor.await_reply(i)).collect();
    {
        let received = Rc::clone(&received);
        reactor.spawn(async move {
            let replies = join_all_unpin(reply_futs).await;
            *received.borrow_mut() = replies.into_iter().map(|r| r.control.request_id).collect();
        });
    }
    // One tick polls the spawned task, which walks join_all_unpin and registers
    // every ReplyFuture's waker before the attach drains.
    reactor.tick(false);

    reactor.attach_w2m(Rc::new(W2mReceiver::new(vec![ptr])));

    let received_check = Rc::clone(&received);
    let timeout = reactor.timer(Instant::now() + TIMEOUT);
    let outcome = reactor.block_on(async move {
        let watch = async move {
            while received_check.borrow().is_empty() {
                YieldOnce::new().await;
            }
        };
        select2(watch, timeout).await
    });
    if let Either::B(()) = outcome {
        unsafe { libc::kill(pid, libc::SIGKILL) };
        panic!(
            "reactor stalled with {} replies received after {TIMEOUT:?} — lost-wake symptom",
            received.borrow().len(),
        );
    }

    assert_eq!(
        *received.borrow(),
        lease.to_vec(),
        "every published req_id must round-trip"
    );

    let mut status: i32 = 0;
    unsafe { libc::waitpid(pid, &mut status, 0) };

    // AsyncCancel the in-flight FUTEX_WAITV before the storage drops.
    reactor.request_shutdown();
    drop(lease);
}

/// An unread publish must make the arm refuse, so the reactor drains instead of
/// arming a `FUTEX_WAITV` for a wake that already happened. The publish lands
/// before the arm, so there is no race to lose.
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

/// The two id spaces share one counter, so bit 31 alone separates them: clear
/// for a reply id, set for a scan id, both wrapping within the regular range.
#[test]
fn reply_and_scan_ids_are_separated_by_bit_31_and_wrap() {
    const FLAG: u64 = SCAN_REQ_ID_FLAG as u64;
    let r = make_reactor();
    r.inner.next_request_id.set(MAX_REGULAR_REQ_ID - 1);

    let lease = r.alloc_replies(3);
    assert_eq!(
        (lease[0], lease[1], lease[2]),
        (MAX_REGULAR_REQ_ID - 1, MAX_REGULAR_REQ_ID, 1),
        "reply ids must wrap to 1 after MAX_REGULAR_REQ_ID"
    );
    assert!(lease.iter().all(|id| id & FLAG == 0), "bit 31 must be clear");

    for _ in 0..3 {
        let id = r.alloc_scan_request_id();
        assert!(id & FLAG != 0, "bit 31 must be set");
        assert!(id & !FLAG <= MAX_REGULAR_REQ_ID, "low bits stay in the regular range");
    }
}
