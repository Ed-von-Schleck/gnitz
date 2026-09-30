//! Client connections: accept dispatch, the send loop, its CQE and its eviction
//! deadline, and how a connection's recv side ends and its socket closes.

use std::io::{Read, Write};
use std::os::fd::OwnedFd;
use std::time::Duration;

use super::super::test_support::*;
use super::*;
use crate::runtime::test_support::try_poll_once;
use gnitz_store::storage::PooledBuf;

/// A send's carry: `body`, over a connection of its own.
fn outbound(r: &Reactor, body: SendBody) -> Outbound {
    Outbound { _conn: client_pair(r).0, body }
}

// ─────────────────────────────────────────────────────────────────
// KIND_OP CQE dispatch + a send op's lifecycle.
//
// Regression guards for:
//   (a) partial-send handling in `send_owned` (OP_SEND on a stream
//       socket can return rc < len); treating one CQE as "done"
//       truncated the scan response at ~208 KB and hung the client;
//   (b) the send's `ops` entry holding its body so the kernel's
//       in-flight pointer stays valid after cancellation.
// ─────────────────────────────────────────────────────────────────

#[test]
fn send_cqe_retires_its_entry_wakes_its_awaiter_and_returns_the_body() {
    let r = make_reactor();
    let (u, mut fut) = r.install_op(Some(outbound(&r, SendBody::Pooled(PooledBuf(vec![0xAB; 16])))));
    let mut fut = Pin::new(&mut fut);
    let (flag, waker) = WakeFlag::new();
    let mut cx = Context::from_waker(&waker);
    assert!(fut.as_mut().poll(&mut cx).is_pending());

    r.dispatch_cqe(u, 16, 0);
    assert!(r.inner.ops.borrow().is_empty(), "the CQE alone retires the entry");
    assert!(flag.woken(), "KIND_OP must wake the op future");
    match fut.as_mut().poll(&mut cx) {
        Poll::Ready((rc, body)) => {
            assert_eq!(rc, 16, "KIND_OP must deliver the CQE rc verbatim");
            assert_eq!(
                body.expect("a send carries its body").body.bytes(),
                &[0xAB; 16],
                "and hand the body back"
            );
        }
        Poll::Pending => panic!("a completed send must resolve"),
    }
}

/// A dropped send keeps its connection and body until the late CQE — a ring slot
/// here, whose `release_cursor` shows when it drops.
#[test]
fn dropped_send_future_keeps_its_body_until_the_cqe() {
    let (receiver, slot) = ring_slot(0);
    let held = receiver.release_cursor(0);

    let r = make_reactor();
    let out = outbound(&r, SendBody::Slot(slot));
    let conn = Rc::clone(&out._conn);
    let (u, mut fut) = r.install_op(Some(out));
    assert!(try_poll_once(&mut fut).is_none());
    drop(fut);
    assert_eq!(r.inner.ops.borrow().len(), 1, "drop must leave the entry to its CQE");
    assert_eq!(Rc::strong_count(&conn), 2, "the entry holds the connection");
    assert_eq!(
        receiver.release_cursor(0),
        held,
        "the kernel may still read the body — it must outlive the future"
    );

    r.dispatch_cqe(u, 64, 0);
    assert_eq!(r.inner.ops.borrow().len(), 0, "the late CQE must retire the entry");
    assert!(receiver.release_cursor(0) > held, "and free the body");
    assert_eq!(Rc::strong_count(&conn), 1, "and release the connection");
}

// ─────────────────────────────────────────────────────────────────
// The recv side and the socket's lifetime.
// ─────────────────────────────────────────────────────────────────

/// A peer's half-close ends the recv side once the frames ahead of it are
/// delivered, and the reactor lets go of the connection; but the socket stays open
/// for as long as any holder keeps the connection, and closes with the last.
/// Observed from the partner end, so no fd number is probed.
#[test]
fn a_closed_connection_delivers_its_queue_and_closes_with_its_last_holder() {
    let r = make_reactor();
    let (conn, mut partner) = registered(&r);
    (&partner).write_all(&framed(b"last")).expect("write");
    partner.shutdown(std::net::Shutdown::Write).expect("half-close");

    let c = Rc::clone(&conn);
    let (last, end) = r.block_on(async move { (c.recv().await, c.recv().await) });
    assert_eq!(last.expect("the queued frame").as_slice(), b"last");
    assert!(end.is_none(), "then recv reports the end");
    assert_eq!(Rc::strong_count(&conn), 1, "the reactor let go of the connection");
    assert!(
        !conn.is_gone(),
        "the end of the recv side does not finish the connection"
    );

    partner.set_nonblocking(true).expect("nonblocking");
    let mut buf = [0u8; 1];
    assert_eq!(
        partner.read(&mut buf).map_err(|e| e.kind()),
        Err(std::io::ErrorKind::WouldBlock),
        "the socket stays open while the connection is held"
    );
    drop(conn);
    assert_eq!(partner.read(&mut buf).ok(), Some(0), "and closes with its last holder");
}

/// A connection holds its slot under the cap for as long as it lives: at the
/// cap an accepted fd is refused and closed, and a dropped connection frees
/// its slot for the next.
#[test]
fn the_connection_cap_refuses_past_it_until_a_connection_drops() {
    let r = make_reactor_with(Limits { max_conns: 1, ..Limits::TEST });
    let (a, _a) = std::os::unix::net::UnixStream::pair().expect("socketpair");
    let (b, mut b_partner) = std::os::unix::net::UnixStream::pair().expect("socketpair");
    let (c, _c) = std::os::unix::net::UnixStream::pair().expect("socketpair");
    let first = r.client_conn(OwnedFd::from(a)).expect("under the cap");
    assert!(r.client_conn(OwnedFd::from(b)).is_none(), "the cap refuses a second");
    let mut buf = [0u8; 1];
    assert_eq!(b_partner.read(&mut buf).ok(), Some(0), "and closes the refused fd");
    drop(first);
    assert!(
        r.client_conn(OwnedFd::from(c)).is_some(),
        "a dropped connection frees its slot"
    );
}

/// A local close ends an armed recv, even one not yet submitted, so a silent
/// peer cannot pin the connection.
#[test]
fn close_ends_an_armed_recv() {
    let r = make_reactor();
    let (conn, _partner) = registered(&r);
    conn.fail();

    // The peer neither writes nor closes: only the shutdown can complete the
    // recv.
    assert!(
        poll_until(&r, || Rc::strong_count(&conn) == 1),
        "close must end the armed recv; without it the reactor holds the connection until the peer acts"
    );
}

/// A payload far larger than the socket buffers completes across many short sends.
#[test]
fn send_owned_loops_until_full_payload_sent_over_socketpair() {
    let (r, conn, receiver) = egress_pair(Limits::TEST, Some(8 * 1024));
    let payload = vec![0x5Au8; 200 * 1024];
    let payload_len = payload.len();
    let drain_t = spawn_drain(receiver, payload_len);

    let r2 = Rc::clone(&r);
    // `conn` drops with the task, closing the sender before the join: on the
    // truncated send this test exists to catch, the drain's blocking `read`
    // would otherwise never return and the whole binary would hang.
    let sent = r.block_on(async move { r2.send_owned(&conn, SendBody::Pooled(PooledBuf(payload))).await });
    let received = drain_t.join().expect("drain thread");
    assert_eq!(
        sent,
        Ok(()),
        "send_owned must loop on partial CQEs until the full payload is sent"
    );
    assert_eq!(
        received, payload_len as u64,
        "receiver must observe every byte — a truncated send would leave the \
         client blocked waiting for bytes that never arrive"
    );
}

/// A peer that never reads is evicted once a send makes no progress past the deadline.
#[test]
fn send_owned_evicts_a_client_that_never_drains() {
    const TIMEOUT: Duration = Duration::from_millis(10);
    let limits = Limits {
        client_send_timeout: TIMEOUT,
        ..Limits::TEST
    };
    let (r, conn, _receiver) = egress_pair(limits, Some(4 * 1024));

    // Far larger than both buffers, and nothing ever reads the other
    // end — the send stalls partway and only the deadline can end it.
    let payload = vec![0x7Au8; 1024 * 1024];
    let (r2, c2) = (Rc::clone(&r), Rc::clone(&conn));
    let start = Instant::now();
    let res = r.block_on(async move { r2.send_owned(&c2, SendBody::Pooled(PooledBuf(payload))).await });
    let elapsed = start.elapsed();

    assert_eq!(res, Err(PeerGone), "a client that never drains must be evicted");
    assert!(conn.is_gone(), "eviction finishes the connection");
    assert!(
        elapsed >= TIMEOUT,
        "eviction must wait out the full deadline ({TIMEOUT:?}), took {elapsed:?}"
    );
    // The eviction shuts the socket down, so the abandoned send completes and
    // releases the connection it held.
    assert!(
        poll_until(&r, || Rc::strong_count(&conn) == 1),
        "the abandoned send must complete and let go of the connection"
    );
}

/// An attached listener hands each connection it accepts to its channel.
#[test]
fn an_attached_listener_delivers_accepted_connections() {
    within(|| {
        let r = make_reactor();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
        let addr = listener.local_addr().expect("addr");
        let mut rx = r.attach_listener(OwnedFd::from(listener));
        let client = std::net::TcpStream::connect(addr).expect("connect");
        let accepted = std::net::TcpStream::from(r.block_on(async move { rx.recv().await }));
        assert_eq!(accepted.peer_addr().ok(), client.local_addr().ok());
    });
}

/// A failed accept ends a listener's multishot accept, and the re-arm is
/// deferred behind a backoff so closing connections get a window. Both
/// listeners can fail in the same window — fd exhaustion is global — and each
/// must come back.
#[test]
fn both_listeners_rearm_after_an_fd_exhaustion_backoff() {
    let r = make_reactor();
    let (a, b) = (fake_listener(), fake_listener());
    let (_rx_a, _rx_b) = (r.attach_listener(a), r.attach_listener(b));

    // `CQE_F_MORE` clear (`flags = 0`) is the kernel saying the multishot SQE
    // is gone; -EMFILE is why.
    r.handle_accept_cqe(0, -libc::EMFILE, 0);
    r.handle_accept_cqe(1, -libc::EMFILE, 0);
    assert_eq!(
        r.inner.tasks.borrow().len(),
        2,
        "each cancelled listener gets its own backoff task"
    );

    let deadline = Instant::now() + Limits::TEST.accept_rearm_backoff + Duration::from_secs(5);
    while !r.inner.tasks.borrow().is_empty() && Instant::now() < deadline {
        r.tick(true);
    }
    assert!(
        r.inner.tasks.borrow().is_empty(),
        "both backoff tasks must fire and re-arm their listener"
    );
}
