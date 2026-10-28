//! Client connections: the accept op, the send loop and its eviction deadline,
//! and how a connection's recv side ends and its socket closes.

use std::io::Write;
use std::os::fd::{FromRawFd, OwnedFd};
use std::os::unix::net::UnixStream;
use std::time::Duration;

use super::super::test_support::*;
use super::*;
use gnitz_wire::framed;
use gnitz_zset::repr::PooledBuf;

/// Shrink the send buffer of `conn` and the receive buffer of `partner` to
/// `bytes`, so a send far larger stalls partway.
fn shrink(conn: &ClientConn, partner: &UnixStream, bytes: i32) {
    for (fd, opt) in [(conn.fd(), libc::SO_SNDBUF), (partner.as_raw_fd(), libc::SO_RCVBUF)] {
        gnitz_foundation::posix_io::set_sockopt_int(fd, libc::SOL_SOCKET, opt, bytes).unwrap();
    }
}

/// A peer's half-close ends the recv side once the frames ahead of it are
/// delivered, and the reactor lets go of the connection; but the socket stays open
/// for as long as any holder keeps the connection, and closes with the last.
/// Observed from the partner end, so no fd number is probed.
#[test]
fn a_closed_connection_delivers_its_queue_and_closes_with_its_last_holder() {
    let r = make_reactor();
    let (conn, partner) = registered(&r);
    (&partner).write_all(&framed(b"last")).expect("write");
    partner.shutdown(std::net::Shutdown::Write).expect("half-close");

    let (last, end) = r.block_on(async { (conn.recv().await, conn.recv().await) });
    assert_eq!(last.expect("the queued frame").as_slice(), b"last");
    assert!(end.is_none(), "then recv reports the end");
    assert_eq!(Rc::strong_count(&conn), 1, "the reactor let go of the connection");
    assert!(
        !conn.is_gone(),
        "the end of the recv side does not finish the connection"
    );

    assert_eq!(
        read_nonblocking(&partner, 1),
        None,
        "the socket stays open while the connection is held"
    );
    drop(conn);
    assert_eq!(
        read_nonblocking(&partner, 1),
        Some(vec![]),
        "and closes with its last holder"
    );
}

/// A connection holds its slot under the cap for as long as it lives: at the
/// cap an accepted fd is refused and closed, and a dropped connection frees
/// its slot for the next.
#[test]
fn the_connection_cap_refuses_past_it_until_a_connection_drops() {
    let r = make_reactor_with(Limits { max_conns: 1, ..Limits::TEST });
    let (first, _a) = client_pair(&r);
    let (b, b_partner) = UnixStream::pair().expect("socketpair");
    assert!(r.client_conn(OwnedFd::from(b)).is_none(), "the cap refuses a second");
    assert_eq!(
        read_nonblocking(&b_partner, 1),
        Some(vec![]),
        "and closes the refused fd"
    );
    drop(first);
    client_pair(&r); // a dropped connection frees its slot
}

/// Under an egress owner, `retire` leaves the socket open to it and wakes it
/// once; `fail` shuts the socket down now, whoever owns egress, which ends an
/// armed recv — even one not yet submitted — so a silent peer cannot pin the
/// connection.
#[test]
fn retire_defers_the_shutdown_to_the_egress_owner_and_fail_ends_the_recv() {
    let r = make_reactor();
    let (conn, partner) = registered(&r);
    let (owner, mut woken) = chan::unbounded::<()>();
    conn.set_egress_owner(owner);

    conn.retire();
    assert!(woken.try_recv().is_some() && woken.try_recv().is_none(), "one wake");
    assert_eq!(read_nonblocking(&partner, 1), None, "the socket stays open");
    conn.fail();
    assert_eq!(read_nonblocking(&partner, 1), Some(vec![]), "fail shuts it down");
    assert!(
        poll_until(&r, || Rc::strong_count(&conn) == 1),
        "the shutdown ends the armed recv"
    );
}

/// A payload far larger than the socket buffers completes across many short
/// sends, each CQE retiring its op.
#[test]
fn send_owned_loops_until_full_payload_sent_over_socketpair() {
    let (r, conn, receiver) = egress_pair();
    shrink(&conn, &receiver, 8 * 1024);
    let payload_len = 200 * 1024;
    let drain_t = spawn_drain(receiver, payload_len);

    let sent = r.block_on(r.send_owned(&conn, SendBody::Pooled(PooledBuf(vec![0x5Au8; payload_len]))));
    // Closes the sender before the join: after a truncated send the drain's
    // blocking `read` would otherwise never return.
    drop(conn);
    let received = drain_t.join().expect("drain thread");
    assert_eq!((sent, received), (Ok(()), payload_len as u64));
    assert!(r.ops.borrow().is_empty());
}

/// A peer that never reads is evicted once a send makes no progress past the
/// deadline. The abandoned send keeps its connection and body — here a ring slot,
/// whose `release_cursor` shows when it drops — until its late CQE, since the
/// kernel may still read the body.
#[test]
fn send_owned_evicts_a_client_that_never_drains() {
    const TIMEOUT: Duration = Duration::from_millis(10);
    let limits = Limits {
        client_send_timeout: TIMEOUT,
        ..Limits::TEST
    };
    let r = make_reactor_with(limits);
    let (conn, receiver) = client_pair(&r);
    shrink(&conn, &receiver, 4 * 1024);
    // Far larger than both buffers, and nothing ever reads the other end: the
    // send stalls partway and only the deadline can end it.
    let (ring, slot) = ring_slot(200 * 1024);
    let held = ring.release_cursor(0);

    let start = Instant::now();
    let res = r.block_on(r.send_owned(&conn, SendBody::Slot(slot)));
    let elapsed = start.elapsed();
    assert_eq!(res, Err(PeerGone), "a client that never drains must be evicted");
    assert!(conn.is_gone(), "eviction finishes the connection");
    assert!(
        elapsed >= TIMEOUT,
        "evicted after {elapsed:?}, before the {TIMEOUT:?} deadline"
    );
    // No pass has drained since the eviction's.
    assert_eq!(
        (r.ops.borrow().len(), Rc::strong_count(&conn), ring.release_cursor(0)),
        (1, 2, held),
        "the abandoned send holds its connection and body"
    );

    // The eviction shut the socket down, so the abandoned send completes.
    assert!(
        poll_until(&r, || Rc::strong_count(&conn) == 1),
        "the abandoned send must complete and let go of the connection"
    );
    assert!(ring.release_cursor(0) > held, "and of its body");
}

/// A failed accept is retried once its backoff has passed: a bound socket that
/// does not listen refuses every accept, and the one in flight takes the first
/// connection after it starts to.
#[test]
fn accept_retries_after_a_failure() {
    let r = make_reactor();
    // SAFETY: a fresh fd from the kernel, owned by nothing else.
    let sock = unsafe { OwnedFd::from_raw_fd(libc::socket(libc::AF_INET, libc::SOCK_STREAM | libc::SOCK_CLOEXEC, 0)) };
    let mut name: libc::sockaddr_in = unsafe { std::mem::zeroed() };
    name.sin_family = libc::AF_INET as libc::sa_family_t;
    name.sin_addr.s_addr = u32::from(std::net::Ipv4Addr::LOCALHOST).to_be();
    let len = size_of::<libc::sockaddr_in>() as libc::socklen_t;
    assert_eq!(
        unsafe { libc::bind(sock.as_raw_fd(), (&raw const name).cast(), len) },
        0
    );
    let listener = leaked(sock);

    let accepted = Rc::new(RefCell::new(None));
    let (r2, a) = (r.clone(), Rc::clone(&accepted));
    r.spawn(async move { *a.borrow_mut() = Some(r2.accept(listener).await) });
    assert!(
        poll_until(&r, || !r.deadlines.borrow().is_empty()),
        "an accept the socket refuses backs off"
    );

    assert_eq!(unsafe { libc::listen(listener.as_raw_fd(), 1) }, 0);
    let addr = std::net::TcpListener::from(listener.try_clone_to_owned().expect("dup"))
        .local_addr()
        .expect("addr");
    let client = std::net::TcpStream::connect(addr).expect("connect");
    assert!(
        poll_until(&r, || accepted.borrow().is_some()),
        "the retry takes the connection"
    );
    let accepted = std::net::TcpStream::from(accepted.take().expect("just seen"));
    assert_eq!(accepted.peer_addr().ok(), client.local_addr().ok());
}
