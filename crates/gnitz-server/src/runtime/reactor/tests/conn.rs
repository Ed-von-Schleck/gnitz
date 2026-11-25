//! Client connections: the frame queue's ceilings, the global inbound-memory
//! budget every `RecvBuf` is charged against and a connection's share of it,
//! the accept op, the send loop and its eviction deadline, how a connection's
//! recv side ends and its socket closes, and what a session changes of each;
//! and corked egress: what waits in a `Peer`'s cork, what it leaves behind, and
//! how a finished connection refuses it.

use std::io::Write;
use std::os::fd::{FromRawFd, OwnedFd};
use std::os::unix::net::UnixStream;
use std::time::Duration;

use super::super::test_support::*;
use super::*;
use gnitz_wire::framed;

/// A reactor over one W2M ring with an inbound cap of `cap`.
fn capped_reactor(cap: usize) -> Reactor {
    make_reactor_with(Limits { inbound_cap: cap, ..Limits::TEST })
}

/// The connection of a [`Peer::over_socketpair`] past its handshake, and the
/// other end.
fn admitted(r: Reactor) -> (Rc<ClientConn>, UnixStream) {
    let (peer, partner) = Peer::over_socketpair(r);
    peer.admit_full_frames();
    (peer.conn, partner)
}

/// Write what of `wire[*written..]` the nonblocking `s` takes now.
fn write_some(mut s: &UnixStream, wire: &[u8], written: &mut usize) {
    match s.write(&wire[*written..]) {
        Ok(n) => *written += n,
        Err(e) => assert_eq!(e.kind(), std::io::ErrorKind::WouldBlock),
    }
}

/// Whether `conn`'s recv is held off past its share.
fn is_paused(r: Reactor, conn: &Rc<ClientConn>) -> bool {
    let recvs = r.conns.recvs.borrow();
    r.conns
        .paused
        .borrow()
        .iter()
        .any(|&key| Rc::ptr_eq(&recvs[key].conn, conn))
}

/// A session whose records are its plaintext, sealed behind what it owes, and
/// whose goodbye is [`Passthrough::BYE`].
#[derive(Default)]
struct Passthrough {
    owed: RefCell<Vec<u8>>,
    /// The length of every seal's plaintext.
    sealed: RefCell<Vec<usize>>,
}

impl Passthrough {
    const BYE: &[u8] = b"bye";
}

impl Session for Passthrough {
    fn ingest(&self, cipher: &[u8], plain: &mut dyn FnMut(&[u8]) -> Result<(), RecvEnd>) -> Result<(), RecvEnd> {
        plain(cipher)
    }

    fn owes(&self) -> bool {
        !self.owed.borrow().is_empty()
    }

    fn seal(&self, plain: &[u8]) -> Vec<u8> {
        self.sealed.borrow_mut().push(plain.len());
        let mut records = self.owed.take();
        records.extend_from_slice(plain);
        records
    }

    fn close(&self) -> Vec<u8> {
        self.owed.borrow_mut().extend_from_slice(Self::BYE);
        self.seal(&[])
    }
}

/// Frames deframed, and why the feed ended, if it did.
pub(super) type Deframed = (Vec<Vec<u8>>, Option<RecvEnd>);

/// The frames `stream` deframes to under a `cap`-byte budget, fed `step` bytes a
/// read, and why the feed ended, if it did.
pub(super) fn deframe(stream: &[u8], step: usize, admitted: bool, cap: usize) -> Deframed {
    let mut q = FrameSink::new(Budget::new(cap));
    if admitted {
        q.max_frame = MAX_FRAME_PAYLOAD;
    }
    let mut deframer = Deframer::default();
    let mut frames = Vec::new();
    for read in stream.chunks(step) {
        let fed = q.feed(&mut deframer, read);
        while let Some(f) = q.try_recv() {
            frames.push(f.as_slice().to_vec());
        }
        if let Err(end) = fed {
            return (frames, Some(end));
        }
    }
    (frames, None)
}

/// A stream fed whole: the frames ahead of a refusal are delivered, and the
/// refusal is decided at the header, before any payload byte is allocated.
#[test]
fn a_queue_deframes_until_a_header_refuses() {
    let past_handshake = framed(&[0x5A; HANDSHAKE_FRAME_MAX + 1]);
    for (cap, admitted, wire, frames, end) in [
        // a 100-byte frame weighs 100: two frames hold 200, the third would pass 250.
        (
            250,
            true,
            framed(&[0xAB; 100]).repeat(3),
            2,
            Some(RecvEnd::CapBreach { want: 100 }),
        ),
        // A 1-byte payload weighs the 64-byte floor: 64 frames fill 4096.
        (
            4096,
            true,
            framed(&[0xCD]).repeat(65),
            64,
            Some(RecvEnd::CapBreach { want: 64 }),
        ),
        (
            usize::MAX,
            true,
            0u32.to_le_bytes().to_vec(),
            0,
            Some(RecvEnd::Protocol),
        ),
        (usize::MAX, false, past_handshake.clone(), 0, Some(RecvEnd::Oversize)),
        (usize::MAX, true, past_handshake, 1, None),
    ] {
        let (got, got_end) = deframe(&wire, wire.len(), admitted, cap);
        assert_eq!((got.len(), got_end), (frames, end), "cap={cap} admitted={admitted}");
    }
}

/// A refusal finishes the connection without delivering a frame: the frames
/// ahead of it are discarded and refunded while the connection is still held,
/// and the socket is shut down so the peer — and any task parked in a send to
/// it — sees the end now. A framing violation and a cap breach alike.
#[test]
fn a_refusal_discards_the_queue_and_shuts_the_socket_down() {
    for (cap, wire) in [
        (usize::MAX, 0u32.to_le_bytes().to_vec()),
        (250, framed(&[0xAB; 100]).repeat(3)),
    ] {
        let r = capped_reactor(cap);
        let (conn, partner) = admitted(r);
        (&partner).write_all(&wire).expect("write");

        assert!(poll_until(r, || conn.is_gone()), "cap={cap}");
        assert!(
            matches!(try_poll_once(conn.recv()), Some(None)),
            "cap={cap}: recv never parks after a refusal"
        );
        assert_eq!(r.conns.inbound.held(), 0, "cap={cap}");
        assert_eq!(
            read_nonblocking(&partner, 1),
            Some(vec![]),
            "cap={cap}: the partner reads EOF"
        );
    }
}

/// In-flight (partial, un-completed) payloads are accounted, and a second
/// connection whose first frame would breach the full cap is refused — the
/// many-connection uncounted-in-flight OOM vector.
#[test]
fn inbound_cap_counts_in_flight_and_refuses_new_conn() {
    // Exactly one 10_000-byte in-flight buffer fits.
    let r = capped_reactor(10_000);
    let (conn1, partner1) = admitted(r);

    // The header claims 10_000 bytes but only 100 arrive: the buffer is charged
    // at the header, yet no frame completes.
    let mut hdr_and_part = 10_000u32.to_le_bytes().to_vec();
    hdr_and_part.extend_from_slice(&[0x11u8; 100]);
    (&partner1).write_all(&hdr_and_part).expect("write");

    assert!(
        poll_until(r, || r.conns.inbound.held() == 10_000),
        "in-flight buffer was not accounted"
    );
    assert!(!conn1.is_readable(), "no frame completes from a partial payload");

    let (conn2, partner2) = admitted(r);
    (&partner2).write_all(&framed(&[0x22u8; 100])).expect("write");
    assert!(
        poll_until(r, || conn2.is_gone()),
        "over-cap second connection was not closed"
    );
    assert_eq!(
        r.conns.inbound.held(),
        10_000,
        "the refused connection allocated nothing"
    );
}

/// Consumption refunds the budget, so traffic far above the cap never trips while
/// the consumer keeps pace, and the budget returns to 0 once the queue drains.
#[test]
fn inbound_cap_accounting_balances_on_consume() {
    // One recv deframes a whole round and charges every frame in it, so the peak
    // is a round rather than a frame — and the cap below admits one.
    let r = capped_reactor(15_000);
    let (conn, partner) = admitted(r);

    let wire = framed(&[0x7Eu8; 1_000]).repeat(10); // each frame weighs 1_000
    for _round in 0..2 {
        (&partner).write_all(&wire).expect("write");
        r.block_on(async {
            for _ in 0..10 {
                assert_eq!(conn.recv().await.expect("frame").as_slice().len(), 1_000);
            }
        });
    }
    assert_eq!(r.conns.inbound.held(), 0, "every frame consumed");
}

/// A connection past its share of the inbound budget is received from no
/// further until its task takes frames back under the share, so what it has
/// queued never refuses another connection's frame; and every frame it sent
/// arrives once its task drains them.
#[test]
fn a_connection_past_its_share_holds_off_and_refuses_no_other() {
    // Past the carry, so one recv completes at most one frame and starts at most
    // one more.
    const FRAME: usize = 40_000;
    const FRAMES: usize = 9;
    const SHARE: usize = 200_000;
    // All nine fit under the cap, but leave no room beside them for the other
    // connection's frame.
    let r = make_reactor_with(Limits {
        inbound_cap: 400_000,
        conn_inbound_share: SHARE,
        ..Limits::TEST
    });
    let (hog, hog_partner) = admitted(r);
    let (other, other_partner) = admitted(r);
    hog_partner.set_nonblocking(true).expect("nonblocking");
    let wire = framed(&[0x33; FRAME]).repeat(FRAMES);
    let mut written = 0;

    assert!(
        poll_until(r, || {
            write_some(&hog_partner, &wire, &mut written);
            is_paused(r, &hog)
        }),
        "the hog's recv is never held off"
    );
    // One recv past the share starts at most one frame.
    let held = r.conns.inbound.held();
    assert!(held <= SHARE + FRAME, "the hog holds {held} B");

    let payload = [0x44; 50_000];
    (&other_partner).write_all(&framed(&payload)).expect("write");
    let got = r.block_on(other.recv()).expect("the other connection's frame");
    assert_eq!(got.as_slice(), payload);
    drop(got);

    let drained = Rc::new(Cell::new(0));
    let (n, conn) = (Rc::clone(&drained), Rc::clone(&hog));
    r.spawn(async move {
        while let Some(frame) = conn.recv().await {
            assert_eq!(frame.as_slice().len(), FRAME);
            n.update(|v| v + 1);
        }
    });
    assert!(
        poll_until(r, || {
            write_some(&hog_partner, &wire, &mut written);
            drained.get() == FRAMES
        }),
        "the hog's frames are delivered once its task takes them"
    );
    assert!(!hog.is_gone() && !other.is_gone());
    assert_eq!(r.conns.inbound.held(), 0);
    hog.retire(); // ends the draining task
    r.block_until_idle();
}

/// A frame past the share is received whole: with nothing queued, no take of
/// the task's could bring the connection under it.
#[test]
fn a_frame_past_the_share_is_received_whole() {
    let r = make_reactor_with(Limits {
        conn_inbound_share: 1_000,
        ..Limits::TEST
    });
    let (conn, partner) = admitted(r);
    partner.set_nonblocking(true).expect("nonblocking");
    let payload = [0x55; 100_000];
    let wire = framed(&payload);
    let (mut written, mut got) = (0, None);
    assert!(
        poll_until(r, || {
            write_some(&partner, &wire, &mut written);
            got = try_poll_once(conn.recv()).flatten();
            got.is_some()
        }),
        "the frame never completes"
    );
    assert_eq!(got.expect("the frame").as_slice(), payload);
}

/// A connection retired while its recv is held off has no recv to complete:
/// the reactor lets go of it at its next submit, refunding its queue.
#[test]
fn a_held_off_connection_retired_is_let_go_of() {
    let r = make_reactor_with(Limits { conn_inbound_share: 0, ..Limits::TEST });
    let (conn, partner) = admitted(r);
    (&partner).write_all(&framed(b"queued")).expect("write");
    assert!(
        poll_until(r, || is_paused(r, &conn)),
        "one queued frame is past a zero share"
    );
    conn.retire();
    r.tick();
    assert_eq!(Rc::strong_count(&conn), 1, "the reactor let go of the connection");
    assert_eq!(r.conns.inbound.held(), 0);
}

/// `readable` resolves on a queued frame and on the end of the recv side, and
/// takes nothing: the frame is still there for `recv`.
#[test]
fn readable_reports_a_frame_or_the_end_and_takes_neither() {
    let r = make_reactor();
    let (conn, partner) = admitted(r);
    assert!(try_poll_once(conn.readable()).is_none(), "nothing sent yet");

    let payload = [7u8; 16];
    (&partner).write_all(&framed(&payload)).expect("write");
    assert!(
        poll_until(r, || try_poll_once(conn.readable()).is_some()),
        "a frame is queued"
    );
    let frame = try_poll_once(conn.recv())
        .flatten()
        .expect("the frame readable reported");
    assert_eq!(frame.as_slice(), payload);
    drop(frame);
    assert!(try_poll_once(conn.readable()).is_none(), "and nothing behind it");

    drop(partner);
    assert!(
        poll_until(r, || try_poll_once(conn.readable()).is_some()),
        "the peer is gone"
    );
}

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
    let (conn, partner) = admitted(r);
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
/// cap an accepted fd is refused and closed, and a connection that ends frees
/// its slot for the next.
#[test]
fn the_connection_cap_refuses_past_it_until_a_connection_ends() {
    let r = make_reactor_with(Limits { max_conns: 1, ..Limits::TEST });
    let (first, _a) = admitted(r);
    let (b, b_partner) = UnixStream::pair().expect("socketpair");
    assert!(r.admit(OwnedFd::from(b)).is_none(), "the cap refuses a second");
    assert_eq!(
        read_nonblocking(&b_partner, 1),
        Some(vec![]),
        "and closes the refused fd"
    );
    first.fail();
    assert!(
        poll_until(r, || Rc::strong_count(&first) == 1),
        "the shutdown ends the recv"
    );
    drop(first);
    admitted(r); // an ended connection frees its slot
}

/// `retire` leaves the socket open; `fail` shuts it down now, which ends an
/// armed recv — even one not yet submitted — so a silent peer cannot pin the
/// connection.
#[test]
fn retire_leaves_the_socket_open_and_fail_ends_the_recv() {
    let r = make_reactor();
    let (conn, partner) = admitted(r);

    conn.retire();
    assert_eq!(read_nonblocking(&partner, 1), None, "the socket stays open");
    conn.fail();
    assert_eq!(read_nonblocking(&partner, 1), Some(vec![]), "fail shuts it down");
    assert!(
        poll_until(r, || Rc::strong_count(&conn) == 1),
        "the shutdown ends the armed recv"
    );
}

/// A recv in flight when its connection retires completes into nothing: its
/// bytes are dropped uncharged and the reactor lets go of the connection.
#[test]
fn a_recv_landing_after_retire_is_discarded() {
    let r = make_reactor();
    let (conn, mut partner) = admitted(r);
    r.tick(); // the recv is submitted

    conn.retire();
    partner.write_all(&gnitz_wire::framed(b"late")).expect("write");
    assert!(
        poll_until(r, || Rc::strong_count(&conn) == 1),
        "the recv's completion releases the connection"
    );
    assert_eq!(r.conns.inbound.held(), 0, "the late bytes were never charged");
    assert_eq!(
        try_poll_once(conn.recv()).map(|f| f.is_none()),
        Some(true),
        "the late frame was not queued"
    );
}

/// Under a session a recv sends what the session owes until the client's
/// first frame, deframes the plaintext the session hands over across recvs, a
/// send leaves sealed turn by turn, in order, and a close shuts the socket down
/// behind the goodbye.
#[test]
fn a_session_seals_what_is_sent_and_closes_behind_its_goodbye() {
    let r = make_reactor();
    let session = Rc::new(Passthrough::default());
    let (accepted, mut partner) = accepted_pair(r);
    let peer = Rc::new(r.connect(accepted, Some(session.clone())));

    session.owed.borrow_mut().extend_from_slice(b"flight");
    let received = Rc::new(Cell::new(false));
    let (p, done) = (Rc::clone(&peer), Rc::clone(&received));
    r.spawn(async move {
        let frame = p.recv().await.expect("the partner is open");
        assert_eq!(frame.as_slice(), b"hello");
        done.set(true);
    });
    assert!(poll_until(r, || read_nonblocking(&partner, 64).is_some_and(|b| b == b"flight")));
    assert!(!received.get(), "no frame is queued yet");
    let hello = framed(b"hello");
    let (head, tail) = hello.split_at(hello.len() - 2);
    partner.write_all(head).expect("write");
    assert!(
        poll_until(r, || r.conns.inbound.held() > 0),
        "the session's plaintext reaches the queue"
    );
    assert!(!received.get(), "the frame is not whole yet");
    partner.write_all(tail).expect("write");
    assert!(poll_until(r, || received.get()));

    // Read concurrently: the body outgrows the socketpair's buffers.
    let body: Vec<u8> = (0..2 * SEAL_TURN_BYTES + 5).map(|i| (i % 251) as u8).collect();
    let (p, sent) = (Rc::clone(&peer), body.clone());
    r.spawn(async move { p.send_raw(SendBody::Owned(sent)).await.expect("sent") });
    let mut got = Vec::new();
    assert!(poll_until(r, || {
        while let Some(bytes) = read_nonblocking(&partner, 64 * 1024) {
            got.extend_from_slice(&bytes);
        }
        got.len() >= body.len()
    }));
    assert!(got == body, "the body arrives as it was sent");
    // The flight, then the body.
    assert_eq!(*session.sealed.borrow(), [0, SEAL_TURN_BYTES, SEAL_TURN_BYTES, 5]);

    r.block_on(peer.close());
    assert_eq!(read_nonblocking(&partner, 64), Some(Passthrough::BYE.to_vec()));
    assert_eq!(read_nonblocking(&partner, 1), Some(vec![]), "then the shutdown");
}

/// A payload far larger than the socket buffers completes across many short
/// sends, each CQE retiring its op.
#[test]
fn send_owned_loops_until_full_payload_sent_over_socketpair() {
    let r = make_reactor();
    let (conn, receiver) = admitted(r);
    shrink(&conn, &receiver, 8 * 1024);
    let payload_len = 200 * 1024;
    let drain_t = spawn_drain(receiver, payload_len);

    let sent = r.block_on(r.send_owned(&conn, SendBody::Owned(vec![0x5Au8; payload_len])));
    // Shuts the socket down before the join: after a truncated send the
    // drain's blocking `read` would otherwise never return.
    conn.fail();
    let received = drain_t.join().expect("drain thread");
    assert_eq!((sent, received), (Ok(()), payload_len as u64));
    assert_eq!(r.ops.len(), 0);
}

/// An empty body is sent with no op, and a send the socket refuses finishes
/// the connection.
#[test]
fn send_owned_skips_an_empty_body_and_fails_the_connection_on_a_refused_send() {
    let r = make_reactor();
    let (conn, partner) = admitted(r);
    let empty = r.send_owned(&conn, SendBody::Owned(Vec::new()));
    assert_eq!(try_poll_once(empty), Some(Ok(())));
    assert_eq!(r.ops.len(), 0, "no op was pushed");
    assert_eq!(read_nonblocking(&partner, 1), None, "nothing was sent");

    drop(partner);
    let sent = r.block_on(r.send_owned(&conn, SendBody::Owned(vec![1])));
    assert_eq!(sent, Err(PeerGone));
    assert!(conn.is_gone());
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
    let (conn, receiver) = admitted(r);
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
        (r.ops.len(), Rc::strong_count(&conn), ring.release_cursor(0)),
        (1, 3, held),
        "the abandoned send holds its connection and body, beside the recv"
    );

    // The eviction shut the socket down, so the abandoned send completes.
    assert!(
        poll_until(r, || Rc::strong_count(&conn) == 1),
        "the abandoned send must complete and let go of the connection"
    );
    assert!(ring.release_cursor(0) > held, "and of its body");
}

/// A failed accept is retried once its backoff has passed: a socket that does
/// not listen refuses every accept, and the one in flight takes the first
/// connection after it starts to. `peer_addr` names the TCP peer through a
/// borrow that leaves the fd open, and is `None` for an AF_UNIX peer.
#[test]
fn accept_retries_after_a_failure_and_names_its_peer() {
    let r = make_reactor();
    // SAFETY: a fresh fd from the kernel, owned by nothing else.
    let sock = unsafe { OwnedFd::from_raw_fd(libc::socket(libc::AF_INET, libc::SOCK_STREAM | libc::SOCK_CLOEXEC, 0)) };
    let listener = leaked(sock);

    let accepted = Rc::new(RefCell::new(None));
    let a = Rc::clone(&accepted);
    r.spawn(async move { *a.borrow_mut() = Some(r.accept(listener).await) });
    assert!(
        poll_until(r, || r.timers.earliest().is_some()),
        "an accept the socket refuses backs off"
    );

    // An unbound socket's listen binds it to an ephemeral port.
    assert_eq!(unsafe { libc::listen(listener.as_raw_fd(), 1) }, 0);
    let port = std::net::TcpListener::from(listener.try_clone_to_owned().expect("dup"))
        .local_addr()
        .expect("addr")
        .port();
    let client = std::net::TcpStream::connect((std::net::Ipv4Addr::LOCALHOST, port)).expect("connect");
    assert!(
        poll_until(r, || accepted.borrow().is_some()),
        "the retry takes the connection"
    );
    let conn = r.connect(accepted.take().expect("just seen"), None);
    for _ in 0..2 {
        assert_eq!(conn.peer_addr(), client.local_addr().ok(), "the fd stays open");
    }
    assert_eq!(
        Peer::over_socketpair(r).0.peer_addr(),
        None,
        "an AF_UNIX peer has no address"
    );
}

/// A slot body held across a wait is copied out of its ring when the ring is
/// given back meanwhile; the wait goes on, and the body's bytes are unchanged.
#[test]
fn a_held_body_is_copied_out_when_its_ring_is_given_back() {
    let (r, mut writers) = reactor_with_rings(1);
    let mut body = SendBody::Slot(writers[0].publish_slot(&r.w2m, 0, 1024));
    let wire = body.bytes().to_vec();
    let held = r.w2m.release_cursor(0);

    let (tx, rx) = oneshot::channel::<u8>();
    let done: Rc<RefCell<Option<(u8, SendBody)>>> = Rc::default();
    let out = Rc::clone(&done);
    r.spawn(async move {
        let got = body.holding(rx).await;
        *out.borrow_mut() = Some((got, body));
    });
    r.tick();
    assert_eq!(r.w2m.release_cursor(0), held, "the wait holds the slot");

    writers[0].name_shortage();
    assert!(
        poll_until(r, || r.w2m.release_cursor(0) > held),
        "the give-back copies the body out"
    );
    assert!(done.borrow().is_none(), "while the wait goes on");
    tx.send(7);
    assert!(poll_until(r, || done.borrow().is_some()));
    let (got, body) = done.take().expect("just seen");
    assert_eq!(got, 7);
    assert_eq!(body.bytes(), &wire[..]);
}

/// A send of a ring slot to a client that has stopped reading holds the slot
/// only until its ring is given back: then the send is cut short, the slot
/// copied out and released, and the send goes on from the copy.
#[test]
fn a_stalled_send_gives_its_slot_back_with_its_ring() {
    let (r, mut writers) = reactor_with_rings(1);
    let (conn, receiver) = admitted(r);
    shrink(&conn, &receiver, 4 * 1024);
    let slot = writers[0].publish_slot(&r.w2m, 0, 24 * 1024);
    let wire = slot.frame_bytes().to_vec();
    let held = r.w2m.release_cursor(0);

    let done = Rc::new(Cell::new(None));
    let (out, c) = (Rc::clone(&done), Rc::clone(&conn));
    r.spawn(async move { out.set(Some(r.send_owned(&c, SendBody::Slot(slot)).await)) });
    for _ in 0..100 {
        r.tick();
    }
    assert_eq!(
        (done.get(), r.w2m.release_cursor(0)),
        (None, held),
        "the send waits on the client, holding its slot"
    );

    writers[0].name_shortage();
    assert!(
        poll_until(r, || r.w2m.release_cursor(0) > held),
        "the give-back frees the slot"
    );
    assert_eq!(done.get(), None, "while the send goes on");

    let reader = spawn_read(receiver, wire.len());
    assert!(
        poll_until(r, || done.get().is_some()),
        "the send finishes from the copy"
    );
    assert_eq!(done.get(), Some(Ok(())));
    assert!(
        reader.join().expect("reader") == wire,
        "the client reads the frame as it was published"
    );
}

/// `send` corks a body that fits beside the cork, ships the cork first when the
/// body would overflow it, and sends a body over half the ceiling alone, a ring
/// slot and a shared body alike; the bytes reach the wire in call order
/// whichever it does.
#[test]
fn send_corks_what_fits_and_never_reorders() {
    const CAP: usize = COALESCE_MAX_BYTES;
    // What the send itself puts on the wire.
    enum Ships {
        Nothing,
        Cork,
        Both,
    }
    let rows = [
        (0, 64, Ships::Nothing),
        (128, 64, Ships::Nothing),
        (0, CAP, Ships::Both),
        (128, CAP, Ships::Both),
        (CAP - 128, CAP / 4, Ships::Cork),
    ];
    for ((pre, pad, ships), shared) in rows.iter().flat_map(|row| [(row, false), (row, true)]) {
        let (pre, pad) = (*pre, *pad);
        let (_ring, slot) = ring_slot(pad);
        let all = [vec![0x5Au8; pre], slot.frame_bytes().to_vec()].concat();
        let body = if shared {
            SendBody::Shared(Rc::new(all[pre..].to_vec()))
        } else {
            SendBody::Slot(slot)
        };
        let r = make_reactor();
        let (mut peer, receiver) = Peer::over_socketpair(r);
        peer.cork(&all[..pre]);
        r.block_on(peer.send(body)).expect("an open peer");

        let wire = read_nonblocking(&receiver, 256 * 1024).unwrap_or_default();
        let (want_wire, want_corked) = match ships {
            Ships::Nothing => (&[][..], all.len()),
            Ships::Cork => (&all[..pre], all.len() - pre),
            Ships::Both => (&all[..], 0),
        };
        assert_eq!(
            (&wire[..], peer.corked_len()),
            (want_wire, want_corked),
            "pre={pre} pad={pad} shared={shared}"
        );

        r.block_on(peer.flush_egress()).expect("an open peer");
        let rest = read_nonblocking(&receiver, 256 * 1024).unwrap_or_default();
        assert_eq!(
            [wire, rest].concat(),
            all,
            "pre={pre} pad={pad} shared={shared}: call order"
        );
    }
}

/// Corking is unbounded on its own; `flush_if_full` ships the cork once it
/// reaches the ceiling, and not a byte before.
#[test]
fn flush_if_full_ships_only_a_full_cork() {
    for corked in [COALESCE_MAX_BYTES - 1, COALESCE_MAX_BYTES] {
        let r = make_reactor();
        let (mut peer, receiver) = Peer::over_socketpair(r);
        peer.cork(&vec![0x3Cu8; corked]);
        r.block_on(peer.flush_if_full()).expect("an open peer");
        let left = peer.corked_len();
        let wire = read_nonblocking(&receiver, 64 * 1024).map_or(0, |b| b.len());
        let full = corked == COALESCE_MAX_BYTES;
        assert_eq!(
            (wire, left),
            if full { (corked, 0) } else { (0, corked) },
            "corked={corked}"
        );
    }
}

/// A corked frame reports a gone peer at its flush; the failure finishes the
/// peer, so every later send and flush refuses, nothing stays corked, and no
/// further request is handed out.
#[test]
fn a_failed_send_finishes_the_peer() {
    let (_big_ring, big) = ring_slot(COALESCE_MAX_BYTES);
    let (_small_ring, small) = ring_slot(64);

    let r = make_reactor();
    let (mut peer, receiver) = Peer::over_socketpair(r);
    drop(receiver);

    r.block_on(async {
        assert_eq!(
            peer.send(SendBody::Slot(small)).await,
            Ok(()),
            "a small slot is corked, not sent"
        );
        assert_eq!(peer.flush_egress().await, Err(PeerGone), "its flush fails");
        assert_eq!(peer.corked_len(), 0);
        assert_eq!(peer.send(SendBody::Slot(big)).await, Err(PeerGone));
        assert_eq!(peer.flush_if_full().await, Err(PeerGone));
        assert_eq!(peer.flush_egress().await, Err(PeerGone));
        assert!(
            peer.next_request().await.is_none(),
            "no request follows a finished peer"
        );
    });
}

/// While a request is already queued `next_request` hands it out and leaves the
/// cork alone, so a pipelined run of replies leaves as one send.
#[test]
fn next_request_keeps_the_cork_while_requests_are_queued() {
    let r = make_reactor();
    let (mut peer, partner) = Peer::over_socketpair(r);
    (&partner)
        .write_all(&[framed(b"a"), framed(b"b")].concat())
        .expect("write");

    r.block_on(async {
        peer.next_request().await.expect("the first request");
        peer.cork(b"reply");
        peer.next_request().await.expect("the second request, already queued");
    });

    assert_eq!(peer.corked_len(), 5, "the reply stays corked");
    assert!(read_nonblocking(&partner, 64).is_none(), "nothing shipped");
}

/// A client that closed its sending side is still answered: `next_request`
/// ships what is corked before it reports the last request taken.
#[test]
fn next_request_ships_to_a_half_closed_client() {
    let r = make_reactor();
    let (mut peer, partner) = Peer::over_socketpair(r);
    (&partner).write_all(&framed(b"a")).expect("write");
    partner.shutdown(std::net::Shutdown::Write).expect("shutdown");

    r.block_on(peer.next_request()).expect("the one request");
    assert!(poll_until(r, || peer.conn.is_readable()), "the close lands");
    peer.cork(b"reply");
    assert!(r.block_on(peer.next_request()).is_none(), "no request follows it");

    assert_eq!(read_nonblocking(&partner, 64).as_deref(), Some(&b"reply"[..]));
}

/// With nothing queued, `next_request` ships what is corked before it parks on
/// the client.
#[test]
fn next_request_ships_before_parking() {
    let r = make_reactor();
    let (mut peer, receiver) = Peer::over_socketpair(r);
    let frame = vec![0x6Bu8; 300];
    peer.cork(&frame);

    let timer = r.sleep(Duration::from_millis(50));
    let parked = matches!(r.block_on(select2(peer.next_request(), timer)), Either::B(()));

    assert!(parked, "no request is queued, so the timer wins");
    assert_eq!(
        read_nonblocking(&receiver, 4096).expect("bytes on the wire"),
        frame,
        "the corked frame left before the park"
    );
}

/// A fenced peer sends nothing until the level reaches the fence, the highest
/// one raised — not its cork's flush, nor a body too large to cork — and then
/// everything, in call order. A fence the level has passed holds nothing back.
#[test]
fn a_fence_holds_every_send_until_its_level_reaches_it() {
    let r = make_reactor();
    let (mut peer, receiver) = Peer::over_socketpair(r);
    let level: &'static Level = Box::leak(Box::default());
    level.raise(3);
    peer.fence(level, 3);
    peer.cork(b"open");
    r.block_on(peer.flush_egress()).expect("an open peer");
    assert_eq!(
        read_nonblocking(&receiver, 64).as_deref(),
        Some(&b"open"[..]),
        "a reached fence holds nothing back"
    );

    peer.cork(b"ack");
    peer.fence(level, 5);
    peer.fence(level, 4);
    let large = Rc::new(vec![0x42u8; COALESCE_MAX_BYTES]);
    let mut sending = Box::pin(peer.send(SendBody::Shared(Rc::clone(&large))));
    level.raise(4);
    assert!(
        try_poll_once(sending.as_mut()).is_none(),
        "the level is below the fence"
    );
    r.tick();
    assert!(
        read_nonblocking(&receiver, 64).is_none(),
        "neither the cork nor the body left"
    );

    let wire = spawn_read(receiver, 3 + COALESCE_MAX_BYTES);
    level.raise(5);
    r.block_on(sending).expect("an open peer");
    let want = [&b"ack"[..], &large].concat();
    assert!(wire.join().unwrap() == want, "both left, in call order");
}

/// A fence holds back sends, not requests: a request already queued is handed
/// out while what is corked waits behind the fence.
#[test]
fn a_fenced_peer_still_hands_out_a_queued_request() {
    let r = make_reactor();
    let (mut peer, partner) = Peer::over_socketpair(r);
    (&partner)
        .write_all(&[framed(b"a"), framed(b"b")].concat())
        .expect("write");
    let level: &'static Level = Box::leak(Box::default());

    r.block_on(peer.next_request()).expect("the first request");
    assert!(peer.request_queued(), "the second is queued behind it");
    peer.cork(b"ack");
    peer.fence(level, 1);
    r.block_on(peer.next_request())
        .expect("the second request, with the fence up");
    assert!(
        read_nonblocking(&partner, 64).is_none(),
        "the ACK waits behind the fence"
    );

    let mut parked = Box::pin(peer.next_request());
    assert!(try_poll_once(parked.as_mut()).is_none(), "no request is queued");
    r.tick();
    assert!(
        read_nonblocking(&partner, 64).is_none(),
        "the flush ahead of the park waits"
    );
    level.raise(1);
    let mut wire = None;
    assert!(poll_until(r, || {
        assert!(try_poll_once(parked.as_mut()).is_none(), "no request is queued");
        wire = read_nonblocking(&partner, 64);
        wire.is_some()
    }));
    assert_eq!(wire.as_deref(), Some(&b"ack"[..]));
}
