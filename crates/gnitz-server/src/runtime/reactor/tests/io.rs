//! Client ingress: the frame deframer's phase machine and the global
//! inbound-memory budget every `RecvBuf` is charged against.

use super::super::test_support::*;
use super::super::*;

/// Drive `wire` into a fresh connection capped at `cap` and assert it is
/// refused: the connection reaped, and the global counter back at 0 — a refused
/// frame must never have been allocated, and a reaped one must be refunded.
fn assert_refused(cap: usize, max_payload: Option<usize>, wire: &[u8], why: &str) {
    unsafe {
        let (read_fd, write_fd) = stream_pair();
        let r = make_reactor();
        r.register_conn(read_fd);
        if let Some(limit) = max_payload {
            r.set_max_payload_len(read_fd, limit);
        }
        r.inbound().set_cap(cap);
        gnitz_store::foundation::posix_io::write_all_fd(write_fd, wire).expect("write");

        assert!(
            poll_until(&r, 20_000, || !r.inner.conns.borrow().contains_key(&read_fd)),
            "{why}"
        );
        assert_eq!(r.inbound().held(), 0, "{why}: budget must be reconciled");
        assert!(
            poll_recv_once(&r, read_fd).unwrap().is_none(),
            "{why}: recv after the close must yield None"
        );
        libc::close(write_fd); // read_fd was closed by reap
    }
}

/// The four ways an inbound frame is refused at its header, before any payload
/// byte is allocated.
#[test]
fn inbound_frames_are_refused_at_the_header() {
    let repeat = |payload: &[u8], n: usize| -> Vec<u8> { (0..n).flat_map(|_| framed(payload)).collect() };

    // frame_weight(100) = 100. Two frames = 200 held; the 3rd pushes 300 > 250.
    assert_refused(
        250,
        Some(1 << 20),
        &repeat(&[0xAB; 100], 3),
        "cumulative weight over cap",
    );

    // 1-byte payloads each weigh the 64-byte floor, so 64 frames = 4096 and the
    // 65th breaches. Without the floor 65 frames would weigh 65 B and never trip.
    assert_refused(
        4096,
        None,
        &repeat(&[0xCD], 65),
        "tiny-frame flood via the weight floor",
    );

    // No `set_max_payload_len`: the ceiling is still the 8-byte HELLO payload.
    assert_refused(
        usize::MAX,
        None,
        &framed(&[0u8; io::HELLO_PRE_HANDSHAKE_LEN + 1]),
        "first frame over the pre-handshake ceiling",
    );

    // A zero-length frame is the close sentinel, not a frame.
    assert_refused(usize::MAX, None, &0u32.to_le_bytes(), "the zero-length close sentinel");
}

/// In-flight (partial, un-completed) payloads are accounted, and a second
/// connection whose first frame would breach the full cap is refused — the
/// many-connection uncounted-in-flight OOM vector.
#[test]
fn inbound_cap_counts_in_flight_and_refuses_new_conn() {
    unsafe {
        let (read_fd, write_fd) = stream_pair();
        let r = make_reactor();
        r.register_conn(read_fd);
        r.set_max_payload_len(read_fd, 1 << 20);
        // Exactly one 10_000-byte in-flight buffer fits.
        r.inbound().set_cap(10_000);

        // Header claims 10_000 bytes but only 100 are delivered: the buffer
        // is malloc'd and counted, yet no frame completes (no MessageDone).
        let mut hdr_and_part = Vec::new();
        hdr_and_part.extend_from_slice(&10_000u32.to_le_bytes());
        hdr_and_part.extend_from_slice(&[0x11u8; 100]);
        gnitz_store::foundation::posix_io::write_all_fd(write_fd, &hdr_and_part).expect("write");

        let counted = poll_until(&r, 10_000, || r.inbound().held() == 10_000);
        assert!(counted, "in-flight buffer was not accounted");
        assert!(
            r.inner.conns.borrow().get(&read_fd).is_none_or(|c| c.q.queued() == 0),
            "no frame should have completed from a partial payload"
        );

        // Second connection whose first frame would breach the now-full cap.
        let (read_fd2, write_fd2) = stream_pair();
        r.register_conn(read_fd2);
        r.set_max_payload_len(read_fd2, 1 << 20);
        gnitz_store::foundation::posix_io::write_all_fd(write_fd2, &framed(&[0x22u8; 100])).expect("write");

        let refused = poll_until(&r, 10_000, || !r.inner.conns.borrow().contains_key(&read_fd2));
        assert!(refused, "over-cap second connection was not closed");
        // Refused connection allocated nothing; the first buffer is intact.
        assert_eq!(r.inbound().held(), 10_000);

        libc::close(write_fd);
        libc::close(write_fd2); // read_fd2 was closed by reap
        libc::close(read_fd); // conn1 never reaped (in-flight)
    }
}

/// Accounting balances: consumption decrements the counter, so total traffic
/// far above the cap never trips as long as the consumer keeps pace, and the
/// counter returns to 0 once the queue fully drains.
#[test]
fn inbound_cap_accounting_balances_on_consume() {
    let (read_fd, write_fd) = unsafe { stream_pair() };
    let r = Rc::new(make_reactor());
    r.register_conn(read_fd);
    r.set_max_payload_len(read_fd, 1 << 20);
    // Cap admits several frames; the pipeline holds ~1 at a time because
    // each frame is popped in the same tick it lands, so 10 frames/round of
    // 1_000-weight traffic (10_000 > cap) never trips.
    r.inbound().set_cap(5_000);

    let payload = vec![0x7Eu8; 1_000]; // frame_weight = 1_000
    for _round in 0..2 {
        let mut wire = Vec::new();
        for _ in 0..10 {
            wire.extend_from_slice(&framed(&payload));
        }
        gnitz_store::foundation::posix_io::write_all_fd(write_fd, &wire).expect("write");
        let r2 = Rc::clone(&r);
        r.block_on(async move {
            for _ in 0..10 {
                let buf = r2.recv(read_fd).await.expect("frame");
                assert_eq!(buf.as_slice().len(), 1_000);
            }
        });
    }
    assert_eq!(
        r.inbound().held(),
        0,
        "counter must return to 0 once every frame is consumed"
    );

    unsafe {
        libc::close(read_fd);
        libc::close(write_fd);
    }
}

// ─────────────────────────────────────────────────────────────────
// RecvState state-machine unit tests: its four transitions (NeedMore,
// HeaderDone, MessageDone, Disconnect) driven directly, with no io_uring.
// The policy `RecvQueue` layers on top is covered by the connection tests
// above.
// ─────────────────────────────────────────────────────────────────

/// One pass through the machine: a split header, a seeded header completing,
/// a split payload, and the reset `take_message` leaves behind. The zero-length
/// `Disconnect` transition is asserted at the observable layer above.
#[test]
fn recv_state_walks_header_then_payload() {
    let mut rs = io::RecvState::new();

    // 2 of 4 header bytes.
    assert!(matches!(rs.advance(2), io::RecvAdvance::NeedMore));
    assert_eq!(rs.remaining().1, 2, "remaining reflects the consumed header bytes");

    rs.seed_header(8);
    assert!(matches!(rs.advance(2), io::RecvAdvance::HeaderDone));

    let buf = unsafe { libc::malloc(8) as *mut u8 };
    rs.start_payload(io::RecvBuf::new(buf, 8, Rc::new(io::InboundBudget::new(usize::MAX))));
    assert!(matches!(rs.advance(5), io::RecvAdvance::NeedMore));
    assert!(matches!(rs.advance(3), io::RecvAdvance::MessageDone));

    // `take_message` yields the owning `RecvBuf` (which frees on drop).
    let ret = rs.take_message();
    assert_eq!((ret.ptr, ret.len), (buf, 8));
    assert_eq!(rs.remaining().1, 4, "take_message must reset to header phase");
}
