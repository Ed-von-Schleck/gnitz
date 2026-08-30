//! Client ingress: the frame deframer's phase machine and the global
//! inbound-memory budget every `RecvBuf` is charged against.

use super::super::test_support::*;
use super::super::*;

/// Cap trips: unconsumed frames whose cumulative weight passes the ceiling
/// close the connection at the breaching header (before malloc), and reap
/// returns the global counter to 0.
#[test]
fn inbound_cap_trips_and_reap_reconciles() {
    unsafe {
        let (read_fd, write_fd) = stream_pair();
        let r = make_reactor();
        r.register_conn(read_fd);
        r.set_max_payload_len(read_fd, 1 << 20);
        // frame_weight(100) = 100. Two frames = 200 held; the 3rd frame's
        // header pushes 200 + 100 = 300 > 250 and is refused before malloc.
        r.inbound().set_cap(250);
        let payload = vec![0xABu8; 100];
        let mut wire = Vec::new();
        for _ in 0..3 {
            wire.extend_from_slice(&framed(&payload));
        }
        gnitz_engine::foundation::posix_io::write_all_fd(write_fd, &wire).expect("write");

        let reaped = poll_until(&r, 10_000, || !r.inner.conns.borrow().contains_key(&read_fd));
        assert!(reaped, "cap-trip connection was never reaped");
        assert_eq!(
            r.inbound().held(),
            0,
            "reap must subtract the reaped connection's undrained share"
        );
        assert!(
            poll_recv_once(&r, read_fd).unwrap().is_none(),
            "recv after a cap-trip close must yield None"
        );

        libc::close(write_fd); // read_fd was closed by reap
    }
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
        gnitz_engine::foundation::posix_io::write_all_fd(write_fd, &hdr_and_part).expect("write");

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
        gnitz_engine::foundation::posix_io::write_all_fd(write_fd2, &framed(&[0x22u8; 100])).expect("write");

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
        gnitz_engine::foundation::posix_io::write_all_fd(write_fd, &wire).expect("write");
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

/// Tiny-frame floor: a flood of 1-byte payloads trips the cap after
/// ~CAP/64 frames (each weighs the 64-byte floor), not CAP — without the
/// floor, 65 one-byte frames weigh 65 B and would never trip.
#[test]
fn inbound_cap_tiny_frame_floor_trips_early() {
    unsafe {
        let (read_fd, write_fd) = stream_pair();
        let r = make_reactor();
        r.register_conn(read_fd);
        // 1-byte payloads ≤ HELLO_PRE_HANDSHAKE_LEN (8), so no
        // set_max_payload_len is needed.
        r.inbound().set_cap(4096);
        // 64 frames = 64 × 64 = 4096 held; the 65th frame's header would
        // make 4160 > 4096 and is refused.
        let mut wire = Vec::new();
        for _ in 0..65 {
            wire.extend_from_slice(&framed(&[0xCD]));
        }
        gnitz_engine::foundation::posix_io::write_all_fd(write_fd, &wire).expect("write");

        let reaped = poll_until(&r, 20_000, || !r.inner.conns.borrow().contains_key(&read_fd));
        assert!(reaped, "tiny-frame flood must trip the cap via the frame_weight floor");
        assert_eq!(
            r.inbound().held(),
            0,
            "reap must reconcile the counter after a tiny-frame trip"
        );

        libc::close(write_fd); // read_fd was closed by reap
    }
}

// ─────────────────────────────────────────────────────────────────
// Per-connection recv policy on the fd path: the pre-HELLO frame
// ceiling and the zero-length close sentinel. Both live in
// `RecvQueue::deliver`, shared with the TLS transport.
// ─────────────────────────────────────────────────────────────────

/// A first frame larger than the pre-handshake ceiling is refused at its
/// header — before any payload byte is allocated — and the connection dies.
#[test]
fn oversize_first_frame_is_refused_at_the_header() {
    unsafe {
        let (read_fd, write_fd) = stream_pair();
        let r = make_reactor();
        r.register_conn(read_fd);
        // No set_max_payload_len: the ceiling is still the 8-byte HELLO
        // payload size, so a 9-byte frame must be refused.
        let wire = framed(&[0u8; io::HELLO_PRE_HANDSHAKE_LEN + 1]);
        gnitz_engine::foundation::posix_io::write_all_fd(write_fd, &wire).expect("write");

        let reaped = poll_until(&r, 10_000, || !r.inner.conns.borrow().contains_key(&read_fd));
        assert!(
            reaped,
            "a frame over the per-connection ceiling must close the connection"
        );
        assert_eq!(r.inbound().held(), 0, "a refused frame must never have been allocated");

        libc::close(write_fd); // read_fd was closed by reap
    }
}

/// A zero-length frame is the close sentinel, not a frame: it closes the
/// connection instead of completing a message.
#[test]
fn zero_length_frame_closes_the_connection() {
    unsafe {
        let (read_fd, write_fd) = stream_pair();
        let r = make_reactor();
        r.register_conn(read_fd);
        gnitz_engine::foundation::posix_io::write_all_fd(write_fd, &0u32.to_le_bytes()).expect("write");

        let reaped = poll_until(&r, 10_000, || !r.inner.conns.borrow().contains_key(&read_fd));
        assert!(reaped, "the zero-length sentinel must close the connection");

        libc::close(write_fd); // read_fd was closed by reap
    }
}

// ─────────────────────────────────────────────────────────────────
// RecvState state-machine unit tests: its four transitions (NeedMore,
// HeaderDone, MessageDone, Disconnect) driven directly, with no io_uring.
// The policy `RecvQueue` layers on top is covered by the connection tests
// above.
// ─────────────────────────────────────────────────────────────────

#[test]
fn recv_state_partial_header_accumulates() {
    let mut rs = io::RecvState::new();
    // Feed 2 of 4 header bytes.
    assert!(matches!(rs.advance(2), io::RecvAdvance::NeedMore));
    let (_, rem) = rs.remaining();
    assert_eq!(rem, 2, "remaining must reflect the 2 consumed header bytes");
}

#[test]
fn recv_state_zero_payload_len_disconnects() {
    let mut rs = io::RecvState::new();
    // hdr_buf is all-zeros → payload_len = 0 → protocol violation.
    assert!(matches!(rs.advance(4), io::RecvAdvance::Disconnect));
}

#[test]
fn recv_state_payload_accumulates_then_message_done() {
    let mut rs = io::RecvState::new();
    rs.seed_header(8);
    assert!(matches!(rs.advance(4), io::RecvAdvance::HeaderDone));

    let buf = unsafe { libc::malloc(8) as *mut u8 };
    rs.start_payload(io::RecvBuf::new(buf, 8, Rc::new(io::InboundBudget::new(usize::MAX))));

    // Partial payload.
    assert!(matches!(rs.advance(5), io::RecvAdvance::NeedMore));
    // Remaining 3 bytes complete the message.
    assert!(matches!(rs.advance(3), io::RecvAdvance::MessageDone));

    // `take_message` yields the owning `RecvBuf` (frees on drop).
    let ret = rs.take_message();
    assert_eq!(ret.ptr, buf);
    assert_eq!(ret.len, 8);

    // After take_message the state must be back in header phase.
    let (_, rem) = rs.remaining();
    assert_eq!(rem, 4, "take_message must reset to header phase");
}
