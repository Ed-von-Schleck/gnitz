//! Client ingress: the global inbound-memory budget every `RecvBuf` is charged
//! against.

use std::io::{Read, Write};

use super::super::test_support::*;
use super::super::{Limits, Reactor};
use super::*;
use crate::runtime::test_support::try_poll_once;

/// A reactor with no W2M rings and an inbound cap of `cap`.
fn capped_reactor(cap: usize) -> Reactor {
    make_reactor_with(Limits { inbound_cap: cap, ..Limits::TEST })
}

/// `wire` into a fresh connection capped at `cap` finishes the connection without
/// delivering a frame: the frames ahead of the refusal are discarded and refunded
/// while the connection is still held, and the socket is shut down so the peer —
/// and any task parked in a send to it — sees the end now.
fn assert_refused(cap: usize, wire: &[u8], why: &str) {
    let r = capped_reactor(cap);
    let (conn, mut partner) = registered(&r);
    (&partner).write_all(wire).expect("write");

    assert!(poll_until(&r, || conn.is_gone()), "{why}");
    assert!(
        matches!(try_poll_once(conn.recv()), Some(None)),
        "{why}: a refusal discards the queue, and recv never parks after it"
    );
    assert_eq!(r.inbound.held(), 0, "{why}: budget must be reconciled");
    partner.set_nonblocking(true).expect("nonblocking");
    assert_eq!(
        partner.read(&mut [0u8; 1]).ok(),
        Some(0),
        "{why}: the partner reads EOF"
    );
}

/// The ways an inbound frame is refused at its header, before any payload byte
/// is allocated.
#[test]
fn inbound_frames_are_refused_at_the_header() {
    let repeat = |payload: &[u8], n: usize| -> Vec<u8> { (0..n).flat_map(|_| framed(payload)).collect() };

    // frame_weight(100) = 100. Two frames = 200 held; the 3rd pushes 300 > 250.
    assert_refused(250, &repeat(&[0xAB; 100], 3), "cumulative weight over cap");

    // 1-byte payloads each weigh the 64-byte floor, so 64 frames = 4096 and the
    // 65th breaches. Without the floor 65 frames would weigh 65 B and never trip.
    assert_refused(4096, &repeat(&[0xCD], 65), "tiny-frame flood via the weight floor");

    assert_refused(
        usize::MAX,
        &0u32.to_le_bytes(),
        "a zero-length prefix is a protocol violation",
    );

    let mut oversize = framed(&[0x42u8; 100]);
    oversize.extend_from_slice(&((gnitz_wire::MAX_FRAME_PAYLOAD + 1) as u32).to_le_bytes());
    assert_refused(usize::MAX, &oversize, "a prefix past the frame ceiling");
}

/// In-flight (partial, un-completed) payloads are accounted, and a second
/// connection whose first frame would breach the full cap is refused — the
/// many-connection uncounted-in-flight OOM vector.
#[test]
fn inbound_cap_counts_in_flight_and_refuses_new_conn() {
    // Exactly one 10_000-byte in-flight buffer fits.
    let r = capped_reactor(10_000);
    let (conn1, partner1) = registered(&r);

    // Header claims 10_000 bytes but only 100 are delivered: the buffer
    // is malloc'd and counted at header-parse time, yet no frame completes.
    let mut hdr_and_part = Vec::new();
    hdr_and_part.extend_from_slice(&10_000u32.to_le_bytes());
    hdr_and_part.extend_from_slice(&[0x11u8; 100]);
    (&partner1).write_all(&hdr_and_part).expect("write");

    let counted = poll_until(&r, || r.inbound.held() == 10_000);
    assert!(counted, "in-flight buffer was not accounted");
    assert!(
        conn1.try_recv().is_none(),
        "no frame should have completed from a partial payload"
    );

    // Second connection whose first frame would breach the now-full cap.
    let (conn2, partner2) = registered(&r);
    (&partner2).write_all(&framed(&[0x22u8; 100])).expect("write");

    assert!(
        poll_until(&r, || conn2.is_gone()),
        "over-cap second connection was not closed"
    );
    // Refused connection allocated nothing; the first buffer is intact.
    assert_eq!(r.inbound.held(), 10_000);
}

/// Accounting balances: consumption decrements the counter, so total traffic
/// far above the cap never trips as long as the consumer keeps pace, and the
/// counter returns to 0 once the queue fully drains.
#[test]
fn inbound_cap_accounting_balances_on_consume() {
    // One recv deframes a whole round and charges every frame in it, so the peak
    // is a round rather than a frame — and the cap below admits one.
    let r = capped_reactor(15_000);
    let (conn, partner) = registered(&r);

    let payload = vec![0x7Eu8; 1_000]; // frame_weight = 1_000
    for _round in 0..2 {
        let mut wire = Vec::new();
        for _ in 0..10 {
            wire.extend_from_slice(&framed(&payload));
        }
        (&partner).write_all(&wire).expect("write");
        let c = Rc::clone(&conn);
        r.block_on(async move {
            for _ in 0..10 {
                let buf = c.recv().await.expect("frame");
                assert_eq!(buf.as_slice().len(), 1_000);
            }
        });
    }
    assert_eq!(
        r.inbound.held(),
        0,
        "counter must return to 0 once every frame is consumed"
    );
}

/// `readable` resolves on a queued frame and on the end of the recv side, and
/// takes nothing: the frame is still there for `recv`.
#[test]
fn readable_reports_a_frame_or_the_end_and_takes_neither() {
    let r = capped_reactor(1 << 20);
    let (conn, partner) = registered(&r);
    assert!(try_poll_once(conn.readable()).is_none(), "nothing sent yet");

    let payload = [7u8; 16];
    (&partner)
        .write_all(&gnitz_wire::frame_len_prefix(payload.len()))
        .expect("write");
    (&partner).write_all(&payload).expect("write");
    assert!(
        poll_until(&r, || try_poll_once(conn.readable()).is_some()),
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
        poll_until(&r, || try_poll_once(conn.readable()).is_some()),
        "the peer is gone"
    );
}
