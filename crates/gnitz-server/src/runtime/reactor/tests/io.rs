//! Client ingress: the frame queue's ceilings and the global inbound-memory
//! budget every `RecvBuf` is charged against.

use std::io::Write;

use super::super::test_support::*;
use super::super::{Limits, Reactor};
use super::*;
use gnitz_wire::framed;

/// A reactor over one W2M ring with an inbound cap of `cap`.
fn capped_reactor(cap: usize) -> Reactor {
    make_reactor_with(Limits { inbound_cap: cap, ..Limits::TEST })
}

/// Frames deframed, and why the feed ended, if it did.
pub(super) type Deframed = (Vec<Vec<u8>>, Option<RecvEnd>);

/// The frames `stream` deframes to under a `cap`-byte budget, fed `step` bytes a
/// read, and why the feed ended, if it did.
pub(super) fn deframe(stream: &[u8], step: usize, admitted: bool, cap: usize) -> Deframed {
    let mut q = RecvQueue::new(Budget::new(cap));
    if admitted {
        q.admit_full_frames();
    }
    let mut frames = Vec::new();
    for read in stream.chunks(step) {
        let fed = q.feed(read);
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
/// it — sees the end now. Both arms: a framing violation retires, a cap breach
/// fails.
#[test]
fn a_refusal_discards_the_queue_and_shuts_the_socket_down() {
    for (cap, wire) in [
        (usize::MAX, 0u32.to_le_bytes().to_vec()),
        (250, framed(&[0xAB; 100]).repeat(3)),
    ] {
        let r = capped_reactor(cap);
        let (conn, partner) = admitted(&r);
        (&partner).write_all(&wire).expect("write");

        assert!(poll_until(&r, || conn.is_gone()), "cap={cap}");
        assert!(
            matches!(try_poll_once(conn.recv()), Some(None)),
            "cap={cap}: recv never parks after a refusal"
        );
        assert_eq!(r.inbound.held(), 0, "cap={cap}");
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
    let (conn1, partner1) = admitted(&r);

    // The header claims 10_000 bytes but only 100 arrive: the buffer is charged
    // at the header, yet no frame completes.
    let mut hdr_and_part = 10_000u32.to_le_bytes().to_vec();
    hdr_and_part.extend_from_slice(&[0x11u8; 100]);
    (&partner1).write_all(&hdr_and_part).expect("write");

    assert!(
        poll_until(&r, || r.inbound.held() == 10_000),
        "in-flight buffer was not accounted"
    );
    assert!(!conn1.is_readable(), "no frame completes from a partial payload");

    let (conn2, partner2) = registered(&r);
    (&partner2).write_all(&framed(&[0x22u8; 100])).expect("write");
    assert!(
        poll_until(&r, || conn2.is_gone()),
        "over-cap second connection was not closed"
    );
    assert_eq!(r.inbound.held(), 10_000, "the refused connection allocated nothing");
}

/// Consumption refunds the budget, so traffic far above the cap never trips while
/// the consumer keeps pace, and the budget returns to 0 once the queue drains.
#[test]
fn inbound_cap_accounting_balances_on_consume() {
    // One recv deframes a whole round and charges every frame in it, so the peak
    // is a round rather than a frame — and the cap below admits one.
    let r = capped_reactor(15_000);
    let (conn, partner) = admitted(&r);

    let wire = framed(&[0x7Eu8; 1_000]).repeat(10); // each frame weighs 1_000
    for _round in 0..2 {
        (&partner).write_all(&wire).expect("write");
        r.block_on(async {
            for _ in 0..10 {
                assert_eq!(conn.recv().await.expect("frame").as_slice().len(), 1_000);
            }
        });
    }
    assert_eq!(r.inbound.held(), 0, "every frame consumed");
}

/// `readable` resolves on a queued frame and on the end of the recv side, and
/// takes nothing: the frame is still there for `recv`.
#[test]
fn readable_reports_a_frame_or_the_end_and_takes_neither() {
    let r = make_reactor();
    let (conn, partner) = registered(&r);
    assert!(try_poll_once(conn.readable()).is_none(), "nothing sent yet");

    let payload = [7u8; 16];
    (&partner).write_all(&framed(&payload)).expect("write");
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
