//! Corked egress: what waits in `Peer`'s accumulator, what it leaves behind, and
//! how a finished connection refuses it.

use std::io::{Read, Write};
use std::time::Duration;

use super::*;
use crate::runtime::reactor::{egress_pair, framed, read_nonblocking, ring_slot, select2, Either, Limits};

/// `send` corks a slot that fits beside the cork, ships the cork first when the
/// slot would overflow it, and sends a slot over half the ceiling alone; the bytes
/// reach the wire in call order whichever it does.
#[test]
fn send_corks_what_fits_and_never_reorders() {
    const CAP: usize = COALESCE_MAX_BYTES;
    // What the send itself puts on the wire.
    enum Ships {
        Nothing,
        Cork,
        Both,
    }
    for (pre, pad, ships) in [
        (0, 64, Ships::Nothing),
        (128, 64, Ships::Nothing),
        (0, CAP, Ships::Both),
        (128, CAP, Ships::Both),
        (CAP - 128, CAP / 4, Ships::Cork),
    ] {
        let (_ring, slot) = ring_slot(pad);
        let all = [vec![0x5Au8; pre], slot.frame_bytes().to_vec()].concat();
        let (r, conn, receiver) = egress_pair(Limits::TEST, None);
        let peer = Peer::new(&r, conn, None);
        peer.cork(&all[..pre]);
        let peer = r.block_on(async move {
            peer.send(SendBody::Slot(slot)).await.expect("an open peer");
            peer
        });

        let wire = read_nonblocking(&receiver, 256 * 1024).unwrap_or_default();
        let (want_wire, want_corked) = match ships {
            Ships::Nothing => (&[][..], all.len()),
            Ships::Cork => (&all[..pre], all.len() - pre),
            Ships::Both => (&all[..], 0),
        };
        assert_eq!(
            (&wire[..], peer.corked_len()),
            (want_wire, want_corked),
            "pre={pre} pad={pad}"
        );

        r.block_on(async move { peer.flush_egress().await.expect("an open peer") });
        let rest = read_nonblocking(&receiver, 256 * 1024).unwrap_or_default();
        assert_eq!([wire, rest].concat(), all, "pre={pre} pad={pad}: call order");
    }
}

/// Corking is unbounded on its own; `flush_if_full` ships the cork once it
/// reaches the ceiling, and not a byte before.
#[test]
fn flush_if_full_ships_only_a_full_cork() {
    for corked in [COALESCE_MAX_BYTES - 1, COALESCE_MAX_BYTES] {
        let (r, conn, receiver) = egress_pair(Limits::TEST, None);
        let peer = Peer::new(&r, conn, None);
        peer.cork(&vec![0x3Cu8; corked]);
        let left = r.block_on(async move {
            peer.flush_if_full().await.expect("an open peer");
            peer.corked_len()
        });
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

    let (r, conn, receiver) = egress_pair(Limits::TEST, None);
    let peer = Peer::new(&r, conn, None);
    drop(receiver);

    r.block_on(async move {
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
    let (r, conn, partner) = egress_pair(Limits::TEST, None);
    let peer = Peer::new(&r, conn, None);
    (&partner)
        .write_all(&[framed(b"a"), framed(b"b")].concat())
        .expect("write");

    let corked = r.block_on(async move {
        peer.next_request().await.expect("the first request");
        peer.cork(b"reply");
        peer.next_request().await.expect("the second request, already queued");
        peer.corked_len()
    });

    assert_eq!(corked, 5, "the reply stays corked");
    assert!(read_nonblocking(&partner, 64).is_none(), "nothing shipped");
}

/// With nothing queued, `next_request` ships what is corked before it parks on
/// the client.
#[test]
fn next_request_ships_before_parking() {
    let (r, conn, receiver) = egress_pair(Limits::TEST, None);
    let peer = Peer::new(&r, conn, None);
    let frame = vec![0x6Bu8; 300];
    peer.cork(&frame);

    let timer = r.sleep(Duration::from_millis(50));
    let parked = r.block_on(async move { matches!(select2(peer.next_request(), timer).await, Either::B(())) });

    assert!(parked, "no request is queued, so the timer wins");
    assert_eq!(
        read_nonblocking(&receiver, 4096).expect("bytes on the wire"),
        frame,
        "the corked frame left before the park"
    );
}

/// A shared body sent behind a non-empty cork joins it when it fits, and
/// otherwise leaves alone behind a flush of what was corked.
#[test]
fn a_shared_body_is_sent_behind_what_is_corked() {
    let (r, conn, receiver) = egress_pair(Limits::TEST, None);
    let peer = Rc::new(Peer::new(&r, conn, None));
    let small = Rc::new(vec![0x11u8; 100]);
    let large = Rc::new(vec![0x21u8; COALESCE_MAX_BYTES]);

    peer.cork(b"reply");
    let sending = Rc::clone(&peer);
    let body = Rc::clone(&small);
    r.block_on(async move { sending.send(SendBody::Shared(body)).await.expect("an open peer") });
    assert_eq!(peer.corked_len(), 105, "a body that fits is corked");
    assert!(
        read_nonblocking(&receiver, 1024).is_none(),
        "nothing leaves before a flush"
    );

    // The read above left the socket non-blocking.
    receiver.set_nonblocking(false).expect("blocking");
    let wire = std::thread::spawn(move || {
        let reader = receiver;
        let mut wire = vec![0u8; 105 + COALESCE_MAX_BYTES];
        (&reader)
            .read_exact(&mut wire)
            .expect("the cork and the body behind it");
        wire
    });
    let sending = Rc::clone(&peer);
    let body = Rc::clone(&large);
    r.block_on(async move { sending.send(SendBody::Shared(body)).await.expect("an open peer") });
    assert_eq!(peer.corked_len(), 0, "a body that does not fit is not corked");
    let want = [&b"reply"[..], &small, &large].concat();
    assert!(wire.join().unwrap() == want, "the body left behind what was corked");
}
