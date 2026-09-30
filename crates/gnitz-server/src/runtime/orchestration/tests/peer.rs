//! Corked egress: what waits in `Peer`'s accumulator, what it leaves behind, and
//! how a finished connection refuses it.

use std::io::Write;
use std::time::{Duration, Instant};

use super::*;
use crate::runtime::reactor::{egress_pair, framed, read_nonblocking, ring_slot, select2, spawn_drain, Either, Limits};

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
            peer.send(slot).await.expect("an open peer");
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
        assert_eq!(peer.send(small).await, Ok(()), "a small slot is corked, not sent");
        assert_eq!(peer.flush_egress().await, Err(PeerGone), "its flush fails");
        assert_eq!(peer.corked_len(), 0);
        assert_eq!(peer.send(big).await, Err(PeerGone));
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

    let r2 = Rc::clone(&r);
    let parked = r.block_on(async move {
        let timer = r2.timer(Instant::now() + Duration::from_millis(50));
        matches!(select2(peer.next_request(), timer).await, Either::B(()))
    });

    assert!(parked, "no request is queued, so the timer wins");
    assert_eq!(
        read_nonblocking(&receiver, 4096).expect("bytes on the wire"),
        frame,
        "the corked frame left before the park"
    );
}

/// W frames as W sends versus `Peer` corking them into one send, the trade
/// `COALESCE_MAX_BYTES` is set from. Only the ratio is meaningful.
///
/// `cd crates && cargo test -p gnitz-server --release fanout_coalesced_egress_bench -- --ignored --nocapture --test-threads=1`
#[test]
#[ignore]
fn fanout_coalesced_egress_bench() {
    use std::hint::black_box;

    const ITERS: usize = 3000;

    for w in [2usize, 4, 8] {
        for total in [4 * 1024usize, 32 * 1024, 64 * 1024, 128 * 1024] {
            let per_frame = total / w;
            let (r, conn, receiver) = egress_pair(Limits::TEST, None);
            let peer = Peer::new(&r, Rc::clone(&conn), None);
            // Both arms push `total` bytes per sample; the reader keeps
            // the socket buffers from ever stalling a send, so the timed
            // region is kernel-op cost, not backpressure.
            let expect = 2 * ITERS * total;
            let drain_t = spawn_drain(receiver, expect);

            let frame = vec![0xA5u8; per_frame];
            let r2 = Rc::clone(&r);
            let (per_frame_dur, coalesced_dur) = r.block_on(async move {
                let (mut a, mut b) = (Duration::ZERO, Duration::ZERO);
                for i in 0..ITERS {
                    // Source buffers are filled outside the timed region
                    // on both arms except the concatenation itself,
                    // which is the copy under test.
                    let bufs: Vec<PooledBuf> = (0..w).map(|_| PooledBuf(frame.clone())).collect();
                    let run_per_frame = async |bufs: Vec<PooledBuf>| {
                        let t = Instant::now();
                        for buf in bufs {
                            let _ = black_box(r2.send_owned(&conn, SendBody::Pooled(buf)).await);
                        }
                        t.elapsed()
                    };
                    let run_coalesced = async || {
                        let t = Instant::now();
                        for _ in 0..w {
                            peer.cork(&frame);
                        }
                        let _ = black_box(peer.flush_egress().await);
                        t.elapsed()
                    };
                    if i % 2 == 0 {
                        a += run_per_frame(bufs).await;
                        b += run_coalesced().await;
                    } else {
                        b += run_coalesced().await;
                        a += run_per_frame(bufs).await;
                    }
                }
                (a, b)
            });

            let seen = drain_t.join().expect("drain thread");
            assert_eq!(seen, expect as u64, "reader must observe every byte both arms sent");

            let delta = coalesced_dur.as_secs_f64() / per_frame_dur.as_secs_f64() - 1.0;
            println!(
                "coalesced egress W={w} total={total}B: coalesced vs per-frame {:+.1}% \
                 (per-frame {:?}/batch, coalesced {:?}/batch)",
                delta * 100.0,
                per_frame_dur / ITERS as u32,
                coalesced_dur / ITERS as u32,
            );
        }
    }
}
