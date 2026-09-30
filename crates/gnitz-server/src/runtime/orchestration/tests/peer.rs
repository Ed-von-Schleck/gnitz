//! Corked egress: what waits in `Peer`'s accumulator, what it leaves behind, and
//! how a finished connection refuses it.

use std::rc::Rc;
use std::time::{Duration, Instant};

use gnitz_store::storage::PooledBuf;

use super::*;
use crate::runtime::reactor::{egress_pair, read_nonblocking, ring_slot, select2, spawn_drain, Either, Limits};

/// Corked replies put nothing on the wire until something flushes, and then
/// leave as one send: the far end reads the whole concatenation in one `read`.
#[test]
fn corked_replies_leave_as_one_send() {
    let (r, conn, receiver) = egress_pair(Limits::TEST, None);
    let receiver = Rc::new(receiver);
    let peer = Peer::new(&r, conn, None);
    let frames: Vec<Vec<u8>> = (0..8u8).map(|i| vec![0xD0 | i; 200]).collect();
    let expected: Vec<u8> = frames.iter().flatten().copied().collect();

    let rx = Rc::clone(&receiver);
    r.block_on(async move {
        for frame in &frames {
            peer.cork(frame);
        }
        assert!(
            read_nonblocking(&rx, 4096).is_none(),
            "corking must put nothing on the wire before a flush",
        );
        peer.flush_egress().await.expect("the flush must send");
    });

    assert_eq!(
        read_nonblocking(&receiver, 4096).expect("bytes on the wire"),
        expected,
        "one flush is one send carrying every corked frame in order",
    );
    drop(receiver);
}

/// A slot too large to cork, sent with bytes corked, puts the corked bytes on the
/// wire first: a zero-copy forward may not overtake a reply already written.
#[test]
fn a_slot_forward_cannot_overtake_a_corked_reply() {
    let (_ring, slot) = ring_slot(COALESCE_MAX_BYTES);
    let slot_bytes = slot.frame_bytes().to_vec();
    assert!(slot_bytes.len() > COALESCE_MAX_BYTES, "the slot goes out alone");

    let (r, conn, receiver) = egress_pair(Limits::TEST, None);
    let peer = Peer::new(&r, conn, None);
    let corked = vec![0x5Au8; 128];

    let c = corked.clone();
    r.block_on(async move {
        peer.cork(&c);
        peer.send(slot).await.expect("the slot forward must send");
        assert_eq!(peer.corked_len(), 0, "the slot went out alone, not corked");
    });

    let seen = read_nonblocking(&receiver, 256 * 1024).expect("bytes on the wire");
    let mut expected = corked;
    expected.extend_from_slice(&slot_bytes);
    assert_eq!(seen, expected, "the corked bytes precede the forwarded slot");
    drop(receiver);
}

/// A small slot sent with bytes corked joins them: nothing reaches the wire until
/// a flush, and that one flush carries both.
#[test]
fn a_small_slot_is_corked_behind_corked_bytes() {
    let (_ring, slot) = ring_slot(64);
    let slot_bytes = slot.frame_bytes().to_vec();

    let (r, conn, receiver) = egress_pair(Limits::TEST, None);
    let receiver = Rc::new(receiver);
    let peer = Peer::new(&r, conn, None);
    let corked = vec![0x5Au8; 128];

    let (c, rx) = (corked.clone(), Rc::clone(&receiver));
    let both = corked.len() + slot_bytes.len();
    r.block_on(async move {
        peer.cork(&c);
        peer.send(slot).await.expect("corking cannot fail");
        assert_eq!(peer.corked_len(), both, "a small slot is corked, not sent");
        assert!(read_nonblocking(&rx, 4096).is_none(), "nothing is on the wire yet");
        peer.flush_egress().await.expect("the flush must send");
    });

    let mut expected = corked;
    expected.extend_from_slice(&slot_bytes);
    assert_eq!(
        read_nonblocking(&receiver, 4096).expect("bytes on the wire"),
        expected,
        "one flush carries both, in order"
    );
    drop(receiver);
}

/// Corking is unbounded on its own; `flush_if_full` is what ships a long run,
/// and it leaves nothing behind once it does.
#[test]
fn a_full_accumulator_ships_between_messages() {
    let (r, conn, receiver) = egress_pair(Limits::TEST, None);
    let peer = Peer::new(&r, conn, None);
    let drain = spawn_drain(receiver, COALESCE_MAX_BYTES);
    let frame = vec![0x3Cu8; 4096];

    r.block_on(async move {
        while peer.corked_len() < COALESCE_MAX_BYTES {
            peer.cork(&frame);
        }
        peer.flush_if_full().await.expect("the flush must send");
        assert_eq!(peer.corked_len(), 0, "the flush ships everything corked");
    });

    assert_eq!(drain.join().expect("drain"), COALESCE_MAX_BYTES as u64);
}

/// A failed send finishes the peer: every later send and flush refuses, nothing
/// stays corked, and no further request is handed out.
#[test]
fn a_failed_send_finishes_the_peer() {
    let (_big_ring, big) = ring_slot(COALESCE_MAX_BYTES);
    let (_small_ring, small) = ring_slot(64);

    let (r, conn, receiver) = egress_pair(Limits::TEST, None);
    let peer = Peer::new(&r, conn, None);
    drop(receiver);

    r.block_on(async move {
        assert_eq!(peer.send(big).await, Err(PeerGone), "a send to a closed partner fails");
        assert_eq!(peer.send(small).await, Err(PeerGone), "a later small send refuses");
        assert_eq!(peer.corked_len(), 0, "and corks nothing");
        assert_eq!(peer.flush_if_full().await, Err(PeerGone));
        assert_eq!(peer.flush_egress().await, Err(PeerGone));
        assert!(
            peer.next_request().await.is_none(),
            "no request follows a finished peer"
        );
    });
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

/// W frames as W sends versus one send of their concatenation, the trade
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
                        let mut buf = PooledBuf::with_capacity(total);
                        for _ in 0..w {
                            buf.0.extend_from_slice(&frame);
                        }
                        let _ = black_box(r2.send_owned(&conn, SendBody::Pooled(buf)).await);
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
