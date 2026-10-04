use super::*;
use crate::runtime::reactor::{egress_pair, spawn_drain, Limits};
use std::time::{Duration, Instant};

/// W frames as W sends versus `Peer` corking them into one send, by frame
/// size: the trade `COALESCE_MAX_BYTES` is set from. Wall clock, because the
/// sends a cork saves are kernel work that an instruction count of this thread
/// leaves out; only the ratio is meaningful.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn fanout_coalesced_egress_bench() {
    const ROUNDS: usize = 3000;
    const CAP: usize = COALESCE_MAX_BYTES;

    for w in [2usize, 8] {
        for frame_len in [CAP / 32, CAP / 8, CAP / 2, CAP, 2 * CAP] {
            let total = w * frame_len;
            let (r, conn, receiver) = egress_pair(Limits::TEST, None);
            let peer = Peer::new(&r, conn, None);
            // The reader keeps the socket buffers from ever stalling a send, so
            // the timed region is kernel-op cost, not backpressure.
            let expect = 2 * ROUNDS * total;
            let drain = spawn_drain(receiver, expect);

            let frame = vec![0xA5u8; frame_len];
            let [per_frame, corked] = r.block_on(async move {
                let mut spent = [Duration::ZERO; 2];
                // Each round runs both arms, the one that goes first alternating.
                // Every buffer is taken before its arm's clock starts, the cork's
                // at the size it will hold: the copy into it is what is timed.
                for i in 0..2 * ROUNDS {
                    if matches!(i % 4, 1 | 2) {
                        *peer.egress.borrow_mut() = Some(PooledBuf::with_capacity(total));
                        let t = Instant::now();
                        for _ in 0..w {
                            peer.cork(&frame);
                        }
                        peer.flush_egress().await.expect("an open peer");
                        spent[1] += t.elapsed();
                    } else {
                        let bufs: Vec<PooledBuf> = (0..w)
                            .map(|_| {
                                let mut buf = PooledBuf::with_capacity(frame_len);
                                buf.extend_from_slice(&frame);
                                buf
                            })
                            .collect();
                        let t = Instant::now();
                        for buf in bufs {
                            peer.send_raw(SendBody::Pooled(buf)).await.expect("an open peer");
                        }
                        spent[0] += t.elapsed();
                    }
                }
                spent
            });

            let seen = drain.join().expect("drain thread");
            assert_eq!(seen, expect as u64, "the reader saw every byte of both arms");
            println!(
                "fanout_coalesced_egress_bench w={w} frame={frame_len:>5}B corked {:+6.1}% ({:?} against {:?} a round)",
                (corked.as_secs_f64() / per_frame.as_secs_f64() - 1.0) * 100.0,
                corked / ROUNDS as u32,
                per_frame / ROUNDS as u32,
            );
        }
    }
}
