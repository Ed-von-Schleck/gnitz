use super::*;
use crate::runtime::reactor::{egress_pair, spawn_drain, Limits};
use std::time::{Duration, Instant};

/// W frames as W sends versus `Peer` corking them into one send, the trade
/// `COALESCE_MAX_BYTES` is set from. Only the ratio is meaningful.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
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
            let r2 = r.clone();
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
