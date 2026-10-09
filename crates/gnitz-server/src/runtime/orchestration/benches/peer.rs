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

/// One pushed train to `n` connections sharing its body: a flush and a lone
/// send of the body against `Peer::send` behind the corked head. Sends and
/// instructions, user and kernel, of the reactor thread alone.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn pushed_train_band_bench() {
    use crate::runtime::reactor::client_pair;
    use gnitz_foundation::perf::Counter;
    const ROUNDS: usize = 2000;
    const CAP: usize = COALESCE_MAX_BYTES;
    const HEAD: usize = 64;
    const ANSWER: usize = 64;
    let user = Counter::instructions();
    let all = Counter::instructions_all();
    let cycles = Counter::cycles_all();

    for n in [1usize, 8] {
        for body_len in [
            CAP / 4,
            CAP / 2 + 1,
            3 * CAP / 4,
            CAP - HEAD - ANSWER,
            CAP - HEAD,
            CAP + 1,
            4 * CAP,
        ] {
            let (r, conn0, receiver0) = egress_pair(Limits::TEST, None);
            let mut first = Some((conn0, receiver0));
            let per_peer = 3 * 2 * ROUNDS * (HEAD + body_len + ANSWER);
            let mut peers = Vec::new();
            let mut drains = Vec::new();
            for _ in 0..n {
                let (conn, receiver) = first.take().unwrap_or_else(|| client_pair(&r));
                peers.push(Peer::new(&r, conn, None));
                drains.push(spawn_drain(receiver, per_peer));
            }
            let peers = Rc::new(peers);
            let body = Rc::new(vec![0xA5u8; body_len]);
            let head = [0x11u8; HEAD];
            let answer = [0x22u8; ANSWER];

            // [arm][0 = sends, 1 = user instr, 2 = user+kernel instr, 3 = cycles]
            let mut cost = [[0u64; 4]; 2];
            // Each counter in a pass of its own: two open at once would count
            // each other's ioctls.
            for (which, counter) in [(1usize, &user), (2, &all), (3, &cycles)] {
                for i in 0..2 * ROUNDS {
                    let arm = usize::from(matches!(i % 4, 1 | 2));
                    let (peers, body) = (Rc::clone(&peers), Rc::clone(&body));
                    let before = SENDS.with(Cell::get);
                    let ((), counted) = counter.measure(|| {
                        r.block_on(async move {
                            for peer in peers.iter() {
                                peer.cork(&head);
                                let body = SendBody::Shared(Rc::clone(&body));
                                if arm == 0 && body_len > CAP / 2 {
                                    peer.flush_egress().await.expect("an open peer");
                                    peer.send_raw(body).await.expect("an open peer");
                                } else {
                                    peer.send(body).await.expect("an open peer");
                                }
                                peer.cork(&answer);
                                peer.flush_egress().await.expect("an open peer");
                            }
                        })
                    });
                    cost[arm][which] += counted;
                    cost[arm][0] += SENDS.with(Cell::get) - before;
                }
            }
            drop(peers);
            for d in drains {
                assert_eq!(
                    d.join().expect("drain thread"),
                    per_peer as u64,
                    "a reader saw every byte of both arms"
                );
            }
            // Sends were counted in all three passes.
            let per = |x: u64, passes: u64| x as f64 / (passes * ROUNDS as u64 * n as u64) as f64;
            let arm = |c: [u64; 4]| {
                format!(
                    "{:.2} sends, {:>6.0} user, {:>6.0} user+kernel instr, {:>6.0} cycles",
                    per(c[0], 3),
                    per(c[1], 1),
                    per(c[2], 1),
                    per(c[3], 1)
                )
            };
            println!(
                "pushed_train_band_bench n={n} body={body_len:>6}B | flush and lone send: {} | Peer::send: {}  (per train per connection)",
                arm(cost[0]),
                arm(cost[1]),
            );
        }
    }
}
