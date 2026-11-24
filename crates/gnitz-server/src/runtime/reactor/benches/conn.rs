use gnitz_foundation::perf::{Counters, Event};

use super::*;
use crate::runtime::reactor::test_support::make_reactor;
use crate::runtime::test_support::spawn_drain;

/// The cost on the reactor thread of the two arms of `round`, `false` and
/// `true`, each run `rounds` times, the arm that goes first alternating:
/// `[arm][kernel sends, user instr, user+kernel instr, user+kernel cycles]`.
fn cost_of_arms(r: Reactor, rounds: usize, mut round: impl AsyncFnMut(bool)) -> [[u64; 4]; 2] {
    let counters = Counters::open([
        Event::INSTRUCTIONS,
        Event::INSTRUCTIONS.with_kernel(),
        Event::CYCLES.with_kernel(),
    ]);
    let mut cost = [[0u64; 4]; 2];
    for i in 0..2 * rounds {
        let arm = matches!(i % 4, 1 | 2);
        let before = r.ops.pushed();
        let ((), counted) = counters.measure(|| r.block_on(round(arm)));
        let c = &mut cost[usize::from(arm)];
        c[0] += r.ops.pushed() - before;
        for (c, k) in c[1..].iter_mut().zip(counted) {
            *c += k;
        }
    }
    cost
}

/// One arm's cost over `units`.
fn per(c: [u64; 4], units: usize) -> String {
    let per = |x: u64| x as f64 / units as f64;
    format!(
        "{:.2} sends, {:>6.0} user, {:>6.0} user+kernel instr, {:>6.0} cycles",
        per(c[0]),
        per(c[1]),
        per(c[2]),
        per(c[3])
    )
}

/// W frames as W sends against `Peer` corking them into one send, by frame
/// size: the trade `COALESCE_MAX_BYTES` is set from.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn fanout_coalesced_egress_bench() {
    const ROUNDS: usize = 3000;
    const CAP: usize = COALESCE_MAX_BYTES;

    for w in [2usize, 8] {
        for frame_len in [CAP / 32, CAP / 8, CAP / 2, CAP, 2 * CAP] {
            let r = make_reactor();
            let (mut peer, receiver) = Peer::over_socketpair(r);
            // The reader keeps the socket buffers from ever stalling a send, so
            // what is counted is kernel-op cost, not backpressure.
            let expect = 2 * ROUNDS * w * frame_len;
            let drain = spawn_drain(receiver, expect);
            let frame = Rc::new(vec![0xA5u8; frame_len]);
            let [per_frame, corked] = cost_of_arms(r, ROUNDS, async |corked| {
                for _ in 0..w {
                    if corked {
                        peer.cork(&frame);
                    } else {
                        let body = SendBody::Shared(Rc::clone(&frame));
                        peer.send_raw(body).await.expect("an open peer");
                    }
                }
                peer.flush_egress().await.expect("an open peer");
            });
            drop(peer);
            assert_eq!(
                drain.join().expect("drain thread"),
                expect as u64,
                "the reader saw every byte of both arms"
            );
            println!(
                "fanout_coalesced_egress_bench w={w} frame={frame_len:>5}B | a send each: {} | corked: {}  (per round)",
                per(per_frame, ROUNDS),
                per(corked, ROUNDS),
            );
        }
    }
}

/// One pushed train to `n` connections sharing its body: a flush and a lone
/// send of the body against `Peer::send` behind the corked head.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn pushed_train_band_bench() {
    const ROUNDS: usize = 2000;
    const CAP: usize = COALESCE_MAX_BYTES;
    const HEAD: usize = 64;
    const ANSWER: usize = 64;

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
            let r = make_reactor();
            let per_peer = 2 * ROUNDS * (HEAD + body_len + ANSWER);
            let (mut peers, drains): (Vec<_>, Vec<_>) = (0..n)
                .map(|_| {
                    let (peer, receiver) = Peer::over_socketpair(r);
                    (peer, spawn_drain(receiver, per_peer))
                })
                .unzip();
            let body = Rc::new(vec![0xA5u8; body_len]);
            let [lone, sent] = cost_of_arms(r, ROUNDS, async |through_send| {
                for peer in &mut peers {
                    peer.cork(&[0x11u8; HEAD]);
                    let body = SendBody::Shared(Rc::clone(&body));
                    if !through_send && body_len > CAP / 2 {
                        peer.flush_egress().await.expect("an open peer");
                        peer.send_raw(body).await.expect("an open peer");
                    } else {
                        peer.send(body).await.expect("an open peer");
                    }
                    peer.cork(&[0x22u8; ANSWER]);
                    peer.flush_egress().await.expect("an open peer");
                }
            });
            drop(peers);
            for d in drains {
                assert_eq!(
                    d.join().expect("drain thread"),
                    per_peer as u64,
                    "a reader saw every byte of both arms"
                );
            }
            println!(
                "pushed_train_band_bench n={n} body={body_len:>6}B | flush and lone send: {} | Peer::send: {}  (per train per connection)",
                per(lone, ROUNDS * n),
                per(sent, ROUNDS * n),
            );
        }
    }
}
