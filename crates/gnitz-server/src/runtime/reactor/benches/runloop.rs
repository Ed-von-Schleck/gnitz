use std::hint::black_box;
use std::io::Write;

use gnitz_foundation::perf::{Counter, Counters, Event, voluntary_ctx_switches};

use super::super::test_support::*;
use super::*;
use crate::runtime::w2m::fixtures::id;

const PASSES: u64 = 100_000;

/// Spawn a task on `r` that parks in `park` over and over, then run `PASSES`
/// passes, `publish` ahead of each pass and outside the count.
/// Every pass must bring the task back from one park.
fn arm(label: &str, r: Reactor, park: impl AsyncFn() + 'static, mut publish: impl FnMut()) {
    let parks = Rc::new(Cell::new(0u64));
    let p = Rc::clone(&parks);
    r.spawn(async move {
        loop {
            park().await;
            p.update(|v| v + 1);
        }
    });
    r.tick(); // the first poll parks the task

    let counters = Counters::open([Event::INSTRUCTIONS, Event::INSTRUCTIONS.with_kernel()]);
    let mut instructions = [0u64; 2];
    for _ in 0..PASSES {
        publish();
        let ((), counted) = counters.measure(|| r.tick());
        instructions = std::array::from_fn(|i| instructions[i] + counted[i]);
    }
    let mut empty = [0u64; 2];
    for _ in 0..PASSES {
        let ((), counted) = counters.measure(|| ());
        empty = std::array::from_fn(|i| empty[i] + counted[i]);
    }
    let [user, all] = std::array::from_fn(|i| (instructions[i] - empty[i]) as f64 / PASSES as f64);
    assert_eq!(parks.get(), PASSES, "{label}: a pass that woke nothing");
    println!("reactor_pass_bench {label:<12} {user:>7.1} user, {all:>8.1} user+kernel instr/pass");
    r.tasks.borrow_mut().clear(); // the task holds the reactor
}

/// What one pass of the loop costs in instructions, by what its one task parked
/// on. Each pass wakes the task from its park and runs it into the next one.
///
/// `task` wakes itself, and is the floor under the others. `op` is a one-shot op
/// through the ring, `op+deadline` the same raced against a timer as every client
/// send is. `recv` is one client frame through the armed recv and the
/// connection's queue. `acks` is a wait on a position every worker publishes,
/// and `train` one row frame of a scan's reply.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn reactor_pass_bench() {
    arm("task", make_reactor(), async || yield_once().await, || ());

    let r = make_reactor();
    let r2 = r.clone();
    arm(
        "op",
        r,
        async move || {
            black_box(nop(&r2).await);
        },
        || (),
    );

    let r = make_reactor();
    let r2 = r.clone();
    let send = async move || {
        let op = select2(nop(&r2), r2.sleep(Limits::TEST.client_send_timeout)).await;
        assert!(matches!(op, Either::A(_)));
    };
    arm("op+deadline", r, send, || ());

    let r = make_reactor();
    let (peer, partner) = Peer::over_socketpair(&r);
    let frame = gnitz_wire::framed(b"x");
    let recv = async move || drop(black_box(peer.recv().await.expect("a frame")));
    arm("recv", r, recv, || (&partner).write_all(&frame).expect("write"));

    for nw in [1, 4, 16] {
        let (r, mut writers) = reactor_with_rings(nw);
        // The position the task waits on, 0 before its first wait.
        let awaited = Rc::new(Cell::new(0));
        let (r2, next) = (r.clone(), Rc::clone(&awaited));
        let acks = async move || {
            next.update(|v| v + 1);
            r2.applied_through(next.get()).acks().await;
        };
        arm(&format!("acks nw={nw}"), r, acks, || {
            writers.iter_mut().for_each(|w| w.applied(awaited.get()));
        });
    }

    let (r, lease, mut writers) = train_lease(1, "ScanSpec");
    let id = lease.id();
    let schema = crate::test_support::make_schema_u64_i64();
    let rows = crate::test_support::certified(crate::test_support::make_batch(&schema, &[(1, 1, 0)]));
    // Leaked: the task parks on the lease's one train for as long as it runs.
    let train = train_of(Box::leak(Box::new(lease)));
    let next = async move || assert!(matches!(black_box(train.next().await), Ok(Some(_))));
    arm("train", r, next, || {
        drop(writers[0].send_train_frame(id, false, Some(&rows)))
    });
}

/// What a flood of worker frames costs the master through its park, and what
/// that park costs the workers.
///
/// Each worker publishes `N` frames from a thread of its own while the reactor
/// runs its loop: frames no lease takes, then the position the run ends on. The
/// publish that takes an arm spends a `FUTEX_WAKE`, counted by the park itself.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn reactor_flood_bench() {
    const N: u32 = 200_000;

    let counter = Counter::instructions();
    for nw in [1, 4, 16] {
        let (r, writers) = reactor_with_rings(nw);
        let wakes_before = r.w2m.wakes();
        let (instructions, sleeps) = std::thread::scope(|s| {
            for mut writer in writers {
                s.spawn(move || {
                    for n in 2..=N {
                        writer.send_msg(id(n), &Default::default());
                    }
                    writer.applied(1);
                });
            }
            let before = voluntary_ctx_switches();
            let (_, instructions) = counter.measure(|| r.block_on(r.applied_through(1).acks()));
            (instructions, voluntary_ctx_switches() - before)
        });
        let wakes = r.w2m.wakes() - wakes_before;
        let msgs = (nw as u64 * N as u64) as f64;
        println!(
            "reactor_flood_bench nw={nw:<2} {:>6.1} instr/msg, per 1k msgs {:>6.2} master sleeps and {:>6.2} worker wakes",
            instructions as f64 / msgs,
            sleeps as f64 * 1000.0 / msgs,
            wakes as f64 * 1000.0 / msgs,
        );
    }
}

/// What one broadcast round costs in the regime of every checkpoint, tick, DDL
/// and scan: the master asleep in its park, and every worker answering one
/// round sent to all of them.
///
/// Each worker is a thread that polls for a new round every 100 µs with a plain
/// sleep, which also stands for its answer latency, and ACKs it. The master runs
/// `ROUNDS` waits, each resolving only on its last ACK, so the loop finishing is
/// the proof every round took `nw` ACKs. The publish that takes an arm spends a
/// `FUTEX_WAKE`, counted by the park itself; `perf stat -e
/// syscalls:sys_enter_futex` over this test run alone counts the same wakes, the
/// writers making no futex call of their own.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn reactor_round_bench() {
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

    const ROUNDS: u64 = 3000;

    for nw in [1, 4, 16] {
        let (r, writers) = reactor_with_rings(nw);
        let round = AtomicU64::new(0); // 0: no round yet
        let stop = AtomicBool::new(false);
        let wakes_before = r.w2m.wakes();
        let sleeps = std::thread::scope(|s| {
            for mut writer in writers {
                let (round, stop) = (&round, &stop);
                s.spawn(move || {
                    let mut last = 0;
                    while !stop.load(Ordering::Acquire) {
                        std::thread::sleep(std::time::Duration::from_micros(100));
                        let at = round.load(Ordering::Acquire);
                        if at != last {
                            writer.applied(at);
                            last = at;
                        }
                    }
                });
            }
            let before = voluntary_ctx_switches();
            for at in 1..=ROUNDS {
                round.store(at, Ordering::Release);
                r.block_on(r.applied_through(at).acks());
            }
            let sleeps = voluntary_ctx_switches() - before;
            stop.store(true, Ordering::Release);
            sleeps
        });
        let wakes = r.w2m.wakes() - wakes_before;
        println!(
            "reactor_round_bench nw={nw:<2} per round {:.2} master sleeps, {:.2} waking publishes",
            sleeps as f64 / ROUNDS as f64,
            wakes as f64 / ROUNDS as f64,
        );
    }
}
