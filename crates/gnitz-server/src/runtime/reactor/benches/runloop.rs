use std::hint::black_box;
use std::io::Write;

use gnitz_foundation::perf::{voluntary_ctx_switches, Counter};

use super::super::test_support::*;
use super::*;
use crate::runtime::frame::WireMsg;
use crate::runtime::park::WorkerSet;
use crate::runtime::w2m::fixtures::id;
use crate::runtime::w2m::RequestId;

const PASSES: u64 = 100_000;

/// Spawn a task on `r` that parks in `park` over and over, then run `PASSES`
/// passes under each counter, `publish` ahead of each pass and outside the count.
/// Every pass must bring the task back from one park.
fn arm(label: &str, r: Reactor, park: impl AsyncFn() + 'static, mut publish: impl FnMut()) {
    let parks = Rc::new(Cell::new(0u64));
    let p = Rc::clone(&parks);
    r.spawn(async move {
        loop {
            park().await;
            p.set(p.get() + 1);
        }
    });
    r.tick(false); // the first poll parks the task

    // One counter at a time: two open at once would count each other's ioctls.
    let [user, all] = [Counter::instructions as fn() -> Counter, Counter::instructions_all].map(|open| {
        let counter = open();
        let mut instructions = 0;
        for _ in 0..PASSES {
            publish();
            instructions += counter.measure(|| r.tick(false)).1;
        }
        let empty = (0..PASSES).map(|_| counter.measure(|| ()).1).sum::<u64>();
        (instructions - empty) as f64 / PASSES as f64
    });
    assert_eq!(parks.get(), 2 * PASSES, "{label}: a pass that woke nothing");
    println!("reactor_pass_bench {label:<12} {user:>7.1} user, {all:>8.1} user+kernel instr/pass");
    r.tasks.borrow_mut().clear(); // the task holds the reactor
}

/// What one pass of the loop costs in instructions, by what its one task parked
/// on. Each pass wakes the task from its park and runs it into the next one.
///
/// `task` wakes itself, and is the floor under the others. `op` is a one-shot op
/// through the ring, `op+deadline` the same raced against a timer as every client
/// send is. `recv` is one client frame through the armed recv, its filter and the
/// connection's queue. `acks` is a lease over every worker, answered by each, and
/// `train` one row frame of a scan's reply.
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
    let (conn, partner) = registered(&r);
    let frame = gnitz_wire::framed(b"x");
    let recv = async move || drop(black_box(conn.recv().await.expect("a frame")));
    arm("recv", r, recv, || (&partner).write_all(&frame).expect("write"));

    for nw in [1, 4, 16] {
        let (r, mut writers) = reactor_with_rings(nw);
        // The wire value of the live lease's id, 0 before the first.
        let id = Rc::new(Cell::new(0));
        let (r2, leased) = (r.clone(), Rc::clone(&id));
        let acks = async move || {
            let lease = r2.lease_acks(WorkerSet::ALL);
            leased.set(lease.id().get());
            lease.acks().await;
        };
        arm(&format!("acks nw={nw}"), r, acks, || {
            if let Some(id) = RequestId::from_wire(id.get()) {
                writers.iter_mut().for_each(|w| w.send_ack(id));
            }
        });
    }

    let (r, mut writers) = reactor_with_rings(1);
    let lease = r.lease_train(WorkerSet::ALL, "ScanSpec");
    let id = lease.id();
    let schema = crate::test_support::make_schema_u64_i64();
    let rows = crate::test_support::make_batch(&schema, &[(1, 1, 0)]);
    let frame = WireMsg::train_frame(0, false, rows.wire_whole());
    let next = async move || assert!(matches!(black_box(lease.next().await), Ok(Some(_))));
    arm("train", r, next, || writers[0].send_msg(id, &frame));
}

/// What a flood of worker frames costs the master through its park, and what
/// that park costs the workers.
///
/// Each worker publishes `N` frames from a thread of its own while the reactor
/// runs its loop: frames no lease takes, then the ACK the run ends on. The
/// publish that takes an arm spends a `FUTEX_WAKE`, counted by the park itself.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn reactor_flood_bench() {
    const N: u32 = 200_000;

    let counter = Counter::instructions();
    for nw in [1, 4, 16] {
        let (r, ready, writers) = booting_reactor(nw);
        let wakes_before = r.w2m.wakes();
        let (instructions, sleeps) = std::thread::scope(|s| {
            for mut writer in writers {
                s.spawn(move || {
                    for n in (2..=N).chain([1]) {
                        writer.send_ack(id(n));
                    }
                });
            }
            let before = voluntary_ctx_switches();
            let (_, instructions) = counter.measure(|| r.block_on(ready.all_acked()));
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
/// and scan: the master asleep in its park, and every worker answering one lease
/// over all of them.
///
/// Each worker is a thread that polls for a new round every 100 µs with a plain
/// sleep, which also stands for its answer latency, and ACKs it. The master runs
/// `ROUNDS` leases, each resolving only on its last ACK, so the loop finishing is
/// the proof every round took `nw` ACKs. The publish that takes an arm spends a
/// `FUTEX_WAKE`, counted by the park itself; `perf stat -e
/// syscalls:sys_enter_futex` over this test run alone counts the same wakes, the
/// writers making no futex call of their own.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn reactor_round_bench() {
    use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};

    const ROUNDS: u32 = 3000;

    for nw in [1, 4, 16] {
        let (r, writers) = reactor_with_rings(nw);
        let round = AtomicU32::new(0); // 0: no round yet
        let stop = AtomicBool::new(false);
        let wakes_before = r.w2m.wakes();
        let sleeps = std::thread::scope(|s| {
            for mut writer in writers {
                let (round, stop) = (&round, &stop);
                s.spawn(move || {
                    let mut last = 0;
                    while !stop.load(Ordering::Acquire) {
                        std::thread::sleep(std::time::Duration::from_micros(100));
                        let id = round.load(Ordering::Acquire);
                        if let Some(req) = RequestId::from_wire(id).filter(|_| id != last) {
                            writer.send_ack(req);
                            last = id;
                        }
                    }
                });
            }
            let before = voluntary_ctx_switches();
            for _ in 0..ROUNDS {
                let lease = r.lease_acks(WorkerSet::ALL);
                round.store(lease.id().get(), Ordering::Release);
                r.block_on(lease.acks());
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
