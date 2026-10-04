use std::hint::black_box;

use gnitz_foundation::perf::{voluntary_ctx_switches, Counter};

use super::super::test_support::*;
use super::*;
use crate::runtime::sal::{SalMessageKind, WorkerSet};
use crate::runtime::wire::WireMsg;

const PASSES: u64 = 100_000;

/// A one-shot op the kernel completes at its submit.
fn nop(r: &Reactor) -> oneshot::Receiver<OpResult> {
    let (u, op) = r.install_op(None);
    // SAFETY: the SQE names no memory.
    unsafe { r.ring.borrow_mut().push(opcode::Nop::new().build(), u) };
    op
}

/// Spawn a task on `r` that parks in `park` over and over, then run `PASSES`
/// passes, `publish` ahead of each and outside the count. Every pass must bring
/// the task back from one park.
fn arm(counter: &Counter, label: &str, r: Reactor, park: impl AsyncFn() + 'static, mut publish: impl FnMut()) {
    let parks = Rc::new(Cell::new(0u64));
    let p = Rc::clone(&parks);
    r.spawn(async move {
        loop {
            park().await;
            p.set(p.get() + 1);
        }
    });
    r.tick(false); // the first poll parks the task
    let mut instructions = 0;
    for _ in 0..PASSES {
        publish();
        instructions += counter.measure(|| r.tick(false)).1;
    }
    let empty = (0..PASSES).map(|_| counter.measure(|| ()).1).sum::<u64>();
    assert_eq!(parks.get(), PASSES, "{label}: a pass that woke nothing");
    println!(
        "reactor_pass_bench {label:<12} {:>7.1} instr/pass",
        (instructions - empty) as f64 / PASSES as f64
    );
    r.tasks.borrow_mut().clear(); // the task holds the reactor
}

/// What one pass of the loop costs in instructions, by what its one task parked
/// on. Each pass wakes the task from its park and runs it into the next one.
///
/// `task` wakes itself, and is the floor under the others. `op` is a one-shot op
/// through the ring, `op+deadline` the same raced against a timer as every client
/// send is. `acks` is a lease over every worker, answered by each, and `train` one
/// row frame of a scan's reply.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn reactor_pass_bench() {
    let counter = Counter::instructions();

    arm(&counter, "task", make_reactor(), async || YieldOnce::new().await, || ());

    let r = make_reactor();
    let r2 = r.clone();
    arm(&counter, "op", r, async move || drop(black_box(nop(&r2).await)), || ());

    let r = make_reactor();
    let r2 = r.clone();
    let send = async move || {
        let op = select2(nop(&r2), r2.sleep(r2.limits.client_send_timeout)).await;
        assert!(matches!(op, Either::A(_)));
    };
    arm(&counter, "op+deadline", r, send, || ());

    for nw in [1, 4, 16] {
        let (r, mut writers) = reactor_with_rings(nw);
        let id = Rc::new(Cell::new(0));
        let (r2, leased) = (r.clone(), Rc::clone(&id));
        let acks = async move || {
            let lease = r2.lease_acks(WorkerSet::ALL);
            leased.set(lease.id());
            lease.acks().await;
        };
        arm(&counter, &format!("acks nw={nw}"), r, acks, || {
            writers.iter_mut().for_each(|w| w.send_ack(id.get()));
        });
    }

    let (r, mut writers) = reactor_with_rings(1);
    let lease = r.lease_train(WorkerSet::ALL, SalMessageKind::ScanSpec);
    let id = lease.id();
    let schema = crate::test_support::make_schema_u64_i64();
    let rows = crate::test_support::make_batch(&schema, &[(1, 1, 0)]);
    let frame = WireMsg {
        data: rows.wire_whole(),
        ..WireMsg::train_frame(0, false)
    };
    let next = async move || assert!(matches!(black_box(lease.next().await), Ok(Some(_))));
    arm(&counter, "train", r, next, || writers[0].send_msg(id, &frame));
}

/// What a flood of worker frames costs the master through its `FUTEX_WAITV`
/// park, and what that park costs the workers.
///
/// Each worker publishes `N` frames from a thread of its own while the reactor
/// runs its loop: frames no lease takes, then the ACK the run ends on. A publish
/// that finds the park armed spends a `FUTEX_WAKE`, sampled just before each. The
/// park arms every ring and a publish takes it from its own ring alone, so one
/// master sleep can cost a wake per worker.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn reactor_flood_bench() {
    const N: u32 = 200_000;

    let counter = Counter::instructions();
    for nw in [1, 4, 16] {
        let (r, writers) = reactor_with_rings(nw);
        let lease = r.lease_ready();
        let (instructions, sleeps, wakes) = std::thread::scope(|s| {
            let floods: Vec<_> = writers
                .into_iter()
                .map(|mut writer| {
                    s.spawn(move || {
                        let mut wakes = 0u64;
                        for id in (BOOT_READY_REQUEST_ID + 1..=N).chain([BOOT_READY_REQUEST_ID]) {
                            wakes += writer.master_parked() as u64;
                            writer.send_ack(id);
                        }
                        wakes
                    })
                })
                .collect();
            let before = voluntary_ctx_switches();
            let ((), instructions) = counter.measure(|| r.block_on(async move { lease.acks().await }));
            let sleeps = voluntary_ctx_switches() - before;
            let wakes: u64 = floods.into_iter().map(|f| f.join().expect("a flood thread")).sum();
            (instructions, sleeps, wakes)
        });
        let kmsgs = (nw as u64 * N as u64) as f64 / 1000.0;
        println!(
            "reactor_flood_bench nw={nw:<2} {:>6.1} instr/msg, per 1k msgs {:>6.2} master sleeps and {:>6.2} worker wakes",
            instructions as f64 / (kmsgs * 1000.0),
            sleeps as f64 / kmsgs,
            wakes as f64 / kmsgs,
        );
    }
}
