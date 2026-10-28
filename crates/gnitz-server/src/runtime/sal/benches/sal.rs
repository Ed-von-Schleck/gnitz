use super::fixtures::test_log;
use super::tests::scan;
use super::{GroupData, SalGroup, SalReader, SalWriter, WorkerSet};
use crate::runtime::reactor::GroupTargets;
use gnitz_foundation::perf::Counter;
use std::hint::black_box;

const GROUPS: u64 = 10_000;

/// `(layout, nw, per_worker)`: a shared group holds one payload at every worker
/// count; a per-worker group holds one per addressed worker.
const LAYOUTS: [(&str, usize, bool); 3] = [("shared", 2, false), ("per worker", 4, true), ("per worker", 16, true)];

/// A log of [`GROUPS`] control-only groups addressed to `set`, written under one
/// hold, and the instructions writing them took.
fn written_log(counter: &Counter, nw: usize, per_worker: bool, set: WorkerSet) -> (SalWriter, u64) {
    let blobs = vec![Vec::new(); nw];
    let log = test_log(32 << 20, nw, 1);
    let group = SalGroup {
        data: if per_worker {
            GroupData::Blobs(&blobs)
        } else {
            GroupData::Same(None)
        },
        targets: GroupTargets { set, ..GroupTargets::SILENT },
        ..SalGroup::new(scan(0, &[]))
    };
    let mut excl = log.excl();
    let ((), written) = counter.measure(|| {
        for _ in 0..GROUPS {
            excl.write(&group).expect("group fits");
        }
    });
    drop(excl);
    (log, written)
}

/// Instructions per control-only group: what the writer spends on one, and
/// what the reader spends on one addressed to it and on one it steps over. The
/// reader is the last worker of the group's width.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn sal_group_bench() {
    let counter = Counter::instructions();
    for (layout, nw, per_worker) in LAYOUTS {
        for addressed in [true, false] {
            let me = nw - 1;
            let set = if addressed {
                WorkerSet::ALL
            } else {
                WorkerSet::ALL.without(me)
            };
            let (log, written) = written_log(&counter, nw, per_worker, set);
            let mut reader = SalReader::new(log.log(), me as u32, 1);
            let (read, walked) = counter.measure(|| {
                let mut read = 0;
                while let Some((msg, slot)) = reader.next() {
                    black_box((msg.head.request_id, slot.len()));
                    read += 1;
                }
                read
            });
            let case = if addressed { "addressed" } else { "stepped over" };
            assert_eq!(read, if addressed { GROUPS } else { 0 }, "{layout} {case} at nw={nw}");
            assert_eq!(
                reader.read_cursor,
                log.cursor(),
                "{layout} {case} at nw={nw}: every group was walked"
            );
            println!(
                "sal_group_bench nw={nw:<2} {layout:<10} {case:<12} {:>6.1} instr/group written, {:>6.1} read",
                written as f64 / GROUPS as f64,
                walked as f64 / GROUPS as f64
            );
        }
    }
}

/// Instructions per group addressed to the reader: [`SalReader::next`] alone,
/// and behind the [`SalReader::is_empty`] check a worker runs before it parks —
/// what the check adds to each group drained.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn sal_park_cycle_bench() {
    let counter = Counter::instructions();
    for (layout, nw, per_worker) in LAYOUTS {
        let me = nw - 1;
        let (log, _) = written_log(&counter, nw, per_worker, WorkerSet::ALL);
        let mut per_group = [0.0; 2];
        for (peek, cost) in [false, true].into_iter().zip(&mut per_group) {
            let mut reader = SalReader::new(log.log(), me as u32, 1);
            let (read, instructions) = counter.measure(|| {
                let mut read = 0u64;
                loop {
                    if peek && black_box(reader.is_empty()) {
                        break;
                    }
                    let Some((msg, slot)) = reader.next() else { break };
                    black_box((msg.head.request_id, slot.len()));
                    read += 1;
                }
                read
            });
            assert_eq!(read, GROUPS, "{layout} at nw={nw}: every group was read");
            *cost = instructions as f64 / GROUPS as f64;
        }
        println!(
            "sal_park_cycle_bench nw={nw:<2} {layout:<10} {:>6.1} instr/group next, {:>6.1} with the peek (+{:.1})",
            per_group[0],
            per_group[1],
            per_group[1] - per_group[0]
        );
    }
}
