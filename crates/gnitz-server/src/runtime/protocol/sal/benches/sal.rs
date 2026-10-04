use super::fixtures::TestLog;
use super::tests::scan;
use super::{DirectGroup, GroupTargets, SalReader, WorkerSet};
use gnitz_foundation::perf::Counter;
use std::hint::black_box;

/// Instructions the reader spends per control-only group: ones addressed to it,
/// and ones it steps over. A shared group holds one payload at every worker
/// count; a per-worker group holds one per addressed worker, and the reader is
/// the last of them.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn sal_read_bench() {
    const GROUPS: u64 = 10_000;

    let counter = Counter::instructions();
    for (layout, nw, per_worker) in [("shared", 2, false), ("per worker", 4, true), ("per worker", 16, true)] {
        let blobs = vec![Vec::new(); nw];
        for addressed in [true, false] {
            let me = nw - 1;
            let set = if addressed {
                WorkerSet::ALL
            } else {
                WorkerSet::ALL.without(me)
            };
            let log = TestLog::new(32 << 20, nw, 1);
            let group = DirectGroup {
                extras: per_worker.then_some(&blobs[..]),
                targets: GroupTargets { set, ..GroupTargets::UNADDRESSED },
                ..DirectGroup::new(scan(0, &[]))
            };
            let excl = log.excl();
            for _ in 0..GROUPS {
                excl.write(&group).expect("group fits");
            }
            let reader = SalReader::new(log.log(), me as u32, 1);
            let (read, instructions) = counter.measure(|| {
                let mut read = 0;
                while let Some((msg, slot)) = reader.next() {
                    black_box((msg.request_id, slot.len()));
                    read += 1;
                }
                read
            });
            let case = if addressed { "addressed" } else { "stepped over" };
            assert_eq!(read, if addressed { GROUPS } else { 0 }, "{layout} {case} at nw={nw}");
            assert_eq!(
                reader.cut(),
                GROUPS,
                "{layout} {case} at nw={nw}: every group was walked"
            );
            println!(
                "sal_read_bench nw={nw:<2} {layout:<10} {case:<12} {:>6.1} instr/group",
                instructions as f64 / GROUPS as f64
            );
        }
    }
}
