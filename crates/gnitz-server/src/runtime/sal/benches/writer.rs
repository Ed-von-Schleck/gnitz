use super::fixtures::{control_push, scan, test_log};
use super::{Group, WorkerSet};
use crate::runtime::w2m::fixtures::id;
use gnitz_foundation::perf::Counter;

const GROUPS: u64 = 10_000;

/// Instructions per control-only zone member, from the scope's opening to its
/// commit: laid out, the last one marked as closing its zone, and published.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn sal_zone_commit_bench() {
    const NW: usize = 4;
    let counter = Counter::instructions();
    let group = control_push(16);
    for members in [1u64, 8, 64] {
        let zones = GROUPS / members;
        let log = test_log(32 << 20, NW, 1);
        let mut excl = log.excl();
        let (last, committed) = counter.measure(|| {
            let mut last = 0;
            for _ in 0..zones {
                let mut scope = excl.begin();
                for _ in 0..members {
                    scope.write(&group, true).expect("group fits");
                }
                last = scope.commit().expect("the zone was open");
            }
            last
        });
        drop(excl);
        assert_eq!(last, log.watermark(), "every zone was committed");
        println!(
            "sal_zone_commit_bench nw={NW} members={members:<2} {:>6.1} instr/group",
            committed as f64 / (zones * members) as f64
        );
    }
}

/// Instructions per payload byte of a group large enough to be laid out in the
/// writer's scratch buffer and written with `pwrite`: one blob per worker.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn sal_large_group_bench() {
    const NW: usize = 4;
    const LARGE_GROUPS: usize = 16;
    let counter = Counter::instructions();
    for blob_len in [4 << 10, 64 << 10, 1 << 20] {
        let blobs = vec![vec![7u8; blob_len]; NW];
        let log = test_log(LARGE_GROUPS * NW * (blob_len + 4096) + (16 << 20), NW, 1);
        let read = scan(16, &[]);
        let group = Group::read(&read, WorkerSet::ALL, Some(&blobs), id(1));
        let mut excl = log.excl();
        // One group first, so the scratch buffer has grown before the count.
        excl.write_group(&group).expect("group fits");
        let ((), written) = counter.measure(|| {
            for _ in 0..LARGE_GROUPS {
                excl.write_group(&group).expect("group fits");
            }
        });
        drop(excl);
        let bytes = (LARGE_GROUPS * NW * blob_len) as f64;
        println!(
            "sal_large_group_bench nw={NW} blob={blob_len:>8} {:>10.0} instr/group, {:.4} instr/payload byte",
            written as f64 / LARGE_GROUPS as f64,
            written as f64 / bytes
        );
    }
}
