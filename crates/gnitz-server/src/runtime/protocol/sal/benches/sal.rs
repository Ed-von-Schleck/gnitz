use super::fixtures::TestLog;
use super::tests::scan;
use super::{DirectGroup, GroupTargets, SalReader, WorkerSet};

/// Instructions retired per [`SalReader::next`] over control-only groups: ones
/// addressed to the reader, and ones it steps over.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn sal_read_bench() {
    use gnitz_foundation::perf::Counter;
    use std::hint::black_box;

    const GROUPS: u64 = 10_000;
    let counter = Counter::instructions().expect("instructions counter");
    for nw in [1usize, 4, 16] {
        for (case, set) in [
            ("addressed", WorkerSet::ALL),
            ("stepped over", WorkerSet::ALL.without(0)),
        ] {
            if set.within(nw).len() == 0 {
                continue;
            }
            let log = TestLog::new(32 << 20, nw, 1);
            let group = DirectGroup {
                targets: GroupTargets {
                    set,
                    request_id: 1,
                    in_request_order: false,
                },
                ..DirectGroup::new(scan(0, &[]))
            };
            let excl = log.excl();
            for _ in 0..GROUPS {
                excl.write(&group).expect("group fits");
            }
            let reader = SalReader::new(log.log(), 0, 1);
            let (read, n) = counter.measure(|| {
                let mut read = 0;
                while let Some((msg, slot)) = reader.next() {
                    black_box((msg.request_id, slot.len()));
                    read += 1;
                }
                read
            });
            assert_eq!(read, if set.contains(0) { GROUPS } else { 0 }, "{case} at NW={nw}");
            eprintln!("sal_read_bench NW={nw:<2} {case}: {} instructions/group", n / GROUPS);
        }
    }
}
