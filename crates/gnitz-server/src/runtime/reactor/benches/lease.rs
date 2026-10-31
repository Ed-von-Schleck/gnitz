use std::hint::black_box;

use gnitz_foundation::perf::Counter;

use super::super::test_support::*;
use super::*;
use crate::runtime::w2m::fixtures::test_rings;

/// Instructions per train frame from its ring to its reader: the drain that
/// routes it and the read that takes it off its lease, by the frame's size. The
/// frames are published outside the count.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn train_route_bench() {
    const LAP: usize = 4;
    let schema = crate::test_support::make_schema_u64_i64();
    for (rows, laps) in [(16usize, 20_000u64), (512, 20_000), (32_768, 500)] {
        let batch =
            crate::test_support::make_batch(&schema, &(0..rows as u64).map(|i| (i, 1, i as i64)).collect::<Vec<_>>());
        let (mut writers, receiver) = test_rings(1, 16 << 20);
        let (r, _ready) = Reactor::new(Limits::TEST, receiver).expect("reactor");
        let lease = r.lease_train(WorkerSet::ALL, "ScanSpec");
        let id = lease.id();
        let counter = Counter::instructions();
        let (mut instructions, mut bytes) = (0, 0);
        for _ in 0..laps {
            for _ in 0..LAP {
                bytes += writers[0].send_train_frame(id, false, Some(&batch)).len();
            }
            instructions += counter
                .measure(|| {
                    r.drain_all_w2m();
                    for _ in 0..LAP {
                        let f = try_poll_once(lease.next())
                            .expect("routed")
                            .expect("no fault")
                            .expect("a row frame");
                        black_box(f.slot.bytes());
                    }
                })
                .1;
        }
        let frames = laps * LAP as u64;
        println!(
            "train_route_bench {:>8} B/frame: {:>9.1} instr/frame",
            bytes as u64 / frames,
            instructions as f64 / frames as f64
        );
    }
}
