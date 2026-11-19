use crate::stream::ordered::OrderedPlan;
use crate::stream::ordered::tests::{Harness, bench_rows};
use crate::test_support::u64_pk_schema;
use gnitz_foundation::perf::Counter;
use gnitz_foundation::rng::mix;
use gnitz_wire::{AggDescriptor, AggFunc, OrderKey, TypeCode};
use gnitz_zset::schema::SchemaColumn;

const N: u64 = 1 << 16;

/// By rows per group, a rank and a running sum: the bulk epoch into an empty
/// index, then one-row epochs that append behind every row of a group and that
/// insert in front of them all — the walk of a group with nothing behind the
/// row to re-emit, and with all of it.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn op_window_bench() {
    const ONES: u64 = 64;
    let counter = Counter::instructions();
    // `grp`, `key` and `val`, behind the U64 id.
    let schema = u64_pk_schema(&[SchemaColumn::new(TypeCode::I64, false); 3]);
    let order = [OrderKey { col: 2, desc: false, nulls_first: false }];
    let sum = [AggDescriptor { agg_op: AggFunc::Sum, col_idx: 3 }];
    for per_group in [16u64, 256, 4096] {
        let groups = N / per_group;
        let group = |i: u64| mix(i) % groups;
        // Bulk keys stand in the middle of the key space.
        let bulk = |i: u64| (mix(i) >> 2) | (1 << 61);
        let plan = OrderedPlan::window(&schema, &[1], &order, &sum, true, false).unwrap();
        let mut h = Harness::new(plan);
        let empty = h.measure(&counter, &bench_rows(&schema, 0..N, group, bulk, 1)) as f64 / N as f64;
        let mut ones = |key: &dyn Fn(u64) -> u64, base: u64| {
            let total: u64 = (base..base + ONES)
                .map(|i| h.measure(&counter, &bench_rows(&schema, i..i + 1, group, key, 1)))
                .sum();
            total as f64 / ONES as f64
        };
        let behind = ones(&|i| (1 << 62) + i, 2 * N);
        let in_front = ones(&|i| 4 * N - i, 3 * N);
        println!(
            "op_window_bench {per_group:4} rows/group: empty {empty:7.1} instr/row; one row behind {behind:9.0}, \
             in front {in_front:9.0} instr/epoch"
        );
    }
}
