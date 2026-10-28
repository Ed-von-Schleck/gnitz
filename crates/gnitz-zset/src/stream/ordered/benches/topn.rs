use crate::schema::SchemaColumn;
use crate::stream::ordered::tests::{bench_rows, Harness};
use crate::stream::ordered::OrderedPlan;
use crate::test_support::{mix, u64_pk_schema};
use gnitz_foundation::perf::Counter;
use gnitz_wire::{OrderKey, TypeCode};

const N: u64 = 1 << 16;

/// `grp` and `key`, behind the U64 id.
const GROUP_AND_KEY: [SchemaColumn; 2] = [SchemaColumn::new(TypeCode::I64, false); 2];

fn by_key(desc: bool) -> [OrderKey; 1] {
    [OrderKey { col: 2, desc, nulls_first: false }]
}

/// By group count, limit and offset: the bulk epochs, then one-row epochs that
/// insert below every group's top-N, insert at a top-N's head, and retract that head.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn op_topn_bench() {
    const ONES: u64 = 256;
    let counter = Counter::instructions();
    let schema = u64_pk_schema(&GROUP_AND_KEY);
    // `(groups, limit, offset)`.
    let cases: [(u64, u64, u64); 8] = [
        (4096, 10, 0),
        (4096, 1000, 0),
        (16, 10, 0),
        (16, 1000, 0),
        (16, 10, 100),
        (16, 1000, 1000),
        (1, 10, 0),
        (1, u64::MAX, 1),
    ];
    for (groups, limit, offset) in cases {
        let group = |i: u64| mix(i) % groups;
        // Bulk keys stand in the upper half of the key space.
        let bulk = |i: u64| (mix(i) >> 2) | (1 << 62);
        let plan = OrderedPlan::top_n(&schema, &[1], &by_key(false), limit, offset).unwrap();
        let mut h = Harness::new(plan);
        let [e1, e2, e3] = h.bulk_epochs(
            &counter,
            &bench_rows(&schema, 0..N, group, bulk, 1),
            &bench_rows(&schema, N..2 * N, group, bulk, 1),
        );
        let mut ones = |key: &dyn Fn(u64) -> u64, base: u64, w: i64| {
            let total: u64 = (base..base + ONES)
                .map(|i| h.measure(&counter, &bench_rows(&schema, i..i + 1, group, key, w)))
                .sum();
            total as f64 / ONES as f64
        };
        let below = ones(&|i| i64::MAX as u64 - i, 3 * N, 1);
        let head = ones(&|i| 5 * N - i, 4 * N, 1);
        let unhead = ones(&|i| 5 * N - i, 4 * N, -1);
        println!(
            "op_topn_bench groups {groups:4} limit {limit:20} offset {offset:4}: empty {e1:7.1}, \
             populated {e2:7.1}, retraction {e3:7.1} instr/row; one row below {below:9.0}, at head {head:9.0}, \
             head retracted {unhead:9.0} instr/epoch"
        );
    }
}

/// The bulk epochs over entries already in index order: the key ascends with
/// the id, and each group is one block of ids.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn op_topn_ordered_bench() {
    let counter = Counter::instructions();
    let schema = u64_pk_schema(&GROUP_AND_KEY);
    for (groups, limit) in [(1u64, 10u64), (1, 100_000), (16, 10)] {
        let group = |i: u64| i * groups / (4 * N);
        let plan = OrderedPlan::top_n(&schema, &[1], &by_key(false), limit, 0).unwrap();
        let [e1, e2, e3] = Harness::new(plan).bulk_epochs(
            &counter,
            &bench_rows(&schema, 0..N, group, |i| i, 1),
            &bench_rows(&schema, N..2 * N, group, |i| i, 1),
        );
        println!(
            "op_topn_ordered_bench groups {groups:2} limit {limit:6}: empty {e1:7.1}, \
             populated {e2:7.1}, retraction {e3:7.1} instr/row"
        );
    }
}

/// The bulk epochs of an ungrouped `ORDER BY key DESC` over ascending keys:
/// every bulk row sorts ahead of every stored one.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn op_topn_descending_bench() {
    let counter = Counter::instructions();
    let schema = u64_pk_schema(&GROUP_AND_KEY);
    for limit in [10u64, 1000] {
        let plan = OrderedPlan::top_n_partial(&schema, &by_key(true), limit, 0).unwrap();
        let [e1, e2, e3] = Harness::new(plan).bulk_epochs(
            &counter,
            &bench_rows(&schema, 0..N, |_| 0, |i| i, 1),
            &bench_rows(&schema, N..2 * N, |_| 0, |i| i, 1),
        );
        println!(
            "op_topn_descending_bench limit {limit:4}: empty {e1:7.1}, \
             populated {e2:7.1}, retraction {e3:7.1} instr/row"
        );
    }
}
