use std::ops::Range;

use super::op_topn::op_topn;
use super::plan::TopNPlan;
use super::tests::Harness;
use crate::repr::{Batch, BatchBuilder};
use crate::schema::{SchemaColumn, SchemaDescriptor, TypeCode};
use crate::test_support::{mix, TestTrace};
use gnitz_foundation::perf::Counter;
use gnitz_wire::OrderKey;

const N: u64 = 1 << 16;

/// The operator over an index the bench drives itself, and the epochs it ran.
struct Epochs {
    h: Harness,
    ran: usize,
}

impl Epochs {
    fn new(plan: TopNPlan) -> Self {
        Epochs { h: Harness::new(plan, &[]), ran: 0 }
    }

    /// One whole epoch over `delta` — its index entries, their fold and the walk
    /// — in instructions. The index is refolded to one run every eighth epoch,
    /// outside the measured region.
    fn run(&mut self, counter: &Counter, delta: &Batch) -> u64 {
        let Harness { plan, index, .. } = &mut self.h;
        let (epoch, instructions) =
            counter.measure(|| op_topn(delta, &mut |first, last| index.cursor_within(first, last), plan));
        index.ingest(epoch.index_entries);
        self.ran += 1;
        if self.ran.is_multiple_of(8) {
            let all = index.cursor().materialize();
            *index = TestTrace::new(plan.index.schema);
            index.ingest((*all).clone());
        }
        instructions
    }

    /// Three bulk epochs — `first` into an empty index, `second` beside it, and
    /// the retraction of `first` — in instructions per delta row.
    fn bulk(&mut self, counter: &Counter, first: &Batch, second: &Batch) -> [f64; 3] {
        let retraction = first.clone().negated().into_consolidated();
        [first, second, &retraction].map(|d| self.run(counter, d) as f64 / d.count as f64)
    }
}

/// Per ORDER BY key form and group key form, over three epochs — an insert
/// against an empty index, a second insert into the same groups, and the
/// retraction of the first: the index's bytes per row, and the instructions per
/// delta row of each whole epoch.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn op_topn_bench() {
    let counter = Counter::instructions();
    let asc = |col| OrderKey { col, desc: false, nulls_first: false };
    type Put = fn(&mut BatchBuilder, u64);
    let i64_key = SchemaColumn::new(TypeCode::I64, false);
    let group_i64 = SchemaColumn::new(TypeCode::I64, false);
    let group_null = SchemaColumn::new(TypeCode::I64, true);
    // `(label, group columns, key column, the key of row i, groups)`.
    let cases: [(&str, &[SchemaColumn], SchemaColumn, Put, u64); 7] = [
        ("global, I64 key", &[], i64_key, |b, i| b.put_u64(mix(i)), 1),
        (
            "I64 group, I64 key",
            &[group_i64],
            i64_key,
            |b, i| b.put_u64(mix(i)),
            4096,
        ),
        // Keys that agree on every byte but the lowest.
        (
            "I64 group, dense I64 key",
            &[group_i64],
            i64_key,
            |b, i| b.put_u64(i % 256),
            4096,
        ),
        (
            "I64 group, nullable I64 key",
            &[group_i64],
            SchemaColumn::new(TypeCode::I64, true),
            |b, i| b.put_opt_int((i % 4 != 0).then_some(mix(i) as u128)),
            4096,
        ),
        (
            "I64 group, STRING key",
            &[group_i64],
            SchemaColumn::new(TypeCode::String, false),
            |b, i| b.put_string(&format!("{:040}", mix(i))),
            4096,
        ),
        (
            "nullable I64 group, I64 key",
            &[group_null],
            i64_key,
            |b, i| b.put_u64(mix(i)),
            4096,
        ),
        (
            "2 nullable I64 groups, I64 key",
            &[group_null; 2],
            i64_key,
            |b, i| b.put_u64(mix(i)),
            4096,
        ),
    ];
    for (label, group, key, put, groups) in cases {
        let mut cols = vec![SchemaColumn::new(TypeCode::U64, false)];
        cols.extend_from_slice(group);
        cols.push(key);
        let schema = SchemaDescriptor::new(&cols, &[0]);
        let group_cols: Vec<u32> = (1..=group.len() as u32).collect();
        let make = |salt: u64| {
            let mut b = BatchBuilder::new(&schema);
            for i in 0..N {
                b.begin_row((i + salt * N) as u128, 1);
                for _ in group {
                    b.put_int((mix(i) % groups) as u128);
                }
                put(&mut b, i + salt * N);
                b.end_row();
            }
            b.finish().into_consolidated()
        };
        let plan = TopNPlan::from_wire(&schema, &group_cols, &[asc(cols.len() as u16 - 1)], 10, 0).unwrap();
        let mut e = Epochs::new(plan);
        let (d1, d2) = (make(1), make(2));
        let index = &e.h.plan.index;
        let bytes = index.batch(&d1, e.h.plan.key.carried()).total_bytes() as f64 / N as f64;
        let pk = index.schema.pk_stride();
        let [e1, e2, e3] = e.bulk(&counter, &d1, &d2);
        println!(
            "op_topn_bench {label:<31} index {bytes:5.1} B/row (pk {pk:2}), empty {e1:6.1}, populated {e2:6.1}, \
             retraction {e3:6.1} instr/row"
        );
    }
}

/// `[U64 id (pk), I64 grp, I64 key]`.
fn window_schema() -> SchemaDescriptor {
    let i64_col = SchemaColumn::new(TypeCode::I64, false);
    SchemaDescriptor::new(&[SchemaColumn::new(TypeCode::U64, false), i64_col, i64_col], &[0])
}

/// One row of [`window_schema`] per id, at weight `w`.
fn window_rows(ids: Range<u64>, group: impl Fn(u64) -> u64, key: impl Fn(u64) -> u64, w: i64) -> Batch {
    let mut b = BatchBuilder::new(&window_schema());
    for i in ids {
        b.begin_row(i as u128, w);
        b.put_int(group(i) as u128);
        b.put_u64(key(i));
        b.end_row();
    }
    b.finish().into_consolidated()
}

fn by_key(desc: bool) -> [OrderKey; 1] {
    [OrderKey { col: 2, desc, nulls_first: false }]
}

/// By group count, limit and offset: the bulk epochs, then one-row epochs that
/// insert below every window, insert at a window's head, and retract that head.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn op_topn_window_bench() {
    const ONES: u64 = 256;
    let counter = Counter::instructions();
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
        let plan = TopNPlan::from_wire(&window_schema(), &[1], &by_key(false), limit, offset).unwrap();
        let mut e = Epochs::new(plan);
        let [e1, e2, e3] = e.bulk(
            &counter,
            &window_rows(0..N, group, bulk, 1),
            &window_rows(N..2 * N, group, bulk, 1),
        );
        let mut ones = |key: &dyn Fn(u64) -> u64, base: u64, w: i64| {
            let total: u64 = (base..base + ONES)
                .map(|i| e.run(&counter, &window_rows(i..i + 1, group, key, w)))
                .sum();
            total as f64 / ONES as f64
        };
        let below = ones(&|i| i64::MAX as u64 - i, 3 * N, 1);
        let head = ones(&|i| 5 * N - i, 4 * N, 1);
        let unhead = ones(&|i| 5 * N - i, 4 * N, -1);
        println!(
            "op_topn_window_bench groups {groups:4} limit {limit:20} offset {offset:4}: empty {e1:7.1}, \
             populated {e2:7.1}, retraction {e3:7.1} instr/row; one row below {below:9.0}, at head {head:9.0}, \
             head retracted {unhead:9.0} instr/epoch"
        );
    }
}

/// The bulk epochs over entries already in index order: the key ascends with
/// the id, and each group is one block of ids.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn op_topn_window_ordered_bench() {
    let counter = Counter::instructions();
    for (groups, limit) in [(1u64, 10u64), (1, 100_000), (16, 10)] {
        let group = |i: u64| i * groups / (4 * N);
        let plan = TopNPlan::from_wire(&window_schema(), &[1], &by_key(false), limit, 0).unwrap();
        let [e1, e2, e3] = Epochs::new(plan).bulk(
            &counter,
            &window_rows(0..N, group, |i| i, 1),
            &window_rows(N..2 * N, group, |i| i, 1),
        );
        println!(
            "op_topn_window_ordered_bench groups {groups:2} limit {limit:6}: empty {e1:7.1}, \
             populated {e2:7.1}, retraction {e3:7.1} instr/row"
        );
    }
}

/// The bulk epochs of an ungrouped `ORDER BY key DESC` over ascending keys:
/// every bulk row sorts ahead of every stored one.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn op_topn_window_descending_bench() {
    let counter = Counter::instructions();
    for limit in [10u64, 1000] {
        let plan = TopNPlan::partial(&window_schema(), &by_key(true), limit, 0).unwrap();
        let [e1, e2, e3] = Epochs::new(plan).bulk(
            &counter,
            &window_rows(0..N, |_| 0, |i| i, 1),
            &window_rows(N..2 * N, |_| 0, |i| i, 1),
        );
        println!(
            "op_topn_window_descending_bench limit {limit:4}: empty {e1:7.1}, \
             populated {e2:7.1}, retraction {e3:7.1} instr/row"
        );
    }
}
