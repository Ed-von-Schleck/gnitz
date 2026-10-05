use super::op_topn::op_topn;
use super::plan::TopNPlan;
use crate::repr::{Batch, BatchBuilder};
use crate::schema::{SchemaColumn, SchemaDescriptor, TypeCode};
use crate::test_support::TestTrace;
use gnitz_foundation::perf::Counter;
use gnitz_wire::OrderKey;

fn mix(i: u64) -> u64 {
    i.wrapping_mul(0x9E37_79B9_7F4A_7C15)
}

/// The operator with its two traces, driven the way the VM drives it.
struct Harness {
    plan: TopNPlan,
    index: TestTrace,
    trace_out: TestTrace,
}

impl Harness {
    fn new(plan: TopNPlan) -> Self {
        let index = TestTrace::new(plan.index.schema);
        let trace_out = TestTrace::new(plan.output_schema);
        Harness { plan, index, trace_out }
    }

    /// One epoch over `delta`: the instructions per delta row of its index
    /// entries, and of `op_topn` alone.
    fn epoch(&mut self, counter: &Counter, delta: &Batch) -> (f64, f64) {
        let (entries, populate) = counter.measure(|| self.plan.index_batch(delta));
        self.index.ingest(entries);
        let (index, trace_out) = (&self.index, &self.trace_out);
        let (out, run) = counter.measure(|| {
            op_topn(
                delta,
                &mut |first, last| trace_out.cursor_within(first, last),
                &mut |first, last| index.cursor_within(first, last),
                &self.plan,
            )
        });
        self.trace_out.ingest(out);
        let n = delta.count as f64;
        (populate as f64 / n, run as f64 / n)
    }
}

/// Per ORDER BY key form and group key form, over three epochs — an insert
/// against an empty index, a second insert into the same groups, and the
/// retraction of the first: the index's bytes per row, and the instructions per
/// delta row of the index entries and of `op_topn`.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn op_topn_bench() {
    const N: u64 = 1 << 16;
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
        let mut h = Harness::new(plan);
        let (d1, d2) = (make(1), make(2));
        let retraction = d1.clone().negated().into_consolidated();
        let bytes = h.plan.index_batch(&d1).total_bytes() as f64 / N as f64;
        let [(p1, e1), (_, e2), (_, e3)] = [&d1, &d2, &retraction].map(|d| h.epoch(&counter, d));
        println!(
            "op_topn_bench {label:<31} index {bytes:5.1} B/row (pk {:2}), entries {p1:6.1}, \
             empty {e1:6.1}, populated {e2:6.1}, retraction {e3:6.1} instr/row",
            h.plan.index.schema.pk_stride(),
        );
    }
}
