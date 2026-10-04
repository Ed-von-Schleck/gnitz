use super::*;
use crate::repr::BatchBuilder;
use crate::schema::{SchemaColumn, SchemaDescriptor, TypeCode};
use crate::test_support::{make_batch, make_schema_u64_i64, pk_u64_two_i64_schema};
use gnitz_foundation::perf::Counter;
use std::hint::black_box;

/// `n` sorted rows in PK groups of `dup`, distinct only in the last payload
/// column, so every compare within a group walks the whole payload.
fn bench_sorted_batch(schema: &SchemaDescriptor, n: usize, dup: usize) -> Batch {
    let last = schema.num_payload_cols() - 1;
    let mut b = BatchBuilder::new(schema);
    for i in 0..n {
        let group = (i / dup) as u128;
        b.begin_row(group, 1);
        for (pi, col) in schema.payload_columns() {
            match (pi == last, col.type_code) {
                (true, _) => b.put_int(i as u128),
                (false, TypeCode::F64) => b.put_float(group as f64),
                (false, _) => b.put_int(group),
            }
        }
        b.end_row();
    }
    b.finish()
}

/// `n` rows over [`pk_u64_two_i64_schema`], PK `key_fn(i)`, weight 1.
fn bench_flush_batch(schema: &SchemaDescriptor, n: usize, key_fn: impl Fn(usize) -> u64) -> Batch {
    let mut b = BatchBuilder::new(schema);
    for i in 0..n {
        b.begin_row(key_fn(i) as u128, 1i64);
        b.put_int(i as i64 as u128);
        b.put_int(((i as i64) * 2) as u128);
        b.end_row();
    }
    b.finish()
}

/// `run_merge`'s compare loop alone — the emit materializes nothing — under a
/// high duplicate-PK rate, over each payload comparator arm.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn run_merge_dup_pk_bench() {
    const K: usize = 4;
    const N: usize = 200_000;
    const DUP: usize = 8;
    // A float, a U128 and a nullable column each force the `Generic` arm.
    let generic = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::F64, false),
            SchemaColumn::new(TypeCode::U128, false),
            SchemaColumn::new(TypeCode::I64, true),
        ],
        &[0],
    );
    let (instructions, cycles) = (Counter::instructions(), Counter::cycles());
    for (label, schema) in [("fixed_int", make_schema_u64_i64()), ("generic", generic)] {
        let batches: Vec<Batch> = (0..K).map(|_| bench_sorted_batch(&schema, N, DUP)).collect();
        let mem: Vec<MemBatch<'_>> = batches.iter().map(|b| b.as_mem_batch()).collect();
        let merge = || {
            let mut sink = 0i64;
            run_merge(black_box(&mem), &schema, |_s, _r, w| sink = sink.wrapping_add(w));
            black_box(sink)
        };
        merge();
        let (instr, cyc) = (instructions.measure(merge).1, cycles.measure(merge).1);
        println!(
            "run_merge_dup_pk_bench {label:<9} {:5.1} instr/row, {:5.1} cycles/row",
            instr as f64 / (K * N) as f64,
            cyc as f64 / (K * N) as f64
        );
    }
}

/// Instructions per call of the two Z-set sums an exchange round ends in —
/// `merge_consolidated` and `Batch::concat` — over N rows in K stripes of one
/// key space. Interleaved stripes are merged; stripes laid end to end take
/// `merge_consolidated`'s concat arm.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn striped_sum_bench() {
    let instructions = Counter::instructions();
    let schema = pk_u64_two_i64_schema();
    for (n, iters) in [(64usize, 10_000usize), (4096, 1000), (1_000_000, 10)] {
        for (k, layout) in [(4usize, "interleaved"), (16, "interleaved"), (16, "end to end")] {
            let per = n / k;
            let stripes: Vec<Batch> = (0..k)
                .map(|j| {
                    bench_flush_batch(&schema, per, |i| match layout {
                        "interleaved" => (i * k + j) as u64,
                        _ => (j * per + i) as u64,
                    })
                })
                .collect();
            let mem: Vec<MemBatch<'_>> = stripes.iter().map(|b| b.as_mem_batch()).collect();
            let rows = per * k;
            assert_eq!(merge_consolidated(&mem, &schema).count, rows, "the merge dropped rows");
            assert_eq!(Batch::concat(&schema, mem.iter().cloned()).count, rows);
            let ((), merged) = instructions.measure(|| {
                for _ in 0..iters {
                    black_box(merge_consolidated(&mem, &schema));
                }
            });
            let ((), concatenated) = instructions.measure(|| {
                for _ in 0..iters {
                    black_box(Batch::concat(&schema, mem.iter().cloned()));
                }
            });
            println!(
                "striped_sum_bench {rows} rows, {k} stripes {layout}: merge {} instr/call, concat {}",
                merged / iters as u64,
                concatenated / iters as u64
            );
        }
    }
}

/// Instructions and cycles per input row of [`Batch::merged_consolidated`], over
/// the shapes its three arms see. `shared_pk_interleave` and `shared_pk_fold`
/// put every row in an equal-PK group, the arm that folds: the first switches
/// side at every row, the second folds every row into its twin. The rest are
/// PK-disjoint and stay in the galloping arms: `alt1` at runs of one row (what a
/// set operation's hashed `_set_pk` produces), `runs4096` at runs of 4096, and
/// `dominant` with one row of the small side between each 4096 of the large.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn merged_consolidated_bench() {
    const N: usize = 500_000;
    const RUN: usize = 4096;
    let (instructions, cycles) = (Counter::instructions(), Counter::cycles());
    let schema = make_schema_u64_i64();
    /// Row `i` of side 0 or 1 as `(pk, payload)`, each side strictly ascending:
    /// the merge takes consolidated operands.
    type Row = fn(usize, usize) -> (usize, usize);
    // Each with the row count of side 1; side 0 holds `N`.
    let cases: [(&str, Row, usize); 5] = [
        ("shared_pk_interleave", |i, side| (i / 8, 2 * (i % 8) + side), N),
        ("shared_pk_fold", |i, _| (i / 8, i % 8), N),
        ("alt1", |i, side| (2 * i + side, i), N),
        ("runs4096", |i, side| ((2 * (i / RUN) + side) * RUN + i % RUN, i), N),
        (
            "dominant",
            |i, side| (if side == 0 { i + i / RUN } else { i * (RUN + 1) + RUN }, i),
            N / RUN,
        ),
    ];
    for (label, row, side1) in cases {
        let [a, b] = [(0, N), (1, side1)].map(|(side, n)| {
            let rows: Vec<(u64, i64, i64)> = (0..n)
                .map(|i| row(i, side))
                .map(|(pk, v)| (pk as u64, 1, v as i64))
                .collect();
            make_batch(&schema, &rows)
        });
        let rows = (N + side1) as f64;
        black_box(a.merged_consolidated(&b, &schema));
        let ((out, instr), cyc) = cycles.measure(|| instructions.measure(|| a.merged_consolidated(&b, &schema)));
        println!(
            "merged_consolidated_bench {label:<20} {:5.1} instr/row, {:5.1} cycles/row ({} rows out)",
            instr as f64 / rows,
            cyc as f64 / rows,
            out.count
        );
    }
}
