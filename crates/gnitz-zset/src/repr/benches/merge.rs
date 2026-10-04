use super::*;
use crate::repr::BatchBuilder;
use crate::schema::{SchemaColumn, SchemaDescriptor, TypeCode};
use crate::test_support::{bench_time, make_batch, make_schema_u128_i64, make_schema_u64_i64, pk_u64_two_i64_schema};

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
    const ITERS: usize = 20;
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
    for (label, schema) in [
        ("fixed_int_stride8", make_schema_u64_i64()),
        ("fixed_int_stride16", make_schema_u128_i64()),
        ("generic", generic),
    ] {
        let batches: Vec<Batch> = (0..K).map(|_| bench_sorted_batch(&schema, N, DUP)).collect();
        let mem: Vec<MemBatch<'_>> = batches.iter().map(|b| b.as_mem_batch()).collect();
        let mut sink = 0i64;
        let elapsed = bench_time(ITERS, || {
            run_merge(&mem, &schema, |_s, _r, w| {
                sink = sink.wrapping_add(std::hint::black_box(w));
            });
        });
        std::hint::black_box(sink);
        let (rows, secs) = (K * N, elapsed.as_secs_f64());
        let rps = (ITERS as f64 * rows as f64) / secs;
        println!("run_merge_{label}: {rows} rows × {ITERS} iters in {secs:.3}s = {rps:.0} rows/s");
    }
}

/// The flush merge of one big run of even keys with four small runs of distinct
/// odd keys spread across it, priced per small-run row.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn merge_consolidated_skewed_bench() {
    use std::hint::black_box;

    let schema = pk_u64_two_i64_schema();
    for (n, d, iters) in [
        (16_384usize, 16usize, 400usize),
        (65_536, 16, 100),
        (262_144, 16, 25),
        (262_144, 4096, 25),
    ] {
        let big = bench_flush_batch(&schema, n, |i| (2 * i) as u64);
        let stride = (n / d).max(1);
        let mut batches: Vec<Batch> = Vec::with_capacity(5);
        batches.push(big);
        for j in 0..4usize {
            batches.push(bench_flush_batch(&schema, d, move |i| {
                ((i * stride) * 2 + 1 + 2 * j) as u64
            }));
        }

        let mem: Vec<MemBatch<'_>> = batches.iter().map(|b| b.as_mem_batch()).collect();
        let total_rows: usize = mem.iter().map(|b| b.count).sum();

        // The untimed warmup takes the allocator, batch pool and page-in.
        let secs = bench_time(iters, || {
            black_box(merge_consolidated(&mem, &schema));
        })
        .as_secs_f64();

        let rps = (iters as f64 * total_rows as f64) / secs;
        let ns_per_delta = secs * 1e9 / (iters as f64 * 4.0 * d as f64);
        println!("merge_skew/N{n}_d{d}: {total_rows} rows  {rps:.0} merge-rows/s  {ns_per_delta:.0} ns/delta-row");
    }
}

/// Instructions per call of the two Z-set sums an exchange round ends in —
/// `merge_consolidated` and `Batch::concat` — over N rows in K stripes of one
/// ascending key space, so the merge interleaves them.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn striped_sum_bench() {
    use std::hint::black_box;

    let instructions = gnitz_foundation::perf::Counter::instructions();
    let schema = pk_u64_two_i64_schema();
    for (n, iters) in [(4usize, 10_000usize), (64, 10_000), (4096, 1000), (1_000_000, 10)] {
        for k in [1usize, 4, 16] {
            let per = (n / k).max(1);
            let stripes: Vec<Batch> = (0..k)
                .map(|j| bench_flush_batch(&schema, per, |i| (i * k + j) as u64))
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
                "striped_sum N{n} K{k}: merge {} instructions per call, concat {}",
                merged / iters as u64,
                concatenated / iters as u64
            );
        }
    }
}

/// Instructions and cycles per input row of [`Batch::merged_consolidated`], over
/// the shapes its three arms see. `shared_pk_interleave` and `shared_pk_fold`
/// put every row in an equal-PK group, the arm that folds: the first switches
/// side at every row, the second folds every row into its twin. `alt1` and
/// `runs4096` are PK-disjoint and stay in the galloping arms, at runs of one row
/// (what a set operation's hashed `_set_pk` produces) and of 4096.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn merged_consolidated_bench() {
    use gnitz_foundation::perf::Counter;
    const N: usize = 500_000;
    const RUN: usize = 4096;
    let (instructions, cycles) = (Counter::instructions(), Counter::cycles());
    let schema = make_schema_u64_i64();
    /// Row `i` of side 0 or 1 as `(pk, payload)`, each side strictly ascending:
    /// the merge takes consolidated operands.
    type Row = fn(usize, usize) -> (usize, usize);
    let cases: [(&str, Row); 4] = [
        ("shared_pk_interleave", |i, side| (i / 8, 2 * (i % 8) + side)),
        ("shared_pk_fold", |i, _| (i / 8, i % 8)),
        ("alt1", |i, side| (2 * i + side, i)),
        ("runs4096", |i, side| ((2 * (i / RUN) + side) * RUN + i % RUN, i)),
    ];
    for (label, row) in cases {
        let [a, b] = [0, 1].map(|side| {
            let rows: Vec<(u64, i64, i64)> = (0..N)
                .map(|i| row(i, side))
                .map(|(pk, v)| (pk as u64, 1, v as i64))
                .collect();
            make_batch(&schema, &rows)
        });
        std::hint::black_box(a.merged_consolidated(&b, &schema));
        let ((out, instr), cyc) = cycles.measure(|| instructions.measure(|| a.merged_consolidated(&b, &schema)));
        println!(
            "merged_consolidated_bench {label:<20} {:5.1} instr/row, {:5.1} cycles/row ({} rows out)",
            instr as f64 / (2 * N) as f64,
            cyc as f64 / (2 * N) as f64,
            out.count
        );
    }
}
