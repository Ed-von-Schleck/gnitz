use super::*;
use crate::repr::BatchBuilder;
use crate::schema::SchemaColumn;
use crate::test_support::{
    make_batch_raw, make_schema_u64_i64, make_string_batch, mix, pk_payload_schema, string_batch, u64_pk_schema,
};
use gnitz_foundation::perf::Counter;
use gnitz_wire::TypeCode;
use std::hint::black_box;

/// `into_consolidated` over a batch with no claim, by the order its rows stand
/// in and by how many of them share a PK. "Ascending but the last" is the input
/// a check for "ascending" costs the most on; the shapes with several rows per
/// PK are the ones whose sort breaks PK ties on the payload, and "every row
/// twice" the only one that folds rows. A dense PK takes every value of a range,
/// as serial ids or a join re-keyed on a foreign key do. The scattered shapes
/// run again at a PK in each wider arm of the sort key, its leading columns
/// constant where it is dense.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn consolidate_bench() {
    const N: u64 = 65_536;
    let counter = Counter::instructions();
    let schema = make_schema_u64_i64();
    /// Row `i` as `(pk, payload)`.
    type Row = fn(u64) -> (u64, u64);
    let shapes: [(&str, Row, u64); 8] = [
        ("ascending", |i| (i + 1, i), N),
        ("ascending but the last", |i| (if i == N - 1 { 0 } else { i + 1 }, i), N),
        ("descending", |i| (N - i, i), N),
        ("scattered", |i| (mix(i) >> 8, i), N),
        ("scattered, 8 payloads per PK", |i| (mix(i / 8) >> 8, mix(i)), N),
        ("scattered, every row twice", |i| (mix(i / 2) >> 8, i / 2), N / 2),
        ("dense, scattered", |i| (dense(i, N), i), N),
        ("dense, 8 payloads per PK", |i| (dense(i / 8, N / 8), mix(i)), N),
    ];
    for (label, row, survivors) in shapes {
        let rows: Vec<(u64, i64, i64)> = (0..N).map(row).map(|(pk, v)| (pk, 1, v as i64)).collect();
        // The first pass takes the pool's first allocations.
        let [_, (out, instructions)] = [(); 2].map(|()| {
            let batch = make_batch_raw(&schema, &rows);
            counter.measure(|| black_box(batch).into_consolidated())
        });
        assert_eq!(out.count as u64, survivors);
        println!(
            "into_consolidated, {label:<28} {:>6.1} instr/row",
            instructions as f64 / N as f64
        );
    }
    for pk_cols in [2usize, 3, 5] {
        let schema = pk_payload_schema(&vec![TypeCode::U64; pk_cols]);
        for dense_pk in [false, true] {
            let [_, (out, instructions)] = [(); 2].map(|()| {
                let mut b = BatchBuilder::new(&schema);
                for i in 0..N {
                    let mut pk = vec![mix(i) as u128; pk_cols];
                    if dense_pk {
                        pk.fill(7);
                        pk[pk_cols - 1] = dense(i, N) as u128;
                    }
                    b.begin_row_natives(&pk, 1);
                    b.put_u64(i);
                    b.end_row();
                }
                let batch = b.finish();
                counter.measure(|| black_box(batch).into_consolidated())
            });
            assert_eq!(out.count as u64, N);
            let shape = if dense_pk { "dense, scattered" } else { "scattered" };
            println!(
                "into_consolidated, {:<28} {:>6.1} instr/row",
                format!("{shape}, {}-byte PK", 8 * pk_cols),
                instructions as f64 / N as f64
            );
        }
    }
}

/// `i` of `0..n`, a power of two, to its place in a scattered permutation of
/// `0..n`.
fn dense(i: u64, n: u64) -> u64 {
    i.wrapping_mul(0x9E37_79B9_7F4A_7C15) & (n - 1)
}

/// `into_consolidated` over the rows a window emits into a view keyed by its
/// partition: PKs in order, each with a run of rows whose payload stands out
/// of order — a retraction and an insertion per row, a few hundred rows a run.
/// Two payload columns fit one 128-bit payload sort key; three span two. The
/// columns are non-null I64, nullable I64 with every fourth value NULL, or F64.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn consolidate_runs_bench() {
    const N: u64 = 1 << 17;
    let counter = Counter::instructions();
    let kinds = [
        ("I64", TypeCode::I64, false),
        ("nullable I64", TypeCode::I64, true),
        ("F64", TypeCode::F64, false),
    ];
    for (kind, tc, nullable) in kinds {
        for payload_cols in [2usize, 3] {
            let schema = u64_pk_schema(&vec![SchemaColumn::new(tc, nullable); payload_cols]);
            for per_pk in [32u64, 256] {
                let [_, (out, instructions)] = [(); 2].map(|()| {
                    let mut b = BatchBuilder::new(&schema);
                    for i in 0..N / 2 {
                        let (pk, id) = (i / (per_pk / 2), mix(i) >> 16);
                        for (w, value) in [(-1, i % per_pk), (1, i % per_pk + 1)] {
                            b.begin_row(pk as u128, w);
                            let put = |b: &mut BatchBuilder, v: u64| match tc {
                                TypeCode::F64 => b.put_float(v as f64),
                                _ => b.put_int(v as u128),
                            };
                            put(&mut b, id);
                            for _ in 1..payload_cols {
                                match nullable && value % 4 == 0 {
                                    true => b.put_null(),
                                    false => put(&mut b, value),
                                }
                            }
                            b.end_row();
                        }
                    }
                    let batch = b.finish();
                    counter.measure(|| black_box(batch).into_consolidated())
                });
                assert_eq!(out.count as u64, N);
                println!(
                    "into_consolidated, {payload_cols} {kind:<12} columns, {per_pk:>3} rows per PK {:>7.1} instr/row",
                    instructions as f64 / N as f64
                );
            }
        }
    }
}

/// `into_consolidated` per batch over many small batches, as single-row writes
/// and small ticks hand it: rows of distinct PKs out of order, and an update —
/// a row retracted and its successor inserted under one PK — per PK. What a
/// call costs before any row does.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn consolidate_small_bench() {
    const BATCHES: u64 = 2_000;
    let counter = Counter::instructions();
    let schema = make_schema_u64_i64();
    for (label, rows, per_pk) in [
        ("rows", 2u64, 1u64),
        ("rows", 8, 1),
        ("rows", 64, 1),
        ("updates", 2, 2),
        ("updates", 8, 2),
    ] {
        let batches: Vec<Batch> = (0..BATCHES)
            .map(|k| {
                let rows: Vec<(u64, i64, i64)> = (0..rows)
                    .map(|i| {
                        (
                            mix(k * 100 + i / per_pk) >> 8,
                            [1, -1][(i % per_pk) as usize],
                            -(i as i64),
                        )
                    })
                    .collect();
                make_batch_raw(&schema, &rows)
            })
            .collect();
        let (kept, instructions) = counter.measure(|| {
            batches
                .into_iter()
                .map(|batch| black_box(black_box(batch).into_consolidated()).count)
                .sum::<usize>()
        });
        // An update's two rows differ in payload, so no row folds.
        assert_eq!(kept as u64, BATCHES * rows);
        println!(
            "into_consolidated, {:>2} {label:<7} a batch {:>9.1} instr/batch",
            rows / per_pk,
            instructions as f64 / BATCHES as f64
        );
    }
}

/// Retired instructions of `append_batch` over a string-bearing source: a 64-row
/// batch of 40-byte strings appended 10⁴ times into one growing destination.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn append_batch_strings_bench() {
    const ROWS: usize = 64;
    const APPENDS: usize = 10_000;
    let src = string_batch(&(0..ROWS as u64).map(|i| (i, format!("{i:040}"))).collect::<Vec<_>>());
    let counter = Counter::instructions();
    let (dst, instructions) = counter.measure(|| {
        let mut dst = Batch::empty_with_schema(src.schema());
        for _ in 0..APPENDS {
            dst.append_batch(black_box(&src));
        }
        dst
    });
    assert_eq!(dst.count, ROWS * APPENDS);
    println!(
        "append_batch_strings_bench: {} instr/append, heap {} bytes",
        instructions / APPENDS as u64,
        dst.blob.len()
    );
}

/// Retired instructions per row of `Batch::from_ranges` copying every other run
/// of a `U64` key over three `U64` columns, by run length.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn from_ranges_run_length_bench() {
    const ROWS: usize = 1 << 16;
    const PASSES: usize = 20;
    let schema = u64_pk_schema(&[SchemaColumn::new(TypeCode::U64, false); 3]);
    let mut b = BatchBuilder::new(&schema);
    for i in 0..ROWS as u128 {
        b.begin_row(i, 1);
        (1..4).for_each(|c| b.put_int(i * c));
        b.end_row();
    }
    let src = b.finish();
    let counter = Counter::instructions();
    for run in [1usize, 2, 4, 16, 256] {
        let ranges: Vec<(usize, usize)> = (0..ROWS).step_by(2 * run).map(|s| (s, s + run)).collect();
        let (copied, instructions) = counter.measure(|| {
            let mut copied = 0;
            for _ in 0..PASSES {
                let out = Batch::from_ranges(black_box(&src), black_box(&ranges), 0);
                copied += black_box(&out).count;
            }
            copied
        });
        assert_eq!(copied, PASSES * ROWS / 2);
        println!(
            "from_ranges_run_length_bench: run {run:>3}: {:.1} instr/row",
            instructions as f64 / copied as f64
        );
    }
}

/// Retired instructions and cycles of a relocating write — one long
/// cell, and 100 rows of two long cells — on a thread whose last cache left
/// the pool a small map, and on one whose last cache relocated 50 000
/// distinct spans.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn blob_cache_bench() {
    const WRITES: u64 = 200;
    let two_strings = u64_pk_schema(&[
        SchemaColumn::new(TypeCode::String, false),
        SchemaColumn::new(TypeCode::String, false),
    ]);
    let build = |rows: usize| {
        let mut b = BatchBuilder::new(&two_strings);
        for i in 0..rows {
            b.begin_row(i as u128, 1);
            b.put_string(&format!("{i:040}"));
            b.put_string(&format!("{i:041}"));
            b.end_row();
        }
        b.finish()
    };
    let (one, hundred, many) = (make_string_batch(&[(1, 1, &[b'x'; 40])]), build(100), build(25_000));
    // A relocating write of every row of `src`: `compacted` carries no heap.
    let write = |src: &Batch| black_box(black_box(src).compacted()).count;
    for src in [&one, &hundred, &many] {
        assert_eq!(write(src), src.count);
    }
    let counters = [("instr", Counter::instructions()), ("cycles", Counter::cycles())];
    for (shape, src) in [("one cell", &one), ("100 rows x 2 string columns", &hundred)] {
        for (unit, counter) in &counters {
            let small: u64 = (0..WRITES)
                .map(|_| {
                    write(src);
                    counter.measure(|| write(src)).1
                })
                .sum();
            let large: u64 = (0..WRITES)
                .map(|_| {
                    write(&many);
                    counter.measure(|| write(src)).1
                })
                .sum();
            println!(
                "blob_cache_bench: {shape}: {} {unit} after a write of its own shape, {} after a 50 000-span write",
                small / WRITES,
                large / WRITES,
            );
        }
    }
}
