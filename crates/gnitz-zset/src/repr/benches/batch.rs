use super::*;
use crate::repr::BatchBuilder;
use crate::schema::{SchemaColumn, SchemaDescriptor, TypeCode};
use crate::test_support::{make_batch_raw, make_schema_u64_i64, make_string_batch};

/// `release_buffers` against `drop(take())` on the case that dominates: clearing
/// a register that is already free. The VM does that for every register of every
/// plan once per epoch, so the per-call constant is the whole comparison.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn batch_release_bench() {
    use crate::test_support::bench_time;
    const ITERS: usize = 2_000_000;
    let schema = make_schema_u64_i64();

    let mut regs: Vec<Batch> = (0..64).map(|_| Batch::empty_with_schema(&schema)).collect();
    let release = bench_time(ITERS / 64, || {
        for b in &mut regs {
            std::hint::black_box(&mut *b).release_buffers();
        }
    });
    let take = bench_time(ITERS / 64, || {
        for b in &mut regs {
            drop(std::hint::black_box(&mut *b).take());
        }
    });

    println!(
        "already-empty register clear: release_buffers {:.1} ns, drop(take()) {:.1} ns",
        release.as_nanos() as f64 / ITERS as f64,
        take.as_nanos() as f64 / ITERS as f64,
    );
}

/// `into_consolidated` over a batch with no claim, by the order its rows stand
/// in: ascending already, ascending but for its last row, descending, and
/// scattered. The second is the input a check for the first costs the most on.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn consolidate_bench() {
    use std::hint::black_box;
    const N: u64 = 65_536;
    const ITERS: u64 = 20;
    let counter = gnitz_foundation::perf::Counter::instructions();
    let schema = make_schema_u64_i64();
    let scatter = |i: u64| i.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 8;
    type Key = fn(u64) -> u64;
    let shapes: [(&str, Key); 4] = [
        ("ascending", |i| i + 1),
        ("ascending but the last", |i| if i == N - 1 { 0 } else { i + 1 }),
        ("descending", |i| N - i),
        ("scattered", scatter),
    ];
    for (label, key) in shapes {
        let rows: Vec<(u64, i64, i64)> = (0..N).map(|i| (key(i), 1, i as i64)).collect();
        let mut instructions = 0;
        for _ in 0..ITERS {
            let batch = make_batch_raw(&schema, &rows);
            let (out, n) = counter.measure(|| black_box(batch).into_consolidated());
            assert_eq!(out.count as u64, N);
            instructions += n;
        }
        println!(
            "into_consolidated, {label:<22} {:>6.1} instr/row",
            instructions as f64 / (ITERS * N) as f64
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
    let values: Vec<Vec<u8>> = (0..ROWS).map(|i| format!("{i:040}").into_bytes()).collect();
    let rows: Vec<(u64, i64, &[u8])> = values.iter().enumerate().map(|(i, v)| (i as u64, 1, &v[..])).collect();
    let src = make_string_batch(&rows);
    let counter = gnitz_foundation::perf::Counter::instructions();
    let (dst, instructions) = counter.measure(|| {
        let mut dst = Batch::empty_with_schema(src.schema());
        for _ in 0..APPENDS {
            dst.append_batch(std::hint::black_box(&src));
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
    let u64_col = SchemaColumn::new(TypeCode::U64, false);
    let schema = SchemaDescriptor::new(&[u64_col; 4], &[0]);
    let mut b = BatchBuilder::new(&schema);
    for i in 0..ROWS as u128 {
        b.begin_row(i, 1);
        (1..4).for_each(|c| b.put_int(i * c));
        b.end_row();
    }
    let src = b.finish();
    let counter = gnitz_foundation::perf::Counter::instructions();
    for run in [1usize, 2, 4, 16, 256] {
        let ranges: Vec<(usize, usize)> = (0..ROWS).step_by(2 * run).map(|s| (s, s + run)).collect();
        let (copied, instructions) = counter.measure(|| {
            let mut copied = 0;
            for _ in 0..PASSES {
                let out = Batch::from_ranges(std::hint::black_box(&src), std::hint::black_box(&ranges), 0);
                copied += std::hint::black_box(&out).count;
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

/// Retired instructions and cycles of a relocating append session — one long
/// cell, and 100 rows of two long cells — on a thread whose last session left
/// the pool a small map, and on one whose last session relocated 50 000
/// distinct spans.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn blob_cache_session_bench() {
    const SESSIONS: u64 = 200;
    let two_strings = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::String, false),
            SchemaColumn::new(TypeCode::String, false),
        ],
        &[0],
    );
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
    // A relocating session over every row of `src`: `compacted` carries no heap.
    let session = |src: &Batch| std::hint::black_box(std::hint::black_box(src).compacted()).count;
    let counters = [
        ("instr", gnitz_foundation::perf::Counter::instructions()),
        ("cycles", gnitz_foundation::perf::Counter::cycles()),
    ];
    for (shape, src) in [("one cell", &one), ("100 rows x 2 string columns", &hundred)] {
        for (unit, counter) in &counters {
            let small: u64 = (0..SESSIONS)
                .map(|_| {
                    session(src);
                    counter.measure(|| session(src)).1
                })
                .sum();
            let large: u64 = (0..SESSIONS)
                .map(|_| {
                    session(&many);
                    counter.measure(|| session(src)).1
                })
                .sum();
            println!(
                "blob_cache_session_bench: {shape}: {} {unit} after a session of its own shape, {} after a 50 000-span session",
                small / SESSIONS,
                large / SESSIONS,
            );
        }
    }
}
