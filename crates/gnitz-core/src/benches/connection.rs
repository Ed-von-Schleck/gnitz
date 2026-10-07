use super::*;
use crate::test_support::{encode_wal_block, wide_rows, wide_schema};
use std::hint::black_box;

/// Instructions to validate and to encode a push of four nullable columns, at
/// one row and at 100k, with no NULL and with one NULL per row.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn push_validate_bench() {
    let counter = gnitz_foundation::perf::Counter::instructions();
    for (nrows, iters) in [(1usize, 2000u64), (100_000, 20)] {
        for null_every in [0usize, 4] {
            let schema = wide_schema(5, true);
            let batch = wide_rows(&schema, nrows, null_every);
            let target = Target { tid: 7, token: 1 };
            let (mut v, mut e) = (0, 0);
            for _ in 0..iters {
                v += counter
                    .measure(|| black_box(black_box(&batch).validate(&schema)).unwrap())
                    .1;
                let push = || Request::Push {
                    target,
                    schema: &schema,
                    batch: &batch,
                    mode: WireConflictMode::Update,
                };
                e += counter.measure(|| black_box(push().encode().unwrap())).1;
            }
            let per_row = |x: u64| x as f64 / (iters * nrows as u64) as f64;
            println!(
                "push_validate_bench {nrows} rows, null every {null_every}: validate {:.2} instr/row, encode {:.2}",
                per_row(v),
                per_row(e),
            );
        }
    }
}

/// Instructions to decode a one-row reply into a fresh batch and into one that
/// already owns its buffers, by column count.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn reply_alloc_cost_bench() {
    const ROUNDS: u64 = 2000;
    let counter = gnitz_foundation::perf::Counter::instructions();
    for ncols in [4usize, 16, 64] {
        let schema = wide_schema(ncols, false);
        let block = encode_wal_block(&wide_rows(&schema, 1, 0));
        let (mut fresh, mut warm) = (0, 0);
        for _ in 0..ROUNDS {
            fresh += counter
                .measure(|| {
                    let mut sink = ZSetBatch::new(&schema);
                    decode_wal_block_into(&mut sink, black_box(&block), &schema).unwrap();
                    black_box(&sink);
                })
                .1;
            let mut sink = ZSetBatch::with_capacity(&schema, 4);
            warm += counter
                .measure(|| {
                    decode_wal_block_into(&mut sink, black_box(&block), &schema).unwrap();
                    black_box(&sink);
                })
                .1;
        }
        println!(
            "reply_alloc_cost_bench {ncols} cols: new + decode + drop {} instr, decode into owned buffers {}",
            fresh / ROUNDS,
            warm / ROUNDS
        );
    }
}
