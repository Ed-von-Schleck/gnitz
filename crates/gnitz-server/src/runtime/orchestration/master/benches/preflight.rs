use super::*;
use crate::test_support::make_schema_u64_i64;

/// The check-batch allocation path, each round over the arena the previous
/// round's batch returned to the pool.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn check_batch_build_bench() {
    use std::hint::black_box;
    use std::time::Instant;

    const ROWS: usize = 20_000;
    const ROUNDS: usize = 2_000;

    let schema = make_schema_u64_i64().pk_only();
    let keys: Vec<[u8; 8]> = (0..ROWS as u64).map(|i| i.to_be_bytes()).collect();

    // One warm round, so the pooled arena is already the right size and the
    // timed loop measures the steady state.
    drop(build_check_batch_pk_bytes(&schema, keys.iter().map(|k| &k[..])));

    let t = Instant::now();
    for _ in 0..ROUNDS {
        let b = build_check_batch_pk_bytes(&schema, keys.iter().map(|k| &k[..]));
        black_box(b.len());
    }
    let per_row = t.elapsed().as_nanos() as f64 / (ROUNDS * ROWS) as f64;
    println!("check_batch_build: {ROWS} rows x {ROUNDS} rounds, {per_row:.2} ns/row");
}
