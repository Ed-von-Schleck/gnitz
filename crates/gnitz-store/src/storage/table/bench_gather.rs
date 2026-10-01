//! Point-gather microbenchmark over a table holding both tiers. Ignored by
//! default; run with:
//!
//! ```text
//! cargo test -p gnitz-store --release pk_set_gather_bench -- --ignored --nocapture --test-threads=1
//! ```

use super::{RecoverySource, StoreBudgets, Table};
use crate::test_support::{make_batch, make_schema_u64_i64};

/// `Table::gather` over RAM runs plus L0 shards at 1, 64 and 4096 keys spread over
/// the table's key span, opening inside the timed region.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn pk_set_gather_bench() {
    use gnitz_foundation::perf::Counter;
    const ROWS: u64 = 1 << 18;
    const ITERS: usize = 200;
    let schema = make_schema_u64_i64();
    let dir = tempfile::tempdir().unwrap();
    let mut t = Table::new(
        dir.path().to_str().unwrap(),
        schema,
        RecoverySource::Rederive { resume_at: None },
        StoreBudgets::new(1 << 20),
    )
    .unwrap();
    // Eight interleaved rounds spill to shards; the last, small one stays in RAM.
    for r in 0..8u64 {
        let rows: Vec<(u64, i64, i64)> = (0..ROWS / 8).map(|k| (k * 8 + r, 1, (k * 8 + r) as i64)).collect();
        t.ingest_owned_batch(make_batch(&schema, &rows)).unwrap();
    }
    let late: Vec<(u64, i64, i64)> = (0..64u64).map(|k| (k * (ROWS / 64) + 1, 1, 7)).collect();
    t.ingest_owned_batch(make_batch(&schema, &late)).unwrap();
    assert!(!t.all_shard_arcs().is_empty(), "the rows reached the shard tier");
    for n in [1u64, 64, 4096] {
        let step = ROWS / (n + 1);
        let key_bytes: Vec<[u8; 8]> = (1..=n).map(|i| (i * step).to_be_bytes()).collect();
        let keys = gnitz_wire::PkKeys::from_keys(8, key_bytes.iter().map(|k| &k[..]));
        let counter = Counter::instructions().expect("instructions counter");
        let mut instructions = 0;
        for _ in 0..ITERS {
            let (out, i) = counter.measure(|| t.gather(keys.clone(), None).drain_chunk(usize::MAX));
            std::hint::black_box(out);
            instructions += i;
        }
        println!("pk_set_gather {n} keys: {} instr/iter", instructions / ITERS as u64);
    }
}
