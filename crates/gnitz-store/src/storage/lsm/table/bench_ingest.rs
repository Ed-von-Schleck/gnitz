//! Delta-ingest microbenchmark: the cost a subscriber pays to apply a received
//! delta, split into its two halves — `Batch::decode_from_wal_block` (the
//! validating decode every frame off a socket lands in) and
//! `Table::ingest_owned_batch`.
//!
//! Total rows are held constant across configs and only the *delta size* varies,
//! so the result answers "does applying one large coalesced delta cost less per
//! row than many small ones" — which is what sizes a per-subscription
//! accumulator cap.

use super::super::batch::Batch;
use super::{RecoverySource, Table};
use crate::schema::SchemaDescriptor;
use crate::test_support::{make_batch_raw, make_schema_u64_i64};

/// One delta of `n` rows at consecutive PKs from `base`, all weight +1. Raw, not
/// `Consolidated`: certifying it would short-circuit `into_consolidated` and the
/// bench would stop measuring the kernel ingest actually runs.
fn make_delta(schema: &SchemaDescriptor, base: u64, n: usize) -> Batch {
    let rows: Vec<(u64, i64, i64)> = (0..n as u64).map(|i| (base + i, 1, (base + i) as i64)).collect();
    make_batch_raw(schema, &rows)
}

#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn delta_ingest_bench() {
    use std::hint::black_box;
    use std::time::Instant;

    let schema = make_schema_u64_i64();
    const TOTAL: usize = 1_000_000;

    // Untimed warmup: thread-local batch pool + arena.
    {
        let dir = tempfile::tempdir().unwrap();
        let mut t = Table::new(
            dir.path().join("warm").to_str().unwrap(),
            schema,
            1,
            RecoverySource::Rederive { resume_at: None },
        )
        .unwrap();
        for k in 0..8 {
            t.ingest_owned_batch(make_delta(&schema, k * 1000, 1000)).unwrap();
        }
    }

    println!(
        "{:>10} {:>8} {:>12} {:>12} {:>12} {:>12} {:>10}",
        "delta_rows", "deltas", "wire_B/row", "decode_ns/r", "ingest_ns/r", "total_ns/r", "ram_runs"
    );

    for (id, &n) in [100usize, 1_000, 10_000, 100_000].iter().enumerate() {
        let k = TOTAL / n;

        // Untimed: build every delta and its wire block up front, so the timed
        // regions hold only the decode and the ingest.
        let mut blocks: Vec<Vec<u8>> = Vec::with_capacity(k);
        for j in 0..k {
            let b = make_delta(&schema, (j * n) as u64, n);
            blocks.push(b.encode_to_wire_vec(7, false));
        }
        let wire_bytes: usize = blocks.iter().map(Vec::len).sum();

        // Phase A — the validating decode, one per received frame.
        let t0 = Instant::now();
        let mut batches: Vec<Batch> = Vec::with_capacity(k);
        for blk in &blocks {
            let (b, _) = Batch::decode_from_wal_block(blk, &schema, false).unwrap();
            batches.push(b);
        }
        let decode_ns = t0.elapsed().as_nanos() as f64;
        black_box(&batches);

        // Phase B — the store ingest, into a table that grows across the run.
        let dir = tempfile::tempdir().unwrap();
        let mut table = Table::new(
            dir.path().join(format!("ingest{id}")).to_str().unwrap(),
            schema,
            100 + id as u32,
            RecoverySource::Rederive { resume_at: None },
        )
        .unwrap();
        let t1 = Instant::now();
        for b in batches {
            table.ingest_owned_batch(b).unwrap();
            // Drain per tick, so this measures the RAM-tier fold rather than the
            // memtable absorbing the whole run. Not what the worker does.
            table.flush().unwrap();
        }
        let ingest_ns = t1.elapsed().as_nanos() as f64;
        let runs = table.ram_tier.len();
        black_box(&table);

        let rows = TOTAL as f64;
        let total_ns = decode_ns + ingest_ns;
        println!(
            "{:>10} {:>8} {:>12.1} {:>12.1} {:>12.1} {:>12.1} {:>10}",
            n,
            k,
            wire_bytes as f64 / rows,
            decode_ns / rows,
            ingest_ns / rows,
            total_ns / rows,
            runs,
        );
    }

    // Discriminator: hold the delta size at 100 and vary the total, so the tick
    // COUNT varies with the store's final size. A flat ns/row means a fixed
    // per-flush cost; ns/row rising with the total means the RAM-tier fold is
    // re-merging a growing window once per tick, i.e. quadratic in tick count.
    println!();
    println!(
        "{:>10} {:>10} {:>8} {:>12} {:>14}",
        "delta_rows", "total_rows", "ticks", "ingest_ns/r", "ingest_ms_tot"
    );
    for (id, &total) in [100_000usize, 250_000, 500_000, 1_000_000].iter().enumerate() {
        let n = 100usize;
        let k = total / n;
        let mut batches: Vec<Batch> = Vec::with_capacity(k);
        for j in 0..k {
            batches.push(make_delta(&schema, (j * n) as u64, n));
        }
        let dir = tempfile::tempdir().unwrap();
        let mut table = Table::new(
            dir.path().join(format!("q{id}")).to_str().unwrap(),
            schema,
            200 + id as u32,
            RecoverySource::Rederive { resume_at: None },
        )
        .unwrap();
        let t = Instant::now();
        for b in batches {
            table.ingest_owned_batch(b).unwrap();
            table.flush().unwrap();
        }
        let ns = t.elapsed().as_nanos() as f64;
        black_box(&table);
        println!(
            "{:>10} {:>10} {:>8} {:>12.1} {:>14.1}",
            n,
            total,
            k,
            ns / total as f64,
            ns / 1e6
        );
    }

    // Churn shape: the same 1M rows, but every delta retracts the row it
    // replaces inside a bounded key set, so consolidation holds the resident
    // tier at ~keys rows instead of letting it grow to `total`. If the growth
    // above is what drives the cost, this stays flat in tick count.
    println!();
    println!(
        "{:>10} {:>10} {:>8} {:>12} {:>14}",
        "keys", "total_rows", "ticks", "ingest_ns/r", "ingest_ms_tot"
    );
    for (id, &keys) in [10_000u64, 50_000, 200_000].iter().enumerate() {
        let n = 100usize;
        let total = 1_000_000usize;
        let k = total / n;
        // `last[pk]` is the value actually stored for that key, so the
        // retraction is a byte-exact (PK, payload) match of the insert it
        // replaces and consolidation really does cancel it. Getting this wrong
        // produces a growing store wearing a churn label.
        let mut last: Vec<Option<i64>> = vec![None; keys as usize];
        let mut batches: Vec<Batch> = Vec::with_capacity(k);
        let mut rows: Vec<(u64, i64, i64)> = Vec::with_capacity(2 * n);
        for j in 0..k {
            rows.clear();
            for i in 0..n as u64 {
                let pk = ((j * n) as u64 + i) % keys;
                let new_val = (j + 1) as i64;
                if let Some(prev) = last[pk as usize] {
                    rows.push((pk, -1, prev));
                }
                rows.push((pk, 1, new_val));
                last[pk as usize] = Some(new_val);
            }
            batches.push(make_batch_raw(&schema, &rows));
        }
        let dir = tempfile::tempdir().unwrap();
        let mut table = Table::new(
            dir.path().join(format!("c{id}")).to_str().unwrap(),
            schema,
            300 + id as u32,
            RecoverySource::Rederive { resume_at: None },
        )
        .unwrap();
        let t = Instant::now();
        for b in batches {
            table.ingest_owned_batch(b).unwrap();
            table.flush().unwrap();
        }
        let ns = t.elapsed().as_nanos() as f64;
        black_box(&table);
        println!(
            "{:>10} {:>10} {:>8} {:>12.1} {:>14.1}",
            keys,
            total,
            k,
            ns / total as f64,
            ns / 1e6
        );
    }
}
