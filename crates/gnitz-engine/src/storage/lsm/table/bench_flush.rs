//! End-to-end microbenchmark for the RAM-tier per-tick flush policy: the
//! composition of `fold_memtable_into_l0` (memtable consolidation + per-run
//! bloom), `compact_in_memory` (window re-merge + re-materialize + re-bloom)
//! and `spill_in_memory_to_disk`, driven at the production trigger thresholds.
//!
//! This is the bench a compaction-policy change must move, and the one whose
//! `perf` profile must reproduce the e2e worker hot-symbol shape. As a child
//! module of `table` it reaches `Table`'s ingest/flush API and the
//! `INMEM_COMPACT_THRESHOLD` const directly.

use super::super::batch::Batch;
use super::{RecoverySource, Table, INMEM_COMPACT_THRESHOLD};
use crate::schema::{type_code, SchemaColumn, SchemaDescriptor};

/// The view-output-store shape of `v_rev`: one hidden U64 group key + two
/// non-null I64 aggregates, 40 B/row, `FixedIntNonnull` payload comparator —
/// the dominant shape in the profiled flush workload.
fn make_schema_flush() -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 0),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    )
}

/// Append one row `(pk, payload, payload, weight)` to `b` (both I64 payload
/// columns carry the same value so a retraction is an exact (PK,payload) match
/// of its insert).
fn push_row(b: &mut Batch, pk: u64, payload: i64, weight: i64) {
    b.extend_pk(pk as u128);
    b.extend_weight(&weight.to_le_bytes());
    b.extend_null_bmp(&0u64.to_le_bytes());
    b.extend_col(0, &payload.to_le_bytes());
    b.extend_col(1, &payload.to_le_bytes());
    b.count += 1;
}

/// `distinct(d)`: tick `t` emits `d` rows with fresh keys `t*d .. t*d+d`,
/// weight +1 — append-only window growth (the INSERT-stream shape).
fn gen_distinct(schema: &SchemaDescriptor, d: usize, ticks: usize) -> Vec<Batch> {
    (0..ticks)
        .map(|t| {
            let mut b = Batch::with_schema(*schema, d.max(1));
            for i in 0..d {
                let k = (t * d + i) as u64;
                push_row(&mut b, k, k as i64, 1);
            }
            b
        })
        .collect()
}

/// `churn(h, d)`: `d/2` updates per tick over a hot key set of size `h`, round
/// robin. Each update retracts the key's current payload (once it has one) and
/// inserts a fresh unique payload — the retract+insert cancels the prior
/// (PK,payload) row, so the steady-state net window is ≈ `h` rows (the
/// UPDATE/re-aggregation shape where the same rows are re-merged forever).
fn gen_churn(schema: &SchemaDescriptor, h: usize, d: usize, ticks: usize) -> Vec<Batch> {
    let updates_per_tick = d / 2;
    let mut last: Vec<Option<i64>> = vec![None; h];
    let mut counter: usize = 0;
    (0..ticks)
        .map(|_| {
            let mut b = Batch::with_schema(*schema, d.max(1));
            for _ in 0..updates_per_tick {
                let k = counter % h;
                let payload = counter as i64;
                counter += 1;
                if let Some(old) = last[k] {
                    push_row(&mut b, k as u64, old, -1);
                }
                push_row(&mut b, k as u64, payload, 1);
                last[k] = Some(payload);
            }
            b
        })
        .collect()
}

enum Gen {
    Distinct(usize),
    Churn(usize, usize),
}

/// The per-tick flush-policy cost at production thresholds. See the module doc.
///
/// ```text
/// cargo test -p gnitz-engine --release flush_cadence_amplification_bench \
///     -- --ignored --nocapture --test-threads=1
/// ```
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn flush_cadence_amplification_bench() {
    use std::hint::black_box;
    use std::time::Instant;

    // 256 KiB arena matching the `Routing::Hashed` per-partition arena.
    const ARENA: u64 = 256 << 10;
    let schema = make_schema_flush();

    // Untimed warmup: warm the thread-local batch pool before the first config.
    {
        let dir = tempfile::tempdir().unwrap();
        let mut table = Table::new(
            dir.path().join("warmup").to_str().unwrap(),
            schema,
            1,
            ARENA,
            RecoverySource::Rederive,
        )
        .unwrap();
        for batch in gen_distinct(&schema, 4, 50) {
            table.ingest_owned_batch(batch).unwrap();
            table.flush().unwrap();
        }
    }

    let configs: [(&str, Gen, usize); 6] = [
        ("distinct_d1", Gen::Distinct(1), 2000),
        ("distinct_d16", Gen::Distinct(16), 2000),
        ("distinct_d4096", Gen::Distinct(4096), 200),
        ("churn_h8192_d2", Gen::Churn(8192, 2), 2000),
        ("churn_h8192_d32", Gen::Churn(8192, 32), 2000),
        ("churn_h65536_d512", Gen::Churn(65536, 512), 1000),
    ];

    for (id, (label, gen, ticks_n)) in configs.into_iter().enumerate() {
        let ticks: Vec<Batch> = match gen {
            Gen::Distinct(d) => gen_distinct(&schema, d, ticks_n),
            Gen::Churn(h, d) => gen_churn(&schema, h, d, ticks_n),
        };
        let ingested: usize = ticks.iter().map(|b| b.count).sum();

        let dir = tempfile::tempdir().unwrap();
        let mut table = Table::new(
            dir.path().join(label).to_str().unwrap(),
            schema,
            100 + id as u32,
            ARENA,
            RecoverySource::Rederive,
        )
        .unwrap();

        let mut merged_out: usize = 0;
        let t = Instant::now();
        for batch in ticks {
            // Sampled before the fold; +1 accounts for the incoming fold's run.
            let runs_before = table.in_memory_runs().count();
            table.ingest_owned_batch(batch).unwrap();
            table.flush().unwrap(); // Rederive → flush_prepare → flush_to_ram
            if runs_before + 1 > INMEM_COMPACT_THRESHOLD {
                merged_out += table.in_memory_runs().map(|b| b.count).sum::<usize>();
            }
        }
        let secs = t.elapsed().as_secs_f64();

        assert!(merged_out > 0, "{label}: compaction never fired");
        assert!(
            table.in_memory_runs().count() <= INMEM_COMPACT_THRESHOLD,
            "{label}: RAM-tier run bound violated",
        );
        black_box(merged_out);

        let rps = ingested as f64 / secs;
        let amp = merged_out as f64 / ingested as f64;
        println!(
            "flush_cadence/{label}: {ingested} rows / {ticks_n} ticks in {secs:.3}s = {rps:.0} ingest-rows/s  merged-out {merged_out} rows  amp {amp:.1}x"
        );
    }
}
