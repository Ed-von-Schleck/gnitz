//! The base-table write path: `enforce_unique_pk` followed by the
//! `ingest_borrowed_batch` it feeds. Every client INSERT, UPDATE and DELETE goes
//! through that pair, and no other bench in the tree reaches it — both table
//! benches build `Rederive` stores, which are never base tables, never carry a
//! shard PK filter and are never point-probed.
//!
//! Two arms, because the two shapes cost nothing alike:
//!
//!   insert : fresh keys, every probe a miss. The whole batch passes through
//!            verbatim, so nothing is rebuilt and the cost is the probe.
//!   update : a bounded key set drawn at random, so nearly every row finds a
//!            stored row. Every such row emits a retraction and cuts the
//!            verbatim run, and the probe walks a real LSM lookup.
//!
//! The update arm draws its keys at random on purpose: a monotone key stream
//! would let each probe land on the page the last one warmed, which is the one
//! thing production arrival order (`decode_client_wire`, arrival-ordered) does
//! not do.
//!
//! Each arm names its rows per push, because the two batch sizes drain through
//! different tiers: at 1000 rows (a bulk load) a push is ~32 KiB, so the memtable
//! passes its 192 KiB budget after ~6 of them and never reaches `FOLD_THRESHOLD`
//! — every fold is the RAM tier's. At 1 row (a single-statement DML) the
//! memtable folds every 16 pushes and drains after ~4900 rows, so its own fold
//! is what the arm prices.
//!
//! Report `ns/row`, not the wall clock of the whole run.

use super::enforce_unique_pk;
use crate::storage::Batch;
use crate::storage::{RecoverySource, StoreBudgets, Table};
use crate::test_support::{make_batch_raw, make_schema_u64_i64};

/// Rows every arm pushes, however it splits them.
const TOTAL_ROWS: usize = 500_000;
/// Key set of the update arm. Large enough that the store spills and the probe
/// crosses tiers, small enough that every push after the first few is an update.
const HOT_KEYS: u64 = 50_000;

#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn unique_pk_bench() {
    use std::hint::black_box;
    use std::time::Instant;

    let schema = make_schema_u64_i64();
    let dir = tempfile::tempdir().unwrap();

    // Untimed warmup: thread-local batch pool + arena.
    {
        let mut t = Table::new(
            dir.path().join("warm").to_str().unwrap(),
            schema,
            1,
            RecoverySource::SalReplay,
            StoreBudgets::default(),
        )
        .unwrap();
        for p in 0..8u64 {
            let rows: Vec<(u64, i64, i64)> = (0..1000).map(|i| (p * 1000 + i, 1, i as i64)).collect();
            let eff = enforce_unique_pk(&t, &schema, make_batch_raw(&schema, &rows));
            t.ingest_borrowed_batch(&eff).unwrap();
        }
    }

    println!(
        "{:>8} {:>10} {:>12} {:>12} {:>14}",
        "arm", "rows", "eff_rows", "ns/row", "ms_total"
    );

    let arms = [
        ("insert", 0u64, 1_000usize),
        ("update", HOT_KEYS, 1_000),
        ("insert1", 0, 1),
    ];
    for (id, &(label, hot, rows_per_push)) in arms.iter().enumerate() {
        let pushes = TOTAL_ROWS / rows_per_push;
        // Untimed: build every push up front, so the timed region holds only the
        // enforcement walk and the store ingest.
        let mut rng = crate::test_rng::Rng::new(0x5EED_1234);
        let batches: Vec<Batch> = (0..pushes)
            .map(|p| {
                let rows: Vec<(u64, i64, i64)> = (0..rows_per_push)
                    .map(|i| {
                        let seq = (p * rows_per_push + i) as u64;
                        let pk = if hot == 0 { seq } else { rng.gen_range(hot) };
                        (pk, 1, seq as i64)
                    })
                    .collect();
                make_batch_raw(&schema, &rows)
            })
            .collect();

        let mut table = Table::new(
            dir.path().join(label).to_str().unwrap(),
            schema,
            100 + id as u32,
            RecoverySource::SalReplay,
            StoreBudgets::default(),
        )
        .unwrap();

        let mut eff_rows = 0usize;
        let t = Instant::now();
        for b in batches {
            let eff = enforce_unique_pk(&table, &schema, b);
            eff_rows += eff.count;
            table.ingest_borrowed_batch(&eff).unwrap();
        }
        let ns = t.elapsed().as_nanos() as f64;
        black_box(&table);

        let rows = (pushes * rows_per_push) as f64;
        println!(
            "{:>8} {:>10} {:>12} {:>12.1} {:>14.1}",
            label,
            rows as u64,
            eff_rows,
            ns / rows,
            ns / 1e6
        );
    }
}
