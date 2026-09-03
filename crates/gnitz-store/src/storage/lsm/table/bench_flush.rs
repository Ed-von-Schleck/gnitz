//! End-to-end microbenchmark for the RAM-tier drain path at production
//! thresholds: `fold_memtable_into_l0`, the tier's own fold at `FOLD_THRESHOLD`
//! (window re-merge + re-materialize) and the ceiling spill in `flush_to_ram`.
//!
//! This is the bench a compaction-policy change must move, and the one whose
//! `perf` profile must reproduce the e2e worker hot-symbol shape. As a child
//! module of `table` it reaches `Table`'s ingest/flush API and the
//! `FOLD_THRESHOLD` const directly.

use super::super::batch::Batch;
use super::super::run_set::FOLD_THRESHOLD;
use super::{RamBudgets, RecoverySource, Table};
use crate::schema::SchemaDescriptor;
use crate::test_support::pk_u64_two_i64_schema;

/// The view-output-store shape of `v_rev`: one hidden U64 group key + two
/// non-null I64 aggregates, 40 B/row, `FixedIntNonnull` payload comparator —
/// the dominant shape in the profiled flush workload.
/// Append one row `(pk, payload, payload, weight)` to `b` (both I64 payload
/// columns carry the same value so a retraction is an exact (PK,payload) match
/// of its insert).
fn push_row(b: &mut Batch, pk: u64, payload: i64, weight: i64) {
    b.extend_pk(pk as u128);
    b.extend_weight(&weight.to_le_bytes());
    b.extend_col(0, &payload.to_le_bytes());
    b.extend_col(1, &payload.to_le_bytes());
    b.commit_row(0);
}

/// One tick of `distinct(d)`: `d` rows with fresh keys `t*d .. t*d+d`, weight +1
/// — append-only window growth (the INSERT-stream shape), and the cheap arrival
/// order for the FLSM tree: every fold lands in fresh key space, so a vertical
/// overlaps nothing.
fn distinct_tick(schema: &SchemaDescriptor, t: usize, d: usize) -> Batch {
    let mut b = Batch::with_capacity(*schema, d.max(1));
    for i in 0..d {
        let k = (t * d + i) as u64;
        push_row(&mut b, k, k as i64, 1);
    }
    b
}

fn gen_distinct(schema: &SchemaDescriptor, d: usize, ticks: usize) -> Vec<Batch> {
    (0..ticks).map(|t| distinct_tick(schema, t, d)).collect()
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
            let mut b = Batch::with_capacity(*schema, d.max(1));
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

/// One tick of `d` rows at keys drawn uniformly over `keyspace`, weight +1 — the
/// arrival order a `map_reindex`'d store sees, and the expensive one for the FLSM
/// tree: every L0 fold reaches every guard, and every vertical lands on terminal
/// bytes already there. Streamed a tick at a time rather than materialized like
/// the generators above, which run far fewer ticks.
fn scatter_tick(schema: &SchemaDescriptor, rng: &mut crate::test_rng::Rng, d: usize, keyspace: u64) -> Batch {
    let mut keys: Vec<u64> = (0..d).map(|_| rng.gen_range(keyspace)).collect();
    keys.sort_unstable();
    let mut b = Batch::with_capacity(*schema, d.max(1));
    for k in keys {
        push_row(&mut b, k, k as i64, 1);
    }
    b
}

enum Gen {
    Distinct(usize),
    Churn(usize, usize),
}

/// The emptied scratch directory both compaction sweeps write into — a real
/// filesystem when `GNITZ_BENCH_DIR` names one, because shard bytes are what
/// they count and a tmpfs prices them differently. `tmp` owns the fallback root
/// and so must outlive the returned path.
/// The default budgets with `GNITZ_RAM_TIER_BYTES` applied: these benches
/// measure RAM-tier fold and compaction behaviour, so the tier is what a run
/// shrinks to reach the disk regime.
fn bench_budgets() -> RamBudgets {
    let defaults = RamBudgets::default();
    RamBudgets {
        ram_tier_bytes: crate::foundation::env::env_num("GNITZ_RAM_TIER_BYTES", defaults.ram_tier_bytes),
        ..defaults
    }
}

fn bench_dir(tmp: &tempfile::TempDir, name: String) -> std::path::PathBuf {
    let root = std::env::var("GNITZ_BENCH_DIR").unwrap_or_else(|_| tmp.path().to_str().unwrap().to_string());
    let dir = std::path::Path::new(&root).join(name);
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

/// Open a fresh compaction-stats window and return the RNG the scatter ticks
/// draw from. The seed lives here so every arm of every sweep replays the same
/// arrival order — `filter_share_of_compaction`'s two arms must report identical
/// compacted bytes or their cycle delta means nothing.
fn start_compaction_sweep() -> crate::test_rng::Rng {
    crate::storage::lsm::shard_index::cstats::reset();
    crate::test_rng::Rng::new(0x5EED_1234)
}

/// The drain-cadence cost at production thresholds. See the module doc.
///
/// `GNITZ_BENCH_FLUSH=0` drops the per-tick `flush()` and lets the memtable
/// drain on its own budget — the two arms price one cadence against the other.
///
/// ```text
/// for f in 1 0; do GNITZ_BENCH_FLUSH=$f \
///   cargo test -p gnitz-store --release flush_cadence_amplification_bench \
///     -- --ignored --nocapture --test-threads=1; done
/// ```
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn flush_cadence_amplification_bench() {
    use std::hint::black_box;
    use std::time::Instant;

    let per_tick_flush = crate::foundation::env::env_flag("GNITZ_BENCH_FLUSH", true);
    let schema = pk_u64_two_i64_schema();

    // Untimed warmup: warm the thread-local batch pool before the first config.
    {
        let dir = tempfile::tempdir().unwrap();
        let mut table = Table::with_budgets(
            dir.path().join("warmup").to_str().unwrap(),
            schema,
            1,
            RecoverySource::Rederive { resume_at: None },
            bench_budgets(),
        )
        .unwrap();
        for batch in gen_distinct(&schema, 4, 50) {
            table.ingest_owned_batch(batch).unwrap();
            if per_tick_flush {
                table.flush().unwrap();
            }
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
        let mut table = Table::with_budgets(
            dir.path().join(label).to_str().unwrap(),
            schema,
            100 + id as u32,
            RecoverySource::Rederive { resume_at: None },
            bench_budgets(),
        )
        .unwrap();

        let mut merged_out: usize = 0;
        let t = Instant::now();
        for batch in ticks {
            let runs_before = table.ram_tier.len();
            table.ingest_owned_batch(batch).unwrap();
            if per_tick_flush {
                table.flush().unwrap(); // Rederive → flush_prepare → flush_to_ram
            }
            // Only a fold shrinks the set, and it re-materializes the whole window
            // — so the post-fold row count is what that merge wrote.
            if table.ram_tier.len() < runs_before {
                merged_out += table.ram_tier.row_count();
            }
        }
        let secs = t.elapsed().as_secs_f64();

        assert!(merged_out > 0 || !per_tick_flush, "{label}: compaction never fired");
        assert!(
            table.ram_tier.len() <= FOLD_THRESHOLD,
            "{label}: RAM-tier run bound violated",
        );
        black_box(merged_out);

        let rps = ingested as f64 / secs;
        let amp = merged_out as f64 / ingested as f64;
        let arm = match per_tick_flush {
            true => "per-tick-flush",
            false => "memtable-budget",
        };
        println!(
            "flush_cadence/{label} [{arm}]: {ingested} rows / {ticks_n} ticks in {secs:.3}s = {rps:.0} ingest-rows/s  merged-out {merged_out} rows  amp {amp:.1}x"
        );
    }
}

/// Compaction bytes read per ingest byte, and the largest single unit each
/// compaction phase reads — the two quantities the FLSM byte targets exist to
/// bound. Stated in bytes because wall-clock on the development machine varies
/// 3.6× on bit-identical compaction work.
///
/// Acceptance: every phase's `max_in` settles at or under `2 × R` (printed), and
/// `bytes_in/ingest` grows as √X rather than linearly across a doubling sweep.
/// The second binds only once `l1_target` (printed) exceeds `16 R` — shrink
/// `GNITZ_RAM_TIER_BYTES` to reach that regime rather than growing the sweep.
///
/// ```text
/// for t in 2000 4000 8000 16000; do
///   GNITZ_BENCH_TICKS=$t GNITZ_BENCH_KEYSPACE=200000000 \
///     cargo test -p gnitz-store --release compaction_amplification_bench \
///     -- --ignored --nocapture --test-threads=1
/// done
/// ```
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn compaction_amplification_bench() {
    const ROWS_PER_TICK: usize = 4096;

    use crate::foundation::env::{env_flag, env_num};
    let ticks_n: usize = env_num("GNITZ_BENCH_TICKS", 2000);
    let keyspace: u64 = env_num("GNITZ_BENCH_KEYSPACE", 200_000_000);
    // Both arrival orders: a monotone stream never makes a vertical overlap
    // anything, so on its own it cannot tell a policy fix from a regression.
    let monotone = env_flag("GNITZ_BENCH_MONOTONE", false);

    let schema = pk_u64_two_i64_schema();
    let tmp = tempfile::tempdir().unwrap();
    let dir = bench_dir(&tmp, format!("wa_{ticks_n}_{keyspace}"));

    let mut table = Table::with_budgets(
        dir.to_str().unwrap(),
        schema,
        7,
        RecoverySource::Rederive { resume_at: None },
        bench_budgets(),
    )
    .unwrap();

    use crate::storage::lsm::shard_index::cstats;
    let mut rng = start_compaction_sweep();
    for t in 0..ticks_n {
        let batch = match monotone {
            true => distinct_tick(&schema, t, ROWS_PER_TICK),
            false => scatter_tick(&schema, &mut rng, ROWS_PER_TICK, keyspace),
        };
        table.ingest_owned_batch(batch).unwrap();
        table.flush().unwrap();
    }

    let phases = cstats::dump();
    let total_in: u64 = phases.iter().map(|p| p.in_bytes).sum();
    let total_out: u64 = phases.iter().map(|p| p.out_bytes).sum();
    // Every spill is folded out of L0 exactly once, so the L0 fold's input is the
    // bytes this store was handed — measured, where a nominal row width would not
    // be (the flush schema's 40 B row lands at ~17 B once its constant regions
    // collapse).
    let spilled = phases[0].in_bytes.max(1);
    println!(
        "compaction_amplification/{ticks_n}t {} keyspace={keyspace}: {}",
        if monotone { "monotone" } else { "scattered" },
        table.shard_index.tree_report()
    );
    for (name, p) in cstats::PHASE_NAMES.iter().zip(&phases) {
        let mean = p.in_bytes.checked_div(p.n as u64).unwrap_or(0);
        println!(
            "  {name:12} n={:<6} max_in={:>12} mean_in={:>12} in={:>13} out={:>13} in_files={}",
            p.n, p.max_in, mean, p.in_bytes, p.out_bytes, p.in_files
        );
    }
    println!(
        "  per spilled byte: read {:.2}  written {:.2}   (spilled {spilled} B)",
        total_in as f64 / spilled as f64,
        total_out as f64 / spilled as f64,
    );
    assert!(phases[0].n > 0, "no compaction ran — the sweep measures nothing");
}

/// The share of a probed store's work that is PK-filter work: the same ingest
/// run with the filter on and off, differenced by an external `perf stat`.
/// Read the difference in cycles — `BinaryFuse8`'s construction trades retired
/// instructions for cache behaviour, so the two counters need not move together
/// and instructions alone can report the wrong sign.
///
/// `SalReplay` because it is the one recovery source whose stores build a
/// filter, and no per-tick `flush()` because a `SalReplay` barrier publishes a
/// manifest per tick and its fsyncs dominate everything being measured — ingest
/// overflow alone drives the RAM tier, the spill and the compaction. Both arms
/// must report the same compacted bytes; if they diverge the arms are not
/// comparable and the cycle delta means nothing.
///
/// ```text
/// BIN=$(find target/release/deps -maxdepth 1 -type f -executable \
///     -name 'gnitz_store-*' ! -name '*.*' | head -1)
/// for t in 4000 16000; do
///   for f in 0 1; do
///     GNITZ_BENCH_DIR=$(realpath tmp)/bfs GNITZ_NO_PK_FILTER=$f GNITZ_BENCH_TICKS=$t \
///       perf stat -e cycles,instructions -x, $BIN filter_share_of_compaction \
///       --ignored --nocapture --test-threads=1
///   done
/// done
/// ```
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn filter_share_of_compaction() {
    const ROWS_PER_TICK: usize = 4096;

    use crate::foundation::env::{env_flag, env_num};
    let ticks_n: usize = env_num("GNITZ_BENCH_TICKS", 4000);
    let keyspace: u64 = env_num("GNITZ_BENCH_KEYSPACE", 200_000_000);
    let filter_off = env_flag("GNITZ_NO_PK_FILTER", false);

    let schema = pk_u64_two_i64_schema();
    let tmp = tempfile::tempdir().unwrap();
    let dir = bench_dir(&tmp, format!("fs_{ticks_n}_{}", filter_off as u8));

    let mut table = Table::with_budgets(
        dir.to_str().unwrap(),
        schema,
        9,
        RecoverySource::SalReplay,
        bench_budgets(),
    )
    .unwrap();
    // The off arm. Force-off only: every store that skips by policy is
    // `Rederive` and never carries a base-table workload, so forcing a filter
    // *on* would measure nothing.
    if filter_off {
        table.shard_index.set_skip_pk_filter_for_test(true);
    }

    use crate::storage::lsm::shard_index::cstats;
    let mut rng = start_compaction_sweep();
    let mut ingested: usize = 0;
    for _ in 0..ticks_n {
        let batch = scatter_tick(&schema, &mut rng, ROWS_PER_TICK, keyspace);
        ingested += batch.count;
        table.ingest_owned_batch(batch).unwrap();
    }

    let phases = cstats::dump();
    let compactions: usize = phases.iter().map(|p| p.n).sum();
    let bytes_in: u64 = phases.iter().map(|p| p.in_bytes).sum();
    assert!(compactions > 0, "no compaction ran — the run measures nothing");
    println!(
        "filter_share/{ticks_n}t filter={}: {ingested} rows  {compactions} compactions  {bytes_in} compacted bytes in",
        if filter_off { "off" } else { "on" },
    );
}
