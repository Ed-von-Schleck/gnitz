//! End-to-end microbenchmark for the RAM-tier drain path at production
//! thresholds: `fold_memtable_into_ram_tier`, the tier's own fold at
//! `FOLD_THRESHOLD` and the ceiling spill in `fold_to_ram`.
//!
//! This is the bench a compaction-policy change must move, and the one whose
//! `perf` profile must reproduce the e2e worker hot-symbol shape. As a child
//! module of `table` it reaches `Table`'s ingest/flush API and the
//! `FOLD_THRESHOLD` const directly.

use super::super::run_set::FOLD_THRESHOLD;
use super::{RecoverySource, StoreBudgets, Table, DEFAULT_RAM_TIER_BYTES};
use crate::test_support::pk_u64_two_i64_schema;
use gnitz_zset::repr::Batch;
use gnitz_zset::repr::BatchBuilder;
use gnitz_zset::schema::SchemaDescriptor;

/// Append one row `(pk, payload, payload, weight)` to `b` (both I64 payload
/// columns carry the same value so a retraction is an exact (PK,payload) match
/// of its insert).
fn push_row(b: &mut BatchBuilder, pk: u64, payload: i64, weight: i64) {
    b.begin_row(pk as u128, weight);
    b.put_int(payload as u128);
    b.put_int(payload as u128);
    b.end_row();
}

/// One tick of `distinct(d)`: `d` rows with fresh keys `t*d .. t*d+d`, weight +1
/// — append-only window growth (the INSERT-stream shape), and the cheap arrival
/// order for the FLSM tree: every fold lands in fresh key space, so a vertical
/// overlaps nothing.
fn distinct_tick(schema: &SchemaDescriptor, t: usize, d: usize) -> Batch {
    let mut b = BatchBuilder::new(schema);
    for i in 0..d {
        let k = (t * d + i) as u64;
        push_row(&mut b, k, k as i64, 1);
    }
    b.finish()
}

fn gen_distinct(schema: &SchemaDescriptor, d: usize, ticks: usize) -> Vec<Batch> {
    (0..ticks).map(|t| distinct_tick(schema, t, d)).collect()
}

/// `churn(h, d)`: `d/2` updates per tick over a hot key set of size `h`, round
/// robin. Each update retracts the key's current payload (once it has one) and
/// inserts a fresh unique payload — the retract+insert cancels the prior
/// (PK,payload) row, so the steady-state net window is ≈ `h` rows (the
/// UPDATE/re-aggregation shape where the same rows are re-folded forever).
fn gen_churn(schema: &SchemaDescriptor, h: usize, d: usize, ticks: usize) -> Vec<Batch> {
    let updates_per_tick = d / 2;
    let mut last: Vec<Option<i64>> = vec![None; h];
    let mut counter: usize = 0;
    (0..ticks)
        .map(|_| {
            let mut b = BatchBuilder::new(schema);
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
            b.finish()
        })
        .collect()
}

/// The arrival shapes `compaction_amplification_bench` sweeps, one tick of
/// `(key, payload, weight)` rows at a time. A scattered tick reaches every
/// guard; the others are the orders real tables arrive in.
struct Arrival {
    shape: String,
    keyspace: u64,
    ticks: usize,
    rng: crate::test_support::Rng,
    /// `churn`: each hot key's live payload, and the round-robin position.
    live: Vec<Option<i64>>,
    next: u64,
}

impl Arrival {
    fn tick(&mut self, t: usize, d: u64) -> Vec<(u64, i64, i64)> {
        let ks = self.keyspace;
        let fresh = |i: u64| t as u64 * d + i;
        match self.shape.as_str() {
            // Uniform over the key space: the order a `map_reindex`'d store sees.
            "scattered" => (0..d)
                .map(|_| self.rng.gen_range(ks))
                .map(|k| (k, k as i64, 1))
                .collect(),
            // Ascending and fresh: an INSERT stream, and every delta store.
            "monotone" => (0..d).map(fresh).map(|k| (k, k as i64, 1)).collect(),
            // 90 % of the rows in the lowest 1 % of the key space.
            "skew" => (0..d)
                .map(|_| match self.rng.gen_range(10) {
                    0 => self.rng.gen_range(ks),
                    _ => self.rng.gen_range(ks / 100),
                })
                .map(|k| (k, k as i64, 1))
                .collect(),
            // 64 interleaved ascending sequences, `(tenant, seq)`.
            "tenants" => (0..d)
                .map(|i| ((i % 64) << 40) | (t as u64 * (d / 64) + i / 64))
                .map(|k| (k, k as i64, 1))
                .collect(),
            // An ascending load for the first half, then scattered fresh
            // payloads over the loaded range.
            "bulk" => match t < self.ticks / 2 {
                true => (0..d).map(fresh).map(|k| (k, k as i64, 1)).collect(),
                false => {
                    let span = (self.ticks / 2) as u64 * d;
                    (0..d).map(|_| (self.rng.gen_range(span), -(t as i64) - 1, 1)).collect()
                }
            },
            // Round-robin updates over `keyspace` hot keys: retract and insert.
            "churn" => {
                self.live.resize(ks as usize, None);
                let mut rows = Vec::with_capacity(d as usize);
                for _ in 0..d / 2 {
                    let k = (self.next % ks) as usize;
                    let payload = self.next as i64;
                    self.next += 1;
                    if let Some(old) = self.live[k].replace(payload) {
                        rows.push((k as u64, old, -1));
                    }
                    rows.push((k as u64, payload, 1));
                }
                rows
            }
            other => panic!("unknown GNITZ_BENCH_SHAPE {other}"),
        }
    }
}

/// `pk_cols` U64 PK columns over two I64 payload columns. The key varies in the
/// last PK column alone, so past two columns every key shares its leading 16
/// bytes.
fn wide_pk_schema(pk_cols: usize) -> SchemaDescriptor {
    use gnitz_wire::TypeCode;
    use gnitz_zset::schema::SchemaColumn;
    let mut cols = vec![SchemaColumn::new(TypeCode::U64, false); pk_cols];
    cols.extend([SchemaColumn::new(TypeCode::I64, false); 2]);
    SchemaDescriptor::new(&cols, &(0..pk_cols as u32).collect::<Vec<_>>())
}

fn wide_key(pk_cols: usize, k: u64) -> Vec<u8> {
    let mut pk = vec![0u8; (pk_cols - 1) * 8];
    pk.extend_from_slice(&k.to_be_bytes());
    pk
}

/// `GNITZ_BENCH_SCRAMBLE=1` makes the payloads incompressible, so a skeleton
/// row is a fraction of a full one and a `deh:` budget has something to evict.
fn arrival_batch(schema: &SchemaDescriptor, pk_cols: usize, mut rows: Vec<(u64, i64, i64)>) -> Batch {
    rows.sort_unstable();
    let scramble = gnitz_foundation::env::env_flag("GNITZ_BENCH_SCRAMBLE", false);
    let mut b = BatchBuilder::new(schema);
    for (k, payload, weight) in rows {
        let payload = match scramble {
            true => (payload as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15) as i64,
            false => payload,
        };
        b.begin_row_bytes(&wide_key(pk_cols, k), weight);
        b.put_int(payload as u128);
        b.put_int(payload as u128);
        b.end_row();
    }
    b.finish()
}

enum Gen {
    Distinct(usize),
    Churn(usize, usize),
}

/// The default budgets with `GNITZ_RAM_TIER_BYTES` applied: these benches
/// measure RAM-tier fold and compaction behaviour, so the tier is what a run
/// shrinks to reach the disk regime.
fn bench_budgets() -> StoreBudgets {
    StoreBudgets::new(gnitz_foundation::env::env_num(
        "GNITZ_RAM_TIER_BYTES",
        DEFAULT_RAM_TIER_BYTES,
    ))
}

/// [`bench_budgets`] under `GNITZ_BENCH_BUDGET`: `deh:<bytes>` a
/// capacity-bounded view's output store, `drop:<bytes>` a delta store.
fn budgeted(budgets: StoreBudgets) -> StoreBudgets {
    let spec = std::env::var("GNITZ_BENCH_BUDGET").unwrap_or_default();
    match spec.split_once(':') {
        Some(("deh", n)) => budgets.bounded(Some(n.parse().unwrap())),
        Some(("drop", n)) => budgets.delta(n.parse().unwrap()),
        _ => budgets,
    }
}

/// The emptied scratch directory both compaction sweeps write into — a real
/// filesystem when `GNITZ_BENCH_DIR` names one, because shard bytes are what
/// they count and a tmpfs prices them differently. `tmp` owns the fallback root
/// and so must outlive the returned path.
fn bench_dir(tmp: &tempfile::TempDir, name: String) -> std::path::PathBuf {
    let root = std::env::var("GNITZ_BENCH_DIR").unwrap_or_else(|_| tmp.path().to_str().unwrap().to_string());
    let dir = std::path::Path::new(&root).join(name);
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

/// Open a fresh compaction-stats window and return the RNG the scatter ticks
/// draw from. The seed lives here so every arm of every sweep replays the same
/// arrival order — `filter_share_of_compaction_bench`'s two arms must report
/// identical compacted bytes or their cycle delta means nothing.
fn start_compaction_sweep() -> crate::test_support::Rng {
    crate::storage::shard_index::cstats::reset();
    crate::test_support::Rng::new(0x5EED_1234)
}

/// The drain-cadence cost at production thresholds. See the module doc.
///
/// `GNITZ_BENCH_FLUSH=0` drops the per-tick `fold_to_ram()` and lets the memtable
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

    let per_tick_flush = gnitz_foundation::env::env_flag("GNITZ_BENCH_FLUSH", true);
    let schema = pk_u64_two_i64_schema();

    // Untimed warmup: warm the thread-local batch pool before the first config.
    {
        let dir = tempfile::tempdir().unwrap();
        let mut table = Table::new(
            dir.path().join("warmup").to_str().unwrap(),
            schema,
            RecoverySource::Rederive { resume_at: None },
            bench_budgets(),
        )
        .unwrap();
        for batch in gen_distinct(&schema, 4, 50) {
            table.ingest_owned_batch(batch).unwrap();
            if per_tick_flush {
                table.fold_to_ram().unwrap();
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

    for (label, gen, ticks_n) in configs {
        let ticks: Vec<Batch> = match gen {
            Gen::Distinct(d) => gen_distinct(&schema, d, ticks_n),
            Gen::Churn(h, d) => gen_churn(&schema, h, d, ticks_n),
        };
        let ingested: usize = ticks.iter().map(|b| b.len()).sum();

        let dir = tempfile::tempdir().unwrap();
        let mut table = Table::new(
            dir.path().join(label).to_str().unwrap(),
            schema,
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
                table.fold_to_ram().unwrap();
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
/// bound. Stated in bytes: the counts are reproducible where wall-clock is not.
///
/// Acceptance: every phase's `max_in` settles at or under `2 × R` (printed), and
/// `bytes_in/ingest` grows as √X rather than linearly across a doubling sweep.
/// The second binds only once `l1_target` (printed) exceeds `16 R` — shrink
/// `GNITZ_RAM_TIER_BYTES` to reach that regime rather than growing the sweep.
///
/// `GNITZ_BENCH_SHAPE` picks the arrival order ([`Arrival`]),
/// `GNITZ_BENCH_PK_COLS` the PK width, `GNITZ_BENCH_BUDGET` a bounded or a delta
/// store. Under a `deh:` budget the last line counts how many of one mid-run
/// tick's keys still read without hydration.
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

    use gnitz_foundation::env::env_num;
    let ticks_n: usize = env_num("GNITZ_BENCH_TICKS", 2000);
    let keyspace: u64 = env_num("GNITZ_BENCH_KEYSPACE", 200_000_000);
    let shape = std::env::var("GNITZ_BENCH_SHAPE").unwrap_or_else(|_| "scattered".into());
    let pk_cols: usize = env_num("GNITZ_BENCH_PK_COLS", 1);

    let schema = wide_pk_schema(pk_cols);
    let tmp = tempfile::tempdir().unwrap();
    let dir = bench_dir(&tmp, format!("wa_{ticks_n}_{keyspace}"));

    let mut table = Table::new(
        dir.to_str().unwrap(),
        schema,
        RecoverySource::Rederive { resume_at: None },
        budgeted(bench_budgets()),
    )
    .unwrap();

    use crate::storage::shard_index::{cstats, CompactionKind};
    let mut arrival = Arrival {
        shape: shape.clone(),
        keyspace,
        ticks: ticks_n,
        rng: start_compaction_sweep(),
        live: Vec::new(),
        next: 0,
    };
    let mut sampled: Vec<u64> = Vec::new();
    for t in 0..ticks_n {
        let rows = arrival.tick(t, ROWS_PER_TICK as u64);
        if t == ticks_n / 2 {
            sampled = rows.iter().map(|r| r.0).collect();
        }
        table.ingest_owned_batch(arrival_batch(&schema, pk_cols, rows)).unwrap();
        table.fold_to_ram().unwrap();
    }

    let phases = cstats::dump();
    let total_in: u64 = phases.values().map(|p| p.in_bytes).sum();
    let total_out: u64 = phases.values().map(|p| p.out_bytes).sum();
    // Every spill is folded out of L0 exactly once, so the L0 fold's input is the
    // bytes this store was handed, as the shard encoding left them.
    let spilled = phases.get(&CompactionKind::L0Fold).map_or(1, |p| p.in_bytes);
    println!(
        "compaction_amplification/{ticks_n}t {shape} pk_cols={pk_cols} keyspace={keyspace}: {}",
        table.shard_index.tree_report()
    );
    for (kind, p) in &phases {
        println!(
            "  {:12} n={:<6} max_in={:>12} mean_in={:>12} in={:>13} out={:>13} in_files={}",
            format!("{kind:?}"),
            p.n,
            p.max_in,
            p.in_bytes / p.n as u64,
            p.in_bytes,
            p.out_bytes,
            p.in_files
        );
    }
    println!(
        "  per spilled byte: read {:.2}  written {:.2}   (spilled {spilled} B)",
        total_in as f64 / spilled as f64,
        total_out as f64 / spilled as f64,
    );
    if table.has_skeleton_rows() {
        let hydrated = sampled
            .iter()
            .filter(|&&k| {
                let key = wide_key(pk_cols, k);
                let mut skeleton = false;
                table
                    .shard_index
                    .find_pk_bytes(&key, gnitz_zset::schema::key::probe_key(&key), |shard, _| {
                        skeleton |= shard.is_skeleton()
                    });
                !skeleton
            })
            .count();
        println!(
            "  hydrated: {hydrated} of the {} keys of tick {}",
            sampled.len(),
            ticks_n / 2
        );
    }
    assert!(!phases.is_empty(), "no compaction ran — the sweep measures nothing");
}

/// The share of a probed store's work that is PK-filter work: the same ingest
/// run with the filter on and off, differenced by an external `perf stat`.
/// Read the difference in cycles — `BinaryFuse8`'s construction trades retired
/// instructions for cache behaviour, so the two counters need not move together
/// and instructions alone can report the wrong sign.
///
/// The on arm is `SalReplay`, the one recovery source whose stores build a
/// filter, and the off arm a rederived store, which builds none. No per-tick
/// `flush()`, because a `SalReplay` barrier publishes a manifest per tick and its fsyncs dominate everything being measured — ingest
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
///       perf stat -e cycles,instructions -x, $BIN filter_share_of_compaction_bench \
///       --ignored --nocapture --test-threads=1
///   done
/// done
/// ```
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn filter_share_of_compaction_bench() {
    const ROWS_PER_TICK: usize = 4096;

    use gnitz_foundation::env::{env_flag, env_num};
    let ticks_n: usize = env_num("GNITZ_BENCH_TICKS", 4000);
    let keyspace: u64 = env_num("GNITZ_BENCH_KEYSPACE", 200_000_000);
    let filter_off = env_flag("GNITZ_NO_PK_FILTER", false);

    let schema = pk_u64_two_i64_schema();
    let tmp = tempfile::tempdir().unwrap();
    let dir = bench_dir(&tmp, format!("fs_{ticks_n}_{}", filter_off as u8));

    // The recovery source is what decides whether a store builds a filter.
    let recovery = match filter_off {
        true => RecoverySource::Rederive { resume_at: None },
        false => RecoverySource::SalReplay,
    };
    let mut table = Table::new(dir.to_str().unwrap(), schema, recovery, bench_budgets()).unwrap();

    use crate::storage::shard_index::cstats;
    let mut arrival = Arrival {
        shape: "scattered".into(),
        keyspace,
        ticks: ticks_n,
        rng: start_compaction_sweep(),
        live: Vec::new(),
        next: 0,
    };
    let mut ingested: usize = 0;
    for _ in 0..ticks_n {
        let rows = arrival.tick(0, ROWS_PER_TICK as u64);
        let batch = arrival_batch(&schema, 1, rows);
        ingested += batch.len();
        table.ingest_owned_batch(batch).unwrap();
    }

    let phases = cstats::dump();
    let compactions: usize = phases.values().map(|p| p.n).sum();
    let bytes_in: u64 = phases.values().map(|p| p.in_bytes).sum();
    assert!(compactions > 0, "no compaction ran — the run measures nothing");
    println!(
        "filter_share/{ticks_n}t filter={}: {ingested} rows  {compactions} compactions  {bytes_in} compacted bytes in",
        if filter_off { "off" } else { "on" },
    );
}
