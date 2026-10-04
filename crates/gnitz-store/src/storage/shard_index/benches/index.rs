use super::super::*;
use super::tests::fresh;
use super::tests::seed_guard;
use super::tests::spread;
use super::tests::stride_schema;
use super::tests::trailing_gk;
use crate::test_support::{make_batch_opk, make_batch_raw, make_schema_u64_i64, Rng};
use gnitz_zset::repr::Batch;
use gnitz_zset::schema::key::probe_key;

/// `ShardIndex::find_pk_bytes` over a tree holding all three levels, at a narrow
/// and a wide PK stride: instructions per probe for present and absent keys.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn shard_probe_bench() {
    use gnitz_foundation::perf::Counter;
    use std::hint::black_box;
    const TERMINAL_GUARDS: u64 = 64;
    const L1_GUARDS: u64 = 16;
    const SPAN: u64 = 1 << 20; // keys one terminal guard covers
    const ROWS: u64 = 2000;
    const PROBES: u64 = 200_000;
    const STEP: u64 = (SPAN / ROWS) & !1;
    let counter = Counter::instructions();
    for pk_cols in [1usize, 3] {
        let tmp = tempfile::tempdir().unwrap();
        let mut idx = fresh(tmp.path(), stride_schema(pk_cols));
        let total = TERMINAL_GUARDS * SPAN;
        let run = |base: u64, step: u64| -> Batch {
            let rows: Vec<_> = (0..ROWS)
                .map(|i| (trailing_gk(pk_cols, base + i * step).pk_bytes().to_vec(), 1, i as i64))
                .collect();
            make_batch_opk(&stride_schema(pk_cols), &rows)
        };
        // Terminal keys are even, so an odd key inside the range is absent there.
        for g in 0..TERMINAL_GUARDS {
            let base = g * SPAN;
            seed_guard(&mut idx, TERMINAL, trailing_gk(pk_cols, base), &run(base, STEP), 1);
        }
        let l1_span = total / L1_GUARDS;
        for g in 0..L1_GUARDS {
            for f in 0..4u64 {
                let base = g * l1_span;
                seed_guard(
                    &mut idx,
                    L1,
                    trailing_gk(pk_cols, base),
                    &run(base + 2 * f, l1_span / ROWS),
                    2,
                );
            }
        }
        for f in 0..4u64 {
            idx.append_l0_run(&run(2 * f, total / ROWS)).unwrap();
        }
        {
            let mut rng = Rng::new(0x5EED_1234);
            let ranges: Vec<(PkBuf, PkBuf)> = (0..PROBES)
                .map(|_| {
                    let key = rng.gen_range(TERMINAL_GUARDS) * SPAN + rng.gen_range(ROWS) * STEP;
                    (trailing_gk(pk_cols, key), trailing_gk(pk_cols, key + 4 * STEP))
                })
                .collect();
            let mut found = 0usize;
            let ((), instructions) = counter.measure(|| {
                for &(lo, hi) in &ranges {
                    found += idx.shard_arcs_in_range(lo, hi, true).count();
                }
            });
            black_box(found);
            println!(
                "shard_range stride {}: {:.1} instr/open ({:.2} shards each)",
                pk_cols * 8,
                instructions as f64 / PROBES as f64,
                found as f64 / PROBES as f64
            );
        }
        for (label, odd) in [("present", 0u64), ("absent", 1)] {
            let mut rng = Rng::new(0x5EED_1234);
            let keys: Vec<PkBuf> = (0..PROBES)
                .map(|_| {
                    let key = rng.gen_range(TERMINAL_GUARDS) * SPAN + rng.gen_range(ROWS) * STEP;
                    trailing_gk(pk_cols, key | odd)
                })
                .collect();
            let mut hits = 0usize;
            let ((), instructions) = counter.measure(|| {
                for k in &keys {
                    let key = k.pk_bytes();
                    idx.find_pk_bytes(key, probe_key(key), |_, _| hits += 1);
                }
            });
            black_box(hits);
            println!(
                "shard_probe stride {} {label}: {:.1} instr/probe ({hits} hits)",
                pk_cols * 8,
                instructions as f64 / PROBES as f64
            );
        }
    }
}

/// What the FLSM compactions read and write per spilled byte, by trigger, and
/// the largest single input of each in units of `R` — the two quantities the
/// byte targets bound, the second asserted. A spill arrives as `Table` delivers one: a consolidated
/// run into L0, then the upkeep. One arm per arrival order and budget a store
/// meets, each asserting the triggers it exists to reach; the two scattered arms
/// are the same store at one and four times the data.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn compaction_amplification_bench() {
    use CompactionKind::*;
    const RUN_ROWS: u64 = 8192;
    const KEYSPACE: u64 = 200_000_000;
    const HOT: u64 = 8 * RUN_ROWS;
    type Rows = Vec<(u64, i64, i64)>;
    // Fresh ascending keys: an INSERT stream, and every delta store.
    let ascending = |run: u64, _: &mut Rng| -> Rows {
        (run * RUN_ROWS..(run + 1) * RUN_ROWS)
            .map(|k| (k, 1, spread(k)))
            .collect()
    };
    // Uniform over the key space: a store keyed by a hash or a join key.
    let scattered = |_: u64, rng: &mut Rng| -> Rows {
        let keys = (0..RUN_ROWS).map(|_| rng.gen_range(KEYSPACE));
        keys.map(|k| (k, 1, spread(k))).collect()
    };
    // Update `u` rewrites key `u % HOT`, retracting the row update `u - HOT`
    // left there: every run retracts rows an older shard holds.
    let churn = |run: u64, _: &mut Rng| -> Rows {
        let updates = run * RUN_ROWS / 2..(run + 1) * RUN_ROWS / 2;
        let retraction = |u: u64| u.checked_sub(HOT).map(|old| (u % HOT, -1, spread(old)));
        updates
            .flat_map(|u| [retraction(u), Some((u % HOT, 1, spread(u)))])
            .flatten()
            .collect()
    };
    // Label, the rows of a run, the budget, the runs spilled, the triggers that must run.
    type Arm = (
        &'static str,
        fn(u64, &mut Rng) -> Rows,
        ShardBudget,
        u64,
        &'static [CompactionKind],
    );
    let arms: [Arm; 6] = [
        ("ascending", ascending, ShardBudget::Unbounded, 400, &[L0Fold]),
        (
            "scattered",
            scattered,
            ShardBudget::Unbounded,
            400,
            &[L0Fold, GuardSplit, Vertical],
        ),
        (
            "scattered, 4x",
            scattered,
            ShardBudget::Unbounded,
            1600,
            &[L0Fold, GuardSplit, Vertical],
        ),
        ("churn", churn, ShardBudget::Unbounded, 400, &[L0Fold, GuardSplit]),
        (
            "scattered, dehydrating",
            scattered,
            ShardBudget::Dehydrate(16 << 20),
            400,
            &[BandCut, Dehydrate],
        ),
        (
            "ascending, dropping",
            ascending,
            ShardBudget::Drop(16 << 20),
            400,
            &[L0Fold],
        ),
    ];

    let schema = make_schema_u64_i64();
    let tmp = tempfile::tempdir().unwrap();
    for (label, rows, budget, runs, reaches) in arms {
        let dir = tmp.path().join(label);
        std::fs::create_dir(&dir).unwrap();
        let mut idx = ShardIndex::open(dir.to_str().unwrap(), schema, budget, true, &ShardSet::default()).unwrap();
        let mut rng = Rng::new(0x5EED_1234);
        let mut spilled = 0;
        cstats::reset();
        for run in 0..runs {
            let run = make_batch_raw(&schema, &rows(run, &mut rng)).into_consolidated();
            idx.append_l0_run(&run).unwrap();
            spilled += idx.levels[L0].entries().last().unwrap().shard.file_len();
            idx.maintain().unwrap();
        }

        let phases = cstats::dump();
        for kind in reaches {
            assert!(phases.contains_key(kind), "{label}: no {kind:?} ran");
        }
        assert_eq!(
            idx.dropped_max() != PkBuf::zeroed(8),
            matches!(budget, ShardBudget::Drop(_)),
            "{label}: dropped a guard"
        );
        let r = idx.l0_run_bytes;
        let levels: Vec<String> = idx
            .levels
            .iter()
            .map(|l| format!("{}B/{}g/{}f", l.bytes(), l.guards.len(), l.entries().count()))
            .collect();
        println!(
            "compaction_amplification_bench {label}: {spilled} B spilled, R={r}, L1 target {}, levels {}",
            idx.l1_target_bytes(),
            levels.join(" ")
        );
        for (kind, p) in &phases {
            assert!(p.max_in <= 2 * r, "{label}: a {kind:?} read {} B, past 2 R", p.max_in);
            println!(
                "  {:<10} n={:<5} largest input {:5.2} R, read {:6.2} and wrote {:6.2} per spilled byte",
                format!("{kind:?}"),
                p.n,
                p.max_in as f64 / r as f64,
                p.in_bytes as f64 / spilled as f64,
                p.out_bytes as f64 / spilled as f64,
            );
        }
    }
}
