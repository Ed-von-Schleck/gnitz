use super::super::*;
use super::tests::fresh;
use super::tests::spread;
use super::tests::stride_schema;
use super::tests::trailing_gk;
use super::tests::trailing_key_batch;
use crate::test_support::{make_batch_raw, make_schema_u64_i64, Rng};
use gnitz_zset::schema::key::probe_key;

/// `ShardIndex::find_pk_bytes` over a tree the upkeep built from scattered
/// spills, at a PK stride in each width arm: instructions per probe.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn shard_probe_bench() {
    use gnitz_foundation::perf::Counter;
    const RUN_ROWS: u64 = 8192;
    const KEYSPACE: u64 = 200_000_000;
    const RUNS: usize = 81 * (L0_COMPACT_THRESHOLD + 1) - 1;
    type Probed = fn(held: u64) -> u64;
    let arms: [(&str, Probed, bool); 3] = [
        ("held", |k| k, true),
        ("absent, inside the extents", |k| k | 1, false),
        ("absent, above every shard", |k| KEYSPACE + k, false),
    ];
    let counter = Counter::instructions();
    for pk_cols in [1usize, 2, 3, 5] {
        let tmp = tempfile::tempdir().unwrap();
        let mut idx = fresh(tmp.path(), stride_schema(pk_cols));
        let mut rng = Rng::new(0x5EED_1234);
        let mut held = Vec::new();
        for _ in 0..RUNS {
            let mut keys: Vec<u64> = (0..RUN_ROWS).map(|_| 2 * rng.gen_range(KEYSPACE / 2)).collect();
            keys.sort_unstable();
            keys.dedup();
            held.extend(keys.iter().step_by(16));
            idx.append_l0_run(&trailing_key_batch(pk_cols, keys).into_consolidated())
                .unwrap();
            idx.maintain().unwrap();
        }
        let (l0, [l1, terminal]) = idx.level_shape();
        assert!(
            l0 == L0_COMPACT_THRESHOLD && l1 > 1 && terminal > 1,
            "L0 must be full over a guarded L1 and terminal level, got {l0}/{l1}/{terminal}",
        );
        println!(
            "shard_probe_bench stride {}: {l0} L0 shards, {l1} L1 and {terminal} terminal guards",
            pk_cols * 8
        );
        for (label, probed, is_held) in arms {
            let keys: Vec<PkBuf> = held.iter().map(|&k| trailing_gk(pk_cols, probed(k))).collect();
            let (hits, instructions) = counter.measure(|| {
                let mut hits = 0;
                for k in &keys {
                    let key = k.pk_bytes();
                    idx.find_pk_bytes(key, probe_key(key), |_, _| hits += 1);
                }
                hits
            });
            assert!(
                if is_held { hits >= keys.len() } else { hits == 0 },
                "{label}: {hits} hits"
            );
            println!("  {label}: {:.1} instr/probe", instructions as f64 / keys.len() as f64);
        }
    }
}

/// What the FLSM compactions read and write per spilled byte, by trigger, and
/// the largest single input of each in units of `R`.
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
