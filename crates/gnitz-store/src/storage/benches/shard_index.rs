use super::tests::{fresh, spread, stride_schema, trailing_gk, trailing_key_batch};
use super::*;
use crate::storage::Cut;
use crate::test_support::{make_batch, make_schema_u64_i64};
use gnitz_foundation::perf::Counter;
use gnitz_foundation::rng::Rng;
use gnitz_zset::schema::probe_key;

/// Rows per spilled run, drawn from a key space this wide.
const RUN_ROWS: u64 = 8192;
const KEYSPACE: u64 = 200_000_000;
const SEED: u64 = 0x5EED_1234;

/// The two read entries over a tree the upkeep built from scattered spills, at
/// a PK stride in each width arm: instructions per `ShardIndex::holding` to a
/// key's first holder, as a unique-PK probe reads it, and per bounded `ShardIndex::shard_arcs` with the shards it hands a cursor.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn shard_probe_bench() {
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
        let mut rng = Rng::new(SEED);
        let mut held = Vec::new();
        for _ in 0..RUNS {
            let mut keys: Vec<u64> = (0..RUN_ROWS).map(|_| 2 * rng.gen_range(KEYSPACE / 2)).collect();
            keys.sort_unstable();
            keys.dedup();
            held.extend(keys.iter().step_by(16));
            idx.append_l0_run(&trailing_key_batch(pk_cols, keys).into_consolidated())
                .unwrap();
            idx.maintain(u64::MAX).unwrap();
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
            let keys: Vec<(PkBuf, u64)> = (held.iter())
                .map(|&k| trailing_gk(pk_cols, probed(k)))
                .map(|k| (k, probe_key(k.pk_bytes())))
                .collect();
            let (hits, instructions) = counter.measure(|| {
                (keys.iter())
                    .filter(|(k, fp)| idx.holding(k.pk_bytes(), *fp).next().is_some())
                    .count()
            });
            assert_eq!(hits, if is_held { keys.len() } else { 0 }, "{label}");
            println!("  {label}: {:.1} instr/probe", instructions as f64 / keys.len() as f64);
        }
        // Label, the range at a held key, the fewest and the most shards it may select.
        type Range = (&'static str, fn(u64) -> (u64, u64), usize, usize);
        let ranges: [Range; 3] = [
            (
                "one key",
                |k| (k, k),
                1,
                l0 + GUARD_FILE_THRESHOLD.iter().sum::<usize>(),
            ),
            ("a 1/1024 band", |k| (k, k + KEYSPACE / 1024), 1, idx.shard_count()),
            ("the key line", |_| (0, KEYSPACE), idx.shard_count(), idx.shard_count()),
        ];
        for (label, range, at_least, at_most) in ranges {
            let bounds: Vec<(PkBuf, PkBuf)> = held
                .iter()
                .step_by(64)
                .map(|&k| range(k))
                .map(|(lo, hi)| (trailing_gk(pk_cols, lo), trailing_gk(pk_cols, hi)))
                .collect();
            let (selected, instructions) = counter.measure(|| {
                let mut selected = Vec::with_capacity(bounds.len());
                for &(lo, hi) in &bounds {
                    selected.push(idx.shard_arcs(Some((lo.pk_bytes(), hi.pk_bytes())), Cut::Now).count());
                }
                selected
            });
            assert!(
                selected.iter().all(|n| (at_least..=at_most).contains(n)),
                "{label}: a range selected under {at_least} or over {at_most} shards"
            );
            println!(
                "  range, {label}: {:.1} instr/open, {:.1} of {} shards",
                instructions as f64 / bounds.len() as f64,
                selected.iter().sum::<usize>() as f64 / bounds.len() as f64,
                idx.shard_count()
            );
        }
    }
}

/// What the FLSM compactions read and write per spilled byte, by trigger, and
/// the largest single input of each in units of `R`; beside them the user-space
/// instructions a spilled row costs to write and to keep up, and the kernel's share as requests: the shard files written,
/// and the shards a barrier syncs at each cadence in `BARRIERS`.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn compaction_amplification_bench() {
    use CompactionKind::*;
    const HOT: u64 = 8 * RUN_ROWS;
    const UPDATED: u64 = 1_000_000;
    /// Spills between two barriers.
    const BARRIERS: [u64; 3] = [1, 8, 64];
    type Rows = Vec<(u64, i64, i64)>;
    // Fresh ascending keys: an INSERT stream.
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
    // Update `u` rewrites one of `UPDATED` keys, in an order that scatters them
    // over the key space, retracting the row update `u - UPDATED` left there:
    // a retraction's row sits wherever the tree has since moved it.
    let updates = |run: u64, _: &mut Rng| -> Rows {
        let key = |u: u64| u % UPDATED * 0x9E37_79B1 % UPDATED;
        let retraction = |u: u64| u.checked_sub(UPDATED).map(|old| (key(u), -1, spread(old)));
        (run * RUN_ROWS / 2..(run + 1) * RUN_ROWS / 2)
            .flat_map(|u| [retraction(u), Some((key(u), 1, spread(u)))])
            .flatten()
            .collect()
    };
    // Label, the rows of a run, the budget and whether the store ends within
    // it, whether its shards carry a PK filter, the runs spilled, the triggers
    // that must run.
    type Arm = (
        &'static str,
        fn(u64, &mut Rng) -> Rows,
        Option<(u64, bool)>,
        bool,
        u64,
        &'static [CompactionKind],
    );
    const ALL: &[CompactionKind] = &[L0Fold, GuardSplit, TierFold, Vertical];
    let arms: [Arm; 9] = [
        (
            "scattered, under the L1 floor",
            scattered,
            None,
            false,
            60,
            &[L0Fold, GuardSplit],
        ),
        (
            "scattered updates",
            updates,
            None,
            false,
            400,
            &[L0Fold, GuardSplit, TierFold],
        ),
        ("ascending", ascending, None, false, 400, &[L0Fold]),
        ("scattered", scattered, None, false, 400, ALL),
        // A store something probes by PK.
        ("scattered, PK filters", scattered, None, true, 400, ALL),
        ("scattered, 4x", scattered, None, false, 1600, ALL),
        ("churn", churn, None, false, 400, &[L0Fold, GuardSplit, GuardMerge]),
        (
            "scattered, dehydrating",
            scattered,
            Some((24 << 20, true)),
            false,
            400,
            &[BandCut, Dehydrate],
        ),
        // A capacity under what the skeleton rows alone take.
        (
            "scattered, at the skeleton floor",
            scattered,
            Some((8 << 20, false)),
            false,
            400,
            &[BandCut, Dehydrate],
        ),
    ];

    let schema = make_schema_u64_i64();
    let tmp = tempfile::tempdir().unwrap();
    let counter = Counter::instructions();
    for (label, rows, budget, filters, runs, reaches) in arms {
        let dir = tmp.path().join(label);
        // A rederived store's shards carry no PK filter.
        let recovery = match filters {
            true => RecoverySource::SalReplay,
            false => RecoverySource::Rederive { resume_at: None },
        };
        let mut idx = ShardIndex::open(
            dir.to_str().unwrap(),
            schema,
            recovery,
            budget.map(|(cap, _)| cap),
            false,
        )
        .unwrap();
        let mut rng = Rng::new(SEED);
        let (mut spilled, mut spilled_rows) = (0, 0);
        let (mut spill, mut upkeep) = (0, 0);
        // A cadence, the seq its last barrier published through and the bytes
        // spilled by then, the shards and the bytes its barriers synced.
        let mut barriers = BARRIERS.map(|every| (every, 0, 0, 0usize, 0u64));
        for n in 0..runs {
            let run = make_batch(&schema, &rows(n, &mut rng)).into_consolidated();
            spill += counter.measure(|| idx.append_l0_run(&run).unwrap()).1;
            spilled += idx.l0.last().unwrap().shard.file_len();
            spilled_rows += run.len();
            upkeep += counter.measure(|| idx.maintain(u64::MAX).unwrap()).1;
            for (every, through, covered, shards, bytes) in &mut barriers {
                if (n + 1) % *every == 0 {
                    for e in idx.all_entries().filter(|e| e.seq > *through) {
                        *shards += 1;
                        *bytes += e.shard.file_len();
                    }
                    (*through, *covered) = (idx.dir.last_seq(), spilled);
                }
            }
        }

        let phases = &idx.stats;
        for kind in reaches {
            assert!(phases.contains_key(kind), "{label}: no {kind:?} ran");
        }
        let resident = idx.resident_bytes();
        if let Some((cap, fits)) = budget {
            assert_eq!(resident <= cap, fits, "{label}: {resident} B resident");
        }
        let r = idx.l0_run_bytes;
        let l0 = format!("{}B/{}f", idx.l0_bytes(), idx.l0.len());
        let levels: Vec<String> = std::iter::once(l0)
            .chain((idx.levels.iter()).map(|l| format!("{}B/{}g/{}f", l.bytes(), l.guards.len(), l.entries().count())))
            .collect();
        println!(
            "compaction_amplification_bench {label}: {spilled} B spilled, R={r}, L1 target {}, levels {}, \
             {resident} B resident{}",
            idx.l1_target_bytes(),
            levels.join(" "),
            budget.map_or(String::new(), |(cap, _)| format!(" of {cap}")),
        );
        println!(
            "  {} rows resident, {} of them retractions",
            idx.total_rows(),
            idx.all_entries().map(|e| e.shard.retraction_rows()).sum::<usize>()
        );
        println!(
            "  {:.1} instr/row to spill and {:.1} to keep up",
            spill as f64 / spilled_rows as f64,
            upkeep as f64 / spilled_rows as f64
        );
        let (read, wrote) = phases
            .values()
            .fold((0, 0), |(i, o), p| (i + p.in_bytes, o + p.out_bytes));
        println!(
            "  every trigger: read {:6.2} and wrote {:6.2} per spilled byte, {:.1} instr per byte read",
            read as f64 / spilled as f64,
            wrote as f64 / spilled as f64,
            upkeep as f64 / read as f64
        );
        let written = idx.dir.last_seq();
        println!(
            "  {:.2} shards written per spill, {} B each, {} of {written} registered",
            written as f64 / runs as f64,
            (spilled + wrote) / written,
            idx.shard_count()
        );
        for (every, _, covered, shards, bytes) in barriers {
            let passed = runs / every;
            if passed > 0 {
                println!(
                    "  a barrier every {every:>2} spills syncs {:5.1} shards, {:.2} bytes per spilled byte",
                    shards as f64 / passed as f64,
                    bytes as f64 / covered as f64
                );
            }
        }
        for (kind, p) in phases {
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

/// A compressing store against a plain twin, over scattered spills of one long
/// STRING value per row through every fold the upkeep owes: per value shape,
/// the codebook it holds and how well that codes, the shards written coded,
/// the bytes resident, and instructions per spilled row to spill and keep up.
/// The last shape turns from values no table codes to ones a table codes well
/// halfway through.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn compressed_upkeep_bench() {
    use crate::test_support::{BASE64, id_rows, prose, random_chars, u64_pk_schema};
    use gnitz_wire::{TypeCode, VALUE_TEXT_MAX, write_uuid};
    use gnitz_zset::schema::SchemaColumn;
    const RUNS: u64 = 200;
    // Label, and the value of key `k` in run `run`.
    type Shape = (&'static str, fn(u64, u64, &mut Rng) -> String);
    let shapes: [Shape; 6] = [
        ("prose", |_, k, _| prose(k as usize)),
        ("sha256 hex", |_, _, rng| {
            format!("{:032x}{:032x}", rng.gen_u128(), rng.gen_u128())
        }),
        ("uuid text", |_, _, rng| {
            write_uuid(rng.gen_u128(), &mut [0; VALUE_TEXT_MAX]).to_string()
        }),
        ("json with a token", |_, k, rng| {
            format!(
                r#"{{"user_id":{k},"session":"{}","active":true}}"#,
                random_chars(rng, BASE64, 22)
            )
        }),
        ("base64 token", |_, _, rng| random_chars(rng, BASE64, 43)),
        ("base64, then prose", |run, k, rng| match run < RUNS / 2 {
            true => random_chars(rng, BASE64, 43),
            false => prose(k as usize),
        }),
    ];
    let schema = u64_pk_schema(&[
        SchemaColumn::new(TypeCode::I64, false),
        SchemaColumn::new(TypeCode::String, false),
    ]);
    let tmp = tempfile::tempdir().unwrap();
    let counter = Counter::instructions();
    for (label, value) in shapes {
        for compress in [false, true] {
            let dir = tmp.path().join(format!("{label}, {compress}"));
            let recovery = RecoverySource::Rederive { resume_at: None };
            let mut idx = ShardIndex::open(dir.to_str().unwrap(), schema, recovery, None, compress).unwrap();
            let mut rng = Rng::new(SEED);
            let (mut spill, mut upkeep, mut rows) = (0, 0, 0);
            for run in 0..RUNS {
                let mut keys: Vec<u64> = (0..RUN_ROWS).map(|_| rng.gen_range(KEYSPACE)).collect();
                keys.sort_unstable();
                keys.dedup();
                let batch = id_rows(&schema, keys.into_iter().map(|k| (k, 1)), |b, k| {
                    b.put_int(k as u128).put_string(&value(run, k, &mut rng));
                });
                rows += batch.len();
                spill += counter.measure(|| idx.append_l0_run(&batch).unwrap()).1;
                upkeep += counter.measure(|| idx.maintain(u64::MAX).unwrap()).1;
            }
            let coded = idx.all_entries().filter(|e| e.shard.heap().plain().is_none()).count();
            assert!(compress || coded == 0, "{label}: a plain store coded a shard");
            assert!(!compress || label != "prose" || coded > 0, "prose went uncoded");
            let book = (idx.codebook.as_ref()).map_or("none".to_string(), |b| {
                format!("{}/16, pays {}", b.trained_sixteenths(), b.pays())
            });
            println!(
                "compressed_upkeep_bench {label}, compress={compress}: codebook {book}, {coded} of {} shards coded, \
                 {} B resident",
                idx.shard_count(),
                idx.resident_bytes(),
            );
            println!(
                "  {:.1} instr/row to spill and {:.1} to keep up",
                spill as f64 / rows as f64,
                upkeep as f64 / rows as f64,
            );
        }
    }
}
