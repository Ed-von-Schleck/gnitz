use super::{flush_barrier, Cut, RecoverySource, DEFAULT_RAM_TIER_BYTES};
use crate::test_support::{make_batch, make_batch_raw, make_schema_u64_i64, new_table};
use gnitz_foundation::perf::{self, Counter};
use gnitz_wire::PkKeys;
use gnitz_zset::repr::Batch;

/// Instructions per row of `Table::ingest` at the cadence a worker
/// runs it: a tick is one ingest and the memtable drains on its own budget. One
/// case per way the drain ends — the memtable's own fold, a batch past the
/// memtable budget, a RAM-tier fold whose cancellation spares the spill, and the
/// spill.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn table_ingest_bench() {
    const UPDATES: u64 = 1_000_000;
    const HOT: u64 = 8192;
    const SMALL_TIER: usize = 1 << 20;
    type Rows = [Option<(u64, i64, i64)>; 2];
    // Update `u` inserts key `u`.
    let fresh = |u: u64| -> Rows { [None, Some((u, 1, u as i64))] };
    // Update `u` rewrites key `u % HOT`, retracting the row update `u - HOT` left there.
    let churn = |u: u64| -> Rows {
        let retraction = u.checked_sub(HOT).map(|old| (u % HOT, -1, old as i64));
        [retraction, Some((u % HOT, 1, u as i64))]
    };
    // Label, updates per tick, the rows of an update, the RAM-tier ceiling, whether it spills.
    type Case = (&'static str, u64, fn(u64) -> Rows, usize, bool);
    let cases: [Case; 5] = [
        ("fresh keys, 1-row ticks", 1, fresh, DEFAULT_RAM_TIER_BYTES, false),
        ("fresh keys, 100-row ticks", 100, fresh, DEFAULT_RAM_TIER_BYTES, false),
        ("fresh keys, 8192-row ticks", 8192, fresh, DEFAULT_RAM_TIER_BYTES, false),
        (
            "8192 hot keys, 16-update ticks, 1 MiB tier",
            16,
            churn,
            SMALL_TIER,
            false,
        ),
        ("fresh keys, 100-row ticks, 1 MiB tier", 100, fresh, SMALL_TIER, true),
    ];

    let schema = make_schema_u64_i64();
    let counter = Counter::instructions();
    let dir = tempfile::tempdir().unwrap();
    for (case, (label, per_tick, update, tier, spills)) in cases.into_iter().enumerate() {
        let ticks: Vec<Batch> = (0..UPDATES / per_tick)
            .map(|t| {
                let rows: Vec<_> = (t * per_tick..(t + 1) * per_tick).flat_map(update).flatten().collect();
                // As a pushed batch arrives: decoded at its row count.
                make_batch_raw(&schema, &rows).trimmed()
            })
            .collect();
        let rows: usize = ticks.iter().map(Batch::len).sum();
        let rederive = RecoverySource::Rederive { resume_at: None };
        let mut table = new_table(dir.path().join(case.to_string()), schema, rederive, tier);
        let ((), instructions) = counter.measure(|| {
            for tick in ticks {
                table.ingest(tick).unwrap();
            }
        });
        assert!(table.ram_tier.row_count() > 0, "{label}: the memtable never drained");
        assert!(
            rows * 32 > tier || tier == DEFAULT_RAM_TIER_BYTES,
            "{label}: fits the tier"
        );
        assert_eq!(!table.all_shard_arcs().is_empty(), spills, "{label}: spilled");
        println!(
            "table_ingest_bench {label:<44} {:8.1} instr/row",
            instructions as f64 / rows as f64
        );
    }
}

/// Updates of a store whose net rows fill `fill` of its RAM tier, in cycles and
/// instructions per update, and the shards it ends with. A tier folded to under
/// its ceiling stays in RAM, and the fuller it is the sooner it is over the
/// ceiling again; each crossing folds the whole tier, in bulk copies that retire
/// next to no instructions.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn ram_tier_ceiling_bench() {
    const TIER: usize = 16 << 20;
    const UPDATES: u64 = 1_500_000;
    const PER_TICK: u64 = 16;
    let schema = make_schema_u64_i64();
    let (instructions, cycles) = (Counter::instructions(), Counter::cycles());
    let dir = tempfile::tempdir().unwrap();
    for fill in [50u64, 85, 92, 96, 99] {
        let hot = (TIER as u64 / 32) * fill / 100;
        let rederive = RecoverySource::Rederive { resume_at: None };
        let mut table = new_table(dir.path().join(fill.to_string()), schema, rederive, TIER);
        let load: Vec<_> = (0..hot).map(|k| (k, 1, 0)).collect();
        for chunk in load.chunks(1000) {
            table.ingest(make_batch_raw(&schema, chunk)).unwrap();
        }
        // Each update retracts the row its key holds and inserts the next.
        let mut version = vec![0i64; hot as usize];
        let ticks: Vec<Batch> = (0..UPDATES / PER_TICK)
            .map(|t| {
                let mut rows = Vec::new();
                for u in t * PER_TICK..(t + 1) * PER_TICK {
                    let key = (u.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 20) % hot;
                    let v = &mut version[key as usize];
                    rows.extend([(key, -1, *v), (key, 1, *v + 1)]);
                    *v += 1;
                }
                make_batch_raw(&schema, &rows)
            })
            .collect();
        let (((), instr), cyc) = cycles.measure(|| {
            instructions.measure(|| {
                for tick in ticks {
                    table.ingest(tick).unwrap();
                }
            })
        });
        assert_eq!(table.full_scan().len() as u64, hot, "{fill}%: the held rows");
        println!(
            "ram_tier_ceiling_bench {fill:>2}% full {:>7.0} cycles/update {:>7.0} instr/update, {} shards",
            cyc as f64 / UPDATES as f64,
            instr as f64 / UPDATES as f64,
            table.all_shard_arcs().len()
        );
    }
}

/// What one `Table::ingest` costs its caller once the store has a
/// disk tier to keep up: the mean call, the costliest one, and how many calls
/// found upkeep owed, over fresh ascending keys and over keys scattered
/// across the key space. A call pays for the upkeep its own bytes owe and
/// stops at a fold's destination, so the costliest is one destination, not the
/// folds a spill sets off.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn ingest_call_bench() {
    const UPDATES: u64 = 8_000_000;
    const PER_TICK: u64 = 100;
    const TIER: usize = 1 << 20;
    let ascending = |u: u64| u;
    let scattered = |u: u64| u.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 1;
    type Case = (&'static str, fn(u64) -> u64);
    let cases: [Case; 2] = [("ascending", ascending), ("scattered", scattered)];

    let schema = make_schema_u64_i64();
    let counter = Counter::instructions();
    let dir = tempfile::tempdir().unwrap();
    for (label, key) in cases {
        let rederive = RecoverySource::Rederive { resume_at: None };
        let mut table = new_table(dir.path().join(label), schema, rederive, TIER);
        let (mut total, mut costliest, mut kept_up) = (0, 0, 0u64);
        for t in 0..UPDATES / PER_TICK {
            let rows: Vec<_> = (t * PER_TICK..(t + 1) * PER_TICK)
                .map(|u| (key(u), 1, u as i64))
                .collect();
            let tick = make_batch_raw(&schema, &rows);
            kept_up += u64::from(table.shard_index.owed());
            let ((), instructions) = counter.measure(|| table.ingest(tick).unwrap());
            total += instructions;
            costliest = costliest.max(instructions);
        }
        let ticks = UPDATES / PER_TICK;
        let (l0, deeper) = table.level_shape();
        assert!(deeper.iter().all(|&guards| guards > 1), "{label}: a level never filled");
        println!(
            "ingest_call_bench {label:<10} {:7.1} K instr/call, the costliest {:6.1} M, {:.2}% of calls found upkeep owed; \
             {l0} L0 shards, {deeper:?} guards",
            total as f64 / ticks as f64 / 1e3,
            costliest as f64 / 1e6,
            kept_up as f64 * 100.0 / ticks as f64,
        );
    }
}

/// Instructions per `Table::gather` of 1, 64 and 4096 keys spread over the key
/// span of a table whose rows sit in shards and in the memtable, the open
/// counted.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn pk_set_gather_bench() {
    const ROWS: u64 = 1 << 18;
    const ITERS: usize = 200;
    let schema = make_schema_u64_i64();
    let dir = tempfile::tempdir().unwrap();
    // A tier of half a round, so every round spills.
    let mut t = new_table(
        dir.path(),
        schema,
        RecoverySource::Rederive { resume_at: None },
        1 << 19,
    );
    for r in 0..8u64 {
        let rows: Vec<(u64, i64, i64)> = (0..ROWS / 8).map(|k| (k * 8 + r, 1, (k * 8 + r) as i64)).collect();
        t.ingest(make_batch(&schema, &rows)).unwrap();
    }
    let late: Vec<(u64, i64, i64)> = (0..64u64).map(|k| (k * (ROWS / 64) + 1, 1, 7)).collect();
    t.ingest(make_batch(&schema, &late)).unwrap();
    assert_eq!(
        (
            t.shard_index.total_rows(),
            t.ram_tier.row_count(),
            t.memtable.row_count()
        ),
        (ROWS as usize, 0, late.len()),
        "every round in a shard, the late rows in the memtable"
    );

    let counter = Counter::instructions();
    for n in [1u64, 64, 4096] {
        let step = ROWS / (n + 1);
        let key_bytes: Vec<[u8; 8]> = (1..=n).map(|i| (i * step).to_be_bytes()).collect();
        let passes = vec![PkKeys::from_keys(8, key_bytes.iter().map(|k| &k[..])); ITERS];
        let (rows, instructions) = counter.measure(|| {
            let gather = |keys| t.gather(keys, Cut::Now).drain_chunk(usize::MAX).map_or(0, |b| b.len());
            passes.into_iter().map(gather).sum::<usize>()
        });
        assert!(rows >= ITERS * n as usize, "every probed key holds a row");
        println!(
            "pk_set_gather_bench {n} keys: {} instr/gather",
            instructions / ITERS as u64
        );
    }
}

/// Instructions per read at `Cut::Sealed` — a `Table::gather` of 1, 64 and 4096
/// keys, and per row of a `cursor_between` over the whole span walked to its
/// end — of a table whose rows sit in shards, by what stands above the cut:
/// nothing; rows pending in RAM, pushed onto held keys one or a thousand at a
/// time; and pending rows a barrier wrote down, a tick's worth and as many as
/// the table holds.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn sealed_read_bench() {
    let schema = make_schema_u64_i64();
    let dir = tempfile::tempdir().unwrap();
    let counter = Counter::instructions();
    // Rows held, rows pending, rows per pending push, whether a barrier wrote them down.
    let cases: [(u64, u64, u64, bool); 5] = [
        (1 << 18, 0, 1, false),
        (1 << 18, 10_000, 1, false),
        (1 << 18, 10_000, 1000, false),
        (1 << 18, 10_000, 1000, true),
        (1 << 20, 1_000_000, 1000, true),
    ];
    for (case, (held, pending, per, written)) in cases.into_iter().enumerate() {
        // A tier of less than a round, so every round spills.
        let mut t = new_table(
            dir.path().join(case.to_string()),
            schema,
            RecoverySource::SalReplay,
            1 << 19,
        );
        for r in 0..8u64 {
            let rows: Vec<_> = (0..held / 8).map(|k| (k * 8 + r, 1, (k * 8 + r) as i64)).collect();
            t.ingest(make_batch(&schema, &rows)).unwrap();
        }
        for from in (0..pending).step_by(per as usize) {
            let row = |s: u64| ((s.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 24) % held, 1, -1 - s as i64);
            let rows: Vec<_> = (from..from + per).map(row).collect();
            t.ingest_pending(make_batch_raw(&schema, &rows));
        }
        if written {
            flush_barrier([&mut t], 0).unwrap();
        }
        assert_eq!(
            t.pending.len() == 0,
            pending == 0 || written,
            "where the pending rows sit"
        );
        let above = match (pending, written) {
            (0, _) => "nothing pending".to_string(),
            (_, false) => format!("{pending} pending in {per}-row pushes"),
            (_, true) => format!("{pending} pending written by a barrier"),
        };
        let label = format!("{held} rows, {above}");
        // The larger store is read fewer times.
        let (gathers, walks) = if held > 1 << 18 { (20, 2) } else { (200, 8) };
        for n in [1u64, 64, 4096] {
            let step = held / (n + 1);
            let key_bytes: Vec<[u8; 8]> = (1..=n).map(|i| (i * step).to_be_bytes()).collect();
            let passes = vec![PkKeys::from_keys(8, key_bytes.iter().map(|k| &k[..])); gathers];
            let (rows, instructions) = counter.measure(|| {
                let gather = |keys| {
                    t.gather(keys, Cut::Sealed)
                        .drain_chunk(usize::MAX)
                        .map_or(0, |b| b.len())
                };
                passes.into_iter().map(gather).sum::<usize>()
            });
            assert_eq!(
                rows,
                gathers * n as usize,
                "{label}: a sealed gather leaves the pending rows out"
            );
            println!(
                "sealed_read_bench {label}, {n} keys: {} instr/gather",
                instructions / gathers as u64
            );
        }
        let (first, last) = (0u64.to_be_bytes(), (held - 1).to_be_bytes());
        let (rows, instructions) = counter.measure(|| {
            (0..walks)
                .map(|_| t.cursor_between(&first, &last, Cut::Sealed).materialize().len())
                .sum::<usize>()
        });
        assert_eq!(
            rows,
            walks * held as usize,
            "{label}: a sealed walk leaves the pending rows out"
        );
        println!(
            "sealed_read_bench {label}, whole span: {:.1} instr/row",
            instructions as f64 / rows as f64
        );
    }
}

/// What one barrier costs over a RAM tier with rows above the cut, and the seal
/// after it: instructions, and the most each added to the resident set. By how
/// the pending rows lie against an ascending tier — past its last key, between
/// its keys, as updates of the rows it holds — and for as many pending rows as
/// a crash leaves, over an empty tier.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn barrier_seal_bench() {
    /// Rows per pending push.
    const PER: u64 = 1000;
    /// Coprime to every span below, so `s * STRIDE % span` visits each key once.
    const STRIDE: u64 = 7919;
    #[derive(Clone, Copy, Debug)]
    enum Lie {
        Past,
        Between,
        Updates,
    }
    let schema = make_schema_u64_i64();
    let dir = tempfile::tempdir().unwrap();
    let counter = Counter::instructions();
    // Rows in the RAM tier, at the even keys; rows pending; how they lie.
    let cases: [(u64, u64, Lie); 4] = [
        (500_000, 10_000, Lie::Past),
        (500_000, 10_000, Lie::Between),
        (500_000, 10_000, Lie::Updates),
        (0, 1_000_000, Lie::Between),
    ];
    for (case, (tier, pending, lie)) in cases.into_iter().enumerate() {
        let mut t = new_table(
            dir.path().join(case.to_string()),
            schema,
            RecoverySource::SalReplay,
            DEFAULT_RAM_TIER_BYTES,
        );
        for from in (0..tier).step_by(PER as usize) {
            let rows: Vec<_> = (from..from + PER).map(|k| (2 * k, 1, k as i64)).collect();
            t.ingest(make_batch(&schema, &rows)).unwrap();
        }
        assert!(t.all_shard_arcs().is_empty(), "the tier's rows are held in RAM");
        let span = tier.max(pending);
        for from in (0..pending).step_by(PER as usize) {
            let rows: Vec<(u64, i64, i64)> = match lie {
                Lie::Past => (from..from + PER).map(|s| (2 * span + s, 1, -1 - s as i64)).collect(),
                Lie::Between => (from..from + PER)
                    .map(|s| (2 * (s * STRIDE % span) + 1, 1, -1 - s as i64))
                    .collect(),
                // An update is two rows: the held row retracted, and its successor.
                Lie::Updates => (from / 2..(from + PER) / 2)
                    .map(|s| (s * STRIDE % span, s))
                    .flat_map(|(k, s)| [(2 * k, -1, k as i64), (2 * k, 1, -1 - s as i64)])
                    .collect(),
            };
            t.ingest_pending(make_batch_raw(&schema, &rows));
        }

        let mib = |resident: Option<perf::Resident>| match resident {
            Some(r) => format!("+{:.1} MiB", r.peak_added() as f64 / (1 << 20) as f64),
            None => format!("n/a without {}", perf::PIN_MMAP_THRESHOLD),
        };
        let resident = perf::Resident::baseline();
        let ((), barrier) = counter.measure(|| flush_barrier([&mut t], 1).unwrap());
        let barrier_peak = mib(resident);
        assert_eq!(
            t.all_shard_arcs().len(),
            1,
            "one shard holds the tier and the pending rows"
        );
        let resident = perf::Resident::baseline();
        let (delta, seal) = counter.measure(|| t.seal().unwrap());
        let seal_peak = mib(resident);
        assert_eq!(delta.map_or(0, |d| d.len()) as u64, pending, "the seal's delta");
        println!(
            "barrier_seal_bench {tier} rows in the tier, {pending} pending {lie:?}: \
             barrier {:.1} M instr, peak {barrier_peak}; seal {:.1} instr/pending row, peak {seal_peak}",
            barrier as f64 / 1e6,
            seal as f64 / pending as f64,
        );
    }
}
