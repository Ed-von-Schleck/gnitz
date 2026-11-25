use super::{Cut, DEFAULT_RAM_TIER_BYTES, RecoverySource, flush_barrier};
use crate::test_support::{certified, make_batch, make_schema_u64_i64, new_table, scratch_table, table_shards};
use gnitz_foundation::perf::{self, Counter, Counters, Event};
use gnitz_foundation::rng::mix;
use gnitz_zset::repr::Batch;
use gnitz_zset::schema::pk_keys_of_images;

/// Instructions per row of `Table::ingest` of fresh keys at the cadence a worker
/// runs it: a tick is one ingest and the memtable drains on its own budget. The
/// small ticks end at the memtable's own fold, the large at a batch past the
/// memtable budget; `ram_tier_ceiling_bench` prices a RAM-tier fold whose
/// cancellation spares the spill, and `ingest_call_bench` the ingests that spill.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn table_ingest_bench() {
    const UPDATES: u64 = 1_000_000;
    let schema = make_schema_u64_i64();
    let counter = Counter::instructions();
    let dir = tempfile::tempdir().unwrap();
    for per_tick in [1u64, 100, 8192] {
        let ticks: Vec<Batch> = (0..UPDATES / per_tick)
            .map(|t| {
                let rows: Vec<_> = (t * per_tick..(t + 1) * per_tick).map(|u| (u, 1, u as i64)).collect();
                // As a pushed batch arrives: decoded at its row count.
                make_batch(&schema, &rows).trimmed()
            })
            .collect();
        let mut table = scratch_table(dir.path().join(per_tick.to_string()), schema);
        let ((), instructions) = counter.measure(|| {
            for tick in ticks {
                table.ingest(tick).unwrap();
            }
        });
        assert!(
            table.ram_tier.row_count() > 0,
            "{per_tick}-row ticks: the memtable never drained"
        );
        assert!(table_shards(&table).is_empty(), "{per_tick}-row ticks: spilled");
        println!(
            "table_ingest_bench {per_tick:>4}-row ticks {:8.1} instr/row",
            instructions as f64 / UPDATES as f64
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
    let counters = Counters::open([Event::INSTRUCTIONS, Event::CYCLES]);
    let dir = tempfile::tempdir().unwrap();
    for fill in [25u64, 50, 85, 92, 96, 99] {
        let hot = (TIER / schema.row_width()) as u64 * fill / 100;
        let rederive = RecoverySource::Rederive { resume_at: None };
        let mut table = new_table(dir.path().join(fill.to_string()), schema, rederive, TIER);
        let load: Vec<_> = (0..hot).map(|k| (k, 1, 0)).collect();
        for chunk in load.chunks(1000) {
            table.ingest(make_batch(&schema, chunk)).unwrap();
        }
        // Each update retracts the row its key holds and inserts the next.
        let mut version = vec![0i64; hot as usize];
        let ticks: Vec<Batch> = (0..UPDATES / PER_TICK)
            .map(|t| {
                let mut rows = Vec::new();
                for u in t * PER_TICK..(t + 1) * PER_TICK {
                    let key = (mix(u) >> 20) % hot;
                    let v = &mut version[key as usize];
                    rows.extend([(key, -1, *v), (key, 1, *v + 1)]);
                    *v += 1;
                }
                make_batch(&schema, &rows)
            })
            .collect();
        let ((), [instr, cyc]) = counters.measure(|| {
            for tick in ticks {
                table.ingest(tick).unwrap();
            }
        });
        assert_eq!(table.full_scan().len() as u64, hot, "{fill}%: the held rows");
        println!(
            "ram_tier_ceiling_bench {fill:>2}% full {:>7.0} cycles/update {:>7.0} instr/update, {} shards",
            cyc as f64 / UPDATES as f64,
            instr as f64 / UPDATES as f64,
            table_shards(&table).len()
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
    let scattered = |u: u64| mix(u) >> 1;
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
            let tick = make_batch(&schema, &rows);
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

/// Instructions per read at `Cut::Sealed`, as a circuit reads a store's trace —
/// a `Table::gather` of 1, 64 and 4096 keys, and per row of a `cursor_between`
/// over the whole span walked to its end — of a table whose rows sit in shards,
/// alone and under heap runs that update some of them.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn sealed_read_bench() {
    /// Heap runs over the shards, and the held rows each updates.
    const RUNS: u64 = 13;
    const PER_RUN: u64 = 64;
    let schema = make_schema_u64_i64();
    let dir = tempfile::tempdir().unwrap();
    let counter = Counter::instructions();
    let cases = [(1u64 << 18, 0), (1 << 18, RUNS), (1 << 20, 0), (1 << 20, RUNS)];
    for (case, (held, runs)) in cases.into_iter().enumerate() {
        // A tier of less than a round, so every round spills.
        let mut t = new_table(
            dir.path().join(case.to_string()),
            schema,
            RecoverySource::SalReplay,
            1 << 19,
        );
        for r in 0..8u64 {
            let rows: Vec<_> = (0..held / 8).map(|k| (k * 8 + r, 1, (k * 8 + r) as i64)).collect();
            t.ingest(certified(make_batch(&schema, &rows))).unwrap();
        }
        assert!(
            t.ram_tiers(Cut::Now).all(|set| set.len() == 0),
            "premise: every row is on disk"
        );
        // `mix(s) & (held - 1)` visits each held key once.
        for r in 0..runs {
            let keys = (r * PER_RUN..(r + 1) * PER_RUN).map(|s| mix(s) & (held - 1));
            let rows: Vec<_> = keys.flat_map(|k| [(k, -1, k as i64), (k, 1, -1 - k as i64)]).collect();
            t.ingest(make_batch(&schema, &rows)).unwrap();
        }
        assert_eq!(
            t.ram_tiers(Cut::Sealed).any(|set| set.len() > 0),
            runs > 0,
            "premise: the updates stay in RAM"
        );
        let label = format!("{held} rows, {runs} heap runs");
        // Each read once unmeasured first, to warm the thread's pools and the heap.
        for n in [1u64, 64, 4096] {
            let step = held / (n + 1);
            let keys = || pk_keys_of_images(8, (1..=n).map(|i| (i * step) as u128));
            let gather = |keys| {
                t.gather(keys, Cut::Sealed)
                    .drain_chunk(usize::MAX)
                    .map_or(0, |b| b.len())
            };
            let (warm, measured) = (keys(), keys());
            gather(warm);
            let (rows, instructions) = counter.measure(|| gather(measured));
            assert_eq!(rows, n as usize, "{label}: a gather's rows");
            println!("sealed_read_bench {label}, {n} keys: {instructions} instr/gather");
        }
        let (first, last) = (0u64.to_be_bytes(), (held - 1).to_be_bytes());
        let walk = || t.cursor_between(&first, &last, Cut::Sealed).materialize().len();
        walk();
        let (rows, instructions) = counter.measure(walk);
        assert_eq!(rows, held as usize, "{label}: a walk's rows");
        println!(
            "sealed_read_bench {label}, whole span: {:.1} instr/row",
            instructions as f64 / rows as f64
        );
    }
}

/// What one barrier costs over rows pending between the keys of a RAM tier,
/// and the seal after it: instructions, and the most each added to the resident
/// set; and for as many pending rows as a crash leaves, over an empty tier.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn barrier_seal_bench() {
    /// Rows per pending push.
    const PER: u64 = 1000;
    /// Coprime to every span below, so `s * STRIDE % span` visits each key once.
    const STRIDE: u64 = 7919;
    let schema = make_schema_u64_i64();
    let dir = tempfile::tempdir().unwrap();
    let counter = Counter::instructions();
    // Rows in the RAM tier, at the even keys; rows pending, at odd keys.
    for (case, (tier, pending)) in [(500_000u64, 10_000u64), (0, 1_000_000)].into_iter().enumerate() {
        let mut t = new_table(
            dir.path().join(case.to_string()),
            schema,
            RecoverySource::SalReplay,
            DEFAULT_RAM_TIER_BYTES,
        );
        for from in (0..tier).step_by(PER as usize) {
            let rows: Vec<_> = (from..from + PER).map(|k| (2 * k, 1, k as i64)).collect();
            t.ingest(certified(make_batch(&schema, &rows))).unwrap();
        }
        assert!(table_shards(&t).is_empty(), "the tier's rows are held in RAM");
        let span = tier.max(pending);
        for from in (0..pending).step_by(PER as usize) {
            let rows: Vec<_> = (from..from + PER)
                .map(|s| (2 * (s * STRIDE % span) + 1, 1, -1 - s as i64))
                .collect();
            t.ingest_pending(make_batch(&schema, &rows));
        }

        let mib = |resident: Option<perf::Resident>| match resident {
            Some(r) => format!("+{:.1} MiB", r.peak_added() as f64 / (1 << 20) as f64),
            None => format!("n/a without {}", perf::PIN_MMAP_THRESHOLD),
        };
        let resident = perf::Resident::baseline();
        let ((), barrier) = counter.measure(|| flush_barrier([&mut t], 1).unwrap());
        let barrier_peak = mib(resident);
        assert_eq!(
            table_shards(&t).len(),
            1 + usize::from(tier > 0),
            "the tier's shard, and the pending rows' own"
        );
        let resident = perf::Resident::baseline();
        let (delta, seal) = counter.measure(|| t.seal().unwrap());
        let seal_peak = mib(resident);
        assert_eq!(delta.map_or(0, |d| d.len()) as u64, pending, "the seal's delta");
        println!(
            "barrier_seal_bench {tier} rows in the tier, {pending} pending: \
             barrier {:.1} M instr, peak {barrier_peak}; seal {:.1} instr/pending row, peak {seal_peak}",
            barrier as f64 / 1e6,
            seal as f64 / pending as f64,
        );
    }
}

/// Instructions per `Table::open_cursor` of a store of 15 runs whose lowest
/// keys are inserted and retracted in different runs, by how many: an open that
/// passes more rows than the store has runs leaves a floor the next opens seek
/// to, so 7 cancelled keys are walked by every open and 8 are not. Per open with
/// the floor judged, and judged anew, as the first open after an ingest does.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn open_cursor_bench() {
    const HELD: u64 = 8192;
    const RUNS: u64 = 13;
    const OPENS: u64 = 10_000;
    let schema = make_schema_u64_i64();
    let dir = tempfile::tempdir().unwrap();
    let counter = Counter::instructions();
    for cancelled in [0u64, 7, 8, 4096] {
        let mut t = scratch_table(dir.path().join(cancelled.to_string()), schema);
        let held: Vec<_> = (0..HELD).map(|k| (k, 1, k as i64)).collect();
        t.ingest(certified(make_batch(&schema, &held))).unwrap();
        t.fold_to_ram().unwrap();
        if cancelled > 0 {
            let retracted: Vec<_> = (0..cancelled).map(|k| (k, -1, k as i64)).collect();
            t.ingest(certified(make_batch(&schema, &retracted))).unwrap();
        }
        for r in 0..RUNS {
            let rows: Vec<_> = (0..64).map(|i| (HELD + r * 64 + i, 1, 0)).collect();
            t.ingest(certified(make_batch(&schema, &rows))).unwrap();
        }
        let first = t.open_cursor(Cut::Now);
        assert_eq!(first.current_pk_bytes(), cancelled.to_be_bytes(), "the first live row");
        drop(first);
        let ((), judged) = counter.measure(|| {
            for _ in 0..OPENS {
                std::hint::black_box(t.open_cursor(Cut::Now));
            }
        });
        let ((), judging) = counter.measure(|| {
            for _ in 0..OPENS {
                t.live_floor.get_mut().judged = false;
                std::hint::black_box(t.open_cursor(Cut::Now));
            }
        });
        println!(
            "open_cursor_bench {cancelled:>5} cancelled keys {:9.1} instr/open, {:9.1} judging",
            judged as f64 / OPENS as f64,
            judging as f64 / OPENS as f64
        );
    }
}

/// Instructions per `Table::unique_live_row_at` over a unique-PK store whose
/// heap tiers are empty, by where the key's newest version sits: rewritten
/// after the bulk load, so a newer shard above the one holding its insert; only
/// inserted; or absent.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn unique_live_row_bench() {
    const KEYS: u64 = 200_000;
    const SPILL: u64 = 8192;
    const UPDATED: u64 = 4096;
    let schema = make_schema_u64_i64();
    let dir = tempfile::tempdir().unwrap();
    let mut t = new_table(dir.path(), schema, RecoverySource::SalReplay, 1 << 18);
    let scattered = |k: u64| k * 0x9E37_79B1 % KEYS;
    for base in (0..KEYS).step_by(SPILL as usize) {
        let rows: Vec<_> = (base..(base + SPILL).min(KEYS)).map(|k| (scattered(k), 1, 0)).collect();
        t.ingest(make_batch(&schema, &rows)).unwrap();
    }
    let updated: Vec<u64> = (0..UPDATED).map(|i| scattered(i * (KEYS / UPDATED))).collect();
    let rows: Vec<_> = updated.iter().flat_map(|&k| [(k, -1, 0), (k, 1, 1)]).collect();
    t.ingest(make_batch(&schema, &rows)).unwrap();
    flush_barrier([&mut t], 0).unwrap();
    assert!(
        t.ram_tiers(Cut::Now).all(|set| set.len() == 0),
        "premise: every row is on disk"
    );
    println!("unique_live_row_bench: {} shards", table_shards(&t).len());

    let counter = Counter::instructions();
    let inserted: Vec<u64> = (0..UPDATED).map(|i| scattered(i * (KEYS / UPDATED) + 1)).collect();
    let absent: Vec<u64> = (0..UPDATED).map(|i| KEYS + i).collect();
    for (label, keys, live) in [
        ("rewritten", &updated, true),
        ("inserted", &inserted, true),
        ("absent", &absent, false),
    ] {
        let keys: Vec<[u8; 8]> = keys.iter().map(|k| k.to_be_bytes()).collect();
        let (found, instructions) = counter.measure(|| {
            keys.iter()
                .filter(|k| std::hint::black_box(t.unique_live_row_at(&k[..])).is_some())
                .count()
        });
        assert_eq!(found, if live { keys.len() } else { 0 }, "{label}");
        println!("  {label}: {:.1} instr/probe", instructions as f64 / keys.len() as f64);
    }
}

/// Rows landed above the cut as a recovery replays them: `Table::ingest_pending`
/// of many pushes of keys scattered over the store's span, then the seal that
/// moves them below it. Cycles and instructions per row of each.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn pending_replay_bench() {
    const ROWS: u64 = 1_000_000;
    /// Coprime to `ROWS`, so `s * STRIDE % ROWS` visits each key once.
    const STRIDE: u64 = 7919;
    let schema = make_schema_u64_i64();
    let counters = Counters::open([Event::INSTRUCTIONS, Event::CYCLES]);
    let dir = tempfile::tempdir().unwrap();
    for per_push in [100u64, 1000] {
        let mut t = scratch_table(dir.path().join(per_push.to_string()), schema);
        let pushes: Vec<Batch> = (0..ROWS / per_push)
            .map(|p| {
                let rows: Vec<_> = (p * per_push..(p + 1) * per_push)
                    .map(|s| (s * STRIDE % ROWS, 1, s as i64))
                    .collect();
                make_batch(&schema, &rows)
            })
            .collect();
        let ((), [li, lc]) = counters.measure(|| pushes.into_iter().for_each(|b| t.ingest_pending(b)));
        let (delta, [si, sc]) = counters.measure(|| t.seal().unwrap());
        assert_eq!(
            delta.map_or(0, |d| d.len()) as u64,
            ROWS,
            "{per_push}-row pushes: the seal's delta"
        );
        println!(
            "pending_replay_bench {per_push:>4}-row pushes: land {:7.1} cycles/row {:7.1} instr/row; seal {:6.1} cycles/row {:6.1} instr/row",
            lc as f64 / ROWS as f64,
            li as f64 / ROWS as f64,
            sc as f64 / ROWS as f64,
            si as f64 / ROWS as f64,
        );
    }
}
