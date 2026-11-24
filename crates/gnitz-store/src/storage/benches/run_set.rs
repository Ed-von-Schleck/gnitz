use super::super::table::{DEFAULT_RAM_TIER_BYTES, MEMTABLE_BYTES};
use super::*;
use crate::test_support::{certified, id_rows, make_schema_pk_u64_payload_string, make_schema_u64_i64};
use gnitz_foundation::perf::{self, Counter, Counters, Event};
use gnitz_foundation::rng::scatter;

/// A consolidated run of `rows`, `(pk, weight)` each and ascending by PK; a
/// string payload is spelled out long enough to live on the heap.
fn run(schema: &SchemaDescriptor, rows: impl IntoIterator<Item = (u64, i64)>) -> Batch {
    let strings = schema.string_payload_slots() > 0;
    certified(id_rows(schema, rows, |b, pk| {
        if strings {
            b.put_string(&format!("{pk:040}"));
        } else {
            b.put_int(pk as u128);
        }
    }))
}

/// `RunSet::fold` over string payloads: instructions per input row, and the
/// resident memory the fold adds at its peak as a multiple of the folded run's
/// bytes — 1.0 is the output alone, on top of the inputs it still holds.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn run_set_fold_bench() {
    const DOMINANT: u64 = 200_000;
    const PER_RUN: u64 = 4_000;
    const REST: u64 = FOLD_THRESHOLD as u64 - 2;
    const SPREAD: u64 = DOMINANT / PER_RUN;
    type Rows = fn(run: u64, row: u64) -> (u64, i64);
    let ascending: Rows = |run, row| (2 * DOMINANT + run * PER_RUN + row, 1);
    let scattered: Rows = |run, row| (2 * (row * SPREAD + run) + 1, 1);
    let updates: Rows = |run, row| {
        let retracted = 2 * (row / 2 * 2 * SPREAD + run);
        [(retracted, -1), (retracted + 1, 1)][(row % 2) as usize]
    };
    let arms: [(&str, bool, u64, Rows); 5] = [
        ("dominant run and one scattered run", true, 1, scattered),
        ("dominant run and ascending runs", true, REST, ascending),
        ("dominant run and scattered runs", true, REST, scattered),
        ("dominant run and scattered updates", true, REST, updates),
        ("scattered runs", false, REST + 1, scattered),
    ];
    let schema = make_schema_pk_u64_payload_string();
    let counter = Counter::instructions();
    for (label, has_dominant, small_runs, rows) in arms {
        let mut set = RunSet::new(usize::MAX);
        if has_dominant {
            set.push(run(&schema, (0..DOMINANT).map(|i| (2 * i, 1))), &schema);
        }
        for k in 0..small_runs {
            set.push(run(&schema, (0..PER_RUN).map(|i| rows(k, i))), &schema);
        }
        assert_eq!(
            set.len(),
            has_dominant as usize + small_runs as usize,
            "{label}: a push folded the set"
        );
        let rows_in = set.row_count();
        let retracted = set.batches().map(|r| r.retracted_rows().count());
        let rows_out = rows_in - 2 * retracted.sum::<usize>();
        let resident = perf::Resident::baseline();
        let ((), instructions) = counter.measure(|| set.fold(&schema));
        let peak = match resident {
            Some(r) => format!("{:.2}x the output", r.peak_added() as f64 / set.bytes as f64),
            None => format!("n/a without {}", perf::PIN_MMAP_THRESHOLD),
        };
        assert_eq!(set.row_count(), rows_out, "{label}");
        println!(
            "run_set_fold_bench {label:<34} {rows_in} rows in, {rows_out} out, {:5.1} instr/row, peak {peak}",
            instructions as f64 / rows_in as f64
        );
    }
}

/// `RunSet::push_run` at a memtable's cadence: a set is pushed `tick`-row runs of
/// fresh keys until it is full, then cleared, as a drain leaves it. Instructions
/// and cycles per pushed row, with the set never probed and with it probed after
/// every push. The cycles price the bulk copies of a fold, which retire next to
/// no instructions.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn run_set_push_bench() {
    const FILLS: usize = 16;
    type Key = fn(u64) -> u64;
    let orders: [(&str, Key); 2] = [("ascending", |seq| seq), ("scattered", scatter)];
    let schemas = [
        ("ints", make_schema_u64_i64()),
        ("strings", make_schema_pk_u64_payload_string()),
    ];
    let counters = Counters::open([Event::INSTRUCTIONS, Event::CYCLES]);
    println!(
        "run_set_push_bench {:<28} {:>9} {:>10} {:>10} {:>13} {:>13}",
        "", "rows/fill", "instr/row", "cycles/row", "probed instr", "probed cycles"
    );
    for (schema_label, schema) in schemas {
        for (order_label, key) in orders {
            for tick in [1u64, 16, 128] {
                let label = format!("{schema_label}, {order_label}, {tick}-row");
                // Every fill's ticks, enough to fill the set, built ahead of the
                // measurement and trimmed, as a pushed batch is decoded.
                let mut rows = 0u64;
                let mut fills = Vec::new();
                for _ in 0..FILLS {
                    let ticks: Vec<Batch> = (0..)
                        .map(|t| {
                            let mut keys: Vec<u64> = (0..tick).map(|i| key(rows + t * tick + i)).collect();
                            keys.sort_unstable();
                            run(&schema, keys.into_iter().map(|pk| (pk, 1))).trimmed()
                        })
                        .scan(0, |bytes, b| {
                            let full = *bytes > MEMTABLE_BYTES;
                            *bytes += b.total_bytes();
                            (!full).then_some(b)
                        })
                        .collect();
                    rows += ticks.iter().map(|b| b.len() as u64).sum::<u64>();
                    fills.push(ticks);
                }
                let [unprobed, probed] = [false, true].map(|probed| {
                    let mut set = RunSet::cancelling(MEMTABLE_BYTES);
                    let mut counts = [0u64; 2];
                    for ticks in &fills {
                        let ticks = ticks.clone();
                        let ((), [i, c]) = counters.measure(|| {
                            for b in ticks {
                                set.push(b, &schema);
                                if probed {
                                    std::hint::black_box(set.may_contain(0));
                                }
                            }
                        });
                        counts = [counts[0] + i, counts[1] + c];
                        assert!(set.is_full(), "{label}: not filled");
                        assert_eq!(set.bloom.get().is_some(), probed, "{label}");
                        set.clear();
                    }
                    counts.map(|n| n as f64 / rows as f64)
                });
                println!(
                    "run_set_push_bench {label:<28} {:>9} {:>10.1} {:>10.1} {:>13.1} {:>13.1}",
                    rows / FILLS as u64,
                    unprobed[0],
                    unprobed[1],
                    probed[0],
                    probed[1]
                );
            }
        }
    }
}

/// `RunSet::holding` on a RAM tier's set, its rows in one run and in as
/// many as a set holds: the probe that builds the bloom in instructions per held
/// row, then scattered probes of held keys and of absent keys inside the held
/// span, in instructions and cycles per probe.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn run_set_probe_bench() {
    const RUNS: u64 = FOLD_THRESHOLD as u64 - 1;
    const ROWS: u64 = RUNS * 32_000;
    const PROBES: u64 = 200_000;
    /// Run `run`'s `row`-th key as an index into the held keys.
    type Layout = fn(run: u64, row: u64, per_run: u64) -> u64;
    let layouts: [(&str, u64, Layout); 3] = [
        ("one run", 1, |_, row, _| row),
        ("disjoint runs", RUNS, |run, row, per_run| run * per_run + row),
        ("interleaved runs", RUNS, |run, row, _| row * RUNS + run),
    ];
    let schema = make_schema_u64_i64();
    let counters = Counters::open([Event::INSTRUCTIONS, Event::CYCLES]);
    // Held keys are even, so `2k + 1` is absent and inside the span.
    let keys = |odd: u64| -> Vec<([u8; 8], u64)> {
        (0..PROBES)
            .map(|p| (2 * (scatter(p) % ROWS) + odd).to_be_bytes())
            .map(|key| (key, probe_key(&key)))
            .collect()
    };
    for (label, runs, layout) in layouts {
        let per_run = ROWS / runs;
        let mut set = RunSet::cancelling(DEFAULT_RAM_TIER_BYTES);
        for r in 0..runs {
            set.push(
                run(&schema, (0..per_run).map(|i| (2 * layout(r, i, per_run), 1))),
                &schema,
            );
        }
        assert_eq!((set.len() as u64, set.row_count() as u64), (runs, ROWS), "{label}");
        assert!(!set.is_full(), "{label}: past the tier's ceiling");

        let (first, _) = keys(0)[0];
        let ((), [build, _]) = counters.measure(|| set.holding(&first, probe_key(&first)).for_each(drop));
        println!(
            "run_set_probe_bench {runs:>2} {label:<17} bloom build {:5.1} instr/row",
            build as f64 / ROWS as f64
        );
        for (kind, odd) in [("hits", 0), ("misses", 1)] {
            let keys = keys(odd);
            let passes = keys.iter().filter(|(_, f)| set.may_contain(*f)).count();
            // A key's first holder, as a unique-PK probe reads it.
            let probe = || -> u64 {
                (keys.iter())
                    .filter_map(|(key, fingerprint)| set.holding(key, *fingerprint).next())
                    .map(|(run, rows)| run.get_weight(rows.start) as u64)
                    .sum()
            };
            // Unmeasured: the first pass builds the filters of the runs its
            // misses reach, so the measured one is the steady state.
            probe();
            let (found, [instr, cyc]) = counters.measure(probe);
            assert_eq!(found, PROBES * (1 - odd), "{label}, {kind}");
            println!(
                "run_set_probe_bench {runs:>2} {label:<17} {kind:<6} {:6.1} instr/probe, {:6.1} cycles/probe, {:5.2}% pass the bloom",
                instr as f64 / PROBES as f64,
                cyc as f64 / PROBES as f64,
                100.0 * passes as f64 / PROBES as f64
            );
        }
    }
}
