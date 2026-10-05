use super::super::table::{DEFAULT_RAM_TIER_BYTES, MEMTABLE_BYTES};
use super::*;
use crate::test_support::{make_schema_pk_u64_payload_string, make_schema_u64_i64};
use gnitz_foundation::perf::{self, Counter};
use gnitz_zset::repr::BatchBuilder;

/// A consolidated run of `rows`, `(pk, weight)` each and ascending by PK; a
/// string payload is spelled out long enough to live on the heap.
fn run(schema: &SchemaDescriptor, rows: impl Iterator<Item = (u64, i64)>) -> Batch {
    let mut b = BatchBuilder::new(schema);
    for (pk, weight) in rows {
        b.begin_row(pk as u128, weight);
        match schema.string_payload_slots() {
            0 => b.put_int(pk as u128),
            _ => b.put_string(&format!("{pk:040}")),
        }
        b.end_row();
    }
    let mut run = b.finish();
    run.certify_consolidated();
    run
}

/// 40 scattered bits, distinct per `seq`.
fn scatter(seq: u64) -> u64 {
    seq.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 24
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
        let retracted = set
            .runs
            .iter()
            .map(|r| (0..r.len()).filter(|&i| r.get_weight(i) < 0).count());
        let rows_out = rows_in - 2 * retracted.sum::<usize>();
        let before = perf::rss_bytes();
        perf::reset_peak_rss();
        let ((), instructions) = counter.measure(|| set.fold(&schema));
        let peak = perf::peak_rss_bytes().saturating_sub(before);
        assert_eq!(set.row_count(), rows_out, "{label}");
        println!(
            "run_set_fold_bench {label:<34} {rows_in} rows in, {rows_out} out, {:5.1} instr/row, peak {:.2}x the output",
            instructions as f64 / rows_in as f64,
            peak as f64 / set.bytes as f64
        );
    }
}

/// `RunSet::push` at a memtable's cadence: a set is pushed `tick`-row runs of
/// fresh keys until it is full, then cleared, as a drain leaves it. Instructions
/// and cycles per pushed row with the set never probed, and instructions with
/// its bloom live from each fill's first push on, beside the rows its folds
/// merged per row pushed. The cycles price the bulk copies of a fold, which
/// retire next to no instructions.
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
    let (counter, cycles) = (Counter::instructions(), Counter::cycles());
    println!(
        "run_set_push_bench {:<28} {:>9} {:>12} {:>10} {:>10} {:>10}",
        "", "rows/fill", "folded/row", "instr/row", "cycles/row", "probed"
    );
    for (schema_label, schema) in schemas {
        for (order_label, key) in orders {
            for tick in [1u64, 16, 256] {
                let mut cells = [0.0; 2];
                let (mut rows, mut folded, mut unprobed_cycles) = (0u64, 0u64, 0u64);
                for (cell, probed) in cells.iter_mut().zip([false, true]) {
                    (rows, folded) = (0, 0);
                    let mut instructions = 0;
                    let mut set = RunSet::new(MEMTABLE_BYTES);
                    for _ in 0..FILLS {
                        // Enough ticks to fill the set, built ahead of the measurement.
                        let ticks: Vec<Batch> = (0..)
                            .map(|t| {
                                let mut keys: Vec<u64> = (0..tick).map(|i| key(rows + t * tick + i)).collect();
                                keys.sort_unstable();
                                run(&schema, keys.into_iter().map(|pk| (pk, 1)))
                            })
                            .scan(0, |bytes, b| {
                                let full = *bytes > MEMTABLE_BYTES;
                                *bytes += b.total_bytes();
                                (!full).then_some(b)
                            })
                            .collect();
                        // What the set's own folds will merge, by its trigger.
                        let (mut runs, mut held) = (0, 0);
                        for _ in &ticks {
                            (runs, held) = (runs + 1, held + tick);
                            if runs == FOLD_THRESHOLD {
                                (runs, folded) = (1, folded + held);
                            }
                        }
                        let (((), i), c) = cycles.measure(|| {
                            counter.measure(|| {
                                for (at, b) in ticks.into_iter().enumerate() {
                                    set.push(b, &schema);
                                    if probed && at == 0 {
                                        std::hint::black_box(set.may_contain(0));
                                    }
                                }
                            })
                        });
                        instructions += i;
                        unprobed_cycles += c * !probed as u64;
                        rows += held;
                        assert_eq!(set.len(), runs, "{schema_label}, {order_label}, {tick}: folds");
                        assert_eq!(set.row_count() as u64, held);
                        assert!(set.is_full(), "{schema_label}, {order_label}, {tick}: not filled");
                        assert_eq!(set.bloom.get().is_some(), probed);
                        set.clear();
                    }
                    *cell = instructions as f64 / rows as f64;
                }
                println!(
                    "run_set_push_bench {:<28} {:>9} {:>12.1} {:>10.1} {:>10.1} {:>10.1}",
                    format!("{schema_label}, {order_label}, {tick}-row"),
                    rows / FILLS as u64,
                    folded as f64 / rows as f64,
                    cells[0],
                    unprobed_cycles as f64 / rows as f64,
                    cells[1]
                );
            }
        }
    }
}

/// `RunSet::find_pk_bytes` on a RAM tier's set, its rows in one run and in as
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
        ("1 run", 1, |_, row, _| row),
        ("15 disjoint runs", RUNS, |run, row, per_run| run * per_run + row),
        ("15 interleaved runs", RUNS, |run, row, _| row * RUNS + run),
    ];
    let schema = make_schema_u64_i64();
    let (instructions, cycles) = (Counter::instructions(), Counter::cycles());
    // Held keys are even, so `2k + 1` is absent and inside the span.
    let keys = |odd: u64| -> Vec<([u8; 8], u64)> {
        (0..PROBES)
            .map(|p| (2 * (scatter(p) % ROWS) + odd).to_be_bytes())
            .map(|key| (key, probe_key(&key)))
            .collect()
    };
    for (label, runs, layout) in layouts {
        let per_run = ROWS / runs;
        let mut set = RunSet::new(DEFAULT_RAM_TIER_BYTES);
        for r in 0..runs {
            set.push(
                run(&schema, (0..per_run).map(|i| (2 * layout(r, i, per_run), 1))),
                &schema,
            );
        }
        assert_eq!((set.len() as u64, set.row_count() as u64), (runs, ROWS), "{label}");
        assert!(!set.is_full(), "{label}: past the tier's ceiling");

        let (first, _) = keys(0)[0];
        let ((), build) = instructions.measure(|| set.find_pk_bytes(&first, probe_key(&first), |_, _| ()));
        println!(
            "run_set_probe_bench {label:<20} bloom build {:5.1} instr/row",
            build as f64 / ROWS as f64
        );
        for (kind, odd) in [("hits", 0), ("misses", 1)] {
            let keys = keys(odd);
            let passes = keys.iter().filter(|(_, f)| set.may_contain(*f)).count();
            let ((found, instr), cyc) = cycles.measure(|| {
                instructions.measure(|| {
                    let mut found = 0u64;
                    for (key, fingerprint) in &keys {
                        set.find_pk_bytes(key, *fingerprint, |run, start| found += run.get_weight(start) as u64);
                    }
                    found
                })
            });
            assert_eq!(found, PROBES * (1 - odd), "{label}, {kind}");
            println!(
                "run_set_probe_bench {label:<20} {kind:<6} {:6.1} instr/probe, {:6.1} cycles/probe, {:5.2}% pass the bloom",
                instr as f64 / PROBES as f64,
                cyc as f64 / PROBES as f64,
                100.0 * passes as f64 / PROBES as f64
            );
        }
    }
}
