use super::tests::cols_of;
use super::tests::delta_for;
use super::tests::engine_with_base;
use super::tests::engine_with_views;
use super::tests::ingested;
use super::tests::left_join_engine;
use super::tests::net_weight;
use super::tests::rows_of;
use super::tests::tick;
use super::tests::view_cols;
use super::*;
use crate::catalog::CatalogEngine;
use crate::test_support::{register_identity_view, scan_all, try_register_view, LocalDrive};
use gnitz_store::relation::Relation;

/// [`engine_with_views`] plus a GROUP BY view over the base.
fn engine_with_every_plan_shape(name: &str) -> (CatalogEngine, u64, u64) {
    let (mut engine, base, views) = engine_with_views(name);
    let base_schema = engine.registry.relation(base).map(Relation::schema).unwrap();

    let (group, aggs) = ([1u32], [gnitz_wire::AggDescriptor::COUNT_STAR]);
    let grouped = *gnitz_zset::stream::ReducePlan::from_wire(&base_schema, &group, &aggs, false)
        .unwrap()
        .output_schema();
    let mut circuit = gnitz_wire::Circuit::default();
    let scan = circuit.input_delta(base, gnitz_wire::ReadBound::None);
    let reduced = circuit.reduce_multi(scan, &group, &aggs);
    circuit.sink(reduced);
    try_register_view(&mut engine, circuit, "vgroup", &cols_of(&grouped), 0, 0).unwrap();
    (engine, base, views.once)
}

/// One-row ticks per cost cell, and rows in the wide tick.
const BENCH_TICKS: u64 = 10_000;

/// Instructions per one-row tick through every plan shape, per tick that brings
/// this worker no row, and per wide tick into eight readers beside its seven
/// batch copies.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn drive_tick_bench() {
    let counter = gnitz_foundation::perf::Counter::instructions().expect("instructions counter");

    let (mut engine, base, once) = engine_with_every_plan_shape("tick_bench_shapes");
    // Compiles every plan outside the measurement.
    let warm = delta_for(&engine, base, &[(0, 1, 0)]);
    tick(&mut engine, base, 1, warm);
    let deltas: Vec<Batch> = (1..=BENCH_TICKS)
        .map(|i| delta_for(&engine, base, &[(i, 1, i as i64)]))
        .collect();
    let ((), instr) = counter.measure(|| {
        for (i, delta) in deltas.into_iter().enumerate() {
            tick(&mut engine, base, i as u64 + 2, delta);
        }
    });
    assert_eq!(net_weight(&mut engine, once), BENCH_TICKS + 1, "every tick landed");
    println!(
        "one-row ticks, every plan shape  {:>10} instr/tick",
        instr / BENCH_TICKS
    );
    let ((), instr) = counter.measure(|| {
        for round in 0..BENCH_TICKS {
            let what = Drive::Tick {
                source: base,
                round: BENCH_TICKS + 2 + round,
            };
            drive(&mut LocalDrive(&mut engine), what, None).unwrap();
        }
    });
    assert_eq!(
        net_weight(&mut engine, once),
        BENCH_TICKS + 1,
        "an empty tick lands nothing"
    );
    println!(
        "empty ticks, every plan shape    {:>10} instr/tick",
        instr / BENCH_TICKS
    );

    let (mut engine, base) = engine_with_base("tick_bench_fanout");
    let cols = view_cols();
    let readers: Vec<u64> = (0..8)
        .map(|r| register_identity_view(&mut engine, base, &format!("v{r}"), &cols))
        .collect();
    let warm = delta_for(&engine, base, &[(0, 1, 0)]);
    tick(&mut engine, base, 1, warm);
    let rows: Vec<(u64, i64, i64)> = (1..=BENCH_TICKS).map(|i| (i, 1, i as i64)).collect();
    let delta = delta_for(&engine, base, &rows);
    let (copies, clone_instr) = counter.measure(|| (0..7).map(|_| Batch::clone(&delta)).collect::<Vec<_>>());
    drop(copies);
    let ((), instr) = counter.measure(|| tick(&mut engine, base, 2, delta));
    assert_eq!(net_weight(&mut engine, readers[7]), BENCH_TICKS + 1, "the tick landed");
    println!("{BENCH_TICKS}-row tick, 8 readers        {instr:>10} instr, of which 7 copies {clone_instr}");
}

/// Instructions per wide tick into the preserved side of `a LEFT JOIN b`, by join
/// key and by whether the other side matches.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn left_join_tick_bench() {
    let counter = gnitz_foundation::perf::Counter::instructions().expect("instructions counter");
    let ids = 1..=BENCH_TICKS;
    for (key_name, key) in [("NOT NULL payload key", 1), ("PK key", 0), ("nullable payload key", 2)] {
        for matched in [false, true] {
            let name = format!("left_join_bench_{key}_{matched}");
            let (mut engine, [a, b], view) = left_join_engine(&name, key);
            if matched {
                let rows: Vec<(u64, [Option<u64>; 1])> = ids.clone().map(|i| (i, [Some(i)])).collect();
                let rows: Vec<(u64, &[Option<u64>])> = rows.iter().map(|(k, w)| (*k, &w[..])).collect();
                let delta = rows_of(&engine, b, 1, &rows);
                let delta = ingested(&mut engine, b, delta);
                tick(&mut engine, b, 1, delta);
            }
            // Compiles the plan outside the measurement, and nets to nothing.
            for (round, weight) in [(2, 1), (3, -1)] {
                let warm = rows_of(&engine, a, weight, &[(0, &[Some(0), Some(0)])]);
                let warm = ingested(&mut engine, a, warm);
                tick(&mut engine, a, round, warm);
            }
            let rows: Vec<(u64, [Option<u64>; 2])> = ids.clone().map(|i| (i, [Some(i), Some(i)])).collect();
            let rows: Vec<(u64, &[Option<u64>])> = rows.iter().map(|(id, c)| (*id, &c[..])).collect();
            let delta = rows_of(&engine, a, 1, &rows);
            let delta = ingested(&mut engine, a, delta);
            let ((), instr) = counter.measure(|| tick(&mut engine, a, 4, delta));

            let out = scan_all(&mut engine, view);
            let w = out.schema().num_payload_cols() - 1;
            let live = |i: &usize| out.get_weight(*i) == 1;
            let null_filled = (0..out.len())
                .filter(live)
                .filter(|&i| gnitz_expr::payload_is_null(&*out, i, w))
                .count() as u64;
            assert_eq!(
                net_weight(&mut engine, view),
                BENCH_TICKS,
                "{key_name}: every row landed"
            );
            assert_eq!(null_filled, if matched { 0 } else { BENCH_TICKS }, "{key_name}");
            let other = if matched {
                "every row matches"
            } else {
                "empty other side"
            };
            println!("{BENCH_TICKS}-row tick, {key_name:<22} {other:<18} {instr:>11} instr");
        }
    }
}
