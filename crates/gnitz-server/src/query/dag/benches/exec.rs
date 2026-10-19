//! Release benchmark: instructions the tick driver costs past the seal, per
//! schedule step of a tick with one row and with none, and per source row of a
//! wide tick — through a closure that exchanges nothing, through views that
//! exchange beside three other workers, and by how many views read the source.
//!
//! ```text
//! cd crates && cargo test -p gnitz-server --release drive_tick_bench \
//!     -- --ignored --nocapture --test-threads=1
//! ```

use super::tests::{base_rows, engine_with_base, engine_with_views, register_count, view_cols};
use super::*;
use crate::catalog::CatalogEngine;
use crate::test_support::{
    col_def, cols_of, make_schema_u64_i64, net_weight, register_identity_view, try_register_view, EchoDrive,
};
use gnitz_foundation::perf::Counter;
use gnitz_wire::{AggDescriptor, Circuit, ReadBound, TypeCode};
use gnitz_zset::stream::ReducePlan;

/// Timed ticks per cell, with one row and with none.
const TICKS: u64 = 10_000;
/// Timed wide ticks per cell, and the fresh ids each carries.
const WIDE_TICKS: u64 = 20;
const WIDE_TICK_ROWS: u64 = 10_000;

/// The distinct values of `v` over all rows.
const VALUES: u64 = 1000;

/// `ids` as base rows at weight 1, `v` one of [`VALUES`].
fn rows(ids: std::ops::Range<u64>) -> Option<Batch> {
    let rows: Vec<(u64, i64, i64)> = ids.map(|id| (id, 1, (id % VALUES) as i64)).collect();
    Some(base_rows(&rows))
}

/// The instructions `deltas`, each one tick of `base`, take to drive.
fn drive(counter: &Counter, host: &mut EchoDrive, base: u64, deltas: Vec<Option<Batch>>) -> u64 {
    let ((), instr) = counter.measure(|| {
        for delta in deltas {
            run_tick(host, base, 1, delta).unwrap();
        }
    });
    instr
}

/// The one-row and no-row cells of `base`'s schedule on `engine`, per step,
/// with the rounds they exchanged; then its wide ticks, per row, from id `TICKS`.
fn cells(counter: &Counter, label: &str, engine: &mut CatalogEngine, base: u64) -> u64 {
    let steps = engine.dag.tick_schedule(base).len() as u64;
    let mut host = EchoDrive::new(engine);
    // Opens every view's operator state outside the measurement.
    run_tick(&mut host, base, 1, None).unwrap();
    let start = host.rounds;
    let one_row = drive(counter, &mut host, base, (0..TICKS).map(|i| rows(i..i + 1)).collect());
    let no_row = drive(counter, &mut host, base, (0..TICKS).map(|_| None).collect());
    let rounds = (host.rounds - start) / (2 * TICKS);
    let wide = (0..WIDE_TICKS).map(|t| rows(TICKS + t * WIDE_TICK_ROWS..TICKS + (t + 1) * WIDE_TICK_ROWS));
    let wide = drive(counter, &mut host, base, wide.collect());
    for (cell, instr) in [("one row", one_row), ("no row", no_row)] {
        println!(
            "{label}, {steps} steps, {rounds} rounds a tick, {cell}: {:>6} instr/step",
            instr / (TICKS * steps)
        );
    }
    println!(
        "{label}, {WIDE_TICK_ROWS}-row tick: {:>6.1} instr/row",
        wide as f64 / (WIDE_TICKS * WIDE_TICK_ROWS) as f64
    );
    rounds
}

/// The tick driver's cost per step and per row, through each fixture.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn drive_tick_bench() {
    let counter = Counter::instructions();
    let all_rows = (TICKS + WIDE_TICKS * WIDE_TICK_ROWS) as i64;

    // A view of two sides, a view over a view, a view two producers feed.
    let (mut engine, base, views) = engine_with_views("tick_bench_closure");
    assert_eq!(cells(&counter, "closure", &mut engine, base), 0);
    assert_eq!(
        net_weight(&engine, views.over_union),
        3 * all_rows,
        "every row reached the last view through both producers"
    );

    // A `GROUP BY`, a global `COUNT(*)` and a hash-keyed `DISTINCT`, each behind
    // an exchange.
    let (mut engine, base) = engine_with_base("tick_bench_rounds", 4);
    let aggs = [AggDescriptor::COUNT_STAR];
    let grouped = ReducePlan::from_wire(&make_schema_u64_i64(), &[1], &aggs, false).unwrap();
    let grouped = cols_of(grouped.output_schema());
    let mut circuit = Circuit::default();
    let scan = circuit.input_delta(base, ReadBound::None);
    circuit.reduce_multi(scan, &[1], &aggs);
    let grouped = try_register_view(&mut engine, circuit, "grouped", &grouped, 0, 0).unwrap();
    let (counted, _) = register_count(&mut engine, base, "counted");
    let mut circuit = Circuit::default();
    let scan = circuit.input_delta(base, ReadBound::None);
    let hashed = circuit.map_hash_row(scan, &[(1, TypeCode::I64)]);
    circuit.distinct(hashed);
    let hashed_cols = [col_def("h", TypeCode::U128), col_def("v", TypeCode::I64)];
    let distinct = try_register_view(&mut engine, circuit, "distinct", &hashed_cols, 0, 0).unwrap();
    assert_eq!(
        cells(&counter, "rounds", &mut engine, base),
        3,
        "a round a step, rows or none"
    );
    assert_eq!(net_weight(&engine, counted), 1);
    for view in [grouped, distinct] {
        assert_eq!(net_weight(&engine, view), VALUES as i64, "view {view}");
    }

    // Wide ticks of a source one view reads and eight do, every reader but the
    // last taking a copy of the delta.
    for readers in [1, 8] {
        let (mut engine, base) = engine_with_base(&format!("tick_bench_fanout_{readers}"), 1);
        let views: Vec<u64> = (0..readers)
            .map(|r| register_identity_view(&mut engine, base, &format!("v{r}"), &view_cols()))
            .collect();
        cells(&counter, &format!("read by {readers}"), &mut engine, base);
        for view in views {
            assert_eq!(net_weight(&engine, view), all_rows, "view {view}");
        }
    }
}
