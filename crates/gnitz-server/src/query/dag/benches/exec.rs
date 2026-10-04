//! Release benchmark: instructions the tick driver costs per schedule step, with
//! one row and with none, and per source row of a wide tick by how many views
//! read the source.
//!
//! ```text
//! cd crates && cargo test -p gnitz --release drive_tick_bench \
//!     -- --ignored --nocapture --test-threads=1
//! ```

use super::tests::{delta_for, engine_with_base, engine_with_views, tick, view_cols};
use super::*;
use crate::test_support::{net_weight, register_identity_view};

/// Timed ticks per cell of the closure, with one row and with none.
const TICKS: u64 = 10_000;
/// Timed wide ticks per reader count, and the fresh ids each carries.
const WIDE_TICKS: u64 = 20;
const WIDE_TICK_ROWS: u64 = 10_000;

/// A tick through [`engine_with_views`]' closure — a view of two sides, a view
/// over a view, a view two producers feed — per step, over one row and over a
/// delta that brings this worker none; then wide ticks of a source one view
/// reads and eight do, every reader but the last taking a copy of the delta.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn drive_tick_bench() {
    let counter = gnitz_foundation::perf::Counter::instructions();

    let (mut engine, base, views) = engine_with_views("tick_bench_closure");
    let steps = engine.dag.tick_schedule(base).len() as u64;
    // Compiles every plan outside the measurement.
    tick(&mut engine, base, None);
    let deltas: Vec<Batch> = (0..TICKS)
        .map(|i| delta_for(&engine, base, &[(i, 1, i as i64)]))
        .collect();
    let ((), one_row) = counter.measure(|| deltas.into_iter().for_each(|delta| tick(&mut engine, base, delta)));
    let ((), no_row) = counter.measure(|| (0..TICKS).for_each(|_| tick(&mut engine, base, None)));
    assert_eq!(
        net_weight(&engine, views.over_union) as u64,
        3 * TICKS,
        "every row reached the last view through both producers"
    );
    for (label, instr) in [("one row", one_row), ("no row", no_row)] {
        println!("{steps}-step tick, {label}: {:>6} instr/step", instr / (TICKS * steps));
    }

    for readers in [1, 8] {
        let (mut engine, base) = engine_with_base(&format!("tick_bench_fanout_{readers}"));
        let cols = view_cols();
        let views: Vec<u64> = (0..readers)
            .map(|r| register_identity_view(&mut engine, base, &format!("v{r}"), &cols))
            .collect();
        tick(&mut engine, base, None);
        let deltas: Vec<Batch> = (0..WIDE_TICKS)
            .map(|t| {
                let ids = t * WIDE_TICK_ROWS..(t + 1) * WIDE_TICK_ROWS;
                let rows: Vec<(u64, i64, i64)> = ids.map(|i| (i, 1, i as i64)).collect();
                delta_for(&engine, base, &rows)
            })
            .collect();
        let ((), instr) = counter.measure(|| deltas.into_iter().for_each(|delta| tick(&mut engine, base, delta)));
        for view in views {
            assert_eq!(
                net_weight(&engine, view) as u64,
                WIDE_TICKS * WIDE_TICK_ROWS,
                "view {view}"
            );
        }
        println!(
            "{WIDE_TICK_ROWS}-row tick, source read by {readers}: {:>6.1} instr/row",
            instr as f64 / (WIDE_TICKS * WIDE_TICK_ROWS) as f64
        );
    }
}
