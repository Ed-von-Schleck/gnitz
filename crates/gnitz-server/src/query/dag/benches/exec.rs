//! Release benchmark: instructions the tick driver costs per schedule step, with
//! one row and with none, and per source row of a wide tick by how many views
//! read the source.
//!
//! ```text
//! cd crates && cargo test -p gnitz --release drive_tick_bench \
//!     -- --ignored --nocapture --test-threads=1
//! ```

use super::tests::{delta_for, drive, engine_with_base, engine_with_views, tick_over, view_cols};
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
    tick_over(&mut engine, base, None);
    let deltas: Vec<Batch> = (0..TICKS)
        .map(|i| delta_for(&engine, base, &[(i, 1, i as i64)]))
        .collect();
    let ((), one_row) = counter.measure(|| deltas.into_iter().for_each(|delta| tick_over(&mut engine, base, delta)));
    let ((), no_row) = counter.measure(|| (0..TICKS).for_each(|_| tick_over(&mut engine, base, None)));
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
        tick_over(&mut engine, base, None);
        let deltas: Vec<Batch> = (0..WIDE_TICKS)
            .map(|t| {
                let ids = t * WIDE_TICK_ROWS..(t + 1) * WIDE_TICK_ROWS;
                let rows: Vec<(u64, i64, i64)> = ids.map(|i| (i, 1, i as i64)).collect();
                delta_for(&engine, base, &rows)
            })
            .collect();
        let ((), instr) = counter.measure(|| deltas.into_iter().for_each(|delta| tick_over(&mut engine, base, delta)));
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

/// A host whose exchange hands this worker its own rows back: every round of a
/// drive runs, at the cost of everything but the mesh.
struct Echo<'a>(&'a mut crate::catalog::CatalogEngine, u64);

impl DriveHost for Echo<'_> {
    fn parts(&mut self) -> (&mut DagEngine, &mut RelationRegistry) {
        (&mut self.0.dag, &mut self.0.registry)
    }

    fn exchange(
        &mut self,
        _view_id: u64,
        batch: Cow<'_, Batch>,
        _plan: &ScatterPlan,
        _fold: bool,
        drained: bool,
    ) -> (Batch, bool) {
        self.1 += 1;
        (batch.into_owned(), drained)
    }
}

/// Ticks of a worker beside three others, through views that exchange: a
/// `GROUP BY`, a global `COUNT(*)` and a hash-keyed `DISTINCT`, per step over
/// one row and per row of a wide tick; then wide ticks of a source eight
/// projections read.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn drive_round_bench() {
    use crate::test_support::{col_def, cols_of, scratch_dir, try_register_view};
    use gnitz_wire::{AggDescriptor, Circuit, ReadBound, TypeCode};
    let counter = gnitz_foundation::perf::Counter::instructions();
    let cols = vec![
        col_def("id", TypeCode::U64),
        col_def("a", TypeCode::I64),
        col_def("b", TypeCode::I64),
    ];
    let delta = |engine: &crate::catalog::CatalogEngine, base: u64, ids: std::ops::Range<u64>| {
        let schema = engine.registry.relation(base).map(Relation::schema).unwrap();
        let mut bb = gnitz_zset::repr::BatchBuilder::new(&schema);
        for id in ids {
            bb.begin_row(id as u128, 1);
            bb.put_int((id % 1000) as u128);
            bb.put_int((id.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 40) as u128);
            bb.end_row();
        }
        bb.finish()
    };

    let mut engine = crate::catalog::CatalogEngine::open(&scratch_dir("dag_exec", "round_bench"), 4).unwrap();
    let base = engine.create_table("public.base", &cols, &[0]).unwrap();
    let schema = engine.registry.relation(base).map(Relation::schema).unwrap();
    let aggs = [AggDescriptor::COUNT_STAR];
    let grouped = *gnitz_zset::stream::ReducePlan::from_wire(&schema, &[1], &aggs, false)
        .unwrap()
        .output_schema();
    let mut circuit = Circuit::default();
    let scan = circuit.input_delta(base, ReadBound::None);
    circuit.reduce_multi(scan, &[1], &aggs);
    try_register_view(&mut engine, circuit, "grouped", &cols_of(&grouped), 0, 0).unwrap();
    let counted = *gnitz_zset::stream::ReducePlan::from_wire(&schema, &[], &aggs, true)
        .unwrap()
        .output_schema();
    let mut circuit = Circuit::default();
    let scan = circuit.input_delta(base, ReadBound::None);
    circuit.reduce_multi(scan, &[], &aggs);
    try_register_view(&mut engine, circuit, "counted", &cols_of(&counted), 0, 0).unwrap();
    let mut circuit = Circuit::default();
    let scan = circuit.input_delta(base, ReadBound::None);
    let hashed = circuit.map_hash_row(scan, &[(1, TypeCode::I64), (2, TypeCode::I64)]);
    circuit.distinct(hashed);
    let hashed_cols = vec![
        col_def("h", TypeCode::U128),
        col_def("a", TypeCode::I64),
        col_def("b", TypeCode::I64),
    ];
    try_register_view(&mut engine, circuit, "distinct", &hashed_cols, 0, 0).unwrap();

    let steps = engine.dag.tick_schedule(base).len() as u64;
    let mut host = Echo(&mut engine, 0);
    drive(&mut host, base, None);
    let before = host.1;
    let ((), one_row) = counter.measure(|| {
        for i in 0..TICKS {
            let d = delta(host.0, base, i..i + 1);
            drive(&mut host, base, Some(d));
        }
    });
    let ((), no_row) = counter.measure(|| {
        for _ in 0..TICKS {
            drive(&mut host, base, None);
        }
    });
    assert_eq!(host.1 - before, 2 * TICKS * steps, "a round a step, rows or none");
    let wide: Vec<Batch> = (0..WIDE_TICKS)
        .map(|t| {
            delta(
                host.0,
                base,
                TICKS + t * WIDE_TICK_ROWS..TICKS + (t + 1) * WIDE_TICK_ROWS,
            )
        })
        .collect();
    let ((), wide_instr) = counter.measure(|| {
        for d in wide {
            drive(&mut host, base, Some(d));
        }
    });
    for (label, instr) in [("one row", one_row), ("no row", no_row)] {
        println!("{steps}-round tick, {label}: {:>6} instr/step", instr / (TICKS * steps));
    }
    println!(
        "{steps}-round {WIDE_TICK_ROWS}-row tick: {:>6.1} instr/row",
        wide_instr as f64 / (WIDE_TICKS * WIDE_TICK_ROWS) as f64
    );

    let mut engine = crate::catalog::CatalogEngine::open(&scratch_dir("dag_exec", "round_bench_fanout"), 1).unwrap();
    let base = engine.create_table("public.base", &cols, &[0]).unwrap();
    let narrow = vec![col_def("id", TypeCode::U64), col_def("b", TypeCode::I64)];
    for r in 0..8 {
        let mut circuit = Circuit::default();
        let scan = circuit.input_delta(base, ReadBound::None);
        circuit.map(scan, &[2]);
        try_register_view(&mut engine, circuit, &format!("p{r}"), &narrow, 0, 0).unwrap();
    }
    tick_over(&mut engine, base, None);
    let wide: Vec<Batch> = (0..WIDE_TICKS)
        .map(|t| delta(&engine, base, t * WIDE_TICK_ROWS..(t + 1) * WIDE_TICK_ROWS))
        .collect();
    let ((), instr) = counter.measure(|| wide.into_iter().for_each(|d| tick_over(&mut engine, base, d)));
    println!(
        "{WIDE_TICK_ROWS}-row tick, source read by 8 projections: {:>6.1} instr/row",
        instr as f64 / (WIDE_TICKS * WIDE_TICK_ROWS) as f64
    );
}
