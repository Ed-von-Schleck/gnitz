//! The tick schedule and the one driver over it.

use super::*;
use crate::catalog::{CatalogColumn, CatalogEngine};
use crate::test_support::{
    circuit_nodes_batch, col_def, make_batch, register_identity_view, scan_all, scanning_circuit, scratch_dir,
    try_register_view, zset_of, LocalDrive, RowKey,
};
use gnitz_store::relation::Relation;
use gnitz_wire::{ColumnDef, TypeCode};
use gnitz_zset::schema::Slot;
use std::collections::HashMap;

fn view_cols() -> Vec<CatalogColumn> {
    vec![col_def("id", TypeCode::U64), col_def("v", TypeCode::I64)]
}

/// A base table `(id U64 PK, v I64)` in a fresh catalog.
fn engine_with_base(name: &str) -> (CatalogEngine, u64) {
    let mut engine = CatalogEngine::open(&scratch_dir("dag_exec", name), 1).unwrap();
    let base = engine.create_table("public.base", &view_cols(), &[0]).unwrap();
    (engine, base)
}

/// The views of [`engine_with_views`], each holding a multiple of the base's
/// delta so that reading the wrong input shows in its weights.
struct Views {
    /// `base UNION ALL base` through two sharded sides: 2×.
    twice: u64,
    /// Identity over the base: 1×.
    once: u64,
    /// Identity over `twice`: 2×, where reading the base would give 1×.
    deep: u64,
    /// `twice UNION ALL once`, a view two producers feed: 3×.
    union: u64,
    /// Identity over `union`: 3×.
    over_union: u64,
}

impl Views {
    fn all(&self) -> [u64; 5] {
        [self.twice, self.once, self.deep, self.union, self.over_union]
    }
}

/// [`engine_with_base`] plus [`Views`].
fn engine_with_views(name: &str) -> (CatalogEngine, u64, Views) {
    let (mut engine, base) = engine_with_base(name);
    let cols = view_cols();
    let mut circuit = gnitz_wire::Circuit::default();
    let sides = [0, 1].map(|_| {
        let scan = circuit.input_delta(base, gnitz_wire::ReadBound::None);
        circuit.shard(scan, &[0])
    });
    let merged = circuit.union(sides[0], sides[1]);
    circuit.sink(merged);
    let twice = try_register_view(&mut engine, circuit, "twice", &cols, 0, 0).unwrap();
    let once = register_identity_view(&mut engine, base, "once", &cols);
    let deep = register_identity_view(&mut engine, twice, "deep", &cols);
    let mut circuit = gnitz_wire::Circuit::default();
    let left = circuit.input_delta(twice, gnitz_wire::ReadBound::None);
    let right = circuit.input_delta(once, gnitz_wire::ReadBound::None);
    let merged = circuit.union(left, right);
    circuit.sink(merged);
    let union = try_register_view(&mut engine, circuit, "union", &cols, 0, 0).unwrap();
    let over_union = register_identity_view(&mut engine, union, "over_union", &cols);
    (engine, base, Views { twice, once, deep, union, over_union })
}

/// `(pk, weight, payload)` rows in `tid`'s own registered schema.
fn delta_for(engine: &CatalogEngine, tid: u64, rows: &[(u64, i64, i64)]) -> Batch {
    let schema = engine
        .registry
        .relation(tid)
        .map(Relation::schema)
        .expect("a registered relation");
    make_batch(&schema, rows)
}

/// The Z-set a relation holds.
fn held(engine: &mut CatalogEngine, tid: u64) -> HashMap<RowKey, i64> {
    let rows = scan_all(engine, tid);
    zset_of(&rows, rows.schema())
}

/// `rows` as a Z-set with every weight scaled by `k`.
fn times(engine: &CatalogEngine, tid: u64, rows: &[(u64, i64, i64)], k: i64) -> HashMap<RowKey, i64> {
    let scaled: Vec<_> = rows.iter().map(|&(pk, w, v)| (pk, w * k, v)).collect();
    let batch = delta_for(engine, tid, &scaled);
    zset_of(&batch, batch.schema())
}

/// One tick of `source` over `delta`.
fn tick(engine: &mut CatalogEngine, source: u64, round: u64, delta: Batch) {
    drive(&mut LocalDrive(engine), Drive::Tick { source, round }, delta).unwrap();
}

// ── The schedule ────────────────────────────────────────────────────────────

/// The schedule names one step per dependency edge out of the source's forward
/// closure, ordered by view id — so a producer always precedes the steps it
/// feeds — and leaves out the views awaiting a rebuild.
#[test]
fn the_schedule_names_every_edge_of_the_closure_in_id_order() {
    let step = |view, producer| Step { view, producer };
    let mut dag = DagEngine::default();
    for (view, sources) in [(2, &[1][..]), (3, &[1]), (4, &[2, 3]), (5, &[4]), (7, &[6])] {
        dag.apply_circuit_delta(&circuit_nodes_batch(view, &scanning_circuit(sources)));
    }

    let every = [step(2, 1), step(3, 1), step(4, 2), step(4, 3), step(5, 4)];
    assert_eq!(dag.tick_schedule(1), every);

    // A rebuild set is closed under dependents.
    dag.set_rebuild([2, 4, 5].into_iter().collect());
    assert_eq!(dag.tick_schedule(1), [step(3, 1)]);
    dag.rebuild_started(2);
    assert_eq!(dag.tick_schedule(1), [step(2, 1), step(3, 1)]);
}

// ── The drivers ─────────────────────────────────────────────────────────────

/// One tick of the base drives the whole dependent closure through real compiled
/// plans: a side per scan of the source, a view over a view reading its
/// producer's output, and a view two producers feed passing its reader both.
#[test]
fn a_tick_drives_the_whole_closure() {
    let (mut engine, base, views) = engine_with_views("evaluate");
    let rows = [(1, 1, 10), (2, 1, 20), (3, -1, 30)];

    let delta = delta_for(&engine, base, &rows);
    tick(&mut engine, base, 1, delta);

    for (vid, k) in [
        (views.twice, 2),
        (views.once, 1),
        (views.deep, 2),
        (views.union, 3),
        (views.over_union, 3),
    ] {
        assert_eq!(held(&mut engine, vid), times(&engine, base, &rows, k), "view {vid}");
    }
}

/// A backfill drive runs the named view alone — not the source's closure, which
/// would double-count into the dependents the source already populated — and
/// not the view's own dependents, which backfill from its store afterwards.
#[test]
fn backfill_chunk_runs_only_the_named_view() {
    let (mut engine, base, views) = engine_with_views("backfill");
    let backfill = |view| Drive::Backfill { view, source: base };
    let rows = [(1, 1, 10)];

    let chunk = delta_for(&engine, base, &rows);
    drive(&mut LocalDrive(&mut engine), backfill(views.twice), chunk).unwrap();
    let empty = delta_for(&engine, base, &[]);
    drive(&mut LocalDrive(&mut engine), backfill(views.twice), empty.clone()).unwrap();
    assert_eq!(held(&mut engine, views.twice), times(&engine, base, &rows, 2));
    for vid in views.all().into_iter().filter(|&v| v != views.twice) {
        assert!(held(&mut engine, vid).is_empty(), "view {vid}");
    }

    let err = drive(&mut LocalDrive(&mut engine), backfill(999_999), empty).unwrap_err();
    assert!(err.contains("is not registered"), "{err}");
}

// ── A side's output relay ───────────────────────────────────────────────────

/// Records the row count and whether the plan keeps rank 0's rows alone for
/// every batch a relay sends, and relays it back.
struct Recorder {
    dag: DagEngine,
    registry: RelationRegistry,
    sent: Vec<(usize, bool)>,
}

impl DriveHost for Recorder {
    fn parts(&mut self) -> (&mut DagEngine, &mut RelationRegistry) {
        (&mut self.dag, &mut self.registry)
    }

    fn exchange(&mut self, _view_id: u64, batch: Cow<'_, Batch>, plan: &ScatterPlan, _fold: bool) -> Batch {
        let splits = plan.share(&batch, Slot::new(0, 2)).len() < batch.len();
        self.sent.push((batch.len(), splits));
        batch.into_owned()
    }
}

/// A side whose output every worker holds whole never reaches the host: each
/// rank keeps its own share, and the ranks' shares partition the output. A
/// round and a broadcast reach it, the round with its plan.
#[test]
fn a_replica_sides_output_is_shared_without_a_round() {
    let schema = crate::test_support::make_schema_u64_i64();
    let rows: Vec<(u64, i64, i64)> = (1..=16).map(|k| (k, 1, k as i64 * 10)).collect();
    let delta = make_batch(&schema, &rows);
    let plan = std::rc::Rc::new(ScatterPlan::group(&schema, &[0]).unwrap());
    let mut shares: Vec<u64> = Vec::new();
    for rank in [0u32, 1] {
        let mut host = Recorder {
            dag: DagEngine::default(),
            registry: RelationRegistry::new("", Slot::new(rank, 2), Default::default()),
            sent: Vec::new(),
        };
        let share = relay(
            &mut host,
            99,
            Cow::Borrowed(&delta),
            Some(&Relay::Share(plan.clone())),
            false,
        );
        shares.extend((0..share.len()).map(|i| share.get_pk(i) as u64));
        relay(
            &mut host,
            99,
            Cow::Borrowed(&delta),
            Some(&Relay::Round(plan.clone())),
            false,
        );
        relay(&mut host, 99, Cow::Borrowed(&delta), Some(&Relay::Broadcast), false);
        assert_eq!(host.sent, [(16, true), (16, false)], "rank {rank}");
    }
    shares.sort_unstable();
    assert_eq!(shares, (1..=16).collect::<Vec<u64>>());
}

// ── Per-epoch cost ──────────────────────────────────────────────────────────

/// One-row ticks per cost cell, and rows in the wide tick.
const BENCH_TICKS: u64 = 10_000;

/// Column records for a view whose output is `schema`.
fn cols_of(schema: &gnitz_zset::schema::SchemaDescriptor) -> Vec<CatalogColumn> {
    (0..schema.num_columns())
        .map(|ci| {
            let c = schema.column(ci).expect("in range");
            ColumnDef::new(format!("c{ci}"), c.type_code, c.nullable).into()
        })
        .collect()
}

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
    let reduced = circuit.reduce_multi(scan, &group, &aggs, false);
    circuit.sink(reduced);
    try_register_view(&mut engine, circuit, "vgroup", &cols_of(&grouped), 0, 0).unwrap();
    (engine, base, views.once)
}

/// The net weight `tid` holds.
fn net_weight(engine: &mut CatalogEngine, tid: u64) -> u64 {
    held(engine, tid).values().sum::<i64>() as u64
}

/// Instructions per one-row tick through every plan shape, and per wide tick
/// into eight readers beside its seven batch copies.
///
/// ```text
/// cd crates && cargo test -p gnitz-server --release drive_tick_bench \
///     -- --ignored --nocapture --test-threads=1
/// ```
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
