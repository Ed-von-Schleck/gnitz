//! The tick schedule and the one driver over it.

use super::*;
use crate::catalog::{CatalogColumn, CatalogEngine};
use crate::test_support::{
    col_def, make_batch, register_identity_view, scratch_dir, sum_weights, try_register_view, LocalDrive,
};
use gnitz_store::relation::Relation;
use gnitz_wire::{ColumnDef, TypeCode};

fn view_cols() -> Vec<CatalogColumn> {
    vec![col_def("id", TypeCode::U64), col_def("v", TypeCode::I64)]
}

/// A base table `(id U64 PK, v I64)` in a fresh catalog.
fn engine_with_base(name: &str) -> (CatalogEngine, u64) {
    let mut engine = CatalogEngine::open(&scratch_dir("dag_exec", name), 1).unwrap();
    let base = engine.create_table("public.base", &view_cols(), &[0]).unwrap();
    (engine, base)
}

/// [`engine_with_base`] plus `base → {va, vb}` and `va → vdeep`: a two-wide
/// fan-out and a second rung over one of its arms.
fn engine_with_fanout(name: &str) -> (CatalogEngine, u64, u64, u64, u64) {
    let (mut engine, base) = engine_with_base(name);
    let cols = view_cols();
    let a = register_identity_view(&mut engine, base, "va", &cols);
    let b = register_identity_view(&mut engine, base, "vb", &cols);
    let deep = register_identity_view(&mut engine, a, "vdeep", &cols);
    (engine, base, a, b, deep)
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

/// The net weight a relation's own store holds.
fn live_weight(engine: &CatalogEngine, tid: u64) -> i64 {
    sum_weights(
        engine
            .registry
            .relation(tid)
            .map(|r| r.cursor())
            .expect("an owned store"),
    )
}

// ── The schedule ────────────────────────────────────────────────────────────

/// The schedule names one step per dependency edge out of the source's forward
/// closure, ordered by view id — so a producer always precedes the steps it
/// feeds, and a tick of an intermediate view runs only what that view reaches.
#[test]
fn the_schedule_names_every_edge_of_the_closure_in_id_order() {
    let (engine, base, a, b, deep) = engine_with_fanout("schedule");
    let step = |view, producer| Step { view, producer };

    let dag = &engine.dag;
    assert_eq!(
        dag.tick_schedule(base),
        vec![step(a, base), step(b, base), step(deep, a)],
        "every edge of the closure, producers first",
    );
    assert_eq!(
        dag.tick_schedule(a),
        vec![step(deep, a)],
        "a tick of an intermediate view runs only what it reaches",
    );
    assert!(dag.tick_schedule(deep).is_empty(), "a terminal view reaches nothing");
}

// ── The drivers ─────────────────────────────────────────────────────────────

/// One tick of the base drives the whole dependent closure through real compiled
/// plans: each view ingests what its own epoch produced, and a view over a view
/// sees its producer's output rather than the base delta.
#[test]
fn a_tick_drives_the_whole_closure() {
    let (mut engine, base, a, b, deep) = engine_with_fanout("evaluate");

    let delta = delta_for(&engine, base, &[(1, 1, 10), (2, 1, 20)]);
    drive(
        &mut LocalDrive(&mut engine),
        Drive::Tick { source: base, round: 1 },
        delta,
    )
    .unwrap();

    assert_eq!(live_weight(&engine, a), 2);
    assert_eq!(live_weight(&engine, b), 2);
    assert_eq!(
        live_weight(&engine, deep),
        2,
        "the second rung sees its producer's output, exactly once"
    );
}

/// A view two producers feed runs one epoch per producer, and the view reading
/// it sees the union of the two — not the last one alone.
#[test]
fn a_two_producer_view_feeds_its_reader_the_union() {
    let (mut engine, base) = engine_with_base("two_producers");
    let cols = view_cols();
    let a = register_identity_view(&mut engine, base, "va", &cols);
    let b = register_identity_view(&mut engine, base, "vb", &cols);
    let mut circuit = gnitz_wire::Circuit::default();
    let left = circuit.input_delta(a, gnitz_wire::ReadBound::None);
    let right = circuit.input_delta(b, gnitz_wire::ReadBound::None);
    let merged = circuit.union(left, right);
    circuit.sink(merged);
    let u = try_register_view(&mut engine, circuit, "vu", &cols, 0, 0).unwrap();
    let d = register_identity_view(&mut engine, u, "vd", &cols);

    let delta = delta_for(&engine, base, &[(1, 1, 10), (2, 1, 20)]);
    drive(
        &mut LocalDrive(&mut engine),
        Drive::Tick { source: base, round: 1 },
        delta,
    )
    .unwrap();

    assert_eq!(live_weight(&engine, u), 4, "both producers' epochs land");
    assert_eq!(live_weight(&engine, d), 4, "its reader sees both, once each");
}

/// A backfill drive runs the named view alone — not the source's closure, which
/// would double-count into the dependents the source already populated.
#[test]
fn backfill_chunk_runs_only_the_named_view() {
    let (mut engine, base) = engine_with_base("backfill");
    let cols = view_cols();
    let a = register_identity_view(&mut engine, base, "va", &cols);
    let b = register_identity_view(&mut engine, base, "vb", &cols);
    let backfill = |view| Drive::Backfill { view, source: base };

    let chunk = delta_for(&engine, base, &[(1, 1, 10)]);
    drive(&mut LocalDrive(&mut engine), backfill(a), chunk).unwrap();
    assert_eq!(live_weight(&engine, a), 1);
    assert_eq!(
        live_weight(&engine, b),
        0,
        "the source's other dependents are untouched"
    );

    let empty = Batch::empty_with_schema(&engine.registry.relation(base).map(Relation::schema).unwrap());
    drive(&mut LocalDrive(&mut engine), backfill(a), empty).unwrap();
    assert_eq!(live_weight(&engine, a), 1, "a chunk with no rows adds none");

    // A view the registry no longer holds is a fail-stop, not a silent skip.
    let empty = Batch::empty_with_schema(&engine.registry.relation(base).map(Relation::schema).unwrap());
    assert!(drive(&mut LocalDrive(&mut engine), backfill(999_999), empty).is_err());
}

// ── A side's output relay ───────────────────────────────────────────────────

/// Records the row count of every batch a relay sends, and relays it back.
struct Recorder<'a> {
    cat: &'a mut CatalogEngine,
    sent: Vec<usize>,
}

impl DriveHost for Recorder<'_> {
    fn parts(&mut self) -> (&mut DagEngine, &mut RelationRegistry) {
        (&mut self.cat.dag, &mut self.cat.registry)
    }

    fn exchange(&mut self, _view_id: u64, batch: Cow<'_, Batch>, _spec: Option<ScatterSpec<'_>>) -> Batch {
        self.sent.push(batch.len());
        batch.into_owned()
    }
}

/// A side whose output every worker holds whole never reaches the host: each
/// rank keeps its own share, and the ranks' shares partition the output. Any
/// other side's output runs a round.
#[test]
fn a_replica_sides_output_is_shared_without_a_round() {
    let rows: Vec<(u64, i64, i64)> = (1..=16).map(|k| (k, 1, k as i64 * 10)).collect();
    let mut shares: Vec<u64> = Vec::new();
    for rank in [0u32, 1] {
        let mut engine = CatalogEngine::open_master(&scratch_dir("dag_exec", &format!("relay_share_{rank}")), 2)
            .unwrap()
            .replay()
            .unwrap();
        let t = engine.create_table("public.t", &view_cols(), &[0]).unwrap();
        engine.registry.reconcile_child_dirs().unwrap();
        engine
            .open_stores(rank, gnitz_store::relation::Residency::Worker)
            .unwrap();

        let delta = delta_for(&engine, t, &rows);
        let relay = Relay {
            view_id: 99,
            elide: false,
            shard_cols: Box::from([0u32]),
        };
        let mut host = Recorder { cat: &mut engine, sent: Vec::new() };
        let share = relay.round(&mut host, Batch::clone(&delta), true).unwrap();
        assert!(host.sent.is_empty(), "rank {rank}: a replica's share runs no round");
        shares.extend((0..share.len()).map(|i| share.get_pk(i) as u64));

        relay.round(&mut host, delta, false).unwrap();
        assert_eq!(
            host.sent,
            vec![rows.len()],
            "rank {rank}: any other output runs a round"
        );
    }
    shares.sort_unstable();
    assert_eq!(
        shares,
        (1..=16).collect::<Vec<u64>>(),
        "the ranks' shares partition the output"
    );
}

// ── Per-epoch cost ──────────────────────────────────────────────────────────

/// One-row ticks per cost cell, and rows in the wide tick.
const BENCH_TICKS: u64 = 10_000;

/// Column records for a view whose output is `schema`.
fn cols_of(schema: &gnitz_store::schema::SchemaDescriptor) -> Vec<CatalogColumn> {
    (0..schema.num_columns())
        .map(|ci| {
            let c = schema.column(ci).expect("in range");
            ColumnDef::new(format!("c{ci}"), c.type_code, c.nullable).into()
        })
        .collect()
}

/// [`engine_with_fanout`] plus a `Unary` plan (GROUP BY) and a `Pair` plan
/// (UNION ALL of two exchanged scans) over the base.
fn engine_with_every_plan_shape(name: &str) -> (CatalogEngine, u64, u64) {
    let (mut engine, base, a, ..) = engine_with_fanout(name);
    let base_schema = engine.registry.relation(base).map(Relation::schema).unwrap();

    let (group, aggs) = ([1u32], [gnitz_wire::AggDescriptor::COUNT_STAR]);
    let grouped = gnitz_store::ops::ReducePlan::from_wire(&base_schema, &group, &aggs, false)
        .unwrap()
        .shape
        .output_schema;
    let mut circuit = gnitz_wire::Circuit::default();
    let scan = circuit.input_delta(base, gnitz_wire::ReadBound::None);
    let reduced = circuit.reduce_multi(scan, &group, &aggs, false);
    circuit.sink(reduced);
    try_register_view(&mut engine, circuit, "vgroup", &cols_of(&grouped), 0, 0).unwrap();

    let mut circuit = gnitz_wire::Circuit::default();
    let sides = [0, 1].map(|_| {
        let scan = circuit.input_delta(base, gnitz_wire::ReadBound::None);
        circuit.shard(scan, &[0])
    });
    let merged = circuit.union(sides[0], sides[1]);
    circuit.sink(merged);
    try_register_view(&mut engine, circuit, "vunion", &view_cols(), 0, 0).unwrap();
    (engine, base, a)
}

/// One tick of `source` over `delta`.
fn tick(engine: &mut CatalogEngine, source: u64, round: u64, delta: Batch) {
    drive(&mut LocalDrive(engine), Drive::Tick { source, round }, delta).unwrap();
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

    let (mut engine, base, a) = engine_with_every_plan_shape("tick_bench_shapes");
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
    assert_eq!(live_weight(&engine, a) as u64, BENCH_TICKS + 1, "every tick landed");
    println!(
        "one-row ticks, every plan shape  {:>10} instr/tick",
        instr / BENCH_TICKS
    );

    let (mut engine, base) = engine_with_base("tick_bench_fanout");
    let cols = view_cols();
    for r in 0..8 {
        register_identity_view(&mut engine, base, &format!("v{r}"), &cols);
    }
    let warm = delta_for(&engine, base, &[(0, 1, 0)]);
    tick(&mut engine, base, 1, warm);
    let rows: Vec<(u64, i64, i64)> = (1..=BENCH_TICKS).map(|i| (i, 1, i as i64)).collect();
    let delta = delta_for(&engine, base, &rows);
    let (copies, clone_instr) = counter.measure(|| (0..7).map(|_| Batch::clone(&delta)).collect::<Vec<_>>());
    drop(copies);
    let ((), instr) = counter.measure(|| tick(&mut engine, base, 2, delta));
    let last = engine.dag.dependents_of(base).last().copied().unwrap();
    assert_eq!(live_weight(&engine, last) as u64, BENCH_TICKS + 1, "the tick landed");
    println!("{BENCH_TICKS}-row tick, 8 readers        {instr:>10} instr, of which 7 copies {clone_instr}");
}
