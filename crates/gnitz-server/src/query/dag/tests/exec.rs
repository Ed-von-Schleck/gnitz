//! The tick schedule and the one driver over it.

use super::*;
use crate::catalog::{CatalogColumn, CatalogEngine};
use crate::test_support::{
    col_def, cols_of, left_join_engine, make_batch, net_weight, register_identity_view, scan_all, scratch_dir,
    try_register_view, zset_of, LocalDrive, RowKey,
};
use gnitz_store::relation::Relation;
use gnitz_wire::TypeCode;
use gnitz_zset::schema::Slot;
use std::collections::HashMap;

pub(super) fn view_cols() -> Vec<CatalogColumn> {
    vec![col_def("id", TypeCode::U64), col_def("v", TypeCode::I64)]
}

/// A base table `(id U64 PK, v I64)` in a fresh catalog.
pub(super) fn engine_with_base(name: &str) -> (CatalogEngine, u64) {
    let mut engine = CatalogEngine::open(&scratch_dir("dag_exec", name), 1).unwrap();
    let base = engine.create_table("public.base", &view_cols(), &[0]).unwrap();
    (engine, base)
}

/// The views of [`engine_with_views`], each holding a multiple of the base's
/// delta so that reading the wrong input shows in its weights.
pub(super) struct Views {
    /// `base UNION ALL base` through two sharded sides: 2×.
    twice: u64,
    /// Identity over the base: 1×.
    once: u64,
    /// Identity over `twice`: 2×, where reading the base would give 1×.
    deep: u64,
    /// `twice UNION ALL once`, a view two producers feed: 3×.
    union: u64,
    /// Identity over `union`: 3×.
    pub(super) over_union: u64,
}

impl Views {
    fn all(&self) -> [u64; 5] {
        [self.twice, self.once, self.deep, self.union, self.over_union]
    }
}

/// [`engine_with_base`] plus [`Views`].
pub(super) fn engine_with_views(name: &str) -> (CatalogEngine, u64, Views) {
    let (mut engine, base) = engine_with_base(name);
    let cols = view_cols();
    let mut circuit = gnitz_wire::Circuit::default();
    let sides = [0, 1].map(|_| {
        let scan = circuit.input_delta(base, gnitz_wire::ReadBound::None);
        circuit.shard(scan)
    });
    circuit.union(sides[0], sides[1]);
    let twice = try_register_view(&mut engine, circuit, "twice", &cols, 0, 0).unwrap();
    let once = register_identity_view(&mut engine, base, "once", &cols);
    let deep = register_identity_view(&mut engine, twice, "deep", &cols);
    let mut circuit = gnitz_wire::Circuit::default();
    let left = circuit.input_delta(twice, gnitz_wire::ReadBound::None);
    let right = circuit.input_delta(once, gnitz_wire::ReadBound::None);
    circuit.union(left, right);
    let union = try_register_view(&mut engine, circuit, "union", &cols, 0, 0).unwrap();
    let over_union = register_identity_view(&mut engine, union, "over_union", &cols);
    (engine, base, Views { twice, once, deep, union, over_union })
}

/// `(pk, weight, payload)` rows in `tid`'s own registered schema.
pub(super) fn delta_for(engine: &CatalogEngine, tid: u64, rows: &[(u64, i64, i64)]) -> Batch {
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

/// One tick of `source` over `delta`; `None` where it brings this process no row.
pub(super) fn tick(engine: &mut CatalogEngine, source: u64, delta: impl Into<Option<Batch>>) {
    drive(&mut LocalDrive(engine), Drive::Tick { source, round: 1 }, delta.into()).unwrap();
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
        dag.dep.link(view, sources.iter().copied());
    }

    let every = [step(2, 1), step(3, 1), step(4, 2), step(4, 3), step(5, 4)];
    assert_eq!(*dag.tick_schedule(1), every);

    // A rebuild set is closed under dependents.
    dag.set_rebuild([2, 4, 5].into_iter().collect());
    assert_eq!(*dag.tick_schedule(1), [step(3, 1)]);
    dag.rebuild.remove(&2);
    assert_eq!(*dag.tick_schedule(1), [step(2, 1), step(3, 1)]);

    // A view linked or dropped after a schedule was read moves that schedule.
    dag.take_rebuild();
    dag.dep.link(8, [5].into_iter());
    assert_eq!(dag.tick_schedule(1)[..5], every);
    assert_eq!(dag.tick_schedule(1)[5..], [step(8, 5)]);
    dag.dep.unlink(4);
    assert_eq!(*dag.tick_schedule(1), [step(2, 1), step(3, 1)]);
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
    tick(&mut engine, base, delta);

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
    drive(&mut LocalDrive(&mut engine), backfill(views.twice), Some(chunk)).unwrap();
    drive(&mut LocalDrive(&mut engine), backfill(views.twice), None).unwrap();
    assert_eq!(held(&mut engine, views.twice), times(&engine, base, &rows, 2));
    for vid in views.all().into_iter().filter(|&v| v != views.twice) {
        assert!(held(&mut engine, vid).is_empty(), "view {vid}");
    }

    let err = drive(&mut LocalDrive(&mut engine), backfill(999_999), None).unwrap_err();
    assert!(err.contains("is not registered"), "{err}");
}

/// A tick that brings this process no row still mints the ground row a global
/// aggregate owes, once; past that it lands nothing, in that view or any other.
#[test]
fn an_empty_tick_lands_only_an_owed_ground_row() {
    let (mut engine, base) = engine_with_base("empty_tick");
    let once = register_identity_view(&mut engine, base, "once", &view_cols());
    let base_schema = engine.registry.relation(base).map(Relation::schema).unwrap();
    let aggs = [gnitz_wire::AggDescriptor::COUNT_STAR];
    let counted = *gnitz_zset::stream::ReducePlan::from_wire(&base_schema, &[], &aggs, true)
        .unwrap()
        .output_schema();
    let mut circuit = gnitz_wire::Circuit::default();
    let scan = circuit.input_delta(base, gnitz_wire::ReadBound::None);
    circuit.reduce_multi(scan, &[], &aggs);
    let count = try_register_view(&mut engine, circuit, "count", &cols_of(&counted), 0, 0).unwrap();

    for round in 1..=2 {
        tick(&mut engine, base, None);
        assert_eq!(net_weight(&engine, count), 1, "round {round}: the ground row, once");
        assert!(held(&mut engine, once).is_empty(), "round {round}");
    }

    let rows = [(1, 1, 10)];
    let delta = delta_for(&engine, base, &rows);
    tick(&mut engine, base, delta);
    assert_eq!(held(&mut engine, once), times(&engine, base, &rows, 1));
    assert_eq!(net_weight(&engine, count), 1, "the count moved, its row count did not");
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

// ── A LEFT JOIN's two re-keys ───────────────────────────────────────────────

/// `rows` of `tid`'s schema at weight `weight`, each a PK and its payload cells.
fn rows_of(engine: &CatalogEngine, tid: u64, weight: i64, rows: &[(u64, &[Option<u64>])]) -> Batch {
    let schema = engine.registry.relation(tid).map(Relation::schema).unwrap();
    let mut bb = gnitz_zset::repr::BatchBuilder::new(&schema);
    for &(pk, cells) in rows {
        bb.begin_row(pk as u128, weight);
        for &cell in cells {
            bb.put_opt_int(cell.map(u128::from));
        }
        bb.end_row();
    }
    bb.finish()
}

/// Push `rows` into table `tid`, and answer what the tick of it is driven over.
fn pushed(engine: &mut CatalogEngine, tid: u64, rows: Batch) -> Option<Batch> {
    engine.ingest_unticked(tid, rows).unwrap();
    engine.registry.seal(tid).unwrap()
}

/// Over a nullable key the preserved side's two re-keys differ in exactly the
/// NULL-keyed rows, which the join drops and the null-fill owes: the view holds
/// such a row once, null-filled, beside a matched row and an unmatched one.
#[test]
fn a_left_join_null_fills_its_null_keyed_preserved_row() {
    let (mut engine, [a, b], view) = left_join_engine(&scratch_dir("dag_exec", "left_join_null_key"), 2);
    let delta = rows_of(&engine, b, 1, &[(1, &[Some(7), Some(70)])]);
    let delta = pushed(&mut engine, b, delta);
    tick(&mut engine, b, delta);
    let delta = rows_of(
        &engine,
        a,
        1,
        &[
            (1, &[Some(10), Some(7)]),
            (2, &[Some(20), None]),
            (3, &[Some(30), Some(9)]),
        ],
    );
    let delta = pushed(&mut engine, a, delta);
    tick(&mut engine, a, delta);

    let want = rows_of(
        &engine,
        view,
        1,
        &[
            // A NULL key packs as zero.
            (0, &[Some(2), Some(20), None, None]),
            (7, &[Some(1), Some(10), Some(7), Some(70)]),
            (9, &[Some(3), Some(30), Some(9), None]),
        ],
    );
    assert_eq!(held(&mut engine, view), zset_of(&want, want.schema()));
}
