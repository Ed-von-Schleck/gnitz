//! The tick schedule and the one driver over it.

use super::*;
use crate::catalog::{CatalogColumn, CatalogEngine};
use crate::test_support::{
    col_def, cols_of, left_join_engine, make_batch, net_weight, register_identity_view, scan_all, scratch_dir,
    try_register_view, zset_of, LocalDrive, RowKey,
};
use gnitz_store::relation::Relation;
use gnitz_wire::TypeCode;
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

/// One tick of `source` on `host` over `delta`, in place of what a seal would
/// answer; `None` where it brings this process no row.
pub(super) fn drive(host: &mut impl DriveHost, source: u64, delta: Option<Batch>) {
    let (dag, registry) = host.parts();
    let schedule = dag.tick_schedule(source);
    let delta = match delta {
        Some(delta) => delta,
        None => Batch::empty_with_schema(&registry.relation_or_err(source).unwrap().schema()),
    };
    run_schedule(host, source, &schedule, 1, delta).unwrap();
}

/// [`drive`] on a host that exchanges nothing.
pub(super) fn tick_over(engine: &mut CatalogEngine, source: u64, delta: impl Into<Option<Batch>>) {
    drive(&mut LocalDrive(engine), source, delta.into());
}

// ── The schedule ────────────────────────────────────────────────────────────

/// The schedule names one step per dependency edge out of the source's forward
/// closure, ordered by view id — so a producer always precedes the steps it
/// feeds — and leaves out the views awaiting a rebuild. Each step says whether
/// it is its producer's last reader and whether a later one reads its view, of
/// the steps the schedule runs.
#[test]
fn the_schedule_names_every_edge_of_the_closure_in_id_order() {
    let edges = |dag: &mut DagEngine| -> Vec<(u64, u64)> {
        dag.tick_schedule(1).iter().map(|s| (s.view, s.producer)).collect()
    };
    let mut dag = DagEngine::default();
    for (view, sources) in [(2, &[1][..]), (3, &[1]), (4, &[2, 3]), (5, &[4]), (7, &[6])] {
        dag.dep.link(view, sources.iter().copied());
    }

    let every = [(2, 1), (3, 1), (4, 2), (4, 3), (5, 4)];
    assert_eq!(edges(&mut dag), every);
    let flags = |dag: &mut DagEngine| -> Vec<(bool, bool)> {
        dag.tick_schedule(1).iter().map(|s| (s.last, s.needed)).collect()
    };
    assert_eq!(
        flags(&mut dag),
        [(false, true), (true, true), (true, true), (true, true), (true, false)],
        "(last, needed) of each"
    );

    // A rebuild set is closed under dependents.
    dag.set_rebuild([2, 4, 5].into_iter().collect());
    assert_eq!(edges(&mut dag), [(3, 1)]);
    assert_eq!(flags(&mut dag), [(true, false)], "of the steps it runs");
    dag.rebuild.remove(&2);
    assert_eq!(edges(&mut dag), [(2, 1), (3, 1)]);

    // A view linked or dropped after a schedule was read moves that schedule.
    dag.take_rebuild();
    dag.dep.link(8, [5].into_iter());
    assert_eq!(edges(&mut dag)[..5], every);
    assert_eq!(edges(&mut dag)[5..], [(8, 5)]);
    dag.dep.unlink(4);
    assert_eq!(edges(&mut dag), [(2, 1), (3, 1)]);
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
    tick_over(&mut engine, base, delta);

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

/// A backfill runs the named view alone — not the source's closure, which
/// would double-count into the dependents the source already populated — and
/// not the view's own dependents, which backfill from its store afterwards.
#[test]
fn backfill_chunk_runs_only_the_named_view() {
    let (mut engine, base, views) = engine_with_views("backfill");
    let rows = [(1, 1, 10)];

    let held_rows = delta_for(&engine, base, &rows);
    engine.registry.ingest(base, held_rows).unwrap();
    backfill(&mut LocalDrive(&mut engine), views.twice).unwrap();
    assert_eq!(held(&mut engine, views.twice), times(&engine, base, &rows, 2));
    for vid in views.all().into_iter().filter(|&v| v != views.twice) {
        assert!(held(&mut engine, vid).is_empty(), "view {vid}");
    }

    backfill(&mut LocalDrive(&mut engine), 999_999).unwrap_err();
}

/// A backfill feeds a join its sources one after the other, each joined against
/// the ones fed before it: the view holds every match once, at the product of
/// its rows' weights.
#[test]
fn a_backfill_of_a_join_feeds_each_source_against_the_ones_before_it() {
    let mut engine = CatalogEngine::open(&scratch_dir("dag_exec", "backfill_join"), 1).unwrap();
    let cols = [col_def("id", TypeCode::U64), col_def("k", TypeCode::U64)];
    let a = engine.create_table("public.a", &cols, &[0]).unwrap();
    let b = engine.create_table("public.b", &cols, &[0]).unwrap();
    let join_cols = [
        col_def("k", TypeCode::U64),
        col_def("a_id", TypeCode::U64),
        col_def("b_id", TypeCode::U64),
    ];
    let circuit = crate::test_support::two_term_join_circuit(a, b, TypeCode::U64);
    let view = try_register_view(&mut engine, circuit, "join", &join_cols, 0, 0).unwrap();
    for (tid, rows) in [
        (a, &[(1, 1, 10), (2, 1, 20), (3, 1, 10)][..]),
        (b, &[(7, 1, 10), (8, 1, 30), (9, 1, 20), (10, 1, 10)]),
    ] {
        let rows = delta_for(&engine, tid, rows);
        engine.registry.ingest(tid, rows).unwrap();
    }
    // One row a chunk: a source fed in several epochs is joined once all the same.
    engine.registry.set_scan_chunk_rows(1);

    backfill(&mut LocalDrive(&mut engine), view).unwrap();

    let want = rows_of(
        &engine,
        view,
        1,
        &[
            (10, &[Some(1), Some(7)]),
            (10, &[Some(1), Some(10)]),
            (10, &[Some(3), Some(7)]),
            (10, &[Some(3), Some(10)]),
            (20, &[Some(2), Some(9)]),
        ],
    );
    assert_eq!(held(&mut engine, view), zset_of(&want, want.schema()));
}

/// A backfill opens the bound its view's circuit recorded — a walk of the index
/// while the catalog holds one, a full scan once it is dropped — and an
/// unbounded view's opens a full scan.
#[test]
fn a_backfill_opens_the_bound_its_view_recorded() {
    use gnitz_wire::{key_image, KeyRange, PkColList, ReadBound};
    use gnitz_zset::repr::SourceCursor;
    let (mut engine, base) = engine_with_base("backfill_bound");
    let held_rows: Vec<(u64, i64, i64)> = (0..200).map(|id| (id, 1, id as i64 * 10)).collect();
    let held_rows = delta_for(&engine, base, &held_rows);
    engine.registry.ingest(base, held_rows).unwrap();
    engine.create_index("public.base", &["v"], false).unwrap();
    let img = |v: i64| key_image(TypeCode::I64, v as u64 as u128);
    let range = KeyRange::new(
        PkColList::from_slice(&[1]),
        &[],
        gnitz_wire::Cut::before(img(500)),
        gnitz_wire::Cut::before(img(600)),
    );
    let identity = |bound| crate::test_support::identity_circuit(base, bound);
    let bounded = try_register_view(
        &mut engine,
        identity(ReadBound::Range(range)),
        "bounded",
        &view_cols(),
        0,
        0,
    );
    let bounded = bounded.unwrap();
    let whole = try_register_view(&mut engine, identity(ReadBound::None), "whole", &view_cols(), 0, 0).unwrap();
    // What `backfill` opens for `view`'s one source.
    let open = |engine: &CatalogEngine, view: u64| {
        let bound = engine.dag.views[&view].code.sources[&base].1.clone();
        engine.registry.open_bound(base, bound, Cut::Sealed).unwrap().0
    };

    assert!(matches!(open(&engine, bounded), SourceCursor::Bounded(_)));
    assert!(matches!(open(&engine, whole), SourceCursor::Full(_)));
    backfill(&mut LocalDrive(&mut engine), bounded).unwrap();
    let want: Vec<(u64, i64, i64)> = (50..60).map(|id| (id, 1, id as i64 * 10)).collect();
    assert_eq!(held(&mut engine, bounded), times(&engine, base, &want, 1));

    engine.drop_index("public__base__idx_v").unwrap();
    assert!(matches!(open(&engine, bounded), SourceCursor::Full(_)));
}

/// A view's backfill mints the ground row a global aggregate owes; a tick that
/// brings this process no row lands nothing, in that view or any other.
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

    tick_over(&mut engine, base, None);
    assert_eq!(net_weight(&engine, count), 0, "no tick mints the ground row");
    backfill(&mut LocalDrive(&mut engine), count).unwrap();
    assert_eq!(net_weight(&engine, count), 1, "the ground row");
    for round in 1..=2 {
        tick_over(&mut engine, base, None);
        assert_eq!(net_weight(&engine, count), 1, "round {round}: the ground row, once");
        assert!(held(&mut engine, once).is_empty(), "round {round}");
    }

    let rows = [(1, 1, 10)];
    let delta = delta_for(&engine, base, &rows);
    tick_over(&mut engine, base, delta);
    assert_eq!(held(&mut engine, once), times(&engine, base, &rows, 1));
    assert_eq!(net_weight(&engine, count), 1, "the count moved, its row count did not");
}

/// The ground row is minted once per copy of the result: on each process holding
/// the view's whole input, else on the one worker the empty group's exchange
/// sends every row to.
#[test]
fn the_ground_row_is_minted_where_the_whole_input_arrives() {
    let schema = crate::test_support::make_schema_u64_i64();
    let keyed = Placement::full_pk(&schema);
    let probe = make_batch(&schema, &[(1, 1, 1)]);
    let group = ScatterPlan::group(&schema, &[]).unwrap();
    let (mints, receives): (Vec<bool>, Vec<bool>) = (0..4)
        .map(|rank| {
            let slot = Slot::new(rank, 4);
            assert!(
                mints_ground_row(slot, Placement::Replicated),
                "a replicated view, {slot:?}"
            );
            (mints_ground_row(slot, keyed), !group.share(&probe, slot).is_empty())
        })
        .unzip();
    assert_eq!(mints, receives);
    assert_eq!(mints.iter().filter(|&&m| m).count(), 1, "one owner");
    assert!(mints_ground_row(Slot::SOLO, keyed), "one worker");
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

/// Push `rows` into table `tid` and tick it.
fn push_and_tick(engine: &mut CatalogEngine, tid: u64, rows: Batch) {
    engine.ingest_unticked(tid, rows).unwrap();
    tick(&mut LocalDrive(engine), tid, 1).unwrap();
}

/// Over a nullable key the preserved side's two re-keys differ in exactly the
/// NULL-keyed rows, which the join drops and the null-fill owes: the view holds
/// such a row once, null-filled, beside a matched row and an unmatched one.
#[test]
fn a_left_join_null_fills_its_null_keyed_preserved_row() {
    let (mut engine, [a, b], view) = left_join_engine(&scratch_dir("dag_exec", "left_join_null_key"), 2);
    let delta = rows_of(&engine, b, 1, &[(1, &[Some(7), Some(70)])]);
    push_and_tick(&mut engine, b, delta);
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
    push_and_tick(&mut engine, a, delta);

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
