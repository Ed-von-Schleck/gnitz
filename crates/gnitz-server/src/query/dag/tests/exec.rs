//! The tick and backfill drivers.

use super::*;
use crate::catalog::CatalogEngine;
use crate::test_support::{
    col_def, cols_of, make_batch, make_schema_u64_i64, push_and_tick, register_identity_view, scan_all, scan_keyed,
    scratch_dir, table_engine, try_register_view, two_term_join_circuit, u64_i64_cols, zset_of, EchoDrive, RowKey,
};
use gnitz_wire::{AggDescriptor, Circuit, GroupAggs, ReadBound, TypeCode};
use gnitz_zset::stream::ReducePlan;
use std::collections::HashMap;

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

/// A base table `(id U64 PK, v I64)` in a fresh catalog plus [`Views`].
pub(super) fn engine_with_views(name: &str) -> (CatalogEngine, u64, Views) {
    let cols = u64_i64_cols();
    let (mut engine, base) = table_engine("dag_exec", name, 1, &cols);
    let mut circuit = Circuit::default();
    let sides = [0, 1].map(|_| {
        let scan = circuit.input_delta(base, ReadBound::None);
        circuit.shard(scan)
    });
    circuit.union(sides[0], sides[1]);
    let twice = try_register_view(&mut engine, circuit, "twice", &cols, 0, 0).unwrap();
    let once = register_identity_view(&mut engine, base, "once", &cols);
    let deep = register_identity_view(&mut engine, twice, "deep", &cols);
    let mut circuit = Circuit::default();
    let left = circuit.input_delta(twice, ReadBound::None);
    let right = circuit.input_delta(once, ReadBound::None);
    circuit.union(left, right);
    let union = try_register_view(&mut engine, circuit, "union", &cols, 0, 0).unwrap();
    let over_union = register_identity_view(&mut engine, union, "over_union", &cols);
    (engine, base, Views { twice, once, deep, union, over_union })
}

/// `(pk, weight, payload)` rows of a base table `(id U64 PK, v I64)`.
pub(super) fn base_rows(rows: &[(u64, i64, i64)]) -> Batch {
    make_batch(&make_schema_u64_i64(), rows)
}

/// A global `COUNT(*)` over `source`, a relation of the base's columns,
/// registered as `name`; and its plan.
pub(super) fn register_count(engine: &mut CatalogEngine, source: u64, name: &str) -> (u64, ReducePlan) {
    let aggs = [AggDescriptor::COUNT_STAR];
    let plan = ReducePlan::from_wire(&make_schema_u64_i64(), &[], &aggs, true).unwrap();
    let mut circuit = Circuit::default();
    let scan = circuit.input_delta(source, ReadBound::None);
    circuit.reduce(scan, GroupAggs { group_cols: vec![], aggs: aggs.to_vec() });
    let view = try_register_view(engine, circuit, name, &cols_of(plan.output_schema()), 0, 0).unwrap();
    (view, plan)
}

/// The Z-set a relation holds.
fn held(engine: &mut CatalogEngine, tid: u64) -> HashMap<RowKey, i64> {
    let rows = scan_all(engine, tid);
    zset_of(&rows, rows.schema())
}

/// `rows` of the base as a Z-set, every weight scaled by `k`.
fn times(rows: &[(u64, i64, i64)], k: i64) -> HashMap<RowKey, i64> {
    let mut z = zset_of(&base_rows(rows), &make_schema_u64_i64());
    z.values_mut().for_each(|w| *w *= k);
    z
}

// ── Ticks ───────────────────────────────────────────────────────────────────

/// A tick of the base drives the whole dependent closure through real compiled
/// plans — a side per scan of the source, a view over a view reading its
/// producer's output, and a view two producers feed passing its reader both —
/// for an insert and for the retraction a delete pushes.
#[test]
fn a_tick_drives_the_whole_closure() {
    let (mut engine, base, views) = engine_with_views("evaluate");
    let ks = [
        (views.twice, 2),
        (views.once, 1),
        (views.deep, 2),
        (views.union, 3),
        (views.over_union, 3),
    ];

    push_and_tick(&mut engine, base, base_rows(&[(1, 1, 10), (2, 1, 20), (3, 1, 30)]));
    push_and_tick(&mut engine, base, base_rows(&[(2, -1, 20)]));
    for (vid, k) in ks {
        assert_eq!(
            held(&mut engine, vid),
            times(&[(1, 1, 10), (3, 1, 30)], k),
            "view {vid}"
        );
    }
}

/// A tick that brings this worker no row still runs every exchange round of
/// every step, an empty producer's readers' among them: the other workers'
/// rounds wait on it.
#[test]
fn an_empty_tick_runs_every_round() {
    let (mut engine, base) = table_engine("dag_exec", "empty_rounds", 4, &u64_i64_cols());
    let once = register_identity_view(&mut engine, base, "once", &u64_i64_cols());
    for (source, name) in [(base, "count"), (once, "count_once")] {
        register_count(&mut engine, source, name);
    }
    let mut host = EchoDrive::new(&mut engine);
    tick(&mut host, base, 1).unwrap();
    assert_eq!(host.rounds, 2, "one round per count, none for the identity");
}

/// A view's backfill mints the ground row a global aggregate owes; a tick that
/// brings this process no row lands nothing, in that view or any other; a row
/// moves the count to what a backfill over it holds.
#[test]
fn an_empty_tick_lands_only_an_owed_ground_row() {
    let (mut engine, base) = table_engine("dag_exec", "empty_tick", 1, &u64_i64_cols());
    let once = register_identity_view(&mut engine, base, "once", &u64_i64_cols());
    let (count, plan) = register_count(&mut engine, base, "count");
    let ground = plan.ground_row().unwrap();
    let ground = zset_of(ground, ground.schema());

    tick(&mut engine, base, 1).unwrap();
    assert!(held(&mut engine, count).is_empty(), "no tick mints the ground row");
    backfill(&mut engine, count).unwrap();
    for round in 0..=2 {
        assert_eq!(held(&mut engine, count), ground, "round {round}: the ground row, once");
        assert!(held(&mut engine, once).is_empty(), "round {round}");
        tick(&mut engine, base, 1).unwrap();
    }

    push_and_tick(&mut engine, base, base_rows(&[(1, 1, 10)]));
    assert_eq!(held(&mut engine, once), times(&[(1, 1, 10)], 1));
    let (oracle, _) = register_count(&mut engine, base, "oracle");
    backfill(&mut engine, oracle).unwrap();
    assert_eq!(held(&mut engine, count), held(&mut engine, oracle));
    assert_ne!(held(&mut engine, count), ground);
}

// ── Backfills ───────────────────────────────────────────────────────────────

/// A backfill runs the named view alone — not the source's closure, which
/// would double-count into the dependents the source already populated — and
/// not the view's own dependents, which backfill from its store afterwards.
#[test]
fn a_backfill_runs_only_the_named_view() {
    let (mut engine, base, views) = engine_with_views("backfill");
    let rows = [(1, 1, 10)];

    engine.registry.ingest(base, base_rows(&rows)).unwrap();
    backfill(&mut engine, views.twice).unwrap();
    assert_eq!(held(&mut engine, views.twice), times(&rows, 2));
    for vid in views.all().into_iter().filter(|&v| v != views.twice) {
        assert!(held(&mut engine, vid).is_empty(), "view {vid}");
    }
}

/// A backfill feeds a join its sources one after the other, each joined against
/// the ones fed before it, one row a chunk: the view holds every match once.
/// Keyed on a non-PK column the join reads its own traces; keyed on the PK it
/// reads the tables' stores, where a source not yet fed must read as empty.
#[test]
fn a_backfill_of_a_join_feeds_each_source_against_the_ones_before_it() {
    let a_rows = [(1, 1, 10), (2, 1, 20), (3, 1, 10)];
    let b_rows = [(2, 1, 10), (3, 1, 30), (4, 1, 20), (5, 1, 10)];
    // `(key, a.id, b.id)` of each match.
    let on_k: &[(u64, u64, u64)] = &[(10, 1, 2), (10, 1, 5), (10, 3, 2), (10, 3, 5), (20, 2, 4)];
    let on_id: &[(u64, u64, u64)] = &[(2, 2, 2), (3, 3, 3)];
    for (key, want) in [(1, on_k), (0, on_id)] {
        let mut engine = CatalogEngine::open(&scratch_dir("dag_exec", &format!("backfill_join_{key}")), 1).unwrap();
        let cols = [col_def("id", TypeCode::U64), col_def("k", TypeCode::U64)];
        let a = engine.create_table("public.a", &cols, &[0]).unwrap();
        let b = engine.create_table("public.b", &cols, &[0]).unwrap();
        let circuit = match key {
            1 => two_term_join_circuit(a, b, TypeCode::U64),
            _ => {
                let mut circuit = Circuit::default();
                let deltas = [a, b].map(|source| scan_keyed(&mut circuit, source, &[(0, TypeCode::U64)]));
                circuit.join(deltas, deltas, gnitz_wire::JoinKind::Equi);
                circuit
            }
        };
        let join_cols = ["k", "a_id", "b_id"].map(|name| col_def(name, TypeCode::U64));
        let view = try_register_view(&mut engine, circuit, "join", &join_cols, 0, 0).unwrap();
        for (tid, rows) in [(a, &a_rows[..]), (b, &b_rows)] {
            let rows = make_batch(&engine.registry.relation_or_err(tid).unwrap().schema(), rows);
            engine.registry.ingest(tid, rows).unwrap();
        }
        engine.registry.set_scan_chunk_rows(1);

        backfill(&mut engine, view).unwrap();

        let schema = engine.registry.relation_or_err(view).unwrap().schema();
        let mut bb = gnitz_zset::repr::BatchBuilder::new(&schema);
        for &(k, a_id, b_id) in want {
            bb.begin_row(k as u128, 1);
            bb.put_u64(a_id);
            bb.put_u64(b_id);
            bb.end_row();
        }
        assert_eq!(
            held(&mut engine, view),
            zset_of(&bb.finish(), &schema),
            "keyed on column {key}"
        );
    }
}

/// The ground row is minted once per copy of the result: on each process holding
/// the view's whole input, else on the one worker the empty group's exchange
/// sends every row to.
#[test]
fn the_ground_row_is_minted_where_the_whole_input_arrives() {
    let schema = make_schema_u64_i64();
    let probe = make_batch(&schema, &[(1, 1, 1)]);
    let group = ScatterPlan::group(&schema, &[]).unwrap();
    let (mints, receives): (Vec<bool>, Vec<bool>) = (0..4)
        .map(|rank| {
            let slot = Slot::new(rank, 4);
            (mints_ground_row(slot, false), !group.share(&probe, slot).is_empty())
        })
        .unzip();
    assert_eq!(mints, receives);
    assert_eq!(mints.iter().filter(|&&m| m).count(), 1, "one owner");
    assert!(mints_ground_row(Slot::SOLO, false), "one worker");
}
