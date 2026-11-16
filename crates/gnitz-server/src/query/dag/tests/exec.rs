//! The tick and backfill drivers.

use super::*;
use crate::catalog::CatalogEngine;
use crate::test_support::{
    EchoDrive, RowKey, chunked, col_def, empty_catalog, empty_catalog_with, make_batch, make_schema_u64_i64,
    push_and_tick, register_count_of, register_identity_view, scan_all, scan_keyed, table_engine, try_register_view,
    two_term_join_circuit, u64_i64_cols, zset_of,
};
use gnitz_store::relation::PkRule;
use gnitz_wire::{Circuit, ReadBound, TypeCode};
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
pub(super) fn engine_with_views() -> (tempfile::TempDir, CatalogEngine, u64, Views) {
    let cols = u64_i64_cols();
    let (dir, mut engine, base) = table_engine(1, &cols);
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
    (dir, engine, base, Views { twice, once, deep, union, over_union })
}

/// `(pk, weight, payload)` rows of a base table `(id U64 PK, v I64)`.
pub(super) fn base_rows(rows: &[(u64, i64, i64)]) -> Batch {
    make_batch(&make_schema_u64_i64(), rows)
}

/// `COUNT(*)` over `source`, a relation of the base's columns, grouped by
/// `group_cols`, registered as `name`; and its plan.
pub(super) fn register_count(
    engine: &mut CatalogEngine,
    source: u64,
    group_cols: &[u32],
    name: &str,
) -> (u64, ReducePlan) {
    register_count_of(engine, source, &make_schema_u64_i64(), group_cols, name)
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
    let (_dir, mut engine, base, views) = engine_with_views();
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
    let (_dir, mut engine, base) = table_engine(4, &u64_i64_cols());
    let once = register_identity_view(&mut engine, base, "once", &u64_i64_cols());
    for (source, name) in [(base, "count"), (once, "count_once")] {
        register_count(&mut engine, source, &[], name);
    }
    let mut host = EchoDrive::new(&mut engine);
    tick(&mut host, base, 1).unwrap();
    assert_eq!(host.rounds, 2, "one round per count, none for the identity");
}

/// A tick's rounds share one wait per wave: every step whose sources are done
/// runs to its round, and a view over one still at a round waits for the next
/// wave. Each view still lands what its own rounds give it, as a backfill
/// over the same rows does.
#[test]
fn a_ticks_independent_rounds_share_one_wait() {
    let (_dir, mut engine, base) = table_engine(4, &u64_i64_cols());
    let once = register_identity_view(&mut engine, base, "once", &u64_i64_cols());
    let (count_v, plan_v) = register_count(&mut engine, base, &[1], "count_v");
    let (count_once, _) = register_count(&mut engine, once, &[1], "count_once");
    let counts = *plan_v.output_schema();
    let (total, _) = register_count_of(&mut engine, count_v, &counts, &[], "total");
    let rows = [(1, 1, 10), (2, 1, 20), (3, 1, 10)];
    engine.ingest_unticked(base, base_rows(&rows), PkRule::Enforce).unwrap();
    let mut host = EchoDrive::new(&mut engine);
    tick(&mut host, base, 1).unwrap();
    // `count_v` and `count_once`, behind `once`, which runs no round, stand at
    // their rounds together; `total` reads `count_v`.
    assert_eq!((host.rounds, host.waits), (3, 2));
    let oracles = [
        (count_v, base, None),
        (count_once, once, None),
        (total, count_v, Some(counts)),
    ];
    for (view, source, schema) in oracles {
        let name = format!("oracle_{view}");
        let (oracle, _) = match schema {
            None => register_count(&mut engine, source, &[1], &name),
            Some(schema) => register_count_of(&mut engine, source, &schema, &[], &name),
        };
        backfill(&mut engine, oracle).unwrap();
        let want = held(&mut engine, oracle);
        assert!(!want.is_empty());
        assert_eq!(held(&mut engine, view), want, "view {view}");
    }
}

/// `base ⋈ COUNT(*) GROUP BY v` on `v`: `(v, id, count)`. `base` is relayed by
/// `v`; a count is already placed by `v`, its PK, so its side runs no round.
fn join_base_with_count(base: u64, counts: u64) -> Circuit {
    use gnitz_wire::NullKeys;
    let mut circuit = Circuit::default();
    // `(source, key column, payload column)`: a count's `v` is its PK.
    let deltas = [(base, 1, 0), (counts, 0, 1)].map(|(source, key, payload)| {
        let key = [(key, TypeCode::I64)];
        let scan = circuit.input_scattered(source, ReadBound::None, Some(key.to_vec()));
        circuit.map_reindex(scan, &key, &[payload], NullKeys::Keep)
    });
    circuit.join(deltas, deltas, gnitz_wire::JoinKind::Equi);
    circuit
}

/// A view scanning a table and a view over that table relays the table's delta
/// in the same wave as the other view's round: only the view's own step and its
/// producer hold a step back. It still lands what a backfill over the same rows
/// lands.
#[test]
fn a_step_waits_only_on_its_own_view_and_its_producer() {
    let (_dir, mut engine, base) = table_engine(4, &u64_i64_cols());
    let (count_v, _) = register_count(&mut engine, base, &[1], "count_v");
    let cols = [
        col_def("v", TypeCode::I64),
        col_def("id", TypeCode::U64),
        col_def("n", TypeCode::I64),
    ];
    let joined = try_register_view(&mut engine, join_base_with_count(base, count_v), "joined", &cols, 0, 0).unwrap();
    let rows = [(1, 1, 10), (2, 1, 20), (3, 1, 10)];
    engine.ingest_unticked(base, base_rows(&rows), PkRule::Enforce).unwrap();
    let mut host = EchoDrive::new(&mut engine);
    tick(&mut host, base, 1).unwrap();
    // `count_v`'s round and `joined`'s relay of `base` share a wait; `joined`'s
    // step over `count_v` then runs on without a round.
    assert_eq!((host.rounds, host.waits), (2, 1));
    let oracle = try_register_view(&mut engine, join_base_with_count(base, count_v), "oracle", &cols, 0, 0).unwrap();
    backfill(&mut engine, oracle).unwrap();
    let want = held(&mut engine, oracle);
    assert!(!want.is_empty());
    assert_eq!(held(&mut engine, joined), want);
}

/// `a ⋈ b` on their `v`, `a`'s side through a `DISTINCT` where `distinct`: a
/// clamp folds the register `a`'s relay round writes, a plain re-key does not.
fn join_on_v(a: u64, b: u64, distinct: bool) -> Circuit {
    use gnitz_wire::NullKeys;
    let mut circuit = Circuit::default();
    let key = [(1, TypeCode::I64)];
    let deltas = [(a, distinct), (b, false)].map(|(source, distinct)| {
        let scan = circuit.input_scattered(source, ReadBound::None, Some(key.to_vec()));
        let rows = match distinct {
            true => circuit.distinct(scan),
            false => scan,
        };
        circuit.map_reindex(rows, &key, &[0], NullKeys::Keep)
    });
    circuit.join(deltas, deltas, gnitz_wire::JoinKind::Equi);
    circuit
}

/// Tables `a` and `b`, `(id U64 PK, v I64)`, beside three other workers, and a
/// [`join_on_v`] view per entry of `distinct`, in that order.
fn joins_on_v(distinct: &[bool]) -> (tempfile::TempDir, CatalogEngine, [u64; 2], Vec<u64>) {
    let (dir, mut engine, a) = table_engine(4, &u64_i64_cols());
    let b = engine.create_table("public.b", &u64_i64_cols(), &[0]).unwrap();
    let cols = [
        col_def("k", TypeCode::I64),
        col_def("a_id", TypeCode::U64),
        col_def("b_id", TypeCode::U64),
    ];
    let views = distinct
        .iter()
        .enumerate()
        .map(|(i, &d)| try_register_view(&mut engine, join_on_v(a, b, d), &format!("j{i}"), &cols, 0, 0).unwrap())
        .collect();
    (dir, engine, [a, b], views)
}

/// Views relaying one source by one scatter key share one exchange round per
/// tick, rows or none, and each holds what it holds when it runs its own round,
/// weights included — the view that folds the round's rows among them, between
/// two that do not.
#[test]
fn views_relaying_one_source_by_one_key_share_its_round() {
    // `(source, rows)` of each tick.
    let ticks: [(usize, &[_]); 5] = [
        (1, &[(1, 1, 10), (2, 1, 20), (3, 1, 10)]),
        (0, &[(1, 1, 10), (2, 1, 20), (4, 1, 30)]),
        (0, &[(2, -1, 20)]),
        (1, &[(3, -1, 10), (5, 1, 20)]),
        (0, &[]),
    ];
    let shapes = [false, true, false];
    let (_dir, mut engine, sources, views) = joins_on_v(&shapes);
    for &(side, rows) in &ticks {
        let mut host = EchoDrive::new(&mut engine);
        host.cat
            .ingest_unticked(sources[side], base_rows(rows), PkRule::Enforce)
            .unwrap();
        tick(&mut host, sources[side], 1).unwrap();
        assert_eq!(host.rounds, 1, "one round of source {side} for every view");
    }
    for (i, (&view, &distinct)) in views.iter().zip(&shapes).enumerate() {
        let (_dir, mut alone, sources, own) = joins_on_v(&[distinct]);
        for &(side, rows) in &ticks {
            push_and_tick(&mut alone, sources[side], base_rows(rows));
        }
        let want = held(&mut alone, own[0]);
        assert!(!want.is_empty(), "view {i} joins some row");
        assert_eq!(held(&mut engine, view), want, "view {i}");
    }
}

/// A view's backfill mints the ground row a global aggregate owes; a tick that
/// brings this process no row lands nothing, in that view or any other; a row
/// moves the count to what a backfill over it holds.
#[test]
fn an_empty_tick_lands_only_an_owed_ground_row() {
    let (_dir, mut engine, base) = table_engine(1, &u64_i64_cols());
    let once = register_identity_view(&mut engine, base, "once", &u64_i64_cols());
    let (count, plan) = register_count(&mut engine, base, &[], "count");
    let ground = plan.ground_row().unwrap();
    let ground = zset_of(&ground, ground.schema());

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
    let (oracle, _) = register_count(&mut engine, base, &[], "oracle");
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
    let (_dir, mut engine, base, views) = engine_with_views();
    let rows = [(1, 1, 10)];

    engine.registry.ingest(base, base_rows(&rows)).unwrap();
    backfill(&mut engine, views.twice).unwrap();
    assert_eq!(held(&mut engine, views.twice), times(&rows, 2));
    for vid in views.all().into_iter().filter(|&v| v != views.twice) {
        assert!(held(&mut engine, vid).is_empty(), "view {vid}");
    }
}

/// A view `name` joining tables `a` and `b`, each `(id, k)`, on column `key` of
/// both: `(key, a_id, b_id)`. Keyed on `k` the join keeps its own traces; keyed
/// on `id` it reads the tables' stores.
fn register_join(engine: &mut CatalogEngine, [a, b]: [u64; 2], key: u32, name: &str) -> u64 {
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
    try_register_view(engine, circuit, name, &join_cols, 0, 0).unwrap()
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
        let (_dir, mut engine) = empty_catalog_with(1, chunked(1));
        let cols = [col_def("id", TypeCode::U64), col_def("k", TypeCode::U64)];
        let a = engine.create_table("public.a", &cols, &[0]).unwrap();
        let b = engine.create_table("public.b", &cols, &[0]).unwrap();
        let view = register_join(&mut engine, [a, b], key, "join");
        for (tid, rows) in [(a, &a_rows[..]), (b, &b_rows)] {
            let rows = make_batch(&engine.registry.relation_or_err(tid).unwrap().schema(), rows);
            engine.registry.ingest(tid, rows).unwrap();
        }

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

/// A backfill reads each source at its cut, and the ticks sealing what lay above
/// it bring the rest, each row once — the order a boot rebuilds a view in, ahead
/// of its replayed tail. A join then pairs each tail with the other table as the
/// earlier steps left it; a view filled once everything is sealed holds the same.
#[test]
fn a_backfill_at_the_cut_and_the_ticks_above_it_fill_a_join_once() {
    let sealed = [[(1, 1, 10), (2, 1, 20)], [(2, 1, 10), (3, 1, 20)]];
    let above = [[(3, 1, 10), (4, 1, 30)], [(4, 1, 30), (5, 1, 10)]];
    for key in [1, 0] {
        let (_dir, mut engine) = empty_catalog(1);
        let cols = [col_def("id", TypeCode::U64), col_def("k", TypeCode::U64)];
        let tables = ["a", "b"].map(|name| engine.create_table(&format!("public.{name}"), &cols, &[0]).unwrap());
        let view = register_join(&mut engine, tables, key, "join");
        for (i, &tid) in tables.iter().enumerate() {
            let schema = engine.registry.relation_or_err(tid).unwrap().schema();
            engine.registry.ingest(tid, make_batch(&schema, &sealed[i])).unwrap();
            engine
                .registry
                .ingest_with(tid, make_batch(&schema, &above[i]), PkRule::Enforce, true)
                .unwrap();
        }

        backfill(&mut engine, view).unwrap();
        let at_cut = held(&mut engine, view);
        for (&tid, round) in tables.iter().zip(2..) {
            tick(&mut engine, tid, round).unwrap();
        }

        let fresh = register_join(&mut engine, tables, key, "fresh");
        backfill(&mut engine, fresh).unwrap();
        let (got, want) = (held(&mut engine, view), held(&mut engine, fresh));
        assert_eq!(got, want, "keyed on column {key}");
        assert!(at_cut.len() < want.len(), "premise: the tail adds matches");
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
