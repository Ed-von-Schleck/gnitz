use super::*;
use crate::catalog::CatalogEngine;
use crate::test_support::{register_identity_view, table_engine, try_register_view, u64_i64_cols};
use gnitz_wire::{Circuit, ReadBound};

fn set(ids: &[u64]) -> FxHashSet<u64> {
    ids.iter().copied().collect()
}

/// A union view over `sources`, each scanned in that order, named `name`.
fn union_view(engine: &mut CatalogEngine, sources: &[u64], name: &str) -> u64 {
    let mut circuit = Circuit::default();
    let scans: Vec<_> = sources
        .iter()
        .map(|&s| circuit.input_delta(s, ReadBound::None))
        .collect();
    scans[1..].iter().fold(scans[0], |acc, &scan| circuit.union(acc, scan));
    try_register_view(engine, circuit, name, &u64_i64_cols(), 0, 0).unwrap()
}

/// A fresh catalog holding base table `public.t` and one more per name in
/// `more`, each `(id U64 PK, v I64)`.
fn engine_with_tables<const N: usize>(more: [&str; N]) -> (tempfile::TempDir, CatalogEngine, u64, [u64; N]) {
    let (dir, mut engine, t) = table_engine(1, &u64_i64_cols());
    let ids = more.map(|m| {
        engine
            .create_table(&format!("public.{m}"), &u64_i64_cols(), &[0])
            .unwrap()
    });
    (dir, engine, t, ids)
}

/// A view's roots and a relation's downstream views walk the same edges
/// transitively, over a three-view chain, a diamond and a disconnected pair: a
/// view has a root exactly when it is downstream of it.
#[test]
fn roots_and_downstream_walk_both_directions_transitively() {
    // chain a → c1 → c2 → c3, diamond d → {d1, d2} → d3, disconnected pair p → p1.
    let (_dir, mut engine, a, [d, p]) = engine_with_tables(["d", "p"]);
    let cols = u64_i64_cols();
    let c1 = register_identity_view(&mut engine, a, "c1", &cols);
    let c2 = register_identity_view(&mut engine, c1, "c2", &cols);
    let c3 = register_identity_view(&mut engine, c2, "c3", &cols);
    let d1 = register_identity_view(&mut engine, d, "d1", &cols);
    let d2 = register_identity_view(&mut engine, d, "d2", &cols);
    let d3 = union_view(&mut engine, &[d1, d2], "d3");
    let p1 = register_identity_view(&mut engine, p, "p1", &cols);
    let dag = &engine.dag;

    for (view, root) in [(c1, a), (c2, a), (c3, a), (d1, d), (d3, d), (p1, p)] {
        assert_eq!(dag.roots_of(view), [root], "view {view}");
    }
    assert!(dag.roots_of(a).is_empty());
    assert!(dag.roots_of(u64::MAX).is_empty());
    // The other direction, so a walk that read the wrong half of the graph
    // cannot pass both.
    assert_eq!(dag.dep.downstream([a]), set(&[c1, c2, c3]));
    assert_eq!(dag.dep.downstream([d]), set(&[d1, d2, d3]));
    assert_eq!(dag.dep.downstream([c1]), set(&[c2, c3]));
    assert_eq!(dag.dep.downstream([d1, d2]), set(&[d3]));
    assert!(dag.dep.downstream([p1, u64::MAX]).is_empty());

    let views = [c1, c2, c3, d1, d2, d3, p1];
    for root in [a, d, p] {
        let rooted = |v: &u64| dag.roots_of(*v).contains(&root);
        let rooted: FxHashSet<u64> = views.into_iter().filter(rooted).collect();
        assert_eq!(rooted, dag.dep.downstream([root]), "views under {root}");
    }
}

/// A view registered again under a dropped id takes the roots of its new plan.
#[test]
fn a_views_roots_are_dropped_and_recomputed_with_its_view() {
    let (_dir, mut engine, t1, [t2]) = engine_with_tables(["t2"]);
    let cols = u64_i64_cols();
    let v1 = register_identity_view(&mut engine, t1, "v1", &cols);
    let v2 = register_identity_view(&mut engine, v1, "v2", &cols);
    assert_eq!(engine.dag.roots_of(v2), [t1]);
    engine.dag.forget(v2);
    assert!(engine.dag.roots_of(v2).is_empty());
    assert_eq!(engine.dag.roots_of(v1), [t1]);

    let circuit = crate::test_support::identity_circuit(t2, ReadBound::None);
    let registry = &engine.registry;
    let schema = crate::test_support::make_schema_u64_i64();
    engine
        .dag
        .register_view(registry, v2, &schema, false, v2, &circuit)
        .unwrap();
    assert_eq!(engine.dag.roots_of(v2), [t2]);
}

/// A view links to each source once however many times its builder named it, in
/// scan order. Forgetting a view drops its edges and its pending rebuild, leaves a
/// sibling over the same source linked, keeps no emptied entry, and is a no-op
/// when repeated or for an id never linked.
#[test]
fn forgetting_a_view_drops_only_what_it_linked() {
    let (_dir, mut engine, t1, [t2]) = engine_with_tables(["t2"]);
    let v = union_view(&mut engine, &[t2, t1, t2], "v");
    let sibling = register_identity_view(&mut engine, t1, "sibling", &u64_i64_cols());
    let dag = &mut engine.dag;
    assert_eq!(dag.sources_of(v).collect::<Vec<_>>(), [t2, t1]);
    assert_eq!(dag.dependents_of(t1), [v, sibling]);
    assert_eq!(dag.dependents_of(t2), [v]);

    for _ in 0..2 {
        dag.forget(v);
        dag.forget(u64::MAX);
        assert!(dag.sources_of(v).next().is_none());
        assert_eq!(dag.dependents_of(t1), [sibling]);
        assert_eq!(dag.sources_of(sibling).collect::<Vec<_>>(), [t1]);
        assert!(!dag.dep.forward.contains_key(&t2));
    }
    dag.forget(sibling);
    assert!(dag.dep.forward.is_empty());
}

/// The schedule names one step per dependency edge out of the source's forward
/// closure, ordered by view id — so a producer always precedes the steps it
/// feeds. Each step says whether it is its producer's last reader.
#[test]
fn the_schedule_names_every_edge_of_the_closure_in_id_order() {
    // `(view, producer, last)` of each step.
    let steps = |dag: &mut DagEngine| -> Vec<(u64, u64, bool)> {
        let steps = dag.tick_schedule(1);
        steps.iter().map(|s| (s.view, s.producer, s.last)).collect()
    };
    let mut dag = DagEngine::default();
    for (view, sources) in [(2, &[1][..]), (3, &[1]), (4, &[2, 3]), (5, &[4]), (7, &[6])] {
        dag.dep.link(view, sources.iter().copied());
    }

    let every = [(2, 1, false), (3, 1, true), (4, 2, true), (4, 3, true), (5, 4, true)];
    assert_eq!(steps(&mut dag), every);

    // A view linked or dropped after a schedule was read moves that schedule.
    dag.dep.link(8, [5]);
    let mut grown = every.to_vec();
    grown.push((8, 5, true));
    assert_eq!(steps(&mut dag), grown);
    dag.dep.unlink(4, [2, 3]);
    assert_eq!(steps(&mut dag), [(2, 1, false), (3, 1, true)]);
}

/// Steps relaying one producer by one scatter key share one round: the first
/// runs it, the last is told no later step reads it, and it folds where any of
/// them folds. Another key, or another producer, is a round of its own, and a
/// step its view does not relay reads none. Only a step that runs a round or
/// relays nothing reads the producer's own rows, so the last of those takes them.
#[test]
fn steps_relaying_one_producer_by_one_key_share_a_round() {
    let by_v = [(1, gnitz_wire::TypeCode::I64)];
    let by_id = [(0, gnitz_wire::TypeCode::U64)];
    // `(view, producer)` → the key the view's scan of the producer relays by.
    // View 3 folds what it gathers, so the round it shares gathers by merge.
    let relay_key = |view: u64, producer: u64| -> RelayKey<'_> {
        match (view, producer) {
            (2 | 5, 1) => Some((&by_v, false)),
            (3, 1) => Some((&by_v, true)),
            (4, 1) => Some((&by_id, false)),
            (5, 2) => Some((&by_v, false)),
            _ => None,
        }
    };
    let edges = vec![(2, 1), (3, 1), (4, 1), (5, 1), (5, 2), (6, 1)];
    let steps = schedule(edges, relay_key);
    let relays: Vec<_> = steps.iter().map(|s| s.relay).collect();
    let slot = |at, first, last, fold| Some(RelaySlot { at, first, last, fold });
    assert_eq!(
        relays,
        [
            slot(0, true, false, true),
            slot(0, false, false, true),
            slot(1, true, true, false),
            slot(0, false, true, true),
            slot(2, true, true, false),
            None
        ]
    );
    let last: Vec<bool> = steps.iter().map(|s| s.last).collect();
    assert_eq!(last, [false, false, false, false, true, true], "of producers 1 and 2");

    // With no direct reader behind them, the step running the shared round is
    // the last to read the producer's own rows.
    let steps = schedule(vec![(2, 1), (3, 1), (5, 1)], relay_key);
    let last: Vec<bool> = steps.iter().map(|s| s.last).collect();
    assert_eq!(last, [true, false, false]);
}

/// A view scans only relations older than it: ticks and backfills take ascending
/// id order as dependency order. The refusal links nothing.
#[test]
fn a_view_scanning_a_relation_not_older_than_it_is_refused() {
    use crate::test_support::{identity_circuit, make_schema_u64_i64};
    use gnitz_store::relation::StoreConfig;
    let registry = RelationRegistry::new("", Slot::SOLO, StoreConfig::default());
    let mut dag = DagEngine::default();
    for source in [5, 6] {
        let circuit = identity_circuit(source, gnitz_wire::ReadBound::None);
        let refused = dag.register_view(&registry, 5, &make_schema_u64_i64(), false, 5, &circuit);
        assert_eq!(
            refused.map(drop),
            Err(format!("scans relation {source}, which is not older than it"))
        );
    }
    assert!(dag.sources_of(5).next().is_none() && !dag.is_scanned(5));
}
