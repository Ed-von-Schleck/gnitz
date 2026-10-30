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
fn engine_with_tables<const N: usize>(name: &str, more: [&str; N]) -> (CatalogEngine, u64, [u64; N]) {
    let (mut engine, t) = table_engine("dag_meta", name, 1, &u64_i64_cols());
    let ids = more.map(|m| {
        engine
            .create_table(&format!("public.{m}"), &u64_i64_cols(), &[0])
            .unwrap()
    });
    (engine, t, ids)
}

/// Both closures walk transitively over a chain, a diamond and a disconnected
/// pair. A seed is reported only when another seed reaches it.
#[test]
fn the_closures_walk_both_directions_transitively() {
    // chain a → c1 → c2, diamond d → {d1, d2} → d3, disconnected pair p → p1.
    let (mut engine, a, [d, p]) = engine_with_tables("closures", ["d", "p"]);
    let cols = u64_i64_cols();
    let c1 = register_identity_view(&mut engine, a, "c1", &cols);
    let c2 = register_identity_view(&mut engine, c1, "c2", &cols);
    let d1 = register_identity_view(&mut engine, d, "d1", &cols);
    let d2 = register_identity_view(&mut engine, d, "d2", &cols);
    let d3 = union_view(&mut engine, &[d1, d2], "d3");
    let p1 = register_identity_view(&mut engine, p, "p1", &cols);
    let dag = &engine.dag;

    assert!(dag.source_closure([]).is_empty());
    assert_eq!(dag.source_closure([c2]), set(&[a, c1]));
    assert_eq!(dag.source_closure([d3]), set(&[d, d1, d2]));
    assert_eq!(dag.source_closure([p1]), set(&[p]));
    assert!(dag.source_closure([a]).is_empty());
    assert!(dag.source_closure([u64::MAX]).is_empty());
    assert_eq!(dag.source_closure([c2, c1]), set(&[a, c1]));
    // The other direction, so a walk that read the wrong half of the graph
    // cannot pass both.
    assert_eq!(dag.dependent_closure([a]), set(&[c1, c2]));
    assert_eq!(dag.dependent_closure([d]), set(&[d1, d2, d3]));
    assert!(dag.dependent_closure([p1]).is_empty());
}

/// A view links to each source once however many scans name it, in first-scan
/// order. Forgetting a view drops its edges and its pending rebuild, leaves a
/// sibling over the same source linked, keeps no emptied entry, and is a no-op
/// when repeated or for an id never linked.
#[test]
fn forgetting_a_view_drops_only_what_it_linked() {
    let (mut engine, t1, [t2]) = engine_with_tables("forget", ["t2"]);
    let v = union_view(&mut engine, &[t2, t1, t2], "v");
    let sibling = register_identity_view(&mut engine, t1, "sibling", &u64_i64_cols());
    let dag = &mut engine.dag;
    assert_eq!(dag.sources_of(v).collect::<Vec<_>>(), [t2, t1]);
    assert_eq!(dag.dependents_of(t1), [v, sibling]);
    assert_eq!(dag.dependents_of(t2), [v]);
    dag.rebuild = [v].into_iter().collect();

    for _ in 0..2 {
        dag.forget(v);
        dag.forget(u64::MAX);
        assert!(dag.sources_of(v).next().is_none() && !dag.awaits_rebuild(v));
        assert_eq!(dag.dependents_of(t1), [sibling]);
        assert_eq!(dag.sources_of(sibling).collect::<Vec<_>>(), [t1]);
        assert!(!dag.dep.forward.contains_key(&t2));
    }
    dag.forget(sibling);
    assert!(dag.dep.forward.is_empty());
}

/// A tick of a relation drives a view while one that scans it does not await
/// its rebuild.
#[test]
fn a_relation_is_ticked_while_a_view_scanning_it_is_not_awaiting_its_rebuild() {
    let mut dag = DagEngine::default();
    dag.dep.link(5, [1]);
    dag.dep.link(6, [1]);
    assert!(dag.is_ticked(1) && !dag.is_ticked(2));

    dag.rebuild = [5].into_iter().collect();
    assert!(dag.is_ticked(1));
    dag.rebuild = [5, 6].into_iter().collect();
    assert!(!dag.is_ticked(1) && dag.is_scanned(1));
}

/// The schedule names one step per dependency edge out of the source's forward
/// closure, ordered by view id — so a producer always precedes the steps it
/// feeds — and leaves out the views awaiting a rebuild. Each step says whether
/// it is its producer's last reader and whether a later one reads its view, of
/// the steps the schedule runs.
#[test]
fn the_schedule_names_every_edge_of_the_closure_in_id_order() {
    // `(view, producer, last, needed)` of each step.
    let steps = |dag: &mut DagEngine| -> Vec<(u64, u64, bool, bool)> {
        let steps = dag.tick_schedule(1);
        steps.iter().map(|s| (s.view, s.producer, s.last, s.needed)).collect()
    };
    let mut dag = DagEngine::default();
    for (view, sources) in [(2, &[1][..]), (3, &[1]), (4, &[2, 3]), (5, &[4]), (7, &[6])] {
        dag.dep.link(view, sources.iter().copied());
    }

    let every = [
        (2, 1, false, true),
        (3, 1, true, true),
        (4, 2, true, true),
        (4, 3, true, true),
        (5, 4, true, false),
    ];
    assert_eq!(steps(&mut dag), every);

    // A rebuild set is closed under dependents.
    dag.rebuild = [2, 4, 5].into_iter().collect();
    assert_eq!(steps(&mut dag), [(3, 1, true, false)], "of the steps it runs");
    dag.rebuild.remove(&2);
    assert_eq!(steps(&mut dag), [(2, 1, false, false), (3, 1, true, false)]);

    // A view linked or dropped after a schedule was read moves that schedule.
    dag.take_rebuild();
    dag.dep.link(8, [5]);
    let mut grown = every.to_vec();
    grown[4].3 = true;
    grown.push((8, 5, true, false));
    assert_eq!(steps(&mut dag), grown);
    dag.dep.unlink(4, [2, 3]);
    assert_eq!(steps(&mut dag), [(2, 1, false, false), (3, 1, true, false)]);
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
    assert!(dag.sources_of(5).next().is_none() && !dag.is_scanned(5) && dag.layout_of(5).is_none());
}
