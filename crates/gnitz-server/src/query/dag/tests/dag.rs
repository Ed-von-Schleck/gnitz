use super::*;

#[test]
fn test_invalidation() {
    let mut dag = DagEngine::new();
    dag.meta.insert(42, Rc::new(ViewMeta::empty()));

    dag.invalidate(42);
    assert!(!dag.meta.contains_key(&42));

    dag.meta.insert(99, Rc::new(ViewMeta::empty()));
    dag.invalidate_all();
    assert!(dag.meta.is_empty());
}

/// Wiring: the production table/view-drop path routes through `evict_meta`.
#[test]
fn unregister_table_evicts_the_dropped_relations_meta() {
    let mut registry = gnitz_store::relation::RelationRegistry::new(
        gnitz_store::storage::Slot::SOLO,
        gnitz_store::relation::StoreConfig::default(),
    );
    let mut dag = DagEngine::new();
    dag.meta.insert(43, Rc::new(ViewMeta::empty()));
    dag.unregister_table(&mut registry, 43);
    assert!(!dag.meta.contains_key(&43), "unregister_table must evict");
}

/// Install `edges` (source → view) into the dep map, the same entries
/// `DepMap::apply` writes per `ScanDelta` node.
fn dag_with_deps(edges: &[(i64, i64)]) -> DagEngine {
    let mut dag = DagEngine::new();
    for &(src, view) in edges {
        dag.dep.forward.entry(src).or_default().push(view);
        dag.dep.reverse.entry(view).or_default().push(src);
        dag.dep.edges.insert((view, src));
    }
    dag
}

/// `source_closure` walks `view → sources` transitively, over a chain, a
/// diamond and a disconnected pair. A seed is not its own source, so neither
/// direction reports it.
#[test]
fn test_source_closure_walks_sources_transitively() {
    // chain 1 → 2 → 3, diamond 10 → {11,12} → 13, disconnected pair 20 → 21.
    let dag = dag_with_deps(&[(1, 2), (2, 3), (10, 11), (10, 12), (11, 13), (12, 13), (20, 21)]);

    assert!(dag.source_closure(vec![]).is_empty());
    assert_eq!(dag.source_closure(vec![3]), [1i64, 2].into_iter().collect());
    assert_eq!(dag.source_closure(vec![13]), [10i64, 11, 12].into_iter().collect());
    assert_eq!(dag.source_closure(vec![21]), [20i64].into_iter().collect());
    assert!(dag.source_closure(vec![1]).is_empty());
    assert!(dag.source_closure(vec![99]).is_empty());
    // The other direction over the same edges, so a walk that read the wrong
    // half of `DepMap` cannot pass both.
    assert_eq!(
        DepMap::closure(&dag.dep.forward, vec![1]),
        [2i64, 3].into_iter().collect::<rustc_hash::FxHashSet<i64>>()
    );
}

/// A `+1` delta links each `(view, source)` pair once however many scans name it,
/// a `-1` delta forgets the view, and both are no-ops when repeated — including a
/// compensation's `-1` whose `+1` never reached the map.
#[test]
fn the_dep_map_follows_circuit_deltas_idempotently() {
    let mut circuit = gnitz_wire::Circuit::default();
    let first = circuit.input_delta(1, gnitz_wire::ReadBound::None);
    circuit.input_delta(1, gnitz_wire::ReadBound::None);
    circuit.input_delta(2, gnitz_wire::ReadBound::None);
    circuit.sink(first);
    let mut bb = gnitz_store::storage::BatchBuilder::new(*crate::catalog::SysFamily::CircuitNodes.schema());
    gnitz_wire::sys_rows::write_circuit_rows(&mut bb, 5, &circuit);
    let plus = bb.finish();
    let mut minus = plus.clone();
    minus.map_weights(i64::wrapping_neg);

    let mut dag = DagEngine::new();
    dag.apply_circuit_delta(&minus);
    assert!(dag.dep.forward.is_empty() && dag.dep.reverse.is_empty());

    for _ in 0..2 {
        dag.apply_circuit_delta(&plus);
        assert_eq!(dag.dep.reverse[&5], [1, 2]);
        assert_eq!(dag.dep.forward[&1], [5]);
        assert_eq!(dag.dep.forward[&2], [5]);
    }
    for _ in 0..2 {
        dag.apply_circuit_delta(&minus);
        assert!(dag.dep.forward.is_empty() && dag.dep.reverse.is_empty());
    }
}
