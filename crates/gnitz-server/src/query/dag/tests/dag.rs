use super::*;

#[test]
fn test_invalidation() {
    let mut dag = DagEngine::new();
    dag.meta.insert(42, Rc::new(ViewMeta::nothing_special()));
    dag.dep.valid = true;

    dag.invalidate(42);
    assert!(!dag.meta.contains_key(&42));
    assert!(dag.dep.valid, "invalidate drops the plan and its meta, not the dep map");

    dag.invalidate_dep_map();
    assert!(!dag.dep.valid);

    dag.meta.insert(99, Rc::new(ViewMeta::nothing_special()));
    dag.invalidate_all();
    assert!(dag.meta.is_empty());
}

/// Wiring: the production table/view-drop path routes through `evict_meta`.
#[test]
fn unregister_table_evicts_the_dropped_relations_meta() {
    let mut registry = gnitz_store::relation::RelationRegistry::new(1);
    let mut dag = DagEngine::new();
    dag.meta.insert(43, Rc::new(ViewMeta::nothing_special()));
    dag.unregister_table(&mut registry, 43);
    assert!(!dag.meta.contains_key(&43), "unregister_table must evict");
}

#[test]
fn test_dep_map_empty() {
    let registry = gnitz_store::relation::RelationRegistry::new(1);
    let mut dag = DagEngine::new();
    dag.get_dep_map(&registry);
    assert!(dag.dep.forward.is_empty());
    assert!(dag.dep.valid);
    assert!(dag.get_source_ids(&registry, 42).is_empty());
}

/// Install `edges` (source → view) into an already-valid dep map, the same
/// pair of entries `DepMap::get_or_rebuild` writes per `ScanDelta` node.
fn dag_with_deps(edges: &[(i64, i64)]) -> DagEngine {
    let mut dag = DagEngine::new();
    for &(src, view) in edges {
        dag.dep.forward.entry(src).or_default().push(view);
        dag.dep.reverse.entry(view).or_default().push(src);
    }
    dag.dep.valid = true;
    dag
}

/// `source_closure` walks `view → sources` transitively, over a chain, a
/// diamond and a disconnected pair. A seed is not its own source, so neither
/// direction reports it.
#[test]
fn test_source_closure_walks_sources_transitively() {
    // chain 1 → 2 → 3, diamond 10 → {11,12} → 13, disconnected pair 20 → 21.
    let registry = gnitz_store::relation::RelationRegistry::new(1);
    let mut dag = dag_with_deps(&[(1, 2), (2, 3), (10, 11), (10, 12), (11, 13), (12, 13), (20, 21)]);

    assert!(dag.source_closure(&registry, vec![]).is_empty());
    assert_eq!(dag.source_closure(&registry, vec![3]), [1i64, 2].into_iter().collect());
    assert_eq!(
        dag.source_closure(&registry, vec![13]),
        [10i64, 11, 12].into_iter().collect()
    );
    assert_eq!(dag.source_closure(&registry, vec![21]), [20i64].into_iter().collect());
    assert!(dag.source_closure(&registry, vec![1]).is_empty());
    assert!(dag.source_closure(&registry, vec![99]).is_empty());
    // The other direction over the same edges, so a walk that read the wrong
    // half of `DepMap` cannot pass both.
    dag.get_dep_map(&registry);
    assert_eq!(
        DepMap::closure(&dag.dep.forward, vec![1]),
        [2i64, 3].into_iter().collect::<rustc_hash::FxHashSet<i64>>()
    );
}
