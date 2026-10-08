use super::*;

fn set(ids: &[u64]) -> FxHashSet<u64> {
    ids.iter().copied().collect()
}

/// Both closures walk transitively over a chain, a diamond and a disconnected
/// pair. A seed is reported only when another seed reaches it.
#[test]
fn the_closures_walk_both_directions_transitively() {
    // chain 1 → 2 → 3, diamond 10 → {11,12} → 13, disconnected pair 20 → 21.
    let mut dag = DagEngine::default();
    for (view, sources) in [
        (2, &[1][..]),
        (3, &[2]),
        (11, &[10]),
        (12, &[10]),
        (13, &[11, 12]),
        (21, &[20]),
    ] {
        dag.dep.link(view, sources.iter().copied());
    }

    assert!(dag.source_closure([]).is_empty());
    assert_eq!(dag.source_closure([3]), set(&[1, 2]));
    assert_eq!(dag.source_closure([13]), set(&[10, 11, 12]));
    assert_eq!(dag.source_closure([21]), set(&[20]));
    assert!(dag.source_closure([1]).is_empty());
    assert!(dag.source_closure([99]).is_empty());
    assert_eq!(dag.source_closure([3, 2]), set(&[1, 2]));
    // The other direction, so a walk that read the wrong half of `DepMap`
    // cannot pass both.
    assert_eq!(dag.dependent_closure([1]), set(&[2, 3]));
    assert_eq!(dag.dependent_closure([10]), set(&[11, 12, 13]));
    assert!(dag.dependent_closure([21]).is_empty());
}

/// A view links to each source once however many scans name it. Forgetting it
/// unlinks it, and both are no-ops when repeated — including a forget of a view
/// that never linked.
#[test]
fn linking_and_forgetting_are_idempotent() {
    let mut dag = DagEngine::default();
    dag.forget(5);
    assert!(dag.dep.forward.is_empty() && dag.dep.reverse.is_empty());

    for _ in 0..2 {
        dag.dep.link(5, [1, 1, 2].into_iter());
        assert_eq!(dag.sources_of(5), [1, 2]);
        assert_eq!(dag.dependents_of(1), [5]);
        assert_eq!(dag.dependents_of(2), [5]);
    }
    for _ in 0..2 {
        dag.forget(5);
        assert!(dag.dep.forward.is_empty() && dag.dep.reverse.is_empty());
    }
}

/// A view that scans nothing leaves no entry behind.
#[test]
fn a_scanless_view_links_nothing() {
    let mut dag = DagEngine::default();
    dag.dep.link(6, std::iter::empty());
    assert!(dag.dep.forward.is_empty() && dag.dep.reverse.is_empty());
}

/// Forgetting one view unlinks only it, leaving a sibling over the same source
/// linked.
#[test]
fn forgetting_a_view_unlinks_only_its_own_edges() {
    let mut dag = DagEngine::default();
    dag.dep.link(5, [1].into_iter());
    dag.dep.link(6, [1].into_iter());
    assert_eq!(dag.dependents_of(1), [5, 6]);

    dag.forget(5);
    assert_eq!(dag.dependents_of(1), [6]);
    assert!(dag.sources_of(5).is_empty());
    assert_eq!(dag.sources_of(6), [1]);
}

/// A tick of a relation drives a view while one that scans it does not await
/// its rebuild.
#[test]
fn a_relation_is_ticked_while_a_view_scanning_it_is_not_awaiting_its_rebuild() {
    let mut dag = DagEngine::default();
    dag.dep.link(5, [1].into_iter());
    dag.dep.link(6, [1].into_iter());
    assert!(dag.is_ticked(1) && !dag.is_ticked(2));

    dag.set_rebuild(set(&[5]));
    assert!(dag.is_ticked(1));
    dag.set_rebuild(set(&[5, 6]));
    assert!(!dag.is_ticked(1) && dag.is_scanned(1));
}
