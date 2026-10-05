use super::*;
use crate::catalog::CatalogEngine;
use crate::test_support::{
    circuit_batch, circuit_cell_batch, col_def, scanning_circuit, scratch_dir, try_register_view,
};
use gnitz_wire::TypeCode;

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

/// A circuit delta not applied yet reaches the base tables its views will: for two
/// new views, the second scanning the first and the first scanning an existing view
/// over a base table and a stream, the answer is the one the dependency map gives
/// once both are linked.
#[test]
fn an_unapplied_circuit_delta_reaches_the_bases_its_views_will() {
    let dir = scratch_dir("dag_meta", "scanned_by");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
    let cols = vec![col_def("id", TypeCode::U64), col_def("v", TypeCode::I64)];
    let table = engine.create_table("public.t", &cols, &[0]).unwrap();
    let stream = engine
        .create_table_with(
            "public.s",
            &cols,
            &[0],
            gnitz_wire::TableProps { stream: true, ..Default::default() },
        )
        .unwrap();
    let existing = try_register_view(&mut engine, scanning_circuit(&[table, stream]), "existing", &cols, 0, 0).unwrap();
    let first = engine.allocate_ids(1).unwrap();
    let second = engine.allocate_ids(1).unwrap();

    let mut circuits = circuit_batch(second, &scanning_circuit(&[first]));
    circuits.append_batch(&circuit_batch(first, &scanning_circuit(&[existing])));
    let scanned = engine.dag.base_tables_scanned_by(&engine.registry, &circuits).unwrap();
    assert_eq!(scanned, [table]);

    engine.dag.dep.link(first, [existing].into_iter());
    engine.dag.dep.link(second, [first].into_iter());
    assert_eq!(
        engine.dag.base_tables_reachable_from(&engine.registry, [first, second]),
        scanned
    );

    let err = engine
        .dag
        .base_tables_scanned_by(&engine.registry, &circuit_cell_batch(first, &[0xff]))
        .unwrap_err();
    assert!(err.starts_with(&format!("view {first}: circuit: truncated")), "{err}");

    let _ = std::fs::remove_dir_all(&dir);
}
