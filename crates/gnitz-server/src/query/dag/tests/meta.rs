use super::*;
use crate::test_support::{circuit_batch, circuit_cell_batch, scanning_circuit};

/// `view`'s CIRCUIT_TAB batch for a circuit scanning each of `sources`.
fn scans(view: u64, sources: &[u64]) -> Batch {
    circuit_batch(view, &scanning_circuit(sources))
}

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
        dag.apply_circuit_delta(&scans(view, sources)).unwrap();
    }

    assert!(dag.source_closure(vec![]).is_empty());
    assert_eq!(dag.source_closure(vec![3]), set(&[1, 2]));
    assert_eq!(dag.source_closure(vec![13]), set(&[10, 11, 12]));
    assert_eq!(dag.source_closure(vec![21]), set(&[20]));
    assert!(dag.source_closure(vec![1]).is_empty());
    assert!(dag.source_closure(vec![99]).is_empty());
    assert_eq!(dag.source_closure(vec![3, 2]), set(&[1, 2]));
    // The other direction, so a walk that read the wrong half of `DepMap`
    // cannot pass both.
    assert_eq!(dag.dependent_closure(vec![1]), set(&[2, 3]));
    assert_eq!(dag.dependent_closure(vec![10]), set(&[11, 12, 13]));
    assert!(dag.dependent_closure(vec![21]).is_empty());
}

/// A `+1` links each `(view, source)` pair once however many scans name it. A
/// `-1` forgets the view, and both are no-ops when repeated — including a
/// compensation's `-1` whose `+1` never reached the map.
#[test]
fn the_dep_map_follows_circuit_deltas_idempotently() {
    let plus = scans(5, &[1, 1, 2]);
    let minus = plus.clone().negated();

    let mut dag = DagEngine::default();
    dag.apply_circuit_delta(&minus).unwrap();
    assert!(dag.dep.forward.is_empty() && dag.dep.reverse.is_empty());

    for _ in 0..2 {
        dag.apply_circuit_delta(&plus).unwrap();
        assert_eq!(dag.sources_of(5), [1, 2]);
        assert_eq!(dag.dependents_of(1), [5]);
        assert_eq!(dag.dependents_of(2), [5]);
    }
    for _ in 0..2 {
        dag.apply_circuit_delta(&minus).unwrap();
        assert!(dag.dep.forward.is_empty() && dag.dep.reverse.is_empty());
    }
}

/// A cell that does not decode is the hook's error, and a circuit that scans
/// nothing leaves no entry behind.
#[test]
fn an_undecodable_cell_is_refused_and_a_scanless_circuit_links_nothing() {
    let mut dag = DagEngine::default();
    let err = dag.apply_circuit_delta(&circuit_cell_batch(5, &[0xff])).unwrap_err();
    assert!(err.starts_with("view 5: circuit: truncated"), "{err}");
    dag.apply_circuit_delta(&circuit_batch(6, &gnitz_wire::Circuit::default()))
        .unwrap();
    assert!(dag.dep.forward.is_empty() && dag.dep.reverse.is_empty());
}

/// One `-1` unlinks only its own view, leaving a sibling over the same source
/// linked.
#[test]
fn a_retraction_unlinks_only_its_own_view() {
    let (five, six) = (scans(5, &[1]), scans(6, &[1]));

    let mut dag = DagEngine::default();
    dag.apply_circuit_delta(&five).unwrap();
    dag.apply_circuit_delta(&six).unwrap();
    assert_eq!(dag.dependents_of(1), [5, 6]);

    dag.apply_circuit_delta(&five.negated()).unwrap();
    assert_eq!(dag.dependents_of(1), [6]);
    assert!(dag.sources_of(5).is_empty());
    assert_eq!(dag.sources_of(6), [1]);
}
