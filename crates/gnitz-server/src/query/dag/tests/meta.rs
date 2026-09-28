use super::*;

/// Install `edges` (source → view) into the dep map, the same entries
/// `DepMap::apply` writes per `ScanDelta` node.
fn dag_with_deps(edges: &[(u64, u64)]) -> DagEngine {
    let mut dag = DagEngine::default();
    for &(src, view) in edges {
        dag.dep.forward.entry(src).or_default().push(view);
        dag.dep.reverse.entry(view).or_default().push(src);
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
    assert_eq!(dag.source_closure(vec![3]), [1u64, 2].into_iter().collect());
    assert_eq!(dag.source_closure(vec![13]), [10u64, 11, 12].into_iter().collect());
    assert_eq!(dag.source_closure(vec![21]), [20u64].into_iter().collect());
    assert!(dag.source_closure(vec![1]).is_empty());
    assert!(dag.source_closure(vec![99]).is_empty());
    // The other direction over the same edges, so a walk that read the wrong
    // half of `DepMap` cannot pass both.
    assert_eq!(
        DepMap::closure(&dag.dep.forward, vec![1]),
        [2u64, 3].into_iter().collect::<rustc_hash::FxHashSet<u64>>()
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

    let mut dag = DagEngine::default();
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

/// One `-1` unlinks only its own view, leaving a sibling over the same source
/// linked.
#[test]
fn a_retraction_unlinks_only_its_own_view() {
    let dag_of = |vid: u64| {
        let mut circuit = gnitz_wire::Circuit::default();
        let d = circuit.input_delta(1, gnitz_wire::ReadBound::None);
        circuit.sink(d);
        let mut bb = gnitz_store::storage::BatchBuilder::new(*crate::catalog::SysFamily::CircuitNodes.schema());
        gnitz_wire::sys_rows::write_circuit_rows(&mut bb, vid, &circuit);
        bb.finish()
    };
    let (five, six) = (dag_of(5), dag_of(6));
    let mut drop_five = five.clone();
    drop_five.map_weights(i64::wrapping_neg);

    let mut dag = DagEngine::default();
    dag.apply_circuit_delta(&five);
    dag.apply_circuit_delta(&six);
    assert_eq!(dag.dependents_of(1), &[5u64, 6][..]);

    dag.apply_circuit_delta(&drop_five);
    assert_eq!(dag.dependents_of(1), &[6u64][..]);
    assert!(dag.sources_of(5).is_empty());
    assert_eq!(dag.sources_of(6), &[1u64][..]);
}
