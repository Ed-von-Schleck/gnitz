use super::*;
use crate::test_support::{circuit_nodes_batch, scanning_circuit};

/// `view`'s CIRCUIT_NODES batch for a circuit scanning each of `sources`.
fn scans(view: u64, sources: &[u64]) -> Batch {
    circuit_nodes_batch(view, &scanning_circuit(sources))
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
        dag.apply_circuit_delta(&scans(view, sources));
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

/// Only a `ScanDelta` row is an edge, and a `+1` links each `(view, source)`
/// pair once however many scans name it. A `-1` forgets the view, and both are
/// no-ops when repeated — including a compensation's `-1` whose `+1` never
/// reached the map.
#[test]
fn the_dep_map_follows_circuit_deltas_idempotently() {
    // Raw rows, not a `Circuit`: a sink carrying a `source_table` is a shape no
    // circuit encodes to.
    let mut bb = gnitz_zset::repr::BatchBuilder::new(crate::catalog::SysFamily::CircuitNodes.schema());
    for (node_id, (opcode, source)) in [
        (gnitz_wire::Opcode::ScanDelta, 1),
        (gnitz_wire::Opcode::ScanDelta, 1),
        (gnitz_wire::Opcode::ScanDelta, 2),
        (gnitz_wire::Opcode::IntegrateSink, 3),
    ]
    .into_iter()
    .enumerate()
    {
        let row = gnitz_wire::sys_rows::CircuitNodeRow {
            view_id: 5,
            node_id: node_id as u64,
            opcode: opcode.as_wire(),
            source_table: Some(source),
            inputs: [None; 2],
            params: None,
        };
        gnitz_wire::sys_rows::write_circuit_node_row(&mut bb, &row, 1);
    }
    let plus = bb.finish();
    let minus = plus.clone().negated();

    let mut dag = DagEngine::default();
    dag.apply_circuit_delta(&minus);
    assert!(dag.dep.forward.is_empty() && dag.dep.reverse.is_empty());

    for _ in 0..2 {
        dag.apply_circuit_delta(&plus);
        assert_eq!(dag.sources_of(5), [1, 2]);
        assert_eq!(dag.dependents_of(1), [5]);
        assert_eq!(dag.dependents_of(2), [5]);
        assert!(!dag.is_scanned(3), "a non-ScanDelta node is no edge");
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
    let (five, six) = (scans(5, &[1]), scans(6, &[1]));

    let mut dag = DagEngine::default();
    dag.apply_circuit_delta(&five);
    dag.apply_circuit_delta(&six);
    assert_eq!(dag.dependents_of(1), [5, 6]);

    dag.apply_circuit_delta(&five.negated());
    assert_eq!(dag.dependents_of(1), [6]);
    assert!(dag.sources_of(5).is_empty());
    assert_eq!(dag.sources_of(6), [1]);
}
