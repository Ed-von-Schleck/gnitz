use super::*;
use gnitz_wire::{JoinKind, MapKind, OpNode};

/// The inner-equi-join shape `emit_equi_join_terms` produces, as node ids:
/// two `ScanDelta`s, a reindex `Map` and an `IntegrateTrace` per side, the
/// two cross-wired `Join(DeltaTrace)` terms behind their normalization maps,
/// a `Union`, a residual `Filter`, a projection `Map`, and the sink.
///
/// ```text
///   0 scanA → 2 reindexA ─┬─→ 4 traceA ──────────┐
///                         └─────────────┐        │
///   1 scanB → 3 reindexB ─┬─→ 5 traceB ─┼→ 6 J_ab│ (delta=2, trace=5)
///                         └─────────────┴────────┴→ 7 J_ba (delta=3, trace=4)
///   6 → 8 map → 10 union ← 9 map ← 7;  10 → 11 filter → 12 map → 13 sink
/// ```
fn equi_join_circuit() -> LoadedCircuit {
    let (nodes, edges) = equi_join_parts();
    loaded_for_test(nodes, edges)
}

/// The same circuit's raw parts, for the tests that break one wire before
/// building it.
#[allow(clippy::type_complexity)]
fn equi_join_parts() -> (HashMap<i32, OpNode>, Vec<(i32, i32, i32)>) {
    let m = |cols: Vec<u32>| OpNode::Map(MapKind::Projection(cols));
    let nodes = HashMap::from([
        (0, scan_delta(100)),
        (1, scan_delta(200)),
        (2, m(vec![0])),
        (3, m(vec![0])),
        (4, OpNode::IntegrateTrace),
        (5, OpNode::IntegrateTrace),
        (6, OpNode::Join(JoinKind::DeltaTrace)),
        (7, OpNode::Join(JoinKind::DeltaTrace)),
        (8, m(vec![0])),
        (9, m(vec![0])),
        (10, OpNode::Union),
        (11, OpNode::Filter(None)),
        (12, m(vec![0])),
        (13, OpNode::IntegrateSink),
    ]);
    let edges = vec![
        (0, 2, PORT_IN),
        (1, 3, PORT_IN),
        (2, 4, PORT_IN),
        (3, 5, PORT_IN),
        (2, 6, PORT_IN_A),
        (5, 6, PORT_TRACE),
        (3, 7, PORT_IN_A),
        (4, 7, PORT_TRACE),
        (6, 8, PORT_IN),
        (7, 9, PORT_IN),
        (8, 10, PORT_IN_A),
        (9, 10, PORT_IN_B),
        (10, 11, PORT_IN),
        (11, 12, PORT_IN),
        (12, 13, PORT_IN),
    ];
    (nodes, edges)
}

/// The seed is resolved by the *cross-wiring*, not by position: `J_a`'s trace
/// port is the other branch's integral, so the trace whose input is `J_a`'s
/// own delta port lives on the sibling join. Getting that backwards would
/// seed the replay from the wrong side and silently compute a different
/// product.
#[test]
fn hydration_seeds_from_the_cross_wired_trace() {
    let lc = equi_join_circuit();
    // `d_a` is the reindex feeding `J_ab`'s delta port (node 2); `t_a` is the
    // trace that integrates *it* (node 4), which hangs off `J_ba`.
    assert_eq!(hydration_nodes(&lc).unwrap(), HydrationNodes::Join { d_a: 2, t_a: 4 },);
}

/// The linear shape resolves to its source relation, through any number of
/// filter/map nodes.
#[test]
fn hydration_of_a_linear_circuit_names_its_source() {
    let lc = loaded_for_test(
        HashMap::from([
            (0, scan_delta(77)),
            (1, OpNode::Filter(None)),
            (2, OpNode::Map(MapKind::Projection(vec![0]))),
            (3, OpNode::IntegrateSink),
        ]),
        vec![(0, 1, PORT_IN), (1, 2, PORT_IN), (2, 3, PORT_IN)],
    );
    assert_eq!(
        hydration_nodes(&lc).unwrap(),
        HydrationNodes::Relation { nid: 0, source: 77 }
    );
}

/// Every structural mismatch is a `Rejected`, never a silent `None`: a
/// bounded view must not reach its store with no way to hydrate it. These are
/// the trust boundary behind the planner's own eligibility gate — no SQL
/// reaches them, which is exactly why they are asserted here.
#[test]
fn a_malformed_circuit_is_rejected_rather_than_guessed_at() {
    let rejected = |lc: LoadedCircuit, what: &str| match hydration_nodes(&lc) {
        Err(CompileError::Rejected(_)) => {}
        other => panic!("{what}: expected Rejected, got {:?}", other.map(|h| format!("{h:?}"))),
    };

    // No sink at all.
    rejected(
        loaded_for_test(HashMap::from([(0, scan_delta(1))]), vec![]),
        "sinkless circuit",
    );
    // A shape the walk cannot replay (a Reduce under the sink).
    rejected(
        loaded_for_test(
            HashMap::from([
                (0, scan_delta(1)),
                (
                    1,
                    OpNode::Reduce {
                        group_cols: vec![0],
                        agg: vec![(gnitz_wire::AggFunc::Count, 0)],
                        global_ground: false,
                        out_key: gnitz_store::schema::ReduceOutKey::SyntheticFold,
                    },
                ),
                (2, OpNode::IntegrateSink),
            ]),
            vec![(0, 1, PORT_IN), (1, 2, PORT_IN)],
        ),
        "reduce under the sink",
    );
    // A union whose inputs are not delta/trace joins.
    rejected(
        loaded_for_test(
            HashMap::from([
                (0, scan_delta(1)),
                (1, scan_delta(2)),
                (2, OpNode::Union),
                (3, OpNode::IntegrateSink),
            ]),
            vec![(0, 2, PORT_IN_A), (1, 2, PORT_IN_B), (2, 3, PORT_IN)],
        ),
        "union of two scans",
    );

    // The cross-wiring broken: both joins trace against the SAME integral, so
    // no trace integrates `J_a`'s own delta port.
    let (nodes, mut edges) = equi_join_parts();
    edges.retain(|&(s, d, p)| !(s == 4 && d == 7 && p == PORT_TRACE));
    edges.push((5, 7, PORT_TRACE));
    rejected(
        loaded_for_test(nodes, edges),
        "trace port is not the other branch's delta integral",
    );

    // A join whose trace port is not an integral at all.
    let (mut nodes, edges) = equi_join_parts();
    nodes.insert(4, OpNode::Filter(None));
    rejected(loaded_for_test(nodes, edges), "trace port is not an integral");
}
