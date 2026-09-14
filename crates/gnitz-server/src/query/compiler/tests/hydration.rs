use super::*;
use gnitz_wire::{JoinKind, MapKind, OpNode};
use std::collections::HashMap;

/// The inner-equi-join shape `equi_prologue` produces, as node ids:
/// two `ScanDelta`s, a reindex `Map` and an `IntegrateTrace` per side, the
/// two cross-wired `Join(Equi)` terms behind their normalization maps,
/// a `Union`, a residual `Filter`, a projection `Map`, and the sink.
///
/// ```text
///   0 scanA → 2 reindexA ─┬─→ 4 traceA ──────────┐
///                         └─────────────┐        │
///   1 scanB → 3 reindexB ─┬─→ 5 traceB ─┼→ 6 J_ab│ (delta=2, trace=5)
///                         └─────────────┴────────┴→ 7 J_ba (delta=3, trace=4)
///   6 → 8 map → 10 union ← 9 map ← 7;  10 → 11 filter → 12 map → 13 sink
/// ```
fn equi_join(mutate: impl FnOnce(&mut HashMap<i32, OpNode>, &mut Vec<(i32, i32, usize)>)) -> LoadedCircuit {
    let m = || OpNode::Map(MapKind::Projection(vec![0]));
    let mut nodes = HashMap::from([
        (0, scan_delta(100)),
        (1, scan_delta(200)),
        (2, m()),
        (3, m()),
        (4, OpNode::IntegrateTrace),
        (5, OpNode::IntegrateTrace),
        (6, OpNode::Join(JoinKind::Equi)),
        (7, OpNode::Join(JoinKind::Equi)),
        (8, m()),
        (9, m()),
        (10, OpNode::Union),
        (11, OpNode::Filter(dummy_expr_blob())),
        (12, m()),
        (13, OpNode::IntegrateSink),
    ]);
    let mut edges = vec![
        (0, 2, SLOT_IN),
        (1, 3, SLOT_IN),
        (2, 4, SLOT_IN),
        (3, 5, SLOT_IN),
        (2, 6, SLOT_IN),
        (5, 6, SLOT_TRACE),
        (3, 7, SLOT_IN),
        (4, 7, SLOT_TRACE),
        (6, 8, SLOT_IN),
        (7, 9, SLOT_IN),
        (8, 10, SLOT_IN),
        (9, 10, SLOT_TRACE),
        (10, 11, SLOT_IN),
        (11, 12, SLOT_IN),
        (12, 13, SLOT_IN),
    ];
    mutate(&mut nodes, &mut edges);
    loaded_for_test(nodes, edges)
}

/// The seed is resolved by the *cross-wiring*, not by position: `J_a`'s trace
/// port is the other branch's integral, so the trace whose input is `J_a`'s
/// own delta port lives on the sibling join. Getting that backwards would
/// seed the replay from the wrong side and silently compute a different
/// product.
#[test]
fn hydration_seeds_from_the_cross_wired_trace() {
    // `d_a` is the reindex feeding `J_ab`'s delta port (node 2); `t_a` is the
    // trace that integrates *it* (node 4), which hangs off `J_ba`.
    assert_eq!(
        hydration_nodes(&equi_join(|_, _| {})).unwrap(),
        HydrationNodes::Join { d_a: 2, t_a: 4 }
    );
}

/// The linear shape resolves to its source relation, through any number of
/// filter/map nodes.
#[test]
fn hydration_of_a_linear_circuit_names_its_source() {
    let lc = loaded_for_test(
        [
            (0, scan_delta(77)),
            (1, OpNode::Filter(dummy_expr_blob())),
            (2, OpNode::Map(MapKind::Projection(vec![0]))),
            (3, OpNode::IntegrateSink),
        ],
        vec![(0, 1, SLOT_IN), (1, 2, SLOT_IN), (2, 3, SLOT_IN)],
    );
    assert_eq!(
        hydration_nodes(&lc).unwrap(),
        HydrationNodes::Relation { nid: 0, source: 77 }
    );
}

/// Every structural mismatch is a named `Rejected`, never a silent `None`: a
/// bounded view must not reach its store with no way to hydrate it. These are
/// the trust boundary behind the planner's own eligibility gate — no SQL
/// reaches them, which is exactly why they are asserted here. The two join
/// branches carry distinct messages, so a rejection says which one it came from.
#[test]
fn a_malformed_circuit_is_rejected_rather_than_guessed_at() {
    let rejected = |lc: LoadedCircuit, want: &str| match hydration_nodes(&lc) {
        Err(CompileError::Rejected(guard)) => assert_eq!(guard, want),
        other => panic!("expected {want:?}, got {:?}", other.map(|h| format!("{h:?}"))),
    };

    rejected(
        loaded_for_test([(0, scan_delta(1))], vec![]),
        "bounded view: circuit has no IntegrateSink",
    );
    // A shape the walk cannot replay (a Reduce under the sink).
    rejected(
        loaded_for_test(
            [
                (0, scan_delta(1)),
                (
                    1,
                    OpNode::Reduce {
                        group_cols: vec![0],
                        agg: vec![gnitz_wire::AggDescriptor {
                            agg_op: gnitz_wire::AggFunc::Count,
                            col_idx: 0,
                        }],
                        global_ground: false,
                    },
                ),
                (2, OpNode::IntegrateSink),
            ],
            vec![(0, 1, SLOT_IN), (1, 2, SLOT_IN)],
        ),
        "bounded view: unsupported circuit shape",
    );
    // A union whose inputs are not delta/trace joins.
    rejected(
        loaded_for_test(
            [
                (0, scan_delta(1)),
                (1, scan_delta(2)),
                (2, OpNode::Union),
                (3, OpNode::IntegrateSink),
            ],
            vec![(0, 2, SLOT_IN), (1, 2, SLOT_TRACE), (2, 3, SLOT_IN)],
        ),
        "bounded view: union input is not an inner delta/trace join",
    );

    // The cross-wiring broken: both joins trace against the SAME integral, so
    // no trace integrates `J_a`'s own delta port.
    rejected(
        equi_join(|_, edges| {
            edges.retain(|&(s, d, p)| !(s == 4 && d == 7 && p == SLOT_TRACE));
            edges.push((5, 7, SLOT_TRACE));
        }),
        "bounded view: the join's trace port is not the other branch's delta integral",
    );

    // A non-integral on either join's trace port, each naming its own branch.
    for (nid, want) in [
        (5, "bounded view: the seeded join's trace port is not an integral"),
        (4, "bounded view: the sibling join's trace port is not an integral"),
    ] {
        rejected(
            equi_join(|nodes, _| drop(nodes.insert(nid, OpNode::Filter(dummy_expr_blob())))),
            want,
        );
    }
}
