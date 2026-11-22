use super::*;
use crate::query::dag::tests::{engine_with_tables, union_view};
use crate::test_support::{register_identity_view, u64_i64_cols};

fn set(ids: &[u64]) -> FxHashSet<u64> {
    ids.iter().copied().collect()
}

/// A view's roots and the views a relation's tick schedule steps walk the same
/// edges transitively, over a three-view chain, a diamond and a disconnected
/// pair: a view has a root exactly when a tick of it reaches the view.
#[test]
fn roots_and_downstream_walk_both_directions_transitively() {
    // chain a → c1 → c2 → c3, diamond d → {d1, d2} → d3, disconnected pair p → p1.
    let (_dir, mut engine, a, [d, p]) = engine_with_tables(["d", "p"]);
    let cols = u64_i64_cols();
    let c1 = register_identity_view(&mut engine, a, "c1", &cols);
    let c2 = register_identity_view(&mut engine, c1, "c2", &cols);
    let c3 = register_identity_view(&mut engine, c2, "c3", &cols);
    let d1 = register_identity_view(&mut engine, d, "d1", &cols);
    let d2 = register_identity_view(&mut engine, d, "d2", &cols);
    let d3 = union_view(&mut engine, &[d1, d2], "d3");
    let p1 = register_identity_view(&mut engine, p, "p1", &cols);
    let dag = &mut engine.dag;
    // The views a schedule over `seeds` steps.
    let downstream = |dag: &DagEngine, seeds: &[u64]| -> FxHashSet<u64> {
        dag.schedule_from(seeds, Vec::new()).iter().map(|s| s.view).collect()
    };

    for (view, root) in [(c1, a), (c2, a), (c3, a), (d1, d), (d3, d), (p1, p)] {
        assert_eq!(dag.roots_of(view), [root], "view {view}");
    }
    assert!(dag.roots_of(a).is_empty());
    assert!(dag.roots_of(u64::MAX).is_empty());
    // The other direction, so a walk that read the wrong half of the graph
    // cannot pass both.
    assert_eq!(downstream(dag, &[a]), set(&[c1, c2, c3]));
    assert_eq!(downstream(dag, &[d]), set(&[d1, d2, d3]));
    assert_eq!(downstream(dag, &[c1]), set(&[c2, c3]));
    assert_eq!(downstream(dag, &[d1, d2]), set(&[d3]));
    assert!(downstream(dag, &[p1, u64::MAX]).is_empty());

    let views = [c1, c2, c3, d1, d2, d3, p1];
    for root in [a, d, p] {
        let rooted = |v: &u64| dag.roots_of(*v).contains(&root);
        let rooted: FxHashSet<u64> = views.into_iter().filter(rooted).collect();
        assert_eq!(rooted, downstream(dag, &[root]), "views under {root}");
        let ticked: FxHashSet<u64> = dag.tick_schedule(root).iter().map(|s| s.view).collect();
        assert_eq!(rooted, ticked, "a tick of {root}");
    }
}

/// The schedule names one step per dependency edge out of the source's forward
/// closure, ordered by view id — so a producer always precedes the steps it
/// feeds. Each step says whether it is its producer's last reader.
#[test]
fn the_schedule_names_every_edge_of_the_closure_in_id_order() {
    // `(view, producer, last)` of each step.
    let steps = |dag: &mut DagEngine, source| -> Vec<(u64, u64, bool)> {
        let steps = dag.tick_schedule(source);
        steps.iter().map(|s| (s.view, s.producer, s.last)).collect()
    };
    let (_dir, mut engine, t, [other]) = engine_with_tables(["other"]);
    let cols = u64_i64_cols();
    let v2 = register_identity_view(&mut engine, t, "v2", &cols);
    let v3 = register_identity_view(&mut engine, t, "v3", &cols);
    let v4 = union_view(&mut engine, &[v2, v3], "v4");
    let v5 = register_identity_view(&mut engine, v4, "v5", &cols);
    register_identity_view(&mut engine, other, "v7", &cols);

    let every = [
        (v2, t, false),
        (v3, t, true),
        (v4, v2, true),
        (v4, v3, true),
        (v5, v4, true),
    ];
    assert_eq!(steps(&mut engine.dag, t), every);

    // A view registered or dropped after a schedule was read moves that schedule.
    let v8 = register_identity_view(&mut engine, v5, "v8", &cols);
    let mut grown = every.to_vec();
    grown.push((v8, v5, true));
    assert_eq!(steps(&mut engine.dag, t), grown);
    engine.dag.forget(v4);
    assert_eq!(steps(&mut engine.dag, t), [(v2, t, false), (v3, t, true)]);
}

/// Steps relaying one producer by one scatter key share one round: the first
/// runs it, the last is told no later step reads it, and it folds where any of
/// them folds. Another key, or another producer, is a round of its own, and a
/// step its view does not relay reads none. Only a step that runs a round or
/// relays nothing reads the producer's own rows, so the last of those takes them.
#[test]
fn steps_relaying_one_producer_by_one_key_share_a_round() {
    let by_v = [(1, gnitz_wire::TypeCode::I64)];
    let by_id = [(0, gnitz_wire::TypeCode::U64)];
    // `(view, producer)` → the key the view's scan of the producer relays by.
    // View 3 folds what it gathers, so the round it shares gathers by merge.
    let relay_key = |view: u64, producer: u64| -> RelayKey<'_> {
        match (view, producer) {
            (2 | 5, 1) => Some((&by_v, false)),
            (3, 1) => Some((&by_v, true)),
            (4, 1) => Some((&by_id, false)),
            (5, 2) => Some((&by_v, false)),
            _ => None,
        }
    };
    let edges = vec![(2, 1), (3, 1), (4, 1), (5, 1), (5, 2), (6, 1)];
    let steps = schedule(edges, relay_key);
    let relays: Vec<_> = steps.iter().map(|s| s.relay).collect();
    let slot = |at, last, fold| Some(RelaySlot { at, last, fold });
    assert_eq!(
        relays,
        [
            slot(0, false, true),
            slot(0, false, true),
            slot(1, true, false),
            slot(0, true, true),
            slot(2, true, false),
            None
        ]
    );
    let last: Vec<bool> = steps.iter().map(|s| s.last).collect();
    assert_eq!(last, [false, false, false, false, true, true], "of producers 1 and 2");

    // With no direct reader behind them, the step running the shared round is
    // the last to read the producer's own rows.
    let steps = schedule(vec![(2, 1), (3, 1), (5, 1)], relay_key);
    let last: Vec<bool> = steps.iter().map(|s| s.last).collect();
    assert_eq!(last, [true, false, false]);
}
