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

/// A view links to each source once however many scans name it, and a view
/// that scans nothing links nothing. Forgetting a view drops its edges and its
/// pending rebuild, leaves a sibling over the same source linked, keeps no
/// emptied entry, and is a no-op when repeated or for an id never linked.
#[test]
fn forgetting_a_view_drops_only_what_it_linked() {
    let mut dag = DagEngine::default();
    dag.dep.link(5, [1, 1, 2]);
    dag.dep.link(6, [1]);
    dag.dep.link(7, []);
    assert_eq!(dag.sources_of(5), [1, 2]);
    assert_eq!(dag.dependents_of(1), [5, 6]);
    assert_eq!(dag.dependents_of(2), [5]);
    assert!(!dag.dep.reverse.contains_key(&7));
    dag.rebuild = [5].into_iter().collect();

    for _ in 0..2 {
        dag.forget(5);
        dag.forget(99);
        assert!(dag.sources_of(5).is_empty() && !dag.awaits_rebuild(5));
        assert_eq!(dag.dependents_of(1), [6]);
        assert_eq!(dag.sources_of(6), [1]);
        assert!(!dag.dep.forward.contains_key(&2));
    }
    dag.forget(6);
    assert!(dag.dep.forward.is_empty() && dag.dep.reverse.is_empty());
}

/// A tick of a relation drives a view while one that scans it does not await
/// its rebuild.
#[test]
fn a_relation_is_ticked_while_a_view_scanning_it_is_not_awaiting_its_rebuild() {
    let mut dag = DagEngine::default();
    dag.dep.link(5, [1]);
    dag.dep.link(6, [1]);
    assert!(dag.is_ticked(1) && !dag.is_ticked(2));

    dag.rebuild = [5].into_iter().collect();
    assert!(dag.is_ticked(1));
    dag.rebuild = [5, 6].into_iter().collect();
    assert!(!dag.is_ticked(1) && dag.is_scanned(1));
}

/// The schedule names one step per dependency edge out of the source's forward
/// closure, ordered by view id — so a producer always precedes the steps it
/// feeds — and leaves out the views awaiting a rebuild. Each step says whether
/// it is its producer's last reader and whether a later one reads its view, of
/// the steps the schedule runs.
#[test]
fn the_schedule_names_every_edge_of_the_closure_in_id_order() {
    // `(view, producer, last, needed)` of each step.
    let steps = |dag: &mut DagEngine| -> Vec<(u64, u64, bool, bool)> {
        let steps = dag.tick_schedule(1);
        steps.iter().map(|s| (s.view, s.producer, s.last, s.needed)).collect()
    };
    let mut dag = DagEngine::default();
    for (view, sources) in [(2, &[1][..]), (3, &[1]), (4, &[2, 3]), (5, &[4]), (7, &[6])] {
        dag.dep.link(view, sources.iter().copied());
    }

    let every = [
        (2, 1, false, true),
        (3, 1, true, true),
        (4, 2, true, true),
        (4, 3, true, true),
        (5, 4, true, false),
    ];
    assert_eq!(steps(&mut dag), every);

    // A rebuild set is closed under dependents.
    dag.rebuild = [2, 4, 5].into_iter().collect();
    assert_eq!(steps(&mut dag), [(3, 1, true, false)], "of the steps it runs");
    dag.rebuild.remove(&2);
    assert_eq!(steps(&mut dag), [(2, 1, false, false), (3, 1, true, false)]);

    // A view linked or dropped after a schedule was read moves that schedule.
    dag.take_rebuild();
    dag.dep.link(8, [5]);
    let mut grown = every.to_vec();
    grown[4].3 = true;
    grown.push((8, 5, true, false));
    assert_eq!(steps(&mut dag), grown);
    dag.dep.unlink(4);
    assert_eq!(steps(&mut dag), [(2, 1, false, false), (3, 1, true, false)]);
}
