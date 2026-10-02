use super::*;

/// Forgetting a relation drops its pending rebuild.
#[test]
fn forgetting_a_view_drops_its_pending_rebuild() {
    let mut dag = DagEngine::default();
    dag.set_rebuild([200].into_iter().collect());
    assert!(dag.awaits_rebuild(200));
    dag.forget(200);
    assert!(!dag.awaits_rebuild(200));
}
