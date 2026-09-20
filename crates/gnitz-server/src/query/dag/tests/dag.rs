use super::*;

/// A plan-less memo, the shape `view_meta` leaves behind.
fn memo() -> Box<ViewEntry> {
    Box::new(ViewEntry { meta: ViewMeta::empty(), plan: None })
}

/// Wiring: the production table/view-drop path drops the memo with the relation.
#[test]
fn unregister_table_evicts_the_dropped_relations_memo() {
    let mut registry = gnitz_store::relation::RelationRegistry::new(
        gnitz_store::storage::Slot::SOLO,
        gnitz_store::relation::StoreConfig::default(),
    );
    let mut dag = DagEngine::new();
    dag.views.insert(43, memo());
    dag.unregister_table(&mut registry, 43);
    assert!(!dag.views.contains_key(&43), "unregister_table must evict");
}
