use super::*;
use crate::query::compiler::{scan_delta, scatter_reindex, ExtTables, PORT_IN};
use gnitz_wire::OpNode;
use std::collections::HashMap;

/// A `ViewMeta` whose relay routing names `src` — the shape `evict_meta`'s
/// `retain` predicate keys on.
fn meta_scanning(src: i64) -> ViewMeta {
    let loaded = compiler::loaded_for_test(
        HashMap::from([
            (0, scan_delta(src as u64)),
            (1, scatter_reindex(&[0])),
            (2, OpNode::IntegrateSink),
        ]),
        vec![(0, 1, PORT_IN), (1, 2, PORT_IN)],
    );
    ViewMeta::derive(&loaded, &ExtTables::default()).expect("fixture routes")
}

/// `evict_meta` must drop the metadata mentioning the id as the owning view OR as
/// a source of another view's routing — a dropped relation can be either, and a
/// stale entry would disagree with the live circuit (over-eviction is safe;
/// entries are recomputed on next touch).
#[test]
fn evict_meta_drops_the_view_and_every_view_routing_from_it() {
    let mut dag = DagEngine::new();
    dag.meta.insert(42, Rc::new(meta_scanning(7)));
    dag.meta.insert(7, Rc::new(meta_scanning(42)));
    dag.evict_meta(42);
    assert!(!dag.meta.contains_key(&42), "the owning view");
    assert!(
        !dag.meta.contains_key(&7),
        "a view whose routing names the id as a source"
    );
}
