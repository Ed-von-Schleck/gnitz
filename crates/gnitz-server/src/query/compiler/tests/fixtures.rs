//! Compiler test fixtures: hand-built circuits and the relation registry they
//! compile against. A child of `compiler`, so it reaches `LoadedCircuit`'s
//! private items.

use super::*;
use gnitz_store::relation::{OnRegister, RelationKind, RelationSpec, StoreConfig, ViewBudgets};
use gnitz_store::storage::Slot;

// Input slots reach the compiler only in hand-written fixtures; every production
// read of an operand goes through [`NodeInputs`]. Slot 0 is a unary operator's
// input and a binary one's delta/left operand, slot 1 the trace/right operand.
pub(in crate::query) const SLOT_IN: usize = 0;
pub(in crate::query) const SLOT_TRACE: usize = 1;

/// Fixture wiring: `(producer, consumer, slot)` triples into the per-node input
/// slots `load_circuit` reads out of a node row's `input_0`/`input_1` columns.
pub(in crate::query) fn wire_slots(edges: &[(i32, i32, usize)]) -> FxHashMap<i32, [Option<i32>; 2]> {
    let mut by_node: FxHashMap<i32, [Option<i32>; 2]> = FxHashMap::default();
    for &(src, dst, slot) in edges {
        by_node.entry(dst).or_default()[slot] = Some(src);
    }
    by_node
}

/// Build a `LoadedCircuit` from raw nodes/edges: the struct's fields are
/// private to `compiler`, so a test outside it cannot construct one.
pub(in crate::query) fn loaded_for_test(
    nodes: impl IntoIterator<Item = (i32, gnitz_wire::OpNode)>,
    edges: Vec<(i32, i32, usize)>,
) -> LoadedCircuit {
    load::topo_sorted(nodes.into_iter().collect(), wire_slots(&edges)).expect("test circuit must be a well-formed DAG")
}

/// The sub-pipeline producing `out`'s value, in topological order.
pub(in crate::query) fn subgraph_ordered(loaded: &LoadedCircuit, out: i32) -> Vec<i32> {
    let set = loaded.ancestors_inclusive(out);
    loaded.ordered_where(|n| set.contains(&n))
}

/// An unbounded delta scan — every fixture circuit's source shape.
pub(in crate::query) fn scan_delta(source: u64) -> gnitz_wire::OpNode {
    gnitz_wire::OpNode::ScanDelta { source, bound: None }
}

/// A `ScatterKey` reindex on `cols` — the routing walks' only variable. The
/// fixtures that vary `keep`, the promotion targets or the role spell the
/// variant out instead, so the field they turn on stays visible.
pub(in crate::query) fn scatter_reindex(cols: &[u32]) -> gnitz_wire::OpNode {
    gnitz_wire::OpNode::Map(gnitz_wire::MapKind::Reindex {
        keep: vec![0],
        key: cols.iter().map(|&c| (c, None)).collect(),
        role: gnitz_wire::ReindexRole::ScatterKey,
    })
}

/// An empty but decodable expr-program blob for tests that need a
/// `Map(Expression { program, .. })` or `Filter(..)` to exist without
/// ever executing it. Built through the real encoder rather than spelled as a
/// byte literal, so it stays decodable when the blob header changes.
pub(in crate::query) fn dummy_expr_blob() -> Vec<u8> {
    gnitz_expr::ExprBuilder::new()
        .build(None)
        .expect("a well-formed program")
        .to_blob_bytes()
}

/// The guard that rejected a build. Naming it is what makes a guard test
/// attributable: a bare `is_err()` also passes when an unrelated guard fires.
pub(in crate::query) fn rejection<T>(r: Result<T, String>) -> String {
    r.map(|_| "a plan").expect_err("expected a rejection")
}

/// Register each `(id, schema)` as a stream — a storeless kind, so a fixture
/// enters the schemas a compile looks up without opening a store.
pub(in crate::query) fn register_sources(
    registry: &mut RelationRegistry,
    rows: impl IntoIterator<Item = (i64, SchemaDescriptor)>,
) {
    for (id, schema) in rows {
        let spec = RelationSpec {
            id,
            kind: RelationKind::Stream,
            schema,
            directory: String::new(),
            budgets: ViewBudgets::default(),
        };
        registry
            .register(spec, OnRegister::Live)
            .expect("a stream registers without a store");
    }
}

/// A fresh single-worker registry holding only `rows`.
pub(in crate::query) fn sources(rows: impl IntoIterator<Item = (i64, SchemaDescriptor)>) -> RelationRegistry {
    let mut registry = RelationRegistry::new(Slot::SOLO, StoreConfig::default());
    register_sources(&mut registry, rows);
    registry
}
