//! The view-dependency graph and the closures over it.

use super::*;
use std::collections::hash_map::Entry;

/// The bidirectional view-dependency index, kept current by every `CircuitNodes` delta.
/// A `forward` / `reverse` entry exists only while it holds an id.
#[derive(Default)]
pub(super) struct DepMap {
    /// source_table_id → [view_ids]
    forward: FxHashMap<u64, Vec<u64>>,
    /// view_id → [source_table_ids]
    reverse: FxHashMap<u64, Vec<u64>>,
}

impl DepMap {
    /// Apply one `CircuitNodes` delta. An edge is one `ScanDelta` row. A view's rows arrive
    /// with the bundle creating it and leave with its drop (the catalog refuses any other
    /// `-1`), so a `+1` links an edge once however many scans name it, and a `-1` forgets
    /// the whole view. Both are idempotent: a Stage-A compensation negates a batch even
    /// when that batch's ingest failed before its hook ran.
    fn apply(&mut self, batch: &Batch) {
        for i in batch.retracted_rows() {
            let view = compiler::read_circuit_node_row(batch, i).view_id;
            for source in self.reverse.remove(&view).into_iter().flatten() {
                if let Entry::Occupied(mut views) = self.forward.entry(source) {
                    views.get_mut().retain(|&v| v != view);
                    if views.get().is_empty() {
                        views.remove();
                    }
                }
            }
        }
        for i in batch.live_rows() {
            let row = compiler::read_circuit_node_row(batch, i);
            let Some(source) = row.scan_source() else {
                continue;
            };
            let view = row.view_id;
            let srcs = self.reverse.entry(view).or_default();
            if !srcs.contains(&source) {
                srcs.push(source);
                self.forward.entry(source).or_default().push(view);
            }
        }
    }

    /// Every id one or more edges from some seed over one half of the map — a
    /// seed only when another seed reaches it. Both directions are this one walk, so they cannot drift: `forward` reaches
    /// a source's dependents, `reverse` reaches a view's sources, and
    /// `apply` writes both halves from the same `ScanDelta` node.
    fn closure(edges: &FxHashMap<u64, Vec<u64>>, seeds: Vec<u64>) -> FxHashSet<u64> {
        let mut reachable: FxHashSet<u64> = FxHashSet::default();
        let mut stack = seeds;
        while let Some(id) = stack.pop() {
            for &next in edges.get(&id).into_iter().flatten() {
                if reachable.insert(next) {
                    stack.push(next);
                }
            }
        }
        reachable
    }
}

impl DagEngine {
    // ── Dependency map ──────────────────────────────────────────────────

    /// Apply one `CircuitNodes` delta to the dependency map.
    pub(crate) fn apply_circuit_delta(&mut self, batch: &Batch) {
        self.dep.apply(batch);
    }

    /// The views that scan `id` directly. Empty when none do.
    pub(crate) fn dependents_of(&self, id: u64) -> &[u64] {
        self.dep.forward.get(&id).map_or(&[], Vec::as_slice)
    }

    /// Whether any view scans `id`.
    pub(crate) fn is_scanned(&self, id: u64) -> bool {
        !self.dependents_of(id).is_empty()
    }

    /// The relations `view_id` scans directly. Empty when it scans none.
    pub(crate) fn sources_of(&self, view_id: u64) -> &[u64] {
        self.dep.reverse.get(&view_id).map_or(&[], Vec::as_slice)
    }

    /// Every relation one or more `view → sources` edges from some seed —
    /// through view sources, down to the bases.
    pub(crate) fn source_closure(&self, seeds: Vec<u64>) -> FxHashSet<u64> {
        DepMap::closure(&self.dep.reverse, seeds)
    }

    /// The other direction over the same edges: every view one or more
    /// `source → dependents` edges from some seed — which views a tick of these
    /// sources reaches.
    pub(crate) fn dependent_closure(&self, seeds: Vec<u64>) -> FxHashSet<u64> {
        DepMap::closure(&self.dep.forward, seeds)
    }

    /// The base tables — not streams — that `seeds`' source chains reach through
    /// view sources, sorted.
    pub(crate) fn base_tables_reachable_from(&self, registry: &RelationRegistry, seeds: Vec<u64>) -> Vec<u64> {
        let mut bases: Vec<u64> = self
            .source_closure(seeds)
            .into_iter()
            .filter(|&s| {
                registry
                    .relation(s)
                    .map(Relation::kind)
                    .is_some_and(|k| k.is_base_table())
            })
            .collect();
        bases.sort_unstable();
        bases
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "tests/meta.rs"]
mod tests;
