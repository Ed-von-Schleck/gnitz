//! The view-dependency graph and the closures over it.

use super::*;
use std::collections::hash_map::Entry;

/// The bidirectional view-dependency index, kept current by every `CircuitNodes` delta.
/// A `forward` / `reverse` entry exists only while it holds an id.
#[derive(Default)]
pub(super) struct DepMap {
    /// source_table_id → [view_ids]
    pub(super) forward: FxHashMap<i64, Vec<i64>>,
    /// view_id → [source_table_ids]
    pub(super) reverse: FxHashMap<i64, Vec<i64>>,
}

impl DepMap {
    /// Apply one `CircuitNodes` delta. An edge is one `ScanDelta` row. A view's rows arrive
    /// with the bundle creating it and leave with its drop (the catalog refuses any other
    /// `-1`), so a `+1` links an edge once however many scans name it, and a `-1` forgets
    /// the whole view. Both are idempotent: a Stage-A compensation negates a batch even
    /// when that batch's ingest failed before its hook ran.
    pub(super) fn apply(&mut self, batch: &Batch) {
        for i in batch.retracted_rows() {
            let view = compiler::read_circuit_node_row(batch, i).view_id as i64;
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
            let Some(source) = row.scan_source().map(|s| s as i64) else {
                continue;
            };
            let view = row.view_id as i64;
            let srcs = self.reverse.entry(view).or_default();
            if !srcs.contains(&source) {
                srcs.push(source);
                self.forward.entry(source).or_default().push(view);
            }
        }
    }

    /// Transitive closure of `seeds` over one half of the map, seeds excluded.
    /// Both directions are this one walk, so they cannot drift: `forward` reaches
    /// a source's dependents, `reverse` reaches a view's sources, and
    /// `apply` writes both halves from the same `ScanDelta` node.
    pub(super) fn closure(edges: &FxHashMap<i64, Vec<i64>>, seeds: Vec<i64>) -> FxHashSet<i64> {
        let mut reachable: FxHashSet<i64> = FxHashSet::default();
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

    /// The views that scan `id` directly. Empty when none do.
    pub(crate) fn dependents_of(&self, id: i64) -> &[i64] {
        self.dep.forward.get(&id).map_or(&[], Vec::as_slice)
    }

    /// The relations `view_id` scans directly. Empty when it scans none.
    pub(crate) fn sources_of(&self, view_id: i64) -> &[i64] {
        self.dep.reverse.get(&view_id).map_or(&[], Vec::as_slice)
    }

    /// Every relation reachable from `seeds` by following `view → sources` edges
    /// — through view sources, down to the bases — with the seeds themselves
    /// excluded.
    pub(crate) fn source_closure(&self, seeds: Vec<i64>) -> FxHashSet<i64> {
        DepMap::closure(&self.dep.reverse, seeds)
    }

    /// The other direction over the same edges: every view reachable from
    /// `seeds` by following `source → dependents`, with the seeds excluded — which
    /// views a tick of these sources reaches.
    pub(crate) fn dependent_closure(&self, seeds: Vec<i64>) -> FxHashSet<i64> {
        DepMap::closure(&self.dep.forward, seeds)
    }

    /// The base tables — not streams — that `seeds`' source chains reach through
    /// view sources, sorted.
    pub(crate) fn base_tables_reachable_from(&self, registry: &RelationRegistry, seeds: Vec<i64>) -> Vec<i64> {
        let mut bases: Vec<i64> = self
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
