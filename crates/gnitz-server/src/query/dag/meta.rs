//! The view-dependency graph, the closures over it, and the placement fold.

use super::*;
use gnitz_store::relation::Relation;
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
    ///
    /// Applies no `tables` kind filter, so a source absent from `tables` is still
    /// reported: the read-freshness test that drives this must not narrow its own
    /// input, or a dropped source would vanish from the closure and read as fresh.
    pub(crate) fn source_closure(&self, seeds: Vec<i64>) -> FxHashSet<i64> {
        DepMap::closure(&self.dep.reverse, seeds)
    }

    /// The other direction over the same edges: every view reachable from
    /// `seeds` by following `source → dependents`, with the seeds excluded — which
    /// views a tick of these sources reaches.
    pub(crate) fn dependent_closure(&self, seeds: Vec<i64>) -> FxHashSet<i64> {
        DepMap::closure(&self.dep.forward, seeds)
    }

    /// Transitive base-table sources of `seeds` (views), deduplicated and
    /// sorted: walk each seed's source chain — recursing through view sources —
    /// down to the base tables. The live CREATE-VIEW drain ticks exactly these
    /// (so every base feeding the new view, directly or through an existing view
    /// source, has its `pending_deltas` delivered to its existing dependents
    /// before the new view backfills); boot's recovery tick sweep drives every
    /// base reachable from *all* views through `drain_tick_blocking`. Sorted for a
    /// reproducible drive order.
    ///
    /// The `is_base_table` filter excludes a stream, so a new view over one is not
    /// preceded by a drain: it backfills from the stream's empty store and starts
    /// accumulating from its own registration. Whether a row pushed just before the
    /// CREATE lands in it therefore depends on whether its tick had already fired.
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

    /// Where a view's rows live — the value `Relation` stamps — folded from
    /// its `sources`' **stamped** placements. `pk_arity` is the view's own
    /// declared PK column count (it is not registered yet, so the arity cannot
    /// be read back off the registry).
    ///
    /// Reading the sources' stamped placement rather than re-deriving "has a
    /// replicated source" from the direct sources is what makes the property
    /// transitive: `hook_relation_register` registers a view after every view it
    /// scans, so each source's answer is already stamped when this runs.
    ///
    /// `Local` is safe whatever the circuit did — a read of a `Local` relation
    /// gathers every worker — where `Keyed` unicasts to the one its key names.
    pub(crate) fn view_placement(
        &mut self,
        registry: &RelationRegistry,
        view_id: i64,
        sources: &[i64],
        pk_arity: usize,
    ) -> Placement {
        // An unregistered source proves nothing, so it reads as the keyed default.
        let placed = |t: &i64| {
            registry
                .relation(*t)
                .map(Relation::schema)
                .map_or((Placement::KEYED_DEFAULT, 0), |s| (s.placement(), s.pk_indices().len()))
        };
        if sources.is_empty() {
            return Placement::KEYED_DEFAULT;
        }
        // Every source in full on every worker ⇒ the view computes its whole
        // result locally on every worker and the read single-sources worker 0.
        if sources.iter().all(|t| placed(t).0.is_replicated()) {
            return Placement::Replicated;
        }
        // Any source that is not key-routed places this view's rows on the worker
        // that produced them, not on the one its key names.
        if sources.iter().any(|t| !placed(t).0.is_key_routed()) {
            return Placement::Local;
        }

        let [src] = sources else {
            return Placement::KEYED_DEFAULT;
        };
        let (source_placement, source_pk_arity) = placed(src);
        // Last, because it can cost a circuit load where the tests above are map lookups.
        match self.view_meta(registry, view_id) {
            Ok(m) if m.places_rows_by_own_key() => Placement::KEYED_DEFAULT,
            Ok(m) if m.pk_source == Some(*src) && source_pk_arity == pk_arity => source_placement,
            _ => Placement::Local,
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "tests/meta.rs"]
mod tests;
