//! Plan-free view metadata: the dependency map, the memoized per-view routing
//! metadata and the placement query, for callers that must never compile — the
//! master's exchange-relay path (compiling creates rank-stamped scratch tables)
//! and boot-time classification, which runs before any plan exists.
//!
//! `DagEngine::preflight_compile` is the one master-side compile, exempt because
//! it redirects to a throwaway root; anything else the master needs routes here.

use super::*;
use gnitz_store::relation::Relation;
use std::collections::hash_map::Entry;
use std::rc::Rc;

/// The bidirectional view-dependency index, kept current by every `CircuitNodes` delta.
/// A `forward` / `reverse` entry exists only while it holds an id.
#[derive(Default)]
pub(super) struct DepMap {
    /// source_table_id → [view_ids]
    pub(in crate::query) forward: FxHashMap<i64, Vec<i64>>,
    /// view_id → [source_table_ids]
    pub(in crate::query) reverse: FxHashMap<i64, Vec<i64>>,
    /// Every `(view, source)` pair the two maps hold, so linking one is a hash probe.
    pub(in crate::query) edges: FxHashSet<(i64, i64)>,
}

impl DepMap {
    /// Apply one `CircuitNodes` delta. An edge is one `ScanDelta` row. A view's rows arrive
    /// with the bundle creating it and leave with its drop (the catalog refuses any other
    /// `-1`), so a `+1` links an edge once however many scans name it, and a `-1` forgets
    /// the whole view. Both are idempotent: a Stage-A compensation negates a batch even
    /// when that batch's ingest failed before its hook ran. Linear in the delta and the
    /// edges it removes.
    pub(super) fn apply(&mut self, batch: &Batch) {
        let mut gone: FxHashSet<i64> = FxHashSet::default();
        let mut touched: FxHashSet<i64> = FxHashSet::default();
        for i in batch.retracted_rows() {
            let view = compiler::read_circuit_node_row(batch, i).view_id as i64;
            if gone.insert(view) {
                for source in self.reverse.remove(&view).into_iter().flatten() {
                    self.edges.remove(&(view, source));
                    touched.insert(source);
                }
            }
        }
        for source in touched {
            if let Entry::Occupied(mut views) = self.forward.entry(source) {
                views.get_mut().retain(|v| !gone.contains(v));
                if views.get().is_empty() {
                    views.remove();
                }
            }
        }
        for i in batch.live_rows() {
            let row = compiler::read_circuit_node_row(batch, i);
            let Some(source) = row.scan_source().map(|s| s as i64) else {
                continue;
            };
            let view = row.view_id as i64;
            if self.edges.insert((view, source)) {
                self.reverse.entry(view).or_default().push(source);
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

    /// The forward (source → views) map.
    pub(crate) fn get_dep_map(&self) -> &FxHashMap<i64, Vec<i64>> {
        &self.dep.forward
    }

    /// Return all direct source table IDs for a view.
    pub(crate) fn get_source_ids(&self, view_id: i64) -> Vec<i64> {
        self.dep.reverse.get(&view_id).cloned().unwrap_or_default()
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
    ///
    /// Beside [`Self::source_closure`] rather than re-derived outside the crate
    /// off `get_dep_map`, because the two directions belong next to each other and
    /// only one of them was reachable from outside.
    pub(crate) fn dependent_closure(&self, seeds: Vec<i64>) -> FxHashSet<i64> {
        DepMap::closure(&self.dep.forward, seeds)
    }

    /// Whether any view scans `id` directly — one step of
    /// [`Self::dependent_closure`], and the one spelling of the test, so no
    /// caller has to know that a `forward` entry is never empty.
    pub(crate) fn has_dependents(&self, id: i64) -> bool {
        self.dep.forward.contains_key(&id)
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
    /// The `Local` arm is deliberately conservative — a view whose source is
    /// `Local` is `Local` even when its own exchange would re-key it. That
    /// direction is always safe (a replicated store holds every row; the read
    /// gathers all workers) and it avoids a second, subtler predicate for "does
    /// this exchange actually run at runtime".
    pub(crate) fn view_placement(
        &mut self,
        registry: &RelationRegistry,
        view_id: i64,
        sources: &[i64],
        pk_arity: usize,
    ) -> Placement {
        // An unregistered source cannot be proven replicated or local, so it reads
        // as the keyed default — the same answer the pre-fold `replicated` probe
        // gave for a missing entry. A sourceless view computes nothing from
        // anywhere and keeps that default too.
        let placement_of = |&t: &i64| {
            registry
                .relation(t)
                .map_or(Placement::KEYED_DEFAULT, |e| e.schema().placement())
        };
        if sources.is_empty() {
            return Placement::KEYED_DEFAULT;
        }
        // Every source in full on every worker ⇒ the view computes its whole
        // result locally on every worker and the read single-sources worker 0.
        if sources.iter().map(placement_of).all(|p| p.is_replicated()) {
            return Placement::Replicated;
        }
        // Any source that is not key-routed places this view's rows on the worker
        // that produced them, not on the one its key names.
        if sources.iter().map(placement_of).any(|p| !p.is_key_routed()) {
            return Placement::Local;
        }

        // Every source is `Keyed`. A single-source view that neither shards nor
        // joins re-emits that source's PK region verbatim, so it must address its
        // rows the way the source does. `pk_arity == |source PK|` stands in for
        // "the view's PK region **is** the source's", which the planner
        // guarantees.
        let [src] = sources else {
            return Placement::KEYED_DEFAULT;
        };
        let (placement, n) = registry
            .relation(*src)
            .map(Relation::schema)
            .map_or((Placement::KEYED_DEFAULT, 0), |s| (s.placement(), s.pk_indices().len()));
        if pk_arity != n {
            return Placement::KEYED_DEFAULT;
        }
        // Last, because it can cost a circuit load where the tests above are map
        // lookups. Its join term is a backstop: a planned join has two distinct
        // sources, so the destructure above has already returned. An unreadable
        // circuit proves nothing, so it keeps the default.
        match self.view_meta(registry, view_id) {
            Some(m) if !m.repartitions => placement,
            _ => Placement::KEYED_DEFAULT,
        }
    }

    // ── ViewMeta (plan-free routing metadata) ───────────────────────────

    /// The memoized per-view routing metadata, computed on first touch. Cheaper
    /// than full compilation: no code emission. `None` — an unreadable or
    /// unroutable circuit — is not memoized, so a later touch retries.
    pub(crate) fn view_meta(&mut self, registry: &RelationRegistry, view_id: i64) -> Option<Rc<ViewMeta>> {
        if let Some(m) = self.meta.get(&view_id) {
            return Some(m.clone());
        }
        let meta = Rc::new(ViewMeta::for_view(registry, view_id)?);
        self.meta.insert(view_id, meta.clone());
        Some(meta)
    }

    /// Drop the memoized metadata mentioning `id` — as the owning view, or as a
    /// join source of another view's map (a dropped relation can be either).
    /// Over-eviction is always safe: entries are recomputed on next touch.
    pub(super) fn evict_meta(&mut self, id: i64) {
        self.meta.remove(&id);
        self.meta.retain(|_, m| !m.routes_source(id));
    }
}

#[cfg(test)]
#[path = "tests/meta.rs"]
mod tests;
