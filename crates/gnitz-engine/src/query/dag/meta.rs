//! Plan-free view metadata: the dependency map, the memoized per-view routing
//! metadata and the placement query, for callers that must never compile — the
//! master's exchange-relay path (compiling creates rank-stamped scratch tables)
//! and boot-time classification, which runs before any plan exists.
//!
//! `DagEngine::preflight_compile` is the one master-side compile, exempt because
//! it redirects to a throwaway root; anything else the master needs routes here.

use super::*;
use std::rc::Rc;

/// The bidirectional view-dependency index with its validity flag bundled in, so
/// "valid but stale" is unreachable: only `get_or_rebuild` sets `valid` (after
/// repopulating both maps), and only `invalidate` clears it.
#[derive(Default)]
pub(super) struct DepMap {
    /// source_table_id → [view_ids]. An entry exists only because an edge was
    /// pushed into it, so a present entry is never empty.
    pub forward: FxHashMap<i64, Vec<i64>>,
    pub reverse: FxHashMap<i64, Vec<i64>>, // view_id → [source_table_ids]
    pub valid: bool,
}

impl DepMap {
    /// Both maps are a pure function of the `CircuitNodes` system table, so only
    /// a writer of that table has dirtied them — the catalog ingest hook, and a
    /// dropped base table whose scan edges outlive it.
    pub(super) fn invalidate(&mut self) {
        self.valid = false;
    }

    /// Rebuild both maps from the CircuitNodes system table if stale and return
    /// the forward (source → views) map. An edge is one `ScanDelta` node, read
    /// through the same reader that builds the circuit, so the graph the
    /// scheduler walks names the sources the circuit actually scans.
    ///
    /// The registry is a disjoint parameter rather than a field of the engine, so
    /// reading the circuit table through it does not double-borrow against
    /// `&mut self.dep`.
    pub(super) fn get_or_rebuild(&mut self, registry: &RelationRegistry) -> &FxHashMap<i64, Vec<i64>> {
        if self.valid {
            return &self.forward;
        }
        self.forward.clear();
        self.reverse.clear();
        // A view scanning one source twice yields two nodes, and a source's
        // forward entries interleave across views — dedup with a seen set
        // instead of a per-row `Vec::contains` scan.
        let mut seen: FxHashSet<(i64, i64)> = FxHashSet::default();
        compiler::for_each_scan_edge(registry, |v_id, dep_tid| {
            if seen.insert((v_id, dep_tid)) {
                self.forward.entry(dep_tid).or_default().push(v_id);
                self.reverse.entry(v_id).or_default().push(dep_tid);
            }
        });
        self.valid = true;
        &self.forward
    }

    /// Transitive closure of `seeds` over one half of the map, seeds excluded.
    /// Both directions are this one walk, so they cannot drift: `forward` reaches
    /// a source's dependents, `reverse` reaches a view's sources, and
    /// `get_or_rebuild` writes both halves from the same `ScanDelta` node.
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

    /// Rebuild the dependency maps from the CircuitNodes system table if stale
    /// and return the forward (source → views) map.
    pub fn get_dep_map(&mut self, registry: &RelationRegistry) -> &FxHashMap<i64, Vec<i64>> {
        self.dep.get_or_rebuild(registry)
    }

    /// Return all direct source table IDs for a view.
    pub fn get_source_ids(&mut self, registry: &RelationRegistry, view_id: i64) -> Vec<i64> {
        self.get_dep_map(registry);
        self.dep.reverse.get(&view_id).cloned().unwrap_or_default()
    }

    /// Every relation reachable from `seeds` by following `view → sources` edges
    /// — through view sources, down to the bases — with the seeds themselves
    /// excluded.
    ///
    /// Applies no `tables` kind filter, so a source absent from `tables` is still
    /// reported: the read-freshness test that drives this must not narrow its own
    /// input, or a dropped source would vanish from the closure and read as fresh.
    pub fn source_closure(&mut self, registry: &RelationRegistry, seeds: Vec<i64>) -> FxHashSet<i64> {
        self.get_dep_map(registry);
        DepMap::closure(&self.dep.reverse, seeds)
    }

    /// The other direction over the same edges: every view reachable from
    /// `seeds` by following `source → dependents`, with the seeds excluded — which
    /// views a tick of these sources reaches.
    ///
    /// Beside [`Self::source_closure`] rather than re-derived outside the crate
    /// off `get_dep_map`, because the two directions belong next to each other and
    /// only one of them was reachable from outside.
    pub fn dependent_closure(&mut self, registry: &RelationRegistry, seeds: Vec<i64>) -> FxHashSet<i64> {
        self.get_dep_map(registry);
        DepMap::closure(&self.dep.forward, seeds)
    }

    /// Whether any view scans `id` directly — one step of
    /// [`Self::dependent_closure`], and the one spelling of the test, so no
    /// caller has to know that a `forward` entry is never empty.
    pub fn has_dependents(&mut self, registry: &RelationRegistry, id: i64) -> bool {
        self.get_dep_map(registry).contains_key(&id)
    }

    /// The distinct ids of `view_ids` in dependency order (Kahn's algorithm over
    /// `get_source_ids(vid) ∩ view_ids`): a source view precedes every dependent
    /// that scans it, so it is registered and backfilled first. Neither order a
    /// batch of views arrives in is dependency order — a DDL bundle carries the
    /// client's submission order, boot replay carries VIEW_TAB PK order — so this
    /// is where the order is established. Acyclic by construction; the no-progress
    /// fallback appends the remainder so a malformed input terminates instead of
    /// spinning.
    pub fn order_by_view_deps(&mut self, registry: &RelationRegistry, view_ids: &[i64]) -> Vec<i64> {
        let mut ids: Vec<i64> = Vec::with_capacity(view_ids.len());
        let mut bundle: FxHashSet<i64> = FxHashSet::default();
        for &vid in view_ids {
            if bundle.insert(vid) {
                ids.push(vid);
            }
        }
        if ids.len() <= 1 {
            return ids;
        }
        self.get_dep_map(registry);
        let reverse = &self.dep.reverse;
        let mut emitted: FxHashSet<i64> = FxHashSet::default();
        let mut order: Vec<i64> = Vec::with_capacity(ids.len());
        while order.len() < ids.len() {
            let before = order.len();
            for &vid in &ids {
                let ready = reverse
                    .get(&vid)
                    .into_iter()
                    .flatten()
                    .all(|s| *s == vid || !bundle.contains(s) || emitted.contains(s));
                if ready && emitted.insert(vid) {
                    order.push(vid);
                }
            }
            if order.len() == before {
                debug_assert!(false, "cycle in view dependencies: {ids:?}");
                order.extend(ids.iter().copied().filter(|v| !emitted.contains(v)));
            }
        }
        order
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
    pub fn base_tables_reachable_from(&mut self, registry: &RelationRegistry, seeds: Vec<i64>) -> Vec<i64> {
        let mut bases: Vec<i64> = self
            .source_closure(registry, seeds)
            .into_iter()
            .filter(|&s| registry.relation_kind(s).is_some_and(|k| k.is_base_table()))
            .collect();
        bases.sort_unstable();
        bases
    }

    /// Where a view's rows live and how far above the bases it sits — the two
    /// values `TableEntry` stamps, both folded from its `sources`' own stamped
    /// entries, so the scheduling key is derived here rather than a second time
    /// at the registering caller.
    pub(crate) fn view_placement(
        &mut self,
        registry: &RelationRegistry,
        view_id: i64,
        sources: &[i64],
        pk_arity: usize,
    ) -> (Placement, i32) {
        let depth = sources
            .iter()
            .filter_map(|&id| registry.entry(id))
            .map(|e| e.depth + 1)
            .max()
            .unwrap_or(0);
        (self.source_placement(registry, view_id, sources, pk_arity), depth)
    }

    /// Where a view's rows live, folded from its `sources`' **stamped**
    /// placements. `pk_arity` is the view's own declared PK column count (it is
    /// not registered yet, so the arity cannot be read back off the registry).
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
    fn source_placement(
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
                .entry(t)
                .map_or(Placement::KEYED_DEFAULT, |e| e.schema.placement())
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
            .get_schema_desc(*src)
            .map_or((Placement::KEYED_DEFAULT, 0), |s| (s.placement(), s.pk_indices().len()));
        if pk_arity != n {
            return Placement::KEYED_DEFAULT;
        }
        // Last, because it can cost a circuit load where the tests above are map
        // lookups. Its join term is a backstop: a planned join has two distinct
        // sources, so the destructure above has already returned.
        if !self.view_meta(registry, view_id).repartitions {
            placement
        } else {
            Placement::KEYED_DEFAULT
        }
    }

    // ── ViewMeta (plan-free routing metadata) ───────────────────────────

    /// The memoized per-view routing metadata, computed on first touch. Cheaper
    /// than full compilation: no code emission.
    pub fn view_meta(&mut self, registry: &RelationRegistry, view_id: i64) -> Rc<ViewMeta> {
        if let Some(m) = self.meta.get(&view_id) {
            return m.clone();
        }
        let meta = Rc::new(ViewMeta::for_view(registry, view_id));
        self.meta.insert(view_id, meta.clone());
        meta
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
