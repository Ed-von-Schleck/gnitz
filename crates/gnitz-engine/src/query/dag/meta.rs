//! Plan-free view metadata: the dependency map and the per-view circuit
//! metadata (`ViewMeta`) consumed by callers that must never compile — the
//! master's exchange-relay path (compiling creates rank-stamped scratch
//! tables) and boot-time classification, which runs before any plan exists.
//!
//! The one master-side compile, `DagEngine::preflight_compile`, is exempt only
//! because it redirects the compile to a throwaway root, keeping rank 0's
//! stamped scratch names off worker 0's real paths. Anything else the master
//! needs still has to route through here.

use super::*;
use std::rc::Rc;

/// Per-view circuit metadata derived from one `load_meta_circuit` pass.
/// Everything a plan-free caller needs; eviction is one map `remove`.
#[derive(Default)]
pub struct ViewMeta {
    /// The sink-nearest `ExchangeShard`'s shard columns — the master relay's
    /// routing key — and `None` when the circuit carries no `ExchangeShard` at
    /// all. The two states are distinct: an ungrouped global aggregate shards on
    /// `∅`, a real exchange that funnels every row onto `worker_for_key(V₀)`.
    pub shard_cols: Option<Rc<[u32]>>,
    /// source table id → the one key the relay packs and routes that source's
    /// delta by. `None` for a source whose scan feeds several distinct reindex
    /// keys: no single pack key co-partitions with the trace sides, so the relay
    /// must refuse the round rather than route by a key nothing was stored under
    /// (see `compiler::load::scatter_key`). Absent = the source carries no
    /// reindex key at all, and the view's own `shard_cols` route it.
    pub join_shard_map: FxHashMap<i64, Option<JoinScatterKey>>,
    /// `Some(n_eq)` iff the view is a non-equi (range / band) join. Drives the
    /// master relay's eq-prefix scatter (`n_eq ≥ 1`) vs broadcast (`n_eq == 0`),
    /// and the worker dispatch's input-relay arm.
    pub range_join_n_eq: Option<u8>,
    /// The circuit carries a `Join` node.
    pub has_join: bool,
    /// The sources whose deltas must go through the join scatter: those carrying
    /// a join/group reindex key, minus those whose native distribution already
    /// matches it (or whose partner is replicated). Probed once per epoch on the
    /// multi-worker dispatch path, which is why it is `Fx`-hashed.
    pub scatter_sources: FxHashSet<i64>,
    /// The unary output `ExchangeShard` is a proven no-op (every row already on
    /// the worker owning its distribution key) — the output IPC is elided.
    pub skips_exchange: bool,
}

/// One source's join/group reindex scatter key: the columns the relay routes by
/// and the per-slot carried promotion targets, mirroring the trace-side reindex
/// Map slot-for-slot. Held pre-split because the relay reads the two halves
/// separately, once per relay round.
pub struct JoinScatterKey {
    pub cols: Rc<[u32]>,
    pub target_tcs: Rc<[u8]>,
}

impl JoinScatterKey {
    fn from_pairs(pairs: Vec<(u32, u8)>) -> JoinScatterKey {
        JoinScatterKey {
            cols: pairs.iter().map(|&(c, _)| c).collect(),
            target_tcs: pairs.iter().map(|&(_, t)| t).collect(),
        }
    }
}

impl ViewMeta {
    /// The answer for a circuit that could not be read or is cyclic: no exchange
    /// skip, no shard or join columns, no range join. Every metadata query then
    /// takes its conservative branch instead of walking a graph that is not there.
    /// Field-for-field the `Default`, and named so the call site reads as that
    /// decision rather than as an accident.
    pub(super) fn nothing_special() -> ViewMeta {
        ViewMeta::default()
    }

    /// Derive the metadata from an already-loaded circuit. The body behind
    /// `view_meta`'s memo miss.
    /// `ext_tables` is what the routing half needs and the circuit alone cannot
    /// supply: co-partitioning and the output-shard elision both test a shard key
    /// against a *source relation's* distribution prefix.
    pub(super) fn from_loaded(loaded: &compiler::LoadedCircuit, ext_tables: &compiler::ExtTables) -> ViewMeta {
        let compiler::ScatterRouting { keys, scatter_sources } = compiler::compute_scatter_routing(loaded, ext_tables);
        ViewMeta {
            shard_cols: compiler::output_exchange_shard(loaded).map(|(_, cols)| cols.into()),
            join_shard_map: keys
                .into_iter()
                .map(|(tid, key)| (tid, key.map(JoinScatterKey::from_pairs)))
                .collect(),
            range_join_n_eq: compiler::circuit_range_join_n_eq(loaded),
            has_join: loaded
                .nodes
                .values()
                .any(|op| matches!(op, gnitz_wire::OpNode::Join(_))),
            scatter_sources,
            skips_exchange: compiler::compute_skips_exchange(loaded, ext_tables),
        }
    }
}

/// The bidirectional view-dependency index with its validity flag bundled in, so
/// "valid but stale" is unreachable: only `get_or_rebuild` sets `valid` (after
/// repopulating both maps), and only `invalidate` clears it.
#[derive(Default)]
pub(super) struct DepMap {
    pub forward: FxHashMap<i64, Vec<i64>>, // source_table_id → [view_ids]
    pub reverse: FxHashMap<i64, Vec<i64>>, // view_id → [source_table_ids]
    pub valid: bool,
}

impl DepMap {
    pub(super) fn invalidate(&mut self) {
        self.valid = false;
    }

    /// Rebuild both maps from the CircuitNodes system table if stale and return
    /// the forward (source → views) map. An edge is one `ScanDelta` node, read
    /// through the same reader that builds the circuit, so the graph the
    /// scheduler walks names the sources the circuit actually scans.
    ///
    /// `nodes` is passed in (a Copy raw pointer) because the table lives on
    /// `DagEngine`; reading it through `self` here would double-borrow against
    /// the `&mut self.dep`.
    pub(super) fn get_or_rebuild(&mut self, nodes: *mut Table) -> &FxHashMap<i64, Vec<i64>> {
        if self.valid {
            return &self.forward;
        }
        self.forward.clear();
        self.reverse.clear();
        // A view scanning one source twice yields two nodes, and a source's
        // forward entries interleave across views — dedup with a seen set
        // instead of a per-row `Vec::contains` scan.
        let mut seen: FxHashSet<(i64, i64)> = FxHashSet::default();
        compiler::for_each_scan_edge(nodes, |v_id, dep_tid| {
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
    pub fn get_dep_map(&mut self) -> &FxHashMap<i64, Vec<i64>> {
        self.dep.get_or_rebuild(self.sys.nodes)
    }

    /// Return all direct source table IDs for a view.
    pub fn get_source_ids(&mut self, view_id: i64) -> Vec<i64> {
        self.get_dep_map();
        self.dep.reverse.get(&view_id).cloned().unwrap_or_default()
    }

    /// Every relation reachable from `seeds` by following `view → sources` edges
    /// — through view sources, down to the bases — with the seeds themselves
    /// excluded.
    ///
    /// Applies no `tables` kind filter, so a source absent from `tables` is still
    /// reported: the read-freshness test that drives this must not narrow its own
    /// input, or a dropped source would vanish from the closure and read as fresh.
    pub fn source_closure(&mut self, seeds: Vec<i64>) -> FxHashSet<i64> {
        self.get_dep_map();
        DepMap::closure(&self.dep.reverse, seeds)
    }

    /// The other direction over the same edges: every view reachable from
    /// `seeds` by following `source → dependents`, with the seeds excluded — which
    /// views a tick of these sources reaches.
    ///
    /// Beside [`Self::source_closure`] rather than re-derived outside the crate
    /// off `get_dep_map`, because the two directions belong next to each other and
    /// only one of them was reachable from outside.
    pub fn dependent_closure(&mut self, seeds: Vec<i64>) -> FxHashSet<i64> {
        self.get_dep_map();
        DepMap::closure(&self.dep.forward, seeds)
    }

    /// The distinct ids of `view_ids` in dependency order (Kahn's algorithm over
    /// `get_source_ids(vid) ∩ view_ids`): a source view precedes every dependent
    /// that scans it, so it is registered and backfilled first. Neither order a
    /// batch of views arrives in is dependency order — a DDL bundle carries the
    /// client's submission order, boot replay carries VIEW_TAB PK order — so this
    /// is where the order is established. Acyclic by construction; the no-progress
    /// fallback appends the remainder so a malformed input terminates instead of
    /// spinning.
    pub fn order_by_view_deps(&mut self, view_ids: &[i64]) -> Vec<i64> {
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
        self.get_dep_map();
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
    pub fn base_tables_reachable_from(&mut self, seeds: Vec<i64>) -> Vec<i64> {
        let mut bases: Vec<i64> = self
            .source_closure(seeds)
            .into_iter()
            .filter(|s| self.tables.get(s).is_some_and(|e| e.kind.is_base_table()))
            .collect();
        bases.sort_unstable();
        bases
    }

    /// A registered relation's wire class, or `None` for an unknown id — the
    /// shape a `FLAG_RESOLVE` descriptor reports. `RelClass` is `Copy`, so the
    /// `tables` borrow ends with the call.
    pub fn relation_class(&self, id: i64) -> Option<gnitz_wire::RelClass> {
        self.tables.get(&id).map(|e| e.class())
    }

    /// A registered relation's kind, or `None` for an unknown id. `RelationKind`
    /// is `Copy`, so the `tables` borrow ends with the call — callers may await
    /// on the result.
    pub fn relation_kind(&self, id: i64) -> Option<RelationKind> {
        self.tables.get(&id).map(|e| e.kind)
    }

    /// True iff `id`'s output is a full copy on every worker — read off the
    /// [`Placement`] stamped on its schema at registration. The one spelling of the
    /// replication probe, so the write broadcast, the read single-sourcing, and the
    /// store shape all read one answer.
    ///
    /// Any **gather** of a replicated relation must therefore single-source it,
    /// taking one worker's copy instead of N identical ones — both the scan
    /// dispatch and the exchange relay read this for that. SEEK already unicasts
    /// to one worker, so it needs no check.
    pub fn relation_is_replicated(&self, id: i64) -> bool {
        self.tables
            .get(&id)
            .is_some_and(|e| e.schema.placement().is_replicated())
    }

    /// True iff at least one registered view carries a delta feed. The master's
    /// idle-poll bookkeeping — the forward-closure walk and the last-round map —
    /// is skipped outright when this is false, which is every server that does not
    /// use the feature. A walk of the registry rather than a maintained counter:
    /// it runs once per emitted tick group, against a relation count in the tens,
    /// beside a SAL write and an eventfd.
    pub fn any_delta_feed(&self) -> bool {
        self.tables.values().any(|e| e.delta_bytes.is_some())
    }

    /// True iff `id` is a relation carrying a delta feed. Answered off the
    /// registry, so it is the same answer on the post-fork master — which holds no
    /// store — as on a worker.
    pub fn relation_has_delta_feed(&self, id: i64) -> bool {
        self.tables.get(&id).is_some_and(|e| e.delta_bytes.is_some())
    }

    /// Every registered view id.
    pub fn view_ids(&self) -> Vec<i64> {
        self.tables
            .iter()
            .filter(|(_, e)| e.kind.is_view())
            .map(|(&id, _)| id)
            .collect()
    }

    /// Where a view's rows live, folded from its `sources`' **stamped**
    /// placements. `pk_arity` is the view's own declared PK column count (it is
    /// not registered yet, so the arity cannot be read back off `self.tables`).
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
    pub(crate) fn view_placement(&mut self, view_id: i64, sources: &[i64], pk_arity: usize) -> Placement {
        // An unregistered source cannot be proven replicated or local, so it reads
        // as the keyed default — the same answer the pre-fold `replicated` probe
        // gave for a missing entry. A sourceless view computes nothing from
        // anywhere and keeps that default too.
        let placement_of = |t: &i64| {
            self.tables
                .get(t)
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
        // joins re-emits that source's PK region verbatim, so its rows sit on the
        // worker owning the *source's* distribution prefix and it must address
        // them the same way. `pk_arity == |source PK|` stands in for "the view's
        // PK region **is** the source's", which the planner's PK placement
        // guarantees.
        //
        // The `has_join` term is a backstop, not a live case: a planned join has
        // two distinct sources (the self-join guard rejects one source feeding
        // both inputs), so the single-source destructure below already returns.
        // It stays because an equi-join carries no `ExchangeShard` yet
        // repartitions its inputs through the runtime join-shard scatter, so
        // `shard_cols` alone would not catch one.
        //
        // The arity test comes first: it is a map lookup, where the circuit
        // metadata can cost a load.
        let [src] = sources else {
            return Placement::KEYED_DEFAULT;
        };
        // One lookup for both facts, which also ends `placement_of`'s borrow of
        // `self` before `view_meta` needs it mutably.
        let (placement, n) = self.tables.get(src).map_or((Placement::KEYED_DEFAULT, 0), |e| {
            (e.schema.placement(), e.schema.pk_indices().len())
        });
        if pk_arity != n {
            return Placement::KEYED_DEFAULT;
        }
        let meta = self.view_meta(view_id);
        if meta.shard_cols.is_none() && !meta.has_join {
            placement
        } else {
            Placement::KEYED_DEFAULT
        }
    }

    // ── ViewMeta (plan-free circuit metadata) ───────────────────────────

    /// Load typed circuit nodes/edges for metadata queries. Cheaper than full
    /// compilation: no optimization passes, no code emission. `None` for a
    /// circuit that cannot be read or is cyclic — one that cannot compile or
    /// execute either (`compile_view` rejects it the same way).
    pub(super) fn load_meta_circuit(&self, view_id: i64) -> Option<compiler::LoadedCircuit> {
        compiler::load_circuit(self.sys, view_id as u64, SchemaDescriptor::default()).ok()
    }

    /// The memoized per-view circuit metadata, computed from ONE
    /// `load_meta_circuit` pass on first touch.
    pub fn view_meta(&mut self, view_id: i64) -> Rc<ViewMeta> {
        if let Some(m) = self.meta.get(&view_id) {
            return m.clone();
        }
        let ext = self.ext_tables();
        let meta = Rc::new(
            self.load_meta_circuit(view_id)
                .as_ref()
                .map_or_else(ViewMeta::nothing_special, |l| ViewMeta::from_loaded(l, &ext)),
        );
        self.meta.insert(view_id, meta.clone());
        meta
    }

    /// Drop the memoized metadata mentioning `id` — as the owning view, or as a
    /// join source of another view's map (a dropped relation can be either).
    /// Over-eviction is always safe: entries are recomputed on next touch.
    pub(super) fn evict_meta(&mut self, id: i64) {
        self.meta.remove(&id);
        self.meta.retain(|_, m| !m.join_shard_map.contains_key(&id));
    }
}
