//! Plan-free view metadata: the dependency map and the per-view circuit
//! metadata (`ViewMeta`) consumed by callers that must never compile — the
//! master's exchange-relay path (compiling creates rank-stamped scratch
//! tables) and boot-time classification, which runs before any plan exists.

use super::*;
use std::rc::Rc;

/// Per-view circuit metadata derived from one `load_meta_circuit` pass.
/// Everything a plan-free caller needs; eviction is one map `remove`.
pub(super) struct ViewMeta {
    /// The output `ExchangeShard`'s shard columns (empty when the view has none).
    pub shard_cols: Rc<[i32]>,
    /// source table id → join/group reindex `(column, carried promotion tc)`
    /// pairs — the scatter key per source, mirroring the trace-side reindex
    /// Map slot-for-slot.
    pub join_shard_map: FxHashMap<i64, Rc<[(i32, u8)]>>,
    /// `Some(n_eq)` iff the view is a non-equi (range / band) join. Drives the
    /// master relay's eq-prefix scatter (`n_eq ≥ 1`) vs broadcast (`n_eq == 0`).
    pub range_join_n_eq: Option<u8>,
    /// The circuit carries an `ExchangeShard` node.
    pub needs_exchange: bool,
    /// The circuit carries a `Join` node.
    pub has_join: bool,
}

impl ViewMeta {
    /// Derive the metadata from an already-loaded circuit. The body behind
    /// `view_meta`'s memo miss.
    ///
    /// `loaded` MUST already be `topo_sort`ed: `compute_join_shard_map` walks
    /// `loaded.outgoing`, which only `topo_sort` populates.
    pub(super) fn from_loaded(loaded: &compiler::LoadedCircuit) -> ViewMeta {
        let shard_cols: Rc<[i32]> = loaded
            .nodes
            .values()
            .find_map(|op| match op {
                gnitz_wire::OpNode::ExchangeShard { shard_cols } => {
                    Some(shard_cols.iter().map(|&c| c as i32).collect::<Vec<_>>())
                }
                _ => None,
            })
            .unwrap_or_default()
            .into();
        let join_shard_map: FxHashMap<i64, Rc<[(i32, u8)]>> = compiler::compute_join_shard_map(loaded)
            .into_iter()
            .map(|(tid, cols)| (tid, cols.into()))
            .collect();
        ViewMeta {
            shard_cols,
            join_shard_map,
            range_join_n_eq: compiler::circuit_range_join_n_eq(loaded),
            needs_exchange: loaded
                .nodes
                .values()
                .any(|op| matches!(op, gnitz_wire::OpNode::ExchangeShard { .. })),
            has_join: loaded
                .nodes
                .values()
                .any(|op| matches!(op, gnitz_wire::OpNode::Join(_))),
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
    pub fn invalidate(&mut self) {
        self.valid = false;
    }

    /// Rebuild both maps from the DepTab system table if stale and return the
    /// forward (source → views) map. `dep_tab` is passed in (a Copy raw pointer)
    /// because the table lives on `DagEngine`; reading it through `self` here would
    /// double-borrow against the `&mut self.dep`.
    pub fn get_or_rebuild(&mut self, dep_tab: *mut Table) -> &FxHashMap<i64, Vec<i64>> {
        if self.valid {
            return &self.forward;
        }
        self.forward.clear();
        self.reverse.clear();
        // Duplicate (view, dep) rows are adjacent per view in PK order, but a
        // source's forward entries interleave across views — dedup with a seen
        // set instead of a per-row `Vec::contains` scan.
        let mut seen: FxHashSet<(i64, i64)> = FxHashSet::default();
        if !dep_tab.is_null() {
            let t = unsafe { &*dep_tab };
            let mut ch = t.open_cursor();
            while ch.valid {
                let w = ch.current_weight;
                if w > 0 {
                    // DepTab compound PK = (view_id, dep_table_id); both live in
                    // the 16-byte PK region as OPK (big-endian for these unsigned
                    // columns): view_id_BE in bytes 0..8, dep_BE in 8..16.
                    let pk = ch.current_pk_bytes();
                    let v_id = u64::from_be_bytes(pk[0..8].try_into().unwrap()) as i64;
                    let dep_tid = u64::from_be_bytes(pk[8..16].try_into().unwrap()) as i64;
                    if dep_tid > 0 && seen.insert((v_id, dep_tid)) {
                        self.forward.entry(dep_tid).or_default().push(v_id);
                        self.reverse.entry(v_id).or_default().push(dep_tid);
                    }
                }
                ch.advance();
            }
        }
        self.valid = true;
        &self.forward
    }
}

impl DagEngine {
    // ── Dependency map ──────────────────────────────────────────────────

    /// Rebuild the dependency maps from the DepTab system table if stale and
    /// return the forward (source → views) map.
    pub fn get_dep_map(&mut self) -> &FxHashMap<i64, Vec<i64>> {
        self.dep.get_or_rebuild(self.sys.dep_tab)
    }

    /// Return all direct source table IDs for a view.
    pub fn get_source_ids(&mut self, view_id: i64) -> Vec<i64> {
        self.get_dep_map();
        self.dep.reverse.get(&view_id).cloned().unwrap_or_default()
    }

    /// Every relation this view's circuit scans — both `ScanDelta` (cascade
    /// dependencies, also returned by `get_source_ids`) AND `ScanTrace` (static
    /// `ext_trace` reads, e.g. the Python circuit-builder's `join(delta, trace)`,
    /// which are deliberately NOT cascade dependencies and so absent from
    /// `get_source_ids`). Deduped. The boot invalid-view verdict's transitive
    /// check needs the ScanTrace targets too: a resumed view that ext_trace-reads
    /// a *rebuilt* source view's output store would otherwise be missed by a
    /// dependency-only walk.
    pub fn all_scan_source_ids(&self, view_id: i64) -> Vec<i64> {
        compiler::scan_source_ids(&self.load_meta_circuit(view_id))
    }

    /// Transitive dependent closure of `seeds` over the view dependency map:
    /// every view reachable by following `source → dependents` edges, with the
    /// seeds themselves excluded.
    pub(super) fn dependent_closure(&mut self, seeds: Vec<i64>) -> FxHashSet<i64> {
        self.get_dep_map();
        let mut reachable: FxHashSet<i64> = FxHashSet::default();
        let mut stack = seeds;
        while let Some(id) = stack.pop() {
            if let Some(deps) = self.dep.forward.get(&id) {
                for &d in deps {
                    if reachable.insert(d) {
                        stack.push(d);
                    }
                }
            }
        }
        reachable
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
        let mut in_deps: FxHashMap<i64, Vec<i64>> = FxHashMap::default();
        for &vid in &ids {
            let deps: Vec<i64> = self
                .get_source_ids(vid)
                .into_iter()
                .filter(|s| *s != vid && bundle.contains(s))
                .collect();
            in_deps.insert(vid, deps);
        }
        let mut emitted: FxHashSet<i64> = FxHashSet::default();
        let mut order: Vec<i64> = Vec::with_capacity(ids.len());
        while order.len() < ids.len() {
            let before = order.len();
            for &vid in &ids {
                if !emitted.contains(&vid) && in_deps[&vid].iter().all(|d| emitted.contains(d)) {
                    order.push(vid);
                    emitted.insert(vid);
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
    pub fn base_tables_reachable_from(&mut self, seeds: Vec<i64>) -> Vec<i64> {
        let mut bases: FxHashSet<i64> = FxHashSet::default();
        // `visited` (seeded with the input views) collapses shared sub-graphs
        // so each view is walked once.
        let mut visited: FxHashSet<i64> = seeds.iter().copied().collect();
        let mut stack = seeds;
        while let Some(node) = stack.pop() {
            for s in self.get_source_ids(node) {
                match self.tables.get(&s).map(|e| e.kind) {
                    Some(RelationKind::BaseTable) => {
                        bases.insert(s);
                    }
                    Some(RelationKind::View) if visited.insert(s) => stack.push(s),
                    _ => {} // unregistered / system source, or already-visited view
                }
            }
        }
        let mut bases: Vec<i64> = bases.into_iter().collect();
        bases.sort_unstable();
        bases
    }

    /// True iff `id` is a registered view. The one spelling of the kind probe.
    pub(crate) fn relation_is_view(&self, id: i64) -> bool {
        self.tables.get(&id).is_some_and(|e| e.kind.is_view())
    }

    /// True iff `id`'s output is a full copy on every worker — the bit stamped on
    /// its schema at registration. The one spelling of the replication probe, so
    /// the write broadcast, the read single-sourcing, and the store shape all read
    /// one answer.
    pub(crate) fn relation_is_replicated(&self, id: i64) -> bool {
        self.tables.get(&id).is_some_and(|e| e.schema.replicated())
    }

    /// Every registered view id.
    pub(crate) fn view_ids(&self) -> Vec<i64> {
        self.tables
            .iter()
            .filter(|(_, e)| e.kind.is_view())
            .map(|(&id, _)| id)
            .collect()
    }

    /// `(any source replicated, every source replicated)` for view `view_id`,
    /// off the reverse dependency map (which records exactly
    /// `circuit.dependencies()`), so it costs no circuit load and no allocation.
    ///
    /// The `any` half is the flag `build_partitioned_storage` routes on: such a
    /// view is built unhashed, so its whole local output sits in one child store
    /// and its rows are NOT keyed by `partition_for_pk`; no key-derived routing
    /// decision holds for it. The `all` half is the view's own stamped
    /// `replicated` bit — strictly stronger, and false for a sourceless view.
    /// Both come from one walk so the two can never be read off different lists.
    pub(crate) fn source_replication(&mut self, view_id: i64) -> (bool, bool) {
        self.get_dep_map();
        let Some(sources) = self.dep.reverse.get(&view_id) else {
            return (false, false);
        };
        let replicated = |tid: &i64| self.tables.get(tid).is_some_and(|e| e.schema.replicated());
        (
            sources.iter().any(replicated),
            !sources.is_empty() && sources.iter().all(replicated),
        )
    }

    /// True iff view `view_id` has **any** replicated source — see
    /// [`source_replication`](Self::source_replication).
    pub(crate) fn view_has_replicated_source(&mut self, view_id: i64) -> bool {
        self.source_replication(view_id).0
    }

    // ── ViewMeta (plan-free circuit metadata) ───────────────────────────

    /// Load typed circuit nodes/edges for metadata queries. Cheaper than full
    /// compilation: no optimization passes, no code emission.
    pub(super) fn load_meta_circuit(&self, view_id: i64) -> compiler::LoadedCircuit {
        let mut loaded = compiler::load_circuit(
            self.sys.nodes,
            self.sys.edges,
            self.sys.node_columns,
            view_id as u64,
            SchemaDescriptor::default(),
        )
        .unwrap_or_default();
        // Populate `outgoing`/`incoming` adjacency so annotation helpers like
        // `reindex_cols_through_filters` can traverse the graph. (`load_circuit`
        // returns a circuit with empty adjacency maps; only `compile_view` runs
        // topo_sort itself.) A malformed cyclic circuit cannot compile or execute
        // (`compile_view` rejects it the same way), so present it as empty here, just
        // like a failed load above — every metadata query then reads the conservative
        // "nothing special" answer (no exchange skip, no shard/join cols, no range
        // join) off an empty circuit instead of walking a cyclic adjacency.
        if compiler::topo_sort(&mut loaded).is_err() {
            return compiler::LoadedCircuit::default();
        }
        loaded
    }

    /// The memoized per-view circuit metadata, computed from ONE
    /// `load_meta_circuit` pass on first touch. (The former per-property memo
    /// caches each paid their own circuit load — up to five per view — and the
    /// join-shard map an extra load per *(view, source)*.)
    pub(super) fn view_meta(&mut self, view_id: i64) -> Rc<ViewMeta> {
        if let Some(m) = self.meta.get(&view_id) {
            return m.clone();
        }
        // `load_meta_circuit` topo-sorts (falling back to an empty circuit on a
        // malformed one), satisfying `from_loaded`'s precondition.
        let meta = Rc::new(ViewMeta::from_loaded(&self.load_meta_circuit(view_id)));
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

    /// The view's output `ExchangeShard` shard columns (empty when none) —
    /// the master relay's routing key.
    pub fn get_shard_cols(&mut self, view_id: i64) -> Rc<[i32]> {
        self.view_meta(view_id).shard_cols.clone()
    }

    /// The join scatter key for `source_id` within `view_id`: reindex
    /// `(column, carried promotion tc)` pairs (empty when the source has no
    /// join reindex). Called once per join source per tick on the master's
    /// serialized exchange-relay path.
    pub fn get_join_shard_cols(&mut self, view_id: i64, source_id: i64) -> Rc<[(i32, u8)]> {
        self.view_meta(view_id)
            .join_shard_map
            .get(&source_id)
            .cloned()
            .unwrap_or_else(|| Rc::from([]))
    }

    /// The equality-conjunct count of a non-equi (range / band) join view, or
    /// `None` if the view is not one. `Some` is the precise discriminator for
    /// the master relay's input routing; the `n_eq` value picks eq-prefix
    /// scatter (`n_eq ≥ 1`, band join) vs broadcast (`n_eq == 0`, pure range).
    pub fn view_range_join_n_eq(&mut self, view_id: i64) -> Option<u8> {
        self.view_meta(view_id).range_join_n_eq
    }

    /// True iff a live CREATE of this view needs the distributed backfill: the
    /// view's circuit carries an `ExchangeShard` node (GROUP BY / reduce /
    /// set-op / range-join all do) or any `Join` node. The `Join` arm is
    /// load-bearing — an equi-join (`DeltaTrace`) repartitions its inputs at
    /// runtime through the join-shard scatter and carries **no**
    /// `ExchangeShard`, so nothing else catches it.
    ///
    /// `pub` for the live CREATE-VIEW path: the catalog hook gates its inline
    /// single-process `backfill_view` on `!view_seeds_exchange_backfill` (plain
    /// projections/filters only), and the executor drives every seeding view
    /// through the distributed backfill (`fan_out_backfill`) instead.
    pub fn view_seeds_exchange_backfill(&mut self, view_id: i64) -> bool {
        let meta = self.view_meta(view_id);
        meta.needs_exchange || meta.has_join
    }
}
