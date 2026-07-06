//! DagEngine: consolidated plan cache, DAG evaluator, and ingestion pipeline.

use rustc_hash::{FxHashMap, FxHashSet};
use std::cell::UnsafeCell;

use crate::ops;
use crate::query::compiler::{self, CompileOutput, ExternalTable, SubPlan};
use crate::query::vm;
use crate::schema::SchemaDescriptor;
use crate::storage::{Batch, CursorHandle, PartitionedTable, RecoverySource, StorageError, Table};
use gnitz_wire::PkColList;

mod store_handle;
pub(crate) use crate::query::compiler::is_worker_scratch_dir_name;
pub(crate) use store_handle::StoreHandle;

impl IndexCircuitEntry {
    /// Interior-mutable access to the owned index Table.
    ///
    /// `index_table` is an `UnsafeCell`, so `get()` yields a `*mut` that is
    /// legal to mutate through even via `&self`. Single-threaded; callers
    /// must ensure no aliasing `&mut` into the same Table is live.
    #[allow(clippy::mut_from_ref)]
    pub fn table_mut(&self) -> &mut Table {
        unsafe { &mut *self.index_table.get() }
    }

    /// The source column list of a unique circuit, `None` for a non-unique one.
    /// The single accessor for the unique-enforcement machinery (filters,
    /// routing cache, has_pk pre-checks). The returned slice has length ≥ 1: a
    /// single-column unique index yields a 1-element list, a composite
    /// `UNIQUE (a, b, …)` the full ordered list. Order is significant (it drives
    /// the leading-key span encoding and prefix seeks).
    #[inline]
    pub fn unique_cols(&self) -> Option<&[u32]> {
        if !self.is_unique {
            return None;
        }
        Some(self.col_indices.as_slice())
    }
}

// ---------------------------------------------------------------------------
// Index circuit entry
// ---------------------------------------------------------------------------

/// A secondary index on a column.
/// Owns the index Table via Box — dropping the entry drops the table.
pub struct IndexCircuitEntry {
    /// The index's declared column list, in order. A 1-element list is the
    /// single-column case. Dedup/lookup is exact ordered-list equality on
    /// `col_indices.as_slice()`; order is significant (it drives leading-prefix
    /// seeks).
    pub col_indices: PkColList,
    /// The index_id of the IDX_TAB row that caused Table::new to be called.
    /// When a second index promotes an incumbent circuit (UNIQUE+FK case), no
    /// new directory is created; this field identifies the actual on-disk path
    /// so the retraction branch queues the correct directory for deletion.
    pub index_id: i64,
    pub index_table: UnsafeCell<Box<Table>>,
    pub index_schema: SchemaDescriptor,
    pub is_unique: bool,
}

/// Fault-injection seam for the push-apply / SAL-replay path; identity in
/// release builds. In a debug build with
/// `GNITZ_INJECT_INGEST_APPLY_ERROR=store|index`, substitutes
/// `Err(StorageError::Io)` for the matching ingest inside
/// `ingest_store_and_indices`, so the fail-stop abort fires deterministically
/// without a real disk fault. No one-shot latch — the process aborts on first
/// fire. The env is read once. Placing the seam at the dag layer keeps
/// `storage` seam-free and fires exactly on the push-apply/replay path (VM
/// integration bypasses it), so a seam-armed server still boots cleanly on a
/// pushless data dir and fails only on the first INSERT apply.
fn inject_ingest_apply_error(
    which: &str,
    r: Result<(), crate::storage::StorageError>,
) -> Result<(), crate::storage::StorageError> {
    #[cfg(debug_assertions)]
    {
        use std::sync::OnceLock;
        static MODE: OnceLock<Option<String>> = OnceLock::new();
        let armed = MODE.get_or_init(|| std::env::var("GNITZ_INJECT_INGEST_APPLY_ERROR").ok());
        if armed.as_deref() == Some(which) {
            return Err(crate::storage::StorageError::Io);
        }
    }
    #[cfg(not(debug_assertions))]
    let _ = which;
    r
}

// ---------------------------------------------------------------------------
// Relation kind — what a top-level relation *is*
// ---------------------------------------------------------------------------

/// What a top-level relation *is*. Bundles every per-kind property that used
/// to be set independently, so the nonsense combinations are unconstructable:
/// a durable relation that also rebuilds from source (double count), an
/// ephemeral one that never rebuilds (permanently empty), a view that tags
/// Pk-unique.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum RelationKind {
    /// System catalog table: durable, single-partition, never rebuilt from
    /// upstream sources (it has none; recovery is LSN-gated SAL replay).
    SystemCatalog,
    /// User base table: durable, partitioned, never rebuilt from upstream
    /// sources; owns a DML-enforced PK. `unique_pk` is the enforcement flag.
    BaseTable { unique_pk: bool },
    /// Materialised view: ephemeral, partitioned, rebuilt from its sources via
    /// the compiled circuit at open and on live CREATE.
    View,
}

// Niche-optimised to the width of the `unique_pk: bool` it replaced, so
// `TableEntry` does not grow and the ingest hot path touches no extra
// cache line.
const _: () = assert!(std::mem::size_of::<RelationKind>() == 1);

impl RelationKind {
    /// How this relation's tail is recovered. `SalReplay` kinds load shards from
    /// the manifest and replay the SAL tail; view output stores are
    /// `RederiveCheckpointed` — erased on open and rebuilt today, but persisted
    /// with generation-stamped manifests by the ephemeral checkpoint round.
    #[inline]
    pub fn recovery_source(self) -> RecoverySource {
        match self {
            RelationKind::SystemCatalog | RelationKind::BaseTable { .. } => RecoverySource::SalReplay,
            RelationKind::View => RecoverySource::RederiveCheckpointed,
        }
    }

    /// True iff this is a user base table (of either `unique_pk` flavor).
    /// Gates what only base tables do: own secondary index circuits (index
    /// projection runs only on the base-table DML paths) and tag
    /// flushed/compacted shards Pk-unique.
    #[inline]
    pub fn is_base_table(self) -> bool {
        matches!(self, RelationKind::BaseTable { .. })
    }
}

// ---------------------------------------------------------------------------
// Table entry — per-table metadata in the entity registry
// ---------------------------------------------------------------------------

pub struct TableEntry {
    pub handle: StoreHandle,
    pub schema: SchemaDescriptor,
    pub kind: RelationKind,
    pub depth: i32,
    pub directory: String,
    pub index_circuits: Vec<IndexCircuitEntry>,
}

impl TableEntry {
    /// The kind's `unique_pk` enforcement flag — true only for a base table
    /// created with a DML-enforced PK; false elsewhere (no `enforce_unique_pk`
    /// runs there). `#[inline]` keeps the hot `ingest_by_ref`/
    /// `ingest_returning_effective` path a trivial match.
    #[inline]
    pub fn unique_pk(&self) -> bool {
        matches!(self.kind, RelationKind::BaseTable { unique_pk: true })
    }
}

/// Type alias — the cache stores CompileOutput directly.
pub(crate) type CachedPlan = CompileOutput;

// ---------------------------------------------------------------------------
// System table references
// ---------------------------------------------------------------------------

/// Handles for the four system tables that the compiler reads: CircuitNodes,
/// CircuitEdges, CircuitNodeColumns, and DepTab. No schemas are threaded — the
/// compiler's cursor readers source each table's schema from the cursor itself
/// (`ReadCursor::schema`), and `get_dep_map` reads DepTab's compound PK directly
/// from the cursor's PK bytes.
pub struct SysTableRefs {
    // Table handles (DagEngine borrows them).
    pub nodes: *mut Table,
    pub edges: *mut Table,
    pub node_columns: *mut Table,
    pub dep_tab: *mut Table,
}

// SAFETY: same single-thread guarantee.
unsafe impl Send for SysTableRefs {}

impl SysTableRefs {
    fn null() -> Self {
        SysTableRefs {
            nodes: std::ptr::null_mut(),
            edges: std::ptr::null_mut(),
            node_columns: std::ptr::null_mut(),
            dep_tab: std::ptr::null_mut(),
        }
    }
}

// ---------------------------------------------------------------------------
// ExchangeCallback — trait for multi-worker exchange IPC
// ---------------------------------------------------------------------------

/// Callback trait for exchange IPC between workers.
///
/// The worker event loop implements this trait to send pre-exchange output
/// to the master (via W2M) and receive relayed data (via SAL).
/// This breaks the circular dependency between worker.rs ↔ dag.rs.
pub trait ExchangeCallback {
    fn do_exchange(&mut self, view_id: i64, batch: &Batch, source_id: i64) -> Batch;
}

// ---------------------------------------------------------------------------
// DagEngine
// ---------------------------------------------------------------------------

/// The per-view exchange-routing metadata caches, grouped so their eviction can
/// never drift from the field set. `clear` (`*self = default`) auto-covers any
/// field added later; `evict` drops every entry mentioning an id and is used for
/// both table and view drops.
#[derive(Default)]
struct ViewPropCache {
    shard_cols: FxHashMap<i64, Vec<i32>>,
    // Each entry is (reindex column, carried promotion target tc) — the carried
    // tc is non-zero for a cross-width join key and threads to the scatter packer.
    join_shard: FxHashMap<(i64, i64), Vec<(i32, u8)>>,
    // Whether a view's output ExchangeShard is a no-op (every row already on its
    // PK-owner) and the IPC shuffle can be skipped. Memoized per view_id.
    skip_exchange: FxHashMap<i64, bool>,
    // Whether a view has an ExchangeShard node at all. Memoized per view_id.
    needs_exchange: FxHashMap<i64, bool>,
    // Whether a view is a non-equi (range) join — its input relay broadcasts and
    // its driver branch double-exchanges. Memoized per view_id.
    range_join: FxHashMap<i64, Option<u8>>,
}

impl ViewPropCache {
    /// Drop every memoized entry mentioning `id` — keyed by it as the owning
    /// view/table, or with it as a join source. Used for dropping either a table
    /// or a view: production routes both through here (`unregister_table`), and a
    /// dropped view can itself be a join source. Over-eviction is always safe
    /// (entries are recomputed on miss), so the broad join_shard retain (matching
    /// `id` as view OR source) is correct for every drop.
    fn evict(&mut self, id: i64) {
        self.shard_cols.remove(&id);
        self.join_shard.retain(|&(v, s), _| v != id && s != id);
        self.skip_exchange.remove(&id);
        self.needs_exchange.remove(&id);
        self.range_join.remove(&id);
    }

    fn clear(&mut self) {
        *self = Self::default();
    }
}

/// The bidirectional view-dependency index with its validity flag bundled in, so
/// "valid but stale" is unreachable: only `get_or_rebuild` sets `valid` (after
/// repopulating both maps), and only `invalidate` clears it.
#[derive(Default)]
struct DepMap {
    forward: FxHashMap<i64, Vec<i64>>, // source_table_id → [view_ids]
    reverse: FxHashMap<i64, Vec<i64>>, // view_id → [source_table_ids]
    valid: bool,
}

impl DepMap {
    fn invalidate(&mut self) {
        self.valid = false;
    }

    /// Rebuild both maps from the DepTab system table if stale and return the
    /// forward (source → views) map. `dep_tab` is passed in (a Copy raw pointer)
    /// because the table lives on `DagEngine`; reading it through `self` here would
    /// double-borrow against the `&mut self.dep`.
    fn get_or_rebuild(&mut self, dep_tab: *mut Table) -> &FxHashMap<i64, Vec<i64>> {
        if self.valid {
            return &self.forward;
        }
        self.forward.clear();
        self.reverse.clear();
        if !dep_tab.is_null() {
            let t = unsafe { &*dep_tab };
            let mut ch = t.open_cursor();
            while ch.cursor.valid {
                let w = ch.cursor.current_weight;
                if w > 0 {
                    // DepTab compound PK = (view_id, dep_table_id); both live in
                    // the 16-byte PK region as OPK (big-endian for these unsigned
                    // columns): view_id_BE in bytes 0..8, dep_BE in 8..16.
                    let pk = ch.cursor.current_pk_bytes();
                    let v_id = u64::from_be_bytes(pk[0..8].try_into().unwrap()) as i64;
                    let dep_tid = u64::from_be_bytes(pk[8..16].try_into().unwrap()) as i64;
                    if dep_tid > 0 {
                        let views = self.forward.entry(dep_tid).or_default();
                        if !views.contains(&v_id) {
                            views.push(v_id);
                        }
                        let sources = self.reverse.entry(v_id).or_default();
                        if !sources.contains(&dep_tid) {
                            sources.push(dep_tid);
                        }
                    }
                }
                ch.cursor.advance();
            }
        }
        self.valid = true;
        &self.forward
    }
}

pub struct DagEngine {
    cache: FxHashMap<i64, CachedPlan>,
    dep: DepMap,
    view_props: ViewPropCache,
    pub(crate) tables: FxHashMap<i64, TableEntry>,
    sys: SysTableRefs,
}

struct PendingEntry {
    depth: i32,
    view_id: i64,
    source_id: i64,
    batch: Batch,
}

// SAFETY: DagEngine is only accessed from a single thread.
unsafe impl Send for DagEngine {}

/// Per-side exchange metadata snapshot: (schema, source_id, post seed reg).
type ExchangeSideMeta = (SchemaDescriptor, i64, u16);

impl DagEngine {
    pub fn new() -> Self {
        DagEngine {
            cache: FxHashMap::default(),
            dep: DepMap::default(),
            view_props: ViewPropCache::default(),
            tables: FxHashMap::default(),
            sys: SysTableRefs::null(),
        }
    }

    // ── System table setup ──────────────────────────────────────────────

    pub fn set_sys_tables(&mut self, sys: SysTableRefs) {
        self.sys = sys;
    }

    // ── Table registry ──────────────────────────────────────────────────

    pub fn register_table(
        &mut self,
        table_id: i64,
        handle: StoreHandle,
        schema: SchemaDescriptor,
        kind: RelationKind,
        depth: i32,
        directory: String,
    ) {
        self.tables.insert(
            table_id,
            TableEntry {
                handle,
                schema,
                kind,
                depth,
                directory,
                index_circuits: Vec::new(),
            },
        );
    }

    pub fn unregister_table(&mut self, table_id: i64) {
        self.tables.remove(&table_id);
        self.cache.remove(&table_id);
        self.view_props.evict(table_id);
        self.dep.invalidate();
    }

    pub fn add_index_circuit(
        &mut self,
        table_id: i64,
        col_indices: &[u32],
        index_id: i64,
        index_table: Box<Table>,
        index_schema: SchemaDescriptor,
        is_unique: bool,
    ) {
        if let Some(entry) = self.tables.get_mut(&table_id) {
            entry.index_circuits.push(IndexCircuitEntry {
                col_indices: PkColList::from_slice(col_indices),
                index_id,
                index_table: UnsafeCell::new(index_table),
                index_schema,
                is_unique,
            });
        }
    }

    pub fn remove_index_circuit(&mut self, table_id: i64, col_indices: &[u32]) {
        if let Some(entry) = self.tables.get_mut(&table_id) {
            // retain() drops non-matching entries, which drops Box<Table> automatically.
            entry
                .index_circuits
                .retain(|ic| ic.col_indices.as_slice() != col_indices);
        }
    }

    /// Replace the index circuit's owned Table (worker-boot re-home to the rank
    /// subdir): drop the fork-inherited parent-dir table and install `t`.
    /// Returns the new table pointer for the backfill; `None` if no circuit on
    /// `col_indices` matches. Dropping the old `Box` closes the inherited table.
    /// Sound: pointer consumers (`get_index_store_handle`, cursors) are fetched
    /// per request, and the swap runs before the worker serves anything.
    pub fn replace_index_table(&mut self, table_id: i64, col_indices: &[u32], t: Box<Table>) -> Option<*mut Table> {
        let entry = self.tables.get_mut(&table_id)?;
        let ic = entry
            .index_circuits
            .iter_mut()
            .find(|ic| ic.col_indices.as_slice() == col_indices)?;
        ic.index_table = UnsafeCell::new(t);
        Some(ic.table_mut() as *mut Table)
    }

    /// Set the uniqueness flag of the index circuit on `col_indices` in place
    /// (the circuit list is deduped by column list, so at most one entry
    /// matches). Promotion (`true`) folds a UNIQUE index into an existing
    /// non-unique circuit when both target the same column list; demotion
    /// (`false`) is used by the DROP INDEX retraction path when the UNIQUE index
    /// is dropped but another index (e.g. an FK auto-index) still covers it.
    pub fn set_index_circuit_uniqueness(&mut self, table_id: i64, col_indices: &[u32], is_unique: bool) {
        if let Some(entry) = self.tables.get_mut(&table_id) {
            if let Some(ic) = entry
                .index_circuits
                .iter_mut()
                .find(|ic| ic.col_indices.as_slice() == col_indices)
            {
                ic.is_unique = is_unique;
            }
        }
    }

    /// Release the delta batches pinned in a view's compiled-plan regfile.
    /// `execute_epoch` only clears deltas at the *start* of an epoch, so after
    /// a backfill the full scanned source dataset and intermediate deltas stay
    /// resident until the next evaluation. Call this after backfill to free
    /// them immediately. Clears both pre-VM and post-VM (aggregation views have
    /// a post-VM that also accumulates delta registers during backfill).
    pub fn clear_view_regfile_deltas(&mut self, view_id: i64) {
        if let Some(plan) = self.cache.get_mut(&view_id) {
            plan.pre.vm.regfile.clear_deltas();
            if let Some(post) = plan.post.as_mut() {
                post.vm.regfile.clear_deltas();
            }
            // Binary set-op views (UNION ALL / EXCEPT / INTERSECT) compile the
            // right-hand side into `side_b`, which owns its own VM regfile; its
            // delta batches stay pinned (pooled batches leaked) until the view is
            // invalidated unless cleared here too.
            if let Some(sb) = plan.side_b.as_mut() {
                sb.plan.vm.regfile.clear_deltas();
            }
        }
    }

    /// Transitive dependent closure of `seeds` over the view dependency map:
    /// every view reachable by following `source → dependents` edges, with the
    /// seeds themselves excluded.
    fn dependent_closure(&mut self, seeds: Vec<i64>) -> FxHashSet<i64> {
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

    /// Distributed-backfill analogue of `backfill_view`'s post-loop
    /// `clear_view_regfile_deltas` (catalog/ddl.rs). After a worker's
    /// `handle_backfill(source_id)` loop, the last chunk's input + intermediate
    /// delta registers stay pinned in the touched views' regfiles. Release them
    /// across `source_id`'s dependent closure, so peak resident memory falls
    /// back to ~O(chunk) once the backfill drains.
    ///
    /// Also carries `backfill_view`'s Ephemeral guard: every view backfilled this
    /// way must be ephemeral, else its manifest-loaded shards would double-count
    /// against the deltas the backfill ingests.
    pub fn clear_regfile_deltas_from_source(&mut self, source_id: i64) {
        for view_id in self.dependent_closure(vec![source_id]) {
            debug_assert!(
                self.tables
                    .get(&view_id)
                    .is_none_or(|e| e.kind.recovery_source() != RecoverySource::SalReplay),
                "distributed backfill into durable relation {view_id}: \
                 would double-count loaded shards",
            );
            self.clear_view_regfile_deltas(view_id);
        }
    }

    // ── Cache management ────────────────────────────────────────────────

    /// Drop one view's cached plan + memoized view properties, leaving it
    /// registered. Callers: the test-only `drop_view`, the dag tests, and the
    /// recovery step-4 output reset (`reset_view_output_for_rebuild`), which needs
    /// the next backfill to recompile the view against its freshly-emptied store
    /// and scratch.
    pub fn invalidate(&mut self, view_id: i64) {
        self.cache.remove(&view_id);
        self.view_props.evict(view_id);
        self.dep.invalidate();
    }

    pub fn invalidate_all(&mut self) {
        self.cache.clear();
        self.view_props.clear();
        self.dep.invalidate();
    }

    pub fn invalidate_dep_map(&mut self) {
        self.dep.invalidate();
    }

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
        let loaded = self.load_meta_circuit(view_id);
        let mut ids: Vec<i64> = Vec::new();
        for op in loaded.nodes.values() {
            if let gnitz_wire::OpNode::ScanDelta(t) | gnitz_wire::OpNode::ScanTrace(t) = op {
                let id = *t as i64;
                if !ids.contains(&id) {
                    ids.push(id);
                }
            }
        }
        ids
    }

    /// Order a DDL bundle's view ids by their intra-bundle dependencies (Kahn's
    /// algorithm over `get_source_ids(vid) ∩ bundle`), so a chain's upstream
    /// hidden view is backfilled before a downstream one scans it. VIEW_TAB row
    /// order carries the registration `depth` (a standing contract), but the
    /// live backfill must not also couple to that order — a deliberately
    /// row-misordered bundle must still backfill. A bundle is acyclic by
    /// construction; the no-progress fallback appends the remainder in input
    /// order so termination holds regardless.
    pub fn order_by_intra_bundle_deps(&mut self, view_ids: &[i64]) -> Vec<i64> {
        if view_ids.len() <= 1 {
            return view_ids.to_vec();
        }
        let bundle: FxHashSet<i64> = view_ids.iter().copied().collect();
        let mut in_deps: FxHashMap<i64, Vec<i64>> = FxHashMap::default();
        for &vid in view_ids {
            let deps: Vec<i64> = self
                .get_source_ids(vid)
                .into_iter()
                .filter(|s| *s != vid && bundle.contains(s))
                .collect();
            in_deps.insert(vid, deps);
        }
        let mut emitted: FxHashSet<i64> = FxHashSet::default();
        let mut order: Vec<i64> = Vec::with_capacity(view_ids.len());
        while order.len() < view_ids.len() {
            let before = order.len();
            for &vid in view_ids {
                if !emitted.contains(&vid) && in_deps[&vid].iter().all(|d| emitted.contains(d)) {
                    order.push(vid);
                    emitted.insert(vid);
                }
            }
            if order.len() == before {
                debug_assert!(false, "cycle in DDL bundle view dependencies: {view_ids:?}");
                for &vid in view_ids {
                    if emitted.insert(vid) {
                        order.push(vid);
                    }
                }
            }
        }
        order
    }

    // ── Metadata queries (lightweight, no compilation) ──────────────────

    /// Load typed circuit nodes/edges for metadata queries. Cheaper than full
    /// compilation: no topo sort, no optimization passes, no code emission.
    fn load_meta_circuit(&self, view_id: i64) -> compiler::LoadedCircuit {
        let mut loaded = compiler::load_circuit(
            self.sys.nodes,
            self.sys.edges,
            self.sys.node_columns,
            view_id as u64,
            SchemaDescriptor::default(),
        )
        .unwrap_or_else(compiler::LoadedCircuit::empty);
        // Populate `outgoing`/`incoming` adjacency so annotation helpers like
        // `reindex_cols_through_filters` can traverse the graph. (`load_circuit`
        // returns a circuit with empty adjacency maps; only `compile_view` runs
        // topo_sort itself.) A malformed cyclic circuit cannot compile or execute
        // (`compile_view` rejects it the same way), so present it as empty here, just
        // like a failed load above — every metadata query then reads the conservative
        // "nothing special" answer (no exchange skip, no shard/join cols, no range
        // join) off an empty circuit instead of walking a cyclic adjacency.
        if compiler::topo_sort(&mut loaded).is_err() {
            return compiler::LoadedCircuit::empty();
        }
        loaded
    }

    /// Extract shard columns for a view without full compilation.
    pub fn get_shard_cols(&mut self, view_id: i64) -> Vec<i32> {
        if let Some(cols) = self.view_props.shard_cols.get(&view_id) {
            return cols.clone();
        }
        let loaded = self.load_meta_circuit(view_id);
        let shard_cols = loaded
            .nodes
            .values()
            .find_map(|op| {
                if let gnitz_wire::OpNode::ExchangeShard { shard_cols: sc } = op {
                    Some(sc.iter().map(|&c| c as i32).collect::<Vec<_>>())
                } else {
                    None
                }
            })
            .unwrap_or_default();
        self.view_props.shard_cols.insert(view_id, shard_cols.clone());
        shard_cols
    }

    /// Get join shard columns for a specific source within a view, each paired
    /// with its carried promotion target tc (`0` = no promotion / derive from
    /// source). The pairs mirror the trace-side reindex Map slot-for-slot.
    pub fn get_join_shard_cols(&mut self, view_id: i64, source_id: i64) -> Vec<(i32, u8)> {
        // Called once per join source per tick on the master's serialized
        // exchange-relay path; cache the result so the hot path skips the
        // full load_circuit + topo_sort (O(V+E) with several allocations).
        if let Some(cols) = self.view_props.join_shard.get(&(view_id, source_id)) {
            return cols.clone();
        }
        let cols = self.compute_join_shard_cols(view_id, source_id);
        self.view_props.join_shard.insert((view_id, source_id), cols.clone());
        cols
    }

    /// Whether `get_join_shard_cols` would be empty, without cloning the cached
    /// Vec — the hot-path arm in `evaluate_dag_multi_worker` tests only emptiness.
    fn join_shard_cols_is_empty(&mut self, view_id: i64, source_id: i64) -> bool {
        if let Some(cols) = self.view_props.join_shard.get(&(view_id, source_id)) {
            return cols.is_empty();
        }
        let cols = self.compute_join_shard_cols(view_id, source_id);
        let empty = cols.is_empty();
        self.view_props.join_shard.insert((view_id, source_id), cols);
        empty
    }

    fn compute_join_shard_cols(&self, view_id: i64, source_id: i64) -> Vec<(i32, u8)> {
        let loaded = self.load_meta_circuit(view_id);

        // Find the scan node (ScanTrace or ScanDelta) for this source.
        // ScanTrace: Python API joins (trace-only source).
        // ScanDelta: SQL joins (reindex both sides as delta inputs).
        let scan_nid = loaded.nodes.iter().find_map(|(&nid, op)| {
            let tid = match op {
                gnitz_wire::OpNode::ScanTrace(t) | gnitz_wire::OpNode::ScanDelta(t) => t,
                _ => return None,
            };
            if *tid as i64 == source_id {
                Some(nid)
            } else {
                None
            }
        });
        let scan_nid = match scan_nid {
            Some(n) => n,
            None => return Vec::new(),
        };

        // Find the downstream Map(Expression { reindex_cols }) node, walking
        // through any intervening Filter nodes (planner emits Filter → Map
        // reindex chains for PK-redistribution views).
        crate::query::compiler::reindex_cols_through_filters(&loaded, scan_nid)
    }

    /// True iff the view's output `ExchangeShard` is a no-op (every row already on
    /// the worker owning its distribution key) and can be skipped: the shard reads
    /// a scan — through any Filter chain — whose distribution prefix is the shard
    /// key. Collapses the former `ExchangeInfo { is_trivial,
    /// is_co_partitioned }` — whose sole reader ANDed both fields — into the one
    /// bit they encoded, making the impossible `{ is_trivial: false,
    /// is_co_partitioned: true }` state unrepresentable.
    fn view_skips_exchange(&mut self, view_id: i64) -> bool {
        if let Some(&skip) = self.view_props.skip_exchange.get(&view_id) {
            return skip;
        }
        let skip = self.compute_view_skips_exchange(view_id);
        self.view_props.skip_exchange.insert(view_id, skip);
        skip
    }

    fn compute_view_skips_exchange(&self, view_id: i64) -> bool {
        let loaded = self.load_meta_circuit(view_id);

        // The view's output ExchangeShard and its shard columns.
        let Some((enid, shard_cols)) = loaded.nodes.iter().find_map(|(&nid, op)| match op {
            gnitz_wire::OpNode::ExchangeShard { shard_cols } => {
                Some((nid, shard_cols.iter().map(|&c| c as i32).collect::<Vec<_>>()))
            }
            _ => None,
        }) else {
            return false;
        };

        // Resolve the shard's source table, walking back through any Filter chain
        // (`scan_tid_through_filters`). Skip iff that table's distribution prefix
        // (`pk_indices[..k]`) is exactly the shard key — an exact-prefix match. For
        // a default full-PK table this is the strict full-PK case as before; for a
        // `CLUSTER BY prefix` table it also lets a (possibly filtered)
        // `GROUP BY prefix` / reduce run locally — every row for a group value
        // already lives on one worker — since this governs every single-source
        // `ExchangeShard` view, not just joins.
        let Some(tid) = crate::query::compiler::scan_tid_through_filters(&loaded, enid) else {
            return false;
        };
        self.tables
            .get(&tid)
            .is_some_and(|entry| entry.schema.shard_cols_match_dist_key(&shard_cols))
    }

    /// True iff every base-table source feeding `view_id` is replicated (and the
    /// view has at least one source). Such a view's `ExchangeShard`s are all
    /// skipped (`compute_co_partitioned` marks replicated sources co-partitioned),
    /// so every worker computes the full result locally — the output is itself
    /// **replicated** and its read must single-source (design §4.2). A view with
    /// any partitioned source (e.g. partitioned ⋈ replicated) is partitioned and
    /// its read gathers normally. A non-base-table source (a nested view) reads as
    /// non-replicated here, so nested-over-replicated views are conservatively
    /// treated as partitioned — the MVP surface is base-table dimensions.
    ///
    /// Sources come from the reverse dependency map, which records exactly
    /// `circuit.dependencies()` — the deduped `ScanDelta` source set — so this
    /// answers the question off the dependency map with no circuit load and no
    /// allocation: `get_dep_map` rebuilds if stale, then the entry is read by
    /// reference (`self.dep` and `self.tables` are disjoint borrows).
    pub(crate) fn view_all_sources_replicated(&mut self, view_id: i64) -> bool {
        self.get_dep_map();
        let Some(sources) = self.dep.reverse.get(&view_id) else {
            return false;
        };
        !sources.is_empty()
            && sources
                .iter()
                .all(|tid| self.tables.get(tid).is_some_and(|e| e.schema.replicated()))
    }

    /// Ensure a view's plan is compiled. Returns true if compilation succeeded.
    pub fn ensure_compiled(&mut self, view_id: i64) -> bool {
        if self.cache.contains_key(&view_id) {
            return true;
        }
        match self.compile_view_internal(view_id) {
            Some(plan) => {
                self.cache.insert(view_id, plan);
                true
            }
            None => false,
        }
    }

    /// Get the exchange input schema for a view's compiled plan.
    /// Returns None if the view has no exchange or isn't compiled.
    pub fn get_exchange_schema(&mut self, view_id: i64) -> Option<SchemaDescriptor> {
        self.ensure_compiled(view_id);
        self.cache.get(&view_id).and_then(|p| p.exchange_in_schema)
    }

    /// The exchange-input schema for `view_id`, falling back to the view's output
    /// schema when the plan records none. The exchange pipeline labels pre-phase
    /// output (and any empty placeholder) with this, never the view's
    /// combine-widened final schema.
    fn exchange_or_view_schema(&mut self, view_id: i64) -> SchemaDescriptor {
        self.get_exchange_schema(view_id)
            .unwrap_or_else(|| self.tables[&view_id].schema)
    }

    /// Check if a view needs exchange (has an `ExchangeShard` node). Memoized in a
    /// sibling `bool` cache: the hot path calls this once per view per tick, and
    /// the underlying `load_meta_circuit` is a `load_circuit` + `topo_sort`
    /// (`O(V+E)` with several allocations). Stays `pub` and derives from the meta
    /// circuit (system tables), not `self.cache`, so it is correct for uncompiled
    /// views — boot-time classification runs before any plan is compiled.
    pub fn view_needs_exchange(&mut self, view_id: i64) -> bool {
        if let Some(&needs) = self.view_props.needs_exchange.get(&view_id) {
            return needs;
        }
        let loaded = self.load_meta_circuit(view_id);
        let needs = loaded
            .nodes
            .values()
            .any(|op| matches!(op, gnitz_wire::OpNode::ExchangeShard { .. }));
        self.view_props.needs_exchange.insert(view_id, needs);
        needs
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
                    Some(RelationKind::BaseTable { .. }) => {
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

    /// The equality-conjunct count of a non-equi (range / band) join view, or
    /// `None` if the view is not one — read off its `Join(DeltaTraceRange)` node.
    /// `Some` is the precise discriminator for the range-join driver branch; the
    /// `n_eq` value drives the input relay's eq-prefix scatter (`n_eq ≥ 1`, band
    /// join) vs. broadcast (`n_eq == 0`, pure range join). It is NOT
    /// `has_join_shard && has_exchange`: that predicate is also true for every
    /// GROUP BY / reduce / single-sided set-op view (a group reindex matches
    /// `reindex_cols_through_filters`), which must keep the plain `has_exchange`
    /// arm. Memoized in `view_props.range_join`, evicted with the sibling view caches.
    pub fn view_range_join_n_eq(&mut self, view_id: i64) -> Option<u8> {
        if let Some(&v) = self.view_props.range_join.get(&view_id) {
            return v;
        }
        let loaded = self.load_meta_circuit(view_id);
        let n_eq = crate::query::compiler::circuit_range_join_n_eq(&loaded);
        self.view_props.range_join.insert(view_id, n_eq);
        n_eq
    }

    /// True iff a live CREATE of this view needs the distributed backfill: the
    /// view's circuit carries an `ExchangeShard` node (GROUP BY / reduce /
    /// set-op / range-join all do) or any `Join` node. The `Join` arm is load-bearing —
    /// an equi-join (`DeltaTrace`) repartitions its inputs at runtime through the
    /// join-shard scatter (arm 4 of the multi-worker step) and carries **no**
    /// `ExchangeShard`, so nothing else catches it. Matching any `Join` rather
    /// than the equi `JoinKind` is exact and leaves no variant list to drift: the
    /// only other kind, a range/band join (`DeltaTraceRange`), always also carries
    /// an `ExchangeShard` and so is already covered by the first arm. (Set-ops are
    /// join-free union/positive_part arithmetic, not `Join`, and likewise carry an
    /// `ExchangeShard`.)
    ///
    /// Loads the meta circuit once and reuses it to warm `view_props.needs_exchange`.
    ///
    /// `pub` for the live CREATE-VIEW path: the catalog hook gates its inline
    /// single-process `backfill_view` on `!view_seeds_exchange_backfill` (plain
    /// projections/filters only), and the executor drives every seeding view
    /// through the distributed backfill (`fan_out_backfill`) instead.
    pub fn view_seeds_exchange_backfill(&mut self, view_id: i64) -> bool {
        let loaded = self.load_meta_circuit(view_id);
        let needs_exchange = loaded
            .nodes
            .values()
            .any(|op| matches!(op, gnitz_wire::OpNode::ExchangeShard { .. }));
        self.view_props.needs_exchange.insert(view_id, needs_exchange);
        needs_exchange
            || loaded
                .nodes
                .values()
                .any(|op| matches!(op, gnitz_wire::OpNode::Join(_)))
    }

    // ── Compilation ─────────────────────────────────────────────────────

    /// Compile a view by reading system tables and calling `compiler::compile_view`.
    fn compile_view_internal(&self, view_id: i64) -> Option<CachedPlan> {
        let entry = self.tables.get(&view_id)?;
        let view_schema = &entry.schema;
        let view_dir = entry.directory.clone();

        // Build external tables array from registered tables
        let ext_tables: Vec<ExternalTable> = self
            .tables
            .iter()
            .map(|(&tid, te)| ExternalTable {
                table_id: tid,
                schema: te.schema,
            })
            .collect();

        let result = unsafe {
            compiler::compile_view(
                view_id as u64,
                self.sys.nodes,
                self.sys.edges,
                self.sys.node_columns,
                &view_dir,
                view_id as u32,
                view_schema,
                &ext_tables,
            )
        };

        match result {
            Ok(output) => {
                gnitz_debug!("dag: compiled view_id={}, pre_regs={}", view_id, output.pre.num_regs);
                Some(output)
            }
            Err(err) => {
                gnitz_warn!("dag: compile_view returned error {:?} for view_id={}", err, view_id);
                None
            }
        }
    }

    // ── Execution ───────────────────────────────────────────────────────

    /// Execute a single epoch of a compiled plan.
    ///
    /// Refreshes ext cursors, loads input, runs the VM, returns the output batch.
    /// Port of `ExecutablePlan.execute_epoch()`.
    pub fn execute_epoch(&mut self, view_id: i64, input: Batch, source_id: i64) -> Option<Batch> {
        // We need to compile (or fetch cached plan) then execute.
        // Since we need &mut self for get_program but also need to borrow
        // plan fields, we first ensure compilation, then work with the cached plan.
        if !self.ensure_compiled(view_id) {
            gnitz_warn!("dag: execute_epoch — no plan for view_id={}", view_id);
            return None;
        }

        // Exchange views (GROUP BY / SELECT DISTINCT / set-ops) need their full
        // multi-phase pipeline; backfill runs single-worker, so route through the
        // local path which handles pre→post and two-sided combines.
        if self.cache.get(&view_id).map(|p| p.post.is_some()).unwrap_or(false) {
            return self.execute_epoch_for_dag(view_id, input, source_id);
        }

        let plan = self.cache.get_mut(&view_id).unwrap();
        Self::execute_sub_plan(view_id, &mut plan.pre, &self.tables, input, source_id)
    }

    /// Check if a source_id is co-partitioned for a view (from cached plan).
    pub fn plan_source_co_partitioned(&mut self, view_id: i64, source_id: i64) -> bool {
        if !self.ensure_compiled(view_id) {
            return false;
        }
        self.cache
            .get(&view_id)
            .map(|p| p.co_partitioned.contains(&source_id))
            .unwrap_or(false)
    }

    // ── Ingestion ───────────────────────────────────────────────────────

    /// Ingest a batch into a table's store + index projections.
    /// Port of `registry.ingest_to_family()` for user tables.
    ///
    /// Stages:
    /// 1. unique_pk enforcement (retract existing, dedup intra-batch)
    /// 2. store.ingest_batch
    /// 3. index projection
    pub fn ingest_to_family(&mut self, table_id: i64, batch: Batch) -> i32 {
        if self.ingest_returning_effective(table_id, batch).is_some() {
            0
        } else {
            -1
        }
    }

    /// Ingest a borrowed batch (no clone) for the common non-unique-PK path.
    /// For unique_pk tables, falls back to cloning + `ingest_returning_effective`.
    pub fn ingest_by_ref(&mut self, table_id: i64, batch: &Batch) -> i32 {
        let entry = match self.tables.get_mut(&table_id) {
            Some(e) => e,
            None => return -1,
        };

        if entry.unique_pk() {
            return self.ingest_to_family(table_id, batch.clone_batch());
        }

        if batch.count == 0 {
            return 0;
        }

        let schema = entry.schema;
        Self::ingest_store_and_indices(table_id, entry, &schema, batch);

        0
    }

    /// Ingest a batch and return the effective batch (after unique_pk
    /// enforcement) — what downstream views need to see. `None` means exactly
    /// "table not registered"; a storage-apply failure never returns
    /// (`ingest_store_and_indices` aborts).
    pub fn ingest_returning_effective(&mut self, table_id: i64, batch: Batch) -> Option<Batch> {
        let entry = match self.tables.get_mut(&table_id) {
            Some(e) => e,
            None => {
                gnitz_warn!("dag: ingest_returning_effective — table_id={} not registered", table_id);
                return None;
            }
        };

        let schema = entry.schema;
        let unique_pk = entry.unique_pk();
        // unique_pk ⟹ Partitioned: every SQL-created base table is registered
        // Partitioned; system tables (the only Borrowed handles) are never
        // unique_pk, so a unique_pk relation always resolves to a PartitionedTable.
        // The debug_assert turns any future stray Borrowed+unique_pk registration
        // into a loud failure in debug; a release build passes the batch through
        // unenforced rather than skipping enforcement on a missing partitioned store.
        let effective_batch = if unique_pk {
            match entry.handle.as_partitioned_mut() {
                Some(ptable) => Self::enforce_unique_pk(ptable, &schema, batch),
                None => {
                    debug_assert!(false, "unique_pk relation {table_id} must be a PartitionedTable");
                    batch
                }
            }
        } else {
            batch
        };

        if effective_batch.count == 0 {
            return Some(effective_batch);
        }

        let entry = self.tables.get_mut(&table_id).unwrap();

        Self::ingest_store_and_indices(table_id, entry, &schema, &effective_batch);

        Some(effective_batch)
    }

    /// Project all index batches from `source`, ingest a clone into the store,
    /// then drain the projected index batches into their respective index tables.
    /// Shared by `ingest_by_ref` and `ingest_returning_effective`.
    ///
    /// A storage error here means committed (or SAL-replayed) data was not
    /// applied while the client already holds a durability ACK, so process state
    /// has diverged from the durable SAL. That is fatal at the point of
    /// detection: `gnitz_fatal_abort!` and let restart + SAL replay re-apply the
    /// batch (its WAL zone stays above the flushed-shard watermark, so it *will*
    /// be replayed). Silent swallowing is the one unsound response — it neither
    /// applies nor replays the entry, and the next checkpoint orphans it.
    fn ingest_store_and_indices(table_id: i64, entry: &mut TableEntry, schema: &SchemaDescriptor, source: &Batch) {
        let index_batches: Vec<Batch> = entry
            .index_circuits
            .iter()
            .map(|ic| Self::batch_project_index(source, ic.col_indices.as_slice(), schema, &ic.index_schema))
            .collect();

        if let Err(e) = inject_ingest_apply_error("store", entry.handle.ingest_borrowed_batch(source)) {
            crate::gnitz_fatal_abort!(
                "dag: base-table ingest failed (table_id={}): {} — committed data \
                 not applied, state diverged from durable SAL; aborting for \
                 restart+replay",
                table_id,
                e,
            );
        }

        for (ic, idx_batch) in entry.index_circuits.iter_mut().zip(index_batches) {
            if idx_batch.count > 0 {
                let index_id = ic.index_id;
                if let Err(e) = inject_ingest_apply_error("index", ic.table_mut().ingest_owned_batch(idx_batch)) {
                    crate::gnitz_fatal_abort!(
                        "dag: secondary-index ingest failed (table_id={}, index_id={}): {} \
                         — index diverged from base table; aborting for restart+replay",
                        table_id,
                        index_id,
                        e,
                    );
                }
            }
        }
    }

    /// Flush a table's WAL through the store handle and every index circuit.
    /// An unregistered `table_id` is a caller bug (debug_assert), a no-op in
    /// release; `Err` is always a storage fault.
    pub fn flush(&mut self, table_id: i64) -> Result<(), StorageError> {
        let Some(entry) = self.tables.get_mut(&table_id) else {
            debug_assert!(false, "flush of unregistered table_id {table_id}");
            return Ok(());
        };
        entry.handle.flush()?;
        for ic in &mut entry.index_circuits {
            ic.table_mut().flush()?;
        }
        Ok(())
    }

    /// Flush `view_id`'s trace, aborting the process on a storage fault: the
    /// callers flush a view they just ingested or backfilled, so a failure is
    /// a RAM-tier spill fault — the trace can no longer be bounded, and
    /// continuing would grow memory unchecked under a sustained fault.
    /// Restart re-derives the view from its base tables (the same disk fault
    /// would already abort the base ingest via `ingest_store_and_indices`).
    pub fn flush_view_or_abort(&mut self, view_id: i64) {
        if let Err(e) = self.flush(view_id) {
            crate::gnitz_fatal_abort!(
                "dag: view trace flush failed (view_id={}): {} — view state \
                 cannot be bounded; aborting for restart+re-derive",
                view_id,
                e,
            );
        }
    }

    /// Collect the tables the base checkpoint round (`FLAG_FLUSH`) flushes:
    /// every registered user relation's partitions plus its index-circuit
    /// tables. System tables (`StoreHandle::Borrowed`) are skipped — workers
    /// never barrier-flush their inherited `_sys` copies. `SalReplay` partitions
    /// publish (`Pending`); rederived partitions and index tables fold to RAM
    /// (`DoneInline`), which the generic flush loop consumes.
    ///
    /// Same `*mut Table` validity argument as `collect_ephemeral_flush_tables`
    /// below.
    pub(crate) fn collect_base_flush_tables(&mut self) -> Vec<*mut Table> {
        let mut out: Vec<*mut Table> = Vec::new();
        for entry in self.tables.values_mut() {
            if let Some(pt) = entry.handle.as_partitioned_mut() {
                for t in pt.partitions_mut() {
                    out.push(t as *mut Table);
                }
            }
            for ic in &mut entry.index_circuits {
                out.push(ic.table_mut() as *mut Table);
            }
        }
        out
    }

    /// Collect the tables the ephemeral checkpoint round force-persists, in two
    /// disjoint sets: (1) every compiled view plan's operator-trace tables, and
    /// (2) every view's output-store partitions. The worker flushes set 1 fully
    /// durable before set 2 (the flush-ordering invariant: any output manifest at
    /// generation G implies that view's traces are durable at G).
    ///
    /// Returned as raw `*mut Table` — owned trace tables are not in `self.tables`
    /// so they cannot be keyed by `tid`, and the engine already passes
    /// `*mut Table`. Valid because the worker flush handler is a synchronous `fn`
    /// on a single-threaded process: no reactor yield and no concurrent
    /// `cache`/`tables` mutation, so the partition set is frozen for the flush.
    /// `cache` and `tables` are separate fields (clean disjoint borrow) and the
    /// two sets are disjoint allocations (scratch dirs vs the view dir). Index
    /// tables are excluded: they live in `TableEntry::index_circuits`, not the
    /// plan cache, and stay erase-at-boot.
    pub(crate) fn collect_ephemeral_flush_tables(&mut self) -> (Vec<*mut Table>, Vec<*mut Table>) {
        fn collect_vm(vm: &mut vm::VmHandle, out: &mut Vec<*mut Table>) {
            // Null owned cursors before the fold so none holds a stale snapshot.
            vm.null_owned_cursors();
            for owned in vm.owned_tables.iter_mut() {
                out.push(&mut **owned as *mut Table);
            }
        }

        let mut traces: Vec<*mut Table> = Vec::new();
        for plan in self.cache.values_mut() {
            collect_vm(&mut plan.pre.vm, &mut traces);
            if let Some(post) = plan.post.as_mut() {
                collect_vm(&mut post.vm, &mut traces);
            }
            if let Some(sb) = plan.side_b.as_mut() {
                collect_vm(&mut sb.plan.vm, &mut traces);
            }
        }

        let mut outputs: Vec<*mut Table> = Vec::new();
        for entry in self.tables.values() {
            if entry.kind != RelationKind::View {
                continue;
            }
            // Views are always `Partitioned`; a replicated view holds exactly its
            // one worker-owned partition (no `part_0`-only miss).
            if let Some(pt) = entry.handle.as_partitioned_mut() {
                for t in pt.partitions_mut() {
                    outputs.push(t as *mut Table);
                }
            }
        }
        (traces, outputs)
    }

    // ── DAG traversal helpers ───────────────────────────────────────────

    /// Seed the initial pending list for a DAG traversal.
    /// Returns entries sorted descending by depth (shallowest at tail for
    /// O(1) pop) plus a position index for merge-on-collision lookups.
    fn build_pending(
        &self,
        view_ids: &[i64],
        source_id: i64,
        delta: Batch,
    ) -> (Vec<PendingEntry>, FxHashMap<(i64, i64), usize>) {
        let last_valid_idx = view_ids.iter().rposition(|&vid| self.tables.contains_key(&vid));
        let mut delta_opt = Some(delta);
        let mut pending: Vec<PendingEntry> = Vec::new();
        if let Some(last_idx) = last_valid_idx {
            for (i, &vid) in view_ids.iter().enumerate() {
                let depth = match self.tables.get(&vid) {
                    Some(e) => e.depth,
                    None => continue,
                };
                let b = if i == last_idx {
                    delta_opt.take().unwrap()
                } else {
                    delta_opt.as_ref().unwrap().clone_batch()
                };
                pending.push(PendingEntry {
                    depth,
                    view_id: vid,
                    source_id,
                    batch: b,
                });
            }
        }
        if let Some(d) = delta_opt {
            crate::storage::batch_pool::recycle(d);
        }
        let mut pending_pos: FxHashMap<(i64, i64), usize> = FxHashMap::default();
        Self::resort_pending(&mut pending, &mut pending_pos);
        (pending, pending_pos)
    }

    // ── drive_dag (DAG traversal driver) ────────────────────────────────

    /// Drives DAG evaluation from a base delta. Seeds the pending queue from
    /// `source_id`'s direct dependents, then repeatedly pops the shallowest
    /// pending edge, runs `execute` on it, ingests the output, and fans that
    /// output — or, for a view that produced nothing, an empty placeholder so
    /// collective exchange rounds stay in lockstep across workers — onto each
    /// downstream edge, until the queue drains. Every modified view trace is
    /// flushed exactly once after the DAG settles.
    ///
    /// `execute` is the per-view step (the exchange-aware
    /// `execute_multi_worker_step`, capturing its `ExchangeCallback`). It takes
    /// the engine as an explicit `&mut Self` argument — borrowed only for the
    /// call, never captured — and returns the view's output delta, or
    /// `None`/empty when the view produced nothing this epoch.
    fn drive_dag(
        &mut self,
        source_id: i64,
        delta: Batch,
        mut execute: impl FnMut(&mut Self, i64, Batch, i64) -> Option<Batch>,
    ) -> i32 {
        self.get_dep_map();
        let view_ids: Vec<i64> = self.dep.forward.get(&source_id).cloned().unwrap_or_default();
        if view_ids.is_empty() {
            return 0;
        }

        let (mut pending, mut pending_pos) = self.build_pending(&view_ids, source_id, delta);
        let mut dirty_views: FxHashSet<i64> = FxHashSet::default();

        while let Some(entry) = pending.pop() {
            let view_id = entry.view_id;
            let src_id = entry.source_id;
            let input = entry.batch;
            pending_pos.remove(&(view_id, src_id));

            // The table may have been dropped between queueing and now.
            if !self.tables.contains_key(&view_id) {
                crate::storage::batch_pool::recycle(input);
                continue;
            }

            let out_delta = execute(&mut *self, view_id, input, src_id);
            let has_output = out_delta.as_ref().is_some_and(|b| b.count > 0);

            if has_output {
                dirty_views.insert(view_id);
                if self.dep.forward.get(&view_id).is_none_or(|d| d.is_empty()) {
                    // Terminal view: move the batch into its family (no clone for
                    // unique_pk) — there is nothing downstream to fan onto.
                    self.ingest_to_family(view_id, out_delta.unwrap());
                    continue;
                }
                self.ingest_by_ref(view_id, out_delta.as_ref().unwrap());
            }

            // Fan the output — or, for a view that produced nothing, an empty
            // placeholder — onto each dependent edge. Borrow the dep list (disjoint
            // from `&self.tables`) rather than cloning; `map_or` yields an empty
            // slice for a view with no dependents, which queue_dependents no-ops.
            let src_schema = self.tables[&view_id].schema;
            let delta = if has_output { out_delta.as_ref() } else { None };
            let dep_view_ids = self.dep.forward.get(&view_id).map_or(&[][..], Vec::as_slice);
            Self::queue_dependents(
                &mut pending,
                &mut pending_pos,
                &self.tables,
                dep_view_ids,
                view_id,
                src_schema,
                delta,
            );
            if let Some(batch) = out_delta {
                crate::storage::batch_pool::recycle(batch);
            }
        }

        // Flush each modified view trace exactly once after the full DAG settles.
        for vid in dirty_views {
            self.flush_view_or_abort(vid);
        }

        0
    }

    /// Multi-worker DAG evaluation with exchange IPC. Each view runs through
    /// `execute_multi_worker_step` (the pre / exchange / post dance); the empty
    /// placeholders `drive_dag` fans downstream keep collective exchange rounds in
    /// lockstep across workers.
    pub fn evaluate_dag_multi_worker<E: ExchangeCallback>(
        &mut self,
        source_id: i64,
        delta: Batch,
        exchange: &mut E,
    ) -> i32 {
        self.drive_dag(source_id, delta, |this, view_id, input, src_id| {
            this.execute_multi_worker_step(view_id, input, src_id, exchange)
        })
    }

    /// Run one multi-worker DAG step: ensure the view's circuit is compiled, then
    /// dispatch on its exchange shape and run the view's epoch, returning the
    /// output delta (`None` when a phase produced nothing). The arms, in priority
    /// order:
    /// 1. Range join — relay the source delta (eq-prefix scatter for a band join,
    ///    broadcast for a pure range join, decided master-side in `prepare_relay`),
    ///    then `run_exchange_pre_post`. Checked FIRST: a range join has
    ///    `has_exchange == true` (its output ExchangeShard) and would otherwise be
    ///    swallowed by arm 3, which never relays the join input.
    /// 2. Binary set-op (`has_exchange` + side B) — two HashRow→ExchangeShard
    ///    sides via `run_two_sided`.
    /// 3. Plain exchange (`has_exchange`) — `run_exchange_pre_post` on the local
    ///    delta, skipping the output IPC when `view_skips_exchange`.
    /// 4. Join shard (`get_join_shard_cols` non-empty AND NOT
    ///    `plan_source_co_partitioned`) — scatter before the pre phase.
    /// 5. No exchange — single-phase execute.
    fn execute_multi_worker_step<E: ExchangeCallback>(
        &mut self,
        view_id: i64,
        input: Batch,
        src_id: i64,
        exchange: &mut E,
    ) -> Option<Batch> {
        self.ensure_compiled(view_id);

        // A view whose sources are all replicated holds the full copy of every source
        // and receives the full (broadcast) delta on every worker, so it computes its
        // entire result locally: the output is itself replicated and the worker-0 scan
        // reads it whole. Run it exactly as single-worker mode does — no exchange IPC.
        // (Equi-join and GROUP BY already reach a local path via the co-partition skip
        // and reduce_multi_local; this intercepts the compiled-exchange shapes: binary
        // set-ops, SELECT DISTINCT, range/band joins.) Every worker evaluates
        // view_all_sources_replicated identically, so they skip the same exchange rounds
        // and the collective barrier stays balanced.
        if self.view_all_sources_replicated(view_id) {
            return self.execute_epoch_for_dag(view_id, input, src_id);
        }

        // Only `has_exchange` is eager — two arms test it. The two single-use
        // checks (`view_skips_exchange` and the join-shard-cols emptiness test)
        // are evaluated at their sole consumer arm below: the range-join arm
        // (taken first) and the set-op arm read neither, so deferring keeps them
        // off the cache lookup and the first-call circuit load.
        let has_exchange = self.view_needs_exchange(view_id);

        if self.view_range_join_n_eq(view_id).is_some() {
            // Arm 1 — range join: relay the source delta, then run the unary
            // exchange pipeline. No co-partition shortcut applies: a pure range
            // probe needs the full delta even when the join key equals the source
            // PK, and a band join always routes through the relay by decision (see
            // `prepare_relay`) rather than taking a co-partition exchange-skip — so
            // the output exchange is unconditional (`skip = false`).
            let input = self.ensure_wire_schema(input, src_id);
            let bc = exchange.do_exchange(view_id, &input, src_id);
            self.run_exchange_pre_post(view_id, bc, src_id, false, exchange)
        } else if has_exchange && self.view_has_side_b(view_id) {
            // Arm 2 — binary set-op: two HashRow→ExchangeShard sides, each
            // scattered by its hash PK, then combined.
            self.run_two_sided(view_id, input, src_id, |pre, side_src| {
                exchange.do_exchange(view_id, &pre, side_src)
            })
        } else if has_exchange {
            // Arm 3 — plain exchange: the unary pipeline on the local delta,
            // eliding the output IPC when the shuffle is a proven no-op.
            let skip = self.view_skips_exchange(view_id);
            self.run_exchange_pre_post(view_id, input, src_id, skip, exchange)
        } else if !self.join_shard_cols_is_empty(view_id, src_id) {
            // Arm 4 — join shard: scatter the delta by the join-shard cols before
            // the pre phase, unless the source is already co-partitioned on them.
            // The col list is tested only here, so its emptiness check is deferred
            // to this arm rather than evaluated eagerly above the chain.
            if self.plan_source_co_partitioned(view_id, src_id) {
                self.execute_pre_phase(view_id, input, src_id)
            } else {
                let input = self.ensure_wire_schema(input, src_id);
                let exchanged = exchange.do_exchange(view_id, &input, src_id);
                self.execute_pre_phase(view_id, exchanged, src_id)
            }
        } else {
            // Arm 5 — no exchange: single-phase execute.
            self.execute_pre_phase(view_id, input, src_id)
        }
    }

    /// Drive ONE view's epoch for a distributed-backfill chunk and ingest its
    /// output into the view's family. Returns true iff the view produced rows
    /// (the caller flushes the view once after the final chunk).
    ///
    /// This is the **view-scoped** analogue of `evaluate_dag_multi_worker`,
    /// which drives `source_id`'s *whole* dependent closure. A live CREATE VIEW
    /// must drive only the new view: the source already has populated existing
    /// dependents that a closure re-drive would double-count. Boot has no such
    /// dependents (every view starts empty), so it keeps the closure driver.
    /// The new view has no dependents of its own yet, so there is nothing to
    /// fan downstream — just run its step and ingest.
    pub fn backfill_view_step_multi_worker<E: ExchangeCallback>(
        &mut self,
        view_id: i64,
        source_id: i64,
        delta: Batch,
        exchange: &mut E,
    ) -> bool {
        if !self.tables.contains_key(&view_id) {
            crate::storage::batch_pool::recycle(delta);
            return false;
        }
        match self.execute_multi_worker_step(view_id, delta, source_id, exchange) {
            Some(out) if out.count > 0 => {
                self.ingest_to_family(view_id, out);
                true
            }
            Some(out) => {
                crate::storage::batch_pool::recycle(out);
                false
            }
            None => false,
        }
    }

    // ── Private helpers ─────────────────────────────────────────────────

    /// Restore the pending queue's descending-depth order and rebuild the
    /// `(view_id, source_id) → index` lookup. Both DAG drivers call this after
    /// pushing a new entry.
    fn resort_pending(pending: &mut [PendingEntry], pending_pos: &mut FxHashMap<(i64, i64), usize>) {
        pending.sort_by_key(|n| std::cmp::Reverse(n.depth));
        pending_pos.clear();
        for (i, pe) in pending.iter().enumerate() {
            pending_pos.insert((pe.view_id, pe.source_id), i);
        }
    }

    /// Queue `view_id`'s output onto each dependent's pending edge.
    ///
    /// `delta` is the producer's output, or `None` when the producer fired with
    /// no output (multi-worker queues empty placeholders so exchange-dependent
    /// views still run; the single-source driver returns early on empty and
    /// always passes `Some`). When present it is merged into an existing pending
    /// entry, or cloned into a new one; when absent a new entry gets an empty
    /// placeholder batch and existing entries are left untouched.
    ///
    /// Every queued batch is labelled with `src_schema` — the PRODUCER's output
    /// schema, never the consumer's. A JOIN consumer's combine-widened final
    /// schema is a different width than the operand batch on this edge; tagging
    /// the operand with it would trip the vm seed guard. `src_schema` must be
    /// `self.tables[&view_id].schema`; `tables` is read only for dependents'
    /// depth, so the immutable borrow does not conflict with the snapshot.
    fn queue_dependents(
        pending: &mut Vec<PendingEntry>,
        pending_pos: &mut FxHashMap<(i64, i64), usize>,
        tables: &FxHashMap<i64, TableEntry>,
        dep_view_ids: &[i64],
        view_id: i64,
        src_schema: SchemaDescriptor,
        delta: Option<&Batch>,
    ) {
        let mut pushed = false;
        for &dep_id in dep_view_ids {
            let dep_depth = match tables.get(&dep_id) {
                Some(e) => e.depth,
                None => continue,
            };

            if let Some(&existing_idx) = pending_pos.get(&(dep_id, view_id)) {
                if let (true, Some(d)) = (existing_idx < pending.len(), delta) {
                    let existing =
                        std::mem::replace(&mut pending[existing_idx].batch, Batch::empty_with_schema(&src_schema));
                    let schema = existing.schema.unwrap_or(src_schema);
                    let merged = ops::op_union(existing, Some(d), &schema);
                    pending[existing_idx].batch = merged;
                }
            } else {
                let batch = match delta {
                    Some(d) => d.clone_batch(),
                    None => Batch::with_schema(src_schema, 0),
                };
                pending.push(PendingEntry {
                    depth: dep_depth,
                    view_id: dep_id,
                    source_id: view_id,
                    batch,
                });
                pushed = true;
            }
        }
        // New entries are not recorded in `pending_pos` here: the end-of-loop
        // `resort_pending` rebuilds it wholesale, and nothing reads a new entry's
        // slot before then — `dep_view_ids` is deduped (get_dep_map), so no later
        // iteration probes a `(dep_id, view_id)` key an earlier one pushed. The
        // pre-existing slots the merge branch *does* probe stay valid because
        // `pending` is only appended to here (the merge mutates a batch in place,
        // never moves an entry). One stable resort at the end restores descending
        // depth order while preserving per-depth insertion order. A resort is
        // needed iff at least one edge was pushed; pure merges change neither
        // membership nor depth.
        if pushed {
            Self::resort_pending(pending, pending_pos);
        }
    }

    /// Build ext cursor handles indexed by register ID.
    /// Returns (cursor_ptrs, storage_owner). Storage must outlive cursor_ptrs.
    #[allow(clippy::vec_box)]
    fn build_ext_cursors(
        tables: &FxHashMap<i64, TableEntry>,
        ext_trace_regs: &[(u16, i64)],
        num_regs: usize,
    ) -> (Vec<*mut libc::c_void>, Vec<Box<CursorHandle>>) {
        let mut ptrs: Vec<*mut libc::c_void> = vec![std::ptr::null_mut(); num_regs];
        let mut storage: Vec<Box<CursorHandle>> = Vec::new();
        for &(reg_id, table_id) in ext_trace_regs {
            if let Some(entry) = tables.get(&table_id) {
                // External-trace reads are the operator-state read path: keep
                // them compacting so L0 on intermediate/trace tables stays
                // bounded (no background compactor yet). These are not
                // validators, so a compaction Err safely degrades to `None`.
                #[allow(clippy::disallowed_methods)] // explicit maintenance: operator-state trace read
                let cursor_opt = entry.handle.create_cursor_compacting().ok();
                if let Some(ch) = cursor_opt {
                    storage.push(Box::new(ch));
                    if (reg_id as usize) < num_regs {
                        // Derive the raw pointer AFTER the move, from the box's
                        // stable heap address inside `storage`. Deriving it from
                        // a local Box and then moving the box would invalidate it
                        // under Stacked/Tree Borrows. The CursorHandle heap
                        // allocation is stable across later Vec growth, so the
                        // pointer remains valid.
                        let p = storage.last_mut().unwrap().as_mut() as *mut CursorHandle;
                        ptrs[reg_id as usize] = p as *mut libc::c_void;
                    }
                }
            }
        }
        (ptrs, storage)
    }

    /// Execute one view's epoch with the pre/post plan split but no exchange IPC —
    /// the backfill path, where a single owner sees every shard.
    fn execute_epoch_for_dag(&mut self, view_id: i64, input: Batch, source_id: i64) -> Option<Batch> {
        if !self.ensure_compiled(view_id) {
            gnitz_warn!("dag: execute_epoch_for_dag — no plan for view_id={}", view_id);
            return None;
        }
        let has_post = self.cache.get(&view_id).unwrap().post.is_some();

        if self.view_has_side_b(view_id) {
            // Two-sided set-op: single worker owns every shard, so run both
            // sides locally (no IPC, relay is identity) and combine.
            return self.run_two_sided(view_id, input, source_id, |pre, _| pre);
        }

        if has_post {
            // Exchange plan: single-worker runs pre then post sequentially.
            // The pre output may be hash-reindexed and unsorted (SELECT DISTINCT);
            // consolidate before the post phase, whose distinct/join operators
            // assume sorted, weight-merged input.
            let exchange_schema = self.exchange_or_view_schema(view_id);
            let pre_result = self
                .execute_pre_phase(view_id, input, source_id)
                .unwrap_or_else(|| Batch::with_schema(exchange_schema, 0));
            let consolidated = Self::consolidate_exchanged(pre_result, &exchange_schema);
            self.execute_post_phase(view_id, consolidated)
        } else {
            self.execute_pre_phase(view_id, input, source_id)
        }
    }

    /// Execute one sub-pipeline epoch. Takes the sub-plan by mutable reference
    /// (to reach the VM's regfile and owned-cursor state) and the engine's table
    /// map by shared reference (for external-trace cursor construction). Both
    /// come from different fields of `DagEngine`, so the caller can hold them as
    /// independent borrows.
    ///
    /// `source_id > 0` selects the sub-plan's input register from its
    /// `source_reg_map`; pass `0` when the sub-plan has a single unambiguous input.
    fn execute_sub_plan(
        view_id: i64,
        sub: &mut SubPlan,
        tables: &FxHashMap<i64, TableEntry>,
        input: Batch,
        source_id: i64,
    ) -> Option<Batch> {
        let (cursor_ptrs, _cursor_storage) =
            Self::build_ext_cursors(tables, &sub.ext_trace_regs, sub.num_regs as usize);
        let actual_in_reg = if source_id > 0 {
            sub.source_reg_map.get(&source_id).copied().unwrap_or(sub.in_reg)
        } else {
            sub.in_reg
        };
        sub.vm.refresh_owned_cursors();
        Self::vm_epoch_result(
            view_id,
            vm::execute_epoch(
                &sub.vm.program,
                &mut sub.vm.regfile,
                input,
                actual_in_reg,
                sub.out_reg,
                &cursor_ptrs,
                &sub.vm.owned_trace_regs,
            ),
        )
    }

    fn execute_pre_phase(&mut self, view_id: i64, input: Batch, source_id: i64) -> Option<Batch> {
        let plan = self.cache.get_mut(&view_id)?;
        Self::execute_sub_plan(view_id, &mut plan.pre, &self.tables, input, source_id)
    }

    fn execute_post_phase(&mut self, view_id: i64, input: Batch) -> Option<Batch> {
        let plan = self.cache.get_mut(&view_id)?;
        Self::execute_sub_plan(view_id, plan.post.as_mut().unwrap(), &self.tables, input, 0)
    }

    /// Whether a view's compiled plan carries a second exchange side (binary
    /// set-op). The plan must already be in the cache.
    fn view_has_side_b(&self, view_id: i64) -> bool {
        self.cache.get(&view_id).map(|p| p.side_b.is_some()).unwrap_or(false)
    }

    /// Run the post phase seeding several exchange-input registers (one per
    /// set-op side). Used after both sides have been exchanged/consolidated.
    fn execute_post_multi(&mut self, view_id: i64, inputs: Vec<(u16, Batch)>) -> Option<Batch> {
        let plan = self.cache.get_mut(&view_id)?;
        let post = plan.post.as_mut()?;
        let (cursor_ptrs, _cursor_storage) =
            Self::build_ext_cursors(&self.tables, &post.ext_trace_regs, post.num_regs as usize);
        post.vm.refresh_owned_cursors();
        Self::vm_epoch_result(
            view_id,
            vm::execute_epoch_multi(
                &post.vm.program,
                &mut post.vm.regfile,
                inputs,
                post.out_reg,
                &cursor_ptrs,
                &post.vm.owned_trace_regs,
            ),
        )
    }

    /// Force a real sort+weight-merge of a post-exchange batch. The exchange
    /// relay concatenates rows from all workers (and a HashRow reindex scrambles
    /// PK order), so the result is not globally sorted — but its `sorted` /
    /// `consolidated` flags are not reliably cleared by the relay path. Clearing
    /// them here guarantees `into_consolidated` actually sorts, which the
    /// downstream merge-walk join/distinct operators depend on.
    fn consolidate_exchanged(mut batch: Batch, schema: &SchemaDescriptor) -> Batch {
        batch.downgrade();
        batch.into_consolidated(schema)
    }

    /// Normalize a VM epoch result into the DAG's `Option<Batch>` convention:
    /// a positive-count batch, or `None` for an empty epoch.
    ///
    /// A VM `Err` is an unrecoverable internal-invariant violation — the only
    /// codes (`-10`/`-11`, vm.rs) mean a Reduce trace cursor the compiler
    /// promised is unbound, i.e. the compiled circuit is malformed. Every VM
    /// operator is otherwise infallible, so there is no data- or query-level
    /// fault to surface. Continuing would drop the delta and permanently desync
    /// the view's integral (and in multi-worker, a skipped exchange round
    /// deadlocks the cluster), so we fail stop. In multi-worker the master's
    /// `watchdog` reaps the exited worker and tears the tick down;
    /// single-worker exits the process. If a recoverable (data/query-level) VM
    /// error is ever introduced, it must be a distinct code routed to
    /// transaction-level failure — never funneled here.
    fn vm_epoch_result(view_id: i64, r: Result<Option<Batch>, i32>) -> Option<Batch> {
        match r {
            Ok(Some(batch)) if batch.count > 0 => Some(batch),
            Ok(_) => None,
            Err(code) => gnitz_fatal_abort!(
                "dag: VM execution error {} for view_id={} — malformed circuit, \
                 cannot continue without producing inconsistent view state",
                code,
                view_id,
            ),
        }
    }

    /// Per-side exchange metadata snapshot for both sides of a two-sided join.
    fn two_sided_meta(&self, view_id: i64) -> Option<(ExchangeSideMeta, ExchangeSideMeta)> {
        let p = self.cache.get(&view_id)?;
        let sb = p.side_b.as_ref()?;
        // A two-sided plan always records side A's pre-exchange output schema;
        // the view's final (combine-widened) schema is a different width for a
        // JOIN and would mislabel side A's operand batch.
        let a_schema = p
            .exchange_in_schema
            .expect("two-sided plan must carry side A's exchange_in_schema");
        // Each set-op side scans exactly one relation (the planner rejects a
        // JOIN on either side), so both ids are real tables — never
        // single_source's ambiguous sentinel 0. run_two_sided routes by
        // `src_id == source_id`; a 0 would silently drop a real-table delta.
        debug_assert!(
            p.side_a_source_id != 0 && sb.source_id != 0,
            "two-sided set-op side resolved to ambiguous source 0"
        );
        Some((
            (a_schema, p.side_a_source_id, p.post.as_ref().unwrap().in_reg),
            (sb.exchange_schema, sb.source_id, sb.post_seed_reg),
        ))
    }

    /// Execute one side of a two-sided set-op: run the sub-pipeline on `input`
    /// (skipped entirely when `None`), relay the result, and consolidate.
    /// Takes the sub-plan and table map as independent borrows.
    #[allow(clippy::too_many_arguments)]
    fn run_one_side(
        view_id: i64,
        sub: &mut SubPlan,
        tables: &FxHashMap<i64, TableEntry>,
        input: Option<Batch>,
        src_id: i64,
        schema: SchemaDescriptor,
        source: i64,
        relay: &mut impl FnMut(Batch, i64) -> Batch,
    ) -> Batch {
        match input {
            Some(delta) => {
                let mut pre = Self::execute_sub_plan(view_id, sub, tables, delta, src_id)
                    .unwrap_or_else(|| Batch::with_schema(schema, 0));
                pre.set_schema(schema);
                Self::consolidate_exchanged(relay(pre, source), &schema)
            }
            None => Batch::empty_with_schema(&schema),
        }
    }

    /// Execute a two-sided set-op view: run each side's sub-pipeline, hand its
    /// output through `relay`, consolidate, then run the combine over both.
    ///
    /// `relay(pre, side_source)` is the repartition step: multi-worker passes
    /// `exchange.do_exchange` (keyed by the side's source so the two IPC rounds
    /// don't collide in the master accumulator); single-worker passes identity
    /// (one worker owns all shards, so no IPC). The combine (Union / positive_part)
    /// needs sorted, weight-merged input, hence the consolidate.
    fn run_two_sided(
        &mut self,
        view_id: i64,
        input: Batch,
        src_id: i64,
        mut relay: impl FnMut(Batch, i64) -> Batch,
    ) -> Option<Batch> {
        let ((a_schema, a_source, a_seed), (b_schema, b_source, b_seed)) = self.two_sided_meta(view_id)?;

        // The delta belongs to whichever side(s) scan its source. An inactive
        // side has no delta this epoch, so its pre-phase (a linear
        // single-relation reshuffle) would emit nothing and the combine
        // integrates an empty seed as a no-op — skip its VM pass, its exchange
        // round, and its consolidate, and seed the combine directly.
        //
        // `a_needs`/`b_needs` derive only from the plan sources and `src_id`,
        // both identical on every worker, so all workers skip the same side's
        // `do_exchange`; the per-(view_id, source_id) barrier stays balanced.
        // Both true only when both sides scan one relation (e.g. `a UNION a`):
        // clone so each side gets the delta, otherwise move to the active side.
        let (a_needs, b_needs) = (src_id == a_source, src_id == b_source);
        debug_assert!(
            a_needs || b_needs,
            "run_two_sided view {view_id}: delta source {src_id} matches neither \
             side ({a_source}, {b_source})",
        );
        let (a_in, b_in) = match (a_needs, b_needs) {
            (true, true) => (Some(input.clone_batch()), Some(input)),
            (true, false) => (Some(input), None),
            (false, true) => (None, Some(input)),
            (false, false) => unreachable!("run_two_sided view {view_id}: source {src_id} matches neither side"),
        };

        // Each scope ends before the next so the mutable cache borrow does not
        // overlap with execute_post_multi's borrow via self.
        let cons_a = {
            let plan = self.cache.get_mut(&view_id).unwrap();
            Self::run_one_side(
                view_id,
                &mut plan.pre,
                &self.tables,
                a_in,
                src_id,
                a_schema,
                a_source,
                &mut relay,
            )
        };
        let cons_b = {
            let plan = self.cache.get_mut(&view_id).unwrap();
            Self::run_one_side(
                view_id,
                &mut plan.side_b.as_mut().unwrap().plan,
                &self.tables,
                b_in,
                src_id,
                b_schema,
                b_source,
                &mut relay,
            )
        };

        self.execute_post_multi(view_id, vec![(a_seed, cons_a), (b_seed, cons_b)])
    }

    /// Stamp a delta headed for the exchange wire with its source table's schema
    /// when it carries none: a row-bearing batch with a `None` schema emits
    /// `FLAG_HAS_DATA` without `FLAG_HAS_SCHEMA` and panics the reactor decode.
    fn ensure_wire_schema(&self, mut input: Batch, src_id: i64) -> Batch {
        if input.schema.is_none() {
            if let Some(entry) = self.tables.get(&src_id) {
                input.set_schema(entry.schema);
            }
        }
        input
    }

    /// Run a unary exchange view's epoch on the multi-worker path: pre phase →
    /// output exchange → post phase. The pre output (and any empty placeholder) is
    /// labelled with the exchange-input schema for the wire encode; the relayed
    /// batch is unsorted after the HashRow reindex / scatter, so it is
    /// consolidated before the post phase, whose distinct/join operators need
    /// sorted, weight-merged input. `skip_exchange` elides the output IPC when the
    /// shuffle is a proven no-op (`view_skips_exchange`).
    ///
    /// This is the multi-worker counterpart of `execute_epoch_for_dag`'s post
    /// path, which runs the same pre → consolidate → post with an identity relay
    /// and deliberately omits the wire-schema stamp (no IPC there).
    fn run_exchange_pre_post<E: ExchangeCallback>(
        &mut self,
        view_id: i64,
        pre_input: Batch,
        src_id: i64,
        skip_exchange: bool,
        exchange: &mut E,
    ) -> Option<Batch> {
        let exchange_schema = self.exchange_or_view_schema(view_id);
        let mut pre = self
            .execute_pre_phase(view_id, pre_input, src_id)
            .unwrap_or_else(|| Batch::with_schema(exchange_schema, 0));
        pre.set_schema(exchange_schema);
        let post_in = if skip_exchange {
            pre
        } else {
            exchange.do_exchange(view_id, &pre, 0)
        };
        let post_in = Self::consolidate_exchanged(post_in, &exchange_schema);
        self.execute_post_phase(view_id, post_in)
    }

    /// unique_pk contract: per-PK accumulated weight ∈ {0, 1}. A pushed row at
    /// |w| > 1 is the row repeated; retract-before-insert collapses repeats to
    /// one live instance (and a delete removes at most one), so normalize
    /// weights to ±1 before the enforcement walk. Must run on the input batch,
    /// not at append time: `append_batch_negated` re-reads the original row,
    /// so clamping only the appended copy would emit `+1` then `-w` for the
    /// same element and drive intra-batch dedup net-negative.
    fn normalize_unique_pk_weights(batch: &mut Batch) {
        for row in 0..batch.count {
            let w = batch.get_weight(row);
            if w > 1 {
                batch.set_weight(row, 1);
            } else if w < -1 {
                batch.set_weight(row, -1);
            }
        }
    }

    /// Enforce unique-PK semantics on an ingest batch: retract any stored row
    /// with the same PK before inserting the new one, and resolve duplicate PKs
    /// within the batch so each surviving PK nets to a single live row.
    ///
    /// Emits the stored-row retraction (`-1`, old payload) into the effective
    /// batch so downstream views see the old payload removed before the new one
    /// lands. Keys on `get_pk_bytes` (verbatim OPK) and dedups on `&[u8]` slices
    /// borrowed from the batch's PK region — correct for every PK width. Never
    /// round-trips through a native `u128`
    /// (which `opk_key` would re-encode, double-flipping a signed PK's sign bit,
    /// so the probe would match no stored row and the retraction would be
    /// silently dropped).
    fn enforce_unique_pk(ptable: &mut PartitionedTable, schema: &SchemaDescriptor, mut batch: Batch) -> Batch {
        // Empty-batch guard: `Batch::with_schema(_, 0)` still allocates (capacity
        // forced to ≥1, plus a pooled data buffer and a blob Vec). Empty batches
        // reach the engine via the `CatalogStore` ingest wrappers, which — unlike
        // the worker loop — do not pre-filter `count == 0`.
        if batch.count == 0 {
            return batch;
        }
        Self::normalize_unique_pk_weights(&mut batch);

        let mut effective = Batch::with_schema(*schema, batch.count * 2);
        // pk → batch row index of the last +1 insertion in this batch. Must be a
        // batch index (not an effective index): `append_batch_negated` reads from
        // `batch`, and the effective batch carries extra store-retraction rows
        // that break any 1:1 correspondence with `row`.
        let mut seen: FxHashMap<&[u8], usize> = FxHashMap::with_capacity_and_hasher(batch.count, Default::default());
        // pk set of store rows already retracted in this batch. `retract_pk_bytes`
        // is read-only (it only arms the `found_*` accessors), so emitting the
        // stored-row retraction more than once per PK would drive downstream
        // weights negative; this set gates the emission to exactly once.
        let mut store_retracted: FxHashSet<&[u8]> =
            FxHashSet::with_capacity_and_hasher(batch.count, Default::default());

        for row in 0..batch.count {
            let w = batch.get_weight(row);
            if w == 0 {
                continue;
            }
            let pkb = batch.get_pk_bytes(row);

            // Stored-row retraction — shared by insert and delete. Probe the
            // store and, the first time this PK is found, emit a retraction of
            // the stored (PK, payload) so downstream views drop the old payload.
            let (_existing_w, found) = ptable.retract_pk_bytes(pkb);
            if found && !store_retracted.contains(pkb) {
                // `retract_pk_bytes` armed the stored row as a `ColumnarSource`
                // view; copy it in at weight -1 via the canonical source-append.
                let found_row = ptable.found_row().expect("retract_pk_bytes reported a found row");
                effective.append_row_from_source_bytes(pkb, -1, &found_row, 0, None);
                store_retracted.insert(pkb);
            }

            if w > 0 {
                // Insert. If this PK was already inserted in this batch, retract
                // that earlier insertion (intra-batch upsert: last value wins).
                if let Some(prev_pos) = seen.get_mut(pkb) {
                    effective.append_batch_negated(&batch, *prev_pos, *prev_pos + 1);
                    *prev_pos = row;
                } else {
                    seen.insert(pkb, row);
                }
                effective.append_batch(&batch, row, row + 1);
            } else {
                // Delete (w < 0). The stored-row retraction above already emitted
                // the removal; here only cancel a prior intra-batch insertion and
                // clear `seen` so a later re-insert of this PK is not re-negated.
                // No `else`: a retraction of a key that is neither stored nor seen
                // has nothing to cancel — passing it through would store a
                // negative-weight phantom row (violating base-table positivity),
                // and dropping it is idempotent under delete replay.
                if let Some(&prev_pos) = seen.get(pkb) {
                    effective.append_batch_negated(&batch, prev_pos, prev_pos + 1);
                    seen.remove(pkb);
                }
            }
        }

        effective
    }

    /// Batch-level index projection.
    ///
    /// Compound-PK index schema layout:
    ///   `(indexed_col [promoted], src_pk_0, src_pk_1, …)`
    /// every column is in the PK, no payload columns. We hand-pack the index
    /// PK bytes: the leading slot is the indexed column value (low bytes of
    /// its LE form, zero-padded out to the index column's width), followed
    /// by each source PK column laid out contiguously after it.
    pub(crate) fn batch_project_index(
        src: &Batch,
        source_col_indices: &[u32],
        src_schema: &SchemaDescriptor,
        idx_schema: &SchemaDescriptor,
    ) -> Batch {
        let src_pk_stride = src_schema.pk_stride() as usize;
        let idx_stride = idx_schema.pk_stride() as usize;

        let mut out = Batch::with_schema(*idx_schema, src.count.max(1));
        // MAX_PK_BYTES bounds every index schema's pk_stride (asserted in
        // SchemaDescriptor::new), so the scratch PK buffer lives on the stack
        // with no per-batch heap allocation. The used [..idx_stride] prefix is
        // fully overwritten each row (the leading [..idx_key_size] OPK-encoded
        // indexed value(s) and trailing source PK suffix); the single zero-init
        // covers the (currently empty) tail.
        let mut idx_pk_buf = [0u8; crate::schema::MAX_PK_BYTES];

        let mb = src.as_mem_batch();

        // Per-column read/encode plan, hoisted once per call (this runs once
        // per index circuit on every base-table push) so the row loop does no
        // per-column method call or schema indexing.
        let spec = crate::schema::IndexKeySpec::new(source_col_indices, src_schema, idx_schema);
        let idx_key_size = spec.key_size();

        for row in 0..src.count {
            let weight = src.get_weight(row);
            if weight == 0 {
                continue;
            }
            // Leading index-key slots: each indexed value re-encoded OPK into
            // its promoted slot. The index table's PK region is
            // order-preserving like any other; seeks (has_pk / seek_by_index)
            // encode the same way. NULL in ANY indexed column ⇒ row not
            // indexed; retractions (weight < 0) DO project, so the index
            // entry retracts with its source row.
            if !spec.write_span(&mb, row, &mut idx_pk_buf) {
                continue;
            }
            // The source PK region is laid out in pk_indices order, so the
            // index's trailing PK suffix is byte-identical to the source's PK
            // (already OPK).
            idx_pk_buf[idx_key_size..idx_key_size + src_pk_stride].copy_from_slice(src.get_pk_bytes(row));
            out.extend_pk_bytes(&idx_pk_buf[..idx_stride]);
            out.extend_weight(&weight.to_le_bytes());
            // Index schema has zero payload columns, but the null_bmp region
            // is still part of the batch layout. Keep the per-row null-bmp
            // append so the batch's region cursors stay in lockstep with
            // `count` independent of `with_schema`'s zero-init.
            out.extend_null_bmp(&0u64.to_le_bytes());
            out.count += 1;
        }

        // `out` is `Raw` from `with_schema`; the `extend_*` loop above never raises
        // it, and the index-table ingest re-sorts/folds it.
        out
    }

    /// Close the DagEngine, dropping all cached plans. Test-only, like the
    /// `CatalogEngine::close` that drives it: the server never closes gracefully.
    #[cfg(test)]
    pub(crate) fn close(&mut self) {
        self.cache.clear();
        self.tables.clear();
        self.view_props.clear();
        self.dep = DepMap::default();
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::foundation::posix_io::raise_fd_limit_for_tests;

    fn dag_test_dir(name: &str) -> String {
        std::env::temp_dir()
            .join(format!("gnitz_dag_test_{name}"))
            .to_str()
            .unwrap()
            .to_owned()
    }

    fn make_test_table(name: &str) -> Box<Table> {
        let schema = SchemaDescriptor::default();
        let dir = dag_test_dir(name);
        let _ = std::fs::remove_dir_all(&dir);
        Box::new(Table::new(&dir, name, schema, 99, 256 * 1024, RecoverySource::Rederive).unwrap())
    }

    #[test]
    fn test_dag_engine_lifecycle() {
        let mut dag = DagEngine::new();
        assert!(dag.cache.is_empty());
        assert!(dag.tables.is_empty());
        dag.close();
    }

    #[test]
    fn test_register_unregister_table() {
        let mut dag = DagEngine::new();
        let schema = SchemaDescriptor::default();
        let mut tbl = make_test_table("reg_unreg");
        dag.register_table(
            100,
            StoreHandle::Borrowed(&mut *tbl as *mut Table),
            schema,
            RelationKind::BaseTable { unique_pk: false },
            0,
            String::new(),
        );
        assert!(dag.tables.contains_key(&100));

        dag.unregister_table(100);
        assert!(!dag.tables.contains_key(&100));
        let _ = std::fs::remove_dir_all(dag_test_dir("reg_unreg"));
    }

    #[test]
    fn test_invalidation() {
        let mut dag = DagEngine::new();
        dag.view_props.shard_cols.insert(42, vec![0]);
        dag.view_props.skip_exchange.insert(42, true);
        dag.view_props.needs_exchange.insert(42, true);
        dag.dep.valid = true;

        dag.invalidate(42);
        assert!(!dag.view_props.shard_cols.contains_key(&42));
        assert!(!dag.view_props.skip_exchange.contains_key(&42));
        assert!(!dag.view_props.needs_exchange.contains_key(&42));
        assert!(!dag.dep.valid);

        dag.dep.valid = true;
        dag.invalidate_dep_map();
        assert!(!dag.dep.valid);

        dag.view_props.shard_cols.insert(99, vec![1]);
        dag.invalidate_all();
        assert!(dag.view_props.shard_cols.is_empty());
    }

    /// `ViewPropCache::evict` and `clear` must touch every field in lockstep — a
    /// field either method forgets would let a dropped view/table's stale entry
    /// survive and disagree with the live circuit. `evict` drops join_shard
    /// entries that mention the id as view OR source (over-eviction is safe).
    #[test]
    fn test_view_prop_cache_eviction() {
        let populate = || {
            let mut c = ViewPropCache::default();
            c.shard_cols.insert(42, vec![0]);
            c.join_shard.insert((42, 7), vec![]); // 42 as the view
            c.join_shard.insert((7, 42), vec![]); // 42 as a join source
            c.skip_exchange.insert(42, true);
            c.needs_exchange.insert(42, true);
            c.range_join.insert(42, Some(1));
            c
        };

        // evict(42): every field keyed by 42 goes, including both join_shard
        // entries — matching 42 as the view (42, 7) and as a source (7, 42).
        let mut ce = populate();
        ce.evict(42);
        assert!(!ce.shard_cols.contains_key(&42));
        assert!(!ce.join_shard.contains_key(&(42, 7)));
        assert!(
            !ce.join_shard.contains_key(&(7, 42)),
            "evict must drop source-keyed join entry"
        );
        assert!(!ce.skip_exchange.contains_key(&42));
        assert!(!ce.needs_exchange.contains_key(&42));
        assert!(!ce.range_join.contains_key(&42));

        // clear(): empties every field (auto-covers any future field via Default).
        let mut cc = populate();
        cc.clear();
        assert!(cc.shard_cols.is_empty());
        assert!(cc.join_shard.is_empty());
        assert!(cc.skip_exchange.is_empty());
        assert!(cc.needs_exchange.is_empty());
        assert!(cc.range_join.is_empty());

        // Wiring: the production table/view-drop path routes through evict.
        let mut dag = DagEngine::new();
        dag.view_props.range_join.insert(43, Some(0));
        dag.unregister_table(43);
        assert!(
            !dag.view_props.range_join.contains_key(&43),
            "unregister_table must evict"
        );
    }

    #[test]
    fn test_add_remove_index_circuit() {
        let mut dag = DagEngine::new();
        let schema = SchemaDescriptor::default();
        let mut tbl = make_test_table("idx_parent");
        dag.register_table(
            50,
            StoreHandle::Borrowed(&mut *tbl as *mut Table),
            schema,
            RelationKind::BaseTable { unique_pk: false },
            0,
            String::new(),
        );
        let idx_tbl = make_test_table("idx_child");
        dag.add_index_circuit(50, &[2], 999, idx_tbl, schema, false);
        assert_eq!(dag.tables[&50].index_circuits.len(), 1);

        dag.remove_index_circuit(50, &[2]);
        assert_eq!(dag.tables[&50].index_circuits.len(), 0);
        dag.close();
        let _ = std::fs::remove_dir_all(dag_test_dir("idx_parent"));
        let _ = std::fs::remove_dir_all(dag_test_dir("idx_child"));
    }

    // `test_validate_graph_*` tests previously exercised
    // `DagEngine::validate_graph_structure`. The function is no longer
    // load-bearing — its only non-test caller (`engine.create_view`) was
    // removed alongside the circuit-graph schema redesign. The compiler's
    // `topo_sort` already rejects cycles, and the typed `OpNode` enum makes
    // "missing primary input" and "missing INTEGRATE sink" structurally
    // checkable at the wire-construction site instead.

    #[test]
    fn test_dep_map_empty() {
        let mut dag = DagEngine::new();
        dag.get_dep_map();
        assert!(dag.dep.forward.is_empty());
        assert!(dag.dep.valid);
    }

    #[test]
    fn test_source_ids_empty() {
        let mut dag = DagEngine::new();
        assert!(dag.get_source_ids(42).is_empty());
    }

    #[test]
    fn test_flush_includes_index_circuits() {
        let mut dag = DagEngine::new();
        let parent_schema = SchemaDescriptor::default();
        let mut tbl = make_test_table("flush_ic_parent");
        dag.register_table(
            70,
            StoreHandle::Borrowed(&mut *tbl as *mut Table),
            parent_schema,
            RelationKind::BaseTable { unique_pk: false },
            0,
            String::new(),
        );

        // Durable index table: flush writes shard_*.db only if called.
        let idx_schema = crate::schema::SchemaDescriptor::minimal_u64();
        let idx_dir = dag_test_dir("flush_ic_idx");
        let _ = std::fs::remove_dir_all(&idx_dir);
        let idx_tbl = Box::new(
            Table::new(
                &idx_dir,
                "flush_ic_idx",
                idx_schema,
                1,
                256 * 1024,
                RecoverySource::SalReplay,
            )
            .unwrap(),
        );
        dag.add_index_circuit(70, &[1], 999, idx_tbl, idx_schema, false);

        // Put one row in the index table's memtable.
        {
            let entry = dag.tables.get_mut(&70).unwrap();
            let mut batch = Batch::with_schema(idx_schema, 1);
            batch.extend_pk(1u128);
            batch.extend_weight(&1i64.to_le_bytes());
            batch.extend_null_bmp(&0u64.to_le_bytes());
            batch.count += 1;
            entry.index_circuits[0].table_mut().ingest_owned_batch(batch).unwrap();
        }

        // With the fix, flush propagates to index circuits and writes a shard.
        // Without the fix, index circuits are skipped and no shard is written.
        dag.flush(70).unwrap();
        let shard_count = std::fs::read_dir(&idx_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_str().unwrap_or("").starts_with("shard_"))
            .count();
        assert!(shard_count > 0, "index circuit shard must be written by flush");

        dag.close();
        let _ = std::fs::remove_dir_all(dag_test_dir("flush_ic_parent"));
        let _ = std::fs::remove_dir_all(idx_dir);
    }

    // R2 regression: a DELETE/UPDATE retraction on a *signed* (I64) PK must
    // actually retract the stored row. The pre-fix narrow arm fed `get_pk`
    // (OPK-widened, sign-flipped) to `retract_pk(u128)`, which re-OPK-encoded it
    // (a second sign flip) so the probe matched no stored row and the retraction
    // was silently dropped. The byte path keys on verbatim OPK and is correct.
    #[test]
    fn test_enforce_unique_pk_signed_negative_retraction() {
        use crate::schema::{type_code, SchemaColumn};
        raise_fd_limit_for_tests();

        let schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::I64, 0), // signed PK
                SchemaColumn::new(type_code::I64, 0), // payload
            ],
            &[0],
        );
        let dir = tempfile::tempdir().unwrap();
        let tdir = dir.path().join("enforce_signed");
        let mut pt = crate::storage::PartitionedTable::new(
            tdir.to_str().unwrap(),
            "enforce_signed",
            schema,
            1234,
            crate::storage::Routing::Hashed,
            RecoverySource::Rederive,
            0,
            256,
        )
        .unwrap();

        // Seed the store with a negative-PK row (PK=-5, payload=100).
        let mut seed = Batch::with_schema(schema, 1);
        seed.extend_pk_opk(&schema, &[(-5i64 as u64) as u128]);
        seed.extend_weight(&1i64.to_le_bytes());
        seed.extend_null_bmp(&0u64.to_le_bytes());
        seed.extend_col(0, &100i64.to_le_bytes());
        seed.count += 1;
        pt.ingest_owned_batch(seed).unwrap();
        let mut opk = [0u8; 8];
        gnitz_wire::encode_pk_column(&(-5i64).to_le_bytes(), type_code::I64, &mut opk);
        assert!(pt.has_pk_bytes(&opk), "seed row must be present");

        // DELETE PK=-5: a -1 retraction batch.
        let mut del = Batch::with_schema(schema, 1);
        del.extend_pk_opk(&schema, &[(-5i64 as u64) as u128]);
        del.extend_weight(&(-1i64).to_le_bytes());
        del.extend_null_bmp(&0u64.to_le_bytes());
        del.extend_col(0, &100i64.to_le_bytes());
        del.count += 1;

        let effective = DagEngine::enforce_unique_pk(&mut pt, &schema, del);

        // The store row must have been *found*: the effective batch carries a
        // single net -1 for PK=-5 with the stored payload. (`retract_pk_bytes` is
        // read-only — the store row is removed when the effective batch is
        // re-ingested, mirroring the real DML pipeline below.)
        assert_eq!(effective.count, 1, "one net retraction row expected");
        assert_eq!(effective.get_weight(0), -1, "effective weight must be -1");
        assert_eq!(effective.get_pk_bytes(0), &opk[..], "retraction PK must be OPK(-5)");

        // Re-ingest the effective batch as the DML pipeline does; the stored
        // negative-PK row then nets to zero and is gone.
        pt.ingest_owned_batch(effective).unwrap();
        assert!(
            !pt.has_pk_bytes(&opk),
            "stored negative-PK row must be gone after retraction"
        );
    }

    // unique_pk contract: per-PK accumulated weight ∈ {0, 1}. A pushed row at
    // |w| > 1 is the row repeated; retract-before-insert collapses repeats to
    // one live instance, so the effective batch must carry unit weights —
    // otherwise a weight-2 PK row lands (two live instances the -1-normalized
    // retraction arm can never fully delete) and the CREATE UNIQUE INDEX PK
    // short-circuit's premise breaks.
    #[test]
    fn test_enforce_unique_pk_weight_normalized() {
        use crate::schema::{type_code, SchemaColumn};
        raise_fd_limit_for_tests();

        let schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0), // PK
                SchemaColumn::new(type_code::I64, 0), // payload
            ],
            &[0],
        );
        let dir = tempfile::tempdir().unwrap();
        let tdir = dir.path().join("enforce_weight_norm");
        let mut pt = crate::storage::PartitionedTable::new(
            tdir.to_str().unwrap(),
            "enforce_weight_norm",
            schema,
            1234,
            crate::storage::Routing::Hashed,
            RecoverySource::Rederive,
            0,
            256,
        )
        .unwrap();

        let row_pk1 = |payload: i64, weight: i64| {
            let mut b = Batch::with_schema(schema, 1);
            b.extend_pk_opk(&schema, &[1u128]);
            b.extend_weight(&weight.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &payload.to_le_bytes());
            b.count += 1;
            b
        };
        let mut opk = [0u8; 8];
        gnitz_wire::encode_pk_column(&1u64.to_le_bytes(), type_code::U64, &mut opk);

        // Case 1 — fresh insert at weight 2: one effective row at weight 1.
        let effective = DagEngine::enforce_unique_pk(&mut pt, &schema, row_pk1(100, 2));
        assert_eq!(effective.count, 1, "one effective insert row expected");
        assert_eq!(effective.get_weight(0), 1, "insert weight must be normalized to 1");
        pt.ingest_owned_batch(effective).unwrap();
        assert!(pt.has_pk_bytes(&opk), "row must be live after the clamped insert");

        // Case 2 — upsert at weight 2 over the committed row: retraction of the
        // stored payload at -1, then the new payload at 1.
        let effective = DagEngine::enforce_unique_pk(&mut pt, &schema, row_pk1(200, 2));
        assert_eq!(effective.count, 2, "stored retraction + new insert expected");
        assert_eq!(effective.get_weight(0), -1, "stored-row retraction must be -1");
        assert_eq!(effective.get_weight(1), 1, "upsert weight must be normalized to 1");
        pt.ingest_owned_batch(effective).unwrap();
        assert!(pt.has_pk_bytes(&opk), "row must be live after the upsert");

        // Case 3 — DELETE at weight -3: one -1 retraction; re-ingest nets the
        // store to exactly zero (no ghost weight survives, no negative net).
        let effective = DagEngine::enforce_unique_pk(&mut pt, &schema, row_pk1(200, -3));
        assert_eq!(effective.count, 1, "one net retraction row expected");
        assert_eq!(
            effective.get_weight(0),
            -1,
            "retraction weight must be normalized to -1"
        );
        pt.ingest_owned_batch(effective).unwrap();
        assert!(!pt.has_pk_bytes(&opk), "row must be fully gone after the delete");
    }

    // Positivity regression: a retraction of a key that is absent (never inserted)
    // or tombstoned (inserted then removed) must NOT pass a negative-weight phantom
    // row through to the store. Pre-fix the `else if !found` arm appended the raw
    // `(-1, filler)` row, leaving a base table at net weight -1.
    #[test]
    fn test_enforce_unique_pk_absent_key_drops_phantom() {
        use crate::schema::{type_code, SchemaColumn};
        raise_fd_limit_for_tests();

        let schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::I64, 0), // signed PK
                SchemaColumn::new(type_code::I64, 0), // payload
            ],
            &[0],
        );
        let dir = tempfile::tempdir().unwrap();
        let tdir = dir.path().join("enforce_absent");
        let mut pt = crate::storage::PartitionedTable::new(
            tdir.to_str().unwrap(),
            "enforce_absent",
            schema,
            1234,
            crate::storage::Routing::Hashed,
            RecoverySource::Rederive,
            0,
            256,
        )
        .unwrap();

        // Seed an unrelated row (PK=-5) so the store is non-empty.
        let mut seed = Batch::with_schema(schema, 1);
        seed.extend_pk_opk(&schema, &[(-5i64 as u64) as u128]);
        seed.extend_weight(&1i64.to_le_bytes());
        seed.extend_null_bmp(&0u64.to_le_bytes());
        seed.extend_col(0, &100i64.to_le_bytes());
        seed.count += 1;
        pt.ingest_owned_batch(seed).unwrap();

        let retract_pk7 = || {
            let mut del = Batch::with_schema(schema, 1);
            del.extend_pk_opk(&schema, &[7u128]);
            del.extend_weight(&(-1i64).to_le_bytes());
            del.extend_null_bmp(&0u64.to_le_bytes());
            del.extend_col(0, &0i64.to_le_bytes());
            del.count += 1;
            del
        };

        // Case 1 — absent key: PK=7 was never inserted. The phantom pass-through
        // is dropped, so the effective batch is empty.
        let effective = DagEngine::enforce_unique_pk(&mut pt, &schema, retract_pk7());
        assert_eq!(effective.count, 0, "absent-key retraction must emit no phantom row");

        // Case 2 — tombstoned key: insert PK=7, retract to net zero, then retract
        // a second time. The first retraction finds the stored row (count 1); the
        // second finds nothing and emits no phantom.
        let mut ins = Batch::with_schema(schema, 1);
        ins.extend_pk_opk(&schema, &[7u128]);
        ins.extend_weight(&1i64.to_le_bytes());
        ins.extend_null_bmp(&0u64.to_le_bytes());
        ins.extend_col(0, &42i64.to_le_bytes());
        ins.count += 1;
        pt.ingest_owned_batch(ins).unwrap();

        let eff1 = DagEngine::enforce_unique_pk(&mut pt, &schema, retract_pk7());
        assert_eq!(
            eff1.count, 1,
            "retracting a present key emits the stored-row retraction"
        );
        pt.ingest_owned_batch(eff1).unwrap(); // PK=7 now nets to zero (tombstoned)

        let eff2 = DagEngine::enforce_unique_pk(&mut pt, &schema, retract_pk7());
        assert_eq!(eff2.count, 0, "tombstoned-key retraction must emit no phantom row");
    }

    // Wide-PK enforcement: a >16-byte compound PK must key the intra-batch
    // `seen` map, the store probe, and the emitted retraction on its full OPK
    // bytes. The other enforce_unique_pk unit tests use narrow single-column
    // PKs, so this is the only coverage of `&[u8]` slice keying at pk_stride > 16.
    #[test]
    fn test_enforce_unique_pk_wide_pk() {
        use crate::test_support::{opk_pk, wide_pk_3xu64_schema, wide_row};
        raise_fd_limit_for_tests();

        let schema = wide_pk_3xu64_schema();
        let dir = tempfile::tempdir().unwrap();
        let tdir = dir.path().join("enforce_wide");
        let mut pt = crate::storage::PartitionedTable::new(
            tdir.to_str().unwrap(),
            "enforce_wide",
            schema,
            555,
            crate::storage::Routing::Hashed,
            RecoverySource::Rederive,
            0,
            256,
        )
        .unwrap();

        let pk24 = |a: u64, b: u64, c: u64| opk_pk(&schema, &[a as u128, b as u128, c as u128]);

        // Seed the store with K=(1,2,3) → 100.
        let eff = DagEngine::enforce_unique_pk(&mut pt, &schema, wide_row(&schema, &pk24(1, 2, 3), 1, 100));
        assert_eq!(eff.count, 1, "fresh wide-PK insert is a single +1 row");
        pt.ingest_owned_batch(eff).unwrap();
        assert!(pt.has_pk_bytes(&pk24(1, 2, 3)), "seed row must be live");

        // Cross-batch upsert: a +1 on the same wide PK retracts the stored
        // payload (keyed and emitted on the full 24 bytes) and inserts the new.
        let eff = DagEngine::enforce_unique_pk(&mut pt, &schema, wide_row(&schema, &pk24(1, 2, 3), 1, 200));
        assert_eq!(eff.count, 2, "stored-row retraction + new insert");
        assert_eq!(eff.get_weight(0), -1, "stored retraction at -1");
        assert_eq!(
            eff.get_pk_bytes(0),
            &pk24(1, 2, 3)[..],
            "retraction keys on the full 24-byte PK"
        );
        assert_eq!(eff.get_weight(1), 1, "new insert at +1");
        pt.ingest_owned_batch(eff).unwrap();
        assert!(pt.has_pk_bytes(&pk24(1, 2, 3)), "row stays live after the upsert");

        // Intra-batch +1, -1, +1 on a fresh wide PK K2=(7,8,9): the delete must
        // clear the `seen` entry so the re-insert is not re-negated. Net +1.
        let mut b = Batch::with_schema(schema, 3);
        for (payload, w) in [(10i64, 1i64), (10, -1), (20, 1)] {
            b.extend_pk_bytes(&pk24(7, 8, 9));
            b.extend_weight(&w.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &payload.to_le_bytes());
            b.count += 1;
        }
        let eff = DagEngine::enforce_unique_pk(&mut pt, &schema, b);
        pt.ingest_owned_batch(eff).unwrap();
        assert!(pt.has_pk_bytes(&pk24(7, 8, 9)), "K2 survives +1,-1,+1 at net +1");
    }

    // vm_epoch_result must gnitz_fatal_abort! (→ _exit(134)) on Err.
    #[test]
    fn test_vm_epoch_result_abort_exit_status() {
        crate::test_support::assert_test_aborts_134(
            "test_vm_epoch_result_abort_internal",
            &[("GNITZ_RUN_ABORT_TEST", "1")],
        );
    }

    // Guard: runs only when GNITZ_RUN_ABORT_TEST=1 (set by the parent test above).
    // Constructs an Err(-10) result and passes it to vm_epoch_result, which must
    // call gnitz_fatal_abort!. The parent asserts exit code 134.
    #[test]
    fn test_vm_epoch_result_abort_internal() {
        if std::env::var("GNITZ_RUN_ABORT_TEST").is_err() {
            return;
        }
        let r: Result<Option<Batch>, i32> = Err(-10);
        DagEngine::vm_epoch_result(42, r);
        unreachable!("vm_epoch_result must not return on Err");
    }

    /// `StoreHandle::Partitioned` must dispatch `recovery_lsn` → the table's
    /// `min_flushed_lsn` (recovery watermark) and `current_lsn` → its `current_lsn`
    /// (the LSN-allocator max). Built on the partial-flush fixture where the two
    /// diverge, so a swapped dispatch is caught. (The underlying min/max
    /// aggregation is pinned storage-side by
    /// `min_flushed_lsn_floors_recovery_watermark_after_partial_flush`.)
    #[test]
    fn store_handle_partitioned_lsn_dispatch() {
        let f = crate::storage::partial_flush_lsn_fixture();
        let (recovery, current) = (f.recovery_lsn, f.current_lsn);
        assert!(recovery < current, "fixture must have min < max to distinguish the two");

        let handle = StoreHandle::Partitioned(std::cell::UnsafeCell::new(Box::new(f.pt)));
        assert_eq!(
            handle.recovery_lsn(),
            recovery,
            "Partitioned recovery_lsn → min_flushed_lsn"
        );
        assert_eq!(
            handle.current_lsn(),
            current,
            "Partitioned current_lsn → max current_lsn"
        );
    }

    // A storage error while applying committed data in `ingest_store_and_indices`
    // must _exit(134) (fail-stop; recovery is restart + SAL replay). Driven via
    // the `GNITZ_INJECT_INGEST_APPLY_ERROR` debug seam. The `index`-stage
    // variant is exercised end-to-end by the
    // `test_ingest_apply_error_aborts_and_replays` e2e test.
    #[test]
    fn test_ingest_apply_error_abort_exit_status() {
        crate::test_support::assert_test_aborts_134(
            "ingest_apply_error_abort_internal",
            &[
                ("GNITZ_RUN_INGEST_ABORT_TEST", "1"),
                ("GNITZ_INJECT_INGEST_APPLY_ERROR", "store"),
            ],
        );
    }

    // Guard: runs the seam-armed ingest only under GNITZ_RUN_INGEST_ABORT_TEST
    // (set by the parent). Registers a base table and ingests one row; the armed
    // "store" seam substitutes Err for the base ingest, tripping the abort.
    #[test]
    fn ingest_apply_error_abort_internal() {
        if std::env::var("GNITZ_RUN_INGEST_ABORT_TEST").is_err() {
            return;
        }
        let mut dag = DagEngine::new();
        let schema = crate::schema::SchemaDescriptor::minimal_u64();
        let dir = dag_test_dir("seam_abort");
        let _ = std::fs::remove_dir_all(&dir);
        let mut tbl =
            Box::new(Table::new(&dir, "seam_abort", schema, 99, 256 * 1024, RecoverySource::Rederive).unwrap());
        dag.register_table(
            70,
            StoreHandle::Borrowed(&mut *tbl as *mut Table),
            schema,
            RelationKind::BaseTable { unique_pk: false },
            0,
            String::new(),
        );
        let mut batch = Batch::with_schema(schema, 1);
        batch.extend_pk(1u128);
        batch.extend_weight(&1i64.to_le_bytes());
        batch.extend_null_bmp(&0u64.to_le_bytes());
        batch.count += 1;
        dag.ingest_to_family(70, batch);
        unreachable!("ingest_store_and_indices must abort when the seam is armed");
    }
}
