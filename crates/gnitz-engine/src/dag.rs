//! DagEngine: consolidated plan cache, DAG evaluator, and ingestion pipeline.

use std::cell::UnsafeCell;
use rustc_hash::{FxHashMap, FxHashSet};

use crate::schema::SchemaDescriptor;
use gnitz_wire::PkColList;
use crate::compiler::{self, CompileOutput, ExternalTable, SubPlan};
use crate::storage::{Batch, CursorHandle, FlushOutcome, FlushWork, Persistence, Table, PartitionedTable, StorageError};
use crate::ops;
use crate::vm;

// ---------------------------------------------------------------------------
// Store handle — views vs base tables
// ---------------------------------------------------------------------------

/// Storage handle of a registered relation. The owned variant drops the
/// underlying storage on Drop; the Borrowed variant holds a non-owning
/// pointer for system tables owned by CatalogEngine directly.
pub enum StoreHandle {
    /// Owned PartitionedTable — used by base tables and views. Wrapped in
    /// `UnsafeCell` so the interior-mutable accessors can hand out `&mut`
    /// through a shared `&self` without violating Stacked Borrows (a raw
    /// pointer derived from a shared reference may not be used to mutate).
    Partitioned(UnsafeCell<Box<PartitionedTable>>),
    /// Non-owning pointer to a Table owned elsewhere (system tables).
    Borrowed(*mut Table),
}

// SAFETY: Borrowed wraps a raw pointer that is only accessed on the
// thread that owns the DagEngine. The DagEngine itself is never shared
// across threads.
unsafe impl Send for StoreHandle {}

impl StoreHandle {
    /// Get a raw pointer to the Table (Borrowed), or null for Partitioned.
    /// SAFETY: the owner keeps the Table alive across any synchronous call
    /// using the pointer. The caller must ensure no aliasing &mut references
    /// exist.
    pub fn table_ptr(&self) -> *mut Table {
        match self {
            StoreHandle::Borrowed(ptr) => *ptr,
            StoreHandle::Partitioned(_) => std::ptr::null_mut(),
        }
    }

    /// Get a raw pointer to the PartitionedTable, or null if not Partitioned.
    pub fn ptable_ptr(&self) -> *mut PartitionedTable {
        match self {
            StoreHandle::Partitioned(cell) => unsafe { &mut **cell.get() as *mut PartitionedTable },
            _ => std::ptr::null_mut(),
        }
    }

    // ------------------------------------------------------------------
    // Interior-mutable accessors
    //
    // DagEngine/CatalogEngine are single-threaded (!Sync) and the
    // HashMap<id, TableEntry> stores owning Boxes whose heap allocations
    // have stable addresses, so the mutation is race-free. But the
    // registry HashMap is read via immutable get(), which would normally
    // prevent handing out &mut to the owned Table / PartitionedTable.
    // These accessors encapsulate the raw-pointer re-borrow that
    // reconciles the lookup API with the mutation need, so call sites
    // stop reimplementing it inline.
    //
    // SAFETY contract for every method below: no aliasing &mut into the
    // same storage may be live across the call.
    // ------------------------------------------------------------------

    /// Get `&mut PartitionedTable` if this handle is Partitioned.
    // Interior mutability through UnsafeCell: the `&mut` is handed out under the
    // SAFETY contract documented above (no live aliasing &mut), not derived from
    // `&self` by reborrow — so clippy's mut_from_ref does not apply.
    #[allow(clippy::mut_from_ref)]
    pub fn as_partitioned_mut(&self) -> Option<&mut PartitionedTable> {
        match self {
            StoreHandle::Partitioned(cell) => Some(unsafe { &mut **cell.get() }),
            _ => None,
        }
    }

    /// Dispatched `has_pk` that works for every variant. Takes a **native**
    /// `u128`; routes via `opk_key` internally. Never feed it `get_pk`
    /// (OPK-widened) — use [`has_pk_bytes`] for verbatim OPK bytes.
    #[cfg(test)] // sole caller is the test-only inline FK check (validate_fk_inline)
    pub fn has_pk(&self, key: u128) -> bool {
        let tptr = self.table_ptr();
        unsafe {
            if !tptr.is_null() { (*tptr).has_pk(key) } else { (*self.ptable_ptr()).has_pk(key) }
        }
    }

    /// Dispatched verbatim-OPK-bytes `has_pk` across all variants. Correct for
    /// every PK width; takes the bytes `Batch::get_pk_bytes` produces, with no
    /// native round-trip (and thus no double-encode for signed/compound PKs).
    pub fn has_pk_bytes(&self, key: &[u8]) -> bool {
        let tptr = self.table_ptr();
        unsafe {
            if !tptr.is_null() {
                (*tptr).has_pk_bytes(key)
            } else {
                (*self.ptable_ptr()).has_pk_bytes(key)
            }
        }
    }

    /// Dispatched non-compacting `open_cursor` across all variants.
    /// Infallible, non-mutating — the recommended default. See
    /// `Table::open_cursor`.
    pub fn open_cursor(&self) -> CursorHandle {
        let tptr = self.table_ptr();
        unsafe {
            if !tptr.is_null() {
                (*tptr).open_cursor()
            } else {
                (*self.ptable_ptr()).open_cursor()
            }
        }
    }

    /// Dispatched compacting cursor across all variants. Maintenance-only —
    /// see `Table::create_cursor_compacting` for the validator hazard this
    /// name surfaces. The lint guards external callers, not this dispatch.
    #[allow(clippy::disallowed_methods)]
    pub fn create_cursor_compacting(&self) -> Result<CursorHandle, StorageError> {
        let tptr = self.table_ptr();
        unsafe {
            if !tptr.is_null() {
                (*tptr).create_cursor_compacting()
            } else {
                (*self.ptable_ptr()).create_cursor_compacting()
            }
        }
    }

    /// Dispatched durable ingest of an already-owned `Batch`. Skips the
    /// regions memcpy round-trip; moves the batch directly into the
    /// storage layer (zero-copy for single-partition stores; one
    /// MemBatch-borrowed scatter for multi-partition).
    pub fn ingest_owned_batch(&self, batch: Batch) -> Result<(), StorageError> {
        let tptr = self.table_ptr();
        unsafe {
            if !tptr.is_null() {
                (*tptr).ingest_owned_batch(batch)
            } else {
                (*self.ptable_ptr()).ingest_owned_batch(batch)
            }
        }
    }

    /// Dispatched flush across all variants.
    pub fn flush(&mut self) -> Result<bool, StorageError> {
        match self {
            StoreHandle::Borrowed(ptr) => unsafe { &mut **ptr }.flush(),
            StoreHandle::Partitioned(cell) => cell.get_mut().flush(),
        }
    }

    /// Dispatched Phase 1 across all variants. Returns one
    /// (partition_idx, FlushWork) per partition that produced deferred
    /// work; for Borrowed `partition_idx` is always 0.
    pub fn flush_prepare(&mut self) -> Result<Vec<(usize, FlushWork)>, StorageError> {
        let table: &mut Table = match self {
            StoreHandle::Borrowed(ptr) => unsafe { &mut **ptr },
            StoreHandle::Partitioned(cell) => return cell.get_mut().flush_prepare(),
        };
        match table.flush_prepare()? {
            FlushOutcome::Empty | FlushOutcome::DoneInline => Ok(Vec::new()),
            FlushOutcome::Pending(w) => Ok(vec![(0, *w)]),
        }
    }

    /// Dispatched Phase 3 across all variants. Returns one dirfd per
    /// committed partition.
    pub fn flush_commit_batch(
        &mut self,
        works: Vec<(usize, FlushWork)>,
    ) -> Result<Vec<libc::c_int>, StorageError> {
        let t: &mut Table = match self {
            StoreHandle::Borrowed(ptr) => unsafe { &mut **ptr },
            StoreHandle::Partitioned(cell) => return cell.get_mut().flush_commit_batch(works),
        };
        let mut out = Vec::with_capacity(works.len());
        for (_, w) in works {
            if let Some(fd) = t.flush_commit(w)? { out.push(fd); }
        }
        Ok(out)
    }

    /// Current LSN of the store (Table: current_lsn field; Partitioned: max across shards).
    pub fn current_lsn(&self) -> u64 {
        match self {
            StoreHandle::Borrowed(ptr) => unsafe { &**ptr }.current_lsn,
            StoreHandle::Partitioned(cell) => unsafe { (**cell.get()).current_lsn() },
        }
    }

    /// Recovery watermark of the store: the LSN below which committed SAL
    /// zones may be skipped on replay. Borrowed → `current_lsn`; Partitioned →
    /// the **min** across partitions (see `PartitionedTable::min_flushed_lsn`),
    /// so a partial family flush never causes the dedupe filter to over-skip a
    /// lagging partition's unflushed rows.
    pub fn recovery_lsn(&self) -> u64 {
        match self {
            StoreHandle::Borrowed(ptr) => unsafe { &**ptr }.current_lsn,
            StoreHandle::Partitioned(cell) => unsafe { (**cell.get()).min_flushed_lsn() },
        }
    }
}

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
        if !self.is_unique { return None; }
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
    /// Storage durability. Durable kinds load shards from the manifest;
    /// ephemeral kinds erase on open and live in memory.
    #[inline]
    pub fn persistence(self) -> Persistence {
        match self {
            RelationKind::SystemCatalog | RelationKind::BaseTable { .. } => Persistence::Durable,
            RelationKind::View => Persistence::Ephemeral,
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
pub type CachedPlan = CompileOutput;

// ---------------------------------------------------------------------------
// System table references
// ---------------------------------------------------------------------------

/// Handles + schemas for the four system tables that the compiler reads:
/// CircuitNodes, CircuitEdges, CircuitNodeColumns, and DepTab.
pub struct SysTableRefs {
    // Table handles (DagEngine borrows them).
    pub nodes: *mut Table,
    pub edges: *mut Table,
    pub node_columns: *mut Table,
    pub dep_tab: *mut Table,
    // Schemas. (dep_tab needs none: get_dep_map reads its compound PK directly
    // from the cursor's PK bytes.)
    pub nodes_schema: SchemaDescriptor,
    pub edges_schema: SchemaDescriptor,
    pub node_columns_schema: SchemaDescriptor,
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
            nodes_schema: SchemaDescriptor::default(),
            edges_schema: SchemaDescriptor::default(),
            node_columns_schema: SchemaDescriptor::default(),
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
    fn do_exchange(
        &mut self,
        view_id: i64,
        batch: &Batch,
        source_id: i64,
    ) -> Batch;
}

// ---------------------------------------------------------------------------
// DagEngine
// ---------------------------------------------------------------------------

pub struct DagEngine {
    cache: FxHashMap<i64, CachedPlan>,
    dep_map: FxHashMap<i64, Vec<i64>>,       // source_table_id → [view_ids]
    source_map: FxHashMap<i64, Vec<i64>>,    // view_id → [source_table_ids]
    dep_map_valid: bool,
    shard_cols_cache: FxHashMap<i64, Vec<i32>>,
    // Each entry is (reindex column, carried promotion target tc) — the carried
    // tc is non-zero for a cross-width join key and threads to the scatter packer.
    join_shard_cols_cache: FxHashMap<(i64, i64), Vec<(i32, u8)>>,
    // Whether a view's output ExchangeShard is a no-op (every row already on its
    // PK-owner) and the IPC shuffle can be skipped. Memoized per view_id; evicted
    // in lockstep with the sibling view caches.
    skip_exchange_cache: FxHashMap<i64, bool>,
    // Whether a view has an ExchangeShard node at all. Memoized per view_id;
    // evicted in lockstep with the sibling view caches.
    needs_exchange_cache: FxHashMap<i64, bool>,
    // Whether a view is a non-equi (range) join — its input relay broadcasts and
    // its driver branch double-exchanges. Memoized per view_id; evicted in
    // lockstep with the sibling view-keyed caches.
    range_join_cache: FxHashMap<i64, Option<u8>>,
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
            dep_map: FxHashMap::default(),
            source_map: FxHashMap::default(),
            dep_map_valid: false,
            shard_cols_cache: FxHashMap::default(),
            join_shard_cols_cache: FxHashMap::default(),
            skip_exchange_cache: FxHashMap::default(),
            needs_exchange_cache: FxHashMap::default(),
            range_join_cache: FxHashMap::default(),
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
        self.tables.insert(table_id, TableEntry {
            handle,
            schema,
            kind,
            depth,
            directory,
            index_circuits: Vec::new(),
        });
    }

    pub fn unregister_table(&mut self, table_id: i64) {
        self.tables.remove(&table_id);
        self.cache.remove(&table_id);
        self.shard_cols_cache.remove(&table_id);
        self.join_shard_cols_cache.retain(|&(v, s), _| v != table_id && s != table_id);
        self.skip_exchange_cache.remove(&table_id);
        self.needs_exchange_cache.remove(&table_id);
        self.range_join_cache.remove(&table_id);
        self.dep_map_valid = false;
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
            entry.index_circuits.retain(|ic| ic.col_indices.as_slice() != col_indices);
        }
    }

    /// Set the uniqueness flag of the index circuit on `col_indices` in place
    /// (the circuit list is deduped by column list, so at most one entry
    /// matches). Promotion (`true`) folds a UNIQUE index into an existing
    /// non-unique circuit when both target the same column list; demotion
    /// (`false`) is used by the DROP INDEX retraction path when the UNIQUE index
    /// is dropped but another index (e.g. an FK auto-index) still covers it.
    pub fn set_index_circuit_uniqueness(&mut self, table_id: i64, col_indices: &[u32], is_unique: bool) {
        if let Some(entry) = self.tables.get_mut(&table_id) {
            if let Some(ic) = entry.index_circuits.iter_mut().find(|ic| ic.col_indices.as_slice() == col_indices) {
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
            plan.pre.vm.regfile.clear_delta_batches();
            if let Some(post) = plan.post.as_mut() {
                post.vm.regfile.clear_delta_batches();
            }
            // Binary set-op views (UNION ALL / EXCEPT / INTERSECT) compile the
            // right-hand side into `side_b`, which owns its own VM regfile; its
            // delta batches stay pinned (pooled batches leaked) until the view is
            // invalidated unless cleared here too.
            if let Some(sb) = plan.side_b.as_mut() {
                sb.plan.vm.regfile.clear_delta_batches();
            }
        }
    }

    /// Distributed-backfill analogue of `backfill_view`'s post-loop
    /// `clear_view_regfile_deltas` (catalog/ddl.rs). A worker's
    /// `handle_backfill(source_id)` drives `evaluate_dag_multi_worker`, which
    /// evaluates the whole transitive dependent closure of `source_id`; the last
    /// chunk's input + intermediate delta registers stay pinned in every touched
    /// view's regfile after the loop. Walk that closure and release them, so peak
    /// resident memory falls back to ~O(chunk) once the backfill drains.
    ///
    /// Also carries `backfill_view`'s Ephemeral guard: every view backfilled this
    /// way must be ephemeral, else its manifest-loaded shards would double-count
    /// against the deltas the backfill ingests.
    pub fn clear_regfile_deltas_from_source(&mut self, source_id: i64) {
        self.get_dep_map();
        let mut stack: Vec<i64> =
            self.dep_map.get(&source_id).cloned().unwrap_or_default();
        let mut seen: FxHashSet<i64> = FxHashSet::default();
        while let Some(view_id) = stack.pop() {
            if !seen.insert(view_id) {
                continue;
            }
            debug_assert!(
                self.tables.get(&view_id)
                    .is_none_or(|e| e.kind.persistence() == Persistence::Ephemeral),
                "distributed backfill into durable relation {view_id}: \
                 would double-count loaded shards",
            );
            self.clear_view_regfile_deltas(view_id);
            if let Some(deps) = self.dep_map.get(&view_id) {
                stack.extend(deps.iter().copied());
            }
        }
    }

    // ── Cache management ────────────────────────────────────────────────

    #[cfg(test)] // sole callers are the test-only drop_view path and the dag tests
    pub fn invalidate(&mut self, view_id: i64) {
        self.cache.remove(&view_id);
        self.shard_cols_cache.remove(&view_id);
        self.join_shard_cols_cache.retain(|&(v, _), _| v != view_id);
        self.skip_exchange_cache.remove(&view_id);
        self.needs_exchange_cache.remove(&view_id);
        self.range_join_cache.remove(&view_id);
        self.dep_map_valid = false;
    }

    pub fn invalidate_all(&mut self) {
        self.cache.clear();
        self.shard_cols_cache.clear();
        self.join_shard_cols_cache.clear();
        self.skip_exchange_cache.clear();
        self.needs_exchange_cache.clear();
        self.range_join_cache.clear();
        self.dep_map_valid = false;
    }

    pub fn invalidate_dep_map(&mut self) {
        self.dep_map_valid = false;
    }

    // ── Dependency map ──────────────────────────────────────────────────

    /// Rebuild the dep_map and source_map from the DepTab system table.
    /// Port of `ProgramCache.get_dep_map()`.
    pub fn get_dep_map(&mut self) -> &FxHashMap<i64, Vec<i64>> {
        if self.dep_map_valid {
            return &self.dep_map;
        }
        self.dep_map.clear();
        self.source_map.clear();

        if !self.sys.dep_tab.is_null() {
            let t = unsafe { &*self.sys.dep_tab };
            {
                let mut ch = t.open_cursor();
                while ch.cursor.valid {
                    let w = ch.cursor.current_weight;
                    if w > 0 {
                        // DepTab compound PK = (view_id, dep_table_id); both live
                        // in the 16-byte PK region as OPK (big-endian for these
                        // unsigned columns): view_id_BE in bytes 0..8, dep_BE in 8..16.
                        let pk = ch.cursor.current_pk_bytes();
                        let v_id = u64::from_be_bytes(pk[0..8].try_into().unwrap()) as i64;
                        let dep_tid = u64::from_be_bytes(pk[8..16].try_into().unwrap()) as i64;
                        if dep_tid > 0 {
                            let views = self.dep_map.entry(dep_tid).or_default();
                            if !views.contains(&v_id) {
                                views.push(v_id);
                            }
                            let sources = self.source_map.entry(v_id).or_default();
                            if !sources.contains(&dep_tid) {
                                sources.push(dep_tid);
                            }
                        }
                    }
                    ch.cursor.advance();
                }
            }
        }
        self.dep_map_valid = true;
        &self.dep_map
    }

    /// Return all direct source table IDs for a view.
    pub fn get_source_ids(&mut self, view_id: i64) -> Vec<i64> {
        self.get_dep_map();
        self.source_map.get(&view_id).cloned().unwrap_or_default()
    }

    // ── Metadata queries (lightweight, no compilation) ──────────────────

    /// Load typed circuit nodes/edges for metadata queries. Cheaper than full
    /// compilation: no topo sort, no optimization passes, no code emission.
    fn load_meta_circuit(&self, view_id: i64) -> compiler::LoadedCircuit {
        let mut loaded = compiler::load_circuit(
            self.sys.nodes, &self.sys.nodes_schema,
            self.sys.edges, &self.sys.edges_schema,
            self.sys.node_columns, &self.sys.node_columns_schema,
            view_id as u64,
            SchemaDescriptor::default(),
        ).unwrap_or_else(compiler::LoadedCircuit::empty);
        // Populate `outgoing`/`incoming` adjacency so annotation helpers like
        // `reindex_cols_through_filters` can traverse the graph. (`load_circuit`
        // returns a circuit with empty adjacency maps; only `compile_view` runs
        // topo_sort itself.) Ignore cycle errors — adjacency is filled regardless.
        let _ = compiler::topo_sort(&mut loaded);
        loaded
    }

    /// Extract shard columns for a view without full compilation.
    pub fn get_shard_cols(&mut self, view_id: i64) -> Vec<i32> {
        if let Some(cols) = self.shard_cols_cache.get(&view_id) {
            return cols.clone();
        }
        let loaded = self.load_meta_circuit(view_id);
        let shard_cols = loaded.nodes.values()
            .find_map(|op| {
                if let gnitz_wire::OpNode::ExchangeShard { shard_cols: sc } = op {
                    Some(sc.iter().map(|&c| c as i32).collect::<Vec<_>>())
                } else {
                    None
                }
            })
            .unwrap_or_default();
        self.shard_cols_cache.insert(view_id, shard_cols.clone());
        shard_cols
    }

    /// Get join shard columns for a specific source within a view, each paired
    /// with its carried promotion target tc (`0` = no promotion / derive from
    /// source). The pairs mirror the trace-side reindex Map slot-for-slot.
    pub fn get_join_shard_cols(&mut self, view_id: i64, source_id: i64) -> Vec<(i32, u8)> {
        // Called once per join source per tick on the master's serialized
        // exchange-relay path; cache the result so the hot path skips the
        // full load_circuit + topo_sort (O(V+E) with several allocations).
        if let Some(cols) = self.join_shard_cols_cache.get(&(view_id, source_id)) {
            return cols.clone();
        }
        let cols = self.compute_join_shard_cols(view_id, source_id);
        self.join_shard_cols_cache.insert((view_id, source_id), cols.clone());
        cols
    }

    /// Whether `get_join_shard_cols` would be empty, without cloning the cached
    /// Vec — the hot-path arm in `evaluate_dag_multi_worker` tests only emptiness.
    fn join_shard_cols_is_empty(&mut self, view_id: i64, source_id: i64) -> bool {
        if let Some(cols) = self.join_shard_cols_cache.get(&(view_id, source_id)) {
            return cols.is_empty();
        }
        let cols = self.compute_join_shard_cols(view_id, source_id);
        let empty = cols.is_empty();
        self.join_shard_cols_cache.insert((view_id, source_id), cols);
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
            if *tid as i64 == source_id { Some(nid) } else { None }
        });
        let scan_nid = match scan_nid { Some(n) => n, None => return Vec::new() };

        // Find the downstream Map(Expression { reindex_cols }) node, walking
        // through any intervening Filter nodes (planner emits Filter → Map
        // reindex chains for PK-redistribution views).
        crate::compiler::reindex_cols_through_filters(&loaded, scan_nid)
    }

    /// True iff the view's output `ExchangeShard` is a no-op (every row already on
    /// the worker owning its distribution key) and can be skipped: the shard reads
    /// a scan — through any Filter chain — whose distribution prefix is the shard
    /// key. Collapses the former `ExchangeInfo { is_trivial,
    /// is_co_partitioned }` — whose sole reader ANDed both fields — into the one
    /// bit they encoded, making the impossible `{ is_trivial: false,
    /// is_co_partitioned: true }` state unrepresentable.
    fn view_skips_exchange(&mut self, view_id: i64) -> bool {
        if let Some(&skip) = self.skip_exchange_cache.get(&view_id) {
            return skip;
        }
        let skip = self.compute_view_skips_exchange(view_id);
        self.skip_exchange_cache.insert(view_id, skip);
        skip
    }

    fn compute_view_skips_exchange(&self, view_id: i64) -> bool {
        let loaded = self.load_meta_circuit(view_id);

        // The view's output ExchangeShard and its shard columns.
        let Some((enid, shard_cols)) = loaded.nodes.iter().find_map(|(&nid, op)| match op {
            gnitz_wire::OpNode::ExchangeShard { shard_cols } =>
                Some((nid, shard_cols.iter().map(|&c| c as i32).collect::<Vec<_>>())),
            _ => None,
        }) else { return false };

        // Resolve the shard's source table, walking back through any Filter chain
        // (`scan_tid_through_filters`). Skip iff that table's distribution prefix
        // (`pk_indices[..k]`) is exactly the shard key — an exact-prefix match. For
        // a default full-PK table this is the strict full-PK case as before; for a
        // `CLUSTER BY prefix` table it also lets a (possibly filtered)
        // `GROUP BY prefix` / reduce run locally — every row for a group value
        // already lives on one worker — since this governs every single-source
        // `ExchangeShard` view, not just joins.
        let Some(tid) = crate::compiler::scan_tid_through_filters(&loaded, enid) else {
            return false;
        };
        self.tables.get(&tid)
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
    /// Sources come from the cached `source_map` (`get_source_ids`), which records
    /// exactly `circuit.dependencies()` — the deduped `ScanDelta` source set — so
    /// this answers the question off the dependency map with no circuit load.
    pub(crate) fn view_all_sources_replicated(&mut self, view_id: i64) -> bool {
        let sources = self.get_source_ids(view_id);
        !sources.is_empty()
            && sources.iter().all(|tid|
                self.tables.get(tid).is_some_and(|e| e.schema.replicated()))
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
        self.cache.get(&view_id)
            .and_then(|p| p.exchange_in_schema)
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
    /// views — the startup `backfill_exchange_views` path relies on that.
    pub fn view_needs_exchange(&mut self, view_id: i64) -> bool {
        if let Some(&needs) = self.needs_exchange_cache.get(&view_id) {
            return needs;
        }
        let loaded = self.load_meta_circuit(view_id);
        let needs = loaded.nodes.values()
            .any(|op| matches!(op, gnitz_wire::OpNode::ExchangeShard { .. }));
        self.needs_exchange_cache.insert(view_id, needs);
        needs
    }

    /// The equality-conjunct count of a non-equi (range / band) join view, or
    /// `None` if the view is not one — read off its `Join(DeltaTraceRange)` node.
    /// `Some` is the precise discriminator for the range-join driver branch; the
    /// `n_eq` value drives the input relay's eq-prefix scatter (`n_eq ≥ 1`, band
    /// join) vs. broadcast (`n_eq == 0`, pure range join). It is NOT
    /// `has_join_shard && has_exchange`: that predicate is also true for every
    /// GROUP BY / reduce / single-sided set-op view (a group reindex matches
    /// `reindex_cols_through_filters`), which must keep the plain `has_exchange`
    /// arm. Memoized in `range_join_cache`, evicted with the sibling view caches.
    pub fn view_range_join_n_eq(&mut self, view_id: i64) -> Option<u8> {
        if let Some(&v) = self.range_join_cache.get(&view_id) {
            return v;
        }
        let loaded = self.load_meta_circuit(view_id);
        let n_eq = crate::compiler::circuit_range_join_n_eq(&loaded);
        self.range_join_cache.insert(view_id, n_eq);
        n_eq
    }

    // ── Compilation ─────────────────────────────────────────────────────

    /// Compile a view by reading system tables and calling `compiler::compile_view`.
    fn compile_view_internal(&self, view_id: i64) -> Option<CachedPlan> {
        let entry = self.tables.get(&view_id)?;
        let view_schema = &entry.schema;
        let view_dir = entry.directory.clone();

        // Build external tables array from registered tables
        let ext_tables: Vec<ExternalTable> = self.tables.iter()
            .map(|(&tid, te)| ExternalTable {
                table_id: tid,
                schema: te.schema,
            })
            .collect();

        let result = unsafe {
            compiler::compile_view(
                view_id as u64,
                self.sys.nodes, self.sys.edges, self.sys.node_columns,
                &self.sys.nodes_schema, &self.sys.edges_schema,
                &self.sys.node_columns_schema,
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
            Err(code) => {
                gnitz_warn!("dag: compile_view returned error {} for view_id={}", code, view_id);
                None
            }
        }
    }

    // ── Execution ───────────────────────────────────────────────────────

    /// Execute a single epoch of a compiled plan.
    ///
    /// Refreshes ext cursors, loads input, runs the VM, returns the output batch.
    /// Port of `ExecutablePlan.execute_epoch()`.
    pub fn execute_epoch(
        &mut self,
        view_id: i64,
        input: Batch,
        source_id: i64,
    ) -> Option<Batch> {
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
        self.cache.get(&view_id)
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
        self.ingest_returning_effective(table_id, batch).0
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
        Self::ingest_store_and_indices(entry, &schema, batch);

        0
    }

    /// Ingest a batch and return the effective batch (after unique_pk enforcement).
    /// The effective batch is what downstream views need to see.
    /// Returns (rc, Option<Batch>).
    pub fn ingest_returning_effective(&mut self, table_id: i64, batch: Batch) -> (i32, Option<Batch>) {
        let entry = match self.tables.get_mut(&table_id) {
            Some(e) => e,
            None => {
                gnitz_warn!("dag: ingest_returning_effective — table_id={} not registered", table_id);
                return (-1, None);
            }
        };

        let schema = entry.schema;
        let unique_pk = entry.unique_pk();
        let ptbl_ptr = entry.handle.ptable_ptr();

        // unique_pk ⟹ Partitioned: every SQL-created base table is registered
        // Partitioned; system tables (the only Borrowed handles) are never
        // unique_pk, so a unique_pk relation always has a non-null ptable. The
        // debug_assert turns any future stray Borrowed+unique_pk registration into
        // a loud failure in debug; a release build passes the batch through
        // unenforced rather than dereferencing a null pointer.
        debug_assert!(
            !(unique_pk && ptbl_ptr.is_null()),
            "unique_pk relation {table_id} must be a PartitionedTable",
        );
        let effective_batch = if unique_pk && !ptbl_ptr.is_null() {
            let ptable = unsafe { &mut *ptbl_ptr };
            Self::enforce_unique_pk(ptable, &schema, batch)
        } else {
            batch
        };

        if effective_batch.count == 0 {
            return (0, Some(effective_batch));
        }

        let entry = self.tables.get_mut(&table_id).unwrap();

        Self::ingest_store_and_indices(entry, &schema, &effective_batch);

        (0, Some(effective_batch))
    }

    /// Project all index batches from `source`, ingest a clone into the store,
    /// then drain the projected index batches into their respective index tables.
    /// Shared by `ingest_by_ref` and `ingest_returning_effective`.
    fn ingest_store_and_indices(
        entry: &mut TableEntry,
        schema: &SchemaDescriptor,
        source: &Batch,
    ) {
        let index_batches: Vec<Batch> = entry.index_circuits.iter()
            .map(|ic| Self::batch_project_index(source, ic.col_indices.as_slice(), schema, &ic.index_schema))
            .collect();
        let _ = entry.handle.ingest_owned_batch(source.clone_batch());
        for (ic, idx_batch) in entry.index_circuits.iter_mut().zip(index_batches) {
            if idx_batch.count > 0 {
                let _ = ic.table_mut().ingest_owned_batch(idx_batch);
            }
        }
    }

    /// Flush a table's WAL.  Returns 0 on success, -1 on missing table or
    /// any underlying storage error (the i32 is what the caller — vm/worker —
    /// already pattern-matches against, so we keep the legacy signature here).
    pub fn flush(&mut self, table_id: i64) -> i32 {
        let entry = match self.tables.get_mut(&table_id) {
            Some(e) => e,
            None => return -1,
        };
        if entry.handle.flush().is_err() {
            return -1;
        }
        for ic in &mut entry.index_circuits {
            if ic.table_mut().flush().is_err() {
                return -1;
            }
        }
        0
    }

    /// Phase 1 of two-phase flush. Walks the table handle and any index
    /// circuits, returning the FlushWorks the caller must drive through
    /// the io_uring fdatasync batch and Phase 3.
    ///
    /// Index circuits are non-durable, so they execute their inline shard
    /// write here and contribute nothing to the returned Vec.
    pub fn flush_prepare(
        &mut self,
        table_id: i64,
    ) -> Result<Vec<(usize, FlushWork)>, String> {
        let entry = match self.tables.get_mut(&table_id) {
            Some(e) => e,
            None => return Ok(vec![]),
        };
        let works = entry.handle.flush_prepare()
            .map_err(|_| format!("flush_prepare failed for table_id={table_id}"))?;
        for ic in &mut entry.index_circuits {
            // Index tables are created Ephemeral, so this is an inline write
            // that returns DoneInline/Empty and resets the memtable. A Pending
            // means a durable index table snuck in — reject it.
            match ic.table_mut().flush_prepare() {
                Ok(FlushOutcome::Empty) | Ok(FlushOutcome::DoneInline) => {}
                Ok(FlushOutcome::Pending(_)) => {
                    return Err(format!(
                        "index flush_prepare unexpectedly returned Pending for table_id={table_id}"
                    ));
                }
                Err(_) => {
                    return Err(format!(
                        "index flush_prepare failed for table_id={table_id}"
                    ));
                }
            }
        }
        Ok(works)
    }

    /// Phase 3 of two-phase flush. Returns one dirfd per partition committed.
    pub fn flush_commit_batch(
        &mut self,
        table_id: i64,
        works: Vec<(usize, FlushWork)>,
    ) -> Result<Vec<libc::c_int>, String> {
        let entry = match self.tables.get_mut(&table_id) {
            Some(e) => e,
            None => return Ok(vec![]),
        };
        entry.handle.flush_commit_batch(works)
            .map_err(|_| format!("flush_commit_batch failed for table_id={table_id}"))
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
                pending.push(PendingEntry { depth, view_id: vid, source_id, batch: b });
            }
        }
        if let Some(d) = delta_opt {
            crate::storage::batch_pool::recycle(d);
        }
        let mut pending_pos: FxHashMap<(i64, i64), usize> = FxHashMap::default();
        Self::resort_pending(&mut pending, &mut pending_pos);
        (pending, pending_pos)
    }

    // ── drive_dag (shared DAG driver) ───────────────────────────────────

    /// Shared engine for both DAG evaluation modes. Seeds the pending queue from
    /// `source_id`'s direct dependents, then repeatedly pops the shallowest
    /// pending edge, runs `execute` on it, ingests the output, and fans that
    /// output onto each downstream edge — until the queue drains. Every modified
    /// view trace is flushed exactly once after the DAG settles.
    ///
    /// `execute` is the per-view step: the single-worker driver runs
    /// `execute_epoch_for_dag`; the multi-worker driver runs the exchange-aware
    /// `execute_multi_worker_step` (capturing its `ExchangeCallback`). It takes
    /// the engine as an explicit `&mut Self` argument — borrowed only for the
    /// call, never captured — and returns the view's output delta, or
    /// `None`/empty when the view produced nothing this epoch.
    ///
    /// `queue_empty` decides the fate of a view that produced no output:
    /// single-worker drops it (`false` — nothing downstream can change), while
    /// multi-worker still fans an empty placeholder onto each dependent (`true`)
    /// so collective exchange rounds stay in lockstep across workers.
    fn drive_dag(
        &mut self,
        source_id: i64,
        delta: Batch,
        queue_empty: bool,
        mut execute: impl FnMut(&mut Self, i64, Batch, i64) -> Option<Batch>,
    ) -> i32 {
        self.get_dep_map();
        let view_ids: Vec<i64> = self.dep_map
            .get(&source_id)
            .cloned()
            .unwrap_or_default();
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
                if self.dep_map.get(&view_id).is_none_or(|d| d.is_empty()) {
                    // Terminal view: move the batch into its family (no clone for
                    // unique_pk) — there is nothing downstream to fan onto.
                    self.ingest_to_family(view_id, out_delta.unwrap());
                    continue;
                }
                self.ingest_by_ref(view_id, out_delta.as_ref().unwrap());
            } else if !queue_empty {
                // Single-worker: an empty output has no downstream effect.
                if let Some(b) = out_delta {
                    crate::storage::batch_pool::recycle(b);
                }
                continue;
            }

            // Fan the output — or, under `queue_empty`, an empty placeholder —
            // onto each dependent edge. Borrow the dep list (disjoint from
            // `&self.tables`) rather than cloning; `map_or` yields an empty slice
            // for a view with no dependents, which queue_dependents no-ops.
            let src_schema = self.tables[&view_id].schema;
            let delta = if has_output { out_delta.as_ref() } else { None };
            let dep_view_ids = self.dep_map.get(&view_id).map_or(&[][..], Vec::as_slice);
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
            let _ = self.flush(vid);
        }

        0
    }

    /// Single-worker DAG evaluation: run each view's epoch directly, with no
    /// exchange IPC. An empty output is dropped — nothing downstream can change.
    pub fn evaluate_dag(&mut self, source_id: i64, delta: Batch) -> i32 {
        gnitz_debug!("dag: evaluate_dag source_id={} delta_count={} tables={}",
            source_id, delta.count, self.tables.len());
        self.drive_dag(source_id, delta, false, |this, view_id, input, src_id| {
            this.execute_epoch_for_dag(view_id, input, src_id)
        })
    }

    /// Multi-worker DAG evaluation with exchange IPC. Same traversal as the
    /// single-worker `evaluate_dag`, but each view runs through
    /// `execute_multi_worker_step` (the pre / exchange / post dance), and
    /// `queue_empty = true` keeps empty placeholders flowing downstream so
    /// collective exchange rounds stay in lockstep across workers.
    pub fn evaluate_dag_multi_worker<E: ExchangeCallback>(
        &mut self,
        source_id: i64,
        delta: Batch,
        exchange: &mut E,
    ) -> i32 {
        self.drive_dag(source_id, delta, true, |this, view_id, input, src_id| {
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
            self.run_two_sided(view_id, input, src_id,
                |pre, side_src| exchange.do_exchange(view_id, &pre, side_src))
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

    // ── Private helpers ─────────────────────────────────────────────────

    /// Restore the pending queue's descending-depth order and rebuild the
    /// `(view_id, source_id) → index` lookup. Both DAG drivers call this after
    /// pushing a new entry.
    fn resort_pending(
        pending: &mut [PendingEntry],
        pending_pos: &mut FxHashMap<(i64, i64), usize>,
    ) {
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
                    let existing = std::mem::replace(
                        &mut pending[existing_idx].batch,
                        Batch::empty_with_schema(&src_schema),
                    );
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

    /// Execute epoch for evaluate_dag, handling pre/post plan split for
    /// single-worker (no exchange IPC).
    fn execute_epoch_for_dag(
        &mut self,
        view_id: i64,
        input: Batch,
        source_id: i64,
    ) -> Option<Batch> {
        if !self.ensure_compiled(view_id) {
            gnitz_warn!("dag: evaluate_dag — no plan for view_id={}", view_id);
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
            let pre_result = self.execute_pre_phase(view_id, input, source_id)
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
        Self::vm_epoch_result(view_id, vm::execute_epoch(
            &sub.vm.program,
            &mut sub.vm.regfile,
            input,
            actual_in_reg,
            sub.out_reg,
            &cursor_ptrs,
            &sub.vm.owned_trace_regs,
        ))
    }

    fn execute_pre_phase(
        &mut self,
        view_id: i64,
        input: Batch,
        source_id: i64,
    ) -> Option<Batch> {
        let plan = self.cache.get_mut(&view_id)?;
        Self::execute_sub_plan(view_id, &mut plan.pre, &self.tables, input, source_id)
    }

    fn execute_post_phase(
        &mut self,
        view_id: i64,
        input: Batch,
    ) -> Option<Batch> {
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
    fn execute_post_multi(
        &mut self,
        view_id: i64,
        inputs: Vec<(u16, Batch)>,
    ) -> Option<Batch> {
        let plan = self.cache.get_mut(&view_id)?;
        let post = plan.post.as_mut()?;
        let (cursor_ptrs, _cursor_storage) =
            Self::build_ext_cursors(&self.tables, &post.ext_trace_regs, post.num_regs as usize);
        post.vm.refresh_owned_cursors();
        Self::vm_epoch_result(view_id, vm::execute_epoch_multi(
            &post.vm.program, &mut post.vm.regfile, inputs, post.out_reg,
            &cursor_ptrs, &post.vm.owned_trace_regs,
        ))
    }

    /// Force a real sort+weight-merge of a post-exchange batch. The exchange
    /// relay concatenates rows from all workers (and a HashRow reindex scrambles
    /// PK order), so the result is not globally sorted — but its `sorted` /
    /// `consolidated` flags are not reliably cleared by the relay path. Clearing
    /// them here guarantees `into_consolidated` actually sorts, which the
    /// downstream merge-walk join/distinct operators depend on.
    fn consolidate_exchanged(mut batch: Batch, schema: &SchemaDescriptor) -> Batch {
        batch.sorted = false;
        batch.consolidated = false;
        batch.into_consolidated(schema).into_inner()
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
    /// `worker_watcher` reaps the exited worker and tears the tick down;
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
                code, view_id,
            ),
        }
    }

    /// Per-side exchange metadata snapshot for both sides of a two-sided join.
    fn two_sided_meta(&self, view_id: i64)
        -> Option<(ExchangeSideMeta, ExchangeSideMeta)>
    {
        let p = self.cache.get(&view_id)?;
        let sb = p.side_b.as_ref()?;
        // A two-sided plan always records side A's pre-exchange output schema;
        // the view's final (combine-widened) schema is a different width for a
        // JOIN and would mislabel side A's operand batch.
        let a_schema = p.exchange_in_schema
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
    /// (one worker owns all shards, so no IPC). The combine (Union / semi /
    /// anti-join) needs sorted, weight-merged input, hence the consolidate.
    fn run_two_sided(
        &mut self,
        view_id: i64,
        input: Batch,
        src_id: i64,
        mut relay: impl FnMut(Batch, i64) -> Batch,
    ) -> Option<Batch> {
        let ((a_schema, a_source, a_seed), (b_schema, b_source, b_seed)) =
            self.two_sided_meta(view_id)?;

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
            (true, true)  => (Some(input.clone_batch()), Some(input)),
            (true, false) => (Some(input), None),
            (false, true)  => (None, Some(input)),
            (false, false) => unreachable!("run_two_sided view {view_id}: source {src_id} matches neither side"),
        };

        // Each scope ends before the next so the mutable cache borrow does not
        // overlap with execute_post_multi's borrow via self.
        let cons_a = {
            let plan = self.cache.get_mut(&view_id).unwrap();
            Self::run_one_side(view_id, &mut plan.pre, &self.tables,
                               a_in, src_id, a_schema, a_source, &mut relay)
        };
        let cons_b = {
            let plan = self.cache.get_mut(&view_id).unwrap();
            Self::run_one_side(view_id, &mut plan.side_b.as_mut().unwrap().plan,
                               &self.tables, b_in, src_id, b_schema, b_source, &mut relay)
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
        let mut pre = self.execute_pre_phase(view_id, pre_input, src_id)
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
            if w > 1 { batch.set_weight(row, 1); }
            else if w < -1 { batch.set_weight(row, -1); }
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
    fn enforce_unique_pk(
        ptable: &mut PartitionedTable,
        schema: &SchemaDescriptor,
        mut batch: Batch,
    ) -> Batch {
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
        let mut seen: FxHashMap<&[u8], usize> =
            FxHashMap::with_capacity_and_hasher(batch.count, Default::default());
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
                effective.append_row_from_ptable_found(ptable, pkb, -1);
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
            if weight == 0 { continue; }
            // Leading index-key slots: each indexed value re-encoded OPK into
            // its promoted slot. The index table's PK region is
            // order-preserving like any other; seeks (has_pk / seek_by_index)
            // encode the same way. NULL in ANY indexed column ⇒ row not
            // indexed; retractions (weight < 0) DO project, so the index
            // entry retracts with its source row.
            if !spec.write_span(&mb, row, &mut idx_pk_buf) { continue; }
            // The source PK region is laid out in pk_indices order, so the
            // index's trailing PK suffix is byte-identical to the source's PK
            // (already OPK).
            idx_pk_buf[idx_key_size..idx_key_size + src_pk_stride]
                .copy_from_slice(src.get_pk_bytes(row));
            out.extend_pk_bytes(&idx_pk_buf[..idx_stride]);
            out.extend_weight(&weight.to_le_bytes());
            // Index schema has zero payload columns, but the null_bmp region
            // is still part of the batch layout. Keep the per-row null-bmp
            // append so the batch's region cursors stay in lockstep with
            // `count` independent of `with_schema`'s zero-init.
            out.extend_null_bmp(&0u64.to_le_bytes());
            out.count += 1;
        }

        out.sorted = false;
        out.consolidated = false;
        out
    }

    /// Close the DagEngine, dropping all cached plans. Test-only, like the
    /// `CatalogEngine::close` that drives it: the server never closes gracefully.
    #[cfg(test)]
    pub fn close(&mut self) {
        self.cache.clear();
        self.tables.clear();
        self.shard_cols_cache.clear();
        self.join_shard_cols_cache.clear();
        self.skip_exchange_cache.clear();
        self.needs_exchange_cache.clear();
        self.range_join_cache.clear();
        self.dep_map.clear();
        self.source_map.clear();
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn dag_test_dir(name: &str) -> String {
        std::env::temp_dir()
            .join(format!("gnitz_dag_test_{name}"))
            .to_str().unwrap().to_owned()
    }

    fn make_test_table(name: &str) -> Box<Table> {
        let schema = SchemaDescriptor::default();
        let dir = dag_test_dir(name);
        let _ = std::fs::remove_dir_all(&dir);
        Box::new(Table::new(&dir, name, schema, 99, 256 * 1024, Persistence::Ephemeral).unwrap())
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
        dag.register_table(100, StoreHandle::Borrowed(&mut *tbl as *mut Table), schema, RelationKind::BaseTable { unique_pk: false }, 0, String::new());
        assert!(dag.tables.contains_key(&100));

        dag.unregister_table(100);
        assert!(!dag.tables.contains_key(&100));
        let _ = std::fs::remove_dir_all(dag_test_dir("reg_unreg"));
    }

    #[test]
    fn test_invalidation() {
        let mut dag = DagEngine::new();
        dag.shard_cols_cache.insert(42, vec![0]);
        dag.skip_exchange_cache.insert(42, true);
        dag.needs_exchange_cache.insert(42, true);
        dag.dep_map_valid = true;

        dag.invalidate(42);
        assert!(!dag.shard_cols_cache.contains_key(&42));
        assert!(!dag.skip_exchange_cache.contains_key(&42));
        assert!(!dag.needs_exchange_cache.contains_key(&42));
        assert!(!dag.dep_map_valid);

        dag.dep_map_valid = true;
        dag.invalidate_dep_map();
        assert!(!dag.dep_map_valid);

        dag.shard_cols_cache.insert(99, vec![1]);
        dag.invalidate_all();
        assert!(dag.shard_cols_cache.is_empty());
    }

    /// `range_join_cache` must be evicted in lockstep with the sibling view-keyed
    /// caches at every invalidation site, or it drifts (a dropped view's stale
    /// `Some(n_eq)`/`None` survives, and the cache disagrees with the live circuit).
    #[test]
    fn test_range_join_cache_eviction() {
        let mut dag = DagEngine::new();

        // invalidate(view_id) drops the per-view entry.
        dag.range_join_cache.insert(42, Some(1));
        dag.invalidate(42);
        assert!(!dag.range_join_cache.contains_key(&42), "invalidate must evict");

        // unregister_table(table_id) drops it (the production view-drop path).
        dag.range_join_cache.insert(43, Some(0));
        dag.unregister_table(43);
        assert!(!dag.range_join_cache.contains_key(&43), "unregister_table must evict");

        // invalidate_all() clears it (the production bulk flush).
        dag.range_join_cache.insert(44, None);
        dag.invalidate_all();
        assert!(dag.range_join_cache.is_empty(), "invalidate_all must clear");
    }

    #[test]
    fn test_add_remove_index_circuit() {
        let mut dag = DagEngine::new();
        let schema = SchemaDescriptor::default();
        let mut tbl = make_test_table("idx_parent");
        dag.register_table(50, StoreHandle::Borrowed(&mut *tbl as *mut Table), schema, RelationKind::BaseTable { unique_pk: false }, 0, String::new());
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
        assert!(dag.dep_map.is_empty());
        assert!(dag.dep_map_valid);
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
        dag.register_table(70, StoreHandle::Borrowed(&mut *tbl as *mut Table), parent_schema, RelationKind::BaseTable { unique_pk: false }, 0, String::new());

        // Durable index table: flush writes shard_*.db only if called.
        let idx_schema = crate::schema::SchemaDescriptor::minimal_u64();
        let idx_dir = dag_test_dir("flush_ic_idx");
        let _ = std::fs::remove_dir_all(&idx_dir);
        let idx_tbl = Box::new(
            Table::new(&idx_dir, "flush_ic_idx", idx_schema, 1, 256 * 1024, Persistence::Durable).unwrap(),
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
        assert_eq!(dag.flush(70), 0);
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
        use crate::schema::{SchemaColumn, type_code};
        crate::util::raise_fd_limit_for_tests();

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
            tdir.to_str().unwrap(), "enforce_signed", schema, 1234, 256, Persistence::Ephemeral, 0, 256,
            crate::storage::partition_arena_size(256),
        ).unwrap();

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
        assert!(!pt.has_pk_bytes(&opk), "stored negative-PK row must be gone after retraction");
    }

    // unique_pk contract: per-PK accumulated weight ∈ {0, 1}. A pushed row at
    // |w| > 1 is the row repeated; retract-before-insert collapses repeats to
    // one live instance, so the effective batch must carry unit weights —
    // otherwise a weight-2 PK row lands (two live instances the -1-normalized
    // retraction arm can never fully delete) and the CREATE UNIQUE INDEX PK
    // short-circuit's premise breaks.
    #[test]
    fn test_enforce_unique_pk_weight_normalized() {
        use crate::schema::{SchemaColumn, type_code};
        crate::util::raise_fd_limit_for_tests();

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
            tdir.to_str().unwrap(), "enforce_weight_norm", schema, 1234, 256, Persistence::Ephemeral, 0, 256,
            crate::storage::partition_arena_size(256),
        ).unwrap();

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
        assert_eq!(effective.get_weight(0), -1, "retraction weight must be normalized to -1");
        pt.ingest_owned_batch(effective).unwrap();
        assert!(!pt.has_pk_bytes(&opk), "row must be fully gone after the delete");
    }

    // Positivity regression: a retraction of a key that is absent (never inserted)
    // or tombstoned (inserted then removed) must NOT pass a negative-weight phantom
    // row through to the store. Pre-fix the `else if !found` arm appended the raw
    // `(-1, filler)` row, leaving a base table at net weight -1.
    #[test]
    fn test_enforce_unique_pk_absent_key_drops_phantom() {
        use crate::schema::{SchemaColumn, type_code};
        crate::util::raise_fd_limit_for_tests();

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
            tdir.to_str().unwrap(), "enforce_absent", schema, 1234, 256, Persistence::Ephemeral, 0, 256,
            crate::storage::partition_arena_size(256),
        ).unwrap();

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
        assert_eq!(eff1.count, 1, "retracting a present key emits the stored-row retraction");
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
        crate::util::raise_fd_limit_for_tests();

        let schema = wide_pk_3xu64_schema();
        let dir = tempfile::tempdir().unwrap();
        let tdir = dir.path().join("enforce_wide");
        let mut pt = crate::storage::PartitionedTable::new(
            tdir.to_str().unwrap(), "enforce_wide", schema, 555, 256, Persistence::Ephemeral, 0, 256,
            crate::storage::partition_arena_size(256),
        ).unwrap();

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
        assert_eq!(eff.get_pk_bytes(0), &pk24(1, 2, 3)[..], "retraction keys on the full 24-byte PK");
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

    // Spawn a clean subprocess to verify that vm_epoch_result calls
    // gnitz_fatal_abort! (→ _exit(134)) on Err. A direct in-process call would
    // terminate the test runner; std::process::Command gives us a clean process
    // with no multi-threaded fork hazard.
    #[test]
    fn test_vm_epoch_result_abort_exit_status() {
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("test_vm_epoch_result_abort_internal")
            .env("GNITZ_RUN_ABORT_TEST", "1")
            .status()
            .unwrap();
        // _exit(134) is a normal (non-signal) exit on Linux, so code() == Some(134).
        assert_eq!(
            status.code(),
            Some(134),
            "vm_epoch_result must call gnitz_fatal_abort! (exit 134) on Err",
        );
    }

    // Guard: runs only when GNITZ_RUN_ABORT_TEST=1 (set by the parent test above).
    // Constructs an Err(-10) result and passes it to vm_epoch_result, which must
    // call gnitz_fatal_abort!. The parent asserts exit code 134.
    #[test]
    fn test_vm_epoch_result_abort_internal() {
        if std::env::var("GNITZ_RUN_ABORT_TEST").is_err() { return; }
        let r: Result<Option<Batch>, i32> = Err(-10);
        DagEngine::vm_epoch_result(42, r);
        unreachable!("vm_epoch_result must not return on Err");
    }
}
