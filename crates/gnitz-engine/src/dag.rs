//! DagEngine: consolidated plan cache, DAG evaluator, and ingestion pipeline.

use std::cell::UnsafeCell;
use std::collections::{HashMap, HashSet};
use rustc_hash::{FxHashMap, FxHashSet};

use crate::schema::SchemaDescriptor;
use crate::compiler::{self, CompileOutput, ExternalTable};
use crate::storage::{Batch, CursorHandle, FlushOutcome, FlushWork, PkBuf, Table, PartitionedTable, StorageError};
use crate::ops;
use crate::vm;

// ---------------------------------------------------------------------------
// Store handle — views vs base tables
// ---------------------------------------------------------------------------

/// Distinguishes ephemeral (view) stores from partitioned (base table) stores.
/// Owned variants (Single, Partitioned) drop the underlying storage on Drop.
/// The Borrowed variant holds a non-owning pointer for system tables owned
/// by CatalogEngine directly.
pub enum StoreHandle {
    /// Owned single Table — used by views in single-worker mode.
    #[allow(dead_code)]
    Single(Box<Table>),
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
    /// Get a raw pointer to the Table (Single/Borrowed), or null for Partitioned.
    /// SAFETY: the Box outlives any synchronous call using the pointer.
    /// The caller must ensure no aliasing &mut references exist.
    pub fn table_ptr(&self) -> *mut Table {
        match self {
            StoreHandle::Single(t) => std::ptr::addr_of!(**t) as *mut Table,
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
    pub fn as_partitioned_mut(&self) -> Option<&mut PartitionedTable> {
        match self {
            StoreHandle::Partitioned(cell) => Some(unsafe { &mut **cell.get() }),
            _ => None,
        }
    }

    /// Dispatched `has_pk` that works for every variant.
    pub fn has_pk(&self, key: u128) -> bool {
        let tptr = self.table_ptr();
        unsafe {
            if !tptr.is_null() { (*tptr).has_pk(key) } else { (*self.ptable_ptr()).has_pk(key) }
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
            StoreHandle::Single(t) => t.flush(),
            StoreHandle::Borrowed(ptr) => unsafe { &mut **ptr }.flush(),
            StoreHandle::Partitioned(cell) => cell.get_mut().flush(),
        }
    }

    /// Dispatched Phase 1 across all variants. Returns one
    /// (partition_idx, FlushWork) per partition that produced deferred
    /// work; for Single/Borrowed `partition_idx` is always 0.
    pub fn flush_prepare(&mut self) -> Result<Vec<(usize, FlushWork)>, StorageError> {
        match self {
            StoreHandle::Single(t) => match t.flush_prepare(true)? {
                FlushOutcome::Empty | FlushOutcome::DoneInline => Ok(Vec::new()),
                FlushOutcome::Pending(w) => Ok(vec![(0, *w)]),
            },
            StoreHandle::Borrowed(ptr) => match unsafe { &mut **ptr }.flush_prepare(true)? {
                FlushOutcome::Empty | FlushOutcome::DoneInline => Ok(Vec::new()),
                FlushOutcome::Pending(w) => Ok(vec![(0, *w)]),
            },
            StoreHandle::Partitioned(cell) => cell.get_mut().flush_prepare(),
        }
    }

    /// Dispatched Phase 3 across all variants. Returns one dirfd per
    /// committed partition.
    pub fn flush_commit_batch(
        &mut self,
        works: Vec<(usize, FlushWork)>,
    ) -> Result<Vec<libc::c_int>, StorageError> {
        let t: &mut Table = match self {
            StoreHandle::Single(t) => t,
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
            StoreHandle::Single(t) => t.current_lsn,
            StoreHandle::Borrowed(ptr) => unsafe { &**ptr }.current_lsn,
            StoreHandle::Partitioned(cell) => unsafe { (**cell.get()).current_lsn() },
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
        unsafe { &mut **self.index_table.get() }
    }
}

// ---------------------------------------------------------------------------
// Index circuit entry
// ---------------------------------------------------------------------------

/// A secondary index on a column.
/// Owns the index Table via Box — dropping the entry drops the table.
pub struct IndexCircuitEntry {
    pub col_idx: u32,
    pub index_table: UnsafeCell<Box<Table>>,
    pub index_schema: SchemaDescriptor,
    pub is_unique: bool,
}

// ---------------------------------------------------------------------------
// Table entry — per-table metadata in the entity registry
// ---------------------------------------------------------------------------

pub struct TableEntry {
    pub handle: StoreHandle,
    pub schema: SchemaDescriptor,
    pub depth: i32,
    pub unique_pk: bool,
    pub directory: String,
    pub index_circuits: Vec<IndexCircuitEntry>,
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
// Exchange info
// ---------------------------------------------------------------------------

/// Cached exchange metadata for a view.
pub struct ExchangeInfo {
    pub is_trivial: bool,
    pub is_co_partitioned: bool,
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
    join_shard_cols_cache: FxHashMap<(i64, i64), Vec<i32>>,
    exchange_info_cache: FxHashMap<i64, ExchangeInfo>,
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

impl DagEngine {
    pub fn new() -> Self {
        DagEngine {
            cache: FxHashMap::default(),
            dep_map: FxHashMap::default(),
            source_map: FxHashMap::default(),
            dep_map_valid: false,
            shard_cols_cache: FxHashMap::default(),
            join_shard_cols_cache: FxHashMap::default(),
            exchange_info_cache: FxHashMap::default(),
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
        depth: i32,
        unique_pk: bool,
        directory: String,
    ) {
        self.tables.insert(table_id, TableEntry {
            handle,
            schema,
            depth,
            unique_pk,
            directory,
            index_circuits: Vec::new(),
        });
    }

    pub fn unregister_table(&mut self, table_id: i64) {
        self.tables.remove(&table_id);
        self.cache.remove(&table_id);
        self.shard_cols_cache.remove(&table_id);
        self.join_shard_cols_cache.retain(|&(v, s), _| v != table_id && s != table_id);
        self.exchange_info_cache.remove(&table_id);
        self.dep_map_valid = false;
    }

    pub fn add_index_circuit(
        &mut self,
        table_id: i64,
        col_idx: u32,
        index_table: Box<Table>,
        index_schema: SchemaDescriptor,
        is_unique: bool,
    ) {
        if let Some(entry) = self.tables.get_mut(&table_id) {
            entry.index_circuits.push(IndexCircuitEntry {
                col_idx,
                index_table: UnsafeCell::new(index_table),
                index_schema,
                is_unique,
            });
        }
    }

    pub fn remove_index_circuit(&mut self, table_id: i64, col_idx: u32) {
        if let Some(entry) = self.tables.get_mut(&table_id) {
            // retain() drops non-matching entries, which drops Box<Table> automatically.
            entry.index_circuits.retain(|ic| ic.col_idx != col_idx);
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
            plan.pre_vm.regfile.clear_delta_batches();
            if let Some(post) = plan.post_vm.as_mut() {
                post.regfile.clear_delta_batches();
            }
        }
    }

    // ── Cache management ────────────────────────────────────────────────

    pub fn invalidate(&mut self, view_id: i64) {
        self.cache.remove(&view_id);
        self.shard_cols_cache.remove(&view_id);
        self.join_shard_cols_cache.retain(|&(v, _), _| v != view_id);
        self.exchange_info_cache.remove(&view_id);
        self.dep_map_valid = false;
    }

    pub fn invalidate_all(&mut self) {
        self.cache.clear();
        self.shard_cols_cache.clear();
        self.join_shard_cols_cache.clear();
        self.exchange_info_cache.clear();
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
                        // in the 16-byte PK region, view_id in bytes 0..8.
                        let pk = ch.cursor.current_pk_bytes();
                        let v_id = u64::from_le_bytes(pk[0..8].try_into().unwrap()) as i64;
                        let dep_tid = u64::from_le_bytes(pk[8..16].try_into().unwrap()) as i64;
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

    /// Get join shard columns for a specific source within a view.
    pub fn get_join_shard_cols(&mut self, view_id: i64, source_id: i64) -> Vec<i32> {
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

    fn compute_join_shard_cols(&self, view_id: i64, source_id: i64) -> Vec<i32> {
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

        // Find the downstream Map(Expression { reindex_col }) node, walking
        // through any intervening Filter nodes (planner emits Filter → Map
        // reindex chains for PK-redistribution views).
        crate::compiler::reindex_cols_through_filters(&loaded, scan_nid)
    }

    /// Get exchange info for a view: (shard_cols, is_trivial, is_co_partitioned).
    pub fn get_exchange_info(&mut self, view_id: i64) -> &ExchangeInfo {
        if self.exchange_info_cache.contains_key(&view_id) {
            return &self.exchange_info_cache[&view_id];
        }

        let loaded = self.load_meta_circuit(view_id);

        let mut shard_cols = Vec::new();
        let mut is_trivial = false;
        let mut is_co_partitioned = false;

        // Find the ExchangeShard node and its shard columns.
        let exchange_nid = loaded.nodes.iter().find_map(|(&nid, op)| {
            if let gnitz_wire::OpNode::ExchangeShard { shard_cols: sc } = op {
                Some((nid, sc.iter().map(|&c| c as i32).collect::<Vec<_>>()))
            } else {
                None
            }
        });
        if let Some((enid, cols)) = exchange_nid {
            shard_cols = cols;

            // Collect nodes that feed directly into ExchangeShard.
            let incoming_srcs: Vec<i32> = loaded.edges.iter()
                .filter(|&&(_, dst, _)| dst == enid)
                .map(|&(src, _, _)| src)
                .collect();

            if incoming_srcs.len() == 1 {
                let src_nid = incoming_srcs[0];
                if !loaded.edges.iter().any(|&(_, dst, _)| dst == src_nid) {
                    is_trivial = true;
                    if let Some(gnitz_wire::OpNode::ScanTrace(tid)) = loaded.nodes.get(&src_nid) {
                        if *tid > 0 && shard_cols.len() == 1 {
                            if let Some(entry) = self.tables.get(&(*tid as i64)) {
                                if entry.schema.is_pk_col(shard_cols[0] as usize) {
                                    is_co_partitioned = true;
                                }
                            }
                        }
                    }
                }
            }
        }

        self.shard_cols_cache.insert(view_id, shard_cols.clone());
        self.exchange_info_cache.insert(view_id, ExchangeInfo { is_trivial, is_co_partitioned });
        &self.exchange_info_cache[&view_id]
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

    /// Check if a view needs exchange (has EXCHANGE_SHARD node).
    pub fn view_needs_exchange(&mut self, view_id: i64) -> bool {
        let loaded = self.load_meta_circuit(view_id);
        loaded.nodes.values().any(|op| matches!(op, gnitz_wire::OpNode::ExchangeShard { .. }))
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
                gnitz_debug!("dag: compiled view_id={}, pre_regs={}", view_id, output.pre_num_regs);
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
        if !self.cache.contains_key(&view_id) {
            if let Some(plan) = self.compile_view_internal(view_id) {
                self.cache.insert(view_id, plan);
            } else {
                gnitz_warn!("dag: execute_epoch — no plan for view_id={}", view_id);
                return None;
            }
        }

        let plan = self.cache.get(&view_id).unwrap();
        let num_regs = plan.pre_num_regs as usize;
        let etr = plan.pre_ext_trace_regs.clone(); // TODO: avoid clone once borrow split is refactored
        let (cursor_ptrs, _cursor_storage) = self.build_ext_cursors(&etr, num_regs);

        let plan = self.cache.get_mut(&view_id).unwrap();

        // Determine input register based on source_id
        let in_reg = if source_id > 0 {
            plan.source_reg_map.get(&source_id).copied().unwrap_or(plan.pre_in_reg)
        } else {
            plan.pre_in_reg
        };

        // Refresh owned cursors
        plan.pre_vm.refresh_owned_cursors();

        // Execute
        match vm::execute_epoch(
            &plan.pre_vm.program,
            &mut plan.pre_vm.regfile,
            input,
            in_reg,
            plan.pre_out_reg,
            &cursor_ptrs,
            &plan.pre_vm.owned_trace_regs,
        ) {
            Ok(Some(batch)) if batch.count > 0 => Some(batch),
            Ok(_) => None,
            Err(code) => {
                gnitz_warn!("dag: execute_epoch error {} for view_id={}", code, view_id);
                None
            }
        }
    }

    /// Check if a source_id is co-partitioned for a view (from cached plan).
    pub fn plan_source_co_partitioned(&mut self, view_id: i64, source_id: i64) -> bool {
        if !self.cache.contains_key(&view_id) {
            if let Some(plan) = self.compile_view_internal(view_id) {
                self.cache.insert(view_id, plan);
            } else {
                return false;
            }
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
        let entry = match self.tables.get(&table_id) {
            Some(e) => e,
            None => return -1,
        };

        if entry.unique_pk {
            return self.ingest_to_family(table_id, batch.clone_batch());
        }

        if batch.count == 0 {
            return 0;
        }

        let schema = entry.schema;

        let entry = self.tables.get_mut(&table_id).unwrap();
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
        let unique_pk = entry.unique_pk;
        let tbl_ptr = entry.handle.table_ptr();
        let ptbl_ptr = entry.handle.ptable_ptr();

        let effective_batch = if unique_pk {
            if !ptbl_ptr.is_null() {
                let ptable = unsafe { &mut *ptbl_ptr };
                self.enforce_unique_pk_partitioned(ptable, &schema, batch)
            } else if !tbl_ptr.is_null() {
                let table = unsafe { &mut *tbl_ptr };
                self.enforce_unique_pk(table, &schema, batch)
            } else {
                batch
            }
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
            .map(|ic| Self::batch_project_index(source, ic.col_idx, schema, &ic.index_schema))
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
            .map_err(|_| format!("flush_prepare failed for table_id={}", table_id))?;
        for ic in &mut entry.index_circuits {
            // Index tables are non-durable; this is an inline write that
            // returns DoneInline/Empty and resets the memtable. No FlushWork.
            match ic.table_mut().flush_prepare(false) {
                Ok(FlushOutcome::Empty) | Ok(FlushOutcome::DoneInline) => {}
                Ok(FlushOutcome::Pending(_)) => {
                    return Err(format!(
                        "index flush_prepare unexpectedly returned Pending for table_id={}",
                        table_id
                    ));
                }
                Err(_) => {
                    return Err(format!(
                        "index flush_prepare failed for table_id={}", table_id
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
            .map_err(|_| format!("flush_commit_batch failed for table_id={}", table_id))
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
        pending.sort_by_key(|n| std::cmp::Reverse(n.depth));
        let mut pending_pos: FxHashMap<(i64, i64), usize> = FxHashMap::default();
        for (i, pe) in pending.iter().enumerate() {
            pending_pos.insert((pe.view_id, pe.source_id), i);
        }
        (pending, pending_pos)
    }

    // ── evaluate_dag (single-worker) ────────────────────────────────────

    /// Full single-worker DAG evaluation.
    pub fn evaluate_dag(&mut self, source_id: i64, delta: Batch) -> i32 {
        gnitz_debug!("dag: evaluate_dag source_id={} delta_count={} tables={}",
            source_id, delta.count, self.tables.len());
        // 1. Get dep_map
        self.get_dep_map();
        let view_ids: Vec<i64> = self.dep_map
            .get(&source_id)
            .cloned()
            .unwrap_or_default();

        if view_ids.is_empty() {
            return 0;
        }

        // 2. Build initial pending list
        let (mut pending, mut pending_pos) = self.build_pending(&view_ids, source_id, delta);
        let mut dirty_views: FxHashSet<i64> = FxHashSet::default();

        // 3. Process
        while let Some(entry) = pending.pop() {
            let view_id = entry.view_id;
            let src_id = entry.source_id;
            let input = entry.batch;
            pending_pos.remove(&(view_id, src_id));

            // Check table still exists (may have been dropped)
            if !self.tables.contains_key(&view_id) {
                crate::storage::batch_pool::recycle(input);
                continue;
            }

            // Execute
            let out_delta = match self.execute_epoch_for_dag(view_id, input, src_id) {
                Some(batch) => batch,
                None => continue,
            };

            if out_delta.count == 0 {
                crate::storage::batch_pool::recycle(out_delta);
                continue;
            }

            // Queue dependents
            let dep_view_ids: Vec<i64> = self.dep_map
                .get(&view_id)
                .cloned()
                .unwrap_or_default();

            if dep_view_ids.is_empty() {
                // Terminal view: move batch directly — no clone for unique_pk.
                self.ingest_to_family(view_id, out_delta);
            } else {
                self.ingest_by_ref(view_id, &out_delta);
                for dep_id in dep_view_ids {
                    let (dep_depth, dep_schema) = match self.tables.get(&dep_id) {
                        Some(e) => (e.depth, e.schema),
                        None => continue,
                    };

                    if let Some(&existing_idx) = pending_pos.get(&(dep_id, view_id)) {
                        if existing_idx < pending.len() {
                            let existing = std::mem::replace(
                                &mut pending[existing_idx].batch,
                                Batch::empty_with_schema(&dep_schema),
                            );
                            let schema = existing.schema.unwrap_or(dep_schema);
                            let merged = ops::op_union(
                                existing,
                                Some(&out_delta),
                                &schema,
                            );
                            pending[existing_idx].batch = merged;
                        }
                    } else {
                        let new_idx = pending.len();
                        pending.push(PendingEntry {
                            depth: dep_depth,
                            view_id: dep_id,
                            source_id: view_id,
                            batch: out_delta.clone_batch(),
                        });
                        pending_pos.insert((dep_id, view_id), new_idx);
                        // Re-sort to maintain descending depth order
                        pending.sort_by_key(|n| std::cmp::Reverse(n.depth));
                        // Rebuild pending_pos
                        pending_pos.clear();
                        for (i, pe) in pending.iter().enumerate() {
                            pending_pos.insert((pe.view_id, pe.source_id), i);
                        }
                    }
                }
                crate::storage::batch_pool::recycle(out_delta);
            }
            dirty_views.insert(view_id);
        }

        // Flush each modified view trace exactly once after the full DAG settles.
        for vid in dirty_views {
            let _ = self.flush(vid);
        }

        0
    }

    // ── evaluate_dag_multi_worker ──────────────────────────────────────

    /// Multi-worker DAG evaluation with exchange IPC.
    ///
    /// Near-copy of `evaluate_dag()` with these critical differences:
    /// 1. Exchange views: call `exchange.do_exchange()` between pre and post
    ///    phases — unless `skip_exchange` (both trivial AND co-partitioned).
    /// 2. Join shard exchange: when `get_join_shard_cols` is non-empty AND NOT
    ///    `plan_source_co_partitioned`, call exchange before execute_epoch.
    /// 3. Always queue downstream views even when output is empty (ensures
    ///    exchange-dependent views still fire).
    /// 4. Exchange schema: use `get_exchange_schema()` for pre-plan output,
    ///    not the view's output schema.
    pub fn evaluate_dag_multi_worker<E: ExchangeCallback>(
        &mut self,
        source_id: i64,
        delta: Batch,
        exchange: &mut E,
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

            if !self.tables.contains_key(&view_id) {
                crate::storage::batch_pool::recycle(input);
                continue;
            }

            self.ensure_compiled(view_id);

            let has_exchange = self.view_needs_exchange(view_id);

            let has_join_shard = {
                let cols = self.get_join_shard_cols(view_id, src_id);
                !cols.is_empty()
            };

            let skip_exchange = {
                let info = self.get_exchange_info(view_id);
                info.is_trivial && info.is_co_partitioned
            };

            let out_delta = if has_exchange {
                // Exchange view: pre-plan → exchange IPC → post-plan
                let exchange_schema = self.get_exchange_schema(view_id)
                    .unwrap_or_else(|| self.tables[&view_id].schema);

                let pre_result = match self.execute_plan_phase(view_id, true, input, src_id) {
                    Some(mut batch) => {
                        batch.set_schema(exchange_schema);
                        batch
                    }
                    None => Batch::with_schema(exchange_schema, 0),
                };

                if skip_exchange {
                    self.execute_plan_phase(view_id, false, pre_result, 0)
                } else {
                    let exchanged = exchange.do_exchange(view_id, &pre_result, 0);
                    self.execute_plan_phase(view_id, false, exchanged, 0)
                }
            } else if has_join_shard {
                // Join shard exchange
                let copart_join = self.plan_source_co_partitioned(view_id, src_id);
                if copart_join {
                    self.execute_plan_phase(view_id, true, input, src_id)
                } else {
                    // Set batch schema for the exchange wire encoding.
                    let mut input_with_schema = input;
                    if input_with_schema.schema.is_none() {
                        if let Some(entry) = self.tables.get(&src_id) {
                            input_with_schema.set_schema(entry.schema);
                        }
                    }
                    let exchanged = exchange.do_exchange(view_id, &input_with_schema, src_id);
                    self.execute_plan_phase(view_id, true, exchanged, src_id)
                }
            } else {
                // No exchange: simple single-phase execution
                self.execute_plan_phase(view_id, true, input, src_id)
            };

            let has_output = out_delta.as_ref().map(|b| b.count > 0).unwrap_or(false);

            // Multi-worker: ALWAYS queue dependents (even empty output)
            let dep_view_ids: Vec<i64> = self.dep_map
                .get(&view_id)
                .cloned()
                .unwrap_or_default();

            if has_output {
                if dep_view_ids.is_empty() {
                    // Terminal view: move batch directly — no clone for unique_pk.
                    self.ingest_to_family(view_id, out_delta.unwrap());
                    dirty_views.insert(view_id);
                    continue;
                }
                self.ingest_by_ref(view_id, out_delta.as_ref().unwrap());
                dirty_views.insert(view_id);
            }

            for dep_id in dep_view_ids {
                let (dep_depth, dep_schema) = match self.tables.get(&dep_id) {
                    Some(e) => (e.depth, e.schema),
                    None => continue,
                };

                if let Some(&existing_idx) = pending_pos.get(&(dep_id, view_id)) {
                    if existing_idx < pending.len() && has_output {
                        let existing = std::mem::replace(
                            &mut pending[existing_idx].batch,
                            Batch::empty_with_schema(&dep_schema),
                        );
                        let schema = existing.schema.unwrap_or(dep_schema);
                        let merged = ops::op_union(
                            existing,
                            out_delta.as_ref(),
                            &schema,
                        );
                        pending[existing_idx].batch = merged;
                    }
                } else {
                    let new_idx = pending.len();
                    if has_output {
                        pending.push(PendingEntry {
                            depth: dep_depth,
                            view_id: dep_id,
                            source_id: view_id,
                            batch: out_delta.as_ref().unwrap().clone_batch(),
                        });
                    } else {
                        pending.push(PendingEntry {
                            depth: dep_depth,
                            view_id: dep_id,
                            source_id: view_id,
                            batch: Batch::with_schema(dep_schema, 0),
                        });
                    }
                    pending_pos.insert((dep_id, view_id), new_idx);
                    pending.sort_by_key(|n| std::cmp::Reverse(n.depth));
                    pending_pos.clear();
                    for (i, pe) in pending.iter().enumerate() {
                        pending_pos.insert((pe.view_id, pe.source_id), i);
                    }
                }
            }
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

    // ── Private helpers ─────────────────────────────────────────────────

    /// Build ext cursor handles indexed by register ID.
    /// Returns (cursor_ptrs, storage_owner). Storage must outlive cursor_ptrs.
    #[allow(clippy::vec_box)]
    fn build_ext_cursors(
        &self,
        ext_trace_regs: &[(u16, i64)],
        num_regs: usize,
    ) -> (Vec<*mut libc::c_void>, Vec<Box<CursorHandle>>) {
        let mut ptrs: Vec<*mut libc::c_void> = vec![std::ptr::null_mut(); num_regs];
        let mut storage: Vec<Box<CursorHandle>> = Vec::new();
        for &(reg_id, table_id) in ext_trace_regs {
            if let Some(entry) = self.tables.get(&table_id) {
                // External-trace reads are the operator-state read path: keep
                // them compacting so L0 on intermediate/trace tables stays
                // bounded (no background compactor yet). These are not
                // validators, so a compaction Err safely degrades to `None`.
                #[allow(clippy::disallowed_methods)] // explicit maintenance: operator-state trace read
                let cursor_opt = entry.handle.create_cursor_compacting().ok();
                if let Some(ch) = cursor_opt {
                    let mut boxed = Box::new(ch);
                    if (reg_id as usize) < num_regs {
                        ptrs[reg_id as usize] = &mut *boxed as *mut CursorHandle as *mut libc::c_void;
                    }
                    storage.push(boxed);
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
        // Ensure plan is compiled
        if !self.cache.contains_key(&view_id) {
            if let Some(plan) = self.compile_view_internal(view_id) {
                self.cache.insert(view_id, plan);
            } else {
                gnitz_warn!("dag: evaluate_dag — no plan for view_id={}", view_id);
                return None;
            }
        }

        let plan = self.cache.get(&view_id).unwrap();
        let has_post = plan.post_vm.is_some();

        if has_post {
            // Exchange plan: single-worker runs pre then post sequentially
            let pre_result = self.execute_plan_phase(view_id, true, input, source_id)?;
            self.execute_plan_phase(view_id, false, pre_result, 0)
        } else {
            self.execute_plan_phase(view_id, true, input, source_id)
        }
    }

    /// Execute either the pre or post phase of a plan.
    fn execute_plan_phase(
        &mut self,
        view_id: i64,
        is_pre: bool,
        input: Batch,
        source_id: i64,
    ) -> Option<Batch> {
        let plan = self.cache.get_mut(&view_id)?;

        let (in_reg, out_reg, num_regs) = if is_pre {
            (plan.pre_in_reg, plan.pre_out_reg, plan.pre_num_regs as usize)
        } else {
            (plan.post_in_reg, plan.post_out_reg, plan.post_num_regs as usize)
        };
        let etr = if is_pre {
            plan.pre_ext_trace_regs.clone()
        } else {
            plan.post_ext_trace_regs.clone()
        };

        let (cursor_ptrs, _cursor_storage) = self.build_ext_cursors(&etr, num_regs);

        let actual_in_reg = if source_id > 0 && is_pre {
            self.cache.get(&view_id).unwrap()
                .source_reg_map.get(&source_id).copied().unwrap_or(in_reg)
        } else {
            in_reg
        };

        let plan = self.cache.get_mut(&view_id).unwrap();
        let vm = if is_pre {
            &mut plan.pre_vm
        } else {
            plan.post_vm.as_mut().unwrap()
        };

        vm.refresh_owned_cursors();

        match vm::execute_epoch(
            &vm.program,
            &mut vm.regfile,
            input,
            actual_in_reg,
            out_reg,
            &cursor_ptrs,
            &vm.owned_trace_regs,
        ) {
            Ok(Some(batch)) if batch.count > 0 => Some(batch),
            Ok(_) => None,
            Err(code) => {
                gnitz_warn!("dag: execute error {} for view_id={}", code, view_id);
                None
            }
        }
    }

    /// Enforce unique PK semantics: retract existing rows before inserting.
    /// Port of `registry._enforce_unique_pk()`.
    /// Used for Single (view) stores where retractions don't need to propagate.
    fn enforce_unique_pk(
        &self,
        table: &mut Table,
        schema: &SchemaDescriptor,
        batch: Batch,
    ) -> Batch {
        if batch.count == 0 {
            return batch;
        }
        if schema.pk_is_wide() {
            return self.enforce_unique_pk_wide(table, schema, batch);
        }

        let mut effective = Batch::with_schema(*schema, batch.count * 2);
        // pk → batch row index of the last +1 insertion in this batch.
        // Must be a batch index (not effective index) because
        // `append_batch_negated` below indexes into `batch`.
        let mut seen: HashMap<u128, usize> = HashMap::with_capacity(batch.count);

        for row in 0..batch.count {
            let w = batch.get_weight(row);
            let pk = batch.get_pk(row);

            if w > 0 {
                // Positive weight = insertion → retract existing if any
                let (_existing_w, _found) = table.retract_pk(pk);

                // Intra-batch dedup: if we've already seen this PK in this batch,
                // retract the previous insertion.
                if let Some(&prev_pos) = seen.get(&pk) {
                    // The previous row at prev_pos was a +1 insertion.
                    // We need to retract it by adding a -1 with the same payload.
                    effective.append_batch_negated(&batch, prev_pos, prev_pos + 1);
                }

                // Add the new insertion
                effective.append_batch(&batch, row, row + 1);
                seen.insert(pk, row);
            } else if w < 0 {
                // Negative weight = deletion
                effective.append_batch(&batch, row, row + 1);
            }
        }

        effective
    }

    /// Wide-PK (`pk_stride > 16`) variant of [`enforce_unique_pk`]. Keys the
    /// dedup map on `PkBuf` (probed zero-copy via `Borrow<[u8]>`) and retracts
    /// via `retract_pk_bytes`, since a wide PK cannot be packed into a `u128`.
    fn enforce_unique_pk_wide(
        &self,
        table: &mut Table,
        schema: &SchemaDescriptor,
        batch: Batch,
    ) -> Batch {
        let mut effective = Batch::with_schema(*schema, batch.count * 2);
        let mut seen: HashMap<PkBuf, usize> = HashMap::with_capacity(batch.count);

        for row in 0..batch.count {
            let w = batch.get_weight(row);
            let pkb = batch.get_pk_bytes(row);

            if w > 0 {
                let (_existing_w, _found) = table.retract_pk_bytes(pkb);
                if let Some(&prev_pos) = seen.get(pkb) {
                    effective.append_batch_negated(&batch, prev_pos, prev_pos + 1);
                }
                effective.append_batch(&batch, row, row + 1);
                seen.insert(PkBuf::from_bytes(pkb), row);
            } else if w < 0 {
                effective.append_batch(&batch, row, row + 1);
            }
        }

        effective
    }

    /// Enforce unique PK for PartitionedTable (user base tables).
    ///
    /// Unlike `enforce_unique_pk` for Single stores, this version propagates
    /// retraction rows into the effective batch.  Downstream views need to see
    /// `(-1, old_row)` + `(+1, new_row)` to incrementally update.
    fn enforce_unique_pk_partitioned(
        &self,
        ptable: &mut PartitionedTable,
        schema: &SchemaDescriptor,
        batch: Batch,
    ) -> Batch {
        if batch.count == 0 {
            return batch;
        }
        if schema.pk_is_wide() {
            return self.enforce_unique_pk_partitioned_wide(ptable, schema, batch);
        }

        let mut effective = Batch::with_schema(*schema, batch.count * 2);
        // pk → batch row index of the last +1 insertion in this batch.
        // Must be a batch index (not effective index) because
        // `append_batch_negated` below indexes into `batch`, and the
        // effective batch may contain extra store-retraction rows that
        // break any 1:1 correspondence with `row`.
        let mut seen: HashMap<u128, usize> = HashMap::with_capacity(batch.count);
        // pk set of store rows already retracted in this batch.
        // `retract_pk` is read-only (sets `found_source` only), so calling
        // it twice for the same PK would emit the stored-row retraction
        // twice and drive downstream weights negative.
        let mut store_retracted: HashSet<u128> = HashSet::with_capacity(batch.count);

        for row in 0..batch.count {
            let w = batch.get_weight(row);
            let pk = batch.get_pk(row);

            if w > 0 {
                // Check store for existing row with this PK
                let (_existing_w, found) = ptable.retract_pk(pk);
                if found && !store_retracted.contains(&pk) {
                    // Emit retraction of existing stored row so downstream views
                    // see the removal of the old payload — exactly once per PK.
                    effective.append_row_from_ptable_found(ptable, batch.get_pk_bytes(row), -1);
                    store_retracted.insert(pk);
                }

                // Intra-batch dedup: retract previous insertion of same PK
                if let Some(&prev_pos) = seen.get(&pk) {
                    effective.append_batch_negated(&batch, prev_pos, prev_pos + 1);
                }

                // Add the new insertion
                effective.append_batch(&batch, row, row + 1);
                seen.insert(pk, row);
            } else if w < 0 {
                // Negative weight = explicit retraction
                let (_existing_w, found) = ptable.retract_pk(pk);
                let store_hit = found && !store_retracted.contains(&pk);
                if store_hit {
                    // Emit retraction using stored payload (the actual column data)
                    effective.append_row_from_ptable_found(ptable, batch.get_pk_bytes(row), -1);
                    store_retracted.insert(pk);
                }

                // Intra-batch dedup: if this PK was inserted earlier in this batch,
                // negate that insertion to cancel it out.
                if let Some(&prev_pos) = seen.get(&pk) {
                    effective.append_batch_negated(&batch, prev_pos, prev_pos + 1);
                    seen.remove(&pk);
                } else if !found {
                    // Not in store and not in batch — pass through as-is
                    effective.append_batch(&batch, row, row + 1);
                }
            }
        }

        effective
    }

    /// Wide-PK (`pk_stride > 16`) variant of [`enforce_unique_pk_partitioned`].
    /// Keys the dedup map and store-retracted set on `PkBuf` (probed zero-copy
    /// via `Borrow<[u8]>`) and retracts via `retract_pk_bytes`.
    fn enforce_unique_pk_partitioned_wide(
        &self,
        ptable: &mut PartitionedTable,
        schema: &SchemaDescriptor,
        batch: Batch,
    ) -> Batch {
        let mut effective = Batch::with_schema(*schema, batch.count * 2);
        let mut seen: HashMap<PkBuf, usize> = HashMap::with_capacity(batch.count);
        let mut store_retracted: HashSet<PkBuf> = HashSet::with_capacity(batch.count);

        for row in 0..batch.count {
            let w = batch.get_weight(row);
            let pkb = batch.get_pk_bytes(row);

            if w > 0 {
                let (_existing_w, found) = ptable.retract_pk_bytes(pkb);
                if found && !store_retracted.contains(pkb) {
                    effective.append_row_from_ptable_found(ptable, pkb, -1);
                    store_retracted.insert(PkBuf::from_bytes(pkb));
                }

                if let Some(&prev_pos) = seen.get(pkb) {
                    effective.append_batch_negated(&batch, prev_pos, prev_pos + 1);
                }

                effective.append_batch(&batch, row, row + 1);
                seen.insert(PkBuf::from_bytes(pkb), row);
            } else if w < 0 {
                let (_existing_w, found) = ptable.retract_pk_bytes(pkb);
                let store_hit = found && !store_retracted.contains(pkb);
                if store_hit {
                    effective.append_row_from_ptable_found(ptable, pkb, -1);
                    store_retracted.insert(PkBuf::from_bytes(pkb));
                }

                if let Some(&prev_pos) = seen.get(pkb) {
                    effective.append_batch_negated(&batch, prev_pos, prev_pos + 1);
                    seen.remove(pkb);
                } else if !found {
                    effective.append_batch(&batch, row, row + 1);
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
        source_col_idx: u32,
        src_schema: &SchemaDescriptor,
        idx_schema: &SchemaDescriptor,
    ) -> Batch {
        let source_col = source_col_idx as usize;
        let is_pk_col = src_schema.is_pk_col(source_col);
        let src_payload_idx = if is_pk_col {
            usize::MAX
        } else {
            src_schema.payload_idx(source_col)
        };
        let src_col_size = src_schema.columns[source_col].size() as usize;
        let src_pk_stride = src_schema.pk_stride() as usize;
        let idx_stride = idx_schema.pk_stride() as usize;
        let idx_key_size = idx_schema.columns[0].size() as usize;
        // If the indexed value is in the PK, find its byte offset once.
        let src_pk_field_off = if is_pk_col {
            src_schema.pk_byte_offset(source_col) as usize
        } else { 0 };

        let mut out = Batch::with_schema(*idx_schema, src.count.max(1));
        // `vec![0u8; idx_stride]` zero-fills once. The leading `[..src_col_size]`
        // and trailing `[idx_key_size..]` slots are fully overwritten per row;
        // the middle `[src_col_size..idx_key_size]` zero-pad stays zero across
        // rows, so no per-row re-zero is needed.
        let mut idx_pk_buf = vec![0u8; idx_stride];

        for row in 0..src.count {
            let weight = src.get_weight(row);
            if weight == 0 { continue; }

            if !is_pk_col {
                let null_word = src.get_null_word(row);
                let payload_bit = src_schema.payload_idx(source_col);
                if (null_word >> payload_bit) & 1 != 0 {
                    continue;
                }
            }

            let src_pk_bytes = src.get_pk_bytes(row);
            // Leading index-key slot: indexed value LE bytes.
            if is_pk_col {
                idx_pk_buf[..src_col_size]
                    .copy_from_slice(&src_pk_bytes[src_pk_field_off..src_pk_field_off + src_col_size]);
            } else {
                let col_data = src.col_data(src_payload_idx);
                let offset = row * src_col_size;
                idx_pk_buf[..src_col_size]
                    .copy_from_slice(&col_data[offset..offset + src_col_size]);
            }

            // The source PK region is laid out in pk_indices order, so the
            // index's trailing PK suffix is byte-identical to the source's PK.
            idx_pk_buf[idx_key_size..idx_key_size + src_pk_stride]
                .copy_from_slice(src_pk_bytes);

            out.extend_pk_bytes(&idx_pk_buf);
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

    /// Close the DagEngine, dropping all cached plans.
    pub fn close(&mut self) {
        self.cache.clear();
        self.tables.clear();
        self.shard_cols_cache.clear();
        self.join_shard_cols_cache.clear();
        self.exchange_info_cache.clear();
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
            .join(format!("gnitz_dag_test_{}", name))
            .to_str().unwrap().to_owned()
    }

    fn make_test_table(name: &str) -> Box<Table> {
        let schema = SchemaDescriptor::default();
        let dir = dag_test_dir(name);
        let _ = std::fs::remove_dir_all(&dir);
        Box::new(Table::new(&dir, name, schema, 99, 256 * 1024, false).unwrap())
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
        let tbl = make_test_table("reg_unreg");
        dag.register_table(100, StoreHandle::Single(tbl), schema, 0, false, String::new());
        assert!(dag.tables.contains_key(&100));

        dag.unregister_table(100);
        assert!(!dag.tables.contains_key(&100));
        let _ = std::fs::remove_dir_all(dag_test_dir("reg_unreg"));
    }

    #[test]
    fn test_invalidation() {
        let mut dag = DagEngine::new();
        dag.shard_cols_cache.insert(42, vec![0]);
        dag.exchange_info_cache.insert(42, ExchangeInfo {
            is_trivial: true,
            is_co_partitioned: false,
        });
        dag.dep_map_valid = true;

        dag.invalidate(42);
        assert!(!dag.shard_cols_cache.contains_key(&42));
        assert!(!dag.exchange_info_cache.contains_key(&42));
        assert!(!dag.dep_map_valid);

        dag.dep_map_valid = true;
        dag.invalidate_dep_map();
        assert!(!dag.dep_map_valid);

        dag.shard_cols_cache.insert(99, vec![1]);
        dag.invalidate_all();
        assert!(dag.shard_cols_cache.is_empty());
    }

    #[test]
    fn test_add_remove_index_circuit() {
        let mut dag = DagEngine::new();
        let schema = SchemaDescriptor::default();
        let tbl = make_test_table("idx_parent");
        dag.register_table(50, StoreHandle::Single(tbl), schema, 0, false, String::new());
        let idx_tbl = make_test_table("idx_child");
        dag.add_index_circuit(50, 2, idx_tbl, schema, false);
        assert_eq!(dag.tables[&50].index_circuits.len(), 1);

        dag.remove_index_circuit(50, 2);
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
        let tbl = make_test_table("flush_ic_parent");
        dag.register_table(70, StoreHandle::Single(tbl), parent_schema, 0, false, String::new());

        // Durable index table: flush writes shard_*.db only if called.
        let idx_schema = crate::schema::SchemaDescriptor::minimal_u64();
        let idx_dir = dag_test_dir("flush_ic_idx");
        let _ = std::fs::remove_dir_all(&idx_dir);
        let idx_tbl = Box::new(
            Table::new(&idx_dir, "flush_ic_idx", idx_schema, 1, 256 * 1024, true).unwrap(),
        );
        dag.add_index_circuit(70, 1, idx_tbl, idx_schema, false);

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
}
