//! DagEngine: consolidated plan cache, DAG evaluator, and ingestion pipeline.

use std::collections::{HashMap, HashSet};

use crate::schema::SchemaDescriptor;
use crate::compiler::{self, CompileOutput, ExternalTable};
use crate::storage::{Batch, CursorHandle, Table, PartitionedTable};
use crate::ops;
use crate::vm;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

// Opcodes needed for metadata queries
const OPCODE_EXCHANGE_SHARD: i32 = 20;
const OPCODE_INTEGRATE: i32 = 7;
const OPCODE_SCAN_TRACE: i32 = 11;

const PARAM_SHARD_COL_BASE: i32 = 128;
const PARAM_JOIN_SOURCE_TABLE: i32 = 11;
const PARAM_REINDEX_COL: i32 = 10;

// ---------------------------------------------------------------------------
// Store handle — views vs base tables
// ---------------------------------------------------------------------------

/// Distinguishes ephemeral (view) stores from partitioned (base table) stores.
/// Owned variants (Single, Partitioned) drop the underlying storage on Drop.
/// The Borrowed variant holds a non-owning pointer for system tables owned
/// by CatalogEngine directly.
pub enum StoreHandle {
    /// Owned single Table — used by views in single-worker mode.
    Single(Box<Table>),
    /// Owned PartitionedTable — used by base tables and views.
    Partitioned(Box<PartitionedTable>),
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
            StoreHandle::Partitioned(pt) => std::ptr::addr_of!(**pt) as *mut PartitionedTable,
            _ => std::ptr::null_mut(),
        }
    }
}

// ---------------------------------------------------------------------------
// Index circuit entry
// ---------------------------------------------------------------------------

/// A secondary index on a column.
/// Owns the index Table via Box — dropping the entry drops the table.
pub struct IndexCircuitEntry {
    pub col_idx: u32,
    pub index_table: Box<Table>,
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

/// Handles + schemas for the 6 circuit system tables.
pub struct SysTableRefs {
    // Table handles (DagEngine borrows them).
    pub nodes: *mut Table,
    pub edges: *mut Table,
    pub sources: *mut Table,
    pub params: *mut Table,
    pub group_cols: *mut Table,
    pub dep_tab: *mut Table,
    // Schemas
    pub nodes_schema: SchemaDescriptor,
    pub edges_schema: SchemaDescriptor,
    pub sources_schema: SchemaDescriptor,
    pub params_schema: SchemaDescriptor,
    pub group_cols_schema: SchemaDescriptor,
    pub dep_tab_schema: SchemaDescriptor,
}

// SAFETY: same single-thread guarantee.
unsafe impl Send for SysTableRefs {}

impl SysTableRefs {
    fn null() -> Self {
        SysTableRefs {
            nodes: std::ptr::null_mut(),
            edges: std::ptr::null_mut(),
            sources: std::ptr::null_mut(),
            params: std::ptr::null_mut(),
            group_cols: std::ptr::null_mut(),
            dep_tab: std::ptr::null_mut(),
            nodes_schema: crate::compiler::empty_schema(),
            edges_schema: crate::compiler::empty_schema(),
            sources_schema: crate::compiler::empty_schema(),
            params_schema: crate::compiler::empty_schema(),
            group_cols_schema: crate::compiler::empty_schema(),
            dep_tab_schema: crate::compiler::empty_schema(),
        }
    }
}

// ---------------------------------------------------------------------------
// Exchange info
// ---------------------------------------------------------------------------

/// Cached exchange metadata for a view.
pub struct ExchangeInfo {
    pub shard_cols: Vec<i32>,
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
    cache: HashMap<i64, CachedPlan>,
    dep_map: HashMap<i64, Vec<i64>>,       // source_table_id → [view_ids]
    source_map: HashMap<i64, Vec<i64>>,    // view_id → [source_table_ids]
    dep_map_valid: bool,
    shard_cols_cache: HashMap<i64, Vec<i32>>,
    exchange_info_cache: HashMap<i64, ExchangeInfo>,
    pub(crate) tables: HashMap<i64, TableEntry>,
    sys: SysTableRefs,
}

// SAFETY: DagEngine is only accessed from a single thread.
unsafe impl Send for DagEngine {}

impl DagEngine {
    pub fn new() -> Self {
        DagEngine {
            cache: HashMap::new(),
            dep_map: HashMap::new(),
            source_map: HashMap::new(),
            dep_map_valid: false,
            shard_cols_cache: HashMap::new(),
            exchange_info_cache: HashMap::new(),
            tables: HashMap::new(),
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
        self.exchange_info_cache.remove(&table_id);
        self.dep_map_valid = false;
    }

    pub fn set_depth(&mut self, table_id: i64, depth: i32) {
        if let Some(entry) = self.tables.get_mut(&table_id) {
            entry.depth = depth;
        }
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
                index_table,
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

    // ── Cache management ────────────────────────────────────────────────

    pub fn invalidate(&mut self, view_id: i64) {
        self.cache.remove(&view_id);
        self.shard_cols_cache.remove(&view_id);
        self.exchange_info_cache.remove(&view_id);
        self.dep_map_valid = false;
    }

    pub fn invalidate_all(&mut self) {
        self.cache.clear();
        self.shard_cols_cache.clear();
        self.exchange_info_cache.clear();
        self.dep_map_valid = false;
    }

    pub fn invalidate_dep_map(&mut self) {
        self.dep_map_valid = false;
    }

    // ── Dependency map ──────────────────────────────────────────────────

    /// Rebuild the dep_map and source_map from the DepTab system table.
    /// Port of `ProgramCache.get_dep_map()`.
    pub fn get_dep_map(&mut self) -> &HashMap<i64, Vec<i64>> {
        if self.dep_map_valid {
            return &self.dep_map;
        }
        self.dep_map.clear();
        self.source_map.clear();

        if !self.sys.dep_tab.is_null() {
            let t = unsafe { &mut *self.sys.dep_tab };
            if let Ok(mut ch) = t.create_cursor() {
                while ch.cursor.valid {
                    let w = ch.cursor.current_weight;
                    if w > 0 {
                        // DepTab PK = view_id, dep_table_id is a payload column.
                        let v_id = ch.cursor.current_key_hi as i64;
                        let dep_tid = compiler::cursor_read_i64(
                            &ch.cursor, 3, &self.sys.dep_tab_schema,
                        );
                        if dep_tid > 0 {
                            let views = self.dep_map.entry(dep_tid).or_insert_with(Vec::new);
                            if !views.contains(&v_id) {
                                views.push(v_id);
                            }
                            let sources = self.source_map.entry(v_id).or_insert_with(Vec::new);
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

    /// Load circuit nodes from the CircuitNodes system table for a view.
    /// Returns [(node_id, opcode)].
    fn load_nodes_meta(&self, view_id: u64) -> Vec<(i32, i32)> {
        let mut result = Vec::new();
        if self.sys.nodes.is_null() { return result; }
        let t = unsafe { &mut *self.sys.nodes };
        let mut ch = match t.create_cursor() {
            Ok(c) => c,
            Err(_) => return result,
        };
        ch.cursor.seek(crate::util::make_pk(0, view_id));
        while ch.cursor.valid && ch.cursor.current_key_hi == view_id {
            if ch.cursor.current_weight > 0 {
                let node_id = ch.cursor.current_key_lo as i32;
                let opcode = compiler::cursor_read_i64(
                    &ch.cursor, 1, &self.sys.nodes_schema,
                ) as i32;
                result.push((node_id, opcode));
            }
            ch.cursor.advance();
        }
        result
    }

    /// Load circuit edges for a view. Returns [(edge_id, src, dst, port)].
    fn load_edges_meta(&self, view_id: u64) -> Vec<(i32, i32, i32, i32)> {
        let mut result = Vec::new();
        if self.sys.edges.is_null() { return result; }
        let t = unsafe { &mut *self.sys.edges };
        let mut ch = match t.create_cursor() {
            Ok(c) => c,
            Err(_) => return result,
        };
        ch.cursor.seek(crate::util::make_pk(0, view_id));
        while ch.cursor.valid && ch.cursor.current_key_hi == view_id {
            if ch.cursor.current_weight > 0 {
                let edge_id = ch.cursor.current_key_lo as i32;
                let src = compiler::cursor_read_i64(
                    &ch.cursor, 1, &self.sys.edges_schema,
                ) as i32;
                let dst = compiler::cursor_read_i64(
                    &ch.cursor, 2, &self.sys.edges_schema,
                ) as i32;
                let port = compiler::cursor_read_i64(
                    &ch.cursor, 3, &self.sys.edges_schema,
                ) as i32;
                result.push((edge_id, src, dst, port));
            }
            ch.cursor.advance();
        }
        result
    }

    /// Load circuit params for a view. Returns {node_id: {slot: value}}.
    fn load_params_meta(&self, view_id: u64) -> HashMap<i32, HashMap<i32, i64>> {
        let mut result: HashMap<i32, HashMap<i32, i64>> = HashMap::new();
        if self.sys.params.is_null() { return result; }
        let t = unsafe { &mut *self.sys.params };
        let mut ch = match t.create_cursor() {
            Ok(c) => c,
            Err(_) => return result,
        };
        ch.cursor.seek(crate::util::make_pk(0, view_id));
        while ch.cursor.valid && ch.cursor.current_key_hi == view_id {
            if ch.cursor.current_weight > 0 {
                let lo64 = ch.cursor.current_key_lo;
                let node_id = (lo64 >> 8) as i32;
                let slot = (lo64 & 0xFF) as i32;
                let value = compiler::cursor_read_i64(
                    &ch.cursor, 1, &self.sys.params_schema,
                );
                result.entry(node_id).or_insert_with(HashMap::new).insert(slot, value);
            }
            ch.cursor.advance();
        }
        result
    }

    /// Load circuit sources for a view. Returns {node_id: table_id}.
    fn load_sources_meta(&self, view_id: u64) -> HashMap<i32, i64> {
        let mut result = HashMap::new();
        if self.sys.sources.is_null() { return result; }
        let t = unsafe { &mut *self.sys.sources };
        let mut ch = match t.create_cursor() {
            Ok(c) => c,
            Err(_) => return result,
        };
        ch.cursor.seek(crate::util::make_pk(0, view_id));
        while ch.cursor.valid && ch.cursor.current_key_hi == view_id {
            if ch.cursor.current_weight > 0 {
                let node_id = ch.cursor.current_key_lo as i32;
                let table_id = compiler::cursor_read_i64(
                    &ch.cursor, 1, &self.sys.sources_schema,
                );
                result.insert(node_id, table_id);
            }
            ch.cursor.advance();
        }
        result
    }

    /// Extract shard columns for a view without full compilation.
    /// Port of `ProgramCache.get_shard_cols()`.
    pub fn get_shard_cols(&mut self, view_id: i64) -> Vec<i32> {
        if let Some(cols) = self.shard_cols_cache.get(&view_id) {
            return cols.clone();
        }
        let nodes = self.load_nodes_meta(view_id as u64);
        let params = self.load_params_meta(view_id as u64);
        for &(nid, op) in &nodes {
            if op == OPCODE_EXCHANGE_SHARD {
                let node_params = params.get(&nid);
                let mut shard_cols = Vec::new();
                let mut idx = 0;
                loop {
                    let key = PARAM_SHARD_COL_BASE + idx;
                    match node_params.and_then(|p| p.get(&key)) {
                        Some(&val) => {
                            shard_cols.push(val as i32);
                            idx += 1;
                        }
                        None => break,
                    }
                }
                self.shard_cols_cache.insert(view_id, shard_cols.clone());
                return shard_cols;
            }
        }
        self.shard_cols_cache.insert(view_id, Vec::new());
        Vec::new()
    }

    /// Get join shard columns for a specific source within a view.
    /// Port of `ProgramCache.get_join_shard_cols()`.
    pub fn get_join_shard_cols(&mut self, view_id: i64, source_id: i64) -> Vec<i32> {
        let nodes = self.load_nodes_meta(view_id as u64);
        let params = self.load_params_meta(view_id as u64);
        let edges = self.load_edges_meta(view_id as u64);

        // Find SCAN_TRACE node for this source_id
        let mut scan_nid: i32 = -1;
        for &(nid, op) in &nodes {
            if op == OPCODE_SCAN_TRACE {
                if let Some(np) = params.get(&nid) {
                    if np.get(&PARAM_JOIN_SOURCE_TABLE).copied().unwrap_or(0) == source_id {
                        scan_nid = nid;
                        break;
                    }
                }
            }
        }
        if scan_nid < 0 {
            return Vec::new();
        }

        // Find MAP node that this SCAN_TRACE feeds into
        for &(_eid, src, dst, _port) in &edges {
            if src == scan_nid {
                if let Some(dst_params) = params.get(&dst) {
                    if let Some(&reindex_col) = dst_params.get(&PARAM_REINDEX_COL) {
                        if reindex_col >= 0 {
                            return vec![reindex_col as i32];
                        }
                    }
                }
            }
        }
        Vec::new()
    }

    /// Get exchange info for a view: (shard_cols, is_trivial, is_co_partitioned).
    /// Port of `ProgramCache.get_exchange_info()`.
    pub fn get_exchange_info(&mut self, view_id: i64) -> &ExchangeInfo {
        if self.exchange_info_cache.contains_key(&view_id) {
            return &self.exchange_info_cache[&view_id];
        }

        let nodes = self.load_nodes_meta(view_id as u64);
        let edges = self.load_edges_meta(view_id as u64);
        let params = self.load_params_meta(view_id as u64);
        let sources = self.load_sources_meta(view_id as u64);

        // Compute in-degrees
        let mut in_deg: HashMap<i32, i32> = HashMap::new();
        for &(nid, _) in &nodes {
            in_deg.insert(nid, 0);
        }
        for &(_eid, _src, dst, _port) in &edges {
            *in_deg.entry(dst).or_insert(0) += 1;
        }

        let mut shard_cols = Vec::new();
        let mut is_trivial = false;
        let mut is_co_partitioned = false;

        for &(nid, op) in &nodes {
            if op == OPCODE_EXCHANGE_SHARD {
                if let Some(node_params) = params.get(&nid) {
                    let mut idx = 0;
                    loop {
                        let key = PARAM_SHARD_COL_BASE + idx;
                        match node_params.get(&key) {
                            Some(&val) => {
                                shard_cols.push(val as i32);
                                idx += 1;
                            }
                            None => break,
                        }
                    }
                }

                // is_trivial: SHARD has exactly 1 incoming edge, and that source
                // node has in-degree 0
                let mut incoming_srcs = Vec::new();
                for &(_eid2, src2, dst2, _port2) in &edges {
                    if dst2 == nid {
                        incoming_srcs.push(src2);
                    }
                }
                if incoming_srcs.len() == 1 {
                    let src_nid = incoming_srcs[0];
                    if in_deg.get(&src_nid).copied().unwrap_or(0) == 0 {
                        is_trivial = true;
                        let src_tid = sources.get(&src_nid).copied().unwrap_or(0);
                        if src_tid > 0 && shard_cols.len() == 1 {
                            if let Some(entry) = self.tables.get(&src_tid) {
                                if shard_cols[0] == entry.schema.pk_index as i32 {
                                    is_co_partitioned = true;
                                }
                            }
                        }
                    }
                }
                break; // only one EXCHANGE_SHARD per view
            }
        }

        // Keep shard_cols_cache coherent
        self.shard_cols_cache.insert(view_id, shard_cols.clone());

        self.exchange_info_cache.insert(view_id, ExchangeInfo {
            shard_cols,
            is_trivial,
            is_co_partitioned,
        });
        &self.exchange_info_cache[&view_id]
    }

    /// Return [(view_id, shard_cols)] for trivial, non-co-partitioned views
    /// that depend on source_table_id.
    /// Port of `ProgramCache.get_preloadable_views()`.
    pub fn get_preloadable_views(&mut self, source_table_id: i64) -> Vec<(i64, Vec<i32>)> {
        self.get_dep_map();
        let view_ids: Vec<i64> = self.dep_map
            .get(&source_table_id)
            .cloned()
            .unwrap_or_default();
        let mut result = Vec::new();
        for view_id in view_ids {
            let info = self.get_exchange_info(view_id);
            if info.is_trivial && !info.is_co_partitioned && !info.shard_cols.is_empty() {
                result.push((view_id, info.shard_cols.clone()));
            }
        }
        result
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
        let nodes = self.load_nodes_meta(view_id as u64);
        for &(_, op) in &nodes {
            if op == OPCODE_EXCHANGE_SHARD {
                return true;
            }
        }
        false
    }

    /// Validate circuit graph structure before persistence.
    /// Port of `ProgramCache.validate_graph_structure()`.
    ///
    /// Returns Ok(()) on success, Err(msg) on validation failure.
    pub fn validate_graph_structure(
        &self,
        nodes: &[(i32, i32)],
        edges: &[(i32, i32, i32, i32)],
        sources: &[(i32, i64)],
    ) -> Result<(), &'static str> {
        if nodes.is_empty() {
            return Err("View graph contains no nodes");
        }

        // Check for primary input (source with table_id=0)
        let has_input = sources.iter().any(|&(_, tid)| tid == 0);
        if !has_input {
            return Err("View graph missing primary input (table_id=0)");
        }

        // Check for sink (INTEGRATE node)
        let has_sink = nodes.iter().any(|&(_, op)| op == OPCODE_INTEGRATE);
        if !has_sink {
            return Err("View graph missing sink (INTEGRATE node)");
        }

        // Cycle detection via Kahn's algorithm
        let mut in_degree: HashMap<i32, i32> = HashMap::new();
        for &(nid, _) in nodes {
            in_degree.insert(nid, 0);
        }
        for &(_eid, _src, dst, _port) in edges {
            *in_degree.entry(dst).or_insert(0) += 1;
        }

        let mut queue: Vec<i32> = Vec::new();
        for &(nid, _) in nodes {
            if in_degree[&nid] == 0 {
                queue.push(nid);
            }
        }

        let mut count = 0;
        while let Some(nid) = queue.pop() {
            count += 1;
            for &(_eid, src, dst, _port) in edges {
                if src == nid {
                    let d = in_degree.get_mut(&dst).unwrap();
                    *d -= 1;
                    if *d == 0 {
                        queue.push(dst);
                    }
                }
            }
        }

        if count != nodes.len() {
            return Err("View graph contains cycles (not a DAG)");
        }

        Ok(())
    }

    // ── Compilation ─────────────────────────────────────────────────────

    /// Get or compile a plan for the given view.
    /// Port of `ProgramCache.get_program()`.
    pub fn get_program(&mut self, view_id: i64) -> Option<&CachedPlan> {
        if self.cache.contains_key(&view_id) {
            return self.cache.get(&view_id);
        }
        match self.compile_view_internal(view_id) {
            Some(plan) => {
                self.cache.insert(view_id, plan);
                self.cache.get(&view_id)
            }
            None => None,
        }
    }

    /// Compile a view by reading system tables and calling `compiler::compile_view`.
    fn compile_view_internal(&self, view_id: i64) -> Option<CachedPlan> {
        let entry = self.tables.get(&view_id)?;
        let view_schema = &entry.schema;
        let view_dir = entry.directory.clone();

        // Build external tables array from registered tables
        let ext_tables: Vec<ExternalTable> = self.tables.iter()
            .map(|(&tid, te)| ExternalTable {
                table_id: tid,
                handle: te.handle.table_ptr(),
                schema: te.schema,
            })
            .collect();

        let result = unsafe {
            compiler::compile_view(
                view_id as u64,
                self.sys.nodes, self.sys.edges, self.sys.sources,
                self.sys.params, self.sys.group_cols, self.sys.dep_tab,
                &self.sys.nodes_schema, &self.sys.edges_schema,
                &self.sys.sources_schema, &self.sys.params_schema,
                &self.sys.group_cols_schema, &self.sys.dep_tab_schema,
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

    /// Execute just the post-plan of a view (after exchange IPC).
    /// Used by multi-worker path.
    pub fn execute_post_epoch(
        &mut self,
        view_id: i64,
        input: Batch,
    ) -> Option<Batch> {
        if !self.cache.contains_key(&view_id) {
            if let Some(plan) = self.compile_view_internal(view_id) {
                self.cache.insert(view_id, plan);
            } else {
                return None;
            }
        }
        self.execute_plan_phase(view_id, false, input, 0)
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

        // Stage 2: ingest into store
        let npc = batch.num_payload_cols();
        let (ptrs, sizes) = batch.to_region_ptrs();
        let entry = self.tables.get_mut(&table_id).unwrap();
        match &mut entry.handle {
            StoreHandle::Single(t) => {
                let _ = t.ingest_batch_from_regions(&ptrs, &sizes, batch.count as u32, npc);
            }
            StoreHandle::Borrowed(ptr) => {
                let table = unsafe { &mut **ptr };
                let _ = table.ingest_batch_from_regions(&ptrs, &sizes, batch.count as u32, npc);
            }
            StoreHandle::Partitioned(pt) => {
                let _ = pt.ingest_batch_from_regions(&ptrs, &sizes, batch.count as u32, npc);
            }
        }

        // Stage 3: index projection
        for ic in &mut entry.index_circuits {
            let idx_batch = Self::batch_project_index(
                batch, ic.col_idx, &schema, &ic.index_schema,
            );
            if idx_batch.count > 0 {
                let idx_npc = idx_batch.num_payload_cols();
                let (idx_ptrs, idx_sizes) = idx_batch.to_region_ptrs();
                let _ = ic.index_table.ingest_batch_from_regions(
                    &idx_ptrs, &idx_sizes, idx_batch.count as u32, idx_npc,
                );
            }
        }

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

        // Stage 2: ingest into store
        let npc = effective_batch.num_payload_cols();
        let (ptrs, sizes) = effective_batch.to_region_ptrs();
        match &mut entry.handle {
            StoreHandle::Single(t) => {
                let _ = t.ingest_batch_from_regions(&ptrs, &sizes, effective_batch.count as u32, npc);
            }
            StoreHandle::Borrowed(ptr) => {
                let table = unsafe { &mut **ptr };
                let _ = table.ingest_batch_from_regions(&ptrs, &sizes, effective_batch.count as u32, npc);
            }
            StoreHandle::Partitioned(pt) => {
                let _ = pt.ingest_batch_from_regions(&ptrs, &sizes, effective_batch.count as u32, npc);
            }
        }

        // Stage 3: index projection
        for ic in &mut entry.index_circuits {
            let idx_batch = Self::batch_project_index(
                &effective_batch, ic.col_idx, &schema, &ic.index_schema,
            );
            if idx_batch.count > 0 {
                let idx_npc = idx_batch.num_payload_cols();
                let (idx_ptrs, idx_sizes) = idx_batch.to_region_ptrs();
                let _ = ic.index_table.ingest_batch_from_regions(
                    &idx_ptrs, &idx_sizes, idx_batch.count as u32, idx_npc,
                );
            }
        }

        (0, Some(effective_batch))
    }

    /// Flush a table's WAL.
    pub fn flush(&mut self, table_id: i64) -> i32 {
        let entry = match self.tables.get_mut(&table_id) {
            Some(e) => e,
            None => return -1,
        };
        match &mut entry.handle {
            StoreHandle::Single(t) => match t.flush() { Ok(_) => 0, Err(e) => e },
            StoreHandle::Borrowed(ptr) => match unsafe { &mut **ptr }.flush() { Ok(_) => 0, Err(e) => e },
            StoreHandle::Partitioned(pt) => match pt.flush() { Ok(_) => 0, Err(e) => e },
        }
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

        // 2. Build initial pending list: (depth, view_id, source_id, batch)
        struct PendingEntry {
            depth: i32,
            view_id: i64,
            source_id: i64,
            batch: Batch,
        }

        let mut pending: Vec<PendingEntry> = Vec::new();
        // pending_pos: (view_id, source_id) → index in pending
        let mut pending_pos: HashMap<(i64, i64), usize> = HashMap::new();
        let mut dirty_views: HashSet<i64> = HashSet::new();

        for &vid in &view_ids {
            let depth = match self.tables.get(&vid) {
                Some(e) => e.depth,
                None => continue, // lagging dep_map
            };
            pending.push(PendingEntry {
                depth,
                view_id: vid,
                source_id,
                batch: delta.clone_batch(),
            });
        }
        crate::storage::batch_pool::recycle(delta);

        // Sort descending by depth (shallowest at tail for O(1) pop)
        pending.sort_by(|a, b| b.depth.cmp(&a.depth));
        // Rebuild pending_pos
        for (i, pe) in pending.iter().enumerate() {
            pending_pos.insert((pe.view_id, pe.source_id), i);
        }

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

            // Ingest into view store (borrow — no clone)
            self.ingest_by_ref(view_id, &out_delta);
            dirty_views.insert(view_id);

            // Queue dependents
            let dep_view_ids: Vec<i64> = self.dep_map
                .get(&view_id)
                .cloned()
                .unwrap_or_default();

            for dep_id in dep_view_ids {
                let (dep_depth, dep_schema) = match self.tables.get(&dep_id) {
                    Some(e) => (e.depth, e.schema),
                    None => continue,
                };

                if let Some(&existing_idx) = pending_pos.get(&(dep_id, view_id)) {
                    if existing_idx < pending.len() {
                        let schema = pending[existing_idx].batch.schema.unwrap_or(dep_schema);
                        let merged = ops::op_union(
                            &pending[existing_idx].batch,
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
                    pending.sort_by(|a, b| b.depth.cmp(&a.depth));
                    // Rebuild pending_pos
                    pending_pos.clear();
                    for (i, pe) in pending.iter().enumerate() {
                        pending_pos.insert((pe.view_id, pe.source_id), i);
                    }
                }
            }
            crate::storage::batch_pool::recycle(out_delta);
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

        struct PendingEntry {
            depth: i32,
            view_id: i64,
            source_id: i64,
            batch: Batch,
        }

        let mut pending: Vec<PendingEntry> = Vec::new();
        let mut pending_pos: HashMap<(i64, i64), usize> = HashMap::new();
        let mut dirty_views: HashSet<i64> = HashSet::new();

        for &vid in &view_ids {
            let depth = match self.tables.get(&vid) {
                Some(e) => e.depth,
                None => continue,
            };
            pending.push(PendingEntry {
                depth,
                view_id: vid,
                source_id,
                batch: delta.clone_batch(),
            });
        }
        crate::storage::batch_pool::recycle(delta);

        pending.sort_by(|a, b| b.depth.cmp(&a.depth));
        for (i, pe) in pending.iter().enumerate() {
            pending_pos.insert((pe.view_id, pe.source_id), i);
        }

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
                        batch.schema = Some(exchange_schema);
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
                            input_with_schema.schema = Some(entry.schema);
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
            if has_output {
                let out = out_delta.as_ref().unwrap();
                self.ingest_by_ref(view_id, out);
                dirty_views.insert(view_id);
            }

            // Multi-worker: ALWAYS queue dependents (even empty output)
            let dep_view_ids: Vec<i64> = self.dep_map
                .get(&view_id)
                .cloned()
                .unwrap_or_default();

            for dep_id in dep_view_ids {
                let (dep_depth, dep_schema) = match self.tables.get(&dep_id) {
                    Some(e) => (e.depth, e.schema),
                    None => continue,
                };

                if let Some(&existing_idx) = pending_pos.get(&(dep_id, view_id)) {
                    if existing_idx < pending.len() && has_output {
                        let schema = pending[existing_idx].batch.schema.unwrap_or(dep_schema);
                        let merged = ops::op_union(
                            &pending[existing_idx].batch,
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
                    pending.sort_by(|a, b| b.depth.cmp(&a.depth));
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
    fn build_ext_cursors(
        &self,
        ext_trace_regs: &[(u16, i64)],
        num_regs: usize,
    ) -> (Vec<*mut libc::c_void>, Vec<Box<CursorHandle<'static>>>) {
        let mut ptrs: Vec<*mut libc::c_void> = vec![std::ptr::null_mut(); num_regs];
        let mut storage: Vec<Box<CursorHandle<'static>>> = Vec::new();
        for &(reg_id, table_id) in ext_trace_regs {
            if let Some(entry) = self.tables.get(&table_id) {
                let tbl_ptr = entry.handle.table_ptr();
                let ptbl_ptr = entry.handle.ptable_ptr();
                let cursor_opt = if !tbl_ptr.is_null() {
                    let tbl = unsafe { &mut *tbl_ptr };
                    let _ = tbl.compact_if_needed();
                    tbl.create_cursor().ok()
                } else if !ptbl_ptr.is_null() {
                    unsafe { &mut *ptbl_ptr }.create_cursor().ok()
                } else {
                    None
                };
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

        let mut effective = Batch::with_schema(*schema, batch.count * 2);
        // Track seen PKs for intra-batch dedup: pk u128 → last position
        let mut seen: HashMap<u128, usize> = HashMap::new();

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
                seen.insert(pk, effective.count - 1);
            } else if w < 0 {
                // Negative weight = deletion
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

        let mut effective = Batch::with_schema(*schema, batch.count * 2);
        let mut seen: HashMap<u128, usize> = HashMap::new();

        for row in 0..batch.count {
            let w = batch.get_weight(row);
            let pk = batch.get_pk(row);

            if w > 0 {
                // Check store for existing row with this PK
                let (_existing_w, found) = ptable.retract_pk(pk);
                if found {
                    // Emit retraction of existing stored row so downstream views
                    // see the removal of the old payload.
                    effective.append_row_from_ptable_found(ptable, pk, -1);
                }

                // Intra-batch dedup: retract previous insertion of same PK
                if let Some(&prev_pos) = seen.get(&pk) {
                    effective.append_batch_negated(&batch, prev_pos, prev_pos + 1);
                }

                // Add the new insertion
                effective.append_batch(&batch, row, row + 1);
                seen.insert(pk, effective.count - 1);
            } else if w < 0 {
                // Negative weight = explicit retraction
                let (_existing_w, found) = ptable.retract_pk(pk);
                if found {
                    // Emit retraction using stored payload (the actual column data)
                    effective.append_row_from_ptable_found(ptable, pk, -1);
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

    /// Batch-level index projection.
    pub(crate) fn batch_project_index(
        src: &Batch,
        source_col_idx: u32,
        src_schema: &SchemaDescriptor,
        idx_schema: &SchemaDescriptor,
    ) -> Batch {
        let src_pk_index = src_schema.pk_index as usize;
        let source_col = source_col_idx as usize;
        let is_pk_col = source_col == src_pk_index;

        let src_payload_idx = if is_pk_col {
            usize::MAX
        } else if source_col < src_pk_index {
            source_col
        } else {
            source_col - 1
        };

        let src_col_size = src_schema.columns[source_col].size as usize;
        let out_payload_col_idx = if idx_schema.pk_index == 0 { 1usize } else { 0usize };
        let out_payload_size = idx_schema.columns[out_payload_col_idx].size as usize;

        let mut out = Batch::with_schema(*idx_schema, src.count.max(1));

        for row in 0..src.count {
            let weight = src.get_weight(row);
            if weight == 0 { continue; }

            // Null check
            if !is_pk_col {
                let null_word = src.get_null_word(row);
                let payload_bit = if source_col < src_pk_index { source_col } else { source_col - 1 };
                if (null_word >> payload_bit) & 1 != 0 {
                    continue; // NULL column → skip
                }
            }

            // Read source column value as index key
            let (key_lo, key_hi) = if is_pk_col {
                crate::util::split_pk(src.get_pk(row))
            } else {
                let col_data = src.col_data(src_payload_idx);
                let offset = row * src_col_size;
                if src_col_size == 8 {
                    let v = u64::from_le_bytes(col_data[offset..offset+8].try_into().unwrap());
                    (v, 0u64)
                } else if src_col_size == 16 {
                    let lo = u64::from_le_bytes(col_data[offset..offset+8].try_into().unwrap());
                    let hi = u64::from_le_bytes(col_data[offset+8..offset+16].try_into().unwrap());
                    (lo, hi)
                } else if src_col_size == 4 {
                    let v = u32::from_le_bytes(col_data[offset..offset+4].try_into().unwrap());
                    (v as u64, 0u64)
                } else if src_col_size == 2 {
                    let v = u16::from_le_bytes(col_data[offset..offset+2].try_into().unwrap());
                    (v as u64, 0u64)
                } else if src_col_size == 1 {
                    (col_data[offset] as u64, 0u64)
                } else {
                    continue;
                }
            };

            // Payload = source row's PK
            let src_pk = src.get_pk(row);
            let (src_pk_lo, src_pk_hi) = crate::util::split_pk(src_pk);

            out.extend_pk_lo(&key_lo.to_le_bytes());
            out.extend_pk_hi(&key_hi.to_le_bytes());
            out.extend_weight(&weight.to_le_bytes());
            out.extend_null_bmp(&0u64.to_le_bytes());

            // Write payload column (source PK mapped to index payload)
            if out_payload_size == 16 {
                out.extend_col(0, &src_pk_lo.to_le_bytes());
                out.extend_col(0, &src_pk_hi.to_le_bytes());
            } else if out_payload_size == 8 {
                out.extend_col(0, &src_pk_lo.to_le_bytes());
            }
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

    fn make_test_table(name: &str) -> Box<Table> {
        let schema = compiler::empty_schema();
        let dir = format!("/tmp/gnitz_dag_test_{}", name);
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
        let schema = compiler::empty_schema();
        let tbl = make_test_table("reg_unreg");
        dag.register_table(100, StoreHandle::Single(tbl), schema, 0, false, String::new());
        assert!(dag.tables.contains_key(&100));

        dag.unregister_table(100);
        assert!(!dag.tables.contains_key(&100));
        let _ = std::fs::remove_dir_all("/tmp/gnitz_dag_test_reg_unreg");
    }

    #[test]
    fn test_invalidation() {
        let mut dag = DagEngine::new();
        dag.shard_cols_cache.insert(42, vec![0]);
        dag.exchange_info_cache.insert(42, ExchangeInfo {
            shard_cols: vec![0],
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
    fn test_set_depth() {
        let mut dag = DagEngine::new();
        let schema = compiler::empty_schema();
        let tbl = make_test_table("set_depth");
        dag.register_table(50, StoreHandle::Single(tbl), schema, 1, false, String::new());
        dag.set_depth(50, 3);
        assert_eq!(dag.tables[&50].depth, 3);
        dag.close();
        let _ = std::fs::remove_dir_all("/tmp/gnitz_dag_test_set_depth");
    }

    #[test]
    fn test_add_remove_index_circuit() {
        let mut dag = DagEngine::new();
        let schema = compiler::empty_schema();
        let tbl = make_test_table("idx_parent");
        dag.register_table(50, StoreHandle::Single(tbl), schema, 0, false, String::new());
        let idx_tbl = make_test_table("idx_child");
        dag.add_index_circuit(50, 2, idx_tbl, schema, false);
        assert_eq!(dag.tables[&50].index_circuits.len(), 1);

        dag.remove_index_circuit(50, 2);
        assert_eq!(dag.tables[&50].index_circuits.len(), 0);
        dag.close();
        let _ = std::fs::remove_dir_all("/tmp/gnitz_dag_test_idx_parent");
        let _ = std::fs::remove_dir_all("/tmp/gnitz_dag_test_idx_child");
    }

    #[test]
    fn test_validate_graph_ok() {
        let dag = DagEngine::new();
        let nodes = vec![(0, 11), (1, 7)]; // SCAN_TRACE, INTEGRATE
        let edges = vec![(0, 0, 1, 0)];
        let sources = vec![(0, 0)]; // primary input

        assert!(dag.validate_graph_structure(&nodes, &edges, &sources).is_ok());
    }

    #[test]
    fn test_validate_graph_no_nodes() {
        let dag = DagEngine::new();
        assert_eq!(
            dag.validate_graph_structure(&[], &[], &[]).unwrap_err(),
            "View graph contains no nodes",
        );
    }

    #[test]
    fn test_validate_graph_no_input() {
        let dag = DagEngine::new();
        let nodes = vec![(0, 7)];
        let sources = vec![(0, 42)]; // not primary input
        assert_eq!(
            dag.validate_graph_structure(&nodes, &[], &sources).unwrap_err(),
            "View graph missing primary input (table_id=0)",
        );
    }

    #[test]
    fn test_validate_graph_no_sink() {
        let dag = DagEngine::new();
        let nodes = vec![(0, 2)]; // MAP, not INTEGRATE
        let sources = vec![(0, 0)];
        assert_eq!(
            dag.validate_graph_structure(&nodes, &[], &sources).unwrap_err(),
            "View graph missing sink (INTEGRATE node)",
        );
    }

    #[test]
    fn test_validate_graph_cycle() {
        let dag = DagEngine::new();
        let nodes = vec![(0, 11), (1, 7)];
        // Cycle: 0→1 and 1→0
        let edges = vec![(0, 0, 1, 0), (1, 1, 0, 0)];
        let sources = vec![(0, 0)];
        assert_eq!(
            dag.validate_graph_structure(&nodes, &edges, &sources).unwrap_err(),
            "View graph contains cycles (not a DAG)",
        );
    }

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
}
