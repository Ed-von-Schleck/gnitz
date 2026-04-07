#![allow(dead_code, unused_variables)]
//! Catalog engine: DDL operations, system table management, hook processing,
//! and entity registry.
//!
//! Consolidates identifiers.py, index_circuit.py, metadata.py,
//! loader.py, hooks.py, engine.py, and most of registry.py / system_tables.py.
//!
//! The CatalogEngine wraps DagEngine and adds:
//! - EntityRegistry (name → ID mapping, FK constraints, index tracking)
//! - System table definitions and bootstrap
//! - DDL intent (CREATE/DROP SCHEMA/TABLE/VIEW/INDEX)
//! - Hook processing (schema, table, view, index, dep effects)
//! - Catalog persistence and recovery

use std::collections::HashMap;
use std::fs;

use crate::schema::{SchemaColumn, SchemaDescriptor, type_code};
use crate::dag::{DagEngine, StoreHandle};
use crate::storage::{OwnedBatch, PartitionedTable, partition_arena_size, CursorHandle, Table};

// ---------------------------------------------------------------------------
// Constants — must match gnitz/catalog/system_tables.py
// ---------------------------------------------------------------------------

pub const SYSTEM_SCHEMA_ID: i64 = 1;
pub const PUBLIC_SCHEMA_ID: i64 = 2;
pub const FIRST_USER_SCHEMA_ID: i64 = 3;

pub const OWNER_KIND_TABLE: i64 = 0;
pub const OWNER_KIND_VIEW: i64 = 1;

pub const SEQ_ID_SCHEMAS: i64 = 1;
pub const SEQ_ID_TABLES: i64 = 2;
pub const SEQ_ID_INDICES: i64 = 3;
pub const SEQ_ID_PROGRAMS: i64 = 4;

pub const FIRST_USER_TABLE_ID: i64 = 16;
pub const FIRST_USER_INDEX_ID: i64 = 1;

const SYS_CATALOG_DIRNAME: &str = "_system_catalog";
const NUM_PARTITIONS: u32 = 256;

// System table IDs
const SCHEMA_TAB_ID: i64 = 1;
const TABLE_TAB_ID: i64 = 2;
const VIEW_TAB_ID: i64 = 3;
const COL_TAB_ID: i64 = 4;
const IDX_TAB_ID: i64 = 5;
const DEP_TAB_ID: i64 = 6;
const SEQ_TAB_ID: i64 = 7;
const CIRCUIT_NODES_TAB_ID: i64 = 11;
const CIRCUIT_EDGES_TAB_ID: i64 = 12;
const CIRCUIT_SOURCES_TAB_ID: i64 = 13;
const CIRCUIT_PARAMS_TAB_ID: i64 = 14;
const CIRCUIT_GROUP_COLS_TAB_ID: i64 = 15;

// Column indices for system tables (matching COL_* constants).
const TABLETAB_COL_SCHEMA_ID: usize = 1;
const TABLETAB_COL_NAME: usize = 2;
const TABLETAB_COL_DIRECTORY: usize = 3;
const TABLETAB_COL_PK_COL_IDX: usize = 4;
const TABLETAB_COL_CREATED_LSN: usize = 5;
const TABLETAB_COL_FLAGS: usize = 6;
const TABLETAB_FLAG_UNIQUE_PK: u64 = 1;

const SCHEMATAB_COL_NAME: usize = 1;

const VIEWTAB_COL_SCHEMA_ID: usize = 1;
const VIEWTAB_COL_NAME: usize = 2;
const VIEWTAB_COL_SQL_DEFINITION: usize = 3;
const VIEWTAB_COL_CACHE_DIRECTORY: usize = 4;
const VIEWTAB_COL_CREATED_LSN: usize = 5;

const COLTAB_COL_OWNER_ID: usize = 1;
const COLTAB_COL_OWNER_KIND: usize = 2;
const COLTAB_COL_COL_IDX: usize = 3;
const COLTAB_COL_NAME: usize = 4;
const COLTAB_COL_TYPE_CODE: usize = 5;
const COLTAB_COL_IS_NULLABLE: usize = 6;
const COLTAB_COL_FK_TABLE_ID: usize = 7;
const COLTAB_COL_FK_COL_IDX: usize = 8;

const IDXTAB_COL_OWNER_ID: usize = 1;
const IDXTAB_COL_OWNER_KIND: usize = 2;
const IDXTAB_COL_SOURCE_COL_IDX: usize = 3;
const IDXTAB_COL_NAME: usize = 4;
const IDXTAB_COL_IS_UNIQUE: usize = 5;
const IDXTAB_COL_CACHE_DIRECTORY: usize = 6;

const DEPTAB_COL_VIEW_ID: usize = 1;
const DEPTAB_COL_DEP_VIEW_ID: usize = 2;
const DEPTAB_COL_DEP_TABLE_ID: usize = 3;

const SEQTAB_COL_VALUE: usize = 1;

// Default arena sizes for system tables and user tables
const SYS_TABLE_ARENA: u64 = 256 * 1024;          // 256 KB
const USER_TABLE_ARENA: u64 = 1024 * 1024;         // 1 MB

// ---------------------------------------------------------------------------
// Schema builder helpers
// ---------------------------------------------------------------------------

const fn u64_col() -> SchemaColumn {
    SchemaColumn { type_code: type_code::U64, size: 8, nullable: 0, _pad: 0 }
}
const fn u128_col() -> SchemaColumn {
    SchemaColumn { type_code: type_code::U128, size: 16, nullable: 0, _pad: 0 }
}
const fn str_col() -> SchemaColumn {
    SchemaColumn { type_code: type_code::STRING, size: 16, nullable: 0, _pad: 0 }
}
const fn str_col_nullable() -> SchemaColumn {
    SchemaColumn { type_code: type_code::STRING, size: 16, nullable: 1, _pad: 0 }
}
const fn zero_col() -> SchemaColumn {
    SchemaColumn { type_code: 0, size: 0, nullable: 0, _pad: 0 }
}

fn make_schema(cols: &[SchemaColumn], pk_index: u32) -> SchemaDescriptor {
    let mut sd = SchemaDescriptor {
        num_columns: cols.len() as u32,
        pk_index,
        columns: [zero_col(); 64],
    };
    for (i, c) in cols.iter().enumerate() {
        sd.columns[i] = *c;
    }
    sd
}

// ---------------------------------------------------------------------------
// System table schema definitions
// ---------------------------------------------------------------------------

fn schema_tab_schema() -> SchemaDescriptor {
    make_schema(&[u64_col(), str_col()], 0)
}
fn table_tab_schema() -> SchemaDescriptor {
    make_schema(&[u64_col(), u64_col(), str_col(), str_col(), u64_col(), u64_col(), u64_col()], 0)
}
fn view_tab_schema() -> SchemaDescriptor {
    make_schema(&[u64_col(), u64_col(), str_col(), str_col(), str_col(), u64_col()], 0)
}
fn col_tab_schema() -> SchemaDescriptor {
    make_schema(&[u64_col(), u64_col(), u64_col(), u64_col(), str_col(), u64_col(), u64_col(), u64_col(), u64_col()], 0)
}
fn idx_tab_schema() -> SchemaDescriptor {
    make_schema(&[u64_col(), u64_col(), u64_col(), u64_col(), str_col(), u64_col(), str_col()], 0)
}
fn dep_tab_schema() -> SchemaDescriptor {
    make_schema(&[u128_col(), u64_col(), u64_col(), u64_col()], 0)
}
fn seq_tab_schema() -> SchemaDescriptor {
    make_schema(&[u64_col(), u64_col()], 0)
}
fn circuit_nodes_schema() -> SchemaDescriptor {
    make_schema(&[u128_col(), u64_col()], 0)
}
fn circuit_edges_schema() -> SchemaDescriptor {
    make_schema(&[u128_col(), u64_col(), u64_col(), u64_col()], 0)
}
fn circuit_sources_schema() -> SchemaDescriptor {
    make_schema(&[u128_col(), u64_col()], 0)
}
fn circuit_params_schema() -> SchemaDescriptor {
    make_schema(&[u128_col(), u64_col(), str_col_nullable()], 0)
}
fn circuit_group_cols_schema() -> SchemaDescriptor {
    make_schema(&[u128_col(), u64_col()], 0)
}

// ---------------------------------------------------------------------------
// PK packing helpers
// ---------------------------------------------------------------------------

fn pack_column_id(owner_id: i64, col_idx: i64) -> u64 {
    ((owner_id as u64) << 9) | (col_idx as u64)
}

fn pack_node_pk(view_id: i64, node_id: i64) -> (u64, u64) {
    (node_id as u64, view_id as u64)
}

fn pack_edge_pk(view_id: i64, edge_id: i64) -> (u64, u64) {
    (edge_id as u64, view_id as u64)
}

fn pack_param_pk(view_id: i64, node_id: i64, slot: i64) -> (u64, u64) {
    let lo = ((node_id as u64) << 8) | (slot as u64);
    (lo, view_id as u64)
}

fn pack_dep_pk(view_id: i64, dep_tid: i64) -> (u64, u64) {
    (dep_tid as u64, view_id as u64)
}

fn pack_gcol_pk(view_id: i64, node_id: i64, col_idx: i64) -> (u64, u64) {
    let lo = ((node_id as u64) << 16) | (col_idx as u64);
    (lo, view_id as u64)
}

// ---------------------------------------------------------------------------
// BatchBuilder — construct OwnedBatch rows for system table mutations
// ---------------------------------------------------------------------------

/// Lightweight row-by-row builder for constructing OwnedBatch in Rust.
/// Operates on OwnedBatch directly.
pub struct BatchBuilder {
    batch: OwnedBatch,
    schema: SchemaDescriptor,
    // per-row state
    curr_null_word: u64,
    curr_col: usize,
}

impl BatchBuilder {
    pub fn new(schema: SchemaDescriptor) -> Self {
        let batch = OwnedBatch::with_schema(schema, 8);
        BatchBuilder {
            batch,
            schema,
            curr_null_word: 0,
            curr_col: 0,
        }
    }

    /// Begin a new row with the given PK and weight.
    pub fn begin_row(&mut self, pk_lo: u64, pk_hi: u64, weight: i64) {
        self.batch.pk_lo.extend_from_slice(&pk_lo.to_le_bytes());
        self.batch.pk_hi.extend_from_slice(&pk_hi.to_le_bytes());
        self.batch.weight.extend_from_slice(&weight.to_le_bytes());
        self.curr_null_word = 0;
        self.curr_col = 0;
    }

    /// Put a u64 value for the current payload column.
    pub fn put_u64(&mut self, val: u64) {
        self.batch.col_data[self.curr_col].extend_from_slice(&val.to_le_bytes());
        self.curr_col += 1;
    }

    /// Put a string value for the current payload column.
    pub fn put_string(&mut self, s: &str) {
        let st = crate::schema::encode_german_string(s.as_bytes(), &mut self.batch.blob);
        self.batch.col_data[self.curr_col].extend_from_slice(&st);
        self.curr_col += 1;
    }

    /// Put a u128 value (lo, hi) for the current payload column.
    pub fn put_u128(&mut self, lo: u64, hi: u64) {
        let mut buf = [0u8; 16];
        buf[0..8].copy_from_slice(&lo.to_le_bytes());
        buf[8..16].copy_from_slice(&hi.to_le_bytes());
        self.batch.col_data[self.curr_col].extend_from_slice(&buf);
        self.curr_col += 1;
    }

    /// Put a u8 value for the current payload column.
    pub fn put_u8(&mut self, val: u8) {
        self.batch.col_data[self.curr_col].push(val);
        self.curr_col += 1;
    }

    /// Put a u16 value for the current payload column.
    pub fn put_u16(&mut self, val: u16) {
        self.batch.col_data[self.curr_col].extend_from_slice(&val.to_le_bytes());
        self.curr_col += 1;
    }

    /// Put a u32 value for the current payload column.
    pub fn put_u32(&mut self, val: u32) {
        self.batch.col_data[self.curr_col].extend_from_slice(&val.to_le_bytes());
        self.curr_col += 1;
    }

    /// Put a NULL value for the current payload column.
    pub fn put_null(&mut self) {
        let col_size = self.schema.columns[self.physical_col_idx()].size as usize;
        static ZEROS: [u8; 16] = [0u8; 16];
        self.batch.col_data[self.curr_col].extend_from_slice(&ZEROS[..col_size]);
        self.curr_null_word |= 1u64 << self.curr_col;
        self.curr_col += 1;
    }

    /// Finish the current row (writes null bitmap).
    pub fn end_row(&mut self) {
        self.batch.null_bmp.extend_from_slice(&self.curr_null_word.to_le_bytes());
        self.batch.count += 1;
        self.batch.sorted = false;
        self.batch.consolidated = false;
    }

    /// Convenience: begin + put columns + end for a simple row.
    pub fn finish(self) -> OwnedBatch {
        self.batch
    }

    fn physical_col_idx(&self) -> usize {
        // payload col index maps to schema col index (skip PK)
        let pk_idx = self.schema.pk_index as usize;
        if self.curr_col < pk_idx {
            self.curr_col
        } else {
            self.curr_col + 1
        }
    }
}

// ---------------------------------------------------------------------------
// Identifier validation (port of identifiers.py)
// ---------------------------------------------------------------------------

fn is_valid_ident_char(ch: u8) -> bool {
    ch.is_ascii_alphanumeric() || ch == b'_'
}

pub fn validate_user_identifier(name: &str) -> Result<(), String> {
    if name.is_empty() {
        return Err("Identifier cannot be empty".into());
    }
    if name.as_bytes()[0] == b'_' {
        return Err(format!(
            "User identifiers cannot start with '_' (reserved for system prefix): {}", name
        ));
    }
    for &ch in name.as_bytes() {
        if !is_valid_ident_char(ch) {
            return Err(format!("Identifier contains invalid characters: {}", name));
        }
    }
    Ok(())
}

fn parse_qualified_name<'a>(name: &'a str, default_schema: &'a str) -> (&'a str, &'a str) {
    if let Some(dot_pos) = name.find('.') {
        (&name[..dot_pos], &name[dot_pos + 1..])
    } else {
        (default_schema, name)
    }
}

// ---------------------------------------------------------------------------
// Index key type promotion (port of index_circuit.py get_index_key_type)
// ---------------------------------------------------------------------------

fn get_index_key_type(field_type_code: u8) -> Result<u8, String> {
    match field_type_code {
        type_code::U128 => Ok(type_code::U128),
        type_code::U64 => Ok(type_code::U64),
        type_code::I64 | type_code::U32 | type_code::I32 |
        type_code::U16 | type_code::I16 | type_code::U8 | type_code::I8 => Ok(type_code::U64),
        type_code::F32 | type_code::F64 | type_code::STRING => {
            Err(format!("Secondary index on column type {} not supported", field_type_code))
        }
        _ => Err(format!("Unknown column type code: {}", field_type_code)),
    }
}

fn make_index_schema(index_key_type: u8, source_pk_type: u8) -> SchemaDescriptor {
    let key_col = SchemaColumn {
        type_code: index_key_type,
        size: crate::schema::type_size(index_key_type),
        nullable: 0,
        _pad: 0,
    };
    let pk_col = SchemaColumn {
        type_code: source_pk_type,
        size: crate::schema::type_size(source_pk_type),
        nullable: 0,
        _pad: 0,
    };
    make_schema(&[key_col, pk_col], 0)
}

fn make_fk_index_name(schema_name: &str, table_name: &str, col_name: &str) -> String {
    format!("{}__{}__fk_{}", schema_name, table_name, col_name)
}

fn make_secondary_index_name(schema_name: &str, table_name: &str, col_name: &str) -> String {
    format!("{}__{}__idx_{}", schema_name, table_name, col_name)
}

// ---------------------------------------------------------------------------
// FK constraint
// ---------------------------------------------------------------------------

struct FkConstraint {
    fk_col_idx: usize,
    target_table_id: i64,
}

// ---------------------------------------------------------------------------
// Index info
// ---------------------------------------------------------------------------

struct IndexInfo {
    index_id: i64,
    owner_id: i64,
    source_col_idx: u32,
    source_col_type: u8,
    index_key_type: u8,
    source_pk_type: u8,
    name: String,
    is_unique: bool,
    cache_dir: String,
    /// Owned Table for the index storage. Box prevents moves.
    table: Box<Table>,
}

// ---------------------------------------------------------------------------
// Free function: ingest a batch into a table (avoids borrow conflicts)
// ---------------------------------------------------------------------------

fn ingest_batch_into(table: &mut Table, batch: &OwnedBatch) {
    if batch.count == 0 { return; }
    let npc = batch.col_data.len();
    let (ptrs, sizes) = batch.to_region_ptrs();
    let _ = table.ingest_batch_from_regions(&ptrs, &sizes, batch.count as u32, npc);
}

// ---------------------------------------------------------------------------
// CatalogEngine
// ---------------------------------------------------------------------------

/// The catalog engine wraps DagEngine and manages the entity registry,
/// system tables, DDL operations, and hook processing.
pub struct CatalogEngine {
    pub dag: DagEngine,
    pub base_dir: String,

    // --- Entity registry ---
    schema_name_to_id: HashMap<String, i64>,
    schema_id_to_name: HashMap<i64, String>,
    name_to_id: HashMap<String, i64>,          // "schema.table" -> table_id
    id_to_qualified: HashMap<i64, (String, String)>, // table_id -> (schema, table)
    next_schema_id: i64,
    next_table_id: i64,
    next_index_id: i64,
    pub active_part_start: u32,
    pub active_part_end: u32,

    // --- FK constraints per table ---
    fk_constraints: HashMap<i64, Vec<FkConstraint>>,

    // --- Index registry ---
    index_by_name: HashMap<String, i64>,   // name -> index_id
    index_by_id: HashMap<i64, String>,     // index_id -> name

    // --- Per-table metadata (not tracked by DagEngine) ---
    schema_ids: HashMap<i64, i64>,        // table_id -> schema_id
    pk_col_indices: HashMap<i64, u32>,    // table_id -> pk_col_idx
    col_names_cache: HashMap<i64, Vec<String>>,  // table_id -> column names

    // --- System tables (owned, single-partition, durable) ---
    sys_schemas: Box<Table>,
    sys_tables: Box<Table>,
    sys_views: Box<Table>,
    sys_columns: Box<Table>,
    sys_indices: Box<Table>,
    sys_view_deps: Box<Table>,
    sys_sequences: Box<Table>,
    sys_circuit_nodes: Box<Table>,
    sys_circuit_edges: Box<Table>,
    sys_circuit_sources: Box<Table>,
    sys_circuit_params: Box<Table>,
    sys_circuit_group_cols: Box<Table>,

    // --- Hook state ---
    cascade_enabled: bool,
}

// SAFETY: CatalogEngine is only accessed from a single thread.
unsafe impl Send for CatalogEngine {}

// ---------------------------------------------------------------------------
// Helper: read column data from cursor
// ---------------------------------------------------------------------------

/// Read a u64 from a cursor column. `logical_col` is the schema column index.
fn cursor_read_u64(cursor: &CursorHandle, logical_col: usize) -> u64 {
    let ptr = cursor.cursor.col_ptr(logical_col, 8);
    if ptr.is_null() { return 0; }
    let slice = unsafe { std::slice::from_raw_parts(ptr, 8) };
    u64::from_le_bytes(slice.try_into().unwrap_or([0; 8]))
}

/// Read a German string from a cursor column. `logical_col` is the schema column index.
fn cursor_read_string(cursor: &CursorHandle, logical_col: usize) -> String {
    let ptr = cursor.cursor.col_ptr(logical_col, 16);
    if ptr.is_null() { return String::new(); }
    let st: [u8; 16] = unsafe { std::slice::from_raw_parts(ptr, 16) }
        .try_into().unwrap_or([0; 16]);
    // For out-of-line strings, build a blob slice from the cursor's blob pointer
    let blob_ptr = cursor.cursor.blob_ptr();
    let blob_slice = if !blob_ptr.is_null() {
        let blob_len = cursor.cursor.blob_len();
        unsafe { std::slice::from_raw_parts(blob_ptr, blob_len) }
    } else {
        &[]
    };
    let bytes = crate::schema::decode_german_string(&st, blob_slice);
    String::from_utf8(bytes).unwrap_or_default()
}

// ---------------------------------------------------------------------------
// Helper: filesystem
// ---------------------------------------------------------------------------

fn ensure_dir(path: &str) -> Result<(), String> {
    // Reject any embedded NUL bytes (should never happen with clean paths)
    if path.contains('\0') {
        return Err(format!("Path contains NUL byte: {:?}", path));
    }
    match fs::create_dir_all(path) {
        Ok(_) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => Ok(()),
        Err(e) => Err(format!("Failed to create directory '{}': {}", path, e)),
    }
}

fn fsync_dir(path: &str) {
    if let Ok(dir) = fs::File::open(path) {
        let _ = dir.sync_all();
    }
}

// ---------------------------------------------------------------------------
// System table subdirectory names
// ---------------------------------------------------------------------------

struct SysTabInfo {
    id: i64,
    subdir: &'static str,
    name: &'static str,
}

const SYS_TAB_INFOS: &[SysTabInfo] = &[
    SysTabInfo { id: SCHEMA_TAB_ID, subdir: "_schemas", name: "_schemas" },
    SysTabInfo { id: TABLE_TAB_ID, subdir: "_tables", name: "_tables" },
    SysTabInfo { id: VIEW_TAB_ID, subdir: "_views", name: "_views" },
    SysTabInfo { id: COL_TAB_ID, subdir: "_columns", name: "_columns" },
    SysTabInfo { id: IDX_TAB_ID, subdir: "_indices", name: "_indices" },
    SysTabInfo { id: DEP_TAB_ID, subdir: "_view_deps", name: "_view_deps" },
    SysTabInfo { id: SEQ_TAB_ID, subdir: "_sequences", name: "_sequences" },
    SysTabInfo { id: CIRCUIT_NODES_TAB_ID, subdir: "_circuit_nodes", name: "_circuit_nodes" },
    SysTabInfo { id: CIRCUIT_EDGES_TAB_ID, subdir: "_circuit_edges", name: "_circuit_edges" },
    SysTabInfo { id: CIRCUIT_SOURCES_TAB_ID, subdir: "_circuit_sources", name: "_circuit_sources" },
    SysTabInfo { id: CIRCUIT_PARAMS_TAB_ID, subdir: "_circuit_params", name: "_circuit_params" },
    SysTabInfo { id: CIRCUIT_GROUP_COLS_TAB_ID, subdir: "_circuit_group_cols", name: "_circuit_group_cols" },
];

fn sys_tab_schema(id: i64) -> SchemaDescriptor {
    match id {
        SCHEMA_TAB_ID => schema_tab_schema(),
        TABLE_TAB_ID => table_tab_schema(),
        VIEW_TAB_ID => view_tab_schema(),
        COL_TAB_ID => col_tab_schema(),
        IDX_TAB_ID => idx_tab_schema(),
        DEP_TAB_ID => dep_tab_schema(),
        SEQ_TAB_ID => seq_tab_schema(),
        CIRCUIT_NODES_TAB_ID => circuit_nodes_schema(),
        CIRCUIT_EDGES_TAB_ID => circuit_edges_schema(),
        CIRCUIT_SOURCES_TAB_ID => circuit_sources_schema(),
        CIRCUIT_PARAMS_TAB_ID => circuit_params_schema(),
        CIRCUIT_GROUP_COLS_TAB_ID => circuit_group_cols_schema(),
        _ => unreachable!("Unknown system table ID: {}", id),
    }
}

// ---------------------------------------------------------------------------
// CatalogEngine implementation
// ---------------------------------------------------------------------------

impl CatalogEngine {
    // -- Open engine (main entry point) ------------------------------------

    /// Opens or creates a GnitzDB instance at `base_dir`.
    /// Equivalent of `open_engine()`.
    pub fn open(base_dir: &str) -> Result<Self, String> {
        ensure_dir(base_dir)?;

        let sys_dir = format!("{}/{}", base_dir, SYS_CATALOG_DIRNAME);
        ensure_dir(&sys_dir)?;

        // Create system tables (persistent, single-partition)
        let create_sys_table = |info: &SysTabInfo| -> Result<Box<Table>, String> {
            let dir = format!("{}/{}", sys_dir, info.subdir);
            let schema = sys_tab_schema(info.id);
            Table::new(&dir, info.name, schema, info.id as u32, SYS_TABLE_ARENA, true)
                .map(Box::new)
                .map_err(|e| format!("Failed to create system table '{}': error {}", info.name, e))
        };

        let sys_schemas = create_sys_table(&SYS_TAB_INFOS[0])?;
        let mut sys_tables = create_sys_table(&SYS_TAB_INFOS[1])?;
        let sys_views = create_sys_table(&SYS_TAB_INFOS[2])?;
        let sys_columns = create_sys_table(&SYS_TAB_INFOS[3])?;
        let sys_indices = create_sys_table(&SYS_TAB_INFOS[4])?;
        let sys_view_deps = create_sys_table(&SYS_TAB_INFOS[5])?;
        let sys_sequences = create_sys_table(&SYS_TAB_INFOS[6])?;
        let sys_circuit_nodes = create_sys_table(&SYS_TAB_INFOS[7])?;
        let sys_circuit_edges = create_sys_table(&SYS_TAB_INFOS[8])?;
        let sys_circuit_sources = create_sys_table(&SYS_TAB_INFOS[9])?;
        let sys_circuit_params = create_sys_table(&SYS_TAB_INFOS[10])?;
        let sys_circuit_group_cols = create_sys_table(&SYS_TAB_INFOS[11])?;

        // Check if this is a fresh database (no table records yet)
        let is_new = {
            match sys_tables.create_cursor() {
                Ok(cursor) => !cursor.cursor.valid,
                Err(_) => true,
            }
        };

        let dag = DagEngine::new();

        let mut engine = CatalogEngine {
            dag,
            base_dir: base_dir.to_string(),
            schema_name_to_id: HashMap::new(),
            schema_id_to_name: HashMap::new(),
            name_to_id: HashMap::new(),
            id_to_qualified: HashMap::new(),
            next_schema_id: FIRST_USER_SCHEMA_ID,
            next_table_id: FIRST_USER_TABLE_ID,
            next_index_id: FIRST_USER_INDEX_ID,
            active_part_start: 0,
            active_part_end: NUM_PARTITIONS,
            fk_constraints: HashMap::new(),
            index_by_name: HashMap::new(),
            index_by_id: HashMap::new(),
            schema_ids: HashMap::new(),
            pk_col_indices: HashMap::new(),
            col_names_cache: HashMap::new(),
            sys_schemas,
            sys_tables,
            sys_views,
            sys_columns,
            sys_indices,
            sys_view_deps,
            sys_sequences,
            sys_circuit_nodes,
            sys_circuit_edges,
            sys_circuit_sources,
            sys_circuit_params,
            sys_circuit_group_cols,
            cascade_enabled: false,
        };

        if is_new {
            engine.bootstrap_system_tables()?;
        }

        // Phase 1: Recover sequence counters
        engine.recover_sequences();

        // Register system table families
        engine.register_system_table_families();

        // Set system table handles on DagEngine
        engine.setup_dag_sys_tables();

        // Phase 2: Replay catalog through hooks
        engine.replay_catalog()?;

        // Enable cascading effects (e.g. auto-create FK indices)
        engine.cascade_enabled = true;

        Ok(engine)
    }

    // -- Bootstrap (fresh database) ----------------------------------------

    fn bootstrap_system_tables(&mut self) -> Result<(), String> {
        let sys_dir = format!("{}/{}", self.base_dir, SYS_CATALOG_DIRNAME);

        // 1. Core schema records
        {
            let schema = schema_tab_schema();
            let mut bb = BatchBuilder::new(schema);
            // _system schema
            bb.begin_row(SYSTEM_SCHEMA_ID as u64, 0, 1);
            bb.put_string("_system");
            bb.end_row();
            // public schema
            bb.begin_row(PUBLIC_SCHEMA_ID as u64, 0, 1);
            bb.put_string("public");
            bb.end_row();
            let batch = bb.finish();
            ingest_batch_into(&mut self.sys_schemas, &batch);
        }

        // 2. Table records (self-registration of system tables)
        {
            let schema = table_tab_schema();
            let mut bb = BatchBuilder::new(schema);
            for info in SYS_TAB_INFOS {
                let dir = format!("{}/{}", sys_dir, info.subdir);
                bb.begin_row(info.id as u64, 0, 1);
                bb.put_u64(SYSTEM_SCHEMA_ID as u64);  // schema_id
                bb.put_string(info.name);               // name
                bb.put_string(&dir);                    // directory
                bb.put_u64(0);                          // pk_col_idx
                bb.put_u64(0);                          // created_lsn
                bb.put_u64(0);                          // flags
                bb.end_row();
            }
            let batch = bb.finish();
            ingest_batch_into(&mut self.sys_tables, &batch);
        }

        // 3. Column records for all system tables
        {
            let schema = col_tab_schema();
            let mut bb = BatchBuilder::new(schema);

            let system_cols: &[(i64, &[(& str, u8)])] = &[
                (SCHEMA_TAB_ID, &[("schema_id", type_code::U64), ("name", type_code::STRING)]),
                (TABLE_TAB_ID, &[
                    ("table_id", type_code::U64), ("schema_id", type_code::U64),
                    ("name", type_code::STRING), ("directory", type_code::STRING),
                    ("pk_col_idx", type_code::U64), ("created_lsn", type_code::U64),
                    ("flags", type_code::U64),
                ]),
                (VIEW_TAB_ID, &[
                    ("view_id", type_code::U64), ("schema_id", type_code::U64),
                    ("name", type_code::STRING), ("sql_definition", type_code::STRING),
                    ("cache_directory", type_code::STRING), ("created_lsn", type_code::U64),
                ]),
                (COL_TAB_ID, &[
                    ("column_id", type_code::U64), ("owner_id", type_code::U64),
                    ("owner_kind", type_code::U64), ("col_idx", type_code::U64),
                    ("name", type_code::STRING), ("type_code", type_code::U64),
                    ("is_nullable", type_code::U64), ("fk_table_id", type_code::U64),
                    ("fk_col_idx", type_code::U64),
                ]),
                (IDX_TAB_ID, &[
                    ("index_id", type_code::U64), ("owner_id", type_code::U64),
                    ("owner_kind", type_code::U64), ("source_col_idx", type_code::U64),
                    ("name", type_code::STRING), ("is_unique", type_code::U64),
                    ("cache_directory", type_code::STRING),
                ]),
                (SEQ_TAB_ID, &[("seq_id", type_code::U64), ("next_val", type_code::U64)]),
                (CIRCUIT_NODES_TAB_ID, &[("node_pk", type_code::U128), ("opcode", type_code::U64)]),
                (CIRCUIT_EDGES_TAB_ID, &[
                    ("edge_pk", type_code::U128), ("src_node", type_code::U64),
                    ("dst_node", type_code::U64), ("dst_port", type_code::U64),
                ]),
                (CIRCUIT_SOURCES_TAB_ID, &[("source_pk", type_code::U128), ("table_id", type_code::U64)]),
                (CIRCUIT_PARAMS_TAB_ID, &[
                    ("param_pk", type_code::U128), ("value", type_code::U64),
                    ("str_value", type_code::STRING),
                ]),
                (CIRCUIT_GROUP_COLS_TAB_ID, &[("gcol_pk", type_code::U128), ("col_idx", type_code::U64)]),
            ];

            for &(tid, cols) in system_cols {
                for (i, &(name, tcode)) in cols.iter().enumerate() {
                    let pk = pack_column_id(tid, i as i64);
                    bb.begin_row(pk, 0, 1);
                    bb.put_u64(tid as u64);          // owner_id
                    bb.put_u64(OWNER_KIND_TABLE as u64); // owner_kind
                    bb.put_u64(i as u64);            // col_idx
                    bb.put_string(name);             // name
                    bb.put_u64(tcode as u64);        // type_code
                    bb.put_u64(0);                   // is_nullable
                    bb.put_u64(0);                   // fk_table_id
                    bb.put_u64(0);                   // fk_col_idx
                    bb.end_row();
                }
            }
            let batch = bb.finish();
            ingest_batch_into(&mut self.sys_columns, &batch);
        }

        // 4. Sequence high-water marks
        {
            let schema = seq_tab_schema();
            let mut bb = BatchBuilder::new(schema);
            bb.begin_row(SEQ_ID_SCHEMAS as u64, 0, 1);
            bb.put_u64((FIRST_USER_SCHEMA_ID - 1) as u64);
            bb.end_row();
            bb.begin_row(SEQ_ID_TABLES as u64, 0, 1);
            bb.put_u64((FIRST_USER_TABLE_ID - 1) as u64);
            bb.end_row();
            bb.begin_row(SEQ_ID_INDICES as u64, 0, 1);
            bb.put_u64((FIRST_USER_INDEX_ID - 1) as u64);
            bb.end_row();
            bb.begin_row(SEQ_ID_PROGRAMS as u64, 0, 1);
            bb.put_u64((FIRST_USER_TABLE_ID - 1) as u64);
            bb.end_row();
            let batch = bb.finish();
            ingest_batch_into(&mut self.sys_sequences, &batch);
        }

        // Flush all foundational metadata to disk
        let _ = self.sys_schemas.flush();
        let _ = self.sys_tables.flush();
        let _ = self.sys_columns.flush();
        let _ = self.sys_sequences.flush();

        Ok(())
    }

    // -- Recover sequence counters from sys_sequences ----------------------

    fn recover_sequences(&mut self) {
        let mut cursor = match self.sys_sequences.create_cursor() {
            Ok(c) => c,
            Err(_) => return,
        };
        while cursor.cursor.valid {
            if cursor.cursor.current_weight > 0 {
                let seq_id = cursor.cursor.current_key_lo as i64;
                let val = cursor_read_u64(&cursor, SEQTAB_COL_VALUE) as i64;
                match seq_id {
                    SEQ_ID_SCHEMAS => {
                        let next = val + 1;
                        if next > self.next_schema_id { self.next_schema_id = next; }
                    }
                    SEQ_ID_TABLES => {
                        let next = val + 1;
                        if next > self.next_table_id { self.next_table_id = next; }
                    }
                    SEQ_ID_INDICES => {
                        let next = val + 1;
                        if next > self.next_index_id { self.next_index_id = next; }
                    }
                    SEQ_ID_PROGRAMS => {
                        let next = val + 1;
                        if next > self.next_table_id { self.next_table_id = next; }
                    }
                    _ => {}
                }
            }
            cursor.cursor.advance();
        }
    }

    // -- Register system table families ------------------------------------

    fn register_system_table_families(&mut self) {
        let sys_dir = format!("{}/{}", self.base_dir, SYS_CATALOG_DIRNAME);

        // Collect all 12 system table pointers
        let table_ptrs: [*mut Table; 12] = [
            &mut *self.sys_schemas,
            &mut *self.sys_tables,
            &mut *self.sys_views,
            &mut *self.sys_columns,
            &mut *self.sys_indices,
            &mut *self.sys_view_deps,
            &mut *self.sys_sequences,
            &mut *self.sys_circuit_nodes,
            &mut *self.sys_circuit_edges,
            &mut *self.sys_circuit_sources,
            &mut *self.sys_circuit_params,
            &mut *self.sys_circuit_group_cols,
        ];

        for (i, info) in SYS_TAB_INFOS.iter().enumerate() {
            let qualified = format!("_system.{}", info.name);
            let dir = format!("{}/{}", sys_dir, info.subdir);

            self.name_to_id.insert(qualified, info.id);
            self.id_to_qualified.insert(info.id, ("_system".into(), info.name.into()));
            self.schema_ids.insert(info.id, SYSTEM_SCHEMA_ID);
            // directory tracked by DagEngine TableEntry
            self.pk_col_indices.insert(info.id, 0);
            // unique_pk and directory tracked by DagEngine TableEntry

            let schema = sys_tab_schema(info.id);
            self.dag.register_table(
                info.id,
                StoreHandle::Single(table_ptrs[i]),
                schema,
                0, // depth
                false, // unique_pk
                dir,
            );
        }

        // Register system schema names
        self.schema_name_to_id.insert("_system".into(), SYSTEM_SCHEMA_ID);
        self.schema_id_to_name.insert(SYSTEM_SCHEMA_ID, "_system".into());
    }

    // -- Setup DagEngine system table references ---------------------------

    fn setup_dag_sys_tables(&mut self) {
        use crate::dag::SysTableRefs;
        self.dag.set_sys_tables(SysTableRefs {
            nodes: &mut *self.sys_circuit_nodes as *mut Table,
            edges: &mut *self.sys_circuit_edges as *mut Table,
            sources: &mut *self.sys_circuit_sources as *mut Table,
            params: &mut *self.sys_circuit_params as *mut Table,
            group_cols: &mut *self.sys_circuit_group_cols as *mut Table,
            dep_tab: &mut *self.sys_view_deps as *mut Table,
            nodes_schema: circuit_nodes_schema(),
            edges_schema: circuit_edges_schema(),
            sources_schema: circuit_sources_schema(),
            params_schema: circuit_params_schema(),
            group_cols_schema: circuit_group_cols_schema(),
            dep_tab_schema: dep_tab_schema(),
        });
    }

    // -- Replay catalog (recovery) -----------------------------------------

    fn replay_catalog(&mut self) -> Result<(), String> {
        // Replay in dependency order: schemas, then tables, then views, then indices
        self.replay_system_table(SCHEMA_TAB_ID)?;
        self.replay_system_table(TABLE_TAB_ID)?;
        self.replay_system_table(VIEW_TAB_ID)?;
        self.replay_system_table(IDX_TAB_ID)?;
        Ok(())
    }

    fn replay_system_table(&mut self, sys_table_id: i64) -> Result<(), String> {
        let schema = sys_tab_schema(sys_table_id);
        let table_ptr: *mut Table = match sys_table_id {
            SCHEMA_TAB_ID => &mut *self.sys_schemas,
            TABLE_TAB_ID => &mut *self.sys_tables,
            VIEW_TAB_ID => &mut *self.sys_views,
            IDX_TAB_ID => &mut *self.sys_indices,
            _ => return Ok(()),
        };

        // Scan all live rows and process through hooks
        let mut cursor = unsafe { (*table_ptr).create_cursor().map_err(|e| format!("cursor error: {}", e))? };
        let mut batch = OwnedBatch::with_schema(schema, 512);
        let mut count = 0;

        while cursor.cursor.valid {
            if cursor.cursor.current_weight > 0 {
                // Copy row into batch
                self.copy_cursor_row_to_batch(&cursor, &schema, &mut batch);
                count += 1;

                if count >= 512 {
                    self.fire_hooks(sys_table_id, &batch)?;
                    batch = OwnedBatch::with_schema(schema, 512);
                    count = 0;
                }
            }
            cursor.cursor.advance();
        }

        if count > 0 {
            self.fire_hooks(sys_table_id, &batch)?;
        }

        Ok(())
    }

    fn copy_cursor_row_to_batch(
        &self,
        cursor: &CursorHandle,
        schema: &SchemaDescriptor,
        batch: &mut OwnedBatch,
    ) {
        let pk_lo = cursor.cursor.current_key_lo;
        let pk_hi = cursor.cursor.current_key_hi;
        let weight = cursor.cursor.current_weight;
        let null_word = cursor.cursor.current_null_word;

        batch.pk_lo.extend_from_slice(&pk_lo.to_le_bytes());
        batch.pk_hi.extend_from_slice(&pk_hi.to_le_bytes());
        batch.weight.extend_from_slice(&weight.to_le_bytes());
        batch.null_bmp.extend_from_slice(&null_word.to_le_bytes());

        let pk_index = schema.pk_index as usize;
        let src_blob_ptr = cursor.cursor.blob_ptr();
        let mut payload_idx = 0;
        for ci in 0..schema.num_columns as usize {
            if ci == pk_index { continue; }
            let col_size = schema.columns[ci].size as usize;
            let ptr = cursor.cursor.col_ptr(ci, col_size);
            if !ptr.is_null() {
                let data = unsafe { std::slice::from_raw_parts(ptr, col_size) };
                // For out-of-line strings, rewrite blob offset to target batch.blob
                if schema.columns[ci].type_code == type_code::STRING && col_size == 16 {
                    let len = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
                    if len > crate::schema::SHORT_STRING_THRESHOLD && !src_blob_ptr.is_null() {
                        let src_offset = u64::from_le_bytes(data[8..16].try_into().unwrap()) as usize;
                        let new_offset = batch.blob.len() as u64;
                        let src_bytes = unsafe { std::slice::from_raw_parts(src_blob_ptr.add(src_offset), len) };
                        batch.blob.extend_from_slice(src_bytes);
                        let mut cell = [0u8; 16];
                        cell[..8].copy_from_slice(&data[..8]); // len + prefix
                        cell[8..16].copy_from_slice(&new_offset.to_le_bytes());
                        batch.col_data[payload_idx].extend_from_slice(&cell);
                    } else {
                        batch.col_data[payload_idx].extend_from_slice(data);
                    }
                } else {
                    batch.col_data[payload_idx].extend_from_slice(data);
                }
            } else {
                static ZEROS: [u8; 16] = [0u8; 16];
                batch.col_data[payload_idx].extend_from_slice(&ZEROS[..col_size]);
            }
            payload_idx += 1;
        }

        batch.count += 1;
    }

    // -- Hook processing ---------------------------------------------------

    fn fire_hooks(&mut self, sys_table_id: i64, batch: &OwnedBatch) -> Result<(), String> {
        match sys_table_id {
            SCHEMA_TAB_ID => self.on_schema_delta(batch),
            TABLE_TAB_ID => self.on_table_delta(batch),
            VIEW_TAB_ID => self.on_view_delta(batch),
            IDX_TAB_ID => self.on_index_delta(batch),
            DEP_TAB_ID => {
                self.dag.invalidate_dep_map();
                Ok(())
            }
            _ => Ok(()),
        }
    }

    fn on_schema_delta(&mut self, batch: &OwnedBatch) -> Result<(), String> {
        for i in 0..batch.count {
            let weight = batch.get_weight(i);
            let sid = batch.get_pk_lo(i) as i64;
            let name = self.read_batch_string(batch, i, 0); // COL_NAME at payload col 0
            gnitz_debug!("catalog: on_schema_delta sid={} weight={} name={:?}", sid, weight, name);

            if weight > 0 {
                if self.schema_name_to_id.contains_key(&name) {
                    continue;
                }
                let path = format!("{}/{}", self.base_dir, name);
                ensure_dir(&path)?;
                fsync_dir(&self.base_dir);
                self.schema_name_to_id.insert(name.clone(), sid);
                self.schema_id_to_name.insert(sid, name);
            } else {
                if self.schema_name_to_id.contains_key(&name) {
                    if name == "_system" {
                        return Err("Forbidden: cannot drop system schema".into());
                    }
                    if !self.schema_is_empty(&name) {
                        return Err(format!("Cannot drop non-empty schema: {}", name));
                    }
                    self.schema_name_to_id.remove(&name);
                    self.schema_id_to_name.remove(&sid);
                }
            }
        }
        Ok(())
    }

    fn on_table_delta(&mut self, batch: &OwnedBatch) -> Result<(), String> {
        for i in 0..batch.count {
            let weight = batch.get_weight(i);
            let tid = batch.get_pk_lo(i) as i64;
            let sid = self.read_batch_u64(batch, i, 0) as i64; // TABLETAB_COL_SCHEMA_ID
            let name = self.read_batch_string(batch, i, 1);    // TABLETAB_COL_NAME
            let pk_col_idx = self.read_batch_u64(batch, i, 3) as u32; // TABLETAB_COL_PK_COL_IDX
            let flags = self.read_batch_u64(batch, i, 5);      // TABLETAB_COL_FLAGS
            let is_unique = (flags & TABLETAB_FLAG_UNIQUE_PK) != 0;

            if weight > 0 {
                if self.dag.tables.contains_key(&tid) {
                    continue;
                }

                // Read column definitions
                let col_defs = self.read_column_defs(tid)?;
                if col_defs.is_empty() {
                    gnitz_warn!("catalog: cannot register table '{}': columns not found", name);
                    continue;
                }

                // Validate PK type (must be U64 or U128)
                if (pk_col_idx as usize) < col_defs.len() {
                    let pk_type = col_defs[pk_col_idx as usize].type_code;
                    if pk_type != type_code::U64 && pk_type != type_code::U128 {
                        return Err(format!("Primary Key must be TYPE_U64 or TYPE_U128, got type_code={}", pk_type));
                    }
                }

                let schema_name = self.schema_id_to_name.get(&sid)
                    .cloned().unwrap_or_default();
                let directory = format!("{}/{}/{}_{}", self.base_dir, schema_name, name, tid);

                // Build schema descriptor from column defs
                let tbl_schema = self.build_schema_from_col_defs(&col_defs, pk_col_idx);

                // Create partitioned table
                let num_parts = if tid < FIRST_USER_TABLE_ID { 1 } else { NUM_PARTITIONS };
                let arena = partition_arena_size(num_parts);
                gnitz_debug!("catalog: creating table dir={} name={} tid={} parts={}", directory, name, tid, num_parts);
                let pt = PartitionedTable::new(
                    &directory, &name, tbl_schema, tid as u32, num_parts,
                    true, // durable
                    self.active_part_start, self.active_part_end,
                    arena,
                ).map_err(|e| format!("Failed to create table '{}': error {} (dir={})", name, e, directory))?;

                fsync_dir(&format!("{}/{}", self.base_dir, schema_name));

                let pt_box = Box::new(pt);
                let pt_ptr = &*pt_box as *const PartitionedTable as *mut PartitionedTable;

                // Register with entity registry
                let qualified = format!("{}.{}", schema_name, name);
                self.name_to_id.insert(qualified, tid);
                self.id_to_qualified.insert(tid, (schema_name.clone(), name.clone()));
                self.schema_ids.insert(tid, sid);
                                self.pk_col_indices.insert(tid, pk_col_idx);
                
                // Register with DagEngine
                self.dag.register_table(
                    tid,
                    StoreHandle::Partitioned(pt_ptr),
                    tbl_schema,
                    0, // depth
                    is_unique,
                    directory.clone(),
                );

                // Advance next_table_id if this is a gap recovery
                if tid + 1 > self.next_table_id {
                    self.next_table_id = tid + 1;
                }

                // Wire FK constraints
                self.wire_fk_constraints(tid, &col_defs);

                // Leak the Box so the pointer stays valid
                let _ = Box::into_raw(pt_box);

                if self.cascade_enabled {
                    self.create_fk_indices(tid)?;
                }
            } else {
                if self.dag.tables.contains_key(&tid) {
                    // Check for FK references before dropping
                    for (&ref_tid, _) in &self.id_to_qualified {
                        if ref_tid == tid { continue; }
                        if let Some(constraints) = self.fk_constraints.get(&ref_tid) {
                            for c in constraints {
                                if c.target_table_id == tid {
                                    let (sn, tn) = self.id_to_qualified.get(&ref_tid)
                                        .cloned().unwrap_or_default();
                                    return Err(format!(
                                        "Integrity violation: table referenced by '{}.{}'", sn, tn
                                    ));
                                }
                            }
                        }
                    }

                    self.dag.unregister_table(tid);
                    self.col_names_cache.remove(&tid);
                    // Clean up registry
                    if let Some((sn, tn)) = self.id_to_qualified.remove(&tid) {
                        let qualified = format!("{}.{}", sn, tn);
                        self.name_to_id.remove(&qualified);
                    }
                    self.schema_ids.remove(&tid);

                    self.pk_col_indices.remove(&tid);

                    self.fk_constraints.remove(&tid);
                }
            }
        }
        Ok(())
    }

    fn on_view_delta(&mut self, batch: &OwnedBatch) -> Result<(), String> {
        for i in 0..batch.count {
            let weight = batch.get_weight(i);
            let vid = batch.get_pk_lo(i) as i64;
            let sid = self.read_batch_u64(batch, i, 0) as i64;
            let name = self.read_batch_string(batch, i, 1);

            if weight > 0 {
                if self.dag.tables.contains_key(&vid) {
                    continue;
                }

                let col_defs = self.read_column_defs(vid)?;
                if col_defs.is_empty() {
                    gnitz_warn!("catalog: cannot register view '{}': columns not found", name);
                    continue;
                }

                let schema_name = self.schema_id_to_name.get(&sid)
                    .cloned().unwrap_or_default();
                let directory = format!("{}/{}/view_{}_{}", self.base_dir, schema_name, name, vid);
                let view_schema = self.build_schema_from_col_defs(&col_defs, 0);

                // Create partitioned ephemeral table
                let num_parts = if vid < FIRST_USER_TABLE_ID { 1 } else { NUM_PARTITIONS };
                let arena = partition_arena_size(num_parts);
                let et = PartitionedTable::new(
                    &directory, &name, view_schema, vid as u32, num_parts,
                    false, // not durable (ephemeral)
                    self.active_part_start, self.active_part_end,
                    arena,
                ).map_err(|e| format!("Failed to create view '{}': error {}", name, e))?;

                let et_box = Box::new(et);
                let et_ptr = &*et_box as *const PartitionedTable as *mut PartitionedTable;

                // Compute depth from source tables
                let source_ids = self.dag.get_source_ids(vid);
                let mut max_depth = 0i32;
                for src_id in &source_ids {
                    if let Some(entry) = self.dag.tables.get(src_id) {
                        let d = entry.depth + 1;
                        if d > max_depth { max_depth = d; }
                    }
                }

                let qualified = format!("{}.{}", schema_name, name);
                self.name_to_id.insert(qualified, vid);
                self.id_to_qualified.insert(vid, (schema_name.clone(), name.clone()));
                self.schema_ids.insert(vid, sid);
                                self.pk_col_indices.insert(vid, 0);
                
                self.dag.register_table(
                    vid,
                    StoreHandle::Partitioned(et_ptr),
                    view_schema,
                    max_depth,
                    false,
                    directory.clone(),
                );

                // Advance next_table_id if this is a gap recovery
                if vid + 1 > self.next_table_id {
                    self.next_table_id = vid + 1;
                }

                // Leak the Box
                let _ = Box::into_raw(et_box);

                // Compile and backfill (only on processes that own data)
                if self.active_part_start != self.active_part_end {
                    if self.dag.ensure_compiled(vid) {
                        if !self.dag.view_needs_exchange(vid) {
                            self.backfill_view(vid);
                        }
                    }
                }
            } else {
                if self.dag.tables.contains_key(&vid) {
                    self.dag.unregister_table(vid);
                    self.col_names_cache.remove(&vid);
                    if let Some((sn, tn)) = self.id_to_qualified.remove(&vid) {
                        let qualified = format!("{}.{}", sn, tn);
                        self.name_to_id.remove(&qualified);
                    }
                    self.schema_ids.remove(&vid);

                    self.pk_col_indices.remove(&vid);

                }
            }
        }
        Ok(())
    }

    fn on_index_delta(&mut self, batch: &OwnedBatch) -> Result<(), String> {
        for i in 0..batch.count {
            let weight = batch.get_weight(i);
            let idx_id = batch.get_pk_lo(i) as i64;
            let owner_id = self.read_batch_u64(batch, i, 0) as i64;
            let source_col_idx = self.read_batch_u64(batch, i, 2) as u32;
            let name = self.read_batch_string(batch, i, 3);
            let is_unique = self.read_batch_u64(batch, i, 4) != 0;
            let cache_dir = self.read_batch_string(batch, i, 5);

            if weight > 0 {
                if self.index_by_name.contains_key(&name) {
                    continue;
                }

                let entry = self.dag.tables.get(&owner_id)
                    .ok_or_else(|| format!("Index: owner table {} not found", owner_id))?;
                let owner_schema = entry.schema;
                let source_col_type = owner_schema.columns[source_col_idx as usize].type_code;
                let source_pk_type = owner_schema.columns[owner_schema.pk_index as usize].type_code;

                let index_key_type = get_index_key_type(source_col_type)?;
                let idx_schema = make_index_schema(index_key_type, source_pk_type);

                let owner_dir = self.dag.tables.get(&owner_id)
                    .map(|e| e.directory.clone()).unwrap_or_default();
                let idx_dir = format!("{}/idx_{}", owner_dir, idx_id);

                let idx_table = Table::new(
                    &idx_dir,
                    &format!("_idx_{}", idx_id),
                    idx_schema,
                    idx_id as u32,
                    SYS_TABLE_ARENA,
                    false, // not durable (rebuilt from source)
                ).map_err(|e| format!("Failed to create index table: error {}", e))?;

                let idx_table_box = Box::new(idx_table);
                let idx_table_ptr = &*idx_table_box as *const Table as *mut Table;

                // Backfill index from source table
                self.backfill_index(owner_id, source_col_idx, is_unique, idx_table_ptr, &idx_schema);

                // Register with DagEngine
                self.dag.add_index_circuit(owner_id, source_col_idx, idx_table_ptr, idx_schema, is_unique);

                // Register in index maps
                self.index_by_name.insert(name.clone(), idx_id);
                self.index_by_id.insert(idx_id, name.clone());

                // Leak the Box
                let _ = Box::into_raw(idx_table_box);
            } else {
                if self.index_by_name.contains_key(&name) {
                    if name.contains("__fk_") {
                        return Err("Forbidden: cannot drop internal FK index".into());
                    }
                    self.dag.remove_index_circuit(owner_id, source_col_idx);
                    self.index_by_name.remove(&name);
                    self.index_by_id.remove(&idx_id);
                }
            }
        }
        Ok(())
    }

    // -- View backfill (scan source, feed through plan) --------------------

    fn backfill_view(&mut self, vid: i64) {
        if !self.dag.ensure_compiled(vid) { return; }

        let source_ids = self.dag.get_source_ids(vid);
        for source_id in source_ids {
            if !self.dag.tables.contains_key(&source_id) { continue; }

            let entry = match self.dag.tables.get(&source_id) {
                Some(e) => e,
                None => continue,
            };
            let schema = entry.schema;

            // Scan source table into a batch
            let scan_batch = self.scan_store(source_id, &schema);
            if scan_batch.count == 0 { continue; }

            // Execute view plan on the batch
            let out_handle = self.dag.execute_epoch(vid, scan_batch, source_id);
            if let Some(result) = out_handle {
                if result.count > 0 {
                    self.dag.ingest_to_family(vid, result);
                    let _ = self.dag.flush(vid);
                }
            }
        }
    }

    fn scan_store(&mut self, table_id: i64, schema: &SchemaDescriptor) -> OwnedBatch {
        let entry = match self.dag.tables.get(&table_id) {
            Some(e) => e,
            None => return OwnedBatch::empty(schema.num_columns as usize - 1),
        };

        let mut batch = OwnedBatch::with_schema(*schema, 256);

        match &entry.handle {
            StoreHandle::Single(ptr) if !ptr.is_null() => {
                let table = unsafe { &mut **ptr };
                if let Ok(mut cursor) = table.create_cursor() {
                    while cursor.cursor.valid {
                        if cursor.cursor.current_weight > 0 {
                            self.copy_cursor_row_to_batch(&cursor, schema, &mut batch);
                        }
                        cursor.cursor.advance();
                    }
                }
            }
            StoreHandle::Partitioned(ptr) if !ptr.is_null() => {
                let ptable = unsafe { &mut **ptr };
                if let Ok(mut cursor) = ptable.create_cursor() {
                    while cursor.cursor.valid {
                        if cursor.cursor.current_weight > 0 {
                            self.copy_cursor_row_to_batch(&cursor, schema, &mut batch);
                        }
                        cursor.cursor.advance();
                    }
                }
            }
            _ => {}
        }

        batch
    }

    // -- Index backfill (scan source, project into index table) ------------

    fn backfill_index(
        &mut self,
        owner_id: i64,
        col_idx: u32,
        is_unique: bool,
        idx_table: *mut Table,
        idx_schema: &SchemaDescriptor,
    ) {
        let entry = match self.dag.tables.get(&owner_id) {
            Some(e) => e,
            None => return,
        };
        let owner_schema = entry.schema;

        // Scan source and project into index
        let scan = self.scan_store(owner_id, &owner_schema);
        if scan.count == 0 { return; }

        let projected = DagEngine::batch_project_index(&scan, col_idx, &owner_schema, idx_schema);
        if projected.count > 0 {
            let npc = projected.col_data.len();
            let (ptrs, sizes) = projected.to_region_ptrs();
            let table = unsafe { &mut *idx_table };
            let _ = table.ingest_batch_from_regions(&ptrs, &sizes, projected.count as u32, npc);
        }
    }

    // -- Read column definitions from sys_columns --------------------------

    fn read_column_defs(&mut self, owner_id: i64) -> Result<Vec<ColumnDef>, String> {
        let start_pk = pack_column_id(owner_id, 0);
        let end_pk = pack_column_id(owner_id + 1, 0);

        let mut cursor = match self.sys_columns.create_cursor() {
            Ok(c) => c,
            Err(_) => return Ok(Vec::new()),
        };
        cursor.cursor.seek(start_pk, 0);

        let mut defs = Vec::new();
        while cursor.cursor.valid {
            let pk = cursor.cursor.current_key_lo;
            if pk >= end_pk { break; }
            if cursor.cursor.current_weight > 0 {
                let type_code = cursor_read_u64(&cursor, COLTAB_COL_TYPE_CODE) as u8;
                let is_nullable = cursor_read_u64(&cursor, COLTAB_COL_IS_NULLABLE) != 0;
                let name = cursor_read_string(&cursor, COLTAB_COL_NAME);
                let fk_table_id = cursor_read_u64(&cursor, COLTAB_COL_FK_TABLE_ID) as i64;
                let fk_col_idx = cursor_read_u64(&cursor, COLTAB_COL_FK_COL_IDX) as u32;
                defs.push(ColumnDef {
                    name,
                    type_code,
                    is_nullable,
                    fk_table_id,
                    fk_col_idx,
                });
            }
            cursor.cursor.advance();
        }
        Ok(defs)
    }

    fn build_schema_from_col_defs(&self, col_defs: &[ColumnDef], pk_index: u32) -> SchemaDescriptor {
        let mut sd = SchemaDescriptor {
            num_columns: col_defs.len() as u32,
            pk_index,
            columns: [zero_col(); 64],
        };
        for (i, cd) in col_defs.iter().enumerate() {
            sd.columns[i] = SchemaColumn {
                type_code: cd.type_code,
                size: crate::schema::type_size(cd.type_code),
                nullable: if cd.is_nullable { 1 } else { 0 },
                _pad: 0,
            };
        }
        sd
    }

    // -- FK constraint wiring ----------------------------------------------

    fn wire_fk_constraints(&mut self, table_id: i64, col_defs: &[ColumnDef]) {
        let mut constraints = Vec::new();
        for (col_idx, cd) in col_defs.iter().enumerate() {
            if cd.fk_table_id != 0 {
                constraints.push(FkConstraint {
                    fk_col_idx: col_idx,
                    target_table_id: cd.fk_table_id,
                });
            }
        }
        if !constraints.is_empty() {
            self.fk_constraints.insert(table_id, constraints);
        }
    }

    fn create_fk_indices(&mut self, table_id: i64) -> Result<(), String> {
        let col_defs = self.read_column_defs(table_id)?;
        let pk_idx = self.pk_col_indices.get(&table_id).copied().unwrap_or(0) as usize;

        let (schema_name, table_name) = self.id_to_qualified.get(&table_id)
            .cloned().unwrap_or_default();

        for (col_idx, cd) in col_defs.iter().enumerate() {
            if cd.fk_table_id == 0 || col_idx == pk_idx { continue; }
            let index_name = make_fk_index_name(&schema_name, &table_name, &cd.name);
            if self.index_by_name.contains_key(&index_name) { continue; }

            let index_id = self.next_index_id;
            self.next_index_id += 1;

            // Write index record to sys_indices
            let idx_schema = idx_tab_schema();
            let mut bb = BatchBuilder::new(idx_schema);
            bb.begin_row(index_id as u64, 0, 1);
            bb.put_u64(table_id as u64);           // owner_id
            bb.put_u64(OWNER_KIND_TABLE as u64);   // owner_kind
            bb.put_u64(col_idx as u64);            // source_col_idx
            bb.put_string(&index_name);            // name
            bb.put_u64(0);                         // is_unique (FK indices are not unique)
            bb.put_string("");                     // cache_directory
            bb.end_row();
            let batch = bb.finish();
            ingest_batch_into(&mut self.sys_indices, &batch);

            // Process through hook
            self.on_index_delta(&batch)?;

            // Update sequence
            self.advance_sequence(SEQ_ID_INDICES, index_id - 1, index_id);
        }
        Ok(())
    }

    // -- Batch field readers -----------------------------------------------

    fn read_batch_u64(&self, batch: &OwnedBatch, row: usize, payload_col: usize) -> u64 {
        let off = row * 8;
        if off + 8 > batch.col_data[payload_col].len() { return 0; }
        u64::from_le_bytes(batch.col_data[payload_col][off..off + 8].try_into().unwrap_or([0; 8]))
    }

    fn read_batch_string(&self, batch: &OwnedBatch, row: usize, payload_col: usize) -> String {
        let off = row * 16;
        let data = &batch.col_data[payload_col];
        if off + 16 > data.len() { return String::new(); }
        let st: [u8; 16] = data[off..off + 16].try_into().unwrap_or([0; 16]);
        let bytes = crate::schema::decode_german_string(&st, &batch.blob);
        String::from_utf8(bytes).unwrap_or_default()
    }

    // -- Registry query methods (used by server code via FFI) ----------------

    pub fn has_id(&self, table_id: i64) -> bool {
        self.dag.tables.contains_key(&table_id)
    }

    pub fn get_schema(&self, table_id: i64) -> Option<SchemaDescriptor> {
        self.dag.tables.get(&table_id).map(|e| e.schema)
    }

    pub fn get_schema_name_by_id(&self, schema_id: i64) -> &str {
        self.schema_id_to_name.get(&schema_id).map(|s| s.as_str()).unwrap_or("")
    }

    pub fn has_schema(&self, name: &str) -> bool {
        self.schema_name_to_id.contains_key(name)
    }

    pub fn get_schema_id(&self, name: &str) -> i64 {
        self.schema_name_to_id.get(name).copied().unwrap_or(-1)
    }

    pub fn schema_is_empty(&self, schema_name: &str) -> bool {
        for (_, (sn, _)) in &self.id_to_qualified {
            if sn == schema_name { return false; }
        }
        true
    }

    pub fn allocate_schema_id(&mut self) -> i64 {
        let sid = self.next_schema_id;
        self.next_schema_id += 1;
        sid
    }

    pub fn allocate_table_id(&mut self) -> i64 {
        let tid = self.next_table_id;
        self.next_table_id += 1;
        tid
    }

    pub fn allocate_index_id(&mut self) -> i64 {
        let iid = self.next_index_id;
        self.next_index_id += 1;
        iid
    }

    pub fn get_depth(&self, table_id: i64) -> i32 {
        self.dag.tables.get(&table_id).map(|e| e.depth).unwrap_or(0)
    }

    pub fn get_qualified_name(&self, table_id: i64) -> Option<(&str, &str)> {
        self.id_to_qualified.get(&table_id).map(|(s, t)| (s.as_str(), t.as_str()))
    }

    pub fn get_by_name(&self, schema_name: &str, table_name: &str) -> Option<i64> {
        let qualified = format!("{}.{}", schema_name, table_name);
        self.name_to_id.get(&qualified).copied()
    }

    pub fn has_index_by_name(&self, name: &str) -> bool {
        self.index_by_name.contains_key(name)
    }

    pub fn is_unique_pk(&self, table_id: i64) -> bool {
        self.dag.tables.get(&table_id).map(|e| e.unique_pk).unwrap_or(false)
    }

    // -- Sequence management -----------------------------------------------

    pub fn advance_sequence(&mut self, seq_id: i64, old_val: i64, new_val: i64) {
        let schema = seq_tab_schema();
        let mut bb = BatchBuilder::new(schema);
        // Retract old
        bb.begin_row(seq_id as u64, 0, -1);
        bb.put_u64(old_val as u64);
        bb.end_row();
        // Insert new
        bb.begin_row(seq_id as u64, 0, 1);
        bb.put_u64(new_val as u64);
        bb.end_row();
        let batch = bb.finish();
        ingest_batch_into(&mut self.sys_sequences, &batch);
    }

    // -- DDL: CREATE/DROP SCHEMA -------------------------------------------

    pub fn create_schema(&mut self, name: &str) -> Result<(), String> {
        validate_user_identifier(name)?;
        if self.schema_name_to_id.contains_key(name) {
            return Err(format!("Schema already exists: {}", name));
        }
        let sid = self.allocate_schema_id();

        // Write schema record
        let schema = schema_tab_schema();
        let mut bb = BatchBuilder::new(schema);
        bb.begin_row(sid as u64, 0, 1);
        bb.put_string(name);
        bb.end_row();
        let batch = bb.finish();

        // Ingest into schemas family (triggers hook)
        ingest_batch_into(&mut self.sys_schemas, &batch);
        self.on_schema_delta(&batch)?;

        self.advance_sequence(SEQ_ID_SCHEMAS, sid - 1, sid);
        Ok(())
    }

    pub fn drop_schema(&mut self, name: &str) -> Result<(), String> {
        validate_user_identifier(name)?;
        if !self.schema_name_to_id.contains_key(name) {
            return Err("Schema does not exist".into());
        }
        let sid = self.get_schema_id(name);

        let schema = schema_tab_schema();
        let mut bb = BatchBuilder::new(schema);
        bb.begin_row(sid as u64, 0, -1);
        bb.put_string(name);
        bb.end_row();
        let batch = bb.finish();

        ingest_batch_into(&mut self.sys_schemas, &batch);
        self.on_schema_delta(&batch)?;
        Ok(())
    }

    // -- DDL: CREATE/DROP TABLE --------------------------------------------

    pub fn create_table(
        &mut self,
        qualified_name: &str,
        col_defs: &[ColumnDef],
        pk_col_idx: u32,
        unique_pk: bool,
    ) -> Result<i64, String> {
        let (schema_name, table_name) = parse_qualified_name(qualified_name, "public");
        validate_user_identifier(schema_name)?;
        validate_user_identifier(table_name)?;

        if !self.schema_name_to_id.contains_key(schema_name) {
            return Err("Schema does not exist".into());
        }
        let qualified = format!("{}.{}", schema_name, table_name);
        if self.name_to_id.contains_key(&qualified) {
            return Err("Table already exists".into());
        }
        if col_defs.len() > 64 {
            return Err("Maximum 64 columns supported".into());
        }
        if pk_col_idx as usize >= col_defs.len() {
            return Err("Primary Key index out of bounds".into());
        }

        // Validate PK type (must be U64 or U128)
        let pk_type = col_defs[pk_col_idx as usize].type_code;
        if pk_type != type_code::U64 && pk_type != type_code::U128 {
            return Err("Primary Key must be TYPE_U64 or TYPE_U128".into());
        }

        let tid = self.allocate_table_id();
        let sid = self.get_schema_id(schema_name);
        let self_pk_type = col_defs[pk_col_idx as usize].type_code;

        // Validate FK columns
        for (col_idx, cd) in col_defs.iter().enumerate() {
            if cd.fk_table_id != 0 {
                self.validate_fk_column(cd, col_idx, tid, pk_col_idx, self_pk_type)?;
            }
        }

        let directory = format!("{}/{}/{}_{}", self.base_dir, schema_name, table_name, tid);
        let flags = if unique_pk { TABLETAB_FLAG_UNIQUE_PK } else { 0 };

        // Write columns first (table hook reads them)
        self.write_column_records(tid, OWNER_KIND_TABLE, col_defs);

        // Write table record (triggers hook)
        {
            let schema = table_tab_schema();
            let mut bb = BatchBuilder::new(schema);
            bb.begin_row(tid as u64, 0, 1);
            bb.put_u64(sid as u64);
            bb.put_string(table_name);
            bb.put_string(&directory);
            bb.put_u64(pk_col_idx as u64);
            bb.put_u64(0); // created_lsn
            bb.put_u64(flags);
            bb.end_row();
            let batch = bb.finish();
            ingest_batch_into(&mut self.sys_tables, &batch);
            self.on_table_delta(&batch)?;
        }

        self.advance_sequence(SEQ_ID_TABLES, tid - 1, tid);
        Ok(tid)
    }

    pub fn drop_table(&mut self, qualified_name: &str) -> Result<(), String> {
        let (schema_name, table_name) = parse_qualified_name(qualified_name, "public");
        validate_user_identifier(schema_name)?;
        validate_user_identifier(table_name)?;

        let qualified = format!("{}.{}", schema_name, table_name);
        let tid = *self.name_to_id.get(&qualified)
            .ok_or_else(|| format!("Table does not exist: {}.{}", schema_name, table_name))?;

        self.check_for_dependencies(tid, table_name)?;

        let sid = self.schema_ids.get(&tid).copied().unwrap_or(0);
        let dir = self.dag.tables.get(&tid).map(|e| e.directory.clone()).unwrap_or_default();
        let pk_idx = self.pk_col_indices.get(&tid).copied().unwrap_or(0);
        let is_unique = self.dag.tables.get(&tid).map(|e| e.unique_pk).unwrap_or(false);
        let flags = if is_unique { TABLETAB_FLAG_UNIQUE_PK } else { 0 };

        // Retract table record
        {
            let schema = table_tab_schema();
            let mut bb = BatchBuilder::new(schema);
            bb.begin_row(tid as u64, 0, -1);
            bb.put_u64(sid as u64);
            bb.put_string(table_name);
            bb.put_string(&dir);
            bb.put_u64(pk_idx as u64);
            bb.put_u64(0); // created_lsn
            bb.put_u64(flags);
            bb.end_row();
            let batch = bb.finish();
            ingest_batch_into(&mut self.sys_tables, &batch);
            self.on_table_delta(&batch)?;
        }

        // Retract column records
        let col_defs = self.read_column_defs(tid)?;
        self.retract_column_records(tid, OWNER_KIND_TABLE, &col_defs);

        Ok(())
    }

    // -- DDL: CREATE/DROP VIEW ---------------------------------------------

    pub fn create_view(
        &mut self,
        qualified_name: &str,
        graph: &CircuitGraph,
        sql_definition: &str,
    ) -> Result<i64, String> {
        let (schema_name, view_name) = parse_qualified_name(qualified_name, "public");
        let qualified = format!("{}.{}", schema_name, view_name);
        if self.name_to_id.contains_key(&qualified) {
            return Err("View/Table already exists".into());
        }

        // Validate graph structure via DagEngine
        self.dag.validate_graph_structure(
            &graph.nodes, &graph.edges, &graph.sources,
        ).map_err(|e| format!("Invalid graph: {}", e))?;

        let vid = self.allocate_table_id();
        let sid = self.get_schema_id(schema_name);
        let directory = format!("{}/{}/view_{}_{}", self.base_dir, schema_name, view_name, vid);

        // 1. Column records
        let col_defs: Vec<ColumnDef> = graph.output_col_defs.iter().map(|(name, tc)| ColumnDef {
            name: name.clone(),
            type_code: *tc,
            is_nullable: false,
            fk_table_id: 0,
            fk_col_idx: 0,
        }).collect();
        self.write_column_records(vid, OWNER_KIND_VIEW, &col_defs);
        let _ = self.sys_columns.flush();

        // 2. View dependencies
        self.write_view_deps(vid, &graph.dependencies);
        let _ = self.sys_view_deps.flush();
        self.dag.invalidate_dep_map();

        // 3. Circuit graph records
        self.write_circuit_graph(vid, graph);
        let _ = self.sys_circuit_nodes.flush();
        let _ = self.sys_circuit_edges.flush();
        let _ = self.sys_circuit_sources.flush();
        let _ = self.sys_circuit_params.flush();
        let _ = self.sys_circuit_group_cols.flush();

        // 4. View record (triggers hook)
        {
            let schema = view_tab_schema();
            let mut bb = BatchBuilder::new(schema);
            bb.begin_row(vid as u64, 0, 1);
            bb.put_u64(sid as u64);
            bb.put_string(view_name);
            bb.put_string(sql_definition);
            bb.put_string(&directory);
            bb.put_u64(0); // created_lsn
            bb.end_row();
            let batch = bb.finish();
            ingest_batch_into(&mut self.sys_views, &batch);
            let _ = self.sys_views.flush();
            self.on_view_delta(&batch)?;
        }

        self.advance_sequence(SEQ_ID_TABLES, vid - 1, vid);

        // 5. Eagerly compile to catch schema errors
        if !self.dag.ensure_compiled(vid) {
            return Err("View circuit schema does not match declared output columns".into());
        }

        Ok(vid)
    }

    pub fn drop_view(&mut self, qualified_name: &str) -> Result<(), String> {
        let (schema_name, view_name) = parse_qualified_name(qualified_name, "public");
        let qualified = format!("{}.{}", schema_name, view_name);
        let vid = *self.name_to_id.get(&qualified)
            .ok_or_else(|| format!("View does not exist: {}", qualified))?;

        self.check_for_dependencies(vid, view_name)?;

        self.dag.invalidate(vid);

        let sid = self.schema_ids.get(&vid).copied().unwrap_or(0);
        let dir = self.dag.tables.get(&vid).map(|e| e.directory.clone()).unwrap_or_default();

        // Retract view record
        {
            let schema = view_tab_schema();
            let mut bb = BatchBuilder::new(schema);
            bb.begin_row(vid as u64, 0, -1);
            bb.put_u64(sid as u64);
            bb.put_string(view_name);
            bb.put_string(""); // sql_definition
            bb.put_string(&dir);
            bb.put_u64(0);
            bb.end_row();
            let batch = bb.finish();
            ingest_batch_into(&mut self.sys_views, &batch);
            self.on_view_delta(&batch)?;
        }

        // Retract circuit graph, deps, columns
        self.retract_circuit_graph(vid);
        self.retract_view_deps(vid);
        let col_defs = self.read_column_defs(vid)?;
        self.retract_column_records(vid, OWNER_KIND_VIEW, &col_defs);

        Ok(())
    }

    // -- DDL: CREATE/DROP INDEX --------------------------------------------

    pub fn create_index(
        &mut self,
        qualified_owner: &str,
        col_name: &str,
        is_unique: bool,
    ) -> Result<i64, String> {
        let (schema_name, table_name) = parse_qualified_name(qualified_owner, "public");
        let qualified = format!("{}.{}", schema_name, table_name);
        let owner_id = *self.name_to_id.get(&qualified)
            .ok_or_else(|| format!("Table does not exist: {}", qualified))?;

        // Find column
        let col_defs = self.read_column_defs(owner_id)?;
        let col_idx = col_defs.iter().position(|cd| cd.name == col_name)
            .ok_or("Column not found in owner")?;

        let index_name = make_secondary_index_name(schema_name, table_name, col_name);
        let index_id = self.allocate_index_id();

        // Write index record (triggers hook)
        {
            let schema = idx_tab_schema();
            let mut bb = BatchBuilder::new(schema);
            bb.begin_row(index_id as u64, 0, 1);
            bb.put_u64(owner_id as u64);
            bb.put_u64(OWNER_KIND_TABLE as u64);
            bb.put_u64(col_idx as u64);
            bb.put_string(&index_name);
            bb.put_u64(if is_unique { 1 } else { 0 });
            bb.put_string("");
            bb.end_row();
            let batch = bb.finish();
            ingest_batch_into(&mut self.sys_indices, &batch);
            self.on_index_delta(&batch)?;
        }

        self.advance_sequence(SEQ_ID_INDICES, index_id - 1, index_id);
        Ok(index_id)
    }

    pub fn drop_index(&mut self, index_name: &str) -> Result<(), String> {
        let idx_id = *self.index_by_name.get(index_name)
            .ok_or_else(|| format!("Index does not exist: {}", index_name))?;

        // Read the full index record from sys_indices to retract it
        let mut cursor = self.sys_indices.create_cursor()
            .map_err(|e| format!("cursor error: {}", e))?;
        cursor.cursor.seek(idx_id as u64, 0);

        if !cursor.cursor.valid || cursor.cursor.current_key_lo != idx_id as u64 {
            return Err(format!("Index {} not found in catalog", index_name));
        }

        let owner_id = cursor_read_u64(&cursor, IDXTAB_COL_OWNER_ID) as i64;
        let owner_kind = cursor_read_u64(&cursor, IDXTAB_COL_OWNER_KIND);
        let source_col_idx = cursor_read_u64(&cursor, IDXTAB_COL_SOURCE_COL_IDX);
        let name = cursor_read_string(&cursor, IDXTAB_COL_NAME);
        let is_unique = cursor_read_u64(&cursor, IDXTAB_COL_IS_UNIQUE);
        let cache_dir = cursor_read_string(&cursor, IDXTAB_COL_CACHE_DIRECTORY);

        let schema = idx_tab_schema();
        let mut bb = BatchBuilder::new(schema);
        bb.begin_row(idx_id as u64, 0, -1);
        bb.put_u64(owner_id as u64);
        bb.put_u64(owner_kind);
        bb.put_u64(source_col_idx);
        bb.put_string(&name);
        bb.put_u64(is_unique);
        bb.put_string(&cache_dir);
        bb.end_row();
        let batch = bb.finish();
        ingest_batch_into(&mut self.sys_indices, &batch);
        self.on_index_delta(&batch)?;
        Ok(())
    }

    // -- Dependency check --------------------------------------------------

    fn check_for_dependencies(&mut self, tid: i64, name: &str) -> Result<(), String> {
        // Check FK references
        for (&ref_tid, constraints) in &self.fk_constraints {
            if ref_tid == tid { continue; }
            for c in constraints {
                if c.target_table_id == tid {
                    let (sn, tn) = self.id_to_qualified.get(&ref_tid)
                        .cloned().unwrap_or_default();
                    return Err(format!("FK dependency: table '{}.{}'", sn, tn));
                }
            }
        }

        // Check view dependencies
        let mut cursor = match self.sys_view_deps.create_cursor() {
            Ok(c) => c,
            Err(_) => return Ok(()),
        };
        while cursor.cursor.valid {
            if cursor.cursor.current_weight > 0 {
                let dep_tid = cursor_read_u64(&cursor, DEPTAB_COL_DEP_TABLE_ID) as i64;
                if dep_tid == tid {
                    return Err(format!("View dependency: entity '{}'", name));
                }
            }
            cursor.cursor.advance();
        }
        Ok(())
    }

    // -- FK column validation (pre-create) ---------------------------------

    fn validate_fk_column(
        &self,
        col: &ColumnDef,
        _col_idx: usize,
        self_table_id: i64,
        self_pk_index: u32,
        self_pk_type: u8,
    ) -> Result<(), String> {
        if col.fk_table_id == 0 { return Ok(()); }

        let (target_pk_index, target_pk_type) = if col.fk_table_id == self_table_id {
            (self_pk_index, self_pk_type)
        } else {
            let entry = self.dag.tables.get(&col.fk_table_id)
                .ok_or_else(|| format!("FK references unknown table_id {}", col.fk_table_id))?;
            (entry.schema.pk_index, entry.schema.columns[entry.schema.pk_index as usize].type_code)
        };

        if col.fk_col_idx != target_pk_index {
            return Err("FK must reference target PK".into());
        }

        let promoted = get_index_key_type(col.type_code)?;
        if promoted != target_pk_type {
            return Err(format!(
                "FK type mismatch: promoted code {} vs target {}",
                promoted, target_pk_type
            ));
        }
        Ok(())
    }

    // -- Write helpers for system tables -----------------------------------

    fn emit_column_records(&mut self, owner_id: i64, kind: i64, col_defs: &[ColumnDef], weight: i64) {
        let schema = col_tab_schema();
        let mut bb = BatchBuilder::new(schema);
        for (i, cd) in col_defs.iter().enumerate() {
            let pk = pack_column_id(owner_id, i as i64);
            bb.begin_row(pk, 0, weight);
            bb.put_u64(owner_id as u64);
            bb.put_u64(kind as u64);
            bb.put_u64(i as u64);
            bb.put_string(&cd.name);
            bb.put_u64(cd.type_code as u64);
            bb.put_u64(if cd.is_nullable { 1 } else { 0 });
            bb.put_u64(cd.fk_table_id as u64);
            bb.put_u64(cd.fk_col_idx as u64);
            bb.end_row();
        }
        let batch = bb.finish();
        ingest_batch_into(&mut self.sys_columns, &batch);
    }

    fn write_column_records(&mut self, owner_id: i64, kind: i64, col_defs: &[ColumnDef]) {
        self.emit_column_records(owner_id, kind, col_defs, 1);
    }

    fn retract_column_records(&mut self, owner_id: i64, kind: i64, col_defs: &[ColumnDef]) {
        self.emit_column_records(owner_id, kind, col_defs, -1);
    }

    fn write_view_deps(&mut self, vid: i64, dep_ids: &[i64]) {
        let schema = dep_tab_schema();
        let mut bb = BatchBuilder::new(schema);
        for &dep_tid in dep_ids {
            let (pk_lo, pk_hi) = pack_dep_pk(vid, dep_tid);
            bb.begin_row(pk_lo, pk_hi, 1);
            bb.put_u64(vid as u64);
            bb.put_u64(0); // dep_view_id
            bb.put_u64(dep_tid as u64);
            bb.end_row();
        }
        let batch = bb.finish();
        ingest_batch_into(&mut self.sys_view_deps, &batch);
    }

    fn retract_view_deps(&mut self, vid: i64) {
        let schema = dep_tab_schema();
        let mut cursor = match self.sys_view_deps.create_cursor() {
            Ok(c) => c,
            Err(_) => return,
        };
        cursor.cursor.seek(0, vid as u64);

        let end_hi = (vid + 1) as u64;
        let mut bb = BatchBuilder::new(schema);
        while cursor.cursor.valid {
            if cursor.cursor.current_key_hi >= end_hi { break; }
            if cursor.cursor.current_key_hi == vid as u64 && cursor.cursor.current_weight > 0 {
                let dep_vid = cursor_read_u64(&cursor, DEPTAB_COL_DEP_VIEW_ID) as i64;
                let dep_tid = cursor_read_u64(&cursor, DEPTAB_COL_DEP_TABLE_ID) as i64;
                let (pk_lo, pk_hi) = pack_dep_pk(vid, dep_tid);
                bb.begin_row(pk_lo, pk_hi, -1);
                bb.put_u64(vid as u64);
                bb.put_u64(dep_vid as u64);
                bb.put_u64(dep_tid as u64);
                bb.end_row();
            }
            cursor.cursor.advance();
        }
        let batch = bb.finish();
        if batch.count > 0 {
            ingest_batch_into(&mut self.sys_view_deps, &batch);
        }
    }

    fn write_circuit_graph(&mut self, vid: i64, graph: &CircuitGraph) {
        // Nodes
        {
            let schema = circuit_nodes_schema();
            let mut bb = BatchBuilder::new(schema);
            for &(node_id, opcode) in &graph.nodes {
                let (pk_lo, pk_hi) = pack_node_pk(vid, node_id as i64);
                bb.begin_row(pk_lo, pk_hi, 1);
                bb.put_u64(opcode as u64);
                bb.end_row();
            }
            let batch = bb.finish();
            ingest_batch_into(&mut self.sys_circuit_nodes, &batch);
        }

        // Edges
        {
            let schema = circuit_edges_schema();
            let mut bb = BatchBuilder::new(schema);
            for &(edge_id, src, dst, port) in &graph.edges {
                let (pk_lo, pk_hi) = pack_edge_pk(vid, edge_id as i64);
                bb.begin_row(pk_lo, pk_hi, 1);
                bb.put_u64(src as u64);
                bb.put_u64(dst as u64);
                bb.put_u64(port as u64);
                bb.end_row();
            }
            let batch = bb.finish();
            ingest_batch_into(&mut self.sys_circuit_edges, &batch);
        }

        // Sources
        {
            let schema = circuit_sources_schema();
            let mut bb = BatchBuilder::new(schema);
            for &(node_id, table_id) in &graph.sources {
                let (pk_lo, pk_hi) = pack_node_pk(vid, node_id as i64);
                bb.begin_row(pk_lo, pk_hi, 1);
                bb.put_u64(table_id as u64);
                bb.end_row();
            }
            let batch = bb.finish();
            ingest_batch_into(&mut self.sys_circuit_sources, &batch);
        }

        // Params
        {
            let schema = circuit_params_schema();
            let mut bb = BatchBuilder::new(schema);
            for &(node_id, slot, value) in &graph.params {
                let (pk_lo, pk_hi) = pack_param_pk(vid, node_id as i64, slot as i64);
                bb.begin_row(pk_lo, pk_hi, 1);
                bb.put_u64(value as u64);
                bb.put_null(); // str_value (NULL)
                bb.end_row();
            }
            let batch = bb.finish();
            ingest_batch_into(&mut self.sys_circuit_params, &batch);
        }

        // Group cols
        {
            let schema = circuit_group_cols_schema();
            let mut bb = BatchBuilder::new(schema);
            for &(node_id, col_idx) in &graph.group_cols {
                let (pk_lo, pk_hi) = pack_gcol_pk(vid, node_id as i64, col_idx as i64);
                bb.begin_row(pk_lo, pk_hi, 1);
                bb.put_u64(col_idx as u64);
                bb.end_row();
            }
            let batch = bb.finish();
            ingest_batch_into(&mut self.sys_circuit_group_cols, &batch);
        }
    }

    fn retract_circuit_graph(&mut self, _vid: i64) {
        // TODO: scan each circuit system table for rows with this view_id
        // and emit retractions. For now, the rows remain as tombstones.
        // The runtime correctly ignores them because the plan cache is invalidated.
    }

    // -- System table accessor -----------------------------------------------

    /// Map a system table ID to a mutable reference. Returns None for unknown IDs.
    fn sys_table_mut(&mut self, table_id: i64) -> Option<&mut Table> {
        match table_id {
            SCHEMA_TAB_ID => Some(&mut self.sys_schemas),
            TABLE_TAB_ID => Some(&mut self.sys_tables),
            VIEW_TAB_ID => Some(&mut self.sys_views),
            COL_TAB_ID => Some(&mut self.sys_columns),
            IDX_TAB_ID => Some(&mut self.sys_indices),
            DEP_TAB_ID => Some(&mut self.sys_view_deps),
            SEQ_TAB_ID => Some(&mut self.sys_sequences),
            CIRCUIT_NODES_TAB_ID => Some(&mut self.sys_circuit_nodes),
            CIRCUIT_EDGES_TAB_ID => Some(&mut self.sys_circuit_edges),
            CIRCUIT_SOURCES_TAB_ID => Some(&mut self.sys_circuit_sources),
            CIRCUIT_PARAMS_TAB_ID => Some(&mut self.sys_circuit_params),
            CIRCUIT_GROUP_COLS_TAB_ID => Some(&mut self.sys_circuit_group_cols),
            _ => None,
        }
    }

    // -- Server ingestion / scan / seek / flush -----------------------------

    /// Ingest a batch into a table family (unique_pk + store + index projection + hooks).
    /// For system tables: fires hooks after ingestion.
    /// For user tables: delegates to DagEngine::ingest_to_family.
    pub fn ingest_to_family(&mut self, table_id: i64, batch: OwnedBatch) -> Result<(), String> {
        if table_id < FIRST_USER_TABLE_ID {
            // System table: ingest into owned Table, then fire hooks.
            let schema = sys_tab_schema(table_id);
            let table: &mut Table = match table_id {
                SCHEMA_TAB_ID => &mut self.sys_schemas,
                TABLE_TAB_ID => &mut self.sys_tables,
                VIEW_TAB_ID => &mut self.sys_views,
                COL_TAB_ID => &mut self.sys_columns,
                IDX_TAB_ID => &mut self.sys_indices,
                DEP_TAB_ID => &mut self.sys_view_deps,
                SEQ_TAB_ID => &mut self.sys_sequences,
                CIRCUIT_NODES_TAB_ID => &mut self.sys_circuit_nodes,
                CIRCUIT_EDGES_TAB_ID => &mut self.sys_circuit_edges,
                CIRCUIT_SOURCES_TAB_ID => &mut self.sys_circuit_sources,
                CIRCUIT_PARAMS_TAB_ID => &mut self.sys_circuit_params,
                CIRCUIT_GROUP_COLS_TAB_ID => &mut self.sys_circuit_group_cols,
                _ => return Err(format!("Unknown system table_id {}", table_id)),
            };
            // Clone batch before ingest (hooks need to read it after table borrows it)
            let mut batch_for_hooks = batch.clone();
            batch_for_hooks.schema = Some(schema);
            ingest_batch_into(table, &batch);
            self.fire_hooks(table_id, &batch_for_hooks)?;
            Ok(())
        } else {
            // User table: DagEngine handles unique_pk, store ingest, index projection.
            let rc = self.dag.ingest_to_family(table_id, batch);
            if rc < 0 {
                Err(format!("ingest_to_family failed for table_id={} rc={}", table_id, rc))
            } else {
                Ok(())
            }
        }
    }

    /// Ingest a user-table batch and return the effective delta (after unique_pk
    /// dedup).  Used by multi-worker push where the worker needs the effective
    /// batch for later DAG evaluation but does NOT evaluate immediately.
    /// System tables are NOT supported (use `ingest_to_family` for those).
    pub fn ingest_returning_effective(&mut self, table_id: i64, batch: OwnedBatch) -> Result<OwnedBatch, String> {
        if table_id < FIRST_USER_TABLE_ID {
            return Err("ingest_returning_effective not supported for system tables".to_string());
        }
        let (rc, effective_opt) = self.dag.ingest_returning_effective(table_id, batch);
        if rc < 0 {
            return Err(format!("ingest failed for table_id={} rc={}", table_id, rc));
        }
        match effective_opt {
            Some(eff) => Ok(eff),
            None => Err(format!("ingest returned no effective batch for table_id={}", table_id)),
        }
    }

    /// Ingest a batch into a user table AND run the single-worker DAG cascade.
    /// For unique_pk tables, the effective batch (with auto-retractions) is
    /// passed to the DAG evaluator so views see correct deltas.
    /// System tables are NOT supported; use ingest_to_family for those.
    pub fn push_and_evaluate(&mut self, table_id: i64, batch: OwnedBatch) -> Result<(), String> {
        if table_id < FIRST_USER_TABLE_ID {
            return Err("push_and_evaluate not supported for system tables".to_string());
        }

        let (rc, effective_opt) = self.dag.ingest_returning_effective(table_id, batch);
        if rc < 0 {
            return Err(format!("ingest failed for table_id={} rc={}", table_id, rc));
        }

        // Flush the source table
        let _ = self.dag.flush(table_id);

        // Run DAG cascade with the effective batch
        if let Some(effective) = effective_opt {
            if effective.count > 0 {
                self.dag.evaluate_dag(table_id, effective);
            }
        }

        Ok(())
    }

    /// Scan all positive-weight rows from a table.
    pub fn scan_family(&mut self, table_id: i64) -> Result<OwnedBatch, String> {
        let schema = if table_id < FIRST_USER_TABLE_ID {
            sys_tab_schema(table_id)
        } else {
            self.dag.tables.get(&table_id)
                .map(|e| e.schema)
                .ok_or_else(|| format!("Unknown table_id {}", table_id))?
        };
        Ok(self.scan_store(table_id, &schema))
    }

    /// Point lookup by PK. Returns a single-row batch if found, None otherwise.
    pub fn seek_family(&mut self, table_id: i64, pk_lo: u64, pk_hi: u64) -> Result<Option<OwnedBatch>, String> {
        let schema = if table_id < FIRST_USER_TABLE_ID {
            sys_tab_schema(table_id)
        } else {
            self.dag.tables.get(&table_id)
                .map(|e| e.schema)
                .ok_or_else(|| format!("Unknown table_id {}", table_id))?
        };

        // Create cursor and seek
        let cursor_result = if table_id < FIRST_USER_TABLE_ID {
            let table: &mut Table = match table_id {
                SCHEMA_TAB_ID => &mut self.sys_schemas,
                TABLE_TAB_ID => &mut self.sys_tables,
                VIEW_TAB_ID => &mut self.sys_views,
                COL_TAB_ID => &mut self.sys_columns,
                IDX_TAB_ID => &mut self.sys_indices,
                DEP_TAB_ID => &mut self.sys_view_deps,
                SEQ_TAB_ID => &mut self.sys_sequences,
                _ => return Ok(None),
            };
            table.create_cursor()
        } else {
            let entry = self.dag.tables.get(&table_id).unwrap();
            match &entry.handle {
                StoreHandle::Single(ptr) if !ptr.is_null() => {
                    let table = unsafe { &mut **ptr };
                    table.create_cursor()
                }
                StoreHandle::Partitioned(ptr) if !ptr.is_null() => {
                    let ptable = unsafe { &mut **ptr };
                    ptable.create_cursor()
                }
                _ => return Ok(None),
            }
        };

        let mut cursor = match cursor_result {
            Ok(c) => c,
            Err(_) => return Ok(None),
        };

        cursor.cursor.seek(pk_lo, pk_hi);
        if !cursor.cursor.valid { return Ok(None); }
        if cursor.cursor.current_key_lo != pk_lo || cursor.cursor.current_key_hi != pk_hi {
            return Ok(None);
        }
        if cursor.cursor.current_weight <= 0 { return Ok(None); }

        let mut batch = OwnedBatch::with_schema(schema, 1);
        self.copy_cursor_row_to_batch(&cursor, &schema, &mut batch);
        Ok(Some(batch))
    }

    /// Index-assisted lookup: look up by secondary index key, resolve to source row.
    pub fn seek_by_index(&mut self, table_id: i64, col_idx: u32, key_lo: u64, key_hi: u64)
        -> Result<Option<OwnedBatch>, String>
    {
        let entry = self.dag.tables.get(&table_id)
            .ok_or_else(|| format!("Unknown table_id {}", table_id))?;

        // Find matching index circuit
        let ic = entry.index_circuits.iter()
            .find(|ic| ic.col_idx == col_idx)
            .ok_or_else(|| format!("No index on col_idx {} for table {}", col_idx, table_id))?;

        if ic.index_handle.is_null() {
            return Ok(None);
        }

        // Seek in index table
        let idx_table = unsafe { &mut *ic.index_handle };
        let mut cursor = idx_table.create_cursor().map_err(|e| format!("cursor error: {}", e))?;
        cursor.cursor.seek(key_lo, key_hi);
        if !cursor.cursor.valid { return Ok(None); }
        if cursor.cursor.current_key_lo != key_lo || cursor.cursor.current_key_hi != key_hi {
            return Ok(None);
        }
        if cursor.cursor.current_weight <= 0 { return Ok(None); }

        // Index payload column 0 is the source table PK.
        // Read it and resolve to the source row.
        let idx_schema = &ic.index_schema;
        let payload_col_idx = if idx_schema.pk_index == 0 { 1usize } else { 0usize };
        let pk_size = idx_schema.columns[payload_col_idx].size as usize;
        let ptr = cursor.cursor.col_ptr(payload_col_idx, pk_size);
        if ptr.is_null() { return Ok(None); }
        let (src_pk_lo, src_pk_hi) = if pk_size == 16 {
            let lo = u64::from_le_bytes(unsafe { std::slice::from_raw_parts(ptr, 8) }.try_into().unwrap());
            let hi = u64::from_le_bytes(unsafe { std::slice::from_raw_parts(ptr.add(8), 8) }.try_into().unwrap());
            (lo, hi)
        } else {
            let mut buf = [0u8; 8];
            let copy_len = pk_size.min(8);
            unsafe { std::ptr::copy_nonoverlapping(ptr, buf.as_mut_ptr(), copy_len) };
            (u64::from_le_bytes(buf), 0u64)
        };

        self.seek_family(table_id, src_pk_lo, src_pk_hi)
    }

    /// Flush a table's WAL.
    pub fn flush_family(&mut self, table_id: i64) -> Result<(), String> {
        if table_id < FIRST_USER_TABLE_ID {
            if let Some(table) = self.sys_table_mut(table_id) {
                table.flush().map_err(|e| format!("flush error: {}", e))?;
            }
            Ok(())
        } else {
            let rc = self.dag.flush(table_id);
            if rc < 0 {
                Err(format!("flush failed for table_id={} rc={}", table_id, rc))
            } else {
                Ok(())
            }
        }
    }

    /// Worker DDL sync: memonly ingest into system table + fire hooks.
    /// Workers receive DDL deltas from master and need to update their registry
    /// without writing to WAL (master owns durability).
    pub fn ddl_sync(&mut self, table_id: i64, batch: OwnedBatch) -> Result<(), String> {
        if table_id >= FIRST_USER_TABLE_ID {
            return Err("ddl_sync only for system tables".into());
        }
        let table: &mut Table = match table_id {
            SCHEMA_TAB_ID => &mut self.sys_schemas,
            TABLE_TAB_ID => &mut self.sys_tables,
            VIEW_TAB_ID => &mut self.sys_views,
            COL_TAB_ID => &mut self.sys_columns,
            IDX_TAB_ID => &mut self.sys_indices,
            DEP_TAB_ID => &mut self.sys_view_deps,
            SEQ_TAB_ID => &mut self.sys_sequences,
            CIRCUIT_NODES_TAB_ID => &mut self.sys_circuit_nodes,
            CIRCUIT_EDGES_TAB_ID => &mut self.sys_circuit_edges,
            CIRCUIT_SOURCES_TAB_ID => &mut self.sys_circuit_sources,
            CIRCUIT_PARAMS_TAB_ID => &mut self.sys_circuit_params,
            CIRCUIT_GROUP_COLS_TAB_ID => &mut self.sys_circuit_group_cols,
            _ => return Err(format!("Unknown system table_id {}", table_id)),
        };
        // Memonly ingest (no WAL)
        let npc = batch.col_data.len();
        let (ptrs, sizes) = batch.to_region_ptrs();
        let _ = table.ingest_batch_memonly_from_regions(&ptrs, &sizes, batch.count as u32, npc);
        // Fire hooks to update registry
        self.fire_hooks(table_id, &batch)?;
        Ok(())
    }

    /// Raw store ingest: SAL recovery path — no unique_pk, no hooks, no index projection.
    pub fn raw_store_ingest(&mut self, table_id: i64, batch: OwnedBatch) -> Result<(), String> {
        let entry = self.dag.tables.get(&table_id)
            .ok_or_else(|| format!("Unknown table_id {}", table_id))?;
        let npc = batch.col_data.len();
        let (ptrs, sizes) = batch.to_region_ptrs();
        match &entry.handle {
            StoreHandle::Single(ptr) if !ptr.is_null() => {
                let table = unsafe { &mut **ptr };
                let _ = table.ingest_batch_from_regions(&ptrs, &sizes, batch.count as u32, npc);
            }
            StoreHandle::Partitioned(ptr) if !ptr.is_null() => {
                let ptable = unsafe { &mut **ptr };
                let _ = ptable.ingest_batch_from_regions(&ptrs, &sizes, batch.count as u32, npc);
            }
            _ => return Err(format!("Null store for table_id {}", table_id)),
        }
        Ok(())
    }

    // -- Partition management (for multi-worker fork) -------------------------

    /// Set active partition range for user tables.
    pub fn set_active_partitions(&mut self, start: u32, end: u32) {
        self.active_part_start = start;
        self.active_part_end = end;
    }

    /// Close all partitions in user tables (master after fork).
    pub fn close_user_table_partitions(&mut self) {
        for (&tid, entry) in &self.dag.tables {
            if tid < FIRST_USER_TABLE_ID { continue; }
            if let StoreHandle::Partitioned(ptr) = &entry.handle {
                if !ptr.is_null() {
                    let ptable = unsafe { &mut **ptr };
                    ptable.close_all_partitions();
                }
            }
        }
    }

    /// Trim worker partitions to assigned range.
    pub fn trim_worker_partitions(&mut self, start: u32, end: u32) {
        for (&tid, entry) in &self.dag.tables {
            if tid < FIRST_USER_TABLE_ID { continue; }
            if let StoreHandle::Partitioned(ptr) = &entry.handle {
                if !ptr.is_null() {
                    let ptable = unsafe { &mut **ptr };
                    ptable.close_partitions_outside(start, end);
                }
            }
        }
    }

    /// Disable WAL on all user tables (workers use SAL for durability).
    pub fn disable_user_table_wal(&mut self) {
        for (&tid, entry) in &self.dag.tables {
            if tid < FIRST_USER_TABLE_ID { continue; }
            if let StoreHandle::Partitioned(ptr) = &entry.handle {
                if !ptr.is_null() {
                    let ptable = unsafe { &mut **ptr };
                    ptable.set_has_wal(false);
                }
            }
        }
    }

    /// Invalidate all cached plans.
    pub fn invalidate_all_plans(&mut self) {
        self.dag.invalidate_all();
    }

    // -- FK / index metadata queries (for distributed validation) -------------

    /// Number of FK constraints on a table.
    pub fn get_fk_count(&self, table_id: i64) -> usize {
        self.fk_constraints.get(&table_id).map(|v| v.len()).unwrap_or(0)
    }

    /// Get FK constraint at index: (fk_col_idx, target_table_id).
    pub fn get_fk_constraint(&self, table_id: i64, idx: usize) -> Option<(usize, i64)> {
        self.fk_constraints.get(&table_id)
            .and_then(|v| v.get(idx))
            .map(|c| (c.fk_col_idx, c.target_table_id))
    }

    /// Get FK column type code (for promote_to_key in distributed validation).
    pub fn get_fk_col_type(&self, table_id: i64, fk_col_idx: usize) -> u8 {
        self.dag.tables.get(&table_id)
            .map(|e| e.schema.columns[fk_col_idx].type_code)
            .unwrap_or(0)
    }

    /// Number of index circuits on a table.
    pub fn get_index_circuit_count(&self, table_id: i64) -> usize {
        self.dag.tables.get(&table_id)
            .map(|e| e.index_circuits.len())
            .unwrap_or(0)
    }

    /// Get index circuit info at index: (col_idx, is_unique, type_code).
    pub fn get_index_circuit_info(&self, table_id: i64, idx: usize)
        -> Option<(u32, bool, u8)>
    {
        self.dag.tables.get(&table_id)
            .and_then(|e| e.index_circuits.get(idx))
            .map(|ic| {
                let type_code = self.dag.tables.get(&table_id)
                    .map(|e| e.schema.columns[ic.col_idx as usize].type_code)
                    .unwrap_or(0);
                (ic.col_idx, ic.is_unique, type_code)
            })
    }

    /// Get index store handle for a specific column index (for worker has_pk via index).
    pub fn get_index_store_handle(&self, table_id: i64, col_idx: u32) -> *mut Table {
        self.dag.tables.get(&table_id)
            .and_then(|e| e.index_circuits.iter().find(|ic| ic.col_idx == col_idx))
            .map(|ic| ic.index_handle)
            .unwrap_or(std::ptr::null_mut())
    }

    /// Get the SchemaDescriptor for the index circuit at position idx.
    pub fn get_index_circuit_schema(&self, table_id: i64, idx: usize) -> Option<SchemaDescriptor> {
        self.dag.tables.get(&table_id)
            .and_then(|e| e.index_circuits.get(idx))
            .map(|ic| ic.index_schema)
    }

    /// Get column names for a table/view. Cached after first lookup.
    pub fn get_column_names(&mut self, table_id: i64) -> Vec<String> {
        if let Some(names) = self.col_names_cache.get(&table_id) {
            return names.clone();
        }
        let names = match self.read_column_defs(table_id) {
            Ok(defs) => defs.into_iter().map(|cd| cd.name).collect(),
            Err(_) => Vec::new(),
        };
        self.col_names_cache.insert(table_id, names.clone());
        names
    }

    // -- Store handle accessors -----------------------------------------------

    /// Get raw PartitionedTable handle for a user table.
    pub fn get_ptable_handle(&self, table_id: i64) -> Option<*mut PartitionedTable> {
        self.dag.tables.get(&table_id).and_then(|e| {
            match &e.handle {
                StoreHandle::Partitioned(ptr) if !ptr.is_null() => Some(*ptr),
                _ => None,
            }
        })
    }

    /// Get schema descriptor for a table.
    pub fn get_schema_desc(&self, table_id: i64) -> Option<SchemaDescriptor> {
        if table_id < FIRST_USER_TABLE_ID {
            Some(sys_tab_schema(table_id))
        } else {
            self.dag.tables.get(&table_id).map(|e| e.schema)
        }
    }

    /// Get mutable DagEngine pointer (for FFI — returns raw pointer to self.dag).
    pub fn get_dag_ptr(&mut self) -> *mut DagEngine {
        &mut self.dag as *mut DagEngine
    }

    // -- Iteration helpers ----------------------------------------------------

    /// Collect all user table IDs.
    pub fn iter_user_table_ids(&self) -> Vec<i64> {
        self.dag.tables.keys()
            .filter(|&&tid| tid >= FIRST_USER_TABLE_ID)
            .copied()
            .collect()
    }

    /// Get max flushed LSN for a table (for SAL recovery).
    pub fn get_max_flushed_lsn(&self, table_id: i64) -> u64 {
        let entry = match self.dag.tables.get(&table_id) {
            Some(e) => e,
            None => return 0,
        };
        match &entry.handle {
            StoreHandle::Single(ptr) if !ptr.is_null() => {
                let table = unsafe { &**ptr };
                table.current_lsn
            }
            StoreHandle::Partitioned(ptr) if !ptr.is_null() => {
                let ptable = unsafe { &**ptr };
                ptable.current_lsn()
            }
            _ => 0,
        }
    }

    // -- Close engine ------------------------------------------------------

    pub fn close(&mut self) {
        // Flush and close all user tables before clearing DagEngine
        let user_tids: Vec<i64> = self.dag.tables.keys()
            .filter(|&&tid| tid >= FIRST_USER_TABLE_ID)
            .copied()
            .collect();
        for tid in &user_tids {
            if let Some(entry) = self.dag.tables.get(tid) {
                match &entry.handle {
                    StoreHandle::Single(ptr) if !ptr.is_null() => {
                        let table = unsafe { &mut **ptr };
                        let _ = table.flush();
                        // Reclaim the leaked Box (index tables)
                        unsafe { let _ = Box::from_raw(*ptr); }
                    }
                    StoreHandle::Partitioned(ptr) if !ptr.is_null() => {
                        let ptable = unsafe { &mut **ptr };
                        let _ = ptable.flush();
                        ptable.close();
                        // Reclaim the leaked Box
                        unsafe { let _ = Box::from_raw(*ptr); }
                    }
                    _ => {}
                }
            }
        }
        self.dag.close();
        let _ = self.sys_schemas.flush();
        let _ = self.sys_tables.flush();
        let _ = self.sys_views.flush();
        let _ = self.sys_columns.flush();
        let _ = self.sys_indices.flush();
        let _ = self.sys_view_deps.flush();
        let _ = self.sys_sequences.flush();
        let _ = self.sys_circuit_nodes.flush();
        let _ = self.sys_circuit_edges.flush();
        let _ = self.sys_circuit_sources.flush();
        let _ = self.sys_circuit_params.flush();
        let _ = self.sys_circuit_group_cols.flush();
    }

    // -- FK inline validation (single-worker) ------------------------------

    pub fn validate_fk_inline(&self, table_id: i64, batch: &OwnedBatch) -> Result<(), String> {
        let constraints = match self.fk_constraints.get(&table_id) {
            Some(c) if !c.is_empty() => c,
            _ => return Ok(()),
        };

        let entry = self.dag.tables.get(&table_id)
            .ok_or_else(|| format!("Unknown table_id {}", table_id))?;
        let schema = entry.schema;

        for constraint in constraints {
            let col_idx = constraint.fk_col_idx;
            let target_id = constraint.target_table_id;

            let target_entry = self.dag.tables.get(&target_id)
                .ok_or_else(|| format!("FK target table {} not found", target_id))?;

            for row in 0..batch.count {
                if batch.get_weight(row) <= 0 { continue; }

                // Check null
                let null_word = batch.get_null_word(row);
                let payload_col = if col_idx < schema.pk_index as usize {
                    col_idx
                } else {
                    col_idx - 1
                };
                if null_word & (1u64 << payload_col) != 0 { continue; }

                // Promote column value to PK key
                let col_type = schema.columns[col_idx].type_code;
                let (fk_lo, fk_hi) = self.promote_to_pk_key(batch, row, payload_col, col_type);

                // Check if target has this PK
                let has = match &target_entry.handle {
                    StoreHandle::Single(ptr) if !ptr.is_null() => {
                        let table = unsafe { &mut **ptr };
                        table.has_pk(fk_lo, fk_hi)
                    }
                    StoreHandle::Partitioned(ptr) if !ptr.is_null() => {
                        let ptable = unsafe { &mut **ptr };
                        ptable.has_pk(fk_lo, fk_hi)
                    }
                    _ => false,
                };

                if !has {
                    let (sn, tn) = self.id_to_qualified.get(&table_id)
                        .cloned().unwrap_or_default();
                    let col_name = &schema.columns[col_idx].type_code.to_string();
                    let (tsn, ttn) = self.id_to_qualified.get(&target_id)
                        .cloned().unwrap_or_default();
                    return Err(format!(
                        "Foreign Key violation in '{}.{}': value not found in target '{}.{}'",
                        sn, tn, tsn, ttn
                    ));
                }
            }
        }
        Ok(())
    }

    /// Validate unique index constraints (single-worker path).
    /// For each unique index on this table, checks that no positive-weight row
    /// in the batch introduces a duplicate index key.
    ///
    /// For unique_pk tables, UPSERT rows (PK already exists) get special
    /// handling: the old index entry will be retracted by enforce_unique_pk,
    /// so we only reject if the NEW value collides with a DIFFERENT row's entry.
    pub fn validate_unique_indices(&mut self, table_id: i64, batch: &OwnedBatch) -> Result<(), String> {
        let entry = self.dag.tables.get(&table_id)
            .ok_or_else(|| format!("Unknown table_id {}", table_id))?;

        // Quick check: any unique index circuits?
        let has_unique = entry.index_circuits.iter().any(|ic| ic.is_unique);
        if !has_unique { return Ok(()); }

        let schema = entry.schema;
        let pk_index = schema.pk_index as usize;

        for ic in &entry.index_circuits {
            if !ic.is_unique { continue; }
            if ic.index_handle.is_null() { continue; }

            let source_col_idx = ic.col_idx as usize;
            let col_type = schema.columns[source_col_idx].type_code;
            let payload_col = if source_col_idx < pk_index {
                source_col_idx
            } else {
                source_col_idx - 1
            };

            let idx_table = unsafe { &mut *ic.index_handle };

            // Track seen keys for batch-internal duplicate detection
            let mut seen: HashMap<(u64, u64), bool> = HashMap::new();

            for row in 0..batch.count {
                if batch.get_weight(row) <= 0 { continue; }

                // Skip null values
                let null_word = batch.get_null_word(row);
                if null_word & (1u64 << payload_col) != 0 { continue; }

                let row_pk_lo = batch.get_pk_lo(row);
                let row_pk_hi = batch.get_pk_hi(row);

                // Determine if this is an UPSERT (PK already exists in store)
                let is_upsert = if entry.unique_pk {
                    match &entry.handle {
                        StoreHandle::Partitioned(ptr) if !ptr.is_null() => {
                            let ptable = unsafe { &mut **ptr };
                            ptable.has_pk(row_pk_lo, row_pk_hi)
                        }
                        StoreHandle::Single(ptr) if !ptr.is_null() => {
                            let table = unsafe { &mut **ptr };
                            table.has_pk(row_pk_lo, row_pk_hi)
                        }
                        _ => false,
                    }
                } else {
                    false
                };

                // Promote column value to index key
                let (key_lo, key_hi) = self.promote_to_pk_key(batch, row, payload_col, col_type);

                // Batch-internal duplicate check
                if seen.contains_key(&(key_lo, key_hi)) {
                    let col_names = self.get_column_names(table_id);
                    let cname = col_names.get(source_col_idx).map(|s| s.as_str()).unwrap_or("?");
                    return Err(format!(
                        "Unique index violation on column '{}': duplicate in batch", cname
                    ));
                }
                seen.insert((key_lo, key_hi), true);

                // Check index store for existing key
                if idx_table.has_pk(key_lo, key_hi) {
                    if is_upsert {
                        // For UPSERT: the index entry might belong to this same row
                        // (same value being re-inserted). Check the index payload (source PK).
                        // Index schema: PK = index_key, payload[0] = source_pk.
                        let (_net_w, found) = idx_table.retract_pk(key_lo, key_hi);
                        if found {
                            // Read the source PK from the index entry's payload
                            let idx_schema = &ic.index_schema;
                            let pk_payload_col = 0usize; // first (only) payload column
                            let pk_size = idx_schema.columns[if idx_schema.pk_index == 0 { 1 } else { 0 }].size as usize;
                            let ptr = idx_table.found_col_ptr(pk_payload_col, pk_size);
                            if !ptr.is_null() {
                                let (src_pk_lo, src_pk_hi) = if pk_size == 16 {
                                    let lo = u64::from_le_bytes(unsafe { std::slice::from_raw_parts(ptr, 8) }.try_into().unwrap());
                                    let hi = u64::from_le_bytes(unsafe { std::slice::from_raw_parts(ptr.add(8), 8) }.try_into().unwrap());
                                    (lo, hi)
                                } else {
                                    let mut buf = [0u8; 8];
                                    unsafe { std::ptr::copy_nonoverlapping(ptr, buf.as_mut_ptr(), pk_size.min(8)) };
                                    (u64::from_le_bytes(buf), 0u64)
                                };
                                // If the index entry belongs to a DIFFERENT PK, it's a violation
                                if src_pk_lo != row_pk_lo || src_pk_hi != row_pk_hi {
                                    let col_names = self.get_column_names(table_id);
                                    let cname = col_names.get(source_col_idx).map(|s| s.as_str()).unwrap_or("?");
                                    return Err(format!(
                                        "Unique index violation on column '{}'", cname
                                    ));
                                }
                                // Same PK — this is fine, enforce_unique_pk will handle it
                            }
                        }
                    } else {
                        let col_names = self.get_column_names(table_id);
                        let cname = col_names.get(source_col_idx).map(|s| s.as_str()).unwrap_or("?");
                        return Err(format!(
                            "Unique index violation on column '{}'", cname
                        ));
                    }
                }
            }
        }
        Ok(())
    }

    fn promote_to_pk_key(&self, batch: &OwnedBatch, row: usize, payload_col: usize, col_type: u8) -> (u64, u64) {
        match col_type {
            type_code::U128 => {
                let data = batch.get_col_ptr(row, payload_col, 16);
                let lo = u64::from_le_bytes(data[0..8].try_into().unwrap());
                let hi = u64::from_le_bytes(data[8..16].try_into().unwrap());
                (lo, hi)
            }
            type_code::U64 | type_code::I64 => {
                let data = batch.get_col_ptr(row, payload_col, 8);
                let lo = u64::from_le_bytes(data[0..8].try_into().unwrap());
                (lo, 0)
            }
            type_code::U32 | type_code::I32 => {
                let data = batch.get_col_ptr(row, payload_col, 4);
                let lo = u32::from_le_bytes(data[0..4].try_into().unwrap()) as u64;
                (lo, 0)
            }
            type_code::U16 | type_code::I16 => {
                let data = batch.get_col_ptr(row, payload_col, 2);
                let lo = u16::from_le_bytes(data[0..2].try_into().unwrap()) as u64;
                (lo, 0)
            }
            type_code::U8 | type_code::I8 => {
                let data = batch.get_col_ptr(row, payload_col, 1);
                (data[0] as u64, 0)
            }
            _ => (0, 0),
        }
    }
}

// ---------------------------------------------------------------------------
// Public types used by FFI and tests
// ---------------------------------------------------------------------------

/// Column definition for create_table.
#[derive(Clone, Debug)]
pub struct ColumnDef {
    pub name: String,
    pub type_code: u8,
    pub is_nullable: bool,
    pub fk_table_id: i64,
    pub fk_col_idx: u32,
}

/// Circuit graph for create_view.
pub struct CircuitGraph {
    pub nodes: Vec<(i32, i32)>,           // (node_id, opcode)
    pub edges: Vec<(i32, i32, i32, i32)>, // (edge_id, src, dst, port)
    pub sources: Vec<(i32, i64)>,         // (node_id, table_id)
    pub params: Vec<(i32, i32, i64)>,     // (node_id, slot, value)
    pub group_cols: Vec<(i32, i32)>,      // (node_id, col_idx)
    pub output_col_defs: Vec<(String, u8)>, // (name, type_code)
    pub dependencies: Vec<i64>,           // source table_ids
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_dir(name: &str) -> String {
        crate::util::raise_fd_limit_for_tests();
        let path = format!("/tmp/gnitz_catalog_test_{}", name);
        let _ = fs::remove_dir_all(&path);
        path
    }

    fn u64_col_def(name: &str) -> ColumnDef {
        ColumnDef { name: name.into(), type_code: type_code::U64, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 }
    }
    fn i64_col_def(name: &str) -> ColumnDef {
        ColumnDef { name: name.into(), type_code: type_code::I64, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 }
    }
    fn u8_col_def(name: &str) -> ColumnDef {
        ColumnDef { name: name.into(), type_code: type_code::U8, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 }
    }
    fn u16_col_def(name: &str) -> ColumnDef {
        ColumnDef { name: name.into(), type_code: type_code::U16, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 }
    }
    fn i32_col_def(name: &str) -> ColumnDef {
        ColumnDef { name: name.into(), type_code: type_code::I32, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 }
    }
    fn str_col_def(name: &str) -> ColumnDef {
        ColumnDef { name: name.into(), type_code: type_code::STRING, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 }
    }
    fn u128_col_def(name: &str) -> ColumnDef {
        ColumnDef { name: name.into(), type_code: type_code::U128, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 }
    }

    fn count_records(table: &mut Table) -> usize {
        let mut count = 0;
        if let Ok(cursor) = table.create_cursor() {
            let mut c = cursor;
            while c.cursor.valid {
                if c.cursor.current_weight > 0 { count += 1; }
                c.cursor.advance();
            }
        }
        count
    }

    /// Build a passthrough (SELECT *) view graph over source_table_id.
    /// Node IDs start at 1, edge IDs start at 1.
    fn make_passthrough_graph(source_table_id: i64, output_cols: &[(String, u8)]) -> CircuitGraph {
        // Node 1: SCAN_TRACE (opcode 11) — input delta (source table_id=0)
        // Node 2: INTEGRATE (opcode 7) — sink
        // Edge 1: node 1 → node 2, port 0 (PORT_IN)
        CircuitGraph {
            nodes: vec![(1, 11), (2, 7)],           // SCAN_TRACE, INTEGRATE
            edges: vec![(1, 1, 2, 0)],              // edge 1: src=1 dst=2 port=0
            sources: vec![(1, 0)],                   // node 1 reads from table_id=0 (primary delta)
            params: vec![],
            group_cols: vec![],
            output_col_defs: output_cols.to_vec(),
            dependencies: vec![source_table_id],
        }
    }

    // ── test_identifiers ─────────────────────────────────────────────────

    #[test]
    fn test_identifiers() {
        // Valid names
        for name in &["orders", "Orders123", "my_table", "a", "A1_b2", "1a", "99_problems"] {
            assert!(validate_user_identifier(name).is_ok(), "Rejected valid: {}", name);
        }
        // Invalid names
        for name in &["_private", "_", "_system", "__init__", "", "has space",
                       "has-dash", "has.dot", "has@", "table$"] {
            assert!(validate_user_identifier(name).is_err(), "Accepted invalid: {}", name);
        }
        // Qualified name parsing
        assert_eq!(parse_qualified_name("orders", "public"), ("public", "orders"));
        assert_eq!(parse_qualified_name("sales.orders", "public"), ("sales", "orders"));
        // Boundary slicing
        assert_eq!(parse_qualified_name(".table", "def"), ("", "table"));
        assert_eq!(parse_qualified_name("schema.", "def"), ("schema", ""));
    }

    // ── test_bootstrap ───────────────────────────────────────────────────

    #[test]
    fn test_bootstrap() {
        let dir = temp_dir("bootstrap");
        let mut engine = CatalogEngine::open(&dir).unwrap();

        // System schemas
        assert!(engine.has_schema("_system"));
        assert!(engine.has_schema("public"));

        // System tables (12 expected)
        assert_eq!(count_records(&mut engine.sys_tables), 12);
        // System columns (46 expected for all 12 system tables)
        assert_eq!(count_records(&mut engine.sys_columns), 46);
        // Sequences (4 expected)
        assert_eq!(count_records(&mut engine.sys_sequences), 4);
        // Schemas (2 expected: _system, public)
        assert_eq!(count_records(&mut engine.sys_schemas), 2);

        // Allocator state
        assert_eq!(engine.next_table_id, FIRST_USER_TABLE_ID);
        assert_eq!(engine.next_schema_id, FIRST_USER_SCHEMA_ID);

        engine.close();
        drop(engine); // Release WAL locks before re-open

        // Idempotent re-open: should not duplicate records
        let mut engine2 = CatalogEngine::open(&dir).unwrap();
        assert_eq!(count_records(&mut engine2.sys_schemas), 2);
        assert_eq!(count_records(&mut engine2.sys_tables), 12);
        engine2.close();

        let _ = fs::remove_dir_all(&dir);
    }

    // ── test_ddl ─────────────────────────────────────────────────────────

    #[test]
    fn test_ddl() {
        let dir = temp_dir("ddl");
        let mut engine = CatalogEngine::open(&dir).unwrap();

        let init_schemas = count_records(&mut engine.sys_schemas);
        let init_tables = count_records(&mut engine.sys_tables);
        let init_cols = count_records(&mut engine.sys_columns);

        // Schema creation
        engine.create_schema("sales").unwrap();
        assert!(engine.has_schema("sales"));
        assert_eq!(count_records(&mut engine.sys_schemas), init_schemas + 1);
        assert!(engine.create_schema("sales").is_err()); // duplicate

        // Table creation
        let cols = vec![u64_col_def("id"), str_col_def("name")];
        let tid = engine.create_table("sales.orders", &cols, 0, true).unwrap();
        assert!(engine.has_id(tid));
        assert_eq!(count_records(&mut engine.sys_tables), init_tables + 1);
        assert_eq!(count_records(&mut engine.sys_columns), init_cols + 2);

        // Drop table (retractions)
        engine.drop_table("sales.orders").unwrap();
        assert!(!engine.has_id(tid));
        assert_eq!(count_records(&mut engine.sys_tables), init_tables);
        assert_eq!(count_records(&mut engine.sys_columns), init_cols);

        // System table drop should fail (identifier starts with '_')
        assert!(engine.drop_table("_system._columns").is_err());

        // Drop schema
        engine.create_schema("temp").unwrap();
        engine.drop_schema("temp").unwrap();
        assert!(!engine.has_schema("temp"));

        engine.close();
        let _ = fs::remove_dir_all(&dir);
    }

    // ── test_edge_cases (26 cases) ────────────────────────────────────────

    #[test]
    fn test_edge_cases() {
        let dir = temp_dir("edge_cases");
        let mut engine = CatalogEngine::open(&dir).unwrap();
        let cols = vec![u64_col_def("id")];

        // 1. Drop non-existent schema
        assert!(engine.drop_schema("nonexistent").is_err());
        // 2. Create table in non-existent schema
        assert!(engine.create_table("nonexistent.tbl", &cols, 0, true).is_err());
        // 3. Drop non-existent table
        assert!(engine.drop_table("public.nonexistent").is_err());
        // 4. Duplicate table
        engine.create_table("public.tbl1", &cols, 0, true).unwrap();
        assert!(engine.create_table("public.tbl1", &cols, 0, true).is_err());

        // 5. Drop non-empty schema
        engine.create_schema("my_schema").unwrap();
        engine.create_table("my_schema.tbl2", &cols, 0, true).unwrap();
        assert!(engine.drop_schema("my_schema").is_err());
        // 6. Drop table then drop schema
        engine.drop_table("my_schema.tbl2").unwrap();
        engine.drop_schema("my_schema").unwrap();
        assert!(!engine.has_schema("my_schema"));

        // 7. Unqualified name defaults to public
        let tid7 = engine.create_table("tbl3", &cols, 0, true).unwrap();
        assert!(engine.get_by_name("public", "tbl3").is_some());
        engine.drop_table("public.tbl3").unwrap();

        // 8. Unqualified drop
        engine.create_table("public.tbl4", &cols, 0, true).unwrap();
        engine.drop_table("tbl4").unwrap();
        assert!(engine.get_by_name("public", "tbl4").is_none());

        // 9. Invalid PK type (STRING)
        assert!(engine.create_table("public.bad_pk",
            &[ColumnDef { name: "id".into(), type_code: type_code::STRING, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 }],
            0, true).is_err());

        // 10. Too many columns (> 64)
        let many: Vec<ColumnDef> = (0..65).map(|i| u64_col_def(&format!("c{}", i))).collect();
        assert!(engine.create_table("public.too_many", &many, 0, true).is_err());

        // 11. Drop system schema
        assert!(engine.drop_schema("_system").is_err());

        // 12. PK index out of bounds
        assert!(engine.create_table("public.bad_idx", &cols, 5, true).is_err());

        // 13. Recreated schema gets new ID
        engine.create_schema("temp").unwrap();
        let sid1 = engine.get_schema_id("temp");
        engine.drop_schema("temp").unwrap();
        engine.create_schema("temp").unwrap();
        let sid2 = engine.get_schema_id("temp");
        assert_ne!(sid1, sid2);
        engine.drop_schema("temp").unwrap();

        // 14. Recreated table gets new ID
        let tid14a = engine.create_table("public.tbl_rc", &cols, 0, true).unwrap();
        engine.drop_table("public.tbl_rc").unwrap();
        let tid14b = engine.create_table("public.tbl_rc", &cols, 0, true).unwrap();
        assert_ne!(tid14a, tid14b);
        engine.drop_table("public.tbl_rc").unwrap();

        // 15. U128 PK support
        let tid15 = engine.create_table("public.u128t", &[u128_col_def("uuid_pk"), str_col_def("data")], 0, true).unwrap();
        let s15 = engine.get_schema(tid15).unwrap();
        assert_eq!(s15.columns[0].type_code, type_code::U128);
        engine.drop_table("public.u128t").unwrap();

        // 18. schema_is_empty
        engine.create_schema("empty_test").unwrap();
        assert!(engine.schema_is_empty("empty_test"));
        engine.create_table("empty_test.tbl", &cols, 0, true).unwrap();
        assert!(!engine.schema_is_empty("empty_test"));
        engine.drop_table("empty_test.tbl").unwrap();
        assert!(engine.schema_is_empty("empty_test"));
        engine.drop_schema("empty_test").unwrap();

        // 19. Case sensitivity
        engine.create_table("public.CaseTest", &cols, 0, true).unwrap();
        engine.create_table("public.casetest", &cols, 0, true).unwrap();
        assert!(engine.get_by_name("public", "CaseTest").is_some());
        assert!(engine.get_by_name("public", "casetest").is_some());
        engine.drop_table("public.CaseTest").unwrap();
        engine.drop_table("public.casetest").unwrap();

        // 24. Invalid schema ID lookup
        assert_eq!(engine.get_schema_name_by_id(999999), "");
        assert_eq!(engine.get_schema_id("nonexistent"), -1);

        engine.close();
        let _ = fs::remove_dir_all(&dir);
    }

    // ── test_unique_pk_metadata ──────────────────────────────────────────

    #[test]
    fn test_unique_pk_metadata() {
        let dir = temp_dir("unique_pk_meta");
        let cols = vec![u64_col_def("id"), str_col_def("val")];

        {
            let mut engine = CatalogEngine::open(&dir).unwrap();
            engine.create_schema("sales").unwrap();
            // default = unique_pk=true
            let tid1 = engine.create_table("sales.u_default", &cols, 0, true).unwrap();
            assert!(engine.is_unique_pk(tid1));
            // explicit false
            let tid2 = engine.create_table("sales.u_off", &cols, 0, false).unwrap();
            assert!(!engine.is_unique_pk(tid2));
            // For restart test
            engine.create_table("sales.u_restart", &cols, 0, true).unwrap();
            engine.close();
        }

        // Verify survival across restart
        {
            let mut engine = CatalogEngine::open(&dir).unwrap();
            let tid_restart = engine.get_by_name("sales", "u_restart").unwrap();
            assert!(engine.is_unique_pk(tid_restart));
            let tid_off = engine.get_by_name("sales", "u_off").unwrap();
            assert!(!engine.is_unique_pk(tid_off));
            engine.close();
        }

        let _ = fs::remove_dir_all(&dir);
    }

    // ── test_restart (with data + sequence recovery) ─────────────────────

    #[test]
    fn test_restart_full() {
        let dir = temp_dir("restart_full");
        let cols = vec![u64_col_def("id"), str_col_def("name")];
        let first_tid;

        {
            let mut engine = CatalogEngine::open(&dir).unwrap();
            engine.create_schema("marketing").unwrap();
            first_tid = engine.create_table("marketing.products", &cols, 0, true).unwrap();

            // Dropped entities should not reappear
            engine.create_schema("trash").unwrap();
            engine.create_table("trash.items", &cols, 0, true).unwrap();
            engine.drop_table("trash.items").unwrap();
            engine.drop_schema("trash").unwrap();

            engine.close();
        }

        {
            let mut engine = CatalogEngine::open(&dir).unwrap();
            assert!(engine.has_schema("marketing"));
            assert!(engine.get_by_name("marketing", "products").is_some());
            // Dropped should stay gone
            assert!(!engine.has_schema("trash"));
            assert!(engine.get_by_name("trash", "items").is_none());
            // Schema layout rebuilt correctly
            let tid = engine.get_by_name("marketing", "products").unwrap();
            let schema = engine.get_schema(tid).unwrap();
            assert_eq!(schema.num_columns, 2);
            // Sequence recovery: new table should get higher ID
            let new_tid = engine.create_table("marketing.other", &cols, 0, true).unwrap();
            assert!(new_tid > first_tid, "Allocator sequence recovery failed: {} <= {}", new_tid, first_tid);
            engine.close();
        }

        let _ = fs::remove_dir_all(&dir);
    }

    // ── test_index_functional_and_fanout ─────────────────────────────────

    /// Regression: verify long strings (> 12 bytes, out-of-line blob) survive
    /// restart via copy_cursor_row_to_batch blob offset rewriting.
    #[test]
    fn test_restart_long_strings() {
        let dir = temp_dir("restart_long_str");
        let long_name = "this_is_a_very_long_table_name_exceeding_inline_threshold";
        assert!(long_name.len() > crate::schema::SHORT_STRING_THRESHOLD);

        {
            let mut engine = CatalogEngine::open(&dir).unwrap();
            engine.create_schema("longtest").unwrap();
            let cols = vec![u64_col_def("id"), str_col_def(long_name)];
            engine.create_table("longtest.tbl", &cols, 0, true).unwrap();
            engine.close();
            drop(engine);
        }
        {
            let mut engine = CatalogEngine::open(&dir).unwrap();
            assert!(engine.get_by_name("longtest", "tbl").is_some());
            // Verify column name survived out-of-line blob round-trip
            let tid = engine.get_by_name("longtest", "tbl").unwrap();
            let col_defs = engine.read_column_defs(tid).unwrap();
            assert_eq!(col_defs.len(), 2);
            assert_eq!(col_defs[1].name, long_name, "Long column name corrupted after restart");
            engine.close();
        }
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_index_creation_and_backfill() {
        let dir = temp_dir("index_backfill");
        let mut engine = CatalogEngine::open(&dir).unwrap();

        let cols = vec![u64_col_def("id"), i64_col_def("val")];
        let tid = engine.create_table("public.tfanout", &cols, 0, true).unwrap();

        // Ingest 5 rows
        let schema = engine.get_schema(tid).unwrap();
        let mut bb = BatchBuilder::new(schema);
        for i in 0..5u64 {
            bb.begin_row(i, 0, 1);
            bb.put_u64((i * 100) as u64);
            bb.end_row();
        }
        let batch = bb.finish();
        engine.dag.ingest_to_family(tid, batch);
        let _ = engine.dag.flush(tid);

        // Create index
        let idx_id = engine.create_index("public.tfanout", "val", false).unwrap();
        assert!(engine.has_index_by_name("public__tfanout__idx_val"));

        // Drop index
        engine.drop_index("public__tfanout__idx_val").unwrap();
        assert!(!engine.has_index_by_name("public__tfanout__idx_val"));

        engine.close();
        let _ = fs::remove_dir_all(&dir);
    }

    // ── test_fk_referential_integrity ────────────────────────────────────

    #[test]
    fn test_fk_referential_integrity() {
        let dir = temp_dir("fk_integrity");
        let mut engine = CatalogEngine::open(&dir).unwrap();

        // Parent table
        let parent_tid = engine.create_table("public.parents", &[u64_col_def("pid")], 0, true).unwrap();

        // Child table with FK
        let child_cols = vec![
            u64_col_def("cid"),
            ColumnDef { name: "pid_fk".into(), type_code: type_code::U64, is_nullable: false,
                        fk_table_id: parent_tid, fk_col_idx: 0 },
        ];
        let child_tid = engine.create_table("public.children", &child_cols, 0, true).unwrap();

        // Insert valid parent
        let parent_schema = engine.get_schema(parent_tid).unwrap();
        let mut pbb = BatchBuilder::new(parent_schema);
        pbb.begin_row(10, 0, 1);
        pbb.end_row();
        let pbatch = pbb.finish();
        engine.dag.ingest_to_family(parent_tid, pbatch);
        let _ = engine.dag.flush(parent_tid);

        // Insert valid child (FK=10 exists in parent)
        let child_schema = engine.get_schema(child_tid).unwrap();
        let mut cbb = BatchBuilder::new(child_schema);
        cbb.begin_row(1, 0, 1);
        cbb.put_u64(10); // valid FK
        cbb.end_row();
        let cbatch = cbb.finish();
        assert!(engine.validate_fk_inline(child_tid, &cbatch).is_ok());

        // Insert invalid child (FK=99, not in parent)
        let mut cbb2 = BatchBuilder::new(child_schema);
        cbb2.begin_row(2, 0, 1);
        cbb2.put_u64(99); // invalid FK
        cbb2.end_row();
        let cbatch2 = cbb2.finish();
        assert!(engine.validate_fk_inline(child_tid, &cbatch2).is_err());

        engine.close();
        let _ = fs::remove_dir_all(&dir);
    }

    // ── test_fk_nullability_and_retractions ──────────────────────────────

    #[test]
    fn test_fk_nullability_and_retractions() {
        let dir = temp_dir("fk_null");
        let mut engine = CatalogEngine::open(&dir).unwrap();

        let parent_tid = engine.create_table("public.p", &[u64_col_def("id")], 0, true).unwrap();
        let child_cols = vec![
            u64_col_def("id"),
            ColumnDef { name: "pid_fk".into(), type_code: type_code::U64, is_nullable: true,
                        fk_table_id: parent_tid, fk_col_idx: 0 },
        ];
        let child_tid = engine.create_table("public.c", &child_cols, 0, true).unwrap();

        // NULL FK should be allowed even if parent is empty
        let child_schema = engine.get_schema(child_tid).unwrap();
        let mut bb = BatchBuilder::new(child_schema);
        bb.begin_row(1, 0, 1);
        bb.put_null();
        bb.end_row();
        let batch = bb.finish();
        assert!(engine.validate_fk_inline(child_tid, &batch).is_ok());

        // Retraction (weight -1) should not trigger FK check
        let mut bb2 = BatchBuilder::new(child_schema);
        bb2.begin_row(2, 0, -1);
        bb2.put_u64(999); // non-existent, but weight is negative
        bb2.end_row();
        let batch2 = bb2.finish();
        assert!(engine.validate_fk_inline(child_tid, &batch2).is_ok());

        engine.close();
        let _ = fs::remove_dir_all(&dir);
    }

    // ── test_fk_protections ──────────────────────────────────────────────

    #[test]
    fn test_fk_drop_protections() {
        let dir = temp_dir("fk_prot");
        let mut engine = CatalogEngine::open(&dir).unwrap();

        let parent_tid = engine.create_table("public.parent", &[u64_col_def("pid")], 0, true).unwrap();
        let child_cols = vec![
            u64_col_def("cid"),
            ColumnDef { name: "pid_fk".into(), type_code: type_code::U64, is_nullable: false,
                        fk_table_id: parent_tid, fk_col_idx: 0 },
        ];
        engine.create_table("public.child", &child_cols, 0, true).unwrap();

        // Cannot drop parent (referenced by child)
        assert!(engine.drop_table("public.parent").is_err());

        // Cannot drop auto-generated FK index
        let idx_name = make_fk_index_name("public", "child", "pid_fk");
        assert!(engine.drop_index(&idx_name).is_err());

        // Drop child first, then parent succeeds
        engine.drop_table("public.child").unwrap();
        engine.drop_table("public.parent").unwrap();

        engine.close();
        let _ = fs::remove_dir_all(&dir);
    }

    // ── test_fk_invalid_targets ──────────────────────────────────────────

    #[test]
    fn test_fk_invalid_targets() {
        let dir = temp_dir("fk_invalid");
        let mut engine = CatalogEngine::open(&dir).unwrap();

        let parent_tid = engine.create_table("public.p",
            &[u64_col_def("pk"), i64_col_def("other")], 0, true).unwrap();

        // FK targeting non-PK column (col_idx=1) should fail
        let bad_cols = vec![
            u64_col_def("pk"),
            ColumnDef { name: "fk".into(), type_code: type_code::I64, is_nullable: false,
                        fk_table_id: parent_tid, fk_col_idx: 1 },
        ];
        assert!(engine.create_table("public.c_bad", &bad_cols, 0, true).is_err());

        engine.close();
        let _ = fs::remove_dir_all(&dir);
    }

    // ── test_fk_self_reference ───────────────────────────────────────────

    #[test]
    fn test_fk_self_reference() {
        let dir = temp_dir("fk_self");
        let mut engine = CatalogEngine::open(&dir).unwrap();

        // Self-referential table: employees.mgr_id -> employees.emp_id
        let next_tid = engine.next_table_id;
        let emp_cols = vec![
            u64_col_def("emp_id"),
            ColumnDef { name: "mgr_id".into(), type_code: type_code::U64, is_nullable: true,
                        fk_table_id: next_tid, fk_col_idx: 0 },
        ];
        let emp_tid = engine.create_table("public.employees", &emp_cols, 0, true).unwrap();
        assert_eq!(emp_tid, next_tid);

        engine.close();
        let _ = fs::remove_dir_all(&dir);
    }

    // ── test_view_backfill_simple ────────────────────────────────────────

    #[test]
    fn test_view_backfill_simple() {
        let dir = temp_dir("view_backfill");
        let mut engine = CatalogEngine::open(&dir).unwrap();

        let cols = vec![u64_col_def("id"), i64_col_def("val")];
        let tid = engine.create_table("public.t", &cols, 0, true).unwrap();

        // Ingest 5 rows
        let schema = engine.get_schema(tid).unwrap();
        let mut bb = BatchBuilder::new(schema);
        for i in 0..5u64 {
            bb.begin_row(i, 0, 1);
            bb.put_u64((i * 10) as u64);
            bb.end_row();
        }
        engine.dag.ingest_to_family(tid, bb.finish());
        let _ = engine.dag.flush(tid);

        // Create passthrough view
        let out_cols = vec![
            ("id".to_string(), type_code::U64),
            ("val".to_string(), type_code::I64),
        ];
        let graph = make_passthrough_graph(tid, &out_cols);
        let vid = engine.create_view("public.v", &graph, "").unwrap();

        // Verify backfill: view should have 5 rows
        let view_entry = engine.dag.tables.get(&vid).unwrap();
        let view_schema = view_entry.schema;
        let batch = engine.scan_store(vid, &view_schema);
        assert_eq!(batch.count, 5, "View backfill: expected 5 rows, got {}", batch.count);

        engine.close();
        let _ = fs::remove_dir_all(&dir);
    }

    // ── test_view_backfill_on_restart ────────────────────────────────────

    #[test]
    fn test_view_backfill_on_restart() {
        let dir = temp_dir("view_restart");
        let cols = vec![u64_col_def("id"), i64_col_def("val")];
        let out_cols = vec![
            ("id".to_string(), type_code::U64),
            ("val".to_string(), type_code::I64),
        ];

        {
            let mut engine = CatalogEngine::open(&dir).unwrap();
            let tid = engine.create_table("public.t", &cols, 0, true).unwrap();
            let graph = make_passthrough_graph(tid, &out_cols);
            engine.create_view("public.v", &graph, "").unwrap();

            // Ingest data AFTER view creation (stored in base table)
            let schema = engine.get_schema(tid).unwrap();
            let mut bb = BatchBuilder::new(schema);
            for i in 0..5u64 {
                bb.begin_row(i, 0, 1);
                bb.put_u64((i * 10) as u64);
                bb.end_row();
            }
            engine.dag.ingest_to_family(tid, bb.finish());
            let _ = engine.dag.flush(tid);
            engine.close();
        }

        // Re-open: view should be backfilled from base table data
        {
            let mut engine = CatalogEngine::open(&dir).unwrap();
            let vid = engine.get_by_name("public", "v").unwrap();
            let view_entry = engine.dag.tables.get(&vid).unwrap();
            let view_schema = view_entry.schema;
            let batch = engine.scan_store(vid, &view_schema);
            assert_eq!(batch.count, 5, "View backfill on restart: expected 5, got {}", batch.count);
            engine.close();
        }

        let _ = fs::remove_dir_all(&dir);
    }

    // ── test_view_on_view_backfill_on_restart ────────────────────────────

    #[test]
    fn test_view_on_view_backfill() {
        let dir = temp_dir("view_on_view");
        let cols = vec![u64_col_def("id"), i64_col_def("val")];
        let out_cols = vec![
            ("id".to_string(), type_code::U64),
            ("val".to_string(), type_code::I64),
        ];

        {
            let mut engine = CatalogEngine::open(&dir).unwrap();
            let tid = engine.create_table("public.t", &cols, 0, true).unwrap();

            // Ingest data
            let schema = engine.get_schema(tid).unwrap();
            let mut bb = BatchBuilder::new(schema);
            for v in &[5u64, 20, 50, 80, 100] {
                bb.begin_row(*v, 0, 1);
                bb.put_u64(*v);
                bb.end_row();
            }
            engine.dag.ingest_to_family(tid, bb.finish());
            let _ = engine.dag.flush(tid);

            let graph1 = make_passthrough_graph(tid, &out_cols);
            let v1_id = engine.create_view("public.v1", &graph1, "").unwrap();

            let graph2 = make_passthrough_graph(v1_id, &out_cols);
            let v2_id = engine.create_view("public.v2", &graph2, "").unwrap();

            // Check depths
            assert_eq!(engine.get_depth(v1_id), 1);
            assert_eq!(engine.get_depth(v2_id), 2);

            engine.close();
        }

        // Re-open: both views should be backfilled
        {
            let mut engine = CatalogEngine::open(&dir).unwrap();
            let v1_id = engine.get_by_name("public", "v1").unwrap();
            let v2_id = engine.get_by_name("public", "v2").unwrap();
            let v1_schema = engine.dag.tables.get(&v1_id).unwrap().schema;
            let v2_schema = engine.dag.tables.get(&v2_id).unwrap().schema;
            let b1 = engine.scan_store(v1_id, &v1_schema);
            let b2 = engine.scan_store(v2_id, &v2_schema);
            assert_eq!(b1.count, 5, "V1 restart: expected 5, got {}", b1.count);
            assert_eq!(b2.count, 5, "V2 restart: expected 5, got {}", b2.count);
            engine.close();
        }

        let _ = fs::remove_dir_all(&dir);
    }

    // ── Additional edge cases ───────────────────────────────────────────

    #[test]
    fn test_edge_cases_extended() {
        let dir = temp_dir("edge_ext");
        let mut engine = CatalogEngine::open(&dir).unwrap();
        let cols = vec![u64_col_def("id")];

        // #16. Multiple dots in qualified name — second part contains dot
        assert!(engine.create_table("public.schema.tbl", &cols, 0, true).is_err());

        // #17. get_by_name on non-existent returns None
        assert!(engine.get_by_name("public", "nonexistent").is_none());

        // #20. has_id / get_schema for valid and invalid IDs
        let tid = engine.create_table("public.reg_test", &cols, 0, true).unwrap();
        assert!(engine.has_id(tid));
        assert!(engine.get_schema(tid).is_some());
        assert!(!engine.has_id(999999));
        assert!(engine.get_schema(999999).is_none());
        engine.drop_table("public.reg_test").unwrap();

        // #26. Creating a user table in _system schema should fail
        // (_system identifier starts with '_' → rejected by validate_user_identifier)
        assert!(engine.create_table("_system.new_tbl", &cols, 0, true).is_err());

        engine.close();
        let _ = fs::remove_dir_all(&dir);
    }

    // ── test_enforce_unique_pk ───────────────────────────────────────────

    /// Test unique_pk enforcement via a single-partition Table (system table range).
    /// DagEngine.enforce_unique_pk only operates on Single stores; PartitionedTable
    /// unique_pk is handled by the ingest layer. E2E test_unique_pk.py
    /// covers the full-stack partitioned case.
    #[test]
    fn test_enforce_unique_pk() {
        let dir = temp_dir("enforce_upk");

        // Create a Table directly (Single store) to test enforce_unique_pk
        ensure_dir(&dir).unwrap();
        let schema = make_schema(&[u64_col(), u64_col()], 0); // id (PK), val
        let tdir = format!("{}/upk_table", dir);
        let mut table = Table::new(&tdir, "upk", schema, 100, SYS_TABLE_ARENA, false).unwrap();

        let mut dag = DagEngine::new();
        let table_ptr = &mut table as *mut Table;
        dag.register_table(100, StoreHandle::Single(table_ptr), schema, 0, true, tdir.clone());

        let make_row = |pk: u64, val: u64, w: i64| -> OwnedBatch {
            let mut bb = BatchBuilder::new(schema);
            bb.begin_row(pk, 0, w);
            bb.put_u64(val);
            bb.end_row();
            bb.finish()
        };

        let scan = |table: &mut Table| -> usize {
            let cursor = table.create_cursor().unwrap();
            let mut c = cursor;
            let mut count = 0;
            while c.cursor.valid {
                if c.cursor.current_weight > 0 { count += 1; }
                c.cursor.advance();
            }
            count
        };

        // ST1: insert new PK
        dag.ingest_to_family(100, make_row(1, 10, 1));
        assert_eq!(scan(&mut table), 1);

        // ST2: insert different PK
        dag.ingest_to_family(100, make_row(2, 20, 1));
        assert_eq!(scan(&mut table), 2);

        // ST3: intra-batch duplicate — last value wins (enforce_unique_pk
        // handles intra-batch dedup correctly)
        let mut bb = BatchBuilder::new(schema);
        bb.begin_row(5, 0, 1); bb.put_u64(10); bb.end_row();
        bb.begin_row(5, 0, 1); bb.put_u64(20); bb.end_row();
        dag.ingest_to_family(100, bb.finish());
        assert_eq!(scan(&mut table), 3, "Intra-batch dup: PK=5 has 1 net row");

        // ST4: intra-batch insert then delete cancel each other
        let mut bb2 = BatchBuilder::new(schema);
        bb2.begin_row(6, 0, 1); bb2.put_u64(10); bb2.end_row();
        bb2.begin_row(6, 0, -1); bb2.put_u64(10); bb2.end_row();
        dag.ingest_to_family(100, bb2.finish());
        // PK=6 cancelled, existing PKs (1,2,5) remain
        assert_eq!(scan(&mut table), 3, "Insert+delete cancel: PK=6 not added");

        // Note: cross-batch upsert (retract stored row + insert new) requires
        // the _enforce_unique_pk path which emits the stored retraction.
        // DagEngine.enforce_unique_pk calls retract_pk but doesn't emit
        // the retraction into the effective batch. Full-stack upsert is
        // tested by E2E test_unique_pk.py (8 tests).

        dag.close();
        let _ = fs::remove_dir_all(&dir);
    }

    // ── test_orphaned_metadata_recovery ─────────────────────────────────

    #[test]
    fn test_orphaned_metadata_recovery() {
        let dir = temp_dir("orphaned");

        // First open: inject an index record pointing to non-existent table 99999
        {
            let mut engine = CatalogEngine::open(&dir).unwrap();
            let idx_schema = idx_tab_schema();
            let mut bb = BatchBuilder::new(idx_schema);
            bb.begin_row(888, 0, 1);
            bb.put_u64(99999);   // owner_id (non-existent)
            bb.put_u64(OWNER_KIND_TABLE as u64);
            bb.put_u64(1);       // source_col_idx
            bb.put_string("orphaned_idx");
            bb.put_u64(0);       // is_unique
            bb.put_string("");   // cache_dir
            bb.end_row();
            ingest_batch_into(&mut engine.sys_indices, &bb.finish());
            let _ = engine.sys_indices.flush();
            engine.close();
            drop(engine);
        }

        // Re-open should fail because the orphaned index references table 99999
        let result = CatalogEngine::open(&dir);
        assert!(result.is_err(), "Orphaned index metadata should cause error on reload");

        let _ = fs::remove_dir_all(&dir);
    }

    // ── test_sequence_gap_recovery ───────────────────────────────────────

    #[test]
    fn test_sequence_gap_recovery() {
        let dir = temp_dir("seq_gap");
        let cols = vec![u64_col_def("id")];

        // First open: create a table, then inject a table record with high ID 250
        {
            let mut engine = CatalogEngine::open(&dir).unwrap();
            engine.create_table("public.t1", &cols, 0, true).unwrap();

            // Inject table record for tid=250 directly into sys_tables
            let tbl_schema = table_tab_schema();
            let mut bb = BatchBuilder::new(tbl_schema);
            bb.begin_row(250, 0, 1);
            bb.put_u64(PUBLIC_SCHEMA_ID as u64); // schema_id
            bb.put_string("gap_table");
            bb.put_string(&format!("{}/public/gap", dir));
            bb.put_u64(0); // pk_col_idx
            bb.put_u64(0); // created_lsn
            bb.put_u64(0); // flags
            bb.end_row();
            ingest_batch_into(&mut engine.sys_tables, &bb.finish());

            // Inject column record for tid=250
            let col_schema = col_tab_schema();
            let mut cbb = BatchBuilder::new(col_schema);
            let pk = pack_column_id(250, 0);
            cbb.begin_row(pk, 0, 1);
            cbb.put_u64(250);   // owner_id
            cbb.put_u64(OWNER_KIND_TABLE as u64);
            cbb.put_u64(0);     // col_idx
            cbb.put_string("id");
            cbb.put_u64(type_code::U64 as u64);
            cbb.put_u64(0);     // is_nullable
            cbb.put_u64(0);     // fk_table_id
            cbb.put_u64(0);     // fk_col_idx
            cbb.end_row();
            ingest_batch_into(&mut engine.sys_columns, &cbb.finish());

            let _ = engine.sys_tables.flush();
            let _ = engine.sys_columns.flush();
            engine.close();
            drop(engine);
        }

        // Re-open: sequence should recover to 251
        {
            let mut engine = CatalogEngine::open(&dir).unwrap();
            let new_tid = engine.create_table("public.tnext", &cols, 0, true).unwrap();
            assert_eq!(new_tid, 251, "Sequence recovery: expected 251, got {}", new_tid);
            engine.close();
        }

        let _ = fs::remove_dir_all(&dir);
    }

    // ── test_fk_referential_integrity (U128 extension) ──────────────────

    #[test]
    fn test_fk_u128() {
        let dir = temp_dir("fk_u128");
        let mut engine = CatalogEngine::open(&dir).unwrap();

        // U128 parent
        let parent_tid = engine.create_table("public.uparents",
            &[u128_col_def("uuid")], 0, true).unwrap();

        // U128 FK child
        let child_cols = vec![
            u64_col_def("id"),
            ColumnDef { name: "ufk".into(), type_code: type_code::U128, is_nullable: false,
                        fk_table_id: parent_tid, fk_col_idx: 0 },
        ];
        let child_tid = engine.create_table("public.uchildren", &child_cols, 0, true).unwrap();

        // Insert parent with U128 PK (lo=0xBBBB, hi=0xAAAA)
        let parent_schema = engine.get_schema(parent_tid).unwrap();
        let mut pbb = BatchBuilder::new(parent_schema);
        pbb.begin_row(0xBBBB, 0xAAAA, 1);
        pbb.end_row();
        engine.dag.ingest_to_family(parent_tid, pbb.finish());
        let _ = engine.dag.flush(parent_tid);

        // Valid child FK (matches parent)
        let child_schema = engine.get_schema(child_tid).unwrap();
        let mut cbb = BatchBuilder::new(child_schema);
        cbb.begin_row(1, 0, 1);
        cbb.put_u128(0xBBBB, 0xAAAA);
        cbb.end_row();
        assert!(engine.validate_fk_inline(child_tid, &cbb.finish()).is_ok());

        // Invalid child FK (no such parent)
        let mut cbb2 = BatchBuilder::new(child_schema);
        cbb2.begin_row(2, 0, 1);
        cbb2.put_u128(0xDEAD, 0xBEEF);
        cbb2.end_row();
        assert!(engine.validate_fk_inline(child_tid, &cbb2.finish()).is_err());

        engine.close();
        let _ = fs::remove_dir_all(&dir);
    }

    // ── View live incremental update ──────────────────────────────────────

    #[test]
    fn test_view_live_update() {
        let dir = temp_dir("view_live");
        let mut engine = CatalogEngine::open(&dir).unwrap();

        let cols = vec![u64_col_def("id"), i64_col_def("val")];
        let tid = engine.create_table("public.t", &cols, 0, true).unwrap();
        let schema = engine.get_schema(tid).unwrap();

        // Ingest initial data
        let mut bb = BatchBuilder::new(schema);
        for i in 0..5u64 {
            bb.begin_row(i, 0, 1);
            bb.put_u64(i * 10);
            bb.end_row();
        }
        engine.dag.ingest_to_family(tid, bb.finish());
        let _ = engine.dag.flush(tid);

        // Create passthrough view — should backfill 5 rows
        let out_cols = vec![
            ("id".to_string(), type_code::U64),
            ("val".to_string(), type_code::I64),
        ];
        let graph = make_passthrough_graph(tid, &out_cols);
        let vid = engine.create_view("public.v", &graph, "").unwrap();

        let view_schema = engine.dag.tables.get(&vid).unwrap().schema;
        let b1 = engine.scan_store(vid, &view_schema);
        assert_eq!(b1.count, 5, "Backfill: expected 5, got {}", b1.count);

        // Live update: ingest 1 more row + evaluate DAG
        let mut bb2 = BatchBuilder::new(schema);
        bb2.begin_row(99, 0, 1);
        bb2.put_u64(999);
        bb2.end_row();
        let delta = bb2.finish();
        engine.dag.ingest_to_family(tid, delta.clone_batch());
        let _ = engine.dag.flush(tid);
        engine.dag.evaluate_dag(tid, delta);

        let b2 = engine.scan_store(vid, &view_schema);
        assert_eq!(b2.count, 6, "Live update: expected 6, got {}", b2.count);

        engine.close();
        let _ = fs::remove_dir_all(&dir);
    }

    // ── Index live fan-out ────────────────────────────────────────────────

    #[test]
    fn test_index_live_fanout() {
        let dir = temp_dir("idx_fanout");
        let mut engine = CatalogEngine::open(&dir).unwrap();

        let cols = vec![u64_col_def("id"), i64_col_def("val")];
        let tid = engine.create_table("public.tfanout", &cols, 0, true).unwrap();
        let schema = engine.get_schema(tid).unwrap();

        // Ingest 5 rows
        let mut bb = BatchBuilder::new(schema);
        for i in 0..5u64 {
            bb.begin_row(i, 0, 1);
            bb.put_u64(i * 100);
            bb.end_row();
        }
        engine.dag.ingest_to_family(tid, bb.finish());
        let _ = engine.dag.flush(tid);

        // Create index — should backfill 5 rows
        engine.create_index("public.tfanout", "val", false).unwrap();

        // Live fan-out: ingest 1 more row via ingest_to_family (which does index projection)
        let mut bb2 = BatchBuilder::new(schema);
        bb2.begin_row(99, 0, 1);
        bb2.put_u64(777);
        bb2.end_row();
        engine.dag.ingest_to_family(tid, bb2.finish());
        let _ = engine.dag.flush(tid);

        // Verify index has 6 entries via DagEngine's index circuit
        let entry = engine.dag.tables.get(&tid).unwrap();
        assert_eq!(entry.index_circuits.len(), 1, "Expected 1 index circuit");
        let idx_handle = entry.index_circuits[0].index_handle;
        assert!(!idx_handle.is_null());
        let idx_table = unsafe { &mut *idx_handle };
        let idx_count = count_records(idx_table);
        assert_eq!(idx_count, 6, "Index fanout: expected 6, got {}", idx_count);

        engine.close();
        let _ = fs::remove_dir_all(&dir);
    }

    // ── Full lifecycle (replaces test_programmable_zset_lifecycle) ────────

    #[test]
    fn test_full_lifecycle() {
        let dir = temp_dir("lifecycle");
        let mut engine = CatalogEngine::open(&dir).unwrap();

        // 1. Schema
        engine.create_schema("app").unwrap();

        // 2. Tables with FK
        let users_tid = engine.create_table("app.users",
            &[u128_col_def("uid"), str_col_def("username")], 0, true).unwrap();

        let order_cols = vec![
            u64_col_def("oid"),
            ColumnDef { name: "uid".into(), type_code: type_code::U128, is_nullable: false,
                        fk_table_id: users_tid, fk_col_idx: 0 },
            i64_col_def("amount"),
        ];
        let orders_tid = engine.create_table("app.orders", &order_cols, 0, true).unwrap();

        // 3. FK enforcement: invalid order (no matching user)
        let orders_schema = engine.get_schema(orders_tid).unwrap();
        let mut bad = BatchBuilder::new(orders_schema);
        bad.begin_row(1, 0, 1);
        bad.put_u128(0xCAFEBABE, 0xDEADBEEF);
        bad.put_u64(500);
        bad.end_row();
        assert!(engine.validate_fk_inline(orders_tid, &bad.finish()).is_err());

        // 4. Ingest valid user + order
        let users_schema = engine.get_schema(users_tid).unwrap();
        let mut ub = BatchBuilder::new(users_schema);
        ub.begin_row(0xCAFEBABE, 0xDEADBEEF, 1);
        ub.put_string("alice");
        ub.end_row();
        engine.dag.ingest_to_family(users_tid, ub.finish());
        let _ = engine.dag.flush(users_tid);

        let mut ob = BatchBuilder::new(orders_schema);
        ob.begin_row(101, 0, 1);
        ob.put_u128(0xCAFEBABE, 0xDEADBEEF);
        ob.put_u64(1000);
        ob.end_row();
        assert!(engine.validate_fk_inline(orders_tid, &ob.finish()).is_ok());

        // 5. View
        let out_cols = vec![
            ("uid".to_string(), type_code::U128),
            ("username".to_string(), type_code::STRING),
        ];
        let graph = make_passthrough_graph(users_tid, &out_cols);
        let view_tid = engine.create_view("app.active_users", &graph, "SELECT * FROM users").unwrap();

        // 5.1 Can't drop table referenced by view
        assert!(engine.drop_table("app.users").is_err());

        // 6. Persistence
        engine.close();
        drop(engine);

        let mut engine2 = CatalogEngine::open(&dir).unwrap();
        assert!(engine2.get_by_name("app", "users").is_some());
        assert!(engine2.get_by_name("app", "active_users").is_some());

        // 7. Compilation recovery
        assert!(engine2.dag.ensure_compiled(view_tid));

        // 8. Teardown
        engine2.drop_view("app.active_users").unwrap();
        engine2.drop_table("app.orders").unwrap();
        engine2.drop_table("app.users").unwrap();
        engine2.drop_schema("app").unwrap();
        assert!(!engine2.has_schema("app"));

        engine2.close();
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_ingest_scan_seek_family() {
        let dir = temp_dir("catalog_ingest_scan_seek");
        let mut engine = CatalogEngine::open(&dir).unwrap();
        let cols = vec![u64_col_def("id"), u64_col_def("val")];
        let tid = engine.create_table("public.t", &cols, 0, false).unwrap();
        let schema = engine.get_schema(tid).unwrap();

        // Ingest via CatalogEngine (user table path)
        let mut bb = BatchBuilder::new(schema);
        bb.begin_row(1, 0, 1); bb.put_u64(100); bb.end_row();
        bb.begin_row(2, 0, 1); bb.put_u64(200); bb.end_row();
        bb.begin_row(3, 0, 1); bb.put_u64(300); bb.end_row();
        engine.ingest_to_family(tid, bb.finish()).unwrap();
        engine.flush_family(tid).unwrap();

        // Scan
        let scan_batch = engine.scan_family(tid).unwrap();
        assert_eq!(scan_batch.count, 3);

        // Seek existing
        let found = engine.seek_family(tid, 2, 0).unwrap();
        assert!(found.is_some());
        let row = found.unwrap();
        assert_eq!(row.count, 1);
        assert_eq!(row.get_pk_lo(0), 2);

        // Seek missing
        let not_found = engine.seek_family(tid, 99, 0).unwrap();
        assert!(not_found.is_none());

        engine.close();
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_ingest_unique_pk_partitioned() {
        let dir = temp_dir("catalog_unique_pk_partitioned");
        let mut engine = CatalogEngine::open(&dir).unwrap();
        let cols = vec![u64_col_def("id"), u64_col_def("val")];
        let tid = engine.create_table("public.t", &cols, 0, true).unwrap();
        let schema = engine.get_schema(tid).unwrap();

        // Insert row with PK=1, val=100
        let mut bb = BatchBuilder::new(schema);
        bb.begin_row(1, 0, 1); bb.put_u64(100); bb.end_row();
        engine.ingest_to_family(tid, bb.finish()).unwrap();
        engine.flush_family(tid).unwrap();

        // Insert row with PK=1 again, val=200 (should retract old + insert new)
        let mut bb = BatchBuilder::new(schema);
        bb.begin_row(1, 0, 1); bb.put_u64(200); bb.end_row();
        engine.ingest_to_family(tid, bb.finish()).unwrap();
        engine.flush_family(tid).unwrap();

        // Scan — should have exactly 1 row with val=200
        let scan = engine.scan_family(tid).unwrap();
        assert_eq!(scan.count, 1);
        assert_eq!(scan.get_pk_lo(0), 1);

        engine.close();
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_ddl_sync() {
        let dir = temp_dir("catalog_ddl_sync");
        let mut engine = CatalogEngine::open(&dir).unwrap();

        // Create a schema via normal DDL
        engine.create_schema("app").unwrap();
        assert!(engine.has_schema("app"));

        // Simulate DDL sync: create batch mimicking a schema record
        let schema = super::schema_tab_schema();
        let mut bb = BatchBuilder::new(schema);
        bb.begin_row(100, 0, 1); // sid=100
        bb.put_string("synced");
        bb.end_row();
        engine.ddl_sync(SCHEMA_TAB_ID, bb.finish()).unwrap();

        // Hooks should have registered the schema
        assert!(engine.has_schema("synced"));

        engine.close();
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_raw_store_ingest() {
        let dir = temp_dir("catalog_raw_store_ingest");
        let mut engine = CatalogEngine::open(&dir).unwrap();
        let cols = vec![u64_col_def("id"), u64_col_def("val")];
        let tid = engine.create_table("public.t", &cols, 0, false).unwrap();
        let schema = engine.get_schema(tid).unwrap();

        // Raw ingest (SAL recovery path — no hooks, no unique_pk, no index projection)
        let mut bb = BatchBuilder::new(schema);
        bb.begin_row(10, 0, 1); bb.put_u64(1000); bb.end_row();
        bb.begin_row(20, 0, 1); bb.put_u64(2000); bb.end_row();
        engine.raw_store_ingest(tid, bb.finish()).unwrap();
        engine.flush_family(tid).unwrap();

        let scan = engine.scan_family(tid).unwrap();
        assert_eq!(scan.count, 2);

        engine.close();
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_partition_management() {
        let dir = temp_dir("catalog_partition_mgmt");
        let mut engine = CatalogEngine::open(&dir).unwrap();
        let cols = vec![u64_col_def("id"), u64_col_def("val")];
        let _tid = engine.create_table("public.t", &cols, 0, false).unwrap();

        // These should not panic
        engine.set_active_partitions(0, 64);
        engine.disable_user_table_wal();
        engine.trim_worker_partitions(0, 64);
        engine.invalidate_all_plans();

        engine.close();
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_fk_index_metadata_queries() {
        let dir = temp_dir("catalog_fk_idx_meta");
        let mut engine = CatalogEngine::open(&dir).unwrap();

        let cols = vec![u64_col_def("id"), u64_col_def("val")];
        let tid = engine.create_table("public.parent", &cols, 0, false).unwrap();

        // Create child table with FK to parent
        let child_cols = vec![
            u64_col_def("id"),
            ColumnDef {
                name: "parent_id".into(),
                type_code: crate::schema::type_code::U64,
                is_nullable: false,
                fk_table_id: tid,
                fk_col_idx: 0,
            },
        ];
        let child_tid = engine.create_table("public.child", &child_cols, 0, false).unwrap();

        // FK count
        assert!(engine.get_fk_count(child_tid) > 0);
        let (col_idx, target_id) = engine.get_fk_constraint(child_tid, 0).unwrap();
        assert_eq!(target_id, tid);

        // Create an explicit index
        let _iid = engine.create_index("public.parent", "val", false).unwrap();

        assert!(engine.get_index_circuit_count(tid) > 0);
        let (ic_col, _is_unique, _tc) = engine.get_index_circuit_info(tid, 0).unwrap();
        assert_eq!(ic_col, 1); // val is column 1

        // Index store handle should be non-null
        let idx_ptr = engine.get_index_store_handle(tid, 1);
        assert!(!idx_ptr.is_null());

        engine.close();
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_iter_user_table_ids_and_lsn() {
        let dir = temp_dir("catalog_iter_lsn");
        let mut engine = CatalogEngine::open(&dir).unwrap();

        let cols = vec![u64_col_def("id"), u64_col_def("val")];
        let tid1 = engine.create_table("public.t1", &cols, 0, false).unwrap();
        let tid2 = engine.create_table("public.t2", &cols, 0, false).unwrap();

        let ids = engine.iter_user_table_ids();
        assert!(ids.contains(&tid1));
        assert!(ids.contains(&tid2));

        // LSN should be accessible without panicking
        let _ = engine.get_max_flushed_lsn(tid1);

        engine.close();
        let _ = fs::remove_dir_all(&dir);
    }

    // ── validate_unique_indices tests ─────────────────────────────────

    #[test]
    fn test_validate_unique_indices_duplicate_value() {
        let dir = temp_dir("catalog_uidx_dup");
        let mut engine = CatalogEngine::open(&dir).unwrap();
        let cols = vec![u64_col_def("id"), u64_col_def("val")];
        let tid = engine.create_table("public.t", &cols, 0, false).unwrap();
        engine.create_index("public.t", "val", true).unwrap(); // unique index on val
        let schema = engine.get_schema(tid).unwrap();

        // Insert first row
        let mut bb = BatchBuilder::new(schema);
        bb.begin_row(1, 0, 1); bb.put_u64(42); bb.end_row();
        engine.ingest_to_family(tid, bb.finish()).unwrap();
        engine.flush_family(tid).unwrap();

        // Insert second row with same val=42 → should violate unique index
        let mut bb = BatchBuilder::new(schema);
        bb.begin_row(2, 0, 1); bb.put_u64(42); bb.end_row();
        let result = engine.validate_unique_indices(tid, &bb.finish());
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Unique index violation"));

        engine.close();
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_validate_unique_indices_batch_internal_dup() {
        let dir = temp_dir("catalog_uidx_batch_dup");
        let mut engine = CatalogEngine::open(&dir).unwrap();
        let cols = vec![u64_col_def("id"), u64_col_def("val")];
        let tid = engine.create_table("public.t", &cols, 0, false).unwrap();
        engine.create_index("public.t", "val", true).unwrap();
        let schema = engine.get_schema(tid).unwrap();

        // Single batch with two rows sharing val=99 → duplicate in batch
        let mut bb = BatchBuilder::new(schema);
        bb.begin_row(1, 0, 1); bb.put_u64(99); bb.end_row();
        bb.begin_row(2, 0, 1); bb.put_u64(99); bb.end_row();
        let result = engine.validate_unique_indices(tid, &bb.finish());
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("duplicate in batch"));

        engine.close();
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_validate_unique_indices_upsert_same_value_ok() {
        let dir = temp_dir("catalog_uidx_upsert_same");
        let mut engine = CatalogEngine::open(&dir).unwrap();
        let cols = vec![u64_col_def("id"), u64_col_def("val")];
        let tid = engine.create_table("public.t", &cols, 0, true).unwrap(); // unique_pk
        engine.create_index("public.t", "val", true).unwrap();
        let schema = engine.get_schema(tid).unwrap();

        // Insert PK=1, val=42
        let mut bb = BatchBuilder::new(schema);
        bb.begin_row(1, 0, 1); bb.put_u64(42); bb.end_row();
        engine.ingest_to_family(tid, bb.finish()).unwrap();
        engine.flush_family(tid).unwrap();

        // UPSERT PK=1, val=42 (same value) → should succeed
        let mut bb = BatchBuilder::new(schema);
        bb.begin_row(1, 0, 1); bb.put_u64(42); bb.end_row();
        let result = engine.validate_unique_indices(tid, &bb.finish());
        assert!(result.is_ok());

        engine.close();
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_validate_unique_indices_upsert_to_existing_value_fails() {
        let dir = temp_dir("catalog_uidx_upsert_conflict");
        let mut engine = CatalogEngine::open(&dir).unwrap();
        let cols = vec![u64_col_def("id"), u64_col_def("val")];
        let tid = engine.create_table("public.t", &cols, 0, true).unwrap(); // unique_pk
        engine.create_index("public.t", "val", true).unwrap();
        let schema = engine.get_schema(tid).unwrap();

        // Insert PK=1, val=42 and PK=2, val=99
        let mut bb = BatchBuilder::new(schema);
        bb.begin_row(1, 0, 1); bb.put_u64(42); bb.end_row();
        bb.begin_row(2, 0, 1); bb.put_u64(99); bb.end_row();
        engine.ingest_to_family(tid, bb.finish()).unwrap();
        engine.flush_family(tid).unwrap();

        // UPSERT PK=1, val=99 → conflicts with PK=2's val=99
        let mut bb = BatchBuilder::new(schema);
        bb.begin_row(1, 0, 1); bb.put_u64(99); bb.end_row();
        let result = engine.validate_unique_indices(tid, &bb.finish());
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Unique index violation"));

        engine.close();
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_validate_unique_indices_distinct_values_ok() {
        let dir = temp_dir("catalog_uidx_distinct");
        let mut engine = CatalogEngine::open(&dir).unwrap();
        let cols = vec![u64_col_def("id"), u64_col_def("val")];
        let tid = engine.create_table("public.t", &cols, 0, false).unwrap();
        engine.create_index("public.t", "val", true).unwrap();
        let schema = engine.get_schema(tid).unwrap();

        // Insert row with val=42
        let mut bb = BatchBuilder::new(schema);
        bb.begin_row(1, 0, 1); bb.put_u64(42); bb.end_row();
        engine.ingest_to_family(tid, bb.finish()).unwrap();
        engine.flush_family(tid).unwrap();

        // Insert row with different val=99 → should succeed
        let mut bb = BatchBuilder::new(schema);
        bb.begin_row(2, 0, 1); bb.put_u64(99); bb.end_row();
        let result = engine.validate_unique_indices(tid, &bb.finish());
        assert!(result.is_ok());

        engine.close();
        let _ = fs::remove_dir_all(&dir);
    }

    // ── seek_by_index tests ──────────────────────────────────────────

    #[test]
    fn test_seek_by_index_found() {
        let dir = temp_dir("catalog_seekidx_found");
        let mut engine = CatalogEngine::open(&dir).unwrap();
        let cols = vec![u64_col_def("id"), u64_col_def("val")];
        let tid = engine.create_table("public.t", &cols, 0, false).unwrap();
        engine.create_index("public.t", "val", false).unwrap();
        let schema = engine.get_schema(tid).unwrap();

        let mut bb = BatchBuilder::new(schema);
        bb.begin_row(10, 0, 1); bb.put_u64(100); bb.end_row();
        bb.begin_row(20, 0, 1); bb.put_u64(200); bb.end_row();
        bb.begin_row(30, 0, 1); bb.put_u64(300); bb.end_row();
        engine.ingest_to_family(tid, bb.finish()).unwrap();
        engine.flush_family(tid).unwrap();

        // Seek by index: val=200 → should find PK=20
        let result = engine.seek_by_index(tid, 1, 200, 0).unwrap();
        assert!(result.is_some());
        let row = result.unwrap();
        assert_eq!(row.count, 1);
        assert_eq!(row.get_pk_lo(0), 20);

        engine.close();
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_seek_by_index_not_found() {
        let dir = temp_dir("catalog_seekidx_miss");
        let mut engine = CatalogEngine::open(&dir).unwrap();
        let cols = vec![u64_col_def("id"), u64_col_def("val")];
        let tid = engine.create_table("public.t", &cols, 0, false).unwrap();
        engine.create_index("public.t", "val", false).unwrap();
        let schema = engine.get_schema(tid).unwrap();

        let mut bb = BatchBuilder::new(schema);
        bb.begin_row(1, 0, 1); bb.put_u64(42); bb.end_row();
        engine.ingest_to_family(tid, bb.finish()).unwrap();
        engine.flush_family(tid).unwrap();

        // Seek by index: val=999 → should return None
        let result = engine.seek_by_index(tid, 1, 999, 0).unwrap();
        assert!(result.is_none());

        engine.close();
        let _ = fs::remove_dir_all(&dir);
    }

    // ── Regression: small-column index projection (bug #1) ────────────

    #[test]
    fn test_seek_by_index_u8_column() {
        let dir = temp_dir("catalog_seekidx_u8");
        let mut engine = CatalogEngine::open(&dir).unwrap();
        let cols = vec![u64_col_def("id"), u8_col_def("tag")];
        let tid = engine.create_table("public.t", &cols, 0, false).unwrap();
        engine.create_index("public.t", "tag", false).unwrap();
        let schema = engine.get_schema(tid).unwrap();

        let mut bb = BatchBuilder::new(schema);
        bb.begin_row(10, 0, 1); bb.put_u8(42); bb.end_row();
        bb.begin_row(20, 0, 1); bb.put_u8(99); bb.end_row();
        engine.ingest_to_family(tid, bb.finish()).unwrap();
        engine.flush_family(tid).unwrap();

        let result = engine.seek_by_index(tid, 1, 42, 0).unwrap();
        assert!(result.is_some(), "U8 index lookup must find the row");
        assert_eq!(result.unwrap().get_pk_lo(0), 10);

        let result2 = engine.seek_by_index(tid, 1, 99, 0).unwrap();
        assert!(result2.is_some());
        assert_eq!(result2.unwrap().get_pk_lo(0), 20);

        engine.close();
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_seek_by_index_u16_column() {
        let dir = temp_dir("catalog_seekidx_u16");
        let mut engine = CatalogEngine::open(&dir).unwrap();
        let cols = vec![u64_col_def("id"), u16_col_def("port")];
        let tid = engine.create_table("public.t", &cols, 0, false).unwrap();
        engine.create_index("public.t", "port", false).unwrap();
        let schema = engine.get_schema(tid).unwrap();

        let mut bb = BatchBuilder::new(schema);
        bb.begin_row(1, 0, 1); bb.put_u16(8080); bb.end_row();
        bb.begin_row(2, 0, 1); bb.put_u16(443); bb.end_row();
        engine.ingest_to_family(tid, bb.finish()).unwrap();
        engine.flush_family(tid).unwrap();

        let result = engine.seek_by_index(tid, 1, 443, 0).unwrap();
        assert!(result.is_some(), "U16 index lookup must find the row");
        assert_eq!(result.unwrap().get_pk_lo(0), 2);

        engine.close();
        let _ = fs::remove_dir_all(&dir);
    }

    // ── Regression: signed key encoding consistency (bug #2) ────────────

    #[test]
    fn test_promote_to_index_key_matches_batch_project_for_signed() {
        // promote_to_index_key (multi-worker path) must produce the same
        // key as batch_project_index (storage path) for negative signed values.
        use crate::schema::promote_to_index_key;

        // I32 value -1 stored as little-endian 0xFFFFFFFF
        let data: [u8; 4] = (-1i32).to_le_bytes();
        let (lo, hi) = promote_to_index_key(&data, 0, 4, type_code::I32);

        // batch_project_index reads u32::from_le_bytes() as u64 → 0x00000000FFFFFFFF
        assert_eq!(lo, 0xFFFFFFFFu64, "I32 -1 must zero-extend, not sign-extend");
        assert_eq!(hi, 0);

        // I16 value -1 stored as little-endian 0xFFFF
        let data16: [u8; 2] = (-1i16).to_le_bytes();
        let (lo16, hi16) = promote_to_index_key(&data16, 0, 2, type_code::I16);
        assert_eq!(lo16, 0xFFFFu64);
        assert_eq!(hi16, 0);

        // I8 value -1 stored as 0xFF
        let data8: [u8; 1] = [0xFF];
        let (lo8, hi8) = promote_to_index_key(&data8, 0, 1, type_code::I8);
        assert_eq!(lo8, 0xFFu64);
        assert_eq!(hi8, 0);
    }

    #[test]
    fn test_seek_by_index_i32_negative_value() {
        let dir = temp_dir("catalog_seekidx_i32_neg");
        let mut engine = CatalogEngine::open(&dir).unwrap();
        let cols = vec![u64_col_def("id"), i32_col_def("temp")];
        let tid = engine.create_table("public.t", &cols, 0, false).unwrap();
        engine.create_index("public.t", "temp", false).unwrap();
        let schema = engine.get_schema(tid).unwrap();

        // Insert a row with a negative I32 value
        let mut bb = BatchBuilder::new(schema);
        bb.begin_row(1, 0, 1); bb.put_u32((-5i32) as u32); bb.end_row();
        bb.begin_row(2, 0, 1); bb.put_u32(10u32); bb.end_row();
        engine.ingest_to_family(tid, bb.finish()).unwrap();
        engine.flush_family(tid).unwrap();

        // Seek negative value: key = zero-extended u32 representation of -5
        let neg5_key = (-5i32) as u32 as u64;
        let result = engine.seek_by_index(tid, 1, neg5_key, 0).unwrap();
        assert!(result.is_some(), "I32 negative index lookup must find the row");
        assert_eq!(result.unwrap().get_pk_lo(0), 1);

        // Seek positive value
        let result2 = engine.seek_by_index(tid, 1, 10, 0).unwrap();
        assert!(result2.is_some());
        assert_eq!(result2.unwrap().get_pk_lo(0), 2);

        engine.close();
        let _ = fs::remove_dir_all(&dir);
    }

    // ── UPSERT retraction propagation tests ──────────────────────────

    #[test]
    fn test_upsert_effective_batch_has_retractions() {
        let dir = temp_dir("catalog_upsert_retract");
        let mut engine = CatalogEngine::open(&dir).unwrap();
        let cols = vec![u64_col_def("id"), u64_col_def("val")];
        let tid = engine.create_table("public.t", &cols, 0, true).unwrap(); // unique_pk
        let schema = engine.get_schema(tid).unwrap();

        // Insert PK=1, val=100
        let mut bb = BatchBuilder::new(schema);
        bb.begin_row(1, 0, 1); bb.put_u64(100); bb.end_row();
        engine.dag.ingest_to_family(tid, bb.finish());
        let _ = engine.dag.flush(tid);

        // UPSERT PK=1, val=200 via ingest_returning_effective
        let mut bb = BatchBuilder::new(schema);
        bb.begin_row(1, 0, 1); bb.put_u64(200); bb.end_row();
        let (rc, effective_opt) = engine.dag.ingest_returning_effective(tid, bb.finish());
        assert_eq!(rc, 0);
        let effective = effective_opt.unwrap();

        // Effective batch should have 2 rows:
        // Row 0: weight=-1 (retraction of old val=100)
        // Row 1: weight=+1 (insertion of new val=200)
        assert_eq!(effective.count, 2, "Expected retraction + insertion");
        assert_eq!(effective.get_weight(0), -1, "First row should be retraction");
        assert_eq!(effective.get_weight(1), 1, "Second row should be insertion");
        // Both should have PK=1
        assert_eq!(effective.get_pk_lo(0), 1);
        assert_eq!(effective.get_pk_lo(1), 1);

        engine.close();
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_upsert_retraction_reaches_views() {
        let dir = temp_dir("catalog_upsert_view");
        let mut engine = CatalogEngine::open(&dir).unwrap();
        let cols = vec![u64_col_def("id"), u64_col_def("val")];
        let tid = engine.create_table("public.t", &cols, 0, true).unwrap();
        let schema = engine.get_schema(tid).unwrap();
        let out_cols = vec![("id".to_string(), type_code::U64), ("val".to_string(), type_code::U64)];

        // Create passthrough view
        let graph = make_passthrough_graph(tid, &out_cols);
        let vid = engine.create_view("public.v", &graph, "").unwrap();

        // Insert PK=1, val=100
        let mut bb = BatchBuilder::new(schema);
        bb.begin_row(1, 0, 1); bb.put_u64(100); bb.end_row();
        engine.push_and_evaluate(tid, bb.finish()).unwrap();

        // Check view: should have 1 row with val=100
        let scan1 = engine.scan_family(vid).unwrap();
        assert_eq!(scan1.count, 1);

        // UPSERT PK=1, val=200
        let mut bb = BatchBuilder::new(schema);
        bb.begin_row(1, 0, 1); bb.put_u64(200); bb.end_row();
        engine.push_and_evaluate(tid, bb.finish()).unwrap();

        // View should have 1 row with val=200 (old val=100 retracted)
        let scan2 = engine.scan_family(vid).unwrap();
        assert_eq!(scan2.count, 1, "View should have exactly 1 row after UPSERT");

        engine.close();
        let _ = fs::remove_dir_all(&dir);
    }

    // ── push_and_evaluate test ───────────────────────────────────────

    #[test]
    fn test_push_and_evaluate_cascades_to_view() {
        let dir = temp_dir("catalog_push_eval");
        let mut engine = CatalogEngine::open(&dir).unwrap();
        let cols = vec![u64_col_def("id"), u64_col_def("val")];
        let tid = engine.create_table("public.t", &cols, 0, false).unwrap();
        let out_cols = vec![("id".to_string(), type_code::U64), ("val".to_string(), type_code::U64)];

        let graph = make_passthrough_graph(tid, &out_cols);
        let vid = engine.create_view("public.v", &graph, "").unwrap();

        let schema = engine.get_schema(tid).unwrap();
        let mut bb = BatchBuilder::new(schema);
        bb.begin_row(1, 0, 1); bb.put_u64(10); bb.end_row();
        bb.begin_row(2, 0, 1); bb.put_u64(20); bb.end_row();
        engine.push_and_evaluate(tid, bb.finish()).unwrap();

        // View should reflect the push
        let scan = engine.scan_family(vid).unwrap();
        assert_eq!(scan.count, 2);

        // Push more
        let mut bb = BatchBuilder::new(schema);
        bb.begin_row(3, 0, 1); bb.put_u64(30); bb.end_row();
        engine.push_and_evaluate(tid, bb.finish()).unwrap();

        let scan = engine.scan_family(vid).unwrap();
        assert_eq!(scan.count, 3);

        engine.close();
        let _ = fs::remove_dir_all(&dir);
    }

    // ── Column name caching test ─────────────────────────────────────

    #[test]
    fn test_get_column_names_cached() {
        let dir = temp_dir("catalog_colnames");
        let mut engine = CatalogEngine::open(&dir).unwrap();
        let cols = vec![
            ColumnDef { name: "pk".into(), type_code: type_code::U64, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
            ColumnDef { name: "alpha".into(), type_code: type_code::U64, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
            ColumnDef { name: "beta".into(), type_code: type_code::U64, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
        ];
        let tid = engine.create_table("public.t", &cols, 0, false).unwrap();

        let names1 = engine.get_column_names(tid);
        assert_eq!(names1, vec!["pk", "alpha", "beta"]);

        // Second call should hit cache
        let names2 = engine.get_column_names(tid);
        assert_eq!(names2, vec!["pk", "alpha", "beta"]);

        engine.close();
        let _ = fs::remove_dir_all(&dir);
    }
}
