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

mod sys_tables;

#[cfg(test)]
mod tests;

use std::collections::HashMap;
use std::fs;
use std::sync::Arc;

use crate::schema::{SchemaColumn, SchemaDescriptor, type_code};
use crate::dag::{DagEngine, StoreHandle};
use crate::storage::{OwnedBatch, PartitionedTable, partition_arena_size, CursorHandle, Table};

// Re-export items used by other crate modules.
pub(crate) use sys_tables::{FIRST_USER_TABLE_ID, SEQ_ID_SCHEMAS, SEQ_ID_TABLES, SEQ_ID_INDICES};
pub(crate) use sys_tables::{SYSTEM_SCHEMA_ID, PUBLIC_SCHEMA_ID};

// Import everything from sys_tables for internal use.
use sys_tables::*;

// ---------------------------------------------------------------------------
// BatchBuilder — construct OwnedBatch rows for system table mutations
// ---------------------------------------------------------------------------

/// Lightweight row-by-row builder for constructing OwnedBatch in Rust.
/// Operates on OwnedBatch directly.
pub(crate) struct BatchBuilder {
    batch: OwnedBatch,
    schema: SchemaDescriptor,
    // per-row state
    curr_null_word: u64,
    curr_col: usize,
}

impl BatchBuilder {
    pub(crate) fn new(schema: SchemaDescriptor) -> Self {
        let batch = OwnedBatch::with_schema(schema, 8);
        BatchBuilder {
            batch,
            schema,
            curr_null_word: 0,
            curr_col: 0,
        }
    }

    /// Begin a new row with the given PK and weight.
    pub(crate) fn begin_row(&mut self, pk_lo: u64, pk_hi: u64, weight: i64) {
        self.batch.ensure_row_capacity();
        self.batch.extend_pk_lo(&pk_lo.to_le_bytes());
        self.batch.extend_pk_hi(&pk_hi.to_le_bytes());
        self.batch.extend_weight(&weight.to_le_bytes());
        self.curr_null_word = 0;
        self.curr_col = 0;
    }

    /// Put a u64 value for the current payload column.
    pub(crate) fn put_u64(&mut self, val: u64) {
        self.batch.extend_col(self.curr_col, &val.to_le_bytes());
        self.curr_col += 1;
    }

    /// Put a string value for the current payload column.
    pub(crate) fn put_string(&mut self, s: &str) {
        let st = crate::schema::encode_german_string(s.as_bytes(), &mut self.batch.blob);
        self.batch.extend_col(self.curr_col, &st);
        self.curr_col += 1;
    }

    /// Put a u128 value (lo, hi) for the current payload column.
    pub(crate) fn put_u128(&mut self, lo: u64, hi: u64) {
        let mut buf = [0u8; 16];
        buf[0..8].copy_from_slice(&lo.to_le_bytes());
        buf[8..16].copy_from_slice(&hi.to_le_bytes());
        self.batch.extend_col(self.curr_col, &buf);
        self.curr_col += 1;
    }

    /// Put a u8 value for the current payload column.
    pub(crate) fn put_u8(&mut self, val: u8) {
        self.batch.extend_col(self.curr_col, &[val]);
        self.curr_col += 1;
    }

    /// Put a u16 value for the current payload column.
    pub(crate) fn put_u16(&mut self, val: u16) {
        self.batch.extend_col(self.curr_col, &val.to_le_bytes());
        self.curr_col += 1;
    }

    /// Put a u32 value for the current payload column.
    pub(crate) fn put_u32(&mut self, val: u32) {
        self.batch.extend_col(self.curr_col, &val.to_le_bytes());
        self.curr_col += 1;
    }

    /// Put a NULL value for the current payload column.
    pub(crate) fn put_null(&mut self) {
        let col_size = self.schema.columns[self.physical_col_idx()].size as usize;
        self.batch.fill_col_zero(self.curr_col, col_size);
        self.curr_null_word |= 1u64 << self.curr_col;
        self.curr_col += 1;
    }

    /// Finish the current row (writes null bitmap).
    pub(crate) fn end_row(&mut self) {
        self.batch.extend_null_bmp(&self.curr_null_word.to_le_bytes());
        self.batch.count += 1;
        self.batch.sorted = false;
        self.batch.consolidated = false;
    }

    /// Convenience: begin + put columns + end for a simple row.
    pub(crate) fn finish(self) -> OwnedBatch {
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

pub(crate) fn validate_user_identifier(name: &str) -> Result<(), String> {
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
    let npc = batch.num_payload_cols();
    let (ptrs, sizes) = batch.to_region_ptrs();
    let _ = table.ingest_batch_from_regions(&ptrs, &sizes, batch.count as u32, npc);
}

// ---------------------------------------------------------------------------
// CatalogEngine
// ---------------------------------------------------------------------------

/// The catalog engine wraps DagEngine and manages the entity registry,
/// system tables, DDL operations, and hook processing.
pub struct CatalogEngine {
    pub(crate) dag: DagEngine,
    pub(crate) base_dir: String,

    // --- Entity registry ---
    schema_name_to_id: HashMap<String, i64>,
    schema_id_to_name: HashMap<i64, String>,
    name_to_id: HashMap<String, i64>,          // "schema.table" -> table_id
    id_to_qualified: HashMap<i64, (String, String)>, // table_id -> (schema, table)
    next_schema_id: i64,
    next_table_id: i64,
    next_index_id: i64,
    active_part_start: u32,
    active_part_end: u32,

    // --- FK constraints per table ---
    fk_constraints: HashMap<i64, Vec<FkConstraint>>,
    // Reverse index: parent_table_id → Vec<(child_table_id, fk_col_idx)>
    fk_parent_map: HashMap<i64, Vec<(i64, usize)>>,

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
            fk_parent_map: HashMap::new(),
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
                StoreHandle::Borrowed(table_ptrs[i]),
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
        copy_cursor_row_with_weight(cursor, schema, batch, cursor.cursor.current_weight);
    }
}

/// Copy a single cursor row into `batch` with an explicit weight.
/// Handles out-of-line German strings by copying blob data and rewriting offsets.
fn copy_cursor_row_with_weight(
    cursor: &CursorHandle,
    schema: &SchemaDescriptor,
    batch: &mut OwnedBatch,
    weight: i64,
) {
        let pk_lo = cursor.cursor.current_key_lo;
        let pk_hi = cursor.cursor.current_key_hi;
        let null_word = cursor.cursor.current_null_word;

        batch.ensure_row_capacity();
        batch.extend_pk_lo(&pk_lo.to_le_bytes());
        batch.extend_pk_hi(&pk_hi.to_le_bytes());
        batch.extend_weight(&weight.to_le_bytes());
        batch.extend_null_bmp(&null_word.to_le_bytes());

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
                        batch.extend_col(payload_idx, &cell);
                    } else {
                        batch.extend_col(payload_idx, data);
                    }
                } else {
                    batch.extend_col(payload_idx, data);
                }
            } else {
                batch.fill_col_zero(payload_idx, col_size);
            }
            payload_idx += 1;
        }

        batch.count += 1;
}

/// Seek a system table by PK, copy the matching row with weight=-1.
/// Returns a single-row retraction batch (or empty batch if PK not found).
fn retract_single_row(table: &mut Table, schema: &SchemaDescriptor, pk_lo: u64, pk_hi: u64) -> OwnedBatch {
    let mut batch = OwnedBatch::with_schema(*schema, 1);
    let mut cursor = match table.create_cursor() {
        Ok(c) => c,
        Err(_) => return batch,
    };
    cursor.cursor.seek(crate::util::make_pk(pk_lo, pk_hi));
    if cursor.cursor.valid
        && cursor.cursor.current_key_lo == pk_lo
        && cursor.cursor.current_key_hi == pk_hi
        && cursor.cursor.current_weight > 0
    {
        copy_cursor_row_with_weight(&cursor, schema, &mut batch, -1);
    }
    batch
}

/// Seek to `(0, pk_hi)` and scan all positive-weight rows with that pk_hi,
/// emitting each as a retraction (weight=-1). Exploits U128 sort order
/// `(pk_hi, pk_lo)` which makes all circuit rows for a given view contiguous.
fn retract_rows_by_pk_hi(table: &mut Table, schema: &SchemaDescriptor, pk_hi: u64) -> OwnedBatch {
    let mut batch = OwnedBatch::with_schema(*schema, 8);
    let mut cursor = match table.create_cursor() {
        Ok(c) => c,
        Err(_) => return batch,
    };
    cursor.cursor.seek(crate::util::make_pk(0, pk_hi));
    while cursor.cursor.valid && cursor.cursor.current_key_hi == pk_hi {
        if cursor.cursor.current_weight > 0 {
            copy_cursor_row_with_weight(&cursor, schema, &mut batch, -1);
        }
        cursor.cursor.advance();
    }
    batch
}

/// Retract all positive-weight rows with the given pk_hi from a system table.
fn retract_and_ingest(table: &mut Table, schema: &SchemaDescriptor, pk_hi: u64) {
    let batch = retract_rows_by_pk_hi(table, schema, pk_hi);
    if batch.count > 0 {
        ingest_batch_into(table, &batch);
    }
}

impl CatalogEngine {
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
            let sid = batch.get_pk(i) as i64;
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
            let tid = batch.get_pk(i) as i64;
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

                // Register with entity registry
                let qualified = format!("{}.{}", schema_name, name);
                self.name_to_id.insert(qualified, tid);
                self.id_to_qualified.insert(tid, (schema_name.clone(), name.clone()));
                self.schema_ids.insert(tid, sid);
                self.pk_col_indices.insert(tid, pk_col_idx);

                // Register with DagEngine — Box ownership transfers here
                self.dag.register_table(
                    tid,
                    StoreHandle::Partitioned(Box::new(pt)),
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

                if self.cascade_enabled {
                    self.create_fk_indices(tid)?;
                }
            } else {
                if self.dag.tables.contains_key(&tid) {
                    // Check for FK references before dropping
                    let children = self.fk_children_of(tid);
                    if let Some(&(child_tid, _)) = children.first() {
                        let (sn, tn) = self.id_to_qualified.get(&child_tid)
                            .cloned().unwrap_or_default();
                        return Err(format!(
                            "Integrity violation: table referenced by '{}.{}'", sn, tn
                        ));
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

                    // Remove this table's FK constraints and clean up parent map
                    if let Some(constraints) = self.fk_constraints.remove(&tid) {
                        for c in &constraints {
                            if let Some(children) = self.fk_parent_map.get_mut(&c.target_table_id) {
                                children.retain(|&(child_tid, _)| child_tid != tid);
                            }
                        }
                    }
                    // Also remove this table from being a parent
                    self.fk_parent_map.remove(&tid);
                }
            }
        }
        Ok(())
    }

    fn on_view_delta(&mut self, batch: &OwnedBatch) -> Result<(), String> {
        for i in 0..batch.count {
            let weight = batch.get_weight(i);
            let vid = batch.get_pk(i) as i64;
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
                    StoreHandle::Partitioned(Box::new(et)),
                    view_schema,
                    max_depth,
                    false,
                    directory.clone(),
                );

                // Advance next_table_id if this is a gap recovery
                if vid + 1 > self.next_table_id {
                    self.next_table_id = vid + 1;
                }

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
            let idx_id = batch.get_pk(i) as i64;
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

                let mut idx_table_box = Box::new(idx_table);
                let idx_table_ptr = &mut *idx_table_box as *mut Table;

                // Backfill index from source table
                self.backfill_index(owner_id, source_col_idx, is_unique, idx_table_ptr, &idx_schema);

                // Register with DagEngine — Box ownership transfers here
                self.dag.add_index_circuit(owner_id, source_col_idx, idx_table_box, idx_schema, is_unique);

                // Register in index maps
                self.index_by_name.insert(name.clone(), idx_id);
                self.index_by_id.insert(idx_id, name.clone());
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
            let owned = Arc::try_unwrap(scan_batch).unwrap_or_else(|a| (*a).clone());
            let out_handle = self.dag.execute_epoch(vid, owned, source_id);
            if let Some(result) = out_handle {
                if result.count > 0 {
                    self.dag.ingest_to_family(vid, result);
                    let _ = self.dag.flush(vid);
                }
            }
        }
    }

    fn scan_store(&mut self, table_id: i64, schema: &SchemaDescriptor) -> Arc<OwnedBatch> {
        let entry = match self.dag.tables.get(&table_id) {
            Some(e) => e,
            None => return Arc::new(OwnedBatch::empty(schema.num_columns as usize - 1)),
        };

        // Extract single-Table handle (Single, Borrowed, or 1-partition Partitioned)
        let tbl: Option<&mut Table> = match &entry.handle {
            StoreHandle::Single(ref t) => {
                Some(unsafe { &mut *(std::ptr::addr_of!(**t) as *mut Table) })
            }
            StoreHandle::Borrowed(ptr) => Some(unsafe { &mut **ptr }),
            StoreHandle::Partitioned(ref pt) => {
                let ptbl = unsafe { &mut *(std::ptr::addr_of!(**pt) as *mut PartitionedTable) };
                if ptbl.num_partitions() == 1 {
                    Some(ptbl.table_mut(0))
                } else {
                    None
                }
            }
        };

        // Single-Table path: try fast paths, then build cursor from already-computed data
        let mut cursor = if let Some(table) = tbl {
            let _ = table.compact_if_needed();
            let snapshot = table.get_snapshot();
            let shards = table.all_shard_arcs();

            // Snapshot is consolidated → all weights > 0 → every row passes filter
            if shards.is_empty() && snapshot.count > 0 {
                return snapshot;
            }

            // All weights non-zero and shard is sorted+consolidated
            if snapshot.count == 0 && shards.len() == 1 && !shards[0].has_ghosts {
                return Arc::new(shards[0].to_owned_batch(schema));
            }

            crate::storage::create_cursor_from_snapshots(&[snapshot], &shards, *schema)
        } else {
            // Multi-partition: delegate to PartitionedTable
            match &entry.handle {
                StoreHandle::Partitioned(ref pt) => {
                    let ptbl = unsafe { &mut *(std::ptr::addr_of!(**pt) as *mut PartitionedTable) };
                    match ptbl.create_cursor() {
                        Ok(c) => c,
                        Err(_) => return Arc::new(OwnedBatch::empty(schema.num_columns as usize - 1)),
                    }
                }
                _ => unreachable!(),
            }
        };

        let estimated = cursor.cursor.estimated_length().max(8);
        let mut batch = OwnedBatch::with_schema(*schema, estimated);
        while cursor.cursor.valid {
            if cursor.cursor.current_weight > 0 {
                self.copy_cursor_row_to_batch(&cursor, schema, &mut batch);
            }
            cursor.cursor.advance();
        }
        Arc::new(batch)
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
            let npc = projected.num_payload_cols();
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
        cursor.cursor.seek(crate::util::make_pk(start_pk, 0));

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
                self.fk_parent_map.entry(cd.fk_table_id)
                    .or_default()
                    .push((table_id, col_idx));
            }
        }
        if !constraints.is_empty() {
            self.fk_constraints.insert(table_id, constraints);
        }
    }

    /// Returns all child tables that have FK constraints targeting `parent_id`.
    fn fk_children_of(&self, parent_id: i64) -> &[(i64, usize)] {
        self.fk_parent_map.get(&parent_id).map(|v| v.as_slice()).unwrap_or(&[])
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
        let col = batch.col_data(payload_col);
        if off + 8 > col.len() { return 0; }
        u64::from_le_bytes(col[off..off + 8].try_into().unwrap_or([0; 8]))
    }

    fn read_batch_string(&self, batch: &OwnedBatch, row: usize, payload_col: usize) -> String {
        let off = row * 16;
        let data = batch.col_data(payload_col);
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

    pub(crate) fn create_table(
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

        // Retract table record — cursor-based to match exact stored payload
        {
            let schema = table_tab_schema();
            let batch = retract_single_row(&mut self.sys_tables, &schema, tid as u64, 0);
            if batch.count > 0 {
                ingest_batch_into(&mut self.sys_tables, &batch);
                self.on_table_delta(&batch)?;
            }
        }

        // Retract column records
        let col_defs = self.read_column_defs(tid)?;
        self.retract_column_records(tid, OWNER_KIND_TABLE, &col_defs);

        Ok(())
    }

    // -- DDL: CREATE/DROP VIEW ---------------------------------------------

    pub(crate) fn create_view(
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

        // Retract view record — cursor-based to match exact stored payload
        {
            let schema = view_tab_schema();
            let batch = retract_single_row(&mut self.sys_views, &schema, vid as u64, 0);
            if batch.count > 0 {
                ingest_batch_into(&mut self.sys_views, &batch);
                self.on_view_delta(&batch)?;
            }
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
        cursor.cursor.seek(crate::util::make_pk(idx_id as u64, 0));

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
        let children = self.fk_children_of(tid);
        if let Some(&(child_tid, _)) = children.first() {
            let (sn, tn) = self.id_to_qualified.get(&child_tid)
                .cloned().unwrap_or_default();
            return Err(format!("FK dependency: table '{}.{}'", sn, tn));
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
        cursor.cursor.seek(crate::util::make_pk(0, vid as u64));

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

    fn retract_circuit_graph(&mut self, vid: i64) {
        let pk_hi = vid as u64;
        for &tab_id in &[CIRCUIT_NODES_TAB_ID, CIRCUIT_EDGES_TAB_ID,
                         CIRCUIT_SOURCES_TAB_ID, CIRCUIT_PARAMS_TAB_ID,
                         CIRCUIT_GROUP_COLS_TAB_ID] {
            let schema = sys_tab_schema(tab_id);
            let table = self.sys_table_mut(tab_id).unwrap();
            retract_and_ingest(table, &schema, pk_hi);
        }
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
    pub fn ingest_to_family(&mut self, table_id: i64, batch: &OwnedBatch) -> Result<(), String> {
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
            // Clone batch for hooks (hooks need to read it after table borrows it)
            let mut batch_for_hooks = batch.clone();
            batch_for_hooks.schema = Some(schema);
            ingest_batch_into(table, batch);
            self.fire_hooks(table_id, &batch_for_hooks)?;
            Ok(())
        } else {
            // User table: DagEngine handles unique_pk, store ingest, index projection.
            let rc = self.dag.ingest_by_ref(table_id, batch);
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
    pub fn ingest_returning_effective(
        &mut self, table_id: i64, batch: OwnedBatch,
    ) -> Result<OwnedBatch, String> {
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
    pub fn scan_family(&mut self, table_id: i64) -> Result<Arc<OwnedBatch>, String> {
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
                StoreHandle::Single(ref t) => {
                    let tbl = unsafe { &mut *(std::ptr::addr_of!(**t) as *mut Table) };
                    tbl.create_cursor()
                }
                StoreHandle::Borrowed(ptr) => unsafe { &mut **ptr }.create_cursor(),
                StoreHandle::Partitioned(ref pt) => {
                    let ptbl = unsafe { &mut *(std::ptr::addr_of!(**pt) as *mut PartitionedTable) };
                    ptbl.create_cursor()
                }
            }
        };

        let mut cursor = match cursor_result {
            Ok(c) => c,
            Err(_) => return Ok(None),
        };

        cursor.cursor.seek(crate::util::make_pk(pk_lo, pk_hi));
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

        // Seek in index table
        let idx_table = unsafe { &mut *(std::ptr::addr_of!(*ic.index_table) as *mut Table) };
        let mut cursor = idx_table.create_cursor().map_err(|e| format!("cursor error: {}", e))?;
        cursor.cursor.seek(crate::util::make_pk(key_lo, key_hi));
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
        let npc = batch.num_payload_cols();
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
        let npc = batch.num_payload_cols();
        let (ptrs, sizes) = batch.to_region_ptrs();
        match &entry.handle {
            StoreHandle::Single(ref t) => {
                let table = unsafe { &mut *(std::ptr::addr_of!(**t) as *mut Table) };
                let _ = table.ingest_batch_from_regions(&ptrs, &sizes, batch.count as u32, npc);
            }
            StoreHandle::Borrowed(ptr) => {
                let table = unsafe { &mut **ptr };
                let _ = table.ingest_batch_from_regions(&ptrs, &sizes, batch.count as u32, npc);
            }
            StoreHandle::Partitioned(ref pt) => {
                let ptable = unsafe { &mut *(std::ptr::addr_of!(**pt) as *mut PartitionedTable) };
                let _ = ptable.ingest_batch_from_regions(&ptrs, &sizes, batch.count as u32, npc);
            }
        }
        Ok(())
    }

    /// SAL recovery replay — unique_pk-aware. Routes user-table batches
    /// through the full `ingest_returning_effective` path so that
    /// retractions (which carry zero-padded payloads on the wire) are
    /// resolved against the actual stored payload instead of being
    /// added as orphaned rows. The retract-and-insert pattern in
    /// `enforce_unique_pk_partitioned` makes the replay idempotent
    /// w.r.t. already-flushed data.
    ///
    /// Index shards see duplicate `(+1, -1)` projections when a batch
    /// is replayed after already having been flushed. These consolidate
    /// to zero on read and are pruned at the next compaction.
    pub fn replay_ingest(&mut self, table_id: i64, batch: OwnedBatch) -> Result<(), String> {
        if table_id < FIRST_USER_TABLE_ID {
            // System tables: use raw ingest (no unique_pk semantics).
            return self.raw_store_ingest(table_id, batch);
        }
        let (rc, _effective) = self.dag.ingest_returning_effective(table_id, batch);
        if rc < 0 {
            return Err(format!("replay_ingest failed for table_id={} rc={}", table_id, rc));
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
            if let StoreHandle::Partitioned(ref pt) = &entry.handle {
                let ptable = unsafe { &mut *(std::ptr::addr_of!(**pt) as *mut PartitionedTable) };
                ptable.close_all_partitions();
            }
        }
    }

    /// Trim worker partitions to assigned range.
    pub fn trim_worker_partitions(&mut self, start: u32, end: u32) {
        for (&tid, entry) in &self.dag.tables {
            if tid < FIRST_USER_TABLE_ID { continue; }
            if let StoreHandle::Partitioned(ref pt) = &entry.handle {
                let ptable = unsafe { &mut *(std::ptr::addr_of!(**pt) as *mut PartitionedTable) };
                ptable.close_partitions_outside(start, end);
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
            .map(|ic| std::ptr::addr_of!(*ic.index_table) as *mut Table)
            .unwrap_or(std::ptr::null_mut())
    }

    /// Get the SchemaDescriptor for the index circuit at position idx.
    pub fn get_index_circuit_schema(&self, table_id: i64, idx: usize) -> Option<SchemaDescriptor> {
        self.dag.tables.get(&table_id)
            .and_then(|e| e.index_circuits.get(idx))
            .map(|ic| ic.index_schema)
    }

    /// Number of child tables that reference `parent_id` via FK.
    pub fn get_fk_children_count(&self, parent_id: i64) -> usize {
        self.fk_parent_map.get(&parent_id).map(|v| v.len()).unwrap_or(0)
    }

    /// True if the table has at least one unique secondary index circuit.
    /// Used to decide whether distributed unique-index validation is needed.
    /// Non-unique circuits (e.g. FK indices) do not count.
    pub fn has_any_unique_index(&self, table_id: i64) -> bool {
        self.dag.tables.get(&table_id)
            .map(|e| e.index_circuits.iter().any(|ic| ic.is_unique))
            .unwrap_or(false)
    }

    /// True if the table was created with `unique_pk=true`. Used by the
    /// distributed validator to decide whether Error-mode inserts need
    /// an against-store PK rejection broadcast.
    pub fn table_has_unique_pk(&self, table_id: i64) -> bool {
        self.dag.tables.get(&table_id)
            .map(|e| e.unique_pk)
            .unwrap_or(false)
    }

    /// Get child info at index: (child_table_id, fk_col_idx).
    pub fn get_fk_child_info(&self, parent_id: i64, idx: usize) -> Option<(i64, usize)> {
        self.fk_parent_map.get(&parent_id)
            .and_then(|v| v.get(idx))
            .copied()
    }

    /// Get the index schema for a specific column's FK index on a table.
    pub fn get_index_schema_by_col(&self, table_id: i64, col_idx: u32) -> Option<SchemaDescriptor> {
        self.dag.tables.get(&table_id)
            .and_then(|e| e.index_circuits.iter().find(|ic| ic.col_idx == col_idx))
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
                StoreHandle::Partitioned(ref pt) => Some(std::ptr::addr_of!(**pt) as *mut PartitionedTable),
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
            StoreHandle::Single(ref t) => t.current_lsn,
            StoreHandle::Borrowed(ptr) => unsafe { &**ptr }.current_lsn,
            StoreHandle::Partitioned(ref pt) => pt.current_lsn(),
        }
    }

    // -- Close engine ------------------------------------------------------

    /// Flush all system tables (memtable → shard). Called at checkpoint and close.
    pub fn flush_all_system_tables(&mut self) {
        for info in SYS_TAB_INFOS {
            if let Some(table) = self.sys_table_mut(info.id) {
                let _ = table.flush_durable();
            }
        }
    }

    pub fn close(&mut self) {
        // Flush and close all user tables before clearing DagEngine
        let user_tids: Vec<i64> = self.dag.tables.keys()
            .filter(|&&tid| tid >= FIRST_USER_TABLE_ID)
            .copied()
            .collect();
        for tid in &user_tids {
            if let Some(entry) = self.dag.tables.get_mut(tid) {
                match &mut entry.handle {
                    StoreHandle::Single(t) => { let _ = t.flush(); }
                    StoreHandle::Partitioned(pt) => { let _ = pt.flush(); pt.close(); }
                    StoreHandle::Borrowed(_) => {} // system tables flushed below
                }
            }
        }
        // tables.clear() in dag.close() drops Box<Table>/Box<PartitionedTable> automatically.
        self.dag.close();
        self.flush_all_system_tables();
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
                let fk_key = crate::util::make_pk(fk_lo, fk_hi);
                let has = match &target_entry.handle {
                    StoreHandle::Single(ref t) => {
                        let table = unsafe { &mut *(std::ptr::addr_of!(**t) as *mut Table) };
                        table.has_pk(fk_key)
                    }
                    StoreHandle::Borrowed(ptr) => unsafe { &mut **ptr }.has_pk(fk_key),
                    StoreHandle::Partitioned(ref pt) => {
                        let ptable = unsafe { &mut *(std::ptr::addr_of!(**pt) as *mut PartitionedTable) };
                        ptable.has_pk(fk_key)
                    }
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

    /// Validate FK RESTRICT on parent DELETE (single-worker path).
    /// For each retraction row, checks whether any child table still references
    /// the PK being deleted via the child's FK index. Returns an error if so.
    pub fn validate_fk_parent_restrict(&self, table_id: i64, batch: &OwnedBatch) -> Result<(), String> {
        let children = self.fk_children_of(table_id);
        if children.is_empty() { return Ok(()); }

        for &(child_tid, fk_col_idx) in children {
            let child_entry = match self.dag.tables.get(&child_tid) {
                Some(e) => e,
                None => continue,
            };
            let ic = match child_entry.index_circuits.iter()
                .find(|ic| ic.col_idx == fk_col_idx as u32)
            {
                Some(ic) => ic,
                None => {
                    return Err(format!(
                        "FK RESTRICT check failed: no index on child table {} col {}",
                        child_tid, fk_col_idx,
                    ));
                }
            };
            let idx_table = unsafe { &mut *(std::ptr::addr_of!(*ic.index_table) as *mut Table) };

            for row in 0..batch.count {
                if batch.get_weight(row) >= 0 { continue; }
                let pk = batch.get_pk(row);
                if idx_table.has_pk(pk) {
                    let (sn, tn) = self.id_to_qualified.get(&table_id)
                        .cloned().unwrap_or_default();
                    let (csn, ctn) = self.id_to_qualified.get(&child_tid)
                        .cloned().unwrap_or_default();
                    return Err(format!(
                        "Foreign Key violation: cannot delete from '{}.{}', row still referenced by '{}.{}'",
                        sn, tn, csn, ctn,
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
            let source_col_idx = ic.col_idx as usize;
            let col_type = schema.columns[source_col_idx].type_code;
            let payload_col = if source_col_idx < pk_index {
                source_col_idx
            } else {
                source_col_idx - 1
            };

            let idx_table = unsafe { &mut *(std::ptr::addr_of!(*ic.index_table) as *mut Table) };

            // Track seen keys for batch-internal duplicate detection
            let mut seen: HashMap<(u64, u64), bool> = HashMap::new();

            for row in 0..batch.count {
                if batch.get_weight(row) <= 0 { continue; }

                // Skip null values
                let null_word = batch.get_null_word(row);
                if null_word & (1u64 << payload_col) != 0 { continue; }

                let row_pk = batch.get_pk(row);

                // Determine if this is an UPSERT (PK already exists in store)
                let is_upsert = if entry.unique_pk {
                    match &entry.handle {
                        StoreHandle::Partitioned(ref pt) => {
                            let ptable = unsafe { &mut *(std::ptr::addr_of!(**pt) as *mut PartitionedTable) };
                            ptable.has_pk(row_pk)
                        }
                        StoreHandle::Single(ref t) => {
                            let table = unsafe { &mut *(std::ptr::addr_of!(**t) as *mut Table) };
                            table.has_pk(row_pk)
                        }
                        StoreHandle::Borrowed(ptr) => unsafe { &mut **ptr }.has_pk(row_pk),
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
                let key_u128 = crate::util::make_pk(key_lo, key_hi);
                if idx_table.has_pk(key_u128) {
                    if is_upsert {
                        // For UPSERT: the index entry might belong to this same row
                        // (same value being re-inserted). Check the index payload (source PK).
                        // Index schema: PK = index_key, payload[0] = source_pk.
                        let (_net_w, found) = idx_table.retract_pk(key_u128);
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
                                let src_pk = crate::util::make_pk(src_pk_lo, src_pk_hi);
                                if src_pk != row_pk {
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
pub(crate) struct ColumnDef {
    pub(crate) name: String,
    pub(crate) type_code: u8,
    pub(crate) is_nullable: bool,
    pub(crate) fk_table_id: i64,
    pub(crate) fk_col_idx: u32,
}

/// Circuit graph for create_view.
pub(crate) struct CircuitGraph {
    pub(crate) nodes: Vec<(i32, i32)>,           // (node_id, opcode)
    pub(crate) edges: Vec<(i32, i32, i32, i32)>, // (edge_id, src, dst, port)
    pub(crate) sources: Vec<(i32, i64)>,         // (node_id, table_id)
    pub(crate) params: Vec<(i32, i32, i64)>,     // (node_id, slot, value)
    pub(crate) group_cols: Vec<(i32, i32)>,      // (node_id, col_idx)
    pub(crate) output_col_defs: Vec<(String, u8)>, // (name, type_code)
    pub(crate) dependencies: Vec<i64>,           // source table_ids
}
