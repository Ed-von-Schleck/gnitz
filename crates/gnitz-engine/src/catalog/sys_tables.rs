//! System table constants, schema definitions, and PK packing helpers.
//!
//! Pure data — no state, no CatalogEngine dependency.

use crate::schema::{SchemaColumn, SchemaDescriptor, type_code};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

pub(crate) const SYSTEM_SCHEMA_ID: i64 = 1;
pub(crate) const PUBLIC_SCHEMA_ID: i64 = 2;
pub(super) const FIRST_USER_SCHEMA_ID: i64 = gnitz_wire::FIRST_USER_SCHEMA_ID as i64;

pub(super) const OWNER_KIND_TABLE: i64 = gnitz_wire::OWNER_KIND_TABLE as i64;
pub(super) const OWNER_KIND_VIEW: i64 = gnitz_wire::OWNER_KIND_VIEW as i64;

pub(crate) const SEQ_ID_SCHEMAS: i64 = 1;
pub(crate) const SEQ_ID_TABLES: i64 = 2;
pub(crate) const SEQ_ID_INDICES: i64 = 3;
pub(super) const SEQ_ID_PROGRAMS: i64 = 4;

pub(crate) const FIRST_USER_TABLE_ID: i64 = gnitz_wire::FIRST_USER_TABLE_ID as i64;
pub(super) const FIRST_USER_INDEX_ID: i64 = 1;

pub(super) const SYS_CATALOG_DIRNAME: &str = "_system_catalog";
pub(super) const NUM_PARTITIONS: u32 = 256;

pub(super) const SCHEMA_TAB_ID: i64             = gnitz_wire::SCHEMA_TAB as i64;
pub(super) const TABLE_TAB_ID: i64              = gnitz_wire::TABLE_TAB as i64;
pub(super) const VIEW_TAB_ID: i64               = gnitz_wire::VIEW_TAB as i64;
pub(super) const COL_TAB_ID: i64                = gnitz_wire::COL_TAB as i64;
pub(super) const IDX_TAB_ID: i64                = gnitz_wire::IDX_TAB as i64;
pub(super) const DEP_TAB_ID: i64                = gnitz_wire::DEP_TAB as i64;
pub(super) const SEQ_TAB_ID: i64                = gnitz_wire::SEQ_TAB as i64;
pub(super) const CIRCUIT_NODES_TAB_ID: i64      = gnitz_wire::CIRCUIT_NODES_TAB as i64;
pub(super) const CIRCUIT_EDGES_TAB_ID: i64      = gnitz_wire::CIRCUIT_EDGES_TAB as i64;
pub(super) const CIRCUIT_SOURCES_TAB_ID: i64    = gnitz_wire::CIRCUIT_SOURCES_TAB as i64;
pub(super) const CIRCUIT_PARAMS_TAB_ID: i64     = gnitz_wire::CIRCUIT_PARAMS_TAB as i64;
pub(super) const CIRCUIT_GROUP_COLS_TAB_ID: i64 = gnitz_wire::CIRCUIT_GROUP_COLS_TAB as i64;

// Column indices for system tables (matching COL_* constants).
pub(super) const TABLETAB_COL_SCHEMA_ID: usize = 1;
pub(super) const TABLETAB_COL_NAME: usize = 2;
pub(super) const TABLETAB_COL_DIRECTORY: usize = 3;
pub(super) const TABLETAB_COL_PK_COL_IDX: usize = 4;
pub(super) const TABLETAB_COL_CREATED_LSN: usize = 5;
pub(super) const TABLETAB_COL_FLAGS: usize = 6;
pub(super) const TABLETAB_FLAG_UNIQUE_PK: u64 = 1;

pub(super) const SCHEMATAB_COL_NAME: usize = 1;

pub(super) const VIEWTAB_COL_SCHEMA_ID: usize = 1;
pub(super) const VIEWTAB_COL_NAME: usize = 2;
pub(super) const VIEWTAB_COL_SQL_DEFINITION: usize = 3;
pub(super) const VIEWTAB_COL_CACHE_DIRECTORY: usize = 4;
pub(super) const VIEWTAB_COL_CREATED_LSN: usize = 5;

pub(super) const COLTAB_COL_OWNER_ID: usize = 1;
pub(super) const COLTAB_COL_OWNER_KIND: usize = 2;
pub(super) const COLTAB_COL_COL_IDX: usize = 3;
pub(super) const COLTAB_COL_NAME: usize = 4;
pub(super) const COLTAB_COL_TYPE_CODE: usize = 5;
pub(super) const COLTAB_COL_IS_NULLABLE: usize = 6;
pub(super) const COLTAB_COL_FK_TABLE_ID: usize = 7;
pub(super) const COLTAB_COL_FK_COL_IDX: usize = 8;

pub(super) const IDXTAB_COL_OWNER_ID: usize = 1;
pub(super) const IDXTAB_COL_OWNER_KIND: usize = 2;
pub(super) const IDXTAB_COL_SOURCE_COL_IDX: usize = 3;
pub(super) const IDXTAB_COL_NAME: usize = 4;
pub(super) const IDXTAB_COL_IS_UNIQUE: usize = 5;
pub(super) const IDXTAB_COL_CACHE_DIRECTORY: usize = 6;

pub(super) const DEPTAB_COL_VIEW_ID: usize = 1;
pub(super) const DEPTAB_COL_DEP_VIEW_ID: usize = 2;
pub(super) const DEPTAB_COL_DEP_TABLE_ID: usize = 3;

pub(super) const SEQTAB_COL_VALUE: usize = 1;

// Default arena sizes for system tables and user tables
pub(super) const SYS_TABLE_ARENA: u64 = 256 * 1024;          // 256 KB

// ---------------------------------------------------------------------------
// Schema builder helpers
// ---------------------------------------------------------------------------

pub(super) const fn u64_col() -> SchemaColumn {
    SchemaColumn { type_code: type_code::U64, size: 8, nullable: 0, _pad: 0 }
}
pub(super) const fn u128_col() -> SchemaColumn {
    SchemaColumn { type_code: type_code::U128, size: 16, nullable: 0, _pad: 0 }
}
pub(super) const fn str_col() -> SchemaColumn {
    SchemaColumn { type_code: type_code::STRING, size: 16, nullable: 0, _pad: 0 }
}
pub(super) const fn str_col_nullable() -> SchemaColumn {
    SchemaColumn { type_code: type_code::STRING, size: 16, nullable: 1, _pad: 0 }
}
pub(super) const fn zero_col() -> SchemaColumn {
    SchemaColumn { type_code: 0, size: 0, nullable: 0, _pad: 0 }
}

pub(super) const fn make_schema(cols: &[SchemaColumn], pk_index: u32) -> SchemaDescriptor {
    let mut sd = SchemaDescriptor {
        num_columns: cols.len() as u32,
        pk_index,
        columns: [zero_col(); 64],
    };
    let mut i = 0;
    while i < cols.len() {
        sd.columns[i] = cols[i];
        i += 1;
    }
    sd
}

// ---------------------------------------------------------------------------
// System table schema definitions
// ---------------------------------------------------------------------------

pub(super) const fn schema_tab_schema() -> SchemaDescriptor {
    make_schema(&[u64_col(), str_col()], 0)
}
pub(super) const fn table_tab_schema() -> SchemaDescriptor {
    make_schema(&[u64_col(), u64_col(), str_col(), str_col(), u64_col(), u64_col(), u64_col()], 0)
}
pub(super) const fn view_tab_schema() -> SchemaDescriptor {
    make_schema(&[u64_col(), u64_col(), str_col(), str_col(), str_col(), u64_col()], 0)
}
pub(super) const fn col_tab_schema() -> SchemaDescriptor {
    make_schema(&[u64_col(), u64_col(), u64_col(), u64_col(), str_col(), u64_col(), u64_col(), u64_col(), u64_col()], 0)
}
pub(super) const fn idx_tab_schema() -> SchemaDescriptor {
    make_schema(&[u64_col(), u64_col(), u64_col(), u64_col(), str_col(), u64_col(), str_col()], 0)
}
pub(super) const fn dep_tab_schema() -> SchemaDescriptor {
    make_schema(&[u128_col(), u64_col(), u64_col(), u64_col()], 0)
}
pub(super) const fn seq_tab_schema() -> SchemaDescriptor {
    make_schema(&[u64_col(), u64_col()], 0)
}
pub(super) const fn circuit_nodes_schema() -> SchemaDescriptor {
    make_schema(&[u128_col(), u64_col()], 0)
}
pub(super) const fn circuit_edges_schema() -> SchemaDescriptor {
    make_schema(&[u128_col(), u64_col(), u64_col(), u64_col()], 0)
}
pub(super) const fn circuit_sources_schema() -> SchemaDescriptor {
    make_schema(&[u128_col(), u64_col()], 0)
}
pub(super) const fn circuit_params_schema() -> SchemaDescriptor {
    make_schema(&[u128_col(), u64_col(), str_col_nullable()], 0)
}
pub(super) const fn circuit_group_cols_schema() -> SchemaDescriptor {
    make_schema(&[u128_col(), u64_col()], 0)
}

// Pre-computed statics — initialised once at program start, never reconstructed.
static S_SCHEMA_TAB:        SchemaDescriptor = schema_tab_schema();
static S_TABLE_TAB:         SchemaDescriptor = table_tab_schema();
static S_VIEW_TAB:          SchemaDescriptor = view_tab_schema();
static S_COL_TAB:           SchemaDescriptor = col_tab_schema();
static S_IDX_TAB:           SchemaDescriptor = idx_tab_schema();
static S_DEP_TAB:           SchemaDescriptor = dep_tab_schema();
static S_SEQ_TAB:           SchemaDescriptor = seq_tab_schema();
static S_CIRCUIT_NODES:     SchemaDescriptor = circuit_nodes_schema();
static S_CIRCUIT_EDGES:     SchemaDescriptor = circuit_edges_schema();
static S_CIRCUIT_SOURCES:   SchemaDescriptor = circuit_sources_schema();
static S_CIRCUIT_PARAMS:    SchemaDescriptor = circuit_params_schema();
static S_CIRCUIT_GROUP_COLS: SchemaDescriptor = circuit_group_cols_schema();

// ---------------------------------------------------------------------------
// PK packing helpers
// ---------------------------------------------------------------------------

pub(super) fn pack_column_id(owner_id: i64, col_idx: i64) -> u64 {
    ((owner_id as u64) << 9) | (col_idx as u64)
}

pub(super) fn pack_node_pk(view_id: i64, node_id: i64) -> (u64, u64) {
    (node_id as u64, view_id as u64)
}

pub(super) fn pack_edge_pk(view_id: i64, edge_id: i64) -> (u64, u64) {
    (edge_id as u64, view_id as u64)
}

pub(super) fn pack_param_pk(view_id: i64, node_id: i64, slot: i64) -> (u64, u64) {
    let lo = ((node_id as u64) << 8) | (slot as u64);
    (lo, view_id as u64)
}

pub(super) fn pack_dep_pk(view_id: i64, dep_tid: i64) -> (u64, u64) {
    (dep_tid as u64, view_id as u64)
}

pub(super) fn pack_gcol_pk(view_id: i64, node_id: i64, col_idx: i64) -> (u64, u64) {
    let lo = ((node_id as u64) << 16) | (col_idx as u64);
    (lo, view_id as u64)
}

// ---------------------------------------------------------------------------
// System table subdirectory names
// ---------------------------------------------------------------------------

pub(super) struct SysTabInfo {
    pub(super) id: i64,
    pub(super) subdir: &'static str,
    pub(super) name: &'static str,
}

pub(super) const SYS_TAB_INFOS: &[SysTabInfo] = &[
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

pub(super) fn sys_tab_schema(id: i64) -> SchemaDescriptor {
    match id {
        SCHEMA_TAB_ID             => S_SCHEMA_TAB,
        TABLE_TAB_ID              => S_TABLE_TAB,
        VIEW_TAB_ID               => S_VIEW_TAB,
        COL_TAB_ID                => S_COL_TAB,
        IDX_TAB_ID                => S_IDX_TAB,
        DEP_TAB_ID                => S_DEP_TAB,
        SEQ_TAB_ID                => S_SEQ_TAB,
        CIRCUIT_NODES_TAB_ID      => S_CIRCUIT_NODES,
        CIRCUIT_EDGES_TAB_ID      => S_CIRCUIT_EDGES,
        CIRCUIT_SOURCES_TAB_ID    => S_CIRCUIT_SOURCES,
        CIRCUIT_PARAMS_TAB_ID     => S_CIRCUIT_PARAMS,
        CIRCUIT_GROUP_COLS_TAB_ID => S_CIRCUIT_GROUP_COLS,
        _ => unreachable!("Unknown system table ID: {}", id),
    }
}
