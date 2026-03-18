use gnitz_protocol::{Schema, ColumnDef, TypeCode};

// Circuit system table IDs
pub const CIRCUIT_NODES_TAB:      u64 = 11;
pub const CIRCUIT_EDGES_TAB:      u64 = 12;
pub const CIRCUIT_SOURCES_TAB:    u64 = 13;
pub const CIRCUIT_PARAMS_TAB:     u64 = 14;
pub const CIRCUIT_GROUP_COLS_TAB: u64 = 15;

// Owner kinds
pub const OWNER_KIND_TABLE: u64 = 0;
pub const OWNER_KIND_VIEW:  u64 = 1;

// Opcodes
pub const OPCODE_SCAN_TRACE:            u64 = 11;
pub const OPCODE_INTEGRATE:             u64 = 7;
pub const OPCODE_FILTER:                u64 = 1;
pub const OPCODE_MAP:                   u64 = 2;
pub const OPCODE_NEGATE:                u64 = 3;
pub const OPCODE_UNION:                 u64 = 4;
pub const OPCODE_JOIN_DELTA_TRACE:      u64 = 5;
pub const OPCODE_JOIN_DELTA_DELTA:      u64 = 6;
pub const OPCODE_DELAY:                 u64 = 8;
pub const OPCODE_REDUCE:                u64 = 9;
pub const OPCODE_DISTINCT:              u64 = 10;
pub const OPCODE_ANTI_JOIN_DELTA_TRACE: u64 = 16;
pub const OPCODE_ANTI_JOIN_DELTA_DELTA: u64 = 17;
pub const OPCODE_SEMI_JOIN_DELTA_TRACE: u64 = 18;
pub const OPCODE_SEMI_JOIN_DELTA_DELTA: u64 = 19;
pub const OPCODE_EXCHANGE_SHARD:        u64 = 20;
pub const OPCODE_EXCHANGE_GATHER:       u64 = 21;
pub const OPCODE_JOIN_DELTA_TRACE_OUTER: u64 = 22;

// Port constants
pub const PORT_IN:    u64 = 0;
pub const PORT_TRACE: u64 = 1;
pub const PORT_IN_A:  u64 = 0;
pub const PORT_IN_B:  u64 = 1;

// Param slots
pub const PARAM_FUNC_ID:         u64 = 0;
pub const PARAM_AGG_FUNC_ID:     u64 = 1;
pub const PARAM_AGG_COL_IDX:     u64 = 6;
pub const PARAM_PROJ_BASE:       u64 = 32;
pub const PARAM_EXPR_BASE:       u64 = 64;
pub const PARAM_EXPR_NUM_REGS:   u64 = 7;
pub const PARAM_EXPR_RESULT_REG: u64 = 8;
pub const PARAM_REINDEX_COL:       u64 = 10;
pub const PARAM_JOIN_SOURCE_TABLE: u64 = 11;
pub const PARAM_SHARD_COL_BASE:    u64 = 128;
pub const PARAM_CONST_STR_BASE:    u64 = 160;
pub const PARAM_GATHER_WORKER:     u64 = 9;
pub const PARAM_TABLE_ID:          u64 = 3;
pub const PARAM_AGG_COUNT:         u64 = 12;
pub const PARAM_AGG_SPEC_BASE:     u64 = 13;

// Aggregate function IDs (must match gnitz/dbsp/functions.py)
pub const AGG_COUNT:          u64 = 1;
pub const AGG_SUM:            u64 = 2;
pub const AGG_MIN:            u64 = 3;
pub const AGG_MAX:            u64 = 4;
pub const AGG_COUNT_NON_NULL: u64 = 5;

// --- Schema constructor functions ---

pub fn schema_tab_schema() -> Schema {
    Schema {
        columns: vec![
            ColumnDef { name: "schema_id".into(), type_code: TypeCode::U64,    is_nullable: false },
            ColumnDef { name: "name".into(),      type_code: TypeCode::String, is_nullable: false },
        ],
        pk_index: 0,
    }
}

pub fn table_tab_schema() -> Schema {
    Schema {
        columns: vec![
            ColumnDef { name: "table_id".into(),    type_code: TypeCode::U64,    is_nullable: false },
            ColumnDef { name: "schema_id".into(),   type_code: TypeCode::U64,    is_nullable: false },
            ColumnDef { name: "name".into(),        type_code: TypeCode::String, is_nullable: false },
            ColumnDef { name: "directory".into(),   type_code: TypeCode::String, is_nullable: false },
            ColumnDef { name: "pk_col_idx".into(),  type_code: TypeCode::U64,    is_nullable: false },
            ColumnDef { name: "created_lsn".into(), type_code: TypeCode::U64,    is_nullable: false },
            ColumnDef { name: "flags".into(),       type_code: TypeCode::U64,    is_nullable: false },
        ],
        pk_index: 0,
    }
}

pub fn col_tab_schema() -> Schema {
    Schema {
        columns: vec![
            ColumnDef { name: "column_id".into(),   type_code: TypeCode::U64,    is_nullable: false },
            ColumnDef { name: "owner_id".into(),    type_code: TypeCode::U64,    is_nullable: false },
            ColumnDef { name: "owner_kind".into(),  type_code: TypeCode::U64,    is_nullable: false },
            ColumnDef { name: "col_idx".into(),     type_code: TypeCode::U64,    is_nullable: false },
            ColumnDef { name: "name".into(),        type_code: TypeCode::String, is_nullable: false },
            ColumnDef { name: "type_code".into(),   type_code: TypeCode::U64,    is_nullable: false },
            ColumnDef { name: "is_nullable".into(), type_code: TypeCode::U64,    is_nullable: false },
            ColumnDef { name: "fk_table_id".into(), type_code: TypeCode::U64,    is_nullable: false },
            ColumnDef { name: "fk_col_idx".into(),  type_code: TypeCode::U64,    is_nullable: false },
        ],
        pk_index: 0,
    }
}

pub fn view_tab_schema() -> Schema {
    Schema {
        columns: vec![
            ColumnDef { name: "view_id".into(),          type_code: TypeCode::U64,    is_nullable: false },
            ColumnDef { name: "schema_id".into(),        type_code: TypeCode::U64,    is_nullable: false },
            ColumnDef { name: "name".into(),             type_code: TypeCode::String, is_nullable: false },
            ColumnDef { name: "sql_definition".into(),   type_code: TypeCode::String, is_nullable: false },
            ColumnDef { name: "cache_directory".into(),  type_code: TypeCode::String, is_nullable: false },
            ColumnDef { name: "created_lsn".into(),      type_code: TypeCode::U64,    is_nullable: false },
        ],
        pk_index: 0,
    }
}

pub fn dep_tab_schema() -> Schema {
    Schema {
        columns: vec![
            ColumnDef { name: "dep_pk".into(),       type_code: TypeCode::U128, is_nullable: false },
            ColumnDef { name: "view_id".into(),      type_code: TypeCode::U64,  is_nullable: false },
            ColumnDef { name: "dep_view_id".into(),  type_code: TypeCode::U64,  is_nullable: false },
            ColumnDef { name: "dep_table_id".into(), type_code: TypeCode::U64,  is_nullable: false },
        ],
        pk_index: 0,
    }
}

pub fn circuit_nodes_schema() -> Schema {
    Schema {
        columns: vec![
            ColumnDef { name: "node_pk".into(), type_code: TypeCode::U128, is_nullable: false },
            ColumnDef { name: "opcode".into(),  type_code: TypeCode::U64,  is_nullable: false },
        ],
        pk_index: 0,
    }
}

pub fn circuit_edges_schema() -> Schema {
    Schema {
        columns: vec![
            ColumnDef { name: "edge_pk".into(),  type_code: TypeCode::U128, is_nullable: false },
            ColumnDef { name: "src_node".into(), type_code: TypeCode::U64,  is_nullable: false },
            ColumnDef { name: "dst_node".into(), type_code: TypeCode::U64,  is_nullable: false },
            ColumnDef { name: "dst_port".into(), type_code: TypeCode::U64,  is_nullable: false },
        ],
        pk_index: 0,
    }
}

pub fn circuit_sources_schema() -> Schema {
    Schema {
        columns: vec![
            ColumnDef { name: "source_pk".into(), type_code: TypeCode::U128, is_nullable: false },
            ColumnDef { name: "table_id".into(),  type_code: TypeCode::U64,  is_nullable: false },
        ],
        pk_index: 0,
    }
}

pub fn circuit_params_schema() -> Schema {
    Schema {
        columns: vec![
            ColumnDef { name: "param_pk".into(),  type_code: TypeCode::U128,   is_nullable: false },
            ColumnDef { name: "value".into(),     type_code: TypeCode::U64,    is_nullable: false },
            ColumnDef { name: "str_value".into(), type_code: TypeCode::String, is_nullable: true },
        ],
        pk_index: 0,
    }
}

pub fn circuit_group_cols_schema() -> Schema {
    Schema {
        columns: vec![
            ColumnDef { name: "gcol_pk".into(), type_code: TypeCode::U128, is_nullable: false },
            ColumnDef { name: "col_idx".into(), type_code: TypeCode::U64,  is_nullable: false },
        ],
        pk_index: 0,
    }
}

// --- CircuitGraph ---

/// Data bag describing a full circuit graph for `create_view_with_circuit`.
#[derive(Clone, Debug)]
pub struct CircuitGraph {
    pub view_id:           u64,
    pub primary_source_id: u64,
    pub nodes:         Vec<(u64, u64)>,           // (node_id, opcode)
    pub edges:         Vec<(u64, u64, u64, u64)>, // (edge_id, src, dst, port)
    pub sources:       Vec<(u64, u64)>,           // (node_id, table_id)
    pub params:        Vec<(u64, u64, u64)>,      // (node_id, slot, value)
    pub group_cols:    Vec<(u64, u64)>,           // (node_id, col_idx)
    pub const_strings: Vec<(u64, u64, String)>,   // (node_id, slot, string_value)
    pub dependencies:  Vec<u64>,                  // dep table_ids
}
