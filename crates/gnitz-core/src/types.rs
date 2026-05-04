use std::sync::OnceLock;
use crate::protocol::{Schema, ColumnDef, TypeCode};

pub use gnitz_wire::{
    // System table IDs
    CIRCUIT_NODES_TAB, CIRCUIT_EDGES_TAB, CIRCUIT_NODE_COLUMNS_TAB,
    // Owner kinds
    OWNER_KIND_TABLE, OWNER_KIND_VIEW,
    // Opcodes
    OPCODE_SCAN_TRACE, OPCODE_INTEGRATE,
    OPCODE_FILTER, OPCODE_MAP, OPCODE_NEGATE, OPCODE_UNION,
    OPCODE_JOIN_DELTA_TRACE, OPCODE_JOIN_DELTA_DELTA, OPCODE_JOIN_DELTA_TRACE_OUTER,
    OPCODE_DELAY, OPCODE_REDUCE, OPCODE_DISTINCT,
    OPCODE_ANTI_JOIN_DELTA_TRACE, OPCODE_ANTI_JOIN_DELTA_DELTA,
    OPCODE_SEMI_JOIN_DELTA_TRACE, OPCODE_SEMI_JOIN_DELTA_DELTA,
    OPCODE_EXCHANGE_SHARD, OPCODE_EXCHANGE_GATHER,
    OPCODE_NULL_EXTEND,
    // Ports
    PORT_IN, PORT_TRACE, PORT_IN_A, PORT_IN_B,
    // Param slots
    PARAM_FUNC_ID, PARAM_AGG_FUNC_ID, PARAM_AGG_COL_IDX,
    PARAM_PROJ_BASE, PARAM_EXPR_BASE, PARAM_EXPR_NUM_REGS,
    PARAM_EXPR_RESULT_REG, PARAM_SHARD_COL_BASE, PARAM_GATHER_WORKER,
    PARAM_REINDEX_COL, PARAM_JOIN_SOURCE_TABLE, PARAM_CONST_STR_BASE,
    PARAM_TABLE_ID, PARAM_AGG_COUNT, PARAM_AGG_SPEC_BASE,
    PARAM_KEY_ONLY, PARAM_NULL_EXTEND_COUNT, PARAM_NULL_EXTEND_COL_BASE,
    // Aggregates
    AGG_COUNT, AGG_SUM, AGG_MIN, AGG_MAX, AGG_COUNT_NON_NULL,
};

// --- Schema constructor functions ---

pub fn schema_tab_schema() -> &'static Schema {
    static INSTANCE: OnceLock<Schema> = OnceLock::new();
    INSTANCE.get_or_init(|| Schema {
        columns: vec![
            ColumnDef { name: "schema_id".into(), type_code: TypeCode::U64,    is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
            ColumnDef { name: "name".into(),      type_code: TypeCode::String, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
        ],
        pk_index: 0,
    })
}

pub fn table_tab_schema() -> &'static Schema {
    static INSTANCE: OnceLock<Schema> = OnceLock::new();
    INSTANCE.get_or_init(|| Schema {
        columns: vec![
            ColumnDef { name: "table_id".into(),    type_code: TypeCode::U64,    is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
            ColumnDef { name: "schema_id".into(),   type_code: TypeCode::U64,    is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
            ColumnDef { name: "name".into(),        type_code: TypeCode::String, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
            ColumnDef { name: "directory".into(),   type_code: TypeCode::String, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
            ColumnDef { name: "pk_col_idx".into(),  type_code: TypeCode::U64,    is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
            ColumnDef { name: "created_lsn".into(), type_code: TypeCode::U64,    is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
            ColumnDef { name: "flags".into(),       type_code: TypeCode::U64,    is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
        ],
        pk_index: 0,
    })
}

pub fn col_tab_schema() -> &'static Schema {
    static INSTANCE: OnceLock<Schema> = OnceLock::new();
    INSTANCE.get_or_init(|| Schema {
        columns: vec![
            ColumnDef { name: "column_id".into(),   type_code: TypeCode::U64,    is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
            ColumnDef { name: "owner_id".into(),    type_code: TypeCode::U64,    is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
            ColumnDef { name: "owner_kind".into(),  type_code: TypeCode::U64,    is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
            ColumnDef { name: "col_idx".into(),     type_code: TypeCode::U64,    is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
            ColumnDef { name: "name".into(),        type_code: TypeCode::String, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
            ColumnDef { name: "type_code".into(),   type_code: TypeCode::U64,    is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
            ColumnDef { name: "is_nullable".into(), type_code: TypeCode::U64,    is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
            ColumnDef { name: "fk_table_id".into(), type_code: TypeCode::U64,    is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
            ColumnDef { name: "fk_col_idx".into(),  type_code: TypeCode::U64,    is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
        ],
        pk_index: 0,
    })
}

pub fn view_tab_schema() -> &'static Schema {
    static INSTANCE: OnceLock<Schema> = OnceLock::new();
    INSTANCE.get_or_init(|| Schema {
        columns: vec![
            ColumnDef { name: "view_id".into(),          type_code: TypeCode::U64,    is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
            ColumnDef { name: "schema_id".into(),        type_code: TypeCode::U64,    is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
            ColumnDef { name: "name".into(),             type_code: TypeCode::String, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
            ColumnDef { name: "sql_definition".into(),   type_code: TypeCode::String, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
            ColumnDef { name: "cache_directory".into(),  type_code: TypeCode::String, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
            ColumnDef { name: "created_lsn".into(),      type_code: TypeCode::U64,    is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
        ],
        pk_index: 0,
    })
}

pub fn dep_tab_schema() -> &'static Schema {
    static INSTANCE: OnceLock<Schema> = OnceLock::new();
    INSTANCE.get_or_init(|| Schema {
        columns: vec![
            ColumnDef { name: "dep_pk".into(),       type_code: TypeCode::U128, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
            ColumnDef { name: "view_id".into(),      type_code: TypeCode::U64,  is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
            ColumnDef { name: "dep_view_id".into(),  type_code: TypeCode::U64,  is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
            ColumnDef { name: "dep_table_id".into(), type_code: TypeCode::U64,  is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
        ],
        pk_index: 0,
    })
}

/// CircuitNodes: one row per node, every operator-specific scalar
/// columnar. PK encodes (view_id, node_id) as `(node_id, view_id)`.
pub fn circuit_nodes_schema() -> &'static Schema {
    static INSTANCE: OnceLock<Schema> = OnceLock::new();
    INSTANCE.get_or_init(|| Schema {
        columns: vec![
            ColumnDef { name: "node_pk".into(),      type_code: TypeCode::U128,   is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
            ColumnDef { name: "view_id".into(),      type_code: TypeCode::U64,    is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
            ColumnDef { name: "node_id".into(),      type_code: TypeCode::U64,    is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
            ColumnDef { name: "opcode".into(),       type_code: TypeCode::U64,    is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
            ColumnDef { name: "source_table".into(), type_code: TypeCode::U64,    is_nullable: true,  fk_table_id: 0, fk_col_idx: 0 },
            ColumnDef { name: "reindex_col".into(),  type_code: TypeCode::U64,    is_nullable: true,  fk_table_id: 0, fk_col_idx: 0 },
            ColumnDef { name: "expr_program".into(), type_code: TypeCode::Blob,   is_nullable: true,  fk_table_id: 0, fk_col_idx: 0 },
        ],
        pk_index: 0,
    })
}

/// CircuitEdges: natural-key composite of (view_id, dst_node, dst_port).
/// PK encoding: hi = view_id, lo = (dst_node << 8) | dst_port.
pub fn circuit_edges_schema() -> &'static Schema {
    static INSTANCE: OnceLock<Schema> = OnceLock::new();
    INSTANCE.get_or_init(|| Schema {
        columns: vec![
            ColumnDef { name: "edge_pk".into(),  type_code: TypeCode::U128, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
            ColumnDef { name: "view_id".into(),  type_code: TypeCode::U64,  is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
            ColumnDef { name: "dst_node".into(), type_code: TypeCode::U64,  is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
            ColumnDef { name: "dst_port".into(), type_code: TypeCode::U64,  is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
            ColumnDef { name: "src_node".into(), type_code: TypeCode::U64,  is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
        ],
        pk_index: 0,
    })
}

/// New CircuitNodeColumns schema (replaces the slot-encoded PARAM_*_BASE
/// regions in CircuitParams plus the standalone CircuitGroupCols table).
///
/// 7 columns: u128 PK + 6 denormalised payload (view_id, node_id, kind,
/// position, value1, value2). For single-value kinds (GROUP, SHARD, PROJ,
/// NULL_EXTEND_TYPECODE), `value1` holds the column index / type code and
/// `value2` is 0. For AGG_SPEC, `value1 = func_id` and `value2 = col_idx`.
pub fn circuit_node_columns_schema() -> &'static Schema {
    static INSTANCE: OnceLock<Schema> = OnceLock::new();
    INSTANCE.get_or_init(|| Schema {
        columns: vec![
            ColumnDef { name: "node_col_pk".into(), type_code: TypeCode::U128, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
            ColumnDef { name: "view_id".into(),     type_code: TypeCode::U64,  is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
            ColumnDef { name: "node_id".into(),     type_code: TypeCode::U64,  is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
            ColumnDef { name: "kind".into(),        type_code: TypeCode::U64,  is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
            ColumnDef { name: "position".into(),    type_code: TypeCode::U64,  is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
            ColumnDef { name: "value1".into(),      type_code: TypeCode::U64,  is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
            ColumnDef { name: "value2".into(),      type_code: TypeCode::U64,  is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
        ],
        pk_index: 0,
    })
}

