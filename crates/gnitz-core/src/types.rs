use std::sync::OnceLock;
use crate::protocol::{Schema, ColumnDef, TypeCode};

pub use gnitz_wire::{
    // System table IDs
    CIRCUIT_NODES_TAB, CIRCUIT_EDGES_TAB, CIRCUIT_NODE_COLUMNS_TAB,
    // Owner kinds
    OWNER_KIND_TABLE, OWNER_KIND_VIEW,
    // Opcodes
    OPCODE_SCAN_TRACE, OPCODE_SCAN_DELTA, OPCODE_INTEGRATE,
    OPCODE_FILTER, OPCODE_MAP, OPCODE_NEGATE, OPCODE_UNION,
    OPCODE_JOIN_DELTA_TRACE, OPCODE_JOIN_DELTA_DELTA, OPCODE_JOIN_DELTA_TRACE_OUTER,
    OPCODE_JOIN_DELTA_TRACE_RANGE, OPCODE_PARTITION_FILTER,
    OPCODE_DELAY, OPCODE_REDUCE, OPCODE_DISTINCT,
    OPCODE_ANTI_JOIN_DELTA_TRACE, OPCODE_ANTI_JOIN_DELTA_DELTA,
    OPCODE_SEMI_JOIN_DELTA_TRACE, OPCODE_SEMI_JOIN_DELTA_DELTA,
    OPCODE_EXCHANGE_SHARD, OPCODE_EXCHANGE_GATHER,
    OPCODE_NULL_EXTEND, OPCODE_INTEGRATE_TRACE,
    OPCODE_MAP_PROJ, OPCODE_MAP_EXPR,
    // Ports
    PORT_IN, PORT_TRACE, PORT_IN_A, PORT_IN_B,
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
        pk_cols: vec![0],
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
        pk_cols: vec![0],
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
        pk_cols: vec![0],
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
            // Packed view-PK column list (gnitz_wire::pack_pk_cols). A bare `0`
            // (flag bit clear) decodes as the single-column PK `[0]`, so existing
            // single-PK views and a freshly-bootstrapped row read back unchanged.
            ColumnDef { name: "pk_col_idx".into(),       type_code: TypeCode::U64,    is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
        ],
        pk_cols: vec![0],
    })
}

pub fn dep_tab_schema() -> &'static Schema {
    static INSTANCE: OnceLock<Schema> = OnceLock::new();
    INSTANCE.get_or_init(|| Schema {
        columns: vec![
            ColumnDef { name: "view_id".into(),      type_code: TypeCode::U64,  is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
            ColumnDef { name: "dep_table_id".into(), type_code: TypeCode::U64,  is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
            ColumnDef { name: "dep_view_id".into(),  type_code: TypeCode::U64,  is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
        ],
        pk_cols: vec![0, 1],
    })
}

pub fn idx_tab_schema() -> &'static Schema {
    static INSTANCE: OnceLock<Schema> = OnceLock::new();
    INSTANCE.get_or_init(|| Schema {
        columns: vec![
            ColumnDef { name: "index_id".into(),        type_code: TypeCode::U64,    is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
            ColumnDef { name: "owner_id".into(),        type_code: TypeCode::U64,    is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
            ColumnDef { name: "owner_kind".into(),      type_code: TypeCode::U64,    is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
            ColumnDef { name: "source_col_idx".into(),  type_code: TypeCode::U64,    is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
            ColumnDef { name: "name".into(),            type_code: TypeCode::String, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
            ColumnDef { name: "is_unique".into(),       type_code: TypeCode::U64,    is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
            ColumnDef { name: "cache_directory".into(), type_code: TypeCode::String, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
        ],
        pk_cols: vec![0],
    })
}

pub(crate) fn schema_from_wire_cols(cols: &[gnitz_wire::WireSysCol], pk_cols: &[usize]) -> Schema {
    Schema {
        columns: cols.iter().map(|c| ColumnDef {
            name:        c.name.into(),
            type_code:   c.type_code,
            is_nullable: c.nullable,
            fk_table_id: 0,
            fk_col_idx:  0,
        }).collect(),
        pk_cols: pk_cols.to_vec(),
    }
}

pub fn circuit_nodes_schema() -> &'static Schema {
    static INSTANCE: OnceLock<Schema> = OnceLock::new();
    INSTANCE.get_or_init(|| schema_from_wire_cols(gnitz_wire::CIRCUIT_NODES_COLS, &[0, 1]))
}

pub fn circuit_edges_schema() -> &'static Schema {
    static INSTANCE: OnceLock<Schema> = OnceLock::new();
    INSTANCE.get_or_init(|| schema_from_wire_cols(gnitz_wire::CIRCUIT_EDGES_COLS, &[0, 1]))
}

pub fn circuit_node_columns_schema() -> &'static Schema {
    static INSTANCE: OnceLock<Schema> = OnceLock::new();
    INSTANCE.get_or_init(|| schema_from_wire_cols(gnitz_wire::CIRCUIT_NODE_COLUMNS_COLS, &[0, 1]))
}

