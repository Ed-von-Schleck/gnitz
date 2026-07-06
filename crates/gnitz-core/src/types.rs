use crate::protocol::{ColumnDef, Schema, TypeCode};
use std::sync::OnceLock;

pub use gnitz_wire::{
    // Aggregates
    AGG_COUNT,
    AGG_COUNT_NON_NULL,
    AGG_MAX,
    AGG_MIN,
    AGG_SUM,
    AGG_SUM_ZERO,
    CIRCUIT_EDGES_TAB,
    // System table IDs
    CIRCUIT_NODES_TAB,
    CIRCUIT_NODE_COLUMNS_TAB,
    // Opcodes
    OPCODE_DISTINCT,
    OPCODE_EXCHANGE_GATHER,
    OPCODE_EXCHANGE_SHARD,
    OPCODE_FILTER,
    OPCODE_INTEGRATE,
    OPCODE_INTEGRATE_TRACE,
    OPCODE_JOIN_DELTA_TRACE,
    OPCODE_JOIN_DELTA_TRACE_RANGE,
    OPCODE_MAP,
    OPCODE_MAP_EXPR,
    OPCODE_MAP_PROJ,
    OPCODE_NEGATE,
    OPCODE_NULL_EXTEND,
    OPCODE_PARTITION_FILTER,
    OPCODE_POSITIVE_PART,
    OPCODE_REDUCE,
    OPCODE_SCAN_DELTA,
    OPCODE_UNION,
    // Owner kinds
    OWNER_KIND_TABLE,
    OWNER_KIND_VIEW,
    // Ports
    PORT_IN,
    PORT_IN_A,
    PORT_IN_B,
    PORT_TRACE,
};

// --- Schema constructor functions ---

pub fn schema_tab_schema() -> &'static Schema {
    static INSTANCE: OnceLock<Schema> = OnceLock::new();
    INSTANCE.get_or_init(|| Schema {
        columns: vec![
            ColumnDef::new("schema_id", TypeCode::U64, false),
            ColumnDef::new("name", TypeCode::String, false),
        ],
        pk_cols: vec![0],
    })
}

pub fn table_tab_schema() -> &'static Schema {
    static INSTANCE: OnceLock<Schema> = OnceLock::new();
    INSTANCE.get_or_init(|| Schema {
        columns: vec![
            ColumnDef::new("table_id", TypeCode::U64, false),
            ColumnDef::new("schema_id", TypeCode::U64, false),
            ColumnDef::new("name", TypeCode::String, false),
            ColumnDef::new("directory", TypeCode::String, false),
            ColumnDef::new("pk_col_idx", TypeCode::U64, false),
            ColumnDef::new("created_lsn", TypeCode::U64, false),
            ColumnDef::new("flags", TypeCode::U64, false),
        ],
        pk_cols: vec![0],
    })
}

pub fn col_tab_schema() -> &'static Schema {
    static INSTANCE: OnceLock<Schema> = OnceLock::new();
    INSTANCE.get_or_init(|| Schema {
        columns: vec![
            ColumnDef::new("column_id", TypeCode::U64, false),
            ColumnDef::new("owner_id", TypeCode::U64, false),
            ColumnDef::new("owner_kind", TypeCode::U64, false),
            ColumnDef::new("col_idx", TypeCode::U64, false),
            ColumnDef::new("name", TypeCode::String, false),
            ColumnDef::new("type_code", TypeCode::U64, false),
            ColumnDef::new("is_nullable", TypeCode::U64, false),
            ColumnDef::new("fk_table_id", TypeCode::U64, false),
            ColumnDef::new("fk_col_idx", TypeCode::U64, false),
            // is_serial marker (index 9): 1 for a SERIAL PK column, else 0. Lets
            // a connection that only fetched the schema distinguish an
            // auto-assigned SERIAL PK from a user-supplied non-null integer PK.
            ColumnDef::new("is_serial", TypeCode::U64, false),
        ],
        pk_cols: vec![0],
    })
}

pub fn view_tab_schema() -> &'static Schema {
    static INSTANCE: OnceLock<Schema> = OnceLock::new();
    INSTANCE.get_or_init(|| Schema {
        columns: vec![
            ColumnDef::new("view_id", TypeCode::U64, false),
            ColumnDef::new("schema_id", TypeCode::U64, false),
            ColumnDef::new("name", TypeCode::String, false),
            ColumnDef::new("sql_definition", TypeCode::String, false),
            ColumnDef::new("cache_directory", TypeCode::String, false),
            ColumnDef::new("created_lsn", TypeCode::U64, false),
            // Packed view-PK column list (gnitz_wire::pack_pk_cols). A bare `0`
            // (flag bit clear) decodes as the single-column PK `[0]`, so existing
            // single-PK views and a freshly-bootstrapped row read back unchanged.
            ColumnDef::new("pk_col_idx", TypeCode::U64, false),
        ],
        pk_cols: vec![0],
    })
}

pub fn dep_tab_schema() -> &'static Schema {
    static INSTANCE: OnceLock<Schema> = OnceLock::new();
    INSTANCE.get_or_init(|| Schema {
        columns: vec![
            ColumnDef::new("view_id", TypeCode::U64, false),
            ColumnDef::new("dep_table_id", TypeCode::U64, false),
            ColumnDef::new("dep_view_id", TypeCode::U64, false),
        ],
        pk_cols: vec![0, 1],
    })
}

pub fn idx_tab_schema() -> &'static Schema {
    static INSTANCE: OnceLock<Schema> = OnceLock::new();
    INSTANCE.get_or_init(|| Schema {
        columns: vec![
            ColumnDef::new("index_id", TypeCode::U64, false),
            ColumnDef::new("owner_id", TypeCode::U64, false),
            ColumnDef::new("owner_kind", TypeCode::U64, false),
            ColumnDef::new("source_col_idx", TypeCode::U64, false),
            ColumnDef::new("name", TypeCode::String, false),
            ColumnDef::new("is_unique", TypeCode::U64, false),
            ColumnDef::new("cache_directory", TypeCode::String, false),
        ],
        pk_cols: vec![0],
    })
}

pub(crate) fn schema_from_wire_cols(cols: &[gnitz_wire::WireSysCol], pk_cols: &[usize]) -> Schema {
    Schema {
        columns: cols
            .iter()
            .map(|c| ColumnDef::new(c.name, c.type_code, c.nullable))
            .collect(),
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
