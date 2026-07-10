use crate::protocol::{ColumnDef, Schema};
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
//
// Every system-table schema is derived from the shared `gnitz-wire` column
// slice, so the client and engine cannot drift on a table's shape.

pub fn schema_tab_schema() -> &'static Schema {
    static INSTANCE: OnceLock<Schema> = OnceLock::new();
    INSTANCE.get_or_init(|| schema_from_wire_cols(gnitz_wire::SCHEMA_TAB_COLS, &[0]))
}

pub fn table_tab_schema() -> &'static Schema {
    static INSTANCE: OnceLock<Schema> = OnceLock::new();
    INSTANCE.get_or_init(|| schema_from_wire_cols(gnitz_wire::TABLE_TAB_COLS, &[0]))
}

pub fn col_tab_schema() -> &'static Schema {
    static INSTANCE: OnceLock<Schema> = OnceLock::new();
    INSTANCE.get_or_init(|| schema_from_wire_cols(gnitz_wire::COL_TAB_COLS, &[0]))
}

pub fn view_tab_schema() -> &'static Schema {
    static INSTANCE: OnceLock<Schema> = OnceLock::new();
    INSTANCE.get_or_init(|| schema_from_wire_cols(gnitz_wire::VIEW_TAB_COLS, &[0]))
}

pub fn dep_tab_schema() -> &'static Schema {
    static INSTANCE: OnceLock<Schema> = OnceLock::new();
    INSTANCE.get_or_init(|| schema_from_wire_cols(gnitz_wire::DEP_TAB_COLS, &[0, 1]))
}

pub fn idx_tab_schema() -> &'static Schema {
    static INSTANCE: OnceLock<Schema> = OnceLock::new();
    INSTANCE.get_or_init(|| schema_from_wire_cols(gnitz_wire::IDX_TAB_COLS, &[0]))
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
