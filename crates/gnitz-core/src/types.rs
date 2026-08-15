use crate::protocol::{ColumnDef, Schema};
use std::sync::OnceLock;

// --- Schema constructor functions ---
//
// Every system-table schema is derived from the shared `gnitz-wire` column
// slice, so the client and engine cannot drift on a table's shape.

pub fn schema_tab_schema() -> &'static Schema {
    static INSTANCE: OnceLock<Schema> = OnceLock::new();
    INSTANCE.get_or_init(|| schema_from_wire_cols(gnitz_wire::SCHEMA_TAB_COLS, gnitz_wire::SCHEMA_TAB_PK))
}

pub fn table_tab_schema() -> &'static Schema {
    static INSTANCE: OnceLock<Schema> = OnceLock::new();
    INSTANCE.get_or_init(|| schema_from_wire_cols(gnitz_wire::TABLE_TAB_COLS, gnitz_wire::TABLE_TAB_PK))
}

pub fn col_tab_schema() -> &'static Schema {
    static INSTANCE: OnceLock<Schema> = OnceLock::new();
    INSTANCE.get_or_init(|| schema_from_wire_cols(gnitz_wire::COL_TAB_COLS, gnitz_wire::COL_TAB_PK))
}

pub fn view_tab_schema() -> &'static Schema {
    static INSTANCE: OnceLock<Schema> = OnceLock::new();
    INSTANCE.get_or_init(|| schema_from_wire_cols(gnitz_wire::VIEW_TAB_COLS, gnitz_wire::VIEW_TAB_PK))
}

pub fn idx_tab_schema() -> &'static Schema {
    static INSTANCE: OnceLock<Schema> = OnceLock::new();
    INSTANCE.get_or_init(|| schema_from_wire_cols(gnitz_wire::IDX_TAB_COLS, gnitz_wire::IDX_TAB_PK))
}

pub(crate) fn schema_from_wire_cols(cols: &[gnitz_wire::WireSysCol], pk_cols: &[u32]) -> Schema {
    Schema {
        columns: cols
            .iter()
            .map(|c| ColumnDef::new(c.name, c.type_code, c.nullable))
            .collect(),
        pk_cols: pk_cols.iter().map(|&c| c as usize).collect(),
    }
}

pub fn circuit_nodes_schema() -> &'static Schema {
    static INSTANCE: OnceLock<Schema> = OnceLock::new();
    INSTANCE.get_or_init(|| schema_from_wire_cols(gnitz_wire::CIRCUIT_NODES_COLS, gnitz_wire::CIRCUIT_FAMILY_PK))
}

pub fn circuit_edges_schema() -> &'static Schema {
    static INSTANCE: OnceLock<Schema> = OnceLock::new();
    INSTANCE.get_or_init(|| schema_from_wire_cols(gnitz_wire::CIRCUIT_EDGES_COLS, gnitz_wire::CIRCUIT_FAMILY_PK))
}

pub fn circuit_node_columns_schema() -> &'static Schema {
    static INSTANCE: OnceLock<Schema> = OnceLock::new();
    INSTANCE.get_or_init(|| schema_from_wire_cols(gnitz_wire::CIRCUIT_NODE_COLUMNS_COLS, gnitz_wire::CIRCUIT_FAMILY_PK))
}
