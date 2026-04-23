use std::sync::OnceLock;
use crate::protocol::{Schema, ColumnDef, TypeCode};

pub use gnitz_wire::{
    // System table IDs
    CIRCUIT_NODES_TAB, CIRCUIT_EDGES_TAB, CIRCUIT_SOURCES_TAB,
    CIRCUIT_PARAMS_TAB, CIRCUIT_GROUP_COLS_TAB,
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

pub fn circuit_nodes_schema() -> &'static Schema {
    static INSTANCE: OnceLock<Schema> = OnceLock::new();
    INSTANCE.get_or_init(|| Schema {
        columns: vec![
            ColumnDef { name: "node_pk".into(), type_code: TypeCode::U128, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
            ColumnDef { name: "opcode".into(),  type_code: TypeCode::U64,  is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
        ],
        pk_index: 0,
    })
}

pub fn circuit_edges_schema() -> &'static Schema {
    static INSTANCE: OnceLock<Schema> = OnceLock::new();
    INSTANCE.get_or_init(|| Schema {
        columns: vec![
            ColumnDef { name: "edge_pk".into(),  type_code: TypeCode::U128, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
            ColumnDef { name: "src_node".into(), type_code: TypeCode::U64,  is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
            ColumnDef { name: "dst_node".into(), type_code: TypeCode::U64,  is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
            ColumnDef { name: "dst_port".into(), type_code: TypeCode::U64,  is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
        ],
        pk_index: 0,
    })
}

pub fn circuit_sources_schema() -> &'static Schema {
    static INSTANCE: OnceLock<Schema> = OnceLock::new();
    INSTANCE.get_or_init(|| Schema {
        columns: vec![
            ColumnDef { name: "source_pk".into(), type_code: TypeCode::U128, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
            ColumnDef { name: "table_id".into(),  type_code: TypeCode::U64,  is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
        ],
        pk_index: 0,
    })
}

pub fn circuit_params_schema() -> &'static Schema {
    static INSTANCE: OnceLock<Schema> = OnceLock::new();
    INSTANCE.get_or_init(|| Schema {
        columns: vec![
            ColumnDef { name: "param_pk".into(),  type_code: TypeCode::U128,   is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
            ColumnDef { name: "value".into(),     type_code: TypeCode::U64,    is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
            ColumnDef { name: "str_value".into(), type_code: TypeCode::String, is_nullable: true, fk_table_id: 0, fk_col_idx: 0 },
        ],
        pk_index: 0,
    })
}

pub fn circuit_group_cols_schema() -> &'static Schema {
    static INSTANCE: OnceLock<Schema> = OnceLock::new();
    INSTANCE.get_or_init(|| Schema {
        columns: vec![
            ColumnDef { name: "gcol_pk".into(), type_code: TypeCode::U128, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
            ColumnDef { name: "col_idx".into(), type_code: TypeCode::U64,  is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
        ],
        pk_index: 0,
    })
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

impl CircuitGraph {
    /// Verify that all param and const_string slots fit in 8 bits.
    ///
    /// The wire encoding packs `(node_id, slot)` as `node_id << 8 | slot` into
    /// the low half of a U128 PK. Slots ≥ 256 silently corrupt the PK and cause
    /// the server to load wrong bytecode params, producing incorrect query results.
    /// Call this before sending any RPCs.
    pub fn validate(&self) -> Result<(), String> {
        for &(node_id, slot, _) in &self.params {
            if slot > 255 {
                return Err(format!(
                    "node {} param slot {} exceeds 8-bit wire limit (max 255); \
                     expression program or aggregate spec list is too large",
                    node_id, slot
                ));
            }
        }
        for &(node_id, slot, _) in &self.const_strings {
            if slot > 255 {
                return Err(format!(
                    "node {} const_string slot {} exceeds 8-bit wire limit (max 255); \
                     expression has too many string constants",
                    node_id, slot
                ));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn empty_graph() -> CircuitGraph {
        CircuitGraph {
            view_id: 1, primary_source_id: 2,
            nodes: vec![], edges: vec![], sources: vec![],
            params: vec![], group_cols: vec![], const_strings: vec![],
            dependencies: vec![],
        }
    }

    #[test]
    fn validate_accepts_max_slot() {
        let mut g = empty_graph();
        g.params.push((1, 255, 42));
        g.const_strings.push((1, 255, "x".into()));
        assert!(g.validate().is_ok());
    }

    #[test]
    fn validate_rejects_param_slot_overflow() {
        let mut g = empty_graph();
        g.params.push((3, 256, 0));
        let err = g.validate().unwrap_err();
        assert!(err.contains("256"), "expected slot in message, got: {}", err);
        assert!(err.contains("node 3"), "expected node id in message, got: {}", err);
    }

    #[test]
    fn validate_rejects_const_string_slot_overflow() {
        let mut g = empty_graph();
        g.const_strings.push((2, 256, "hello".into()));
        assert!(g.validate().is_err());
    }

    #[test]
    fn validate_expr_base_boundary() {
        // PARAM_EXPR_BASE = 64; slot 64+191=255 is the last safe bytecode word.
        // Slot 64+192=256 is the first to overflow.
        use gnitz_wire::{PARAM_EXPR_BASE, PARAM_CONST_STR_BASE, PARAM_NULL_EXTEND_COL_BASE};
        let mut g = empty_graph();
        g.params.push((1, PARAM_EXPR_BASE + 191, 0)); // last safe expr word
        assert!(g.validate().is_ok());
        g.params.push((1, PARAM_EXPR_BASE + 192, 0)); // first overflowing expr word
        assert!(g.validate().is_err());

        let mut g2 = empty_graph();
        g2.const_strings.push((1, PARAM_CONST_STR_BASE + 95, "a".into())); // safe
        assert!(g2.validate().is_ok());
        g2.const_strings.push((1, PARAM_CONST_STR_BASE + 96, "b".into())); // overflow
        assert!(g2.validate().is_err());

        let mut g3 = empty_graph();
        g3.params.push((1, PARAM_NULL_EXTEND_COL_BASE + 63, 0)); // safe
        assert!(g3.validate().is_ok());
        g3.params.push((1, PARAM_NULL_EXTEND_COL_BASE + 64, 0)); // overflow
        assert!(g3.validate().is_err());
    }
}
