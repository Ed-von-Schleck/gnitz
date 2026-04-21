use super::*;

// ---------------------------------------------------------------------------
// Public types
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

// ---------------------------------------------------------------------------
// FK constraint
// ---------------------------------------------------------------------------

pub(crate) struct FkConstraint {
    pub(crate) fk_col_idx: usize,
    pub(crate) target_table_id: i64,
}

// ---------------------------------------------------------------------------
// Index info
// ---------------------------------------------------------------------------

pub(crate) struct IndexInfo {
    pub(crate) index_id: i64,
    pub(crate) owner_id: i64,
    pub(crate) source_col_idx: u32,
    pub(crate) source_col_type: u8,
    pub(crate) index_key_type: u8,
    pub(crate) source_pk_type: u8,
    pub(crate) name: String,
    pub(crate) is_unique: bool,
    pub(crate) cache_dir: String,
    /// Owned Table for the index storage. Box prevents moves.
    pub(crate) table: Box<Table>,
}
