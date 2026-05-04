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

// `CircuitGraph` previously lived here as the DTO for the now-deleted
// `engine.create_view(qname, &CircuitGraph, sql)` direct path. After the
// circuit-graph schema redesign, views always reach the engine via the
// wire path (`client.rs::create_view_with_circuit`) using the typed
// `gnitz_core::Circuit` instead.

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
