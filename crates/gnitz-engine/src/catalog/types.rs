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
    /// Hidden key slot (COL_TAB `is_hidden`). The engine never branches on it —
    /// it is echoed verbatim into reply schema blocks (`META_FLAG_HIDDEN`) so
    /// clients can suppress the column in presentation.
    pub(crate) is_hidden: bool,
}

// ---------------------------------------------------------------------------
// FK constraint
// ---------------------------------------------------------------------------

pub(crate) struct FkConstraint {
    pub(crate) fk_col_idx: usize,     // child column position
    pub(crate) target_table_id: i64,  // parent table id
    pub(crate) target_col_idx: usize, // parent referenced column position
}

/// A child-side FK reference seen from the parent's perspective. The two
/// `usize` positions are easy to transpose, so they are named rather than left
/// as a bare tuple.
#[derive(Clone, Copy)]
pub(crate) struct FkParentRef {
    pub(crate) child_tid: i64,        // referencing child table id
    pub(crate) fk_col_idx: usize,     // child column position
    pub(crate) parent_col_idx: usize, // referenced parent column position
}
