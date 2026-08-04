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

/// One FK constraint as a directed edge, identical whichever end it was reached
/// from: `fk_by_child` holds the edges whose `child_tid` is the key,
/// `fk_by_parent` those whose `parent_tid` is. The two column positions are
/// easy to transpose, so they are named rather than left as a bare tuple.
#[derive(Clone, Copy)]
pub(crate) struct FkEdge {
    /// Referencing child table id.
    pub(crate) child_tid: i64,
    /// Child column position.
    pub(crate) fk_col: usize,
    /// Referenced parent table id.
    pub(crate) parent_tid: i64,
    /// Referenced parent column position.
    pub(crate) parent_col: usize,
}
