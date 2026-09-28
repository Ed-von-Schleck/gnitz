use gnitz_wire::ColumnDef;

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// A catalog column: the logical column plus its FK, already resolved.
#[derive(Clone, Debug)]
pub(crate) struct CatalogColumn {
    pub(crate) def: ColumnDef,
    /// The referenced parent table; `0` means "no FK".
    pub(crate) fk_table_id: u64,
    pub(crate) fk_col_idx: u32,
}

/// A column with no FK.
impl From<ColumnDef> for CatalogColumn {
    fn from(def: ColumnDef) -> Self {
        CatalogColumn { def, fk_table_id: 0, fk_col_idx: 0 }
    }
}

// ---------------------------------------------------------------------------
// FK constraint
// ---------------------------------------------------------------------------

/// One FK constraint as a directed edge, identical whichever end it was reached
/// from: the child's relation entry holds the edges it declares, `fk_by_parent`
/// those whose `parent_tid` is the key. The two column positions are
/// easy to transpose, so they are named rather than left as a bare tuple.
#[derive(Clone, Copy)]
pub(crate) struct FkEdge {
    /// Referencing child table id.
    pub(crate) child_tid: u64,
    /// Child column position.
    pub(crate) fk_col: usize,
    /// Referenced parent table id.
    pub(crate) parent_tid: u64,
    /// Referenced parent column position.
    pub(crate) parent_col: usize,
}
