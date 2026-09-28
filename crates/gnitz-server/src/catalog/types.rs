use gnitz_wire::ColType;

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// Column definition for create_table.
#[derive(Clone, Debug)]
pub(crate) struct ColumnDef {
    pub(crate) name: String,
    /// The column's type. A DECIMAL's scale (COL_TAB `scale`) is echoed verbatim
    /// into reply schema blocks; the engine never branches on it.
    pub(crate) ty: ColType,
    pub(crate) is_nullable: bool,
    pub(crate) fk_table_id: u64,
    pub(crate) fk_col_idx: u32,
    /// Hidden key slot (COL_TAB `is_hidden`). The engine never branches on it —
    /// it is echoed verbatim into reply schema blocks (`ColMeta::hidden`) so
    /// clients can suppress the column in presentation.
    pub(crate) is_hidden: bool,
}

impl ColumnDef {
    /// The plain column of type `ty` — no nullability, no FK, no marker flags —
    /// so a construction site names only what it actually varies.
    pub(crate) fn new(name: impl Into<String>, ty: ColType) -> Self {
        ColumnDef {
            name: name.into(),
            ty,
            is_nullable: false,
            fk_table_id: 0,
            fk_col_idx: 0,
            is_hidden: false,
        }
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
