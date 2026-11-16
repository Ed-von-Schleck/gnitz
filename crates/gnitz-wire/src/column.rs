//! The **logical column**: the one definition of a relation's column that the
//! client and the engine both hold, and that the schema record and the `COL_TAB`
//! row each describe.

use std::sync::Arc;

use crate::ColType;
use crate::schema_block::SchemaBlockCol;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ColumnDef {
    /// Shared, so copying a schema bumps a refcount per column instead of
    /// allocating a name.
    pub name: Arc<str>,
    /// The column's logical type, a DECIMAL's scale included.
    pub ty: ColType,
    pub is_nullable: bool,
    /// A physical column no name the user writes can reach.
    pub is_hidden: bool,
}

impl ColumnDef {
    /// A visible column of logical type `ty`; a bare [`TypeCode`](crate::TypeCode) converts to
    /// its scale-0 type, and a DECIMAL column states its scale through
    /// [`ColType::decimal`].
    pub fn new(name: impl Into<Arc<str>>, ty: impl Into<ColType>, is_nullable: bool) -> Self {
        Self {
            name: name.into(),
            ty: ty.into(),
            is_nullable,
            is_hidden: false,
        }
    }

    /// Mark this column hidden.
    pub fn hidden(mut self) -> Self {
        self.is_hidden = true;
        self
    }

    /// This column's entry in a schema record.
    pub fn block_col(&self) -> SchemaBlockCol<'_> {
        SchemaBlockCol {
            ty: self.ty,
            nullable: self.is_nullable,
            hidden: self.is_hidden,
            name: self.name.as_bytes(),
        }
    }
}
