//! The **logical column**: the one definition of a relation's column that the
//! client and the engine both hold, and that the schema record and the `COL_TAB`
//! row each describe.

use std::sync::Arc;

use crate::ColType;
use crate::schema_record::RecordCol;

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

    /// This column's entry in the named schema record a RESOLVE carries.
    pub fn record_col(&self) -> RecordCol<'_> {
        RecordCol {
            ty: self.ty,
            nullable: self.is_nullable,
            hidden: self.is_hidden,
            name: &self.name,
        }
    }

    /// This column's entry in the nameless shape record a push and a SAL group
    /// carry: its type, scale included, and nullability.
    pub fn shape_col(&self) -> RecordCol<'_> {
        RecordCol {
            ty: self.ty,
            nullable: self.is_nullable,
            hidden: false,
            name: "",
        }
    }
}
