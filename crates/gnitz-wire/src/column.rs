//! The **logical column**: the one definition of a relation's column that the
//! client and the engine both hold, and that the schema record and the `COL_TAB`
//! row each describe.

use crate::schema_block::SchemaBlockCol;
use crate::{ColType, TypeCode};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ColumnDef {
    pub name: String,
    /// The column's logical type, a DECIMAL's scale included.
    pub ty: ColType,
    pub is_nullable: bool,
    /// A physical column no name the user writes can reach.
    pub is_hidden: bool,
}

impl ColumnDef {
    /// A visible column.
    pub fn new(name: impl Into<String>, type_code: TypeCode, is_nullable: bool) -> Self {
        Self::typed(name, ColType::of(type_code), is_nullable)
    }

    /// [`ColumnDef::new`] from a logical type, which is how a DECIMAL column,
    /// or a column declared from a computed expression, states its scale.
    pub fn typed(name: impl Into<String>, ty: ColType, is_nullable: bool) -> Self {
        Self {
            name: name.into(),
            ty,
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
