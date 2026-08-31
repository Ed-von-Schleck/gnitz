//! The catalog-level test helpers only this crate uses — the store-level ones
//! are `gnitz-store`'s sibling file, compiled here through a `#[path]`.
//!
//! Unlike [`super::shared`], this file is compiled once, inside `gnitz-engine`,
//! so it may name crate-internals and does not widen the testkit's surface.
//! Nothing here needs the first yet; the second is what keeps a helper only this
//! crate calls out of another crate's API.

use gnitz_engine::catalog::ColumnDef;
use gnitz_wire::type_code;

use super::shared::col_def;

/// A plain non-nullable UUID column.
pub fn uuid_def(name: &str) -> ColumnDef {
    col_def(name, type_code::UUID)
}

/// A nullable column of the given type.
pub fn nullable_def(name: &str, type_code: u8) -> ColumnDef {
    ColumnDef {
        is_nullable: true,
        ..col_def(name, type_code)
    }
}

/// A column of `type_code` carrying an FK onto `(parent_tid, parent_col)`.
pub fn fk_def(name: &str, type_code: u8, parent_tid: i64, parent_col: u32) -> ColumnDef {
    ColumnDef {
        fk_table_id: parent_tid,
        fk_col_idx: parent_col,
        ..col_def(name, type_code)
    }
}
