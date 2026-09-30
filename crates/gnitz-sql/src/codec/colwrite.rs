//! Column value encoding: SQL literal → one region cell, in the canonical region order.
//!
//! One encoder, [`append_value_to_col`], writes both an INSERT cell and a SET
//! literal, so `300` means the same thing whichever verb writes it; and
//! [`check_not_null`] is the NOT NULL verdict both DML write paths take.

use crate::codec::literal::assign;
use crate::error::GnitzSqlError;
use crate::ir::BExpr;
use gnitz_core::push_zero_cell;
use gnitz_wire::{ColumnDef, TypeCode};

/// Append one written literal to column region `col`, spilling a string into
/// `blob`. Dispatch is on the *column type*, never on the literal: `5` into a
/// UUID column is the decimal spelling.
pub(crate) fn append_value_to_col<R>(
    col: &mut Vec<u8>,
    blob: &mut Vec<u8>,
    def: &ColumnDef,
    lit: &BExpr<R>,
) -> Result<(), GnitzSqlError> {
    let tc = def.ty.tc;
    match (lit, tc) {
        // NULL is the one value every column type encodes alike: a zeroed cell
        // of the type's own stride, with the null bit set by the caller.
        (BExpr::LitNull, _) => push_zero_cell(col, tc),
        (BExpr::LitStr(s), TypeCode::String) => {
            col.extend_from_slice(&gnitz_wire::encode_german_string(s.as_bytes(), blob))
        }
        _ => col.extend_from_slice(&native_value(lit, def)?.to_le_bytes()[..tc.wire_stride()]),
    }
    Ok(())
}

/// A literal as the native image of a column's cell, refused naming the column.
pub(crate) fn native_value<R>(lit: &BExpr<R>, def: &ColumnDef) -> Result<u128, GnitzSqlError> {
    assign(lit, def.ty).map_err(|m| GnitzSqlError::Rejected(format!("column '{}': {m}", def.name)))
}

/// Reject a NULL destined for a NOT NULL column, naming it.
///
/// Both DML write paths check here rather than leaning on `ZSetBatch::validate`
/// at the wire, because neither reaches the wire in time: `ON CONFLICT DO
/// UPDATE` reads its incoming VALUES batch to build the merged row and never
/// pushes it, and a transaction's buffered row is read back by later statements
/// long before COMMIT validates it. Either reader is a program resolved with
/// `no_nulls = true`, which would take the filler zero for a real value.
pub(crate) fn check_not_null(col_def: &ColumnDef, is_null: bool) -> Result<(), GnitzSqlError> {
    if is_null && !col_def.is_nullable {
        return Err(GnitzSqlError::Rejected(format!(
            "NULL value in column '{}' violates NOT NULL",
            col_def.name
        )));
    }
    Ok(())
}

#[cfg(test)]
#[path = "tests/colwrite.rs"]
mod tests;
