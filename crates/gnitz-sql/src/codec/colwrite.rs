//! Column value encoding: SQL literal → one §6 region cell.
//!
//! One encoder, [`append_value_to_col`], writes both an INSERT cell and a SET
//! literal, so `300` means the same thing whichever verb writes it; and
//! [`check_not_null`] is the NOT NULL verdict both DML write paths take.

use crate::codec::literal::{place, Placed};
use crate::error::GnitzSqlError;
use crate::ir::BExpr;
use gnitz_core::{push_zero_cell, ColumnDef, TypeCode};

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
    let refuse = |m: &str| Err(GnitzSqlError::Bind(format!("column '{}': {m}", def.name)));
    // NULL is the one value every column type encodes alike: a zeroed cell of
    // the type's own stride, with the null bit set by the caller.
    if matches!(lit, BExpr::LitNull) {
        push_zero_cell(col, tc);
        return Ok(());
    }
    match tc {
        TypeCode::F32 | TypeCode::F64 => {
            let Some(v) = float_value(lit) else {
                return refuse("string literal for non-string column");
            };
            // F32 rounds through binary64, as `ZSetBatch::append` does for a
            // Python float, so the two ingest paths agree bit for bit.
            match tc {
                TypeCode::F32 => col.extend_from_slice(&(v as f32).to_le_bytes()),
                _ => col.extend_from_slice(&v.to_le_bytes()),
            }
            Ok(())
        }
        TypeCode::String => match lit {
            BExpr::LitStr(s) => {
                col.extend_from_slice(&gnitz_wire::encode_german_string(s.as_bytes(), blob));
                Ok(())
            }
            _ => refuse("number literal for string column"),
        },
        // A BLOB column is *not* text-writable: it takes bytes, and a written
        // literal spells none.
        TypeCode::Blob => match lit {
            BExpr::LitStr(_) => refuse("string literal for non-string column"),
            _ => refuse("number literal for blob column"),
        },
        _ => {
            let v = native_value(lit, def)?;
            col.extend_from_slice(&v.to_le_bytes()[..tc.wire_stride()]);
            Ok(())
        }
    }
}

/// A literal as the native image of a column stored as an integer.
pub(crate) fn native_value<R>(lit: &BExpr<R>, def: &ColumnDef) -> Result<u128, GnitzSqlError> {
    let ty = def.ty;
    let text = || lit.literal_text();
    match place(lit, ty) {
        Some(
            Placed::At(v)
            | Placed::Between { nearest: v, .. }
            | Placed::Below { nearest: Some(v) }
            | Placed::Above { nearest: Some(v) },
        ) => Ok(v),
        Some(Placed::Below { .. } | Placed::Above { .. }) => Err(format!("{ty} value out of range: {}", text())),
        None if matches!(lit, BExpr::LitStr(_)) => Err(format!("invalid {ty} literal: {}", text())),
        None => Err(format!("{} is not a {ty} value", text())),
    }
    .map_err(|m| GnitzSqlError::Bind(format!("column '{}': {m}", def.name)))
}

/// A float column's value; `None` for a string. A magnitude past `i128` is
/// included, which a DOUBLE holds and no integer parse would.
fn float_value<R>(lit: &BExpr<R>) -> Option<f64> {
    if let Some(v) = lit.int_literal() {
        return Some(v as f64);
    }
    match lit {
        BExpr::LitFloat { v, .. } => Some(*v),
        BExpr::LitWide(n) if n.neg => Some(-(n.mag as f64)),
        BExpr::LitWide(n) => Some(n.mag as f64),
        _ => None,
    }
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
        return Err(GnitzSqlError::Bind(format!(
            "NULL value in column '{}' violates NOT NULL",
            col_def.name
        )));
    }
    Ok(())
}

#[cfg(test)]
#[path = "tests/colwrite.rs"]
mod tests;
