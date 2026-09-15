//! Column value encoding: SQL literal → one §6 region cell.
//!
//! One encoder, [`append_value_to_col`], writes both an INSERT cell and a SET
//! literal, so `300` means the same thing whichever verb writes it; and
//! [`check_not_null`] is the NOT NULL verdict both DML write paths take.

use crate::ast_util::Constant;
use crate::codec::pk_codec::KeyLitError;
use crate::error::GnitzSqlError;
use crate::ir::BExpr;
use gnitz_core::{push_zero_cell, ColType, ColumnDef, TypeCode};

/// Append one written constant to column region `col`, spilling a string into
/// `blob`. Dispatch is on the *column type*, never on the literal: `'5'` into an
/// F64 column is `5.0` and `5` into a UUID column is the decimal spelling.
pub(crate) fn append_value_to_col(
    col: &mut Vec<u8>,
    blob: &mut Vec<u8>,
    ty: ColType,
    c: &Constant,
) -> Result<(), GnitzSqlError> {
    let tc = ty.tc;
    // NULL is the one value every column type encodes alike: a zeroed cell of
    // the type's own stride, with the null bit set by the caller.
    if matches!(c.lit, BExpr::LitNull) {
        push_zero_cell(col, tc);
        return Ok(());
    }
    match tc {
        TypeCode::F32 | TypeCode::F64 => {
            // Negating the parsed magnitude is exact at every IEEE value, `-0.0`
            // and `-inf` included — which is why the sign travels beside it.
            let mag = float_magnitude(c)?;
            let v = if c.negated { -mag } else { mag };
            // F32 rounds through binary64, as `ZSetBatch::append` does for a
            // Python float, so the two ingest paths agree bit for bit.
            match tc {
                TypeCode::F32 => col.extend_from_slice(&(v as f32).to_le_bytes()),
                _ => col.extend_from_slice(&v.to_le_bytes()),
            }
            Ok(())
        }
        TypeCode::String => match &c.lit {
            BExpr::LitStr(s) => {
                col.extend_from_slice(&gnitz_wire::encode_german_string(s.as_bytes(), blob));
                Ok(())
            }
            _ => Err(GnitzSqlError::Bind("number literal for string column".to_string())),
        },
        // A BLOB column is *not* text-writable: it takes bytes, and a written
        // literal spells none.
        TypeCode::Blob => Err(GnitzSqlError::Bind(match c.lit {
            BExpr::LitStr(_) => "string literal for non-string column".to_string(),
            _ => "number literal for blob column".to_string(),
        })),
        // Every integer column, narrow and wide alike, a DECIMAL column, plus a
        // UUID column's single-quoted spelling — one literal grammar, through
        // `pk_codec`.
        _ => {
            let packed = c.key_packed(ty).map_err(|e| {
                GnitzSqlError::Bind(match e {
                    KeyLitError::NotNumeric if matches!(c.lit, BExpr::LitStr(_)) => {
                        "string literal for non-string column".to_string()
                    }
                    KeyLitError::NotNumeric => format!("{c} is not a {ty} value"),
                    KeyLitError::NotOfType => format!("invalid {} literal: {c}", tc.wire_name()),
                    KeyLitError::NegativeIntoUnsigned | KeyLitError::OutOfRange => {
                        format!("{ty} value out of range: {c}")
                    }
                })
            })?;
            col.extend_from_slice(&packed.to_le_bytes()[..tc.wire_stride()]);
            Ok(())
        }
    }
}

/// A float column's value with the sign not yet applied. Every numeric spelling
/// reaches one, a magnitude past `i128` included — which a DOUBLE holds and no
/// integer parse would.
fn float_magnitude(c: &Constant) -> Result<f64, GnitzSqlError> {
    if let Some(v) = c.lit.int_literal() {
        return Ok(v as f64);
    }
    match &c.lit {
        BExpr::LitFloat { v, .. } => Ok(*v),
        BExpr::LitWide(n) => Ok(n.mag as f64),
        _ => Err(GnitzSqlError::Bind("string literal for non-string column".to_string())),
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
