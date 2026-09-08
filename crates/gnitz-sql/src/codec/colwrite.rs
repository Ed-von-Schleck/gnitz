//! Column value encoding: SQL literal / computed value → one §6 region cell.
//!
//! Two write paths share this module. INSERT appends written constants
//! (`append_value_to_col`); SET / `ON CONFLICT DO UPDATE` append computed
//! values (`append_column_value`). Both range-check against the column's type
//! and reject an out-of-range value through the same `pk_codec` packer, so `300`
//! means the same thing whichever verb writes it.
//!
//! Their admissible domains differ asymmetrically: [`set_target_admits`] limits
//! SET to `FixedInt ∪ String`, so INSERT can write a float, a UUID or a
//! `DECIMAL(38,0)` value that SET refuses; nothing SET admits is refused here.

use crate::ast_util::Constant;
use crate::codec::pk_codec::{pack_pk_value, KeyLitError};
use crate::error::GnitzSqlError;
use crate::ir::BExpr;
use gnitz_core::{push_zero_cell, ColType, ColumnDef, FixedInt, TypeCode};

/// A computed SET / `DO UPDATE` value.
///
/// `Int` is `i128` rather than `i64` because a SET target may be `U64`, whose
/// upper half no `i64` spells: a literal above `i64::MAX` binds as a wide
/// literal and is parsed against the target's type code before it gets here.
/// Every SET-admissible integer type is ≤ 8 bytes ([`set_target_admits`] gates
/// on `FixedInt`), so `i128` covers the whole domain with room for the sign.
#[derive(Clone)]
pub(crate) enum ColumnValue {
    Int(i128),
    Str(Vec<u8>),
    Null,
}

/// Append one INSERT constant to column region `col`, spilling a string into
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
            let mag = float_magnitude(c, tc)?;
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
                    KeyLitError::NotNumeric => "string literal for non-string column".to_string(),
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
fn float_magnitude(c: &Constant, tc: TypeCode) -> Result<f64, GnitzSqlError> {
    match &c.lit {
        BExpr::LitFloat(v) => Ok(*v),
        BExpr::LitInt(v) => Ok(*v as f64),
        BExpr::LitWide(s) => s
            .parse::<f64>()
            .map_err(|_| GnitzSqlError::Bind(format!("invalid {tc:?}: {s}"))),
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

/// Which column types a SET value of each kind may target — the rule
/// [`append_column_value`] enforces, stated once so the SET compiler can apply
/// it before any row is fetched instead of only when one matched. `NULL` is
/// admissible everywhere and needs no entry.
pub(crate) fn set_target_admits(tc: TypeCode, str_valued: bool) -> bool {
    if str_valued {
        tc == TypeCode::String
    } else {
        FixedInt::from_type_code(tc).is_some()
    }
}

/// Append one computed SET / `DO UPDATE` value to column region `col`, spilling
/// a string into `blob`.
pub(crate) fn append_column_value(
    col: &mut Vec<u8>,
    blob: &mut Vec<u8>,
    cv: ColumnValue,
    tc: TypeCode,
) -> Result<(), GnitzSqlError> {
    // `classify_set_rhs` settles the kind match per statement, where it can name
    // the column; this is the same rule restated where the two `unreachable!`s
    // below rely on it, and is unreachable in a well-formed compile.
    debug_assert!(
        matches!(cv, ColumnValue::Null) || set_target_admits(tc, matches!(cv, ColumnValue::Str(_))),
        "SET value kind must be settled by classify_set_rhs, not here ({tc:?})"
    );
    match cv {
        ColumnValue::Null => push_zero_cell(col, tc),
        // Range-checked and packed by the same `pk_codec` rule INSERT uses: an
        // out-of-range value declines rather than wrapping to its low bits.
        ColumnValue::Int(i) => {
            let packed =
                pack_pk_value(tc, i).ok_or_else(|| GnitzSqlError::Bind(format!("{tc:?} value out of range: {i}")))?;
            col.extend_from_slice(&packed.to_le_bytes()[..tc.wire_stride()]);
        }
        ColumnValue::Str(s) => col.extend_from_slice(&gnitz_wire::encode_german_string(&s, blob)),
    }
    Ok(())
}

#[cfg(test)]
#[path = "tests/colwrite.rs"]
mod tests;
