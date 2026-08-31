//! Column value encoding: SQL literal / computed value → `ColData`.
//!
//! Two write paths share this module. INSERT appends parsed literals
//! (`append_value_to_col`); SET / `ON CONFLICT DO UPDATE` append computed
//! values (`append_column_value`). Both range-check against the column's type
//! and reject an out-of-range value, through the same `pk_codec` packer — so
//! `300` means the same thing whichever verb writes it, and neither path can
//! land a value the other would refuse.

use crate::codec::pk_codec::{pack_pk_value, parse_pk_literal_packed, parse_uuid_str};
use crate::error::GnitzSqlError;
use gnitz_core::{ColData, ColumnDef, FixedInt, TypeCode};
use sqlparser::ast::{Expr, UnaryOperator, Value};

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
    Str(String),
    Null,
}

pub(crate) fn append_value_to_col(col: &mut ColData, tc: TypeCode, val_expr: &Expr) -> Result<(), GnitzSqlError> {
    // sqlparser lexes a literal's sign as a separate unary operator, so `+5` and
    // `-5` are both `UnaryOp`, never a bare `Value::Number`. Both signs unwrap
    // here for the same reason `pk_codec::extract_sql_literal` accepts both: the
    // PK slot and the payload slot of one INSERT row must agree on what a signed
    // literal is.
    let (val_expr, negated) = match val_expr {
        Expr::UnaryOp {
            op: op @ (UnaryOperator::Minus | UnaryOperator::Plus),
            expr,
        } => (expr.as_ref(), matches!(op, UnaryOperator::Minus)),
        e => (e, false),
    };

    match val_expr {
        Expr::Value(vws) => {
            match &vws.value {
                Value::Null => {
                    col.push_null(tc);
                    Ok(())
                }
                Value::Number(n, _) => {
                    // A `Value::Number` is bare digits; the sign is the `UnaryOp`
                    // peeled above. `sign` re-attaches it for the messages only.
                    let sign = if negated { "-" } else { "" };
                    match col {
                        ColData::Fixed(buf) => {
                            match tc {
                                // IEEE negation of the parsed magnitude is exact at
                                // every value, `-0.0` and `-inf` included.
                                TypeCode::F32 => {
                                    let v = n
                                        .parse::<f32>()
                                        .map_err(|_| GnitzSqlError::Bind(format!("invalid f32: {sign}{n}")))?;
                                    buf.extend_from_slice(&(if negated { -v } else { v }).to_le_bytes());
                                }
                                TypeCode::F64 => {
                                    let v = n
                                        .parse::<f64>()
                                        .map_err(|_| GnitzSqlError::Bind(format!("invalid f64: {sign}{n}")))?;
                                    buf.extend_from_slice(&(if negated { -v } else { v }).to_le_bytes());
                                }
                                // Every integer column, narrow (U8..I64) and wide
                                // (U128/UUID/I128) alike. Route through the single source of truth
                                // for literal acceptance (`pk_codec`) so the INSERT-value path and
                                // PK-seek routing accept/reject identically. The packed u128's low
                                // `wire_stride()` bytes are the column's LE image at every width.
                                _ => {
                                    let packed = parse_pk_literal_packed(tc, n, negated).ok_or_else(|| {
                                        GnitzSqlError::Bind(format!("{tc:?} value out of range: {sign}{n}"))
                                    })?;
                                    buf.extend_from_slice(&packed.to_le_bytes()[..tc.wire_stride()]);
                                }
                            }
                            Ok(())
                        }
                        ColData::Strings(_) => Err(GnitzSqlError::Bind("number literal for string column".to_string())),
                        ColData::Bytes(_) => Err(GnitzSqlError::Bind("number literal for blob column".to_string())),
                    }
                }
                Value::SingleQuotedString(s) | Value::DoubleQuotedString(s) => match col {
                    ColData::Strings(v) => {
                        v.push(Some(s.clone()));
                        Ok(())
                    }
                    // UUID: the one non-STRING type a text literal spells a value
                    // of. Both write paths gate on the same `admits_text_literal`
                    // predicate, so neither can start accepting text for a type
                    // the other rejects — the divergence class this replaces was
                    // bare hex reaching a U128 column through the Python binding
                    // alone.
                    ColData::Fixed(buf) if tc.admits_text_literal() => {
                        buf.extend_from_slice(&parse_uuid_str(s)?.to_le_bytes());
                        Ok(())
                    }
                    _ => Err(GnitzSqlError::Bind("string literal for non-string column".to_string())),
                },
                _ => Err(GnitzSqlError::Unsupported(format!(
                    "unsupported value in INSERT: {:?}",
                    vws.value
                ))),
            }
        }
        _ => Err(GnitzSqlError::Unsupported(format!(
            "unsupported value expression in INSERT: {val_expr:?}"
        ))),
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
        // Implies `ColData::Fixed`: the variant is chosen from the type code.
        FixedInt::from_type_code(tc).is_some()
    }
}

pub(crate) fn append_column_value(col: &mut ColData, cv: ColumnValue, tc: TypeCode) -> Result<(), GnitzSqlError> {
    // `classify_set_rhs` settles the kind match per statement, where it can name
    // the column; this is the same rule restated where the two `unreachable!`s
    // below rely on it, and is unreachable in a well-formed compile.
    debug_assert!(
        matches!(cv, ColumnValue::Null) || set_target_admits(tc, matches!(cv, ColumnValue::Str(_))),
        "SET value kind must be settled by classify_set_rhs, not here ({tc:?})"
    );
    match cv {
        ColumnValue::Null => col.push_null(tc),
        // Range-checked and packed by the same `pk_codec` rule INSERT uses: an
        // out-of-range value declines rather than wrapping to its low bits.
        ColumnValue::Int(i) => match col {
            ColData::Fixed(buf) => {
                let packed = pack_pk_value(tc, i)
                    .ok_or_else(|| GnitzSqlError::Bind(format!("{tc:?} value out of range: {i}")))?;
                buf.extend_from_slice(&packed.to_le_bytes()[..tc.wire_stride()]);
            }
            other => unreachable!("a fixed-int type code implies ColData::Fixed, got {other:?}"),
        },
        ColumnValue::Str(s) => match col {
            ColData::Strings(v) => v.push(Some(s)),
            other => unreachable!("TypeCode::String implies ColData::Strings, got {other:?}"),
        },
    }
    Ok(())
}

#[cfg(test)]
#[path = "tests/colwrite.rs"]
mod tests;
