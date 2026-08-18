//! Column value encoding: SQL literal / computed value → `ColData`.
//!
//! Two write paths share this module. INSERT appends parsed literals
//! (`append_value_to_col`); SET / `ON CONFLICT DO UPDATE` append computed
//! values (`append_column_value`). They differ deliberately in their numeric
//! policy — INSERT range-checks each literal and rejects out-of-range, while
//! SET *wraps* (two's-complement cast) — so the shared mechanic
//! (`encode_numeric`) is the byte emission, not the accept/reject decision.

use crate::codec::pk_codec::{parse_pk_literal_packed, parse_uuid_str};
use crate::error::GnitzSqlError;
use gnitz_core::{ColData, ColumnDef, FixedInt, TypeCode};
use sqlparser::ast::{Expr, UnaryOperator, Value};

#[derive(Clone)]
pub(crate) enum ColumnValue {
    Int(i64),
    Str(String),
    Null,
}

/// Append the native little-endian bytes of integer `value`, wrapped
/// (two's-complement truncated) to `fi`'s width — the SET / `DO UPDATE` cast
/// policy (`i as u8`, `i as i16`, …). This is `FixedInt::pack` without its
/// in-range debug-assert: the SET path wraps an out-of-range value rather than
/// rejecting it, so masking to the width directly preserves that behavior. The
/// INSERT and PK-literal paths instead range-check (declining out-of-range)
/// before they reach the wire and never route through here.
fn encode_numeric(buf: &mut Vec<u8>, fi: FixedInt, value: i64) {
    let width = fi.width();
    let packed = (value as u128) & (u128::MAX >> (128 - 8 * width));
    buf.extend_from_slice(&packed.to_le_bytes()[..width]);
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
                    ColData::Fixed(buf) if tc == TypeCode::UUID => {
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
    let str_valued = matches!(cv, ColumnValue::Str(_));
    if !matches!(cv, ColumnValue::Null) && !set_target_admits(tc, str_valued) {
        return Err(GnitzSqlError::Bind(format!(
            "cannot assign {} value to a {tc:?} column",
            if str_valued { "string" } else { "integer" }
        )));
    }
    match cv {
        ColumnValue::Null => col.push_null(tc),
        // Wrap-cast to the column's width (`i as u8`/`as i16`/…), the
        // long-standing SET semantics, via the shared byte emitter.
        ColumnValue::Int(i) => match col {
            ColData::Fixed(buf) => encode_numeric(buf, FixedInt::from_type_code(tc).expect("admitted above"), i),
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
mod tests {
    use super::*;
    use crate::test_support::{num_expr, uuid_schema_payload, uuid_str_expr};

    /// The lone UUID cell of a `Fixed` column, from its 16 LE bytes.
    fn uuid_cell(col: &ColData) -> u128 {
        let ColData::Fixed(b) = col else {
            panic!("expected Fixed")
        };
        u128::from_le_bytes(b[..16].try_into().unwrap())
    }

    #[test]
    fn test_null_append_insert_update_identical_all_variants() {
        let null_expr = Expr::value(Value::Null);
        // (fresh empty ColData, wire type, expected NULL encoding) per variant.
        let cases: [(ColData, TypeCode, ColData); 4] = [
            (ColData::Fixed(Vec::new()), TypeCode::U32, ColData::Fixed(vec![0u8; 4])),
            (
                ColData::Strings(Vec::new()),
                TypeCode::String,
                ColData::Strings(vec![None]),
            ),
            (ColData::Bytes(Vec::new()), TypeCode::Blob, ColData::Bytes(vec![None])),
            (
                ColData::Fixed(Vec::new()),
                TypeCode::UUID,
                ColData::Fixed(vec![0u8; 16]),
            ),
        ];
        for (empty, tc, expected) in cases {
            let mut via_insert = empty.clone();
            append_value_to_col(&mut via_insert, tc, &null_expr).unwrap();
            let mut via_update = empty.clone();
            append_column_value(&mut via_update, ColumnValue::Null, tc).unwrap();
            assert_eq!(via_insert, expected, "INSERT NULL encoding for {tc:?}");
            assert_eq!(via_update, expected, "UPDATE NULL encoding for {tc:?}");
            assert_eq!(via_insert, via_update, "INSERT vs UPDATE NULL must match for {tc:?}");
        }
    }

    /// Every fixed-int width wraps to the byte-identical encoding the per-type
    /// `i as uN` casts produced before `encode_numeric` collapsed them.
    #[test]
    fn encode_numeric_matches_native_casts() {
        let cases: [(TypeCode, i64, Vec<u8>); 8] = [
            (TypeCode::U8, 300, vec![300u16 as u8]),
            (TypeCode::I8, -5, vec![(-5i8) as u8]),
            (TypeCode::U16, 70000, (70000u32 as u16).to_le_bytes().to_vec()),
            (TypeCode::I16, -2, (-2i16).to_le_bytes().to_vec()),
            (TypeCode::U32, -1, ((-1i64) as u32).to_le_bytes().to_vec()),
            (TypeCode::I32, -1, (-1i32).to_le_bytes().to_vec()),
            (TypeCode::U64, -1, ((-1i64) as u64).to_le_bytes().to_vec()),
            (TypeCode::I64, i64::MIN, i64::MIN.to_le_bytes().to_vec()),
        ];
        for (tc, v, expected) in cases {
            let mut col = ColData::Fixed(Vec::new());
            append_column_value(&mut col, ColumnValue::Int(v), tc).unwrap();
            assert_eq!(col, ColData::Fixed(expected), "encode for {tc:?} value {v}");
        }
    }

    /// INSERT integer literals encode byte-identically to the old per-type
    /// `parse::<uN>()` + `to_le_bytes` ladder across widths and signs, and
    /// out-of-range / wrong-sign literals are still rejected (the `FixedInt::range`
    /// check that replaced the per-type `parse`).
    #[test]
    fn insert_int_encoding_matches_native_and_range_checks() {
        use sqlparser::ast::UnaryOperator;

        fn num(n: &str) -> Expr {
            Expr::value(Value::Number(n.into(), false))
        }
        fn neg(n: &str) -> Expr {
            Expr::UnaryOp {
                op: UnaryOperator::Minus,
                expr: Box::new(num(n)),
            }
        }
        fn encoded(tc: TypeCode, e: &Expr) -> Result<Vec<u8>, GnitzSqlError> {
            let mut col = ColData::Fixed(Vec::new());
            append_value_to_col(&mut col, tc, e)?;
            match col {
                ColData::Fixed(b) => Ok(b),
                _ => unreachable!(),
            }
        }

        // Byte-identical to the native casts at every width / sign / type edge.
        assert_eq!(encoded(TypeCode::U8, &num("255")).unwrap(), vec![255u8]);
        assert_eq!(encoded(TypeCode::I8, &neg("5")).unwrap(), vec![(-5i8) as u8]);
        assert_eq!(encoded(TypeCode::U16, &num("65535")).unwrap(), 65535u16.to_le_bytes());
        assert_eq!(encoded(TypeCode::I16, &neg("2")).unwrap(), (-2i16).to_le_bytes());
        assert_eq!(encoded(TypeCode::I32, &neg("1")).unwrap(), (-1i32).to_le_bytes());
        assert_eq!(
            encoded(TypeCode::U64, &num("18446744073709551615")).unwrap(),
            u64::MAX.to_le_bytes()
        );
        assert_eq!(
            encoded(TypeCode::I64, &neg("9223372036854775808")).unwrap(),
            i64::MIN.to_le_bytes()
        );

        // Out-of-range and wrong-sign literals are rejected.
        assert!(encoded(TypeCode::U8, &num("256")).is_err());
        assert!(encoded(TypeCode::I8, &num("128")).is_err());
        assert!(encoded(TypeCode::U8, &neg("1")).is_err());
        assert!(encoded(TypeCode::I32, &num("3000000000")).is_err());
    }

    // ------------------------------------------------------------------
    // append_value_to_col — UUID column accepts both a single-quoted UUID
    // string and a decimal u128 literal.
    // ------------------------------------------------------------------

    #[test]
    fn test_uuid_non_pk_string_literal_accepted() {
        let schema = uuid_schema_payload();
        let mut batch = gnitz_core::ZSetBatch::new(&schema);
        // col 1 is UUID
        append_value_to_col(
            &mut batch.columns[1],
            TypeCode::UUID,
            &uuid_str_expr("550e8400-e29b-41d4-a716-446655440000"),
        )
        .unwrap();
        assert_eq!(
            uuid_cell(&batch.columns[1]),
            0x550e8400_e29b_41d4_a716_446655440000_u128
        );
    }

    #[test]
    fn test_uuid_decimal_literal_still_accepted() {
        let schema = uuid_schema_payload();
        let mut batch = gnitz_core::ZSetBatch::new(&schema);
        let big_val: u128 = 0x550e8400_e29b_41d4_a716_446655440000_u128;
        append_value_to_col(&mut batch.columns[1], TypeCode::UUID, &num_expr(&big_val.to_string())).unwrap();
        assert_eq!(uuid_cell(&batch.columns[1]), big_val);
    }

    /// A leading `+` reaches the payload slot the same way it reaches the PK
    /// slot: `pk_codec::extract_sql_literal` accepts both signs, so one INSERT
    /// row must not take `+1` for its key and refuse `+2` for its payload.
    #[test]
    fn a_unary_plus_literal_is_accepted_like_the_pk_slot_accepts_it() {
        let plus = Expr::UnaryOp {
            op: UnaryOperator::Plus,
            expr: Box::new(num_expr("2")),
        };
        let mut col = ColData::Fixed(Vec::new());
        append_value_to_col(&mut col, TypeCode::U32, &plus).expect("`+2` must bind");
        let ColData::Fixed(b) = &col else {
            panic!("expected Fixed")
        };
        assert_eq!(b.as_slice(), 2u32.to_le_bytes());
    }
}
