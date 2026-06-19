//! Column value encoding: SQL literal / computed value → `ColData`.
//!
//! Two write paths share this module. INSERT appends parsed literals
//! (`append_value_to_col`); SET / `ON CONFLICT DO UPDATE` append computed
//! values (`append_column_value`). They differ deliberately in their numeric
//! policy — INSERT range-checks each literal and rejects out-of-range, while
//! SET *wraps* (two's-complement cast) — so the shared mechanic
//! (`encode_numeric`) is the byte emission, not the accept/reject decision.

use crate::codec::pk_codec::parse_uuid_str;
use crate::error::GnitzSqlError;
use gnitz_core::{ColData, FixedInt, TypeCode};
use sqlparser::ast::{Expr, UnaryOperator, Value};

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

/// Append a NULL for column `col` of wire type `tc`. The NULL encoding is
/// uniform across all four `ColData` variants, so INSERT and UPDATE share it.
pub(crate) fn append_null(col: &mut ColData, tc: TypeCode) {
    match col {
        ColData::Fixed(buf) => buf.extend(std::iter::repeat_n(0u8, tc.wire_stride())),
        ColData::Strings(v) => v.push(None),
        ColData::Bytes(v) => v.push(None),
        ColData::U128s(v) => v.push(0u128),
    }
}

pub(crate) fn append_value_to_col(col: &mut ColData, tc: TypeCode, val_expr: &Expr) -> Result<(), GnitzSqlError> {
    // sqlparser parses negative number literals as UnaryOp(Minus, Number(...)).
    // Unwrap here so the rest of the function sees a plain Value::Number.
    let (val_expr, negated) = match val_expr {
        Expr::UnaryOp {
            op: UnaryOperator::Minus,
            expr,
        } => (expr.as_ref(), true),
        e => (e, false),
    };

    match val_expr {
        Expr::Value(vws) => {
            match &vws.value {
                Value::Null => {
                    append_null(col, tc);
                    Ok(())
                }
                Value::Number(n, _) => {
                    // Build the effective numeric string with optional leading minus.
                    let neg_buf;
                    let n: &str = if negated {
                        neg_buf = format!("-{n}");
                        &neg_buf
                    } else {
                        n.as_str()
                    };
                    match col {
                        ColData::Fixed(buf) => {
                            match tc {
                                TypeCode::F32 => {
                                    let v = n
                                        .parse::<f32>()
                                        .map_err(|_| GnitzSqlError::Bind(format!("invalid f32: {n}")))?;
                                    buf.extend_from_slice(&v.to_le_bytes());
                                }
                                TypeCode::F64 => {
                                    let v = n
                                        .parse::<f64>()
                                        .map_err(|_| GnitzSqlError::Bind(format!("invalid f64: {n}")))?;
                                    buf.extend_from_slice(&v.to_le_bytes());
                                }
                                // Integer columns (U8..I64); the sign is already folded into `n`.
                                // One parse + range-check + pack via `FixedInt`, matching the
                                // accept/reject set of the PK-literal path (`parse_pk_literal_packed`).
                                // `from_type_code` is `None` only for the non-integer fixed types
                                // that never back a `ColData::Fixed` column.
                                _ => {
                                    let fi = FixedInt::from_type_code(tc).ok_or_else(|| {
                                        GnitzSqlError::Bind(format!("unexpected type {tc:?} for number literal"))
                                    })?;
                                    let (min, max) = fi.range();
                                    let v = n.parse::<i128>().ok().filter(|v| (min..=max).contains(v)).ok_or_else(
                                        || GnitzSqlError::Bind(format!("{tc:?} value out of range: {n}")),
                                    )?;
                                    buf.extend_from_slice(&fi.pack(v).to_le_bytes()[..fi.width()]);
                                }
                            }
                            Ok(())
                        }
                        ColData::U128s(v) => {
                            let val = n
                                .parse::<u128>()
                                .map_err(|_| GnitzSqlError::Bind(format!("invalid u128: {n}")))?;
                            v.push(val);
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
                    ColData::U128s(v) if tc == TypeCode::UUID => {
                        v.push(parse_uuid_str(s)?);
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

pub(crate) fn append_column_value(col: &mut ColData, cv: ColumnValue, tc: TypeCode) -> Result<(), GnitzSqlError> {
    match cv {
        ColumnValue::Null => append_null(col, tc),
        ColumnValue::Int(i) => match col {
            ColData::Fixed(buf) => {
                // Wrap-cast to the column's width (`i as u8`/`as i16`/…), the
                // long-standing SET semantics, via the shared byte emitter.
                let fi = FixedInt::from_type_code(tc)
                    .ok_or_else(|| GnitzSqlError::Bind(format!("cannot assign Int to {tc:?}")))?;
                encode_numeric(buf, fi, i);
            }
            _ => return Err(GnitzSqlError::Bind("Int value for non-numeric column".to_string())),
        },
        ColumnValue::Str(s) => match col {
            ColData::Strings(v) => v.push(Some(s)),
            _ => return Err(GnitzSqlError::Bind("String value for non-string column".to_string())),
        },
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{num_expr, uuid_schema_payload, uuid_str_expr};

    #[test]
    fn test_null_append_insert_update_identical_all_variants() {
        let null_expr = Expr::Value(sqlparser::ast::ValueWithSpan {
            value: Value::Null,
            span: sqlparser::tokenizer::Span::empty(),
        });
        // (fresh empty ColData, wire type, expected NULL encoding) per variant.
        let cases: [(ColData, TypeCode, ColData); 4] = [
            (ColData::Fixed(Vec::new()), TypeCode::U32, ColData::Fixed(vec![0u8; 4])),
            (
                ColData::Strings(Vec::new()),
                TypeCode::String,
                ColData::Strings(vec![None]),
            ),
            (ColData::Bytes(Vec::new()), TypeCode::Blob, ColData::Bytes(vec![None])),
            (ColData::U128s(Vec::new()), TypeCode::UUID, ColData::U128s(vec![0u128])),
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
        use sqlparser::ast::{UnaryOperator, ValueWithSpan};
        use sqlparser::tokenizer::Span;

        fn num(n: &str) -> Expr {
            Expr::Value(ValueWithSpan {
                value: Value::Number(n.into(), false),
                span: Span::empty(),
            })
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
        if let ColData::U128s(v) = &batch.columns[1] {
            assert_eq!(v[0], 0x550e8400_e29b_41d4_a716_446655440000_u128);
        } else {
            panic!("expected U128s");
        }
    }

    #[test]
    fn test_uuid_decimal_literal_still_accepted() {
        let schema = uuid_schema_payload();
        let mut batch = gnitz_core::ZSetBatch::new(&schema);
        let big_val: u128 = 0x550e8400_e29b_41d4_a716_446655440000_u128;
        append_value_to_col(&mut batch.columns[1], TypeCode::UUID, &num_expr(&big_val.to_string())).unwrap();
        if let ColData::U128s(v) = &batch.columns[1] {
            assert_eq!(v[0], big_val);
        } else {
            panic!("expected U128s");
        }
    }
}
