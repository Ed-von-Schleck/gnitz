//! Literal parsing + PK/seek key packing — the partition-routing source of
//! truth.
//!
//! `parse_pk_literal_packed` is the single helper through which every INSERT and
//! SEEK PK literal flows (`extract_pk_value`, plus `try_col_eq_literal` /
//! `try_extract_pk_in` in the WHERE planner), so the master cannot route an
//! INSERT and a DELETE for the same key to different workers.

use crate::error::GnitzSqlError;
use gnitz_core::{FixedInt, PkTuple, Schema, TypeCode};
use sqlparser::ast::{Expr, UnaryOperator, Value};

pub(crate) fn parse_uuid_str(s: &str) -> Result<u128, GnitzSqlError> {
    let s = s.trim();
    let hex: String = if s.len() == 36
        && s.as_bytes()[8] == b'-'
        && s.as_bytes()[13] == b'-'
        && s.as_bytes()[18] == b'-'
        && s.as_bytes()[23] == b'-'
    {
        format!("{}{}{}{}{}", &s[..8], &s[9..13], &s[14..18], &s[19..23], &s[24..])
    } else if s.len() == 32 {
        s.to_string()
    } else {
        return Err(GnitzSqlError::Bind(format!("invalid UUID literal: {s:?}")));
    };
    u128::from_str_radix(&hex, 16).map_err(|_| GnitzSqlError::Bind(format!("invalid UUID literal: {s:?}")))
}

/// Parse a numeric SQL literal as `i128`, applying `negated` (the literal sat
/// under `Expr::UnaryOp(Minus, _)`) by prepending `-` to the digit string
/// before parsing — that rule accepts a type minimum's own digit string
/// (e.g. `i64::MIN`'s), which a parse-then-negate would overflow. `i128`
/// covers every ≤8-byte type with room to spare, so an out-of-type-range
/// literal is *representable* — callers classify it against
/// `FixedInt::range` and decline or saturate, never wrap.
pub(crate) fn parse_literal_i128(n_str: &str, negated: bool) -> Option<i128> {
    let s_owned;
    let s: &str = if negated {
        s_owned = format!("-{n_str}");
        &s_owned
    } else {
        n_str
    };
    s.parse::<i128>().ok()
}

/// Parse a numeric SQL literal into its packed-u128 PK form: the low
/// `wire_stride` bytes carry the column's native LE encoding
/// (`FixedInt::pack` — e.g. `-1_i8` → `0xFF`, not `0xFFFF_FFFF_FFFF_FFFF`),
/// the rest stay zero. An out-of-type-range literal declines (`None`) instead
/// of wrapping: without the `FixedInt::range` check, `x = 3000000000` on an
/// I32 column would cast to `-1294967296` and seek the wrong value (silent
/// wrong rows — the cff7c58-class trap).
///
/// This helper is the single source of truth for INSERT/SEEK PK routing —
/// `extract_pk_value`, `try_col_eq_literal`, and `try_extract_pk_in` all
/// dispatch through it so the master cannot send INSERT and DELETE for the
/// same key to different workers.
pub(crate) fn parse_pk_literal_packed(tc: TypeCode, n_str: &str, negated: bool) -> Option<u128> {
    match tc {
        // 16-byte types are their own packed form. U128/UUID take the full
        // unsigned range; I128 (the internal join-key promotion type) is
        // two's complement at 16 bytes, exactly `as u128`.
        TypeCode::U128 | TypeCode::UUID => {
            if negated {
                return None;
            }
            n_str.parse::<u128>().ok()
        }
        TypeCode::I128 => parse_literal_i128(n_str, negated).map(|v| v as u128),
        _ => {
            let fi = FixedInt::from_type_code(tc)?;
            let (min, max) = fi.range();
            let v = parse_literal_i128(n_str, negated)?;
            (min <= v && v <= max).then(|| fi.pack(v))
        }
    }
}

/// A SQL literal extracted from an `Expr` for PK/seek routing. `Number`'s
/// second field is `negated` (the literal sat under `UnaryOp(Minus, _)`);
/// `Str` carries the unescaped single-quoted contents.
pub(crate) enum SqlLiteral<'a> {
    Number(&'a str, bool),
    Str(&'a str),
    Null,
}

/// Centralizes the `Expr::Value` / `UnaryOp(Minus, Number)` unwrap shared by
/// the SEEK/INSERT parse sites (`parse_one_pk_literal`, `try_col_eq_literal`,
/// `try_extract_pk_in`).
///
/// Matches `SingleQuotedString` only — NOT `DoubleQuotedString`: in
/// `GenericDialect` a double-quoted token is an identifier, so treating
/// `col = "x"` as a UUID seek literal would silently change which queries
/// take the index fast path.
pub(crate) fn extract_sql_literal(expr: &Expr) -> Option<SqlLiteral<'_>> {
    match expr {
        Expr::Value(vws) => match &vws.value {
            Value::Null => Some(SqlLiteral::Null),
            Value::Number(n, _) => Some(SqlLiteral::Number(n, false)),
            Value::SingleQuotedString(s) => Some(SqlLiteral::Str(s)),
            _ => None,
        },
        Expr::UnaryOp {
            op: UnaryOperator::Minus,
            expr,
        } => match expr.as_ref() {
            Expr::Value(vws) => match &vws.value {
                Value::Number(n, _) => Some(SqlLiteral::Number(n, true)),
                _ => None,
            },
            _ => None,
        },
        _ => None,
    }
}

/// Parse one PK column literal at `pk_expr` into its packed u128 form.
/// Routes through `parse_pk_literal_packed` (numerics) or `parse_uuid_str`
/// (UUID); the returned u128's low `wire_stride` bytes carry the column's
/// native LE bytes.
pub(crate) fn parse_one_pk_literal(pk_expr: &Expr, tc: TypeCode, col_name: &str) -> Result<u128, GnitzSqlError> {
    match extract_sql_literal(pk_expr) {
        Some(SqlLiteral::Number(n, negated)) => parse_pk_literal_packed(tc, n, negated).ok_or_else(|| {
            if negated
                && matches!(
                    tc,
                    TypeCode::U8 | TypeCode::U16 | TypeCode::U32 | TypeCode::U64 | TypeCode::U128 | TypeCode::UUID
                )
            {
                GnitzSqlError::Bind(format!(
                    "PK column '{col_name}' of type {tc:?} does not accept negative literals"
                ))
            } else {
                let s_disp = if negated { format!("-{n}") } else { n.to_string() };
                GnitzSqlError::Bind(format!("PK column '{col_name}' value is not a valid {tc:?}: {s_disp}"))
            }
        }),
        // UUID accepts a single-quoted UUID string; non-UUID PKs are numeric only.
        Some(SqlLiteral::Str(s)) if tc == TypeCode::UUID => parse_uuid_str(s),
        _ => Err(GnitzSqlError::Bind(format!(
            "PK column '{col_name}' value must be a numeric literal"
        ))),
    }
}

/// Extract the primary key from a VALUES row as a `PkTuple`. Walks the PK
/// columns in pk-list order, dispatches each through `parse_one_pk_literal`,
/// and copies the column's native LE bytes into the tuple buffer.
pub(crate) fn extract_pk_value(row: &[Expr], schema: &Schema) -> Result<PkTuple, GnitzSqlError> {
    let stride = schema.pk_stride() as u8;
    let mut tuple = PkTuple::new(stride);
    let mut off = 0usize;
    for &pi in schema.pk_indices() {
        let pk_expr = row.get(pi).ok_or_else(|| {
            GnitzSqlError::Bind(format!(
                "PK column '{}' missing from INSERT row",
                schema.columns[pi].name
            ))
        })?;
        let tc = schema.columns[pi].type_code;
        let v = parse_one_pk_literal(pk_expr, tc, &schema.columns[pi].name)?;
        let w = tc.wire_stride();
        tuple.buf[off..off + w].copy_from_slice(&v.to_le_bytes()[..w]);
        off += w;
    }
    debug_assert_eq!(off, stride as usize);
    Ok(tuple)
}

pub(crate) fn is_null_expr(expr: &Expr) -> bool {
    matches!(expr, Expr::Value(vws) if matches!(vws.value, Value::Null))
}

#[cfg(test)]
mod tests {
    use super::*;
    // `try_col_eq_literal` / `try_extract_pk_in` live in the WHERE planner
    // (`dml`); the parity test below pins all three routing entry points to the
    // same packed u128.
    use crate::dml::{try_col_eq_literal, try_extract_pk_in};
    use gnitz_core::ColumnDef;
    use sqlparser::ast::{BinaryOperator, ValueWithSpan};
    use sqlparser::tokenizer::Span;

    fn col_def(name: &str, tc: TypeCode, nullable: bool) -> ColumnDef {
        ColumnDef {
            name: name.into(),
            type_code: tc,
            is_nullable: nullable,
            fk_table_id: 0,
            fk_col_idx: 0,
        }
    }

    fn pk_schema(pk_tc: TypeCode) -> Schema {
        Schema {
            columns: vec![col_def("id", pk_tc, false), col_def("v", TypeCode::I64, false)],
            pk_cols: vec![0],
        }
    }

    fn uuid_schema_pk() -> Schema {
        Schema {
            columns: vec![col_def("id", TypeCode::UUID, false)],
            pk_cols: vec![0],
        }
    }

    fn num_expr(n: &str) -> Expr {
        Expr::Value(ValueWithSpan {
            value: Value::Number(n.into(), false),
            span: Span::empty(),
        })
    }

    fn neg_num_expr(n: &str) -> Expr {
        Expr::UnaryOp {
            op: UnaryOperator::Minus,
            expr: Box::new(Expr::Value(ValueWithSpan {
                value: Value::Number(n.into(), false),
                span: Span::empty(),
            })),
        }
    }

    fn uuid_str_expr(s: &str) -> Expr {
        Expr::Value(ValueWithSpan {
            value: Value::SingleQuotedString(s.into()),
            span: Span::empty(),
        })
    }

    fn dquote_expr(s: &str) -> Expr {
        Expr::Value(ValueWithSpan {
            value: Value::DoubleQuotedString(s.into()),
            span: Span::empty(),
        })
    }

    fn eq_expr(col: &str, rhs: Expr) -> Expr {
        Expr::BinaryOp {
            left: Box::new(Expr::Identifier(sqlparser::ast::Ident::new(col))),
            op: BinaryOperator::Eq,
            right: Box::new(rhs),
        }
    }

    fn in_list_expr(col: &str, items: Vec<Expr>) -> Expr {
        Expr::InList {
            expr: Box::new(Expr::Identifier(sqlparser::ast::Ident::new(col))),
            list: items,
            negated: false,
        }
    }

    #[test]
    fn test_parse_uuid_str_standard_format() {
        let v = parse_uuid_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        assert_eq!(v, 0x550e8400_e29b_41d4_a716_446655440000_u128);
    }

    #[test]
    fn test_parse_uuid_str_no_hyphens() {
        let v = parse_uuid_str("550e8400e29b41d4a716446655440000").unwrap();
        assert_eq!(v, 0x550e8400_e29b_41d4_a716_446655440000_u128);
    }

    #[test]
    fn test_parse_uuid_str_invalid_rejected() {
        assert!(parse_uuid_str("not-a-uuid").is_err());
        assert!(parse_uuid_str("zzzzzzzz-zzzz-zzzz-zzzz-zzzzzzzzzzzz").is_err());
        assert!(parse_uuid_str("").is_err());
    }

    #[test]
    fn test_uuid_pk_string_literal_accepted() {
        let schema = uuid_schema_pk();
        let row = vec![uuid_str_expr("550e8400-e29b-41d4-a716-446655440000")];
        let pk = extract_pk_value(&row, &schema).unwrap();
        // UUID PK has stride 16; the parsed u128 lives in the low 16 bytes.
        assert_eq!(pk.to_u128().unwrap(), 0x550e8400_e29b_41d4_a716_446655440000_u128);
    }

    /// All three parser entry points must agree byte-for-byte on the u128
    /// produced by a given (PK type, literal) pair. Master routes SEEK via
    /// `partition_for_key(pk)`; drift between any pair sends INSERT and
    /// DELETE to different workers.
    fn check_pk_parity(pk_tc: TypeCode, literal: Expr, expected: u128) {
        let schema = pk_schema(pk_tc);

        // 1. extract_pk_value (INSERT row).
        let row = vec![literal.clone(), num_expr("0")];
        let got_insert = extract_pk_value(&row, &schema).unwrap_or_else(|e| panic!("extract_pk_value({pk_tc:?}): {e}"));
        assert_eq!(got_insert.to_u128().unwrap(), expected, "extract_pk_value");

        // 2. try_col_eq_literal (WHERE pk = literal).
        let where_expr = eq_expr("id", literal.clone());
        let got_eq = try_col_eq_literal(&where_expr, &schema).expect("try_col_eq_literal returned None");
        assert_eq!(got_eq, (0, expected), "try_col_eq_literal");

        // 3. try_extract_pk_in (WHERE pk IN (literal)).
        let in_expr = in_list_expr("id", vec![literal]);
        let got_in = try_extract_pk_in(&in_expr, &schema).expect("try_extract_pk_in returned None");
        assert_eq!(got_in, vec![expected], "try_extract_pk_in");
    }

    #[test]
    fn pk_parity_i8_neg1() {
        check_pk_parity(TypeCode::I8, neg_num_expr("1"), (-1i8 as u8) as u128);
    }

    #[test]
    fn pk_parity_i16_neg1() {
        check_pk_parity(TypeCode::I16, neg_num_expr("1"), (-1i16 as u16) as u128);
    }

    #[test]
    fn pk_parity_i32_neg1() {
        check_pk_parity(TypeCode::I32, neg_num_expr("1"), (-1i32 as u32) as u128);
    }

    #[test]
    fn pk_parity_i64_neg1() {
        check_pk_parity(TypeCode::I64, neg_num_expr("1"), ((-1i64) as u64) as u128);
    }

    #[test]
    fn pk_parity_i64_min() {
        // Regression for the prepend-`-` parse rule: `(i64::MIN as u64) as u128`.
        check_pk_parity(
            TypeCode::I64,
            neg_num_expr("9223372036854775808"),
            (i64::MIN as u64) as u128,
        );
    }

    #[test]
    fn pk_parity_u16_max() {
        check_pk_parity(TypeCode::U16, num_expr("65535"), 65535u128);
    }

    #[test]
    fn pk_parity_u32_max() {
        check_pk_parity(TypeCode::U32, num_expr("4294967295"), 4294967295u128);
    }

    #[test]
    fn extract_pk_value_u64_rejects_negative() {
        let schema = pk_schema(TypeCode::U64);
        let row = vec![neg_num_expr("1"), num_expr("0")];
        let err = extract_pk_value(&row, &schema).expect_err("U64 PK must reject negative literal");
        assert!(err.to_string().contains("negative"), "error: {err}");
    }

    #[test]
    fn extract_pk_value_u128_rejects_negative() {
        let schema = pk_schema(TypeCode::U128);
        let row = vec![neg_num_expr("1"), num_expr("0")];
        assert!(extract_pk_value(&row, &schema).is_err());
    }

    #[test]
    fn try_extract_pk_in_negative_i32_list() {
        // Today's u64-only body returned None for negative items, falling
        // through to the slow full-scan path. Native-PK fast path must hit.
        let schema = pk_schema(TypeCode::I32);
        let expr = in_list_expr("id", vec![neg_num_expr("1"), neg_num_expr("2")]);
        let got = try_extract_pk_in(&expr, &schema).expect("should match fast path");
        assert_eq!(got, vec![(-1i32 as u32) as u128, (-2i32 as u32) as u128]);
    }

    #[test]
    fn test_double_quoted_pk_value_rejected() {
        // INSERT path: a double-quoted value in a UUID PK slot is not a literal.
        let err = parse_one_pk_literal(
            &dquote_expr("550e8400-e29b-41d4-a716-446655440000"),
            TypeCode::UUID,
            "id",
        )
        .expect_err("double-quoted UUID PK literal must be rejected");
        assert!(err.to_string().contains("numeric literal"), "error: {err}");
    }

    #[test]
    fn parse_pk_literal_packed_rejects_out_of_range() {
        // The regression guard for the truncation fix: an I32 literal above the
        // type max declines (None) instead of wrapping to -1294967296.
        assert_eq!(parse_pk_literal_packed(TypeCode::I32, "3000000000", false), None);
        assert_eq!(parse_pk_literal_packed(TypeCode::I32, "100", false), Some(100));
        assert_eq!(
            parse_pk_literal_packed(TypeCode::I32, "5", true),
            Some((-5i32 as u32) as u128)
        );
        // Unsigned ≤8B range-checks too.
        assert_eq!(parse_pk_literal_packed(TypeCode::U8, "300", false), None);
        assert_eq!(parse_pk_literal_packed(TypeCode::U8, "255", false), Some(255));
        // I128 (the internal join-key type): full-width two's complement,
        // negatives included.
        assert_eq!(
            parse_pk_literal_packed(TypeCode::I128, "5", true),
            Some((-5i128) as u128)
        );
    }
}
