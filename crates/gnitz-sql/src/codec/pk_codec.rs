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
    use crate::test_support::{
        col_def, compound_schema_u64_u64, dquote_expr, neg_num_expr, num_expr, pk_schema, uuid_schema_pk, uuid_str_expr,
    };
    use gnitz_core::ZSetBatch;

    fn compound_schema_u64_u64_u128() -> Schema {
        Schema {
            columns: vec![
                col_def("a", TypeCode::U64, false),
                col_def("b", TypeCode::U64, false),
                col_def("c", TypeCode::U128, false),
                col_def("v", TypeCode::I64, true),
            ],
            pk_cols: vec![0, 1, 2],
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

    #[test]
    fn compound_pk_extract_pk_value_packs_le_bytes() {
        let schema = compound_schema_u64_u64();
        let row = vec![num_expr("1"), num_expr("2"), num_expr("99")];
        let pk = extract_pk_value(&row, &schema).unwrap();
        assert_eq!(pk.stride, 16);
        let mut expect = [0u8; 16];
        expect[0..8].copy_from_slice(&1u64.to_le_bytes());
        expect[8..16].copy_from_slice(&2u64.to_le_bytes());
        assert_eq!(pk.as_bytes(), &expect[..]);
    }

    #[test]
    fn compound_pk_extract_pk_value_wide_region() {
        let schema = compound_schema_u64_u64_u128();
        let row = vec![num_expr("1"), num_expr("2"), num_expr("3"), num_expr("99")];
        let pk = extract_pk_value(&row, &schema).unwrap();
        // pk_stride = 8 + 8 + 16 = 32 → wide-region path.
        assert_eq!(pk.stride, 32);
        let mut expect = [0u8; 32];
        expect[0..8].copy_from_slice(&1u64.to_le_bytes());
        expect[8..16].copy_from_slice(&2u64.to_le_bytes());
        expect[16..32].copy_from_slice(&3u128.to_le_bytes());
        assert_eq!(pk.as_bytes(), &expect[..]);
    }

    #[test]
    fn compound_pk_zset_batch_new_uses_bytes_variant() {
        let schema = compound_schema_u64_u64();
        let batch = ZSetBatch::new(&schema);
        match &batch.pks {
            gnitz_core::PkColumn::Bytes { stride, buf } => {
                assert_eq!(*stride, 16);
                assert!(buf.is_empty());
            }
            other => panic!("expected PkColumn::Bytes, got {other:?}"),
        }
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
