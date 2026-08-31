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

/// A SET value out of its column's range is rejected, not truncated to the
/// low bits. Each case used to write the wrapped byte pattern in the comment.
#[test]
fn set_value_out_of_range_is_rejected() {
    let cases: [(TypeCode, i128); 7] = [
        (TypeCode::U8, 300),     // wrapped to 44
        (TypeCode::U8, -1),      // wrapped to 255
        (TypeCode::I8, 128),     // wrapped to -128
        (TypeCode::U16, 70000),  // wrapped to 4464
        (TypeCode::I16, -32769), // wrapped to 32767
        (TypeCode::U32, -1),     // wrapped to 4294967295
        (TypeCode::U64, -1),     // wrapped to u64::MAX
    ];
    for (tc, v) in cases {
        let mut col = ColData::Fixed(Vec::new());
        let e = append_column_value(&mut col, ColumnValue::Int(v), tc).unwrap_err();
        assert!(
            format!("{e:?}").contains("out of range"),
            "{tc:?} value {v} must be rejected, got {e:?}"
        );
    }
}

/// In-range SET values encode to the column's native little-endian image at
/// every width and sign — including the upper half of U64, which no `i64`
/// spells and which the wrap of `-1` used to be the only route to.
#[test]
fn set_value_in_range_encodes_natively() {
    let cases: [(TypeCode, i128, Vec<u8>); 8] = [
        (TypeCode::U8, 255, vec![255u8]),
        (TypeCode::I8, -5, vec![(-5i8) as u8]),
        (TypeCode::U16, 65535, 65535u16.to_le_bytes().to_vec()),
        (TypeCode::I16, -2, (-2i16).to_le_bytes().to_vec()),
        (TypeCode::U32, 4294967295, 4294967295u32.to_le_bytes().to_vec()),
        (TypeCode::I32, -1, (-1i32).to_le_bytes().to_vec()),
        (TypeCode::U64, u64::MAX as i128, u64::MAX.to_le_bytes().to_vec()),
        (TypeCode::I64, i64::MIN as i128, i64::MIN.to_le_bytes().to_vec()),
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
