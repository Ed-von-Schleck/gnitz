use super::*;
use crate::ast_util::bind_constant;
use crate::test_support::{num_expr, uuid_schema_payload, uuid_str_expr};
use sqlparser::ast::Expr;

/// The lone UUID cell of a column region, from its 16 LE bytes.
fn uuid_cell(col: &[u8]) -> u128 {
    u128::from_le_bytes(col[..16].try_into().unwrap())
}

/// Encode one written constant into a fresh column region, the way INSERT does.
fn encoded(tc: TypeCode, e: &Expr) -> Result<Vec<u8>, GnitzSqlError> {
    let mut col = Vec::new();
    append_value_to_col(&mut col, &mut Vec::new(), ColType::of(tc), &bind_constant(e)?)?;
    Ok(col)
}

/// `src` as an INSERT row carries it — signs and parentheses included.
fn expr(src: &str) -> Expr {
    crate::test_support::parse_expr_sql(src)
}

#[test]
fn a_null_cell_is_a_zeroed_cell_of_the_type_stride() {
    // (wire type, expected NULL encoding) per column kind — a zeroed cell of the
    // type's own stride, German-string columns included.
    let cases: [(TypeCode, Vec<u8>); 4] = [
        (TypeCode::U32, vec![0u8; 4]),
        (TypeCode::String, vec![0u8; 16]),
        (TypeCode::Blob, vec![0u8; 16]),
        (TypeCode::UUID, vec![0u8; 16]),
    ];
    for (tc, expected) in cases {
        let mut blob = Vec::new();
        let mut col = Vec::new();
        append_value_to_col(
            &mut col,
            &mut blob,
            ColType::of(tc),
            &bind_constant(&expr("NULL")).unwrap(),
        )
        .unwrap();
        assert_eq!(col, expected, "NULL encoding for {tc:?}");
        assert!(blob.is_empty(), "a NULL cell spills nothing for {tc:?}");
    }
}

/// INSERT integer literals encode byte-identically to the native
/// `parse::<uN>()` + `to_le_bytes` ladder across widths and signs, and
/// out-of-range / wrong-sign literals are rejected by the `FixedInt::range`
/// check.
#[test]
fn insert_int_encoding_matches_native_and_range_checks() {
    // Byte-identical to the native casts at every width / sign / type edge.
    assert_eq!(encoded(TypeCode::U8, &expr("255")).unwrap(), vec![255u8]);
    assert_eq!(encoded(TypeCode::I8, &expr("-5")).unwrap(), vec![(-5i8) as u8]);
    assert_eq!(encoded(TypeCode::U16, &expr("65535")).unwrap(), 65535u16.to_le_bytes());
    assert_eq!(encoded(TypeCode::I16, &expr("-2")).unwrap(), (-2i16).to_le_bytes());
    assert_eq!(encoded(TypeCode::I32, &expr("-1")).unwrap(), (-1i32).to_le_bytes());
    assert_eq!(
        encoded(TypeCode::U64, &expr("18446744073709551615")).unwrap(),
        u64::MAX.to_le_bytes()
    );
    assert_eq!(
        encoded(TypeCode::I64, &expr("-9223372036854775808")).unwrap(),
        i64::MIN.to_le_bytes()
    );

    // Out-of-range and wrong-sign literals are rejected.
    assert!(encoded(TypeCode::U8, &expr("256")).is_err());
    assert!(encoded(TypeCode::I8, &expr("128")).is_err());
    assert!(encoded(TypeCode::U8, &expr("-1")).is_err());
    assert!(encoded(TypeCode::I32, &expr("3000000000")).is_err());
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
    let c = bind_constant(&uuid_str_expr("550e8400-e29b-41d4-a716-446655440000")).unwrap();
    let gnitz_core::ZSetBatch { payload, blob, .. } = &mut batch;
    append_value_to_col(&mut payload[0].bytes, blob, ColType::of(TypeCode::UUID), &c).unwrap();
    assert_eq!(
        uuid_cell(&batch.payload[0].bytes),
        0x550e8400_e29b_41d4_a716_446655440000_u128
    );
}

#[test]
fn test_uuid_decimal_literal_still_accepted() {
    let big_val: u128 = 0x550e8400_e29b_41d4_a716_446655440000_u128;
    assert_eq!(
        uuid_cell(&encoded(TypeCode::UUID, &num_expr(&big_val.to_string())).unwrap()),
        big_val
    );
}

/// A UUID column takes a *valid* UUID string; an invalid one is named, not
/// silently declined the way the seek recognizers decline it.
#[test]
fn a_uuid_column_names_an_invalid_uuid_string() {
    let e = encoded(TypeCode::UUID, &uuid_str_expr("not-a-uuid")).unwrap_err();
    assert!(format!("{e:?}").contains("UUID"), "got {e:?}");
}

/// A leading `+` reaches the payload slot the same way it reaches the PK slot:
/// one written constant decoder serves both, so an INSERT row cannot take `+1`
/// for its key and refuse `+2` for its payload.
#[test]
fn a_unary_plus_literal_is_accepted_like_the_pk_slot_accepts_it() {
    assert_eq!(encoded(TypeCode::U32, &expr("+2")).unwrap(), 2u32.to_le_bytes());
    // Parentheses peel too, in either order.
    assert_eq!(encoded(TypeCode::U32, &expr("(2)")).unwrap(), 2u32.to_le_bytes());
    assert_eq!(encoded(TypeCode::U32, &expr("(+2)")).unwrap(), 2u32.to_le_bytes());
}

/// Every numeric spelling reaches a float column, not only a written fraction:
/// a bare integer, a magnitude past `i128` (which no integer parse would hold),
/// and a wide negative.
#[test]
fn a_float_column_takes_every_numeric_spelling() {
    fn f64_of(col: &[u8]) -> f64 {
        f64::from_le_bytes(col[..8].try_into().unwrap())
    }
    fn f32_of(col: &[u8]) -> f32 {
        f32::from_le_bytes(col[..4].try_into().unwrap())
    }

    assert_eq!(f64_of(&encoded(TypeCode::F64, &expr("5")).unwrap()), 5.0);
    assert_eq!(f32_of(&encoded(TypeCode::F32, &expr("5")).unwrap()), 5.0f32);
    assert_eq!(
        f64_of(&encoded(TypeCode::F64, &expr("-18446744073709551616")).unwrap()),
        -18446744073709551616.0
    );
    assert_eq!(
        f32_of(&encoded(TypeCode::F32, &expr("-18446744073709551616")).unwrap()),
        -18446744073709551616.0f32
    );
    // Above `i128::MAX`: `NumLit::to_i128` would decline this, a DOUBLE holds it.
    assert_eq!(
        f64_of(&encoded(TypeCode::F64, &expr("340282366920938463463374607431768211455")).unwrap()),
        340282366920938463463374607431768211455.0
    );
    // An F32 literal rounds through binary64, matching `ZSetBatch::append`.
    assert_eq!(f32_of(&encoded(TypeCode::F32, &expr("0.1")).unwrap()), 0.1f64 as f32);
}

/// `-0` into a float column keeps its sign — which is why the sign travels
/// beside the magnitude instead of being folded into it.
#[test]
fn negative_zero_into_a_float_column_keeps_its_sign() {
    for (tc, bits) in [
        (TypeCode::F64, (-0.0f64).to_le_bytes().to_vec()),
        (TypeCode::F32, (-0.0f32).to_le_bytes().to_vec()),
    ] {
        assert_eq!(encoded(tc, &expr("-0")).unwrap(), bits, "{tc:?}");
    }
}

/// The two kind rejections: a number into a String column, and a string into a
/// column that is neither String nor UUID — a BLOB included, whose values are
/// bytes, not text.
#[test]
fn a_literal_of_the_wrong_kind_is_rejected_by_the_column_type() {
    let e = encoded(TypeCode::String, &expr("5")).unwrap_err();
    assert!(
        format!("{e:?}").contains("number literal for string column"),
        "got {e:?}"
    );
    for tc in [TypeCode::U32, TypeCode::F64, TypeCode::Blob] {
        let e = encoded(tc, &expr("'5'")).unwrap_err();
        assert!(
            format!("{e:?}").contains("string literal for non-string column"),
            "{tc:?}: got {e:?}"
        );
    }
}

/// A non-integral literal into an integer column names the literal it refused.
#[test]
fn a_float_literal_into_an_integer_column_names_the_literal() {
    let e = encoded(TypeCode::I64, &expr("1.5")).unwrap_err();
    assert!(format!("{e:?}").contains("1.5 is not a"), "got {e:?}");
}

/// A sign on a string is not a value: either sign used to write `abc`. Refused
/// by the decoder, so no column type can be reached with one.
#[test]
fn a_signed_string_literal_is_rejected() {
    for src in ["-'abc'", "+'abc'"] {
        let e = bind_constant(&expr(src)).unwrap_err();
        assert!(format!("{e:?}").contains("sign"), "{src}: got {e:?}");
    }
}
