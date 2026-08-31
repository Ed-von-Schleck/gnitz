use super::*;
use crate::test_support::{
    col_def, compound_schema_u64_u64, dquote_expr, neg_num_expr, num_expr, pk_schema, uuid_schema_pk, uuid_str_expr,
};

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
    assert_eq!(pk.split_wire().0, 0x550e8400_e29b_41d4_a716_446655440000_u128);
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
