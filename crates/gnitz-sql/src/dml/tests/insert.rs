use super::*;
use crate::test_support::{
    col_def, compound_schema_u64_u64, extract_pk_value, neg_num_expr, num_expr, parse_expr_sql, pk_schema, two_col,
    uuid_schema_pk, uuid_str_expr,
};
use gnitz_core::TypeCode;
use gnitz_expr::SchemaFacts;

// `two_col` has columns ["pk", "val"] in schema order.
fn idents(names: &[&str]) -> Vec<ObjectName> {
    names
        .iter()
        .map(|n| ObjectName::from(sqlparser::ast::Ident::new(*n)))
        .collect()
}

fn err_of(r: Result<RowShape, GnitzSqlError>) -> GnitzSqlError {
    r.err().expect("must be rejected")
}

/// `(id SERIAL PK, val I64)` — the shape whose SERIAL column takes no user value.
/// Column 0 is the SERIAL column.
fn serial_schema() -> Schema {
    Schema {
        columns: vec![
            gnitz_core::ColumnDef::new("id", TypeCode::U64, false),
            gnitz_core::ColumnDef::new("val", TypeCode::I64, true),
        ],
        pk_cols: vec![0],
    }
}

#[test]
fn insert_no_column_list_maps_every_visible_non_serial_column() {
    let shape = insert_row_shape(&[], &two_col(TypeCode::I64), None).unwrap();
    assert_eq!(shape.slot_of, vec![Some(0), Some(1)]);
    assert_eq!(shape.expected, 2);
}

#[test]
fn insert_no_column_list_skips_the_serial_column() {
    let shape = insert_row_shape(&[], &serial_schema(), Some(0)).unwrap();
    assert_eq!(shape.slot_of, vec![None, Some(0)]);
    assert_eq!(shape.expected, 1);
}

#[test]
fn insert_in_order_full_list_is_ok() {
    // Full set, schema order, case-insensitive → the identity map.
    let shape = insert_row_shape(&idents(&["pk", "VAL"]), &two_col(TypeCode::I64), None).unwrap();
    assert_eq!(shape.slot_of, vec![Some(0), Some(1)]);
    assert_eq!(shape.expected, 2);
}

#[test]
fn insert_reordered_list_remaps_the_slots() {
    let shape = insert_row_shape(&idents(&["val", "pk"]), &two_col(TypeCode::I64), None).unwrap();
    assert_eq!(shape.slot_of, vec![Some(1), Some(0)]);
    assert_eq!(shape.expected, 2);
}

/// A column the list omits takes no slot; the row loop writes it NULL.
#[test]
fn insert_partial_list_leaves_the_omitted_column_unmapped() {
    let shape = insert_row_shape(&idents(&["pk"]), &two_col(TypeCode::I64), None).unwrap();
    assert_eq!(shape.slot_of, vec![Some(0), None]);
    assert_eq!(shape.expected, 1);
}

#[test]
fn insert_wrong_name_is_rejected() {
    let err = err_of(insert_row_shape(
        &idents(&["pk", "nope"]),
        &two_col(TypeCode::I64),
        None,
    ));
    assert!(matches!(err, GnitzSqlError::Bind(_)), "got {err:?}");
}

#[test]
fn insert_duplicate_name_is_rejected() {
    let err = err_of(insert_row_shape(&idents(&["pk", "PK"]), &two_col(TypeCode::I64), None));
    assert!(err.to_string().contains("more than once"), "got {err}");
}

/// PostgreSQL refuses `INSERT INTO t (t.a)`; a qualifier must not be dropped.
#[test]
fn insert_qualified_name_is_rejected() {
    let qualified = vec![ObjectName::from(vec![
        sqlparser::ast::Ident::new("t"),
        sqlparser::ast::Ident::new("pk"),
    ])];
    let err = err_of(insert_row_shape(&qualified, &two_col(TypeCode::I64), None));
    assert!(matches!(err, GnitzSqlError::Plan(_)), "got {err:?}");
}

#[test]
fn insert_naming_the_serial_column_is_rejected() {
    let err = err_of(insert_row_shape(&idents(&["id", "val"]), &serial_schema(), Some(0)));
    assert!(err.to_string().contains("SERIAL"), "got {err}");
}

/// A column list that omits the PK is caught where the PK is planned, not by the
/// slot map: an omitted column is otherwise a legal NULL.
#[test]
fn insert_omitting_the_pk_is_rejected_by_the_pk_plan() {
    let schema = two_col(TypeCode::I64);
    let shape = insert_row_shape(&idents(&["val"]), &schema, None).unwrap();
    let err = PkPlan::written(&shape.slot_of, &schema)
        .err()
        .expect("an omitted PK must be rejected");
    assert!(err.to_string().contains("PK column 'pk' missing"), "got {err}");
}

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
fn test_uuid_pk_string_literal_accepted() {
    let schema = uuid_schema_pk();
    let row = vec![uuid_str_expr("550e8400-e29b-41d4-a716-446655440000")];
    let pk = extract_pk_value(&row, &schema).unwrap();
    assert_eq!(pk, schema.opk_key_cols(&[0x550e8400_e29b_41d4_a716_446655440000_u128]));
}

#[test]
fn compound_pk_extract_pk_value_packs_opk_bytes() {
    let schema = compound_schema_u64_u64();
    let row = vec![num_expr("1"), num_expr("2"), num_expr("99")];
    let pk = extract_pk_value(&row, &schema).unwrap();
    assert_eq!(pk.width(), 16);
    let mut expect = [0u8; 16];
    expect[0..8].copy_from_slice(&1u64.to_be_bytes());
    expect[8..16].copy_from_slice(&2u64.to_be_bytes());
    assert_eq!(pk.pk_bytes(), &expect[..]);
}

#[test]
fn compound_pk_extract_pk_value_wide_region() {
    let schema = compound_schema_u64_u64_u128();
    let row = vec![num_expr("1"), num_expr("2"), num_expr("3"), num_expr("99")];
    let pk = extract_pk_value(&row, &schema).unwrap();
    // pk_stride = 8 + 8 + 16 = 32 → wide-region path.
    assert_eq!(pk.width(), 32);
    let mut expect = [0u8; 32];
    expect[0..8].copy_from_slice(&1u64.to_be_bytes());
    expect[8..16].copy_from_slice(&2u64.to_be_bytes());
    expect[16..32].copy_from_slice(&3u128.to_be_bytes());
    assert_eq!(pk.pk_bytes(), &expect[..]);
}

#[test]
fn extract_pk_value_u64_rejects_negative() {
    let schema = pk_schema(TypeCode::U64);
    let row = vec![neg_num_expr("1"), num_expr("0")];
    let err = extract_pk_value(&row, &schema).expect_err("U64 PK must reject negative literal");
    let m = err.to_string();
    assert!(m.contains("out of range") && m.contains("'id'"), "error: {m}");
}

#[test]
fn extract_pk_value_u128_rejects_negative() {
    let schema = pk_schema(TypeCode::U128);
    let row = vec![neg_num_expr("1"), num_expr("0")];
    assert!(extract_pk_value(&row, &schema).is_err());
}

/// A UUID PK takes a *valid* UUID string; an invalid one is named with the
/// column.
#[test]
fn an_invalid_uuid_pk_string_names_the_column() {
    let schema = uuid_schema_pk();
    let row = vec![uuid_str_expr("not-a-uuid")];
    let err = extract_pk_value(&row, &schema).expect_err("an invalid UUID PK literal must be rejected");
    let m = err.to_string();
    assert!(m.contains("invalid UUID") && m.contains("'id'"), "error: {m}");
}

/// A negated zero names the same value `0` does, so an unsigned column takes it.
#[test]
fn negative_zero_is_accepted_by_an_unsigned_wide_pk() {
    for tc in [TypeCode::U128, TypeCode::UUID] {
        let schema = pk_schema(tc);
        let row = vec![neg_num_expr("0"), num_expr("0")];
        let pk = extract_pk_value(&row, &schema).unwrap_or_else(|e| panic!("{tc:?}: {e}"));
        assert_eq!(pk, schema.opk_key_cols(&[0]), "{tc:?}");
    }
}

/// A NULL PK cell is the column's NOT NULL violation.
#[test]
fn a_null_pk_cell_violates_not_null() {
    let schema = pk_schema(TypeCode::I64);
    let err = extract_pk_value(&[parse_expr_sql("NULL"), num_expr("0")], &schema).unwrap_err();
    assert!(err.to_string().contains("violates NOT NULL"), "error: {err}");
}

/// A statement whose last SERIAL id passes the column type's maximum is refused
/// before any row is pushed; one whose last id is the maximum is not.
#[test]
fn a_serial_id_past_the_type_maximum_is_exhausted() {
    let max = i16::MAX as u64;
    let schema = pk_schema(TypeCode::I16);
    let plan = PkPlan::serial(max - 1, 2, TypeCode::I16).unwrap();
    let mut dst = PkColumn::empty_for_schema(&schema);
    plan.push(&schema, 1, &[], &mut dst).unwrap();
    assert_eq!(dst.get_tuple(0), schema.opk_key_cols(&[max as u128]));
    for (base, n, next) in [(max, 2, max + 1), (max + 5, 1, max + 5), (u64::MAX, 2, u64::MAX)] {
        match PkPlan::serial(base, n, TypeCode::I16) {
            Err(GnitzSqlError::Bind(m)) => {
                assert!(m.contains(&format!("exhausted: next value {next} ")), "{m}")
            }
            _ => panic!("expected exhaustion for base {base}, {n} rows"),
        }
    }
}
