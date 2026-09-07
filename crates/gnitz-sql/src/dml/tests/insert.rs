use super::*;
use crate::test_support::{parse_expr_sql, two_col};
use gnitz_core::TypeCode;

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
fn serial_schema() -> Schema {
    Schema {
        columns: vec![
            gnitz_core::ColumnDef::new("id", TypeCode::U64, false).serial(),
            gnitz_core::ColumnDef::new("val", TypeCode::I64, true),
        ],
        pk_cols: vec![0],
    }
}

#[test]
fn insert_no_column_list_maps_every_visible_non_serial_column() {
    let shape = insert_row_shape(&[], &two_col(TypeCode::I64)).unwrap();
    assert_eq!(shape.slot_of, vec![Some(0), Some(1)]);
    assert_eq!(shape.expected, 2);
    assert_eq!(shape.serial_ci, None);
}

#[test]
fn insert_no_column_list_skips_the_serial_column() {
    let shape = insert_row_shape(&[], &serial_schema()).unwrap();
    assert_eq!(shape.slot_of, vec![None, Some(0)]);
    assert_eq!(shape.expected, 1);
    assert_eq!(shape.serial_ci, Some(0));
}

#[test]
fn insert_in_order_full_list_is_ok() {
    // Full set, schema order, case-insensitive → the identity map.
    let shape = insert_row_shape(&idents(&["pk", "VAL"]), &two_col(TypeCode::I64)).unwrap();
    assert_eq!(shape.slot_of, vec![Some(0), Some(1)]);
    assert_eq!(shape.expected, 2);
}

#[test]
fn insert_reordered_list_remaps_the_slots() {
    let shape = insert_row_shape(&idents(&["val", "pk"]), &two_col(TypeCode::I64)).unwrap();
    assert_eq!(shape.slot_of, vec![Some(1), Some(0)]);
    assert_eq!(shape.expected, 2);
}

/// A column the list omits takes no slot; the row loop writes it NULL.
#[test]
fn insert_partial_list_leaves_the_omitted_column_unmapped() {
    let shape = insert_row_shape(&idents(&["pk"]), &two_col(TypeCode::I64)).unwrap();
    assert_eq!(shape.slot_of, vec![Some(0), None]);
    assert_eq!(shape.expected, 1);
}

#[test]
fn insert_wrong_name_is_rejected() {
    let err = err_of(insert_row_shape(&idents(&["pk", "nope"]), &two_col(TypeCode::I64)));
    assert!(matches!(err, GnitzSqlError::Bind(_)), "got {err:?}");
}

#[test]
fn insert_duplicate_name_is_rejected() {
    let err = err_of(insert_row_shape(&idents(&["pk", "PK"]), &two_col(TypeCode::I64)));
    assert!(err.to_string().contains("more than once"), "got {err}");
}

/// PostgreSQL refuses `INSERT INTO t (t.a)`; a qualifier must not be dropped.
#[test]
fn insert_qualified_name_is_rejected() {
    let qualified = vec![ObjectName::from(vec![
        sqlparser::ast::Ident::new("t"),
        sqlparser::ast::Ident::new("pk"),
    ])];
    let err = err_of(insert_row_shape(&qualified, &two_col(TypeCode::I64)));
    assert!(matches!(err, GnitzSqlError::Plan(_)), "got {err:?}");
}

#[test]
fn insert_naming_the_serial_column_is_rejected() {
    let err = err_of(insert_row_shape(&idents(&["id", "val"]), &serial_schema()));
    assert!(err.to_string().contains("SERIAL"), "got {err}");
}

/// A column list that omits the PK is caught where the PK is planned, not by the
/// slot map: an omitted column is otherwise a legal NULL.
#[test]
fn insert_omitting_the_pk_is_rejected_by_the_pk_plan() {
    let schema = two_col(TypeCode::I64);
    let shape = insert_row_shape(&idents(&["val"]), &schema).unwrap();
    let err = PkPlan::written(&shape.slot_of, &schema)
        .err()
        .expect("an omitted PK must be rejected");
    assert!(err.to_string().contains("PK column 'pk' missing"), "got {err}");
}

/// An `EXCLUDED.col` reference anywhere the binder can reach — inside CASE,
/// BETWEEN, function arguments, IN lists — must be detected, so the compound
/// RHS is rejected rather than the qualifier being silently dropped and the
/// reference bound to the existing row's column.
#[test]
fn excluded_detected_in_every_operand_position() {
    for src in [
        "EXCLUDED.a",
        "val + EXCLUDED.a",
        "-EXCLUDED.a",
        "(EXCLUDED.a)",
        "COALESCE(EXCLUDED.a, 0)",
        "CASE WHEN EXCLUDED.a > 0 THEN 1 ELSE 0 END",
        "CASE val WHEN 1 THEN EXCLUDED.a END",
        "val BETWEEN EXCLUDED.a AND 10",
        "val IN (1, EXCLUDED.a)",
        "EXCLUDED.a IS NULL",
        "CAST(EXCLUDED.a AS BIGINT)",
        "EXCLUDED.a::BIGINT",
        "CEIL(EXCLUDED.a)",
        "FLOOR(EXCLUDED.a)",
        "ABS(EXCLUDED.a)",
        "GREATEST(val, EXCLUDED.a)",
        // Both of LIKE's bound sub-expressions, which the binder reaches.
        "EXCLUDED.s LIKE 'a%'",
        "s ILIKE EXCLUDED.s",
    ] {
        assert!(
            expr_contains_excluded(&parse_expr_sql(src)),
            "must detect EXCLUDED in {src}"
        );
    }
    for src in ["val + 1", "COALESCE(val, 0)", "t.a", "CASE WHEN val > 0 THEN 1 END"] {
        assert!(!expr_contains_excluded(&parse_expr_sql(src)), "false positive on {src}");
    }
}
