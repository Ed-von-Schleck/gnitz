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

#[test]
fn insert_no_column_list_is_ok() {
    assert!(validate_insert_column_list(&[], &two_col(TypeCode::I64)).is_ok());
}

#[test]
fn insert_in_order_full_list_is_ok() {
    // Full set, schema order, case-insensitive → accepted.
    assert!(validate_insert_column_list(&idents(&["pk", "VAL"]), &two_col(TypeCode::I64)).is_ok());
}

#[test]
fn insert_reordered_list_is_rejected() {
    let err = validate_insert_column_list(&idents(&["val", "pk"]), &two_col(TypeCode::I64)).unwrap_err();
    assert!(matches!(err, GnitzSqlError::Unsupported(_)), "got {err:?}");
}

#[test]
fn insert_partial_list_is_rejected() {
    let err = validate_insert_column_list(&idents(&["pk"]), &two_col(TypeCode::I64)).unwrap_err();
    assert!(matches!(err, GnitzSqlError::Unsupported(_)), "got {err:?}");
}

#[test]
fn insert_wrong_name_is_rejected() {
    let err = validate_insert_column_list(&idents(&["pk", "nope"]), &two_col(TypeCode::I64)).unwrap_err();
    assert!(matches!(err, GnitzSqlError::Unsupported(_)), "got {err:?}");
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
