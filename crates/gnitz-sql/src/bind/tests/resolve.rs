use super::*;
use gnitz_core::{ColumnDef, TypeCode};

fn col(name: &str, tc: TypeCode) -> ColumnDef {
    ColumnDef::new(name, tc, false)
}

#[test]
fn test_find_unique_column_unique() {
    let cols = vec![col("a", TypeCode::U64), col("b", TypeCode::I64)];
    assert_eq!(find_unique_column(&cols, "a").unwrap(), Some(0));
    assert_eq!(find_unique_column(&cols, "B").unwrap(), Some(1)); // case-insensitive
}

#[test]
fn test_find_unique_column_absent() {
    let cols = vec![col("a", TypeCode::U64)];
    assert_eq!(find_unique_column(&cols, "missing").unwrap(), None);
}

#[test]
fn test_find_unique_column_duplicate_is_ambiguous() {
    // Two case-insensitively equal names (as a `SELECT *` join view produces).
    let cols = vec![col("Id", TypeCode::U64), col("ID", TypeCode::U64)];
    match find_unique_column(&cols, "id") {
        Err(GnitzSqlError::Bind(s)) => assert!(s.contains("ambiguous"), "got: {s}"),
        other => panic!("expected Bind(ambiguous), got {other:?}"),
    }
}
