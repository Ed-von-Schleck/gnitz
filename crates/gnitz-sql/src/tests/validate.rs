use super::*;
use crate::test_support::{parse_stmt, rejected};
use gnitz_wire::TypeCode;

/// A computed item is declared nullable, in the register image of what it
/// computes, under its alias or its position's name.
#[test]
fn a_computed_column_is_declared_in_its_register_image() {
    assert_eq!(
        computed_column(None, 3, TypeCode::String.into()).unwrap(),
        ColumnDef::new("_expr3", TypeCode::String, true)
    );
    assert_eq!(
        computed_column(Some("x".into()), 0, TypeCode::F32.into()).unwrap(),
        ColumnDef::new("x", TypeCode::F64, true)
    );
    assert_eq!(
        computed_column(None, 0, TypeCode::I16.into()).unwrap(),
        ColumnDef::new("_expr0", TypeCode::I64, true)
    );
    assert_eq!(
        rejected(computed_column(None, 0, ColType::decimal(19))),
        "DECIMAL scale 19 exceeds 18; CAST an operand to a narrower scale"
    );
}

/// One slot per named key, filled case-insensitively; any other key, a repeat
/// under any spelling, or any clause form but `WITH (key = value, …)` is refused.
#[test]
fn kv_options_fills_named_slots_and_refuses_anything_else() {
    let decode = |clause: &str| {
        let sql = format!("CREATE TABLE t (id BIGINT PRIMARY KEY) {clause}");
        let sqlparser::ast::Statement::CreateTable(c) = parse_stmt(&sql) else {
            panic!("not a CREATE TABLE: {sql}");
        };
        kv_options(&c.table_options, "X", ["a", "b"]).map(|slots| slots.map(|v| v.map(ToString::to_string)))
    };
    assert_eq!(decode("").unwrap(), [None, None]);
    assert_eq!(decode("WITH (B = 2)").unwrap(), [None, Some("2".into())]);
    assert_eq!(
        decode("WITH (b = 2, a = 1)").unwrap(),
        [Some("1".into()), Some("2".into())]
    );
    for (clause, needle) in [
        ("WITH (c = 1)", "unknown X option 'c'"),
        ("WITH (a = 1, A = 2)", "X option `a` is given more than once"),
        ("OPTIONS(a = 1)", "OPTIONS (…) is not supported"),
    ] {
        let m = rejected(decode(clause));
        assert!(m.contains(needle), "{clause}: {m}");
    }
}
