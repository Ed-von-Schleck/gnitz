use super::*;
use crate::test_support::rejected;

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
