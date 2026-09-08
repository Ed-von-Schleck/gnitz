use super::*;
use crate::ir::{BoundExpr, StrFunc};
use gnitz_core::{ColumnDef, Schema, TypeCode};

#[test]
fn validate_user_name_rejects_reserved_and_malformed() {
    // Leading `_` is reserved (system prefix + synthesized `__h…` views).
    assert!(matches!(validate_user_name("_hidden"), Err(GnitzSqlError::Plan(_))));
    assert!(matches!(validate_user_name("_seg4096"), Err(GnitzSqlError::Plan(_))));
    // Empty and illegal characters.
    assert!(matches!(validate_user_name(""), Err(GnitzSqlError::Plan(_))));
    assert!(matches!(validate_user_name("bad-name"), Err(GnitzSqlError::Plan(_))));
    assert!(matches!(validate_user_name("a.b"), Err(GnitzSqlError::Plan(_))));
    // Ordinary names — including an internal `_` — are accepted.
    assert!(validate_user_name("orders").is_ok());
    assert!(validate_user_name("my_view2").is_ok());
}

/// A computed STRING column must be *declared* STRING. The register image
/// maps STRING to I64, which was right while every computed value was an
/// 8-byte register.
#[test]
fn a_computed_string_projection_declares_a_string_column() {
    let schema = Schema {
        columns: vec![
            ColumnDef::new("pk", TypeCode::U64, true),
            ColumnDef::new("s", TypeCode::String, true),
        ],
        pk_cols: vec![0],
    };
    let e = BoundExpr::StrCall {
        f: StrFunc::Upper,
        args: vec![BoundExpr::ColRef(1)],
    };
    let nominal = e.infer_ty(&schema.columns);
    assert_eq!(nominal.tc, TypeCode::String);
    let def = computed_column(None, 0, nominal);
    assert_eq!(def.type_code, TypeCode::String);
    assert!(def.is_nullable);
    // A numeric expression still takes its register image.
    assert_eq!(computed_column(None, 0, TypeCode::F32.into()).type_code, TypeCode::F64);
}
