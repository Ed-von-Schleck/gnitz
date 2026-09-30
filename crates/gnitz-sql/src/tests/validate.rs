use super::*;
use crate::ir::{BoundExpr, StrFunc};
use gnitz_core::Schema;
use gnitz_wire::{ColumnDef, TypeCode};

#[test]
fn validate_user_name_rejects_reserved_and_malformed() {
    // Leading `_` is reserved (system prefix + synthesized `_seg…` segments).
    assert!(matches!(validate_user_name("_hidden"), Err(GnitzSqlError::Rejected(_))));
    assert!(matches!(
        validate_user_name("_seg4096"),
        Err(GnitzSqlError::Rejected(_))
    ));
    // Empty and illegal characters.
    assert!(matches!(validate_user_name(""), Err(GnitzSqlError::Rejected(_))));
    assert!(matches!(
        validate_user_name("bad-name"),
        Err(GnitzSqlError::Rejected(_))
    ));
    assert!(matches!(validate_user_name("a.b"), Err(GnitzSqlError::Rejected(_))));
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
    let def = computed_column(None, 0, nominal).unwrap();
    assert_eq!(def.ty.tc, TypeCode::String);
    assert!(def.is_nullable);
    // A numeric expression still takes its register image.
    assert_eq!(
        computed_column(None, 0, TypeCode::F32.into()).unwrap().ty.tc,
        TypeCode::F64
    );
}

/// A synthesized key list is capped by the PK-list width, whatever assembled it
/// — a join's reindex slots or a join output's pair PK. A zero-slot list is the
/// keyless join, which this rule has nothing to say about.
#[test]
fn pk_list_arity_bounds() {
    reject_pk_list_arity("join key list", 0).unwrap();
    reject_pk_list_arity("join key list", gnitz_wire::PK_LIST_MAX_COLS).unwrap();
    let over = reject_pk_list_arity("range JOIN output PK", gnitz_wire::PK_LIST_MAX_COLS + 1).unwrap_err();
    let GnitzSqlError::Rejected(msg) = over else {
        panic!("expected Unsupported, got {over:?}");
    };
    assert!(msg.contains("range JOIN output PK"), "{msg}");
}

/// One slot per named key, filled case-insensitively; any other key, a repeat
/// under any spelling, or any clause form but `WITH (key = value, …)` is refused.
#[test]
fn kv_options_fills_named_slots_and_refuses_anything_else() {
    let decode = |clause: &str| {
        let sql = format!("CREATE TABLE t (id BIGINT PRIMARY KEY) {clause}");
        let sqlparser::ast::Statement::CreateTable(c) = crate::test_support::parse_stmt(&sql) else {
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
        (
            "WITH (c = 1)",
            "unknown X option 'c'; the supported options are `a`, `b`",
        ),
        ("WITH (a = 1, A = 2)", "X option `a` is given more than once"),
        ("OPTIONS(a = 1)", "OPTIONS (…) is not supported"),
    ] {
        match decode(clause) {
            Err(GnitzSqlError::Rejected(m)) => assert!(m.contains(needle), "{clause}: {m}"),
            other => panic!("{clause}: {other:?}"),
        }
    }
}
