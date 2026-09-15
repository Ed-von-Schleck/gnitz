use super::*;
use crate::test_support::{col_def, parse_query};
use gnitz_core::TypeCode;
use sqlparser::ast::Expr;

/// The ORDER BY keys of `sql`.
fn keys_of(q: &sqlparser::ast::Query) -> Vec<OrderKey<'_>> {
    parse_order_by(q.order_by.as_ref()).unwrap()
}

/// A hidden leading key, then `a`, `b`, `c`.
fn hidden_led() -> Vec<ColumnDef> {
    vec![
        col_def("_group_pk", TypeCode::U128, false).hidden(),
        col_def("a", TypeCode::I64, false),
        col_def("b", TypeCode::I64, false),
        col_def("c", TypeCode::I64, false),
    ]
}

#[test]
fn a_position_names_a_visible_column() {
    let cols = hidden_led();
    let q = parse_query("SELECT * FROM t ORDER BY 1, 3");
    assert_eq!(key_slots(&keys_of(&q), &cols, []).unwrap(), vec![1, 3]);
    for sql in ["SELECT * FROM t ORDER BY 0", "SELECT * FROM t ORDER BY 4"] {
        let q = parse_query(sql);
        match key_slots(&keys_of(&q), &cols, []) {
            Err(GnitzSqlError::Unsupported(m)) => assert!(m.contains("ORDER BY position"), "{sql}: {m}"),
            other => panic!("{sql}: {other:?}"),
        }
    }
}

#[test]
fn expression_keys_consume_placements_in_order() {
    let cols = hidden_led();
    let q = parse_query("SELECT * FROM t ORDER BY a + 1, 2, b + 1 DESC");
    let keys = keys_of(&q);
    assert_eq!(order_exprs(&keys).len(), 2);
    assert_eq!(key_slots(&keys, &cols, [7, 5]).unwrap(), vec![7, 2, 5]);
    let wire = wire_keys(&keys, &cols, [7, 5]).unwrap();
    assert_eq!(
        wire.iter().map(|k| (k.col, k.desc)).collect::<Vec<_>>(),
        [(7, false), (2, false), (5, true)]
    );
    assert!(matches!(key_slots(&keys, &cols, [7]), Err(GnitzSqlError::Internal(_))));
}

#[test]
fn null_placement_defaults_follow_the_direction() {
    let q = parse_query("SELECT * FROM t ORDER BY a, b DESC, c ASC NULLS FIRST, a DESC NULLS LAST");
    let dirs: Vec<(bool, bool)> = keys_of(&q).iter().map(OrderKey::dir).collect();
    assert_eq!(dirs, [(false, false), (true, true), (false, true), (true, false)]);
}

#[test]
fn extract_limit_offset_literals_and_errors() {
    let q = parse_query("SELECT * FROM t LIMIT 3 OFFSET 2");
    assert_eq!(extract_limit(&q).unwrap(), Some(3));
    assert_eq!(extract_offset(&q).unwrap(), 2);
    // MySQL `LIMIT off, lim`.
    let q = parse_query("SELECT * FROM t LIMIT 2, 3");
    assert_eq!(extract_limit(&q).unwrap(), Some(3));
    assert_eq!(extract_offset(&q).unwrap(), 2);
    // Absent → None / 0.
    let q = parse_query("SELECT * FROM t");
    assert_eq!(extract_limit(&q).unwrap(), None);
    assert_eq!(extract_offset(&q).unwrap(), 0);
    // A non-integer-literal errors instead of silently degrading.
    for sql in [
        "SELECT * FROM t LIMIT 1+1",
        "SELECT * FROM t LIMIT 'x'",
        "SELECT * FROM t LIMIT -1",
    ] {
        assert!(
            matches!(extract_limit(&parse_query(sql)), Err(GnitzSqlError::Unsupported(_))),
            "{sql} must error"
        );
    }
    for sql in [
        "SELECT * FROM t LIMIT 1 OFFSET 1+1",
        "SELECT * FROM t LIMIT 1 OFFSET 'x'",
    ] {
        assert!(
            matches!(extract_offset(&parse_query(sql)), Err(GnitzSqlError::Unsupported(_))),
            "{sql} must error"
        );
    }
}

#[test]
fn resolve_order_by_rejects_clickhouse_duckdb_extensions() {
    // `ORDER BY ALL` / `WITH FILL` / `INTERPOLATE` only reach `resolve_order_by`
    // as their typed AST forms under a dialect that parses them (GenericDialect
    // parses `ALL` as an identifier), so drive the arms directly.
    use sqlparser::ast::{Ident, Interpolate, OrderByExpr, OrderByOptions, WithFill};
    let ident_key = || OrderByExpr {
        expr: Expr::Identifier(Ident::new("v")),
        options: OrderByOptions::default(),
        with_fill: None,
    };

    let all = OrderBy {
        kind: OrderByKind::All(OrderByOptions::default()),
        interpolate: None,
    };
    assert!(matches!(resolve_order_by(&all), Err(GnitzSqlError::Unsupported(_))));

    let interpolate = OrderBy {
        kind: OrderByKind::Expressions(vec![ident_key()]),
        interpolate: Some(Interpolate { exprs: None }),
    };
    assert!(matches!(
        resolve_order_by(&interpolate),
        Err(GnitzSqlError::Unsupported(_))
    ));

    let with_fill = OrderBy {
        kind: OrderByKind::Expressions(vec![OrderByExpr {
            expr: Expr::Identifier(Ident::new("v")),
            options: OrderByOptions::default(),
            with_fill: Some(WithFill { from: None, to: None, step: None }),
        }]),
        interpolate: None,
    };
    assert!(matches!(
        resolve_order_by(&with_fill),
        Err(GnitzSqlError::Unsupported(_))
    ));
}
