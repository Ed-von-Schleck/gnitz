use super::*;
use crate::test_support::parse_query;
use sqlparser::ast::Expr;

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
