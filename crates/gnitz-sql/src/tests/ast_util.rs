use super::*;
use crate::test_support::parse_expr_sql;
use gnitz_core::TypeCode;

/// One row per operand position `bind_structural` recurses into. A position
/// [`expr_operands`] misses looks aggregate-free, routing the query to the scalar
/// path — where the binder then reaches the aggregate it was told was not there.
///
/// LIKE's `pattern` and TRIM's `trim_what` are absent on purpose: both require a
/// compile-time literal, so an aggregate there is rejected either way.
#[test]
fn expr_operands_reaches_every_position_the_binder_recurses_into() {
    for src in [
        // The dedicated AST nodes — CEIL/FLOOR/CAST/SUBSTRING/POSITION are
        // keyword-dispatched and never arrive as `Expr::Function`.
        "CAST(SUM(x) AS INT)",
        "SUM(x)::INT",
        "TRY_CAST(SUM(x) AS INT)",
        "CEIL(SUM(x))",
        "FLOOR(SUM(x))",
        "ABS(CAST(SUM(x) AS INT))",
        "SUBSTRING(MIN(s) FROM 1 FOR 2)",
        "SUBSTRING(s FROM SUM(x) FOR 2)",
        "SUBSTRING(s FROM 1 FOR SUM(x))",
        "POSITION(MIN(s) IN s)",
        "POSITION('a' IN MIN(s))",
        // Operators and parentheses.
        "SUM(x) + 1",
        "1 + SUM(x)",
        "-SUM(x)",
        "(SUM(x))",
        "SUM(x) IS NULL",
        "SUM(x) IS NOT NULL",
        "SUM(x) BETWEEN 1 AND 2",
        "1 BETWEEN SUM(x) AND 2",
        "1 BETWEEN 2 AND SUM(x)",
        "SUM(x) IS DISTINCT FROM 1",
        "1 IS NOT DISTINCT FROM SUM(x)",
        "SUM(x) IN (1, 2)",
        "1 IN (2, SUM(x))",
        // CASE, in each of its four operand positions.
        "CASE SUM(x) WHEN 1 THEN 2 ELSE 3 END",
        "CASE WHEN SUM(x) > 1 THEN 2 ELSE 3 END",
        "CASE WHEN a > 1 THEN SUM(x) ELSE 3 END",
        "CASE WHEN a > 1 THEN 2 ELSE SUM(x) END",
        // A call's arguments, and an inline window specification's keys — a
        // windowed call is not itself an aggregate, so only the planted one counts.
        "ABS(SUM(x))",
        "COUNT(*) OVER (PARTITION BY SUM(x))",
        "COUNT(*) OVER (ORDER BY SUM(x))",
    ] {
        assert!(expr_has_aggregate(&parse_expr_sql(src)), "{src}");
        let mut seen = 0usize;
        for_each_agg_call::<()>(&parse_expr_sql(src), &mut |_| {
            seen += 1;
            Ok(())
        })
        .unwrap();
        assert_eq!(seen, 1, "{src}: the aggregate must be collected exactly once");
    }
}

/// Same walker, the subquery consumers: a subquery under a CAST must still
/// route the view to the subquery-bearing builder.
#[test]
fn expr_operands_exposes_a_subquery_under_a_cast() {
    assert!(expr_any(
        &parse_expr_sql("CAST((SELECT 1) AS INT)"),
        &is_scalar_subquery
    ));
    assert!(expr_any(
        &parse_expr_sql("CAST(x AS INT) IN (SELECT y FROM t)"),
        &is_exists_in
    ));
}

/// The options of the single `*` item of `SELECT … FROM t`.
fn wildcard_item(sql: &str) -> WildcardAdditionalOptions {
    match *crate::test_support::parse_query(sql).body {
        sqlparser::ast::SetExpr::Select(s) => match s.projection.into_iter().next().unwrap() {
            SelectItem::Wildcard(o) => o,
            other => panic!("not a bare wildcard: {other}"),
        },
        other => panic!("not a SELECT: {other}"),
    }
}

/// A wildcard expands the visible columns in source order; `EXCEPT`/`EXCLUDE`
/// drop by name, `RENAME` relabels, both case-insensitively; a hidden column is
/// neither expanded nor addressable.
#[test]
fn a_wildcard_expands_through_its_modifiers() {
    let cols = vec![
        ColumnDef::new("id", TypeCode::I64, false),
        ColumnDef::new("a", TypeCode::I64, false),
        ColumnDef::new("b", TypeCode::I64, false),
        ColumnDef::new("_hid", TypeCode::I64, false).hidden(),
    ];
    let rows: &[(&str, &[(usize, &str)])] = &[
        ("SELECT * FROM t", &[(0, "id"), (1, "a"), (2, "b")]),
        ("SELECT * EXCEPT (a) FROM t", &[(0, "id"), (2, "b")]),
        ("SELECT * EXCLUDE (a) FROM t", &[(0, "id"), (2, "b")]),
        ("SELECT * EXCLUDE (a, b) FROM t", &[(0, "id")]),
        ("SELECT * EXCEPT (ID) FROM t", &[(1, "a"), (2, "b")]),
        ("SELECT * RENAME (a AS x) FROM t", &[(0, "id"), (1, "x"), (2, "b")]),
        ("SELECT * EXCEPT (b) RENAME (A AS X) FROM t", &[(0, "id"), (1, "X")]),
    ];
    for (sql, want) in rows {
        let got: Vec<(usize, String)> = expand_wildcard_item(&wildcard_item(sql), &cols, "SELECT")
            .unwrap()
            .into_iter()
            .map(|(i, c)| (i, c.name))
            .collect();
        let want: Vec<(usize, String)> = want.iter().map(|&(i, n)| (i, n.to_string())).collect();
        assert_eq!(got, want, "{sql}");
    }
    // `(sql, variant, substring)`: a modifier gnitz does not honor is
    // `Unsupported`; a name resolving to nothing in the relation is `Bind`; a
    // modifier list contradicting itself is `Plan`.
    let rejected: &[(&str, &str, &str)] = &[
        ("SELECT * REPLACE (a + 1 AS a) FROM t", "Unsupported", "REPLACE"),
        ("SELECT * ILIKE 'a%' FROM t", "Unsupported", "ILIKE"),
        ("SELECT * EXCEPT (nope) FROM t", "Bind", "unknown column 'nope'"),
        ("SELECT * EXCEPT (_hid) FROM t", "Bind", "unknown column '_hid'"),
        (
            "SELECT * EXCEPT (a) RENAME (a AS x) FROM t",
            "Plan",
            "excluded column 'a'",
        ),
        ("SELECT * RENAME (a AS x, a AS y) FROM t", "Plan", "'a' twice"),
    ];
    for (sql, variant, want) in rejected {
        let (got, msg) = match expand_wildcard_item(&wildcard_item(sql), &cols, "SELECT").unwrap_err() {
            GnitzSqlError::Unsupported(m) => ("Unsupported", m),
            GnitzSqlError::Bind(m) => ("Bind", m),
            GnitzSqlError::Plan(m) => ("Plan", m),
            other => panic!("{sql}: unexpected {other:?}"),
        };
        assert_eq!(got, *variant, "{sql}: {msg}");
        assert!(msg.contains(want), "{sql}: {msg}");
    }
}
