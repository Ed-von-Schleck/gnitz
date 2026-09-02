use super::*;
use crate::test_support::parse_expr_sql;
use gnitz_core::TypeCode;

/// CEIL/FLOOR/CAST reach the binder as dedicated AST nodes rather than as
/// function calls, so [`expr_operands`] has to name their operand. Falling
/// into the wildcard would make each of these look aggregate-free and route
/// the query to the scalar path.
#[test]
fn expr_operands_reaches_through_the_dedicated_nodes() {
    for src in [
        "CAST(SUM(x) AS INT)",
        "SUM(x)::INT",
        "TRY_CAST(SUM(x) AS INT)",
        "CEIL(SUM(x))",
        "FLOOR(SUM(x))",
        "ABS(CAST(SUM(x) AS INT))",
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

/// The single `*` item of `SELECT … FROM t`.
fn wildcard_item(sql: &str) -> SelectItem {
    match *crate::test_support::parse_query(sql).body {
        sqlparser::ast::SetExpr::Select(s) => s.projection.into_iter().next().unwrap(),
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
    // `(sql, unsupported, substring)`: a modifier gnitz does not honor is
    // `Unsupported`; a name that resolves to nothing, or a contradictory
    // drop/rename, is `Bind`.
    let rejected: &[(&str, bool, &str)] = &[
        ("SELECT * REPLACE (a + 1 AS a) FROM t", true, "REPLACE"),
        ("SELECT * ILIKE 'a%' FROM t", true, "ILIKE"),
        ("SELECT * EXCEPT (nope) FROM t", false, "unknown column 'nope'"),
        ("SELECT * EXCEPT (_hid) FROM t", false, "unknown column '_hid'"),
        (
            "SELECT * EXCEPT (a) RENAME (a AS x) FROM t",
            false,
            "excluded column 'a'",
        ),
        ("SELECT * RENAME (a AS x, a AS y) FROM t", false, "'a' twice"),
    ];
    for (sql, unsupported, want) in rejected {
        let msg = match expand_wildcard_item(&wildcard_item(sql), &cols, "SELECT").unwrap_err() {
            GnitzSqlError::Unsupported(m) if *unsupported => m,
            GnitzSqlError::Bind(m) if !*unsupported => m,
            other => panic!("{sql}: unexpected {other:?}"),
        };
        assert!(msg.contains(want), "{sql}: {msg}");
    }
}
