use super::*;
use crate::test_support::{col, parse_expr_sql, rejected};
use gnitz_wire::TypeCode;

/// One row per operand position `bind_structural` recurses into, an aggregate
/// planted in it. A position [`expr_operands`] misses looks aggregate-free,
/// routing the query to the scalar path — where the binder then reaches the
/// aggregate it was told was not there.
#[test]
fn expr_operands_reaches_every_position_the_binder_recurses_into() {
    for src in [
        // The dedicated AST nodes — CEIL/FLOOR/CAST/EXTRACT/SUBSTRING/POSITION
        // are keyword-dispatched and never arrive as `Expr::Function`.
        "CAST(SUM(x) AS INT)",
        "EXTRACT(YEAR FROM MAX(d))",
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
        "TRIM(MIN(s))",
        "TRIM('x' FROM MIN(s))",
        // Both must be literals, and the binder says so only if the walk gets
        // there.
        "TRIM(MIN(s) FROM s)",
        "MIN(s) LIKE 'a%'",
        "s LIKE MIN(s)",
        "s ILIKE MIN(s)",
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
        "COUNT(*) OVER (PARTITION BY SUM(x) ORDER BY a)",
        "COUNT(*) OVER (PARTITION BY a ORDER BY SUM(x))",
    ] {
        let mut seen = 0usize;
        for_each_agg_call::<()>(&parse_expr_sql(src), &mut |_| {
            seen += 1;
            Ok(())
        })
        .unwrap();
        assert_eq!(seen, 1, "{src}: the aggregate must be collected exactly once");
    }
}

/// The walk reports whether the expression *is* an aggregate, parentheses aside,
/// or merely contains one.
#[test]
fn for_each_agg_call_tells_an_aggregate_from_a_wrapper_over_one() {
    for (src, is_aggregate) in [
        ("SUM(x)", true),
        ("((SUM(x)))", true),
        ("ABS(SUM(x))", false),
        ("SUM(x) OVER ()", false),
        ("x + 1", false),
    ] {
        let top = for_each_agg_call::<()>(&parse_expr_sql(src), &mut |_| Ok(())).unwrap();
        assert_eq!(top, is_aggregate, "{src}");
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
        col("id", TypeCode::I64),
        col("a", TypeCode::I64),
        col("b", TypeCode::I64),
        col("_hid", TypeCode::I64).hidden(),
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
    // `(sql, substring)`: a modifier gnitz does not honor, a name resolving to
    // nothing in the relation, and a modifier list contradicting itself.
    let refused: &[(&str, &str)] = &[
        ("SELECT * REPLACE (a + 1 AS a) FROM t", "REPLACE"),
        ("SELECT * ILIKE 'a%' FROM t", "ILIKE"),
        ("SELECT * EXCEPT (nope) FROM t", "unknown column 'nope'"),
        ("SELECT * EXCEPT (_hid) FROM t", "unknown column '_hid'"),
        ("SELECT * EXCEPT (a) RENAME (a AS x) FROM t", "excluded column 'a'"),
        ("SELECT * RENAME (a AS x, a AS y) FROM t", "'a' twice"),
    ];
    for (sql, want) in refused {
        let msg = rejected(expand_wildcard_item(&wildcard_item(sql), &cols, "SELECT"));
        assert!(msg.contains(want), "{sql}: {msg}");
    }
}

// ---------------------------------------------------------------------------
// The one literal / constant decoder
// ---------------------------------------------------------------------------

/// A numeric literal binds as the narrowest exact form its text has: an `i64`, a
/// wide magnitude up to `u128`, and past that — or with a fraction or exponent —
/// the float it reads as, beside the decimal it spells.
#[test]
fn a_numeric_literal_binds_as_its_narrowest_exact_form() {
    let wide = |mag| BExpr::LitWide(NumLit { mag, neg: false });
    for (src, want) in [
        ("42", BExpr::<usize>::LitInt(42)),
        ("9223372036854775807", BExpr::LitInt(i64::MAX)),
        ("9223372036854775808", wide(1 << 63)),
        ("18446744073709551615", wide(u64::MAX.into())),
        ("340282366920938463463374607431768211455", wide(u128::MAX)),
        (
            "340282366920938463463374607431768211456",
            BExpr::LitFloat { v: 3.402823669209385e38, dec: None },
        ),
        ("1.5", BExpr::LitFloat { v: 1.5, dec: Some((15, 1)) }),
        ("1e3", BExpr::LitFloat { v: 1000.0, dec: Some((1000, 0)) }),
    ] {
        let got = bind_literal(&Value::Number(src.into(), false)).unwrap();
        assert_eq!(got, want, "{src}");
    }
}
