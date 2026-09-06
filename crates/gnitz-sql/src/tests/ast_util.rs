use super::*;
use crate::error::GnitzSqlError;
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

// ---------------------------------------------------------------------------
// The one literal / constant decoder
// ---------------------------------------------------------------------------

fn num(n: &str) -> Result<crate::ir::BoundExpr, GnitzSqlError> {
    bind_literal(&Value::Number(n.into(), false))
}

#[test]
fn bind_literal_wide_int_to_litwide() {
    // u64::MAX overflows i64 and is non-fractional → bound faithfully as a
    // `LitWide` carrying the raw magnitude string (not coerced to f64, not an
    // error). The recognizer parses it byte-exactly; the un-servable case
    // rejects at the compile boundary.
    match num("18446744073709551615") {
        Ok(BExpr::LitWide(s)) => assert_eq!(s, "18446744073709551615"),
        other => panic!("expected LitWide, got {other:?}"),
    }
}

#[test]
fn bind_literal_accepts_fractional_and_exponent_floats() {
    assert!(matches!(num("1.5"), Ok(BExpr::LitFloat(_))));
    assert!(matches!(num("1e3"), Ok(BExpr::LitFloat(_))));
}

#[test]
fn bind_literal_accepts_in_range_integer() {
    assert!(matches!(num("42"), Ok(BExpr::LitInt(42))));
}

/// A constant position peels parentheses and one sign, in either order, and
/// hands the magnitude back separately from the sign.
#[test]
fn bind_constant_peels_parens_and_one_sign() {
    for (src, want_neg) in [
        ("5", false),
        ("+5", false),
        ("-5", true),
        ("((-5))", true),
        ("(-(5))", true),
    ] {
        let c = bind_constant(&parse_expr_sql(src)).unwrap_or_else(|e| panic!("{src}: {e}"));
        assert!(matches!(c.lit, BExpr::LitInt(5)), "{src}: {:?}", c.lit);
        assert_eq!(c.negated, want_neg, "{src}");
    }
}

/// A written sign implies a numeric literal or NULL — the invariant every
/// consumer rests on, enforced in the one constructor. Either sign over a
/// string used to be discarded silently.
#[test]
fn bind_constant_refuses_a_sign_on_a_string() {
    assert!(bind_constant(&parse_expr_sql("-'abc'")).is_err());
    assert!(bind_constant(&parse_expr_sql("+'abc'")).is_err());
    assert!(matches!(
        bind_constant(&parse_expr_sql("'abc'")).unwrap().lit,
        BExpr::LitStr(_)
    ));
    // A sign over NULL is the NULL it spells, which is what an INSERT cell reads.
    for src in ["+NULL", "-NULL", "NULL"] {
        assert!(
            matches!(bind_constant(&parse_expr_sql(src)).unwrap().lit, BExpr::LitNull),
            "{src}"
        );
    }
}

#[test]
fn bind_constant_rejects_a_non_constant() {
    for src in ["a", "1 + 1", "-(a)", "ABS(1)"] {
        assert!(bind_constant(&parse_expr_sql(src)).is_err(), "{src}");
    }
}

/// The sign travels beside the magnitude so `-0` survives; folding it in would
/// make the two indistinguishable.
#[test]
fn bind_constant_keeps_negative_zero_distinguishable() {
    let neg = bind_constant(&parse_expr_sql("-0")).unwrap();
    let pos = bind_constant(&parse_expr_sql("0")).unwrap();
    assert_eq!(neg.lit, pos.lit);
    assert!(neg.negated && !pos.negated);
    assert_eq!(neg.to_string(), "-0");
}

/// LIMIT/OFFSET read the same decoder, so `(10)` and `+10` are counts; a
/// negative or fractional one names itself in the message.
#[test]
fn expr_usize_literal_reads_the_constant_decoder() {
    for src in ["10", "+10", "(10)", "((+10))"] {
        assert_eq!(expr_usize_literal(&parse_expr_sql(src), "LIMIT").unwrap(), 10, "{src}");
    }
    for (src, want) in [("-1", "'-1'"), ("1.5", "'1.5'")] {
        let e = expr_usize_literal(&parse_expr_sql(src), "LIMIT").unwrap_err();
        assert!(e.to_string().contains(want), "{src}: {e}");
    }
    for src in ["'x'", "NULL", "a"] {
        let e = expr_usize_literal(&parse_expr_sql(src), "LIMIT").unwrap_err();
        assert!(e.to_string().contains("not an expression"), "{src}: {e}");
    }
}

/// ORDER BY / GROUP BY positions stay narrower than a constant on purpose:
/// `(1)` and `+1` are expressions over the output, not positions into it.
#[test]
fn clause_position_is_narrower_than_a_constant() {
    assert_eq!(clause_position(&parse_expr_sql("1"), "ORDER BY").unwrap(), Some(1));
    for src in ["(1)", "+1", "a"] {
        assert_eq!(
            clause_position(&parse_expr_sql(src), "ORDER BY").unwrap(),
            None,
            "{src}"
        );
    }
}
