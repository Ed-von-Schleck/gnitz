use super::*;
use crate::test_support::{col, parse_expr_sql, rejected};
use gnitz_wire::TypeCode;

/// An aggregate is found wherever it is written outside a subquery's body — the
/// `IS` tests included — and a subquery's own aggregate is not this body's.
#[test]
fn expr_has_aggregate_searches_every_position_outside_a_subquery() {
    for (src, want) in [
        ("(SUM(x) > 0) IS TRUE", true),
        ("(SUM(x) > 0) IS NOT TRUE", true),
        ("(SUM(x) > 0) IS FALSE", true),
        ("(SUM(x) > 0) IS NOT FALSE", true),
        ("(SUM(x) > 0) IS UNKNOWN", true),
        ("(SUM(x) > 0) IS NOT UNKNOWN", true),
        ("COUNT(*) OVER (PARTITION BY a ORDER BY SUM(x))", true),
        ("SUM(x) OVER ()", false),
        ("(SELECT SUM(x) FROM t)", false),
        ("EXISTS (SELECT SUM(x) FROM t)", false),
        ("a IN (SELECT SUM(x) FROM t)", false),
        ("a > ALL (SELECT SUM(x) FROM t)", false),
        ("(SELECT MAX(x) FROM t) + SUM(y)", true),
        ("SUM(y) + (SELECT MAX(x) FROM t)", true),
    ] {
        assert_eq!(expr_has_aggregate(&parse_expr_sql(src)), want, "{src}");
    }
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
