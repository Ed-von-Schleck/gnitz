use super::*;
use crate::test_support::{assert_rejects, col, parse_query};
use gnitz_wire::TypeCode;
use sqlparser::ast::SetExpr;

/// The options of the single `*` item of `SELECT … FROM t`.
fn wildcard_item(sql: &str) -> WildcardAdditionalOptions {
    match *parse_query(sql).body {
        SetExpr::Select(s) => match s.projection.into_iter().next().unwrap() {
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
    // nothing in the relation, a modifier list contradicting itself, and a
    // qualified EXCLUDE name.
    let refused: &[(&str, &str)] = &[
        ("SELECT * REPLACE (a + 1 AS a) FROM t", "REPLACE"),
        ("SELECT * ILIKE 'a%' FROM t", "ILIKE"),
        ("SELECT * EXCEPT (nope) FROM t", "unknown column 'nope'"),
        ("SELECT * EXCEPT (_hid) FROM t", "unknown column '_hid'"),
        ("SELECT * EXCEPT (a) RENAME (a AS x) FROM t", "excluded column 'a'"),
        ("SELECT * RENAME (a AS x, a AS y) FROM t", "'a' twice"),
        ("SELECT * EXCLUDE (t.a) FROM t", "qualified SELECT * EXCLUDE"),
        ("SELECT * EXCLUDE t.a FROM t", "qualified SELECT * EXCLUDE"),
        ("SELECT * EXCLUDE (a, nosuch.b) FROM t", "qualified SELECT * EXCLUDE"),
    ];
    for (sql, want) in refused {
        assert_rejects(sql, expand_wildcard_item(&wildcard_item(sql), &cols, "SELECT"), want);
    }
}
