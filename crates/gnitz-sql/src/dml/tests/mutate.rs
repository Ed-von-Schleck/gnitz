use super::*;
use crate::dml::plan_write;
use crate::test_support::{TestCatalog, assert_rejects, catalog, col, ncol, parse_stmt, rel, table};
use gnitz_wire::{ReadBound, RelClass, TypeCode};

fn plan(cat: &dyn Catalog, sql: &str) -> Result<WritePlan, GnitzSqlError> {
    plan_write(&parse_stmt(sql), cat, false)
}

/// | name | shape |
/// |---|---|
/// | `t` | `(id PK, v NOT NULL, s TEXT)` |
/// | `st` | a stream `(id PK, v)` |
/// | `vw` | a view `(id PK, v)` |
/// | `sys` | a system table `(id PK, v)` |
fn cat() -> TestCatalog {
    let i = TypeCode::I64;
    let idv = || vec![col("id", i), ncol("v", i)];
    catalog(vec![
        (
            "t",
            table(1, vec![col("id", i), col("v", i), ncol("s", TypeCode::String)], vec![0]),
        ),
        ("st", rel(2, RelClass::Stream, idv(), vec![0], vec![])),
        ("vw", rel(3, RelClass::View, idv(), vec![0], vec![])),
        ("sys", rel(4, RelClass::Catalog, idv(), vec![0], vec![])),
    ])
}

/// A clause gnitz does not honour is named rather than dropped, as is a target
/// that holds no row to mutate — all before any row is read.
#[test]
fn a_refused_mutation_names_its_rule() {
    let cat = cat();
    for (sql, needle) in [
        ("UPDATE t SET v = 9 WHERE id = 1 RETURNING id", "RETURNING"),
        ("UPDATE t SET v = 1 LIMIT 1", "LIMIT"),
        ("UPDATE OR REPLACE t SET v = 1", "OR (conflict clause)"),
        ("DELETE FROM t WHERE id = 1 RETURNING id", "RETURNING"),
        ("DELETE FROM t LIMIT 1", "LIMIT"),
        ("DELETE FROM t ORDER BY id", "ORDER BY"),
        // The join forms are refused before any name resolves, so `other` need
        // not exist.
        ("UPDATE t SET v = o.v FROM other o WHERE t.id = o.id", "join-update"),
        ("DELETE FROM t USING other o WHERE t.id = o.id", "join-delete"),
        (
            "UPDATE t JOIN other o ON t.id = o.id SET v = 1",
            "exactly one simple FROM",
        ),
        // A written alias displaces the table name, and a qualifier naming
        // anything else is not silently ignored.
        ("UPDATE t AS x SET v = 1 WHERE t.v = 1", "not found"),
        ("DELETE FROM t WHERE nope.v = 1", "not found"),
        // Only a DDL writes the catalog, a view is read-only, and a stream holds
        // no row.
        (
            "UPDATE sys SET v = 9 WHERE id = 1",
            "'sys' is a system table; UPDATE requires a base table",
        ),
        (
            "DELETE FROM sys WHERE id = 1",
            "'sys' is a system table; DELETE requires a base table",
        ),
        ("UPDATE vw SET v = 1 WHERE id = 1", "is a view"),
        ("DELETE FROM st WHERE id = 1", "is a stream"),
    ] {
        assert_rejects(sql, plan(&cat, sql), needle);
    }
}

/// UPDATE and DELETE read what their WHERE selects, qualified by the table's name
/// or its written alias: a pinned key as that key, any other predicate as a
/// filtered scan, and no WHERE as every row.
#[test]
fn a_mutation_reads_what_its_where_selects() {
    let cat = cat();
    for (head, q) in [
        ("DELETE FROM t", "t"),
        ("DELETE FROM t AS x", "x"),
        ("UPDATE t SET v = 0", "t"),
        ("UPDATE t AS x SET v = 0", "x"),
    ] {
        for (pred, key, filtered) in [
            (String::new(), None, false),
            (format!(" WHERE {q}.id = 1"), Some(1), false),
            (format!(" WHERE {q}.v = 5"), None, true),
        ] {
            let sql = format!("{head}{pred}");
            let plan = plan(&cat, &sql).unwrap_or_else(|e| panic!("`{sql}`: {e:?}"));
            let (Write::Update { bound, predicate, .. } | Write::Delete { bound, predicate }) = plan.write else {
                panic!("`{sql}` is neither an UPDATE nor a DELETE plan")
            };
            let want = key.map_or(ReadBound::None, |k| ReadBound::PkSet(plan.target.schema.pk_keys([[k]])));
            assert_eq!((bound, !predicate.is_empty()), (want, filtered), "`{sql}`");
        }
    }
}
