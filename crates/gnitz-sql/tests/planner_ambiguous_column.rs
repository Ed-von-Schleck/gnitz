#![cfg(feature = "integration")]

//! A `SELECT *` join view deliberately surfaces both sides' same-named columns
//! (e.g. both tables' `id`), so a derived relation's output schema can carry a
//! name twice. An unqualified — or single-relation qualified — reference to such
//! a name is ambiguous (standard SQL) and must error, not silently bind the
//! first (left) column. These tests pin every by-name resolution site, plus the
//! "dup-named views stay usable positionally" contract that the fix must not
//! regress.

use gnitz_sql::{GnitzSqlError, SqlPlanner};
use gnitz_test_harness::ServerHandle;

mod common;
use common::*;

/// Build `l`, `r`, and `jv = SELECT * FROM l JOIN r ON l.val = r.val`, whose
/// output schema is `[_join_pk, id, val, id, val]` — `id` and `val` each appear
/// twice.
fn setup_dup_view(client: &mut gnitz_core::GnitzClient, sn: &str) {
    let mut p = SqlPlanner::new(client, sn);
    p.execute("CREATE TABLE l (id BIGINT PRIMARY KEY, val BIGINT)").unwrap();
    p.execute("CREATE TABLE r (id BIGINT PRIMARY KEY, val BIGINT)").unwrap();
    p.execute("CREATE VIEW jv AS SELECT * FROM l JOIN r ON l.val = r.val")
        .unwrap();
}

/// Assert `r` is a `Bind` error whose message reports an ambiguous column.
fn assert_ambiguous(r: Result<Vec<gnitz_sql::SqlResult>, GnitzSqlError>) {
    match must_err(r) {
        GnitzSqlError::Bind(s) => assert!(s.contains("ambiguous"), "expected an 'ambiguous' Bind error, got: {s}"),
        e => panic!("expected Bind(ambiguous), got {:?}", e),
    }
}

// ── ambiguity errors ─────────────────────────────────────────────────────────

/// Direct SELECT projection — `apply_projection` (§5.2). `id` resolves to both
/// `l.id` and `r.id`.
#[test]
fn test_direct_select_ambiguous_projection() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    setup_dup_view(&mut client, &sn);

    let mut p = SqlPlanner::new(&mut client, &sn);
    assert_ambiguous(p.execute("SELECT id FROM jv"));
}

/// Direct SELECT WHERE — the seek fast path declines on ambiguity, and the
/// bind-first guard (§5.2) surfaces the precise ambiguity error rather than the
/// generic "WHERE on non-indexed column" message. The projection (`_join_pk`)
/// is unambiguous, so the error can only come from the WHERE column.
#[test]
fn test_direct_select_ambiguous_where() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    setup_dup_view(&mut client, &sn);

    let mut p = SqlPlanner::new(&mut client, &sn);
    assert_ambiguous(p.execute("SELECT _join_pk FROM jv WHERE id = 5"));
}

/// View projection — `build_projection` (§5.3).
#[test]
fn test_create_view_ambiguous_projection() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    setup_dup_view(&mut client, &sn);

    let mut p = SqlPlanner::new(&mut client, &sn);
    assert_ambiguous(p.execute("CREATE VIEW v2 AS SELECT id FROM jv"));
    drop(p);
    assert!(
        client.resolve_table_or_view_id(&sn, "v2").is_err(),
        "ambiguous view must not be registered"
    );
}

/// View WHERE — `bind_expr` `Expr::Identifier` (§5.1). The `SELECT *` projection
/// is fine; the WHERE column is ambiguous.
#[test]
fn test_create_view_ambiguous_where() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    setup_dup_view(&mut client, &sn);

    let mut p = SqlPlanner::new(&mut client, &sn);
    assert_ambiguous(p.execute("CREATE VIEW v5 AS SELECT * FROM jv WHERE id = 5"));
}

/// GROUP BY clause — `execute_create_group_by_view` (§5.3).
#[test]
fn test_create_view_ambiguous_group_by() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    setup_dup_view(&mut client, &sn);

    let mut p = SqlPlanner::new(&mut client, &sn);
    assert_ambiguous(p.execute("CREATE VIEW v3 AS SELECT COUNT(*) FROM jv GROUP BY id"));
}

/// HAVING aggregate argument — `extract_func_arg_col` (§5.3), which does NOT
/// route through `bind_expr`. The group key (`id`) and SELECT list are
/// unambiguous; only the HAVING `SUM(val)` references the duplicated name.
#[test]
fn test_create_view_ambiguous_having() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    let mut p = SqlPlanner::new(&mut client, &sn);
    // jv2 schema: [_join_pk, id, val, rid, val] — `id`/`rid` unique, `val` dup.
    p.execute("CREATE TABLE l2 (id BIGINT PRIMARY KEY, val BIGINT)")
        .unwrap();
    p.execute("CREATE TABLE r2 (rid BIGINT PRIMARY KEY, val BIGINT)")
        .unwrap();
    p.execute("CREATE VIEW jv2 AS SELECT * FROM l2 JOIN r2 ON l2.val = r2.val")
        .unwrap();

    assert_ambiguous(p.execute("CREATE VIEW gv AS SELECT id, COUNT(*) FROM jv2 GROUP BY id HAVING SUM(val) > 0"));
}

/// Qualified reference within a dup-named source of a join — `b.id` resolves to
/// both of `jv`'s `id` columns via `resolve_qualified_column` (§5.1). The
/// qualifier names the relation, not left-vs-right within it, so it is still
/// ambiguous.
#[test]
fn test_join_qualified_ambiguous_source() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    setup_dup_view(&mut client, &sn);

    let mut p = SqlPlanner::new(&mut client, &sn);
    // `_join_pk` is I64 (the `l.val = r.val` common type), so `x BIGINT` joins it
    // same-type; `b.id` is the ambiguous projection.
    p.execute("CREATE TABLE t (x BIGINT PRIMARY KEY)").unwrap();
    assert_ambiguous(p.execute("CREATE VIEW jq AS SELECT a.x, b.id FROM t a JOIN jv b ON a.x = b._join_pk"));
}

// ── dup-named views stay usable positionally (regression guards) ──────────────

/// Creating the dup-named join view itself must keep working (the `SELECT *`
/// wildcard contract surfaces both `id`/`val` pairs).
#[test]
fn test_create_dup_named_view_succeeds() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    setup_dup_view(&mut client, &sn);
    let (_, schema) = client.resolve_table_or_view_id(&sn, "jv").unwrap();
    let names: Vec<&str> = schema.columns.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(
        names,
        ["_join_pk", "id", "val", "id", "val"],
        "dup-named join view schema must surface both sides' columns"
    );
}

/// `SELECT *` over a dup-named view (positional read, no by-name reference).
#[test]
fn test_select_star_from_dup_view_succeeds() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    setup_dup_view(&mut client, &sn);
    let mut p = SqlPlanner::new(&mut client, &sn);
    p.execute("SELECT * FROM jv").unwrap();
}

/// Wildcard pass-through view over a dup-named source (§5.3): the
/// `reject_duplicate_column_names` guard is gated on `!is_wildcard`, so a
/// `SELECT *` that carries the dup names through positionally is allowed, and the
/// resulting view is itself readable positionally.
#[test]
fn test_wildcard_passthrough_over_dup_view_succeeds() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    setup_dup_view(&mut client, &sn);
    let mut p = SqlPlanner::new(&mut client, &sn);
    p.execute("CREATE VIEW v2 AS SELECT * FROM jv").unwrap();
    p.execute("SELECT * FROM v2").unwrap();
}

/// A reference to a non-duplicated name in a dup-named view still resolves.
#[test]
fn test_unique_name_in_dup_view_resolves() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    setup_dup_view(&mut client, &sn);
    let mut p = SqlPlanner::new(&mut client, &sn);
    p.execute("SELECT _join_pk FROM jv").unwrap();
}

// ── views are read-only: INSERT / UPDATE / DELETE reject a view (§5.4) ────────

/// Assert `r` is an `Unsupported` error reporting a view target.
fn assert_is_view(r: Result<Vec<gnitz_sql::SqlResult>, GnitzSqlError>) {
    match must_err(r) {
        GnitzSqlError::Unsupported(s) => assert!(
            s.contains("is a view"),
            "expected an 'is a view' Unsupported error, got: {s}"
        ),
        e => panic!("expected Unsupported(is a view), got {:?}", e),
    }
}

#[test]
fn test_insert_into_view_rejected() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    setup_dup_view(&mut client, &sn);
    let mut p = SqlPlanner::new(&mut client, &sn);
    assert_is_view(p.execute("INSERT INTO jv VALUES (1, 2, 3, 4, 5)"));
}

#[test]
fn test_update_view_rejected() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    setup_dup_view(&mut client, &sn);
    let mut p = SqlPlanner::new(&mut client, &sn);
    assert_is_view(p.execute("UPDATE jv SET val = 1 WHERE _join_pk = 1"));
}

#[test]
fn test_delete_from_view_rejected() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    setup_dup_view(&mut client, &sn);
    let mut p = SqlPlanner::new(&mut client, &sn);
    assert_is_view(p.execute("DELETE FROM jv"));
}

#[test]
fn test_create_index_on_view_rejected() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    setup_dup_view(&mut client, &sn);
    let mut p = SqlPlanner::new(&mut client, &sn);
    assert_is_view(p.execute("CREATE INDEX ix ON jv (id)"));
}
