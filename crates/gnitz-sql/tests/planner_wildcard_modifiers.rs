#![cfg(feature = "integration")]

//! `SELECT * EXCEPT/EXCLUDE/RENAME (...)` honored, `REPLACE`/`ILIKE` rejected,
//! across every wildcard-expansion surface (single-table, join, set-op,
//! scalar-subquery, EXISTS/mark, direct SELECT, CTE body) plus the shared
//! validation in `WildcardRewrite::for_item`.

use gnitz_core::{GnitzClient, Schema};
use gnitz_sql::{GnitzSqlError, SqlPlanner};
use gnitz_test_harness::ServerHandle;

mod common;
use common::*;

/// Lowercased names of the *visible* (presentation) columns — the wildcard
/// modifiers only ever touch these; a hidden synthetic/PK slot is invisible.
fn visible_names(s: &Schema) -> Vec<String> {
    s.visible_columns().map(|(_, c)| c.name.to_lowercase()).collect()
}

fn view_schema(client: &mut GnitzClient, sn: &str, view: &str) -> Schema {
    client.resolve_table_or_view_id(sn, view).unwrap().1
}

fn select_rows(client: &mut GnitzClient, sn: &str, sql: &str) -> (gnitz_core::Schema, gnitz_core::ZSetBatch) {
    let mut p = SqlPlanner::new(client, sn);
    match p.execute(sql).unwrap().pop().unwrap() {
        gnitz_sql::SqlResult::Rows { schema, batch } => (schema, batch),
        other => panic!("expected Rows, got {other:?}"),
    }
}

// ── Single-table CREATE VIEW ─────────────────────────────────────────────────

#[test]
fn test_view_except_drops_column() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    exec(
        &mut client,
        &sn,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, name TEXT, age BIGINT)",
    );
    exec(&mut client, &sn, "CREATE VIEW v AS SELECT * EXCEPT (age) FROM t");

    let s = view_schema(&mut client, &sn, "v");
    assert_eq!(visible_names(&s), vec!["id", "name"], "age dropped, order kept");
    assert_eq!(s.pk_indices(), &[0], "PK still at slot 0");
}

#[test]
fn test_view_exclude_is_synonym_of_except() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    exec(
        &mut client,
        &sn,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, name TEXT, age BIGINT)",
    );
    exec(&mut client, &sn, "CREATE VIEW ve AS SELECT * EXCEPT (age) FROM t");
    exec(&mut client, &sn, "CREATE VIEW vx AS SELECT * EXCLUDE (age) FROM t");

    assert_eq!(
        visible_names(&view_schema(&mut client, &sn, "ve")),
        visible_names(&view_schema(&mut client, &sn, "vx")),
        "EXCLUDE and EXCEPT produce the same column set"
    );
}

#[test]
fn test_view_rename_relabels_in_place() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    exec(
        &mut client,
        &sn,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, name TEXT, age BIGINT)",
    );
    exec(
        &mut client,
        &sn,
        "CREATE VIEW v AS SELECT * RENAME (age AS years) FROM t",
    );

    let s = view_schema(&mut client, &sn, "v");
    assert_eq!(
        visible_names(&s),
        vec!["id", "name", "years"],
        "age relabeled to years, position unchanged"
    );
    assert_eq!(s.pk_indices(), &[0]);
}

#[test]
fn test_view_except_and_rename_combined() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    exec(
        &mut client,
        &sn,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, a BIGINT, b BIGINT, d BIGINT)",
    );
    exec(
        &mut client,
        &sn,
        "CREATE VIEW v AS SELECT * EXCEPT (a) RENAME (b AS c) FROM t",
    );

    assert_eq!(
        visible_names(&view_schema(&mut client, &sn, "v")),
        vec!["id", "c", "d"],
        "a dropped, b→c, d kept"
    );
}

#[test]
fn test_view_except_is_case_insensitive() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    exec(
        &mut client,
        &sn,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, name TEXT, age BIGINT)",
    );
    exec(&mut client, &sn, "CREATE VIEW v AS SELECT * EXCEPT (AGE) FROM t");
    assert_eq!(
        visible_names(&view_schema(&mut client, &sn, "v")),
        vec!["id", "name"],
        "AGE matches age case-insensitively"
    );
}

#[test]
fn test_view_except_unknown_column_rejected() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    exec(&mut client, &sn, "CREATE TABLE t (id BIGINT PRIMARY KEY, a BIGINT)");
    match try_exec(&mut client, &sn, "CREATE VIEW v AS SELECT * EXCEPT (nope) FROM t").unwrap_err() {
        GnitzSqlError::Bind(s) => assert!(s.contains("nope"), "got: {s}"),
        e => panic!("expected Bind, got {e:?}"),
    }
    assert!(client.resolve_table_or_view_id(&sn, "v").is_err(), "must not register");
}

#[test]
fn test_view_except_rename_contradiction_rejected() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    exec(
        &mut client,
        &sn,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, a BIGINT, b BIGINT)",
    );
    match try_exec(
        &mut client,
        &sn,
        "CREATE VIEW v AS SELECT * EXCEPT (a) RENAME (a AS b) FROM t",
    )
    .unwrap_err()
    {
        GnitzSqlError::Bind(s) => assert!(s.contains("excluded"), "got: {s}"),
        e => panic!("expected Bind, got {e:?}"),
    }
}

#[test]
fn test_view_rename_duplicate_source_rejected() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    exec(&mut client, &sn, "CREATE TABLE t (id BIGINT PRIMARY KEY, a BIGINT)");
    match try_exec(
        &mut client,
        &sn,
        "CREATE VIEW v AS SELECT * RENAME (a AS x, a AS y) FROM t",
    )
    .unwrap_err()
    {
        GnitzSqlError::Bind(s) => assert!(s.contains("twice"), "got: {s}"),
        e => panic!("expected Bind, got {e:?}"),
    }
}

#[test]
fn test_view_except_pk_reprepended_hidden() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    exec(
        &mut client,
        &sn,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, a BIGINT, b BIGINT)",
    );
    // Dropping the PK on a CREATE VIEW is like `SELECT a, b`: place_pk_front
    // re-prepends the source PK as a hidden slot so the physical key rides on.
    exec(&mut client, &sn, "CREATE VIEW v AS SELECT * EXCEPT (id) FROM t");

    let s = view_schema(&mut client, &sn, "v");
    assert_eq!(s.pk_indices(), &[0], "PK physically at slot 0");
    assert!(s.columns[0].is_hidden, "re-prepended PK is hidden");
    assert!(
        s.columns[0].name.eq_ignore_ascii_case("id"),
        "hidden slot is the source PK"
    );
    assert_eq!(visible_names(&s), vec!["a", "b"], "id not presented");
}

#[test]
fn test_view_except_data_correct() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    exec(
        &mut client,
        &sn,
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL, b BIGINT NOT NULL)",
    );
    exec(&mut client, &sn, "CREATE VIEW v AS SELECT * EXCEPT (a) FROM t");
    exec(&mut client, &sn, "INSERT INTO t VALUES (1, 10, 100), (2, 20, 200)");
    assert_eq!(
        payload_rows(&mut client, &sn, "v", &["id", "b"]),
        vec![vec![1, 100], vec![2, 200]],
        "only id + b survive with correct values"
    );
    // Incremental delete tracks.
    exec(&mut client, &sn, "DELETE FROM t WHERE id = 1");
    assert_eq!(
        payload_rows(&mut client, &sn, "v", &["id", "b"]),
        vec![vec![2, 200]],
        "delete propagates through the reduced projection"
    );
}

// ── Join view ────────────────────────────────────────────────────────────────

#[test]
fn test_join_except_drops_all_matching() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    exec(
        &mut client,
        &sn,
        "CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, av BIGINT NOT NULL)",
    );
    exec(
        &mut client,
        &sn,
        "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, bv BIGINT NOT NULL)",
    );
    // A `SELECT *` join surfaces two `k` columns (a.k, b.k); EXCEPT (k) drops
    // BOTH (drop-all-matching over bare names).
    exec(
        &mut client,
        &sn,
        "CREATE VIEW v AS SELECT * EXCEPT (k) FROM a JOIN b ON a.k = b.k",
    );
    let s = view_schema(&mut client, &sn, "v");
    let vis = visible_names(&s);
    assert_eq!(vis.iter().filter(|n| *n == "k").count(), 0, "both k columns dropped");
    assert!(vis.contains(&"av".to_string()) && vis.contains(&"bv".to_string()));
}

#[test]
fn test_join_rename_all_matching() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    exec(
        &mut client,
        &sn,
        "CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, av BIGINT NOT NULL)",
    );
    exec(
        &mut client,
        &sn,
        "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, bv BIGINT NOT NULL)",
    );
    // RENAME (av AS x) — a fresh, non-colliding target relabels cleanly.
    exec(
        &mut client,
        &sn,
        "CREATE VIEW v AS SELECT * EXCEPT (k, id) RENAME (av AS x) FROM a JOIN b ON a.k = b.k",
    );
    let vis = visible_names(&view_schema(&mut client, &sn, "v"));
    assert!(vis.contains(&"x".to_string()), "av renamed to x: {vis:?}");
    assert!(!vis.contains(&"av".to_string()));
}

// ── Set-op view ──────────────────────────────────────────────────────────────

#[test]
fn test_set_op_except_both_branches() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    exec(
        &mut client,
        &sn,
        "CREATE TABLE a (id BIGINT PRIMARY KEY, x BIGINT NOT NULL, y BIGINT NOT NULL)",
    );
    exec(
        &mut client,
        &sn,
        "CREATE TABLE b (id BIGINT PRIMARY KEY, x BIGINT NOT NULL, y BIGINT NOT NULL)",
    );
    exec(
        &mut client,
        &sn,
        "CREATE VIEW v AS SELECT * EXCEPT (x) FROM a EXCEPT SELECT * EXCEPT (x) FROM b",
    );
    let vis = visible_names(&view_schema(&mut client, &sn, "v"));
    assert!(
        !vis.contains(&"x".to_string()) && vis.contains(&"y".to_string()),
        "{vis:?}"
    );
}

#[test]
fn test_set_op_except_one_sided_arity_error() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    exec(
        &mut client,
        &sn,
        "CREATE TABLE a (id BIGINT PRIMARY KEY, x BIGINT NOT NULL, y BIGINT NOT NULL)",
    );
    exec(
        &mut client,
        &sn,
        "CREATE TABLE b (id BIGINT PRIMARY KEY, x BIGINT NOT NULL, y BIGINT NOT NULL)",
    );
    // One branch reduces its arity → the existing column-count check trips.
    let err = try_exec(
        &mut client,
        &sn,
        "CREATE VIEW v AS SELECT * EXCEPT (x) FROM a UNION SELECT * FROM b",
    )
    .unwrap_err();
    assert!(
        matches!(&err, GnitzSqlError::Plan(s) if s.contains("column count")),
        "expected arity mismatch, got {err:?}"
    );
}

#[test]
fn test_distinct_star_except() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    exec(
        &mut client,
        &sn,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, x BIGINT NOT NULL, y BIGINT NOT NULL)",
    );
    // Exercises the set_op.rs bare-wildcard gate flip: DISTINCT routes through
    // resolve_set_projection, which must NOT treat `* EXCEPT` as a bare `*`.
    exec(&mut client, &sn, "CREATE VIEW v AS SELECT DISTINCT * EXCEPT (x) FROM t");
    let vis = visible_names(&view_schema(&mut client, &sn, "v"));
    assert!(
        !vis.contains(&"x".to_string()) && vis.contains(&"y".to_string()),
        "{vis:?}"
    );
}

#[test]
fn test_set_op_except_drops_float_column() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    exec(
        &mut client,
        &sn,
        "CREATE TABLE a (id BIGINT PRIMARY KEY, d DOUBLE NOT NULL, g BIGINT NOT NULL)",
    );
    exec(
        &mut client,
        &sn,
        "CREATE TABLE b (id BIGINT PRIMARY KEY, d DOUBLE NOT NULL, g BIGINT NOT NULL)",
    );
    // The float `d` is dropped BEFORE reject_float_keys sees the row-identity
    // columns, so a set op that would otherwise be rejected now succeeds.
    exec(
        &mut client,
        &sn,
        "CREATE VIEW v AS SELECT * EXCEPT (d) FROM a UNION SELECT * EXCEPT (d) FROM b",
    );
    let vis = visible_names(&view_schema(&mut client, &sn, "v"));
    assert!(
        !vis.contains(&"d".to_string()) && vis.contains(&"g".to_string()),
        "{vis:?}"
    );
}

// ── Scalar-subquery view (H-materialization) ─────────────────────────────────

#[test]
fn test_scalar_subquery_except_keeps_where_over_h() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    exec(
        &mut client,
        &sn,
        "CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, v BIGINT NOT NULL)",
    );
    exec(
        &mut client,
        &sn,
        "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, w BIGINT NOT NULL)",
    );
    // `v` is EXCEPT-dropped from the OUTPUT but still referenced by the WHERE —
    // proving H (the join intermediate) stays complete/un-renamed.
    exec(
        &mut client,
        &sn,
        "CREATE VIEW v AS SELECT * EXCEPT (v) FROM a \
         WHERE v > 5 AND (SELECT COUNT(*) FROM b WHERE b.k = a.k) >= 1",
    );
    let vis = visible_names(&view_schema(&mut client, &sn, "v"));
    assert!(!vis.contains(&"v".to_string()), "v dropped from output: {vis:?}");
    assert!(vis.contains(&"id".to_string()) && vis.contains(&"k".to_string()));
}

#[test]
fn test_scalar_subquery_rename_over_h() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    exec(
        &mut client,
        &sn,
        "CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, v BIGINT NOT NULL)",
    );
    exec(
        &mut client,
        &sn,
        "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, w BIGINT NOT NULL)",
    );
    exec(
        &mut client,
        &sn,
        "CREATE VIEW v AS SELECT * RENAME (v AS vv) FROM a \
         WHERE (SELECT COUNT(*) FROM b WHERE b.k = a.k) >= 1",
    );
    let vis = visible_names(&view_schema(&mut client, &sn, "v"));
    assert!(
        vis.contains(&"vv".to_string()) && !vis.contains(&"v".to_string()),
        "{vis:?}"
    );
}

// ── EXISTS semijoin + mark views ─────────────────────────────────────────────

#[test]
fn test_exists_semijoin_except() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    exec(
        &mut client,
        &sn,
        "CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, v BIGINT NOT NULL)",
    );
    exec(
        &mut client,
        &sn,
        "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL)",
    );
    exec(
        &mut client,
        &sn,
        "CREATE VIEW v AS SELECT * EXCEPT (v) FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.k = a.k)",
    );
    let vis = visible_names(&view_schema(&mut client, &sn, "v"));
    assert!(
        !vis.contains(&"v".to_string()) && vis.contains(&"k".to_string()),
        "{vis:?}"
    );
}

#[test]
fn test_mark_view_except() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    exec(
        &mut client,
        &sn,
        "CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, v BIGINT NOT NULL)",
    );
    exec(
        &mut client,
        &sn,
        "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL)",
    );
    // Subquery under OR → mark path (build_mark_projection, arm 6).
    exec(
        &mut client,
        &sn,
        "CREATE VIEW v AS SELECT * EXCEPT (v) FROM a \
         WHERE a.k IN (SELECT k FROM b) OR a.v > 100",
    );
    let vis = visible_names(&view_schema(&mut client, &sn, "v"));
    assert!(
        !vis.contains(&"v".to_string()) && vis.contains(&"k".to_string()),
        "{vis:?}"
    );
}

// ── Direct SELECT ────────────────────────────────────────────────────────────

#[test]
fn test_direct_select_except() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    exec(
        &mut client,
        &sn,
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL, b BIGINT NOT NULL)",
    );
    exec(&mut client, &sn, "INSERT INTO t VALUES (1, 10, 100), (2, 20, 200)");
    let (s, b) = select_rows(&mut client, &sn, "SELECT * EXCEPT (a) FROM t");
    assert_eq!(
        s.columns.iter().map(|c| c.name.to_lowercase()).collect::<Vec<_>>(),
        vec!["id", "b"],
        "a excluded from direct SELECT result schema"
    );
    assert_eq!(b.len(), 2);
}

#[test]
fn test_direct_select_rename() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    exec(
        &mut client,
        &sn,
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL)",
    );
    exec(&mut client, &sn, "INSERT INTO t VALUES (1, 10)");
    let (s, _) = select_rows(&mut client, &sn, "SELECT * RENAME (a AS x) FROM t");
    assert_eq!(
        s.columns.iter().map(|c| c.name.to_lowercase()).collect::<Vec<_>>(),
        vec!["id", "x"],
        "a renamed to x in direct SELECT result schema"
    );
}

#[test]
fn test_direct_select_except_pk_rejected() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    exec(
        &mut client,
        &sn,
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL)",
    );
    // Direct SELECT has no place_pk_front — dropping the PK hits the same
    // pre-existing "must include a PRIMARY KEY column" rejection as `SELECT a`.
    match try_exec(&mut client, &sn, "SELECT * EXCEPT (id) FROM t").unwrap_err() {
        GnitzSqlError::Unsupported(s) => assert!(s.contains("PRIMARY KEY"), "got: {s}"),
        e => panic!("expected Unsupported, got {e:?}"),
    }
}

// ── REPLACE / ILIKE rejection (single unbypassable authority) ─────────────────

#[test]
fn test_replace_rejected_in_view() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    exec(&mut client, &sn, "CREATE TABLE t (id BIGINT PRIMARY KEY, a BIGINT)");
    match try_exec(
        &mut client,
        &sn,
        "CREATE VIEW v AS SELECT * REPLACE (a + 1 AS a) FROM t",
    )
    .unwrap_err()
    {
        GnitzSqlError::Unsupported(s) => assert!(s.contains("REPLACE"), "got: {s}"),
        e => panic!("expected Unsupported, got {e:?}"),
    }
    assert!(client.resolve_table_or_view_id(&sn, "v").is_err(), "must not register");
}

#[test]
fn test_ilike_rejected_in_view() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    exec(&mut client, &sn, "CREATE TABLE t (id BIGINT PRIMARY KEY, a BIGINT)");
    match try_exec(&mut client, &sn, "CREATE VIEW v AS SELECT * ILIKE 'a%' FROM t").unwrap_err() {
        GnitzSqlError::Unsupported(s) => assert!(s.contains("ILIKE"), "got: {s}"),
        e => panic!("expected Unsupported, got {e:?}"),
    }
}

#[test]
fn test_replace_rejected_direct_select() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    exec(&mut client, &sn, "CREATE TABLE t (id BIGINT PRIMARY KEY, a BIGINT)");
    match try_exec(&mut client, &sn, "SELECT * REPLACE (a + 1 AS a) FROM t").unwrap_err() {
        GnitzSqlError::Unsupported(s) => assert!(s.contains("REPLACE"), "got: {s}"),
        e => panic!("expected Unsupported, got {e:?}"),
    }
}

#[test]
fn test_replace_rejected_in_cte_body() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    exec(&mut client, &sn, "CREATE TABLE t (id BIGINT PRIMARY KEY, a BIGINT)");
    // The dispatch.rs:474 gate flip is load-bearing: without it the CTE body
    // would take the identity fast path and silently drop REPLACE.
    match try_exec(
        &mut client,
        &sn,
        "CREATE VIEW v AS WITH c AS (SELECT * REPLACE (a + 1 AS a) FROM t) SELECT * FROM c",
    )
    .unwrap_err()
    {
        GnitzSqlError::Unsupported(s) => assert!(s.contains("REPLACE"), "got: {s}"),
        e => panic!("expected Unsupported, got {e:?}"),
    }
    assert!(client.resolve_table_or_view_id(&sn, "v").is_err(), "must not register");
}
