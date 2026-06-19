#![cfg(feature = "integration")]

use gnitz_sql::{GnitzSqlError, SqlPlanner};
use gnitz_test_harness::ServerHandle;

mod common;
use common::*;

// ── DISTINCT over a compound-PK source ───────────────────────────────
// Membership is decided by the projected content hash (`_distinct_pk`),
// independent of the source PK's arity, so a compound PK needs no special
// handling — these confirm the unified byte path serves every arity.

#[test]
fn test_select_distinct_compound_pk_subset() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    {
        let mut p = SqlPlanner::new(&mut client, &sn);
        p.execute("CREATE TABLE t (a BIGINT, b BIGINT, c1 BIGINT NOT NULL, PRIMARY KEY (a, b))")
            .unwrap();
        p.execute("CREATE VIEW v AS SELECT DISTINCT a FROM t").unwrap();
        p.execute("INSERT INTO t (a, b, c1) VALUES (1, 10, 100), (1, 20, 200), (2, 10, 300)")
            .unwrap();
    }
    let (schema, batch) = read_view(&mut client, &sn, "v");
    assert_eq!(batch.len(), 2, "DISTINCT a must collapse rows differing only in b");
    let a_col = col_idx(&schema, "a");
    let mut vals: Vec<i64> = (0..batch.len()).map(|r| i64_at(&batch, a_col, r)).collect();
    vals.sort();
    assert_eq!(vals, vec![1, 2]);
}

#[test]
fn test_select_distinct_compound_pk_star() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    {
        let mut p = SqlPlanner::new(&mut client, &sn);
        p.execute("CREATE TABLE t (a BIGINT, b BIGINT, c1 BIGINT NOT NULL, PRIMARY KEY (a, b))")
            .unwrap();
        p.execute("CREATE VIEW v AS SELECT DISTINCT * FROM t").unwrap();
        p.execute("INSERT INTO t (a, b, c1) VALUES (1, 10, 7), (1, 20, 7), (2, 10, 9)")
            .unwrap();
    }
    let (schema, batch) = read_view(&mut client, &sn, "v");
    assert_eq!(batch.len(), 3, "full-row identity: three distinct rows");
    assert_eq!(schema.columns.len(), 4, "synthetic pk + a + b + c1");
}

#[test]
fn test_select_distinct_incrementality() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    {
        let mut p = SqlPlanner::new(&mut client, &sn);
        p.execute("CREATE TABLE t (a BIGINT, b BIGINT, c1 BIGINT NOT NULL, PRIMARY KEY (a, b))")
            .unwrap();
        p.execute("CREATE VIEW v AS SELECT DISTINCT c1 FROM t").unwrap();
        p.execute("INSERT INTO t (a, b, c1) VALUES (1, 10, 7), (1, 20, 7), (2, 10, 9)")
            .unwrap();
    }
    let (schema, batch) = read_view(&mut client, &sn, "v");
    assert_eq!(batch.len(), 2, "dedup ignores both PK columns");
    let c1 = col_idx(&schema, "c1");
    let mut vals: Vec<i64> = (0..batch.len()).map(|r| i64_at(&batch, c1, r)).collect();
    vals.sort();
    assert_eq!(vals, vec![7, 9]);

    {
        let mut p = SqlPlanner::new(&mut client, &sn);
        p.execute("DELETE FROM t WHERE a=1 AND b=10").unwrap();
    }
    let (_, batch2) = read_view(&mut client, &sn, "v");
    assert_eq!(batch2.len(), 2, "one of the 7s deleted, 7 still carried by (1,20)");

    {
        let mut p = SqlPlanner::new(&mut client, &sn);
        p.execute("DELETE FROM t WHERE a=1 AND b=20").unwrap();
    }
    let (_, batch3) = read_view(&mut client, &sn, "v");
    assert_eq!(batch3.len(), 1, "the other 7 deleted, 7 is retracted");
    assert_eq!(i64_at(&batch3, c1, 0), 9);
}

#[test]
fn test_select_distinct_wide_pk_regression() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    {
        let mut p = SqlPlanner::new(&mut client, &sn);
        p.execute("CREATE TABLE t (id DECIMAL(38, 0) PRIMARY KEY, c1 BIGINT NOT NULL)")
            .unwrap();
        p.execute("CREATE VIEW v AS SELECT DISTINCT c1 FROM t").unwrap();
        p.execute("INSERT INTO t (id, c1) VALUES (1, 7), (2, 7), (3, 9)")
            .unwrap();
    }
    let (schema, batch) = read_view(&mut client, &sn, "v");
    assert_eq!(batch.len(), 2);
    let c1 = col_idx(&schema, "c1");
    let mut vals: Vec<i64> = (0..batch.len()).map(|r| i64_at(&batch, c1, r)).collect();
    vals.sort();
    assert_eq!(vals, vec![7, 9]);
}

// ── item 14: SELECT DISTINCT projects and deduplicates ───────────────

#[test]
fn test_select_distinct_projects_and_dedups() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    {
        let mut p = SqlPlanner::new(&mut client, &sn);
        p.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, c1 BIGINT NOT NULL, c2 BIGINT NOT NULL)")
            .unwrap();
        // DISTINCT over c1 only: c1 values {7,7,9} → distinct {7,9}.
        p.execute("CREATE VIEW v AS SELECT DISTINCT c1 FROM t").unwrap();
        p.execute("INSERT INTO t (id, c1, c2) VALUES (1, 7, 100), (2, 7, 200), (3, 9, 300)")
            .unwrap();
    }
    let (schema, batch) = read_view(&mut client, &sn, "v");
    // Schema is [_distinct_pk, c1] — id and c2 must not leak.
    assert!(schema.columns.iter().any(|c| c.name.eq_ignore_ascii_case("c1")));
    assert!(
        !schema.columns.iter().any(|c| c.name.eq_ignore_ascii_case("id")),
        "unselected `id` must not appear: {:?}",
        schema.columns.iter().map(|c| &c.name).collect::<Vec<_>>()
    );
    assert!(
        !schema.columns.iter().any(|c| c.name.eq_ignore_ascii_case("c2")),
        "unselected `c2` must not appear"
    );
    assert_eq!(schema.columns.len(), 2, "exactly _distinct_pk + c1");

    // Two distinct c1 values, not three rows.
    assert_eq!(batch.len(), 2, "DISTINCT c1 must collapse the two c1=7 rows");
    let c1 = col_idx(&schema, "c1");
    let mut vals: Vec<i64> = (0..batch.len()).map(|r| i64_at(&batch, c1, r)).collect();
    vals.sort();
    assert_eq!(vals, vec![7, 9], "distinct c1 values must be {{7, 9}}");
}

// ── float DISTINCT row-identity rejection (Fix A2) ───────────────────
//
// A DISTINCT identity hashes each projected column's raw IEEE-754 bytes
// (reindex_hash_row), so a float column splits -0.0/+0.0 and distinct-NaN rows
// that SQL set semantics treat as equal. Both the explicit projection and the
// `SELECT DISTINCT *` early-return path must reject it.

#[test]
fn test_select_distinct_float_column_rejected() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    let mut p = SqlPlanner::new(&mut client, &sn);
    p.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, d DOUBLE NOT NULL, g BIGINT NOT NULL)")
        .unwrap();

    // Explicit float projection.
    let err = p.execute("CREATE VIEW v AS SELECT DISTINCT d FROM t").unwrap_err();
    assert!(
        matches!(&err, GnitzSqlError::Unsupported(s) if s.contains("float")),
        "expected float-identity Unsupported, got {:?}",
        err,
    );

    // `SELECT DISTINCT *` over a table with a float column hits the all-* early
    // return, which must also be guarded.
    let err2 = p.execute("CREATE VIEW v2 AS SELECT DISTINCT * FROM t").unwrap_err();
    assert!(
        matches!(&err2, GnitzSqlError::Unsupported(s) if s.contains("float")),
        "expected float-identity Unsupported for DISTINCT *, got {:?}",
        err2,
    );

    // A non-float DISTINCT still succeeds.
    p.execute("CREATE VIEW v3 AS SELECT DISTINCT g FROM t").unwrap();
}

// ── Duplicate output column names rejected (DISTINCT view) ────────────

#[test]
fn test_distinct_duplicate_output_columns_rejected() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    let mut p = SqlPlanner::new(&mut client, &sn);
    p.execute("CREATE TABLE di_dup (id BIGINT PRIMARY KEY, a BIGINT NOT NULL, b BIGINT NOT NULL)")
        .unwrap();
    let err = p
        .execute("CREATE VIEW v_di_dup AS SELECT DISTINCT a AS x, b AS x FROM di_dup")
        .unwrap_err();
    match err {
        GnitzSqlError::Plan(s) => assert!(s.contains("duplicate column"), "got: {}", s),
        e => panic!("expected Plan, got {:?}", e),
    }
}
