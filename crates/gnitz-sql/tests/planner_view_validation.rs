#![cfg(feature = "integration")]

use gnitz_sql::{GnitzSqlError, SqlPlanner};
use gnitz_test_harness::ServerHandle;

mod common;
use common::*;

// ── item 21: LIMIT/OFFSET in VIEW definitions ────────────────────────

#[test]
fn test_view_limit_rejected() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    let mut p = SqlPlanner::new(&mut client, &sn);
    p.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT)").unwrap();
    let err = p.execute("CREATE VIEW v AS SELECT * FROM t LIMIT 10").unwrap_err();
    match err {
        GnitzSqlError::Unsupported(s) => assert!(s.to_uppercase().contains("LIMIT"), "got: {}", s),
        e => panic!("expected Unsupported, got {:?}", e),
    }
    // The rejected view must not be registered.
    assert!(
        client.resolve_table_or_view_id(&sn, "v").is_err(),
        "LIMIT view must not be registered"
    );
}

// ── item 22: set operation column type mismatch ──────────────────────

#[test]
fn test_set_op_type_mismatch_rejected() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    let mut p = SqlPlanner::new(&mut client, &sn);
    p.execute("CREATE TABLE a (id BIGINT PRIMARY KEY, val BIGINT)").unwrap();
    p.execute("CREATE TABLE b (id BIGINT PRIMARY KEY, val TEXT)").unwrap();
    let err = p
        .execute("CREATE VIEW v AS SELECT * FROM a UNION ALL SELECT * FROM b")
        .unwrap_err();
    match err {
        GnitzSqlError::Plan(s) => assert!(s.contains("type mismatch"), "got: {}", s),
        e => panic!("expected Plan, got {:?}", e),
    }
}

// ── item 23: duplicate column names in CREATE TABLE ──────────────────

#[test]
fn test_duplicate_column_name_rejected() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    let mut p = SqlPlanner::new(&mut client, &sn);
    let err = p
        .execute("CREATE TABLE dup (a BIGINT, a BIGINT, PRIMARY KEY(a))")
        .unwrap_err();
    match err {
        GnitzSqlError::Plan(s) => assert!(s.to_lowercase().contains("duplicate column"), "got: {}", s),
        e => panic!("expected Plan, got {:?}", e),
    }
    assert!(
        client.resolve_table_id(&sn, "dup").is_err(),
        "table with duplicate column must not be registered"
    );
}

// ── item 24: self-join via pass-through wrap ─────────────────────────

#[test]
fn test_self_join_supported() {
    // A direct self-join registers: the repeated relation is wrapped in a
    // distinct-source pass-through hidden view, so the two join inputs are
    // never the same source (single-source-per-epoch holds).
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    let mut p = SqlPlanner::new(&mut client, &sn);
    p.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, fk BIGINT NOT NULL)")
        .unwrap();
    p.execute("CREATE VIEW v AS SELECT t1.id AS i1, t2.id AS i2 FROM t AS t1 JOIN t AS t2 ON t1.fk = t2.fk")
        .unwrap();
    p.execute("INSERT INTO t VALUES (1, 7), (2, 7), (3, 9)").unwrap();
    // fk=7 pairs {1,2}×{1,2}; fk=9 pairs {3}×{3}.
    let rows = payload_rows(&mut client, &sn, "v", &["i1", "i2"]);
    assert_eq!(rows, vec![vec![1, 1], vec![1, 2], vec![2, 1], vec![2, 2], vec![3, 3]]);
}

// ── item 26: self-referencing foreign key ────────────────────────────

#[test]
fn test_self_referencing_fk_accepted() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    {
        let mut p = SqlPlanner::new(&mut client, &sn);
        p.execute("CREATE TABLE hier (id BIGINT PRIMARY KEY, parent_id BIGINT REFERENCES hier(id))")
            .unwrap();
    }
    let (_, s) = client.resolve_table_id(&sn, "hier").unwrap();
    let fk = s
        .columns
        .iter()
        .find(|c| c.name.eq_ignore_ascii_case("parent_id"))
        .unwrap();
    // Self-reference encodes the target table id as 0 (sentinel for "same table").
    assert_eq!(fk.fk_table_id, 0, "self-ref FK encodes same-table sentinel");
}

// ── item 17: wide-join combined column count > MAX_COLUMNS ────────────

#[test]
fn test_wide_join_column_count_rejected() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    let mut p = SqlPlanner::new(&mut client, &sn);
    // Two tables of 33 columns each: join output = 1 (_join_pk) + 33 + 33 = 67 > 65.
    let mk = |name: &str| {
        let mut cols: Vec<String> = vec!["id BIGINT PRIMARY KEY".into(), "fk BIGINT NOT NULL".into()];
        for i in 0..31 {
            cols.push(format!("c{} BIGINT", i));
        }
        format!("CREATE TABLE {} ({})", name, cols.join(", "))
    };
    p.execute(&mk("wl")).unwrap();
    p.execute(&mk("wr")).unwrap();
    let err = p
        .execute("CREATE VIEW v AS SELECT * FROM wl JOIN wr ON wl.fk = wr.fk")
        .unwrap_err();
    match err {
        GnitzSqlError::Unsupported(s) => assert!(s.contains("column") || s.contains("MAX_COLUMNS"), "got: {}", s),
        e => panic!("expected Unsupported, got {:?}", e),
    }
}
