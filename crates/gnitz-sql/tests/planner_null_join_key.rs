#![cfg(feature = "integration")]

//! Plan-structure tests for NULL equi-join-key gating.
//!
//! A NULL join key must match nothing (SQL 3VL). The planner gates NULL keys
//! out of the match with `IS NOT NULL` filters; a NOT NULL key emits no filter
//! node at all, so the common-case plan is byte-identical to the original.
//! These tests assert the emitted Filter-node count for each key-nullability /
//! join-type combination by reading back the circuit's `nodes` system table.

use gnitz_sql::{SqlPlanner, SqlResult};
use gnitz_test_harness::ServerHandle;

mod common;
use common::*;

fn view_id_of(results: &[SqlResult]) -> u64 {
    results
        .iter()
        .find_map(|r| match r {
            SqlResult::ViewCreated { view_id } => Some(*view_id),
            _ => None,
        })
        .expect("CREATE VIEW did not return a view id")
}

/// All-NOT-NULL keys: zero Filter nodes — byte-identical to the original plan.
#[test]
fn test_not_null_keys_emit_no_filter() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    {
        let mut p = SqlPlanner::new(&mut client, &sn);
        p.execute("CREATE TABLE l (id BIGINT NOT NULL PRIMARY KEY, fk BIGINT NOT NULL)")
            .unwrap();
        p.execute("CREATE TABLE r (k BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)")
            .unwrap();
        let inner = p
            .execute("CREATE VIEW vi AS SELECT * FROM l JOIN r ON l.fk = r.k")
            .unwrap();
        let left = p
            .execute("CREATE VIEW vl AS SELECT * FROM l LEFT JOIN r ON l.fk = r.k")
            .unwrap();
        let vi = view_id_of(&inner);
        let vl = view_id_of(&left);
        assert_eq!(filter_node_count(&mut client, vi), 0, "INNER NOT NULL: no filter");
        assert_eq!(filter_node_count(&mut client, vl), 0, "LEFT NOT NULL: no filter");
    }
}

/// INNER JOIN, nullable inner (right) key: one `IS NOT NULL` filter on the right.
#[test]
fn test_inner_nullable_right_key_emits_one_filter() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    {
        let mut p = SqlPlanner::new(&mut client, &sn);
        p.execute("CREATE TABLE l (id BIGINT NOT NULL PRIMARY KEY, fk BIGINT NOT NULL)")
            .unwrap();
        p.execute("CREATE TABLE r (k BIGINT NOT NULL PRIMARY KEY, rk BIGINT NULL)")
            .unwrap();
        let res = p
            .execute("CREATE VIEW v AS SELECT * FROM l JOIN r ON l.fk = r.rk")
            .unwrap();
        let vid = view_id_of(&res);
        assert_eq!(filter_node_count(&mut client, vid), 1);
    }
}

/// INNER JOIN, nullable left key: one `IS NOT NULL` filter on the left
/// (INNER drops NULL left keys outright — no bypass split).
#[test]
fn test_inner_nullable_left_key_emits_one_filter() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    {
        let mut p = SqlPlanner::new(&mut client, &sn);
        p.execute("CREATE TABLE l (id BIGINT NOT NULL PRIMARY KEY, fk BIGINT NULL)")
            .unwrap();
        p.execute("CREATE TABLE r (k BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)")
            .unwrap();
        let res = p
            .execute("CREATE VIEW v AS SELECT * FROM l JOIN r ON l.fk = r.k")
            .unwrap();
        let vid = view_id_of(&res);
        assert_eq!(filter_node_count(&mut client, vid), 1);
    }
}

/// LEFT JOIN, nullable preserved (left) key, NOT NULL right: one `IS NOT NULL`
/// filter gates the inner match. The NULL-keyed preserved rows are null-filled by
/// the filter-free `positive_part(A − π_A(inner))` path (which re-keys the
/// unfiltered input), not a dedicated `IS NULL` bypass — so one filter total.
#[test]
fn test_left_nullable_preserved_key_emits_one_filter() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    {
        let mut p = SqlPlanner::new(&mut client, &sn);
        p.execute("CREATE TABLE l (id BIGINT NOT NULL PRIMARY KEY, fk BIGINT NULL)")
            .unwrap();
        p.execute("CREATE TABLE r (k BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)")
            .unwrap();
        let res = p
            .execute("CREATE VIEW v AS SELECT * FROM l LEFT JOIN r ON l.fk = r.k")
            .unwrap();
        let vid = view_id_of(&res);
        assert_eq!(filter_node_count(&mut client, vid), 1);
    }
}

/// LEFT JOIN, both keys nullable: one `IS NOT NULL` filter per side gates the
/// inner match (left + right = two). The preserved (left) side's NULL-keyed rows
/// null-fill via the filter-free `positive_part` path, adding no bypass filter.
#[test]
fn test_left_both_nullable_emits_two_filters() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    {
        let mut p = SqlPlanner::new(&mut client, &sn);
        p.execute("CREATE TABLE l (id BIGINT NOT NULL PRIMARY KEY, fk BIGINT NULL)")
            .unwrap();
        p.execute("CREATE TABLE r (k BIGINT NOT NULL PRIMARY KEY, rk BIGINT NULL)")
            .unwrap();
        let res = p
            .execute("CREATE VIEW v AS SELECT * FROM l LEFT JOIN r ON l.fk = r.rk")
            .unwrap();
        let vid = view_id_of(&res);
        assert_eq!(filter_node_count(&mut client, vid), 2);
    }
}
