#![cfg(feature = "integration")]

//! Regression tests for the IN-list DELETE path (Bug 1: positivity).
//!
//! `DELETE … WHERE pk IN (…)` must seek each key and retract only the ones that
//! exist; a retraction of an absent or tombstoned key would otherwise inject a
//! negative-weight phantom row into the base table (positivity violation), and
//! `RowsAffected` must report the true count, not the raw list length.

mod common;
use common::*;
use gnitz_test_harness::ServerHandle;

#[test]
fn delete_in_list_absent_key_is_noop() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (mut client, sn) = make_planner(&srv);
    exec(&mut client, &sn, "CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT)");
    exec(&mut client, &sn, "INSERT INTO t VALUES (1, 10)");

    let before = payload_rows(&mut client, &sn, "t", &["id", "v"]);
    let n = affected(&mut client, &sn, "DELETE FROM t WHERE id IN (2, 3)");
    assert_eq!(n, 0, "absent keys delete 0 rows");
    let after = payload_rows(&mut client, &sn, "t", &["id", "v"]);
    assert_eq!(before, after, "absent-key DELETE must not mutate the table");

    // A phantom retraction at id=2, if injected, is not healed by this insert;
    // the table must contain exactly one clean row per id.
    exec(&mut client, &sn, "INSERT INTO t VALUES (2, 20)");
    assert_eq!(
        payload_rows(&mut client, &sn, "t", &["id", "v"]),
        vec![vec![1, 10], vec![2, 20]],
        "re-insert at a previously-absent key yields exactly one clean row",
    );
}

#[test]
fn delete_in_list_counts_distinct_existing_only() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (mut client, sn) = make_planner(&srv);
    exec(&mut client, &sn, "CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT)");
    exec(&mut client, &sn, "INSERT INTO t VALUES (1, 10)");
    exec(&mut client, &sn, "INSERT INTO t VALUES (2, 20)");

    // 1,2 present; 99 absent; 1 duplicated → 2 distinct existing rows deleted.
    let n = affected(&mut client, &sn, "DELETE FROM t WHERE id IN (1, 2, 99, 1)");
    assert_eq!(n, 2, "count reflects distinct existing rows, not list length");
    assert!(
        payload_rows(&mut client, &sn, "t", &["id", "v"]).is_empty(),
        "both present rows removed; the absent key 99 left no phantom",
    );
}

#[test]
fn delete_in_list_duplicate_existing_is_idempotent() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (mut client, sn) = make_planner(&srv);
    exec(&mut client, &sn, "CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT)");
    exec(&mut client, &sn, "INSERT INTO t VALUES (1, 10)");

    let n = affected(&mut client, &sn, "DELETE FROM t WHERE id IN (1, 1)");
    assert_eq!(n, 1, "a duplicated existing key deletes one row");
    assert!(payload_rows(&mut client, &sn, "t", &["id", "v"]).is_empty());
}
