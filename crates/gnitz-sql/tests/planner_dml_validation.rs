#![cfg(feature = "integration")]

//! DML arity/duplicate-assignment validation (Bugs 6 & 7).
//!
//! Bug 6: a column appearing twice in an UPDATE / ON CONFLICT DO UPDATE SET list
//! was silently first-wins; standard SQL rejects it.
//! Bug 7: an INSERT VALUES row with more expressions than columns silently
//! discarded the extras; standard SQL rejects it.

mod common;
use common::*;
use gnitz_test_harness::ServerHandle;

// ── Bug 6: duplicate column in SET list ──────────────────────────────

#[test]
fn update_duplicate_column_assignment_is_rejected() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    exec(&mut client, &sn, "CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT)");
    exec(&mut client, &sn, "INSERT INTO t VALUES (1, 10)");
    let result = try_exec(&mut client, &sn, "UPDATE t SET v = 1, v = 2 WHERE id = 1");
    assert!(
        result.is_err(),
        "UPDATE with duplicate column assignment must be rejected"
    );
    // The rejected UPDATE must not have mutated the row.
    assert_eq!(
        payload_rows(&mut client, &sn, "t", &["id", "v"]),
        vec![vec![1, 10]],
        "rejected UPDATE leaves the row unchanged",
    );
}

#[test]
fn on_conflict_do_update_duplicate_column_is_rejected() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    exec(&mut client, &sn, "CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT)");
    let result = try_exec(
        &mut client,
        &sn,
        "INSERT INTO t VALUES (1, 1) ON CONFLICT (id) DO UPDATE SET v = 10, v = 20",
    );
    assert!(
        result.is_err(),
        "ON CONFLICT DO UPDATE with duplicate column assignment must be rejected"
    );
}

// ── Bug 7: INSERT value arity ────────────────────────────────────────

#[test]
fn insert_excess_values_is_rejected() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    exec(&mut client, &sn, "CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT)");
    let result = try_exec(&mut client, &sn, "INSERT INTO t VALUES (1, 10, 99)");
    assert!(result.is_err(), "INSERT with more values than columns must be rejected");
    assert!(
        payload_rows(&mut client, &sn, "t", &["id", "v"]).is_empty(),
        "the rejected INSERT must not have inserted a row",
    );
}

#[test]
fn insert_too_few_values_is_rejected() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    exec(&mut client, &sn, "CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT)");
    let result = try_exec(&mut client, &sn, "INSERT INTO t VALUES (1)");
    assert!(
        result.is_err(),
        "INSERT with fewer values than columns must be rejected"
    );
}

#[test]
fn insert_exact_column_count_succeeds() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    exec(&mut client, &sn, "CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT)");
    exec(&mut client, &sn, "INSERT INTO t VALUES (1, 10)");
    assert_eq!(payload_rows(&mut client, &sn, "t", &["id", "v"]), vec![vec![1, 10]],);
}
