#![cfg(feature = "integration")]

//! Three-valued-logic (3VL) regression tests for `AND`/`OR` in residual WHERE
//! predicates (Bug 3). `eval_expr` must short-circuit `TRUE OR any → TRUE` and
//! `FALSE AND any → FALSE` *before* propagating a NULL operand, instead of
//! eagerly returning NULL the moment either side is NULL.
//!
//! Exercised through `DELETE … WHERE`: an `OR` at the top of the predicate
//! cannot be decomposed into a PK seek, so it routes to the full-scan path that
//! calls `eval_expr`; the `AND` case is observed under `NOT`, where FALSE and
//! NULL produce different outcomes.

mod common;
use common::*;
use gnitz_test_harness::ServerHandle;

#[test]
fn or_with_null_lhs_uses_true_rhs() {
    // val IS NULL for every row. `val = 5 OR id = 1`:
    //   id=1 → NULL OR TRUE  = TRUE  → deleted
    //   id=2 → NULL OR FALSE = NULL  → kept
    // The eager-NULL bug returns NULL for both and deletes nothing.
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    exec(&mut client, &sn, "CREATE TABLE t (id BIGINT PRIMARY KEY, val BIGINT)");
    exec(&mut client, &sn, "INSERT INTO t VALUES (1, NULL)");
    exec(&mut client, &sn, "INSERT INTO t VALUES (2, NULL)");

    let n = affected(&mut client, &sn, "DELETE FROM t WHERE val = 5 OR id = 1");
    assert_eq!(n, 1, "NULL OR TRUE deletes id=1; NULL OR FALSE keeps id=2");
    assert_eq!(
        payload_rows(&mut client, &sn, "t", &["id"]),
        vec![vec![2]],
        "only id=2 (NULL OR FALSE = NULL) survives",
    );
}

#[test]
fn or_with_null_rhs_uses_true_lhs() {
    // `id = 1 OR val = 5` for row (1, NULL): TRUE OR NULL = TRUE → deleted.
    // The eager-NULL bug evaluates the NULL RHS and returns NULL → not deleted.
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    exec(&mut client, &sn, "CREATE TABLE t (id BIGINT PRIMARY KEY, val BIGINT)");
    exec(&mut client, &sn, "INSERT INTO t VALUES (1, NULL)");

    let n = affected(&mut client, &sn, "DELETE FROM t WHERE id = 1 OR val = 5");
    assert_eq!(n, 1, "TRUE OR NULL is TRUE → row deleted");
    assert!(
        payload_rows(&mut client, &sn, "t", &["id"]).is_empty(),
        "the only row is removed",
    );
}

#[test]
fn false_and_null_is_false_under_not() {
    // Row (1, a=0, b=NULL). `NOT (a = 5 AND b = 10)`:
    //   a=5 → FALSE, b=10 → NULL.  FALSE AND NULL = FALSE; NOT FALSE = TRUE → deleted.
    // The eager-NULL bug makes the inner AND = NULL, so NOT NULL = NULL → not deleted.
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
    exec(&mut client, &sn, "INSERT INTO t VALUES (1, 0, NULL)");

    let n = affected(&mut client, &sn, "DELETE FROM t WHERE NOT (a = 5 AND b = 10)");
    assert_eq!(n, 1, "FALSE AND NULL = FALSE → NOT FALSE = TRUE → row deleted");
    assert!(
        payload_rows(&mut client, &sn, "t", &["id"]).is_empty(),
        "the only row is removed",
    );
}

#[test]
fn null_and_true_is_null_excludes_row() {
    // Row (1, a=NULL, b=1). `a = 5 AND b = 1`: NULL AND TRUE = NULL → not deleted.
    // (Both pre- and post-fix exclude the row; this guards the NULL→excluded path.)
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
    exec(&mut client, &sn, "INSERT INTO t VALUES (1, NULL, 1)");

    let n = affected(&mut client, &sn, "DELETE FROM t WHERE a = 5 AND b = 1");
    assert_eq!(n, 0, "NULL AND TRUE is NULL (UNKNOWN) → row excluded");
    assert_eq!(
        payload_rows(&mut client, &sn, "t", &["id"]),
        vec![vec![1]],
        "the row is untouched",
    );
}
