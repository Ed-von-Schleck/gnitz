#![cfg(feature = "integration")]

//! `CREATE INDEX` name-uniqueness regression tests (Bug 4). The SQL path pushed
//! directly to IDX_TAB with no duplicate-name guard, so two rows could share a
//! name; `drop_index_by_name` stops after the first match, orphaning the second.
//! `client.create_index` now rejects a duplicate name before allocating an id.

mod common;
use common::*;
use gnitz_test_harness::ServerHandle;

#[test]
fn create_index_duplicate_name_is_rejected() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    exec(&mut client, &sn, "CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT)");
    exec(&mut client, &sn, "CREATE INDEX ON t(v)"); // first: ok
    let result = try_exec(&mut client, &sn, "CREATE INDEX ON t(v)"); // second: same auto-name
    assert!(
        result.is_err(),
        "second CREATE INDEX with the same auto-generated name must be rejected"
    );
}

#[test]
fn create_index_explicit_duplicate_name_is_rejected() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    exec(
        &mut client,
        &sn,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT, w BIGINT)",
    );
    exec(&mut client, &sn, "CREATE INDEX my_idx ON t(v)");
    let result = try_exec(&mut client, &sn, "CREATE INDEX my_idx ON t(w)");
    assert!(result.is_err(), "explicit duplicate index name must be rejected");
}

#[test]
fn create_index_distinct_names_succeed() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    exec(
        &mut client,
        &sn,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT, w BIGINT)",
    );
    exec(&mut client, &sn, "CREATE INDEX iv ON t(v)");
    // A distinct name on a different column must still be accepted.
    exec(&mut client, &sn, "CREATE INDEX iw ON t(w)");
}
