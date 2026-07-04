#![cfg(feature = "integration")]

//! `CREATE INDEX` name-uniqueness regression tests (Bug 4). The SQL path pushed
//! directly to IDX_TAB with no duplicate-name guard, so two rows could share a
//! name; `drop_index_by_name` stops after the first match, orphaning the second.
//! `client.create_index` now rejects a duplicate name before allocating an id.

mod common;
use common::*;
use gnitz_sql::GnitzSqlError;
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

#[test]
fn unnamed_index_name_collision_disambiguated() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    exec(
        &mut client,
        &sn,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, a BIGINT, b_c BIGINT, a_b BIGINT, c BIGINT)",
    );
    // `(a, b_c)` and `(a_b, c)` both render `…__idx_a_b_c`; the second auto-name
    // is disambiguated to `…__idx_a_b_c_2`. Both indexes exist under distinct names.
    exec(&mut client, &sn, "CREATE INDEX ON t(a, b_c)");
    exec(&mut client, &sn, "CREATE INDEX ON t(a_b, c)");
    exec(&mut client, &sn, &format!("DROP INDEX {sn}__t__idx_a_b_c"));
    exec(&mut client, &sn, &format!("DROP INDEX {sn}__t__idx_a_b_c_2"));
}

#[test]
fn same_columns_auto_index_rejected() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    exec(&mut client, &sn, "CREATE TABLE t (id BIGINT PRIMARY KEY, a BIGINT)");
    exec(&mut client, &sn, "CREATE INDEX ON t(a)");
    // The identical column set already carries the auto base name → rejected by
    // the same-columns structural check, NOT silently disambiguated to `…__idx_a_2`.
    let result = try_exec(&mut client, &sn, "CREATE INDEX ON t(a)");
    match result {
        Err(GnitzSqlError::Plan(s)) => assert!(s.contains("already exists"), "got: {}", s),
        other => panic!("expected Plan(already exists), got {:?}", other),
    }
}

#[test]
fn client_gateway_rejects_invalid_index_name() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    exec(&mut client, &sn, "CREATE TABLE t (id BIGINT PRIMARY KEY, a BIGINT)");
    let (tid, schema) = client.resolve_table_id(&sn, "t").unwrap();
    // Bypass the planner: the client (catalog gateway) must itself reject a name
    // outside the ASCII identifier charset — non-SQL front ends (capi) reach it
    // without planner validation, and `canon_name`'s collision-free-fold premise
    // depends on that charset.
    let err = client.create_index(tid, &[1], &[schema.columns[1].type_code], "bad-name", false);
    assert!(err.is_err(), "client gateway must reject a non-identifier index name");
}

#[test]
fn same_columns_named_index_coexists() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    exec(&mut client, &sn, "CREATE TABLE t (id BIGINT PRIMARY KEY, a BIGINT)");
    // A named index on `a` does not carry the auto base name, so it neither blocks
    // the later auto-name nor is disturbed by it (the reject keys on
    // `(name == base) ∧ (cols == col_indices)`, not on columns alone).
    exec(&mut client, &sn, "CREATE INDEX my_idx ON t(a)");
    exec(&mut client, &sn, "CREATE INDEX ON t(a)");
    exec(&mut client, &sn, "DROP INDEX my_idx");
    exec(&mut client, &sn, &format!("DROP INDEX {sn}__t__idx_a"));
}
