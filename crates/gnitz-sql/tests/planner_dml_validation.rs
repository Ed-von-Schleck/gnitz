#![cfg(feature = "integration")]

//! DML arity/duplicate-assignment validation (Bugs 6 & 7).
//!
//! Bug 6: a column appearing twice in an UPDATE / ON CONFLICT DO UPDATE SET list
//! was silently first-wins; standard SQL rejects it.
//! Bug 7: an INSERT VALUES row with more expressions than columns silently
//! discarded the extras; standard SQL rejects it.

mod common;
use common::*;
use gnitz_sql::GnitzSqlError;
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

// ── C1: explicit INSERT column lists (accept in-order, reject reordered/partial)

#[test]
fn insert_reordered_column_list_is_rejected() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    exec(&mut client, &sn, "CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT)");
    let result = try_exec(&mut client, &sn, "INSERT INTO t (v, id) VALUES (10, 1)");
    assert!(
        result.is_err(),
        "INSERT with a reordered column list must be rejected (it would silently swap values)"
    );
    assert!(
        payload_rows(&mut client, &sn, "t", &["id", "v"]).is_empty(),
        "the rejected INSERT must not have inserted a row",
    );
}

#[test]
fn insert_in_order_column_list_succeeds() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    exec(&mut client, &sn, "CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT)");
    // A full column list in schema order is accepted (positional write is correct).
    exec(&mut client, &sn, "INSERT INTO t (id, v) VALUES (1, 10)");
    assert_eq!(payload_rows(&mut client, &sn, "t", &["id", "v"]), vec![vec![1, 10]]);
}

#[test]
fn insert_partial_column_list_is_rejected() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    exec(&mut client, &sn, "CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT)");
    let result = try_exec(&mut client, &sn, "INSERT INTO t (id) VALUES (1)");
    assert!(result.is_err(), "INSERT with a partial column list must be rejected");
}

/// `UPDATE … SET` range-checks its value exactly as INSERT does. It used to wrap
/// two's-complement (`SET tiny = 300` storing 44), so the same value had two
/// meanings depending on which verb wrote it.
#[test]
fn update_set_rejects_out_of_range_value() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    exec(
        &mut client,
        &sn,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, u8c TINYINT UNSIGNED, i16c SMALLINT, u64c BIGINT UNSIGNED)",
    );
    exec(&mut client, &sn, "INSERT INTO t VALUES (1, 1, 1, 1)");
    // The narrow columns are 1 and 2 bytes wide, so the i64 helpers cannot read
    // them; decode the two cells directly.
    fn narrow(client: &mut gnitz_core::GnitzClient, sn: &str) -> (i64, i64) {
        let (s, b) = read_sql(client, sn, "SELECT u8c, i16c FROM t");
        let fixed = |ci: usize, w: usize| match &b.columns[ci] {
            gnitz_core::ColData::Fixed(v) => v[..w].to_vec(),
            other => panic!("expected Fixed, got {other:?}"),
        };
        let u8v = fixed(col_idx(&s, "u8c"), 1)[0] as i64;
        let i16v = i16::from_le_bytes(fixed(col_idx(&s, "i16c"), 2).try_into().unwrap()) as i64;
        (u8v, i16v)
    }
    for sql in [
        "UPDATE t SET u8c = 300",
        "UPDATE t SET u8c = -1",
        "UPDATE t SET i16c = 40000",
        "UPDATE t SET u64c = -1",
        // A computed RHS runs through the VM and must range-check the same way.
        "UPDATE t SET u8c = u8c + 300",
    ] {
        let e = try_exec(&mut client, &sn, sql).unwrap_err();
        assert!(
            format!("{e:?}").contains("out of range"),
            "`{sql}` must be rejected as out of range, got {e:?}"
        );
    }
    // Nothing was written by any rejected statement.
    assert_eq!(narrow(&mut client, &sn), (1i64, 1i64));
    // In-range values still write, at every width and sign.
    exec(&mut client, &sn, "UPDATE t SET u8c = 255, i16c = -32768");
    assert_eq!(narrow(&mut client, &sn), (255i64, -32768i64));
}

/// Removing SET's wrap must not cost the upper half of U64: an integer literal
/// above `i64::MAX` binds as a wide literal, which the SET path previously could
/// not compile at all. `SET u64_col = 18446744073709551615` now writes the value
/// INSERT would, rather than being reachable only through the `-1` wrap.
#[test]
fn update_set_writes_the_full_u64_domain() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    exec(
        &mut client,
        &sn,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, u BIGINT UNSIGNED)",
    );
    exec(&mut client, &sn, "INSERT INTO t VALUES (1, 0)");
    exec(&mut client, &sn, "UPDATE t SET u = 18446744073709551615");
    let (s, b) = read_sql(&mut client, &sn, "SELECT u FROM t");
    let ci = col_idx(&s, "u");
    assert_eq!(
        cell_i64(&s, &b, ci, 0) as u64,
        u64::MAX,
        "SET reaches the full U64 domain, as INSERT does"
    );
    // A wide literal that does not fit the target is still rejected.
    let e = try_exec(&mut client, &sn, "UPDATE t SET u = 99999999999999999999999").unwrap_err();
    assert!(format!("{e:?}").contains("out of range"), "got {e:?}");
}
