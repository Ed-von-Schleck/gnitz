#![cfg(feature = "integration")]

//! Planner-level tests for the on-demand (transient) query executor: an ad-hoc
//! `SELECT` that no thin client-side path can serve is compiled into the same
//! DBSP circuit a `CREATE VIEW` builds, run ONCE over the committed base
//! snapshot as an unregistered RAM-only relation, streamed back, and discarded.
//!
//! Scope: what is decided HERE, in the SQL layer — which shapes route to the
//! executor vs stay thin, which are rejected, and whether the planner's own error
//! surfaces survive the route. The distributed behaviour of the drive itself
//! (weight correctness per shuffle shape, the replicated-output guard, the
//! range-join relay, the drive/DDL exclusion) is covered end-to-end at
//! `GNITZ_WORKERS=4` in `gnitz-py/tests/test_transient_executor.py`, which is the
//! only place those can actually fail.

use gnitz_core::{GnitzClient, Schema, ZSetBatch};
use gnitz_sql::{SqlPlanner, SqlResult};
use gnitz_test_harness::ServerHandle;

mod common;
use common::*;

/// Run an ad-hoc SELECT and return its (schema, batch).
fn query(client: &mut GnitzClient, sn: &str, sql: &str) -> (Schema, ZSetBatch) {
    let mut p = SqlPlanner::new(client, sn);
    match p.execute(sql).unwrap().pop().unwrap() {
        SqlResult::Rows { schema, batch } => (schema, batch),
        _ => panic!("expected Rows from {sql:?}"),
    }
}

/// `(value, weight)` pairs of integer column `col`, sorted — comparable across
/// two runs regardless of emission order. Weights are kept: this is a Z-set
/// engine, and a distribution bug shows up as a weight multiple with every row
/// still "present".
fn col_weights(schema: &Schema, batch: &ZSetBatch, col: &str) -> Vec<(i64, i64)> {
    let ci = col_idx(schema, col);
    let mut out: Vec<(i64, i64)> = (0..batch.len())
        .filter(|&r| batch.weights[r] > 0)
        .map(|r| (cell_i64(schema, batch, ci, r), batch.weights[r]))
        .collect();
    out.sort();
    out
}

/// The visible column names of a result, lowercased.
fn visible_names(schema: &Schema) -> Vec<String> {
    schema
        .columns
        .iter()
        .filter(|c| !c.is_hidden)
        .map(|c| c.name.to_lowercase())
        .collect()
}

fn seed(client: &mut GnitzClient, sn: &str) {
    exec(
        client,
        sn,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, g BIGINT NOT NULL, s TEXT, f DOUBLE)",
    );
    exec(
        client,
        sn,
        "INSERT INTO t (id, g, s, f) VALUES \
         (1, 10, 'a', 1.5), (2, 10, 'b', 2.5), (3, 20, 'a', 1.5), \
         (4, 20, 'c', 3.5), (5, 30, 'b', 2.5), (6, 30, 'a', 4.5)",
    );
}

// ── The routed shapes agree with the view path ───────────────────────────────

/// A single-table GROUP BY routes to the executor and must agree with the
/// equivalent `CREATE VIEW` — same rows, same weights. The view is the trusted
/// oracle: the same circuit, driven by the maintained backfill instead.
#[test]
fn group_by_matches_view() {
    let Some(srv) = ServerHandle::start() else { return };
    let (mut client, sn) = make_planner(&srv);
    seed(&mut client, &sn);

    let sql = "SELECT g, COUNT(*) AS n FROM t GROUP BY g";
    exec(&mut client, &sn, &format!("CREATE VIEW v_gb AS {sql}"));
    let (vs, vb) = read_view(&mut client, &sn, "v_gb");
    let (ts, tb) = query(&mut client, &sn, sql);

    assert_eq!(
        col_weights(&ts, &tb, "g"),
        col_weights(&vs, &vb, "g"),
        "the ad-hoc executor must agree with CREATE VIEW on rows AND weights"
    );
    assert_eq!(
        col_weights(&ts, &tb, "n"),
        col_weights(&vs, &vb, "n"),
        "…including the aggregate column"
    );
}

/// A WHERE on a non-indexed STRING column: the thin path cannot evaluate this
/// residual, so it routes to the executor and must still be correct.
#[test]
fn where_on_string_column_routes_and_is_correct() {
    let Some(srv) = ServerHandle::start() else { return };
    let (mut client, sn) = make_planner(&srv);
    seed(&mut client, &sn);
    let (s, b) = query(&mut client, &sn, "SELECT id, s FROM t WHERE s = 'a'");
    assert_eq!(
        col_weights(&s, &b, "id"),
        vec![(1, 1), (3, 1), (6, 1)],
        "s = 'a' selects ids 1, 3, 6 exactly once each"
    );
}

/// A WHERE on a FLOAT column likewise routes to the executor.
#[test]
fn where_on_float_column_routes_and_is_correct() {
    let Some(srv) = ServerHandle::start() else { return };
    let (mut client, sn) = make_planner(&srv);
    seed(&mut client, &sn);
    let (s, b) = query(&mut client, &sn, "SELECT id FROM t WHERE f > 2.0");
    assert_eq!(
        col_weights(&s, &b, "id"),
        vec![(2, 1), (4, 1), (5, 1), (6, 1)],
        "f > 2.0 selects ids 2, 4, 5, 6"
    );
}

// ── Routing: thin vs executor ────────────────────────────────────────────────

/// A PK equality still serves on the THIN path — the executor must not swallow
/// the cheap point lookup.
#[test]
fn pk_equality_stays_thin_and_is_correct() {
    let Some(srv) = ServerHandle::start() else { return };
    let (mut client, sn) = make_planner(&srv);
    seed(&mut client, &sn);
    let (s, b) = query(&mut client, &sn, "SELECT id, g FROM t WHERE id = 5");
    assert_eq!(col_weights(&s, &b, "id"), vec![(5, 1)], "one row, weight 1");
}

/// `WHERE i64_col = 3.5` extracts no seek key (a non-i64-shaped literal), so it
/// falls through the same routing to the executor rather than hard-erroring. The
/// answer is the empty set — no BIGINT equals 3.5.
#[test]
fn non_integral_literal_against_int_column_routes_and_returns_empty() {
    let Some(srv) = ServerHandle::start() else { return };
    let (mut client, sn) = make_planner(&srv);
    seed(&mut client, &sn);
    let (s, b) = query(&mut client, &sn, "SELECT id FROM t WHERE g = 3.5");
    assert!(
        col_weights(&s, &b, "id").is_empty(),
        "no BIGINT equals 3.5 — the executor serves this as the empty set, not an error"
    );
}

// ── ORDER BY / LIMIT / OFFSET are SERVED on the executor path ────────────────

/// The shared client-side ordering sink applies over the streamed transient
/// result exactly as it does over a thin one — served, not rejected.
#[test]
fn executor_path_serves_order_by_limit_offset() {
    let Some(srv) = ServerHandle::start() else { return };
    let (mut client, sn) = make_planner(&srv);
    seed(&mut client, &sn);

    let (s, b) = query(
        &mut client,
        &sn,
        "SELECT g, COUNT(*) AS n FROM t GROUP BY g ORDER BY g DESC LIMIT 2",
    );
    let g = col_idx(&s, "g");
    let got: Vec<i64> = (0..b.len()).map(|r| i64_at(&b, g, r)).collect();
    assert_eq!(got, vec![30, 20], "ORDER BY g DESC LIMIT 2 over a GROUP BY transient");

    let (s, b) = query(
        &mut client,
        &sn,
        "SELECT g, COUNT(*) AS n FROM t GROUP BY g ORDER BY g DESC LIMIT 2 OFFSET 1",
    );
    let g = col_idx(&s, "g");
    let got: Vec<i64> = (0..b.len()).map(|r| i64_at(&b, g, r)).collect();
    assert_eq!(got, vec![20, 10], "OFFSET 1 shifts the window");
}

// ── Hidden synthetic-slot presentation ───────────────────────────────────────

/// A GROUP BY / DISTINCT / set-op adds a synthetic key column (`_group_pk` /
/// `_set_pk`). Presentation strips hidden columns, so the result is correct ONLY
/// IF the planner flags those slots hidden in `output_columns` — the same
/// contract a view scan relies on. A leaked synthetic key would surface an
/// internal hash to the user.
#[test]
fn synthetic_key_slots_are_hidden_from_user_rows() {
    let Some(srv) = ServerHandle::start() else { return };
    let (mut client, sn) = make_planner(&srv);
    seed(&mut client, &sn);

    for (sql, projected) in [
        ("SELECT g, COUNT(*) AS n FROM t GROUP BY g", vec!["g", "n"]),
        ("SELECT DISTINCT g FROM t", vec!["g"]),
        (
            "SELECT g FROM t WHERE g = 10 UNION SELECT g FROM t WHERE g = 30",
            vec!["g"],
        ),
    ] {
        let (s, _) = query(&mut client, &sn, sql);
        assert_eq!(
            visible_names(&s),
            projected,
            "only the projected columns may be visible for {sql:?}; any synthetic key must be hidden"
        );
    }
}

// ── Error surfaces survive the executor route ────────────────────────────────

/// Routing to the executor must not swallow or reshape a binder error: an
/// unknown column still surfaces as the planner's own error, not as a transient
/// decode/exec failure.
#[test]
fn binder_errors_survive_the_executor_route() {
    let Some(srv) = ServerHandle::start() else { return };
    let (mut client, sn) = make_planner(&srv);
    seed(&mut client, &sn);
    let err = try_exec(&mut client, &sn, "SELECT nosuchcol, COUNT(*) FROM t GROUP BY nosuchcol")
        .expect_err("unknown column must error");
    let msg = format!("{err:?}").to_lowercase();
    assert!(
        msg.contains("nosuchcol") || msg.contains("column"),
        "the binder's own error must survive the executor route, got: {err:?}"
    );
}

/// A shape the executor cannot serve (multi-segment: a 3-way join) is rejected by
/// the planner's `segments.len() == 1` guard and points at CREATE VIEW — never
/// silently mis-answered.
#[test]
fn multi_segment_query_is_rejected_not_misanswered() {
    let Some(srv) = ServerHandle::start() else { return };
    let (mut client, sn) = make_planner(&srv);
    seed(&mut client, &sn);
    exec(
        &mut client,
        &sn,
        "CREATE TABLE u (id BIGINT PRIMARY KEY, k BIGINT NOT NULL)",
    );
    exec(
        &mut client,
        &sn,
        "CREATE TABLE w (id BIGINT PRIMARY KEY, k BIGINT NOT NULL)",
    );
    let err = try_exec(
        &mut client,
        &sn,
        "SELECT t.id FROM t JOIN u ON t.g = u.k JOIN w ON u.k = w.k",
    )
    .expect_err("a 3-way join compiles to a multi-segment chain and must be rejected");
    assert!(
        format!("{err:?}").contains("CREATE VIEW"),
        "the rejection must point the user at CREATE VIEW, got: {err:?}"
    );
}

/// A crafted frame whose circuit names a TRANSIENT-band id as its `ScanDelta`
/// source must be rejected by the master before any drive. The SQL planner only
/// ever names durable relations; a band id could alias a CONCURRENT ad-hoc
/// query's in-flight transient (drives overlap), silently driving its
/// half-built store into this result.
#[test]
fn transient_band_scan_source_is_rejected() {
    let Some(srv) = ServerHandle::start() else { return };
    let (mut client, _sn) = make_planner(&srv);

    // Hand-built circuit (unreachable through the planner): one ScanDelta of
    // the first transient-band id (1 << 31, the engine's TRANSIENT_ID_BASE),
    // sunk.
    let mut cb = gnitz_core::CircuitBuilder::new(1, 1u64 << 31);
    let d = cb.input_delta();
    cb.sink(d);
    let schema = Schema::from_parts(
        vec![gnitz_core::ColumnDef::new("id", gnitz_core::TypeCode::U64, false)],
        vec![0],
    )
    .unwrap();

    let err = client
        .run_query(cb.build(), &schema)
        .expect_err("a transient-band ScanDelta source must be rejected, not driven");
    assert!(
        format!("{err:?}").contains("transient band"),
        "the rejection must name the cause, got: {err:?}"
    );
}

/// The same-relation INTERSECT/EXCEPT guard still fires on the executor route: a
/// single `source_id` feeding both sides of an anti-join in one epoch would break
/// the bilinear form, so the planner rejects it rather than mis-answer.
#[test]
fn same_relation_anti_join_is_still_rejected() {
    let Some(srv) = ServerHandle::start() else { return };
    let (mut client, sn) = make_planner(&srv);
    seed(&mut client, &sn);
    assert!(
        try_exec(&mut client, &sn, "SELECT g FROM t EXCEPT SELECT g FROM t WHERE g < 20").is_err(),
        "EXCEPT whose two inputs are the same relation must be rejected, not mis-answered"
    );
}

// ── Lifecycle ────────────────────────────────────────────────────────────────

/// Every drive tears down on every exit path and ids are never recycled, so
/// repeating an ad-hoc query must give an identical answer. A leaked store, a
/// stale cached plan, or a reused id would drift the weights.
#[test]
fn repeated_transients_are_stable() {
    let Some(srv) = ServerHandle::start() else { return };
    let (mut client, sn) = make_planner(&srv);
    seed(&mut client, &sn);
    let sql = "SELECT g, COUNT(*) AS n FROM t GROUP BY g";
    let (s0, b0) = query(&mut client, &sn, sql);
    let first = col_weights(&s0, &b0, "n");
    for i in 0..8 {
        let (s, b) = query(&mut client, &sn, sql);
        assert_eq!(col_weights(&s, &b, "n"), first, "run {i} drifted from run 0");
    }
}

/// A transient reads the committed base snapshot at drive time.
#[test]
fn transient_reflects_writes_between_runs() {
    let Some(srv) = ServerHandle::start() else { return };
    let (mut client, sn) = make_planner(&srv);
    seed(&mut client, &sn);
    let sql = "SELECT g, COUNT(*) AS n FROM t GROUP BY g HAVING g = 10";
    let (s, b) = query(&mut client, &sn, sql);
    assert_eq!(col_weights(&s, &b, "n"), vec![(2, 1)], "g=10 starts with 2 rows");

    exec(&mut client, &sn, "INSERT INTO t (id, g, s, f) VALUES (7, 10, 'z', 9.5)");
    let (s, b) = query(&mut client, &sn, sql);
    assert_eq!(
        col_weights(&s, &b, "n"),
        vec![(3, 1)],
        "the next transient must see the newly committed row"
    );
}
