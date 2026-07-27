#![cfg(feature = "integration")]

//! Planner-level tests for the ad-hoc `SELECT` surface: an ad-hoc SELECT reads
//! exactly one relation, served by the parameterized bounded read
//! (`plan_read_spec`) or, for aggregate / DISTINCT shapes, the fold sink
//! (`execute_aggregate_select`). A query that *derives* a new relation — a JOIN, a
//! set operation, an EXISTS/IN or scalar subquery, a derived table, a
//! non-pass-through CTE — is rejected from the AST alone with one actionable
//! message pointing at CREATE VIEW. A single-relation read the direct path cannot
//! express (a LIKE / string-function WHERE, an ORDER BY expression) is a
//! feature-named `Unsupported`, never the derivation template.
//!
//! Scope: what is decided HERE, in the SQL layer — which shapes are served, which
//! are rejected and with which message. The distributed correctness of the read /
//! fold itself (weight correctness per shuffle shape) is covered end-to-end at
//! `GNITZ_WORKERS=4` in the `gnitz-py` suites.

use gnitz_core::{GnitzClient, Schema, ZSetBatch};
use gnitz_sql::{GnitzSqlError, SqlPlanner, SqlResult};
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

/// Assert `sql` is rejected as a §2 derivation with the given `construct`: the
/// exact template naming the construct once, and pointing at CREATE VIEW.
fn assert_derivation_rejection(client: &mut GnitzClient, sn: &str, sql: &str, construct: &str) {
    let err = try_exec(client, sn, sql).expect_err(&format!("{sql:?} must be rejected as a derivation"));
    let GnitzSqlError::Unsupported(msg) = &err else {
        panic!("expected Unsupported for {sql:?}, got {err:?}");
    };
    assert!(
        msg.contains("this query derives a new one"),
        "must use the derivation template for {sql:?}, got: {msg}"
    );
    assert!(
        msg.contains(&format!("({construct})")),
        "must name the construct '{construct}' for {sql:?}, got: {msg}"
    );
    assert_eq!(
        msg.matches(construct).count(),
        1,
        "the construct name must appear exactly once for {sql:?}, got: {msg}"
    );
    assert!(
        msg.contains("CREATE VIEW"),
        "the remedy must point at CREATE VIEW for {sql:?}, got: {msg}"
    );
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

/// A second table, for JOIN / set-op / subquery rejection shapes.
fn seed_u(client: &mut GnitzClient, sn: &str) {
    exec(client, sn, "CREATE TABLE u (id BIGINT PRIMARY KEY, k BIGINT NOT NULL)");
    exec(client, sn, "INSERT INTO u (id, k) VALUES (1, 10), (2, 20)");
}

// ── Served shapes: agreement with the view path ──────────────────────────────

/// A single-table GROUP BY folds via the ReadSpec fold sink and must agree with
/// the equivalent `CREATE VIEW` — same rows, same weights. The view is the trusted
/// oracle: the same reduce, driven by the maintained backfill instead.
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
        "the ad-hoc fold must agree with CREATE VIEW on rows AND weights"
    );
    assert_eq!(
        col_weights(&ts, &tb, "n"),
        col_weights(&vs, &vb, "n"),
        "…including the aggregate column"
    );
}

/// A WHERE on a non-indexed STRING column compiles into the server-side read-spec
/// predicate and must still be correct.
#[test]
fn where_on_string_column_served_and_is_correct() {
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

/// A WHERE on a FLOAT column likewise compiles into the read-spec predicate.
#[test]
fn where_on_float_column_served_and_is_correct() {
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

/// A PK equality serves via the bounded read — the cheap point lookup is not lost.
#[test]
fn pk_equality_stays_thin_and_is_correct() {
    let Some(srv) = ServerHandle::start() else { return };
    let (mut client, sn) = make_planner(&srv);
    seed(&mut client, &sn);
    let (s, b) = query(&mut client, &sn, "SELECT id, g FROM t WHERE id = 5");
    assert_eq!(col_weights(&s, &b, "id"), vec![(5, 1)], "one row, weight 1");
}

/// `WHERE i64_col = 3.5` extracts no seek key (a non-i64-shaped literal), so the
/// whole predicate runs server-side. The answer is the empty set — no BIGINT
/// equals 3.5 — not an error.
#[test]
fn non_integral_literal_against_int_column_served_and_returns_empty() {
    let Some(srv) = ServerHandle::start() else { return };
    let (mut client, sn) = make_planner(&srv);
    seed(&mut client, &sn);
    let (s, b) = query(&mut client, &sn, "SELECT id FROM t WHERE g = 3.5");
    assert!(
        col_weights(&s, &b, "id").is_empty(),
        "no BIGINT equals 3.5 — served as the empty set, not an error"
    );
}

/// ORDER BY / LIMIT / OFFSET apply as the shared client-side ordering sink over
/// the fold result.
#[test]
fn order_by_limit_offset_served() {
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
    assert_eq!(got, vec![30, 20], "ORDER BY g DESC LIMIT 2 over a GROUP BY fold");

    let (s, b) = query(
        &mut client,
        &sn,
        "SELECT g, COUNT(*) AS n FROM t GROUP BY g ORDER BY g DESC LIMIT 2 OFFSET 1",
    );
    let g = col_idx(&s, "g");
    let got: Vec<i64> = (0..b.len()).map(|r| i64_at(&b, g, r)).collect();
    assert_eq!(got, vec![20, 10], "OFFSET 1 shifts the window");
}

/// A GROUP BY / DISTINCT / (now-rejected set-op via view) adds a synthetic key
/// column (`_group_pk` / `_set_pk`). Presentation strips hidden columns, so the
/// result is correct ONLY IF the planner flags those slots hidden. A leaked
/// synthetic key would surface an internal hash to the user.
#[test]
fn synthetic_key_slots_are_hidden_from_user_rows() {
    let Some(srv) = ServerHandle::start() else { return };
    let (mut client, sn) = make_planner(&srv);
    seed(&mut client, &sn);

    for (sql, projected) in [
        ("SELECT g, COUNT(*) AS n FROM t GROUP BY g", vec!["g", "n"]),
        ("SELECT DISTINCT g FROM t", vec!["g"]),
    ] {
        let (s, _) = query(&mut client, &sn, sql);
        assert_eq!(
            visible_names(&s),
            projected,
            "only the projected columns may be visible for {sql:?}; any synthetic key must be hidden"
        );
    }
}

// ── Error surfaces ───────────────────────────────────────────────────────────

/// A binder error (unknown column) surfaces as the planner's own error.
#[test]
fn binder_errors_surface() {
    let Some(srv) = ServerHandle::start() else { return };
    let (mut client, sn) = make_planner(&srv);
    seed(&mut client, &sn);
    let err = try_exec(&mut client, &sn, "SELECT nosuchcol, COUNT(*) FROM t GROUP BY nosuchcol")
        .expect_err("unknown column must error");
    let msg = format!("{err:?}").to_lowercase();
    assert!(
        msg.contains("nosuchcol") || msg.contains("column"),
        "the binder's own error must surface, got: {err:?}"
    );
}

// ── Derivation rejections (§2 template, each construct named once) ────────────

#[test]
fn join_is_rejected_as_derivation() {
    let Some(srv) = ServerHandle::start() else { return };
    let (mut client, sn) = make_planner(&srv);
    seed(&mut client, &sn);
    seed_u(&mut client, &sn);
    assert_derivation_rejection(&mut client, &sn, "SELECT t.id FROM t JOIN u ON t.g = u.k", "JOIN");
}

/// A comma-join is a shape CREATE VIEW rejects too, so the derivation template's
/// "CREATE VIEW <name> AS <your query>" advice would be false for it: it gets its
/// own message advising the explicit-JOIN rewrite instead.
#[test]
fn comma_join_gets_the_rewrite_advice_not_the_template() {
    let Some(srv) = ServerHandle::start() else { return };
    let (mut client, sn) = make_planner(&srv);
    seed(&mut client, &sn);
    seed_u(&mut client, &sn);
    let err =
        try_exec(&mut client, &sn, "SELECT t.id FROM t, u WHERE t.g = u.k").expect_err("a comma-join must be rejected");
    let GnitzSqlError::Unsupported(msg) = &err else {
        panic!("expected Unsupported, got {err:?}");
    };
    assert!(
        !msg.contains("this query derives a new one"),
        "a comma-join must not use the derivation template (its advice would be false), got: {msg}"
    );
    assert!(
        msg.contains("comma-join") && msg.contains("explicit JOIN") && msg.contains("CREATE VIEW"),
        "the comma-join rejection must advise the explicit-JOIN + CREATE VIEW rewrite, got: {msg}"
    );
}

#[test]
fn set_operation_is_rejected_as_derivation() {
    let Some(srv) = ServerHandle::start() else { return };
    let (mut client, sn) = make_planner(&srv);
    seed(&mut client, &sn);
    for sql in [
        "SELECT g FROM t WHERE g = 10 UNION SELECT g FROM t WHERE g = 30",
        "SELECT g FROM t INTERSECT SELECT g FROM t WHERE g < 30",
        "SELECT g FROM t EXCEPT SELECT g FROM t WHERE g < 20",
    ] {
        assert_derivation_rejection(&mut client, &sn, sql, "set operation");
    }
}

#[test]
fn exists_in_subquery_is_rejected_as_derivation() {
    let Some(srv) = ServerHandle::start() else { return };
    let (mut client, sn) = make_planner(&srv);
    seed(&mut client, &sn);
    seed_u(&mut client, &sn);
    // A clean top-level EXISTS conjunct (the semi/anti-join fast path shape).
    assert_derivation_rejection(
        &mut client,
        &sn,
        "SELECT id FROM t WHERE EXISTS (SELECT 1 FROM u WHERE u.k = t.g)",
        "EXISTS/IN subquery",
    );
    // An IN subquery.
    assert_derivation_rejection(
        &mut client,
        &sn,
        "SELECT id FROM t WHERE g IN (SELECT k FROM u)",
        "EXISTS/IN subquery",
    );
    // A subquery in an arbitrary boolean position (the mark shape).
    assert_derivation_rejection(
        &mut client,
        &sn,
        "SELECT id FROM t WHERE g = 10 OR EXISTS (SELECT 1 FROM u WHERE u.k = t.g)",
        "EXISTS/IN subquery",
    );
}

/// An anti-join-shaped query (NOT EXISTS) rejects with the §2 template — the
/// planner rejects from the AST, so the old same-relation source-id hazard never
/// arises.
#[test]
fn anti_join_is_rejected_as_derivation() {
    let Some(srv) = ServerHandle::start() else { return };
    let (mut client, sn) = make_planner(&srv);
    seed(&mut client, &sn);
    seed_u(&mut client, &sn);
    assert_derivation_rejection(
        &mut client,
        &sn,
        "SELECT id FROM t WHERE NOT EXISTS (SELECT 1 FROM u WHERE u.k = t.g)",
        "EXISTS/IN subquery",
    );
}

#[test]
fn scalar_subquery_is_rejected_as_derivation() {
    let Some(srv) = ServerHandle::start() else { return };
    let (mut client, sn) = make_planner(&srv);
    seed(&mut client, &sn);
    seed_u(&mut client, &sn);
    // A scalar aggregate subquery in the projection.
    assert_derivation_rejection(
        &mut client,
        &sn,
        "SELECT id, (SELECT MAX(k) FROM u) FROM t",
        "scalar subquery",
    );
}

#[test]
fn derived_table_in_from_is_rejected_as_derivation() {
    let Some(srv) = ServerHandle::start() else { return };
    let (mut client, sn) = make_planner(&srv);
    seed(&mut client, &sn);
    assert_derivation_rejection(
        &mut client,
        &sn,
        "SELECT x FROM (SELECT g AS x FROM t) d",
        "derived table in FROM",
    );
}

// ── Feature-named rejections (§4 — NOT the derivation template) ───────────────

/// A single-relation read using a feature the direct path cannot express is a
/// feature-named `Unsupported`, never the derivation template.
fn assert_feature_rejection(client: &mut GnitzClient, sn: &str, sql: &str) -> String {
    let err = try_exec(client, sn, sql).expect_err(&format!("{sql:?} must be rejected"));
    let GnitzSqlError::Unsupported(msg) = &err else {
        panic!("expected Unsupported for {sql:?}, got {err:?}");
    };
    assert!(
        !msg.contains("this query derives a new one"),
        "a feature limit must NOT use the derivation template for {sql:?}, got: {msg}"
    );
    msg.clone()
}

#[test]
fn like_where_is_a_feature_rejection() {
    let Some(srv) = ServerHandle::start() else { return };
    let (mut client, sn) = make_planner(&srv);
    seed(&mut client, &sn);
    assert_feature_rejection(&mut client, &sn, "SELECT id FROM t WHERE s LIKE 'a%'");
}

#[test]
fn order_by_expression_is_a_feature_rejection() {
    let Some(srv) = ServerHandle::start() else { return };
    let (mut client, sn) = make_planner(&srv);
    seed(&mut client, &sn);
    assert_feature_rejection(&mut client, &sn, "SELECT id FROM t ORDER BY id + 1");
}

/// An ad-hoc HAVING compiles through the same expression compiler a grouped
/// view's post-reduce FILTER uses, so a string HAVING is served and what is left
/// rejected is exactly what CREATE VIEW rejects too — with the compiler's own
/// message, not advice to build a view.
#[test]
fn having_feature_limits_are_compiler_errors() {
    let Some(srv) = ServerHandle::start() else { return };
    let (mut client, sn) = make_planner(&srv);
    seed(&mut client, &sn);

    let (s, b) = query(
        &mut client,
        &sn,
        "SELECT s, COUNT(*) AS n FROM t GROUP BY s HAVING s = 'a'",
    );
    assert_eq!(col_weights(&s, &b, "n"), vec![(3, 1)], "s = 'a' groups 3 rows");

    // A 128-bit column has no VM register, on either path.
    exec(
        &mut client,
        &sn,
        "CREATE TABLE tw (id BIGINT PRIMARY KEY, w DECIMAL(38,0))",
    );
    exec(&mut client, &sn, "INSERT INTO tw (id, w) VALUES (1, 1), (2, 2)");
    let msg = assert_feature_rejection(&mut client, &sn, "SELECT w, COUNT(*) FROM tw GROUP BY w HAVING w > 5");
    assert!(
        msg.contains("128-bit") && msg.contains("\"w\""),
        "the rejection must name the offending column and the limitation, got: {msg}"
    );
    assert!(
        !msg.contains("CREATE VIEW") && !msg.contains("primary-key seek"),
        "a HAVING the shared compiler rejects must not advise a view or a PK seek, got: {msg}"
    );
}

// ── Pass-through CTEs read through the direct path ───────────────────────────

/// A pass-through CTE over a table inlines to the base table and reads via the
/// read spec — WHERE over the aliased CTE still filters.
#[test]
fn passthrough_cte_over_table_served() {
    let Some(srv) = ServerHandle::start() else { return };
    let (mut client, sn) = make_planner(&srv);
    seed(&mut client, &sn);
    let (s, b) = query(
        &mut client,
        &sn,
        "WITH x AS (SELECT * FROM t) SELECT id FROM x WHERE g = 10",
    );
    assert_eq!(
        col_weights(&s, &b, "id"),
        vec![(1, 1), (2, 1)],
        "the CTE inlines to t and the WHERE filters g = 10"
    );
}

/// A pass-through CTE over a VIEW inlines to that view and reads through it.
#[test]
fn passthrough_cte_over_view_served() {
    let Some(srv) = ServerHandle::start() else { return };
    let (mut client, sn) = make_planner(&srv);
    seed(&mut client, &sn);
    exec(
        &mut client,
        &sn,
        "CREATE VIEW v_ge20 AS SELECT id, g FROM t WHERE g >= 20",
    );
    let (s, b) = query(
        &mut client,
        &sn,
        "WITH x AS (SELECT * FROM v_ge20) SELECT id FROM x WHERE g = 30",
    );
    assert_eq!(
        col_weights(&s, &b, "id"),
        vec![(5, 1), (6, 1)],
        "the CTE inlines to the view and the WHERE filters g = 30"
    );
}

/// A non-pass-through CTE (a WHERE'd body) derives, and one such CTE rejects the
/// whole query with the §2 template.
#[test]
fn non_passthrough_cte_is_rejected_as_derivation() {
    let Some(srv) = ServerHandle::start() else { return };
    let (mut client, sn) = make_planner(&srv);
    seed(&mut client, &sn);
    assert_derivation_rejection(
        &mut client,
        &sn,
        "WITH x AS (SELECT id FROM t WHERE g = 10) SELECT id FROM x",
        "non-pass-through CTE",
    );
}

/// A CTE clause CREATE VIEW cannot serve either (DISTINCT, the exotic tail)
/// keeps its targeted clause error — the derivation template's CREATE VIEW
/// advice would be false for it.
#[test]
fn unsupported_cte_clause_keeps_its_targeted_error() {
    let Some(srv) = ServerHandle::start() else { return };
    let (mut client, sn) = make_planner(&srv);
    seed(&mut client, &sn);
    let msg = assert_feature_rejection(&mut client, &sn, "WITH x AS (SELECT DISTINCT g FROM t) SELECT g FROM x");
    assert!(
        msg.contains("CTE 'x'") && msg.contains("DISTINCT"),
        "the rejection must name the CTE and the offending clause, got: {msg}"
    );
}

// ── Lifecycle ────────────────────────────────────────────────────────────────

/// Repeating an ad-hoc read gives an identical answer — the read holds no
/// per-query server state.
#[test]
fn repeated_reads_are_stable() {
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

/// An ad-hoc read sees the committed base snapshot at read time.
#[test]
fn reads_reflect_writes_between_runs() {
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
        "the next read must see the newly committed row"
    );
}
