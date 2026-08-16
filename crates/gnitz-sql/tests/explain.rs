#![cfg(feature = "integration")]

//! `EXPLAIN <select>` — the access decisions the ad-hoc read path makes, as rows.
//!
//! EXPLAIN runs the *identical* planning `execute_select` runs and diverges only
//! at the tail, so these tests pin two things: the line vocabulary (what each
//! access strategy, projection, fold and ordering decision is called), and the
//! agreement of the rejections — a query EXPLAIN cannot describe returns exactly
//! the error executing it returns.

use gnitz_core::{ColData, GnitzClient};
use gnitz_sql::GnitzSqlError;
use gnitz_test_harness::ServerHandle;

mod common;
use common::*;

/// The `plan` column of an EXPLAIN reply, in row order. The reply's PK is the
/// hidden 1-based line number, so the batch order IS the line order.
fn explain(client: &mut GnitzClient, sn: &str, sql: &str) -> Vec<String> {
    let (schema, batch) = read_sql(client, sn, sql);
    assert_eq!(
        visible_names(&schema),
        vec!["plan".to_string()],
        "EXPLAIN presents one visible column"
    );
    let ci = col_idx(&schema, "plan");
    match &batch.columns[ci] {
        ColData::Strings(v) => v.iter().map(|s| s.clone().expect("no EXPLAIN line is NULL")).collect(),
        other => panic!("expected a Strings column, got {:?}", std::mem::discriminant(other)),
    }
}

/// The one line starting with `prefix` — used when a test cares about a single
/// decision and not the whole plan.
fn line(client: &mut GnitzClient, sn: &str, sql: &str, prefix: &str) -> String {
    let lines = explain(client, sn, sql);
    lines
        .iter()
        .find(|l| l.starts_with(prefix))
        .unwrap_or_else(|| panic!("no `{prefix}` line in {lines:?} for `{sql}`"))
        .clone()
}

/// `(id U64 pk, v U64, w U64)` with a secondary index on `v` — every access rung
/// is reachable from this one table.
fn seed(client: &mut GnitzClient, sn: &str) {
    exec(
        client,
        sn,
        "CREATE TABLE t (id BIGINT UNSIGNED PRIMARY KEY, v BIGINT UNSIGNED NOT NULL, w BIGINT UNSIGNED NOT NULL)",
    );
    exec(client, sn, "CREATE INDEX t_v ON t (v)");
    exec(
        client,
        sn,
        "INSERT INTO t (id, v, w) VALUES (1, 10, 100), (2, 20, 200), (3, 10, 300)",
    );
}

// ── Access ───────────────────────────────────────────────────────────────────

/// Every WHERE shape names the walk it takes. These are the decisions a user
/// cannot otherwise see: whether `WHERE id = 5` became a point lookup or a scan,
/// and whether an index was used.
#[test]
fn each_where_shape_names_its_access() {
    let Some(srv) = ServerHandle::start() else { return };
    let (mut client, sn) = make_planner(&srv);
    seed(&mut client, &sn);

    for (sql, want) in [
        ("SELECT v FROM t", "access: full scan"),
        ("SELECT v FROM t WHERE id = 5", "access: pk point lookup"),
        ("SELECT v FROM t WHERE id > 5", "access: pk range walk"),
        // `try_extract_pk_in` deduplicates, so this gathers 2 keys, not 3.
        (
            "SELECT v FROM t WHERE id IN (1, 1, 2)",
            "access: pk set gather (2 keys)",
        ),
        // The conjuncts still ride the predicate, so the worker's selectivity
        // gate may drop the walk.
        (
            "SELECT w FROM t WHERE v = 5",
            "access: index range on (v) — may be traded for a full scan on low selectivity",
        ),
        // A literal past the VM's `i64` constant has no compiled form, so the
        // conjunct is stripped and the walk must apply it.
        (
            "SELECT w FROM t WHERE v = 18446744073709551615",
            "access: index range on (v) — exact walk, never traded",
        ),
    ] {
        assert_eq!(
            line(&mut client, &sn, &format!("EXPLAIN {sql}"), "access:"),
            want,
            "{sql}"
        );
    }
}

/// A compound PK: an equality on the leading column alone names a key *group* —
/// a range walk — while pinning every column names one key.
#[test]
fn a_compound_pk_prefix_is_a_range_and_the_full_key_is_a_point() {
    let Some(srv) = ServerHandle::start() else { return };
    let (mut client, sn) = make_planner(&srv);
    exec(
        &mut client,
        &sn,
        "CREATE TABLE c (a BIGINT UNSIGNED NOT NULL, b BIGINT UNSIGNED NOT NULL, x BIGINT, PRIMARY KEY (a, b))",
    );

    assert_eq!(
        line(&mut client, &sn, "EXPLAIN SELECT x FROM c WHERE a = 1", "access:"),
        "access: pk range walk"
    );
    assert_eq!(
        line(
            &mut client,
            &sn,
            "EXPLAIN SELECT x FROM c WHERE a = 1 AND b = 2",
            "access:"
        ),
        "access: pk point lookup"
    );
}

/// The predicate line reports presence, not a conjunct count: an exact bound
/// ships no predicate at all, a bound with anything left over ships one.
#[test]
fn the_predicate_line_reports_whether_one_ships() {
    let Some(srv) = ServerHandle::start() else { return };
    let (mut client, sn) = make_planner(&srv);
    seed(&mut client, &sn);

    for (sql, want) in [
        ("SELECT v FROM t WHERE id = 5", "predicate: none"),
        ("SELECT v FROM t WHERE id IN (1, 2)", "predicate: none"),
        ("SELECT v FROM t WHERE v = 18446744073709551615", "predicate: none"),
        ("SELECT v FROM t WHERE id = 5 AND w > 5", "predicate: server-side"),
        // The non-exact index bound keeps the whole WHERE in the predicate.
        ("SELECT w FROM t WHERE v = 5", "predicate: server-side"),
    ] {
        assert_eq!(
            line(&mut client, &sn, &format!("EXPLAIN {sql}"), "predicate:"),
            want,
            "{sql}"
        );
    }
}

// ── Projection, ordering, and the whole-plan shape ───────────────────────────

/// The bare `SELECT *` scan — the one route that builds no `ReadSpec`, which the
/// access line names — and the full line list it prints. Projecting the same
/// columns explicitly takes the ordinary read-spec path over the same full scan.
#[test]
fn a_bare_wildcard_scan_prints_the_whole_plan() {
    let Some(srv) = ServerHandle::start() else { return };
    let (mut client, sn) = make_planner(&srv);
    seed(&mut client, &sn);

    assert_eq!(
        explain(&mut client, &sn, "EXPLAIN SELECT * FROM t"),
        vec![
            "read table t",
            "access: full scan (unprojected)",
            "predicate: none",
            "projection: 3 columns",
            "order/limit: none",
        ]
    );
    assert_eq!(
        line(&mut client, &sn, "EXPLAIN SELECT id, v, w FROM t", "access:"),
        "access: full scan"
    );
}

/// The projection counts the columns the client sees. An ORDER BY on a
/// non-projected source column adds a hidden reply column, reported separately so
/// the visible count still matches the SELECT list.
#[test]
fn the_projection_counts_visible_columns_and_ordering_extras() {
    let Some(srv) = ServerHandle::start() else { return };
    let (mut client, sn) = make_planner(&srv);
    seed(&mut client, &sn);

    assert_eq!(
        line(&mut client, &sn, "EXPLAIN SELECT v, w FROM t", "projection:"),
        "projection: 2 columns"
    );
    assert_eq!(
        line(
            &mut client,
            &sn,
            "EXPLAIN SELECT id, v FROM t ORDER BY w",
            "projection:"
        ),
        "projection: 2 columns (+1 for ordering)"
    );
}

/// Where the ORDER BY / LIMIT / OFFSET work happens: a per-worker cut, a
/// per-worker early stop, the client's sort of the concatenated replies, and the
/// client's window.
#[test]
fn order_and_limit_name_which_side_does_the_work() {
    let Some(srv) = ServerHandle::start() else { return };
    let (mut client, sn) = make_planner(&srv);
    seed(&mut client, &sn);

    for (sql, want) in [
        // The per-worker cut is OFFSET+LIMIT deep, because the client windows.
        (
            "SELECT v FROM t ORDER BY v LIMIT 10 OFFSET 2",
            "order/limit: server top-12, client sort, client window",
        ),
        (
            "SELECT v FROM t LIMIT 10",
            "order/limit: server early-stop 10, client window",
        ),
        ("SELECT v FROM t ORDER BY v", "order/limit: client sort"),
        ("SELECT v FROM t", "order/limit: none"),
        // All fold finishing is client-side.
        (
            "SELECT v, COUNT(*) FROM t GROUP BY v ORDER BY 1 LIMIT 5",
            "order/limit: client sort, client window",
        ),
        // `LIMIT 0` is answered from the reply schema alone — both sinks
        // short-circuit before dispatching, so no request is described.
        ("SELECT v FROM t LIMIT 0", "order/limit: no request (LIMIT 0)"),
        (
            "SELECT v FROM t ORDER BY v LIMIT 0 OFFSET 3",
            "order/limit: no request (LIMIT 0)",
        ),
        ("SELECT COUNT(*) FROM t LIMIT 0", "order/limit: no request (LIMIT 0)"),
    ] {
        assert_eq!(
            line(&mut client, &sn, &format!("EXPLAIN {sql}"), "order/limit:"),
            want,
            "{sql}"
        );
    }
}

// ── Folds ────────────────────────────────────────────────────────────────────

/// The fold line names the physical reduce the worker runs — which is why AVG
/// shows as its `SUM` + `COUNT_NON_NULL` pair rather than as `AVG`.
#[test]
fn the_fold_line_names_the_physical_reduce() {
    let Some(srv) = ServerHandle::start() else { return };
    let (mut client, sn) = make_planner(&srv);
    seed(&mut client, &sn);

    for (sql, want) in [
        ("SELECT COUNT(*) FROM t", "fold: global aggregate: COUNT(*)"),
        (
            "SELECT v, SUM(w), AVG(w) FROM t GROUP BY v",
            "fold: group by (v): SUM(w), SUM(w), COUNT_NON_NULL(w)",
        ),
        ("SELECT v FROM t GROUP BY v", "fold: group by (v)"),
        ("SELECT DISTINCT v, w FROM t", "fold: distinct on (v, w)"),
        (
            "SELECT v, COUNT(*) FROM t GROUP BY v HAVING COUNT(*) > 1",
            "fold: group by (v): COUNT(*); HAVING applied client-side",
        ),
    ] {
        assert_eq!(
            line(&mut client, &sn, &format!("EXPLAIN {sql}"), "fold:"),
            want,
            "{sql}"
        );
    }
}

/// A fold plan prints no projection line — the fold's own shape IS its output —
/// and still carries its access decision.
#[test]
fn a_fold_plan_prints_its_access_but_no_projection() {
    let Some(srv) = ServerHandle::start() else { return };
    let (mut client, sn) = make_planner(&srv);
    seed(&mut client, &sn);

    assert_eq!(
        explain(&mut client, &sn, "EXPLAIN SELECT COUNT(*) FROM t WHERE id = 5"),
        vec![
            "read table t",
            "access: pk point lookup",
            "predicate: none",
            "fold: global aggregate: COUNT(*)",
            "order/limit: none",
        ]
    );
}

// ── Relation kind ────────────────────────────────────────────────────────────

/// A view read can drain the pending ticks of its source closure before serving.
/// EXPLAIN cannot know staleness at plan time, so line 1 names the condition.
#[test]
fn a_view_read_names_its_tick_drain() {
    let Some(srv) = ServerHandle::start() else { return };
    let (mut client, sn) = make_planner(&srv);
    seed(&mut client, &sn);
    exec(&mut client, &sn, "CREATE VIEW tv AS SELECT id, v FROM t WHERE w > 0");

    assert_eq!(
        line(&mut client, &sn, "EXPLAIN SELECT * FROM tv", "read "),
        "read view tv (drains pending ticks when stale)"
    );
    assert_eq!(
        line(&mut client, &sn, "EXPLAIN SELECT * FROM t", "read "),
        "read table t"
    );
}

/// A pass-through CTE resolves to its source relation, so the alias reports the
/// source's kind — the case no catalog probe of the alias could answer, since the
/// alias is not a catalog name.
#[test]
fn a_passthrough_cte_reports_its_sources_kind() {
    let Some(srv) = ServerHandle::start() else { return };
    let (mut client, sn) = make_planner(&srv);
    seed(&mut client, &sn);
    exec(&mut client, &sn, "CREATE VIEW tv AS SELECT id, v FROM t WHERE w > 0");

    assert_eq!(
        line(
            &mut client,
            &sn,
            "EXPLAIN WITH c AS (SELECT * FROM tv) SELECT * FROM c",
            "read "
        ),
        "read view c (drains pending ticks when stale)"
    );
    assert_eq!(
        line(
            &mut client,
            &sn,
            "EXPLAIN WITH c AS (SELECT * FROM t) SELECT * FROM c",
            "read "
        ),
        "read table c"
    );
}

// ── Rejections: the plan of a query that has no plan is its rejection ────────

/// Error text with the parser's source locations erased. A message that quotes
/// the offending AST node quotes its `Span` too, and the `EXPLAIN ` prefix shifts
/// every column offset — which says nothing about whether the two paths raised
/// the same rejection.
fn without_spans(e: &GnitzSqlError) -> String {
    let text = format!("{e:?}");
    let mut out = String::with_capacity(text.len());
    let mut rest = text.as_str();
    while let Some(start) = rest.find("Location(") {
        let end = rest[start..].find(')').expect("a Location( is closed") + start;
        out.push_str(&rest[..start]);
        out.push_str("Location(_");
        rest = &rest[end..];
    }
    out.push_str(rest);
    out
}

/// EXPLAIN reuses the executing path's whole front end, so a query it cannot
/// describe returns the *identical* error executing it returns — the derivation
/// template for a derived relation, the feature-named `Unsupported` for a shape
/// the read spec cannot express.
#[test]
fn explain_returns_the_selects_own_rejection() {
    let Some(srv) = ServerHandle::start() else { return };
    let (mut client, sn) = make_planner(&srv);
    seed(&mut client, &sn);
    exec(
        &mut client,
        &sn,
        "CREATE TABLE u (id BIGINT UNSIGNED PRIMARY KEY, k BIGINT)",
    );

    for sql in [
        "SELECT t.v FROM t JOIN u ON t.id = u.id",
        "SELECT v FROM t UNION SELECT k FROM u",
        "SELECT v FROM t WHERE EXISTS (SELECT 1 FROM u WHERE u.id = t.id)",
        "SELECT v FROM t WHERE CAST(v AS CHAR) LIKE CAST(w AS CHAR)",
        "SELECT v FROM t ORDER BY v + 1",
    ] {
        let direct = try_exec(&mut client, &sn, sql).expect_err(&format!("`{sql}` must be rejected"));
        let explained = try_exec(&mut client, &sn, &format!("EXPLAIN {sql}"))
            .expect_err(&format!("`EXPLAIN {sql}` must be rejected"));
        assert_eq!(
            without_spans(&direct),
            without_spans(&explained),
            "EXPLAIN must return the same rejection as executing `{sql}`"
        );
    }
}

// ── The statement surface ────────────────────────────────────────────────────

/// Only the default option set is accepted; every non-default flag/format and
/// every non-SELECT inner statement is `Unsupported`. Bare `DESCRIBE t` is a
/// different statement (table introspection) and is out of scope.
#[test]
fn only_the_plain_explain_of_a_select_is_accepted() {
    let Some(srv) = ServerHandle::start() else { return };
    let (mut client, sn) = make_planner(&srv);
    seed(&mut client, &sn);

    for sql in [
        "EXPLAIN INSERT INTO t (id, v, w) VALUES (9, 9, 9)",
        "EXPLAIN ANALYZE SELECT v FROM t",
        "EXPLAIN VERBOSE SELECT v FROM t",
        "EXPLAIN QUERY PLAN SELECT v FROM t",
        "EXPLAIN ESTIMATE SELECT v FROM t",
        "EXPLAIN FORMAT JSON SELECT v FROM t",
        "EXPLAIN (FORMAT JSON) SELECT v FROM t",
        "DESCRIBE t",
    ] {
        let e = try_exec(&mut client, &sn, sql).expect_err(&format!("`{sql}` must be rejected"));
        assert!(
            matches!(e, GnitzSqlError::Unsupported(_)),
            "`{sql}` must be Unsupported, got {e:?}"
        );
    }
}

/// `EXPLAIN` / `DESCRIBE` / `DESC` are the same statement: the introducer is
/// inert phrasing.
#[test]
fn every_introducer_spelling_describes_the_same_plan() {
    let Some(srv) = ServerHandle::start() else { return };
    let (mut client, sn) = make_planner(&srv);
    seed(&mut client, &sn);

    let want = explain(&mut client, &sn, "EXPLAIN SELECT v FROM t WHERE id = 5");
    for sql in [
        "DESC SELECT v FROM t WHERE id = 5",
        "DESCRIBE SELECT v FROM t WHERE id = 5",
    ] {
        assert_eq!(explain(&mut client, &sn, sql), want, "{sql}");
    }
}

/// EXPLAIN issues strictly less than the `SELECT` already allowed inside a
/// transaction — catalog lookups and no data-path request — so it runs there and
/// leaves the transaction open.
#[test]
fn explain_runs_inside_a_transaction() {
    let Some(srv) = ServerHandle::start() else { return };
    let (mut client, sn) = make_planner(&srv);
    seed(&mut client, &sn);

    exec(&mut client, &sn, "BEGIN");
    assert_eq!(
        line(&mut client, &sn, "EXPLAIN SELECT v FROM t WHERE id = 5", "access:"),
        "access: pk point lookup"
    );
    // The transaction is still open: COMMIT succeeds rather than raising
    // "no transaction open".
    exec(&mut client, &sn, "COMMIT");
}
