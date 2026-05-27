#![cfg(feature = "integration")]

use gnitz_core::GnitzClient;
use gnitz_sql::{GnitzSqlError, SqlPlanner, SqlResult};
use gnitz_test_harness::ServerHandle;

fn must_err(r: Result<Vec<SqlResult>, GnitzSqlError>) -> GnitzSqlError {
    match r { Ok(_) => panic!("expected error, got Ok"), Err(e) => e }
}

fn make_planner(srv: &ServerHandle) -> (GnitzClient, String) {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let sn = format!("cte{}", SEQ.fetch_add(1, Ordering::Relaxed));
    let mut client = GnitzClient::connect(&srv.sock_path).unwrap();
    client.create_schema(&sn).unwrap();
    (client, sn)
}

// ── item 45: CTE column aliases applied ──────────────────────────────

#[test]
fn test_cte_column_aliases_applied() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (mut client, sn) = make_planner(&srv);
    {
        let mut p = SqlPlanner::new(&mut client, &sn);
        p.execute("CREATE TABLE t (a BIGINT PRIMARY KEY, b BIGINT NOT NULL)").unwrap();
        p.execute(
            "CREATE VIEW v AS WITH cte(alias_a, alias_b) AS (SELECT a, b FROM t) \
             SELECT alias_a FROM cte"
        ).unwrap();
    }
    let (_, s) = client.resolve_table_or_view_id(&sn, "v").unwrap();
    assert!(s.columns[0].name.eq_ignore_ascii_case("alias_a"),
        "outer reference to CTE alias must bind, got {:?}", s.columns[0].name);
}

// ── item 45: CTE alias count mismatch ────────────────────────────────

#[test]
fn test_cte_alias_count_mismatch_rejected() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (mut client, sn) = make_planner(&srv);
    let mut p = SqlPlanner::new(&mut client, &sn);
    p.execute("CREATE TABLE t (a BIGINT PRIMARY KEY, b BIGINT NOT NULL)").unwrap();
    let err = must_err(p.execute(
        "CREATE VIEW v AS WITH cte(x, y, z) AS (SELECT a, b FROM t) SELECT x FROM cte"));
    match err {
        GnitzSqlError::Plan(s) => assert!(s.contains("alias"), "got: {}", s),
        e => panic!("expected Plan, got {:?}", e),
    }
    assert!(client.resolve_table_or_view_id(&sn, "v").is_err(),
        "mismatched-alias CTE view must not be registered");
}

// ── item 15: a CTE body with WHERE/projection must be rejected, not ──
// silently discarded (which would return the unfiltered base table).

#[test]
fn test_cte_with_where_rejected() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (mut client, sn) = make_planner(&srv);
    let mut p = SqlPlanner::new(&mut client, &sn);
    p.execute("CREATE TABLE t (a BIGINT PRIMARY KEY, b BIGINT NOT NULL)").unwrap();
    let err = must_err(p.execute(
        "CREATE VIEW v AS WITH cte AS (SELECT * FROM t WHERE b > 5) SELECT a FROM cte"));
    assert!(matches!(err, GnitzSqlError::Unsupported(_)),
        "CTE with WHERE must be rejected (not silently discarded), got {:?}", err);
    assert!(client.resolve_table_or_view_id(&sn, "v").is_err(),
        "rejected-CTE view must not be registered");
}

#[test]
fn test_cte_subset_projection_rejected() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (mut client, sn) = make_planner(&srv);
    let mut p = SqlPlanner::new(&mut client, &sn);
    p.execute("CREATE TABLE t (a BIGINT PRIMARY KEY, b BIGINT NOT NULL)").unwrap();
    // Projection drops column b — not an identity pass-through.
    let err = must_err(p.execute(
        "CREATE VIEW v AS WITH cte AS (SELECT a FROM t) SELECT a FROM cte"));
    assert!(matches!(err, GnitzSqlError::Unsupported(_)),
        "CTE with a subset projection must be rejected, got {:?}", err);
}

#[test]
fn test_cte_plain_passthrough_accepted() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (mut client, sn) = make_planner(&srv);
    let mut p = SqlPlanner::new(&mut client, &sn);
    p.execute("CREATE TABLE t (a BIGINT PRIMARY KEY, b BIGINT NOT NULL)").unwrap();
    // Identity pass-through (all columns in order) stays supported.
    p.execute("CREATE VIEW v AS WITH cte AS (SELECT * FROM t) SELECT a FROM cte").unwrap();
    assert!(client.resolve_table_or_view_id(&sn, "v").is_ok());
}
