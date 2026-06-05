#![cfg(feature = "integration")]

use gnitz_core::GnitzClient;
use gnitz_sql::{GnitzSqlError, SqlPlanner, SqlResult};
use gnitz_test_harness::ServerHandle;

fn must_err(r: Result<Vec<SqlResult>, GnitzSqlError>) -> GnitzSqlError {
    match r { Ok(_) => panic!("expected error, got Ok"), Err(e) => e }
}

// Returns (client, schema_name) with a unique schema already created.
// The schema name is unique per call so parallel tests don't collide.
fn make_planner(srv: &ServerHandle) -> (GnitzClient, String) {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let sn = format!("s{}", SEQ.fetch_add(1, Ordering::Relaxed));
    let mut client = GnitzClient::connect(&srv.sock_path).unwrap();
    client.create_schema(&sn).unwrap();
    (client, sn)
}

// ── CREATE VIEW JOIN — SELECT item variants ──────────────────────────────────

/// Qualified alias: `left_t.col AS alias` — previously fell through to unsupported.
#[test]
fn test_join_select_qualified_alias() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (mut client, sn) = make_planner(&srv);
    let mut p = SqlPlanner::new(&mut client, &sn);

    p.execute("CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, fk BIGINT NOT NULL, v BIGINT NOT NULL)").unwrap();
    p.execute("CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, fk BIGINT NOT NULL, w BIGINT NOT NULL)").unwrap();

    let r = p.execute(
        "CREATE VIEW v AS \
         SELECT a.id AS aid, b.id AS bid, a.v AS aval, b.w AS bval \
         FROM a JOIN b ON a.fk = b.fk"
    );
    assert!(r.is_ok(), "qualified alias in JOIN SELECT failed: {:?}", r.err());
}

/// Unqualified alias: `col AS alias`.
#[test]
fn test_join_select_unqualified_alias() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (mut client, sn) = make_planner(&srv);
    let mut p = SqlPlanner::new(&mut client, &sn);

    p.execute("CREATE TABLE x (id BIGINT NOT NULL PRIMARY KEY, fk BIGINT NOT NULL, xv BIGINT NOT NULL)").unwrap();
    p.execute("CREATE TABLE y (id BIGINT NOT NULL PRIMARY KEY, fk BIGINT NOT NULL, yv BIGINT NOT NULL)").unwrap();

    let r = p.execute(
        "CREATE VIEW v AS SELECT xv AS xcol, yv AS ycol FROM x JOIN y ON x.fk = y.fk"
    );
    assert!(r.is_ok(), "unqualified alias in JOIN SELECT failed: {:?}", r.err());
}

/// No aliases — table-qualified column refs.
#[test]
fn test_join_select_no_alias() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (mut client, sn) = make_planner(&srv);
    let mut p = SqlPlanner::new(&mut client, &sn);

    p.execute("CREATE TABLE p (id BIGINT NOT NULL PRIMARY KEY, fk BIGINT NOT NULL)").unwrap();
    p.execute("CREATE TABLE q (id BIGINT NOT NULL PRIMARY KEY, fk BIGINT NOT NULL, qv BIGINT NOT NULL)").unwrap();

    let r = p.execute("CREATE VIEW v AS SELECT p.id, q.qv FROM p JOIN q ON p.fk = q.fk");
    assert!(r.is_ok(), "qualified refs without alias failed: {:?}", r.err());
}

/// Wildcard SELECT *.
#[test]
fn test_join_select_wildcard() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (mut client, sn) = make_planner(&srv);
    let mut p = SqlPlanner::new(&mut client, &sn);

    p.execute("CREATE TABLE l (id BIGINT NOT NULL PRIMARY KEY, fk BIGINT NOT NULL)").unwrap();
    p.execute("CREATE TABLE r (id BIGINT NOT NULL PRIMARY KEY, fk BIGINT NOT NULL)").unwrap();

    let r = p.execute("CREATE VIEW v AS SELECT * FROM l JOIN r ON l.fk = r.fk");
    assert!(r.is_ok(), "JOIN SELECT * failed: {:?}", r.err());
}

/// Mixed: alias + no-alias in same SELECT.
#[test]
fn test_join_select_mixed_alias_and_bare() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (mut client, sn) = make_planner(&srv);
    let mut p = SqlPlanner::new(&mut client, &sn);

    p.execute("CREATE TABLE m (id BIGINT NOT NULL PRIMARY KEY, fk BIGINT NOT NULL, mv BIGINT NOT NULL)").unwrap();
    p.execute("CREATE TABLE n (id BIGINT NOT NULL PRIMARY KEY, fk BIGINT NOT NULL, nv BIGINT NOT NULL)").unwrap();

    let r = p.execute(
        "CREATE VIEW v AS SELECT m.id AS mid, n.nv FROM m JOIN n ON m.fk = n.fk"
    );
    assert!(r.is_ok(), "mixed alias/bare in JOIN SELECT failed: {:?}", r.err());
}

// ── Composite (multi-column) equijoin keys ───────────────────────────────────

/// Newly accepted at k = 1: a parenthesized single-pair ON now succeeds
/// (Expr::Nested is unwrapped, matching the WHERE/HAVING paths). Previously
/// rejected with Unsupported.
#[test]
fn test_join_paren_single_pair_accepted() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (mut client, sn) = make_planner(&srv);
    let mut p = SqlPlanner::new(&mut client, &sn);

    p.execute("CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, fk BIGINT NOT NULL)").unwrap();
    p.execute("CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, fk BIGINT NOT NULL)").unwrap();

    let r = p.execute("CREATE VIEW v AS SELECT * FROM a JOIN b ON (a.fk = b.fk)");
    assert!(r.is_ok(), "parenthesized single-pair ON failed: {:?}", r.err());
}

/// Planner rejection: a float join key. Fires from validate_join_key_pair inside
/// extraction, before any circuit is built.
#[test]
fn test_join_reject_float_key() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (mut client, sn) = make_planner(&srv);
    let mut p = SqlPlanner::new(&mut client, &sn);

    p.execute("CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, fk DOUBLE NOT NULL)").unwrap();
    p.execute("CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, fk DOUBLE NOT NULL)").unwrap();

    let err = must_err(p.execute("CREATE VIEW v AS SELECT * FROM a JOIN b ON a.fk = b.fk"));
    assert!(matches!(err, GnitzSqlError::Unsupported(_)), "expected Unsupported, got {:?}", err);
    assert!(client.resolve_table_or_view_id(&sn, "v").is_err(), "no view should be registered");
}

/// Planner rejection: an encoding-mismatched pair (U32 = U64). The two reindex
/// sides emit different strides, so the join is rejected at CREATE.
#[test]
fn test_join_reject_encoding_mismatch() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (mut client, sn) = make_planner(&srv);
    let mut p = SqlPlanner::new(&mut client, &sn);

    p.execute("CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, fk INT UNSIGNED NOT NULL)").unwrap();
    p.execute("CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, fk BIGINT UNSIGNED NOT NULL)").unwrap();

    let err = must_err(p.execute("CREATE VIEW v AS SELECT * FROM a JOIN b ON a.fk = b.fk"));
    assert!(matches!(err, GnitzSqlError::Unsupported(_)), "expected Unsupported, got {:?}", err);
    assert!(client.resolve_table_or_view_id(&sn, "v").is_err(), "no view should be registered");
}

/// Planner rejection: a mixed string/native pair (VARCHAR = U128). A string
/// content hash never equals a native key, so it is rejected at CREATE.
#[test]
fn test_join_reject_mixed_string_native() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (mut client, sn) = make_planner(&srv);
    let mut p = SqlPlanner::new(&mut client, &sn);

    p.execute("CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, s VARCHAR NOT NULL)").unwrap();
    p.execute("CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, n DECIMAL(38,0) NOT NULL)").unwrap();

    let err = must_err(p.execute("CREATE VIEW v AS SELECT * FROM a JOIN b ON a.s = b.n"));
    assert!(matches!(err, GnitzSqlError::Unsupported(_)), "expected Unsupported, got {:?}", err);
    assert!(client.resolve_table_or_view_id(&sn, "v").is_err(), "no view should be registered");
}

/// Planner rejection: a six-pair ON conjunction exceeds MAX_PK_COLUMNS (5).
#[test]
fn test_join_reject_arity_cap() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (mut client, sn) = make_planner(&srv);
    let mut p = SqlPlanner::new(&mut client, &sn);

    p.execute("CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, \
               c0 BIGINT NOT NULL, c1 BIGINT NOT NULL, c2 BIGINT NOT NULL, \
               c3 BIGINT NOT NULL, c4 BIGINT NOT NULL, c5 BIGINT NOT NULL)").unwrap();
    p.execute("CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, \
               c0 BIGINT NOT NULL, c1 BIGINT NOT NULL, c2 BIGINT NOT NULL, \
               c3 BIGINT NOT NULL, c4 BIGINT NOT NULL, c5 BIGINT NOT NULL)").unwrap();

    let err = must_err(p.execute(
        "CREATE VIEW v AS SELECT * FROM a JOIN b ON \
         a.c0 = b.c0 AND a.c1 = b.c1 AND a.c2 = b.c2 AND \
         a.c3 = b.c3 AND a.c4 = b.c4 AND a.c5 = b.c5"));
    assert!(matches!(err, GnitzSqlError::Unsupported(_)), "expected Unsupported, got {:?}", err);
    assert!(client.resolve_table_or_view_id(&sn, "v").is_err(), "no view should be registered");
}

/// k ≥ 2 fail-closed boundary: a type-valid two-pair CREATE VIEW is rejected by
/// the planner gate (not by validate_join_key_pair). The error names "composite"
/// to distinguish the gate from the type rejections above. No view is registered;
/// the view-storage successor flips this to success deliberately.
#[test]
fn test_join_composite_two_pair_gated() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (mut client, sn) = make_planner(&srv);
    let mut p = SqlPlanner::new(&mut client, &sn);

    p.execute("CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, x BIGINT NOT NULL, y BIGINT NOT NULL)").unwrap();
    p.execute("CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, x BIGINT NOT NULL, y BIGINT NOT NULL)").unwrap();

    let err = must_err(p.execute(
        "CREATE VIEW v AS SELECT * FROM a JOIN b ON a.x = b.x AND a.y = b.y"));
    match err {
        GnitzSqlError::Unsupported(s) => {
            assert!(s.contains("composite") && s.contains("not yet supported"),
                    "expected the composite-key gate message, got: {}", s);
        }
        e => panic!("expected Unsupported, got {:?}", e),
    }
    assert!(client.resolve_table_or_view_id(&sn, "v").is_err(), "no view should be registered");
}
