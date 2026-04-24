#![cfg(feature = "integration")]

mod helpers;

use gnitz_core::GnitzClient;
use gnitz_sql::SqlPlanner;
use helpers::ServerHandle;

// Returns (client, schema_name) with a unique schema already created.
// The schema name is unique per call so parallel tests don't collide.
fn make_planner(srv: &ServerHandle) -> (GnitzClient, String) {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let sn = format!("s{}", SEQ.fetch_add(1, Ordering::Relaxed));
    let client = GnitzClient::connect(&srv.sock_path).unwrap();
    client.create_schema(&sn).unwrap();
    (client, sn)
}

// ── CREATE VIEW JOIN — SELECT item variants ──────────────────────────────────

/// Qualified alias: `left_t.col AS alias` — previously fell through to unsupported.
#[test]
fn test_join_select_qualified_alias() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (client, sn) = make_planner(&srv);
    let p = SqlPlanner::new(&client, &sn);

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
    let (client, sn) = make_planner(&srv);
    let p = SqlPlanner::new(&client, &sn);

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
    let (client, sn) = make_planner(&srv);
    let p = SqlPlanner::new(&client, &sn);

    p.execute("CREATE TABLE p (id BIGINT NOT NULL PRIMARY KEY, fk BIGINT NOT NULL)").unwrap();
    p.execute("CREATE TABLE q (id BIGINT NOT NULL PRIMARY KEY, fk BIGINT NOT NULL, qv BIGINT NOT NULL)").unwrap();

    let r = p.execute("CREATE VIEW v AS SELECT p.id, q.qv FROM p JOIN q ON p.fk = q.fk");
    assert!(r.is_ok(), "qualified refs without alias failed: {:?}", r.err());
}

/// Wildcard SELECT *.
#[test]
fn test_join_select_wildcard() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (client, sn) = make_planner(&srv);
    let p = SqlPlanner::new(&client, &sn);

    p.execute("CREATE TABLE l (id BIGINT NOT NULL PRIMARY KEY, fk BIGINT NOT NULL)").unwrap();
    p.execute("CREATE TABLE r (id BIGINT NOT NULL PRIMARY KEY, fk BIGINT NOT NULL)").unwrap();

    let r = p.execute("CREATE VIEW v AS SELECT * FROM l JOIN r ON l.fk = r.fk");
    assert!(r.is_ok(), "JOIN SELECT * failed: {:?}", r.err());
}

/// Mixed: alias + no-alias in same SELECT.
#[test]
fn test_join_select_mixed_alias_and_bare() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (client, sn) = make_planner(&srv);
    let p = SqlPlanner::new(&client, &sn);

    p.execute("CREATE TABLE m (id BIGINT NOT NULL PRIMARY KEY, fk BIGINT NOT NULL, mv BIGINT NOT NULL)").unwrap();
    p.execute("CREATE TABLE n (id BIGINT NOT NULL PRIMARY KEY, fk BIGINT NOT NULL, nv BIGINT NOT NULL)").unwrap();

    let r = p.execute(
        "CREATE VIEW v AS SELECT m.id AS mid, n.nv FROM m JOIN n ON m.fk = n.fk"
    );
    assert!(r.is_ok(), "mixed alias/bare in JOIN SELECT failed: {:?}", r.err());
}
