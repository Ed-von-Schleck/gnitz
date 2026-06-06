#![cfg(feature = "integration")]

use gnitz_sql::{GnitzSqlError, SqlPlanner};
use gnitz_test_harness::ServerHandle;

mod common;
use common::*;

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

/// Planner rejection: an ON conjunction one wider than the codec cap is rejected
/// as a clean `Unsupported` (not a `pack_pk_cols` panic). Written relative to
/// `PK_LIST_MAX_COLS` so it tracks a future bump rather than hardcoding the count.
#[test]
fn test_join_reject_arity_cap() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (mut client, sn) = make_planner(&srv);
    let mut p = SqlPlanner::new(&mut client, &sn);

    let n = gnitz_core::PK_LIST_MAX_COLS + 1; // one past the cap
    let key_cols = (0..n).map(|i| format!("c{i} BIGINT NOT NULL")).collect::<Vec<_>>().join(", ");
    p.execute(&format!("CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, {key_cols})")).unwrap();
    p.execute(&format!("CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, {key_cols})")).unwrap();

    let on = (0..n).map(|i| format!("a.c{i} = b.c{i}")).collect::<Vec<_>>().join(" AND ");
    let err = must_err(p.execute(&format!("CREATE VIEW v AS SELECT * FROM a JOIN b ON {on}")));
    match err {
        GnitzSqlError::Unsupported(s) => assert!(
            s.contains("equijoin key columns") && s.contains(&n.to_string()),
            "expected the arity-cap message naming {n}, got: {s}"),
        e => panic!("expected Unsupported, got {:?}", e),
    }
    assert!(client.resolve_table_or_view_id(&sn, "v").is_err(), "no view should be registered");
}

/// k = PK_LIST_MAX_COLS (the widest accepted composite key) registers cleanly,
/// carrying a k-column synthetic `_join_pk`. Guards the accept side of the cap.
#[test]
fn test_join_max_arity_accepted() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (mut client, sn) = make_planner(&srv);
    let k = gnitz_core::PK_LIST_MAX_COLS;
    {
        let mut p = SqlPlanner::new(&mut client, &sn);
        let key_cols = (0..k).map(|i| format!("c{i} BIGINT NOT NULL")).collect::<Vec<_>>().join(", ");
        p.execute(&format!("CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, {key_cols})")).unwrap();
        p.execute(&format!("CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, {key_cols})")).unwrap();
        let on = (0..k).map(|i| format!("a.c{i} = b.c{i}")).collect::<Vec<_>>().join(" AND ");
        p.execute(&format!("CREATE VIEW v AS SELECT * FROM a JOIN b ON {on}")).unwrap();
    }
    let (_, s) = client.resolve_table_or_view_id(&sn, "v").unwrap();
    assert_eq!(s.pk_indices(), (0..k).collect::<Vec<usize>>().as_slice(),
        "the k synthetic _join_pk columns are the view PK");
}

/// k = 2 composite equijoin: the view registers with a 2-column synthetic
/// `_join_pk`, and incremental INNER-join contents match a full recompute.
/// (The prior fail-closed gate is removed; this is its deliberate successor.)
#[test]
fn test_join_composite_two_pair_inner_data() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (mut client, sn) = make_planner(&srv);
    exec(&mut client, &sn,
        "CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, x BIGINT NOT NULL, y BIGINT NOT NULL, av BIGINT NOT NULL)");
    exec(&mut client, &sn,
        "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, x BIGINT NOT NULL, y BIGINT NOT NULL, bv BIGINT NOT NULL)");
    exec(&mut client, &sn,
        "CREATE VIEW v AS SELECT a.x, a.y, a.av, b.bv FROM a JOIN b ON a.x = b.x AND a.y = b.y");

    // The view PK is the 2 synthetic join-key columns.
    let (_, s) = client.resolve_table_or_view_id(&sn, "v").unwrap();
    assert_eq!(s.pk_indices(), &[0, 1], "k=2 join PK is the two _join_pk columns");

    // (x,y) matches: (10,100) a1·b1, (30,300) a3·b3. (20,*) differs in y → no match.
    exec(&mut client, &sn,
        "INSERT INTO a (id, x, y, av) VALUES (1, 10, 100, 1), (2, 20, 200, 2), (3, 30, 300, 3)");
    exec(&mut client, &sn,
        "INSERT INTO b (id, x, y, bv) VALUES (1, 10, 100, 11), (2, 20, 999, 22), (3, 30, 300, 33)");
    assert_eq!(payload_rows(&mut client, &sn, "v", &["x", "y", "av", "bv"]),
        vec![vec![10, 100, 1, 11], vec![30, 300, 3, 33]],
        "INNER k=2 join keeps only rows agreeing on both key columns");

    // Incremental: a later b row that completes the (20,200) pair must join in.
    exec(&mut client, &sn, "INSERT INTO b (id, x, y, bv) VALUES (4, 20, 200, 44)");
    assert_eq!(payload_rows(&mut client, &sn, "v", &["x", "y", "av", "bv"]),
        vec![vec![10, 100, 1, 11], vec![20, 200, 2, 44], vec![30, 300, 3, 33]],
        "incremental INNER join admits the newly-completed composite-key pair");
}
