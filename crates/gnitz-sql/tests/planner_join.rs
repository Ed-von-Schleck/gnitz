#![cfg(feature = "integration")]

use gnitz_core::{
    OPCODE_JOIN_DELTA_TRACE_RANGE, OPCODE_PARTITION_FILTER,
    OPCODE_MAP_EXPR, OPCODE_MAP_PROJ, OPCODE_DISTINCT, OPCODE_NEGATE, OPCODE_UNION,
    OPCODE_NULL_EXTEND, OPCODE_EXCHANGE_SHARD, OPCODE_INTEGRATE_TRACE,
    OPCODE_JOIN_DELTA_TRACE, OPCODE_JOIN_DELTA_TRACE_OUTER,
    OPCODE_ANTI_JOIN_DELTA_TRACE, OPCODE_SEMI_JOIN_DELTA_TRACE, OPCODE_DELAY,
    OPCODE_SCAN_DELTA, OPCODE_REDUCE,
};
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

/// Planner acceptance: a cross-sign integer pair whose unsigned side is ≤ 4 bytes
/// (`INT UNSIGNED` = `BIGINT`, i.e. U32 = I64). The pair promotes to the narrowest
/// signed type holding both ranges (I64) and co-partitions byte-for-byte, so
/// CREATE VIEW succeeds and the view resolves.
#[test]
fn test_join_accept_cross_sign_narrow() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (mut client, sn) = make_planner(&srv);
    let mut p = SqlPlanner::new(&mut client, &sn);

    p.execute("CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, fk INT UNSIGNED NOT NULL)").unwrap();
    p.execute("CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, fk BIGINT NOT NULL)").unwrap();

    let r = p.execute("CREATE VIEW v AS SELECT * FROM a JOIN b ON a.fk = b.fk");
    assert!(r.is_ok(), "narrow cross-sign join (U32 = I64) should be accepted: {:?}", r.err());
    assert!(client.resolve_table_or_view_id(&sn, "v").is_ok(), "view should be registered");

    // A second pair: TINYINT UNSIGNED (U8) = SMALLINT (I16), promoting to I16.
    let mut p = SqlPlanner::new(&mut client, &sn);
    p.execute("CREATE TABLE c (id BIGINT NOT NULL PRIMARY KEY, fk TINYINT UNSIGNED NOT NULL)").unwrap();
    p.execute("CREATE TABLE d (id BIGINT NOT NULL PRIMARY KEY, fk SMALLINT NOT NULL)").unwrap();
    let r = p.execute("CREATE VIEW w AS SELECT * FROM c JOIN d ON c.fk = d.fk");
    assert!(r.is_ok(), "narrow cross-sign join (U8 = I16) should be accepted: {:?}", r.err());
    assert!(client.resolve_table_or_view_id(&sn, "w").is_ok(), "view should be registered");
}

/// Planner acceptance: a cross-sign pair whose unsigned side is `U64`
/// (`BIGINT UNSIGNED` = `BIGINT`, i.e. U64 = I64). Its faithful common type is the
/// signed-128 type `I128`; the pair promotes to it and co-partitions byte-for-byte,
/// so CREATE VIEW succeeds and the view resolves.
#[test]
fn test_join_accept_cross_sign_u64() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (mut client, sn) = make_planner(&srv);
    let mut p = SqlPlanner::new(&mut client, &sn);

    p.execute("CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, fk BIGINT UNSIGNED NOT NULL)").unwrap();
    p.execute("CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, fk BIGINT NOT NULL)").unwrap();

    let r = p.execute("CREATE VIEW v AS SELECT * FROM a JOIN b ON a.fk = b.fk");
    assert!(r.is_ok(), "cross-sign join (U64 = I64) should be accepted: {:?}", r.err());
    assert!(client.resolve_table_or_view_id(&sn, "v").is_ok(), "view should be registered");
}

/// Planner rejection: the surviving cross-sign reject — the unsigned side is
/// 128-bit (`DECIMAL(38,0)`/`UUID` = a signed integer). Its faithful common type
/// is a signed-256 type that does not exist, so the join is rejected at CREATE.
#[test]
fn test_join_reject_cross_sign() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (mut client, sn) = make_planner(&srv);
    let mut p = SqlPlanner::new(&mut client, &sn);

    p.execute("CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, fk DECIMAL(38,0) NOT NULL)").unwrap();
    p.execute("CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, fk BIGINT NOT NULL)").unwrap();

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

// ── Equijoin views over compound / wide-PK source tables ─────────────────────
//
// The output PK of an equijoin view is never the source PK — it is the synthetic
// `_join_pk` derived from the equijoin key. A compound/wide source PK rides
// through the reindex as ordinary payload (decoded to native values), so
// `payload_rows` reads those columns directly; the synthetic `_join_pk` columns
// are the PK region, asserted via `resolve_table_or_view_id(...).pk_indices()`.

/// Single-key join over a compound-PK source: the compound source PK rides as
/// payload and the output PK is the lone synthetic join column.
#[test]
fn test_join_compound_pk_source_single_key() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (mut client, sn) = make_planner(&srv);
    exec(&mut client, &sn,
        "CREATE TABLE a (k1 BIGINT NOT NULL, k2 BIGINT NOT NULL, fk BIGINT NOT NULL, av BIGINT NOT NULL, PRIMARY KEY (k1, k2))");
    exec(&mut client, &sn,
        "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, fk BIGINT NOT NULL, bv BIGINT NOT NULL)");
    exec(&mut client, &sn,
        "CREATE VIEW v AS SELECT a.k1, a.k2, a.av, b.bv FROM a JOIN b ON a.fk = b.fk");

    let (_, s) = client.resolve_table_or_view_id(&sn, "v").unwrap();
    assert_eq!(s.pk_indices(), &[0], "single-key join over a compound-PK source: output PK is the lone _join_pk");

    exec(&mut client, &sn,
        "INSERT INTO a (k1, k2, fk, av) VALUES (1, 100, 7, 11), (2, 200, 9, 22)");
    exec(&mut client, &sn,
        "INSERT INTO b (id, fk, bv) VALUES (1, 7, 70), (2, 9, 90)");

    assert_eq!(payload_rows(&mut client, &sn, "v", &["k1", "k2", "av", "bv"]),
        vec![vec![1, 100, 11, 70], vec![2, 200, 22, 90]],
        "compound source PK rides as payload, decoded to native values");
}

/// Composite join key over two compound-PK sources: output PK is the 2 synthetic
/// `_join_pk` columns; the key is drawn from non-PK columns of both sides.
#[test]
fn test_join_compound_pk_source_composite_key() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (mut client, sn) = make_planner(&srv);
    exec(&mut client, &sn,
        "CREATE TABLE a (k1 BIGINT NOT NULL, k2 BIGINT NOT NULL, x BIGINT NOT NULL, y BIGINT NOT NULL, av BIGINT NOT NULL, PRIMARY KEY (k1, k2))");
    exec(&mut client, &sn,
        "CREATE TABLE b (j1 BIGINT NOT NULL, j2 BIGINT NOT NULL, x BIGINT NOT NULL, y BIGINT NOT NULL, bv BIGINT NOT NULL, PRIMARY KEY (j1, j2))");
    exec(&mut client, &sn,
        "CREATE VIEW v AS SELECT a.k1, a.av, b.bv FROM a JOIN b ON a.x = b.x AND a.y = b.y");

    let (_, s) = client.resolve_table_or_view_id(&sn, "v").unwrap();
    assert_eq!(s.pk_indices(), &[0, 1], "k=2 join over compound sources: output PK is the two _join_pk cols");

    exec(&mut client, &sn,
        "INSERT INTO a (k1, k2, x, y, av) VALUES (1, 1, 10, 100, 1), (2, 2, 20, 200, 2)");
    exec(&mut client, &sn,
        "INSERT INTO b (j1, j2, x, y, bv) VALUES (5, 5, 10, 100, 11), (6, 6, 20, 999, 22)");
    assert_eq!(payload_rows(&mut client, &sn, "v", &["k1", "av", "bv"]),
        vec![vec![1, 1, 11]],
        "(20,200) vs (20,999) disagree on y — only (10,100) joins");

    exec(&mut client, &sn, "INSERT INTO b (j1, j2, x, y, bv) VALUES (7, 7, 20, 200, 44)");
    assert_eq!(payload_rows(&mut client, &sn, "v", &["k1", "av", "bv"]),
        vec![vec![1, 1, 11], vec![2, 2, 44]],
        "incremental join admits the newly-completed pair");
}

/// LEFT join whose left source has a compound PK: unmatched left rows emit
/// `(left payload incl. compound PK, NULL right)`.
#[test]
fn test_join_compound_pk_source_left_outer() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (mut client, sn) = make_planner(&srv);
    exec(&mut client, &sn,
        "CREATE TABLE a (k1 BIGINT NOT NULL, k2 BIGINT NOT NULL, fk BIGINT NOT NULL, \
         av BIGINT NOT NULL, PRIMARY KEY (k1, k2))");
    exec(&mut client, &sn,
        "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, fk BIGINT NOT NULL, bv BIGINT NOT NULL)");
    exec(&mut client, &sn,
        "CREATE VIEW v AS SELECT a.k1, a.k2, a.av, b.bv FROM a LEFT JOIN b ON a.fk = b.fk");

    exec(&mut client, &sn,
        "INSERT INTO a (k1, k2, fk, av) VALUES (1, 100, 7, 11), (2, 200, 9, 22)");
    exec(&mut client, &sn,
        "INSERT INTO b (id, fk, bv) VALUES (1, 7, 70)");

    let (schema, batch) = read_view(&mut client, &sn, "v");
    assert_eq!(batch.len(), 2);

    let k1_ci  = col_idx(&schema, "k1");
    let k2_ci  = col_idx(&schema, "k2");
    let av_ci  = col_idx(&schema, "av");
    let bv_ci  = col_idx(&schema, "bv");
    // bv_payload_idx: schema [_join_pk(pk), k1, k2, av, bv]; 1 PK col; bv payload = bv_ci - 1
    let bv_payload_idx = bv_ci - schema.pk_cols.len();

    let (r_matched, r_unmatched) =
        if i64_at(&batch, k1_ci, 0) == 1 { (0, 1) } else { (1, 0) };

    // Matched row (fk=7)
    assert!(!is_null_at(&batch, bv_payload_idx, r_matched));
    assert_eq!(i64_at(&batch, bv_ci, r_matched), 70);

    // Unmatched row (fk=9): compound source PK intact, right side null
    assert_eq!(i64_at(&batch, k1_ci, r_unmatched), 2);
    assert_eq!(i64_at(&batch, k2_ci, r_unmatched), 200);
    assert_eq!(i64_at(&batch, av_ci, r_unmatched), 22);
    assert!(is_null_at(&batch, bv_payload_idx, r_unmatched),
        "unmatched left row: bv must be null");
}

/// Wide (> 16-byte) compound source PK: a 3-BIGINT PK (24-byte stride). Two source
/// rows that share the first 16 OPK bytes and differ only past byte 16 must
/// survive ingest/scan as distinct and join independently.
#[test]
fn test_join_compound_pk_source_wide_tiebreak() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (mut client, sn) = make_planner(&srv);
    exec(&mut client, &sn,
        "CREATE TABLE a (k1 BIGINT NOT NULL, k2 BIGINT NOT NULL, k3 BIGINT NOT NULL, \
         fk BIGINT NOT NULL, av BIGINT NOT NULL, PRIMARY KEY (k1, k2, k3))");
    exec(&mut client, &sn,
        "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, fk BIGINT NOT NULL, bv BIGINT NOT NULL)");
    exec(&mut client, &sn,
        "CREATE VIEW v AS SELECT a.k1, a.k2, a.k3, a.av, b.bv FROM a JOIN b ON a.fk = b.fk");

    let (_, s) = client.resolve_table_or_view_id(&sn, "v").unwrap();
    assert_eq!(s.pk_indices(), &[0]);

    // (1,1,1) and (1,1,2) share the first 16 OPK bytes; differ only at byte 16+.
    exec(&mut client, &sn,
        "INSERT INTO a (k1, k2, k3, fk, av) VALUES (1, 1, 1, 7, 11), (1, 1, 2, 7, 22)");
    exec(&mut client, &sn,
        "INSERT INTO b (id, fk, bv) VALUES (1, 7, 70)");

    assert_eq!(
        payload_rows(&mut client, &sn, "v", &["k1", "k2", "k3", "av", "bv"]),
        vec![vec![1, 1, 1, 11, 70], vec![1, 1, 2, 22, 70]],
        "rows sharing first 16 OPK bytes survive as distinct and both join"
    );
}

/// Incremental UPDATE / DELETE of source rows over a compound-PK source.
#[test]
fn test_join_compound_pk_source_update_delete() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (mut client, sn) = make_planner(&srv);
    exec(&mut client, &sn,
        "CREATE TABLE a (k1 BIGINT NOT NULL, k2 BIGINT NOT NULL, fk BIGINT NOT NULL, av BIGINT NOT NULL, PRIMARY KEY (k1, k2))");
    exec(&mut client, &sn,
        "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, fk BIGINT NOT NULL, bv BIGINT NOT NULL)");
    exec(&mut client, &sn,
        "CREATE VIEW v AS SELECT a.k1, a.k2, a.av, b.bv FROM a JOIN b ON a.fk = b.fk");

    exec(&mut client, &sn,
        "INSERT INTO a (k1, k2, fk, av) VALUES (1, 100, 7, 11), (2, 200, 9, 22)");
    exec(&mut client, &sn,
        "INSERT INTO b (id, fk, bv) VALUES (1, 7, 70), (2, 9, 90)");
    assert_eq!(payload_rows(&mut client, &sn, "v", &["k1", "k2", "av", "bv"]),
        vec![vec![1, 100, 11, 70], vec![2, 200, 22, 90]]);

    // UPDATE a source payload column: retract+insert reflected on next read.
    exec(&mut client, &sn, "UPDATE a SET av = 111 WHERE k1 = 1 AND k2 = 100");
    assert_eq!(payload_rows(&mut client, &sn, "v", &["k1", "k2", "av", "bv"]),
        vec![vec![1, 100, 111, 70], vec![2, 200, 22, 90]],
        "incremental UPDATE recomputes only the touched join row");

    // DELETE a source row: removes its join output.
    exec(&mut client, &sn, "DELETE FROM a WHERE k1 = 2 AND k2 = 200");
    assert_eq!(payload_rows(&mut client, &sn, "v", &["k1", "k2", "av", "bv"]),
        vec![vec![1, 100, 111, 70]],
        "incremental DELETE drops the removed source row's join output");
}

// ── Duplicate output column names rejected (join view) ────────────────

#[test]
fn test_join_duplicate_output_columns_rejected() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (mut client, sn) = make_planner(&srv);
    let mut p = SqlPlanner::new(&mut client, &sn);
    p.execute("CREATE TABLE jl_dup (id BIGINT PRIMARY KEY, a BIGINT NOT NULL)").unwrap();
    p.execute("CREATE TABLE jr_dup (id BIGINT PRIMARY KEY, b BIGINT NOT NULL)").unwrap();
    let err = must_err(p.execute(
        "CREATE VIEW v_jn_dup AS SELECT jl_dup.a AS x, jr_dup.b AS x \
         FROM jl_dup JOIN jr_dup ON jl_dup.id = jr_dup.id"));
    match err {
        GnitzSqlError::Plan(s) => assert!(s.contains("duplicate column"), "got: {}", s),
        e => panic!("expected Plan, got {:?}", e),
    }
}

// ── Range / band join: planner admission ─────────────────────────────

/// A pure range join registers cleanly: its output PK is the source-PK pair.
#[test]
fn test_range_join_accepts_pure_range() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (mut client, sn) = make_planner(&srv);
    let mut p = SqlPlanner::new(&mut client, &sn);
    p.execute("CREATE TABLE rj_a (id BIGINT NOT NULL PRIMARY KEY, x BIGINT NOT NULL)").unwrap();
    p.execute("CREATE TABLE rj_b (id BIGINT NOT NULL PRIMARY KEY, y BIGINT NOT NULL)").unwrap();
    // Alias the two `id` columns so the projection has no duplicate output names.
    p.execute("CREATE VIEW rj_v AS SELECT rj_a.id AS aid, rj_b.id AS bid, rj_b.y AS by FROM rj_a JOIN rj_b ON rj_a.x < rj_b.y").unwrap();
    assert!(client.resolve_table_or_view_id(&sn, "rj_v").is_ok(), "range-join view should register");
}

/// A band join (eq prefix + range) registers cleanly.
#[test]
fn test_range_join_accepts_band() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (mut client, sn) = make_planner(&srv);
    let mut p = SqlPlanner::new(&mut client, &sn);
    p.execute("CREATE TABLE bj_a (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, lo BIGINT NOT NULL)").unwrap();
    p.execute("CREATE TABLE bj_b (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, t BIGINT NOT NULL)").unwrap();
    p.execute("CREATE VIEW bj_v AS SELECT * FROM bj_a JOIN bj_b ON bj_a.k = bj_b.k AND bj_a.lo <= bj_b.t").unwrap();
    assert!(client.resolve_table_or_view_id(&sn, "bj_v").is_ok(), "band-join view should register");
}

/// A band LEFT JOIN (≥1 equality conjunct + a range conjunct) registers cleanly —
/// the null-fill is partition-local on the eq-prefix scatter.
#[test]
fn test_band_left_join_accepts() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (mut client, sn) = make_planner(&srv);
    let mut p = SqlPlanner::new(&mut client, &sn);
    p.execute("CREATE TABLE bl_a (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, lo BIGINT NOT NULL)").unwrap();
    p.execute("CREATE TABLE bl_b (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, t BIGINT NOT NULL)").unwrap();
    p.execute("CREATE VIEW bl_v AS SELECT bl_a.id AS aid, bl_b.id AS bid \
               FROM bl_a LEFT JOIN bl_b ON bl_a.k = bl_b.k AND bl_a.lo <= bl_b.t").unwrap();
    assert!(client.resolve_table_or_view_id(&sn, "bl_v").is_ok(), "band LEFT-join view should register");
}

/// A pure-range LEFT JOIN (no equality conjunct) now registers: existence of a
/// match collapses to a threshold test against a single scalar — `MAX`/`MIN` of
/// the right range column — so the null-fill is `null_extend(A ⋈ {m})`, a range
/// join whose trace side is one row. That rides the single pair-PK output
/// exchange like the inner pairs (no second sequential exchange). The scalar `m`
/// is computed in an internal one-row aggregate view, seeded by a one-row
/// sentinel so it is never empty. See plans/range-join-left-outer-pure-range.md.
#[test]
fn test_pure_range_left_join_accepts() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (mut client, sn) = make_planner(&srv);
    let mut p = SqlPlanner::new(&mut client, &sn);
    p.execute("CREATE TABLE rl_a (id BIGINT NOT NULL PRIMARY KEY, x BIGINT NOT NULL)").unwrap();
    p.execute("CREATE TABLE rl_b (id BIGINT NOT NULL PRIMARY KEY, y BIGINT NOT NULL)").unwrap();
    assert!(p.execute(
        "CREATE VIEW rl_v AS SELECT rl_a.id AS aid, rl_b.id AS bid \
         FROM rl_a LEFT JOIN rl_b ON rl_a.x < rl_b.y").is_ok(),
        "pure-range LEFT JOIN should now register");
    assert!(client.resolve_table_or_view_id(&sn, "rl_v").is_ok(),
        "pure-range LEFT-join view should register");
}

/// A pure-range LEFT JOIN whose promoted compare type is 16-byte (here `U64` vs
/// `I64` → `I128`) is rejected, not accepted: the threshold null-fill aggregates the
/// range column with a `MIN`/`MAX` reduce, which — like the binder's MIN/MAX — handles
/// only ≤8-byte integer keys. Pins that the planner returns a clean `Unsupported`
/// (building no `__m`/sentinel objects) rather than a reduce over a width it cannot
/// aggregate.
#[test]
fn test_pure_range_left_join_rejects_wide_compare_type() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (mut client, sn) = make_planner(&srv);
    let mut p = SqlPlanner::new(&mut client, &sn);
    p.execute("CREATE TABLE rlw_a (id BIGINT NOT NULL PRIMARY KEY, x BIGINT UNSIGNED NOT NULL)").unwrap();
    p.execute("CREATE TABLE rlw_b (id BIGINT NOT NULL PRIMARY KEY, y BIGINT NOT NULL)").unwrap();
    let err = must_err(p.execute(
        "CREATE VIEW rlw_v AS SELECT rlw_a.id AS aid, rlw_b.id AS bid \
         FROM rlw_a LEFT JOIN rlw_b ON rlw_a.x < rlw_b.y"));
    match err {
        GnitzSqlError::Unsupported(s) => assert!(s.contains("compare type"), "got: {s}"),
        e => panic!("expected Unsupported, got {:?}", e),
    }
    assert!(client.resolve_table_or_view_id(&sn, "rlw_v").is_err(), "no view should be registered");
}

/// The pure-range LEFT null-fill, pinned at the circuit level. Built over the
/// SAME tables as an INNER pure-range view, the LEFT view adds the THRESHOLD
/// form — NOT the band set-difference. Concretely it adds **+2** range
/// `DeltaTraceRange` terms (the inner has 2; the LEFT has 4 — the matched
/// null-fill `ΔA_keep ⋈ trace_m` and `Δm ⋈ trace_a`), exactly one `null_extend`,
/// and a third `ScanDelta` source + a third `IntegrateTrace` (the `__m` delta and
/// its one-row `trace_m`). Crucially it adds **zero** `distinct` and **zero**
/// `negate` (no negate-driven set-difference) and keeps exactly **one**
/// `ExchangeShard` (the pair-PK output) — `trace_m` carries no `PartitionFilter`.
/// The auxiliary `__m` view is registered separately and is a scalar `REDUCE`.
#[test]
fn test_pure_range_left_join_circuit_shape() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (mut client, sn) = make_planner(&srv);
    {
        let mut p = SqlPlanner::new(&mut client, &sn);
        p.execute("CREATE TABLE pls_a (id BIGINT NOT NULL PRIMARY KEY, x BIGINT NOT NULL)").unwrap();
        p.execute("CREATE TABLE pls_b (id BIGINT NOT NULL PRIMARY KEY, y BIGINT NOT NULL)").unwrap();
        // INNER and LEFT pure-range views over identical tables and ON clause.
        p.execute("CREATE VIEW pls_inner AS SELECT pls_a.id AS aid, pls_b.id AS bid \
                   FROM pls_a JOIN pls_b ON pls_a.x < pls_b.y").unwrap();
        p.execute("CREATE VIEW pls_left AS SELECT pls_a.id AS aid, pls_b.id AS bid \
                   FROM pls_a LEFT JOIN pls_b ON pls_a.x < pls_b.y").unwrap();
    }
    let inner = client.resolve_table_or_view_id(&sn, "pls_inner").unwrap().0;
    let left  = client.resolve_table_or_view_id(&sn, "pls_left").unwrap().0;

    let nodes = scan_circuit_nodes(&mut client);
    let nodes = nodes.as_ref();
    let n_inner = |op: u64| opcode_node_count(nodes, inner, op);
    let n_left  = |op: u64| opcode_node_count(nodes, left, op);

    // Threshold form: +2 range terms (4 total), +1 null_extend, +1 ScanDelta
    // source (__m), +1 IntegrateTrace (trace_m). NO distinct, NO negate.
    assert_eq!(n_left(OPCODE_JOIN_DELTA_TRACE_RANGE), 4, "inner 2 + null-fill 2 range terms");
    assert_eq!(n_inner(OPCODE_JOIN_DELTA_TRACE_RANGE), 2, "INNER pure-range has 2 range terms");
    assert_eq!(n_left(OPCODE_NULL_EXTEND) - n_inner(OPCODE_NULL_EXTEND), 1, "one null_extend");
    assert_eq!(n_left(OPCODE_SCAN_DELTA) - n_inner(OPCODE_SCAN_DELTA), 1, "third source: the __m delta");
    assert_eq!(n_left(OPCODE_INTEGRATE_TRACE) - n_inner(OPCODE_INTEGRATE_TRACE), 1, "trace_m");

    // The threshold null-fill is NOT the band set-difference.
    assert_eq!(n_left(OPCODE_DISTINCT), 0, "threshold form has no distinct");
    assert_eq!(n_left(OPCODE_NEGATE), 0, "threshold form has no negate");
    assert_eq!(n_inner(OPCODE_DISTINCT), 0, "INNER pure-range has no distinct");

    // Single output exchange; trace_m is replicated (no PartitionFilter), and the
    // inner's two PartitionFilters (one per side) are unchanged.
    assert_eq!(n_left(OPCODE_EXCHANGE_SHARD), 1, "exactly one pair-PK output exchange");
    assert_eq!(n_inner(OPCODE_EXCHANGE_SHARD), 1, "INNER pure-range has one output exchange");
    assert_eq!(n_left(OPCODE_PARTITION_FILTER), 2, "two side PartitionFilters; trace_m has none");

    // No outer/anti/semi/equi join machinery, no Delay.
    assert_eq!(n_left(OPCODE_JOIN_DELTA_TRACE_OUTER), 0, "no outer-join operator");
    assert_eq!(n_left(OPCODE_ANTI_JOIN_DELTA_TRACE), 0, "no anti-join");
    assert_eq!(n_left(OPCODE_SEMI_JOIN_DELTA_TRACE), 0, "no semi-join");
    assert_eq!(n_left(OPCODE_JOIN_DELTA_TRACE), 0, "no equi delta-trace join");
    assert_eq!(n_left(OPCODE_DELAY), 0, "no Delay node");

    // The auxiliary `__m` aggregate view is registered separately and is a scalar
    // REDUCE (it must not be counted as part of `pls_left`'s opcodes).
    let m_view = client.resolve_table_or_view_id(&sn, "__pls_left__m")
        .expect("the internal __m aggregate view should be registered").0;
    assert_eq!(opcode_node_count(nodes, m_view, OPCODE_REDUCE), 1, "__m is a scalar MIN/MAX reduce");
    assert_eq!(n_left(OPCODE_REDUCE), 0, "v_left itself carries no reduce");
}

/// Two range conjuncts are rejected (the one-range-conjunct rule).
#[test]
fn test_range_join_reject_two_range_conjuncts() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (mut client, sn) = make_planner(&srv);
    let mut p = SqlPlanner::new(&mut client, &sn);
    p.execute("CREATE TABLE r2_a (id BIGINT NOT NULL PRIMARY KEY, x BIGINT NOT NULL, w BIGINT NOT NULL)").unwrap();
    p.execute("CREATE TABLE r2_b (id BIGINT NOT NULL PRIMARY KEY, y BIGINT NOT NULL, z BIGINT NOT NULL)").unwrap();
    let err = must_err(p.execute("CREATE VIEW r2_v AS SELECT * FROM r2_a JOIN r2_b ON r2_a.x < r2_b.y AND r2_a.w > r2_b.z"));
    assert!(matches!(err, GnitzSqlError::Unsupported(_)), "expected Unsupported, got {:?}", err);
    assert!(client.resolve_table_or_view_id(&sn, "r2_v").is_err(), "no view should be registered");
}

/// A string/blob range pair is rejected (content hash is not order-preserving),
/// even though the same columns may equijoin.
#[test]
fn test_range_join_reject_string_range_pair() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (mut client, sn) = make_planner(&srv);
    let mut p = SqlPlanner::new(&mut client, &sn);
    p.execute("CREATE TABLE rs_a (id BIGINT NOT NULL PRIMARY KEY, s VARCHAR NOT NULL)").unwrap();
    p.execute("CREATE TABLE rs_b (id BIGINT NOT NULL PRIMARY KEY, s VARCHAR NOT NULL)").unwrap();
    let err = must_err(p.execute("CREATE VIEW rs_v AS SELECT * FROM rs_a JOIN rs_b ON rs_a.s < rs_b.s"));
    match err {
        GnitzSqlError::Unsupported(s) => assert!(s.contains("order-preserving"), "got: {s}"),
        e => panic!("expected Unsupported, got {:?}", e),
    }
}

/// A range self-join is rejected (the self-join guard fires for both join kinds).
#[test]
fn test_range_join_reject_self_join() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (mut client, sn) = make_planner(&srv);
    let mut p = SqlPlanner::new(&mut client, &sn);
    p.execute("CREATE TABLE rself (id BIGINT NOT NULL PRIMARY KEY, x BIGINT NOT NULL, y BIGINT NOT NULL)").unwrap();
    let err = must_err(p.execute("CREATE VIEW rself_v AS SELECT * FROM rself a JOIN rself b ON a.x < b.y"));
    assert!(matches!(err, GnitzSqlError::Unsupported(_)), "expected Unsupported, got {:?}", err);
}

/// The output pair-PK count cap: two 2-column-PK tables sum to a 4-column pair-PK
/// (accepted), but adding a third PK column each would exceed it. Here both PKs
/// are 2 columns, so pair-PK = 4 = cap → accepted.
#[test]
fn test_range_join_pair_pk_at_cap() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (mut client, sn) = make_planner(&srv);
    let mut p = SqlPlanner::new(&mut client, &sn);
    p.execute("CREATE TABLE rp_a (k1 BIGINT NOT NULL, k2 BIGINT NOT NULL, x BIGINT NOT NULL, PRIMARY KEY (k1, k2))").unwrap();
    p.execute("CREATE TABLE rp_b (k1 BIGINT NOT NULL, k2 BIGINT NOT NULL, y BIGINT NOT NULL, PRIMARY KEY (k1, k2))").unwrap();
    p.execute("CREATE VIEW rp_v AS SELECT rp_a.x, rp_b.y FROM rp_a JOIN rp_b ON rp_a.x < rp_b.y").unwrap();
    assert!(client.resolve_table_or_view_id(&sn, "rp_v").is_ok(), "4-column pair-PK is at the cap, should register");
}

// ── Range/band-join circuit shape (the §1 PartitionFilter split) ─────────────

/// The §1 split, pinned at the circuit level. A band join (`n_eq ≥ 1`) is
/// eq-prefix-scattered, so its per-side trace is already partitioned and the
/// circuit carries NO `PartitionFilter`. A pure range join (`n_eq == 0`)
/// broadcasts, so each side keeps a `PartitionFilter` to drop the rows the worker
/// does not own before integrating — two of them, one per side. Both are range
/// circuits: each has two `Join(DeltaTraceRange)` terms (the bilinear AB/BA form).
#[test]
fn test_range_join_partition_filter_circuit_shape() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (mut client, sn) = make_planner(&srv);
    {
        let mut p = SqlPlanner::new(&mut client, &sn);
        p.execute("CREATE TABLE cs_a (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, lo BIGINT NOT NULL)").unwrap();
        p.execute("CREATE TABLE cs_b (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, t BIGINT NOT NULL)").unwrap();
        // Band join (one eq conjunct + one range conjunct).
        p.execute("CREATE VIEW cs_band AS SELECT cs_a.id AS aid, cs_b.id AS bid \
                   FROM cs_a JOIN cs_b ON cs_a.k = cs_b.k AND cs_a.lo <= cs_b.t").unwrap();
        // Pure range join (no eq conjunct).
        p.execute("CREATE VIEW cs_pure AS SELECT cs_a.id AS aid, cs_b.id AS bid \
                   FROM cs_a JOIN cs_b ON cs_a.lo <= cs_b.t").unwrap();
    }
    let band = client.resolve_table_or_view_id(&sn, "cs_band").unwrap().0;
    let pure = client.resolve_table_or_view_id(&sn, "cs_pure").unwrap().0;

    // The nodes table is invariant once both circuits are built — scan it once and
    // count over the single batch.
    let nodes = scan_circuit_nodes(&mut client);
    let nodes = nodes.as_ref();

    assert_eq!(opcode_node_count(nodes, band, OPCODE_PARTITION_FILTER), 0,
        "band join scatters by the eq prefix — no PartitionFilter");
    assert_eq!(opcode_node_count(nodes, pure, OPCODE_PARTITION_FILTER), 2,
        "pure range join broadcasts — one PartitionFilter per side");

    assert_eq!(opcode_node_count(nodes, band, OPCODE_JOIN_DELTA_TRACE_RANGE), 2,
        "band join is a range circuit — two DeltaTraceRange terms");
    assert_eq!(opcode_node_count(nodes, pure, OPCODE_JOIN_DELTA_TRACE_RANGE), 2,
        "pure range join — two DeltaTraceRange terms");
}

// ── Band LEFT-join circuit shape (the null-fill set-difference) ───────────────

/// The band LEFT null-fill, pinned at the circuit level. Built over the SAME
/// tables as an INNER band view, the LEFT view adds exactly the set-difference
/// chain `distinct(π_A(inner))` → `negate` → `union` (A − D) → `null_extend` →
/// re-key → project → `union` (inner ∪ null-fill): `map_reindex ×3`, `map ×2`,
/// `distinct ×1`, `negate ×1`, `union ×2`, `null_extend ×1`. Crucially it adds
/// **zero** ExchangeShard, range/anti/semi/outer Join, IntegrateTrace, Delay, or
/// PartitionFilter — the null-fill is partition-local and reuses the inner output,
/// riding the one existing pair-PK output exchange. The INNER view is unchanged
/// (no distinct / null_extend / negate).
#[test]
fn test_band_left_join_circuit_shape() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (mut client, sn) = make_planner(&srv);
    {
        let mut p = SqlPlanner::new(&mut client, &sn);
        p.execute("CREATE TABLE bls_a (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, lo BIGINT NOT NULL)").unwrap();
        p.execute("CREATE TABLE bls_b (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, t BIGINT NOT NULL)").unwrap();
        // INNER and LEFT band views over identical tables and ON clause.
        p.execute("CREATE VIEW bls_inner AS SELECT bls_a.id AS aid, bls_b.id AS bid \
                   FROM bls_a JOIN bls_b ON bls_a.k = bls_b.k AND bls_a.lo <= bls_b.t").unwrap();
        p.execute("CREATE VIEW bls_left AS SELECT bls_a.id AS aid, bls_b.id AS bid \
                   FROM bls_a LEFT JOIN bls_b ON bls_a.k = bls_b.k AND bls_a.lo <= bls_b.t").unwrap();
    }
    let inner = client.resolve_table_or_view_id(&sn, "bls_inner").unwrap().0;
    let left  = client.resolve_table_or_view_id(&sn, "bls_left").unwrap().0;

    let nodes = scan_circuit_nodes(&mut client);
    let nodes = nodes.as_ref();
    let n_inner = |op: u64| opcode_node_count(nodes, inner, op);
    let n_left  = |op: u64| opcode_node_count(nodes, left, op);

    // The null-fill adds exactly this set-difference chain over the inner skeleton.
    assert_eq!(n_left(OPCODE_MAP_EXPR)    - n_inner(OPCODE_MAP_EXPR),    3, "rekey_a + a_all + nf_rekey");
    assert_eq!(n_left(OPCODE_MAP_PROJ)    - n_inner(OPCODE_MAP_PROJ),    2, "proj_a + nf_proj");
    assert_eq!(n_left(OPCODE_DISTINCT)    - n_inner(OPCODE_DISTINCT),    1, "one distinct(π_A(inner))");
    assert_eq!(n_left(OPCODE_NEGATE)      - n_inner(OPCODE_NEGATE),      1, "negate(D) for A − D");
    assert_eq!(n_left(OPCODE_UNION)       - n_inner(OPCODE_UNION),       2, "A − D diff + inner ∪ null-fill");
    assert_eq!(n_left(OPCODE_NULL_EXTEND) - n_inner(OPCODE_NULL_EXTEND), 1, "null_extend(diff)");

    // No extra exchange, no extra join machinery, no extra trace/delay/partition —
    // the null-fill is partition-local and reuses the inner range output.
    assert_eq!(n_left(OPCODE_EXCHANGE_SHARD),      n_inner(OPCODE_EXCHANGE_SHARD), "no added ExchangeShard");
    assert_eq!(n_left(OPCODE_JOIN_DELTA_TRACE_RANGE), n_inner(OPCODE_JOIN_DELTA_TRACE_RANGE), "no added range Join");
    assert_eq!(n_left(OPCODE_INTEGRATE_TRACE),     n_inner(OPCODE_INTEGRATE_TRACE), "no added IntegrateTrace");
    assert_eq!(n_left(OPCODE_PARTITION_FILTER), 0, "band LEFT carries no PartitionFilter");
    assert_eq!(n_left(OPCODE_DELAY), 0, "no Delay node");
    assert_eq!(n_left(OPCODE_ANTI_JOIN_DELTA_TRACE), 0, "set-difference form uses no anti-join");
    assert_eq!(n_left(OPCODE_SEMI_JOIN_DELTA_TRACE), 0, "no semi-join");
    assert_eq!(n_left(OPCODE_JOIN_DELTA_TRACE), 0, "no equi delta-trace join");
    assert_eq!(n_left(OPCODE_JOIN_DELTA_TRACE_OUTER), 0, "no outer-join operator");

    // Exactly one distinct, and the INNER view is shape-unchanged by the LEFT path.
    assert_eq!(n_left(OPCODE_DISTINCT), 1, "exactly one distinct in the band LEFT circuit");
    assert_eq!(n_inner(OPCODE_DISTINCT), 0, "INNER band view has no distinct");
    assert_eq!(n_inner(OPCODE_NULL_EXTEND), 0, "INNER band view has no null_extend");
    assert_eq!(n_inner(OPCODE_NEGATE), 0, "INNER band view has no negate");
}
