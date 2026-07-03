#![cfg(feature = "integration")]

use gnitz_core::{
    OPCODE_DELAY, OPCODE_DISTINCT, OPCODE_EXCHANGE_SHARD, OPCODE_INTEGRATE_TRACE, OPCODE_JOIN_DELTA_TRACE,
    OPCODE_JOIN_DELTA_TRACE_RANGE, OPCODE_MAP_EXPR, OPCODE_MAP_PROJ, OPCODE_NEGATE, OPCODE_NULL_EXTEND,
    OPCODE_PARTITION_FILTER, OPCODE_POSITIVE_PART, OPCODE_REDUCE, OPCODE_SCAN_DELTA, OPCODE_UNION,
};
use gnitz_sql::{GnitzSqlError, SqlPlanner};
use gnitz_test_harness::ServerHandle;

mod common;
use common::*;

// ── CREATE VIEW JOIN — SELECT item variants ──────────────────────────────────

/// Qualified alias: `left_t.col AS alias` — previously fell through to unsupported.
#[test]
fn test_join_select_qualified_alias() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    let mut p = SqlPlanner::new(&mut client, &sn);

    p.execute("CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, fk BIGINT NOT NULL, v BIGINT NOT NULL)")
        .unwrap();
    p.execute("CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, fk BIGINT NOT NULL, w BIGINT NOT NULL)")
        .unwrap();

    let r = p.execute(
        "CREATE VIEW v AS \
         SELECT a.id AS aid, b.id AS bid, a.v AS aval, b.w AS bval \
         FROM a JOIN b ON a.fk = b.fk",
    );
    assert!(r.is_ok(), "qualified alias in JOIN SELECT failed: {:?}", r.err());
}

/// Unqualified alias: `col AS alias`.
#[test]
fn test_join_select_unqualified_alias() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    let mut p = SqlPlanner::new(&mut client, &sn);

    p.execute("CREATE TABLE x (id BIGINT NOT NULL PRIMARY KEY, fk BIGINT NOT NULL, xv BIGINT NOT NULL)")
        .unwrap();
    p.execute("CREATE TABLE y (id BIGINT NOT NULL PRIMARY KEY, fk BIGINT NOT NULL, yv BIGINT NOT NULL)")
        .unwrap();

    let r = p.execute("CREATE VIEW v AS SELECT xv AS xcol, yv AS ycol FROM x JOIN y ON x.fk = y.fk");
    assert!(r.is_ok(), "unqualified alias in JOIN SELECT failed: {:?}", r.err());
}

/// No aliases — table-qualified column refs.
#[test]
fn test_join_select_no_alias() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    let mut p = SqlPlanner::new(&mut client, &sn);

    p.execute("CREATE TABLE p (id BIGINT NOT NULL PRIMARY KEY, fk BIGINT NOT NULL)")
        .unwrap();
    p.execute("CREATE TABLE q (id BIGINT NOT NULL PRIMARY KEY, fk BIGINT NOT NULL, qv BIGINT NOT NULL)")
        .unwrap();

    let r = p.execute("CREATE VIEW v AS SELECT p.id, q.qv FROM p JOIN q ON p.fk = q.fk");
    assert!(r.is_ok(), "qualified refs without alias failed: {:?}", r.err());
}

/// Wildcard SELECT *.
#[test]
fn test_join_select_wildcard() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    let mut p = SqlPlanner::new(&mut client, &sn);

    p.execute("CREATE TABLE l (id BIGINT NOT NULL PRIMARY KEY, fk BIGINT NOT NULL)")
        .unwrap();
    p.execute("CREATE TABLE r (id BIGINT NOT NULL PRIMARY KEY, fk BIGINT NOT NULL)")
        .unwrap();

    let r = p.execute("CREATE VIEW v AS SELECT * FROM l JOIN r ON l.fk = r.fk");
    assert!(r.is_ok(), "JOIN SELECT * failed: {:?}", r.err());
}

/// Mixed: alias + no-alias in same SELECT.
#[test]
fn test_join_select_mixed_alias_and_bare() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    let mut p = SqlPlanner::new(&mut client, &sn);

    p.execute("CREATE TABLE m (id BIGINT NOT NULL PRIMARY KEY, fk BIGINT NOT NULL, mv BIGINT NOT NULL)")
        .unwrap();
    p.execute("CREATE TABLE n (id BIGINT NOT NULL PRIMARY KEY, fk BIGINT NOT NULL, nv BIGINT NOT NULL)")
        .unwrap();

    let r = p.execute("CREATE VIEW v AS SELECT m.id AS mid, n.nv FROM m JOIN n ON m.fk = n.fk");
    assert!(r.is_ok(), "mixed alias/bare in JOIN SELECT failed: {:?}", r.err());
}

// ── Composite (multi-column) equijoin keys ───────────────────────────────────

/// Newly accepted at k = 1: a parenthesized single-pair ON now succeeds
/// (Expr::Nested is unwrapped, matching the WHERE/HAVING paths). Previously
/// rejected with Unsupported.
#[test]
fn test_join_paren_single_pair_accepted() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    let mut p = SqlPlanner::new(&mut client, &sn);

    p.execute("CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, fk BIGINT NOT NULL)")
        .unwrap();
    p.execute("CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, fk BIGINT NOT NULL)")
        .unwrap();

    let r = p.execute("CREATE VIEW v AS SELECT * FROM a JOIN b ON (a.fk = b.fk)");
    assert!(r.is_ok(), "parenthesized single-pair ON failed: {:?}", r.err());
}

/// Planner rejection: a float join key. Fires from validate_join_key_pair inside
/// extraction, before any circuit is built.
#[test]
fn test_join_reject_float_key() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    let mut p = SqlPlanner::new(&mut client, &sn);

    p.execute("CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, fk DOUBLE NOT NULL)")
        .unwrap();
    p.execute("CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, fk DOUBLE NOT NULL)")
        .unwrap();

    let err = p
        .execute("CREATE VIEW v AS SELECT * FROM a JOIN b ON a.fk = b.fk")
        .unwrap_err();
    assert!(
        matches!(err, GnitzSqlError::Unsupported(_)),
        "expected Unsupported, got {:?}",
        err
    );
    assert!(
        client.resolve_table_or_view_id(&sn, "v").is_err(),
        "no view should be registered"
    );
}

/// Planner acceptance: a cross-sign integer pair whose unsigned side is ≤ 4 bytes
/// (`INT UNSIGNED` = `BIGINT`, i.e. U32 = I64). The pair promotes to the narrowest
/// signed type holding both ranges (I64) and co-partitions byte-for-byte, so
/// CREATE VIEW succeeds and the view resolves.
#[test]
fn test_join_accept_cross_sign_narrow() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    let mut p = SqlPlanner::new(&mut client, &sn);

    p.execute("CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, fk INT UNSIGNED NOT NULL)")
        .unwrap();
    p.execute("CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, fk BIGINT NOT NULL)")
        .unwrap();

    let r = p.execute("CREATE VIEW v AS SELECT * FROM a JOIN b ON a.fk = b.fk");
    assert!(
        r.is_ok(),
        "narrow cross-sign join (U32 = I64) should be accepted: {:?}",
        r.err()
    );
    assert!(
        client.resolve_table_or_view_id(&sn, "v").is_ok(),
        "view should be registered"
    );

    // A second pair: TINYINT UNSIGNED (U8) = SMALLINT (I16), promoting to I16.
    let mut p = SqlPlanner::new(&mut client, &sn);
    p.execute("CREATE TABLE c (id BIGINT NOT NULL PRIMARY KEY, fk TINYINT UNSIGNED NOT NULL)")
        .unwrap();
    p.execute("CREATE TABLE d (id BIGINT NOT NULL PRIMARY KEY, fk SMALLINT NOT NULL)")
        .unwrap();
    let r = p.execute("CREATE VIEW w AS SELECT * FROM c JOIN d ON c.fk = d.fk");
    assert!(
        r.is_ok(),
        "narrow cross-sign join (U8 = I16) should be accepted: {:?}",
        r.err()
    );
    assert!(
        client.resolve_table_or_view_id(&sn, "w").is_ok(),
        "view should be registered"
    );
}

/// Planner acceptance: a cross-sign pair whose unsigned side is `U64`
/// (`BIGINT UNSIGNED` = `BIGINT`, i.e. U64 = I64). Its faithful common type is the
/// signed-128 type `I128`; the pair promotes to it and co-partitions byte-for-byte,
/// so CREATE VIEW succeeds and the view resolves.
#[test]
fn test_join_accept_cross_sign_u64() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    let mut p = SqlPlanner::new(&mut client, &sn);

    p.execute("CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, fk BIGINT UNSIGNED NOT NULL)")
        .unwrap();
    p.execute("CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, fk BIGINT NOT NULL)")
        .unwrap();

    let r = p.execute("CREATE VIEW v AS SELECT * FROM a JOIN b ON a.fk = b.fk");
    assert!(
        r.is_ok(),
        "cross-sign join (U64 = I64) should be accepted: {:?}",
        r.err()
    );
    assert!(
        client.resolve_table_or_view_id(&sn, "v").is_ok(),
        "view should be registered"
    );
}

/// Planner rejection: the surviving cross-sign reject — the unsigned side is
/// 128-bit (`DECIMAL(38,0)`/`UUID` = a signed integer). Its faithful common type
/// is a signed-256 type that does not exist, so the join is rejected at CREATE.
#[test]
fn test_join_reject_cross_sign() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    let mut p = SqlPlanner::new(&mut client, &sn);

    p.execute("CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, fk DECIMAL(38,0) NOT NULL)")
        .unwrap();
    p.execute("CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, fk BIGINT NOT NULL)")
        .unwrap();

    let err = p
        .execute("CREATE VIEW v AS SELECT * FROM a JOIN b ON a.fk = b.fk")
        .unwrap_err();
    assert!(
        matches!(err, GnitzSqlError::Unsupported(_)),
        "expected Unsupported, got {:?}",
        err
    );
    assert!(
        client.resolve_table_or_view_id(&sn, "v").is_err(),
        "no view should be registered"
    );
}

/// Planner rejection: a mixed string/native pair (VARCHAR = U128). A string
/// content hash never equals a native key, so it is rejected at CREATE.
#[test]
fn test_join_reject_mixed_string_native() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    let mut p = SqlPlanner::new(&mut client, &sn);

    p.execute("CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, s VARCHAR NOT NULL)")
        .unwrap();
    p.execute("CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, n DECIMAL(38,0) NOT NULL)")
        .unwrap();

    let err = p
        .execute("CREATE VIEW v AS SELECT * FROM a JOIN b ON a.s = b.n")
        .unwrap_err();
    assert!(
        matches!(err, GnitzSqlError::Unsupported(_)),
        "expected Unsupported, got {:?}",
        err
    );
    assert!(
        client.resolve_table_or_view_id(&sn, "v").is_err(),
        "no view should be registered"
    );
}

/// Planner rejection: an ON conjunction one wider than the codec cap is rejected
/// as a clean `Unsupported` (not a `pack_pk_cols` panic). Written relative to
/// `PK_LIST_MAX_COLS` so it tracks a future bump rather than hardcoding the count.
#[test]
fn test_join_reject_arity_cap() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    let mut p = SqlPlanner::new(&mut client, &sn);

    let n = gnitz_core::PK_LIST_MAX_COLS + 1; // one past the cap
    let key_cols = (0..n)
        .map(|i| format!("c{i} BIGINT NOT NULL"))
        .collect::<Vec<_>>()
        .join(", ");
    p.execute(&format!("CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, {key_cols})"))
        .unwrap();
    p.execute(&format!("CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, {key_cols})"))
        .unwrap();

    let on = (0..n)
        .map(|i| format!("a.c{i} = b.c{i}"))
        .collect::<Vec<_>>()
        .join(" AND ");
    let err = p
        .execute(&format!("CREATE VIEW v AS SELECT * FROM a JOIN b ON {on}"))
        .unwrap_err();
    match err {
        GnitzSqlError::Unsupported(s) => assert!(
            s.contains("equijoin key columns") && s.contains(&n.to_string()),
            "expected the arity-cap message naming {n}, got: {s}"
        ),
        e => panic!("expected Unsupported, got {:?}", e),
    }
    assert!(
        client.resolve_table_or_view_id(&sn, "v").is_err(),
        "no view should be registered"
    );
}

/// k = PK_LIST_MAX_COLS (the widest accepted composite key) registers cleanly,
/// carrying a k-column synthetic `_join_pk`. Guards the accept side of the cap.
#[test]
fn test_join_max_arity_accepted() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    let k = gnitz_core::PK_LIST_MAX_COLS;
    {
        let mut p = SqlPlanner::new(&mut client, &sn);
        let key_cols = (0..k)
            .map(|i| format!("c{i} BIGINT NOT NULL"))
            .collect::<Vec<_>>()
            .join(", ");
        p.execute(&format!("CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, {key_cols})"))
            .unwrap();
        p.execute(&format!("CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, {key_cols})"))
            .unwrap();
        let on = (0..k)
            .map(|i| format!("a.c{i} = b.c{i}"))
            .collect::<Vec<_>>()
            .join(" AND ");
        p.execute(&format!("CREATE VIEW v AS SELECT * FROM a JOIN b ON {on}"))
            .unwrap();
    }
    let (_, s) = client.resolve_table_or_view_id(&sn, "v").unwrap();
    assert_eq!(
        s.pk_indices(),
        (0..k).collect::<Vec<usize>>().as_slice(),
        "the k synthetic _join_pk columns are the view PK"
    );
}

/// k = 2 composite equijoin: the view registers with a 2-column synthetic
/// `_join_pk`, and incremental INNER-join contents match a full recompute.
/// (The prior fail-closed gate is removed; this is its deliberate successor.)
#[test]
fn test_join_composite_two_pair_inner_data() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    exec(
        &mut client,
        &sn,
        "CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, x BIGINT NOT NULL, y BIGINT NOT NULL, av BIGINT NOT NULL)",
    );
    exec(
        &mut client,
        &sn,
        "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, x BIGINT NOT NULL, y BIGINT NOT NULL, bv BIGINT NOT NULL)",
    );
    exec(
        &mut client,
        &sn,
        "CREATE VIEW v AS SELECT a.x, a.y, a.av, b.bv FROM a JOIN b ON a.x = b.x AND a.y = b.y",
    );

    // The view PK is the 2 synthetic join-key columns.
    let (_, s) = client.resolve_table_or_view_id(&sn, "v").unwrap();
    assert_eq!(s.pk_indices(), &[0, 1], "k=2 join PK is the two _join_pk columns");

    // (x,y) matches: (10,100) a1·b1, (30,300) a3·b3. (20,*) differs in y → no match.
    exec(
        &mut client,
        &sn,
        "INSERT INTO a (id, x, y, av) VALUES (1, 10, 100, 1), (2, 20, 200, 2), (3, 30, 300, 3)",
    );
    exec(
        &mut client,
        &sn,
        "INSERT INTO b (id, x, y, bv) VALUES (1, 10, 100, 11), (2, 20, 999, 22), (3, 30, 300, 33)",
    );
    assert_eq!(
        payload_rows(&mut client, &sn, "v", &["x", "y", "av", "bv"]),
        vec![vec![10, 100, 1, 11], vec![30, 300, 3, 33]],
        "INNER k=2 join keeps only rows agreeing on both key columns"
    );

    // Incremental: a later b row that completes the (20,200) pair must join in.
    exec(&mut client, &sn, "INSERT INTO b (id, x, y, bv) VALUES (4, 20, 200, 44)");
    assert_eq!(
        payload_rows(&mut client, &sn, "v", &["x", "y", "av", "bv"]),
        vec![vec![10, 100, 1, 11], vec![20, 200, 2, 44], vec![30, 300, 3, 33]],
        "incremental INNER join admits the newly-completed composite-key pair"
    );
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
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    exec(&mut client, &sn,
        "CREATE TABLE a (k1 BIGINT NOT NULL, k2 BIGINT NOT NULL, fk BIGINT NOT NULL, av BIGINT NOT NULL, PRIMARY KEY (k1, k2))");
    exec(
        &mut client,
        &sn,
        "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, fk BIGINT NOT NULL, bv BIGINT NOT NULL)",
    );
    exec(
        &mut client,
        &sn,
        "CREATE VIEW v AS SELECT a.k1, a.k2, a.av, b.bv FROM a JOIN b ON a.fk = b.fk",
    );

    let (_, s) = client.resolve_table_or_view_id(&sn, "v").unwrap();
    assert_eq!(
        s.pk_indices(),
        &[0],
        "single-key join over a compound-PK source: output PK is the lone _join_pk"
    );

    exec(
        &mut client,
        &sn,
        "INSERT INTO a (k1, k2, fk, av) VALUES (1, 100, 7, 11), (2, 200, 9, 22)",
    );
    exec(
        &mut client,
        &sn,
        "INSERT INTO b (id, fk, bv) VALUES (1, 7, 70), (2, 9, 90)",
    );

    assert_eq!(
        payload_rows(&mut client, &sn, "v", &["k1", "k2", "av", "bv"]),
        vec![vec![1, 100, 11, 70], vec![2, 200, 22, 90]],
        "compound source PK rides as payload, decoded to native values"
    );
}

/// Composite join key over two compound-PK sources: output PK is the 2 synthetic
/// `_join_pk` columns; the key is drawn from non-PK columns of both sides.
#[test]
fn test_join_compound_pk_source_composite_key() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    exec(&mut client, &sn,
        "CREATE TABLE a (k1 BIGINT NOT NULL, k2 BIGINT NOT NULL, x BIGINT NOT NULL, y BIGINT NOT NULL, av BIGINT NOT NULL, PRIMARY KEY (k1, k2))");
    exec(&mut client, &sn,
        "CREATE TABLE b (j1 BIGINT NOT NULL, j2 BIGINT NOT NULL, x BIGINT NOT NULL, y BIGINT NOT NULL, bv BIGINT NOT NULL, PRIMARY KEY (j1, j2))");
    exec(
        &mut client,
        &sn,
        "CREATE VIEW v AS SELECT a.k1, a.av, b.bv FROM a JOIN b ON a.x = b.x AND a.y = b.y",
    );

    let (_, s) = client.resolve_table_or_view_id(&sn, "v").unwrap();
    assert_eq!(
        s.pk_indices(),
        &[0, 1],
        "k=2 join over compound sources: output PK is the two _join_pk cols"
    );

    exec(
        &mut client,
        &sn,
        "INSERT INTO a (k1, k2, x, y, av) VALUES (1, 1, 10, 100, 1), (2, 2, 20, 200, 2)",
    );
    exec(
        &mut client,
        &sn,
        "INSERT INTO b (j1, j2, x, y, bv) VALUES (5, 5, 10, 100, 11), (6, 6, 20, 999, 22)",
    );
    assert_eq!(
        payload_rows(&mut client, &sn, "v", &["k1", "av", "bv"]),
        vec![vec![1, 1, 11]],
        "(20,200) vs (20,999) disagree on y — only (10,100) joins"
    );

    exec(
        &mut client,
        &sn,
        "INSERT INTO b (j1, j2, x, y, bv) VALUES (7, 7, 20, 200, 44)",
    );
    assert_eq!(
        payload_rows(&mut client, &sn, "v", &["k1", "av", "bv"]),
        vec![vec![1, 1, 11], vec![2, 2, 44]],
        "incremental join admits the newly-completed pair"
    );
}

/// LEFT join whose left source has a compound PK: unmatched left rows emit
/// `(left payload incl. compound PK, NULL right)`.
#[test]
fn test_join_compound_pk_source_left_outer() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    exec(
        &mut client,
        &sn,
        "CREATE TABLE a (k1 BIGINT NOT NULL, k2 BIGINT NOT NULL, fk BIGINT NOT NULL, \
         av BIGINT NOT NULL, PRIMARY KEY (k1, k2))",
    );
    exec(
        &mut client,
        &sn,
        "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, fk BIGINT NOT NULL, bv BIGINT NOT NULL)",
    );
    exec(
        &mut client,
        &sn,
        "CREATE VIEW v AS SELECT a.k1, a.k2, a.av, b.bv FROM a LEFT JOIN b ON a.fk = b.fk",
    );

    exec(
        &mut client,
        &sn,
        "INSERT INTO a (k1, k2, fk, av) VALUES (1, 100, 7, 11), (2, 200, 9, 22)",
    );
    exec(&mut client, &sn, "INSERT INTO b (id, fk, bv) VALUES (1, 7, 70)");

    let (schema, batch) = read_view(&mut client, &sn, "v");
    assert_eq!(batch.len(), 2);

    let k1_ci = col_idx(&schema, "k1");
    let k2_ci = col_idx(&schema, "k2");
    let av_ci = col_idx(&schema, "av");
    let bv_ci = col_idx(&schema, "bv");
    // bv_payload_idx: schema [_join_pk(pk), k1, k2, av, bv]; 1 PK col; bv payload = bv_ci - 1
    let bv_payload_idx = bv_ci - schema.pk_cols.len();

    let (r_matched, r_unmatched) = if i64_at(&batch, k1_ci, 0) == 1 { (0, 1) } else { (1, 0) };

    // Matched row (fk=7)
    assert!(!is_null_at(&batch, bv_payload_idx, r_matched));
    assert_eq!(i64_at(&batch, bv_ci, r_matched), 70);

    // Unmatched row (fk=9): compound source PK intact, right side null
    assert_eq!(i64_at(&batch, k1_ci, r_unmatched), 2);
    assert_eq!(i64_at(&batch, k2_ci, r_unmatched), 200);
    assert_eq!(i64_at(&batch, av_ci, r_unmatched), 22);
    assert!(
        is_null_at(&batch, bv_payload_idx, r_unmatched),
        "unmatched left row: bv must be null"
    );
}

/// Wide (> 16-byte) compound source PK: a 3-BIGINT PK (24-byte stride). Two source
/// rows that share the first 16 OPK bytes and differ only past byte 16 must
/// survive ingest/scan as distinct and join independently.
#[test]
fn test_join_compound_pk_source_wide_tiebreak() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    exec(
        &mut client,
        &sn,
        "CREATE TABLE a (k1 BIGINT NOT NULL, k2 BIGINT NOT NULL, k3 BIGINT NOT NULL, \
         fk BIGINT NOT NULL, av BIGINT NOT NULL, PRIMARY KEY (k1, k2, k3))",
    );
    exec(
        &mut client,
        &sn,
        "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, fk BIGINT NOT NULL, bv BIGINT NOT NULL)",
    );
    exec(
        &mut client,
        &sn,
        "CREATE VIEW v AS SELECT a.k1, a.k2, a.k3, a.av, b.bv FROM a JOIN b ON a.fk = b.fk",
    );

    let (_, s) = client.resolve_table_or_view_id(&sn, "v").unwrap();
    assert_eq!(s.pk_indices(), &[0]);

    // (1,1,1) and (1,1,2) share the first 16 OPK bytes; differ only at byte 16+.
    exec(
        &mut client,
        &sn,
        "INSERT INTO a (k1, k2, k3, fk, av) VALUES (1, 1, 1, 7, 11), (1, 1, 2, 7, 22)",
    );
    exec(&mut client, &sn, "INSERT INTO b (id, fk, bv) VALUES (1, 7, 70)");

    assert_eq!(
        payload_rows(&mut client, &sn, "v", &["k1", "k2", "k3", "av", "bv"]),
        vec![vec![1, 1, 1, 11, 70], vec![1, 1, 2, 22, 70]],
        "rows sharing first 16 OPK bytes survive as distinct and both join"
    );
}

/// Incremental UPDATE / DELETE of source rows over a compound-PK source.
#[test]
fn test_join_compound_pk_source_update_delete() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    exec(&mut client, &sn,
        "CREATE TABLE a (k1 BIGINT NOT NULL, k2 BIGINT NOT NULL, fk BIGINT NOT NULL, av BIGINT NOT NULL, PRIMARY KEY (k1, k2))");
    exec(
        &mut client,
        &sn,
        "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, fk BIGINT NOT NULL, bv BIGINT NOT NULL)",
    );
    exec(
        &mut client,
        &sn,
        "CREATE VIEW v AS SELECT a.k1, a.k2, a.av, b.bv FROM a JOIN b ON a.fk = b.fk",
    );

    exec(
        &mut client,
        &sn,
        "INSERT INTO a (k1, k2, fk, av) VALUES (1, 100, 7, 11), (2, 200, 9, 22)",
    );
    exec(
        &mut client,
        &sn,
        "INSERT INTO b (id, fk, bv) VALUES (1, 7, 70), (2, 9, 90)",
    );
    assert_eq!(
        payload_rows(&mut client, &sn, "v", &["k1", "k2", "av", "bv"]),
        vec![vec![1, 100, 11, 70], vec![2, 200, 22, 90]]
    );

    // UPDATE a source payload column: retract+insert reflected on next read.
    exec(&mut client, &sn, "UPDATE a SET av = 111 WHERE k1 = 1 AND k2 = 100");
    assert_eq!(
        payload_rows(&mut client, &sn, "v", &["k1", "k2", "av", "bv"]),
        vec![vec![1, 100, 111, 70], vec![2, 200, 22, 90]],
        "incremental UPDATE recomputes only the touched join row"
    );

    // DELETE a source row: removes its join output.
    exec(&mut client, &sn, "DELETE FROM a WHERE k1 = 2 AND k2 = 200");
    assert_eq!(
        payload_rows(&mut client, &sn, "v", &["k1", "k2", "av", "bv"]),
        vec![vec![1, 100, 111, 70]],
        "incremental DELETE drops the removed source row's join output"
    );
}

// ── Duplicate output column names rejected (join view) ────────────────

#[test]
fn test_join_duplicate_output_columns_rejected() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    let mut p = SqlPlanner::new(&mut client, &sn);
    p.execute("CREATE TABLE jl_dup (id BIGINT PRIMARY KEY, a BIGINT NOT NULL)")
        .unwrap();
    p.execute("CREATE TABLE jr_dup (id BIGINT PRIMARY KEY, b BIGINT NOT NULL)")
        .unwrap();
    let err = p
        .execute(
            "CREATE VIEW v_jn_dup AS SELECT jl_dup.a AS x, jr_dup.b AS x \
         FROM jl_dup JOIN jr_dup ON jl_dup.id = jr_dup.id",
        )
        .unwrap_err();
    match err {
        GnitzSqlError::Plan(s) => assert!(s.contains("duplicate column"), "got: {}", s),
        e => panic!("expected Plan, got {:?}", e),
    }
}

// ── Range / band join: planner admission ─────────────────────────────

/// A pure range join registers cleanly: its output PK is the source-PK pair.
#[test]
fn test_range_join_accepts_pure_range() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    let mut p = SqlPlanner::new(&mut client, &sn);
    p.execute("CREATE TABLE rj_a (id BIGINT NOT NULL PRIMARY KEY, x BIGINT NOT NULL)")
        .unwrap();
    p.execute("CREATE TABLE rj_b (id BIGINT NOT NULL PRIMARY KEY, y BIGINT NOT NULL)")
        .unwrap();
    // Alias the two `id` columns so the projection has no duplicate output names.
    p.execute("CREATE VIEW rj_v AS SELECT rj_a.id AS aid, rj_b.id AS bid, rj_b.y AS by FROM rj_a JOIN rj_b ON rj_a.x < rj_b.y").unwrap();
    assert!(
        client.resolve_table_or_view_id(&sn, "rj_v").is_ok(),
        "range-join view should register"
    );
}

/// A band join (eq prefix + range) registers cleanly.
#[test]
fn test_range_join_accepts_band() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    let mut p = SqlPlanner::new(&mut client, &sn);
    p.execute("CREATE TABLE bj_a (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, lo BIGINT NOT NULL)")
        .unwrap();
    p.execute("CREATE TABLE bj_b (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, t BIGINT NOT NULL)")
        .unwrap();
    p.execute("CREATE VIEW bj_v AS SELECT * FROM bj_a JOIN bj_b ON bj_a.k = bj_b.k AND bj_a.lo <= bj_b.t")
        .unwrap();
    assert!(
        client.resolve_table_or_view_id(&sn, "bj_v").is_ok(),
        "band-join view should register"
    );
}

/// A band LEFT JOIN (≥1 equality conjunct + a range conjunct) registers cleanly —
/// the null-fill is partition-local on the eq-prefix scatter.
#[test]
fn test_band_left_join_accepts() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    let mut p = SqlPlanner::new(&mut client, &sn);
    p.execute("CREATE TABLE bl_a (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, lo BIGINT NOT NULL)")
        .unwrap();
    p.execute("CREATE TABLE bl_b (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, t BIGINT NOT NULL)")
        .unwrap();
    p.execute(
        "CREATE VIEW bl_v AS SELECT bl_a.id AS aid, bl_b.id AS bid \
               FROM bl_a LEFT JOIN bl_b ON bl_a.k = bl_b.k AND bl_a.lo <= bl_b.t",
    )
    .unwrap();
    assert!(
        client.resolve_table_or_view_id(&sn, "bl_v").is_ok(),
        "band LEFT-join view should register"
    );
}

/// A pure-range LEFT JOIN (no equality conjunct) now registers: existence of a
/// match collapses to a threshold test against a single scalar — `MAX`/`MIN` of
/// the right range column — so the null-fill is the subtraction `A − (A ⋈ {m})`
/// against a one-row `m`. That rides the single pair-PK output exchange like the
/// inner pairs (no second sequential exchange). The scalar `m` is computed by an
/// INLINE shard-free reduce over the broadcast `b` — no catalog helpers, no
/// sentinel.
#[test]
fn test_pure_range_left_join_accepts() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    let mut p = SqlPlanner::new(&mut client, &sn);
    p.execute("CREATE TABLE rl_a (id BIGINT NOT NULL PRIMARY KEY, x BIGINT NOT NULL)")
        .unwrap();
    p.execute("CREATE TABLE rl_b (id BIGINT NOT NULL PRIMARY KEY, y BIGINT NOT NULL)")
        .unwrap();
    assert!(
        p.execute(
            "CREATE VIEW rl_v AS SELECT rl_a.id AS aid, rl_b.id AS bid \
         FROM rl_a LEFT JOIN rl_b ON rl_a.x < rl_b.y"
        )
        .is_ok(),
        "pure-range LEFT JOIN should now register"
    );
    assert!(
        client.resolve_table_or_view_id(&sn, "rl_v").is_ok(),
        "pure-range LEFT-join view should register"
    );
}

/// A pure-range LEFT JOIN whose promoted compare type is 16-byte (here `U64` vs
/// `I64` → `I128`) is rejected: the threshold null-fill reduces the range column
/// with `MIN`/`MAX`, which has no 16-byte accumulator. A ≤8-byte integer range
/// column is now accepted (see `test_pure_range_left_join_accepts_narrow_int`);
/// only the 16-byte case stays unsupported. Pins that the planner returns a clean
/// `Unsupported` up front rather than failing the compile.
#[test]
fn test_pure_range_left_join_rejects_wide_compare_type() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    let mut p = SqlPlanner::new(&mut client, &sn);
    p.execute("CREATE TABLE rlw_a (id BIGINT NOT NULL PRIMARY KEY, x BIGINT UNSIGNED NOT NULL)")
        .unwrap();
    p.execute("CREATE TABLE rlw_b (id BIGINT NOT NULL PRIMARY KEY, y BIGINT NOT NULL)")
        .unwrap();
    let err = p
        .execute(
            "CREATE VIEW rlw_v AS SELECT rlw_a.id AS aid, rlw_b.id AS bid \
         FROM rlw_a LEFT JOIN rlw_b ON rlw_a.x < rlw_b.y",
        )
        .unwrap_err();
    match err {
        GnitzSqlError::Unsupported(s) => assert!(s.contains("16-byte accumulator"), "got: {s}"),
        e => panic!("expected Unsupported, got {:?}", e),
    }
    assert!(
        client.resolve_table_or_view_id(&sn, "rlw_v").is_err(),
        "no view should be registered"
    );
}

/// Narrow integer range columns (`INT`/I32, `INT UNSIGNED`/U32, `SMALLINT`/I16)
/// register for a pure-range LEFT JOIN. MIN/MAX preserves the source integer type,
/// so the inline threshold reduce emits the narrow type that `reindex_m` consumes
/// at its native width directly — no widen-to-I64 step to break the reindex. These
/// do NOT compile under the old `{I64, U64}`-only cap.
#[test]
fn test_pure_range_left_join_accepts_narrow_int() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    let types = ["INT", "INT UNSIGNED", "SMALLINT"];
    {
        let mut p = SqlPlanner::new(&mut client, &sn);
        for (i, ty) in types.iter().enumerate() {
            p.execute(&format!(
                "CREATE TABLE na_{i} (id BIGINT NOT NULL PRIMARY KEY, x {ty} NOT NULL)"
            ))
            .unwrap();
            p.execute(&format!(
                "CREATE TABLE nb_{i} (id BIGINT NOT NULL PRIMARY KEY, y {ty} NOT NULL)"
            ))
            .unwrap();
            assert!(
                p.execute(&format!(
                    "CREATE VIEW nv_{i} AS SELECT na_{i}.id AS aid, nb_{i}.id AS bid \
                 FROM na_{i} LEFT JOIN nb_{i} ON na_{i}.x < nb_{i}.y"
                ))
                .is_ok(),
                "pure-range LEFT on a narrow int ({ty}) should register"
            );
        }
    }
    for (i, ty) in types.iter().enumerate() {
        assert!(
            client.resolve_table_or_view_id(&sn, &format!("nv_{i}")).is_ok(),
            "narrow-int pure-range LEFT view ({ty}) should register"
        );
    }
}

/// A pure-range LEFT JOIN on a 16-byte integer range column (`DECIMAL(38,0)` → U128)
/// is rejected: its threshold null-fill has no 16-byte MIN/MAX accumulator. The cap
/// is specific to the pure-range LEFT null-fill — the matching INNER pure-range join
/// over the same 16-byte column still registers (only the LEFT direction reduces).
#[test]
fn test_pure_range_left_join_rejects_wide_int_range_column() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    {
        let mut p = SqlPlanner::new(&mut client, &sn);
        p.execute("CREATE TABLE wr_a (id BIGINT NOT NULL PRIMARY KEY, x DECIMAL(38,0) NOT NULL)")
            .unwrap();
        p.execute("CREATE TABLE wr_b (id BIGINT NOT NULL PRIMARY KEY, y DECIMAL(38,0) NOT NULL)")
            .unwrap();
        let err = p
            .execute(
                "CREATE VIEW wr_left AS SELECT wr_a.id AS aid, wr_b.id AS bid \
             FROM wr_a LEFT JOIN wr_b ON wr_a.x < wr_b.y",
            )
            .unwrap_err();
        match err {
            GnitzSqlError::Unsupported(s) => assert!(s.contains("16-byte accumulator"), "got: {s}"),
            e => panic!("expected Unsupported, got {:?}", e),
        }
        // The matching INNER join over the same 16-byte column still registers — the
        // cap is on the LEFT null-fill's reduce, not on 16-byte range joins as such.
        p.execute(
            "CREATE VIEW wr_inner AS SELECT wr_a.id AS aid, wr_b.id AS bid \
             FROM wr_a JOIN wr_b ON wr_a.x < wr_b.y",
        )
        .unwrap();
    }
    assert!(
        client.resolve_table_or_view_id(&sn, "wr_left").is_err(),
        "no LEFT view should register"
    );
    assert!(
        client.resolve_table_or_view_id(&sn, "wr_inner").is_ok(),
        "INNER pure-range over a 16-byte column still registers"
    );
}

/// A band LEFT JOIN (≥1 equality conjunct + range conjunct) on a 16-byte range
/// column (`DECIMAL(38,0)` → U128) registers: band's null-fill is the cap-free
/// `A − distinct(π_A(inner))` set-difference (no MIN/MAX), so it has NO range-type
/// restriction — the pure-range cap relaxation leaves band entirely untouched.
#[test]
fn test_band_left_join_accepts_wide_range_column() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    let mut p = SqlPlanner::new(&mut client, &sn);
    p.execute("CREATE TABLE bw_a (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, lo DECIMAL(38,0) NOT NULL)")
        .unwrap();
    p.execute("CREATE TABLE bw_b (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, t DECIMAL(38,0) NOT NULL)")
        .unwrap();
    assert!(
        p.execute(
            "CREATE VIEW bw_v AS SELECT bw_a.id AS aid, bw_b.id AS bid \
         FROM bw_a LEFT JOIN bw_b ON bw_a.k = bw_b.k AND bw_a.lo <= bw_b.t"
        )
        .is_ok(),
        "band LEFT on a 16-byte range column should register (band is uncapped)"
    );
    assert!(
        client.resolve_table_or_view_id(&sn, "bw_v").is_ok(),
        "band LEFT-join view over a 16-byte range column should register"
    );
}

/// The pure-range LEFT null-fill, pinned at the circuit level. Built over the
/// SAME tables as an INNER pure-range view, the LEFT view adds the THRESHOLD
/// SUBTRACTION form — NOT the band set-difference, and with NO catalog helpers.
/// Concretely it adds **+2** range `DeltaTraceRange` terms (inner 2, LEFT 4 — the
/// matched `int_a ⋈ trace_m` and `reindex_m ⋈ trace_a`), exactly one `null_extend`,
/// an INLINE shard-free `REDUCE` (the `m = MAX/MIN(b.y)` threshold, emitted
/// already typed as the compare type — no relabel) and its one-row `trace_m` (one
/// extra `IntegrateTrace`), plus one `negate` and one extra `union` (the
/// `A − matched` subtraction). Crucially the sources stay the SAME two
/// (`a`, `b` — no `__m`/`__sent` delta), it keeps exactly **one** `ExchangeShard`
/// (the pair-PK output — the reduce is shard-free) and the inner's two
/// `PartitionFilter`s (`trace_m`/`reindex_m` have none), and adds **zero**
/// `distinct`. No `__pls_left__m`/`__pls_left__sent` catalog objects are created.
#[test]
fn test_pure_range_left_join_circuit_shape() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    {
        let mut p = SqlPlanner::new(&mut client, &sn);
        p.execute("CREATE TABLE pls_a (id BIGINT NOT NULL PRIMARY KEY, x BIGINT NOT NULL)")
            .unwrap();
        p.execute("CREATE TABLE pls_b (id BIGINT NOT NULL PRIMARY KEY, y BIGINT NOT NULL)")
            .unwrap();
        // INNER and LEFT pure-range views over identical tables and ON clause.
        p.execute(
            "CREATE VIEW pls_inner AS SELECT pls_a.id AS aid, pls_b.id AS bid \
                   FROM pls_a JOIN pls_b ON pls_a.x < pls_b.y",
        )
        .unwrap();
        p.execute(
            "CREATE VIEW pls_left AS SELECT pls_a.id AS aid, pls_b.id AS bid \
                   FROM pls_a LEFT JOIN pls_b ON pls_a.x < pls_b.y",
        )
        .unwrap();
    }
    let inner = client.resolve_table_or_view_id(&sn, "pls_inner").unwrap().0;
    let left = client.resolve_table_or_view_id(&sn, "pls_left").unwrap().0;

    let nodes = scan_circuit_nodes(&mut client);
    let nodes = nodes.as_ref();
    let n_inner = |op: u64| opcode_node_count(nodes, inner, op);
    let n_left = |op: u64| opcode_node_count(nodes, left, op);

    // Threshold SUBTRACTION form: +2 range terms (4 total), +1 null_extend, SAME
    // two sources (no __m delta), +1 IntegrateTrace (trace_m).
    assert_eq!(
        n_left(OPCODE_JOIN_DELTA_TRACE_RANGE),
        4,
        "inner 2 + null-fill 2 range terms"
    );
    assert_eq!(
        n_inner(OPCODE_JOIN_DELTA_TRACE_RANGE),
        2,
        "INNER pure-range has 2 range terms"
    );
    assert_eq!(
        n_left(OPCODE_NULL_EXTEND) - n_inner(OPCODE_NULL_EXTEND),
        1,
        "one null_extend"
    );
    assert_eq!(
        n_left(OPCODE_SCAN_DELTA) - n_inner(OPCODE_SCAN_DELTA),
        0,
        "same two sources (a, b) — no helper delta source"
    );
    assert_eq!(
        n_left(OPCODE_INTEGRATE_TRACE) - n_inner(OPCODE_INTEGRATE_TRACE),
        1,
        "trace_m"
    );

    // The INLINE threshold reduce: one shard-free REDUCE (emitting the threshold
    // already typed as the compare type, so no relabel), absent from the INNER view.
    assert_eq!(n_left(OPCODE_REDUCE), 1, "v_left carries the inline MIN/MAX reduce");
    assert_eq!(n_inner(OPCODE_REDUCE), 0, "INNER pure-range has no reduce");

    // The `A − matched` subtraction: one negate + one extra union beyond the inner's
    // single merge union (the matched-union and the (A − matched) union). NOT band's
    // distinct.
    assert_eq!(n_left(OPCODE_DISTINCT), 0, "threshold form has no distinct");
    assert_eq!(n_inner(OPCODE_DISTINCT), 0, "INNER pure-range has no distinct");
    assert_eq!(n_left(OPCODE_NEGATE), 1, "subtraction form: one negate (A − matched)");
    assert_eq!(
        n_left(OPCODE_UNION) - n_inner(OPCODE_UNION),
        3,
        "subtraction adds the matched-union and the (A − matched) union beyond the merge union"
    );

    // Single output exchange (the reduce is SHARD-FREE — run locally on every worker
    // over the broadcast b); trace_m is replicated (no PartitionFilter); the inner's
    // two side PartitionFilters are unchanged.
    assert_eq!(
        n_left(OPCODE_EXCHANGE_SHARD),
        1,
        "exactly one pair-PK output exchange (shard-free reduce)"
    );
    assert_eq!(
        n_inner(OPCODE_EXCHANGE_SHARD),
        1,
        "INNER pure-range has one output exchange"
    );
    assert_eq!(
        n_left(OPCODE_PARTITION_FILTER),
        2,
        "two side PartitionFilters; trace_m has none"
    );

    // No equi join machinery, no Delay.
    assert_eq!(n_left(OPCODE_JOIN_DELTA_TRACE), 0, "no equi delta-trace join");
    assert_eq!(n_left(OPCODE_DELAY), 0, "no Delay node");

    // No per-view catalog helpers are created (the user-surface leak this redesign
    // removes): neither the old `__m` aggregate view nor the `__sent` sentinel table.
    assert!(
        client.resolve_table_or_view_id(&sn, "__pls_left__m").is_err(),
        "no internal __m aggregate view should be registered"
    );
    assert!(
        client.resolve_table_or_view_id(&sn, "__pls_left__sent").is_err(),
        "no internal __sent sentinel table should be registered"
    );
}

/// Two range conjuncts now register: the first is the physical band/range, the
/// second a residual post-join filter (INNER).
#[test]
fn test_range_join_two_range_conjuncts_registers() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    let mut p = SqlPlanner::new(&mut client, &sn);
    p.execute("CREATE TABLE r2_a (id BIGINT NOT NULL PRIMARY KEY, x BIGINT NOT NULL, w BIGINT NOT NULL)")
        .unwrap();
    p.execute("CREATE TABLE r2_b (id BIGINT NOT NULL PRIMARY KEY, y BIGINT NOT NULL, z BIGINT NOT NULL)")
        .unwrap();
    p.execute("CREATE VIEW r2_v AS SELECT * FROM r2_a JOIN r2_b ON r2_a.x < r2_b.y AND r2_a.w > r2_b.z")
        .unwrap();
    assert!(
        client.resolve_table_or_view_id(&sn, "r2_v").is_ok(),
        "first range physical, second range residual — view should register"
    );
}

/// A string/blob range pair is rejected (content hash is not order-preserving),
/// even though the same columns may equijoin.
#[test]
fn test_range_join_reject_string_range_pair() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    let mut p = SqlPlanner::new(&mut client, &sn);
    p.execute("CREATE TABLE rs_a (id BIGINT NOT NULL PRIMARY KEY, s VARCHAR NOT NULL)")
        .unwrap();
    p.execute("CREATE TABLE rs_b (id BIGINT NOT NULL PRIMARY KEY, s VARCHAR NOT NULL)")
        .unwrap();
    let err = p
        .execute("CREATE VIEW rs_v AS SELECT * FROM rs_a JOIN rs_b ON rs_a.s < rs_b.s")
        .unwrap_err();
    match err {
        GnitzSqlError::Unsupported(s) => assert!(s.contains("order-preserving"), "got: {s}"),
        e => panic!("expected Unsupported, got {:?}", e),
    }
}

/// A range self-join registers: the repeated relation is wrapped in a
/// distinct-source pass-through hidden view, so the two join inputs are never
/// the same source (single-source-per-epoch holds).
#[test]
fn test_range_join_self_join_wraps() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    let mut p = SqlPlanner::new(&mut client, &sn);
    p.execute("CREATE TABLE rself (id BIGINT NOT NULL PRIMARY KEY, x BIGINT NOT NULL, y BIGINT NOT NULL)")
        .unwrap();
    p.execute("CREATE VIEW rself_v AS SELECT a.id AS ai, b.id AS bi FROM rself a JOIN rself b ON a.x < b.y")
        .unwrap();
    p.execute("INSERT INTO rself VALUES (1, 1, 10), (2, 5, 2)").unwrap();
    // Pairs where left.x < right.y: (1,1) 1<10, (1,2) 1<2, (2,1) 5<10; not (2,2) 5<2.
    let rows = payload_rows(&mut client, &sn, "rself_v", &["ai", "bi"]);
    assert_eq!(rows, vec![vec![1, 1], vec![1, 2], vec![2, 1]]);
}

/// The output pair-PK count cap: two 2-column-PK tables sum to a 4-column pair-PK
/// (accepted), but adding a third PK column each would exceed it. Here both PKs
/// are 2 columns, so pair-PK = 4 = cap → accepted.
#[test]
fn test_range_join_pair_pk_at_cap() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    let mut p = SqlPlanner::new(&mut client, &sn);
    p.execute("CREATE TABLE rp_a (k1 BIGINT NOT NULL, k2 BIGINT NOT NULL, x BIGINT NOT NULL, PRIMARY KEY (k1, k2))")
        .unwrap();
    p.execute("CREATE TABLE rp_b (k1 BIGINT NOT NULL, k2 BIGINT NOT NULL, y BIGINT NOT NULL, PRIMARY KEY (k1, k2))")
        .unwrap();
    p.execute("CREATE VIEW rp_v AS SELECT rp_a.x, rp_b.y FROM rp_a JOIN rp_b ON rp_a.x < rp_b.y")
        .unwrap();
    assert!(
        client.resolve_table_or_view_id(&sn, "rp_v").is_ok(),
        "4-column pair-PK is at the cap, should register"
    );
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
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    {
        let mut p = SqlPlanner::new(&mut client, &sn);
        p.execute("CREATE TABLE cs_a (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, lo BIGINT NOT NULL)")
            .unwrap();
        p.execute("CREATE TABLE cs_b (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, t BIGINT NOT NULL)")
            .unwrap();
        // Band join (one eq conjunct + one range conjunct).
        p.execute(
            "CREATE VIEW cs_band AS SELECT cs_a.id AS aid, cs_b.id AS bid \
                   FROM cs_a JOIN cs_b ON cs_a.k = cs_b.k AND cs_a.lo <= cs_b.t",
        )
        .unwrap();
        // Pure range join (no eq conjunct).
        p.execute(
            "CREATE VIEW cs_pure AS SELECT cs_a.id AS aid, cs_b.id AS bid \
                   FROM cs_a JOIN cs_b ON cs_a.lo <= cs_b.t",
        )
        .unwrap();
    }
    let band = client.resolve_table_or_view_id(&sn, "cs_band").unwrap().0;
    let pure = client.resolve_table_or_view_id(&sn, "cs_pure").unwrap().0;

    // The nodes table is invariant once both circuits are built — scan it once and
    // count over the single batch.
    let nodes = scan_circuit_nodes(&mut client);
    let nodes = nodes.as_ref();

    assert_eq!(
        opcode_node_count(nodes, band, OPCODE_PARTITION_FILTER),
        0,
        "band join scatters by the eq prefix — no PartitionFilter"
    );
    assert_eq!(
        opcode_node_count(nodes, pure, OPCODE_PARTITION_FILTER),
        2,
        "pure range join broadcasts — one PartitionFilter per side"
    );

    assert_eq!(
        opcode_node_count(nodes, band, OPCODE_JOIN_DELTA_TRACE_RANGE),
        2,
        "band join is a range circuit — two DeltaTraceRange terms"
    );
    assert_eq!(
        opcode_node_count(nodes, pure, OPCODE_JOIN_DELTA_TRACE_RANGE),
        2,
        "pure range join — two DeltaTraceRange terms"
    );
}

// ── Band LEFT-join circuit shape (the null-fill set-difference) ───────────────

/// The band LEFT null-fill, pinned at the circuit level. Built over the SAME
/// tables as an INNER band view, the LEFT view adds exactly the set-difference
/// chain `distinct(π_A(inner))` → `negate` → `union` (A − D) → `null_extend` →
/// re-key → project → `union` (inner ∪ null-fill): `map_reindex ×3`, `map ×2`,
/// `distinct ×1`, `negate ×1`, `union ×2`, `null_extend ×1`. Crucially it adds
/// **zero** ExchangeShard, range/anti/outer Join, IntegrateTrace, Delay, or
/// PartitionFilter — the null-fill is partition-local and reuses the inner output,
/// riding the one existing pair-PK output exchange. The INNER view is unchanged
/// (no distinct / null_extend / negate).
#[test]
fn test_band_left_join_circuit_shape() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    {
        let mut p = SqlPlanner::new(&mut client, &sn);
        p.execute("CREATE TABLE bls_a (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, lo BIGINT NOT NULL)")
            .unwrap();
        p.execute("CREATE TABLE bls_b (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, t BIGINT NOT NULL)")
            .unwrap();
        // INNER and LEFT band views over identical tables and ON clause.
        p.execute(
            "CREATE VIEW bls_inner AS SELECT bls_a.id AS aid, bls_b.id AS bid \
                   FROM bls_a JOIN bls_b ON bls_a.k = bls_b.k AND bls_a.lo <= bls_b.t",
        )
        .unwrap();
        p.execute(
            "CREATE VIEW bls_left AS SELECT bls_a.id AS aid, bls_b.id AS bid \
                   FROM bls_a LEFT JOIN bls_b ON bls_a.k = bls_b.k AND bls_a.lo <= bls_b.t",
        )
        .unwrap();
    }
    let inner = client.resolve_table_or_view_id(&sn, "bls_inner").unwrap().0;
    let left = client.resolve_table_or_view_id(&sn, "bls_left").unwrap().0;

    let nodes = scan_circuit_nodes(&mut client);
    let nodes = nodes.as_ref();
    let n_inner = |op: u64| opcode_node_count(nodes, inner, op);
    let n_left = |op: u64| opcode_node_count(nodes, left, op);

    // The null-fill adds exactly this set-difference chain over the inner skeleton.
    assert_eq!(
        n_left(OPCODE_MAP_EXPR) - n_inner(OPCODE_MAP_EXPR),
        3,
        "rekey_a + a_all + nf_rekey"
    );
    assert_eq!(
        n_left(OPCODE_MAP_PROJ) - n_inner(OPCODE_MAP_PROJ),
        2,
        "proj_a + nf_proj"
    );
    assert_eq!(
        n_left(OPCODE_POSITIVE_PART) - n_inner(OPCODE_POSITIVE_PART),
        1,
        "one positive_part(A − π_A(inner))"
    );
    assert_eq!(
        n_left(OPCODE_NEGATE) - n_inner(OPCODE_NEGATE),
        1,
        "negate(π_A(inner)) for A − π_A(inner)"
    );
    assert_eq!(
        n_left(OPCODE_UNION) - n_inner(OPCODE_UNION),
        2,
        "A − π_A(inner) diff + inner ∪ null-fill"
    );
    assert_eq!(
        n_left(OPCODE_NULL_EXTEND) - n_inner(OPCODE_NULL_EXTEND),
        1,
        "null_extend(diff)"
    );

    // No extra exchange, no extra join machinery, no extra trace/delay/partition —
    // the null-fill is partition-local and reuses the inner range output.
    assert_eq!(
        n_left(OPCODE_EXCHANGE_SHARD),
        n_inner(OPCODE_EXCHANGE_SHARD),
        "no added ExchangeShard"
    );
    assert_eq!(
        n_left(OPCODE_JOIN_DELTA_TRACE_RANGE),
        n_inner(OPCODE_JOIN_DELTA_TRACE_RANGE),
        "no added range Join"
    );
    assert_eq!(
        n_left(OPCODE_INTEGRATE_TRACE),
        n_inner(OPCODE_INTEGRATE_TRACE),
        "no added IntegrateTrace"
    );
    assert_eq!(
        n_left(OPCODE_PARTITION_FILTER),
        0,
        "band LEFT carries no PartitionFilter"
    );
    assert_eq!(n_left(OPCODE_DELAY), 0, "no Delay node");
    assert_eq!(n_left(OPCODE_JOIN_DELTA_TRACE), 0, "no equi delta-trace join");

    // Exactly one positive_part, no distinct, and the INNER view is shape-unchanged.
    assert_eq!(
        n_left(OPCODE_POSITIVE_PART),
        1,
        "exactly one positive_part in the band LEFT circuit"
    );
    assert_eq!(n_left(OPCODE_DISTINCT), 0, "weight-exact band LEFT uses no distinct");
    assert_eq!(n_inner(OPCODE_POSITIVE_PART), 0, "INNER band view has no positive_part");
    assert_eq!(n_inner(OPCODE_NULL_EXTEND), 0, "INNER band view has no null_extend");
    assert_eq!(n_inner(OPCODE_NEGATE), 0, "INNER band view has no negate");
}

// ── Residual ON predicates (INNER) ───────────────────────────────────────────
//
// A non-equi/non-range conjunct (or a second range) is collected as a residual
// and spliced as ONE linear Filter over the join output. Every key column below
// is NOT NULL, so the residual Filter is the only Filter node — `filter_node_count
// == 1` both proves the splice happened and that no stray null-key filter inflates
// it. Data correctness lives in the Python E2E suite (`test_join_residual.py`).

/// Equijoin + inequality residual: registers, the projected user columns follow
/// the `_join_pk`, and exactly one Filter (the residual) is spliced.
#[test]
fn test_residual_equi_plus_inequality() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    exec(
        &mut client,
        &sn,
        "CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, t BIGINT NOT NULL)",
    );
    exec(
        &mut client,
        &sn,
        "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, t BIGINT NOT NULL)",
    );
    exec(
        &mut client,
        &sn,
        "CREATE VIEW v AS SELECT a.id AS aid, b.id AS bid FROM a JOIN b ON a.k = b.k AND a.t <> b.t",
    );
    let (vid, s) = client.resolve_table_or_view_id(&sn, "v").unwrap();
    assert_eq!(s.pk_indices(), &[0], "single equi key → one _join_pk PK column");
    assert!(
        s.columns.iter().any(|c| c.name == "aid") && s.columns.iter().any(|c| c.name == "bid"),
        "projected user columns present: {:?}",
        s.columns.iter().map(|c| &c.name).collect::<Vec<_>>()
    );
    assert_eq!(filter_node_count(&mut client, vid), 1, "exactly the residual Filter");
}

/// Control: the same join WITHOUT a residual has no Filter node — confirms the
/// count above is the residual, not an incidental filter.
#[test]
fn test_residual_absent_no_filter() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    exec(
        &mut client,
        &sn,
        "CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, t BIGINT NOT NULL)",
    );
    exec(
        &mut client,
        &sn,
        "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, t BIGINT NOT NULL)",
    );
    exec(
        &mut client,
        &sn,
        "CREATE VIEW v AS SELECT a.id AS aid, b.id AS bid FROM a JOIN b ON a.k = b.k",
    );
    let (vid, _) = client.resolve_table_or_view_id(&sn, "v").unwrap();
    assert_eq!(filter_node_count(&mut client, vid), 0, "no residual ⇒ no Filter");
}

/// Equijoin + column-vs-literal residual (`a.r = 5`).
#[test]
fn test_residual_equi_plus_literal() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    exec(
        &mut client,
        &sn,
        "CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, r BIGINT NOT NULL)",
    );
    exec(
        &mut client,
        &sn,
        "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL)",
    );
    exec(
        &mut client,
        &sn,
        "CREATE VIEW v AS SELECT * FROM a JOIN b ON a.k = b.k AND a.r = 5",
    );
    let (vid, _) = client.resolve_table_or_view_id(&sn, "v").unwrap();
    assert_eq!(filter_node_count(&mut client, vid), 1, "residual Filter spliced");
}

/// Equijoin + residual referencing an arithmetic expression (`a.lo + 1 < b.hi`).
#[test]
fn test_residual_arithmetic_expr() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    exec(
        &mut client,
        &sn,
        "CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, lo BIGINT NOT NULL)",
    );
    exec(
        &mut client,
        &sn,
        "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, hi BIGINT NOT NULL)",
    );
    exec(
        &mut client,
        &sn,
        "CREATE VIEW v AS SELECT * FROM a JOIN b ON a.k = b.k AND a.lo + 1 < b.hi",
    );
    assert!(
        client.resolve_table_or_view_id(&sn, "v").is_ok(),
        "arithmetic residual should register"
    );
}

/// Equijoin + OR-group residual (`(a.x > 5 OR b.y < 3)`): the catch-all collects
/// the whole OR as one residual conjunct.
#[test]
fn test_residual_or_group() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    exec(
        &mut client,
        &sn,
        "CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, x BIGINT NOT NULL)",
    );
    exec(
        &mut client,
        &sn,
        "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, y BIGINT NOT NULL)",
    );
    exec(
        &mut client,
        &sn,
        "CREATE VIEW v AS SELECT * FROM a JOIN b ON a.k = b.k AND (a.x > 5 OR b.y < 3)",
    );
    let (vid, _) = client.resolve_table_or_view_id(&sn, "v").unwrap();
    assert_eq!(
        filter_node_count(&mut client, vid),
        1,
        "OR-group residual Filter spliced"
    );
}

/// Equijoin + BETWEEN residual (`a.v BETWEEN 1 AND 10`): desugars to `>= AND <=`.
#[test]
fn test_residual_between() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    exec(
        &mut client,
        &sn,
        "CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, v BIGINT NOT NULL)",
    );
    exec(
        &mut client,
        &sn,
        "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL)",
    );
    exec(
        &mut client,
        &sn,
        "CREATE VIEW v AS SELECT * FROM a JOIN b ON a.k = b.k AND a.v BETWEEN 1 AND 10",
    );
    assert!(
        client.resolve_table_or_view_id(&sn, "v").is_ok(),
        "BETWEEN residual should register"
    );
}

/// Band (eq + range) + a second-range residual (`a.lo < b.hi AND a.x > b.y`).
#[test]
fn test_residual_band_plus_second_range() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    exec(
        &mut client,
        &sn,
        "CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, lo BIGINT NOT NULL, x BIGINT NOT NULL)",
    );
    exec(
        &mut client,
        &sn,
        "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, hi BIGINT NOT NULL, y BIGINT NOT NULL)",
    );
    exec(
        &mut client,
        &sn,
        "CREATE VIEW v AS SELECT a.id AS aid, b.id AS bid FROM a JOIN b ON a.k = b.k AND a.lo < b.hi AND a.x > b.y",
    );
    let (vid, _) = client.resolve_table_or_view_id(&sn, "v").unwrap();
    assert_eq!(
        filter_node_count(&mut client, vid),
        1,
        "band path: residual Filter spliced"
    );
}

/// Pure range + residual (`a.x < b.y AND a.r <> b.s`).
#[test]
fn test_residual_pure_range() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    exec(
        &mut client,
        &sn,
        "CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, x BIGINT NOT NULL, r BIGINT NOT NULL)",
    );
    exec(
        &mut client,
        &sn,
        "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, y BIGINT NOT NULL, s BIGINT NOT NULL)",
    );
    exec(
        &mut client,
        &sn,
        "CREATE VIEW v AS SELECT a.id AS aid, b.id AS bid FROM a JOIN b ON a.x < b.y AND a.r <> b.s",
    );
    let (vid, _) = client.resolve_table_or_view_id(&sn, "v").unwrap();
    assert_eq!(
        filter_node_count(&mut client, vid),
        1,
        "pure-range path: residual Filter spliced"
    );
}

/// String residual (`a.s1 <> b.s2`, both VARCHAR): content comparison.
#[test]
fn test_residual_string_inequality() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    exec(
        &mut client,
        &sn,
        "CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, s1 VARCHAR NOT NULL)",
    );
    exec(
        &mut client,
        &sn,
        "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, s2 VARCHAR NOT NULL)",
    );
    exec(
        &mut client,
        &sn,
        "CREATE VIEW v AS SELECT * FROM a JOIN b ON a.k = b.k AND a.s1 <> b.s2",
    );
    assert!(
        client.resolve_table_or_view_id(&sn, "v").is_ok(),
        "string residual should register"
    );
}

// A BLOB-column residual (`a.b1 <> b.b2`) exercises the same §6.6a path, but BLOB
// columns are not SQL-DDL-creatable (`sql_type_to_typecode` has no BLOB keyword —
// blobs are created only via the wire `ColumnDef` API), so it cannot be expressed
// as a CREATE VIEW here. The blob content-opcode lowering is proven directly by
// `expr.rs`'s `blob_cmp_matches_string_cmp` unit test (byte-identical to STRING).

/// LEFT/OUTER + residual is rejected (§6.3): the residual would have to
/// participate in the outer null-fill.
#[test]
fn test_residual_left_join_rejected() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    exec(
        &mut client,
        &sn,
        "CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, t BIGINT NOT NULL)",
    );
    exec(
        &mut client,
        &sn,
        "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, t BIGINT NOT NULL)",
    );
    let err = try_exec(
        &mut client,
        &sn,
        "CREATE VIEW v AS SELECT * FROM a LEFT JOIN b ON a.k = b.k AND a.t <> b.t",
    )
    .unwrap_err();
    assert!(
        matches!(err, GnitzSqlError::Unsupported(_)),
        "expected Unsupported, got {:?}",
        err
    );
    assert!(
        client.resolve_table_or_view_id(&sn, "v").is_err(),
        "no view should be registered"
    );
}

/// A residual-only ON (no equi/range anchor) is rejected — the anchor guard fires
/// (a residual cannot stand alone).
#[test]
fn test_residual_only_on_rejected() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    exec(
        &mut client,
        &sn,
        "CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, t BIGINT NOT NULL)",
    );
    exec(
        &mut client,
        &sn,
        "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, t BIGINT NOT NULL)",
    );
    let err = try_exec(
        &mut client,
        &sn,
        "CREATE VIEW v AS SELECT * FROM a JOIN b ON a.t <> b.t",
    )
    .unwrap_err();
    assert!(
        matches!(err, GnitzSqlError::Bind(_)),
        "expected Bind (no anchor), got {:?}",
        err
    );
    assert!(
        client.resolve_table_or_view_id(&sn, "v").is_err(),
        "no view should be registered"
    );
}

/// An unsupported residual construct (`LIKE`) is rejected at CREATE when the
/// residual binder reaches an expression type the core does not handle.
#[test]
fn test_residual_like_rejected() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    exec(
        &mut client,
        &sn,
        "CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, t VARCHAR NOT NULL)",
    );
    exec(
        &mut client,
        &sn,
        "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, t VARCHAR NOT NULL)",
    );
    let err = try_exec(
        &mut client,
        &sn,
        "CREATE VIEW v AS SELECT * FROM a JOIN b ON a.k = b.k AND a.t LIKE b.t",
    )
    .unwrap_err();
    assert!(
        matches!(err, GnitzSqlError::Unsupported(_)),
        "expected Unsupported, got {:?}",
        err
    );
    assert!(
        client.resolve_table_or_view_id(&sn, "v").is_err(),
        "no view should be registered"
    );
}

/// A mixed string↔int residual (`a.s <> b.n`, VARCHAR vs BIGINT) is rejected by
/// the hardened compiler (§6.6b) — not silently miscompiled into an integer load.
#[test]
fn test_residual_mixed_string_int_rejected() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    exec(
        &mut client,
        &sn,
        "CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, s VARCHAR NOT NULL)",
    );
    exec(
        &mut client,
        &sn,
        "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, n BIGINT NOT NULL)",
    );
    let err = try_exec(
        &mut client,
        &sn,
        "CREATE VIEW v AS SELECT * FROM a JOIN b ON a.k = b.k AND a.s <> b.n",
    )
    .unwrap_err();
    assert!(
        matches!(err, GnitzSqlError::Unsupported(_)),
        "expected Unsupported, got {:?}",
        err
    );
    assert!(
        client.resolve_table_or_view_id(&sn, "v").is_err(),
        "no view should be registered"
    );
}

// ── Case-insensitive qualified alias (A1) ────────────────────────────────────

/// The join `AliasMap` is keyed by lowercased aliases, but the case-preserving
/// dialect hands the ON clause the raw spelling. An uppercase `A` referencing
/// alias `a` must still resolve (SQL identifiers are case-insensitive). Bug path:
/// `ON A.x` routes through resolve_join_col_ref -> resolve_qualified_column("A", ...).
#[test]
fn test_join_uppercase_alias_in_on_clause() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    let mut p = SqlPlanner::new(&mut client, &sn);

    p.execute("CREATE TABLE t (x BIGINT NOT NULL PRIMARY KEY, p BIGINT NOT NULL)")
        .unwrap();
    p.execute("CREATE TABLE u (y BIGINT NOT NULL PRIMARY KEY, q BIGINT NOT NULL)")
        .unwrap();

    let r = p.execute("CREATE VIEW v AS SELECT a.p AS ap, b.q AS bq FROM t a JOIN u b ON A.x = b.y");
    assert!(r.is_ok(), "uppercase alias in ON clause failed: {:?}", r.err());
}

// ── Band LEFT JOIN over a non-base-table preserved side (A2) ─────────────────

/// A band (equi + range) LEFT JOIN builds its null-fill as the weight-exact
/// `positive_part(A − π_A(inner))`, which subtracts the RAW matched multiplicity
/// and clamps the result — correct even for a bag-valued (non-unique-PK) preserved
/// side. So a view left side (a UNION ALL that drops the PK, collapsing duplicate
/// projected content to one `_set_pk` identity of weight >= 2) now REGISTERS, no
/// base-table guard. (The data-level bag correctness is asserted by the
/// `test_band_left_*` e2e suite.)
#[test]
fn test_band_left_join_accepts_view_preserved_side() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    let mut p = SqlPlanner::new(&mut client, &sn);

    p.execute("CREATE TABLE t1 (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, lo BIGINT NOT NULL)")
        .unwrap();
    p.execute("CREATE TABLE t2 (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, lo BIGINT NOT NULL)")
        .unwrap();
    p.execute("CREATE TABLE inr (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, t BIGINT NOT NULL)")
        .unwrap();
    p.execute("CREATE TABLE lt (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, lo BIGINT NOT NULL)")
        .unwrap();
    // Genuinely bag-valued: the projection DROPS the PK (`id`), so two source rows
    // sharing (k, lo) collapse to one _set_pk identity of weight 2. uv: [_set_pk, k, lo].
    p.execute("CREATE VIEW uv AS SELECT k, lo FROM t1 UNION ALL SELECT k, lo FROM t2")
        .unwrap();

    // Band LEFT over a base table on the left compiles (control).
    let ok = p.execute(
        "CREATE VIEW good AS SELECT lt.id AS a, inr.id AS b \
         FROM lt LEFT JOIN inr ON lt.k = inr.k AND lt.lo <= inr.t",
    );
    assert!(ok.is_ok(), "band LEFT over a base table must compile: {:?}", ok.err());

    // Band LEFT (k equi + lo<=t range) over the bag-valued view now REGISTERS —
    // `positive_part` is weight-exact, so no base-table guard rejects it.
    let bag = p.execute(
        "CREATE VIEW bag AS SELECT uv.k AS a, inr.id AS b \
         FROM uv LEFT JOIN inr ON uv.k = inr.k AND uv.lo <= inr.t",
    );
    assert!(
        bag.is_ok(),
        "band LEFT over a bag-valued view must now compile (positive_part is weight-exact): {:?}",
        bag.err()
    );
    // `p`'s &mut client borrow ends above (no further `p` use), so the direct probe is ok.
    assert!(
        client.resolve_table_or_view_id(&sn, "bag").is_ok(),
        "accepted bag-valued band LEFT view must register"
    );
}

// ── RIGHT / FULL OUTER JOIN ───────────────────────────────────────────────────
//
// RIGHT/FULL fall out of LEFT by symmetry: no new engine operator, opcode, or
// inner-join term — only the mirror null-fill `ν_B = positive_part(B − π_B(inner))`
// and FULL = `ν_A` + `ν_B`. The circuit-shape tests pin that RIGHT adds exactly one
// `positive_part` + one `null_extend` over the inner skeleton (FULL: two each), with
// the SAME two inner join terms (no extra join, trace, or exchange). Data correctness
// lives in the Python E2E suite (`test_right_full_join.py`).

/// Equi RIGHT and FULL register and project user columns after the `_join_pk`.
#[test]
fn test_equi_right_full_join_accepts() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    let mut p = SqlPlanner::new(&mut client, &sn);
    p.execute("CREATE TABLE er_a (id BIGINT NOT NULL PRIMARY KEY, fk BIGINT NOT NULL, av BIGINT NOT NULL)")
        .unwrap();
    p.execute("CREATE TABLE er_b (id BIGINT NOT NULL PRIMARY KEY, bv BIGINT NOT NULL)")
        .unwrap();
    assert!(
        p.execute(
            "CREATE VIEW er_r AS SELECT er_a.av AS av, er_b.bv AS bv FROM er_a RIGHT JOIN er_b ON er_a.fk = er_b.id"
        )
        .is_ok(),
        "equi RIGHT JOIN should register"
    );
    assert!(
        p.execute(
            "CREATE VIEW er_f AS SELECT er_a.av AS av, er_b.bv AS bv FROM er_a FULL JOIN er_b ON er_a.fk = er_b.id"
        )
        .is_ok(),
        "equi FULL JOIN should register"
    );
}

/// Equi RIGHT/FULL null-fill, pinned at the circuit level against an INNER view over
/// identical tables. RIGHT adds exactly one `positive_part` (`ν_B`) and one
/// `null_extend`; FULL adds two of each (`ν_A` + `ν_B`). Both keep the inner's TWO
/// `JOIN_DELTA_TRACE` terms (no extra join) and add no exchange.
#[test]
fn test_equi_right_full_circuit_shape() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    {
        let mut p = SqlPlanner::new(&mut client, &sn);
        p.execute("CREATE TABLE ers_a (id BIGINT NOT NULL PRIMARY KEY, fk BIGINT NOT NULL)")
            .unwrap();
        p.execute("CREATE TABLE ers_b (id BIGINT NOT NULL PRIMARY KEY, bv BIGINT NOT NULL)")
            .unwrap();
        for (name, jt) in [("ers_i", "JOIN"), ("ers_r", "RIGHT JOIN"), ("ers_f", "FULL JOIN")] {
            p.execute(&format!(
                "CREATE VIEW {name} AS SELECT ers_b.bv AS bv \
                 FROM ers_a {jt} ers_b ON ers_a.fk = ers_b.id"
            ))
            .unwrap();
        }
    }
    let inner = client.resolve_table_or_view_id(&sn, "ers_i").unwrap().0;
    let right = client.resolve_table_or_view_id(&sn, "ers_r").unwrap().0;
    let full = client.resolve_table_or_view_id(&sn, "ers_f").unwrap().0;

    let nodes = scan_circuit_nodes(&mut client);
    let nodes = nodes.as_ref();
    let n = |vid: u64, op: u64| opcode_node_count(nodes, vid, op);

    // The two inner join terms (ΔA⋈I(B), ΔB⋈I(A)) are present and UNCHANGED — RIGHT/FULL
    // reuse the inner output, adding no join.
    for vid in [inner, right, full] {
        assert_eq!(
            n(vid, OPCODE_JOIN_DELTA_TRACE),
            2,
            "exactly the 2 inner equi join terms"
        );
        assert_eq!(n(vid, OPCODE_EXCHANGE_SHARD), 0, "equi join has no output exchange");
        assert_eq!(n(vid, OPCODE_DISTINCT), 0, "weight-exact null-fill uses no distinct");
    }

    // INNER: no null-fill. RIGHT: ν_B (1 positive_part + 1 null_extend). FULL: ν_A + ν_B.
    assert_eq!(n(inner, OPCODE_POSITIVE_PART), 0, "INNER has no positive_part");
    assert_eq!(n(inner, OPCODE_NULL_EXTEND), 0, "INNER has no null_extend");
    assert_eq!(n(right, OPCODE_POSITIVE_PART), 1, "RIGHT emits one ν_B positive_part");
    assert_eq!(
        n(right, OPCODE_NULL_EXTEND),
        1,
        "RIGHT null-extends the left columns once"
    );
    assert_eq!(n(full, OPCODE_POSITIVE_PART), 2, "FULL emits ν_A and ν_B");
    assert_eq!(n(full, OPCODE_NULL_EXTEND), 2, "FULL null-extends both sides");
}

/// A band (`n_eq ≥ 1`) RIGHT and FULL register cleanly — partition-local on the
/// eq-prefix scatter, riding the inner pairs' pair-PK output exchange.
#[test]
fn test_band_right_full_join_accepts() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    let mut p = SqlPlanner::new(&mut client, &sn);
    p.execute("CREATE TABLE br_a (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, lo BIGINT NOT NULL)")
        .unwrap();
    p.execute("CREATE TABLE br_b (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, t BIGINT NOT NULL)")
        .unwrap();
    assert!(
        p.execute(
            "CREATE VIEW br_r AS SELECT br_a.id AS aid, br_b.id AS bid \
             FROM br_a RIGHT JOIN br_b ON br_a.k = br_b.k AND br_a.lo <= br_b.t"
        )
        .is_ok(),
        "band RIGHT JOIN should register"
    );
    assert!(
        p.execute(
            "CREATE VIEW br_f AS SELECT br_a.id AS aid, br_b.id AS bid \
             FROM br_a FULL JOIN br_b ON br_a.k = br_b.k AND br_a.lo <= br_b.t"
        )
        .is_ok(),
        "band FULL JOIN should register"
    );
}

/// Band RIGHT/FULL null-fill, pinned at the circuit level. Same shape as the equi
/// case but over `JOIN_DELTA_TRACE_RANGE` terms, and riding the inner pairs' SINGLE
/// pair-PK `ExchangeShard` (no extra exchange added by the null-fill).
#[test]
fn test_band_right_full_circuit_shape() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    {
        let mut p = SqlPlanner::new(&mut client, &sn);
        p.execute("CREATE TABLE brs_a (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, lo BIGINT NOT NULL)")
            .unwrap();
        p.execute("CREATE TABLE brs_b (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, t BIGINT NOT NULL)")
            .unwrap();
        for (name, jt) in [("brs_i", "JOIN"), ("brs_r", "RIGHT JOIN"), ("brs_f", "FULL JOIN")] {
            p.execute(&format!(
                "CREATE VIEW {name} AS SELECT brs_a.id AS aid, brs_b.id AS bid \
                 FROM brs_a {jt} brs_b ON brs_a.k = brs_b.k AND brs_a.lo <= brs_b.t"
            ))
            .unwrap();
        }
    }
    let inner = client.resolve_table_or_view_id(&sn, "brs_i").unwrap().0;
    let right = client.resolve_table_or_view_id(&sn, "brs_r").unwrap().0;
    let full = client.resolve_table_or_view_id(&sn, "brs_f").unwrap().0;

    let nodes = scan_circuit_nodes(&mut client);
    let nodes = nodes.as_ref();
    let n = |vid: u64, op: u64| opcode_node_count(nodes, vid, op);

    // The two inner range terms are present and unchanged; exactly one pair-PK exchange
    // is shared by inner pairs and null-fills.
    for vid in [inner, right, full] {
        assert_eq!(
            n(vid, OPCODE_JOIN_DELTA_TRACE_RANGE),
            2,
            "exactly the 2 inner range terms"
        );
        assert_eq!(n(vid, OPCODE_JOIN_DELTA_TRACE), 0, "no equi delta-trace join");
        assert_eq!(
            n(vid, OPCODE_EXCHANGE_SHARD),
            1,
            "the single shared pair-PK output exchange"
        );
        assert_eq!(n(vid, OPCODE_DISTINCT), 0, "weight-exact null-fill uses no distinct");
        assert_eq!(n(vid, OPCODE_PARTITION_FILTER), 0, "band carries no PartitionFilter");
    }

    assert_eq!(n(inner, OPCODE_POSITIVE_PART), 0, "INNER band has no positive_part");
    assert_eq!(n(inner, OPCODE_NULL_EXTEND), 0, "INNER band has no null_extend");
    assert_eq!(
        n(right, OPCODE_POSITIVE_PART),
        1,
        "band RIGHT emits one ν_B positive_part"
    );
    assert_eq!(
        n(right, OPCODE_NULL_EXTEND),
        1,
        "band RIGHT null-extends the left columns once"
    );
    assert_eq!(n(full, OPCODE_POSITIVE_PART), 2, "band FULL emits ν_A and ν_B");
    assert_eq!(n(full, OPCODE_NULL_EXTEND), 2, "band FULL null-extends both sides");
}

/// Pure-range (`n_eq == 0`, a sole inequality with no equi conjunct) RIGHT and FULL
/// are rejected with a clean `Unsupported` — their mirror null-fill has no `π(inner)`
/// witness. LEFT pure-range stays supported.
#[test]
fn test_pure_range_right_full_rejected() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    {
        let mut p = SqlPlanner::new(&mut client, &sn);
        p.execute("CREATE TABLE prr_a (id BIGINT NOT NULL PRIMARY KEY, x BIGINT NOT NULL)")
            .unwrap();
        p.execute("CREATE TABLE prr_b (id BIGINT NOT NULL PRIMARY KEY, y BIGINT NOT NULL)")
            .unwrap();
        for (name, jt) in [("prr_r", "RIGHT"), ("prr_f", "FULL")] {
            let err = p
                .execute(&format!(
                    "CREATE VIEW {name} AS SELECT prr_a.id AS aid, prr_b.id AS bid \
                     FROM prr_a {jt} JOIN prr_b ON prr_a.x < prr_b.y"
                ))
                .unwrap_err();
            match err {
                GnitzSqlError::Unsupported(s) => {
                    assert!(s.contains("pure-range RIGHT/FULL"), "got: {s}")
                }
                e => panic!("expected Unsupported, got {e:?}"),
            }
        }
        // LEFT pure-range over the same tables still registers.
        assert!(
            p.execute(
                "CREATE VIEW prr_l AS SELECT prr_a.id AS aid, prr_b.id AS bid \
                 FROM prr_a LEFT JOIN prr_b ON prr_a.x < prr_b.y"
            )
            .is_ok(),
            "pure-range LEFT is unaffected"
        );
    }
    for name in ["prr_r", "prr_f"] {
        assert!(
            client.resolve_table_or_view_id(&sn, name).is_err(),
            "no pure-range {name} view should register"
        );
    }
    assert!(
        client.resolve_table_or_view_id(&sn, "prr_l").is_ok(),
        "pure-range LEFT view registered"
    );
}

/// A 16-byte pure-range FULL gets the pure-range-unsupported message, NOT the LEFT
/// ≤8-byte-range-column message: `Full` satisfies both `preserves_right()` (n_eq == 0)
/// and `preserves_left()` (n_eq == 0), and the RIGHT/FULL rejection MUST run first.
#[test]
fn test_pure_range_full_wide_gets_purerange_message() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    let mut p = SqlPlanner::new(&mut client, &sn);
    p.execute("CREATE TABLE prw_a (id BIGINT NOT NULL PRIMARY KEY, x DECIMAL(38,0) NOT NULL)")
        .unwrap();
    p.execute("CREATE TABLE prw_b (id BIGINT NOT NULL PRIMARY KEY, y DECIMAL(38,0) NOT NULL)")
        .unwrap();
    let err = p
        .execute(
            "CREATE VIEW prw_f AS SELECT prw_a.id AS aid, prw_b.id AS bid \
             FROM prw_a FULL JOIN prw_b ON prw_a.x < prw_b.y",
        )
        .unwrap_err();
    match err {
        GnitzSqlError::Unsupported(s) => {
            assert!(
                s.contains("pure-range RIGHT/FULL"),
                "wrong message (ordering bug?): {s}"
            );
            assert!(
                !s.contains("16-byte accumulator"),
                "must not be the LEFT 8-byte-gate message: {s}"
            );
        }
        e => panic!("expected Unsupported, got {e:?}"),
    }
}
