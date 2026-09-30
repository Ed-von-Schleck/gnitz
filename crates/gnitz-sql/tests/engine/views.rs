//! Maintained-view data contracts that only a running engine can show, each
//! read back with its Z-set weights across four workers.

use super::*;
use gnitz_wire::WireStatus::Error;

// ── Set operations ───────────────────────────────────────────────────────────

/// EXCEPT and INTERSECT match on the whole row, not on the source PK; and each
/// side is a set: two source rows projecting to one value weigh 1, and a second
/// covering row on the other side changes nothing.
#[test]
fn set_ops_use_full_row_identity_over_distinct_sides() {
    let mut db = Db::boot(4);
    db.exec(
        "CREATE TABLE a (id BIGINT PRIMARY KEY, c BIGINT NOT NULL);
         CREATE TABLE b (id BIGINT PRIMARY KEY, c BIGINT NOT NULL);
         CREATE VIEW ve AS SELECT * FROM a EXCEPT SELECT * FROM b;
         CREATE VIEW vi AS SELECT * FROM a INTERSECT SELECT * FROM b;
         CREATE VIEW pe AS SELECT c FROM a EXCEPT SELECT c FROM b;
         CREATE VIEW pi AS SELECT c FROM a INTERSECT SELECT c FROM b;
         INSERT INTO a VALUES (1, 100), (2, 200), (3, 5), (4, 5)",
    );
    let check = |db: &mut Db, ve: &[[i64; 3]], vi: &[[i64; 3]], pe: &[[i64; 2]], pi: &[[i64; 2]]| {
        assert_eq!(db.scan("ve", &["id", "c"]), ve);
        assert_eq!(db.scan("vi", &["id", "c"]), vi);
        assert_eq!(db.scan("pe", &["c"]), pe);
        assert_eq!(db.scan("pi", &["c"]), pi);
    };
    let a = [[1, 100, 1], [2, 200, 1], [3, 5, 1], [4, 5, 1]];
    check(&mut db, &a, &[], &[[5, 1], [100, 1], [200, 1]], &[]);

    db.exec("INSERT INTO b VALUES (1, 200), (2, 200), (3, 5)");
    check(
        &mut db,
        &[[1, 100, 1], [4, 5, 1]],
        &[[2, 200, 1], [3, 5, 1]],
        &[[100, 1]],
        &[[5, 1], [200, 1]],
    );

    db.exec("INSERT INTO b VALUES (4, 5); DELETE FROM b WHERE id = 2");
    check(
        &mut db,
        &[[1, 100, 1], [2, 200, 1]],
        &[[3, 5, 1], [4, 5, 1]],
        &[[100, 1]],
        &[[5, 1], [200, 1]],
    );
}

// ── Aggregates ───────────────────────────────────────────────────────────────

/// A group whose every value is NULL still exists: SUM/MIN/MAX/AVG read as NULL
/// while COUNT(x) is 0 and COUNT(*) counts the rows — from inception and after a
/// delete empties the group of values. AVG ignores NULLs.
#[test]
fn aggregate_values_and_nulls() {
    let mut db = Db::boot(4);
    db.exec(
        "CREATE TABLE t (id BIGINT PRIMARY KEY, g BIGINT NOT NULL, x BIGINT, y BIGINT NOT NULL, fx DOUBLE NOT NULL);
         CREATE VIEW vx AS SELECT g, SUM(x) AS sx, MIN(x) AS mnx, MAX(x) AS mxx, AVG(x) AS ax, \
         COUNT(x) AS cx, COUNT(*) AS ca, SUM(fx) AS sfx, AVG(fx) AS afx FROM t GROUP BY g;
         CREATE VIEW vg AS SELECT SUM(y) AS sy, MIN(y) AS mny, MAX(y) AS mxy, COUNT(*) AS ca FROM t;
         INSERT INTO t (id, g, x, y, fx) VALUES \
         (1, 5, NULL, 1, 1.5), (2, 5, NULL, 2, 2.5), \
         (3, 6, NULL, 3, 3.0), (4, 6, 3, 4, 5.0), \
         (5, 7, 4, 10, 4.0)",
    );
    let vx = ["g", "sx", "mnx", "mxx", "ax", "cx", "ca", "sfx", "afx"];
    let vg = ["sy", "mny", "mxy", "ca"];
    let all_null_5 = [5, NULL, NULL, NULL, NULL, 0, 2, 4, 2, 1];
    let g7 = [7, 4, 4, 4, 4, 1, 1, 4, 4, 1];
    assert_eq!(db.scan("vx", &vx), [all_null_5, [6, 3, 3, 3, 3, 1, 2, 8, 4, 1], g7]);
    assert_eq!(db.scan("vg", &vg), [[20, 1, 10, 5, 1]]);

    // Group 6 loses its only non-NULL x but keeps a row.
    db.exec("DELETE FROM t WHERE id = 4");
    assert_eq!(
        db.scan("vx", &vx),
        [all_null_5, [6, NULL, NULL, NULL, NULL, 0, 1, 3, 3, 1], g7]
    );
    assert_eq!(db.scan("vg", &vg), [[16, 1, 10, 4, 1]]);
}

// ── Compound-PK sources ──────────────────────────────────────────────────────

/// A projection over a three-column PK keeps rows that differ only in the
/// trailing key column apart, through both the pure-map arm (a column subset)
/// and the expression arm (duplicate-PK copies and a computed column), across
/// INSERT, UPDATE and DELETE.
#[test]
fn compound_pk_projection_incremental() {
    let mut db = Db::boot(4);
    db.exec(
        "CREATE TABLE t (a BIGINT, b BIGINT, c BIGINT, val BIGINT NOT NULL, extra BIGINT NOT NULL, \
         PRIMARY KEY (a, b, c));
         CREATE VIEW v_map AS SELECT a, b, c, val FROM t;
         CREATE VIEW v_expr AS SELECT a, b, c, val, a AS a2, c AS c2, val + 1 AS vp FROM t;
         INSERT INTO t (a, b, c, val, extra) VALUES (1, 1, 1, 100, 9), (1, 1, 2, 200, 8), (1, 2, 1, 300, 7)",
    );
    let check = |db: &mut Db, rows: &[[i64; 4]]| {
        let map: Vec<Vec<i64>> = rows.iter().map(|r| r.to_vec()).collect();
        let expr: Vec<Vec<i64>> = rows.iter().map(|&[a, b, c, v]| vec![a, b, c, v, a, c, v + 1]).collect();
        assert_eq!(db.scan("v_map", &["a", "b", "c", "val"]), at_weight_one(&map));
        let expr_cols = ["a", "b", "c", "val", "a2", "c2", "vp"];
        assert_eq!(db.scan("v_expr", &expr_cols), at_weight_one(&expr));
    };

    check(&mut db, &[[1, 1, 1, 100], [1, 1, 2, 200], [1, 2, 1, 300]]);
    db.exec("UPDATE t SET val = 250 WHERE a = 1 AND b = 1 AND c = 2");
    check(&mut db, &[[1, 1, 1, 100], [1, 1, 2, 250], [1, 2, 1, 300]]);
    db.exec("DELETE FROM t WHERE a = 1 AND b = 1 AND c = 1");
    check(&mut db, &[[1, 1, 2, 250], [1, 2, 1, 300]]);
}

// ── Reduce maps ──────────────────────────────────────────────────────────────

/// A grouped view may compute on the way into the reduce (a computed key or
/// aggregate argument) and on the way out (arithmetic over aggregates); a SELECT
/// item names a group key by what it binds to; `GROUP BY <n>` is the n-th
/// SELECT item; HAVING binds its own aggregate, not the first one in the list.
#[test]
fn reduce_maps_and_group_key_resolution() {
    /// `(view, body, columns, rows at weight 1)`.
    type Case = (
        &'static str,
        &'static str,
        &'static [&'static str],
        &'static [&'static [i64]],
    );
    let views: [Case; 14] = [
        (
            "pre_arg",
            "SELECT k, SUM(a * b) AS s FROM t GROUP BY k",
            &["k", "s"],
            &[&[1, 26], &[2, 6]],
        ),
        (
            "pre_key",
            "SELECT a + b AS ab, COUNT(*) AS c FROM t GROUP BY a + b",
            &["ab", "c"],
            &[&[5, 2], &[9, 1]],
        ),
        (
            "post",
            "SELECT k, SUM(a) + 1 AS s FROM t GROUP BY k",
            &["k", "s"],
            &[&[1, 7], &[2, 3]],
        ),
        (
            "qualified",
            "SELECT a + b AS ab, COUNT(*) AS c FROM t GROUP BY t.a + b",
            &["ab", "c"],
            &[&[5, 2], &[9, 1]],
        ),
        (
            "parens",
            "SELECT (a + b) AS ab, SUM((a * b)) AS s FROM t GROUP BY a + b",
            &["ab", "s"],
            &[&[5, 12], &[9, 20]],
        ),
        (
            "reused",
            "SELECT (a + b) * 2 AS x, SUM(a + b) AS s FROM t GROUP BY a + b",
            &["x", "s"],
            &[&[10, 10], &[18, 9]],
        ),
        (
            "overlapping",
            "SELECT a + b AS s, (a + b) * 2 AS d, COUNT(*) AS c FROM t GROUP BY a + b, (a + b) * 2",
            &["s", "d", "c"],
            &[&[5, 10, 2], &[9, 18, 1]],
        ),
        (
            "null_test",
            "SELECT a + b AS ab, COUNT(*) AS c FROM t GROUP BY a + b HAVING (a + b) IS NOT NULL",
            &["ab", "c"],
            &[&[5, 2], &[9, 1]],
        ),
        (
            "having",
            "SELECT k, SUM(a) AS s1, MAX(b) AS m2 FROM t GROUP BY k HAVING MAX(b) = 3",
            &["k", "s1", "m2"],
            &[&[2, 2, 3]],
        ),
        (
            "p_col",
            "SELECT k, COUNT(*) AS c FROM t GROUP BY 1",
            &["k", "c"],
            &[&[1, 2], &[2, 1]],
        ),
        (
            "p_expr",
            "SELECT a + b AS ab, COUNT(*) AS c FROM t GROUP BY 1",
            &["ab", "c"],
            &[&[5, 2], &[9, 1]],
        ),
        (
            "p_two",
            "SELECT k, a AS x, COUNT(*) AS c FROM t GROUP BY 2, 1",
            &["k", "x", "c"],
            &[&[1, 2, 1], &[1, 4, 1], &[2, 2, 1]],
        ),
        (
            "j_expr",
            "SELECT l.v + 1 AS x, COUNT(*) AS c FROM l JOIN r ON l.k = r.k GROUP BY l.v",
            &["x", "c"],
            &[&[11, 1], &[21, 1]],
        ),
        (
            "j_both",
            "SELECT l.v + r.v AS x, COUNT(*) AS c FROM l JOIN r ON l.k = r.k GROUP BY l.v + r.v",
            &["x", "c"],
            &[&[110, 1], &[120, 1]],
        ),
    ];
    let mut db = Db::boot(4);
    db.exec(
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, a BIGINT NOT NULL, b BIGINT NOT NULL);
         CREATE TABLE l (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, v BIGINT NOT NULL);
         CREATE TABLE r (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, v BIGINT NOT NULL)",
    );
    for (name, body, _, _) in views {
        db.exec(&format!("CREATE VIEW {name} AS {body}"));
    }
    db.exec(
        "INSERT INTO t VALUES (1, 1, 2, 3), (2, 1, 4, 5), (3, 2, 2, 3);
         INSERT INTO l VALUES (1, 1, 10), (2, 1, 20);
         INSERT INTO r VALUES (1, 1, 100)",
    );
    for (name, _, cols, want) in views {
        let want: Vec<Vec<i64>> = want.iter().map(|r| r.to_vec()).collect();
        assert_eq!(db.scan(name, cols), at_weight_one(&want), "{name}");
    }

    // An insert joins the existing a+b=5 group; deleting the last members of a
    // group removes it rather than leaving a ghost.
    db.exec("INSERT INTO t VALUES (4, 1, 1, 4)");
    assert_eq!(db.scan("pre_key", &["ab", "c"]), [[5, 3, 1], [9, 1, 1]]);
    db.exec("DELETE FROM t WHERE id = 1; DELETE FROM t WHERE id = 4");
    assert_eq!(db.scan("pre_key", &["ab", "c"]), [[5, 1, 1], [9, 1, 1]]);
    assert_eq!(db.scan("pre_arg", &["k", "s"]), [[1, 20, 1], [2, 6, 1]]);
}

// ── Ad-hoc fold vs. maintained view ──────────────────────────────────────────

/// The ad-hoc GROUP BY / DISTINCT fold agrees with the maintained view on rows,
/// weights and visible columns; ORDER BY … LIMIT … OFFSET windows the fold.
#[test]
fn adhoc_fold_matches_view() {
    let mut db = Db::boot(4);
    let gb = "SELECT g, COUNT(*) AS n FROM t GROUP BY g";
    let distinct = "SELECT DISTINCT g FROM t";
    db.exec(&format!(
        "CREATE TABLE t (id BIGINT PRIMARY KEY, g BIGINT NOT NULL);
         CREATE VIEW v_gb AS {gb};
         CREATE VIEW v_d AS {distinct};
         INSERT INTO t VALUES (1, 10), (2, 10), (3, 20), (4, 20), (5, 30), (6, 30)"
    ));

    for (sql, view, cols, expected) in [
        (gb, "v_gb", &["g", "n"][..], vec![vec![10, 2], vec![20, 2], vec![30, 2]]),
        (distinct, "v_d", &["g"], vec![vec![10], vec![20], vec![30]]),
    ] {
        assert_eq!(db.rows(sql, cols), at_weight_one(&expected), "{sql}");
        assert_eq!(db.scan(view, cols), at_weight_one(&expected), "{view}");
        assert_eq!(visible_names(&db.read(sql).0), cols);
        assert_eq!(visible_names(&db.read(&format!("SELECT * FROM {view}")).0), cols);
    }
    assert_eq!(
        db.rows(&format!("{gb} ORDER BY g DESC LIMIT 2 OFFSET 1"), &["g", "n"]),
        [[10, 2, 1], [20, 2, 1]]
    );
}

// ── Joins ────────────────────────────────────────────────────────────────────

/// A flipped operator or wrong constant admits the `ind = 6` row; a flipped
/// residual picks the other `b` row.
#[test]
fn where_and_residual_weights_multiworker() {
    let mut db = Db::boot(4);
    db.exec(
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, ind BIGINT NOT NULL, other BIGINT NOT NULL);
         CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, v BIGINT NOT NULL);
         CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, w BIGINT NOT NULL);
         CREATE VIEW vw AS SELECT other FROM t WHERE ind = 5;
         CREATE VIEW vr AS SELECT a.id AS aid, b.id AS bid FROM a JOIN b ON a.k = b.k AND a.v > b.w;
         INSERT INTO t VALUES (1, 5, 100), (2, 6, 200), (3, 5, 300);
         INSERT INTO a VALUES (1, 10, 100);
         INSERT INTO b VALUES (1, 10, 50), (2, 10, 200)",
    );
    assert_eq!(db.scan("vw", &["other"]), [[100, 1], [300, 1]]);
    assert_eq!(db.scan("vr", &["aid", "bid"]), [[1, 1, 1]]);
    db.exec("DELETE FROM t WHERE id = 3; DELETE FROM b WHERE id = 1");
    assert_eq!(db.scan("vw", &["other"]), [[100, 1]]);
    assert!(db.scan("vr", &["aid", "bid"]).is_empty());
}

/// The same table on both sides of a join — equi twice over, and range — at
/// weight 1 per pair: a pair counted on both sides reads as weight 2.
#[test]
fn self_join_weights_multiworker() {
    let mut db = Db::boot(4);
    db.exec(
        "CREATE TABLE emp (id BIGINT NOT NULL PRIMARY KEY, mgr BIGINT NOT NULL, nm BIGINT NOT NULL);
         CREATE VIEW v2 AS SELECT e.nm AS emp, m.nm AS boss FROM emp e JOIN emp m ON e.mgr = m.id;
         CREATE VIEW v3 AS SELECT e.nm AS emp, m.nm AS boss, g.nm AS grandboss \
         FROM emp e JOIN emp m ON e.mgr = m.id JOIN emp g ON m.mgr = g.id;
         CREATE VIEW vr AS SELECT a.id AS lo, b.id AS hi FROM emp a JOIN emp b ON a.nm < b.nm;
         INSERT INTO emp VALUES (1, 0, 100), (2, 1, 200), (3, 2, 300)",
    );
    let check = |db: &mut Db, v2: &[[i64; 3]], v3: &[[i64; 4]], vr: &[[i64; 3]]| {
        assert_eq!(db.scan("v2", &["emp", "boss"]), v2);
        assert_eq!(db.scan("v3", &["emp", "boss", "grandboss"]), v3);
        assert_eq!(db.scan("vr", &["lo", "hi"]), vr);
    };
    check(
        &mut db,
        &[[200, 100, 1], [300, 200, 1]],
        &[[300, 200, 100, 1]],
        &[[1, 2, 1], [1, 3, 1], [2, 3, 1]],
    );

    db.exec("INSERT INTO emp VALUES (4, 2, 400)");
    check(
        &mut db,
        &[[200, 100, 1], [300, 200, 1], [400, 200, 1]],
        &[[300, 200, 100, 1], [400, 200, 100, 1]],
        &[[1, 2, 1], [1, 3, 1], [1, 4, 1], [2, 3, 1], [2, 4, 1], [3, 4, 1]],
    );

    // Every chain reaches employee 1; retracting it empties the 3-level view.
    db.exec("DELETE FROM emp WHERE id = 1");
    check(
        &mut db,
        &[[300, 200, 1], [400, 200, 1]],
        &[],
        &[[2, 3, 1], [2, 4, 1], [3, 4, 1]],
    );
}

/// `a3 ⋈ b3` is materialized as a hidden segment that the third step reads. A
/// segment carrying the wrong columns or weights doubles or drops a chain row;
/// a LEFT step over it null-fills at the segment's weight.
#[test]
fn three_way_chain_weights_multiworker() {
    let mut db = Db::boot(4);
    db.exec(
        "CREATE TABLE a3 (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, av BIGINT NOT NULL);
         CREATE TABLE b3 (id BIGINT NOT NULL PRIMARY KEY, bk BIGINT NOT NULL, bv BIGINT NOT NULL);
         CREATE TABLE c3 (id BIGINT NOT NULL PRIMARY KEY, cv BIGINT NOT NULL);
         CREATE VIEW vi AS SELECT a3.av AS av, b3.bv AS bv, c3.cv AS cv \
         FROM a3 JOIN b3 ON a3.k = b3.bk JOIN c3 ON a3.id = c3.id;
         CREATE VIEW vl AS SELECT a3.av AS av, b3.bv AS bv, c3.cv AS cv \
         FROM a3 JOIN b3 ON a3.k = b3.bk LEFT JOIN c3 ON a3.id = c3.id;
         INSERT INTO a3 VALUES (1, 10, 100), (2, 10, 101);
         INSERT INTO b3 VALUES (1, 10, 200);
         INSERT INTO c3 VALUES (1, 300)",
    );
    let cols = ["av", "bv", "cv"];
    assert_eq!(db.scan("vi", &cols), [[100, 200, 300, 1]]);
    assert_eq!(db.scan("vl", &cols), [[100, 200, 300, 1], [101, 200, NULL, 1]]);

    db.exec("DELETE FROM a3 WHERE id = 2");
    assert_eq!(db.scan("vi", &cols), [[100, 200, 300, 1]]);
    assert_eq!(db.scan("vl", &cols), [[100, 200, 300, 1]]);

    // The segment emptied; the matched and the null-filled row go with it.
    db.exec("DELETE FROM b3 WHERE id = 1");
    assert!(db.scan("vi", &cols).is_empty());
    assert!(db.scan("vl", &cols).is_empty());
}

/// A LEFT JOIN whose preserved side has a compound PK null-fills its unmatched
/// rows once, with the compound key intact.
#[test]
fn left_join_over_compound_pk_preserved_side() {
    let mut db = Db::boot(4);
    db.exec(
        "CREATE TABLE a (k1 BIGINT NOT NULL, k2 BIGINT NOT NULL, fk BIGINT NOT NULL, \
         av BIGINT NOT NULL, PRIMARY KEY (k1, k2));
         CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, fk BIGINT NOT NULL, bv BIGINT NOT NULL);
         CREATE VIEW v AS SELECT a.k1, a.k2, a.av, b.bv FROM a LEFT JOIN b ON a.fk = b.fk;
         INSERT INTO a (k1, k2, fk, av) VALUES (1, 100, 7, 11), (2, 200, 9, 22);
         INSERT INTO b (id, fk, bv) VALUES (1, 7, 70)",
    );
    assert_eq!(
        db.scan("v", &["k1", "k2", "av", "bv"]),
        [[1, 100, 11, 70, 1], [2, 200, 22, NULL, 1]]
    );
}

// ── Capacity-bounded views ───────────────────────────────────────────────────

/// A bounded filter/projection (over a table, a compound-PK table, a view) and
/// a bounded inner equi-join with a residual hold the same rows as their bodies.
#[test]
fn bounded_views_hold_their_rows() {
    let mut db = Db::boot(4);
    db.exec(
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL, s TEXT);
         CREATE TABLE u (id BIGINT NOT NULL PRIMARY KEY, tid BIGINT NOT NULL, w BIGINT NOT NULL);
         CREATE TABLE c (a BIGINT NOT NULL, b BIGINT NOT NULL, x BIGINT NOT NULL, PRIMARY KEY (a, b));
         CREATE VIEW plain AS SELECT id, v FROM t;
         CREATE VIEW l1 WITH (capacity = '1 MB') AS SELECT id, v, s FROM t WHERE v > 2;
         CREATE VIEW l2 WITH (capacity = '1 MB') AS SELECT a, b, x FROM c WHERE x < 9;
         CREATE VIEW l3 WITH (capacity = '1 MB') AS SELECT id, v FROM plain;
         CREATE VIEW j1 WITH (capacity = '1 MB') AS \
         SELECT t.id, t.s, u.w FROM t JOIN u ON t.id = u.tid AND u.w <> t.v;
         INSERT INTO t VALUES (1, 1, 'a'), (2, 3, 'b'), (3, 5, NULL);
         INSERT INTO u VALUES (1, 1, 7), (2, 3, 3), (3, 3, 9), (4, 2, 3);
         INSERT INTO c VALUES (1, 1, 5), (1, 2, 10), (2, 1, 8)",
    );
    assert_eq!(db.scan("l1", &["id", "v"]), [[2, 3, 1], [3, 5, 1]]);
    assert_eq!(db.scan("l2", &["a", "b", "x"]), [[1, 1, 5, 1], [2, 1, 8, 1]]);
    assert_eq!(db.scan("l3", &["id", "v"]), [[1, 1, 1], [2, 3, 1], [3, 5, 1]]);
    assert_eq!(db.scan("j1", &["id", "w"]), [[1, 7, 1], [3, 3, 1], [3, 9, 1]]);
}

/// Which bodies may be capacity-bounded, as a live server decides it.
#[test]
fn bounded_view_eligibility_is_the_engines() {
    let mut db = Db::boot(4);
    db.exec(
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL);
         CREATE TABLE u (id BIGINT NOT NULL PRIMARY KEY, tid BIGINT NOT NULL, w BIGINT NOT NULL)",
    );
    let bounded = |body: &str| format!("CREATE VIEW b WITH (capacity = '1 MB') AS {body}");
    for body in [
        "SELECT tid, SUM(w) AS s FROM u GROUP BY tid",
        "SELECT id, v FROM t WHERE NOT EXISTS (SELECT 1 FROM u WHERE u.tid = t.id)",
        "SELECT DISTINCT v FROM t",
        "SELECT id, v FROM t UNION ALL SELECT id, w FROM u",
        "SELECT t.id, u.w FROM t LEFT JOIN u ON t.id = u.tid",
        "SELECT t.id, u.w FROM t RIGHT JOIN u ON t.id = u.tid",
        "SELECT t.id, u.w FROM t FULL JOIN u ON t.id = u.tid",
        "SELECT t.id, u.w FROM t JOIN u ON t.id = u.tid AND t.v < u.w",
        "SELECT t.id, u.w FROM t CROSS JOIN u",
        "SELECT id FROM t ORDER BY v LIMIT 1",
    ] {
        db.refuses(&bounded(body), Refused(Error), "capacity-bounded view:");
    }
    // A body the planner splits into a chain never reaches the engine's rule.
    db.refuses(
        &bounded("SELECT d.v FROM (SELECT DISTINCT v FROM t) d"),
        Rejected,
        "more than one view",
    );
    for body in [
        "SELECT t.id, t.v + u.w AS z FROM t JOIN u ON t.id = u.tid",
        "SELECT id, v FROM t WHERE EXISTS (SELECT 1 FROM u WHERE u.tid = t.id)",
        "SELECT id, v FROM t WHERE id IN (SELECT tid FROM u)",
        "SELECT d.id FROM (SELECT id, v FROM t) d",
    ] {
        db.exec(&bounded(body));
        db.exec("DROP VIEW b");
    }
}
