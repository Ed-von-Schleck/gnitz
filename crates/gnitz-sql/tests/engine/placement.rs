//! Placement contracts at four workers, asserted as whole weighted multisets.
//! A co-partition skip over rows that are not co-located, a view addressed by
//! its key rather than where its rows were made, or a replicated source counted
//! once per worker all read as wrong weights here.

use super::*;
use std::collections::BTreeMap;

/// Columns `cols` of every row, sorted.
fn project(rows: &[Vec<i64>], cols: &[usize]) -> Vec<Vec<i64>> {
    let mut out: Vec<Vec<i64>> = rows.iter().map(|r| cols.iter().map(|&c| r[c]).collect()).collect();
    out.sort();
    out
}

/// `[key…, COUNT(*), SUM(val)]` per group of `rows`, every group at weight 1.
fn grouped(rows: &[Vec<i64>], keys: &[usize], val: usize) -> Vec<Vec<i64>> {
    let mut m: BTreeMap<Vec<i64>, (i64, i64)> = BTreeMap::new();
    for r in rows {
        let e = m.entry(keys.iter().map(|&k| r[k]).collect()).or_insert((0, 0));
        e.0 += 1;
        e.1 += r[val];
    }
    let groups: Vec<Vec<i64>> = m.into_iter().map(|(k, (n, s))| [k, vec![n, s]].concat()).collect();
    at_weight_one(&groups)
}

// ── Every view shape over prefix-clustered tables matches a recompute ────────

/// `t(a, b, v) PRIMARY KEY (a, b) CLUSTER BY a`: 20 groups of 5, `v = 100a + b`.
fn t_rows() -> Vec<Vec<i64>> {
    (1..=20i64)
        .flat_map(|a| (1..=5i64).map(move |b| vec![a, b, a * 100 + b]))
        .collect()
}

/// `t3(a, b, c, v) CLUSTER BY a, b`: 18 `(a, b)` groups of 2.
fn t3_rows() -> Vec<Vec<i64>> {
    (1..=3i64)
        .flat_map(|a| (1..=6i64).flat_map(move |b| (1..=2i64).map(move |c| vec![a, b, c, a * 10000 + b * 100 + c])))
        .collect()
}

/// `ts(a, b, c, v) CLUSTER BY a` with a signed `a` straddling zero; `tf` is the
/// same shape shifted above zero under `CLUSTER BY a, b, c`.
fn ts_rows() -> Vec<Vec<i64>> {
    [-3i64, -2, -1, 1, 2, 3]
        .iter()
        .flat_map(|&a| (1..=3i64).flat_map(move |b| (1..=2i64).map(move |c| vec![a, b, c, b * 10 + c])))
        .collect()
}

/// `sb(b1, b2, vb)` under the default full-PK distribution: `t`'s keys with an
/// odd `b`, plus one key `t` never holds.
fn sb_rows() -> Vec<Vec<i64>> {
    let mut rows: Vec<Vec<i64>> = (1..=20i64)
        .flat_map(|a| [1i64, 3, 5].into_iter().map(move |b| vec![a, b, a * 1000 + b]))
        .collect();
    rows.push(vec![99, 99, 0]);
    rows
}

/// `sb2(b1, b2, vb2)`, default distribution: `t`'s keys with `b <= 3`.
fn sb2_rows() -> Vec<Vec<i64>> {
    (1..=20i64)
        .flat_map(|a| (1..=3i64).map(move |b| vec![a, b, a + b]))
        .collect()
}

fn create_tables(db: &mut Db) {
    let two = "a BIGINT UNSIGNED, b BIGINT UNSIGNED, v BIGINT NOT NULL, PRIMARY KEY (a, b)";
    let three = "b BIGINT UNSIGNED, c BIGINT UNSIGNED, v BIGINT NOT NULL, PRIMARY KEY (a, b, c)";
    let sb = |v: &str| format!("b1 BIGINT UNSIGNED, b2 BIGINT UNSIGNED, {v} BIGINT NOT NULL, PRIMARY KEY (b1, b2)");
    db.exec(&format!(
        "CREATE TABLE t ({two}) CLUSTER BY a;
         CREATE TABLE u (id BIGINT UNSIGNED PRIMARY KEY, w BIGINT NOT NULL);
         CREATE TABLE t3 (a BIGINT UNSIGNED, {three}) CLUSTER BY a, b;
         CREATE TABLE ts (a BIGINT, {three}) CLUSTER BY a;
         CREATE TABLE tf (a BIGINT UNSIGNED, {three}) CLUSTER BY a, b, c;
         CREATE TABLE sb ({});
         CREATE TABLE sb2 ({})",
        sb("vb"),
        sb("vb2")
    ));
}

fn insert_all(db: &mut Db) {
    db.insert("t", &["a", "b", "v"], &t_rows());
    let u: Vec<Vec<i64>> = (1..=20i64).map(|id| vec![id, id * 7]).collect();
    db.insert("u", &["id", "w"], &u);
    db.insert("t3", &["a", "b", "c", "v"], &t3_rows());
    db.insert("ts", &["a", "b", "c", "v"], &ts_rows());
    let tf: Vec<Vec<i64>> = ts_rows().iter().map(|r| vec![r[0] + 4, r[1], r[2], r[3]]).collect();
    db.insert("tf", &["a", "b", "c", "v"], &tf);
    db.insert("sb", &["b1", "b2", "vb"], &sb_rows());
    db.insert("sb2", &["b1", "b2", "vb2"], &sb2_rows());
}

/// Every shape whose placement depends on a distribution prefix: the linear
/// forms (direct, PK-dropping, view-over-view, derived table, CTE), GROUP BY on
/// the prefix / a non-prefix column / the full PK / its permutation, the same
/// under a WHERE, the downstream shapes over a linear view, a full-key join of a
/// k=1 side against a k=2 side, a default-distribution join, and the k=2 /
/// signed / k=|PK| prefixes.
fn create_views(db: &mut Db) {
    db.exec(
        "CREATE VIEW mv AS SELECT a, b, v FROM t WHERE v > 0;
         CREATE VIEW mv_pk AS SELECT v FROM t;
         CREATE VIEW mv2 AS SELECT a, b FROM mv;
         CREATE VIEW mvd AS SELECT a, b FROM (SELECT a, b, v FROM t WHERE v > 0) d;
         CREATE VIEW mvc AS WITH x AS (SELECT a, b, v FROM t WHERE v > 0) SELECT a, b FROM x;
         CREATE VIEW g_pre AS SELECT a AS ka, COUNT(*) AS n, SUM(v) AS s FROM t GROUP BY a;
         CREATE VIEW g_other AS SELECT b AS kb, COUNT(*) AS n, SUM(v) AS s FROM t GROUP BY b;
         CREATE VIEW g_full AS SELECT a AS ka, b AS kb, COUNT(*) AS n FROM t GROUP BY a, b;
         CREATE VIEW g_perm AS SELECT a AS ka, b AS kb, COUNT(*) AS n FROM t GROUP BY b, a;
         CREATE VIEW gf_pre AS SELECT a AS ka, COUNT(*) AS n, SUM(v) AS s FROM t WHERE b >= 3 GROUP BY a;
         CREATE VIEW gf_other AS SELECT b AS kb, COUNT(*) AS n, SUM(v) AS s FROM t WHERE b >= 3 GROUP BY b;
         CREATE VIEW gv_a AS SELECT a AS ka, COUNT(*) AS n, SUM(v) AS s FROM mv GROUP BY a;
         CREATE VIEW gv_b AS SELECT b AS kb, COUNT(*) AS n, SUM(v) AS s FROM mv GROUP BY b;
         CREATE VIEW mj AS SELECT m.a AS ka, m.b AS kb, u.w AS w FROM mv m JOIN u ON m.a = u.id;
         CREATE VIEW j_full AS SELECT t.a AS k1, t.b AS k2, t.v AS v, s.vb AS vb \
         FROM t JOIN sb s ON t.a = s.b1 AND t.b = s.b2;
         CREATE VIEW dj AS SELECT x.b1 AS k1, x.b2 AS k2, x.vb AS vb, y.vb2 AS vb2 \
         FROM sb x JOIN sb2 y ON x.b1 = y.b1 AND x.b2 = y.b2;
         CREATE VIEW v3 AS SELECT a, b, c, v FROM t3 WHERE v > 0;
         CREATE VIEW g3_base AS SELECT a AS ka, b AS kb, COUNT(*) AS n FROM t3 GROUP BY a, b;
         CREATE VIEW g3_view AS SELECT a AS ka, b AS kb, COUNT(*) AS n FROM v3 GROUP BY a, b;
         CREATE VIEW gs AS SELECT a AS ka, COUNT(*) AS n, SUM(v) AS s FROM ts GROUP BY a;
         CREATE VIEW gf AS SELECT a AS ka, b AS kb, c AS kc, COUNT(*) AS n FROM tf GROUP BY a, b, c",
    );
}

/// Every view against a recompute over `t` (the table the test later deletes
/// from) and the fixed contents of the others, plus keyed reads of `mv`: a
/// keyed read seeks the one worker its key names, so it answers only if the row
/// was placed there too.
fn assert_views(db: &mut Db, t: &[Vec<i64>], when: &str) {
    let mut check = |view: &str, cols: &[&str], want: Vec<Vec<i64>>| {
        assert_eq!(db.scan(view, cols), want, "{view} ({when})");
    };
    let ab = at_weight_one(&project(t, &[0, 1]));
    let full = project(&grouped(t, &[0, 1], 2), &[0, 1, 2, 4]);
    check("mv", &["a", "b", "v"], at_weight_one(t));
    check("mv_pk", &["v"], at_weight_one(&project(t, &[2])));
    check("mv2", &["a", "b"], ab.clone());
    check("mvd", &["a", "b"], ab.clone());
    check("mvc", &["a", "b"], ab);
    check("g_pre", &["ka", "n", "s"], grouped(t, &[0], 2));
    check("g_other", &["kb", "n", "s"], grouped(t, &[1], 2));
    check("g_full", &["ka", "kb", "n"], full.clone());
    check("g_perm", &["ka", "kb", "n"], full);
    let filtered: Vec<Vec<i64>> = t.iter().filter(|r| r[1] >= 3).cloned().collect();
    check("gf_pre", &["ka", "n", "s"], grouped(&filtered, &[0], 2));
    check("gf_other", &["kb", "n", "s"], grouped(&filtered, &[1], 2));
    check("gv_a", &["ka", "n", "s"], grouped(t, &[0], 2));
    check("gv_b", &["kb", "n", "s"], grouped(t, &[1], 2));
    let mj: Vec<Vec<i64>> = t.iter().map(|r| vec![r[0], r[1], r[0] * 7]).collect();
    check("mj", &["ka", "kb", "w"], at_weight_one(&mj));
    let j_full: Vec<Vec<i64>> = t
        .iter()
        .filter(|r| r[1] % 2 == 1)
        .map(|r| vec![r[0], r[1], r[2], r[0] * 1000 + r[1]])
        .collect();
    check("j_full", &["k1", "k2", "v", "vb"], at_weight_one(&j_full));
    let dj: Vec<Vec<i64>> = (1..=20i64)
        .flat_map(|a| [1i64, 3].into_iter().map(move |b| vec![a, b, a * 1000 + b, a + b]))
        .collect();
    check("dj", &["k1", "k2", "vb", "vb2"], at_weight_one(&dj));
    check("v3", &["a", "b", "c", "v"], at_weight_one(&t3_rows()));
    let g3 = project(&grouped(&t3_rows(), &[0, 1], 3), &[0, 1, 2, 4]);
    check("g3_base", &["ka", "kb", "n"], g3.clone());
    check("g3_view", &["ka", "kb", "n"], g3);
    check("gs", &["ka", "n", "s"], grouped(&ts_rows(), &[0], 3));
    let gf: Vec<Vec<i64>> = ts_rows().iter().map(|r| vec![r[0] + 4, r[1], r[2], 1]).collect();
    check("gf", &["ka", "kb", "kc", "n"], at_weight_one(&gf));

    for (a, b) in [(1i64, 1i64), (7, 3), (13, 2), (20, 1)] {
        let want = at_weight_one(&t.iter().filter(|r| r[..2] == [a, b]).cloned().collect::<Vec<_>>());
        let sql = format!("SELECT * FROM mv WHERE a = {a} AND b = {b}");
        assert_eq!(db.rows(&sql, &["a", "b", "v"]), want, "{sql} ({when})");
    }
    let group7 = at_weight_one(&t.iter().filter(|r| r[0] == 7).cloned().collect::<Vec<_>>());
    assert_eq!(
        db.rows("SELECT * FROM mv WHERE a = 7", &["a", "b", "v"]),
        group7,
        "({when})"
    );
}

/// Both creation orders — views before the data materialize tick by tick,
/// views after it are backfilled — then a retraction through every shape. A row
/// placed on one worker and addressed on another is missing here; a group
/// aggregated on two workers is doubled.
#[test]
fn views_over_prefix_clustered_tables_match_a_recompute_multiworker() {
    for views_first in [true, false] {
        let when = if views_first { "incremental" } else { "backfill" };
        let mut db = Db::boot(4);
        create_tables(&mut db);
        if views_first {
            create_views(&mut db);
            insert_all(&mut db);
        } else {
            insert_all(&mut db);
            create_views(&mut db);
        }
        assert_views(&mut db, &t_rows(), when);

        assert_eq!(db.affected("DELETE FROM t WHERE b >= 4"), 40, "{when}");
        let survivors: Vec<Vec<i64>> = t_rows().into_iter().filter(|r| r[1] < 4).collect();
        assert_eq!(db.scan("t", &["a", "b", "v"]), at_weight_one(&survivors));
        assert_views(&mut db, &survivors, &format!("{when}, after delete"));
    }
}

/// Two rows sharing a clustering prefix but not the PK suffix are found by an
/// UPSERT and a DELETE on that prefix — a probe on the wrong slice leaves a
/// duplicate PK or a ghost.
#[test]
fn prefix_twins_are_found_by_upsert_and_delete_multiworker() {
    let mut db = Db::boot(4);
    db.exec(
        "CREATE TABLE u (k1 BIGINT UNSIGNED, k2 BIGINT UNSIGNED, v BIGINT NOT NULL, PRIMARY KEY (k1, k2)) CLUSTER BY k1;
         INSERT INTO u (k1, k2, v) VALUES (5, 1, 100), (5, 2, 200), (5, 3, 300), (8, 1, 1);
         INSERT INTO u (k1, k2, v) VALUES (5, 1, 999) ON CONFLICT DO UPDATE SET v = EXCLUDED.v",
    );
    assert_eq!(db.affected("DELETE FROM u WHERE k1 = 5 AND k2 = 2"), 1);
    assert_eq!(
        db.scan("u", &["k1", "k2", "v"]),
        at_weight_one(&[vec![5, 1, 999], vec![5, 3, 300], vec![8, 1, 1]]),
    );
}

// ── Placement is transitive down a view chain ────────────────────────────────

/// `d` replicated, `f` and `p` partitioned; every `f` row references a `d` row.
/// A linear view over `f ⋈ d` produces its rows where the join did, not where
/// their key hashes, and so does every view over it: rows addressed by key over
/// such a source are silently missing.
#[test]
fn placement_is_transitive_down_a_view_chain_multiworker() {
    let mut db = Db::boot(4);
    let dim_of = |id: i64| (id - 1) % 10 + 1;
    db.exec(
        "CREATE TABLE d (id BIGINT UNSIGNED PRIMARY KEY, nm BIGINT NOT NULL) WITH (replicated = true);
         CREATE TABLE f (id BIGINT UNSIGNED PRIMARY KEY, r BIGINT UNSIGNED NOT NULL);
         CREATE TABLE p (id BIGINT UNSIGNED PRIMARY KEY, q BIGINT NOT NULL);
         CREATE VIEW jm AS SELECT f.id AS fid, d.nm AS nm FROM f JOIN d ON f.r = d.id;
         CREATE VIEW dv AS SELECT fid, nm FROM jm WHERE nm > 0;
         CREATE VIEW dv2 AS SELECT fid FROM dv;
         CREATE VIEW gj AS SELECT nm AS knm, COUNT(*) AS n FROM jm GROUP BY nm;
         CREATE VIEW rv AS SELECT id, nm FROM d WHERE nm > 0;
         CREATE VIEW ua AS SELECT id FROM d UNION ALL SELECT id FROM f;
         CREATE VIEW uav AS SELECT id FROM ua WHERE id > 0;
         CREATE VIEW pj AS SELECT f.id AS fid, p.q AS q FROM f JOIN p ON f.id = p.id;
         CREATE VIEW pjv AS SELECT fid, q FROM pj WHERE q > 0",
    );
    let d: Vec<Vec<i64>> = (1..=10i64).map(|id| vec![id, id * 10]).collect();
    db.insert("d", &["id", "nm"], &d);
    let f: Vec<Vec<i64>> = (1..=100i64).map(|id| vec![id, dim_of(id)]).collect();
    db.insert("f", &["id", "r"], &f);
    let p: Vec<Vec<i64>> = (1..=100i64).map(|id| vec![id, id * 3]).collect();
    db.insert("p", &["id", "q"], &p);

    let joined: Vec<Vec<i64>> = (1..=100i64).map(|id| vec![id, dim_of(id) * 10]).collect();
    assert_eq!(db.scan("jm", &["fid", "nm"]), at_weight_one(&joined));
    assert_eq!(db.scan("dv", &["fid", "nm"]), at_weight_one(&joined));
    let fids: Vec<Vec<i64>> = (1..=100i64).map(|id| vec![id]).collect();
    assert_eq!(db.scan("dv2", &["fid"]), at_weight_one(&fids));
    let gj: Vec<Vec<i64>> = (1..=10i64).map(|k| vec![k * 10, 10]).collect();
    assert_eq!(db.scan("gj", &["knm", "n"]), at_weight_one(&gj));

    // The shapes around it: a fully replicated view reads back one copy; a bag
    // union of a replicated and a partitioned side keeps both branches through a
    // linear hop; a join of two partitioned sides and its hop stay key-placed.
    assert_eq!(db.scan("rv", &["id", "nm"]), at_weight_one(&d));
    let bag: Vec<Vec<i64>> = (1..=100i64).map(|id| vec![id, if id <= 10 { 2 } else { 1 }]).collect();
    assert_eq!(db.scan("ua", &["id"]), bag);
    assert_eq!(db.scan("uav", &["id"]), bag);
    assert_eq!(db.scan("pj", &["fid", "q"]), at_weight_one(&p));
    assert_eq!(db.scan("pjv", &["fid", "q"]), at_weight_one(&p));
    // A keyed read of the hop over the replicated join gathers; one of the
    // key-placed hop seeks its owner.
    for id in [1i64, 37, 100] {
        let sql = format!("SELECT * FROM dv WHERE fid = {id}");
        assert_eq!(db.rows(&sql, &["fid", "nm"]), [[id, dim_of(id) * 10, 1]], "{sql}");
        let sql = format!("SELECT * FROM pjv WHERE fid = {id}");
        assert_eq!(db.rows(&sql, &["fid", "q"]), [[id, id * 3, 1]], "{sql}");
    }
}

// ── Replicated sources ───────────────────────────────────────────────────────

/// Every worker holds a replicated relation in full. A view over only replicated
/// sources is replicated too, so each downstream exchange must not relay one
/// copy per worker: `g = 7` reads 120 instead of 30 when it does. The tick path,
/// the backfill path (views created after the data) and a mixed-source control
/// share one server.
#[test]
fn replicated_chain_weights_multiworker() {
    let mut db = Db::boot(4);
    db.exec(
        "CREATE TABLE rt (id BIGINT NOT NULL PRIMARY KEY, g BIGINT NOT NULL, x BIGINT NOT NULL) \
         WITH (replicated = true);
         CREATE TABLE ru (id BIGINT NOT NULL PRIMARY KEY, g BIGINT NOT NULL, x BIGINT NOT NULL) \
         WITH (replicated = true);
         CREATE TABLE fact (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL);
         CREATE VIEW vbase AS SELECT g, SUM(x) AS s FROM rt GROUP BY g;
         CREATE VIEW rv AS SELECT id, g, x FROM rt;
         CREATE VIEW vagg AS SELECT g, SUM(x) AS s FROM rv GROUP BY g;
         CREATE VIEW vsum AS SELECT SUM(x) AS s FROM rv;
         CREATE VIEW vunion AS SELECT id, x FROM rv UNION ALL SELECT id, x FROM ru;
         -- Joined on a non-PK column of `rv`, so the join cannot be co-partitioned.
         CREATE VIEW vjoin AS SELECT fact.id AS fid, rv.x AS x FROM fact JOIN rv ON fact.k = rv.g;
         CREATE VIEW v3 AS SELECT g, s FROM vagg;
         CREATE VIEW vmixagg AS SELECT fid, SUM(x) AS s FROM vjoin GROUP BY fid;
         CREATE VIEW vdist AS SELECT DISTINCT g FROM rv;
         CREATE VIEW vlin AS SELECT id, x FROM rv WHERE x > 0;
         CREATE VIEW vcv AS SELECT COUNT(*) AS c FROM rv WHERE x > 1000;
         CREATE VIEW vcb AS SELECT COUNT(*) AS c FROM rt WHERE x > 1000;
         INSERT INTO rt VALUES (1, 7, 10), (2, 7, 20), (3, 8, 5);
         INSERT INTO ru VALUES (1, 7, 10), (2, 7, 20), (3, 8, 5);
         INSERT INTO fact VALUES (1, 7), (2, 8);
         CREATE VIEW rv_b AS SELECT id, g, x FROM rt;
         CREATE VIEW vagg_b AS SELECT g, SUM(x) AS s FROM rv_b GROUP BY g",
    );
    for view in ["vbase", "vagg", "v3", "vagg_b"] {
        assert_eq!(db.scan(view, &["g", "s"]), [[7, 30, 1], [8, 5, 1]], "{view}");
    }
    assert_eq!(db.scan("vsum", &["s"]), [[35, 1]]);
    assert_eq!(
        db.scan("vunion", &["id", "x"]),
        [[1, 10, 2], [2, 20, 2], [3, 5, 2]],
        "weight 2, one per side, not one per worker per side",
    );
    assert_eq!(db.scan("vjoin", &["fid", "x"]), [[1, 10, 1], [1, 20, 1], [2, 5, 1]]);
    assert_eq!(
        db.scan("vmixagg", &["fid", "s"]),
        [[1, 30, 1], [2, 5, 1]],
        "a mixed-source view is not replicated",
    );
    assert_eq!(db.scan("vdist", &["g"]), [[7, 1], [8, 1]]);
    assert_eq!(db.scan("vlin", &["id", "x"]), [[1, 10, 1], [2, 20, 1], [3, 5, 1]]);
    // The empty global aggregate: exactly one ground row, on the worker the read goes to.
    assert_eq!(db.scan("vcv", &["c"]), [[0, 1]]);
    assert_eq!(db.scan("vcb", &["c"]), [[0, 1]]);

    db.exec("DELETE FROM rt WHERE id = 2");
    for view in ["vbase", "vagg", "v3", "vagg_b"] {
        assert_eq!(
            db.scan(view, &["g", "s"]),
            [[7, 10, 1], [8, 5, 1]],
            "{view} after the retraction"
        );
    }
}
