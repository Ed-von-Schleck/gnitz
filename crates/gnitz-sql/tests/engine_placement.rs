#![cfg(feature = "integration")]

//! Placement contracts at four workers, asserted as whole weighted multisets.
//! A co-partition skip over rows that are not co-located, a view addressed by
//! its key rather than where its rows were made, or a replicated source counted
//! once per worker all read as wrong weights here.

mod common;
use common::*;
use gnitz_core::GnitzClient;
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
    let groups: Vec<Vec<i64>> = m
        .into_iter()
        .map(|(mut k, (n, s))| {
            k.push(n);
            k.push(s);
            k
        })
        .collect();
    at_weight_one(&groups)
}

// ── CLUSTER BY admission ─────────────────────────────────────────────────────

#[test]
fn cluster_by_admission() {
    let (_srv, mut client, sn) = boot(1);
    let two = "a BIGINT UNSIGNED, b BIGINT UNSIGNED, v BIGINT NOT NULL, PRIMARY KEY (a, b)";
    exec(&mut client, &sn, &format!("CREATE TABLE ok1 ({two}) CLUSTER BY a"));
    exec(&mut client, &sn, &format!("CREATE TABLE ok2 ({two}) CLUSTER BY a, b"));
    exec(
        &mut client,
        &sn,
        "CREATE TABLE ok3 (a BIGINT UNSIGNED PRIMARY KEY, v BIGINT NOT NULL) CLUSTER BY a",
    );
    for (clause, variant, msg) in [
        ("CLUSTER BY b", "Plan", "leading prefix"),
        ("CLUSTER BY b, a", "Plan", "leading prefix"),
        ("CLUSTER BY v", "Plan", "is not a PRIMARY KEY column"),
        ("CLUSTER BY nope", "Bind", "not found"),
        ("WITH (replicated = true) CLUSTER BY a", "Plan", "mutually exclusive"),
    ] {
        assert_rejects_variant(
            &mut client,
            &sn,
            &format!("CREATE TABLE bad ({two}) {clause}"),
            variant,
            msg,
        );
    }
    assert_rejects_variant(
        &mut client,
        &sn,
        "CREATE TABLE bad (a BIGINT UNSIGNED, b BIGINT UNSIGNED, c BIGINT UNSIGNED, v BIGINT NOT NULL, \
         PRIMARY KEY (a, b, c)) CLUSTER BY a, c",
        "Plan",
        "leading prefix",
    );
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

fn create_tables(client: &mut GnitzClient, sn: &str) {
    let two = "a BIGINT UNSIGNED, b BIGINT UNSIGNED, v BIGINT NOT NULL, PRIMARY KEY (a, b)";
    let three = "b BIGINT UNSIGNED, c BIGINT UNSIGNED, v BIGINT NOT NULL, PRIMARY KEY (a, b, c)";
    for ddl in [
        format!("CREATE TABLE t ({two}) CLUSTER BY a"),
        "CREATE TABLE u (id BIGINT UNSIGNED PRIMARY KEY, w BIGINT NOT NULL)".to_string(),
        format!("CREATE TABLE t3 (a BIGINT UNSIGNED, {three}) CLUSTER BY a, b"),
        format!("CREATE TABLE ts (a BIGINT, {three}) CLUSTER BY a"),
        format!("CREATE TABLE tf (a BIGINT UNSIGNED, {three}) CLUSTER BY a, b, c"),
        "CREATE TABLE sb (b1 BIGINT UNSIGNED, b2 BIGINT UNSIGNED, vb BIGINT NOT NULL, PRIMARY KEY (b1, b2))"
            .to_string(),
        "CREATE TABLE sb2 (b1 BIGINT UNSIGNED, b2 BIGINT UNSIGNED, vb2 BIGINT NOT NULL, PRIMARY KEY (b1, b2))"
            .to_string(),
    ] {
        exec(client, sn, &ddl);
    }
}

fn insert_all(client: &mut GnitzClient, sn: &str) {
    insert_rows(client, sn, "t", &["a", "b", "v"], &t_rows());
    let u: Vec<Vec<i64>> = (1..=20i64).map(|id| vec![id, id * 7]).collect();
    insert_rows(client, sn, "u", &["id", "w"], &u);
    insert_rows(client, sn, "t3", &["a", "b", "c", "v"], &t3_rows());
    insert_rows(client, sn, "ts", &["a", "b", "c", "v"], &ts_rows());
    let tf: Vec<Vec<i64>> = ts_rows().iter().map(|r| vec![r[0] + 4, r[1], r[2], r[3]]).collect();
    insert_rows(client, sn, "tf", &["a", "b", "c", "v"], &tf);
    insert_rows(client, sn, "sb", &["b1", "b2", "vb"], &sb_rows());
    insert_rows(client, sn, "sb2", &["b1", "b2", "vb2"], &sb2_rows());
}

/// Every shape whose placement depends on a distribution prefix: the linear
/// forms (direct, PK-dropping, view-over-view, derived table, CTE), GROUP BY on
/// the prefix / a non-prefix column / the full PK, the same under a WHERE, the
/// downstream shapes over a linear view, a full-key join of a k=1 side against a
/// k=2 side, a default-distribution join, and the k=2 / signed / k=|PK| prefixes.
fn create_views(client: &mut GnitzClient, sn: &str) {
    for ddl in [
        "CREATE VIEW mv AS SELECT a, b, v FROM t WHERE v > 0",
        "CREATE VIEW mv_pk AS SELECT v FROM t",
        "CREATE VIEW mv2 AS SELECT a, b FROM mv",
        "CREATE VIEW mvd AS SELECT a, b FROM (SELECT a, b, v FROM t WHERE v > 0) d",
        "CREATE VIEW mvc AS WITH x AS (SELECT a, b, v FROM t WHERE v > 0) SELECT a, b FROM x",
        "CREATE VIEW g_pre AS SELECT a AS ka, COUNT(*) AS n, SUM(v) AS s FROM t GROUP BY a",
        "CREATE VIEW g_other AS SELECT b AS kb, COUNT(*) AS n, SUM(v) AS s FROM t GROUP BY b",
        "CREATE VIEW g_full AS SELECT a AS ka, b AS kb, COUNT(*) AS n FROM t GROUP BY a, b",
        "CREATE VIEW gf_pre AS SELECT a AS ka, COUNT(*) AS n, SUM(v) AS s FROM t WHERE b >= 3 GROUP BY a",
        "CREATE VIEW gf_other AS SELECT b AS kb, COUNT(*) AS n, SUM(v) AS s FROM t WHERE b >= 3 GROUP BY b",
        "CREATE VIEW gv_a AS SELECT a AS ka, COUNT(*) AS n, SUM(v) AS s FROM mv GROUP BY a",
        "CREATE VIEW gv_b AS SELECT b AS kb, COUNT(*) AS n, SUM(v) AS s FROM mv GROUP BY b",
        "CREATE VIEW mj AS SELECT m.a AS ka, m.b AS kb, u.w AS w FROM mv m JOIN u ON m.a = u.id",
        "CREATE VIEW j_full AS SELECT t.a AS k1, t.b AS k2, t.v AS v, s.vb AS vb \
         FROM t JOIN sb s ON t.a = s.b1 AND t.b = s.b2",
        "CREATE VIEW dj AS SELECT x.b1 AS k1, x.b2 AS k2, x.vb AS vb, y.vb2 AS vb2 \
         FROM sb x JOIN sb2 y ON x.b1 = y.b1 AND x.b2 = y.b2",
        "CREATE VIEW v3 AS SELECT a, b, c, v FROM t3 WHERE v > 0",
        "CREATE VIEW g3_base AS SELECT a AS ka, b AS kb, COUNT(*) AS n FROM t3 GROUP BY a, b",
        "CREATE VIEW g3_view AS SELECT a AS ka, b AS kb, COUNT(*) AS n FROM v3 GROUP BY a, b",
        "CREATE VIEW gs AS SELECT a AS ka, COUNT(*) AS n, SUM(v) AS s FROM ts GROUP BY a",
        "CREATE VIEW gf AS SELECT a AS ka, b AS kb, c AS kc, COUNT(*) AS n FROM tf GROUP BY a, b, c",
    ] {
        exec(client, sn, ddl);
    }
}

/// Every view against a recompute over `t` (the table the test later deletes
/// from) and the fixed contents of the others.
fn assert_views(client: &mut GnitzClient, sn: &str, t: &[Vec<i64>], when: &str) {
    let check = |client: &mut GnitzClient, view: &str, cols: &[&str], want: Vec<Vec<i64>>| {
        assert_eq!(view_rows(client, sn, view, cols), want, "{view} ({when})");
    };
    let ab = at_weight_one(&project(t, &[0, 1]));
    check(client, "mv", &["a", "b", "v"], at_weight_one(t));
    check(client, "mv_pk", &["v"], at_weight_one(&project(t, &[2])));
    check(client, "mv2", &["a", "b"], ab.clone());
    check(client, "mvd", &["a", "b"], ab.clone());
    check(client, "mvc", &["a", "b"], ab);
    check(client, "g_pre", &["ka", "n", "s"], grouped(t, &[0], 2));
    check(client, "g_other", &["kb", "n", "s"], grouped(t, &[1], 2));
    check(
        client,
        "g_full",
        &["ka", "kb", "n"],
        project(&grouped(t, &[0, 1], 2), &[0, 1, 2, 4]),
    );
    let filtered: Vec<Vec<i64>> = t.iter().filter(|r| r[1] >= 3).cloned().collect();
    check(client, "gf_pre", &["ka", "n", "s"], grouped(&filtered, &[0], 2));
    check(client, "gf_other", &["kb", "n", "s"], grouped(&filtered, &[1], 2));
    check(client, "gv_a", &["ka", "n", "s"], grouped(t, &[0], 2));
    check(client, "gv_b", &["kb", "n", "s"], grouped(t, &[1], 2));
    let mj: Vec<Vec<i64>> = t.iter().map(|r| vec![r[0], r[1], r[0] * 7]).collect();
    check(client, "mj", &["ka", "kb", "w"], at_weight_one(&mj));
    let j_full: Vec<Vec<i64>> = t
        .iter()
        .filter(|r| r[1] % 2 == 1)
        .map(|r| vec![r[0], r[1], r[2], r[0] * 1000 + r[1]])
        .collect();
    check(client, "j_full", &["k1", "k2", "v", "vb"], at_weight_one(&j_full));
    let dj: Vec<Vec<i64>> = (1..=20i64)
        .flat_map(|a| [1i64, 3].into_iter().map(move |b| vec![a, b, a * 1000 + b, a + b]))
        .collect();
    check(client, "dj", &["k1", "k2", "vb", "vb2"], at_weight_one(&dj));
    check(client, "v3", &["a", "b", "c", "v"], at_weight_one(&t3_rows()));
    let g3 = project(&grouped(&t3_rows(), &[0, 1], 3), &[0, 1, 2, 4]);
    check(client, "g3_base", &["ka", "kb", "n"], g3.clone());
    check(client, "g3_view", &["ka", "kb", "n"], g3);
    check(client, "gs", &["ka", "n", "s"], grouped(&ts_rows(), &[0], 3));
    let gf: Vec<Vec<i64>> = ts_rows().iter().map(|r| vec![r[0] + 4, r[1], r[2], 1]).collect();
    check(client, "gf", &["ka", "kb", "kc", "n"], at_weight_one(&gf));
}

/// Both creation orders on one server — views before the data materialize tick
/// by tick, views after it are backfilled — then a retraction through every
/// shape. A row placed on one worker and addressed on another is missing here;
/// a group aggregated on two workers is doubled.
#[test]
fn views_over_prefix_clustered_tables_match_a_recompute_multiworker() {
    let (_srv, mut client, sn) = boot(4);
    let sn_backfill = format!("{sn}b");
    client.create_schema(&sn_backfill).unwrap();
    for (sn, when) in [(&sn, "incremental"), (&sn_backfill, "backfill")] {
        create_tables(&mut client, sn);
        if when == "incremental" {
            create_views(&mut client, sn);
            insert_all(&mut client, sn);
        } else {
            insert_all(&mut client, sn);
            create_views(&mut client, sn);
        }
        assert_views(&mut client, sn, &t_rows(), when);

        assert_eq!(affected(&mut client, sn, "DELETE FROM t WHERE b >= 4"), 40, "{when}");
        let survivors: Vec<Vec<i64>> = t_rows().into_iter().filter(|r| r[1] < 4).collect();
        assert_eq!(
            view_rows(&mut client, sn, "t", &["a", "b", "v"]),
            at_weight_one(&survivors)
        );
        assert_views(&mut client, sn, &survivors, &format!("{when}, after delete"));
    }
}

/// A keyed read of a prefix-placed view seeks the one worker its key names, so
/// it answers only if the row was placed there too; and two rows sharing the
/// prefix but not the PK suffix are found by an UPSERT and a DELETE on that
/// prefix — a probe on the wrong slice leaves a duplicate PK or a ghost.
#[test]
fn keyed_reads_and_prefix_twins_multiworker() {
    let (_srv, mut client, sn) = boot(4);
    exec(
        &mut client,
        &sn,
        "CREATE TABLE t (a BIGINT UNSIGNED, b BIGINT UNSIGNED, v BIGINT NOT NULL, PRIMARY KEY (a, b)) CLUSTER BY a",
    );
    exec(&mut client, &sn, "CREATE VIEW mv AS SELECT a, b, v FROM t WHERE v > 0");
    insert_rows(&mut client, &sn, "t", &["a", "b", "v"], &t_rows());
    for (a, b) in [(1i64, 1i64), (7, 3), (13, 5), (20, 2)] {
        assert_eq!(
            rows(
                &mut client,
                &sn,
                &format!("SELECT * FROM mv WHERE a = {a} AND b = {b}"),
                &["a", "b", "v"]
            ),
            vec![vec![a, b, a * 100 + b, 1]],
            "point read of ({a}, {b})",
        );
    }
    let group7: Vec<Vec<i64>> = t_rows().into_iter().filter(|r| r[0] == 7).collect();
    assert_eq!(
        rows(&mut client, &sn, "SELECT * FROM mv WHERE a = 7", &["a", "b", "v"]),
        at_weight_one(&group7),
        "a whole prefix group reads back complete",
    );

    exec(
        &mut client,
        &sn,
        "CREATE TABLE u (k1 BIGINT UNSIGNED, k2 BIGINT UNSIGNED, v BIGINT NOT NULL, PRIMARY KEY (k1, k2)) CLUSTER BY k1",
    );
    exec(
        &mut client,
        &sn,
        "INSERT INTO u (k1, k2, v) VALUES (5, 1, 100), (5, 2, 200), (5, 3, 300), (8, 1, 1)",
    );
    exec(
        &mut client,
        &sn,
        "INSERT INTO u (k1, k2, v) VALUES (5, 1, 999) ON CONFLICT DO UPDATE SET v = EXCLUDED.v",
    );
    assert_eq!(affected(&mut client, &sn, "DELETE FROM u WHERE k1 = 5 AND k2 = 2"), 1);
    assert_eq!(
        view_rows(&mut client, &sn, "u", &["k1", "k2", "v"]),
        at_weight_one(&[vec![5, 1, 999], vec![5, 3, 300], vec![8, 1, 1]]),
        "the UPSERT replaced its twin and the DELETE removed the other",
    );
}

// ── Placement is transitive down a view chain ────────────────────────────────

/// `d` replicated, `f` and `p` partitioned; every `f` row references a `d` row.
/// A linear view over `f ⋈ d` produces its rows where the join did, not where
/// their key hashes, and so does every view over it: rows addressed by key over
/// such a source are silently missing.
#[test]
fn placement_is_transitive_down_a_view_chain_multiworker() {
    let (_srv, mut client, sn) = boot(4);
    let dim_of = |id: i64| (id - 1) % 10 + 1;
    for ddl in [
        "CREATE TABLE d (id BIGINT UNSIGNED PRIMARY KEY, nm BIGINT NOT NULL) WITH (replicated = true)",
        "CREATE TABLE f (id BIGINT UNSIGNED PRIMARY KEY, r BIGINT UNSIGNED NOT NULL)",
        "CREATE TABLE p (id BIGINT UNSIGNED PRIMARY KEY, q BIGINT NOT NULL)",
        "CREATE VIEW jm AS SELECT f.id AS fid, d.nm AS nm FROM f JOIN d ON f.r = d.id",
        "CREATE VIEW dv AS SELECT fid, nm FROM jm WHERE nm > 0",
        "CREATE VIEW dv2 AS SELECT fid FROM dv",
        "CREATE VIEW gj AS SELECT nm AS knm, COUNT(*) AS n FROM jm GROUP BY nm",
        "CREATE VIEW rv AS SELECT id, nm FROM d WHERE nm > 0",
        "CREATE VIEW ua AS SELECT id FROM d UNION ALL SELECT id FROM f",
        "CREATE VIEW uav AS SELECT id FROM ua WHERE id > 0",
        "CREATE VIEW pj AS SELECT f.id AS fid, p.q AS q FROM f JOIN p ON f.id = p.id",
        "CREATE VIEW pjv AS SELECT fid, q FROM pj WHERE q > 0",
    ] {
        exec(&mut client, &sn, ddl);
    }
    let d: Vec<Vec<i64>> = (1..=10i64).map(|id| vec![id, id * 10]).collect();
    insert_rows(&mut client, &sn, "d", &["id", "nm"], &d);
    let f: Vec<Vec<i64>> = (1..=100i64).map(|id| vec![id, dim_of(id)]).collect();
    insert_rows(&mut client, &sn, "f", &["id", "r"], &f);
    let p: Vec<Vec<i64>> = (1..=100i64).map(|id| vec![id, id * 3]).collect();
    insert_rows(&mut client, &sn, "p", &["id", "q"], &p);

    let joined: Vec<Vec<i64>> = (1..=100i64).map(|id| vec![id, dim_of(id) * 10]).collect();
    assert_eq!(
        view_rows(&mut client, &sn, "jm", &["fid", "nm"]),
        at_weight_one(&joined)
    );
    assert_eq!(
        view_rows(&mut client, &sn, "dv", &["fid", "nm"]),
        at_weight_one(&joined)
    );
    let fids: Vec<Vec<i64>> = (1..=100i64).map(|id| vec![id]).collect();
    assert_eq!(view_rows(&mut client, &sn, "dv2", &["fid"]), at_weight_one(&fids));
    let gj: Vec<Vec<i64>> = (1..=10i64).map(|k| vec![k * 10, 10]).collect();
    assert_eq!(view_rows(&mut client, &sn, "gj", &["knm", "n"]), at_weight_one(&gj));
    for id in [1i64, 37, 100] {
        assert_eq!(
            rows(
                &mut client,
                &sn,
                &format!("SELECT * FROM dv WHERE fid = {id}"),
                &["fid", "nm"]
            ),
            vec![vec![id, dim_of(id) * 10, 1]],
            "keyed read of the hop over the replicated join gathers",
        );
    }

    // The shapes around it: a fully replicated view reads back one copy; a bag
    // union of a replicated and a partitioned side keeps both branches through a
    // linear hop; a join of two partitioned sides and its hop stay key-placed.
    assert_eq!(view_rows(&mut client, &sn, "rv", &["id", "nm"]), at_weight_one(&d));
    let bag: Vec<Vec<i64>> = fids.iter().cloned().chain((1..=10i64).map(|id| vec![id])).collect();
    assert_eq!(view_rows(&mut client, &sn, "ua", &["id"]), at_weight_one(&bag));
    assert_eq!(view_rows(&mut client, &sn, "uav", &["id"]), at_weight_one(&bag));
    assert_eq!(view_rows(&mut client, &sn, "pj", &["fid", "q"]), at_weight_one(&p));
    assert_eq!(view_rows(&mut client, &sn, "pjv", &["fid", "q"]), at_weight_one(&p));
    for id in [1i64, 37, 100] {
        assert_eq!(
            rows(
                &mut client,
                &sn,
                &format!("SELECT * FROM pjv WHERE fid = {id}"),
                &["fid", "q"]
            ),
            vec![vec![id, id * 3, 1]],
            "keyed read of the key-placed hop seeks its owner",
        );
    }
}

// ── Predicate content, which no circuit shape pins ───────────────────────────

/// A flipped operator or wrong constant admits the `ind = 6` row; a flipped
/// residual picks the other `b` row.
#[test]
fn where_and_residual_weights_multiworker() {
    let (_srv, mut client, sn) = boot(4);
    for ddl in [
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, ind BIGINT NOT NULL, other BIGINT NOT NULL)",
        "CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, v BIGINT NOT NULL)",
        "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, w BIGINT NOT NULL)",
        "CREATE VIEW vw AS SELECT other FROM t WHERE ind = 5",
        "CREATE VIEW vr AS SELECT a.id AS aid, b.id AS bid FROM a JOIN b ON a.k = b.k AND a.v > b.w",
        "INSERT INTO t VALUES (1, 5, 100), (2, 6, 200), (3, 5, 300)",
        "INSERT INTO a VALUES (1, 10, 100)",
        "INSERT INTO b VALUES (1, 10, 50), (2, 10, 200)",
    ] {
        exec(&mut client, &sn, ddl);
    }
    assert_eq!(
        view_rows(&mut client, &sn, "vw", &["other"]),
        vec![vec![100, 1], vec![300, 1]]
    );
    assert_eq!(view_rows(&mut client, &sn, "vr", &["aid", "bid"]), vec![vec![1, 1, 1]]);
    exec(&mut client, &sn, "DELETE FROM t WHERE id = 3");
    exec(&mut client, &sn, "DELETE FROM b WHERE id = 1");
    assert_eq!(view_rows(&mut client, &sn, "vw", &["other"]), vec![vec![100, 1]]);
    assert_eq!(
        view_rows(&mut client, &sn, "vr", &["aid", "bid"]),
        Vec::<Vec<i64>>::new()
    );
}

// ── Self-joins ───────────────────────────────────────────────────────────────

/// The same table on both sides of a join, once and twice over: a pair counted
/// on both sides reads as weight 2.
#[test]
fn self_join_weights_multiworker() {
    let (_srv, mut client, sn) = boot(4);
    for ddl in [
        "CREATE TABLE emp (id BIGINT NOT NULL PRIMARY KEY, mgr BIGINT NOT NULL, nm BIGINT NOT NULL)",
        "CREATE VIEW v2 AS SELECT e.nm AS emp, m.nm AS boss FROM emp e JOIN emp m ON e.mgr = m.id",
        "CREATE VIEW v3 AS SELECT e.nm AS emp, m.nm AS boss, g.nm AS grandboss \
         FROM emp e JOIN emp m ON e.mgr = m.id JOIN emp g ON m.mgr = g.id",
        "INSERT INTO emp VALUES (1, 0, 100), (2, 1, 200), (3, 2, 300)",
    ] {
        exec(&mut client, &sn, ddl);
    }
    let v2 = |client: &mut GnitzClient| view_rows(client, &sn, "v2", &["emp", "boss"]);
    let v3 = |client: &mut GnitzClient| view_rows(client, &sn, "v3", &["emp", "boss", "grandboss"]);
    assert_eq!(v2(&mut client), vec![vec![200, 100, 1], vec![300, 200, 1]]);
    assert_eq!(v3(&mut client), vec![vec![300, 200, 100, 1]]);

    exec(&mut client, &sn, "INSERT INTO emp VALUES (4, 2, 400)");
    assert_eq!(
        v2(&mut client),
        vec![vec![200, 100, 1], vec![300, 200, 1], vec![400, 200, 1]]
    );
    assert_eq!(v3(&mut client), vec![vec![300, 200, 100, 1], vec![400, 200, 100, 1]]);

    // Every chain reaches employee 1; retracting it empties the 3-level view.
    exec(&mut client, &sn, "DELETE FROM emp WHERE id = 1");
    assert_eq!(v2(&mut client), vec![vec![300, 200, 1], vec![400, 200, 1]]);
    assert_eq!(v3(&mut client), Vec::<Vec<i64>>::new());
}

// ── Replicated sources ───────────────────────────────────────────────────────

/// Every worker holds a replicated relation in full. A view over only replicated
/// sources is replicated too, so each downstream exchange must not relay one
/// copy per worker: `g = 7` reads 120 instead of 30 when it does. The tick path,
/// the backfill path (views created after the data) and a mixed-source control
/// share one server.
#[test]
fn replicated_chain_weights_multiworker() {
    let (_srv, mut client, sn) = boot(4);
    for ddl in [
        "CREATE TABLE rt (id BIGINT NOT NULL PRIMARY KEY, g BIGINT NOT NULL, x BIGINT NOT NULL) \
         WITH (replicated = true)",
        "CREATE TABLE ru (id BIGINT NOT NULL PRIMARY KEY, g BIGINT NOT NULL, x BIGINT NOT NULL) \
         WITH (replicated = true)",
        "CREATE TABLE fact (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL)",
        "CREATE VIEW vbase AS SELECT g, SUM(x) AS s FROM rt GROUP BY g",
        "CREATE VIEW rv AS SELECT id, g, x FROM rt",
        "CREATE VIEW vagg AS SELECT g, SUM(x) AS s FROM rv GROUP BY g",
        "CREATE VIEW vsum AS SELECT SUM(x) AS s FROM rv",
        "CREATE VIEW vunion AS SELECT id, x FROM rv UNION ALL SELECT id, x FROM ru",
        // Joined on a non-PK column of `rv`, so the join cannot be co-partitioned.
        "CREATE VIEW vjoin AS SELECT fact.id AS fid, rv.x AS x FROM fact JOIN rv ON fact.k = rv.g",
        "CREATE VIEW v3 AS SELECT g, s FROM vagg",
        "CREATE VIEW vmixagg AS SELECT fid, SUM(x) AS s FROM vjoin GROUP BY fid",
        "CREATE VIEW vdist AS SELECT DISTINCT g FROM rv",
        "CREATE VIEW vlin AS SELECT id, x FROM rv WHERE x > 0",
        "CREATE VIEW vcv AS SELECT COUNT(*) AS c FROM rv WHERE x > 1000",
        "CREATE VIEW vcb AS SELECT COUNT(*) AS c FROM rt WHERE x > 1000",
        "INSERT INTO rt VALUES (1, 7, 10), (2, 7, 20), (3, 8, 5)",
        "INSERT INTO ru VALUES (1, 7, 10), (2, 7, 20), (3, 8, 5)",
        "INSERT INTO fact VALUES (1, 7), (2, 8)",
        "CREATE VIEW rv_b AS SELECT id, g, x FROM rt",
        "CREATE VIEW vagg_b AS SELECT g, SUM(x) AS s FROM rv_b GROUP BY g",
    ] {
        exec(&mut client, &sn, ddl);
    }
    let gs = vec![vec![7, 30, 1], vec![8, 5, 1]];
    for view in ["vbase", "vagg", "v3", "vagg_b"] {
        assert_eq!(view_rows(&mut client, &sn, view, &["g", "s"]), gs, "{view}");
    }
    assert_eq!(view_rows(&mut client, &sn, "vsum", &["s"]), vec![vec![35, 1]]);
    // A bag union keeps the two sides as separate rows under a per-branch key.
    let side = [vec![1, 10], vec![2, 20], vec![3, 5]];
    let both_sides: Vec<Vec<i64>> = side.iter().chain(&side).cloned().collect();
    assert_eq!(
        view_rows(&mut client, &sn, "vunion", &["id", "x"]),
        at_weight_one(&both_sides),
        "one row per side, not one per worker per side",
    );
    assert_eq!(
        view_rows(&mut client, &sn, "vjoin", &["fid", "x"]),
        vec![vec![1, 10, 1], vec![1, 20, 1], vec![2, 5, 1]]
    );
    assert_eq!(
        view_rows(&mut client, &sn, "vmixagg", &["fid", "s"]),
        vec![vec![1, 30, 1], vec![2, 5, 1]],
        "a mixed-source view is not replicated",
    );
    assert_eq!(
        view_rows(&mut client, &sn, "vdist", &["g"]),
        vec![vec![7, 1], vec![8, 1]]
    );
    assert_eq!(
        view_rows(&mut client, &sn, "vlin", &["id", "x"]),
        vec![vec![1, 10, 1], vec![2, 20, 1], vec![3, 5, 1]]
    );
    // The empty global aggregate: exactly one ground row, on the worker the read goes to.
    assert_eq!(view_rows(&mut client, &sn, "vcv", &["c"]), vec![vec![0, 1]]);
    assert_eq!(view_rows(&mut client, &sn, "vcb", &["c"]), vec![vec![0, 1]]);

    exec(&mut client, &sn, "DELETE FROM rt WHERE id = 2");
    let gs = vec![vec![7, 10, 1], vec![8, 5, 1]];
    for view in ["vbase", "vagg", "v3", "vagg_b"] {
        assert_eq!(
            view_rows(&mut client, &sn, view, &["g", "s"]),
            gs,
            "{view} after the retraction"
        );
    }
}

// ── Chains with a cut segment ────────────────────────────────────────────────

/// `a3 ⋈ b3` is materialized as a hidden segment that the third step reads. A
/// segment carrying the wrong columns or weights doubles or drops a chain row;
/// a LEFT step over it null-fills at the segment's weight.
#[test]
fn three_way_chain_weights_multiworker() {
    let (_srv, mut client, sn) = boot(4);
    for ddl in [
        "CREATE TABLE a3 (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, av BIGINT NOT NULL)",
        "CREATE TABLE b3 (id BIGINT NOT NULL PRIMARY KEY, bk BIGINT NOT NULL, bv BIGINT NOT NULL)",
        "CREATE TABLE c3 (id BIGINT NOT NULL PRIMARY KEY, cv BIGINT NOT NULL)",
        "CREATE VIEW vi AS SELECT a3.av AS av, b3.bv AS bv, c3.cv AS cv \
         FROM a3 JOIN b3 ON a3.k = b3.bk JOIN c3 ON a3.id = c3.id",
        "CREATE VIEW vl AS SELECT a3.av AS av, b3.bv AS bv \
         FROM a3 JOIN b3 ON a3.k = b3.bk LEFT JOIN c3 ON a3.id = c3.id",
        "INSERT INTO a3 VALUES (1, 10, 100), (2, 10, 101)",
        "INSERT INTO b3 VALUES (1, 10, 200)",
        "INSERT INTO c3 VALUES (1, 300)",
    ] {
        exec(&mut client, &sn, ddl);
    }
    let vi = |client: &mut GnitzClient| view_rows(client, &sn, "vi", &["av", "bv", "cv"]);
    let vl = |client: &mut GnitzClient| view_rows(client, &sn, "vl", &["av", "bv"]);
    assert_eq!(vi(&mut client), vec![vec![100, 200, 300, 1]]);
    assert_eq!(
        vl(&mut client),
        vec![vec![100, 200, 1], vec![101, 200, 1]],
        "a3(2) null-fills once"
    );

    exec(&mut client, &sn, "DELETE FROM a3 WHERE id = 2");
    assert_eq!(vi(&mut client), vec![vec![100, 200, 300, 1]]);
    assert_eq!(vl(&mut client), vec![vec![100, 200, 1]]);

    exec(&mut client, &sn, "DELETE FROM b3 WHERE id = 1");
    assert_eq!(vi(&mut client), Vec::<Vec<i64>>::new());
    assert_eq!(
        vl(&mut client),
        Vec::<Vec<i64>>::new(),
        "the segment emptied; the matched and the null-filled row go with it"
    );
}
