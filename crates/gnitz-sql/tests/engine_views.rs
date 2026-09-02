#![cfg(feature = "integration")]

//! Maintained-view data contracts that only a running engine can show, each
//! read back with its Z-set weights across four workers.

use gnitz_core::{Schema, ZSetBatch};

mod common;
use common::*;

/// The row of `batch` whose integer column `col` equals `value`.
fn row_where(schema: &Schema, batch: &ZSetBatch, col: &str, value: i64) -> usize {
    let ci = col_idx(schema, col);
    (0..batch.len())
        .find(|&r| cell_i64(schema, batch, ci, r) == value)
        .unwrap_or_else(|| panic!("no row with {col} = {value}"))
}

// ── Set operations ───────────────────────────────────────────────────────────

/// EXCEPT and INTERSECT match on the whole row, not on the source PK.
#[test]
fn set_ops_use_full_row_identity() {
    let (_srv, mut client, sn) = boot(4);
    exec(
        &mut client,
        &sn,
        "CREATE TABLE a (id BIGINT PRIMARY KEY, val BIGINT NOT NULL)",
    );
    exec(
        &mut client,
        &sn,
        "CREATE TABLE b (id BIGINT PRIMARY KEY, val BIGINT NOT NULL)",
    );
    exec(
        &mut client,
        &sn,
        "CREATE VIEW ve AS SELECT * FROM a EXCEPT SELECT * FROM b",
    );
    exec(
        &mut client,
        &sn,
        "CREATE VIEW vi AS SELECT * FROM a INTERSECT SELECT * FROM b",
    );
    exec(&mut client, &sn, "INSERT INTO a VALUES (1, 100), (2, 200)");
    exec(&mut client, &sn, "INSERT INTO b VALUES (1, 200), (2, 200)");

    let cols = ["id", "val"];
    assert_eq!(view_rows(&mut client, &sn, "ve", &cols), vec![vec![1, 100, 1]]);
    assert_eq!(view_rows(&mut client, &sn, "vi", &cols), vec![vec![2, 200, 1]]);

    exec(&mut client, &sn, "DELETE FROM b WHERE id = 2");
    assert_eq!(
        view_rows(&mut client, &sn, "ve", &cols),
        vec![vec![1, 100, 1], vec![2, 200, 1]]
    );
    assert!(view_rows(&mut client, &sn, "vi", &cols).is_empty());
}

/// Each side of EXCEPT / INTERSECT is a set: two source rows projecting to one
/// value weigh 1, and a second covering row on the other side changes nothing.
#[test]
fn set_op_sides_are_distinct() {
    let (_srv, mut client, sn) = boot(4);
    exec(
        &mut client,
        &sn,
        "CREATE TABLE a (id BIGINT PRIMARY KEY, c BIGINT NOT NULL)",
    );
    exec(
        &mut client,
        &sn,
        "CREATE TABLE b (id BIGINT PRIMARY KEY, c BIGINT NOT NULL)",
    );
    exec(
        &mut client,
        &sn,
        "CREATE VIEW ve AS SELECT c FROM a EXCEPT SELECT c FROM b",
    );
    exec(
        &mut client,
        &sn,
        "CREATE VIEW vi AS SELECT c FROM a INTERSECT SELECT c FROM b",
    );

    exec(&mut client, &sn, "INSERT INTO a VALUES (1, 5), (2, 5)");
    assert_eq!(view_rows(&mut client, &sn, "ve", &["c"]), vec![vec![5, 1]]);
    assert!(view_rows(&mut client, &sn, "vi", &["c"]).is_empty());

    exec(&mut client, &sn, "INSERT INTO b VALUES (1, 5)");
    assert!(view_rows(&mut client, &sn, "ve", &["c"]).is_empty());
    assert_eq!(view_rows(&mut client, &sn, "vi", &["c"]), vec![vec![5, 1]]);

    exec(&mut client, &sn, "INSERT INTO b VALUES (2, 5)");
    assert!(view_rows(&mut client, &sn, "ve", &["c"]).is_empty());
    assert_eq!(view_rows(&mut client, &sn, "vi", &["c"]), vec![vec![5, 1]]);
}

// ── Aggregates ───────────────────────────────────────────────────────────────

/// A group whose every value is NULL still exists: SUM/MIN/MAX/AVG read as NULL
/// while COUNT(x) is 0 and COUNT(*) counts the rows — from inception and after a
/// delete empties the group of values. AVG ignores NULLs and is never NaN.
#[test]
fn aggregate_values_and_nulls() {
    let (_srv, mut client, sn) = boot(4);
    exec(
        &mut client,
        &sn,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, g BIGINT NOT NULL, x BIGINT, y BIGINT NOT NULL, fx DOUBLE NOT NULL)",
    );
    exec(
        &mut client,
        &sn,
        "CREATE VIEW vx AS SELECT g, SUM(x) AS sx, MIN(x) AS mnx, MAX(x) AS mxx, AVG(x) AS ax, \
         COUNT(x) AS cx, COUNT(*) AS ca, SUM(fx) AS sfx, AVG(fx) AS afx FROM t GROUP BY g",
    );
    exec(
        &mut client,
        &sn,
        "CREATE VIEW vg AS SELECT SUM(y) AS sy, MIN(y) AS mny, MAX(y) AS mxy, COUNT(*) AS ca FROM t",
    );
    exec(
        &mut client,
        &sn,
        "INSERT INTO t (id, g, x, y, fx) VALUES \
         (1, 5, NULL, 1, 1.5), (2, 5, NULL, 2, 2.5), \
         (3, 6, NULL, 3, 3.0), (4, 6, 3, 4, 5.0), \
         (5, 7, 4, 10, 4.0)",
    );

    let check = |client: &mut gnitz_core::GnitzClient, all_null: &[i64], live: &[(i64, i64, i64, i64)]| {
        let (schema, batch) = read_sql(client, &sn, "SELECT * FROM vx");
        assert_eq!(batch.len(), all_null.len() + live.len());
        let ci = |name: &str| col_idx(&schema, name);
        for &g in all_null {
            let r = row_where(&schema, &batch, "g", g);
            for c in ["sx", "mnx", "mxx", "ax"] {
                assert!(is_null_at(&schema, &batch, ci(c), r), "{c} of the all-NULL group {g}");
            }
            assert!(!is_null_at(&schema, &batch, ci("cx"), r));
            assert_eq!(cell_i64(&schema, &batch, ci("cx"), r), 0);
        }
        for &(g, x, count, all) in live {
            let r = row_where(&schema, &batch, "g", g);
            for c in ["sx", "mnx", "mxx"] {
                assert_eq!(cell_i64(&schema, &batch, ci(c), r), x, "{c} of group {g}");
            }
            assert!(!is_null_at(&schema, &batch, ci("ax"), r));
            assert!((cell_f64(&batch, ci("ax"), r) - x as f64).abs() < 1e-9);
            assert_eq!(cell_i64(&schema, &batch, ci("cx"), r), count);
            assert_eq!(cell_i64(&schema, &batch, ci("ca"), r), all);
        }
        for r in 0..batch.len() {
            assert_eq!(batch.weights[r], 1);
            assert!(cell_f64(&batch, ci("afx"), r).is_finite());
        }
    };

    check(&mut client, &[5], &[(6, 3, 1, 2), (7, 4, 1, 1)]);
    let (schema, batch) = read_sql(&mut client, &sn, "SELECT * FROM vx");
    let r5 = row_where(&schema, &batch, "g", 5);
    assert_eq!(cell_i64(&schema, &batch, col_idx(&schema, "ca"), r5), 2);
    assert!((cell_f64(&batch, col_idx(&schema, "sfx"), r5) - 4.0).abs() < 1e-9);
    assert!((cell_f64(&batch, col_idx(&schema, "afx"), r5) - 2.0).abs() < 1e-9);
    assert_eq!(
        view_rows(&mut client, &sn, "vg", &["sy", "mny", "mxy", "ca"]),
        vec![vec![20, 1, 10, 5, 1]]
    );

    // Group 6 loses its only non-NULL x but keeps a row.
    exec(&mut client, &sn, "DELETE FROM t WHERE id = 4");
    check(&mut client, &[5, 6], &[(7, 4, 1, 1)]);
    assert_eq!(
        view_rows(&mut client, &sn, "vg", &["sy", "mny", "mxy", "ca"]),
        vec![vec![16, 1, 10, 4, 1]]
    );
}

// ── Compound-PK sources ──────────────────────────────────────────────────────

/// A projection over a three-column PK keeps rows that differ only in the
/// trailing key column apart, through both the pure-map arm (a column subset)
/// and the expression arm (duplicate-PK copies and a computed column), across
/// INSERT, UPDATE and DELETE.
#[test]
fn compound_pk_projection_incremental() {
    let (_srv, mut client, sn) = boot(4);
    exec(
        &mut client,
        &sn,
        "CREATE TABLE t (a BIGINT, b BIGINT, c BIGINT, val BIGINT NOT NULL, extra BIGINT NOT NULL, \
         PRIMARY KEY (a, b, c))",
    );
    exec(&mut client, &sn, "CREATE VIEW v_map AS SELECT a, b, c, val FROM t");
    exec(
        &mut client,
        &sn,
        "CREATE VIEW v_expr AS SELECT a, b, c, val, a AS a2, c AS c2, val + 1 AS vp FROM t",
    );
    exec(
        &mut client,
        &sn,
        "INSERT INTO t (a, b, c, val, extra) VALUES (1, 1, 1, 100, 9), (1, 1, 2, 200, 8), (1, 2, 1, 300, 7)",
    );

    let map_cols = ["a", "b", "c", "val"];
    let expr_cols = ["a", "b", "c", "val", "a2", "c2", "vp"];
    let check = |client: &mut gnitz_core::GnitzClient, rows: &[(i64, i64, i64, i64)]| {
        let map: Vec<Vec<i64>> = rows.iter().map(|&(a, b, c, v)| vec![a, b, c, v]).collect();
        let expr: Vec<Vec<i64>> = rows.iter().map(|&(a, b, c, v)| vec![a, b, c, v, a, c, v + 1]).collect();
        assert_eq!(view_rows(client, &sn, "v_map", &map_cols), at_weight_one(&map));
        assert_eq!(view_rows(client, &sn, "v_expr", &expr_cols), at_weight_one(&expr));
    };

    check(&mut client, &[(1, 1, 1, 100), (1, 1, 2, 200), (1, 2, 1, 300)]);
    exec(
        &mut client,
        &sn,
        "UPDATE t SET val = 250 WHERE a = 1 AND b = 1 AND c = 2",
    );
    check(&mut client, &[(1, 1, 1, 100), (1, 1, 2, 250), (1, 2, 1, 300)]);
    exec(&mut client, &sn, "DELETE FROM t WHERE a = 1 AND b = 1 AND c = 1");
    check(&mut client, &[(1, 1, 2, 250), (1, 2, 1, 300)]);
}

/// GROUP BY one component of a compound PK aggregates across workers; the full
/// PK and its permutation both keep every singleton group.
#[test]
fn compound_pk_group_by_multiworker() {
    let (_srv, mut client, sn) = boot(4);
    exec(
        &mut client,
        &sn,
        "CREATE TABLE t (a BIGINT UNSIGNED, b BIGINT UNSIGNED, v BIGINT NOT NULL, PRIMARY KEY (a, b))",
    );
    exec(
        &mut client,
        &sn,
        "CREATE VIEW g_part AS SELECT a AS ka, COUNT(*) AS n, SUM(v) AS s FROM t GROUP BY a",
    );
    exec(
        &mut client,
        &sn,
        "CREATE VIEW g_full AS SELECT a AS ka, b AS kb, COUNT(*) AS n FROM t GROUP BY a, b",
    );
    exec(
        &mut client,
        &sn,
        "CREATE VIEW g_perm AS SELECT a AS ka, b AS kb, COUNT(*) AS n FROM t GROUP BY b, a",
    );
    exec(
        &mut client,
        &sn,
        "INSERT INTO t (a, b, v) VALUES (1, 1, 10), (1, 2, 20), (1, 3, 30), (2, 1, 40), (2, 2, 50), (3, 1, 60)",
    );

    assert_eq!(
        view_rows(&mut client, &sn, "g_part", &["ka", "n", "s"]),
        at_weight_one(&[vec![1, 3, 60], vec![2, 2, 90], vec![3, 1, 60]])
    );
    let singletons = at_weight_one(&[
        vec![1, 1, 1],
        vec![1, 2, 1],
        vec![1, 3, 1],
        vec![2, 1, 1],
        vec![2, 2, 1],
        vec![3, 1, 1],
    ]);
    assert_eq!(view_rows(&mut client, &sn, "g_full", &["ka", "kb", "n"]), singletons);
    assert_eq!(view_rows(&mut client, &sn, "g_perm", &["ka", "kb", "n"]), singletons);
}

// ── Reduce maps ──────────────────────────────────────────────────────────────

/// A grouped view may compute on the way into the reduce (a computed key or
/// aggregate argument) and on the way out (arithmetic over aggregates); a SELECT
/// item names a group key by what it binds to; `GROUP BY <n>` is the n-th
/// SELECT item; HAVING binds its own aggregate, not the first one in the list.
#[test]
fn reduce_maps_and_group_key_resolution() {
    let (_srv, mut client, sn) = boot(4);
    exec(
        &mut client,
        &sn,
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, a BIGINT NOT NULL, b BIGINT NOT NULL)",
    );
    for tbl in ["l", "r"] {
        exec(
            &mut client,
            &sn,
            &format!("CREATE TABLE {tbl} (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, v BIGINT NOT NULL)"),
        );
    }
    let j = "FROM l JOIN r ON l.k = r.k";
    for (name, body) in [
        ("pre_arg", "SELECT k, SUM(a * b) AS s FROM t GROUP BY k".to_string()),
        (
            "pre_key",
            "SELECT a + b AS ab, COUNT(*) AS c FROM t GROUP BY a + b".to_string(),
        ),
        ("post", "SELECT k, SUM(a) + 1 AS s FROM t GROUP BY k".to_string()),
        (
            "qualified",
            "SELECT a + b AS ab, COUNT(*) AS c FROM t GROUP BY t.a + b".to_string(),
        ),
        (
            "parens",
            "SELECT (a + b) AS ab, SUM((a * b)) AS s FROM t GROUP BY a + b".to_string(),
        ),
        (
            "reused",
            "SELECT (a + b) * 2 AS x, SUM(a + b) AS s FROM t GROUP BY a + b".to_string(),
        ),
        (
            "overlapping",
            "SELECT a + b AS s, (a + b) * 2 AS d, COUNT(*) AS c FROM t GROUP BY a + b, (a + b) * 2".to_string(),
        ),
        (
            "null_test",
            "SELECT a + b AS ab, COUNT(*) AS c FROM t GROUP BY a + b HAVING (a + b) IS NOT NULL".to_string(),
        ),
        (
            "having",
            "SELECT k, SUM(a) AS s1, MAX(b) AS m2 FROM t GROUP BY k HAVING MAX(b) = 3".to_string(),
        ),
        ("p_col", "SELECT k, COUNT(*) AS c FROM t GROUP BY 1".to_string()),
        (
            "p_expr",
            "SELECT a + b AS ab, COUNT(*) AS c FROM t GROUP BY 1".to_string(),
        ),
        (
            "p_two",
            "SELECT k, a AS x, COUNT(*) AS c FROM t GROUP BY 2, 1".to_string(),
        ),
        ("j_expr", format!("SELECT l.v + 1 AS x, COUNT(*) AS c {j} GROUP BY l.v")),
        (
            "j_both",
            format!("SELECT l.v + r.v AS x, COUNT(*) AS c {j} GROUP BY l.v + r.v"),
        ),
    ] {
        exec(&mut client, &sn, &format!("CREATE VIEW {name} AS {body}"));
    }

    insert_rows(
        &mut client,
        &sn,
        "t",
        &["id", "k", "a", "b"],
        &[vec![1, 1, 2, 3], vec![2, 1, 4, 5], vec![3, 2, 2, 3]],
    );
    insert_rows(
        &mut client,
        &sn,
        "l",
        &["id", "k", "v"],
        &[vec![1, 1, 10], vec![2, 1, 20]],
    );
    insert_rows(&mut client, &sn, "r", &["id", "k", "v"], &[vec![1, 1, 100]]);

    let expect = [
        ("pre_arg", vec!["k", "s"], vec![vec![1, 26], vec![2, 6]]),
        ("pre_key", vec!["ab", "c"], vec![vec![5, 2], vec![9, 1]]),
        ("post", vec!["k", "s"], vec![vec![1, 7], vec![2, 3]]),
        ("qualified", vec!["ab", "c"], vec![vec![5, 2], vec![9, 1]]),
        ("parens", vec!["ab", "s"], vec![vec![5, 12], vec![9, 20]]),
        ("reused", vec!["x", "s"], vec![vec![10, 10], vec![18, 9]]),
        ("overlapping", vec!["s", "d", "c"], vec![vec![5, 10, 2], vec![9, 18, 1]]),
        ("null_test", vec!["ab", "c"], vec![vec![5, 2], vec![9, 1]]),
        ("having", vec!["k", "s1", "m2"], vec![vec![2, 2, 3]]),
        ("p_col", vec!["k", "c"], vec![vec![1, 2], vec![2, 1]]),
        ("p_expr", vec!["ab", "c"], vec![vec![5, 2], vec![9, 1]]),
        (
            "p_two",
            vec!["k", "x", "c"],
            vec![vec![1, 2, 1], vec![1, 4, 1], vec![2, 2, 1]],
        ),
        ("j_expr", vec!["x", "c"], vec![vec![11, 1], vec![21, 1]]),
        ("j_both", vec!["x", "c"], vec![vec![110, 1], vec![120, 1]]),
    ];
    for (view, cols, rows) in &expect {
        assert_eq!(view_rows(&mut client, &sn, view, cols), at_weight_one(rows), "{view}");
    }

    // An insert joins the existing a+b=5 group; deleting the last members of a
    // group removes it rather than leaving a ghost.
    insert_rows(&mut client, &sn, "t", &["id", "k", "a", "b"], &[vec![4, 1, 1, 4]]);
    assert_eq!(
        view_rows(&mut client, &sn, "pre_key", &["ab", "c"]),
        at_weight_one(&[vec![5, 3], vec![9, 1]])
    );
    exec(&mut client, &sn, "DELETE FROM t WHERE id = 1");
    exec(&mut client, &sn, "DELETE FROM t WHERE id = 4");
    assert_eq!(
        view_rows(&mut client, &sn, "pre_key", &["ab", "c"]),
        at_weight_one(&[vec![5, 1], vec![9, 1]])
    );
    assert_eq!(
        view_rows(&mut client, &sn, "pre_arg", &["k", "s"]),
        at_weight_one(&[vec![1, 20], vec![2, 6]])
    );
}

// ── Ad-hoc fold vs. maintained view ──────────────────────────────────────────

/// The ad-hoc GROUP BY / DISTINCT fold agrees with the maintained view on rows,
/// weights and visible columns; ORDER BY … LIMIT … OFFSET windows the fold.
#[test]
fn adhoc_fold_matches_view() {
    let (_srv, mut client, sn) = boot(4);
    exec(
        &mut client,
        &sn,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, g BIGINT NOT NULL, s TEXT, f DOUBLE)",
    );
    let gb = "SELECT g, COUNT(*) AS n FROM t GROUP BY g";
    let distinct = "SELECT DISTINCT g FROM t";
    exec(&mut client, &sn, &format!("CREATE VIEW v_gb AS {gb}"));
    exec(&mut client, &sn, &format!("CREATE VIEW v_d AS {distinct}"));
    exec(
        &mut client,
        &sn,
        "INSERT INTO t (id, g, s, f) VALUES \
         (1, 10, 'a', 1.5), (2, 10, 'b', 2.5), (3, 20, 'a', 1.5), \
         (4, 20, 'c', 3.5), (5, 30, 'b', 2.5), (6, 30, 'a', 4.5)",
    );

    for (sql, view, cols, expected) in [
        (gb, "v_gb", vec!["g", "n"], vec![vec![10, 2], vec![20, 2], vec![30, 2]]),
        (distinct, "v_d", vec!["g"], vec![vec![10], vec![20], vec![30]]),
    ] {
        assert_eq!(rows(&mut client, &sn, sql, &cols), at_weight_one(&expected), "{sql}");
        assert_eq!(
            view_rows(&mut client, &sn, view, &cols),
            at_weight_one(&expected),
            "{view}"
        );
        let (adhoc, _) = read_sql(&mut client, &sn, sql);
        let (maintained, _) = read_sql(&mut client, &sn, &format!("SELECT * FROM {view}"));
        assert_eq!(visible_names(&adhoc), visible_names(&maintained));
        assert_eq!(visible_names(&adhoc), cols);
    }

    assert_eq!(
        rows(
            &mut client,
            &sn,
            &format!("{gb} ORDER BY g DESC LIMIT 2 OFFSET 1"),
            &["g", "n"]
        ),
        at_weight_one(&[vec![10, 2], vec![20, 2]])
    );
}

// ── Joins ────────────────────────────────────────────────────────────────────

/// A LEFT JOIN whose preserved side has a compound PK null-fills its unmatched
/// rows once, with the compound key intact.
#[test]
fn left_join_over_compound_pk_preserved_side() {
    let (_srv, mut client, sn) = boot(4);
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

    assert_eq!(
        view_rows(&mut client, &sn, "v", &["k1", "k2", "av"]),
        at_weight_one(&[vec![1, 100, 11], vec![2, 200, 22]])
    );
    let (schema, batch) = read_sql(&mut client, &sn, "SELECT * FROM v");
    let bv = col_idx(&schema, "bv");
    let matched = row_where(&schema, &batch, "k1", 1);
    let unmatched = row_where(&schema, &batch, "k1", 2);
    assert!(!is_null_at(&schema, &batch, bv, matched));
    assert_eq!(cell_i64(&schema, &batch, bv, matched), 70);
    assert!(is_null_at(&schema, &batch, bv, unmatched));
}

/// A range self-join pairs a relation with itself at weight 1 per pair.
#[test]
fn range_self_join() {
    let (_srv, mut client, sn) = boot(4);
    exec(
        &mut client,
        &sn,
        "CREATE TABLE rself (id BIGINT NOT NULL PRIMARY KEY, x BIGINT NOT NULL, y BIGINT NOT NULL)",
    );
    exec(
        &mut client,
        &sn,
        "CREATE VIEW rself_v AS SELECT a.id AS ai, b.id AS bi FROM rself a JOIN rself b ON a.x < b.y",
    );
    exec(&mut client, &sn, "INSERT INTO rself VALUES (1, 1, 10), (2, 5, 2)");
    assert_eq!(
        view_rows(&mut client, &sn, "rself_v", &["ai", "bi"]),
        at_weight_one(&[vec![1, 1], vec![1, 2], vec![2, 1]])
    );
}

// ── Capacity-bounded views ───────────────────────────────────────────────────

/// A bounded filter/projection (over a table, a compound-PK table, a view) and
/// a bounded inner equi-join hold the same rows as their bodies; a rename keeps
/// the bound, so nothing can be created over the renamed view.
#[test]
fn bounded_views_hold_their_rows_and_survive_rename() {
    let (_srv, mut client, sn) = boot(4);
    exec(
        &mut client,
        &sn,
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL, s TEXT)",
    );
    exec(
        &mut client,
        &sn,
        "CREATE TABLE u (id BIGINT NOT NULL PRIMARY KEY, tid BIGINT NOT NULL, w BIGINT NOT NULL)",
    );
    exec(
        &mut client,
        &sn,
        "CREATE TABLE c (a BIGINT NOT NULL, b BIGINT NOT NULL, x BIGINT NOT NULL, PRIMARY KEY (a, b))",
    );
    exec(&mut client, &sn, "CREATE VIEW plain AS SELECT id, v FROM t");
    exec(
        &mut client,
        &sn,
        "CREATE VIEW l1 WITH (capacity = '1 MB') AS SELECT id, v, s FROM t WHERE v > 2",
    );
    exec(
        &mut client,
        &sn,
        "CREATE VIEW l2 WITH (capacity = '1 MB') AS SELECT a, b, x FROM c WHERE x < 9",
    );
    exec(
        &mut client,
        &sn,
        "CREATE VIEW l3 WITH (capacity = '1 MB') AS SELECT id, v FROM plain",
    );
    exec(
        &mut client,
        &sn,
        "CREATE VIEW j1 WITH (capacity = '1 MB') AS \
         SELECT t.id, t.s, u.w FROM t JOIN u ON t.id = u.tid AND u.w <> t.v",
    );
    exec(
        &mut client,
        &sn,
        "INSERT INTO t VALUES (1, 1, 'a'), (2, 3, 'b'), (3, 5, NULL)",
    );
    exec(
        &mut client,
        &sn,
        "INSERT INTO u VALUES (1, 1, 7), (2, 3, 3), (3, 3, 9), (4, 2, 3)",
    );
    exec(
        &mut client,
        &sn,
        "INSERT INTO c VALUES (1, 1, 5), (1, 2, 10), (2, 1, 8)",
    );

    assert_eq!(
        view_rows(&mut client, &sn, "l1", &["id", "v"]),
        at_weight_one(&[vec![2, 3], vec![3, 5]])
    );
    assert_eq!(
        view_rows(&mut client, &sn, "l2", &["a", "b", "x"]),
        at_weight_one(&[vec![1, 1, 5], vec![2, 1, 8]])
    );
    assert_eq!(
        view_rows(&mut client, &sn, "l3", &["id", "v"]),
        at_weight_one(&[vec![1, 1], vec![2, 3], vec![3, 5]])
    );
    assert_eq!(
        view_rows(&mut client, &sn, "j1", &["id", "w"]),
        at_weight_one(&[vec![1, 7], vec![3, 3], vec![3, 9]])
    );

    exec(&mut client, &sn, "ALTER TABLE l1 RENAME TO l1r");
    assert_rejects_variant(
        &mut client,
        &sn,
        "CREATE VIEW over AS SELECT id FROM l1r",
        "Unsupported",
        "capacity-bounded",
    );
}
