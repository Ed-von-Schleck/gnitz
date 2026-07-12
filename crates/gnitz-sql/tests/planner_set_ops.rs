#![cfg(feature = "integration")]

use gnitz_core::GnitzClient;
use gnitz_sql::{GnitzSqlError, SqlPlanner};
use gnitz_test_harness::ServerHandle;
use gnitz_wire::{OPCODE_JOIN_DELTA_TRACE, OPCODE_POSITIVE_PART};

mod common;
use common::*;

/// Create tables A,B (id BIGINT PK, val BIGINT) and the EXCEPT+INTERSECT views,
/// BEFORE any rows exist, so subsequent inserts arrive as separate incremental
/// epochs (the correct incremental-view test shape).
fn setup_views(client: &mut GnitzClient, sn: &str) {
    let mut p = SqlPlanner::new(client, sn);
    p.execute("CREATE TABLE a (id BIGINT PRIMARY KEY, val BIGINT NOT NULL)")
        .unwrap();
    p.execute("CREATE TABLE b (id BIGINT PRIMARY KEY, val BIGINT NOT NULL)")
        .unwrap();
    p.execute("CREATE VIEW ve AS SELECT * FROM a EXCEPT SELECT * FROM b")
        .unwrap();
    p.execute("CREATE VIEW vi AS SELECT * FROM a INTERSECT SELECT * FROM b")
        .unwrap();
}

fn insert(client: &mut GnitzClient, sn: &str, tbl: &str, id: i64, val: i64) {
    let mut p = SqlPlanner::new(client, sn);
    // Positional (no column list): callers use tables whose payload column is
    // named `val` *or* `c`, and INSERT is full-row positional anyway.
    p.execute(&format!("INSERT INTO {} VALUES ({}, {})", tbl, id, val))
        .unwrap();
}

// ── Set operations over compound-PK sources ──────────────────────────
// The synthetic `_set_pk` content hash decides membership at every source
// PK arity, so a compound-PK source needs no special handling.

#[test]
fn test_set_ops_compound_pk() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    {
        let mut p = SqlPlanner::new(&mut client, &sn);
        p.execute("CREATE TABLE t1 (a BIGINT, b BIGINT, val BIGINT NOT NULL, PRIMARY KEY (a, b))")
            .unwrap();
        p.execute("CREATE TABLE t2 (a BIGINT, b BIGINT, val BIGINT NOT NULL, PRIMARY KEY (a, b))")
            .unwrap();
        p.execute("CREATE VIEW v_union AS SELECT val FROM t1 UNION SELECT val FROM t2")
            .unwrap();
        p.execute("CREATE VIEW v_union_all AS SELECT val FROM t1 UNION ALL SELECT val FROM t2")
            .unwrap();
        p.execute("CREATE VIEW v_except AS SELECT val FROM t1 EXCEPT SELECT val FROM t2")
            .unwrap();
        p.execute("CREATE VIEW v_intersect AS SELECT val FROM t1 INTERSECT SELECT val FROM t2")
            .unwrap();

        p.execute("INSERT INTO t1 (a, b, val) VALUES (1, 1, 100), (1, 2, 200), (2, 1, 300)")
            .unwrap();
        p.execute("INSERT INTO t2 (a, b, val) VALUES (1, 1, 200), (2, 2, 400)")
            .unwrap();
    }

    // UNION (distinct): 200 appears on both sides but collapses to one row.
    let (schema, batch) = read_view(&mut client, &sn, "v_union");
    let val_idx = col_idx(&schema, "val"); // [_set_pk, val]: val at 1 for every view here
    let mut vals: Vec<i64> = (0..batch.len()).map(|r| i64_at(&batch, val_idx, r)).collect();
    vals.sort();
    assert_eq!(vals, vec![100, 200, 300, 400]);

    // UNION ALL: the branch_id discriminator keeps both copies of 200.
    let (_, batch) = read_view(&mut client, &sn, "v_union_all");
    let mut vals: Vec<i64> = (0..batch.len())
        .flat_map(|r| vec![i64_at(&batch, val_idx, r); batch.weights[r] as usize])
        .collect();
    vals.sort();
    assert_eq!(vals, vec![100, 200, 200, 300, 400]);

    // EXCEPT: t1.val \ t2.val = {100, 300}.
    let (_, batch) = read_view(&mut client, &sn, "v_except");
    let mut vals: Vec<i64> = (0..batch.len()).map(|r| i64_at(&batch, val_idx, r)).collect();
    vals.sort();
    assert_eq!(vals, vec![100, 300]);

    // INTERSECT: t1.val ∩ t2.val = {200}.
    let (_, batch) = read_view(&mut client, &sn, "v_intersect");
    let mut vals: Vec<i64> = (0..batch.len()).map(|r| i64_at(&batch, val_idx, r)).collect();
    vals.sort();
    assert_eq!(vals, vec![200]);
}

#[test]
fn test_set_ops_incrementality_compound_pk() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    {
        let mut p = SqlPlanner::new(&mut client, &sn);
        p.execute("CREATE TABLE t1 (a BIGINT, b BIGINT, val BIGINT NOT NULL, PRIMARY KEY (a, b))")
            .unwrap();
        p.execute("CREATE TABLE t2 (a BIGINT, b BIGINT, val BIGINT NOT NULL, PRIMARY KEY (a, b))")
            .unwrap();
        p.execute("CREATE VIEW v_except AS SELECT val FROM t1 EXCEPT SELECT val FROM t2")
            .unwrap();

        p.execute("INSERT INTO t1 (a, b, val) VALUES (1, 1, 100), (1, 2, 200)")
            .unwrap();
    }
    let (schema, batch) = read_view(&mut client, &sn, "v_except");
    let val_idx = col_idx(&schema, "val");
    let mut vals: Vec<i64> = (0..batch.len()).map(|r| i64_at(&batch, val_idx, r)).collect();
    vals.sort();
    assert_eq!(vals, vec![100, 200]);

    {
        let mut p = SqlPlanner::new(&mut client, &sn);
        p.execute("INSERT INTO t2 (a, b, val) VALUES (1, 1, 100)").unwrap();
    }
    let (_, batch2) = read_view(&mut client, &sn, "v_except");
    let mut vals: Vec<i64> = (0..batch2.len()).map(|r| i64_at(&batch2, val_idx, r)).collect();
    vals.sort();
    assert_eq!(vals, vec![200]);

    {
        let mut p = SqlPlanner::new(&mut client, &sn);
        p.execute("DELETE FROM t1 WHERE a=1 AND b=2").unwrap();
    }
    let (_, batch3) = read_view(&mut client, &sn, "v_except");
    assert_eq!(batch3.len(), 0);
}

#[test]
fn test_set_ops_wide_pk_regression() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    {
        let mut p = SqlPlanner::new(&mut client, &sn);
        p.execute("CREATE TABLE t1 (id DECIMAL(38, 0) PRIMARY KEY, val BIGINT NOT NULL)")
            .unwrap();
        p.execute("CREATE TABLE t2 (id DECIMAL(38, 0) PRIMARY KEY, val BIGINT NOT NULL)")
            .unwrap();
        p.execute("CREATE VIEW v AS SELECT val FROM t1 UNION SELECT val FROM t2")
            .unwrap();

        p.execute("INSERT INTO t1 (id, val) VALUES (1, 100)").unwrap();
        p.execute("INSERT INTO t2 (id, val) VALUES (1, 200)").unwrap();
    }
    let (schema, batch) = read_view(&mut client, &sn, "v");
    let val_idx = col_idx(&schema, "val");
    let mut vals: Vec<i64> = (0..batch.len()).map(|r| i64_at(&batch, val_idx, r)).collect();
    vals.sort();
    assert_eq!(vals, vec![100, 200]);
}

// ── item 36: EXCEPT/INTERSECT use full-row identity, not source PK ────

#[test]
fn test_except_full_row_identity() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    setup_views(&mut client, &sn);
    // A=(1,100), B=(1,200): same PK=1, different payload. EXCEPT keeps (1,100)
    // — it is not in B. The old source-PK reindex wrongly excluded it.
    insert(&mut client, &sn, "a", 1, 100);
    insert(&mut client, &sn, "b", 1, 200);
    let (schema, batch) = read_view(&mut client, &sn, "ve");
    assert_eq!(batch.len(), 1, "EXCEPT must keep (1,100); it is not present in B");
    assert_eq!(i64_at(&batch, col_idx(&schema, "id"), 0), 1);
    assert_eq!(i64_at(&batch, col_idx(&schema, "val"), 0), 100);
}

#[test]
fn test_intersect_full_row_identity() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    setup_views(&mut client, &sn);
    // A=(1,100), B=(1,200): no identical full row → INTERSECT empty. The old
    // source-PK reindex wrongly matched on PK=1.
    insert(&mut client, &sn, "a", 1, 100);
    insert(&mut client, &sn, "b", 1, 200);
    let (_schema, batch) = read_view(&mut client, &sn, "vi");
    assert_eq!(batch.len(), 0, "INTERSECT must be empty: no identical full row");
}

// ── sanity: identical full rows DO match under hash-row identity ─────

#[test]
fn test_set_ops_identical_rows_match() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    setup_views(&mut client, &sn);
    // A=(1,100), B=(1,100): identical full row (separate epochs).
    insert(&mut client, &sn, "a", 1, 100);
    insert(&mut client, &sn, "b", 1, 100);

    let (schema, ib) = read_view(&mut client, &sn, "vi");
    assert_eq!(ib.len(), 1, "INTERSECT of identical rows yields one row");
    assert_eq!(i64_at(&ib, col_idx(&schema, "id"), 0), 1);
    assert_eq!(i64_at(&ib, col_idx(&schema, "val"), 0), 100);

    let (_s, eb) = read_view(&mut client, &sn, "ve");
    assert_eq!(eb.len(), 0, "EXCEPT of identical rows yields nothing");
}

// ── item 37: a set-op side projects only the selected column ─────────

#[test]
fn test_union_projection_applied() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    {
        let mut p = SqlPlanner::new(&mut client, &sn);
        p.execute("CREATE TABLE a (id BIGINT PRIMARY KEY, name BIGINT NOT NULL)")
            .unwrap();
        p.execute("CREATE TABLE b (id BIGINT PRIMARY KEY, name BIGINT NOT NULL)")
            .unwrap();
        // Project only `name`; `id` must NOT leak into the view schema.
        p.execute("CREATE VIEW v AS SELECT name FROM a UNION SELECT name FROM b")
            .unwrap();
        p.execute("INSERT INTO a (id, name) VALUES (1, 100)").unwrap();
        p.execute("INSERT INTO b (id, name) VALUES (2, 100)").unwrap();
    }
    let (schema, batch) = read_view(&mut client, &sn, "v");
    // schema is [_set_pk, name] — exactly one projected payload column.
    assert!(schema.columns.iter().any(|c| c.name.eq_ignore_ascii_case("name")));
    assert!(
        !schema.columns.iter().any(|c| c.name.eq_ignore_ascii_case("id")),
        "projected-out column `id` must not appear, got {:?}",
        schema.columns.iter().map(|c| &c.name).collect::<Vec<_>>()
    );
    assert_eq!(schema.columns.len(), 2, "exactly _set_pk + name");
    // UNION distinct: both sides have name=100 → one row.
    assert_eq!(batch.len(), 1, "UNION distinct over identical projected value → 1 row");
    assert_eq!(i64_at(&batch, col_idx(&schema, "name"), 0), 100);
}

// ── item 27: EXCEPT/INTERSECT lift each side through distinct ────
//
// Two B rows with different PKs but identical projected content must change
// B's set membership only ONCE. EXCEPT DISTINCT = positive_part(da − db) with
// da = distinct(A), db = distinct(B): without the leaf distinct the second B
// insert would push db to 2, driving da − db = 1 − 2 = −1 and underflowing the
// clamp's input; distinct caps db at 1 so the difference stays at 0.

#[test]
fn test_except_distinct_lifting_no_underflow() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    {
        let mut p = SqlPlanner::new(&mut client, &sn);
        p.execute("CREATE TABLE a (id BIGINT PRIMARY KEY, name BIGINT NOT NULL)")
            .unwrap();
        p.execute("CREATE TABLE b (id BIGINT PRIMARY KEY, name BIGINT NOT NULL)")
            .unwrap();
        p.execute("CREATE VIEW v AS SELECT name FROM a EXCEPT SELECT name FROM b")
            .unwrap();
        // A has name=100. B gains two rows, both name=100 (distinct PKs).
        p.execute("INSERT INTO a (id, name) VALUES (1, 100)").unwrap();
        p.execute("INSERT INTO b (id, name) VALUES (1, 100)").unwrap();
        p.execute("INSERT INTO b (id, name) VALUES (2, 100)").unwrap();
    }
    let (_schema, batch) = read_view(&mut client, &sn, "v");
    // name=100 is in both A and B → excluded. The second identical-content B
    // insert must not push the weight to -1 (which would surface as a row).
    assert_eq!(
        batch.len(),
        0,
        "EXCEPT must stay empty; the duplicate projected B row must not underflow the weight"
    );
}

// ── same-source INTERSECT/EXCEPT rejection ──────────────────────────
//
// Both branches resolving to the SAME source relation id collapse to one
// dependency edge: a single delta drives both branch pipelines in one pass and
// the cross-correction term is dropped (silently wrong results). The guard
// rejects exactly that. (The join planner solves the same constraint by
// wrapping the repeated relation in a pass-through hidden view; set-op sides
// do not wrap yet.) The discriminator is source-id equality, NOT base-table
// overlap — two different views over the same base produce two edges and stay
// accepted (see the two-view tests below).

fn make_t_with_c(client: &mut GnitzClient, sn: &str) {
    let mut p = SqlPlanner::new(client, sn);
    p.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, c BIGINT NOT NULL)")
        .unwrap();
}

/// Create table `t(id, c)`, run any `extra_setup` statements, then assert
/// `view_sql` is rejected by the same-source guard's "same relation" error.
fn assert_same_relation_rejected(extra_setup: &[&str], view_sql: &str) {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    make_t_with_c(&mut client, &sn);
    if !extra_setup.is_empty() {
        let mut p = SqlPlanner::new(&mut client, &sn);
        for sql in extra_setup {
            p.execute(sql).unwrap();
        }
    }
    let mut p = SqlPlanner::new(&mut client, &sn);
    match p.execute(view_sql).unwrap_err() {
        GnitzSqlError::Unsupported(s) => assert!(s.contains("same relation"), "got: {}", s),
        e => panic!("expected Unsupported, got {:?}", e),
    }
}

#[test]
fn test_same_table_except_rejected() {
    assert_same_relation_rejected(
        &[],
        "CREATE VIEW v AS SELECT c FROM t WHERE c > 0 EXCEPT SELECT c FROM t WHERE c < 10",
    );
}

#[test]
fn test_same_table_intersect_rejected() {
    assert_same_relation_rejected(
        &[],
        "CREATE VIEW v AS SELECT c FROM t WHERE c > 0 INTERSECT SELECT c FROM t WHERE c < 10",
    );
}

#[test]
fn test_same_view_except_rejected() {
    // Both branches read the same view → same source id → rejected.
    assert_same_relation_rejected(
        &["CREATE VIEW vt AS SELECT id, c FROM t WHERE c > 0"],
        "CREATE VIEW v AS SELECT c FROM vt EXCEPT SELECT c FROM vt",
    );
}

#[test]
fn test_same_view_intersect_rejected() {
    assert_same_relation_rejected(
        &["CREATE VIEW vt AS SELECT id, c FROM t WHERE c > 0"],
        "CREATE VIEW v AS SELECT c FROM vt INTERSECT SELECT c FROM vt",
    );
}

// ── non-over-rejection: two DIFFERENT views over the same base, accepted ────
//
// The decisive test that the guard keys on source-id equality, not base-table
// overlap. vt1/vt2 are distinct views (distinct source ids) over the same t.

#[test]
fn test_two_views_same_base_except_accepted_and_correct() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    make_t_with_c(&mut client, &sn);
    {
        let mut p = SqlPlanner::new(&mut client, &sn);
        p.execute("CREATE VIEW vt1 AS SELECT id, c FROM t WHERE c > 0").unwrap();
        p.execute("CREATE VIEW vt2 AS SELECT id, c FROM t WHERE c > 50")
            .unwrap();
        // Must NOT raise: vt1 and vt2 have distinct source ids.
        p.execute("CREATE VIEW v AS SELECT c FROM vt1 EXCEPT SELECT c FROM vt2")
            .unwrap();
        p.execute("INSERT INTO t (id, c) VALUES (1, 10), (2, 60)").unwrap();
    }
    // vt1 = {10, 60}, vt2 = {60}; EXCEPT = {10}.
    let (schema, batch) = read_view(&mut client, &sn, "v");
    assert_eq!(batch.len(), 1, "EXCEPT of two distinct views must be {{10}}");
    assert_eq!(i64_at(&batch, col_idx(&schema, "c"), 0), 10);
}

#[test]
fn test_two_views_same_base_intersect_accepted_and_correct() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    make_t_with_c(&mut client, &sn);
    {
        let mut p = SqlPlanner::new(&mut client, &sn);
        p.execute("CREATE VIEW vt1 AS SELECT id, c FROM t WHERE c > 0").unwrap();
        p.execute("CREATE VIEW vt2 AS SELECT id, c FROM t WHERE c > 50")
            .unwrap();
        p.execute("CREATE VIEW v AS SELECT c FROM vt1 INTERSECT SELECT c FROM vt2")
            .unwrap();
        p.execute("INSERT INTO t (id, c) VALUES (1, 10), (2, 60)").unwrap();
    }
    // vt1 = {10, 60}, vt2 = {60}; INTERSECT = {60}.
    let (schema, batch) = read_view(&mut client, &sn, "v");
    assert_eq!(batch.len(), 1, "INTERSECT of two distinct views must be {{60}}");
    assert_eq!(i64_at(&batch, col_idx(&schema, "c"), 0), 60);
}

// ── same-source UNION / UNION ALL: accepted (linear, no correction term) ────

#[test]
fn test_same_table_union_distinct_accepted_and_correct() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    make_t_with_c(&mut client, &sn);
    {
        let mut p = SqlPlanner::new(&mut client, &sn);
        p.execute("CREATE VIEW v AS SELECT c FROM t WHERE c > 0 UNION SELECT c FROM t WHERE c < 100")
            .unwrap();
        p.execute("INSERT INTO t (id, c) VALUES (1, 50), (2, 150)").unwrap();
    }
    // left {50,150}, right {50}; UNION distinct = {50, 150}.
    let (schema, batch) = read_view(&mut client, &sn, "v");
    let mut vals: Vec<i64> = (0..batch.len())
        .map(|r| i64_at(&batch, col_idx(&schema, "c"), r))
        .collect();
    vals.sort();
    assert_eq!(vals, vec![50, 150]);
}

#[test]
fn test_same_table_union_all_accepted_and_correct() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    make_t_with_c(&mut client, &sn);
    {
        let mut p = SqlPlanner::new(&mut client, &sn);
        p.execute("CREATE VIEW v AS SELECT c FROM t WHERE c > 0 UNION ALL SELECT c FROM t WHERE c < 100")
            .unwrap();
        p.execute("INSERT INTO t (id, c) VALUES (1, 50), (2, 150)").unwrap();
    }
    // c=50 matches both branches → weight 2; c=150 matches only the first.
    let (schema, batch) = read_view(&mut client, &sn, "v");
    let ci = col_idx(&schema, "c");
    let mut total_50 = 0i64;
    let mut total_150 = 0i64;
    for r in 0..batch.len() {
        let w = batch.weights[r];
        match i64_at(&batch, ci, r) {
            50 => total_50 += w,
            150 => total_150 += w,
            other => panic!("unexpected value {}", other),
        }
    }
    assert_eq!(total_50, 2, "c=50 satisfies both UNION ALL branches → weight 2");
    assert_eq!(total_150, 1, "c=150 satisfies only the first branch → weight 1");
}

// ── join scope boundary (regression guard for the plan's §1) ────────────────

#[test]
fn test_transitive_self_join_accepted_and_correct() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    {
        let mut p = SqlPlanner::new(&mut client, &sn);
        p.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, k BIGINT NOT NULL, v BIGINT NOT NULL)")
            .unwrap();
        // vt is a view over t; the join names t and vt → two dependency edges.
        p.execute("CREATE VIEW vt AS SELECT id, k, v FROM t WHERE v > 0")
            .unwrap();
        p.execute("CREATE VIEW j AS SELECT t.id AS lid, vt.id AS rid FROM t JOIN vt ON t.k = vt.k")
            .unwrap();
        p.execute("INSERT INTO t (id, k, v) VALUES (1, 5, 10), (2, 5, 20)")
            .unwrap();
    }
    // Both rows have v>0 → vt has both; join on k=5 → 2×2 = 4 product rows.
    let (_schema, batch) = read_view(&mut client, &sn, "j");
    assert_eq!(batch.len(), 4, "transitive self-join must recover the full 2x2 product");
}

// ── §4: EXCEPT lifts its LEFT branch through distinct (weight-1) ─────────────
//
// Two left rows with distinct PKs projecting to the same value must contribute
// weight 1 to EXCEPT, not weight 2. `map_hash_row` consolidates them into one
// synthetic-PK tuple of weight 2; anti-joining the RAW left_node would pass that
// weight 2 through, and a later right-side cover (which retracts only weight 1)
// would leave the value surviving at weight 1. Over two distinct tables so the
// same-source guard does not reject.

#[test]
fn test_except_left_duplicate_projected_weight_one() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    {
        let mut p = SqlPlanner::new(&mut client, &sn);
        p.execute("CREATE TABLE a (id BIGINT PRIMARY KEY, c BIGINT NOT NULL)")
            .unwrap();
        p.execute("CREATE TABLE b (id BIGINT PRIMARY KEY, c BIGINT NOT NULL)")
            .unwrap();
        p.execute("CREATE VIEW v AS SELECT c FROM a EXCEPT SELECT c FROM b")
            .unwrap();
        // Two left rows projecting to c=5; right side still empty.
        p.execute("INSERT INTO a (id, c) VALUES (1, 5), (2, 5)").unwrap();
    }
    assert_eq!(
        view_value_weight(&mut client, &sn, "v", "c", 5),
        1,
        "EXCEPT must lift the left branch through distinct: c=5 carried by two source rows → weight 1, not 2"
    );

    // Right side now covers c=5 → the difference nets to 0 (a raw weight-2 left
    // would leave it surviving at weight 1).
    insert(&mut client, &sn, "b", 1, 5);
    assert_eq!(
        view_value_weight(&mut client, &sn, "v", "c", 5),
        0,
        "once the right side covers c=5 the EXCEPT must net to 0"
    );
}

#[test]
fn test_intersect_left_duplicate_projected_weight_one() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    {
        let mut p = SqlPlanner::new(&mut client, &sn);
        p.execute("CREATE TABLE a (id BIGINT PRIMARY KEY, c BIGINT NOT NULL)")
            .unwrap();
        p.execute("CREATE TABLE b (id BIGINT PRIMARY KEY, c BIGINT NOT NULL)")
            .unwrap();
        p.execute("CREATE VIEW v AS SELECT c FROM a INTERSECT SELECT c FROM b")
            .unwrap();
        // Two left rows projecting to c=5; right side still empty (cL=2, cR=0).
        p.execute("INSERT INTO a (id, c) VALUES (1, 5), (2, 5)").unwrap();
    }
    // cL=2, cR=0 → min(da, db) = min(1, 0) = 0: c=5 not in the right set.
    assert_eq!(
        view_value_weight(&mut client, &sn, "v", "c", 5),
        0,
        "INTERSECT must be empty while the right side lacks c=5"
    );

    // Right side now has one c=5 (cL=2, cR=1) → min(da, db) = min(1, 1) = 1, NOT
    // the raw left multiplicity 2: the leaf distinct clamps da to 1.
    insert(&mut client, &sn, "b", 1, 5);
    assert_eq!(
        view_value_weight(&mut client, &sn, "v", "c", 5),
        1,
        "INTERSECT must clamp the duplicated left value to weight 1, not 2"
    );
}

// ── §7: per-operator output nullability ─────────────────────────────────────
//
// `||` over both sides is sound but imprecise for INTERSECT (a tuple must be in
// BOTH sides → AND) and EXCEPT (every output is a left-side tuple → exactly the
// left's). Tightening only removes the flag when the operator's tuple algebra
// forbids NULL, so it is a strict precision gain.

/// Nullability of column `col` in the `SELECT *` schema of view `view`.
fn view_col_nullable(client: &mut GnitzClient, sn: &str, view: &str, col: &str) -> bool {
    let (schema, _batch) = read_view(client, sn, view);
    schema.columns[col_idx(&schema, col)].is_nullable
}

/// Tables `nn(id PK, v BIGINT NOT NULL)` and `nl(id PK, v BIGINT)` — same type,
/// one NOT NULL, one nullable — so each operator's bound can be observed.
fn setup_nullability_tables(client: &mut GnitzClient, sn: &str) {
    let mut p = SqlPlanner::new(client, sn);
    p.execute("CREATE TABLE nn (id BIGINT PRIMARY KEY, v BIGINT NOT NULL)")
        .unwrap();
    p.execute("CREATE TABLE nl (id BIGINT PRIMARY KEY, v BIGINT)").unwrap();
}

#[test]
fn test_intersect_nullability_is_and() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    setup_nullability_tables(&mut client, &sn);
    {
        let mut p = SqlPlanner::new(&mut client, &sn);
        // NOT NULL ∩ nullable → AND → NOT NULL.
        p.execute("CREATE VIEW v AS SELECT v FROM nn INTERSECT SELECT v FROM nl")
            .unwrap();
    }
    assert!(
        !view_col_nullable(&mut client, &sn, "v", "v"),
        "INTERSECT column is nullable only if BOTH sides are; here the left is NOT NULL"
    );
}

#[test]
fn test_except_nullability_equals_left() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    setup_nullability_tables(&mut client, &sn);
    {
        let mut p = SqlPlanner::new(&mut client, &sn);
        // left NOT NULL, right nullable → output NOT NULL (the right is ignored).
        p.execute("CREATE VIEW v_nn AS SELECT v FROM nn EXCEPT SELECT v FROM nl")
            .unwrap();
        // left nullable, right NOT NULL → output nullable (equals the left).
        p.execute("CREATE VIEW v_nl AS SELECT v FROM nl EXCEPT SELECT v FROM nn")
            .unwrap();
    }
    assert!(
        !view_col_nullable(&mut client, &sn, "v_nn", "v"),
        "EXCEPT nullability equals the LEFT input's; NOT NULL left → NOT NULL output"
    );
    assert!(
        view_col_nullable(&mut client, &sn, "v_nl", "v"),
        "EXCEPT nullability equals the LEFT input's, independent of the right; nullable left → nullable output"
    );
}

#[test]
fn test_union_nullability_is_or() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    setup_nullability_tables(&mut client, &sn);
    {
        let mut p = SqlPlanner::new(&mut client, &sn);
        // A union tuple may originate from either side → OR retained → nullable.
        p.execute("CREATE VIEW v_u AS SELECT v FROM nn UNION SELECT v FROM nl")
            .unwrap();
        p.execute("CREATE VIEW v_ua AS SELECT v FROM nn UNION ALL SELECT v FROM nl")
            .unwrap();
    }
    assert!(
        view_col_nullable(&mut client, &sn, "v_u", "v"),
        "UNION keeps the OR: a nullable side makes the output nullable"
    );
    assert!(
        view_col_nullable(&mut client, &sn, "v_ua", "v"),
        "UNION ALL keeps the OR: a nullable side makes the output nullable"
    );
}

// ── float set-op row-identity rejection (Fix A2) ─────────────────────
//
// A set operation's row identity hashes each projected column's raw IEEE-754
// bytes (reindex_hash_row), so a float column splits -0.0/+0.0 and distinct-NaN
// rows that SQL set semantics treat as equal. UNION/EXCEPT/INTERSECT over a
// float column must be rejected; a non-float set-op still succeeds.

#[test]
fn test_set_op_float_column_rejected() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    let mut p = SqlPlanner::new(&mut client, &sn);
    p.execute("CREATE TABLE a (id BIGINT PRIMARY KEY, d DOUBLE NOT NULL, g BIGINT NOT NULL)")
        .unwrap();
    p.execute("CREATE TABLE b (id BIGINT PRIMARY KEY, d DOUBLE NOT NULL, g BIGINT NOT NULL)")
        .unwrap();

    for op in ["UNION", "EXCEPT", "INTERSECT"] {
        let err = p
            .execute(&format!("CREATE VIEW vbad AS SELECT d FROM a {op} SELECT d FROM b"))
            .unwrap_err();
        assert!(
            matches!(&err, GnitzSqlError::Unsupported(s) if s.contains("float")),
            "expected float-identity Unsupported for {op}, got {:?}",
            err,
        );
    }

    // A non-float set-op still succeeds.
    p.execute("CREATE VIEW vg AS SELECT g FROM a UNION SELECT g FROM b")
        .unwrap();
}

// ── Duplicate output column names rejected (set-op view) ──────────────

#[test]
fn test_set_op_duplicate_output_columns_rejected() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    let mut p = SqlPlanner::new(&mut client, &sn);
    p.execute("CREATE TABLE so_dup (id BIGINT PRIMARY KEY, a BIGINT NOT NULL, b BIGINT NOT NULL)")
        .unwrap();
    let err = p
        .execute(
            "CREATE VIEW v_so_dup AS SELECT a AS x, b AS x FROM so_dup \
         UNION SELECT a AS x, b AS x FROM so_dup",
        )
        .unwrap_err();
    match err {
        GnitzSqlError::Plan(s) => assert!(s.contains("duplicate column"), "got: {}", s),
        e => panic!("expected Plan, got {:?}", e),
    }
}

/// INTERSECT/EXCEPT DISTINCT compile to the join-free weight-clamp algebra: the
/// view circuits must carry NO join/anti-join nodes of any kind and DO carry a
/// positive_part node. (Semi-join is fully retired, so there is no opcode left to
/// assert zero on — its absence is structural.)
#[test]
fn test_set_op_distinct_join_free_circuit_shape() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    {
        let mut p = SqlPlanner::new(&mut client, &sn);
        p.execute("CREATE TABLE jf_a (id BIGINT PRIMARY KEY, c BIGINT NOT NULL)")
            .unwrap();
        p.execute("CREATE TABLE jf_b (id BIGINT PRIMARY KEY, c BIGINT NOT NULL)")
            .unwrap();
        p.execute("CREATE VIEW jf_except AS SELECT c FROM jf_a EXCEPT SELECT c FROM jf_b")
            .unwrap();
        p.execute("CREATE VIEW jf_intersect AS SELECT c FROM jf_a INTERSECT SELECT c FROM jf_b")
            .unwrap();
    }
    let except = client.resolve_table_or_view_id(&sn, "jf_except").unwrap().0;
    let intersect = client.resolve_table_or_view_id(&sn, "jf_intersect").unwrap().0;

    let nodes = scan_circuit_nodes(&mut client);
    let nodes = nodes.as_ref();
    for (vid, name) in [(except, "EXCEPT"), (intersect, "INTERSECT")] {
        let n = |op: u64| opcode_node_count(nodes, vid, op);
        assert_eq!(n(OPCODE_JOIN_DELTA_TRACE), 0, "{name} DISTINCT must have no equi join");
        assert_eq!(
            n(OPCODE_POSITIVE_PART),
            1,
            "{name} DISTINCT must emit exactly one positive_part node"
        );
    }
}
