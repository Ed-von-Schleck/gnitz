#![cfg(feature = "integration")]

use gnitz_core::{GnitzClient, ColData, Schema, ZSetBatch};
use gnitz_sql::{GnitzSqlError, SqlPlanner, SqlResult};
use gnitz_test_harness::ServerHandle;

fn must_err(r: Result<Vec<SqlResult>, GnitzSqlError>) -> GnitzSqlError {
    match r {
        Ok(_) => panic!("expected error, got Ok"),
        Err(e) => e,
    }
}

fn make_planner(srv: &ServerHandle) -> (GnitzClient, String) {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let sn = format!("so{}", SEQ.fetch_add(1, Ordering::Relaxed));
    let mut client = GnitzClient::connect(&srv.sock_path).unwrap();
    client.create_schema(&sn).unwrap();
    (client, sn)
}

fn read_view(client: &mut GnitzClient, sn: &str, view: &str) -> (Schema, ZSetBatch) {
    let mut p = SqlPlanner::new(client, sn);
    let mut res = p.execute(&format!("SELECT * FROM {}", view)).unwrap();
    match res.pop().unwrap() {
        SqlResult::Rows { schema, batch } => (schema, batch),
        _ => panic!("expected Rows"),
    }
}

fn col_idx(schema: &Schema, name: &str) -> usize {
    schema.columns.iter().position(|c| c.name.eq_ignore_ascii_case(name))
        .unwrap_or_else(|| panic!("column '{}' not in {:?}", name,
            schema.columns.iter().map(|c| &c.name).collect::<Vec<_>>()))
}

fn i64_at(batch: &ZSetBatch, col: usize, row: usize) -> i64 {
    match &batch.columns[col] {
        ColData::Fixed(b) => i64::from_le_bytes(b[row*8..row*8+8].try_into().unwrap()),
        other => panic!("expected Fixed col, got {:?}", std::mem::discriminant(other)),
    }
}

/// Create tables A,B (id BIGINT PK, val BIGINT) and the EXCEPT+INTERSECT views,
/// BEFORE any rows exist, so subsequent inserts arrive as separate incremental
/// epochs (the correct incremental-view test shape).
fn setup_views(client: &mut GnitzClient, sn: &str) {
    let mut p = SqlPlanner::new(client, sn);
    p.execute("CREATE TABLE a (id BIGINT PRIMARY KEY, val BIGINT NOT NULL)").unwrap();
    p.execute("CREATE TABLE b (id BIGINT PRIMARY KEY, val BIGINT NOT NULL)").unwrap();
    p.execute("CREATE VIEW ve AS SELECT * FROM a EXCEPT SELECT * FROM b").unwrap();
    p.execute("CREATE VIEW vi AS SELECT * FROM a INTERSECT SELECT * FROM b").unwrap();
}

fn insert(client: &mut GnitzClient, sn: &str, tbl: &str, id: i64, val: i64) {
    let mut p = SqlPlanner::new(client, sn);
    p.execute(&format!("INSERT INTO {} (id, val) VALUES ({}, {})", tbl, id, val)).unwrap();
}

// ── item 36: EXCEPT/INTERSECT use full-row identity, not source PK ────

#[test]
fn test_except_full_row_identity() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
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
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
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
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
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
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (mut client, sn) = make_planner(&srv);
    {
        let mut p = SqlPlanner::new(&mut client, &sn);
        p.execute("CREATE TABLE a (id BIGINT PRIMARY KEY, name BIGINT NOT NULL)").unwrap();
        p.execute("CREATE TABLE b (id BIGINT PRIMARY KEY, name BIGINT NOT NULL)").unwrap();
        // Project only `name`; `id` must NOT leak into the view schema.
        p.execute("CREATE VIEW v AS SELECT name FROM a UNION SELECT name FROM b").unwrap();
        p.execute("INSERT INTO a (id, name) VALUES (1, 100)").unwrap();
        p.execute("INSERT INTO b (id, name) VALUES (2, 100)").unwrap();
    }
    let (schema, batch) = read_view(&mut client, &sn, "v");
    // schema is [_set_pk, name] — exactly one projected payload column.
    assert!(schema.columns.iter().any(|c| c.name.eq_ignore_ascii_case("name")));
    assert!(!schema.columns.iter().any(|c| c.name.eq_ignore_ascii_case("id")),
        "projected-out column `id` must not appear, got {:?}",
        schema.columns.iter().map(|c| &c.name).collect::<Vec<_>>());
    assert_eq!(schema.columns.len(), 2, "exactly _set_pk + name");
    // UNION distinct: both sides have name=100 → one row.
    assert_eq!(batch.len(), 1, "UNION distinct over identical projected value → 1 row");
    assert_eq!(i64_at(&batch, col_idx(&schema, "name"), 0), 100);
}

// ── item 27: EXCEPT/INTERSECT lift the right side through distinct ────
//
// Two B rows with different PKs but identical projected content must change
// B's set membership only ONCE. Without distinct lifting the second insert
// re-fires the correction semi-join, driving the EXCEPT output to weight -1.

#[test]
fn test_except_distinct_lifting_no_underflow() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (mut client, sn) = make_planner(&srv);
    {
        let mut p = SqlPlanner::new(&mut client, &sn);
        p.execute("CREATE TABLE a (id BIGINT PRIMARY KEY, name BIGINT NOT NULL)").unwrap();
        p.execute("CREATE TABLE b (id BIGINT PRIMARY KEY, name BIGINT NOT NULL)").unwrap();
        p.execute("CREATE VIEW v AS SELECT name FROM a EXCEPT SELECT name FROM b").unwrap();
        // A has name=100. B gains two rows, both name=100 (distinct PKs).
        p.execute("INSERT INTO a (id, name) VALUES (1, 100)").unwrap();
        p.execute("INSERT INTO b (id, name) VALUES (1, 100)").unwrap();
        p.execute("INSERT INTO b (id, name) VALUES (2, 100)").unwrap();
    }
    let (_schema, batch) = read_view(&mut client, &sn, "v");
    // name=100 is in both A and B → excluded. The second identical-content B
    // insert must not push the weight to -1 (which would surface as a row).
    assert_eq!(batch.len(), 0,
        "EXCEPT must stay empty; the duplicate projected B row must not underflow the weight");
}

// ── same-source INTERSECT/EXCEPT rejection (mirror of the self-join guard) ──
//
// Both branches resolving to the SAME source relation id collapse to one
// dependency edge: a single delta drives both branch pipelines in one pass and
// the cross-correction term is dropped (silently wrong results). The guard
// rejects exactly that. The discriminator is source-id equality, NOT base-table
// overlap — two different views over the same base produce two edges and stay
// accepted (see the two-view tests below).

fn make_t_with_c(client: &mut GnitzClient, sn: &str) {
    let mut p = SqlPlanner::new(client, sn);
    p.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, c BIGINT NOT NULL)").unwrap();
}

/// Create table `t(id, c)`, run any `extra_setup` statements, then assert
/// `view_sql` is rejected by the same-source guard's "same relation" error.
fn assert_same_relation_rejected(extra_setup: &[&str], view_sql: &str) {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (mut client, sn) = make_planner(&srv);
    make_t_with_c(&mut client, &sn);
    if !extra_setup.is_empty() {
        let mut p = SqlPlanner::new(&mut client, &sn);
        for sql in extra_setup {
            p.execute(sql).unwrap();
        }
    }
    let mut p = SqlPlanner::new(&mut client, &sn);
    match must_err(p.execute(view_sql)) {
        GnitzSqlError::Unsupported(s) => assert!(s.contains("same relation"), "got: {}", s),
        e => panic!("expected Unsupported, got {:?}", e),
    }
}

#[test]
fn test_same_table_except_rejected() {
    assert_same_relation_rejected(
        &[],
        "CREATE VIEW v AS SELECT c FROM t WHERE c > 0 EXCEPT SELECT c FROM t WHERE c < 10");
}

#[test]
fn test_same_table_intersect_rejected() {
    assert_same_relation_rejected(
        &[],
        "CREATE VIEW v AS SELECT c FROM t WHERE c > 0 INTERSECT SELECT c FROM t WHERE c < 10");
}

#[test]
fn test_same_view_except_rejected() {
    // Both branches read the same view → same source id → rejected.
    assert_same_relation_rejected(
        &["CREATE VIEW vt AS SELECT id, c FROM t WHERE c > 0"],
        "CREATE VIEW v AS SELECT c FROM vt EXCEPT SELECT c FROM vt");
}

#[test]
fn test_same_view_intersect_rejected() {
    assert_same_relation_rejected(
        &["CREATE VIEW vt AS SELECT id, c FROM t WHERE c > 0"],
        "CREATE VIEW v AS SELECT c FROM vt INTERSECT SELECT c FROM vt");
}

// ── non-over-rejection: two DIFFERENT views over the same base, accepted ────
//
// The decisive test that the guard keys on source-id equality, not base-table
// overlap. vt1/vt2 are distinct views (distinct source ids) over the same t.

#[test]
fn test_two_views_same_base_except_accepted_and_correct() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (mut client, sn) = make_planner(&srv);
    make_t_with_c(&mut client, &sn);
    {
        let mut p = SqlPlanner::new(&mut client, &sn);
        p.execute("CREATE VIEW vt1 AS SELECT id, c FROM t WHERE c > 0").unwrap();
        p.execute("CREATE VIEW vt2 AS SELECT id, c FROM t WHERE c > 50").unwrap();
        // Must NOT raise: vt1 and vt2 have distinct source ids.
        p.execute("CREATE VIEW v AS SELECT c FROM vt1 EXCEPT SELECT c FROM vt2").unwrap();
        p.execute("INSERT INTO t (id, c) VALUES (1, 10), (2, 60)").unwrap();
    }
    // vt1 = {10, 60}, vt2 = {60}; EXCEPT = {10}.
    let (schema, batch) = read_view(&mut client, &sn, "v");
    assert_eq!(batch.len(), 1, "EXCEPT of two distinct views must be {{10}}");
    assert_eq!(i64_at(&batch, col_idx(&schema, "c"), 0), 10);
}

#[test]
fn test_two_views_same_base_intersect_accepted_and_correct() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (mut client, sn) = make_planner(&srv);
    make_t_with_c(&mut client, &sn);
    {
        let mut p = SqlPlanner::new(&mut client, &sn);
        p.execute("CREATE VIEW vt1 AS SELECT id, c FROM t WHERE c > 0").unwrap();
        p.execute("CREATE VIEW vt2 AS SELECT id, c FROM t WHERE c > 50").unwrap();
        p.execute("CREATE VIEW v AS SELECT c FROM vt1 INTERSECT SELECT c FROM vt2").unwrap();
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
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (mut client, sn) = make_planner(&srv);
    make_t_with_c(&mut client, &sn);
    {
        let mut p = SqlPlanner::new(&mut client, &sn);
        p.execute(
            "CREATE VIEW v AS SELECT c FROM t WHERE c > 0 UNION SELECT c FROM t WHERE c < 100"
        ).unwrap();
        p.execute("INSERT INTO t (id, c) VALUES (1, 50), (2, 150)").unwrap();
    }
    // left {50,150}, right {50}; UNION distinct = {50, 150}.
    let (schema, batch) = read_view(&mut client, &sn, "v");
    let mut vals: Vec<i64> = (0..batch.len())
        .map(|r| i64_at(&batch, col_idx(&schema, "c"), r)).collect();
    vals.sort();
    assert_eq!(vals, vec![50, 150]);
}

#[test]
fn test_same_table_union_all_accepted_and_correct() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (mut client, sn) = make_planner(&srv);
    make_t_with_c(&mut client, &sn);
    {
        let mut p = SqlPlanner::new(&mut client, &sn);
        p.execute(
            "CREATE VIEW v AS SELECT c FROM t WHERE c > 0 UNION ALL SELECT c FROM t WHERE c < 100"
        ).unwrap();
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
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (mut client, sn) = make_planner(&srv);
    {
        let mut p = SqlPlanner::new(&mut client, &sn);
        p.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, k BIGINT NOT NULL, v BIGINT NOT NULL)").unwrap();
        // vt is a view over t; the join names t and vt → two dependency edges.
        p.execute("CREATE VIEW vt AS SELECT id, k, v FROM t WHERE v > 0").unwrap();
        p.execute(
            "CREATE VIEW j AS SELECT t.id AS lid, vt.id AS rid FROM t JOIN vt ON t.k = vt.k"
        ).unwrap();
        p.execute("INSERT INTO t (id, k, v) VALUES (1, 5, 10), (2, 5, 20)").unwrap();
    }
    // Both rows have v>0 → vt has both; join on k=5 → 2×2 = 4 product rows.
    let (_schema, batch) = read_view(&mut client, &sn, "j");
    assert_eq!(batch.len(), 4, "transitive self-join must recover the full 2x2 product");
}

#[test]
fn test_direct_self_join_still_rejected() {
    // Mirror of planner_view_validation.rs:81; kept here so the set-op scope
    // boundary is documented in one place.
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (mut client, sn) = make_planner(&srv);
    {
        let mut p = SqlPlanner::new(&mut client, &sn);
        p.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, k BIGINT NOT NULL)").unwrap();
    }
    let mut p = SqlPlanner::new(&mut client, &sn);
    let err = must_err(p.execute(
        "CREATE VIEW j AS SELECT a.id FROM t AS a JOIN t AS b ON a.k = b.k"));
    match err {
        GnitzSqlError::Unsupported(s) => assert!(
            s.to_lowercase().contains("self-join"), "got: {}", s),
        e => panic!("expected Unsupported, got {:?}", e),
    }
}
