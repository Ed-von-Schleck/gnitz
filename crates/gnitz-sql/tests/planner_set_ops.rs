#![cfg(feature = "integration")]

use gnitz_core::{GnitzClient, ColData, Schema, ZSetBatch};
use gnitz_sql::{SqlPlanner, SqlResult};
use gnitz_test_harness::ServerHandle;

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
