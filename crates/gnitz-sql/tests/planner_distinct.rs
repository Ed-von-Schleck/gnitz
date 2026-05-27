#![cfg(feature = "integration")]

use gnitz_core::{GnitzClient, ColData, Schema, ZSetBatch};
use gnitz_sql::{SqlPlanner, SqlResult};
use gnitz_test_harness::ServerHandle;

fn make_planner(srv: &ServerHandle) -> (GnitzClient, String) {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let sn = format!("ds{}", SEQ.fetch_add(1, Ordering::Relaxed));
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

// ── item 14: SELECT DISTINCT projects and deduplicates ───────────────

#[test]
fn test_select_distinct_projects_and_dedups() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (mut client, sn) = make_planner(&srv);
    {
        let mut p = SqlPlanner::new(&mut client, &sn);
        p.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, c1 BIGINT NOT NULL, c2 BIGINT NOT NULL)").unwrap();
        // DISTINCT over c1 only: c1 values {7,7,9} → distinct {7,9}.
        p.execute("CREATE VIEW v AS SELECT DISTINCT c1 FROM t").unwrap();
        p.execute("INSERT INTO t (id, c1, c2) VALUES (1, 7, 100), (2, 7, 200), (3, 9, 300)").unwrap();
    }
    let (schema, batch) = read_view(&mut client, &sn, "v");
    // Schema is [_distinct_pk, c1] — id and c2 must not leak.
    assert!(schema.columns.iter().any(|c| c.name.eq_ignore_ascii_case("c1")));
    assert!(!schema.columns.iter().any(|c| c.name.eq_ignore_ascii_case("id")),
        "unselected `id` must not appear: {:?}",
        schema.columns.iter().map(|c| &c.name).collect::<Vec<_>>());
    assert!(!schema.columns.iter().any(|c| c.name.eq_ignore_ascii_case("c2")),
        "unselected `c2` must not appear");
    assert_eq!(schema.columns.len(), 2, "exactly _distinct_pk + c1");

    // Two distinct c1 values, not three rows.
    assert_eq!(batch.len(), 2, "DISTINCT c1 must collapse the two c1=7 rows");
    let c1 = col_idx(&schema, "c1");
    let mut vals: Vec<i64> = (0..batch.len()).map(|r| i64_at(&batch, c1, r)).collect();
    vals.sort();
    assert_eq!(vals, vec![7, 9], "distinct c1 values must be {{7, 9}}");
}
