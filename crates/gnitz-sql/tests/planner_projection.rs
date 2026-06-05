#![cfg(feature = "integration")]

use gnitz_core::GnitzClient;
use gnitz_sql::SqlPlanner;
use gnitz_test_harness::ServerHandle;

fn make_planner(srv: &ServerHandle) -> (GnitzClient, String) {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let sn = format!("pj{}", SEQ.fetch_add(1, Ordering::Relaxed));
    let mut client = GnitzClient::connect(&srv.sock_path).unwrap();
    client.create_schema(&sn).unwrap();
    (client, sn)
}

// ── item 40: PK column move must preserve remaining order ─────────────

#[test]
fn test_projection_pk_move_preserves_order() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (mut client, sn) = make_planner(&srv);
    {
        let mut p = SqlPlanner::new(&mut client, &sn);
        // PK `id` is at source column index 2.
        p.execute("CREATE TABLE t (name TEXT, age BIGINT, id BIGINT PRIMARY KEY)").unwrap();
        // SELECT name, age, id → PK moves to front; remaining order must be name, age.
        p.execute("CREATE VIEW v AS SELECT name, age, id FROM t").unwrap();
    }
    let (_, s) = client.resolve_table_or_view_id(&sn, "v").unwrap();
    let names: Vec<String> = s.columns.iter().map(|c| c.name.to_lowercase()).collect();
    assert_eq!(names, vec!["id", "name", "age"],
        "PK move must shift remaining columns, not swap");
}

// ── item 43: PK column with alias is PassThrough, not Computed ─────────

#[test]
fn test_projection_pk_alias_accepted() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (mut client, sn) = make_planner(&srv);
    {
        let mut p = SqlPlanner::new(&mut client, &sn);
        p.execute("CREATE TABLE employees (id BIGINT PRIMARY KEY, name TEXT)").unwrap();
        p.execute("CREATE VIEW v AS SELECT id AS employee_id, name FROM employees").unwrap();
    }
    let (_, s) = client.resolve_table_or_view_id(&sn, "v").unwrap();
    assert!(s.columns[0].name.eq_ignore_ascii_case("employee_id"),
        "aliased PK must be the first output column, got {:?}", s.columns[0].name);
    assert!(!s.columns[0].is_nullable, "PK column must stay non-nullable");
}
