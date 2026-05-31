#![cfg(feature = "integration")]

//! Plan-structure tests for NULL equi-join-key gating.
//!
//! A NULL join key must match nothing (SQL 3VL). The planner gates NULL keys
//! out of the match with `IS NOT NULL` filters; a NOT NULL key emits no filter
//! node at all, so the common-case plan is byte-identical to the original.
//! These tests assert the emitted Filter-node count for each key-nullability /
//! join-type combination by reading back the circuit's `nodes` system table.

use gnitz_core::{GnitzClient, ColData, PkColumn, CIRCUIT_NODES_TAB, OPCODE_FILTER};
use gnitz_sql::{SqlPlanner, SqlResult};
use gnitz_test_harness::ServerHandle;

fn make_planner(srv: &ServerHandle) -> (GnitzClient, String) {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let sn = format!("snull{}", SEQ.fetch_add(1, Ordering::Relaxed));
    let mut client = GnitzClient::connect(&srv.sock_path).unwrap();
    client.create_schema(&sn).unwrap();
    (client, sn)
}

fn view_id_of(results: &[SqlResult]) -> u64 {
    results
        .iter()
        .find_map(|r| match r {
            SqlResult::ViewCreated { view_id } => Some(*view_id),
            _ => None,
        })
        .expect("CREATE VIEW did not return a view id")
}

/// Count Filter nodes belonging to `vid` in the circuit `nodes` system table.
/// The compound PK is (view_id, sub) packed LE into 16 bytes (view_id in the
/// low 8); the opcode is schema column 3 (Fixed u64-LE, non-PK).
fn filter_node_count(client: &mut GnitzClient, vid: u64) -> usize {
    let (_, batch, _) = client.scan(CIRCUIT_NODES_TAB).unwrap();
    let batch = match batch {
        Some(b) => b,
        None => return 0,
    };
    let opcodes = match &batch.columns[3] {
        ColData::Fixed(buf) => buf,
        other => panic!("opcode column not Fixed: {other:?}"),
    };
    let mut count = 0;
    for i in 0..batch.len() {
        let pk = match &batch.pks {
            PkColumn::Bytes { .. } => batch.pks.get_bytes(i),
            other => panic!("circuit nodes PK not wide bytes: {other:?}"),
        };
        let row_vid = u64::from_le_bytes(pk[0..8].try_into().unwrap());
        if row_vid != vid {
            continue;
        }
        let opcode = u64::from_le_bytes(opcodes[i * 8..i * 8 + 8].try_into().unwrap());
        if opcode == OPCODE_FILTER {
            count += 1;
        }
    }
    count
}

/// All-NOT-NULL keys: zero Filter nodes — byte-identical to the original plan.
#[test]
fn test_not_null_keys_emit_no_filter() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (mut client, sn) = make_planner(&srv);
    {
        let mut p = SqlPlanner::new(&mut client, &sn);
        p.execute("CREATE TABLE l (id BIGINT NOT NULL PRIMARY KEY, fk BIGINT NOT NULL)").unwrap();
        p.execute("CREATE TABLE r (k BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)").unwrap();
        let inner = p.execute("CREATE VIEW vi AS SELECT * FROM l JOIN r ON l.fk = r.k").unwrap();
        let left  = p.execute("CREATE VIEW vl AS SELECT * FROM l LEFT JOIN r ON l.fk = r.k").unwrap();
        let vi = view_id_of(&inner);
        let vl = view_id_of(&left);
        assert_eq!(filter_node_count(&mut client, vi), 0, "INNER NOT NULL: no filter");
        assert_eq!(filter_node_count(&mut client, vl), 0, "LEFT NOT NULL: no filter");
    }
}

/// INNER JOIN, nullable inner (right) key: one `IS NOT NULL` filter on the right.
#[test]
fn test_inner_nullable_right_key_emits_one_filter() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (mut client, sn) = make_planner(&srv);
    {
        let mut p = SqlPlanner::new(&mut client, &sn);
        p.execute("CREATE TABLE l (id BIGINT NOT NULL PRIMARY KEY, fk BIGINT NOT NULL)").unwrap();
        p.execute("CREATE TABLE r (k BIGINT NOT NULL PRIMARY KEY, rk BIGINT NULL)").unwrap();
        let res = p.execute("CREATE VIEW v AS SELECT * FROM l JOIN r ON l.fk = r.rk").unwrap();
        let vid = view_id_of(&res);
        assert_eq!(filter_node_count(&mut client, vid), 1);
    }
}

/// INNER JOIN, nullable left key: one `IS NOT NULL` filter on the left
/// (INNER drops NULL left keys outright — no bypass split).
#[test]
fn test_inner_nullable_left_key_emits_one_filter() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (mut client, sn) = make_planner(&srv);
    {
        let mut p = SqlPlanner::new(&mut client, &sn);
        p.execute("CREATE TABLE l (id BIGINT NOT NULL PRIMARY KEY, fk BIGINT NULL)").unwrap();
        p.execute("CREATE TABLE r (k BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)").unwrap();
        let res = p.execute("CREATE VIEW v AS SELECT * FROM l JOIN r ON l.fk = r.k").unwrap();
        let vid = view_id_of(&res);
        assert_eq!(filter_node_count(&mut client, vid), 1);
    }
}

/// LEFT JOIN, nullable preserved (left) key, NOT NULL right: the preserved side
/// is split into `IS NOT NULL` (match) and `IS NULL` (bypass) → two filters.
#[test]
fn test_left_nullable_preserved_key_emits_split() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (mut client, sn) = make_planner(&srv);
    {
        let mut p = SqlPlanner::new(&mut client, &sn);
        p.execute("CREATE TABLE l (id BIGINT NOT NULL PRIMARY KEY, fk BIGINT NULL)").unwrap();
        p.execute("CREATE TABLE r (k BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)").unwrap();
        let res = p.execute("CREATE VIEW v AS SELECT * FROM l LEFT JOIN r ON l.fk = r.k").unwrap();
        let vid = view_id_of(&res);
        assert_eq!(filter_node_count(&mut client, vid), 2);
    }
}

/// LEFT JOIN, both keys nullable: right `IS NOT NULL` + left split = 3 filters.
#[test]
fn test_left_both_nullable_emits_three_filters() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (mut client, sn) = make_planner(&srv);
    {
        let mut p = SqlPlanner::new(&mut client, &sn);
        p.execute("CREATE TABLE l (id BIGINT NOT NULL PRIMARY KEY, fk BIGINT NULL)").unwrap();
        p.execute("CREATE TABLE r (k BIGINT NOT NULL PRIMARY KEY, rk BIGINT NULL)").unwrap();
        let res = p.execute("CREATE VIEW v AS SELECT * FROM l LEFT JOIN r ON l.fk = r.rk").unwrap();
        let vid = view_id_of(&res);
        assert_eq!(filter_node_count(&mut client, vid), 3);
    }
}
