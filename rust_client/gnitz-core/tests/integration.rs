#![cfg(feature = "integration")]

mod helpers;

use gnitz_core::{Connection, alloc_table_id, alloc_schema_id, push, scan,
                 SCHEMA_TAB, FIRST_USER_TABLE_ID};
use gnitz_protocol::{Schema, ColumnDef, TypeCode, ZSetBatch, ColData};
use helpers::ServerHandle;

#[test]
fn test_connect_disconnect() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let conn = Connection::connect(&srv.sock_path).unwrap();
    conn.close();
}

#[test]
fn test_alloc_ids() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let conn = Connection::connect(&srv.sock_path).unwrap();

    let mut ids = Vec::new();
    for _ in 0..10 {
        ids.push(alloc_table_id(&conn).unwrap());
    }

    // All >= FIRST_USER_TABLE_ID
    for &id in &ids {
        assert!(id >= FIRST_USER_TABLE_ID);
    }

    // All distinct
    let mut sorted = ids.clone();
    sorted.sort_unstable();
    sorted.dedup();
    assert_eq!(sorted.len(), 10);

    conn.close();
}

#[test]
fn test_push_scan_roundtrip() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let conn = Connection::connect(&srv.sock_path).unwrap();

    // SCHEMA_TAB: pk=U64 (col 0), name=STRING (col 1)
    let schema_tab_schema = Schema {
        columns: vec![
            ColumnDef { name: "schema_id".into(), type_code: TypeCode::U64,    is_nullable: false },
            ColumnDef { name: "name".into(),      type_code: TypeCode::String, is_nullable: false },
        ],
        pk_index: 0,
    };

    // Allocate 5 unique IDs to use as schema record PKs
    let pks: Vec<u64> = (0..5).map(|_| alloc_schema_id(&conn).unwrap()).collect();

    let batch = ZSetBatch {
        pk_lo:   pks.clone(),
        pk_hi:   vec![0; 5],
        weights: vec![1; 5],
        nulls:   vec![0; 5],
        columns: vec![
            ColData::Fixed(vec![]),
            ColData::Strings((0..5usize).map(|i| Some(format!("test_{}", i))).collect()),
        ],
    };
    push(&conn, SCHEMA_TAB, &schema_tab_schema, &batch).unwrap();

    let (wire_schema, data) = scan(&conn, SCHEMA_TAB).unwrap();
    assert!(wire_schema.is_some(), "scan must return schema");
    let data = data.unwrap();

    // Filter for our PKs (server may have other system schema records)
    let our_pks: std::collections::HashSet<u64> = pks.into_iter().collect();
    let found: Vec<_> = data.pk_lo.iter().copied()
        .zip(data.weights.iter().copied())
        .filter(|(pk, _)| our_pks.contains(pk))
        .collect();

    assert_eq!(found.len(), 5, "expected 5 pushed rows; got {}", found.len());
    for (_, weight) in &found {
        assert!(*weight > 0, "all weights must be positive");
    }

    conn.close();
}
