#![cfg(feature = "integration")]

mod helpers;

use gnitz_core::{Connection, alloc_table_id, alloc_schema_id, push, scan,
                 SCHEMA_TAB, FIRST_USER_TABLE_ID};
use gnitz_core::GnitzClient;
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

// --- Phase 4: GnitzClient DDL/DML tests ---

#[test]
fn test_scan_system_tables() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let client = GnitzClient::connect(&srv.sock_path).unwrap();

    let (_, data) = client.scan(SCHEMA_TAB).unwrap();
    let data = data.expect("SCHEMA_TAB must return rows");
    assert!(!data.is_empty(), "expected at least one schema row");

    // Collect names from Strings column at index 1
    let names: Vec<&str> = (0..data.len())
        .filter(|&i| data.weights[i] > 0)
        .filter_map(|i| {
            if let gnitz_protocol::ColData::Strings(v) = &data.columns[1] {
                v[i].as_deref()
            } else { None }
        })
        .collect();

    assert!(names.contains(&"_system"), "_system schema missing; got: {:?}", names);
    assert!(names.contains(&"public"),  "public schema missing; got: {:?}", names);

    client.close();
}

#[test]
fn test_create_drop_schema() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let client = GnitzClient::connect(&srv.sock_path).unwrap();

    let sid = client.create_schema("myapp").unwrap();
    assert!(sid >= gnitz_core::FIRST_USER_SCHEMA_ID, "schema id too small: {}", sid);

    // Verify it appears in scan
    let (_, data) = client.scan(SCHEMA_TAB).unwrap();
    let data = data.unwrap();
    let found = (0..data.len()).any(|i| {
        data.weights[i] > 0 && data.pk_lo[i] == sid
    });
    assert!(found, "newly created schema {} not found in scan", sid);

    // Drop it
    client.drop_schema("myapp").unwrap();

    // Verify it no longer has positive weight
    let (_, data2) = client.scan(SCHEMA_TAB).unwrap();
    let data2 = data2.unwrap();
    let still_present = (0..data2.len()).any(|i| {
        data2.weights[i] > 0 && data2.pk_lo[i] == sid
    });
    assert!(!still_present, "schema {} still present after drop", sid);

    client.close();
}

#[test]
fn test_create_drop_table() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let client = GnitzClient::connect(&srv.sock_path).unwrap();

    client.create_schema("s1").unwrap();

    let cols = vec![
        ColumnDef { name: "id".into(),    type_code: TypeCode::U64, is_nullable: false },
        ColumnDef { name: "value".into(), type_code: TypeCode::I64, is_nullable: false },
    ];
    let tid = client.create_table("s1", "t1", &cols, 0, true).unwrap();
    assert!(tid >= FIRST_USER_TABLE_ID, "table id too small: {}", tid);

    // Scan user table — should be empty but return a schema
    let (wire_schema, data) = client.scan(tid).unwrap();
    assert!(wire_schema.is_some(), "scan of new table must return schema");
    let data = data.unwrap();
    assert!(data.is_empty(), "new table must be empty");

    // Drop
    client.drop_table("s1", "t1").unwrap();

    client.close();
}

#[test]
fn test_push_and_scan() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let client = GnitzClient::connect(&srv.sock_path).unwrap();

    client.create_schema("s2").unwrap();

    let cols = vec![
        ColumnDef { name: "id".into(),    type_code: TypeCode::U64, is_nullable: false },
        ColumnDef { name: "value".into(), type_code: TypeCode::I64, is_nullable: false },
    ];
    let tid = client.create_table("s2", "t2", &cols, 0, true).unwrap();

    let table_schema = Schema {
        columns: cols,
        pk_index: 0,
    };

    // Push 3 rows: (10, 100), (20, 200), (30, 300)
    let mut batch = ZSetBatch::new(&table_schema);
    for &(pk, val) in &[(10u64, 100i64), (20, 200), (30, 300)] {
        batch.pk_lo.push(pk);
        batch.pk_hi.push(0);
        batch.weights.push(1);
        batch.nulls.push(0);
        if let ColData::Fixed(buf) = &mut batch.columns[1] {
            buf.extend_from_slice(&val.to_le_bytes());
        }
    }
    client.push(tid, &table_schema, &batch).unwrap();

    let (_, data) = client.scan(tid).unwrap();
    let data = data.unwrap();
    assert_eq!(data.len(), 3, "expected 3 rows; got {}", data.len());

    let mut pks: Vec<u64> = data.pk_lo.clone();
    pks.sort_unstable();
    assert_eq!(pks, vec![10, 20, 30]);

    client.close();
}

#[test]
fn test_delete_rows() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let client = GnitzClient::connect(&srv.sock_path).unwrap();

    client.create_schema("s3").unwrap();

    let cols = vec![
        ColumnDef { name: "id".into(),    type_code: TypeCode::U64, is_nullable: false },
        ColumnDef { name: "value".into(), type_code: TypeCode::I64, is_nullable: false },
    ];
    let tid = client.create_table("s3", "t3", &cols, 0, true).unwrap();

    let table_schema = Schema {
        columns: cols,
        pk_index: 0,
    };

    // Push rows with pk 10, 11, 12
    let mut batch = ZSetBatch::new(&table_schema);
    for &(pk, val) in &[(10u64, 1i64), (11, 2), (12, 3)] {
        batch.pk_lo.push(pk);
        batch.pk_hi.push(0);
        batch.weights.push(1);
        batch.nulls.push(0);
        if let ColData::Fixed(buf) = &mut batch.columns[1] {
            buf.extend_from_slice(&val.to_le_bytes());
        }
    }
    client.push(tid, &table_schema, &batch).unwrap();

    // Delete middle row (pk=11)
    client.delete(tid, &table_schema, &[(11u64, 0u64)]).unwrap();

    let (_, data) = client.scan(tid).unwrap();
    let data = data.unwrap();
    let positive: Vec<u64> = data.pk_lo.iter().copied()
        .zip(data.weights.iter().copied())
        .filter(|(_, w)| *w > 0)
        .map(|(pk, _)| pk)
        .collect();
    assert_eq!(positive.len(), 2, "expected 2 rows after delete; got {:?}", positive);
    assert!(!positive.contains(&11), "deleted row 11 still present");

    client.close();
}

#[test]
fn test_string_columns() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let client = GnitzClient::connect(&srv.sock_path).unwrap();

    client.create_schema("s4").unwrap();

    let cols = vec![
        ColumnDef { name: "id".into(),    type_code: TypeCode::U64,    is_nullable: false },
        ColumnDef { name: "label".into(), type_code: TypeCode::String, is_nullable: false },
        ColumnDef { name: "note".into(),  type_code: TypeCode::String, is_nullable: true  },
    ];
    let tid = client.create_table("s4", "t4", &cols, 0, true).unwrap();

    let table_schema = Schema { columns: cols, pk_index: 0 };

    // Push 2 rows: (1, "hello", Some("world")) and (2, "foo", None)
    let mut batch = ZSetBatch::new(&table_schema);
    for &pk in &[1u64, 2u64] {
        batch.pk_lo.push(pk);
        batch.pk_hi.push(0);
        batch.weights.push(1);
        batch.nulls.push(0);
    }
    if let ColData::Strings(v) = &mut batch.columns[1] {
        v.push(Some("hello".to_string()));
        v.push(Some("foo".to_string()));
    }
    if let ColData::Strings(v) = &mut batch.columns[2] {
        v.push(Some("world".to_string()));
        v.push(None);
    }
    // note column (col 2) is payload_idx=1 (col 0 is pk, col 1 is payload 0, col 2 is payload 1)
    // Row 1 (pk=2) has null note → set bit 1 in nulls[1]
    batch.nulls[1] |= 1u64 << 1;
    client.push(tid, &table_schema, &batch).unwrap();

    let (_, data) = client.scan(tid).unwrap();
    let data = data.unwrap();
    assert_eq!(data.len(), 2, "expected 2 rows");

    let idx1 = data.pk_lo.iter().position(|&x| x == 1).expect("row pk=1 missing");
    if let ColData::Strings(labels) = &data.columns[1] {
        assert_eq!(labels[idx1].as_deref(), Some("hello"));
    }
    if let ColData::Strings(notes) = &data.columns[2] {
        assert_eq!(notes[idx1].as_deref(), Some("world"));
    }

    let idx2 = data.pk_lo.iter().position(|&x| x == 2).expect("row pk=2 missing");
    if let ColData::Strings(notes) = &data.columns[2] {
        assert!(notes[idx2].is_none(), "expected null note for pk=2");
    }

    client.close();
}

#[test]
fn test_resolve_table_id() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let client = GnitzClient::connect(&srv.sock_path).unwrap();

    client.create_schema("s5").unwrap();

    let cols = vec![
        ColumnDef { name: "id".into(),    type_code: TypeCode::U64,    is_nullable: false },
        ColumnDef { name: "name".into(),  type_code: TypeCode::String, is_nullable: false },
        ColumnDef { name: "score".into(), type_code: TypeCode::F64,    is_nullable: true  },
    ];
    let tid = client.create_table("s5", "t5", &cols, 0, true).unwrap();

    let (resolved_tid, schema) = client.resolve_table_id("s5", "t5").unwrap();
    assert_eq!(resolved_tid, tid);
    assert!(resolved_tid >= FIRST_USER_TABLE_ID);
    assert_eq!(schema.columns.len(), 3);
    assert_eq!(schema.pk_index, 0);
    assert_eq!(schema.columns[0].name, "id");
    assert_eq!(schema.columns[0].type_code, TypeCode::U64);
    assert_eq!(schema.columns[1].name, "name");
    assert_eq!(schema.columns[1].type_code, TypeCode::String);
    assert_eq!(schema.columns[2].name, "score");
    assert_eq!(schema.columns[2].type_code, TypeCode::F64);
    assert!(schema.columns[2].is_nullable);

    client.close();
}
