#![cfg(feature = "integration")]

mod helpers;

use gnitz_core::{Connection, SCHEMA_TAB, FIRST_USER_TABLE_ID};
use gnitz_core::{GnitzClient, ExprBuilder, CircuitBuilder};
use gnitz_core::{Schema, ColumnDef, TypeCode, ZSetBatch, ColData, PkColumn};
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
        ids.push(conn.alloc_table_id().unwrap());
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
            ColumnDef { name: "schema_id".into(), type_code: TypeCode::U64,    is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
            ColumnDef { name: "name".into(),      type_code: TypeCode::String, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
        ],
        pk_index: 0,
    };

    // Allocate 5 unique IDs to use as schema record PKs
    let pks: Vec<u64> = (0..5).map(|_| conn.alloc_schema_id().unwrap()).collect();

    let batch = ZSetBatch {
        pks:     PkColumn::U64s(pks.clone()),
        weights: vec![1; 5],
        nulls:   vec![0; 5],
        columns: vec![
            ColData::Fixed(vec![]),
            ColData::Strings((0..5usize).map(|i| Some(format!("test_{}", i))).collect()),
        ],
    };
    conn.push(SCHEMA_TAB, &schema_tab_schema, &batch).unwrap();

    let (wire_schema, data, _) = conn.scan(SCHEMA_TAB).unwrap();
    assert!(wire_schema.is_some(), "scan must return schema");
    let data = data.unwrap();

    // Filter for our PKs (server may have other system schema records)
    let our_pks: std::collections::HashSet<u64> = pks.into_iter().collect();
    let found: Vec<_> = data.pks.to_vec_u128().into_iter().map(|x| x as u64)
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

    let (_, data, _) = client.scan(SCHEMA_TAB).unwrap();
    let data = data.expect("SCHEMA_TAB must return rows");
    assert!(!data.is_empty(), "expected at least one schema row");

    // Collect names from Strings column at index 1
    let names: Vec<&str> = (0..data.len())
        .filter(|&i| data.weights[i] > 0)
        .filter_map(|i| {
            if let ColData::Strings(v) = &data.columns[1] {
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
    let (_, data, _) = client.scan(SCHEMA_TAB).unwrap();
    let data = data.unwrap();
    let found = (0..data.len()).any(|i| {
        data.weights[i] > 0 && data.pks.get(i) as u64 == sid
    });
    assert!(found, "newly created schema {} not found in scan", sid);

    // Drop it
    client.drop_schema("myapp").unwrap();

    // Verify it no longer has positive weight
    let (_, data2, _) = client.scan(SCHEMA_TAB).unwrap();
    let data2 = data2.unwrap();
    let still_present = (0..data2.len()).any(|i| {
        data2.weights[i] > 0 && data2.pks.get(i) as u64 == sid
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
        ColumnDef { name: "id".into(),    type_code: TypeCode::U64, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
        ColumnDef { name: "value".into(), type_code: TypeCode::I64, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
    ];
    let tid = client.create_table("s1", "t1", &cols, 0, true).unwrap();
    assert!(tid >= FIRST_USER_TABLE_ID, "table id too small: {}", tid);

    // Scan user table — should be empty but return a schema.
    // Empty tables send no data block (FLAG_HAS_DATA requires non-empty batch),
    // so data_batch is None; that is the correct "empty" signal.
    let (wire_schema, data, _) = client.scan(tid).unwrap();
    assert!(wire_schema.is_some(), "scan of new table must return schema");
    assert!(data.is_none(), "new table must be empty (no data block)");

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
        ColumnDef { name: "id".into(),    type_code: TypeCode::U64, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
        ColumnDef { name: "value".into(), type_code: TypeCode::I64, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
    ];
    let tid = client.create_table("s2", "t2", &cols, 0, true).unwrap();

    let table_schema = Schema {
        columns: cols,
        pk_index: 0,
    };

    // Push 3 rows: (10, 100), (20, 200), (30, 300)
    let mut batch = ZSetBatch::new(&table_schema);
    for &(pk, val) in &[(10u64, 100i64), (20, 200), (30, 300)] {
        batch.pks.push_u128(pk as u128);
        batch.weights.push(1);
        batch.nulls.push(0);
        if let ColData::Fixed(buf) = &mut batch.columns[1] {
            buf.extend_from_slice(&val.to_le_bytes());
        }
    }
    client.push(tid, &table_schema, &batch).unwrap();

    let (_, data, _) = client.scan(tid).unwrap();
    let data = data.unwrap();
    assert_eq!(data.len(), 3, "expected 3 rows; got {}", data.len());

    let mut pks: Vec<u64> = data.pks.to_vec_u128().into_iter().map(|x| x as u64).collect();
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
        ColumnDef { name: "id".into(),    type_code: TypeCode::U64, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
        ColumnDef { name: "value".into(), type_code: TypeCode::I64, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
    ];
    let tid = client.create_table("s3", "t3", &cols, 0, true).unwrap();

    let table_schema = Schema {
        columns: cols,
        pk_index: 0,
    };

    // Push rows with pk 10, 11, 12
    let mut batch = ZSetBatch::new(&table_schema);
    for &(pk, val) in &[(10u64, 1i64), (11, 2), (12, 3)] {
        batch.pks.push_u128(pk as u128);
        batch.weights.push(1);
        batch.nulls.push(0);
        if let ColData::Fixed(buf) = &mut batch.columns[1] {
            buf.extend_from_slice(&val.to_le_bytes());
        }
    }
    client.push(tid, &table_schema, &batch).unwrap();

    // Delete middle row (pk=11)
    client.delete(tid, &table_schema, &[11u128]).unwrap();

    let (_, data, _) = client.scan(tid).unwrap();
    let data = data.unwrap();
    let positive: Vec<u64> = data.pks.to_vec_u128().into_iter().map(|x| x as u64)
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
        ColumnDef { name: "id".into(),    type_code: TypeCode::U64,    is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
        ColumnDef { name: "label".into(), type_code: TypeCode::String, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
        ColumnDef { name: "note".into(),  type_code: TypeCode::String, is_nullable: true, fk_table_id: 0, fk_col_idx: 0 },
    ];
    let tid = client.create_table("s4", "t4", &cols, 0, true).unwrap();

    let table_schema = Schema { columns: cols, pk_index: 0 };

    // Push 2 rows: (1, "hello", Some("world")) and (2, "foo", None)
    let mut batch = ZSetBatch::new(&table_schema);
    for &pk in &[1u64, 2u64] {
        batch.pks.push_u128(pk as u128);
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

    let (_, data, _) = client.scan(tid).unwrap();
    let data = data.unwrap();
    assert_eq!(data.len(), 2, "expected 2 rows");

    let idx1 = (0..data.pks.len()).position(|i| data.pks.get(i) as u64 == 1).expect("row pk=1 missing");
    if let ColData::Strings(labels) = &data.columns[1] {
        assert_eq!(labels[idx1].as_deref(), Some("hello"));
    }
    if let ColData::Strings(notes) = &data.columns[2] {
        assert_eq!(notes[idx1].as_deref(), Some("world"));
    }

    let idx2 = (0..data.pks.len()).position(|i| data.pks.get(i) as u64 == 2).expect("row pk=2 missing");
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
        ColumnDef { name: "id".into(),    type_code: TypeCode::U64,    is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
        ColumnDef { name: "name".into(),  type_code: TypeCode::String, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
        ColumnDef { name: "score".into(), type_code: TypeCode::F64,    is_nullable: true, fk_table_id: 0, fk_col_idx: 0 },
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

// --- Phase 5: circuit and expression builder tests ---

/// Read i64 from a Fixed ColData column at row i (stride = 8).
fn col_i64(col: &ColData, i: usize) -> i64 {
    match col {
        ColData::Fixed(b) => i64::from_le_bytes(b[i*8..(i+1)*8].try_into().unwrap()),
        _ => panic!("col_i64: expected Fixed"),
    }
}

#[test]
fn test_filter_view() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let client = GnitzClient::connect(&srv.sock_path).unwrap();

    client.create_schema("sv1").unwrap();
    let cols = vec![
        ColumnDef { name: "pk".into(),  type_code: TypeCode::U64, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
        ColumnDef { name: "val".into(), type_code: TypeCode::I64, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
    ];
    let tid = client.create_table("sv1", "ft1", &cols, 0, true).unwrap();

    // Build filter: val > 50
    let mut eb = ExprBuilder::new();
    let r0 = eb.load_col_int(1);  // col 1 = val
    let r1 = eb.load_const(50);
    let r2 = eb.cmp_gt(r0, r1);
    let expr = eb.build(r2);

    let vid = client.alloc_table_id().unwrap();
    let mut cb = CircuitBuilder::new(vid, tid);
    let inp = cb.input_delta();
    let f   = cb.filter(inp, Some(expr));
    cb.sink(f);
    let circuit = cb.build();

    let table_schema = Schema { columns: cols, pk_index: 0 };
    client.create_view_with_circuit("sv1", "filter_v", "", circuit, &table_schema.columns).unwrap();

    let mut batch = ZSetBatch::new(&table_schema);
    for &(pk, val) in &[(1u64, 10i64), (2, 30), (3, 50), (4, 70), (5, 90)] {
        batch.pks.push_u128(pk as u128);
        batch.weights.push(1);
        batch.nulls.push(0);
        if let ColData::Fixed(buf) = &mut batch.columns[1] {
            buf.extend_from_slice(&val.to_le_bytes());
        }
    }
    client.push(tid, &table_schema, &batch).unwrap();

    let (_, data, _) = client.scan(vid).unwrap();
    let data = data.unwrap();
    let mut positive_pks: Vec<u64> = data.pks.to_vec_u128().into_iter().map(|x| x as u64)
        .zip(data.weights.iter().copied())
        .filter(|(_, w)| *w > 0)
        .map(|(pk, _)| pk)
        .collect();
    positive_pks.sort_unstable();
    assert_eq!(positive_pks, vec![4, 5],
        "filter val>50 should keep pks 4 and 5; got {:?}", positive_pks);

    client.close();
}

#[test]
fn test_reduce_view() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let client = GnitzClient::connect(&srv.sock_path).unwrap();

    client.create_schema("sv2").unwrap();
    let cols = vec![
        ColumnDef { name: "pk".into(),       type_code: TypeCode::U64, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
        ColumnDef { name: "group_id".into(), type_code: TypeCode::I64, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
        ColumnDef { name: "val".into(),      type_code: TypeCode::I64, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
    ];
    let tid = client.create_table("sv2", "rt2", &cols, 0, true).unwrap();

    let vid = client.alloc_table_id().unwrap();
    let mut cb = CircuitBuilder::new(vid, tid);
    let inp = cb.input_delta();
    // group by col 1, SUM col 2; agg_func_id=2 (SUM)
    let red = cb.reduce(inp, &[1], 2, 2);
    cb.sink(red);
    let circuit = cb.build();

    let out_cols = vec![
        ColumnDef { name: "_group_hash".into(), type_code: TypeCode::U128, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
        ColumnDef { name: "group_id".into(),    type_code: TypeCode::I64,  is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
        ColumnDef { name: "agg".into(),         type_code: TypeCode::I64,  is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
    ];
    client.create_view_with_circuit("sv2", "reduce_v", "", circuit, &out_cols).unwrap();

    let table_schema = Schema { columns: cols, pk_index: 0 };
    let mut batch = ZSetBatch::new(&table_schema);
    // group=1: vals 10, 20 → sum=30
    // group=2: vals 30, 40 → sum=70
    for &(pk, gid, val) in &[(1u64, 1i64, 10i64), (2, 1, 20), (3, 2, 30), (4, 2, 40)] {
        batch.pks.push_u128(pk as u128);
        batch.weights.push(1);
        batch.nulls.push(0);
        if let ColData::Fixed(buf) = &mut batch.columns[1] { buf.extend_from_slice(&gid.to_le_bytes()); }
        if let ColData::Fixed(buf) = &mut batch.columns[2] { buf.extend_from_slice(&val.to_le_bytes()); }
    }
    client.push(tid, &table_schema, &batch).unwrap();

    let (_, data, _) = client.scan(vid).unwrap();
    let data = data.unwrap();

    let mut group1_sum: i64 = 0;
    let mut group2_sum: i64 = 0;
    for i in 0..data.len() {
        if data.weights[i] <= 0 { continue; }
        let gid = col_i64(&data.columns[1], i);
        let agg = col_i64(&data.columns[2], i);
        if gid == 1 { group1_sum += agg; }
        if gid == 2 { group2_sum += agg; }
    }
    assert_eq!(group1_sum, 30, "group1 sum should be 30; got {}", group1_sum);
    assert_eq!(group2_sum, 70, "group2 sum should be 70; got {}", group2_sum);

    client.close();
}

#[test]
fn test_join_view() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let client = GnitzClient::connect(&srv.sock_path).unwrap();

    client.create_schema("sv3").unwrap();
    let cols = vec![
        ColumnDef { name: "pk".into(),  type_code: TypeCode::U64, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
        ColumnDef { name: "val".into(), type_code: TypeCode::I64, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
    ];
    let table_schema = Schema { columns: cols, pk_index: 0 };
    let tid_a = client.create_table("sv3", "jt3a", &table_schema.columns, 0, true).unwrap();
    let tid_b = client.create_table("sv3", "jt3b", &table_schema.columns, 0, true).unwrap();

    // Pre-populate B with pk=1,2,3
    let mut batch_b = ZSetBatch::new(&table_schema);
    for &pk in &[1u64, 2, 3] {
        batch_b.pks.push_u128(pk as u128);
        batch_b.weights.push(1);
        batch_b.nulls.push(0);
        if let ColData::Fixed(buf) = &mut batch_b.columns[1] { buf.extend_from_slice(&0i64.to_le_bytes()); }
    }
    client.push(tid_b, &table_schema, &batch_b).unwrap();

    let vid = client.alloc_table_id().unwrap();
    let mut cb = CircuitBuilder::new(vid, tid_a);
    let inp    = cb.input_delta();
    let joined = cb.join(inp, tid_b);
    cb.sink(joined);
    let circuit = cb.build();

    let view_cols = vec![
        ColumnDef { name: "pk".into(),    type_code: TypeCode::U64, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
        ColumnDef { name: "val_a".into(), type_code: TypeCode::I64, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
        ColumnDef { name: "val_b".into(), type_code: TypeCode::I64, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
    ];
    client.create_view_with_circuit("sv3", "join_v", "", circuit, &view_cols).unwrap();

    // Push A rows: pk=1 (matches B), pk=4 (no match)
    let mut batch_a = ZSetBatch::new(&table_schema);
    for &pk in &[1u64, 4] {
        batch_a.pks.push_u128(pk as u128);
        batch_a.weights.push(1);
        batch_a.nulls.push(0);
        if let ColData::Fixed(buf) = &mut batch_a.columns[1] { buf.extend_from_slice(&0i64.to_le_bytes()); }
    }
    client.push(tid_a, &table_schema, &batch_a).unwrap();

    let (_, data, _) = client.scan(vid).unwrap();
    let data = data.unwrap();
    let positive_pks: Vec<u64> = data.pks.to_vec_u128().into_iter().map(|x| x as u64)
        .zip(data.weights.iter().copied())
        .filter(|(_, w)| *w > 0)
        .map(|(pk, _)| pk)
        .collect();
    assert!(positive_pks.contains(&1), "pk=1 should be in join output; got {:?}", positive_pks);
    assert!(!positive_pks.contains(&4), "pk=4 should not be in join output (no match in B)");

    client.close();
}

#[test]
fn test_anti_join_view() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let client = GnitzClient::connect(&srv.sock_path).unwrap();

    client.create_schema("sv4").unwrap();
    let cols = vec![
        ColumnDef { name: "pk".into(),  type_code: TypeCode::U64, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
        ColumnDef { name: "val".into(), type_code: TypeCode::I64, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
    ];
    let table_schema = Schema { columns: cols, pk_index: 0 };
    let tid_a = client.create_table("sv4", "ajt4a", &table_schema.columns, 0, true).unwrap();
    let tid_b = client.create_table("sv4", "ajt4b", &table_schema.columns, 0, true).unwrap();

    // Pre-populate B with pk=1,2
    let mut batch_b = ZSetBatch::new(&table_schema);
    for &pk in &[1u64, 2] {
        batch_b.pks.push_u128(pk as u128);
        batch_b.weights.push(1);
        batch_b.nulls.push(0);
        if let ColData::Fixed(buf) = &mut batch_b.columns[1] { buf.extend_from_slice(&0i64.to_le_bytes()); }
    }
    client.push(tid_b, &table_schema, &batch_b).unwrap();

    let vid = client.alloc_table_id().unwrap();
    let mut cb = CircuitBuilder::new(vid, tid_a);
    let inp  = cb.input_delta();
    let anti = cb.anti_join(inp, tid_b);
    cb.sink(anti);
    let circuit = cb.build();

    client.create_view_with_circuit("sv4", "antijoin_v", "", circuit, &table_schema.columns).unwrap();

    // Push A rows: pk=1,2,3; only pk=3 has no match in B
    let mut batch_a = ZSetBatch::new(&table_schema);
    for &pk in &[1u64, 2, 3] {
        batch_a.pks.push_u128(pk as u128);
        batch_a.weights.push(1);
        batch_a.nulls.push(0);
        if let ColData::Fixed(buf) = &mut batch_a.columns[1] { buf.extend_from_slice(&0i64.to_le_bytes()); }
    }
    client.push(tid_a, &table_schema, &batch_a).unwrap();

    let (_, data, _) = client.scan(vid).unwrap();
    let data = data.unwrap();
    let positive_pks: Vec<u64> = data.pks.to_vec_u128().into_iter().map(|x| x as u64)
        .zip(data.weights.iter().copied())
        .filter(|(_, w)| *w > 0)
        .map(|(pk, _)| pk)
        .collect();
    assert_eq!(positive_pks, vec![3],
        "anti-join should keep only pk=3 (no match in B); got {:?}", positive_pks);

    client.close();
}

#[test]
fn test_exchange_multi_worker() {
    let srv = match ServerHandle::start_n(4) { Some(s) => s, None => return };
    let client = GnitzClient::connect(&srv.sock_path).unwrap();

    client.create_schema("sv5").unwrap();
    let cols = vec![
        ColumnDef { name: "pk".into(), type_code: TypeCode::U64, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
    ];
    let tid = client.create_table("sv5", "et5", &cols, 0, true).unwrap();
    let table_schema = Schema { columns: cols, pk_index: 0 };

    // Push 1000 rows
    let mut batch = ZSetBatch::new(&table_schema);
    for pk in 1u64..=1000 {
        batch.pks.push_u128(pk as u128);
        batch.weights.push(1);
        batch.nulls.push(0);
    }
    client.push(tid, &table_schema, &batch).unwrap();

    let (_, data, _) = client.scan(tid).unwrap();
    let data = data.unwrap();
    let mut pks: Vec<u64> = data.pks.to_vec_u128().into_iter().map(|x| x as u64)
        .zip(data.weights.iter().copied())
        .filter(|(_, w)| *w > 0)
        .map(|(pk, _)| pk)
        .collect();
    pks.sort_unstable();
    assert_eq!(pks.len(), 1000, "expected 1000 rows; got {}", pks.len());
    assert_eq!(pks[0], 1);
    assert_eq!(pks[999], 1000);

    client.close();
}

#[test]
fn test_incremental_update() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let client = GnitzClient::connect(&srv.sock_path).unwrap();

    client.create_schema("sv6").unwrap();
    let table_schema = Schema {
        columns: vec![
            ColumnDef { name: "pk".into(),       type_code: TypeCode::U64, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
            ColumnDef { name: "group_id".into(), type_code: TypeCode::I64, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
            ColumnDef { name: "val".into(),      type_code: TypeCode::I64, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
        ],
        pk_index: 0,
    };
    let tid = client.create_table("sv6", "iu6", &table_schema.columns, 0, true).unwrap();

    let vid = client.alloc_table_id().unwrap();
    let mut cb = CircuitBuilder::new(vid, tid);
    let inp = cb.input_delta();
    let red = cb.reduce(inp, &[1], 2, 2);
    cb.sink(red);
    let circuit = cb.build();

    let out_cols = vec![
        ColumnDef { name: "_group_hash".into(), type_code: TypeCode::U128, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
        ColumnDef { name: "group_id".into(),    type_code: TypeCode::I64,  is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
        ColumnDef { name: "agg".into(),         type_code: TypeCode::I64,  is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
    ];
    client.create_view_with_circuit("sv6", "incr_v", "", circuit, &out_cols).unwrap();

    // Tick 1: push pk=1,2 (group=1, vals=10,20) and pk=3,4 (group=2, vals=30,40)
    let mut batch = ZSetBatch::new(&table_schema);
    for &(pk, gid, val) in &[(1u64, 1i64, 10i64), (2, 1, 20), (3, 2, 30), (4, 2, 40)] {
        batch.pks.push_u128(pk as u128);
        batch.weights.push(1);
        batch.nulls.push(0);
        if let ColData::Fixed(buf) = &mut batch.columns[1] { buf.extend_from_slice(&gid.to_le_bytes()); }
        if let ColData::Fixed(buf) = &mut batch.columns[2] { buf.extend_from_slice(&val.to_le_bytes()); }
    }
    client.push(tid, &table_schema, &batch).unwrap();

    // Verify tick 1: group1=30, group2=70
    let (_, data1, _) = client.scan(vid).unwrap();
    let data1 = data1.unwrap();
    let (mut g1, mut g2) = (0i64, 0i64);
    for i in 0..data1.len() {
        if data1.weights[i] <= 0 { continue; }
        let gid = col_i64(&data1.columns[1], i);
        let agg = col_i64(&data1.columns[2], i);
        if gid == 1 { g1 += agg; }
        if gid == 2 { g2 += agg; }
    }
    assert_eq!(g1, 30, "tick1 group1 should be 30; got {}", g1);
    assert_eq!(g2, 70, "tick1 group2 should be 70; got {}", g2);

    // Tick 2: retract pk=1 (group=1, val=10)
    client.delete(tid, &table_schema, &[1u128]).unwrap();

    // Verify tick 2: group1=20, group2=70 (unchanged)
    let (_, data2, _) = client.scan(vid).unwrap();
    let data2 = data2.unwrap();
    let (mut g1, mut g2) = (0i64, 0i64);
    for i in 0..data2.len() {
        if data2.weights[i] <= 0 { continue; }
        let gid = col_i64(&data2.columns[1], i);
        let agg = col_i64(&data2.columns[2], i);
        if gid == 1 { g1 += agg; }
        if gid == 2 { g2 += agg; }
    }
    assert_eq!(g1, 20, "tick2 group1 should be 20 after retraction; got {}", g1);
    assert_eq!(g2, 70, "tick2 group2 should be 70 (unchanged); got {}", g2);

    client.close();
}

// --- Bulk / large-scale tests ---

#[test]
fn test_bulk_filter() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let client = GnitzClient::connect(&srv.sock_path).unwrap();

    client.create_schema("bf1").unwrap();
    let cols = vec![
        ColumnDef { name: "pk".into(),  type_code: TypeCode::U64, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
        ColumnDef { name: "val".into(), type_code: TypeCode::I64, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
    ];
    let tid = client.create_table("bf1", "bft1", &cols, 0, true).unwrap();
    let table_schema = Schema { columns: cols, pk_index: 0 };

    // Build filter: val > 50_000
    let mut eb = ExprBuilder::new();
    let r0 = eb.load_col_int(1);   // col 1 = val
    let r1 = eb.load_const(50_000);
    let r2 = eb.cmp_gt(r0, r1);
    let expr = eb.build(r2);

    let vid = client.alloc_table_id().unwrap();
    let mut cb = CircuitBuilder::new(vid, tid);
    let inp = cb.input_delta();
    let f   = cb.filter(inp, Some(expr));
    cb.sink(f);
    let circuit = cb.build();
    client.create_view_with_circuit("bf1", "bfv1", "", circuit, &table_schema.columns).unwrap();

    // Push 100 000 rows: pk=i, val=i
    let mut batch = ZSetBatch::new(&table_schema);
    for pk in 1u64..=100_000 {
        batch.pks.push_u128(pk as u128);
        batch.weights.push(1);
        batch.nulls.push(0);
        if let ColData::Fixed(buf) = &mut batch.columns[1] {
            buf.extend_from_slice(&(pk as i64).to_le_bytes());
        }
    }
    client.push(tid, &table_schema, &batch).unwrap();

    let (_, data, _) = client.scan(vid).unwrap();
    let data = data.unwrap();
    let positive: Vec<u64> = data.pks.to_vec_u128().into_iter().map(|x| x as u64)
        .zip(data.weights.iter().copied())
        .filter(|(_, w)| *w > 0)
        .map(|(pk, _)| pk)
        .collect();
    assert_eq!(positive.len(), 50_000,
        "filter val>50000 should keep 50000 rows; got {}", positive.len());

    client.close();
}

#[test]
#[ignore]
fn test_bulk_exchange_multi_worker() {
    let srv = match ServerHandle::start_n(4) { Some(s) => s, None => return };
    let client = GnitzClient::connect(&srv.sock_path).unwrap();

    client.create_schema("bem1").unwrap();
    let cols = vec![
        ColumnDef { name: "pk".into(),  type_code: TypeCode::U64, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
        ColumnDef { name: "val".into(), type_code: TypeCode::I64, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
    ];
    let tid = client.create_table("bem1", "bemt1", &cols, 0, true).unwrap();
    let table_schema = Schema { columns: cols, pk_index: 0 };

    // Push 500 000 rows in a single batch
    let mut batch = ZSetBatch::new(&table_schema);
    for pk in 1u64..=500_000 {
        batch.pks.push_u128(pk as u128);
        batch.weights.push(1);
        batch.nulls.push(0);
        if let ColData::Fixed(buf) = &mut batch.columns[1] {
            buf.extend_from_slice(&(pk as i64).to_le_bytes());
        }
    }
    client.push(tid, &table_schema, &batch).unwrap();

    let (_, data, _) = client.scan(tid).unwrap();
    let data = data.unwrap();
    let positive_count = data.weights.iter().filter(|&&w| w > 0).count();
    assert_eq!(positive_count, 500_000,
        "expected 500000 rows after exchange fan-out; got {}", positive_count);

    client.close();
}

#[test]
fn test_left_join_view() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let client = GnitzClient::connect(&srv.sock_path).unwrap();

    client.create_schema("sv7").unwrap();
    let schema_a = Schema {
        columns: vec![
            ColumnDef { name: "pk".into(),    type_code: TypeCode::U64, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
            ColumnDef { name: "val_a".into(), type_code: TypeCode::I64, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
        ],
        pk_index: 0,
    };
    let schema_b = Schema {
        columns: vec![
            ColumnDef { name: "pk".into(),    type_code: TypeCode::U64, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
            ColumnDef { name: "val_b".into(), type_code: TypeCode::I64, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
        ],
        pk_index: 0,
    };
    let tid_a = client.create_table("sv7", "lj7a", &schema_a.columns, 0, true).unwrap();
    let tid_b = client.create_table("sv7", "lj7b", &schema_b.columns, 0, true).unwrap();

    // Pre-populate B: only pk=1 exists (pk=2 will be unmatched)
    let mut batch_b = ZSetBatch::new(&schema_b);
    batch_b.pks.push_u128(1u128);
    batch_b.weights.push(1);
    batch_b.nulls.push(0);
    if let ColData::Fixed(buf) = &mut batch_b.columns[1] { buf.extend_from_slice(&100i64.to_le_bytes()); }
    client.push(tid_b, &schema_b, &batch_b).unwrap();

    // Circuit: A_delta → left_join(B trace) → sink
    let vid = client.alloc_table_id().unwrap();
    let mut cb = CircuitBuilder::new(vid, tid_a);
    let inp = cb.input_delta();
    let joined = cb.left_join(inp, tid_b);
    cb.sink(joined);
    let circuit = cb.build();

    // Output: [pk: U64, val_a: I64, val_b: I64 nullable]
    let out_cols = vec![
        ColumnDef { name: "pk".into(),    type_code: TypeCode::U64, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
        ColumnDef { name: "val_a".into(), type_code: TypeCode::I64, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
        ColumnDef { name: "val_b".into(), type_code: TypeCode::I64, is_nullable: true,  fk_table_id: 0, fk_col_idx: 0 },
    ];
    client.create_view_with_circuit("sv7", "lj_v", "", circuit, &out_cols).unwrap();

    // Push A: pk=1 matches B (val_b=100), pk=2 has no match in B
    let mut batch_a = ZSetBatch::new(&schema_a);
    for &(pk, val_a) in &[(1u64, 10i64), (2, 20)] {
        batch_a.pks.push_u128(pk as u128);
        batch_a.weights.push(1);
        batch_a.nulls.push(0);
        if let ColData::Fixed(buf) = &mut batch_a.columns[1] { buf.extend_from_slice(&val_a.to_le_bytes()); }
    }
    client.push(tid_a, &schema_a, &batch_a).unwrap();

    let (_, data, _) = client.scan(vid).unwrap();
    let data = data.unwrap();

    // Both rows must appear (left join never drops left-side rows)
    let pks: Vec<u64> = (0..data.pks.len())
        .filter(|&i| data.weights[i] > 0)
        .map(|i| data.pks.get(i) as u64)
        .collect();
    assert!(pks.contains(&1), "left join must include matched pk=1; got {:?}", pks);
    assert!(pks.contains(&2), "left join must include unmatched pk=2; got {:?}", pks);

    // val_b is at logical column index 2; payload index = 2 - 1 = 1 → null bit 1
    let null_bit_val_b = 1u64 << 1;

    let idx1 = (0..data.pks.len())
        .position(|i| data.pks.get(i) as u64 == 1 && data.weights[i] > 0)
        .expect("row pk=1 missing");
    assert_eq!(data.nulls[idx1] & null_bit_val_b, 0, "pk=1: val_b must not be null");
    assert_eq!(col_i64(&data.columns[2], idx1), 100, "pk=1: val_b must be 100");

    let idx2 = (0..data.pks.len())
        .position(|i| data.pks.get(i) as u64 == 2 && data.weights[i] > 0)
        .expect("row pk=2 missing");
    assert_ne!(data.nulls[idx2] & null_bit_val_b, 0, "pk=2: val_b must be null (no match in B)");

    client.close();
}

#[test]
fn test_distinct_view() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let client = GnitzClient::connect(&srv.sock_path).unwrap();

    client.create_schema("sv8").unwrap();
    let table_schema = Schema {
        columns: vec![
            ColumnDef { name: "pk".into(),  type_code: TypeCode::U64, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
            ColumnDef { name: "val".into(), type_code: TypeCode::I64, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
        ],
        pk_index: 0,
    };
    let tid = client.create_table("sv8", "dt8", &table_schema.columns, 0, true).unwrap();

    // union(inp, inp) doubles all weights to 2; distinct reduces back to 1.
    let vid = client.alloc_table_id().unwrap();
    let mut cb = CircuitBuilder::new(vid, tid);
    let inp     = cb.input_delta();
    let doubled = cb.union(inp, inp);
    let deduped = cb.distinct(doubled);
    cb.sink(deduped);
    let circuit = cb.build();

    client.create_view_with_circuit("sv8", "distinct_v", "", circuit, &table_schema.columns).unwrap();

    let mut batch = ZSetBatch::new(&table_schema);
    for &(pk, val) in &[(1u64, 10i64), (2, 20), (3, 30)] {
        batch.pks.push_u128(pk as u128);
        batch.weights.push(1);
        batch.nulls.push(0);
        if let ColData::Fixed(buf) = &mut batch.columns[1] { buf.extend_from_slice(&val.to_le_bytes()); }
    }
    client.push(tid, &table_schema, &batch).unwrap();

    let (_, data, _) = client.scan(vid).unwrap();
    let data = data.unwrap();
    let positive: Vec<(u64, i64)> = (0..data.pks.len())
        .filter(|&i| data.weights[i] > 0)
        .map(|i| (data.pks.get(i) as u64, data.weights[i]))
        .collect();

    assert_eq!(positive.len(), 3, "distinct must keep all 3 rows; got {:?}", positive);
    for &(pk, w) in &positive {
        assert_eq!(w, 1, "distinct must reduce weight to 1 for pk={}; got weight={}", pk, w);
    }

    client.close();
}

#[test]
fn test_min_max_aggregate_view() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let client = GnitzClient::connect(&srv.sock_path).unwrap();

    client.create_schema("sv9").unwrap();
    let table_schema = Schema {
        columns: vec![
            ColumnDef { name: "pk".into(),       type_code: TypeCode::U64, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
            ColumnDef { name: "group_id".into(), type_code: TypeCode::I64, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
            ColumnDef { name: "val".into(),      type_code: TypeCode::I64, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
        ],
        pk_index: 0,
    };
    let tid = client.create_table("sv9", "ma9", &table_schema.columns, 0, true).unwrap();

    let agg_out_cols = |agg_name: &str| vec![
        ColumnDef { name: "_hash".into(),    type_code: TypeCode::U128, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
        ColumnDef { name: "group_id".into(), type_code: TypeCode::I64,  is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
        ColumnDef { name: agg_name.into(),   type_code: TypeCode::I64,  is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
    ];

    // MIN view (agg_func_id = 3)
    let vid_min = client.alloc_table_id().unwrap();
    {
        let mut cb = CircuitBuilder::new(vid_min, tid);
        let inp = cb.input_delta();
        let red = cb.reduce(inp, &[1], 3, 2);
        cb.sink(red);
        client.create_view_with_circuit("sv9", "min_v", "", cb.build(), &agg_out_cols("min_val")).unwrap();
    }

    // MAX view (agg_func_id = 4)
    let vid_max = client.alloc_table_id().unwrap();
    {
        let mut cb = CircuitBuilder::new(vid_max, tid);
        let inp = cb.input_delta();
        let red = cb.reduce(inp, &[1], 4, 2);
        cb.sink(red);
        client.create_view_with_circuit("sv9", "max_v", "", cb.build(), &agg_out_cols("max_val")).unwrap();
    }

    // group=1: vals [10, 5, 20] → min=5, max=20
    // group=2: vals [30, 25]    → min=25, max=30
    let mut batch = ZSetBatch::new(&table_schema);
    for &(pk, gid, val) in &[(1u64,1i64,10i64),(2,1,5),(3,1,20),(4,2,30),(5,2,25)] {
        batch.pks.push_u128(pk as u128);
        batch.weights.push(1);
        batch.nulls.push(0);
        if let ColData::Fixed(buf) = &mut batch.columns[1] { buf.extend_from_slice(&gid.to_le_bytes()); }
        if let ColData::Fixed(buf) = &mut batch.columns[2] { buf.extend_from_slice(&val.to_le_bytes()); }
    }
    client.push(tid, &table_schema, &batch).unwrap();

    let read_groups = |vid| -> (i64, i64) {
        let (_, data, _) = client.scan(vid).unwrap();
        let data = data.unwrap();
        let (mut g1, mut g2) = (i64::MAX, i64::MAX);
        for i in 0..data.pks.len() {
            if data.weights[i] <= 0 { continue; }
            let gid = col_i64(&data.columns[1], i);
            let agg = col_i64(&data.columns[2], i);
            if gid == 1 { g1 = agg; }
            if gid == 2 { g2 = agg; }
        }
        (g1, g2)
    };

    let (g1_min, g2_min) = read_groups(vid_min);
    assert_eq!(g1_min, 5,  "MIN group=1 should be 5; got {}", g1_min);
    assert_eq!(g2_min, 25, "MIN group=2 should be 25; got {}", g2_min);

    let (g1_max, g2_max) = read_groups(vid_max);
    assert_eq!(g1_max, 20, "MAX group=1 should be 20; got {}", g1_max);
    assert_eq!(g2_max, 30, "MAX group=2 should be 30; got {}", g2_max);

    client.close();
}
