use super::*;

// ── test_index_creation_and_backfill ────────────────────────────────────

#[test]
fn test_index_creation_and_backfill() {
    let dir = temp_dir("index_backfill");
    let mut engine = CatalogEngine::open(&dir).unwrap();

    let cols = vec![u64_col_def("id"), i64_col_def("val")];
    let tid = engine.create_table("public.tfanout", &cols, 0, true).unwrap();

    // Ingest 5 rows
    let schema = engine.get_schema(tid).unwrap();
    let mut bb = BatchBuilder::new(schema);
    for i in 0..5u64 {
        bb.begin_row(i, 0, 1);
        bb.put_u64((i * 100) as u64);
        bb.end_row();
    }
    let batch = bb.finish();
    engine.dag.ingest_to_family(tid, batch);
    let _ = engine.dag.flush(tid);

    // Create index
    let idx_id = engine.create_index("public.tfanout", "val", false).unwrap();
    assert!(engine.has_index_by_name("public__tfanout__idx_val"));

    // Drop index
    engine.drop_index("public__tfanout__idx_val").unwrap();
    assert!(!engine.has_index_by_name("public__tfanout__idx_val"));

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── test_index_live_fanout ────────────────────────────────────────────────

#[test]
fn test_index_live_fanout() {
    let dir = temp_dir("idx_fanout");
    let mut engine = CatalogEngine::open(&dir).unwrap();

    let cols = vec![u64_col_def("id"), i64_col_def("val")];
    let tid = engine.create_table("public.tfanout", &cols, 0, true).unwrap();
    let schema = engine.get_schema(tid).unwrap();

    // Ingest 5 rows
    let mut bb = BatchBuilder::new(schema);
    for i in 0..5u64 {
        bb.begin_row(i, 0, 1);
        bb.put_u64(i * 100);
        bb.end_row();
    }
    engine.dag.ingest_to_family(tid, bb.finish());
    let _ = engine.dag.flush(tid);

    // Create index — should backfill 5 rows
    engine.create_index("public.tfanout", "val", false).unwrap();

    // Live fan-out: ingest 1 more row via ingest_to_family (which does index projection)
    let mut bb2 = BatchBuilder::new(schema);
    bb2.begin_row(99, 0, 1);
    bb2.put_u64(777);
    bb2.end_row();
    engine.dag.ingest_to_family(tid, bb2.finish());
    let _ = engine.dag.flush(tid);

    // Verify index has 6 entries via DagEngine's index circuit
    let entry = engine.dag.tables.get_mut(&tid).unwrap();
    assert_eq!(entry.index_circuits.len(), 1, "Expected 1 index circuit");
    let idx_table = &mut *entry.index_circuits[0].index_table;
    let idx_count = count_records(idx_table);
    assert_eq!(idx_count, 6, "Index fanout: expected 6, got {}", idx_count);

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── validate_unique_indices tests ─────────────────────────────────

#[test]
fn test_validate_unique_indices_duplicate_value() {
    let dir = temp_dir("catalog_uidx_dup");
    let mut engine = CatalogEngine::open(&dir).unwrap();
    let cols = vec![u64_col_def("id"), u64_col_def("val")];
    let tid = engine.create_table("public.t", &cols, 0, false).unwrap();
    engine.create_index("public.t", "val", true).unwrap(); // unique index on val
    let schema = engine.get_schema(tid).unwrap();

    // Insert first row
    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(1, 0, 1); bb.put_u64(42); bb.end_row();
    engine.ingest_to_family(tid, &bb.finish()).unwrap();
    engine.flush_family(tid).unwrap();

    // Insert second row with same val=42 → should violate unique index
    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(2, 0, 1); bb.put_u64(42); bb.end_row();
    let result = engine.validate_unique_indices(tid, &bb.finish());
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("Unique index violation"));

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_validate_unique_indices_batch_internal_dup() {
    let dir = temp_dir("catalog_uidx_batch_dup");
    let mut engine = CatalogEngine::open(&dir).unwrap();
    let cols = vec![u64_col_def("id"), u64_col_def("val")];
    let tid = engine.create_table("public.t", &cols, 0, false).unwrap();
    engine.create_index("public.t", "val", true).unwrap();
    let schema = engine.get_schema(tid).unwrap();

    // Single batch with two rows sharing val=99 → duplicate in batch
    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(1, 0, 1); bb.put_u64(99); bb.end_row();
    bb.begin_row(2, 0, 1); bb.put_u64(99); bb.end_row();
    let result = engine.validate_unique_indices(tid, &bb.finish());
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("duplicate in batch"));

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_validate_unique_indices_upsert_same_value_ok() {
    let dir = temp_dir("catalog_uidx_upsert_same");
    let mut engine = CatalogEngine::open(&dir).unwrap();
    let cols = vec![u64_col_def("id"), u64_col_def("val")];
    let tid = engine.create_table("public.t", &cols, 0, true).unwrap(); // unique_pk
    engine.create_index("public.t", "val", true).unwrap();
    let schema = engine.get_schema(tid).unwrap();

    // Insert PK=1, val=42
    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(1, 0, 1); bb.put_u64(42); bb.end_row();
    engine.ingest_to_family(tid, &bb.finish()).unwrap();
    engine.flush_family(tid).unwrap();

    // UPSERT PK=1, val=42 (same value) → should succeed
    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(1, 0, 1); bb.put_u64(42); bb.end_row();
    let result = engine.validate_unique_indices(tid, &bb.finish());
    assert!(result.is_ok());

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_validate_unique_indices_upsert_to_existing_value_fails() {
    let dir = temp_dir("catalog_uidx_upsert_conflict");
    let mut engine = CatalogEngine::open(&dir).unwrap();
    let cols = vec![u64_col_def("id"), u64_col_def("val")];
    let tid = engine.create_table("public.t", &cols, 0, true).unwrap(); // unique_pk
    engine.create_index("public.t", "val", true).unwrap();
    let schema = engine.get_schema(tid).unwrap();

    // Insert PK=1, val=42 and PK=2, val=99
    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(1, 0, 1); bb.put_u64(42); bb.end_row();
    bb.begin_row(2, 0, 1); bb.put_u64(99); bb.end_row();
    engine.ingest_to_family(tid, &bb.finish()).unwrap();
    engine.flush_family(tid).unwrap();

    // UPSERT PK=1, val=99 → conflicts with PK=2's val=99
    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(1, 0, 1); bb.put_u64(99); bb.end_row();
    let result = engine.validate_unique_indices(tid, &bb.finish());
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("Unique index violation"));

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_validate_unique_indices_distinct_values_ok() {
    let dir = temp_dir("catalog_uidx_distinct");
    let mut engine = CatalogEngine::open(&dir).unwrap();
    let cols = vec![u64_col_def("id"), u64_col_def("val")];
    let tid = engine.create_table("public.t", &cols, 0, false).unwrap();
    engine.create_index("public.t", "val", true).unwrap();
    let schema = engine.get_schema(tid).unwrap();

    // Insert row with val=42
    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(1, 0, 1); bb.put_u64(42); bb.end_row();
    engine.ingest_to_family(tid, &bb.finish()).unwrap();
    engine.flush_family(tid).unwrap();

    // Insert row with different val=99 → should succeed
    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(2, 0, 1); bb.put_u64(99); bb.end_row();
    let result = engine.validate_unique_indices(tid, &bb.finish());
    assert!(result.is_ok());

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── seek_by_index tests ──────────────────────────────────────────

#[test]
fn test_seek_by_index_found() {
    let dir = temp_dir("catalog_seekidx_found");
    let mut engine = CatalogEngine::open(&dir).unwrap();
    let cols = vec![u64_col_def("id"), u64_col_def("val")];
    let tid = engine.create_table("public.t", &cols, 0, false).unwrap();
    engine.create_index("public.t", "val", false).unwrap();
    let schema = engine.get_schema(tid).unwrap();

    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(10, 0, 1); bb.put_u64(100); bb.end_row();
    bb.begin_row(20, 0, 1); bb.put_u64(200); bb.end_row();
    bb.begin_row(30, 0, 1); bb.put_u64(300); bb.end_row();
    engine.ingest_to_family(tid, &bb.finish()).unwrap();
    engine.flush_family(tid).unwrap();

    // Seek by index: val=200 → should find PK=20
    let result = engine.seek_by_index(tid, 1, 200, 0).unwrap();
    assert!(result.is_some());
    let row = result.unwrap();
    assert_eq!(row.count, 1);
    assert_eq!(row.get_pk(0), 20);

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_seek_by_index_not_found() {
    let dir = temp_dir("catalog_seekidx_miss");
    let mut engine = CatalogEngine::open(&dir).unwrap();
    let cols = vec![u64_col_def("id"), u64_col_def("val")];
    let tid = engine.create_table("public.t", &cols, 0, false).unwrap();
    engine.create_index("public.t", "val", false).unwrap();
    let schema = engine.get_schema(tid).unwrap();

    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(1, 0, 1); bb.put_u64(42); bb.end_row();
    engine.ingest_to_family(tid, &bb.finish()).unwrap();
    engine.flush_family(tid).unwrap();

    // Seek by index: val=999 → should return None
    let result = engine.seek_by_index(tid, 1, 999, 0).unwrap();
    assert!(result.is_none());

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── Regression: small-column index projection (bug #1) ────────────

#[test]
fn test_seek_by_index_u8_column() {
    let dir = temp_dir("catalog_seekidx_u8");
    let mut engine = CatalogEngine::open(&dir).unwrap();
    let cols = vec![u64_col_def("id"), u8_col_def("tag")];
    let tid = engine.create_table("public.t", &cols, 0, false).unwrap();
    engine.create_index("public.t", "tag", false).unwrap();
    let schema = engine.get_schema(tid).unwrap();

    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(10, 0, 1); bb.put_u8(42); bb.end_row();
    bb.begin_row(20, 0, 1); bb.put_u8(99); bb.end_row();
    engine.ingest_to_family(tid, &bb.finish()).unwrap();
    engine.flush_family(tid).unwrap();

    let result = engine.seek_by_index(tid, 1, 42, 0).unwrap();
    assert!(result.is_some(), "U8 index lookup must find the row");
    assert_eq!(result.unwrap().get_pk(0), 10);

    let result2 = engine.seek_by_index(tid, 1, 99, 0).unwrap();
    assert!(result2.is_some());
    assert_eq!(result2.unwrap().get_pk(0), 20);

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_seek_by_index_u16_column() {
    let dir = temp_dir("catalog_seekidx_u16");
    let mut engine = CatalogEngine::open(&dir).unwrap();
    let cols = vec![u64_col_def("id"), u16_col_def("port")];
    let tid = engine.create_table("public.t", &cols, 0, false).unwrap();
    engine.create_index("public.t", "port", false).unwrap();
    let schema = engine.get_schema(tid).unwrap();

    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(1, 0, 1); bb.put_u16(8080); bb.end_row();
    bb.begin_row(2, 0, 1); bb.put_u16(443); bb.end_row();
    engine.ingest_to_family(tid, &bb.finish()).unwrap();
    engine.flush_family(tid).unwrap();

    let result = engine.seek_by_index(tid, 1, 443, 0).unwrap();
    assert!(result.is_some(), "U16 index lookup must find the row");
    assert_eq!(result.unwrap().get_pk(0), 2);

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── Regression: signed key encoding consistency (bug #2) ────────────

#[test]
fn test_promote_to_index_key_matches_batch_project_for_signed() {
    // promote_to_index_key (multi-worker path) must produce the same
    // key as batch_project_index (storage path) for negative signed values.
    use crate::schema::promote_to_index_key;

    // I32 value -1 stored as little-endian 0xFFFFFFFF
    let data: [u8; 4] = (-1i32).to_le_bytes();
    let (lo, hi) = promote_to_index_key(&data, 0, 4, type_code::I32);

    // batch_project_index reads u32::from_le_bytes() as u64 → 0x00000000FFFFFFFF
    assert_eq!(lo, 0xFFFFFFFFu64, "I32 -1 must zero-extend, not sign-extend");
    assert_eq!(hi, 0);

    // I16 value -1 stored as little-endian 0xFFFF
    let data16: [u8; 2] = (-1i16).to_le_bytes();
    let (lo16, hi16) = promote_to_index_key(&data16, 0, 2, type_code::I16);
    assert_eq!(lo16, 0xFFFFu64);
    assert_eq!(hi16, 0);

    // I8 value -1 stored as 0xFF
    let data8: [u8; 1] = [0xFF];
    let (lo8, hi8) = promote_to_index_key(&data8, 0, 1, type_code::I8);
    assert_eq!(lo8, 0xFFu64);
    assert_eq!(hi8, 0);
}

#[test]
fn test_seek_by_index_i32_negative_value() {
    let dir = temp_dir("catalog_seekidx_i32_neg");
    let mut engine = CatalogEngine::open(&dir).unwrap();
    let cols = vec![u64_col_def("id"), i32_col_def("temp")];
    let tid = engine.create_table("public.t", &cols, 0, false).unwrap();
    engine.create_index("public.t", "temp", false).unwrap();
    let schema = engine.get_schema(tid).unwrap();

    // Insert a row with a negative I32 value
    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(1, 0, 1); bb.put_u32((-5i32) as u32); bb.end_row();
    bb.begin_row(2, 0, 1); bb.put_u32(10u32); bb.end_row();
    engine.ingest_to_family(tid, &bb.finish()).unwrap();
    engine.flush_family(tid).unwrap();

    // Seek negative value: key = zero-extended u32 representation of -5
    let neg5_key = (-5i32) as u32 as u64;
    let result = engine.seek_by_index(tid, 1, neg5_key, 0).unwrap();
    assert!(result.is_some(), "I32 negative index lookup must find the row");
    assert_eq!(result.unwrap().get_pk(0), 1);

    // Seek positive value
    let result2 = engine.seek_by_index(tid, 1, 10, 0).unwrap();
    assert!(result2.is_some());
    assert_eq!(result2.unwrap().get_pk(0), 2);

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_drop_table_cleans_up_indices() {
    let dir = temp_dir("drop_table_idx");
    let mut engine = CatalogEngine::open(&dir).unwrap();

    let cols = vec![u64_col_def("pk"), i64_col_def("val")];
    let tid = engine.create_table("public.idx_tbl", &cols, 0, true).unwrap();
    engine.create_index("public.idx_tbl", "val", true).unwrap();

    assert!(engine.dag.tables.contains_key(&tid));
    assert_eq!(engine.dag.tables.get(&tid).unwrap().index_circuits.len(), 1);

    // Drop index then table
    let idx_name = make_secondary_index_name("public", "idx_tbl", "val");
    engine.drop_index(&idx_name).unwrap();
    assert_eq!(engine.dag.tables.get(&tid).unwrap().index_circuits.len(), 0);

    engine.drop_table("public.idx_tbl").unwrap();
    assert!(!engine.dag.tables.contains_key(&tid));

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── Regression: drop_table must cascade to owned indices ─────────────
// Bug 1.3: drop_table used to only retract sys_tables + sys_columns,
// leaving sys_indices rows whose owner table no longer existed.  On the
// next restart, replay_catalog would fail with "Index: owner table N
// not found", leaving the database un-openable.

#[test]
fn test_drop_table_cascades_secondary_index() {
    let dir = temp_dir("drop_table_cascade_sec");
    let mut engine = CatalogEngine::open(&dir).unwrap();

    let cols = vec![u64_col_def("pk"), i64_col_def("val")];
    let tid = engine.create_table("public.cascade_tbl", &cols, 0, true).unwrap();
    engine.create_index("public.cascade_tbl", "val", false).unwrap();

    let idx_name = make_secondary_index_name("public", "cascade_tbl", "val");
    assert!(engine.caches.index_by_name.contains_key(idx_name.as_str()));
    let idx_records_before = count_records(&mut engine.sys_indices);
    assert!(idx_records_before >= 1);

    // Drop the table WITHOUT dropping the index first.
    engine.drop_table("public.cascade_tbl").unwrap();

    assert!(!engine.caches.index_by_name.contains_key(idx_name.as_str()),
            "in-memory index registry must forget the index");
    assert!(!engine.dag.tables.contains_key(&tid));
    assert_eq!(count_records(&mut engine.sys_indices), idx_records_before - 1,
               "sys_indices must retract exactly one record");

    // Reopen: before the fix, replay_catalog would fail here because
    // the orphaned sys_indices row references tid which no longer exists.
    engine.close();
    drop(engine);
    let mut engine2 = CatalogEngine::open(&dir).unwrap();
    assert!(!engine2.caches.index_by_name.contains_key(idx_name.as_str()));
    engine2.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_drop_table_cascades_fk_index() {
    let dir = temp_dir("drop_table_cascade_fk");
    let mut engine = CatalogEngine::open(&dir).unwrap();

    let parent_cols = vec![u64_col_def("pk"), u64_col_def("name")];
    let parent_tid = engine.create_table("public.parent", &parent_cols, 0, true).unwrap();

    let child_cols = vec![
        u64_col_def("pk"),
        ColumnDef {
            name: "parent_ref".into(),
            type_code: type_code::U64,
            is_nullable: false,
            fk_table_id: parent_tid,
            fk_col_idx: 0,
        },
    ];
    engine.create_table("public.child", &child_cols, 0, true).unwrap();

    let fk_idx_name = make_fk_index_name("public", "child", "parent_ref");
    assert!(engine.caches.index_by_name.contains_key(fk_idx_name.as_str()),
            "FK index must be auto-created with the child table");

    // drop_index refuses to drop __fk_ indices, so drop_table is the only
    // valid path for this cleanup. Before the fix: the on_index_delta
    // hook's __fk_ guard blocked drop_table from cascading.
    engine.drop_table("public.child").unwrap();
    assert!(!engine.caches.index_by_name.contains_key(fk_idx_name.as_str()),
            "FK index must be removed by drop_table cascade");

    // Reopen catalog — must succeed.
    engine.close();
    drop(engine);
    let mut engine2 = CatalogEngine::open(&dir).unwrap();
    assert!(!engine2.caches.index_by_name.contains_key(fk_idx_name.as_str()));
    engine2.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── Regression: create_index rollback must clean up all caches ────────
// If hook_index_register fails (e.g. unique index on a column with
// duplicate values), the earlier apply_index_by_{name,id} hooks have
// already mutated the caches. The rollback must reverse those writes
// or the name/id caches end up pointing at a ghost index.

#[test]
fn test_create_unique_index_duplicate_rolls_back_cleanly() {
    let dir = temp_dir("unique_idx_rollback");
    let mut engine = CatalogEngine::open(&dir).unwrap();

    let cols = vec![u64_col_def("id"), u64_col_def("val")];
    let tid = engine.create_table("public.t", &cols, 0, true).unwrap();
    let schema = engine.get_schema(tid).unwrap();

    // Seed duplicate values on `val` so a unique index over it cannot be built.
    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(1, 0, 1); bb.put_u64(42); bb.end_row();
    bb.begin_row(2, 0, 1); bb.put_u64(42); bb.end_row();
    engine.ingest_to_family(tid, &bb.finish()).unwrap();
    engine.flush_family(tid).unwrap();

    let idx_name = make_secondary_index_name("public", "t", "val");
    let idx_records_before = count_records(&mut engine.sys_indices);

    // Attempt should fail because of duplicate values.
    let result = engine.create_index("public.t", "val", true);
    assert!(result.is_err(), "unique index over duplicates must fail");

    // All catalog-visible state must have reverted: the ghost name/id
    // entries created by apply_index_by_{name,id} are the thing the
    // rollback is responsible for sweeping up.
    assert!(!engine.caches.index_by_name.contains_key(idx_name.as_str()),
            "index_by_name must not retain a ghost entry after rollback");
    assert!(!engine.dag.tables.get(&tid).unwrap()
                .index_circuits.iter().any(|ic| ic.col_idx == 1),
            "DAG must not retain a half-built index circuit");
    assert_eq!(count_records(&mut engine.sys_indices), idx_records_before,
               "sys_indices must net out to zero after rollback");

    // The failed attempt must not block a later successful, non-unique index
    // on the same column.
    engine.create_index("public.t", "val", false).unwrap();
    assert!(engine.caches.index_by_name.contains_key(idx_name.as_str()));

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}
