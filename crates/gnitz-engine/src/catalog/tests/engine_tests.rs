use super::*;

// ── test_enforce_unique_pk ───────────────────────────────────────────

/// Test unique_pk enforcement via a single-partition Table (system table range).
/// DagEngine.enforce_unique_pk only operates on Single stores; PartitionedTable
/// unique_pk is handled by the ingest layer.
#[test]
fn test_enforce_unique_pk() {
    let dir = temp_dir("enforce_upk");

    // Create a Table directly (Single store) to test enforce_unique_pk
    ensure_dir(&dir).unwrap();
    let schema = make_schema(&[u64_col(), u64_col()], 0); // id (PK), val
    let tdir = format!("{}/upk_table", dir);
    let mut table = Table::new(&tdir, "upk", schema, 100, SYS_TABLE_ARENA, false).unwrap();

    let mut dag = DagEngine::new();
    let table_ptr = &mut table as *mut Table;
    dag.register_table(100, StoreHandle::Borrowed(table_ptr), schema, 0, true, tdir.clone());

    let make_row = |pk: u64, val: u64, w: i64| -> Batch {
        let mut bb = BatchBuilder::new(schema);
        bb.begin_row(pk, 0, w);
        bb.put_u64(val);
        bb.end_row();
        bb.finish()
    };

    let scan = |table: &mut Table| -> usize {
        let cursor = table.create_cursor().unwrap();
        let mut c = cursor;
        let mut count = 0;
        while c.cursor.valid {
            if c.cursor.current_weight > 0 { count += 1; }
            c.cursor.advance();
        }
        count
    };

    // ST1: insert new PK
    dag.ingest_to_family(100, make_row(1, 10, 1));
    assert_eq!(scan(&mut table), 1);

    // ST2: insert different PK
    dag.ingest_to_family(100, make_row(2, 20, 1));
    assert_eq!(scan(&mut table), 2);

    // ST3: intra-batch duplicate — last value wins (enforce_unique_pk
    // handles intra-batch dedup correctly)
    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(5, 0, 1); bb.put_u64(10); bb.end_row();
    bb.begin_row(5, 0, 1); bb.put_u64(20); bb.end_row();
    dag.ingest_to_family(100, bb.finish());
    assert_eq!(scan(&mut table), 3, "Intra-batch dup: PK=5 has 1 net row");

    // ST4: intra-batch insert then delete cancel each other
    let mut bb2 = BatchBuilder::new(schema);
    bb2.begin_row(6, 0, 1); bb2.put_u64(10); bb2.end_row();
    bb2.begin_row(6, 0, -1); bb2.put_u64(10); bb2.end_row();
    dag.ingest_to_family(100, bb2.finish());
    // PK=6 cancelled, existing PKs (1,2,5) remain
    assert_eq!(scan(&mut table), 3, "Insert+delete cancel: PK=6 not added");

    // Note: cross-batch upsert (retract stored row + insert new) requires
    // the _enforce_unique_pk path which emits the stored retraction.
    // DagEngine.enforce_unique_pk calls retract_pk but doesn't emit
    // the retraction into the effective batch. Full-stack upsert is covered
    // by the E2E suite.

    dag.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── test_orphaned_metadata_recovery ─────────────────────────────────

#[test]
fn test_orphaned_metadata_recovery() {
    let dir = temp_dir("orphaned");

    // First open: inject an index record pointing to non-existent table 99999
    {
        let mut engine = CatalogEngine::open(&dir).unwrap();
        let idx_schema = idx_tab_schema();
        let mut bb = BatchBuilder::new(idx_schema);
        bb.begin_row(888, 0, 1);
        bb.put_u64(99999);   // owner_id (non-existent)
        bb.put_u64(OWNER_KIND_TABLE as u64);
        bb.put_u64(1);       // source_col_idx
        bb.put_string("orphaned_idx");
        bb.put_u64(0);       // is_unique
        bb.put_string("");   // cache_dir
        bb.end_row();
        ingest_batch_into(&mut engine.sys_indices, &bb.finish());
        let _ = engine.sys_indices.flush();
        engine.close();
        drop(engine);
    }

    // Re-open should fail because the orphaned index references table 99999
    let result = CatalogEngine::open(&dir);
    assert!(result.is_err(), "Orphaned index metadata should cause error on reload");

    let _ = fs::remove_dir_all(&dir);
}

// ── test_sequence_gap_recovery ───────────────────────────────────────

#[test]
fn test_sequence_gap_recovery() {
    let dir = temp_dir("seq_gap");
    let cols = vec![u64_col_def("id")];

    // First open: create a table, then inject a table record with high ID 250
    {
        let mut engine = CatalogEngine::open(&dir).unwrap();
        engine.create_table("public.t1", &cols, 0, true).unwrap();

        // Inject table record for tid=250 directly into sys_tables
        let tbl_schema = table_tab_schema();
        let mut bb = BatchBuilder::new(tbl_schema);
        bb.begin_row(250, 0, 1);
        bb.put_u64(PUBLIC_SCHEMA_ID as u64); // schema_id
        bb.put_string("gap_table");
        bb.put_string(&format!("{}/public/gap", dir));
        bb.put_u64(0); // pk_col_idx
        bb.put_u64(0); // created_lsn
        bb.put_u64(0); // flags
        bb.end_row();
        ingest_batch_into(&mut engine.sys_tables, &bb.finish());

        // Inject column record for tid=250
        let col_schema = col_tab_schema();
        let mut cbb = BatchBuilder::new(col_schema);
        let pk = pack_column_id(250, 0);
        cbb.begin_row(pk, 0, 1);
        cbb.put_u64(250);   // owner_id
        cbb.put_u64(OWNER_KIND_TABLE as u64);
        cbb.put_u64(0);     // col_idx
        cbb.put_string("id");
        cbb.put_u64(type_code::U64 as u64);
        cbb.put_u64(0);     // is_nullable
        cbb.put_u64(0);     // fk_table_id
        cbb.put_u64(0);     // fk_col_idx
        cbb.end_row();
        ingest_batch_into(&mut engine.sys_columns, &cbb.finish());

        let _ = engine.sys_tables.flush();
        let _ = engine.sys_columns.flush();
        engine.close();
        drop(engine);
    }

    // Re-open: sequence should recover to 251
    {
        let mut engine = CatalogEngine::open(&dir).unwrap();
        let new_tid = engine.create_table("public.tnext", &cols, 0, true).unwrap();
        assert_eq!(new_tid, 251, "Sequence recovery: expected 251, got {}", new_tid);
        engine.close();
    }

    let _ = fs::remove_dir_all(&dir);
}

// ── test_ingest_scan_seek_family ──────────────────────────────────────

#[test]
fn test_ingest_scan_seek_family() {
    let dir = temp_dir("catalog_ingest_scan_seek");
    let mut engine = CatalogEngine::open(&dir).unwrap();
    let cols = vec![u64_col_def("id"), u64_col_def("val")];
    let tid = engine.create_table("public.t", &cols, 0, false).unwrap();
    let schema = engine.get_schema(tid).unwrap();

    // Ingest via CatalogEngine (user table path)
    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(1, 0, 1); bb.put_u64(100); bb.end_row();
    bb.begin_row(2, 0, 1); bb.put_u64(200); bb.end_row();
    bb.begin_row(3, 0, 1); bb.put_u64(300); bb.end_row();
    engine.ingest_to_family(tid, &bb.finish()).unwrap();
    engine.flush_family(tid).unwrap();

    // Scan
    let scan_batch = engine.scan_family(tid).unwrap();
    assert_eq!(scan_batch.count, 3);

    // Seek existing
    let found = engine.seek_family(tid, 2, 0).unwrap();
    assert!(found.is_some());
    let row = found.unwrap();
    assert_eq!(row.count, 1);
    assert_eq!(row.get_pk(0), 2);

    // Seek missing
    let not_found = engine.seek_family(tid, 99, 0).unwrap();
    assert!(not_found.is_none());

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── test_ingest_unique_pk_partitioned ─────────────────────────────────

#[test]
fn test_ingest_unique_pk_partitioned() {
    let dir = temp_dir("catalog_unique_pk_partitioned");
    let mut engine = CatalogEngine::open(&dir).unwrap();
    let cols = vec![u64_col_def("id"), u64_col_def("val")];
    let tid = engine.create_table("public.t", &cols, 0, true).unwrap();
    let schema = engine.get_schema(tid).unwrap();

    // Insert row with PK=1, val=100
    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(1, 0, 1); bb.put_u64(100); bb.end_row();
    engine.ingest_to_family(tid, &bb.finish()).unwrap();
    engine.flush_family(tid).unwrap();

    // Insert row with PK=1 again, val=200 (should retract old + insert new)
    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(1, 0, 1); bb.put_u64(200); bb.end_row();
    engine.ingest_to_family(tid, &bb.finish()).unwrap();
    engine.flush_family(tid).unwrap();

    // Scan — should have exactly 1 row with val=200
    let scan = engine.scan_family(tid).unwrap();
    assert_eq!(scan.count, 1);
    assert_eq!(scan.get_pk(0), 1);

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── test_ddl_sync ─────────────────────────────────────────────────────

#[test]
fn test_ddl_sync() {
    let dir = temp_dir("catalog_ddl_sync");
    let mut engine = CatalogEngine::open(&dir).unwrap();

    // Create a schema via normal DDL
    engine.create_schema("app").unwrap();
    assert!(engine.has_schema("app"));

    // Simulate DDL sync: create batch mimicking a schema record
    let schema = schema_tab_schema();
    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(100, 0, 1); // sid=100
    bb.put_string("synced");
    bb.end_row();
    engine.ddl_sync(SCHEMA_TAB_ID, bb.finish()).unwrap();

    // Hooks should have registered the schema
    assert!(engine.has_schema("synced"));

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── test_raw_store_ingest ─────────────────────────────────────────────

#[test]
fn test_raw_store_ingest() {
    let dir = temp_dir("catalog_raw_store_ingest");
    let mut engine = CatalogEngine::open(&dir).unwrap();
    let cols = vec![u64_col_def("id"), u64_col_def("val")];
    let tid = engine.create_table("public.t", &cols, 0, false).unwrap();
    let schema = engine.get_schema(tid).unwrap();

    // Raw ingest (SAL recovery path — no hooks, no unique_pk, no index projection)
    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(10, 0, 1); bb.put_u64(1000); bb.end_row();
    bb.begin_row(20, 0, 1); bb.put_u64(2000); bb.end_row();
    engine.raw_store_ingest(tid, bb.finish()).unwrap();
    engine.flush_family(tid).unwrap();

    let scan = engine.scan_family(tid).unwrap();
    assert_eq!(scan.count, 2);

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── test_partition_management ─────────────────────────────────────────

#[test]
fn test_partition_management() {
    let dir = temp_dir("catalog_partition_mgmt");
    let mut engine = CatalogEngine::open(&dir).unwrap();
    let cols = vec![u64_col_def("id"), u64_col_def("val")];
    let _tid = engine.create_table("public.t", &cols, 0, false).unwrap();

    // These should not panic
    engine.set_active_partitions(0, 64);
    engine.trim_worker_partitions(0, 64);
    engine.invalidate_all_plans();

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── test_fk_index_metadata_queries ───────────────────────────────────

#[test]
fn test_fk_index_metadata_queries() {
    let dir = temp_dir("catalog_fk_idx_meta");
    let mut engine = CatalogEngine::open(&dir).unwrap();

    let cols = vec![u64_col_def("id"), u64_col_def("val")];
    let tid = engine.create_table("public.parent", &cols, 0, false).unwrap();

    // Create child table with FK to parent
    let child_cols = vec![
        u64_col_def("id"),
        ColumnDef {
            name: "parent_id".into(),
            type_code: crate::schema::type_code::U64,
            is_nullable: false,
            fk_table_id: tid,
            fk_col_idx: 0,
        },
    ];
    let child_tid = engine.create_table("public.child", &child_cols, 0, false).unwrap();

    // FK count
    assert!(engine.get_fk_count(child_tid) > 0);
    let (col_idx, target_id) = engine.get_fk_constraint(child_tid, 0).unwrap();
    assert_eq!(target_id, tid);

    // Create an explicit index
    let _iid = engine.create_index("public.parent", "val", false).unwrap();

    assert!(engine.get_index_circuit_count(tid) > 0);
    let (ic_col, _is_unique, _tc) = engine.get_index_circuit_info(tid, 0).unwrap();
    assert_eq!(ic_col, 1); // val is column 1

    // Index store handle should be non-null
    let idx_ptr = engine.get_index_store_handle(tid, 1);
    assert!(!idx_ptr.is_null());

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── test_iter_user_table_ids_and_lsn ────────────────────────────────

#[test]
fn test_iter_user_table_ids_and_lsn() {
    let dir = temp_dir("catalog_iter_lsn");
    let mut engine = CatalogEngine::open(&dir).unwrap();

    let cols = vec![u64_col_def("id"), u64_col_def("val")];
    let tid1 = engine.create_table("public.t1", &cols, 0, false).unwrap();
    let tid2 = engine.create_table("public.t2", &cols, 0, false).unwrap();

    let ids = engine.iter_user_table_ids();
    assert!(ids.contains(&tid1));
    assert!(ids.contains(&tid2));

    // LSN should be accessible without panicking
    let _ = engine.get_max_flushed_lsn(tid1);

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── Column name caching test ─────────────────────────────────────

#[test]
fn test_get_column_names_cached() {
    let dir = temp_dir("catalog_colnames");
    let mut engine = CatalogEngine::open(&dir).unwrap();
    let cols = vec![
        ColumnDef { name: "pk".into(), type_code: type_code::U64, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
        ColumnDef { name: "alpha".into(), type_code: type_code::U64, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
        ColumnDef { name: "beta".into(), type_code: type_code::U64, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
    ];
    let tid = engine.create_table("public.t", &cols, 0, false).unwrap();

    let names1 = engine.get_column_names(tid);
    assert_eq!(names1, vec!["pk", "alpha", "beta"]);

    // Second call should hit cache
    let names2 = engine.get_column_names(tid);
    assert_eq!(names2, vec!["pk", "alpha", "beta"]);

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}
