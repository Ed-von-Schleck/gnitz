use super::*;

// ── test_identifiers ─────────────────────────────────────────────────

#[test]
fn test_identifiers() {
    // Valid names
    for name in &["orders", "Orders123", "my_table", "a", "A1_b2", "1a", "99_problems"] {
        assert!(validate_user_identifier(name).is_ok(), "Rejected valid: {name}");
    }
    // Invalid names
    for name in &["_private", "_", "_system", "__init__", "", "has space",
                   "has-dash", "has.dot", "has@", "table$"] {
        assert!(validate_user_identifier(name).is_err(), "Accepted invalid: {name}");
    }
    // Qualified name parsing
    assert_eq!(parse_qualified_name("orders", "public"), ("public", "orders"));
    assert_eq!(parse_qualified_name("sales.orders", "public"), ("sales", "orders"));
    // Boundary slicing
    assert_eq!(parse_qualified_name(".table", "def"), ("", "table"));
    assert_eq!(parse_qualified_name("schema.", "def"), ("schema", ""));
}

// ── test_bootstrap ───────────────────────────────────────────────────

#[test]
fn test_bootstrap() {
    let dir = temp_dir("bootstrap");
    let mut engine = CatalogEngine::open(&dir).unwrap();

    assert!(engine.has_schema("_system"));
    assert!(engine.has_schema("public"));
    assert_eq!(engine.next_table_id, FIRST_USER_TABLE_ID);
    assert_eq!(engine.next_schema_id, FIRST_USER_SCHEMA_ID);

    let schemas_before = count_records(&mut engine.sys_schemas);
    let tables_before = count_records(&mut engine.sys_tables);

    engine.close();
    drop(engine); // Release WAL locks before re-open

    // Idempotent re-open: bootstrap must not duplicate records
    let mut engine2 = CatalogEngine::open(&dir).unwrap();
    assert_eq!(count_records(&mut engine2.sys_schemas), schemas_before);
    assert_eq!(count_records(&mut engine2.sys_tables), tables_before);
    engine2.close();

    let _ = fs::remove_dir_all(&dir);
}

// ── test_ddl ─────────────────────────────────────────────────────────

#[test]
fn test_ddl() {
    let dir = temp_dir("ddl");
    let mut engine = CatalogEngine::open(&dir).unwrap();

    let init_schemas = count_records(&mut engine.sys_schemas);
    let init_tables = count_records(&mut engine.sys_tables);
    let init_cols = count_records(&mut engine.sys_columns);

    // Schema creation
    engine.create_schema("sales").unwrap();
    assert!(engine.has_schema("sales"));
    assert_eq!(count_records(&mut engine.sys_schemas), init_schemas + 1);
    assert!(engine.create_schema("sales").is_err()); // duplicate

    // Table creation
    let cols = vec![u64_col_def("id"), str_col_def("name")];
    let tid = engine.create_table("sales.orders", &cols, &[0], true).unwrap();
    assert!(engine.has_id(tid));
    assert_eq!(count_records(&mut engine.sys_tables), init_tables + 1);
    assert_eq!(count_records(&mut engine.sys_columns), init_cols + 2);

    // Drop table (retractions)
    engine.drop_table("sales.orders").unwrap();
    assert!(!engine.has_id(tid));
    assert_eq!(count_records(&mut engine.sys_tables), init_tables);
    assert_eq!(count_records(&mut engine.sys_columns), init_cols);

    // System table drop should fail (identifier starts with '_')
    assert!(engine.drop_table("_system._columns").is_err());

    // Drop schema
    engine.create_schema("temp").unwrap();
    engine.drop_schema("temp").unwrap();
    assert!(!engine.has_schema("temp"));

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── test_edge_cases (26 cases) ────────────────────────────────────────

#[test]
fn test_edge_cases() {
    let dir = temp_dir("edge_cases");
    let mut engine = CatalogEngine::open(&dir).unwrap();
    let cols = vec![u64_col_def("id")];

    // 1. Drop non-existent schema
    assert!(engine.drop_schema("nonexistent").is_err());
    // 2. Create table in non-existent schema
    assert!(engine.create_table("nonexistent.tbl", &cols, &[0], true).is_err());
    // 3. Drop non-existent table
    assert!(engine.drop_table("public.nonexistent").is_err());
    // 4. Duplicate table
    engine.create_table("public.tbl1", &cols, &[0], true).unwrap();
    assert!(engine.create_table("public.tbl1", &cols, &[0], true).is_err());

    // 5. Drop non-empty schema cascades (PostgreSQL-style): the
    //    contained table goes first, then the schema itself.
    engine.create_schema("my_schema").unwrap();
    engine.create_table("my_schema.tbl2", &cols, &[0], true).unwrap();
    engine.drop_schema("my_schema").unwrap();
    assert!(!engine.has_schema("my_schema"));
    assert!(engine.get_by_name("my_schema", "tbl2").is_none(),
        "cascade must drop the contained table");

    // 7. Unqualified name defaults to public
    let _tid7 = engine.create_table("tbl3", &cols, &[0], true).unwrap();
    assert!(engine.get_by_name("public", "tbl3").is_some());
    engine.drop_table("public.tbl3").unwrap();

    // 8. Unqualified drop
    engine.create_table("public.tbl4", &cols, &[0], true).unwrap();
    engine.drop_table("tbl4").unwrap();
    assert!(engine.get_by_name("public", "tbl4").is_none());

    // 9. Invalid PK type (STRING)
    assert!(engine.create_table("public.bad_pk",
        &[ColumnDef { name: "id".into(), type_code: type_code::STRING, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 }],
        &[0], true).is_err());

    // 10. Too many columns (> MAX_COLUMNS = 65)
    let many: Vec<ColumnDef> = (0..66).map(|i| u64_col_def(&format!("c{i}"))).collect();
    assert!(engine.create_table("public.too_many", &many, &[0], true).is_err());

    // 11. Drop system schema
    assert!(engine.drop_schema("_system").is_err());

    // 12. PK index out of bounds
    assert!(engine.create_table("public.bad_idx", &cols, &[5], true).is_err());

    // 13. Recreated schema gets new ID
    engine.create_schema("temp").unwrap();
    let sid1 = engine.get_schema_id("temp");
    engine.drop_schema("temp").unwrap();
    engine.create_schema("temp").unwrap();
    let sid2 = engine.get_schema_id("temp");
    assert_ne!(sid1, sid2);
    engine.drop_schema("temp").unwrap();

    // 14. Recreated table gets new ID
    let tid14a = engine.create_table("public.tbl_rc", &cols, &[0], true).unwrap();
    engine.drop_table("public.tbl_rc").unwrap();
    let tid14b = engine.create_table("public.tbl_rc", &cols, &[0], true).unwrap();
    assert_ne!(tid14a, tid14b);
    engine.drop_table("public.tbl_rc").unwrap();

    // 15. U128 PK support
    let tid15 = engine.create_table("public.u128t", &[u128_col_def("uuid_pk"), str_col_def("data")], &[0], true).unwrap();
    let s15 = engine.get_schema(tid15).unwrap();
    assert_eq!(s15.columns[0].type_code, type_code::U128);
    engine.drop_table("public.u128t").unwrap();

    // 18. schema_is_empty
    engine.create_schema("empty_test").unwrap();
    assert!(engine.schema_is_empty("empty_test"));
    engine.create_table("empty_test.tbl", &cols, &[0], true).unwrap();
    assert!(!engine.schema_is_empty("empty_test"));
    engine.drop_table("empty_test.tbl").unwrap();
    assert!(engine.schema_is_empty("empty_test"));
    engine.drop_schema("empty_test").unwrap();

    // 19. Case sensitivity
    engine.create_table("public.CaseTest", &cols, &[0], true).unwrap();
    engine.create_table("public.casetest", &cols, &[0], true).unwrap();
    assert!(engine.get_by_name("public", "CaseTest").is_some());
    assert!(engine.get_by_name("public", "casetest").is_some());
    engine.drop_table("public.CaseTest").unwrap();
    engine.drop_table("public.casetest").unwrap();

    // 24. Invalid schema ID lookup
    assert_eq!(engine.get_schema_name_by_id(999999), "");
    assert_eq!(engine.get_schema_id("nonexistent"), -1);

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── test_drop_schema_cascade_with_view ──────────────────────────────────
// Exercises views_by_schema cache and the cache-based collect_schema_members:
// schema_is_empty must return false when a view (not just a table) exists,
// and drop_schema must collect and drop the view before the table.

// `test_drop_schema_cascade_with_view` and `test_schema_is_empty_view_only`
// previously relied on `engine.create_view(qname, &CircuitGraph, sql)` —
// removed alongside the circuit-graph schema redesign. Equivalent end-to-end
// coverage runs through the wire path in the Python E2E suite.

// ── test_unique_pk_metadata ──────────────────────────────────────────

#[test]
fn test_unique_pk_metadata() {
    let dir = temp_dir("unique_pk_meta");
    let cols = vec![u64_col_def("id"), str_col_def("val")];

    {
        let mut engine = CatalogEngine::open(&dir).unwrap();
        engine.create_schema("sales").unwrap();
        // default = unique_pk=true
        let tid1 = engine.create_table("sales.u_default", &cols, &[0], true).unwrap();
        assert!(engine.table_has_unique_pk(tid1));
        // explicit false
        let tid2 = engine.create_table("sales.u_off", &cols, &[0], false).unwrap();
        assert!(!engine.table_has_unique_pk(tid2));
        // For restart test
        engine.create_table("sales.u_restart", &cols, &[0], true).unwrap();
        engine.close();
    }

    // Verify survival across restart
    {
        let mut engine = CatalogEngine::open(&dir).unwrap();
        let tid_restart = engine.get_by_name("sales", "u_restart").unwrap();
        assert!(engine.table_has_unique_pk(tid_restart));
        let tid_off = engine.get_by_name("sales", "u_off").unwrap();
        assert!(!engine.table_has_unique_pk(tid_off));
        engine.close();
    }

    let _ = fs::remove_dir_all(&dir);
}

// ── test_restart (with data + sequence recovery) ─────────────────────

#[test]
fn test_restart_full() {
    let dir = temp_dir("restart_full");
    let cols = vec![u64_col_def("id"), str_col_def("name")];
    let first_tid;

    {
        let mut engine = CatalogEngine::open(&dir).unwrap();
        engine.create_schema("marketing").unwrap();
        first_tid = engine.create_table("marketing.products", &cols, &[0], true).unwrap();

        // Dropped entities should not reappear
        engine.create_schema("trash").unwrap();
        engine.create_table("trash.items", &cols, &[0], true).unwrap();
        engine.drop_table("trash.items").unwrap();
        engine.drop_schema("trash").unwrap();

        engine.close();
    }

    {
        let mut engine = CatalogEngine::open(&dir).unwrap();
        assert!(engine.has_schema("marketing"));
        assert!(engine.get_by_name("marketing", "products").is_some());
        // Dropped should stay gone
        assert!(!engine.has_schema("trash"));
        assert!(engine.get_by_name("trash", "items").is_none());
        // Schema layout rebuilt correctly
        let tid = engine.get_by_name("marketing", "products").unwrap();
        let schema = engine.get_schema(tid).unwrap();
        assert_eq!(schema.num_columns(), 2);
        // Sequence recovery: new table should get higher ID
        let new_tid = engine.create_table("marketing.other", &cols, &[0], true).unwrap();
        assert!(new_tid > first_tid, "Allocator sequence recovery failed: {new_tid} <= {first_tid}");
        engine.close();
    }

    let _ = fs::remove_dir_all(&dir);
}

/// Regression: verify long strings (> 12 bytes, out-of-line blob) survive
/// restart via copy_cursor_row_to_batch blob offset rewriting.
#[test]
fn test_restart_long_strings() {
    let dir = temp_dir("restart_long_str");
    let long_name = "this_is_a_very_long_table_name_exceeding_inline_threshold";
    assert!(long_name.len() > crate::schema::SHORT_STRING_THRESHOLD);

    {
        let mut engine = CatalogEngine::open(&dir).unwrap();
        engine.create_schema("longtest").unwrap();
        let cols = vec![u64_col_def("id"), str_col_def(long_name)];
        engine.create_table("longtest.tbl", &cols, &[0], true).unwrap();
        engine.close();
        drop(engine);
    }
    {
        let mut engine = CatalogEngine::open(&dir).unwrap();
        assert!(engine.get_by_name("longtest", "tbl").is_some());
        // Verify column name survived out-of-line blob round-trip
        let tid = engine.get_by_name("longtest", "tbl").unwrap();
        let col_defs = engine.read_column_defs(tid);
        assert_eq!(col_defs.len(), 2);
        assert_eq!(col_defs[1].name, long_name, "Long column name corrupted after restart");
        engine.close();
    }
    let _ = fs::remove_dir_all(&dir);
}

// ── Additional edge cases ───────────────────────────────────────────

#[test]
fn test_edge_cases_extended() {
    let dir = temp_dir("edge_ext");
    let mut engine = CatalogEngine::open(&dir).unwrap();
    let cols = vec![u64_col_def("id")];

    // #16. Multiple dots in qualified name — second part contains dot
    assert!(engine.create_table("public.schema.tbl", &cols, &[0], true).is_err());

    // #17. get_by_name on non-existent returns None
    assert!(engine.get_by_name("public", "nonexistent").is_none());

    // #20. has_id / get_schema for valid and invalid IDs
    let tid = engine.create_table("public.reg_test", &cols, &[0], true).unwrap();
    assert!(engine.has_id(tid));
    assert!(engine.get_schema(tid).is_some());
    assert!(!engine.has_id(999999));
    assert!(engine.get_schema(999999).is_none());
    engine.drop_table("public.reg_test").unwrap();

    // #26. Creating a user table in _system schema should fail
    // (_system identifier starts with '_' → rejected by validate_user_identifier)
    assert!(engine.create_table("_system.new_tbl", &cols, &[0], true).is_err());

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── test_nullable_pk_rejected ─────────────────────────────────────────
// The PK region has no null bitmap (foundations.md §6); the catalog must
// refuse to record a nullable PK regardless of its type.

#[test]
fn test_nullable_pk_rejected() {
    let dir = temp_dir("nullable_pk");
    let mut engine = CatalogEngine::open(&dir).unwrap();

    let cols = vec![
        ColumnDef {
            name: "id".into(),
            type_code: type_code::U64,
            is_nullable: true,
            fk_table_id: 0,
            fk_col_idx:  0,
        },
        str_col_def("name"),
    ];
    let err = engine.create_table("public.bad_pk_null", &cols, &[0], true).unwrap_err();
    assert!(err.contains("nullable"), "expected nullable-PK error, got: {err}");

    // Sanity: same shape with is_nullable=false succeeds.
    let cols_ok = vec![u64_col_def("id"), str_col_def("name")];
    engine.create_table("public.ok_pk", &cols_ok, &[0], true).unwrap();

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── test_hook_table_register_rejects_malformed_pk ────────────────────

// Drives crafted/malformed packed PK values through the production
// wire-ingest path (`ingest_to_family` → `fire_hooks` →
// `hook_table_register`) and asserts each is rejected with an `Err`
// rather than panicking the server via a `SchemaDescriptor::new`
// `assert!`.
#[test]
fn test_hook_table_register_rejects_malformed_pk() {
    let dir = temp_dir("pk_reject");
    let mut engine = CatalogEngine::open(&dir).unwrap();

    // Columns: [c0 U64 non-null, c1 STRING non-null, c2 U64 nullable, c3 F32 non-null].
    let col_defs = vec![
        u64_col_def("c0"),
        str_col_def("c1"),
        ColumnDef { name: "c2".into(), type_code: type_code::U64, is_nullable: true, fk_table_id: 0, fk_col_idx: 0 },
        ColumnDef { name: "c3".into(), type_code: type_code::F32, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
    ];
    let tid = engine.allocate_table_id();
    engine.write_column_records(tid, OWNER_KIND_TABLE, &col_defs).unwrap();

    let mut assert_rejects = |raw_pk_cols: u64, snippet: &str| {
        let batch = build_table_tab_row(&dir, tid, raw_pk_cols, "bad_table");
        let res = engine.ingest_to_family(TABLE_TAB_ID, &batch);
        let err = res.expect_err(&format!("expected Err containing '{snippet}', got Ok"));
        assert!(err.contains(snippet), "expected '{snippet}', got: {err}");
    };

    // Count out of range (decoded count, not clamped slice length).
    assert_rejects(PK_LIST_PACKED_FLAG, "out of range 1..=4");          // count 0
    assert_rejects(PK_LIST_PACKED_FLAG | 5, "out of range 1..=4");      // count 5
    assert_rejects(PK_LIST_PACKED_FLAG | 15, "out of range 1..=4");     // count 15
    // Out-of-bounds index (index 5, only 3 columns).
    assert_rejects(PK_LIST_PACKED_FLAG | 1 | (5 << 4), "out of bounds");
    // Duplicate index [0, 0]. The `(0 << N)` forms document the slot layout
    // (count=2, idx0@bit4=0, idx1@bit11=0) even though they evaluate to 0.
    #[allow(clippy::identity_op)]
    let packed = PK_LIST_PACKED_FLAG | 2 | (0u64 << 4) | (0u64 << 11);
    assert_rejects(packed, "duplicate column");
    // Non-integer PK column (c1 is STRING).
    assert_rejects(PK_LIST_PACKED_FLAG | 1 | (1 << 4), "must be a fixed-width integer");
    // Float PK column (c3 is F32) — floats break the byte-equal PK contract.
    assert_rejects(PK_LIST_PACKED_FLAG | 1 | (3 << 4), "must be a fixed-width integer");
    // Nullable PK column (c2 is nullable).
    assert_rejects(PK_LIST_PACKED_FLAG | 1 | (2 << 4), "must not be nullable");

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── test_pk_change_invalidates_col_name_cache ────────────────────────

// `apply_pk_col_of` fires before `hook_table_register` and invalidates
// the column-name cache only when a table's PK column assignment
// changes. An identical retract/reinsert (same PK column) must NOT
// invalidate. Also pins PK-list reconstruction through `create_table`.
#[test]
fn test_pk_change_invalidates_col_name_cache() {
    let dir = temp_dir("pk_cache_inval");
    let mut engine = CatalogEngine::open(&dir).unwrap();

    let cols = vec![u64_col_def("a"), u64_col_def("b")];
    let tid = engine.create_table("public.t", &cols, &[0], true).unwrap();

    // Populate the col-name cache.
    let _ = engine.get_column_names(tid);
    assert!(engine.caches.col_names.contains_key(&tid));

    let ingest_pk = |engine: &mut CatalogEngine, raw_pk_cols: u64| {
        let batch = build_table_tab_row(&dir, tid, raw_pk_cols, "t");
        engine.ingest_to_family(TABLE_TAB_ID, &batch).unwrap();
    };

    // Identical retract/reinsert (same PK column 0): no invalidation.
    ingest_pk(&mut engine, pack_pk_cols(&[0]));
    assert!(
        engine.caches.col_names.contains_key(&tid),
        "identical PK reinsert must not invalidate the col-name cache",
    );

    // PK column changes 0 → 1: invalidation.
    ingest_pk(&mut engine, pack_pk_cols(&[1]));
    assert!(
        !engine.caches.col_names.contains_key(&tid),
        "PK column change must invalidate the col-name cache",
    );

    // Drop and recreate with a different PK column; the reconstructed
    // schema must reflect the new PK list.
    engine.drop_table("public.t").unwrap();
    let tid2 = engine.create_table("public.t", &cols, &[1], true).unwrap();
    let schema = engine.get_schema(tid2).unwrap();
    assert_eq!(schema.pk_indices(), &[1]);

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── test_apply_pk_col_of_round_trips_compound_list ──────────────────

// `apply_pk_col_of` must store the full PK column list in the cache,
// not just the first element.
#[test]
fn test_apply_pk_col_of_round_trips_compound_list() {
    let dir = temp_dir("pk_col_of_roundtrip");
    let mut engine = CatalogEngine::open(&dir).unwrap();

    // Single-column PK: cache entry equals PkColList::single(idx).
    let single_tid: i64 = 9001;
    let batch_single = build_table_tab_row(&dir, single_tid, pack_pk_cols(&[2]), "t");
    engine.apply_pk_col_of(TABLE_TAB_ID, &batch_single).unwrap();
    assert_eq!(
        engine.caches.pk_col_of.get(&single_tid).copied(),
        Some(PkColList::single(2)),
    );

    // Compound PK lists 2..=4 round-trip element-for-element.
    for (tid_offset, pk_cols) in [
        (0i64, vec![0u32, 1]),
        (1,    vec![0u32, 3, 5]),
        (2,    vec![1u32, 2, 7, 11]),
    ] {
        let tid: i64 = 9100 + tid_offset;
        let batch = build_table_tab_row(&dir, tid, pack_pk_cols(&pk_cols), "t");
        engine.apply_pk_col_of(TABLE_TAB_ID, &batch).unwrap();
        let stored = engine.caches.pk_col_of.get(&tid)
            .copied().expect("pk_col_of entry must exist after apply");
        assert_eq!(stored.as_slice(), pk_cols.as_slice(),
            "compound PK list must round-trip in full (input={pk_cols:?})");
    }

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── test_drop_view_removes_directory ─────────────────────────────────
// DROP VIEW must delete the view's on-disk scratch directory, not just the
// logical catalog rows. Once the catalog entry is gone no later DROP could
// target the leaked directory, so the cleanup has to happen inside drop_view.

#[test]
fn test_drop_view_removes_directory() {
    let dir = temp_dir("drop_view_dir");
    let mut engine = CatalogEngine::open(&dir).unwrap();

    // A view needs a base table to reference.
    let base_cols = vec![u64_col_def("id")];
    engine.create_table("public.base", &base_cols, &[0], true).unwrap();

    // Register a view via the raw system-table path (create_view was removed).
    // Column records must precede the VIEW_TAB row (hook invariant).
    let vid = engine.next_table_id;
    let view_cols = vec![u64_col_def("id")];
    engine.write_column_records(vid, OWNER_KIND_VIEW, &view_cols).unwrap();

    let batch = build_view_tab_row(vid, "myview", "SELECT id FROM base");
    engine.ingest_to_family(VIEW_TAB_ID, &batch).unwrap();

    // The register hook created the physical view directory on disk.
    let view_dir = engine.dag.tables.get(&vid)
        .expect("view registered in dag")
        .directory.clone();
    assert!(std::path::Path::new(&view_dir).exists(),
        "view dir should exist after create: {view_dir}");

    // Drop removes catalog metadata and queues the physical directory into
    // pending_dir_deletions (deferred so the rmdir can't race the WAL fsync).
    // In production the DDL executor drains the queue once the zone is durable;
    // drive that step directly here.
    engine.drop_view("public.myview").unwrap();
    engine.drain_pending_dir_deletions();
    assert!(!std::path::Path::new(&view_dir).exists(),
        "view dir must be deleted on drop: {view_dir}");

    // No double-drop: the view is gone from the catalog.
    assert!(engine.drop_view("public.myview").is_err(),
        "second drop must error");

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── test_drop_view_cascades_columns_and_view_deps ────────────────────
// DROP VIEW retracts only the VIEW_TAB row; the cascade in hook_view_register
// must clean up BOTH the view's sys_columns rows and its sys_view_deps rows.
// The sys_view_deps assertion is the regression guard for removing drop_view's
// manual dep retraction — it proves the VIEW_TAB cascade
// (cascade_retract_circuit_and_deps) still clears the dep rows on its own.

#[test]
fn test_drop_view_cascades_columns_and_view_deps() {
    let dir = temp_dir("drop_view_cascade");
    let mut engine = CatalogEngine::open(&dir).unwrap();

    let base_tid = engine.create_table("public.base", &[u64_col_def("id")], &[0], true).unwrap();

    // Baseline: system + base-table column rows; no view-dep rows yet.
    let base_cols = count_records(&mut engine.sys_columns);
    let base_deps = count_records(&mut engine.sys_view_deps);

    // Register a view (column records precede the VIEW_TAB row) and a
    // dependency row on the base table.
    let vid = engine.next_table_id;
    let view_cols = vec![u64_col_def("id")];
    engine.write_column_records(vid, OWNER_KIND_VIEW, &view_cols).unwrap();

    let batch = build_view_tab_row(vid, "depview", "SELECT id FROM base");
    engine.ingest_to_family(VIEW_TAB_ID, &batch).unwrap();

    engine.write_view_deps(vid, &[base_tid]).unwrap();

    assert!(count_records(&mut engine.sys_columns) > base_cols,
        "view column rows must be present before drop");
    assert!(count_records(&mut engine.sys_view_deps) > base_deps,
        "view-dep row must be present before drop");

    // Drop the view: the VIEW_TAB -1 cascade must retract both families.
    engine.drop_view("public.depview").unwrap();
    engine.drain_pending_dir_deletions();

    assert_eq!(count_records(&mut engine.sys_columns), base_cols,
        "sys_columns must return to baseline after drop_view (column cascade)");
    assert_eq!(count_records(&mut engine.sys_view_deps), base_deps,
        "sys_view_deps must return to baseline after drop_view (dep cascade)");

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── ddl_emitters_use_no_raw_handle_capability ────────────────────────
// Capability guard: the DDL emitters mutate catalog state only through
// submit / submit_local / submit_retraction. They must never name a `sys_*`
// handle directly — no `ingest_batch_into`, no `sys_table_mut`, no direct
// `self.sys_*` field access. A violation reintroduces the fused capability the
// applier/emitter split removed, so it fails here.

#[test]
fn ddl_emitters_use_no_raw_handle_capability() {
    let src = include_str!("../ddl.rs");
    assert!(!src.contains("ingest_batch_into"),
        "ddl.rs must not call ingest_batch_into — route writes through submit/submit_local");
    assert!(!src.contains("sys_table_mut"),
        "ddl.rs must not call sys_table_mut — the emitters cannot name a sys_* handle");
    assert!(!src.contains("self.sys_"),
        "ddl.rs must not touch a sys_* handle directly — emit a delta via submit instead");
}

// ── test_view_backfill_chunked_matches_unchunked ──────────────────────────
// backfill_view streams the source through drain_chunk-sized epochs; the
// view contents must equal the base table row-for-row regardless of the
// chunk size, including long STRING payloads (each chunk relocates its
// strings into a fresh blob arena, so a chunked scan exercises cross-chunk
// blob handling that a whole-table materialization never did).

#[test]
fn test_view_backfill_chunked_matches_unchunked() {
    let dir = temp_dir("view_backfill_chunked");
    let mut engine = CatalogEngine::open(&dir).unwrap();

    let cols = vec![u64_col_def("id"), str_col_def("name")];
    let tid = engine.create_table("public.base", &cols, &[0], true).unwrap();
    let schema = engine.get_schema(tid).unwrap();

    // Long strings (above the inline threshold) force blob-arena relocation
    // in every chunk.
    let n = 10u64;
    let mut bb = BatchBuilder::new(schema);
    for i in 0..n {
        bb.begin_row(i as u128, 1);
        bb.put_string(&format!("row_{:02}_{}", i, "x".repeat(40)));
        bb.end_row();
    }
    engine.ingest_to_family(tid, &bb.finish()).unwrap();

    // 10 rows / chunk 3 → 4 epochs through the identity plan.
    engine.ddl_scan_chunk_rows = 3;

    let vid = engine.allocate_table_id();
    write_identity_circuit(&mut engine, vid, tid);
    engine.write_column_records(vid, OWNER_KIND_VIEW, &cols).unwrap();
    engine.write_view_deps(vid, &[tid]).unwrap();
    let batch = build_view_tab_row(vid, "v_chunk", "SELECT * FROM base");
    engine.ingest_to_family(VIEW_TAB_ID, &batch).unwrap();

    fn scan_rows(engine: &CatalogEngine, id: i64) -> Vec<(u128, i64, String)> {
        let mut c = engine.dag.tables.get(&id).unwrap().handle.open_cursor();
        let mut rows = Vec::new();
        while c.cursor.valid {
            rows.push((c.cursor.current_key, c.cursor.current_weight,
                       cursor_read_string(&c, 1)));
            c.cursor.advance();
        }
        rows
    }

    let base_rows = scan_rows(&engine, tid);
    assert_eq!(base_rows.len() as u64, n, "base table must hold every row");
    assert_eq!(scan_rows(&engine, vid), base_rows,
        "chunked view backfill must equal the base table row-for-row");

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}
