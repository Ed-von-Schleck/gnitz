use super::*;

// ── test_identifiers ─────────────────────────────────────────────────

#[test]
fn test_identifiers() {
    // Valid names
    for name in &["orders", "Orders123", "my_table", "a", "A1_b2", "1a", "99_problems"] {
        assert!(validate_user_identifier(name).is_ok(), "Rejected valid: {}", name);
    }
    // Invalid names
    for name in &["_private", "_", "_system", "__init__", "", "has space",
                   "has-dash", "has.dot", "has@", "table$"] {
        assert!(validate_user_identifier(name).is_err(), "Accepted invalid: {}", name);
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
    let tid = engine.create_table("sales.orders", &cols, 0, true).unwrap();
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
    assert!(engine.create_table("nonexistent.tbl", &cols, 0, true).is_err());
    // 3. Drop non-existent table
    assert!(engine.drop_table("public.nonexistent").is_err());
    // 4. Duplicate table
    engine.create_table("public.tbl1", &cols, 0, true).unwrap();
    assert!(engine.create_table("public.tbl1", &cols, 0, true).is_err());

    // 5. Drop non-empty schema cascades (PostgreSQL-style): the
    //    contained table goes first, then the schema itself.
    engine.create_schema("my_schema").unwrap();
    engine.create_table("my_schema.tbl2", &cols, 0, true).unwrap();
    engine.drop_schema("my_schema").unwrap();
    assert!(!engine.has_schema("my_schema"));
    assert!(engine.get_by_name("my_schema", "tbl2").is_none(),
        "cascade must drop the contained table");

    // 7. Unqualified name defaults to public
    let tid7 = engine.create_table("tbl3", &cols, 0, true).unwrap();
    assert!(engine.get_by_name("public", "tbl3").is_some());
    engine.drop_table("public.tbl3").unwrap();

    // 8. Unqualified drop
    engine.create_table("public.tbl4", &cols, 0, true).unwrap();
    engine.drop_table("tbl4").unwrap();
    assert!(engine.get_by_name("public", "tbl4").is_none());

    // 9. Invalid PK type (STRING)
    assert!(engine.create_table("public.bad_pk",
        &[ColumnDef { name: "id".into(), type_code: type_code::STRING, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 }],
        0, true).is_err());

    // 10. Too many columns (> 64)
    let many: Vec<ColumnDef> = (0..65).map(|i| u64_col_def(&format!("c{}", i))).collect();
    assert!(engine.create_table("public.too_many", &many, 0, true).is_err());

    // 11. Drop system schema
    assert!(engine.drop_schema("_system").is_err());

    // 12. PK index out of bounds
    assert!(engine.create_table("public.bad_idx", &cols, 5, true).is_err());

    // 13. Recreated schema gets new ID
    engine.create_schema("temp").unwrap();
    let sid1 = engine.get_schema_id("temp");
    engine.drop_schema("temp").unwrap();
    engine.create_schema("temp").unwrap();
    let sid2 = engine.get_schema_id("temp");
    assert_ne!(sid1, sid2);
    engine.drop_schema("temp").unwrap();

    // 14. Recreated table gets new ID
    let tid14a = engine.create_table("public.tbl_rc", &cols, 0, true).unwrap();
    engine.drop_table("public.tbl_rc").unwrap();
    let tid14b = engine.create_table("public.tbl_rc", &cols, 0, true).unwrap();
    assert_ne!(tid14a, tid14b);
    engine.drop_table("public.tbl_rc").unwrap();

    // 15. U128 PK support
    let tid15 = engine.create_table("public.u128t", &[u128_col_def("uuid_pk"), str_col_def("data")], 0, true).unwrap();
    let s15 = engine.get_schema(tid15).unwrap();
    assert_eq!(s15.columns[0].type_code, type_code::U128);
    engine.drop_table("public.u128t").unwrap();

    // 18. schema_is_empty
    engine.create_schema("empty_test").unwrap();
    assert!(engine.schema_is_empty("empty_test"));
    engine.create_table("empty_test.tbl", &cols, 0, true).unwrap();
    assert!(!engine.schema_is_empty("empty_test"));
    engine.drop_table("empty_test.tbl").unwrap();
    assert!(engine.schema_is_empty("empty_test"));
    engine.drop_schema("empty_test").unwrap();

    // 19. Case sensitivity
    engine.create_table("public.CaseTest", &cols, 0, true).unwrap();
    engine.create_table("public.casetest", &cols, 0, true).unwrap();
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

// ── test_unique_pk_metadata ──────────────────────────────────────────

#[test]
fn test_unique_pk_metadata() {
    let dir = temp_dir("unique_pk_meta");
    let cols = vec![u64_col_def("id"), str_col_def("val")];

    {
        let mut engine = CatalogEngine::open(&dir).unwrap();
        engine.create_schema("sales").unwrap();
        // default = unique_pk=true
        let tid1 = engine.create_table("sales.u_default", &cols, 0, true).unwrap();
        assert!(engine.is_unique_pk(tid1));
        // explicit false
        let tid2 = engine.create_table("sales.u_off", &cols, 0, false).unwrap();
        assert!(!engine.is_unique_pk(tid2));
        // For restart test
        engine.create_table("sales.u_restart", &cols, 0, true).unwrap();
        engine.close();
    }

    // Verify survival across restart
    {
        let mut engine = CatalogEngine::open(&dir).unwrap();
        let tid_restart = engine.get_by_name("sales", "u_restart").unwrap();
        assert!(engine.is_unique_pk(tid_restart));
        let tid_off = engine.get_by_name("sales", "u_off").unwrap();
        assert!(!engine.is_unique_pk(tid_off));
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
        first_tid = engine.create_table("marketing.products", &cols, 0, true).unwrap();

        // Dropped entities should not reappear
        engine.create_schema("trash").unwrap();
        engine.create_table("trash.items", &cols, 0, true).unwrap();
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
        assert_eq!(schema.num_columns, 2);
        // Sequence recovery: new table should get higher ID
        let new_tid = engine.create_table("marketing.other", &cols, 0, true).unwrap();
        assert!(new_tid > first_tid, "Allocator sequence recovery failed: {} <= {}", new_tid, first_tid);
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
        engine.create_table("longtest.tbl", &cols, 0, true).unwrap();
        engine.close();
        drop(engine);
    }
    {
        let mut engine = CatalogEngine::open(&dir).unwrap();
        assert!(engine.get_by_name("longtest", "tbl").is_some());
        // Verify column name survived out-of-line blob round-trip
        let tid = engine.get_by_name("longtest", "tbl").unwrap();
        let col_defs = engine.read_column_defs(tid).unwrap();
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
    assert!(engine.create_table("public.schema.tbl", &cols, 0, true).is_err());

    // #17. get_by_name on non-existent returns None
    assert!(engine.get_by_name("public", "nonexistent").is_none());

    // #20. has_id / get_schema for valid and invalid IDs
    let tid = engine.create_table("public.reg_test", &cols, 0, true).unwrap();
    assert!(engine.has_id(tid));
    assert!(engine.get_schema(tid).is_some());
    assert!(!engine.has_id(999999));
    assert!(engine.get_schema(999999).is_none());
    engine.drop_table("public.reg_test").unwrap();

    // #26. Creating a user table in _system schema should fail
    // (_system identifier starts with '_' → rejected by validate_user_identifier)
    assert!(engine.create_table("_system.new_tbl", &cols, 0, true).is_err());

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}
