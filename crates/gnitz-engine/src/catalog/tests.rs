use super::*;
use super::sys_tables::*;

use std::fs;

fn temp_dir(name: &str) -> String {
    crate::util::raise_fd_limit_for_tests();
    let path = format!("/tmp/gnitz_catalog_test_{}", name);
    let _ = fs::remove_dir_all(&path);
    path
}

fn u64_col_def(name: &str) -> ColumnDef {
    ColumnDef { name: name.into(), type_code: type_code::U64, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 }
}
fn i64_col_def(name: &str) -> ColumnDef {
    ColumnDef { name: name.into(), type_code: type_code::I64, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 }
}
fn u8_col_def(name: &str) -> ColumnDef {
    ColumnDef { name: name.into(), type_code: type_code::U8, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 }
}
fn u16_col_def(name: &str) -> ColumnDef {
    ColumnDef { name: name.into(), type_code: type_code::U16, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 }
}
fn i32_col_def(name: &str) -> ColumnDef {
    ColumnDef { name: name.into(), type_code: type_code::I32, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 }
}
fn str_col_def(name: &str) -> ColumnDef {
    ColumnDef { name: name.into(), type_code: type_code::STRING, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 }
}
fn u128_col_def(name: &str) -> ColumnDef {
    ColumnDef { name: name.into(), type_code: type_code::U128, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 }
}

fn count_records(table: &mut Table) -> usize {
    let mut count = 0;
    if let Ok(cursor) = table.create_cursor() {
        let mut c = cursor;
        while c.cursor.valid {
            if c.cursor.current_weight > 0 { count += 1; }
            c.cursor.advance();
        }
    }
    count
}

/// Build a passthrough (SELECT *) view graph over source_table_id.
/// Node IDs start at 1, edge IDs start at 1.
fn make_passthrough_graph(source_table_id: i64, output_cols: &[(String, u8)]) -> CircuitGraph {
    // Node 1: SCAN_TRACE (opcode 11) — input delta (source table_id=0)
    // Node 2: INTEGRATE (opcode 7) — sink
    // Edge 1: node 1 → node 2, port 0 (PORT_IN)
    CircuitGraph {
        nodes: vec![(1, 11), (2, 7)],           // SCAN_TRACE, INTEGRATE
        edges: vec![(1, 1, 2, 0)],              // edge 1: src=1 dst=2 port=0
        sources: vec![(1, 0)],                   // node 1 reads from table_id=0 (primary delta)
        params: vec![],
        group_cols: vec![],
        output_col_defs: output_cols.to_vec(),
        dependencies: vec![source_table_id],
    }
}

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

    // System schemas
    assert!(engine.has_schema("_system"));
    assert!(engine.has_schema("public"));

    // System tables (12 expected)
    assert_eq!(count_records(&mut engine.sys_tables), 12);
    // System columns (46 expected for all 12 system tables)
    assert_eq!(count_records(&mut engine.sys_columns), 46);
    // Sequences (4 expected)
    assert_eq!(count_records(&mut engine.sys_sequences), 4);
    // Schemas (2 expected: _system, public)
    assert_eq!(count_records(&mut engine.sys_schemas), 2);

    // Allocator state
    assert_eq!(engine.next_table_id, FIRST_USER_TABLE_ID);
    assert_eq!(engine.next_schema_id, FIRST_USER_SCHEMA_ID);

    engine.close();
    drop(engine); // Release WAL locks before re-open

    // Idempotent re-open: should not duplicate records
    let mut engine2 = CatalogEngine::open(&dir).unwrap();
    assert_eq!(count_records(&mut engine2.sys_schemas), 2);
    assert_eq!(count_records(&mut engine2.sys_tables), 12);
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

// ── test_index_functional_and_fanout ─────────────────────────────────

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

// ── test_fk_referential_integrity ────────────────────────────────────

#[test]
fn test_fk_referential_integrity() {
    let dir = temp_dir("fk_integrity");
    let mut engine = CatalogEngine::open(&dir).unwrap();

    // Parent table
    let parent_tid = engine.create_table("public.parents", &[u64_col_def("pid")], 0, true).unwrap();

    // Child table with FK
    let child_cols = vec![
        u64_col_def("cid"),
        ColumnDef { name: "pid_fk".into(), type_code: type_code::U64, is_nullable: false,
                    fk_table_id: parent_tid, fk_col_idx: 0 },
    ];
    let child_tid = engine.create_table("public.children", &child_cols, 0, true).unwrap();

    // Insert valid parent
    let parent_schema = engine.get_schema(parent_tid).unwrap();
    let mut pbb = BatchBuilder::new(parent_schema);
    pbb.begin_row(10, 0, 1);
    pbb.end_row();
    let pbatch = pbb.finish();
    engine.dag.ingest_to_family(parent_tid, pbatch);
    let _ = engine.dag.flush(parent_tid);

    // Insert valid child (FK=10 exists in parent)
    let child_schema = engine.get_schema(child_tid).unwrap();
    let mut cbb = BatchBuilder::new(child_schema);
    cbb.begin_row(1, 0, 1);
    cbb.put_u64(10); // valid FK
    cbb.end_row();
    let cbatch = cbb.finish();
    assert!(engine.validate_fk_inline(child_tid, &cbatch).is_ok());

    // Insert invalid child (FK=99, not in parent)
    let mut cbb2 = BatchBuilder::new(child_schema);
    cbb2.begin_row(2, 0, 1);
    cbb2.put_u64(99); // invalid FK
    cbb2.end_row();
    let cbatch2 = cbb2.finish();
    assert!(engine.validate_fk_inline(child_tid, &cbatch2).is_err());

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── test_fk_nullability_and_retractions ──────────────────────────────

#[test]
fn test_fk_nullability_and_retractions() {
    let dir = temp_dir("fk_null");
    let mut engine = CatalogEngine::open(&dir).unwrap();

    let parent_tid = engine.create_table("public.p", &[u64_col_def("id")], 0, true).unwrap();
    let child_cols = vec![
        u64_col_def("id"),
        ColumnDef { name: "pid_fk".into(), type_code: type_code::U64, is_nullable: true,
                    fk_table_id: parent_tid, fk_col_idx: 0 },
    ];
    let child_tid = engine.create_table("public.c", &child_cols, 0, true).unwrap();

    // NULL FK should be allowed even if parent is empty
    let child_schema = engine.get_schema(child_tid).unwrap();
    let mut bb = BatchBuilder::new(child_schema);
    bb.begin_row(1, 0, 1);
    bb.put_null();
    bb.end_row();
    let batch = bb.finish();
    assert!(engine.validate_fk_inline(child_tid, &batch).is_ok());

    // Retraction (weight -1) should not trigger FK check
    let mut bb2 = BatchBuilder::new(child_schema);
    bb2.begin_row(2, 0, -1);
    bb2.put_u64(999); // non-existent, but weight is negative
    bb2.end_row();
    let batch2 = bb2.finish();
    assert!(engine.validate_fk_inline(child_tid, &batch2).is_ok());

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── test_fk_protections ──────────────────────────────────────────────

#[test]
fn test_fk_drop_protections() {
    let dir = temp_dir("fk_prot");
    let mut engine = CatalogEngine::open(&dir).unwrap();

    let parent_tid = engine.create_table("public.parent", &[u64_col_def("pid")], 0, true).unwrap();
    let child_cols = vec![
        u64_col_def("cid"),
        ColumnDef { name: "pid_fk".into(), type_code: type_code::U64, is_nullable: false,
                    fk_table_id: parent_tid, fk_col_idx: 0 },
    ];
    engine.create_table("public.child", &child_cols, 0, true).unwrap();

    // Cannot drop parent (referenced by child)
    assert!(engine.drop_table("public.parent").is_err());

    // Cannot drop auto-generated FK index
    let idx_name = make_fk_index_name("public", "child", "pid_fk");
    assert!(engine.drop_index(&idx_name).is_err());

    // Drop child first, then parent succeeds
    engine.drop_table("public.child").unwrap();
    engine.drop_table("public.parent").unwrap();

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── test_fk_invalid_targets ──────────────────────────────────────────

#[test]
fn test_fk_invalid_targets() {
    let dir = temp_dir("fk_invalid");
    let mut engine = CatalogEngine::open(&dir).unwrap();

    let parent_tid = engine.create_table("public.p",
        &[u64_col_def("pk"), i64_col_def("other")], 0, true).unwrap();

    // FK targeting non-PK column (col_idx=1) should fail
    let bad_cols = vec![
        u64_col_def("pk"),
        ColumnDef { name: "fk".into(), type_code: type_code::I64, is_nullable: false,
                    fk_table_id: parent_tid, fk_col_idx: 1 },
    ];
    assert!(engine.create_table("public.c_bad", &bad_cols, 0, true).is_err());

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── test_fk_self_reference ───────────────────────────────────────────

#[test]
fn test_fk_self_reference() {
    let dir = temp_dir("fk_self");
    let mut engine = CatalogEngine::open(&dir).unwrap();

    // Self-referential table: employees.mgr_id -> employees.emp_id
    let next_tid = engine.next_table_id;
    let emp_cols = vec![
        u64_col_def("emp_id"),
        ColumnDef { name: "mgr_id".into(), type_code: type_code::U64, is_nullable: true,
                    fk_table_id: next_tid, fk_col_idx: 0 },
    ];
    let emp_tid = engine.create_table("public.employees", &emp_cols, 0, true).unwrap();
    assert_eq!(emp_tid, next_tid);

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── test_fk_parent_restrict_blocks_delete ────────────────────────────

#[test]
fn test_fk_parent_restrict_blocks_delete() {
    let dir = temp_dir("fk_restrict");
    let mut engine = CatalogEngine::open(&dir).unwrap();

    let parent_tid = engine.create_table("public.parent", &[u64_col_def("pid")], 0, true).unwrap();
    let child_cols = vec![
        u64_col_def("cid"),
        ColumnDef { name: "fk".into(), type_code: type_code::U64, is_nullable: false,
                    fk_table_id: parent_tid, fk_col_idx: 0 },
    ];
    let child_tid = engine.create_table("public.child", &child_cols, 0, true).unwrap();

    // Insert parent PK=10
    let ps = engine.get_schema(parent_tid).unwrap();
    let mut pbb = BatchBuilder::new(ps);
    pbb.begin_row(10, 0, 1);
    pbb.end_row();
    engine.dag.ingest_to_family(parent_tid, pbb.finish());
    let _ = engine.dag.flush(parent_tid);

    // Insert child FK=10
    let cs = engine.get_schema(child_tid).unwrap();
    let mut cbb = BatchBuilder::new(cs);
    cbb.begin_row(1, 0, 1);
    cbb.put_u64(10);
    cbb.end_row();
    let cbatch = cbb.finish();
    assert!(engine.validate_fk_inline(child_tid, &cbatch).is_ok());
    engine.dag.ingest_to_family(child_tid, cbatch);
    let _ = engine.dag.flush(child_tid);

    // Retract parent PK=10 — should be blocked by RESTRICT
    let mut del = BatchBuilder::new(ps);
    del.begin_row(10, 0, -1);
    del.end_row();
    let del_batch = del.finish();
    let result = engine.validate_fk_parent_restrict(parent_tid, &del_batch);
    assert!(result.is_err(), "DELETE parent with existing child should fail");
    assert!(result.unwrap_err().contains("Foreign Key violation"));

    // Retract parent PK=99 (no child references it) — should succeed
    let mut del2 = BatchBuilder::new(ps);
    del2.begin_row(99, 0, -1);
    del2.end_row();
    assert!(engine.validate_fk_parent_restrict(parent_tid, &del2.finish()).is_ok());

    // Insert-only batch on parent table — RESTRICT should be a no-op
    let mut ins = BatchBuilder::new(ps);
    ins.begin_row(20, 0, 1);
    ins.end_row();
    assert!(engine.validate_fk_parent_restrict(parent_tid, &ins.finish()).is_ok());

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── test_fk_parent_map_cleanup ──────────────────────────────────────

#[test]
fn test_fk_parent_map_cleanup() {
    let dir = temp_dir("fk_pmap");
    let mut engine = CatalogEngine::open(&dir).unwrap();

    let parent_tid = engine.create_table("public.parent", &[u64_col_def("pid")], 0, true).unwrap();
    let child_cols = vec![
        u64_col_def("cid"),
        ColumnDef { name: "fk".into(), type_code: type_code::U64, is_nullable: false,
                    fk_table_id: parent_tid, fk_col_idx: 0 },
    ];
    engine.create_table("public.child", &child_cols, 0, true).unwrap();

    // Verify parent_map populated
    assert!(!engine.fk_children_of(parent_tid).is_empty());

    // Drop child — parent_map should be cleaned up
    engine.drop_table("public.child").unwrap();
    assert!(engine.fk_children_of(parent_tid).is_empty(),
            "fk_parent_map not cleaned up after child drop");

    // Drop parent should now succeed
    engine.drop_table("public.parent").unwrap();

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── test_fk_multiple_children_same_parent ───────────────────────────

#[test]
fn test_fk_multiple_children_same_parent() {
    let dir = temp_dir("fk_multi_child");
    let mut engine = CatalogEngine::open(&dir).unwrap();

    let parent_tid = engine.create_table("public.parent", &[u64_col_def("pid")], 0, true).unwrap();

    let mk_child = |engine: &mut CatalogEngine, name: &str| -> i64 {
        let cols = vec![
            u64_col_def("cid"),
            ColumnDef { name: "fk".into(), type_code: type_code::U64, is_nullable: false,
                        fk_table_id: parent_tid, fk_col_idx: 0 },
        ];
        engine.create_table(name, &cols, 0, true).unwrap()
    };
    let child1_tid = mk_child(&mut engine, "public.child1");
    let child2_tid = mk_child(&mut engine, "public.child2");

    // Both children should reference the parent
    let children = engine.fk_children_of(parent_tid);
    assert_eq!(children.len(), 2);

    // Cannot drop parent while either child exists
    assert!(engine.drop_table("public.parent").is_err());

    // Insert parent + child1 row, verify RESTRICT
    let ps = engine.get_schema(parent_tid).unwrap();
    let mut pbb = BatchBuilder::new(ps);
    pbb.begin_row(10, 0, 1);
    pbb.end_row();
    engine.dag.ingest_to_family(parent_tid, pbb.finish());
    let _ = engine.dag.flush(parent_tid);

    let cs = engine.get_schema(child1_tid).unwrap();
    let mut cbb = BatchBuilder::new(cs);
    cbb.begin_row(1, 0, 1);
    cbb.put_u64(10);
    cbb.end_row();
    engine.dag.ingest_to_family(child1_tid, cbb.finish());
    let _ = engine.dag.flush(child1_tid);

    // DELETE parent blocked by child1
    let mut del = BatchBuilder::new(ps);
    del.begin_row(10, 0, -1);
    del.end_row();
    assert!(engine.validate_fk_parent_restrict(parent_tid, &del.finish()).is_err());

    // Drop child1 — still blocked by child2
    engine.drop_table("public.child1").unwrap();
    assert!(engine.drop_table("public.parent").is_err());

    // Drop child2 — now parent can be dropped
    engine.drop_table("public.child2").unwrap();
    assert!(engine.fk_children_of(parent_tid).is_empty());
    engine.drop_table("public.parent").unwrap();

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── test_view_backfill_simple ────────────────────────────────────────

#[test]
fn test_view_backfill_simple() {
    let dir = temp_dir("view_backfill");
    let mut engine = CatalogEngine::open(&dir).unwrap();

    let cols = vec![u64_col_def("id"), i64_col_def("val")];
    let tid = engine.create_table("public.t", &cols, 0, true).unwrap();

    // Ingest 5 rows
    let schema = engine.get_schema(tid).unwrap();
    let mut bb = BatchBuilder::new(schema);
    for i in 0..5u64 {
        bb.begin_row(i, 0, 1);
        bb.put_u64((i * 10) as u64);
        bb.end_row();
    }
    engine.dag.ingest_to_family(tid, bb.finish());
    let _ = engine.dag.flush(tid);

    // Create passthrough view
    let out_cols = vec![
        ("id".to_string(), type_code::U64),
        ("val".to_string(), type_code::I64),
    ];
    let graph = make_passthrough_graph(tid, &out_cols);
    let vid = engine.create_view("public.v", &graph, "").unwrap();

    // Verify backfill: view should have 5 rows
    let view_entry = engine.dag.tables.get(&vid).unwrap();
    let view_schema = view_entry.schema;
    let batch = engine.scan_store(vid, &view_schema);
    assert_eq!(batch.count, 5, "View backfill: expected 5 rows, got {}", batch.count);

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── test_view_backfill_on_restart ────────────────────────────────────

#[test]
fn test_view_backfill_on_restart() {
    let dir = temp_dir("view_restart");
    let cols = vec![u64_col_def("id"), i64_col_def("val")];
    let out_cols = vec![
        ("id".to_string(), type_code::U64),
        ("val".to_string(), type_code::I64),
    ];

    {
        let mut engine = CatalogEngine::open(&dir).unwrap();
        let tid = engine.create_table("public.t", &cols, 0, true).unwrap();
        let graph = make_passthrough_graph(tid, &out_cols);
        engine.create_view("public.v", &graph, "").unwrap();

        // Ingest data AFTER view creation (stored in base table)
        let schema = engine.get_schema(tid).unwrap();
        let mut bb = BatchBuilder::new(schema);
        for i in 0..5u64 {
            bb.begin_row(i, 0, 1);
            bb.put_u64((i * 10) as u64);
            bb.end_row();
        }
        engine.dag.ingest_to_family(tid, bb.finish());
        let _ = engine.dag.flush(tid);
        engine.close();
    }

    // Re-open: view should be backfilled from base table data
    {
        let mut engine = CatalogEngine::open(&dir).unwrap();
        let vid = engine.get_by_name("public", "v").unwrap();
        let view_entry = engine.dag.tables.get(&vid).unwrap();
        let view_schema = view_entry.schema;
        let batch = engine.scan_store(vid, &view_schema);
        assert_eq!(batch.count, 5, "View backfill on restart: expected 5, got {}", batch.count);
        engine.close();
    }

    let _ = fs::remove_dir_all(&dir);
}

// ── test_view_on_view_backfill_on_restart ────────────────────────────

#[test]
fn test_view_on_view_backfill() {
    let dir = temp_dir("view_on_view");
    let cols = vec![u64_col_def("id"), i64_col_def("val")];
    let out_cols = vec![
        ("id".to_string(), type_code::U64),
        ("val".to_string(), type_code::I64),
    ];

    {
        let mut engine = CatalogEngine::open(&dir).unwrap();
        let tid = engine.create_table("public.t", &cols, 0, true).unwrap();

        // Ingest data
        let schema = engine.get_schema(tid).unwrap();
        let mut bb = BatchBuilder::new(schema);
        for v in &[5u64, 20, 50, 80, 100] {
            bb.begin_row(*v, 0, 1);
            bb.put_u64(*v);
            bb.end_row();
        }
        engine.dag.ingest_to_family(tid, bb.finish());
        let _ = engine.dag.flush(tid);

        let graph1 = make_passthrough_graph(tid, &out_cols);
        let v1_id = engine.create_view("public.v1", &graph1, "").unwrap();

        let graph2 = make_passthrough_graph(v1_id, &out_cols);
        let v2_id = engine.create_view("public.v2", &graph2, "").unwrap();

        // Check depths
        assert_eq!(engine.get_depth(v1_id), 1);
        assert_eq!(engine.get_depth(v2_id), 2);

        engine.close();
    }

    // Re-open: both views should be backfilled
    {
        let mut engine = CatalogEngine::open(&dir).unwrap();
        let v1_id = engine.get_by_name("public", "v1").unwrap();
        let v2_id = engine.get_by_name("public", "v2").unwrap();
        let v1_schema = engine.dag.tables.get(&v1_id).unwrap().schema;
        let v2_schema = engine.dag.tables.get(&v2_id).unwrap().schema;
        let b1 = engine.scan_store(v1_id, &v1_schema);
        let b2 = engine.scan_store(v2_id, &v2_schema);
        assert_eq!(b1.count, 5, "V1 restart: expected 5, got {}", b1.count);
        assert_eq!(b2.count, 5, "V2 restart: expected 5, got {}", b2.count);
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

// ── test_fk_referential_integrity (U128 extension) ──────────────────

#[test]
fn test_fk_u128() {
    let dir = temp_dir("fk_u128");
    let mut engine = CatalogEngine::open(&dir).unwrap();

    // U128 parent
    let parent_tid = engine.create_table("public.uparents",
        &[u128_col_def("uuid")], 0, true).unwrap();

    // U128 FK child
    let child_cols = vec![
        u64_col_def("id"),
        ColumnDef { name: "ufk".into(), type_code: type_code::U128, is_nullable: false,
                    fk_table_id: parent_tid, fk_col_idx: 0 },
    ];
    let child_tid = engine.create_table("public.uchildren", &child_cols, 0, true).unwrap();

    // Insert parent with U128 PK (lo=0xBBBB, hi=0xAAAA)
    let parent_schema = engine.get_schema(parent_tid).unwrap();
    let mut pbb = BatchBuilder::new(parent_schema);
    pbb.begin_row(0xBBBB, 0xAAAA, 1);
    pbb.end_row();
    engine.dag.ingest_to_family(parent_tid, pbb.finish());
    let _ = engine.dag.flush(parent_tid);

    // Valid child FK (matches parent)
    let child_schema = engine.get_schema(child_tid).unwrap();
    let mut cbb = BatchBuilder::new(child_schema);
    cbb.begin_row(1, 0, 1);
    cbb.put_u128(0xBBBB, 0xAAAA);
    cbb.end_row();
    assert!(engine.validate_fk_inline(child_tid, &cbb.finish()).is_ok());

    // Invalid child FK (no such parent)
    let mut cbb2 = BatchBuilder::new(child_schema);
    cbb2.begin_row(2, 0, 1);
    cbb2.put_u128(0xDEAD, 0xBEEF);
    cbb2.end_row();
    assert!(engine.validate_fk_inline(child_tid, &cbb2.finish()).is_err());

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── View live incremental update ──────────────────────────────────────

#[test]
fn test_view_live_update() {
    let dir = temp_dir("view_live");
    let mut engine = CatalogEngine::open(&dir).unwrap();

    let cols = vec![u64_col_def("id"), i64_col_def("val")];
    let tid = engine.create_table("public.t", &cols, 0, true).unwrap();
    let schema = engine.get_schema(tid).unwrap();

    // Ingest initial data
    let mut bb = BatchBuilder::new(schema);
    for i in 0..5u64 {
        bb.begin_row(i, 0, 1);
        bb.put_u64(i * 10);
        bb.end_row();
    }
    engine.dag.ingest_to_family(tid, bb.finish());
    let _ = engine.dag.flush(tid);

    // Create passthrough view — should backfill 5 rows
    let out_cols = vec![
        ("id".to_string(), type_code::U64),
        ("val".to_string(), type_code::I64),
    ];
    let graph = make_passthrough_graph(tid, &out_cols);
    let vid = engine.create_view("public.v", &graph, "").unwrap();

    let view_schema = engine.dag.tables.get(&vid).unwrap().schema;
    let b1 = engine.scan_store(vid, &view_schema);
    assert_eq!(b1.count, 5, "Backfill: expected 5, got {}", b1.count);

    // Live update: ingest 1 more row + evaluate DAG
    let mut bb2 = BatchBuilder::new(schema);
    bb2.begin_row(99, 0, 1);
    bb2.put_u64(999);
    bb2.end_row();
    let delta = bb2.finish();
    engine.dag.ingest_by_ref(tid, &delta);
    let _ = engine.dag.flush(tid);
    engine.dag.evaluate_dag(tid, delta);

    let b2 = engine.scan_store(vid, &view_schema);
    assert_eq!(b2.count, 6, "Live update: expected 6, got {}", b2.count);

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── Index live fan-out ────────────────────────────────────────────────

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

// ── Full lifecycle (replaces test_programmable_zset_lifecycle) ────────

#[test]
fn test_full_lifecycle() {
    let dir = temp_dir("lifecycle");
    let mut engine = CatalogEngine::open(&dir).unwrap();

    // 1. Schema
    engine.create_schema("app").unwrap();

    // 2. Tables with FK
    let users_tid = engine.create_table("app.users",
        &[u128_col_def("uid"), str_col_def("username")], 0, true).unwrap();

    let order_cols = vec![
        u64_col_def("oid"),
        ColumnDef { name: "uid".into(), type_code: type_code::U128, is_nullable: false,
                    fk_table_id: users_tid, fk_col_idx: 0 },
        i64_col_def("amount"),
    ];
    let orders_tid = engine.create_table("app.orders", &order_cols, 0, true).unwrap();

    // 3. FK enforcement: invalid order (no matching user)
    let orders_schema = engine.get_schema(orders_tid).unwrap();
    let mut bad = BatchBuilder::new(orders_schema);
    bad.begin_row(1, 0, 1);
    bad.put_u128(0xCAFEBABE, 0xDEADBEEF);
    bad.put_u64(500);
    bad.end_row();
    assert!(engine.validate_fk_inline(orders_tid, &bad.finish()).is_err());

    // 4. Ingest valid user + order
    let users_schema = engine.get_schema(users_tid).unwrap();
    let mut ub = BatchBuilder::new(users_schema);
    ub.begin_row(0xCAFEBABE, 0xDEADBEEF, 1);
    ub.put_string("alice");
    ub.end_row();
    engine.dag.ingest_to_family(users_tid, ub.finish());
    let _ = engine.dag.flush(users_tid);

    let mut ob = BatchBuilder::new(orders_schema);
    ob.begin_row(101, 0, 1);
    ob.put_u128(0xCAFEBABE, 0xDEADBEEF);
    ob.put_u64(1000);
    ob.end_row();
    assert!(engine.validate_fk_inline(orders_tid, &ob.finish()).is_ok());

    // 5. View
    let out_cols = vec![
        ("uid".to_string(), type_code::U128),
        ("username".to_string(), type_code::STRING),
    ];
    let graph = make_passthrough_graph(users_tid, &out_cols);
    let view_tid = engine.create_view("app.active_users", &graph, "SELECT * FROM users").unwrap();

    // 5.1 Can't drop table referenced by view
    assert!(engine.drop_table("app.users").is_err());

    // 6. Persistence
    engine.close();
    drop(engine);

    let mut engine2 = CatalogEngine::open(&dir).unwrap();
    assert!(engine2.get_by_name("app", "users").is_some());
    assert!(engine2.get_by_name("app", "active_users").is_some());

    // 7. Compilation recovery
    assert!(engine2.dag.ensure_compiled(view_tid));

    // 8. Teardown
    engine2.drop_view("app.active_users").unwrap();
    engine2.drop_table("app.orders").unwrap();
    engine2.drop_table("app.users").unwrap();
    engine2.drop_schema("app").unwrap();
    assert!(!engine2.has_schema("app"));

    engine2.close();
    let _ = fs::remove_dir_all(&dir);
}

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

// ── UPSERT retraction propagation tests ──────────────────────────

#[test]
fn test_upsert_effective_batch_has_retractions() {
    let dir = temp_dir("catalog_upsert_retract");
    let mut engine = CatalogEngine::open(&dir).unwrap();
    let cols = vec![u64_col_def("id"), u64_col_def("val")];
    let tid = engine.create_table("public.t", &cols, 0, true).unwrap(); // unique_pk
    let schema = engine.get_schema(tid).unwrap();

    // Insert PK=1, val=100
    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(1, 0, 1); bb.put_u64(100); bb.end_row();
    engine.dag.ingest_to_family(tid, bb.finish());
    let _ = engine.dag.flush(tid);

    // UPSERT PK=1, val=200 via ingest_returning_effective
    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(1, 0, 1); bb.put_u64(200); bb.end_row();
    let (rc, effective_opt) = engine.dag.ingest_returning_effective(tid, bb.finish());
    assert_eq!(rc, 0);
    let effective = effective_opt.unwrap();

    // Effective batch should have 2 rows:
    // Row 0: weight=-1 (retraction of old val=100)
    // Row 1: weight=+1 (insertion of new val=200)
    assert_eq!(effective.count, 2, "Expected retraction + insertion");
    assert_eq!(effective.get_weight(0), -1, "First row should be retraction");
    assert_eq!(effective.get_weight(1), 1, "Second row should be insertion");
    // Both should have PK=1
    assert_eq!(effective.get_pk(0), 1);
    assert_eq!(effective.get_pk(1), 1);

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_upsert_retraction_reaches_views() {
    let dir = temp_dir("catalog_upsert_view");
    let mut engine = CatalogEngine::open(&dir).unwrap();
    let cols = vec![u64_col_def("id"), u64_col_def("val")];
    let tid = engine.create_table("public.t", &cols, 0, true).unwrap();
    let schema = engine.get_schema(tid).unwrap();
    let out_cols = vec![("id".to_string(), type_code::U64), ("val".to_string(), type_code::U64)];

    // Create passthrough view
    let graph = make_passthrough_graph(tid, &out_cols);
    let vid = engine.create_view("public.v", &graph, "").unwrap();

    // Insert PK=1, val=100
    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(1, 0, 1); bb.put_u64(100); bb.end_row();
    engine.push_and_evaluate(tid, bb.finish()).unwrap();

    // Check view: should have 1 row with val=100
    let scan1 = engine.scan_family(vid).unwrap();
    assert_eq!(scan1.count, 1);

    // UPSERT PK=1, val=200
    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(1, 0, 1); bb.put_u64(200); bb.end_row();
    engine.push_and_evaluate(tid, bb.finish()).unwrap();

    // View should have 1 row with val=200 (old val=100 retracted)
    let scan2 = engine.scan_family(vid).unwrap();
    assert_eq!(scan2.count, 1, "View should have exactly 1 row after UPSERT");

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── push_and_evaluate test ───────────────────────────────────────

#[test]
fn test_push_and_evaluate_cascades_to_view() {
    let dir = temp_dir("catalog_push_eval");
    let mut engine = CatalogEngine::open(&dir).unwrap();
    let cols = vec![u64_col_def("id"), u64_col_def("val")];
    let tid = engine.create_table("public.t", &cols, 0, false).unwrap();
    let out_cols = vec![("id".to_string(), type_code::U64), ("val".to_string(), type_code::U64)];

    let graph = make_passthrough_graph(tid, &out_cols);
    let vid = engine.create_view("public.v", &graph, "").unwrap();

    let schema = engine.get_schema(tid).unwrap();
    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(1, 0, 1); bb.put_u64(10); bb.end_row();
    bb.begin_row(2, 0, 1); bb.put_u64(20); bb.end_row();
    engine.push_and_evaluate(tid, bb.finish()).unwrap();

    // View should reflect the push
    let scan = engine.scan_family(vid).unwrap();
    assert_eq!(scan.count, 2);

    // Push more
    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(3, 0, 1); bb.put_u64(30); bb.end_row();
    engine.push_and_evaluate(tid, bb.finish()).unwrap();

    let scan = engine.scan_family(vid).unwrap();
    assert_eq!(scan.count, 3);

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

// ── Ghost record / retraction tests ─────────────────────────────────

#[test]
fn test_drop_view_cleans_sys_views() {
    let dir = temp_dir("drop_view_clean");
    let mut engine = CatalogEngine::open(&dir).unwrap();

    // Baseline
    let base_views = count_records(&mut engine.sys_views);

    // Create source table
    let cols = vec![u64_col_def("id"), i64_col_def("val")];
    let tid = engine.create_table("public.src", &cols, 0, true).unwrap();

    // Create view
    let graph = make_passthrough_graph(tid, &[
        ("id".into(), type_code::U64), ("val".into(), type_code::I64),
    ]);
    let vid = engine.create_view("public.vw", &graph, "SELECT * FROM src").unwrap();
    assert_eq!(count_records(&mut engine.sys_views), base_views + 1);

    // Drop view — sys_views must return to baseline (no ghost rows)
    engine.drop_view("public.vw").unwrap();
    let _ = engine.sys_views.flush();
    assert_eq!(count_records(&mut engine.sys_views), base_views,
        "Ghost rows remain in sys_views after DROP VIEW");

    engine.drop_table("public.src").unwrap();
    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_drop_view_cleans_circuit_tables() {
    let dir = temp_dir("drop_view_circuit");
    let mut engine = CatalogEngine::open(&dir).unwrap();

    let base_nodes = count_records(&mut engine.sys_circuit_nodes);
    let base_edges = count_records(&mut engine.sys_circuit_edges);
    let base_sources = count_records(&mut engine.sys_circuit_sources);

    let cols = vec![u64_col_def("id"), i64_col_def("val")];
    let tid = engine.create_table("public.src2", &cols, 0, true).unwrap();

    let graph = make_passthrough_graph(tid, &[
        ("id".into(), type_code::U64), ("val".into(), type_code::I64),
    ]);
    let _vid = engine.create_view("public.vw2", &graph, "SELECT * FROM src2").unwrap();

    // Verify circuit rows were written
    let _ = engine.sys_circuit_nodes.flush();
    assert!(count_records(&mut engine.sys_circuit_nodes) > base_nodes);

    // Drop view — all circuit tables must return to baseline
    engine.drop_view("public.vw2").unwrap();
    let _ = engine.sys_circuit_nodes.flush();
    let _ = engine.sys_circuit_edges.flush();
    let _ = engine.sys_circuit_sources.flush();
    assert_eq!(count_records(&mut engine.sys_circuit_nodes), base_nodes,
        "Ghost rows in sys_circuit_nodes");
    assert_eq!(count_records(&mut engine.sys_circuit_edges), base_edges,
        "Ghost rows in sys_circuit_edges");
    assert_eq!(count_records(&mut engine.sys_circuit_sources), base_sources,
        "Ghost rows in sys_circuit_sources");

    engine.drop_table("public.src2").unwrap();
    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_repeated_create_drop_view() {
    let dir = temp_dir("repeated_view");
    let mut engine = CatalogEngine::open(&dir).unwrap();

    let base_views = count_records(&mut engine.sys_views);
    let base_nodes = count_records(&mut engine.sys_circuit_nodes);

    let cols = vec![u64_col_def("id"), i64_col_def("val")];
    let tid = engine.create_table("public.rep_src", &cols, 0, true).unwrap();

    for i in 0..5 {
        let vname = format!("public.rep_v{}", i);
        let graph = make_passthrough_graph(tid, &[
            ("id".into(), type_code::U64), ("val".into(), type_code::I64),
        ]);
        let _vid = engine.create_view(&vname, &graph, "SELECT *").unwrap();
        engine.drop_view(&vname).unwrap();
    }

    let _ = engine.sys_views.flush();
    let _ = engine.sys_circuit_nodes.flush();
    assert_eq!(count_records(&mut engine.sys_views), base_views,
        "sys_views grew after 5 create+drop cycles");
    assert_eq!(count_records(&mut engine.sys_circuit_nodes), base_nodes,
        "sys_circuit_nodes grew after 5 create+drop cycles");

    engine.drop_table("public.rep_src").unwrap();
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
    assert!(engine.index_by_name.contains_key(&idx_name));
    let idx_records_before = count_records(&mut engine.sys_indices);
    assert!(idx_records_before >= 1);

    // Drop the table WITHOUT dropping the index first.
    engine.drop_table("public.cascade_tbl").unwrap();

    assert!(!engine.index_by_name.contains_key(&idx_name),
            "in-memory index registry must forget the index");
    assert!(!engine.dag.tables.contains_key(&tid));
    assert_eq!(count_records(&mut engine.sys_indices), idx_records_before - 1,
               "sys_indices must retract exactly one record");

    // Reopen: before the fix, replay_catalog would fail here because
    // the orphaned sys_indices row references tid which no longer exists.
    engine.close();
    drop(engine);
    let mut engine2 = CatalogEngine::open(&dir).unwrap();
    assert!(!engine2.index_by_name.contains_key(&idx_name));
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
    assert!(engine.index_by_name.contains_key(&fk_idx_name),
            "FK index must be auto-created with the child table");

    // drop_index refuses to drop __fk_ indices, so drop_table is the only
    // valid path for this cleanup. Before the fix: the on_index_delta
    // hook's __fk_ guard blocked drop_table from cascading.
    engine.drop_table("public.child").unwrap();
    assert!(!engine.index_by_name.contains_key(&fk_idx_name),
            "FK index must be removed by drop_table cascade");

    // Reopen catalog — must succeed.
    engine.close();
    drop(engine);
    let mut engine2 = CatalogEngine::open(&dir).unwrap();
    assert!(!engine2.index_by_name.contains_key(&fk_idx_name));
    engine2.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── Regression: intra-batch modifications to the same PK ─────────────
// Bug 1.1: enforce_unique_pk_partitioned calls ptable.retract_pk which
// is read-only (sets found_source; doesn't mutate the store).  Calling
// it twice for the same PK in one batch emitted the stored-row
// retraction twice, driving downstream weights negative.  Additionally,
// `seen` stored an effective-relative index but was fed to
// append_batch_negated(&batch, ...), negating the wrong row once store
// retractions shifted effective.count ahead of the batch row index.

#[test]
fn test_intra_batch_insert_then_delete_same_pk() {
    // [(+1, pk=1, v=200), (-1, pk=1)] with store (pk=1, v=100) should
    // net to "row (1, 100) retracted, nothing inserted".
    let dir = temp_dir("intra_batch_ins_del");
    let mut engine = CatalogEngine::open(&dir).unwrap();
    let cols = vec![u64_col_def("id"), u64_col_def("val")];
    let tid = engine.create_table("public.t", &cols, 0, true).unwrap();
    let schema = engine.get_schema(tid).unwrap();

    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(1, 0, 1); bb.put_u64(100); bb.end_row();
    engine.dag.ingest_to_family(tid, bb.finish());
    let _ = engine.dag.flush(tid);

    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(1, 0, 1); bb.put_u64(200); bb.end_row();
    bb.begin_row(1, 0, -1); bb.put_u64(0); bb.end_row();
    let effective = engine.ingest_returning_effective(tid, bb.finish()).unwrap();

    // Count retractions of the stored (pk=1, val=100) row.  Must be
    // exactly 1; the bug produced 2.
    let mut retractions_of_100 = 0;
    let col = effective.col_data(0);
    for i in 0..effective.count {
        if effective.get_pk(i) == 1 && effective.get_weight(i) == -1 {
            let v = u64::from_le_bytes(col[i*8..i*8+8].try_into().unwrap());
            if v == 100 { retractions_of_100 += 1; }
        }
    }
    assert_eq!(retractions_of_100, 1,
               "stored row (val=100) must be retracted exactly once; got {}",
               retractions_of_100);

    // Net weight per distinct (pk, val) in the effective batch should
    // leave val=100 at -1 (cancels the stored +1) and val=200 at 0.
    let mut net: HashMap<(u128, u64), i64> = HashMap::new();
    for i in 0..effective.count {
        let pk = effective.get_pk(i);
        let v = u64::from_le_bytes(col[i*8..i*8+8].try_into().unwrap());
        *net.entry((pk, v)).or_insert(0) += effective.get_weight(i);
    }
    assert_eq!(net.get(&(1u128, 100u64)).copied().unwrap_or(0), -1,
               "val=100 must net to -1 (retract stored)");
    assert_eq!(net.get(&(1u128, 200u64)).copied().unwrap_or(0), 0,
               "val=200 must net to 0 (insert+delete cancel)");

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_intra_batch_double_insert_same_pk() {
    // [(+1, pk=1, v=A), (+1, pk=1, v=B)] with store (pk=1, v=old).
    // Expected semantics: stored `old` retracted, `A` transient (+1/-1),
    // `B` inserted.
    let dir = temp_dir("intra_batch_dbl_ins");
    let mut engine = CatalogEngine::open(&dir).unwrap();
    let cols = vec![u64_col_def("id"), u64_col_def("val")];
    let tid = engine.create_table("public.t", &cols, 0, true).unwrap();
    let schema = engine.get_schema(tid).unwrap();

    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(1, 0, 1); bb.put_u64(10); bb.end_row();
    engine.dag.ingest_to_family(tid, bb.finish());
    let _ = engine.dag.flush(tid);

    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(1, 0, 1); bb.put_u64(20); bb.end_row();
    bb.begin_row(1, 0, 1); bb.put_u64(30); bb.end_row();
    let effective = engine.ingest_returning_effective(tid, bb.finish()).unwrap();

    let col = effective.col_data(0);
    let mut net: HashMap<(u128, u64), i64> = HashMap::new();
    for i in 0..effective.count {
        let pk = effective.get_pk(i);
        let v = u64::from_le_bytes(col[i*8..i*8+8].try_into().unwrap());
        *net.entry((pk, v)).or_insert(0) += effective.get_weight(i);
    }

    // Bug manifestations this test would have caught:
    //   - double retraction bug: net(1, 10) would be -2, not -1.
    //   - `seen` index bug: net(1, 20) would be +1 (A survives because
    //     the wrong row got negated), and net(1, 30) net 0.
    assert_eq!(net.get(&(1u128, 10u64)).copied().unwrap_or(0), -1,
               "stored row v=10 retracted exactly once");
    assert_eq!(net.get(&(1u128, 20u64)).copied().unwrap_or(0), 0,
               "transient v=20 must net to 0 (inserted then superseded)");
    assert_eq!(net.get(&(1u128, 30u64)).copied().unwrap_or(0), 1,
               "final v=30 must net to +1");

    // And the store after ingest should have a single row (1, 30).
    let scan = engine.scan_family(tid).unwrap();
    assert_eq!(scan.count, 1, "store must hold exactly one live row");
    let scol = scan.col_data(0);
    let live_val = u64::from_le_bytes(scol[0..8].try_into().unwrap());
    assert_eq!(live_val, 30);

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_intra_batch_same_pk_propagates_to_view() {
    // End-to-end: a view watching the base table must see the correct
    // net delta, not ghost negative-weight rows from double retraction.
    let dir = temp_dir("intra_batch_view");
    let mut engine = CatalogEngine::open(&dir).unwrap();
    let cols = vec![u64_col_def("id"), u64_col_def("val")];
    let tid = engine.create_table("public.t", &cols, 0, true).unwrap();
    let schema = engine.get_schema(tid).unwrap();

    let out_cols = vec![
        ("id".to_string(), type_code::U64),
        ("val".to_string(), type_code::U64),
    ];
    let graph = make_passthrough_graph(tid, &out_cols);
    let vid = engine.create_view("public.v", &graph, "").unwrap();

    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(1, 0, 1); bb.put_u64(100); bb.end_row();
    engine.push_and_evaluate(tid, bb.finish()).unwrap();
    assert_eq!(engine.scan_family(vid).unwrap().count, 1);

    // Intra-batch: replace val 100 → 200 → 300.
    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(1, 0, 1); bb.put_u64(200); bb.end_row();
    bb.begin_row(1, 0, 1); bb.put_u64(300); bb.end_row();
    engine.push_and_evaluate(tid, bb.finish()).unwrap();

    let scan = engine.scan_family(vid).unwrap();
    assert_eq!(scan.count, 1, "view must hold exactly one live row");
    let col = scan.col_data(0);
    let v = u64::from_le_bytes(col[0..8].try_into().unwrap());
    assert_eq!(v, 300);

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}
