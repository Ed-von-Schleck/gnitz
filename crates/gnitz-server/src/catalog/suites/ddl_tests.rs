use super::*;
use gnitz_wire::{COLTAB_PAY_NAME, COLTAB_PAY_OWNER_KIND};
use std::collections::HashMap;

// ── test_identifiers ─────────────────────────────────────────────────

#[test]
fn test_identifiers() {
    // Valid names
    for name in &["orders", "Orders123", "my_table", "a", "A1_b2", "1a", "99_problems"] {
        assert!(validate_user_identifier(name).is_ok(), "Rejected valid: {name}");
    }
    // Invalid names
    for name in &[
        "_private",
        "_",
        "_system",
        "__init__",
        "",
        "has space",
        "has-dash",
        "has.dot",
        "has@",
        "table$",
    ] {
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
    let engine = CatalogEngine::open(&dir, 1).unwrap();

    assert!(engine.has_schema("_system"));
    assert!(engine.has_schema("public"));
    assert_eq!(engine.next_table_id, FIRST_USER_TABLE_ID);
    assert_eq!(engine.next_schema_id, FIRST_USER_SCHEMA_ID);

    let schemas_before = count_records(engine.sys_relation(SysFamily::Schema).cursor());
    let tables_before = count_records(engine.sys_relation(SysFamily::Table).cursor());

    engine.close();

    // Idempotent re-open: bootstrap must not duplicate records
    let engine2 = CatalogEngine::open(&dir, 1).unwrap();
    assert_eq!(
        count_records(engine2.sys_relation(SysFamily::Schema).cursor()),
        schemas_before
    );
    assert_eq!(
        count_records(engine2.sys_relation(SysFamily::Table).cursor()),
        tables_before
    );
    engine2.close();

    let _ = fs::remove_dir_all(&dir);
}

/// What a client scanning `_columns` on a fresh server sees: every system
/// family describes itself with one row per column of its wire list, named and
/// in `col_idx` order, under its own id — and nothing else is described.
///
/// The only guard on bootstrap writing one row per wire column, in `col_idx`
/// order and under the right owner. Both sides derive from the same
/// `gnitz-wire` slice, so it cannot catch the schema and its self-description
/// drifting apart.
#[test]
fn bootstrap_self_description_matches_the_wire_column_lists() {
    let dir = temp_dir("bootstrap_self_description");
    let engine = CatalogEngine::open(&dir, 1).unwrap();

    // Every live COL_TAB row, grouped by the table it describes.
    let mut described: HashMap<u64, Vec<(u64, String)>> = HashMap::new();
    let mut c = engine.sys_relation(SysFamily::Column).cursor();
    while c.valid {
        if c.current_weight > 0 {
            let (src, row) = c.current_row_source();
            if payload_u64(src, row, COLTAB_PAY_OWNER_KIND) == OWNER_KIND_TABLE as u64 {
                // COL_TAB PK = `(owner_id, col_idx)`.
                let (owner, col_idx) = gnitz_wire::unpack_pair_pk(c.current_key_narrow());
                let entry = (col_idx, payload_string(src, row, COLTAB_PAY_NAME));
                described.entry(owner).or_default().push(entry);
            }
        }
        c.advance();
    }

    // Driven off `SysFamily::ALL` rather than a hand-written list, so a family
    // added later is covered without touching this test.
    for family in SysFamily::ALL {
        let mut rows = described
            .remove(&(family.id() as u64))
            .unwrap_or_else(|| panic!("family {} describes no columns", family.name()));
        rows.sort_by_key(|(idx, _)| *idx);
        let names: Vec<&str> = rows.iter().map(|(_, n)| n.as_str()).collect();
        let expected: Vec<&str> = family.wire().cols.iter().map(|c| c.name).collect();
        assert_eq!(names, expected, "family {} self-description", family.name());
    }
    // A fresh server holds no user tables, so the system families are the whole
    // of COL_TAB — anything left over is a row describing a table that is not a
    // registered family.
    assert!(
        described.is_empty(),
        "COL_TAB describes unregistered table ids: {:?}",
        described.keys().collect::<Vec<_>>()
    );

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── test_ddl ─────────────────────────────────────────────────────────

#[test]
fn test_ddl() {
    let dir = temp_dir("ddl");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();

    let init_schemas = count_records(engine.sys_relation(SysFamily::Schema).cursor());
    let init_tables = count_records(engine.sys_relation(SysFamily::Table).cursor());
    let init_cols = count_records(engine.sys_relation(SysFamily::Column).cursor());

    // Schema creation
    engine.create_schema("sales").unwrap();
    assert!(engine.has_schema("sales"));
    assert_eq!(
        count_records(engine.sys_relation(SysFamily::Schema).cursor()),
        init_schemas + 1
    );
    assert!(engine.create_schema("sales").is_err()); // duplicate

    // Table creation
    let cols = vec![col_def("id", type_code::U64), col_def("name", type_code::STRING)];
    let tid = engine.create_table("sales.orders", &cols, &[0]).unwrap();
    assert!(engine.registry().has_id(tid));
    assert_eq!(
        count_records(engine.sys_relation(SysFamily::Table).cursor()),
        init_tables + 1
    );
    assert_eq!(
        count_records(engine.sys_relation(SysFamily::Column).cursor()),
        init_cols + 2
    );

    // Drop table (retractions)
    engine.drop_table("sales.orders").unwrap();
    assert!(!engine.registry().has_id(tid));
    assert_eq!(
        count_records(engine.sys_relation(SysFamily::Table).cursor()),
        init_tables
    );
    assert_eq!(
        count_records(engine.sys_relation(SysFamily::Column).cursor()),
        init_cols
    );

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
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
    let cols = vec![col_def("id", type_code::U64)];

    // 1. Drop non-existent schema
    assert!(engine.drop_schema("nonexistent").is_err());
    // 2. Create table in non-existent schema
    assert!(engine.create_table("nonexistent.tbl", &cols, &[0]).is_err());
    // 3. Drop non-existent table
    assert!(engine.drop_table("public.nonexistent").is_err());
    // 4. Duplicate table
    engine.create_table("public.tbl1", &cols, &[0]).unwrap();
    assert!(engine.create_table("public.tbl1", &cols, &[0]).is_err());

    // 5. Drop non-empty schema cascades (PostgreSQL-style): the
    //    contained table goes first, then the schema itself.
    engine.create_schema("my_schema").unwrap();
    engine.create_table("my_schema.tbl2", &cols, &[0]).unwrap();
    engine.drop_schema("my_schema").unwrap();
    assert!(!engine.has_schema("my_schema"));
    assert!(
        engine.get_by_name("my_schema", "tbl2").is_none(),
        "cascade must drop the contained table"
    );

    // 7. Unqualified name defaults to public
    let _tid7 = engine.create_table("tbl3", &cols, &[0]).unwrap();
    assert!(engine.get_by_name("public", "tbl3").is_some());
    engine.drop_table("public.tbl3").unwrap();

    // 8. Unqualified drop
    engine.create_table("public.tbl4", &cols, &[0]).unwrap();
    engine.drop_table("tbl4").unwrap();
    assert!(engine.get_by_name("public", "tbl4").is_none());

    // 9. Invalid PK type (STRING)
    assert!(engine
        .create_table("public.bad_pk", &[col_def("id", type_code::STRING)], &[0])
        .is_err());

    // 10. Too many columns (> MAX_COLUMNS = 65)
    let many: Vec<ColumnDef> = (0..66).map(|i| col_def(&format!("c{i}"), type_code::U64)).collect();
    assert!(engine.create_table("public.too_many", &many, &[0]).is_err());

    // 11. Drop system schema
    assert!(engine.drop_schema("_system").is_err());

    // 12. PK index out of bounds
    assert!(engine.create_table("public.bad_idx", &cols, &[5]).is_err());

    // 13. Recreated schema gets new ID
    engine.create_schema("temp").unwrap();
    let sid1 = engine.schema_id("temp").expect("the schema exists");
    engine.drop_schema("temp").unwrap();
    engine.create_schema("temp").unwrap();
    let sid2 = engine.schema_id("temp").expect("the schema exists");
    assert_ne!(sid1, sid2);
    engine.drop_schema("temp").unwrap();

    // 14. Recreated table gets new ID
    let tid14a = engine.create_table("public.tbl_rc", &cols, &[0]).unwrap();
    engine.drop_table("public.tbl_rc").unwrap();
    let tid14b = engine.create_table("public.tbl_rc", &cols, &[0]).unwrap();
    assert_ne!(tid14a, tid14b);
    engine.drop_table("public.tbl_rc").unwrap();

    // 15. U128 PK support
    let tid15 = engine
        .create_table(
            "public.u128t",
            &[col_def("uuid_pk", type_code::U128), col_def("data", type_code::STRING)],
            &[0],
        )
        .unwrap();
    let s15 = engine.registry().relation(tid15).map(Relation::schema).unwrap();
    assert_eq!(s15.columns[0].type_code, type_code::U128);
    engine.drop_table("public.u128t").unwrap();

    // 18. schema_is_empty
    engine.create_schema("empty_test").unwrap();
    assert!(engine.schema_is_empty("empty_test"));
    engine.create_table("empty_test.tbl", &cols, &[0]).unwrap();
    assert!(!engine.schema_is_empty("empty_test"));
    engine.drop_table("empty_test.tbl").unwrap();
    assert!(engine.schema_is_empty("empty_test"));
    engine.drop_schema("empty_test").unwrap();

    // 19. Names are stored canonically: a mixed-case relation name is refused,
    // because every cache key and qualified name here is compared byte-wise
    // against the folded form the client stores.
    let err = engine.create_table("public.CaseTest", &cols, &[0]).unwrap_err();
    assert!(err.contains("not canonical"), "{err}");
    assert!(engine.get_by_name("public", "CaseTest").is_none());
    engine.create_table("public.casetest", &cols, &[0]).unwrap();
    engine.drop_table("public.casetest").unwrap();

    // 24. Invalid schema ID lookup
    assert_eq!(engine.schema_id("nonexistent"), None);

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── test_nonempty_schema_drop_rejected ───────────────────────────────────
// The engine-side guard in precheck_family rejects a raw SCHEMA_TAB -1 on a
// non-empty schema BEFORE any WAL write, so a member is never orphaned. Every
// DROP SCHEMA empties the schema first — the client's cascade in production, the
// fixture's member loop here — so this bare guard is reachable only by a direct
// retraction, driven here to prove the rejection is loud and orphans nothing.

#[test]
fn test_nonempty_schema_drop_rejected() {
    let dir = temp_dir("nonempty_schema_drop_rejected");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();

    engine.create_schema("s").unwrap();
    let tid = engine
        .create_table("s.t", &[col_def("id", type_code::U64)], &[0])
        .unwrap();
    let sid = engine.schema_id("s").expect("the schema exists");
    assert!(!engine.schema_is_empty("s"), "precondition: schema has a member");

    // A direct SCHEMA_TAB -1 (bypassing any cascade) is rejected by the guard.
    let err = engine.submit_retraction(SysFamily::Schema, sid as u128).unwrap_err();
    assert!(
        err.contains("Schema not empty"),
        "expected a non-empty-schema rejection, got: {err}"
    );

    // Nothing was orphaned: the schema, its member, and the caches all survive.
    assert!(engine.has_schema("s"), "schema must survive a rejected drop");
    assert_eq!(
        engine.get_by_name("s", "t"),
        Some(tid),
        "member row + caches must survive a rejected drop"
    );

    // Emptying the schema first lets the same drop succeed (guard now passes).
    engine.drop_table("s.t").unwrap();
    engine.drop_schema("s").unwrap();
    assert!(!engine.has_schema("s"), "schema gone after dropping its member first");

    let _ = fs::remove_dir_all(&dir);
}

// ── test_restart (with data + sequence recovery) ─────────────────────

#[test]
fn test_restart_full() {
    let dir = temp_dir("restart_full");
    let cols = vec![col_def("id", type_code::U64), col_def("name", type_code::STRING)];
    let first_tid;

    {
        let mut engine = CatalogEngine::open(&dir, 1).unwrap();
        engine.create_schema("marketing").unwrap();
        first_tid = engine.create_table("marketing.products", &cols, &[0]).unwrap();

        // Dropped entities should not reappear
        engine.create_schema("trash").unwrap();
        engine.create_table("trash.items", &cols, &[0]).unwrap();
        engine.drop_table("trash.items").unwrap();
        engine.drop_schema("trash").unwrap();

        engine.close();
    }

    {
        let mut engine = CatalogEngine::open(&dir, 1).unwrap();
        assert!(engine.has_schema("marketing"));
        assert!(engine.get_by_name("marketing", "products").is_some());
        // Dropped should stay gone
        assert!(!engine.has_schema("trash"));
        assert!(engine.get_by_name("trash", "items").is_none());
        // Schema layout rebuilt correctly
        let tid = engine.get_by_name("marketing", "products").unwrap();
        let schema = engine.registry().relation(tid).map(Relation::schema).unwrap();
        assert_eq!(schema.num_columns(), 2);
        // Sequence recovery: new table should get higher ID
        let new_tid = engine.create_table("marketing.other", &cols, &[0]).unwrap();
        assert!(
            new_tid > first_tid,
            "Allocator sequence recovery failed: {new_tid} <= {first_tid}"
        );
        engine.close();
    }

    let _ = fs::remove_dir_all(&dir);
}

/// Regression: verify long strings (> 12 bytes, out-of-line blob) survive
/// restart via the cursor's copy_current_row_into blob offset rewriting.
#[test]
fn test_restart_long_strings() {
    let dir = temp_dir("restart_long_str");
    let long_name = "this_is_a_very_long_table_name_exceeding_inline_threshold";
    assert!(long_name.len() > gnitz_wire::SHORT_STRING_THRESHOLD);

    {
        let mut engine = CatalogEngine::open(&dir, 1).unwrap();
        engine.create_schema("longtest").unwrap();
        let cols = vec![col_def("id", type_code::U64), col_def(long_name, type_code::STRING)];
        engine.create_table("longtest.tbl", &cols, &[0]).unwrap();
        engine.close();
    }
    {
        let mut engine = CatalogEngine::open(&dir, 1).unwrap();
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
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
    let cols = vec![col_def("id", type_code::U64)];

    // #16. Multiple dots in qualified name — second part contains dot
    assert!(engine.create_table("public.schema.tbl", &cols, &[0]).is_err());

    // #17. get_by_name on non-existent returns None
    assert!(engine.get_by_name("public", "nonexistent").is_none());

    // #20. has_id / get_schema for valid and invalid IDs
    let tid = engine.create_table("public.reg_test", &cols, &[0]).unwrap();
    assert!(engine.registry().has_id(tid));
    assert!(engine.registry().relation(tid).map(Relation::schema).is_some());
    assert!(!engine.registry().has_id(999999));
    assert!(engine.registry().relation(999999).map(Relation::schema).is_none());
    engine.drop_table("public.reg_test").unwrap();

    // #26. A relation in a schema the catalog does not hold is rejected. The
    // `_system` schema itself is not: the engine's name rule is
    // `reject_unstorable_name`, deliberately weaker than the client's
    // leading-`_` identifier policy, so a raw bundle may name one.
    assert!(engine.create_table("nosuchschema.new_tbl", &cols, &[0]).is_err());

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── test_nullable_pk_rejected ─────────────────────────────────────────
// The PK region has no null bitmap; the catalog must
// refuse to record a nullable PK regardless of its type.

#[test]
fn test_nullable_pk_rejected() {
    let dir = temp_dir("nullable_pk");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();

    let cols = vec![nullable_def("id", type_code::U64), col_def("name", type_code::STRING)];
    let err = engine.create_table("public.bad_pk_null", &cols, &[0]).unwrap_err();
    assert!(err.contains("nullable"), "expected nullable-PK error, got: {err}");

    // Sanity: same shape with is_nullable=false succeeds.
    let cols_ok = vec![col_def("id", type_code::U64), col_def("name", type_code::STRING)];
    engine.create_table("public.ok_pk", &cols_ok, &[0]).unwrap();

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── test_hook_relation_register_rejects_malformed_pk ─────────────────

// Drives crafted/malformed packed PK values through the production
// wire-ingest path (`ingest_to_family` → `fire_hooks` →
// `hook_relation_register`) and asserts each is rejected with an `Err`
// rather than panicking the server via a `SchemaDescriptor::new`
// `assert!`.
#[test]
fn test_hook_relation_register_rejects_malformed_pk() {
    let dir = temp_dir("pk_reject");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();

    // Columns: [c0 U64 non-null, c1 STRING non-null, c2 U64 nullable, c3 F32 non-null].
    let col_defs = vec![
        col_def("c0", type_code::U64),
        col_def("c1", type_code::STRING),
        nullable_def("c2", type_code::U64),
        col_def("c3", type_code::F32),
    ];
    let tid = engine.allocate_table_id().unwrap();
    engine.write_column_records(tid, OWNER_KIND_TABLE, &col_defs).unwrap();

    let mut assert_rejects = |raw_pk_cols: u64, snippet: &str| {
        let batch = build_table_tab_row(tid, raw_pk_cols, "bad_table");
        let res = engine.ingest_to_family(TABLE_TAB_ID, &batch);
        let err = res.expect_err(&format!("expected Err containing '{snippet}', got Ok"));
        assert!(err.contains(snippet), "expected '{snippet}', got: {err}");
    };

    // A flag-clear word names no list at all, rejected at the decode before any
    // count is read.
    assert_rejects(0, "carries no packed-list flag");
    // Count out of range, rejected at the decode.
    assert_rejects(PK_LIST_PACKED_FLAG, "at least one column"); // count 0
    assert_rejects(PK_LIST_PACKED_FLAG | 5, "out of range 1..=4"); // count 5
    assert_rejects(PK_LIST_PACKED_FLAG | 15, "out of range 1..=4"); // count 15
                                                                    // Out-of-bounds index (index 5, only 3 columns).
    assert_rejects(PK_LIST_PACKED_FLAG | 1 | (5 << 4), "out of bounds");
    // Duplicate index [0, 0]. The `(0 << N)` forms document the slot layout
    // (count=2, idx0@bit4=0, idx1@bit11=0) even though they evaluate to 0.
    #[allow(clippy::identity_op)]
    let packed = PK_LIST_PACKED_FLAG | 2 | (0u64 << 4) | (0u64 << 11);
    assert_rejects(packed, "names column 0 twice");
    // Non-integer PK column (c1 is STRING).
    assert_rejects(PK_LIST_PACKED_FLAG | 1 | (1 << 4), "only fixed-width integer");
    // Float PK column (c3 is F32) — floats break the byte-equal PK contract.
    assert_rejects(PK_LIST_PACKED_FLAG | 1 | (3 << 4), "only fixed-width integer");
    // Nullable PK column (c2 is nullable).
    assert_rejects(PK_LIST_PACKED_FLAG | 1 | (2 << 4), "must not be nullable");

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── test_pk_list_round_trips_into_registered_schema ──────────────────

// `TABLE_TAB.pk_col_idx` is reconstructed element-for-element into the
// registered schema's PK list, for a single-column and for compound PKs.
#[test]
fn test_pk_list_round_trips_into_registered_schema() {
    let dir = temp_dir("pk_list_roundtrip");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();

    let cols = vec![col_def("a", type_code::U64), col_def("b", type_code::U64)];

    // Single-column PK, and a recreate under a different PK column: the
    // reconstructed schema reflects the new list, not the old one.
    let tid = engine.create_table("public.t", &cols, &[0]).unwrap();
    assert_eq!(
        engine
            .registry()
            .relation(tid)
            .map(Relation::schema)
            .unwrap()
            .pk_indices(),
        &[0]
    );
    engine.drop_table("public.t").unwrap();
    let tid2 = engine.create_table("public.t", &cols, &[1]).unwrap();
    assert_eq!(
        engine
            .registry()
            .relation(tid2)
            .map(Relation::schema)
            .unwrap()
            .pk_indices(),
        &[1]
    );

    // Compound PK lists round-trip in full.
    let wide: Vec<_> = (0..8).map(|i| col_def(&format!("c{i}"), type_code::U64)).collect();
    for (n, pk_cols) in [vec![0u32, 1], vec![0u32, 3, 5], vec![1u32, 2, 7]]
        .into_iter()
        .enumerate()
    {
        let name = format!("public.w{n}");
        let tid = engine.create_table(&name, &wide, &pk_cols).unwrap();
        assert_eq!(
            engine
                .registry()
                .relation(tid)
                .map(Relation::schema)
                .unwrap()
                .pk_indices(),
            pk_cols.as_slice(),
            "compound PK list must round-trip in full (input={pk_cols:?})"
        );
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
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();

    // A view needs a base table to reference.
    let base_cols = vec![col_def("id", type_code::U64)];
    engine.create_table("public.base", &base_cols, &[0]).unwrap();

    // Register a view via the raw system-table path (create_view was removed).
    // Column records must precede the VIEW_TAB row (hook invariant).
    let vid = engine.next_table_id;
    let view_cols = vec![col_def("id", type_code::U64)];
    engine.write_column_records(vid, OWNER_KIND_VIEW, &view_cols).unwrap();

    let batch = build_view_tab_row(vid, "myview");
    engine.ingest_to_family(VIEW_TAB_ID, &batch).unwrap();

    // The register hook created the physical view directory on disk.
    let view_dir = engine
        .registry()
        .relation_or_err(vid)
        .expect("view registered in dag")
        .directory()
        .to_string();
    assert!(
        std::path::Path::new(&view_dir).exists(),
        "view dir should exist after create: {view_dir}"
    );

    // Drop removes catalog metadata and leaves the physical directory to the
    // orphan sweep, which production runs once every worker has applied the
    // drop; drive both steps directly here.
    engine.drop_view("public.myview").unwrap();
    let _ = engine.drain_pending_broadcasts();
    engine.reclaim_orphan_dirs();
    assert!(
        !std::path::Path::new(&view_dir).exists(),
        "view dir must be deleted by the sweep after the drop: {view_dir}"
    );

    // No double-drop: the view is gone from the catalog.
    assert!(engine.drop_view("public.myview").is_err(), "second drop must error");

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── test_drop_view_cascades_columns_and_circuit_rows ─────────────────
// DROP VIEW retracts only the VIEW_TAB row; the expanded cascade must clear both
// the view's column rows and its circuit rows.

#[test]
fn test_drop_view_cascades_columns_and_circuit_rows() {
    let dir = temp_dir("drop_view_cascade");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();

    let base_tid = engine
        .create_table("public.base", &[col_def("id", type_code::U64)], &[0])
        .unwrap();

    // Baseline: system + base-table column rows; no circuit rows yet.
    let base_cols = count_records(engine.sys_relation(SysFamily::Column).cursor());
    let base_nodes = count_records(engine.sys_relation(SysFamily::CircuitNodes).cursor());

    // Register a view (column and circuit records precede the VIEW_TAB row).
    let vid = engine.next_table_id;
    let view_cols = vec![col_def("id", type_code::U64)];
    write_identity_circuit(&mut engine, vid, base_tid, None);
    engine.write_column_records(vid, OWNER_KIND_VIEW, &view_cols).unwrap();

    let batch = build_view_tab_row(vid, "depview");
    engine.ingest_to_family(VIEW_TAB_ID, &batch).unwrap();

    assert!(
        count_records(engine.sys_relation(SysFamily::Column).cursor()) > base_cols,
        "view column rows must be present before drop"
    );
    assert!(
        count_records(engine.sys_relation(SysFamily::CircuitNodes).cursor()) > base_nodes,
        "circuit node rows must be present before drop"
    );

    // Drop the view: the VIEW_TAB -1 cascade must retract both families.
    engine.drop_view("public.depview").unwrap();

    assert_eq!(
        count_records(engine.sys_relation(SysFamily::Column).cursor()),
        base_cols,
        "sys_columns must return to baseline after drop_view (column cascade)"
    );
    assert_eq!(
        count_records(engine.sys_relation(SysFamily::CircuitNodes).cursor()),
        base_nodes,
        "circuit rows must return to baseline after drop_view (circuit cascade)"
    );

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── ddl_emitters_use_no_raw_handle_capability ────────────────────────
// Capability guard: a DDL emitter mutates catalog state only by submitting a
// delta (submit / submit_retraction). It may READ a family's
// store — `submit_retraction` copies the live row it is about to negate — but it
// must never write one directly, which would skip the precheck, the hooks and
// the broadcast queue in one line. Pinned as source text because no runtime
// assertion can observe the absence of a call.
//
// Each forbidden name is one the fixture could write and must not:
// `registry.ingest` is deliberately absent, since `ingest_to_family` calls it
// for a user table and routes every system id to `submit`.

#[test]
fn ddl_emitters_use_no_raw_handle_capability() {
    let src = include_str!("ddl_fixture.rs");
    for forbidden in ["ingest_owned_batch", "ingest_borrowed", "apply_family"] {
        assert!(
            !src.contains(forbidden),
            "a DDL emitter must not call {forbidden} — emit a delta via submit instead"
        );
    }
}

// ── drop_cascade_broadcasts_index_owner_columns_in_order ─────────────
// Workers apply the queue in this order, which no end-state assertion can see.
#[test]
fn drop_cascade_broadcasts_index_owner_columns_in_order() {
    let dir = temp_dir("drop_cascade_broadcast_order");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();

    // Table with one (non-unique) secondary index.
    let cols = vec![col_def("id", type_code::U64), col_def("val", type_code::I64)];
    let tid = engine.create_table("public.t", &cols, &[0]).unwrap();
    engine.create_index("public.t", &["val"], false).unwrap();

    // Clear the broadcasts accumulated by create_table + create_index.
    let _ = engine.drain_pending_broadcasts();

    // Submit the table retraction; `submit` retracts the owned index (IDX) ahead
    // of it and the columns (COL) behind it.
    engine.submit_retraction(SysFamily::Table, tid as u128).unwrap();

    // Collect the broadcast family-id sequence.
    let tids: Vec<i64> = engine.drain_pending_broadcasts().iter().map(|(f, _)| f.id()).collect();

    let pos = |id: i64| tids.iter().position(|&t| t == id);
    let idx_pos = pos(IDX_TAB_ID).unwrap_or_else(|| panic!("IDX_TAB retraction must be broadcast; seq={tids:?}"));
    let col_pos = pos(COL_TAB_ID).unwrap_or_else(|| panic!("COL_TAB retraction must be broadcast; seq={tids:?}"));
    let tab_pos = pos(TABLE_TAB_ID).unwrap_or_else(|| panic!("TABLE_TAB retraction must be broadcast; seq={tids:?}"));

    assert!(
        idx_pos < tab_pos,
        "secondary-index retraction must broadcast before its owner table: seq={tids:?}"
    );
    assert!(
        tab_pos < col_pos,
        "column retraction must broadcast after its owner table: seq={tids:?}"
    );

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── table_retract_applies_qname_before_id ────────────────────────────
// Within apply_entity_caches' retract arm, the qname is removed BEFORE the
// entity_by_id entry. That order matters: the qualified name is
// reconstructed from entity_by_id (still present) to remove the
// entity_by_qname entry. Reverse the two and the -1 reads an
// already-removed entity_by_id, leaks a stale entity_by_qname[qn] → old
// tid, and the recreate is then rejected by the qname-uniqueness guard (or
// resolves to the wrong tid).
//
// End-state cache assertions on a single create/drop miss this: both orders
// leave the same caches. The teeth show only across a drop + same-name
// recreate.
#[test]
fn table_retract_applies_qname_before_id() {
    let dir = temp_dir("table_retract_qname_before_id");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();

    let cols = vec![col_def("id", type_code::U64), col_def("val", type_code::I64)];

    // Create, then drop, a table named `public.t`.
    let tid1 = engine.create_table("public.t", &cols, &[0]).unwrap();
    assert_eq!(engine.caches.entity_by_qname.get("public.t").copied(), Some(tid1));
    engine.drop_table("public.t").unwrap();

    // The drop must have cleared the qname mapping; a reversed qname/id
    // retraction order leaves it pointing at the dropped tid.
    assert!(
        !engine.caches.entity_by_qname.contains_key("public.t"),
        "drop must clear entity_by_qname; a reversed retraction order leaks a stale mapping"
    );

    // Recreate under the same qualified name. A leaked stale mapping would
    // make the qname-uniqueness guard reject this create.
    let recreate = engine.create_table("public.t", &cols, &[0]);
    assert!(
        recreate.is_ok(),
        "recreate of a dropped same-name table must succeed; a stale qname mapping blocks it: {recreate:?}"
    );
    let tid2 = recreate.unwrap();
    assert_ne!(tid1, tid2, "recreated table must get a fresh tid");

    // entity_by_qname must resolve to the NEW tid.
    assert_eq!(
        engine.caches.entity_by_qname.get("public.t").copied(),
        Some(tid2),
        "entity_by_qname must resolve to the NEW tid after drop + recreate"
    );

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── replicated_bit_is_transitive_and_survives_replay ─────────────────
// A view is stamped replicated iff every source it scans is, so the property
// climbs a view chain: base → producer → consumer. Registration reads its
// sources' already-registered state, so `hook_relation_register` must process a batch
// in dependency order — which neither order it sees is. Both are reproduced here:
// the live batch carries the consumer before the producer, and replay walks
// VIEW_TAB in PK order, in which the consumer's id is the LOWER one (a chain's
// user-named view is minted before its body is bound). Get it wrong and the
// consumer stamps `false`, flipping its store to Hashed across a restart.
#[test]
fn replicated_bit_is_transitive_and_survives_replay() {
    let dir = temp_dir("replicated_bit_transitive");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();

    let cols = vec![col_def("id", type_code::U64), col_def("x", type_code::I64)];

    // A REPLICATED base table.
    let rt = create_flagged_table(&mut engine, "rt", &cols, &[0], replicated_flags());

    // A partitioned base table, for the negative direction.
    let pt = engine.create_table("public.pt", &cols, &[0]).unwrap();

    // Consumer ids allocated BEFORE their producers, on both chains.
    let r_consumer = engine.allocate_table_id().unwrap();
    let r_producer = engine.allocate_table_id().unwrap();
    let p_consumer = engine.allocate_table_id().unwrap();
    let p_producer = engine.allocate_table_id().unwrap();

    for (vid, src) in [
        (r_producer, rt),
        (r_consumer, r_producer),
        (p_producer, pt),
        (p_consumer, p_producer),
    ] {
        write_identity_circuit(&mut engine, vid, src, None);
        engine.write_column_records(vid, OWNER_KIND_VIEW, &cols).unwrap();
    }

    // One VIEW_TAB batch, consumers first — the dependency-reversed row order.
    let mut bb = BatchBuilder::new(*SysFamily::View.schema());
    for (vid, name) in [
        (r_consumer, "rv2"),
        (p_consumer, "pv2"),
        (r_producer, "rv"),
        (p_producer, "pv"),
    ] {
        push_view_tab_row(&mut bb, 1, vid, name, 0, 0, 0);
    }
    engine.ingest_to_family(VIEW_TAB_ID, &bb.finish()).unwrap();

    // (replicated, depth) for a registered relation.
    let stamp = |e: &mut CatalogEngine, id: i64| {
        let CatalogEngine { registry, dag, .. } = e;
        let t = registry.relation_or_err(id).expect("registered");
        (t.is_replicated(), dag.depth_of(registry, id))
    };
    let assert_stamps = |e: &mut CatalogEngine, when: &str| {
        assert!(stamp(e, rt).0, "replicated base table ({when})");
        assert_eq!(stamp(e, r_producer), (true, 1), "view over a replicated table ({when})");
        assert_eq!(
            stamp(e, r_consumer),
            (true, 2),
            "view over a replicated VIEW — the bit and the depth must both climb ({when})"
        );
        assert_eq!(stamp(e, p_producer), (false, 1), "view over a table ({when})");
        assert_eq!(stamp(e, p_consumer), (false, 2), "view over a view ({when})");
    };
    assert_stamps(&mut engine, "live CREATE");

    engine.close();

    let mut engine2 = CatalogEngine::open(&dir, 1).unwrap();
    assert_stamps(&mut engine2, "after replay");
    engine2.close();

    let _ = fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// Delta feeds (`WITH (delta = …)`)
// ---------------------------------------------------------------------------

/// `capacity` and `delta` are refused **together**, here as well as at the SQL
/// layer. This is the trust boundary: driven from a registration the SQL planner
/// cannot produce, so a test that only went through SQL would pass with the rule
/// absent.
///
/// The reason is not cost and not effort. A bounded view's read hydrates its
/// missing keys from the *source relation's live store*, which `handle_push`
/// advances outside any tick — so a `Delta(0)` over a partly-dehydrated view
/// reports round `T` while already carrying an un-ticked push, and the next poll
/// delivers that same push again as round `T+1`, at double weight, with no error
/// and no row-set difference.
#[test]
fn a_view_declaring_both_capacity_and_delta_is_rejected() {
    let dir = temp_dir("capacity_and_delta_together");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
    let cols = vec![
        crate::test_support::col_def("id", gnitz_wire::type_code::U64),
        crate::test_support::col_def("v", gnitz_wire::type_code::I64),
    ];
    let tid = create_flagged_table(&mut engine, "t", &cols, &[0], 0);

    let err = try_register_identity_view(&mut engine, tid, "both", &cols, 4 << 20, 4 << 20)
        .expect_err("the pair must be refused");
    assert!(err.contains("capacity") && err.contains("delta"), "got: {err}");

    // Either alone is accepted, so the rejection is about the pair.
    try_register_identity_view(&mut engine, tid, "bounded", &cols, 4 << 20, 0).expect("capacity alone");
    try_register_identity_view(&mut engine, tid, "fed", &cols, 0, 4 << 20).expect("delta alone");

    std::fs::remove_dir_all(&dir).ok();
}

/// A fed view's delta store prepends a `_tick` key column, so a view already at
/// `MAX_COLUMNS` cannot carry a feed. Refused at registration, on every process:
/// the post-fork master opens no user store at all, so leaving it to the store
/// open would be a worker-side fatal abort taken after the client was told the
/// CREATE succeeded.
#[test]
fn a_view_at_the_column_limit_cannot_carry_a_feed() {
    let dir = temp_dir("fed_view_at_the_column_limit");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
    let cols: Vec<ColumnDef> = (0..gnitz_wire::MAX_COLUMNS)
        .map(|i| crate::test_support::col_def(&format!("c{i}"), gnitz_wire::type_code::U64))
        .collect();
    let tid = create_flagged_table(&mut engine, "wide", &cols, &[0], 0);

    let err = try_register_identity_view(&mut engine, tid, "fed_wide", &cols, 0, 4 << 20)
        .expect_err("the stamp is a 66th column");
    assert!(err.contains("delta feed"), "got: {err}");

    // The same view without a feed is fine, so the rejection is about the stamp.
    try_register_identity_view(&mut engine, tid, "plain_wide", &cols, 0, 0).expect("unfed wide view");

    std::fs::remove_dir_all(&dir).ok();
}

// ── duplicate visible column names ───────────────────────────────────

// Driven through the wire-ingest path, since the SQL planner rejects the shape
// before it gets here. A stream carries the rule because it is an ingestion
// point; a hidden column does not, being invisible to name resolution.
#[test]
fn duplicate_visible_column_names_are_rejected_for_a_table_and_a_stream() {
    let dir = temp_dir("duplicate_column_names");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();

    for (name, flags) in [("dup_table", 0), ("dup_stream", stream_flags())] {
        let col_defs = vec![
            col_def("id", type_code::U64),
            col_def("a", type_code::I64),
            // The catalog folds names, so a case difference is the same name.
            col_def("A", type_code::I64),
        ];
        let tid = engine.allocate_table_id().unwrap();
        engine.write_column_records(tid, OWNER_KIND_TABLE, &col_defs).unwrap();
        let batch = build_table_tab_row_flags(tid, pack_pk_cols(&[0]), name, flags);
        let err = engine
            .ingest_to_family(TABLE_TAB_ID, &batch)
            .expect_err("a duplicate visible column name must be refused");
        assert!(err.contains("duplicate column name"), "{name}: {err}");
    }

    // A hidden column repeating a visible name still registers.
    let col_defs = vec![
        col_def("id", type_code::U64),
        col_def("a", type_code::I64),
        ColumnDef {
            is_hidden: true,
            ..col_def("a", type_code::I64)
        },
    ];
    let tid = engine.allocate_table_id().unwrap();
    engine.write_column_records(tid, OWNER_KIND_TABLE, &col_defs).unwrap();
    let batch = build_table_tab_row_flags(tid, pack_pk_cols(&[0]), "hidden_dup", 0);
    engine.ingest_to_family(TABLE_TAB_ID, &batch).unwrap();

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── One TABLE_TAB batch drops several tables, one batch per family ───────────

#[test]
fn set_based_table_drop_queues_one_batch_per_family() {
    let dir = temp_dir("set_based_table_drop");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
    let cols = vec![col_def("id", type_code::U64), col_def("val", type_code::I64)];
    let mut tids = Vec::new();
    for name in ["a", "b", "c"] {
        tids.push(engine.create_table(&format!("public.{name}"), &cols, &[0]).unwrap());
        engine.create_index(&format!("public.{name}"), &["val"], false).unwrap();
    }
    let _ = engine.drain_pending_broadcasts();

    let drop = engine.retract_pk_list(SysFamily::Table, tids.iter().map(|&t| t as u128).collect());
    engine.submit(SysFamily::Table, drop).unwrap();

    let families: Vec<SysFamily> = engine.drain_pending_broadcasts().into_iter().map(|(f, _)| f).collect();
    assert_eq!(families, [SysFamily::Index, SysFamily::Table, SysFamily::Column]);
    assert!(tids.iter().all(|&t| !engine.registry().has_id(t)));

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── A view drop retracts the internal segments it owns ───────────────────────

/// Register view `V` over a fresh base table, then segment `S` whose VIEW_TAB
/// row names `V` as its owner. Returns `(V, S)`.
fn view_with_segment(engine: &mut CatalogEngine) -> (i64, i64) {
    let base = engine
        .create_table("public.base", &[col_def("id", type_code::U64)], &[0])
        .unwrap();
    let cols = vec![col_def("id", type_code::U64)];
    let register = |engine: &mut CatalogEngine, name: &str, owner: i64| {
        let vid = engine.next_table_id;
        write_identity_circuit(engine, vid, base, None);
        engine.write_column_records(vid, OWNER_KIND_VIEW, &cols).unwrap();
        let mut bb = BatchBuilder::new(*SysFamily::View.schema());
        push_view_tab_row(&mut bb, 1, vid, name, 0, 0, owner);
        engine.submit(SysFamily::View, bb.finish()).unwrap();
        vid
    };
    let v = register(engine, "v", 0);
    let s = register(engine, "v__seg", v);
    (v, s)
}

/// Live rows of pair-keyed `family` under `leading`.
fn band_rows(engine: &CatalogEngine, family: SysFamily, leading: i64) -> usize {
    let (start, end) = family.band(leading);
    let (cursor, _) = engine
        .sys_relation(family)
        .range_cursor(start.pk_bytes(), Some(end.pk_bytes()));
    count_records(cursor)
}

#[test]
fn view_drop_retracts_its_segments() {
    let dir = temp_dir("view_drop_segments");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
    let (v, s) = view_with_segment(&mut engine);
    let _ = engine.drain_pending_broadcasts();

    engine.submit_retraction(SysFamily::View, v as u128).unwrap();

    let families: Vec<SysFamily> = engine.drain_pending_broadcasts().into_iter().map(|(f, _)| f).collect();
    assert_eq!(families, [SysFamily::View, SysFamily::CircuitNodes, SysFamily::Column]);
    for id in [v, s] {
        assert!(!engine.registry().has_id(id));
        assert_eq!(band_rows(&engine, SysFamily::CircuitNodes, id), 0);
        assert_eq!(band_rows(&engine, SysFamily::Column, id), 0);
    }

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn dropping_a_view_with_its_segment_retracts_the_segment_once() {
    let dir = temp_dir("view_drop_segment_once");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
    let (v, s) = view_with_segment(&mut engine);

    let drop = engine.retract_pk_list(SysFamily::View, vec![v as u128, s as u128]);
    engine.submit(SysFamily::View, drop).unwrap();

    assert!(!engine.registry().has_id(v) && !engine.registry().has_id(s));
    assert_eq!(
        count_negative_records(engine.sys_relation(SysFamily::View).cursor()),
        0,
        "the segment must not be retracted twice"
    );

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}
