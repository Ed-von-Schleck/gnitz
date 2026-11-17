use super::*;
use crate::test_support::u64_i64_cols;
use gnitz_store::relation::PkRule;
use gnitz_wire::{ColTabRow, ColumnDef};
use std::collections::HashMap;

// ── test_bootstrap ───────────────────────────────────────────────────

#[test]
fn test_bootstrap() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().to_str().unwrap();
    let engine = CatalogEngine::open(dir, 1).unwrap();

    assert!(engine.schema_id("_system").is_some());
    assert!(engine.schema_id("public").is_some());
    assert_eq!(engine.next_catalog_id(), gnitz_wire::FIRST_USER_TABLE_ID);

    let schemas_before = count_records(engine.sys_relation(SysFamily::Schema).cursor());
    let tables_before = count_records(engine.sys_relation(SysFamily::Table).cursor());

    engine.close();

    // Idempotent re-open: bootstrap must not duplicate records
    let engine2 = CatalogEngine::open(dir, 1).unwrap();
    assert_eq!(
        count_records(engine2.sys_relation(SysFamily::Schema).cursor()),
        schemas_before
    );
    assert_eq!(
        count_records(engine2.sys_relation(SysFamily::Table).cursor()),
        tables_before
    );
    engine2.close();
}

/// What a client scanning `columns` on a fresh server sees: every system
/// family describes itself with one row per column of its wire list, named and
/// in `col_idx` order, under its own id — and nothing else is described.
///
/// The only guard on bootstrap writing one row per wire column, in `col_idx`
/// order and under the right owner. Both sides derive from the same
/// `gnitz-wire` slice, so it cannot catch the schema and its self-description
/// drifting apart.
#[test]
fn bootstrap_self_description_matches_the_wire_column_lists() {
    let (_dir, engine) = empty_catalog(1);
    assert_self_description(&engine);
    engine.close();
}

fn assert_self_description(engine: &CatalogEngine) {
    // Every live COL_TAB row, grouped by the table it describes.
    let mut described: HashMap<u64, Vec<(u64, String)>> = HashMap::new();
    let mut c = engine.sys_relation(SysFamily::Column).cursor();
    while c.is_valid() {
        if c.current_weight() > 0 {
            let (src, row) = c.current_row_source();
            let r = ColTabRow::read(src, row).unwrap();
            described
                .entry(r.owner_id)
                .or_default()
                .push((r.col_idx, r.name.into_owned()));
        }
        c.advance();
    }

    // Driven off `SysFamily::ALL` rather than a hand-written list, so a family
    // added later is covered without touching this test.
    for family in SysFamily::ALL {
        let mut rows = described
            .remove(&{ family.id() })
            .unwrap_or_else(|| panic!("family {} describes no columns", family.name()));
        rows.sort_by_key(|(idx, _)| *idx);
        let names: Vec<&str> = rows.iter().map(|(_, n)| n.as_str()).collect();
        let expected: Vec<&str> = family.def().cols.iter().map(|c| c.name).collect();
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
}

/// Open a fresh database, close it, and unlink `family`'s manifest — a first
/// boot that crashed inside its flush barrier with every other family published.
fn fresh_db_missing_manifest(family: SysFamily) -> tempfile::TempDir {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().to_str().unwrap();
    CatalogEngine::open(dir, 1).unwrap().close();
    let manifest = format!("{}/manifest.bin", relation_dir(dir, family.id()));
    fs::remove_file(&manifest).unwrap();
    tmp
}

/// A family whose manifest never published is seeded again, and only it: the
/// families that did publish keep their seed rows at weight 1.
#[test]
fn an_unpublished_tables_family_is_reseeded_alone() {
    let tmp = fresh_db_missing_manifest(SysFamily::Table);
    let dir = tmp.path().to_str().unwrap();
    let engine = CatalogEngine::open(dir, 1).unwrap();

    let col_rows: usize = SysFamily::ALL.iter().map(|f| f.def().cols.len()).sum();
    for (family, rows) in [
        (SysFamily::Schema, 2),
        (SysFamily::Column, col_rows),
        (SysFamily::Table, SysFamily::COUNT),
    ] {
        let weights = weights(engine.sys_relation(family).cursor());
        assert_eq!(weights.len(), rows, "{} row count", family.name());
        assert!(
            weights.iter().all(|&w| w == 1),
            "{} weights: {weights:?}",
            family.name()
        );
    }

    engine.close();
}

/// An unpublished `columns` does not stop the open: the system families'
/// entries are compile-time data, and the reseed restores the self-description.
#[test]
fn an_unpublished_columns_family_is_reseeded() {
    let tmp = fresh_db_missing_manifest(SysFamily::Column);
    let dir = tmp.path().to_str().unwrap();
    let engine = CatalogEngine::open(dir, 1).unwrap();
    assert_self_description(&engine);
    engine.close();
}

// ── test_ddl ─────────────────────────────────────────────────────────

#[test]
fn test_ddl() {
    let (_dir, mut engine) = empty_catalog(1);

    let init_schemas = count_records(engine.sys_relation(SysFamily::Schema).cursor());
    let init_tables = count_records(engine.sys_relation(SysFamily::Table).cursor());
    let init_cols = count_records(engine.sys_relation(SysFamily::Column).cursor());

    // Schema creation
    engine.create_schema("sales").unwrap();
    assert!(engine.schema_id("sales").is_some());
    assert_eq!(
        count_records(engine.sys_relation(SysFamily::Schema).cursor()),
        init_schemas + 1
    );
    assert!(
        engine
            .create_schema("sales")
            .unwrap_err()
            .contains("Schema already exists")
    );

    // Table creation
    let cols = vec![col_def("id", TypeCode::U64), col_def("name", TypeCode::String)];
    let tid = engine.create_table("sales.orders", &cols, &[0]).unwrap();
    assert!(engine.registry.relation(tid).is_some());
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
    assert!(engine.registry.relation(tid).is_none());
    assert_eq!(
        count_records(engine.sys_relation(SysFamily::Table).cursor()),
        init_tables
    );
    assert_eq!(
        count_records(engine.sys_relation(SysFamily::Column).cursor()),
        init_cols
    );

    // A system family's TABLE_TAB row sits below the user id floor, so its
    // retraction is refused.
    assert!(
        engine
            .drop_table("_system.columns")
            .unwrap_err()
            .contains("cannot DROP a system")
    );

    // Drop schema
    engine.create_schema("temp").unwrap();
    engine.drop_schema("temp").unwrap();
    assert!(engine.schema_id("temp").is_none());

    engine.close();
}

// ── test_edge_cases ───────────────────────────────────────────────────

#[test]
fn test_edge_cases() {
    let (_dir, mut engine) = empty_catalog(1);
    let cols = vec![col_def("id", TypeCode::U64)];

    // Drop non-empty schema cascades (PostgreSQL-style): the
    //    contained table goes first, then the schema itself.
    engine.create_schema("my_schema").unwrap();
    engine.create_table("my_schema.tbl2", &cols, &[0]).unwrap();
    engine.drop_schema("my_schema").unwrap();
    assert!(engine.schema_id("my_schema").is_none());
    assert!(
        engine.get_by_name("my_schema", "tbl2").is_none(),
        "cascade must drop the contained table"
    );

    // Too many columns (> MAX_COLUMNS = 65)
    let many: Vec<CatalogColumn> = (0..66).map(|i| col_def(&format!("c{i}"), TypeCode::U64)).collect();
    assert!(engine.create_table("public.too_many", &many, &[0]).is_err());

    // Drop system schema
    assert!(
        engine
            .drop_schema("_system")
            .unwrap_err()
            .contains("cannot DROP a system")
    );

    // Recreated schema gets new ID
    engine.create_schema("temp").unwrap();
    let sid1 = engine.schema_id("temp").expect("the schema exists");
    engine.drop_schema("temp").unwrap();
    engine.create_schema("temp").unwrap();
    let sid2 = engine.schema_id("temp").expect("the schema exists");
    assert_ne!(sid1, sid2);
    engine.drop_schema("temp").unwrap();

    // U128 PK support
    let tid15 = engine
        .create_table(
            "public.u128t",
            &[col_def("uuid_pk", TypeCode::U128), col_def("data", TypeCode::String)],
            &[0],
        )
        .unwrap();
    let s15 = engine.registry.relation(tid15).map(Relation::schema).unwrap();
    assert_eq!(s15.columns()[0].type_code, TypeCode::U128);
    engine.drop_table("public.u128t").unwrap();

    // Names are stored canonically: a mixed-case relation name is refused,
    // because every cache key and qualified name here is compared byte-wise
    // against the folded form the client stores.
    let err = engine.create_table("public.CaseTest", &cols, &[0]).unwrap_err();
    assert!(err.contains("not canonical"), "{err}");
    assert!(engine.get_by_name("public", "CaseTest").is_none());
    engine.create_table("public.casetest", &cols, &[0]).unwrap();
    engine.drop_table("public.casetest").unwrap();

    // Multiple dots in qualified name — second part contains dot
    assert!(engine.create_table("public.schema.tbl", &cols, &[0]).is_err());

    engine.close();
}

// ── test_nonempty_schema_drop_rejected ───────────────────────────────────
// precheck_family refuses a bare SCHEMA_TAB -1 on a non-empty schema before
// anything is written; a DROP SCHEMA cascade never sends one.

#[test]
fn test_nonempty_schema_drop_rejected() {
    let (_dir, mut engine) = empty_catalog(1);

    engine.create_schema("s").unwrap();
    let tid = engine
        .create_table("s.t", &[col_def("id", TypeCode::U64)], &[0])
        .unwrap();
    let sid = engine.schema_id("s").expect("the schema exists");
    assert!(
        engine.caches.relation_by_name.contains_key(&sid),
        "precondition: schema has a member"
    );

    // A direct SCHEMA_TAB -1 (bypassing any cascade) is rejected by the guard.
    let err = engine.submit_retraction(SysFamily::Schema, sid).unwrap_err();
    assert!(
        err.contains("Schema not empty"),
        "expected a non-empty-schema rejection, got: {err}"
    );

    // Nothing was orphaned: the schema, its member, and the caches all survive.
    assert!(engine.schema_id("s").is_some(), "schema must survive a rejected drop");
    assert_eq!(
        engine.get_by_name("s", "t"),
        Some(tid),
        "member row + caches must survive a rejected drop"
    );

    // Emptying the schema first lets the same drop succeed (guard now passes).
    engine.drop_table("s.t").unwrap();
    engine.drop_schema("s").unwrap();
    assert!(
        engine.schema_id("s").is_none(),
        "schema gone after dropping its member first"
    );
}

// ── test_restart (with data + sequence recovery) ─────────────────────

#[test]
fn test_restart_full() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().to_str().unwrap();
    let cols = vec![col_def("id", TypeCode::U64), col_def("name", TypeCode::String)];
    let first_tid;

    {
        let mut engine = CatalogEngine::open(dir, 1).unwrap();
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
        let mut engine = CatalogEngine::open(dir, 1).unwrap();
        assert!(engine.schema_id("marketing").is_some());
        assert!(engine.get_by_name("marketing", "products").is_some());
        // Dropped should stay gone
        assert!(engine.schema_id("trash").is_none());
        assert!(engine.get_by_name("trash", "items").is_none());
        // Schema layout rebuilt correctly
        let tid = engine.get_by_name("marketing", "products").unwrap();
        let schema = engine.registry.relation(tid).map(Relation::schema).unwrap();
        assert_eq!(schema.num_columns(), 2);
        // Sequence recovery: new table should get higher ID
        let new_tid = engine.create_table("marketing.other", &cols, &[0]).unwrap();
        assert!(
            new_tid > first_tid,
            "Allocator sequence recovery failed: {new_tid} <= {first_tid}"
        );
        engine.close();
    }
}

/// Regression: verify long strings (> 12 bytes, out-of-line blob) survive
/// restart via the cursor's copy_current_row_into blob offset rewriting.
#[test]
fn test_restart_long_strings() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().to_str().unwrap();
    let long_name = "this_is_a_very_long_table_name_exceeding_inline_threshold";
    assert!(long_name.len() > gnitz_wire::SHORT_STRING_THRESHOLD);

    {
        let mut engine = CatalogEngine::open(dir, 1).unwrap();
        engine.create_schema("longtest").unwrap();
        let cols = vec![col_def("id", TypeCode::U64), col_def(long_name, TypeCode::String)];
        engine.create_table("longtest.tbl", &cols, &[0]).unwrap();
        engine.close();
    }
    {
        let engine = CatalogEngine::open(dir, 1).unwrap();
        assert!(engine.get_by_name("longtest", "tbl").is_some());
        // Verify column name survived out-of-line blob round-trip
        let tid = engine.get_by_name("longtest", "tbl").unwrap();
        let col_defs = engine.read_column_defs(tid).unwrap();
        assert_eq!(col_defs.len(), 2);
        assert_eq!(
            &*col_defs[1].def.name, long_name,
            "Long column name corrupted after restart"
        );
        engine.close();
    }
}

// ── test_hook_relation_register_rejects_malformed_pk ─────────────────

// Drives crafted/malformed packed PK values through the production
// bundle path (`apply_bundle` → `fire_hooks` →
// `hook_relation_register`) and asserts each is rejected with an `Err`
// rather than panicking the server via a `SchemaDescriptor::new`
// `assert!`.
#[test]
fn test_hook_relation_register_rejects_malformed_pk() {
    let (_dir, mut engine) = empty_catalog(1);

    // Columns: [c0 U64 non-null, c1 STRING non-null, c2 U64 nullable, c3 F32 non-null].
    let col_defs = vec![
        col_def("c0", TypeCode::U64),
        col_def("c1", TypeCode::String),
        nullable_def("c2", TypeCode::U64),
        col_def("c3", TypeCode::F32),
    ];
    let tid = engine.allocate_ids(1).unwrap();

    let mut assert_rejects = |raw_pk_cols: u64, snippet: &str| {
        let blocks = [
            (SysFamily::Column, col_tab_batch(tid, &col_defs, 1)),
            (
                SysFamily::Table,
                table_tab_row_words(tid, "bad_table", raw_pk_cols, gnitz_wire::TableProps::default()),
            ),
        ];
        let res = apply_ddl(&mut engine, blocks);
        let err = res.expect_err(&format!("expected Err containing '{snippet}', got Ok"));
        assert!(err.contains(snippet), "expected '{snippet}', got: {err}");
    };

    // Count out of range, rejected at the decode.
    assert_rejects(0, "at least one column"); // count 0
    assert_rejects(5, "out of range 1..=4"); // count 5
    assert_rejects(15, "out of range 1..=4"); // count 15

    // Out-of-bounds index (index 5, only 3 columns).
    assert_rejects(1 | (5 << 4), "out of bounds");
    // Duplicate index [0, 0]. The `(0 << N)` forms document the slot layout
    // (count=2, idx0@bit4=0, idx1@bit11=0) even though they evaluate to 0.
    #[allow(clippy::identity_op)]
    let packed = 2 | (0u64 << 4) | (0u64 << 11);
    assert_rejects(packed, "names column 0 twice");
    // Non-integer PK column (c1 is STRING).
    assert_rejects(1 | (1 << 4), "only fixed-width integer");
    // Float PK column (c3 is F32) — floats break the byte-equal PK contract.
    assert_rejects(1 | (3 << 4), "only fixed-width integer");
    // Nullable PK column (c2 is nullable).
    assert_rejects(1 | (2 << 4), "must not be nullable");

    engine.close();
}

// ── test_pk_list_round_trips_into_registered_schema ──────────────────

/// `TABLE_TAB.pk_col_idx` is reconstructed element-for-element into the
/// registered schema's PK list, for a single-column and for compound PKs in any
/// column order, at create and again after a reopen.
#[test]
fn test_pk_list_round_trips_into_registered_schema() {
    let (_dir, mut engine) = empty_catalog(1);
    let pk_of = |engine: &CatalogEngine, tid: u64| {
        engine
            .registry
            .relation(tid)
            .map(Relation::schema)
            .unwrap()
            .pk_cols()
            .to_vec()
    };

    let cols = vec![col_def("a", TypeCode::U64), col_def("b", TypeCode::U64)];
    let mut live: Vec<(String, Vec<u32>)> = Vec::new();

    // Single-column PK, and a recreate under a different PK column: the
    // reconstructed schema reflects the new list, not the old one.
    let tid = engine.create_table("public.t", &cols, &[0]).unwrap();
    assert_eq!(pk_of(&engine, tid), [0]);
    engine.drop_table("public.t").unwrap();
    let tid2 = engine.create_table("public.t", &cols, &[1]).unwrap();
    assert_eq!(pk_of(&engine, tid2), [1]);
    live.push(("t".to_string(), vec![1]));

    // Compound PK lists round-trip in full, including one out of column order.
    let wide: Vec<_> = (0..8).map(|i| col_def(&format!("c{i}"), TypeCode::U64)).collect();
    for (n, pk_cols) in [vec![0u32, 1], vec![0u32, 3, 5], vec![1u32, 2, 7], vec![2u32, 1]]
        .into_iter()
        .enumerate()
    {
        let name = format!("w{n}");
        let tid = engine.create_table(&format!("public.{name}"), &wide, &pk_cols).unwrap();
        assert_eq!(
            pk_of(&engine, tid),
            pk_cols,
            "compound PK list must round-trip in full (input={pk_cols:?})"
        );
        live.push((name, pk_cols));
    }

    let dir = engine.registry.base_dir().to_owned();
    engine.close();
    let engine = CatalogEngine::open(&dir, 1).unwrap();
    for (name, pk_cols) in &live {
        let tid = engine.get_by_name("public", name).unwrap();
        assert_eq!(&pk_of(&engine, tid), pk_cols, "PK list of {name} must survive a reopen");
    }

    engine.close();
}

// ── test_drop_view_removes_directory ─────────────────────────────────
// DROP VIEW must delete the view's on-disk scratch directory, not just the
// logical catalog rows. Once the catalog entry is gone no later DROP could
// target the leaked directory, so the cleanup has to happen inside drop_view.

#[test]
fn test_drop_view_removes_directory() {
    let (_dir, mut engine) = empty_catalog(1);

    // A view needs a base table to reference.
    let base_cols = vec![col_def("id", TypeCode::U64)];
    let base = engine.create_table("public.base", &base_cols, &[0]).unwrap();

    let vid = register_identity_view(&mut engine, base, "myview", &base_cols);

    // The register hook created the physical view directory on disk.
    let view_dir = relation_dir(engine.registry.base_dir(), vid);
    assert!(
        std::path::Path::new(&view_dir).exists(),
        "view dir should exist after create: {view_dir}"
    );

    // Drop removes catalog metadata and leaves the physical directory to the
    // orphan sweep, which production runs once every worker has applied the
    // drop; drive both steps directly here.
    engine.drop_view("public.myview").unwrap();
    engine.registry.reclaim_orphan_relation_dirs().unwrap();
    assert!(
        !std::path::Path::new(&view_dir).exists(),
        "view dir must be deleted by the sweep after the drop: {view_dir}"
    );

    engine.close();
}

// ── drop_table_retracts_its_serial_sequence_row ──────────────────────
#[test]
fn drop_table_retracts_its_serial_sequence_row() {
    let (_dir, mut engine) = empty_catalog(1);

    let tid = engine.create_serial_table("public.t").unwrap();
    reserve(&mut engine, tid, 64).unwrap();
    assert_eq!(engine.sequence_value(tid), Some(64));

    engine.submit_retraction(SysFamily::Table, tid).unwrap();
    assert_eq!(engine.sequence_value(tid), None, "the drop must retract the SERIAL row");

    engine.close();
}

// ── dropped_name_resolves_to_its_recreation ──────────────────────────
#[test]
fn dropped_name_resolves_to_its_recreation() {
    let (_dir, mut engine) = empty_catalog(1);

    let cols = u64_i64_cols();

    // Create, then drop, a table named `public.t`.
    let tid1 = engine.create_table("public.t", &cols, &[0]).unwrap();
    assert_eq!(engine.get_by_name("public", "t"), Some(tid1));
    engine.drop_table("public.t").unwrap();

    // The drop must have cleared the qname mapping.
    assert!(engine.get_by_name("public", "t").is_none(), "drop must clear the name");

    // Recreate under the same qualified name. A leaked stale mapping would
    // make the qname-uniqueness guard reject this create.
    let recreate = engine.create_table("public.t", &cols, &[0]);
    assert!(
        recreate.is_ok(),
        "recreate of a dropped same-name table must succeed; a stale qname mapping blocks it: {recreate:?}"
    );
    let tid2 = recreate.unwrap();
    assert_ne!(tid1, tid2, "recreated table must get a fresh tid");

    // The name must resolve to the NEW tid.
    assert_eq!(
        engine.get_by_name("public", "t"),
        Some(tid2),
        "the name must resolve to the NEW tid after drop + recreate"
    );

    engine.close();
}

// ── replicated_bit_is_transitive_and_survives_replay ─────────────────
// A view is replicated iff every source it scans is, so the property climbs a
// view chain: base → producer → consumer. The live batch carries the consumer's
// row before the producer's; the register hook registers in id order, which
// ascends along scan edges. Replay must reproduce the same placements.
#[test]
fn replicated_bit_is_transitive_and_survives_replay() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().to_str().unwrap();
    let mut engine = CatalogEngine::open(dir, 1).unwrap();

    let cols = u64_i64_cols();

    // A REPLICATED base table.
    let rt = engine
        .create_table_with(
            "public.rt",
            &cols,
            &[0],
            gnitz_wire::TableProps {
                distribution: gnitz_wire::TableDistribution::Replicated,
                ..Default::default()
            },
        )
        .unwrap();

    // A partitioned base table, for the negative direction.
    let pt = engine.create_table("public.pt", &cols, &[0]).unwrap();

    // Ids ascend along scan edges, as view registration requires.
    let r_producer = engine.allocate_ids(1).unwrap();
    let r_consumer = engine.allocate_ids(1).unwrap();
    let p_producer = engine.allocate_ids(1).unwrap();
    let p_consumer = engine.allocate_ids(1).unwrap();

    let views = [
        (r_producer, rt),
        (r_consumer, r_producer),
        (p_producer, pt),
        (p_consumer, p_producer),
    ]
    .map(|(vid, src)| {
        (
            vid,
            crate::test_support::identity_circuit(src, gnitz_wire::ReadBound::None),
        )
    });

    // One VIEW_TAB batch, consumers first — the dependency-reversed row order.
    let rows = sys_batch(
        [
            (r_consumer, "rv2"),
            (p_consumer, "pv2"),
            (r_producer, "rv"),
            (p_producer, "pv"),
        ]
        .map(|(vid, name)| (view_tab_row(vid, name), 1)),
    );
    let created = views.iter().map(|(vid, circuit)| (*vid, circuit, &cols[..]));
    apply_ddl(&mut engine, view_blocks(created, rows)).unwrap();

    let stamp = |e: &mut CatalogEngine, id: u64| {
        e.registry
            .relation_or_err(id)
            .expect("registered")
            .placement()
            .is_replicated()
    };
    let assert_stamps = |e: &mut CatalogEngine, when: &str| {
        assert!(stamp(e, rt), "replicated base table ({when})");
        assert!(stamp(e, r_producer), "view over a replicated table ({when})");
        assert!(
            stamp(e, r_consumer),
            "view over a replicated VIEW — the bit must climb ({when})"
        );
        assert!(!stamp(e, p_producer), "view over a table ({when})");
        assert!(!stamp(e, p_consumer), "view over a view ({when})");
    };
    assert_stamps(&mut engine, "live CREATE");

    engine.close();

    let mut engine2 = CatalogEngine::open(dir, 1).unwrap();
    assert_stamps(&mut engine2, "after replay");
    engine2.close();
}

// ---------------------------------------------------------------------------
// Delta feeds (`WITH (delta = …)`)
// ---------------------------------------------------------------------------

/// A registration naming both `capacity` and `delta` is refused by the engine,
/// below the SQL layer (`ViewBudget::from_row`).
#[test]
fn a_view_declaring_both_capacity_and_delta_is_rejected() {
    let (_dir, mut engine) = empty_catalog(1);
    let cols = u64_i64_cols();
    let tid = engine.create_table("public.t", &cols, &[0]).unwrap();

    let err = try_register_identity_view(&mut engine, tid, "both", &cols, 4 << 20, 4 << 20)
        .expect_err("the pair must be refused");
    assert!(err.contains("capacity") && err.contains("delta"), "got: {err}");

    // Either alone is accepted, so the rejection is about the pair.
    try_register_identity_view(&mut engine, tid, "bounded", &cols, 4 << 20, 0).expect("capacity alone");
    try_register_identity_view(&mut engine, tid, "fed", &cols, 0, 4 << 20).expect("delta alone");
}

/// A bounded view a view may not read is named by its qualified name, not its id:
/// the engine's message is the one every client that bypasses the SQL planner sees.
#[test]
fn a_bounded_view_source_is_named() {
    let (_dir, mut engine) = empty_catalog(1);
    let cols = u64_i64_cols();
    let tid = engine.create_table("public.t", &cols, &[0]).unwrap();
    let bv = try_register_identity_view(&mut engine, tid, "bv", &cols, 4 << 20, 0).expect("a bounded view");

    let err = try_register_identity_view(&mut engine, bv, "over_bv", &cols, 0, 0).expect_err("a leaf view");
    assert!(
        err.contains("'public.bv', which is a capacity-bounded view"),
        "got: {err}"
    );
}

/// A feed keeps a view's deltas in the view's own schema, so a view at
/// `MAX_COLUMNS` carries one as any other does.
#[test]
fn a_view_at_the_column_limit_carries_a_feed() {
    let (_dir, mut engine) = empty_catalog(1);
    let cols: Vec<CatalogColumn> = (0..gnitz_wire::MAX_COLUMNS)
        .map(|i| crate::test_support::col_def(&format!("c{i}"), gnitz_wire::TypeCode::U64))
        .collect();
    let tid = engine.create_table("public.wide", &cols, &[0]).unwrap();

    try_register_identity_view(&mut engine, tid, "fed_wide", &cols, 0, 4 << 20).expect("fed wide view");
}

// ── duplicate visible column names ───────────────────────────────────

// Driven through the wire-ingest path, since the SQL planner rejects the shape
// before it gets here. A stream carries the rule because it is an ingestion
// point; a hidden column does not, being invisible to name resolution.
#[test]
fn duplicate_visible_column_names_are_rejected_for_a_table_and_a_stream() {
    let (_dir, mut engine) = empty_catalog(1);

    let stream = gnitz_wire::TableProps { stream: true, ..Default::default() };
    for (name, props) in [("dup_table", gnitz_wire::TableProps::default()), ("dup_stream", stream)] {
        let col_defs = vec![
            col_def("id", TypeCode::U64),
            col_def("a", TypeCode::I64),
            // The catalog folds names, so a case difference is the same name.
            col_def("A", TypeCode::I64),
        ];
        let tid = engine.allocate_ids(1).unwrap();
        let blocks = [
            (SysFamily::Column, col_tab_batch(tid, &col_defs, 1)),
            (
                SysFamily::Table,
                table_tab_row_words(tid, name, PkColList::from_slice(&[0]).pack(), props),
            ),
        ];
        let err = apply_ddl(&mut engine, blocks).expect_err("a duplicate visible column name must be refused");
        assert!(err.contains("duplicate column name"), "{name}: {err}");
    }

    // A hidden column repeating a visible name still registers.
    let col_defs = vec![
        col_def("id", TypeCode::U64),
        col_def("a", TypeCode::I64),
        CatalogColumn {
            def: ColumnDef::new("a", TypeCode::I64, false).hidden(),
            fk: None,
        },
    ];
    engine.create_table("public.hidden_dup", &col_defs, &[0]).unwrap();

    engine.close();
}

// ── One TABLE_TAB batch drops several tables, one batch per family ───────────

/// Workers apply the zone in this order, which no end-state assertion can see.
#[test]
fn set_based_table_drop_applies_one_group_per_family() {
    let (_dir, mut engine) = empty_catalog(1);
    let cols = vec![col_def("id", TypeCode::U64), col_def("val", TypeCode::I64)];
    let mut tids = Vec::new();
    for name in ["a", "b", "c"] {
        tids.push(engine.create_table(&format!("public.{name}"), &cols, &[0]).unwrap());
        engine.create_index(&format!("public.{name}"), &["val"], false).unwrap();
    }

    let drop = engine.retract_under(SysFamily::Table, &tids);
    let mut families: Vec<SysFamily> = Vec::new();
    let mut sink = |f: SysFamily, _: &Batch| {
        families.push(f);
        Ok(())
    };
    engine
        .apply_bundle(bundle([(SysFamily::Table, drop)]), &mut sink)
        .unwrap();
    assert_eq!(families, [SysFamily::Index, SysFamily::Table, SysFamily::Column]);
    assert!(tids.iter().all(|&t| engine.registry.relation(t).is_none()));

    engine.close();
}

// ── A view drop retracts the internal segments it owns ───────────────────────

/// Register view `V` over a fresh base table together with segment `S`, whose
/// VIEW_TAB row names `V` as its owner. Returns `(V, S)`.
fn view_with_segment(engine: &mut CatalogEngine) -> (u64, u64) {
    let base = engine
        .create_table("public.base", &[col_def("id", TypeCode::U64)], &[0])
        .unwrap();
    let cols = [col_def("id", TypeCode::U64)];
    let v = engine.allocate_ids(2).unwrap();
    let s = v + 1;
    let identity = crate::test_support::identity_circuit(base, gnitz_wire::ReadBound::None);
    let rows = sys_batch([(v, "v", 0), (s, "_seg", v)].map(|(vid, name, owner)| {
        let row = ViewTabRow {
            owner_view_id: owner,
            ..view_tab_row(vid, name)
        };
        (row, 1)
    }));
    let created = [v, s].map(|vid| (vid, &identity, &cols[..]));
    apply_ddl(engine, view_blocks(created, rows)).unwrap();
    (v, s)
}

/// What an applied bundle reports it did is read off the groups it applied, so
/// it carries the drop cascade: a view's segments, a table's indexes.
#[test]
fn applied_ddl_reports_the_cascade_and_what_it_created() {
    let (_dir, mut engine) = empty_catalog(1);
    let no_log = &mut |_: SysFamily, _: &Batch| Ok(());

    // A view over `views`: a CREATE VIEW writes a family a view scans.
    let watcher = register_identity_view(&mut engine, SysFamily::View.id(), "w", &SysFamily::View.column_defs());
    let base = engine
        .create_table("public.src", &[col_def("id", TypeCode::U64)], &[0])
        .unwrap();
    let cols = [col_def("id", TypeCode::U64)];
    let vid = engine.allocate_ids(1).unwrap();
    let rows = sys_batch([(view_tab_row(vid, "created"), 1)]);
    let identity = crate::test_support::identity_circuit(base, gnitz_wire::ReadBound::None);
    let created = engine
        .apply_bundle(bundle(view_blocks([(vid, &identity, &cols[..])], rows)), no_log)
        .unwrap();
    assert_eq!(created.created_views, [vid]);
    assert_eq!(created.held_above, [SysFamily::View.id()]);
    assert!(created.dropped_relations.is_empty() && created.dropped_indexes.is_empty());
    engine.drop_view("public.w").unwrap();
    assert!(engine.registry.relation(watcher).is_none());

    let (v, s) = view_with_segment(&mut engine);
    let drop = engine.retract_under(SysFamily::View, &[v]);
    let dropped = engine.apply_bundle(bundle([(SysFamily::View, drop)]), no_log).unwrap();
    let mut relations = dropped.dropped_relations.clone();
    relations.sort_unstable();
    assert_eq!(relations, [v, s], "the segment drops with its view");

    let tid = engine
        .create_table(
            "public.t",
            &[col_def("id", TypeCode::U64), col_def("val", TypeCode::I64)],
            &[0],
        )
        .unwrap();
    engine.create_index("public.t", &["val"], false).unwrap();
    let drop = engine.retract_under(SysFamily::Table, &[tid]);
    let dropped = engine.apply_bundle(bundle([(SysFamily::Table, drop)]), no_log).unwrap();
    assert_eq!(dropped.dropped_relations, [tid]);
    let indexes: Vec<_> = dropped
        .dropped_indexes
        .iter()
        .map(|(owner, cols)| (*owner, cols.as_slice().to_vec()))
        .collect();
    assert_eq!(indexes, [(tid, vec![1])], "the index drops with its table");

    engine.close();
}

#[test]
fn view_drop_retracts_its_segments() {
    let (_dir, mut engine) = empty_catalog(1);
    let (v, s) = view_with_segment(&mut engine);

    let drop = engine.retract_under(SysFamily::View, &[v]);
    let mut families: Vec<SysFamily> = Vec::new();
    let mut sink = |f: SysFamily, _: &Batch| {
        families.push(f);
        Ok(())
    };
    engine
        .apply_bundle(bundle([(SysFamily::View, drop)]), &mut sink)
        .unwrap();
    assert_eq!(families, [SysFamily::View, SysFamily::Circuit, SysFamily::Column]);
    for id in [v, s] {
        assert!(engine.registry.relation(id).is_none());
        assert_eq!(rows_under(&engine, SysFamily::Circuit, id), 0);
        assert_eq!(rows_under(&engine, SysFamily::Column, id), 0);
    }

    engine.close();
}

#[test]
fn dropping_a_view_with_its_segment_retracts_the_segment_once() {
    let (_dir, mut engine) = empty_catalog(1);
    let (v, s) = view_with_segment(&mut engine);

    let drop = engine.retract_under(SysFamily::View, &[v, s]);
    apply_ddl(&mut engine, [(SysFamily::View, drop)]).unwrap();

    assert!(engine.registry.relation(v).is_none() && engine.registry.relation(s).is_none());
    assert_eq!(
        count_negative_records(engine.sys_relation(SysFamily::View).cursor()),
        0,
        "the segment must not be retracted twice"
    );

    engine.close();
}

// ── One relation id names one relation ───────────────────────────────
#[test]
fn a_view_create_at_a_registered_table_id_is_refused() {
    let (_dir, mut engine) = empty_catalog(1);
    let cols = vec![col_def("id", TypeCode::U64)];

    // In its own bundle, against a committed table.
    let tid = engine.create_table("public.t", &cols, &[0]).unwrap();
    let err = apply_ddl(
        &mut engine,
        [(SysFamily::View, sys_batch([(view_tab_row(tid, "v"), 1)]))],
    )
    .expect_err("a view create at a table's id must be refused");
    assert!(err.contains(&format!("{tid}")) && err.contains("single-use"), "{err}");
    assert!(engine.registry.relation(tid).is_some_and(|r| r.kind().is_base_table()));

    // In the same bundle as the TABLE_TAB create.
    let both = engine.allocate_ids(1).unwrap();
    let blocks = [
        (SysFamily::Column, col_tab_batch(both, &cols, 1)),
        (SysFamily::Table, table_tab_batch(&[(both, "both", 1)])),
        (SysFamily::View, sys_batch([(view_tab_row(both, "both_v"), 1)])),
    ];
    let err = apply_ddl(&mut engine, blocks).expect_err("one bundle may not create one id as a table and a view");
    assert!(err.contains(&format!("{both}")) && err.contains("single-use"), "{err}");
    assert!(engine.registry.relation(both).is_none());
    assert_eq!(
        engine.qualified_name_or_unknown(both),
        ("?".to_string(), "?".to_string())
    );

    engine.close();
}

#[test]
fn only_a_scanned_table_holds_a_push_for_its_tick() {
    use crate::test_support::make_batch;
    let cols = u64_i64_cols();
    let (_dir, mut engine, tid) = table_engine(1, &cols);
    let schema = engine.registry.relation(tid).map(Relation::schema).unwrap();

    engine
        .ingest_unticked(tid, make_batch(&schema, &[(1, 1, 10)]), PkRule::Enforce)
        .unwrap();
    assert!(engine.registry.seal(tid).unwrap().is_none());
    assert_eq!(scan_all(&mut engine, tid).len(), 1);

    try_register_identity_view(&mut engine, tid, "v", &cols, 0, 0).unwrap();
    engine
        .ingest_unticked(tid, make_batch(&schema, &[(2, 1, 20)]), PkRule::Enforce)
        .unwrap();
    assert_eq!(engine.registry.seal(tid).unwrap().map(|b| b.len()), Some(1));

    engine.close();
}
