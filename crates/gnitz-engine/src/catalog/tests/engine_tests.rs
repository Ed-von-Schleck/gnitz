use super::*;

// ── test_enforce_unique_pk ───────────────────────────────────────────

/// Unique-PK enforcement through the production partitioned path: insert,
/// intra-batch dedup, insert+delete cancellation, and the `+1, -1, +1`
/// re-insert regression (the deleted Single-store variant netted this to 0 by
/// failing to clear its intra-batch `seen` map on the delete).
#[test]
fn test_enforce_unique_pk() {
    let dir = temp_dir("enforce_upk");
    let mut engine = CatalogEngine::open(&dir).unwrap();
    let cols = vec![u64_col_def("id"), u64_col_def("val")];
    let tid = engine.create_table("public.t", &cols, &[0], true).unwrap(); // unique_pk ⟹ Partitioned
    let schema = engine.get_schema(tid).unwrap();

    let make_row = |pk: u64, val: u64, w: i64| -> Batch {
        let mut bb = BatchBuilder::new(schema);
        bb.begin_row(pk as u128, w);
        bb.put_u64(val);
        bb.end_row();
        bb.finish()
    };
    let live = |engine: &mut CatalogEngine| -> usize {
        engine.flush_family(tid).unwrap();
        engine.scan_family(tid).unwrap().count
    };

    // ST1: insert a new PK.
    engine.ingest_to_family(tid, &make_row(1, 10, 1)).unwrap();
    assert_eq!(live(&mut engine), 1);

    // ST2: insert a different PK.
    engine.ingest_to_family(tid, &make_row(2, 20, 1)).unwrap();
    assert_eq!(live(&mut engine), 2);

    // ST3: intra-batch duplicate — last value wins (one net row for PK=5).
    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(5u128, 1);
    bb.put_u64(10);
    bb.end_row();
    bb.begin_row(5u128, 1);
    bb.put_u64(20);
    bb.end_row();
    engine.ingest_to_family(tid, &bb.finish()).unwrap();
    assert_eq!(live(&mut engine), 3);

    // ST4: intra-batch insert then delete cancel (PK=6 not added).
    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(6u128, 1);
    bb.put_u64(10);
    bb.end_row();
    bb.begin_row(6u128, -1);
    bb.put_u64(10);
    bb.end_row();
    engine.ingest_to_family(tid, &bb.finish()).unwrap();
    assert_eq!(live(&mut engine), 3);

    // ST5: regression — +1, -1, +1 on a fresh PK must net to +1, not 0.
    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(7u128, 1);
    bb.put_u64(100);
    bb.end_row();
    bb.begin_row(7u128, -1);
    bb.put_u64(100);
    bb.end_row();
    bb.begin_row(7u128, 1);
    bb.put_u64(200);
    bb.end_row();
    engine.ingest_to_family(tid, &bb.finish()).unwrap();
    assert_eq!(live(&mut engine), 4, "+1,-1,+1 must leave PK=7 live");

    engine.close();
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
        bb.begin_row(888u128, 1);
        bb.put_u64(99999); // owner_id (non-existent)
        bb.put_u64(OWNER_KIND_TABLE as u64);
        bb.put_u64(1); // source_col_idx
        bb.put_string("orphaned_idx");
        bb.put_u64(0); // is_unique
        bb.put_string(""); // cache_dir
        bb.end_row();
        engine.sys_indices.ingest_borrowed_batch(&bb.finish()).unwrap();
        let _ = engine.sys_indices.flush();
        engine.close();
        drop(engine);
    }

    // Re-open should fail because the orphaned index references table 99999
    let result = CatalogEngine::open(&dir);
    assert!(result.is_err(), "Orphaned index metadata should cause error on reload");

    let _ = fs::remove_dir_all(&dir);
}

// ── user-table SERIAL sequences ──────────────────────────────────────

/// `reserve_user_sequence` seeds an absent sequence at base 1, hands out
/// contiguous ranges, and advances the in-memory high-water each call.
#[test]
fn test_reserve_user_sequence_seed_and_contiguous() {
    let dir = temp_dir("reserve_user_seq");
    let mut engine = CatalogEngine::open(&dir).unwrap();
    let seq_id = FIRST_USER_TABLE_ID;

    // Seed-on-first-use: absent sequence ⇒ base 1, high-water 64.
    let (base1, delta1, _) = engine.reserve_user_sequence(seq_id, 64);
    assert_eq!(base1, 1);
    assert_eq!(engine.user_sequences.get(&seq_id).copied(), Some(64));
    // The delta is a retract(old)+insert(new) pair.
    assert_eq!(delta1.count, 2);

    // The next range is contiguous: base 65, high-water 128.
    let (base2, _delta2, _) = engine.reserve_user_sequence(seq_id, 64);
    assert_eq!(base2, 65);
    assert_eq!(engine.user_sequences.get(&seq_id).copied(), Some(128));

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

/// `saturating_add` keeps a reservation at the i64 ceiling from panicking
/// (unreachable in practice — the client overflow guard rejects far sooner).
#[test]
fn test_reserve_user_sequence_saturates() {
    let dir = temp_dir("reserve_user_seq_sat");
    let mut engine = CatalogEngine::open(&dir).unwrap();
    let seq_id = FIRST_USER_TABLE_ID;
    engine.user_sequences.insert(seq_id, i64::MAX - 1);

    let (base, _delta, _) = engine.reserve_user_sequence(seq_id, 64);
    assert_eq!(base, i64::MAX); // hw + 1
    assert_eq!(engine.user_sequences.get(&seq_id).copied(), Some(i64::MAX));

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

/// End-to-end durable round-trip: reserve → ingest the delta through the real
/// family path (firing `hook_sequence_register`) → flush → reopen. Recovery must
/// restore the high-water (the phantom retract of `(seq_id, 0)` on first use is
/// weight-filtered), and the next reservation continues at `high_water + 1`.
#[test]
fn test_user_sequence_durable_roundtrip() {
    let dir = temp_dir("user_seq_roundtrip");
    let user_seq = FIRST_USER_TABLE_ID + 3;
    {
        let mut engine = CatalogEngine::open(&dir).unwrap();
        let (base, delta, _) = engine.reserve_user_sequence(user_seq, 64);
        assert_eq!(base, 1);
        // Clear the synchronously-set map entry to prove the hook re-populates it.
        engine.user_sequences.remove(&user_seq);
        engine.ingest_to_family(SEQ_TAB_ID, &delta).unwrap();
        assert_eq!(engine.user_sequences.get(&user_seq).copied(), Some(64));
        let _ = engine.sys_sequences.flush();
        engine.close();
    }
    let mut engine = CatalogEngine::open(&dir).unwrap();
    assert_eq!(engine.user_sequences.get(&user_seq).copied(), Some(64));
    let (base2, _delta, _) = engine.reserve_user_sequence(user_seq, 64);
    assert_eq!(base2, 65, "next id continues after the recovered high-water");
    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

/// The `>= FIRST_USER_TABLE_ID` recovery guard must ignore a stray sequence id
/// in the empty 4..16 gap, never misclassifying it as a user sequence.
#[test]
fn test_recover_ignores_sub_user_seq_id() {
    let dir = temp_dir("recover_gap_guard");
    let stray = 7i64; // below FIRST_USER_TABLE_ID
    {
        let mut engine = CatalogEngine::open(&dir).unwrap();
        let schema = seq_tab_schema();
        let mut bb = BatchBuilder::new(schema);
        bb.begin_row(stray as u128, 1);
        bb.put_u64(999);
        bb.end_row();
        engine.sys_sequences.ingest_borrowed_batch(&bb.finish()).unwrap();
        let _ = engine.sys_sequences.flush();
        engine.close();
    }
    let mut engine = CatalogEngine::open(&dir).unwrap();
    assert!(!engine.user_sequences.contains_key(&stray));
    engine.close();
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
        engine.create_table("public.t1", &cols, &[0], true).unwrap();

        // Inject table record for tid=250 directly into sys_tables
        let tbl_schema = table_tab_schema();
        let mut bb = BatchBuilder::new(tbl_schema);
        bb.begin_row(250u128, 1);
        bb.put_u64(PUBLIC_SCHEMA_ID as u64); // schema_id
        bb.put_string("gap_table");
        bb.put_string(&format!("{dir}/public/gap"));
        bb.put_u64(0); // pk_col_idx
        bb.put_u64(0); // created_lsn
        bb.put_u64(0); // flags
        bb.end_row();
        engine.sys_tables.ingest_borrowed_batch(&bb.finish()).unwrap();

        // Inject column record for tid=250
        let col_schema = col_tab_schema();
        let mut cbb = BatchBuilder::new(col_schema);
        let pk = pack_column_id(250, 0);
        cbb.begin_row(pk as u128, 1);
        cbb.put_u64(250); // owner_id
        cbb.put_u64(OWNER_KIND_TABLE as u64);
        cbb.put_u64(0); // col_idx
        cbb.put_string("id");
        cbb.put_u64(type_code::U64 as u64);
        cbb.put_u64(0); // is_nullable
        cbb.put_u64(0); // fk_table_id
        cbb.put_u64(0); // fk_col_idx
        cbb.put_u64(0); // is_serial
        cbb.end_row();
        engine.sys_columns.ingest_borrowed_batch(&cbb.finish()).unwrap();

        let _ = engine.sys_tables.flush();
        let _ = engine.sys_columns.flush();
        engine.close();
        drop(engine);
    }

    // Re-open: sequence should recover to 251
    {
        let mut engine = CatalogEngine::open(&dir).unwrap();
        let new_tid = engine.create_table("public.tnext", &cols, &[0], true).unwrap();
        assert_eq!(new_tid, 251, "Sequence recovery: expected 251, got {new_tid}");
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
    let tid = engine.create_table("public.t", &cols, &[0], false).unwrap();
    let schema = engine.get_schema(tid).unwrap();

    // Ingest via CatalogEngine (user table path)
    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(1u128, 1);
    bb.put_u64(100);
    bb.end_row();
    bb.begin_row(2u128, 1);
    bb.put_u64(200);
    bb.end_row();
    bb.begin_row(3u128, 1);
    bb.put_u64(300);
    bb.end_row();
    engine.ingest_to_family(tid, &bb.finish()).unwrap();
    engine.flush_family(tid).unwrap();

    // Scan
    let scan_batch = engine.scan_family(tid).unwrap();
    assert_eq!(scan_batch.count, 3);

    // Seek existing
    let found = engine.seek_family(tid, 2u128, &[]).unwrap();
    assert!(found.is_some());
    let row = found.unwrap();
    assert_eq!(row.count, 1);
    assert_eq!(row.get_pk(0), 2);

    // Seek missing
    let not_found = engine.seek_family(tid, 99u128, &[]).unwrap();
    assert!(not_found.is_none());

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── seek_family resolves a system-table row (post-collapse) ───────────

/// After the FLAG_SEEK collapse there is no system-table fast path: a
/// `table_id < FIRST_USER_TABLE_ID` seek flows through the same
/// `seek_family → seek_opk_bytes → seek_family_bytes` chain as user tables,
/// resolving its schema via `sys_tab_schema`. `create_table` writes a TABLE_TAB
/// row keyed by the new table-id — a single narrow U64 PK, stride 8 — so seeking
/// TABLE_TAB by that id drives the empty-`extra` narrow path end to end through
/// the system-table schema resolver.
#[test]
fn seek_family_resolves_system_table_row() {
    let dir = temp_dir("catalog_seek_system_table");
    let mut engine = CatalogEngine::open(&dir).unwrap();
    let cols = vec![u64_col_def("id"), u64_col_def("val")];
    let tid = engine.create_table("public.t", &cols, &[0], true).unwrap();

    // System-table point seek (narrow stride 8, empty extra).
    let found = engine.seek_family(TABLE_TAB_ID, tid as u128, &[]).unwrap();
    assert!(
        found.is_some(),
        "seek_family must resolve the TABLE_TAB row for the created table",
    );
    assert_eq!(found.unwrap().count, 1);

    // A missing system-table key returns None — not an error, not a panic.
    let missing = engine.seek_family(TABLE_TAB_ID, 9_999_999u128, &[]).unwrap();
    assert!(missing.is_none(), "absent system-table key seeks to None");

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── test_ingest_unique_pk_partitioned ─────────────────────────────────

#[test]
fn test_ingest_unique_pk_partitioned() {
    let dir = temp_dir("catalog_unique_pk_partitioned");
    let mut engine = CatalogEngine::open(&dir).unwrap();
    let cols = vec![u64_col_def("id"), u64_col_def("val")];
    let tid = engine.create_table("public.t", &cols, &[0], true).unwrap();
    let schema = engine.get_schema(tid).unwrap();

    // Insert row with PK=1, val=100
    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(1u128, 1);
    bb.put_u64(100);
    bb.end_row();
    engine.ingest_to_family(tid, &bb.finish()).unwrap();
    engine.flush_family(tid).unwrap();

    // Insert row with PK=1 again, val=200 (should retract old + insert new)
    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(1u128, 1);
    bb.put_u64(200);
    bb.end_row();
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
    bb.begin_row(100u128, 1); // sid=100
    bb.put_string("synced");
    bb.end_row();
    engine.ddl_sync(SCHEMA_TAB_ID, bb.finish()).unwrap();

    // Hooks should have registered the schema
    assert!(engine.has_schema("synced"));

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── test_ddl_sync_zone_lsn_tracking ──────────────────────────────────

#[test]
fn test_ddl_sync_zone_lsn_tracking() {
    // Invariant: while a DDL zone is open, `ingest_to_family` pins each
    // touched system table's `current_lsn` to that zone LSN. After successive
    // DDL zones, `get_max_flushed_lsn` reports the most recent zone LSN,
    // and recovery can skip already-applied groups.
    use crate::catalog::sys_tables::{COL_TAB_ID, SCHEMA_TAB_ID, TABLE_TAB_ID};
    use std::num::NonZeroU64;

    let zone = |lsn: u64| NonZeroU64::new(lsn).unwrap();
    let dir = temp_dir("catalog_zone_lsn");
    let mut engine = CatalogEngine::open(&dir).unwrap();

    // Zone 5: create a schema. SCHEMA_TAB pinned to lsn=5.
    engine.ctx.open_ddl_zone(zone(5));
    engine.create_schema("z").unwrap();
    assert_eq!(engine.get_max_flushed_lsn(SCHEMA_TAB_ID), 5);

    // Zone 7: create a table. TABLE_TAB and COL_TAB pinned to lsn=7;
    // SCHEMA_TAB stays at 5 (untouched in this zone).
    engine.ctx.open_ddl_zone(zone(7));
    let cols = vec![u64_col_def("id"), u64_col_def("val")];
    let _tid = engine.create_table("z.t", &cols, &[0], false).unwrap();
    assert_eq!(engine.get_max_flushed_lsn(TABLE_TAB_ID), 7);
    assert_eq!(engine.get_max_flushed_lsn(COL_TAB_ID), 7);
    assert_eq!(
        engine.get_max_flushed_lsn(SCHEMA_TAB_ID),
        5,
        "SCHEMA_TAB stays at the most recent zone that touched it"
    );

    // Zone 9: another table. TABLE_TAB and COL_TAB advance to lsn=9.
    engine.ctx.open_ddl_zone(zone(9));
    let _tid2 = engine.create_table("z.t2", &cols, &[0], false).unwrap();
    assert_eq!(engine.get_max_flushed_lsn(TABLE_TAB_ID), 9);
    assert_eq!(engine.get_max_flushed_lsn(COL_TAB_ID), 9);

    // collect_all_flushed_lsns covers every system table.
    let map = engine.collect_all_flushed_lsns();
    assert_eq!(map.get(&SCHEMA_TAB_ID), Some(&5));
    assert_eq!(map.get(&TABLE_TAB_ID), Some(&9));
    assert_eq!(map.get(&COL_TAB_ID), Some(&9));

    // max_table_current_lsn is at least the highest zone LSN observed.
    assert!(engine.max_table_current_lsn() >= 9);

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── test_raw_store_ingest ─────────────────────────────────────────────

#[test]
fn test_raw_store_ingest() {
    let dir = temp_dir("catalog_raw_store_ingest");
    let mut engine = CatalogEngine::open(&dir).unwrap();
    let cols = vec![u64_col_def("id"), u64_col_def("val")];
    let tid = engine.create_table("public.t", &cols, &[0], false).unwrap();
    let schema = engine.get_schema(tid).unwrap();

    // Raw ingest (SAL recovery path — no hooks, no unique_pk, no index projection)
    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(10u128, 1);
    bb.put_u64(1000);
    bb.end_row();
    bb.begin_row(20u128, 1);
    bb.put_u64(2000);
    bb.end_row();
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
    let _tid = engine.create_table("public.t", &cols, &[0], false).unwrap();

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
    let tid = engine.create_table("public.parent", &cols, &[0], false).unwrap();

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
    let child_tid = engine.create_table("public.child", &child_cols, &[0], false).unwrap();

    // FK count
    assert!(engine.get_fk_count(child_tid) > 0);
    let (_col_idx, target_id, _parent_col) = engine.get_fk_constraint(child_tid, 0).unwrap();
    assert_eq!(target_id, tid);

    // Create an explicit index
    let _iid = engine.create_index("public.parent", &["val"], false).unwrap();

    assert!(engine.get_index_circuit_count(tid) > 0);
    let (ic_cols, _is_unique) = engine.get_index_circuit_info(tid, 0).unwrap();
    assert_eq!(ic_cols.as_slice(), [1]); // val is column 1

    // Index store handle should be non-null
    let idx_ptr = engine.get_index_store_handle(tid, &[1]);
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
    let tid1 = engine.create_table("public.t1", &cols, &[0], false).unwrap();
    let tid2 = engine.create_table("public.t2", &cols, &[0], false).unwrap();

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
        ColumnDef {
            name: "pk".into(),
            type_code: type_code::U64,
            is_nullable: false,
            fk_table_id: 0,
            fk_col_idx: 0,
        },
        ColumnDef {
            name: "alpha".into(),
            type_code: type_code::U64,
            is_nullable: false,
            fk_table_id: 0,
            fk_col_idx: 0,
        },
        ColumnDef {
            name: "beta".into(),
            type_code: type_code::U64,
            is_nullable: false,
            fk_table_id: 0,
            fk_col_idx: 0,
        },
    ];
    let tid = engine.create_table("public.t", &cols, &[0], false).unwrap();

    let names1 = engine.get_column_names(tid);
    assert_eq!(names1, vec!["pk", "alpha", "beta"]);

    // Second call should hit cache
    let names2 = engine.get_column_names(tid);
    assert_eq!(names2, vec!["pk", "alpha", "beta"]);

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_view_deps_compound_pk_round_trip() {
    // After the dep_tab compound-PK migration, get_dep_map must extract
    // (view_id, dep_table_id) from the PK region. Write deps for view 7 →
    // tables 100 and 200, then verify the rebuilt dep/source maps.
    let dir = temp_dir("view_deps_compound");
    let mut engine = CatalogEngine::open(&dir).unwrap();

    engine.write_view_deps(7, &[100, 200]).unwrap();
    engine.dag.invalidate_dep_map();

    let dep_map = engine.dag.get_dep_map().clone();
    assert_eq!(dep_map.get(&100), Some(&vec![7]), "table 100 must feed view 7");
    assert_eq!(dep_map.get(&200), Some(&vec![7]), "table 200 must feed view 7");

    let mut sources = engine.dag.get_source_ids(7);
    sources.sort_unstable();
    assert_eq!(sources, vec![100, 200], "view 7 sources extracted from PK");

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_circuit_table_surface_introspectable() {
    let dir = temp_dir("circuit_surface");
    let mut engine = CatalogEngine::open(&dir).unwrap();

    // Inject a row directly into CIRCUIT_NODES so the store is non-empty.
    // Compound-PK layout: (view_id U64, sub U64) primary key, then payload
    // node_id U64, opcode U64, source_table U64?, expr_program BLOB?.
    let schema = circuit_nodes_schema();
    let mut bb = BatchBuilder::new(schema);
    let pk = pack_view_pk(0, 42); // view_id=0, sub=node_id=42
    bb.begin_row(pk, 1);
    bb.put_u64(42); // node_id
    bb.put_u64(11); // opcode
    bb.put_null(); // source_table (NULL)
    bb.put_null(); // expr_program (NULL)
    bb.end_row();
    engine.ingest_to_family(CIRCUIT_NODES_TAB_ID, &bb.finish()).unwrap();

    // The new schema is SQL-introspectable — `SELECT * FROM CircuitNodes`
    // must return what we just inserted (full-scan path, used by SQL planner).
    let scan = engine.scan_family(CIRCUIT_NODES_TAB_ID).unwrap();
    assert_eq!(scan.count, 1, "scan_family must expose CircuitNodes rows");

    // Compound PK: seek by the 16-byte at-rest PK region. begin_row writes
    // extend_pk(pk) = pk.to_be_bytes(), the OPK image (view_id_BE ++ sub_BE).
    let pk_be = pk.to_be_bytes();
    let pk_bytes = &pk_be[..16];
    let found = engine.seek_family_bytes(CIRCUIT_NODES_TAB_ID, pk_bytes).unwrap();
    assert!(found.is_some(), "seek_family_bytes must find CircuitNodes row by PK");
    let found = found.unwrap();
    assert_eq!(found.count, 1, "seek_family_bytes must return exactly one row");

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_read_batch_string_out_of_bounds_offset_returns_empty() {
    // A registry batch whose STRING struct is a long string (len > 12) with a blob
    // offset past the (empty) arena must read back empty, not abort: read_batch_string
    // disposes of the decoder's None via unwrap_or_default. Reachable only via corrupt
    // wire input — a correct client never emits an out-of-bounds offset.
    let dir = temp_dir("read_batch_oob");
    let mut engine = CatalogEngine::open(&dir).unwrap();

    let schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U128, 0),
            SchemaColumn::new(type_code::STRING, 0),
        ],
        &[0],
    );
    let mut batch = Batch::with_schema(schema, 1);
    batch.extend_pk(1);
    batch.extend_weight(&1i64.to_le_bytes());
    batch.extend_null_bmp(&0u64.to_le_bytes());
    let mut st = [0u8; 16];
    st[0..4].copy_from_slice(&100u32.to_le_bytes()); // len 100 (> 12 → reads blob)
    st[8..16].copy_from_slice(&0u64.to_le_bytes()); // offset 0 into empty blob
    batch.extend_col(0, &st);
    batch.count += 1;

    // payload_col 0 is the STRING column; the corrupt offset must decode to "".
    assert_eq!(engine.read_batch_string(&batch, 0, 0), String::new());

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}
