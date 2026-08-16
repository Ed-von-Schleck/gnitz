use super::*;

// ── test_enforce_unique_pk ───────────────────────────────────────────

/// Unique-PK enforcement through the production store path: insert,
/// intra-batch dedup, insert+delete cancellation, and the `+1, -1, +1`
/// re-insert regression (the deleted Single-store variant netted this to 0 by
/// failing to clear its intra-batch `seen` map on the delete).
#[test]
fn test_enforce_unique_pk() {
    let dir = temp_dir("enforce_upk");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
    let cols = vec![col_def("id", type_code::U64), col_def("val", type_code::U64)];
    let tid = engine.create_table("public.t", &cols, &[0]).unwrap();
    let schema = engine.get_schema_desc(tid).unwrap();

    let make_row = |pk: u64, val: u64, w: i64| -> Batch {
        let mut bb = BatchBuilder::new(schema);
        bb.begin_row(pk as u128, w);
        bb.put_u64(val);
        bb.end_row();
        bb.finish()
    };
    let live = |engine: &mut CatalogEngine| -> usize {
        engine.flush_family(tid).unwrap();
        engine.scan_family(tid).unwrap().0.count
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
        let mut engine = CatalogEngine::open(&dir, 1).unwrap();
        engine
            .sys_store_mut(SysFamily::Index)
            .ingest_borrowed_batch(&idx_tab_batch(888, 99999, 1, "orphaned_idx", false, 1))
            .unwrap();
        let _ = engine.sys_store_mut(SysFamily::Index).flush();
        engine.close();
        drop(engine);
    }

    // Re-open should fail because the orphaned index references table 99999
    let result = CatalogEngine::open(&dir, 1);
    assert!(result.is_err(), "Orphaned index metadata should cause error on reload");

    let _ = fs::remove_dir_all(&dir);
}

// ── user-table SERIAL sequences ──────────────────────────────────────

/// `reserve_user_sequence` seeds an absent sequence at base 1, hands out
/// contiguous ranges, and advances the in-memory high-water each call.
#[test]
fn test_reserve_user_sequence_seed_and_contiguous() {
    let dir = temp_dir("reserve_user_seq");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
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
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
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
        let mut engine = CatalogEngine::open(&dir, 1).unwrap();
        let (base, delta, _) = engine.reserve_user_sequence(user_seq, 64);
        assert_eq!(base, 1);
        // Clear the synchronously-set map entry to prove the hook re-populates it.
        engine.user_sequences.remove(&user_seq);
        engine.ingest_to_family(SEQ_TAB_ID, &delta).unwrap();
        assert_eq!(engine.user_sequences.get(&user_seq).copied(), Some(64));
        let _ = engine.sys_store_mut(SysFamily::Sequence).flush();
        engine.close();
    }
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
    assert_eq!(engine.user_sequences.get(&user_seq).copied(), Some(64));
    let (base2, _delta, _) = engine.reserve_user_sequence(user_seq, 64);
    assert_eq!(base2, 65, "next id continues after the recovered high-water");
    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

/// `recover_sequences` recovers the checkpoint generation (seq id 4) and the
/// topology (seq id 5) written by `bump_checkpoint_generation` / `record_topology`,
/// COW-inheritable by forked workers and read by commit-3 recovery.
#[test]
fn test_recover_checkpoint_gen_and_topology() {
    let dir = temp_dir("recover_ckpt_records");
    let expected_topology = crate::storage::topology_word(4);
    {
        let mut engine = CatalogEngine::open(&dir, 1).unwrap();
        assert_eq!(engine.committed_generation, 0, "fresh DB starts at generation 0");
        assert_eq!(engine.recorded_topology, 0, "fresh DB has no topology row");
        // Boot order: the topology row is written first and its durability
        // rides the following gen bump's system-table flush.
        engine.record_topology(4);
        assert_eq!(engine.recorded_topology, expected_topology);
        assert_eq!(engine.bump_checkpoint_generation(), 1);
        assert_eq!(engine.bump_checkpoint_generation(), 2, "generation is monotonic");
        engine.close();
    }
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
    assert_eq!(
        engine.committed_generation, 2,
        "recovered checkpoint generation survives a reopen",
    );
    assert_eq!(
        engine.recorded_topology, expected_topology,
        "recovered topology (worker_count << 32 | STATE_FORMAT) survives a reopen",
    );
    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

/// `recovery_start_generation_bump` advances the durable checkpoint generation
/// G → G+1 and the in-memory field, and the subsequent `boot_checkpoint` bump
/// then goes G+1 → G+2 (retracting G+1, not G, so `_sequences` stays clean). Each
/// step is monotonic, recovered across a reopen via the `.max()` arm.
#[test]
fn test_recovery_start_generation_bump_monotonic() {
    let dir = temp_dir("recovery_start_gen_bump");
    {
        // Simulate two prior checkpoints so the recovered G is 2, not 0.
        let mut engine = CatalogEngine::open(&dir, 1).unwrap();
        engine.record_topology(4);
        assert_eq!(engine.bump_checkpoint_generation(), 1);
        assert_eq!(engine.bump_checkpoint_generation(), 2);
        engine.close();
    }
    {
        // Recovery start: bump G=2 → 3 durably; the field advances.
        let mut engine = CatalogEngine::open(&dir, 1).unwrap();
        assert_eq!(engine.committed_generation, 2, "recovered G");
        engine.recovery_start_generation_bump().unwrap();
        assert_eq!(
            engine.committed_generation, 3,
            "recovery-start bump advances the field to G+1"
        );
        // boot_checkpoint's bump then retracts G+1 and inserts G+2.
        assert_eq!(engine.bump_checkpoint_generation(), 4, "boot_checkpoint goes G+1 → G+2");
        engine.close();
    }
    // The final durable generation survives a reopen monotonically.
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
    assert_eq!(
        engine.committed_generation, 4,
        "recovery-start + boot_checkpoint bumps recovered monotonically",
    );
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
        let mut engine = CatalogEngine::open(&dir, 1).unwrap();
        let schema = SysFamily::Sequence.schema();
        let mut bb = BatchBuilder::new(schema);
        bb.begin_row(stray as u128, 1);
        bb.put_u64(999);
        bb.end_row();
        engine
            .sys_store_mut(SysFamily::Sequence)
            .ingest_borrowed_batch(&bb.finish())
            .unwrap();
        let _ = engine.sys_store_mut(SysFamily::Sequence).flush();
        engine.close();
    }
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
    assert!(!engine.user_sequences.contains_key(&stray));
    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── test_sequence_gap_recovery ───────────────────────────────────────

#[test]
fn test_sequence_gap_recovery() {
    let dir = temp_dir("seq_gap");
    let cols = vec![col_def("id", type_code::U64)];

    // First open: create a table, then inject a table record with high ID 250
    {
        let mut engine = CatalogEngine::open(&dir, 1).unwrap();
        engine.create_table("public.t1", &cols, &[0]).unwrap();

        // Inject table record for tid=250 directly into sys_tables
        engine
            .sys_store_mut(SysFamily::Table)
            .ingest_borrowed_batch(&build_table_tab_row(250, 0, "gap_table"))
            .unwrap();

        // Inject column record for tid=250
        let mut cbb = BatchBuilder::new(SysFamily::Column.schema());
        push_col_tab_row(&mut cbb, 250, OWNER_KIND_TABLE, 0, &col_def("id", type_code::U64), 1);
        engine
            .sys_store_mut(SysFamily::Column)
            .ingest_borrowed_batch(&cbb.finish())
            .unwrap();

        let _ = engine.sys_store_mut(SysFamily::Table).flush();
        let _ = engine.sys_store_mut(SysFamily::Column).flush();
        engine.close();
        drop(engine);
    }

    // Re-open: sequence should recover to 251
    {
        let mut engine = CatalogEngine::open(&dir, 1).unwrap();
        let new_tid = engine.create_table("public.tnext", &cols, &[0]).unwrap();
        assert_eq!(new_tid, 251, "Sequence recovery: expected 251, got {new_tid}");
        engine.close();
    }

    let _ = fs::remove_dir_all(&dir);
}

// ── test_ingest_scan_seek_family ──────────────────────────────────────

#[test]
fn test_ingest_scan_seek_family() {
    let dir = temp_dir("catalog_ingest_scan_seek");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
    let cols = vec![col_def("id", type_code::U64), col_def("val", type_code::U64)];
    let tid = engine.create_table("public.t", &cols, &[0]).unwrap();
    let schema = engine.get_schema_desc(tid).unwrap();

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
    let scan_batch = engine.scan_family(tid).unwrap().0;
    assert_eq!(scan_batch.count, 3);

    // Seek existing
    let found = engine.seek_family(tid, 2u128, &[]).unwrap().0;
    assert!(found.is_some());
    let row = found.unwrap();
    assert_eq!(row.count, 1);
    assert_eq!(row.get_pk(0), 2);

    // Seek missing
    let not_found = engine.seek_family(tid, 99u128, &[]).unwrap().0;
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
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
    let cols = vec![col_def("id", type_code::U64), col_def("val", type_code::U64)];
    let tid = engine.create_table("public.t", &cols, &[0]).unwrap();

    // System-table point seek (narrow stride 8, empty extra).
    let found = engine.seek_family(TABLE_TAB_ID, tid as u128, &[]).unwrap().0;
    assert!(
        found.is_some(),
        "seek_family must resolve the TABLE_TAB row for the created table",
    );
    assert_eq!(found.unwrap().count, 1);

    // A missing system-table key returns None — not an error, not a panic.
    let missing = engine.seek_family(TABLE_TAB_ID, 9_999_999u128, &[]).unwrap().0;
    assert!(missing.is_none(), "absent system-table key seeks to None");

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── test_ingest_pk_enforced_through_the_store ───────────────────────────────

#[test]
fn test_ingest_pk_enforced_through_the_store() {
    let dir = temp_dir("catalog_pk_enforced_store");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
    let cols = vec![col_def("id", type_code::U64), col_def("val", type_code::U64)];
    let tid = engine.create_table("public.t", &cols, &[0]).unwrap();
    let schema = engine.get_schema_desc(tid).unwrap();

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
    let scan = engine.scan_family(tid).unwrap().0;
    assert_eq!(scan.count, 1);
    assert_eq!(scan.get_pk(0), 1);

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── test_ddl_sync ─────────────────────────────────────────────────────

#[test]
fn test_ddl_sync() {
    let dir = temp_dir("catalog_ddl_sync");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();

    // Create a schema via normal DDL
    engine.create_schema("app").unwrap();
    assert!(engine.has_schema("app"));

    // Simulate DDL sync: create batch mimicking a schema record
    let schema = SysFamily::Schema.schema();
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
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();

    // Zone 5: create a schema. SCHEMA_TAB pinned to lsn=5.
    engine.ctx.open_ddl_zone(zone(5));
    engine.create_schema("z").unwrap();
    assert_eq!(engine.get_max_flushed_lsn(SCHEMA_TAB_ID), 5);

    // Zone 7: create a table. TABLE_TAB and COL_TAB pinned to lsn=7;
    // SCHEMA_TAB stays at 5 (untouched in this zone).
    engine.ctx.open_ddl_zone(zone(7));
    let cols = vec![col_def("id", type_code::U64), col_def("val", type_code::U64)];
    let _tid = engine.create_table("z.t", &cols, &[0]).unwrap();
    assert_eq!(engine.get_max_flushed_lsn(TABLE_TAB_ID), 7);
    assert_eq!(engine.get_max_flushed_lsn(COL_TAB_ID), 7);
    assert_eq!(
        engine.get_max_flushed_lsn(SCHEMA_TAB_ID),
        5,
        "SCHEMA_TAB stays at the most recent zone that touched it"
    );

    // Zone 9: another table. TABLE_TAB and COL_TAB advance to lsn=9.
    engine.ctx.open_ddl_zone(zone(9));
    let _tid2 = engine.create_table("z.t2", &cols, &[0]).unwrap();
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

// ── test_store_detach ─────────────────────────────────────────────────

/// Detaching (what the post-fork master does) leaves every registered relation
/// readable-as-empty and writable-as-a-no-op, rather than panicking or holding a
/// second live `Table` on worker 0's directory.
#[test]
fn test_store_detach() {
    let dir = temp_dir("catalog_store_detach");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
    let cols = vec![col_def("id", type_code::U64), col_def("val", type_code::U64)];
    let tid = engine.create_table("public.t", &cols, &[0]).unwrap();
    assert!(engine.owns_stores);

    engine.detach_user_stores();
    assert!(!engine.owns_stores);
    let entry = engine.dag.tables.get(&tid).unwrap();
    assert!(entry.handle.is_detached());
    assert!(!entry.open_cursor().valid, "a detached store reads empty");
    assert_eq!(entry.handle.current_lsn(), 0);
    engine.invalidate_all_plans();

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── test_fk_index_metadata_queries ───────────────────────────────────

#[test]
fn test_fk_index_metadata_queries() {
    let dir = temp_dir("catalog_fk_idx_meta");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();

    let cols = vec![col_def("id", type_code::U64), col_def("val", type_code::U64)];
    let tid = engine.create_table("public.parent", &cols, &[0]).unwrap();

    // Create child table with FK to parent
    let child_cols = vec![
        col_def("id", type_code::U64),
        fk_def("parent_id", crate::schema::type_code::U64, tid, 0),
    ];
    let child_tid = engine.create_table("public.child", &child_cols, &[0]).unwrap();

    // FK count
    assert!(!engine.fk_constraints_of(child_tid).is_empty());
    let target_id = engine.fk_constraints_of(child_tid)[0].parent_tid;
    assert_eq!(target_id, tid);

    // Create an explicit index
    let _iid = engine.create_index("public.parent", &["val"], false).unwrap();

    assert!(!engine.index_circuits(tid).is_empty());
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
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();

    let cols = vec![col_def("id", type_code::U64), col_def("val", type_code::U64)];
    let tid1 = engine.create_table("public.t1", &cols, &[0]).unwrap();
    let tid2 = engine.create_table("public.t2", &cols, &[0]).unwrap();

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
fn test_column_defs_cached() {
    let dir = temp_dir("catalog_colnames");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
    let cols = vec![
        col_def("pk", type_code::U64),
        col_def("alpha", type_code::U64),
        col_def("beta", type_code::U64),
    ];
    let tid = engine.create_table("public.t", &cols, &[0]).unwrap();

    let names =
        |e: &mut CatalogEngine| -> Vec<String> { e.read_column_defs(tid).iter().map(|cd| cd.name.clone()).collect() };
    assert_eq!(names(&mut engine), vec!["pk", "alpha", "beta"]);

    // Second call should hit cache
    assert!(engine.caches.col_defs.contains_key(&tid));
    assert_eq!(names(&mut engine), vec!["pk", "alpha", "beta"]);

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_dep_map_is_the_scan_delta_nodes() {
    // The dep map is derived from the circuit's `ScanDelta` nodes: the view id
    // off the compound PK region, the source off the `source_table` column. Two
    // distinct sources give two edges; a repeated source gives one; and neither
    // a non-`ScanDelta` node carrying a `source_table` nor a non-positive source
    // id gives any.
    let dir = temp_dir("dep_map_scan_delta");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();

    write_circuit_chain(
        &mut engine,
        7,
        &[
            (gnitz_wire::OPCODE_SCAN_DELTA, Some(100), None),
            (gnitz_wire::OPCODE_SCAN_DELTA, Some(200), None),
            (gnitz_wire::OPCODE_SCAN_DELTA, Some(100), None),
            (gnitz_wire::OPCODE_SCAN_DELTA, Some(0), None),
            (gnitz_wire::OPCODE_INTEGRATE, Some(300), None),
        ],
    );

    let dep_map = engine.dag.get_dep_map().clone();
    assert_eq!(dep_map.get(&100), Some(&vec![7]), "the repeated source yields one edge");
    assert_eq!(dep_map.get(&200), Some(&vec![7]), "table 200 must feed view 7");
    assert_eq!(dep_map.get(&300), None, "a non-ScanDelta node contributes no edge");
    assert_eq!(dep_map.get(&0), None, "a non-positive source id contributes no edge");

    let mut sources = engine.dag.get_source_ids(7);
    sources.sort_unstable();
    assert_eq!(sources, vec![100, 200], "both ScanDelta sources of view 7");

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_dep_map_view_on_view_chain() {
    // A view over a view: each segment's own `ScanDelta` is its edge, in both
    // map directions.
    let dir = temp_dir("dep_map_view_chain");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();

    write_identity_circuit(&mut engine, 7, 100, None);
    write_identity_circuit(&mut engine, 8, 7, None);

    let dep_map = engine.dag.get_dep_map().clone();
    assert_eq!(dep_map.get(&100), Some(&vec![7]), "base 100 feeds view 7");
    assert_eq!(dep_map.get(&7), Some(&vec![8]), "view 7 feeds view 8");
    assert_eq!(engine.dag.get_source_ids(7), vec![100]);
    assert_eq!(engine.dag.get_source_ids(8), vec![7]);
    // The dependency order every cascade walks — `hook_view_register`'s
    // registration order and `compute_invalid_views`' invalidity propagation.
    assert_eq!(engine.dag.order_by_view_deps(&[8, 7]), vec![7, 8]);

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

/// A retired view contributes no edges: DROP VIEW and the ALTER VIEW
/// `replaces` path both retract the outgoing view's circuit rows, and the map
/// is rebuilt from what remains.
#[test]
fn test_dep_map_drops_a_retired_views_edges() {
    let dir = temp_dir("dep_map_retired");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();

    let cols = vec![col_def("id", type_code::U64)];
    let tid = engine.create_table("public.t", &cols, &[0]).unwrap();

    let v1 = register_identity_view(&mut engine, tid, "v1", &cols);
    assert_eq!(engine.dag.get_dep_map().get(&tid), Some(&vec![v1]));

    // ALTER VIEW: one VIEW_TAB batch retiring v1 and registering v2.
    let v2 = engine.allocate_table_id();
    write_identity_circuit(&mut engine, v2, tid, None);
    engine.write_column_records(v2, OWNER_KIND_VIEW, &cols).unwrap();
    let mut bb = BatchBuilder::new(SysFamily::View.schema());
    push_view_tab_row(&mut bb, -1, v1, "v1", "");
    push_view_tab_row(&mut bb, 1, v2, "v1", "");
    engine.ingest_to_family(VIEW_TAB_ID, &bb.finish()).unwrap();
    assert_eq!(
        engine.dag.get_dep_map().get(&tid),
        Some(&vec![v2]),
        "the replaced view's edge is gone, the replacement's is present"
    );

    // DROP VIEW retires the last edge, so the base table is a dep-map orphan.
    engine.drop_view("public.v1").unwrap();
    engine.drain_pending_dir_deletions();
    assert_eq!(engine.dag.get_dep_map().get(&tid), None);

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

/// The two dependent-view RESTRICTs — DROP TABLE and DROP COLUMN — both read
/// the CIRCUIT_NODES-backed map.
#[test]
fn test_dependent_view_restricts_fire_from_circuit_rows() {
    let dir = temp_dir("dep_map_restrict");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();

    let cols = vec![col_def("id", type_code::U64), col_def("val", type_code::U64)];
    let tid = engine.create_table("public.t", &cols, &[0]).unwrap();

    register_identity_view(&mut engine, tid, "v", &cols);

    let err = engine.drop_table("public.t").unwrap_err();
    assert!(err.contains("View dependency"), "DROP TABLE RESTRICT: {err}");

    // DROP NOT NULL on `val` — an `is_nullable 0→1` rewrite pair.
    let mut nullable = cols[1].clone();
    nullable.is_nullable = true;
    let mut bb = BatchBuilder::new(SysFamily::Column.schema());
    push_col_tab_row(&mut bb, tid, OWNER_KIND_TABLE, 1, &cols[1], -1);
    push_col_tab_row(&mut bb, tid, OWNER_KIND_TABLE, 1, &nullable, 1);
    let err = engine.precheck_family(SysFamily::Column, &bb.finish()).unwrap_err();
    assert!(err.contains("dependent views"), "DROP NOT NULL RESTRICT: {err}");

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_circuit_table_surface_introspectable() {
    let dir = temp_dir("circuit_surface");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();

    // Inject a row directly into CIRCUIT_NODES so the store is non-empty.
    // Compound-PK layout: (view_id U64, sub U64) primary key, then payload
    // node_id U64, opcode U64, source_table U64?, expr_program BLOB?.
    let schema = SysFamily::CircuitNodes.schema();
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
    let scan = engine.scan_family(CIRCUIT_NODES_TAB_ID).unwrap().0;
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
fn test_read_payload_string_out_of_bounds_offset_returns_empty() {
    // A batch whose STRING struct is a long string (len > 12) with a blob
    // offset past the (empty) arena must read back empty, not abort:
    // Batch::read_payload_string disposes of the decoder's None via
    // unwrap_or_default. Reachable only via corrupt wire input — a correct
    // client never emits an out-of-bounds offset.

    let schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U128, 0),
            SchemaColumn::new(type_code::STRING, 0),
        ],
        &[0],
    );
    let mut batch = Batch::with_capacity(schema, 1);
    batch.extend_pk(1);
    batch.extend_weight(&1i64.to_le_bytes());
    batch.extend_null_bmp(&0u64.to_le_bytes());
    let mut st = [0u8; 16];
    st[0..4].copy_from_slice(&100u32.to_le_bytes()); // len 100 (> 12 → reads blob)
    st[8..16].copy_from_slice(&0u64.to_le_bytes()); // offset 0 into empty blob
    batch.extend_col(0, &st);
    batch.count += 1;

    // payload_col 0 is the STRING column; the corrupt offset must decode to "".
    assert_eq!(batch.read_payload_string(0, 0), String::new());
}
