use super::*;
use gnitz_wire::control::Target;

// ── test_orphaned_metadata_recovery ─────────────────────────────────

#[test]
fn test_orphaned_metadata_recovery() {
    let dir = temp_dir("orphaned");

    // First open: inject an index record pointing to non-existent table 99999
    {
        let mut engine = CatalogEngine::open(&dir, 1).unwrap();
        engine
            .registry
            .ingest(
                SysFamily::Index.id(),
                idx_tab_batch(888, 99999, &[1], "orphaned_idx", false, 1),
            )
            .unwrap();
        let _ = engine.registry.checkpoint_system(engine.system_zone);
        engine.close();
    }

    // The orphaned index names a table the reopened catalog does not hold.
    let err = CatalogEngine::open(&dir, 1).err().expect("the reopen must fail");
    assert!(err.contains("99999"), "{err}");

    let _ = fs::remove_dir_all(&dir);
}

// ── user-table SERIAL sequences ──────────────────────────────────────

#[test]
fn test_reserve_user_sequence_seed_and_contiguous() {
    let dir = temp_dir("reserve_user_seq");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
    let seq_id = engine.create_serial_table("public.t").unwrap();

    let (base1, delta1) = engine.reserve_user_sequence(seq_id, 64).unwrap();
    assert_eq!(base1, 1);
    assert_eq!(delta1.len(), 1, "first use inserts, retracts nothing");
    assert_eq!(delta1.get_weight(0), 1);
    engine.ingest_to_family(gnitz_wire::SEQ_TAB, &delta1).unwrap();
    assert_eq!(engine.sequence_value(seq_id), Some(64));

    let (base2, delta2) = engine.reserve_user_sequence(seq_id, 64).unwrap();
    assert_eq!(base2, 65);
    assert_eq!(delta2.len(), 2);
    assert_eq!(delta2.get_weight(0), -1);
    assert_eq!(payload_u64(&delta2, 0, 0), 64, "the -1 must carry the live high-water");
    assert_eq!(delta2.get_weight(1), 1);
    assert_eq!(payload_u64(&delta2, 1, 0), 128);
    engine.ingest_to_family(gnitz_wire::SEQ_TAB, &delta2).unwrap();
    assert_eq!(engine.sequence_value(seq_id), Some(128));

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_reserve_user_sequence_rejects_exhausted_range() {
    let dir = temp_dir("reserve_user_seq_exhausted");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();

    let full = engine.create_serial_table("public.full").unwrap();
    engine
        .ingest_to_family(gnitz_wire::SEQ_TAB, &engine.sequence_delta(full, (i64::MAX - 1) as u64))
        .unwrap();
    let err = engine.reserve_user_sequence(full, 64).err().unwrap();
    assert!(err.contains("invalid or exhausted"), "{err}");
    let edge = engine.create_serial_table("public.edge").unwrap();
    engine.reserve_user_sequence(edge, 0).err().unwrap();
    engine.reserve_user_sequence(edge, 1 << 63).err().unwrap();
    engine.reserve_user_sequence(edge + 1000, 1).err().unwrap();

    engine
        .ingest_to_family(
            gnitz_wire::SEQ_TAB,
            &engine.sequence_delta(edge, (i64::MAX - 65) as u64),
        )
        .unwrap();
    let (base, delta) = engine.reserve_user_sequence(edge, 64).unwrap();
    assert_eq!(base, i64::MAX - 64);
    assert_eq!(payload_u64(&delta, 1, 0), (i64::MAX - 1) as u64, "last = i64::MAX - 1");

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

/// `count` arrives raw off the wire.
#[test]
fn allocate_ids_hands_out_contiguous_runs_under_the_ceiling() {
    let dir = temp_dir("alloc_ids");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
    let a = engine.allocate_ids(3).unwrap();
    assert_eq!(engine.allocate_ids(1).unwrap(), a + 3);
    for count in [0, gnitz_wire::CATALOG_ID_CEILING, u64::MAX] {
        engine.allocate_ids(count).unwrap_err();
    }
    assert_eq!(engine.allocate_ids(1).unwrap(), a + 4, "a refused run consumes nothing");
    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

/// An id enters the catalog only through `allocate_ids`. A `+1` row at one the
/// counter has not reached is refused before anything applies, so it cannot
/// raise the counter to the ceiling and exhaust every later allocation.
#[test]
fn a_row_at_an_unallocated_id_is_refused_and_leaves_allocation_working() {
    let dir = temp_dir("unallocated_id");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
    let next = engine.allocate_ids(1).unwrap() + 1;
    for id in [next, next + 4096, gnitz_wire::CATALOG_ID_CEILING - 1] {
        let err = engine
            .submit(SysFamily::Schema, schema_tab_batch(&[(id, "forged", 1)]))
            .unwrap_err();
        assert!(err.contains("never allocated"), "schema {id}: {err}");
    }
    assert_eq!(engine.allocate_ids(1).unwrap(), next);
    engine
        .submit(SysFamily::Schema, schema_tab_batch(&[(next, "granted", 1)]))
        .unwrap();
    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_user_sequence_durable_roundtrip() {
    let dir = temp_dir("user_seq_roundtrip");
    let user_seq;
    {
        let mut engine = CatalogEngine::open(&dir, 1).unwrap();
        user_seq = engine.create_serial_table("public.t").unwrap();
        let (base, delta) = engine.reserve_user_sequence(user_seq, 64).unwrap();
        assert_eq!(base, 1);
        engine.ingest_to_family(gnitz_wire::SEQ_TAB, &delta).unwrap();
        assert_eq!(engine.sequence_value(user_seq), Some(64));
        let _ = engine.registry.checkpoint_system(engine.system_zone);
        engine.close();
    }
    let engine = CatalogEngine::open(&dir, 1).unwrap();
    assert_eq!(engine.sequence_value(user_seq), Some(64));
    let (base2, _delta) = engine.reserve_user_sequence(user_seq, 64).unwrap();
    assert_eq!(base2, 65, "next id continues after the recovered high-water");
    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_recover_checkpoint_gen_and_topology() {
    let dir = temp_dir("recover_ckpt_records");
    let expected_topology = crate::catalog::sequences::topology_word(4);
    {
        let mut engine = CatalogEngine::open(&dir, 1).unwrap();
        assert_eq!(engine.durable_generation(), 0);
        assert_eq!(engine.sequence_value(SEQ_ID_TOPOLOGY).unwrap_or(0), 0);
        engine.record_topology(4).unwrap();
        assert_eq!(engine.sequence_value(SEQ_ID_TOPOLOGY).unwrap_or(0), expected_topology);
        assert_eq!(engine.advance_durable_generation().unwrap(), 1);
        assert_eq!(engine.advance_durable_generation().unwrap(), 2);
        engine.close();
    }
    let engine = CatalogEngine::open(&dir, 1).unwrap();
    assert_eq!(engine.durable_generation(), 2);
    assert_eq!(engine.sequence_value(SEQ_ID_TOPOLOGY).unwrap_or(0), expected_topology);
    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_boot_generation_advance_monotonic() {
    let dir = temp_dir("recovery_start_gen_bump");
    {
        let mut engine = CatalogEngine::open(&dir, 1).unwrap();
        engine.record_topology(4).unwrap();
        assert_eq!(engine.advance_durable_generation().unwrap(), 1);
        assert_eq!(engine.advance_durable_generation().unwrap(), 2);
        engine.close();
    }
    {
        let mut engine = CatalogEngine::open(&dir, 1).unwrap();
        assert_eq!(engine.durable_generation(), 2);
        assert_eq!(engine.advance_durable_generation().unwrap(), 3);
        assert_eq!(engine.resume_generation, 2);
        assert_eq!(engine.sequence_value(SEQ_ID_CHECKPOINT_GEN), Some(3));
        assert_eq!(engine.advance_durable_generation().unwrap(), 4);
        engine.close();
    }
    let engine = CatalogEngine::open(&dir, 1).unwrap();
    assert_eq!(engine.durable_generation(), 4);
    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

/// A drop, a system flush and a reopen must not hand out again an id a dropped
/// relation held.
#[test]
fn a_dropped_relations_id_is_not_handed_out_again() {
    let dir = temp_dir("dropped_id_not_reissued");
    let cols = vec![col_def("id", TypeCode::U64)];
    let tid;
    {
        let mut engine = CatalogEngine::open(&dir, 1).unwrap();
        tid = engine.create_table("public.dropped", &cols, &[0]).unwrap();
        engine.submit_retraction(SysFamily::Table, tid).unwrap();
        engine.flush_all_system_tables().unwrap();
        engine.close();
    }
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
    assert!(!engine.registry.has_id(tid));
    assert!(engine.allocate_ids(1).unwrap() > tid);
    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── test_sequence_gap_recovery ───────────────────────────────────────

#[test]
fn test_sequence_gap_recovery() {
    let dir = temp_dir("seq_gap");
    let cols = vec![col_def("id", TypeCode::U64)];

    // First open: create a table, then inject a table record with high ID 250
    {
        let mut engine = CatalogEngine::open(&dir, 1).unwrap();
        engine.create_table("public.t1", &cols, &[0]).unwrap();

        // Inject table record for tid=250 directly into sys_tables
        engine
            .registry
            .ingest(SysFamily::Table.id(), table_tab_batch(&[(250, "gap_table", 1)]))
            .unwrap();

        // Inject column record for tid=250
        let mut cbb = BatchBuilder::new(SysFamily::Column.schema());
        col_def("id", TypeCode::U64).write_col_tab_row(&mut cbb, 250, 0, 1);
        engine.registry.ingest(SysFamily::Column.id(), cbb.finish()).unwrap();

        let _ = engine.registry.checkpoint_system(engine.system_zone);
        let _ = engine.registry.checkpoint_system(engine.system_zone);
        engine.close();
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
    let cols = vec![col_def("id", TypeCode::U64), col_def("val", TypeCode::U64)];
    let tid = engine.create_table("public.t", &cols, &[0]).unwrap();
    let schema = engine.registry.relation(tid).map(Relation::schema).unwrap();

    // Ingest via CatalogEngine (user table path)
    let mut bb = BatchBuilder::new(&schema);
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
    engine.registry.checkpoint_base().unwrap();

    // Scan
    let scan_batch = scan_all(&mut engine, tid);
    assert_eq!(scan_batch.len(), 3);

    // Point read, present
    let row = pk_group_native(&mut engine, tid, 2);
    assert_eq!(row.len(), 1);
    assert_eq!(row.get_pk(0), 2);

    // Point read, missing
    assert!(pk_group_native(&mut engine, tid, 99).is_empty());

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── a point read resolves a system-table row ───────────

/// A system family answers a point read like any relation: `create_table` writes a
/// TABLE_TAB row keyed by the new table id.
#[test]
fn point_read_resolves_system_table_row() {
    let dir = temp_dir("catalog_seek_system_table");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
    let cols = vec![col_def("id", TypeCode::U64), col_def("val", TypeCode::U64)];
    let tid = engine.create_table("public.t", &cols, &[0]).unwrap();

    assert_eq!(
        pk_group_native(&mut engine, gnitz_wire::TABLE_TAB, tid as u128).len(),
        1,
        "the TABLE_TAB row of the created table"
    );
    assert!(pk_group_native(&mut engine, gnitz_wire::TABLE_TAB, 9_999_999).is_empty());

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── test_ingest_pk_enforced_through_the_store ───────────────────────────────

#[test]
fn test_ingest_pk_enforced_through_the_store() {
    let dir = temp_dir("catalog_pk_enforced_store");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
    let cols = vec![col_def("id", TypeCode::U64), col_def("val", TypeCode::U64)];
    let tid = engine.create_table("public.t", &cols, &[0]).unwrap();
    let schema = engine.registry.relation(tid).map(Relation::schema).unwrap();

    // Insert row with PK=1, val=100
    let mut bb = BatchBuilder::new(&schema);
    bb.begin_row(1u128, 1);
    bb.put_u64(100);
    bb.end_row();
    engine.ingest_to_family(tid, &bb.finish()).unwrap();
    engine.registry.checkpoint_base().unwrap();

    // Insert row with PK=1 again, val=200 (should retract old + insert new)
    let mut bb = BatchBuilder::new(&schema);
    bb.begin_row(1u128, 1);
    bb.put_u64(200);
    bb.end_row();
    engine.ingest_to_family(tid, &bb.finish()).unwrap();
    engine.registry.checkpoint_base().unwrap();

    // Scan — should have exactly 1 row with val=200
    let scan = scan_all(&mut engine, tid);
    assert_eq!(scan.len(), 1);
    assert_eq!(scan.get_pk(0), 1);
    assert_eq!(payload_u64(&*scan, 0, 0), 200, "the later write must win the PK");

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
    assert!(engine.schema_id("app").is_some());

    engine
        .ddl_sync(gnitz_wire::SCHEMA_TAB, schema_tab_batch(&[(100, "synced", 1)]))
        .unwrap();

    // Hooks should have registered the schema
    assert!(engine.schema_id("synced").is_some());

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── the_newest_applied_zone_is_every_familys_replay_floor ──────────────

/// `close` also writes the next-id `_sequences` row outside any zone, which must
/// not move a floor.
#[test]
fn the_newest_applied_zone_is_every_familys_replay_floor() {
    let dir = temp_dir("catalog_zone_lsn");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
    let cols = vec![col_def("id", TypeCode::U64), col_def("val", TypeCode::U64)];

    let _ = engine.drain_pending_broadcasts();
    engine.create_schema("z").unwrap();
    engine.mark_zone_applied(5);
    let _ = engine.drain_pending_broadcasts();
    engine.create_table("z.t", &cols, &[0]).unwrap();
    engine.mark_zone_applied(7);
    let _ = engine.drain_pending_broadcasts();
    engine.create_table("z.t2", &cols, &[0]).unwrap();
    engine.mark_zone_applied(9);
    assert_eq!(engine.system_zone, 9);

    engine.close();
    let engine = CatalogEngine::open(&dir, 1).unwrap();
    let map = engine.registry.system_replay_floors();
    assert!(map.contains_key(&gnitz_wire::TABLE_TAB));
    assert!(map.keys().all(|&t| t < gnitz_wire::FIRST_USER_TABLE_ID));
    assert!(
        map.values().all(|&floor| floor == 9),
        "every family flushed zone 9: {map:?}"
    );
    assert_eq!(engine.system_zone, 9);

    drop(engine);
    let _ = fs::remove_dir_all(&dir);
}

// ── replayed_ddl_sync_group_is_not_replayed_after_a_flush ────────────

/// A crash between the boot flush and the SAL reset replays the tail again; the
/// flushed floor is what keeps a re-applied group from applying twice.
#[test]
fn replayed_ddl_sync_group_is_not_replayed_after_a_flush() {
    let dir = temp_dir("catalog_ddl_sync_pin");
    let mut unreplayed = CatalogEngine::open_master(&dir, 1, Default::default()).unwrap();

    unreplayed
        .stage(gnitz_wire::SCHEMA_TAB, 500, schema_tab_batch(&[(100, "synced", 1)]))
        .unwrap();

    unreplayed.replay().unwrap().close();
    let engine = CatalogEngine::open(&dir, 1).unwrap();
    let flushed = engine.registry.system_replay_floors()[&gnitz_wire::SCHEMA_TAB];
    assert_eq!(flushed, 500, "the flushed SCHEMA_TAB must dedup the group at lsn 500");
    assert!(engine.schema_id("synced").is_some());

    drop(engine);
    let _ = fs::remove_dir_all(&dir);
}

// ── test_master_holds_no_user_store ───────────────────────────────

/// The master registers every relation but opens only the system families'
/// stores.
#[test]
fn test_master_holds_no_user_store() {
    let dir = temp_dir("catalog_master_no_user_store");
    let mut engine = CatalogEngine::open_master(&dir, 1, Default::default())
        .unwrap()
        .replay()
        .unwrap();
    let cols = vec![col_def("id", TypeCode::U64), col_def("val", TypeCode::U64)];
    let tid = engine.create_table("public.t", &cols, &[0]).unwrap();

    let entry = engine.registry.relation_or_err(tid).unwrap();
    assert!(
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| entry.cursor())).is_err(),
        "the master holds no user store to read"
    );
    for family in [SysFamily::Schema, SysFamily::Table, SysFamily::Column] {
        assert!(
            engine.sys_relation(family).cursor().valid,
            "{} still reads its seed rows",
            family.name()
        );
    }

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── test_fk_index_metadata_queries ───────────────────────────────────

#[test]
fn test_fk_index_metadata_queries() {
    let dir = temp_dir("catalog_fk_idx_meta");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();

    let cols = vec![col_def("id", TypeCode::U64), col_def("val", TypeCode::U64)];
    let tid = engine.create_table("public.parent", &cols, &[0]).unwrap();

    // Create child table with FK to parent
    let child_cols = vec![
        col_def("id", TypeCode::U64),
        fk_def("parent_id", gnitz_wire::TypeCode::U64, tid, 0),
    ];
    let child_tid = engine.create_table("public.child", &child_cols, &[0]).unwrap();

    // FK count
    assert!(!engine.fk_constraints_of(child_tid).is_empty());
    let target_id = engine.fk_constraints_of(child_tid)[0].parent_tid;
    assert_eq!(target_id, tid);

    // Create an explicit index
    let _iid = engine.create_index("public.parent", &["val"], false).unwrap();

    assert!(!engine
        .registry
        .relation(tid)
        .map_or(&[][..], Relation::indexes)
        .is_empty());
    let ic_cols = engine.registry.relation(tid).map_or(&[][..], Relation::indexes)[0].cols();
    assert_eq!(ic_cols.as_slice(), [1]); // val is column 1

    // The index circuit resolves, so its store is reachable.
    assert!(engine.registry.relation(tid).and_then(|r| r.index_on(&[1])).is_some());

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_dep_map_view_on_view_chain() {
    // A registered view's edges reach the map, and boot registers them again.
    let dir = temp_dir("dep_map_view_chain");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
    let cols = vec![col_def("id", TypeCode::U64)];
    let tid = engine.create_table("public.t", &cols, &[0]).unwrap();

    let v1 = register_identity_view(&mut engine, tid, "v1", &cols);
    let v2 = register_identity_view(&mut engine, v1, "v2", &cols);
    assert_eq!(engine.dag.dependents_of(v1), [v2]);
    engine.close();

    let engine = CatalogEngine::open(&dir, 1).unwrap();
    assert_eq!(engine.dag.dependents_of(tid), [v1]);
    assert_eq!(engine.dag.dependents_of(v1), [v2]);
    assert_eq!(engine.dag.sources_of(v2), [v1]);
    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

/// A retired view contributes no edges: DROP VIEW and the ALTER VIEW
/// `replaces` path both unregister the outgoing view, and the map forgets it.
#[test]
fn test_dep_map_drops_a_retired_views_edges() {
    let dir = temp_dir("dep_map_retired");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();

    let cols = vec![col_def("id", TypeCode::U64)];
    let tid = engine.create_table("public.t", &cols, &[0]).unwrap();

    let v1 = register_identity_view(&mut engine, tid, "v1", &cols);
    assert_eq!(engine.dag.dependents_of(tid), &[v1][..]);

    // ALTER VIEW: one VIEW_TAB batch retiring v1 and registering v2.
    let v2 = engine.allocate_ids(1).unwrap();
    write_identity_circuit(&mut engine, v2, tid, gnitz_wire::ReadBound::None);
    engine.write_column_records(v2, &cols).unwrap();
    let mut bb = BatchBuilder::new(SysFamily::View.schema());
    push_view_tab_row(&mut bb, -1, v1, "v1", 0, 0, 0);
    push_view_tab_row(&mut bb, 1, v2, "v1", 0, 0, 0);
    engine.ingest_to_family(gnitz_wire::VIEW_TAB, &bb.finish()).unwrap();
    assert_eq!(
        engine.dag.dependents_of(tid),
        &[v2][..],
        "the replaced view's edge is gone, the replacement's is present"
    );

    // DROP VIEW retires the last edge, so the base table is a dep-map orphan.
    engine.drop_view("public.v1").unwrap();
    assert!(engine.dag.dependents_of(tid).is_empty());

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

/// The two dependent-view RESTRICTs — DROP TABLE and DROP COLUMN — both read
/// the dependency map.
#[test]
fn test_dependent_view_restricts_fire_from_registered_views() {
    let dir = temp_dir("dep_map_restrict");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();

    let cols = vec![col_def("id", TypeCode::U64), col_def("val", TypeCode::U64)];
    let tid = engine.create_table("public.t", &cols, &[0]).unwrap();

    register_identity_view(&mut engine, tid, "v", &cols);

    let err = engine.drop_table("public.t").unwrap_err();
    assert!(err.contains("View dependency"), "DROP TABLE RESTRICT: {err}");

    // DROP NOT NULL on `val` — an `is_nullable 0→1` rewrite pair.
    let mut nullable = cols[1].clone();
    nullable.def.is_nullable = true;
    let mut bb = BatchBuilder::new(SysFamily::Column.schema());
    cols[1].write_col_tab_row(&mut bb, tid, 1, -1);
    nullable.write_col_tab_row(&mut bb, tid, 1, 1);
    let err = engine.precheck_family(SysFamily::Column, &bb.finish()).unwrap_err();
    assert!(err.contains("dependent views"), "DROP NOT NULL RESTRICT: {err}");

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_circuit_table_surface_introspectable() {
    let dir = temp_dir("circuit_surface");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();

    // Inject a row directly into CIRCUIT_TAB so the store is non-empty: view 107,
    // with a single node, through the shared row codec.
    let mut circuit = gnitz_wire::Circuit::default();
    circuit.input_delta(100, gnitz_wire::ReadBound::None);
    write_circuit(&mut engine, 107, circuit);

    // The family is SQL-introspectable — `SELECT * FROM _circuits` must return
    // what we just inserted (full-scan path, used by SQL planner).
    let scan = scan_all(&mut engine, gnitz_wire::CIRCUIT_TAB);
    assert_eq!(scan.len(), 1, "scan must expose the circuit row");

    // A point read by the view id.
    let pk_bytes = opk_pk(SysFamily::Circuit.schema(), &[107]);
    let found = pk_group(&mut engine, gnitz_wire::CIRCUIT_TAB, &pk_bytes);
    assert_eq!(found.len(), 1, "the circuit row by PK");

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── The name indexes ────────────────────────────────────────────────────────

/// A rename hands the relation's id from the old name to the new one, and a
/// schema's entry goes with its last relation.
#[test]
fn the_relation_name_index_follows_renames_and_drops() {
    let dir = temp_dir("relation_name_index");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
    engine.create_schema("s").unwrap();
    let sid = engine.schema_id("s").unwrap();
    let cols = vec![col_def("id", TypeCode::U64)];
    let tid = engine.create_table("public.orig", &cols, &[0]).unwrap();
    let member = engine.create_table("s.only", &cols, &[0]).unwrap();
    assert_eq!(engine.relation_id(sid, "only"), Some(member));
    assert_eq!(engine.relation_id(PUBLIC_SCHEMA_ID, "only"), None);

    let pair = table_tab_batch(&[(tid, "orig", -1), (tid, "renamed", 1)]);
    engine.submit(SysFamily::Table, pair).unwrap();
    assert_eq!(engine.relation_id(PUBLIC_SCHEMA_ID, "orig"), None);
    assert_eq!(engine.relation_id(PUBLIC_SCHEMA_ID, "renamed"), Some(tid));

    engine.drop_table("s.only").unwrap();
    assert!(!engine.caches.relation_by_name.contains_key(&sid));
    assert!(engine.schema_is_empty("s"));

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

/// A relation's descriptor token changes exactly when a RESOLVE of its name
/// would answer differently: an index, a rename and a drop each move it, and DDL
/// on another relation or an advance of its own sequence do not.
#[test]
fn a_descriptor_token_follows_the_resolve_answer() {
    let dir = temp_dir("descriptor_token");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
    let cols = vec![col_def("id", TypeCode::U64), col_def("a", TypeCode::I64)];
    let tid = engine.create_table("public.t", &cols, &[0]).unwrap();
    let token = engine.resolve_token(tid).unwrap();
    assert_ne!(token, 0);
    engine.check_token(Target { tid, token }).unwrap();
    engine.check_token(tid.into()).unwrap();

    let other = engine.create_table("public.other", &cols, &[0]).unwrap();
    engine.create_index("public.other", &["a"], false).unwrap();
    assert_eq!(engine.resolve_token(tid), Some(token));
    assert_ne!(engine.resolve_token(other), Some(token));

    let serial = engine.create_serial_table("public.sr").unwrap();
    let before = engine.resolve_token(serial);
    let (_, reserved) = engine.reserve_user_sequence(serial, 64).unwrap();
    engine.submit(SysFamily::Sequence, reserved).unwrap();
    assert_eq!(engine.resolve_token(serial), before);
    assert_eq!(engine.resolve_token(tid), Some(token));

    let stale = |engine: &CatalogEngine, token| {
        let fault = engine.check_token(Target { tid, token }).unwrap_err();
        assert_eq!(fault.status, gnitz_wire::WireStatus::StaleCatalog);
        engine.resolve_token(tid)
    };
    let pair = table_tab_batch(&[(tid, "t", -1), (tid, "renamed", 1)]);
    engine.submit(SysFamily::Table, pair).unwrap();
    let renamed = stale(&engine, token).unwrap();
    engine.create_index("public.renamed", &["id"], false).unwrap();
    let indexed = stale(&engine, renamed).unwrap();
    engine.drop_table("public.renamed").unwrap();
    assert_eq!(stale(&engine, indexed), None);

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

/// An index name is taken by a live index and by an earlier row of the same batch,
/// and free again once its index is dropped.
#[test]
fn an_index_name_is_claimed_once() {
    let cols = vec![
        col_def("id", TypeCode::U64),
        col_def("a", TypeCode::I64),
        col_def("b", TypeCode::I64),
    ];
    let (mut engine, tid, dir) = table_fixture("index_name_claimed_once", &cols);
    let index = |engine: &mut CatalogEngine, col: u32, name: &str| {
        let id = engine.allocate_ids(1).unwrap();
        idx_tab_batch(id, tid, &[col], name, false, 1)
    };

    let live = index(&mut engine, 1, "ix");
    engine.submit(SysFamily::Index, live).unwrap();
    let second = index(&mut engine, 2, "ix");
    let err = engine.precheck_family(SysFamily::Index, &second).unwrap_err();
    assert_eq!(err, "Index already exists: ix");

    let mut twice = index(&mut engine, 2, "other");
    twice.append_batch(&index(&mut engine, 1, "other"));
    let err = engine.precheck_family(SysFamily::Index, &twice).unwrap_err();
    assert_eq!(err, "Index already exists: other");

    engine.drop_index("ix").unwrap();
    engine
        .precheck_family(SysFamily::Index, &second)
        .expect("a dropped index frees its name");

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}
