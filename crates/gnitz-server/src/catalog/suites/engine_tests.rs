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
    let cols = vec![col_def("id", TypeCode::U64), col_def("val", TypeCode::U64)];
    let tid = engine.create_table("public.t", &cols, &[0]).unwrap();
    let schema = engine.registry.relation(tid).map(Relation::schema).unwrap();

    let make_row = |pk: u64, val: u64, w: i64| -> Batch {
        let mut bb = BatchBuilder::new(schema);
        bb.begin_row(pk as u128, w);
        bb.put_u64(val);
        bb.end_row();
        bb.finish()
    };
    let live = |engine: &mut CatalogEngine| -> usize {
        engine.registry.checkpoint_base().unwrap();
        engine.scan(tid).unwrap().len()
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
            .registry
            .ingest(
                SysFamily::Index.id(),
                idx_tab_batch(
                    888,
                    99999,
                    1,
                    "orphaned_idx",
                    gnitz_wire::IndexProps { is_unique: false },
                    1,
                ),
            )
            .unwrap();
        let _ = engine.registry.checkpoint_system(engine.system_zone);
        engine.close();
    }

    // Re-open should fail because the orphaned index references table 99999
    let result = CatalogEngine::open(&dir, 1);
    assert!(result.is_err(), "Orphaned index metadata should cause error on reload");

    let _ = fs::remove_dir_all(&dir);
}

// ── user-table SERIAL sequences ──────────────────────────────────────

#[test]
fn test_reserve_user_sequence_seed_and_contiguous() {
    let dir = temp_dir("reserve_user_seq");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
    let seq_id = engine
        .create_table("public.t", &[col_def("id", TypeCode::U64)], &[0])
        .unwrap();

    let (base1, delta1) = engine.reserve_user_sequence(seq_id, 64).unwrap();
    assert_eq!(base1, 1);
    assert_eq!(delta1.len(), 1, "first use inserts, retracts nothing");
    assert_eq!(delta1.get_weight(0), 1);
    engine.ingest_to_family(SEQ_TAB_ID, &delta1).unwrap();
    assert_eq!(engine.sequence_value(seq_id), Some(64));

    let (base2, delta2) = engine.reserve_user_sequence(seq_id, 64).unwrap();
    assert_eq!(base2, 65);
    assert_eq!(delta2.len(), 2);
    assert_eq!(delta2.get_weight(0), -1);
    assert_eq!(payload_u64(&delta2, 0, 0), 64, "the -1 must carry the live high-water");
    assert_eq!(delta2.get_weight(1), 1);
    assert_eq!(payload_u64(&delta2, 1, 0), 128);
    engine.ingest_to_family(SEQ_TAB_ID, &delta2).unwrap();
    assert_eq!(engine.sequence_value(seq_id), Some(128));

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_reserve_user_sequence_rejects_exhausted_range() {
    let dir = temp_dir("reserve_user_seq_exhausted");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();

    let cols = [col_def("id", TypeCode::U64)];
    let full = engine.create_table("public.full", &cols, &[0]).unwrap();
    engine
        .ingest_to_family(SEQ_TAB_ID, &engine.sequence_delta(full, (i64::MAX - 1) as u64))
        .unwrap();
    let err = engine.reserve_user_sequence(full, 64).err().unwrap();
    assert!(err.contains("invalid or exhausted"), "{err}");
    engine.reserve_user_sequence(full, 0).err().unwrap();
    let edge = engine.create_table("public.edge", &cols, &[0]).unwrap();
    engine.reserve_user_sequence(edge, 1 << 63).err().unwrap();
    engine.reserve_user_sequence(edge + 1000, 1).err().unwrap();

    engine
        .ingest_to_family(SEQ_TAB_ID, &engine.sequence_delta(edge, (i64::MAX - 65) as u64))
        .unwrap();
    let (base, delta) = engine.reserve_user_sequence(edge, 64).unwrap();
    assert_eq!(base, i64::MAX - 64);
    assert_eq!(payload_u64(&delta, 1, 0), (i64::MAX - 1) as u64, "last = i64::MAX - 1");

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_user_sequence_durable_roundtrip() {
    let dir = temp_dir("user_seq_roundtrip");
    let user_seq;
    {
        let mut engine = CatalogEngine::open(&dir, 1).unwrap();
        user_seq = engine
            .create_table("public.t", &[col_def("id", TypeCode::U64)], &[0])
            .unwrap();
        let (base, delta) = engine.reserve_user_sequence(user_seq, 64).unwrap();
        assert_eq!(base, 1);
        engine.ingest_to_family(SEQ_TAB_ID, &delta).unwrap();
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
    let expected_topology = crate::catalog::registry::topology_word(4);
    {
        let mut engine = CatalogEngine::open(&dir, 1).unwrap();
        assert_eq!(engine.durable_generation(), 0);
        assert_eq!(engine.sequence_value(SEQ_ID_TOPOLOGY).unwrap_or(0), 0);
        engine.record_topology(4).unwrap();
        assert_eq!(engine.sequence_value(SEQ_ID_TOPOLOGY).unwrap_or(0), expected_topology);
        assert_eq!(engine.bump_checkpoint_generation().unwrap(), 1);
        assert_eq!(engine.bump_checkpoint_generation().unwrap(), 2);
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
        assert_eq!(engine.bump_checkpoint_generation().unwrap(), 1);
        assert_eq!(engine.bump_checkpoint_generation().unwrap(), 2);
        engine.close();
    }
    {
        let mut engine = CatalogEngine::open(&dir, 1).unwrap();
        assert_eq!(engine.durable_generation(), 2);
        assert_eq!(engine.advance_durable_generation().unwrap(), 3);
        assert_eq!(engine.registry.resume_generation(), 2);
        assert_eq!(engine.sequence_value(SEQ_ID_CHECKPOINT_GEN), Some(3));
        assert_eq!(engine.bump_checkpoint_generation().unwrap(), 4);
        assert_eq!(engine.registry.resume_generation(), 4);
        engine.close();
    }
    let engine = CatalogEngine::open(&dir, 1).unwrap();
    assert_eq!(engine.durable_generation(), 4);
    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

/// A drop, a system flush and a reopen must not hand out again an id that was
/// registered without being allocated.
#[test]
fn test_raised_counter_survives_drop_and_flush() {
    let dir = temp_dir("raised_counter_survives_drop");
    let cols = vec![col_def("id", TypeCode::U64)];
    let tid;
    {
        let mut engine = CatalogEngine::open(&dir, 1).unwrap();
        tid = engine.next_id + 500;
        engine.write_column_records(tid, &cols).unwrap();
        engine
            .submit(
                SysFamily::Table,
                build_table_tab_row(tid, pack_pk_cols(&[0]), "unallocated"),
            )
            .unwrap();
        assert!(engine.next_id > tid);
        engine.submit_retraction(SysFamily::Table, tid).unwrap();
        engine.flush_all_system_tables().unwrap();
        engine.close();
    }
    let engine = CatalogEngine::open(&dir, 1).unwrap();
    assert!(!engine.registry.has_id(tid));
    assert!(engine.next_id > tid, "next_id {} must stay past {tid}", engine.next_id);
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
            .ingest(
                SysFamily::Table.id(),
                build_table_tab_row(250, pack_pk_cols(&[0]), "gap_table"),
            )
            .unwrap();

        // Inject column record for tid=250
        let mut cbb = BatchBuilder::new(*SysFamily::Column.schema());
        write_col_tab_row(&mut cbb, &col_def("id", TypeCode::U64).col_tab_row(250, 0), 1);
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
    engine.registry.checkpoint_base().unwrap();

    // Scan
    let scan_batch = engine.scan(tid).unwrap();
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
        pk_group_native(&mut engine, TABLE_TAB_ID, tid as u128).len(),
        1,
        "the TABLE_TAB row of the created table"
    );
    assert!(pk_group_native(&mut engine, TABLE_TAB_ID, 9_999_999).is_empty());

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
    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(1u128, 1);
    bb.put_u64(100);
    bb.end_row();
    engine.ingest_to_family(tid, &bb.finish()).unwrap();
    engine.registry.checkpoint_base().unwrap();

    // Insert row with PK=1 again, val=200 (should retract old + insert new)
    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(1u128, 1);
    bb.put_u64(200);
    bb.end_row();
    engine.ingest_to_family(tid, &bb.finish()).unwrap();
    engine.registry.checkpoint_base().unwrap();

    // Scan — should have exactly 1 row with val=200
    let scan = engine.scan(tid).unwrap();
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
    assert!(engine.has_schema("app"));

    // Simulate DDL sync: create batch mimicking a schema record
    let schema = SysFamily::Schema.schema();
    let mut bb = BatchBuilder::new(*schema);
    bb.begin_row(100u128, 1); // sid=100
    bb.put_string("synced");
    bb.end_row();
    engine.ddl_sync(SCHEMA_TAB_ID, bb.finish()).unwrap();

    // Hooks should have registered the schema
    assert!(engine.has_schema("synced"));

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── the_newest_applied_zone_is_every_familys_replay_floor ──────────────

/// `close` also writes the next-id `_sequences` row outside any zone, which must
/// not move a floor.
#[test]
fn the_newest_applied_zone_is_every_familys_replay_floor() {
    use crate::catalog::sys_tables::TABLE_TAB_ID;

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
    assert!(map.contains_key(&TABLE_TAB_ID));
    assert!(map.keys().all(|&t| t < FIRST_USER_TABLE_ID));
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
    let mut unreplayed = CatalogEngine::open_master(&dir, 1).unwrap();

    let mut bb = BatchBuilder::new(*SysFamily::Schema.schema());
    bb.begin_row(100u128, 1);
    bb.put_string("synced");
    bb.end_row();
    unreplayed.stage(SCHEMA_TAB_ID, 500, bb.finish()).unwrap();

    unreplayed.replay().unwrap().close();
    let engine = CatalogEngine::open(&dir, 1).unwrap();
    let flushed = engine.registry.system_replay_floors()[&SCHEMA_TAB_ID];
    assert_eq!(flushed, 500, "the flushed SCHEMA_TAB must dedup the group at lsn 500");
    assert!(engine.has_schema("synced"));

    drop(engine);
    let _ = fs::remove_dir_all(&dir);
}

// ── test_master_holds_no_user_store ───────────────────────────────

/// The master registers every relation but opens only the system families'
/// stores.
#[test]
fn test_master_holds_no_user_store() {
    let dir = temp_dir("catalog_master_no_user_store");
    let mut engine = CatalogEngine::open_master(&dir, 1).unwrap().replay().unwrap();
    let cols = vec![col_def("id", TypeCode::U64), col_def("val", TypeCode::U64)];
    let tid = engine.create_table("public.t", &cols, &[0]).unwrap();

    assert!(!engine.registry.residency().owns_stores());
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
fn test_dep_map_is_the_scan_delta_nodes() {
    // The dep map is derived from the circuit's `ScanDelta` nodes: the view id
    // off the compound PK region, the source off the `source_table` column. Two
    // distinct sources give two edges; a repeated source gives one; and a
    // non-`ScanDelta` node carrying a `source_table` gives none.
    let dir = temp_dir("dep_map_scan_delta");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();

    // Raw rows, not a `Circuit`: a sink carrying a `source_table` is a shape no
    // circuit encodes to.
    let mut bb = BatchBuilder::new(*SysFamily::CircuitNodes.schema());
    for (node_id, (opcode, source)) in [
        (gnitz_wire::Opcode::ScanDelta, 100),
        (gnitz_wire::Opcode::ScanDelta, 200),
        (gnitz_wire::Opcode::ScanDelta, 100),
        (gnitz_wire::Opcode::IntegrateSink, 300),
    ]
    .into_iter()
    .enumerate()
    {
        gnitz_wire::sys_rows::write_circuit_node_row(
            &mut bb,
            &gnitz_wire::sys_rows::CircuitNodeRow {
                view_id: 400,
                node_id: node_id as u64,
                opcode: opcode.as_wire(),
                source_table: Some(source),
                inputs: [None; 2],
                params: None,
            },
            1,
        );
    }
    engine.submit(SysFamily::CircuitNodes, bb.finish()).unwrap();

    assert_eq!(
        engine.dag.dependents_of(100),
        &[400i64][..],
        "the repeated source yields one edge"
    );
    assert_eq!(
        engine.dag.dependents_of(200),
        &[400i64][..],
        "table 200 must feed view 400"
    );
    assert!(
        engine.dag.dependents_of(300).is_empty(),
        "a non-ScanDelta node contributes no edge"
    );

    let mut sources = engine.dag.sources_of(400).to_vec();
    sources.sort_unstable();
    assert_eq!(sources, vec![100, 200], "both ScanDelta sources of view 400");

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_dep_map_view_on_view_chain() {
    // A view over a view: each segment's own `ScanDelta` is its edge, in both
    // map directions.
    let dir = temp_dir("dep_map_view_chain");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();

    write_identity_circuit(&mut engine, 107, 100, gnitz_wire::ReadBound::None);
    write_identity_circuit(&mut engine, 108, 107, gnitz_wire::ReadBound::None);

    assert_eq!(engine.dag.dependents_of(100), &[107i64][..], "base 100 feeds view 107");
    assert_eq!(engine.dag.dependents_of(107), &[108i64][..], "view 107 feeds view 108");
    assert_eq!(engine.dag.sources_of(107), &[100i64][..]);
    assert_eq!(engine.dag.sources_of(108), &[107i64][..]);
    engine.close();

    // Boot replays the circuit rows into the map.
    let engine = CatalogEngine::open(&dir, 1).unwrap();
    assert_eq!(engine.dag.sources_of(107), &[100i64][..]);
    assert_eq!(engine.dag.sources_of(108), &[107i64][..]);
    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

/// A retired view contributes no edges: DROP VIEW and the ALTER VIEW
/// `replaces` path both retract the outgoing view's circuit rows, and the map
/// forgets the view with them.
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
    let mut bb = BatchBuilder::new(*SysFamily::View.schema());
    push_view_tab_row(&mut bb, -1, v1, "v1", 0, 0, 0);
    push_view_tab_row(&mut bb, 1, v2, "v1", 0, 0, 0);
    engine.ingest_to_family(VIEW_TAB_ID, &bb.finish()).unwrap();
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
/// the CIRCUIT_NODES-backed map.
#[test]
fn test_dependent_view_restricts_fire_from_circuit_rows() {
    let dir = temp_dir("dep_map_restrict");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();

    let cols = vec![col_def("id", TypeCode::U64), col_def("val", TypeCode::U64)];
    let tid = engine.create_table("public.t", &cols, &[0]).unwrap();

    register_identity_view(&mut engine, tid, "v", &cols);

    let err = engine.drop_table("public.t").unwrap_err();
    assert!(err.contains("View dependency"), "DROP TABLE RESTRICT: {err}");

    // DROP NOT NULL on `val` — an `is_nullable 0→1` rewrite pair.
    let mut nullable = cols[1].clone();
    nullable.is_nullable = true;
    let mut bb = BatchBuilder::new(*SysFamily::Column.schema());
    write_col_tab_row(&mut bb, &cols[1].col_tab_row(tid, 1), -1);
    write_col_tab_row(&mut bb, &nullable.col_tab_row(tid, 1), 1);
    let err = engine.precheck_family(SysFamily::Column, &bb.finish()).unwrap_err();
    assert!(err.contains("dependent views"), "DROP NOT NULL RESTRICT: {err}");

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_circuit_table_surface_introspectable() {
    let dir = temp_dir("circuit_surface");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();

    // Inject a row directly into CIRCUIT_NODES so the store is non-empty: view 107,
    // with a single node, through the shared row codec.
    let mut circuit = gnitz_wire::Circuit::default();
    circuit.input_delta(100, gnitz_wire::ReadBound::None);
    write_circuit(&mut engine, 107, circuit);

    // The new schema is SQL-introspectable — `SELECT * FROM CircuitNodes`
    // must return what we just inserted (full-scan path, used by SQL planner).
    let scan = engine.scan(CIRCUIT_NODES_TAB_ID).unwrap();
    assert_eq!(scan.len(), 1, "scan must expose CircuitNodes rows");

    // Compound PK: a point read by the 16-byte at-rest `(view_id, node_id)` OPK region.
    let pk_bytes = opk_pk(SysFamily::CircuitNodes.schema(), &[107, 0]);
    let found = pk_group(&mut engine, CIRCUIT_NODES_TAB_ID, &pk_bytes);
    assert_eq!(found.len(), 1, "the CircuitNodes row by PK");

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}
