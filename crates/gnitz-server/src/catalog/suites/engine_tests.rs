use super::*;
use crate::test_support::table_blocks;
use gnitz_wire::Target;

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
    let mut engine = fresh("reserve_user_seq");
    let seq_id = engine.create_serial_table("public.t").unwrap();

    let (base1, delta1) = reserve(&mut engine, seq_id, 64).unwrap();
    assert_eq!(base1, 1);
    assert_eq!(delta1.len(), 1, "first use inserts, retracts nothing");
    assert_eq!(delta1.get_weight(0), 1);
    assert_eq!(engine.sequence_value(seq_id), Some(64));

    let (base2, delta2) = reserve(&mut engine, seq_id, 64).unwrap();
    assert_eq!(base2, 65);
    assert_eq!(delta2.len(), 2);
    assert_eq!(delta2.get_weight(0), -1);
    assert_eq!(payload_u64(&delta2, 0, 0), 64, "the -1 must carry the live high-water");
    assert_eq!(delta2.get_weight(1), 1);
    assert_eq!(payload_u64(&delta2, 1, 0), 128);
    assert_eq!(engine.sequence_value(seq_id), Some(128));

    finish(engine);
}

#[test]
fn test_reserve_user_sequence_rejects_exhausted_range() {
    let mut engine = fresh("reserve_user_seq_exhausted");

    // A run whose last id would be i64::MAX, the ceiling.
    let full = engine.create_serial_table("public.full").unwrap();
    reserve(&mut engine, full, (i64::MAX - 64) as u64).unwrap();
    let err = reserve(&mut engine, full, 64).err().unwrap();
    assert!(err.contains("invalid or exhausted"), "{err}");
    let edge = engine.create_serial_table("public.edge").unwrap();
    reserve(&mut engine, edge, 0).err().unwrap();
    reserve(&mut engine, edge, 1 << 63).err().unwrap();
    reserve(&mut engine, edge + 1000, 1).err().unwrap();
    assert_eq!(engine.sequence_value(edge), None, "a refused reservation moves nothing");

    reserve(&mut engine, edge, (i64::MAX - 65) as u64).unwrap();
    let (base, delta) = reserve(&mut engine, edge, 64).unwrap();
    assert_eq!(base, (i64::MAX - 64) as u64);
    assert_eq!(payload_u64(&delta, 1, 0), (i64::MAX - 1) as u64, "last = i64::MAX - 1");

    finish(engine);
}

/// `count` arrives raw off the wire.
#[test]
fn allocate_ids_hands_out_contiguous_runs_under_the_ceiling() {
    let mut engine = fresh("alloc_ids");
    let a = engine.allocate_ids(3).unwrap();
    assert_eq!(engine.allocate_ids(1).unwrap(), a + 3);
    for count in [0, gnitz_wire::CATALOG_ID_CEILING, u64::MAX] {
        engine.allocate_ids(count).unwrap_err();
    }
    assert_eq!(engine.allocate_ids(1).unwrap(), a + 4, "a refused run consumes nothing");
    // The last id below the ceiling is the last one handed out.
    let rest = gnitz_wire::CATALOG_ID_CEILING - engine.next_id;
    engine.allocate_ids(rest + 1).unwrap_err();
    assert_eq!(engine.allocate_ids(rest).unwrap(), a + 5);
    finish(engine);
}

/// An id enters the catalog only through `allocate_ids`. A `+1` row at one the
/// counter has not reached is refused before anything applies, so it cannot
/// raise the counter to the ceiling and exhaust every later allocation.
#[test]
fn a_row_at_an_unallocated_id_is_refused_and_leaves_allocation_working() {
    let mut engine = fresh("unallocated_id");
    let next = engine.allocate_ids(1).unwrap() + 1;
    for id in [next, next + 4096, gnitz_wire::CATALOG_ID_CEILING - 1] {
        let err = apply_ddl(
            &mut engine,
            [(SysFamily::Schema, schema_tab_batch(&[(id, "forged", 1)]))],
        )
        .unwrap_err();
        assert!(err.contains("never allocated"), "schema {id}: {err}");
    }
    assert_eq!(engine.allocate_ids(1).unwrap(), next);
    apply_ddl(
        &mut engine,
        [(SysFamily::Schema, schema_tab_batch(&[(next, "granted", 1)]))],
    )
    .unwrap();
    finish(engine);
}

#[test]
fn test_user_sequence_durable_roundtrip() {
    let dir = temp_dir("user_seq_roundtrip");
    let user_seq;
    {
        let mut engine = CatalogEngine::open(&dir, 1).unwrap();
        user_seq = engine.create_serial_table("public.t").unwrap();
        let (base, _) = reserve(&mut engine, user_seq, 64).unwrap();
        assert_eq!(base, 1);
        assert_eq!(engine.sequence_value(user_seq), Some(64));
        engine.close();
    }
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
    assert_eq!(engine.sequence_value(user_seq), Some(64));
    let (base2, _delta) = reserve(&mut engine, user_seq, 64).unwrap();
    assert_eq!(base2, 65, "next id continues after the recovered high-water");
    finish(engine);
}

#[test]
fn test_boot_generation_advance_monotonic() {
    let dir = temp_dir("recovery_start_gen_bump");
    {
        let mut engine = CatalogEngine::open(&dir, 1).unwrap();
        engine.record_topology(1).unwrap();
        assert_eq!(engine.advance_durable_generation().unwrap(), 1);
        assert_eq!(engine.advance_durable_generation().unwrap(), 2);
        engine.close();
    }
    {
        let mut engine = CatalogEngine::open(&dir, 1).unwrap();
        assert_eq!(engine.durable_generation(), 2);
        assert_eq!(engine.advance_durable_generation().unwrap(), 3);
        assert_eq!(engine.resume_at, Some(2));
        assert_eq!(engine.advance_durable_generation().unwrap(), 4);
        engine.close();
    }
    let engine = CatalogEngine::open(&dir, 1).unwrap();
    assert_eq!(engine.durable_generation(), 4);
    finish(engine);
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
    assert!(engine.registry.relation(tid).is_none());
    assert!(engine.allocate_ids(1).unwrap() > tid);
    finish(engine);
}

/// A DDL may name an id a dropped relation held; the relation it registers
/// there has a registration of its own.
#[test]
fn a_relation_created_again_under_its_id_is_a_new_registration() {
    let dir = temp_dir("recreated_registration");
    let cols = vec![col_def("id", TypeCode::U64)];
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
    let sid = engine.schema_id("public").unwrap();
    let tid = engine.create_table("public.again", &cols, &[0]).unwrap();
    let first = engine.registration(tid).expect("registered");
    engine.submit_retraction(SysFamily::Table, tid).unwrap();
    assert_eq!(engine.registration(tid), None);

    apply_ddl(
        &mut engine,
        table_blocks(tid, sid, "again", &cols, &[0], Default::default()),
    )
    .unwrap();
    let again = engine.registration(tid).expect("registered again");
    assert_ne!(again, first);
    finish(engine);
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
        let columns = col_tab_batch(250, &[col_def("id", TypeCode::U64)], 1);
        engine.registry.ingest(SysFamily::Column.id(), columns).unwrap();

        engine.close();
    }

    // Re-open: sequence should recover to 251
    {
        let mut engine = CatalogEngine::open(&dir, 1).unwrap();
        let new_tid = engine.create_table("public.tnext", &cols, &[0]).unwrap();
        assert_eq!(new_tid, 251, "Sequence recovery: expected 251, got {new_tid}");
        finish(engine);
    }
}

// ── test_ingest_scan_seek_family ──────────────────────────────────────

#[test]
fn test_ingest_scan_seek_family() {
    let mut engine = fresh("catalog_ingest_scan_seek");
    let cols = vec![col_def("id", TypeCode::U64), col_def("val", TypeCode::U64)];
    let tid = engine.create_table("public.t", &cols, &[0]).unwrap();

    // Ingest via CatalogEngine (user table path)
    engine
        .registry
        .ingest(tid, rows(&engine, tid, 1, [1, 2, 3], |i| [i * 100]))
        .unwrap();
    engine.registry.checkpoint_base().unwrap();

    // Scan
    let scan_batch = scan_all(&mut engine, tid);
    assert_eq!(scan_batch.len(), 3);

    // Point read, present
    let row = pk_group_native(&mut engine, tid, 2);
    assert_eq!(row.len(), 1);
    assert_eq!(gnitz_wire::widen_pk_be(row.get_pk_bytes(0)), 2);

    // Point read, missing
    assert!(pk_group_native(&mut engine, tid, 99).is_empty());

    finish(engine);
}

// ── a point read resolves a system-table row ───────────

/// A system family answers a point read like any relation: `create_table` writes a
/// TABLE_TAB row keyed by the new table id.
#[test]
fn point_read_resolves_system_table_row() {
    let mut engine = fresh("catalog_seek_system_table");
    let cols = vec![col_def("id", TypeCode::U64), col_def("val", TypeCode::U64)];
    let tid = engine.create_table("public.t", &cols, &[0]).unwrap();

    assert_eq!(
        pk_group_native(&mut engine, gnitz_wire::TABLE_TAB, tid as u128).len(),
        1,
        "the TABLE_TAB row of the created table"
    );
    assert!(pk_group_native(&mut engine, gnitz_wire::TABLE_TAB, 9_999_999).is_empty());

    finish(engine);
}

// ── test_ingest_pk_enforced_through_the_store ───────────────────────────────

#[test]
fn test_ingest_pk_enforced_through_the_store() {
    let mut engine = fresh("catalog_pk_enforced_store");
    let cols = vec![col_def("id", TypeCode::U64), col_def("val", TypeCode::U64)];
    let tid = engine.create_table("public.t", &cols, &[0]).unwrap();

    // Insert row with PK=1, val=100
    engine
        .registry
        .ingest(tid, rows(&engine, tid, 1, [1], |_| [100]))
        .unwrap();
    engine.registry.checkpoint_base().unwrap();

    // Insert row with PK=1 again, val=200 (should retract old + insert new)
    engine
        .registry
        .ingest(tid, rows(&engine, tid, 1, [1], |_| [200]))
        .unwrap();
    engine.registry.checkpoint_base().unwrap();

    // Scan — should have exactly 1 row with val=200
    let scan = scan_all(&mut engine, tid);
    assert_eq!(scan.len(), 1);
    assert_eq!(gnitz_wire::widen_pk_be(scan.get_pk_bytes(0)), 1);
    assert_eq!(payload_u64(&*scan, 0, 0), 200, "the later write must win the PK");

    finish(engine);
}

// ── test_ddl_sync ─────────────────────────────────────────────────────

#[test]
fn test_ddl_sync() {
    let mut engine = fresh("catalog_ddl_sync");

    // Create a schema via normal DDL
    engine.create_schema("app").unwrap();
    assert!(engine.schema_id("app").is_some());

    engine
        .ddl_sync(SysFamily::Schema, schema_tab_batch(&[(100, "synced", 1)]))
        .unwrap();

    // Hooks should have registered the schema
    assert!(engine.schema_id("synced").is_some());

    finish(engine);
}

// ── the_newest_applied_zone_is_every_familys_replay_floor ──────────────

/// `close` also writes the next-id `sequences` row outside any zone, which must
/// not move a floor.
#[test]
fn the_newest_applied_zone_is_every_familys_replay_floor() {
    let dir = temp_dir("catalog_zone_lsn");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
    let cols = vec![col_def("id", TypeCode::U64), col_def("val", TypeCode::U64)];

    engine.create_schema("z").unwrap();
    engine.mark_zone_applied(5);
    engine.create_table("z.t", &cols, &[0]).unwrap();
    engine.mark_zone_applied(7);
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

    discard(engine);
}

// ── replayed_ddl_sync_group_is_not_replayed_after_a_flush ────────────

/// A crash between the boot flush and the SAL reset replays the tail again; the
/// flushed floor is what keeps a re-applied group from applying twice.
#[test]
fn replayed_ddl_sync_group_is_not_replayed_after_a_flush() {
    let dir = temp_dir("catalog_ddl_sync_pin");
    let mut unreplayed = CatalogEngine::open_master(&dir, 1, Default::default()).unwrap();

    unreplayed
        .stage(SysFamily::Schema, 500, schema_tab_batch(&[(100, "synced", 1)]))
        .unwrap();

    unreplayed.replay().unwrap().close();
    let engine = CatalogEngine::open(&dir, 1).unwrap();
    let flushed = engine.registry.system_replay_floors()[&gnitz_wire::SCHEMA_TAB];
    assert_eq!(flushed, 500, "the flushed SCHEMA_TAB must dedup the group at lsn 500");
    assert!(engine.schema_id("synced").is_some());

    discard(engine);
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
            engine.sys_relation(family).cursor().is_valid(),
            "{} still reads its seed rows",
            family.name()
        );
    }

    finish(engine);
}

// ── test_fk_index_metadata_queries ───────────────────────────────────

#[test]
fn test_fk_index_metadata_queries() {
    let mut engine = fresh("catalog_fk_idx_meta");

    let cols = vec![col_def("id", TypeCode::U64), col_def("val", TypeCode::U64)];
    let tid = engine.create_table("public.parent", &cols, &[0]).unwrap();

    // Create child table with FK to parent
    let child_cols = vec![
        col_def("id", TypeCode::U64),
        fk_def("parent_id", gnitz_wire::TypeCode::U64, tid, 0),
    ];
    let child_tid = engine.create_table("public.child", &child_cols, &[0]).unwrap();

    // FK count
    assert!(!engine.fks_as_child(child_tid).is_empty());
    let target_id = engine.fks_as_child(child_tid)[0].parent_tid;
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

    finish(engine);
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
    assert_eq!(engine.dag.sources_of(v2).collect::<Vec<_>>(), [v1]);
    finish(engine);
}

/// A retired view contributes no edges: DROP VIEW and the ALTER VIEW
/// `replaces` path both unregister the outgoing view, and the map forgets it.
#[test]
fn test_dep_map_drops_a_retired_views_edges() {
    let mut engine = fresh("dep_map_retired");

    let cols = vec![col_def("id", TypeCode::U64)];
    let tid = engine.create_table("public.t", &cols, &[0]).unwrap();

    let v1 = register_identity_view(&mut engine, tid, "v1", &cols);
    assert_eq!(engine.dag.dependents_of(tid), &[v1][..]);

    // ALTER VIEW: one VIEW_TAB batch retiring v1 and registering v2.
    let v2 = engine.allocate_ids(1).unwrap();
    let views = sys_batch([(view_tab_row(v1, "v1"), -1), (view_tab_row(v2, "v1"), 1)]);
    let identity = crate::test_support::identity_circuit(tid, gnitz_wire::ReadBound::None);
    apply_ddl(&mut engine, view_blocks([(v2, &identity, &cols[..])], views)).unwrap();
    assert_eq!(
        engine.dag.dependents_of(tid),
        &[v2][..],
        "the replaced view's edge is gone, the replacement's is present"
    );

    // DROP VIEW retires the last edge, so the base table is a dep-map orphan.
    engine.drop_view("public.v1").unwrap();
    assert!(engine.dag.dependents_of(tid).is_empty());

    finish(engine);
}

/// The two dependent-view RESTRICTs — DROP TABLE and DROP COLUMN — both read
/// the dependency map.
#[test]
fn test_dependent_view_restricts_fire_from_registered_views() {
    let mut engine = fresh("dep_map_restrict");

    let cols = vec![col_def("id", TypeCode::U64), col_def("val", TypeCode::U64)];
    let tid = engine.create_table("public.t", &cols, &[0]).unwrap();

    register_identity_view(&mut engine, tid, "v", &cols);

    let err = engine.drop_table("public.t").unwrap_err();
    assert!(err.contains("dependent views (public.v)"), "DROP TABLE RESTRICT: {err}");

    // DROP NOT NULL on `val` — an `is_nullable 0→1` rewrite pair.
    let pair = col_alter_pair(tid, 1, &cols[1], |c| c.def.is_nullable = true);
    let err = engine.precheck_family(SysFamily::Column, &pair).unwrap_err();
    assert!(err.contains("dependent views"), "DROP NOT NULL RESTRICT: {err}");

    finish(engine);
}

#[test]
fn test_circuit_table_surface_introspectable() {
    let mut engine = fresh("circuit_surface");

    // Inject a row directly into CIRCUIT_TAB so the store is non-empty: view 107,
    // with a single node, through the shared row codec — as a worker applies it,
    // since no bundle carries a circuit without its view.
    let mut circuit = gnitz_wire::Circuit::default();
    circuit.input_delta(100, gnitz_wire::ReadBound::None);
    engine
        .ddl_sync(SysFamily::Circuit, circuit_batch(107, &circuit))
        .unwrap();

    // The family is SQL-introspectable — `SELECT * FROM _system.circuits` must return
    // what we just inserted (full-scan path, used by SQL planner).
    let scan = scan_all(&mut engine, gnitz_wire::CIRCUIT_TAB);
    assert_eq!(scan.len(), 1, "scan must expose the circuit row");

    // A point read by the view id.
    let pk_bytes = opk_pk(SysFamily::Circuit.schema(), &[107]);
    let found = pk_group(&mut engine, gnitz_wire::CIRCUIT_TAB, &pk_bytes);
    assert_eq!(found.len(), 1, "the circuit row by PK");

    finish(engine);
}

// ── The name indexes ────────────────────────────────────────────────────────

/// A rename hands the relation's id from the old name to the new one, and a
/// schema's entry goes with its last relation.
#[test]
fn the_relation_name_index_follows_renames_and_drops() {
    let mut engine = fresh("relation_name_index");
    engine.create_schema("s").unwrap();
    let sid = engine.schema_id("s").unwrap();
    let cols = vec![col_def("id", TypeCode::U64)];
    let tid = engine.create_table("public.orig", &cols, &[0]).unwrap();
    let member = engine.create_table("s.only", &cols, &[0]).unwrap();
    assert_eq!(engine.relation_id(sid, "only"), Some(member));
    assert_eq!(engine.relation_id(PUBLIC_SCHEMA_ID, "only"), None);

    let pair = table_tab_batch(&[(tid, "orig", -1), (tid, "renamed", 1)]);
    apply_ddl(&mut engine, [(SysFamily::Table, pair)]).unwrap();
    assert_eq!(engine.relation_id(PUBLIC_SCHEMA_ID, "orig"), None);
    assert_eq!(engine.relation_id(PUBLIC_SCHEMA_ID, "renamed"), Some(tid));

    engine.drop_table("s.only").unwrap();
    assert!(!engine.caches.relation_by_name.contains_key(&sid));

    finish(engine);
}

/// A relation's descriptor token changes exactly when a RESOLVE of its name
/// would answer differently: an index, a rename and a drop each move it, and DDL
/// on another relation or an advance of its own sequence do not.
#[test]
fn a_descriptor_token_follows_the_resolve_answer() {
    let mut engine = fresh("descriptor_token");
    let cols = vec![col_def("id", TypeCode::U64), col_def("a", TypeCode::I64)];
    let tid = engine.create_table("public.t", &cols, &[0]).unwrap();
    let resolve_token = |engine: &CatalogEngine, tid| engine.resolve_answer(tid).map(|(_, _, t)| t);
    let token = resolve_token(&engine, tid).unwrap();
    assert_ne!(token, 0);
    engine.check_token(Target { tid, token }).unwrap();
    engine.check_token(tid.into()).unwrap();

    let other = engine.create_table("public.other", &cols, &[0]).unwrap();
    engine.create_index("public.other", &["a"], false).unwrap();
    assert_eq!(resolve_token(&engine, tid), Some(token));
    assert_ne!(resolve_token(&engine, other), Some(token));

    let serial = engine.create_serial_table("public.sr").unwrap();
    let before = resolve_token(&engine, serial);
    reserve(&mut engine, serial, 64).unwrap();
    assert_eq!(resolve_token(&engine, serial), before);
    assert_eq!(resolve_token(&engine, tid), Some(token));

    let stale = |engine: &CatalogEngine, token| {
        let fault = engine.check_token(Target { tid, token }).unwrap_err();
        assert_eq!(fault.status, gnitz_wire::WireStatus::StaleCatalog);
        resolve_token(engine, tid)
    };
    let pair = table_tab_batch(&[(tid, "t", -1), (tid, "renamed", 1)]);
    apply_ddl(&mut engine, [(SysFamily::Table, pair)]).unwrap();
    let renamed = stale(&engine, token).unwrap();
    engine.create_index("public.renamed", &["id"], false).unwrap();
    let indexed = stale(&engine, renamed).unwrap();
    engine.drop_table("public.renamed").unwrap();
    assert_eq!(stale(&engine, indexed), None);

    finish(engine);
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
    let (mut engine, tid) = table_engine("catalog", "index_name_claimed_once", 1, &cols);
    let index = |engine: &mut CatalogEngine, col: u32, name: &str| {
        let id = engine.allocate_ids(1).unwrap();
        idx_tab_batch(id, tid, &[col], name, false, 1)
    };

    let live = index(&mut engine, 1, "ix");
    apply_ddl(&mut engine, [(SysFamily::Index, live)]).unwrap();
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

    finish(engine);
}
