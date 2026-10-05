use super::*;
use std::fs;

/// Every malformed CREATE below is refused by `precheck_family`, which takes the
/// catalog by `&self`: a refusal there has written nothing.
#[test]
fn a_malformed_create_is_refused_at_the_precheck() {
    let dir = temp_dir("atomicity_precheck");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
    let cols = vec![
        col_def("id", TypeCode::U64),
        col_def("val", TypeCode::I64),
        col_def("ts", TypeCode::I64),
    ];
    let taken = engine.create_table("public.taken", &cols, &[0]).unwrap();
    engine.create_index("public.taken", &["val"], false).unwrap();
    let view = register_identity_view(&mut engine, taken, "a_view", &cols);
    let bounded = try_register_identity_view(&mut engine, taken, "a_bounded_view", &cols, 1 << 20, 0).unwrap();

    // A fresh relation id carrying `cols` as its column records.
    let mut with_cols = |cols: &[CatalogColumn]| {
        let id = engine.allocate_ids(1).unwrap();
        engine.write_column_records(id, cols).unwrap();
        id
    };
    let no_cols = with_cols(&[]);
    let string_pk = with_cols(&[col_def("label", TypeCode::String)]);
    let dup_name = with_cols(&cols);
    let twins = [with_cols(&cols), with_cols(&cols)];
    let too_wide: Vec<_> = (0..=gnitz_wire::MAX_COLUMNS)
        .map(|i| col_def(&format!("c{i}"), TypeCode::U64))
        .collect();
    let too_wide = with_cols(&too_wide);
    // Column records at indices 0 and 2: `col_tab_batch` numbers by position.
    let gapped = engine.allocate_ids(1).unwrap();
    let mut bb = BatchBuilder::new(SysFamily::Column.schema());
    for col_idx in [0, 2] {
        col_def("c", TypeCode::U64).write_col_tab_row(&mut bb, gapped, col_idx, 1);
    }
    engine.submit(SysFamily::Column, bb.finish()).unwrap();
    let unregistered = engine.allocate_ids(1).unwrap();
    let index_on = |owner: u64, cols: &[u32], name: &str| idx_tab_batch(unregistered + 1, owner, cols, name, false, 1);

    for (family, batch, why) in [
        (
            SysFamily::Table,
            table_tab_batch(&[(no_cols, "t", 1)]),
            "has no column records",
        ),
        (
            SysFamily::Table,
            table_tab_batch(&[(string_pk, "t", 1)]),
            "has type_code STRING",
        ),
        (SysFamily::Table, table_tab_batch(&[(gapped, "t", 1)]), "non-contiguous"),
        (
            SysFamily::Table,
            table_tab_batch(&[(dup_name, "taken", 1)]),
            "already exists: public.taken",
        ),
        // The name test reads a cache this batch has not been applied to, so
        // two rows under one new name both pass it.
        (
            SysFamily::Table,
            table_tab_batch(&[(twins[0], "twins", 1), (twins[1], "twins", 1)]),
            "already exists: public.twins",
        ),
        (
            SysFamily::View,
            build_view_tab_row(no_cols, "v"),
            "has no column records",
        ),
        (SysFamily::View, build_view_tab_row(too_wide, "v"), "columns (max"),
        (
            SysFamily::Index,
            index_on(unregistered, &[0], "ix"),
            "is not registered",
        ),
        // A bounded view's sweep drops the payload an entry is projected from.
        (
            SysFamily::Index,
            index_on(bounded, &[1], "ix"),
            "only a base table or a view without a capacity can be indexed",
        ),
        // A view's circuit cannot refuse a duplicate.
        (
            SysFamily::Index,
            idx_tab_batch(unregistered + 1, view, &[1], "ix", true, 1),
            "cannot carry a UNIQUE index",
        ),
        (
            SysFamily::Index,
            index_on(taken, &[2], "public__taken__idx_val"),
            "Index already exists",
        ),
    ] {
        let err = engine.precheck_family(family, &batch).unwrap_err();
        assert!(err.contains(why), "{why}: {err}");
    }

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── next_id follows applied ids ──────────────────────────────────────────────

#[test]
fn test_next_id_advances_on_index_register() {
    let dir = temp_dir("atomicity_idx_seq");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();

    let cols = vec![col_def("id", TypeCode::U64), col_def("val", TypeCode::I64)];
    let tid = engine.create_table("public.seqsync", &cols, &[0]).unwrap();

    // An index id far ahead of the local counter.
    let large_idx_id = engine.next_id + 500;
    let batch = idx_tab_batch(large_idx_id, tid, &[1], "public__seqsync__idx_val_sync", false, 1);
    engine.ingest_to_family(gnitz_wire::IDX_TAB, &batch).unwrap();

    assert!(
        engine.next_id > large_idx_id,
        "next_id ({}) must exceed the registered idx_id ({}) after \
         the IDX_TAB row is applied",
        engine.next_id,
        large_idx_id
    );

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_next_id_advances_on_schema_register() {
    let dir = temp_dir("atomicity_schema_seq");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();

    let large_sid = engine.next_id + 500;
    let batch = schema_tab_batch(&[(large_sid, "recovered_schema", 1)]);
    engine.ingest_to_family(gnitz_wire::SCHEMA_TAB, &batch).unwrap();

    assert!(
        engine.next_id > large_sid,
        "next_id ({}) must exceed the registered sid ({}) so \
         allocate_ids never re-issues it",
        engine.next_id,
        large_sid,
    );

    let _ = fs::remove_dir_all(&dir);
}

// ── DROP SCHEMA must not probe the table-keyed dep map ───────────────────────

#[test]
fn test_drop_schema_id_colliding_with_dependent_table_id_ok() {
    // A client chooses the id its SCHEMA_TAB row carries, so a schema id can equal
    // a table id; dropping that schema must not read the table's dependents.
    let dir = temp_dir("atomicity_schema_id_collision");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();

    // Table T (in schema `owner`) with a dependent view V → dep_map[T] = [V].
    engine.create_schema("owner").unwrap();
    let tid = engine
        .create_table("owner.t", &[col_def("id", TypeCode::U64)], &[0])
        .unwrap();
    let vid = register_identity_view(&mut engine, tid, "v", &[col_def("id", TypeCode::U64)]);
    assert_eq!(
        engine.dag.dependents_of(tid),
        &[vid][..],
        "precondition: dependency edge T -> V must be present"
    );

    engine
        .submit(SysFamily::Schema, schema_tab_batch(&[(tid, "victim", 1)]))
        .unwrap();
    assert_eq!(
        engine.schema_id("victim").expect("the schema exists"),
        tid,
        "test setup: victim schema id must collide with tid"
    );

    // The victim schema is empty and unrelated to T/V. Dropping it must
    // succeed despite its id colliding with a table that has a dependent view.
    engine.drop_schema("victim").unwrap();
    assert!(
        !engine.caches.schema_by_name.contains_key("victim"),
        "victim schema must be gone after a successful DROP SCHEMA"
    );

    let _ = fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// DDL_TXN bundle rollback: `apply_bundle`, then `compensate_stage_a`, as
// `handle_ddl_txn` does.
// ---------------------------------------------------------------------------

/// A CREATE bundle `[COL_TAB, TABLE_TAB]` whose TABLE_TAB fails **precheck**
/// (duplicate name) must leave neither an orphan COL_TAB nor a ghost `-1` TABLE_TAB.
#[test]
fn ddl_txn_precheck_failure_no_orphan_or_ghost() {
    let dir = temp_dir("ddl_txn_precheck_ghost");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();

    let cols = vec![col_def("id", TypeCode::U64), col_def("val", TypeCode::U64)];
    // Occupy the qualified name "public.dupname".
    engine.create_table("public.dupname", &cols, &[0]).unwrap();
    let cols_before = count_records(engine.sys_relation(SysFamily::Column).cursor());
    let tables_before = count_records(engine.sys_relation(SysFamily::Table).cursor());
    // The setup is not part of the bundle being compensated.
    let _ = engine.drain_pending_broadcasts();

    let new_tid = engine.allocate_ids(1).unwrap();
    // COL_TAB is applied first; TABLE_TAB then fails its precheck, so nothing of
    // it is queued or applied.
    let err = engine
        .apply_bundle(bundle([
            (SysFamily::Table, table_tab_batch(&[(new_tid, "dupname", 1)])),
            (SysFamily::Column, col_tab_batch(new_tid, &cols, 1)),
        ]))
        .expect_err("duplicate-name TABLE_TAB must fail precheck");
    assert!(err.contains("already exists: public.dupname"), "{err}");
    engine.compensate_stage_a().unwrap();

    // The durable property: no orphan COL_TAB, no ghost -1 TABLE_TAB.
    assert_eq!(
        count_records(engine.sys_relation(SysFamily::Column).cursor()),
        cols_before,
        "orphan COL_TAB rows must be negated to zero"
    );
    assert_eq!(
        count_records(engine.sys_relation(SysFamily::Table).cursor()),
        tables_before,
        "no ghost -1 TABLE_TAB row"
    );
    assert_eq!(
        engine.qualified_name_or_unknown(new_tid),
        ("?".to_string(), "?".to_string()),
        "no phantom entity survives for the reusable new_tid"
    );

    let _ = fs::remove_dir_all(&dir);
}

/// A CREATE bundle `[COL_TAB, TABLE_TAB]` whose TABLE_TAB passes precheck but fails
/// in its register hook — a plain file where the relation's directory must go —
/// must net both families to zero.
#[test]
fn ddl_txn_hook_failure_is_compensated() {
    let dir = temp_dir("ddl_txn_hook_rollback");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();

    let cols = vec![col_def("id", TypeCode::U64), col_def("val", TypeCode::U64)];
    let cols_before = count_records(engine.sys_relation(SysFamily::Column).cursor());
    let tables_before = count_records(engine.sys_relation(SysFamily::Table).cursor());

    let _ = engine.drain_pending_broadcasts();
    let new_tid = engine.allocate_ids(1).unwrap();
    let blocker = relation_dir(&dir, new_tid);
    fs::write(&blocker, b"not a directory").unwrap();

    // The precheck reads no path, so the blocker is invisible to it.
    let err = engine
        .apply_bundle(bundle([
            (SysFamily::Column, col_tab_batch(new_tid, &cols, 1)),
            (SysFamily::Table, table_tab_batch(&[(new_tid, "hooktbl", 1)])),
        ]))
        .expect_err("register_relation must fail when the relation directory cannot be made");
    assert!(err.starts_with(&format!("table 'hooktbl' (id={new_tid}) ")), "{err}");
    engine.compensate_stage_a().unwrap();

    assert!(
        std::path::Path::new(&blocker).is_file(),
        "a path this call did not create must survive the failed registration"
    );

    assert_eq!(
        count_records(engine.sys_relation(SysFamily::Table).cursor()),
        tables_before,
        "the failed TABLE_TAB row must net to zero"
    );
    assert_eq!(
        count_records(engine.sys_relation(SysFamily::Column).cursor()),
        cols_before,
        "COL_TAB rows must net to zero (negated exactly once)"
    );
    assert!(
        !engine.registry.has_id(new_tid),
        "no registered table survives the rollback"
    );

    let _ = fs::remove_dir_all(&dir);
}

// ── `_sequences` never accumulates an unmatched retraction ───────────────────

#[test]
fn sequence_advances_leave_no_negative_ghost() {
    let dir = temp_dir("atomicity_seq_no_ghost");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();

    // First use of every master scalar: the next id, the checkpoint generation
    // and the topology word are all seeded on demand.
    engine.create_schema("s").unwrap();
    let cols = vec![col_def("id", TypeCode::U64), col_def("val", TypeCode::I64)];
    engine.create_table("s.t", &cols, &[0]).unwrap();
    engine.create_index("s.t", &["val"], false).unwrap();
    engine.record_topology(1).unwrap();
    engine.advance_durable_generation().unwrap();
    // …and a second round, where each retraction now has a live row to cancel.
    engine.create_table("s.t2", &cols, &[0]).unwrap();
    engine.record_topology(4).unwrap();
    engine.advance_durable_generation().unwrap();

    assert_eq!(
        count_negative_records(engine.sys_relation(SysFamily::Sequence).cursor()),
        0,
        "_sequences must hold no net-negative row"
    );

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── Compensating a DROP must not stage the restored relation's directory ─────
// `compensate_stage_a` negates a dropped relation's `-1` back to `+1`, so the
// register hook runs on a directory that already holds live shards. Staging it
// for crash-cleanup would let a failure inside the re-registration delete them,
// and the next boot — replaying the same TABLE_TAB row, since the DDL never
// reached the SAL — would bring the table back empty, with no error.

#[test]
fn compensating_a_drop_keeps_the_restored_relation_directory() {
    let cols = vec![col_def("id", TypeCode::U64), col_def("val", TypeCode::U64)];
    let (mut engine, tid, dir) = table_fixture("compensate_drop_keeps_dir", &cols);
    let reldir = relation_dir(&dir, tid);
    // The fixture's own CREATE is a committed DDL; only the bundle below is the
    // one being compensated.
    engine.drain_pending_broadcasts();

    // The failed bundle's DROP: applied and enqueued, so compensation drains it.
    let drop_batch = engine.retract_under(SysFamily::Table, &[tid]);
    assert_eq!(
        drop_batch.len(),
        1,
        "the fixture table must have one live TABLE_TAB row"
    );
    engine.submit(SysFamily::Table, drop_batch).unwrap();
    assert!(!engine.registry.has_id(tid), "the drop must unregister the table");
    assert!(
        std::path::Path::new(&reldir).is_dir(),
        "a drop leaves the directory to the sweep"
    );

    // Make the restoring `build_relation_store` fail: a plain file where the
    // per-worker child directory belongs, which `Table::new` cannot open.
    let child = fs::read_dir(&reldir)
        .unwrap()
        .flatten()
        .find(|e| e.file_type().is_ok_and(|t| t.is_dir()))
        .expect("one store child")
        .path();
    fs::remove_dir_all(&child).unwrap();
    fs::write(&child, b"not a directory").unwrap();

    let err = engine.compensate_stage_a().unwrap_err();
    assert!(
        err.contains("Stage-A DDL compensation failed"),
        "unexpected error: {err}"
    );
    assert!(
        std::path::Path::new(&reldir).is_dir(),
        "the restored relation's directory must survive a failure inside its re-registration"
    );

    let _ = fs::remove_dir_all(&dir);
}

// ── A compensated CREATE TABLE leaves nothing of the table behind ────────────
// Its IDX_TAB family fails after COL_TAB and TABLE_TAB applied.

#[test]
fn compensated_create_table_leaves_no_trace() {
    let dir = temp_dir("compensated_create_table");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
    let parent = engine
        .create_table("public.parent", &[col_def("pid", TypeCode::U64)], &[0])
        .unwrap();
    let _ = engine.drain_pending_broadcasts();
    let tid = engine.allocate_ids(1).unwrap();
    let idx_id = engine.allocate_ids(1).unwrap();
    let before = sys_row_counts(&engine);

    let cols = vec![
        col_def("id", TypeCode::U64),
        fk_def("pid", TypeCode::U64, parent, 0),
        col_def("val", TypeCode::I64),
    ];
    for (family, batch) in [
        (SysFamily::Column, col_tab_batch(tid, &cols, 1)),
        (SysFamily::Table, table_tab_batch(&[(tid, "child", 1)])),
    ] {
        engine.submit(family, batch).unwrap();
    }
    assert!(engine.registry.relation(tid).unwrap().index_on(&[1]).is_some());

    let blocker = engine
        .registry
        .child_dir(tid, ChildKind::Index(gnitz_wire::PkColList::from_slice(&[2])));
    fs::write(&blocker, b"not a directory").unwrap();
    let idx = idx_tab_batch(idx_id, tid, &[2], "public__child__idx_val", false, 1);
    assert!(engine.submit(SysFamily::Index, idx).is_err());
    engine.compensate_stage_a().unwrap();

    assert!(!engine.caches.relations.contains_key(&tid));
    assert!(!engine.registry.has_id(tid));
    assert!(engine.index_ids_of(tid).is_empty());
    assert!(
        !engine.fk_children_of(parent).iter().any(|e| e.child_tid == tid),
        "the parent must keep no edge from the uncreated child"
    );
    assert_eq!(
        sys_row_counts(&engine),
        before,
        "no system family may keep a row of the table"
    );
    engine.reclaim_orphan_dirs();
    assert!(
        !std::path::Path::new(&relation_dir(&dir, tid)).exists(),
        "the sweep must reclaim the uncreated table's directory"
    );

    let _ = fs::remove_dir_all(&dir);
}
