use super::*;

// ── test_index_live_fanout ────────────────────────────────────────────────

#[test]
fn test_index_live_fanout() {
    let (_dir, mut engine) = empty_catalog(1);

    let cols = vec![col_def("id", TypeCode::U64), col_def("val", TypeCode::I64)];
    let tid = engine.create_table("public.tfanout", &cols, &[0]).unwrap();

    // Ingest 5 rows
    engine
        .registry
        .ingest(tid, rows(&engine, tid, 1, 0..5, |i| [i * 100]))
        .unwrap();
    engine.registry.checkpoint_base().unwrap();

    // Create index — fills from the 5 rows
    engine.create_index("public.tfanout", &["val"], false).unwrap();

    // Live fan-out: ingest 1 more row via the registry (which does index projection)
    engine
        .registry
        .ingest(tid, rows(&engine, tid, 1, [99], |_| [777]))
        .unwrap();
    engine.registry.checkpoint_base().unwrap();

    // Verify index has 6 entries via DagEngine's index circuit
    assert_eq!(
        index_weight(&engine, tid),
        6,
        "Index fanout: one entry per row at weight 1"
    );

    engine.close();
}

/// A CREATE INDEX whose hook fails leaves nothing behind, and broadcasts no `-1`
/// for the `+1` it never sent.
#[test]
fn test_failed_create_index_rolls_back() {
    let (_dir, mut engine, tid) = table_engine(1, &[col_def("id", TypeCode::U64), col_def("val", TypeCode::U64)]);
    engine
        .registry
        .ingest(tid, rows(&engine, tid, 1, [1], |_| [42]))
        .unwrap();
    engine.registry.checkpoint_base().unwrap();

    // The id `create_index` is about to allocate.
    let failed_idx_id = engine.next_catalog_id();
    // A file where this process's index store goes.
    let blocker = engine
        .registry
        .child_dir(tid, ChildKind::Index(gnitz_wire::PkColList::from_slice(&[1])));
    fs::write(&blocker, b"not a directory").unwrap();
    let idx_name = make_secondary_index_name("public", "t", "val");

    assert!(
        engine.create_index("public.t", &["val"], true).is_err(),
        "a blocked index directory must fail the create"
    );
    assert!(
        std::path::Path::new(&blocker).is_file(),
        "a path the create did not make survives its rollback"
    );
    assert!(
        engine.registry.relation(tid).unwrap().indexes().is_empty(),
        "no half-built index may stay registered"
    );
    assert!(
        !engine.has_index_by_name(&idx_name),
        "the name must not resolve to a ghost index"
    );
    assert_eq!(
        idx_weights_for(&engine, failed_idx_id),
        Vec::<i64>::new(),
        "a failed create must leave no stored weight under its id"
    );

    // The failed attempt does not block a later index on the same column.
    fs::remove_file(&blocker).unwrap();
    engine.create_index("public.t", &["val"], false).unwrap();
    assert!(engine.has_index_by_name(&idx_name));

    engine.close();
}

// ── drop_table cascades to owned indices ─────────────────────────────
// A IDX_TAB row whose owner is gone fails the next boot's replay, leaving
// the database un-openable.

#[test]
fn test_drop_table_cascades_fk_index() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().to_str().unwrap();
    let mut engine = CatalogEngine::open(dir, 1).unwrap();

    let parent_tid = fk_parent(&mut engine);
    let child_tid = engine
        .create_table("public.child", &fk_child_cols(parent_tid), &[0])
        .unwrap();

    assert!(
        engine
            .registry
            .relation(child_tid)
            .is_some_and(|e| e.index_on(&[1]).is_some()),
        "the FK column's circuit must be derived with the child table"
    );

    engine.drop_table("public.child").unwrap();
    assert!(engine.registry.relation(child_tid).is_none());

    // Reopen catalog — must succeed, and no catalog index names the child.
    engine.close();
    let engine2 = CatalogEngine::open(dir, 1).unwrap();
    assert!(engine2.registry.relation(child_tid).is_none());
    assert!(
        engine2.index_ids_of(child_tid).is_empty(),
        "no index row may exist for the dropped child"
    );
    engine2.close();
}

// ── drop_table retracts every index the table owns ──

#[test]
fn test_drop_table_cascades_multiple_indices() {
    let (_dir, mut engine, tid) = table_engine(
        1,
        &[
            col_def("id", TypeCode::U64),
            col_def("val1", TypeCode::I64),
            col_def("val2", TypeCode::I64),
        ],
    );

    engine.create_index("public.t", &["val1"], false).unwrap();
    engine.create_index("public.t", &["val2"], false).unwrap();

    let owned_indices = |engine: &CatalogEngine| engine.index_ids_of(tid);
    assert_eq!(
        owned_indices(&engine).len(),
        2,
        "IDX_TAB must name both indices under the table"
    );
    let (name1, name2) = (
        make_secondary_index_name("public", "t", "val1"),
        make_secondary_index_name("public", "t", "val2"),
    );
    assert!(engine.has_index_by_name(&name1));
    assert!(engine.has_index_by_name(&name2));

    let idx_count_before = count_records(engine.sys_relation(SysFamily::Index).cursor());
    assert!(idx_count_before >= 2);

    // Drop the table — cascade must retract both indices
    engine.drop_table("public.t").unwrap();

    assert!(engine.registry.relation(tid).is_none(), "table DAG entry must be gone");
    assert!(!engine.has_index_by_name(&name1), "idx1 must be gone");
    assert!(!engine.has_index_by_name(&name2), "idx2 must be gone");
    assert!(
        owned_indices(&engine).is_empty(),
        "IDX_TAB must name no index under the dropped table"
    );
    assert_eq!(
        count_records(engine.sys_relation(SysFamily::Index).cursor()),
        idx_count_before - 2,
        "IDX_TAB must retract exactly two records"
    );

    // Reopen: replay must succeed without orphaned IDX_TAB rows
    let dir = engine.registry.base_dir().to_owned();
    engine.close();
    let engine2 = CatalogEngine::open(&dir, 1).unwrap();
    assert!(!engine2.has_index_by_name(&name1));
    assert!(!engine2.has_index_by_name(&name2));
    engine2.close();
}

#[test]
fn drop_table_removes_the_relation_entry() {
    let (_dir, mut engine, tid) = table_engine(1, &[col_def("id", TypeCode::U64), col_def("val", TypeCode::I64)]);
    engine.create_index("public.t", &["val"], false).unwrap();

    let rename = col_alter_pair(tid, 1, &col_def("val", TypeCode::I64), |c| c.def.name = "val2".into());
    apply_ddl(&mut engine, [(SysFamily::Column, rename)]).unwrap();

    assert!(
        engine.caches.relations.contains_key(&tid),
        "live table must have a relation entry"
    );

    engine.drop_table("public.t").unwrap();

    assert!(
        !engine.caches.relations.contains_key(&tid),
        "the relation entry must be removed by the drop"
    );

    engine.close();
}

// ── UNIQUE index on STRING/BLOB rejected at DDL ──────────────────────────

#[test]
fn test_create_unique_index_on_string_blob_rejected() {
    let (_dir, mut engine) = empty_catalog(1);
    let blob_col = col_def("data", TypeCode::Blob);
    let cols = vec![
        col_def("id", TypeCode::U64),
        col_def("name", TypeCode::String),
        blob_col,
    ];
    engine.create_table("public.t", &cols, &[0]).unwrap();

    // Unique or not, the rejection is the same one: STRING and BLOB are no key
    // columns, so the PK rule refuses them and `precheck_family` never reaches
    // the registration.
    for unique in [true, false] {
        for col in ["name", "data"] {
            let e = engine.create_index("public.t", &[col], unique).unwrap_err();
            assert!(
                e.contains("Index: column list column") && e.contains("can be key columns"),
                "unique={unique} col={col} got: {e}"
            );
        }
    }

    engine.close();
}

// ── UNIQUE+FK promotion / demotion (column-level unique constraint) ───────
//
// A UNIQUE index on an FK column promotes the column's FK circuit; dropping it
// demotes the circuit rather than destroying it.

/// Uniqueness of the index circuit on `col`, or `None` if no circuit exists.
fn circuit_unique(engine: &CatalogEngine, tid: u64, col: u32) -> Option<bool> {
    engine
        .registry
        .relation(tid)
        .and_then(|e| e.index_on(&[col]))
        .map(SecondaryIndex::is_unique)
}

/// Registered through `ddl_sync`, an FK column's circuit has no IDX_TAB row and
/// outlives the UNIQUE index that promoted it.
#[test]
fn test_fk_circuit_is_derived_and_survives_its_unique_index() {
    let (_dir, mut engine) = empty_catalog(1);
    let parent_tid = fk_parent(&mut engine);
    let idx_rows_before = count_records(engine.sys_relation(SysFamily::Index).cursor());

    let child_tid = engine.next_catalog_id();
    engine
        .register_table(child_tid, PUBLIC_SCHEMA_ID, "child", &fk_child_cols(parent_tid), &[0])
        .unwrap();
    let claims = engine
        .registry
        .relation(child_tid)
        .and_then(|e| e.index_on(&[1]))
        .map(|ix| ix.claims().to_vec());
    assert_eq!(claims, Some(vec![IndexClaim::ForeignKey]));
    assert_eq!(
        count_records(engine.sys_relation(SysFamily::Index).cursor()),
        idx_rows_before
    );

    engine.create_index("public.child", &["fk"], true).unwrap();
    assert_eq!(circuit_unique(&engine, child_tid, 1), Some(true));
    engine
        .drop_index(&make_secondary_index_name("public", "child", "fk"))
        .unwrap();
    assert_eq!(circuit_unique(&engine, child_tid, 1), Some(false));

    engine.close();
}

/// Drop the indexes `idx_ids` in a bundle that also drops a scratch table, and
/// refuse it at the Table group. A drop-only bundle applies in reverse
/// `SysFamily::ALL` order, so the Index group is applied, then undone while its
/// owner stays registered.
fn apply_index_drop_then_refuse(engine: &mut CatalogEngine, idx_ids: &[u64]) {
    let scratch = engine
        .create_table("public.scratch", &[col_def("id", TypeCode::U64)], &[0])
        .unwrap();
    let blocks = bundle([
        (SysFamily::Index, engine.retract_under(SysFamily::Index, idx_ids)),
        (SysFamily::Table, engine.retract_under(SysFamily::Table, &[scratch])),
    ]);
    let mut sink = |f: SysFamily, _: &Batch| match f {
        SysFamily::Table => Err("the log refuses the zone".into()),
        _ => Ok(()),
    };
    let refused = engine.apply_bundle(blocks, &mut sink);
    assert!(
        matches!(refused, Err(ZoneError::Refused(_))),
        "the bundle must be refused and undone"
    );
}

/// A shared circuit's directory is named by its column list, so undoing the
/// drop of the last index on it re-opens the directory it already had — the one
/// the workers, which never saw the undone drop, still hold.
#[test]
fn undone_drop_of_a_shared_index_keeps_its_directory() {
    let (_dir, mut engine, tid) = table_engine(1, &[col_def("id", TypeCode::U64), col_def("val", TypeCode::U64)]);
    let mut ids = Vec::new();
    for name in ["public__t__a", "public__t__b"] {
        let id = engine.allocate_ids(1).unwrap();
        let idx = idx_tab_batch(id, tid, &[1], name, false, 1);
        apply_ddl(&mut engine, [(SysFamily::Index, idx)]).unwrap();
        ids.push(id);
    }
    engine.submit_retraction(SysFamily::Index, ids[0]).unwrap();

    let reldir = relation_dir(engine.registry.base_dir(), tid);
    let index_dirs = || {
        let mut names: Vec<String> = fs::read_dir(&reldir)
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .filter(|n| n.starts_with("idx_"))
            .collect();
        names.sort();
        names
    };
    let before = index_dirs();
    assert_eq!(before.len(), 1, "{before:?}");

    apply_index_drop_then_refuse(&mut engine, &ids[1..]);
    assert_eq!(engine.index_ids_of(tid), vec![ids[1]]);
    assert!(engine.registry.relation(tid).unwrap().index_on(&[1]).is_some());
    assert_eq!(index_dirs(), before);

    engine.close();
}

#[test]
fn test_unique_index_over_fk_column_distinct_data_promotes() {
    // Masking-bug regression (clean data): CREATE UNIQUE INDEX over an FK column
    // populated with distinct values must promote and enforce, not be masked.
    let (_dir, mut engine) = empty_catalog(1);

    let parent_tid = fk_parent(&mut engine);
    let child_tid = engine
        .create_table("public.child", &fk_child_cols(parent_tid), &[0])
        .unwrap();

    // The FK column's circuit is registered non-unique at create time.
    assert_eq!(circuit_unique(&engine, child_tid, 1), Some(false));

    // Seed DISTINCT fk values (a registry ingest bypasses FK validation).
    let seeded = rows(&engine, child_tid, 1, [1, 2], |i| [i * 10]);
    engine.registry.ingest(child_tid, seeded).unwrap();
    engine.registry.checkpoint_base().unwrap();

    engine.create_index("public.child", &["fk"], true).unwrap();
    assert_eq!(circuit_unique(&engine, child_tid, 1), Some(true));

    engine.close();
}

#[test]
fn test_drop_index_permitted_on_lone_pk_target() {
    // A redundant unique index on a lone-PK column that is an FK target may be
    // dropped: the PK itself preserves uniqueness for FK child validation.
    let (_dir, mut engine) = empty_catalog(1);

    let parent_tid = fk_parent(&mut engine);
    engine
        .create_table("public.child", &fk_child_cols(parent_tid), &[0])
        .unwrap();

    // Redundant unique index on the parent's lone PK column.
    engine.create_index("public.parent", &["pid"], true).unwrap();
    let idx = make_secondary_index_name("public", "parent", "pid");
    engine
        .drop_index(&idx)
        .expect("drop must be permitted: lone PK preserves uniqueness");

    engine.close();
}

#[test]
fn test_drop_unique_index_on_non_pk_fk_target_blocked() {
    // A non-PK FK-target column with a unique index cannot have that index
    // dropped while a child references it and no other unique index survives.
    let (_dir, mut engine) = empty_catalog(1);

    // Parent (id PK, email) with a UNIQUE index on the non-PK `email` column.
    let parent_cols = vec![col_def("id", TypeCode::U64), col_def("email", TypeCode::U64)];
    let parent_tid = engine.create_table("public.parent", &parent_cols, &[0]).unwrap();
    engine.create_index("public.parent", &["email"], true).unwrap();

    // Child references parent.email (col 1), legal because email is unique.
    let child_cols = vec![col_def("cid", TypeCode::U64), fk_def("e", TypeCode::U64, parent_tid, 1)];
    engine.create_table("public.child", &child_cols, &[0]).unwrap();

    let idx = make_secondary_index_name("public", "parent", "email");
    let r = engine.drop_index(&idx);
    assert!(
        r.is_err(),
        "dropping the sole unique index on an FK target must be blocked"
    );
    assert!(
        r.unwrap_err().contains("no unique index would remain"),
        "error must explain the uniqueness requirement"
    );

    engine.close();
}

// ── chunked index fill ───────────────────────────────────────────────────
//
// A small `scan_chunk_rows` spreads a handful of rows over several chunks.

#[test]
fn test_unique_index_chunked_fill_distinct_succeeds() {
    // Every row must be projected exactly once across several chunks.
    let (_dir, mut engine, tid) = table_engine(1, &[col_def("id", TypeCode::U64), col_def("val", TypeCode::U64)]);
    engine
        .registry
        .ingest(tid, rows(&engine, tid, 1, 0..10, |i| [100 + i]))
        .unwrap();
    engine.registry.checkpoint_base().unwrap();

    engine.registry.set_scan_chunk_rows(3);
    engine.create_index("public.t", &["val"], true).unwrap();

    assert_eq!(
        index_weight(&engine, tid),
        10,
        "a chunked fill must project every row exactly once"
    );

    engine.close();
}

// ── Composite (multi-column) secondary index tests ──────────────────────

#[test]
fn test_composite_index_drop_exact_list() {
    // With both a single-column (a) index and a composite (a, b) index, dropping
    // (a, b) leaves the (a) index serving and removes only the (a, b) circuit.
    let (_dir, mut engine, tid) = table_engine(
        1,
        &[
            col_def("id", TypeCode::U64),
            col_def("a", TypeCode::U64),
            col_def("b", TypeCode::U64),
        ],
    );
    engine.create_index("public.t", &["a"], false).unwrap();
    engine.create_index("public.t", &["a", "b"], false).unwrap();

    assert!(engine.registry.relation(tid).and_then(|r| r.index_on(&[1])).is_some());
    assert!(engine
        .registry
        .relation(tid)
        .and_then(|r| r.index_on(&[1, 2]))
        .is_some());

    // Drop the composite by its generated name (cols joined by '_').
    engine.drop_index("public__t__idx_a_b").unwrap();

    assert!(
        engine.registry.relation(tid).and_then(|r| r.index_on(&[1])).is_some(),
        "single-column (a) index must survive"
    );
    assert!(
        engine
            .registry
            .relation(tid)
            .and_then(|r| r.index_on(&[1, 2]))
            .is_none(),
        "composite (a, b) circuit must be removed"
    );

    engine.close();
}

#[test]
fn test_composite_unique_index_registers_unique_circuit() {
    // CREATE UNIQUE INDEX over multiple columns registers one circuit carrying
    // the whole column list and the uniqueness flag. Enforcement over the
    // composite span runs distributed and is covered end-to-end.
    let (_dir, mut engine, tid) = table_engine(
        1,
        &[
            col_def("id", TypeCode::U64),
            col_def("a", TypeCode::U64),
            col_def("b", TypeCode::U64),
        ],
    );

    engine
        .create_index("public.t", &["a", "b"], true)
        .expect("composite UNIQUE (a, b) must be created");

    let ic = engine
        .registry
        .relation(tid)
        .and_then(|r| r.index_on(&[1, 2]))
        .expect("composite circuit must resolve on its whole column list");
    assert!(ic.is_unique(), "(a, b) was created UNIQUE");
    assert!(
        engine.registry.relation(tid).and_then(|r| r.index_on(&[1])).is_none(),
        "a composite circuit must not answer a single-column lookup"
    );

    engine.close();
}

// ── Undoing a DROP INDEX refills the restored circuit ────────────────────

#[test]
fn undone_drop_index_refills_the_restored_circuit() {
    let (_dir, mut engine, tid) = table_engine(1, &[col_def("id", TypeCode::U64), col_def("val", TypeCode::U64)]);
    engine
        .registry
        .ingest(tid, rows(&engine, tid, 1, [20], |_| [200]))
        .unwrap();
    engine.registry.checkpoint_base().unwrap();

    let idx_id = engine.create_index("public.t", &["val"], false).unwrap();

    apply_index_drop_then_refuse(&mut engine, &[idx_id]);

    let index = engine.registry.relation(tid).and_then(|r| r.index_on(&[1]));
    let entries = index.expect("the drop is undone").cursor().drain_chunk(usize::MAX);
    let entries = entries.expect("the restored index must hold the flushed row");
    // A U64's key image is its value: the entry is `val`, then the row's PK.
    assert_eq!(
        (entries.len(), gnitz_wire::widen_pk_be(entries.get_pk_bytes(0))),
        (1, 200 << 64 | 20)
    );

    engine.close();
}
