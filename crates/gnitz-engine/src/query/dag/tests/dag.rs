use super::*;

/// A scratch directory for one test. `scratch_dir` removes it on entry, so
/// nothing here cleans up on exit: the dirs are small, carry no SAL, and
/// surviving a run is what makes a failure investigable.
fn dag_test_dir(name: &str) -> String {
    crate::test_support::scratch_dir("dag", name)
}

fn make_test_table(name: &str) -> Box<Table> {
    let schema = SchemaDescriptor::minimal_u64();
    let dir = dag_test_dir(name);
    Box::new(Table::new(&dir, schema, 99, RecoverySource::Rederive { resume_at: None }).unwrap())
}

#[test]
fn test_register_unregister_table() {
    let mut dag = DagEngine::new();
    let schema = SchemaDescriptor::minimal_u64();
    let mut tbl = make_test_table("reg_unreg");
    dag.register_table(
        100,
        TableEntry::new(
            RelationStores {
                handle: StoreHandle::Borrowed(&mut *tbl as *mut Table),
                delta: None,
            },
            schema,
            RelationKind::BaseTable,
            0,
            String::new(),
            ViewBudgets::default(),
        ),
    );
    assert!(dag.tables.contains_key(&100));

    dag.unregister_table(100);
    assert!(!dag.tables.contains_key(&100));
}

#[test]
fn test_invalidation() {
    let mut dag = DagEngine::new();
    dag.meta.insert(42, Rc::new(meta::meta_with_source(7)));
    dag.dep.valid = true;

    dag.invalidate(42);
    assert!(!dag.meta.contains_key(&42));
    assert!(dag.dep.valid, "invalidate drops the plan and its meta, not the dep map");

    dag.invalidate_dep_map();
    assert!(!dag.dep.valid);

    dag.meta.insert(99, Rc::new(meta::meta_with_source(7)));
    dag.invalidate_all();
    assert!(dag.meta.is_empty());
}

/// `evict_meta` must drop the metadata mentioning the id as the owning view
/// OR as a join source of another view's map — a dropped relation can be
/// either, and a stale entry would disagree with the live circuit
/// (over-eviction is safe; entries are recomputed on next touch).
#[test]
fn test_view_meta_eviction() {
    let mut dag = DagEngine::new();
    dag.meta.insert(42, Rc::new(meta::meta_with_source(7))); // 42 as the view
    dag.meta.insert(7, Rc::new(meta::meta_with_source(42))); // 42 as a source of view 7
    dag.evict_meta(42);
    assert!(!dag.meta.contains_key(&42));
    assert!(
        !dag.meta.contains_key(&7),
        "evict must drop views whose map mentions the id as a source"
    );

    // Wiring: the production table/view-drop path routes through evict_meta.
    let mut dag = DagEngine::new();
    dag.meta.insert(43, Rc::new(meta::meta_with_source(1)));
    dag.unregister_table(43);
    assert!(!dag.meta.contains_key(&43), "unregister_table must evict");
}

#[test]
fn test_add_remove_index_circuit() {
    let mut dag = DagEngine::new();
    // A real 3-column owner schema: registration precomputes the circuit's
    // `key_spec` from it, which locates indexed column 2.
    let schema = SchemaDescriptor::new(
        &[crate::schema::SchemaColumn::new(crate::schema::type_code::U64, 0); 3],
        &[0],
    );
    let mut tbl = make_test_table("idx_parent");
    dag.register_table(
        50,
        TableEntry::new(
            RelationStores {
                handle: StoreHandle::Borrowed(&mut *tbl as *mut Table),
                delta: None,
            },
            schema,
            RelationKind::BaseTable,
            0,
            String::new(),
            ViewBudgets::default(),
        ),
    );
    let idx_tbl = make_test_table("idx_child");
    dag.add_index_circuit(50, &[2], 999, idx_tbl, schema, false);
    assert_eq!(dag.tables[&50].index_circuits.len(), 1);

    dag.remove_index_circuit(50, &[2]);
    assert_eq!(dag.tables[&50].index_circuits.len(), 0);
    dag.close();
}

#[test]
fn test_dep_map_empty() {
    let mut dag = DagEngine::new();
    dag.get_dep_map();
    assert!(dag.dep.forward.is_empty());
    assert!(dag.dep.valid);
    assert!(dag.get_source_ids(42).is_empty());
}

/// Install `edges` (source → view) into an already-valid dep map, the same
/// pair of entries `DepMap::get_or_rebuild` writes per `ScanDelta` node.
fn dag_with_deps(edges: &[(i64, i64)]) -> DagEngine {
    let mut dag = DagEngine::new();
    for &(src, view) in edges {
        dag.dep.forward.entry(src).or_default().push(view);
        dag.dep.reverse.entry(view).or_default().push(src);
    }
    dag.dep.valid = true;
    dag
}

/// `source_closure` walks `view → sources` transitively, over a chain, a
/// diamond and a disconnected pair. A seed is not its own source, so neither
/// direction reports it.
#[test]
fn test_source_closure_walks_sources_transitively() {
    // chain 1 → 2 → 3, diamond 10 → {11,12} → 13, disconnected pair 20 → 21.
    let mut dag = dag_with_deps(&[(1, 2), (2, 3), (10, 11), (10, 12), (11, 13), (12, 13), (20, 21)]);

    assert!(dag.source_closure(vec![]).is_empty());
    assert_eq!(dag.source_closure(vec![3]), [1i64, 2].into_iter().collect());
    assert_eq!(dag.source_closure(vec![13]), [10i64, 11, 12].into_iter().collect());
    assert_eq!(dag.source_closure(vec![21]), [20i64].into_iter().collect());
    assert!(dag.source_closure(vec![1]).is_empty());
    assert!(dag.source_closure(vec![99]).is_empty());
    // The other direction over the same edges, so a walk that read the wrong
    // half of `DepMap` cannot pass both.
    dag.get_dep_map();
    assert_eq!(
        DepMap::closure(&dag.dep.forward, vec![1]),
        [2i64, 3].into_iter().collect::<rustc_hash::FxHashSet<i64>>()
    );
}

#[test]
fn test_flush_includes_index_circuits() {
    let mut dag = DagEngine::new();
    // A real 2-column owner schema: registration precomputes the circuit's
    // `key_spec` from it, which locates indexed column 1.
    let parent_schema = SchemaDescriptor::new(
        &[crate::schema::SchemaColumn::new(crate::schema::type_code::U64, 0); 2],
        &[0],
    );
    let mut tbl = make_test_table("flush_ic_parent");
    dag.register_table(
        70,
        TableEntry::new(
            RelationStores {
                handle: StoreHandle::Borrowed(&mut *tbl as *mut Table),
                delta: None,
            },
            parent_schema,
            RelationKind::BaseTable,
            0,
            String::new(),
            ViewBudgets::default(),
        ),
    );

    // Durable index table: flush writes shard_*.db only if called.
    let idx_schema = crate::schema::SchemaDescriptor::minimal_u64();
    let idx_dir = dag_test_dir("flush_ic_idx");
    let idx_tbl = Box::new(Table::new(&idx_dir, idx_schema, 1, RecoverySource::SalReplay).unwrap());
    dag.add_index_circuit(70, &[1], 999, idx_tbl, idx_schema, false);

    // Put one row in the index table's memtable.
    {
        let entry = dag.tables.get_mut(&70).unwrap();
        let mut batch = Batch::with_capacity(idx_schema, 1);
        batch.extend_pk(1u128);
        batch.extend_weight(&1i64.to_le_bytes());
        batch.extend_null_bmp(&0u64.to_le_bytes());
        batch.count += 1;
        entry.index_circuits[0].table_mut().ingest_owned_batch(batch).unwrap();
    }

    dag.flush(70).unwrap();
    let shard_count = std::fs::read_dir(&idx_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_str().unwrap_or("").starts_with("shard_"))
        .count();
    assert!(shard_count > 0, "index circuit shard must be written by flush");

    dag.close();
}

// A storage error while applying committed data in `ingest_store_and_indices`
// is returned rather than swallowed; the process that owns the recovery
// decision makes it (for a server, restart + SAL replay, asserted beside its
// own call site and end-to-end by `test_ingest_apply_error_aborts_and_replays`).
// Driven via the `GNITZ_INJECT_INGEST_APPLY_ERROR` debug seam.
#[test]
fn test_ingest_apply_error_is_returned() {
    // The full path, not the bare name: `run_test_in_child` filters with
    // `--exact`.
    let name = "query::dag::tests::ingest_apply_error_returned_internal";
    let out = crate::test_support::run_test_in_child(name, &[("GNITZ_INJECT_INGEST_APPLY_ERROR", "store")]);
    crate::test_support::assert_child_ok(
        &out,
        "the seam-armed child must return the error rather than swallow it",
    );
}

// Runs only in the re-exec'd child, which is where the armed seam is read.
// Registers a view and ingests one row; the "store" seam substitutes Err for
// the store ingest. `View`, not `BaseTable`: a base table must be a
// `Partitioned` handle (it runs `enforce_unique_pk`), and this fixture holds
// a `Borrowed` one.
#[test]
fn ingest_apply_error_returned_internal() {
    if !crate::test_support::in_child_test() {
        return;
    }
    let mut dag = DagEngine::new();
    let schema = crate::schema::SchemaDescriptor::minimal_u64();
    let dir = dag_test_dir("seam_abort");
    let mut tbl = Box::new(Table::new(&dir, schema, 99, RecoverySource::Rederive { resume_at: None }).unwrap());
    dag.register_table(
        70,
        TableEntry::new(
            RelationStores {
                handle: StoreHandle::Borrowed(&mut *tbl as *mut Table),
                delta: None,
            },
            schema,
            RelationKind::View,
            0,
            String::new(),
            ViewBudgets::default(),
        ),
    );
    let mut batch = Batch::with_capacity(schema, 1);
    batch.extend_pk(1u128);
    batch.extend_weight(&1i64.to_le_bytes());
    batch.extend_null_bmp(&0u64.to_le_bytes());
    batch.count += 1;
    assert!(
        matches!(
            dag.ingest_returning_effective(70, batch),
            Err(crate::storage::StorageError::Io(_))
        ),
        "ingest_store_and_indices must return the storage error when the seam is armed",
    );
    println!("{}", crate::test_support::CHILD_OK);
}
