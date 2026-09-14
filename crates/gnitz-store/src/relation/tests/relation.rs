use super::*;

fn solo_registry() -> RelationRegistry {
    RelationRegistry::new(Slot::SOLO, StoreConfig::default())
}

/// A scratch directory for one test. `scratch_dir` removes it on entry, so
/// nothing here cleans up on exit: the dirs are small, carry no SAL, and
/// surviving a run is what makes a failure investigable.
fn relation_test_dir(name: &str) -> String {
    crate::test_support::scratch_dir("relation", name)
}

/// Enter `id`, opening its store under `directory` — which `add_index` also
/// creates `idx_<id>` under.
fn register_entry(
    registry: &mut RelationRegistry,
    id: i64,
    schema: SchemaDescriptor,
    kind: RelationKind,
    directory: String,
) {
    let spec = RelationSpec {
        id,
        kind,
        schema,
        directory,
        budgets: ViewBudgets::default(),
    };
    registry.register(spec, OnRegister::Live).unwrap();
}

#[test]
fn test_register_unregister_table() {
    let mut registry = solo_registry();
    let schema = crate::test_support::pk_only_schema(&[crate::schema::type_code::U64]);
    register_entry(
        &mut registry,
        100,
        schema,
        RelationKind::BaseTable,
        relation_test_dir("reg_unreg"),
    );
    assert!(registry.has_id(100));

    registry.unregister(100);
    assert!(!registry.has_id(100));
}

#[test]
fn test_add_remove_index_circuit() {
    let mut registry = solo_registry();
    // A real 3-column owner schema: registration precomputes the circuit's
    // `key_spec` from it, which locates indexed column 2.
    let schema = SchemaDescriptor::new(
        &[crate::schema::SchemaColumn::new(crate::schema::type_code::U64, 0); 3],
        &[0],
    );
    let owner_dir = relation_test_dir("idx_parent_owner");
    register_entry(&mut registry, 50, schema, RelationKind::BaseTable, owner_dir);
    registry.add_index(50, 999, &[2], false).unwrap();
    assert_eq!(registry.relation(50).unwrap().indexes().len(), 1);

    registry.remove_index(50, &[2]);
    assert_eq!(registry.relation(50).unwrap().indexes().len(), 0);
    registry.close();
}

/// `UniquePreflight` hands `index_cols` its `seek_col_idx` raw, where `HasPk` would
/// have read `0` through `probe_key_columns` as the relation's own PK store.
/// Neither `0` nor a garbage non-zero word names a column list.
#[test]
fn a_flag_clear_seek_col_idx_names_no_index() {
    let mut registry = solo_registry();
    let schema = SchemaDescriptor::new(
        &[crate::schema::SchemaColumn::new(crate::schema::type_code::U64, 0); 3],
        &[0],
    );
    let owner_dir = relation_test_dir("seek_col_idx_zero_owner");
    register_entry(&mut registry, 50, schema, RelationKind::BaseTable, owner_dir);
    registry.add_index(50, 999, &[2], false).unwrap();

    assert!(registry
        .index_cols(50, gnitz_wire::pack_pk_cols(&[2]), "unique pre-flight")
        .is_ok());
    for raw in [gnitz_wire::PROBE_KEYSPACE_PK, 2] {
        let err = registry
            .index_cols(50, raw, "unique pre-flight")
            .expect_err("a flag-clear word names no column list");
        assert!(err.to_string().contains("invalid column list"), "{raw}: {err}");
    }
    registry.close();
}

/// An index store is rederived, so a base round only folds it to RAM; the
/// ephemeral round is what force-persists it, index circuits included.
#[test]
fn ephemeral_flush_includes_index_circuits() {
    let mut registry = solo_registry();
    // A real 2-column owner schema: registration precomputes the circuit's
    // `key_spec` from it, which locates indexed column 1.
    let parent_schema = SchemaDescriptor::new(
        &[crate::schema::SchemaColumn::new(crate::schema::type_code::U64, 0); 2],
        &[0],
    );
    let owner_dir = relation_test_dir("flush_ic_owner");
    register_entry(
        &mut registry,
        70,
        parent_schema,
        RelationKind::BaseTable,
        owner_dir.clone(),
    );
    registry.add_index(70, 999, &[1], false).unwrap();

    // Put one row in the index table's memtable.
    {
        let ic = registry.relation_mut(70).and_then(|r| r.index_on_mut(&[1])).unwrap();
        let index_schema = ic.schema();
        let mut batch = Batch::with_capacity(&index_schema, 1);
        batch.extend_pk(1u128);
        batch.extend_weight(&1i64.to_le_bytes());
        batch.extend_null_bmp(&0u64.to_le_bytes());
        batch.count += 1;
        ic.ingest_owned_batch(batch).unwrap();
    }

    registry.checkpoint_ephemeral(1, []).unwrap();
    let idx_dir = ChildAddr::Index { id: 999 }.dir(&owner_dir);
    let store_dir = ChildAddr::worker(Slot::SOLO).dir(&idx_dir);
    let shard_count = std::fs::read_dir(&store_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_str().unwrap_or("").starts_with("shard_"))
        .count();
    assert!(
        shard_count > 0,
        "the ephemeral round must publish the index circuit's shard"
    );

    registry.close();
}

/// A fed view's delta store takes the epoch output **weights and all**: the
/// round's own net weight per (PK, payload), not a row set. Two rounds against
/// one key — `+1` then `-1` — must both be retained, because the output store
/// folds them to nothing and the feed is the only place they survive.
#[test]
fn a_fed_view_retains_each_round_at_its_own_weight() {
    let mut registry = solo_registry();
    let schema = crate::test_support::pk_only_schema(&[crate::schema::type_code::U64]);
    let vid = gnitz_wire::FIRST_USER_TABLE_ID as i64;
    registry
        .register(
            RelationSpec {
                id: vid,
                kind: RelationKind::View,
                schema,
                directory: relation_test_dir("fed_view_delta"),
                budgets: ViewBudgets {
                    capacity_bytes: None,
                    delta_bytes: Some(1 << 20),
                },
            },
            OnRegister::Live,
        )
        .unwrap();

    let row = |w: i64| {
        let mut b = Batch::with_capacity(&schema, 1);
        b.extend_pk(7u128);
        b.extend_weight(&w.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.count += 1;
        b
    };
    registry.ingest_view_delta(vid, row(1), Some(4), false).unwrap();
    registry.ingest_view_delta(vid, row(-1), Some(5), false).unwrap();

    // The output store folded the pair away — which is exactly why the feed has
    // to have kept both.
    let entry = registry.relation_or_err(vid).unwrap();
    let mut live = 0i64;
    let mut cur = entry.cursor();
    while cur.valid {
        live += cur.current_weight;
        cur.advance();
    }
    assert_eq!(live, 0, "the output store's fold annihilates the pair");

    let feed = entry.delta_or_err().expect("a fed view holds a delta store");
    let stride = feed.schema().pk_stride();
    let mut rounds: Vec<(u64, i64)> = Vec::new();
    let floor = vec![0u8; stride];
    let (mut cur, _) = feed.range_cursor(&floor, None);
    while cur.valid {
        // The delta PK is `round` big-endian, then the view's own PK.
        let tick = u64::from_be_bytes(cur.current_pk_bytes()[..8].try_into().unwrap());
        rounds.push((tick, cur.current_weight));
        cur.advance();
    }
    assert_eq!(rounds, vec![(4, 1), (5, -1)], "each round keeps its own weight");

    registry.close();
}

// A storage error while applying committed data in `ingest_store_and_indices`
// is returned rather than swallowed; the process that owns the recovery
// decision makes it (for a server, restart + SAL replay, asserted beside its
// own call site and end-to-end by `test_ingest_apply_error_aborts_and_replays`).
// Driven via the `GNITZ_INJECT_INGEST_APPLY_ERROR` debug seam.
#[test]
fn test_ingest_apply_error_is_returned() {
    let out = crate::test_support::run_test_in_child(
        module_path!(),
        "ingest_apply_error_returned_internal",
        &[("GNITZ_INJECT_INGEST_APPLY_ERROR", "store")],
    );
    crate::test_support::assert_child_ok(
        &out,
        "the seam-armed child must return the error rather than swallow it",
    );
}

// Runs only in the re-exec'd child, which is where the armed seam is read.
// Registers a view and ingests one row; the "store" seam substitutes Err for
// the store ingest. `View`, not `BaseTable`: a base table would run
// `enforce_unique_pk` against the fixture's store first.
#[test]
fn ingest_apply_error_returned_internal() {
    if !crate::test_support::in_child_test() {
        return;
    }
    let mut registry = solo_registry();
    let schema = crate::test_support::pk_only_schema(&[crate::schema::type_code::U64]);
    let tid = gnitz_wire::FIRST_USER_TABLE_ID as i64;
    register_entry(
        &mut registry,
        tid,
        schema,
        RelationKind::View,
        relation_test_dir("seam_abort"),
    );
    let mut batch = Batch::with_capacity(&schema, 1);
    batch.extend_pk(1u128);
    batch.extend_weight(&1i64.to_le_bytes());
    batch.extend_null_bmp(&0u64.to_le_bytes());
    batch.count += 1;
    assert!(
        matches!(
            registry.ingest(tid, batch),
            Err(StoreError::Storage {
                err: crate::storage::StorageError::Io(_),
                ..
            })
        ),
        "ingest_store_and_indices must return the storage error when the seam is armed",
    );
    println!("{}", crate::test_support::CHILD_OK);
}
