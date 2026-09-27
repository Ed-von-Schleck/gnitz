use super::*;

/// A one-worker registry over a fresh base directory named `name`.
fn solo_registry(name: &str) -> RelationRegistry {
    RelationRegistry::new(&relation_test_dir(name), Slot::SOLO, StoreConfig::default())
}

/// A scratch directory for one test. `scratch_dir` removes it on entry, so
/// nothing here cleans up on exit: the dirs are small, carry no SAL, and
/// surviving a run is what makes a failure investigable.
fn relation_test_dir(name: &str) -> String {
    crate::test_support::scratch_dir("relation", name)
}

/// Enter `id` and open its store; returns its relation directory, which
/// `add_index` also opens its index children under.
fn register_entry(registry: &mut RelationRegistry, id: i64, schema: SchemaDescriptor, kind: RelationKind) -> String {
    let spec = RelationSpec { id, kind, schema };
    registry.register(spec).unwrap();
    relation_dir(registry.base_dir(), id)
}

#[test]
fn test_register_unregister_table() {
    let mut registry = solo_registry("reg_unreg");
    let schema = crate::test_support::pk_only_schema(&[crate::schema::TypeCode::U64]);
    register_entry(&mut registry, 100, schema, RelationKind::BaseTable);
    assert!(registry.has_id(100));

    registry.unregister(100);
    assert!(!registry.has_id(100));
}

#[test]
fn test_add_remove_index_circuit() {
    let mut registry = solo_registry("idx_parent_owner");
    // A real 3-column owner schema: registration precomputes the circuit's
    // `key_spec` from it, which locates indexed column 2.
    let schema = SchemaDescriptor::new(
        &[crate::schema::SchemaColumn::new(crate::schema::TypeCode::U64, false); 3],
        &[0],
    );
    register_entry(&mut registry, 50, schema, RelationKind::BaseTable);
    registry.add_index(50, 999, &[2], false).unwrap();
    assert_eq!(registry.relation(50).unwrap().indexes().len(), 1);

    registry.release_index(50, 999);
    assert_eq!(registry.relation(50).unwrap().indexes().len(), 0);
}

#[test]
fn a_circuit_lives_while_one_claim_remains() {
    let mut registry = solo_registry("idx_claims_owner");
    let schema = SchemaDescriptor::new(
        &[crate::schema::SchemaColumn::new(crate::schema::TypeCode::U64, false); 3],
        &[0],
    );
    register_entry(&mut registry, 50, schema, RelationKind::BaseTable);
    registry.add_index(50, 70, &[2], false).unwrap();
    registry.add_index(50, 71, &[2], true).unwrap();
    let circuit = |r: &RelationRegistry| r.relation(50).unwrap().index_on(&[2]).map(|ix| ix.is_unique());
    assert_eq!(
        registry.relation(50).unwrap().indexes().len(),
        1,
        "one circuit per column list"
    );
    assert_eq!(circuit(&registry), Some(true));

    registry.release_index(50, 71);
    assert_eq!(
        circuit(&registry),
        Some(false),
        "the non-unique claim keeps the circuit"
    );

    registry.release_index(50, 12345);
    registry.release_index(51, 70);
    assert_eq!(circuit(&registry), Some(false), "an unknown claim or owner is a no-op");

    registry.release_index(50, 70);
    assert_eq!(circuit(&registry), None, "the last release drops the circuit");
}

#[test]
fn a_master_creates_no_index_directory() {
    let mut registry = RelationRegistry::master(&relation_test_dir("idx_master_dir"), 1, StoreConfig::default());
    let schema = SchemaDescriptor::new(
        &[crate::schema::SchemaColumn::new(crate::schema::TypeCode::U64, false); 2],
        &[0],
    );
    let owner_dir = register_entry(&mut registry, 50, schema, RelationKind::BaseTable);
    registry.add_index(50, 999, &[1], false).unwrap();
    let index = ChildAddr {
        kind: ChildKind::Index(gnitz_wire::PkColList::from_slice(&[1])),
        slot: Slot::SOLO,
    };
    assert!(!std::path::Path::new(&index.dir(&owner_dir)).exists());
}

/// `UniquePreflight` hands `index_cols` its `arg1` raw, where `HasPk` would
/// have read `0` through `probe_key_columns` as the relation's own PK store.
/// Neither `0` nor a garbage non-zero word names a column list.
#[test]
fn a_flag_clear_arg1_names_no_index() {
    let mut registry = solo_registry("arg1_zero_owner");
    let schema = SchemaDescriptor::new(
        &[crate::schema::SchemaColumn::new(crate::schema::TypeCode::U64, false); 3],
        &[0],
    );
    register_entry(&mut registry, 50, schema, RelationKind::BaseTable);
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
}

/// An index store is rederived, so the base round never visits it; the
/// ephemeral round is what force-persists it, index circuits included.
#[test]
fn ephemeral_flush_includes_index_circuits() {
    let mut registry = solo_registry("flush_ic_owner");
    // A real 2-column owner schema: registration precomputes the circuit's
    // `key_spec` from it, which locates indexed column 1.
    let parent_schema = SchemaDescriptor::new(
        &[crate::schema::SchemaColumn::new(crate::schema::TypeCode::U64, false); 2],
        &[0],
    );
    let owner_dir = register_entry(&mut registry, 70, parent_schema, RelationKind::BaseTable);
    registry.add_index(70, 999, &[1], false).unwrap();

    // Put one row in the index table's memtable.
    {
        let mut batch = Batch::with_capacity(&parent_schema, 1);
        batch.extend_pk(1u128);
        batch.extend_weight(&1i64.to_le_bytes());
        batch.extend_null_bmp(&0u64.to_le_bytes());
        batch.extend_col(0, &7u64.to_le_bytes());
        batch.count += 1;
        let ic = registry.relation_mut(70).and_then(|r| r.index_on_mut(&[1])).unwrap();
        assert!(ic.project_and_ingest(&batch).unwrap(), "the projection is not empty");
    }

    registry.set_resume_generation(1);
    registry.checkpoint_ephemeral([]).unwrap();
    let store_dir = ChildAddr {
        kind: ChildKind::Index(gnitz_wire::PkColList::from_slice(&[1])),
        slot: Slot::SOLO,
    }
    .dir(&owner_dir);
    let shard_count = std::fs::read_dir(&store_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_str().unwrap_or("").starts_with("shard_"))
        .count();
    assert!(
        shard_count > 0,
        "the ephemeral round must publish the index circuit's shard"
    );
}

/// A fed view's delta store takes the epoch output **weights and all**: the
/// round's own net weight per (PK, payload), not a row set. Two rounds against
/// one key — `+1` then `-1` — must both be retained, because the output store
/// folds them to nothing and the feed is the only place they survive.
#[test]
fn a_fed_view_retains_each_round_at_its_own_weight() {
    let mut registry = solo_registry("fed_view_delta");
    let schema = crate::test_support::pk_only_schema(&[crate::schema::TypeCode::U64]);
    let vid = gnitz_wire::FIRST_USER_TABLE_ID as i64;
    registry
        .register(RelationSpec {
            id: vid,
            kind: RelationKind::View(ViewProps::Fed { delta_bytes: 1 << 20 }),
            schema,
        })
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

    let feed = entry.delta().expect("a fed view holds a delta store");
    let stride = feed.schema().pk_stride();
    let mut rounds: Vec<(u64, i64)> = Vec::new();
    let floor = vec![0u8; stride];
    let (mut cur, _) = feed.range_cursor(Some((gnitz_wire::PkBuf::from_bytes(&floor), None)));
    while cur.valid {
        // The delta PK is `round` big-endian, then the view's own PK.
        let tick = u64::from_be_bytes(cur.current_pk_bytes()[..8].try_into().unwrap());
        rounds.push((tick, cur.current_weight));
        cur.advance();
    }
    assert_eq!(rounds, vec![(4, 1), (5, -1)], "each round keeps its own weight");
}

// A storage error while applying committed data is returned rather than
// swallowed, so the process that owns the recovery decision is the one that
// makes it. Driven via the `GNITZ_INJECT_INGEST_APPLY_ERROR` debug seam.
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
    let mut registry = solo_registry("seam_abort");
    let schema = crate::test_support::pk_only_schema(&[crate::schema::TypeCode::U64]);
    let tid = gnitz_wire::FIRST_USER_TABLE_ID as i64;
    register_entry(&mut registry, tid, schema, RelationKind::View(ViewProps::Plain));
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
        "the ingest must return the storage error when the seam is armed",
    );
    println!("{}", crate::test_support::CHILD_OK);
}

/// Not unique, or covering the PK: either one excludes an index.
#[test]
fn a_unique_index_covering_the_pk_has_nothing_left_to_check() {
    let mut registry = solo_registry("unique_to_check");
    let schema = SchemaDescriptor::new(
        &[crate::schema::SchemaColumn::new(crate::schema::TypeCode::U64, false); 3],
        &[0],
    );
    register_entry(&mut registry, 60, schema, RelationKind::BaseTable);
    registry.add_index(60, 901, &[0], true).unwrap(); // exactly the PK
    registry.add_index(60, 902, &[2, 0], true).unwrap(); // the PK plus a payload column
    registry.add_index(60, 903, &[1], true).unwrap(); // the only real check
    registry.add_index(60, 904, &[2], false).unwrap(); // not unique at all

    let r = registry.relation(60).unwrap();
    let cols: Vec<Vec<u32>> = r
        .unique_indexes_to_check()
        .map(|ic| ic.cols().as_slice().to_vec())
        .collect();
    assert_eq!(cols, vec![vec![1u32]]);
}

/// Only a directory named exactly as a relation's is read, and a damaged
/// manifest holds no record.
#[test]
fn persisted_records_reads_well_formed_relation_manifests() {
    let mut registry = solo_registry("persisted_records");
    registry.set_resume_enabled(true);
    let schema = crate::test_support::pk_only_schema(&[crate::schema::TypeCode::U64]);
    let view = RelationKind::View(ViewProps::Plain);
    let (good, damaged) = (123, 124);
    let good_dir = register_entry(&mut registry, good, schema, view);
    let damaged_dir = register_entry(&mut registry, damaged, schema, view);
    registry.set_caller_record(good, b"good".to_vec()).unwrap();
    registry.set_caller_record(damaged, b"damaged".to_vec()).unwrap();
    registry.checkpoint_ephemeral([]).unwrap();

    let rows = ChildAddr { kind: ChildKind::Rows, slot: Slot::SOLO };
    let manifest = rows.manifest(&damaged_dir);
    let mut bytes = std::fs::read(&manifest).unwrap();
    let last = bytes.len() - 1;
    bytes[last] ^= 1;
    std::fs::write(&manifest, bytes).unwrap();
    // A name that parses to `good`'s id but is not the name its directory has.
    let alias = format!("{}/0{good}", relations_dir(&registry.base_dir));
    let alias_rows = rows.dir(&alias);
    std::fs::create_dir_all(&alias_rows).unwrap();
    std::fs::copy(rows.manifest(&good_dir), format!("{alias_rows}/manifest.bin")).unwrap();

    let records: Vec<(i64, Vec<u8>)> = registry
        .persisted_records()
        .unwrap()
        .into_iter()
        .map(|(id, r)| (id, r.unwrap()))
        .collect();
    assert_eq!(records, vec![(good, b"good".to_vec())]);
}

/// `reopen_view` enters a view only when its rows open from the manifest.
#[test]
fn reopen_view_refuses_a_view_with_no_manifest() {
    let mut registry = solo_registry("reopen_view");
    let schema = crate::test_support::pk_only_schema(&[crate::schema::TypeCode::U64]);
    let spec = |id| RelationSpec {
        id,
        kind: RelationKind::View(ViewProps::Plain),
        schema,
    };
    register_entry(&mut registry, 7, schema, RelationKind::View(ViewProps::Plain));
    registry.checkpoint_ephemeral([]).unwrap();
    registry.unregister(7);

    registry.reopen_view(spec(7)).expect("a published view reopens");
    assert!(registry.relation(7).is_some_and(Relation::resumed));
    assert!(registry.reopen_view(spec(8)).is_err());
    assert!(registry.relation(8).is_none(), "a refused view is not entered");
}
