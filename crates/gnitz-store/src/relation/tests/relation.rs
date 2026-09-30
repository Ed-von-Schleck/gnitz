use super::*;
use crate::storage::BatchBuilder;

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
fn register_entry(registry: &mut RelationRegistry, id: u64, schema: SchemaDescriptor, kind: RelationKind) -> String {
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
    registry
        .add_index(50, IndexClaim::Index { id: 999, unique: false }, &[2])
        .unwrap();
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
    registry
        .add_index(50, IndexClaim::Index { id: 70, unique: false }, &[2])
        .unwrap();
    registry
        .add_index(50, IndexClaim::Index { id: 71, unique: true }, &[2])
        .unwrap();
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
    registry
        .add_index(50, IndexClaim::Index { id: 999, unique: false }, &[1])
        .unwrap();
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
    registry
        .add_index(50, IndexClaim::Index { id: 999, unique: false }, &[2])
        .unwrap();

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
    registry
        .add_index(70, IndexClaim::Index { id: 999, unique: false }, &[1])
        .unwrap();

    // Put one row in the index table's memtable.
    {
        let mut batch = BatchBuilder::new(parent_schema);
        batch.begin_row(1u128, 1i64);
        batch.put_int(7);
        batch.end_row();
        let batch = batch.finish();
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
    let tid = gnitz_wire::FIRST_USER_TABLE_ID;
    register_entry(&mut registry, tid, schema, RelationKind::View(ViewProps::Plain));
    let mut batch = BatchBuilder::new(schema);
    batch.begin_row(1u128, 1i64);
    batch.end_row();
    let batch = batch.finish();
    assert!(
        matches!(registry.ingest(tid, batch), Err(e) if e.contains("io error")),
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
    registry
        .add_index(60, IndexClaim::Index { id: 901, unique: true }, &[0])
        .unwrap(); // exactly the PK
    registry
        .add_index(60, IndexClaim::Index { id: 902, unique: true }, &[2, 0])
        .unwrap(); // the PK plus a payload column
    registry
        .add_index(60, IndexClaim::Index { id: 903, unique: true }, &[1])
        .unwrap(); // the only real check
    registry
        .add_index(60, IndexClaim::Index { id: 904, unique: false }, &[2])
        .unwrap(); // not unique at all

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
    std::fs::copy(rows.manifest(&good_dir), rows.manifest(&alias)).unwrap();

    let records: Vec<(u64, Vec<u8>)> = registry
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
    assert!(registry.reopen_view(spec(8)).is_err());
    assert!(registry.relation(8).is_none(), "a refused view is not entered");
}

/// A registered view, its rows and one declared trace `t` published at the
/// resume generation; returns the view's spec and relation directory.
fn published_traced_view(registry: &mut RelationRegistry, id: u64) -> (RelationSpec, String) {
    let schema = crate::test_support::pk_only_schema(&[crate::schema::TypeCode::U64]);
    let spec = RelationSpec {
        id,
        kind: RelationKind::View(ViewProps::Plain),
        schema,
    };
    let dir = register_entry(registry, id, schema, spec.kind);
    let mut layout = StateLayout::default();
    layout.declare("t".to_string(), schema);
    let mut state = CircuitState::open(registry, id, layout).unwrap();
    registry.checkpoint_ephemeral([&mut state]).unwrap();
    (spec, dir)
}

/// A worker told to resume a view whose rows manifest is gone fails its store
/// open.
#[test]
fn open_stores_refuses_a_resumed_view_without_its_rows() {
    let base = relation_test_dir("open_stores_resume");
    let (spec, dir) = published_traced_view(&mut RelationRegistry::new(&base, Slot::SOLO, StoreConfig::default()), 7);
    let open = || {
        let mut master = RelationRegistry::master(&base, 1, StoreConfig::default());
        master.register(spec).unwrap();
        master.reconcile_child_dirs().unwrap();
        master.open_stores(0, Residency::Worker, |_| true).map(drop)
    };
    open().expect("a published view resumes");

    std::fs::remove_file(ChildAddr { kind: ChildKind::Rows, slot: Slot::SOLO }.manifest(&dir)).unwrap();
    assert!(open().is_err());
}

/// A resumed view whose declared trace has no manifest at the generation its
/// rows resumed from refuses to open its operator state.
#[test]
fn circuit_state_refuses_a_resumed_trace_without_its_manifest() {
    let base = relation_test_dir("circuit_state_resume");
    let (spec, dir) = published_traced_view(&mut RelationRegistry::new(&base, Slot::SOLO, StoreConfig::default()), 7);
    let open = || {
        let mut registry = RelationRegistry::new(&base, Slot::SOLO, StoreConfig::default());
        registry.reopen_view(spec).unwrap();
        let mut layout = StateLayout::default();
        layout.declare("t".to_string(), spec.schema);
        CircuitState::open(&registry, spec.id, layout).map(drop)
    };
    open().expect("a published trace resumes");

    let trace = ChildAddr {
        kind: ChildKind::Scratch("t"),
        slot: Slot::SOLO,
    };
    std::fs::remove_file(trace.manifest(&dir)).unwrap();
    assert!(open().is_err());
}
