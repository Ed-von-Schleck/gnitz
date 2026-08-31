use super::*;
use crate::storage::RecoverySource;

/// A scratch directory for one test. `scratch_dir` removes it on entry, so
/// nothing here cleans up on exit: the dirs are small, carry no SAL, and
/// surviving a run is what makes a failure investigable.
fn relation_test_dir(name: &str) -> String {
    crate::test_support::scratch_dir("relation", name)
}

fn make_test_table(name: &str) -> Box<Table> {
    let schema = SchemaDescriptor::minimal_u64();
    let dir = relation_test_dir(name);
    Box::new(Table::new(&dir, schema, 99, RecoverySource::Rederive { resume_at: None }).unwrap())
}

/// Enter `id` over a table the test keeps owning.
///
/// SAFETY: every caller holds its `Box<Table>` in a local that outlives the
/// registry it hands the table to.
fn register_borrowed_entry(
    registry: &mut RelationRegistry,
    id: i64,
    table: &mut Table,
    schema: SchemaDescriptor,
    kind: RelationKind,
) {
    unsafe { registry.register_borrowed(id, table, schema, kind, String::new()) }
}

#[test]
fn test_register_unregister_table() {
    let mut registry = RelationRegistry::new(1);
    let schema = SchemaDescriptor::minimal_u64();
    let mut tbl = make_test_table("reg_unreg");
    register_borrowed_entry(&mut registry, 100, &mut tbl, schema, RelationKind::BaseTable);
    assert!(registry.has_id(100));

    registry.unregister(100);
    assert!(!registry.has_id(100));
}

#[test]
fn test_add_remove_index_circuit() {
    let mut registry = RelationRegistry::new(1);
    // A real 3-column owner schema: registration precomputes the circuit's
    // `key_spec` from it, which locates indexed column 2.
    let schema = SchemaDescriptor::new(
        &[crate::schema::SchemaColumn::new(crate::schema::type_code::U64, 0); 3],
        &[0],
    );
    let mut tbl = make_test_table("idx_parent");
    register_borrowed_entry(&mut registry, 50, &mut tbl, schema, RelationKind::BaseTable);
    let idx_tbl = make_test_table("idx_child");
    registry.add_index_circuit(50, &[2], 999, idx_tbl, schema, false);
    assert_eq!(registry.index_circuits(50).len(), 1);

    registry.remove_index_circuit(50, &[2]);
    assert_eq!(registry.index_circuits(50).len(), 0);
    registry.close();
}

#[test]
fn test_flush_includes_index_circuits() {
    let mut registry = RelationRegistry::new(1);
    // A real 2-column owner schema: registration precomputes the circuit's
    // `key_spec` from it, which locates indexed column 1.
    let parent_schema = SchemaDescriptor::new(
        &[crate::schema::SchemaColumn::new(crate::schema::type_code::U64, 0); 2],
        &[0],
    );
    let mut tbl = make_test_table("flush_ic_parent");
    register_borrowed_entry(&mut registry, 70, &mut tbl, parent_schema, RelationKind::BaseTable);

    // Durable index table: flush writes shard_*.db only if called.
    let idx_schema = SchemaDescriptor::minimal_u64();
    let idx_dir = relation_test_dir("flush_ic_idx");
    let idx_tbl = Box::new(Table::new(&idx_dir, idx_schema, 1, RecoverySource::SalReplay).unwrap());
    registry.add_index_circuit(70, &[1], 999, idx_tbl, idx_schema, false);

    // Put one row in the index table's memtable.
    {
        let entry = registry.table_entry(70).unwrap();
        let mut batch = Batch::with_capacity(idx_schema, 1);
        batch.extend_pk(1u128);
        batch.extend_weight(&1i64.to_le_bytes());
        batch.extend_null_bmp(&0u64.to_le_bytes());
        batch.count += 1;
        entry.index_circuits[0].ingest_owned_batch(batch).unwrap();
    }

    registry.flush(70).unwrap();
    let shard_count = std::fs::read_dir(&idx_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_str().unwrap_or("").starts_with("shard_"))
        .count();
    assert!(shard_count > 0, "index circuit shard must be written by flush");

    registry.close();
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
    let name = "relation::tests::ingest_apply_error_returned_internal";
    let out = crate::test_support::run_test_in_child(name, &[("GNITZ_INJECT_INGEST_APPLY_ERROR", "store")]);
    crate::test_support::assert_child_ok(
        &out,
        "the seam-armed child must return the error rather than swallow it",
    );
}

// Runs only in the re-exec'd child, which is where the armed seam is read.
// Registers a view and ingests one row; the "store" seam substitutes Err for
// the store ingest. `View`, not `BaseTable`: a base table would run
// `enforce_unique_pk` against the fixture's `Borrowed` handle.
#[test]
fn ingest_apply_error_returned_internal() {
    if !crate::test_support::in_child_test() {
        return;
    }
    let mut registry = RelationRegistry::new(1);
    let schema = SchemaDescriptor::minimal_u64();
    let dir = relation_test_dir("seam_abort");
    let mut tbl = Box::new(Table::new(&dir, schema, 99, RecoverySource::Rederive { resume_at: None }).unwrap());
    // A user id: the public ingest entry rejects the system band outright.
    let tid = gnitz_wire::FIRST_USER_TABLE_ID as i64;
    register_borrowed_entry(&mut registry, tid, &mut tbl, schema, RelationKind::View);
    let mut batch = Batch::with_capacity(schema, 1);
    batch.extend_pk(1u128);
    batch.extend_weight(&1i64.to_le_bytes());
    batch.extend_null_bmp(&0u64.to_le_bytes());
    batch.count += 1;
    assert!(
        matches!(
            registry.ingest_returning_effective(tid, batch),
            Err(IngestError::Storage(crate::storage::StorageError::Io(_)))
        ),
        "ingest_store_and_indices must return the storage error when the seam is armed",
    );
    println!("{}", crate::test_support::CHILD_OK);
}
