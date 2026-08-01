use super::*;
use crate::schema::{make_index_schema, IndexKeySpec, MAX_PK_BYTES};

// ── test_index_creation_and_backfill ────────────────────────────────────

#[test]
fn test_index_creation_and_backfill() {
    let dir = temp_dir("index_backfill");
    let mut engine = CatalogEngine::open(&dir).unwrap();

    let cols = vec![col_def("id", type_code::U64), col_def("val", type_code::I64)];
    let tid = engine.create_table("public.tfanout", &cols, &[0]).unwrap();

    // Ingest 5 rows
    let schema = engine.get_schema(tid).unwrap();
    let mut bb = BatchBuilder::new(schema);
    for i in 0..5u64 {
        bb.begin_row(i as u128, 1);
        bb.put_u64(i * 100);
        bb.end_row();
    }
    let batch = bb.finish();
    engine.dag.ingest_to_family(tid, batch);
    let _ = engine.dag.flush(tid);

    // Create index
    let _idx_id = engine.create_index("public.tfanout", &["val"], false).unwrap();
    assert!(engine.has_index_by_name("public__tfanout__idx_val"));

    // Drop index
    engine.drop_index("public__tfanout__idx_val").unwrap();
    assert!(!engine.has_index_by_name("public__tfanout__idx_val"));

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── test_index_live_fanout ────────────────────────────────────────────────

#[test]
fn test_index_live_fanout() {
    let dir = temp_dir("idx_fanout");
    let mut engine = CatalogEngine::open(&dir).unwrap();

    let cols = vec![col_def("id", type_code::U64), col_def("val", type_code::I64)];
    let tid = engine.create_table("public.tfanout", &cols, &[0]).unwrap();
    let schema = engine.get_schema(tid).unwrap();

    // Ingest 5 rows
    let mut bb = BatchBuilder::new(schema);
    for i in 0..5u64 {
        bb.begin_row(i as u128, 1);
        bb.put_u64(i * 100);
        bb.end_row();
    }
    engine.dag.ingest_to_family(tid, bb.finish());
    let _ = engine.dag.flush(tid);

    // Create index — should backfill 5 rows
    engine.create_index("public.tfanout", &["val"], false).unwrap();

    // Live fan-out: ingest 1 more row via ingest_to_family (which does index projection)
    let mut bb2 = BatchBuilder::new(schema);
    bb2.begin_row(99u128, 1);
    bb2.put_u64(777);
    bb2.end_row();
    engine.dag.ingest_to_family(tid, bb2.finish());
    let _ = engine.dag.flush(tid);

    // Verify index has 6 entries via DagEngine's index circuit
    let entry = engine.dag.tables.get_mut(&tid).unwrap();
    assert_eq!(entry.index_circuits.len(), 1, "Expected 1 index circuit");
    let idx_table = entry.index_circuits[0].table_mut();
    let idx_count = count_records(idx_table);
    assert_eq!(idx_count, 6, "Index fanout: expected 6, got {idx_count}");

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_system_table_flush_compacts_l0() {
    // Each flush_family on a system table writes an L0 shard; without the
    // compact_if_needed call they accumulate unbounded. Drive many flushes and
    // assert the shard count stays bounded.
    let dir = temp_dir("sys_compact_l0");
    let mut engine = CatalogEngine::open(&dir).unwrap();
    let schema = SysFamily::Index.schema();
    let flushes = 40u64;
    for i in 0..flushes {
        let mut bb = BatchBuilder::new(schema);
        bb.begin_row(i as u128, 1);
        bb.put_u64(0); // owner_id
        bb.put_u64(0); // owner_kind
        bb.put_u64(0); // source_col_idx
        bb.put_string(&format!("idx{i}"));
        bb.put_u64(0); // is_unique
        bb.put_string(""); // cache_directory
        bb.end_row();
        engine
            .sys_store_mut(SysFamily::Index)
            .ingest_borrowed_batch(&bb.finish())
            .unwrap();
        engine.flush_family(IDX_TAB_ID).unwrap();
    }
    let shards = engine.sys_store_mut(SysFamily::Index).all_shard_arcs().len();
    assert!(
        (shards as u64) < flushes / 2,
        "system catalog L0 must be compacted: {shards} shards after {flushes} flushes"
    );
    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_create_index_duplicate_rejected() {
    // Creating the same index twice must fail on the second attempt rather
    // than silently orphaning the first index circuit.
    let (mut engine, _tid, dir) = table_fixture(
        "idx_dup_create",
        &[col_def("id", type_code::U64), col_def("val", type_code::I64)],
    );

    let first = engine.create_index("public.t", &["val"], false);
    assert!(first.is_ok(), "first index creation should succeed");
    assert!(engine.has_index_by_name("public__t__idx_val"));

    let second = engine.create_index("public.t", &["val"], false);
    assert!(second.is_err(), "duplicate index creation must be rejected");
    assert!(
        second.unwrap_err().contains("already exists"),
        "error must mention the index already exists"
    );

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_unique_index_failure_no_broadcast_poisoning() {
    // A failed UNIQUE index creation (duplicate values) must NOT enqueue a
    // negative-weight IDX_TAB broadcast: the +1 was never broadcast (hook
    // failed before enqueue), so a broadcast −1 would orphan a row on workers.
    let (mut engine, tid, dir) = table_fixture(
        "idx_broadcast_poison",
        &[col_def("id", type_code::U64), col_def("val", type_code::U64)],
    );
    let schema = engine.get_schema(tid).unwrap();

    // Two rows sharing val=42.
    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(1u128, 1);
    bb.put_u64(42);
    bb.end_row();
    bb.begin_row(2u128, 1);
    bb.put_u64(42);
    bb.end_row();
    engine.ingest_to_family(tid, &bb.finish()).unwrap();
    engine.flush_family(tid).unwrap();

    // Clear broadcasts accumulated by table/column creation + ingest.
    let _ = engine.drain_pending_broadcasts();

    // UNIQUE index over duplicate values must fail.
    let res = engine.create_index("public.t", &["val"], true);
    assert!(res.is_err(), "unique index over duplicate values must fail");
    // The rejection must carry the qualified table and column context.
    let err = res.unwrap_err();
    assert!(
        err.contains("public.t"),
        "create-index error should name the table: {err}"
    );
    assert!(err.contains("val"), "create-index error should name the column: {err}");

    // No IDX_TAB broadcast may carry a negative weight for the failed index.
    let broadcasts = engine.drain_pending_broadcasts();
    for (tab, batch) in &broadcasts {
        if *tab == IDX_TAB_ID {
            for i in 0..batch.count {
                assert!(
                    batch.get_weight(i) >= 0,
                    "no negative-weight IDX_TAB broadcast may be enqueued on rollback"
                );
            }
        }
    }

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── validate_unique_indices tests ─────────────────────────────────

#[test]
fn test_validate_unique_indices_duplicate_value() {
    let (mut engine, tid, dir) = table_fixture(
        "catalog_uidx_dup",
        &[col_def("id", type_code::U64), col_def("val", type_code::U64)],
    );
    engine.create_index("public.t", &["val"], true).unwrap(); // unique index on val
    let schema = engine.get_schema(tid).unwrap();

    // Insert first row
    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(1u128, 1);
    bb.put_u64(42);
    bb.end_row();
    engine.ingest_to_family(tid, &bb.finish()).unwrap();
    engine.flush_family(tid).unwrap();

    // Insert second row with same val=42 → should violate unique index
    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(2u128, 1);
    bb.put_u64(42);
    bb.end_row();
    let result = engine.validate_unique_indices(tid, &bb.finish());
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(err.contains("Unique index violation"));
    // The diagnostic must carry the qualified table and column, not a bare column.
    assert!(err.contains("public.t"), "violation should name the table: {err}");
    assert!(err.contains("val"), "violation should name the column: {err}");

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// Same first-line committed-duplicate rejection, but after a
// `backfill_all_indexes` rebuild (the worker-boot path) replaces the index
// Table and re-derives the committed holder from the base slice. The
// committed-holder probe in validate_unique_indices must still see the value.
#[test]
fn test_validate_unique_indices_duplicate_after_backfill_all_rebuild() {
    let (mut engine, tid, dir) = table_fixture(
        "catalog_uidx_dup_after_rebuild",
        &[col_def("id", type_code::U64), col_def("val", type_code::U64)],
    );
    engine.create_index("public.t", &["val"], true).unwrap(); // unique index on val
    let schema = engine.get_schema(tid).unwrap();

    // Commit the holder row (val=42) and flush it to the base shards.
    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(1u128, 1);
    bb.put_u64(42);
    bb.end_row();
    engine.ingest_to_family(tid, &bb.finish()).unwrap();
    engine.flush_family(tid).unwrap();

    // Rebuild every index from its base slice, replacing the fork-inherited copy.
    engine.backfill_all_indexes().unwrap();

    // Insert a second row with the same val=42 → must still violate the unique index.
    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(2u128, 1);
    bb.put_u64(42);
    bb.end_row();
    let result = engine.validate_unique_indices(tid, &bb.finish());
    assert!(result.is_err(), "committed duplicate must be rejected after rebuild");
    assert!(result.unwrap_err().contains("Unique index violation"));

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_validate_unique_indices_batch_internal_dup() {
    let (mut engine, tid, dir) = table_fixture(
        "catalog_uidx_batch_dup",
        &[col_def("id", type_code::U64), col_def("val", type_code::U64)],
    );
    engine.create_index("public.t", &["val"], true).unwrap();
    let schema = engine.get_schema(tid).unwrap();

    // Single batch with two rows sharing val=99 → duplicate in batch
    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(1u128, 1);
    bb.put_u64(99);
    bb.end_row();
    bb.begin_row(2u128, 1);
    bb.put_u64(99);
    bb.end_row();
    let result = engine.validate_unique_indices(tid, &bb.finish());
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("duplicate in batch"));

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_validate_unique_indices_upsert_same_value_ok() {
    let (mut engine, tid, dir) = table_fixture(
        "catalog_uidx_upsert_same",
        &[col_def("id", type_code::U64), col_def("val", type_code::U64)],
    );
    engine.create_index("public.t", &["val"], true).unwrap();
    let schema = engine.get_schema(tid).unwrap();

    // Insert PK=1, val=42
    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(1u128, 1);
    bb.put_u64(42);
    bb.end_row();
    engine.ingest_to_family(tid, &bb.finish()).unwrap();
    engine.flush_family(tid).unwrap();

    // UPSERT PK=1, val=42 (same value) → should succeed
    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(1u128, 1);
    bb.put_u64(42);
    bb.end_row();
    let result = engine.validate_unique_indices(tid, &bb.finish());
    assert!(result.is_ok());

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_validate_unique_indices_upsert_to_existing_value_fails() {
    let (mut engine, tid, dir) = table_fixture(
        "catalog_uidx_upsert_conflict",
        &[col_def("id", type_code::U64), col_def("val", type_code::U64)],
    );
    engine.create_index("public.t", &["val"], true).unwrap();
    let schema = engine.get_schema(tid).unwrap();

    // Insert PK=1, val=42 and PK=2, val=99
    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(1u128, 1);
    bb.put_u64(42);
    bb.end_row();
    bb.begin_row(2u128, 1);
    bb.put_u64(99);
    bb.end_row();
    engine.ingest_to_family(tid, &bb.finish()).unwrap();
    engine.flush_family(tid).unwrap();

    // UPSERT PK=1, val=99 → conflicts with PK=2's val=99
    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(1u128, 1);
    bb.put_u64(99);
    bb.end_row();
    let result = engine.validate_unique_indices(tid, &bb.finish());
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("Unique index violation"));

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_validate_unique_indices_distinct_values_ok() {
    let (mut engine, tid, dir) = table_fixture(
        "catalog_uidx_distinct",
        &[col_def("id", type_code::U64), col_def("val", type_code::U64)],
    );
    engine.create_index("public.t", &["val"], true).unwrap();
    let schema = engine.get_schema(tid).unwrap();

    // Insert row with val=42
    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(1u128, 1);
    bb.put_u64(42);
    bb.end_row();
    engine.ingest_to_family(tid, &bb.finish()).unwrap();
    engine.flush_family(tid).unwrap();

    // Insert row with different val=99 → should succeed
    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(2u128, 1);
    bb.put_u64(99);
    bb.end_row();
    let result = engine.validate_unique_indices(tid, &bb.finish());
    assert!(result.is_ok());

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── atomic transfer / swap / bulk-shift validation tests ─────────────────
//
// A single delta batch may rearrange unique values among rows so the
// post-batch state is unique, even though validation runs pre-apply against
// committed storage. These lock in the exemption rule: a committed collision is
// exempt only when the holder releases the value in this batch (explicit
// retraction of its (PK, value), or — for an upsert row — the holder being
// itself an upserted PK).

#[test]
fn test_validate_unique_transfer_accepted() {
    let (mut engine, tid, dir) = table_fixture(
        "catalog_uidx_transfer",
        &[col_def("id", type_code::U64), col_def("val", type_code::U64)],
    );
    engine.create_index("public.t", &["val"], true).unwrap();
    let schema = engine.get_schema(tid).unwrap();

    // Committed: P1/val=5.
    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(1u128, 1);
    bb.put_u64(5);
    bb.end_row();
    engine.ingest_to_family(tid, &bb.finish()).unwrap();
    engine.flush_family(tid).unwrap();

    // Transfer val=5 from P1 to P2 in one batch: {retract P1/5, insert P2/5}.
    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(1u128, -1);
    bb.put_u64(5);
    bb.end_row();
    bb.begin_row(2u128, 1);
    bb.put_u64(5);
    bb.end_row();
    let result = engine.validate_unique_indices(tid, &bb.finish());
    assert!(result.is_ok(), "transfer should be accepted: {result:?}");

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_validate_unique_swap_accepted() {
    let (mut engine, tid, dir) = table_fixture(
        "catalog_uidx_swap",
        &[col_def("id", type_code::U64), col_def("val", type_code::U64)],
    );
    engine.create_index("public.t", &["val"], true).unwrap();
    let schema = engine.get_schema(tid).unwrap();

    // Committed: P1/val=5, P2/val=6.
    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(1u128, 1);
    bb.put_u64(5);
    bb.end_row();
    bb.begin_row(2u128, 1);
    bb.put_u64(6);
    bb.end_row();
    engine.ingest_to_family(tid, &bb.finish()).unwrap();
    engine.flush_family(tid).unwrap();

    // Explicit swap: retract both, re-insert crossed.
    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(1u128, -1);
    bb.put_u64(5);
    bb.end_row();
    bb.begin_row(2u128, -1);
    bb.put_u64(6);
    bb.end_row();
    bb.begin_row(1u128, 1);
    bb.put_u64(6);
    bb.end_row();
    bb.begin_row(2u128, 1);
    bb.put_u64(5);
    bb.end_row();
    let result = engine.validate_unique_indices(tid, &bb.finish());
    assert!(result.is_ok(), "swap should be accepted: {result:?}");

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_validate_unique_bulk_shift_accepted() {
    let (mut engine, tid, dir) = table_fixture(
        "catalog_uidx_bulk_shift",
        &[col_def("id", type_code::U64), col_def("val", type_code::U64)],
    );
    engine.create_index("public.t", &["val"], true).unwrap();
    let schema = engine.get_schema(tid).unwrap();

    // Committed dense sequence: (1,1),(2,2),(3,3).
    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(1u128, 1);
    bb.put_u64(1);
    bb.end_row();
    bb.begin_row(2u128, 1);
    bb.put_u64(2);
    bb.end_row();
    bb.begin_row(3u128, 1);
    bb.put_u64(3);
    bb.end_row();
    engine.ingest_to_family(tid, &bb.finish()).unwrap();
    engine.flush_family(tid).unwrap();

    // `UPDATE t SET val = val + 1`: same-PK +1 upserts, no retraction. The holder
    // of each new value is the previous row, itself upserted off that value.
    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(1u128, 1);
    bb.put_u64(2);
    bb.end_row();
    bb.begin_row(2u128, 1);
    bb.put_u64(3);
    bb.end_row();
    bb.begin_row(3u128, 1);
    bb.put_u64(4);
    bb.end_row();
    let result = engine.validate_unique_indices(tid, &bb.finish());
    assert!(result.is_ok(), "bulk shift should be accepted: {result:?}");

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_validate_unique_bulk_swap_accepted() {
    let (mut engine, tid, dir) = table_fixture(
        "catalog_uidx_bulk_swap",
        &[col_def("id", type_code::U64), col_def("val", type_code::U64)],
    );
    engine.create_index("public.t", &["val"], true).unwrap();
    let schema = engine.get_schema(tid).unwrap();

    // Committed: (1,1),(2,2).
    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(1u128, 1);
    bb.put_u64(1);
    bb.end_row();
    bb.begin_row(2u128, 1);
    bb.put_u64(2);
    bb.end_row();
    engine.ingest_to_family(tid, &bb.finish()).unwrap();
    engine.flush_family(tid).unwrap();

    // `UPDATE t SET val = 3 - val`: same-PK +1 upserts swapping the two values.
    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(1u128, 1);
    bb.put_u64(2);
    bb.end_row();
    bb.begin_row(2u128, 1);
    bb.put_u64(1);
    bb.end_row();
    let result = engine.validate_unique_indices(tid, &bb.finish());
    assert!(result.is_ok(), "bulk swap should be accepted: {result:?}");

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_validate_unique_genuine_duplicate_rejected() {
    let (mut engine, tid, dir) = table_fixture(
        "catalog_uidx_genuine_dup",
        &[col_def("id", type_code::U64), col_def("val", type_code::U64)],
    );
    engine.create_index("public.t", &["val"], true).unwrap();
    let schema = engine.get_schema(tid).unwrap();

    // Committed: P1/val=5.
    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(1u128, 1);
    bb.put_u64(5);
    bb.end_row();
    engine.ingest_to_family(tid, &bb.finish()).unwrap();
    engine.flush_family(tid).unwrap();

    // P2/val=5 with no retraction freeing 5 → genuine duplicate.
    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(2u128, 1);
    bb.put_u64(5);
    bb.end_row();
    let result = engine.validate_unique_indices(tid, &bb.finish());
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("Unique index violation"));

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_validate_unique_holder_touched_value_not_retracted_rejected() {
    let (mut engine, tid, dir) = table_fixture(
        "catalog_uidx_holder_touched",
        &[col_def("id", type_code::U64), col_def("val", type_code::U64)],
    );
    engine.create_index("public.t", &["val"], true).unwrap();
    let schema = engine.get_schema(tid).unwrap();

    // Committed: P1/val=5.
    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(1u128, 1);
    bb.put_u64(5);
    bb.end_row();
    engine.ingest_to_family(tid, &bb.finish()).unwrap();
    engine.flush_family(tid).unwrap();

    // {P1/val=7, P2/val=5}: P1 is touched but val=5 is NOT retracted, and P2 is a
    // fresh PK — not an upsert, so it cannot ride P1's implicit retraction and
    // P2/5 is a genuine dup.
    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(1u128, 1);
    bb.put_u64(7);
    bb.end_row();
    bb.begin_row(2u128, 1);
    bb.put_u64(5);
    bb.end_row();
    let result = engine.validate_unique_indices(tid, &bb.finish());
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("Unique index violation"));

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_validate_unique_forged_retraction_rejected() {
    let (mut engine, tid, dir) = table_fixture(
        "catalog_uidx_forged_retraction",
        &[col_def("id", type_code::U64), col_def("val", type_code::U64)],
    );
    engine.create_index("public.t", &["val"], true).unwrap();
    let schema = engine.get_schema(tid).unwrap();

    // Committed: P1/val=5.
    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(1u128, 1);
    bb.put_u64(5);
    bb.end_row();
    engine.ingest_to_family(tid, &bb.finish()).unwrap();
    engine.flush_family(tid).unwrap();

    // {retract P3/5, insert P2/5}: the retraction names P3, which does NOT hold
    // val=5 (P1 does). The exemption keys on (holder PK, value), so it does not
    // fire and P2/5 is rejected.
    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(3u128, -1);
    bb.put_u64(5);
    bb.end_row();
    bb.begin_row(2u128, 1);
    bb.put_u64(5);
    bb.end_row();
    let result = engine.validate_unique_indices(tid, &bb.finish());
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("Unique index violation"));

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_validate_unique_forged_retraction_freed_by_upserted_holder_rejected() {
    let (mut engine, tid, dir) = table_fixture(
        "catalog_uidx_forged_upserted",
        &[col_def("id", type_code::U64), col_def("val", type_code::U64)],
    );
    engine.create_index("public.t", &["val"], true).unwrap();
    let schema = engine.get_schema(tid).unwrap();

    // Committed: P2/val=6.
    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(2u128, 1);
    bb.put_u64(6);
    bb.end_row();
    engine.ingest_to_family(tid, &bb.finish()).unwrap();
    engine.flush_family(tid).unwrap();

    // {P2/val=7, P4/val=6, retract P3/6}: P4/6 is a FRESH insert (not an upsert)
    // routed to the verify by 6 ∈ retracted_vals. Holder P2 is upserted (to 7) but
    // the is_upsert gate withholds the implicit exemption from the fresh P4, and
    // the retraction names P3 not P2, so neither exemption fires.
    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(2u128, 1);
    bb.put_u64(7);
    bb.end_row();
    bb.begin_row(4u128, 1);
    bb.put_u64(6);
    bb.end_row();
    bb.begin_row(3u128, -1);
    bb.put_u64(6);
    bb.end_row();
    let result = engine.validate_unique_indices(tid, &bb.finish());
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("Unique index violation"));

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_validate_unique_net_zero_pk_not_upsert_rejected() {
    let (mut engine, tid, dir) = table_fixture(
        "catalog_uidx_net_zero",
        &[col_def("id", type_code::U64), col_def("val", type_code::U64)],
    );
    engine.create_index("public.t", &["val"], true).unwrap();
    let schema = engine.get_schema(tid).unwrap();

    // Committed: (1,5),(2,6).
    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(1u128, 1);
    bb.put_u64(5);
    bb.end_row();
    bb.begin_row(2u128, 1);
    bb.put_u64(6);
    bb.end_row();
    engine.ingest_to_family(tid, &bb.finish()).unwrap();
    engine.flush_family(tid).unwrap();

    // {P1/val=6, retract P1/5, P2/val=7}: P1 carries both a +1 and a -1 (net 0),
    // so it is not net-positive and not an upsert. P1/6 then collides with P2's
    // committed val=6, freed only implicitly — rejected.
    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(1u128, 1);
    bb.put_u64(6);
    bb.end_row();
    bb.begin_row(1u128, -1);
    bb.put_u64(5);
    bb.end_row();
    bb.begin_row(2u128, 1);
    bb.put_u64(7);
    bb.end_row();
    let result = engine.validate_unique_indices(tid, &bb.finish());
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("Unique index violation"));

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_validate_unique_two_insert_same_value_rejected_despite_retraction() {
    let (mut engine, tid, dir) = table_fixture(
        "catalog_uidx_two_insert_dup",
        &[col_def("id", type_code::U64), col_def("val", type_code::U64)],
    );
    engine.create_index("public.t", &["val"], true).unwrap();
    let schema = engine.get_schema(tid).unwrap();

    // {retract P9/7, insert P1/5, insert P2/5}: the two +1 rows sharing val=5 trip
    // the `seen` set before any exemption is consulted.
    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(9u128, -1);
    bb.put_u64(7);
    bb.end_row();
    bb.begin_row(1u128, 1);
    bb.put_u64(5);
    bb.end_row();
    bb.begin_row(2u128, 1);
    bb.put_u64(5);
    bb.end_row();
    let result = engine.validate_unique_indices(tid, &bb.finish());
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("duplicate in batch"));

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── seek_by_index tests ──────────────────────────────────────────

#[test]
fn test_seek_by_index_found() {
    let (mut engine, tid, dir) = table_fixture(
        "catalog_seekidx_found",
        &[col_def("id", type_code::U64), col_def("val", type_code::U64)],
    );
    engine.create_index("public.t", &["val"], false).unwrap();
    let schema = engine.get_schema(tid).unwrap();

    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(10u128, 1);
    bb.put_u64(100);
    bb.end_row();
    bb.begin_row(20u128, 1);
    bb.put_u64(200);
    bb.end_row();
    bb.begin_row(30u128, 1);
    bb.put_u64(300);
    bb.end_row();
    engine.ingest_to_family(tid, &bb.finish()).unwrap();
    engine.flush_family(tid).unwrap();

    // Seek by index: val=200 → should find PK=20
    let result = engine.seek_by_index(tid, &[1], &[200u128]).unwrap().0;
    assert!(result.is_some());
    let row = result.unwrap();
    assert_eq!(row.count, 1);
    assert_eq!(row.get_pk(0), 20);

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_seek_by_index_not_found() {
    let (mut engine, tid, dir) = table_fixture(
        "catalog_seekidx_miss",
        &[col_def("id", type_code::U64), col_def("val", type_code::U64)],
    );
    engine.create_index("public.t", &["val"], false).unwrap();
    let schema = engine.get_schema(tid).unwrap();

    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(1u128, 1);
    bb.put_u64(42);
    bb.end_row();
    engine.ingest_to_family(tid, &bb.finish()).unwrap();
    engine.flush_family(tid).unwrap();

    // Seek by index: val=999 → should return None
    let result = engine.seek_by_index(tid, &[1], &[999u128]).unwrap().0;
    assert!(result.is_none());

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── Regression: negative I64 index key storage and retrieval ────────

#[test]
fn test_seek_by_index_negative_i64() {
    let (mut engine, tid, dir) = table_fixture(
        "catalog_seekidx_neg_i64",
        &[col_def("id", type_code::U64), col_def("score", type_code::I64)],
    );
    engine.create_index("public.t", &["score"], false).unwrap();
    let schema = engine.get_schema(tid).unwrap();

    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(1u128, 1);
    bb.put_u64((-5i64) as u64);
    bb.end_row();
    bb.begin_row(2u128, 1);
    bb.put_u64((-1i64) as u64);
    bb.end_row();
    bb.begin_row(3u128, 1);
    bb.put_u64(0);
    bb.end_row();
    bb.begin_row(4u128, 1);
    bb.put_u64(10);
    bb.end_row();
    engine.ingest_to_family(tid, &bb.finish()).unwrap();
    engine.flush_family(tid).unwrap();

    // The seek key is the value's native bit pattern (2's complement); the index
    // stores it order-preserving (signed I64 OPK) and the seek re-encodes
    // identically, so an equality lookup finds it regardless of sign.
    let result = engine.seek_by_index(tid, &[1], &[(-1i64) as u64 as u128]).unwrap().0;
    assert!(result.is_some(), "index must find row with score=-1");
    assert_eq!(result.unwrap().get_pk(0), 2);

    let result2 = engine.seek_by_index(tid, &[1], &[(-5i64) as u64 as u128]).unwrap().0;
    assert!(result2.is_some(), "index must find row with score=-5");
    assert_eq!(result2.unwrap().get_pk(0), 1);

    let result3 = engine.seek_by_index(tid, &[1], &[10u128]).unwrap().0;
    assert!(result3.is_some(), "index must still find positive values");
    assert_eq!(result3.unwrap().get_pk(0), 4);

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── Regression: negative I32 index key storage and retrieval ────────

#[test]
fn test_seek_by_index_negative_i32() {
    let (mut engine, tid, dir) = table_fixture(
        "catalog_seekidx_neg_i32",
        &[col_def("id", type_code::U64), col_def("score", type_code::I32)],
    );
    engine.create_index("public.t", &["score"], false).unwrap();
    let schema = engine.get_schema(tid).unwrap();

    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(1u128, 1);
    bb.put_u32((-1i32) as u32);
    bb.end_row();
    bb.begin_row(2u128, 1);
    bb.put_u32((-100i32) as u32);
    bb.end_row();
    bb.begin_row(3u128, 1);
    bb.put_u32(42u32);
    bb.end_row();
    engine.ingest_to_family(tid, &bb.finish()).unwrap();
    engine.flush_family(tid).unwrap();

    // The seek key is the I32 value's zero-extended native bit pattern; projection
    // and seek both sign-extend it from I32 into the promoted signed I64 index
    // column, so the equality lookup matches.
    let result = engine.seek_by_index(tid, &[1], &[(-1i32) as u32 as u128]).unwrap().0;
    assert!(result.is_some(), "index must find row with score=-1");
    assert_eq!(result.unwrap().get_pk(0), 1);

    let result2 = engine.seek_by_index(tid, &[1], &[(-100i32) as u32 as u128]).unwrap().0;
    assert!(result2.is_some(), "index must find row with score=-100");
    assert_eq!(result2.unwrap().get_pk(0), 2);

    let result3 = engine.seek_by_index(tid, &[1], &[42u128]).unwrap().0;
    assert!(result3.is_some(), "index must still find positive values");
    assert_eq!(result3.unwrap().get_pk(0), 3);

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── Regression: small-column index projection (bug #1) ────────────

#[test]
fn test_seek_by_index_u8_column() {
    let (mut engine, tid, dir) = table_fixture(
        "catalog_seekidx_u8",
        &[col_def("id", type_code::U64), col_def("tag", type_code::U8)],
    );
    engine.create_index("public.t", &["tag"], false).unwrap();
    let schema = engine.get_schema(tid).unwrap();

    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(10u128, 1);
    bb.put_u8(42);
    bb.end_row();
    bb.begin_row(20u128, 1);
    bb.put_u8(99);
    bb.end_row();
    engine.ingest_to_family(tid, &bb.finish()).unwrap();
    engine.flush_family(tid).unwrap();

    let result = engine.seek_by_index(tid, &[1], &[42u128]).unwrap().0;
    assert!(result.is_some(), "U8 index lookup must find the row");
    assert_eq!(result.unwrap().get_pk(0), 10);

    let result2 = engine.seek_by_index(tid, &[1], &[99u128]).unwrap().0;
    assert!(result2.is_some());
    assert_eq!(result2.unwrap().get_pk(0), 20);

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_seek_by_index_u16_column() {
    let (mut engine, tid, dir) = table_fixture(
        "catalog_seekidx_u16",
        &[col_def("id", type_code::U64), col_def("port", type_code::U16)],
    );
    engine.create_index("public.t", &["port"], false).unwrap();
    let schema = engine.get_schema(tid).unwrap();

    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(1u128, 1);
    bb.put_u16(8080);
    bb.end_row();
    bb.begin_row(2u128, 1);
    bb.put_u16(443);
    bb.end_row();
    engine.ingest_to_family(tid, &bb.finish()).unwrap();
    engine.flush_family(tid).unwrap();

    let result = engine.seek_by_index(tid, &[1], &[443u128]).unwrap().0;
    assert!(result.is_some(), "U16 index lookup must find the row");
    assert_eq!(result.unwrap().get_pk(0), 2);

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_drop_table_cleans_up_indices() {
    let dir = temp_dir("drop_table_idx");
    let mut engine = CatalogEngine::open(&dir).unwrap();

    let cols = vec![col_def("pk", type_code::U64), col_def("val", type_code::I64)];
    let tid = engine.create_table("public.idx_tbl", &cols, &[0]).unwrap();
    engine.create_index("public.idx_tbl", &["val"], true).unwrap();

    assert!(engine.dag.tables.contains_key(&tid));
    assert_eq!(engine.dag.tables.get(&tid).unwrap().index_circuits.len(), 1);

    // Drop index then table
    let idx_name = make_secondary_index_name("public", "idx_tbl", "val");
    engine.drop_index(&idx_name).unwrap();
    assert_eq!(engine.dag.tables.get(&tid).unwrap().index_circuits.len(), 0);

    engine.drop_table("public.idx_tbl").unwrap();
    assert!(!engine.dag.tables.contains_key(&tid));

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── Regression: drop_table must cascade to owned indices ─────────────
// Bug 1.3: drop_table used to only retract sys_tables + sys_columns,
// leaving sys_indices rows whose owner table no longer existed.  On the
// next restart, replay_catalog would fail with "Index: owner table N
// not found", leaving the database un-openable.

#[test]
fn test_drop_table_cascades_secondary_index() {
    let dir = temp_dir("drop_table_cascade_sec");
    let mut engine = CatalogEngine::open(&dir).unwrap();

    let cols = vec![col_def("pk", type_code::U64), col_def("val", type_code::I64)];
    let tid = engine.create_table("public.cascade_tbl", &cols, &[0]).unwrap();
    engine.create_index("public.cascade_tbl", &["val"], false).unwrap();

    let idx_name = make_secondary_index_name("public", "cascade_tbl", "val");
    assert!(engine.caches.index_by_name.contains_key(idx_name.as_str()));
    let idx_records_before = count_records(engine.sys_store_mut(SysFamily::Index));
    assert!(idx_records_before >= 1);

    // Drop the table WITHOUT dropping the index first.
    engine.drop_table("public.cascade_tbl").unwrap();

    assert!(
        !engine.caches.index_by_name.contains_key(idx_name.as_str()),
        "in-memory index registry must forget the index"
    );
    assert!(!engine.dag.tables.contains_key(&tid));
    assert_eq!(
        count_records(engine.sys_store_mut(SysFamily::Index)),
        idx_records_before - 1,
        "sys_indices must retract exactly one record"
    );

    // Reopen: before the fix, replay_catalog would fail here because
    // the orphaned sys_indices row references tid which no longer exists.
    engine.close();
    drop(engine);
    let mut engine2 = CatalogEngine::open(&dir).unwrap();
    assert!(!engine2.caches.index_by_name.contains_key(idx_name.as_str()));
    engine2.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_drop_table_cascades_fk_index() {
    let dir = temp_dir("drop_table_cascade_fk");
    let mut engine = CatalogEngine::open(&dir).unwrap();

    let parent_cols = vec![col_def("pk", type_code::U64), col_def("name", type_code::U64)];
    let parent_tid = engine.create_table("public.parent", &parent_cols, &[0]).unwrap();

    let child_cols = vec![
        col_def("pk", type_code::U64),
        ColumnDef {
            name: "parent_ref".into(),
            type_code: type_code::U64,
            is_nullable: false,
            fk_table_id: parent_tid,
            fk_col_idx: 0,
            is_hidden: false,
        },
    ];
    engine.create_table("public.child", &child_cols, &[0]).unwrap();

    let fk_idx_name = make_fk_index_name("public", "child", "parent_ref");
    assert!(
        engine.caches.index_by_name.contains_key(fk_idx_name.as_str()),
        "FK index must be auto-created with the child table"
    );

    // drop_index refuses to drop __fk_ indices, so drop_table is the only
    // valid path for this cleanup. Before the fix: the on_index_delta
    // hook's __fk_ guard blocked drop_table from cascading.
    engine.drop_table("public.child").unwrap();
    assert!(
        !engine.caches.index_by_name.contains_key(fk_idx_name.as_str()),
        "FK index must be removed by drop_table cascade"
    );

    // Reopen catalog — must succeed.
    engine.close();
    drop(engine);
    let mut engine2 = CatalogEngine::open(&dir).unwrap();
    assert!(!engine2.caches.index_by_name.contains_key(fk_idx_name.as_str()));
    engine2.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── Regression: indices_by_owner cache drives cascade_retract_indices ──
// drop_table must cascade-retract ALL owned indices. With the O(N) full-scan
// implementation this always worked, but the cache-based implementation must
// correctly track every idx_id under the same owner and emit one retraction
// per index via retract_single_row.

#[test]
fn test_drop_table_cascades_multiple_indices() {
    let (mut engine, tid, dir) = table_fixture(
        "cascade_multi_idx",
        &[
            col_def("id", type_code::U64),
            col_def("val1", type_code::I64),
            col_def("val2", type_code::I64),
        ],
    );

    let idx1 = engine.create_index("public.t", &["val1"], false).unwrap();
    let idx2 = engine.create_index("public.t", &["val2"], false).unwrap();

    // Both indices tracked in cache
    assert_eq!(
        engine.caches.indices_by_owner.get(&tid).map(|v| v.len()),
        Some(2),
        "indices_by_owner must track both indices"
    );
    assert!(engine.caches.index_by_id.contains_key(&idx1));
    assert!(engine.caches.index_by_id.contains_key(&idx2));

    let idx_count_before = count_records(engine.sys_store_mut(SysFamily::Index));
    assert!(idx_count_before >= 2);

    // Drop the table — cascade must retract both indices
    engine.drop_table("public.t").unwrap();

    assert!(!engine.dag.tables.contains_key(&tid), "table DAG entry must be gone");
    assert!(
        !engine.caches.index_by_id.contains_key(&idx1),
        "idx1 must be removed from index_by_id"
    );
    assert!(
        !engine.caches.index_by_id.contains_key(&idx2),
        "idx2 must be removed from index_by_id"
    );
    assert!(
        engine
            .caches
            .indices_by_owner
            .get(&tid)
            .map(|v| v.is_empty())
            .unwrap_or(true),
        "indices_by_owner for dropped table must be empty"
    );
    assert_eq!(
        count_records(engine.sys_store_mut(SysFamily::Index)),
        idx_count_before - 2,
        "sys_indices must retract exactly two records"
    );

    // Reopen: replay must succeed without orphaned sys_indices rows
    engine.close();
    drop(engine);
    let mut engine2 = CatalogEngine::open(&dir).unwrap();
    assert!(!engine2.caches.index_by_id.contains_key(&idx1));
    assert!(!engine2.caches.index_by_id.contains_key(&idx2));
    engine2.close();

    let _ = fs::remove_dir_all(&dir);
}

// ── Regression: create_index rollback must clean up all caches ────────
// If hook_index_register fails (e.g. unique index on a column with
// duplicate values), the earlier apply_index_by_{name,id} hooks have
// already mutated the caches. The rollback must reverse those writes
// or the name/id caches end up pointing at a ghost index.

// ── Compound-PK source: secondary index ──────────────────────────────
//
// Sanity coverage for the compound-PK index path: build a source table
// with two PK columns, register a secondary index on a non-PK column,
// ingest rows, and check that `seek_by_index` returns the expected
// source rows. Retract one row and verify the index reports it gone.

#[test]
fn test_compound_pk_secondary_index_seek() {
    let dir = temp_dir("compound_pk_idx_seek");
    let mut engine = CatalogEngine::open(&dir).unwrap();

    // Compound PK = (a: U32, b: U32), payload = val: U64.
    // Source PK stride = 8 → index PK stride = 8 (promoted U64) + 8 = 16,
    // which keeps the index cursor on the narrow-PK fast path.
    let cols = vec![
        ColumnDef {
            name: "a".into(),
            type_code: type_code::U32,
            is_nullable: false,
            fk_table_id: 0,
            fk_col_idx: 0,
            is_hidden: false,
        },
        ColumnDef {
            name: "b".into(),
            type_code: type_code::U32,
            is_nullable: false,
            fk_table_id: 0,
            fk_col_idx: 0,
            is_hidden: false,
        },
        col_def("val", type_code::U64),
    ];
    let tid = engine.create_table("public.cpk_t", &cols, &[0, 1]).unwrap();
    engine.create_index("public.cpk_t", &["val"], false).unwrap();
    let schema = engine.get_schema(tid).unwrap();
    assert_eq!(schema.pk_stride(), 8, "compound (U32,U32) PK stride should be 8");

    let mut b = Batch::with_capacity(schema, 4);
    let rows: &[(u32, u32, u64)] = &[
        (10, 1, 100),
        (20, 1, 200),
        (10, 2, 300), // same a as row 0, different b
        (40, 1, 200), // same val as row 1, different PK
    ];
    for &(a, bcol, val) in rows {
        // `extend_pk_bytes` takes the OPK image verbatim, so a compound PK must
        // be built through `extend_pk_opk` — a native-LE concatenation is not the
        // at-rest form (§6) and would ingest a PK region the engine forbids.
        b.extend_pk_opk(&schema, &[a as u128, bcol as u128]);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &val.to_le_bytes());
        b.count += 1;
    }
    engine.ingest_to_family(tid, &b).unwrap();
    engine.flush_family(tid).unwrap();

    // val=100 → exactly one match
    let r = engine.seek_by_index(tid, &[2], &[100u128]).unwrap().0;
    assert!(r.is_some(), "val=100 should find a row");
    assert_eq!(r.unwrap().count, 1);

    // val=300 → one match (a=10, b=2)
    let r = engine.seek_by_index(tid, &[2], &[300u128]).unwrap().0;
    assert!(r.is_some(), "val=300 should find a row");

    // val=999 → miss
    let r = engine.seek_by_index(tid, &[2], &[999u128]).unwrap().0;
    assert!(r.is_none(), "val=999 should miss");

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_compound_pk_secondary_index_retract() {
    let dir = temp_dir("compound_pk_idx_retract");
    let mut engine = CatalogEngine::open(&dir).unwrap();

    let cols = vec![
        ColumnDef {
            name: "a".into(),
            type_code: type_code::U32,
            is_nullable: false,
            fk_table_id: 0,
            fk_col_idx: 0,
            is_hidden: false,
        },
        ColumnDef {
            name: "b".into(),
            type_code: type_code::U32,
            is_nullable: false,
            fk_table_id: 0,
            fk_col_idx: 0,
            is_hidden: false,
        },
        col_def("val", type_code::U64),
    ];
    let tid = engine.create_table("public.cpk_r", &cols, &[0, 1]).unwrap();
    engine.create_index("public.cpk_r", &["val"], false).unwrap();
    let schema = engine.get_schema(tid).unwrap();

    let mut b = Batch::with_capacity(schema, 2);
    b.extend_pk_opk(&schema, &[7, 3]);
    b.extend_weight(&1i64.to_le_bytes());
    b.extend_null_bmp(&0u64.to_le_bytes());
    b.extend_col(0, &500u64.to_le_bytes());
    b.count += 1;
    engine.ingest_to_family(tid, &b).unwrap();
    engine.flush_family(tid).unwrap();

    assert!(engine.seek_by_index(tid, &[2], &[500u128]).unwrap().0.is_some());

    // Retract the same row.
    let mut r = Batch::with_capacity(schema, 1);
    r.extend_pk_opk(&schema, &[7, 3]);
    r.extend_weight(&(-1i64).to_le_bytes());
    r.extend_null_bmp(&0u64.to_le_bytes());
    r.extend_col(0, &500u64.to_le_bytes());
    r.count += 1;
    engine.ingest_to_family(tid, &r).unwrap();
    engine.flush_family(tid).unwrap();

    assert!(
        engine.seek_by_index(tid, &[2], &[500u128]).unwrap().0.is_none(),
        "after retraction the indexed value must not resolve to a row"
    );

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_create_unique_index_duplicate_rolls_back_cleanly() {
    let (mut engine, tid, dir) = table_fixture(
        "unique_idx_rollback",
        &[col_def("id", type_code::U64), col_def("val", type_code::U64)],
    );
    let schema = engine.get_schema(tid).unwrap();

    // Seed duplicate values on `val` so a unique index over it cannot be built.
    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(1u128, 1);
    bb.put_u64(42);
    bb.end_row();
    bb.begin_row(2u128, 1);
    bb.put_u64(42);
    bb.end_row();
    engine.ingest_to_family(tid, &bb.finish()).unwrap();
    engine.flush_family(tid).unwrap();

    let idx_name = make_secondary_index_name("public", "t", "val");
    let idx_records_before = count_records(engine.sys_store_mut(SysFamily::Index));

    // Attempt should fail because of duplicate values.
    let result = engine.create_index("public.t", &["val"], true);
    assert!(result.is_err(), "unique index over duplicates must fail");

    // All catalog-visible state must have reverted: the ghost name/id
    // entries created by apply_index_by_{name,id} are the thing the
    // rollback is responsible for sweeping up.
    assert!(
        !engine.caches.index_by_name.contains_key(idx_name.as_str()),
        "index_by_name must not retain a ghost entry after rollback"
    );
    assert!(
        !engine
            .dag
            .tables
            .get(&tid)
            .unwrap()
            .index_circuits
            .iter()
            .any(|ic| ic.col_indices.as_slice() == [1]),
        "DAG must not retain a half-built index circuit"
    );
    assert_eq!(
        count_records(engine.sys_store_mut(SysFamily::Index)),
        idx_records_before,
        "sys_indices must net out to zero after rollback"
    );

    // The failed attempt must not block a later successful, non-unique index
    // on the same column.
    engine.create_index("public.t", &["val"], false).unwrap();
    assert!(engine.caches.index_by_name.contains_key(idx_name.as_str()));

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── seek_by_index orphaned-entry infinite-loop guard ─────────────────────
//
// An orphaned index entry (positive weight, no matching source row) must
// terminate the seek scan, not spin: seek_by_index must seek once and walk
// forward, never re-seek to the same orphan on each iteration.

#[test]
fn test_seek_by_index_orphan_entry_terminates() {
    let dir = temp_dir("seek_by_index_orphan");
    let mut engine = CatalogEngine::open(&dir).unwrap();

    let cols = vec![col_def("id", type_code::U64), col_def("val", type_code::U64)];
    let tid = engine.create_table("public.orphan_t", &cols, &[0]).unwrap();
    engine.create_index("public.orphan_t", &["val"], false).unwrap();

    // Locate the index circuit on `val` (col_idx 1) and grab its schema.
    let n = engine.get_index_circuit_count(tid);
    let idx_pos = (0..n)
        .find(|&i| engine.get_index_circuit_info(tid, i).unwrap().0.as_slice() == [1])
        .expect("index circuit on col 1");
    let idx_schema = engine.get_index_circuit_schema(tid, idx_pos).unwrap();
    let idx_key_size = idx_schema.columns[0].size() as usize;

    // Forge an orphan index row directly into the index table, bypassing the
    // source-table ingest: index PK = (promoted indexed value || src_pk),
    // weight +1, but no source row with src_pk=12345 exists.
    let stride = idx_schema.pk_stride() as usize;
    let mut pk = vec![0u8; stride];
    pk[..idx_key_size].copy_from_slice(&777u64.to_le_bytes()[..idx_key_size]);
    pk[idx_key_size..idx_key_size + 8].copy_from_slice(&12345u64.to_le_bytes());

    let mut b = Batch::with_capacity(idx_schema, 1);
    b.extend_pk_bytes(&pk);
    b.extend_weight(&1i64.to_le_bytes());
    b.extend_null_bmp(&0u64.to_le_bytes());
    b.count += 1;

    let handle = engine.get_index_store_handle(tid, &[1]) as *mut Table;
    assert!(!handle.is_null());
    unsafe {
        (*handle).ingest_owned_batch(b).unwrap();
        (*handle).flush().unwrap();
    }

    // The indexed value resolves to an orphan whose source row is missing.
    // Must return None and, crucially, must not hang.
    let r = engine.seek_by_index(tid, &[1], &[777u128]).unwrap().0;
    assert!(r.is_none(), "orphan index entry must resolve to no source row");

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── index epoch: durable client-cache invalidation token ─────────────────
//
// Each owner-affecting IDX_TAB write bumps the per-table index epoch (wraps
// 255 → 1, never 0). Clients echo their cached epoch in GET_INDICES; the server
// re-sends the projected (col_idx, is_unique) list only on a mismatch. These
// guard the §1 counter mechanics directly: bump on every index DDL, never 0,
// and the post-cascade purge that stops a dropped tid from leaking a counter.

#[test]
fn index_version_bumps_on_create_and_drop_index() {
    let (mut engine, tid, dir) = table_fixture(
        "index_version_create_drop",
        &[col_def("id", type_code::U64), col_def("val", type_code::I64)],
    );

    // No index DDL yet → the base epoch (absent ⇒ 1), never 0.
    assert_eq!(engine.get_index_version(tid), 1);

    engine.create_index("public.t", &["val"], false).unwrap();
    let after_create = engine.get_index_version(tid);
    assert!(
        after_create != 1 && after_create != 0,
        "CREATE INDEX must move the epoch off the base, never to 0: got {after_create}"
    );

    let idx_name = make_secondary_index_name("public", "t", "val");
    engine.drop_index(&idx_name).unwrap();
    let after_drop = engine.get_index_version(tid);
    assert!(
        after_drop != after_create && after_drop != 0,
        "DROP INDEX must move the epoch again, never to 0: got {after_drop}"
    );

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn index_version_bumps_on_unique_index() {
    let (mut engine, tid, dir) = table_fixture(
        "index_version_unique",
        &[col_def("id", type_code::U64), col_def("val", type_code::U64)],
    );
    assert_eq!(engine.get_index_version(tid), 1);
    engine.create_index("public.t", &["val"], true).unwrap(); // UNIQUE
    assert!(
        engine.get_index_version(tid) > 1,
        "CREATE UNIQUE INDEX must bump the epoch"
    );
    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn index_version_bumps_on_fk_auto_index() {
    // The FK auto-index is created with the child table; its IDX_TAB +1 must
    // bump the child's epoch just like an explicit CREATE INDEX.
    let dir = temp_dir("index_version_fk_auto");
    let mut engine = CatalogEngine::open(&dir).unwrap();
    let parent_cols = vec![col_def("pk", type_code::U64), col_def("name", type_code::U64)];
    let parent_tid = engine.create_table("public.parent", &parent_cols, &[0]).unwrap();
    let child_cols = vec![
        col_def("pk", type_code::U64),
        ColumnDef {
            name: "parent_ref".into(),
            type_code: type_code::U64,
            is_nullable: false,
            fk_table_id: parent_tid,
            fk_col_idx: 0,
            is_hidden: false,
        },
    ];
    let child_tid = engine.create_table("public.child", &child_cols, &[0]).unwrap();
    assert!(
        engine.get_index_version(child_tid) > 1,
        "FK auto-index creation must bump the child's index epoch"
    );
    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn drop_table_purges_both_version_counters() {
    // Regression for the post-cascade purge. Dropping a table must remove BOTH
    // its index_version and schema_version entries. The drop cascade re-ingests
    // IDX_TAB / COL_TAB retractions whose appliers `or_insert` the counters
    // straight back, so a removal in apply_entity_caches (which fires *before*
    // the cascade) is immediately undone — leaking memory for a tid that is
    // never reused. The purge runs at the tail of the drop hook, after the
    // cascade, so both must end up absent.
    let (mut engine, tid, dir) = table_fixture(
        "drop_purges_versions",
        &[col_def("id", type_code::U64), col_def("val", type_code::I64)],
    );
    engine.create_index("public.t", &["val"], false).unwrap();

    assert!(
        engine.caches.index_version.contains_key(&tid),
        "live indexed table must have an index_version entry"
    );
    assert!(
        engine.caches.schema_version.contains_key(&tid),
        "live table must have a schema_version entry"
    );

    engine.drop_table("public.t").unwrap();

    assert!(
        !engine.caches.index_version.contains_key(&tid),
        "index_version must be purged post-cascade (the index retraction re-creates it)"
    );
    assert!(
        !engine.caches.schema_version.contains_key(&tid),
        "schema_version must be purged post-cascade (the column retraction re-creates it)"
    );

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── UNIQUE index on STRING/BLOB rejected at DDL ──────────────────────────

#[test]
fn test_create_unique_index_on_string_blob_rejected() {
    let dir = temp_dir("uidx_string_reject");
    let mut engine = CatalogEngine::open(&dir).unwrap();
    let blob_col = ColumnDef {
        name: "data".into(),
        type_code: type_code::BLOB,
        is_nullable: false,
        fk_table_id: 0,
        fk_col_idx: 0,
        is_hidden: false,
    };
    let cols = vec![
        col_def("id", type_code::U64),
        col_def("name", type_code::STRING),
        blob_col,
    ];
    engine.create_table("public.t", &cols, &[0]).unwrap();

    // UNIQUE on STRING/BLOB hits the dedicated unique-index guard first, which
    // has its own specific message (distinct from the non-unique gate below).
    let e_str = engine.create_index("public.t", &["name"], true).unwrap_err();
    assert!(e_str.contains("UNIQUE index on STRING or BLOB"), "got: {e_str}");
    let e_blob = engine.create_index("public.t", &["data"], true).unwrap_err();
    assert!(e_blob.contains("UNIQUE index on STRING or BLOB"), "got: {e_blob}");

    // Non-unique secondary indexes on STRING/BLOB are also rejected, by the
    // pre-existing get_index_key_type gate (these column types have no
    // collision-free u128 key representation for ordered index seeks).
    assert!(engine.create_index("public.t", &["name"], false).is_err());
    assert!(engine.create_index("public.t", &["data"], false).is_err());

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── UNIQUE+FK promotion / demotion (column-level unique constraint) ───────
//
// A column that is BOTH a foreign key and UNIQUE registers its FK auto-index
// (non-unique) first; the later unique index must PROMOTE that incumbent
// circuit, not be deduped away. DROP INDEX of the user unique index must DEMOTE
// (the FK auto-index remains), not destroy the circuit.

use std::path::Path;

/// Uniqueness of the index circuit on `col`, or `None` if no circuit exists.
fn circuit_unique(engine: &CatalogEngine, tid: i64, col: u32) -> Option<bool> {
    let n = engine.get_index_circuit_count(tid);
    (0..n)
        .filter_map(|i| engine.get_index_circuit_info(tid, i))
        .find(|(c, _)| c.as_slice() == [col])
        .map(|(_, u)| u)
}

/// Count `idx_*` sub-directories under a table directory.
fn count_idx_dirs(tbl_dir: &str) -> usize {
    std::fs::read_dir(tbl_dir)
        .map(|rd| {
            rd.flatten()
                .filter(|e| e.file_name().to_string_lossy().starts_with("idx_"))
                .count()
        })
        .unwrap_or(0)
}

fn fk_col_def(name: &str, parent_tid: i64, parent_col: u32) -> ColumnDef {
    ColumnDef {
        name: name.into(),
        type_code: type_code::U64,
        is_nullable: false,
        fk_table_id: parent_tid,
        fk_col_idx: parent_col,
        is_hidden: false,
    }
}

#[test]
fn test_promote_unique_index_over_fk_column_empty() {
    let dir = temp_dir("promote_unique_fk_empty");
    let mut engine = CatalogEngine::open(&dir).unwrap();

    let parent_tid = engine
        .create_table("public.parent", &[col_def("id", type_code::U64)], &[0])
        .unwrap();
    let child_cols = vec![col_def("cid", type_code::U64), fk_col_def("refc", parent_tid, 0)];
    let child_tid = engine.create_table("public.child", &child_cols, &[0]).unwrap();

    // FK auto-index registered (non-unique) on col 1 at create time.
    assert_eq!(circuit_unique(&engine, child_tid, 1), Some(false));

    // A UNIQUE index over the same column promotes the incumbent circuit; on an
    // empty table this is a pure flag flip (no duplicate data to scan).
    engine.create_index("public.child", &["refc"], true).unwrap();
    assert_eq!(
        circuit_unique(&engine, child_tid, 1),
        Some(true),
        "the UNIQUE index must promote the FK circuit to unique"
    );

    // Enforcement: a batch-internal duplicate on refc is now rejected.
    let schema = engine.get_schema(child_tid).unwrap();
    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(1u128, 1);
    bb.put_u64(7);
    bb.end_row();
    bb.begin_row(2u128, 1);
    bb.put_u64(7);
    bb.end_row();
    assert!(
        engine.validate_unique_indices(child_tid, &bb.finish()).is_err(),
        "promoted unique index must reject duplicate refc values"
    );

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_unique_index_over_fk_column_distinct_data_promotes() {
    // Masking-bug regression (clean data): CREATE UNIQUE INDEX over an FK column
    // populated with distinct values must promote and enforce, not be masked.
    let dir = temp_dir("promote_unique_fk_distinct");
    let mut engine = CatalogEngine::open(&dir).unwrap();

    let parent_tid = engine
        .create_table("public.parent", &[col_def("id", type_code::U64)], &[0])
        .unwrap();
    let child_cols = vec![col_def("cid", type_code::U64), fk_col_def("refc", parent_tid, 0)];
    let child_tid = engine.create_table("public.child", &child_cols, &[0]).unwrap();

    // Seed DISTINCT refc values (ingest_to_family bypasses FK validation).
    let schema = engine.get_schema(child_tid).unwrap();
    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(1u128, 1);
    bb.put_u64(10);
    bb.end_row();
    bb.begin_row(2u128, 1);
    bb.put_u64(20);
    bb.end_row();
    engine.ingest_to_family(child_tid, &bb.finish()).unwrap();
    engine.flush_family(child_tid).unwrap();

    engine.create_index("public.child", &["refc"], true).unwrap();
    assert_eq!(circuit_unique(&engine, child_tid, 1), Some(true));

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_unique_index_over_fk_column_duplicate_data_rejected() {
    // Masking-bug regression (dirty data): CREATE UNIQUE INDEX over an FK column
    // with DUPLICATE values must fail and net sys_indices back to zero, instead
    // of silently dropping the constraint.
    let dir = temp_dir("promote_unique_fk_dup");
    let mut engine = CatalogEngine::open(&dir).unwrap();

    let parent_tid = engine
        .create_table("public.parent", &[col_def("id", type_code::U64)], &[0])
        .unwrap();
    let child_cols = vec![col_def("cid", type_code::U64), fk_col_def("refc", parent_tid, 0)];
    let child_tid = engine.create_table("public.child", &child_cols, &[0]).unwrap();

    // Seed DUPLICATE refc values.
    let schema = engine.get_schema(child_tid).unwrap();
    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(1u128, 1);
    bb.put_u64(42);
    bb.end_row();
    bb.begin_row(2u128, 1);
    bb.put_u64(42);
    bb.end_row();
    engine.ingest_to_family(child_tid, &bb.finish()).unwrap();
    engine.flush_family(child_tid).unwrap();

    let before = count_records(engine.sys_store_mut(SysFamily::Index));
    let r = engine.create_index("public.child", &["refc"], true);
    assert!(r.is_err(), "unique index over duplicate FK data must fail");
    assert_eq!(
        count_records(engine.sys_store_mut(SysFamily::Index)),
        before,
        "the failed unique index row must net out of sys_indices"
    );
    // The incumbent FK circuit stays non-unique (promotion never committed).
    assert_eq!(circuit_unique(&engine, child_tid, 1), Some(false));

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_drop_unique_index_on_fk_column_demotes() {
    // Dropping the user unique index of a UNIQUE+FK column must DEMOTE the
    // circuit (the FK auto-index still covers the column), not destroy it.
    let dir = temp_dir("demote_unique_fk");
    let mut engine = CatalogEngine::open(&dir).unwrap();

    let parent_tid = engine
        .create_table("public.parent", &[col_def("id", type_code::U64)], &[0])
        .unwrap();
    let child_cols = vec![col_def("cid", type_code::U64), fk_col_def("refc", parent_tid, 0)];
    let child_tid = engine.create_table("public.child", &child_cols, &[0]).unwrap();

    engine.create_index("public.child", &["refc"], true).unwrap();
    assert_eq!(circuit_unique(&engine, child_tid, 1), Some(true));

    let user_idx = make_secondary_index_name("public", "child", "refc");
    let fk_idx = make_fk_index_name("public", "child", "refc");
    engine.drop_index(&user_idx).unwrap();

    // Circuit remains (col 1 still indexed) but is no longer unique; the FK
    // auto-index survives so FK lookups keep working.
    assert_eq!(
        circuit_unique(&engine, child_tid, 1),
        Some(false),
        "circuit must be demoted, not destroyed"
    );
    assert!(
        engine.has_index_by_name(&fk_idx),
        "the FK auto-index must survive the unique-index drop"
    );

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_drop_unique_index_on_fk_column_keeps_shared_directory() {
    // The UNIQUE index promotes the FK circuit and builds NO second directory —
    // the circuit's directory carries the FK index's id. Dropping the unique
    // index must NOT delete that shared directory (the FK still needs it); a
    // subsequent drop_table removes the whole table dir, leaving no orphan.
    let dir = temp_dir("dir_correct_unique_fk");
    let mut engine = CatalogEngine::open(&dir).unwrap();

    let parent_tid = engine
        .create_table("public.parent", &[col_def("id", type_code::U64)], &[0])
        .unwrap();
    let child_cols = vec![col_def("cid", type_code::U64), fk_col_def("refc", parent_tid, 0)];
    let child_tid = engine.create_table("public.child", &child_cols, &[0]).unwrap();
    engine.create_index("public.child", &["refc"], true).unwrap();

    let tbl_dir = format!("{dir}/public/t_{child_tid}");
    assert_eq!(
        count_idx_dirs(&tbl_dir),
        1,
        "promotion must reuse the FK index directory, not build a second one"
    );

    // Drop the user unique index: demotion keeps the FK directory in place.
    let user_idx = make_secondary_index_name("public", "child", "refc");
    engine.drop_index(&user_idx).unwrap();
    engine.drain_pending_dir_deletions();
    assert_eq!(
        count_idx_dirs(&tbl_dir),
        1,
        "dropping the unique index must not delete the shared FK directory"
    );

    // drop_table cascades the FK index (using the circuit's creating index_id
    // for the deletion path) and removes the whole table directory.
    engine.drop_table("public.child").unwrap();
    engine.drain_pending_dir_deletions();
    assert!(!Path::new(&tbl_dir).exists(), "no orphan idx_* dir may remain");

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_failed_create_index_leaves_no_directory() {
    // A unique create_index that fails on duplicate data must drain the
    // pre-staged index directory, leaving no orphan idx_* dir on disk.
    let (mut engine, tid, dir) = table_fixture(
        "failed_create_index_dir",
        &[col_def("id", type_code::U64), col_def("val", type_code::U64)],
    );
    let schema = engine.get_schema(tid).unwrap();
    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(1u128, 1);
    bb.put_u64(9);
    bb.end_row();
    bb.begin_row(2u128, 1);
    bb.put_u64(9);
    bb.end_row();
    engine.ingest_to_family(tid, &bb.finish()).unwrap();
    engine.flush_family(tid).unwrap();

    let tbl_dir = format!("{dir}/public/t_{tid}");
    assert!(engine.create_index("public.t", &["val"], true).is_err());
    assert_eq!(
        count_idx_dirs(&tbl_dir),
        0,
        "a failed unique create_index must leave no index directory behind"
    );

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_drop_index_permitted_on_lone_pk_target() {
    // A redundant unique index on a lone-PK column that is an FK target may be
    // dropped: the PK itself preserves uniqueness for FK child validation.
    let dir = temp_dir("drop_idx_lone_pk_fk");
    let mut engine = CatalogEngine::open(&dir).unwrap();

    let parent_tid = engine
        .create_table("public.parent", &[col_def("id", type_code::U64)], &[0])
        .unwrap();
    let child_cols = vec![col_def("cid", type_code::U64), fk_col_def("p", parent_tid, 0)];
    engine.create_table("public.child", &child_cols, &[0]).unwrap();

    // Redundant unique index on the parent's lone PK column.
    engine.create_index("public.parent", &["id"], true).unwrap();
    let idx = make_secondary_index_name("public", "parent", "id");
    engine
        .drop_index(&idx)
        .expect("drop must be permitted: lone PK preserves uniqueness");

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_drop_unique_index_on_non_pk_fk_target_blocked() {
    // A non-PK FK-target column with a unique index cannot have that index
    // dropped while a child references it and no other unique index survives.
    let dir = temp_dir("drop_idx_nonpk_fk_blocked");
    let mut engine = CatalogEngine::open(&dir).unwrap();

    // Parent (id PK, email) with a UNIQUE index on the non-PK `email` column.
    let parent_cols = vec![col_def("id", type_code::U64), col_def("email", type_code::U64)];
    let parent_tid = engine.create_table("public.parent", &parent_cols, &[0]).unwrap();
    engine.create_index("public.parent", &["email"], true).unwrap();

    // Child references parent.email (col 1), legal because email is unique.
    let child_cols = vec![col_def("cid", type_code::U64), fk_col_def("e", parent_tid, 1)];
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
    let _ = fs::remove_dir_all(&dir);
}

// ── chunked unique-index backfill: cross-chunk duplicate detection ────────
//
// The unique-index backfill scans the owner chunk-wise (drain_chunk); a
// duplicate pair split across chunks is only visible through the `seen` set
// carried across chunks. Shrink `ddl_scan_chunk_rows` so a handful of rows
// spans several chunks, and place the duplicate pair at the PK extremes
// (the scan is in PK merge order).

#[test]
fn test_unique_index_duplicate_across_chunks_rejected() {
    let (mut engine, tid, dir) = table_fixture(
        "unique_idx_dup_cross_chunk",
        &[col_def("id", type_code::U64), col_def("val", type_code::U64)],
    );
    let schema = engine.get_schema(tid).unwrap();

    // The only duplicate pair is val=42 at pk 0 and pk 9 — first and last
    // chunk at chunk_rows = 3.
    let mut bb = BatchBuilder::new(schema);
    for i in 0..10u64 {
        bb.begin_row(i as u128, 1);
        bb.put_u64(if i == 0 || i == 9 { 42 } else { 100 + i });
        bb.end_row();
    }
    engine.ingest_to_family(tid, &bb.finish()).unwrap();
    engine.flush_family(tid).unwrap();

    engine.ddl_scan_chunk_rows = 3;
    let before = count_records(engine.sys_store_mut(SysFamily::Index));
    let r = engine.create_index("public.t", &["val"], true);
    assert!(r.is_err(), "cross-chunk duplicate must fail the unique backfill");
    assert_eq!(
        count_records(engine.sys_store_mut(SysFamily::Index)),
        before,
        "the failed unique index row must net out of sys_indices"
    );

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_unique_index_duplicate_within_chunk_rejected() {
    let (mut engine, tid, dir) = table_fixture(
        "unique_idx_dup_within_chunk",
        &[col_def("id", type_code::U64), col_def("val", type_code::U64)],
    );
    let schema = engine.get_schema(tid).unwrap();

    // Duplicate pair at pk 0 and pk 1 — both inside the first chunk of 4.
    let mut bb = BatchBuilder::new(schema);
    for i in 0..10u64 {
        bb.begin_row(i as u128, 1);
        bb.put_u64(if i <= 1 { 42 } else { 100 + i });
        bb.end_row();
    }
    engine.ingest_to_family(tid, &bb.finish()).unwrap();
    engine.flush_family(tid).unwrap();

    engine.ddl_scan_chunk_rows = 4;
    assert!(
        engine.create_index("public.t", &["val"], true).is_err(),
        "within-chunk duplicate must still fail the unique backfill"
    );

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_unique_index_chunked_backfill_distinct_succeeds() {
    // Positive control for the chunked scan: distinct data must build the
    // index across several chunks with every row projected exactly once.
    let (mut engine, tid, dir) = table_fixture(
        "unique_idx_chunked_ok",
        &[col_def("id", type_code::U64), col_def("val", type_code::U64)],
    );
    let schema = engine.get_schema(tid).unwrap();

    let mut bb = BatchBuilder::new(schema);
    for i in 0..10u64 {
        bb.begin_row(i as u128, 1);
        bb.put_u64(100 + i);
        bb.end_row();
    }
    engine.ingest_to_family(tid, &bb.finish()).unwrap();
    engine.flush_family(tid).unwrap();

    engine.ddl_scan_chunk_rows = 3;
    engine.create_index("public.t", &["val"], true).unwrap();

    let entry = engine.dag.tables.get_mut(&tid).unwrap();
    assert_eq!(entry.index_circuits.len(), 1);
    assert_eq!(
        count_records(entry.index_circuits[0].table_mut()),
        10,
        "chunked backfill must project every row exactly once"
    );

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_promote_unique_duplicate_across_chunks_rejected() {
    // Promotion (UNIQUE over an incumbent FK circuit) validates through the
    // same chunked scan; a cross-chunk duplicate must reject the promotion
    // and leave the incumbent non-unique.
    let dir = temp_dir("promote_dup_cross_chunk");
    let mut engine = CatalogEngine::open(&dir).unwrap();

    let parent_tid = engine
        .create_table("public.parent", &[col_def("id", type_code::U64)], &[0])
        .unwrap();
    let child_cols = vec![col_def("cid", type_code::U64), fk_col_def("refc", parent_tid, 0)];
    let child_tid = engine.create_table("public.child", &child_cols, &[0]).unwrap();

    // Duplicate refc=42 at pk 0 and pk 9; distinct in between (ingest_to_family
    // bypasses FK validation).
    let schema = engine.get_schema(child_tid).unwrap();
    let mut bb = BatchBuilder::new(schema);
    for i in 0..10u64 {
        bb.begin_row(i as u128, 1);
        bb.put_u64(if i == 0 || i == 9 { 42 } else { 100 + i });
        bb.end_row();
    }
    engine.ingest_to_family(child_tid, &bb.finish()).unwrap();
    engine.flush_family(child_tid).unwrap();

    engine.ddl_scan_chunk_rows = 3;
    assert!(
        engine.create_index("public.child", &["refc"], true).is_err(),
        "cross-chunk duplicate must fail the promotion scan"
    );
    assert_eq!(
        circuit_unique(&engine, child_tid, 1),
        Some(false),
        "the incumbent FK circuit must stay non-unique"
    );

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── Composite (multi-column) secondary index tests ──────────────────────

#[test]
fn test_composite_index_full_key_seek() {
    let (mut engine, tid, dir) = table_fixture(
        "composite_full_key",
        &[
            col_def("id", type_code::U64),
            col_def("a", type_code::U64),
            col_def("b", type_code::U64),
        ],
    );
    engine.create_index("public.t", &["a", "b"], false).unwrap();
    let schema = engine.get_schema(tid).unwrap();

    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(10u128, 1);
    bb.put_u64(1);
    bb.put_u64(100);
    bb.end_row();
    bb.begin_row(20u128, 1);
    bb.put_u64(1);
    bb.put_u64(200);
    bb.end_row();
    bb.begin_row(30u128, 1);
    bb.put_u64(2);
    bb.put_u64(100);
    bb.end_row();
    engine.ingest_to_family(tid, &bb.finish()).unwrap();
    engine.flush_family(tid).unwrap();

    // Full-key seek (a=1, b=200) → PK 20 only. cols a=1, b=2.
    let r = engine.seek_by_index(tid, &[1, 2], &[1u128, 200u128]).unwrap().0;
    let r = r.expect("full-key composite seek must find a row");
    assert_eq!(r.count, 1);
    assert_eq!(r.get_pk(0), 20);

    // A full key that matches no row → None.
    let none = engine.seek_by_index(tid, &[1, 2], &[1u128, 999u128]).unwrap().0;
    assert!(none.is_none());

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_composite_index_leading_prefix_seek() {
    let (mut engine, tid, dir) = table_fixture(
        "composite_prefix",
        &[
            col_def("id", type_code::U64),
            col_def("a", type_code::U64),
            col_def("b", type_code::U64),
        ],
    );
    engine.create_index("public.t", &["a", "b"], false).unwrap();
    let schema = engine.get_schema(tid).unwrap();

    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(10u128, 1);
    bb.put_u64(1);
    bb.put_u64(100);
    bb.end_row();
    bb.begin_row(20u128, 1);
    bb.put_u64(1);
    bb.put_u64(200);
    bb.end_row();
    bb.begin_row(30u128, 1);
    bb.put_u64(2);
    bb.put_u64(100);
    bb.end_row();
    engine.ingest_to_family(tid, &bb.finish()).unwrap();
    engine.flush_family(tid).unwrap();

    // Leading-prefix seek over (a, b) supplying only a=1 (K=1 < N=2) must match
    // every row with a=1 (PKs 10 and 20), regardless of b.
    let r = engine.seek_by_index(tid, &[1, 2], &[1u128]).unwrap().0;
    let r = r.expect("leading-prefix seek must find rows");
    let mut pks: Vec<u128> = (0..r.count).map(|i| r.get_pk(i)).collect();
    pks.sort();
    assert_eq!(pks, vec![10, 20]);

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_composite_index_signed_unsigned_u128_mix() {
    // Mixed column widths/signedness: i32, u64, u128. Equality-correct seek for
    // a negative leading value and a wide trailing value.
    let (mut engine, tid, dir) = table_fixture(
        "composite_mix",
        &[
            col_def("id", type_code::U64),
            col_def("a", type_code::I32),
            col_def("b", type_code::U64),
            col_def("c", type_code::U128),
        ],
    );
    engine.create_index("public.t", &["a", "b", "c"], false).unwrap();
    let schema = engine.get_schema(tid).unwrap();

    let mut bb = BatchBuilder::new(schema);
    // (a=-5, b=7, c=2^70+3)
    let big: u128 = (1u128 << 70) | 3;
    bb.begin_row(1u128, 1);
    bb.put_u32((-5i32) as u32);
    bb.put_u64(7);
    bb.put_u128(big);
    bb.end_row();
    bb.begin_row(2u128, 1);
    bb.put_u32(9u32);
    bb.put_u64(7);
    bb.put_u128(big);
    bb.end_row();
    engine.ingest_to_family(tid, &bb.finish()).unwrap();
    engine.flush_family(tid).unwrap();

    // i32(-5) is its zero-extended u32 bit pattern as the native key.
    let r = engine
        .seek_by_index(tid, &[1, 2, 3], &[(-5i32) as u32 as u128, 7u128, big])
        .unwrap()
        .0;
    let r = r.expect("mixed-width composite seek must find the row");
    assert_eq!(r.count, 1);
    assert_eq!(r.get_pk(0), 1);

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_composite_index_null_in_any_key_skipped() {
    // A NULL in any indexed column omits the row from the index, so a seek by
    // the non-null columns must not find it.
    let (mut engine, tid, dir) = table_fixture(
        "composite_null",
        &[
            col_def("id", type_code::U64),
            col_def("a", type_code::U64),
            ColumnDef {
                name: "b".into(),
                type_code: type_code::U64,
                is_nullable: true,
                fk_table_id: 0,
                fk_col_idx: 0,
                is_hidden: false,
            },
        ],
    );
    engine.create_index("public.t", &["a", "b"], false).unwrap();
    let schema = engine.get_schema(tid).unwrap();

    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(10u128, 1);
    bb.put_u64(1);
    bb.put_null();
    bb.end_row();
    bb.begin_row(20u128, 1);
    bb.put_u64(1);
    bb.put_u64(200);
    bb.end_row();
    engine.ingest_to_family(tid, &bb.finish()).unwrap();
    engine.flush_family(tid).unwrap();

    // Leading-prefix seek a=1 must find only PK 20 (PK 10 has NULL b → not indexed).
    let r = engine.seek_by_index(tid, &[1, 2], &[1u128]).unwrap().0;
    let r = r.expect("seek must find the non-null row");
    assert_eq!(r.count, 1);
    assert_eq!(r.get_pk(0), 20);

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_composite_index_drop_exact_list() {
    // With both a single-column (a) index and a composite (a, b) index, dropping
    // (a, b) leaves the (a) index serving and removes only the (a, b) circuit.
    let (mut engine, tid, dir) = table_fixture(
        "composite_drop",
        &[
            col_def("id", type_code::U64),
            col_def("a", type_code::U64),
            col_def("b", type_code::U64),
        ],
    );
    engine.create_index("public.t", &["a"], false).unwrap();
    engine.create_index("public.t", &["a", "b"], false).unwrap();

    assert!(engine.index_circuit_for_cols(tid, &[1]).is_some());
    assert!(engine.index_circuit_for_cols(tid, &[1, 2]).is_some());

    // Drop the composite by its generated name (cols joined by '_').
    engine.drop_index("public__t__idx_a_b").unwrap();

    assert!(
        engine.index_circuit_for_cols(tid, &[1]).is_some(),
        "single-column (a) index must survive"
    );
    assert!(
        engine.index_circuit_for_cols(tid, &[1, 2]).is_none(),
        "composite (a, b) circuit must be removed"
    );

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_composite_index_does_not_answer_single_column() {
    // A composite (a, b) index does not satisfy a single-column lookup on a
    // (exact-list match) — only the full (a, b) list resolves.
    let (mut engine, tid, dir) = table_fixture(
        "composite_single_miss",
        &[
            col_def("id", type_code::U64),
            col_def("a", type_code::U64),
            col_def("b", type_code::U64),
        ],
    );
    engine.create_index("public.t", &["a", "b"], false).unwrap();

    assert!(
        engine.index_circuit_for_cols(tid, &[1]).is_none(),
        "composite index must not answer a single-column [a] query"
    );
    assert!(engine.index_circuit_for_cols(tid, &[1, 2]).is_some());

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_composite_unique_created_and_enforced() {
    // CREATE UNIQUE INDEX over multiple columns is now supported, and the
    // in-batch validator enforces uniqueness over the composite (a, b) span:
    // a duplicate (a, b) is rejected, two rows differing only in b are admitted.
    let (mut engine, tid, dir) = table_fixture(
        "composite_unique_ok",
        &[
            col_def("id", type_code::U64),
            col_def("a", type_code::U64),
            col_def("b", type_code::U64),
        ],
    );

    engine
        .create_index("public.t", &["a", "b"], true)
        .expect("composite UNIQUE (a, b) must be created");
    let schema = engine.get_schema(tid).unwrap();

    // Commit (id=1, a=5, b=1).
    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(1u128, 1);
    bb.put_u64(5);
    bb.put_u64(1);
    bb.end_row();
    engine.ingest_to_family(tid, &bb.finish()).unwrap();
    engine.flush_family(tid).unwrap();

    // A second row with the same (a, b) = (5, 1) violates the composite index.
    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(2u128, 1);
    bb.put_u64(5);
    bb.put_u64(1);
    bb.end_row();
    let err = engine
        .validate_unique_indices(tid, &bb.finish())
        .expect_err("duplicate (a, b) must violate");
    assert!(err.contains("Unique index violation"), "got: {err}");
    // The diagnostic names both columns of the composite.
    assert!(
        err.contains('a') && err.contains('b'),
        "violation should name (a, b): {err}"
    );

    // A row sharing only a (a=5, b=2) is a distinct composite → admitted.
    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(3u128, 1);
    bb.put_u64(5);
    bb.put_u64(2);
    bb.end_row();
    assert!(
        engine.validate_unique_indices(tid, &bb.finish()).is_ok(),
        "rows differing only in the trailing column must be admitted"
    );

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_make_index_schema_composite_layout() {
    use crate::schema::{type_code as tc, SchemaColumn, SchemaDescriptor};
    // Source: PK = (id: U64); payload a: U32, b: U128.
    let src = SchemaDescriptor::new(
        &[
            SchemaColumn::new(tc::U64, 0),
            SchemaColumn::new(tc::U32, 0),
            SchemaColumn::new(tc::U128, 0),
        ],
        &[0],
    );
    // Index on (a:U32 → U64, b:U128 → U128). Leading region = 8 + 16 = 24 bytes;
    // source PK suffix = U64 = 8 bytes. Arity 3, stride 32.
    let idx = make_index_schema(&[1, 2], &src).unwrap();
    assert_eq!(idx.num_columns(), 3); // 2 promoted + 1 src pk
    assert_eq!(idx.pk_indices(), &[0, 1, 2]); // every column in the PK
    assert_eq!(idx.columns[0].type_code, tc::U64);
    assert_eq!(idx.columns[1].type_code, tc::U128);
    assert_eq!(idx.columns[2].type_code, tc::U64); // src pk column type
    assert_eq!(idx.pk_stride() as usize, 8 + 16 + 8);
}

#[test]
fn test_make_index_schema_over_limit_errs_not_panics() {
    use crate::schema::{type_code as tc, SchemaColumn, SchemaDescriptor};
    // Source with a 2-column PK (id0, id1). A 4-column index → arity 4 + 2 = 6 >
    // MAX_PK_COLUMNS (5): must return Err, never abort via SchemaDescriptor::new.
    let src = SchemaDescriptor::new(
        &[
            SchemaColumn::new(tc::U64, 0),
            SchemaColumn::new(tc::U64, 0),
            SchemaColumn::new(tc::U64, 0),
            SchemaColumn::new(tc::U64, 0),
            SchemaColumn::new(tc::U64, 0),
            SchemaColumn::new(tc::U64, 0),
        ],
        &[0, 1],
    );
    match make_index_schema(&[2, 3, 4, 5], &src) {
        Err(e) => assert!(e.contains("arity"), "got: {e}"),
        Ok(_) => panic!("over-arity index schema must be Err, not a panic/abort"),
    }
}

#[test]
fn test_seek_prefix_matches_projection() {
    // The seek encoder (IndexKeySpec::seek_prefix) and the projection encoder
    // (batch_project_index) must produce byte-identical leading-key bytes for the
    // same native values — that equality is what makes every composite seek find
    // the projected entry.
    use crate::schema::{type_code as tc, IndexKeySpec, SchemaColumn, SchemaDescriptor};
    let src = SchemaDescriptor::new(
        &[
            SchemaColumn::new(tc::U64, 0),
            SchemaColumn::new(tc::I32, 0),
            SchemaColumn::new(tc::U64, 0),
        ],
        &[0],
    );
    let idx = make_index_schema(&[1, 2], &src).unwrap();

    // Project one row (id=1, a=-5, b=42) and read back its leading-key bytes.
    let mut bb = BatchBuilder::new(src);
    bb.begin_row(1u128, 1);
    bb.put_u32((-5i32) as u32);
    bb.put_u64(42);
    bb.end_row();
    let projected = {
        let b = bb.finish();
        crate::query::DagEngine::batch_project_index(&b, &crate::schema::IndexKeySpec::new(&[1, 2], &src, &idx), &idx)
    };
    assert_eq!(projected.count, 1);

    let key_size: usize = (0..2).map(|i| idx.columns[i].size() as usize).sum();
    let proj_key = &projected.get_pk_bytes(0)[..key_size];

    let spec = IndexKeySpec::new(&[1, 2], &src, &idx);
    let (opk, plen) = spec.seek_prefix(&[(-5i32) as u32 as u128, 42u128]);
    assert_eq!(plen, key_size);
    assert_eq!(
        &opk[..plen],
        proj_key,
        "seek prefix bytes must equal projected leading-key bytes"
    );
}

// ── Signed secondary-index ordering (order-preserving signed leading key) ────

/// Zero-extended native u128 (two's-complement low `sz` bytes) of a value — the
/// exact form `payload_native_key`/`pk_native_key` produce.
fn native_u128_at(v: i64, sz: usize) -> u128 {
    let mask = if sz >= 16 { u128::MAX } else { (1u128 << (sz * 8)) - 1 };
    (v as u64 as u128) & mask
}

/// Project a single value (zero-extended native `u128`) through a one-column
/// secondary index — `src` column 1 indexed by `idx` — returning the OPK
/// leading-key span the write path (`IndexKeySpec::write_span` via
/// `batch_project_index`) stores.
fn project_leading_span(src: SchemaDescriptor, idx: &SchemaDescriptor, native: u128) -> Vec<u8> {
    let mut bb = BatchBuilder::new(src);
    bb.begin_row(1u128, 1);
    match src.columns[1].size() {
        1 => bb.put_u8(native as u8),
        2 => bb.put_u16(native as u16),
        4 => bb.put_u32(native as u32),
        8 => bb.put_u64(native as u64),
        16 => bb.put_u128(native),
        sz => unreachable!("unexpected column width {sz}"),
    }
    bb.end_row();
    let projected = {
        let b = bb.finish();
        crate::query::DagEngine::batch_project_index(&b, &crate::schema::IndexKeySpec::new(&[1], &src, idx), idx)
    };
    let key_size = idx.columns[0].size() as usize;
    projected.get_pk_bytes(0)[..key_size].to_vec()
}

#[test]
fn signed_index_width_ladder_promotes_to_i64_and_orders() {
    // Every signed width promotes to the 8-byte signed I64 index key (so the
    // record stride is unchanged) and produces an ORDER-PRESERVING leading key
    // at its own width — the `encode_pk_column_promoted` sign-extension puts
    // negatives below non-negatives, the invariant a future range / ordered
    // index scan relies on. Fails under the old U64 promotion (negatives sorted
    // AFTER non-negatives); passes now.
    use crate::schema::{type_code as tc, SchemaColumn, SchemaDescriptor};
    for &(t, sz) in &[(tc::I8, 1usize), (tc::I16, 2), (tc::I32, 4), (tc::I64, 8)] {
        let src = SchemaDescriptor::new(&[SchemaColumn::new(tc::U64, 0), SchemaColumn::new(t, 0)], &[0]);
        let idx = make_index_schema(&[1], &src).unwrap();
        assert_eq!(idx.columns[0].type_code, tc::I64, "tc={t} must promote to I64");
        assert_eq!(idx.columns[0].size(), 8, "promoted signed key keeps the 8-byte width");

        let lo = if sz == 8 { i64::MIN } else { -(1i64 << (sz * 8 - 1)) };
        let hi = if sz == 8 { i64::MAX } else { (1i64 << (sz * 8 - 1)) - 1 };
        let values = [lo, -3, -1, 0, 1, hi];
        let spans: Vec<Vec<u8>> = values
            .iter()
            .map(|&v| project_leading_span(src, &idx, native_u128_at(v, sz)))
            .collect();
        for w in spans.windows(2) {
            assert!(
                w[0] < w[1],
                "tc={t} spans must sort numerically: {:?} !< {:?}",
                w[0],
                w[1]
            );
        }
    }
}

// ── IndexKeySpec::write_span byte-equivalence ───────────────────────────────
//
// `write_span` reduces two operand paths to claimed identities: an unpromoted PK
// source is copied verbatim out of the OPK region (skipping decode∘encode), and
// a payload source is handed to the encoder straight from its slot (skipping the
// widen-into-u128-and-reslice). A wrong byte in either silently corrupts a
// secondary index, so both are pinned against an explicit oracle rather than
// inferred — plus a structural gate on the source→index type pair set the
// identities depend on.

/// Independent oracle for `write_span`: `IndexKeySpec::seek_prefix` is the
/// **seek-side** encoder, maintained separately, and it consumes native `u128`
/// values rather than reading a row — so feeding it `ColumnLocator::native_key`
/// exercises a read path `write_span` no longer takes (`write_span` reads raw
/// `bytes` and promotes; this decodes to native first). `None` is the NULL skip.
fn write_span_reference(
    owner: &SchemaDescriptor,
    spec: &IndexKeySpec,
    cols: &[u32],
    mb: &crate::storage::MemBatch<'_>,
    row: usize,
) -> Option<[u8; MAX_PK_BYTES]> {
    let mut natives = Vec::with_capacity(cols.len());
    for &c in cols {
        let loc = owner.locate(c as usize);
        if loc.is_null(mb, row) {
            return None;
        }
        natives.push(loc.native_key(mb, row));
    }
    Some(spec.seek_prefix(&natives).0)
}

#[test]
fn write_span_matches_the_oracle_on_compound_null_and_entry_shapes() {
    // Compound PK `(U32, I64)` so the second indexed column is a PK column at a
    // NON-ZERO byte offset — the coordinate the verbatim arm slices with.
    let src = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U32, 0),
            SchemaColumn::new(type_code::I64, 0),
            SchemaColumn::new(type_code::I32, 1), // nullable payload
        ],
        &[0, 1],
    );
    let cols = [1u32, 2];
    let idx = make_index_schema(&cols, &src).unwrap();
    let spec = IndexKeySpec::new(&cols, &src, &idx);

    let mut bb = BatchBuilder::new(src);
    for (i, &(a, v)) in [(7u32, -1i64), (0, 0), (u32::MAX, i64::MIN), (3, i64::MAX)]
        .iter()
        .enumerate()
    {
        bb.begin_row_opk(&[a as u128, v as u64 as u128], 1);
        bb.put_i32(-(i as i32));
        bb.end_row();
    }
    // A NULL in the *trailing* indexed column: the leading column's bytes are
    // written, then `write_span` bails with `false`.
    bb.begin_row_opk(&[9, 5], 1);
    bb.put_null();
    bb.end_row();
    let b = bb.finish();
    let null_row = b.count - 1;

    let mb = b.as_mem_batch();
    let stride = src.pk_stride() as usize;
    for row in 0..b.count {
        let mut got = [0u8; MAX_PK_BYTES];
        let g = spec.write_span(&mb, row, &mut got);
        let want = write_span_reference(&src, &spec, &cols, &mb, row);
        assert_eq!(g, want.is_some(), "row={row}: skip verdicts must agree");
        assert_eq!(g, row != null_row, "row={row}: only the NULL row is skipped");
        if let Some(want) = want {
            assert_eq!(got[..spec.key_size()], want[..spec.key_size()], "row={row}");
        }

        // `write_entry` = span ‖ source-PK OPK suffix, at the span's width.
        let mut entry = [0u8; MAX_PK_BYTES];
        assert_eq!(spec.write_entry(&mb, row, &mut entry), g);
        if g {
            assert_eq!(entry[..spec.key_size()], got[..spec.key_size()]);
            assert_eq!(
                &entry[spec.key_size()..spec.key_size() + stride],
                mb.get_pk_bytes(row),
                "row={row}: PK suffix must be the row's OPK bytes, verbatim",
            );
        }
    }
}

#[test]
fn write_span_matches_seek_prefix_across_type_ladder() {
    // Write/seek byte-equality: the projected leading-key span (write side) must
    // byte-equal `index_opk_prefix` (seek side) for the same value, at every
    // type — the equality a partial application of the signed-encoding change
    // would break (and which keeps `WHERE col = v` seeks correct).
    use crate::schema::{type_code as tc, SchemaColumn, SchemaDescriptor};
    let cases: &[(u8, usize, &[i64])] = &[
        (tc::I8, 1, &[-128, -1, 0, 1, 127]),
        (tc::I16, 2, &[-32768, -1, 0, 1, 32767]),
        (tc::U16, 2, &[0, 1, 42, 65535]),
        (tc::I32, 4, &[i32::MIN as i64, -42, -1, 0, 1, 42, i32::MAX as i64]),
        (tc::I64, 8, &[i64::MIN, -42, -1, 0, 1, 42, i64::MAX]),
        (tc::U8, 1, &[0, 1, 200, 255]),
        (tc::U32, 4, &[0, 1, 42, u32::MAX as i64]),
        (tc::U64, 8, &[0, 1, 42, -1 /* = u64::MAX bits */]),
        (tc::U128, 16, &[0, 1, 42, -1]),
        (tc::UUID, 16, &[0, 1, 42, -1]),
    ];
    for &(t, sz, values) in cases {
        let src = SchemaDescriptor::new(&[SchemaColumn::new(tc::U64, 0), SchemaColumn::new(t, 0)], &[0]);
        let idx = make_index_schema(&[1], &src).unwrap();
        let idx_type = idx.columns[0].type_code;
        let idx_size = idx.columns[0].size() as usize;
        // The same column as the table's PK — the source shape that reaches
        // `write_span`'s PK arm (verbatim copy when unpromoted, decode+encode
        // otherwise). The payload source alone cannot exercise it.
        let pk_src = SchemaDescriptor::new(&[SchemaColumn::new(t, 0), SchemaColumn::new(tc::U64, 0)], &[0]);
        let pk_idx = make_index_schema(&[0], &pk_src).unwrap();
        assert_eq!(pk_idx.columns[0].type_code, idx_type);
        let pk_spec = IndexKeySpec::new(&[0], &pk_src, &pk_idx);

        for &v in values {
            let native = native_u128_at(v, sz);
            let write_span = project_leading_span(src, &idx, native);
            let seek = crate::schema::index_opk_prefix(native, t, idx_type);
            assert_eq!(
                &write_span[..],
                &seek[..idx_size],
                "write/seek byte mismatch for tc={t} v={v}"
            );

            let mut bb = BatchBuilder::new(pk_src);
            bb.begin_row_opk(&[native], 1);
            bb.put_u64(0);
            bb.end_row();
            let b = bb.finish();
            let mut span = [0u8; MAX_PK_BYTES];
            assert!(pk_spec.write_span(&b.as_mem_batch(), 0, &mut span));
            assert_eq!(
                &span[..idx_size],
                &seek[..idx_size],
                "PK-source write/seek byte mismatch for tc={t} v={v}"
            );
        }
    }
}

#[test]
fn composite_index_signed_leading_unsigned_tiebreak_orders() {
    // Index on (signed i32, unsigned u64): the leading signed column orders
    // numerically (negatives below non-negatives) and the unsigned column breaks
    // ties. Asserts the full composite span sorts in tuple-numeric order.
    use crate::schema::{type_code as tc, SchemaColumn, SchemaDescriptor};
    let src = SchemaDescriptor::new(
        &[
            SchemaColumn::new(tc::U64, 0),
            SchemaColumn::new(tc::I32, 0),
            SchemaColumn::new(tc::U64, 0),
        ],
        &[0],
    );
    let idx = make_index_schema(&[1, 2], &src).unwrap();
    let key_size: usize = (0..2).map(|i| idx.columns[i].size() as usize).sum();
    // (a, b) in strictly ascending numeric order — including same-`a` tie pairs.
    let rows: &[(i32, u64)] = &[(i32::MIN, 5), (-1, 0), (-1, 9), (0, 0), (0, 1), (1, 0), (i32::MAX, 7)];
    let mut spans: Vec<Vec<u8>> = Vec::new();
    for (i, &(a, b)) in rows.iter().enumerate() {
        let mut bb = BatchBuilder::new(src);
        bb.begin_row((i as u128) + 1, 1);
        bb.put_u32(a as u32);
        bb.put_u64(b);
        bb.end_row();
        let projected = {
            let b = bb.finish();
            crate::query::DagEngine::batch_project_index(
                &b,
                &crate::schema::IndexKeySpec::new(&[1, 2], &src, &idx),
                &idx,
            )
        };
        spans.push(projected.get_pk_bytes(0)[..key_size].to_vec());
    }
    for w in spans.windows(2) {
        assert!(
            w[0] < w[1],
            "composite (signed, unsigned) spans must sort in tuple-numeric order"
        );
    }
}

// ── seek_by_index_range tests ────────────────────────────────────────────
//
// Ordered range scans over a secondary index, expressed as the half-open cut
// interval `[start, end)` the SQL planner sends: `x > v` ⇒ start `After(v)`,
// `x >= v` ⇒ start `Before(v)`, `x < v` ⇒ end `Before(v)`, `x <= v` ⇒ end
// `After(v)`, and an unconstrained side ⇒ the column type's edge cut.

use gnitz_wire::{
    Cut::{self, After, Before},
    RangeDescriptor, TypeCode,
};

/// The U64 type-edge cuts — what the planner sends for an unconstrained side
/// on a U64 range column (`Cut::type_edges`, the planner's own mapping).
const OPEN_BELOW: Cut = Cut::type_edges(TypeCode::U64).unwrap().0;
const OPEN_ABOVE: Cut = Cut::type_edges(TypeCode::U64).unwrap().1;

/// Collect the positive-weight source PKs returned by a range scan, sorted.
fn range_pks(engine: &mut CatalogEngine, tid: i64, cols: &[u32], eq: &[u128], start: Cut, end: Cut) -> Vec<u128> {
    let desc = RangeDescriptor::new(eq, start, end);
    let r = engine.seek_by_index_range(tid, cols, &desc).unwrap().0;
    let mut pks: Vec<u128> = match r {
        Some(b) => (0..b.count)
            .filter(|&i| b.get_weight(i) > 0)
            .map(|i| b.get_pk(i))
            .collect(),
        None => Vec::new(),
    };
    pks.sort();
    pks
}

#[test]
fn test_seek_by_index_range_unsigned_pure_range() {
    let (mut engine, tid, dir) = table_fixture(
        "catalog_range_unsigned",
        &[col_def("id", type_code::U64), col_def("x", type_code::U64)],
    );
    engine.create_index("public.t", &["x"], false).unwrap();
    let schema = engine.get_schema(tid).unwrap();

    // x ∈ {0,10,20,30} at PKs {1,2,3,4}.
    let mut bb = BatchBuilder::new(schema);
    for (pk, x) in [(1u128, 0u64), (2, 10), (3, 20), (4, 30)] {
        bb.begin_row(pk, 1);
        bb.put_u64(x);
        bb.end_row();
    }
    engine.ingest_to_family(tid, &bb.finish()).unwrap();
    engine.flush_family(tid).unwrap();

    let c = &[1u32];
    // x > 10  → {20,30} = PKs {3,4}
    assert_eq!(range_pks(&mut engine, tid, c, &[], After(10), OPEN_ABOVE), vec![3, 4]);
    // x >= 10 → {10,20,30} = PKs {2,3,4}
    assert_eq!(
        range_pks(&mut engine, tid, c, &[], Before(10), OPEN_ABOVE),
        vec![2, 3, 4]
    );
    // x < 20  → {0,10} = PKs {1,2}
    assert_eq!(range_pks(&mut engine, tid, c, &[], OPEN_BELOW, Before(20)), vec![1, 2]);
    // x <= 20 → {0,10,20} = PKs {1,2,3}
    assert_eq!(
        range_pks(&mut engine, tid, c, &[], OPEN_BELOW, After(20)),
        vec![1, 2, 3]
    );
    // 10 < x < 30 → {20} = PK {3}
    assert_eq!(range_pks(&mut engine, tid, c, &[], After(10), Before(30)), vec![3]);
    // 10 <= x <= 20 → {10,20} = PKs {2,3}
    assert_eq!(range_pks(&mut engine, tid, c, &[], Before(10), After(20)), vec![2, 3]);

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_seek_by_index_range_signed_between() {
    let (mut engine, tid, dir) = table_fixture(
        "catalog_range_signed",
        &[col_def("id", type_code::U64), col_def("x", type_code::I32)],
    );
    engine.create_index("public.t", &["x"], false).unwrap();
    let schema = engine.get_schema(tid).unwrap();

    // x ∈ {-10,-5,0,5,10} at PKs {1..5}. The cff7c58 payoff: OPK(-5) < OPK(5).
    let mut bb = BatchBuilder::new(schema);
    for (pk, x) in [(1u128, -10i32), (2, -5), (3, 0), (4, 5), (5, 10)] {
        bb.begin_row(pk, 1);
        bb.put_u32(x as u32);
        bb.end_row();
    }
    engine.ingest_to_family(tid, &bb.finish()).unwrap();
    engine.flush_family(tid).unwrap();

    let pk = |v: i32| (v as u32) as u128;
    let c = &[1u32];
    // The I32 type-edge cuts an unconstrained side widens to.
    let (open_below, open_above) = Cut::type_edges(TypeCode::I32).unwrap();
    // x BETWEEN -5 AND 5 → {-5,0,5} = PKs {2,3,4} (contiguous signed interval).
    assert_eq!(
        range_pks(&mut engine, tid, c, &[], Before(pk(-5)), After(pk(5))),
        vec![2, 3, 4]
    );
    // x > -5 → {0,5,10} = PKs {3,4,5}
    assert_eq!(
        range_pks(&mut engine, tid, c, &[], After(pk(-5)), open_above),
        vec![3, 4, 5]
    );
    // x < 0 → {-10,-5} = PKs {1,2}
    assert_eq!(
        range_pks(&mut engine, tid, c, &[], open_below, Before(pk(0))),
        vec![1, 2]
    );

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_seek_by_index_range_composite_eq_prefix() {
    let (mut engine, tid, dir) = table_fixture(
        "catalog_range_composite",
        &[
            col_def("id", type_code::U64),
            col_def("a", type_code::U64),
            col_def("b", type_code::U64),
        ],
    );
    engine.create_index("public.t", &["a", "b"], false).unwrap();
    let schema = engine.get_schema(tid).unwrap();

    // (a,b): three a==7 rows, one a==8 row (must never enter the a==7 scan).
    let mut bb = BatchBuilder::new(schema);
    for (pk, a, b) in [(1u128, 7u64, 10u64), (2, 7, 49), (3, 7, 50), (4, 8, 0)] {
        bb.begin_row(pk, 1);
        bb.put_u64(a);
        bb.put_u64(b);
        bb.end_row();
    }
    engine.ingest_to_family(tid, &bb.finish()).unwrap();
    engine.flush_family(tid).unwrap();

    let c = &[1u32, 2u32]; // index (a, b)
                           // a = 7 AND b < 50 → {(7,10),(7,49)} = PKs {1,2}; (8,0) is absent (the end
                           // cut sits inside the a==7 group's key space).
    assert_eq!(range_pks(&mut engine, tid, c, &[7], OPEN_BELOW, Before(50)), vec![1, 2]);
    // a = 7 AND b <= 50 → {(7,10),(7,49),(7,50)} = PKs {1,2,3}
    assert_eq!(
        range_pks(&mut engine, tid, c, &[7], OPEN_BELOW, After(50)),
        vec![1, 2, 3]
    );
    // a = 7 AND b > 10 → {(7,49),(7,50)} = PKs {2,3}; the end cut After(u64::MAX)
    // carries into the equality prefix — the first a==8 key — so the scan stops
    // exactly at the group boundary.
    assert_eq!(range_pks(&mut engine, tid, c, &[7], After(10), OPEN_ABOVE), vec![2, 3]);

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_seek_by_index_range_open_ended() {
    let (mut engine, tid, dir) = table_fixture(
        "catalog_range_open",
        &[col_def("id", type_code::U64), col_def("x", type_code::U64)],
    );
    engine.create_index("public.t", &["x"], false).unwrap();
    let schema = engine.get_schema(tid).unwrap();

    let mut bb = BatchBuilder::new(schema);
    for (pk, x) in [(1u128, 1u64), (2, 2), (3, 3), (4, 4)] {
        bb.begin_row(pk, 1);
        bb.put_u64(x);
        bb.end_row();
    }
    engine.ingest_to_family(tid, &bb.finish()).unwrap();
    engine.flush_family(tid).unwrap();

    let c = &[1u32];
    // x > 2 (unbounded above) → {3,4}
    assert_eq!(range_pks(&mut engine, tid, c, &[], After(2), OPEN_ABOVE), vec![3, 4]);
    // x < 3 (unbounded below) → {1,2}
    assert_eq!(range_pks(&mut engine, tid, c, &[], OPEN_BELOW, Before(3)), vec![1, 2]);
    // both edges (a saturated `x < HUGE`) → every indexed row.
    assert_eq!(
        range_pks(&mut engine, tid, c, &[], OPEN_BELOW, OPEN_ABOVE),
        vec![1, 2, 3, 4]
    );

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_seek_by_index_range_exclusive_lower_large_dup_group() {
    let (mut engine, tid, dir) = table_fixture(
        "catalog_range_dupgroup",
        &[col_def("id", type_code::U64), col_def("x", type_code::U64)],
    );
    engine.create_index("public.t", &["x"], false).unwrap();
    let schema = engine.get_schema(tid).unwrap();

    // A large x==10 duplicate group (distinct source PKs, including u64::MAX whose
    // OPK source-PK suffix is all-0xFF), plus two strictly-greater rows.
    let mut bb = BatchBuilder::new(schema);
    for pk in [1u128, 2, 3, 4, 5, u64::MAX as u128] {
        bb.begin_row(pk, 1);
        bb.put_u64(10);
        bb.end_row();
    }
    bb.begin_row(100, 1);
    bb.put_u64(20);
    bb.end_row();
    bb.begin_row(101, 1);
    bb.put_u64(30);
    bb.end_row();
    engine.ingest_to_family(tid, &bb.finish()).unwrap();
    engine.flush_family(tid).unwrap();

    let c = &[1u32];
    // x > 10: After(10) cuts past the whole duplicate group — including the
    // all-0xFF-source-PK member — in one O(log N) seek, no per-row skip.
    assert_eq!(
        range_pks(&mut engine, tid, c, &[], After(10), OPEN_ABOVE),
        vec![100, 101]
    );
    // x >= 10: Before(10) keeps the whole group plus the greater rows.
    assert_eq!(
        range_pks(&mut engine, tid, c, &[], Before(10), OPEN_ABOVE),
        vec![1, 2, 3, 4, 5, 100, 101, u64::MAX as u128]
    );

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_seek_by_index_range_retraction() {
    let (mut engine, tid, dir) = table_fixture(
        "catalog_range_retract",
        &[col_def("id", type_code::U64), col_def("x", type_code::U64)],
    );
    engine.create_index("public.t", &["x"], false).unwrap();
    let schema = engine.get_schema(tid).unwrap();

    let mut bb = BatchBuilder::new(schema);
    for (pk, x) in [(1u128, 5u64), (2, 15), (3, 25)] {
        bb.begin_row(pk, 1);
        bb.put_u64(x);
        bb.end_row();
    }
    engine.ingest_to_family(tid, &bb.finish()).unwrap();
    engine.flush_family(tid).unwrap();

    // Retract PK 2 (x=15) → net weight 0; it must vanish from the range scan
    // (no ghost entry) at both the index and the source resolve.
    let schema = engine.get_schema(tid).unwrap();
    let mut rb = BatchBuilder::new(schema);
    rb.begin_row(2, -1);
    rb.put_u64(15);
    rb.end_row();
    engine.ingest_to_family(tid, &rb.finish()).unwrap();
    engine.flush_family(tid).unwrap();

    let c = &[1u32];
    // x > 0 → {5,25} = PKs {1,3}; PK 2 (retracted x=15) absent.
    assert_eq!(range_pks(&mut engine, tid, c, &[], After(0), OPEN_ABOVE), vec![1, 3]);

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_seek_by_index_range_null_excluded() {
    let (mut engine, tid, dir) = table_fixture(
        "catalog_range_null",
        &[
            col_def("id", type_code::U64),
            ColumnDef {
                name: "x".into(),
                type_code: type_code::I64,
                is_nullable: true,
                fk_table_id: 0,
                fk_col_idx: 0,
                is_hidden: false,
            },
        ],
    );
    engine.create_index("public.t", &["x"], false).unwrap();
    let schema = engine.get_schema(tid).unwrap();

    // PK 2 has x = NULL — absent from the index, so excluded from every range.
    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(1u128, 1);
    bb.put_u64(5);
    bb.end_row();
    bb.begin_row(2u128, 1);
    bb.put_null();
    bb.end_row();
    bb.begin_row(3u128, 1);
    bb.put_u64(15);
    bb.end_row();
    engine.ingest_to_family(tid, &bb.finish()).unwrap();
    engine.flush_family(tid).unwrap();

    let c = &[1u32];
    // The I64 type-edge cuts (the range column is I64 here, not U64).
    let (open_below, open_above) = Cut::type_edges(TypeCode::I64).unwrap();
    // x < 100 → {5,15} = PKs {1,3}; the NULL-x PK 2 never appears.
    assert_eq!(range_pks(&mut engine, tid, c, &[], open_below, Before(100)), vec![1, 3]);
    // a full edge-to-edge scan also excludes NULL (index has no entry for it).
    assert_eq!(range_pks(&mut engine, tid, c, &[], open_below, open_above), vec![1, 3]);

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_seek_by_index_range_no_range_column_errs() {
    let (mut engine, tid, dir) = table_fixture(
        "catalog_range_arity",
        &[col_def("id", type_code::U64), col_def("x", type_code::U64)],
    );
    engine.create_index("public.t", &["x"], false).unwrap();

    // n_eq == arity → no range column; the pub method must self-guard with Err,
    // never panic indexing past the column list.
    let r = engine.seek_by_index_range(tid, &[1], &RangeDescriptor::new(&[10], After(0), OPEN_ABOVE));
    assert!(r.is_err(), "n_eq == index arity must be rejected");
    assert!(r.err().unwrap().contains("no range column"));

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_seek_by_index_range_exclusive_lower_type_max() {
    // Start-cut successor overflow: `x > u64::MAX` starts at After(MAX), the
    // byte successor of the maximal group — which does not exist (`+∞`) →
    // provably empty. `x >= u64::MAX` starts Before(MAX) and keeps the group.
    let (mut engine, tid, dir) = table_fixture(
        "catalog_range_excl_lower_max",
        &[col_def("id", type_code::U64), col_def("x", type_code::U64)],
    );
    engine.create_index("public.t", &["x"], false).unwrap();
    let schema = engine.get_schema(tid).unwrap();

    let mut bb = BatchBuilder::new(schema);
    for (pk, x) in [(1u128, 10u64), (2, 20), (3, u64::MAX), (4, u64::MAX)] {
        bb.begin_row(pk, 1);
        bb.put_u64(x);
        bb.end_row();
    }
    engine.ingest_to_family(tid, &bb.finish()).unwrap();
    engine.flush_family(tid).unwrap();

    let c = &[1u32];
    let max = u64::MAX as u128;
    // x > u64::MAX → nothing above the maximal group.
    assert_eq!(
        range_pks(&mut engine, tid, c, &[], After(max), OPEN_ABOVE),
        Vec::<u128>::new()
    );
    // x >= u64::MAX → exactly the two MAX rows.
    assert_eq!(range_pks(&mut engine, tid, c, &[], Before(max), OPEN_ABOVE), vec![3, 4]);

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_seek_by_index_range_inclusive_upper_type_max() {
    // End-cut successor overflow: `x <= u64::MAX` ends at After(MAX), the byte
    // successor of the maximal group — which does not exist (`+∞`) → scan to
    // the table end (every row, including the MAX one).
    let (mut engine, tid, dir) = table_fixture(
        "catalog_range_incl_upper_max",
        &[col_def("id", type_code::U64), col_def("x", type_code::U64)],
    );
    engine.create_index("public.t", &["x"], false).unwrap();
    let schema = engine.get_schema(tid).unwrap();

    let mut bb = BatchBuilder::new(schema);
    for (pk, x) in [(1u128, 10u64), (2, 20), (3, u64::MAX)] {
        bb.begin_row(pk, 1);
        bb.put_u64(x);
        bb.end_row();
    }
    engine.ingest_to_family(tid, &bb.finish()).unwrap();
    engine.flush_family(tid).unwrap();

    let c = &[1u32];
    // x <= u64::MAX → every indexed row, the MAX row included.
    assert_eq!(
        range_pks(&mut engine, tid, c, &[], OPEN_BELOW, After(u64::MAX as u128)),
        vec![1, 2, 3]
    );

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_seek_by_index_range_carry_ripples_into_eq_prefix() {
    // An After cut on a range slot at the type max carries the successor out of
    // the range slot and into the equality prefix. Index (a, b); the (8, 0) row
    // must never leak into an a==7 scan.
    let (mut engine, tid, dir) = table_fixture(
        "catalog_range_carry_eq",
        &[
            col_def("id", type_code::U64),
            col_def("a", type_code::U64),
            col_def("b", type_code::U64),
        ],
    );
    engine.create_index("public.t", &["a", "b"], false).unwrap();
    let schema = engine.get_schema(tid).unwrap();

    let mut bb = BatchBuilder::new(schema);
    for (pk, a, b) in [(1u128, 7u64, u64::MAX), (2, 8, 0u64)] {
        bb.begin_row(pk, 1);
        bb.put_u64(a);
        bb.put_u64(b);
        bb.end_row();
    }
    engine.ingest_to_family(tid, &bb.finish()).unwrap();
    engine.flush_family(tid).unwrap();

    let c = &[1u32, 2u32]; // index (a, b)
    let max = u64::MAX as u128;
    // a = 7 AND b > u64::MAX → empty: the start cut After(7, MAX) carries onto
    // the first a==8 key, where the end cut already sits, so start == end. The
    // (8, 0) row must not leak in.
    assert_eq!(
        range_pks(&mut engine, tid, c, &[7], After(max), OPEN_ABOVE),
        Vec::<u128>::new()
    );
    // a = 7 AND b <= u64::MAX → only (7, u64::MAX); (8, 0) is a different group.
    assert_eq!(range_pks(&mut engine, tid, c, &[7], OPEN_BELOW, After(max)), vec![1]);

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── collect-then-resolve: sorted source-PK resolution ────────────────────
//
// `seek_by_index` (prefix) and `seek_by_index_range` collect the matched
// source PKs, sort them (skipping the sort only for a full-arity equality
// seek, whose single duplicate group is already ascending), then resolve in
// one monotone forward sweep. These tests pin the observable contract: the
// resolved multiset, net-weight handling, the wide-PK byte path, and the
// sort-skip branch.

/// Collect `(pk, payload_col0, weight)` for every positive-weight row of a
/// narrow-PK result batch, sorted by PK — the order-insensitive reference form
/// for comparing a seek result against an expected multiset.
fn result_triples(r: Option<Batch>) -> Vec<(u128, u64, i64)> {
    let mut out: Vec<(u128, u64, i64)> = match r {
        Some(b) => {
            let col = b.col_data(0);
            (0..b.count)
                .filter(|&i| b.get_weight(i) > 0)
                .map(|i| {
                    let v = u64::from_le_bytes(col[i * 8..i * 8 + 8].try_into().unwrap());
                    (b.get_pk(i), v, b.get_weight(i))
                })
                .collect()
        }
        None => Vec::new(),
    };
    out.sort();
    out
}

#[test]
fn test_seek_by_index_range_multi_group_sorted_with_retraction() {
    // A range spanning ≥ 2 duplicate groups, with source PKs deliberately
    // scrambled so the index-emission order (2,5,8,1,3,7) interleaves across
    // groups and the resolve sort genuinely reorders to (1,2,3,5,7,8). The
    // resolved multiset must match a scan-and-filter reference; a retraction in
    // one group must drop that row at net weight 0.
    let (mut engine, tid, dir) = table_fixture(
        "catalog_range_multigroup",
        &[col_def("id", type_code::U64), col_def("x", type_code::U64)],
    );
    engine.create_index("public.t", &["x"], false).unwrap();
    let schema = engine.get_schema(tid).unwrap();

    // x=10 at PKs {5,2,8}, x=20 at PKs {3,7,1} — two duplicate groups.
    let mut bb = BatchBuilder::new(schema);
    for (pk, x) in [(5u128, 10u64), (2, 10), (8, 10), (3, 20), (7, 20), (1, 20)] {
        bb.begin_row(pk, 1);
        bb.put_u64(x);
        bb.end_row();
    }
    engine.ingest_to_family(tid, &bb.finish()).unwrap();
    engine.flush_family(tid).unwrap();

    // x ∈ [10, 20] → all six rows, each at weight 1.
    let r = engine
        .seek_by_index_range(tid, &[1], &RangeDescriptor::new(&[], Before(10), After(20)))
        .unwrap()
        .0;
    assert_eq!(
        result_triples(r),
        vec![(1, 20, 1), (2, 10, 1), (3, 20, 1), (5, 10, 1), (7, 20, 1), (8, 10, 1)],
    );

    // Retract PK 5 (x=10) → net weight 0; it must vanish from the scan.
    let schema = engine.get_schema(tid).unwrap();
    let mut rb = BatchBuilder::new(schema);
    rb.begin_row(5, -1);
    rb.put_u64(10);
    rb.end_row();
    engine.ingest_to_family(tid, &rb.finish()).unwrap();
    engine.flush_family(tid).unwrap();

    let r = engine
        .seek_by_index_range(tid, &[1], &RangeDescriptor::new(&[], Before(10), After(20)))
        .unwrap()
        .0;
    assert_eq!(
        result_triples(r),
        vec![(1, 20, 1), (2, 10, 1), (3, 20, 1), (7, 20, 1), (8, 10, 1)],
        "retracted PK 5 must be absent at net weight 0",
    );

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_seek_by_index_prefix_multi_group_sorted() {
    // A leading-prefix seek (a only) over a composite index (a, b) spans every
    // b-group under that a. The source PKs interleave across the b-groups
    // (emission 2,5,1,8), so the prefix path must sort (natives.len() <
    // col_indices.len()) before resolving. A row under a different a must not
    // leak in.
    let (mut engine, tid, dir) = table_fixture(
        "catalog_prefix_multigroup",
        &[
            col_def("id", type_code::U64),
            col_def("a", type_code::U64),
            col_def("b", type_code::U64),
        ],
    );
    engine.create_index("public.t", &["a", "b"], false).unwrap();
    let schema = engine.get_schema(tid).unwrap();

    // a=7: b=100 at PKs {5,2}, b=200 at PKs {8,1}. a=8 at PK 99 (must not match).
    let mut bb = BatchBuilder::new(schema);
    for (pk, a, b) in [
        (5u128, 7u64, 100u64),
        (2, 7, 100),
        (8, 7, 200),
        (1, 7, 200),
        (99, 8, 100),
    ] {
        bb.begin_row(pk, 1);
        bb.put_u64(a);
        bb.put_u64(b);
        bb.end_row();
    }
    engine.ingest_to_family(tid, &bb.finish()).unwrap();
    engine.flush_family(tid).unwrap();

    // Prefix seek a=7 → PKs {1,2,5,8}, all with a=7; PK 99 (a=8) absent.
    let r = engine
        .seek_by_index(tid, &[1, 2], &[7u128])
        .unwrap()
        .0
        .expect("prefix seek must find the a=7 rows");
    let mut pks: Vec<u128> = (0..r.count)
        .filter(|&i| r.get_weight(i) > 0)
        .map(|i| r.get_pk(i))
        .collect();
    pks.sort();
    assert_eq!(pks, vec![1, 2, 5, 8]);
    assert!(
        (0..r.count).all(|i| r.get_weight(i) == 1),
        "every matched row is at weight 1"
    );

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_seek_by_index_range_empty_interval_short_circuits() {
    // A non-degenerate `[start, end)` (start < end) that nonetheless straddles a
    // value gap matches no row: the walk collects nothing, so the `pks.is_empty()`
    // short-circuit returns `Ok(None)` without opening the base cursor. (The
    // `start ≥ end` / `+∞` short-circuits fire *before* the walk and so do not
    // exercise this path.)
    let (mut engine, tid, dir) = table_fixture(
        "catalog_range_empty_interval",
        &[col_def("id", type_code::U64), col_def("x", type_code::U64)],
    );
    engine.create_index("public.t", &["x"], false).unwrap();
    let schema = engine.get_schema(tid).unwrap();

    // Values {10, 30}; the half-open interval for 15 < x < 25 is non-empty but
    // contains no indexed value.
    let mut bb = BatchBuilder::new(schema);
    for (pk, x) in [(1u128, 10u64), (2, 30)] {
        bb.begin_row(pk, 1);
        bb.put_u64(x);
        bb.end_row();
    }
    engine.ingest_to_family(tid, &bb.finish()).unwrap();
    engine.flush_family(tid).unwrap();

    let r = engine
        .seek_by_index_range(tid, &[1], &RangeDescriptor::new(&[], After(15), Before(25)))
        .unwrap()
        .0;
    assert!(
        r.is_none(),
        "an empty-but-valid interval returns Ok(None) via pks.is_empty()"
    );

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_seek_by_index_range_wide_pk_collect_sort_resolve() {
    // Regression guard for dropping the `MAX_PK_BYTES` scratch-copy: a wide
    // (`src_pk_stride` = 24 > 16) source PK must round-trip through
    // collect → sort → resolve carrying its full width. Each PK varies past byte
    // 16 (col c), so a 16-byte-truncated key would resolve the wrong row; `PkBuf`
    // stores all 24 bytes inline and the resolve seeks the full key. The index
    // emits the entries by indexed value x (collected order (3,_,1),(1,_,5),
    // (2,_,9)), which is not source-PK order, so the resolve sort genuinely
    // reorders by the full 24-byte key.
    //
    // Wide PKs are DDL-rejected for base tables, so this builds the DAG table and
    // index circuit directly (as `wide_pk_validation.rs` does). The leading PK
    // column is distinct per row: the base flush orders the shard by the wide PK,
    // which the resolve's binary-search seek relies on.
    use crate::query::{DagEngine, RelationKind, StoreHandle};
    use crate::schema::{SchemaColumn, SchemaDescriptor};
    use crate::storage::{RecoverySource, Table};

    fn u64c() -> SchemaColumn {
        SchemaColumn::new(type_code::U64, 0)
    }
    fn pk24(a: u64, b: u64, c: u64) -> [u8; 24] {
        // Unsigned compound PK: OPK == big-endian per column.
        let mut p = [0u8; 24];
        p[0..8].copy_from_slice(&a.to_be_bytes());
        p[8..16].copy_from_slice(&b.to_be_bytes());
        p[16..24].copy_from_slice(&c.to_be_bytes());
        p
    }

    let dir = temp_dir("catalog_range_wide_pk");
    let mut engine = CatalogEngine::open(&dir).unwrap();
    let tid = engine.next_table_id;

    // pk_stride = 24: three U64 PK columns + one U64 payload `x` (source col 3).
    let schema = SchemaDescriptor::new(&[u64c(), u64c(), u64c(), u64c()], &[0, 1, 2]);
    let idx_schema = make_index_schema(&[3], &schema).unwrap();

    // (pk, x): distinct leading column, distinct trailing column (past byte 16);
    // indexed values 10/20/30 chosen so the index emission order (by x) differs
    // from the source-PK sort order.
    let rows: [([u8; 24], u64); 3] = [(pk24(3, 0, 1), 10), (pk24(1, 0, 5), 20), (pk24(2, 0, 9), 30)];
    let mut bb = Batch::with_capacity(schema, rows.len());
    for &(pk, x) in &rows {
        bb.extend_pk_bytes(&pk);
        bb.extend_weight(&1i64.to_le_bytes());
        bb.extend_null_bmp(&0u64.to_le_bytes());
        bb.extend_col(0, &x.to_le_bytes());
        bb.count += 1;
    }
    // Rows were appended out of PK order; the batch is `Raw` (the constructor
    // default), so the ingest's `into_consolidated` sorts the shard.
    let idx_batch = DagEngine::batch_project_index(
        &bb,
        &crate::schema::IndexKeySpec::new(&[3], &schema, &idx_schema),
        &idx_schema,
    );

    let mut base = Box::new(
        Table::new(
            &format!("{dir}/base"),
            schema,
            tid as u32,
            256 * 1024,
            RecoverySource::Rederive,
        )
        .unwrap(),
    );
    let mut idx = Table::new(
        &format!("{dir}/idx"),
        idx_schema,
        tid as u32 + 1,
        256 * 1024,
        RecoverySource::Rederive,
    )
    .unwrap();
    base.ingest_owned_batch(bb).unwrap();
    base.flush().unwrap();
    idx.ingest_owned_batch(idx_batch).unwrap();
    idx.flush().unwrap();

    engine.dag.register_table(
        tid,
        StoreHandle::Borrowed(&mut *base as *mut Table),
        schema,
        RelationKind::BaseTable,
        0,
        dir.clone(),
    );
    engine
        .dag
        .add_index_circuit(tid, &[3], tid + 1, Box::new(idx), idx_schema, false);

    // x ∈ [10, 30] → all three wide-PK rows, resolved by their full 24-byte key.
    let r = engine
        .seek_by_index_range(tid, &[3], &RangeDescriptor::new(&[], Before(10), After(30)))
        .unwrap()
        .0
        .expect("wide-PK range scan must resolve all three rows");
    let mut got: Vec<([u8; 24], u64)> = (0..r.count)
        .filter(|&i| r.get_weight(i) > 0)
        .map(|i| {
            let pk: [u8; 24] = r.get_pk_bytes(i).try_into().unwrap();
            let x = u64::from_le_bytes(r.col_data(0)[i * 8..i * 8 + 8].try_into().unwrap());
            (pk, x)
        })
        .collect();
    got.sort();
    let mut want = vec![(pk24(3, 0, 1), 10), (pk24(1, 0, 5), 20), (pk24(2, 0, 9), 30)];
    want.sort();
    assert_eq!(got, want, "wide source PKs must resolve at full 24-byte width");

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_seek_by_index_full_arity_nonunique_group_ascending() {
    // A full-arity equality seek (`natives.len() == col_indices.len()`) on a
    // NON-unique index pins the one duplicate group and returns EVERY member.
    // The rows are *inserted* out of PK order (9,3,6); the result comes back in
    // ascending PK order (3,6,9) — the gather's storage-order sweep.
    let (mut engine, tid, dir) = table_fixture(
        "catalog_full_arity_skip",
        &[col_def("id", type_code::U64), col_def("x", type_code::U64)],
    );
    engine.create_index("public.t", &["x"], false).unwrap();
    let schema = engine.get_schema(tid).unwrap();

    // x=50 at PKs {9,3,6} (inserted scrambled); x=60 at PK 1 (must not match).
    let mut bb = BatchBuilder::new(schema);
    for (pk, x) in [(9u128, 50u64), (3, 50), (6, 50), (1, 60)] {
        bb.begin_row(pk, 1);
        bb.put_u64(x);
        bb.end_row();
    }
    engine.ingest_to_family(tid, &bb.finish()).unwrap();
    engine.flush_family(tid).unwrap();

    let r = engine
        .seek_by_index(tid, &[1], &[50u128])
        .unwrap()
        .0
        .expect("full-arity equality seek must find the x=50 group");
    // Read PKs in *result-batch order* (no re-sort): the gather emits in
    // ascending source-PK storage order regardless of insertion order.
    let pks_in_order: Vec<u128> = (0..r.count).map(|i| r.get_pk(i)).collect();
    assert_eq!(
        pks_in_order,
        vec![3, 6, 9],
        "every duplicate-group member returned, in ascending source-PK order"
    );
    assert!((0..r.count).all(|i| r.get_weight(i) == 1));

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

/// Collect `(pk, a, b, weight)` (payload slots 0, 1) for every positive-weight
/// row, sorted — the composite-index reference form.
fn result_ab_quads(r: Option<Batch>) -> Vec<(u128, u64, u64, i64)> {
    let mut out: Vec<(u128, u64, u64, i64)> = match r {
        Some(b) => (0..b.count)
            .filter(|&i| b.get_weight(i) > 0)
            .map(|i| {
                (
                    b.get_pk(i),
                    b.read_payload_u64(i, 0),
                    b.read_payload_u64(i, 1),
                    b.get_weight(i),
                )
            })
            .collect(),
        None => Vec::new(),
    };
    out.sort();
    out
}

#[test]
fn test_seek_by_index_composite_prefix_null_gate() {
    // Index (a, b) with b nullable; rows (pk=1,a=5,b=NULL) and (pk=1,a=5,b=9).
    // The NULL-b row is absent from the index (`batch_project_index` skips a row
    // with a NULL in any indexed column), so the prefix seek on a=5 must return
    // only (1,5,9). The full-arity gather spec re-applies the all-column NULL
    // gate; a seek-side (prefix-arity) spec would resurrect the NULL row.
    let (mut engine, tid, dir) = table_fixture(
        "catalog_composite_prefix_null",
        &[
            col_def("id", type_code::U64),
            col_def("a", type_code::U64),
            ColumnDef {
                name: "b".into(),
                type_code: type_code::U64,
                is_nullable: true,
                fk_table_id: 0,
                fk_col_idx: 0,
                is_hidden: false,
            },
        ],
    );
    engine.create_index("public.t", &["a", "b"], false).unwrap();
    let schema = engine.get_schema(tid).unwrap();

    let mut bb = BatchBuilder::new(schema);
    // (1, 5, NULL)
    bb.begin_row(1u128, 1);
    bb.put_u64(5);
    bb.put_null();
    bb.end_row();
    // (1, 5, 9)
    bb.begin_row(1u128, 1);
    bb.put_u64(5);
    bb.put_u64(9);
    bb.end_row();
    engine.ingest_to_family(tid, &bb.finish()).unwrap();
    engine.flush_family(tid).unwrap();

    let r = engine.seek_by_index(tid, &[1, 2], &[5u128]).unwrap().0;
    assert_eq!(
        result_ab_quads(r),
        vec![(1, 5, 9, 1)],
        "the NULL-b row is not in the index and must not be resurrected by the gather"
    );

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_seek_by_index_multi_value_regression() {
    // The span filter must be behaviour-identical on a base table (one live row
    // per PK ⇒ resolving by PK equals resolving by span). Point seek and range
    // both return exactly the expected rows at weight 1.
    let (mut engine, tid, dir) = table_fixture(
        "catalog_multi_value_regression",
        &[col_def("id", type_code::U64), col_def("val", type_code::U64)],
    );
    engine.create_index("public.t", &["val"], false).unwrap();
    let schema = engine.get_schema(tid).unwrap();

    let mut bb = BatchBuilder::new(schema);
    for (pk, val) in [(1u128, 10u64), (2, 20), (3, 30)] {
        bb.begin_row(pk, 1);
        bb.put_u64(val);
        bb.end_row();
    }
    engine.ingest_to_family(tid, &bb.finish()).unwrap();
    engine.flush_family(tid).unwrap();

    let r = engine.seek_by_index(tid, &[1], &[20u128]).unwrap().0;
    assert_eq!(result_triples(r), vec![(2, 20, 1)]);

    // val ∈ [10, 20]
    let rr = engine
        .seek_by_index_range(tid, &[1], &RangeDescriptor::new(&[], Before(10), After(20)))
        .unwrap()
        .0;
    assert_eq!(result_triples(rr), vec![(1, 10, 1), (2, 20, 1)]);

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}
