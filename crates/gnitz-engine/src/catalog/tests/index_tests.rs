use super::*;

// ── test_index_creation_and_backfill ────────────────────────────────────

#[test]
fn test_index_creation_and_backfill() {
    let dir = temp_dir("index_backfill");
    let mut engine = CatalogEngine::open(&dir).unwrap();

    let cols = vec![u64_col_def("id"), i64_col_def("val")];
    let tid = engine.create_table("public.tfanout", &cols, &[0], true).unwrap();

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
    let idx_id = engine.create_index("public.tfanout", "val", false).unwrap();
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

    let cols = vec![u64_col_def("id"), i64_col_def("val")];
    let tid = engine.create_table("public.tfanout", &cols, &[0], true).unwrap();
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
    engine.create_index("public.tfanout", "val", false).unwrap();

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
    assert_eq!(idx_count, 6, "Index fanout: expected 6, got {}", idx_count);

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
    let schema = idx_tab_schema();
    let flushes = 40u64;
    for i in 0..flushes {
        let mut bb = BatchBuilder::new(schema);
        bb.begin_row(i as u128, 1);
        bb.put_u64(0);                 // owner_id
        bb.put_u64(0);                 // owner_kind
        bb.put_u64(0);                 // source_col_idx
        bb.put_string(&format!("idx{}", i));
        bb.put_u64(0);                 // is_unique
        bb.put_string("");             // cache_directory
        bb.end_row();
        ingest_batch_into(&mut engine.sys_indices, &bb.finish());
        engine.flush_family(IDX_TAB_ID).unwrap();
    }
    let shards = engine.sys_indices.all_shard_arcs().len();
    assert!(
        (shards as u64) < flushes / 2,
        "system catalog L0 must be compacted: {} shards after {} flushes",
        shards, flushes
    );
    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_create_index_duplicate_rejected() {
    // Creating the same index twice must fail on the second attempt rather
    // than silently orphaning the first index circuit.
    let dir = temp_dir("idx_dup_create");
    let mut engine = CatalogEngine::open(&dir).unwrap();
    let cols = vec![u64_col_def("id"), i64_col_def("val")];
    engine.create_table("public.t", &cols, &[0], true).unwrap();

    let first = engine.create_index("public.t", "val", false);
    assert!(first.is_ok(), "first index creation should succeed");
    assert!(engine.has_index_by_name("public__t__idx_val"));

    let second = engine.create_index("public.t", "val", false);
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
    let dir = temp_dir("idx_broadcast_poison");
    let mut engine = CatalogEngine::open(&dir).unwrap();
    let cols = vec![u64_col_def("id"), u64_col_def("val")];
    let tid = engine.create_table("public.t", &cols, &[0], false).unwrap();
    let schema = engine.get_schema(tid).unwrap();

    // Two rows sharing val=42.
    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(1u128, 1); bb.put_u64(42); bb.end_row();
    bb.begin_row(2u128, 1); bb.put_u64(42); bb.end_row();
    engine.ingest_to_family(tid, &bb.finish()).unwrap();
    engine.flush_family(tid).unwrap();

    // Clear broadcasts accumulated by table/column creation + ingest.
    let _ = engine.drain_pending_broadcasts();

    // UNIQUE index over duplicate values must fail.
    let res = engine.create_index("public.t", "val", true);
    assert!(res.is_err(), "unique index over duplicate values must fail");
    // The rejection must carry the qualified table and column context.
    let err = res.unwrap_err();
    assert!(err.contains("public.t"), "create-index error should name the table: {err}");
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
    let dir = temp_dir("catalog_uidx_dup");
    let mut engine = CatalogEngine::open(&dir).unwrap();
    let cols = vec![u64_col_def("id"), u64_col_def("val")];
    let tid = engine.create_table("public.t", &cols, &[0], false).unwrap();
    engine.create_index("public.t", "val", true).unwrap(); // unique index on val
    let schema = engine.get_schema(tid).unwrap();

    // Insert first row
    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(1u128, 1); bb.put_u64(42); bb.end_row();
    engine.ingest_to_family(tid, &bb.finish()).unwrap();
    engine.flush_family(tid).unwrap();

    // Insert second row with same val=42 → should violate unique index
    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(2u128, 1); bb.put_u64(42); bb.end_row();
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

#[test]
fn test_validate_unique_indices_batch_internal_dup() {
    let dir = temp_dir("catalog_uidx_batch_dup");
    let mut engine = CatalogEngine::open(&dir).unwrap();
    let cols = vec![u64_col_def("id"), u64_col_def("val")];
    let tid = engine.create_table("public.t", &cols, &[0], false).unwrap();
    engine.create_index("public.t", "val", true).unwrap();
    let schema = engine.get_schema(tid).unwrap();

    // Single batch with two rows sharing val=99 → duplicate in batch
    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(1u128, 1); bb.put_u64(99); bb.end_row();
    bb.begin_row(2u128, 1); bb.put_u64(99); bb.end_row();
    let result = engine.validate_unique_indices(tid, &bb.finish());
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("duplicate in batch"));

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_validate_unique_indices_upsert_same_value_ok() {
    let dir = temp_dir("catalog_uidx_upsert_same");
    let mut engine = CatalogEngine::open(&dir).unwrap();
    let cols = vec![u64_col_def("id"), u64_col_def("val")];
    let tid = engine.create_table("public.t", &cols, &[0], true).unwrap(); // unique_pk
    engine.create_index("public.t", "val", true).unwrap();
    let schema = engine.get_schema(tid).unwrap();

    // Insert PK=1, val=42
    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(1u128, 1); bb.put_u64(42); bb.end_row();
    engine.ingest_to_family(tid, &bb.finish()).unwrap();
    engine.flush_family(tid).unwrap();

    // UPSERT PK=1, val=42 (same value) → should succeed
    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(1u128, 1); bb.put_u64(42); bb.end_row();
    let result = engine.validate_unique_indices(tid, &bb.finish());
    assert!(result.is_ok());

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_validate_unique_indices_upsert_to_existing_value_fails() {
    let dir = temp_dir("catalog_uidx_upsert_conflict");
    let mut engine = CatalogEngine::open(&dir).unwrap();
    let cols = vec![u64_col_def("id"), u64_col_def("val")];
    let tid = engine.create_table("public.t", &cols, &[0], true).unwrap(); // unique_pk
    engine.create_index("public.t", "val", true).unwrap();
    let schema = engine.get_schema(tid).unwrap();

    // Insert PK=1, val=42 and PK=2, val=99
    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(1u128, 1); bb.put_u64(42); bb.end_row();
    bb.begin_row(2u128, 1); bb.put_u64(99); bb.end_row();
    engine.ingest_to_family(tid, &bb.finish()).unwrap();
    engine.flush_family(tid).unwrap();

    // UPSERT PK=1, val=99 → conflicts with PK=2's val=99
    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(1u128, 1); bb.put_u64(99); bb.end_row();
    let result = engine.validate_unique_indices(tid, &bb.finish());
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("Unique index violation"));

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_validate_unique_indices_distinct_values_ok() {
    let dir = temp_dir("catalog_uidx_distinct");
    let mut engine = CatalogEngine::open(&dir).unwrap();
    let cols = vec![u64_col_def("id"), u64_col_def("val")];
    let tid = engine.create_table("public.t", &cols, &[0], false).unwrap();
    engine.create_index("public.t", "val", true).unwrap();
    let schema = engine.get_schema(tid).unwrap();

    // Insert row with val=42
    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(1u128, 1); bb.put_u64(42); bb.end_row();
    engine.ingest_to_family(tid, &bb.finish()).unwrap();
    engine.flush_family(tid).unwrap();

    // Insert row with different val=99 → should succeed
    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(2u128, 1); bb.put_u64(99); bb.end_row();
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
// retraction of its (PK, value), or — on a unique_pk table, for an upsert row —
// the holder being itself an upserted PK).

#[test]
fn test_validate_unique_transfer_accepted() {
    let dir = temp_dir("catalog_uidx_transfer");
    let mut engine = CatalogEngine::open(&dir).unwrap();
    let cols = vec![u64_col_def("id"), u64_col_def("val")];
    let tid = engine.create_table("public.t", &cols, &[0], false).unwrap(); // non-unique_pk
    engine.create_index("public.t", "val", true).unwrap();
    let schema = engine.get_schema(tid).unwrap();

    // Committed: P1/val=5.
    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(1u128, 1); bb.put_u64(5); bb.end_row();
    engine.ingest_to_family(tid, &bb.finish()).unwrap();
    engine.flush_family(tid).unwrap();

    // Transfer val=5 from P1 to P2 in one batch: {retract P1/5, insert P2/5}.
    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(1u128, -1); bb.put_u64(5); bb.end_row();
    bb.begin_row(2u128, 1); bb.put_u64(5); bb.end_row();
    let result = engine.validate_unique_indices(tid, &bb.finish());
    assert!(result.is_ok(), "transfer should be accepted: {:?}", result);

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_validate_unique_swap_accepted() {
    let dir = temp_dir("catalog_uidx_swap");
    let mut engine = CatalogEngine::open(&dir).unwrap();
    let cols = vec![u64_col_def("id"), u64_col_def("val")];
    let tid = engine.create_table("public.t", &cols, &[0], true).unwrap(); // unique_pk
    engine.create_index("public.t", "val", true).unwrap();
    let schema = engine.get_schema(tid).unwrap();

    // Committed: P1/val=5, P2/val=6.
    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(1u128, 1); bb.put_u64(5); bb.end_row();
    bb.begin_row(2u128, 1); bb.put_u64(6); bb.end_row();
    engine.ingest_to_family(tid, &bb.finish()).unwrap();
    engine.flush_family(tid).unwrap();

    // Explicit swap: retract both, re-insert crossed.
    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(1u128, -1); bb.put_u64(5); bb.end_row();
    bb.begin_row(2u128, -1); bb.put_u64(6); bb.end_row();
    bb.begin_row(1u128, 1); bb.put_u64(6); bb.end_row();
    bb.begin_row(2u128, 1); bb.put_u64(5); bb.end_row();
    let result = engine.validate_unique_indices(tid, &bb.finish());
    assert!(result.is_ok(), "swap should be accepted: {:?}", result);

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_validate_unique_bulk_shift_accepted() {
    let dir = temp_dir("catalog_uidx_bulk_shift");
    let mut engine = CatalogEngine::open(&dir).unwrap();
    let cols = vec![u64_col_def("id"), u64_col_def("val")];
    let tid = engine.create_table("public.t", &cols, &[0], true).unwrap(); // unique_pk
    engine.create_index("public.t", "val", true).unwrap();
    let schema = engine.get_schema(tid).unwrap();

    // Committed dense sequence: (1,1),(2,2),(3,3).
    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(1u128, 1); bb.put_u64(1); bb.end_row();
    bb.begin_row(2u128, 1); bb.put_u64(2); bb.end_row();
    bb.begin_row(3u128, 1); bb.put_u64(3); bb.end_row();
    engine.ingest_to_family(tid, &bb.finish()).unwrap();
    engine.flush_family(tid).unwrap();

    // `UPDATE t SET val = val + 1`: same-PK +1 upserts, no retraction. The holder
    // of each new value is the previous row, itself upserted off that value.
    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(1u128, 1); bb.put_u64(2); bb.end_row();
    bb.begin_row(2u128, 1); bb.put_u64(3); bb.end_row();
    bb.begin_row(3u128, 1); bb.put_u64(4); bb.end_row();
    let result = engine.validate_unique_indices(tid, &bb.finish());
    assert!(result.is_ok(), "bulk shift should be accepted: {:?}", result);

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_validate_unique_bulk_swap_accepted() {
    let dir = temp_dir("catalog_uidx_bulk_swap");
    let mut engine = CatalogEngine::open(&dir).unwrap();
    let cols = vec![u64_col_def("id"), u64_col_def("val")];
    let tid = engine.create_table("public.t", &cols, &[0], true).unwrap(); // unique_pk
    engine.create_index("public.t", "val", true).unwrap();
    let schema = engine.get_schema(tid).unwrap();

    // Committed: (1,1),(2,2).
    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(1u128, 1); bb.put_u64(1); bb.end_row();
    bb.begin_row(2u128, 1); bb.put_u64(2); bb.end_row();
    engine.ingest_to_family(tid, &bb.finish()).unwrap();
    engine.flush_family(tid).unwrap();

    // `UPDATE t SET val = 3 - val`: same-PK +1 upserts swapping the two values.
    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(1u128, 1); bb.put_u64(2); bb.end_row();
    bb.begin_row(2u128, 1); bb.put_u64(1); bb.end_row();
    let result = engine.validate_unique_indices(tid, &bb.finish());
    assert!(result.is_ok(), "bulk swap should be accepted: {:?}", result);

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_validate_unique_genuine_duplicate_rejected() {
    let dir = temp_dir("catalog_uidx_genuine_dup");
    let mut engine = CatalogEngine::open(&dir).unwrap();
    let cols = vec![u64_col_def("id"), u64_col_def("val")];
    let tid = engine.create_table("public.t", &cols, &[0], false).unwrap();
    engine.create_index("public.t", "val", true).unwrap();
    let schema = engine.get_schema(tid).unwrap();

    // Committed: P1/val=5.
    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(1u128, 1); bb.put_u64(5); bb.end_row();
    engine.ingest_to_family(tid, &bb.finish()).unwrap();
    engine.flush_family(tid).unwrap();

    // P2/val=5 with no retraction freeing 5 → genuine duplicate.
    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(2u128, 1); bb.put_u64(5); bb.end_row();
    let result = engine.validate_unique_indices(tid, &bb.finish());
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("Unique index violation"));

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_validate_unique_holder_touched_value_not_retracted_rejected() {
    let dir = temp_dir("catalog_uidx_holder_touched");
    let mut engine = CatalogEngine::open(&dir).unwrap();
    let cols = vec![u64_col_def("id"), u64_col_def("val")];
    let tid = engine.create_table("public.t", &cols, &[0], false).unwrap(); // non-unique_pk
    engine.create_index("public.t", "val", true).unwrap();
    let schema = engine.get_schema(tid).unwrap();

    // Committed: P1/val=5.
    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(1u128, 1); bb.put_u64(5); bb.end_row();
    engine.ingest_to_family(tid, &bb.finish()).unwrap();
    engine.flush_family(tid).unwrap();

    // {P1/val=7, P2/val=5}: P1 is touched but val=5 is NOT retracted, and there is
    // no enforce_unique_pk on a non-unique_pk table, so P2/5 is a genuine dup.
    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(1u128, 1); bb.put_u64(7); bb.end_row();
    bb.begin_row(2u128, 1); bb.put_u64(5); bb.end_row();
    let result = engine.validate_unique_indices(tid, &bb.finish());
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("Unique index violation"));

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_validate_unique_forged_retraction_rejected() {
    let dir = temp_dir("catalog_uidx_forged_retraction");
    let mut engine = CatalogEngine::open(&dir).unwrap();
    let cols = vec![u64_col_def("id"), u64_col_def("val")];
    let tid = engine.create_table("public.t", &cols, &[0], false).unwrap();
    engine.create_index("public.t", "val", true).unwrap();
    let schema = engine.get_schema(tid).unwrap();

    // Committed: P1/val=5.
    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(1u128, 1); bb.put_u64(5); bb.end_row();
    engine.ingest_to_family(tid, &bb.finish()).unwrap();
    engine.flush_family(tid).unwrap();

    // {retract P3/5, insert P2/5}: the retraction names P3, which does NOT hold
    // val=5 (P1 does). The exemption keys on (holder PK, value), so it does not
    // fire and P2/5 is rejected.
    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(3u128, -1); bb.put_u64(5); bb.end_row();
    bb.begin_row(2u128, 1); bb.put_u64(5); bb.end_row();
    let result = engine.validate_unique_indices(tid, &bb.finish());
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("Unique index violation"));

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_validate_unique_forged_retraction_freed_by_upserted_holder_rejected() {
    let dir = temp_dir("catalog_uidx_forged_upserted");
    let mut engine = CatalogEngine::open(&dir).unwrap();
    let cols = vec![u64_col_def("id"), u64_col_def("val")];
    let tid = engine.create_table("public.t", &cols, &[0], true).unwrap(); // unique_pk
    engine.create_index("public.t", "val", true).unwrap();
    let schema = engine.get_schema(tid).unwrap();

    // Committed: P2/val=6.
    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(2u128, 1); bb.put_u64(6); bb.end_row();
    engine.ingest_to_family(tid, &bb.finish()).unwrap();
    engine.flush_family(tid).unwrap();

    // {P2/val=7, P4/val=6, retract P3/6}: P4/6 is a FRESH insert (not an upsert)
    // routed to the verify by 6 ∈ retracted_vals. Holder P2 is upserted (to 7) but
    // the is_upsert gate withholds the implicit exemption from the fresh P4, and
    // the retraction names P3 not P2, so neither exemption fires.
    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(2u128, 1); bb.put_u64(7); bb.end_row();
    bb.begin_row(4u128, 1); bb.put_u64(6); bb.end_row();
    bb.begin_row(3u128, -1); bb.put_u64(6); bb.end_row();
    let result = engine.validate_unique_indices(tid, &bb.finish());
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("Unique index violation"));

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_validate_unique_net_zero_pk_not_upsert_rejected() {
    let dir = temp_dir("catalog_uidx_net_zero");
    let mut engine = CatalogEngine::open(&dir).unwrap();
    let cols = vec![u64_col_def("id"), u64_col_def("val")];
    let tid = engine.create_table("public.t", &cols, &[0], true).unwrap(); // unique_pk
    engine.create_index("public.t", "val", true).unwrap();
    let schema = engine.get_schema(tid).unwrap();

    // Committed: (1,5),(2,6).
    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(1u128, 1); bb.put_u64(5); bb.end_row();
    bb.begin_row(2u128, 1); bb.put_u64(6); bb.end_row();
    engine.ingest_to_family(tid, &bb.finish()).unwrap();
    engine.flush_family(tid).unwrap();

    // {P1/val=6, retract P1/5, P2/val=7}: P1 carries both a +1 and a -1 (net 0),
    // so it is not net-positive and not an upsert. P1/6 then collides with P2's
    // committed val=6, freed only implicitly — rejected.
    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(1u128, 1); bb.put_u64(6); bb.end_row();
    bb.begin_row(1u128, -1); bb.put_u64(5); bb.end_row();
    bb.begin_row(2u128, 1); bb.put_u64(7); bb.end_row();
    let result = engine.validate_unique_indices(tid, &bb.finish());
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("Unique index violation"));

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_validate_unique_two_insert_same_value_rejected_despite_retraction() {
    let dir = temp_dir("catalog_uidx_two_insert_dup");
    let mut engine = CatalogEngine::open(&dir).unwrap();
    let cols = vec![u64_col_def("id"), u64_col_def("val")];
    let tid = engine.create_table("public.t", &cols, &[0], false).unwrap();
    engine.create_index("public.t", "val", true).unwrap();
    let schema = engine.get_schema(tid).unwrap();

    // {retract P9/7, insert P1/5, insert P2/5}: the two +1 rows sharing val=5 trip
    // the `seen` set before any exemption is consulted.
    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(9u128, -1); bb.put_u64(7); bb.end_row();
    bb.begin_row(1u128, 1); bb.put_u64(5); bb.end_row();
    bb.begin_row(2u128, 1); bb.put_u64(5); bb.end_row();
    let result = engine.validate_unique_indices(tid, &bb.finish());
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("duplicate in batch"));

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── seek_by_index tests ──────────────────────────────────────────

#[test]
fn test_seek_by_index_found() {
    let dir = temp_dir("catalog_seekidx_found");
    let mut engine = CatalogEngine::open(&dir).unwrap();
    let cols = vec![u64_col_def("id"), u64_col_def("val")];
    let tid = engine.create_table("public.t", &cols, &[0], false).unwrap();
    engine.create_index("public.t", "val", false).unwrap();
    let schema = engine.get_schema(tid).unwrap();

    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(10u128, 1); bb.put_u64(100); bb.end_row();
    bb.begin_row(20u128, 1); bb.put_u64(200); bb.end_row();
    bb.begin_row(30u128, 1); bb.put_u64(300); bb.end_row();
    engine.ingest_to_family(tid, &bb.finish()).unwrap();
    engine.flush_family(tid).unwrap();

    // Seek by index: val=200 → should find PK=20
    let result = engine.seek_by_index(tid, 1, &200u64.to_le_bytes()).unwrap();
    assert!(result.is_some());
    let row = result.unwrap();
    assert_eq!(row.count, 1);
    assert_eq!(row.get_pk(0), 20);

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_seek_by_index_not_found() {
    let dir = temp_dir("catalog_seekidx_miss");
    let mut engine = CatalogEngine::open(&dir).unwrap();
    let cols = vec![u64_col_def("id"), u64_col_def("val")];
    let tid = engine.create_table("public.t", &cols, &[0], false).unwrap();
    engine.create_index("public.t", "val", false).unwrap();
    let schema = engine.get_schema(tid).unwrap();

    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(1u128, 1); bb.put_u64(42); bb.end_row();
    engine.ingest_to_family(tid, &bb.finish()).unwrap();
    engine.flush_family(tid).unwrap();

    // Seek by index: val=999 → should return None
    let result = engine.seek_by_index(tid, 1, &999u64.to_le_bytes()).unwrap();
    assert!(result.is_none());

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── Regression: negative I64 index key storage and retrieval ────────

#[test]
fn test_seek_by_index_negative_i64() {
    let dir = temp_dir("catalog_seekidx_neg_i64");
    let mut engine = CatalogEngine::open(&dir).unwrap();
    let cols = vec![u64_col_def("id"), i64_col_def("score")];
    let tid = engine.create_table("public.t", &cols, &[0], false).unwrap();
    engine.create_index("public.t", "score", false).unwrap();
    let schema = engine.get_schema(tid).unwrap();

    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(1u128, 1); bb.put_u64((-5i64) as u64); bb.end_row();
    bb.begin_row(2u128, 1); bb.put_u64((-1i64) as u64); bb.end_row();
    bb.begin_row(3u128, 1); bb.put_u64(0); bb.end_row();
    bb.begin_row(4u128, 1); bb.put_u64(10); bb.end_row();
    engine.ingest_to_family(tid, &bb.finish()).unwrap();
    engine.flush_family(tid).unwrap();

    // Index stores I64 values as their raw bit pattern (2's complement).
    let result = engine.seek_by_index(tid, 1, &((-1i64) as u64).to_le_bytes()).unwrap();
    assert!(result.is_some(), "index must find row with score=-1");
    assert_eq!(result.unwrap().get_pk(0), 2);

    let result2 = engine.seek_by_index(tid, 1, &((-5i64) as u64).to_le_bytes()).unwrap();
    assert!(result2.is_some(), "index must find row with score=-5");
    assert_eq!(result2.unwrap().get_pk(0), 1);

    let result3 = engine.seek_by_index(tid, 1, &10u64.to_le_bytes()).unwrap();
    assert!(result3.is_some(), "index must still find positive values");
    assert_eq!(result3.unwrap().get_pk(0), 4);

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── Regression: negative I32 index key storage and retrieval ────────

#[test]
fn test_seek_by_index_negative_i32() {
    let dir = temp_dir("catalog_seekidx_neg_i32");
    let mut engine = CatalogEngine::open(&dir).unwrap();
    let cols = vec![u64_col_def("id"), i32_col_def("score")];
    let tid = engine.create_table("public.t", &cols, &[0], false).unwrap();
    engine.create_index("public.t", "score", false).unwrap();
    let schema = engine.get_schema(tid).unwrap();

    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(1u128, 1); bb.put_u32((-1i32) as u32); bb.end_row();
    bb.begin_row(2u128, 1); bb.put_u32((-100i32) as u32); bb.end_row();
    bb.begin_row(3u128, 1); bb.put_u32(42u32); bb.end_row();
    engine.ingest_to_family(tid, &bb.finish()).unwrap();
    engine.flush_family(tid).unwrap();

    // I32 values are zero-extended to u64 in the index (NOT sign-extended).
    let result = engine.seek_by_index(tid, 1, &((-1i32) as u32).to_le_bytes()).unwrap();
    assert!(result.is_some(), "index must find row with score=-1");
    assert_eq!(result.unwrap().get_pk(0), 1);

    let result2 = engine.seek_by_index(tid, 1, &((-100i32) as u32).to_le_bytes()).unwrap();
    assert!(result2.is_some(), "index must find row with score=-100");
    assert_eq!(result2.unwrap().get_pk(0), 2);

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── Regression: small-column index projection (bug #1) ────────────

#[test]
fn test_seek_by_index_u8_column() {
    let dir = temp_dir("catalog_seekidx_u8");
    let mut engine = CatalogEngine::open(&dir).unwrap();
    let cols = vec![u64_col_def("id"), u8_col_def("tag")];
    let tid = engine.create_table("public.t", &cols, &[0], false).unwrap();
    engine.create_index("public.t", "tag", false).unwrap();
    let schema = engine.get_schema(tid).unwrap();

    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(10u128, 1); bb.put_u8(42); bb.end_row();
    bb.begin_row(20u128, 1); bb.put_u8(99); bb.end_row();
    engine.ingest_to_family(tid, &bb.finish()).unwrap();
    engine.flush_family(tid).unwrap();

    let result = engine.seek_by_index(tid, 1, &[42u8]).unwrap();
    assert!(result.is_some(), "U8 index lookup must find the row");
    assert_eq!(result.unwrap().get_pk(0), 10);

    let result2 = engine.seek_by_index(tid, 1, &[99u8]).unwrap();
    assert!(result2.is_some());
    assert_eq!(result2.unwrap().get_pk(0), 20);

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_seek_by_index_u16_column() {
    let dir = temp_dir("catalog_seekidx_u16");
    let mut engine = CatalogEngine::open(&dir).unwrap();
    let cols = vec![u64_col_def("id"), u16_col_def("port")];
    let tid = engine.create_table("public.t", &cols, &[0], false).unwrap();
    engine.create_index("public.t", "port", false).unwrap();
    let schema = engine.get_schema(tid).unwrap();

    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(1u128, 1); bb.put_u16(8080); bb.end_row();
    bb.begin_row(2u128, 1); bb.put_u16(443); bb.end_row();
    engine.ingest_to_family(tid, &bb.finish()).unwrap();
    engine.flush_family(tid).unwrap();

    let result = engine.seek_by_index(tid, 1, &443u16.to_le_bytes()).unwrap();
    assert!(result.is_some(), "U16 index lookup must find the row");
    assert_eq!(result.unwrap().get_pk(0), 2);

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── Regression: native index key space, payload == PK (signed) ──────

#[test]
fn index_native_key_payload_matches_pk_signed() {
    // The FK/index key space is NATIVE (zero-extended bits, signedness erased
    // by the U64 promotion). A value derived from a native-LE payload column
    // must equal the value decoded from the same value stored OPK in a PK
    // column, so FK existence checks agree. has_pk/seek_by_index then re-encode
    // this native value to OPK to hit the order-preserving index.
    use crate::schema::{pk_native_key, payload_native_key};
    use gnitz_wire::encode_pk_column;

    for &(tc, sz) in &[
        (type_code::I8, 1usize), (type_code::I16, 2),
        (type_code::I32, 4), (type_code::I64, 8),
    ] {
        for v in [i64::MIN >> (64 - sz * 8), -1i64, 0, 1, i64::MAX >> (64 - sz * 8)] {
            let le = &v.to_le_bytes()[..sz];
            let from_payload = payload_native_key(le, 0, sz, tc);
            let mut opk_col = vec![0u8; sz];
            encode_pk_column(le, tc, &mut opk_col);
            let from_pk = pk_native_key(&opk_col, 0, sz, tc);
            assert_eq!(
                from_payload, from_pk,
                "tc={tc} v={v}: payload (native LE) and PK (decoded OPK) index keys must agree",
            );
        }
    }

    // I32 -1 → zero-extended native bits 0xFFFFFFFF (NOT sign-extended).
    assert_eq!(payload_native_key(&(-1i32).to_le_bytes(), 0, 4, type_code::I32), 0xFFFF_FFFFu128);
    assert_eq!(payload_native_key(&(-1i16).to_le_bytes(), 0, 2, type_code::I16), 0xFFFFu128);
    assert_eq!(payload_native_key(&[0xFFu8], 0, 1, type_code::I8), 0xFFu128);
}

#[test]
fn test_seek_by_index_i32_negative_value() {
    let dir = temp_dir("catalog_seekidx_i32_neg");
    let mut engine = CatalogEngine::open(&dir).unwrap();
    let cols = vec![u64_col_def("id"), i32_col_def("temp")];
    let tid = engine.create_table("public.t", &cols, &[0], false).unwrap();
    engine.create_index("public.t", "temp", false).unwrap();
    let schema = engine.get_schema(tid).unwrap();

    // Insert a row with a negative I32 value
    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(1u128, 1); bb.put_u32((-5i32) as u32); bb.end_row();
    bb.begin_row(2u128, 1); bb.put_u32(10u32); bb.end_row();
    engine.ingest_to_family(tid, &bb.finish()).unwrap();
    engine.flush_family(tid).unwrap();

    // Seek negative value: key = zero-extended u32 representation of -5
    let neg5_key = ((-5i32) as u32).to_le_bytes();
    let result = engine.seek_by_index(tid, 1, &neg5_key).unwrap();
    assert!(result.is_some(), "I32 negative index lookup must find the row");
    assert_eq!(result.unwrap().get_pk(0), 1);

    // Seek positive value
    let result2 = engine.seek_by_index(tid, 1, &10u32.to_le_bytes()).unwrap();
    assert!(result2.is_some());
    assert_eq!(result2.unwrap().get_pk(0), 2);

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_drop_table_cleans_up_indices() {
    let dir = temp_dir("drop_table_idx");
    let mut engine = CatalogEngine::open(&dir).unwrap();

    let cols = vec![u64_col_def("pk"), i64_col_def("val")];
    let tid = engine.create_table("public.idx_tbl", &cols, &[0], true).unwrap();
    engine.create_index("public.idx_tbl", "val", true).unwrap();

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

    let cols = vec![u64_col_def("pk"), i64_col_def("val")];
    let tid = engine.create_table("public.cascade_tbl", &cols, &[0], true).unwrap();
    engine.create_index("public.cascade_tbl", "val", false).unwrap();

    let idx_name = make_secondary_index_name("public", "cascade_tbl", "val");
    assert!(engine.caches.index_by_name.contains_key(idx_name.as_str()));
    let idx_records_before = count_records(&mut engine.sys_indices);
    assert!(idx_records_before >= 1);

    // Drop the table WITHOUT dropping the index first.
    engine.drop_table("public.cascade_tbl").unwrap();

    assert!(!engine.caches.index_by_name.contains_key(idx_name.as_str()),
            "in-memory index registry must forget the index");
    assert!(!engine.dag.tables.contains_key(&tid));
    assert_eq!(count_records(&mut engine.sys_indices), idx_records_before - 1,
               "sys_indices must retract exactly one record");

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

    let parent_cols = vec![u64_col_def("pk"), u64_col_def("name")];
    let parent_tid = engine.create_table("public.parent", &parent_cols, &[0], true).unwrap();

    let child_cols = vec![
        u64_col_def("pk"),
        ColumnDef {
            name: "parent_ref".into(),
            type_code: type_code::U64,
            is_nullable: false,
            fk_table_id: parent_tid,
            fk_col_idx: 0,
        },
    ];
    engine.create_table("public.child", &child_cols, &[0], true).unwrap();

    let fk_idx_name = make_fk_index_name("public", "child", "parent_ref");
    assert!(engine.caches.index_by_name.contains_key(fk_idx_name.as_str()),
            "FK index must be auto-created with the child table");

    // drop_index refuses to drop __fk_ indices, so drop_table is the only
    // valid path for this cleanup. Before the fix: the on_index_delta
    // hook's __fk_ guard blocked drop_table from cascading.
    engine.drop_table("public.child").unwrap();
    assert!(!engine.caches.index_by_name.contains_key(fk_idx_name.as_str()),
            "FK index must be removed by drop_table cascade");

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
    let dir = temp_dir("cascade_multi_idx");
    let mut engine = CatalogEngine::open(&dir).unwrap();

    let cols = vec![u64_col_def("id"), i64_col_def("val1"), i64_col_def("val2")];
    let tid = engine.create_table("public.t", &cols, &[0], true).unwrap();

    let idx1 = engine.create_index("public.t", "val1", false).unwrap();
    let idx2 = engine.create_index("public.t", "val2", false).unwrap();

    // Both indices tracked in cache
    assert_eq!(engine.caches.indices_by_owner.get(&tid).map(|v| v.len()), Some(2),
        "indices_by_owner must track both indices");
    assert!(engine.caches.index_by_id.contains_key(&idx1));
    assert!(engine.caches.index_by_id.contains_key(&idx2));

    let idx_count_before = count_records(&mut engine.sys_indices);
    assert!(idx_count_before >= 2);

    // Drop the table — cascade must retract both indices
    engine.drop_table("public.t").unwrap();

    assert!(!engine.dag.tables.contains_key(&tid), "table DAG entry must be gone");
    assert!(!engine.caches.index_by_id.contains_key(&idx1), "idx1 must be removed from index_by_id");
    assert!(!engine.caches.index_by_id.contains_key(&idx2), "idx2 must be removed from index_by_id");
    assert!(engine.caches.indices_by_owner.get(&tid).map(|v| v.is_empty()).unwrap_or(true),
        "indices_by_owner for dropped table must be empty");
    assert_eq!(count_records(&mut engine.sys_indices), idx_count_before - 2,
        "sys_indices must retract exactly two records");

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
        ColumnDef { name: "a".into(), type_code: type_code::U32,
                    is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
        ColumnDef { name: "b".into(), type_code: type_code::U32,
                    is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
        u64_col_def("val"),
    ];
    let tid = engine.create_table("public.cpk_t", &cols, &[0, 1], false).unwrap();
    engine.create_index("public.cpk_t", "val", false).unwrap();
    let schema = engine.get_schema(tid).unwrap();
    assert_eq!(schema.pk_stride(), 8, "compound (U32,U32) PK stride should be 8");

    let mut b = Batch::with_schema(schema, 4);
    let rows: &[(u32, u32, u64)] = &[
        (10, 1, 100),
        (20, 1, 200),
        (10, 2, 300), // same a as row 0, different b
        (40, 1, 200), // same val as row 1, different PK
    ];
    let mut pk_buf = vec![0u8; 8];
    for &(a, bcol, val) in rows {
        pk_buf[..4].copy_from_slice(&a.to_le_bytes());
        pk_buf[4..8].copy_from_slice(&bcol.to_le_bytes());
        b.extend_pk_bytes(&pk_buf);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &val.to_le_bytes());
        b.count += 1;
    }
    engine.ingest_to_family(tid, &b).unwrap();
    engine.flush_family(tid).unwrap();

    // val=100 → exactly one match
    let r = engine.seek_by_index(tid, 2, &100u64.to_le_bytes()).unwrap();
    assert!(r.is_some(), "val=100 should find a row");
    assert_eq!(r.unwrap().count, 1);

    // val=300 → one match (a=10, b=2)
    let r = engine.seek_by_index(tid, 2, &300u64.to_le_bytes()).unwrap();
    assert!(r.is_some(), "val=300 should find a row");

    // val=999 → miss
    let r = engine.seek_by_index(tid, 2, &999u64.to_le_bytes()).unwrap();
    assert!(r.is_none(), "val=999 should miss");

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_compound_pk_secondary_index_retract() {
    let dir = temp_dir("compound_pk_idx_retract");
    let mut engine = CatalogEngine::open(&dir).unwrap();

    let cols = vec![
        ColumnDef { name: "a".into(), type_code: type_code::U32,
                    is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
        ColumnDef { name: "b".into(), type_code: type_code::U32,
                    is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
        u64_col_def("val"),
    ];
    let tid = engine.create_table("public.cpk_r", &cols, &[0, 1], true).unwrap();
    engine.create_index("public.cpk_r", "val", false).unwrap();
    let schema = engine.get_schema(tid).unwrap();

    let mut b = Batch::with_schema(schema, 2);
    let mut pk_buf = vec![0u8; 8];
    pk_buf[..4].copy_from_slice(&7u32.to_le_bytes());
    pk_buf[4..8].copy_from_slice(&3u32.to_le_bytes());
    b.extend_pk_bytes(&pk_buf);
    b.extend_weight(&1i64.to_le_bytes());
    b.extend_null_bmp(&0u64.to_le_bytes());
    b.extend_col(0, &500u64.to_le_bytes());
    b.count += 1;
    engine.ingest_to_family(tid, &b).unwrap();
    engine.flush_family(tid).unwrap();

    assert!(engine.seek_by_index(tid, 2, &500u64.to_le_bytes()).unwrap().is_some());

    // Retract the same row.
    let mut r = Batch::with_schema(schema, 1);
    r.extend_pk_bytes(&pk_buf);
    r.extend_weight(&(-1i64).to_le_bytes());
    r.extend_null_bmp(&0u64.to_le_bytes());
    r.extend_col(0, &500u64.to_le_bytes());
    r.count += 1;
    engine.ingest_to_family(tid, &r).unwrap();
    engine.flush_family(tid).unwrap();

    assert!(engine.seek_by_index(tid, 2, &500u64.to_le_bytes()).unwrap().is_none(),
        "after retraction the indexed value must not resolve to a row");

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_create_unique_index_duplicate_rolls_back_cleanly() {
    let dir = temp_dir("unique_idx_rollback");
    let mut engine = CatalogEngine::open(&dir).unwrap();

    let cols = vec![u64_col_def("id"), u64_col_def("val")];
    let tid = engine.create_table("public.t", &cols, &[0], true).unwrap();
    let schema = engine.get_schema(tid).unwrap();

    // Seed duplicate values on `val` so a unique index over it cannot be built.
    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(1u128, 1); bb.put_u64(42); bb.end_row();
    bb.begin_row(2u128, 1); bb.put_u64(42); bb.end_row();
    engine.ingest_to_family(tid, &bb.finish()).unwrap();
    engine.flush_family(tid).unwrap();

    let idx_name = make_secondary_index_name("public", "t", "val");
    let idx_records_before = count_records(&mut engine.sys_indices);

    // Attempt should fail because of duplicate values.
    let result = engine.create_index("public.t", "val", true);
    assert!(result.is_err(), "unique index over duplicates must fail");

    // All catalog-visible state must have reverted: the ghost name/id
    // entries created by apply_index_by_{name,id} are the thing the
    // rollback is responsible for sweeping up.
    assert!(!engine.caches.index_by_name.contains_key(idx_name.as_str()),
            "index_by_name must not retain a ghost entry after rollback");
    assert!(!engine.dag.tables.get(&tid).unwrap()
                .index_circuits.iter().any(|ic| ic.col_idx == 1),
            "DAG must not retain a half-built index circuit");
    assert_eq!(count_records(&mut engine.sys_indices), idx_records_before,
               "sys_indices must net out to zero after rollback");

    // The failed attempt must not block a later successful, non-unique index
    // on the same column.
    engine.create_index("public.t", "val", false).unwrap();
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

    let cols = vec![u64_col_def("id"), u64_col_def("val")];
    let tid = engine.create_table("public.orphan_t", &cols, &[0], false).unwrap();
    engine.create_index("public.orphan_t", "val", false).unwrap();

    // Locate the index circuit on `val` (col_idx 1) and grab its schema.
    let n = engine.get_index_circuit_count(tid);
    let idx_pos = (0..n)
        .find(|&i| engine.get_index_circuit_info(tid, i).unwrap().0 == 1)
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

    let mut b = Batch::with_schema(idx_schema, 1);
    b.extend_pk_bytes(&pk);
    b.extend_weight(&1i64.to_le_bytes());
    b.extend_null_bmp(&0u64.to_le_bytes());
    b.count += 1;

    let handle = engine.get_index_store_handle(tid, 1) as *mut Table;
    assert!(!handle.is_null());
    unsafe {
        (*handle).ingest_owned_batch_memonly(b).unwrap();
        (*handle).flush().unwrap();
    }

    // The indexed value resolves to an orphan whose source row is missing.
    // Must return None and, crucially, must not hang.
    let r = engine.seek_by_index(tid, 1, &777u64.to_le_bytes()).unwrap();
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
    let dir = temp_dir("index_version_create_drop");
    let mut engine = CatalogEngine::open(&dir).unwrap();
    let cols = vec![u64_col_def("id"), i64_col_def("val")];
    let tid = engine.create_table("public.t", &cols, &[0], true).unwrap();

    // No index DDL yet → the base epoch (absent ⇒ 1), never 0.
    assert_eq!(engine.get_index_version(tid), 1);

    engine.create_index("public.t", "val", false).unwrap();
    let after_create = engine.get_index_version(tid);
    assert!(after_create != 1 && after_create != 0,
        "CREATE INDEX must move the epoch off the base, never to 0: got {after_create}");

    let idx_name = make_secondary_index_name("public", "t", "val");
    engine.drop_index(&idx_name).unwrap();
    let after_drop = engine.get_index_version(tid);
    assert!(after_drop != after_create && after_drop != 0,
        "DROP INDEX must move the epoch again, never to 0: got {after_drop}");

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn index_version_bumps_on_unique_index() {
    let dir = temp_dir("index_version_unique");
    let mut engine = CatalogEngine::open(&dir).unwrap();
    let cols = vec![u64_col_def("id"), u64_col_def("val")];
    let tid = engine.create_table("public.t", &cols, &[0], false).unwrap();
    assert_eq!(engine.get_index_version(tid), 1);
    engine.create_index("public.t", "val", true).unwrap();   // UNIQUE
    assert!(engine.get_index_version(tid) > 1,
        "CREATE UNIQUE INDEX must bump the epoch");
    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn index_version_bumps_on_fk_auto_index() {
    // The FK auto-index is created with the child table; its IDX_TAB +1 must
    // bump the child's epoch just like an explicit CREATE INDEX.
    let dir = temp_dir("index_version_fk_auto");
    let mut engine = CatalogEngine::open(&dir).unwrap();
    let parent_cols = vec![u64_col_def("pk"), u64_col_def("name")];
    let parent_tid = engine.create_table("public.parent", &parent_cols, &[0], true).unwrap();
    let child_cols = vec![
        u64_col_def("pk"),
        ColumnDef {
            name: "parent_ref".into(), type_code: type_code::U64,
            is_nullable: false, fk_table_id: parent_tid, fk_col_idx: 0,
        },
    ];
    let child_tid = engine.create_table("public.child", &child_cols, &[0], true).unwrap();
    assert!(engine.get_index_version(child_tid) > 1,
        "FK auto-index creation must bump the child's index epoch");
    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn drop_table_purges_both_version_counters() {
    // Regression for the post-cascade purge. Dropping a table must remove BOTH
    // its index_version and schema_version entries. The drop cascade re-ingests
    // IDX_TAB / COL_TAB retractions whose appliers `or_insert` the counters
    // straight back, so a removal in apply_entity_by_id (which fires *before*
    // the cascade) is immediately undone — leaking memory for a tid that is
    // never reused. The purge runs at the tail of the drop hook, after the
    // cascade, so both must end up absent.
    let dir = temp_dir("drop_purges_versions");
    let mut engine = CatalogEngine::open(&dir).unwrap();
    let cols = vec![u64_col_def("id"), i64_col_def("val")];
    let tid = engine.create_table("public.t", &cols, &[0], true).unwrap();
    engine.create_index("public.t", "val", false).unwrap();

    assert!(engine.caches.index_version.contains_key(&tid),
        "live indexed table must have an index_version entry");
    assert!(engine.caches.schema_version.contains_key(&tid),
        "live table must have a schema_version entry");

    engine.drop_table("public.t").unwrap();

    assert!(!engine.caches.index_version.contains_key(&tid),
        "index_version must be purged post-cascade (the index retraction re-creates it)");
    assert!(!engine.caches.schema_version.contains_key(&tid),
        "schema_version must be purged post-cascade (the column retraction re-creates it)");

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── UNIQUE index on STRING/BLOB rejected at DDL ──────────────────────────

#[test]
fn test_create_unique_index_on_string_blob_rejected() {
    let dir = temp_dir("uidx_string_reject");
    let mut engine = CatalogEngine::open(&dir).unwrap();
    let blob_col = ColumnDef {
        name: "data".into(), type_code: type_code::BLOB,
        is_nullable: false, fk_table_id: 0, fk_col_idx: 0,
    };
    let cols = vec![u64_col_def("id"), str_col_def("name"), blob_col];
    engine.create_table("public.t", &cols, &[0], false).unwrap();

    // UNIQUE on STRING/BLOB hits the dedicated unique-index guard first, which
    // has its own specific message (distinct from the non-unique gate below).
    let e_str = engine.create_index("public.t", "name", true).unwrap_err();
    assert!(e_str.contains("UNIQUE index on STRING or BLOB"), "got: {e_str}");
    let e_blob = engine.create_index("public.t", "data", true).unwrap_err();
    assert!(e_blob.contains("UNIQUE index on STRING or BLOB"), "got: {e_blob}");

    // Non-unique secondary indexes on STRING/BLOB are also rejected, by the
    // pre-existing get_index_key_type gate (these column types have no
    // collision-free u128 key representation for ordered index seeks).
    assert!(engine.create_index("public.t", "name", false).is_err());
    assert!(engine.create_index("public.t", "data", false).is_err());

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
    (0..n).filter_map(|i| engine.get_index_circuit_info(tid, i))
        .find(|(c, _, _)| *c == col)
        .map(|(_, u, _)| u)
}

/// Count `idx_*` sub-directories under a table directory.
fn count_idx_dirs(tbl_dir: &str) -> usize {
    std::fs::read_dir(tbl_dir)
        .map(|rd| rd.flatten()
            .filter(|e| e.file_name().to_string_lossy().starts_with("idx_"))
            .count())
        .unwrap_or(0)
}

fn fk_col_def(name: &str, parent_tid: i64, parent_col: u32) -> ColumnDef {
    ColumnDef {
        name: name.into(), type_code: type_code::U64, is_nullable: false,
        fk_table_id: parent_tid, fk_col_idx: parent_col,
    }
}

#[test]
fn test_promote_unique_index_over_fk_column_empty() {
    let dir = temp_dir("promote_unique_fk_empty");
    let mut engine = CatalogEngine::open(&dir).unwrap();

    let parent_tid = engine.create_table("public.parent", &[u64_col_def("id")], &[0], true).unwrap();
    let child_cols = vec![u64_col_def("cid"), fk_col_def("refc", parent_tid, 0)];
    let child_tid = engine.create_table("public.child", &child_cols, &[0], true).unwrap();

    // FK auto-index registered (non-unique) on col 1 at create time.
    assert_eq!(circuit_unique(&engine, child_tid, 1), Some(false));

    // A UNIQUE index over the same column promotes the incumbent circuit; on an
    // empty table this is a pure flag flip (no duplicate data to scan).
    engine.create_index("public.child", "refc", true).unwrap();
    assert_eq!(circuit_unique(&engine, child_tid, 1), Some(true),
        "the UNIQUE index must promote the FK circuit to unique");

    // Enforcement: a batch-internal duplicate on refc is now rejected.
    let schema = engine.get_schema(child_tid).unwrap();
    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(1u128, 1); bb.put_u64(7); bb.end_row();
    bb.begin_row(2u128, 1); bb.put_u64(7); bb.end_row();
    assert!(engine.validate_unique_indices(child_tid, &bb.finish()).is_err(),
        "promoted unique index must reject duplicate refc values");

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_unique_index_over_fk_column_distinct_data_promotes() {
    // Masking-bug regression (clean data): CREATE UNIQUE INDEX over an FK column
    // populated with distinct values must promote and enforce, not be masked.
    let dir = temp_dir("promote_unique_fk_distinct");
    let mut engine = CatalogEngine::open(&dir).unwrap();

    let parent_tid = engine.create_table("public.parent", &[u64_col_def("id")], &[0], true).unwrap();
    let child_cols = vec![u64_col_def("cid"), fk_col_def("refc", parent_tid, 0)];
    let child_tid = engine.create_table("public.child", &child_cols, &[0], true).unwrap();

    // Seed DISTINCT refc values (ingest_to_family bypasses FK validation).
    let schema = engine.get_schema(child_tid).unwrap();
    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(1u128, 1); bb.put_u64(10); bb.end_row();
    bb.begin_row(2u128, 1); bb.put_u64(20); bb.end_row();
    engine.ingest_to_family(child_tid, &bb.finish()).unwrap();
    engine.flush_family(child_tid).unwrap();

    engine.create_index("public.child", "refc", true).unwrap();
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

    let parent_tid = engine.create_table("public.parent", &[u64_col_def("id")], &[0], true).unwrap();
    let child_cols = vec![u64_col_def("cid"), fk_col_def("refc", parent_tid, 0)];
    let child_tid = engine.create_table("public.child", &child_cols, &[0], true).unwrap();

    // Seed DUPLICATE refc values.
    let schema = engine.get_schema(child_tid).unwrap();
    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(1u128, 1); bb.put_u64(42); bb.end_row();
    bb.begin_row(2u128, 1); bb.put_u64(42); bb.end_row();
    engine.ingest_to_family(child_tid, &bb.finish()).unwrap();
    engine.flush_family(child_tid).unwrap();

    let before = count_records(&mut engine.sys_indices);
    let r = engine.create_index("public.child", "refc", true);
    assert!(r.is_err(), "unique index over duplicate FK data must fail");
    assert_eq!(count_records(&mut engine.sys_indices), before,
        "the failed unique index row must net out of sys_indices");
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

    let parent_tid = engine.create_table("public.parent", &[u64_col_def("id")], &[0], true).unwrap();
    let child_cols = vec![u64_col_def("cid"), fk_col_def("refc", parent_tid, 0)];
    let child_tid = engine.create_table("public.child", &child_cols, &[0], true).unwrap();

    engine.create_index("public.child", "refc", true).unwrap();
    assert_eq!(circuit_unique(&engine, child_tid, 1), Some(true));

    let user_idx = make_secondary_index_name("public", "child", "refc");
    let fk_idx = make_fk_index_name("public", "child", "refc");
    engine.drop_index(&user_idx).unwrap();

    // Circuit remains (col 1 still indexed) but is no longer unique; the FK
    // auto-index survives so FK lookups keep working.
    assert_eq!(circuit_unique(&engine, child_tid, 1), Some(false),
        "circuit must be demoted, not destroyed");
    assert!(engine.has_index_by_name(&fk_idx),
        "the FK auto-index must survive the unique-index drop");

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

    let parent_tid = engine.create_table("public.parent", &[u64_col_def("id")], &[0], true).unwrap();
    let child_cols = vec![u64_col_def("cid"), fk_col_def("refc", parent_tid, 0)];
    let child_tid = engine.create_table("public.child", &child_cols, &[0], true).unwrap();
    engine.create_index("public.child", "refc", true).unwrap();

    let tbl_dir = format!("{}/public/child_{}", dir, child_tid);
    assert_eq!(count_idx_dirs(&tbl_dir), 1,
        "promotion must reuse the FK index directory, not build a second one");

    // Drop the user unique index: demotion keeps the FK directory in place.
    let user_idx = make_secondary_index_name("public", "child", "refc");
    engine.drop_index(&user_idx).unwrap();
    engine.drain_pending_dir_deletions();
    assert_eq!(count_idx_dirs(&tbl_dir), 1,
        "dropping the unique index must not delete the shared FK directory");

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
    let dir = temp_dir("failed_create_index_dir");
    let mut engine = CatalogEngine::open(&dir).unwrap();

    let cols = vec![u64_col_def("id"), u64_col_def("val")];
    let tid = engine.create_table("public.t", &cols, &[0], true).unwrap();
    let schema = engine.get_schema(tid).unwrap();
    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(1u128, 1); bb.put_u64(9); bb.end_row();
    bb.begin_row(2u128, 1); bb.put_u64(9); bb.end_row();
    engine.ingest_to_family(tid, &bb.finish()).unwrap();
    engine.flush_family(tid).unwrap();

    let tbl_dir = format!("{}/public/t_{}", dir, tid);
    assert!(engine.create_index("public.t", "val", true).is_err());
    assert_eq!(count_idx_dirs(&tbl_dir), 0,
        "a failed unique create_index must leave no index directory behind");

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_drop_index_permitted_on_lone_pk_target() {
    // A redundant unique index on a lone-PK column that is an FK target may be
    // dropped: the PK itself preserves uniqueness for FK child validation.
    let dir = temp_dir("drop_idx_lone_pk_fk");
    let mut engine = CatalogEngine::open(&dir).unwrap();

    let parent_tid = engine.create_table("public.parent", &[u64_col_def("id")], &[0], true).unwrap();
    let child_cols = vec![u64_col_def("cid"), fk_col_def("p", parent_tid, 0)];
    engine.create_table("public.child", &child_cols, &[0], true).unwrap();

    // Redundant unique index on the parent's lone PK column.
    engine.create_index("public.parent", "id", true).unwrap();
    let idx = make_secondary_index_name("public", "parent", "id");
    engine.drop_index(&idx).expect("drop must be permitted: lone PK preserves uniqueness");

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
    let parent_cols = vec![u64_col_def("id"), u64_col_def("email")];
    let parent_tid = engine.create_table("public.parent", &parent_cols, &[0], true).unwrap();
    engine.create_index("public.parent", "email", true).unwrap();

    // Child references parent.email (col 1), legal because email is unique.
    let child_cols = vec![u64_col_def("cid"), fk_col_def("e", parent_tid, 1)];
    engine.create_table("public.child", &child_cols, &[0], true).unwrap();

    let idx = make_secondary_index_name("public", "parent", "email");
    let r = engine.drop_index(&idx);
    assert!(r.is_err(), "dropping the sole unique index on an FK target must be blocked");
    assert!(r.unwrap_err().contains("no unique index would remain"),
        "error must explain the uniqueness requirement");

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}
