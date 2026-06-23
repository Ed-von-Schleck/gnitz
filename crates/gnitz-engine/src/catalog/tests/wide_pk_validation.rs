//! Single-worker catalog constraint checks for primary keys wider than 16
//! bytes (`pk_stride > 16`). A wide PK is always compound (e.g. three U64
//! columns = 24 bytes); the SQL planner and `create_table` both reject it at
//! DDL time, so these tests build the DAG tables and index circuits directly,
//! bypassing the stride gate, and drive `validate_unique_indices` /
//! `seek_family_bytes` on the wide path.

use super::*;
use crate::query::{DagEngine, RelationKind, StoreHandle};
use crate::schema::{type_code, SchemaColumn, SchemaDescriptor};
use crate::storage::{Batch, Persistence, Table};

fn u64c() -> SchemaColumn {
    SchemaColumn::new(type_code::U64, 0)
}

/// `pk_stride` = 24 (wide): three U64 PK columns + one U64 payload `val`.
fn wide_unique_schema() -> SchemaDescriptor {
    SchemaDescriptor::new(&[u64c(), u64c(), u64c(), u64c()], &[0, 1, 2])
}

fn pk24(a: u64, b: u64, c: u64) -> [u8; 24] {
    // Compound PK of unsigned U64 columns: OPK == big-endian per column, which
    // is what the wide-PK write path stores and what the FK RESTRICT check
    // decodes (pk_native_key) when resolving a referenced PK column.
    let mut p = [0u8; 24];
    p[0..8].copy_from_slice(&a.to_be_bytes());
    p[8..16].copy_from_slice(&b.to_be_bytes());
    p[16..24].copy_from_slice(&c.to_be_bytes());
    p
}

/// Build a batch for `wide_unique_schema`: rows of (pk, val, weight).
fn wide_val_batch(schema: &SchemaDescriptor, rows: &[([u8; 24], u64, i64)]) -> Batch {
    let mut b = Batch::with_schema(*schema, rows.len().max(1));
    for &(pk, val, w) in rows {
        b.extend_pk_bytes(&pk);
        b.extend_weight(&w.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &val.to_le_bytes());
        b.count += 1;
    }
    b
}

/// Register a wide-PK table on `engine.dag` with a UNIQUE secondary index on
/// the `val` column (source col 3), seeded with `base_rows` in both the base
/// table and the projected index. Bypasses `create_table` (stride gate) and
/// `ingest_to_family`, seeding the base and index tables directly so it can
/// attach a unique secondary index over an explicit wide PK with byte-level
/// control; it does not exercise the enforcement path. The dag borrows the base
/// table; the caller must keep the returned Box alive for the engine's use.
fn setup_wide_unique(
    engine: &mut CatalogEngine,
    tid: i64,
    dir: &str,
    base_rows: &[([u8; 24], u64, i64)],
) -> Box<Table> {
    let schema = wide_unique_schema();
    let idx_schema = make_index_schema(&[3], &schema).unwrap();

    let mut base = Box::new(
        Table::new(
            &format!("{dir}/base"),
            "base",
            schema,
            tid as u32,
            256 * 1024,
            Persistence::Ephemeral,
        )
        .unwrap(),
    );
    let mut idx = Table::new(
        &format!("{dir}/idx"),
        "idx",
        idx_schema,
        tid as u32 + 1,
        256 * 1024,
        Persistence::Ephemeral,
    )
    .unwrap();

    let bb = wide_val_batch(&schema, base_rows);
    let idx_batch = DagEngine::batch_project_index(&bb, &[3], &schema, &idx_schema);
    base.ingest_owned_batch(bb).unwrap();
    base.flush().unwrap();
    idx.ingest_owned_batch(idx_batch).unwrap();
    idx.flush().unwrap();

    engine.dag.register_table(
        tid,
        StoreHandle::Borrowed(&mut *base as *mut Table),
        schema,
        RelationKind::BaseTable { unique_pk: true },
        0,
        dir.to_string(),
    );
    engine
        .dag
        .add_index_circuit(tid, &[3], tid + 1, Box::new(idx), idx_schema, true);
    base
}

// ── index_circuit_for_col existence + uniqueness lookup ────────────────

#[test]
fn index_circuit_for_col_finds_index_and_uniqueness() {
    let dir = temp_dir("index_circuit_for_col");
    let mut engine = CatalogEngine::open(&dir).unwrap();
    let tid = engine.next_table_id;

    // setup_wide_unique installs a UNIQUE secondary index on source col 3.
    let _base = setup_wide_unique(&mut engine, tid, &dir, &[(pk24(1, 1, 1), 42, 1)]);

    // The indexed column resolves to its circuit, carrying the uniqueness flag.
    let ic = engine
        .index_circuit_for_cols(tid, &[3])
        .expect("indexed column must resolve");
    assert!(ic.is_unique, "col 3 was created UNIQUE");
    // An unindexed column resolves to nothing …
    assert!(
        engine.index_circuit_for_cols(tid, &[0]).is_none(),
        "unindexed column has no circuit"
    );
    // … and so does an unknown table.
    assert!(
        engine.index_circuit_for_cols(tid + 9999, &[3]).is_none(),
        "unknown table has no circuit"
    );

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── UNIQUE index, wide PK ──────────────────────────────────────────────

#[test]
fn wide_unique_rejects_duplicate_value() {
    let dir = temp_dir("wide_uidx_dup");
    let mut engine = CatalogEngine::open(&dir).unwrap();
    let tid = engine.next_table_id;

    // Base row: pk=(1,1,1), val=42.
    let _base = setup_wide_unique(&mut engine, tid, &dir, &[(pk24(1, 1, 1), 42, 1)]);
    let schema = wide_unique_schema();

    // New row with a DIFFERENT wide PK but the SAME indexed value → duplicate.
    let batch = wide_val_batch(&schema, &[(pk24(2, 2, 2), 42, 1)]);
    let result = engine.validate_unique_indices(tid, &batch);
    assert!(
        result.is_err(),
        "duplicate indexed value on a distinct wide PK must be rejected"
    );
    assert!(result.unwrap_err().contains("Unique index violation"));

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn wide_unique_prefix_collision_no_false_upsert() {
    let dir = temp_dir("wide_uidx_prefix");
    let mut engine = CatalogEngine::open(&dir).unwrap();
    let tid = engine.next_table_id;

    // Two base rows whose wide PKs share their first 16 bytes (a,b equal) but
    // differ past byte 16 (c differs), carrying DIFFERENT indexed values.
    let pk_a = pk24(7, 7, 100);
    let pk_b = pk24(7, 7, 200);
    let _base = setup_wide_unique(&mut engine, tid, &dir, &[(pk_a, 10, 1), (pk_b, 42, 1)]);
    let schema = wide_unique_schema();

    // UPSERT row A (its PK already lives in the base table) with B's value 42.
    // The existing index entry for 42 belongs to B, whose PK shares A's first
    // 16 bytes. A 16-byte-truncated compare would misread B's entry as A's own
    // and silently admit the collision; the full-bytes compare rejects it.
    let batch = wide_val_batch(&schema, &[(pk_a, 42, 1)]);
    let result = engine.validate_unique_indices(tid, &batch);
    assert!(
        result.is_err(),
        "16-byte-prefix-colliding wide PKs must not be treated as the same row"
    );
    assert!(result.unwrap_err().contains("Unique index violation"));

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn wide_unique_genuine_upsert_admitted() {
    let dir = temp_dir("wide_uidx_upsert");
    let mut engine = CatalogEngine::open(&dir).unwrap();
    let tid = engine.next_table_id;

    // Base row: pk=(5,6,7), val=42.
    let pk = pk24(5, 6, 7);
    let _base = setup_wide_unique(&mut engine, tid, &dir, &[(pk, 42, 1)]);
    let schema = wide_unique_schema();

    // Re-insert the SAME wide PK with the unchanged indexed value. The existing
    // index entry is this very row (enforce_unique_pk will retract it), so the
    // full-bytes source-PK compare matches and the upsert is admitted.
    let batch = wide_val_batch(&schema, &[(pk, 42, 1)]);
    assert!(
        engine.validate_unique_indices(tid, &batch).is_ok(),
        "re-inserting an existing wide PK with an unchanged value is a legal upsert",
    );

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// (wide_fk_restrict_pk_column_target removed with the test-only
// `validate_fk_parent_restrict`; wide-PK FK RESTRICT is covered end-to-end.)

#[test]
fn wide_pk_seek_family_bytes_resolves_non_pk_col() {
    let dir = temp_dir("wide_fk_nonpk");
    let mut engine = CatalogEngine::open(&dir).unwrap();

    // Parent: wide PK (cols 0..3) + non-PK column `email` (col 3). This is the
    // only test that resolves a genuinely wide (24-byte) PK via
    // `seek_family_bytes` and reads back a committed non-PK column value.
    let parent_tid = engine.next_table_id;
    let parent_schema = wide_unique_schema(); // [u64;4], pk [0,1,2], col 3 = email
    let parent_pk = pk24(100, 200, 300);
    let pb = wide_val_batch(&parent_schema, &[(parent_pk, 555, 1)]);
    let mut pbase = Table::new(
        &format!("{dir}/p_base"),
        "pbase",
        parent_schema,
        parent_tid as u32,
        256 * 1024,
        Persistence::Ephemeral,
    )
    .unwrap();
    pbase.ingest_owned_batch(pb).unwrap();
    pbase.flush().unwrap();
    engine.dag.register_table(
        parent_tid,
        StoreHandle::Borrowed(&mut pbase as *mut Table),
        parent_schema,
        RelationKind::BaseTable { unique_pk: true },
        0,
        dir.clone(),
    );

    // seek_family_bytes must resolve the committed parent row by full PK bytes.
    let seen = engine.seek_family_bytes(parent_tid, &parent_pk).unwrap();
    assert!(
        seen.is_some(),
        "seek_family_bytes must find the live wide-PK parent row"
    );
    assert_eq!(
        read_u64_col(&seen.unwrap(), 0),
        555,
        "resolved the referenced email value"
    );

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

fn read_u64_col(batch: &Batch, payload_idx: usize) -> u64 {
    let d = batch.col_data(payload_idx);
    u64::from_le_bytes(d[0..8].try_into().unwrap())
}

// ── seek_family_bytes primitive agreement (narrow PK) ──────────────────

#[test]
fn seek_family_bytes_matches_seek_family_narrow() {
    let dir = temp_dir("seek_bytes_narrow");
    let mut engine = CatalogEngine::open(&dir).unwrap();

    // Plain narrow U64-PK table created through the normal path.
    let cols = vec![u64_col_def("id"), u64_col_def("val")];
    let tid = engine.create_table("public.t", &cols, &[0], true).unwrap();
    let schema = engine.get_schema(tid).unwrap();

    let mut bb = BatchBuilder::new(schema);
    for i in 1..=3u64 {
        bb.begin_row(i as u128, 1);
        bb.put_u64(i * 10);
        bb.end_row();
    }
    engine.dag.ingest_to_family(tid, bb.finish());
    let _ = engine.dag.flush(tid);

    // Retract key 2 so it is present-but-dead.
    let mut del = BatchBuilder::new(schema);
    del.begin_row(2u128, -1);
    del.put_u64(20);
    del.end_row();
    engine.dag.ingest_to_family(tid, del.finish());
    let _ = engine.dag.flush(tid);

    // Present (1, 3), retracted (2), and absent (99) must agree across forms.
    for key in [1u64, 2, 3, 99] {
        // seek_family_bytes takes the at-rest OPK bytes; for an unsigned U64 PK
        // that is big-endian. seek_family takes the native u128 and OPK-encodes.
        let bytes = key.to_be_bytes();
        let via_u128 = engine.seek_family(tid, key as u128, &[]).unwrap();
        let via_bytes = engine.seek_family_bytes(tid, &bytes).unwrap();
        assert_eq!(
            via_u128.is_some(),
            via_bytes.is_some(),
            "seek presence diverged for key {key}",
        );
        if let (Some(a), Some(b)) = (&via_u128, &via_bytes) {
            assert_eq!(read_u64_col(a, 0), read_u64_col(b, 0), "payload diverged for key {key}");
        }
    }

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}
