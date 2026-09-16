//! Single-worker catalog checks for primary keys wider than 16 bytes
//! (`pk_stride > 16`). A wide PK is always compound (e.g. three U64 columns =
//! 24 bytes). These tests seed the DAG tables and index circuits directly so a
//! wide PK can be written with byte-level control, and drive
//! `Relation::index_on` / the byte-keyed `seek` on the wide path. Unique-index
//! enforcement over a wide PK runs distributed and is covered end-to-end.

use super::*;
use gnitz_store::relation::{OnRegister, Relation, RelationKind};
use gnitz_store::schema::SchemaDescriptor;
use gnitz_store::storage::BatchBuilder;

/// `pk_stride` = 24 (wide): three U64 PK columns + one U64 payload `val`.
fn wide_unique_schema() -> SchemaDescriptor {
    SchemaDescriptor::new(&[u64c(), u64c(), u64c(), u64c()], &[0, 1, 2])
}

/// Build a batch for `wide_unique_schema`: rows of (pk, val, weight).
fn wide_val_batch(schema: &SchemaDescriptor, rows: &[([u8; 24], u64, i64)]) -> Batch {
    let mut b = BatchBuilder::new(*schema);
    for &(pk, val, w) in rows {
        b.begin_row_bytes(&pk, w);
        b.put_int(val as u128);
        b.end_row();
    }
    b.finish()
}

/// Register a wide-PK table owning a UNIQUE secondary index on col 3, seeded
/// with `base_rows`. Bypasses `create_table`'s stride gate and
/// `ingest_to_family`, so it does not exercise the enforcement path. The base
/// store lands at `<dir>/w0of1` and the index at `<dir>/idx_<tid+1>/w0of1`.
fn setup_wide_unique(engine: &mut CatalogEngine, tid: i64, dir: &str, base_rows: &[([u8; 24], u64, i64)]) {
    let schema = wide_unique_schema();
    engine
        .registry_mut()
        .register(
            RelationSpec {
                id: tid,
                kind: RelationKind::BaseTable,
                schema,
                directory: dir.to_string(),
                props: ViewProps::default(),
            },
            OnRegister::Live,
        )
        .unwrap();
    engine.registry_mut().add_index(tid, tid + 1, &[3], true).unwrap();
    // The registry projects the index itself, from the same `key_spec` and index
    // schema a hand-written projection would use.
    engine
        .registry_mut()
        .ingest(tid, wide_val_batch(&schema, base_rows))
        .unwrap();
    engine.registry_mut().flush(tid).unwrap();
}

// ── index_circuit_for_col existence + uniqueness lookup ────────────────

#[test]
fn index_circuit_for_col_finds_index_and_uniqueness() {
    let dir = temp_dir("index_circuit_for_col");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
    let tid = engine.next_id;

    // setup_wide_unique installs a UNIQUE secondary index on source col 3.
    setup_wide_unique(&mut engine, tid, &dir, &[(pk24(1, 1, 1), 42, 1)]);

    // The indexed column resolves to its circuit, carrying the uniqueness flag.
    let ic = engine
        .registry()
        .relation(tid)
        .and_then(|r| r.index_on(&[3]))
        .expect("indexed column must resolve");
    assert!(ic.is_unique(), "col 3 was created UNIQUE");
    // An unindexed column resolves to nothing …
    assert!(
        engine.registry().relation(tid).and_then(|r| r.index_on(&[0])).is_none(),
        "unindexed column has no circuit"
    );
    // … and so does an unknown table.
    assert!(
        engine
            .registry()
            .relation(tid + 9999)
            .and_then(|r| r.index_on(&[3]))
            .is_none(),
        "unknown table has no circuit"
    );

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn wide_pk_seek_family_resolves_non_pk_col() {
    let dir = temp_dir("wide_fk_nonpk");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();

    // Parent: wide PK (cols 0..3) + non-PK column `email` (col 3). This is the
    // only test that resolves a genuinely wide (24-byte) PK via the byte-keyed
    // `seek` and reads back a committed non-PK column value.
    let parent_tid = engine.next_id;
    let parent_schema = wide_unique_schema(); // [u64;4], pk [0,1,2], col 3 = email
    let parent_pk = pk24(100, 200, 300);
    let pb = wide_val_batch(&parent_schema, &[(parent_pk, 555, 1)]);
    engine
        .registry_mut()
        .register(
            RelationSpec {
                id: parent_tid,
                kind: RelationKind::BaseTable,
                schema: parent_schema,
                directory: dir.clone(),
                props: ViewProps::default(),
            },
            OnRegister::Live,
        )
        .unwrap();
    engine.registry_mut().ingest(parent_tid, pb).unwrap();
    engine.registry_mut().flush(parent_tid).unwrap();

    // A point read resolves the committed parent row by full PK bytes.
    let seen = pk_group(&mut engine, parent_tid, &parent_pk);
    assert_eq!(seen.len(), 1, "the live wide-PK parent row");
    assert_eq!(read_u64_col(&seen, 0), 555, "resolved the referenced email value");

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

fn read_u64_col(batch: &Batch, payload_idx: usize) -> u64 {
    let d = batch.col_data(payload_idx);
    u64::from_le_bytes(d[0..8].try_into().unwrap())
}

// ── native-key and byte-key point reads agree (narrow PK) ───────

#[test]
fn native_and_byte_point_reads_agree_narrow() {
    let dir = temp_dir("seek_bytes_narrow");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();

    // Plain narrow U64-PK table created through the normal path.
    let cols = vec![col_def("id", type_code::U64), col_def("val", type_code::U64)];
    let tid = engine.create_table("public.t", &cols, &[0]).unwrap();
    let schema = engine.registry().relation(tid).map(Relation::schema).unwrap();

    let mut bb = BatchBuilder::new(schema);
    for i in 1..=3u64 {
        bb.begin_row(i as u128, 1);
        bb.put_u64(i * 10);
        bb.end_row();
    }
    engine.ingest_to_family(tid, &bb.finish()).unwrap();
    engine.registry_mut().flush(tid).unwrap();

    // Retract key 2 so it is present-but-dead.
    let mut del = BatchBuilder::new(schema);
    del.begin_row(2u128, -1);
    del.put_u64(20);
    del.end_row();
    engine.ingest_to_family(tid, &del.finish()).unwrap();
    engine.registry_mut().flush(tid).unwrap();

    // Present (1, 3), retracted (2), and absent (99) must agree across forms.
    for key in [1u64, 2, 3, 99] {
        // The native key is LE; the at-rest OPK of an unsigned U64 PK is big-endian.
        let via_u128 = pk_group_native(&mut engine, tid, key as u128);
        let via_bytes = pk_group(&mut engine, tid, &key.to_be_bytes());
        assert_eq!(via_u128.len(), via_bytes.len(), "presence diverged for key {key}");
        if !via_u128.is_empty() {
            assert_eq!(
                read_u64_col(&via_u128, 0),
                read_u64_col(&via_bytes, 0),
                "payload diverged for key {key}"
            );
        }
    }

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}
