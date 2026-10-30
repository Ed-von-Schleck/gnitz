//! Single-worker catalog checks for primary keys wider than 16 bytes, which
//! only a compound PK reaches.

use super::*;
use crate::test_support::make_batch_opk;
use gnitz_store::relation::RelationKind;
use gnitz_wire::payload_u64;

/// Register a wide-PK table owning a UNIQUE secondary index on col 3, seeded
/// with `base_rows` of `(pk, weight, val)`. Bypasses `create_table`'s stride
/// gate and the push path, so it does not exercise the enforcement path.
fn setup_wide_unique(engine: &mut CatalogEngine, tid: u64, base_rows: &[([u8; 24], i64, i64)]) {
    // `pk_stride` = 24 (wide): three U64 PK columns + one payload `val`.
    let schema = pk_payload_schema(&[TypeCode::U64; 3]);
    register_full_pk(&mut engine.registry, tid, RelationKind::BaseTable, schema);
    engine
        .registry
        .add_indexes(
            tid,
            &[(
                IndexClaim::Index { id: tid + 1, unique: true },
                gnitz_wire::PkColList::from_slice(&[3]),
            )],
        )
        .unwrap();
    engine.registry.ingest(tid, make_batch_opk(&schema, base_rows)).unwrap();
    engine.registry.checkpoint_base().unwrap();
}

// ── index_circuit_for_col existence + uniqueness lookup ────────────────

#[test]
fn index_circuit_for_col_finds_index_and_uniqueness() {
    let mut engine = fresh("index_circuit_for_col");
    let tid = engine.next_id;

    // setup_wide_unique installs a UNIQUE secondary index on source col 3.
    setup_wide_unique(&mut engine, tid, &[(pk24(1, 1, 1), 1, 42)]);

    // The indexed column resolves to its circuit, carrying the uniqueness flag.
    let ic = engine
        .registry
        .relation(tid)
        .and_then(|r| r.index_on(&[3]))
        .expect("indexed column must resolve");
    assert!(ic.is_unique(), "col 3 was created UNIQUE");
    // An unindexed column resolves to nothing …
    assert!(
        engine.registry.relation(tid).and_then(|r| r.index_on(&[0])).is_none(),
        "unindexed column has no circuit"
    );
    // … and so does an unknown table.
    assert!(
        engine
            .registry
            .relation(tid + 9999)
            .and_then(|r| r.index_on(&[3]))
            .is_none(),
        "unknown table has no circuit"
    );

    finish(engine);
}

#[test]
fn wide_pk_seek_family_resolves_non_pk_col() {
    let mut engine = fresh("wide_fk_nonpk");

    let parent_tid = engine.next_id;
    let parent_pk = pk24(100, 200, 300);
    setup_wide_unique(&mut engine, parent_tid, &[(parent_pk, 1, 555)]);

    // A point read resolves the committed parent row by full PK bytes.
    let seen = pk_group(&mut engine, parent_tid, &parent_pk);
    assert_eq!(seen.len(), 1, "the live wide-PK parent row");
    assert_eq!(payload_u64(&*seen, 0, 0), 555, "the row's payload");

    finish(engine);
}

// ── native-key and byte-key point reads agree (narrow PK) ───────

#[test]
fn native_and_byte_point_reads_agree_narrow() {
    let mut engine = fresh("seek_bytes_narrow");

    // Plain narrow U64-PK table created through the normal path.
    let cols = vec![col_def("id", TypeCode::U64), col_def("val", TypeCode::U64)];
    let tid = engine.create_table("public.t", &cols, &[0]).unwrap();

    engine
        .registry
        .ingest(tid, rows(&engine, tid, 1, 1..=3, |i| [i * 10]))
        .unwrap();
    engine.registry.checkpoint_base().unwrap();

    // Retract key 2 so it is present-but-dead.
    engine
        .registry
        .ingest(tid, rows(&engine, tid, -1, [2], |_| [20]))
        .unwrap();
    engine.registry.checkpoint_base().unwrap();

    // Present (1, 3), retracted (2), and absent (99) must agree across forms.
    for key in [1u64, 2, 3, 99] {
        // The native key is LE; the at-rest OPK of an unsigned U64 PK is big-endian.
        let via_u128 = pk_group_native(&mut engine, tid, key as u128);
        let via_bytes = pk_group(&mut engine, tid, &key.to_be_bytes());
        assert_eq!(via_u128.len(), via_bytes.len(), "presence diverged for key {key}");
        if !via_u128.is_empty() {
            assert_eq!(
                payload_u64(&*via_u128, 0, 0),
                payload_u64(&*via_bytes, 0, 0),
                "payload diverged for key {key}"
            );
        }
    }

    finish(engine);
}
