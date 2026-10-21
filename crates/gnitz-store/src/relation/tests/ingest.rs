use crate::relation::{ChildKind, RelationKind};
use crate::storage::{read_at, read_intact};
use crate::test_support::{
    assert_index_holds, id_rows, make_batch_raw, make_schema_u64_i64, pk_u64_two_i64_schema, relation_fixture, zset_of,
    TID,
};
use gnitz_wire::PkColList;

/// The append path sizes by the store's region count, so a batch of another
/// payload arity is refused before anything is written.
#[test]
fn ingest_refuses_a_batch_of_another_arity() {
    let mut registry = relation_fixture(RelationKind::BaseTable, make_schema_u64_i64(), &[], []);
    let wide = id_rows(&pk_u64_two_i64_schema(), [(1, 1)], |bb, _| {
        bb.put_int(1);
        bb.put_int(2);
    });
    assert!(registry.ingest(TID, wide).is_err());
    assert_eq!(
        registry.relation(TID).unwrap().full_scan().len(),
        0,
        "nothing was written"
    );
}

/// An index is fed the batch as the PK rule left it, so an upsert moves the
/// entry: only the live row's entry remains, at weight 1. An upsert of the row
/// the key already holds retracts and re-inserts one entry, a pair that cancels
/// before the index store takes it.
#[test]
fn an_upsert_moves_the_index_entry() {
    let schema = make_schema_u64_i64();
    let mut registry = relation_fixture(RelationKind::BaseTable, schema, &[&[1]], []);
    for val in [10, 10, 10, 20] {
        let row = make_batch_raw(&schema, &[(1, 1, val)]);
        registry.ingest(TID, row.clone()).unwrap();
        let ix = registry.relation(TID).unwrap().index_on(&[1]).unwrap();
        assert_index_holds(ix, &row, format_args!("after {val}"));
        if val == 10 {
            assert_eq!(
                ix.store.held().estimated_rows(),
                1,
                "the identical upserts wrote nothing"
            );
        }
    }
}

/// An index store is rederived: the base round leaves it, the ephemeral round
/// publishes it at the generation it is handed.
#[test]
fn only_the_ephemeral_round_publishes_an_index() {
    let mut registry = relation_fixture(RelationKind::BaseTable, make_schema_u64_i64(), &[&[1]], []);
    let dir = registry.child_dir(TID, ChildKind::Index(PkColList::from_slice(&[1])));

    registry.checkpoint_base().unwrap();
    assert!(read_intact(&dir).unwrap().is_none(), "the base round skips it");
    registry.checkpoint_ephemeral([], 3, |_| true).unwrap();
    assert!(read_at(&dir, 3).unwrap().is_some());
}

/// Rows ingested above a base table's cut meet the PK rule against the stored
/// rows and the pending ones alike; a seal answers them as one consolidated
/// delta and drains them, and an ingest below the cut seals them first.
#[test]
fn a_seal_answers_the_pending_rows_as_the_pk_rule_left_them() {
    let schema = make_schema_u64_i64();
    let rows = |rows: &[(u64, i64, i64)]| make_batch_raw(&schema, rows);
    let mut registry = relation_fixture(
        RelationKind::BaseTable,
        schema,
        &[],
        [rows(&[(1, 1, 10), (2, 1, 20), (3, 1, 30)])],
    );
    registry.ingest_pending(TID, rows(&[(1, 1, 11)])).unwrap();
    // The second upsert of key 1 retracts the pending row, not the stored one.
    registry
        .ingest_pending(TID, rows(&[(1, 1, 12), (2, -1, 20), (4, 1, 40)]))
        .unwrap();
    let delta = registry.seal(TID).unwrap().expect("rows sat above the cut");
    assert!(delta.is_consolidated());
    assert_eq!(
        zset_of(&delta, &schema),
        zset_of(&rows(&[(1, -1, 10), (1, 1, 12), (2, -1, 20), (4, 1, 40)]), &schema)
    );
    assert!(registry.seal(TID).unwrap().is_none(), "a seal drains the pending rows");

    registry.ingest_pending(TID, rows(&[(3, -1, 30)])).unwrap();
    registry.ingest(TID, rows(&[(5, 1, 50)])).unwrap();
    assert!(
        registry.seal(TID).unwrap().is_none(),
        "the ingest sealed the pending rows"
    );
    assert_eq!(
        zset_of(&registry.relation(TID).unwrap().full_scan(), &schema),
        zset_of(&rows(&[(1, 1, 12), (4, 1, 40), (5, 1, 50)]), &schema)
    );
}
