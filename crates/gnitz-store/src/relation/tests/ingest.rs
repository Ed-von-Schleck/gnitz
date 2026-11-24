use crate::relation::{ChildKind, Landing, PkRule, RelationKind};
use crate::storage::{read_at, read_intact};
use crate::test_support::{
    TID, assert_index_holds, batch_rows, id_rows, make_batch, make_schema_u64_i64, pk_u64_two_i64_schema,
    relation_fixture,
};
use gnitz_wire::{PkColList, ViewBudget};
use gnitz_zset::repr::Batch;

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
/// entry: only the live row's entry remains, at weight 1.
#[test]
fn an_upsert_moves_the_index_entry() {
    let schema = make_schema_u64_i64();
    let mut registry = relation_fixture(RelationKind::BaseTable, schema, &[&[1]], []);
    for val in [10, 10, 20] {
        let row = make_batch(&schema, &[(1, 1, val)]);
        registry.ingest(TID, row.clone()).unwrap();
        assert_index_holds(registry.relation(TID).unwrap(), &[1], &row, format_args!("after {val}"));
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

/// A view's rows land only at the cut, so pending rows are refused for one.
#[test]
fn pending_rows_are_refused_for_a_view() {
    let schema = make_schema_u64_i64();
    let mut registry = relation_fixture(RelationKind::View(ViewBudget::Plain), schema, &[], []);
    let landed = registry.land(
        TID,
        make_batch(&schema, &[(1, 1, 10)]),
        Landing::Pending,
        PkRule::Enforce,
    );
    let Err(err) = landed else {
        panic!("a view takes no pending rows")
    };
    assert!(err.contains("is a view"), "{err}");
    assert!(registry.seal(TID).unwrap().is_none());
}

/// Rows ingested above a base table's cut meet the PK rule against the stored
/// rows and the pending ones alike; a seal answers them as one consolidated
/// delta and drains them, and an ingest below the cut seals them first.
#[test]
fn a_seal_answers_the_pending_rows_as_the_pk_rule_left_them() {
    let schema = make_schema_u64_i64();
    let rows = |rows: &[(u64, i64, i64)]| make_batch(&schema, rows);
    let mut registry = relation_fixture(
        RelationKind::BaseTable,
        schema,
        &[],
        [rows(&[(1, 1, 10), (2, 1, 20), (3, 1, 30)])],
    );
    registry
        .land(TID, rows(&[(1, 1, 11)]), Landing::Pending, PkRule::Enforce)
        .unwrap();
    // The second upsert of key 1 retracts the pending row, not the stored one.
    registry
        .land(
            TID,
            rows(&[(1, 1, 12), (2, -1, 20), (4, 1, 40)]),
            Landing::Pending,
            PkRule::Enforce,
        )
        .unwrap();
    let delta = registry.seal(TID).unwrap().expect("rows sat above the cut");
    assert!(delta.is_consolidated());
    assert_eq!(batch_rows(&delta), [(1, -1, 10), (1, 1, 12), (2, -1, 20), (4, 1, 40)]);
    assert!(registry.seal(TID).unwrap().is_none(), "a seal drains the pending rows");

    registry
        .land(TID, rows(&[(3, -1, 30)]), Landing::Pending, PkRule::Enforce)
        .unwrap();
    registry.ingest(TID, rows(&[(5, 1, 50)])).unwrap();
    assert!(
        registry.seal(TID).unwrap().is_none(),
        "the ingest sealed the pending rows"
    );
    assert_eq!(
        batch_rows(&registry.relation(TID).unwrap().full_scan()),
        [(1, 1, 12), (4, 1, 40), (5, 1, 50)]
    );
}

/// A stream holds no rows: a seal answers its pending batches as pushed, a
/// key's rows and weights unfolded, and a landing at the cut writes nothing and
/// drops what was pending. Empty pushes leave nothing to seal.
#[test]
fn a_stream_seals_its_pending_pushes_as_pushed() {
    let schema = make_schema_u64_i64();
    let mut registry = relation_fixture(RelationKind::Stream, schema, &[], []);
    let mut push = |rows: &[(u64, i64, i64)], landing| {
        registry
            .land(TID, make_batch(&schema, rows), landing, PkRule::Enforce)
            .unwrap()
    };
    push(&[(1, 1, 10), (1, 1, 10)], Landing::Pending);
    push(&[(1, 2, 10)], Landing::Pending);
    push(&[], Landing::Pending);
    let delta = registry.seal(TID).unwrap().expect("pushes since the last seal");
    assert_eq!(batch_rows(&delta), [(1, 1, 10), (1, 1, 10), (1, 2, 10)]);
    assert!(registry.seal(TID).unwrap().is_none(), "a seal drains the pushes");

    let mut push = |rows: &[(u64, i64, i64)], landing| {
        registry
            .land(TID, make_batch(&schema, rows), landing, PkRule::Enforce)
            .unwrap()
    };
    push(&[(2, 1, 20)], Landing::Pending);
    let echo = Landing::Cut { round: None, echo: true };
    assert!(push(&[(3, 1, 30)], echo).is_none(), "a stream applies nothing");
    push(&[], Landing::Pending);
    assert!(registry.seal(TID).unwrap().is_none());
}

/// An echo answers the batch as applied — consolidated, after the PK rule — and
/// nothing when the rule leaves no row.
#[test]
fn an_echo_answers_the_batch_as_applied() {
    let schema = make_schema_u64_i64();
    let rows = |rows: &[(u64, i64, i64)]| make_batch(&schema, rows);
    let mut registry = relation_fixture(RelationKind::BaseTable, schema, &[], [rows(&[(1, 1, 10)])]);
    let echo = Landing::Cut { round: None, echo: true };
    let mut land = |batch| registry.land(TID, batch, echo, PkRule::Enforce).unwrap();
    let applied = land(rows(&[(2, 1, 20), (1, 1, 11), (2, 1, 20)])).expect("rows were applied");
    assert!(applied.is_consolidated());
    assert_eq!(batch_rows(&applied), [(1, -1, 10), (1, 1, 11), (2, 1, 20)]);
    assert!(
        land(rows(&[(2, 1, 20)])).is_none(),
        "an identical upsert applies nothing"
    );
}

/// Clearing a view drops its rows and its indexes' entries.
#[test]
fn clearing_a_view_empties_its_rows_and_indexes() {
    let schema = make_schema_u64_i64();
    let rows = make_batch(&schema, &[(1, 1, 10), (2, 1, 20)]);
    let mut registry = relation_fixture(RelationKind::View(ViewBudget::Plain), schema, &[&[1]], [rows]);
    registry.clear_rows(TID).unwrap();
    let view = registry.relation(TID).unwrap();
    assert!(view.full_scan().is_empty());
    assert_index_holds(view, &[1], &Batch::empty_with_schema(&schema), "a cleared index");
}
