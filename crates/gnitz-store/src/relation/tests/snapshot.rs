use super::*;
use crate::relation::StoreConfig;
use crate::test_support::{
    batch_rows, make_batch_raw, make_schema_u64_i64, relation_fixture, relation_fixture_with, rows_spec, spec,
    spilling, TID,
};
use gnitz_wire::snapshot::{PageRequest, MAX_PAGE_ROWS};
use gnitz_zset::algebra::Slot;
use gnitz_zset::repr::Batch;

fn page(layout: u64, limit: u64) -> PageRequest {
    PageRequest { layout, limit, after: Vec::new() }
}

fn round(lo: u64, hi: u64, val: i64) -> Batch {
    let rows: Vec<_> = (lo..hi).map(|k| (k, 1, val)).collect();
    make_batch_raw(&make_schema_u64_i64(), &rows)
}

#[test]
fn a_pin_answers_as_the_store_stood_whatever_it_takes_after() {
    let schema = make_schema_u64_i64();
    let rounds = (0..8).map(|i| round(i * 10, i * 10 + 10, 1));
    // Spilling, so later rounds fold the pinned runs and retire their shards.
    let mut r = relation_fixture_with(
        spilling(),
        Slot::SOLO,
        spec(TID, RelationKind::BaseTable, schema),
        schema,
        &[],
        rounds,
    );
    r.checkpoint_base().unwrap();
    let layout = schema.layout_digest();
    let before = batch_rows(&r.scan_spec(TID, rows_spec(None, None), layout, None).unwrap());
    assert_eq!(before, (0..80).map(|k| (k, 1, 1)).collect::<Vec<_>>());

    let pin = r.pin();
    // Overwrite every row, delete half, and keep spilling and flushing so the
    // pinned runs are folded away and their shard files retired.
    for i in 0..8 {
        let rows: Vec<(u64, i64, i64)> = (i * 10..i * 10 + 10).flat_map(|k| [(k, -1, 1), (k, 1, 2)]).collect();
        r.ingest(TID, make_batch_raw(&schema, &rows)).unwrap();
        r.checkpoint_base().unwrap();
    }
    let gone: Vec<_> = (0..40).map(|k| (k, -1, 2)).collect();
    r.ingest(TID, make_batch_raw(&schema, &gone)).unwrap();
    r.checkpoint_base().unwrap();

    let live = batch_rows(&r.scan_spec(TID, rows_spec(None, None), layout, None).unwrap());
    assert_eq!(live, (40..80).map(|k| (k, 1, 2)).collect::<Vec<_>>());
    assert_eq!(
        batch_rows(&pin.page(TID, &page(layout, MAX_PAGE_ROWS)).unwrap()),
        before
    );
}

#[test]
fn a_pin_holds_only_what_its_process_publishes() {
    let schema = make_schema_u64_i64();
    let tmp = tempfile::tempdir().unwrap();
    // A master publishes the system families' stores alone: a base table's is a
    // worker's.
    let mut r = RelationRegistry::master(tmp.path().to_str().unwrap(), 1, StoreConfig::default());
    r.register(spec(TID, RelationKind::BaseTable, schema), schema).unwrap();
    let pin = r.pin();
    let err = pin.page(TID, &page(schema.layout_digest(), 1)).err().unwrap();
    assert!(err.contains("holds no relation"), "{err}");
    assert!(!pin.holds(TID));

    // An origin publishes every store it registers.
    let origin = relation_fixture(RelationKind::BaseTable, schema, &[], [round(0, 3, 1)]);
    assert!(origin.pin().holds(TID));
}
