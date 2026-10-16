use super::*;
use crate::relation::StoreConfig;
use crate::test_support::{make_batch_raw, make_schema_u64_i64, payload0_i64, relation_fixture_with, rows_spec, TID};
use gnitz_wire::snapshot::{PageRequest, MAX_PAGE_ROWS};
use gnitz_zset::repr::Batch;

/// A store whose RAM tier spills after a few rows, so a handful of rounds puts
/// rows in shards and later rounds fold and retire them.
fn spilling() -> StoreConfig {
    StoreConfig {
        ram_tier_bytes: 256,
        ..StoreConfig::default()
    }
}

fn page(layout: u64, limit: u64) -> PageRequest {
    PageRequest { layout, limit, after: None }
}

fn rows_of(b: &Batch) -> Vec<(u64, i64, i64)> {
    (0..b.len())
        .map(|i| (b.get_pk(i) as u64, b.get_weight(i), payload0_i64(b, i)))
        .collect()
}

fn round(lo: u64, hi: u64, val: i64) -> Batch {
    let rows: Vec<_> = (lo..hi).map(|k| (k, 1, val)).collect();
    make_batch_raw(&make_schema_u64_i64(), &rows)
}

#[test]
fn a_pin_answers_as_the_store_stood_whatever_it_takes_after() {
    let schema = make_schema_u64_i64();
    let rounds = (0..8).map(|i| round(i * 10, i * 10 + 10, 1));
    let mut r = relation_fixture_with(spilling(), RelationKind::BaseTable, schema, &[], rounds);
    r.checkpoint_base().unwrap();
    let layout = schema.layout_digest();
    let mut before = rows_of(&r.scan_spec(TID, rows_spec(None, None), layout, None).unwrap());
    before.sort_unstable();
    assert_eq!(before.len(), 80);

    r.pin(7, |k| k == RelationKind::BaseTable).unwrap();
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

    let mut live = rows_of(&r.scan_spec(TID, rows_spec(None, None), layout, None).unwrap());
    live.sort_unstable();
    assert_eq!(live, (40..80).map(|k| (k, 1, 2)).collect::<Vec<_>>());
    assert_eq!(
        rows_of(&r.pinned_page(7, TID, &page(layout, MAX_PAGE_ROWS)).unwrap()),
        before
    );

    assert!(r.unpin(7));
    assert!(!r.unpin(7));
    let err = r.pinned_page(7, TID, &page(layout, 1)).err().unwrap();
    assert!(err.contains("not pinned"), "{err}");
}

#[test]
fn a_pin_holds_only_what_its_filter_keeps_and_its_id_is_its_own() {
    let schema = make_schema_u64_i64();
    let mut r = relation_fixture_with(
        StoreConfig::default(),
        RelationKind::BaseTable,
        schema,
        &[],
        [round(0, 3, 1)],
    );
    r.pin(1, |k| k == RelationKind::SystemCatalog).unwrap();
    let err = r.pinned_page(1, TID, &page(schema.layout_digest(), 1)).err().unwrap();
    assert!(err.contains("holds no relation"), "{err}");
    assert!(r.pin(1, |_| true).is_err());
    assert_eq!(r.snapshots.len(), 1);
    assert!(!r.pins(1, TID));
}
