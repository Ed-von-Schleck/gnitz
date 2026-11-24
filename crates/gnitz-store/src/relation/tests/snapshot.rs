use super::*;
use crate::relation::{ChildKind, RelationSpec, StoreConfig};
use crate::storage::shard_files;
use crate::test_support::{
    TID, batch_rows, make_batch, make_schema_u64_i64, relation_fixture_with, rows_spec, spec, spilling,
};
use gnitz_wire::ViewBudget;
use gnitz_wire::snapshot::{MAX_PAGE_ROWS, PageRequest};
use gnitz_zset::algebra::Placement;
use gnitz_zset::repr::Batch;

fn round(lo: u64, hi: u64, val: i64) -> Batch {
    let rows: Vec<_> = (lo..hi).map(|k| (k, 1, val)).collect();
    make_batch(&make_schema_u64_i64(), &rows)
}

#[test]
fn a_pin_answers_as_the_store_stood_whatever_it_takes_after() {
    let schema = make_schema_u64_i64();
    let rounds = (0..8).map(|i| round(i * 10, i * 10 + 10, 1));
    // Spilling, so later rounds fold the pinned runs and retire their shards.
    let mut r = relation_fixture_with(spilling(), RelationKind::BaseTable, schema, &[], rounds);
    r.checkpoint_base().unwrap();
    let layout = schema.layout_digest();
    let before = batch_rows(&r.scan_spec(TID, rows_spec(None, None), layout, None).unwrap());
    assert_eq!(before, (0..80).map(|k| (k, 1, 1)).collect::<Vec<_>>());

    let pin = r.pin();
    let dir = r.child_dir(TID, ChildKind::Rows);
    let pinned_shards = shard_files(&dir).unwrap();
    // Overwrite every row, delete half, and keep spilling and flushing so the
    // pinned runs are folded away and their shard files retired.
    for i in 0..8 {
        let rows: Vec<(u64, i64, i64)> = (i * 10..i * 10 + 10).flat_map(|k| [(k, -1, 1), (k, 1, 2)]).collect();
        r.ingest(TID, make_batch(&schema, &rows)).unwrap();
        r.checkpoint_base().unwrap();
    }
    let gone: Vec<_> = (0..40).map(|k| (k, -1, 2)).collect();
    r.ingest(TID, make_batch(&schema, &gone)).unwrap();
    r.checkpoint_base().unwrap();
    let live_shards = shard_files(&dir).unwrap();
    assert!(
        pinned_shards
            .iter()
            .all(|pinned| live_shards.iter().all(|live| live.0 != pinned.0)),
        "every pinned shard is retired"
    );

    let live = batch_rows(&r.scan_spec(TID, rows_spec(None, None), layout, None).unwrap());
    assert_eq!(live, (40..80).map(|k| (k, 1, 2)).collect::<Vec<_>>());
    let page = PageRequest {
        layout,
        limit: MAX_PAGE_ROWS,
        after: Vec::new(),
    };
    assert_eq!(batch_rows(&pin.page(TID, &page).unwrap()), before);
}

/// A pin holds a store whose every key nets 0 or 1 and which its process
/// publishes: a master's system family, unpartitioned, and a worker's base
/// table at its own slot — a replicated one empty but at its owner — but no
/// view, and no store a master does not publish.
#[test]
fn a_pin_holds_only_what_its_process_publishes() {
    let schema = make_schema_u64_i64();
    let tmp = tempfile::tempdir().unwrap();
    let base = tmp.path().to_str().unwrap();
    let (catalog, keyed, replicated, view) = (TID - 1, TID, TID + 1, TID + 2);
    let register = |r: &mut RelationRegistry| {
        let replicated = RelationSpec {
            placement: Placement::Replicated,
            ..spec(replicated, RelationKind::BaseTable, schema)
        };
        for spec in [
            spec(catalog, RelationKind::SystemCatalog, schema),
            spec(keyed, RelationKind::BaseTable, schema),
            replicated,
            spec(view, RelationKind::View(ViewBudget::Plain), schema),
        ] {
            r.register(spec, schema).unwrap();
            if r.relation(spec.id).unwrap().table.is_some() {
                r.ingest(spec.id, round(0, 3, 1)).unwrap();
            }
        }
    };
    // The share each pinned store is split over, and whether it pinned runs.
    let held = |pin: &Snapshot, id| pin.stores.get(&id).map(|s| (s.share, !s.runs.is_empty()));

    let mut master = RelationRegistry::master(base, 2, StoreConfig::default());
    register(&mut master);
    let pin = master.pin();
    assert_eq!(held(&pin, catalog), Some((Slot::SOLO, true)));
    for id in [keyed, replicated, view] {
        assert!(!pin.holds(id), "a master publishes no user store: {id}");
    }

    let rank_1 = Slot::new(1, 2);
    let mut worker = RelationRegistry::new(&format!("{base}/rank_1"), rank_1, StoreConfig::default());
    register(&mut worker);
    let pin = worker.pin();
    assert_eq!(held(&pin, catalog), Some((Slot::SOLO, true)));
    assert_eq!(held(&pin, keyed), Some((rank_1, true)));
    assert_eq!(
        held(&pin, replicated),
        Some((rank_1, false)),
        "rank 0 owns a replicated table"
    );
    assert!(!pin.holds(view), "a view's keys may net above 1");
}
