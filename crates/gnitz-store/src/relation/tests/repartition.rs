//! The shard shape a relayout writes: what its output carries, and where in the
//! level structure it registers.

use super::*;
use crate::storage::{read_intact, shard_path};
use crate::test_support::{make_batch_raw, make_schema_u64_i64, zset_of};

/// A `w{k}of{of}` child of `rel`, opened as the durable ingest path opens one.
fn open_child(rel: &str, k: u32, of: u32) -> Table {
    Table::new(
        &ChildAddr {
            kind: ChildKind::Rows,
            slot: Slot::new(k, of),
        }
        .dir(rel),
        make_schema_u64_i64(),
        RecoverySource::SalReplay,
        StoreBudgets::default(),
    )
    .unwrap()
}

/// `n` rows at weight 1, payload `-pk`, as `rel`'s complete published
/// 1-worker set; returns them.
fn seed_set(rel: &str, n: u64) -> Batch {
    let rows: Vec<(u64, i64, i64)> = (0..n).map(|pk| (pk, 1, -(pk as i64))).collect();
    let batch = make_batch_raw(&make_schema_u64_i64(), &rows);
    let mut t = open_child(rel, 0, 1);
    t.ingest_borrowed_batch(&batch).unwrap();
    t.flush().unwrap();
    batch
}

/// A relayout cuts each target into filtered terminal runs and moves every row
/// once, payload intact.
#[test]
fn a_relayout_writes_filtered_terminal_runs() {
    let dir = tempfile::tempdir().unwrap();
    let rel = dir.path().join("rel");
    let rel = rel.to_str().unwrap();
    let schema = make_schema_u64_i64();

    let seeded = seed_set(rel, 400);
    // Each target passes the 4 KiB RAM tier only after several 64-row chunks.
    repartition_relation(rel, &schema, 2, 4096, 64).unwrap();

    let mut got = std::collections::HashMap::new();
    for k in 0..2 {
        let t = open_child(rel, k, 2);
        let shards = t.all_shard_arcs();
        assert!(shards.len() > 1, "child {k} cut {} run(s)", shards.len());
        assert!(
            shards.iter().all(|s| s.has_shard_filter()),
            "child {k}: every run carries a PK filter"
        );
        assert_eq!(t.level_shape(), (0, [0, shards.len()]), "child {k}: terminal placement");
        for (row, w) in zset_of(&t.full_scan(), &schema) {
            assert!(got.insert(row, w).is_none(), "child {k}: a row lands on one child");
        }
    }
    assert_eq!(got, zset_of(&seeded, &schema));
    let source = ChildAddr { kind: ChildKind::Rows, slot: Slot::SOLO }.dir(rel);
    assert!(!std::path::Path::new(&source).exists(), "the source set is removed");
}

#[test]
fn a_corrupt_source_body_fails_the_relayout_and_keeps_the_source_set() {
    let dir = tempfile::tempdir().unwrap();
    let rel = dir.path().join("rel");
    let rel = rel.to_str().unwrap();
    seed_set(rel, 100);

    let source = ChildAddr { kind: ChildKind::Rows, slot: Slot::SOLO }.dir(rel);
    let entries = read_intact(&source).unwrap().unwrap().shards.entries;
    assert_eq!(entries.len(), 1, "the seeded set flushed one shard");
    let shard = shard_path(&source, entries[0].seq);
    crate::test_support::flip_last_byte_in_place(&shard);

    assert!(matches!(
        repartition_relation(rel, &make_schema_u64_i64(), 2, 4096, 64),
        Err(e) if e.contains("corrupt: body checksum")
    ));
    assert!(std::path::Path::new(&shard).exists(), "the source shard survives");
    assert_eq!(
        open_child(rel, 0, 1).full_scan().len(),
        100,
        "the source set still reads whole"
    );
}
