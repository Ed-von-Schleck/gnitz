//! The shard shape a relayout writes: what its output carries, and where in the
//! level structure it registers.

use super::*;
use crate::storage::BatchBuilder;
use crate::test_support::make_schema_u64_i64;

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

/// Write `rel`'s complete 1-worker set holding `rows`, published.
fn seed_set(rel: &str, rows: &[(u128, i64)]) {
    let mut t = open_child(rel, 0, 1);
    let mut bb = BatchBuilder::new(make_schema_u64_i64());
    for &(pk, x) in rows {
        bb.begin_row(pk, 1);
        bb.put_int(x as u128);
        bb.end_row();
    }
    t.ingest_owned_batch(bb.finish()).unwrap();
    t.flush().unwrap();
}

/// A relayout cuts each target into filtered terminal runs and moves every row
/// once.
#[test]
fn a_relayout_writes_filtered_terminal_runs() {
    let dir = tempfile::tempdir().unwrap();
    let rel = dir.path().join("rel");
    let rel = rel.to_str().unwrap();

    let rows: Vec<(u128, i64)> = (0..400u128).map(|id| (id, id as i64)).collect();
    seed_set(rel, &rows);
    // Each target passes the 4 KiB RAM tier only after several 64-row chunks.
    repartition_relation(rel, &make_schema_u64_i64(), 2, 4096, 64).unwrap();

    let mut keys = Vec::new();
    for k in 0..2 {
        let t = open_child(rel, k, 2);
        let (shards, filtered) = t.pk_filter_census();
        assert!(shards > 1, "child {k} cut {shards} run(s)");
        assert_eq!(filtered, shards, "child {k}: every run carries a PK filter");
        assert_eq!(t.level_shape(), (0, [0, shards]), "child {k}: terminal placement");
        let scan = t.full_scan();
        for i in 0..scan.count {
            assert_eq!(scan.get_weight(i), 1, "child {k} row {i}");
            keys.push(u64::from_be_bytes(scan.get_pk_bytes(i).try_into().unwrap()));
        }
    }
    keys.sort_unstable();
    assert_eq!(keys, (0..400u64).collect::<Vec<_>>());
    assert!(
        !std::path::Path::new(&ChildAddr { kind: ChildKind::Rows, slot: Slot::SOLO }.dir(rel)).exists(),
        "the source set is removed"
    );
}

#[test]
fn a_corrupt_source_body_fails_the_relayout_and_keeps_the_source_set() {
    let dir = tempfile::tempdir().unwrap();
    let rel = dir.path().join("rel");
    let rel = rel.to_str().unwrap();
    seed_set(rel, &(0..100u128).map(|id| (id, id as i64)).collect::<Vec<_>>());

    let source = ChildAddr { kind: ChildKind::Rows, slot: Slot::SOLO }.dir(rel);
    let shards: Vec<_> = std::fs::read_dir(&source)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.file_name().unwrap().to_str().unwrap().starts_with("shard_"))
        .collect();
    assert_eq!(shards.len(), 1, "the seeded set flushed one shard");
    crate::test_support::flip_last_byte_in_place(&shards[0]);

    assert!(matches!(
        repartition_relation(rel, &make_schema_u64_i64(), 2, 4096, 64),
        Err(e) if e.contains("corrupt: body checksum")
    ));
    assert!(shards[0].exists(), "the source shard survives");
    assert_eq!(
        open_child(rel, 0, 1).full_scan().count,
        100,
        "the source set still reads whole"
    );
}
