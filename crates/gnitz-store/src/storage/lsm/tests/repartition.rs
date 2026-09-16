//! The shard shape a relayout writes: what its output carries, and where in the
//! level structure it registers.

use super::*;
use crate::schema::{type_code, SchemaColumn, SchemaDescriptor};
use crate::storage::BatchBuilder;

/// `(id U64 PK | x I64)`, keyed.
fn schema() -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    )
}

/// A `w{k}of{of}` child of `rel`, opened as the durable ingest path opens one.
fn open_child(rel: &str, k: u32, of: u32, tid: u32) -> Table {
    Table::new(
        &ChildAddr::Worker { rank: k, of }.dir(rel),
        schema(),
        tid,
        RecoverySource::SalReplay,
        StoreBudgets::default(),
    )
    .unwrap()
}

/// Write `rel`'s complete 1-worker set holding `rows`, published.
fn seed_set(rel: &str, tid: u32, rows: &[(u128, i64)]) {
    let mut t = open_child(rel, 0, 1, tid);
    let mut bb = BatchBuilder::new(schema());
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
    let tid = 9100u32;

    let rows: Vec<(u128, i64)> = (0..400u128).map(|id| (id, id as i64)).collect();
    seed_set(rel, tid, &rows);
    // Each target passes the 4 KiB RAM tier only after several 64-row chunks.
    repartition_relation(rel, &schema(), tid, 2, 4096, 64).unwrap();

    let mut keys = Vec::new();
    for k in 0..2 {
        let t = open_child(rel, k, 2, tid);
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
        !std::path::Path::new(&ChildAddr::Worker { rank: 0, of: 1 }.dir(rel)).exists(),
        "the source set is removed"
    );
}
