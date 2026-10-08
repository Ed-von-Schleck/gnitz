use super::*;
use crate::relation::{relation_dir, ChildKind};
use crate::storage::flush_barrier;
use crate::test_support::{make_batch, make_schema_u64_i64, scratch_table};
use gnitz_zset::schema::Slot;

/// `(files, rows, bytes)` of relation `id`'s store `store` across its levels.
fn store(usage: &DiskUsage, id: u64, store: &str) -> (u64, u64, u64) {
    usage
        .shards
        .iter()
        .filter(|((rel, label, _), _)| *rel == id && label == store)
        .fold((0, 0, 0), |(f, r, b), (_, t)| (f + t.files, r + t.rows, b + t.bytes))
}

/// Publish `rows` as one shard of relation `id`'s `kind` store under `base`.
fn publish(base: &str, id: u64, kind: ChildKind, rows: &[(u64, i64, i64)]) -> String {
    let dir = ChildAddr { kind, slot: Slot::SOLO }.dir(&relation_dir(base, id));
    std::fs::create_dir_all(&dir).unwrap();
    let schema = make_schema_u64_i64();
    let mut table = scratch_table(&dir, schema);
    table.ingest(make_batch(&schema, rows)).unwrap();
    flush_barrier([&mut table], 0).unwrap();
    dir
}

#[test]
fn a_directory_is_summed_by_store_with_identical_shards_named() {
    let tmp = tempfile::tempdir().unwrap();
    let base = tmp.path().to_str().unwrap();
    let rows: Vec<(u64, i64, i64)> = (1..=50).map(|i| (i, 1, i as i64 * 3)).collect();
    let view_rows = publish(base, 20, ChildKind::Rows, &rows);
    publish(base, 20, ChildKind::Scratch("reduce_2"), &rows);
    publish(base, 21, ChildKind::Rows, &rows[..7]);
    std::fs::write(format!("{base}/wal.sal"), [0u8; 100]).unwrap();
    // A system family keeps its one store in its relation directory.
    let system = relation_dir(base, 3);
    std::fs::create_dir_all(&system).unwrap();
    let mut table = scratch_table(&system, make_schema_u64_i64());
    table.ingest(make_batch(&make_schema_u64_i64(), &rows[..3])).unwrap();
    flush_barrier([&mut table], 0).unwrap();
    // A shard file the manifest does not name, and a second name of one it does.
    std::fs::copy(format!("{view_rows}/shard_1.db"), format!("{view_rows}/shard_9.db")).unwrap();
    let linked = ChildAddr {
        kind: ChildKind::Rows,
        slot: Slot::new(1, 2),
    }
    .dir(&relation_dir(base, 20));
    std::fs::create_dir_all(&linked).unwrap();
    std::fs::hard_link(format!("{view_rows}/shard_1.db"), format!("{linked}/shard_1.db")).unwrap();

    let usage = disk_usage(base).unwrap();
    let one = std::fs::metadata(format!("{view_rows}/shard_1.db")).unwrap().len();
    assert_eq!(store(&usage, 20, "scratch_reduce_2"), (1, 50, one));
    assert_eq!(store(&usage, 21, "rows").1, 7);
    assert_eq!(store(&usage, 3, "rows").1, 3);
    assert_eq!(store(&usage, 20, "rows"), (1, 50, one));
    assert_eq!(usage.linked.files, 1);
    // A shard's prefix digest is seeded with its name, so the copy reads as none.
    assert_eq!((usage.unreadable.files, usage.unreadable.bytes), (1, one));
    assert_eq!(
        usage.identical.values().map(|t| t.bytes).sum::<u64>(),
        one,
        "the view's rows and its trace are one shard twice"
    );
    assert_eq!(usage.shards[&(20, "rows".to_string(), Some(0))].rows, 50);
    assert_eq!(usage.other["wal.sal"].bytes, 100);
    assert_eq!(usage.other["manifest.bin"].files, 4);
    assert!(
        usage.other.keys().all(|name| !name.starts_with("shard_")),
        "every shard is a store's"
    );

    let report = usage.to_string();
    assert!(report.contains("identical shards: 1 files"), "{report}");
    assert!(report.contains("held by 20 rows = 20 scratch_reduce_2"), "{report}");
    assert!(report.contains("hard links: 1 further names"), "{report}");
}

#[test]
fn a_directory_with_no_relations_reports_nothing() {
    let tmp = tempfile::tempdir().unwrap();
    let usage = disk_usage(tmp.path().to_str().unwrap()).unwrap();
    assert_eq!(usage.shard_bytes(), 0);
}
