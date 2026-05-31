use super::*;

use std::path::Path;

// Locks the checkpoint-gated directory-removal contract independent of the
// multi-process race: a durable DROP must defer physical removal (the dir
// survives until the gating checkpoint), and the checkpoint drain must then
// remove it.
#[test]
fn defer_then_drain_gated_deletions() {
    let dir = temp_dir("defer_then_drain_gated");
    let mut engine = CatalogEngine::open(&dir).unwrap();
    let cols = vec![u64_col_def("id")];

    engine.create_schema("s").unwrap();
    let tid = engine.create_table("s.t", &cols, &[0], true).unwrap();
    let tbl_dir = format!("{}/s/t_{}", dir, tid);
    assert!(Path::new(&tbl_dir).exists());

    // DROP SCHEMA cascade queues the table dir and the schema dir.
    engine.drop_schema("s").unwrap();
    assert!(!engine.pending_dir_deletions.is_empty(),
        "DROP must queue dirs for removal");

    // The DROP-success path defers instead of removing (workers may still be
    // applying a CREATE of the same entity over the shared on-disk tree).
    engine.defer_pending_dir_deletions();
    assert!(engine.pending_dir_deletions.is_empty(),
        "defer must drain the in-flight queue");
    assert!(!engine.checkpoint_gated_deletions.is_empty(),
        "defer must populate the gated queue");
    assert!(Path::new(&tbl_dir).exists(),
        "dir must survive until the gating checkpoint");

    // The gating checkpoint fires: now safe to physically remove.
    engine.drain_checkpoint_gated_deletions();
    assert!(engine.checkpoint_gated_deletions.is_empty());
    assert!(!Path::new(&tbl_dir).exists(),
        "checkpoint drain must physically remove the gated dir");
}

// Locks the cancellation contract: a DROP SCHEMA + CREATE SCHEMA with no
// intervening checkpoint must not let the gating drain wipe the recreated
// (name-based) schema dir and the new table beneath it.
#[test]
fn drop_then_recreate_schema_survives_gated_drain() {
    let dir = temp_dir("recreate_schema_trap");
    let mut engine = CatalogEngine::open(&dir).unwrap();
    let cols = vec![u64_col_def("id")];

    engine.create_schema("s").unwrap();
    engine.create_table("s.t", &cols, &[0], true).unwrap();
    // DROP SCHEMA cascade queues <dir>/s/t_<old> and <dir>/s.
    engine.drop_schema("s").unwrap();
    engine.defer_pending_dir_deletions(); // DROP-success path defers removal

    // Recreate before any checkpoint drains the gated queue.
    engine.create_schema("s").unwrap(); // cancels the gated <dir>/s removal
    let new_tid = engine.create_table("s.t", &cols, &[0], true).unwrap();
    let new_dir = format!("{}/s/t_{}", dir, new_tid);
    assert!(Path::new(&new_dir).exists());

    // The gating checkpoint fires (drained from the checkpoint post-ack path).
    engine.drain_checkpoint_gated_deletions();

    // RED without the cancel: drain removed <dir>/s recursively, wiping new_dir.
    assert!(Path::new(&new_dir).exists(),
        "recreated schema's new table dir must survive the gated drain");
    assert!(Path::new(&format!("{}/s", dir)).exists());
}
