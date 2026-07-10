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
    let cols = vec![col_def("id", type_code::U64)];

    engine.create_schema("s").unwrap();
    let tid = engine.create_table("s.t", &cols, &[0], true).unwrap();
    let tbl_dir = format!("{dir}/s/t_{tid}");
    assert!(Path::new(&tbl_dir).exists());

    // DROP SCHEMA cascade queues the table dir and the schema dir.
    engine.drop_schema("s").unwrap();
    assert!(
        !engine.pending_dir_deletions.is_empty(),
        "DROP must queue dirs for removal"
    );

    // The DROP-success path defers instead of removing (workers may still be
    // applying a CREATE of the same entity over the shared on-disk tree).
    engine.defer_pending_dir_deletions();
    assert!(
        engine.pending_dir_deletions.is_empty(),
        "defer must drain the in-flight queue"
    );
    assert!(
        !engine.checkpoint_gated_deletions.is_empty(),
        "defer must populate the gated queue"
    );
    assert!(
        Path::new(&tbl_dir).exists(),
        "dir must survive until the gating checkpoint"
    );

    // The gating checkpoint fires: now safe to physically remove.
    engine.drain_checkpoint_gated_deletions();
    assert!(engine.checkpoint_gated_deletions.is_empty());
    assert!(
        !Path::new(&tbl_dir).exists(),
        "checkpoint drain must physically remove the gated dir"
    );
}

// Locks the cancellation contract: a DROP SCHEMA + CREATE SCHEMA with no
// intervening checkpoint must not let the gating drain wipe the recreated
// (name-based) schema dir and the new table beneath it.
#[test]
fn drop_then_recreate_schema_survives_gated_drain() {
    let dir = temp_dir("recreate_schema_trap");
    let mut engine = CatalogEngine::open(&dir).unwrap();
    let cols = vec![col_def("id", type_code::U64)];

    engine.create_schema("s").unwrap();
    engine.create_table("s.t", &cols, &[0], true).unwrap();
    // DROP SCHEMA cascade queues <dir>/s/t_<old> and <dir>/s.
    engine.drop_schema("s").unwrap();
    engine.defer_pending_dir_deletions(); // DROP-success path defers removal

    // Recreate before any checkpoint drains the gated queue.
    engine.create_schema("s").unwrap(); // cancels the gated <dir>/s removal
    let new_tid = engine.create_table("s.t", &cols, &[0], true).unwrap();
    let new_dir = format!("{dir}/s/t_{new_tid}");
    assert!(Path::new(&new_dir).exists());

    // The gating checkpoint fires (drained from the checkpoint post-ack path).
    engine.drain_checkpoint_gated_deletions();

    // RED without the cancel: drain removed <dir>/s recursively, wiping new_dir.
    assert!(
        Path::new(&new_dir).exists(),
        "recreated schema's new table dir must survive the gated drain"
    );
    assert!(Path::new(&format!("{dir}/s")).exists());
}

// ---------------------------------------------------------------------------
// gc_orphan_directories — boot-time orphan-directory sweep
// ---------------------------------------------------------------------------

// An orphaned table directory (the residue of a vanished drop, absent from the
// live catalog) is reclaimed; the live table dir and its flushed shards survive.
#[test]
fn gc_reclaims_orphan_table_dir() {
    let dir = temp_dir("gc_orphan_table");
    let mut engine = CatalogEngine::open(&dir).unwrap();
    let cols = vec![col_def("id", type_code::U64), col_def("val", type_code::U64)];

    let tid = engine.create_table("public.t", &cols, &[0], true).unwrap();
    let schema = engine.get_schema(tid).unwrap();
    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(1u128, 1);
    bb.put_u64(10);
    bb.end_row();
    engine.ingest_to_family(tid, &bb.finish()).unwrap();
    engine.flush_family(tid).unwrap();
    let live_dir = format!("{dir}/public/t_{tid}");
    assert!(Path::new(&live_dir).exists());

    // Fabricate a sibling orphan dir: table-shaped (`<name>_<digits>`), no live
    // entity — the residue of a DROP whose gated deletion was lost to a crash.
    let ghost = format!("{}/public/ghost_{}", dir, tid + 9999);
    std::fs::create_dir_all(&ghost).unwrap();

    engine.gc_orphan_directories();

    assert!(!Path::new(&ghost).exists(), "orphan table dir must be reclaimed");
    assert!(Path::new(&live_dir).exists(), "live table dir must survive");
    assert!(
        engine.seek_family(tid, 1u128, &[]).unwrap().0.is_some(),
        "live table must still read back after the sweep"
    );

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// An orphaned view directory (`view_<name>_<vid>` shape) is reclaimed.
#[test]
fn gc_reclaims_orphan_view_dir() {
    let dir = temp_dir("gc_orphan_view");
    let mut engine = CatalogEngine::open(&dir).unwrap();
    let cols = vec![col_def("id", type_code::U64)];
    let tid = engine.create_table("public.t", &cols, &[0], true).unwrap();
    let live_dir = format!("{dir}/public/t_{tid}");

    let ghost = format!("{}/public/view_ghost_{}", dir, 4242);
    std::fs::create_dir_all(&ghost).unwrap();

    engine.gc_orphan_directories();

    assert!(!Path::new(&ghost).exists(), "orphan view dir must be reclaimed");
    assert!(Path::new(&live_dir).exists(), "live table dir must survive");

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// An orphaned `idx_<id>` sub-dir inside a live table dir is reclaimed; the live
// index dir and a non-index sibling sub-dir survive.
#[test]
fn gc_reclaims_orphan_index_dir() {
    let dir = temp_dir("gc_orphan_index");
    let mut engine = CatalogEngine::open(&dir).unwrap();
    let cols = vec![col_def("id", type_code::U64), col_def("val", type_code::U64)];
    let tid = engine.create_table("public.t", &cols, &[0], true).unwrap();
    let idx_id = engine.create_index("public.t", &["val"], false).unwrap();

    let tbl_dir = format!("{dir}/public/t_{tid}");
    let live_idx = format!("{tbl_dir}/idx_{idx_id}");
    assert!(Path::new(&live_idx).exists(), "live index dir must exist");

    // A fabricated orphan index dir, plus a non-index sub-dir that the pattern
    // guard must leave alone.
    let ghost_idx = format!("{}/idx_{}", tbl_dir, idx_id + 9999);
    std::fs::create_dir_all(&ghost_idx).unwrap();
    let non_idx = format!("{tbl_dir}/data_keep");
    std::fs::create_dir_all(&non_idx).unwrap();

    engine.gc_orphan_directories();

    assert!(!Path::new(&ghost_idx).exists(), "orphan index dir must be reclaimed");
    assert!(Path::new(&live_idx).exists(), "live index dir must survive");
    assert!(Path::new(&non_idx).exists(), "non-index sub-dir must be untouched");

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// A mix of live tables (flushed + empty), an index, and a second schema is left
// entirely untouched by the sweep.
#[test]
fn gc_leaves_live_entities_untouched() {
    let dir = temp_dir("gc_live_untouched");
    let mut engine = CatalogEngine::open(&dir).unwrap();
    let cols = vec![col_def("id", type_code::U64), col_def("val", type_code::U64)];

    let t1 = engine.create_table("public.flushed", &cols, &[0], true).unwrap();
    let schema = engine.get_schema(t1).unwrap();
    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(1u128, 1);
    bb.put_u64(7);
    bb.end_row();
    engine.ingest_to_family(t1, &bb.finish()).unwrap();
    engine.flush_family(t1).unwrap();
    let i1 = engine.create_index("public.flushed", &["val"], false).unwrap();

    let t2 = engine.create_table("public.empty", &cols, &[0], true).unwrap();

    engine.create_schema("s2").unwrap();
    let t3 = engine.create_table("s2.t", &cols, &[0], true).unwrap();

    let dirs = [
        format!("{dir}/public/flushed_{t1}"),
        format!("{dir}/public/flushed_{t1}/idx_{i1}"),
        format!("{dir}/public/empty_{t2}"),
        format!("{dir}/s2/t_{t3}"),
    ];
    for d in &dirs {
        assert!(Path::new(d).exists(), "precondition: {d} exists");
    }

    engine.gc_orphan_directories();

    for d in &dirs {
        assert!(Path::new(d).exists(), "live dir {d} must survive");
    }
    assert!(engine.pending_dir_deletions.is_empty());
    assert_eq!(
        engine.scan_family(t1).unwrap().0.count,
        1,
        "flushed table must still read back after the sweep"
    );

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// Sub-dirs that are not table/view-shaped (`<name>_<digits>`) are never removed.
#[test]
fn gc_skips_non_table_shaped_entries() {
    let dir = temp_dir("gc_non_table_entries");
    let mut engine = CatalogEngine::open(&dir).unwrap();
    let cols = vec![col_def("id", type_code::U64)];
    engine.create_table("public.t", &cols, &[0], true).unwrap();

    let keep1 = format!("{dir}/public/notatable"); // no underscore
    let keep2 = format!("{dir}/public/foo_notanumber"); // non-numeric suffix
    std::fs::create_dir_all(&keep1).unwrap();
    std::fs::create_dir_all(&keep2).unwrap();

    engine.gc_orphan_directories();

    assert!(Path::new(&keep1).exists(), "no-underscore dir must survive");
    assert!(Path::new(&keep2).exists(), "non-numeric-suffix dir must survive");

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// Running the sweep twice removes the orphan once and is a no-op the second time.
#[test]
fn gc_is_idempotent() {
    let dir = temp_dir("gc_idempotent");
    let mut engine = CatalogEngine::open(&dir).unwrap();
    let cols = vec![col_def("id", type_code::U64)];
    let tid = engine.create_table("public.t", &cols, &[0], true).unwrap();
    let live = format!("{dir}/public/t_{tid}");

    let ghost = format!("{}/public/ghost_{}", dir, tid + 5000);
    std::fs::create_dir_all(&ghost).unwrap();

    engine.gc_orphan_directories();
    assert!(!Path::new(&ghost).exists());
    assert!(Path::new(&live).exists());

    engine.gc_orphan_directories(); // second run: no-op
    assert!(!Path::new(&ghost).exists());
    assert!(Path::new(&live).exists(), "live dir must survive a repeat sweep");

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// The drain reclaims a replayed DROP SCHEMA's subtree that the schema-scoped
// scan cannot reach (the schema is gone from schema_by_id), and empties the
// queue SAL replay re-populated.
#[test]
fn gc_drains_sal_replay_queue() {
    let dir = temp_dir("gc_drain_replay_queue");
    let mut engine = CatalogEngine::open(&dir).unwrap();

    let gone_schema = format!("{dir}/goneschema");
    let gone_table = format!("{gone_schema}/t_1");
    std::fs::create_dir_all(&gone_table).unwrap();
    // Residue a replayed DROP SCHEMA leaves on the queue.
    engine.pending_dir_deletions.push(gone_schema.clone());

    engine.gc_orphan_directories();

    assert!(
        !Path::new(&gone_schema).exists(),
        "replayed DROP SCHEMA subtree must be reclaimed by the drain"
    );
    assert!(engine.pending_dir_deletions.is_empty(), "queue must be drained");

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// A recreated same-name schema whose live path SAL replay left in the queue
// survives the drain — the cancel_gated_deletion fix removes that residue when
// the recreating CREATE re-fires hook_schema_dir. RED if cancel_gated_deletion
// clears only the gated queue.
#[test]
fn gc_recreated_schema_survives_drain() {
    let dir = temp_dir("gc_recreate_schema_drain");
    let mut engine = CatalogEngine::open(&dir).unwrap();
    let schema_path = format!("{dir}/s");

    // Reproduce the recovery residue directly: a replayed DROP s left <base>/s
    // on pending_dir_deletions.
    engine.pending_dir_deletions.push(schema_path.clone());

    // The replayed CREATE s re-fires hook_schema_dir → cancel_gated_deletion,
    // which must clear the DROP's residue for the path being recreated.
    engine.create_schema("s").unwrap();
    assert!(
        !engine.pending_dir_deletions.contains(&schema_path),
        "CREATE SCHEMA must clear the replayed DROP's residue from \
         pending_dir_deletions (cancel_gated_deletion fix)"
    );

    let cols = vec![col_def("id", type_code::U64)];
    let tid = engine.create_table("s.t", &cols, &[0], true).unwrap();
    let tbl = format!("{dir}/s/t_{tid}");
    assert!(Path::new(&tbl).exists());

    engine.gc_orphan_directories();

    assert!(
        Path::new(&schema_path).exists(),
        "recreated schema dir must survive the drain"
    );
    assert!(
        Path::new(&tbl).exists(),
        "table under the recreated schema must survive"
    );

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}
