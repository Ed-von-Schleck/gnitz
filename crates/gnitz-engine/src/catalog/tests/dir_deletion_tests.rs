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
    let tid = engine.create_table("s.t", &cols, &[0]).unwrap();
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
    engine.create_table("s.t", &cols, &[0]).unwrap();
    // DROP SCHEMA cascade queues <dir>/s/t_<old> and <dir>/s.
    engine.drop_schema("s").unwrap();
    engine.defer_pending_dir_deletions(); // DROP-success path defers removal

    // Recreate before any checkpoint drains the gated queue.
    engine.create_schema("s").unwrap(); // cancels the gated <dir>/s removal
    let new_tid = engine.create_table("s.t", &cols, &[0]).unwrap();
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

    let tid = engine.create_table("public.t", &cols, &[0]).unwrap();
    let schema = engine.get_schema_desc(tid).unwrap();
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

// An orphaned view directory (`view_<name>_<vid>` shape) is reclaimed, as is the
// pre-flight compile's throwaway root left by a crash mid-compile — the reason
// `preflight_dir` puts it under a schema dir with a numeric suffix.
#[test]
fn gc_reclaims_orphan_view_dir() {
    let dir = temp_dir("gc_orphan_view");
    let mut engine = CatalogEngine::open(&dir).unwrap();
    let cols = vec![col_def("id", type_code::U64)];
    let tid = engine.create_table("public.t", &cols, &[0]).unwrap();
    let live_dir = format!("{dir}/public/t_{tid}");

    let ghost = format!("{}/public/view_ghost_{}", dir, 4242);
    std::fs::create_dir_all(&ghost).unwrap();
    let preflight = preflight_dir(&dir, "public", 4242);
    std::fs::create_dir_all(format!("{preflight}/scratch_x_w0")).unwrap();

    engine.gc_orphan_directories();

    assert!(!Path::new(&ghost).exists(), "orphan view dir must be reclaimed");
    assert!(
        !Path::new(&preflight).exists(),
        "orphaned pre-flight root must be reclaimed: {preflight}"
    );
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
    let tid = engine.create_table("public.t", &cols, &[0]).unwrap();
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

    let t1 = engine.create_table("public.flushed", &cols, &[0]).unwrap();
    let schema = engine.get_schema_desc(t1).unwrap();
    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(1u128, 1);
    bb.put_u64(7);
    bb.end_row();
    engine.ingest_to_family(t1, &bb.finish()).unwrap();
    engine.flush_family(t1).unwrap();
    let i1 = engine.create_index("public.flushed", &["val"], false).unwrap();

    let t2 = engine.create_table("public.empty", &cols, &[0]).unwrap();

    engine.create_schema("s2").unwrap();
    let t3 = engine.create_table("s2.t", &cols, &[0]).unwrap();

    // Id-only directories (§4): `t_{tid}`, regardless of the table name.
    let dirs = [
        format!("{dir}/public/t_{t1}"),
        format!("{dir}/public/t_{t1}/idx_{i1}"),
        format!("{dir}/public/t_{t2}"),
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
    engine.create_table("public.t", &cols, &[0]).unwrap();

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
    let tid = engine.create_table("public.t", &cols, &[0]).unwrap();
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
    let tid = engine.create_table("s.t", &cols, &[0]).unwrap();
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

// ---------------------------------------------------------------------------
// Child-dir reconciliation (`reconcile_child_dirs`)
//
// A single-partition (replicated) store names its one child by the rank that
// owns it (`rep_{k}`); a hashed store names its 256 children by partition index
// (`part_{p}`). The grammars are disjoint, so the boot pass can reclaim what
// this topology does not own — and seed a launched rank's missing copy from
// `rep_0` — without a directory→shape index.
// ---------------------------------------------------------------------------

/// Register a REPLICATED base table with one row flushed, and return
/// `(tid, relation_directory)`.
fn replicated_table_with_a_shard(engine: &mut CatalogEngine, dir: &str, flush: bool) -> (i64, String) {
    let cols = vec![col_def("id", type_code::U64), col_def("x", type_code::I64)];
    let rt = create_flagged_table(engine, dir, "rt", &cols, &[0], gnitz_wire::pack_table_flags(true, 0));

    let rel_dir = engine.dag.tables[&rt].directory.clone();
    let mut bb = BatchBuilder::new(engine.get_schema_desc(rt).unwrap());
    bb.begin_row(1u128, 1);
    bb.put_i64(7);
    bb.end_row();
    engine.ingest_to_family(rt, &bb.finish()).unwrap();
    if flush {
        engine.flush_family(rt).unwrap();
    }
    (rt, rel_dir)
}

/// Sorted file names directly under `path`.
fn file_names(path: &str) -> Vec<String> {
    let mut v: Vec<String> = fs::read_dir(path)
        .unwrap()
        .flatten()
        .filter(|e| e.file_type().map(|t| t.is_file()).unwrap_or(false))
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .collect();
    v.sort();
    v
}

fn fabricate_dir(path: &str, marker: &str) {
    fs::create_dir_all(path).unwrap();
    fs::write(format!("{path}/{marker}"), b"x").unwrap();
}

#[test]
fn retired_copies_are_reclaimed_and_missing_ones_seeded() {
    let dir = temp_dir("reconcile_child_dirs_core");
    let mut engine = CatalogEngine::open(&dir).unwrap();
    let (_rt, rel) = replicated_table_with_a_shard(&mut engine, &dir, true);
    assert!(Path::new(&format!("{rel}/rep_0/manifest.bin")).exists());
    let rep0_files = file_names(&format!("{rel}/rep_0"));

    // (i) A shrink from 4 workers (plus a scratch dir from a rank that no longer
    // exists). Ranks below the launched count keep their own copies untouched.
    for k in 1..4 {
        let d = format!("{rel}/rep_{k}");
        fabricate_dir(&d, "marker");
        fs::write(format!("{d}/manifest.bin"), b"stub").unwrap();
    }
    fabricate_dir(&format!("{rel}/scratch_agg_w7"), "marker");

    engine.reconcile_child_dirs(3).unwrap();

    assert!(
        Path::new(&format!("{rel}/rep_0")).exists(),
        "worker 0's copy is the source"
    );
    assert!(!Path::new(&format!("{rel}/rep_3")).exists(), "rank 3 is retired at W=3");
    assert!(!Path::new(&format!("{rel}/scratch_agg_w7")).exists());
    for k in 1..3 {
        assert!(
            Path::new(&format!("{rel}/rep_{k}/marker")).exists(),
            "rep_{k} already has a manifest, so it is current and must not be rebuilt"
        );
    }

    // (ii) A rank whose copy is gone entirely is rebuilt from rep_0.
    fs::remove_dir_all(format!("{rel}/rep_2")).unwrap();
    engine.reconcile_child_dirs(3).unwrap();
    assert_eq!(file_names(&format!("{rel}/rep_2")), rep0_files);
    assert!(!Path::new(&format!("{rel}/rep_2/marker")).exists());

    // (iii) A torn repair — shards present, no manifest — is redone, not adopted.
    fs::remove_dir_all(format!("{rel}/rep_1")).unwrap();
    fabricate_dir(&format!("{rel}/rep_1"), "0000000000000001.shard");
    engine.reconcile_child_dirs(3).unwrap();
    assert_eq!(
        file_names(&format!("{rel}/rep_1")),
        rep0_files,
        "a manifest-less copy is rebuilt, so the manifest is the completeness witness"
    );

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn reconcile_skips_a_never_flushed_table() {
    let dir = temp_dir("reconcile_child_dirs_unflushed");
    let mut engine = CatalogEngine::open(&dir).unwrap();
    let (_rt, rel) = replicated_table_with_a_shard(&mut engine, &dir, false);
    assert!(!Path::new(&format!("{rel}/rep_0/manifest.bin")).exists());

    fabricate_dir(&format!("{rel}/rep_9"), "marker");
    engine.reconcile_child_dirs(4).unwrap();

    for k in 1..4 {
        assert!(
            !Path::new(&format!("{rel}/rep_{k}")).exists(),
            "nothing to copy from an unflushed table, so rep_{k} stays absent"
        );
    }
    assert!(!Path::new(&format!("{rel}/rep_9")).exists(), "rank 9 is retired at W=4");

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn reconcile_never_touches_a_hashed_store() {
    let dir = temp_dir("reconcile_child_dirs_hashed");
    let mut engine = CatalogEngine::open(&dir).unwrap();
    let cols = vec![col_def("id", type_code::U64), col_def("x", type_code::I64)];
    let pt = engine.create_table("public.pt", &cols, &[0]).unwrap();
    let hashed = engine.dag.tables[&pt].directory.clone();
    for p in 0..NUM_PARTITIONS {
        assert!(Path::new(&format!("{hashed}/part_{p}")).exists());
    }

    // A hashed store's per-worker ranges tile 0..256 exactly at every worker
    // count, so no partition is ever stale — the sweep must keep all 256.
    for n in [1u32, 2, 4] {
        engine.reconcile_child_dirs(n).unwrap();
        for p in 0..NUM_PARTITIONS {
            assert!(
                Path::new(&format!("{hashed}/part_{p}")).exists(),
                "partition {p} of a hashed store must survive reconcile at W={n}"
            );
        }
    }

    // Cross-grammar residue, both directions: what a shape flip leaves behind.
    fabricate_dir(&format!("{hashed}/rep_1"), "marker");
    let (_rt, rel) = replicated_table_with_a_shard(&mut engine, &dir, true);
    for p in [64u32, 192] {
        fabricate_dir(&format!("{rel}/part_{p}"), "marker");
    }

    engine.reconcile_child_dirs(4).unwrap();

    assert!(
        !Path::new(&format!("{hashed}/rep_1")).exists(),
        "a rep_* child under a hashed store is residue"
    );
    for p in [64u32, 192] {
        assert!(
            !Path::new(&format!("{rel}/part_{p}")).exists(),
            "a part_* child under a single-partition store is residue"
        );
    }
    assert!(Path::new(&format!("{rel}/rep_0")).exists());

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}
