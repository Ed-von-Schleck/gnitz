use super::*;

use std::path::Path;

// Locks the checkpoint-gated directory-removal contract independent of the
// multi-process race: a durable DROP must defer physical removal (the dir
// survives until the gating checkpoint), and the checkpoint drain must then
// remove it.
#[test]
fn defer_then_drain_gated_deletions() {
    let dir = temp_dir("defer_then_drain_gated");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
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
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
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
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
    let cols = vec![col_def("id", type_code::U64), col_def("val", type_code::U64)];

    let tid = engine.create_table("public.t", &cols, &[0]).unwrap();
    let schema = engine.registry().get_schema_desc(tid).unwrap();
    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(1u128, 1);
    bb.put_u64(10);
    bb.end_row();
    engine.ingest_to_family(tid, &bb.finish()).unwrap();
    engine.registry_mut().flush(tid).unwrap();
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
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
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
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
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
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
    let cols = vec![col_def("id", type_code::U64), col_def("val", type_code::U64)];

    let t1 = engine.create_table("public.flushed", &cols, &[0]).unwrap();
    let schema = engine.registry().get_schema_desc(t1).unwrap();
    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(1u128, 1);
    bb.put_u64(7);
    bb.end_row();
    engine.ingest_to_family(t1, &bb.finish()).unwrap();
    engine.registry_mut().flush(t1).unwrap();
    let i1 = engine.create_index("public.flushed", &["val"], false).unwrap();

    let t2 = engine.create_table("public.empty", &cols, &[0]).unwrap();

    engine.create_schema("s2").unwrap();
    let t3 = engine.create_table("s2.t", &cols, &[0]).unwrap();

    // Id-only directories: `t_{tid}`, regardless of the table name.
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
        engine.scan_family(t1).unwrap().0.len(),
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
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
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
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
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
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();

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
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
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
// Boot relayout (`repartition_relation`) and child-dir reclamation
// (`reconcile_child_dirs`)
//
// A relation's children are named `w{k}of{n}`, so a set laid out for a different
// worker count is a disjoint set of names. That is what lets the boot rewrite
// write the new layout beside the old one and lets the sweep afterwards reclaim
// whatever the rewrite already consumed. Both run inside `CatalogEngine::open`,
// so reopening the same directory at a different count is the whole test.
// ---------------------------------------------------------------------------

/// Register a REPLICATED base table with one row, and return
/// `(tid, relation_directory)`.
fn replicated_table_with_a_shard(engine: &mut CatalogEngine, flush: bool) -> (i64, String) {
    let cols = vec![col_def("id", type_code::U64), col_def("x", type_code::I64)];
    let rt = create_flagged_table(engine, "rt", &cols, &[0], replicated_flags());

    let rel_dir = engine.registry().table_entry(rt).unwrap().directory.clone();
    let mut bb = BatchBuilder::new(engine.registry().get_schema_desc(rt).unwrap());
    bb.begin_row(1u128, 1);
    bb.put_int(7);
    bb.end_row();
    engine.ingest_to_family(rt, &bb.finish()).unwrap();
    if flush {
        engine.registry_mut().flush(rt).unwrap();
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

/// `rel`'s `w{k}of{of}` child directory, and its manifest — the grammar spelled
/// once here rather than in every assertion below.
fn child_path(rel: &str, k: u32, of: u32) -> String {
    ChildAddr::Worker { rank: k, of }.dir(rel)
}

fn child_manifest(rel: &str, k: u32, of: u32) -> String {
    ChildAddr::Worker { rank: k, of }.manifest(rel)
}

/// Open one child directly, outside any catalog. A `CatalogEngine` is one
/// process at rank 0, so a test that needs a *complete* `w{k}of{n}` set — the
/// state a real cluster's checkpoint leaves behind — has to write the other
/// ranks itself.
fn open_child(rel: &str, k: u32, of: u32, schema: SchemaDescriptor, tid: i64) -> gnitz_store::storage::Table {
    gnitz_store::storage::Table::new(&child_path(rel, k, of), schema, tid as u32, RecoverySource::SalReplay).unwrap()
}

/// The subset of `rows` that `worker_for_pk` places on worker `k` of `of` — the
/// router's own verdict, so seeding and expectation cannot disagree.
fn rows_for_worker(schema: &SchemaDescriptor, rows: &[(u128, i64)], of: u32, k: u32) -> Vec<(u128, i64)> {
    rows.iter()
        .copied()
        .filter(|&(pk, _)| {
            let opk = gnitz_store::schema::key::opk_key(schema, &pk.to_le_bytes());
            schema.worker_for_pk(opk.pk_bytes(), of as usize) == k as usize
        })
        .collect()
}

/// The sorted `(worker, pk, weight)` triples `rows` must read back as at `of`
/// workers — what `set_rows` is compared against.
fn expected_placement(schema: &SchemaDescriptor, rows: &[(u128, i64)], of: u32) -> Vec<(u32, u128, i64)> {
    let mut want: Vec<(u32, u128, i64)> = rows
        .iter()
        .map(|&(pk, _)| {
            let opk = gnitz_store::schema::key::opk_key(schema, &pk.to_le_bytes());
            (schema.worker_for_pk(opk.pk_bytes(), of as usize) as u32, pk, 1)
        })
        .collect();
    want.sort_unstable();
    want
}

/// Ingest `rows` into one already-open child and publish it.
fn fill_child(t: &mut gnitz_store::storage::Table, schema: SchemaDescriptor, rows: &[(u128, i64)]) {
    if !rows.is_empty() {
        let mut bb = BatchBuilder::new(schema);
        for &(pk, x) in rows {
            bb.begin_row(pk, 1);
            bb.put_int(x as u128);
            bb.end_row();
        }
        t.ingest_borrowed_batch(&bb.finish()).unwrap();
    }
    t.flush().unwrap();
}

/// Sorted `(worker, pk, weight)` triples across every child of the `of`-worker set.
fn set_rows(rel: &str, of: u32, schema: SchemaDescriptor, tid: i64) -> Vec<(u32, u128, i64)> {
    let mut v = Vec::new();
    for k in 0..of {
        let t = open_child(rel, k, of, schema, tid);
        let batch = t.open_cursor().materialize();
        for i in 0..batch.len() {
            v.push((k, batch.get_pk(i), batch.get_weight(i)));
        }
    }
    v.sort_unstable();
    v
}

/// Replace the `of`-worker child set of `tid` with exactly `rows`, each routed by
/// `worker_for_pk` and every child published — including the empty ones, which is
/// what a real checkpoint's unconditional base round does. Any existing set at
/// that count is discarded first, so the result is the set and nothing else.
fn seed_child_set(rel: &str, of: u32, schema: SchemaDescriptor, tid: i64, rows: &[(u128, i64)]) {
    for k in 0..of {
        gnitz_store::storage::remove_child(&child_path(rel, k, of));
        let mut t = open_child(rel, k, of, schema, tid);
        fill_child(&mut t, schema, &rows_for_worker(&schema, rows, of, k));
    }
}

#[test]
fn retired_children_are_reclaimed() {
    let dir = temp_dir("reconcile_retired");
    let mut engine = CatalogEngine::open(&dir, 3).unwrap();
    let cols = vec![col_def("id", type_code::U64), col_def("x", type_code::I64)];
    let tid = engine.create_table("public.t", &cols, &[0]).unwrap();
    let rel = engine.registry().table_entry(tid).unwrap().directory.clone();

    // A set laid out for another count, a rank above the launched count, and a
    // scratch dir from a rank that no longer exists.
    for k in 0..2 {
        fabricate_dir(&child_path(&rel, k, 2), "marker");
    }
    fabricate_dir(&format!("{rel}/w9of3"), "marker");
    fabricate_dir(&format!("{rel}/scratch_agg_w7"), "marker");
    // Not a child at all: an index dir must be left alone.
    fabricate_dir(&format!("{rel}/idx_7"), "marker");

    engine.registry().reconcile_child_dirs();

    assert!(
        Path::new(&child_path(&rel, 0, 3)).exists(),
        "this process's own child is what it owns"
    );
    for k in 0..2 {
        assert!(!Path::new(&child_path(&rel, k, 2)).exists(), "wrong layout count");
    }
    assert!(!Path::new(&format!("{rel}/w9of3")).exists(), "rank 9 is not launched");
    assert!(!Path::new(&format!("{rel}/scratch_agg_w7")).exists());
    assert!(
        Path::new(&format!("{rel}/idx_7")).exists(),
        "an index dir is not a child"
    );

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

/// A replicated table's children are copies of one another, so growing the
/// worker count hard-links rank 0's shards into every launched rank and retires
/// the old set.
#[test]
fn replicated_table_is_relinked_at_a_new_worker_count() {
    let dir = temp_dir("repartition_replicated");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
    let (rt, rel) = replicated_table_with_a_shard(&mut engine, true);
    let schema = engine.registry().get_schema_desc(rt).unwrap();
    let source_files = file_names(&child_path(&rel, 0, 1));
    assert!(source_files.contains(&"manifest.bin".to_string()));
    engine.close();

    let engine = CatalogEngine::open(&dir, 3).unwrap();
    for k in 0..3 {
        assert_eq!(
            file_names(&child_path(&rel, k, 3)),
            source_files,
            "every launched rank holds a copy of the replicated dataset"
        );
    }
    assert_eq!(
        set_rows(&rel, 3, schema, rt),
        (0..3).map(|k| (k, 1u128, 1i64)).collect::<Vec<_>>(),
        "every rank reads back the full copy"
    );
    assert!(
        !Path::new(&child_path(&rel, 0, 1)).exists(),
        "the source set is retired"
    );

    drop(engine);
    let _ = fs::remove_dir_all(&dir);
}

/// The round trip that matters: over a grid of `(W_old, W_new)`, every row must
/// come back at its exact weight and land on `worker_for_pk(pk, W_new)`. Weights,
/// not row presence — the failure mode the retry guard exists for is weight 2,
/// which every presence-only check passes.
#[test]
fn keyed_table_round_trips_across_worker_counts() {
    const N: u128 = 300;
    for (w_old, w_new) in [(1u32, 1u32), (1, 4), (4, 1), (2, 3), (3, 2), (4, 5), (5, 8), (8, 4)] {
        let dir = temp_dir(&format!("repartition_grid_{w_old}_{w_new}"));
        let cols = vec![col_def("id", type_code::U64), col_def("x", type_code::I64)];

        let mut engine = CatalogEngine::open(&dir, w_old).unwrap();
        let tid = engine.create_table("public.t", &cols, &[0]).unwrap();
        let rel = engine.registry().table_entry(tid).unwrap().directory.clone();
        let schema = engine.registry().get_schema_desc(tid).unwrap();
        engine.close();

        let rows: Vec<(u128, i64)> = (0..N).map(|id| (id, id as i64 * 10)).collect();
        seed_child_set(&rel, w_old, schema, tid, &rows);

        let engine = CatalogEngine::open(&dir, w_new).unwrap();
        let got = set_rows(&rel, w_new, schema, tid);
        let want = expected_placement(&schema, &rows, w_new);
        assert_eq!(got, want, "{w_old}->{w_new}: the Z-set must survive exactly");
        if w_old != w_new {
            assert!(
                !Path::new(&child_path(&rel, 0, w_old)).exists(),
                "{w_old}->{w_new}: the source set must be retired"
            );
        }
        drop(engine);
        let _ = fs::remove_dir_all(&dir);
    }
}

/// A `CLUSTER BY` table whose rows all share the distribution prefix puts every
/// row on one worker, so most children are empty. The unconditional base-round
/// publish is what gives those empty children a manifest — without it the set is
/// never complete and the relayout silently loses every row.
#[test]
fn repartition_handles_a_relation_with_empty_children() {
    let dir = temp_dir("repartition_empty_child");
    let cols = vec![
        col_def("a", type_code::U64),
        col_def("b", type_code::U64),
        col_def("x", type_code::I64),
    ];
    let mut engine = CatalogEngine::open(&dir, 3).unwrap();
    // CLUSTER BY (a): every row shares `a = 1`, so all of them hash alike.
    let tid = create_flagged_table(&mut engine, "cb", &cols, &[0, 1], clustered_flags(1));
    let rel = engine.registry().table_entry(tid).unwrap().directory.clone();
    let schema = engine.registry().get_schema_desc(tid).unwrap();
    engine.close();

    // `opk_key` reads the packed native value in PK-list order, so PK column 0
    // (`a`) is the LOW u128 half: `a = 1` for every row, `b` varies.
    let rows: Vec<(u128, i64)> = (0..20u128).map(|b| (1u128 | (b << 64), b as i64)).collect();
    seed_child_set(&rel, 3, schema, tid, &rows);
    let occupied = (0..3)
        .filter(|&k| {
            !open_child(&rel, k, 3, schema, tid)
                .open_cursor()
                .materialize()
                .is_empty()
        })
        .count();
    assert_eq!(occupied, 1, "a shared distribution prefix puts every row on one worker");
    for k in 0..3 {
        assert!(
            Path::new(&child_manifest(&rel, k, 3)).exists(),
            "an empty child still publishes a manifest, which is what makes the set complete"
        );
    }

    let engine = CatalogEngine::open(&dir, 2).unwrap();
    let got = set_rows(&rel, 2, schema, tid);
    assert_eq!(got.len(), 20, "every row survives a relayout off a skewed set");
    // The relayout is a second shard writer, and it writes *base* shards — the
    // one kind that is point-probed, so its output must carry a PK filter.
    let census: Vec<(usize, usize)> = (0..2)
        .map(|k| open_child(&rel, k, 2, schema, tid).pk_filter_census())
        .collect();
    assert!(census.iter().any(|&(n, _)| n > 0), "the relayout wrote shards");
    assert!(
        census.iter().all(|&(n, filtered)| filtered == n),
        "a relayout writes probed base shards",
    );
    assert!(got.iter().all(|&(_, _, w)| w == 1), "no row may double");
    let mut pks: Vec<u128> = got.iter().map(|&(_, pk, _)| pk).collect();
    pks.sort_unstable();
    assert_eq!(pks, rows.iter().map(|&(pk, _)| pk).collect::<Vec<_>>());
    drop(engine);
    let _ = fs::remove_dir_all(&dir);
}

/// Crash after the target set is written but before the source is removed: two
/// complete sets survive. The layout sequence — one above the source's — is what
/// picks the newer one; resolving by "whichever count the boot happens to launch"
/// would come up on stale rows the moment the cluster reboots at the old count.
#[test]
fn two_complete_sets_resolve_by_layout_sequence() {
    let dir = temp_dir("repartition_two_sets");
    let cols = vec![col_def("id", type_code::U64), col_def("x", type_code::I64)];
    let mut engine = CatalogEngine::open(&dir, 2).unwrap();
    let tid = engine.create_table("public.t", &cols, &[0]).unwrap();
    let rel = engine.registry().table_entry(tid).unwrap().directory.clone();
    let schema = engine.registry().get_schema_desc(tid).unwrap();
    engine.close();

    // A 2-set holding the current content, relayed to a 4-set — which the
    // relayout stamps one layout sequence above its source's.
    let new_rows: Vec<(u128, i64)> = (0..40u128).map(|id| (id, id as i64 + 1000)).collect();
    seed_child_set(&rel, 2, schema, tid, &new_rows);
    let engine = CatalogEngine::open(&dir, 4).unwrap();
    drop(engine);
    assert!(!Path::new(&child_path(&rel, 0, 2)).exists());

    // A resurrected 2-set holding the *old* content — the state a crash between
    // "target durable" and "source removed" leaves. Freshly published, so it
    // carries layout sequence 0, below the relayed 4-set's.
    let old_rows: Vec<(u128, i64)> = (0..40u128).map(|id| (id, id as i64)).collect();
    seed_child_set(&rel, 2, schema, tid, &old_rows);

    // Reboot at a third count: the newer (4-worker) set must be the source.
    let engine = CatalogEngine::open(&dir, 3).unwrap();
    let got = set_rows(&rel, 3, schema, tid);
    assert_eq!(got.len(), 40);
    for k in 0..3 {
        let t = open_child(&rel, k, 3, schema, tid);
        let batch = t.open_cursor().materialize();
        for i in 0..batch.len() {
            assert!(
                batch.read_payload_u64(i, 0) >= 1000,
                "the stale 2-set won: row {} carries the old payload",
                batch.get_pk(i)
            );
        }
    }
    drop(engine);
    let _ = fs::remove_dir_all(&dir);
}

/// A repartition that died part-way through writing its target must not have its
/// leftovers merged into the retry: the two would consolidate and every base row
/// would reach weight 2 — a positivity violation no DELETE can repair, since
/// `enforce_unique_pk` emits a single `-1` against the located row.
#[test]
fn a_partial_target_is_cleared_rather_than_merged() {
    let dir = temp_dir("repartition_partial_target");
    let cols = vec![col_def("id", type_code::U64), col_def("x", type_code::I64)];
    let mut engine = CatalogEngine::open(&dir, 2).unwrap();
    let tid = engine.create_table("public.t", &cols, &[0]).unwrap();
    let rel = engine.registry().table_entry(tid).unwrap().directory.clone();
    let schema = engine.registry().get_schema_desc(tid).unwrap();
    engine.close();

    let rows: Vec<(u128, i64)> = (0..40u128).map(|id| (id, id as i64)).collect();
    seed_child_set(&rel, 2, schema, tid, &rows);

    // Attempt 1 got as far as `w0of4`, holding rows it had already placed.
    let mut torn = open_child(&rel, 0, 4, schema, tid);
    fill_child(&mut torn, schema, &rows_for_worker(&schema, &rows, 4, 0));
    drop(torn);

    let engine = CatalogEngine::open(&dir, 4).unwrap();
    let got = set_rows(&rel, 4, schema, tid);
    let want = expected_placement(&schema, &rows, 4);
    assert_eq!(got, want, "attempt 1's rows must be cleared, not summed to weight 2");
    drop(engine);
    let _ = fs::remove_dir_all(&dir);
}

/// A boot that aborts between two workers' publishes leaves a torn set at the
/// launched count. Nothing has moved and nothing is missing — a child with no
/// manifest holds no reachable rows — so the next boot must carry on rather than
/// refuse, and every published row must still be there.
#[test]
fn a_torn_set_at_the_launched_count_is_not_a_relayout() {
    let dir = temp_dir("repartition_torn_at_launched");
    let cols = vec![col_def("id", type_code::U64), col_def("x", type_code::I64)];
    let mut engine = CatalogEngine::open(&dir, 3).unwrap();
    let tid = engine.create_table("public.t", &cols, &[0]).unwrap();
    let rel = engine.registry().table_entry(tid).unwrap().directory.clone();
    let schema = engine.registry().get_schema_desc(tid).unwrap();
    engine.close();

    let rows: Vec<(u128, i64)> = (0..60u128).map(|id| (id, id as i64)).collect();
    seed_child_set(&rel, 3, schema, tid, &rows);
    // Worker 2 never got as far as publishing.
    fs::remove_file(child_manifest(&rel, 2, 3)).unwrap();
    let published = set_rows(&rel, 3, schema, tid);

    let engine = CatalogEngine::open(&dir, 3).unwrap();
    assert_eq!(
        set_rows(&rel, 3, schema, tid),
        published,
        "a torn set at the launched count is left exactly as it was"
    );
    drop(engine);
    let _ = fs::remove_dir_all(&dir);
}

/// A child directory in no grammar this build knows may hold rows the sweep
/// would never reclaim and the store would never read. Refusing to boot is what
/// keeps that from looking like "never checkpointed".
#[test]
fn repartition_refuses_an_unreadable_child_grammar() {
    let dir = temp_dir("repartition_refuses");
    let cols = vec![col_def("id", type_code::U64), col_def("x", type_code::I64)];
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
    let tid = engine.create_table("public.t", &cols, &[0]).unwrap();
    let rel = engine.registry().table_entry(tid).unwrap().directory.clone();
    engine.close();

    fabricate_dir(&format!("{rel}/part_7"), "manifest.bin");
    let Err(err) = CatalogEngine::open(&dir, 1) else {
        panic!("an unknown child grammar must refuse to boot");
    };
    assert!(err.contains("part_7"), "{err}");
    let _ = fs::remove_dir_all(&dir);
}

/// A manifest at a worker count that has no complete set is data this pass
/// cannot place. Coming up without it would be silent loss, so the boot refuses.
#[test]
fn repartition_refuses_a_torn_set_at_a_foreign_count() {
    let dir = temp_dir("repartition_refuses_torn");
    let cols = vec![col_def("id", type_code::U64), col_def("x", type_code::I64)];
    let mut engine = CatalogEngine::open(&dir, 4).unwrap();
    let tid = engine.create_table("public.t", &cols, &[0]).unwrap();
    let rel = engine.registry().table_entry(tid).unwrap().directory.clone();
    let schema = engine.registry().get_schema_desc(tid).unwrap();
    engine.close();

    seed_child_set(
        &rel,
        4,
        schema,
        tid,
        &(0..40u128).map(|id| (id, id as i64)).collect::<Vec<_>>(),
    );
    fs::remove_file(child_manifest(&rel, 3, 4)).unwrap();

    let Err(err) = CatalogEngine::open(&dir, 2) else {
        panic!("an incomplete foreign-count set must refuse to boot");
    };
    assert!(err.contains("Refusing to boot"), "{err}");
    let _ = fs::remove_dir_all(&dir);
}

/// The `(worker, pk, weight)` triples a replicated `rows` must read back as at
/// `of` workers — every rank holds every row. Sibling of [`expected_placement`],
/// which is the key-routed rule.
fn expected_replication(rows: &[(u128, i64)], of: u32) -> Vec<(u32, u128, i64)> {
    (0..of)
        .flat_map(|k| rows.iter().map(move |&(pk, _)| (k, pk, 1i64)))
        .collect()
}

/// Fill every child of a replicated relation's `of`-worker set with all of
/// `rows` and publish it. `seed_child_set`'s hash routing is the keyed rule and
/// would leave each replica holding a slice.
fn seed_replicated_set(rel: &str, of: u32, schema: SchemaDescriptor, tid: i64, rows: &[(u128, i64)]) {
    for k in 0..of {
        gnitz_store::storage::remove_child(&child_path(rel, k, of));
        let mut t = open_child(rel, k, of, schema, tid);
        fill_child(&mut t, schema, rows);
    }
}

/// A replicated child set is a set of copies, so one survivor is a whole usable
/// copy and a rank that never published still leaves the set relayable. Any
/// survivor is safe, not only the newest: linking from a lagging one lowers the
/// target's watermark and replays more, and cannot lose a row. The same shape on
/// a keyed relation is a hash slice, not a copy, so it is still refused.
#[test]
fn a_replicated_set_relays_from_one_surviving_rank() {
    for missing in [1u32, 0] {
        let dir = temp_dir(&format!("repartition_replicated_partial_{missing}"));
        let mut engine = CatalogEngine::open(&dir, 3).unwrap();
        let cols = vec![col_def("id", type_code::U64), col_def("x", type_code::I64)];
        let rt = create_flagged_table(&mut engine, "rt", &cols, &[0], replicated_flags());
        let rel = engine.registry().table_entry(rt).unwrap().directory.clone();
        let schema = engine.registry().get_schema_desc(rt).unwrap();
        engine.close();

        let rows: Vec<(u128, i64)> = (0..30u128).map(|id| (id, id as i64)).collect();
        seed_replicated_set(&rel, 3, schema, rt, &rows);
        fs::remove_file(child_manifest(&rel, missing, 3)).unwrap();

        let engine = CatalogEngine::open(&dir, 2).unwrap();
        let got = set_rows(&rel, 2, schema, rt);
        assert_eq!(
            got,
            expected_replication(&rows, 2),
            "rank {missing} missing: every launched rank holds the copy"
        );
        assert!(
            !Path::new(&child_path(&rel, 0, 3)).exists(),
            "rank {missing} missing: the source set is retired"
        );
        drop(engine);
        let _ = fs::remove_dir_all(&dir);
    }
}

/// The relaxation reaches the relay-source decision only. A relayout that
/// crashed after its first target's publish leaves a *partial* set at the
/// launched count carrying a layout sequence one above the source's; treating
/// that as the live set would report `Current` and bring the relation up with an
/// empty store on every rank whose child was never written — silently, and
/// differently per worker.
#[test]
fn a_partial_target_at_the_launched_count_is_not_current() {
    let dir = temp_dir("repartition_partial_target_replicated");
    let mut engine = CatalogEngine::open(&dir, 2).unwrap();
    let cols = vec![col_def("id", type_code::U64), col_def("x", type_code::I64)];
    let rt = create_flagged_table(&mut engine, "rt", &cols, &[0], replicated_flags());
    let rel = engine.registry().table_entry(rt).unwrap().directory.clone();
    let schema = engine.registry().get_schema_desc(rt).unwrap();
    engine.close();

    // A 2-set relayed to 4, so the 4-set carries layout sequence 1.
    let rows: Vec<(u128, i64)> = (0..30u128).map(|id| (id, id as i64)).collect();
    seed_replicated_set(&rel, 2, schema, rt, &rows);
    let engine = CatalogEngine::open(&dir, 4).unwrap();
    drop(engine);
    for k in 0..4 {
        assert!(Path::new(&child_manifest(&rel, k, 4)).exists());
    }

    // Tear two ranks out of it and put a fresh 2-set (sequence 0) back beside
    // it: the torn 4-set now outranks the complete 2-set on sequence alone.
    for k in 2..4 {
        gnitz_store::storage::remove_child(&child_path(&rel, k, 4));
    }
    seed_replicated_set(&rel, 2, schema, rt, &rows);

    let engine = CatalogEngine::open(&dir, 4).unwrap();
    assert_eq!(
        set_rows(&rel, 4, schema, rt),
        expected_replication(&rows, 4),
        "the torn 4-set must be relaid over, not accepted as current"
    );
    drop(engine);
    let _ = fs::remove_dir_all(&dir);
}

/// A relayout's output is already what a guard-partitioned level requires —
/// globally ascending, non-overlapping, consolidated, one guard key per shard —
/// so it registers at the terminal level. At L0 it would instead sit as a run
/// nothing compacts until the next spill, whose single fold would then mint an
/// `l0_run_bytes` the size of the whole child: a running max that never decays,
/// leaving the store one guard for the rest of its life.
#[test]
fn a_relayout_registers_its_shards_at_the_terminal_level() {
    let dir = temp_dir("repartition_terminal_level");
    let cols = vec![col_def("id", type_code::U64), col_def("x", type_code::I64)];
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
    let tid = engine.create_table("public.t", &cols, &[0]).unwrap();
    let rel = engine.registry().table_entry(tid).unwrap().directory.clone();
    let schema = engine.registry().get_schema_desc(tid).unwrap();
    engine.close();

    let rows: Vec<(u128, i64)> = (0..400u128).map(|id| (id, id as i64)).collect();
    seed_child_set(&rel, 1, schema, tid, &rows);

    let engine = CatalogEngine::open(&dir, 2).unwrap();
    assert_eq!(set_rows(&rel, 2, schema, tid), expected_placement(&schema, &rows, 2));
    for k in 0..2 {
        let t = open_child(&rel, k, 2, schema, tid);
        let (shards, _) = t.pk_filter_census();
        assert!(shards > 0, "child {k} holds no shard");
        // Levels are 0-based in memory, so the relayout's output is the last:
        // nothing in L0 or L1, one terminal guard per shard.
        assert_eq!(t.level_shape(), (0, vec![0, shards]), "child {k}: terminal placement");
    }
    drop(engine);
    let _ = fs::remove_dir_all(&dir);
}

/// Rebooting at the same count moves no data: the live set is already at the
/// launched count, so no shard is rewritten.
#[test]
fn repartition_is_not_run_on_an_unchanged_restart() {
    let dir = temp_dir("repartition_unchanged");
    let cols = vec![col_def("id", type_code::U64), col_def("x", type_code::I64)];
    let mut engine = CatalogEngine::open(&dir, 2).unwrap();
    let tid = engine.create_table("public.t", &cols, &[0]).unwrap();
    let rel = engine.registry().table_entry(tid).unwrap().directory.clone();
    let schema = engine.registry().get_schema_desc(tid).unwrap();
    engine.close();

    seed_child_set(
        &rel,
        2,
        schema,
        tid,
        &(0..50u128).map(|id| (id, id as i64)).collect::<Vec<_>>(),
    );
    let before: Vec<(String, std::time::SystemTime)> = (0..2)
        .flat_map(|k| {
            let child = child_path(&rel, k, 2);
            file_names(&child)
                .into_iter()
                .map(move |f| {
                    let full = format!("{child}/{f}");
                    let m = fs::metadata(&full).unwrap().modified().unwrap();
                    (full, m)
                })
                .collect::<Vec<_>>()
        })
        .collect();
    assert!(!before.is_empty());

    let engine = CatalogEngine::open(&dir, 2).unwrap();
    for (full, mtime) in &before {
        assert_eq!(
            fs::metadata(full).unwrap().modified().unwrap(),
            *mtime,
            "{full} was rewritten on an unchanged restart"
        );
    }
    drop(engine);
    let _ = fs::remove_dir_all(&dir);
}

/// A child with no manifest holds no reachable rows, so a relation that was
/// never checkpointed has nothing to relay — the launched set is simply created
/// empty rather than refused.
#[test]
fn repartition_skips_a_never_flushed_table() {
    let dir = temp_dir("repartition_unflushed");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
    let (_rt, rel) = replicated_table_with_a_shard(&mut engine, false);
    // Make the catalog durable WITHOUT flushing the user store, so the relation
    // is registered at the next open while its child carries no manifest.
    engine.flush_all_system_tables().unwrap();
    assert!(!Path::new(&child_manifest(&rel, 0, 1)).exists());
    drop(engine);

    let engine = CatalogEngine::open(&dir, 4).unwrap();
    assert!(Path::new(&child_path(&rel, 0, 4)).exists());
    assert!(!Path::new(&child_manifest(&rel, 0, 4)).exists());
    drop(engine);
    let _ = fs::remove_dir_all(&dir);
}
