//! Reopen-rebuild idempotency for ephemeral relations.
//!
//! Secondary indices are repopulated from their sources exactly once when the
//! engine reopens: their storage is erased at open (`RecoverySource::Rederive`)
//! so the backfill is the *sole* population — if that storage were durable, the
//! loaded shards plus the backfill recompute would sum and every weight would
//! double. These tests are the regression guard for that invariant, which is
//! structural since `RelationKind` derives durability and the rebuild decision
//! from one value.
//!
//! Views are *not* rebuilt at catalog open. The inline open-time `backfill_view`
//! is gated live-only, so a `CatalogEngine::open` in isolation reopens views
//! **empty**; boot view state lives in the runtime layer — checkpoint resume for
//! generation-valid views, the master-driven invalid-view rebuild otherwise —
//! and is exercised by the E2E suite, not this single-process catalog test.
//! These tests assert the catalog-layer contract: index rebuilds once, view
//! defers (comes back empty).

use super::*;

/// Net weight summed over every (PK, payload) the cursor yields. Row *counts*
/// would hide a double-materialisation: rebuilding the same rows twice leaves
/// the row set identical and only the weights doubled.
fn sum_weights(mut c: ReadCursor) -> i64 {
    let mut sum = 0;
    while c.valid {
        sum += c.current_weight;
        c.advance();
    }
    sum
}

// ── index_rebuilds_once_view_defers_on_reopen ───────────────────────────
// Create a base table with N rows, a secondary index, and an identity view
// over it; close; reopen. The base table must come back from its durable
// shards (non-empty — otherwise the index rebuilding to an empty result would
// pass the equality vacuously), the secondary index must hold exactly the
// single-materialisation weights (not doubled), and the view must come back
// **empty** — its rebuild is deferred to the runtime layer (worker pass +
// master cascade), not the catalog open.
//
// This single-process path exercises `backfill_index` (still inline at open)
// and the live CREATE VIEW backfill (is_live during the test's CREATE). The
// boot resume/rebuild of views is covered by the E2E suite.

#[test]
fn index_rebuilds_once_view_defers_on_reopen() {
    const N: i64 = 7;
    let dir = temp_dir("reopen_rebuild_once");

    let mut engine = CatalogEngine::open(&dir).unwrap();

    let cols = vec![u64_col_def("id"), i64_col_def("val")];
    let tid = engine.create_table("public.base", &cols, &[0], true).unwrap();
    let schema = engine.get_schema(tid).unwrap();
    let mut bb = BatchBuilder::new(schema);
    for i in 0..N as u64 {
        bb.begin_row(i as u128, 1);
        bb.put_u64(i * 10);
        bb.end_row();
    }
    engine.ingest_to_family(tid, &bb.finish()).unwrap();

    // Secondary index on val (backfills the N committed rows).
    engine.create_index("public.base", &["val"], false).unwrap();

    // Identity view over base. Circuit and dep rows must precede the VIEW_TAB
    // row so registration can compile and backfill.
    let vid = engine.allocate_table_id();
    write_identity_circuit(&mut engine, vid, tid);
    engine.write_column_records(vid, OWNER_KIND_VIEW, &cols).unwrap();
    engine.write_view_deps(vid, &[tid]).unwrap();

    let batch = build_view_tab_row(vid, "v_base", "SELECT * FROM base");
    engine.ingest_to_family(VIEW_TAB_ID, &batch).unwrap();

    // Single-materialisation reference: the live CREATE backfilled once.
    let view_entry = engine.dag.tables.get(&vid).expect("view registered");
    assert_eq!(
        sum_weights(view_entry.handle.open_cursor()),
        N,
        "live CREATE VIEW must materialise the base rows exactly once"
    );
    let base_entry = engine.dag.tables.get_mut(&tid).unwrap();
    assert_eq!(base_entry.index_circuits.len(), 1);
    assert_eq!(
        sum_weights(base_entry.index_circuits[0].table_mut().open_cursor()),
        N,
        "live CREATE INDEX must backfill the base rows exactly once"
    );

    engine.close();
    drop(engine); // release locks before re-open

    let mut engine2 = CatalogEngine::open(&dir).unwrap();

    // The base table must be non-empty after reopen: it came back from its
    // durable shards. Without this guard an empty base would rebuild an empty
    // index and the equality below would pass vacuously.
    let base_entry = engine2.dag.tables.get(&tid).expect("base table replayed");
    assert_eq!(
        sum_weights(base_entry.handle.open_cursor()),
        N,
        "base table must survive close() → open() from its durable shards"
    );

    // The view's ephemeral storage was erased at open and is NOT rebuilt at the
    // catalog layer: the inline open-time backfill is gated live-only, and boot
    // view rebuild moved to the runtime worker pass + master cascade (E2E).
    let view_entry = engine2.dag.tables.get(&vid).expect("view replayed");
    assert_eq!(
        sum_weights(view_entry.handle.open_cursor()),
        0,
        "view must come back empty at catalog open — rebuild deferred to runtime"
    );

    // Same invariant for the secondary index.
    let base_entry = engine2.dag.tables.get_mut(&tid).unwrap();
    assert_eq!(base_entry.index_circuits.len(), 1, "index circuit replayed");
    assert_eq!(
        sum_weights(base_entry.index_circuits[0].table_mut().open_cursor()),
        N,
        "index must rebuild from source exactly once on reopen, not double"
    );

    engine2.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── index_rebuilds_across_chunk_boundary_view_defers ─────────────────────
// Boot backfills stream the source in DDL_SCAN_CHUNK_ROWS-sized chunks. The
// chunk size cannot be shrunk before open() (the backfill runs during shard
// replay, before any test code can touch the engine), so exercise the real
// boundary with a base table one chunk plus a remainder wide. The secondary
// index must rebuild across that boundary exactly once; the view defers to the
// runtime layer and comes back empty. Also covers the boot index path with the
// unique duplicate check gated off (replay is not live), since the rebuild
// itself must still ingest every chunk.

#[test]
fn index_rebuilds_across_chunk_boundary_view_defers() {
    let n: usize = crate::storage::DDL_SCAN_CHUNK_ROWS + 3;
    let dir = temp_dir("reopen_rebuild_chunked");

    let mut engine = CatalogEngine::open(&dir).unwrap();

    let cols = vec![u64_col_def("id"), i64_col_def("val")];
    let tid = engine.create_table("public.base", &cols, &[0], true).unwrap();
    let schema = engine.get_schema(tid).unwrap();
    let mut next = 0usize;
    while next < n {
        let mut bb = BatchBuilder::new(schema);
        for i in next..(next + 8192).min(n) {
            bb.begin_row(i as u128, 1);
            bb.put_u64((i * 10) as u64);
            bb.end_row();
        }
        engine.ingest_to_family(tid, &bb.finish()).unwrap();
        next += 8192;
    }

    // `val` is distinct, so the unique flavour also covers the live chunked
    // duplicate scan (seen-set across chunks) finding nothing.
    engine.create_index("public.base", &["val"], true).unwrap();

    let vid = engine.allocate_table_id();
    write_identity_circuit(&mut engine, vid, tid);
    engine.write_column_records(vid, OWNER_KIND_VIEW, &cols).unwrap();
    engine.write_view_deps(vid, &[tid]).unwrap();
    let batch = build_view_tab_row(vid, "v_base", "SELECT * FROM base");
    engine.ingest_to_family(VIEW_TAB_ID, &batch).unwrap();

    let view_entry = engine.dag.tables.get(&vid).expect("view registered");
    assert_eq!(
        sum_weights(view_entry.handle.open_cursor()),
        n as i64,
        "live CREATE VIEW must materialise the base rows exactly once"
    );

    engine.close();
    drop(engine);

    let mut engine2 = CatalogEngine::open(&dir).unwrap();

    let base_entry = engine2.dag.tables.get(&tid).expect("base table replayed");
    assert_eq!(
        sum_weights(base_entry.handle.open_cursor()),
        n as i64,
        "base table must survive close() → open() from its durable shards"
    );

    // View is not rebuilt at the catalog layer (deferred to the runtime worker
    // pass + master cascade); the index below is what exercises the chunk
    // boundary at open.
    let view_entry = engine2.dag.tables.get(&vid).expect("view replayed");
    assert_eq!(
        sum_weights(view_entry.handle.open_cursor()),
        0,
        "view must come back empty at catalog open — rebuild deferred to runtime"
    );

    let base_entry = engine2.dag.tables.get_mut(&tid).unwrap();
    assert_eq!(base_entry.index_circuits.len(), 1, "index circuit replayed");
    assert_eq!(
        sum_weights(base_entry.index_circuits[0].table_mut().open_cursor()),
        n as i64,
        "index must rebuild across the chunk boundary exactly once"
    );

    engine2.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── backfill_all_indexes_rebuilds_exactly_once ──────────────────────────
// `backfill_all_indexes` is the worker-boot rebuild: it re-creates each index
// Table (at this process's `index_table_dir` — the parent dir in a Standalone
// unit test) and repopulates it from the base slice. Assert it is a *replace*
// (single materialisation, never additive/doubled) and idempotent across
// repeated calls, and that every key still resolves to its source PK afterward.
//
// This exercises the Standalone role path directly, without a fork: no unit
// test sets a process role, so `index_table_dir` targets the parent dir and the
// call is a legal idempotent re-create-and-rebuild.
#[test]
fn backfill_all_indexes_rebuilds_exactly_once() {
    const N: i64 = 7;
    let dir = temp_dir("backfill_all_indexes_once");
    let mut engine = CatalogEngine::open(&dir).unwrap();

    let cols = vec![u64_col_def("id"), u64_col_def("val")];
    let tid = engine.create_table("public.base", &cols, &[0], true).unwrap();
    let schema = engine.get_schema(tid).unwrap();
    let mut bb = BatchBuilder::new(schema);
    for i in 0..N as u64 {
        bb.begin_row(i as u128, 1);
        bb.put_u64(i * 10);
        bb.end_row();
    }
    engine.ingest_to_family(tid, &bb.finish()).unwrap();
    engine.flush_family(tid).unwrap();

    // Non-unique secondary index on `val`; the live CREATE backfills once.
    engine.create_index("public.base", &["val"], false).unwrap();

    // Reference invariant: every key resolves to its PK, total index weight == N
    // (a doubled rebuild would sum to 2N with an identical row set).
    let assert_index_intact = |engine: &mut CatalogEngine| {
        for i in 0..N as u64 {
            let hit = engine.seek_by_index(tid, &[1], &[(i * 10) as u128]).unwrap();
            let row = hit.unwrap_or_else(|| panic!("val {} must resolve by index", i * 10));
            assert_eq!(row.count, 1, "one source row per distinct val");
            assert_eq!(row.get_pk(0), i as u128, "val {} must resolve to PK {}", i * 10, i);
        }
        let entry = engine.dag.tables.get_mut(&tid).unwrap();
        assert_eq!(entry.index_circuits.len(), 1);
        assert_eq!(
            sum_weights(entry.index_circuits[0].table_mut().open_cursor()),
            N,
            "index must hold exactly one materialisation, not an additive rebuild"
        );
    };

    assert_index_intact(&mut engine);

    // Replace-and-rebuild: swaps in a fresh Table, not additive onto the old.
    engine.backfill_all_indexes().unwrap();
    assert_index_intact(&mut engine);

    // Idempotent across repeated calls.
    engine.backfill_all_indexes().unwrap();
    assert_index_intact(&mut engine);

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}
