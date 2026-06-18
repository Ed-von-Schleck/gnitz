//! Reopen-rebuild idempotency: ephemeral relations (views, secondary indices)
//! must be repopulated from their sources exactly once when the engine
//! reopens. Their storage is erased at open (`Persistence::Ephemeral`) so the
//! backfill is the *sole* population — if that storage were durable, the
//! loaded shards plus the backfill recompute would sum and every weight would
//! double. These tests are the regression guard for that invariant, which is
//! structural since `RelationKind` derives durability and the rebuild
//! decision from one value.

use super::*;

/// Net weight summed over every (PK, payload) the cursor yields. Row *counts*
/// would hide a double-materialisation: rebuilding the same rows twice leaves
/// the row set identical and only the weights doubled.
fn sum_weights(mut c: CursorHandle) -> i64 {
    let mut sum = 0;
    while c.cursor.valid {
        sum += c.cursor.current_weight;
        c.cursor.advance();
    }
    sum
}

// ── view_and_index_rebuild_once_on_reopen ───────────────────────────────
// Create a base table with N rows, a secondary index, and an identity view
// over it; close; reopen. The base table must come back from its durable
// shards (non-empty — otherwise the view rebuilding to an empty result would
// pass the equality vacuously), and the view and index must hold exactly the
// single-materialisation weights, not doubled.
//
// This single-process path exercises `backfill_view`/`backfill_index` only
// (the view is non-exchange). The multi-worker exchange-view rebuild
// (`backfill_exchange_views`) is covered by the E2E suite.

#[test]
fn view_and_index_rebuild_once_on_reopen() {
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
    // view and the equality below would pass vacuously.
    let base_entry = engine2.dag.tables.get(&tid).expect("base table replayed");
    assert_eq!(
        sum_weights(base_entry.handle.open_cursor()),
        N,
        "base table must survive close() → open() from its durable shards"
    );

    // The view's ephemeral storage was erased at open and rebuilt from the
    // base table by backfill_view — exactly once. A doubled total means the
    // rebuild ran against storage that was also loaded from disk.
    let view_entry = engine2.dag.tables.get(&vid).expect("view replayed");
    assert_eq!(
        sum_weights(view_entry.handle.open_cursor()),
        N,
        "view must rebuild from source exactly once on reopen, not double"
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

// ── view_and_index_rebuild_across_chunk_boundary ─────────────────────────
// Boot backfills stream the source in DDL_SCAN_CHUNK_ROWS-sized chunks. The
// chunk size cannot be shrunk before open() (the backfill runs during shard
// replay, before any test code can touch the engine), so exercise the real
// boundary with a base table one chunk plus a remainder wide. Also covers
// the boot path with the unique duplicate check gated off (replay is not
// live), since the rebuild itself must still ingest every chunk.

#[test]
fn view_and_index_rebuild_across_chunk_boundary() {
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

    let view_entry = engine2.dag.tables.get(&vid).expect("view replayed");
    assert_eq!(
        sum_weights(view_entry.handle.open_cursor()),
        n as i64,
        "view must rebuild across the chunk boundary exactly once"
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
