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
//! Views are *not* rebuilt at catalog open. `hook_view_register` registers a
//! view empty and never fills it, so a `CatalogEngine::open` in isolation
//! reopens views **empty**; boot view state lives in the runtime layer —
//! checkpoint resume for generation-valid views, the master-driven invalid-view
//! rebuild otherwise — and is exercised by the E2E suite, not this
//! single-process catalog test. These tests assert the catalog-layer contract:
//! index rebuilds once, view defers (comes back empty).

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
// single-materialisation weights (not doubled), and the view must be **empty**
// both at registration and after reopen — every view is populated by the
// runtime layer's distributed backfill, which this single-process engine has
// no counterpart to.
//
// This path therefore exercises `backfill_index` (still inline at open) against
// the *absence* of any catalog-layer view fill. View population is covered by
// the E2E suite.

#[test]
fn index_rebuilds_once_view_defers_on_reopen() {
    const N: i64 = 7;
    let dir = temp_dir("reopen_rebuild_once");

    let mut engine = CatalogEngine::open(&dir).unwrap();

    let cols = vec![col_def("id", type_code::U64), col_def("val", type_code::I64)];
    let tid = engine.create_table("public.base", &cols, &[0]).unwrap();
    let schema = engine.get_schema_desc(tid).unwrap();
    let mut bb = BatchBuilder::new(schema);
    for i in 0..N as u64 {
        bb.begin_row(i as u128, 1);
        bb.put_u64(i * 10);
        bb.end_row();
    }
    engine.ingest_to_family(tid, &bb.finish()).unwrap();

    // Secondary index on val (backfills the N committed rows).
    engine.create_index("public.base", &["val"], false).unwrap();

    // Identity view over base. The circuit rows precede the VIEW_TAB row, the
    // order registration needs to resolve the view's sources and schema.
    let vid = register_identity_view(&mut engine, tid, "v_base", &cols);

    // Registration alone materialises nothing — the runtime's distributed
    // backfill is the sole driver, and a catalog-layer fill here would
    // double-count against it.
    let view_entry = engine.dag.tables.get(&vid).expect("view registered");
    assert_eq!(
        sum_weights(view_entry.handle.open_cursor()),
        0,
        "hook_view_register must leave the view empty"
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
    // catalog layer: boot view state is the runtime layer's (checkpoint resume,
    // else the master-driven rebuild), covered by the E2E suite.
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

// ── index_rebuilds_across_chunk_boundary ─────────────────────────────────
// Boot backfills stream the source in DDL_SCAN_CHUNK_ROWS-sized chunks. The
// chunk size cannot be shrunk before open() (the backfill runs during shard
// replay, before any test code can touch the engine), so exercise the real
// boundary with a base table one chunk plus a remainder wide. The secondary
// index must rebuild across that boundary exactly once. Also covers the boot
// index path with the unique duplicate check gated off (replay is not live),
// since the rebuild itself must still ingest every chunk. (The catalog layer
// never fills a view at all; `index_rebuilds_once_view_defers_on_reopen` pins
// that.)

#[test]
fn index_rebuilds_across_chunk_boundary() {
    let n: usize = crate::catalog::DDL_SCAN_CHUNK_ROWS + 3;
    let dir = temp_dir("reopen_rebuild_chunked");

    let mut engine = CatalogEngine::open(&dir).unwrap();

    let cols = vec![col_def("id", type_code::U64), col_def("val", type_code::I64)];
    let tid = engine.create_table("public.base", &cols, &[0]).unwrap();
    let schema = engine.get_schema_desc(tid).unwrap();
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

    engine.close();
    drop(engine);

    let mut engine2 = CatalogEngine::open(&dir).unwrap();

    let base_entry = engine2.dag.tables.get(&tid).expect("base table replayed");
    assert_eq!(
        sum_weights(base_entry.handle.open_cursor()),
        n as i64,
        "base table must survive close() → open() from its durable shards"
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

    let cols = vec![col_def("id", type_code::U64), col_def("val", type_code::U64)];
    let tid = engine.create_table("public.base", &cols, &[0]).unwrap();
    let schema = engine.get_schema_desc(tid).unwrap();
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
            let hit = engine.seek_by_index(tid, &[1], &[(i * 10) as u128]).unwrap().0;
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
