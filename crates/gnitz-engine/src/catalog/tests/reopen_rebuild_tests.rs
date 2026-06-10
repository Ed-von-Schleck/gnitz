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

/// Build the minimal identity circuit `ScanDelta(base) → Integrate` for
/// `vid` and write its rows through the applied-delta path. The payload
/// column layout follows `gnitz_wire::CIRCUIT_NODES_COLS` /
/// `CIRCUIT_EDGES_COLS`; the compound PK `(view_id, sub)` is packed by
/// `pack_view_pk`.
fn write_identity_circuit(engine: &mut CatalogEngine, vid: i64, base_tid: i64) {
    let nodes_schema = sys_tab_schema(CIRCUIT_NODES_TAB_ID);
    let mut bb = BatchBuilder::new(nodes_schema);
    // node 0: ScanDelta(base_tid)
    bb.begin_row(pack_view_pk(vid, 0), 1);
    bb.put_u64(0);
    bb.put_u64(gnitz_wire::OPCODE_SCAN_DELTA);
    bb.put_u64(base_tid as u64); // source_table
    bb.put_null();               // reindex_col
    bb.put_null();               // expr_program
    bb.end_row();
    // node 1: Integrate (terminal sink — moves the delta into the view store)
    bb.begin_row(pack_view_pk(vid, 1), 1);
    bb.put_u64(1);
    bb.put_u64(gnitz_wire::OPCODE_INTEGRATE);
    bb.put_null();
    bb.put_null();
    bb.put_null();
    bb.end_row();
    engine.ingest_to_family(CIRCUIT_NODES_TAB_ID, &bb.finish()).unwrap();

    let edges_schema = sys_tab_schema(CIRCUIT_EDGES_TAB_ID);
    let mut bb = BatchBuilder::new(edges_schema);
    bb.begin_row(pack_view_pk(vid, 0), 1);
    bb.put_u64(1);                  // dst_node
    bb.put_u64(gnitz_wire::PORT_IN); // dst_port
    bb.put_u64(0);                  // src_node
    bb.end_row();
    engine.ingest_to_family(CIRCUIT_EDGES_TAB_ID, &bb.finish()).unwrap();
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
    engine.create_index("public.base", "val", false).unwrap();

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
    assert_eq!(sum_weights(view_entry.handle.open_cursor()), N,
        "live CREATE VIEW must materialise the base rows exactly once");
    let base_entry = engine.dag.tables.get_mut(&tid).unwrap();
    assert_eq!(base_entry.index_circuits.len(), 1);
    assert_eq!(sum_weights(base_entry.index_circuits[0].table_mut().open_cursor()), N,
        "live CREATE INDEX must backfill the base rows exactly once");

    engine.close();
    drop(engine); // release locks before re-open

    let mut engine2 = CatalogEngine::open(&dir).unwrap();

    // The base table must be non-empty after reopen: it came back from its
    // durable shards. Without this guard an empty base would rebuild an empty
    // view and the equality below would pass vacuously.
    let base_entry = engine2.dag.tables.get(&tid).expect("base table replayed");
    assert_eq!(sum_weights(base_entry.handle.open_cursor()), N,
        "base table must survive close() → open() from its durable shards");

    // The view's ephemeral storage was erased at open and rebuilt from the
    // base table by backfill_view — exactly once. A doubled total means the
    // rebuild ran against storage that was also loaded from disk.
    let view_entry = engine2.dag.tables.get(&vid).expect("view replayed");
    assert_eq!(sum_weights(view_entry.handle.open_cursor()), N,
        "view must rebuild from source exactly once on reopen, not double");

    // Same invariant for the secondary index.
    let base_entry = engine2.dag.tables.get_mut(&tid).unwrap();
    assert_eq!(base_entry.index_circuits.len(), 1, "index circuit replayed");
    assert_eq!(sum_weights(base_entry.index_circuits[0].table_mut().open_cursor()), N,
        "index must rebuild from source exactly once on reopen, not double");

    engine2.close();
    let _ = fs::remove_dir_all(&dir);
}
