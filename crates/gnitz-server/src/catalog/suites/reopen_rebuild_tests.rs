//! Reopen idempotency for derived relations. A reopened index holds exactly one
//! materialisation — reloaded from its checkpoint or refilled from its owner,
//! never both — so these tests read net weights, not row sets.
//!
//! Views are *not* rebuilt at catalog open. `hook_relation_register` registers a
//! view empty and never fills it, so a `CatalogEngine::open` in isolation
//! reopens views **empty**; boot view state is the server's —
//! checkpoint resume for generation-valid views, the master-driven invalid-view
//! rebuild otherwise — and is exercised by the E2E suite, not this
//! single-process catalog test. These tests assert the catalog-layer contract:
//! index rebuilds once, view defers (comes back empty), and a view's operator
//! traces resume — or are rejected — together with its output store.

use super::*;

/// Rows in every fixture that does not need a specific size.
const N: i64 = 7;

/// A `(id, val)` table named `name`, holding `N` rows at `val = id * 10`.
/// Returns its id and the column defs, which every caller needs again.
fn seed_base(engine: &mut CatalogEngine, name: &str) -> (u64, Vec<CatalogColumn>) {
    let cols = vec![col_def("id", TypeCode::U64), col_def("val", TypeCode::U64)];
    let tid = engine.create_table(name, &cols, &[0]).unwrap();
    let schema = engine.registry.relation(tid).map(Relation::schema).unwrap();
    let mut bb = BatchBuilder::new(&schema);
    for i in 0..N as u64 {
        bb.begin_row(i as u128, 1);
        bb.put_u64(i * 10);
        bb.end_row();
    }
    engine.ingest_to_family(tid, &bb.finish()).unwrap();
    (tid, cols)
}

/// `public.base` seeded and flushed to shards, plus one non-unique secondary
/// index on `val`. Returns the table id.
fn base_with_index(engine: &mut CatalogEngine) -> u64 {
    let (tid, _) = seed_base(engine, "public.base");
    engine.registry.checkpoint_base().unwrap();
    engine.create_index("public.base", &["val"], false).unwrap();
    tid
}

/// Net weight of `tid`'s single index circuit.
fn index_weight(engine: &mut CatalogEngine, tid: u64) -> i64 {
    let entry = engine.registry.relation_or_err(tid).unwrap();
    assert_eq!(entry.indexes().len(), 1, "index circuit replayed");
    sum_weights(entry.indexes()[0].cursor())
}

// ── index_rebuilds_once_view_defers_on_reopen ───────────────────────────
// A base table with N rows, a secondary index and an identity view over it,
// closed and reopened: the base comes back non-empty, the index at exactly one
// materialisation, and the view empty — only the server's backfill fills a view.

#[test]
fn index_rebuilds_once_view_defers_on_reopen() {
    let dir = temp_dir("reopen_rebuild_once");

    let mut engine = CatalogEngine::open(&dir, 1).unwrap();

    let cols = vec![col_def("id", TypeCode::U64), col_def("val", TypeCode::I64)];
    let tid = engine.create_table("public.base", &cols, &[0]).unwrap();
    let schema = engine.registry.relation(tid).map(Relation::schema).unwrap();
    let mut bb = BatchBuilder::new(&schema);
    for i in 0..N as u64 {
        bb.begin_row(i as u128, 1);
        bb.put_u64(i * 10);
        bb.end_row();
    }
    engine.ingest_to_family(tid, &bb.finish()).unwrap();

    // Secondary index on val, filled from the N committed rows.
    engine.create_index("public.base", &["val"], false).unwrap();

    // Identity view over base. The circuit rows precede the VIEW_TAB row, the
    // order registration needs to resolve the view's sources and schema.
    let vid = register_identity_view(&mut engine, tid, "v_base", &cols);

    // Registration alone materialises nothing — the server's distributed
    // backfill is the sole driver, and a catalog-layer fill here would
    // double-count against it.
    let view_entry = engine.registry.relation_or_err(vid).expect("view registered");
    assert_eq!(
        sum_weights(view_entry.cursor()),
        0,
        "hook_relation_register must leave the view empty"
    );
    let base_entry = engine.registry.relation_or_err(tid).unwrap();
    assert_eq!(base_entry.indexes().len(), 1);
    assert_eq!(
        sum_weights(base_entry.indexes()[0].cursor()),
        N,
        "live CREATE INDEX must fill from the base rows exactly once"
    );

    engine.close();

    let engine2 = CatalogEngine::open(&dir, 1).unwrap();

    // The base table must be non-empty after reopen: it came back from its
    // durable shards. Without this guard an empty base would rebuild an empty
    // index and the equality below would pass vacuously.
    let base_entry = engine2.registry.relation_or_err(tid).expect("base table replayed");
    assert_eq!(
        sum_weights(base_entry.cursor()),
        N,
        "base table must survive close() → open() from its durable shards"
    );

    // The view's ephemeral storage was erased at open and is NOT rebuilt at the
    // catalog layer: boot view state is the server's (checkpoint resume,
    // else the master-driven rebuild), covered by the E2E suite.
    let view_entry = engine2.registry.relation_or_err(vid).expect("view replayed");
    assert_eq!(
        sum_weights(view_entry.cursor()),
        0,
        "view must come back empty at catalog open — rebuild deferred to the server"
    );

    // Same invariant for the secondary index.
    let base_entry = engine2.registry.relation_or_err(tid).unwrap();
    assert_eq!(base_entry.indexes().len(), 1, "index circuit replayed");
    assert_eq!(
        sum_weights(base_entry.indexes()[0].cursor()),
        N,
        "index must rebuild from source exactly once on reopen, not double"
    );

    engine2.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── index_rebuilds_across_chunk_boundary ─────────────────────────────────
// The open's index rebuild streams the base in `scan_chunk_rows` chunks: a base
// one chunk plus a remainder wide must rebuild across the boundary exactly once.

#[test]
fn index_rebuilds_across_chunk_boundary() {
    let n: usize = gnitz_store::relation::StoreConfig::default().scan_chunk_rows + 3;
    let dir = temp_dir("reopen_rebuild_chunked");

    let mut engine = CatalogEngine::open(&dir, 1).unwrap();

    let cols = vec![col_def("id", TypeCode::U64), col_def("val", TypeCode::I64)];
    let tid = engine.create_table("public.base", &cols, &[0]).unwrap();
    let schema = engine.registry.relation(tid).map(Relation::schema).unwrap();
    let mut next = 0usize;
    while next < n {
        let mut bb = BatchBuilder::new(&schema);
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

    let engine2 = CatalogEngine::open(&dir, 1).unwrap();

    let base_entry = engine2.registry.relation_or_err(tid).expect("base table replayed");
    assert_eq!(
        sum_weights(base_entry.cursor()),
        n as i64,
        "base table must survive close() → open() from its durable shards"
    );

    let base_entry = engine2.registry.relation_or_err(tid).unwrap();
    assert_eq!(base_entry.indexes().len(), 1, "index circuit replayed");
    assert_eq!(
        sum_weights(base_entry.indexes()[0].cursor()),
        n as i64,
        "index must rebuild across the chunk boundary exactly once"
    );

    engine2.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── Index checkpoint resume ─────────────────────────────────────────────

/// Whether `tid`'s index on `val` came back from its checkpoint.
fn index_resumed(engine: &CatalogEngine, tid: u64) -> bool {
    engine
        .registry
        .relation(tid)
        .and_then(|r| r.index_on(&[1]))
        .unwrap()
        .resumed()
}

/// `base_with_index`, plus the state a completed checkpoint leaves behind:
/// the recorded topology — at `recorded_workers` workers — and generation the
/// resume gate compares against, and the index published through an ephemeral
/// round stamped at that generation. Returns `(table id, generation)`.
fn checkpointed_table_with_index(dir: &str, recorded_workers: u32) -> (u64, u64) {
    let mut engine = CatalogEngine::open(dir, 1).unwrap();
    let tid = base_with_index(&mut engine);

    // The two halves of the verdict, written the way a boot writes them.
    engine.record_topology(recorded_workers).unwrap();
    let g = engine.bump_checkpoint_generation().unwrap();

    // The index is the only rederived store this table owns, so the ephemeral
    // round publishes exactly it.
    engine.registry.checkpoint_ephemeral([]).unwrap();

    engine.close();
    (tid, g)
}

// ── index_rebuild_is_skipped_after_resume ───────────────────────────────
#[test]
fn index_rebuild_is_skipped_after_resume() {
    let dir = temp_dir("index_resume_skips_rebuild");
    let (tid, g) = checkpointed_table_with_index(&dir, 1);

    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
    assert!(
        engine.topology_matches() && engine.registry.resume_generation() == g,
        "a matching topology leaves the recovered generation as the whole verdict"
    );
    assert!(
        index_resumed(&engine, tid),
        "a generation-valid index must resume from its checkpoint, not rebuild"
    );
    assert_eq!(
        index_weight(&mut engine, tid),
        N,
        "the resumed index holds exactly one materialisation"
    );

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── index_rebuild_forced_by_topology_change ─────────────────────────────
// The checkpoint recorded four workers and this boot launches one: the
// generation matches, the topology does not.
#[test]
fn index_rebuild_forced_by_topology_change() {
    let dir = temp_dir("index_resume_topology_change");
    let (tid, _g) = checkpointed_table_with_index(&dir, 4);

    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
    assert!(
        !engine.topology_matches(),
        "a foreign topology must refuse every manifest"
    );
    assert!(
        !index_resumed(&engine, tid),
        "a topology change must erase the index despite a matching generation"
    );
    assert_eq!(
        index_weight(&mut engine, tid),
        N,
        "the open's rebuild replaces the erased state, never adds to it"
    );

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── The view twin: one policy for a view's output store and its traces ───
//
// A view's operator traces must look for the same manifest generation its output
// store was opened under. The two are decided at different moments — the store at
// registration, the traces at compile — so a checkpoint landing between them is
// what these two tests put there.

/// `public.vbase` plus a backfilled `DISTINCT` view over it, whose operator trace
/// is the clamp history. Returns `(table id, view id)`, with the view's output
/// store and trace both published at one generation and the engine closed — the
/// state a worker reopens into, plan cache empty.
fn checkpointed_traced_view(dir: &str) -> (u64, u64) {
    let mut engine = CatalogEngine::open(dir, 1).unwrap();
    let (tid, cols) = seed_base(&mut engine, "public.vbase");

    let vid = engine.allocate_ids(1).unwrap();
    let mut circuit = gnitz_wire::Circuit::default();
    let scan = circuit.input_delta(tid, gnitz_wire::ReadBound::None);
    let distinct = circuit.distinct(scan);
    circuit.sink(distinct);
    write_circuit(&mut engine, vid, circuit);
    engine.write_column_records(vid, &cols).unwrap();
    engine
        .ingest_to_family(gnitz_wire::VIEW_TAB, &build_view_tab_row(vid, "v_traced"))
        .unwrap();
    backfill(&mut engine, vid, &[tid]);

    engine.record_topology(1).unwrap();
    let g = engine.bump_checkpoint_generation().unwrap();
    engine.flush_ephemeral_round(g).unwrap();
    assert_eq!(engine.registry.resume_generation(), g);

    engine.close();
    (tid, vid)
}

// ── view_traces_resume_with_their_output_store ──────────────────────────
// A compile must open the traces against the generation the *output store*
// accepted, not whatever the engine holds when the compile runs. After a bump
// between the two, a trace looking for `g + 1` is erased: an empty integral
// under a full output store, which re-inserting a present row exposes as a
// second copy in the view.
#[test]
fn view_traces_resume_with_their_output_store() {
    let dir = temp_dir("view_traces_resume_with_output");
    let (tid, vid) = checkpointed_traced_view(&dir);

    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
    let view_weight = |engine: &CatalogEngine| sum_weights(engine.registry.relation(vid).unwrap().cursor());
    assert_eq!(view_weight(&engine), N, "the output store resumes");
    engine.bump_checkpoint_generation().unwrap();
    engine.dag.open_plan(&engine.registry, vid).unwrap();

    let schema = engine.registry.relation(tid).map(Relation::schema).unwrap();
    let mut bb = BatchBuilder::new(&schema);
    bb.begin_row(0, 1);
    bb.put_u64(0);
    bb.end_row();
    let what = crate::query::Drive::Tick { source: tid, round: 1 };
    crate::query::drive(&mut LocalDrive(&mut engine), what, bb.finish()).unwrap();
    assert_eq!(view_weight(&engine), N, "a resumed trace already holds the row");

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── uncompiled_view_traces_invalidate_the_view ──────────────────────────
// The other half: the ephemeral round stamps every output store but only the
// traces of the views in the plan cache, so a view that goes through a checkpoint
// uncompiled lands an output store one generation ahead of its integral. The boot
// verdict must reject that pair rather than resume it.
#[test]
fn uncompiled_view_traces_invalidate_the_view() {
    let dir = temp_dir("view_uncompiled_traces_invalidate");
    let (_, vid) = checkpointed_traced_view(&dir);

    // A checkpoint with nothing in the plan cache — what a boot checkpoint is for
    // a view no tick sweep reaches.
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
    let g2 = engine.bump_checkpoint_generation().unwrap();
    engine.flush_ephemeral_round(g2).unwrap();
    assert_eq!(engine.registry.resume_generation(), g2);
    engine.close();

    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
    engine.compute_invalid_views();
    assert!(
        engine.dag.awaits_rebuild(vid),
        "an output store ahead of its traces must be rebuilt, not resumed"
    );

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}
