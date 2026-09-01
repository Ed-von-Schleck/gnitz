//! Reopen-rebuild idempotency for derived relations.
//!
//! A secondary index is populated exactly once when the engine reopens, by
//! whichever of the two paths applies: its shards reload when the manifest
//! carries the committed checkpoint generation, and otherwise they are erased and
//! the backfill re-derives them from the owner. Doing both would sum the loaded
//! shards and the recompute, doubling every weight — so these tests read net
//! weights, and the resume tests at the end read the rebuild *count* too, since
//! a silent rebuild produces the same rows.
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
fn seed_base(engine: &mut CatalogEngine, name: &str) -> (i64, Vec<ColumnDef>) {
    let cols = vec![col_def("id", type_code::U64), col_def("val", type_code::U64)];
    let tid = engine.create_table(name, &cols, &[0]).unwrap();
    let schema = engine.registry().get_schema_desc(tid).unwrap();
    let mut bb = BatchBuilder::new(schema);
    for i in 0..N as u64 {
        bb.begin_row(i as u128, 1);
        bb.put_u64(i * 10);
        bb.end_row();
    }
    engine.ingest_to_family(tid, &bb.finish()).unwrap();
    (tid, cols)
}

/// `public.base` seeded and flushed to shards, plus one non-unique secondary
/// index on `val` that the live CREATE backfills. Returns the table id.
fn base_with_index(engine: &mut CatalogEngine) -> i64 {
    let (tid, _) = seed_base(engine, "public.base");
    engine.registry_mut().flush(tid).unwrap();
    engine.create_index("public.base", &["val"], false).unwrap();
    tid
}

/// Net weight of `tid`'s single index circuit.
fn index_weight(engine: &mut CatalogEngine, tid: i64) -> i64 {
    let entry = engine.registry().table_entry(tid).unwrap();
    assert_eq!(entry.index_circuits.len(), 1, "index circuit replayed");
    sum_weights(entry.index_circuits[0].open_cursor())
}

// ── index_rebuilds_once_view_defers_on_reopen ───────────────────────────
// Create a base table with N rows, a secondary index, and an identity view
// over it; close; reopen. The base table must come back from its durable
// shards (non-empty — otherwise the index rebuilding to an empty result would
// pass the equality vacuously), the secondary index must hold exactly the
// single-materialisation weights (not doubled), and the view must be **empty**
// both at registration and after reopen — every view is populated by the
// server's distributed backfill, which this single-process engine has no
// counterpart to.
//
// This path therefore exercises `backfill_index` (still inline at open) against
// the *absence* of any catalog-layer view fill. View population is covered by
// the E2E suite.

#[test]
fn index_rebuilds_once_view_defers_on_reopen() {
    let dir = temp_dir("reopen_rebuild_once");

    let mut engine = CatalogEngine::open(&dir, 1).unwrap();

    let cols = vec![col_def("id", type_code::U64), col_def("val", type_code::I64)];
    let tid = engine.create_table("public.base", &cols, &[0]).unwrap();
    let schema = engine.registry().get_schema_desc(tid).unwrap();
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

    // Registration alone materialises nothing — the server's distributed
    // backfill is the sole driver, and a catalog-layer fill here would
    // double-count against it.
    let view_entry = engine.registry().table_entry(vid).expect("view registered");
    assert_eq!(
        sum_weights(view_entry.open_cursor()),
        0,
        "hook_relation_register must leave the view empty"
    );
    let base_entry = engine.registry().table_entry(tid).unwrap();
    assert_eq!(base_entry.index_circuits.len(), 1);
    assert_eq!(
        sum_weights(base_entry.index_circuits[0].open_cursor()),
        N,
        "live CREATE INDEX must backfill the base rows exactly once"
    );

    engine.close();

    let engine2 = CatalogEngine::open(&dir, 1).unwrap();

    // The base table must be non-empty after reopen: it came back from its
    // durable shards. Without this guard an empty base would rebuild an empty
    // index and the equality below would pass vacuously.
    let base_entry = engine2.registry().table_entry(tid).expect("base table replayed");
    assert_eq!(
        sum_weights(base_entry.open_cursor()),
        N,
        "base table must survive close() → open() from its durable shards"
    );

    // The view's ephemeral storage was erased at open and is NOT rebuilt at the
    // catalog layer: boot view state is the server's (checkpoint resume,
    // else the master-driven rebuild), covered by the E2E suite.
    let view_entry = engine2.registry().table_entry(vid).expect("view replayed");
    assert_eq!(
        sum_weights(view_entry.open_cursor()),
        0,
        "view must come back empty at catalog open — rebuild deferred to the server"
    );

    // Same invariant for the secondary index.
    let base_entry = engine2.registry().table_entry(tid).unwrap();
    assert_eq!(base_entry.index_circuits.len(), 1, "index circuit replayed");
    assert_eq!(
        sum_weights(base_entry.index_circuits[0].open_cursor()),
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
    let n: usize = gnitz_store::relation::DDL_SCAN_CHUNK_ROWS + 3;
    let dir = temp_dir("reopen_rebuild_chunked");

    let mut engine = CatalogEngine::open(&dir, 1).unwrap();

    let cols = vec![col_def("id", type_code::U64), col_def("val", type_code::I64)];
    let tid = engine.create_table("public.base", &cols, &[0]).unwrap();
    let schema = engine.registry().get_schema_desc(tid).unwrap();
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

    let engine2 = CatalogEngine::open(&dir, 1).unwrap();

    let base_entry = engine2.registry().table_entry(tid).expect("base table replayed");
    assert_eq!(
        sum_weights(base_entry.open_cursor()),
        n as i64,
        "base table must survive close() → open() from its durable shards"
    );

    let base_entry = engine2.registry().table_entry(tid).unwrap();
    assert_eq!(base_entry.index_circuits.len(), 1, "index circuit replayed");
    assert_eq!(
        sum_weights(base_entry.index_circuits[0].open_cursor()),
        n as i64,
        "index must rebuild across the chunk boundary exactly once"
    );

    engine2.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── backfill_all_indexes_rebuilds_exactly_once ──────────────────────────
// `backfill_all_indexes` is the worker-boot rebuild: it re-creates each index
// Table (at this process's `index_dir` — the parent dir in a Standalone
// unit test) and repopulates it from the base slice. Assert it is a *replace*
// (single materialisation, never additive/doubled) and idempotent across
// repeated calls, and that every key still resolves to its source PK afterward.
//
// This exercises the Standalone role path directly, without a fork: no unit
// test sets a process role, so `index_dir` targets the parent dir and the
// call is a legal idempotent re-create-and-rebuild.
#[test]
fn backfill_all_indexes_rebuilds_exactly_once() {
    let dir = temp_dir("backfill_all_indexes_once");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
    let tid = base_with_index(&mut engine);

    // Reference invariant: every key resolves to its PK, total index weight == N
    // (a doubled rebuild would sum to 2N with an identical row set).
    let assert_index_intact = |engine: &mut CatalogEngine| {
        for i in 0..N as u64 {
            let hit = engine
                .registry_mut()
                .seek_by_index(tid, &[1], &[(i * 10) as u128])
                .unwrap()
                .0;
            let row = hit.unwrap_or_else(|| panic!("val {} must resolve by index", i * 10));
            assert_eq!(row.len(), 1, "one source row per distinct val");
            assert_eq!(row.get_pk(0), i as u128, "val {} must resolve to PK {}", i * 10, i);
        }
        assert_eq!(
            index_weight(engine, tid),
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

// ── Index checkpoint resume ─────────────────────────────────────────────
// An index is derived state like a view's output store: the checkpoint's
// ephemeral round publishes it with a generation-stamped manifest, and the next
// open reloads it instead of re-deriving it from a full scan of the owner. The
// verdict has two halves — the generation AND the topology — and both are pinned
// below, by the rebuild *count* rather than the row set (see
// `Table::resumed_from_checkpoint`).

/// `base_with_index`, plus the state a completed checkpoint leaves behind:
/// the recorded topology and generation the resume gate compares against, and
/// the index published through an ephemeral round stamped at that generation.
/// Returns `(table id, generation)`.
fn checkpointed_table_with_index(dir: &str) -> (i64, u64) {
    let mut engine = CatalogEngine::open(dir, 1).unwrap();
    let tid = base_with_index(&mut engine);

    // The two halves of the verdict, written the way a boot writes them.
    engine.record_topology(1).unwrap();
    let g = engine.bump_checkpoint_generation().unwrap();

    // The index is the only rederived store this table owns, so the ephemeral
    // round publishes exactly it.
    engine.registry_mut().flush_ephemeral_outputs(g).unwrap();

    engine.close();
    (tid, g)
}

// ── index_rebuild_is_skipped_after_resume ───────────────────────────────
// An index whose manifest carries the committed generation reloads its shards,
// so neither the registration hook nor the boot rebuild may re-derive it — the
// full-slice scan this mechanism exists to remove. A rebuild that ran anyway
// would show up twice over: in the returned count, and (had the hook backfilled
// on top of the loaded shards) in doubled weights.
#[test]
fn index_rebuild_is_skipped_after_resume() {
    let dir = temp_dir("index_resume_skips_rebuild");
    let (tid, g) = checkpointed_table_with_index(&dir);

    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
    assert_eq!(
        engine.registry().rederive_source(),
        RecoverySource::Rederive { resume_at: Some(g) },
        "a matching topology leaves the recovered generation as the whole verdict"
    );
    assert_eq!(
        engine.backfill_all_indexes().unwrap(),
        0,
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
// The other half of the verdict. A `STATE_FORMAT` bump — the project's lever for
// "every rederived relation must be rebuilt" — changes the topology word at an
// unchanged worker count, which no other path exercises. The generation still
// matches the manifest, so a gate that read it alone would silently resume.
#[test]
fn index_rebuild_forced_by_topology_change() {
    let dir = temp_dir("index_resume_topology_change");
    let (tid, _g) = checkpointed_table_with_index(&dir);

    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
    // Simulate the format bump: the recorded word no longer matches this boot's.
    engine
        .registry_mut()
        .set_recorded_topology(gnitz_store::storage::topology_word(1) + 1);
    assert_eq!(
        engine.registry().rederive_source(),
        RecoverySource::Rederive { resume_at: None },
        "a foreign topology must refuse every manifest"
    );
    assert_eq!(
        engine.backfill_all_indexes().unwrap(),
        1,
        "a topology change must rebuild the index despite a matching generation"
    );
    assert_eq!(
        index_weight(&mut engine, tid),
        N,
        "the rebuild replaces the erased state, never adds to it"
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

/// `public.vbase` plus a trace-bearing view over it. Returns the view id, with
/// its output store and its operator traces both published at one generation and
/// the engine closed — the state a worker reopens into, plan cache empty.
///
/// The `IntegrateTrace` node is what makes the compile create a scratch child at
/// all; an identity circuit would leave nothing for either test to catch on.
fn checkpointed_traced_view(dir: &str) -> i64 {
    let mut engine = CatalogEngine::open(dir, 1).unwrap();
    let (tid, cols) = seed_base(&mut engine, "public.vbase");

    let vid = engine.allocate_table_id().unwrap();
    write_circuit_chain(
        &mut engine,
        vid,
        &[
            (gnitz_wire::OPCODE_SCAN_DELTA, Some(tid), None),
            (gnitz_wire::OPCODE_INTEGRATE_TRACE, None, None),
            (gnitz_wire::OPCODE_INTEGRATE, None, None),
        ],
    );
    engine.write_column_records(vid, OWNER_KIND_VIEW, &cols).unwrap();
    engine
        .ingest_to_family(VIEW_TAB_ID, &build_view_tab_row(vid, "v_traced", "SELECT * FROM vbase"))
        .unwrap();
    assert!(
        engine.dag.ensure_compiled(&engine.registry, vid).unwrap(),
        "the fixture view must compile"
    );

    engine.record_topology(1).unwrap();
    let g = engine.bump_checkpoint_generation().unwrap();
    assert_eq!(engine.flush_ephemeral_round().unwrap(), g);

    engine.close();
    vid
}

// ── view_traces_resume_with_their_output_store ──────────────────────────
// A compile must not sample the resume generation independently of the store it
// is compiling for: it opens the traces against the generation the *output store*
// accepted, not whatever the engine holds when the compile happens to run. A
// generation bump between the two — which is all it takes — otherwise leaves the
// compile looking for `g + 1` while the manifests say `g`, and `Table::new`
// erases them: an empty integral under a full output store, with the view nowhere
// in `invalid_views`.
#[test]
fn view_traces_resume_with_their_output_store() {
    let dir = temp_dir("view_traces_resume_with_output");
    let vid = checkpointed_traced_view(&dir);

    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
    assert!(
        engine.registry().store_resumed(vid),
        "the fixture must leave a resumable output store"
    );
    engine.bump_checkpoint_generation().unwrap();
    assert!(engine.dag.ensure_compiled(&engine.registry, vid).unwrap());

    // The compiled plan's own tables — both the set the next ephemeral round
    // would publish and the integral this view reads through.
    let traces = engine.dag.collect_ephemeral_trace_tables(&engine.registry);
    assert!(!traces.is_empty(), "a trace-bearing view must contribute trace tables");
    for t in traces {
        assert!(
            t.resumed_from_checkpoint(),
            "a trace opened against a generation its manifest never carried is erased"
        );
    }

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
    let vid = checkpointed_traced_view(&dir);

    // A checkpoint with nothing in the plan cache — what a boot checkpoint is for
    // a view no tick sweep reaches.
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
    let g2 = engine.bump_checkpoint_generation().unwrap();
    assert_eq!(engine.flush_ephemeral_round().unwrap(), g2);
    engine.close();

    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
    engine.compute_invalid_views();
    assert!(
        engine.invalid_views.contains(&vid),
        "an output store ahead of its traces must be rebuilt, not resumed"
    );

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}
