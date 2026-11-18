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
    let batch = rows(engine, tid, 1, 0..N as u64, |id| [id * 10]);
    engine.registry.ingest(tid, batch).unwrap();
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

// ── index_rebuilds_once_view_defers_on_reopen ───────────────────────────
// A base table with N rows, a secondary index and an identity view over it,
// closed and reopened: the base comes back non-empty, the index at exactly one
// materialisation, and the view empty — only the server's backfill fills a view.

#[test]
fn index_rebuilds_once_view_defers_on_reopen() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().to_str().unwrap();

    let mut engine = CatalogEngine::open(dir, 1).unwrap();

    let (tid, cols) = seed_base(&mut engine, "public.base");

    // Secondary index on val, filled from the N committed rows.
    engine.create_index("public.base", &["val"], false).unwrap();

    // Identity view over base. The circuit rows precede the VIEW_TAB row, the
    // order registration needs to resolve the view's sources and schema.
    let vid = register_identity_view(&mut engine, tid, "v_base", &cols);

    // Registration alone materialises nothing — the server's distributed
    // backfill is the sole driver, and a catalog-layer fill here would
    // double-count against it.
    assert_eq!(
        net_weight(&engine, vid),
        0,
        "hook_relation_register must leave the view empty"
    );
    assert_eq!(
        index_weight(&engine, tid),
        N,
        "live CREATE INDEX must fill from the base rows exactly once"
    );

    engine.close();

    let engine2 = CatalogEngine::open(dir, 1).unwrap();

    // The base table must be non-empty after reopen: it came back from its
    // durable shards. Without this guard an empty base would rebuild an empty
    // index and the equality below would pass vacuously.
    assert_eq!(
        net_weight(&engine2, tid),
        N,
        "base table must survive close() → open() from its durable shards"
    );

    // The view's ephemeral storage was erased at open and is NOT rebuilt at the
    // catalog layer: boot view state is the server's (checkpoint resume,
    // else the master-driven rebuild), covered by the E2E suite.
    assert_eq!(
        net_weight(&engine2, vid),
        0,
        "view must come back empty at catalog open — rebuild deferred to the server"
    );

    // Same invariant for the secondary index.
    assert_eq!(
        index_weight(&engine2, tid),
        N,
        "index must rebuild from source exactly once on reopen, not double"
    );

    engine2.close();
}

// ── index_rebuilds_across_chunk_boundary ─────────────────────────────────
// The open's index rebuild streams the base in `scan_chunk_rows` chunks: a base
// one chunk plus a remainder wide must rebuild across the boundary exactly once.

#[test]
fn index_rebuilds_across_chunk_boundary() {
    let config = chunked(4);
    let n = config.scan_chunk_rows + 3;
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().to_str().unwrap();

    let mut engine = CatalogEngine::open_with(dir, 1, config).unwrap();

    let cols = vec![col_def("id", TypeCode::U64), col_def("val", TypeCode::I64)];
    let tid = engine.create_table("public.base", &cols, &[0]).unwrap();
    let base = rows(&engine, tid, 1, 0..n as u64, |i| [i * 10]);
    engine.registry.ingest(tid, base).unwrap();

    // `val` is distinct, so the unique flavour also covers the live chunked
    // duplicate scan (seen-set across chunks) finding nothing.
    engine.create_index("public.base", &["val"], true).unwrap();

    engine.close();

    let engine2 = CatalogEngine::open_with(dir, 1, chunked(4)).unwrap();

    assert_eq!(
        net_weight(&engine2, tid),
        n as i64,
        "base table must survive close() → open() from its durable shards"
    );
    assert_eq!(
        index_weight(&engine2, tid),
        n as i64,
        "index must rebuild across the chunk boundary exactly once"
    );

    engine2.close();
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
/// the generation the resume gate compares against, and the index published
/// through an ephemeral round stamped at that generation. Returns `(table id,
/// generation)`.
fn checkpointed_table_with_index(dir: &str) -> (u64, u64) {
    let mut engine = CatalogEngine::open(dir, 1).unwrap();
    let tid = base_with_index(&mut engine);

    let g = engine.advance_durable_generation().unwrap();

    // The index is the only rederived store this table owns, so the ephemeral
    // round publishes exactly it.
    engine.registry.checkpoint_ephemeral([], g, |_| true).unwrap();

    engine.close();
    (tid, g)
}

// ── index_rebuild_is_skipped_after_resume ───────────────────────────────
#[test]
fn index_rebuild_is_skipped_after_resume() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().to_str().unwrap();
    let (tid, g) = checkpointed_table_with_index(dir);

    let engine = CatalogEngine::open(dir, 1).unwrap();
    assert_eq!(engine.boot.resume_at, g, "the recovered generation is the resume point");
    assert!(
        index_resumed(&engine, tid),
        "a generation-valid index must resume from its checkpoint, not rebuild"
    );
    assert_eq!(
        index_weight(&engine, tid),
        N,
        "the resumed index holds exactly one materialisation"
    );

    engine.close();
}

// ── index_rebuild_forced_by_worker_count_change ─────────────────────────
// The checkpoint was taken at one worker and this boot launches two: the
// generation matches, but the index it stamped is another slot's.
#[test]
fn index_rebuild_forced_by_worker_count_change() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().to_str().unwrap();
    let (tid, g) = checkpointed_table_with_index(dir);

    let engine = CatalogEngine::open(dir, 2).unwrap();
    assert_eq!(engine.boot.resume_at, g);
    assert!(
        !index_resumed(&engine, tid),
        "an index stamped at another worker count must not resume despite a matching generation"
    );
    let share = net_weight(&engine, tid);
    assert!(0 < share && share < N, "rank 0 of 2 holds a share of the base: {share}");
    assert_eq!(
        index_weight(&engine, tid),
        share,
        "the open's rebuild covers this rank's share of the base exactly once"
    );

    engine.close();
}

// ── The view twin: one policy for a view's output store and its traces ───
//
// A view's operator traces resume from the generation its output store loaded:
// CircuitState::open reads the output's loaded mark, whenever the traces first
// open.

/// A push of row `(id, val)` into [`seed_base`]'s table `tid`, and its tick.
fn tick(engine: &mut CatalogEngine, tid: u64, id: u64, val: u64) {
    let pushed = rows(engine, tid, 1, [id], |_| [val]);
    push_and_tick(engine, tid, pushed);
}

/// `public.vbase` plus a backfilled `DISTINCT` view over it, whose operator trace
/// is the clamp history. Returns `(table id, view id)`, with the view's output
/// store and trace both published at one generation and the engine closed — the
/// state a worker reopens into, plan cache empty.
fn checkpointed_traced_view(dir: &str) -> (u64, u64) {
    let mut engine = CatalogEngine::open(dir, 1).unwrap();
    let (tid, cols) = seed_base(&mut engine, "public.vbase");

    let vid = traced_view(&mut engine, tid, "v_traced", &cols);

    let g = engine.advance_durable_generation().unwrap();
    engine.flush_ephemeral_round(g).unwrap();

    engine.close();
    (tid, vid)
}

// ── view_traces_resume_with_their_output_store ──────────────────────────
// The first tick opens the traces at the generation the output store resumed
// from.
#[test]
fn view_traces_resume_with_their_output_store() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().to_str().unwrap();
    let (tid, vid) = checkpointed_traced_view(dir);

    let mut engine = CatalogEngine::open(dir, 1).unwrap();
    assert_eq!(net_weight(&engine, vid), N, "the output store resumes");

    // An update: its retraction leaves the view only through a trace that holds
    // the row it retracts.
    tick(&mut engine, tid, 0, 1);
    assert_eq!(net_weight(&engine, vid), N, "a resumed trace already holds the row");

    engine.close();
}

// ── an_unrun_view_checkpoints_its_traces_and_resumes ────────────────────
// The other half: a checkpoint stamps a resumed view's traces with its output
// store even where nothing has run the view since the boot, so the next boot
// finds both at one generation and resumes the view. A round at the generation
// the traces already carry — a clean boot's — leaves them closed.
#[test]
fn an_unrun_view_checkpoints_its_traces_and_resumes() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().to_str().unwrap();
    let (tid, vid) = checkpointed_traced_view(dir);

    // Checkpoints no tick precedes — what a boot checkpoint is for a view the
    // replayed tail does not reach: first at the generation the boot resumed, then at
    // the next one.
    let mut engine = CatalogEngine::open(dir, 1).unwrap();
    let g = engine.durable_generation();
    engine.flush_ephemeral_round(g).unwrap();
    assert!(
        !engine.dag.state_opened(vid),
        "traces already at the round's generation stay closed"
    );
    let g2 = engine.advance_durable_generation().unwrap();
    engine.flush_ephemeral_round(g2).unwrap();
    assert!(engine.dag.state_opened(vid), "a moved generation restamps the traces");
    engine.close();

    let mut engine = CatalogEngine::open(dir, 1).unwrap();
    assert!(!engine.boot.rebuild.contains(&vid), "the view resumes");
    tick(&mut engine, tid, 0, 1);
    assert_eq!(net_weight(&engine, vid), N, "a resumed trace still holds the row");

    engine.close();
}

// ── a_missing_trace_child_invalidates_the_view ──────────────────────────
// A crash part-way through removing a view's children can take a trace child and
// leave the output store behind. The verdict reads the children the compiled
// circuit declares, so the missing one rejects the view; a resume would fail the
// first open of its operator state instead.
#[test]
fn a_missing_trace_child_invalidates_the_view() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().to_str().unwrap();
    let (_, vid) = checkpointed_traced_view(dir);
    remove_trace(dir, vid);

    let engine = CatalogEngine::open(dir, 1).unwrap();
    assert!(
        engine.boot.rebuild.contains(&vid),
        "a view missing a trace child must be rebuilt, not resumed"
    );

    engine.close();
}

/// Remove the trace child view `vid` published in `dir`, as a crash part-way
/// through retiring the view's children can.
fn remove_trace(dir: &str, vid: u64) {
    let children = published_children(dir, vid);
    let trace = children
        .iter()
        .find(|c| c.starts_with("scratch_"))
        .expect("a traced view checkpoints its trace");
    fs::remove_dir_all(format!("{}/{trace}", relation_dir(dir, vid))).unwrap();
}

// ── Chains ───────────────────────────────────────────────────────────────

/// [`try_register_chain`] of an identity segment over `tid` and a DISTINCT owner
/// over it: the owner alone holds a trace.
fn register_distinct_chain(
    engine: &mut CatalogEngine,
    tid: u64,
    cols: &[CatalogColumn],
    capacity_bytes: u64,
) -> Result<(u64, u64), String> {
    let identity = crate::test_support::identity_circuit(tid, gnitz_wire::ReadBound::None);
    try_register_chain(engine, &identity, cols, distinct_circuit, cols, capacity_bytes)
}

// ── a_chain_resumes_or_rebuilds_as_one ──────────────────────────────────
// A chain's segment holds no rows, so the verdict that rejects one of its views
// has to reject the segments that feed that view's rebuild — which a check of
// the segment's own children would keep.
#[test]
fn a_chain_resumes_or_rebuilds_as_one() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().to_str().unwrap();
    let mut engine = CatalogEngine::open(dir, 1).unwrap();
    let (tid, cols) = seed_base(&mut engine, "public.cbase");
    let (seg, top) = register_distinct_chain(&mut engine, tid, &cols, 0).unwrap();

    backfill_set(&mut engine, &[seg, top]);
    assert_eq!(
        (net_weight(&engine, seg), net_weight(&engine, top)),
        (0, N),
        "a segment stores nothing at its build"
    );

    tick(&mut engine, tid, N as u64, 0);
    assert_eq!(
        (net_weight(&engine, seg), net_weight(&engine, top)),
        (0, N + 1),
        "a tick passes through it"
    );

    let g = engine.advance_durable_generation().unwrap();
    engine.flush_ephemeral_round(g).unwrap();
    engine.close();

    // Kept: the segment reopens holding nothing, and takes nothing.
    let mut engine = CatalogEngine::open(dir, 1).unwrap();
    assert!(!engine.boot.rebuild.contains(&seg) && !engine.boot.rebuild.contains(&top));
    tick(&mut engine, tid, N as u64 + 1, 0);
    assert_eq!(net_weight(&engine, seg), 0, "a kept chain's segment stays empty");
    engine.close();

    // The owner alone holds a trace; the segment has none to lose.
    remove_trace(dir, top);
    let engine = CatalogEngine::open(dir, 1).unwrap();
    assert!(engine.boot.rebuild.contains(&top), "the owner lost its trace");
    assert!(engine.boot.rebuild.contains(&seg), "its segment rebuilds with it");

    engine.close();
}

// ── only_its_own_unbounded_chain_scans_a_segment ────────────────────────
// A built segment holds no rows, so a view outside its chain would backfill from
// nothing, and a capacity-bounded owner would hydrate from nothing.
#[test]
fn only_its_own_unbounded_chain_scans_a_segment() {
    let (_dir, mut engine) = empty_catalog(1);
    let (tid, cols) = seed_base(&mut engine, "public.sbase");
    let (seg, _) = register_distinct_chain(&mut engine, tid, &cols, 0).unwrap();

    let foreign = try_register_identity_view(&mut engine, seg, "foreign", &cols, 0, 0).unwrap_err();
    assert!(foreign.contains("a segment of view"), "got: {foreign}");
    let bounded = register_distinct_chain(&mut engine, tid, &cols, 4 << 20).unwrap_err();
    assert!(bounded.contains("a segment of view"), "got: {bounded}");
    // Nor can it be indexed: the entries would outlive the rows its build drops.
    let iid = engine.allocate_ids(1).unwrap();
    let indexed = engine
        .precheck_family(SysFamily::Index, &idx_tab_batch(iid, seg, &[1], "ix", false, 1))
        .unwrap_err();
    assert!(indexed.contains("a segment of a view's chain"), "got: {indexed}");

    engine.close();
}

// ── a_rejected_view_opens_empty_and_loses_its_stale_traces ──────────────
// A view the verdict rejects is opened under that verdict: its output store
// empty, and no trace of an earlier compile left beside it.
#[test]
fn a_rejected_view_opens_empty_and_loses_its_stale_traces() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().to_str().unwrap();
    let (_, vid) = checkpointed_traced_view(dir);

    let engine = CatalogEngine::open(dir, 1).unwrap();
    let stale = engine
        .registry
        .child_dir(vid, gnitz_store::relation::ChildKind::Scratch("stale"));
    engine.close();
    fs::create_dir_all(&stale).unwrap();
    remove_trace(dir, vid);

    let engine = CatalogEngine::open(dir, 1).unwrap();
    assert!(engine.boot.rebuild.contains(&vid));
    assert!(!std::path::Path::new(&stale).exists(), "a stale trace is erased");
    assert_eq!(net_weight(&engine, vid), 0, "the output store opens empty");

    engine.close();
}

// ── a_rebuilt_chain_takes_its_owners_dependents ─────────────────────────
// A chain rebuilds whole, and whatever a member of it feeds rebuilds with it —
// here through an owner that does not scan its segment, so nothing downstream
// of the rejected segment reaches the owner's dependent. Kept, that dependent
// would hold what a source since refilled had fed it.
#[test]
fn a_rebuilt_chain_takes_its_owners_dependents() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().to_str().unwrap();
    let mut engine = CatalogEngine::open(dir, 1).unwrap();
    let (tid, cols) = seed_base(&mut engine, "public.dbase");
    let identity = |_| crate::test_support::identity_circuit(tid, gnitz_wire::ReadBound::None);
    let (seg, top) = register_chain(&mut engine, &traced_circuit(tid, &cols), &cols, identity, &cols);
    let over_top = register_identity_view(&mut engine, top, "over_top", &cols);
    backfill_set(&mut engine, &[seg, top, over_top]);
    let g = engine.advance_durable_generation().unwrap();
    engine.flush_ephemeral_round(g).unwrap();
    engine.close();

    remove_trace(dir, seg);
    let mut engine = CatalogEngine::open(dir, 1).unwrap();
    for view in [seg, top, over_top] {
        assert!(engine.boot.rebuild.contains(&view), "view {view}");
    }
    // The boot's order: the rebuilds, then the replayed tail's tick.
    backfill_set(&mut engine, &[seg, top, over_top]);
    tick(&mut engine, tid, N as u64, 0);
    assert_eq!(
        (net_weight(&engine, top), net_weight(&engine, over_top)),
        (N + 1, N + 1)
    );
    engine.close();
}
