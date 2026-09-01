//! The master-side `CREATE VIEW` pre-flight compile, and the one-bundle
//! replacement it protects.
//!
//! Without the pre-flight, a view whose circuit the engine cannot compile is
//! still created, still resolvable, and returns no rows forever — the same
//! observable as a correct view over a source matching nothing. The verdict is
//! unreportable from the workers, where the first compile would otherwise
//! happen: that is inside the backfill, after the DDL is durable and every
//! worker has applied it. Compiling on the master inside the DDL is what makes
//! it a client-visible error.

use super::*;

/// A predicate blob whose register file exceeds `MAX_REGS`, so
/// `LogicalProgram::from_wire` rejects it. Forged word by word: the client's
/// `ExprBuilder` refuses to build an over-cap program, so only a corrupt
/// circuit can carry one — which is the input the pre-flight exists to catch.
fn over_cap_pred_blob() -> Vec<u8> {
    let n = gnitz_expr::MAX_REGS as u32 + 1;
    let code: Vec<u32> = (0..n)
        .flat_map(|dst| gnitz_expr::LogicalInstr::LoadConst { val: dst as i64 }.to_wire())
        .collect();
    gnitz_wire::encode_expr_blob(n - 1, &code, &[], &[] as &[&[u8]])
}

/// `ScanDelta(base_tid) → IntegrateTrace → Filter(pred) → Integrate` for `vid`.
/// The filter blob is what decides whether the view compiles.
///
/// The trace node sits *ahead* of the filter deliberately: it is what makes the
/// compile home a scratch child under the compile directory, so the residue
/// assertions below have something to catch on the accepting path *and* on the
/// rejecting one. With the filter first, a rejection would return before any
/// directory existed and "nothing left behind" would hold vacuously.
fn write_filtered_circuit(engine: &mut CatalogEngine, vid: i64, base_tid: i64, pred: &[u8]) {
    write_circuit_chain(
        engine,
        vid,
        &[
            (gnitz_wire::OPCODE_SCAN_DELTA, Some(base_tid), None),
            (gnitz_wire::OPCODE_INTEGRATE_TRACE, None, None),
            (gnitz_wire::OPCODE_FILTER, None, Some(pred)),
            (gnitz_wire::OPCODE_INTEGRATE, None, None),
        ],
    );
}

/// Register `vid` as a view over `base_tid` whose filter is `pred`, exactly as
/// the DDL ingest loop does: circuit and columns first, then the VIEW_TAB row
/// (the hook invariant).
fn register_filtered_view(engine: &mut CatalogEngine, base_tid: i64, name: &str, pred: &[u8]) -> i64 {
    let vid = engine.next_table_id;
    write_filtered_circuit(engine, vid, base_tid, pred);
    let cols = vec![col_def("id", type_code::U64), col_def("v", type_code::I64)];
    engine.write_column_records(vid, OWNER_KIND_VIEW, &cols).unwrap();
    engine
        .ingest_to_family(VIEW_TAB_ID, &build_view_tab_row(vid, name, "SELECT id, v FROM base"))
        .unwrap();
    vid
}

/// Names of the entries directly under `public`'s schema directory — where the
/// pre-flight's throwaway root lives.
fn schema_entries(dir: &str) -> Vec<String> {
    let mut names = gnitz_store::storage::subdir_names(&format!("{dir}/public"));
    names.sort();
    names
}

// ── The pre-flight verdict ──────────────────────────────────────────────────

/// A circuit the engine cannot compile must come back as an error while the DDL
/// is still undoable, and a compilable one must come back clean. Either way it
/// keeps nothing — the throwaway root exists only for the duration of the
/// compile, so a rejected `CREATE VIEW` leaves no directory behind and a later
/// valid view of the same name is unobstructed.
#[test]
fn test_preflight_compile_verdict_and_no_residue() {
    let dir = temp_dir("preflight_verdict");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
    let base_cols = vec![col_def("id", type_code::U64), col_def("v", type_code::I64)];
    let base_tid = engine.create_table("public.base", &base_cols, &[0]).unwrap();

    let before = schema_entries(&dir);

    // A compilable circuit: a well-formed predicate over the base's own columns.
    let ok_vid = register_filtered_view(&mut engine, base_tid, "vok", &pred_lt_blob(1, 100));
    assert!(
        engine.preflight_view_compile(ok_vid).is_ok(),
        "a well-formed circuit must pass the pre-flight"
    );

    // The compile really does home its scratch children under `preflight_dir`:
    // occupy that exact path with a plain file and the same circuit can no
    // longer create them, so it is rejected. Without this the residue assertions
    // below would hold vacuously. It also covers the I/O rejection case — a
    // child table that cannot be created fails the compile instead of yielding a
    // view that never persists its differential state.
    let root = format!("{dir}/public/_preflight_{ok_vid}");
    fs::write(&root, b"").unwrap();
    let io_msg = engine
        .preflight_view_compile(ok_vid)
        .expect_err("an unusable compile directory must fail the pre-flight");
    // `create_dir_all` hits a plain file where a directory belongs (ENOTDIR);
    // the errno must reach the client's string, not flatten to "io error".
    let want = gnitz_store::storage::StorageError::Io(libc::ENOTDIR).to_string();
    assert!(
        io_msg.contains("child table create failed") && io_msg.contains(&want),
        "the rejection must name the failing step and its errno, got: {io_msg}"
    );
    fs::remove_file(&root).unwrap();

    // An over-cap predicate: the register file exceeds MAX_REGS, so the filter's
    // program is rejected, and the message names the limit it exceeded.
    let bad_vid = register_filtered_view(&mut engine, base_tid, "vbad", &over_cap_pred_blob());
    let msg = engine
        .preflight_view_compile(bad_vid)
        .expect_err("an over-cap predicate must fail the pre-flight");
    assert!(
        msg.contains("registers") && msg.contains(&gnitz_expr::MAX_REGS.to_string()),
        "the rejection must state the register limit, got: {msg}"
    );

    // Neither compile left a `_preflight_*` root; the only new entries are the
    // two views' own directories, created by the register hook.
    let after = schema_entries(&dir);
    let new: Vec<&String> = after.iter().filter(|n| !before.contains(n)).collect();
    assert!(
        new.iter().all(|n| !n.starts_with("_preflight")),
        "the pre-flight root must be removed on both paths, found: {new:?}"
    );

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── The one-bundle replacement ──────────────────────────────────────────────

/// `ALTER VIEW` is one DDL zone: the outgoing view's `-1` and the fresh chain's
/// `+1` in a single VIEW_TAB batch, so a rejected new definition leaves the old
/// view untouched. The guard that stands in the way is the qualified-name
/// collision check — precheck runs before apply, so the caches still map the
/// name to the outgoing id. An incumbent this same bundle retires is not a
/// collision; one it does not retire still is.
#[test]
fn test_precheck_admits_a_bundle_that_retires_the_name_it_reuses() {
    let dir = temp_dir("preflight_qname");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
    let base_cols = vec![col_def("id", type_code::U64), col_def("v", type_code::I64)];
    let base_tid = engine.create_table("public.base", &base_cols, &[0]).unwrap();
    let old_vid = register_filtered_view(&mut engine, base_tid, "vw", &pred_lt_blob(1, 100));

    // The replacement's own rows must exist before its VIEW_TAB row is checked.
    let new_vid = engine.next_table_id;
    write_filtered_circuit(&mut engine, new_vid, base_tid, &pred_lt_blob(1, 50));
    let cols = vec![col_def("id", type_code::U64), col_def("v", type_code::I64)];
    engine.write_column_records(new_vid, OWNER_KIND_VIEW, &cols).unwrap();

    // Reusing the live name without retiring the incumbent is still a collision.
    let mut bb = BatchBuilder::new(SysFamily::View.schema());
    push_view_tab_row(&mut bb, 1, new_vid, "vw", "SELECT id, v FROM base", 0, 0);
    let collide = bb.finish();
    assert!(
        engine.precheck_family(SysFamily::View, &collide).is_err(),
        "a second live view under one name must still be rejected"
    );

    // The same `+1` preceded by the incumbent's `-1` — one ALTER VIEW bundle —
    // is admitted. The `-1` reproduces the live row's full payload, which the
    // the retraction CAS requires.
    let mut bb = BatchBuilder::new(SysFamily::View.schema());
    push_view_tab_row(&mut bb, -1, old_vid, "vw", "SELECT id, v FROM base", 0, 0);
    push_view_tab_row(&mut bb, 1, new_vid, "vw", "SELECT id, v FROM base", 0, 0);
    let replace = bb.finish();
    engine
        .precheck_family(SysFamily::View, &replace)
        .expect("a bundle that retires the incumbent may reuse its name");

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

/// Rolling back a replacing bundle has to undo each PK by what the bundle did to
/// *that* PK: the one VIEW_TAB batch registers the new chain and retires the
/// incumbent, so the rollback tears one down and restores the other — opposite
/// family orders, and opposite verdicts on the two directories. Getting it wrong
/// is not a cosmetic loss: removing every queued directory would take the
/// restored view's live data with it, and unregistering the incumbent's rows
/// before its columns are back would fail the rollback and abort the node.
#[test]
fn test_rollback_of_a_replacing_bundle_restores_the_incumbent() {
    let dir = temp_dir("preflight_rollback");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
    let base_cols = vec![col_def("id", type_code::U64), col_def("v", type_code::I64)];
    let base_tid = engine.create_table("public.base", &base_cols, &[0]).unwrap();
    let old_vid = register_filtered_view(&mut engine, base_tid, "vw", &pred_lt_blob(1, 100));
    let old_dir = engine.registry().table_entry(old_vid).unwrap().directory.clone();

    // The handler discards stale queue entries before the bundle it is about to
    // apply; the setup above is not part of that bundle.
    let _ = engine.drain_pending_broadcasts();

    // The replacing bundle: the new chain's own rows, then one VIEW_TAB batch
    // carrying the incumbent's `-1` and the replacement's `+1`.
    let new_vid = engine.next_table_id;
    write_filtered_circuit(&mut engine, new_vid, base_tid, &pred_lt_blob(1, 50));
    let cols = vec![col_def("id", type_code::U64), col_def("v", type_code::I64)];
    engine.write_column_records(new_vid, OWNER_KIND_VIEW, &cols).unwrap();
    let mut bb = BatchBuilder::new(SysFamily::View.schema());
    push_view_tab_row(&mut bb, -1, old_vid, "vw", "SELECT id, v FROM base", 0, 0);
    push_view_tab_row(&mut bb, 1, new_vid, "vw", "SELECT id, v FROM base", 0, 0);
    engine.ingest_to_family(VIEW_TAB_ID, &bb.finish()).unwrap();
    let new_dir = engine.registry().table_entry(new_vid).unwrap().directory.clone();
    assert!(!engine.registry().has_id(old_vid), "the bundle retires the incumbent");

    // The pre-flight rejects the replacement's circuit — the bundle fails after
    // VIEW_TAB was applied, exactly where the handler compensates.
    engine.compensate_stage_a(None).unwrap();

    assert!(
        engine.registry().has_id(old_vid),
        "the incumbent must be registered again"
    );
    assert!(
        std::path::Path::new(&old_dir).exists(),
        "the incumbent's data directory must survive: {old_dir}"
    );
    assert!(
        !engine.registry().has_id(new_vid),
        "the replacement must be unregistered"
    );
    assert!(
        !std::path::Path::new(&new_dir).exists(),
        "the replacement's directory must be removed: {new_dir}"
    );

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}
