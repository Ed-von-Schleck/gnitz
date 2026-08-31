//! Streams: the flag → `RelationKind::Stream` dispatch, the storeless
//! registration it implies, and the two catalog rules that guard a view over one.

use super::*;

/// A `WITH (stream = true)` TABLE_TAB row must register as a stream with no store
/// and no directory. Driven through `hook_relation_register` rather than a hand-built
/// `RelationKind`, so the flag-to-kind dispatch is what is under test.
#[test]
fn stream_flag_registers_storeless_with_no_directory() {
    let dir = temp_dir("stream_registers_storeless");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
    let cols = vec![col_def("id", type_code::U64), col_def("amount", type_code::I64)];

    let sid = create_flagged_table(&mut engine, "s", &cols, &[0], stream_flags());
    let tid = create_flagged_table(&mut engine, "t", &cols, &[0], 0);

    let entry = engine.registry().table_entry(sid).expect("stream registered");
    assert_eq!(entry.kind, RelationKind::Stream);
    assert!(entry.is_storeless(), "a stream holds no store");
    assert!(
        !std::path::Path::new(&entry.directory).exists(),
        "a stream gets no directory: {}",
        entry.directory
    );
    // Its reads are empty rather than erroring, and its LSN never advances.
    assert_eq!(entry.full_scan().count, 0);
    assert_eq!(entry.current_lsn(), 0);

    // The same word with the bit clear is still an ordinary base table with a
    // directory, so the assertions above are about the flag and not the fixture.
    let base = engine.registry().table_entry(tid).expect("table registered");
    assert_eq!(base.kind, RelationKind::BaseTable);
    assert!(!base.is_storeless());
    assert!(std::path::Path::new(&base.directory).exists());

    fs::remove_dir_all(&dir).ok();
}

/// A stream must be absent from the recovery dedup map, not present at LSN 0:
/// present, a torn stream group would demote the zone its coalesced base push
/// rides in.
#[test]
fn a_stream_is_absent_from_the_recovery_dedup_map() {
    let dir = temp_dir("stream_absent_from_dedup_map");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
    let cols = vec![col_def("id", type_code::U64), col_def("amount", type_code::I64)];
    let sid = create_flagged_table(&mut engine, "s", &cols, &[0], stream_flags());
    let tid = create_flagged_table(&mut engine, "t", &cols, &[0], 0);

    let map = engine.registry().user_flushed_lsns();
    assert!(
        !map.contains_key(&sid),
        "a stream must not enter the map at all, not even at 0"
    );
    assert!(map.contains_key(&tid), "the base table beside it still must");

    fs::remove_dir_all(&dir).ok();
}

/// A stream is not a push-conflict target and not an FK parent, so the two
/// predicates that gate the master preflight must stay false for one.
#[test]
fn stream_reads_no_committed_state() {
    let dir = temp_dir("stream_reads_no_committed_state");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
    let cols = vec![col_def("id", type_code::U64), col_def("amount", type_code::I64)];
    let sid = create_flagged_table(&mut engine, "s", &cols, &[0], stream_flags());

    assert!(!engine.push_reads_committed_state(sid, gnitz_wire::WireConflictMode::Update));
    fs::remove_dir_all(&dir).ok();
}

/// A `WITH (capacity = …)` view over a stream must be refused at registration: its
/// skeleton rows are recomputed from the source store, which a stream answers with
/// zero rows rather than an error. Driven from a registration the SQL planner cannot
/// produce — the hand-built-circuit path is the one this rule exists to cover, and a
/// test that only went through SQL would pass with the rule absent.
#[test]
fn bounded_view_over_a_stream_is_rejected() {
    let dir = temp_dir("bounded_view_over_stream");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
    let cols = vec![col_def("id", type_code::U64), col_def("amount", type_code::I64)];
    let sid = create_flagged_table(&mut engine, "s", &cols, &[0], stream_flags());
    let tid = create_flagged_table(&mut engine, "t", &cols, &[0], 0);

    let register_bounded = |engine: &mut CatalogEngine, src: i64, name: &str| {
        try_register_identity_view(engine, src, name, &cols, 4 << 20)
    };

    let err = register_bounded(&mut engine, sid, "bounded_over_stream").expect_err("must be rejected");
    assert!(err.contains("is a stream"), "got: {err}");
    assert!(err.contains(&sid.to_string()), "must name the source: {err}");

    // The same registration over a base table is accepted, so the rejection is
    // about the source's kind and not about the capacity clause.
    register_bounded(&mut engine, tid, "bounded_over_table").expect("bounded view over a table");

    // An *unbounded* view over a stream is exactly what a stream is for.
    register_identity_view(&mut engine, sid, "unbounded_over_stream", &cols);

    fs::remove_dir_all(&dir).ok();
}

/// Neither the live CREATE-VIEW drain nor boot's recovery sweep ticks a stream. Both
/// read this one function, which is what is under test.
#[test]
fn the_drive_set_excludes_a_stream() {
    let dir = temp_dir("stream_excluded_from_sweep");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
    let cols = vec![col_def("id", type_code::U64), col_def("amount", type_code::I64)];
    let sid = create_flagged_table(&mut engine, "s", &cols, &[0], stream_flags());
    let tid = create_flagged_table(&mut engine, "t", &cols, &[0], 0);
    let over_stream = register_identity_view(&mut engine, sid, "v_s", &cols);
    let over_table = register_identity_view(&mut engine, tid, "v_t", &cols);

    let driven = engine
        .dag
        .base_tables_reachable_from(&engine.registry, vec![over_stream, over_table]);
    assert!(driven.contains(&tid), "a base table feeding a view is driven");
    assert!(!driven.contains(&sid), "a stream must never be driven");

    fs::remove_dir_all(&dir).ok();
}

/// A stream-fed view's checkpointed state must never be resumed: its stream inputs
/// are gone at boot. The verdict propagates to a view over it, which phase 2 reaches
/// only because phase 1 put something in the invalid set.
///
/// Every view here is put in the state that *would* resume — matching topology and an
/// output manifest at the committed generation — because that is the only state in
/// which the stream clause decides anything. Without the checkpoint the manifests are
/// absent, every view is invalid on that term alone, and the test passes with the
/// clause deleted.
#[test]
fn stream_fed_views_are_invalid_at_boot() {
    let dir = temp_dir("stream_fed_views_invalid");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
    let cols = vec![col_def("id", type_code::U64), col_def("amount", type_code::I64)];
    let sid = create_flagged_table(&mut engine, "s", &cols, &[0], stream_flags());
    let tid = create_flagged_table(&mut engine, "t", &cols, &[0], 0);

    let direct = register_identity_view(&mut engine, sid, "v_direct", &cols);
    let downstream = register_identity_view(&mut engine, direct, "v_downstream", &cols);
    let over_table = register_identity_view(&mut engine, tid, "v_table", &cols);

    // The two halves of the resume verdict, written the way a boot writes them,
    // then every view's output store published through an ephemeral round at that
    // generation — a completed checkpoint.
    engine.record_topology(1).unwrap();
    let g = engine.bump_checkpoint_generation().unwrap();
    engine.registry_mut().flush_ephemeral_outputs(g).unwrap();

    engine.compute_invalid_views();
    assert!(engine.view_is_invalid(direct), "a direct stream source invalidates");
    assert!(engine.view_is_invalid(downstream), "and the verdict cascades");
    assert!(
        !engine.view_is_invalid(over_table),
        "a checkpointed view over a base table resumes, so the rejection is about the stream"
    );

    fs::remove_dir_all(&dir).ok();
}
