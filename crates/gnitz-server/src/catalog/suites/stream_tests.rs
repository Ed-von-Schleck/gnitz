//! Streams: the flag → `RelationKind::Stream` dispatch, the storeless
//! registration it implies, and the rules that guard a view over one.

use super::*;

/// A `WITH (stream = true)` TABLE_TAB row must register as a stream with no store
/// and no directory. Driven through `hook_relation_register` rather than a hand-built
/// `RelationKind`, so the flag-to-kind dispatch is what is under test.
#[test]
fn stream_flag_registers_storeless_with_no_directory() {
    let dir = temp_dir("stream_registers_storeless");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
    let cols = vec![col_def("id", TypeCode::U64), col_def("amount", TypeCode::I64)];

    let sid = create_flagged_table(&mut engine, "s", &cols, &[0], stream_flags());
    let tid = create_flagged_table(&mut engine, "t", &cols, &[0], 0);

    let entry = engine.registry.relation_or_err(sid).expect("stream registered");
    assert_eq!(entry.kind(), RelationKind::Stream);
    // No directory is what says it holds no store: `build_relation_store`
    // creates one for every kind that opens one.
    let stream_dir = relation_dir(&dir, sid);
    assert!(
        !std::path::Path::new(&stream_dir).exists(),
        "a stream gets no directory: {stream_dir}"
    );
    // A view backfill over it drains nothing.
    let vid = register_identity_view(&mut engine, sid, "v_s", &cols);
    let mut cursor = engine.open_source_cursor(vid, sid).unwrap();
    assert!(
        cursor.drain_chunk(64).is_none(),
        "a stream's source cursor drains nothing"
    );

    // The same word with the bit clear is still an ordinary base table with a
    // directory, so the assertions above are about the flag and not the fixture.
    let base = engine.registry.relation_or_err(tid).expect("table registered");
    assert_eq!(base.kind(), RelationKind::BaseTable);
    assert!(std::path::Path::new(&relation_dir(&dir, tid)).exists());

    fs::remove_dir_all(&dir).ok();
}

/// A stream is not a push-conflict target and not an FK parent, so the two
/// predicates that gate the master preflight must stay false for one.
#[test]
fn stream_reads_no_committed_state() {
    let dir = temp_dir("stream_reads_no_committed_state");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
    let cols = vec![col_def("id", TypeCode::U64), col_def("amount", TypeCode::I64)];
    let sid = create_flagged_table(&mut engine, "s", &cols, &[0], stream_flags());

    assert!(!engine.push_reads_committed_state(sid, gnitz_wire::WireConflictMode::Update));
    fs::remove_dir_all(&dir).ok();
}

/// A bounded filter/projection over a stream is refused at compile; over a table,
/// and a bounded join over a stream, it compiles.
#[test]
fn a_bounded_linear_view_over_a_stream_is_rejected_at_compile() {
    let dir = temp_dir("bounded_view_over_stream");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
    let cols = vec![col_def("id", TypeCode::U64), col_def("amount", TypeCode::I64)];
    let sid = create_flagged_table(&mut engine, "s", &cols, &[0], stream_flags());
    let tid = create_flagged_table(&mut engine, "t", &cols, &[0], 0);

    let linear = try_register_identity_view(&mut engine, sid, "bounded_over_stream", &cols, 4 << 20, 0).unwrap();
    let err = crate::query::preflight_compile(&engine.registry, linear).expect_err("must be rejected");
    assert!(err.contains("over a stream"), "got: {err}");

    let over_table = try_register_identity_view(&mut engine, tid, "bounded_over_table", &cols, 4 << 20, 0).unwrap();
    crate::query::preflight_compile(&engine.registry, over_table).expect("bounded view over a table");

    let join_cols = vec![
        col_def("k", TypeCode::I64),
        col_def("s_id", TypeCode::U64),
        col_def("t_id", TypeCode::U64),
    ];
    let circuit = crate::test_support::two_term_join_circuit(sid, tid, TypeCode::I64);
    let join = try_register_view(&mut engine, circuit, "bounded_join", &join_cols, 4 << 20, 0).unwrap();
    crate::query::preflight_compile(&engine.registry, join).expect("a bounded join over a stream compiles");

    register_identity_view(&mut engine, sid, "unbounded_over_stream", &cols);

    fs::remove_dir_all(&dir).ok();
}

/// Neither the live CREATE-VIEW drain nor boot's recovery sweep ticks a stream. Both
/// read this one function, which is what is under test.
#[test]
fn the_drive_set_excludes_a_stream() {
    let dir = temp_dir("stream_excluded_from_sweep");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
    let cols = vec![col_def("id", TypeCode::U64), col_def("amount", TypeCode::I64)];
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
    let cols = vec![col_def("id", TypeCode::U64), col_def("amount", TypeCode::I64)];
    let sid = create_flagged_table(&mut engine, "s", &cols, &[0], stream_flags());
    let tid = create_flagged_table(&mut engine, "t", &cols, &[0], 0);

    let direct = register_identity_view(&mut engine, sid, "v_direct", &cols);
    let downstream = register_identity_view(&mut engine, direct, "v_downstream", &cols);
    let over_table = register_identity_view(&mut engine, tid, "v_table", &cols);

    // The two halves of the resume verdict, written the way a boot writes them,
    // then every view's output store published through an ephemeral round at that
    // generation — a completed checkpoint.
    engine.record_topology(1).unwrap();
    let g = engine.advance_durable_generation().unwrap();
    engine.registry.checkpoint_ephemeral([], g).unwrap();
    engine.close();

    let engine = CatalogEngine::open(&dir, 1).unwrap();
    assert!(engine.dag.awaits_rebuild(direct), "a direct stream source invalidates");
    assert!(engine.dag.awaits_rebuild(downstream), "and the verdict cascades");
    assert!(
        !engine.dag.awaits_rebuild(over_table),
        "a checkpointed view over a base table resumes, so the rejection is about the stream"
    );

    fs::remove_dir_all(&dir).ok();
}
