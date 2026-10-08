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
    // A view's backfill over it reads nothing, whatever was pushed.
    let vid = register_identity_view(&mut engine, sid, "v_s", &cols);
    let pushed = rows(&engine, sid, 1, [1], |id| [id]);
    engine.ingest_unticked(sid, pushed).unwrap();
    backfill(&mut engine, vid);
    assert_eq!(held(&engine, vid), (0, 0), "a stream's backfill reads nothing");

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
    let err = engine
        .dag
        .preflight_compile(&engine.registry, linear)
        .expect_err("must be rejected");
    assert!(err.contains("over a stream"), "got: {err}");

    let over_table = try_register_identity_view(&mut engine, tid, "bounded_over_table", &cols, 4 << 20, 0).unwrap();
    engine
        .dag
        .preflight_compile(&engine.registry, over_table)
        .expect("bounded view over a table");

    let join_cols = vec![
        col_def("k", TypeCode::I64),
        col_def("s_id", TypeCode::U64),
        col_def("t_id", TypeCode::U64),
    ];
    let circuit = crate::test_support::two_term_join_circuit(sid, tid, TypeCode::I64);
    let join = try_register_view(&mut engine, circuit, "bounded_join", &join_cols, 4 << 20, 0).unwrap();
    engine
        .dag
        .preflight_compile(&engine.registry, join)
        .expect("a bounded join over a stream compiles");

    register_identity_view(&mut engine, sid, "unbounded_over_stream", &cols);

    fs::remove_dir_all(&dir).ok();
}

/// Boot's recovery sweep ticks no stream: it reads this one function, which is
/// what is under test.
#[test]
fn the_drive_set_excludes_a_stream() {
    let dir = temp_dir("stream_excluded_from_sweep");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
    let cols = vec![col_def("id", TypeCode::U64), col_def("amount", TypeCode::I64)];
    let sid = create_flagged_table(&mut engine, "s", &cols, &[0], stream_flags());
    let tid = create_flagged_table(&mut engine, "t", &cols, &[0], 0);
    register_identity_view(&mut engine, sid, "v_s", &cols);
    register_identity_view(&mut engine, tid, "v_t", &cols);

    let driven = engine.dag.scanned_base_tables(&engine.registry);
    assert_eq!(driven, [tid], "the base table a view scans, and not the stream");

    fs::remove_dir_all(&dir).ok();
}

/// A stream-fed view's checkpointed state must never be resumed: its stream inputs
/// are gone at boot. The verdict propagates to a view over it.
///
/// Every view here is put in the state that *would* resume — matching topology and an
/// output manifest at the committed generation, published past the engine's own round,
/// which leaves these views out — because that is the only state in which the stream
/// clause decides anything. Without those manifests every view is invalid on that term
/// alone, and the test passes with the clause deleted.
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
    engine.registry.checkpoint_ephemeral([], g, |_| true).unwrap();
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

/// An ephemeral round publishes nothing of a view a stream reaches — neither its
/// output store nor a trace — and everything of a view over a table.
#[test]
fn an_ephemeral_round_publishes_no_view_a_stream_reaches() {
    let dir = temp_dir("stream_views_unpublished");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
    let cols = vec![col_def("id", TypeCode::U64), col_def("amount", TypeCode::I64)];
    let sid = create_flagged_table(&mut engine, "s", &cols, &[0], stream_flags());
    let tid = create_flagged_table(&mut engine, "t", &cols, &[0], 0);

    let direct = traced_view(&mut engine, sid, "v_direct", &cols);
    let downstream = traced_view(&mut engine, direct, "v_downstream", &cols);
    let over_table = traced_view(&mut engine, tid, "v_table", &cols);

    engine.record_topology(1).unwrap();
    let g = engine.advance_durable_generation().unwrap();
    engine.flush_ephemeral_round(g).unwrap();

    assert_eq!(published_children(&dir, direct), Vec::<String>::new());
    assert_eq!(published_children(&dir, downstream), Vec::<String>::new());
    let table_children = published_children(&dir, over_table);
    assert!(table_children.contains(&"w0of1".to_string()), "got {table_children:?}");
    assert!(
        table_children.iter().any(|c| c.starts_with("scratch_")),
        "the trace of a view over a table is published: {table_children:?}"
    );
    engine.close();

    // What the round left out is what the boot rebuilds, and nothing else.
    let engine = CatalogEngine::open(&dir, 1).unwrap();
    assert!(engine.dag.awaits_rebuild(direct));
    assert!(engine.dag.awaits_rebuild(downstream));
    assert!(!engine.dag.awaits_rebuild(over_table));

    fs::remove_dir_all(&dir).ok();
}
