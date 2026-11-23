//! Streams: the flag → `RelationKind::Stream` dispatch, the storeless
//! registration it implies, and the rules that guard a view over one.

use super::*;
use crate::test_support::u64_i64_cols;
use gnitz_store::relation::PkRule;

/// A `WITH (stream = true)` TABLE_TAB row must register as a stream with no store
/// and no directory. Driven through `hook_relation_register` rather than a hand-built
/// `RelationKind`, so the flag-to-kind dispatch is what is under test.
#[test]
fn stream_flag_registers_storeless_with_no_directory() {
    let (_dir, mut engine) = empty_catalog(1);
    let cols = u64_i64_cols();

    let sid = create_stream(&mut engine, "public.s", &cols);
    let tid = engine.create_table("public.t", &cols, &[0]).unwrap();

    let entry = engine.registry.relation_or_err(sid).expect("stream registered");
    assert_eq!(entry.kind(), RelationKind::Stream);
    assert!(entry.pk_repeats());
    // No directory is what says it holds no store: `attach_rows`
    // creates one for every kind that opens one.
    let stream_dir = relation_dir(engine.registry.base_dir(), sid);
    assert!(
        !std::path::Path::new(&stream_dir).exists(),
        "a stream gets no directory: {stream_dir}"
    );
    // A view's backfill over it reads nothing, whatever was pushed.
    let vid = register_identity_view(&mut engine, sid, "v_s", &cols);
    let pushed = rows(&engine, sid, 1, [1], |id| [id]);
    engine.ingest_unticked(sid, pushed, PkRule::Enforce).unwrap();
    backfill(&mut engine, vid);
    assert_eq!(held(&engine, vid), (0, 0), "a stream's backfill reads nothing");

    // The same word with the bit clear is still an ordinary base table with a
    // directory, so the assertions above are about the flag and not the fixture.
    let base = engine.registry.relation_or_err(tid).expect("table registered");
    assert_eq!(base.kind(), RelationKind::BaseTable);
    assert!(!base.pk_repeats());
    assert!(std::path::Path::new(&relation_dir(engine.registry.base_dir(), tid)).exists());
}

/// A bounded filter/projection over a stream is refused at compile; over a table,
/// and a bounded join over a stream, it compiles.
#[test]
fn a_bounded_linear_view_over_a_stream_is_rejected_at_compile() {
    let (_dir, mut engine) = empty_catalog(1);
    let cols = u64_i64_cols();
    let sid = create_stream(&mut engine, "public.s", &cols);
    let tid = engine.create_table("public.t", &cols, &[0]).unwrap();

    let err = try_register_identity_view(&mut engine, sid, "bounded_over_stream", &cols, 4 << 20, 0)
        .expect_err("must be rejected");
    assert!(err.contains("over a stream"), "got: {err}");

    try_register_identity_view(&mut engine, tid, "bounded_over_table", &cols, 4 << 20, 0)
        .expect("bounded view over a table");

    let join_cols = vec![
        col_def("k", TypeCode::I64),
        col_def("s_id", TypeCode::U64),
        col_def("t_id", TypeCode::U64),
    ];
    let circuit = crate::test_support::two_term_join_circuit(sid, tid, TypeCode::I64);
    try_register_view(&mut engine, circuit, "bounded_join", &join_cols, 4 << 20, 0)
        .expect("a bounded join over a stream compiles");

    register_identity_view(&mut engine, sid, "unbounded_over_stream", &cols);
}

/// A view reaching a stream awaits a rebuild at reopen, and so does one over it.
#[test]
fn stream_fed_views_are_invalid_at_boot() {
    let (_dir, mut engine) = empty_catalog(1);
    let cols = u64_i64_cols();
    let sid = create_stream(&mut engine, "public.s", &cols);
    assert_unreplayed_source_rebuilds_at_reopen(engine, sid, &cols);
}

/// An ephemeral round publishes no store of a view a stream reaches.
#[test]
fn an_ephemeral_round_publishes_no_view_a_stream_reaches() {
    let (_dir, mut engine) = empty_catalog(1);
    let cols = u64_i64_cols();
    let sid = create_stream(&mut engine, "public.s", &cols);
    assert_round_publishes_nothing_reaching(engine, sid, &cols);
}
