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

/// `ScanDelta(base_tid) → Filter(pred) → Distinct → Integrate` for `vid`. The
/// filter blob is what decides whether the view compiles.
fn write_filtered_circuit(engine: &mut CatalogEngine, vid: u64, base_tid: u64, pred: &[u8]) {
    let mut circuit = gnitz_wire::Circuit::default();
    let scan = circuit.input_delta(base_tid, gnitz_wire::ReadBound::None);
    let filter = circuit.filter(scan, pred.to_vec());
    let distinct = circuit.distinct(filter);
    circuit.sink(distinct);
    write_circuit(engine, vid, circuit);
}

/// Register `vid` as a view over `base_tid` whose filter is `pred`, exactly as
/// `apply_bundle` does: circuit and columns first, then the VIEW_TAB row
/// (the hook invariant).
fn register_filtered_view(engine: &mut CatalogEngine, base_tid: u64, name: &str, pred: &[u8]) -> u64 {
    let vid = engine.allocate_ids(1).unwrap();
    write_filtered_circuit(engine, vid, base_tid, pred);
    let cols = vec![col_def("id", TypeCode::U64), col_def("v", TypeCode::I64)];
    engine.write_column_records(vid, &cols).unwrap();
    engine
        .ingest_to_family(gnitz_wire::VIEW_TAB, &build_view_tab_row(vid, name))
        .unwrap();
    vid
}

// ── The pre-flight verdict ──────────────────────────────────────────────────

/// A circuit the engine cannot compile must come back as an error while the DDL
/// is still undoable, and a compilable one must come back clean.
#[test]
fn test_preflight_compile_verdict() {
    let dir = temp_dir("preflight_verdict");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
    let base_cols = vec![col_def("id", TypeCode::U64), col_def("v", TypeCode::I64)];
    let base_tid = engine.create_table("public.base", &base_cols, &[0]).unwrap();

    // A compilable circuit: a well-formed predicate over the base's own columns.
    let ok_vid = register_filtered_view(
        &mut engine,
        base_tid,
        "vok",
        &cmp_const(gnitz_expr::CmpOp::Lt, 1, 100).to_blob_bytes(),
    );
    assert!(
        crate::query::preflight_compile(&engine.registry, ok_vid).is_ok(),
        "a well-formed circuit must pass the pre-flight"
    );

    // A predicate blob the expression decoder refuses — only a corrupt circuit
    // can carry one, since the client's builder cannot — and the message is the
    // decoder's own.
    let bad_vid = register_filtered_view(&mut engine, base_tid, "vbad", &[0xFF]);
    let msg = crate::query::preflight_compile(&engine.registry, bad_vid)
        .expect_err("an undecodable predicate must fail the pre-flight");
    assert!(
        msg.contains("expr blob"),
        "the rejection must be the decoder's, got: {msg}"
    );

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

/// A circuit whose routing the engine refuses is rejected before the DDL is
/// durable.
#[test]
fn a_view_whose_circuit_is_unroutable_is_rejected_before_the_sal() {
    let dir = temp_dir("preflight_routing");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
    let cols = vec![col_def("id", TypeCode::U64), col_def("v", TypeCode::I64)];
    let a = engine.create_table("public.a", &cols, &[0]).unwrap();
    let b = engine.create_table("public.b", &cols, &[0]).unwrap();

    // The setup is not part of the bundle being compensated.
    let _ = engine.drain_pending_broadcasts();
    let vid = engine.next_id;
    let circuit = equi_join_circuit(a, b, TypeCode::I64, [false, true]);
    let err =
        try_register_view(&mut engine, circuit, "v", &cols, 0, 0).expect_err("the register hook refuses the circuit");
    assert!(
        err.contains(&format!("source {a} feeds a join and states no scatter key")),
        "got: {err}"
    );
    engine.compensate_stage_a().unwrap();
    assert!(!engine.registry.has_id(vid), "the view is not registered");
    assert!(
        engine.live_sys_row(SysFamily::View, vid).is_none(),
        "no VIEW_TAB row of it is live"
    );

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

/// A view bundle whose circuit cell does not decode, and one whose circuit the
/// register hook cannot route, are each refused at the view's row and compensated:
/// no view, no circuit row, and nothing depending on the source.
#[test]
fn a_view_bundle_with_an_unusable_circuit_is_refused_and_compensated() {
    let dir = temp_dir("preflight_unusable_circuit");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
    let cols = vec![col_def("id", TypeCode::U64), col_def("v", TypeCode::I64)];
    let base = engine.create_table("public.base", &cols, &[0]).unwrap();
    let other = engine.create_table("public.other", &cols, &[0]).unwrap();
    let circuits_before = count_records(engine.sys_relation(SysFamily::Circuit).cursor());

    let undecodable = |vid: u64| crate::test_support::circuit_cell_batch(vid, &[0xFF]);
    // Decodes, and joins the base on a key it never states a route for.
    let unroutable = |vid: u64| {
        let circuit = equi_join_circuit(base, other, TypeCode::I64, [false, true]);
        crate::test_support::circuit_batch(vid, &circuit)
    };
    type Rows<'a> = &'a dyn Fn(u64) -> Batch;
    let cases: [(Rows, &str); 2] = [
        (&undecodable, "circuit: truncated"),
        (&unroutable, "feeds a join and states no scatter key"),
    ];
    for (rows, want) in cases {
        // The setup is not part of the bundle being compensated.
        let _ = engine.drain_pending_broadcasts();
        let vid = engine.allocate_ids(1).unwrap();
        engine.write_column_records(vid, &cols).unwrap();
        engine.submit(SysFamily::Circuit, rows(vid)).unwrap();
        let err = engine
            .submit(SysFamily::View, build_view_tab_row(vid, "v"))
            .expect_err("the view's registration is refused");
        assert!(err.starts_with(&format!("view 'v' (id={vid}) ")), "got: {err}");
        assert!(err.contains(want), "got: {err}");
        engine.compensate_stage_a().unwrap();

        assert!(engine.dag.dependents_of(base).is_empty(), "{want}");
        assert!(!engine.registry.has_id(vid), "the view is not registered");
        assert_eq!(
            count_records(engine.sys_relation(SysFamily::Circuit).cursor()),
            circuits_before,
            "{want}"
        );
    }

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

/// A capacity-bounded view is a leaf even to a view its own bundle creates, and the
/// refused bundle leaves neither view an edge.
#[test]
fn a_bundle_may_not_create_a_view_over_its_own_bounded_view() {
    let dir = temp_dir("preflight_bounded_in_bundle");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
    let cols = vec![col_def("id", TypeCode::U64), col_def("v", TypeCode::I64)];
    let base = engine.create_table("public.base", &cols, &[0]).unwrap();
    let _ = engine.drain_pending_broadcasts();
    let bounded = engine.allocate_ids(1).unwrap();
    let over = engine.allocate_ids(1).unwrap();

    let identity = |vid, source| {
        let circuit = crate::test_support::identity_circuit(source, gnitz_wire::ReadBound::None);
        crate::test_support::circuit_batch(vid, &circuit)
    };
    let mut circuits = identity(bounded, base);
    circuits.append_batch(&identity(over, bounded));
    let mut columns = col_tab_batch(bounded, &cols, 1);
    columns.append_batch(&col_tab_batch(over, &cols, 1));
    let mut views = BatchBuilder::new(SysFamily::View.schema());
    push_view_tab_row(&mut views, 1, over, "over", 0, 0, 0);
    push_view_tab_row(&mut views, 1, bounded, "bounded", 4 << 20, 0, 0);

    let err = engine
        .apply_bundle(bundle([
            (SysFamily::Circuit, circuits),
            (SysFamily::Column, columns),
            (SysFamily::View, views.finish()),
        ]))
        .expect_err("a bounded view is a leaf");
    assert!(
        err.contains("reads 'public.bounded', which is a capacity-bounded view"),
        "got: {err}"
    );
    engine.compensate_stage_a().unwrap();

    for id in [base, bounded, over] {
        assert!(engine.dag.dependents_of(id).is_empty(), "{id}");
        assert!(engine.dag.sources_of(id).is_empty(), "{id}");
    }
    assert!(!engine.registry.has_id(bounded) && !engine.registry.has_id(over));

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── The one-bundle replacement ──────────────────────────────────────────────

/// `ALTER VIEW` is one DDL zone: the outgoing view's `-1` and the fresh chain's
/// `+1` in a single VIEW_TAB batch, so a rejected new definition leaves the old
/// view untouched. The guard that stands in the way is the qualified-name
/// collision check — precheck runs before apply, so the name index still maps the
/// name to the outgoing id. An incumbent this same bundle retires is not a
/// collision; one it does not retire still is.
#[test]
fn test_precheck_admits_a_bundle_that_retires_the_name_it_reuses() {
    let dir = temp_dir("preflight_qname");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
    let base_cols = vec![col_def("id", TypeCode::U64), col_def("v", TypeCode::I64)];
    let base_tid = engine.create_table("public.base", &base_cols, &[0]).unwrap();
    let old_vid = register_filtered_view(
        &mut engine,
        base_tid,
        "vw",
        &cmp_const(gnitz_expr::CmpOp::Lt, 1, 100).to_blob_bytes(),
    );

    // The replacement's own rows must exist before its VIEW_TAB row is checked.
    let new_vid = engine.allocate_ids(1).unwrap();
    write_filtered_circuit(
        &mut engine,
        new_vid,
        base_tid,
        &cmp_const(gnitz_expr::CmpOp::Lt, 1, 50).to_blob_bytes(),
    );
    let cols = vec![col_def("id", TypeCode::U64), col_def("v", TypeCode::I64)];
    engine.write_column_records(new_vid, &cols).unwrap();

    // Reusing the live name without retiring the incumbent is still a collision.
    let mut bb = BatchBuilder::new(SysFamily::View.schema());
    push_view_tab_row(&mut bb, 1, new_vid, "vw", 0, 0, 0);
    let collide = bb.finish();
    assert!(
        engine.precheck_family(SysFamily::View, &collide).is_err(),
        "a second live view under one name must still be rejected"
    );

    // The same `+1` preceded by the incumbent's `-1` — one ALTER VIEW bundle —
    // is admitted. The `-1` reproduces the live row's full payload, which the
    // the retraction CAS requires.
    let mut bb = BatchBuilder::new(SysFamily::View.schema());
    push_view_tab_row(&mut bb, -1, old_vid, "vw", 0, 0, 0);
    push_view_tab_row(&mut bb, 1, new_vid, "vw", 0, 0, 0);
    let replace = bb.finish();
    engine
        .precheck_family(SysFamily::View, &replace)
        .expect("a bundle that retires the incumbent may reuse its name");

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

/// One VIEW_TAB batch registers the replacement and retires the incumbent; its
/// rollback must restore the incumbent and retire the replacement.
#[test]
fn test_rollback_of_a_replacing_bundle_restores_the_incumbent() {
    let dir = temp_dir("preflight_rollback");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
    let base_cols = vec![col_def("id", TypeCode::U64), col_def("v", TypeCode::I64)];
    let base_tid = engine.create_table("public.base", &base_cols, &[0]).unwrap();
    let old_vid = register_filtered_view(
        &mut engine,
        base_tid,
        "vw",
        &cmp_const(gnitz_expr::CmpOp::Lt, 1, 100).to_blob_bytes(),
    );
    let old_dir = relation_dir(&dir, old_vid);

    // The setup is not part of the bundle being compensated.
    let _ = engine.drain_pending_broadcasts();

    // The replacing bundle: the new chain's own rows, then one VIEW_TAB batch
    // carrying the incumbent's `-1` and the replacement's `+1`.
    let new_vid = engine.allocate_ids(1).unwrap();
    write_filtered_circuit(
        &mut engine,
        new_vid,
        base_tid,
        &cmp_const(gnitz_expr::CmpOp::Lt, 1, 50).to_blob_bytes(),
    );
    let cols = vec![col_def("id", TypeCode::U64), col_def("v", TypeCode::I64)];
    engine.write_column_records(new_vid, &cols).unwrap();
    let mut bb = BatchBuilder::new(SysFamily::View.schema());
    push_view_tab_row(&mut bb, -1, old_vid, "vw", 0, 0, 0);
    push_view_tab_row(&mut bb, 1, new_vid, "vw", 0, 0, 0);
    engine.ingest_to_family(gnitz_wire::VIEW_TAB, &bb.finish()).unwrap();
    let new_dir = relation_dir(&dir, new_vid);
    assert!(!engine.registry.has_id(old_vid), "the bundle retires the incumbent");

    // The pre-flight rejects the replacement's circuit — the bundle fails after
    // VIEW_TAB was applied, exactly where the handler compensates.
    engine.compensate_stage_a().unwrap();
    engine.reclaim_orphan_dirs();

    assert!(
        engine.registry.has_id(old_vid),
        "the incumbent must be registered again"
    );
    assert!(
        std::path::Path::new(&old_dir).exists(),
        "the incumbent's data directory must survive: {old_dir}"
    );
    assert!(!engine.registry.has_id(new_vid), "the replacement must be unregistered");
    assert!(
        !std::path::Path::new(&new_dir).exists(),
        "the sweep must reclaim the replacement's directory: {new_dir}"
    );

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}
