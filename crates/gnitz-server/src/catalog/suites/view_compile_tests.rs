//! The compile a `CREATE VIEW` runs on the master, and the one-bundle replacement
//! it protects.
//!
//! A view's register hook compiles its circuit, and the master fires that hook
//! inside the DDL, while the bundle is still undoable: a circuit the engine
//! cannot compile is a client-visible error, and no view is created.

use super::*;

/// `ScanDelta(base_tid) → Filter(pred) → Distinct`. The filter blob is what
/// decides whether the view compiles.
fn filtered_circuit(base_tid: u64, pred: &[u8]) -> gnitz_wire::Circuit {
    let mut circuit = gnitz_wire::Circuit::default();
    let scan = circuit.input_delta(base_tid, gnitz_wire::ReadBound::None);
    let filter = circuit.filter(scan, pred.to_vec());
    circuit.distinct(filter);
    circuit
}

fn view_cols() -> Vec<CatalogColumn> {
    vec![col_def("id", TypeCode::U64), col_def("v", TypeCode::I64)]
}

/// Register a view over `base_tid` whose filter is `pred`, as one bundle.
fn try_register_filtered_view(
    engine: &mut CatalogEngine,
    base_tid: u64,
    name: &str,
    pred: &[u8],
) -> Result<u64, String> {
    try_register_view(engine, filtered_circuit(base_tid, pred), name, &view_cols(), 0, 0)
}

/// The blocks creating `vid` as a view over `base_tid` whose filter is `pred`,
/// with `views` as its VIEW_TAB block.
fn filtered_view_blocks(vid: u64, base_tid: u64, pred: &[u8], views: Batch) -> [(SysFamily, Batch); 3] {
    view_blocks([(vid, &filtered_circuit(base_tid, pred), &view_cols()[..])], views)
}

// ── The compile verdict ─────────────────────────────────────────────────────

/// A circuit the engine cannot compile must come back as an error while the DDL
/// is still undoable, and a compilable one must come back clean.
#[test]
fn test_compile_verdict() {
    let dir = temp_dir("view_compile_verdict");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
    let base_cols = vec![col_def("id", TypeCode::U64), col_def("v", TypeCode::I64)];
    let base_tid = engine.create_table("public.base", &base_cols, &[0]).unwrap();

    // A compilable circuit: a well-formed predicate over the base's own columns.
    let pred = cmp_const(gnitz_expr::CmpOp::Lt, 1, 100).to_blob_bytes();
    let ok_vid =
        try_register_filtered_view(&mut engine, base_tid, "vok", &pred).expect("a well-formed circuit compiles");
    assert!(engine.registry.has_id(ok_vid));

    // A predicate blob the expression decoder refuses — only a corrupt circuit
    // can carry one, since the client's builder cannot — and the message is the
    // decoder's own.
    let bad_vid = engine.next_id;
    let msg = try_register_filtered_view(&mut engine, base_tid, "vbad", &[0xFF])
        .expect_err("an undecodable predicate fails the compile");
    assert!(
        msg.contains("expr blob"),
        "the rejection must be the decoder's, got: {msg}"
    );
    assert!(!engine.registry.has_id(bad_vid), "the refused view is not registered");
    assert!(engine.live_sys_row(SysFamily::View, bad_vid).is_none());

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

/// A circuit whose routing the engine refuses is rejected before the DDL is
/// durable.
#[test]
fn a_view_whose_circuit_is_unroutable_is_rejected_before_the_sal() {
    let dir = temp_dir("view_compile_routing");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
    let cols = vec![col_def("id", TypeCode::U64), col_def("v", TypeCode::I64)];
    let a = engine.create_table("public.a", &cols, &[0]).unwrap();
    let b = engine.create_table("public.b", &cols, &[0]).unwrap();

    let vid = engine.next_id;
    let circuit = equi_join_circuit(a, b, TypeCode::I64, [false, true]);
    let err =
        try_register_view(&mut engine, circuit, "v", &cols, 0, 0).expect_err("the register hook refuses the circuit");
    assert!(
        err.contains(&format!("source {a} feeds a join and states no scatter key")),
        "got: {err}"
    );
    assert!(!engine.registry.has_id(vid), "the view is not registered");
    assert!(
        engine.live_sys_row(SysFamily::View, vid).is_none(),
        "no VIEW_TAB row of it is live"
    );

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

/// A view bundle whose circuit cell does not decode, one whose circuit the
/// register hook cannot route, and one holding no circuit row for its view are
/// each refused at the view's row and undone: no view, no circuit row, and
/// nothing depending on the source.
#[test]
fn a_view_bundle_with_an_unusable_circuit_is_refused_and_undone() {
    let dir = temp_dir("view_compile_unusable_circuit");
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
    let absent = |_: u64| BatchBuilder::new(SysFamily::Circuit.schema()).finish();
    type Rows<'a> = &'a dyn Fn(u64) -> Batch;
    let cases: [(Rows, &str); 3] = [
        (&undecodable, "circuit: truncated"),
        (&unroutable, "feeds a join and states no scatter key"),
        (&absent, "has no circuit row"),
    ];
    for (rows, want) in cases {
        let vid = engine.allocate_ids(1).unwrap();
        let blocks = [
            (SysFamily::Column, col_tab_batch(vid, &cols, 1)),
            (SysFamily::Circuit, rows(vid)),
            (SysFamily::View, build_view_tab_row(vid, "v")),
        ];
        let err = apply_ddl(&mut engine, blocks).expect_err("the view's registration is refused");
        assert!(err.starts_with(&format!("view 'v' (id={vid}) ")), "got: {err}");
        assert!(err.contains(want), "got: {err}");

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
    let dir = temp_dir("view_compile_bounded_in_bundle");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
    let cols = vec![col_def("id", TypeCode::U64), col_def("v", TypeCode::I64)];
    let base = engine.create_table("public.base", &cols, &[0]).unwrap();
    let bounded = engine.allocate_ids(1).unwrap();
    let over = engine.allocate_ids(1).unwrap();

    let identity = |source| crate::test_support::identity_circuit(source, gnitz_wire::ReadBound::None);
    let circuits = [identity(base), identity(bounded)];
    let created = [(bounded, &circuits[0], &cols[..]), (over, &circuits[1], &cols[..])];
    let mut views = BatchBuilder::new(SysFamily::View.schema());
    push_view_tab_row(&mut views, 1, over, "over", 0, 0, 0);
    push_view_tab_row(&mut views, 1, bounded, "bounded", 4 << 20, 0, 0);

    let err = apply_ddl(&mut engine, view_blocks(created, views.finish())).expect_err("a bounded view is a leaf");
    assert!(
        err.contains("reads 'public.bounded', which is a capacity-bounded view"),
        "got: {err}"
    );

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
    let dir = temp_dir("view_compile_qname");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
    let base_cols = vec![col_def("id", TypeCode::U64), col_def("v", TypeCode::I64)];
    let base_tid = engine.create_table("public.base", &base_cols, &[0]).unwrap();
    let pred = cmp_const(gnitz_expr::CmpOp::Lt, 1, 100).to_blob_bytes();
    let old_vid = try_register_filtered_view(&mut engine, base_tid, "vw", &pred).unwrap();

    // The replacement's column records must be stored before its VIEW_TAB row is
    // checked: staged as a worker applies them, since no bundle carries them alone.
    let new_vid = engine.allocate_ids(1).unwrap();
    engine
        .ddl_sync(SysFamily::Column.id(), col_tab_batch(new_vid, &view_cols(), 1))
        .unwrap();

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
    let dir = temp_dir("view_compile_rollback");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
    let base_cols = vec![col_def("id", TypeCode::U64), col_def("v", TypeCode::I64)];
    let base_tid = engine.create_table("public.base", &base_cols, &[0]).unwrap();
    let pred = cmp_const(gnitz_expr::CmpOp::Lt, 1, 100).to_blob_bytes();
    let old_vid = try_register_filtered_view(&mut engine, base_tid, "vw", &pred).unwrap();
    let old_dir = relation_dir(&dir, old_vid);

    // The replacing bundle: the new chain's own rows, and one VIEW_TAB batch
    // carrying the incumbent's `-1` and the replacement's `+1`.
    let new_vid = engine.allocate_ids(1).unwrap();
    let mut bb = BatchBuilder::new(SysFamily::View.schema());
    push_view_tab_row(&mut bb, -1, old_vid, "vw", 0, 0, 0);
    push_view_tab_row(&mut bb, 1, new_vid, "vw", 0, 0, 0);
    let pred = cmp_const(gnitz_expr::CmpOp::Lt, 1, 50).to_blob_bytes();
    let zone = engine
        .apply_bundle(bundle(filtered_view_blocks(new_vid, base_tid, &pred, bb.finish())))
        .unwrap();
    let new_dir = relation_dir(&dir, new_vid);
    assert!(!engine.registry.has_id(old_vid), "the bundle retires the incumbent");
    assert!(engine.registry.has_id(new_vid), "and registers the replacement");

    // The log refuses the zone: the driver undoes it.
    engine.undo(zone).unwrap();
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

/// `a EXCEPT ALL b` over leaves keyed by their own PK equals its definition,
/// weights included, after a backfill and after each write to either side.
#[test]
fn a_clamp_over_its_sources_stores_holds_its_definition_through_ticks() {
    use std::collections::BTreeMap;
    let dir = temp_dir("clamp_over_source_stores");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
    let cols = vec![col_def("id", TypeCode::U64), col_def("val", TypeCode::U64)];
    let [a, b] = ["public.a", "public.b"].map(|name| engine.create_table(name, &cols, &[0]).unwrap());
    // `b` agrees with `a` on ids 5..10 but 7.
    let mut held: [BTreeMap<u64, u64>; 2] = [
        (0..10).map(|id| (id, id)).collect(),
        (5..15).map(|id| (id, if id == 7 { 70 } else { id })).collect(),
    ];
    for (tid, rows_of) in [(a, &held[0]), (b, &held[1])] {
        let batch = rows(&engine, tid, 1, rows_of.keys().copied(), |id| [rows_of[&id]]);
        engine.registry.ingest(tid, batch).unwrap();
    }

    let mut circuit = Circuit::default();
    let [da, db] = [a, b].map(|s| {
        let leaf = circuit.input_delta(s, gnitz_wire::ReadBound::None);
        circuit.shard(leaf)
    });
    circuit.positive_diff(da, db);
    let vid = try_register_view(&mut engine, circuit, "v", &cols, 0, 0).unwrap();
    backfill(&mut engine, vid);

    let check = |engine: &mut CatalogEngine, held: &[BTreeMap<u64, u64>; 2], step: &str| {
        let out = read_rows(engine, vid, gnitz_wire::ReadBound::None);
        let mut got: Vec<(u64, u64, i64)> = (0..out.len())
            .map(|i| (out.get_pk(i) as u64, payload_u64(&*out, i, 0), out.get_weight(i)))
            .collect();
        got.sort();
        let want: Vec<(u64, u64, i64)> = held[0]
            .iter()
            .filter(|&(id, val)| held[1].get(id) != Some(val))
            .map(|(&id, &val)| (id, val, 1))
            .collect();
        assert_eq!(got, want, "{step}");
    };
    check(&mut engine, &held, "backfill");

    // (side, id, Some(val) to write, None to delete)
    let steps: &[(usize, u64, Option<u64>)] = &[
        (0, 20, Some(20)), // a-only: enters
        (1, 20, Some(20)), // matched: leaves
        (0, 3, Some(99)),  // an update: one element out, one in
        (1, 6, None),      // the match goes: a's row enters
        (1, 3, Some(99)),  // matches a's new row: it leaves
        (0, 7, Some(70)),  // onto b's row: leaves
        (0, 7, None),      // gone from a
        (1, 7, None),      // and from b: nothing to enter
        (0, 7, Some(7)),   // back in a: enters
    ];
    for &(side, id, write) in steps {
        let tid = [a, b][side];
        let batch = match write {
            Some(val) => {
                held[side].insert(id, val);
                rows(&engine, tid, 1, [id], |_| [val])
            }
            None => {
                let val = held[side].remove(&id).expect("a held row");
                rows(&engine, tid, -1, [id], |_| [val])
            }
        };
        engine.ingest_unticked(tid, batch).unwrap();
        seal_and_tick(&mut engine, tid);
        check(&mut engine, &held, &format!("side {side} id {id} {write:?}"));
    }
    engine.close();
    let _ = fs::remove_dir_all(&dir);
}
