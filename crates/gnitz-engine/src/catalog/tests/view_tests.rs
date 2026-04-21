use super::*;

// ── test_view_backfill_simple ────────────────────────────────────────

#[test]
fn test_view_backfill_simple() {
    let dir = temp_dir("view_backfill");
    let mut engine = CatalogEngine::open(&dir).unwrap();

    let cols = vec![u64_col_def("id"), i64_col_def("val")];
    let tid = engine.create_table("public.t", &cols, 0, true).unwrap();

    // Ingest 5 rows
    let schema = engine.get_schema(tid).unwrap();
    let mut bb = BatchBuilder::new(schema);
    for i in 0..5u64 {
        bb.begin_row(i, 0, 1);
        bb.put_u64((i * 10) as u64);
        bb.end_row();
    }
    engine.dag.ingest_to_family(tid, bb.finish());
    let _ = engine.dag.flush(tid);

    // Create passthrough view
    let out_cols = vec![
        ("id".to_string(), type_code::U64),
        ("val".to_string(), type_code::I64),
    ];
    let graph = make_passthrough_graph(tid, &out_cols);
    let vid = engine.create_view("public.v", &graph, "").unwrap();

    // Verify backfill: view should have 5 rows
    let view_entry = engine.dag.tables.get(&vid).unwrap();
    let view_schema = view_entry.schema;
    let batch = engine.scan_store(vid, &view_schema);
    assert_eq!(batch.count, 5, "View backfill: expected 5 rows, got {}", batch.count);

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── test_view_backfill_on_restart ────────────────────────────────────

#[test]
fn test_view_backfill_on_restart() {
    let dir = temp_dir("view_restart");
    let cols = vec![u64_col_def("id"), i64_col_def("val")];
    let out_cols = vec![
        ("id".to_string(), type_code::U64),
        ("val".to_string(), type_code::I64),
    ];

    {
        let mut engine = CatalogEngine::open(&dir).unwrap();
        let tid = engine.create_table("public.t", &cols, 0, true).unwrap();
        let graph = make_passthrough_graph(tid, &out_cols);
        engine.create_view("public.v", &graph, "").unwrap();

        // Ingest data AFTER view creation (stored in base table)
        let schema = engine.get_schema(tid).unwrap();
        let mut bb = BatchBuilder::new(schema);
        for i in 0..5u64 {
            bb.begin_row(i, 0, 1);
            bb.put_u64((i * 10) as u64);
            bb.end_row();
        }
        engine.dag.ingest_to_family(tid, bb.finish());
        let _ = engine.dag.flush(tid);
        engine.close();
    }

    // Re-open: view should be backfilled from base table data
    {
        let mut engine = CatalogEngine::open(&dir).unwrap();
        let vid = engine.get_by_name("public", "v").unwrap();
        let view_entry = engine.dag.tables.get(&vid).unwrap();
        let view_schema = view_entry.schema;
        let batch = engine.scan_store(vid, &view_schema);
        assert_eq!(batch.count, 5, "View backfill on restart: expected 5, got {}", batch.count);
        engine.close();
    }

    let _ = fs::remove_dir_all(&dir);
}

// ── test_view_on_view_backfill_on_restart ────────────────────────────

#[test]
fn test_view_on_view_backfill() {
    let dir = temp_dir("view_on_view");
    let cols = vec![u64_col_def("id"), i64_col_def("val")];
    let out_cols = vec![
        ("id".to_string(), type_code::U64),
        ("val".to_string(), type_code::I64),
    ];

    {
        let mut engine = CatalogEngine::open(&dir).unwrap();
        let tid = engine.create_table("public.t", &cols, 0, true).unwrap();

        // Ingest data
        let schema = engine.get_schema(tid).unwrap();
        let mut bb = BatchBuilder::new(schema);
        for v in &[5u64, 20, 50, 80, 100] {
            bb.begin_row(*v, 0, 1);
            bb.put_u64(*v);
            bb.end_row();
        }
        engine.dag.ingest_to_family(tid, bb.finish());
        let _ = engine.dag.flush(tid);

        let graph1 = make_passthrough_graph(tid, &out_cols);
        let v1_id = engine.create_view("public.v1", &graph1, "").unwrap();

        let graph2 = make_passthrough_graph(v1_id, &out_cols);
        let v2_id = engine.create_view("public.v2", &graph2, "").unwrap();

        // Check depths
        assert_eq!(engine.get_depth(v1_id), 1);
        assert_eq!(engine.get_depth(v2_id), 2);

        engine.close();
    }

    // Re-open: both views should be backfilled
    {
        let mut engine = CatalogEngine::open(&dir).unwrap();
        let v1_id = engine.get_by_name("public", "v1").unwrap();
        let v2_id = engine.get_by_name("public", "v2").unwrap();
        let v1_schema = engine.dag.tables.get(&v1_id).unwrap().schema;
        let v2_schema = engine.dag.tables.get(&v2_id).unwrap().schema;
        let b1 = engine.scan_store(v1_id, &v1_schema);
        let b2 = engine.scan_store(v2_id, &v2_schema);
        assert_eq!(b1.count, 5, "V1 restart: expected 5, got {}", b1.count);
        assert_eq!(b2.count, 5, "V2 restart: expected 5, got {}", b2.count);
        engine.close();
    }

    let _ = fs::remove_dir_all(&dir);
}

// ── View live incremental update ──────────────────────────────────────

#[test]
fn test_view_live_update() {
    let dir = temp_dir("view_live");
    let mut engine = CatalogEngine::open(&dir).unwrap();

    let cols = vec![u64_col_def("id"), i64_col_def("val")];
    let tid = engine.create_table("public.t", &cols, 0, true).unwrap();
    let schema = engine.get_schema(tid).unwrap();

    // Ingest initial data
    let mut bb = BatchBuilder::new(schema);
    for i in 0..5u64 {
        bb.begin_row(i, 0, 1);
        bb.put_u64(i * 10);
        bb.end_row();
    }
    engine.dag.ingest_to_family(tid, bb.finish());
    let _ = engine.dag.flush(tid);

    // Create passthrough view — should backfill 5 rows
    let out_cols = vec![
        ("id".to_string(), type_code::U64),
        ("val".to_string(), type_code::I64),
    ];
    let graph = make_passthrough_graph(tid, &out_cols);
    let vid = engine.create_view("public.v", &graph, "").unwrap();

    let view_schema = engine.dag.tables.get(&vid).unwrap().schema;
    let b1 = engine.scan_store(vid, &view_schema);
    assert_eq!(b1.count, 5, "Backfill: expected 5, got {}", b1.count);

    // Live update: ingest 1 more row + evaluate DAG
    let mut bb2 = BatchBuilder::new(schema);
    bb2.begin_row(99, 0, 1);
    bb2.put_u64(999);
    bb2.end_row();
    let delta = bb2.finish();
    engine.dag.ingest_by_ref(tid, &delta);
    let _ = engine.dag.flush(tid);
    engine.dag.evaluate_dag(tid, delta);

    let b2 = engine.scan_store(vid, &view_schema);
    assert_eq!(b2.count, 6, "Live update: expected 6, got {}", b2.count);

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── Ghost record / retraction tests ─────────────────────────────────

#[test]
fn test_drop_view_cleans_sys_views() {
    let dir = temp_dir("drop_view_clean");
    let mut engine = CatalogEngine::open(&dir).unwrap();

    // Baseline
    let base_views = count_records(&mut engine.sys_views);

    // Create source table
    let cols = vec![u64_col_def("id"), i64_col_def("val")];
    let tid = engine.create_table("public.src", &cols, 0, true).unwrap();

    // Create view
    let graph = make_passthrough_graph(tid, &[
        ("id".into(), type_code::U64), ("val".into(), type_code::I64),
    ]);
    let vid = engine.create_view("public.vw", &graph, "SELECT * FROM src").unwrap();
    assert_eq!(count_records(&mut engine.sys_views), base_views + 1);

    // Drop view — sys_views must return to baseline (no ghost rows)
    engine.drop_view("public.vw").unwrap();
    let _ = engine.sys_views.flush();
    assert_eq!(count_records(&mut engine.sys_views), base_views,
        "Ghost rows remain in sys_views after DROP VIEW");

    engine.drop_table("public.src").unwrap();
    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_drop_view_cleans_circuit_tables() {
    let dir = temp_dir("drop_view_circuit");
    let mut engine = CatalogEngine::open(&dir).unwrap();

    let base_nodes = count_records(&mut engine.sys_circuit_nodes);
    let base_edges = count_records(&mut engine.sys_circuit_edges);
    let base_sources = count_records(&mut engine.sys_circuit_sources);

    let cols = vec![u64_col_def("id"), i64_col_def("val")];
    let tid = engine.create_table("public.src2", &cols, 0, true).unwrap();

    let graph = make_passthrough_graph(tid, &[
        ("id".into(), type_code::U64), ("val".into(), type_code::I64),
    ]);
    let _vid = engine.create_view("public.vw2", &graph, "SELECT * FROM src2").unwrap();

    // Verify circuit rows were written
    let _ = engine.sys_circuit_nodes.flush();
    assert!(count_records(&mut engine.sys_circuit_nodes) > base_nodes);

    // Drop view — all circuit tables must return to baseline
    engine.drop_view("public.vw2").unwrap();
    let _ = engine.sys_circuit_nodes.flush();
    let _ = engine.sys_circuit_edges.flush();
    let _ = engine.sys_circuit_sources.flush();
    assert_eq!(count_records(&mut engine.sys_circuit_nodes), base_nodes,
        "Ghost rows in sys_circuit_nodes");
    assert_eq!(count_records(&mut engine.sys_circuit_edges), base_edges,
        "Ghost rows in sys_circuit_edges");
    assert_eq!(count_records(&mut engine.sys_circuit_sources), base_sources,
        "Ghost rows in sys_circuit_sources");

    engine.drop_table("public.src2").unwrap();
    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_repeated_create_drop_view() {
    let dir = temp_dir("repeated_view");
    let mut engine = CatalogEngine::open(&dir).unwrap();

    let base_views = count_records(&mut engine.sys_views);
    let base_nodes = count_records(&mut engine.sys_circuit_nodes);

    let cols = vec![u64_col_def("id"), i64_col_def("val")];
    let tid = engine.create_table("public.rep_src", &cols, 0, true).unwrap();

    for i in 0..5 {
        let vname = format!("public.rep_v{}", i);
        let graph = make_passthrough_graph(tid, &[
            ("id".into(), type_code::U64), ("val".into(), type_code::I64),
        ]);
        let _vid = engine.create_view(&vname, &graph, "SELECT *").unwrap();
        engine.drop_view(&vname).unwrap();
    }

    let _ = engine.sys_views.flush();
    let _ = engine.sys_circuit_nodes.flush();
    assert_eq!(count_records(&mut engine.sys_views), base_views,
        "sys_views grew after 5 create+drop cycles");
    assert_eq!(count_records(&mut engine.sys_circuit_nodes), base_nodes,
        "sys_circuit_nodes grew after 5 create+drop cycles");

    engine.drop_table("public.rep_src").unwrap();
    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── Full lifecycle (replaces test_programmable_zset_lifecycle) ────────

#[test]
fn test_full_lifecycle() {
    let dir = temp_dir("lifecycle");
    let mut engine = CatalogEngine::open(&dir).unwrap();

    // 1. Schema
    engine.create_schema("app").unwrap();

    // 2. Tables with FK
    let users_tid = engine.create_table("app.users",
        &[u128_col_def("uid"), str_col_def("username")], 0, true).unwrap();

    let order_cols = vec![
        u64_col_def("oid"),
        ColumnDef { name: "uid".into(), type_code: type_code::U128, is_nullable: false,
                    fk_table_id: users_tid, fk_col_idx: 0 },
        i64_col_def("amount"),
    ];
    let orders_tid = engine.create_table("app.orders", &order_cols, 0, true).unwrap();

    // 3. FK enforcement: invalid order (no matching user)
    let orders_schema = engine.get_schema(orders_tid).unwrap();
    let mut bad = BatchBuilder::new(orders_schema);
    bad.begin_row(1, 0, 1);
    bad.put_u128(0xCAFEBABE, 0xDEADBEEF);
    bad.put_u64(500);
    bad.end_row();
    assert!(engine.validate_fk_inline(orders_tid, &bad.finish()).is_err());

    // 4. Ingest valid user + order
    let users_schema = engine.get_schema(users_tid).unwrap();
    let mut ub = BatchBuilder::new(users_schema);
    ub.begin_row(0xCAFEBABE, 0xDEADBEEF, 1);
    ub.put_string("alice");
    ub.end_row();
    engine.dag.ingest_to_family(users_tid, ub.finish());
    let _ = engine.dag.flush(users_tid);

    let mut ob = BatchBuilder::new(orders_schema);
    ob.begin_row(101, 0, 1);
    ob.put_u128(0xCAFEBABE, 0xDEADBEEF);
    ob.put_u64(1000);
    ob.end_row();
    assert!(engine.validate_fk_inline(orders_tid, &ob.finish()).is_ok());

    // 5. View
    let out_cols = vec![
        ("uid".to_string(), type_code::U128),
        ("username".to_string(), type_code::STRING),
    ];
    let graph = make_passthrough_graph(users_tid, &out_cols);
    let view_tid = engine.create_view("app.active_users", &graph, "SELECT * FROM users").unwrap();

    // 5.1 Can't drop table referenced by view
    assert!(engine.drop_table("app.users").is_err());

    // 6. Persistence
    engine.close();
    drop(engine);

    let mut engine2 = CatalogEngine::open(&dir).unwrap();
    assert!(engine2.get_by_name("app", "users").is_some());
    assert!(engine2.get_by_name("app", "active_users").is_some());

    // 7. Compilation recovery
    assert!(engine2.dag.ensure_compiled(view_tid));

    // 8. Teardown
    engine2.drop_view("app.active_users").unwrap();
    engine2.drop_table("app.orders").unwrap();
    engine2.drop_table("app.users").unwrap();
    engine2.drop_schema("app").unwrap();
    assert!(!engine2.has_schema("app"));

    engine2.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── push_and_evaluate test ───────────────────────────────────────

#[test]
fn test_push_and_evaluate_cascades_to_view() {
    let dir = temp_dir("catalog_push_eval");
    let mut engine = CatalogEngine::open(&dir).unwrap();
    let cols = vec![u64_col_def("id"), u64_col_def("val")];
    let tid = engine.create_table("public.t", &cols, 0, false).unwrap();
    let out_cols = vec![("id".to_string(), type_code::U64), ("val".to_string(), type_code::U64)];

    let graph = make_passthrough_graph(tid, &out_cols);
    let vid = engine.create_view("public.v", &graph, "").unwrap();

    let schema = engine.get_schema(tid).unwrap();
    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(1, 0, 1); bb.put_u64(10); bb.end_row();
    bb.begin_row(2, 0, 1); bb.put_u64(20); bb.end_row();
    engine.push_and_evaluate(tid, bb.finish()).unwrap();

    // View should reflect the push
    let scan = engine.scan_family(vid).unwrap();
    assert_eq!(scan.count, 2);

    // Push more
    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(3, 0, 1); bb.put_u64(30); bb.end_row();
    engine.push_and_evaluate(tid, bb.finish()).unwrap();

    let scan = engine.scan_family(vid).unwrap();
    assert_eq!(scan.count, 3);

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── UPSERT retraction propagation tests ──────────────────────────

#[test]
fn test_upsert_effective_batch_has_retractions() {
    let dir = temp_dir("catalog_upsert_retract");
    let mut engine = CatalogEngine::open(&dir).unwrap();
    let cols = vec![u64_col_def("id"), u64_col_def("val")];
    let tid = engine.create_table("public.t", &cols, 0, true).unwrap(); // unique_pk
    let schema = engine.get_schema(tid).unwrap();

    // Insert PK=1, val=100
    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(1, 0, 1); bb.put_u64(100); bb.end_row();
    engine.dag.ingest_to_family(tid, bb.finish());
    let _ = engine.dag.flush(tid);

    // UPSERT PK=1, val=200 via ingest_returning_effective
    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(1, 0, 1); bb.put_u64(200); bb.end_row();
    let (rc, effective_opt) = engine.dag.ingest_returning_effective(tid, bb.finish());
    assert_eq!(rc, 0);
    let effective = effective_opt.unwrap();

    // Effective batch should have 2 rows:
    // Row 0: weight=-1 (retraction of old val=100)
    // Row 1: weight=+1 (insertion of new val=200)
    assert_eq!(effective.count, 2, "Expected retraction + insertion");
    assert_eq!(effective.get_weight(0), -1, "First row should be retraction");
    assert_eq!(effective.get_weight(1), 1, "Second row should be insertion");
    // Both should have PK=1
    assert_eq!(effective.get_pk(0), 1);
    assert_eq!(effective.get_pk(1), 1);

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_upsert_retraction_reaches_views() {
    let dir = temp_dir("catalog_upsert_view");
    let mut engine = CatalogEngine::open(&dir).unwrap();
    let cols = vec![u64_col_def("id"), u64_col_def("val")];
    let tid = engine.create_table("public.t", &cols, 0, true).unwrap();
    let schema = engine.get_schema(tid).unwrap();
    let out_cols = vec![("id".to_string(), type_code::U64), ("val".to_string(), type_code::U64)];

    // Create passthrough view
    let graph = make_passthrough_graph(tid, &out_cols);
    let vid = engine.create_view("public.v", &graph, "").unwrap();

    // Insert PK=1, val=100
    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(1, 0, 1); bb.put_u64(100); bb.end_row();
    engine.push_and_evaluate(tid, bb.finish()).unwrap();

    // Check view: should have 1 row with val=100
    let scan1 = engine.scan_family(vid).unwrap();
    assert_eq!(scan1.count, 1);

    // UPSERT PK=1, val=200
    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(1, 0, 1); bb.put_u64(200); bb.end_row();
    engine.push_and_evaluate(tid, bb.finish()).unwrap();

    // View should have 1 row with val=200 (old val=100 retracted)
    let scan2 = engine.scan_family(vid).unwrap();
    assert_eq!(scan2.count, 1, "View should have exactly 1 row after UPSERT");

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── Regression: intra-batch modifications to the same PK ─────────────
// Bug 1.1: enforce_unique_pk_partitioned calls ptable.retract_pk which
// is read-only (sets found_source; doesn't mutate the store).  Calling
// it twice for the same PK in one batch emitted the stored-row
// retraction twice, driving downstream weights negative.  Additionally,
// `seen` stored an effective-relative index but was fed to
// append_batch_negated(&batch, ...), negating the wrong row once store
// retractions shifted effective.count ahead of the batch row index.

#[test]
fn test_intra_batch_insert_then_delete_same_pk() {
    // [(+1, pk=1, v=200), (-1, pk=1)] with store (pk=1, v=100) should
    // net to "row (1, 100) retracted, nothing inserted".
    let dir = temp_dir("intra_batch_ins_del");
    let mut engine = CatalogEngine::open(&dir).unwrap();
    let cols = vec![u64_col_def("id"), u64_col_def("val")];
    let tid = engine.create_table("public.t", &cols, 0, true).unwrap();
    let schema = engine.get_schema(tid).unwrap();

    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(1, 0, 1); bb.put_u64(100); bb.end_row();
    engine.dag.ingest_to_family(tid, bb.finish());
    let _ = engine.dag.flush(tid);

    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(1, 0, 1); bb.put_u64(200); bb.end_row();
    bb.begin_row(1, 0, -1); bb.put_u64(0); bb.end_row();
    let effective = engine.ingest_returning_effective(tid, bb.finish()).unwrap();

    // Count retractions of the stored (pk=1, val=100) row.  Must be
    // exactly 1; the bug produced 2.
    let mut retractions_of_100 = 0;
    let col = effective.col_data(0);
    for i in 0..effective.count {
        if effective.get_pk(i) == 1 && effective.get_weight(i) == -1 {
            let v = u64::from_le_bytes(col[i*8..i*8+8].try_into().unwrap());
            if v == 100 { retractions_of_100 += 1; }
        }
    }
    assert_eq!(retractions_of_100, 1,
               "stored row (val=100) must be retracted exactly once; got {}",
               retractions_of_100);

    // Net weight per distinct (pk, val) in the effective batch should
    // leave val=100 at -1 (cancels the stored +1) and val=200 at 0.
    let mut net: HashMap<(u128, u64), i64> = HashMap::new();
    for i in 0..effective.count {
        let pk = effective.get_pk(i);
        let v = u64::from_le_bytes(col[i*8..i*8+8].try_into().unwrap());
        *net.entry((pk, v)).or_insert(0) += effective.get_weight(i);
    }
    assert_eq!(net.get(&(1u128, 100u64)).copied().unwrap_or(0), -1,
               "val=100 must net to -1 (retract stored)");
    assert_eq!(net.get(&(1u128, 200u64)).copied().unwrap_or(0), 0,
               "val=200 must net to 0 (insert+delete cancel)");

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_intra_batch_double_insert_same_pk() {
    // [(+1, pk=1, v=A), (+1, pk=1, v=B)] with store (pk=1, v=old).
    // Expected semantics: stored `old` retracted, `A` transient (+1/-1),
    // `B` inserted.
    let dir = temp_dir("intra_batch_dbl_ins");
    let mut engine = CatalogEngine::open(&dir).unwrap();
    let cols = vec![u64_col_def("id"), u64_col_def("val")];
    let tid = engine.create_table("public.t", &cols, 0, true).unwrap();
    let schema = engine.get_schema(tid).unwrap();

    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(1, 0, 1); bb.put_u64(10); bb.end_row();
    engine.dag.ingest_to_family(tid, bb.finish());
    let _ = engine.dag.flush(tid);

    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(1, 0, 1); bb.put_u64(20); bb.end_row();
    bb.begin_row(1, 0, 1); bb.put_u64(30); bb.end_row();
    let effective = engine.ingest_returning_effective(tid, bb.finish()).unwrap();

    let col = effective.col_data(0);
    let mut net: HashMap<(u128, u64), i64> = HashMap::new();
    for i in 0..effective.count {
        let pk = effective.get_pk(i);
        let v = u64::from_le_bytes(col[i*8..i*8+8].try_into().unwrap());
        *net.entry((pk, v)).or_insert(0) += effective.get_weight(i);
    }

    // Bug manifestations this test would have caught:
    //   - double retraction bug: net(1, 10) would be -2, not -1.
    //   - `seen` index bug: net(1, 20) would be +1 (A survives because
    //     the wrong row got negated), and net(1, 30) net 0.
    assert_eq!(net.get(&(1u128, 10u64)).copied().unwrap_or(0), -1,
               "stored row v=10 retracted exactly once");
    assert_eq!(net.get(&(1u128, 20u64)).copied().unwrap_or(0), 0,
               "transient v=20 must net to 0 (inserted then superseded)");
    assert_eq!(net.get(&(1u128, 30u64)).copied().unwrap_or(0), 1,
               "final v=30 must net to +1");

    // And the store after ingest should have a single row (1, 30).
    let scan = engine.scan_family(tid).unwrap();
    assert_eq!(scan.count, 1, "store must hold exactly one live row");
    let scol = scan.col_data(0);
    let live_val = u64::from_le_bytes(scol[0..8].try_into().unwrap());
    assert_eq!(live_val, 30);

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_intra_batch_same_pk_propagates_to_view() {
    // End-to-end: a view watching the base table must see the correct
    // net delta, not ghost negative-weight rows from double retraction.
    let dir = temp_dir("intra_batch_view");
    let mut engine = CatalogEngine::open(&dir).unwrap();
    let cols = vec![u64_col_def("id"), u64_col_def("val")];
    let tid = engine.create_table("public.t", &cols, 0, true).unwrap();
    let schema = engine.get_schema(tid).unwrap();

    let out_cols = vec![
        ("id".to_string(), type_code::U64),
        ("val".to_string(), type_code::U64),
    ];
    let graph = make_passthrough_graph(tid, &out_cols);
    let vid = engine.create_view("public.v", &graph, "").unwrap();

    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(1, 0, 1); bb.put_u64(100); bb.end_row();
    engine.push_and_evaluate(tid, bb.finish()).unwrap();
    assert_eq!(engine.scan_family(vid).unwrap().count, 1);

    // Intra-batch: replace val 100 → 200 → 300.
    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(1, 0, 1); bb.put_u64(200); bb.end_row();
    bb.begin_row(1, 0, 1); bb.put_u64(300); bb.end_row();
    engine.push_and_evaluate(tid, bb.finish()).unwrap();

    let scan = engine.scan_family(vid).unwrap();
    assert_eq!(scan.count, 1, "view must hold exactly one live row");
    let col = scan.col_data(0);
    let v = u64::from_le_bytes(col[0..8].try_into().unwrap());
    assert_eq!(v, 300);

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}
