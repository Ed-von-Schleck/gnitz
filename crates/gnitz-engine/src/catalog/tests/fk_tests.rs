use super::*;

// ── test_fk_lock_set ─────────────────────────────────────────────────

#[test]
fn test_fk_lock_set() {
    let dir = temp_dir("fk_lock_set");
    let mut engine = CatalogEngine::open(&dir).unwrap();

    // Table with no constraints (unique_pk=false): no lock needed, empty set.
    let plain_tid = engine.create_table("public.plain", &[u64_col_def("id")], 0, false).unwrap();
    assert!(engine.fk_lock_set(plain_tid).is_empty());

    // unique_pk=true table, no FK yet: needs a lock for itself, but no peers.
    let parent_tid = engine.create_table("public.parent", &[u64_col_def("pid")], 0, true).unwrap();
    assert_eq!(engine.fk_lock_set(parent_tid), vec![parent_tid]);

    // Add a child with FK to parent. Now both tables share a lock neighborhood,
    // returned sorted ascending so both use the same acquisition order.
    let child_cols = vec![
        u64_col_def("cid"),
        ColumnDef { name: "fk".into(), type_code: type_code::U64, is_nullable: false,
                    fk_table_id: parent_tid, fk_col_idx: 0 },
    ];
    let child_tid = engine.create_table("public.child", &child_cols, 0, true).unwrap();
    let mut expected = vec![parent_tid, child_tid];
    expected.sort_unstable();
    assert_eq!(engine.fk_lock_set(child_tid), expected, "child sees parent in its lock set");
    assert_eq!(engine.fk_lock_set(parent_tid), expected, "parent sees child in its lock set");

    // Second child: parent's neighborhood grows; each child only sees itself + parent.
    let child2_cols = vec![
        u64_col_def("cid"),
        ColumnDef { name: "fk".into(), type_code: type_code::U64, is_nullable: false,
                    fk_table_id: parent_tid, fk_col_idx: 0 },
    ];
    let child2_tid = engine.create_table("public.child2", &child2_cols, 0, true).unwrap();
    let mut expected3 = vec![parent_tid, child_tid, child2_tid];
    expected3.sort_unstable();
    assert_eq!(engine.fk_lock_set(parent_tid), expected3);
    let mut expected_c2 = vec![parent_tid, child2_tid];
    expected_c2.sort_unstable();
    assert_eq!(engine.fk_lock_set(child2_tid), expected_c2);

    // Drop child: parent's neighborhood shrinks back to itself + child2 only.
    engine.drop_table("public.child").unwrap();
    assert_eq!(engine.fk_lock_set(parent_tid), expected_c2);

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── test_fk_referential_integrity ────────────────────────────────────

#[test]
fn test_fk_referential_integrity() {
    let dir = temp_dir("fk_integrity");
    let mut engine = CatalogEngine::open(&dir).unwrap();

    // Parent table
    let parent_tid = engine.create_table("public.parents", &[u64_col_def("pid")], 0, true).unwrap();

    // Child table with FK
    let child_cols = vec![
        u64_col_def("cid"),
        ColumnDef { name: "pid_fk".into(), type_code: type_code::U64, is_nullable: false,
                    fk_table_id: parent_tid, fk_col_idx: 0 },
    ];
    let child_tid = engine.create_table("public.children", &child_cols, 0, true).unwrap();

    // Insert valid parent
    let parent_schema = engine.get_schema(parent_tid).unwrap();
    let mut pbb = BatchBuilder::new(parent_schema);
    pbb.begin_row(10, 0, 1);
    pbb.end_row();
    let pbatch = pbb.finish();
    engine.dag.ingest_to_family(parent_tid, pbatch);
    let _ = engine.dag.flush(parent_tid);

    // Insert valid child (FK=10 exists in parent)
    let child_schema = engine.get_schema(child_tid).unwrap();
    let mut cbb = BatchBuilder::new(child_schema);
    cbb.begin_row(1, 0, 1);
    cbb.put_u64(10); // valid FK
    cbb.end_row();
    let cbatch = cbb.finish();
    assert!(engine.validate_fk_inline(child_tid, &cbatch).is_ok());

    // Insert invalid child (FK=99, not in parent)
    let mut cbb2 = BatchBuilder::new(child_schema);
    cbb2.begin_row(2, 0, 1);
    cbb2.put_u64(99); // invalid FK
    cbb2.end_row();
    let cbatch2 = cbb2.finish();
    assert!(engine.validate_fk_inline(child_tid, &cbatch2).is_err());

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── test_fk_nullability_and_retractions ──────────────────────────────

#[test]
fn test_fk_nullability_and_retractions() {
    let dir = temp_dir("fk_null");
    let mut engine = CatalogEngine::open(&dir).unwrap();

    let parent_tid = engine.create_table("public.p", &[u64_col_def("id")], 0, true).unwrap();
    let child_cols = vec![
        u64_col_def("id"),
        ColumnDef { name: "pid_fk".into(), type_code: type_code::U64, is_nullable: true,
                    fk_table_id: parent_tid, fk_col_idx: 0 },
    ];
    let child_tid = engine.create_table("public.c", &child_cols, 0, true).unwrap();

    // NULL FK should be allowed even if parent is empty
    let child_schema = engine.get_schema(child_tid).unwrap();
    let mut bb = BatchBuilder::new(child_schema);
    bb.begin_row(1, 0, 1);
    bb.put_null();
    bb.end_row();
    let batch = bb.finish();
    assert!(engine.validate_fk_inline(child_tid, &batch).is_ok());

    // Retraction (weight -1) should not trigger FK check
    let mut bb2 = BatchBuilder::new(child_schema);
    bb2.begin_row(2, 0, -1);
    bb2.put_u64(999); // non-existent, but weight is negative
    bb2.end_row();
    let batch2 = bb2.finish();
    assert!(engine.validate_fk_inline(child_tid, &batch2).is_ok());

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── test_fk_protections ──────────────────────────────────────────────

#[test]
fn test_fk_drop_protections() {
    let dir = temp_dir("fk_prot");
    let mut engine = CatalogEngine::open(&dir).unwrap();

    let parent_tid = engine.create_table("public.parent", &[u64_col_def("pid")], 0, true).unwrap();
    let child_cols = vec![
        u64_col_def("cid"),
        ColumnDef { name: "pid_fk".into(), type_code: type_code::U64, is_nullable: false,
                    fk_table_id: parent_tid, fk_col_idx: 0 },
    ];
    engine.create_table("public.child", &child_cols, 0, true).unwrap();

    // Cannot drop parent (referenced by child)
    assert!(engine.drop_table("public.parent").is_err());

    // Cannot drop auto-generated FK index
    let idx_name = make_fk_index_name("public", "child", "pid_fk");
    assert!(engine.drop_index(&idx_name).is_err());

    // Drop child first, then parent succeeds
    engine.drop_table("public.child").unwrap();
    engine.drop_table("public.parent").unwrap();

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── test_fk_invalid_targets ──────────────────────────────────────────

#[test]
fn test_fk_invalid_targets() {
    let dir = temp_dir("fk_invalid");
    let mut engine = CatalogEngine::open(&dir).unwrap();

    let parent_tid = engine.create_table("public.p",
        &[u64_col_def("pk"), i64_col_def("other")], 0, true).unwrap();

    // FK targeting non-PK column (col_idx=1) should fail
    let bad_cols = vec![
        u64_col_def("pk"),
        ColumnDef { name: "fk".into(), type_code: type_code::I64, is_nullable: false,
                    fk_table_id: parent_tid, fk_col_idx: 1 },
    ];
    assert!(engine.create_table("public.c_bad", &bad_cols, 0, true).is_err());

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── test_fk_self_reference ───────────────────────────────────────────

#[test]
fn test_fk_self_reference() {
    let dir = temp_dir("fk_self");
    let mut engine = CatalogEngine::open(&dir).unwrap();

    // Self-referential table: employees.mgr_id -> employees.emp_id
    let next_tid = engine.next_table_id;
    let emp_cols = vec![
        u64_col_def("emp_id"),
        ColumnDef { name: "mgr_id".into(), type_code: type_code::U64, is_nullable: true,
                    fk_table_id: next_tid, fk_col_idx: 0 },
    ];
    let emp_tid = engine.create_table("public.employees", &emp_cols, 0, true).unwrap();
    assert_eq!(emp_tid, next_tid);

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── test_fk_parent_restrict_blocks_delete ────────────────────────────

#[test]
fn test_fk_parent_restrict_blocks_delete() {
    let dir = temp_dir("fk_restrict");
    let mut engine = CatalogEngine::open(&dir).unwrap();

    let parent_tid = engine.create_table("public.parent", &[u64_col_def("pid")], 0, true).unwrap();
    let child_cols = vec![
        u64_col_def("cid"),
        ColumnDef { name: "fk".into(), type_code: type_code::U64, is_nullable: false,
                    fk_table_id: parent_tid, fk_col_idx: 0 },
    ];
    let child_tid = engine.create_table("public.child", &child_cols, 0, true).unwrap();

    // Insert parent PK=10
    let ps = engine.get_schema(parent_tid).unwrap();
    let mut pbb = BatchBuilder::new(ps);
    pbb.begin_row(10, 0, 1);
    pbb.end_row();
    engine.dag.ingest_to_family(parent_tid, pbb.finish());
    let _ = engine.dag.flush(parent_tid);

    // Insert child FK=10
    let cs = engine.get_schema(child_tid).unwrap();
    let mut cbb = BatchBuilder::new(cs);
    cbb.begin_row(1, 0, 1);
    cbb.put_u64(10);
    cbb.end_row();
    let cbatch = cbb.finish();
    assert!(engine.validate_fk_inline(child_tid, &cbatch).is_ok());
    engine.dag.ingest_to_family(child_tid, cbatch);
    let _ = engine.dag.flush(child_tid);

    // Retract parent PK=10 — should be blocked by RESTRICT
    let mut del = BatchBuilder::new(ps);
    del.begin_row(10, 0, -1);
    del.end_row();
    let del_batch = del.finish();
    let result = engine.validate_fk_parent_restrict(parent_tid, &del_batch);
    assert!(result.is_err(), "DELETE parent with existing child should fail");
    assert!(result.unwrap_err().contains("Foreign Key violation"));

    // Retract parent PK=99 (no child references it) — should succeed
    let mut del2 = BatchBuilder::new(ps);
    del2.begin_row(99, 0, -1);
    del2.end_row();
    assert!(engine.validate_fk_parent_restrict(parent_tid, &del2.finish()).is_ok());

    // Insert-only batch on parent table — RESTRICT should be a no-op
    let mut ins = BatchBuilder::new(ps);
    ins.begin_row(20, 0, 1);
    ins.end_row();
    assert!(engine.validate_fk_parent_restrict(parent_tid, &ins.finish()).is_ok());

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── test_fk_parent_map_cleanup ──────────────────────────────────────

#[test]
fn test_fk_parent_map_cleanup() {
    let dir = temp_dir("fk_pmap");
    let mut engine = CatalogEngine::open(&dir).unwrap();

    let parent_tid = engine.create_table("public.parent", &[u64_col_def("pid")], 0, true).unwrap();
    let child_cols = vec![
        u64_col_def("cid"),
        ColumnDef { name: "fk".into(), type_code: type_code::U64, is_nullable: false,
                    fk_table_id: parent_tid, fk_col_idx: 0 },
    ];
    engine.create_table("public.child", &child_cols, 0, true).unwrap();

    // Verify parent_map populated
    assert!(!engine.fk_children_of(parent_tid).is_empty());

    // Drop child — parent_map should be cleaned up
    engine.drop_table("public.child").unwrap();
    assert!(engine.fk_children_of(parent_tid).is_empty(),
            "fk_parent_map not cleaned up after child drop");

    // Drop parent should now succeed
    engine.drop_table("public.parent").unwrap();

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── test_fk_multiple_children_same_parent ───────────────────────────

#[test]
fn test_fk_multiple_children_same_parent() {
    let dir = temp_dir("fk_multi_child");
    let mut engine = CatalogEngine::open(&dir).unwrap();

    let parent_tid = engine.create_table("public.parent", &[u64_col_def("pid")], 0, true).unwrap();

    let mk_child = |engine: &mut CatalogEngine, name: &str| -> i64 {
        let cols = vec![
            u64_col_def("cid"),
            ColumnDef { name: "fk".into(), type_code: type_code::U64, is_nullable: false,
                        fk_table_id: parent_tid, fk_col_idx: 0 },
        ];
        engine.create_table(name, &cols, 0, true).unwrap()
    };
    let child1_tid = mk_child(&mut engine, "public.child1");
    let child2_tid = mk_child(&mut engine, "public.child2");

    // Both children should reference the parent
    let children = engine.fk_children_of(parent_tid);
    assert_eq!(children.len(), 2);

    // Cannot drop parent while either child exists
    assert!(engine.drop_table("public.parent").is_err());

    // Insert parent + child1 row, verify RESTRICT
    let ps = engine.get_schema(parent_tid).unwrap();
    let mut pbb = BatchBuilder::new(ps);
    pbb.begin_row(10, 0, 1);
    pbb.end_row();
    engine.dag.ingest_to_family(parent_tid, pbb.finish());
    let _ = engine.dag.flush(parent_tid);

    let cs = engine.get_schema(child1_tid).unwrap();
    let mut cbb = BatchBuilder::new(cs);
    cbb.begin_row(1, 0, 1);
    cbb.put_u64(10);
    cbb.end_row();
    engine.dag.ingest_to_family(child1_tid, cbb.finish());
    let _ = engine.dag.flush(child1_tid);

    // DELETE parent blocked by child1
    let mut del = BatchBuilder::new(ps);
    del.begin_row(10, 0, -1);
    del.end_row();
    assert!(engine.validate_fk_parent_restrict(parent_tid, &del.finish()).is_err());

    // Drop child1 — still blocked by child2
    engine.drop_table("public.child1").unwrap();
    assert!(engine.drop_table("public.parent").is_err());

    // Drop child2 — now parent can be dropped
    engine.drop_table("public.child2").unwrap();
    assert!(engine.fk_children_of(parent_tid).is_empty());
    engine.drop_table("public.parent").unwrap();

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── test_fk_referential_integrity (U128 extension) ──────────────────

#[test]
fn test_fk_u128() {
    let dir = temp_dir("fk_u128");
    let mut engine = CatalogEngine::open(&dir).unwrap();

    // U128 parent
    let parent_tid = engine.create_table("public.uparents",
        &[u128_col_def("uuid")], 0, true).unwrap();

    // U128 FK child
    let child_cols = vec![
        u64_col_def("id"),
        ColumnDef { name: "ufk".into(), type_code: type_code::U128, is_nullable: false,
                    fk_table_id: parent_tid, fk_col_idx: 0 },
    ];
    let child_tid = engine.create_table("public.uchildren", &child_cols, 0, true).unwrap();

    // Insert parent with U128 PK (lo=0xBBBB, hi=0xAAAA)
    let parent_schema = engine.get_schema(parent_tid).unwrap();
    let mut pbb = BatchBuilder::new(parent_schema);
    pbb.begin_row(0xBBBB, 0xAAAA, 1);
    pbb.end_row();
    engine.dag.ingest_to_family(parent_tid, pbb.finish());
    let _ = engine.dag.flush(parent_tid);

    // Valid child FK (matches parent)
    let child_schema = engine.get_schema(child_tid).unwrap();
    let mut cbb = BatchBuilder::new(child_schema);
    cbb.begin_row(1, 0, 1);
    cbb.put_u128(0xBBBB, 0xAAAA);
    cbb.end_row();
    assert!(engine.validate_fk_inline(child_tid, &cbb.finish()).is_ok());

    // Invalid child FK (no such parent)
    let mut cbb2 = BatchBuilder::new(child_schema);
    cbb2.begin_row(2, 0, 1);
    cbb2.put_u128(0xDEAD, 0xBEEF);
    cbb2.end_row();
    assert!(engine.validate_fk_inline(child_tid, &cbb2.finish()).is_err());

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}
