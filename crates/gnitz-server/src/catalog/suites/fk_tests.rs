use super::*;

// ── test_fk_lock_set ─────────────────────────────────────────────────

/// `tid`'s lock set as `lock_tables_exclusive` acquires it: sorted and deduped.
fn lock_set(engine: &CatalogEngine, tid: u64) -> Vec<u64> {
    let mut set: Vec<u64> = engine.fk_lock_set(tid).collect();
    set.sort_unstable();
    set.dedup();
    set
}

#[test]
fn test_fk_lock_set() {
    let dir = temp_dir("fk_lock_set");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();

    // A base table with no FK yet: needs a lock for itself (its writes run
    // enforce_unique_pk against the store), but no peers.
    let parent_tid = engine
        .create_table("public.parent", &[col_def("pid", TypeCode::U64)], &[0])
        .unwrap();
    assert_eq!(lock_set(&engine, parent_tid), vec![parent_tid]);

    // Add a child with FK to parent. Now both tables share a lock neighborhood.
    let child_cols = vec![
        col_def("cid", TypeCode::U64),
        fk_def("fk", TypeCode::U64, parent_tid, 0),
    ];
    let child_tid = engine.create_table("public.child", &child_cols, &[0]).unwrap();
    let mut expected = vec![parent_tid, child_tid];
    expected.sort_unstable();
    assert_eq!(
        lock_set(&engine, child_tid),
        expected,
        "child sees parent in its lock set"
    );
    assert_eq!(
        lock_set(&engine, parent_tid),
        expected,
        "parent sees child in its lock set"
    );

    // Second child: parent's neighborhood grows; each child only sees itself + parent.
    let child2_cols = vec![
        col_def("cid", TypeCode::U64),
        fk_def("fk", TypeCode::U64, parent_tid, 0),
    ];
    let child2_tid = engine.create_table("public.child2", &child2_cols, &[0]).unwrap();
    let mut expected3 = vec![parent_tid, child_tid, child2_tid];
    expected3.sort_unstable();
    assert_eq!(lock_set(&engine, parent_tid), expected3);
    let mut expected_c2 = vec![parent_tid, child2_tid];
    expected_c2.sort_unstable();
    assert_eq!(lock_set(&engine, child2_tid), expected_c2);

    // Drop child: parent's neighborhood shrinks back to itself + child2 only.
    engine.drop_table("public.child").unwrap();
    assert_eq!(lock_set(&engine, parent_tid), expected_c2);

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── test_fk_protections ──────────────────────────────────────────────

#[test]
fn test_fk_drop_protections() {
    let dir = temp_dir("fk_prot");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();

    let parent_tid = engine
        .create_table("public.parent", &[col_def("pid", TypeCode::U64)], &[0])
        .unwrap();
    let child_cols = vec![
        col_def("cid", TypeCode::U64),
        fk_def("pid_fk", TypeCode::U64, parent_tid, 0),
    ];
    let child_tid = engine.create_table("public.child", &child_cols, &[0]).unwrap();

    // The FK column carries a derived circuit, claimed under its column index.
    let fk_circuit = |engine: &CatalogEngine| {
        engine
            .registry
            .relation(child_tid)
            .and_then(|e| e.index_on(&[1]))
            .map(|ix| ix.claims().to_vec())
    };
    assert_eq!(fk_circuit(&engine), Some(vec![IndexClaim::ForeignKey]));

    // Cannot drop parent (referenced by child), and the refusal leaves the
    // child's circuit in place.
    assert!(engine.drop_table("public.parent").is_err());
    assert_eq!(fk_circuit(&engine), Some(vec![IndexClaim::ForeignKey]));

    // Drop child first, then parent succeeds
    engine.drop_table("public.child").unwrap();
    engine.drop_table("public.parent").unwrap();

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── Dropping an FK child, alone or with its parent, leaves no edge ───────────

#[test]
fn dropped_fk_child_leaves_no_edge() {
    let dir = temp_dir("fk_child_drop_lock");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
    let parent_tid = engine
        .create_table("public.parent", &[col_def("pid", TypeCode::U64)], &[0])
        .unwrap();
    let child_cols = vec![
        col_def("cid", TypeCode::U64),
        fk_def("pid_fk", TypeCode::U64, parent_tid, 0),
    ];
    let child_tid = engine.create_table("public.child", &child_cols, &[0]).unwrap();

    engine.drop_table("public.child").unwrap();

    assert!(engine.fk_constraints_of(child_tid).is_empty());
    assert!(engine.fk_children_of(parent_tid).is_empty());
    assert!(
        !engine.caches.relations.contains_key(&child_tid),
        "a dropped child must keep no relation entry"
    );
    assert_eq!(lock_set(&engine, parent_tid), vec![parent_tid]);

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn co_dropped_fk_parent_and_child_leave_no_edge() {
    let dir = temp_dir("fk_co_drop_lock");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
    let parent_tid = engine
        .create_table("public.parent", &[col_def("pid", TypeCode::U64)], &[0])
        .unwrap();
    let child_cols = vec![
        col_def("cid", TypeCode::U64),
        fk_def("pid_fk", TypeCode::U64, parent_tid, 0),
    ];
    let child_tid = engine.create_table("public.child", &child_cols, &[0]).unwrap();

    // One TABLE_TAB batch: the parent unregisters before the child's column rows
    // retract.
    let drop = engine.retract_under(SysFamily::Table, &[parent_tid, child_tid]);
    engine.submit(SysFamily::Table, drop).unwrap();

    assert!(engine.fk_constraints_of(child_tid).is_empty());
    assert!(engine.fk_children_of(parent_tid).is_empty());
    for tid in [parent_tid, child_tid] {
        assert!(
            !engine.caches.relations.contains_key(&tid),
            "co-dropped table {tid} must keep no relation entry"
        );
    }
    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn creating_a_child_of_a_parent_the_same_delta_drops_is_refused() {
    let dir = temp_dir("fk_child_of_dropped_parent");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
    let parent_tid = engine
        .create_table("public.parent", &[col_def("pid", TypeCode::U64)], &[0])
        .unwrap();
    let child_tid = engine.allocate_ids(1).unwrap();
    engine
        .write_column_records(
            child_tid,
            &[
                col_def("cid", TypeCode::U64),
                fk_def("pid_fk", TypeCode::U64, parent_tid, 0),
            ],
        )
        .unwrap();

    let mut batch = engine.retract_under(SysFamily::Table, &[parent_tid]);
    batch.append_batch(&build_table_tab_row(child_tid, pack_pk_cols(&[0]), "child"));
    let err = engine
        .submit(SysFamily::Table, batch)
        .expect_err("a child of a parent this delta drops must be refused");
    assert!(err.contains("which this transaction drops"), "{err}");
    assert!(engine.registry.has_id(parent_tid));
    assert!(!engine.registry.has_id(child_tid));

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── test_fk_invalid_targets ──────────────────────────────────────────

#[test]
fn test_fk_invalid_targets() {
    let dir = temp_dir("fk_invalid");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();

    let parent_tid = engine
        .create_table(
            "public.p",
            &[col_def("pk", TypeCode::U64), col_def("other", TypeCode::I64)],
            &[0],
        )
        .unwrap();

    // FK targeting non-PK column (col_idx=1) should fail
    let bad_cols = vec![col_def("pk", TypeCode::U64), fk_def("fk", TypeCode::I64, parent_tid, 1)];
    assert!(engine.create_table("public.c_bad", &bad_cols, &[0]).is_err());

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── test_fk_child_type_must_equal_parent ─────────────────────────────

/// FK values are compared as the referenced column's key image, which is injective
/// at one type only, so a child column must carry exactly the parent's type.
#[test]
fn test_fk_child_type_must_equal_parent() {
    let dir = temp_dir("fk_narrowing");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();

    let parent_tid = engine
        .create_table("public.p", &[col_def("id", TypeCode::I32)], &[0])
        .unwrap();

    // I64 child → I32 parent: `index_key_type` maps both to I64, but the child's
    // domain does not fit the parent's.
    let narrowing = vec![
        col_def("cid", TypeCode::I64),
        fk_def("pid", TypeCode::I64, parent_tid, 0),
    ];
    let err = engine
        .create_table("public.c_narrow", &narrowing, &[0])
        .expect_err("a narrowing FK child must be refused");
    assert!(err.contains("FK type mismatch"), "got: {err}");

    // A narrower child: SQL rewrites it to the parent's type, so the engine
    // refuses it too.
    let widening = vec![
        col_def("cid", TypeCode::I64),
        fk_def("pid", TypeCode::I16, parent_tid, 0),
    ];
    let err = engine
        .create_table("public.c_wide", &widening, &[0])
        .expect_err("a narrower FK child must be refused");
    assert!(err.contains("FK type mismatch"), "got: {err}");

    let same = vec![
        col_def("cid", TypeCode::I64),
        fk_def("pid", TypeCode::I32, parent_tid, 0),
    ];
    engine.create_table("public.c_same", &same, &[0]).unwrap();

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── test_fk_self_reference ───────────────────────────────────────────

#[test]
fn test_fk_self_reference() {
    let dir = temp_dir("fk_self");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();

    // Self-referential table: employees.mgr_id -> employees.emp_id
    let next_tid = engine.next_id;
    let emp_cols = vec![
        col_def("emp_id", TypeCode::U64),
        CatalogColumn {
            fk_table_id: next_tid,
            fk_col_idx: 0,
            ..nullable_def("mgr_id", TypeCode::U64)
        },
    ];
    let emp_tid = engine.create_table("public.employees", &emp_cols, &[0]).unwrap();
    assert_eq!(emp_tid, next_tid);

    // The constraint registers in BOTH FK caches with owner == target, so every
    // reader that walks either one sees it: the insert rule reaches it through
    // `fk_constraints_of`, the delete-restrict rule through `fk_children_of`.
    let as_child = engine.fk_constraints_of(emp_tid);
    assert_eq!(as_child.len(), 1);
    assert_eq!(as_child[0].fk_col, 1);
    assert_eq!(as_child[0].parent_tid, emp_tid);
    assert_eq!(as_child[0].parent_col, 0);

    let as_parent = engine.fk_children_of(emp_tid);
    assert_eq!(as_parent.len(), 1);
    assert_eq!(as_parent[0].child_tid, emp_tid);
    assert_eq!(as_parent[0].fk_col, 1);
    assert_eq!(as_parent[0].parent_col, 0);

    // Both endpoints are the same table, so the acquired set dedupes to one tid —
    // a writer takes exactly one guard, not the same guard twice.
    assert_eq!(lock_set(&engine, emp_tid), vec![emp_tid]);

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── test_push_reads_committed_state ──────────────────────────────────

#[test]
fn test_push_reads_committed_state() {
    use gnitz_wire::WireConflictMode::{Error, Update};

    let dir = temp_dir("push_reads_state");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();

    // Plain unconstrained base table: an upsert validates nothing, so it may
    // hold its table lock shared. The same table under Error mode must not —
    // duplicate-key rejection is a master-side pre-flight against the store.
    let plain_tid = engine
        .create_table(
            "public.plain",
            &[col_def("pid", TypeCode::U64), col_def("val", TypeCode::U64)],
            &[0],
        )
        .unwrap();
    assert!(!engine.push_reads_committed_state(plain_tid, Update));
    assert!(engine.push_reads_committed_state(plain_tid, Error));

    // A unique secondary index is validated against committed rows.
    let uniq_tid = engine
        .create_table(
            "public.uniq",
            &[col_def("pid", TypeCode::U64), col_def("val", TypeCode::U64)],
            &[0],
        )
        .unwrap();
    assert!(!engine.push_reads_committed_state(uniq_tid, Update));
    engine.create_index("public.uniq", &["val"], true).unwrap();
    assert!(engine.push_reads_committed_state(uniq_tid, Update));

    // A non-unique index does not validate anything.
    let idx_tid = engine
        .create_table(
            "public.plain_idx",
            &[col_def("pid", TypeCode::U64), col_def("val", TypeCode::U64)],
            &[0],
        )
        .unwrap();
    engine.create_index("public.plain_idx", &["val"], false).unwrap();
    assert!(!engine.push_reads_committed_state(idx_tid, Update));

    // Both sides of a two-table FK: the child probes the parent for its FK
    // target, the parent probes its children for restrict-on-delete.
    let parent_tid = engine
        .create_table("public.p", &[col_def("pid", TypeCode::U64)], &[0])
        .unwrap();
    let child_cols = vec![
        col_def("cid", TypeCode::U64),
        fk_def("fk", TypeCode::U64, parent_tid, 0),
    ];
    let child_tid = engine.create_table("public.c", &child_cols, &[0]).unwrap();
    assert!(engine.push_reads_committed_state(child_tid, Update));
    assert!(engine.push_reads_committed_state(parent_tid, Update));

    // Self-referential FK: its lock set dedupes to one tid (asserted in
    // `test_fk_self_reference`), so a predicate reading lock-set cardinality
    // would call this batchable. Both FK terms are non-zero, so it stays on the
    // exclusive guard.
    let next_tid = engine.next_id;
    let tree_cols = vec![
        col_def("id", TypeCode::U64),
        CatalogColumn {
            fk_table_id: next_tid,
            fk_col_idx: 0,
            ..nullable_def("parent_id", TypeCode::U64)
        },
    ];
    let tree_tid = engine.create_table("public.tree", &tree_cols, &[0]).unwrap();
    assert!(engine.push_reads_committed_state(tree_tid, Update));

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// (test_fk_parent_restrict_blocks_delete removed: it existed only to exercise
// a deleted test-only inline parent-restrict check. FK
// RESTRICT-on-DELETE is covered end-to-end by gnitz-py/tests/admissibility/test_fk.py.)

// ── test_fk_multiple_children_same_parent ───────────────────────────

#[test]
fn test_fk_multiple_children_same_parent() {
    let dir = temp_dir("fk_multi_child");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();

    let parent_tid = engine
        .create_table("public.parent", &[col_def("pid", TypeCode::U64)], &[0])
        .unwrap();

    let mk_child = |engine: &mut CatalogEngine, name: &str| -> u64 {
        let cols = vec![
            col_def("cid", TypeCode::U64),
            fk_def("fk", TypeCode::U64, parent_tid, 0),
        ];
        engine.create_table(name, &cols, &[0]).unwrap()
    };
    let _child1_tid = mk_child(&mut engine, "public.child1");
    let _child2_tid = mk_child(&mut engine, "public.child2");

    // Both children should reference the parent
    let children = engine.fk_children_of(parent_tid);
    assert_eq!(children.len(), 2);

    // Cannot drop parent while either child exists
    assert!(engine.drop_table("public.parent").is_err());

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

// ── test_fk_auto_index_covers_pk_members ─────────────────────────────
// A parent's RESTRICT probe reads every child through its FK circuit, so a PK
// member carries one exactly as a plain column does.

#[test]
fn test_fk_auto_index_covers_pk_members() {
    let dir = temp_dir("fk_compound_pk_member");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();

    let parent_tid = engine
        .create_table("public.parent", &[col_def("pid", TypeCode::U64)], &[0])
        .unwrap();
    let fk_col = |name: &str| fk_def(name, TypeCode::U64, parent_tid, 0);

    // PK = (a, pid_fk): the FK is PK column 1. `plain_fk` is not a PK column.
    let child_cols = vec![col_def("a", TypeCode::U64), fk_col("pid_fk"), fk_col("plain_fk")];
    let child_tid = engine.create_table("public.child", &child_cols, &[0, 1]).unwrap();

    let child = engine.registry.relation(child_tid).unwrap();
    for c in [1, 2] {
        assert_eq!(
            child.index_on(&[c]).map(|ix| ix.claims().to_vec()),
            Some(vec![IndexClaim::ForeignKey]),
            "col {c}"
        );
    }

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}
