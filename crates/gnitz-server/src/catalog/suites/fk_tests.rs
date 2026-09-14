use super::*;

// ── test_fk_lock_set ─────────────────────────────────────────────────

#[test]
fn test_fk_lock_set() {
    let dir = temp_dir("fk_lock_set");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();

    // A base table with no FK yet: needs a lock for itself (its writes run
    // enforce_unique_pk against the store), but no peers.
    let parent_tid = engine
        .create_table("public.parent", &[col_def("pid", type_code::U64)], &[0])
        .unwrap();
    assert_eq!(engine.fk_lock_set(parent_tid), vec![parent_tid]);

    // Add a child with FK to parent. Now both tables share a lock neighborhood,
    // returned sorted ascending so both use the same acquisition order.
    let child_cols = vec![
        col_def("cid", type_code::U64),
        fk_def("fk", type_code::U64, parent_tid, 0),
    ];
    let child_tid = engine.create_table("public.child", &child_cols, &[0]).unwrap();
    let mut expected = vec![parent_tid, child_tid];
    expected.sort_unstable();
    assert_eq!(
        engine.fk_lock_set(child_tid),
        expected,
        "child sees parent in its lock set"
    );
    assert_eq!(
        engine.fk_lock_set(parent_tid),
        expected,
        "parent sees child in its lock set"
    );

    // Second child: parent's neighborhood grows; each child only sees itself + parent.
    let child2_cols = vec![
        col_def("cid", type_code::U64),
        fk_def("fk", type_code::U64, parent_tid, 0),
    ];
    let child2_tid = engine.create_table("public.child2", &child2_cols, &[0]).unwrap();
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

// ── test_fk_protections ──────────────────────────────────────────────

#[test]
fn test_fk_drop_protections() {
    let dir = temp_dir("fk_prot");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();

    let parent_tid = engine
        .create_table("public.parent", &[col_def("pid", type_code::U64)], &[0])
        .unwrap();
    let child_cols = vec![
        col_def("cid", type_code::U64),
        fk_def("pid_fk", type_code::U64, parent_tid, 0),
    ];
    let child_tid = engine.create_table("public.child", &child_cols, &[0]).unwrap();

    // The FK column carries a derived circuit, id = its column index.
    let fk_circuit = |engine: &CatalogEngine| {
        engine
            .registry()
            .relation(child_tid)
            .and_then(|e| e.index_on(&[1]))
            .map(SecondaryIndex::id)
    };
    assert_eq!(fk_circuit(&engine), Some(1));

    // Cannot drop parent (referenced by child), and the refusal leaves the
    // child's circuit in place.
    assert!(engine.drop_table("public.parent").is_err());
    assert_eq!(fk_circuit(&engine), Some(1));

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
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();

    let parent_tid = engine
        .create_table(
            "public.p",
            &[col_def("pk", type_code::U64), col_def("other", type_code::I64)],
            &[0],
        )
        .unwrap();

    // FK targeting non-PK column (col_idx=1) should fail
    let bad_cols = vec![
        col_def("pk", type_code::U64),
        fk_def("fk", type_code::I64, parent_tid, 1),
    ];
    assert!(engine.create_table("public.c_bad", &bad_cols, &[0]).is_err());

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── test_fk_narrowing_child_rejected ─────────────────────────────────

/// I64 and I32 promote to the same index-key code, so a promoted-equality gate
/// admitted the pair and the lone-PK probe then wrote 8 bytes into a 4-byte slot.
#[test]
fn test_fk_narrowing_child_rejected() {
    let dir = temp_dir("fk_narrowing");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();

    let parent_tid = engine
        .create_table("public.p", &[col_def("id", type_code::I32)], &[0])
        .unwrap();

    // I64 child → I32 parent: `index_key_type` maps both to I64, but the child's
    // domain does not fit the parent's.
    let narrowing = vec![
        col_def("cid", type_code::I64),
        fk_def("pid", type_code::I64, parent_tid, 0),
    ];
    let err = engine
        .create_table("public.c_narrow", &narrowing, &[0])
        .expect_err("a narrowing FK child must be refused");
    assert!(err.contains("FK type mismatch"), "got: {err}");

    // The widening direction the encoder handles correctly stays admitted.
    let widening = vec![
        col_def("cid", type_code::I64),
        fk_def("pid", type_code::I16, parent_tid, 0),
    ];
    engine.create_table("public.c_wide", &widening, &[0]).unwrap();

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── test_fk_self_reference ───────────────────────────────────────────

#[test]
fn test_fk_self_reference() {
    let dir = temp_dir("fk_self");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();

    // Self-referential table: employees.mgr_id -> employees.emp_id
    let next_tid = engine.next_table_id;
    let emp_cols = vec![
        col_def("emp_id", type_code::U64),
        ColumnDef {
            is_nullable: true,
            ..fk_def("mgr_id", type_code::U64, next_tid, 0)
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

    // Both endpoints are the same table, so the lock set dedupes to one tid —
    // a writer takes exactly one guard, not the same guard twice.
    assert_eq!(engine.fk_lock_set(emp_tid), vec![emp_tid]);

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
            &[col_def("pid", type_code::U64), col_def("val", type_code::U64)],
            &[0],
        )
        .unwrap();
    assert!(!engine.push_reads_committed_state(plain_tid, Update));
    assert!(engine.push_reads_committed_state(plain_tid, Error));

    // A unique secondary index is validated against committed rows.
    let uniq_tid = engine
        .create_table(
            "public.uniq",
            &[col_def("pid", type_code::U64), col_def("val", type_code::U64)],
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
            &[col_def("pid", type_code::U64), col_def("val", type_code::U64)],
            &[0],
        )
        .unwrap();
    engine.create_index("public.plain_idx", &["val"], false).unwrap();
    assert!(!engine.push_reads_committed_state(idx_tid, Update));

    // Both sides of a two-table FK: the child probes the parent for its FK
    // target, the parent probes its children for restrict-on-delete.
    let parent_tid = engine
        .create_table("public.p", &[col_def("pid", type_code::U64)], &[0])
        .unwrap();
    let child_cols = vec![
        col_def("cid", type_code::U64),
        fk_def("fk", type_code::U64, parent_tid, 0),
    ];
    let child_tid = engine.create_table("public.c", &child_cols, &[0]).unwrap();
    assert!(engine.push_reads_committed_state(child_tid, Update));
    assert!(engine.push_reads_committed_state(parent_tid, Update));

    // Self-referential FK: its lock set dedupes to one tid (asserted in
    // `test_fk_self_reference`), so a predicate reading lock-set cardinality
    // would call this batchable. Both FK terms are non-zero, so it stays on the
    // exclusive guard.
    let next_tid = engine.next_table_id;
    let tree_cols = vec![
        col_def("id", type_code::U64),
        ColumnDef {
            is_nullable: true,
            ..fk_def("parent_id", type_code::U64, next_tid, 0)
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
        .create_table("public.parent", &[col_def("pid", type_code::U64)], &[0])
        .unwrap();

    let mk_child = |engine: &mut CatalogEngine, name: &str| -> i64 {
        let cols = vec![
            col_def("cid", type_code::U64),
            fk_def("fk", type_code::U64, parent_tid, 0),
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

// ── test_fk_auto_index_skips_non_leading_pk_column ───────────────────
// Every PK column is skipped, not just the leading one.

#[test]
fn test_fk_auto_index_skips_non_leading_pk_column() {
    let dir = temp_dir("fk_compound_pk_skip");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();

    let parent_tid = engine
        .create_table("public.parent", &[col_def("pid", type_code::U64)], &[0])
        .unwrap();
    let fk_col = |name: &str| fk_def(name, type_code::U64, parent_tid, 0);

    // PK = (a, pid_fk): the FK is PK column 1, not 0. `plain_fk` is not a PK column.
    let child_cols = vec![col_def("a", type_code::U64), fk_col("pid_fk"), fk_col("plain_fk")];
    let child_tid = engine.create_table("public.child", &child_cols, &[0, 1]).unwrap();

    let child = engine.registry().relation(child_tid).unwrap();
    assert!(
        child.index_on(&[1]).is_none(),
        "an FK at a non-leading PK position is covered by the PK region — no circuit",
    );
    assert!(
        child.index_on(&[2]).is_some(),
        "an FK on a non-PK column must still get its circuit",
    );

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}
