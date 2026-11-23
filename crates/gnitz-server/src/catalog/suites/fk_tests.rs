use super::*;
use crate::test_support::u64_i64_cols;
use gnitz_wire::{FkAction, FkRef};

// ── test_write_lock_set ─────────────────────────────────────────────────

/// The lock set of a write to `tid` that `deletes` or does not.
fn lock_set_of(engine: &CatalogEngine, tid: u64, deletes: bool) -> Vec<u64> {
    engine.write_lock_set([(tid, deletes)]).to_vec()
}

/// A deleting write's lock set.
fn lock_set(engine: &CatalogEngine, tid: u64) -> Vec<u64> {
    lock_set_of(engine, tid, true)
}

#[test]
fn test_write_lock_set() {
    let (_dir, mut engine) = empty_catalog(1);

    // A base table with no FK yet: needs a lock for itself (its writes run
    // enforce_unique_pk against the store), but no peers.
    let parent_tid = fk_parent(&mut engine);
    assert_eq!(lock_set(&engine, parent_tid), vec![parent_tid]);

    // Add a child with FK to parent. Now both tables share a lock neighborhood.
    let child_tid = engine
        .create_table("public.child", &fk_child_cols(parent_tid), &[0])
        .unwrap();
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
    let child2_tid = engine
        .create_table("public.child2", &fk_child_cols(parent_tid), &[0])
        .unwrap();
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
}

/// A deleting write locks the children, restricting ones included, of every
/// table a chain of cascading edges reaches from it, none beyond a restricting
/// edge, and no parent of a table it only cascades to; a write that deletes
/// nothing locks no further than its own children. An edge carries its column's
/// action.
#[test]
fn a_write_locks_what_its_cascade_can_delete_from() {
    let (_dir, mut engine) = empty_catalog(1);
    let mut table = |name: &str, fks: &[(u64, FkAction)]| {
        let mut cols = vec![col_def("id", TypeCode::U64)];
        for (i, &(parent, on_delete)) in fks.iter().enumerate() {
            cols.push(fk_def_on_delete(&format!("r{i}"), TypeCode::U64, parent, 0, on_delete));
        }
        engine.create_table(&format!("public.{name}"), &cols, &[0]).unwrap()
    };
    use FkAction::{Cascade, Restrict};
    //   root ◄─cascade─ mid ◄─cascade─ leaf ◄─restrict─ guard ◄─cascade─ beyond
    //                    │                 └─cascade─► other (a second parent of leaf)
    //   root ◄─restrict─ held ◄─cascade─ under
    let root = table("root", &[]);
    let other = table("other", &[]);
    let mid = table("mid", &[(root, Cascade)]);
    let leaf = table("leaf", &[(mid, Cascade), (other, Cascade)]);
    let guard = table("guard", &[(leaf, Restrict)]);
    let beyond = table("beyond", &[(guard, Cascade)]);
    let held = table("held", &[(root, Restrict)]);
    let under = table("under", &[(held, Cascade)]);

    assert_eq!(lock_set(&engine, root), [root, mid, leaf, guard, held]);
    assert_eq!(lock_set(&engine, mid), [root, mid, leaf, guard]);
    assert_eq!(lock_set(&engine, leaf), [other, mid, leaf, guard]);
    assert_eq!(lock_set(&engine, other), [other, leaf, guard]);
    assert_eq!(lock_set(&engine, guard), [leaf, guard, beyond]);
    assert_eq!(lock_set(&engine, held), [root, held, under]);
    assert_eq!(lock_set_of(&engine, root, false), [root, mid, held]);
    assert_eq!(lock_set_of(&engine, mid, false), [root, mid, leaf]);
    // A transaction's set is the union of its families' sets.
    let union = engine.write_lock_set([(other, false), (held, false)]);
    assert_eq!(union[..], [root, other, leaf, held, under]);
    let _ = (beyond, under);

    let actions = |tid| -> Vec<FkAction> { engine.fks_as_child(tid).iter().map(|e| e.on_delete).collect() };
    assert_eq!(actions(leaf), [Cascade, Cascade]);
    assert_eq!(actions(guard), [Restrict]);
    let by_parent: Vec<_> = engine
        .fks_as_parent(root)
        .iter()
        .map(|e| (e.child_tid, e.on_delete))
        .collect();
    assert_eq!(by_parent, [(mid, Cascade), (held, Restrict)]);

    engine.close();
}

/// A self-referencing cascade ends the walk at the table it started from.
#[test]
fn a_self_cascading_table_locks_itself_once() {
    let (_dir, mut engine) = empty_catalog(1);
    let tid = engine.next_catalog_id();
    let cols = vec![
        col_def("id", TypeCode::U64),
        fk_def_on_delete("parent", TypeCode::U64, tid, 0, FkAction::Cascade),
    ];
    assert_eq!(engine.create_table("public.tree", &cols, &[0]).unwrap(), tid);
    assert_eq!(lock_set(&engine, tid), [tid]);
    engine.close();
}

// ── Dropping an FK child, alone or with its parent, leaves no edge ───────────

#[test]
fn dropped_fk_child_leaves_no_edge() {
    let (_dir, mut engine) = empty_catalog(1);
    let parent_tid = fk_parent(&mut engine);
    let child_tid = engine
        .create_table("public.child", &fk_child_cols(parent_tid), &[0])
        .unwrap();

    engine.drop_table("public.child").unwrap();

    assert!(engine.fks_as_child(child_tid).is_empty());
    assert!(engine.fks_as_parent(parent_tid).is_empty());
    assert!(
        !engine.caches.relations.contains_key(&child_tid),
        "a dropped child must keep no relation entry"
    );
    assert_eq!(lock_set(&engine, parent_tid), vec![parent_tid]);

    engine.close();
}

/// One TABLE_TAB batch dropping an FK parent and its child, in either row order:
/// parent-first unregisters the parent before the child's column rows retract,
/// child-first hands the precheck its dropped ids out of order.
#[test]
fn co_dropped_fk_parent_and_child_leave_no_edge() {
    for child_first in [false, true] {
        let (_dir, mut engine) = empty_catalog(1);
        let parent_tid = fk_parent(&mut engine);
        let child_tid = engine
            .create_table("public.child", &fk_child_cols(parent_tid), &[0])
            .unwrap();

        let order = if child_first {
            [child_tid, parent_tid]
        } else {
            [parent_tid, child_tid]
        };
        let mut drop = engine.retract_under(SysFamily::Table, &order[..1]);
        drop.append_batch(&engine.retract_under(SysFamily::Table, &order[1..]));
        apply_ddl(&mut engine, [(SysFamily::Table, drop)]).unwrap();

        assert!(engine.fks_as_child(child_tid).is_empty());
        assert!(engine.fks_as_parent(parent_tid).is_empty());
        for tid in [parent_tid, child_tid] {
            assert!(
                !engine.caches.relations.contains_key(&tid),
                "co-dropped table {tid} must keep no relation entry"
            );
        }
        engine.close();
    }
}

#[test]
fn creating_a_child_of_a_parent_the_same_delta_drops_is_refused() {
    let (_dir, mut engine) = empty_catalog(1);
    let parent_tid = fk_parent(&mut engine);
    let child_tid = engine.allocate_ids(1).unwrap();

    let mut batch = engine.retract_under(SysFamily::Table, &[parent_tid]);
    batch.append_batch(&table_tab_batch(&[(child_tid, "child", 1)]));
    let blocks = [
        (
            SysFamily::Column,
            col_tab_batch(child_tid, &fk_child_cols(parent_tid), 1),
        ),
        (SysFamily::Table, batch),
    ];
    let err = apply_ddl(&mut engine, blocks).expect_err("a child of a parent this delta drops must be refused");
    assert!(err.contains("which this transaction drops"), "{err}");
    assert!(engine.registry.relation(parent_tid).is_some());
    assert!(engine.registry.relation(child_tid).is_none());

    engine.close();
}

// ── test_fk_invalid_targets ──────────────────────────────────────────

#[test]
fn test_fk_invalid_targets() {
    let (_dir, mut engine) = empty_catalog(1);
    let u = TypeCode::U64;

    let parent_tid = engine.create_table("public.p", &u64_i64_cols(), &[0]).unwrap();
    let view_tid = register_identity_view(&mut engine, parent_tid, "pv", &u64_i64_cols());
    let stream_tid = create_stream(&mut engine, "public.ps", &[col_def("id", u)]);
    let cp_cols = [col_def("a", u), col_def("b", u), col_def("c", u)];
    let cp_tid = engine.create_table("public.cp", &cp_cols, &[0, 1]).unwrap();
    engine.create_index("public.cp", &["c"], true).unwrap();
    engine.create_index("public.cp", &["b", "c"], true).unwrap();
    let uq_tid = engine
        .create_table("public.uq", &[col_def("id", u), col_def("v", u)], &[0])
        .unwrap();
    engine.create_index("public.uq", &["v"], true).unwrap();
    let sys_tid = SysFamily::Table.id();

    let cases: [(u64, u32, TypeCode, &str); 8] = [
        (
            parent_tid,
            1,
            TypeCode::I64,
            "'fk' references public.p.v, which is neither its lone primary key nor \
             covered by a single-column UNIQUE index",
        ),
        (
            view_tid,
            0,
            u,
            "'fk' references public.pv, which is a view; a FOREIGN KEY must reference a base table",
        ),
        (stream_tid, 0, u, "'fk' references public.ps, which is a stream"),
        (sys_tid, 0, u, "which is a system table"),
        // A compound PK has no lone column; only a single-column UNIQUE index
        // makes a member a target.
        (cp_tid, 0, u, "references public.cp.a, which is neither"),
        // A unique `(b, c)` does not make `b` alone unique.
        (cp_tid, 1, u, "references public.cp.b, which is neither"),
        (cp_tid, 9, u, "references column 9 of public.cp, which has 3 columns"),
        (cp_tid, 2, u, ""),
    ];
    for (n, (tid, col, ty, want)) in cases.into_iter().enumerate() {
        let cols = vec![col_def("pk", u), fk_def("fk", ty, tid, col)];
        let got = engine.create_table(&format!("public.c{n}"), &cols, &[0]);
        match want {
            "" => assert!(got.is_ok(), "{tid}.{col}: {got:?}"),
            want => {
                let err = got.expect_err(want);
                assert!(err.contains(want), "{tid}.{col}: want {want:?}, got {err:?}");
            }
        }
    }
    let cols = vec![col_def("pk", u), fk_def("fk", u, uq_tid, 1)];
    engine.create_table("public.c_uq", &cols, &[0]).unwrap();

    engine.close();
}

/// The self-referencing target is the lone PK: the table holds no index yet, so
/// even a UNIQUE its own CREATE declares cannot count.
#[test]
fn a_self_fk_must_target_the_lone_pk() {
    let (_dir, mut engine) = empty_catalog(1);
    let u = TypeCode::U64;
    for (cols, pk, want) in [
        (
            vec![col_def("id", u), col_def("tag", u), fk_def("p", u, 0, 1)],
            &[0][..],
            "FK column 'p' references 'tag' of its own table; a self-referencing FK \
             must reference the table's lone primary key column",
        ),
        (
            vec![col_def("a", u), col_def("b", u), fk_def("p", u, 0, 0)],
            &[0, 1],
            "references 'a' of its own table",
        ),
        (
            vec![col_def("id", u), fk_def("p", u, 0, 7)],
            &[0],
            "FK column 'p' references column 7 of its own table, which has 2 columns",
        ),
        (vec![col_def("id", u), fk_def("p", u, 0, 0)], &[0], ""),
    ] {
        // The id the create takes.
        let tid = engine.next_catalog_id();
        let mut cols = cols;
        cols.iter_mut()
            .filter_map(|c| c.fk.as_mut())
            .for_each(|fk| fk.table_id = tid);
        let got = engine.create_table(&format!("public.t{tid}"), &cols, pk);
        match want {
            "" => assert!(got.is_ok(), "{got:?}"),
            want => {
                let err = got.expect_err(want);
                assert!(err.contains(want), "want {want:?}, got {err:?}");
            }
        }
    }
    engine.close();
}

/// A stream holds no rows for a referential action to check, so none of its
/// columns carries an FK; the refusal names the stream and the column.
#[test]
fn a_stream_carries_no_foreign_key() {
    let (_dir, mut engine) = empty_catalog(1);
    let parent_tid = engine.create_table("public.p", &u64_i64_cols(), &[0]).unwrap();
    let cols = [col_def("id", TypeCode::U64), fk_def("r", TypeCode::U64, parent_tid, 0)];
    let stream = gnitz_wire::TableProps { stream: true, ..Default::default() };
    let err = engine.create_table_with("public.s", &cols, &[0], stream).unwrap_err();
    assert!(
        err.contains("stream 's'") && err.contains("column 'r' may not carry a FOREIGN KEY"),
        "{err}"
    );
    engine.close();
}

// ── test_fk_child_type_must_equal_parent ─────────────────────────────

/// FK values are compared as the referenced column's key image, which is injective
/// at one type only, so a child column must carry exactly the parent's type.
#[test]
fn test_fk_child_type_must_equal_parent() {
    let (_dir, mut engine) = empty_catalog(1);

    let parent_tid = engine
        .create_table("public.p", &[col_def("id", TypeCode::I32)], &[0])
        .unwrap();

    // I64 child → I32 parent: the child's domain does not fit the parent's.
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

    // UUID is its own type: a UUID child under a U64 parent is refused, and so
    // is a U128 child under a UUID parent, though the two share a width.
    for (n, (parent_pk, child)) in [
        (col_def("id", TypeCode::U64), TypeCode::UUID),
        (uuid_def("id"), TypeCode::U128),
    ]
    .into_iter()
    .enumerate()
    {
        let ptid = engine
            .create_table(&format!("public.up{n}"), &[parent_pk], &[0])
            .unwrap();
        let child_cols = vec![col_def("cid", TypeCode::U64), fk_def("pid", child, ptid, 0)];
        let err = engine
            .create_table(&format!("public.uc{n}"), &child_cols, &[0])
            .expect_err("an FK child of another type must be refused");
        assert!(err.contains("FK type mismatch"), "{child:?} child: {err}");
    }

    engine.close();
}

/// A DECIMAL's scale is part of its type: the stored integers of two scales
/// never mean the same number.
#[test]
fn a_decimal_fk_child_must_carry_the_parents_scale() {
    let (_dir, mut engine) = empty_catalog(1);
    let decimal = |name: &str, scale: u8| CatalogColumn {
        def: gnitz_wire::ColumnDef::new(name, gnitz_wire::ColType::decimal(scale), false),
        fk: None,
    };
    let parent_tid = engine.create_table("public.p", &[decimal("id", 4)], &[0]).unwrap();
    let child = |scale: u8| {
        let fk = Some(FkRef {
            table_id: parent_tid,
            col: 0,
            on_delete: FkAction::Restrict,
        });
        vec![
            col_def("cid", TypeCode::U64),
            CatalogColumn { fk, ..decimal("pid", scale) },
        ]
    };

    let err = engine
        .create_table("public.c_scale2", &child(2), &[0])
        .expect_err("a child of another scale must be refused");
    assert!(err.contains("FK type mismatch"), "got: {err}");
    engine.create_table("public.c_scale4", &child(4), &[0]).unwrap();

    engine.close();
}

/// An I128 column is a key column like any other: an FK may reference one, and
/// the child's FK index keys on it.
#[test]
fn an_i128_column_is_an_fk_target() {
    let (_dir, mut engine) = empty_catalog(1);
    let parent_tid = engine
        .create_table("public.p", &[col_def("id", TypeCode::I128)], &[0])
        .unwrap();
    let child = vec![
        col_def("cid", TypeCode::U64),
        fk_def("pid", TypeCode::I128, parent_tid, 0),
    ];
    let child_tid = engine.create_table("public.c", &child, &[0]).unwrap();
    let child = engine.registry.relation(child_tid).unwrap();
    let index = child.index_on(&[1]).unwrap();
    assert_eq!(index.key_spec().index_schema(&child.schema()).pk_stride(), 16 + 8);

    engine.close();
}

// ── test_fk_self_reference ───────────────────────────────────────────

#[test]
fn test_fk_self_reference() {
    let (_dir, mut engine) = empty_catalog(1);

    // Self-referential table: employees.mgr_id -> employees.emp_id
    let next_tid = engine.next_catalog_id();
    let emp_cols = vec![
        col_def("emp_id", TypeCode::U64),
        CatalogColumn {
            fk: Some(FkRef {
                table_id: next_tid,
                col: 0,
                on_delete: FkAction::Restrict,
            }),
            ..nullable_def("mgr_id", TypeCode::U64)
        },
    ];
    let emp_tid = engine.create_table("public.employees", &emp_cols, &[0]).unwrap();
    assert_eq!(emp_tid, next_tid);

    // The constraint registers in BOTH FK caches with owner == target, so the
    // insert rule (child side) and the delete-restrict rule (parent side) both see it.
    let as_child = engine.fks_as_child(emp_tid);
    assert_eq!(as_child.len(), 1);
    assert_eq!(as_child[0].fk_col, 1);
    assert_eq!(as_child[0].parent_tid, emp_tid);
    assert_eq!(as_child[0].parent_col, 0);

    let as_parent = engine.fks_as_parent(emp_tid);
    assert_eq!(as_parent.len(), 1);
    assert_eq!(as_parent[0].child_tid, emp_tid);
    assert_eq!(as_parent[0].fk_col, 1);
    assert_eq!(as_parent[0].parent_col, 0);

    // Both endpoints are the same table, so the acquired set dedupes to one tid —
    // a writer takes exactly one guard, not the same guard twice.
    assert_eq!(lock_set(&engine, emp_tid), vec![emp_tid]);

    engine.close();
}

// ── test_push_reads_committed_state ──────────────────────────────────

#[test]
fn test_push_reads_committed_state() {
    use gnitz_wire::WireConflictMode::{Error, Update};

    let (_dir, mut engine) = empty_catalog(1);

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
    let parent_tid = fk_parent(&mut engine);
    let child_tid = engine
        .create_table("public.c", &fk_child_cols(parent_tid), &[0])
        .unwrap();
    assert!(engine.push_reads_committed_state(child_tid, Update));
    assert!(engine.push_reads_committed_state(parent_tid, Update));

    // Self-referential FK: its lock set dedupes to one tid (asserted in
    // `test_fk_self_reference`), so a predicate reading lock-set cardinality
    // would call this batchable. Both FK terms are non-zero, so it stays on the
    // exclusive guard.
    let next_tid = engine.next_catalog_id();
    let tree_cols = vec![
        col_def("id", TypeCode::U64),
        CatalogColumn {
            fk: Some(FkRef {
                table_id: next_tid,
                col: 0,
                on_delete: FkAction::Restrict,
            }),
            ..nullable_def("parent_id", TypeCode::U64)
        },
    ];
    let tree_tid = engine.create_table("public.tree", &tree_cols, &[0]).unwrap();
    assert!(engine.push_reads_committed_state(tree_tid, Update));

    // A stream is no conflict target and no FK parent.
    let sid = create_stream(&mut engine, "public.s", &u64_i64_cols());
    assert!(!engine.push_reads_committed_state(sid, Update));

    engine.close();
}

// ── test_fk_multiple_children_same_parent ───────────────────────────

#[test]
fn test_fk_multiple_children_same_parent() {
    let (_dir, mut engine) = empty_catalog(1);

    let parent_tid = fk_parent(&mut engine);
    let child1_tid = engine
        .create_table("public.child1", &fk_child_cols(parent_tid), &[0])
        .unwrap();
    engine
        .create_table("public.child2", &fk_child_cols(parent_tid), &[0])
        .unwrap();

    // Both children should reference the parent
    let children = engine.fks_as_parent(parent_tid);
    assert_eq!(children.len(), 2);

    // Cannot drop parent while either child exists, and the refusal leaves
    // the child's FK circuit in place.
    assert!(engine.drop_table("public.parent").is_err());
    assert_eq!(
        engine
            .registry
            .relation(child1_tid)
            .and_then(|e| e.index_on(&[1]))
            .map(|ix| ix.claims().to_vec()),
        Some(vec![IndexClaim::ForeignKey]),
    );

    // Drop child1 — still blocked by child2
    engine.drop_table("public.child1").unwrap();
    assert!(engine.drop_table("public.parent").is_err());

    // Drop child2 — now parent can be dropped
    engine.drop_table("public.child2").unwrap();
    assert!(engine.fks_as_parent(parent_tid).is_empty());
    engine.drop_table("public.parent").unwrap();

    engine.close();
}

// ── test_fk_auto_index_covers_pk_members ─────────────────────────────
// A parent's RESTRICT probe reads every child through its FK circuit, so a PK
// member carries one exactly as a plain column does.

#[test]
fn test_fk_auto_index_covers_pk_members() {
    let (_dir, mut engine) = empty_catalog(1);

    let parent_tid = fk_parent(&mut engine);
    let fk_col = |name: &str| fk_def(name, TypeCode::U64, parent_tid, 0);

    // PK = (a, pid_fk): the FK is PK column 1. `plain_fk` is not a PK column.
    let child_cols = vec![col_def("a", TypeCode::U64), fk_col("pid_fk"), fk_col("plain_fk")];
    let child_tid = engine.create_table("public.child", &child_cols, &[0, 1]).unwrap();

    // PK = (pid_lead, line): the FK leads the PK, so its index reads the PK.
    let lead_cols = vec![fk_col("pid_lead"), col_def("line", TypeCode::U64)];
    let lead_tid = engine.create_table("public.lead", &lead_cols, &[0, 1]).unwrap();

    for (tid, c, reads_pk) in [(child_tid, 1, false), (child_tid, 2, false), (lead_tid, 0, true)] {
        let ix = engine.registry.relation(tid).and_then(|r| r.index_on(&[c]));
        assert_eq!(
            ix.map(|ix| (ix.claims().to_vec(), ix.reads_owner_pk())),
            Some((vec![IndexClaim::ForeignKey], reads_pk)),
            "table {tid} col {c}"
        );
        let dir = engine
            .registry
            .child_dir(tid, ChildKind::Index(PkColList::from_slice(&[c])));
        assert_eq!(std::path::Path::new(&dir).exists(), !reads_pk, "table {tid} col {c}");
    }

    engine.close();
}
