//! ALTER catalog mechanics driven directly at the `CatalogEngine` layer: the
//! reconciling register hooks (a rename pair fires no cascade), the id-only
//! directory resume across a reopen, and the column-ALTER guards. The
//! end-to-end SQL surface is in `crates/gnitz-sql/tests/engine/ddl.rs`.

use super::*;
use crate::test_support::u64_i64_cols;
use std::path::Path;

// ── Reconciling hooks: a rename fires no teardown ───────────────────────────

/// A rename pair renames the table in place in either row order: a raw
/// `DDL_TXN` block or an undo can hand the hooks the `+1` first. The names are
/// longer than 12 bytes, so they live in the blob heap and the `-1` carries heap
/// offsets of its own; the CAS compares string content, not region bytes.
#[test]
fn a_rename_pair_in_either_order_renames_in_place() {
    let (old, new) = ("original_long_name", "renamed_to_a_long_name");
    for insert_first in [false, true] {
        let (_dir, mut engine) = empty_catalog(1);
        let cols = vec![col_def("id", TypeCode::U64), col_def("v", TypeCode::U64)];
        let tid = engine.create_table(&format!("public.{old}"), &cols, &[0]).unwrap();
        let table_path = relation_dir(engine.registry.base_dir(), tid);
        assert!(Path::new(&table_path).exists());

        let mut pair = [(tid, old, -1), (tid, new, 1)];
        if insert_first {
            pair.reverse();
        }
        apply_ddl(&mut engine, [(SysFamily::Table, table_tab_batch(&pair))]).unwrap();

        assert!(
            engine.registry.relation(tid).is_some(),
            "insert_first={insert_first}: unregistered"
        );
        assert!(
            Path::new(&table_path).exists(),
            "insert_first={insert_first}: dir deleted"
        );
        assert_eq!(
            engine.get_by_name("public", new),
            Some(tid),
            "insert_first={insert_first}"
        );
        assert!(
            engine.get_by_name("public", old).is_none(),
            "insert_first={insert_first}"
        );
        assert_eq!(
            engine.qualified_name_or_unknown(tid).1,
            new,
            "insert_first={insert_first}"
        );
        assert_eq!(
            rows_under(&engine, SysFamily::Table, tid),
            1,
            "insert_first={insert_first}: a rename leaves exactly one live row"
        );

        engine.close();
    }
}

// ── Id-only dirs resume after a rename + reopen ─────────────────────────────

#[test]
fn rename_then_reopen_resolves_flushed_data() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().to_str().unwrap();
    let tid;
    {
        let mut engine = CatalogEngine::open(dir, 1).unwrap();
        let cols = vec![col_def("id", TypeCode::U64), col_def("v", TypeCode::U64)];
        tid = engine.create_table("public.orig", &cols, &[0]).unwrap();
        // Flush a row so only the on-disk (id-only) path can serve it after reopen.
        engine
            .registry
            .ingest(tid, rows(&engine, tid, 1, [7], |_| [70]))
            .unwrap();
        engine.registry.checkpoint_base().unwrap();

        let pair = table_tab_batch(&[(tid, "orig", -1), (tid, "renamed", 1)]);
        apply_ddl(&mut engine, [(SysFamily::Table, pair)]).unwrap();
        engine.close();
    }

    // Reopen: boot replay re-registers at the id-only path `{tid}` (unchanged by
    // the rename), so the flushed row still resolves under the new name.
    let mut engine = CatalogEngine::open(dir, 1).unwrap();
    assert_eq!(
        engine.get_by_name("public", "renamed"),
        Some(tid),
        "renamed relation must resolve after reopen"
    );
    assert!(
        !pk_group_native(&mut engine, tid, 7).is_empty(),
        "flushed row must resolve after rename + reopen (id-only dir was untouched)"
    );

    engine.close();
}

// ── The column-ALTER owner guard covers every rewrite pair ──────────────────

fn rename_to(new_name: &str) -> impl FnOnce(&mut CatalogColumn) {
    move |c: &mut CatalogColumn| c.def.name = new_name.into()
}

/// RENAME COLUMN on a view, which is no user base table; the SQL planner is not
/// the trust boundary that stops it.
#[test]
fn column_rename_on_non_base_owner_rejected() {
    let (_dir, mut engine) = empty_catalog(1);

    let cols = vec![col_def("id", TypeCode::U64)];
    let tid = engine.create_table("public.t", &cols, &[0]).unwrap();
    let vid = register_identity_view(&mut engine, tid, "v", &cols);

    let err = precheck(
        &engine,
        SysFamily::Column,
        &col_alter_pair(vid, 0, &cols[0], rename_to("id2")),
    )
    .unwrap_err();
    assert!(err.contains("not a user base table"), "view owner: {err}");

    engine.close();
}

/// The two guards a rename skips: a rename is legal on a PK column and on a
/// table with dependent views (views bind columns by ordinal).
#[test]
fn column_rename_on_pk_column_and_with_dependent_views_accepted() {
    let (_dir, mut engine) = empty_catalog(1);

    let cols = vec![col_def("id", TypeCode::U64), col_def("val", TypeCode::U64)];
    let tid = engine.create_table("public.t", &cols, &[0]).unwrap();
    let vid = register_identity_view(&mut engine, tid, "v", &cols);
    assert_eq!(
        engine.dag.dependents_of(tid),
        &[vid][..],
        "precondition: the table has a dependent view"
    );

    for (col_idx, new_name) in [(0usize, "id2"), (1, "val2")] {
        precheck(
            &engine,
            SysFamily::Column,
            &col_alter_pair(tid, col_idx as i64, &cols[col_idx], rename_to(new_name)),
        )
        .expect("renaming a column with dependent views is legal");
    }

    engine.close();
}

// ── The column-ALTER arm owns every drop guard ──────────────────────────────

fn hide(c: &mut CatalogColumn) {
    c.def.is_hidden = true;
}

fn unnull(c: &mut CatalogColumn) {
    c.def.is_nullable = true;
}

/// A column an FK binds cannot be hidden.
#[test]
fn hiding_a_foreign_key_column_rejected() {
    let (_dir, mut engine) = empty_catalog(1);
    let ptid = engine
        .create_table("public.p", &[col_def("id", TypeCode::U64)], &[0])
        .unwrap();
    let cols = vec![col_def("id", TypeCode::U64), fk_def("r", TypeCode::U64, ptid, 0)];
    let tid = engine.create_table("public.t", &cols, &[0]).unwrap();

    let err = precheck(&engine, SysFamily::Column, &col_alter_pair(tid, 1, &cols[1], hide)).unwrap_err();
    assert!(err.contains("carries a foreign key"), "{err}");

    engine.close();
}

#[test]
fn hiding_an_indexed_column_rejected_but_unnulling_it_accepted() {
    let cols = vec![col_def("id", TypeCode::U64), col_def("c", TypeCode::U64)];
    let (_dir, mut engine, tid) = table_engine(1, &cols);
    engine.create_index("public.t", &["c"], false).unwrap();

    let err = precheck(&engine, SysFamily::Column, &col_alter_pair(tid, 1, &cols[1], hide)).unwrap_err();
    assert!(err.contains("secondary index"), "{err}");
    precheck(&engine, SysFamily::Column, &col_alter_pair(tid, 1, &cols[1], unnull))
        .expect("DROP NOT NULL on an indexed column is legal");

    engine.close();
}

#[test]
fn hiding_or_unnulling_a_pk_column_rejected() {
    let cols = vec![col_def("id", TypeCode::U64), col_def("v", TypeCode::U64)];
    let (_dir, engine, tid) = table_engine(1, &cols);

    for mutate in [hide as fn(&mut CatalogColumn), unnull] {
        let err = precheck(&engine, SysFamily::Column, &col_alter_pair(tid, 0, &cols[0], mutate)).unwrap_err();
        assert!(err.contains("primary-key"), "{err}");
    }

    engine.close();
}

/// The engine holds a SERIAL table to one PK column of a SERIAL-eligible type.
#[test]
fn a_serial_table_has_one_narrow_integer_pk() {
    let (_dir, mut engine) = empty_catalog(1);
    let serial = gnitz_wire::TableProps { serial: true, ..Default::default() };
    let cols = |pk_tc| vec![col_def("id", pk_tc), col_def("v", TypeCode::U64)];

    let err = engine
        .create_table_with("public.compound", &cols(TypeCode::I64), &[0, 1], serial)
        .unwrap_err();
    assert!(err.contains("one SERIAL column"), "{err}");
    for (i, tc) in [TypeCode::U128, TypeCode::Date, TypeCode::Decimal]
        .into_iter()
        .enumerate()
    {
        let err = engine
            .create_table_with(&format!("public.bad{i}"), &cols(tc), &[0], serial)
            .unwrap_err();
        assert!(
            err.contains("SERIAL needs an integer of at most 8 bytes"),
            "{tc:?}: {err}"
        );
    }
    engine
        .create_table_with("public.ok", &cols(TypeCode::I64), &[0], serial)
        .expect("a lone I64 SERIAL PK is legal");

    engine.close();
}

/// Two visible columns of one name make the relation unregisterable at the next
/// boot, so an ADD COLUMN or a RENAME COLUMN onto a visible name is refused —
/// named as malformed even when the table has a dependent view.
#[test]
fn a_duplicate_visible_column_name_is_named_before_dependent_views() {
    let cols = vec![col_def("id", TypeCode::U64), col_def("v", TypeCode::U64)];
    let (_dir, mut engine, tid) = table_engine(1, &cols);
    register_identity_view(&mut engine, tid, "vw", &cols);

    let add = crate::test_support::sys_batch([(ColTabRow::of(tid, 2, &nullable_def("v", TypeCode::U64).def, None), 1)]);
    let err = precheck(&engine, SysFamily::Column, &add).unwrap_err();
    assert!(err.contains("duplicate column name"), "add: {err}");
    let err = precheck(
        &engine,
        SysFamily::Column,
        &col_alter_pair(tid, 1, &cols[1], rename_to("id")),
    )
    .unwrap_err();
    assert!(err.contains("duplicate column name"), "rename: {err}");

    engine.close();
}

/// Once hidden, a column is gone from every surface: neither a rename nor a new
/// index may name it.
#[test]
fn a_dropped_column_cannot_be_renamed_or_indexed() {
    let cols = vec![col_def("id", TypeCode::U64), col_def("a", TypeCode::U64)];
    let (_dir, mut engine, tid) = table_engine(1, &cols);
    apply_ddl(
        &mut engine,
        [(SysFamily::Column, col_alter_pair(tid, 1, &cols[1], hide))],
    )
    .unwrap();
    let mut hidden = cols[1].clone();
    hidden.def.is_hidden = true;

    let err = precheck(
        &engine,
        SysFamily::Column,
        &col_alter_pair(tid, 1, &hidden, rename_to("z")),
    )
    .unwrap_err();
    assert!(err.contains("dropped"), "rename: {err}");

    let idx = idx_tab_batch(engine.allocate_ids(1).unwrap(), tid, &[1], "public__t__idx_a", false, 1);
    let err = precheck(&engine, SysFamily::Index, &idx).unwrap_err();
    assert!(err.contains("dropped"), "index: {err}");

    engine.close();
}

// ── A column ALTER is confined to its own bundle ─────────────────────────────

#[test]
fn a_column_alter_must_be_its_bundles_only_change() {
    let (_dir, mut engine, tid) = table_engine(1, &u64_i64_cols());
    let other = engine.create_table("public.u", &u64_i64_cols(), &[0]).unwrap();
    let append = |owner: u64, bb: &mut BatchBuilder| {
        ColTabRow::of(owner, 2, &nullable_def("w", TypeCode::I64).def, None).write(bb, 1);
    };

    // An append bundled with an index family.
    let mut bb = BatchBuilder::new(SysFamily::Column.schema());
    append(tid, &mut bb);
    let index_id = engine.allocate_ids(1).unwrap();
    let blocks = [
        (SysFamily::Column, bb.finish()),
        (
            SysFamily::Index,
            idx_tab_batch(index_id, tid, &[1], "public__t__idx_v", false, 1),
        ),
    ];
    let err = apply_ddl(&mut engine, blocks).unwrap_err();
    assert!(err.contains("only change"), "{err}");

    // One column block altering two owners.
    let mut bb = BatchBuilder::new(SysFamily::Column.schema());
    append(tid, &mut bb);
    append(other, &mut bb);
    let err = apply_ddl(&mut engine, [(SysFamily::Column, bb.finish())]).unwrap_err();
    assert!(err.contains("only change"), "{err}");

    // One column block changing two columns of one owner: a rename pair on
    // column 1 and an append at index 2.
    let mut cols = col_alter_pair(tid, 1, &u64_i64_cols()[1], rename_to("v2"));
    let mut bb = BatchBuilder::new(SysFamily::Column.schema());
    append(tid, &mut bb);
    cols.append_batch(&bb.finish());
    let err = apply_ddl(&mut engine, [(SysFamily::Column, cols)]).unwrap_err();
    assert!(err.contains("only change"), "{err}");

    engine.close();
}
