//! ALTER catalog mechanics driven directly at the `CatalogEngine` layer: the
//! reconciling register hooks (a rename pair fires no cascade), the id-only
//! directory resume across a reopen, and the column-ALTER guards. The
//! end-to-end SQL surface is in `crates/gnitz-sql/tests/engine/ddl.rs`.

use super::*;
use gnitz_wire::sys_rows::{write_table_tab_row, TableTabRow};
use gnitz_wire::{RELTAB_PAY_SCHEMA_ID, TABTAB_PAY_FLAGS, TABTAB_PAY_PK_COL_IDX};
use std::path::Path;

/// A rename of `tid` to `new_name` as its two TABLE_TAB rows, `[-1, +1]`: the
/// `-1` copies the live row, the `+1` differs from it only in `name`.
fn table_rename_rows(engine: &CatalogEngine, tid: u64, new_name: &str) -> [Batch; 2] {
    let minus = engine.retract_under(SysFamily::Table, &[tid]);
    let mut bb = BatchBuilder::new(SysFamily::Table.schema());
    let row = TableTabRow {
        table_id: tid,
        schema_id: payload_u64(&minus, 0, RELTAB_PAY_SCHEMA_ID),
        name: new_name,
        pk_col_idx: payload_u64(&minus, 0, TABTAB_PAY_PK_COL_IDX),
        flags: payload_u64(&minus, 0, TABTAB_PAY_FLAGS),
    };
    write_table_tab_row(&mut bb, &row, 1);
    [minus, bb.finish()]
}

fn table_rename_pair(engine: &CatalogEngine, tid: u64, new_name: &str) -> Batch {
    let [mut pair, plus] = table_rename_rows(engine, tid, new_name);
    pair.append_batch(&plus);
    pair
}

// ── Reconciling hooks: a rename fires no teardown ───────────────────────────

#[test]
fn rename_fires_no_cascade_and_leaves_dir_untouched() {
    let dir = temp_dir("alter_no_cascade");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
    let cols = vec![col_def("id", TypeCode::U64), col_def("v", TypeCode::U64)];
    let tid = engine.create_table("public.orig", &cols, &[0]).unwrap();
    let table_path = relation_dir(&dir, tid);
    assert!(Path::new(&table_path).exists());

    let pair = table_rename_pair(&engine, tid, "renamed");
    engine.ingest_to_family(gnitz_wire::TABLE_TAB, &pair).unwrap();

    // The registration survives, so its directory is live.
    assert!(engine.registry.has_id(tid), "rename must not unregister the table");
    assert!(Path::new(&table_path).exists(), "rename must not delete the table dir");
    // Caches reflect the new name; no persistent ghost row.
    assert!(engine.get_by_name("public", "renamed").is_some());
    assert!(engine.get_by_name("public", "orig").is_none());
    assert_eq!(
        rows_under(&engine, SysFamily::Table, tid),
        1,
        "a rename must leave exactly one live row"
    );

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── Id-only dirs resume after a rename + reopen ─────────────────────────────

#[test]
fn rename_then_reopen_resolves_flushed_data() {
    let dir = temp_dir("alter_reopen");
    let tid;
    {
        let mut engine = CatalogEngine::open(&dir, 1).unwrap();
        let cols = vec![col_def("id", TypeCode::U64), col_def("v", TypeCode::U64)];
        tid = engine.create_table("public.orig", &cols, &[0]).unwrap();
        // Flush a row so only the on-disk (id-only) path can serve it after reopen.
        let schema = engine.registry.relation(tid).map(Relation::schema).unwrap();
        let mut bb = BatchBuilder::new(&schema);
        bb.begin_row(7u128, 1);
        bb.put_u64(70);
        bb.end_row();
        engine.ingest_to_family(tid, &bb.finish()).unwrap();
        engine.registry.checkpoint_base().unwrap();

        let pair = table_rename_pair(&engine, tid, "renamed");
        engine.ingest_to_family(gnitz_wire::TABLE_TAB, &pair).unwrap();
        engine.close();
    }

    // Reopen: boot replay re-registers at the id-only path `{tid}` (unchanged by
    // the rename), so the flushed row still resolves under the new name.
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
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
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn valid_long_name_rename_accepted() {
    // Regression against a naive region `memcmp`: a name > 12 bytes lives in the
    // blob heap, so the `-1` and live rows carry independent heap offsets. The CAS
    // compares German-string content, so a valid rename to a long name is accepted.
    let dir = temp_dir("alter_long_ok");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
    let cols = vec![col_def("id", TypeCode::U64)];
    let tid = engine.create_table("public.original_long_name", &cols, &[0]).unwrap();
    let pair = table_rename_pair(&engine, tid, "renamed_to_a_long_name");
    engine.ingest_to_family(gnitz_wire::TABLE_TAB, &pair).unwrap();
    assert!(engine.get_by_name("public", "renamed_to_a_long_name").is_some());
    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── The column-ALTER owner guard covers every rewrite pair ──────────────────

fn rename_to(new_name: &str) -> impl FnOnce(&mut CatalogColumn) + '_ {
    move |c: &mut CatalogColumn| c.def.name = new_name.to_string()
}

/// RENAME COLUMN on a view, which is no user base table; the SQL planner is not
/// the trust boundary that stops it.
#[test]
fn column_rename_on_non_base_owner_rejected() {
    let dir = temp_dir("alter_col_owner");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();

    let cols = vec![col_def("id", TypeCode::U64)];
    let tid = engine.create_table("public.t", &cols, &[0]).unwrap();
    let vid = register_identity_view(&mut engine, tid, "v", &cols);

    let err = engine
        .precheck_family(SysFamily::Column, &col_alter_pair(vid, 0, &cols[0], rename_to("id2")))
        .unwrap_err();
    assert!(err.contains("not a user base table"), "view owner: {err}");

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

/// The two guards a rename skips: a rename is legal on a PK column and on a
/// table with dependent views (views bind columns by ordinal).
#[test]
fn column_rename_on_pk_column_and_with_dependent_views_accepted() {
    let dir = temp_dir("alter_col_rename_ok");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();

    let cols = vec![col_def("id", TypeCode::U64), col_def("val", TypeCode::U64)];
    let tid = engine.create_table("public.t", &cols, &[0]).unwrap();
    let vid = register_identity_view(&mut engine, tid, "v", &cols);
    assert_eq!(
        engine.dag.dependents_of(tid),
        &[vid][..],
        "precondition: the table has a dependent view"
    );

    for (col_idx, new_name) in [(0usize, "id2"), (1, "val2")] {
        engine
            .precheck_family(
                SysFamily::Column,
                &col_alter_pair(tid, col_idx as i64, &cols[col_idx], rename_to(new_name)),
            )
            .expect("renaming a column with dependent views is legal");
    }

    engine.close();
    let _ = fs::remove_dir_all(&dir);
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
    let dir = temp_dir("alter_hide_fk");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
    let ptid = engine
        .create_table("public.p", &[col_def("id", TypeCode::U64)], &[0])
        .unwrap();
    let cols = vec![col_def("id", TypeCode::U64), fk_def("r", TypeCode::U64, ptid, 0)];
    let tid = engine.create_table("public.t", &cols, &[0]).unwrap();

    let err = engine
        .precheck_family(SysFamily::Column, &col_alter_pair(tid, 1, &cols[1], hide))
        .unwrap_err();
    assert!(err.contains("carries a foreign key"), "{err}");

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn hiding_an_indexed_column_rejected_but_unnulling_it_accepted() {
    let cols = vec![col_def("id", TypeCode::U64), col_def("c", TypeCode::U64)];
    let (mut engine, tid, dir) = table_fixture("alter_hide_indexed", &cols);
    engine.create_index("public.t", &["c"], false).unwrap();

    let err = engine
        .precheck_family(SysFamily::Column, &col_alter_pair(tid, 1, &cols[1], hide))
        .unwrap_err();
    assert!(err.contains("secondary index"), "{err}");
    engine
        .precheck_family(SysFamily::Column, &col_alter_pair(tid, 1, &cols[1], unnull))
        .expect("DROP NOT NULL on an indexed column is legal");

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn hiding_or_unnulling_a_pk_column_rejected() {
    let cols = vec![col_def("id", TypeCode::U64), col_def("v", TypeCode::U64)];
    let (engine, tid, dir) = table_fixture("alter_pk_col", &cols);

    for mutate in [hide as fn(&mut CatalogColumn), unnull] {
        let err = engine
            .precheck_family(SysFamily::Column, &col_alter_pair(tid, 0, &cols[0], mutate))
            .unwrap_err();
        assert!(err.contains("primary-key"), "{err}");
    }

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

/// The engine holds a SERIAL table to one PK column of a SERIAL-eligible type.
#[test]
fn a_serial_table_has_one_narrow_integer_pk() {
    let dir = temp_dir("serial_pk");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
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
    let _ = fs::remove_dir_all(&dir);
}

/// Two visible columns of one name make the relation unregisterable at the next
/// boot, so an ADD COLUMN or a RENAME COLUMN onto a visible name is refused —
/// named as malformed even when the table has a dependent view.
#[test]
fn a_duplicate_visible_column_name_is_named_before_dependent_views() {
    let cols = vec![col_def("id", TypeCode::U64), col_def("v", TypeCode::U64)];
    let (mut engine, tid, dir) = table_fixture("alter_add_dup_dep", &cols);
    register_identity_view(&mut engine, tid, "vw", &cols);

    let mut bb = BatchBuilder::new(SysFamily::Column.schema());
    nullable_def("v", TypeCode::U64).write_col_tab_row(&mut bb, tid, 2, 1);
    let err = engine.precheck_family(SysFamily::Column, &bb.finish()).unwrap_err();
    assert!(err.contains("duplicate column name"), "add: {err}");
    let err = engine
        .precheck_family(SysFamily::Column, &col_alter_pair(tid, 1, &cols[1], rename_to("id")))
        .unwrap_err();
    assert!(err.contains("duplicate column name"), "rename: {err}");

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

/// Once hidden, a column is gone from every surface: neither a rename nor a new
/// index may name it.
#[test]
fn a_dropped_column_cannot_be_renamed_or_indexed() {
    let cols = vec![col_def("id", TypeCode::U64), col_def("a", TypeCode::U64)];
    let (mut engine, tid, dir) = table_fixture("alter_dropped_col", &cols);
    engine
        .ingest_to_family(gnitz_wire::COL_TAB, &col_alter_pair(tid, 1, &cols[1], hide))
        .unwrap();
    let mut hidden = cols[1].clone();
    hidden.def.is_hidden = true;

    let err = engine
        .precheck_family(SysFamily::Column, &col_alter_pair(tid, 1, &hidden, rename_to("z")))
        .unwrap_err();
    assert!(err.contains("dropped"), "rename: {err}");

    let idx = idx_tab_batch(
        engine.allocate_ids(1).unwrap(),
        tid,
        &[1],
        "public__t__idx_a",
        gnitz_wire::IndexProps { is_unique: false },
        1,
    );
    let err = engine.precheck_family(SysFamily::Index, &idx).unwrap_err();
    assert!(err.contains("dropped"), "index: {err}");

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── The hook canonicalizer repairs a `+1`-first rewrite pair ────────────────
// A raw `DDL_TXN` block or a compensation can hand the hooks one.

#[test]
fn insert_first_rename_pair_lands_the_new_name() {
    let dir = temp_dir("alter_insert_first_pair");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
    let cols = vec![col_def("id", TypeCode::U64), col_def("v", TypeCode::U64)];
    let tid = engine.create_table("public.orig", &cols, &[0]).unwrap();

    let [minus, mut pair] = table_rename_rows(&engine, tid, "renamed");
    pair.append_batch(&minus);
    engine.ingest_to_family(gnitz_wire::TABLE_TAB, &pair).unwrap();

    assert!(
        engine.get_by_name("public", "renamed").is_some(),
        "the new name must resolve"
    );
    assert!(
        engine.get_by_name("public", "orig").is_none(),
        "the outgoing name must be unmapped"
    );
    assert_eq!(
        engine.qualified_name_or_unknown(tid).1,
        "renamed",
        "the catalog must name the table by its new name"
    );
    assert!(
        engine.registry.has_id(tid),
        "a rename must not unregister the table, whatever the row order"
    );
    assert_eq!(
        rows_under(&engine, SysFamily::Table, tid),
        1,
        "a rename leaves exactly one live row"
    );

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── A column ALTER is confined to its own bundle ─────────────────────────────

#[test]
fn a_column_alter_must_be_its_bundles_only_change() {
    let (mut engine, tid, dir) = table_fixture(
        "alter_confined_to_bundle",
        &[col_def("id", TypeCode::U64), col_def("v", TypeCode::I64)],
    );
    let other = engine
        .create_table(
            "public.u",
            &[col_def("id", TypeCode::U64), col_def("v", TypeCode::I64)],
            &[0],
        )
        .unwrap();
    let append = |owner: u64, bb: &mut BatchBuilder| {
        nullable_def("w", TypeCode::I64).write_col_tab_row(bb, owner, 2, 1);
    };

    // An append bundled with an index family.
    let mut bb = BatchBuilder::new(SysFamily::Column.schema());
    append(tid, &mut bb);
    let mut families: [Option<Batch>; SysFamily::COUNT] = std::array::from_fn(|_| None);
    families[SysFamily::Column.index()] = Some(bb.finish());
    families[SysFamily::Index.index()] = Some(idx_tab_batch(
        engine.allocate_ids(1).unwrap(),
        tid,
        &[1],
        "public__t__idx_v",
        gnitz_wire::IndexProps { is_unique: false },
        1,
    ));
    let err = engine.precheck_bundle(&families).unwrap_err();
    assert!(err.contains("only change"), "{err}");

    // One column block altering two owners.
    let mut bb = BatchBuilder::new(SysFamily::Column.schema());
    append(tid, &mut bb);
    append(other, &mut bb);
    let mut families: [Option<Batch>; SysFamily::COUNT] = std::array::from_fn(|_| None);
    families[SysFamily::Column.index()] = Some(bb.finish());
    let err = engine.precheck_bundle(&families).unwrap_err();
    assert!(err.contains("only change"), "{err}");

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}
