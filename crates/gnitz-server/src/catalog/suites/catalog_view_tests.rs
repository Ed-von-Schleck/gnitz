//! Views over a system family: which side of a family's cut a worker lands a
//! catalog delta on, what a backfill reads of it, what a boot keeps of such a
//! view, and the two relations the catalog refuses beside its own.

use super::*;
use gnitz_wire::sys_rows::{SysRow, TableTabRow, ViewTabRow};

const TABLES: u64 = gnitz_wire::TABLE_TAB;

/// An identity view over the `tables` family.
fn tables_view(engine: &mut CatalogEngine, name: &str) -> u64 {
    register_identity_view(engine, TABLES, name, &SysFamily::Table.column_defs())
}

/// A worker holds a scanned family's delta above the cut: the family reads it at
/// once, a view created meanwhile backfills the sealed prefix alone, and the tick
/// brings the delta to both views once.
#[test]
fn a_scanned_familys_delta_waits_above_the_cut_for_its_tick() {
    let dir = temp_dir("catalog_view_delta_above_cut");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
    let seeds = SysFamily::ALL.len();
    let cols = vec![col_def("id", TypeCode::U64), col_def("val", TypeCode::U64)];

    let first = tables_view(&mut engine, "v_first");
    backfill(&mut engine, first);
    assert_eq!(held(&engine, first), (seeds, seeds as i64), "the seed rows");

    let tid = engine.allocate_ids(1).unwrap();
    engine.register_table(tid, PUBLIC_SCHEMA_ID, "t", &cols, &[0]).unwrap();
    assert_eq!(held(&engine, first), (seeds, seeds as i64), "no tick, no row");
    assert_eq!(
        held(&engine, TABLES),
        (seeds + 1, seeds as i64 + 1),
        "the family reads it"
    );

    let second = tables_view(&mut engine, "v_second");
    backfill(&mut engine, second);
    assert_eq!(
        held(&engine, second),
        (seeds, seeds as i64),
        "a backfill reads the sealed prefix"
    );

    seal_and_tick(&mut engine, TABLES);
    for view in [first, second] {
        assert_eq!(held(&engine, view), (seeds + 1, seeds as i64 + 1), "view {view}");
    }
    assert!(engine.registry.seal(TABLES).unwrap().is_none());

    discard(engine);
}

#[test]
fn a_ddl_sync_into_an_unscanned_family_leaves_nothing_above_its_cut() {
    let dir = temp_dir("catalog_unscanned_family_sealed");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
    let cols = vec![col_def("id", TypeCode::U64), col_def("val", TypeCode::U64)];
    // A view over `tables` alone: `columns` stays unscanned.
    let view = tables_view(&mut engine, "v_tables");
    backfill(&mut engine, view);

    let tid = engine.allocate_ids(1).unwrap();
    engine.register_table(tid, PUBLIC_SCHEMA_ID, "t", &cols, &[0]).unwrap();

    assert!(engine.registry.seal(gnitz_wire::COL_TAB).unwrap().is_none());
    assert_eq!(engine.registry.seal(TABLES).unwrap().map(|d| d.len()), Some(1));

    discard(engine);
}

/// The verdict's own clause: every view here holds a manifest at the committed
/// generation, published past the engine's round, so nothing but its source
/// rejects the two that reach a family.
#[test]
fn a_view_reaching_a_family_awaits_rebuild_at_reopen() {
    let dir = temp_dir("catalog_views_invalid_at_boot");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
    let cols = vec![col_def("id", TypeCode::U64), col_def("amount", TypeCode::I64)];
    let tid = engine.create_table("public.t", &cols, &[0]).unwrap();

    let direct = tables_view(&mut engine, "v_direct");
    let downstream = register_identity_view(&mut engine, direct, "v_downstream", &SysFamily::Table.column_defs());
    let over_table = register_identity_view(&mut engine, tid, "v_table", &cols);

    engine.record_topology(1).unwrap();
    let g = engine.advance_durable_generation().unwrap();
    engine.registry.checkpoint_ephemeral([], g, |_| true).unwrap();
    engine.close();

    let engine = CatalogEngine::open(&dir, 1).unwrap();
    assert!(engine.dag.awaits_rebuild(direct), "a family source invalidates");
    assert!(engine.dag.awaits_rebuild(downstream), "and the verdict cascades");
    assert!(
        !engine.dag.awaits_rebuild(over_table),
        "a view over a base table resumes"
    );

    fs::remove_dir_all(&dir).ok();
}

#[test]
fn an_ephemeral_round_publishes_no_view_a_family_reaches() {
    let dir = temp_dir("catalog_views_unpublished");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
    let cols = vec![col_def("id", TypeCode::U64), col_def("amount", TypeCode::I64)];
    let tid = engine.create_table("public.t", &cols, &[0]).unwrap();
    let family_cols = SysFamily::Table.column_defs();

    let direct = traced_view(&mut engine, TABLES, "v_direct", &family_cols);
    let downstream = traced_view(&mut engine, direct, "v_downstream", &family_cols);
    let over_table = traced_view(&mut engine, tid, "v_table", &cols);

    engine.record_topology(1).unwrap();
    let g = engine.advance_durable_generation().unwrap();
    engine.flush_ephemeral_round(g).unwrap();

    assert_eq!(published_children(&dir, direct), Vec::<String>::new());
    assert_eq!(published_children(&dir, downstream), Vec::<String>::new());
    let table_children = published_children(&dir, over_table);
    assert!(table_children.contains(&"w0of1".to_string()), "got {table_children:?}");
    engine.close();

    let engine = CatalogEngine::open(&dir, 1).unwrap();
    assert!(engine.dag.awaits_rebuild(direct));
    assert!(engine.dag.awaits_rebuild(downstream));
    assert!(!engine.dag.awaits_rebuild(over_table));

    fs::remove_dir_all(&dir).ok();
}

#[test]
fn the_system_schema_takes_no_relation() {
    let dir = temp_dir("catalog_system_schema_closed");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
    let cols = vec![col_def("id", TypeCode::U64), col_def("val", TypeCode::U64)];
    let before = sys_row_counts(&engine);
    let pk_col_idx = PkColList::from_slice(&[0]).pack();

    let tid = engine.allocate_ids(1).unwrap();
    let mut table = BatchBuilder::new(SysFamily::Table.schema());
    TableTabRow {
        table_id: tid,
        schema_id: SYSTEM_SCHEMA_ID,
        name: "mine",
        pk_col_idx,
        flags: gnitz_wire::TableProps::default().pack(),
    }
    .write(&mut table, 1);
    let table = vec![
        (SysFamily::Column, col_tab_batch(tid, &cols, 1)),
        (SysFamily::Table, table.finish()),
    ];

    let vid = engine.allocate_ids(1).unwrap();
    let mut view = BatchBuilder::new(SysFamily::View.schema());
    ViewTabRow {
        view_id: vid,
        schema_id: SYSTEM_SCHEMA_ID,
        name: "mine",
        pk_col_idx,
        capacity_bytes: 0,
        delta_bytes: 0,
        owner_view_id: 0,
        pk_repeats: 0,
    }
    .write(&mut view, 1);
    let family_cols = SysFamily::Table.column_defs();
    let circuit = crate::test_support::identity_circuit(TABLES, gnitz_wire::ReadBound::None);
    let view: Vec<_> = view_blocks([(vid, &circuit, &family_cols[..])], view.finish()).into();

    for blocks in [table, view] {
        let err = apply_ddl(&mut engine, blocks).expect_err("a relation in the system schema");
        assert!(err.contains("the system schema takes no relation"), "{err}");
        assert_eq!(sys_row_counts(&engine), before);
    }
    assert!(!engine.registry.has_id(tid) && !engine.registry.has_id(vid));
    assert!(!engine.dag.is_scanned(TABLES));

    discard(engine);
}

#[test]
fn no_view_scans_sequences() {
    let dir = temp_dir("catalog_sequences_unscanned");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
    let before = sys_row_counts(&engine);
    let seq = SysFamily::Sequence.id();
    let cols = SysFamily::Sequence.column_defs();

    let vid = engine.allocate_ids(1).unwrap();
    let mut view = BatchBuilder::new(SysFamily::View.schema());
    push_view_tab_row(&mut view, 1, vid, "v_seq", 0, 0, 0);
    let circuit = crate::test_support::identity_circuit(seq, gnitz_wire::ReadBound::None);
    let err = apply_ddl(&mut engine, view_blocks([(vid, &circuit, &cols[..])], view.finish()))
        .expect_err("a view over sequences");
    assert!(err.contains("holds no set a view can scan"), "{err}");
    assert!(err.contains("_system.sequences"), "{err}");

    assert!(!engine.dag.is_scanned(seq), "the refused view left no edge");
    assert!(engine.dag.sources_of(vid).is_empty());
    assert!(!engine.registry.has_id(vid));
    // allocate_ids moved `sequences` itself; every other family is as it was.
    let after = sys_row_counts(&engine);
    for family in SysFamily::ALL {
        if family != SysFamily::Sequence {
            assert_eq!(after[family.index()], before[family.index()], "{}", family.name());
        }
    }

    discard(engine);
}
