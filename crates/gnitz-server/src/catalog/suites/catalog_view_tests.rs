//! Views over a system family: which side of a family's cut a worker lands a
//! catalog delta on, what a backfill reads of it, what a boot keeps of such a
//! view, and the two relations the catalog refuses beside its own.

use super::*;

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
    let (_dir, mut engine) = empty_catalog(1);
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
}

#[test]
fn a_ddl_sync_into_an_unscanned_family_leaves_nothing_above_its_cut() {
    let (_dir, mut engine) = empty_catalog(1);
    let cols = vec![col_def("id", TypeCode::U64), col_def("val", TypeCode::U64)];
    // A view over `tables` alone: `columns` stays unscanned.
    let view = tables_view(&mut engine, "v_tables");
    backfill(&mut engine, view);

    let tid = engine.allocate_ids(1).unwrap();
    engine.register_table(tid, PUBLIC_SCHEMA_ID, "t", &cols, &[0]).unwrap();

    assert!(engine.registry.seal(gnitz_wire::COL_TAB).unwrap().is_none());
    assert_eq!(engine.registry.seal(TABLES).unwrap().map(|d| d.len()), Some(1));
}

/// A view reaching a family awaits a rebuild at reopen, and so does one over it.
#[test]
fn a_view_reaching_a_family_awaits_rebuild_at_reopen() {
    let (_dir, engine) = empty_catalog(1);
    assert_unreplayed_source_rebuilds_at_reopen(engine, TABLES, &SysFamily::Table.column_defs());
}

/// An ephemeral round publishes no store of a view a family reaches.
#[test]
fn an_ephemeral_round_publishes_no_view_a_family_reaches() {
    let (_dir, engine) = empty_catalog(1);
    assert_round_publishes_nothing_reaching(engine, TABLES, &SysFamily::Table.column_defs());
}

#[test]
fn the_system_schema_takes_no_relation() {
    let (_dir, mut engine) = empty_catalog(1);
    let cols = vec![col_def("id", TypeCode::U64), col_def("val", TypeCode::U64)];
    let tid = engine.allocate_ids(1).unwrap();
    let table = TableTabRow {
        schema_id: SYSTEM_SCHEMA_ID,
        ..table_tab_row(tid, "mine")
    };
    let table = vec![
        (SysFamily::Column, col_tab_batch(tid, &cols, 1)),
        (SysFamily::Table, sys_batch([(table, 1)])),
    ];

    let vid = engine.allocate_ids(1).unwrap();
    let before = sys_row_counts(&engine);
    let view = ViewTabRow {
        schema_id: SYSTEM_SCHEMA_ID,
        ..view_tab_row(vid, "mine")
    };
    let family_cols = SysFamily::Table.column_defs();
    let circuit = crate::test_support::identity_circuit(TABLES, gnitz_wire::ReadBound::None);
    let view: Vec<_> = view_blocks([(vid, &circuit, &family_cols[..])], sys_batch([(view, 1)])).into();

    for blocks in [table, view] {
        let err = apply_ddl(&mut engine, blocks).expect_err("a relation in the system schema");
        assert!(err.contains("the system schema takes no relation"), "{err}");
        assert_eq!(sys_row_counts(&engine), before);
    }
    assert!(engine.registry.relation(tid).is_none() && engine.registry.relation(vid).is_none());
    assert!(!engine.dag.is_scanned(TABLES));
}

#[test]
fn no_view_scans_sequences() {
    let (_dir, mut engine) = empty_catalog(1);
    let before = sys_row_counts(&engine);
    let seq = SysFamily::Sequence.id();
    let cols = SysFamily::Sequence.column_defs();

    let vid = engine.allocate_ids(1).unwrap();
    let view = sys_batch([(view_tab_row(vid, "v_seq"), 1)]);
    let circuit = crate::test_support::identity_circuit(seq, gnitz_wire::ReadBound::None);
    let err =
        apply_ddl(&mut engine, view_blocks([(vid, &circuit, &cols[..])], view)).expect_err("a view over sequences");
    assert!(err.contains("holds no set a view can scan"), "{err}");
    assert!(err.contains("_system.sequences"), "{err}");

    assert!(!engine.dag.is_scanned(seq), "the refused view left no edge");
    assert!(engine.dag.sources_of(vid).next().is_none());
    assert!(engine.registry.relation(vid).is_none());
    // allocate_ids moved `sequences` itself; every other family is as it was.
    let after = sys_row_counts(&engine);
    for family in SysFamily::ALL {
        if family != SysFamily::Sequence {
            assert_eq!(after[family.index()], before[family.index()], "{}", family.name());
        }
    }
}
