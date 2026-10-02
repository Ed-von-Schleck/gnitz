//! The per-PK CAS + net retraction contract, which every family shares: a `-1`
//! must reproduce the live row, and no write may leave a PK at a net weight
//! outside `{0, 1}`. Each family's drop consumers unmap cached state read off
//! the batch payload, so a `-1` the store does not hold must never reach them.

use super::*;

#[test]
fn a_write_must_retract_the_live_row_and_leave_one_or_none() {
    let dir = temp_dir("sysretract_contract");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();

    // One live user row in every family, under names long enough to sit in the
    // blob heap.
    engine.create_schema("a_schema_with_a_long_name").unwrap();
    let sid = engine.schema_id("a_schema_with_a_long_name").unwrap();
    let cols = [
        col_def("id", TypeCode::U64),
        col_def("a_long_column_name", TypeCode::U64),
    ];
    let table = "a_schema_with_a_long_name.a_table_with_a_long_name";
    let serial = gnitz_wire::TableProps { serial: true, ..Default::default() };
    let tid = engine.create_table_with(table, &cols, &[0], serial).unwrap();
    let (_, reserved) = engine.reserve_user_sequence(tid, 64).unwrap();
    engine.ingest_to_family(gnitz_wire::SEQ_TAB, &reserved).unwrap();
    let idx = engine.create_index(table, &["a_long_column_name"], false).unwrap();
    let vid = register_identity_view(&mut engine, tid, "a_view_with_a_long_name", &cols);
    let unused = engine.allocate_ids(1).unwrap();

    for family in SysFamily::ALL {
        let id = match family {
            SysFamily::Schema => sid,
            SysFamily::Table | SysFamily::Column | SysFamily::Sequence => tid,
            SysFamily::View | SysFamily::Circuit => vid,
            SysFamily::Index => idx,
        };

        // The sys stores run no `enforce_unique_pk`, so nothing but the net
        // stops a re-pushed `+1` from leaving two live heads under one PK.
        let live = engine.retract_under(family, &[id]).negated();
        assert!(!live.is_empty(), "{family:?}: the fixture holds no live row");
        let err = engine.precheck_family(family, &live).unwrap_err();
        assert!(err.contains("net weight 2"), "{family:?}: {err}");

        // A retraction in the shape the family admits: a rewrite pair where it
        // declares one, a bare `-1` where a client may drop a row. A circuit row
        // admits neither.
        if family == SysFamily::Circuit {
            continue;
        }
        let retraction = |leading: u64| {
            let mut bb = BatchBuilder::new(family.schema());
            push_sys_row(&mut bb, family, [leading, 0], -1, |_| 7);
            if family.pair_change_mask().is_some() {
                push_sys_row(&mut bb, family, [leading, 0], 1, |_| 7);
            }
            bb.finish()
        };
        for (leading, why) in [(id, "differs from the current one"), (unused, "no longer exists")] {
            let err = engine.precheck_family(family, &retraction(leading)).unwrap_err();
            assert!(err.contains(why), "{family:?}: {err}");
        }
    }

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

/// The accepted path: `drop_index` builds its `-1` by copying the live row, so
/// the pair must consolidate away and leave no stored row at all.
#[test]
fn create_then_drop_unique_index_cancels_to_empty() {
    let dir = temp_dir("idx_create_drop_cancels");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();

    let cols = vec![col_def("pk", TypeCode::U64), col_def("val", TypeCode::I64)];
    engine.create_table("public.cancels", &cols, &[0]).unwrap();
    let idx_id = engine.create_index("public.cancels", &["val"], true).unwrap();
    assert_eq!(
        idx_weights_for(&engine, idx_id),
        vec![1],
        "the create leaves one live row"
    );

    engine
        .drop_index(&make_secondary_index_name("public", "cancels", "val"))
        .unwrap();
    assert!(
        idx_weights_for(&engine, idx_id).is_empty(),
        "the drop's `-1` must cancel the create's `+1`, leaving no stored row"
    );

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}
