use super::*;
use crate::test_support::{make_batch_raw, make_schema_u64_i64, u64_i64_cols};
use gnitz_wire::TypeCode;

/// A write to a parent referenced through its lone PK column probes nothing
/// unless it deletes: an upsert resolves with no worker answering, and a
/// retraction waits on the child-index probe of the key it removes.
#[test]
fn a_lone_pk_parent_probes_only_for_the_keys_it_deletes() {
    use crate::runtime::master::fixtures::test_dispatcher;
    use crate::runtime::test_support::try_poll_once;
    use crate::test_support::empty_catalog;
    use crate::test_support::fk_child_cols;

    let (_tmp, mut engine) = empty_catalog(1);
    let parent = engine.create_table("public.parent", &u64_i64_cols(), &[0]).unwrap();
    engine
        .create_table("public.child", &fk_child_cols(parent), &[0])
        .unwrap();
    let schema = engine.registry.relation(parent).unwrap().schema();
    let (disp, _) = test_dispatcher(1, engine);
    let checks = Checks::new();

    let write = |rows: &[(u64, i64, i64)]| {
        vec![TxnFamily::new(
            parent,
            WireConflictMode::Update,
            FamilyRows::Built(make_batch_raw(&schema, rows)),
        )]
    };
    let mut upsert = write(&[(1, 1, 10), (2, 1, 20)]);
    assert!(matches!(
        try_poll_once(checks.validate_txn(&disp, &mut upsert)),
        Some(Ok(()))
    ));
    let mut retraction = write(&[(1, -1, 10)]);
    assert!(try_poll_once(checks.validate_txn(&disp, &mut retraction)).is_none());

    disp.into_catalog().close();
}

/// `(pk, weight, value)` rows; `None` is a NULL value.
type Rows<'r> = &'r [(u64, i64, Option<i64>)];

/// A table with a UNIQUE index on its nullable payload column, behind a
/// one-worker dispatcher: its id, its schema, and the index's column list.
fn uniquely_indexed_table() -> (tempfile::TempDir, MasterDispatcher, u64, SchemaDescriptor, PkColList) {
    use crate::catalog::SysFamily;
    use crate::runtime::master::fixtures::test_dispatcher;
    use crate::test_support::empty_catalog;
    use crate::test_support::{apply_ddl, idx_tab_batch, nullable_def};

    let (tmp, mut engine) = empty_catalog(1);
    let cols = [u64_i64_cols()[0].clone(), nullable_def("v", TypeCode::I64)];
    let tid = engine.create_table("public.t", &cols, &[0]).unwrap();
    let id = engine.allocate_ids(1).unwrap();
    let idx = idx_tab_batch(id, tid, &[1], "public__t__idx_v", true, 1);
    apply_ddl(&mut engine, [(SysFamily::Index, idx)]).unwrap();
    let schema = engine.registry.relation(tid).unwrap().schema();
    let (disp, _) = test_dispatcher(1, engine);
    (tmp, disp, tid, schema, PkColList::from_slice(&[1]))
}

/// An Update family of `rows` of [`uniquely_indexed_table`].
fn update_family(tid: u64, schema: &SchemaDescriptor, rows: Rows<'_>) -> TxnFamily {
    let mut bb = gnitz_zset::repr::BatchBuilder::new(schema);
    for &(pk, w, v) in rows {
        bb.begin_row(pk as u128, w);
        bb.put_opt_int(v.map(|v| v as u128));
        bb.end_row();
    }
    TxnFamily::new(tid, WireConflictMode::Update, FamilyRows::Built(bb.finish()))
}

/// U-SEC's in-bundle duplicate verdict over a table with a UNIQUE index on its
/// nullable payload column, each write over a filter seeded empty so none
/// probes: one row per PK (packed a column at a time once there are enough of
/// them) and a PK touched twice (packed per surviving row) decide alike, across
/// families, deletes, and NULLs, which are unindexed.
#[test]
fn a_unique_index_refuses_exactly_the_spans_two_surviving_rows_claim() {
    use super::super::unique_filter::UniqueFilter;
    use crate::runtime::test_support::try_poll_once;

    let (_tmp, disp, tid, schema, cols) = uniquely_indexed_table();
    let checks = Checks::new();
    let refused = |fams: &[Rows<'_>]| {
        checks.unique_filter_seed(tid, cols, UniqueFilter::new());
        let mut families: Vec<TxnFamily> = fams.iter().map(|&r| update_family(tid, &schema, r)).collect();
        match try_poll_once(checks.validate_txn(&disp, &mut families)) {
            Some(Ok(())) => false,
            Some(Err(e)) => {
                assert!(e.text.contains("duplicate in batch"), "{}", e.text);
                true
            }
            None => panic!("a write over an empty filter probes nothing"),
        }
    };
    let cases: [(&str, &[Rows<'_>], bool); 9] = [
        (
            "distinct",
            &[&[(1, 1, Some(1)), (2, 1, Some(2)), (3, 1, Some(3)), (4, 1, Some(4))]],
            false,
        ),
        (
            "shared",
            &[&[(1, 1, Some(1)), (2, 1, Some(2)), (3, 1, Some(1)), (4, 1, Some(4))]],
            true,
        ),
        ("two rows, shared", &[&[(1, 1, Some(5)), (2, 1, Some(5))]], true),
        (
            "NULLs",
            &[&[(1, 1, None), (2, 1, None), (3, 1, Some(3)), (4, 1, None)]],
            false,
        ),
        // A deleted row's value is no claim.
        (
            "deleted",
            &[&[(1, -1, Some(9)), (2, 1, Some(9)), (3, 1, Some(10)), (4, 1, Some(11))]],
            false,
        ),
        (
            "across families",
            &[
                &[(1, 1, Some(1)), (2, 1, Some(2)), (3, 1, Some(3))],
                &[(4, 1, Some(3)), (5, 1, Some(5))],
            ],
            true,
        ),
        (
            "apart across families",
            &[
                &[(1, 1, Some(1)), (2, 1, Some(2)), (3, 1, Some(3))],
                &[(4, 1, Some(4)), (5, 1, Some(5))],
            ],
            false,
        ),
        // PK 1 is inserted, then deleted: only PKs 2..=4 survive.
        (
            "a PK touched twice",
            &[
                &[(1, 1, Some(7)), (2, 1, Some(7)), (3, 1, Some(8)), (4, 1, Some(9))],
                &[(1, -1, Some(7))],
            ],
            false,
        ),
        (
            "a PK touched twice, shared",
            &[
                &[(1, 1, Some(7)), (2, 1, Some(8)), (3, 1, Some(8)), (4, 1, Some(9))],
                &[(1, -1, Some(7))],
            ],
            true,
        ),
    ];
    for (label, fams, want) in cases {
        assert_eq!(refused(fams), want, "{label}");
    }

    disp.into_catalog().close();
}

/// A filter takes a write's spans at the write's verdict: a refused write's
/// stay proven absent, and an admitted write's send the next write that claims
/// one to a probe.
#[test]
fn a_filter_takes_the_spans_of_an_admitted_write_alone() {
    use super::super::unique_filter::UniqueFilter;
    use crate::runtime::test_support::try_poll_once;

    let (_tmp, disp, tid, schema, cols) = uniquely_indexed_table();
    let checks = Checks::new();
    checks.unique_filter_seed(tid, cols, UniqueFilter::new());
    let validate = |rows: Rows<'_>| {
        let mut families = vec![update_family(tid, &schema, rows)];
        try_poll_once(checks.validate_txn(&disp, &mut families))
    };

    let refused = validate(&[(1, 1, Some(5)), (2, 1, Some(5)), (3, 1, Some(6))]);
    assert!(matches!(refused, Some(Err(e)) if e.text.contains("duplicate in batch")));
    assert!(matches!(validate(&[(4, 1, Some(5)), (5, 1, None)]), Some(Ok(()))));
    assert!(validate(&[(6, 1, Some(5))]).is_none());

    disp.into_catalog().close();
}

/// Only a lone Error family of positive weights is held fresh: an Update
/// family, a retraction, and a second family to the same table each keep the
/// enforced rule.
#[test]
fn only_a_lone_positive_error_family_is_fresh() {
    let family = |tid, mode, weights: &[i64]| {
        let rows: Vec<(u64, i64, i64)> = weights.iter().enumerate().map(|(i, &w)| (i as u64, w, 0)).collect();
        TxnFamily::new(
            tid,
            mode,
            FamilyRows::Built(make_batch_raw(&make_schema_u64_i64(), &rows)),
        )
    };
    let mut families = vec![
        family(1, WireConflictMode::Error, &[1, 1]),
        family(2, WireConflictMode::Update, &[1]),
        family(3, WireConflictMode::Error, &[1, -1]),
        family(4, WireConflictMode::Error, &[1]),
        family(4, WireConflictMode::Error, &[1]),
        family(5, WireConflictMode::Error, &[]),
    ];
    mark_fresh(&mut families);
    let rules: Vec<PkRule> = families.iter().map(|f| f.pk_rule).collect();
    use PkRule::{Enforce, Fresh};
    assert_eq!(rules, [Fresh, Enforce, Enforce, Enforce, Enforce, Fresh]);
}
