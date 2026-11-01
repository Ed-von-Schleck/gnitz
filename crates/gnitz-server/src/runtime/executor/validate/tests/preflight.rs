use super::*;
use crate::test_support::{opk_pk, pk_only_schema, u64_i64_cols};
use gnitz_wire::TypeCode;

/// A write to a parent referenced through its lone PK column probes nothing
/// unless it deletes: an upsert resolves with no worker answering, and a
/// retraction waits on the child-index probe of the key it removes.
#[test]
fn a_lone_pk_parent_probes_only_for_the_keys_it_deletes() {
    use crate::runtime::master::fixtures::test_dispatcher;
    use crate::runtime::test_support::empty_catalog;
    use crate::runtime::test_support::try_poll_once;
    use crate::test_support::{fk_child_cols, make_batch_raw};

    let (_tmp, mut engine) = empty_catalog();
    let parent = engine.create_table("public.parent", &u64_i64_cols(), &[0]).unwrap();
    engine
        .create_table("public.child", &fk_child_cols(parent), &[0])
        .unwrap();
    let schema = engine.registry.relation(parent).unwrap().schema();
    let (disp, _) = test_dispatcher(1, engine);
    let checks = Checks::new();

    let write = |rows: &[(u64, i64, i64)]| {
        vec![TxnFamily {
            tid: parent,
            mode: WireConflictMode::Update,
            rows: FamilyRows::Built(make_batch_raw(&schema, rows)),
        }]
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

/// Each PK column decodes at its own offset and width, signed and
/// wider-than-16-byte keys included.
#[test]
fn format_pk_bytes_renders_every_pk_column_from_its_opk_image() {
    for (tc, v, want) in [
        (TypeCode::I8, -5i128, "-5"),
        (TypeCode::I16, -300, "-300"),
        (TypeCode::I32, -70_000, "-70000"),
        (TypeCode::I64, i64::MIN as i128, "-9223372036854775808"),
        (TypeCode::U8, 200, "200"),
        (TypeCode::U64, u64::MAX as i128, "18446744073709551615"),
        (
            TypeCode::U128,
            u128::MAX as i128,
            "340282366920938463463374607431768211455",
        ),
        (TypeCode::I128, -1, "-1"),
        (TypeCode::I128, i128::MIN, "-170141183460469231731687303715884105728"),
    ] {
        let schema = pk_only_schema(&[tc]);
        assert_eq!(
            format_pk_bytes(&schema, &opk_pk(&schema, &[v as u128])),
            want,
            "type {tc}"
        );
    }

    let uuid = pk_only_schema(&[TypeCode::UUID]);
    let v = 0x550e8400_e29b_41d4_a716_446655440000u128;
    assert_eq!(
        format_pk_bytes(&uuid, &opk_pk(&uuid, &[v])),
        "550e8400-e29b-41d4-a716-446655440000",
        "a UUID renders from all 128 bits, not a truncated low word",
    );

    // 24-byte compound PK: every column read at its own offset.
    let wide = pk_only_schema(&[TypeCode::U64; 3]);
    assert_eq!(format_pk_bytes(&wide, &opk_pk(&wide, &[7, 8, 9])), "7, 8, 9");
}

/// U-SEC's in-bundle duplicate verdict over a table with a UNIQUE index on its
/// nullable payload column, whose filter is seeded empty so no write probes:
/// one row per PK (packed a column at a time once there are enough of them)
/// and a PK touched twice (packed per surviving row) decide alike, across
/// families, deletes, and NULLs, which are unindexed.
#[test]
fn a_unique_index_refuses_exactly_the_spans_two_surviving_rows_claim() {
    use super::super::unique_filter::UniqueFilter;
    use crate::catalog::SysFamily;
    use crate::runtime::master::fixtures::test_dispatcher;
    use crate::runtime::test_support::{empty_catalog, try_poll_once};
    use crate::test_support::{apply_ddl, idx_tab_batch, nullable_def};
    use gnitz_zset::repr::BatchBuilder;

    let (_tmp, mut engine) = empty_catalog();
    let cols = [u64_i64_cols()[0].clone(), nullable_def("v", TypeCode::I64)];
    let tid = engine.create_table("public.t", &cols, &[0]).unwrap();
    let id = engine.allocate_ids(1).unwrap();
    let idx = idx_tab_batch(id, tid, &[1], "public__t__idx_v", true, 1);
    apply_ddl(&mut engine, [(SysFamily::Index, idx)]).unwrap();
    let schema = engine.registry.relation(tid).unwrap().schema();
    let (disp, _) = test_dispatcher(1, engine);
    let checks = Checks::new();
    checks.unique_filter_seed(tid, PkColList::from_slice(&[1]), UniqueFilter::new());

    // `(pk, weight, value)` rows per family; `None` is a NULL value.
    type Rows<'r> = &'r [(u64, i64, Option<i64>)];
    let family = |rows: Rows<'_>| {
        let mut bb = BatchBuilder::new(&schema);
        for &(pk, w, v) in rows {
            bb.begin_row(pk as u128, w);
            bb.put_opt_int(v.map(|v| v as u128));
            bb.end_row();
        }
        TxnFamily {
            tid,
            mode: WireConflictMode::Update,
            rows: FamilyRows::Built(bb.finish()),
        }
    };
    let refused = |fams: &[Rows<'_>]| {
        let mut families: Vec<TxnFamily> = fams.iter().map(|&r| family(r)).collect();
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
