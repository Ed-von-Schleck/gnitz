use super::*;
use crate::runtime::test_support::try_poll_once;
use crate::test_support::{make_batch, make_schema_u64_i64, opk_pk, pk_only_schema, u64_i64_cols};
use gnitz_wire::TypeCode;

/// Seed the filter of the unique index on `cols` of `tid` empty, as an empty
/// table's is.
fn seed_empty_filter(checks: &Checks, tid: u64, cols: PkColList) {
    checks
        .unique_filters
        .borrow_mut()
        .insert((tid, cols), UniqueFilter::default());
}

/// A write to a parent referenced through its lone PK column probes nothing
/// unless it deletes: an upsert resolves with no worker answering, and a
/// retraction waits on the child-index probe of the key it removes.
#[test]
fn a_lone_pk_parent_probes_only_for_the_keys_it_deletes() {
    use crate::runtime::master::fixtures::test_dispatcher;
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
            FamilyRows::Built(make_batch(&schema, rows)),
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
    let (_tmp, disp, tid, schema, cols) = uniquely_indexed_table();
    let checks = Checks::new();
    let refused = |fams: &[Rows<'_>]| {
        seed_empty_filter(&checks, tid, cols);
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
    let (_tmp, disp, tid, schema, cols) = uniquely_indexed_table();
    let checks = Checks::new();
    seed_empty_filter(&checks, tid, cols);
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

/// Only a table whose every family is an Error family of positive weights is
/// fresh: an Update family, a retraction or a zero weight keeps the enforced
/// rule on every family of its table.
#[test]
fn only_a_table_of_positive_error_families_is_fresh() {
    use crate::test_support::empty_catalog;
    use WireConflictMode::{Error, Update};

    let (_tmp, mut engine) = empty_catalog(1);
    let tids: Vec<u64> = (1..=7)
        .map(|i| {
            engine
                .create_table(&format!("public.t{i}"), &u64_i64_cols(), &[0])
                .unwrap()
        })
        .collect();
    let family = |t: usize, mode, first: u64, weights: &[i64]| {
        let rows: Vec<(u64, i64, i64)> = (weights.iter().enumerate())
            .map(|(i, &w)| (first + i as u64, w, 0))
            .collect();
        TxnFamily::new(
            tids[t],
            mode,
            FamilyRows::Built(make_batch(&make_schema_u64_i64(), &rows)),
        )
    };
    let families = vec![
        family(0, Error, 0, &[1, 1]),
        family(1, Update, 0, &[1]),
        family(2, Error, 0, &[1, -1]),
        family(3, Error, 0, &[1]),
        family(3, Error, 1, &[1]),
        family(4, Error, 0, &[]),
        family(5, Error, 0, &[1]),
        family(5, Update, 1, &[1]),
        family(6, Error, 0, &[1, 0]),
    ];
    let b = TxnBundle::new(&engine, &families).unwrap_or_else(|e| panic!("{}", e.text));
    let fresh: Vec<bool> = (tids.iter()).map(|&tid| b.get(tid).is_some_and(|t| t.fresh)).collect();
    assert_eq!(fresh, [true, false, false, true, true, false, false]);
}

/// An admitted write that caps its index's filter marks it churned, so the
/// next write to the index rebuilds it from the live index; a filter with no
/// room proves nothing, so that write probes.
#[test]
fn an_admitted_write_that_caps_its_filter_marks_it_churned() {
    let (_tmp, disp, tid, schema, cols) = uniquely_indexed_table();
    let checks = Checks { unique_filter_cap: 1, ..Checks::new() };
    seed_empty_filter(&checks, tid, cols);
    let validate = |rows: Rows<'_>| {
        let mut families = vec![update_family(tid, &schema, rows)];
        try_poll_once(checks.validate_txn(&disp, &mut families))
    };

    assert!(matches!(validate(&[(1, 1, Some(1)), (2, 1, Some(2))]), Some(Ok(()))));
    assert!(matches!(
        checks.unique_filters.borrow()[&(tid, cols)],
        UniqueFilter::Churned
    ));
    assert!(validate(&[(3, 1, Some(3))]).is_none());

    disp.into_catalog().close();
}

/// A key two Error families insert is a duplicate in the batch where they are
/// one run, consecutive families of one table and mode, and a committed key
/// where a family of another table or mode parts them.
#[test]
fn a_key_inserted_twice_clashes_in_the_batch_only_within_one_run() {
    use crate::test_support::empty_catalog;
    use WireConflictMode::{Error, Update};

    let (_tmp, mut engine) = empty_catalog(1);
    let t = engine.create_table("public.t", &u64_i64_cols(), &[0]).unwrap();
    let u = engine.create_table("public.u", &u64_i64_cols(), &[0]).unwrap();
    let schema = engine.registry.relation(t).unwrap().schema();
    let family = |tid, mode, pk| TxnFamily::new(tid, mode, FamilyRows::Built(make_batch(&schema, &[(pk, 1, 0)])));
    let clash = |families: &[TxnFamily]| match TxnBundle::new(&engine, families) {
        Ok(_) => "none",
        Err(e) if e.text.contains("Batch contains multiple rows") => "in batch",
        Err(e) if e.text.contains("already exists") => "committed",
        Err(e) => panic!("{}", e.text),
    };

    assert_eq!(clash(&[family(t, Error, 1), family(t, Error, 1)]), "in batch");
    assert_eq!(
        clash(&[family(t, Error, 1), family(t, Error, 2), family(t, Error, 1)]),
        "in batch"
    );
    assert_eq!(
        clash(&[family(t, Error, 1), family(u, Error, 1), family(t, Error, 1)]),
        "committed"
    );
    assert_eq!(
        clash(&[family(t, Error, 1), family(t, Update, 2), family(t, Error, 1)]),
        "committed"
    );
    assert_eq!(clash(&[family(t, Update, 1), family(t, Error, 1)]), "committed");
    assert_eq!(clash(&[family(t, Error, 1), family(u, Error, 1)]), "none");

    engine.close();
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
        (TypeCode::Bool, 1, "true"),
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
            format_pk_bytes(&schema, &opk_pk(&schema, &[v as u128]), &[]),
            want,
            "type {tc}"
        );
    }

    let uuid = pk_only_schema(&[TypeCode::UUID]);
    let v = 0x550e8400_e29b_41d4_a716_446655440000u128;
    assert_eq!(
        format_pk_bytes(&uuid, &opk_pk(&uuid, &[v]), &[]),
        "550e8400-e29b-41d4-a716-446655440000",
        "a UUID renders from all 128 bits, not a truncated low word",
    );

    // 24-byte compound PK: every column read at its own offset.
    let wide = pk_only_schema(&[TypeCode::U64; 3]);
    assert_eq!(format_pk_bytes(&wide, &opk_pk(&wide, &[7, 8, 9]), &[]), "7, 8, 9");
}

/// A calendar key renders as its date or timestamp, and a DECIMAL key at the
/// scale its catalog column carries; a DECIMAL with no scale given as its
/// stored integer.
#[test]
fn format_pk_bytes_spells_a_typed_key_as_a_client_prints_it() {
    let day = gnitz_wire::MICROS_PER_DAY;
    let (date, ts, dec) = (TypeCode::Date, TypeCode::Timestamp, TypeCode::Decimal);
    let schema = pk_only_schema(&[date, ts, dec]);
    let key = opk_pk(
        &schema,
        &[
            (-1i32) as u32 as u128,
            (19_723 * day + day / 2 + 500_000) as u64 as u128,
            (-110i64) as u64 as u128,
        ],
    );
    assert_eq!(
        format_pk_bytes(&schema, &key, &[0, 0, 2]),
        "1969-12-31, 2024-01-01 12:00:00.5, -1.10"
    );
    assert_eq!(
        format_pk_bytes(&schema, &key, &[]),
        "1969-12-31, 2024-01-01 12:00:00.5, -110"
    );
}
