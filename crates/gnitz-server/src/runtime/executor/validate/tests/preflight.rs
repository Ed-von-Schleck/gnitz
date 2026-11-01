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
