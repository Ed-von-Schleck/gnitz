use super::*;
use crate::test_support::{opk_pk, pk_only_schema, pk_payload_schema};
use gnitz_wire::TypeCode;

/// Row `j` of the check batch holds `keys[j]` once the call returns — the order
/// F1/F2 map replies back by — each image stored as the key column's bytes.
#[test]
fn build_check_batch_sorts_the_keys_into_its_rows() {
    for tc in [TypeCode::I32, TypeCode::I64, TypeCode::I128] {
        let span = pk_only_schema(&[tc]);
        let mut keys = [5i32, -1, 0].map(|v| gnitz_wire::key_image(tc, v as u128));

        let batch = build_check_batch(&span, &mut keys);

        assert!(keys.is_sorted());
        let rows: Vec<&[u8]> = (0..batch.len()).map(|j| batch.get_pk_bytes(j)).collect();
        let want: Vec<Vec<u8>> = [-1i64, 0, 5].iter().map(|&v| opk_pk(&span, &[v as u128])).collect();
        assert_eq!(rows, want, "{tc}");
    }
}

/// An own-PK probe goes where the relation's keys are held. A replicated
/// relation's is spread over the full PK instead, since every worker holds it
/// whole.
#[test]
fn an_own_pk_probe_follows_a_keyed_placement_and_spreads_a_replicated_one() {
    use gnitz_store::relation::{RelationKind, RelationRegistry, RelationSpec, StoreConfig};
    let schema = pk_payload_schema(&[TypeCode::U64, TypeCode::U64]);
    let (full, prefix) = (Placement::full_pk(&schema), Placement::keyed(&schema, 1));
    for (placement, want) in [(full, full), (prefix, prefix), (Placement::Replicated, full)] {
        // A stream is storeless, so it registers without a directory.
        let mut registry = RelationRegistry::new("", gnitz_zset::algebra::Slot::SOLO, StoreConfig::default());
        let spec = RelationSpec {
            id: 16,
            kind: RelationKind::Stream,
            schema,
            placement,
            pk_repeats: false,
        };
        registry.register(spec).unwrap();
        assert_eq!(probe_placement(registry.relation(16).unwrap()), want, "{placement:?}");
    }
}

/// A write to a parent referenced through its lone PK column probes nothing
/// unless it deletes: an upsert resolves with no worker answering, and a
/// retraction waits on the child-index probe of the key it removes.
#[test]
fn a_lone_pk_parent_probes_only_for_the_keys_it_deletes() {
    use super::super::fixtures::test_dispatcher;
    use crate::runtime::test_support::try_poll_once;
    use crate::test_support::{col_def, fk_def, make_batch_raw};

    let tmp = tempfile::tempdir().unwrap();
    let mut engine = CatalogEngine::open(tmp.path().to_str().unwrap(), 1).unwrap();
    let cols = [col_def("pid", TypeCode::U64), col_def("v", TypeCode::I64)];
    let parent = engine.create_table("public.parent", &cols, &[0]).unwrap();
    let cols = [col_def("cid", TypeCode::U64), fk_def("fk", TypeCode::U64, parent, 0)];
    engine.create_table("public.child", &cols, &[0]).unwrap();
    let schema = engine.registry.relation(parent).unwrap().schema();
    let (disp, _) = test_dispatcher(vec![0], &mut engine);

    let write = |rows: &[(u64, i64, i64)]| {
        vec![TxnFamily {
            tid: parent,
            mode: WireConflictMode::Update,
            batch: make_batch_raw(&schema, rows),
        }]
    };
    let mut upsert = write(&[(1, 1, 10), (2, 1, 20)]);
    assert!(matches!(
        try_poll_once(disp.validate_txn_distributed(&mut upsert)),
        Some(Ok(()))
    ));
    let mut retraction = write(&[(1, -1, 10)]);
    assert!(try_poll_once(disp.validate_txn_distributed(&mut retraction)).is_none());

    drop(disp);
    engine.close();
}

/// `format_pk_bytes` decodes each column out of the OPK image at its own
/// offset and width. The signed arms are what a native-LE read would get
/// wrong (OPK flips the sign bit), and a compound PK wider than 16 bytes is
/// what a `u128` key form could not carry at all.
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
