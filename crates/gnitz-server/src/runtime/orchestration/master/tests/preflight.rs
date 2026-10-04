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
        let mut registry = RelationRegistry::new("", gnitz_zset::schema::Slot::SOLO, StoreConfig::default());
        let spec = RelationSpec {
            id: 16,
            kind: RelationKind::Stream,
            schema,
            placement,
        };
        registry.register(spec).unwrap();
        assert_eq!(probe_placement(registry.relation(16).unwrap()), want, "{placement:?}");
    }
}

#[test]
fn row_of_names_the_row_that_is_a_key() {
    let schema = pk_only_schema(&[TypeCode::U64, TypeCode::U64]);
    let key = |a: u64, b: u64| opk_pk(&schema, &[a as u128, b as u128]);
    let rows = [key(1, 0), key(3, 1), key(5, 9), key(7, 0)];
    let keys = build_check_batch_pk_bytes(&schema, rows.iter().map(|k| &k[..]));
    assert_eq!(row_of(&keys, &key(3, 1)), Some(1), "a present key");
    assert_eq!(row_of(&keys, &key(3, 2)), None, "an absent key");
    assert_eq!(row_of(&keys, &key(9, 0)), None, "an absent key past the last row");
    assert_eq!(row_of(&keys, &key(0, 0)), None, "a key below every row");
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
        [TxnFamily {
            tid: parent,
            mode: WireConflictMode::Update,
            batch: make_batch_raw(&schema, rows),
        }]
    };
    let upsert = write(&[(1, 1, 10), (2, 1, 20)]);
    assert!(matches!(
        try_poll_once(disp.validate_txn_distributed(&upsert)),
        Some(Ok(()))
    ));
    let retraction = write(&[(1, -1, 10)]);
    assert!(try_poll_once(disp.validate_txn_distributed(&retraction)).is_none());

    drop(disp);
    engine.close();
}
