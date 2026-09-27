use super::enforce_unique_pk;
use crate::schema::{SchemaDescriptor, TypeCode};
use crate::storage::{Batch, Table};
use crate::test_support::{
    make_batch_bytes, make_batch_opk, make_schema_pk_u64_payload_string, opk_pk, payload0_i64, pk_payload_schema,
    read_german_string, scratch_table, stored_payload0_i64, zset_of,
};

/// The live payload the store holds at `key`, if any.
fn live_payload(pt: &Table, key: &[u8]) -> Option<i64> {
    pt.live_row_at(key).1.as_ref().map(stored_payload0_i64)
}

/// Push `rows` through the rule, check the effective batch against `expect` as a
/// Z-set and a row count, ingest it, and return it.
fn step(
    pt: &mut Table,
    schema: &SchemaDescriptor,
    rows: &[(&[u8], i64, i64)],
    expect: &[(&[u8], i64, i64)],
    what: &str,
) -> Batch {
    let eff = enforce_unique_pk(pt, make_batch_opk(schema, rows));
    assert_eq!(
        zset_of(&eff, schema),
        zset_of(&make_batch_opk(schema, expect), schema),
        "{what}: effective Z-set"
    );
    assert_eq!(eff.count, expect.len(), "{what}: effective row count");
    pt.ingest_borrowed_batch(&eff).unwrap();
    eff
}

/// One script of pushes, run at every PK shape.
#[test]
fn enforce_unique_pk_holds_at_every_pk_shape() {
    struct Case {
        name: &'static str,
        pk_types: &'static [TypeCode],
        /// Native PK column values of key `i`.
        key: fn(u8) -> Vec<u128>,
    }

    let cases = [
        Case {
            name: "u64",
            pk_types: &[TypeCode::U64],
            key: |i| vec![i as u128],
        },
        // Negative keys, which OPK sign-flips.
        Case {
            name: "signed i64",
            pk_types: &[TypeCode::I64],
            key: |i| vec![-(i as i64) as u64 as u128],
        },
        Case {
            name: "narrow u8",
            pk_types: &[TypeCode::U8],
            key: |i| vec![i as u128],
        },
        Case {
            name: "wide 3xu64",
            pk_types: &[TypeCode::U64; 3],
            key: |i| vec![i as u128, i as u128 + 1, i as u128 + 2],
        },
    ];

    for case in cases {
        let name = case.name;
        let schema = pk_payload_schema(case.pk_types);
        let dir = tempfile::tempdir().unwrap();
        let mut pt = scratch_table(dir.path().to_str().unwrap(), schema);
        let k: Vec<Vec<u8>> = (0..=10).map(|i| opk_pk(&schema, &(case.key)(i))).collect();
        let (k1, k2, k3, k4) = (&k[1][..], &k[2][..], &k[3][..], &k[4][..]);
        let s = &schema;

        step(
            &mut pt,
            s,
            &[(k1, 2, 100)],
            &[(k1, 1, 100)],
            &format!("{name}: fresh +2"),
        );
        assert_eq!(live_payload(&pt, k1), Some(100), "{name}");

        step(
            &mut pt,
            s,
            &[(k1, 2, 200)],
            &[(k1, -1, 100), (k1, 1, 200)],
            &format!("{name}: upsert +2"),
        );
        assert_eq!(live_payload(&pt, k1), Some(200), "{name}");

        step(
            &mut pt,
            s,
            &[(k1, -3, 0)],
            &[(k1, -1, 200)],
            &format!("{name}: delete -3"),
        );
        assert_eq!(live_payload(&pt, k1), None, "{name}");

        step(
            &mut pt,
            s,
            &[(k1, -1, 0), (k2, -1, 0)],
            &[],
            &format!("{name}: delete of tombstoned and absent keys"),
        );
        assert_eq!(live_payload(&pt, k1), None, "{name}");
        assert_eq!(live_payload(&pt, k2), None, "{name}");

        step(
            &mut pt,
            s,
            &[(k2, 1, 10), (k2, -1, 10), (k2, 1, 20)],
            &[(k2, 1, 20)],
            &format!("{name}: fresh + - +"),
        );
        assert_eq!(live_payload(&pt, k2), Some(20), "{name}");

        step(
            &mut pt,
            s,
            &[(k3, 1, 10), (k3, 1, 20)],
            &[(k3, 1, 20)],
            &format!("{name}: fresh + +"),
        );
        assert_eq!(live_payload(&pt, k3), Some(20), "{name}");

        step(
            &mut pt,
            s,
            &[(k4, 1, 10), (k4, -1, 10)],
            &[],
            &format!("{name}: fresh + -"),
        );
        assert_eq!(live_payload(&pt, k4), None, "{name}");

        step(
            &mut pt,
            s,
            &[(k2, -1, 0), (k2, 1, 30)],
            &[(k2, -1, 20), (k2, 1, 30)],
            &format!("{name}: stored - +"),
        );
        assert_eq!(live_payload(&pt, k2), Some(30), "{name}");

        step(
            &mut pt,
            s,
            &[(k2, 1, 40), (k2, 1, 50)],
            &[(k2, -1, 30), (k2, 1, 50)],
            &format!("{name}: stored + +"),
        );
        assert_eq!(live_payload(&pt, k2), Some(50), "{name}");

        step(
            &mut pt,
            s,
            &[(&k[5], 1, 1), (&k[6], 0, 2), (&k[7], 1, 3)],
            &[(&k[5], 1, 1), (&k[7], 1, 3)],
            &format!("{name}: zero row"),
        );
        assert_eq!(live_payload(&pt, &k[6]), None, "{name}");

        let fresh: [(&[u8], i64, i64); 3] = [(&k[10], 1, 1), (&k[8], 1, 2), (&k[9], 1, 3)];
        let eff = step(&mut pt, s, &fresh, &fresh, &format!("{name}: fresh passthrough"));
        for (r, &(pk, w, v)) in fresh.iter().enumerate() {
            assert_eq!(eff.get_pk_bytes(r), pk, "{name}: passthrough row {r} PK");
            assert_eq!(eff.get_weight(r), w, "{name}: passthrough row {r} weight");
            let got = payload0_i64(&eff, r);
            assert_eq!(got, v, "{name}: passthrough row {r} payload");
        }
    }
}

/// Heap strings read back intact, from kept rows and from a stored row's
/// retraction alike.
#[test]
fn enforce_unique_pk_keeps_heap_strings_intact() {
    let schema = make_schema_pk_u64_payload_string();
    let dir = tempfile::tempdir().unwrap();
    let mut pt = scratch_table(dir.path().to_str().unwrap(), schema);
    let long1: &[u8] = b"the stored payload, long enough for the heap";
    let long2: &[u8] = b"the replacing payload, also on the heap";
    let long3: &[u8] = b"a fresh key's payload, on the heap as well";

    let seed = enforce_unique_pk(&pt, make_batch_bytes(&schema, &[(1, 1, long1)]));
    pt.ingest_owned_batch(seed).unwrap();

    let eff = enforce_unique_pk(
        &pt,
        make_batch_bytes(&schema, &[(1, 1, long2), (2, -1, b""), (3, 1, long3)]),
    );
    assert_eq!(
        zset_of(&eff, &schema),
        zset_of(
            &make_batch_bytes(&schema, &[(1, 1, long2), (1, -1, long1), (3, 1, long3)]),
            &schema
        ),
    );
    assert_eq!(eff.count, 3);
    let rows: Vec<(i64, Vec<u8>)> = (0..eff.count)
        .map(|r| (eff.get_weight(r), read_german_string(&eff, 0, r)))
        .collect();
    assert_eq!(
        rows,
        vec![(1, long2.to_vec()), (1, long3.to_vec()), (-1, long1.to_vec())]
    );
}
