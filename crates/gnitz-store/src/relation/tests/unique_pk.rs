use super::enforce_unique_pk;
use crate::schema::TypeCode;
use crate::test_support::{
    make_batch_bytes, make_batch_opk, make_schema_pk_u64_payload_string, opk_pk, payload0_i64, pk_payload_schema,
    scratch_table, zset_of,
};

/// `(key index, weight, payload)`.
type Row = (usize, i64, i64);

/// A key's `(key index, net weight, live payload)` after a step.
type Live = (usize, i64, Option<i64>);

/// A push, the effective batch it must become, and each named key's state once
/// that is ingested.
type Step = (&'static str, &'static [Row], &'static [Row], &'static [Live]);

const SCRIPT: &[Step] = &[
    ("fresh +2", &[(1, 2, 100)], &[(1, 1, 100)], &[(1, 1, Some(100))]),
    (
        "upsert +2",
        &[(1, 2, 200)],
        &[(1, -1, 100), (1, 1, 200)],
        &[(1, 1, Some(200))],
    ),
    ("delete -3", &[(1, -3, 0)], &[(1, -1, 200)], &[(1, 0, None)]),
    (
        "delete of tombstoned and absent keys",
        &[(1, -1, 0), (2, -1, 0)],
        &[],
        &[(1, 0, None), (2, 0, None)],
    ),
    (
        "fresh + - +",
        &[(2, 1, 10), (2, -1, 10), (2, 1, 20)],
        &[(2, 1, 20)],
        &[(2, 1, Some(20))],
    ),
    (
        "fresh + +",
        &[(3, 1, 10), (3, 1, 20)],
        &[(3, 1, 20)],
        &[(3, 1, Some(20))],
    ),
    ("fresh + -", &[(4, 1, 10), (4, -1, 10)], &[], &[(4, 0, None)]),
    (
        "stored - +",
        &[(2, -1, 0), (2, 1, 30)],
        &[(2, -1, 20), (2, 1, 30)],
        &[(2, 1, Some(30))],
    ),
    (
        "stored + +",
        &[(2, 1, 40), (2, 1, 50)],
        &[(2, -1, 30), (2, 1, 50)],
        &[(2, 1, Some(50))],
    ),
    (
        "stored + -",
        &[(2, 1, 60), (2, -1, 60)],
        &[(2, -1, 50)],
        &[(2, 0, None)],
    ),
    (
        "zero row",
        &[(5, 1, 1), (6, 0, 2), (7, 1, 3)],
        &[(5, 1, 1), (7, 1, 3)],
        &[(5, 1, Some(1)), (6, 0, None), (7, 1, Some(3))],
    ),
    (
        "fresh passthrough",
        &[(10, 1, 1), (8, 1, 2), (9, 1, 3)],
        &[(10, 1, 1), (8, 1, 2), (9, 1, 3)],
        &[(8, 1, Some(2)), (9, 1, Some(3)), (10, 1, Some(1))],
    ),
];

/// One script of pushes, run at every PK shape. Every key's net weight stays in
/// `{0, 1}`, and the effective batch carries no cancelling pair.
#[test]
fn enforce_unique_pk_holds_at_every_pk_shape() {
    type Key = fn(usize) -> Vec<u128>;
    let cases: [(&str, &[TypeCode], Key); 4] = [
        ("u64", &[TypeCode::U64], |i| vec![i as u128]),
        // Negative keys, which OPK sign-flips.
        ("signed i64", &[TypeCode::I64], |i| vec![-(i as i64) as u64 as u128]),
        ("narrow u8", &[TypeCode::U8], |i| vec![i as u128]),
        ("wide 3xu64", &[TypeCode::U64; 3], |i| {
            vec![i as u128, i as u128 + 1, i as u128 + 2]
        }),
    ];

    for (name, pk_types, key) in cases {
        let schema = pk_payload_schema(pk_types);
        let dir = tempfile::tempdir().unwrap();
        let mut pt = scratch_table(dir.path(), schema);
        let k: Vec<Vec<u8>> = (0..=10).map(|i| opk_pk(&schema, &key(i))).collect();
        let batch = |rows: &[Row]| {
            let rows: Vec<(&[u8], i64, i64)> = rows.iter().map(|&(i, w, v)| (&k[i][..], w, v)).collect();
            make_batch_opk(&schema, &rows)
        };

        for &(step, push, want, live) in SCRIPT {
            let what = format!("{name}: {step}");
            let eff = enforce_unique_pk(&pt, batch(push));
            assert_eq!(zset_of(&eff, &schema), zset_of(&batch(want), &schema), "{what}");
            assert_eq!(eff.count, want.len(), "{what}: effective row count");
            pt.ingest_borrowed_batch(&eff).unwrap();
            for &(i, net, payload) in live {
                let (w, row) = pt.live_row_at(&k[i]);
                assert_eq!(
                    (w, row.as_ref().map(|r| payload0_i64(&r.run, r.row))),
                    (net, payload),
                    "{what}: key {i}"
                );
            }
        }
    }
}

/// Heap strings read back intact, from kept rows and from a stored row's
/// retraction alike.
#[test]
fn enforce_unique_pk_keeps_heap_strings_intact() {
    let schema = make_schema_pk_u64_payload_string();
    let dir = tempfile::tempdir().unwrap();
    let mut pt = scratch_table(dir.path(), schema);
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
}
