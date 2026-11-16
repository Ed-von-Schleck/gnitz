use super::{PkRule, enforce_unique_pk, under_pk_rule};
use crate::storage::flush_barrier;
use crate::test_support::{
    make_batch_bytes, make_batch_opk, make_schema_pk_u64_payload_string, opk_pk, payload0_i64, pk_payload_schema,
    scratch_table, table_shards, zset_of,
};
use gnitz_foundation::rng::Rng;
use gnitz_wire::TypeCode;
use gnitz_zset::repr::Batch;
use gnitz_zset::schema::SchemaDescriptor;

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
    ("identical upsert", &[(7, 1, 3)], &[], &[(7, 1, Some(3))]),
    (
        "fresh passthrough",
        &[(10, 1, 1), (8, 1, 2), (9, 1, 3)],
        &[(10, 1, 1), (8, 1, 2), (9, 1, 3)],
        &[(8, 1, Some(2)), (9, 1, Some(3)), (10, 1, Some(1))],
    ),
];

/// One script of pushes, run at every PK shape, its stored rows in RAM and in
/// shards. Every key's net weight stays in `{0, 1}`, and the effective batch is
/// exactly the script's, row for row: an update that rewrites a row unchanged
/// cancels to nothing.
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

    for ((name, pk_types, key), flushed) in cases.into_iter().flat_map(|c| [(c, false), (c, true)]) {
        let schema = pk_payload_schema(pk_types);
        let dir = tempfile::tempdir().unwrap();
        let mut pt = scratch_table(dir.path(), schema);
        let k: Vec<Vec<u8>> = (0..=10).map(|i| opk_pk(&schema, &key(i))).collect();
        let batch = |rows: &[Row]| {
            let rows: Vec<(&[u8], i64, i64)> = rows.iter().map(|&(i, w, v)| (&k[i][..], w, v)).collect();
            make_batch_opk(&schema, &rows)
        };

        for &(step, push, want, live) in SCRIPT {
            let what = format!("{name}, flushed {flushed}: {step}");
            let eff = enforce_unique_pk(&pt, batch(push));
            assert_eq!(zset_of(&eff, &schema), zset_of(&batch(want), &schema), "{what}");
            assert_eq!(eff.len(), want.len(), "{what}: effective row count");
            pt.ingest(eff).unwrap();
            if flushed {
                flush_barrier([&mut pt], 0).unwrap();
            }
            for &(i, net, payload) in live {
                let row = pt.unique_live_row_at(&k[i]);
                assert_eq!(
                    (
                        pt.net_weight_at(&k[i]),
                        row.as_ref().map(|r| r.source()).map(|(src, i)| payload0_i64(src, i))
                    ),
                    (net, payload),
                    "{what}: key {i}"
                );
            }
        }
    }
}

/// Heap strings read back intact, from kept rows and from the retraction of a
/// row stored in a shard alike.
#[test]
fn enforce_unique_pk_keeps_heap_strings_intact() {
    let schema = make_schema_pk_u64_payload_string();
    let dir = tempfile::tempdir().unwrap();
    let mut pt = scratch_table(dir.path(), schema);
    let long1: &[u8] = b"the stored payload, long enough for the heap";
    let long2: &[u8] = b"the replacing payload, also on the heap";
    let long3: &[u8] = b"a fresh key's payload, on the heap as well";

    let seed = enforce_unique_pk(&pt, make_batch_bytes(&schema, &[(1, 1, long1)]));
    pt.ingest(seed).unwrap();
    flush_barrier([&mut pt], 0).unwrap();
    assert!(!table_shards(&pt).is_empty());

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
    assert_eq!(eff.len(), 3);
}

/// A batch in PK order takes the run walk and any other batch the map; both
/// must give the same effective batch, weights included. Random batches with
/// repeated keys, mixed weights and differing payloads are enforced as given
/// and stably sorted by PK, which keeps each key's rows in push order, against
/// one store that the effective batch then advances.
#[test]
fn enforce_unique_pk_ordered_walk_matches_map() {
    let schema = pk_payload_schema(&[TypeCode::I64]);
    let dir = tempfile::tempdir().unwrap();
    let mut pt = scratch_table(dir.path(), schema);
    let k: Vec<Vec<u8>> = (0..12i64).map(|i| opk_pk(&schema, &[(i - 6) as u64 as u128])).collect();
    let mut rng = Rng::new(0x2545_F491_4F6C_DD1D);

    let mut unordered_pushes = 0;
    for round in 0..300 {
        let len = 1 + rng.gen_range(24) as usize;
        let rows: Vec<(usize, i64, i64)> = (0..len)
            .map(|_| {
                (
                    rng.gen_range(k.len() as u64) as usize,
                    rng.gen_range(5) as i64 - 2,
                    rng.gen_range(3) as i64,
                )
            })
            .collect();
        let mut sorted = rows.clone();
        sorted.sort_by(|a, b| k[a.0].cmp(&k[b.0]));
        let batch = |rows: &[(usize, i64, i64)]| {
            let rows: Vec<(&[u8], i64, i64)> = rows.iter().map(|&(i, w, v)| (&k[i][..], w, v)).collect();
            make_batch_opk(&schema, &rows)
        };
        unordered_pushes += usize::from(rows != sorted);

        let from_map = enforce_unique_pk(&pt, batch(&rows));
        let from_walk = enforce_unique_pk(&pt, batch(&sorted));
        assert_eq!(
            zset_of(&from_map, &schema),
            zset_of(&from_walk, &schema),
            "round {round}: {rows:?}"
        );
        // The walk's batch is claimed consolidated, which a debug build verifies.
        assert!(from_walk.is_consolidated(), "round {round}: walk output unconsolidated");
        assert_eq!(
            from_map.into_consolidated().len(),
            from_walk.len(),
            "round {round}: effective row count"
        );
        pt.ingest(from_walk).unwrap();
        if round % 50 == 49 {
            flush_barrier([&mut pt], 0).unwrap();
        }
    }
    assert!(unordered_pushes > 200, "{unordered_pushes} pushes took the map");
}

/// A store holding keys `0..4`, and the OPK of keys `0..8`.
fn fresh_fixture() -> (tempfile::TempDir, crate::storage::Table, SchemaDescriptor, Vec<Vec<u8>>) {
    let schema = pk_payload_schema(&[TypeCode::U64]);
    let dir = tempfile::tempdir().unwrap();
    let mut pt = scratch_table(dir.path(), schema);
    let k: Vec<Vec<u8>> = (0..8u128).map(|i| opk_pk(&schema, &[i])).collect();
    let held: Vec<(&[u8], i64, i64)> = (0..4).map(|i| (&k[i][..], 1, i as i64)).collect();
    pt.ingest(make_batch_opk(&schema, &held)).unwrap();
    (dir, pt, schema, k)
}

/// A batch of `rows`, `(key index, weight, payload)`, over keys `k`.
fn rows_of(schema: &SchemaDescriptor, k: &[Vec<u8>], rows: &[Row]) -> Batch {
    let rows: Vec<(&[u8], i64, i64)> = rows.iter().map(|&(i, w, v)| (&k[i][..], w, v)).collect();
    make_batch_opk(schema, &rows)
}

/// On a batch whose keys are distinct, weighted 1 and not held — unsorted
/// too — the fresh rule's effective batch is the enforced rule's.
#[test]
fn fresh_rule_matches_enforce_on_fresh_keys() {
    let (_dir, pt, schema, k) = fresh_fixture();
    for rows in [&[(6, 1, 60), (4, 1, 40), (7, 1, 70)][..], &[(5, 1, 50)], &[]] {
        let fresh = under_pk_rule(&pt, rows_of(&schema, &k, rows), PkRule::Fresh);
        let enforced = under_pk_rule(&pt, rows_of(&schema, &k, rows), PkRule::Enforce);
        assert_eq!(zset_of(&fresh, &schema), zset_of(&enforced, &schema), "{rows:?}");
        assert_eq!(fresh.len(), rows.len(), "{rows:?}");
    }
}

/// A debug build refuses a batch admitted as fresh that meets a held key.
#[cfg(debug_assertions)]
#[test]
#[should_panic(expected = "admitted as fresh")]
fn fresh_rule_refuses_a_held_key() {
    let (_dir, pt, schema, k) = fresh_fixture();
    under_pk_rule(&pt, rows_of(&schema, &k, &[(5, 1, 50), (2, 1, 20)]), PkRule::Fresh);
}

/// A debug build refuses a batch admitted as fresh that repeats a key.
#[cfg(debug_assertions)]
#[test]
#[should_panic(expected = "admitted as fresh")]
fn fresh_rule_refuses_a_repeated_key() {
    let (_dir, pt, schema, k) = fresh_fixture();
    under_pk_rule(&pt, rows_of(&schema, &k, &[(5, 1, 50), (5, 1, 51)]), PkRule::Fresh);
}

/// A debug build refuses a batch admitted as fresh that weighs a row other than 1.
#[cfg(debug_assertions)]
#[test]
#[should_panic(expected = "admitted as fresh")]
fn fresh_rule_refuses_a_weight_other_than_one() {
    let (_dir, pt, schema, k) = fresh_fixture();
    under_pk_rule(&pt, rows_of(&schema, &k, &[(5, 2, 50)]), PkRule::Fresh);
}
