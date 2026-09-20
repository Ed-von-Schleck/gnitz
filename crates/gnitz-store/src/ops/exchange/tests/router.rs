use super::*;
use crate::ops::group_key::GroupKeyCols;
use crate::schema::{type_code, SchemaColumn, SchemaDescriptor};
use crate::test_support::{
    make_batch, make_batch_bytes, make_batch_raw, make_schema_pk_u64_payload_string, make_schema_u64_i64,
};

#[test]
fn test_worker_filter_keeps_only_this_workers_rows() {
    let schema = make_schema_u64_i64();
    let num_workers = 4u32;
    let rows: Vec<(u64, i64, i64)> = (0..40u64).map(|i| (i * 7 + 1, 1, i as i64)).collect();
    let batch = make_batch(&schema, &rows);

    // Each row kept by exactly the worker that owns its PK; the
    // union across workers is the whole batch with no duplication.
    let mut total_kept = 0usize;
    for wid in 0..num_workers {
        let out = op_worker_filter(&batch, wid, num_workers);
        total_kept += out.count;
        for r in 0..out.count {
            let pk = out.get_pk_bytes(r);
            let owner = worker_for_pk_bytes(pk, num_workers as usize);
            assert_eq!(owner as u32, wid, "row routed to wrong worker");
        }
    }
    assert_eq!(total_kept, batch.count, "worker filter dropped or duplicated rows");
}

#[test]
fn test_worker_filter_single_worker_keeps_all() {
    let schema = make_schema_u64_i64();
    let batch = make_batch(&schema, &[(1, 1, 10), (2, 1, 20), (3, 1, 30)]);
    let out = op_worker_filter(&batch, 0, 1);
    assert_eq!(out.count, 3, "(0, 1) must keep every row");
}

#[test]
fn test_worker_filter_empty_in_empty_out() {
    let schema = make_schema_u64_i64();
    let batch = make_batch(&schema, &[]);
    let out = op_worker_filter(&batch, 1, 4);
    assert_eq!(out.count, 0);
}

/// A `JoinKey` scatter's packed route equals the per-type OPK image of the same
/// column — an integer's `opk_image`, a string's `german_string_promote_key`, a
/// compound-PK sub-column's group fold — which is what co-partitions the delta
/// scatter with the reindexed trace. The NULL arms route by the
/// canonically-zeroed key slot, where the reindex Map stamps them too.
#[test]
fn test_scatter_key_packed_matches_image_routing() {
    use crate::storage::MemBatch;

    // Run the `JoinKey` `ScatterKey` over `row` and return its worker.
    const NW: usize = 4;
    fn packed(schema: &SchemaDescriptor, cols: &[u32], mb: &MemBatch, row: usize) -> usize {
        let key: Vec<gnitz_wire::ReindexSlot> = cols.iter().map(|&c| (c, None)).collect();
        let mut sk = ScatterKey::new(ScatterSpec::JoinKey(&key), schema, NW).expect("the fixture key routes");
        sk.worker(mb, row)
    }

    // (1) non-null I64 payload; (2) NULL I64 payload (nullable col).
    {
        let schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::I64, 1),
            ],
            &[0],
        );
        let mut b = Batch::with_capacity(&schema, 2);
        b.extend_pk(1u128);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &(-7i64).to_le_bytes());
        b.count += 1;
        b.extend_pk(2u128);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&1u64.to_le_bytes()); // col1 NULL, slot zeroed
        b.extend_col(0, &0i64.to_le_bytes());
        b.count += 1;
        let mb = b.as_mem_batch();
        for row in 0..2 {
            // Image: routable-int → loc.opk_image → worker_for_key (null-blind).
            let image = worker_for_key(schema.locate(1).opk_image(&mb, row), NW);
            assert_eq!(packed(&schema, &[1], &mb, row), image, "I64 row {row}");
        }
    }

    // (3) STRING payload; (4) NULL STRING payload.
    {
        let schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::STRING, 1),
            ],
            &[0],
        );
        let mut b = Batch::with_capacity(&schema, 2);
        b.extend_pk(1u128);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col_blob(0, b"abc");
        b.count += 1;
        b.extend_pk(2u128);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&1u64.to_le_bytes()); // col1 NULL, zeroed struct
        b.extend_col(0, &[0u8; 16]);
        b.count += 1;
        let mb = b.as_mem_batch();
        for row in 0..2 {
            // Image: string → german_string_promote_key → worker_for_key.
            let image = worker_for_key(
                crate::schema::key::german_string_promote_key(mb.get_col_ptr(row, 0, 16), mb.blob),
                NW,
            );
            assert_eq!(packed(&schema, &[1], &mb, row), image, "STRING row {row}");
        }
    }

    // (5) U128 payload key: `is_pk_eligible` includes U128, so the image comes
    // from the wide arm of `loc.opk_image`.
    {
        let schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::U128, 0),
            ],
            &[0],
        );
        let mut b = Batch::with_capacity(&schema, 1);
        b.extend_pk(1u128);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &(u128::MAX - 7).to_le_bytes());
        b.count += 1;
        let mb = b.as_mem_batch();
        let image = worker_for_key(schema.locate(1).opk_image(&mb, 0), NW);
        assert_eq!(packed(&schema, &[1], &mb, 0), image, "U128 payload");
    }

    // (6) single sub-column of a compound PK — the shape whose image comes from
    // the group fold `GroupKeyCols::key_row` (its Pk arm is `opk_image`).
    {
        let schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U32, 0),
                SchemaColumn::new(type_code::I32, 0),
                SchemaColumn::new(type_code::I64, 0),
            ],
            &[0, 1],
        );
        let mut b = Batch::with_capacity(&schema, 1);
        let mut pk = [0u8; 8];
        gnitz_wire::encode_pk_column(&7u32.to_le_bytes(), type_code::U32, &mut pk[0..4]);
        gnitz_wire::encode_pk_column(&(-9i32).to_le_bytes(), type_code::I32, &mut pk[4..8]);
        b.extend_pk_bytes(&pk);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &11i64.to_le_bytes());
        b.count += 1;
        let mb = b.as_mem_batch();
        for col in [0u32, 1u32] {
            let image = worker_for_key(GroupKeyCols::new(&schema, &[col]).unwrap().key_row(&mb, 0), NW);
            assert_eq!(packed(&schema, &[col], &mb, 0), image, "compound-PK sub-col {col}");
        }
    }
}

/// The route arrives off a client-pushed circuit row, so every shape the schema
/// cannot route is refused rather than panicking inside `locate` or the packer.
#[test]
fn scatter_key_refuses_a_key_this_schema_cannot_route() {
    let schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::F64, 1),
        ],
        &[0],
    );
    let nw = 4;
    assert!(
        ScatterKey::new(ScatterSpec::GroupKey(&[7]), &schema, nw).is_err(),
        "a group key naming a column the schema has not got"
    );
    assert!(
        ScatterKey::new(ScatterSpec::JoinKey(&[(7, None)]), &schema, nw).is_err(),
        "a reindex key naming a column the schema has not got"
    );
    assert!(
        ScatterKey::new(ScatterSpec::JoinKey(&[(1, None)]), &schema, nw).is_err(),
        "a reindex key over a float column, which no OPK packs"
    );
}

/// Run one `GroupKey` scatter's router over `row`.
fn group_worker(schema: &SchemaDescriptor, cols: &[u32], b: &Batch, row: usize, nw: usize) -> usize {
    ScatterKey::new(ScatterSpec::GroupKey(cols), schema, nw)
        .expect("the fixture key routes")
        .worker(&b.as_mem_batch(), row)
}

/// Rows sharing a `Fold` routing key land on one worker whatever their PKs — the
/// co-partition GROUP BY and the set ops depend on. A string is covered in both
/// length classes, since inline and heap cells are read differently.
#[test]
fn equal_group_keys_route_to_one_worker() {
    let nw = 4;

    let schema = make_schema_u64_i64();
    let b = make_batch(&schema, &[(1, 1, 42), (2, 1, 42), (3, 1, 42), (4, 1, 7)]);
    let owner = group_worker(&schema, &[1], &b, 0, nw);
    for row in 1..3 {
        assert_eq!(
            group_worker(&schema, &[1], &b, row, nw),
            owner,
            "I64 group 42 row {row}"
        );
    }

    let s_schema = make_schema_pk_u64_payload_string();
    // "hello" is ≤ 12 bytes and lives inline; the long one lives in the heap.
    let long: &[u8] = b"this is a longer string for heap";
    let sb = make_batch_bytes(
        &s_schema,
        &[
            (1, 1, b"hello"),
            (2, 1, b"hello"),
            (3, 1, b"world"),
            (10, 1, long),
            (11, 1, long),
        ],
    );
    assert_eq!(
        group_worker(&s_schema, &[1], &sb, 0, nw),
        group_worker(&s_schema, &[1], &sb, 1, nw),
        "equal inline strings must route together"
    );
    assert_eq!(
        group_worker(&s_schema, &[1], &sb, 3, nw),
        group_worker(&s_schema, &[1], &sb, 4, nw),
        "equal heap strings must route together"
    );
}

/// A payload column routes by its value's OPK image, so a payload FK lands where
/// the same value stored as a PK column would.
#[test]
fn a_payload_group_key_routes_by_the_values_opk_image() {
    let schema = make_schema_u64_i64();
    let nw = 4;
    let vals: Vec<i64> = (0..64i64).map(|i| i * 997 + 1).collect();
    let rows: Vec<(u64, i64, i64)> = vals.iter().enumerate().map(|(i, &v)| (i as u64 + 1, 1, v)).collect();
    let b = make_batch(&schema, &rows);

    for (row, &v) in vals.iter().enumerate() {
        let mut opk = [0u8; 8];
        gnitz_wire::encode_pk_column(&v.to_le_bytes(), type_code::I64, &mut opk);
        assert_eq!(
            group_worker(&schema, &[1], &b, row, nw),
            worker_for_pk_bytes(&opk, nw),
            "val={v}"
        );
    }
}

/// Every [`ScatterKey::route_into`] arm lands each row where
/// [`ScatterKey::worker`] puts it, at that row's own weight, and drops weight-0.
#[test]
fn route_into_agrees_with_worker_on_every_scatter_kind() {
    let schema = make_schema_u64_i64();
    let nw = 4;
    let b = make_batch_raw(&schema, &[(1, 1, 70), (9, 3, 70), (4, 0, 55), (6, -2, 13)]);
    let mb = b.as_mem_batch();

    for spec in [
        ScatterSpec::GroupKey(&[0u32]),     // PkBytes: the key IS the PK list
        ScatterSpec::GroupKey(&[1u32]),     // Fold: a payload group key
        ScatterSpec::JoinKey(&[(1, None)]), // Packed: a non-PK reindex key
    ] {
        let mut pool: Vec<Vec<(u32, u32, i64)>> = Vec::new();
        let slots = crate::storage::reset_slots(&mut pool, nw);
        ScatterKey::new(spec, &schema, nw)
            .expect("the fixture key routes")
            .route_into(&mb, 3, slots);

        let mut key = ScatterKey::new(spec, &schema, nw).expect("the fixture key routes");
        let mut want: Vec<(usize, u32, u32, i64)> = (0..b.count)
            .filter(|&r| mb.get_weight(r) != 0)
            .map(|r| (key.worker(&mb, r), 3u32, r as u32, mb.get_weight(r)))
            .collect();
        let mut got: Vec<(usize, u32, u32, i64)> = slots
            .iter()
            .enumerate()
            .flat_map(|(w, rows)| rows.iter().map(move |&(si, r, wt)| (w, si, r, wt)))
            .collect();
        want.sort();
        got.sort();
        assert_eq!(got, want, "{spec:?}: route_into must match worker row for row");
    }
}
