use super::*;
use crate::ops::group_key::GroupKeyCols;
use crate::schema::{type_code, SchemaColumn, SchemaDescriptor};
use crate::test_support::{make_batch, make_schema_u64_i64};

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
        let out = op_worker_filter(&batch, &schema, wid, num_workers);
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
    let out = op_worker_filter(&batch, &schema, 0, 1);
    assert_eq!(out.count, 3, "(0, 1) must keep every row");
}

#[test]
fn test_worker_filter_empty_in_empty_out() {
    let schema = make_schema_u64_i64();
    let batch = make_batch(&schema, &[]);
    let out = op_worker_filter(&batch, &schema, 1, 4);
    assert_eq!(out.count, 0);
}

/// The `ScatterKey` collapse replaced the single-column `route_partition_key`
/// (routable-int → `route_key`, string → `german_string_promote_key`) and the
/// `compound_join_packer` path with one packed-`ReindexPacker` route.
/// For every reachable `JoinKey` shape, the packed owner must equal
/// what the pre-collapse routing produced — so no row moves workers. The
/// NULL arms are null-blind by design (they read the canonically-zeroed key
/// slot), exactly as the deleted `route_partition_key` was.
#[test]
fn test_scatter_key_packed_matches_legacy_routing() {
    use crate::storage::MemBatch;

    // Run the `JoinKey` `ScatterKey` over `row` and return its worker.
    const NW: usize = 4;
    fn packed(schema: &SchemaDescriptor, cols: &[u32], mb: &MemBatch, row: usize) -> usize {
        let key: Vec<gnitz_wire::ReindexSlot> = cols.iter().map(|&c| (c, None)).collect();
        let mut sk = ScatterKey::new(ScatterSpec::JoinKey(&key), schema, NW).expect("the fixture key routes");
        assert!(!sk.is_pk_routed(), "test key shapes must take the packed route");
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
            // Legacy: routable-int → loc.route_key → worker_for_key (null-blind).
            let legacy = worker_for_key(schema.locate(1).route_key(&mb, row), NW);
            assert_eq!(packed(&schema, &[1], &mb, row), legacy, "I64 row {row}");
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
            // Legacy: string → german_string_promote_key → worker_for_key.
            let legacy = worker_for_key(
                crate::schema::key::german_string_promote_key(mb.get_col_ptr(row, 0, 16), mb.blob),
                NW,
            );
            assert_eq!(packed(&schema, &[1], &mb, row), legacy, "STRING row {row}");
        }
    }

    // (5) U128 payload key: `is_pk_eligible` includes U128, so the
    // wide arm of `loc.route_key` fed `worker_for_key`.
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
        let legacy = worker_for_key(schema.locate(1).route_key(&mb, 0), NW);
        assert_eq!(packed(&schema, &[1], &mb, 0), legacy, "U128 payload");
    }

    // (6) single sub-column of a compound PK — the one legacy join-key
    // shape that fell through `route_partition_key`'s non-PK guard to the
    // group fold `GroupKeyCols::key_row` (its Pk arm is `pk_route_key`).
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
            let legacy = worker_for_key(GroupKeyCols::new(&schema, &[col]).key_row(&mb, 0), NW);
            assert_eq!(packed(&schema, &[col], &mb, 0), legacy, "compound-PK sub-col {col}");
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
