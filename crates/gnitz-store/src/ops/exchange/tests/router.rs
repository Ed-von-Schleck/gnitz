use super::*;
use crate::ops::group_key::GroupOutKey;
use crate::schema::{Placement, SchemaColumn, SchemaDescriptor, TypeCode};
use crate::test_support::{make_batch, make_batch_bytes, make_schema_pk_u64_payload_string, make_schema_u64_i64};

/// The worker `key`'s linear walk sends each row of `mb` to, in row order.
fn workers(key: &ScatterKey, schema: &SchemaDescriptor, mb: &MemBatch, nw: usize) -> Vec<usize> {
    let mut pool = Vec::new();
    let slots = crate::storage::reset_slots(&mut pool, nw);
    key.route(std::slice::from_ref(mb), schema, false, slots);
    let mut out = vec![usize::MAX; mb.count];
    for (w, rows) in slots.iter().enumerate() {
        for &(_, r, _) in rows {
            out[r as usize] = w;
        }
    }
    out
}

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
        let out = op_worker_filter(&batch, Slot::new(wid, num_workers));
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
    let out = op_worker_filter(&batch, Slot::SOLO);
    assert_eq!(out.count, 3, "(0, 1) must keep every row");
}

#[test]
fn test_worker_filter_empty_in_empty_out() {
    let schema = make_schema_u64_i64();
    let batch = make_batch(&schema, &[]);
    let out = op_worker_filter(&batch, Slot::new(1, 4));
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
        let sk = ScatterKey::new(ScatterSpec::JoinKey(&key), schema).expect("the fixture key routes");
        workers(&sk, schema, mb, NW)[row]
    }

    // (1) non-null I64 payload; (2) NULL I64 payload (nullable col).
    {
        let schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(TypeCode::U64, false),
                SchemaColumn::new(TypeCode::I64, true),
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
                SchemaColumn::new(TypeCode::U64, false),
                SchemaColumn::new(TypeCode::String, true),
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
                SchemaColumn::new(TypeCode::U64, false),
                SchemaColumn::new(TypeCode::U128, false),
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

    // (6) single sub-column of a compound PK.
    {
        let schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(TypeCode::U32, false),
                SchemaColumn::new(TypeCode::I32, false),
                SchemaColumn::new(TypeCode::I64, false),
            ],
            &[0, 1],
        );
        let mut b = Batch::with_capacity(&schema, 1);
        let mut pk = [0u8; 8];
        gnitz_wire::encode_pk_column(&7u32.to_le_bytes(), TypeCode::U32, &mut pk[0..4]);
        gnitz_wire::encode_pk_column(&(-9i32).to_le_bytes(), TypeCode::I32, &mut pk[4..8]);
        b.extend_pk_bytes(&pk);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &11i64.to_le_bytes());
        b.count += 1;
        let mb = b.as_mem_batch();
        for col in [0u32, 1u32] {
            let (gk, _) = GroupOutKey::new(&schema, &[col], []).unwrap();
            let image = worker_for_key(gk.identity(&mb, 0), NW);
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
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::F64, true),
        ],
        &[0],
    );
    assert!(
        ScatterKey::new(ScatterSpec::GroupKey(&[7]), &schema).is_err(),
        "a group key naming a column the schema has not got"
    );
    assert!(
        ScatterKey::new(ScatterSpec::JoinKey(&[(7, None)]), &schema).is_err(),
        "a reindex key naming a column the schema has not got"
    );
    assert!(
        ScatterKey::new(ScatterSpec::JoinKey(&[(1, None)]), &schema).is_err(),
        "a reindex key over a float column, which no OPK packs"
    );
}

/// Run one `GroupKey` scatter's router over `row`.
fn group_worker(schema: &SchemaDescriptor, cols: &[u32], b: &Batch, row: usize, nw: usize) -> usize {
    let key = ScatterKey::new(ScatterSpec::GroupKey(cols), schema).expect("the fixture key routes");
    workers(&key, schema, &b.as_mem_batch(), nw)[row]
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
        gnitz_wire::encode_pk_column(&v.to_le_bytes(), TypeCode::I64, &mut opk);
        assert_eq!(
            group_worker(&schema, &[1], &b, row, nw),
            worker_for_pk_bytes(&opk, nw),
            "val={v}"
        );
    }
}

// ── routes_to_native_owner: the exchange-elision predicate ──────────────

/// A 3-column compound PK `(U32, I32, U64)` + an I64 payload, so a distribution
/// prefix has a proper-prefix width to be wrong at.
fn three_col_schema(placement: Placement) -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U32, false),
            SchemaColumn::new(TypeCode::I32, false),
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::I64, false),
        ],
        &[0, 1, 2],
    )
    .with_placement(placement)
}

/// `n` rows over [`three_col_schema`], their PK columns varying independently.
fn three_col_batch(schema: &SchemaDescriptor, n: usize) -> Batch {
    let mut b = Batch::with_capacity(schema, n);
    for i in 0..n as u32 {
        let mut pk = [0u8; 16];
        gnitz_wire::encode_pk_column(&(i % 5 + 1).to_le_bytes(), TypeCode::U32, &mut pk[0..4]);
        gnitz_wire::encode_pk_column(&(-(i as i32) * 3).to_le_bytes(), TypeCode::I32, &mut pk[4..8]);
        gnitz_wire::encode_pk_column(&(u64::from(i) * 31 + 7).to_le_bytes(), TypeCode::U64, &mut pk[8..16]);
        b.extend_pk_bytes(&pk);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &i64::from(i).to_le_bytes());
        b.count += 1;
    }
    b
}

/// The unpromoted key slots for `cols`.
fn slots_of(cols: &[u32]) -> Vec<gnitz_wire::ReindexSlot> {
    cols.iter().map(|&c| (c, None)).collect()
}

/// The predicate's whole promise: wherever it accepts, the scatter it describes
/// routes every row to the worker the table-key router already placed it on.
#[test]
fn every_accepted_spec_routes_each_row_to_its_tables_own_worker() {
    const NW: usize = 4;
    let mut accepted = 0usize;
    for k in [1u8, 2, 3] {
        let schema = three_col_schema(Placement::Keyed { prefix_len: k });
        let b = three_col_batch(&schema, 24);
        let mb = b.as_mem_batch();
        for cols in [&[0u32][..], &[0, 1], &[0, 1, 2], &[1], &[1, 0]] {
            let js = slots_of(cols);
            for spec in [ScatterSpec::GroupKey(cols), ScatterSpec::JoinKey(&js)] {
                if !spec.routes_to_native_owner(&schema) {
                    continue;
                }
                accepted += 1;
                let key = ScatterKey::new(spec, &schema).expect("an accepted spec routes");
                let got = workers(&key, &schema, &mb, NW);
                for (row, &got) in got.iter().enumerate() {
                    assert_eq!(
                        got,
                        schema.worker_for_pk(mb.get_pk_bytes(row), NW),
                        "k={k}, {spec:?}, row {row}"
                    );
                }
            }
        }
    }
    assert_eq!(accepted, 5, "the accept side must cover both kinds at both ends");
}

/// Over a 2-of-3 proper prefix a `GroupKey` really does land its rows elsewhere,
/// where the same columns as a `JoinKey` pack the prefix's own OPK bytes: the
/// refusal is about the hash, not the width.
#[test]
fn a_proper_prefix_group_key_is_refused_and_does_route_elsewhere() {
    const NW: usize = 4;
    let schema = three_col_schema(Placement::Keyed { prefix_len: 2 });
    let cols = [0u32, 1];
    assert!(!ScatterSpec::GroupKey(&cols).routes_to_native_owner(&schema));
    assert!(ScatterSpec::JoinKey(&slots_of(&cols)).routes_to_native_owner(&schema));

    let b = three_col_batch(&schema, 24);
    let mb = b.as_mem_batch();
    let key = ScatterKey::new(ScatterSpec::GroupKey(&cols), &schema).expect("the fixture key routes");
    let got = workers(&key, &schema, &mb, NW);
    assert!(
        (0..mb.count).any(|row| got[row] != schema.worker_for_pk(mb.get_pk_bytes(row), NW)),
        "the fold must disagree with the prefix hash somewhere"
    );
}

/// The distribution prefix must match exactly, carry no promotion, and belong to
/// a relation `worker_for_pk` actually places.
#[test]
fn routes_to_native_owner_is_the_exact_unpromoted_distribution_prefix() {
    for (k, want) in [(1u8, &[0u32][..]), (2, &[0, 1]), (0, &[0, 1, 2])] {
        let s = three_col_schema(Placement::Keyed { prefix_len: k }); // k = 0 is the default: the whole PK
        for cand in [&[][..], &[0], &[0, 1], &[0, 1, 2], &[1], &[1, 0]] {
            assert_eq!(
                ScatterSpec::JoinKey(&slots_of(cand)).routes_to_native_owner(&s),
                cand == want,
                "k={k}: {cand:?} against the distribution key {want:?}"
            );
        }
        let promoted: Vec<gnitz_wire::ReindexSlot> = want
            .iter()
            .map(|&c| (c, (c == 0).then_some(gnitz_wire::TypeCode::U64)))
            .collect();
        assert!(
            !ScatterSpec::JoinKey(&promoted).routes_to_native_owner(&s),
            "k={k}: a carried target moves the slot off its source column's width"
        );
    }
    for p in [Placement::Replicated, Placement::Local] {
        let s = three_col_schema(p);
        for cand in [&[][..], &[0], &[0, 1, 2]] {
            assert!(
                !ScatterSpec::GroupKey(cand).routes_to_native_owner(&s),
                "{p:?}: {cand:?}"
            );
            assert!(
                !ScatterSpec::JoinKey(&slots_of(cand)).routes_to_native_owner(&s),
                "{p:?}: {cand:?}"
            );
        }
    }
}

/// A `GroupKey` over the distribution prefix routes natively only at the two
/// ends of the key width, so a `CLUSTER BY` of 2+ proper-prefix columns buys a
/// `GROUP BY` on it no locality.
#[test]
fn a_group_key_routes_natively_only_at_the_two_ends_of_the_prefix() {
    for (k, cols, want) in [(1u8, &[0u32][..], true), (2, &[0, 1], false), (3, &[0, 1, 2], true)] {
        let s = three_col_schema(Placement::Keyed { prefix_len: k });
        assert_eq!(ScatterSpec::GroupKey(cols).routes_to_native_owner(&s), want, "k={k}");
    }
}

/// The routing invariant: a group scatter sends each row to the owner of the
/// output PK the reduce keys its group by.
#[test]
fn a_group_scatter_routes_each_row_to_its_output_pks_owner() {
    const NW: usize = 4;
    let schema = three_col_schema(Placement::Keyed { prefix_len: 0 });
    let b = three_col_batch(&schema, 24);
    let mb = b.as_mem_batch();
    for cols in [
        &[][..],
        &[3],
        &[0],
        &[1],
        &[0, 1],
        &[1, 0],
        &[0, 1, 2],
        &[2, 0, 1],
        &[3, 0],
    ] {
        let (gk, _) = GroupOutKey::new(&schema, cols, []).expect("the fixture group keys");
        let key = ScatterKey::new(ScatterSpec::GroupKey(cols), &schema).expect("the fixture key routes");
        let got = workers(&key, &schema, &mb, NW);
        for (row, &got) in got.iter().enumerate() {
            assert_eq!(
                got,
                worker_for_pk_bytes(gk.out_pk(&mb, row).bytes(), NW),
                "{cols:?} row {row}"
            );
        }
    }
}

/// Every PK-eligible type code.
fn pk_eligible_types() -> impl Iterator<Item = TypeCode> {
    TypeCode::ALL.iter().copied().filter(|t| t.is_pk_eligible())
}

/// `tc`'s min, 0 and max, as native little-endian cells of its width.
fn extreme_cells(tc: TypeCode) -> [Vec<u8>; 3] {
    let w = tc.wire_stride();
    let (min, max) = if tc.is_signed_int() {
        let mut min = vec![0u8; w];
        min[w - 1] = 0x80;
        let mut max = vec![0xffu8; w];
        max[w - 1] = 0x7f;
        (min, max)
    } else {
        (vec![0u8; w], vec![0xffu8; w])
    };
    [min, vec![0u8; w], max]
}

/// An unpromoted join key over the PK's leading columns hashes the row's own
/// leading PK bytes, which is what the reindex Map packs for it.
#[test]
fn an_unpromoted_pk_prefix_join_key_hashes_the_bytes_the_map_packs() {
    for tc in pk_eligible_types() {
        let schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(tc, false),
                SchemaColumn::new(TypeCode::U64, false),
                SchemaColumn::new(TypeCode::I64, false),
            ],
            &[0, 1],
        );
        let w = tc.wire_stride();
        let mut b = Batch::with_capacity(&schema, 3);
        for (i, cell) in extreme_cells(tc).iter().enumerate() {
            let mut pk = vec![0u8; w + 8];
            gnitz_wire::encode_pk_column(cell, tc, &mut pk[..w]);
            gnitz_wire::encode_pk_column(&(i as u64 * 77).to_le_bytes(), TypeCode::U64, &mut pk[w..]);
            b.extend_pk_bytes(&pk);
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &0i64.to_le_bytes());
            b.count += 1;
        }
        let mb = b.as_mem_batch();
        for k in [1usize, 2] {
            let slots = slots_of(&[0, 1][..k]);
            let key = ScatterKey::new(ScatterSpec::JoinKey(&slots), &schema).expect("the fixture key routes");
            let ScatterKey::PkPrefix(n) = key else {
                panic!("{tc:?} k={k}: not a PK prefix")
            };
            let packer = ReindexPacker::new(&schema, &slots).expect("the fixture key packs");
            let mut buf = [0u8; MAX_PK_BYTES];
            for row in 0..mb.count {
                assert_eq!(
                    packer.pack_prefix(&mut buf, &mb, row),
                    &mb.get_pk_bytes(row)[..n],
                    "{tc:?} k={k} row {row}"
                );
            }
        }
    }
}

/// A single unpromoted PK-eligible join column routes by its OPK image, landing
/// each row where the reindex Map's packed bytes do — as a nullable payload
/// column (NULL rows included) and as a non-leading PK column.
#[test]
fn a_single_column_join_key_routes_by_the_image_the_map_packs() {
    const NW: usize = 4;
    let check = |schema: &SchemaDescriptor, b: &Batch, c: u32| {
        let mb = b.as_mem_batch();
        let slots = [(c, None)];
        let key = ScatterKey::new(ScatterSpec::JoinKey(&slots), schema).expect("the fixture key routes");
        assert!(
            matches!(key, ScatterKey::Image(_)),
            "{:?}: not an image",
            schema.columns[c as usize].type_code
        );
        let packer = ReindexPacker::new(schema, &slots).expect("the fixture key packs");
        let got = workers(&key, schema, &mb, NW);
        let mut buf = [0u8; MAX_PK_BYTES];
        for (row, &got) in got.iter().enumerate() {
            let want = worker_for_pk_bytes(packer.pack_prefix(&mut buf, &mb, row), NW);
            assert_eq!(got, want, "{:?} row {row}", schema.columns[c as usize].type_code);
        }
    };
    for tc in pk_eligible_types() {
        let w = tc.wire_stride();
        // A nullable payload column, then the same values plus a NULL.
        let schema = SchemaDescriptor::new(
            &[SchemaColumn::new(TypeCode::U64, false), SchemaColumn::new(tc, true)],
            &[0],
        );
        let mut b = Batch::with_capacity(&schema, 4);
        let cells = extreme_cells(tc);
        for (i, cell) in cells.iter().chain([&vec![0u8; w]]).enumerate() {
            b.extend_pk(i as u128 + 1);
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&u64::from(i == cells.len()).to_le_bytes());
            b.extend_col(0, cell);
            b.count += 1;
        }
        check(&schema, &b, 1);

        // A non-leading PK column.
        let schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(TypeCode::U64, false),
                SchemaColumn::new(tc, false),
                SchemaColumn::new(TypeCode::I64, false),
            ],
            &[0, 1],
        );
        let mut b = Batch::with_capacity(&schema, 3);
        for (i, cell) in cells.iter().enumerate() {
            let mut pk = vec![0u8; 8 + w];
            gnitz_wire::encode_pk_column(&(i as u64 + 1).to_le_bytes(), TypeCode::U64, &mut pk[..8]);
            gnitz_wire::encode_pk_column(cell, tc, &mut pk[8..]);
            b.extend_pk_bytes(&pk);
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &0i64.to_le_bytes());
            b.count += 1;
        }
        check(&schema, &b, 1);
    }
}

/// A packed join key's linear walk, which packs a chunk of rows at a time, sends
/// every row of nonzero weight where its own packed key hashes, across several
/// chunks and over a promoted slot.
#[test]
fn a_packed_linear_route_hashes_each_rows_own_packed_key() {
    const NW: usize = 4;
    let schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::I32, false),
            SchemaColumn::new(TypeCode::U16, false),
        ],
        &[0],
    );
    let n = 600;
    let mut b = Batch::with_capacity(&schema, n);
    for i in 0..n as u64 {
        b.extend_pk(u128::from(i));
        b.extend_weight(&i64::from(i % 7 != 3).to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &((i as i32) * -977).to_le_bytes());
        b.extend_col(1, &((i * 13) as u16).to_le_bytes());
        b.count += 1;
    }
    let mb = b.as_mem_batch();
    let slots = [(1, Some(gnitz_wire::TypeCode::I64)), (2, None)];
    let key = ScatterKey::new(ScatterSpec::JoinKey(&slots), &schema).unwrap();
    let packer = ReindexPacker::new(&schema, &slots).unwrap();
    let got = workers(&key, &schema, &mb, NW);
    let mut buf = [0u8; MAX_PK_BYTES];
    for (row, &got) in got.iter().enumerate() {
        let want = match mb.get_weight(row) {
            0 => usize::MAX,
            _ => {
                packer.pack_into(&mut buf[..packer.out_stride], &mb, row);
                worker_for_pk_bytes(&buf[..packer.out_stride], NW)
            }
        };
        assert_eq!(got, want, "row {row}");
    }
}
