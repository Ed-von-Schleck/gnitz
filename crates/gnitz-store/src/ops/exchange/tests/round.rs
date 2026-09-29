use super::*;
use crate::schema::key::{compare_pk_bytes, ReindexPacker};
use crate::schema::ColumnTable;
use crate::schema::{ground_owner, worker_for_pk_bytes};
use crate::schema::{SchemaColumn, SchemaDescriptor};
use crate::test_support::{
    make_batch, make_batch_i64pk, make_batch_opk, make_batch_raw, make_batch_u128, make_schema_i64pk_i64,
    make_schema_u128_i64, make_schema_u64_i64, make_wide_batch, opk_pk, pk_payload_schema, wide_pk_3xu64_schema,
};
use gnitz_wire::TypeCode;
use std::cmp::Ordering;

/// `batch` routed by `spec`, each worker's rows copied out.
fn scatter(batch: &Batch, spec: ScatterSpec<'_>, num_workers: usize) -> Vec<Batch> {
    let mut pool = Vec::new();
    let rows = op_exchange_route(batch, spec, &mut pool, num_workers).expect("the fixture key routes");
    rows.iter().map(|r| batch.ascending_subset(r)).collect()
}

/// [`op_exchange_gather`] over owned slices, skipping the empty ones as the
/// mesh does.
fn gather(slices: &[&Batch], schema: &SchemaDescriptor) -> Batch {
    let live: Vec<&Batch> = slices.iter().copied().filter(|b| b.count > 0).collect();
    let mem: Vec<MemBatch> = live.iter().map(|b| b.as_mem_batch()).collect();
    op_exchange_gather(&mem, schema, live.iter().all(|b| b.is_consolidated()))
}

fn total_rows(batches: &[Batch]) -> usize {
    batches.iter().map(|b| b.count).sum()
}

/// The single I64 payload every fixture schema below carries at slot 0.
fn payload(b: &Batch, row: usize) -> i64 {
    gnitz_wire::read_i64_le(b.col_data(0), row * 8)
}

/// [`make_batch_opk`] plus the `Consolidated` certification the shared builder
/// deliberately withholds.
fn consolidated_opk(schema: &SchemaDescriptor, rows: &[(&[u8], i64, i64)]) -> Batch {
    let mut b = make_batch_opk(schema, rows);
    b.certify_layout(Layout::Consolidated);
    b
}

/// A label, the routing columns, the sources, and whether they are
/// consolidated.
type PkCase<'a> = (&'a str, &'a [u32], Vec<Batch>, bool);

/// Routing across the PK widths whose orderings disagree, and a consolidated
/// batch's slices keeping its order:
/// a (U64, U64) read as one u128 reverses column priority, a native I64 sorts
/// negatives last without the encoder's sign flip, and the 3×U64 sources share
/// a 16-byte OPK prefix and carry a byte-equal PK at two payloads.
#[test]
fn pk_routed_scatter_routes_every_row_to_its_owner() {
    let nw = 4;

    let u64_s = make_schema_u64_i64();
    let u128_s = make_schema_u128_i64();
    let i64_s = make_schema_i64pk_i64();
    let comp_s = pk_payload_schema(&[TypeCode::U64; 2]);
    let wide_s = wide_pk_3xu64_schema();
    assert!(
        comp_s.pk_cols().len() > 1 && comp_s.pk_stride() == 16,
        "narrow compound PK"
    );
    assert!(wide_s.pk_stride() > 16, "wide compound PK");

    let comp = |c0: u64, c1: u64| opk_pk(&comp_s, &[c0 as u128, c1 as u128]);
    let (c1_10, c2_0, c5_9) = (comp(1, 10), comp(2, 0), comp(5, 9));
    let (c1_5, c1_15, c3_3) = (comp(1, 5), comp(1, 15), comp(3, 3));
    let (c1_7, c2_2, c7_0) = (comp(1, 7), comp(2, 2), comp(7, 0));

    let cases: Vec<PkCase> = vec![
        (
            "u64 pk, one consolidated source",
            &[0],
            vec![make_batch(
                &u64_s,
                &[
                    (1, 1, 10),
                    (7, 1, 70),
                    (42, 1, 42),
                    (255, 1, 25),
                    (1024, 1, 12),
                    (999_983, 1, 99),
                ],
            )],
            true,
        ),
        (
            "u64 pk, two consolidated sources",
            &[0],
            vec![
                make_batch(&u64_s, &[(1, 1, 10), (5, 1, 50), (9, 1, 90)]),
                make_batch(&u64_s, &[(2, 1, 20), (6, 1, 60), (10, 1, 100)]),
            ],
            true,
        ),
        (
            "u64 pk, two raw sources",
            &[0],
            vec![
                make_batch_raw(&u64_s, &[(5, 1, 50), (1, 1, 10)]),
                make_batch_raw(&u64_s, &[(9, 1, 90), (2, 1, 20)]),
            ],
            false,
        ),
        (
            "u128 pk, one consolidated source",
            &[0],
            vec![make_batch_u128(
                &u128_s,
                &[
                    (1u128 << 64, 1, 1),
                    ((7u128 << 64) | 42, 1, 2),
                    ((0xCAFE_BABEu128 << 64) | 0xDEAD_BEEF, 1, 3),
                    (u128::MAX, 1, 4),
                ],
            )],
            true,
        ),
        (
            "i64 pk, two consolidated sources",
            &[0],
            vec![
                make_batch_i64pk(&i64_s, &[(-100, 1, 10), (-1, 1, 11), (5, 1, 12)]),
                make_batch_i64pk(&i64_s, &[(-50, 1, 20), (0, 1, 21), (100, 1, 22)]),
            ],
            true,
        ),
        (
            "2xu64 compound pk, three consolidated sources",
            &[0, 1],
            vec![
                consolidated_opk(&comp_s, &[(&c1_10, 1, 11), (&c2_0, 1, 12), (&c5_9, 1, 13)]),
                consolidated_opk(&comp_s, &[(&c1_5, 1, 21), (&c1_15, 1, 22), (&c3_3, 1, 23)]),
                consolidated_opk(&comp_s, &[(&c1_7, 1, 31), (&c2_2, 1, 32), (&c7_0, 1, 33)]),
            ],
            true,
        ),
        (
            "3xu64 wide pk, three consolidated sources",
            &[0, 1, 2],
            vec![
                make_wide_batch(&wide_s, &[(0, 0, 0, 1, 10), (1, 1, 1, 1, 11), (1, 1, 256, 1, 12)]),
                make_wide_batch(
                    &wide_s,
                    &[
                        (1, 1, 257, 1, 20),
                        (2, 2, 2, 1, 21),
                        (3, 3, 3, 1, 99),
                        (256, 0, 0, 1, 22),
                    ],
                ),
                make_wide_batch(&wide_s, &[(1, 1, 2, 1, 30), (3, 3, 3, 1, 31), (7, 0, 0, 1, 32)]),
            ],
            true,
        ),
    ];

    for (label, cols, srcs, consolidated) in cases {
        assert_eq!(
            srcs.iter().all(|b| b.is_consolidated()),
            consolidated,
            "{label}: the fixture must carry the claim it is written for"
        );
        for src in &srcs {
            let out = scatter(src, ScatterSpec::GroupKey(cols), nw);
            assert_eq!(out.len(), nw, "{label}: one batch per worker");
            assert_eq!(total_rows(&out), src.count, "{label}: no dropped or duplicated rows");

            for (w, sb) in out.iter().enumerate() {
                for r in 0..sb.count {
                    assert_eq!(
                        worker_for_pk_bytes(sb.get_pk_bytes(r), nw),
                        w,
                        "{label}: row {r} on the wrong worker"
                    );
                }
                if sb.count == 0 || !consolidated {
                    continue;
                }
                // The tag, not `is_consolidated()`, which is vacuous under two rows.
                assert_eq!(sb.layout(), Layout::Consolidated, "{label}: worker {w} layout");
                for r in 1..sb.count {
                    let by_pk = compare_pk_bytes(sb.get_pk_bytes(r - 1), sb.get_pk_bytes(r));
                    let strictly_up =
                        by_pk == Ordering::Less || (by_pk == Ordering::Equal && payload(sb, r - 1) < payload(sb, r));
                    assert!(
                        strictly_up,
                        "{label}: worker {w} row {r} not strictly (PK, payload)-ascending"
                    );
                }
            }
        }
    }
}

/// A weight-0 row is not a Z-set element, so no worker is sent it.
#[test]
fn the_route_drops_weight_zero_rows() {
    let schema = make_schema_u64_i64();
    let b = make_batch_raw(
        &schema,
        &[(5, 1, 50), (1, 1, 10), (3, 0, 30), (9, 1, 90), (2, 1, 20), (7, 0, 70)],
    );
    let out = scatter(&b, ScatterSpec::GroupKey(&[0u32]), 4);
    assert_eq!(total_rows(&out), 4, "both weight-0 rows are dropped");
    let mut keys: Vec<u64> = Vec::new();
    for (w, sb) in out.iter().enumerate() {
        for r in 0..sb.count {
            assert_ne!(sb.get_weight(r), 0, "worker {w} row {r}: a ghost reached the output");
            keys.push(sb.get_pk(r) as u64);
        }
    }
    keys.sort();
    assert_eq!(keys, vec![1, 2, 5, 9]);
}

/// A scatter keyed by no columns sends every row to [`ground_owner`] — the
/// worker the compiler lets seed a global aggregate's ground row.
#[test]
fn a_keyless_group_scatter_routes_every_row_to_the_ground_owner() {
    let schema = make_schema_u64_i64();
    let b = make_batch_raw(&schema, &[(5, 1, 50), (1, 1, 10), (9, -1, 90)]);
    for nw in [1, 2, 3, 4, 7, 16, 64] {
        let out = scatter(&b, ScatterSpec::GroupKey(&[]), nw);
        let owner = ground_owner(nw);
        assert_eq!(out[owner].count, 3, "nw={nw}: every row reaches worker {owner}");
        assert_eq!(total_rows(&out), 3, "nw={nw}: no row reaches another worker");
    }
}

/// An empty round still yields one row list per worker, and still refuses a
/// spec this schema cannot route — the key is built before any row is read.
#[test]
fn an_empty_round_yields_one_empty_list_per_worker_and_still_refuses() {
    let schema = make_schema_u64_i64();
    let empty = Batch::empty_with_schema(&schema);
    let out = scatter(&empty, ScatterSpec::GroupKey(&[0u32]), 4);
    assert_eq!(out.len(), 4);
    assert_eq!(total_rows(&out), 0);
    assert!(
        op_exchange_route(&empty, ScatterSpec::GroupKey(&[7u32]), &mut Vec::new(), 4).is_err(),
        "a column the schema has not got is refused on an empty round too"
    );
}

/// Z-Set `+` across the slices, not a PK-ordered concatenation: a retraction
/// cancels its insert, a repeated (PK, payload) sums, and two payloads at one
/// key stay two elements, in payload order.
#[test]
fn the_gather_folds_across_consolidated_slices() {
    let schema = make_schema_u64_i64();
    let b0 = make_batch(&schema, &[(1, 1, 10), (2, 1, 20), (3, 1, 31)]);
    let b1 = make_batch(&schema, &[(1, -1, 10), (2, 2, 20), (3, 1, 30), (4, 1, 40)]);

    let got = gather(&[&b0, &b1], &schema);
    assert_eq!(got.layout(), Layout::Consolidated);
    let rows: Vec<(u64, i64, i64)> = (0..got.count)
        .map(|r| (got.get_pk(r) as u64, payload(&got, r), got.get_weight(r)))
        .collect();
    assert_eq!(rows, vec![(2, 20, 3), (3, 30, 1), (3, 31, 1), (4, 40, 1)]);
}

// -----------------------------------------------------------------------
// Join-key scatter co-partition
// -----------------------------------------------------------------------

/// `(pk, weight, payload)` of every row, in order.
fn rows_of(b: &Batch) -> Vec<(u128, i64, i64)> {
    (0..b.count)
        .map(|i| (b.get_pk(i), b.get_weight(i), payload(b, i)))
        .collect()
}

/// [`op_exchange_share`] hands each worker exactly what a round under the same
/// spec routes it, weight-0 rows dropped, for every key kind.
#[test]
fn a_share_is_what_the_round_routes_that_worker() {
    let schema = make_schema_u64_i64();
    let rows: Vec<(u64, i64, i64)> = (0..64u64)
        .map(|i| (i * 7 + 1, [1, 0, -2][i as usize % 3], i as i64 % 5))
        .collect();
    let batch = make_batch_raw(&schema, &rows);
    let join = [(1u32, None)];
    for spec in [
        ScatterSpec::GroupKey(&[0]),
        ScatterSpec::GroupKey(&[1]),
        ScatterSpec::JoinKey(&join),
    ] {
        for nw in [1usize, 2, 4] {
            let routed = scatter(&batch, spec, nw);
            for (rank, want) in routed.iter().enumerate() {
                let got = op_exchange_share(&batch, spec, crate::schema::Slot::new(rank as u32, nw as u32)).unwrap();
                assert_eq!(rows_of(&got), rows_of(want), "{spec:?}, rank {rank} of {nw}");
            }
        }
    }
}

/// Per receiver, [`op_exchange_gather`] over the slice each sender's own
/// [`op_exchange_route`] routed it equals that receiver's share of every sender's
/// partition summed, with (PK, payload) pairs repeated across senders and
/// retractions that cancel across them. Consolidated partitions agree row for row
/// and on the claim; raw ones as Z-sets.
#[test]
fn gathering_each_senders_slices_equals_routing_the_summed_partitions() {
    let schema = make_schema_u64_i64();
    let spec = ScatterSpec::GroupKey(&[0]);
    let mut rng = crate::test_rng::Rng::new(0x9a7e);
    for nw in [1usize, 2, 4] {
        for consolidated in [true, false] {
            for case in 0..40 {
                let parts: Vec<Batch> = (0..nw)
                    .map(|_| {
                        let rows: Vec<(u64, i64, i64)> = (0..rng.gen_range(12))
                            .map(|_| {
                                (
                                    rng.gen_range(16),
                                    [-1, 1, 2][rng.gen_range(3) as usize],
                                    rng.gen_range(3) as i64,
                                )
                            })
                            .collect();
                        let raw = make_batch_raw(&schema, &rows);
                        if consolidated {
                            raw.into_consolidated(&schema)
                        } else {
                            raw
                        }
                    })
                    .collect();
                let summed = Batch::concat(&schema, parts.iter().map(Batch::as_mem_batch)).into_consolidated(&schema);
                let want = scatter(&summed, spec, nw);
                let per_sender: Vec<Vec<Batch>> = parts.iter().map(|p| scatter(p, spec, nw)).collect();
                for (r, want) in want.iter().enumerate() {
                    let slices: Vec<&Batch> = per_sender.iter().map(|s| &s[r]).collect();
                    let got = gather(&slices, &schema);
                    let label = format!("W={nw} consolidated={consolidated} case {case} receiver {r}");
                    if consolidated {
                        assert_eq!(rows_of(&got), rows_of(want), "{label}");
                        assert!(got.count < 2 || got.layout() == Layout::Consolidated, "{label}");
                    } else {
                        assert_eq!(rows_of(&got.into_consolidated(&schema)), rows_of(want), "{label}");
                    }
                }
            }
        }
    }
}

/// Every output row landed on the worker its packed `_join_pk` bytes dictate, no
/// row was dropped, and the `twins` rows — which share a join key — co-located.
fn check_copartition(
    result: &[Batch],
    packer: &ReindexPacker,
    num_workers: usize,
    expect_rows: usize,
    twins: Option<(u64, u64)>,
    label: &str,
) {
    assert_eq!(total_rows(result), expect_rows, "{label}: no dropped rows");
    let mut worker_of_pk = std::collections::HashMap::new();
    for (w, sb) in result.iter().enumerate() {
        for r in 0..sb.count {
            let mut buf = [0u8; crate::schema::MAX_PK_BYTES];
            let route = worker_for_pk_bytes(packer.pack_prefix(&mut buf, &sb.as_mem_batch(), r), num_workers);
            assert_eq!(route, w, "{label}: row routed to wrong worker");
            worker_of_pk.insert(sb.get_pk(r) as u64, w);
        }
    }
    if let Some((a, b)) = twins {
        assert_eq!(
            worker_of_pk.get(&a),
            worker_of_pk.get(&b),
            "{label}: equal join keys must co-locate",
        );
    }
}

/// Schema with a compound, non-PK join key spanning a signed I64 and a U128
/// column (packed stride 24 > 16). The PK is a separate U64, so the join key is
/// NOT the PK and routing takes the packer arm.
fn make_join_key_schema() -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),  // col0: PK
            SchemaColumn::new(TypeCode::I64, false),  // col1: signed join key part
            SchemaColumn::new(TypeCode::U128, false), // col2: wide join key part
        ],
        &[0],
    )
}

/// Two payload columns, which [`make_batch_opk`]'s one-`put_int` row cannot fill.
fn make_join_key_batch(schema: &SchemaDescriptor, rows: &[(u64, i64, u128)]) -> Batch {
    let mut b = Batch::with_capacity(schema, rows.len().max(1));
    for &(pk, c1, c2) in rows {
        b.extend_pk(pk as u128);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &c1.to_le_bytes()); // I64 payload (pi 0)
        b.extend_col(1, &c2.to_le_bytes()); // U128 payload (pi 1)
        b.count += 1;
    }
    b.certify_layout(Layout::Consolidated);
    b
}

/// A compound join key routes by the packed OPK bytes the reindex Map writes as
/// `_join_pk`, not by the group key — signed-negative and >16-byte
/// composites.
#[test]
fn compound_join_key_scatter_copartitions() {
    let schema = make_join_key_schema();
    let cols = [1u32, 2u32]; // (I64, U128) join key — non-PK, stride 24.
    let num_workers = 4;
    // PK ascending (consolidated invariant). Rows 0 and 2 share the join key
    // (-5, 100) → must land on the same worker.
    let rows: &[(u64, i64, u128)] = &[
        (1, -5, 100),
        (2, 7, 100),
        (3, -5, 100),
        (4, i64::MIN, 0),
        (5, 3, 0xdead_beef_cafe_0001),
        (6, -1, 1),
    ];
    let cb = make_join_key_batch(&schema, rows);
    let key: Vec<gnitz_wire::ReindexSlot> = cols.iter().map(|&c| (c, None)).collect();
    let packer = ReindexPacker::new(&schema, &key).unwrap();

    let out = scatter(&cb, ScatterSpec::JoinKey(&key), num_workers);
    check_copartition(&out, &packer, num_workers, rows.len(), Some((1, 3)), "compound key");
}

/// The packer fires on promotion too, not only on a compound key: an I32 → I64
/// payload key, and one that IS the source PK, where routing must take the
/// `T`-wide `_join_pk` rather than the narrow native PK bytes.
#[test]
fn promoted_single_join_key_scatter_copartitions() {
    let num_workers = 4;

    // ---- (1) Payload key: [U64 PK, I32 payload], reindex col1 → I64. ----
    let schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::I32, false),
        ],
        &[0],
    );
    // PK ascending; rows pk=1 and pk=4 share key -5 → must co-locate.
    let rows: &[(u64, i32)] = &[(1, -5), (2, 7), (3, i32::MIN), (4, -5), (5, 0), (6, -1)];
    let mut b = Batch::with_capacity(&schema, rows.len());
    for &(pk, key) in rows {
        b.extend_pk(pk as u128);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &key.to_le_bytes());
        b.count += 1;
    }
    b.certify_layout(Layout::Consolidated);
    let key = [(1u32, Some(TypeCode::I64))];
    let packer = ReindexPacker::new(&schema, &key).unwrap();
    let out = scatter(&b, ScatterSpec::JoinKey(&key), num_workers);
    check_copartition(&out, &packer, num_workers, rows.len(), Some((1, 4)), "payload key");

    // ---- (2) PK key: [I32 PK, U64 payload], reindex col0 → I64. ----
    let pk_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::I32, false),
            SchemaColumn::new(TypeCode::U64, false),
        ],
        &[0],
    );
    // OPK order: i32::MIN sign-flips to 0x0000_0000 → sorts first, then ascending.
    let pk_rows: &[(i32, u64)] = &[(i32::MIN, 9), (-5, 9), (-1, 9), (3, 9)];
    let mut pb = Batch::with_capacity(&pk_schema, pk_rows.len());
    for &(pk, v) in pk_rows {
        let mut opk = [0u8; 4];
        gnitz_wire::encode_pk_column(&pk.to_le_bytes(), TypeCode::I32, &mut opk);
        pb.extend_pk_bytes(&opk);
        pb.extend_weight(&1i64.to_le_bytes());
        pb.extend_null_bmp(&0u64.to_le_bytes());
        pb.extend_col(0, &v.to_le_bytes());
        pb.count += 1;
    }
    pb.certify_layout(Layout::Consolidated);
    let pk_key = [(0u32, Some(TypeCode::I64))];
    let pk_packer = ReindexPacker::new(&pk_schema, &pk_key).unwrap();
    let pk_out = scatter(&pb, ScatterSpec::JoinKey(&pk_key), num_workers);
    check_copartition(&pk_out, &pk_packer, num_workers, pk_rows.len(), None, "promoted PK key");
}

/// A U64 PK plus two I64 payload columns: the multi-column payload key benches.
fn make_schema_u64_2xi64() -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::I64, false),
            SchemaColumn::new(TypeCode::I64, false),
        ],
        &[0],
    )
}

/// `n` rows over a U64-PK, all-I64-payload `schema`, PKs `0..n`, each payload a
/// spread function of the PK. Certified consolidated iff asked.
fn bench_stripe(schema: &SchemaDescriptor, n: usize, consolidated: bool) -> Batch {
    let mut b = crate::storage::BatchBuilder::new(*schema);
    for i in 0..n {
        let pk = i as u64;
        b.begin_row(pk as u128, 1);
        for c in 1..schema.columns.len() as i64 {
            b.put_int((pk as i64).wrapping_mul(2_654_435_761 + c) as u128);
        }
        b.end_row();
    }
    let mut b = b.finish();
    if consolidated {
        b.certify_layout(Layout::Consolidated);
    }
    b
}

/// Release-only microbench of one [`ScatterSpec`]'s route through
/// [`op_exchange_route`], 1M rows to 4 workers. Compare instructions retired:
/// `perf stat -e instructions:u <test-bin> --exact <bench> --ignored --test-threads=1`.
fn route_kind_bench(name: &str, spec: ScatterSpec<'_>, schema: &SchemaDescriptor) {
    use std::hint::black_box;
    use std::time::Instant;

    const N: usize = 1_000_000;
    const ITERS: usize = 20;
    const WORKERS: usize = 4;
    let batch = bench_stripe(schema, N, false);
    let mut pool = Vec::new();
    let routed = |pool: &mut Vec<Vec<u32>>| -> usize {
        let rows = op_exchange_route(&batch, spec, pool, WORKERS).expect("the bench key routes");
        rows.iter().map(Vec::len).sum()
    };
    assert_eq!(routed(&mut pool), N, "{name}: the route dropped rows");

    let t = Instant::now();
    let mut acc = 0usize;
    for _ in 0..ITERS {
        acc += black_box(routed(&mut pool));
    }
    let secs = t.elapsed().as_secs_f64();
    println!(
        "{name}: {:.1} Mrows/s ({N} rows × {ITERS} iters in {secs:.3}s, checksum {acc})",
        (N * ITERS) as f64 / secs / 1e6,
    );
}

#[test]
#[ignore]
fn scatter_route_pk_bench() {
    route_kind_bench("pk", ScatterSpec::GroupKey(&[0]), &make_schema_u64_i64());
}

#[test]
#[ignore]
fn scatter_route_image_join_bench() {
    route_kind_bench("image_join", ScatterSpec::JoinKey(&[(1, None)]), &make_schema_u64_i64());
}

#[test]
#[ignore]
fn scatter_route_image_group_bench() {
    route_kind_bench("image_group", ScatterSpec::GroupKey(&[1]), &make_schema_u64_i64());
}

#[test]
#[ignore]
fn scatter_route_packed_bench() {
    route_kind_bench(
        "packed",
        ScatterSpec::JoinKey(&[(1, None), (2, None)]),
        &make_schema_u64_2xi64(),
    );
}

#[test]
#[ignore]
fn scatter_route_fold_bench() {
    route_kind_bench("fold", ScatterSpec::GroupKey(&[1, 2]), &make_schema_u64_2xi64());
}

/// Release-only microbench of [`op_exchange_gather`] over one receiver's slice
/// from each of K senders, over both arms: `raw` is the production default, a
/// base-table delta reaching the round `Raw` and an output round ending at a
/// reindex `Map` that downgrades. K=1 (replicated, or single-worker) and K=4
/// are the reachable range, K=16 the headroom point; each sender holds a
/// disjoint stripe of one ascending key space, so the merge genuinely
/// interleaves them.
///
/// `cd crates && cargo test -p gnitz-store --release exchange_gather_bench -- --ignored --nocapture --test-threads=1`
#[test]
#[ignore]
fn exchange_gather_bench() {
    use std::hint::black_box;
    use std::time::Instant;

    let schema = make_schema_u64_i64();
    const N: usize = 1_000_000;
    const ITERS: usize = 20;
    const WORKERS: usize = 4;
    type Build = fn(&SchemaDescriptor, &[(u64, i64, i64)]) -> Batch;

    for (arm, build) in [("consolidated", make_batch as Build), ("raw", make_batch_raw as Build)] {
        for k in [1usize, 4, 16] {
            let per = N / k;
            // Sender j's slice for receiver 0 of its own stripe.
            let slices: Vec<Batch> = (0..k)
                .map(|j| {
                    let rows: Vec<(u64, i64, i64)> = (0..per).map(|i| ((i * k + j) as u64, 1, i as i64)).collect();
                    let stripe = build(&schema, &rows);
                    scatter(&stripe, ScatterSpec::GroupKey(&[0u32]), WORKERS).swap_remove(0)
                })
                .collect();
            let refs: Vec<&Batch> = slices.iter().collect();
            let rows: usize = slices.iter().map(|b| b.count).sum();

            let warm = gather(&refs, &schema);
            assert_eq!(warm.count, rows, "{arm}/K={k}: gather dropped rows");
            assert_eq!(
                warm.layout() == Layout::Consolidated,
                arm == "consolidated",
                "{arm}/K={k}: fixture on the wrong arm"
            );

            let t = Instant::now();
            let mut acc = 0usize;
            for _ in 0..ITERS {
                acc += black_box(gather(&refs, &schema).count);
            }
            let secs = t.elapsed().as_secs_f64();
            println!(
                "exchange_gather/{arm}/K{k}: {:.1} Mrows/s ({rows} rows × {ITERS} iters in {secs:.3}s, checksum {acc})",
                (rows * ITERS) as f64 / secs / 1e6,
            );
        }
    }
}

/// Instructions per [`op_exchange_route`] call on small deltas — the per-call
/// fixed cost a round adds — at (rows, workers).
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn exchange_route_small_delta_bench() {
    use gnitz_foundation::perf::Counter;
    use std::hint::black_box;
    const ITERS: u64 = 10_000;
    let schema = make_schema_u64_i64();
    let instructions = Counter::instructions().unwrap();
    let spec = ScatterSpec::GroupKey(&[0]);
    for (n, workers) in [(1, 4), (64, 16), (1024, 16)] {
        let batch = bench_stripe(&schema, n, true);
        let mut pool = Vec::new();
        black_box(op_exchange_route(&batch, spec, &mut pool, workers).unwrap());
        let ((), i) = instructions.measure(|| {
            for _ in 0..ITERS {
                black_box(op_exchange_route(&batch, spec, &mut pool, workers).unwrap());
            }
        });
        println!("{n} rows -> {workers} workers: {} instructions per call", i / ITERS);
    }
}
