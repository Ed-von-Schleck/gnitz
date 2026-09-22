use super::*;
use crate::schema::key::{compare_pk_bytes, ReindexPacker};
use crate::schema::{SchemaColumn, SchemaDescriptor};
use crate::test_support::{
    make_batch, make_batch_i64pk, make_batch_opk, make_batch_raw, make_batch_u128, make_schema_i64pk_i64,
    make_schema_u128_i64, make_schema_u64_i64, make_wide_batch, opk_pk, pk_payload_schema, wide_pk_3xu64_schema,
};
use gnitz_wire::{worker_for_pk_bytes, TypeCode};
use std::cmp::Ordering;

fn scatter(sources: &[&Batch], spec: ScatterSpec<'_>, schema: &SchemaDescriptor, num_workers: usize) -> Vec<Batch> {
    op_relay_scatter(sources, spec, schema, num_workers).expect("the fixture key routes")
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

/// A label, the schema and routing columns, the sources, and whether they are
/// consolidated.
type PkCase<'a> = (&'a str, &'a SchemaDescriptor, &'a [u32], Vec<Batch>, bool);

/// Routing and the layout claim across the PK widths whose orderings disagree:
/// a (U64, U64) read as one u128 reverses column priority, a native I64 sorts
/// negatives last without the encoder's sign flip, and the 3×U64 sources share
/// a 16-byte OPK prefix and carry a byte-equal PK at two payloads.
///
/// One consolidated source is the replicated / muted shape, where only one
/// worker publishes rows.
#[test]
fn pk_routed_scatter_routes_and_claims_by_the_gate() {
    let nw = 4;

    let u64_s = make_schema_u64_i64();
    let u128_s = make_schema_u128_i64();
    let i64_s = make_schema_i64pk_i64();
    let comp_s = pk_payload_schema(&[TypeCode::U64; 2]);
    let wide_s = wide_pk_3xu64_schema();
    assert!(
        comp_s.pk_indices().len() > 1 && comp_s.pk_stride() == 16,
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
            &u64_s,
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
            &u64_s,
            &[0],
            vec![
                make_batch(&u64_s, &[(1, 1, 10), (5, 1, 50), (9, 1, 90)]),
                make_batch(&u64_s, &[(2, 1, 20), (6, 1, 60), (10, 1, 100)]),
            ],
            true,
        ),
        (
            "u64 pk, two raw sources",
            &u64_s,
            &[0],
            vec![
                make_batch_raw(&u64_s, &[(5, 1, 50), (1, 1, 10)]),
                make_batch_raw(&u64_s, &[(9, 1, 90), (2, 1, 20)]),
            ],
            false,
        ),
        (
            "u128 pk, one consolidated source",
            &u128_s,
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
            &i64_s,
            &[0],
            vec![
                make_batch_i64pk(&i64_s, &[(-100, 1, 10), (-1, 1, 11), (5, 1, 12)]),
                make_batch_i64pk(&i64_s, &[(-50, 1, 20), (0, 1, 21), (100, 1, 22)]),
            ],
            true,
        ),
        (
            "2xu64 compound pk, three consolidated sources",
            &comp_s,
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
            &wide_s,
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

    for (label, schema, cols, srcs, consolidated) in cases {
        let refs: Vec<&Batch> = srcs.iter().collect();
        assert_eq!(
            refs.iter().all(|b| b.is_consolidated()),
            consolidated,
            "{label}: the fixture must reach the arm it is written for"
        );
        let out = scatter(&refs, ScatterSpec::GroupKey(cols), schema, nw);
        assert_eq!(out.len(), nw, "{label}: one batch per worker");
        assert_eq!(
            total_rows(&out),
            srcs.iter().map(|b| b.count).sum::<usize>(),
            "{label}: no dropped or duplicated rows"
        );

        for (w, sb) in out.iter().enumerate() {
            for r in 0..sb.count {
                assert_eq!(
                    worker_for_pk_bytes(sb.get_pk_bytes(r), nw),
                    w,
                    "{label}: row {r} on the wrong worker"
                );
            }
            if sb.count == 0 {
                continue;
            }
            // The tag, not `is_consolidated()`, which is vacuous under two rows.
            let want = if consolidated {
                Layout::Consolidated
            } else {
                Layout::Raw
            };
            assert_eq!(sb.layout(), want, "{label}: worker {w} layout");
            if !consolidated {
                // Per-source concatenated, so only routing and conservation hold.
                continue;
            }
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

/// The linear arm drops weight-0 rows and claims nothing. The fixture is
/// multi-row and out of order because `is_consolidated` answers `true` for any
/// batch of under two live rows, certified or not.
#[test]
fn the_linear_arm_drops_weight_zero_rows_and_claims_nothing() {
    let schema = make_schema_u64_i64();
    let nw = 4;
    let b0 = make_batch_raw(&schema, &[(5, 1, 50), (1, 1, 10), (3, 0, 30)]);
    let b1 = make_batch_raw(&schema, &[(9, 1, 90), (2, 1, 20), (7, 0, 70)]);
    assert!(
        !b0.is_consolidated() && !b1.is_consolidated(),
        "the fixture must reach the linear arm"
    );

    let out = scatter(&[&b0, &b1], ScatterSpec::GroupKey(&[0u32]), &schema, nw);
    assert_eq!(total_rows(&out), 4, "both weight-0 rows are dropped");
    let mut keys: Vec<u64> = Vec::new();
    for (w, sb) in out.iter().enumerate() {
        if sb.count > 0 {
            assert_eq!(sb.layout(), Layout::Raw, "worker {w}: the linear arm claims nothing");
        }
        for r in 0..sb.count {
            assert_ne!(sb.get_weight(r), 0, "worker {w} row {r}: a ghost reached the output");
            keys.push(sb.get_pk(r) as u64);
        }
    }
    keys.sort();
    assert_eq!(keys, vec![1, 2, 5, 9]);
}

/// An empty round still yields one batch per worker, and still refuses a spec
/// this schema cannot route — the key is built before any row is read.
#[test]
fn an_empty_round_yields_one_empty_batch_per_worker_and_still_refuses() {
    let schema = make_schema_u64_i64();
    let out = scatter(&[], ScatterSpec::GroupKey(&[0u32]), &schema, 4);
    assert_eq!(out.len(), 4);
    assert_eq!(total_rows(&out), 0);
    assert!(
        op_relay_scatter(&[], ScatterSpec::GroupKey(&[7u32]), &schema, 4).is_err(),
        "a column the schema has not got is refused on an empty round too"
    );
}

/// Z-Set `+` across the sources, not a PK-ordered concatenation: a retraction
/// cancels its insert, a repeated (PK, payload) sums, and two payloads at one
/// key stay two elements.
#[test]
fn the_merge_arm_folds_across_sources() {
    let schema = make_schema_u64_i64();
    let nw = 4;
    // pk=1 cancels outright; pk=2 sums to 3; pk=3 carries two payloads at one
    // key, which stay two elements; pk=4 is single-sided.
    let b0 = make_batch(&schema, &[(1, 1, 10), (2, 1, 20), (3, 1, 31)]);
    let b1 = make_batch(&schema, &[(1, -1, 10), (2, 2, 20), (3, 1, 30), (4, 1, 40)]);

    let out = scatter(&[&b0, &b1], ScatterSpec::GroupKey(&[0u32]), &schema, nw);

    let mut got: Vec<(u64, i64, i64)> = Vec::new();
    for sb in out.iter().filter(|s| s.count > 0) {
        assert_eq!(sb.layout(), Layout::Consolidated, "each slice is consolidated");
        for r in 0..sb.count {
            got.push((sb.get_pk(r) as u64, payload(sb, r), sb.get_weight(r)));
        }
        // The two pk=3 elements co-locate (equal PK → equal partition) and emit
        // in payload order, not in source order.
        for r in 1..sb.count {
            let by_pk = compare_pk_bytes(sb.get_pk_bytes(r - 1), sb.get_pk_bytes(r));
            assert!(
                by_pk == Ordering::Less || (by_pk == Ordering::Equal && payload(sb, r - 1) < payload(sb, r)),
                "row {r} not strictly (PK, payload)-ascending",
            );
        }
    }
    got.sort();
    assert_eq!(got, vec![(2, 20, 3), (3, 30, 1), (3, 31, 1), (4, 40, 1)]);
}

// -----------------------------------------------------------------------
// Join-key scatter co-partition
// -----------------------------------------------------------------------

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
/// `_join_pk`, not by `GroupKeyCols::key_row` — signed-negative and >16-byte
/// composites, through both arms of the gate.
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

    // One contributing source: the linear arm.
    let one = scatter(&[&cb], ScatterSpec::JoinKey(&key), &schema, num_workers);
    check_copartition(&one, &packer, num_workers, rows.len(), Some((1, 3)), "one source");

    // Two: the N-way merge, which routes a folded group by its exemplar row.
    // Split the PK-ascending rows by parity so the walk genuinely interleaves
    // them; pk=1 and pk=3 both live in the odd source yet must still co-locate.
    let odd = make_join_key_batch(&schema, &[rows[0], rows[2], rows[4]]);
    let even = make_join_key_batch(&schema, &[rows[1], rows[3], rows[5]]);
    let two = scatter(&[&odd, &even], ScatterSpec::JoinKey(&key), &schema, num_workers);
    check_copartition(&two, &packer, num_workers, rows.len(), Some((1, 3)), "two sources");
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
    let out = scatter(&[&b], ScatterSpec::JoinKey(&key), &schema, num_workers);
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
    let pk_out = scatter(&[&pb], ScatterSpec::JoinKey(&pk_key), &pk_schema, num_workers);
    check_copartition(&pk_out, &pk_packer, num_workers, pk_rows.len(), None, "promoted PK key");
}

/// Release-only microbench of [`op_relay_scatter`] over 1M rows keyed by one I64
/// payload column — the single-column `JoinKey` route.
/// `cd crates && cargo test -p gnitz-store --release scatter_route_bench -- --ignored --nocapture --test-threads=1`
#[test]
#[ignore]
fn scatter_route_bench() {
    use std::hint::black_box;
    use std::time::Instant;

    let schema = make_schema_u64_i64();
    const N: usize = 1_000_000;
    const ITERS: usize = 20;
    let rows: Vec<(u64, i64, i64)> = (0..N)
        .map(|i| (i as u64, 1, (i as i64).wrapping_mul(2_654_435_761)))
        .collect();
    let cb = make_batch(&schema, &rows);
    let sources = [&cb];

    // Warm up (and pin the invariant: the route must not drop rows).
    let warm = scatter(&sources, ScatterSpec::JoinKey(&[(1, None)]), &schema, 4);
    assert_eq!(total_rows(&warm), N, "scatter dropped rows");

    let t = Instant::now();
    let mut acc = 0usize;
    for _ in 0..ITERS {
        let out = scatter(&sources, ScatterSpec::JoinKey(&[(1, None)]), &schema, 4);
        acc += black_box(total_rows(&out));
    }
    let secs = t.elapsed().as_secs_f64();
    println!(
        "scatter_route_bench: {:.1} Mrows/s ({N} rows × {ITERS} iters in {secs:.3}s, checksum {acc})",
        (N * ITERS) as f64 / secs / 1e6,
    );
}

/// Release-only microbench of the relay's two walks over both arms of the gate.
/// `raw` is the production default: a base-table delta reaches the relay `Raw`,
/// and an output relay ends at a reindex `Map` that downgrades. K is the
/// contributing *worker* count — K=1 (replicated, or single-worker) and K=4 are
/// the reachable range, K=16 the headroom point — and each source is a disjoint
/// stripe of one ascending key space, so the merge genuinely interleaves them.
///
/// `cd crates && cargo test -p gnitz-store --release relay_scatter_merge_bench -- --ignored --nocapture --test-threads=1`
#[test]
#[ignore]
fn relay_scatter_merge_bench() {
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
            let batches: Vec<Batch> = (0..k)
                .map(|j| {
                    let rows: Vec<(u64, i64, i64)> = (0..per).map(|i| ((i * k + j) as u64, 1, i as i64)).collect();
                    build(&schema, &rows)
                })
                .collect();
            let sources: Vec<&Batch> = batches.iter().collect();

            // Warm up, and pin the invariants the timing would otherwise hide:
            // the stripes are PK-disjoint, so nothing folds and no row is
            // dropped, and the fixture reaches the arm it is labelled for.
            let warm = scatter(&sources, ScatterSpec::GroupKey(&[0u32]), &schema, WORKERS);
            assert_eq!(total_rows(&warm), per * k, "{arm}/K={k}: scatter dropped rows");
            assert_eq!(
                sources.iter().all(|b| b.is_consolidated()),
                arm == "consolidated",
                "{arm}/K={k}: fixture on the wrong arm"
            );

            // Two timings: the scatter alone, and the scatter plus the
            // `into_consolidated` a receiving reader that needs net weights runs
            // on its slice. A certified slice returns by move where a `Raw` one
            // pays a full argsort into a fresh arena.
            for (label, consolidate) in [("scatter", false), ("scatter+consolidate", true)] {
                let t = Instant::now();
                let mut acc = 0usize;
                for _ in 0..ITERS {
                    let out = scatter(&sources, ScatterSpec::GroupKey(&[0u32]), &schema, WORKERS);
                    acc += if consolidate {
                        black_box(
                            out.into_iter()
                                .map(|b| b.into_consolidated(&schema).count)
                                .sum::<usize>(),
                        )
                    } else {
                        black_box(total_rows(&out))
                    };
                }
                let secs = t.elapsed().as_secs_f64();
                println!(
                    "relay_scatter_merge/{arm}/K{k}/{label}: {:.1} Mrows/s ({} rows × {ITERS} iters in {secs:.3}s, checksum {acc})",
                    (per * k * ITERS) as f64 / secs / 1e6,
                    per * k,
                );
            }
        }
    }
}
