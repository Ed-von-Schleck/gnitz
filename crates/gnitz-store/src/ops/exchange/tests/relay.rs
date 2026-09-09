use super::*;
use crate::schema::key::{compare_pk_bytes, ReindexPacker};
use crate::schema::{type_code, SchemaColumn, SchemaDescriptor};
use crate::test_support::{
    make_batch, make_batch_bytes, make_batch_i64pk, make_schema_pk_u64_payload_string, make_schema_u128_i64,
    make_schema_u64_i64, make_wide_batch, opk_pk, pk_payload_schema, wide_pk_3xu64_schema,
};
use gnitz_wire::{worker_for_key, worker_for_pk_bytes, TypeCode};
use std::cmp::Ordering;

/// Scatter pre-consolidated batches across workers. Every source must satisfy
/// the consolidated invariant; each output slice is consolidated too.
fn scatter_by_group_key(
    sources: &[Option<&Batch>],
    col_indices: &[u32],
    schema: &SchemaDescriptor,
    num_workers: usize,
) -> Vec<Batch> {
    op_relay_scatter_consolidated(sources, ScatterSpec::GroupKey(col_indices), schema, num_workers)
        .expect("the fixture key routes")
}

fn total_rows(batches: &[Batch]) -> usize {
    batches.iter().map(|b| b.count).sum()
}

#[test]
fn test_relay_scatter_wide_pk_order_and_routing() {
    let schema = wide_pk_3xu64_schema();
    let num_workers = 4;
    // (1,1,*) prefix-twins span the three sources: they share the leading
    // BE(1)++BE(1) 16-byte OPK prefix and differ only in the trailing column,
    // so the merge comparator must order them on the full OPK bytes rather than
    // any leading-prefix shortcut. The multi-byte values are LE/BE order
    // inversions — c2 ∈ {1,2,256,257} and c0 ∈ {2,256} — so OPK encoding is
    // load-bearing for the builder's sorted assert (a dropped BE flip sorts 256
    // before 1/2 and trips it). Additionally, b1 and b2 both carry PK (3,3,3)
    // with DISTINCT payloads (99 vs 31): a byte-EQUAL wide PK, where the payload
    // comparator alone decides the order and keeps them two elements.
    let b0 = make_wide_batch(
        &schema,
        &[
            (0, 0, 0, 1, 10),
            (1, 1, 1, 1, 11),
            (1, 1, 256, 1, 12), // shares BE(1)++BE(1) prefix with (1,1,1); c2 multi-byte
        ],
    );
    let b1 = make_wide_batch(
        &schema,
        &[
            (1, 1, 257, 1, 20), // third prefix-twin
            (2, 2, 2, 1, 21),
            (3, 3, 3, 1, 99),   // byte-equal twin of b2's (3,3,3); distinct payload 99
            (256, 0, 0, 1, 22), // c0 multi-byte: under LE this sorts before (2,2,2)
        ],
    );
    let b2 = make_wide_batch(
        &schema,
        &[
            (1, 1, 2, 1, 30), // fourth prefix-twin
            (3, 3, 3, 1, 31), // byte-equal twin of b1's (3,3,3); distinct payload 31
            (7, 0, 0, 1, 32),
        ],
    );
    let sources: Vec<Option<&Batch>> = vec![Some(&b0), Some(&b1), Some(&b2)];
    let result = scatter_by_group_key(&sources, schema.pk_indices(), &schema, num_workers);

    assert_eq!(total_rows(&result), 10);
    for (w, sb) in result.iter().enumerate() {
        // (a) Non-decreasing under the column-aware comparator.
        for r in 1..sb.count {
            let prev = sb.get_pk_bytes(r - 1);
            let cur = sb.get_pk_bytes(r);
            assert_ne!(
                compare_pk_bytes(prev, cur),
                std::cmp::Ordering::Greater,
                "worker {w} row {r} out of order",
            );
        }
        // (b) Every row routed to worker_for_pk_bytes's worker.
        for r in 0..sb.count {
            let pk = sb.get_pk_bytes(r);
            let expected = worker_for_pk_bytes(pk, num_workers);
            assert_eq!(expected, w, "wide PK routed to wrong worker");
        }
    }

    // (c) Byte-equal wide PK, distinct payloads. b1 and b2 both carry PK
    // (3,3,3), so `compare_pk_bytes` ties and the payload comparator decides:
    // the two stay separate elements, ordered 31 before 99. Both co-locate
    // (equal PK → equal partition) and emit adjacently in merge order.
    let pk_333 = opk_pk(&schema, &[3, 3, 3]);
    let mut payloads_333 = Vec::new();
    for sb in &result {
        for r in 0..sb.count {
            if sb.get_pk_bytes(r) == pk_333.as_slice() {
                payloads_333.push(i64::from_le_bytes(sb.col_data(0)[r * 8..r * 8 + 8].try_into().unwrap()));
            }
        }
    }
    assert_eq!(
        payloads_333,
        vec![31, 99],
        "byte-equal wide PK (3,3,3) must order its two copies by payload, not by source",
    );
}

/// Compound (U64, U64) PK schema: pk_stride = 16 (narrow), pk_count = 2.
/// Exercises the column-aware cached-key comparator: a raw packed-u128 `<` over
/// the wrong byte order would put the trailing column in the high bits and
/// reverse column priority — but `order_cache` packs the OPK image, so it does
/// not.
/// Pack (c0, c1) into a u128 the way `MemBatch::get_pk` would read a
/// 16-byte PK region: low 8 bytes = c0, high 8 bytes = c1.
fn mk_compound_pk(c0: u64, c1: u64) -> u128 {
    ((c1 as u128) << 64) | (c0 as u128)
}

fn make_narrow_compound_batch(schema: &SchemaDescriptor, rows: &[(u128, i64, i64)]) -> Batch {
    let mut b = Batch::with_capacity(schema, rows.len().max(1));
    for &(pk, w, val) in rows {
        // `mk_compound_pk` packs c0 in the low 8 bytes and c1 in the high 8.
        // The compound PK at rest is OPK = col0_BE ++ col1_BE, so encode the
        // two native column values through extend_pk_opk rather than writing
        // the raw u128 (which would byte-reverse the column order).
        let c0 = pk as u64 as u128;
        let c1 = (pk >> 64) as u64 as u128;
        b.extend_pk_opk(&[c0, c1]);
        b.extend_weight(&w.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &val.to_le_bytes());
        b.count += 1;
    }
    b.certify_layout(Layout::Consolidated);
    b
}

#[test]
fn test_relay_scatter_narrow_compound_pk_order_and_routing() {
    // (U64, U64) compound PK: low-8-byte collisions force the column-aware
    // comparator. (1, 10) packed = (10<<64)|1, (2, 0) packed = 2; raw u128
    // `<` would order (2, 0) before (1, 10), but lexicographic order says
    // (1, 10) < (2, 0). Likewise (1, 5) < (1, 10) < (1, 15) lexicographically.
    let schema = pk_payload_schema(&[type_code::U64; 2]);
    // A 16-byte key: the cached `pack_pk_be` is the whole PK, so the comparator
    // decides on the register compare alone and never runs the byte tiebreak.
    assert!(schema.pk_stride() <= 16, "pk_stride must be 16 (narrow)");
    assert!(schema.pk_indices().len() > 1, "fixture is a compound (multi-column) PK");
    let num_workers = 4;
    let b0 = make_narrow_compound_batch(
        &schema,
        &[
            (mk_compound_pk(1, 10), 1, 11),
            (mk_compound_pk(2, 0), 1, 12),
            (mk_compound_pk(5, 9), 1, 13),
        ],
    );
    let b1 = make_narrow_compound_batch(
        &schema,
        &[
            (mk_compound_pk(1, 5), 1, 21),
            (mk_compound_pk(1, 15), 1, 22),
            (mk_compound_pk(3, 3), 1, 23),
        ],
    );
    let b2 = make_narrow_compound_batch(
        &schema,
        &[
            (mk_compound_pk(1, 7), 1, 31),
            (mk_compound_pk(2, 2), 1, 32),
            (mk_compound_pk(7, 0), 1, 33),
        ],
    );
    let sources: Vec<Option<&Batch>> = vec![Some(&b0), Some(&b1), Some(&b2)];
    // col_indices = [0, 1] is the full PK set: exercises the compound-PK-set
    // is_pk_routing path (route_pk via worker_for_pk_bytes over the OPK bytes).
    let result = scatter_by_group_key(&sources, &[0u32, 1u32], &schema, num_workers);

    assert_eq!(total_rows(&result), 9);
    for (w, sb) in result.iter().enumerate() {
        // (a) Non-decreasing under the column-aware comparator.
        for r in 1..sb.count {
            let prev = sb.get_pk_bytes(r - 1);
            let cur = sb.get_pk_bytes(r);
            assert_ne!(
                compare_pk_bytes(prev, cur),
                Ordering::Greater,
                "worker {w} row {r} out of order (compound PK)",
            );
        }
        // (b) Routing matches worker_for_pk_bytes (the canonical PK route
        // for narrow PKs via fill_worker_indices' is_compound_pk path).
        for r in 0..sb.count {
            let pk = sb.get_pk_bytes(r);
            let expected = worker_for_pk_bytes(pk, num_workers);
            assert_eq!(expected, w, "compound PK routed to wrong worker");
        }
    }
}

fn make_schema_i64_i64() -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::I64, 0),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    )
}

#[test]
fn test_relay_scatter_i64_pk_signed_ordering() {
    // Native I64 PK with negative values. The multi-source merge walk
    // orders rows by OPK bytes (canonical signed order) — raw u128 ordering
    // would put -1 (0xFFFF...) after +1. The per-worker output is left Raw
    // (≥2 sources, PK-only merge), but each worker's rows must still be in
    // OPK order; that is what this test guards.
    let schema = make_schema_i64_i64();
    let num_workers = 4;
    // Each source sorted ascending under signed I64 order.
    let b0 = make_batch_i64pk(&schema, &[(-100, 1, 10), (-1, 1, 11), (5, 1, 12)]);
    let b1 = make_batch_i64pk(&schema, &[(-50, 1, 20), (0, 1, 21), (100, 1, 22)]);
    let sources: Vec<Option<&Batch>> = vec![Some(&b0), Some(&b1)];
    let result = scatter_by_group_key(&sources, &[0u32], &schema, num_workers);

    assert_eq!(total_rows(&result), 6);
    for (w, sb) in result.iter().enumerate() {
        for r in 1..sb.count {
            let prev = sb.get_pk(r - 1);
            let cur = sb.get_pk(r);
            assert_ne!(
                compare_pk_bytes(sb.get_pk_bytes(r - 1), sb.get_pk_bytes(r)),
                Ordering::Greater,
                "worker {w} row {r}: signed pks out of order (prev={prev:#x} cur={cur:#x})",
            );
        }
    }
}

#[test]
fn test_relay_scatter_wide_pk_single_source_bulk_drain() {
    let schema = wide_pk_3xu64_schema();
    let num_workers = 4;
    // Exactly one active source → the num_active == 1 bulk-drain path.
    // One value past 255 (c0 = 256) so the drain routes a realistic wide OPK
    // and the shared builder's sorted assert covers this path, which has no
    // ordering check of its own (under LE, 256 would sort before (7,8,9)).
    let b0 = make_wide_batch(
        &schema,
        &[
            (1, 2, 3, 1, 1),
            (4, 5, 6, 1, 2),
            (7, 8, 9, 1, 3),
            (256, 11, 12, 1, 4), // c0 multi-byte: under LE this sorts before (7,8,9)
        ],
    );
    let sources: Vec<Option<&Batch>> = vec![Some(&b0)];
    let result = scatter_by_group_key(&sources, schema.pk_indices(), &schema, num_workers);

    assert_eq!(total_rows(&result), 4);
    for (w, sb) in result.iter().enumerate() {
        for r in 0..sb.count {
            let pk = sb.get_pk_bytes(r);
            let expected = worker_for_pk_bytes(pk, num_workers);
            assert_eq!(expected, w, "bulk-drain wide PK routed to wrong worker");
        }
    }
}

#[test]
fn test_repartition_batch_pk_routing() {
    let schema = make_schema_u64_i64();
    let num_workers = 4;
    let pk_vals: &[u64] = &[1, 7, 42, 100, 255, 1024, 65537, 999983];

    let mut b = Batch::with_capacity(&schema, pk_vals.len());

    for &pk in pk_vals {
        b.extend_pk(pk as u128);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &0i64.to_le_bytes());
        b.count += 1;
    }

    let sub_batches = op_repartition_batches(&[Some(&b)], ScatterSpec::GroupKey(&[0u32]), &schema, num_workers)
        .expect("the fixture key routes");
    assert_eq!(total_rows(&sub_batches), pk_vals.len());

    for &pk in pk_vals {
        let expected = worker_for_key(pk as u128, num_workers);
        let found = (0..sub_batches[expected].count).any(|r| (sub_batches[expected].get_pk(r) as u64) == pk);
        assert!(found, "pk={pk} not found in worker {expected}");
    }
}

#[test]
fn test_repartition_batch_u128_pk() {
    let schema = make_schema_u128_i64();
    let num_workers = 4;
    let pks: &[u128] = &[
        1u128 << 64,
        (0xCAFE_BABEu128 << 64) | 0xDEAD_BEEF,
        u128::MAX,
        (7u128 << 64) | 42,
    ];

    let n = pks.len();
    let mut b = Batch::with_capacity(&schema, n);

    for &pk in pks {
        b.extend_pk(pk);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &0i64.to_le_bytes());
        b.count += 1;
    }

    let sub_batches = op_repartition_batches(&[Some(&b)], ScatterSpec::GroupKey(&[0u32]), &schema, num_workers)
        .expect("the fixture key routes");
    assert_eq!(total_rows(&sub_batches), n);

    for &pk in pks {
        let expected = worker_for_key(pk, num_workers);
        let found = (0..sub_batches[expected].count).any(|r| sub_batches[expected].get_pk(r) == pk);
        assert!(found, "pk={pk} not in worker {expected}");
    }
}

#[test]
fn test_repartition_batch_group_col() {
    let schema = make_schema_u64_i64();
    let num_workers = 4;
    let same_val: i64 = 42;

    let mut b = Batch::with_capacity(&schema, 4);

    for pk in [1u64, 2, 3, 4] {
        b.extend_pk(pk as u128);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &same_val.to_le_bytes());
        b.count += 1;
    }

    let sub_batches = op_repartition_batches(&[Some(&b)], ScatterSpec::GroupKey(&[1u32]), &schema, num_workers)
        .expect("the fixture key routes");
    assert_eq!(total_rows(&sub_batches), 4);
    let non_empty = sub_batches.iter().filter(|sb| sb.count > 0).count();
    assert_eq!(non_empty, 1, "all rows with same group key must go to one worker");
}

#[test]
fn test_repartition_batch_string_col() {
    let schema = make_schema_pk_u64_payload_string();
    let num_workers = 4;

    // Short string "hello" (≤ 12 bytes): two rows must go to same worker
    let b = make_batch_bytes(&schema, &[(1, 1, b"hello"), (2, 1, b"hello"), (3, 1, b"world")]);
    let sub_batches = op_repartition_batches(&[Some(&b)], ScatterSpec::GroupKey(&[1u32]), &schema, num_workers)
        .expect("the fixture key routes");
    assert_eq!(total_rows(&sub_batches), 3);

    let mut worker_of_1 = None;
    for (w, batch) in sub_batches.iter().enumerate().take(num_workers) {
        for r in 0..batch.count {
            if (batch.get_pk(r) as u64) == 1 {
                worker_of_1 = Some(w);
            }
        }
    }
    let w1 = worker_of_1.expect("pk=1 must be in some worker");
    let pk2_same = (0..sub_batches[w1].count).any(|r| (sub_batches[w1].get_pk(r) as u64) == 2);
    assert!(pk2_same, "same short string 'hello' must route to same worker");

    // Long string (> 12 bytes): two rows must go to same worker
    let long_str: &[u8] = b"this is a longer string for heap";
    let b2 = make_batch_bytes(&schema, &[(10, 1, long_str), (11, 1, long_str)]);
    let sub2 = op_repartition_batches(&[Some(&b2)], ScatterSpec::GroupKey(&[1u32]), &schema, num_workers)
        .expect("the fixture key routes");
    assert_eq!(total_rows(&sub2), 2);

    let mut worker_of_10 = None;
    for (w, batch) in sub2.iter().enumerate().take(num_workers) {
        for r in 0..batch.count {
            if (batch.get_pk(r) as u64) == 10 {
                worker_of_10 = Some(w);
            }
        }
    }
    let w10 = worker_of_10.expect("pk=10 must be in some worker");
    let pk11_same = (0..sub2[w10].count).any(|r| (sub2[w10].get_pk(r) as u64) == 11);
    assert!(pk11_same, "same long string must route to same worker");
}

#[test]
fn test_repartition_batch_pk_routing_propagates_flags() {
    let schema = make_schema_u64_i64(); // PK = col 0 (U64), payload = col 1 (I64)
    let b = make_batch(&schema, &[(1, 1, 10), (2, 1, 20), (3, 1, 30)]);
    assert!(b.is_consolidated());

    // PK routing (col 0 == pk_indices()): single source ⇒ the claim propagates.
    let pk_routed = op_repartition_batches(&[Some(&b)], ScatterSpec::GroupKey(&[0u32]), &schema, 4)
        .expect("the fixture key routes");
    for sb in pk_routed.iter().filter(|s| s.count > 0) {
        assert_eq!(
            sb.layout(),
            Layout::Consolidated,
            "PK-routed sub-batch must inherit consolidated"
        );
    }

    // Non-PK routing (col 1): hash distribution destroys PK order ⇒ no flags.
    let hash_routed = op_repartition_batches(&[Some(&b)], ScatterSpec::GroupKey(&[1u32]), &schema, 4)
        .expect("the fixture key routes");
    for sb in hash_routed.iter().filter(|s| s.count > 0) {
        assert_eq!(sb.layout(), Layout::Raw, "non-PK-routed sub-batch must claim nothing");
    }
}

#[test]
fn test_repartition_batches_pk_routing_single_vs_multi_source() {
    let schema = make_schema_u64_i64();
    // Single contributing source, PK routing: propagate both flags.
    let b0 = make_batch(&schema, &[(1, 1, 10), (2, 1, 20), (3, 1, 30)]);
    let single: Vec<Option<&Batch>> = vec![Some(&b0)];
    let out =
        op_repartition_batches(&single, ScatterSpec::GroupKey(&[0u32]), &schema, 4).expect("the fixture key routes");
    for sb in out.iter().filter(|s| s.count > 0) {
        assert_eq!(
            sb.layout(),
            Layout::Consolidated,
            "single-source PK-routed must inherit consolidated"
        );
    }

    // Two contributing sources, PK routing: per-source-concatenated output is
    // not globally sorted, so nothing propagates.
    let b1 = make_batch(&schema, &[(4, 1, 40), (5, 1, 50), (6, 1, 60)]);
    let multi: Vec<Option<&Batch>> = vec![Some(&b0), Some(&b1)];
    let out =
        op_repartition_batches(&multi, ScatterSpec::GroupKey(&[0u32]), &schema, 4).expect("the fixture key routes");
    for sb in out.iter().filter(|s| s.count > 0) {
        assert_eq!(sb.layout(), Layout::Raw, "multi-source scatter must claim nothing");
    }
}

#[test]
fn test_relay_scatter_consolidated_path() {
    let schema = make_schema_u64_i64();
    let num_workers = 4;
    let b0 = make_batch(&schema, &[(1, 1, 10), (5, 1, 50), (9, 1, 90)]);
    let b1 = make_batch(&schema, &[(2, 1, 20), (6, 1, 60), (10, 1, 100)]);

    let sources: Vec<Option<&Batch>> = vec![Some(&b0), Some(&b1)];
    let result = scatter_by_group_key(&sources, &[0u32], &schema, num_workers);

    assert_eq!(total_rows(&result), 6);
    for sb in result.iter().filter(|s| s.count > 0) {
        // The N-way merge folds across sources, so each slice is consolidated —
        // independently of the tag, its rows must be strictly PK-ascending here
        // (these PKs are distinct).
        assert!(sb.is_consolidated(), "multi-source merge output must be consolidated");
        for r in 1..sb.count {
            assert_eq!(
                compare_pk_bytes(sb.get_pk_bytes(r - 1), sb.get_pk_bytes(r)),
                Ordering::Less,
                "merged path output not PK-ordered at row {r}",
            );
        }
    }

    // Single contributing source: the linear route, which folds nothing because
    // its one source already is.
    let single: Vec<Option<&Batch>> = vec![Some(&b0)];
    let result = scatter_by_group_key(&single, &[0u32], &schema, num_workers);
    assert_eq!(total_rows(&result), 3);
    for sb in result.iter().filter(|s| s.count > 0) {
        assert!(sb.is_consolidated(), "single-source route output must be consolidated");
    }
}

/// The multi-source relay is Z-Set `+` across its sources, not a PK-ordered
/// concatenation: one source's retraction of another's row must cancel inside
/// the relay, and a repeated (PK, payload) must come out at the summed weight.
/// Both are what the `Consolidated` claim on each slice asserts.
#[test]
fn test_relay_scatter_folds_across_sources() {
    let schema = make_schema_u64_i64();
    let num_workers = 4;
    // pk=1 cancels outright; pk=2 sums to 3; pk=3 carries two payloads at one
    // key, which stay two elements; pk=4 is single-sided.
    let b0 = make_batch(&schema, &[(1, 1, 10), (2, 1, 20), (3, 1, 30)]);
    let b1 = make_batch(&schema, &[(1, -1, 10), (2, 2, 20), (3, 1, 31), (4, 1, 40)]);

    let result = scatter_by_group_key(&[Some(&b0), Some(&b1)], &[0u32], &schema, num_workers);

    let mut got: Vec<(u64, i64, i64)> = Vec::new();
    for sb in result.iter().filter(|s| s.count > 0) {
        assert!(sb.is_consolidated(), "each slice must be consolidated");
        for r in 0..sb.count {
            got.push((
                sb.get_pk(r) as u64,
                gnitz_wire::read_i64_le(sb.col_data(0), r * 8),
                sb.get_weight(r),
            ));
        }
    }
    got.sort();
    assert_eq!(got, vec![(2, 20, 3), (3, 30, 1), (3, 31, 1), (4, 40, 1)]);
}

#[test]
fn test_relay_scatter_fallback_path() {
    let schema = make_schema_u64_i64();
    let num_workers = 4;
    let b0 = make_batch(&schema, &[(1, 1, 10)]);
    let b1 = make_batch(&schema, &[(2, 1, 20)]);

    let sources: Vec<Option<&Batch>> = vec![Some(&b0), Some(&b1)];
    let result = op_repartition_batches(&sources, ScatterSpec::GroupKey(&[0u32]), &schema, num_workers)
        .expect("the fixture key routes");

    assert_eq!(total_rows(&result), 2);
    for sb in &result {
        if sb.count > 0 {
            assert_eq!(sb.layout(), Layout::Raw, "non-consolidated path output claims nothing");
        }
    }
}

#[test]
fn test_repartition_row_count() {
    let schema = make_schema_u64_i64();
    let rows: Vec<(u64, i64, i64)> = (1u64..=100).map(|i| (i, 1, i as i64 * 10)).collect();
    let b = make_batch(&schema, &rows);

    let sub_batches = op_repartition_batches(&[Some(&b)], ScatterSpec::GroupKey(&[0u32]), &schema, 4)
        .expect("the fixture key routes");
    assert_eq!(total_rows(&sub_batches), 100, "total rows must equal input count");
}

#[test]
fn test_repartition_routing_contract() {
    let schema = make_schema_u64_i64();
    let num_workers = 4;
    let vals: Vec<i64> = (0..64i64).map(|i| i * 997 + 1).collect();

    let mut b = Batch::with_capacity(&schema, vals.len());

    for (i, &v) in vals.iter().enumerate() {
        b.extend_pk((i + 1) as u128);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &v.to_le_bytes());
        b.count += 1;
    }

    let sub_batches = op_repartition_batches(&[Some(&b)], ScatterSpec::GroupKey(&[1u32]), &schema, num_workers)
        .expect("the fixture key routes");
    assert_eq!(total_rows(&sub_batches), vals.len());

    for &v in &vals {
        // Routing by a payload column uses the canonical route key (signed
        // columns are sign-flipped via payload_route_key) so a payload FK
        // routes identically to the same value stored as a PK column.
        let route_key = gnitz_wire::payload_route_key(&v.to_le_bytes(), 0, 8, type_code::I64);
        let expected_worker = worker_for_key(route_key, num_workers);
        let found = (0..sub_batches[expected_worker].count).any(|r| {
            i64::from_le_bytes(
                sub_batches[expected_worker].col_data(0)[r * 8..r * 8 + 8]
                    .try_into()
                    .unwrap(),
            ) == v
        });
        assert!(found, "val={v} should be in worker {expected_worker}");
    }
}

#[test]
fn test_repartition_merged_duplicate_rows() {
    let schema = make_schema_u64_i64();
    let num_workers = 4;
    // The same (PK=1, val=10) element in two sources is ONE Z-Set element at
    // weight 2, not two rows: the relay's merge folds it.
    let b0 = make_batch(&schema, &[(1, 1, 10)]);
    let b1 = make_batch(&schema, &[(1, 1, 10)]);
    let sources: Vec<Option<&Batch>> = vec![Some(&b0), Some(&b1)];
    let result = scatter_by_group_key(&sources, &[0u32], &schema, num_workers);

    assert_eq!(total_rows(&result), 1, "the shared element folds to one row");
    for sb in result.iter().filter(|s| s.count > 0) {
        assert!(sb.is_consolidated(), "the folded slice is consolidated");
        assert_eq!(sb.get_weight(0), 2, "weights sum across sources");
    }
}

#[test]
fn test_relay_scatter_consolidated_output_order() {
    let schema = make_schema_u64_i64();
    // Source 0: PKs 1, 3, 5 (odd); source 1: PKs 2, 4, 6 (even).
    // After merge-walk each worker's batch must be PK-sorted.
    let b0 = make_batch(&schema, &[(1, 1, 0), (3, 1, 0), (5, 1, 0)]);
    let b1 = make_batch(&schema, &[(2, 1, 0), (4, 1, 0), (6, 1, 0)]);
    let sources: Vec<Option<&Batch>> = vec![Some(&b0), Some(&b1)];
    let result = scatter_by_group_key(&sources, &[0u32], &schema, 4);
    for sb in &result {
        for r in 1..sb.count {
            assert!(
                sb.get_pk(r) >= sb.get_pk(r - 1),
                "output must be PK-sorted (non-decreasing) within each worker"
            );
        }
    }
}

#[test]
fn test_relay_scatter_consolidated_tie_breaking() {
    // Both sources emit PK=1. Source 0's row must appear before source 1's row
    // in every worker's output (ascending si tie-break).
    let schema = make_schema_u64_i64();
    let b0 = make_batch(&schema, &[(1, 1, 10)]);
    let b1 = make_batch(&schema, &[(1, 1, 20)]);
    let sources: Vec<Option<&Batch>> = vec![Some(&b0), Some(&b1)];
    let result = scatter_by_group_key(&sources, &[0u32], &schema, 4);
    for sb in &result {
        if sb.count >= 2 {
            assert_eq!(sb.get_pk(0), sb.get_pk(1));
            let val0 = i64::from_le_bytes(sb.col_data(0)[0..8].try_into().unwrap());
            let val1 = i64::from_le_bytes(sb.col_data(0)[8..16].try_into().unwrap());
            assert_eq!(val0, 10, "source 0 row must come first");
            assert_eq!(val1, 20, "source 1 row must come second");
        }
    }
}

#[test]
fn test_op_repartition_batches_compound_pk_routes_by_bytes() {
    // op_repartition_batches_mode must route compound-PK rows by raw PK
    // bytes (worker_for_pk_bytes), matching the write path's own route. The
    // pre-fix code only branched on (single_pk && wide), falling through to
    // the group-key hash path for narrow compound PKs — which would route to
    // a different worker than the data lives on.
    let schema = pk_payload_schema(&[type_code::U64; 2]);
    let num_workers = 4;
    let b0 = make_narrow_compound_batch(
        &schema,
        &[
            (mk_compound_pk(1, 10), 1, 11),
            (mk_compound_pk(2, 0), 1, 12),
            (mk_compound_pk(5, 9), 1, 13),
        ],
    );
    let b1 = make_narrow_compound_batch(
        &schema,
        &[
            (mk_compound_pk(1, 5), 1, 21),
            (mk_compound_pk(3, 3), 1, 23),
            (mk_compound_pk(7, 0), 1, 33),
        ],
    );
    let sources: Vec<Option<&Batch>> = vec![Some(&b0), Some(&b1)];
    // col_indices = [0, 1] = the full PK set → compound-PK routing.
    let sub_batches = op_repartition_batches(&sources, ScatterSpec::GroupKey(&[0u32, 1u32]), &schema, num_workers)
        .expect("the fixture key routes");
    assert_eq!(total_rows(&sub_batches), 6);
    for (w, sb) in sub_batches.iter().enumerate() {
        for r in 0..sb.count {
            let pk = sb.get_pk_bytes(r);
            let expected = worker_for_pk_bytes(pk, num_workers);
            assert_eq!(expected, w, "compound-PK row routed to wrong worker");
        }
    }
}

// -----------------------------------------------------------------------
// Compound join-key scatter co-partition (both production functions)
// -----------------------------------------------------------------------

/// Schema with a compound, non-PK join key spanning a signed I64 and a U128
/// column (packed stride 24 > 16). The PK is a separate U64 so the join key
/// is NOT the PK and routing goes through the `route_group` / packer arm.
fn make_join_key_schema() -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),  // col0: PK
            SchemaColumn::new(type_code::I64, 0),  // col1: signed join key part
            SchemaColumn::new(type_code::U128, 0), // col2: wide join key part
        ],
        &[0],
    )
}

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

#[test]
fn test_compound_join_promote_scatter_copartitions_both_functions() {
    // A compound join-key scatter must route every row by the SAME packed
    // OPK bytes the reindex Map writes as `_join_pk` — i.e. by
    // `worker_for_pk_bytes(ReindexPacker::pack(cols, row))`, NOT by
    // `GroupKeyCols::key_row`. Exercised through BOTH production scatter
    // functions: the non-consolidated row loop in `op_repartition_batches_mode`
    // and the consolidated merge-walk's `route_group` in
    // `op_relay_scatter_consolidated_mode` — the latter through both the
    // single-source bulk-drain AND the multi-source K-way winner-select loop
    // (the merge body the unified walk rewrites). Covers signed-negative and
    // >16-byte composite keys, including a duplicate join key that must
    // co-locate (the no-dropped-rows / matching-keys-together guarantee).
    let schema = make_join_key_schema();
    let cols = [1u32, 2u32]; // (I64, U128) join key — non-PK, stride 24.
    let num_workers = 4;
    // PK ascending (consolidated invariant). Rows 0 and 3 share the join key
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
    let expected_worker = |sb: &Batch, r: usize| -> usize {
        let mut buf = [0u8; crate::schema::MAX_PK_BYTES];
        packer.pack_into(&mut buf[..packer.out_stride], &sb.as_mem_batch(), r);
        worker_for_pk_bytes(&buf[..packer.out_stride], num_workers)
    };

    // Helper: assert every output row routed to the worker its packed key
    // dictates, no rows dropped, and the shared-key rows co-located.
    let check = |result: &[Batch], label: &str| {
        assert_eq!(total_rows(result), rows.len(), "{label}: no dropped rows");
        let mut worker_of_pk = std::collections::HashMap::new();
        for (w, sb) in result.iter().enumerate() {
            for r in 0..sb.count {
                assert_eq!(expected_worker(sb, r), w, "{label}: row routed to wrong worker");
                worker_of_pk.insert(sb.get_pk(r) as u64, w);
            }
        }
        // Rows with pk=1 and pk=3 share the join key (-5, 100): same worker.
        assert_eq!(
            worker_of_pk.get(&1),
            worker_of_pk.get(&3),
            "{label}: matching join keys must co-locate",
        );
    };

    // (a) Non-consolidated path.
    let key: Vec<gnitz_wire::ReindexSlot> = cols.iter().map(|&c| (c, None)).collect();
    let repart = op_repartition_batches(&[Some(&cb)], ScatterSpec::JoinKey(&key), &schema, num_workers)
        .expect("the fixture key routes");
    check(&repart, "op_repartition_batches");

    // (b) Consolidated merge-walk path, single source → bulk-drain `route_group`.
    let consol = op_relay_scatter_consolidated(&[Some(&cb)], ScatterSpec::JoinKey(&key), &schema, num_workers)
        .expect("the fixture key routes");
    check(&consol, "op_relay_scatter_consolidated");

    // (c) Consolidated merge-walk path, TWO sources → the K-way winner-select
    // loop drives `route_group` (the body the unified walk rewrites). Split the
    // PK-ascending rows by parity into two sorted, consolidated sources so the
    // walk genuinely interleaves them; pk=1 and pk=3 (join key (-5,100)) both
    // live in the odd source yet must still co-locate by packed `_join_pk`.
    let cb_odd = make_join_key_batch(&schema, &[rows[0], rows[2], rows[4]]); // pk 1,3,5
    let cb_even = make_join_key_batch(&schema, &[rows[1], rows[3], rows[5]]); // pk 2,4,6
    let consol_multi = op_relay_scatter_consolidated(
        &[Some(&cb_odd), Some(&cb_even)],
        ScatterSpec::JoinKey(&key),
        &schema,
        num_workers,
    )
    .expect("the fixture key routes");
    check(
        &consol_multi,
        "op_relay_scatter_consolidated (2 sources, K-way route_group)",
    );
}

/// A SINGLE promoted join key (arity 1, carrying a target `T`) must also route
/// through the `ReindexPacker` — the packer fires on promotion, not just on a
/// compound key. Covers both a payload key (`Narrow` I32 → I64, including a
/// negative value so sign-extension is exercised) and a key that is the source
/// PK (`Pk` arm), where the native-PK fast-path must be GATED OFF so the row
/// routes by its `T`-wide `_join_pk`, not its narrow PK bytes.
#[test]
fn test_single_key_promote_scatter_copartitions() {
    let num_workers = 4;

    // ---- (1) Payload key: [U64 PK, I32 payload], reindex col1 → I64. ----
    let schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I32, 0),
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
    let cb = b;
    let key = [(1u32, Some(TypeCode::I64))];

    let packer = ReindexPacker::new(&schema, &key).unwrap();
    let expected_worker = |sb: &Batch, r: usize| -> usize {
        let mut buf = [0u8; crate::schema::MAX_PK_BYTES];
        packer.pack_into(&mut buf[..packer.out_stride], &sb.as_mem_batch(), r);
        worker_for_pk_bytes(&buf[..packer.out_stride], num_workers)
    };
    let check = |result: &[Batch], label: &str| {
        assert_eq!(total_rows(result), rows.len(), "{label}: no dropped rows");
        let mut worker_of_pk = std::collections::HashMap::new();
        for (w, sb) in result.iter().enumerate() {
            for r in 0..sb.count {
                assert_eq!(
                    expected_worker(sb, r),
                    w,
                    "{label}: promoted single key routed to wrong worker"
                );
                worker_of_pk.insert(sb.get_pk(r) as u64, w);
            }
        }
        assert_eq!(
            worker_of_pk.get(&1),
            worker_of_pk.get(&4),
            "{label}: equal promoted keys must co-locate"
        );
    };
    let repart = op_repartition_batches(&[Some(&cb)], ScatterSpec::JoinKey(&key), &schema, num_workers)
        .expect("the fixture key routes");
    check(&repart, "payload-key op_repartition_batches");
    let consol = op_relay_scatter_consolidated(&[Some(&cb)], ScatterSpec::JoinKey(&key), &schema, num_workers)
        .expect("the fixture key routes");
    check(&consol, "payload-key op_relay_scatter_consolidated");

    // ---- (2) PK key: [I32 PK, U64 payload], reindex col0 → I64. The native
    // PK fast-path must be gated off so routing uses the packed I64 key. ----
    let pk_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::I32, 0),
            SchemaColumn::new(type_code::U64, 0),
        ],
        &[0],
    );
    // OPK order: i32::MIN sign-flips to 0x0000_0000 → sorts first, then ascending.
    let pk_rows: &[(i32, u64)] = &[(i32::MIN, 9), (-5, 9), (-1, 9), (3, 9)];
    let mut pb = Batch::with_capacity(&pk_schema, pk_rows.len());
    for &(pk, v) in pk_rows {
        let mut opk = [0u8; 4];
        gnitz_wire::encode_pk_column(&pk.to_le_bytes(), type_code::I32, &mut opk);
        pb.extend_pk_bytes(&opk);
        pb.extend_weight(&1i64.to_le_bytes());
        pb.extend_null_bmp(&0u64.to_le_bytes());
        pb.extend_col(0, &v.to_le_bytes());
        pb.count += 1;
    }
    pb.certify_layout(Layout::Consolidated);
    let pk_cb = pb;
    let pk_key = [(0u32, Some(TypeCode::I64))];
    let pk_packer = ReindexPacker::new(&pk_schema, &pk_key).unwrap();
    let pk_repart = op_repartition_batches(&[Some(&pk_cb)], ScatterSpec::JoinKey(&pk_key), &pk_schema, num_workers)
        .expect("the fixture key routes");
    assert_eq!(total_rows(&pk_repart), pk_rows.len(), "PK-key: no dropped rows");
    for (w, sb) in pk_repart.iter().enumerate() {
        for r in 0..sb.count {
            let mut buf = [0u8; crate::schema::MAX_PK_BYTES];
            pk_packer.pack_into(&mut buf[..pk_packer.out_stride], &sb.as_mem_batch(), r);
            assert_eq!(
                worker_for_pk_bytes(&buf[..pk_packer.out_stride], num_workers),
                w,
                "PK-key fast-path must be gated: routed by native PK not packed T"
            );
        }
    }
}

/// Release-only microbench of the single-column `JoinKey` scatter route: the
/// whole of `op_relay_scatter_consolidated_mode` over 1M rows keyed by one I64
/// payload column.
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
    let sources = [Some(&cb)];

    // Warm up (and pin the invariant: the route must not drop rows).
    let warm = op_relay_scatter_consolidated(&sources, ScatterSpec::JoinKey(&[(1, None)]), &schema, 4)
        .expect("the fixture key routes");
    assert_eq!(total_rows(&warm), N, "scatter dropped rows");

    let t = Instant::now();
    let mut acc = 0usize;
    for _ in 0..ITERS {
        let out = op_relay_scatter_consolidated(&sources, ScatterSpec::JoinKey(&[(1, None)]), &schema, 4)
            .expect("the fixture key routes");
        acc += black_box(total_rows(&out));
    }
    let secs = t.elapsed().as_secs_f64();
    println!(
        "scatter_route_bench: {:.1} Mrows/s ({N} rows × {ITERS} iters in {secs:.3}s, checksum {acc})",
        (N * ITERS) as f64 / secs / 1e6,
    );
}

/// Release-only microbench for the consolidated relay's two routes: the linear
/// sweep at one contributing source, and the shared N-way merge above it. K is
/// the contributing *worker* count, so K=1 (a replicated source, or a
/// single-worker run) and K=4 are the reachable range; K=16 is the headroom
/// point. Each source is a disjoint stripe of one ascending key space — the
/// shape a `CREATE VIEW` backfill's per-worker chunks and a set-op-fed
/// input-delta scatter both produce — so the merge genuinely interleaves them.
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

    for k in [1usize, 4, 16] {
        let per = N / k;
        let batches: Vec<Batch> = (0..k)
            .map(|j| {
                let rows: Vec<(u64, i64, i64)> = (0..per).map(|i| ((i * k + j) as u64, 1, i as i64)).collect();
                make_batch(&schema, &rows)
            })
            .collect();
        let sources: Vec<Option<&Batch>> = batches.iter().map(Some).collect();

        // Warm up, and pin the invariant the timing would otherwise hide: the
        // stripes are PK-disjoint, so nothing folds and no row is dropped.
        let warm = op_relay_scatter_consolidated(&sources, ScatterSpec::GroupKey(&[0u32]), &schema, WORKERS)
            .expect("the fixture key routes");
        assert_eq!(total_rows(&warm), per * k, "K={k}: scatter dropped rows");

        // Two timings: the scatter alone, and the scatter plus the
        // `into_consolidated` the receiving side runs on each slice
        // (`dag::exec::run_side`). The second is the one the layout contract
        // moves — a certified slice returns by move where a `Raw` one pays a
        // full argsort into a fresh arena.
        for (label, consolidate) in [("scatter", false), ("scatter+consolidate", true)] {
            let t = Instant::now();
            let mut acc = 0usize;
            for _ in 0..ITERS {
                let out = op_relay_scatter_consolidated(&sources, ScatterSpec::GroupKey(&[0u32]), &schema, WORKERS)
                    .expect("the fixture key routes");
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
                "relay_scatter_merge/K{k}/{label}: {:.1} Mrows/s ({} rows × {ITERS} iters in {secs:.3}s, checksum {acc})",
                (per * k * ITERS) as f64 / secs / 1e6,
                per * k,
            );
        }
    }
}
