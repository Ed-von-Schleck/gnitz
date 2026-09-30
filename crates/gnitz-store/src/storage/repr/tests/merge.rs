/// A malformed long cell relocates to the canonical empty string.
#[test]
fn test_malformed_long_string_fallback_clean_header() {
    let mut src_cell = [0u8; 16];
    // length = 20  (> SHORT_STRING_THRESHOLD = 12)
    src_cell[0..4].copy_from_slice(&20u32.to_le_bytes());
    // prefix = 0xDEADBEEF  (stale garbage that must be zeroed on fallback)
    src_cell[4..8].copy_from_slice(&0xDEAD_BEEF_u32.to_le_bytes());
    // heap_offset = 999  (well beyond src_blob)
    src_cell[8..16].copy_from_slice(&999u64.to_le_bytes());

    let src_blob = vec![0u8; 4]; // far too small
    let mut dst_blob: Vec<u8> = Vec::new();

    let result = relocate_german_string_vec(&src_cell, &src_blob, &mut dst_blob, None);

    assert_eq!(
        u32::from_le_bytes(result[0..4].try_into().unwrap()),
        0,
        "fallback must emit length=0",
    );
    assert_eq!(&result[4..8], &[0u8; 4], "fallback must zero the prefix field");
    assert_eq!(&result[8..16], &[0u8; 8], "fallback must leave blob offset zero");
    assert!(dst_blob.is_empty(), "fallback must not extend dst_blob");
    assert!(gnitz_wire::german_string_cell_ok(&result, &dst_blob));
}

/// A relocated cell is rebuilt, so a skewed pad, which would split one element's
/// weight across two rows, does not survive it.
#[test]
fn test_relocate_canonicalizes_pad_bytes() {
    let mut dst_blob: Vec<u8> = Vec::new();
    for content in [&b""[..], b"a", b"abc", b"abcd", b"abcdefghijkl"] {
        let clean = gnitz_wire::encode_german_string(content, &mut Vec::new());
        let mut dirty = clean;
        for b in dirty[4 + content.len()..16].iter_mut() {
            *b = 0xFF;
        }
        assert_eq!(
            relocate_german_string_vec(&dirty, &[], &mut dst_blob, None),
            clean,
            "relocation must rebuild {content:?} in canonical form",
        );
    }
    assert!(dst_blob.is_empty(), "inline cells must not touch the blob");
}

#[test]
fn test_relocate_german_string_vec_cache_hit_dedups() {
    // Long-string source: length=20, payload at offset 0 in src_blob.
    let payload: &[u8] = b"hello world test dat";
    assert_eq!(payload.len(), 20);
    let src_blob: Vec<u8> = payload.to_vec();

    let mut src_cell = [0u8; 16];
    src_cell[0..4].copy_from_slice(&20u32.to_le_bytes());
    src_cell[4..8].copy_from_slice(&payload[0..4]);
    src_cell[8..16].copy_from_slice(&0u64.to_le_bytes());

    let mut dst_blob: Vec<u8> = Vec::new();
    let mut cache = BlobCache::new(0);

    let r1 = relocate_german_string_vec(&src_cell, &src_blob, &mut dst_blob, Some(&mut cache));
    let after_first = dst_blob.len();
    assert_eq!(after_first, 20, "first call must append payload exactly once");

    let r2 = relocate_german_string_vec(&src_cell, &src_blob, &mut dst_blob, Some(&mut cache));
    assert_eq!(dst_blob.len(), 20, "cache hit must not append a second copy");
    assert_eq!(&r1[8..16], &r2[8..16], "both calls must return the same offset");
}

#[test]
fn prorated_blob_cap_is_the_rounded_up_share_within_the_heap() {
    assert_eq!(prorated_blob_cap(1000, 100, 10), 100);
    assert_eq!(
        prorated_blob_cap(10, 100, 5),
        5,
        "a sub-byte share rounds up to a byte per row"
    );
    assert_eq!(prorated_blob_cap(100, 10, 20), 100, "never more than the whole heap");
    assert_eq!(prorated_blob_cap(usize::MAX, 2, usize::MAX), usize::MAX, "no overflow");
    assert_eq!(prorated_blob_cap(0, 10, 5), 0);
    assert_eq!(prorated_blob_cap(10, 0, 5), 0);
}

/// A cache takes a pooled map only at its first relocation, and returns it on
/// drop.
#[test]
fn blob_cache_takes_a_pooled_map_only_when_it_relocates() {
    let pooled = || {
        BLOB_CACHE_POOL.with(|p| {
            let items = p.take();
            let n = items.len();
            p.set(items);
            n
        })
    };
    BLOB_CACHE_POOL.with(|p| {
        p.set(VecDeque::from([SpanMap::with_capacity_and_hasher(
            16,
            Default::default(),
        )]))
    });

    drop(BlobCache::new(8));
    assert_eq!(pooled(), 1, "a cache that relocates nothing leaves the pool alone");

    let payload: &[u8] = b"hello world test dat";
    let mut src_cell = [0u8; 16];
    src_cell[0..4].copy_from_slice(&20u32.to_le_bytes());
    src_cell[4..8].copy_from_slice(&payload[0..4]);
    let mut cache = BlobCache::new(8);
    relocate_german_string_vec(&src_cell, payload, &mut Vec::new(), Some(&mut cache));
    assert_eq!(pooled(), 0, "the first relocation takes the pooled map");
    drop(cache);
    assert_eq!(pooled(), 1, "drop returns it");

    let mut widest = BlobCache::new(usize::MAX);
    widest.map();
    drop(widest);
    assert_eq!(pooled(), 1, "a map reserved to the cap is still pooled");
    BLOB_CACHE_POOL.with(|p| p.take());
}

use super::*;
use crate::schema::payload_order::compare_full_rows;
use crate::schema::{SchemaColumn, SchemaDescriptor, TypeCode};
use crate::storage::BatchBuilder;
use crate::test_support::{
    bench_time, make_batch_opk, make_batch_u128_raw, make_schema_pk_u64_payload_string, make_schema_u128_i64,
    make_schema_u64_i64, make_string_batch, opk_pk, payload0_i64, pk_payload_schema, pk_u64_two_i64_schema,
    read_german_string, zset_of, RowKey,
};

/// `MemBatch`'s per-row accessors address the cells its region accessors hold:
/// the [`BatchView`] contract.
#[test]
fn batchview_row_matches_region() {
    let schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),  // PK
            SchemaColumn::new(TypeCode::I32, false),  // payload slot 0, 4 bytes
            SchemaColumn::new(TypeCode::U128, false), // payload slot 1, 16 bytes
            SchemaColumn::new(TypeCode::I64, true),   // payload slot 2, 8 bytes, nullable
        ],
        &[0],
    );
    const ROWS: usize = 5;
    let mut bb = crate::storage::BatchBuilder::new(schema);
    for row in 0..ROWS {
        bb.begin_row(row as u128, 1);
        bb.put_int(-(row as i32) as u128);
        bb.put_int((row as u128) << 100);
        // NULL slot 2 on the odd rows, so the bitmap is not uniformly zero.
        if row % 2 == 0 {
            bb.put_int(row as u128);
        } else {
            bb.put_null();
        }
        bb.end_row();
    }
    let b = bb.finish();
    let pk_vals: Vec<u128> = (0..ROWS as u128).collect();
    gnitz_expr::assert_batchview_consistent(
        &b.as_mem_batch(),
        ROWS,
        &[(0, 4), (1, 16), (2, 8)],
        &[(TypeCode::U64, 0, &pk_vals)],
    );
}

/// `n` sorted rows in PK groups of `dup`, distinct only in the last payload
/// column, so every compare within a group walks the whole payload.
fn bench_sorted_batch(schema: &SchemaDescriptor, n: usize, dup: usize) -> Batch {
    let last = schema.num_payload_cols() - 1;
    let mut b = BatchBuilder::new(*schema);
    for i in 0..n {
        let group = (i / dup) as u128;
        b.begin_row(group, 1);
        for (pi, col) in schema.payload_columns() {
            match (pi == last, col.type_code) {
                (true, _) => b.put_int(i as u128),
                (false, TypeCode::F64) => b.put_float(group as f64),
                (false, _) => b.put_int(group),
            }
        }
        b.end_row();
    }
    b.finish()
}

/// `run_merge`'s compare loop alone — the emit materializes nothing — under a
/// high duplicate-PK rate, over each payload comparator arm.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn run_merge_dup_pk_bench() {
    const K: usize = 4;
    const N: usize = 200_000;
    const DUP: usize = 8;
    const ITERS: usize = 20;
    // A float, a U128 and a nullable column each force the `Generic` arm.
    let generic = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::F64, false),
            SchemaColumn::new(TypeCode::U128, false),
            SchemaColumn::new(TypeCode::I64, true),
        ],
        &[0],
    );
    for (label, schema) in [
        ("fixed_int_stride8", make_schema_u64_i64()),
        ("fixed_int_stride16", make_schema_u128_i64()),
        ("generic", generic),
    ] {
        let batches: Vec<Batch> = (0..K).map(|_| bench_sorted_batch(&schema, N, DUP)).collect();
        let mem: Vec<MemBatch<'_>> = batches.iter().map(|b| b.as_mem_batch()).collect();
        let mut sink = 0i64;
        let elapsed = bench_time(ITERS, || {
            run_merge(&mem, &schema, |_s, _r, w| {
                sink = sink.wrapping_add(std::hint::black_box(w));
            });
        });
        std::hint::black_box(sink);
        let (rows, secs) = (K * N, elapsed.as_secs_f64());
        let rps = (ITERS as f64 * rows as f64) / secs;
        println!("run_merge_{label}: {rows} rows × {ITERS} iters in {secs:.3}s = {rps:.0} rows/s");
    }
}

/// Build an owned `Batch` of `n` rows for `make_schema_flush`: PK from
/// `key_fn(i)`, weight +1, both I64 payload columns derived from the row
/// index (payloads are irrelevant when all PKs are distinct).
fn bench_flush_batch(schema: &SchemaDescriptor, n: usize, key_fn: impl Fn(usize) -> u64) -> Batch {
    let mut b = BatchBuilder::new(*schema);
    for i in 0..n {
        b.begin_row(key_fn(i) as u128, 1i64);
        b.put_int(i as i64 as u128);
        b.put_int(((i as i64) * 2) as u128);
        b.end_row();
    }
    b.finish()
}

/// The N-way merge of one big run of even keys with four small runs of distinct
/// odd keys spread across it, priced per small-run row.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn merge_batches_skewed_bench() {
    use std::hint::black_box;

    let schema = pk_u64_two_i64_schema();
    for (n, d, iters) in [
        (16_384usize, 16usize, 400usize),
        (65_536, 16, 100),
        (262_144, 16, 25),
        (262_144, 4096, 25),
    ] {
        let big = bench_flush_batch(&schema, n, |i| (2 * i) as u64);
        let stride = (n / d).max(1);
        let mut batches: Vec<Batch> = Vec::with_capacity(5);
        batches.push(big);
        for j in 0..4usize {
            batches.push(bench_flush_batch(&schema, d, move |i| {
                ((i * stride) * 2 + 1 + 2 * j) as u64
            }));
        }

        let mem: Vec<MemBatch<'_>> = batches.iter().map(|b| b.as_mem_batch()).collect();
        let total_rows: usize = mem.iter().map(|b| b.count).sum();

        // The untimed warmup takes the allocator, batch pool and page-in.
        let secs = bench_time(iters, || {
            black_box(merge_batches(&mem, &schema));
        })
        .as_secs_f64();

        let rps = (iters as f64 * total_rows as f64) / secs;
        let ns_per_delta = secs * 1e9 / (iters as f64 * 4.0 * d as f64);
        println!("merge_skew/N{n}_d{d}: {total_rows} rows  {rps:.0} merge-rows/s  {ns_per_delta:.0} ns/delta-row");
    }
}

/// The `(pk_bytes, weight, i64_payload)` of each row of a single-payload-column
/// batch, at any PK stride.
fn i64_rows(b: &Batch) -> Vec<(Vec<u8>, i64, i64)> {
    (0..b.count)
        .map(|i| (b.get_pk_bytes(i).to_vec(), b.get_weight(i), payload0_i64(b, i)))
        .collect()
}

/// [`i64_rows`] with each PK widened to its native unsigned value.
fn narrow(b: &Batch) -> Vec<(u128, i64, i64)> {
    i64_rows(b)
        .into_iter()
        .map(|(pk, w, v)| (gnitz_wire::widen_pk_be(&pk), w, v))
        .collect()
}

/// The N-way merge's survivors, materialized with every string relocated.
fn merge_batches(batches: &[MemBatch], schema: &SchemaDescriptor) -> Batch {
    let total = batches.iter().map(|b| b.count).sum();
    let mut survivors: Vec<(u32, u32, i64)> = Vec::with_capacity(total);
    run_merge(batches, schema, |src, row, w| {
        survivors.push((src as u32, row as u32, w))
    });
    super::super::scatter::UnifiedSet::whole(batches, schema).materialize(&survivors, total)
}

fn merge_all(batches: &[Batch], schema: &SchemaDescriptor) -> Batch {
    let mem: Vec<MemBatch<'_>> = batches.iter().map(|b| b.as_mem_batch()).collect();
    merge_batches(&mem, schema)
}

/// The two-way Z-set `+`, folded left over consolidated operands. Addition is
/// associative, so it reaches the N-way merge's result.
fn merged_pairwise(batches: &[Batch], schema: &SchemaDescriptor) -> Batch {
    batches.iter().fold(Batch::empty_with_schema(schema), |acc, b| {
        acc.merged_consolidated(&b.clone().into_consolidated(), schema)
    })
}

/// `got` is the Z-set sum of `inputs`: one row per element, strictly ascending
/// in (PK, payload) order.
fn assert_folds(inputs: &[Batch], got: &Batch, what: &str) {
    let schema = got.schema();
    let mut want: std::collections::HashMap<RowKey, i64> = std::collections::HashMap::new();
    for b in inputs {
        for (key, w) in zset_of(b, schema) {
            *want.entry(key).or_insert(0) += w;
        }
    }
    want.retain(|_, w| *w != 0);
    assert_eq!(zset_of(got, schema), want, "{what}: the Z-set sum");
    assert_eq!(got.count, want.len(), "{what}: one row per element");
    let mb = got.as_mem_batch();
    for r in 1..got.count {
        assert_eq!(
            compare_full_rows(schema, &mb, r - 1, &mb, r),
            Ordering::Less,
            "{what}: rows {} and {r} out of order",
            r - 1
        );
    }
}

/// The Z-set fold, as `(label, inputs, expected output)`.
type FoldCase<'a> = (&'a str, &'a [&'a [(u128, i64, i64)]], &'a [(u128, i64, i64)]);

const FOLD_CASES: &[FoldCase] = &[
    (
        "a single source passes through",
        &[&[(10, 1, 100), (20, 1, 200), (30, 1, 300)]],
        &[(10, 1, 100), (20, 1, 200), (30, 1, 300)],
    ),
    (
        "three sources interleave",
        &[
            &[(10, 1, 100), (40, 1, 400)],
            &[(20, 1, 200), (50, 1, 500)],
            &[(30, 1, 300), (60, 1, 600)],
        ],
        &[
            (10, 1, 100),
            (20, 1, 200),
            (30, 1, 300),
            (40, 1, 400),
            (50, 1, 500),
            (60, 1, 600),
        ],
    ),
    (
        "weights sum across sources",
        &[&[(10, 1, 100)], &[(10, 2, 100)]],
        &[(10, 3, 100)],
    ),
    (
        "a ghost drops and its neighbours survive",
        &[&[(10, 1, 100), (20, 1, 200)], &[(10, -1, 100)], &[(30, 1, 300)]],
        &[(20, 1, 200), (30, 1, 300)],
    ),
    ("no sources at all", &[], &[]),
    (
        "an empty source beside a live one",
        &[&[], &[(10, 1, 100)]],
        &[(10, 1, 100)],
    ),
    (
        "the PK's high word separates",
        &[&[(10, 1, 100)], &[((1u128 << 64) | 10, 1, 200)]],
        &[(10, 1, 100), ((1u128 << 64) | 10, 1, 200)],
    ),
    (
        "a zero-weight input row never reaches the writer",
        &[&[(10, 0, 100), (20, 1, 200)]],
        &[(20, 1, 200)],
    ),
    (
        "duplicates within one source fold",
        &[&[(10, 1, 100), (10, 1, 100), (20, 1, 200)]],
        &[(10, 2, 100), (20, 1, 200)],
    ),
    (
        "one PK's payloads interleave across sources and fold apart",
        &[&[(5, 1, 100)], &[(5, 1, 200)], &[(5, -1, 100)]],
        &[(5, 1, 200)],
    ),
    (
        "one PK with distinct payloads stays distinct",
        &[&[(10, 1, 100), (10, 1, 200), (20, 1, 300)]],
        &[(10, 1, 100), (10, 1, 200), (20, 1, 300)],
    ),
];

#[test]
fn nway_merge_folds_the_zset() {
    let schema = make_schema_u128_i64();
    for &(what, inputs, want) in FOLD_CASES {
        let batches: Vec<Batch> = inputs.iter().map(|rows| make_batch_u128_raw(&schema, rows)).collect();
        assert_eq!(narrow(&merge_all(&batches, &schema)), want, "{what}");
        assert_eq!(narrow(&merged_pairwise(&batches, &schema)), want, "{what}: pairwise");
    }
}

/// `consolidate_groups` reaches the same fold from one unsorted batch: it
/// sorts by (PK, payload) first, so every [`FOLD_CASES`] expectation holds over
/// the concatenated, shuffled inputs.
#[test]
fn consolidate_groups_folds_the_zset() {
    let schema = make_schema_u128_i64();
    for &(what, inputs, want) in FOLD_CASES {
        let mut rows: Vec<(u128, i64, i64)> = inputs.concat();
        rows.reverse(); // arrive unsorted; the sort is the point
        assert_eq!(
            narrow(&make_batch_u128_raw(&schema, &rows).into_consolidated()),
            want,
            "{what}"
        );
    }
}

// `MAX_PK_COLUMNS == 5`, so the 64/80-byte strides use U128 PK columns
// rather than 8/10 U64 columns.
const WIDE_24: &[TypeCode] = &[TypeCode::U64, TypeCode::U64, TypeCode::U64];
const WIDE_64: &[TypeCode] = &[TypeCode::U128, TypeCode::U128, TypeCode::U128, TypeCode::U128];
const WIDE_80: &[TypeCode] = &[
    TypeCode::U128,
    TypeCode::U128,
    TypeCode::U128,
    TypeCode::U128,
    TypeCode::U128,
];

/// Every `pk_width_dispatch` arm orders by OPK bytes through all three entry
/// points. The payloads are all equal, so only the PK compare keeps keys apart.
#[test]
fn every_pk_width_orders_by_opk_bytes() {
    use TypeCode::*;
    // (PK column types, PK column values in strictly ascending OPK order).
    let cases: &[(&[TypeCode], &[&[u128]])] = &[
        (&[U8], &[&[0], &[1], &[127], &[128], &[255]]),
        (&[U16], &[&[0], &[1], &[256], &[32768], &[65535]]),
        (&[U32], &[&[0], &[1], &[1 << 16], &[1 << 31], &[u32::MAX as u128]]),
        // Compound, stride 8: the leading column dominates.
        (&[U32, U32], &[&[1, 9], &[1, 10], &[2, 0]]),
        // Stride 11 — a width that is neither a power of two nor a register size.
        (&[U64, U16, U8], &[&[1, 2, 2], &[1, 2, 3], &[1, 3, 0], &[2, 0, 0]]),
        // Stride 16: col0 must dominate. A regression to a plain numeric compare
        // over the packed u128 would order col1-major and disagree here.
        (&[U64, U64], &[&[1, 256], &[1, 257], &[256, 1], &[256, 2]]),
        // Past 16 bytes, keys sharing their leading 16.
        (WIDE_24, &[&[1, 1, 0], &[1, 1, 100], &[1, 2, 0], &[2, 0, 0]]),
        (WIDE_64, &[&[1, 0, 0, 0], &[1, 0, 0, 1], &[1, 0, 0, 2], &[2, 0, 0, 0]]),
        (WIDE_80, &[&[1, 1, 0, 0, 0], &[1, 1, 0, 0, 5], &[1, 1, 0, 1, 0]]),
    ];

    for &(tcs, vals) in cases {
        let schema = pk_payload_schema(tcs);
        let stride = schema.pk_stride();
        let want: Vec<Vec<u8>> = vals.iter().map(|v| opk_pk(&schema, v)).collect();
        let pks = |b: Batch| -> Vec<Vec<u8>> { (0..b.count).map(|i| b.get_pk_bytes(i).to_vec()).collect() };
        // Fed in descending order, so nothing passes by arriving sorted.
        let desc: Vec<(&Vec<u8>, i64, i64)> = want.iter().rev().map(|k| (k, 1, 0)).collect();
        let asc: Vec<(&Vec<u8>, i64, i64)> = want.iter().map(|k| (k, 1, 0)).collect();

        // One single-row source per key: the N-way merge orders them.
        let batches: Vec<Batch> = desc
            .iter()
            .map(|r| make_batch_opk(&schema, std::slice::from_ref(r)))
            .collect();
        assert_eq!(pks(merge_all(&batches, &schema)), want, "stride {stride}: merge");
        assert_eq!(
            pks(make_batch_opk(&schema, &desc).into_consolidated()),
            want,
            "stride {stride}: consolidate_groups"
        );
        // Already in order on the way in: the sort must be a no-op, not a
        // reshuffle of the equal-key groups.
        assert_eq!(
            pks(make_batch_opk(&schema, &asc).into_consolidated()),
            want,
            "stride {stride}: consolidate_groups (pre-sorted)"
        );
    }
}

// -----------------------------------------------------------------------
// Columnar materialization of strings and NULLs
// -----------------------------------------------------------------------

/// `(pk, weight, string, blob, int)`; `None` is NULL.
type MatRow = (u64, i64, Option<&'static [u8]>, Option<&'static [u8]>, Option<i64>);

const LONG_A: &[u8] = b"long_string_value_A_padpadpadpad"; // > 12 → spills to blob
const LONG_DUP: &[u8] = b"duplicated_long_string_payload_zz"; // > 12, reused across columns

/// Three consolidated runs: pk=10 folds by string content across runs, pk=40
/// cancels.
const MAT_RUNS: [&[MatRow]; 3] = [
    &[
        (10, 1, Some(b""), Some(LONG_A), Some(100)),
        (20, 2, Some(b"short"), Some(LONG_DUP), None),
        (30, 1, None, Some(b"abc"), Some(-5)),
        (40, 1, Some(b"ghost"), Some(b"ghost2"), Some(1)),
    ],
    &[
        (10, 3, Some(b""), Some(LONG_A), Some(100)),
        (25, 1, Some(LONG_DUP), Some(b"x"), Some(9)),
        (50, 1, Some(b"last"), None, None),
    ],
    &[
        (40, -1, Some(b"ghost"), Some(b"ghost2"), Some(1)),
        (60, 1, None, Some(b"tail"), None),
    ],
];

/// The relocating and the carrying scatter both materialize the Z-set sum, at a
/// stride-16 PK and at a stride-24 one whose keys share their leading 16 bytes.
#[test]
fn merge_materializes_the_zset_sum() {
    for pk_cols in [&[TypeCode::U128][..], &[TypeCode::U64; 3]] {
        let mut cols: Vec<SchemaColumn> = pk_cols.iter().map(|&tc| SchemaColumn::new(tc, false)).collect();
        cols.extend([
            SchemaColumn::new(TypeCode::String, true),
            SchemaColumn::new(TypeCode::Blob, true),
            SchemaColumn::new(TypeCode::I64, true),
        ]);
        let pk: Vec<u32> = (0..pk_cols.len() as u32).collect();
        let schema = SchemaDescriptor::new(&cols, &pk);

        let runs: Vec<Batch> = MAT_RUNS
            .iter()
            .map(|rows| {
                let mut b = BatchBuilder::new(schema);
                for &(k, w, s, x, n) in rows.iter() {
                    let mut key = vec![0u128; pk_cols.len()];
                    key[pk_cols.len() - 1] = k as u128;
                    b.begin_row_bytes(&opk_pk(&schema, &key), w);
                    for cell in [s, x] {
                        match cell {
                            Some(v) => b.put_blob(v),
                            None => b.put_null(),
                        }
                    }
                    b.put_opt_int(n.map(|v| v as u128));
                    b.end_row();
                }
                let mut b = b.finish();
                b.certify_layout(Layout::Consolidated);
                b
            })
            .collect();
        let mem: Vec<MemBatch<'_>> = runs.iter().map(|b| b.as_mem_batch()).collect();
        let stride = schema.pk_stride();
        let relocated = merge_batches(&mem, &schema);
        assert_folds(&runs, &relocated, &format!("stride {stride}: relocating scatter"));
        assert_folds(
            &runs,
            &merge_consolidated(&mem, &schema),
            &format!("stride {stride}: carrying scatter"),
        );
        assert_eq!(relocated.count, 6, "stride {stride}: pk=10 folds, pk=40 cancels");
    }
}

// -----------------------------------------------------------------------
// Random batches at every PK width
// -----------------------------------------------------------------------

mod fold_proptest {
    use super::*;
    use proptest::prelude::*;

    /// One schema per `pk_width_dispatch` arm: `≤8` (strides 1, 4, 8), `9..=16`,
    /// `17..=32` and the `>32` byte fallback.
    fn schemas() -> Vec<SchemaDescriptor> {
        vec![
            pk_payload_schema(&[TypeCode::U8]),
            pk_payload_schema(&[TypeCode::I64]),
            pk_payload_schema(&[TypeCode::I32]),
            pk_payload_schema(&[TypeCode::U32, TypeCode::U64]),
            pk_payload_schema(&[TypeCode::U64, TypeCode::I32]),
            pk_payload_schema(WIDE_24),
            pk_payload_schema(WIDE_80),
        ]
    }

    /// Rows over eight keys differing only in their first and last byte, with
    /// small weight and payload domains, so folds and ghost cancels are common.
    fn arb_rows(stride: usize) -> impl Strategy<Value = Vec<(Vec<u8>, i64, i64)>> {
        let row = (0u8..2, 0u8..4, -3i64..=3i64, 0i64..4i64);
        (
            prop::collection::vec(any::<u8>(), stride),
            prop::collection::vec(row, 0..40),
        )
            .prop_map(move |(base, rows)| {
                rows.into_iter()
                    .map(|(lead, tail, w, v)| {
                        let mut pk = base.clone();
                        pk[0] = lead;
                        pk[stride - 1] = base[stride - 1].wrapping_add(tail);
                        (pk, w, v)
                    })
                    .collect()
            })
    }

    proptest! {
        /// `consolidate_groups` over one batch, and the N-way and pairwise
        /// merges over its consolidated chunks, all materialize its Z-set.
        #[test]
        fn every_fold_path_reaches_the_zset(
            (si, rows) in (0usize..schemas().len()).prop_flat_map(|si| {
                let stride = schemas()[si].pk_stride();
                (Just(si), arb_rows(stride))
            })
        ) {
            let s = schemas()[si];
            let whole = make_batch_opk(&s, &rows);
            assert_folds(std::slice::from_ref(&whole), &whole.clone().into_consolidated(), "consolidate");
            let runs: Vec<Batch> = rows.chunks(13).map(|c| make_batch_opk(&s, c).into_consolidated()).collect();
            assert_folds(&runs, &merge_all(&runs, &s), "N-way merge");
            assert_folds(&runs, &merged_pairwise(&runs, &s), "pairwise merge");
        }
    }
}

// ── Carried heaps: dead-byte accounting and read-back ───────────────────

/// Every row as `(pk, weight, bytes)`.
fn rows_of(b: &Batch) -> Vec<(u128, i64, Vec<u8>)> {
    (0..b.count)
        .map(|row| {
            let s = read_german_string(b, 0, row);
            (b.get_pk(row), b.get_weight(row), s)
        })
        .collect()
}

/// A pair that cancels leaves both its rows' spans behind; a pair that sums
/// keeps one and leaves the other's.
#[test]
fn merged_consolidated_charges_the_rows_the_fold_drops() {
    let schema = make_schema_pk_u64_payload_string();
    let (x, y) = ([b'x'; 20], [b'y'; 30]);
    let a = make_string_batch(&[(1, 1, &x), (2, 1, &y)]);

    let cancel = a.merged_consolidated(&make_string_batch(&[(1, -1, &x)]), &schema);
    assert_eq!(rows_of(&cancel), [(2, 1, y.to_vec())]);
    assert_eq!(cancel.dead_heap, 2 * x.len(), "both rows of the cancelled pair");

    let summed = a.merged_consolidated(&make_string_batch(&[(1, 2, &x)]), &schema);
    assert_eq!(rows_of(&summed), [(1, 3, x.to_vec()), (2, 1, y.to_vec())]);
    assert_eq!(summed.dead_heap, x.len(), "the other side's copy of the summed row");
}

/// A merge that drops nothing carries both heaps whole with nothing dead.
#[test]
fn an_append_only_merge_charges_nothing() {
    let schema = make_schema_pk_u64_payload_string();
    let a = make_string_batch(&[(1, 1, &[b'a'; 20]), (3, 1, &[b'c'; 20])]);
    let b = make_string_batch(&[(2, 1, &[b'b'; 20]), (4, 1, &[b'd'; 20])]);
    let out = a.merged_consolidated(&b, &schema);
    assert_eq!(out.dead_heap, 0);
    assert_eq!(out.blob().len(), a.blob().len() + b.blob().len());
    assert_eq!(out.count, 4);
}

/// The carried arm reads back what Z-set `+` defines, across galloped runs of
/// either side and a shared-PK group that interleaves both sides and folds an
/// equal element — for both carried heaps, and with each side relocating.
#[test]
fn a_carried_merge_reads_back_every_string() {
    use std::collections::BTreeMap;
    let schema = make_schema_pk_u64_payload_string();
    let v = |c: u8, n: usize| vec![c; n];
    let (p, q, r, s, t) = (v(b'p', 14), v(b'q', 40), v(b'r', 25), v(b's', 33), v(b't', 17));
    let a_rows: Vec<(u64, i64, &[u8])> = vec![(1, 1, &p), (2, 1, b"short"), (5, 1, &q), (5, 2, &s), (9, 1, &t)];
    let b_rows: Vec<(u64, i64, &[u8])> = vec![(3, 1, &r), (4, 1, &p), (5, 1, &r), (5, -2, &s), (8, 1, &q)];
    let mut want: BTreeMap<(u128, Vec<u8>), i64> = BTreeMap::new();
    for &(pk, w, bytes) in a_rows.iter().chain(&b_rows) {
        *want.entry((pk as u128, bytes.to_vec())).or_default() += w;
    }
    let want: Vec<(u128, i64, Vec<u8>)> = want
        .into_iter()
        .filter(|&(_, w)| w != 0)
        .map(|((pk, bytes), w)| (pk, w, bytes))
        .collect();

    for (pad_a, pad_b) in [(0, 0), (1000, 0), (0, 1000)] {
        let pad = |b: Batch, n: usize| {
            let mut b = b;
            b.blob.extend(std::iter::repeat_n(0u8, n));
            b.dead_heap += n;
            b
        };
        let a = pad(make_string_batch(&a_rows), pad_a);
        let b = pad(make_string_batch(&b_rows), pad_b);
        let out = a.merged_consolidated(&b, &schema);
        assert_eq!(rows_of(&out), want, "padding ({pad_a}, {pad_b})");
        assert!(
            out.blob().len() < 1000,
            "a wasteful side relocates rather than carrying its padding"
        );
    }
}
