// The malformed-long-string fallback must zero both the length field
// (dest[0..4]) and the prefix field (dest[4..8]) — a prefix left holding the
// corrupt source cell's bytes would compare unequal to a canonical empty
// string that reads identically.
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

/// The relocator rebuilds a cell rather than copying its bytes, so a skewed
/// pad — compare-visible but invisible to `german_string_content`, i.e. the
/// shape that splits one Z-set element's weight across two rows — cannot
/// survive a merge, scatter or map projection.
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

use super::super::batch::{Batch, Layout};
use super::*;
use crate::schema::key::compare_pk_bytes;
use crate::schema::payload_order::compare_full_rows;
use crate::schema::{SchemaColumn, SchemaDescriptor, TypeCode};
use crate::storage::BatchBuilder;
use crate::test_support::{make_schema_u128_i64, make_string_batch, pk_payload_schema, pk_u64_two_i64_schema};

/// Build an owned `Batch` from a row tuple list. Tests obtain a `MemBatch`
/// view via `batch.as_mem_batch()`.
fn make_batch_i64(rows: &[(u128, i64, i64)]) -> Batch {
    crate::test_support::make_batch_u128_raw(&make_schema_u128_i64(), rows)
}

/// `MemBatch`'s per-row accessors must address exactly the cell its region
/// accessors hold — the [`BatchView`] contract, checked through the shared
/// assertion so this batch and every other implementor (notably the client
/// adapter, whose `col_data` is a slot map rather than a flat region) are
/// held to one rule by one piece of code. Here it is near-tautological: both
/// sides derive the same payload-region offset, so it catches an index typo.
///
/// It lives in `merge.rs`, not `schema.rs`: constructing a `Batch` there would
/// be the first `schema → storage` code reference in that file, the up-edge
/// the locator/batch split exists to prevent. `#[cfg(test)]` does not exempt it.
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

/// Build a large sorted `(PK | I64)` batch with a high duplicate-PK rate:
/// `dup` consecutive rows share a PK with distinct ascending payloads, which
/// puts the equal-PK payload tiebreak and the group fold on the measured path.
/// The schema's `pk_stride` selects the width.
fn bench_sorted_batch(schema: &SchemaDescriptor, n: usize, dup: usize) -> Batch {
    let mut b = BatchBuilder::new(*schema);
    for i in 0..n {
        b.begin_row((i / dup) as u128, 1i64);
        b.put_int(i as i64 as u128);
        b.end_row();
    }
    b.finish()
}

/// Throughput of `run_merge`'s comparator-driven N-way merge at stride 8 and
/// 16 with a high duplicate-PK rate, using a cheap emit (no materialization) so
/// the measured cost is the loser-tree compare loop, not the row copy. A
/// regression guard for `compare_pk_ordering` (the merge has no `current_key`,
/// so this is the comparator change undiluted).
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn run_merge_dup_pk_bench() {
    use std::time::Instant;
    const K: usize = 4;
    const N: usize = 200_000;
    const DUP: usize = 8;
    const ITERS: usize = 20;
    let s8 = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::I64, false),
        ],
        &[0],
    );
    for (label, schema) in [("stride8", s8), ("stride16", make_schema_u128_i64())] {
        let batches: Vec<Batch> = (0..K).map(|_| bench_sorted_batch(&schema, N, DUP)).collect();
        let mem: Vec<MemBatch<'_>> = batches.iter().map(|b| b.as_mem_batch()).collect();
        let sorted: Vec<MemBatch> = mem.to_vec();
        let mut sink = 0i64;
        let t = Instant::now();
        for _ in 0..ITERS {
            run_merge(&sorted, &schema, |_s, _r, w| {
                sink = sink.wrapping_add(std::hint::black_box(w));
            });
        }
        let secs = t.elapsed().as_secs_f64();
        std::hint::black_box(sink);
        let rows = K * N;
        let rps = (ITERS as f64 * rows as f64) / secs;
        println!("run_merge_{label}: {rows} rows × {ITERS} iters in {secs:.3}s = {rps:.0} rows/s");
    }
}

/// A payload that forces `compare_rows` onto its `Generic` arm: a float, a
/// 128-bit column and a nullable one each disqualify the fixed-int fast path.
fn bench_generic_schema() -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::F64, false),
            SchemaColumn::new(TypeCode::U128, false),
            SchemaColumn::new(TypeCode::I64, true),
        ],
        &[0],
    )
}

/// [`bench_sorted_batch`] for [`bench_generic_schema`]: the two leading payload
/// columns are constant within a PK group, so a tie walks the float and the
/// 128-bit dispatch before the trailing I64 resolves it.
fn bench_sorted_generic_batch(schema: &SchemaDescriptor, n: usize, dup: usize) -> Batch {
    let mut b = BatchBuilder::new(*schema);
    for i in 0..n {
        let group = (i / dup) as u64;
        b.begin_row(group as u128, 1);
        b.put_float(group as f64);
        b.put_int(group as u128);
        b.put_int(i as u128);
        b.end_row();
    }
    b.finish()
}

/// [`run_merge_dup_pk_bench`] over the `Generic` payload comparator, which the
/// benches above never reach: their non-nullable `I64` payload takes the
/// fixed-int fast path instead of `cmp_col_window`.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn run_merge_dup_pk_generic_bench() {
    const K: usize = 4;
    const N: usize = 200_000;
    const DUP: usize = 8;
    const ITERS: usize = 20;
    let schema = bench_generic_schema();
    let batches: Vec<Batch> = (0..K).map(|_| bench_sorted_generic_batch(&schema, N, DUP)).collect();
    let sorted: Vec<MemBatch<'_>> = batches.iter().map(|b| b.as_mem_batch()).collect();
    let mut sink = 0i64;
    let elapsed = crate::test_support::bench_time(ITERS, || {
        run_merge(&sorted, &schema, |_s, _r, w| {
            sink = sink.wrapping_add(std::hint::black_box(w));
        });
    });
    std::hint::black_box(sink);
    let (rows, secs) = (K * N, elapsed.as_secs_f64());
    let rps = (ITERS as f64 * rows as f64) / secs;
    println!("run_merge_generic: {rows} rows × {ITERS} iters in {secs:.3}s = {rps:.0} rows/s");
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

/// The N-way merge over **1 big run + 4 small runs**, priced per small-run row.
/// Big run = `N` even keys; each small run = `d` odd keys uniformly interleaved
/// across the big range (distinct per run via a `+2j` offset), so no
/// (PK,payload) ties and consolidation drops nothing.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn merge_batches_skewed_bench() {
    use super::super::batch::write_to_batch;
    use std::hint::black_box;
    use std::time::Instant;

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
        let sorted: Vec<MemBatch> = mem.to_vec();
        let total_rows: usize = sorted.iter().map(|b| b.count).sum();

        // Warm up the path once (allocator, batch pool, page-in); untimed.
        black_box(write_to_batch(&schema, total_rows, 0, |w| {
            merge_batches(&sorted, &schema, w);
        }));

        let t = Instant::now();
        for _ in 0..iters {
            let b = write_to_batch(&schema, total_rows, 0, |w| {
                merge_batches(&sorted, &schema, w);
            });
            black_box(&b);
        }
        let secs = t.elapsed().as_secs_f64();

        let rps = (iters as f64 * total_rows as f64) / secs;
        let ns_per_delta = secs * 1e9 / (iters as f64 * 4.0 * d as f64);
        println!("merge_skew/N{n}_d{d}: {total_rows} rows  {rps:.0} merge-rows/s  {ns_per_delta:.0} ns/delta-row");
    }
}

/// A merge/consolidate result as native `(PK, weight, payload)` rows.
fn narrow(rows: Vec<(Vec<u8>, i64, i64)>, pk_stride: usize) -> Vec<(u128, i64, i64)> {
    rows.into_iter()
        .map(|(pk, w, v)| (crate::test_support::read_pk_opk(&pk, 0, pk_stride), w, v))
        .collect()
}

fn merge_to_rows(batches: &[Batch], schema: &SchemaDescriptor) -> Vec<(u128, i64, i64)> {
    narrow(merge_to_rows_wide(batches, schema), schema.pk_stride())
}

fn run_consolidate(b: &Batch, schema: &SchemaDescriptor) -> Vec<(u128, i64, i64)> {
    narrow(run_consolidate_bytes(b, schema), schema.pk_stride())
}

/// The Z-set fold, as `(inputs, expected output)`. Weights of matching
/// (PK, payload) elements sum, net-zero elements drop, and rows sharing a PK but
/// differing in payload stay distinct and order by payload. These are properties
/// of the fold alone — `merge::drive`'s group boundary is width-agnostic (an
/// OPK byte compare plus the payload comparator), which the width sweep below
/// covers.
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
        "one PK with distinct payloads stays distinct",
        &[&[(10, 1, 100), (10, 1, 200), (20, 1, 300)]],
        &[(10, 1, 100), (10, 1, 200), (20, 1, 300)],
    ),
];

#[test]
fn nway_merge_folds_the_zset() {
    let schema = make_schema_u128_i64();
    for &(what, inputs, want) in FOLD_CASES {
        let batches: Vec<Batch> = inputs.iter().map(|rows| make_batch_i64(rows)).collect();
        assert_eq!(merge_to_rows(&batches, &schema), want, "{what}");
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
        assert_eq!(run_consolidate(&make_batch_i64(&rows), &schema), want, "{what}");
    }
}

// -----------------------------------------------------------------------
// Narrow-region merge dispatch: signed / narrow-unsigned / compound
// -----------------------------------------------------------------------

/// OPK-encode a single PK column's native little-endian bytes. The PK region is
/// OPK at rest, so a fixture must store OPK bytes for the merge/sort path to
/// order it the way an ingested row would be ordered.
fn opk_pk(le: &[u8], tc: TypeCode) -> Vec<u8> {
    let mut out = vec![0u8; le.len()];
    gnitz_wire::encode_pk_column(le, tc, &mut out);
    out
}

/// Build a batch from owned `(pk_bytes, weight, payload)` rows.
fn make_batch_bytes(schema: &SchemaDescriptor, rows: &[(Vec<u8>, i64, i64)]) -> Batch {
    let borrowed: Vec<(&[u8], i64, i64)> = rows.iter().map(|(pk, w, v)| (&pk[..], *w, *v)).collect();
    crate::test_support::make_batch_opk(schema, &borrowed)
}

/// The payload column of each output row, which every ordering case below uses
/// to record the rank its PK should land at.
fn ranks(rows: &[(Vec<u8>, i64, i64)]) -> Vec<i64> {
    rows.iter().map(|r| r.2).collect()
}

/// Every PK width, through all three entry points, ordered against its OPK bytes.
///
/// What varies here is *width*, not type. On the PK axis the merge is type-blind
/// — it compares OPK bytes and dispatches only on the stride — so the strides
/// below cover one case per `pk_width_dispatch` arm (`≤8`, `9..=16`, `17..=32`,
/// `>32`). That a given type's OPK image sorts like its native values is a
/// property of the encoder, pinned in `schema::key`, not re-derived here.
#[test]
fn every_pk_width_orders_by_opk_bytes() {
    // Signed values are passed as their two's-complement image: `opk_pk` reads
    // the low `size()` little-endian bytes and applies the sign flip.
    let s8 = |v: i8| (v as u8) as u128;
    let s64 = |v: i64| (v as u64) as u128;

    // (PK column types, PK column values in strictly ascending OPK order).
    let cases: Vec<(&[TypeCode], Vec<Vec<u128>>)> = vec![
        (&[TypeCode::U8], vec![vec![0], vec![1], vec![127], vec![128], vec![255]]),
        // The sign flip is the only reason negatives sort first.
        (
            &[TypeCode::I8],
            vec![vec![s8(-128)], vec![s8(-1)], vec![0], vec![1], vec![s8(127)]],
        ),
        (
            &[TypeCode::U16],
            vec![vec![0], vec![1], vec![256], vec![32768], vec![65535]],
        ),
        (
            &[TypeCode::U32],
            vec![vec![0], vec![1], vec![1 << 16], vec![1 << 31], vec![u32::MAX as u128]],
        ),
        (
            &[TypeCode::I64],
            vec![
                vec![s64(i64::MIN)],
                vec![s64(-1)],
                vec![0],
                vec![1],
                vec![s64(i64::MAX)],
            ],
        ),
        // Compound, stride 8: the leading column dominates.
        (
            &[TypeCode::U32, TypeCode::U32],
            vec![vec![1, 9], vec![1, 10], vec![2, 0]],
        ),
        // Stride 11 — a width that is neither a power of two nor a register size.
        (
            &[TypeCode::U64, TypeCode::U16, TypeCode::U8],
            vec![vec![1, 2, 2], vec![1, 2, 3], vec![1, 3, 0], vec![2, 0, 0]],
        ),
        // Stride 16: col0 must dominate. A regression to a plain numeric compare
        // over the packed u128 would order col1-major and disagree here.
        (
            &[TypeCode::U64, TypeCode::U64],
            vec![vec![1, 256], vec![1, 257], vec![256, 1], vec![256, 2]],
        ),
        // Past 16 bytes the leading-prefix compare ties and the byte tiebreak
        // decides: every pair below agrees on its first two columns.
        (
            WIDE_24,
            vec![vec![1, 1, 0], vec![1, 1, 100], vec![1, 2, 0], vec![2, 0, 0]],
        ),
        (
            WIDE_80,
            vec![vec![1, 1, 0, 0, 0], vec![1, 1, 0, 0, 5], vec![1, 1, 0, 1, 0]],
        ),
    ];

    for (tcs, vals) in cases {
        let schema = pk_payload_schema(tcs);
        let stride = schema.pk_stride();
        let n = vals.len();
        let opk = |i: usize| crate::test_support::opk_pk(&schema, &vals[i]);
        let want: Vec<i64> = (0..n as i64).collect();
        // Feed every path in descending order, so nothing passes by arriving sorted.
        let shuffled: Vec<(Vec<u8>, i64, i64)> = (0..n).rev().map(|i| (opk(i), 1, i as i64)).collect();
        let sorted: Vec<(Vec<u8>, i64, i64)> = (0..n).map(|i| (opk(i), 1, i as i64)).collect();

        // One single-row source per key: the N-way merge orders them.
        let batches: Vec<Batch> = shuffled
            .iter()
            .map(|r| make_batch_bytes(&schema, std::slice::from_ref(r)))
            .collect();
        let m = merge_to_rows_wide(&batches, &schema);
        assert_eq!(ranks(&m), want, "stride {stride}: merge");

        let c = run_consolidate_bytes(&make_batch_bytes(&schema, &shuffled), &schema);
        assert_eq!(ranks(&c), want, "stride {stride}: consolidate_groups");

        // Already in order on the way in: the sort must be a no-op, not a
        // reshuffle of the equal-key groups.
        let f = run_consolidate_bytes(&make_batch_bytes(&schema, &sorted), &schema);
        assert_eq!(ranks(&f), want, "stride {stride}: consolidate_groups (pre-sorted)");

        // The PK survives as its OPK image, not as some re-packing of it.
        let got_pks: Vec<Vec<u8>> = m.into_iter().map(|r| r.0).collect();
        assert_eq!(
            got_pks,
            (0..n).map(opk).collect::<Vec<_>>(),
            "stride {stride}: pk bytes"
        );
    }
}

// -----------------------------------------------------------------------
// Wide-region merge dispatch (pk_stride > 16). Ordering and group detection
// rest on `compare_pk_bytes` over the whole key. The PKs below are built so the
// leading 16 bytes collide while a later column differs, which keeps the
// leading-prefix fast reject from firing and puts the full-byte compare on the
// path.
// -----------------------------------------------------------------------

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

/// OPK PK bytes: col0 = `lead`, last col = `tail`, all middle columns 0.
/// Each unsigned column is stored big-endian (its OPK form), concatenated
/// in pk-list order. The packed low-16 prefix is fully determined by the
/// leading columns (col0, plus zeros), so two PKs sharing `lead` but
/// differing in `tail` collide in the prefix yet are distinct under
/// `compare_pk_bytes` (which orders col0 → … → last). Holds for every
/// stride here.
fn wpk(tcs: &[TypeCode], lead: u128, tail: u128) -> Vec<u8> {
    let last = tcs.len() - 1;
    let mut v = Vec::new();
    for (i, &tc) in tcs.iter().enumerate() {
        let val: u128 = if i == 0 {
            lead
        } else if i == last {
            tail
        } else {
            0
        };
        let cw = tc.wire_stride();
        // Big-endian = OPK for an unsigned column: take the low `cw` bytes
        // of the BE u128 representation (the value fits within `cw` here).
        v.extend_from_slice(&val.to_be_bytes()[16 - cw..]);
    }
    v
}

/// Run `run` against a fresh single-payload-column `DirectWriter` and return
/// the `(pk_bytes, weight, i64_payload)` of each output row. Width-agnostic:
/// drives both narrow and wide strides (callers build the schema explicitly).
fn writer_run(
    schema: &SchemaDescriptor,
    total_rows: usize,
    run: impl FnOnce(&mut DirectWriter),
) -> Vec<(Vec<u8>, i64, i64)> {
    let stride = schema.pk_stride();
    let rows = total_rows.max(1);
    let mut out_pk = vec![0u8; rows * stride];
    let mut out_w = vec![0u8; rows * 8];
    let mut out_n = vec![0u8; rows * 8];
    let mut out_c = vec![0u8; rows * 8];
    let mut out_b: Vec<u8> = Vec::with_capacity(1);
    let count;
    {
        let mut writer = DirectWriter::new(
            &mut out_pk,
            &mut out_w,
            &mut out_n,
            vec![&mut out_c],
            &mut out_b,
            schema,
            0,
        );
        run(&mut writer);
        count = writer.count;
    }
    (0..count)
        .map(|i| {
            let pk = out_pk[i * stride..(i + 1) * stride].to_vec();
            let w = gnitz_wire::read_i64_le(&out_w, i * 8);
            let v = gnitz_wire::read_i64_le(&out_c, i * 8);
            (pk, w, v)
        })
        .collect()
}

/// One-shot merge into a writer whose arena is already sized to the Σ-input
/// upper bound, for the tests and microbench that pre-size their writer.
fn merge_batches(batches: &[MemBatch], schema: &SchemaDescriptor, writer: &mut DirectWriter) {
    let mut survivors: Vec<(u32, u32, i64)> = Vec::with_capacity(batches.iter().map(|b| b.count).sum());
    run_merge(batches, schema, |src, row, w| {
        survivors.push((src as u32, row as u32, w))
    });
    let mut cols = Vec::new();
    let unified: Vec<UnifiedSource> = batches
        .iter()
        .map(|b| mem_batch_to_unified(b, schema, &mut cols))
        .collect();
    super::super::scatter::scatter_unified_sources(&unified, &cols, &survivors, writer);
}

fn merge_to_rows_wide(batches: &[Batch], schema: &SchemaDescriptor) -> Vec<(Vec<u8>, i64, i64)> {
    let mem: Vec<MemBatch<'_>> = batches.iter().map(|b| b.as_mem_batch()).collect();
    let sorted: Vec<MemBatch> = mem.to_vec();
    let total: usize = sorted.iter().map(|b| b.count).sum();
    writer_run(schema, total, |w| {
        merge_batches(&sorted, schema, w);
    })
}

/// Byte-form consolidation harness: returns `(pk_bytes, weight, i64_payload)`
/// per output row. Width-agnostic (sibling of the packed-`u64` `run_consolidate`
/// above, for PK shapes whose bytes don't fit a single `u64`).
fn run_consolidate_bytes(b: &Batch, schema: &SchemaDescriptor) -> Vec<(Vec<u8>, i64, i64)> {
    let mb = b.as_mem_batch();
    let total = mb.count;
    let mut survivors: Vec<(u32, u32, i64)> = Vec::with_capacity(total);
    consolidate_groups(&mb, schema, &mut survivors);
    let mut cols = Vec::with_capacity(schema.num_payload_cols());
    let unified = [mem_batch_to_unified(&mb, schema, &mut cols)];
    writer_run(schema, total, |w| {
        super::super::scatter::scatter_unified_sources(&unified, &cols, &survivors, w);
    })
}

#[test]
fn wide_nway_merge_ordering_low16_collision() {
    for (tcs, want_stride) in [(WIDE_24, 24usize), (WIDE_64, 64), (WIDE_80, 80)] {
        let s = pk_payload_schema(tcs);
        assert_eq!(s.pk_stride(), want_stride);
        // A < B < C < D < E by compare_pk_bytes. B,C,D share col0
        // (= the low-16 prefix); only the trailing column distinguishes
        // them, so the packed-prefix reject cannot separate them.
        let a = wpk(tcs, 0, 0);
        let b = wpk(tcs, 1, 0);
        let c = wpk(tcs, 1, 1);
        let d = wpk(tcs, 1, 2);
        let e = wpk(tcs, 2, 0);
        // Three sorted batches, each internally ordered.
        let b1 = make_batch_bytes(&s, &[(a.clone(), 1, 0), (c.clone(), 1, 2)]);
        let b2 = make_batch_bytes(&s, &[(b.clone(), 1, 1), (e.clone(), 1, 4)]);
        let b3 = make_batch_bytes(&s, &[(d.clone(), 1, 3)]);
        let out = merge_to_rows_wide(&[b1, b2, b3], &s);
        assert_eq!(
            out.len(),
            5,
            "stride={want_stride}: distinct low16-colliding PKs must not fold"
        );
        assert_eq!(
            out.iter().map(|r| r.2).collect::<Vec<_>>(),
            vec![0, 1, 2, 3, 4],
            "stride={want_stride}: payload order tracks compare_pk_bytes order",
        );
        let expect_pks = [a, b, c, d, e];
        for (i, r) in out.iter().enumerate() {
            assert_eq!(r.0, expect_pks[i], "stride={want_stride}: pk row {i}");
            assert_eq!(r.1, 1, "stride={want_stride}: weight row {i}");
        }
        // Output is fully ordered under the column-aware comparator.
        for w in out.windows(2) {
            assert_eq!(
                compare_pk_bytes(&w[0].0, &w[1].0),
                Ordering::Less,
                "stride={want_stride}: output not strictly ordered by compare_pk_bytes",
            );
        }
    }
}

#[test]
fn wide_distinct_pk_identical_payload_not_folded() {
    // Different wide PKs sharing their 16-byte prefix, carrying an identical
    // payload — so `eq_payload` cannot hold them apart and only the `same_pk`
    // closure's full-byte compare can. A prefix-only compare folds them.
    let tcs = WIDE_24;
    let s = pk_payload_schema(tcs);
    let p1 = wpk(tcs, 7, 0);
    let p2 = wpk(tcs, 7, 1);
    let b1 = make_batch_bytes(&s, &[(p1.clone(), 1, 42)]);
    let b2 = make_batch_bytes(&s, &[(p2.clone(), 1, 42)]);
    let out = merge_to_rows_wide(&[b1, b2], &s);
    assert_eq!(out.len(), 2, "prefix-colliding distinct PKs must stay separate");
    assert_eq!(out[0], (p1, 1, 42));
    assert_eq!(out[1], (p2, 1, 42));
}

#[test]
fn test_merge_same_pk_nonadjacent_payload_interleave() {
    let schema = make_schema_u128_i64();
    let b1 = make_batch_i64(&[(5, 1, 100)]);
    let b2 = make_batch_i64(&[(5, 1, 200)]);
    let b3 = make_batch_i64(&[(5, -1, 100)]);

    let result = merge_to_rows(&[b1, b2, b3], &schema);
    assert_eq!(
        result.len(),
        1,
        "val=100 +1/-1 pair must cancel; only val=200 survives, got {result:?}"
    );
    assert_eq!(result[0], (5, 1, 200));
}

#[test]
fn wide_consolidate_prefix_collision_and_identical_payload() {
    let tcs = WIDE_24;
    let s = pk_payload_schema(tcs);
    // Pre-sorted: (1,1,0) and (1,1,1) collide in low 16 AND carry an
    // identical payload (val=0); they must remain two distinct rows.
    // (1,1,2) is a third distinct PK.
    let p0 = wpk(tcs, 1, 0);
    let p1 = wpk(tcs, 1, 1);
    let p2 = wpk(tcs, 1, 2);
    let b = make_batch_bytes(&s, &[(p0.clone(), 1, 0), (p1.clone(), 1, 0), (p2.clone(), 1, 5)]);
    let out = run_consolidate_bytes(&b, &s);
    assert_eq!(out.len(), 3, "distinct prefix-colliding PKs must not fold");
    assert_eq!(out[0], (p0, 1, 0));
    assert_eq!(out[1], (p1, 1, 0));
    assert_eq!(out[2], (p2, 1, 5));

    // Adjacent same PK + same payload DOES fold; net-zero drops.
    let q = wpk(tcs, 2, 2);
    let r = wpk(tcs, 3, 3);
    let b2 = make_batch_bytes(
        &s,
        &[
            (q.clone(), 1, 7),
            (q.clone(), 1, 7),
            (r.clone(), 1, -1),
            (r.clone(), -1, -1),
        ],
    );
    let out2 = run_consolidate_bytes(&b2, &s);
    assert_eq!(out2, vec![(q, 2, 7)]);
}

// -----------------------------------------------------------------------
// Columnar materialization against an independent oracle
//
// `run_merge` + `scatter_unified_sources` is the tree's only column-at-a-time
// materializer, and this is its adversarial exercise: a schema of two
// German-string columns plus a nullable int, over runs carrying long / inline /
// empty / duplicate strings, and null STRING and int cells.
// The oracle is the same argsort-fold-drop-ghosts reference `consolidate_*`
// checks against, generalized to this schema and run over the runs'
// concatenation, so it shares no code with the merge it judges.
//
// Every assertion is on decoded content, never on raw blob/struct bytes: those
// differ once two STRING columns spill into one heap.
// -----------------------------------------------------------------------
mod merge_materialize_vs_reference {
    use super::*;

    /// A payload cell for a German-string column.
    enum S {
        /// Non-null value with this content.
        V(Vec<u8>),
        Null,
    }

    /// A payload cell for the nullable fixed-width int column.
    enum I {
        V(i64),
        Null,
    }

    struct RowSpec {
        pk: Vec<u8>,
        w: i64,
        c0: S,
        c1: S,
        c2: I,
    }

    fn put_str(b: &mut BatchBuilder, c: &S) {
        match c {
            S::V(bytes) => b.put_blob(bytes),
            S::Null => b.put_null(),
        }
    }

    /// Build a consolidated run. Rows must be in ascending PK order (PKs are
    /// distinct within a run here, so PK order == (PK, payload) order) and carry
    /// non-zero weights — the certify debug-verifies both.
    fn build_run(schema: &SchemaDescriptor, rows: Vec<RowSpec>) -> Batch {
        let mut b = BatchBuilder::new(*schema);
        for row in &rows {
            b.begin_row_bytes(&row.pk, row.w);
            put_str(&mut b, &row.c0);
            put_str(&mut b, &row.c1);
            match row.c2 {
                I::V(v) => b.put_int(v as u128),
                I::Null => b.put_null(),
            }
            b.end_row();
        }
        let mut b = b.finish();
        b.certify_layout(Layout::Consolidated);
        b
    }

    struct OutBufs {
        count: usize,
        pk_stride: usize,
        pk: Vec<u8>,
        nb: Vec<u8>,
        wt: Vec<u8>,
        cols: Vec<Vec<u8>>,
        blob: Vec<u8>,
    }

    fn materialize(
        schema: &SchemaDescriptor,
        total_rows: usize,
        total_blob: usize,
        run: impl FnOnce(&mut DirectWriter),
    ) -> OutBufs {
        let pk_stride = schema.pk_stride();
        let rows = total_rows.max(1);
        let mut pk = vec![0u8; rows * pk_stride];
        let mut wt = vec![0u8; rows * 8];
        let mut nb = vec![0u8; rows * 8];
        let col_sizes: Vec<usize> = schema.payload_columns().map(|(_, c)| c.size() as usize).collect();
        let mut cols: Vec<Vec<u8>> = col_sizes.iter().map(|&cs| vec![0u8; rows * cs]).collect();
        let mut blob: Vec<u8> = Vec::with_capacity(total_blob.max(1));
        let count;
        {
            let col_refs: Vec<&mut [u8]> = cols.iter_mut().map(|c| c.as_mut_slice()).collect();
            let mut writer = DirectWriter::new(&mut pk, &mut wt, &mut nb, col_refs, &mut blob, schema, total_rows);
            run(&mut writer);
            count = writer.count;
        }
        OutBufs { count, pk_stride, pk, nb, wt, cols, blob }
    }

    #[derive(Debug, PartialEq)]
    enum CellVal {
        Null,
        Str(Vec<u8>),
        Int(i64),
    }

    /// One decoded row: `(pk_bytes, weight, null_word, cells)`. Null cells decode
    /// to `Null` from the null bit alone (cell bytes ignored); strings decode to
    /// content against `blob` — never the raw struct/offset bytes, which two
    /// independent materializations legitimately place differently.
    type Row = (Vec<u8>, i64, u64, Vec<CellVal>);

    fn decode_cells(
        schema: &SchemaDescriptor,
        nw: u64,
        blob: &[u8],
        cell: impl Fn(usize, usize) -> Vec<u8>,
    ) -> Vec<CellVal> {
        schema
            .payload_columns()
            .map(|(pi, col)| {
                let cs = col.size() as usize;
                if gnitz_wire::null_word_get(nw, pi) {
                    CellVal::Null
                } else if col.type_code.is_german_string() {
                    let st: [u8; 16] = cell(pi, 16).try_into().unwrap();
                    CellVal::Str(gnitz_wire::try_decode_german_string(&st, blob).expect("valid string"))
                } else {
                    CellVal::Int(i64::from_le_bytes(cell(pi, cs).try_into().unwrap()))
                }
            })
            .collect()
    }

    fn decode(schema: &SchemaDescriptor, out: &OutBufs) -> Vec<Row> {
        (0..out.count)
            .map(|i| {
                let pk = out.pk[i * out.pk_stride..(i + 1) * out.pk_stride].to_vec();
                let w = gnitz_wire::read_i64_le(&out.wt, i * 8);
                let nw = gnitz_wire::read_u64_le(&out.nb, i * 8);
                let cells = decode_cells(schema, nw, &out.blob, |pi, cs| {
                    out.cols[pi][i * cs..i * cs + cs].to_vec()
                });
                (pk, w, nw, cells)
            })
            .collect()
    }

    /// The independent oracle: concatenate the runs into one batch, argsort it by
    /// `compare_full_rows`, fold equal (PK, payload) groups and
    /// drop the net-zero ones — the same reference shape `consolidate_reference`
    /// uses, decoded to this module's three-payload-column rows.
    fn reference_fold(schema: &SchemaDescriptor, runs: &[Batch]) -> Vec<Row> {
        let mut all = Batch::with_capacity(schema, runs.iter().map(|b| b.count).sum());
        for r in runs {
            all.append_batch(r, 0, r.count);
        }
        let mb = all.as_mem_batch();
        let n = mb.count;
        let mut idx: Vec<usize> = (0..n).collect();
        idx.sort_by(|&x, &y| compare_full_rows(schema, &mb, x, &mb, y));
        let mut out = Vec::new();
        let mut i = 0;
        while i < n {
            let head = idx[i];
            let mut w = mb.get_weight(head);
            i += 1;
            while i < n && compare_full_rows(schema, &mb, head, &mb, idx[i]) == Ordering::Equal {
                w += mb.get_weight(idx[i]);
                i += 1;
            }
            if w != 0 {
                let nw = mb.get_null_word(head);
                let cells = decode_cells(schema, nw, mb.blob, |pi, cs| mb.get_col_ptr(head, pi, cs).to_vec());
                out.push((mb.get_pk_bytes(head).to_vec(), w, nw, cells));
            }
        }
        out
    }

    /// Materialize `runs` through the merge + columnar scatter and assert it
    /// matches the oracle. Returns the decoded survivors for caller-specific
    /// assertions.
    fn assert_matches_reference(schema: &SchemaDescriptor, runs: Vec<Batch>) -> Vec<Row> {
        let mem: Vec<MemBatch<'_>> = runs.iter().map(|b| b.as_mem_batch()).collect();
        let total_rows: usize = mem.iter().map(|b| b.count).sum();
        let total_blob: usize = mem.iter().map(|b| b.blob.len()).sum();

        let out = materialize(schema, total_rows, total_blob, |writer| {
            merge_batches(&mem, schema, writer);
        });
        let got = decode(schema, &out);
        assert!(!got.is_empty(), "merge produced no survivors");
        assert_eq!(
            got,
            reference_fold(schema, &runs),
            "merge + columnar scatter diverged from the argsort/fold reference"
        );
        got
    }

    const LONG_A: &[u8] = b"long_string_value_A_padpadpadpad"; // > 12 → spills to blob
    const LONG_DUP: &[u8] = b"duplicated_long_string_payload_zz"; // > 12, reused across columns

    /// The shared adversarial dataset, parameterized by a PK encoder so the
    /// same survivors drive both the stride-16 (single U128 PK) and stride-24
    /// (compound 3×U64 PK → scatter dynamic arm) schemas.
    fn build_dataset(schema: &SchemaDescriptor, pk: impl Fn(u64) -> Vec<u8>) -> Vec<Batch> {
        let a = build_run(
            schema,
            vec![
                RowSpec {
                    pk: pk(10),
                    w: 1,
                    c0: S::V(b"".to_vec()),
                    c1: S::V(LONG_A.to_vec()),
                    c2: I::V(100),
                },
                RowSpec {
                    pk: pk(20),
                    w: 2,
                    c0: S::V(b"short".to_vec()),
                    c1: S::V(LONG_DUP.to_vec()),
                    c2: I::Null,
                },
                RowSpec {
                    pk: pk(30),
                    w: 1,
                    c0: S::Null,
                    c1: S::V(b"abc".to_vec()),
                    c2: I::V(-5),
                },
                RowSpec {
                    pk: pk(40),
                    w: 1,
                    c0: S::V(b"ghost".to_vec()),
                    c1: S::V(b"ghost2".to_vec()),
                    c2: I::V(1),
                },
            ],
        );
        // Run B: pk=10 repeats Run A's (PK, payload) by *content* (a different
        // blob offset) → the merge folds them to w=4.
        let b = build_run(
            schema,
            vec![
                RowSpec {
                    pk: pk(10),
                    w: 3,
                    c0: S::V(b"".to_vec()),
                    c1: S::V(LONG_A.to_vec()),
                    c2: I::V(100),
                },
                RowSpec {
                    pk: pk(25),
                    w: 1,
                    c0: S::V(LONG_DUP.to_vec()),
                    c1: S::V(b"x".to_vec()),
                    c2: I::V(9),
                },
                RowSpec {
                    pk: pk(50),
                    w: 1,
                    c0: S::V(b"last".to_vec()),
                    c1: S::Null,
                    c2: I::Null,
                },
            ],
        );
        // Run C carries the null STRING cell; its pk=40 cancels Run A's (net
        // zero → dropped).
        let c = build_run(
            schema,
            vec![
                RowSpec {
                    pk: pk(40),
                    w: -1,
                    c0: S::V(b"ghost".to_vec()),
                    c1: S::V(b"ghost2".to_vec()),
                    c2: I::V(1),
                },
                RowSpec {
                    pk: pk(60),
                    w: 1,
                    c0: S::Null,
                    c1: S::V(b"tail".to_vec()),
                    c2: I::Null,
                },
            ],
        );
        vec![a, b, c]
    }

    fn schema_simple() -> SchemaDescriptor {
        SchemaDescriptor::new(
            &[
                SchemaColumn::new(TypeCode::U128, false),
                SchemaColumn::new(TypeCode::String, true),
                SchemaColumn::new(TypeCode::Blob, true),
                SchemaColumn::new(TypeCode::I64, true),
            ],
            &[0],
        )
    }

    fn schema_compound() -> SchemaDescriptor {
        SchemaDescriptor::new(
            &[
                SchemaColumn::new(TypeCode::U64, false),
                SchemaColumn::new(TypeCode::U64, false),
                SchemaColumn::new(TypeCode::U64, false),
                SchemaColumn::new(TypeCode::String, true),
                SchemaColumn::new(TypeCode::Blob, true),
                SchemaColumn::new(TypeCode::I64, true),
            ],
            &[0, 1, 2],
        )
    }

    #[test]
    fn single_pk_stride16_matches_reference() {
        let s = schema_simple();
        assert_eq!(s.pk_stride(), 16);
        let runs = build_dataset(&s, |v| (v as u128).to_be_bytes().to_vec());
        let rows = assert_matches_reference(&s, runs);

        // Concrete pins: pk=40 ghost dropped, pk=10 folded across runs to w=4.
        assert_eq!(rows.len(), 6, "expected 6 survivors (pk=40 cancels)");
        let pk10 = (10u128).to_be_bytes().to_vec();
        let row10 = rows.iter().find(|r| r.0 == pk10).expect("pk=10 present");
        assert_eq!(row10.1, 4, "pk=10 weight folds across runs");
        assert_eq!(
            row10.3[0],
            CellVal::Str(b"".to_vec()),
            "empty string survives as empty, not null"
        );
        assert_eq!(row10.3[1], CellVal::Str(LONG_A.to_vec()), "spilled long-string content");
        assert_eq!(row10.3[2], CellVal::Int(100));
        assert!(
            !rows.iter().any(|r| r.0 == (40u128).to_be_bytes().to_vec()),
            "pk=40 ghost must be dropped",
        );
    }

    #[test]
    fn compound_pk_stride24_dynamic_arm_matches_reference() {
        let s = schema_compound();
        assert_eq!(s.pk_stride(), 24, "stride 24 → scatter dynamic arm");
        // col0 = col1 = 0, col2 = value: ascending value == ascending PK, and
        // every PK shares the low-16 prefix (col0||col1) — also exercises the
        // merge's compare_pk_bytes tiebreak.
        let runs = build_dataset(&s, |v| {
            let mut p = Vec::with_capacity(24);
            p.extend_from_slice(&0u64.to_be_bytes());
            p.extend_from_slice(&0u64.to_be_bytes());
            p.extend_from_slice(&v.to_be_bytes());
            p
        });
        let rows = assert_matches_reference(&s, runs);
        assert_eq!(rows.len(), 6, "expected 6 survivors (pk=40 cancels)");
    }
}

// -----------------------------------------------------------------------
// OPK consolidation-output equivalence (signed / compound / wide)
//
// `consolidate_groups` sorts by an order-preserving key rather than a
// per-comparison typed column decode. These tests pin that the consolidated
// output is identical to an independent reference built directly from
// `compare_pk_bytes` + `compare_rows` — the canonical total order — for the
// signed, compound, and wide PK shapes the sort key covers.
// -----------------------------------------------------------------------

/// Independent reference: argsort by `compare_full_rows`,
/// fold consecutive equal `(PK, payload)` groups, drop net-zero ghosts.
fn consolidate_reference(b: &Batch, schema: &SchemaDescriptor) -> Vec<(Vec<u8>, i64, i64)> {
    let mb = b.as_mem_batch();
    let n = mb.count;
    let mut idx: Vec<usize> = (0..n).collect();
    idx.sort_by(|&x, &y| compare_full_rows(schema, &mb, x, &mb, y));
    let mut out = Vec::new();
    let mut i = 0;
    while i < n {
        let head = idx[i];
        let mut w = mb.get_weight(head);
        i += 1;
        while i < n && compare_full_rows(schema, &mb, head, &mb, idx[i]) == Ordering::Equal {
            w += mb.get_weight(idx[i]);
            i += 1;
        }
        if w != 0 {
            let pk = mb.get_pk_bytes(head).to_vec();
            let v = gnitz_wire::read_signed_exact(mb.get_col_ptr(head, 0, 8));
            out.push((pk, w, v));
        }
    }
    out
}

fn assert_consolidate_matches_reference(schema: &SchemaDescriptor, rows: &[(Vec<u8>, i64, i64)]) {
    let b = make_batch_bytes(schema, rows);
    let got = run_consolidate_bytes(&b, schema);
    let want = consolidate_reference(&b, schema);
    assert_eq!(got, want, "OPK consolidated output diverged from reference");
    // Independent of the reference's grouping: the OPK sort must leave the
    // output non-descending under the canonical column-aware comparator.
    for w in got.windows(2) {
        assert_ne!(
            compare_pk_bytes(&w[0].0, &w[1].0),
            Ordering::Greater,
            "OPK output not ordered by compare_pk_bytes",
        );
    }
}

/// OPK bytes for a single I64 PK column (BE with the sign bit flipped).
fn i64_pk(v: i64) -> Vec<u8> {
    opk_pk(&v.to_le_bytes(), TypeCode::I64)
}

/// Decode a single-column OPK PK back to its native I64 value.
#[test]
fn opk_single_signed_i64_negatives() {
    let s = pk_payload_schema(&[TypeCode::I64]);
    // Unsorted, spanning negatives and extremes, with a fold and a ghost.
    let rows = vec![
        (i64_pk(5), 1, 50),
        (i64_pk(-1), 1, 10),
        (i64_pk(i64::MIN), 1, 99),
        (i64_pk(-1), 2, 10), // folds with row 1 → weight 3
        (i64_pk(i64::MAX), 1, 7),
        (i64_pk(0), 1, 0),
        (i64_pk(5), -1, 50), // ghost-cancels row 0
        (i64_pk(-100), 1, 3),
    ];
    assert_consolidate_matches_reference(&s, &rows);
    // Concrete order check: negatives precede non-negatives.
    let out = run_consolidate_bytes(&make_batch_bytes(&s, &rows), &s);
    let pks: Vec<i64> = out.iter().map(|r| crate::test_support::opk_pk_i64(&r.0)).collect();
    assert_eq!(pks, vec![i64::MIN, -100, -1, 0, i64::MAX]);
}

#[test]
fn opk_compound_unsigned_first_column_dominates() {
    let s = pk_payload_schema(&[TypeCode::U32, TypeCode::U64]);
    // OPK: each unsigned column big-endian, concatenated in pk-list order.
    let mk = |a: u32, b: u64| {
        let mut v = Vec::with_capacity(12);
        v.extend_from_slice(&a.to_be_bytes());
        v.extend_from_slice(&b.to_be_bytes());
        v
    };
    // Second column large enough to invert order under a raw LE u128 compare;
    // first column must still dominate.
    let rows = vec![
        (mk(2, 1), 1, 20),
        (mk(1, u64::MAX), 1, 10),
        (mk(1, u64::MAX), 1, 10), // fold → weight 2
        (mk(1, 5), 1, 11),
    ];
    assert_consolidate_matches_reference(&s, &rows);
}

#[test]
fn opk_compound_mixed_signed_negative_second_column() {
    let s = pk_payload_schema(&[TypeCode::U64, TypeCode::I32]);
    // OPK per column: U64 big-endian, I32 big-endian with sign bit flipped.
    let mk = |a: u64, b: i32| {
        let mut v = Vec::with_capacity(12);
        v.extend_from_slice(&a.to_be_bytes());
        v.extend_from_slice(&opk_pk(&b.to_le_bytes(), TypeCode::I32));
        v
    };
    let rows = vec![
        (mk(1, 0), 1, 1),
        (mk(1, -5), 1, 2), // negative second column sorts before 0
        (mk(1, i32::MIN), 1, 3),
        (mk(0, i32::MAX), 1, 4),
        (mk(1, -5), -1, 2), // ghost
    ];
    assert_consolidate_matches_reference(&s, &rows);
}

#[test]
fn opk_wide_prefix_straddle_and_collision() {
    // WIDE_24: col straddling byte 16 is the third U64. wpk shares the
    // 16-byte OPK prefix when `lead` matches, so the encoder's BE prefix
    // must still order by the trailing column.
    let tcs = WIDE_24;
    let s = pk_payload_schema(tcs);
    let rows = vec![
        (wpk(tcs, 4, 4), 1, 30),
        (wpk(tcs, 1, 2), 1, 20),
        (wpk(tcs, 1, 0), 1, 10),
        (wpk(tcs, 1, 0), 2, 10),  // fold
        (wpk(tcs, 4, 4), -1, 30), // ghost
        (wpk(tcs, 1, 9), 1, 99),
    ];
    assert_consolidate_matches_reference(&s, &rows);
}

mod opk_consolidate_proptest {
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
            pk_payload_schema(&[TypeCode::U64, TypeCode::U64, TypeCode::U64]),
            pk_payload_schema(WIDE_80),
        ]
    }

    fn arb_rows(stride: usize) -> impl Strategy<Value = Vec<(Vec<u8>, i64, i64)>> {
        // Small weight and payload domains make folds and ghost-cancels
        // likely; random PK bytes exercise the full ordering.
        let row = (prop::collection::vec(any::<u8>(), stride), -3i64..=3i64, 0i64..4i64);
        prop::collection::vec(row, 0..40)
    }

    proptest! {
        /// New `consolidate_groups` output equals the `compare_pk_bytes`
        /// reference for every covered PK shape, over random batches.
        #[test]
        fn consolidate_matches_reference(
            (si, rows) in (0usize..schemas().len()).prop_flat_map(|si| {
                let stride = schemas()[si].pk_stride();
                (Just(si), arb_rows(stride))
            })
        ) {
            let s = schemas()[si];
            assert_consolidate_matches_reference(&s, &rows);
        }
    }
}

// ── Carried heaps: dead-byte accounting and read-back ───────────────────

/// Every row as `(pk, weight, bytes)`.
fn rows_of(b: &Batch) -> Vec<(u128, i64, Vec<u8>)> {
    (0..b.count)
        .map(|row| {
            let s = crate::test_support::read_german_string(b, 0, row);
            (b.get_pk(row), b.get_weight(row), s)
        })
        .collect()
}

/// A pair that cancels leaves both its rows' spans behind; a pair that sums
/// keeps one and leaves the other's.
#[test]
fn merged_consolidated_charges_the_rows_the_fold_drops() {
    let schema = crate::test_support::make_schema_pk_u64_payload_string();
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
    let schema = crate::test_support::make_schema_pk_u64_payload_string();
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
    let schema = crate::test_support::make_schema_pk_u64_payload_string();
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
