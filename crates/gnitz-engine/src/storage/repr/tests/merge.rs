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
    let mut cache: BlobCache = BlobCache::default();

    let r1 = relocate_german_string_vec(&src_cell, &src_blob, &mut dst_blob, Some(&mut cache));
    let after_first = dst_blob.len();
    assert_eq!(after_first, 20, "first call must append payload exactly once");

    let r2 = relocate_german_string_vec(&src_cell, &src_blob, &mut dst_blob, Some(&mut cache));
    assert_eq!(dst_blob.len(), 20, "cache hit must not append a second copy");
    assert_eq!(&r1[8..16], &r2[8..16], "both calls must return the same offset");
}

use super::super::batch::{Batch, Layout};
use super::*;
use crate::schema::{type_code, SchemaColumn, SchemaDescriptor};
use crate::test_support::{make_schema_u128_i64, pk_payload_schema};

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
            SchemaColumn::new(type_code::U64, 0),  // PK
            SchemaColumn::new(type_code::I32, 0),  // payload slot 0, 4 bytes
            SchemaColumn::new(type_code::U128, 0), // payload slot 1, 16 bytes
            SchemaColumn::new(type_code::I64, 1),  // payload slot 2, 8 bytes, nullable
        ],
        &[0],
    );
    const ROWS: usize = 5;
    let mut bb = crate::storage::BatchBuilder::new(schema);
    for row in 0..ROWS {
        bb.begin_row(row as u128, 1);
        bb.put_i32(-(row as i32));
        bb.put_u128((row as u128) << 100);
        // NULL slot 2 on the odd rows, so the bitmap is not uniformly zero.
        if row % 2 == 0 {
            bb.put_i64(row as i64);
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
        &[(type_code::U64, 0, &pk_vals)],
    );
}

/// Build a large sorted `(PK | I64)` batch with a high duplicate-PK rate:
/// `dup` consecutive rows share a PK with distinct ascending payloads, which
/// puts the equal-PK payload tiebreak and the group fold on the measured path.
/// The schema's `pk_stride` selects the width.
fn bench_sorted_batch(schema: &SchemaDescriptor, n: usize, dup: usize) -> Batch {
    let mut b = Batch::with_capacity(*schema, n.max(1));
    for i in 0..n {
        b.extend_pk((i / dup) as u128);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &(i as i64).to_le_bytes());
        b.count += 1;
    }
    b
}

/// Throughput of `run_merge`'s comparator-driven N-way merge at stride 8 and
/// 16 with a high duplicate-PK rate, using a cheap emit (no `write_row`) so the
/// measured cost is the loser-tree compare loop, not row materialization. A
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
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 0),
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

fn make_schema_flush() -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 0),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    )
}

/// Build an owned `Batch` of `n` rows for `make_schema_flush`: PK from
/// `key_fn(i)`, weight +1, both I64 payload columns derived from the row
/// index (payloads are irrelevant when all PKs are distinct).
fn bench_flush_batch(schema: &SchemaDescriptor, n: usize, key_fn: impl Fn(usize) -> u64) -> Batch {
    let mut b = Batch::with_capacity(*schema, n.max(1));
    for i in 0..n {
        b.extend_pk(key_fn(i) as u128);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &(i as i64).to_le_bytes());
        b.extend_col(1, &((i as i64) * 2).to_le_bytes());
        b.count += 1;
    }
    b
}

/// One RAM-tier fold unit of work at its real skewed run shape: the
/// full `consolidate_batches` composition (`write_to_batch` arena +
/// `run_merge` + `scatter_unified_sources`) over **1 big run +
/// 4 small runs**. Existing merge benches use balanced K=4 only; this is the
/// per-delta-row price the tick-cadence amplification factor multiplies. Big
/// run = `N` even keys; each small run = `d` odd keys uniformly interleaved
/// across the big range (distinct per run via a `+2j` offset), so no
/// (PK,payload) ties and consolidation drops nothing.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn merge_batches_skewed_bench() {
    use super::super::batch::write_to_batch;
    use std::hint::black_box;
    use std::time::Instant;

    let schema = make_schema_flush();
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
    narrow(merge_to_rows_wide(batches, schema), schema.pk_stride() as usize)
}

fn run_consolidate(b: &Batch, schema: &SchemaDescriptor) -> Vec<(u128, i64, i64)> {
    narrow(run_consolidate_bytes(b, schema), schema.pk_stride() as usize)
}

/// The Z-set fold, as `(inputs, expected output)`. Weights of matching
/// (PK, payload) elements sum, net-zero elements drop, and rows sharing a PK but
/// differing in payload stay distinct and order by payload. These are properties
/// of the fold alone — `drive_merge` takes `same_pk`/`eq_payload` as closures, so
/// they are independent of PK width, which the width sweep below covers.
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

/// `sort_and_consolidate` reaches the same fold from one unsorted batch: it
/// sorts by (PK, payload) first, so every [`FOLD_CASES`] expectation holds over
/// the concatenated, shuffled inputs.
#[test]
fn sort_and_consolidate_folds_the_zset() {
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
fn opk_pk(le: &[u8], tc: u8) -> Vec<u8> {
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
    let cases: Vec<(&[u8], Vec<Vec<u128>>)> = vec![
        (
            &[type_code::U8],
            vec![vec![0], vec![1], vec![127], vec![128], vec![255]],
        ),
        // The sign flip is the only reason negatives sort first.
        (
            &[type_code::I8],
            vec![vec![s8(-128)], vec![s8(-1)], vec![0], vec![1], vec![s8(127)]],
        ),
        (
            &[type_code::U16],
            vec![vec![0], vec![1], vec![256], vec![32768], vec![65535]],
        ),
        (
            &[type_code::U32],
            vec![vec![0], vec![1], vec![1 << 16], vec![1 << 31], vec![u32::MAX as u128]],
        ),
        (
            &[type_code::I64],
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
            &[type_code::U32, type_code::U32],
            vec![vec![1, 9], vec![1, 10], vec![2, 0]],
        ),
        // Stride 11 — a width that is neither a power of two nor a register size.
        (
            &[type_code::U64, type_code::U16, type_code::U8],
            vec![vec![1, 2, 2], vec![1, 2, 3], vec![1, 3, 0], vec![2, 0, 0]],
        ),
        // Stride 16: col0 must dominate. A regression to a plain numeric compare
        // over the packed u128 would order col1-major and disagree here.
        (
            &[type_code::U64, type_code::U64],
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
        let stride = schema.pk_stride() as usize;
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
        assert_eq!(ranks(&c), want, "stride {stride}: sort_and_consolidate");

        let f = run_fold_wide(&make_batch_bytes(&schema, &sorted), &schema);
        assert_eq!(ranks(&f), want, "stride {stride}: fold_sorted");

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
const WIDE_24: &[u8] = &[type_code::U64, type_code::U64, type_code::U64];
const WIDE_64: &[u8] = &[type_code::U128, type_code::U128, type_code::U128, type_code::U128];
const WIDE_80: &[u8] = &[
    type_code::U128,
    type_code::U128,
    type_code::U128,
    type_code::U128,
    type_code::U128,
];

/// OPK PK bytes: col0 = `lead`, last col = `tail`, all middle columns 0.
/// Each unsigned column is stored big-endian (its OPK form), concatenated
/// in pk-list order. The packed low-16 prefix is fully determined by the
/// leading columns (col0, plus zeros), so two PKs sharing `lead` but
/// differing in `tail` collide in the prefix yet are distinct under
/// `compare_pk_bytes` (which orders col0 → … → last). Holds for every
/// stride here.
fn wpk(tcs: &[u8], lead: u128, tail: u128) -> Vec<u8> {
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
        let cw = gnitz_wire::wire_stride(tc);
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
    let stride = schema.pk_stride() as usize;
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
        count = writer.row_count();
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

/// One-shot flush merge into a writer whose arena is already sized to the
/// Σ-input upper bound. Production runs the two kernels itself so it can size
/// the arena to the survivor count instead (`run_set::consolidate_batches`), so
/// this form exists for the tests and microbench that pre-size their writer.
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
    writer_run(schema, total, |w| {
        sort_and_consolidate(&mb, schema, w);
    })
}

fn run_fold_wide(b: &Batch, schema: &SchemaDescriptor) -> Vec<(Vec<u8>, i64, i64)> {
    let mb = b.as_mem_batch();
    let total = mb.count;
    writer_run(schema, total, |w| {
        fold_sorted(&mb, schema, w);
    })
}

#[test]
fn wide_nway_merge_ordering_low16_collision() {
    for (tcs, want_stride) in [(WIDE_24, 24usize), (WIDE_64, 64), (WIDE_80, 80)] {
        let s = pk_payload_schema(tcs);
        assert_eq!(s.pk_stride() as usize, want_stride);
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
fn wide_fold_sorted_prefix_collision_and_identical_payload() {
    let tcs = WIDE_24;
    let s = pk_payload_schema(tcs);
    // Pre-sorted: (1,1,0) and (1,1,1) collide in low 16 AND carry an
    // identical payload (val=0); they must remain two distinct rows.
    // (1,1,2) is a third distinct PK.
    let p0 = wpk(tcs, 1, 0);
    let p1 = wpk(tcs, 1, 1);
    let p2 = wpk(tcs, 1, 2);
    let b = make_batch_bytes(&s, &[(p0.clone(), 1, 0), (p1.clone(), 1, 0), (p2.clone(), 1, 5)]);
    let out = run_fold_wide(&b, &s);
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
    let out2 = run_fold_wide(&b2, &s);
    assert_eq!(out2, vec![(q, 2, 7)]);
}

// -----------------------------------------------------------------------
// Columnar materialization differential
//
// The flush merge materializes survivors column-at-a-time through
// `scatter_unified_sources`, where `write_row` goes
// row-at-a-time. These tests pin the two materializations value-identical —
// same decoded PK / payload / weight / null bit / row count — over an
// adversarial schema (two German-string columns + a nullable int) with
// long / inline / empty / duplicate strings, a null STRING cell, and
// garbage-under-null cells in a `consolidated`-flagged run. The garbage
// case is load-bearing: `write_row` zero-fills a null cell while the columnar
// copy takes the source bytes verbatim, so the two diverge in raw bytes but
// must agree on the decoded value because the null bit governs. Assertions
// are on decoded content, never raw blob/struct bytes (which legitimately
// differ row-major vs column-major once two STRING columns spill).
// -----------------------------------------------------------------------
mod columnar_materialize_differential {
    use super::*;

    /// A payload cell for a German-string column.
    enum S {
        /// Non-null value with this content.
        V(Vec<u8>),
        /// Null cell, zeroed struct (the normal in-tree shape).
        Null,
        /// Null cell whose 16-byte struct carries these non-zero bytes — the
        /// invariant-violating shape the columnar copy must still decode to
        /// NULL via the null bit (`write_row` zero-fills it instead).
        NullGarbage([u8; 16]),
    }

    /// A payload cell for the nullable fixed-width int column.
    enum I {
        V(i64),
        Null,
        NullGarbage(i64),
    }

    struct RowSpec {
        pk: Vec<u8>,
        w: i64,
        c0: S,
        c1: S,
        c2: I,
    }

    fn enc_str(b: &mut Batch, pi: usize, nw: &mut u64, c: &S) -> [u8; 16] {
        match c {
            S::V(bytes) => gnitz_wire::encode_german_string(bytes, &mut b.blob),
            S::Null => {
                gnitz_wire::null_word_set(nw, pi, true);
                [0u8; 16]
            }
            S::NullGarbage(st) => {
                gnitz_wire::null_word_set(nw, pi, true);
                *st
            }
        }
    }

    fn enc_int(pi: usize, nw: &mut u64, c: &I) -> [u8; 8] {
        match c {
            I::V(v) => v.to_le_bytes(),
            I::Null => {
                gnitz_wire::null_word_set(nw, pi, true);
                [0u8; 8]
            }
            I::NullGarbage(v) => {
                gnitz_wire::null_word_set(nw, pi, true);
                v.to_le_bytes()
            }
        }
    }

    /// Build a (PK, payload)-sorted run. Rows must be in ascending PK order
    /// (PKs are distinct here, so PK order == (PK, payload) order).
    /// `layout` must be `Sorted` or `Consolidated`; it is certified (verified in
    /// debug) so the data must actually match the claim. Pass `Consolidated` to
    /// model a consolidated-run whose null cells the columnar copy trusts rather
    /// than re-zeroing.
    fn build_run(schema: &SchemaDescriptor, layout: Layout, rows: Vec<RowSpec>) -> Batch {
        let mut b = Batch::empty_with_schema(schema);
        b.reserve_rows(rows.len().max(1));
        for row in &rows {
            let mut nw = 0u64;
            let st0 = enc_str(&mut b, 0, &mut nw, &row.c0);
            let st1 = enc_str(&mut b, 1, &mut nw, &row.c1);
            let ic = enc_int(2, &mut nw, &row.c2);
            b.extend_pk_bytes(&row.pk);
            b.extend_weight(&row.w.to_le_bytes());
            b.extend_null_bmp(&nw.to_le_bytes());
            b.extend_col(0, &st0);
            b.extend_col(1, &st1);
            b.extend_col(2, &ic);
            b.count += 1;
        }
        b.certify_layout(layout, schema);
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
        let pk_stride = schema.pk_stride() as usize;
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
            count = writer.row_count();
        }
        OutBufs {
            count,
            pk_stride,
            pk,
            nb,
            wt,
            cols,
            blob,
        }
    }

    #[derive(Debug, PartialEq)]
    enum CellVal {
        Null,
        Str(Vec<u8>),
        Int(i64),
    }

    /// Decode an output into `(pk_bytes, weight, null_word, cells)`. Null cells
    /// decode to `Null` from the null bit alone (cell bytes ignored); strings
    /// decode to content against the output's own blob (never the raw
    /// struct/offset bytes, which differ row- vs column-major).
    fn decode(schema: &SchemaDescriptor, out: &OutBufs) -> Vec<(Vec<u8>, i64, u64, Vec<CellVal>)> {
        (0..out.count)
            .map(|i| {
                let pk = out.pk[i * out.pk_stride..(i + 1) * out.pk_stride].to_vec();
                let w = gnitz_wire::read_i64_le(&out.wt, i * 8);
                let nw = gnitz_wire::read_u64_le(&out.nb, i * 8);
                let cells = schema
                    .payload_columns()
                    .map(|(pi, col)| {
                        let cs = col.size() as usize;
                        if gnitz_wire::null_word_get(nw, pi) {
                            CellVal::Null
                        } else if gnitz_wire::is_german_string(col.type_code) {
                            let st: [u8; 16] = out.cols[pi][i * 16..i * 16 + 16].try_into().unwrap();
                            CellVal::Str(gnitz_wire::try_decode_german_string(&st, &out.blob).expect("valid string"))
                        } else {
                            CellVal::Int(i64::from_le_bytes(
                                out.cols[pi][i * cs..i * cs + cs].try_into().unwrap(),
                            ))
                        }
                    })
                    .collect();
                (pk, w, nw, cells)
            })
            .collect()
    }

    /// Materialize `runs` both ways and assert value-identical output. Returns
    /// the decoded survivors for caller-specific assertions.
    fn assert_paths_agree(schema: &SchemaDescriptor, runs: Vec<Batch>) -> Vec<(Vec<u8>, i64, u64, Vec<CellVal>)> {
        let mem: Vec<MemBatch<'_>> = runs.iter().map(|b| b.as_mem_batch()).collect();
        let sorted: Vec<MemBatch> = mem.to_vec();
        let total_rows: usize = sorted.iter().map(|b| b.count).sum();
        let total_blob: usize = sorted.iter().map(|b| b.blob.len()).sum();

        // Capture the merge's (src, row, net_weight) emission stream.
        let mut stream: Vec<(usize, usize, i64)> = Vec::new();
        run_merge(&sorted, schema, |s, r, w| stream.push((s, r, w)));
        assert!(!stream.is_empty(), "merge produced no survivors");

        // Row-major reference: replay write_row over the stream.
        let ref_out = materialize(schema, total_rows, total_blob, |writer| {
            for &(s, r, w) in &stream {
                writer.write_row(&sorted[s], r, w);
            }
        });
        // Column-major: the merge_batches path.
        let col_out = materialize(schema, total_rows, total_blob, |writer| {
            merge_batches(&sorted, schema, writer);
        });

        let ref_rows = decode(schema, &ref_out);
        let col_rows = decode(schema, &col_out);
        assert_eq!(ref_rows.len(), col_rows.len(), "survivor row count diverged");
        assert_eq!(ref_rows, col_rows, "row-major vs column-major materialization diverged");
        ref_rows
    }

    const LONG_A: &[u8] = b"long_string_value_A_padpadpadpad"; // > 12 → spills to blob
    const LONG_DUP: &[u8] = b"duplicated_long_string_payload_zz"; // > 12, reused across columns

    /// The shared adversarial dataset, parameterized by a PK encoder so the
    /// same survivors drive both the stride-16 (single U128 PK) and stride-24
    /// (compound 3×U64 PK → scatter dynamic arm) schemas.
    fn build_dataset(schema: &SchemaDescriptor, pk: impl Fn(u64) -> Vec<u8>) -> Vec<Batch> {
        // Inline-garbage struct under a null STRING cell: length 5, non-zero
        // prefix — relocates inline (no blob touch), so write_row's zero-fill
        // and the columnar verbatim copy differ in bytes but both decode NULL.
        let mut sgarbage = [0u8; 16];
        sgarbage[0..4].copy_from_slice(&5u32.to_le_bytes());
        sgarbage[4..9].copy_from_slice(b"GARBG");

        // Run A (sorted, not consolidated).
        let a = build_run(
            schema,
            Layout::Sorted,
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
        // Run B (sorted, not consolidated). pk=10 repeats Run A's (PK, payload)
        // by *content* (a different blob offset) → the merge folds them to w=4.
        let b = build_run(
            schema,
            Layout::Sorted,
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
                    c2: I::NullGarbage(0x7777_7777_7777_7777),
                },
            ],
        );
        // Run C (consolidated; carries garbage-under-null cells).
        // pk=40 cancels Run A's pk=40 (net zero → dropped).
        let c = build_run(
            schema,
            Layout::Consolidated,
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
                    c0: S::NullGarbage(sgarbage),
                    c1: S::V(b"tail".to_vec()),
                    c2: I::NullGarbage(0x1234_5678_9abc_def0),
                },
            ],
        );
        vec![a, b, c]
    }

    fn schema_simple() -> SchemaDescriptor {
        SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U128, 0),
                SchemaColumn::new(type_code::STRING, 1),
                SchemaColumn::new(type_code::BLOB, 1),
                SchemaColumn::new(type_code::I64, 1),
            ],
            &[0],
        )
    }

    fn schema_compound() -> SchemaDescriptor {
        SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::STRING, 1),
                SchemaColumn::new(type_code::BLOB, 1),
                SchemaColumn::new(type_code::I64, 1),
            ],
            &[0, 1, 2],
        )
    }

    #[test]
    fn single_pk_stride16_row_vs_column_major_identical() {
        let s = schema_simple();
        assert_eq!(s.pk_stride(), 16);
        let runs = build_dataset(&s, |v| (v as u128).to_be_bytes().to_vec());
        let rows = assert_paths_agree(&s, runs);

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
    fn compound_pk_stride24_dynamic_arm_row_vs_column_major_identical() {
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
        let rows = assert_paths_agree(&s, runs);
        assert_eq!(rows.len(), 6, "expected 6 survivors (pk=40 cancels)");
    }
}

// -----------------------------------------------------------------------
// OPK consolidation-output equivalence (signed / compound / wide)
//
// `sort_and_consolidate` sorts by an order-preserving key rather than a
// per-comparison typed column decode. These tests pin that the consolidated
// output is identical to an independent reference built directly from
// `compare_pk_bytes` + `compare_rows` — the canonical total order — for the
// signed, compound, and wide PK shapes the sort key covers.
// -----------------------------------------------------------------------

/// Independent reference: argsort by `compare_pk_bytes` then `compare_rows`,
/// fold consecutive equal `(PK, payload)` groups, drop net-zero ghosts.
fn consolidate_reference(b: &Batch, schema: &SchemaDescriptor) -> Vec<(Vec<u8>, i64, i64)> {
    let mb = b.as_mem_batch();
    let n = mb.count;
    let mut idx: Vec<usize> = (0..n).collect();
    idx.sort_by(
        |&x, &y| match compare_pk_bytes(mb.get_pk_bytes(x), mb.get_pk_bytes(y)) {
            Ordering::Equal => super::super::columnar::compare_rows(schema, &mb, x, &mb, y),
            ord => ord,
        },
    );
    let mut out = Vec::new();
    let mut i = 0;
    while i < n {
        let head = idx[i];
        let mut w = mb.get_weight(head);
        i += 1;
        while i < n
            && compare_pk_bytes(mb.get_pk_bytes(head), mb.get_pk_bytes(idx[i])) == Ordering::Equal
            && super::super::columnar::compare_rows(schema, &mb, head, &mb, idx[i]) == Ordering::Equal
        {
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
    opk_pk(&v.to_le_bytes(), type_code::I64)
}

/// Decode a single-column OPK PK back to its native I64 value.
#[test]
fn opk_single_signed_i64_negatives() {
    let s = pk_payload_schema(&[type_code::I64]);
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
    let s = pk_payload_schema(&[type_code::U32, type_code::U64]);
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
    let s = pk_payload_schema(&[type_code::U64, type_code::I32]);
    // OPK per column: U64 big-endian, I32 big-endian with sign bit flipped.
    let mk = |a: u64, b: i32| {
        let mut v = Vec::with_capacity(12);
        v.extend_from_slice(&a.to_be_bytes());
        v.extend_from_slice(&opk_pk(&b.to_le_bytes(), type_code::I32));
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
            pk_payload_schema(&[type_code::U8]),
            pk_payload_schema(&[type_code::I64]),
            pk_payload_schema(&[type_code::I32]),
            pk_payload_schema(&[type_code::U32, type_code::U64]),
            pk_payload_schema(&[type_code::U64, type_code::I32]),
            pk_payload_schema(&[type_code::U64, type_code::U64, type_code::U64]),
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
        /// New `sort_and_consolidate` output equals the `compare_pk_bytes`
        /// reference for every covered PK shape, over random batches.
        #[test]
        fn consolidate_matches_reference(
            (si, rows) in (0usize..schemas().len()).prop_flat_map(|si| {
                let stride = schemas()[si].pk_stride() as usize;
                (Just(si), arb_rows(stride))
            })
        ) {
            let s = schemas()[si];
            assert_consolidate_matches_reference(&s, &rows);
        }
    }
}
