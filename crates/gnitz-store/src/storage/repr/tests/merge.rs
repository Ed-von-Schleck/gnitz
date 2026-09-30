use super::*;
use crate::schema::{SchemaColumn, SchemaDescriptor, TypeCode};
use crate::storage::repr::shard_file::ShardWriteOpts;
use crate::storage::repr::shard_reader::MappedShard;
use crate::storage::BatchBuilder;
use crate::test_support::{
    arb_fold_case, assert_folds, bench_time, fold_batch, fold_schemas, make_batch_u128_raw,
    make_schema_pk_u64_payload_string, make_schema_u128_i64, make_schema_u64_i64, make_string_batch, map_shard,
    payload0_i64, pk_u64_two_i64_schema,
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
    let mut bb = BatchBuilder::new(schema);
    for row in 0..ROWS {
        bb.begin_row(row as u128, 1);
        bb.put_int(-(row as i32) as u128);
        bb.put_int((row as u128) << 100);
        // NULL slot 2 on the odd rows, so the bitmap is not uniformly zero.
        bb.put_opt_int((row % 2 == 0).then_some(row as u128));
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

/// `n` rows over [`pk_u64_two_i64_schema`], PK `key_fn(i)`, weight 1.
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

/// The flush merge of one big run of even keys with four small runs of distinct
/// odd keys spread across it, priced per small-run row.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn merge_consolidated_skewed_bench() {
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
            black_box(merge_consolidated(&mem, &schema));
        })
        .as_secs_f64();

        let rps = (iters as f64 * total_rows as f64) / secs;
        let ns_per_delta = secs * 1e9 / (iters as f64 * 4.0 * d as f64);
        println!("merge_skew/N{n}_d{d}: {total_rows} rows  {rps:.0} merge-rows/s  {ns_per_delta:.0} ns/delta-row");
    }
}

// ── The Z-set fold, over every path that runs it ────────────────────────

/// The N-way merge's survivors, materialized with every string relocated.
fn merge_relocating<S: ColumnarSource>(batches: &[S], schema: &SchemaDescriptor) -> Batch {
    let total = batches.iter().map(|b| b.row_count()).sum();
    let mut survivors: Vec<(u32, u32, i64)> = Vec::with_capacity(total);
    run_merge(batches, schema, |src, row, w| {
        survivors.push((src as u32, row as u32, w))
    });
    super::super::scatter::UnifiedSet::whole(batches, schema).materialize(&survivors, total)
}

/// Every fold the engine runs over `runs`, each sorted by (PK, payload): in-batch
/// consolidation of their concatenation, both N-way scatters over batches, the
/// relocating one over each run folded into a shard (every other one FoR-packed),
/// and the pairwise merge. Each is checked to reach their Z-set sum.
fn every_fold(schema: &SchemaDescriptor, runs: &[Batch]) -> Vec<(&'static str, Batch)> {
    let mem: Vec<MemBatch> = runs.iter().map(Batch::as_mem_batch).collect();
    let pairwise = runs.iter().fold(Batch::empty_with_schema(schema), |acc, b| {
        acc.merged_consolidated(&b.clone().into_consolidated(), schema)
    });
    let dir = tempfile::tempdir().unwrap();
    let shards: Vec<_> = runs
        .iter()
        .map(|b| b.clone().into_consolidated())
        .filter(|b| b.count > 0)
        .enumerate()
        .map(|(i, b)| {
            let opts = ShardWriteOpts {
                pack_ints: i % 2 == 1,
                ..ShardWriteOpts::default()
            };
            map_shard(&dir.path().join(format!("{i}.db")), &b, opts)
        })
        .collect();
    let shards: Vec<&MappedShard> = shards.iter().map(|s| &**s).collect();
    let folds = vec![
        (
            "consolidate",
            Batch::concat(schema, mem.iter().cloned()).into_consolidated(),
        ),
        ("N-way relocating", merge_relocating(&mem, schema)),
        ("N-way carrying", merge_consolidated(&mem, schema)),
        ("N-way over shards", merge_relocating(&shards, schema)),
        ("pairwise", pairwise),
    ];
    for (what, got) in &folds {
        assert_folds(runs, got, what);
        got.debug_verify_dead_heap();
    }
    folds
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
fn every_fold_path_reaches_each_fold_case() {
    let schema = make_schema_u128_i64();
    for &(what, inputs, want) in FOLD_CASES {
        let runs: Vec<Batch> = inputs.iter().map(|rows| make_batch_u128_raw(&schema, rows)).collect();
        for (path, got) in every_fold(&schema, &runs) {
            let rows: Vec<(u128, i64, i64)> = (0..got.count)
                .map(|i| (got.get_pk(i), got.get_weight(i), payload0_i64(&got, i)))
                .collect();
            assert_eq!(rows, want, "{what}: {path}");
        }
    }
}

proptest::proptest! {
    #[test]
    fn every_fold_path_reaches_the_zset((si, rows) in arb_fold_case()) {
        let s = fold_schemas()[si];
        let runs: Vec<Batch> = rows.chunks(13).map(|c| fold_batch(&s, c).into_consolidated()).collect();
        every_fold(&s, &runs);
    }
}

// ── Carried heaps: dead-byte accounting and read-back ───────────────────

/// Both heaps are carried whole, and the dead bound is exactly the long bytes
/// of the rows the fold drops: both rows of a cancelled pair, one side's copy
/// of a summed row, nothing for a disjoint merge.
#[test]
fn merged_consolidated_charges_exactly_the_dropped_rows() {
    let schema = make_schema_pk_u64_payload_string();
    let (x, y) = ([b'x'; 20], [b'y'; 30]);
    let a = make_string_batch(&[(1, 1, &x), (2, 1, &y)]);
    for (b_rows, dead) in [
        (&[(1, -1, &x[..])][..], 2 * x.len()),
        (&[(1, 2, &x[..])], x.len()),
        (&[(3, 1, &x[..])], 0),
    ] {
        let b = make_string_batch(b_rows);
        let out = a.merged_consolidated(&b, &schema);
        assert_folds(&[a.clone(), b.clone()], &out, "merge");
        out.debug_verify_dead_heap();
        assert_eq!(
            (out.dead_heap, out.blob().len()),
            (dead, a.blob().len() + b.blob().len()),
            "{b_rows:?}"
        );
    }
}

/// The carried arm reads back the Z-set sum across galloped runs of either side
/// and a shared-PK group that interleaves both sides and folds an equal element,
/// with each side in turn wasteful enough to relocate.
#[test]
fn a_carried_merge_reads_back_every_string() {
    let schema = make_schema_pk_u64_payload_string();
    let v = |c: u8, n: usize| vec![c; n];
    let (p, q, r, s, t) = (v(b'p', 14), v(b'q', 40), v(b'r', 25), v(b's', 33), v(b't', 17));
    let a_rows: Vec<(u64, i64, &[u8])> = vec![(1, 1, &p), (2, 1, b"short"), (5, 1, &q), (5, 2, &s), (9, 1, &t)];
    let b_rows: Vec<(u64, i64, &[u8])> = vec![(3, 1, &r), (4, 1, &p), (5, 1, &r), (5, -2, &s), (8, 1, &q)];
    let padded = |rows: &[(u64, i64, &[u8])], pad: usize| {
        let mut b = make_string_batch(rows);
        b.blob.extend(std::iter::repeat_n(0u8, pad));
        b.dead_heap += pad;
        b
    };
    for (pad_a, pad_b) in [(0, 0), (1000, 0), (0, 1000)] {
        let (a, b) = (padded(&a_rows, pad_a), padded(&b_rows, pad_b));
        let out = a.merged_consolidated(&b, &schema);
        assert_folds(&[a, b], &out, &format!("padding ({pad_a}, {pad_b})"));
        out.debug_verify_dead_heap();
        assert!(
            out.blob().len() < 1000,
            "a wasteful side relocates rather than carrying its padding"
        );
    }
}
