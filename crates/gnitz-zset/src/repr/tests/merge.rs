use super::*;
use crate::repr::shard_file::ShardWriteOpts;
use crate::repr::shard_reader::MappedShard;
use crate::repr::BatchBuilder;
use crate::schema::{SchemaColumn, SchemaDescriptor, TypeCode};
use crate::test_support::{
    arb_fold_case, assert_folds, fold_batch, fold_schemas, make_batch_u128_raw, make_schema_pk_u64_payload_string,
    make_schema_u128_i64, make_schema_u64_i64, make_string_batch, map_shard, payload0_i64,
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
    let mut bb = BatchBuilder::new(&schema);
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

/// Every fold the engine runs over `runs`, each sorted by (PK, payload), checked
/// to reach their Z-set sum.
fn every_fold(schema: &SchemaDescriptor, runs: &[Batch]) -> Vec<(&'static str, Batch)> {
    let mem: Vec<MemBatch> = runs.iter().map(Batch::as_mem_batch).collect();
    let consolidated: Vec<Batch> = runs.iter().map(|b| b.clone().into_consolidated()).collect();
    let consolidated_mem: Vec<MemBatch> = consolidated.iter().map(Batch::as_mem_batch).collect();
    let pairwise = consolidated.iter().fold(Batch::empty_with_schema(schema), |acc, b| {
        acc.merged_consolidated(b, schema)
    });
    let dir = tempfile::tempdir().unwrap();
    let shards: Vec<_> = consolidated
        .iter()
        .filter(|b| b.count > 0)
        .enumerate()
        .map(|(i, b)| map_shard(&dir.path().join(format!("{i}.db")), b, ShardWriteOpts::default()))
        .collect();
    let shards: Vec<&MappedShard> = shards.iter().map(|s| &**s).collect();
    let folds = vec![
        (
            "consolidate",
            Batch::concat(schema, mem.iter().cloned()).into_consolidated(),
        ),
        ("N-way relocating", merge_relocating(&mem, schema)),
        ("N-way carrying", merge_rows(&mem, schema)),
        ("N-way sum", merge_consolidated(&consolidated_mem, schema)),
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
    fn every_fold_path_reaches_the_zset(
        (si, mut rows) in arb_fold_case(),
        runs in 1usize..6,
        ascending in proptest::prelude::any::<bool>(),
    ) {
        let s = fold_schemas()[si];
        // In PK order the runs ascend, and a PK group can straddle two of them.
        if ascending {
            rows.sort_by(|a, b| a.0.cmp(&b.0));
        }
        let runs: Vec<Batch> = rows
            .chunks(rows.len().div_ceil(runs).max(1))
            .map(|c| fold_batch(&s, c).into_consolidated())
            .collect();
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

/// Sources that ascend past one another merge to their rows in order, an empty
/// one among them or not; two that meet at one PK are folded.
#[test]
fn ascending_sources_merge_to_their_rows_in_order() {
    use crate::test_support::{make_batch, weighted_rows};
    let schema = make_schema_u64_i64();
    let (low, empty) = (make_batch(&schema, &[(1, 1, 10), (2, 1, 20)]), make_batch(&schema, &[]));
    let high = make_batch(&schema, &[(3, 1, 30), (4, 1, 40)]);
    let merge = |sources: &[&Batch]| {
        let mem: Vec<MemBatch> = sources.iter().map(|b| b.as_mem_batch()).collect();
        let out = merge_consolidated(&mem, &schema);
        assert!(out.is_consolidated());
        weighted_rows(&out)
    };
    let all = make_batch(&schema, &[(1, 1, 10), (2, 1, 20), (3, 1, 30), (4, 1, 40)]);
    assert_eq!(merge(&[&low, &empty, &high]), weighted_rows(&all));
    assert_eq!(
        merge(&[&high, &low]),
        weighted_rows(&all),
        "descending sources interleave"
    );

    let meets = make_batch(&schema, &[(2, -1, 20), (5, 1, 50)]);
    let folded = make_batch(&schema, &[(1, 1, 10), (5, 1, 50)]);
    assert_eq!(merge(&[&low, &meets]), weighted_rows(&folded));
    assert_eq!(merge(&[]), vec![]);
}
