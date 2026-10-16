use super::*;
use crate::repr::shard_reader::MappedShard;
use crate::schema::{SchemaDescriptor, TypeCode};
use crate::test_support::{
    arb_fold_case, assert_folds, fold_batch, fold_schemas, make_batch_u128_raw, make_schema_pk_u64_payload_string,
    make_schema_u128_i64, make_string_batch, map_shard,
};

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
fn every_fold(case: &str, schema: &SchemaDescriptor, runs: &[Batch]) {
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
        .map(|(i, b)| map_shard(&dir.path().join(format!("{i}.db")), b))
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
    for (path, got) in &folds {
        assert_folds(runs, got, &format!("{case}: {path}"));
        got.debug_verify_dead_heap();
    }
}

/// Runs in (PK, payload) order, some with in-run duplicates and zero weights,
/// that every fold path must take to their Z-set sum, as `(label, runs)`.
type FoldCase<'a> = (&'a str, &'a [&'a [(u128, i64, i64)]]);

const FOLD_CASES: &[FoldCase] = &[
    (
        "a single source passes through",
        &[&[(10, 1, 100), (20, 1, 200), (30, 1, 300)]],
    ),
    (
        "three sources interleave",
        &[
            &[(10, 1, 100), (40, 1, 400)],
            &[(20, 1, 200), (50, 1, 500)],
            &[(30, 1, 300), (60, 1, 600)],
        ],
    ),
    (
        "ascending sources with an empty one among them",
        &[&[(1, 1, 10), (2, 1, 20)], &[], &[(3, 1, 30), (4, 1, 40)]],
    ),
    (
        "descending sources",
        &[&[(3, 1, 30), (4, 1, 40)], &[(1, 1, 10), (2, 1, 20)]],
    ),
    (
        "ascending sources that meet at one PK",
        &[&[(1, 1, 10), (2, 1, 20)], &[(2, -1, 20), (5, 1, 50)]],
    ),
    ("weights sum across sources", &[&[(10, 1, 100)], &[(10, 2, 100)]]),
    (
        "a ghost drops and its neighbours survive",
        &[&[(10, 1, 100), (20, 1, 200)], &[(10, -1, 100)], &[(30, 1, 300)]],
    ),
    ("no sources at all", &[]),
    ("an empty source beside a live one", &[&[], &[(10, 1, 100)]]),
    (
        "the PK's high word separates",
        &[&[(10, 1, 100)], &[((1u128 << 64) | 10, 1, 200)]],
    ),
    (
        "a zero-weight input row never reaches the writer",
        &[&[(10, 0, 100), (20, 1, 200)]],
    ),
    (
        "duplicates within one source fold",
        &[&[(10, 1, 100), (10, 1, 100), (20, 1, 200)]],
    ),
    (
        "one PK's payloads interleave across sources and fold apart",
        &[&[(5, 1, 100)], &[(5, 1, 200)], &[(5, -1, 100)]],
    ),
    (
        "one PK with distinct payloads stays distinct",
        &[&[(10, 1, 100), (10, 1, 200), (20, 1, 300)]],
    ),
];

#[test]
fn every_fold_path_reaches_each_fold_case() {
    let schema = make_schema_u128_i64();
    for &(what, inputs) in FOLD_CASES {
        let runs: Vec<Batch> = inputs.iter().map(|rows| make_batch_u128_raw(&schema, rows)).collect();
        every_fold(what, &schema, &runs);
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
        every_fold("", &s, &runs);
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
    for (pad_a, pad_b) in [(0, 0), (1000, 0), (0, 1000)] {
        let (a, b) = (
            make_string_batch(&a_rows).padded(pad_a),
            make_string_batch(&b_rows).padded(pad_b),
        );
        let out = a.merged_consolidated(&b, &schema);
        assert_folds(&[a, b], &out, &format!("padding ({pad_a}, {pad_b})"));
        out.debug_verify_dead_heap();
        assert!(
            out.blob().len() < 1000,
            "a wasteful side relocates rather than carrying its padding"
        );
    }
}

/// A merge over wide PKs folds to the Z-set its sources sum to, in (PK,
/// payload) order, wherever the keys differ: in the bytes every row shares
/// (none do, then), in the span the tournament is keyed by, and behind it.
#[test]
fn wide_pk_merges_fold_to_the_sum_of_their_sources() {
    use crate::test_support::{make_batch_opk, pk_payload_schema, Rng};
    let mut rng = Rng::new(11);
    for pk_cols in [3usize, 5] {
        let schema = pk_payload_schema(&vec![TypeCode::U64; pk_cols]);
        // The columns that vary; the others are one value in every row.
        for varying in 0..1u32 << pk_cols {
            let sources: Vec<Batch> = (0..1 + rng.gen_range(5))
                .map(|_| {
                    let rows: Vec<(Vec<u8>, i64, i64)> = (0..rng.gen_range(60))
                        .map(|_| {
                            let pk = (0..pk_cols).flat_map(|c| {
                                let v = if varying >> c & 1 == 1 { rng.gen_range(3) } else { 7 };
                                v.to_be_bytes()
                            });
                            (
                                pk.collect(),
                                [-1, 1, 2][rng.gen_range(3) as usize],
                                rng.gen_range(2) as i64,
                            )
                        })
                        .collect();
                    make_batch_opk(&schema, &rows).into_consolidated()
                })
                .collect();
            let mem: Vec<MemBatch> = sources.iter().map(|b| b.as_mem_batch()).collect();
            assert_folds(
                &sources,
                &merge_consolidated(&mem, &schema),
                &format!("{pk_cols} columns, varying {varying:b}"),
            );
        }
    }
}

/// `into_consolidated` folds a batch of fixed-width integer payloads to its
/// Z-set, whether the payload fits one 128-bit sort key or spans several — over
/// signed and unsigned columns at their extremes, and over PKs that arrive in
/// order and out of it.
#[test]
fn consolidation_folds_fixed_int_payloads_of_every_width() {
    use crate::schema::SchemaColumn;
    use crate::test_support::Rng;
    use TypeCode::{I16, I32, I64, I8, U16, U64, U8};
    let mut rng = Rng::new(13);
    for payload in [
        &[I64][..],
        &[U8, I16, I32],
        &[I64, I64],
        &[I64, I64, I8],
        &[I32, U64, I64],
        &[U16; 9],
        &[I8, U64, U64, I32],
    ] {
        let mut cols = vec![SchemaColumn::new(U64, false)];
        cols.extend(payload.iter().map(|&t| SchemaColumn::new(t, false)));
        let schema = SchemaDescriptor::new(&cols, &[0]);
        for grouped in [false, true] {
            let mut pks: Vec<u64> = (0..600).map(|_| rng.gen_range(5)).collect();
            if grouped {
                pks.sort();
            }
            let mut b = crate::repr::BatchBuilder::new(&schema);
            for pk in pks {
                b.begin_row(pk as u128, [-1, 1, 2][rng.gen_range(3) as usize]);
                // Either type's extremes and the values next to its zero.
                for (_, col) in schema.payload_columns() {
                    let top = 1u128 << (col.size() * 8 - 1);
                    b.put_int([0, 1, top - 1, top, 2 * top - 1][rng.gen_range(5) as usize]);
                }
                b.end_row();
            }
            let batch = b.finish();
            let got = batch.clone().into_consolidated();
            assert_folds(
                std::slice::from_ref(&batch),
                &got,
                &format!("{payload:?}, grouped {grouped}"),
            );
        }
    }
}
