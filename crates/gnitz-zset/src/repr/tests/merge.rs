use super::*;
use crate::schema::payload_order::compare_full_rows;
use crate::schema::SchemaDescriptor;
use crate::test_support::{
    arb_fold_case, assert_folds, fold_batch, make_schema_pk_u64_payload_string, make_string_batch, FoldRow,
};

// ── The Z-set fold, over every path that runs it ────────────────────────

/// `rows` as a run sorted by (PK, payload), its duplicates and zero weights
/// left unfolded.
fn sorted_run(schema: &SchemaDescriptor, rows: &[FoldRow]) -> Batch {
    let b = fold_batch(schema, rows);
    let mb = b.as_mem_batch();
    let mut order: Vec<usize> = (0..rows.len()).collect();
    order.sort_by(|&x, &y| compare_full_rows(schema, &mb, x, &mb, y));
    fold_batch(schema, &order.iter().map(|&i| rows[i].clone()).collect::<Vec<_>>())
}

proptest::proptest! {
    /// Every fold path takes runs — each sorted by (PK, payload), holding in-run
    /// duplicates and zero weights, or nothing — to their Z-set sum. In PK order
    /// the runs ascend, and a PK group can straddle two of them.
    #[test]
    fn every_fold_path_reaches_the_zset(
        (s, mut rows) in arb_fold_case(),
        mut cuts in proptest::collection::vec(0usize..40, 0..5),
        ascending: bool,
    ) {
        if ascending {
            rows.sort_by(|a, b| a.0.cmp(&b.0));
        }
        cuts.push(rows.len());
        cuts.iter_mut().for_each(|c| *c = (*c).min(rows.len()));
        cuts.sort_unstable();
        let runs: Vec<Batch> = std::iter::once(0)
            .chain(cuts.iter().copied())
            .zip(cuts.iter().copied())
            .map(|(from, to)| sorted_run(&s, &rows[from..to]))
            .collect();
        let mem: Vec<MemBatch> = runs.iter().map(Batch::as_mem_batch).collect();
        let consolidated: Vec<Batch> = runs.iter().map(|b| b.clone().into_consolidated()).collect();
        let consolidated_mem: Vec<MemBatch> = consolidated.iter().map(Batch::as_mem_batch).collect();
        let pairwise = consolidated
            .iter()
            .fold(Batch::empty_with_schema(&s), |acc, b| {
                merge_consolidated(&[acc.as_mem_batch(), b.as_mem_batch()], &s)
            });
        for (path, got) in [
            ("consolidate", Batch::concat(&s, mem.iter().cloned()).into_consolidated()),
            ("N-way", merge_rows(&mem, &s)),
            ("N-way sum", merge_consolidated(&consolidated_mem, &s)),
            ("pairwise", pairwise),
        ] {
            assert_folds(&runs, &got, path);
            got.debug_verify_dead_heap();
        }
    }
}

// ── Carried heaps: dead-byte accounting and read-back ───────────────────

/// Both heaps are carried whole, and the dead bound is exactly the long bytes
/// of the rows the fold drops: both rows of a cancelled pair, one side's copy
/// of a summed row, nothing for a disjoint merge.
#[test]
fn a_two_way_sum_charges_exactly_the_dropped_rows() {
    let schema = make_schema_pk_u64_payload_string();
    let (x, y) = ([b'x'; 20], [b'y'; 30]);
    let a = make_string_batch(&[(1, 1, &x), (2, 1, &y)]);
    for (b_rows, dead) in [
        (&[(1, -1, &x[..])][..], 2 * x.len()),
        (&[(1, 2, &x[..])], x.len()),
        (&[(3, 1, &x[..])], 0),
    ] {
        let b = make_string_batch(b_rows);
        let out = merge_consolidated(&[a.as_mem_batch(), b.as_mem_batch()], &schema);
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
        let out = merge_consolidated(&[a.as_mem_batch(), b.as_mem_batch()], &schema);
        assert_folds(&[a, b], &out, &format!("padding ({pad_a}, {pad_b})"));
        out.debug_verify_dead_heap();
        assert!(
            out.blob().len() < 1000,
            "a wasteful side relocates rather than carrying its padding"
        );
    }
}
