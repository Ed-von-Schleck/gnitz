use super::*;
use crate::test_support::{
    arb_fold_case, assert_folds, bench_time, fold_batch, fold_schemas, make_batch, make_batch_raw, make_schema_u64_i64,
    weighted_rows,
};
use proptest::prelude::*;
use std::hint::black_box;

proptest! {
    /// Z-set `+` over the slices at every PK width, with NULLs and heap strings:
    /// consolidated slices fold into one consolidated batch — a retraction
    /// cancels its insert, a repeated (PK, payload) sums, two payloads at one key
    /// stay two elements — and raw ones are concatenated in order.
    #[test]
    fn the_gather_is_the_zset_sum_of_its_slices(
        (si, rows) in arb_fold_case(),
        senders in 1usize..6,
        consolidated in any::<bool>(),
    ) {
        let schema = fold_schemas()[si];
        let slices: Vec<Batch> = rows
            .chunks(rows.len().div_ceil(senders).max(1))
            .map(|rows| fold_batch(&schema, rows))
            .map(|b| if consolidated { b.into_consolidated() } else { b })
            .collect();
        let mem: Vec<MemBatch> = slices.iter().map(Batch::as_mem_batch).collect();
        let got = op_exchange_gather(&mem, &schema, consolidated);
        if consolidated {
            assert_folds(&slices, &got, "consolidated slices");
            prop_assert!(got.is_consolidated());
        } else {
            let concatenated: Vec<_> = slices.iter().flat_map(weighted_rows).collect();
            prop_assert_eq!(weighted_rows(&got), concatenated);
        }
    }
}

/// Release-only microbench of [`op_exchange_gather`] over K senders' slices, over
/// both arms: `raw` is the production default, a base-table delta reaching the
/// round unconsolidated and an output round ending at a reindex `Map` that clears the claim. K=1
/// (one sender with rows for this receiver) and K=4 are the reachable range, K=16
/// the headroom point; each sender holds a disjoint stripe of one ascending key
/// space, so the merge genuinely interleaves them.
///
/// `cd crates && cargo test -p gnitz-zset --release exchange_gather_bench -- --ignored --nocapture --test-threads=1`
#[test]
#[ignore]
fn exchange_gather_bench() {
    let schema = make_schema_u64_i64();
    const N: usize = 1_000_000;
    const ITERS: usize = 20;
    type Build = fn(&SchemaDescriptor, &[(u64, i64, i64)]) -> Batch;

    for (consolidated, build) in [(true, make_batch as Build), (false, make_batch_raw as Build)] {
        for k in [1usize, 4, 16] {
            let slices: Vec<Batch> = (0..k)
                .map(|j| {
                    let rows: Vec<(u64, i64, i64)> = (0..N / k).map(|i| ((i * k + j) as u64, 1, i as i64)).collect();
                    build(&schema, &rows)
                })
                .collect();
            let mem: Vec<MemBatch> = slices.iter().map(Batch::as_mem_batch).collect();
            let rows = N / k * k;
            assert_eq!(
                op_exchange_gather(&mem, &schema, consolidated).count,
                rows,
                "gather dropped rows"
            );
            let secs = bench_time(ITERS, || {
                black_box(op_exchange_gather(&mem, &schema, consolidated));
            })
            .as_secs_f64();
            println!(
                "exchange_gather/consolidated={consolidated}/K{k}: {:.1} Mrows/s ({rows} rows × {ITERS} iters in {secs:.3}s)",
                (rows * ITERS) as f64 / secs / 1e6,
            );
        }
    }
}
