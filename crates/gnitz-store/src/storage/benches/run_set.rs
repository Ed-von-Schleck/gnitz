use super::tests::string_run;
use super::*;

/// Fold a 200k-row dominant run with fourteen 10k-row runs of 40-byte strings;
/// only the fold is measured. `append` runs hold fresh keys only; `churn` runs
/// are half retractions of dominant rows and half fresh keys.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn run_set_fold_strings_bench() {
    use gnitz_foundation::perf::Counter;
    use std::hint::black_box;
    use std::time::Instant;
    const DOMINANT: u64 = 200_000;
    const RUNS: u64 = 14;
    const PER_RUN: u64 = 10_000;
    let schema = crate::test_support::make_schema_pk_u64_payload_string();
    let value = |pk: u64| format!("{pk:010}-{}", "s".repeat(29)).into_bytes();
    let dominant = string_run(
        &schema,
        &(0..DOMINANT).map(|i| (2 * i, 1, value(2 * i))).collect::<Vec<_>>(),
    );
    let append: Vec<Batch> = (0..RUNS)
        .map(|k| {
            let keys = k * PER_RUN..(k + 1) * PER_RUN;
            string_run(
                &schema,
                &keys.map(|j| (2 * j + 1, 1, value(2 * j + 1))).collect::<Vec<_>>(),
            )
        })
        .collect();
    let churn: Vec<Batch> = (0..RUNS)
        .map(|k| {
            let rows: Vec<_> = (k * PER_RUN / 2..(k + 1) * PER_RUN / 2)
                .flat_map(|j| [(2 * j, -1, value(2 * j)), (2 * j + 1, 1, value(2 * j + 1))])
                .collect();
            string_run(&schema, &rows)
        })
        .collect();
    let instructions = Counter::instructions().unwrap();
    for (shape, small) in [("append", &append), ("churn", &churn)] {
        for _ in 0..5 {
            let mut set = RunSet::new(usize::MAX);
            set.push(dominant.clone(), &schema);
            for run in small {
                set.push(run.clone(), &schema);
            }
            assert_eq!(set.len(), 1 + RUNS as usize, "the fold must not have run yet");
            let t = Instant::now();
            let ((), i) = instructions.measure(|| set.fold(&schema));
            let elapsed = t.elapsed();
            black_box(&set.runs);
            println!("run_set_fold_strings {shape}: {i} instructions, {elapsed:?}");
        }
    }
}
