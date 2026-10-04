use super::*;
use gnitz_zset::repr::BatchBuilder;

/// `RunSet::fold` over string payloads: instructions per input row.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn run_set_fold_bench() {
    use gnitz_foundation::perf::Counter;
    const DOMINANT: u64 = 200_000;
    const PER_RUN: u64 = 4_000;
    const REST: u64 = FOLD_THRESHOLD as u64 - 2;
    const SPREAD: u64 = DOMINANT / PER_RUN;
    type Rows = fn(run: u64, row: u64) -> (u64, i64);
    let ascending: Rows = |run, row| (2 * DOMINANT + run * PER_RUN + row, 1);
    let scattered: Rows = |run, row| (2 * (row * SPREAD + run) + 1, 1);
    let updates: Rows = |run, row| {
        let retracted = 2 * (row / 2 * 2 * SPREAD + run);
        [(retracted, -1), (retracted + 1, 1)][(row % 2) as usize]
    };
    let arms: [(&str, bool, u64, Rows); 5] = [
        ("dominant run and one scattered run", true, 1, scattered),
        ("dominant run and ascending runs", true, REST, ascending),
        ("dominant run and scattered runs", true, REST, scattered),
        ("dominant run and scattered updates", true, REST, updates),
        ("scattered runs", false, REST + 1, scattered),
    ];
    let schema = crate::test_support::make_schema_pk_u64_payload_string();
    let run = |n: u64, row: &dyn Fn(u64) -> (u64, i64)| {
        let mut b = BatchBuilder::new(&schema);
        for (pk, weight) in (0..n).map(row) {
            b.begin_row(pk as u128, weight);
            b.put_string(&format!("{pk:040}"));
            b.end_row();
        }
        let mut run = b.finish();
        run.certify_consolidated();
        run
    };
    let counter = Counter::instructions();
    for (label, has_dominant, small_runs, rows) in arms {
        let mut set = RunSet::new(usize::MAX);
        if has_dominant {
            set.push(run(DOMINANT, &|i| (2 * i, 1)), &schema);
        }
        for k in 0..small_runs {
            set.push(run(PER_RUN, &|i| rows(k, i)), &schema);
        }
        assert_eq!(
            set.len(),
            has_dominant as usize + small_runs as usize,
            "{label}: a push folded the set"
        );
        let rows_in = set.row_count();
        let retracted = set
            .runs
            .iter()
            .map(|r| (0..r.len()).filter(|&i| r.get_weight(i) < 0).count());
        let rows_out = rows_in - 2 * retracted.sum::<usize>();
        let ((), instructions) = counter.measure(|| set.fold(&schema));
        assert_eq!(set.row_count(), rows_out, "{label}");
        println!(
            "run_set_fold_bench {label}: {rows_in} rows in, {rows_out} out, {:.1} instr/row",
            instructions as f64 / rows_in as f64
        );
    }
}
