use super::super::fixtures::test_dispatcher;
use super::*;
use crate::runtime::test_support::try_poll_once;
use crate::test_support::{make_batch_raw, scratch_dir, u64_i64_cols};
use gnitz_foundation::perf::Counter;

/// What the master spends on an insert before it can commit it, in instructions
/// per row: the bundle's fold, the batch of keys to probe, and that probe routed
/// to the workers and laid out on the SAL. The validation is run up to its wait
/// on the workers' answers.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn preflight_probe_bench() {
    let dir = scratch_dir("master", "preflight_probe_bench");
    let mut engine = CatalogEngine::open(&dir, 1).expect("open catalog");
    let cols = u64_i64_cols();
    let tid = engine.create_table("public.t", &cols, &[0]).unwrap();
    let schema = engine.registry.relation(tid).unwrap().schema();

    let counter = Counter::instructions();
    for (order, rows) in [1u64, 100, 20_000]
        .into_iter()
        .flat_map(|n| [("ascending", n), ("scrambled", n)])
    {
        let mut ids: Vec<u64> = (0..rows).collect();
        if order == "scrambled" {
            // A fixed Fisher-Yates shuffle under xorshift64.
            let mut x = 0x9E37_79B9_7F4A_7C15u64;
            for i in (1..ids.len()).rev() {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                ids.swap(i, (x % (i as u64 + 1)) as usize);
            }
        }
        let inserted: Vec<_> = ids.iter().map(|&i| (i, 1, i as i64)).collect();
        // Its own: no worker drains the SAL the validations write to.
        let (disp, _) = test_dispatcher(vec![0; 4], &mut engine);
        let mut families = vec![TxnFamily {
            tid,
            mode: WireConflictMode::Error,
            batch: make_batch_raw(&schema, &inserted),
        }];
        let mut validate = || counter.measure(|| try_poll_once(disp.validate_txn_distributed(&mut families)));
        validate(); // the fold's and the scatter's first allocations
        let (verdict, instructions) = validate();
        assert!(verdict.is_none(), "{rows} rows: a verdict with no worker answering");
        println!(
            "preflight_probe_bench {rows:>5} rows {order:<9} {:>7.1} instr/row",
            instructions as f64 / rows as f64
        );
    }

    drop(engine);
    let _ = std::fs::remove_dir_all(dir);
}
