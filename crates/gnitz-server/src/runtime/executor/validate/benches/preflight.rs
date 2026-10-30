use super::*;
use crate::runtime::master::fixtures::test_dispatcher;
use crate::runtime::test_support::try_poll_once;
use crate::test_support::{make_batch_raw, table_engine, u64_i64_cols};
use gnitz_foundation::perf::Counter;

/// What the master spends on an insert before it can commit it, in instructions
/// per row: the bundle's fold, the batch of keys to probe, and that probe routed
/// to the workers and laid out on the SAL. The validation is run up to its wait
/// on the workers' answers.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn preflight_probe_bench() {
    let (mut engine, tid) = table_engine("validate", "preflight_probe_bench", 1, &u64_i64_cols());
    let schema = engine.registry.relation(tid).unwrap().schema();

    let counter = Counter::instructions();
    for (order, rows) in [1u64, 100, 20_000]
        .into_iter()
        .flat_map(|n| [("ascending", n), ("scrambled", n)])
    {
        let mut ids: Vec<u64> = (0..rows).collect();
        if order == "scrambled" {
            gnitz_zset_testkit::Rng::new(0x9E37_79B9_7F4A_7C15).shuffle(&mut ids);
        }
        let inserted: Vec<_> = ids.iter().map(|&i| (i, 1, i as i64)).collect();
        // Its own: no worker drains the SAL the validations write to.
        let (disp, _) = test_dispatcher(4, engine);
        let checks = Checks::new();
        let mut families = vec![TxnFamily {
            tid,
            mode: WireConflictMode::Error,
            batch: make_batch_raw(&schema, &inserted),
        }];
        let mut validate = || counter.measure(|| try_poll_once(checks.validate_txn(&disp, &mut families)));
        validate(); // the fold's and the scatter's first allocations
        let (verdict, instructions) = validate();
        assert!(verdict.is_none(), "{rows} rows: a verdict with no worker answering");
        println!(
            "preflight_probe_bench {rows:>5} rows {order:<9} {:>7.1} instr/row",
            instructions as f64 / rows as f64
        );
        engine = disp.into_catalog();
    }

    let dir = engine.registry.base_dir().to_owned();
    drop(engine);
    let _ = std::fs::remove_dir_all(dir);
}
