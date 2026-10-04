use super::super::fixtures::test_dispatcher;
use super::*;
use crate::runtime::test_support::try_poll_once;
use crate::test_support::{col_def, make_batch_raw, scratch_dir};
use gnitz_foundation::perf::Counter;
use gnitz_wire::TypeCode;

/// What the master spends on an insert before it can commit it, in instructions
/// per row: the bundle's fold, the batch of keys to probe, and that probe routed
/// to the workers and laid out on the SAL. The validation is run up to its wait
/// on the workers' answers.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn preflight_probe_bench() {
    let dir = scratch_dir("master", "preflight_probe_bench");
    let mut engine = CatalogEngine::open(&dir, 1).expect("open catalog");
    let cols = [col_def("id", TypeCode::U64), col_def("val", TypeCode::I64)];
    let tid = engine.create_table("public.t", &cols, &[0]).unwrap();
    let schema = engine.registry.relation(tid).unwrap().schema();
    let (disp, _) = test_dispatcher(vec![0; 4], &mut engine);

    let counter = Counter::instructions();
    for rows in [1u64, 100, 20_000] {
        let inserted: Vec<_> = (0..rows).map(|i| (i, 1, i as i64)).collect();
        let families = [TxnFamily {
            tid,
            mode: WireConflictMode::Error,
            batch: make_batch_raw(&schema, &inserted),
        }];
        let validate = || counter.measure(|| try_poll_once(disp.validate_txn_distributed(&families)));
        validate(); // the fold's and the scatter's first allocations
        let (verdict, instructions) = validate();
        assert!(verdict.is_none(), "{rows} rows: a verdict with no worker answering");
        println!(
            "preflight_probe_bench {rows:>5} rows {:>7.1} instr/row",
            instructions as f64 / rows as f64
        );
    }

    drop(disp);
    drop(engine);
    let _ = std::fs::remove_dir_all(dir);
}
