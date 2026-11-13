use super::super::fixtures::test_dispatcher;
use crate::test_support::col_def;
use crate::test_support::empty_catalog;
use gnitz_foundation::perf::Counter;
use gnitz_wire::TypeCode;

/// What the master pays to write one Tick group and take its rounds, in
/// instructions per group, by how many relations the catalog holds and how many
/// the group ticks.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn emit_tick_bench() {
    const GROUPS: u64 = 200;
    let cols = [col_def("id", TypeCode::U64), col_def("v", TypeCode::U64)];
    let counter = Counter::instructions();
    for (relations, ticked) in [(50, 1), (50, 50), (1000, 1), (1000, 50)] {
        let (_tmp, mut engine) = empty_catalog(1);
        let tids: Vec<u64> = (0..relations)
            .map(|i| engine.create_table(&format!("public.t{i}"), &cols, &[0]).unwrap())
            .collect();
        let (disp, _) = test_dispatcher(0, engine);
        let tids = &tids[..ticked];
        let alone = disp.alone();
        let mut excl = disp.sal().excl();
        let mut run = || {
            excl.checkpoint_reset();
            counter.measure(|| {
                for _ in 0..GROUPS {
                    drop(std::hint::black_box(disp.write_tick(
                        &alone,
                        &mut excl,
                        std::hint::black_box(tids),
                    )));
                }
            })
        };
        run(); // the SAL's pages and the ledger's table are taken once
        let ((), instructions) = run();
        println!(
            "emit_tick_bench {relations:>4} relations {ticked:>2} tids {:>8.1} instr/group",
            instructions as f64 / GROUPS as f64
        );
        drop(excl);
        disp.into_catalog().close();
    }
}
