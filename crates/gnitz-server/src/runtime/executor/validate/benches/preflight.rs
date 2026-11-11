use super::super::unique_filter::UniqueFilter;
use super::*;
use crate::catalog::SysFamily;
use crate::runtime::master::fixtures::test_dispatcher;
use crate::runtime::test_support::try_poll_once;
use crate::test_support::{apply_ddl, col_def, idx_tab_batch, make_batch_raw, table_engine, u64_i64_cols};
use gnitz_foundation::perf::Counter;
use gnitz_wire::TypeCode;
use gnitz_zset::repr::BatchBuilder;

/// What the master spends on an insert before it can commit it, in instructions
/// per row: the bundle's fold, the batch of keys to probe, and that probe routed
/// to the workers and laid out on the SAL. The validation is run up to its wait
/// on the workers' answers.
///
/// Over a table with a UNIQUE index on its payload column, the filter is seeded
/// empty, as an empty table's is, so the write's spans are built and the filter
/// proves every one absent.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn preflight_probe_bench() {
    let shapes: [(&str, TypeCode, bool); 3] = [
        ("plain", TypeCode::I64, false),
        ("unique i64", TypeCode::I64, true),
        ("unique uuid", TypeCode::UUID, true),
    ];
    let counter = Counter::instructions();
    for (shape, payload, unique) in shapes {
        let cols = [u64_i64_cols()[0].clone(), col_def("v", payload)];
        let name = format!("preflight_probe_bench_{}", shape.replace(' ', "_"));
        let (mut engine, tid) = table_engine("validate", &name, 1, &cols);
        if unique {
            let id = engine.allocate_ids(1).unwrap();
            let idx = idx_tab_batch(id, tid, &[1], "public__t__idx_v", true, 1);
            apply_ddl(&mut engine, [(SysFamily::Index, idx)]).unwrap();
        }
        let schema = engine.registry.relation(tid).unwrap().schema();

        for (order, rows) in [1u64, 100, 20_000]
            .into_iter()
            .flat_map(|n| [("ascending", n), ("scrambled", n)])
        {
            let mut ids: Vec<u64> = (0..rows).collect();
            if order == "scrambled" {
                gnitz_zset_testkit::Rng::new(0x9E37_79B9_7F4A_7C15).shuffle(&mut ids);
            }
            let mut bb = BatchBuilder::new(&schema);
            for &i in &ids {
                bb.begin_row(i as u128, 1);
                match payload {
                    TypeCode::UUID => bb.put_int(gnitz_zset_testkit::mix(i) as u128 | (i as u128) << 64),
                    _ => bb.put_int(i as u128),
                }
                bb.end_row();
            }
            let inserted = bb.finish();
            // Its own: no worker drains the SAL the validations write to.
            let (disp, _) = test_dispatcher(4, engine);
            let checks = Checks::new();
            if unique {
                checks.unique_filter_seed(tid, PkColList::from_slice(&[1]), UniqueFilter::new());
            }
            let mut families = vec![TxnFamily::new(
                tid,
                WireConflictMode::Error,
                FamilyRows::Built(inserted),
            )];
            let mut validate = || counter.measure(|| try_poll_once(checks.validate_txn(&disp, &mut families)));
            validate(); // the fold's and the scatter's first allocations
            let (verdict, instructions) = validate();
            assert!(
                verdict.is_none(),
                "{shape}, {rows} rows: a verdict with no worker answering"
            );
            println!(
                "preflight_probe_bench {shape:<11} {rows:>5} rows {order:<9} {:>7.1} instr/row",
                instructions as f64 / rows as f64
            );
            engine = disp.into_catalog();
        }

        let dir = engine.registry.base_dir().to_owned();
        drop(engine);
        let _ = std::fs::remove_dir_all(dir);
    }
}

/// What the master spends on an upsert into a table whose payload column a
/// child's FK references, in instructions per row, by phase: the bundle's fold,
/// the PK-column probe planned over every key, and that probe's replies folded
/// in, each key answered as committed. The replies are synthetic: one frame per
/// worker, each holding the keys `key % 4` sends it, ascending.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn preflight_sink_bench() {
    use crate::test_support::fk_def;

    let (mut engine, parent) = table_engine("validate", "preflight_sink_bench", 1, &u64_i64_cols());
    let id = engine.allocate_ids(1).unwrap();
    let idx = idx_tab_batch(id, parent, &[1], "public__t__idx_v", true, 1);
    apply_ddl(&mut engine, [(SysFamily::Index, idx)]).unwrap();
    let child_cols = [col_def("cid", TypeCode::U64), fk_def("fk", TypeCode::I64, parent, 1)];
    engine.create_table("public.child", &child_cols, &[0]).unwrap();
    let schema = engine.registry.relation(parent).unwrap().schema();
    let reply = engine
        .registry
        .relation(parent)
        .unwrap()
        .probe_reply_schema(Probe::PkColumn(1))
        .unwrap();

    let counter = Counter::instructions();
    for (order, rows) in [1u64, 100, 20_000]
        .into_iter()
        .flat_map(|n| [("ascending", n), ("scrambled", n)])
    {
        let mut ids: Vec<u64> = (0..rows).collect();
        if order == "scrambled" {
            gnitz_zset_testkit::Rng::new(0x9E37_79B9_7F4A_7C15).shuffle(&mut ids);
        }
        let upserts: Vec<_> = ids.iter().map(|&i| (i, 1, (i + 1_000_000) as i64)).collect();
        let families = vec![TxnFamily::new(
            parent,
            WireConflictMode::Update,
            FamilyRows::Built(make_batch_raw(&schema, &upserts)),
        )];
        let frames: Vec<Batch> = (0..4)
            .map(|w| {
                let replies: Vec<_> = (0..rows).filter(|i| i % 4 == w).map(|i| (i, 1, i as i64)).collect();
                make_batch_raw(&reply, &replies)
            })
            .collect();

        let (disp, _) = test_dispatcher(4, engine);
        let run = || {
            let (b, fold) = counter.measure(|| TxnBundle::new(&disp.cat(), &families).unwrap());
            let mut b = std::hint::black_box(b);
            let mut checks: Vec<Check<Rule>> = Vec::new();
            let ((), plan) = counter.measure(|| plan_pk_checks(&disp.cat(), &b, &mut checks).unwrap());
            let [Check { plan: Rule::Gather { old, .. }, .. }] = &mut checks[..] else {
                panic!("one PK-column probe");
            };
            let ((), sink) = counter.measure(|| {
                let t = b.get_mut(parent).unwrap();
                for f in &frames {
                    fold_pk_reply(&disp, t, &f.as_mem_batch(), &reply, Some(old)).unwrap();
                }
            });
            assert_eq!(old.len() as u64, rows, "every key's before-image");
            [fold, plan, sink]
        };
        run(); // first allocations
        let [fold, plan, sink] = run().map(|i| i as f64 / rows as f64);
        println!(
            "preflight_sink_bench {rows:>5} rows {order:<9} fold {fold:>7.1} plan {plan:>7.1} sink {sink:>7.1} \
             total {:>7.1} instr/row",
            fold + plan + sink
        );
        engine = disp.into_catalog();
    }

    let dir = engine.registry.base_dir().to_owned();
    drop(engine);
    let _ = std::fs::remove_dir_all(dir);
}
