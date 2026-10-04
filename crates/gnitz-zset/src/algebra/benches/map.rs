use super::tests::compute;
use super::tests::make_int_batch;
use super::tests::make_schema;
use super::tests::project;
use super::tests::reindex_on;
use super::MapPlan;
use crate::repr::{Batch, BatchBuilder};
use crate::schema::{SchemaColumn, SchemaDescriptor, TypeCode};
use gnitz_expr::{LogicalInstr, LogicalProgram, Reg, Sink};
use gnitz_wire::{MapKind, NullKeys};

/// Retired instructions of the map driver: `evaluate_map_batch` over whole
/// batches, and `append_map_ranges` over one range (`*_whole`) and over `R`-row
/// runs with 16-row gaps (`*_r{R}`), where `COMPACT_RUN_LEN` decides.
/// Difference two pass counts, one shape per process:
///
///   cargo build -p gnitz-zset --release --tests
///   for s in reindex permute proj_keep_str proj_drop_str int3_whole int3_r16 upper_r16 permute_r1; do for p in 1 21; do \
///     GNITZ_BENCH_SHAPE=$s GNITZ_BENCH_PASSES=$p perf stat -e instructions:u \
///     cargo test -p gnitz-zset --release map_ranges_bench -- --ignored --nocapture
///   done; done
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn map_ranges_bench() {
    use gnitz_expr::IntArithOp;
    use std::hint::black_box;

    const N: usize = 262_144;
    const GAP: usize = 16;
    let passes = gnitz_foundation::perf::bench_passes();
    let only = std::env::var("GNITZ_BENCH_SHAPE").unwrap_or_else(|_| "all".to_string());
    let driven = |name: &str| only == "all" || only == name;
    let mut n_selected = 0usize;
    let mut acc = 0usize;

    // --- Reindex map: [U64 PK, I64, I64] reindexed on col 1, the source PK and
    // both payload columns kept — the equijoin / GROUP BY repartition shape.
    // Column 0 is what puts a `ColumnLocator::Pk` copy in the loop; the keep-set
    // rules retain the source PK on every one of those.
    let rx_in = make_schema(&[TypeCode::U64, TypeCode::I64, TypeCode::I64]);
    let mut rx_batch = BatchBuilder::new(&rx_in);
    for i in 0..N as u64 {
        rx_batch.begin_row(i as u128, 1i64);
        rx_batch.put_int(i.wrapping_mul(2_654_435_761) as u128);
        rx_batch.put_int((!i) as u128);
        rx_batch.end_row();
    }
    let rx_batch = rx_batch.finish();
    let mut rx_plan = MapPlan::from_wire(&rx_in, &reindex_on(&rx_in, &[1], vec![0, 1, 2], NullKeys::Keep)).unwrap();

    // --- String source: [U64 PK, STRING], every cell past the inline threshold
    // so every one is heap-backed and an emit grows the output blob.
    let str_in = make_schema(&[TypeCode::U64, TypeCode::String]);
    let mut str_batch = BatchBuilder::new(&str_in);
    for i in 0..N {
        str_batch.begin_row(i as u128, 1i64);
        str_batch.put_blob(format!("row-{i:012}-payload").as_bytes());
        str_batch.end_row();
    }
    let str_batch = str_batch.finish();
    let upper = || {
        let prog = LogicalProgram::new(
            vec![
                LogicalInstr::LoadColStr { col: 1 },
                LogicalInstr::StrCase { a: Reg(0), upper: true },
            ],
            vec![Sink::Reg(Reg(1))],
            vec![],
        );
        compute(&str_in, prog, &[(TypeCode::String, true)])
    };
    let mut se_plan = upper();

    // --- Integer source: [U64 PK, I64, I64, I64], and a map computing three
    // columns out of it.
    let int_in = make_schema(&[TypeCode::U64, TypeCode::I64, TypeCode::I64, TypeCode::I64]);
    let mut int_batch = BatchBuilder::new(&int_in);
    for i in 0..N as u64 {
        int_batch.begin_row(i as u128, 1);
        for pi in 0..3u64 {
            int_batch.put_int((i.wrapping_mul(2_654_435_761 + pi) % 1000) as u128);
        }
        int_batch.end_row();
    }
    let int_batch = int_batch.finish();
    let arith = |op, a, b| LogicalInstr::IntArith { op, a: Reg(a), b: Reg(b) };
    let int3 = || {
        let prog = LogicalProgram::new(
            vec![
                LogicalInstr::LoadCol { col: 1 },
                LogicalInstr::LoadCol { col: 2 },
                LogicalInstr::LoadCol { col: 3 },
                arith(IntArithOp::Add, 0, 1),
                arith(IntArithOp::Mul, 1, 2),
                arith(IntArithOp::Sub, 2, 0),
            ],
            vec![Sink::Reg(Reg(3)), Sink::Reg(Reg(4)), Sink::Reg(Reg(5))],
            vec![],
        );
        compute(&int_in, prog, &[(TypeCode::I64, true); 3])
    };
    // A pure projection that moves every payload column to another slot.
    let mut permute = project(&int_in, &[3, 1, 2]);

    // --- Hash-row maps: [U64 PK, payload...] -> [U128 hash PK, payload...], the
    // set-op / DISTINCT leaf. Fixed-width shapes on both sides of the fold's
    // stack arm, and heap-backed strings beside integers.
    let hash_row = |name: &'static str, payload: &[TypeCode]| {
        let mut in_tcs = vec![TypeCode::U64];
        in_tcs.extend_from_slice(payload);
        let in_schema = make_schema(&in_tcs);
        let mut batch = BatchBuilder::new(&in_schema);
        for i in 0..N as u64 {
            batch.begin_row(i as u128, 1);
            for (pi, &tc) in payload.iter().enumerate() {
                if tc == TypeCode::String {
                    batch.put_string(&format!("row-{i:012}-payload"));
                } else {
                    batch.put_int(i.wrapping_mul(2_654_435_761 + pi as u64) as u128);
                }
            }
            batch.end_row();
        }
        let batch = batch.finish();
        let cols = (1..).zip(payload.iter().copied()).collect();
        let plan = MapPlan::from_wire(&in_schema, &MapKind::HashRow { cols }).unwrap();
        (name, plan, batch)
    };
    const I64: TypeCode = TypeCode::I64;
    const STR: TypeCode = TypeCode::String;
    let mut hash_rows = [
        hash_row("hash_row[i64x2]", &[I64, I64]),
        hash_row("hash_row[i64x10]", &[I64; 10]),
        hash_row("hash_row[i64,str]", &[I64, STR]),
        hash_row("hash_row[i64x3,str]", &[I64, I64, I64, STR]),
    ];

    // --- Two heap-backed string columns, [U64 PK, STRING, STRING], and a
    // projection keeping both, or only the first.
    let str2_in = make_schema(&[TypeCode::U64, TypeCode::String, TypeCode::String]);
    let mut str2_batch = BatchBuilder::new(&str2_in);
    for i in 0..N {
        str2_batch.begin_row(i as u128, 1i64);
        str2_batch.put_string(&format!("first-{i:016}-payload-first"));
        str2_batch.put_string(&format!("second-{i:016}-payload-second"));
        str2_batch.end_row();
    }
    let str2_batch = str2_batch.finish();
    let (mut keep_str, mut drop_str) = (project(&str2_in, &[1, 2]), project(&str2_in, &[1]));

    // --- Column copies: one column of `[pk..., payload]` copied into an I64 slot,
    // widened from a PK or a payload cell, or decoded at its own width.
    let col_copy = |name: &'static str, pk: &[TypeCode], payload: TypeCode, col: u32| {
        let mut cols: Vec<SchemaColumn> = pk.iter().map(|&t| SchemaColumn::new(t, false)).collect();
        cols.push(SchemaColumn::new(payload, false));
        let in_schema = SchemaDescriptor::new(&cols, &(0..pk.len() as u32).collect::<Vec<_>>());
        let mut batch = BatchBuilder::new(&in_schema);
        for i in 0..N as u64 {
            let natives: Vec<u128> = (0..pk.len() as u64).map(|c| (i + c) as u32 as u128).collect();
            batch.begin_row_natives(&natives, 1);
            batch.put_int(i.wrapping_mul(2_654_435_761) as u16 as u128);
            batch.end_row();
        }
        let plan = compute(&in_schema, LogicalProgram::copy_cols(&[col]), &[(TypeCode::I64, false)]);
        (name, plan, batch.finish())
    };
    let mut col_copies = [
        col_copy("widen_pk_i32", &[TypeCode::I32], TypeCode::I64, 0),
        col_copy("widen_pk_i32_u64", &[TypeCode::I32, TypeCode::U64], TypeCode::I64, 0),
        col_copy("widen_payload_i32", &[TypeCode::U64], TypeCode::I32, 1),
        col_copy("widen_payload_u16", &[TypeCode::U64], TypeCode::U16, 1),
        col_copy("copy_pk_i64", &[TypeCode::I64], TypeCode::I64, 0),
    ];

    // Whole-batch shapes, through `evaluate_map_batch`.
    let whole = [
        ("reindex", &mut rx_plan, &rx_batch),
        ("str_emit", &mut se_plan, &str_batch),
        ("permute", &mut permute, &int_batch),
        ("proj_keep_str", &mut keep_str, &str2_batch),
        ("proj_drop_str", &mut drop_str, &str2_batch),
    ]
    .into_iter()
    .chain(hash_rows.iter_mut().map(|(name, plan, batch)| (*name, plan, &*batch)))
    .chain(col_copies.iter_mut().map(|(name, plan, batch)| (*name, plan, &*batch)));
    let counter = gnitz_foundation::perf::Counter::instructions().expect("instructions counter");
    for (name, plan, src) in whole {
        if !driven(name) {
            continue;
        }
        n_selected += 1;
        // Untimed: the batch pool holds an arena of the output's size.
        black_box(plan.evaluate_map_batch(src));
        let ((), instructions) = counter.measure(|| {
            for _ in 0..passes {
                let out = plan.evaluate_map_batch(black_box(src));
                acc = acc.wrapping_add(out.count).wrapping_add(out.blob().len());
                black_box(&out);
            }
        });
        println!(
            "map_ranges_bench {name}: {:.1} instr/row",
            instructions as f64 / (passes * N) as f64
        );
    }

    // Range shapes, through `append_map_ranges`: one range, and `r`-row runs with
    // `GAP`-row gaps.
    let runs = |r: usize| -> Vec<(usize, usize)> { (0..N).step_by(r + GAP).map(|s| (s, (s + r).min(N))).collect() };
    let mut shapes: Vec<(String, Vec<(usize, usize)>)> = vec![("whole".to_string(), vec![(0, N)])];
    shapes.extend([1usize, 4, 16, 64, 128, 256].map(|r| (format!("r{r}"), runs(r))));
    for (family, mut plan, src) in [
        ("int3", int3(), &int_batch),
        ("upper", upper(), &str_batch),
        ("permute", project(&int_in, &[3, 1, 2]), &int_batch),
    ] {
        let mut keeper = Batch::empty_with_schema(plan.out_schema());
        for (suffix, ranges) in &shapes {
            if !driven(&format!("{family}_{suffix}")) {
                continue;
            }
            n_selected += 1;
            for _ in 0..passes {
                keeper.clear();
                plan.append_map_ranges(black_box(src), &mut keeper, ranges);
                acc = acc.wrapping_add(keeper.count).wrapping_add(keeper.blob().len());
            }
            let survivors: usize = ranges.iter().map(|&(s, e)| e - s).sum();
            println!(
                "map_ranges_bench {family}_{suffix}: {} ranges, {survivors} survivors",
                ranges.len()
            );
        }
    }
    println!(
        "map_ranges_bench shape={only} passes={passes} n={N} acc={}",
        black_box(acc)
    );
    assert!(n_selected > 0, "GNITZ_BENCH_SHAPE matched nothing: {only:?}");
}

/// A `Drop` reindex against the `IS NOT NULL` filter it replaces, at NULL keys
/// spread evenly so every survivor run is short.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn reindex_drop_null_keys_bench() {
    use std::hint::black_box;
    const N: usize = 64 * 1024;
    const ITERS: usize = 50;
    let s = make_schema(&[TypeCode::U64, TypeCode::I64, TypeCode::I64, TypeCode::I64]);
    let counters = [
        gnitz_foundation::perf::Counter::instructions(),
        gnitz_foundation::perf::Counter::cycles(),
    ];
    let plan = |nulls| MapPlan::from_wire(&s, &reindex_on(&s, &[1], vec![1, 2, 3], nulls)).unwrap();
    let mut not_null = LogicalProgram::new(
        vec![LogicalInstr::IsNull { col: 1, invert: true }],
        vec![Sink::Reg(Reg(0))],
        vec![],
    )
    .resolve_filter(&s)
    .unwrap();

    // A first pass that prints nothing: the allocator's first large batches pay
    // for mapping fresh memory, which would land on whichever arm runs first.
    for (warm, null_every) in [
        (true, 10),
        (false, 0usize),
        (false, 100),
        (false, 32),
        (false, 16),
        (false, 10),
    ] {
        let is_null = |i: usize| null_every != 0 && i.is_multiple_of(null_every);
        let cells: Vec<[Option<i64>; 3]> = (0..N)
            .map(|i| {
                [
                    (!is_null(i)).then_some((i as i64 * 7919) % 4096),
                    Some(i as i64),
                    Some(-(i as i64)),
                ]
            })
            .collect();
        let rows: Vec<(u64, i64, &[Option<i64>])> =
            cells.iter().enumerate().map(|(i, c)| (i as u64, 1, &c[..])).collect();
        let batch = make_int_batch(&s, &rows);
        let (mut keep, mut drop) = (plan(NullKeys::Keep), plan(NullKeys::Drop));
        let pct = if null_every == 0 {
            0.0
        } else {
            100.0 / null_every as f64
        };
        let arm = |name: &str, f: &mut dyn FnMut()| {
            if warm {
                return f();
            }
            let t = crate::test_support::bench_time(ITERS, &mut *f);
            let per_row = |v: f64| v / N as f64;
            let [instrs, cycles] = counters.each_ref().map(|c| {
                c.as_ref()
                    .map_or("-".into(), |c| format!("{:.1}", per_row(c.measure(&mut *f).1 as f64)))
            });
            println!(
                "{pct:>4.1}% NULL  {name:<28} {:>7.2} ns/row  {instrs:>7} instr/row  {cycles:>7} cycles/row",
                per_row(t.as_nanos() as f64 / ITERS as f64),
            );
        };
        arm("(a) filter + reindex", &mut || {
            let filtered = crate::algebra::op_filter(&batch, &mut not_null);
            black_box(keep.evaluate_map_batch(filtered.as_ref().unwrap_or(&batch)));
        });
        arm("(a) drop reindex", &mut || {
            black_box(drop.evaluate_map_batch(&batch));
        });
        arm("(b) reindex + clone", &mut || {
            let all = keep.evaluate_map_batch(&batch);
            black_box(Batch::clone(&all));
            black_box(all);
        });
        arm("(b) keep + drop reindex", &mut || {
            black_box(drop.evaluate_map_batch(&batch));
            black_box(keep.evaluate_map_batch(&batch));
        });
    }
}
