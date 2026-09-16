//! Retired-instruction benchmarks for the evaluator's kernels — `#[ignore]`d, so
//! `make test` never runs them, and meaningless in a debug build.
//!
//! Each prints nothing useful on its own: run it at two `GNITZ_BENCH_PASSES`
//! counts and difference them, so batch setup and process start cancel out. Each
//! bench's own doc carries its `perf` invocation. Wall-clock on the development
//! machines swings far wider than the effects being measured, so it is never
//! reported — judge on `perf stat -e instructions:u`.
//!
//! Every bench still *builds and checks* every shape it knows; the `GNITZ_BENCH_*`
//! selectors cut only the driven loop, so a differenced pair attributes its cost
//! to the one shape named rather than to their sum. [`selected`] is what makes a
//! misspelled selector fail loudly instead of differencing to a 0 % effect.

use gnitz_wire::{type_code, FixedInt};

use crate::batch::MORSEL;
use crate::test_support::{
    both_arms, filter_prog, is_null_op, make_n_col_view, map_prog, passing_rows, push_payload_cols, scalar_prog,
    schema_pk_ints, schema_pk_strings, set_row_pk, FilterShape, TestSchema, TestView,
};
use crate::{CmpOp, ConstIdx, Evaluator, ExprBuilder, IntArithOp, LogicalInstr, LogicalProgram, Reg, Sink};

/// Assert the `GNITZ_BENCH_*` selector matched at least one of the shapes the
/// bench built. A misspelled selector would otherwise drive nothing and
/// difference to a 0 % effect instead of failing.
fn selected(var: &str, only: &str, count: usize) {
    assert!(count > 0, "{var} matched nothing: {only:?}");
}

/// The evidence for keeping the fused `StrColConst` opcode over the
/// register channel on the `col <op> 'const'` filter loop — ~1M rows,
/// non-nullable STRING. Both channels are built for every domain and their hit
/// counts asserted equal, which is the only differential correctness check
/// between them; `GNITZ_BENCH_CHANNEL` and `GNITZ_BENCH_DOMAIN` then cut the
/// *driven* region down to one, so a `perf stat` over two pass counts
/// differences to one channel's retired instructions on one domain.
///
/// Retired instructions per row per operator, `n = 1M`, differenced over
/// `GNITZ_BENCH_PASSES` 1 and 21 — the register channel is 1.6× to 6× the fused
/// one on every domain:
///
/// | domain             | fused | registers |
/// |--------------------|-------|-----------|
/// | mixed              |  22.5 |     112.0 |
/// | long               |  77.9 |     127.6 |
/// | digits-first       |  19.8 |     118.6 |
/// | abcd-shared-prefix |  53.9 |     118.6 |
///
/// The controlled pair is `digits-first` against `abcd-shared-prefix`: same
/// lengths, same content bytes, differing only in whether the 4-byte prefix
/// collides. Only the cell form can short-circuit on that prefix — a `StrView`
/// carries none — so the fused cost moves between the two and the register cost
/// does not, and the gap remaining at `abcd*` is the register lane's own cost of
/// materialising each row into a `MORSEL`-wide lane. Matching the lengths is
/// what makes that attributable.
///
///   for d in mixed long digits-first abcd-shared-prefix; do
///     for c in fused registers; do for p in 1 201; do \
///       GNITZ_BENCH_DOMAIN=$d GNITZ_BENCH_CHANNEL=$c GNITZ_BENCH_PASSES=$p \
///       perf stat -e instructions:u cargo test -p gnitz-expr --release \
///         str_const_filter_bench -- --ignored --nocapture --test-threads=1
///   done; done; done
#[test]
#[ignore]
fn str_const_filter_bench() {
    let passes = bench_passes();
    let channel = std::env::var("GNITZ_BENCH_CHANNEL").unwrap_or_else(|_| "both".to_string());
    assert!(
        matches!(channel.as_str(), "both" | "fused" | "registers"),
        "GNITZ_BENCH_CHANNEL must be both/fused/registers, got {channel:?}"
    );
    let only = std::env::var("GNITZ_BENCH_DOMAIN").unwrap_or_else(|_| "all".to_string());
    let (run_fused, run_regs) = (channel != "registers", channel != "fused");

    let schema = schema_pk_strings(1, false);
    let n = 1_000_000usize;
    // (domain, constant, value per row). `mixed` is the original fixture:
    // ~1/16 rows match and every 7th row is a long (heap-backed) string.
    type Domain = (&'static str, &'static str, fn(usize) -> String);
    let domains: [Domain; 4] = [
        ("mixed", "match_target", |row| {
            if row % 16 == 0 {
                "match_target".to_string()
            } else if row % 7 == 0 {
                format!("long_string_variant_number_{row}")
            } else {
                format!("k{}", row % 97)
            }
        }),
        ("long", "long_string_variant_number_42", |row| {
            format!("long_string_variant_number_{}", row % 97)
        }),
        // The controlled pair: `{i}abcd` and `abcd{i}` hold the same bytes at the
        // same lengths, so the prefix is the only thing that differs.
        ("digits-first", "42abcd", |row| format!("{}abcd", row % 97)),
        ("abcd-shared-prefix", "abcd42", |row| format!("abcd{}", row % 97)),
    ];

    let mut n_selected = 0usize;
    for (domain, constant, value) in domains {
        let mut mb = TestView::new(n, schema.pk_stride());
        push_payload_cols(&mut mb, &schema);
        for row in 0..n {
            set_row_pk(&mut mb, &schema, row, row as u64 + 1);
            mb.set_string(row, 0, value(row).as_bytes());
        }

        for (name, op) in [("eq", CmpOp::Eq), ("lt", CmpOp::Lt)] {
            let consts = vec![constant.as_bytes().to_vec()];
            let fused = filter_prog(
                &schema,
                vec![LogicalInstr::StrColConst { op, col: 1, const_idx: ConstIdx(0) }],
                Reg(0),
                consts.clone(),
            );
            let regs = filter_prog(
                &schema,
                vec![
                    LogicalInstr::LoadColStr { col: 1 },
                    LogicalInstr::LoadConstStr { const_idx: ConstIdx(0) },
                    LogicalInstr::StrCmp { op, a: Reg(0), b: Reg(1) },
                ],
                Reg(2),
                consts,
            );

            // Also the warm-up, and outside the driven region — so both channels
            // are still built and compared even when only one is driven.
            let count = |f: &Evaluator| {
                let mut ranges = Vec::new();
                f.filter_ranges(&mb, &mut ranges);
                ranges.iter().map(|&(s, e)| e - s).sum::<usize>()
            };
            let hits = count(&fused);
            assert_eq!(hits, count(&regs), "{domain}/{name}: the channels disagree");

            let run = |f: &Evaluator| {
                let mut ranges = Vec::new();
                let mut h = 0usize;
                for _ in 0..passes {
                    f.filter_ranges(&mb, &mut ranges);
                    h += ranges.len();
                }
                std::hint::black_box(h);
            };
            for (want, ev) in [(run_fused, &fused), (run_regs, &regs)] {
                if want && (only == "all" || only == domain) {
                    n_selected += 1;
                    run(ev);
                }
            }
            println!("str_const_filter_bench {domain}/{name}: passes={passes} n={n} hits={hits}");
        }
    }
    selected("GNITZ_BENCH_DOMAIN", &only, n_selected);
}

/// The pass count the `#[ignore]`d benches loop over, from `GNITZ_BENCH_PASSES`.
/// Two runs at different counts, differenced, cancel everything that happens
/// once per process.
fn bench_passes() -> usize {
    std::env::var("GNITZ_BENCH_PASSES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1)
}

/// Retired-instruction harness for the filter kernels.
///
///   for s in pk nullable; do for p in 1 501; do \
///     GNITZ_BENCH_SHAPE=$s GNITZ_BENCH_PASSES=$p perf stat -e instructions:u \
///     cargo test -p gnitz-expr --release filter_kernel_bench -- --ignored --nocapture
///   done; done
#[test]
#[ignore]
fn filter_kernel_bench() {
    let passes = bench_passes();
    // Every shape is still built and checked; only the driven loop is skipped,
    // so a `perf stat` over the process attributes its pass-count difference to
    // the one named here rather than to their sum.
    let only = std::env::var("GNITZ_BENCH_SHAPE").unwrap_or_else(|_| "all".to_string());
    let driven = |name: &str| only == "all" || only == name;
    let n = 200_000usize;

    // `pk > n/2` — the PK-region load.
    let pk_schema = schema_pk_ints(1, false);
    let pk_view = make_n_col_view(&pk_schema, n, |_, _| 1, |_, _| false);
    let pk_filter = filter_prog(
        &pk_schema,
        vec![
            LogicalInstr::LoadColInt { col: 0 },
            LogicalInstr::LoadConst { val: (n / 2) as i64 },
            LogicalInstr::Cmp { op: CmpOp::Gt, a: Reg(0), b: Reg(1) },
        ],
        Reg(2),
        vec![],
    );

    // `-a > 0 AND b < 80` over nullable columns — unary, the 3VL AND, and the
    // per-column null-bit gather.
    let nn_schema = schema_pk_ints(2, true);
    let nn_view = make_n_col_view(
        &nn_schema,
        n,
        |row, col| ((row * 7 + col) % 100) as i64,
        |row, _| row % 32 == 0,
    );
    let nn_filter = filter_prog(
        &nn_schema,
        vec![
            LogicalInstr::LoadColInt { col: 1 },
            LogicalInstr::IntUnary {
                op: crate::program::IntUnaryOp::Neg,
                a: Reg(0),
            },
            LogicalInstr::LoadConst { val: 0 },
            LogicalInstr::Cmp { op: CmpOp::Gt, a: Reg(1), b: Reg(2) },
            LogicalInstr::LoadColInt { col: 2 },
            LogicalInstr::LoadConst { val: 80 },
            LogicalInstr::Cmp { op: CmpOp::Lt, a: Reg(4), b: Reg(5) },
            LogicalInstr::BoolBinary { is_or: false, a: Reg(3), b: Reg(6) },
        ],
        Reg(7),
        vec![],
    );

    // Five literal comparisons over one NOT NULL column: the `no_nulls` arm's
    // cheapest per-row work carrying the most constant registers, which is where
    // a per-morsel constant refill would show.
    let lit_schema = schema_pk_ints(1, false);
    let lit_view = make_n_col_view(&lit_schema, n, |row, _| (row % 1000) as i64, |_, _| false);
    let mut lit_instrs = vec![LogicalInstr::LoadColInt { col: 1 }];
    let mut acc_reg = None;
    for (op, val) in [
        (CmpOp::Gt, 1i64),
        (CmpOp::Lt, 999),
        (CmpOp::Ne, 5),
        (CmpOp::Ne, 7),
        (CmpOp::Ne, 9),
    ] {
        // A register IS the index of the instruction that writes it, and the
        // `BoolBinary` pushes make that index no function of the loop counter —
        // so each one is read off the stream immediately before its push.
        let k = lit_instrs.len() as u16;
        lit_instrs.push(LogicalInstr::LoadConst { val });
        let c = lit_instrs.len() as u16;
        lit_instrs.push(LogicalInstr::Cmp { op, a: Reg(0), b: Reg(k) });
        acc_reg = Some(match acc_reg {
            None => c,
            Some(prev) => {
                let d = lit_instrs.len() as u16;
                lit_instrs.push(LogicalInstr::BoolBinary { is_or: false, a: Reg(prev), b: Reg(c) });
                d
            }
        });
    }
    let lit_result = acc_reg.expect("the literal chain has at least one compare");
    let lit_filter = filter_prog(&lit_schema, lit_instrs, Reg(lit_result), vec![]);

    let mut hits = 0usize;
    let mut n_selected = 0usize;
    let mut ranges = Vec::new();
    for (name, ev, view) in [
        ("pk", &pk_filter, &pk_view),
        ("nullable", &nn_filter, &nn_view),
        ("literals", &lit_filter, &lit_view),
    ] {
        if !driven(name) {
            continue;
        }
        n_selected += 1;
        for _ in 0..passes {
            ev.filter_ranges(view, &mut ranges);
            hits += ranges.len();
        }
    }
    println!(
        "filter_kernel_bench passes={passes} n={n} hits={}",
        std::hint::black_box(hits)
    );
    // A misspelled shape would otherwise drive nothing and difference to a 0 %
    // effect instead of failing.
    selected("GNITZ_BENCH_SHAPE", &only, n_selected);
}

/// `col1 IS NULL AND col2 > k AND ... ` over `is_null_bench_schema`: `n_cmp`
/// compares of NOT NULL columns hung off one null test, so the whole predicate
/// still resolves `no_nulls`. `n_cmp` sets the chain depth, which is what scales
/// the per-conjunct cost the arms are being compared on.
fn is_null_chain(k: i64, n_cmp: u16) -> FilterShape {
    let mut instrs = vec![is_null_op(1)];
    if n_cmp == 0 {
        // No compare, so no constant to load — an unread `LoadConst` would still
        // cost a register write per morsel and blunt the bare shape's figure.
        return (instrs, Reg(0));
    }
    instrs.push(LogicalInstr::LoadConst { val: k });
    let mut acc = 0u16;
    for i in 0..n_cmp {
        let base = 2 + i * 3;
        instrs.push(LogicalInstr::LoadColInt { col: u32::from(i) + 2 });
        instrs.push(LogicalInstr::Cmp { op: CmpOp::Gt, a: Reg(base), b: Reg(1) });
        instrs.push(LogicalInstr::BoolBinary {
            is_or: false,
            a: Reg(acc),
            b: Reg(base + 1),
        });
        acc = base + 2;
    }
    (instrs, Reg(acc))
}

/// One nullable column (the null test's) plus four NOT NULL ones (the
/// compares'). Mixing the two is what the bench is about: a compare over a
/// nullable column would hold the program on the nullable arm through its own
/// load, whatever the null test is classified as.
fn is_null_bench_schema() -> TestSchema {
    let mut cols = vec![(type_code::U64, false), (type_code::I64, true)];
    cols.extend(std::iter::repeat_n((type_code::I64, false), 4));
    TestSchema::new(&cols, &[0])
}

/// A/B for the arm an `IS [NOT] NULL` predicate lands on. Each shape is built
/// twice from one instruction stream — once as resolution classifies it
/// (`no_nulls`), once forced onto the nullable arm — and the two are asserted to
/// select the same rows before either is driven. Prints no measurement itself,
/// like [`filter_kernel_bench`]: `GNITZ_BENCH_SHAPE` and `GNITZ_BENCH_ARM` cut
/// the run down to one driven loop, and differencing two pass counts under
/// `perf` cancels fixture construction, the warm-up and process start.
///
///   for s in bare one_and chain_spread chain_clustered \
///            chain_nonselective chain_rare map; do
///     for arm in fast nullable; do for p in 1 201; do \
///       GNITZ_BENCH_SHAPE=$s GNITZ_BENCH_ARM=$arm GNITZ_BENCH_PASSES=$p \
///       perf stat -e instructions:u,cycles:u cargo test -p gnitz-expr --release \
///         is_null_arm_bench -- --ignored --nocapture --test-threads=1
///   done; done; done
///
/// Moving to the fast arm is cheaper on every shape here, at
/// `-C target-cpu=x86-64-v3` (what `crates/.cargo/config.toml` ships): −3.0 %
/// retired instructions on `map`, −5.9 % on `bare`, −11.1 % on `one_and`, and
/// −13.8 % to −14.5 % across the four 4-conjunct chains. Those figures came off
/// **one build**: the direction and the rank order reproduce, the magnitudes run
/// 10–40 % smaller on a fresh one, so re-measure before trading on a number.
/// What separates the
/// chain shapes from each other is only their NULL arrangement, and it barely
/// separates them at all — the arms run the same kernels over the same word
/// count. The compares read NOT NULL columns, which are outside
/// `nullable_slots`, so the nullable arm clears their null words rather than
/// gathering per row; what is left is the null bookkeeping the fast arm has
/// none of.
///
/// Take both events. `instructions:u` repeats here to under 0.001 %, `cycles:u`
/// to a few percent; the first is the reproducible one, the second is the one
/// that sees a stall. Neither is a constant — batch size, NULL rate and
/// clustering all move them.
#[test]
#[ignore]
fn is_null_arm_bench() {
    let passes = bench_passes();
    let arm = std::env::var("GNITZ_BENCH_ARM").unwrap_or_else(|_| "both".to_string());
    let (run_fast, run_nullable) = (arm != "nullable", arm != "fast");
    // Which shape to drive. Every shape is still built and checked; only the
    // driven loop is skipped, so a `perf stat` over the process attributes its
    // pass-count difference to the one named here.
    let only = std::env::var("GNITZ_BENCH_SHAPE").unwrap_or_else(|_| "all".to_string());
    // Both selectors are decoded by inequality, so a typo would silently drive
    // nothing (or both arms) and read out as a 0 % effect rather than an error.
    assert!(
        matches!(arm.as_str(), "both" | "fast" | "nullable"),
        "GNITZ_BENCH_ARM must be both/fast/nullable, got {arm:?}"
    );
    let driven = |name: &str, want: bool| want && (only == "all" || only == name);
    let n = 200_000usize;
    let schema = is_null_bench_schema();

    // Three views, shared by the shapes that want the same NULL arrangement.
    // They span how the NULLs are distributed rather than just how many there
    // are: `spread` puts 16 per morsel, `rare` 4, and `clustered` gives 7 of
    // every 8 morsels no NULL at all — the arrangement a morsel-granular
    // optimization would be most sensitive to.
    let value = |row: usize, col: usize| ((row * 7 + col * 13) % 100) as i64;
    let spread = make_n_col_view(&schema, n, value, |row, col| col == 0 && row.is_multiple_of(16));
    let clustered = make_n_col_view(&schema, n, value, |row, col| {
        col == 0 && (row / MORSEL).is_multiple_of(8)
    });
    let rare = make_n_col_view(&schema, n, value, |row, col| col == 0 && row.is_multiple_of(64));

    // `k = 50` passes about half the rows; `k = -1` passes every row, which is
    // the non-selective variant.
    let shapes: [(&str, &TestView, FilterShape); 6] = [
        ("bare", &spread, is_null_chain(50, 0)),
        ("one_and", &spread, is_null_chain(50, 1)),
        ("chain_spread", &spread, is_null_chain(50, 4)),
        ("chain_clustered", &clustered, is_null_chain(50, 4)),
        ("chain_nonselective", &spread, is_null_chain(-1, 4)),
        ("chain_rare", &rare, is_null_chain(50, 4)),
    ];

    let mut n_selected = 0usize;
    for (name, view, (instrs, result_reg)) in &shapes {
        let (fast, nullable) = both_arms(name, || filter_prog(&schema, instrs.clone(), *result_reg, vec![]));
        // Also the warm-up, and outside the driven region.
        let passed = passing_rows(&fast, view);
        assert_eq!(passed, passing_rows(&nullable, view), "{name}: the arms disagree");
        let hits = passed.iter().filter(|&&p| p).count();

        let run = |ev: &Evaluator| {
            let mut ranges = Vec::new();
            let mut h = 0usize;
            for _ in 0..passes {
                ev.filter_ranges(*view, &mut ranges);
                h += ranges.len();
            }
            std::hint::black_box(h);
        };
        for (want, ev) in [(run_fast, &fast), (run_nullable, &nullable)] {
            if driven(name, want) {
                n_selected += 1;
                run(ev);
            }
        }
        println!("is_null_arm_bench {name}: passes={passes} n={n} hits={hits}");
    }

    // The map drive, which leaves through a register sink rather than a bitmap.
    let out_schema = schema_pk_ints(1, false);
    let map_instrs = vec![is_null_op(1)];
    let (fast, nullable) = both_arms("map", || {
        map_prog(
            &schema,
            &out_schema,
            map_instrs.clone(),
            vec![Sink::Reg(Reg(0))],
            vec![],
        )
    });
    // The warm-up doubles as the agreement check, as it does per filter shape.
    let emitted = |ev: &Evaluator| {
        let mut vals = Vec::with_capacity(n);
        ev.eval_morsels(&spread, 0, n, |_, out| vals.extend_from_slice(out.reg_values(0)));
        vals
    };
    assert_eq!(emitted(&fast), emitted(&nullable), "map: the arms disagree");
    let run = |ev: &Evaluator| {
        let mut acc = 0i64;
        for _ in 0..passes {
            ev.eval_morsels(&spread, 0, n, |_, out| acc += out.reg_values(0).iter().sum::<i64>());
        }
        std::hint::black_box(acc);
    };
    for (want, ev) in [(run_fast, &fast), (run_nullable, &nullable)] {
        if driven("map", want) {
            n_selected += 1;
            run(ev);
        }
    }
    println!("is_null_arm_bench map: passes={passes} n={n}");
    // A misspelled shape name would otherwise drive nothing at all, and the two
    // pass counts would difference to a 0 % effect instead of failing.
    selected("GNITZ_BENCH_SHAPE", &only, n_selected);
}

/// One `len`-byte haystack per row in the single STRING payload slot, matching
/// neither `%needle%` nor `%a%b%`: a fixed length is what makes a per-byte slope
/// readable, where [`str_bench_view`]'s alternating widths average two regimes.
/// Every row differs, so no matcher can be hoisted out of the row loop.
fn fixed_len_str_view(schema: &TestSchema, n: usize, len: usize) -> TestView {
    let mut v = TestView::new(n, schema.pk_stride());
    push_payload_cols(&mut v, schema);
    for row in 0..n {
        set_row_pk(&mut v, schema, row, row as u64 + 1);
        // `x` and the digits hold neither literal, so every scan runs to the end
        // — the non-matching haystack the slope is measured on.
        let mut s = vec![b'x'; len];
        s[row % len] = b'0' + (row % 10) as u8;
        v.set_string(row, 0, &s);
    }
    v
}

/// One row of `cols` German strings per payload slot, alternating either side of
/// the 12-byte inline boundary so a string bench drives both the in-place inline
/// view and the blob view.
fn str_bench_view(schema: &TestSchema, n: usize, cols: usize) -> TestView {
    let mut v = TestView::new(n, schema.pk_stride());
    push_payload_cols(&mut v, schema);
    for row in 0..n {
        set_row_pk(&mut v, schema, row, row as u64 + 1);
        for pi in 0..cols {
            let s = if row % 3 == 0 {
                format!("row-{row}-col-{pi}-past-the-inline-boundary")
            } else {
                format!("r{}{pi}", row % 100)
            };
            v.set_string(row, pi, s.as_bytes());
        }
    }
    v
}

/// Retired-instruction harness for the kernels [`filter_kernel_bench`] cannot
/// reach: the ones whose result is not a predicate. Same protocol — run at two
/// pass counts and difference, never report wall-clock.
///
///   for s in int_cast int_div select str_len str_upper str_like str_substr \
///            str_concat int_to_str map \
///            str_contains_12 str_contains_128 str_contains_512 \
///            str_generic_12 str_generic_128 str_generic_512 \
///            str_side_64 str_side_512; do
///     for p in 1 501; do \
///       GNITZ_BENCH_SHAPE=$s GNITZ_BENCH_PASSES=$p perf stat -e instructions:u \
///       cargo test -p gnitz-expr --release expr_kernel_bench -- --ignored --nocapture
///   done; done
///
/// One shape per opcode family, never combined: a scalar `idiv` swamps a cast by
/// an order of magnitude, so a shared shape would difference to that one arm.
///
/// The three suffixed families carry a haystack length because their cost is a
/// slope in it, not a constant: `Contains` and `Generic` scan the value, and
/// `RIGHT` walks it. `str_like`'s own `%boundary` pattern specializes to
/// `Suffix`, which answers off the tail alone and enters neither scan.
#[test]
#[ignore]
fn expr_kernel_bench() {
    let passes = bench_passes();
    let only = std::env::var("GNITZ_BENCH_SHAPE").unwrap_or_else(|_| "all".to_string());
    let driven = |name: &str| only == "all" || only == name;
    let mut n_selected = 0usize;
    let n = 200_000usize;

    // --- scalar shapes over two nullable I64 columns ---
    let ints = schema_pk_ints(2, true);
    let int_view = make_n_col_view(
        &ints,
        n,
        |row, col| ((row * 7 + col) % 1000 + 1) as i64,
        |row, _| row % 32 == 0,
    );
    let load2 = |c: u32| LogicalInstr::LoadColInt { col: c };

    let int_cast = scalar_prog(
        &ints,
        vec![load2(1), LogicalInstr::IntCast { a: Reg(0), fi: FixedInt::I32 }],
        Reg(1),
        vec![],
    );
    let int_div = scalar_prog(
        &ints,
        vec![
            load2(1),
            LogicalInstr::LoadConst { val: 7 },
            LogicalInstr::IntArith {
                op: IntArithOp::Div,
                a: Reg(0),
                b: Reg(1),
            },
        ],
        Reg(2),
        vec![],
    );
    // `CASE WHEN a > b THEN a ELSE b END` over nullable columns — the blend's
    // nullable arm, which nothing else in the tree drives.
    let select = scalar_prog(
        &ints,
        vec![
            load2(1),
            load2(2),
            LogicalInstr::Cmp { op: CmpOp::Gt, a: Reg(0), b: Reg(1) },
            LogicalInstr::Select { cond: Reg(2), a: Reg(0), b: Reg(1) },
        ],
        Reg(3),
        vec![],
    );
    let int_to_str = scalar_prog(
        &ints,
        vec![load2(1), LogicalInstr::IntToStr { a: Reg(0) }],
        Reg(1),
        vec![],
    );

    // --- string shapes over two NOT NULL STRING columns ---
    let strs = schema_pk_strings(2, false);
    let str_view = str_bench_view(&strs, n, 2);
    let load_str = |c: u32| LogicalInstr::LoadColStr { col: c };

    let str_len = scalar_prog(
        &strs,
        vec![load_str(1), LogicalInstr::StrLen { a: Reg(0), chars: false }],
        Reg(1),
        vec![],
    );
    let str_upper = scalar_prog(
        &strs,
        vec![load_str(1), LogicalInstr::StrCase { a: Reg(0), upper: true }],
        Reg(1),
        vec![],
    );
    let str_like = scalar_prog(
        &strs,
        vec![
            load_str(1),
            LogicalInstr::StrLike {
                src: Reg(0),
                escape: None,
                pat_idx: ConstIdx(0),
                ci: false,
            },
        ],
        Reg(1),
        vec![b"%boundary".to_vec()],
    );
    let str_substr = scalar_prog(
        &strs,
        vec![
            load_str(1),
            LogicalInstr::LoadConst { val: 2 },
            LogicalInstr::LoadConst { val: 6 },
            LogicalInstr::StrSubstr {
                src: Reg(0),
                start_reg: Reg(1),
                len_reg: Some(Reg(2)),
            },
        ],
        Reg(3),
        vec![],
    );
    let str_concat = scalar_prog(
        &strs,
        vec![
            load_str(1),
            load_str(2),
            LogicalInstr::StrConcat { a: Reg(0), b: Reg(1), skip_null: false },
        ],
        Reg(2),
        vec![],
    );

    // --- LIKE shapes whose cost is a slope in the haystack: `Contains` runs
    //     `find`'s scan, `Generic` the anchor slide. One STRING column, so the
    //     view is the fixture and the pattern is the shape.
    let str1 = schema_pk_strings(1, false);
    let like_lens = [12usize, 128, 512];
    let like_views: Vec<TestView> = like_lens.iter().map(|&l| fixed_len_str_view(&str1, n, l)).collect();
    let like_prog = |pat: &[u8]| {
        scalar_prog(
            &str1,
            vec![
                load_str(1),
                LogicalInstr::StrLike {
                    src: Reg(0),
                    escape: None,
                    pat_idx: ConstIdx(0),
                    ci: false,
                },
            ],
            Reg(1),
            vec![pat.to_vec()],
        )
    };
    let contains = like_prog(b"%needle%");
    let generic = like_prog(b"%a%b%");

    // --- `RIGHT(c, 10)`: the one kernel whose walk is from the far end.
    let side_lens = [64usize, 512];
    let side_views: Vec<TestView> = side_lens.iter().map(|&l| fixed_len_str_view(&str1, n, l)).collect();
    let str_side = scalar_prog(
        &str1,
        vec![
            load_str(1),
            LogicalInstr::LoadConst { val: 10 },
            LogicalInstr::StrSide { src: Reg(0), n_reg: Reg(1), left: false },
        ],
        Reg(2),
        vec![],
    );

    // --- a real map: six compute opcodes plus two register sinks, driven through
    //     `eval_morsels` the way a maintained view's projection is ---
    let map_in = schema_pk_ints(3, false);
    let map_out = schema_pk_ints(2, false);
    let map_view = make_n_col_view(&map_in, n, |row, col| ((row * 7 + col) % 1000) as i64, |_, _| false);
    let map = map_prog(
        &map_in,
        &map_out,
        vec![
            load2(1),
            load2(2),
            LogicalInstr::LoadColInt { col: 3 },
            LogicalInstr::IntArith {
                op: IntArithOp::Add,
                a: Reg(0),
                b: Reg(1),
            },
            LogicalInstr::IntArith {
                op: IntArithOp::Mul,
                a: Reg(3),
                b: Reg(2),
            },
            LogicalInstr::IntArith {
                op: IntArithOp::Sub,
                a: Reg(4),
                b: Reg(0),
            },
        ],
        vec![Sink::Reg(Reg(5)), Sink::Reg(Reg(3))],
        vec![],
    );

    let mut acc = 0i64;
    // Scalar-result shapes: sum the result register, as a register sink into an 8-byte
    // slot would read it.
    let mut scalar_shapes: Vec<(&str, &Evaluator, &TestView, usize)> = vec![
        ("int_cast", &int_cast, &int_view, 1usize),
        ("int_div", &int_div, &int_view, 2),
        ("select", &select, &int_view, 3),
        ("str_len", &str_len, &str_view, 1),
        ("str_like", &str_like, &str_view, 1),
        ("map", &map, &map_view, 5),
    ];
    // Named per length, so one `perf` pair reads one point of the slope.
    for (k, name) in ["str_contains_12", "str_contains_128", "str_contains_512"]
        .iter()
        .enumerate()
    {
        scalar_shapes.push((name, &contains, &like_views[k], 1));
    }
    for (k, name) in ["str_generic_12", "str_generic_128", "str_generic_512"]
        .iter()
        .enumerate()
    {
        scalar_shapes.push((name, &generic, &like_views[k], 1));
    }
    for (name, ev, view, reg) in scalar_shapes {
        if !driven(name) {
            continue;
        }
        n_selected += 1;
        for _ in 0..passes {
            ev.eval_morsels(view, 0, n, |_, out| acc += out.reg_values(reg).iter().sum::<i64>());
        }
    }
    // String-result shapes: resolve every view, as a string sink does.
    let mut str_shapes: Vec<(&str, &Evaluator, &TestView, usize)> = vec![
        ("int_to_str", &int_to_str, &int_view, 1usize),
        ("str_upper", &str_upper, &str_view, 1),
        ("str_substr", &str_substr, &str_view, 3),
        ("str_concat", &str_concat, &str_view, 2),
    ];
    for (k, name) in ["str_side_64", "str_side_512"].iter().enumerate() {
        str_shapes.push((name, &str_side, &side_views[k], 2));
    }
    for (name, ev, view, reg) in str_shapes {
        if !driven(name) {
            continue;
        }
        n_selected += 1;
        for _ in 0..passes {
            ev.eval_morsels(view, 0, n, |_, out| {
                for i in 0..out.rows() {
                    acc += out.str_bytes(reg, i).len() as i64;
                }
            });
        }
    }
    println!(
        "expr_kernel_bench passes={passes} n={n} acc={}",
        std::hint::black_box(acc)
    );
    selected("GNITZ_BENCH_SHAPE", &only, n_selected);
}

/// Retired instructions for the per-request decode of an ad-hoc predicate:
/// `from_blob` plus the `resolve_filter` that decodes its const pool. A
/// predicate-only read reaches every worker, so this pair runs once per request
/// per worker.
///
/// `n` is the reachable maximum: an `IN` list over a non-PK integer column stays
/// in the residual, riding one pool entry of 8 bytes per item with no item-count
/// cap below `MAX_READ_SPEC_BYTES`.
///
/// `GNITZ_BENCH_POOL` picks the pool's order, which is the whole point: the two
/// shapes hold the same values and differ only in whether `resolve`'s `is_sorted`
/// guard can answer without sorting, so differencing them prices the guard.
///
/// At the `n` below, differenced over `GNITZ_BENCH_PASSES` 1 and 201:
///
/// | pool shape | instructions per decode + resolve |
/// |------------|----------------------------------:|
/// | ascending  |                           570,030 |
/// | scrambled  |                        46,412,905 |
///
/// The sort dominates whenever it runs, which is what the guard keeps the
/// shipped shape off.
///
/// Build first — `perf stat` around a cold `cargo test` measures the compile,
/// not the bench.
///
///   cargo build -p gnitz-expr --release --tests
///   for s in ascending scrambled; do for p in 1 201; do \
///     GNITZ_BENCH_POOL=$s GNITZ_BENCH_PASSES=$p perf stat -e instructions:u \
///     cargo test -p gnitz-expr --release from_blob_bench -- --ignored --nocapture
///   done; done
#[test]
#[ignore]
fn from_blob_bench() {
    let passes = bench_passes();
    let shape = std::env::var("GNITZ_BENCH_POOL").unwrap_or_else(|_| "ascending".to_string());
    let schema = schema_pk_ints(1, false);
    let n = 262_144usize;
    let set: Vec<i64> = match shape.as_str() {
        // What the client ships: sorted and deduped, so the guard answers off one
        // scan and no sort runs.
        "ascending" => (0..n as i64).collect(),
        // The same values permuted — `n` is a power of two and the multiplier odd,
        // so this is a bijection — leaving order the only difference, and the sort
        // running in full.
        "scrambled" => (0..n).map(|i| (i.wrapping_mul(2_654_435_761) % n) as i64).collect(),
        other => panic!("GNITZ_BENCH_POOL must be ascending/scrambled, got {other:?}"),
    };
    let mut b = ExprBuilder::new();
    let set_idx = b.add_const_int_set(&set);
    let col = b.emit(LogicalInstr::LoadColInt { col: 1 });
    let hit = b.emit(LogicalInstr::IntInSet { value_reg: col, set_idx });
    let blob = b.build(Some(hit)).expect("a well-formed predicate").to_blob_bytes();

    let mut acc = 0usize;
    for _ in 0..passes {
        let prog = LogicalProgram::from_blob(std::hint::black_box(&blob), "bench").expect("decodes");
        acc += prog.resolve_filter(&schema).expect("resolves").prog.int_sets[0].len();
    }
    println!(
        "from_blob_bench shape={shape} passes={passes} n={n} acc={}",
        std::hint::black_box(acc)
    );
}
