//! Instruction benchmarks for the evaluator's kernels, through `RowFilter`, `ScalarEval` and `MapEval`.

use gnitz_wire::{FixedInt, TypeCode};

use crate::eval::Resolved;
use crate::morsel::MORSEL;
use crate::program::{FloatUnaryOp, IntUnaryOp};
use crate::simd::Level;
use crate::simd::encode_f64;
use crate::test_support::{
    Meter, TestView, bench_rows, fill, filter_prog, filter_verdicts, in_set, int, map_prog, map_rows, passing_rows,
    result_prog, schema_pk, schema_pk_ints, schema_pk_strings, strings, view,
};
use crate::{
    CalendarOp, CmpOp, ConstIdx, FloatArithOp, IntArithOp, LikePattern, LogicalInstr as L, LogicalInstr,
    LogicalProgram, Reg, RowFilter, SchemaDescriptor, Sink, TrimMode,
};
use gnitz_foundation::rng::mix;

/// `k` values spread over 1..=1000.
fn spread(k: usize) -> impl Iterator<Item = i64> {
    (0..k as i64).map(move |i| i * (1000 / k as i64) + 1)
}

fn k(val: i64) -> LogicalInstr {
    L::LoadConst { val, unsigned: false }
}

/// The fused `StrColConst` against the register channel on `col <op> 'const'`
/// over ~1M NOT NULL strings, per domain. `digits-first` and
/// `abcd-shared-prefix` differ only in whether the prefix the fused compare
/// short-circuits on collides; `long` and `long-distinct` only in whether the
/// heap strings share theirs.
///
/// `col LIKE 'prefix%'` runs the same domains through the fused `StrColPrefix`
/// and through the matcher over a string register, and `col LIKE 'const'`
/// through the matcher, which the planner lowers to the fused `eq` instead.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn str_const_filter_bench() {
    let meter = Meter::new();
    let schema = schema_pk_strings(1, false);
    let n = 1_000_000usize;
    // (domain, constant, LIKE prefix, value per row). In `mixed` ~1/16 rows
    // match and every 7th row is a long (heap-backed) string.
    type Domain = (&'static str, &'static str, &'static str, fn(usize) -> String);
    let domains: [Domain; 5] = [
        ("mixed", "match_target", "match", |row| {
            if row % 16 == 0 {
                "match_target".to_string()
            } else if row % 7 == 0 {
                format!("long_string_variant_number_{row}")
            } else {
                format!("k{}", row % 97)
            }
        }),
        (
            "long",
            "long_string_variant_number_42",
            "long_string_variant_number_4",
            |row| format!("long_string_variant_number_{}", row % 97),
        ),
        // The controlled pair: `{i}abcd` and `abcd{i}` hold the same bytes at the
        // same lengths, so the prefix is the only thing that differs.
        ("long-distinct", "42_long_string_variant_number", "42_long", |row| {
            format!("{}_long_string_variant_number", row % 97)
        }),
        ("digits-first", "42abcd", "42", |row| format!("{}abcd", row % 97)),
        ("abcd-shared-prefix", "abcd42", "abcd4", |row| {
            format!("abcd{}", row % 97)
        }),
    ];

    for (domain, constant, prefix, value) in domains {
        let mb = fill(&schema, n, |row, _| value(row), |_, _| false);
        for (name, op) in [("eq", CmpOp::Eq), ("ne", CmpOp::Ne), ("lt", CmpOp::Lt)] {
            let consts = vec![constant.as_bytes().to_vec()];
            let mut fused = filter_prog(
                &schema,
                vec![L::StrColConst { op, col: 1, const_idx: ConstIdx(0) }],
                consts.clone(),
            );
            let mut regs = filter_prog(
                &schema,
                vec![
                    L::LoadColStr { col: 1 },
                    L::LoadConstStr { const_idx: ConstIdx(0) },
                    L::StrCmp { op, a: Reg(0), b: Reg(1) },
                ],
                consts,
            );
            assert_eq!(
                passing_rows(&mut fused, &mb),
                passing_rows(&mut regs, &mb),
                "{domain}/{name}: the channels disagree"
            );
            for (chan, ev) in [("fused", &mut fused), ("registers", &mut regs)] {
                meter.filter("str_const_filter_bench", &format!("{domain}/{name}/{chan}"), ev, &mb);
            }
        }
        let matcher = |pattern: String| {
            let pattern = LikePattern::encode(&pattern, None).unwrap().as_bytes().to_vec();
            filter_prog(
                &schema,
                vec![
                    L::LoadColStr { col: 1 },
                    L::StrLike {
                        src: Reg(0),
                        pat_idx: ConstIdx(0),
                        ci: false,
                    },
                ],
                vec![pattern],
            )
        };
        let mut fused = filter_prog(
            &schema,
            vec![L::StrColPrefix { col: 1, const_idx: ConstIdx(0) }],
            vec![prefix.as_bytes().to_vec()],
        );
        let mut regs = matcher(format!("{prefix}%"));
        assert_eq!(
            passing_rows(&mut fused, &mb),
            passing_rows(&mut regs, &mb),
            "{domain}/like-prefix: the channels disagree"
        );
        let mut exact = matcher(constant.to_string());
        for (chan, ev) in [
            ("like-prefix/fused", &mut fused),
            ("like-prefix/registers", &mut regs),
            ("like-exact/registers", &mut exact),
        ] {
            meter.filter("str_const_filter_bench", &format!("{domain}/{chan}"), ev, &mb);
        }
    }
}

/// The filter kernels.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn filter_kernel_bench() {
    let meter = Meter::new();
    let n = bench_rows();
    let cmp = |col, op, val| vec![L::LoadCol { col }, k(val), L::Cmp { op, a: Reg(0), b: Reg(1) }];

    // `pk > n/2` over a `U64` key, and over an `I64` one: the signed OPK decode.
    let pk = schema_pk_ints(1, false);
    let pk_view = fill(&pk, n, |_, _| 1i64, |_, _| false);
    let pk_i64 = SchemaDescriptor::new(
        &[
            crate::SchemaColumn::new(TypeCode::I64, false),
            crate::SchemaColumn::new(TypeCode::I64, false),
        ],
        &[0],
    );
    let pk_i64_view = fill(&pk_i64, n, |_, _| 1i64, |_, _| false);

    // One NOT NULL column holding `row % 1000`: as an `I32`, a payload load
    // narrower than the 8-byte arm every other shape reads; as an `F32`, the
    // widening load; as an `I64`, the literal shapes.
    let thousands = |tc, cell: fn(usize) -> i64| view(&[int(tc, (0..n).map(cell))]);
    let (i32_schema, i32_view) = thousands(TypeCode::I32, |row| (row % 1000) as i64);
    let (f32_schema, f32_view) = thousands(TypeCode::F32, |row| ((row % 1000) as f32).to_bits() as i64);
    let (lit_schema, lit_view) = thousands(TypeCode::I64, |row| (row % 1000) as i64);

    // `-a > 0 AND b < 80` over nullable columns — unary, the 3VL AND, and the
    // per-column null-bit gather.
    let nn = schema_pk_ints(2, true);
    let nn_view = fill(
        &nn,
        n,
        |row, col| ((row * 7 + col) % 100) as i64,
        |row, _| row % 32 == 0,
    );
    let nn_instrs = vec![
        L::LoadCol { col: 1 },
        L::IntUnary { op: IntUnaryOp::Neg, a: Reg(0) },
        k(0),
        L::Cmp { op: CmpOp::Gt, a: Reg(1), b: Reg(2) },
        L::LoadCol { col: 2 },
        k(80),
        L::Cmp { op: CmpOp::Lt, a: Reg(4), b: Reg(5) },
        L::BoolBinary { is_or: false, a: Reg(3), b: Reg(6) },
    ];

    // Five literal comparisons over one column: the untracked-null arm's cheapest
    // per-row work carrying the most constant registers, which is where a
    // per-morsel constant refill would show.
    let mut lit_instrs = vec![L::LoadCol { col: 1 }];
    for (i, (op, val)) in [
        (CmpOp::Gt, 1),
        (CmpOp::Lt, 999),
        (CmpOp::Ne, 5),
        (CmpOp::Ne, 7),
        (CmpOp::Ne, 9),
    ]
    .into_iter()
    .enumerate()
    {
        let acc = Reg(lit_instrs.len() as u16 - 1);
        lit_instrs.push(k(val));
        let c = Reg(lit_instrs.len() as u16);
        lit_instrs.push(L::Cmp { op, a: Reg(0), b: Reg(c.0 - 1) });
        if i > 0 {
            lit_instrs.push(L::BoolBinary { is_or: false, a: acc, b: c });
        }
    }
    let mut not_instrs = cmp(1, CmpOp::Gt, 500);
    not_instrs.push(L::BoolNot { a: Reg(2) });

    // `s1 = s2` / `s1 < s2` over two NOT NULL string columns: short strings that
    // agree on every third row, and long ones sharing a prefix on every seventh.
    let cc = schema_pk_strings(2, false);
    let cc_view = fill(
        &cc,
        n,
        |row, pi| match (row % 7, row % 3) {
            (0, _) => format!("long_string_variant_number_{}", (row + pi * (row % 2)) % 97),
            (_, 0) => format!("k{}", row % 97),
            _ => format!("k{}", (row + pi) % 97),
        },
        |_, _| false,
    );
    let col_col = |op| vec![L::StrColCol { op, col_a: 1, col_b: 2 }];
    let f32_gt = vec![
        L::LoadCol { col: 1 },
        k(encode_f64(500.0)),
        L::FCmp { op: CmpOp::Gt, a: Reg(0), b: Reg(1) },
    ];

    let mut shapes: Vec<(String, RowFilter, &TestView)> = vec![
        (
            "colcol_eq".into(),
            filter_prog(&cc, col_col(CmpOp::Eq), vec![]),
            &cc_view,
        ),
        (
            "colcol_lt".into(),
            filter_prog(&cc, col_col(CmpOp::Lt), vec![]),
            &cc_view,
        ),
        (
            "pk".into(),
            filter_prog(&pk, cmp(0, CmpOp::Gt, (n / 2) as i64), vec![]),
            &pk_view,
        ),
        (
            "pk_i64".into(),
            filter_prog(&pk_i64, cmp(0, CmpOp::Gt, (n / 2) as i64), vec![]),
            &pk_i64_view,
        ),
        (
            "i32".into(),
            filter_prog(&i32_schema, cmp(1, CmpOp::Gt, 500), vec![]),
            &i32_view,
        ),
        ("f32".into(), filter_prog(&f32_schema, f32_gt, vec![]), &f32_view),
        ("nullable".into(), filter_prog(&nn, nn_instrs, vec![]), &nn_view),
        (
            "literals".into(),
            filter_prog(&lit_schema, lit_instrs, vec![]),
            &lit_view,
        ),
        ("not".into(), filter_prog(&lit_schema, not_instrs, vec![]), &lit_view),
    ];
    // The list sized either side of where the kernel stops scanning it.
    for k in [2, 4, 8, 16, 32, 64, 128, 1000] {
        let (instrs, consts) = in_set(spread(k));
        shapes.push((format!("in{k}"), filter_prog(&lit_schema, instrs, consts), &lit_view));
    }
    for (name, mut f, view) in shapes {
        meter.filter("filter_kernel_bench", &name, &mut f, view);
    }
}

/// `col1 IS NULL AND col2 > k AND …` over `n_cmp` NOT NULL columns, so the
/// whole predicate resolves no nullable register.
fn is_null_chain(bound: i64, n_cmp: u32) -> Vec<LogicalInstr> {
    let mut instrs = vec![L::IsNull { col: 1, invert: false }];
    if n_cmp == 0 {
        // No compare, so no constant to load — an unread `LoadConst` would still
        // cost a register write per morsel and blunt the bare shape's figure.
        return instrs;
    }
    instrs.push(k(bound));
    for col in 2..n_cmp + 2 {
        let acc = Reg(if col == 2 { 0 } else { instrs.len() as u16 - 1 });
        instrs.push(L::LoadCol { col });
        let loaded = Reg(instrs.len() as u16 - 1);
        instrs.push(L::Cmp { op: CmpOp::Gt, a: loaded, b: Reg(1) });
        instrs.push(L::BoolBinary {
            is_or: false,
            a: acc,
            b: Reg(loaded.0 + 1),
        });
    }
    instrs
}

/// No register's null words tracked against every register's, for an
/// `IS [NOT] NULL` predicate, and for a null test into a register sink. Read the cycles too:
/// only they see a stall.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn is_null_arm_bench() {
    let meter = Meter::new();
    let n = bench_rows();
    // One nullable column for the null test, four NOT NULL ones for the compares.
    let mut cols = vec![
        crate::SchemaColumn::new(TypeCode::U64, false),
        crate::SchemaColumn::new(TypeCode::I64, true),
    ];
    cols.extend(std::iter::repeat_n(crate::SchemaColumn::new(TypeCode::I64, false), 4));
    let schema = SchemaDescriptor::new(&cols, &[0]);

    // NULLs 16 per morsel, 4 per morsel, and none in 7 of every 8 morsels.
    let value = |row: usize, col: usize| ((row * 7 + col * 13) % 100) as i64;
    let spread = fill(&schema, n, value, |row, col| col == 0 && row.is_multiple_of(16));
    let clustered = fill(&schema, n, value, |row, col| {
        col == 0 && (row / MORSEL).is_multiple_of(8)
    });
    let rare = fill(&schema, n, value, |row, col| col == 0 && row.is_multiple_of(64));

    // `k = 50` passes about half the rows; `k = -1` passes every row, which is
    // the non-selective variant.
    let shapes: [(&str, &TestView, Vec<LogicalInstr>); 6] = [
        ("bare", &spread, is_null_chain(50, 0)),
        ("one_and", &spread, is_null_chain(50, 1)),
        ("chain_spread", &spread, is_null_chain(50, 4)),
        ("chain_clustered", &clustered, is_null_chain(50, 4)),
        ("chain_nonselective", &spread, is_null_chain(-1, 4)),
        ("chain_rare", &rare, is_null_chain(50, 4)),
    ];
    for (name, view, instrs) in shapes {
        let mut f = filter_prog(&schema, instrs, vec![]);
        assert_eq!(f.null_regs(), 0, "{name}: must resolve no nullable register");
        // Also the arms' agreement check.
        passing_rows(&mut f, view);
        for (arm, null_regs) in [("fast", 0), ("nullable", u64::MAX)] {
            f.set_arm(null_regs, Level::new());
            meter.filter("is_null_arm_bench", &format!("{name}/{arm}"), &mut f, view);
        }
    }

    // The map, which leaves through a register sink rather than a bitmap.
    let out = schema_pk_ints(1, false);
    let mut map = map_prog(&schema, &out, is_null_chain(0, 0), vec![Sink::Reg(Reg(0))], vec![]);
    map_rows(&mut map, &out, &spread);
    let mut dst = TestView::new(&out, n);
    for (arm, null_regs) in [("fast", 0), ("nullable", u64::MAX)] {
        map.set_arm(null_regs, Level::new());
        meter.levels("is_null_arm_bench", &format!("map/{arm}"), n, &mut map, |m| {
            m.write_computed(&spread, 0, n, &mut dst, 0)
        });
    }
}

/// A range walk over one nullable `I64` payload column and no predicate: the
/// null exclusion and the range compare of `RangeMembership`.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn range_walk_bench() {
    use gnitz_wire::{KeyRange, PkColList, key_image};
    let meter = Meter::new();
    let n = bench_rows();
    let schema = schema_pk_ints(1, true);
    let view = fill(&schema, n, |row, _| (row % 1000) as i64, |row, _| row % 16 == 0);
    let range = KeyRange::new(
        PkColList::from_slice(&[1]),
        &[],
        key_image(TypeCode::I64, 100),
        key_image(TypeCode::I64, 600),
    );
    let mut f = RowFilter::for_read(&[], Some(&range), &schema).expect("a walk");
    filter_verdicts(&mut f, &view);
    meter.filter("range_walk_bench", "nullable I64 in [100, 600]", &mut f, &view);
}

/// `n` rows of `unit` repeated to `len` bytes, one ASCII byte per row turned
/// into a digit so no matcher hoists out of the row loop.
fn fixed_len_str_view(schema: &SchemaDescriptor, n: usize, len: usize, unit: &[u8]) -> TestView {
    let base: Vec<u8> = unit.iter().copied().cycle().take(len).collect();
    fill(
        schema,
        n,
        |row, _| {
            let mut s = base.clone();
            if s[row % len].is_ascii() {
                s[row % len] = b'0' + (row % 10) as u8;
            }
            s
        },
        |_, _| false,
    )
}

/// One string per row of payload slot `pi`, alternating either side of the
/// 12-byte inline boundary so a string kernel reads both the inline view and the
/// blob view.
fn either_side_of_inline(row: usize, pi: usize) -> String {
    match row % 3 {
        0 => format!("row-{row}-col-{pi}-past-the-inline-boundary"),
        _ => format!("r{}{pi}", row % 100),
    }
}

/// A computed column, as a projection writes it: a map whose one register
/// sink, `instrs`' last, fills a `tc` slot.
type Shape = (String, LogicalProgram, SchemaDescriptor);

fn slot(name: impl Into<String>, instrs: Vec<LogicalInstr>, consts: Vec<Vec<u8>>, tc: TypeCode) -> Shape {
    (name.into(), result_prog(instrs, consts), schema_pk(&[tc], true))
}

/// The kernels behind a computed column, each written through
/// `write_computed` as a maintained view's projection runs it. Views are built
/// one at a time, its shapes driven, then dropped; a numeric suffix is the
/// string length a family's cost is a slope in.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn expr_kernel_bench() {
    use TypeCode as T;
    let meter = Meter::new();
    let n = bench_rows();
    let run = |schema: &SchemaDescriptor, view: &TestView, shapes: Vec<Shape>| {
        for (name, prog, out) in shapes {
            let mut ev = prog.resolve_map(schema, &out).unwrap_or_else(|e| panic!("{name}: {e}"));
            let mut dst = TestView::new(&out, n);
            meter.levels("expr_kernel_bench", &name, n, &mut ev, |ev| {
                dst.clear_blob();
                ev.write_computed(view, 0, n, &mut dst, 0)
            });
        }
    };
    let (c1, c2) = (L::LoadCol { col: 1 }, L::LoadCol { col: 2 });
    let (s1, s2) = (L::LoadColStr { col: 1 }, L::LoadColStr { col: 2 });
    let r = Reg;
    let pattern = |p: &str| vec![LikePattern::encode(p, None).unwrap().as_bytes().to_vec()];

    // The shapes over two integer columns, once nullable and once NOT NULL: the
    // blend, the extremum and the compare on either arm, and `GREATEST` over
    // floats, which orders by `total_cmp`.
    let pairs = |suffix: &str| {
        vec![
            slot(
                format!("select{suffix}"),
                vec![
                    c1,
                    c2,
                    L::Cmp { op: CmpOp::Gt, a: r(0), b: r(1) },
                    L::Select { cond: r(2), a: r(0), b: r(1) },
                ],
                vec![],
                T::I64,
            ),
            slot(
                format!("minmax{suffix}"),
                vec![c1, c2, L::IntMinMax2 { a: r(0), b: r(1), is_max: true }],
                vec![],
                T::I64,
            ),
            slot(
                format!("cmp_value{suffix}"),
                vec![c1, c2, L::Cmp { op: CmpOp::Gt, a: r(0), b: r(1) }],
                vec![],
                T::Bool,
            ),
            slot(
                format!("fminmax{suffix}"),
                vec![
                    c1,
                    c2,
                    L::IntToFloat { a: r(0) },
                    L::IntToFloat { a: r(1) },
                    L::FloatMinMax2 { a: r(2), b: r(3), is_max: true },
                ],
                vec![],
                T::F64,
            ),
        ]
    };

    let ints = schema_pk_ints(2, true);
    let int_view = fill(
        &ints,
        n,
        |row, col| ((row * 7 + col) % 1000 + 1) as i64,
        |row, _| row % 32 == 0,
    );
    let mut shapes = pairs("");
    shapes.extend([
        slot(
            "int_cast",
            vec![c1, L::IntCast { a: r(0), fi: FixedInt::I32 }],
            vec![],
            T::I32,
        ),
        slot(
            "int_div",
            vec![c1, k(7), L::IntArith { op: IntArithOp::Div, a: r(0), b: r(1) }],
            vec![],
            T::I64,
        ),
        slot(
            "int_to_str",
            vec![c1, L::IntToStr { a: r(0), scale: 0 }],
            vec![],
            T::String,
        ),
        // Each to-text spelling over a spread of values: a DECIMAL at scale 2,
        // a day count, and a microsecond count with a fraction.
        slot(
            "dec2_to_str",
            vec![
                c1,
                k(123_457),
                L::IntArith { op: IntArithOp::Mul, a: r(0), b: r(1) },
                L::IntToStr { a: r(2), scale: 2 },
            ],
            vec![],
            T::String,
        ),
        slot(
            "date_to_str",
            vec![
                c1,
                k(37),
                L::IntArith { op: IntArithOp::Mul, a: r(0), b: r(1) },
                L::TemporalToStr { a: r(2), micros: false },
            ],
            vec![],
            T::String,
        ),
        slot(
            "ts_to_str",
            vec![
                c1,
                k(86_400_000_123),
                L::IntArith { op: IntArithOp::Mul, a: r(0), b: r(1) },
                L::TemporalToStr { a: r(2), micros: true },
            ],
            vec![],
            T::String,
        ),
    ]);
    run(&ints, &int_view, shapes);
    drop(int_view);

    let ints_nn = schema_pk_ints(3, false);
    let nn_view = fill(&ints_nn, n, |row, col| ((row * 7 + col) % 1000) as i64, |_, _| false);
    let floats = |op: L| vec![c1, L::IntToFloat { a: r(0) }, op];
    let cal = |op, micros: bool| {
        let mut instrs = vec![c1];
        if micros {
            instrs.extend([k(86_400_000_123), L::IntArith { op: IntArithOp::Mul, a: r(0), b: r(1) }]);
        }
        instrs.push(L::Calendar {
            op,
            a: Reg(instrs.len() as u16 - 1),
            micros,
        });
        instrs
    };
    let mut shapes = pairs("_nn");
    for k in [2, 8, 64] {
        let (instrs, consts) = in_set(spread(k));
        shapes.push(slot(format!("in{k}_value"), instrs, consts, T::Bool));
    }
    shapes.extend([
        slot("cal_year_date", cal(CalendarOp::Year, false), vec![], T::I64),
        slot("cal_year_ts", cal(CalendarOp::Year, true), vec![], T::I64),
        slot("cal_hour_ts", cal(CalendarOp::Hour, true), vec![], T::I64),
        slot("cal_trunc_hour_ts", cal(CalendarOp::TruncHour, true), vec![], T::I64),
        slot("cal_trunc_month_ts", cal(CalendarOp::TruncMonth, true), vec![], T::I64),
        slot("cal_to_days", cal(CalendarOp::ToDays, true), vec![], T::I64),
        slot(
            "float_mul",
            vec![
                c1,
                c2,
                L::IntToFloat { a: r(0) },
                L::IntToFloat { a: r(1) },
                L::FloatArith { op: FloatArithOp::Mul, a: r(2), b: r(3) },
            ],
            vec![],
            T::F64,
        ),
        slot(
            "float_round",
            floats(L::FloatUnary { op: FloatUnaryOp::Round, a: r(1) }),
            vec![],
            T::F64,
        ),
        slot(
            "float_to_int",
            floats(L::FloatToInt { a: r(1), fi: FixedInt::I32 }),
            vec![],
            T::I64,
        ),
        slot(
            "float_to_str",
            floats(L::FloatToStr { a: r(1), f32: false }),
            vec![],
            T::String,
        ),
    ]);
    // A real map: six compute opcodes and two register sinks.
    let map = LogicalProgram::new(
        vec![
            c1,
            c2,
            L::LoadCol { col: 3 },
            L::IntArith { op: IntArithOp::Add, a: r(0), b: r(1) },
            L::IntArith { op: IntArithOp::Mul, a: r(3), b: r(2) },
            L::IntArith { op: IntArithOp::Sub, a: r(4), b: r(0) },
        ],
        vec![Sink::Reg(r(5)), Sink::Reg(r(3))],
        vec![],
    )
    .unwrap();
    shapes.push(("map".into(), map, schema_pk_ints(2, false)));
    run(&ints_nn, &nn_view, shapes);
    drop(nn_view);

    let strs = schema_pk_strings(2, false);
    let str_view = fill(&strs, n, either_side_of_inline, |_, _| false);
    run(
        &strs,
        &str_view,
        vec![
            slot("str_len", vec![s1, L::StrLen { a: r(0), chars: false }], vec![], T::I64),
            slot(
                "str_like",
                vec![
                    s1,
                    L::StrLike {
                        src: r(0),
                        pat_idx: ConstIdx(0),
                        ci: false,
                    },
                ],
                pattern("%boundary"),
                T::Bool,
            ),
            slot(
                "str_upper",
                vec![s1, L::StrCase { a: r(0), upper: true }],
                vec![],
                T::String,
            ),
            slot(
                "str_substr",
                vec![
                    s1,
                    k(2),
                    k(6),
                    L::StrSubstr {
                        src: r(0),
                        start_reg: r(1),
                        len_reg: Some(r(2)),
                    },
                ],
                vec![],
                T::String,
            ),
            slot(
                "str_concat",
                vec![s1, s2, L::StrConcat { a: r(0), b: r(1), skip_null: false }],
                vec![],
                T::String,
            ),
            slot(
                "str_trim",
                vec![
                    s1,
                    L::StrTrim {
                        a: r(0),
                        mode: TrimMode::Both,
                        set_idx: ConstIdx(0),
                    },
                ],
                vec![b"r0".to_vec()],
                T::String,
            ),
            slot(
                "str_replace",
                vec![
                    s1,
                    L::LoadConstStr { const_idx: ConstIdx(0) },
                    L::LoadConstStr { const_idx: ConstIdx(1) },
                    L::StrReplace { s: r(0), from: r(1), to: r(2) },
                ],
                vec![b"-".to_vec(), b"--".to_vec()],
                T::String,
            ),
            slot(
                "str_split_part",
                vec![
                    s1,
                    L::LoadConstStr { const_idx: ConstIdx(0) },
                    k(2),
                    L::StrSplitPart { s: r(0), delim: r(1), n_reg: r(2) },
                ],
                vec![b"-".to_vec()],
                T::String,
            ),
        ],
    );
    drop(str_view);

    // Decimal text: an integer, and a float with a fraction and an exponent.
    let digits = schema_pk_strings(2, false);
    let digit_view = fill(
        &digits,
        n,
        |row, pi| match pi {
            0 => format!("{}", (row as i64 * 7919) % 2_000_003 - 1_000_000),
            _ => format!("{}.{}e{}", row % 1000, row % 97, row % 7),
        },
        |_, _| false,
    );
    run(
        &digits,
        &digit_view,
        vec![
            slot(
                "str_to_int",
                vec![s1, L::StrToInt { a: r(0), fi: FixedInt::I64 }],
                vec![],
                T::I64,
            ),
            slot("str_to_float", vec![s2, L::StrToFloat { a: r(0) }], vec![], T::F64),
        ],
    );
    drop(digit_view);

    // `CASE WHEN k > 500 THEN s1 ELSE s2 END`: a condition no branch predictor
    // learns, and a NULL in the first branch so the blend runs on the nullable
    // arm.
    let shuffled = |row: usize| ((mix(row as u64) >> 40) % 1000) as i64;
    let string = |pi: usize, null: fn(usize) -> bool| {
        strings((0..n).map(|row| (!null(row)).then(|| either_side_of_inline(row, pi).into_bytes())))
    };
    let (sel, sel_view) = view(&[
        int(T::I64, (0..n).map(shuffled)),
        string(1, |row| row % 32 == 0),
        string(2, |_| false),
    ]);
    run(
        &sel,
        &sel_view,
        vec![slot(
            "str_select",
            vec![
                c1,
                k(500),
                L::Cmp { op: CmpOp::Gt, a: r(0), b: r(1) },
                L::LoadColStr { col: 2 },
                L::LoadColStr { col: 3 },
                L::StrSelect { cond: r(2), a: r(3), b: r(4) },
            ],
            vec![],
            T::String,
        )],
    );
    drop(sel_view);

    // One string column of `unit` repeated, so the view is the fixture and the
    // program is the shape.
    let str1 = schema_pk_strings(1, false);
    let like = |name: &str, len: usize, p: &str, ci: bool| {
        slot(
            format!("{name}_{len}"),
            vec![s1, L::StrLike { src: r(0), pat_idx: ConstIdx(0), ci }],
            pattern(p),
            T::Bool,
        )
    };
    let contains = |len| like("str_contains", len, "%needle%", false);
    let generic = |len| like("str_generic", len, "%a%b%", false);
    let chars = |len| {
        slot(
            format!("str_chars_{len}"),
            vec![s1, L::StrLen { a: r(0), chars: true }],
            vec![],
            T::I64,
        )
    };
    let reverse = |name: &str| slot(name, vec![s1, L::StrReverse { a: r(0) }], vec![], T::String);
    // RIGHT is the one kernel whose walk is from the far end; a window 200
    // characters in is where the offset walks skip whole words.
    let side = |name: &str, n: i64, left: bool| {
        slot(
            name,
            vec![s1, k(n), L::StrSide { src: r(0), n_reg: r(1), left }],
            vec![],
            T::String,
        )
    };
    let strpos = |len| {
        slot(
            format!("str_strpos_{len}"),
            vec![
                s1,
                L::LoadConstStr { const_idx: ConstIdx(0) },
                L::StrPos { hay: r(0), needle: r(1) },
            ],
            vec![b"needle".to_vec()],
            T::I64,
        )
    };
    let utf8 = "aéb".as_bytes();
    let fixtures: Vec<(&[u8], usize, Vec<Shape>)> = vec![
        (
            b"x",
            12,
            vec![
                contains(12),
                generic(12),
                slot(
                    "str_lpad_12",
                    vec![
                        s1,
                        k(40),
                        L::LoadConstStr { const_idx: ConstIdx(0) },
                        L::StrPad {
                            s: r(0),
                            n_reg: r(1),
                            fill: r(2),
                            left: true,
                        },
                    ],
                    vec![b" ".to_vec()],
                    T::String,
                ),
            ],
        ),
        (b"x", 64, vec![side("str_side_64", 10, false)]),
        (b"x", 128, vec![contains(128), generic(128), reverse("str_reverse_128")]),
        (
            b"x",
            512,
            vec![contains(512), generic(512), side("str_side_512", 10, false)],
        ),
        (utf8, 12, vec![chars(12)]),
        // A `%` ahead of `_`s.
        (
            utf8,
            128,
            vec![
                chars(128),
                reverse("str_reverse_utf8_128"),
                like("str_like_any3", 128, "%___", false),
            ],
        ),
        (
            utf8,
            512,
            vec![
                chars(512),
                side("str_left200", 200, true),
                side("str_right200", 200, false),
                slot(
                    "str_substr_far",
                    vec![
                        s1,
                        k(200),
                        k(10),
                        L::StrSubstr {
                            src: r(0),
                            start_reg: r(1),
                            len_reg: Some(r(2)),
                        },
                    ],
                    vec![],
                    T::String,
                ),
            ],
        ),
        // A frequent first needle byte.
        (
            b"xxxxxxxn",
            128,
            vec![like("str_contains_freq", 128, "%needle%", false), strpos(128)],
        ),
        (
            b"xxxxxxxn",
            512,
            vec![like("str_contains_freq", 512, "%needle%", false), strpos(512)],
        ),
        (b"xxxNxxxn", 12, vec![like("str_icontains", 12, "%NeedLe%", true)]),
        (b"xxxNxxxn", 128, vec![like("str_iprefix", 128, "NeedLe%", true)]),
        (b"xxxNxxxn", 512, vec![like("str_icontains", 512, "%NeedLe%", true)]),
        // Dense false candidates, so the generic walk resumes on each.
        (b"nexn", 128, vec![like("str_generic_resume", 128, "%n_q%", false)]),
        (b"nexn", 512, vec![like("str_generic_resume", 512, "%n_q%", false)]),
    ];
    for (unit, len, shapes) in fixtures {
        run(&str1, &fixed_len_str_view(&str1, n, len, unit), shapes);
    }
}
