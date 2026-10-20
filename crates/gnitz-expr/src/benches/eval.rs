//! Instruction benchmarks for the evaluator's kernels, `#[ignore]`d and
//! meaningful only in release:
//!
//!   cargo test -p gnitz-expr --release <bench> -- --ignored --nocapture --test-threads=1
//!
//! Each case prints the user-mode instructions and cycles per row of one warmed
//! pass, at every instruction set its kernels can run at: `native`, and `avx2`
//! on a CPU that has more, which is what a CPU without it runs. Between levels
//! compare cycles: a wider instruction retires as one whatever it costs to
//! execute. `GNITZ_BENCH_ROWS` sets the rows of an evaluator bench's batch.

use gnitz_foundation::perf::Counter;
use gnitz_wire::{FixedInt, RowSource, TypeCode};

use crate::batch::{encode_f64, MORSEL};
use crate::eval::Resolved;
use crate::program::{FloatUnaryOp, IntUnaryOp};
use crate::simd::{self, Level};
use crate::test_support::{
    filter_prog, filter_verdicts, int, make_n_col_view, make_string_view, map_prog, map_rows, passing_rows, schema_pk,
    schema_pk_ints, schema_pk_strings, simd_levels, view, Cell, Col, TestView,
};
use crate::{
    CalendarOp, CmpOp, ConstIdx, FloatArithOp, IntArithOp, LikePattern, LogicalInstr as L, LogicalInstr,
    LogicalProgram, Reg, RowFilter, SchemaDescriptor, Sink, TrimMode,
};

/// The counters every case is read through.
struct Meter {
    instructions: Counter,
    cycles: Counter,
}

impl Meter {
    fn new() -> Self {
        Meter {
            instructions: Counter::instructions(),
            cycles: Counter::cycles(),
        }
    }

    /// Print `case`'s instructions and cycles per row of `pass`, which covers
    /// `rows` rows, after one pass of warm-up.
    fn pass<T>(&self, bench: &str, case: &str, rows: usize, mut pass: impl FnMut() -> T) {
        std::hint::black_box(pass());
        let ((_, instructions), cycles) = self
            .cycles
            .measure(|| self.instructions.measure(|| std::hint::black_box(pass())));
        println!(
            "{bench} {case:<36} {:8.2} instr/row {:8.2} cycles/row",
            instructions as f64 / rows as f64,
            cycles as f64 / rows as f64
        );
    }

    /// [`Self::pass`] of `ev` at every instruction set of [`simd_levels`], on
    /// the arm it is on.
    fn levels<E: Resolved, T>(
        &self,
        bench: &str,
        case: &str,
        rows: usize,
        ev: &mut E,
        mut pass: impl FnMut(&mut E) -> T,
    ) {
        let no_nulls = ev.no_nulls();
        for (level_name, level) in simd_levels() {
            ev.set_arm(no_nulls, level);
            self.pass(bench, &format!("{case} @{level_name}"), rows, || pass(ev));
        }
        ev.set_arm(no_nulls, Level::new());
    }

    /// [`Self::levels`] of a filter's `ranges` over `view`.
    fn filter(&self, bench: &str, case: &str, f: &mut RowFilter, view: &TestView) {
        let mut ranges = Vec::new();
        self.levels(bench, case, view.row_count(), f, |f| {
            f.ranges(view, &mut ranges);
            ranges.len()
        });
    }
}

/// The rows a batch of the evaluator benches holds, from `GNITZ_BENCH_ROWS`: a
/// count below [`MORSEL`] measures what a small delta pays per batch.
fn bench_rows() -> usize {
    gnitz_foundation::env::env_num("GNITZ_BENCH_ROWS", 200_000)
}

/// `col1 IN (…)` over `set`, which is sorted and distinct.
fn in_set(set: impl IntoIterator<Item = i64>) -> (Vec<LogicalInstr>, Vec<Vec<u8>>) {
    let set: Vec<i64> = set.into_iter().collect();
    let instrs = vec![
        L::LoadCol { col: 1 },
        L::IntInSet { value_reg: Reg(0), set_idx: ConstIdx(0) },
    ];
    (instrs, vec![gnitz_wire::as_le_bytes(&set).to_vec()])
}

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
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn str_const_filter_bench() {
    let meter = Meter::new();
    let schema = schema_pk_strings(1, false);
    let n = 1_000_000usize;
    // (domain, constant, value per row). In `mixed` ~1/16 rows match and every
    // 7th row is a long (heap-backed) string.
    type Domain = (&'static str, &'static str, fn(usize) -> String);
    let domains: [Domain; 5] = [
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
        ("long-distinct", "42_long_string_variant_number", |row| {
            format!("{}_long_string_variant_number", row % 97)
        }),
        ("digits-first", "42abcd", |row| format!("{}abcd", row % 97)),
        ("abcd-shared-prefix", "abcd42", |row| format!("abcd{}", row % 97)),
    ];

    for (domain, constant, value) in domains {
        let mb = make_string_view(&schema, n, |row, _| value(row), |_, _| false);
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
    let pk_view = make_n_col_view(&pk, n, |_, _| 1, |_, _| false);
    let pk_i64 = SchemaDescriptor::new(
        &[
            crate::SchemaColumn::new(TypeCode::I64, false),
            crate::SchemaColumn::new(TypeCode::I64, false),
        ],
        &[0],
    );
    let pk_i64_view = make_n_col_view(&pk_i64, n, |_, _| 1, |_, _| false);

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
    let nn_view = make_n_col_view(
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

    // Five literal comparisons over one column: the `no_nulls` arm's cheapest
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
    let cc_view = make_string_view(
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
/// whole predicate resolves `no_nulls`.
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

/// The `no_nulls` arm against the nullable one for an `IS [NOT] NULL`
/// predicate, and for a null test into a register sink. Read the cycles too:
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
    let spread = make_n_col_view(&schema, n, value, |row, col| col == 0 && row.is_multiple_of(16));
    let clustered = make_n_col_view(&schema, n, value, |row, col| {
        col == 0 && (row / MORSEL).is_multiple_of(8)
    });
    let rare = make_n_col_view(&schema, n, value, |row, col| col == 0 && row.is_multiple_of(64));

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
        assert!(f.no_nulls(), "{name}: must resolve no_nulls");
        // Also the arms' agreement check.
        passing_rows(&mut f, view);
        for (arm, no_nulls) in [("fast", true), ("nullable", false)] {
            f.set_arm(no_nulls, Level::new());
            meter.filter("is_null_arm_bench", &format!("{name}/{arm}"), &mut f, view);
        }
    }

    // The map, which leaves through a register sink rather than a bitmap.
    let out = schema_pk_ints(1, false);
    let mut map = map_prog(&schema, &out, is_null_chain(0, 0), vec![Sink::Reg(Reg(0))], vec![]);
    map_rows(&mut map, &out, &spread);
    let mut dst = TestView::for_schema(&out, n);
    for (arm, no_nulls) in [("fast", true), ("nullable", false)] {
        map.set_arm(no_nulls, Level::new());
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
    use gnitz_wire::{key_image, Cut, KeyRange, PkColList};
    let meter = Meter::new();
    let n = bench_rows();
    let schema = schema_pk_ints(1, true);
    let view = make_n_col_view(&schema, n, |row, _| (row % 1000) as i64, |row, _| row % 16 == 0);
    let range = KeyRange::new(
        PkColList::from_slice(&[1]),
        &[],
        Cut::before(key_image(TypeCode::I64, 100)),
        Cut::after(key_image(TypeCode::I64, 600)),
    );
    let mut f = RowFilter::for_read(&[], Some(&range), &schema).expect("a walk");
    filter_verdicts(&mut f, &view);
    meter.filter("range_walk_bench", "nullable I64 in [100, 600]", &mut f, &view);
}

/// `n` rows of `unit` repeated to `len` bytes, one ASCII byte per row turned
/// into a digit so no matcher hoists out of the row loop.
fn fixed_len_str_view(schema: &SchemaDescriptor, n: usize, len: usize, unit: &[u8]) -> TestView {
    let base: Vec<u8> = unit.iter().copied().cycle().take(len).collect();
    make_string_view(
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
    let result = Reg(instrs.len() as u16 - 1);
    let prog = LogicalProgram::new(instrs, vec![Sink::Reg(result)], consts);
    (name.into(), prog, schema_pk(&[tc], true))
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
            let mut dst = TestView::for_schema(&out, n);
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
    let int_view = make_n_col_view(
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
        slot("int_to_str", vec![c1, L::IntToStr { a: r(0) }], vec![], T::String),
    ]);
    run(&ints, &int_view, shapes);
    drop(int_view);

    let ints_nn = schema_pk_ints(3, false);
    let nn_view = make_n_col_view(&ints_nn, n, |row, col| ((row * 7 + col) % 1000) as i64, |_, _| false);
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
        slot("float_to_str", floats(L::FloatToStr { a: r(1) }), vec![], T::String),
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
    );
    shapes.push(("map".into(), map, schema_pk_ints(2, false)));
    run(&ints_nn, &nn_view, shapes);
    drop(nn_view);

    let strs = schema_pk_strings(2, false);
    let str_view = make_string_view(&strs, n, either_side_of_inline, |_, _| false);
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
    let digit_view = make_string_view(
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
    let shuffled = |row: usize| (((row as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 40) % 1000) as i64;
    let string = |pi: usize, null: fn(usize) -> bool| Col {
        tc: T::String,
        cells: (0..n)
            .map(|row| (!null(row)).then(|| Cell::Str(either_side_of_inline(row, pi).into_bytes())))
            .collect(),
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

/// The null-word permutation of a copy-only map over eight nullable columns: a
/// column prefix, every column moved one slot down, and the columns reversed.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn null_perm_bench() {
    let meter = Meter::new();
    let n = bench_rows();
    let in_schema = schema_pk_ints(8, true);
    let view = make_n_col_view(
        &in_schema,
        n,
        |row, col| (row + col) as i64,
        |row, col| (row * 7 + col * 13) % 5 == 0,
    );
    let shapes: [(&str, Vec<u32>); 3] = [
        ("prefix", (1..=7).collect()),
        ("shift", (2..=8).collect()),
        ("reversed", (1..=8).rev().collect()),
    ];
    for (name, cols) in shapes {
        let out_schema = schema_pk_ints(cols.len(), true);
        let mut map = LogicalProgram::copy_cols(&cols)
            .resolve_map(&in_schema, &out_schema)
            .expect("a copy-only map");
        let mut out = TestView::for_schema(&out_schema, n);
        meter.pass("null_perm_bench", name, n, || {
            map.write_computed(&view, 0, n, &mut out, 0)
        });
    }
}

/// The decode a predicate-only read pays per request per worker: `from_blob`
/// and the `resolve_filter` that decodes an IN list's pool entry, per entry.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn from_blob_bench() {
    let meter = Meter::new();
    let schema = schema_pk_ints(1, false);
    let n = 262_144usize;
    let (instrs, consts) = in_set(0..n as i64);
    let blob = LogicalProgram::new(instrs, vec![Sink::Reg(Reg(1))], consts).to_blob_bytes();
    meter.pass("from_blob_bench", "IN list", n, || {
        let prog = LogicalProgram::from_blob(std::hint::black_box(&blob)).expect("decodes");
        prog.resolve_filter(&schema).expect("resolves")
    });
}

/// [`crate::simd`]'s kernels alone, over one morsel that stays in cache, at
/// each level.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn mask_kernel_bench() {
    use std::hint::black_box;
    /// Passes per measurement: a morsel alone is too few instructions to
    /// outweigh the counter's own.
    const PASSES: usize = 1000;
    let meter = Meter::new();
    let a: Vec<i64> = (0..MORSEL).map(|i| (i * 7919 % 1000) as i64).collect();
    let b = vec![500i64; MORSEL];
    let floats = |v: &[i64]| -> Vec<i64> { v.iter().map(|&x| encode_f64(x as f64)).collect() };
    let (fa, fb) = (floats(&a), floats(&b));
    let nulls: Vec<u8> = (0..MORSEL as u64)
        .flat_map(|i| (i.is_multiple_of(5) as u64 * 2).to_le_bytes())
        .collect();
    let take = [0x5a5a_1234_dead_beef, 7, u64::MAX, 0x0f0f_0f0f_0f0f_0f0f];
    let set: Vec<i64> = (0..32).map(|i| i * 31 + 1).collect();
    let mut bits = [0u64; MORSEL / 64];
    let mut lanes = vec![0i64; MORSEL];

    for (level_name, level) in simd_levels() {
        let (a, b, fa, fb) = (
            black_box(&a[..]),
            black_box(&b[..]),
            black_box(&fa[..]),
            black_box(&fb[..]),
        );
        macro_rules! drive {
            ($name:expr, $kernel:expr) => {
                meter.pass(
                    "mask_kernel_bench",
                    &format!("{} @{level_name}", $name),
                    MORSEL * PASSES,
                    || {
                        for _ in 0..PASSES {
                            $kernel;
                            black_box((&bits, &lanes));
                        }
                    },
                )
            };
        }
        drive!("gt", simd::pred_bits::<simd::GtSigned>(level, a, b, &mut bits));
        drive!(
            "gt_unsigned",
            simd::pred_bits::<simd::GtUnsigned>(level, a, b, &mut bits)
        );
        drive!("gt_float", simd::pred_bits::<simd::GtFloat>(level, fa, fb, &mut bits));
        drive!("ne", simd::pred_bits::<simd::Not<simd::Eq>>(level, a, b, &mut bits));
        drive!("gt_total", simd::pred_bits::<simd::GtTotal>(level, fa, fb, &mut bits));
        drive!("truthy", simd::truthy_bits(level, a, &mut bits));
        for k in [2, 4, 8, 32] {
            drive!(
                format!("in{k}"),
                simd::in_set_bits(level, a, black_box(&set[..k]), &mut bits)
            );
        }
        drive!("nulls", simd::null_bits(level, black_box(&nulls), 2, &mut bits));
        drive!("blend", simd::blend(level, black_box(&take), a, b, &mut lanes));
        drive!("bit_lanes", simd::bit_lanes(level, black_box(&take), &mut lanes));
    }
}
