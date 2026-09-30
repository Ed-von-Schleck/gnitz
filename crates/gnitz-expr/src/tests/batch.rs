use crate::{ConstIdx, FloatArithOp, IntArithOp, Reg, TrimMode};
use gnitz_wire::{FixedInt, TypeCode};

use super::{decode_f64, encode_f64, eval_batch, scan_filter_bits, with_str_bufs, EvalScratch, MORSEL};
use crate::eval::Resolved;
use crate::program::{FloatUnaryOp, IntUnaryOp};
use crate::test_support::{
    filter_prog, make_string_view, passing_rows, row_strs, row_values, runs, scalar_prog, schema_pk_strings,
    TestSchema, TestView,
};
use crate::{CalendarOp, CmpOp, LogicalInstr, ScalarEval};

// ---------------------------------------------------------------------------
// The harness: one U64 PK plus the operand columns a program loads in order
// ---------------------------------------------------------------------------

/// One operand column: its type and each row's cell, `None` for NULL.
#[derive(Clone)]
struct Col {
    tc: TypeCode,
    cells: Vec<Option<Cell>>,
}

/// An integer, a float's `f64` bit pattern, or a string.
#[derive(Clone)]
enum Cell {
    Num(i64),
    Str(Vec<u8>),
}

impl Col {
    fn nullable(&self) -> bool {
        self.cells.iter().any(Option::is_none)
    }

    /// What a NULL cell stores: a value no live row holds, truthy and past the
    /// string inline threshold, so a kernel that read it shows in its result.
    fn poison(&self) -> Cell {
        match self.tc {
            TypeCode::String => Cell::Str(b"poison-in-a-null-cell".to_vec()),
            _ => Cell::Num(0x2A2A_2A2A_2A2A_2A2A),
        }
    }

    fn load(&self, col: u32) -> LogicalInstr {
        match self.tc {
            TypeCode::String => LogicalInstr::LoadColStr { col },
            TypeCode::F32 | TypeCode::F64 => LogicalInstr::LoadColFloat { col },
            _ => LogicalInstr::LoadColInt { col },
        }
    }
}

fn int<T: Into<Option<i64>>>(tc: TypeCode, vals: impl IntoIterator<Item = T>) -> Col {
    let cells = vals.into_iter().map(|v| v.into().map(Cell::Num)).collect();
    Col { tc, cells }
}

fn i64s<T: Into<Option<i64>>>(vals: impl IntoIterator<Item = T>) -> Col {
    int(TypeCode::I64, vals)
}

fn f64s<T: Into<Option<f64>>>(vals: impl IntoIterator<Item = T>) -> Col {
    int(TypeCode::F64, vals.into_iter().map(|v| v.into().map(encode_f64)))
}

fn text<'a, T: Into<Option<&'a str>>>(vals: impl IntoIterator<Item = T>) -> Col {
    let cells = vals
        .into_iter()
        .map(|v| v.into().map(|s| Cell::Str(s.as_bytes().to_vec())))
        .collect();
    Col { tc: TypeCode::String, cells }
}

/// A program loading each of `cols` into its own register, in order, then `mk`'s
/// instructions over those registers; the last is the result.
fn prog(cols: &[Col], consts: Vec<Vec<u8>>, mk: impl FnOnce(&[Reg]) -> Vec<LogicalInstr>) -> (ScalarEval, TestView) {
    let mut schema_cols = vec![(TypeCode::U64, false)];
    schema_cols.extend(cols.iter().map(|c| (c.tc, c.nullable())));
    let schema = TestSchema::new(&schema_cols, &[0]);
    let mut view = TestView::for_schema(&schema, cols[0].cells.len());
    for (pi, c) in cols.iter().enumerate() {
        for (row, cell) in c.cells.iter().enumerate() {
            match cell.clone().unwrap_or_else(|| c.poison()) {
                Cell::Num(x) => view.set_int(row, pi, x),
                Cell::Str(s) => view.set_string(row, pi, &s),
            }
            if cell.is_none() {
                view.set_null(row, pi);
            }
        }
    }
    let mut instrs: Vec<LogicalInstr> = cols.iter().zip(1..).map(|(c, col)| c.load(col)).collect();
    let regs: Vec<Reg> = (0..cols.len() as u16).map(Reg).collect();
    instrs.extend(mk(&regs));
    (scalar_prog(&schema, instrs, consts), view)
}

/// Every row's result of an integer-valued [`prog`], as the register's `i64`
/// image — a float's bit pattern.
fn ints(cols: &[Col], mk: impl FnOnce(&[Reg]) -> Vec<LogicalInstr>) -> Vec<Option<i64>> {
    let (mut ev, view) = prog(cols, vec![], mk);
    int_rows(&mut ev, &view)
}

/// Every row's result of a string-valued [`prog`], as text.
fn strs(cols: &[Col], mk: impl FnOnce(&[Reg]) -> Vec<LogicalInstr>) -> Vec<Option<String>> {
    let (mut ev, view) = prog(cols, vec![], mk);
    text_rows(&mut ev, &view)
}

fn int_rows(ev: &mut ScalarEval, view: &TestView) -> Vec<Option<i64>> {
    row_values(ev, view).into_iter().map(|v| v.map(|x| x as i64)).collect()
}

fn text_rows(ev: &mut ScalarEval, view: &TestView) -> Vec<Option<String>> {
    row_strs(ev, view)
        .into_iter()
        .map(|s| s.map(|b| String::from_utf8(b).expect("a string result is UTF-8")))
        .collect()
}

/// `rows` as [`text_rows`] reports them.
fn texts(rows: &[Option<&str>]) -> Vec<Option<String>> {
    rows.iter().map(|r| r.map(String::from)).collect()
}

/// Every ordered pair of `corpus`, as the two operand columns.
fn pairs<T: Copy>(corpus: &[T]) -> (Vec<T>, Vec<T>) {
    corpus.iter().flat_map(|&x| corpus.iter().map(move |&y| (x, y))).unzip()
}

// ---------------------------------------------------------------------------
// SELECT (CASE blend)
// ---------------------------------------------------------------------------

/// SELECT takes the chosen branch's value and null bit; a NULL condition
/// chooses `b`.
#[test]
fn select_takes_the_chosen_branch() {
    let n = 77;
    let cond = |row: usize| (row as i64) % 3 - 1; // cycles -1, 0, 1
    let branch = |row: usize, col: usize| 1000 * col as i64 + row as i64;
    for nullable in [false, true] {
        let null = |row: usize, col: usize| nullable && row.is_multiple_of([5, 7, 11][col]);
        let col = |col: usize, f: &dyn Fn(usize) -> i64| i64s((0..n).map(|row| (!null(row, col)).then(|| f(row))));
        let got = ints(
            &[
                col(0, &cond),
                col(1, &|row| branch(row, 1)),
                col(2, &|row| branch(row, 2)),
            ],
            |r| vec![LogicalInstr::Select { cond: r[0], a: r[1], b: r[2] }],
        );
        let want: Vec<Option<i64>> = (0..n)
            .map(|row| {
                let col = if !null(row, 0) && cond(row) != 0 { 1 } else { 2 };
                (!null(row, col)).then(|| branch(row, col))
            })
            .collect();
        assert_eq!(got, want, "nullable={nullable}");
    }
}

/// `CASE WHEN c1 THEN v1 WHEN c2 THEN v2 END` lowers to
/// `select(c1, v1, select(c2, v2, load_null()))`: the first truthy WHEN wins,
/// and with none — a NULL condition included — the implicit ELSE is NULL.
#[test]
fn case_picks_the_first_truthy_when_or_the_implicit_null() {
    let got = ints(&[i64s([Some(1), Some(0), Some(0), None]), i64s([1, 5, 0, 0])], |r| {
        vec![
            LogicalInstr::LoadConst { val: 10, unsigned: false },      // r2 = v1
            LogicalInstr::LoadConst { val: 20, unsigned: false },      // r3 = v2
            LogicalInstr::LoadNull,                                    // r4 = ELSE
            LogicalInstr::Select { cond: r[1], a: Reg(3), b: Reg(4) }, // c2 ? v2 : NULL
            LogicalInstr::Select { cond: r[0], a: Reg(2), b: Reg(5) }, // c1 ? v1 : inner
        ]
    });
    assert_eq!(got, [Some(10), Some(20), None, None]);
}

/// A boolean register consumed by an arithmetic opcode is read as a value, so it
/// cannot be `bit_only`: the AND result must land in `regs` for the add to read.
#[test]
fn a_boolean_feeding_arithmetic_lands_in_regs() {
    let got = ints(&[i64s([2, 2]), i64s([3, 0])], |r| {
        vec![
            LogicalInstr::LoadConst { val: 1, unsigned: false },
            LogicalInstr::Cmp { op: CmpOp::Gt, a: r[0], b: Reg(2) },
            LogicalInstr::Cmp { op: CmpOp::Gt, a: r[1], b: Reg(2) },
            LogicalInstr::BoolBinary { is_or: false, a: Reg(3), b: Reg(4) },
            LogicalInstr::LoadConst { val: 10, unsigned: false },
            LogicalInstr::IntArith {
                op: IntArithOp::Add,
                a: Reg(5),
                b: Reg(6),
            },
        ]
    });
    assert_eq!(got, [Some(11), Some(10)]);
}

// ---------------------------------------------------------------------------
// Register loads
// ---------------------------------------------------------------------------

/// Every fixed-width integer type loads its register image from a payload slot
/// on either side of a compound key, and from that key's second column.
#[test]
fn every_int_load_reads_its_value_from_either_region() {
    const PATTERNS: [u64; 10] = [
        0,
        1,
        0x7F,
        0x80,
        0xFF,
        0x8000,
        0x8000_0000,
        1 << 63,
        u64::MAX,
        0x0123_4567_89AB_CDEF,
    ];
    for fi in [
        FixedInt::U8,
        FixedInt::I8,
        FixedInt::U16,
        FixedInt::I16,
        FixedInt::U32,
        FixedInt::I32,
        FixedInt::U64,
        FixedInt::I64,
    ] {
        let tc = fi.type_code();
        let schema = TestSchema::new(
            &[
                (tc, false),            // payload slot 0
                (TypeCode::U16, false), // key column 0
                (tc, false),            // key column 1, at byte 2
                (tc, false),            // payload slot 1
            ],
            &[1, 2],
        );
        // Each column's value on the row holding `p`, distinct per slot.
        let value = |col: u32, p: u64| if col == 3 { p.rotate_left(8) } else { p };
        let mut view = TestView::for_schema(&schema, PATTERNS.len());
        for (row, &p) in PATTERNS.iter().enumerate() {
            for col in [0, 2, 3] {
                view.set_native(&schema, row, col, value(col as u32, p).into());
            }
        }
        for col in [0, 2, 3] {
            let mut ev = scalar_prog(&schema, vec![LogicalInstr::LoadColInt { col }], vec![]);
            let want: Vec<Option<i64>> = PATTERNS
                .iter()
                .map(|&p| Some(fi.unpack(u128::from(value(col, p)))))
                .collect();
            assert_eq!(int_rows(&mut ev, &view), want, "{tc} column {col}");
        }
    }
}

// ---------------------------------------------------------------------------
// Numeric scalar functions and numeric CAST
// ---------------------------------------------------------------------------

/// Float results as `f64` bits, so a NaN equals a NaN and `-0.0` differs from
/// `0.0` — the cells where the float ops are defined apart from each other.
fn float_bits(rows: &[Option<i64>]) -> Vec<Option<u64>> {
    rows.iter().map(|r| r.map(|x| canonical_bits(decode_f64(x)))).collect()
}

fn canonical_bits(x: f64) -> u64 {
    if x.is_nan() {
        f64::NAN.to_bits()
    } else {
        x.to_bits()
    }
}

/// Every integer op is the wrapping Rust op, and `/` `%` NULL a zero divisor.
#[test]
fn int_arithmetic_is_the_wrapping_rust_op() {
    let corpus = [i64::MIN, -7, -1, 0, 1, 3, i64::MAX];
    let (a, b) = pairs(&corpus);
    let a_col = || i64s(a.iter().copied().chain([1]));
    let b_col = i64s(b.iter().map(|&y| Some(y)).chain([None]));
    for op in IntArithOp::ALL.iter().copied() {
        let f: fn(i64, i64) -> Option<i64> = match op {
            IntArithOp::Add => |x, y| Some(x.wrapping_add(y)),
            IntArithOp::Sub => |x, y| Some(x.wrapping_sub(y)),
            IntArithOp::Mul => |x, y| Some(x.wrapping_mul(y)),
            IntArithOp::Div => |x, y| (y != 0).then(|| x.wrapping_div(y)),
            IntArithOp::Mod => |x, y| (y != 0).then(|| x.wrapping_rem(y)),
        };
        let want: Vec<Option<i64>> = a.iter().zip(&b).map(|(&x, &y)| f(x, y)).chain([None]).collect();
        let got = ints(&[a_col(), b_col.clone()], |r| {
            vec![LogicalInstr::IntArith { op, a: r[0], b: r[1] }]
        });
        assert_eq!(got, want, "{op:?}");
    }

    let vals = i64s(corpus.iter().map(|&x| Some(x)).chain([None]));
    for op in IntUnaryOp::ALL.iter().copied() {
        let f: fn(i64) -> i64 = match op {
            IntUnaryOp::Neg => i64::wrapping_neg,
            IntUnaryOp::Abs => i64::wrapping_abs,
            IntUnaryOp::Sign => i64::signum,
        };
        let want: Vec<Option<i64>> = corpus.iter().map(|&x| Some(f(x))).chain([None]).collect();
        assert_eq!(
            ints(std::slice::from_ref(&vals), |r| vec![LogicalInstr::IntUnary {
                op,
                a: r[0]
            }]),
            want,
            "{op:?}"
        );
    }
    // SIGN over an unsigned register reads it as never negative.
    assert_eq!(
        ints(&[int(TypeCode::U64, [u64::MAX as i64, 0])], |r| vec![
            LogicalInstr::IntUnary { op: IntUnaryOp::Sign, a: r[0] }
        ]),
        [Some(1), Some(0)]
    );
}

/// Every float op is the IEEE op, and `/` NULLs a zero divisor.
#[test]
fn float_arithmetic_is_the_ieee_op() {
    let corpus = [0.0f64, -0.0, 1.5, -2.0, 10.0, f64::INFINITY, f64::NAN];
    let (x, y) = pairs(&corpus);
    for op in FloatArithOp::ALL.iter().copied() {
        let f: fn(f64, f64) -> Option<f64> = match op {
            FloatArithOp::Add => |x, y| Some(x + y),
            FloatArithOp::Sub => |x, y| Some(x - y),
            FloatArithOp::Mul => |x, y| Some(x * y),
            FloatArithOp::Div => |x, y| (y != 0.0).then(|| x / y),
            FloatArithOp::Pow => |x, y| Some(x.powf(y)),
        };
        let got = ints(&[f64s(x.clone()), f64s(y.clone())], |r| {
            vec![LogicalInstr::FloatArith { op, a: r[0], b: r[1] }]
        });
        let want: Vec<Option<u64>> = x.iter().zip(&y).map(|(&x, &y)| f(x, y).map(canonical_bits)).collect();
        assert_eq!(float_bits(&got), want, "{op:?}");
    }
}

/// Every `FloatUnaryOp` is the `f64` method it is named for.
#[test]
fn float_unary_ops_match_ieee() {
    let inputs = [-0.0f64, 0.0, 2.5, 3.5, -2.5, -1.7, 4.0, 1000.0, f64::INFINITY, f64::NAN];
    // One NULL row, so null propagation is swept too.
    let vals = f64s(inputs.iter().map(|&x| Some(x)).chain([None]));
    for op in FloatUnaryOp::ALL.iter().copied() {
        let f: fn(f64) -> f64 = match op {
            FloatUnaryOp::Neg => |x| -x,
            FloatUnaryOp::Abs => f64::abs,
            FloatUnaryOp::Floor => f64::floor,
            FloatUnaryOp::Ceil => f64::ceil,
            FloatUnaryOp::Round => f64::round_ties_even,
            FloatUnaryOp::Trunc => f64::trunc,
            FloatUnaryOp::Sqrt => f64::sqrt,
            FloatUnaryOp::Ln => f64::ln,
            FloatUnaryOp::Log10 => f64::log10,
            FloatUnaryOp::Exp => f64::exp,
            // SIGN(-0.0) is +0.0: zero has no sign to report.
            FloatUnaryOp::Sign => |x| if x == 0.0 || x.is_nan() { x.abs() } else { x.signum() },
        };
        let got = ints(std::slice::from_ref(&vals), |r| {
            vec![LogicalInstr::FloatUnary { op, a: r[0] }]
        });
        let want: Vec<Option<u64>> = inputs
            .iter()
            .map(|&x| Some(canonical_bits(f(x))))
            .chain([None])
            .collect();
        assert_eq!(float_bits(&got), want, "{op:?}");
    }
}

#[test]
fn float_to_f32_nulls_only_on_finite_overflow() {
    // The value just above f32::MAX that rounds DOWN to it is not an overflow.
    let just_over = f32::MAX as f64 + 2.0f64.powi(102);
    let inputs = [
        0.1f64,
        1e300,
        1e-300,
        f64::INFINITY,
        f64::NAN,
        f32::MAX as f64,
        just_over,
    ];
    let got = ints(&[f64s(inputs)], |r| vec![LogicalInstr::FloatToF32 { a: r[0] }]);
    let want = [
        Some(0.1f32 as f64), // rounded through f32 precision
        None,                // finite beyond f32 range
        Some(0.0),           // underflow flushes to zero
        Some(f64::INFINITY),
        Some(f64::NAN),
        Some(f32::MAX as f64),
        Some(f32::MAX as f64),
    ];
    assert_eq!(float_bits(&got), want.map(|w| w.map(canonical_bits)));
}

const FIXED_INTS: [FixedInt; 8] = [
    FixedInt::U8,
    FixedInt::I8,
    FixedInt::U16,
    FixedInt::I16,
    FixedInt::U32,
    FixedInt::I32,
    FixedInt::U64,
    FixedInt::I64,
];

/// `CAST` to every integer type from either source signedness: the value
/// survives iff the source reads it inside the target's range, and then keeps
/// its register image. The corpus sits on both sides of every width's bounds.
#[test]
fn int_cast_keeps_exactly_the_values_in_the_target_range() {
    let corpus: Vec<i64> = [
        0i128, 1, -1, 127, 128, -128, -129, 255, 256, 32_767, 32_768, -32_768, -32_769, 65_535, 65_536,
    ]
    .into_iter()
    .chain([i32::MAX as i128, i32::MIN as i128, u32::MAX as i128])
    .flat_map(|x| [x, x + 1])
    .map(|x| x as i64)
    .chain([i64::MIN, i64::MAX])
    .collect();
    for source in [TypeCode::I64, TypeCode::U64] {
        let read = |bits: i64| match source {
            TypeCode::U64 => i128::from(bits as u64),
            _ => i128::from(bits),
        };
        for fi in FIXED_INTS {
            let (lo, hi) = fi.range();
            let got = ints(&[int(source, corpus.iter().copied())], |r| {
                vec![LogicalInstr::IntCast { a: r[0], fi }]
            });
            let want: Vec<Option<i64>> = corpus
                .iter()
                .map(|&x| (lo..=hi).contains(&read(x)).then_some(x))
                .collect();
            assert_eq!(got, want, "{source} -> {fi:?}");
        }
    }
}

/// Float to every integer type: truncation toward zero, then a half-open range
/// check — the upper bound is exclusive, so `2^63` is out of I64 and `127.9` in
/// I8. NaN and the infinities fail every comparison, so they are NULL.
#[test]
fn float_to_int_truncates_then_range_checks() {
    let corpus = [
        2.7,
        -2.7,
        -0.5,
        -1.0,
        127.9,
        128.0,
        -128.9,
        -129.0,
        255.5,
        256.0,
        32_767.5,
        -32_768.9,
        -32_769.0,
        65_535.9,
        2_147_483_647.9,
        -2_147_483_648.5,
        -2_147_483_649.0,
        4_294_967_295.5,
        4_294_967_296.0,
        9_223_372_036_854_775_000.0,
        9_223_372_036_854_775_808.0,
        18_446_744_073_709_549_568.0,
        18_446_744_073_709_551_616.0,
        1e300,
        f64::INFINITY,
        f64::NEG_INFINITY,
        f64::NAN,
    ];
    for fi in FIXED_INTS {
        let (lo, hi) = fi.range();
        let got = ints(&[f64s(corpus)], |r| vec![LogicalInstr::FloatToInt { a: r[0], fi }]);
        let want: Vec<Option<i64>> = corpus
            .iter()
            .map(|&x| {
                let t = x.trunc();
                (t.is_finite() && (lo..=hi).contains(&(t as i128))).then_some(t as i128 as i64)
            })
            .collect();
        assert_eq!(got, want, "{fi:?}");
    }
}

/// `op` over an ordering, as the compare kernels answer it.
fn cmp_want(op: CmpOp, ord: std::cmp::Ordering) -> i64 {
    i64::from(match op {
        CmpOp::Eq => ord.is_eq(),
        CmpOp::Ne => ord.is_ne(),
        CmpOp::Gt => ord.is_gt(),
        CmpOp::Ge => ord.is_ge(),
        CmpOp::Lt => ord.is_lt(),
        CmpOp::Le => ord.is_le(),
    })
}

#[test]
fn int_compare_reads_each_operand_with_its_own_signedness() {
    // Every ordered pair of a corpus straddling the sign boundary, in all four
    // column-type pairs: `-1` is `u64::MAX` read unsigned, so a mixed pair is
    // where one shared signedness would answer wrongly.
    let (a, b) = pairs(&[i64::MIN, -5, -1, 0, 1, 1 << 62, i64::MAX]);
    let value = |tc: TypeCode, bits: i64| match tc {
        TypeCode::U64 => i128::from(bits as u64),
        _ => i128::from(bits),
    };
    for a_tc in [TypeCode::I64, TypeCode::U64] {
        for b_tc in [TypeCode::I64, TypeCode::U64] {
            for op in CmpOp::ALL.iter().copied() {
                let got = ints(&[int(a_tc, a.clone()), int(b_tc, b.clone())], |r| {
                    vec![LogicalInstr::Cmp { op, a: r[0], b: r[1] }]
                });
                let want: Vec<Option<i64>> = a
                    .iter()
                    .zip(&b)
                    .map(|(&x, &y)| Some(cmp_want(op, value(a_tc, x).cmp(&value(b_tc, y)))))
                    .collect();
                assert_eq!(got, want, "{op:?} ({a_tc}, {b_tc})");
            }
        }
    }
}

#[test]
fn float_compare_is_ieee_so_every_nan_comparison_is_false() {
    // The corpus is the set where IEEE differs from a total order: NaN is
    // unordered against everything including itself, and -0.0 == 0.0.
    let (x, y) = pairs(&[0.0f64, -0.0, 1.5, -1.5, f64::INFINITY, f64::NEG_INFINITY, f64::NAN]);
    for op in CmpOp::ALL.iter().copied() {
        let got = ints(&[f64s(x.clone()), f64s(y.clone())], |r| {
            vec![LogicalInstr::FCmp { op, a: r[0], b: r[1] }]
        });
        let want: Vec<Option<i64>> = x
            .iter()
            .zip(&y)
            .map(|(x, y)| match x.partial_cmp(y) {
                Some(ord) => Some(cmp_want(op, ord)),
                // Unordered: only `!=` holds, every other comparison is false.
                None => Some(i64::from(op == CmpOp::Ne)),
            })
            .collect();
        assert_eq!(got, want, "{op:?}");
    }
}

#[test]
fn minmax2_skips_nulls_and_is_null_only_when_both_are() {
    let fold = |a: Col, b: Col, is_max| ints(&[a, b], |r| vec![LogicalInstr::IntMinMax2 { a: r[0], b: r[1], is_max }]);
    let a = || i64s([Some(1), Some(5), Some(9), None, None]);
    let b = || i64s([Some(2), Some(3), None, Some(7), None]);
    // A NULL side yields the other, whatever it stores; both NULL is NULL.
    assert_eq!(fold(a(), b(), true), [Some(2), Some(5), Some(9), Some(7), None]);
    assert_eq!(fold(a(), b(), false), [Some(1), Some(3), Some(9), Some(7), None]);

    // The unsigned arm, which the signed corpus above cannot reach: as `u64`,
    // `u64::MAX` is the maximum, while read as `i64` the same bits are `-1` and
    // would come out the minimum. Only the column's type selects the arm.
    let u64s = |x: i64| int(TypeCode::U64, [x]);
    assert_eq!(fold(u64s(-1), u64s(1), true), [Some(-1)]);
    assert_eq!(fold(u64s(-1), u64s(1), false), [Some(1)]);
}

#[test]
fn float_minmax2_uses_total_cmp_order() {
    let fold = |is_max| {
        let a = f64s([5.0, 5.0, -0.0, f64::INFINITY]);
        let b = f64s([f64::NAN, 2.0, 0.0, f64::NAN]);
        float_bits(&ints(&[a, b], |r| {
            vec![LogicalInstr::FloatMinMax2 { a: r[0], b: r[1], is_max }]
        }))
    };
    let bits = |xs: [f64; 4]| xs.map(|x| Some(canonical_bits(x))).to_vec();
    // NaN is the total-order max, and +0.0 beats -0.0.
    assert_eq!(fold(true), bits([f64::NAN, 5.0, 0.0, f64::NAN]));
    assert_eq!(fold(false), bits([5.0, 2.0, -0.0, f64::INFINITY]));
}

/// A calendar op reads a DATE column as days and a TIMESTAMP one as
/// microseconds; `ToMicros` NULLs a day count past `i64` microseconds.
#[test]
fn calendar_ops_read_their_columns_representation() {
    use crate::calendar::{days_to_micros, eval, MICROS_PER_DAY};
    let days = [-719_528i64, -1, 0, 19_782, 2_932_896];
    let micros: Vec<i64> = days.iter().map(|d| d * MICROS_PER_DAY + 49_507_000_005).collect();
    for op in CalendarOp::ALL.iter().copied().filter(|&op| op != CalendarOp::ToMicros) {
        for (tc, vals, is_micros) in [
            (TypeCode::Date, &days[..], false),
            (TypeCode::Timestamp, &micros[..], true),
        ] {
            if op == CalendarOp::ToDays && !is_micros {
                continue;
            }
            let got = ints(&[int(tc, vals.iter().map(|&v| Some(v)).chain([None]))], |r| {
                vec![LogicalInstr::Calendar { op, a: r[0], micros: is_micros }]
            });
            let want: Vec<Option<i64>> = vals
                .iter()
                .map(|&v| Some(eval(op, v, is_micros)))
                .chain([None])
                .collect();
            assert_eq!(got, want, "{op:?} over {tc}");
        }
    }
    let far = [
        0i64,
        19_782,
        i64::MAX / MICROS_PER_DAY + 1,
        i64::MIN / MICROS_PER_DAY - 1,
    ];
    let got = ints(&[int(TypeCode::Date, far)], |r| {
        vec![LogicalInstr::Calendar {
            op: CalendarOp::ToMicros,
            a: r[0],
            micros: false,
        }]
    });
    let want: Vec<Option<i64>> = far
        .iter()
        .map(|&d| Some(days_to_micros(d)).filter(|r| !r.1).map(|r| r.0))
        .collect();
    assert_eq!(got, want);
}

// ---------------------------------------------------------------------------
// The filter-bit scan
// ---------------------------------------------------------------------------

/// `scan_filter_bits` against a bit-by-bit run collector: every maximal run of
/// set bits below `n`, over single bits at both ends of a word, whole words set
/// and clear, and runs crossing and ending on word boundaries.
#[test]
fn scan_filter_bits_reports_every_maximal_run() {
    let naive = |bits: &[u64], n: usize| runs(&(0..n).map(|i| bits[i / 64] >> (i % 64) & 1 != 0).collect::<Vec<_>>());
    let words = [
        0,
        u64::MAX,
        1,
        1 << 63,
        0x5555_5555_5555_5555,
        0xF0F0_0000_0000_000F,
        0x8000_0000_0000_0001,
    ];
    for a in words {
        for b in words {
            for c in words {
                for n in [1, 63, 64, 65, 127, 128, 150, 191, 192] {
                    let mut bits = [a, b, c];
                    bits[n / 64..].iter_mut().enumerate().for_each(|(k, w)| {
                        *w &= if k == 0 { gnitz_wire::low_bits_mask(n % 64) } else { 0 };
                    });
                    let mut got = Vec::new();
                    scan_filter_bits(&bits[..n.div_ceil(64)], n, &mut got);
                    assert_eq!(got, naive(&bits, n), "{bits:x?} n={n}");
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// String registers
// ---------------------------------------------------------------------------

/// Every loaded string column is addressed in place — a short cell in its
/// column region, a long one in the blob — never copied into the arena.
#[test]
fn every_string_column_addresses_its_own_region_in_place() {
    const N: usize = 12;
    let schema = schema_pk_strings(N, false);
    // Mixed widths: a short cell is addressed inline in its column region, a
    // long one in the blob.
    let vals: Vec<String> = (0..N)
        .map(|c| match c % 2 {
            0 => format!("c{c}"),
            _ => format!("column-{c}-is-past-twelve-bytes"),
        })
        .collect();
    let view = make_string_view(&schema, 1, |_, c| &vals[c], |_, _| false);

    let mut instrs: Vec<LogicalInstr> = (0..N).map(|c| LogicalInstr::LoadColStr { col: c as u32 + 1 }).collect();
    // Fold left, so every column's view is resolved into the result.
    for c in 1..N {
        let acc = if c == 1 { Reg(0) } else { Reg(instrs.len() as u16 - 1) };
        instrs.push(LogicalInstr::StrConcat {
            a: acc,
            b: Reg(c as u16),
            skip_null: false,
        });
    }
    let mut ev = scalar_prog(&schema, instrs, vec![]);

    assert_eq!(
        ev.prog().str_cols,
        (1u64 << N) - 1,
        "every string column the program loads is registered at its own slot",
    );
    // Drive the loads alone and read back which buffer each lane resolved
    // against — the claim the concatenation below cannot make, since its own
    // result always lands in the arena.
    let mut scratch = EvalScratch::new(ev.prog());
    with_str_bufs(ev.prog(), &view, |bufs| {
        eval_batch(ev.prog(), &view, bufs, 0, 1, &mut scratch)
    });
    for (c, val) in vals.iter().enumerate() {
        let want = match gnitz_wire::german_string_inline(crate::BatchView::col_data(&view, c, 16)) {
            Some(_) => super::SRC_COL_BASE + c as u32,
            None => super::SRC_BLOB,
        };
        assert_eq!(
            scratch.str_views[c * MORSEL].src,
            want,
            "column {c} ({} bytes) loaded through the wrong buffer",
            val.len(),
        );
    }

    assert_eq!(text_rows(&mut ev, &view), [Some(vals.concat())]);
}

/// Values crossing the 12-byte inline/heap boundary, so every kernel is driven
/// over both cell classes and over a lane whose buffer discriminator changes
/// row to row.
const CELL_CLASSES: [&str; 5] = [
    "",
    "abcdefghijk",   // 11 — inline
    "abcdefghijkl",  // 12 — inline, at the threshold
    "abcdefghijklm", // 13 — first heap length
    "abcdefghijklmnopqrstuvwxyz",
];

/// UPPER and LOWER fold ASCII only: the neighbours of `a-z`/`A-Z` pass through,
/// and — a documented deviation from PostgreSQL under a UTF-8 locale — so does
/// `ß`, over both cell classes and a NULL.
#[test]
fn case_fold_is_ascii_only() {
    let vals: Vec<Option<&str>> = CELL_CLASSES
        .iter()
        .copied()
        .chain(["`az{", "@AZ[", "straße"])
        .map(Some)
        .chain([None])
        .collect();
    for upper in [true, false] {
        let got = strs(&[text(vals.clone())], |r| {
            vec![LogicalInstr::StrCase { a: r[0], upper }]
        });
        let want: Vec<Option<String>> = vals
            .iter()
            .map(|v| {
                v.map(|s| {
                    if upper {
                        s.to_ascii_uppercase()
                    } else {
                        s.to_ascii_lowercase()
                    }
                })
            })
            .collect();
        assert_eq!(got, want, "upper={upper}");
    }
}

#[test]
fn length_counts_characters_and_octets_separately() {
    // A combining mark and a ZWJ emoji, then a NULL.
    let vals = [
        Some("abc"),
        Some("e\u{0301}"),
        Some("\u{1F468}\u{200D}\u{1F469}\u{200D}\u{1F467}"),
        None,
    ];
    let len = |chars| ints(&[text(vals)], |r| vec![LogicalInstr::StrLen { a: r[0], chars }]);
    // The emoji is 5 codepoints (three faces joined by two ZWJs) in 18 bytes —
    // the byte/character distinction OCTET_LENGTH exists to expose.
    assert_eq!(len(true), [Some(3), Some(2), Some(5), None]);
    assert_eq!(len(false), [Some(3), Some(3), Some(18), None]);
}

/// SUBSTRING is the character window `[start, start + len)` clipped to the
/// string, over bounds at both `i64` extremes.
#[test]
fn substring_is_the_clipped_character_window() {
    let window = |s: &str, start: i64, len: Option<i64>| -> Option<String> {
        let n = s.chars().count() as i128;
        let end = match len {
            Some(l) if l < 0 => return None, // PostgreSQL errors; every domain error here is NULL
            Some(l) => i128::from(start) + i128::from(l),
            None => i128::MAX,
        };
        let lo = i128::from(start).clamp(1, n + 1);
        let hi = end.clamp(lo, n + 1);
        Some(s.chars().skip((lo - 1) as usize).take((hi - lo) as usize).collect())
    };
    let subjects = ["abc", "äöü", "", "abcdefghijklmnop"];
    let starts: Vec<i64> = [i64::MIN, i64::MAX].into_iter().chain(-3..=8).collect();
    let lens: Vec<Option<i64>> = [None, Some(i64::MAX)].into_iter().chain((-1..=8).map(Some)).collect();
    let mut rows: Vec<(&str, i64, Option<i64>)> = Vec::new();
    for s in subjects {
        for &start in &starts {
            rows.extend(lens.iter().map(|&len| (s, start, len)));
        }
    }
    let s_col = || text(rows.iter().map(|r| Some(r.0)));
    let start_col = || i64s(rows.iter().map(|r| r.1));

    let got = strs(&[s_col(), start_col()], |r| {
        vec![LogicalInstr::StrSubstr {
            src: r[0],
            start_reg: r[1],
            len_reg: None,
        }]
    });
    let want: Vec<Option<String>> = rows.iter().map(|&(s, st, _)| window(s, st, None)).collect();
    assert_eq!(got, want, "without FOR");

    // A NULL length stands for the absent FOR, so the column is nullable.
    let got = strs(&[s_col(), start_col(), i64s(rows.iter().map(|r| r.2))], |r| {
        vec![LogicalInstr::StrSubstr {
            src: r[0],
            start_reg: r[1],
            len_reg: Some(r[2]),
        }]
    });
    let want: Vec<Option<String>> = rows
        .iter()
        .map(|&(s, st, l)| l.and_then(|l| window(s, st, Some(l))))
        .collect();
    assert_eq!(got, want, "with FOR");
}

/// `StrSubstr` reads its `start` and `len` through `IntReg`, whose arm is chosen
/// by the register's U64 tracking — here reached from a `U64` column,
/// `SUBSTRING(s FROM ucol)`.
#[test]
fn substring_bounds_read_an_unsigned_register_as_unsigned() {
    // Row 0 takes a bound that fits either reading; row 1 takes one whose bits
    // are `-1` as `i64` and `u64::MAX` as `u64`. Signed, `-1` would open the
    // window before the string and yield a prefix; unsigned it is past the end.
    let got = strs(&[text(["abcdef"; 2]), int(TypeCode::U64, [3, -1])], |r| {
        vec![LogicalInstr::StrSubstr {
            src: r[0],
            start_reg: r[1],
            len_reg: None,
        }]
    });
    assert_eq!(got, texts(&[Some("cdef"), Some("")]));
}

#[test]
fn substring_of_a_computed_string_is_a_sub_view_of_the_arena() {
    let got = strs(&[text(["abcdefghijklmnop"])], |r| {
        vec![
            LogicalInstr::StrCase { a: r[0], upper: true },
            LogicalInstr::LoadConst { val: 3, unsigned: false },
            LogicalInstr::LoadConst { val: 4, unsigned: false },
            LogicalInstr::StrSubstr {
                src: Reg(1),
                start_reg: Reg(2),
                len_reg: Some(Reg(3)),
            },
        ]
    });
    assert_eq!(got, texts(&[Some("CDEF")]));
}

/// Each mode strips only its own ends, and BOTH is exactly LEADING then
/// TRAILING, including where the two overlap.
#[test]
fn trim_strips_the_selected_ends_only() {
    let trim = |mode: TrimMode| {
        let (mut ev, view) = prog(&[text(["xyaxybyx", "xyxy", "", "abc"])], vec![b"xy".to_vec()], |r| {
            vec![LogicalInstr::StrTrim { a: r[0], mode, set_idx: ConstIdx(0) }]
        });
        text_rows(&mut ev, &view)
    };
    assert_eq!(
        trim(TrimMode::Leading),
        texts(&[Some("axybyx"), Some(""), Some(""), Some("abc")])
    );
    assert_eq!(
        trim(TrimMode::Trailing),
        texts(&[Some("xyaxyb"), Some(""), Some(""), Some("abc")])
    );
    assert_eq!(
        trim(TrimMode::Both),
        texts(&[Some("axyb"), Some(""), Some(""), Some("abc")])
    );
}

/// LIKE over both cell classes and a NULL row, which is NULL whatever the
/// matcher answered on its stored bytes — `'%'` matches anything.
#[test]
fn like_over_inline_and_heap_cells_with_a_null_row() {
    let like = |pattern: &str, ci: bool| {
        let (mut ev, view) = prog(
            &[text([Some("abc"), Some("abcdefghijklm"), Some("xyz"), None])],
            vec![crate::LikePattern::encode(pattern, None).unwrap().as_bytes().to_vec()],
            |r| vec![LogicalInstr::StrLike { src: r[0], pat_idx: ConstIdx(0), ci }],
        );
        int_rows(&mut ev, &view)
    };
    assert_eq!(like("abc", false), [Some(1), Some(0), Some(0), None]);
    // A heap cell reached through its view, not its inline prefix.
    assert_eq!(like("%ijklm", false), [Some(0), Some(1), Some(0), None]);
    assert_eq!(like("%", false), [Some(1), Some(1), Some(1), None]);
    // ILIKE folds ASCII case; LIKE does not.
    assert_eq!(like("ABC", true), [Some(1), Some(0), Some(0), None]);
    assert_eq!(like("ABC", false), [Some(0), Some(0), Some(0), None]);
}

/// The two null rules are the whole difference between `||` and `CONCAT`, and
/// CONCAT's is asymmetric so a NULL accumulator still propagates.
#[test]
fn concat_null_rules_differ_by_operand_side() {
    let concat = |skip_null: bool| {
        strs(
            &[
                text([Some("ab"), None, Some("ab")]),
                text([Some("cd"), Some("cd"), None]),
            ],
            |r| vec![LogicalInstr::StrConcat { a: r[0], b: r[1], skip_null }],
        )
    };
    // || propagates NULL from either side; CONCAT's NULL argument contributes
    // the empty string, but a NULL accumulator still propagates.
    assert_eq!(concat(false), texts(&[Some("abcd"), None, None]));
    assert_eq!(concat(true), texts(&[Some("abcd"), None, Some("ab")]));
}

/// The three string-compare channels agree with `compare_german_strings`, the
/// order consolidation uses, over pairs diverging inside and past the 4-byte
/// prefix a fused compare short-circuits on — with and without NULLs.
#[test]
fn every_string_compare_channel_agrees_with_the_cell_comparator() {
    let corpus: &[&[u8]] = &[
        b"",
        b"a",
        b"ab",
        b"ab\0",
        b"abcd",
        b"abce",
        b"ba",
        b"ac",
        b"abcdefghijkl",
        b"abcdefghijklm",
        b"abcdefghijklmnopqrst",
        b"abcdefghijklmnopqrsu",
        &[0x80],
        &[0xFF, 0x00],
        "zzz".as_bytes(),
    ];
    let len = corpus.len();
    let mut blob = Vec::new();
    let cells: Vec<[u8; 16]> = corpus
        .iter()
        .map(|s| gnitz_wire::encode_german_string(s, &mut blob))
        .collect();

    for nullable in [false, true] {
        // Row `i * len + j` compares corpus[i] against corpus[j].
        let null = |row: usize, c: usize| nullable && row.is_multiple_of([7, 11][c]);
        let schema = schema_pk_strings(2, nullable);
        let view = make_string_view(
            &schema,
            len * len,
            |row, c| [corpus[row / len], corpus[row % len]][c],
            null,
        );
        let want = |op: CmpOp, row: usize| {
            let (i, j) = (row / len, row % len);
            let ord = gnitz_wire::compare_german_strings(&cells[i], &blob, &cells[j], &blob);
            (!null(row, 0) && !null(row, 1)).then(|| cmp_want(op, ord))
        };
        let run = |instrs: Vec<LogicalInstr>, consts| int_rows(&mut scalar_prog(&schema, instrs, consts), &view);
        for op in CmpOp::ALL.iter().copied() {
            let all_rows: Vec<Option<i64>> = (0..len * len).map(|row| want(op, row)).collect();
            let by_regs = vec![
                LogicalInstr::LoadColStr { col: 1 },
                LogicalInstr::LoadColStr { col: 2 },
                LogicalInstr::StrCmp { op, a: Reg(0), b: Reg(1) },
            ];
            assert_eq!(run(by_regs, vec![]), all_rows, "nullable={nullable} StrCmp {op:?}");
            let in_place = vec![LogicalInstr::StrColCol { op, col_a: 1, col_b: 2 }];
            assert_eq!(run(in_place, vec![]), all_rows, "nullable={nullable} StrColCol {op:?}");
            // The constant is baked into the program, so it is checked on the
            // rows whose right-hand side it is; only the left column is read.
            for (j, b) in corpus.iter().enumerate() {
                let vs_const = vec![LogicalInstr::StrColConst { op, col: 1, const_idx: ConstIdx(0) }];
                let got = run(vs_const, vec![b.to_vec()]);
                for row in (j..len * len).step_by(len) {
                    let want = (!null(row, 0)).then(|| {
                        cmp_want(
                            op,
                            gnitz_wire::compare_german_strings(&cells[row / len], &blob, &cells[j], &blob),
                        )
                    });
                    assert_eq!(got[row], want, "nullable={nullable} StrColConst {op:?} row {row}");
                }
            }
        }
    }
}

/// The fused compares name pool entries 2 then 1 behind an IN set at 0, so no
/// cell index equals its pool index; both constants live on the program's heap.
#[test]
fn a_cell_index_is_dense_over_the_constants_the_fused_compare_names() {
    let schema = schema_pk_strings(2, false);
    let a: &[u8] = b"alpha-value-past-twelve";
    let b: &[u8] = b"bravo-value-past-twelve-and-then-some";
    let zulu: &[u8] = b"zulu-value-past-twelve";
    // PKs are 1..=4; the set admits rows 1 and 2.
    let vals: [[&[u8]; 2]; 4] = [[zulu, zulu], [a, b], [a, b"short"], [b"short", b]];
    let view = make_string_view(&schema, vals.len(), |r, c| vals[r][c], |_, _| false);
    let mut ev = filter_prog(
        &schema,
        vec![
            LogicalInstr::LoadColInt { col: 0 },
            LogicalInstr::IntInSet { value_reg: Reg(0), set_idx: ConstIdx(0) },
            LogicalInstr::StrColConst {
                op: CmpOp::Eq,
                col: 2,
                const_idx: ConstIdx(2),
            },
            LogicalInstr::StrColConst {
                op: CmpOp::Eq,
                col: 1,
                const_idx: ConstIdx(1),
            },
            LogicalInstr::BoolBinary { is_or: false, a: Reg(2), b: Reg(3) },
            LogicalInstr::BoolBinary { is_or: false, a: Reg(1), b: Reg(4) },
        ],
        vec![gnitz_wire::as_le_bytes(&[2i64, 3]).to_vec(), a.to_vec(), b.to_vec()],
    );
    assert_eq!(passing_rows(&mut ev, &view), vec![false, true, false, false]);
}

#[test]
fn int_to_text_reads_the_source_signedness_from_the_register_tracking() {
    // Above i64::MAX, only the register's U64 tracking keeps the text unsigned;
    // 0 and -7 are the digit loop's shortest outputs.
    let to_text = |col: Col| strs(&[col], |r| vec![LogicalInstr::IntToStr { a: r[0] }]);
    assert_eq!(
        to_text(int(TypeCode::U64, [u64::MAX as i64, 0])),
        [Some(u64::MAX.to_string()), Some("0".into())]
    );
    assert_eq!(
        to_text(i64s([i64::MIN, -7])),
        [Some(i64::MIN.to_string()), Some("-7".into())]
    );
}

/// A float's text is at most 24 bytes and parses back to the same bits, the
/// sign of zero included.
#[test]
fn float_to_text_is_bounded_and_round_trips() {
    let vals = [
        0.0,
        -0.0,
        1.5,
        -1.5,
        1e-4,
        9.999e14,
        1e15,
        1e-5,
        1e300,
        -1e300,
        f64::MAX,
        f64::MIN_POSITIVE,
        5e-324,
        f64::INFINITY,
        f64::NEG_INFINITY,
        f64::NAN,
    ];
    let got = strs(&[f64s(vals)], |r| vec![LogicalInstr::FloatToStr { a: r[0] }]);
    for (f, text) in vals.iter().zip(got) {
        let text = text.expect("a float's text is never NULL");
        assert!(text.len() <= 24, "{f} rendered {} bytes: {text}", text.len());
        // PostgreSQL's spelling for the non-finite values, not Rust's `inf`.
        match *f {
            f if f.is_nan() => assert_eq!(text, "NaN"),
            f64::INFINITY => assert_eq!(text, "Infinity"),
            f64::NEG_INFINITY => assert_eq!(text, "-Infinity"),
            f => assert_eq!(
                text.parse::<f64>().unwrap().to_bits(),
                f.to_bits(),
                "{f} must round-trip bit-exactly through {text}"
            ),
        }
    }
}

#[test]
fn text_to_int_accepts_only_plain_decimal_and_range_checks_the_target() {
    let parse = |vals: &[&str], fi| {
        ints(&[text(vals.iter().copied())], |r| {
            vec![LogicalInstr::StrToInt { a: r[0], fi }]
        })
    };
    let cases = [
        ("42", Some(42)),
        (" 42 ", Some(42)),
        ("\t-7\n", Some(-7)),
        ("+7", Some(7)),
        ("-0", Some(0)),
        ("", None),
        ("   ", None),
        ("1.5", None),
        ("42abc", None),
        ("-", None),
        // PostgreSQL 16+ accepts these; the decimal loop deliberately does not.
        ("0x10", None),
        ("1_000", None),
        // 39 digits overflows i128's accumulate; the checked ops make it NULL,
        // never a wrap.
        ("999999999999999999999999999999999999999", None),
    ];
    let (vals, want): (Vec<&str>, Vec<Option<i64>>) = cases.into_iter().unzip();
    assert_eq!(parse(&vals, FixedInt::I64), want);
    // Range-checked against the *target*, not i64.
    assert_eq!(
        parse(&["127", "128", "-128", "-129"], FixedInt::I8),
        [Some(127), None, Some(-128), None]
    );
}

/// A U64 target must re-seed the register's unsigned tracking, or every
/// downstream ordered compare picks the signed variant on a value above 2^63.
#[test]
fn text_to_u64_seeds_the_unsigned_tracking() {
    let cmp = |fi: FixedInt| {
        ints(&[text([u64::MAX.to_string().as_str()])], |r| {
            vec![
                LogicalInstr::StrToInt { a: r[0], fi },
                LogicalInstr::LoadConst { val: 5, unsigned: false },
                LogicalInstr::Cmp { op: CmpOp::Gt, a: Reg(1), b: Reg(2) },
            ]
        })
    };
    assert_eq!(cmp(FixedInt::U64), [Some(1)], "u64::MAX > 5 under unsigned order");
    // The same text does not fit I64 at all, so the parse itself NULLs the row —
    // there is no signed reading of this value to compare wrongly.
    assert_eq!(cmp(FixedInt::I64), [None]);
}

#[test]
fn text_to_float_parses_or_nulls() {
    let cases = [
        ("1.5", Some(1.5)),
        (" -2.5e3 ", Some(-2500.0)),
        ("42", Some(42.0)),
        ("", None),
        ("abc", None),
        ("1.5x", None),
    ];
    let (vals, want): (Vec<&str>, Vec<Option<f64>>) = cases.into_iter().unzip();
    let got = ints(&[text(vals)], |r| vec![LogicalInstr::StrToFloat { a: r[0] }]);
    assert_eq!(got.into_iter().map(|r| r.map(decode_f64)).collect::<Vec<_>>(), want);
}

/// The arena is truncated back to its constant prefix at the top of every
/// morsel, so a constant's view, which lives in that prefix, must survive every
/// reset intact.
#[test]
fn a_constant_string_outlives_every_morsel_reset() {
    let n = MORSEL + 7;
    let schema = schema_pk_strings(1, false);
    let view = make_string_view(&schema, n, |row, _| format!("row{row}-abcdefghijklmnop"), |_, _| false);
    let constant = vec![LogicalInstr::LoadConstStr { const_idx: ConstIdx(0) }];
    assert_eq!(
        text_rows(
            &mut scalar_prog(&schema, constant, vec![b"constant-value".to_vec()]),
            &view
        ),
        vec![Some("constant-value".to_string()); n]
    );
}

/// The blend must carry the *chosen* branch's null bit, not the union — that is
/// what makes `COALESCE(s, 'default')` yield the default rather than NULL.
#[test]
fn string_select_takes_the_chosen_branch_and_its_null_bit() {
    let select = |a: Col, b: Col| {
        strs(&[i64s([1, 0, 1]), a, b], |r| {
            vec![LogicalInstr::StrSelect { cond: r[0], a: r[1], b: r[2] }]
        })
    };
    // Row 0's taken branch is NULL, row 2's untaken one.
    assert_eq!(
        select(
            text([None, Some("yes"), Some("yes")]),
            text([Some("no"), Some("no"), None])
        ),
        texts(&[None, Some("no"), Some("yes")])
    );
    assert_eq!(
        select(text(["yes"; 3]), text(["no"; 3])),
        texts(&[Some("yes"), Some("no"), Some("yes")])
    );
}

// ---------------------------------------------------------------------------
// The register null test
// ---------------------------------------------------------------------------

/// `IS [NOT] NULL` over a register reads its null lane whatever class the
/// register is, and its own result is never NULL. On the `no_nulls` arm no lane
/// exists and nothing is NULL, so the answer is the constant.
#[test]
fn is_null_reg_reads_the_null_lane_of_either_class() {
    for invert in [false, true] {
        let is_null = |null: bool| Some(i64::from(null ^ invert));
        let test = |col: Col| ints(&[col], |r| vec![LogicalInstr::IsNullReg { a: r[0], invert }]);
        assert_eq!(
            test(i64s([Some(5), None])),
            [is_null(false), is_null(true)],
            "integer, invert = {invert}"
        );
        assert_eq!(
            test(text([Some("abc"), None])),
            [is_null(false), is_null(true)],
            "string, invert = {invert}"
        );
        assert_eq!(
            test(i64s([0, 1, 2])),
            [is_null(false); 3],
            "NOT NULL, invert = {invert}"
        );
    }
}

// ---------------------------------------------------------------------------
// The multi-operand string producers
// ---------------------------------------------------------------------------

/// LEFT/RIGHT count characters, a negative count drops from the other end,
/// and an oversized one is the whole string. RIGHT walks from the end, so its
/// row is where an off-by-one in the backward walk shows.
#[test]
fn left_and_right_count_characters_from_either_end() {
    let side = |left: bool| {
        strs(&[text(["héllo"; 5]), i64s([2, -1, 10, 0, -10])], |r| {
            vec![LogicalInstr::StrSide { src: r[0], n_reg: r[1], left }]
        })
    };
    assert_eq!(
        side(true),
        texts(&[Some("hé"), Some("héll"), Some("héllo"), Some(""), Some("")])
    );
    assert_eq!(
        side(false),
        texts(&[Some("lo"), Some("éllo"), Some("héllo"), Some(""), Some("")])
    );
}

/// STRPOS is a 1-based *character* index, 0 when absent, 1 for an empty needle.
#[test]
fn strpos_is_a_character_index() {
    let got = ints(&[text(["héllo"; 4]), text(["l", "", "z", "éll"])], |r| {
        vec![LogicalInstr::StrPos { hay: r[0], needle: r[1] }]
    });
    assert_eq!(got, [Some(3), Some(1), Some(0), Some(2)]);
}

/// REVERSE reverses characters, so a multibyte sequence stays intact.
#[test]
fn reverse_reverses_characters_not_bytes() {
    let got = strs(&[text(["héllo", "", "a"])], |r| {
        vec![LogicalInstr::StrReverse { a: r[0] }]
    });
    assert_eq!(got, texts(&[Some("olléh"), Some(""), Some("a")]));
}

/// REPLACE rewrites every non-overlapping occurrence left to right; an empty
/// or absent pattern passes the subject through unchanged.
#[test]
fn replace_rewrites_every_occurrence_left_to_right() {
    let got = strs(
        &[
            text(["aXbXc", "abc", "aaa", "abc", "XX"]),
            text(["X", "", "aa", "z", "X"]),
            text(["--", "z", "b", "q", ""]),
        ],
        |r| vec![LogicalInstr::StrReplace { s: r[0], from: r[1], to: r[2] }],
    );
    assert_eq!(
        got,
        texts(&[Some("a--b--c"), Some("abc"), Some("ba"), Some("abc"), Some("")])
    );
}

/// The subject and the replacement may both live in the arena the kernel is
/// growing: the views are re-resolved across every push.
#[test]
fn replace_over_arena_operands_rewrites_correctly() {
    let got = strs(&[text(["axbxc"]), text(["X"]), text(["yy"])], |r| {
        vec![
            LogicalInstr::StrCase { a: r[0], upper: true },
            LogicalInstr::StrCase { a: r[2], upper: true },
            LogicalInstr::StrReplace { s: Reg(3), from: r[1], to: Reg(4) },
        ]
    });
    assert_eq!(got, texts(&[Some("AYYBYYC")]));
}

/// LPAD/RPAD measure in characters, cycle the fill, truncate a longer subject,
/// and NULL a result past the byte ceiling.
#[test]
fn pad_measures_characters_and_truncates_a_long_subject() {
    let pad = |left: bool| {
        strs(
            &[
                text(["hé", "hé", "hello", "a", "a", "a", "a", "a", "a", "a"]),
                text(["xy", "éx", "x", "x", "", "x", "xyz", "xyz", "x", "é"]),
                // The last two pass the byte ceiling by count, and by fill width.
                i64s([5, 5, 3, 0, 3, -2, 8, 2, 5_000_000_000, 4_000_000_000]),
            ],
            |r| vec![LogicalInstr::StrPad { s: r[0], n_reg: r[2], fill: r[1], left }],
        )
    };
    let want = |ss: [&str; 8]| texts(&ss.map(Some)).into_iter().chain([None, None]).collect::<Vec<_>>();
    assert_eq!(
        pad(true),
        want(["xyxhé", "éxéhé", "hel", "", "a", "", "xyzxyzxa", "xa"])
    );
    assert_eq!(
        pad(false),
        want(["héxyx", "hééxé", "hel", "", "a", "", "axyzxyzx", "ax"])
    );
}

/// SPLIT_PART indexes fields from either end, is empty past the last field,
/// treats an empty delimiter as one field, and is NULL for field zero.
#[test]
fn split_part_indexes_fields_from_either_end_and_nulls_on_zero() {
    let got = strs(
        &[
            text(["a,b,c"; 8]),
            text([",", ",", ",", ",", ",", "", "", ","]),
            i64s([2, -1, 5, -5, 1, 1, 2, 0]),
        ],
        |r| vec![LogicalInstr::StrSplitPart { s: r[0], delim: r[1], n_reg: r[2] }],
    );
    assert_eq!(
        got,
        texts(&[
            Some("b"),
            Some("c"),
            Some(""),
            Some(""),
            Some("a"),
            Some("a,b,c"),
            Some(""),
            None
        ])
    );
}

/// A NULL operand of any class makes the producer's row NULL — the shared
/// propagation over the mixed operand list.
#[test]
fn string_producers_propagate_a_null_operand_of_either_class() {
    let got = strs(&[text([None, Some("abc")]), i64s([Some(1), None])], |r| {
        vec![LogicalInstr::StrSide { src: r[0], n_reg: r[1], left: true }]
    });
    assert_eq!(got, [None, None]);
}
