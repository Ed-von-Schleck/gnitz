use crate::{ConstIdx, FloatArithOp, IntArithOp, Reg, TrimMode};
use gnitz_wire::{FixedInt, TypeCode};

use super::{decode_f64, encode_f64, eval_batch, with_str_bufs, EvalScratch, MORSEL};
use crate::eval::Resolved;
use crate::program::{FloatUnaryOp, IntUnaryOp};
use crate::test_support::{
    both_arms, filter_prog, make_int_view, make_n_col_view, make_string_view, passing_rows, row_strs, row_values,
    scalar_prog, schema_pk_ints, schema_pk_strings, TestSchema, TestView,
};
use crate::{CmpOp, LogicalInstr, ResolvedProgram, RowSource, ScalarEval};

/// `eval_batch` over rows `0..m` with the string-buffer table `eval_morsels`
/// assembles, for the tests that read the register file itself.
fn drive(prog: &ResolvedProgram, mb: &dyn crate::BatchView, m: usize, scratch: &mut EvalScratch) {
    with_str_bufs(prog, mb, |bufs| eval_batch(prog, mb, bufs, 0, m, scratch));
}

/// Resolve a test program down to the raw evaluable form [`drive`] takes.
fn resolved(schema: &TestSchema, instrs: Vec<LogicalInstr>, result_reg: Reg) -> ResolvedProgram {
    scalar_prog(schema, instrs, result_reg, vec![]).into_prog()
}

/// Every row's result, as the register's `i64` image — a float's bit pattern.
fn int_rows(ev: &mut ScalarEval, view: &TestView) -> Vec<Option<i64>> {
    row_values(ev, view).into_iter().map(|v| v.map(|x| x as i64)).collect()
}

/// Every row's string result as text, `None` for NULL.
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

/// A PK column is the one operand that reaches arithmetic through `LoadPk`
/// rather than a payload load, and a NULL operand must carry through to the
/// result — the `null_or2` path every binary opcode shares.
#[test]
fn arithmetic_reads_a_pk_operand_and_propagates_a_null_one() {
    let schema = schema_pk_ints(1, true);
    let mb = make_int_view(&schema, &[(1, 0, &[10]), (2, 0, &[20]), (3, 1, &[30])]);
    let instrs = vec![
        LogicalInstr::LoadColInt { col: 0 }, // the PK
        LogicalInstr::LoadColInt { col: 1 },
        LogicalInstr::IntArith {
            op: IntArithOp::Add,
            a: Reg(0),
            b: Reg(1),
        },
    ];
    assert_eq!(
        row_values(&mut scalar_prog(&schema, instrs, Reg(2), vec![]), &mb),
        [Some(11), Some(22), None]
    );
}

// ---------------------------------------------------------------------------
// SELECT (CASE blend)
// ---------------------------------------------------------------------------

/// SELECT against a per-row 3VL reference across every word/morsel boundary, on
/// both arms: NOT NULL branches take `no_nulls` and blend values alone, while
/// the nullable arm blends null masks at word granularity and values row by
/// row, so a tail-word or boundary bug shows up as a mismatch on some row.
#[test]
fn select_boundary_sweep() {
    let cond_val = |row: usize| (row as i64) % 3 - 1; // cycles -1, 0, 1
    let branch_val = |row: usize, col: usize| 1000 * col as i64 + row as i64;
    let is_null = |row: usize, col: usize| row.is_multiple_of([5, 7, 11][col]);
    for nullable in [false, true] {
        let schema = schema_pk_ints(3, nullable);
        let instrs = vec![
            LogicalInstr::LoadColInt { col: 1 }, // cond
            LogicalInstr::LoadColInt { col: 2 }, // a
            LogicalInstr::LoadColInt { col: 3 }, // b
            LogicalInstr::Select { cond: Reg(0), a: Reg(1), b: Reg(2) },
        ];
        let mut ev = scalar_prog(&schema, instrs, Reg(3), vec![]);
        assert_eq!(ev.prog().no_nulls, !nullable);
        let null = |row, col| nullable && is_null(row, col);
        for &n in &[1, 63, 64, 65, 128, 256, 257] {
            let mb = make_n_col_view(
                &schema,
                n,
                |row, col| if col == 0 { cond_val(row) } else { branch_val(row, col) },
                null,
            );
            let want: Vec<Option<i128>> = (0..n)
                .map(|row| {
                    let col = if !null(row, 0) && cond_val(row) != 0 { 1 } else { 2 };
                    (!null(row, col)).then(|| i128::from(branch_val(row, col)))
                })
                .collect();
            assert_eq!(row_values(&mut ev, &mb), want, "nullable={nullable} n={n}");
        }
    }
}

/// `CASE WHEN c1 THEN v1 WHEN c2 THEN v2 END` lowers to
/// `select(c1, v1, select(c2, v2, load_null()))`: the first truthy WHEN wins,
/// and with none — a NULL condition included — the implicit ELSE is NULL.
#[test]
fn case_picks_the_first_truthy_when_or_the_implicit_null() {
    let schema = schema_pk_ints(2, true);
    let instrs = vec![
        LogicalInstr::LoadColInt { col: 1 },                         // c1
        LogicalInstr::LoadColInt { col: 2 },                         // c2
        LogicalInstr::LoadConst { val: 10, unsigned: false },        // v1
        LogicalInstr::LoadConst { val: 20, unsigned: false },        // v2
        LogicalInstr::LoadNull,                                      // ELSE
        LogicalInstr::Select { cond: Reg(1), a: Reg(3), b: Reg(4) }, // c2 ? v2 : NULL
        LogicalInstr::Select { cond: Reg(0), a: Reg(2), b: Reg(5) }, // c1 ? v1 : inner
    ];
    // (c1, c2) per row; a NULL c1 stores a truthy value.
    let conds = [(1, 1), (0, 5), (0, 0), (7, 0)];
    let mb = make_n_col_view(
        &schema,
        conds.len(),
        |row, col| [conds[row].0, conds[row].1][col],
        |row, col| row == 3 && col == 0,
    );
    assert_eq!(
        row_values(&mut scalar_prog(&schema, instrs, Reg(6), vec![]), &mb),
        [Some(10), Some(20), None, None]
    );
}

// ---------------------------------------------------------------------------
// Register loads and the null words they write
// ---------------------------------------------------------------------------

/// A boolean register consumed by an arithmetic opcode is read as a value, so it
/// cannot be `bit_only`: the AND result must land in `regs` for the add to read.
/// Nullable columns keep this off `no_nulls`, where `BoolBinary` writes `regs`
/// regardless.
#[test]
fn a_boolean_feeding_arithmetic_lands_in_regs() {
    let schema = schema_pk_ints(2, true);
    let mb = make_int_view(&schema, &[(1, 0, &[2, 3]), (2, 0, &[2, 0])]);
    let instrs = vec![
        LogicalInstr::LoadColInt { col: 1 },
        LogicalInstr::LoadConst { val: 1, unsigned: false },
        LogicalInstr::Cmp { op: CmpOp::Gt, a: Reg(0), b: Reg(1) },
        LogicalInstr::LoadColInt { col: 2 },
        LogicalInstr::Cmp { op: CmpOp::Gt, a: Reg(3), b: Reg(1) },
        LogicalInstr::BoolBinary { is_or: false, a: Reg(2), b: Reg(4) },
        LogicalInstr::LoadConst { val: 10, unsigned: false },
        LogicalInstr::IntArith {
            op: IntArithOp::Add,
            a: Reg(5),
            b: Reg(6),
        },
    ];
    assert_eq!(
        row_values(&mut scalar_prog(&schema, instrs, Reg(7), vec![]), &mb),
        [Some(11), Some(10)]
    );
}

/// A `NOT NULL` load *clears* its destination's null words rather than skipping
/// the write, so a word left behind by an earlier morsel can never surface as a
/// phantom NULL. This is the test that fails if the clear is ever turned into a
/// skip.
///
/// At the `eval_batch` level because an evaluator owns its scratch privately,
/// and the null words are dirtied by writing them directly: a scratch is sized
/// and seeded for exactly one program, so no drive can leave another program's
/// bits in this one's register file.
#[test]
fn not_null_load_clears_stale_null_bits() {
    let schema = TestSchema::new(
        &[(TypeCode::U64, false), (TypeCode::I64, true), (TypeCode::I64, false)],
        &[0],
    );
    let m = 128;
    let words = m / 64;
    let mb = make_n_col_view(&schema, m, |row, _| row as i64, |row, col| col == 0 && row % 2 == 0);

    // The `NOT NULL` load classifies onto the fast arm on its own; forced onto
    // the nullable one it runs the kernel whose write is the subject here.
    let mut not_null_load = resolved(&schema, vec![LogicalInstr::LoadColInt { col: 2 }], Reg(0));
    assert!(not_null_load.no_nulls, "a NOT NULL load must classify as no_nulls");
    not_null_load.no_nulls = false;

    let mut scratch = EvalScratch::new(&not_null_load);
    scratch.null_bits[0..words].fill(u64::MAX);
    drive(&not_null_load, &mb, m, &mut scratch);
    assert!(
        scratch.null_bits[0..words].iter().all(|&w| w == 0),
        "a NOT NULL load left a stale null bit behind: {:?}",
        &scratch.null_bits[0..words],
    );
}

/// Every `LoadPayloadInt` width in both signednesses, and a compound PK whose
/// two columns differ in signedness — the arms `FixedInt` selects between. A
/// swapped `U16`/`I16` arm or a dropped OPK sign-flip miscomputes silently.
#[test]
fn int_loads_cover_every_width_and_both_pk_signednesses() {
    const ROWS: usize = 5;
    // ci0 = I32 PK, ci1 = U16 PK, in PK-list order [1, 0] — so the U16 sits at
    // OPK byte 0 and the I32 at byte 2, and column order does not decide either.
    let cols = [
        (TypeCode::I32, false),
        (TypeCode::U16, false),
        (TypeCode::U8, true),
        (TypeCode::I8, true),
        (TypeCode::U16, true),
        (TypeCode::I16, true),
        (TypeCode::U32, true),
        (TypeCode::I32, true),
        (TypeCode::U64, true),
        (TypeCode::I64, true),
    ];
    let schema = TestSchema::new(&cols, &[1, 0]);

    // Per column, the i64 register image each of the five rows must load. An
    // unsigned column's "-1" slot is the all-ones bit pattern, which reads back
    // as that type's MAX — and as -1 for U64, whose register *is* the storage
    // type.
    let pk_bits: [u64; ROWS] = [0, u64::MAX, 1, 0x8000_8000, 0x7FFF_7FFF];
    let expected: [[i64; ROWS]; 10] = [
        [0, -1, 1, -2_147_450_880, 2_147_450_879], // ci0: I32 PK, low 4 bytes of pk_bits
        [0, 65535, 1, 32768, 32767],               // ci1: U16 PK, low 2 bytes of pk_bits
        [0, 255, 0, 1, 255],                       // U8
        [-128, -1, 0, 1, 127],                     // I8
        [0, 65535, 0, 1, 65535],                   // U16
        [-32768, -1, 0, 1, 32767],                 // I16
        [0, 4_294_967_295, 0, 1, 4_294_967_295],   // U32
        [i32::MIN as i64, -1, 0, 1, i32::MAX as i64], // I32
        [0, -1, 0, 1, -1],                         // U64
        [i64::MIN, -1, 0, 1, i64::MAX],            // I64
    ];

    let payloads: Vec<Vec<i64>> = (0..ROWS)
        .map(|r| expected[2..].iter().map(|c| c[r]).collect())
        .collect();
    let rows: Vec<(u64, u64, &[i64])> = (0..ROWS).map(|r| (pk_bits[r], 0, payloads[r].as_slice())).collect();
    let mb = make_int_view(&schema, &rows);

    // Load every column into its own register and read the register file back
    // directly — the image is what is under test, so nothing is interposed
    // between the load and the assertion.
    let instrs = (0..cols.len() as u32)
        .map(|ci| LogicalInstr::LoadColInt { col: ci })
        .collect();
    let prog = resolved(&schema, instrs, Reg(0));
    let mut scratch = EvalScratch::new(&prog);
    drive(&prog, &mb, ROWS, &mut scratch);

    for (ci, col_expected) in expected.iter().enumerate() {
        assert_eq!(
            &scratch.regs[ci * MORSEL..ci * MORSEL + ROWS],
            col_expected,
            "column {ci} ({:?})",
            cols[ci].0
        );
    }
}

/// `LoadPk` reads fixed-width `&[u8; W]` arrays, so each width is one load plus
/// a byte swap, where `gnitz_wire::decode_opk_i64` takes the width as a slice
/// length — two spellings of the OPK→i64 inverse that must agree for every
/// width and both signednesses. Each type resolves its own `FixedInt` from the
/// column's type code.
#[test]
fn pk_loads_agree_with_the_wire_opk_decoder() {
    for (tc, fi) in [
        (TypeCode::U8, FixedInt::U8),
        (TypeCode::I8, FixedInt::I8),
        (TypeCode::U16, FixedInt::U16),
        (TypeCode::I16, FixedInt::I16),
        (TypeCode::U32, FixedInt::U32),
        (TypeCode::I32, FixedInt::I32),
        (TypeCode::U64, FixedInt::U64),
        (TypeCode::I64, FixedInt::I64),
    ] {
        // A single-column PK of this type; the values sweep both sign extremes
        // and the midpoint.
        let schema = TestSchema::new(&[(tc, false), (TypeCode::I64, false)], &[0]);
        let vals: [u64; 6] = [0, 1, u64::MAX, 1 << 63, (1 << 63) - 1, 0x0123_4567_89ab_cdef];
        let rows: Vec<(u64, u64, &[i64])> = vals.iter().map(|&v| (v, 0u64, &[0i64][..])).collect();
        let view = make_int_view(&schema, &rows);
        let mut ev = scalar_prog(&schema, vec![LogicalInstr::LoadColInt { col: 0 }], Reg(0), vec![]);
        let want: Vec<Option<i64>> = (0..vals.len())
            .map(|row| Some(gnitz_wire::decode_opk_i64(&view.get_pk_bytes(row)[..fi.width()], fi)))
            .collect();
        assert_eq!(int_rows(&mut ev, &view), want, "type {tc}");
    }
}

// ---------------------------------------------------------------------------
// Numeric scalar functions and numeric CAST
// ---------------------------------------------------------------------------

/// Run a one-operand program over one nullable payload column of type
/// `payload_tc`, returning each row's result. `mk` builds the instruction under
/// test from the operand register, which is loaded from column 1.
///
/// The type is a parameter because it is what `resolve` reads the register's
/// signedness off: an `I64` column tracks signed, a `U64` one unsigned, so the
/// same instruction reaches a different kernel arm. Float values ride an 8-byte
/// column as their `encode_f64` bit pattern.
fn run_unary_rows(
    payload_tc: TypeCode,
    vals: &[i64],
    nulls: &[bool],
    mk: impl Fn(Reg) -> LogicalInstr,
) -> Vec<Option<i64>> {
    let schema = TestSchema::new(&[(TypeCode::U64, false), (payload_tc, true)], &[0]);
    let view = make_n_col_view(&schema, vals.len(), |row, _| vals[row], |row, _| nulls[row]);
    let instrs = vec![LogicalInstr::LoadColInt { col: 1 }, mk(Reg(0))];
    int_rows(&mut scalar_prog(&schema, instrs, Reg(1), vec![]), &view)
}

/// Two-operand form of [`run_unary_rows`], `a` and `b` in nullable columns of
/// their own types.
fn run_binary_rows(
    (a_tc, b_tc): (TypeCode, TypeCode),
    a: &[i64],
    b: &[i64],
    a_null: &[bool],
    b_null: &[bool],
    mk: impl Fn(Reg, Reg) -> LogicalInstr,
) -> Vec<Option<i64>> {
    let schema = TestSchema::new(&[(TypeCode::U64, false), (a_tc, true), (b_tc, true)], &[0]);
    let view = make_n_col_view(
        &schema,
        a.len(),
        |row, col| [a, b][col][row],
        |row, col| [a_null, b_null][col][row],
    );
    let instrs = vec![
        LogicalInstr::LoadColInt { col: 1 },
        LogicalInstr::LoadColInt { col: 2 },
        mk(Reg(0), Reg(1)),
    ];
    int_rows(&mut scalar_prog(&schema, instrs, Reg(2), vec![]), &view)
}

const I64S: (TypeCode, TypeCode) = (TypeCode::I64, TypeCode::I64);

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

/// Every ordered pair of `corpus`, as the two operand columns.
fn pairs<T: Copy>(corpus: &[T]) -> (Vec<T>, Vec<T>) {
    corpus.iter().flat_map(|&x| corpus.iter().map(move |&y| (x, y))).unzip()
}

/// Every integer arithmetic and unary op against the Rust operation it is
/// defined as, over every pair of a corpus straddling zero and both extremes:
/// `+ - *`, negation and `ABS` wrap at the register width, and `/` `%` NULL the
/// row on a zero divisor rather than trapping. A trailing row with a NULL
/// operand is NULL out.
#[test]
fn int_arithmetic_is_the_wrapping_rust_op() {
    let corpus = [i64::MIN, -7, -1, 0, 1, 3, i64::MAX];
    let (mut a, mut b) = pairs(&corpus);
    let want = |f: fn(i64, i64) -> Option<i64>| a.iter().zip(&b).map(|(&x, &y)| f(x, y)).chain([None]).collect();
    type Reference = fn(i64, i64) -> Option<i64>;
    let binary: [(IntArithOp, Reference); 5] = [
        (IntArithOp::Add, |x, y| Some(x.wrapping_add(y))),
        (IntArithOp::Sub, |x, y| Some(x.wrapping_sub(y))),
        (IntArithOp::Mul, |x, y| Some(x.wrapping_mul(y))),
        (IntArithOp::Div, |x, y| (y != 0).then(|| x.wrapping_div(y))),
        (IntArithOp::Mod, |x, y| (y != 0).then(|| x.wrapping_rem(y))),
    ];
    let wants: Vec<Vec<Option<i64>>> = binary.iter().map(|&(_, f)| want(f)).collect();
    a.push(1);
    b.push(1);
    let no = vec![false; a.len()];
    let b_null: Vec<bool> = (0..a.len()).map(|row| row == a.len() - 1).collect();
    for ((op, _), want) in binary.into_iter().zip(wants) {
        let got = run_binary_rows(I64S, &a, &b, &no, &b_null, |a, b| LogicalInstr::IntArith { op, a, b });
        assert_eq!(got, want, "{op:?}");
    }

    let nulls: Vec<bool> = corpus.iter().map(|_| false).chain([true]).collect();
    let vals: Vec<i64> = corpus.iter().copied().chain([1]).collect();
    for (op, f) in [
        (IntUnaryOp::Neg, i64::wrapping_neg as fn(i64) -> i64),
        (IntUnaryOp::Abs, i64::wrapping_abs),
        (IntUnaryOp::Sign, i64::signum),
    ] {
        let want: Vec<Option<i64>> = corpus.iter().map(|&x| Some(f(x))).chain([None]).collect();
        assert_eq!(
            run_unary_rows(TypeCode::I64, &vals, &nulls, |a| LogicalInstr::IntUnary { op, a }),
            want,
            "{op:?}"
        );
    }
    // SIGN over an unsigned register reads it as never negative.
    assert_eq!(
        run_unary_rows(TypeCode::U64, &[u64::MAX as i64, 0], &[false; 2], |a| {
            LogicalInstr::IntUnary { op: IntUnaryOp::Sign, a }
        }),
        [Some(1), Some(0)]
    );
}

/// Every float arithmetic op against its IEEE operation over every pair of a
/// corpus holding both zeroes, the infinities and NaN — except that a zero
/// divisor NULLs the row, as the integer divide does. A domain error is NaN or
/// an infinity, never NULL.
#[test]
fn float_arithmetic_is_the_ieee_op() {
    let corpus = [0.0f64, -0.0, 1.5, -2.0, 10.0, f64::INFINITY, f64::NAN];
    let (x, y) = pairs(&corpus);
    let (a, b): (Vec<i64>, Vec<i64>) = (
        x.iter().map(|&v| encode_f64(v)).collect(),
        y.iter().map(|&v| encode_f64(v)).collect(),
    );
    let no = vec![false; a.len()];
    for (op, f) in [
        (FloatArithOp::Add, (|x, y| Some(x + y)) as fn(f64, f64) -> Option<f64>),
        (FloatArithOp::Sub, |x, y| Some(x - y)),
        (FloatArithOp::Mul, |x, y| Some(x * y)),
        (FloatArithOp::Div, |x, y| (y != 0.0).then(|| x / y)),
        (FloatArithOp::Pow, |x, y| Some(x.powf(y))),
    ] {
        let got = run_binary_rows(I64S, &a, &b, &no, &no, |a, b| LogicalInstr::FloatArith { op, a, b });
        let want: Vec<Option<u64>> = x.iter().zip(&y).map(|(&x, &y)| f(x, y).map(canonical_bits)).collect();
        assert_eq!(float_bits(&got), want, "{op:?}");
    }
}

/// Every `FloatUnaryOp` against the `f64` method it is defined as. Swept rather
/// than sampled: the ties-to-even and signed-zero cells are the only ones where
/// several ops differ from each other, and the domain edges are where the
/// transcendentals answer NaN or an infinity rather than NULL.
#[test]
fn float_unary_ops_match_ieee() {
    let inputs = [-0.0f64, 0.0, 2.5, 3.5, -2.5, -1.7, 4.0, 1000.0, f64::INFINITY, f64::NAN];
    // One NULL row, so null propagation is swept too.
    let vals: Vec<i64> = inputs.iter().map(|&f| encode_f64(f)).chain([encode_f64(1.0)]).collect();
    let nulls: Vec<bool> = inputs.iter().map(|_| false).chain([true]).collect();
    // SIGN(-0.0) is +0.0: zero has no sign to report.
    let sign = |x: f64| {
        if x == 0.0 {
            0.0
        } else if x.is_nan() {
            x
        } else {
            x.signum()
        }
    };
    for (op, reference) in [
        (FloatUnaryOp::Neg, (|x: f64| -x) as fn(f64) -> f64),
        (FloatUnaryOp::Abs, f64::abs),
        (FloatUnaryOp::Floor, f64::floor),
        (FloatUnaryOp::Ceil, f64::ceil),
        (FloatUnaryOp::Round, f64::round_ties_even),
        (FloatUnaryOp::Trunc, f64::trunc),
        (FloatUnaryOp::Sqrt, f64::sqrt),
        (FloatUnaryOp::Ln, f64::ln),
        (FloatUnaryOp::Log10, f64::log10),
        (FloatUnaryOp::Exp, f64::exp),
        (FloatUnaryOp::Sign, sign),
    ] {
        let got = run_unary_rows(TypeCode::I64, &vals, &nulls, |a| LogicalInstr::FloatUnary { op, a });
        let want: Vec<Option<u64>> = inputs
            .iter()
            .map(|&x| Some(canonical_bits(reference(x))))
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
    let vals: Vec<i64> = inputs.iter().map(|&f| encode_f64(f)).collect();
    let got = run_unary_rows(TypeCode::I64, &vals, &[false; 7], |a| LogicalInstr::FloatToF32 { a });
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

#[test]
fn int_cast_range_checks_per_target_and_source_signedness() {
    let vals = [200i64, -1, 127, 128, i64::MIN];
    let no = [false; 5];
    let cast = |fi| move |a| LogicalInstr::IntCast { a, fi };
    assert_eq!(
        run_unary_rows(TypeCode::I64, &vals, &no, cast(FixedInt::I8)),
        [None, Some(-1), Some(127), None, None],
        "signed source -> I8: only -128..=127 survive"
    );
    assert_eq!(
        run_unary_rows(TypeCode::I64, &vals, &no, cast(FixedInt::U64)),
        [Some(200), None, Some(127), Some(128), None],
        "signed source -> U64: the check degenerates to not negative"
    );
    // The U64 column's own type is what makes the source unsigned, so 2^63 is
    // read as a u64 and exceeds I64.
    assert_eq!(
        run_unary_rows(TypeCode::U64, &[5, i64::MIN], &[false; 2], cast(FixedInt::I64)),
        [Some(5), None]
    );
}

/// Truncation toward zero, then a half-open range check: the upper bound is
/// exclusive, so `2^63` is out of I64 and `127.9` in I8. NaN fails every
/// comparison, so it is NULL.
#[test]
fn float_to_int_truncates_and_bounds_exclusively() {
    let run = |fi, xs: &[f64]| {
        let vals: Vec<i64> = xs.iter().map(|&f| encode_f64(f)).collect();
        run_unary_rows(TypeCode::I64, &vals, &vec![false; xs.len()], |a| {
            LogicalInstr::FloatToInt { a, fi }
        })
    };
    assert_eq!(
        run(
            FixedInt::I64,
            &[
                2.7,
                -2.7,
                f64::NAN,
                f64::INFINITY,
                1e300,
                9223372036854775808.0,
                9223372036854775000.0
            ]
        ),
        [Some(2), Some(-2), None, None, None, None, Some(9223372036854774784)]
    );
    assert_eq!(
        run(FixedInt::I8, &[127.9, 128.0, -128.0, -128.9]),
        [Some(127), None, Some(-128), Some(-128)]
    );
    // The U64 target's own arm.
    assert_eq!(
        run(
            FixedInt::U64,
            &[-0.5, -1.0, 18446744073709551616.0, 18446744073709549568.0]
        ),
        [Some(0), None, None, Some(-2048)]
    );
}

/// Every arm of the integer and float compare kernels, against the Rust
/// operator on the same values. `Instr::Cmp` branches on `(op, order)` and
/// `Instr::FCmp` on `op`, so the axes are swept rather than sampled: a
/// hand-picked pair per operator leaves the arms that only differ on extreme
/// values — the unsigned ones above `2^63`, and every float comparison against
/// NaN — reading as covered while never being evaluated.
const CMP_OPS: [CmpOp; 6] = [CmpOp::Eq, CmpOp::Ne, CmpOp::Gt, CmpOp::Ge, CmpOp::Lt, CmpOp::Le];

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
    let no = vec![false; a.len()];
    let value = |tc: TypeCode, bits: i64| match tc {
        TypeCode::U64 => i128::from(bits as u64),
        _ => i128::from(bits),
    };
    for (a_tc, b_tc) in [
        (TypeCode::I64, TypeCode::I64),
        (TypeCode::U64, TypeCode::U64),
        (TypeCode::U64, TypeCode::I64),
        (TypeCode::I64, TypeCode::U64),
    ] {
        for op in CMP_OPS {
            let got = run_binary_rows((a_tc, b_tc), &a, &b, &no, &no, |a, b| LogicalInstr::Cmp { op, a, b });
            let want: Vec<Option<i64>> = a
                .iter()
                .zip(&b)
                .map(|(&x, &y)| Some(cmp_want(op, value(a_tc, x).cmp(&value(b_tc, y)))))
                .collect();
            assert_eq!(got, want, "{op:?} ({a_tc}, {b_tc})");
        }
    }
}

#[test]
fn float_compare_is_ieee_so_every_nan_comparison_is_false() {
    // The corpus is the set where IEEE differs from a total order: NaN is
    // unordered against everything including itself, and -0.0 == 0.0.
    let (x, y) = pairs(&[0.0f64, -0.0, 1.5, -1.5, f64::INFINITY, f64::NEG_INFINITY, f64::NAN]);
    let (a, b): (Vec<i64>, Vec<i64>) = (
        x.iter().map(|&v| encode_f64(v)).collect(),
        y.iter().map(|&v| encode_f64(v)).collect(),
    );
    let no = vec![false; a.len()];
    for op in CMP_OPS {
        let got = run_binary_rows(I64S, &a, &b, &no, &no, |a, b| LogicalInstr::FCmp { op, a, b });
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
    let a = [1i64, 5, 9, 0, 0];
    let b = [2i64, 3, 0, 7, 0];
    let an = [false, false, false, true, true];
    let bn = [false, false, true, false, true];
    let fold = |tcs, a: &[i64], b: &[i64], an: &[bool], bn: &[bool], is_max| {
        run_binary_rows(tcs, a, b, an, bn, |a, b| LogicalInstr::IntMinMax2 { a, b, is_max })
    };
    // A NULL side yields the other, whatever its value; both NULL is NULL.
    assert_eq!(
        fold(I64S, &a, &b, &an, &bn, true),
        [Some(2), Some(5), Some(9), Some(7), None]
    );
    assert_eq!(
        fold(I64S, &a, &b, &an, &bn, false),
        [Some(1), Some(3), Some(9), Some(7), None]
    );

    // The unsigned arm, which the signed corpus above cannot reach: as `u64`,
    // `u64::MAX` is the maximum, while read as `i64` the same bits are `-1` and
    // would come out the minimum. Only the column's type selects the arm.
    let u64s = (TypeCode::U64, TypeCode::U64);
    let (big, small, no) = ([u64::MAX as i64], [1i64], [false]);
    assert_eq!(fold(u64s, &big, &small, &no, &no, true), [Some(u64::MAX as i64)]);
    assert_eq!(fold(u64s, &big, &small, &no, &no, false), [Some(1)]);
}

#[test]
fn float_minmax2_uses_total_cmp_order() {
    let f = encode_f64;
    let a = [f(5.0), f(5.0), f(-0.0), f(f64::INFINITY)];
    let b = [f(f64::NAN), f(2.0), f(0.0), f(f64::NAN)];
    let no = [false; 4];
    let fold = |is_max| {
        let got = run_binary_rows(I64S, &a, &b, &no, &no, |a, b| LogicalInstr::FloatMinMax2 {
            a,
            b,
            is_max,
        });
        float_bits(&got)
    };
    let bits = |xs: [f64; 4]| xs.map(|x| Some(canonical_bits(x))).to_vec();
    // NaN is the total-order max, and +0.0 beats -0.0.
    assert_eq!(fold(true), bits([f64::NAN, 5.0, 0.0, f64::NAN]));
    assert_eq!(fold(false), bits([5.0, 2.0, -0.0, f64::INFINITY]));
}

// ---------------------------------------------------------------------------
// String registers
// ---------------------------------------------------------------------------

/// A program over `strs.len()` nullable string columns followed by
/// `ints.len()` nullable I64 columns, each given as its column of values; `mk`
/// gets the loaded registers in that order and its last instruction is the
/// result.
fn mixed_prog(
    strs: &[&[&[u8]]],
    ints: &[&[i64]],
    consts: Vec<Vec<u8>>,
    mk: impl Fn(&[Reg]) -> Vec<LogicalInstr>,
) -> (ScalarEval, TestView) {
    let rows = strs.first().map_or_else(|| ints[0].len(), |c| c.len());
    let mut cols = vec![(TypeCode::U64, false)];
    cols.extend(strs.iter().map(|_| (TypeCode::String, true)));
    cols.extend(ints.iter().map(|_| (TypeCode::I64, true)));
    let schema = TestSchema::new(&cols, &[0]);
    let mut view = TestView::for_schema(&schema, rows);
    for row in 0..rows {
        for (pi, col) in strs.iter().enumerate() {
            view.set_string(row, pi, col[row]);
        }
        for (k, col) in ints.iter().enumerate() {
            view.set_int(row, strs.len() + k, col[row]);
        }
    }
    let mut instrs: Vec<LogicalInstr> = (0..strs.len())
        .map(|k| LogicalInstr::LoadColStr { col: k as u32 + 1 })
        .collect();
    instrs.extend((0..ints.len()).map(|k| LogicalInstr::LoadColInt { col: (strs.len() + k) as u32 + 1 }));
    let regs: Vec<Reg> = (0..instrs.len()).map(|i| Reg(i as u16)).collect();
    instrs.extend(mk(&regs));
    let result = Reg(instrs.len() as u16 - 1);
    (scalar_prog(&schema, instrs, result, consts), view)
}

/// [`mixed_prog`] over one nullable STRING column with per-row nulls; `mk` gets
/// its loaded register.
fn str_prog(
    vals: &[&[u8]],
    nulls: &[bool],
    consts: Vec<Vec<u8>>,
    mk: impl Fn(Reg) -> Vec<LogicalInstr>,
) -> (ScalarEval, TestView) {
    let (ev, mut view) = mixed_prog(&[vals], &[], consts, |r| mk(r[0]));
    for (row, &is_null) in nulls.iter().enumerate() {
        if is_null {
            view.set_null(row, 0);
        }
    }
    (ev, view)
}

fn run_str_rows(vals: &[&[u8]], nulls: &[bool], mk: impl Fn(Reg) -> Vec<LogicalInstr>) -> Vec<Option<String>> {
    let (mut ev, view) = str_prog(vals, nulls, vec![], mk);
    text_rows(&mut ev, &view)
}

/// The same, but reading a *scalar* register — LENGTH, LIKE, the compares, the
/// text→number parses.
fn run_str_to_scalar(vals: &[&[u8]], nulls: &[bool], mk: impl Fn(Reg) -> Vec<LogicalInstr>) -> Vec<Option<i64>> {
    let (mut ev, view) = str_prog(vals, nulls, vec![], mk);
    int_rows(&mut ev, &view)
}

/// Far more distinct string columns than a program is likely to name: every one
/// addresses its own column region in place, because the buffer slot *is* the
/// payload slot. A short cell's view points into the column region and a long
/// one into the blob — no column is ever copied into the arena on load.
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
    let view = make_string_view(&schema, 1, |_, c| &vals[c]);

    let mut instrs: Vec<LogicalInstr> = (0..N).map(|c| LogicalInstr::LoadColStr { col: c as u32 + 1 }).collect();
    // Fold left, so every column's view is resolved into the result.
    let mut acc = Reg(0);
    for c in 1..N {
        let dst = Reg(instrs.len() as u16);
        instrs.push(LogicalInstr::StrConcat {
            a: acc,
            b: Reg(c as u16),
            skip_null: false,
        });
        acc = dst;
    }
    let mut ev = scalar_prog(&schema, instrs, acc, vec![]);

    assert_eq!(
        ev.prog().str_cols,
        (1u64 << N) - 1,
        "every string column the program loads is registered at its own slot",
    );
    // Drive the loads alone and read back which buffer each lane resolved
    // against — the claim the concatenation below cannot make, since its own
    // result always lands in the arena.
    let mut scratch = EvalScratch::new(ev.prog());
    drive(ev.prog(), &view, 1, &mut scratch);
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
const CELL_CLASSES: [&[u8]; 5] = [
    b"",
    b"abcdefghijk",   // 11 — inline
    b"abcdefghijkl",  // 12 — inline, at the threshold
    b"abcdefghijklm", // 13 — first heap length
    b"abcdefghijklmnopqrstuvwxyz",
];

#[test]
fn case_fold_is_ascii_only_and_leaves_other_bytes_alone() {
    // The ASCII boundary bytes on both sides of `a-z`/`A-Z`, and a multibyte
    // UTF-8 sequence.
    let vals: &[&[u8]] = &[b"`az{", b"@AZ[", "straße".as_bytes()];
    let fold = |upper| run_str_rows(vals, &[false; 3], |a| vec![LogicalInstr::StrCase { a, upper }]);
    // Only a-z folds; the neighbours pass through. Documented deviation from
    // PostgreSQL under a UTF-8 locale: ß is untouched.
    assert_eq!(fold(true), texts(&[Some("`AZ{"), Some("@AZ["), Some("STRAßE")]));
    assert_eq!(fold(false), texts(&[Some("`az{"), Some("@az["), Some("straße")]));
}

#[test]
fn case_fold_round_trips_every_cell_class_and_propagates_null() {
    let nulls = [false, false, true, false, false];
    let got = run_str_rows(&CELL_CLASSES, &nulls, |a| {
        vec![
            LogicalInstr::StrCase { a, upper: true },
            LogicalInstr::StrCase { a: Reg(1), upper: false },
        ]
    });
    let want: Vec<Option<String>> = CELL_CLASSES
        .iter()
        .zip(nulls)
        .map(|(s, null)| (!null).then(|| String::from_utf8(s.to_vec()).unwrap()))
        .collect();
    assert_eq!(got, want, "UPPER then LOWER is the identity on ASCII");
}

#[test]
fn length_counts_characters_and_octets_separately() {
    // A combining mark and a ZWJ emoji, then a NULL.
    let vals: &[&[u8]] = &[
        b"abc",
        "e\u{0301}".as_bytes(),
        "\u{1F468}\u{200D}\u{1F469}\u{200D}\u{1F467}".as_bytes(),
        b"abc",
    ];
    let nulls = [false, false, false, true];
    let len = |chars| run_str_to_scalar(vals, &nulls, |a| vec![LogicalInstr::StrLen { a, chars }]);
    // The emoji is 5 codepoints (three faces joined by two ZWJs) in 18 bytes —
    // the byte/character distinction OCTET_LENGTH exists to expose.
    assert_eq!(len(true), [Some(3), Some(2), Some(5), None]);
    assert_eq!(len(false), [Some(3), Some(3), Some(18), None]);
}

/// The window rule is the totality proof: every endpoint is clamped in i128 to
/// `[0, N]` over 0-based indices before any narrowing, so no start/length pair can panic or read
/// out of the string.
#[test]
fn substring_window_matches_postgres_and_is_total() {
    let one = |s: &str, start: i64, len: Option<i64>| {
        let mut r = run_str_rows(&[s.as_bytes()], &[false], |_| {
            let mut instrs = vec![LogicalInstr::LoadConst { val: start, unsigned: false }];
            let len_reg = len.map(|l| {
                instrs.push(LogicalInstr::LoadConst { val: l, unsigned: false });
                Reg(2)
            });
            instrs.push(LogicalInstr::StrSubstr { src: Reg(0), start_reg: Reg(1), len_reg });
            instrs
        });
        r.pop().unwrap()
    };
    let is = |got: Option<String>, want: &str| assert_eq!(got.as_deref(), Some(want));

    is(one("abc", 1, None), "abc");
    is(one("abc", 2, None), "bc");
    // A start at or past the end is empty, never a panic.
    is(one("abc", 4, None), "");
    // A window reaching below position 1 keeps only its part from 1 on.
    is(one("abc", -1, None), "abc");
    is(one("abc", 0, Some(2)), "a");
    is(one("abc", 1, Some(0)), "");
    is(one("abc", 2, Some(1)), "b");
    // A negative length is NULL (PostgreSQL errors; here every domain error is
    // a NULL).
    assert_eq!(one("abc", 1, Some(-1)), None);
    // Bounds near the i64 extremes: the i128 window absorbs the sum.
    is(one("abc", i64::MAX, None), "");
    is(one("abc", i64::MIN, Some(i64::MAX)), "");
    is(one("abc", 1, Some(i64::MAX)), "abc");
    // Windows are character units, not bytes. The clamp ceiling is the *byte*
    // length, which only bounds the character count, so a window that lands
    // between the two must still resolve to the string's end: "äöü" is 3
    // characters in 6 bytes, and starts/lengths in 4..=6 exercise that gap.
    is(one("äöü", 2, Some(1)), "ö");
    is(one("äöü", 2, None), "öü");
    is(one("äöü", 4, None), "");
    is(one("äöü", 3, Some(5)), "ü");
    is(one("äöü", 1, Some(4)), "äöü");
    is(one("äöü", 5, Some(2)), "");
    // A heap-backed source yields a sub-view of the heap, not a copy.
    is(one("abcdefghijklmnop", 14, Some(2)), "no");
}

/// `StrSubstr` reads its `start` and `len` through `IntReg`, whose arm is chosen
/// by the register's U64 tracking. Every other substring test drives the bounds
/// from a signed `LoadConst`; here the unsigned arm is reached from a `U64`
/// column — `SUBSTRING(s FROM ucol)`.
#[test]
fn substring_bounds_read_an_unsigned_register_as_unsigned() {
    let schema = TestSchema::new(
        &[
            (TypeCode::U64, false),
            (TypeCode::String, false),
            (TypeCode::U64, false),
        ],
        &[0],
    );
    let mut view = TestView::for_schema(&schema, 2);
    // Row 0 takes a bound that fits either reading; row 1 takes one whose bits
    // are `-1` as `i64` and `u64::MAX` as `u64`. Signed, `-1` would open the
    // window before the string and yield a prefix; unsigned it is past the end.
    for (row, start) in [3, -1].into_iter().enumerate() {
        view.set_string(row, 0, b"abcdef");
        view.set_int(row, 1, start);
    }
    let instrs = vec![
        LogicalInstr::LoadColStr { col: 1 },
        LogicalInstr::LoadColInt { col: 2 },
        LogicalInstr::StrSubstr {
            src: Reg(0),
            start_reg: Reg(1),
            len_reg: None,
        },
    ];
    let mut ev = scalar_prog(&schema, instrs, Reg(2), vec![]);
    assert_eq!(text_rows(&mut ev, &view), texts(&[Some("cdef"), Some("")]));
}

#[test]
fn substring_of_a_computed_string_is_a_sub_view_of_the_arena() {
    let got = run_str_rows(&[b"abcdefghijklmnop"], &[false], |a| {
        vec![
            LogicalInstr::StrCase { a, upper: true },
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
    let vals: &[&[u8]] = &[b"xyaxybyx", b"xyxy", b"", b"abc"];
    let trim = |mode: TrimMode| {
        let (mut ev, view) = str_prog(vals, &[false; 4], vec![b"xy".to_vec()], |a| {
            vec![LogicalInstr::StrTrim { a, mode, set_idx: ConstIdx(0) }]
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
    let vals: &[&[u8]] = &[b"abc", b"abcdefghijklm", b"xyz", b"abc"];
    let nulls = [false, false, false, true];
    let like = |pattern: &str, ci: bool| {
        let (mut ev, view) = str_prog(
            vals,
            &nulls,
            vec![crate::LikePattern::encode(pattern, None).unwrap().as_bytes().to_vec()],
            |a| vec![LogicalInstr::StrLike { src: a, pat_idx: ConstIdx(0), ci }],
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
    let schema = schema_pk_strings(2, true);
    let mut view = make_string_view(&schema, 3, |_, c| ["ab", "cd"][c]);
    view.set_null(1, 0); // a NULL
    view.set_null(2, 1); // b NULL
    let concat = |skip_null: bool| {
        let instrs = vec![
            LogicalInstr::LoadColStr { col: 1 },
            LogicalInstr::LoadColStr { col: 2 },
            LogicalInstr::StrConcat { a: Reg(0), b: Reg(1), skip_null },
        ];
        text_rows(&mut scalar_prog(&schema, instrs, Reg(2), vec![]), &view)
    };
    // || propagates NULL from either side; CONCAT's NULL argument contributes
    // the empty string, but a NULL accumulator still propagates.
    assert_eq!(concat(false), texts(&[Some("abcd"), None, None]));
    assert_eq!(concat(true), texts(&[Some("abcd"), None, Some("ab")]));
}

/// All three string-compare channels must agree with `compare_german_strings`,
/// which is the order consolidation uses. A disagreement would split one Z-set
/// element's weight across rows that never merge.
///
/// The channels are separate kernels reaching the same comparator: `StrCmp`
/// reads two registers, while `StrColCol` and `StrColConst` compare cells in
/// place and can short-circuit on the 4-byte inline prefix that a register lane
/// does not carry. Sweeping one corpus across all three is what holds the
/// prefix fast path to the same answer as the general one — the corpus is built
/// around that boundary, with pairs agreeing and diverging inside the first
/// four bytes, at the 12-byte inline/heap threshold, and past it. Every pair is
/// a row of one batch, so inline and heap cells alternate within a morsel.
///
/// Both nullability arms, because a nullable string column keeps the program off
/// `no_nulls` and the fused kernels then take the masked route. No row here is
/// NULL, so both arms owe the same verdicts.
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
    // Row `i * len + j` compares corpus[i] against corpus[j].
    let want = |op: CmpOp, row: usize| {
        let (i, j) = (row / len, row % len);
        Some(cmp_want(
            op,
            gnitz_wire::compare_german_strings(&cells[i], &blob, &cells[j], &blob),
        ))
    };

    for nullable in [false, true] {
        let schema = schema_pk_strings(2, nullable);
        let view = make_string_view(&schema, len * len, |row, c| [corpus[row / len], corpus[row % len]][c]);
        let run = |instrs: Vec<LogicalInstr>, consts| {
            let result = Reg(instrs.len() as u16 - 1);
            int_rows(&mut scalar_prog(&schema, instrs, result, consts), &view)
        };
        for op in CMP_OPS {
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
            // rows whose right-hand side it is.
            for (j, b) in corpus.iter().enumerate() {
                let vs_const = vec![LogicalInstr::StrColConst { op, col: 1, const_idx: ConstIdx(0) }];
                let got = run(vs_const, vec![b.to_vec()]);
                for row in (j..len * len).step_by(len) {
                    assert_eq!(
                        got[row], all_rows[row],
                        "nullable={nullable} StrColConst {op:?} row {row}"
                    );
                }
            }
        }
    }
}

/// A cell index is not a const-pool index. `resolve` encodes a cell only for
/// the constants a `StrColConst` names, and numbers them by first reference —
/// so `WHERE pk IN (2,3) AND t = 'b' AND s = 'a'` puts the packed `IN` set at
/// pool 0 with no cell, and pool 2 ahead of pool 1 in `const_cells`. Every
/// index here differs from the pool index it came from, so a resolver that
/// passed the pool index through would read past a two-element vector.
///
/// Both constants are past `SHORT_STRING_THRESHOLD`, so their cells carry heap
/// offsets into the program's own constant arena — the second at a non-zero
/// one — not into the batch's blob, where row 0 holds other long values: a
/// constant resolved against the batch's blob lands on unrelated bytes rather
/// than matching by luck.
#[test]
fn a_cell_index_is_dense_over_the_constants_the_fused_compare_names() {
    let schema = schema_pk_strings(2, false);
    let a: &[u8] = b"alpha-value-past-twelve";
    let b: &[u8] = b"bravo-value-past-twelve-and-then-some";
    let zulu: &[u8] = b"zulu-value-past-twelve";
    // PKs are 1..=4; the set admits rows 1 and 2.
    let vals: [[&[u8]; 2]; 4] = [[zulu, zulu], [a, b], [a, b"short"], [b"short", b]];
    let view = make_string_view(&schema, vals.len(), |r, c| vals[r][c]);
    let set: Vec<u8> = [2i64, 3].iter().flat_map(|v| v.to_le_bytes()).collect();
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
        Reg(5),
        vec![set, a.to_vec(), b.to_vec()],
    );
    assert_eq!(passing_rows(&mut ev, &view), vec![false, true, false, false]);
}

#[test]
fn int_to_text_reads_the_source_signedness_from_the_register_tracking() {
    // A U64 column above i64::MAX has a negative i64 bit pattern, so the
    // resolve-time tracking is the only thing that keeps the text unsigned.
    let schema = TestSchema::new(
        &[(TypeCode::U64, false), (TypeCode::U64, true), (TypeCode::I64, true)],
        &[0],
    );
    // Row 1 pins the digit loop's own edges: zero is the one magnitude with no
    // significant digit, and -7 is the one-digit negative.
    let view = make_int_view(&schema, &[(1, 0, &[u64::MAX as i64, i64::MIN]), (2, 0, &[0, -7])]);
    let text = |col: u32| {
        let instrs = vec![LogicalInstr::LoadColInt { col }, LogicalInstr::IntToStr { a: Reg(0) }];
        text_rows(&mut scalar_prog(&schema, instrs, Reg(1), vec![]), &view)
    };
    assert_eq!(text(1), [Some(u64::MAX.to_string()), Some("0".into())]);
    assert_eq!(text(2), [Some(i64::MIN.to_string()), Some("-7".into())]);
}

/// The magnitude switch is what bounds the output: Rust's positional `Display`
/// renders `1e300` as 301 digits. Every value must also survive the round trip
/// back through the parse — the sign of zero included, which is what keeps a
/// retraction cancelling.
#[test]
fn float_to_text_is_bounded_and_round_trips() {
    let schema = TestSchema::new(&[(TypeCode::U64, false), (TypeCode::F64, true)], &[0]);
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
    let view = make_n_col_view(&schema, vals.len(), |row, _| encode_f64(vals[row]), |_, _| false);
    let mut ev = scalar_prog(
        &schema,
        vec![
            LogicalInstr::LoadColFloat { col: 1 },
            LogicalInstr::FloatToStr { a: Reg(0) },
        ],
        Reg(1),
        vec![],
    );
    for (f, text) in vals.iter().zip(text_rows(&mut ev, &view)) {
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
    let cases: &[(&[u8], Option<i64>)] = &[
        (b"42", Some(42)),
        (b" 42 ", Some(42)),
        (b"\t-7\n", Some(-7)),
        (b"+7", Some(7)),
        (b"-0", Some(0)),
        (b"", None),
        (b"   ", None),
        (b"1.5", None),
        (b"42abc", None),
        (b"-", None),
        // PostgreSQL 16+ accepts these; the decimal loop deliberately does not.
        (b"0x10", None),
        (b"1_000", None),
        // 39 digits overflows i128's accumulate; the checked ops make it NULL,
        // never a wrap.
        (b"999999999999999999999999999999999999999", None),
    ];
    let (vals, want): (Vec<&[u8]>, Vec<Option<i64>>) = cases.iter().copied().unzip();
    let parse = |vals: &[&[u8]], fi| {
        run_str_to_scalar(vals, &vec![false; vals.len()], |a| {
            vec![LogicalInstr::StrToInt { a, fi }]
        })
    };
    assert_eq!(parse(&vals, FixedInt::I64), want);
    // Range-checked against the *target*, not i64.
    assert_eq!(
        parse(&[b"127", b"128", b"-128", b"-129"], FixedInt::I8),
        [Some(127), None, Some(-128), None]
    );
}

/// A U64 target must re-seed the register's unsigned tracking, or every
/// downstream ordered compare picks the signed variant on a value above 2^63.
#[test]
fn text_to_u64_seeds_the_unsigned_tracking() {
    let schema = schema_pk_strings(1, true);
    let view = make_string_view(&schema, 1, |_, _| u64::MAX.to_string());
    let cmp = |fi: FixedInt| {
        let instrs = vec![
            LogicalInstr::LoadColStr { col: 1 },
            LogicalInstr::StrToInt { a: Reg(0), fi },
            LogicalInstr::LoadConst { val: 5, unsigned: false },
            LogicalInstr::Cmp { op: CmpOp::Gt, a: Reg(1), b: Reg(2) },
        ];
        row_values(&mut scalar_prog(&schema, instrs, Reg(3), vec![]), &view)
    };
    assert_eq!(cmp(FixedInt::U64), [Some(1)], "u64::MAX > 5 under unsigned order");
    // The same text does not fit I64 at all, so the parse itself NULLs the row —
    // there is no signed reading of this value to compare wrongly.
    assert_eq!(cmp(FixedInt::I64), [None]);
}

#[test]
fn text_to_float_parses_or_nulls() {
    let cases: &[(&[u8], Option<f64>)] = &[
        (b"1.5", Some(1.5)),
        (b" -2.5e3 ", Some(-2500.0)),
        (b"42", Some(42.0)),
        (b"", None),
        (b"abc", None),
        (b"1.5x", None),
    ];
    let (vals, want): (Vec<&[u8]>, Vec<Option<f64>>) = cases.iter().copied().unzip();
    let got = run_str_to_scalar(&vals, &vec![false; vals.len()], |a| {
        vec![LogicalInstr::StrToFloat { a }]
    });
    assert_eq!(got.into_iter().map(|r| r.map(decode_f64)).collect::<Vec<_>>(), want);
}

/// A morsel-crossing run: the arena is truncated back to its constant prefix at
/// the top of every morsel, so a lane that survived into the next one would read
/// another row's bytes — and a constant's view, which lives in the prefix, must
/// survive every reset intact.
#[test]
fn string_lanes_are_per_morsel_and_constants_outlive_the_reset() {
    let n = MORSEL + 7;
    let schema = schema_pk_strings(1, false);
    let text = |row: usize| format!("row{row}-abcdefghijklmnop");
    let view = make_string_view(&schema, n, |row, _| text(row));
    let upper = vec![
        LogicalInstr::LoadColStr { col: 1 },
        LogicalInstr::StrCase { a: Reg(0), upper: true },
    ];
    assert_eq!(
        text_rows(&mut scalar_prog(&schema, upper, Reg(1), vec![]), &view),
        (0..n).map(|row| Some(text(row).to_uppercase())).collect::<Vec<_>>()
    );
    let constant = vec![LogicalInstr::LoadConstStr { const_idx: ConstIdx(0) }];
    assert_eq!(
        text_rows(
            &mut scalar_prog(&schema, constant, Reg(0), vec![b"constant-value".to_vec()]),
            &view
        ),
        vec![Some("constant-value".to_string()); n]
    );
}

/// The blend must carry the *chosen* branch's null bit, not the union — that is
/// what makes `COALESCE(s, 'default')` yield the default rather than NULL.
#[test]
fn string_select_takes_the_chosen_branch_and_its_null_bit() {
    let schema = schema_pk_strings(2, true);
    // PKs are 1..=3.
    let mut view = make_string_view(&schema, 3, |_, c| ["yes", "no"][c]);
    view.set_null(0, 0); // row 0: the taken branch (`a`) is NULL
    view.set_null(2, 1); // row 2: the untaken branch (`b`) is NULL

    // cond = (pk != 2): rows 0 and 2 take `a`, row 1 takes `b`.
    let mut ev = scalar_prog(
        &schema,
        vec![
            LogicalInstr::LoadColInt { col: 0 },
            LogicalInstr::LoadConst { val: 2, unsigned: false },
            LogicalInstr::Cmp { op: CmpOp::Ne, a: Reg(0), b: Reg(1) },
            LogicalInstr::LoadColStr { col: 1 },
            LogicalInstr::LoadColStr { col: 2 },
            LogicalInstr::StrSelect { cond: Reg(2), a: Reg(3), b: Reg(4) },
        ],
        Reg(5),
        vec![],
    );
    assert_eq!(text_rows(&mut ev, &view), texts(&[None, Some("no"), Some("yes")]));
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
        let test = |a| LogicalInstr::IsNullReg { a, invert };
        assert_eq!(
            run_unary_rows(TypeCode::I64, &[5, 0], &[false, true], test),
            [is_null(false), is_null(true)],
            "integer operand, invert = {invert}"
        );
        assert_eq!(
            run_str_to_scalar(&[b"abc", b""], &[false, true], |a| vec![test(a)]),
            [is_null(false), is_null(true)],
            "string operand, invert = {invert}"
        );
        let schema = schema_pk_ints(1, false);
        let view = make_n_col_view(&schema, 3, |row, _| row as i64, |_, _| false);
        let mut ev = scalar_prog(
            &schema,
            vec![LogicalInstr::LoadColInt { col: 1 }, test(Reg(0))],
            Reg(1),
            vec![],
        );
        assert!(ev.prog().no_nulls);
        assert_eq!(
            int_rows(&mut ev, &view),
            [is_null(false); 3],
            "no_nulls arm, invert = {invert}"
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
    let s: &[&[u8]] = &["héllo".as_bytes(); 5];
    let n: &[i64] = &[2, -1, 10, 0, -10];
    let side = |left: bool| {
        let (mut ev, view) = mixed_prog(&[s], &[n], vec![], |r| {
            vec![LogicalInstr::StrSide { src: r[0], n_reg: r[1], left }]
        });
        text_rows(&mut ev, &view)
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

/// `StrSide` is instantiated infallible, so it merges no fail mask — not even a
/// provably-zero one. Both arms must still produce the answer, and neither a
/// NULL, since a dropped fail flag on the `no_nulls` arm is exactly what the
/// instantiation asserts cannot happen.
#[test]
fn right_is_infallible_on_both_arms() {
    let schema = schema_pk_strings(1, false);
    let vals = ["héllo wörld", "", "abc", "日本語"];
    let view = make_string_view(&schema, vals.len(), |r, _| vals[r]);
    let instrs = vec![
        LogicalInstr::LoadColStr { col: 1 },
        LogicalInstr::LoadConst { val: 2, unsigned: false },
        LogicalInstr::StrSide { src: Reg(0), n_reg: Reg(1), left: false },
    ];
    let (mut fast, mut nullable) = both_arms("str_side", || scalar_prog(&schema, instrs.clone(), Reg(2), vec![]));
    let want = texts(&[Some("ld"), Some(""), Some("bc"), Some("本語")]);
    assert_eq!(text_rows(&mut fast, &view), want, "fast arm");
    assert_eq!(text_rows(&mut nullable, &view), want, "nullable arm");
}

/// STRPOS is a 1-based *character* index, 0 when absent, 1 for an empty needle.
#[test]
fn strpos_is_a_character_index() {
    let hay: &[&[u8]] = &["héllo".as_bytes(); 4];
    let needle: &[&[u8]] = &[b"l", b"", b"z", "éll".as_bytes()];
    let (mut ev, view) = mixed_prog(&[hay, needle], &[], vec![], |r| {
        vec![LogicalInstr::StrPos { hay: r[0], needle: r[1] }]
    });
    assert_eq!(row_values(&mut ev, &view), [Some(3), Some(1), Some(0), Some(2)]);
}

/// REVERSE reverses characters, so a multibyte sequence stays intact.
#[test]
fn reverse_reverses_characters_not_bytes() {
    let got = run_str_rows(&["héllo".as_bytes(), b"", b"a"], &[false; 3], |a| {
        vec![LogicalInstr::StrReverse { a }]
    });
    assert_eq!(got, texts(&[Some("olléh"), Some(""), Some("a")]));
}

/// REPLACE rewrites every non-overlapping occurrence left to right; an empty
/// or absent pattern passes the subject through unchanged.
#[test]
fn replace_rewrites_every_occurrence_left_to_right() {
    let s: &[&[u8]] = &[b"aXbXc", b"abc", b"aaa", b"abc", b"XX"];
    let from: &[&[u8]] = &[b"X", b"", b"aa", b"z", b"X"];
    let to: &[&[u8]] = &[b"--", b"z", b"b", b"q", b""];
    let (mut ev, view) = mixed_prog(&[s, from, to], &[], vec![], |r| {
        vec![LogicalInstr::StrReplace { s: r[0], from: r[1], to: r[2] }]
    });
    assert_eq!(
        text_rows(&mut ev, &view),
        texts(&[Some("a--b--c"), Some("abc"), Some("ba"), Some("abc"), Some("")])
    );
}

/// The subject and the replacement may both live in the arena the kernel is
/// growing: the views are re-resolved across every push.
#[test]
fn replace_over_arena_operands_rewrites_correctly() {
    let s: &[&[u8]] = &[b"axbxc"];
    let from: &[&[u8]] = &[b"X"];
    let to: &[&[u8]] = &[b"yy"];
    let (mut ev, view) = mixed_prog(&[s, from, to], &[], vec![], |r| {
        vec![
            LogicalInstr::StrCase { a: r[0], upper: true },
            LogicalInstr::StrCase { a: r[2], upper: true },
            LogicalInstr::StrReplace { s: Reg(3), from: r[1], to: Reg(4) },
        ]
    });
    assert_eq!(text_rows(&mut ev, &view), texts(&[Some("AYYBYYC")]));
}

/// LPAD/RPAD measure in characters, cycle the fill, truncate a longer subject
/// to its first `n` characters, and pad nothing for an empty fill.
#[test]
fn pad_measures_characters_and_truncates_a_long_subject() {
    let s: &[&[u8]] = &["hé".as_bytes(), "hé".as_bytes(), b"hello", b"a", b"a", b"a", b"a", b"a"];
    let n: &[i64] = &[5, 5, 3, 0, 3, -2, 8, 2];
    let fill: &[&[u8]] = &[b"xy", "éx".as_bytes(), b"x", b"x", b"", b"x", b"xyz", b"xyz"];
    let pad = |left: bool| {
        let (mut ev, view) = mixed_prog(&[s, fill], &[n], vec![], |r| {
            vec![LogicalInstr::StrPad { s: r[0], n_reg: r[2], fill: r[1], left }]
        });
        text_rows(&mut ev, &view)
    };
    let want = |ss: [&str; 8]| texts(&ss.map(Some));
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
    let s: &[&[u8]] = &[b"a,b,c".as_slice(); 8];
    let d: &[&[u8]] = &[b",", b",", b",", b",", b",", b"", b"", b","];
    let n: &[i64] = &[2, -1, 5, -5, 1, 1, 2, 0];
    let (mut ev, view) = mixed_prog(&[s, d], &[n], vec![], |r| {
        vec![LogicalInstr::StrSplitPart { s: r[0], delim: r[1], n_reg: r[2] }]
    });
    assert_eq!(
        text_rows(&mut ev, &view),
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
    let s: &[&[u8]] = &[b"abc", b"abc"];
    let n: &[i64] = &[1, 1];
    let (mut ev, mut view) = mixed_prog(&[s], &[n], vec![], |r| {
        vec![LogicalInstr::StrSide { src: r[0], n_reg: r[1], left: true }]
    });
    view.set_null(0, 0); // the subject
    view.set_null(1, 1); // the count
    assert_eq!(text_rows(&mut ev, &view), [None, None]);
}
