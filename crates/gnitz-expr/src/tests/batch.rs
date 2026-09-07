// `0 * MORSEL`, `1 * MORSEL`, `0 * NULL_WORDS_PER_REG` etc. are deliberate
// layout-documenting expressions making the register/word index explicit at
// each access site; collapsing them obscures which register is in use.
#![allow(clippy::erasing_op, clippy::identity_op)]

use std::ops::Neg;

use crate::{ConstIdx, FloatArithOp, IntArithOp, Reg, TrimMode};
use gnitz_wire::{type_code, FixedInt};
use std::num::NonZeroU8;

use super::{decode_f64, encode_f64, eval_batch, with_str_bufs, EvalScratch, MORSEL, NULL_WORDS_PER_REG};
use crate::program::{FloatUnaryOp, IntUnaryOp};
use crate::test_support::{
    both_arms, filter_prog, make_int_view, make_n_col_view, make_string_view, passing_rows, push_payload_cols, row_str,
    row_value, scalar_prog, schema_pk_ints, schema_pk_strings, set_row_pk, TestSchema, TestView,
};
use crate::{CmpOp, Evaluator, LogicalInstr, ResolvedProgram, RowSource};

/// `eval_batch` with the string-buffer table the drive methods assemble. These
/// tests sit below `Evaluator`, so they build the same preamble it does.
fn drive(prog: &ResolvedProgram, mb: &dyn crate::BatchView, start: usize, m: usize, scratch: &mut EvalScratch) {
    with_str_bufs(prog, mb, |bufs| eval_batch(prog, mb, bufs, start, m, scratch));
}

/// Resolve a test program down to the raw evaluable form the kernel tests drive
/// `eval_batch` with. The `Evaluator` wrapper is the *caller's* surface; these
/// tests are below it.
fn resolved(schema: &TestSchema, instrs: Vec<LogicalInstr>, result_reg: Reg) -> ResolvedProgram {
    scalar_prog(schema, instrs, result_reg, vec![]).prog
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
    let prog = resolved(&schema, instrs, Reg(2));
    let mut scratch = EvalScratch::new(&prog);
    drive(&prog, &mb, 0, 3, &mut scratch);

    assert_eq!(&scratch.regs[2 * MORSEL..2 * MORSEL + 2], &[11, 22]);
    // Row 2's payload is NULL, so the sum is NULL however the PK reads.
    assert_eq!(scratch.null_bits[2 * NULL_WORDS_PER_REG], 0b100);
}

// ---------------------------------------------------------------------------
// SELECT (CASE blend) — word/morsel boundary sweep
// ---------------------------------------------------------------------------

/// SELECT over nullable branches, differential against a per-row 3VL reference,
/// across every word/morsel boundary (1, 63, 64, 65, 128, 256, 257 rows). The
/// nullable arm blends null masks at word granularity and values row-by-row, so
/// a tail-word or boundary bug shows up as a value/null mismatch on some row.
#[test]
fn select_boundary_sweep() {
    // Schema: pk(u64), cond(i64 nullable), a(i64 nullable), b(i64 nullable).
    let schema = schema_pk_ints(3, true);
    let instrs = vec![
        LogicalInstr::LoadColInt { col: 1 }, // cond
        LogicalInstr::LoadColInt { col: 2 }, // a
        LogicalInstr::LoadColInt { col: 3 }, // b
        LogicalInstr::Select { cond: Reg(0), a: Reg(1), b: Reg(2) },
    ];
    let prog = resolved(&schema, instrs, Reg(3));

    // Row-parameterized generators (must match the closures passed to make_n_col_view).
    let cond_val = |row: usize| (row as i64) % 3 - 1; // cycles -1, 0, 1
    let a_val = |row: usize| 1000 + row as i64;
    let b_val = |row: usize| 2000 + row as i64;
    let cond_null = |row: usize| row.is_multiple_of(5);
    let a_null = |row: usize| row.is_multiple_of(7);
    let b_null = |row: usize| row.is_multiple_of(11);

    for &n in &[1, 63, 64, 65, 128, 256, 257] {
        let mb = make_n_col_view(
            &schema,
            n,
            |row, col| match col {
                0 => cond_val(row),
                1 => a_val(row),
                _ => b_val(row),
            },
            |row, col| match col {
                0 => cond_null(row),
                1 => a_null(row),
                _ => b_null(row),
            },
        );

        let mut scratch = EvalScratch::new(&prog);
        for morsel_start in (0..n).step_by(MORSEL) {
            let m = MORSEL.min(n - morsel_start);
            drive(&prog, &mb, morsel_start, m, &mut scratch);
            let r = 3usize; // result_reg
            for i in 0..m {
                let row = morsel_start + i;
                let take_a = !cond_null(row) && cond_val(row) != 0;
                let (exp_val, exp_null) = if take_a {
                    (a_val(row), a_null(row))
                } else {
                    (b_val(row), b_null(row))
                };
                let got_null = (scratch.null_bits[r * NULL_WORDS_PER_REG + i / 64] >> (i % 64)) & 1 != 0;
                assert_eq!(got_null, exp_null, "n={n} row={row}: null mismatch");
                if !exp_null {
                    assert_eq!(scratch.regs[r * MORSEL + i], exp_val, "n={n} row={row}: value mismatch");
                }
            }
        }
    }
}

/// SELECT on the no-nulls fast arm: NOT NULL branches select `no_nulls=true`, so
/// the value blend runs through `regs_split` with no mask tracking. Verify the blend.
#[test]
fn select_no_nulls_fast_arm() {
    // cond / a / b are all NOT NULL, which selects the no_nulls fast arm.
    let schema = schema_pk_ints(3, false);
    let instrs = vec![
        LogicalInstr::LoadColInt { col: 1 },
        LogicalInstr::LoadColInt { col: 2 },
        LogicalInstr::LoadColInt { col: 3 },
        LogicalInstr::Select { cond: Reg(0), a: Reg(1), b: Reg(2) },
    ];
    let prog = resolved(&schema, instrs, Reg(3));
    assert!(prog.no_nulls, "NOT NULL branches must select the no_nulls fast arm");

    let n = 130usize; // crosses a 64-bit word and the MORSEL boundary isn't hit, but words are
    let mb = make_n_col_view(
        &schema,
        n,
        |row, col| match col {
            0 => (row % 2) as i64, // alternating truthy/false
            1 => 1000 + row as i64,
            _ => 2000 + row as i64,
        },
        |_, _| false,
    );
    let mut scratch = EvalScratch::new(&prog);
    drive(&prog, &mb, 0, n, &mut scratch);
    for row in 0..n {
        let expected = if row % 2 == 1 {
            1000 + row as i64
        } else {
            2000 + row as i64
        };
        assert_eq!(scratch.regs[3 * MORSEL + row], expected, "row {row}: no-nulls blend");
    }
}

// ---------------------------------------------------------------------------
// Register loads and the null words they write
// ---------------------------------------------------------------------------

/// Boolean register consumed by an arithmetic opcode forces it OUT of
/// bit_only. The AND result must still appear correctly in `regs` for the
/// downstream add.
#[test]
fn bit_only_demotion_when_bool_feeds_arithmetic() {
    let schema = schema_pk_ints(2, true);
    // col1=2, col2=3, no nulls. Both > 1, so AND = 1. Add 0 → result = 1.
    let mb = make_int_view(&schema, &[(1, 0, &[2, 3])]);

    let instrs = vec![
        LogicalInstr::LoadColInt { col: 1 },                             // r0 = col1
        LogicalInstr::LoadConst { val: 1 },                              // r1 = 1
        LogicalInstr::Cmp { op: CmpOp::Gt, a: Reg(0), b: Reg(1) },       // r2 = col1 > 1
        LogicalInstr::LoadColInt { col: 2 },                             // r3 = col2
        LogicalInstr::Cmp { op: CmpOp::Gt, a: Reg(3), b: Reg(1) },       // r4 = col2 > 1
        LogicalInstr::BoolBinary { is_or: false, a: Reg(2), b: Reg(4) }, // r5 = r2 AND r4    (consumed by ADD → not bit_only)
        LogicalInstr::LoadConst { val: 0 },                              // r6 = 0
        LogicalInstr::IntArith {
            op: IntArithOp::Add,
            a: Reg(5),
            b: Reg(6),
        }, // r7 = r5 + 0 = bool-as-int
    ];
    let prog = resolved(&schema, instrs, Reg(7));
    // r5 is bool-produced but consumed by INT_ADD (non-bool). Must be demoted.
    assert!(
        !prog.is_bit_only(5),
        "bool reg fed into arithmetic must NOT be bit_only",
    );

    let mut scratch = EvalScratch::new(&prog);
    drive(&prog, &mb, 0, 1, &mut scratch);
    // r5 lives in regs as 0/1; r7 = r5 + 0 = 1.
    assert_eq!(
        scratch.regs[5 * MORSEL],
        1,
        "AND result must land in regs when !bit_only"
    );
    assert_eq!(scratch.regs[7 * MORSEL], 1, "downstream arithmetic reads bool as i64");
}

/// A `NOT NULL` load *clears* its destination's null words rather than skipping
/// the write, so a word left behind by an earlier morsel can never surface as a
/// phantom NULL. This is the test that fails if the clear is ever turned into a
/// skip.
///
/// At the `eval_batch` level because an `Evaluator` owns its scratch privately,
/// and the null words are dirtied by writing them directly: a scratch is sized
/// and seeded for exactly one program, so no drive can leave another program's
/// bits in this one's register file.
#[test]
fn not_null_load_clears_stale_null_bits() {
    let schema = TestSchema::new(
        &[(type_code::U64, false), (type_code::I64, true), (type_code::I64, false)],
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
    drive(&not_null_load, &mb, 0, m, &mut scratch);
    assert!(
        scratch.null_bits[0..words].iter().all(|&w| w == 0),
        "a NOT NULL load left a stale null bit behind: {:?}",
        &scratch.null_bits[0..words],
    );
}

/// Every `LoadPayloadInt` width in both signednesses, and a compound PK whose
/// two columns differ in signedness — the arms `FixedInt` selects between. A
/// swapped `U16`/`I16` arm or a dropped OPK sign-flip miscomputes silently, and
/// no other test in this crate loads a narrow or signed column through the row
/// kernels.
#[test]
fn int_loads_cover_every_width_and_both_pk_signednesses() {
    const ROWS: usize = 5;
    // ci0 = I32 PK, ci1 = U16 PK, in PK-list order [1, 0] — so the U16 sits at
    // OPK byte 0 and the I32 at byte 2, and column order does not decide either.
    let cols = [
        (type_code::I32, false),
        (type_code::U16, false),
        (type_code::U8, true),
        (type_code::I8, true),
        (type_code::U16, true),
        (type_code::I16, true),
        (type_code::U32, true),
        (type_code::I32, true),
        (type_code::U64, true),
        (type_code::I64, true),
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
    drive(&prog, &mb, 0, ROWS, &mut scratch);

    for (ci, col_expected) in expected.iter().enumerate() {
        for (r, want) in col_expected.iter().enumerate() {
            assert_eq!(
                scratch.regs[ci * MORSEL + r],
                *want,
                "column {ci} ({:?}), row {r}",
                cols[ci].0
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Numeric scalar functions and numeric CAST
// ---------------------------------------------------------------------------

/// Run a one-operand program over `n` rows of one nullable payload column of
/// type `payload_tc`, returning `(value, is_null)` per row. `mk` builds the
/// instruction under test from `(dst, a)`; the operand register is loaded from
/// column 1.
///
/// The type is a parameter for the same reason it is one on [`run_binary_rows`]:
/// it is what `resolve` reads the register's signedness off, and `IntCast` /
/// `IntToFloat` carry a `signed` flag that selects a different kernel arm.
fn run_unary_rows(payload_tc: u8, vals: &[i64], nulls: &[bool], mk: impl Fn(Reg) -> LogicalInstr) -> Vec<(i64, bool)> {
    let schema = TestSchema::new(&[(type_code::U64, false), (payload_tc, true)], &[0]);
    let n = vals.len();
    let view = make_n_col_view(&schema, n, |row, _| vals[row], |row, _| nulls[row]);
    let instrs = vec![LogicalInstr::LoadColInt { col: 1 }, mk(Reg(0))];
    let prog = resolved(&schema, instrs, Reg(1));
    let mut scratch = EvalScratch::new(&prog);
    drive(&prog, &view, 0, n, &mut scratch);
    (0..n)
        .map(|i| {
            let v = scratch.regs[1 * MORSEL + i];
            let null = (scratch.null_bits[1 * NULL_WORDS_PER_REG + i / 64] >> (i % 64)) & 1 != 0;
            (v, null)
        })
        .collect()
}

/// Two-operand form of [`run_unary_rows`] over two nullable payload columns of
/// type `payload_tc`. The type is a parameter because it is what `resolve`
/// reads the register's signedness off: an `I64` column tracks signed, a `U64`
/// one unsigned, so the same instruction reaches a different kernel arm.
/// Float values ride an 8-byte column as their `encode_f64` bit pattern.
fn run_binary_rows(
    payload_tc: u8,
    a: &[i64],
    b: &[i64],
    a_null: &[bool],
    b_null: &[bool],
    mk: impl Fn(Reg, Reg) -> LogicalInstr,
) -> Vec<(i64, bool)> {
    let schema = TestSchema::new(&[(type_code::U64, false), (payload_tc, true), (payload_tc, true)], &[0]);
    let n = a.len();
    let view = make_n_col_view(
        &schema,
        n,
        |row, col| if col == 0 { a[row] } else { b[row] },
        |row, col| if col == 0 { a_null[row] } else { b_null[row] },
    );
    let instrs = vec![
        LogicalInstr::LoadColInt { col: 1 },
        LogicalInstr::LoadColInt { col: 2 },
        mk(Reg(0), Reg(1)),
    ];
    let prog = resolved(&schema, instrs, Reg(2));
    let mut scratch = EvalScratch::new(&prog);
    drive(&prog, &view, 0, n, &mut scratch);
    (0..n)
        .map(|i| {
            let v = scratch.regs[2 * MORSEL + i];
            let null = (scratch.null_bits[2 * NULL_WORDS_PER_REG + i / 64] >> (i % 64)) & 1 != 0;
            (v, null)
        })
        .collect()
}

fn fu(op: FloatUnaryOp) -> impl Fn(Reg) -> LogicalInstr {
    move |a| LogicalInstr::FloatUnary { op, a }
}

#[test]
fn int_abs_wraps_at_min_and_propagates_null() {
    let vals = [5i64, -5, 0, i64::MIN, 7];
    let nulls = [false, false, false, false, true];
    let out = run_unary_rows(type_code::I64, &vals, &nulls, |a| LogicalInstr::IntUnary {
        op: IntUnaryOp::Abs,
        a,
    });
    assert_eq!(out[0], (5, false));
    assert_eq!(out[1], (5, false));
    assert_eq!(out[2], (0, false));
    // Same-width cast, so it wraps rather than producing NULL.
    assert_eq!(out[3], (i64::MIN, false));
    assert!(out[4].1, "NULL in, NULL out");
}

/// Every `FloatUnaryOp` against the `f64` method it is defined as. Swept rather
/// than sampled: a hand-picked list left `Neg` unevaluated, and the ties-to-even
/// and signed-zero cells are the only ones where the ops differ from each other.
#[test]
fn float_unary_ops_match_ieee() {
    let inputs = [-0.0f64, 2.5, 3.5, -2.5, -1.7, f64::NAN];
    let vals: Vec<i64> = inputs.iter().map(|&f| encode_f64(f)).collect();
    let mut nulls = vec![false; vals.len()];
    // One NULL row per op, so null propagation is swept too rather than pinned once.
    let vals = [vals, vec![encode_f64(1.0)]].concat();
    nulls.push(true);

    for (op, reference) in [
        (FloatUnaryOp::Neg, f64::neg as fn(f64) -> f64),
        (FloatUnaryOp::Abs, f64::abs),
        (FloatUnaryOp::Floor, f64::floor),
        (FloatUnaryOp::Ceil, f64::ceil),
        (FloatUnaryOp::Round, f64::round_ties_even),
        (FloatUnaryOp::Trunc, f64::trunc),
    ] {
        let out = run_unary_rows(type_code::I64, &vals, &nulls, fu(op));
        for (i, &x) in inputs.iter().enumerate() {
            let (got, want) = (decode_f64(out[i].0), reference(x));
            assert!(!out[i].1, "{op:?}({x}) must not be NULL");
            assert!(
                got == want || (got.is_nan() && want.is_nan()),
                "{op:?}({x}): got {got}, want {want}"
            );
            // `==` cannot separate the zeroes, and `Neg`/`Abs`/`Round` on -0.0
            // are exactly where the sign is the whole answer.
            assert_eq!(
                got.is_sign_negative(),
                want.is_sign_negative(),
                "{op:?}({x}): wrong zero sign"
            );
        }
        assert!(out[inputs.len()].1, "{op:?}: NULL in, NULL out");
    }
}

#[test]
fn float_to_f32_nulls_only_on_finite_overflow() {
    let vals: Vec<i64> = [0.1f64, 1e300, 1e-300, f64::INFINITY, f64::NAN, f32::MAX as f64]
        .iter()
        .map(|&f| encode_f64(f))
        .collect();
    let nulls = vec![false; vals.len()];
    let out = run_unary_rows(type_code::I64, &vals, &nulls, |a| LogicalInstr::FloatToF32 { a });
    let get = |i: usize| decode_f64(out[i].0);

    assert_eq!(get(0), 0.1f32 as f64, "rounded through f32 precision");
    assert!(out[1].1, "finite value beyond f32 range is NULL");
    assert_eq!(get(2), 0.0, "underflow flushes to zero, not NULL");
    assert!(!out[2].1);
    assert!(get(3).is_infinite() && !out[3].1, "inf passes through");
    assert!(get(4).is_nan() && !out[4].1, "NaN passes through");
    assert_eq!(get(5), f32::MAX as f64, "f32::MAX itself is not overflow");
    assert!(!out[5].1);

    // The values just above f32::MAX that round DOWN to it must not be NULLed.
    let just_over = f32::MAX as f64 + 2.0f64.powi(102);
    let v = vec![encode_f64(just_over)];
    let out = run_unary_rows(type_code::I64, &v, &[false], |a| LogicalInstr::FloatToF32 { a });
    assert!(!out[0].1, "rounds down to f32::MAX, so not an overflow");
    assert_eq!(decode_f64(out[0].0), f32::MAX as f64);
}

#[test]
fn int_cast_range_checks_per_target_and_source_signedness() {
    let vals = [200i64, -1, 127, 128, i64::MIN];
    let nulls = vec![false; vals.len()];

    // Signed source -> I8: only -128..=127 survive.
    let out = run_unary_rows(type_code::I64, &vals, &nulls, |a| LogicalInstr::IntCast {
        a,
        fi: FixedInt::I8,
    });
    assert!(out[0].1, "200 out of I8");
    assert_eq!(out[1], (-1, false));
    assert_eq!(out[2], (127, false));
    assert!(out[3].1, "128 out of I8");
    assert!(out[4].1);

    // Signed source -> U64: the check degenerates to "not negative".
    let out = run_unary_rows(type_code::I64, &vals, &nulls, |a| LogicalInstr::IntCast {
        a,
        fi: FixedInt::U64,
    });
    assert_eq!(out[0], (200, false));
    assert!(out[1].1, "-1 has no U64 image");
    assert!(out[4].1);

    // Unsigned source -> I64: values >= 2^63 fail. The U64 taint comes from the
    // column's own type, which is what makes `src_signed` false.
    let out = run_unary_rows(type_code::U64, &[5, i64::MIN], &[false, false], |a| {
        LogicalInstr::IntCast { a, fi: FixedInt::I64 }
    });
    assert_eq!(out[0], (5, false), "5 fits I64");
    assert!(out[1].1, "2^63 read as u64 exceeds I64");
}

#[test]
fn float_to_int_truncates_and_bounds_exclusively() {
    let vals: Vec<i64> = [
        2.7f64,
        -2.7,
        f64::NAN,
        f64::INFINITY,
        1e300,
        9223372036854775808.0, // 2^63 exactly: out of I64
        9223372036854775000.0, // < 2^63: in range
    ]
    .iter()
    .map(|&f| encode_f64(f))
    .collect();
    let nulls = vec![false; vals.len()];
    let out = run_unary_rows(type_code::I64, &vals, &nulls, |a| LogicalInstr::FloatToInt {
        a,
        fi: FixedInt::I64,
    });
    assert_eq!(out[0], (2, false), "truncate toward zero");
    assert_eq!(out[1], (-2, false), "truncate toward zero");
    assert!(out[2].1, "NaN fails every comparison");
    assert!(out[3].1);
    assert!(out[4].1);
    assert!(out[5].1, "2^63 is excluded by the half-open upper bound");
    assert!(!out[6].1);

    // Narrow target: the same exclusive rule admits exactly -128..=127.
    let vals: Vec<i64> = [127.9f64, 128.0, -128.0, -128.9]
        .iter()
        .map(|&f| encode_f64(f))
        .collect();
    let out = run_unary_rows(type_code::I64, &vals, &[false; 4], |a| LogicalInstr::FloatToInt {
        a,
        fi: FixedInt::I8,
    });
    assert_eq!(out[0], (127, false));
    assert!(out[1].1);
    assert_eq!(out[2], (-128, false));
    assert_eq!(out[3], (-128, false), "-128.9 truncates toward zero to -128, in range");
}

/// `div_like` is the one kernel behind integer `Div`/`Mod` and float `Div`: it
/// nulls the row on a zero divisor rather than trapping, and a NULL divisor is
/// ordinary null propagation. Row 0 is live in every case, so a kernel that
/// nulled unconditionally would not pass.
#[test]
fn division_nulls_the_row_on_a_zero_or_null_divisor() {
    let a = [10i64, 10, 10];
    let b = [3i64, 0, 3];
    let no = [false; 3];
    let b_null = [false, false, true];

    // Only the null flag is asserted on rows 1 and 2: a NULL row's value half is
    // deliberately undefined, which is why a result is read back as an `Option`.
    let quot = run_binary_rows(type_code::I64, &a, &b, &no, &b_null, |a, b| LogicalInstr::IntArith {
        op: IntArithOp::Div,
        a,
        b,
    });
    assert_eq!((quot[0], quot[1].1, quot[2].1), ((3, false), true, true));

    let rem = run_binary_rows(type_code::I64, &a, &b, &no, &b_null, |a, b| LogicalInstr::IntArith {
        op: IntArithOp::Mod,
        a,
        b,
    });
    assert_eq!((rem[0], rem[1].1, rem[2].1), ((1, false), true, true));

    let fa = a.map(|x| encode_f64(x as f64));
    let fb = b.map(|x| encode_f64(x as f64));
    let fdiv = run_binary_rows(type_code::I64, &fa, &fb, &no, &b_null, |a, b| {
        LogicalInstr::FloatArith { op: FloatArithOp::Div, a, b }
    });
    assert_eq!(decode_f64(fdiv[0].0), 10.0 / 3.0);
    assert!(fdiv[1].1 && fdiv[2].1, "a zero and a NULL divisor both null the row");
}

/// Every arm of the integer and float compare kernels, against the Rust
/// operator on the same values. `Instr::Cmp` branches on `(op, signed)` and
/// `Instr::FCmp` on `op`, so the axes are swept rather than sampled: a
/// hand-picked pair per operator leaves the arms that only differ on extreme
/// values — the unsigned ones above `2^63`, and every float comparison against
/// NaN — reading as covered while never being evaluated.
const CMP_OPS: [CmpOp; 6] = [CmpOp::Eq, CmpOp::Ne, CmpOp::Gt, CmpOp::Ge, CmpOp::Lt, CmpOp::Le];

fn cmp_want(op: CmpOp, ord: std::cmp::Ordering, eq: bool) -> i64 {
    use std::cmp::Ordering::*;
    i64::from(match op {
        CmpOp::Eq => eq,
        CmpOp::Ne => !eq,
        CmpOp::Gt => ord == Greater,
        CmpOp::Ge => ord != Less,
        CmpOp::Lt => ord == Less,
        CmpOp::Le => ord != Greater,
    })
}

#[test]
fn int_compare_agrees_with_the_rust_operator_at_both_signednesses() {
    // Every ordered pair of a corpus straddling the sign boundary, so each
    // operator sees both orders of every pair. `u64::MAX` is `-1` as `i64`:
    // it sorts top unsigned and bottom signed, which is the whole difference
    // between the two kernel arms.
    let corpus = [0i64, 1, -1, 42, i64::MIN, i64::MAX];
    let pairs: Vec<(i64, i64)> = corpus.iter().flat_map(|&x| corpus.map(|y| (x, y))).collect();
    let a: Vec<i64> = pairs.iter().map(|p| p.0).collect();
    let b: Vec<i64> = pairs.iter().map(|p| p.1).collect();
    let no = vec![false; pairs.len()];

    for (payload_tc, signed) in [(type_code::I64, true), (type_code::U64, false)] {
        for op in CMP_OPS {
            let got = run_binary_rows(payload_tc, &a, &b, &no, &no, |a, b| LogicalInstr::Cmp { op, a, b });
            for (i, &(x, y)) in pairs.iter().enumerate() {
                let ord = if signed { x.cmp(&y) } else { (x as u64).cmp(&(y as u64)) };
                let want = cmp_want(op, ord, x == y);
                assert_eq!(got[i], (want, false), "{op:?} signed={signed} ({x}, {y})");
            }
        }
    }
}

#[test]
fn float_compare_is_ieee_so_every_nan_comparison_is_false() {
    let f = encode_f64;
    // The corpus is the set where IEEE differs from a total order: NaN is
    // unordered against everything including itself, and -0.0 == 0.0.
    let corpus = [0.0f64, -0.0, 1.5, -1.5, f64::INFINITY, f64::NEG_INFINITY, f64::NAN];
    let pairs: Vec<(f64, f64)> = corpus.iter().flat_map(|&x| corpus.map(|y| (x, y))).collect();
    let a: Vec<i64> = pairs.iter().map(|p| f(p.0)).collect();
    let b: Vec<i64> = pairs.iter().map(|p| f(p.1)).collect();
    let no = vec![false; pairs.len()];

    for op in CMP_OPS {
        let got = run_binary_rows(type_code::I64, &a, &b, &no, &no, |a, b| LogicalInstr::FCmp { op, a, b });
        for (i, &(x, y)) in pairs.iter().enumerate() {
            let want = match x.partial_cmp(&y) {
                Some(ord) => cmp_want(op, ord, x == y),
                // Unordered: only `!=` holds, every other comparison is false.
                None => i64::from(op == CmpOp::Ne),
            };
            assert_eq!(got[i], (want, false), "{op:?} ({x}, {y})");
        }
    }
}

#[test]
fn minmax2_skips_nulls_and_is_null_only_when_both_are() {
    let a = [1i64, 5, 9, 0, 0];
    let b = [2i64, 3, 0, 7, 0];
    let an = [false, false, false, true, true];
    let bn = [false, false, true, false, true];

    let max = run_binary_rows(type_code::I64, &a, &b, &an, &bn, |a, b| LogicalInstr::IntMinMax2 {
        a,
        b,
        is_max: true,
    });
    assert_eq!(max[0], (2, false));
    assert_eq!(max[1], (5, false));
    assert_eq!(max[2], (9, false), "b NULL -> a");
    assert_eq!(max[3], (7, false), "a NULL -> b");
    assert!(max[4].1, "both NULL -> NULL");

    let min = run_binary_rows(type_code::I64, &a, &b, &an, &bn, |a, b| LogicalInstr::IntMinMax2 {
        a,
        b,
        is_max: false,
    });
    assert_eq!(min[0], (1, false));
    assert_eq!(min[2], (9, false), "b NULL -> a, regardless of value");
    assert!(min[4].1);

    // The unsigned arm, which the signed corpus above cannot reach: as `u64`,
    // `u64::MAX` is the maximum, while read as `i64` the same bits are `-1` and
    // would come out the minimum. Only the column's type selects the arm.
    let (big, no) = ([u64::MAX as i64, u64::MAX as i64], [false, false]);
    let small = [1i64, 1];
    let umax = run_binary_rows(type_code::U64, &big, &small, &no, &no, |a, b| {
        LogicalInstr::IntMinMax2 { a, b, is_max: true }
    });
    assert_eq!(umax[0], (u64::MAX as i64, false), "unsigned MAX(u64::MAX, 1)");
    let umin = run_binary_rows(type_code::U64, &big, &small, &no, &no, |a, b| {
        LogicalInstr::IntMinMax2 { a, b, is_max: false }
    });
    assert_eq!(umin[0], (1, false), "unsigned MIN(u64::MAX, 1)");
}

#[test]
fn float_minmax2_uses_total_cmp_order() {
    let f = encode_f64;
    let a = [f(5.0), f(5.0), f(-0.0), f(f64::INFINITY)];
    let b = [f(f64::NAN), f(2.0), f(0.0), f(f64::NAN)];
    let no = [false; 4];

    let max = run_binary_rows(type_code::I64, &a, &b, &no, &no, |a, b| LogicalInstr::FloatMinMax2 {
        a,
        b,
        is_max: true,
    });
    assert!(decode_f64(max[0].0).is_nan(), "NaN is the total-order max");
    assert_eq!(decode_f64(max[1].0), 5.0);
    assert!(
        decode_f64(max[2].0).is_sign_positive(),
        "+0.0 beats -0.0 under total_cmp"
    );
    assert!(decode_f64(max[3].0).is_nan(), "NaN outranks +inf");

    let min = run_binary_rows(type_code::I64, &a, &b, &no, &no, |a, b| LogicalInstr::FloatMinMax2 {
        a,
        b,
        is_max: false,
    });
    assert_eq!(decode_f64(min[0].0), 5.0, "NaN loses MIN");
    assert!(decode_f64(min[2].0).is_sign_negative(), "-0.0 wins MIN under total_cmp");
}

// ---------------------------------------------------------------------------
// String registers
// ---------------------------------------------------------------------------

/// [`mixed_prog`] over one nullable STRING column (payload slot 0, column 1)
/// with per-row nulls; `mk` gets its loaded register.
fn str_prog(
    vals: &[&[u8]],
    nulls: &[bool],
    consts: Vec<Vec<u8>>,
    mk: impl Fn(Reg) -> Vec<LogicalInstr>,
) -> (Evaluator, TestView) {
    let (ev, mut view) = mixed_prog(&[vals], &[], consts, |r| mk(r[0]));
    for (row, &is_null) in nulls.iter().enumerate() {
        if is_null {
            view.set_null(row, 0);
        }
    }
    (ev, view)
}

/// A program over `strs.len()` nullable string columns followed by
/// `ints.len()` nullable I64 columns, each given as its column of values; `mk`
/// gets the loaded registers in that order and its last instruction is the
/// result.
fn mixed_prog(
    strs: &[&[&[u8]]],
    ints: &[&[i64]],
    consts: Vec<Vec<u8>>,
    mk: impl Fn(&[Reg]) -> Vec<LogicalInstr>,
) -> (Evaluator, TestView) {
    let rows = strs.first().map_or_else(|| ints[0].len(), |c| c.len());
    let mut cols = vec![(type_code::U64, false)];
    cols.extend(strs.iter().map(|_| (type_code::STRING, true)));
    cols.extend(ints.iter().map(|_| (type_code::I64, true)));
    let schema = TestSchema::new(&cols, &[0]);
    let mut view = TestView::new(rows, schema.pk_stride());
    push_payload_cols(&mut view, &schema);
    for row in 0..rows {
        set_row_pk(&mut view, &schema, row, row as u64 + 1);
        for (pi, col) in strs.iter().enumerate() {
            view.set_string(row, pi, col[row]);
        }
        for (k, col) in ints.iter().enumerate() {
            view.set_payload(row, strs.len() + k, &col[row].to_le_bytes());
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

/// Every row's *string* register, read back.
fn str_rows(ev: &Evaluator, view: &TestView, n: usize) -> Vec<(Vec<u8>, bool)> {
    (0..n).map(|i| row_str(ev, view, i)).collect()
}

fn run_str_rows(vals: &[&[u8]], nulls: &[bool], mk: impl Fn(Reg) -> Vec<LogicalInstr>) -> Vec<(Vec<u8>, bool)> {
    let (ev, view) = str_prog(vals, nulls, vec![], mk);
    str_rows(&ev, &view, vals.len())
}

/// The same, but reading a *scalar* register — LENGTH, LIKE, the compares, the
/// text→number parses.
fn run_str_to_scalar(vals: &[&[u8]], nulls: &[bool], mk: impl Fn(Reg) -> Vec<LogicalInstr>) -> Vec<Option<i64>> {
    let (ev, view) = str_prog(vals, nulls, vec![], mk);
    (0..vals.len()).map(|i| row_value(&ev, &view, i)).collect()
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
    let vals: Vec<Vec<u8>> = (0..N)
        .map(|c| {
            if c % 2 == 0 {
                format!("c{c}").into_bytes()
            } else {
                format!("column-{c}-is-past-twelve-bytes").into_bytes()
            }
        })
        .collect();
    let cells: Vec<&[u8]> = vals.iter().map(Vec::as_slice).collect();
    let view = make_string_view(&schema, &[cells.as_slice()]);

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
    let ev = scalar_prog(&schema, instrs, acc, vec![]);

    assert_eq!(
        ev.prog.str_cols,
        (1u64 << N) - 1,
        "every string column the program loads is registered at its own slot",
    );
    // Drive the loads alone and read back which buffer each lane resolved
    // against — the claim the concatenation below cannot make, since its own
    // result always lands in the arena.
    let mut scratch = EvalScratch::new(&ev.prog);
    drive(&ev.prog, &view, 0, 1, &mut scratch);
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

    assert_eq!(row_str(&ev, &view, 0).0, vals.concat());
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
    // The ASCII boundary bytes on both sides of `a-z`/`A-Z`, a multibyte UTF-8
    // sequence, and a lone continuation byte.
    let vals: &[&[u8]] = &[b"`az{", b"@AZ[", "straße".as_bytes(), &[0xC3, 0x9F, 0x80]];
    let up = run_str_rows(vals, &[false; 4], |a| vec![LogicalInstr::StrCase { a, upper: true }]);
    assert_eq!(up[0].0, b"`AZ{", "only a-z folds; the neighbours pass through");
    assert_eq!(up[1].0, b"@AZ[");
    // Documented deviation from PostgreSQL under a UTF-8 locale: ß is untouched.
    assert_eq!(up[2].0, "STRAßE".as_bytes());
    assert_eq!(up[3].0, &[0xC3, 0x9F, 0x80]);

    let lo = run_str_rows(vals, &[false; 4], |a| vec![LogicalInstr::StrCase { a, upper: false }]);
    assert_eq!(lo[0].0, b"`az{");
    assert_eq!(lo[1].0, b"@az[");
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
    for (i, want) in CELL_CLASSES.iter().enumerate() {
        assert_eq!(got[i].1, nulls[i], "row {i} nullness");
        if !nulls[i] {
            assert_eq!(&got[i].0, want, "UPPER then LOWER is the identity on ASCII");
        }
    }
}

#[test]
fn length_counts_characters_and_octets_separately() {
    // A combining mark, a ZWJ emoji, and bytes that are not UTF-8 at all — the
    // engine is byte-transparent, so the count must stay total.
    let vals: &[&[u8]] = &[
        b"abc",
        "e\u{0301}".as_bytes(),
        "\u{1F468}\u{200D}\u{1F469}\u{200D}\u{1F467}".as_bytes(),
        &[0xFF, 0xFE, 0x41],
    ];
    let chars = run_str_to_scalar(vals, &[false; 4], |a| vec![LogicalInstr::StrLen { a, chars: true }]);
    let bytes = run_str_to_scalar(vals, &[false; 4], |a| vec![LogicalInstr::StrLen { a, chars: false }]);
    // The emoji is 5 codepoints (three faces joined by two ZWJs) in 18 bytes —
    // the byte/character distinction OCTET_LENGTH exists to expose.
    assert_eq!(chars, [Some(3), Some(2), Some(5), Some(3)]);
    assert_eq!(bytes, [Some(3), Some(3), Some(18), Some(3)]);
}

#[test]
fn length_of_null_is_null() {
    let got = run_str_to_scalar(&[b"abc"], &[true], |a| vec![LogicalInstr::StrLen { a, chars: true }]);
    assert!(got[0].is_none());
}

/// The window rule is the totality proof: every endpoint is clamped in i128 to
/// `[1, N + 1]` before any narrowing, so no start/length pair can panic or read
/// out of the string.
#[test]
fn substring_window_matches_postgres_and_is_total() {
    let subst = |start: i64, len: Option<i64>| {
        let mut instrs = vec![LogicalInstr::LoadConst { val: start }];
        let len_reg = len.map(|l| {
            instrs.push(LogicalInstr::LoadConst { val: l });
            Reg(2)
        });
        instrs.push(LogicalInstr::StrSubstr { src: Reg(0), start_reg: Reg(1), len_reg });
        instrs
    };
    let one = |s: &[u8], start: i64, len: Option<i64>| {
        let r = run_str_rows(&[s], &[false], |_| subst(start, len));
        r[0].clone()
    };

    assert_eq!(one(b"abc", 1, None).0, b"abc");
    assert_eq!(one(b"abc", 2, None).0, b"bc");
    // A start at or past the end is empty, never a panic.
    assert_eq!(one(b"abc", 4, None).0, b"");
    // A start at or below 0 is the whole string: the upper clamp is N+1 because
    // the window is half-open, so clamping to N would lose the last character.
    assert_eq!(one(b"abc", -1, None).0, b"abc");
    assert_eq!(one(b"abc", 0, Some(2)).0, b"a");
    assert_eq!(one(b"abc", 1, Some(0)).0, b"");
    assert_eq!(one(b"abc", 2, Some(1)).0, b"b");
    // A negative length is NULL (PostgreSQL errors; here every domain error is
    // a NULL).
    assert!(one(b"abc", 1, Some(-1)).1);
    // Bounds near the i64 extremes: the i128 window absorbs the sum.
    assert_eq!(one(b"abc", i64::MAX, None).0, b"");
    assert_eq!(one(b"abc", i64::MIN, Some(i64::MAX)).0, b"");
    assert_eq!(one(b"abc", 1, Some(i64::MAX)).0, b"abc");
    // Windows are character units, not bytes. The clamp ceiling is the *byte*
    // length, which only bounds the character count, so a window that lands
    // between the two must still resolve to the string's end: "äöü" is 3
    // characters in 6 bytes, and starts/lengths in 4..=6 exercise that gap.
    assert_eq!(one("äöü".as_bytes(), 2, Some(1)).0, "ö".as_bytes());
    assert_eq!(one("äöü".as_bytes(), 2, None).0, "öü".as_bytes());
    assert_eq!(one("äöü".as_bytes(), 4, None).0, b"");
    assert_eq!(one("äöü".as_bytes(), 3, Some(5)).0, "ü".as_bytes());
    assert_eq!(one("äöü".as_bytes(), 1, Some(4)).0, "äöü".as_bytes());
    assert_eq!(one("äöü".as_bytes(), 5, Some(2)).0, b"");
    // A lone continuation byte belongs to no character, so the value has zero
    // characters and every window over it is empty — even `FROM 1`, which is the
    // identity on every well-formed value. The clamp ceiling is the byte length,
    // so this is the case that distinguishes it from the character count.
    assert_eq!(one(&[0x80], 1, None).0, b"");
    assert_eq!(one(&[0x80], i64::MIN, None).0, b"");
    assert_eq!(one(&[0x80, 0x80], 1, Some(1)).0, b"");
    // A continuation byte *after* a character start is part of that character.
    assert_eq!(one(&[0x80, b'a'], 1, None).0, b"a");
    // A heap-backed source yields a sub-view of the heap, not a copy.
    assert_eq!(one(b"abcdefghijklmnop", 14, Some(2)).0, b"no");
}

/// `StrSubstr` reads its `start` and `len` through `IntReg`, whose arm is chosen
/// by the register's U64 tracking. Every other substring test
/// drives the bounds from `LoadConst`, which is always signed-tracked, so the
/// unsigned arm is only reachable from a `U64` column — `SUBSTRING(s FROM ucol)`.
#[test]
fn substring_bounds_read_an_unsigned_register_as_unsigned() {
    let schema = TestSchema::new(
        &[
            (type_code::U64, false),
            (type_code::STRING, false),
            (type_code::U64, false),
        ],
        &[0],
    );
    let mut view = TestView::new(2, schema.pk_stride());
    push_payload_cols(&mut view, &schema);
    for row in 0..2 {
        set_row_pk(&mut view, &schema, row, row as u64 + 1);
        view.set_string(row, 0, b"abcdef");
    }
    // Row 0 takes a bound that fits either reading; row 1 takes one whose bits
    // are `-1` as `i64` and `u64::MAX` as `u64`. Signed, `-1` would open the
    // window before the string and yield a prefix; unsigned it is past the end.
    view.set_payload(0, 1, &3u64.to_le_bytes());
    view.set_payload(1, 1, &u64::MAX.to_le_bytes());

    let instrs = vec![
        LogicalInstr::LoadColStr { col: 1 },
        LogicalInstr::LoadColInt { col: 2 },
        LogicalInstr::StrSubstr {
            src: Reg(0),
            start_reg: Reg(1),
            len_reg: None,
        },
    ];
    let ev = scalar_prog(&schema, instrs, Reg(2), vec![]);
    assert_eq!(row_str(&ev, &view, 0).0, b"cdef");
    assert_eq!(
        row_str(&ev, &view, 1).0,
        b"",
        "an unsigned bound past the end yields the empty string, not a prefix"
    );
}

#[test]
fn substring_of_a_computed_string_is_a_sub_view_of_the_arena() {
    let got = run_str_rows(&[b"abcdefghijklmnop"], &[false], |a| {
        vec![
            LogicalInstr::StrCase { a, upper: true },
            LogicalInstr::LoadConst { val: 3 },
            LogicalInstr::LoadConst { val: 4 },
            LogicalInstr::StrSubstr {
                src: Reg(1),
                start_reg: Reg(2),
                len_reg: Some(Reg(3)),
            },
        ]
    });
    assert_eq!(got[0].0, b"CDEF");
}

#[test]
fn trim_strips_the_selected_ends_only() {
    let set = b"xy".to_vec();
    let vals: &[&[u8]] = &[b"xyaxybyx", b"xyxy", b"", b"abc"];
    let run = |mode: TrimMode| {
        let (ev, view) = str_prog(vals, &[false; 4], vec![set.clone()], |a| {
            vec![LogicalInstr::StrTrim { a, mode, set_idx: ConstIdx(0) }]
        });
        (0..vals.len()).map(|i| row_str(&ev, &view, i).0).collect::<Vec<_>>()
    };
    let (both, leading, trailing) = (run(TrimMode::Both), run(TrimMode::Leading), run(TrimMode::Trailing));
    assert_eq!(leading, [b"axybyx".to_vec(), b"".into(), b"".into(), b"abc".into()]);
    assert_eq!(trailing, [b"xyaxyb".to_vec(), b"".into(), b"".into(), b"abc".into()]);
    assert_eq!(both, [b"axyb".to_vec(), b"".into(), b"".into(), b"abc".into()]);
    // BOTH is exactly LEADING then TRAILING, including where the two overlap.
    for i in 0..vals.len() {
        let mut want = leading[i].clone();
        while want.last().is_some_and(|b| set.contains(b)) {
            want.pop();
        }
        assert_eq!(both[i], want, "row {i}");
    }
}

/// LIKE over both cell classes and a NULL row. A NULL row normalizes
/// whatever the matcher answered on its stored bytes — `'%'` matches anything —
/// so it reports `(0, true)` like every other producer.
#[test]
fn like_over_inline_and_heap_cells_with_a_null_row() {
    let vals: &[&[u8]] = &[b"abc", b"abcdefghijklm", b"xyz", b"abc"];
    let nulls = [false, false, false, true];
    let run = |pattern: &str, ci: bool| {
        let (ev, view) = str_prog(vals, &nulls, vec![pattern.as_bytes().to_vec()], |a| {
            vec![LogicalInstr::StrLike {
                src: a,
                escape: NonZeroU8::new(b'\\'),
                pat_idx: ConstIdx(0),
                ci,
            }]
        });
        (0..vals.len()).map(|i| row_value(&ev, &view, i)).collect::<Vec<_>>()
    };
    let null_row = None;
    assert_eq!(run("abc", false), [Some(1), Some(0), Some(0), null_row]);
    // A heap cell reached through its view, not its inline prefix.
    assert_eq!(run("%ijklm", false), [Some(0), Some(1), Some(0), null_row]);
    assert_eq!(run("%", false), [Some(1), Some(1), Some(1), null_row]);
    // ILIKE folds ASCII case; LIKE does not.
    assert_eq!(run("ABC", true), [Some(1), Some(0), Some(0), null_row]);
    assert_eq!(run("ABC", false), [Some(0), Some(0), Some(0), null_row]);
}

/// The two null rules are the whole difference between `||` and `CONCAT`, and
/// CONCAT's is asymmetric so a NULL accumulator still propagates.
#[test]
fn concat_null_rules_differ_by_operand_side() {
    let schema = schema_pk_strings(2, true);
    let rows: Vec<&[&[u8]]> = vec![&[b"ab", b"cd"], &[b"ab", b"cd"], &[b"ab", b"cd"]];
    let mut view = make_string_view(&schema, &rows);
    view.set_null(1, 0); // a NULL
    view.set_null(2, 1); // b NULL

    let run = |skip_null: bool| {
        let instrs = vec![
            LogicalInstr::LoadColStr { col: 1 },
            LogicalInstr::LoadColStr { col: 2 },
            LogicalInstr::StrConcat { a: Reg(0), b: Reg(1), skip_null },
        ];
        let ev = scalar_prog(&schema, instrs, Reg(2), vec![]);
        (0..3).map(|i| row_str(&ev, &view, i)).collect::<Vec<_>>()
    };

    let prop = run(false);
    assert_eq!(prop[0].0, b"abcd");
    assert!(prop[1].1 && prop[2].1, "|| propagates NULL from either side");

    let skip = run(true);
    assert_eq!(skip[0].0, b"abcd");
    assert!(skip[1].1, "a NULL accumulator still propagates");
    assert_eq!(skip[2].0, b"ab", "a NULL argument contributes the empty string");
    assert!(!skip[2].1);
}

#[test]
fn concat_is_classified_null_producing_so_the_no_nulls_arm_cannot_take_it() {
    // Non-nullable columns: everything else about the program is `no_nulls`, but
    // the u32::MAX length verdict needs a null word to record itself in.
    let schema = schema_pk_strings(2, false);
    let concat = scalar_prog(
        &schema,
        vec![
            LogicalInstr::LoadColStr { col: 1 },
            LogicalInstr::LoadColStr { col: 2 },
            LogicalInstr::StrConcat { a: Reg(0), b: Reg(1), skip_null: false },
        ],
        Reg(2),
        vec![],
    );
    assert!(!concat.prog.no_nulls);

    let upper = scalar_prog(
        &schema,
        vec![
            LogicalInstr::LoadColStr { col: 1 },
            LogicalInstr::StrCase { a: Reg(0), upper: true },
        ],
        Reg(1),
        vec![],
    );
    assert!(upper.prog.no_nulls, "a pure transform keeps the no_nulls arm");
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
/// four bytes, at the 12-byte inline/heap threshold, and past it.
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
    const OPS: [CmpOp; 6] = [CmpOp::Eq, CmpOp::Ne, CmpOp::Gt, CmpOp::Ge, CmpOp::Lt, CmpOp::Le];
    let mut blob = Vec::new();
    let cells: Vec<[u8; 16]> = corpus
        .iter()
        .map(|s| gnitz_wire::encode_german_string(s, &mut blob))
        .collect();

    for nullable in [false, true] {
        let schema = schema_pk_strings(2, nullable);
        // The two column-addressed channels depend only on the operator, so they are
        // built once and driven over every pair rather than rebuilt inside the loop.
        let regs = OPS.map(|op| {
            scalar_prog(
                &schema,
                vec![
                    LogicalInstr::LoadColStr { col: 1 },
                    LogicalInstr::LoadColStr { col: 2 },
                    LogicalInstr::StrCmp { op, a: Reg(0), b: Reg(1) },
                ],
                Reg(2),
                vec![],
            )
        });
        let col_col = OPS.map(|op| {
            scalar_prog(
                &schema,
                vec![LogicalInstr::StrColCol { op, col_a: 1, col_b: 2 }],
                Reg(0),
                vec![],
            )
        });

        for (j, b) in corpus.iter().enumerate() {
            // The constant is baked into the program, so this channel alone is
            // rebuilt per right-hand value.
            let col_const = OPS.map(|op| {
                scalar_prog(
                    &schema,
                    vec![LogicalInstr::StrColConst { op, col: 1, const_idx: ConstIdx(0) }],
                    Reg(0),
                    vec![b.to_vec()],
                )
            });
            for (i, a) in corpus.iter().enumerate() {
                let view = make_string_view(&schema, &[&[a, b]]);
                let want = gnitz_wire::compare_german_strings(&cells[i], &blob, &cells[j], &blob);
                let want = [
                    want.is_eq() as i64,
                    want.is_ne() as i64,
                    want.is_gt() as i64,
                    want.is_ge() as i64,
                    want.is_lt() as i64,
                    want.is_le() as i64,
                ];
                for (channel, evs) in [("StrCmp", &regs), ("StrColCol", &col_col), ("StrColConst", &col_const)] {
                    let got: Vec<i64> = evs
                        .iter()
                        .map(|ev| row_value(ev, &view, 0).expect("not NULL"))
                        .collect();
                    assert_eq!(got, want, "nullable={nullable} {channel}: {a:?} vs {b:?}");
                }
            }
        }
    }
}

/// A constant past `SHORT_STRING_THRESHOLD` has no inline content: its cell
/// carries a heap offset into the program's own constant arena, not into the
/// batch's blob. Two constants, so the second sits at a non-zero offset, and no
/// row holds either one at the offset its cell names — so resolving a constant
/// against the batch's blob lands on unrelated bytes rather than matching by
/// luck.
#[test]
fn heap_backed_constants_at_two_arena_offsets() {
    let schema = schema_pk_strings(2, false);
    let a: &[u8] = b"alpha-value-past-twelve";
    let b: &[u8] = b"bravo-value-past-twelve-and-then-some";
    let vals: Vec<[&[u8]; 2]> = vec![
        [b"zulu-value-past-twelve", b"zulu-value-past-twelve"],
        [a, b],
        [a, b"short"],
        [b"short", b],
    ];
    let rows: Vec<&[&[u8]]> = vals.iter().map(|r| &r[..]).collect();
    let view = make_string_view(&schema, &rows);
    let ev = filter_prog(
        &schema,
        vec![
            LogicalInstr::StrColConst {
                op: CmpOp::Eq,
                col: 1,
                const_idx: ConstIdx(0),
            },
            LogicalInstr::StrColConst {
                op: CmpOp::Eq,
                col: 2,
                const_idx: ConstIdx(1),
            },
            LogicalInstr::BoolBinary { is_or: false, a: Reg(0), b: Reg(1) },
        ],
        Reg(2),
        vec![a.to_vec(), b.to_vec()],
    );
    assert_eq!(passing_rows(&ev, &view), vec![false, true, false, false]);
}

/// A cell index is not a const-pool index. `resolve` encodes a cell only for
/// the constants a `StrColConst` names, and numbers them by first reference —
/// so `WHERE pk IN (2,3) AND t = 'b' AND s = 'a'` puts the packed `IN` set at
/// pool 0 with no cell, and pool 2 ahead of pool 1 in `const_cells`. Every
/// index here differs from the pool index it came from, so a resolver that
/// passed the pool index through would read past a two-element vector.
#[test]
fn a_cell_index_is_dense_over_the_constants_the_fused_compare_names() {
    let schema = schema_pk_strings(2, false);
    let a: &[u8] = b"alpha-value-past-twelve";
    let b: &[u8] = b"bravo-value-past-twelve";
    // PKs are 1..=4; the set admits rows 1 and 2.
    let vals: Vec<[&[u8]; 2]> = vec![[a, b], [a, b], [a, b"short"], [b"short", b]];
    let rows: Vec<&[&[u8]]> = vals.iter().map(|r| &r[..]).collect();
    let view = make_string_view(&schema, &rows);
    let set: Vec<u8> = [2i64, 3].iter().flat_map(|v| v.to_le_bytes()).collect();
    let ev = filter_prog(
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
    assert_eq!(passing_rows(&ev, &view), vec![false, true, false, false]);
}

#[test]
fn int_to_text_reads_the_source_signedness_from_the_register_tracking() {
    // A U64 column above i64::MAX has a negative i64 bit pattern, so the
    // resolve-time tracking is the only thing that keeps the text unsigned.
    let schema = TestSchema::new(
        &[(type_code::U64, false), (type_code::U64, true), (type_code::I64, true)],
        &[0],
    );
    // Row 1 pins the digit loop's own edges: zero is the one magnitude with no
    // significant digit, and -7 is the one-digit negative.
    let view = make_int_view(&schema, &[(1, 0, &[u64::MAX as i64, i64::MIN]), (2, 0, &[0, -7])]);
    let text = |col: u32, row: usize| {
        let ev = scalar_prog(
            &schema,
            vec![LogicalInstr::LoadColInt { col }, LogicalInstr::IntToStr { a: Reg(0) }],
            Reg(1),
            vec![],
        );
        row_str(&ev, &view, row).0
    };
    assert_eq!(text(1, 0), u64::MAX.to_string().as_bytes());
    assert_eq!(text(2, 0), i64::MIN.to_string().as_bytes());
    assert_eq!(text(1, 1), b"0");
    assert_eq!(text(2, 1), b"-7");
}

/// The magnitude switch is what bounds the output: Rust's positional `Display`
/// renders `1e300` as 301 digits. Every value must also survive the round trip
/// back through the parse.
#[test]
fn float_to_text_is_bounded_and_round_trips() {
    let schema = TestSchema::new(&[(type_code::U64, false), (type_code::F64, true)], &[0]);
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
    let rows: Vec<(u64, u64, Vec<i64>)> = vals
        .iter()
        .enumerate()
        .map(|(i, &f)| (i as u64 + 1, 0, vec![encode_f64(f)]))
        .collect();
    let row_refs: Vec<(u64, u64, &[i64])> = rows.iter().map(|(p, n, v)| (*p, *n, v.as_slice())).collect();
    let view = make_int_view(&schema, &row_refs);

    let ev = scalar_prog(
        &schema,
        vec![
            LogicalInstr::LoadColFloat { col: 1 },
            LogicalInstr::FloatToStr { a: Reg(0) },
        ],
        Reg(1),
        vec![],
    );
    for (i, &f) in vals.iter().enumerate() {
        let text = String::from_utf8(row_str(&ev, &view, i).0).expect("decimal text is ASCII");
        assert!(text.len() <= 24, "{f} rendered {} bytes: {text}", text.len());
        // PostgreSQL's spelling for the non-finite values, not Rust's `inf`.
        match f {
            f if f.is_nan() => assert_eq!(text, "NaN"),
            f64::INFINITY => assert_eq!(text, "Infinity"),
            f64::NEG_INFINITY => assert_eq!(text, "-Infinity"),
            _ => assert_eq!(
                text.parse::<f64>().unwrap().to_bits(),
                f.to_bits(),
                "{f} must round-trip bit-exactly through {text}"
            ),
        }
    }
    // The sign of zero survives, which is what keeps a retraction cancelling.
    assert_eq!(row_str(&ev, &view, 1).0, b"-0");
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
    let vals: Vec<&[u8]> = cases.iter().map(|(s, _)| *s).collect();
    let got = run_str_to_scalar(&vals, &vec![false; cases.len()], |a| {
        vec![LogicalInstr::StrToInt { a, fi: FixedInt::I64 }]
    });
    for (i, (s, want)) in cases.iter().enumerate() {
        match want {
            Some(v) => assert_eq!(got[i], Some(*v), "{s:?}"),
            None => assert!(got[i].is_none(), "{s:?} must be NULL"),
        }
    }

    // Range-checked against the *target*, not i64.
    let narrow = run_str_to_scalar(&[b"127", b"128", b"-128", b"-129"], &[false; 4], |a| {
        vec![LogicalInstr::StrToInt { a, fi: FixedInt::I8 }]
    });
    assert_eq!(
        narrow.iter().map(|r| r.is_none()).collect::<Vec<_>>(),
        [false, true, false, true]
    );
}

/// A U64 target must re-seed the register's unsigned tracking, or every
/// downstream ordered compare picks the signed variant on a value above 2^63.
#[test]
fn text_to_u64_seeds_the_unsigned_tracking() {
    let schema = schema_pk_strings(1, true);
    let view = make_string_view(&schema, &[&[u64::MAX.to_string().as_bytes()]]);
    let cmp = |fi: FixedInt| {
        let ev = scalar_prog(
            &schema,
            vec![
                LogicalInstr::LoadColStr { col: 1 },
                LogicalInstr::StrToInt { a: Reg(0), fi },
                LogicalInstr::LoadConst { val: 5 },
                LogicalInstr::Cmp { op: CmpOp::Gt, a: Reg(1), b: Reg(2) },
            ],
            Reg(3),
            vec![],
        );
        row_value(&ev, &view, 0)
    };
    assert_eq!(cmp(FixedInt::U64), Some(1), "u64::MAX > 5 under unsigned order");
    // The same text does not fit I64 at all, so the parse itself NULLs the row —
    // there is no signed reading of this value to compare wrongly.
    assert!(cmp(FixedInt::I64).is_none());
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
    let vals: Vec<&[u8]> = cases.iter().map(|(s, _)| *s).collect();
    let got = run_str_to_scalar(&vals, &vec![false; cases.len()], |a| {
        vec![LogicalInstr::StrToFloat { a }]
    });
    for (i, (s, want)) in cases.iter().enumerate() {
        match want {
            Some(f) => assert_eq!(got[i].map(decode_f64), Some(*f), "{s:?}"),
            None => assert!(got[i].is_none(), "{s:?} must be NULL"),
        }
    }
    // Invalid UTF-8 is NULL, not a panic: the engine is byte-transparent.
    let bad = run_str_to_scalar(&[&[0xFF, 0xFE]], &[false], |a| vec![LogicalInstr::StrToFloat { a }]);
    assert!(bad[0].is_none());
}

/// A morsel-crossing run: the arena is truncated back to its constant prefix at
/// the top of every morsel, so a lane that survived into the next one would read
/// another row's bytes.
#[test]
fn computed_strings_do_not_leak_across_morsels() {
    let n = MORSEL + 7;
    let schema = schema_pk_strings(1, false);
    let owned: Vec<Vec<u8>> = (0..n)
        .map(|i| format!("row{i}-abcdefghijklmnop").into_bytes())
        .collect();
    let cells: Vec<&[u8]> = owned.iter().map(Vec::as_slice).collect();
    let rows: Vec<&[&[u8]]> = cells.iter().map(std::slice::from_ref).collect();
    let view = make_string_view(&schema, &rows);

    let ev = scalar_prog(
        &schema,
        vec![
            LogicalInstr::LoadColStr { col: 1 },
            LogicalInstr::StrCase { a: Reg(0), upper: true },
        ],
        Reg(1),
        vec![],
    );
    let mut seen = Vec::new();
    ev.eval_morsels(&view, 0, n, |start, out| {
        for i in 0..out.rows() {
            seen.push((start + i, out.str_bytes(1, i).to_vec()));
        }
    });
    assert_eq!(seen.len(), n);
    for (row, bytes) in seen {
        assert_eq!(bytes, owned[row].to_ascii_uppercase(), "row {row}");
    }
}

/// A const view lives in the arena's non-cleared prefix, so it must survive
/// every morsel reset intact.
#[test]
fn string_constants_survive_the_per_morsel_arena_reset() {
    let n = MORSEL + 3;
    let schema = schema_pk_strings(1, false);
    let view = make_string_view(&schema, &vec![&[b"x".as_slice()][..]; n]);
    let ev = scalar_prog(
        &schema,
        vec![LogicalInstr::LoadConstStr { const_idx: ConstIdx(0) }],
        Reg(0),
        vec![b"constant-value".to_vec()],
    );
    let mut rows = 0usize;
    ev.eval_morsels(&view, 0, n, |_, out| {
        for i in 0..out.rows() {
            assert_eq!(out.str_bytes(0, i), b"constant-value");
            rows += 1;
        }
    });
    assert_eq!(rows, n);
}

/// The blend must carry the *chosen* branch's null bit, not the union — that is
/// what makes `COALESCE(s, 'default')` yield the default rather than NULL.
#[test]
fn string_select_takes_the_chosen_branch_and_its_null_bit() {
    let schema = schema_pk_strings(2, true);
    // PKs are 1..=3; `make_string_view` assigns them in row order.
    let rows: Vec<&[&[u8]]> = vec![&[b"yes", b"no"], &[b"yes", b"no"], &[b"yes", b"no"]];
    let mut view = make_string_view(&schema, &rows);
    view.set_null(0, 0); // row 0: the taken branch (`a`) is NULL
    view.set_null(2, 1); // row 2: the untaken branch (`b`) is NULL

    // cond = (pk != 2): rows 0 and 2 take `a`, row 1 takes `b`.
    let ev = scalar_prog(
        &schema,
        vec![
            LogicalInstr::LoadColInt { col: 0 },
            LogicalInstr::LoadConst { val: 2 },
            LogicalInstr::Cmp { op: CmpOp::Ne, a: Reg(0), b: Reg(1) },
            LogicalInstr::LoadColStr { col: 1 },
            LogicalInstr::LoadColStr { col: 2 },
            LogicalInstr::StrSelect { cond: Reg(2), a: Reg(3), b: Reg(4) },
        ],
        Reg(5),
        vec![],
    );
    let got: Vec<(Vec<u8>, bool)> = (0..3).map(|i| row_str(&ev, &view, i)).collect();
    assert!(got[0].1, "row 0 takes the NULL branch");
    assert_eq!(got[1].0, b"no", "row 1 takes the else branch");
    assert!(!got[1].1);
    assert_eq!(got[2].0, b"yes", "row 2's NULL is on the branch not taken");
    assert!(!got[2].1);
}

/// The payload twin of [`pk_loads_agree_with_the_wire_opk_decoder`]:
/// `load_int!` re-spells `FixedInt::decode_le_i64`'s eight-arm widening, and a
/// divergence is a silently wrong *value*, not a crash.
#[test]
fn payload_loads_agree_with_the_wire_le_decoder() {
    for (tc, fi) in PK_WIDTH_MATRIX {
        // PK column 0 keeps the schema well-formed; column 1 is the subject.
        let schema = TestSchema::new(&[(type_code::U64, false), (tc, false)], &[0]);
        let vals: [i64; 6] = [0, 1, -1, i64::MIN, i64::MAX, 0x0123_4567_89ab_cdef];
        let rows: Vec<(u64, u64, &[i64])> = vals.iter().map(|v| (0u64, 0u64, std::slice::from_ref(v))).collect();
        let view = make_int_view(&schema, &rows);

        let ev = scalar_prog(&schema, vec![LogicalInstr::LoadColInt { col: 1 }], Reg(0), vec![]);
        for row in 0..vals.len() {
            let got = row_value(&ev, &view, row).expect("a non-nullable column is never null");
            let want = fi.decode_le_i64(view.get_col_ptr(row, 0, fi.width()));
            assert_eq!(
                got, want,
                "type {tc}, row {row}: load_int! disagrees with decode_le_i64"
            );
        }
    }
}

/// Every `(type_code, FixedInt)` pair the two load kernels must cover.
const PK_WIDTH_MATRIX: [(u8, FixedInt); 8] = [
    (type_code::U8, FixedInt::U8),
    (type_code::I8, FixedInt::I8),
    (type_code::U16, FixedInt::U16),
    (type_code::I16, FixedInt::I16),
    (type_code::U32, FixedInt::U32),
    (type_code::I32, FixedInt::I32),
    (type_code::U64, FixedInt::U64),
    (type_code::I64, FixedInt::I64),
];

/// `LoadPk`'s kernel is a fourth spelling of the OPK→i64 inverse: it reads
/// fixed-width `&[u8; W]` arrays so each width is one load plus a byte swap,
/// where `gnitz_wire::decode_opk_i64` takes the width as a slice length. The two
/// must agree for every width and both signednesses — a divergence here is a
/// silently wrong *value*, not a crash — and `decode_opk_i64` is itself
/// cross-checked against the wire crate's other two spellings, so pinning
/// against it puts this kernel inside that same check.
#[test]
fn pk_loads_agree_with_the_wire_opk_decoder() {
    for (tc, fi) in PK_WIDTH_MATRIX {
        // A single-column PK of this type, plus one payload column so the schema
        // has a slot; the values sweep both sign extremes and the midpoint.
        let schema = TestSchema::new(&[(tc, false), (type_code::I64, false)], &[0]);
        let vals: [u64; 6] = [0, 1, u64::MAX, 1 << 63, (1 << 63) - 1, 0x0123_4567_89ab_cdef];
        let rows: Vec<(u64, u64, &[i64])> = vals.iter().map(|&v| (v, 0u64, &[0i64][..])).collect();
        let view = make_int_view(&schema, &rows);

        let ev = scalar_prog(&schema, vec![LogicalInstr::LoadColInt { col: 0 }], Reg(0), vec![]);
        for row in 0..vals.len() {
            let got = row_value(&ev, &view, row).expect("a PK column is never null");
            let want = gnitz_wire::decode_opk_i64(&view.get_pk_bytes(row)[..fi.width()], fi);
            assert_eq!(got, want, "type {tc}, row {row}: LoadPk disagrees with decode_opk_i64");
        }
    }
}

// ---------------------------------------------------------------------------
// The register null test
// ---------------------------------------------------------------------------

/// `IS [NOT] NULL` over a register reads its null lane whatever class the
/// register is, and its own result is never NULL.
#[test]
fn is_null_reg_reads_the_null_lane_of_either_class() {
    for invert in [false, true] {
        let got = run_unary_rows(type_code::I64, &[5, 0], &[false, true], |a| LogicalInstr::IsNullReg {
            a,
            invert,
        });
        let want = |is_null: bool| ((is_null ^ invert) as i64, false);
        assert_eq!(got, [want(false), want(true)], "invert = {invert}");
        let got = run_str_to_scalar(&[b"abc", b""], &[false, true], |a| {
            vec![LogicalInstr::IsNullReg { a, invert }]
        });
        assert_eq!(
            got,
            [Some((invert) as i64), Some((!invert) as i64)],
            "string operand, invert = {invert}"
        );
    }
}

/// On the `no_nulls` arm no lane exists and nothing is NULL: the answer is the
/// constant, read off the value lane the kernel filled.
#[test]
fn is_null_reg_on_the_no_nulls_arm_is_the_constant() {
    let schema = TestSchema::new(&[(type_code::U64, false), (type_code::I64, false)], &[0]);
    let view = make_n_col_view(&schema, 3, |row, _| row as i64, |_, _| false);
    for invert in [false, true] {
        let prog = resolved(
            &schema,
            vec![
                LogicalInstr::LoadColInt { col: 1 },
                LogicalInstr::IsNullReg { a: Reg(0), invert },
            ],
            Reg(1),
        );
        assert!(prog.no_nulls);
        let mut scratch = EvalScratch::new(&prog);
        drive(&prog, &view, 0, 3, &mut scratch);
        assert_eq!(&scratch.regs[MORSEL..MORSEL + 3], &[invert as i64; 3]);
    }
}

// ---------------------------------------------------------------------------
// SIGN, the transcendentals and POWER
// ---------------------------------------------------------------------------

/// SIGN over an unsigned register reads it as never negative — the `signed`
/// flag the resolve-time tracking sets — and over a float keeps the domain.
#[test]
fn sign_reads_its_operand_signedness_and_keeps_the_float_domain() {
    let signed = run_unary_rows(type_code::I64, &[-5, 0, 7, i64::MIN], &[false; 4], |a| {
        LogicalInstr::IntUnary { op: IntUnaryOp::Sign, a }
    });
    assert_eq!(signed, [(-1, false), (0, false), (1, false), (-1, false)]);
    let unsigned = run_unary_rows(type_code::U64, &[u64::MAX as i64, 0], &[false; 2], |a| {
        LogicalInstr::IntUnary { op: IntUnaryOp::Sign, a }
    });
    assert_eq!(unsigned, [(1, false), (0, false)]);
    let vals: Vec<i64> = [-2.5, 0.0, -0.0, 3.0, f64::NAN]
        .iter()
        .map(|&f| encode_f64(f))
        .collect();
    let got = run_unary_rows(type_code::I64, &vals, &[false; 5], |a| LogicalInstr::FloatUnary {
        op: FloatUnaryOp::Sign,
        a,
    });
    let got: Vec<f64> = got.iter().map(|&(v, _)| decode_f64(v)).collect();
    assert_eq!(&got[..4], &[-1.0, 0.0, 0.0, 1.0]);
    assert!(got[4].is_nan(), "SIGN(NaN) is NaN, not a sign");
}

/// Every transcendental is its operand's IEEE result — a domain error is NaN
/// or an infinity, never a NULL, which is what keeps them off the null-making
/// list.
#[test]
fn transcendentals_and_power_are_the_ieee_result() {
    let f = |op: FloatUnaryOp, x: f64| -> (f64, bool) {
        let [(v, null)] = run_unary_rows(type_code::I64, &[encode_f64(x)], &[false], |a| {
            LogicalInstr::FloatUnary { op, a }
        })[..] else {
            unreachable!()
        };
        (decode_f64(v), null)
    };
    assert_eq!(f(FloatUnaryOp::Sqrt, 4.0), (2.0, false));
    assert!(f(FloatUnaryOp::Sqrt, -1.0).0.is_nan());
    assert_eq!(f(FloatUnaryOp::Ln, 0.0), (f64::NEG_INFINITY, false));
    assert_eq!(f(FloatUnaryOp::Ln, std::f64::consts::E), (1.0, false));
    assert_eq!(f(FloatUnaryOp::Log10, 1000.0), (3.0, false));
    assert_eq!(f(FloatUnaryOp::Exp, 0.0), (1.0, false));
    let got = run_binary_rows(
        type_code::I64,
        &[encode_f64(2.0), encode_f64(0.0)],
        &[encode_f64(10.0), encode_f64(-1.0)],
        &[false; 2],
        &[false; 2],
        |a, b| LogicalInstr::FloatArith { op: FloatArithOp::Pow, a, b },
    );
    assert_eq!(decode_f64(got[0].0), 1024.0);
    assert_eq!(decode_f64(got[1].0), f64::INFINITY);
    assert!(!got[1].1, "0 ^ -1 is an infinity, not NULL");
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
    let run = |vals: &[&[u8]], counts: &[i64], left: bool| {
        let (ev, view) = mixed_prog(&[vals], &[counts], vec![], |r| {
            vec![LogicalInstr::StrSide { src: r[0], n_reg: r[1], left }]
        });
        str_rows(&ev, &view, vals.len())
            .into_iter()
            .map(|(v, _)| v)
            .collect::<Vec<_>>()
    };
    let want = |ss: [&str; 5]| ss.iter().map(|x| x.as_bytes().to_vec()).collect::<Vec<_>>();
    assert_eq!(run(s, n, true), want(["hé", "héll", "héllo", "", ""]));
    assert_eq!(run(s, n, false), want(["lo", "éllo", "héllo", "", ""]));

    // Bytes with no character start at all, and a count past the total. `0x80`
    // is a continuation byte, so it belongs to the character before it and the
    // last row is two characters, not three; a string of nothing but
    // continuation bytes has no character at all and RIGHT drops all of it.
    let odd: &[&[u8]] = &[
        &[0x80, 0x80],
        &[0x80, 0x80],
        b"",
        &[0xFF, b'a', 0x80],
        &[0xFF, b'a', 0x80],
    ];
    let counts: &[i64] = &[1, 9, 3, 1, 9];
    assert_eq!(
        run(odd, counts, false),
        vec![vec![], vec![], vec![], vec![b'a', 0x80], vec![0xFF, b'a', 0x80]],
    );
}

/// `StrSide` is the one string producer instantiated infallible, so it merges no
/// fail mask — not even a provably-zero one. Both arms must still answer the
/// same, and the `no_nulls` one must produce no NULL at all, since a dropped
/// fail flag there is exactly what the instantiation asserts cannot happen.
#[test]
fn right_is_infallible_and_agrees_across_the_arms() {
    let schema = schema_pk_strings(1, false);
    let vals: Vec<&[u8]> = vec!["héllo wörld".as_bytes(), b"", b"abc", &[0x80, 0xFF, b'z']];
    let rows: Vec<&[&[u8]]> = vals.iter().map(std::slice::from_ref).collect();
    let view = make_string_view(&schema, &rows);
    let instrs = vec![
        LogicalInstr::LoadColStr { col: 1 },
        LogicalInstr::LoadConst { val: 2 },
        LogicalInstr::StrSide { src: Reg(0), n_reg: Reg(1), left: false },
    ];
    let (fast, nullable) = both_arms("str_side", || scalar_prog(&schema, instrs.clone(), Reg(2), vec![]));
    let read = |ev: &Evaluator| str_rows(ev, &view, vals.len());
    let got = read(&fast);
    assert_eq!(got, read(&nullable), "the arms disagree");
    assert!(!got.iter().any(|&(_, is_null)| is_null), "RIGHT never makes a NULL");
    // The last fixture leads with a continuation byte, which begins no
    // character of its own: the value is the two characters `0xFF` and `z`, so
    // RIGHT(2) keeps both and the leading byte falls off.
    let want: Vec<&[u8]> = vec!["ld".as_bytes(), b"", b"bc", &[0xFF, b'z']];
    assert_eq!(got.iter().map(|(v, _)| v.as_slice()).collect::<Vec<_>>(), want);
}

/// STRPOS is a 1-based *character* index, 0 when absent, 1 for an empty needle.
#[test]
fn strpos_is_a_character_index() {
    let hay: &[&[u8]] = &["héllo".as_bytes(); 4];
    let needle: &[&[u8]] = &[b"l", b"", b"z", "éll".as_bytes()];
    let (ev, view) = mixed_prog(&[hay, needle], &[], vec![], |r| {
        vec![LogicalInstr::StrPos { hay: r[0], needle: r[1] }]
    });
    let got: Vec<Option<i64>> = (0..4).map(|i| row_value(&ev, &view, i)).collect();
    assert_eq!(got, [Some(3), Some(1), Some(0), Some(2)]);
}

/// REVERSE reverses characters, so a multibyte sequence stays intact.
#[test]
fn reverse_reverses_characters_not_bytes() {
    let got = run_str_rows(&["héllo".as_bytes(), b"", b"a"], &[false; 3], |a| {
        vec![LogicalInstr::StrReverse { a }]
    });
    assert_eq!(got[0].0, "olléh".as_bytes());
    assert_eq!(got[1].0, b"");
    assert_eq!(got[2].0, b"a");
}

/// REPLACE rewrites every non-overlapping occurrence left to right; an empty
/// or absent pattern passes the subject through unchanged.
#[test]
fn replace_rewrites_every_occurrence_left_to_right() {
    let s: &[&[u8]] = &[b"aXbXc", b"abc", b"aaa", b"abc", b"XX"];
    let from: &[&[u8]] = &[b"X", b"", b"aa", b"z", b"X"];
    let to: &[&[u8]] = &[b"--", b"z", b"b", b"q", b""];
    let (ev, view) = mixed_prog(&[s, from, to], &[], vec![], |r| {
        vec![LogicalInstr::StrReplace { s: r[0], from: r[1], to: r[2] }]
    });
    let got: Vec<Vec<u8>> = str_rows(&ev, &view, 5).into_iter().map(|(v, _)| v).collect();
    assert_eq!(
        got,
        [
            b"a--b--c".to_vec(),
            b"abc".into(),
            b"ba".into(),
            b"abc".into(),
            b"".into()
        ]
    );
}

/// The subject and the replacement may both live in the arena the kernel is
/// growing: the views are re-resolved across every push.
#[test]
fn replace_over_arena_operands_rewrites_correctly() {
    let s: &[&[u8]] = &[b"axbxc"];
    let from: &[&[u8]] = &[b"X"];
    let to: &[&[u8]] = &[b"yy"];
    let (ev, view) = mixed_prog(&[s, from, to], &[], vec![], |r| {
        vec![
            LogicalInstr::StrCase { a: r[0], upper: true },
            LogicalInstr::StrCase { a: r[2], upper: true },
            LogicalInstr::StrReplace { s: Reg(3), from: r[1], to: Reg(4) },
        ]
    });
    assert_eq!(row_str(&ev, &view, 0).0, b"AYYBYYC");
}

/// LPAD/RPAD measure in characters, cycle the fill, truncate a longer subject
/// to its first `n` characters, and pad nothing for an empty fill.
#[test]
fn pad_measures_characters_and_truncates_a_long_subject() {
    let s: &[&[u8]] = &["hé".as_bytes(), "hé".as_bytes(), b"hello", b"a", b"a", b"a"];
    let n: &[i64] = &[5, 5, 3, 0, 3, -2];
    let fill: &[&[u8]] = &[b"xy", "éx".as_bytes(), b"x", b"x", b"", b"x"];
    let run = |left: bool| {
        let (ev, view) = mixed_prog(&[s, fill], &[n], vec![], |r| {
            vec![LogicalInstr::StrPad { s: r[0], n_reg: r[2], fill: r[1], left }]
        });
        str_rows(&ev, &view, 6).into_iter().map(|(v, _)| v).collect::<Vec<_>>()
    };
    let want = |ss: [&str; 6]| ss.iter().map(|x| x.as_bytes().to_vec()).collect::<Vec<_>>();
    assert_eq!(run(true), want(["xyxhé", "éxéhé", "hel", "", "a", ""]));
    assert_eq!(run(false), want(["héxyx", "hééxé", "hel", "", "a", ""]));
}

/// SPLIT_PART indexes fields from either end, is empty past the last field,
/// treats an empty delimiter as one field, and is NULL for field zero.
#[test]
fn split_part_indexes_fields_from_either_end_and_nulls_on_zero() {
    let s: &[&[u8]] = &[b"a,b,c".as_slice(); 8];
    let d: &[&[u8]] = &[b",", b",", b",", b",", b",", b"", b"", b","];
    let n: &[i64] = &[2, -1, 5, -5, 1, 1, 2, 0];
    let (ev, view) = mixed_prog(&[s, d], &[n], vec![], |r| {
        vec![LogicalInstr::StrSplitPart { s: r[0], delim: r[1], n_reg: r[2] }]
    });
    let got = str_rows(&ev, &view, 8);
    let want: Vec<(Vec<u8>, bool)> = [
        (b"b".to_vec(), false),
        (b"c".to_vec(), false),
        (b"".to_vec(), false),
        (b"".to_vec(), false),
        (b"a".to_vec(), false),
        (b"a,b,c".to_vec(), false),
        (b"".to_vec(), false),
        (b"".to_vec(), true),
    ]
    .into_iter()
    .collect();
    assert_eq!(got, want);
}

/// A NULL operand of any class makes the producer's row NULL — the shared
/// propagation over the mixed operand list.
#[test]
fn string_producers_propagate_a_null_operand_of_either_class() {
    let s: &[&[u8]] = &[b"abc", b"abc"];
    let n: &[i64] = &[1, 1];
    let (ev, mut view) = mixed_prog(&[s], &[n], vec![], |r| {
        vec![LogicalInstr::StrSide { src: r[0], n_reg: r[1], left: true }]
    });
    view.set_null(0, 0);
    view.set_null(1, 1);
    assert!(row_str(&ev, &view, 0).1, "NULL subject");
    assert!(row_str(&ev, &view, 1).1, "NULL count");
}

/// A scalar program's result register is read as a value, so a word-level
/// boolean producer (`NOT`, the register null test) must unpack into it even
/// though nothing else reads the register.
#[test]
fn a_word_level_boolean_scalar_result_is_unpacked() {
    let got = run_unary_rows(type_code::I64, &[5, 0, 3], &[false, false, true], |a| {
        LogicalInstr::BoolNot { a }
    });
    assert_eq!(got, [(0, false), (1, false), (0, true)]);
}

/// `IS [NOT] NULL` over a PK column, which is never NULL, resolves to the
/// constant the kernel fills for a NOT NULL payload slot.
#[test]
fn is_null_over_a_pk_column_is_the_constant() {
    let schema = schema_pk_ints(1, true);
    let view = make_n_col_view(&schema, 3, |row, _| row as i64, |_, _| false);
    for invert in [false, true] {
        let prog = resolved(&schema, vec![LogicalInstr::IsNull { col: 0, invert }], Reg(0));
        let mut scratch = EvalScratch::new(&prog);
        drive(&prog, &view, 0, 3, &mut scratch);
        assert_eq!(&scratch.regs[..3], &[invert as i64; 3]);
    }
}
