// `0 * MORSEL`, `1 * MORSEL`, `0 * NULL_WORDS_PER_REG` etc. are deliberate
// layout-documenting expressions making the register/word index explicit at
// each access site; collapsing them obscures which register is in use.
#![allow(clippy::erasing_op, clippy::identity_op)]

use gnitz_wire::type_code;

use super::{eval_batch, EvalScratch, MORSEL, NULL_WORDS_PER_REG};
use crate::eval::read_reg_row0;
use crate::program::IntUnaryOp;
use crate::test_support::{
    bits_to_float, filter_prog, float_to_bits, make_int_row, make_int_view, make_n_col_view, scalar_prog,
    schema_pk_ints, TestSchema, TestView,
};
use crate::{CmpOp, LogicalInstr, ResolvedProgram};

/// Resolve a test program down to the raw evaluable form the kernel tests drive
/// `eval_batch` with. The `Evaluator` wrapper is the *caller's* surface; these
/// tests are below it.
fn resolved(schema: &TestSchema, instrs: Vec<LogicalInstr>, num_regs: u32, result_reg: u32) -> ResolvedProgram {
    scalar_prog(schema, instrs, num_regs, result_reg, vec![]).prog
}

/// A register file sized by hand — the split-borrow helper tests below are the
/// only ones with no program to size it from. Everything else goes through
/// `EvalScratch::ensure_capacity(&prog, _)`, which is what keeps the scratch's
/// nullability arm and the program's from disagreeing.
fn raw_scratch(num_regs: usize, no_nulls: bool, n: usize) -> EvalScratch {
    let mut s = EvalScratch {
        no_nulls,
        ..Default::default()
    };
    let null_cap = if no_nulls { 0 } else { num_regs * NULL_WORDS_PER_REG };
    s.grow(num_regs * MORSEL, null_cap, n.div_ceil(64));
    s
}

/// One `split_windows` backs both scratch buffers and both arities, so drive
/// every shape the kernels ask of it: two sources and three over `regs` (the
/// binary opcodes and SELECT's blend), and two over `null_bits` (the null
/// propagation, whose windows are words rather than values).
#[test]
fn test_scratch_splits_disjoint_windows() {
    let mut s = raw_scratch(4, /* no_nulls = */ false, MORSEL);
    let m = 4;
    for i in 0..m {
        s.regs[0 * MORSEL + i] = (i as i64) + 1;
        s.regs[1 * MORSEL + i] = (i as i64) * 10;
        s.regs[2 * MORSEL + i] = 100;
    }
    {
        let ([ra, rb], rd) = s.regs_split([0, 1], 3, m);
        for i in 0..m {
            rd[i] = ra[i] + rb[i];
        }
    }
    assert_eq!(&s.regs[3 * MORSEL..3 * MORSEL + m], &[1, 12, 23, 34]);
    {
        let ([ra, rb, rc], rd) = s.regs_split([0, 1, 2], 3, m);
        for i in 0..m {
            rd[i] = ra[i] + rb[i] + rc[i];
        }
    }
    assert_eq!(&s.regs[3 * MORSEL..3 * MORSEL + m], &[101, 112, 123, 134]);

    s.null_bits[0 * NULL_WORDS_PER_REG] = 0b1010;
    s.null_bits[1 * NULL_WORDS_PER_REG] = 0b0110;
    {
        let ([na, nb], nd) = s.null_split([0, 1], 2, 1);
        nd[0] = na[0] | nb[0];
    }
    assert_eq!(s.null_bits[2 * NULL_WORDS_PER_REG], 0b1110);
}

#[test]
fn test_eval_batch_add() {
    let schema = schema_pk_ints(1, true);
    let mb = make_int_view(&schema, &[(1, 0, &[10]), (2, 0, &[20]), (3, 0, &[30])]);

    // r0 = pk (LoadColInt col 0 → resolves to Instr::LoadPk), r1 = col[1]
    // (the first payload), r2 = r0 + r1
    let instrs = vec![
        LogicalInstr::LoadColInt { dst: 0, col: 0 },
        LogicalInstr::LoadColInt { dst: 1, col: 1 },
        LogicalInstr::IntAdd { dst: 2, a: 0, b: 1 },
    ];
    let prog = resolved(&schema, instrs, 3, 2);
    let mut scratch = EvalScratch::default();
    scratch.ensure_capacity(&prog, 3);
    eval_batch(&prog, &mb, 0, 3, &mut scratch);

    // row 0: pk=1, val=10, sum=11
    assert_eq!(scratch.regs[2 * MORSEL + 0], 11);
    // row 1: pk=2, val=20, sum=22
    assert_eq!(scratch.regs[2 * MORSEL + 1], 22);
    // row 2: pk=3, val=30, sum=33
    assert_eq!(scratch.regs[2 * MORSEL + 2], 33);
}

// ---------------------------------------------------------------------------
// Edge-case golden tests at m=1
//
// These pin the points where the m=1 path through eval_batch could most
// plausibly diverge from the vectorized one. They each set up a one-row input
// and assert against a manually computed expected value via a direct
// `eval_batch`.
// ---------------------------------------------------------------------------

#[test]
fn golden_int_div_zero_divisor_single_row() {
    let schema = schema_pk_ints(1, true);
    let mb = make_int_row(&schema, &[10], 0);

    // r0 = col1 = 10, r1 = 0, r2 = r0 / r1 → null
    let instrs = vec![
        LogicalInstr::LoadColInt { dst: 0, col: 1 },
        LogicalInstr::LoadConst { dst: 1, val: 0 },
        LogicalInstr::IntDiv { dst: 2, a: 0, b: 1 },
    ];
    let prog = resolved(&schema, instrs, 3, 2);

    let mut scratch = EvalScratch::default();
    scratch.ensure_capacity(&prog, 1);
    eval_batch(&prog, &mb, 0, 1, &mut scratch);
    let is_null = (scratch.null_bits[2 * NULL_WORDS_PER_REG] & 1) != 0;
    assert!(is_null, "INT_DIV by zero must produce NULL at m=1");
    // The zero-mask merge into the destination null word must leave high bits zero.
    assert_eq!(
        scratch.null_bits[2 * NULL_WORDS_PER_REG] & !1u64,
        0,
        "high bits of dst null word must stay zero at m=1",
    );
}

#[test]
fn golden_float_div_zero_divisor_single_row() {
    let schema = TestSchema::new(&[(type_code::U64, false), (type_code::F64, true)], &[0]);
    // arbitrary non-zero F64 bits
    let mb = make_int_view(&schema, &[(1, 0, &[float_to_bits(2.5)])]);

    // r0 = col1 (f64), r1 = 0.0 bits, r2 = r0 / r1 → null
    let instrs = vec![
        LogicalInstr::LoadColFloat { dst: 0, col: 1 },
        LogicalInstr::LoadConst { dst: 1, val: 0 },
        LogicalInstr::FloatDiv { dst: 2, a: 0, b: 1 },
    ];
    let prog = resolved(&schema, instrs, 3, 2);

    let mut scratch = EvalScratch::default();
    scratch.ensure_capacity(&prog, 1);
    eval_batch(&prog, &mb, 0, 1, &mut scratch);
    let is_null = (scratch.null_bits[2 * NULL_WORDS_PER_REG] & 1) != 0;
    assert!(is_null, "FLOAT_DIV by zero must produce NULL at m=1");
    assert_eq!(
        scratch.null_bits[2 * NULL_WORDS_PER_REG] & !1u64,
        0,
        "high bits of dst null word must stay zero at m=1",
    );
}

/// Build a 1-row view with two nullable I64 columns plus a u64 PK.
fn run_bool_combinator(schema: &TestSchema, batch: &TestView, op: fn(u16, u16, u16) -> LogicalInstr) -> (i64, bool) {
    let instrs = vec![
        LogicalInstr::LoadColInt { dst: 0, col: 1 },
        LogicalInstr::LoadColInt { dst: 1, col: 2 },
        op(2, 0, 1),
    ];
    let prog = resolved(schema, instrs, 3, 2);
    let mut scratch = EvalScratch::default();
    scratch.ensure_capacity(&prog, 1);
    eval_batch(&prog, batch, 0, 1, &mut scratch);
    let val = read_reg_row0(&prog, &scratch, 2);
    let is_null = (scratch.null_bits[2 * NULL_WORDS_PER_REG] & 1) != 0;
    // The 3VL whole-word path writes the full u64 for word 0; bits beyond
    // bit 0 must be zero at m=1.
    assert_eq!(
        scratch.null_bits[2 * NULL_WORDS_PER_REG] & !1u64,
        0,
        "3VL whole-word path must leave high bits of dst null word zero at m=1",
    );
    (val, is_null)
}

#[test]
fn golden_bool_and_3vl_single_row() {
    let schema = schema_pk_ints(2, true);
    // TRUE AND NULL = NULL: col1=1, col2=null (bit 1 set in null word)
    let b = make_int_row(&schema, &[1, 0], 1u64 << 1);
    let (_, n) = run_bool_combinator(&schema, &b, |dst, a, b| LogicalInstr::BoolAnd { dst, a, b });
    assert!(n, "TRUE AND NULL must be NULL at m=1");

    // FALSE AND NULL = FALSE: col1=0, col2=null
    let b = make_int_row(&schema, &[0, 0], 1u64 << 1);
    let (v, n) = run_bool_combinator(&schema, &b, |dst, a, b| LogicalInstr::BoolAnd { dst, a, b });
    assert!(!n, "FALSE AND NULL must not be NULL at m=1");
    assert_eq!(v, 0, "FALSE AND NULL must be FALSE at m=1");
}

#[test]
fn golden_bool_or_3vl_single_row() {
    let schema = schema_pk_ints(2, true);
    // NULL OR TRUE = TRUE: col1=null (bit 0), col2=1
    let b = make_int_row(&schema, &[0, 1], 1u64 << 0);
    let (v, n) = run_bool_combinator(&schema, &b, |dst, a, b| LogicalInstr::BoolOr { dst, a, b });
    assert!(!n, "NULL OR TRUE must not be NULL at m=1");
    assert_eq!(v, 1, "NULL OR TRUE must be TRUE at m=1");

    // NULL OR FALSE = NULL: col1=null, col2=0
    let b = make_int_row(&schema, &[0, 0], 1u64 << 0);
    let (_, n) = run_bool_combinator(&schema, &b, |dst, a, b| LogicalInstr::BoolOr { dst, a, b });
    assert!(n, "NULL OR FALSE must be NULL at m=1");
}

#[test]
fn golden_int_neg_null_source_single_row() {
    let schema = schema_pk_ints(1, true);
    // col1 null → INT_NEG result null. null_or1 at m=1.
    let mb = make_int_row(&schema, &[0], 1);
    let instrs = vec![
        LogicalInstr::LoadColInt { dst: 0, col: 1 },
        LogicalInstr::IntUnary {
            op: IntUnaryOp::Neg,
            dst: 1,
            a: 0,
        },
    ];
    let prog = resolved(&schema, instrs, 2, 1);
    let mut scratch = EvalScratch::default();
    scratch.ensure_capacity(&prog, 1);
    eval_batch(&prog, &mb, 0, 1, &mut scratch);
    assert!(
        (scratch.null_bits[1 * NULL_WORDS_PER_REG] & 1) != 0,
        "INT_NEG of NULL must be NULL at m=1",
    );
}

#[test]
fn golden_bool_not_null_source_single_row() {
    let schema = schema_pk_ints(1, true);
    let mb = make_int_row(&schema, &[0], 1);
    let instrs = vec![
        LogicalInstr::LoadColInt { dst: 0, col: 1 },
        LogicalInstr::BoolNot { dst: 1, a: 0 },
    ];
    let prog = resolved(&schema, instrs, 2, 1);
    let mut scratch = EvalScratch::default();
    scratch.ensure_capacity(&prog, 1);
    eval_batch(&prog, &mb, 0, 1, &mut scratch);
    assert!(
        (scratch.null_bits[1 * NULL_WORDS_PER_REG] & 1) != 0,
        "NOT NULL must be NULL at m=1",
    );
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
        LogicalInstr::LoadColInt { dst: 0, col: 1 }, // cond
        LogicalInstr::LoadColInt { dst: 1, col: 2 }, // a
        LogicalInstr::LoadColInt { dst: 2, col: 3 }, // b
        LogicalInstr::Select {
            dst: 3,
            cond: 0,
            a: 1,
            b: 2,
        },
    ];
    let prog = resolved(&schema, instrs, 4, 3);

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

        let mut scratch = EvalScratch::default();
        for morsel_start in (0..n).step_by(MORSEL) {
            let m = MORSEL.min(n - morsel_start);
            scratch.ensure_capacity(&prog, m);
            eval_batch(&prog, &mb, morsel_start, m, &mut scratch);
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
/// the value blend runs through `reg4` with no mask tracking. Verify the blend.
#[test]
fn select_no_nulls_fast_arm() {
    // cond / a / b are all NOT NULL, which selects the no_nulls fast arm.
    let schema = schema_pk_ints(3, false);
    let instrs = vec![
        LogicalInstr::LoadColInt { dst: 0, col: 1 },
        LogicalInstr::LoadColInt { dst: 1, col: 2 },
        LogicalInstr::LoadColInt { dst: 2, col: 3 },
        LogicalInstr::Select {
            dst: 3,
            cond: 0,
            a: 1,
            b: 2,
        },
    ];
    let prog = resolved(&schema, instrs, 4, 3);
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
    let mut scratch = EvalScratch::default();
    scratch.ensure_capacity(&prog, n);
    eval_batch(&prog, &mb, 0, n, &mut scratch);
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
// Bit-only / bool_bits vectorization tests
// ---------------------------------------------------------------------------

/// Boolean register consumed by an arithmetic opcode forces it OUT of
/// bit_only. The AND result must still appear correctly in `regs` for the
/// downstream add.
#[test]
fn bit_only_demotion_when_bool_feeds_arithmetic() {
    let schema = schema_pk_ints(2, true);
    // col1=2, col2=3, no nulls. Both > 1, so AND = 1. Add 0 → result = 1.
    let mb = make_int_row(&schema, &[2, 3], 0);

    let instrs = vec![
        LogicalInstr::LoadColInt { dst: 0, col: 1 }, // r0 = col1
        LogicalInstr::LoadConst { dst: 1, val: 1 },  // r1 = 1
        LogicalInstr::Cmp {
            op: CmpOp::Gt,
            dst: 2,
            a: 0,
            b: 1,
        }, // r2 = col1 > 1
        LogicalInstr::LoadColInt { dst: 3, col: 2 }, // r3 = col2
        LogicalInstr::Cmp {
            op: CmpOp::Gt,
            dst: 4,
            a: 3,
            b: 1,
        }, // r4 = col2 > 1
        LogicalInstr::BoolAnd { dst: 5, a: 2, b: 4 }, // r5 = r2 AND r4    (consumed by ADD → not bit_only)
        LogicalInstr::LoadConst { dst: 6, val: 0 },  // r6 = 0
        LogicalInstr::IntAdd { dst: 7, a: 5, b: 6 }, // r7 = r5 + 0 = bool-as-int
    ];
    let prog = resolved(&schema, instrs, 8, 7);
    // r5 is bool-produced but consumed by INT_ADD (non-bool). Must be demoted.
    assert!(
        !prog.is_bit_only(5),
        "bool reg fed into arithmetic must NOT be bit_only",
    );

    let mut scratch = EvalScratch::default();
    scratch.ensure_capacity(&prog, 1);
    eval_batch(&prog, &mb, 0, 1, &mut scratch);
    // r5 lives in regs as 0/1; r7 = r5 + 0 = 1.
    assert_eq!(
        scratch.regs[5 * MORSEL],
        1,
        "AND result must land in regs when !bit_only"
    );
    assert_eq!(scratch.regs[7 * MORSEL], 1, "downstream arithmetic reads bool as i64");
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
        .map(|ci| LogicalInstr::LoadColInt {
            dst: ci as u16,
            col: ci,
        })
        .collect();
    let prog = resolved(&schema, instrs, cols.len() as u32, 0);
    let mut scratch = EvalScratch::default();
    scratch.ensure_capacity(&prog, ROWS);
    eval_batch(&prog, &mb, 0, ROWS, &mut scratch);

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
// AND-chain dead-tail short-circuit (the runtime skip in eval_batch)
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// A/B microbench for the AND-chain dead-tail skip (regression artifact).
// ---------------------------------------------------------------------------

/// Drive `eval_batch` over every morsel of `mb`, XOR-folding the terminal filter
/// word into a checksum (anti-elision + an ON/OFF equivalence guard). The skip
/// lives entirely in `eval_batch`; the filter's bit extraction is a fixed
/// per-morsel cost, so isolating `eval_batch` measures the mechanism directly.
fn run_chain_eval(prog: &ResolvedProgram, mb: &TestView, n: usize, scratch: &mut EvalScratch) -> u64 {
    let base = prog.result_reg as usize * NULL_WORDS_PER_REG;
    let mut checksum = 0u64;
    for morsel_start in (0..n).step_by(MORSEL) {
        let m = MORSEL.min(n - morsel_start);
        eval_batch(prog, mb, morsel_start, m, scratch);
        for w in 0..m.div_ceil(64) {
            checksum ^= scratch.bool_bits[base + w] & !scratch.null_bits[base + w];
        }
    }
    checksum
}

/// Time `run_chain_eval` with the skip ON (computed mask) vs OFF (mask zeroed)
/// over `min` of `ITERS` order-alternated iterations; assert ON and OFF agree.
fn bench_chain(label: &str, prog_on: &ResolvedProgram, prog_off: &ResolvedProgram, mb: &TestView, n: usize) {
    use std::hint::black_box;
    use std::time::Instant;

    let mut scratch = EvalScratch::default();
    scratch.ensure_capacity(prog_on, MORSEL);

    // Warm up and confirm the skip does not change the result.
    let c_on = run_chain_eval(prog_on, mb, n, &mut scratch);
    let c_off = run_chain_eval(prog_off, mb, n, &mut scratch);
    assert_eq!(c_on, c_off, "{label}: skip ON/OFF produced different filter results");

    const ITERS: usize = 60;
    let (mut min_on, mut min_off) = (u128::MAX, u128::MAX);
    for i in 0..ITERS {
        // Alternate which variant is timed first to cancel ordering drift.
        if i % 2 == 0 {
            let s = Instant::now();
            black_box(run_chain_eval(prog_on, mb, n, &mut scratch));
            min_on = min_on.min(s.elapsed().as_nanos());
            let s = Instant::now();
            black_box(run_chain_eval(prog_off, mb, n, &mut scratch));
            min_off = min_off.min(s.elapsed().as_nanos());
        } else {
            let s = Instant::now();
            black_box(run_chain_eval(prog_off, mb, n, &mut scratch));
            min_off = min_off.min(s.elapsed().as_nanos());
            let s = Instant::now();
            black_box(run_chain_eval(prog_on, mb, n, &mut scratch));
            min_on = min_on.min(s.elapsed().as_nanos());
        }
    }
    let (ms_on, ms_off) = (min_on as f64 / 1e6, min_off as f64 / 1e6);
    let delta = (min_off as f64 - min_on as f64) / min_off as f64 * 100.0;
    println!(
        "{label}: skip ON {ms_on:.2} ms  OFF {ms_off:.2} ms  ({n} rows)  -> {:.1}% {}",
        delta.abs(),
        if delta >= 0.0 {
            "faster with skip"
        } else {
            "slower with skip"
        },
    );
}

/// A/B microbench: 1M-row batch, 5-clause nullable chain
/// `a > t AND b > 0 AND c > 0 AND d > 0 AND e > 0`, skip toggled via the trigger
/// mask in one binary. Run with:
///   cargo test -p gnitz-expr --release and_chain_skip_bench \
///       -- --ignored --nocapture --test-threads=1
#[test]
#[ignore]
fn and_chain_skip_bench() {
    // PK + 5 nullable I64 columns.
    let schema = schema_pk_ints(5, true);

    // a > THRESHOLD AND b > 0 AND c > 0 AND d > 0 AND e > 0  (16 regs, result r15).
    let build_instrs = |threshold: i64| {
        vec![
            LogicalInstr::LoadColInt { dst: 0, col: 1 }, // a
            LogicalInstr::LoadConst { dst: 1, val: threshold },
            LogicalInstr::Cmp {
                op: CmpOp::Gt,
                dst: 2,
                a: 0,
                b: 1,
            },
            LogicalInstr::LoadColInt { dst: 3, col: 2 }, // b
            LogicalInstr::LoadConst { dst: 4, val: 0 },
            LogicalInstr::Cmp {
                op: CmpOp::Gt,
                dst: 5,
                a: 3,
                b: 4,
            },
            LogicalInstr::BoolAnd { dst: 6, a: 2, b: 5 }, // trigger
            LogicalInstr::LoadColInt { dst: 7, col: 3 },  // c
            LogicalInstr::Cmp {
                op: CmpOp::Gt,
                dst: 8,
                a: 7,
                b: 4,
            },
            LogicalInstr::BoolAnd { dst: 9, a: 6, b: 8 }, // trigger
            LogicalInstr::LoadColInt { dst: 10, col: 4 }, // d
            LogicalInstr::Cmp {
                op: CmpOp::Gt,
                dst: 11,
                a: 10,
                b: 4,
            },
            LogicalInstr::BoolAnd { dst: 12, a: 9, b: 11 }, // trigger
            LogicalInstr::LoadColInt { dst: 13, col: 5 },   // e
            LogicalInstr::Cmp {
                op: CmpOp::Gt,
                dst: 14,
                a: 13,
                b: 4,
            },
            LogicalInstr::BoolAnd { dst: 15, a: 12, b: 14 }, // result
        ]
    };

    const N: usize = 1_000_000;

    // Favorable: a = row (clustered), threshold at ~70% so the first ~70% of
    // morsels are entirely a <= t (definite-FALSE); the rest pass all 5 clauses
    // (b..e = 10 > 0). No nulls in a/b so the skip fires cleanly.
    let favorable = make_n_col_view(
        &schema,
        N,
        |row, col| if col == 0 { row as i64 } else { 10 },
        |_, _| false,
    );
    // Neutral: a = 10 with threshold -1 (always true), so every morsel keeps a
    // survivor and the skip never fires — measures the alive-reduce overhead.
    let neutral = make_n_col_view(&schema, N, |_, _| 10, |_, _| false);

    let mut variants: Vec<(&str, &TestView, i64)> = vec![
        ("favorable", &favorable, (N as i64 * 7) / 10),
        ("neutral", &neutral, -1),
    ];
    for (label, mb, threshold) in variants.drain(..) {
        let prog_on = filter_prog(&schema, build_instrs(threshold), 16, 15, vec![]).prog;
        let mut prog_off = filter_prog(&schema, build_instrs(threshold), 16, 15, vec![]).prog;
        assert_ne!(prog_on.chain_trigger_mask, 0, "bench prog should have a detected chain");
        prog_off.chain_trigger_mask = 0;
        bench_chain(label, &prog_on, &prog_off, mb, N);
    }
}

// ---------------------------------------------------------------------------
// Numeric scalar functions and numeric CAST
// ---------------------------------------------------------------------------

use crate::program::FloatUnaryOp;

/// Run a one-operand program over `n` rows of a single nullable I64 column,
/// returning `(value, is_null)` per row. `mk` builds the instruction under test
/// from `(dst, a)`; the operand register is loaded from column 1.
fn run_unary_rows(vals: &[i64], nulls: &[bool], mk: impl Fn(u16, u16) -> LogicalInstr) -> Vec<(i64, bool)> {
    let schema = schema_pk_ints(1, true);
    let n = vals.len();
    let view = make_n_col_view(&schema, n, |row, _| vals[row], |row, _| nulls[row]);
    let instrs = vec![LogicalInstr::LoadColInt { dst: 0, col: 1 }, mk(1, 0)];
    let prog = resolved(&schema, instrs, 2, 1);
    let mut scratch = EvalScratch::default();
    scratch.ensure_capacity(&prog, n);
    eval_batch(&prog, &view, 0, n, &mut scratch);
    (0..n)
        .map(|i| {
            let v = scratch.regs[1 * MORSEL + i];
            let null = (scratch.null_bits[1 * NULL_WORDS_PER_REG + i / 64] >> (i % 64)) & 1 != 0;
            (v, null)
        })
        .collect()
}

/// Two-operand form of [`run_unary_rows`] over two nullable I64 columns.
fn run_binary_rows(
    a: &[i64],
    b: &[i64],
    a_null: &[bool],
    b_null: &[bool],
    mk: impl Fn(u16, u16, u16) -> LogicalInstr,
) -> Vec<(i64, bool)> {
    let schema = schema_pk_ints(2, true);
    let n = a.len();
    let view = make_n_col_view(
        &schema,
        n,
        |row, col| if col == 0 { a[row] } else { b[row] },
        |row, col| if col == 0 { a_null[row] } else { b_null[row] },
    );
    let instrs = vec![
        LogicalInstr::LoadColInt { dst: 0, col: 1 },
        LogicalInstr::LoadColInt { dst: 1, col: 2 },
        mk(2, 0, 1),
    ];
    let prog = resolved(&schema, instrs, 3, 2);
    let mut scratch = EvalScratch::default();
    scratch.ensure_capacity(&prog, n);
    eval_batch(&prog, &view, 0, n, &mut scratch);
    (0..n)
        .map(|i| {
            let v = scratch.regs[2 * MORSEL + i];
            let null = (scratch.null_bits[2 * NULL_WORDS_PER_REG + i / 64] >> (i % 64)) & 1 != 0;
            (v, null)
        })
        .collect()
}

fn fu(op: FloatUnaryOp) -> impl Fn(u16, u16) -> LogicalInstr {
    move |dst, a| LogicalInstr::FloatUnary { op, dst, a }
}

#[test]
fn int_abs_wraps_at_min_and_propagates_null() {
    let vals = [5i64, -5, 0, i64::MIN, 7];
    let nulls = [false, false, false, false, true];
    let out = run_unary_rows(&vals, &nulls, |dst, a| LogicalInstr::IntUnary {
        op: IntUnaryOp::Abs,
        dst,
        a,
    });
    assert_eq!(out[0], (5, false));
    assert_eq!(out[1], (5, false));
    assert_eq!(out[2], (0, false));
    // Rule 3: same-width, so it wraps rather than producing NULL.
    assert_eq!(out[3], (i64::MIN, false));
    assert!(out[4].1, "NULL in, NULL out");
}

#[test]
fn float_unary_ops_match_ieee() {
    let vals: Vec<i64> = [-0.0f64, 2.5, 3.5, -2.5, -1.7, f64::NAN]
        .iter()
        .map(|&f| float_to_bits(f))
        .collect();
    let nulls = vec![false; vals.len()];
    let get = |out: &Vec<(i64, bool)>, i: usize| bits_to_float(out[i].0);

    let abs = run_unary_rows(&vals, &nulls, fu(FloatUnaryOp::Abs));
    assert!(get(&abs, 0).is_sign_positive(), "abs(-0.0) is +0.0");
    assert!(get(&abs, 4) == 1.7);
    assert!(get(&abs, 5).is_nan(), "abs(NaN) is NaN");

    let round = run_unary_rows(&vals, &nulls, fu(FloatUnaryOp::Round));
    assert_eq!(get(&round, 1), 2.0, "round_ties_even(2.5) = 2");
    assert_eq!(get(&round, 2), 4.0, "round_ties_even(3.5) = 4");
    assert_eq!(get(&round, 3), -2.0, "round_ties_even(-2.5) = -2");

    let floor = run_unary_rows(&vals, &nulls, fu(FloatUnaryOp::Floor));
    assert_eq!(get(&floor, 4), -2.0);
    let ceil = run_unary_rows(&vals, &nulls, fu(FloatUnaryOp::Ceil));
    assert_eq!(get(&ceil, 4), -1.0);
    let trunc = run_unary_rows(&vals, &nulls, fu(FloatUnaryOp::Trunc));
    assert_eq!(get(&trunc, 4), -1.0, "trunc is toward zero");
}

#[test]
fn float_to_f32_nulls_only_on_finite_overflow() {
    let vals: Vec<i64> = [0.1f64, 1e300, 1e-300, f64::INFINITY, f64::NAN, f32::MAX as f64]
        .iter()
        .map(|&f| float_to_bits(f))
        .collect();
    let nulls = vec![false; vals.len()];
    let out = run_unary_rows(&vals, &nulls, |dst, a| LogicalInstr::FloatToF32 { dst, a });
    let get = |i: usize| bits_to_float(out[i].0);

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
    let v = vec![float_to_bits(just_over)];
    let out = run_unary_rows(&v, &[false], |dst, a| LogicalInstr::FloatToF32 { dst, a });
    assert!(!out[0].1, "rounds down to f32::MAX, so not an overflow");
    assert_eq!(bits_to_float(out[0].0), f32::MAX as f64);
}

#[test]
fn int_cast_range_checks_per_target_and_source_signedness() {
    let vals = [200i64, -1, 127, 128, i64::MIN];
    let nulls = vec![false; vals.len()];

    // Signed source -> I8: only -128..=127 survive.
    let out = run_unary_rows(&vals, &nulls, |dst, a| LogicalInstr::IntCast {
        dst,
        a,
        tc: type_code::I8 as u32,
    });
    assert!(out[0].1, "200 out of I8");
    assert_eq!(out[1], (-1, false));
    assert_eq!(out[2], (127, false));
    assert!(out[3].1, "128 out of I8");
    assert!(out[4].1);

    // Signed source -> U64: the check degenerates to "not negative".
    let out = run_unary_rows(&vals, &nulls, |dst, a| LogicalInstr::IntCast {
        dst,
        a,
        tc: type_code::U64 as u32,
    });
    assert_eq!(out[0], (200, false));
    assert!(out[1].1, "-1 has no U64 image");
    assert!(out[4].1);

    // Unsigned source -> I64: values >= 2^63 fail. The U64 taint comes from a
    // U64 column load, which is what makes `src_signed` false.
    let schema = TestSchema::new(&[(type_code::U64, false), (type_code::U64, true)], &[0]);
    let n = 2;
    let big = i64::MIN; // bit pattern 2^63
    let view = make_n_col_view(&schema, n, |row, _| if row == 0 { 5 } else { big }, |_, _| false);
    let instrs = vec![
        LogicalInstr::LoadColInt { dst: 0, col: 1 },
        LogicalInstr::IntCast {
            dst: 1,
            a: 0,
            tc: type_code::I64 as u32,
        },
    ];
    let prog = resolved(&schema, instrs, 2, 1);
    let mut scratch = EvalScratch::default();
    scratch.ensure_capacity(&prog, n);
    eval_batch(&prog, &view, 0, n, &mut scratch);
    assert_eq!(scratch.regs[1 * MORSEL], 5);
    assert!((scratch.null_bits[1 * NULL_WORDS_PER_REG] & 1) == 0, "5 fits I64");
    assert!(
        (scratch.null_bits[1 * NULL_WORDS_PER_REG] >> 1) & 1 != 0,
        "2^63 read as u64 exceeds I64"
    );
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
    .map(|&f| float_to_bits(f))
    .collect();
    let nulls = vec![false; vals.len()];
    let out = run_unary_rows(&vals, &nulls, |dst, a| LogicalInstr::FloatToInt {
        dst,
        a,
        tc: type_code::I64 as u32,
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
        .map(|&f| float_to_bits(f))
        .collect();
    let out = run_unary_rows(&vals, &[false; 4], |dst, a| LogicalInstr::FloatToInt {
        dst,
        a,
        tc: type_code::I8 as u32,
    });
    assert_eq!(out[0], (127, false));
    assert!(out[1].1);
    assert_eq!(out[2], (-128, false));
    assert_eq!(out[3], (-128, false), "-128.9 truncates toward zero to -128, in range");
}

#[test]
fn minmax2_skips_nulls_and_is_null_only_when_both_are() {
    let a = [1i64, 5, 9, 0, 0];
    let b = [2i64, 3, 0, 7, 0];
    let an = [false, false, false, true, true];
    let bn = [false, false, true, false, true];

    let max = run_binary_rows(&a, &b, &an, &bn, |dst, a, b| LogicalInstr::IntMinMax2 {
        dst,
        a,
        b,
        is_max: true,
    });
    assert_eq!(max[0], (2, false));
    assert_eq!(max[1], (5, false));
    assert_eq!(max[2], (9, false), "b NULL -> a");
    assert_eq!(max[3], (7, false), "a NULL -> b");
    assert!(max[4].1, "both NULL -> NULL");

    let min = run_binary_rows(&a, &b, &an, &bn, |dst, a, b| LogicalInstr::IntMinMax2 {
        dst,
        a,
        b,
        is_max: false,
    });
    assert_eq!(min[0], (1, false));
    assert_eq!(min[2], (9, false), "b NULL -> a, regardless of value");
    assert!(min[4].1);
}

#[test]
fn float_minmax2_uses_total_cmp_order() {
    let f = float_to_bits;
    let a = [f(5.0), f(5.0), f(-0.0), f(f64::INFINITY)];
    let b = [f(f64::NAN), f(2.0), f(0.0), f(f64::NAN)];
    let no = [false; 4];

    let max = run_binary_rows(&a, &b, &no, &no, |dst, a, b| LogicalInstr::FloatMinMax2 {
        dst,
        a,
        b,
        is_max: true,
    });
    assert!(bits_to_float(max[0].0).is_nan(), "NaN is the total-order max");
    assert_eq!(bits_to_float(max[1].0), 5.0);
    assert!(
        bits_to_float(max[2].0).is_sign_positive(),
        "+0.0 beats -0.0 under total_cmp"
    );
    assert!(bits_to_float(max[3].0).is_nan(), "NaN outranks +inf");

    let min = run_binary_rows(&a, &b, &no, &no, |dst, a, b| LogicalInstr::FloatMinMax2 {
        dst,
        a,
        b,
        is_max: false,
    });
    assert_eq!(bits_to_float(min[0].0), 5.0, "NaN loses MIN");
    assert!(
        bits_to_float(min[2].0).is_sign_negative(),
        "-0.0 wins MIN under total_cmp"
    );
}
