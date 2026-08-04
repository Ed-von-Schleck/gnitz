// 3.14 is a deliberate float-bit-pattern test fixture, not an approximation
// of PI meant to be replaced with std::f64::consts::PI.
#![allow(clippy::approx_constant)]

use gnitz_wire::type_code;

use super::{classify_registers, RegisterRoles};
use crate::program::IntUnaryOp;
use crate::test_support::{
    bits_to_float, filter_prog, float_to_bits, make_int_view, make_string_view, scalar_prog, schema_pk_ints,
    schema_pk_strings, TestSchema, TestView,
};
use crate::{CmpOp, ColKind, Evaluator, ExprValidateErr, Instr, LogicalInstr, LogicalProgram, StrOp};

/// Run `ev` at m=1 over `(mb, row)` and report
/// `(predicate value, predicate is_null, EMIT null mask, EMIT values)` — the
/// map-side read, where each EMIT'd register lands in an output payload slot
/// and a NULL register stores 0 with its output bit set.
fn eval_with_emit(ev: &Evaluator, mb: &TestView, row: usize) -> (i64, bool, u64, Vec<i64>) {
    let mut emit_vals: Vec<i64> = Vec::new();
    let mut emit_null_mask: u64 = 0;
    ev.eval_morsels(mb, row, 1, |_, out| {
        for (src, payload) in ev.emit_targets() {
            let mut is_null = false;
            out.for_each_null_row(src as usize, |_| is_null = true);
            if is_null {
                emit_vals.push(0);
                emit_null_mask |= 1u64 << payload;
            } else {
                emit_vals.push(out.reg_values(src as usize)[0]);
            }
        }
    });
    let (val, is_null) = ev.eval_row(mb, row);
    (val, is_null, emit_null_mask, emit_vals)
}

#[test]
fn test_int_comparisons() {
    // Schema: 2 columns, col0=PK(U64), col1=I64
    let schema = schema_pk_ints(1, true);
    let mb = make_int_view(&schema, &[(1, 0, &[42])]);

    // r0 = load_col_int(1), r1 = load_const(42), r2 = cmp_eq(r0, r1)
    let instrs = vec![
        LogicalInstr::LoadColInt { dst: 0, col: 1 }, // r0 = col[1]
        LogicalInstr::LoadConst { dst: 1, val: 42 }, // r1 = 42
        LogicalInstr::Cmp {
            op: CmpOp::Eq,
            dst: 2,
            a: 0,
            b: 1,
        }, // r2 = (r0 == r1)
    ];
    let prog = scalar_prog(&schema, instrs, 3, 2, vec![]);
    let (val, is_null) = prog.eval_row(&mb, 0);
    assert_eq!(val, 1);
    assert!(!is_null);

    // Test NE
    let instrs = vec![
        LogicalInstr::LoadColInt { dst: 0, col: 1 },
        LogicalInstr::LoadConst { dst: 1, val: 42 },
        LogicalInstr::Cmp {
            op: CmpOp::Ne,
            dst: 2,
            a: 0,
            b: 1,
        },
    ];
    let prog = scalar_prog(&schema, instrs, 3, 2, vec![]);
    let (val, _) = prog.eval_row(&mb, 0);
    assert_eq!(val, 0);

    // Test GT
    let instrs = vec![
        LogicalInstr::LoadColInt { dst: 0, col: 1 },
        LogicalInstr::LoadConst { dst: 1, val: 10 },
        LogicalInstr::Cmp {
            op: CmpOp::Gt,
            dst: 2,
            a: 0,
            b: 1,
        },
    ];
    let prog = scalar_prog(&schema, instrs, 3, 2, vec![]);
    let (val, _) = prog.eval_row(&mb, 0);
    assert_eq!(val, 1);
}

#[test]
fn test_int_arithmetic() {
    let schema = schema_pk_ints(2, true);
    let mb = make_int_view(&schema, &[(1, 0, &[10, 3])]);

    // ADD: 10 + 3 = 13
    let instrs = vec![
        LogicalInstr::LoadColInt { dst: 0, col: 1 },
        LogicalInstr::LoadColInt { dst: 1, col: 2 },
        LogicalInstr::IntAdd { dst: 2, a: 0, b: 1 },
    ];
    let prog = scalar_prog(&schema, instrs, 3, 2, vec![]);
    let (val, _) = prog.eval_row(&mb, 0);
    assert_eq!(val, 13);

    // DIV by zero → NULL
    let instrs = vec![
        LogicalInstr::LoadColInt { dst: 0, col: 1 },
        LogicalInstr::LoadConst { dst: 1, val: 0 },
        LogicalInstr::IntDiv { dst: 2, a: 0, b: 1 },
    ];
    let prog = scalar_prog(&schema, instrs, 3, 2, vec![]);
    let (_, is_null) = prog.eval_row(&mb, 0);
    assert!(is_null);

    // MOD by zero → NULL
    let instrs = vec![
        LogicalInstr::LoadColInt { dst: 0, col: 1 },
        LogicalInstr::LoadConst { dst: 1, val: 0 },
        LogicalInstr::IntMod { dst: 2, a: 0, b: 1 },
    ];
    let prog = scalar_prog(&schema, instrs, 3, 2, vec![]);
    let (_, is_null) = prog.eval_row(&mb, 0);
    assert!(is_null);

    // NEG: -10
    let instrs = vec![
        LogicalInstr::LoadColInt { dst: 0, col: 1 },
        LogicalInstr::IntUnary {
            op: IntUnaryOp::Neg,
            dst: 1,
            a: 0,
        },
    ];
    let prog = scalar_prog(&schema, instrs, 2, 1, vec![]);
    let (val, _) = prog.eval_row(&mb, 0);
    assert_eq!(val, -10);
}

#[test]
fn test_float_arithmetic_and_comparison() {
    let schema = TestSchema::with_pk_at(0, &[type_code::U64, type_code::F64, type_code::F64]);
    // Store floats as i64 bits
    let a_bits = float_to_bits(3.14);
    let b_bits = float_to_bits(2.0);
    let mb = make_int_view(&schema, &[(1, 0, &[a_bits, b_bits])]);

    // FLOAT_ADD: 3.14 + 2.0
    let instrs = vec![
        LogicalInstr::LoadColFloat { dst: 0, col: 1 },
        LogicalInstr::LoadColFloat { dst: 1, col: 2 },
        LogicalInstr::FloatAdd { dst: 2, a: 0, b: 1 },
    ];
    let prog = scalar_prog(&schema, instrs, 3, 2, vec![]);
    let (val, _) = prog.eval_row(&mb, 0);
    let result = bits_to_float(val);
    assert!((result - 5.14).abs() < 1e-10);

    // FLOAT_DIV by zero → NULL
    let instrs = vec![
        LogicalInstr::LoadColFloat { dst: 0, col: 1 },
        LogicalInstr::LoadConst { dst: 1, val: 0 }, // zero bits
        LogicalInstr::FloatDiv { dst: 2, a: 0, b: 1 },
    ];
    let prog = scalar_prog(&schema, instrs, 3, 2, vec![]);
    let (_, is_null) = prog.eval_row(&mb, 0);
    assert!(is_null);

    // FCMP_GT: 3.14 > 2.0
    let instrs = vec![
        LogicalInstr::LoadColFloat { dst: 0, col: 1 },
        LogicalInstr::LoadColFloat { dst: 1, col: 2 },
        LogicalInstr::FCmp {
            op: CmpOp::Gt,
            dst: 2,
            a: 0,
            b: 1,
        },
    ];
    let prog = scalar_prog(&schema, instrs, 3, 2, vec![]);
    let (val, _) = prog.eval_row(&mb, 0);
    assert_eq!(val, 1);
}

#[test]
fn test_null_propagation() {
    let schema = schema_pk_ints(2, true);
    // col1 is null (bit 0 set), col2 is not null
    let mb = make_int_view(&schema, &[(1, 1, &[0, 5])]);

    // ADD with one null operand → null
    let instrs = vec![
        LogicalInstr::LoadColInt { dst: 0, col: 1 },
        LogicalInstr::LoadColInt { dst: 1, col: 2 },
        LogicalInstr::IntAdd { dst: 2, a: 0, b: 1 },
    ];
    let prog = scalar_prog(&schema, instrs, 3, 2, vec![]);
    let (_, is_null) = prog.eval_row(&mb, 0);
    assert!(is_null);
}

#[test]
fn test_is_null_is_not_null() {
    let schema = schema_pk_ints(2, true);
    // col1 is null (bit 0 set), col2 is not null
    let mb = make_int_view(&schema, &[(1, 1, &[0, 5])]);

    // IS_NULL(col1) → 1 (always non-null result)
    let instrs = vec![LogicalInstr::IsNull { dst: 0, col: 1 }];
    let prog = scalar_prog(&schema, instrs, 1, 0, vec![]);
    let (val, is_null) = prog.eval_row(&mb, 0);
    assert_eq!(val, 1);
    assert!(!is_null);

    // IS_NOT_NULL(col1) → 0
    let instrs = vec![LogicalInstr::IsNotNull { dst: 0, col: 1 }];
    let prog = scalar_prog(&schema, instrs, 1, 0, vec![]);
    let (val, is_null) = prog.eval_row(&mb, 0);
    assert_eq!(val, 0);
    assert!(!is_null);

    // IS_NULL(col2) → 0
    let instrs = vec![LogicalInstr::IsNull { dst: 0, col: 2 }];
    let prog = scalar_prog(&schema, instrs, 1, 0, vec![]);
    let (val, _) = prog.eval_row(&mb, 0);
    assert_eq!(val, 0);
}

#[test]
fn test_boolean_combinators() {
    let schema = schema_pk_ints(1, true);
    let mb = make_int_view(&schema, &[(1, 0, &[1])]);

    // AND(1, 0) → 0
    let instrs = vec![
        LogicalInstr::LoadColInt { dst: 0, col: 1 },
        LogicalInstr::LoadConst { dst: 1, val: 0 },
        LogicalInstr::BoolAnd { dst: 2, a: 0, b: 1 },
    ];
    let prog = scalar_prog(&schema, instrs, 3, 2, vec![]);
    let (val, _) = prog.eval_row(&mb, 0);
    assert_eq!(val, 0);

    // OR(1, 0) → 1
    let instrs = vec![
        LogicalInstr::LoadColInt { dst: 0, col: 1 },
        LogicalInstr::LoadConst { dst: 1, val: 0 },
        LogicalInstr::BoolOr { dst: 2, a: 0, b: 1 },
    ];
    let prog = scalar_prog(&schema, instrs, 3, 2, vec![]);
    let (val, _) = prog.eval_row(&mb, 0);
    assert_eq!(val, 1);

    // NOT(1) → 0
    let instrs = vec![
        LogicalInstr::LoadColInt { dst: 0, col: 1 },
        LogicalInstr::BoolNot { dst: 1, a: 0 },
    ];
    let prog = scalar_prog(&schema, instrs, 2, 1, vec![]);
    let (val, _) = prog.eval_row(&mb, 0);
    assert_eq!(val, 0);
}

#[test]
fn test_load_const_encoding() {
    let schema = schema_pk_ints(1, true);
    let mb = make_int_view(&schema, &[(1, 0, &[0])]);

    // Test large constant: 0x00000001_00000002 = (1 << 32) | 2 = 4294967298
    // Wire form: lo 32 bits = 2, hi 32 bits = 1.
    let instrs = vec![LogicalInstr::LoadConst {
        dst: 0,
        val: ((1i64) << 32) | (2i64 & 0xFFFF_FFFF),
    }];
    let prog = scalar_prog(&schema, instrs, 1, 0, vec![]);
    let (val, is_null) = prog.eval_row(&mb, 0);
    assert!(!is_null);
    assert_eq!(val, (1i64 << 32) | 2);

    // Test negative constant: -1 (the wire low/high split is reconstructed by
    // `from_wire`; the typed instruction carries the full i64 value directly).
    let instrs = vec![LogicalInstr::LoadConst { dst: 0, val: -1 }];
    let prog = scalar_prog(&schema, instrs, 1, 0, vec![]);
    let (val, _) = prog.eval_row(&mb, 0);
    assert_eq!(val, -1);
}

#[test]
fn test_string_eq_const() {
    let schema = schema_pk_strings(1, true);
    // One row holding the short (inline, ≤12 byte) German string "hello".
    let mb = make_string_view(&schema, &[&[b"hello".as_slice()]]);

    // STR_COL_EQ_CONST(col1, "hello") → 1
    let instrs = vec![LogicalInstr::StrColConst {
        op: StrOp::Eq,
        dst: 0,
        col: 1,
        const_idx: 0,
    }];
    let prog = scalar_prog(&schema, instrs, 1, 0, vec![b"hello".to_vec()]);
    let (val, is_null) = prog.eval_row(&mb, 0);
    assert!(!is_null);
    assert_eq!(val, 1);

    // STR_COL_EQ_CONST(col1, "world") → 0
    let instrs = vec![LogicalInstr::StrColConst {
        op: StrOp::Eq,
        dst: 0,
        col: 1,
        const_idx: 0,
    }];
    let prog = scalar_prog(&schema, instrs, 1, 0, vec![b"world".to_vec()]);
    let (val, _) = prog.eval_row(&mb, 0);
    assert_eq!(val, 0);
}

#[test]
fn test_int_to_float() {
    let schema = schema_pk_ints(1, true);
    let mb = make_int_view(&schema, &[(1, 0, &[42])]);

    let instrs = vec![
        LogicalInstr::LoadColInt { dst: 0, col: 1 },
        LogicalInstr::IntToFloat { dst: 1, a: 0 },
    ];
    let prog = scalar_prog(&schema, instrs, 2, 1, vec![]);
    let (val, _) = prog.eval_row(&mb, 0);
    assert_eq!(bits_to_float(val), 42.0);
}

#[test]
fn test_unsigned_opcode_swap_and_eval() {
    // U64-typed schema: pk=col0(U64), payload=col1(U64). `resolve` tracks col1's
    // register as U64 and must select the unsigned form of the comparison /
    // division / cast instructions (`signed: false`). A mis-routed or missing
    // unsigned arm is silent for values >= 2^63 — this pin makes it loud by both
    // (1) asserting the resolved instruction carries `signed: false` and (2)
    // running eval on v=u64::MAX and asserting the UNSIGNED result, which diverges
    // from the signed interpretation that all other tests (BIGINT < 2^63) exercise.
    let schema = TestSchema::with_pk_at(0, &[type_code::U64, type_code::U64]);

    // v = u64::MAX (bit pattern 0xFFFF...FF, i.e. -1 as i64); divisor const = 2.
    // Unsigned: MAX > 100, MAX/2 = 9223372036854775807, MAX%2 = 1.
    // Signed:   -1 < 100 (false), -1/2 = 0,             -1%2 = -1.
    let v_bits = u64::MAX as i64; // == -1i64
    let mb = make_int_view(&schema, &[(1, 0, &[v_bits])]);

    // (a) col1 > 100  → the Gt compare must resolve to the unsigned form.
    let instrs = vec![
        LogicalInstr::LoadColInt { dst: 0, col: 1 },  // r0 = col1 (U64)
        LogicalInstr::LoadConst { dst: 1, val: 100 }, // r1 = 100
        LogicalInstr::Cmp {
            op: CmpOp::Gt,
            dst: 2,
            a: 0,
            b: 1,
        }, // r2 = (r0 > r1)
    ];
    let prog_gt = scalar_prog(&schema, instrs, 3, 2, vec![]);
    // Instruction 2 is the comparison after resolution; the U64 operand must
    // select the unsigned form.
    assert!(
        matches!(
            prog_gt.prog.instrs[2],
            Instr::Cmp {
                op: CmpOp::Gt,
                signed: false,
                ..
            }
        ),
        "U64 operand must select unsigned CMP_GT"
    );
    let (val, is_null) = prog_gt.eval_row(&mb, 0);
    assert!(!is_null);
    assert_eq!(
        val, 1,
        "u64::MAX > 100 is TRUE under unsigned compare; signed (-1 > 100) is false",
    );

    // (b) col1 / 2  → IntDiv must resolve to the unsigned form.
    let instrs = vec![
        LogicalInstr::LoadColInt { dst: 0, col: 1 },
        LogicalInstr::LoadConst { dst: 1, val: 2 },
        LogicalInstr::IntDiv { dst: 2, a: 0, b: 1 },
    ];
    let prog_div = scalar_prog(&schema, instrs, 3, 2, vec![]);
    assert!(
        matches!(prog_div.prog.instrs[2], Instr::IntDiv { signed: false, .. }),
        "U64 operand must select unsigned IntDiv"
    );
    let (val, is_null) = prog_div.eval_row(&mb, 0);
    assert!(!is_null);
    assert_eq!(
        val, 9223372036854775807,
        "u64::MAX / 2 == 9223372036854775807 (unsigned); signed -1/2 would be 0",
    );

    // (c) col1 % 2  → IntMod must resolve to the unsigned form.
    let instrs = vec![
        LogicalInstr::LoadColInt { dst: 0, col: 1 },
        LogicalInstr::LoadConst { dst: 1, val: 2 },
        LogicalInstr::IntMod { dst: 2, a: 0, b: 1 },
    ];
    let prog_mod = scalar_prog(&schema, instrs, 3, 2, vec![]);
    assert!(
        matches!(prog_mod.prog.instrs[2], Instr::IntMod { signed: false, .. }),
        "U64 operand must select unsigned IntMod"
    );
    let (val, is_null) = prog_mod.eval_row(&mb, 0);
    assert!(!is_null);
    assert_eq!(val, 1, "u64::MAX % 2 == 1 (unsigned); signed -1 % 2 would be -1",);

    // (d) CAST col1 to float  → IntToFloat must resolve to the unsigned form.
    let instrs = vec![
        LogicalInstr::LoadColInt { dst: 0, col: 1 }, // r0 = col1 (U64)
        LogicalInstr::IntToFloat { dst: 1, a: 0 },   // r1 = (f64) r0
    ];
    let prog_cast = scalar_prog(&schema, instrs, 2, 1, vec![]);
    // Instruction 1 is the cast after resolution; the U64 operand must select
    // the unsigned form.
    assert!(
        matches!(prog_cast.prog.instrs[1], Instr::IntToFloat { signed: false, .. }),
        "U64 operand must select unsigned IntToFloat"
    );
    let (val, is_null) = prog_cast.eval_row(&mb, 0);
    assert!(!is_null);
    let f = bits_to_float(val);
    assert_eq!(
        f,
        u64::MAX as f64,
        "u64::MAX cast to float is ~1.8e19 (unsigned); signed (-1) would be -1.0",
    );
    assert!(f > 0.0, "unsigned cast of u64::MAX must be a large positive float");
}

#[test]
fn test_emit_with_targets() {
    let schema = schema_pk_ints(2, true);
    let mb = make_int_view(&schema, &[(1, 0, &[10, 20])]);

    // Compute col1 + col2, EMIT to payload col 0
    let instrs = vec![
        LogicalInstr::LoadColInt { dst: 0, col: 1 },
        LogicalInstr::LoadColInt { dst: 1, col: 2 },
        LogicalInstr::IntAdd { dst: 2, a: 0, b: 1 },
        LogicalInstr::Emit { src: 2, out: 0 }, // emit r2 to payload col 0
    ];
    let prog = scalar_prog(&schema, instrs, 3, 2, vec![]);

    let (val, is_null, mask, emit_vals) = eval_with_emit(&prog, &mb, 0);
    assert_eq!(val, 30); // 10 + 20
    assert!(!is_null);
    assert_eq!(mask, 0);
    assert_eq!(emit_vals[0], 30);
}

#[test]
fn test_div_by_zero_null_semantics() {
    // Schema: pk(u64), col1(i64), col2(i64)
    let schema = schema_pk_ints(2, true);
    // Row: pk=1, col1=10, col2=3; null_word=0 (no nulls)
    let mb = make_int_view(&schema, &[(1, 0, &[10, 3])]);

    // 1. INT_DIV by literal 0 → NULL
    let instrs = vec![
        LogicalInstr::LoadColInt { dst: 0, col: 1 },
        LogicalInstr::LoadConst { dst: 1, val: 0 },
        LogicalInstr::IntDiv { dst: 2, a: 0, b: 1 },
    ];
    let prog = scalar_prog(&schema, instrs, 3, 2, vec![]);
    let (_, is_null) = prog.eval_row(&mb, 0);
    assert!(is_null);

    // 2. INT_MOD by literal 0 → NULL
    let instrs = vec![
        LogicalInstr::LoadColInt { dst: 0, col: 1 },
        LogicalInstr::LoadConst { dst: 1, val: 0 },
        LogicalInstr::IntMod { dst: 2, a: 0, b: 1 },
    ];
    let prog = scalar_prog(&schema, instrs, 3, 2, vec![]);
    let (_, is_null) = prog.eval_row(&mb, 0);
    assert!(is_null);

    // 3. FLOAT_DIV by 0.0 bits → NULL
    let instrs = vec![
        LogicalInstr::LoadColInt { dst: 0, col: 1 },
        LogicalInstr::LoadConst { dst: 1, val: 0 },
        LogicalInstr::FloatDiv { dst: 2, a: 0, b: 1 },
    ];
    let prog = scalar_prog(&schema, instrs, 3, 2, vec![]);
    let (_, is_null) = prog.eval_row(&mb, 0);
    assert!(is_null);

    // 4. INT_DIV by non-null non-zero → correct quotient, not null
    let instrs = vec![
        LogicalInstr::LoadColInt { dst: 0, col: 1 },
        LogicalInstr::LoadColInt { dst: 1, col: 2 },
        LogicalInstr::IntDiv { dst: 2, a: 0, b: 1 },
    ];
    let prog = scalar_prog(&schema, instrs, 3, 2, vec![]);
    let (val, is_null) = prog.eval_row(&mb, 0);
    assert!(!is_null);
    assert_eq!(val, 3); // 10 / 3 = 3

    // 5. INT_DIV where divisor column is null → NULL (null propagation)
    // null_word bit 0 = col1 null; use col2 (bit 1) as divisor with col1 null
    // Schema col indices: col1=payload_idx 0, col2=payload_idx 1
    // Build a row where col2 is null (null_word bit 1 set)
    let batch_null_div = make_int_view(&schema, &[(1, 2, &[10, 3])]);
    let mb_null_div = batch_null_div;
    let instrs = vec![
        LogicalInstr::LoadColInt { dst: 0, col: 1 },
        LogicalInstr::LoadColInt { dst: 1, col: 2 },
        LogicalInstr::IntDiv { dst: 2, a: 0, b: 1 },
    ];
    let prog = scalar_prog(&schema, instrs, 3, 2, vec![]);
    let (_, is_null) = prog.eval_row(&mb_null_div, 0);
    assert!(is_null);

    // 6. EMIT of INT_DIV-by-zero result → emit_null_mask bit set, buffer contains 0
    let instrs = vec![
        LogicalInstr::LoadColInt { dst: 0, col: 1 },
        LogicalInstr::LoadConst { dst: 1, val: 0 },
        LogicalInstr::IntDiv { dst: 2, a: 0, b: 1 },
        LogicalInstr::Emit { src: 2, out: 0 },
    ];
    let prog = scalar_prog(&schema, instrs, 3, 2, vec![]);
    let (_, _, mask, emit_vals) = eval_with_emit(&prog, &mb, 0);
    assert_eq!(mask & 1, 1); // bit 0 set → null
    assert_eq!(emit_vals[0], 0);
}

#[test]
fn test_cmp_ge_lt_le() {
    let schema = schema_pk_ints(1, true);
    let mb = make_int_view(&schema, &[(1, 0, &[42])]);

    // GE: 42 >= 42 → 1
    let instrs = vec![
        LogicalInstr::LoadColInt { dst: 0, col: 1 },
        LogicalInstr::LoadConst { dst: 1, val: 42 },
        LogicalInstr::Cmp {
            op: CmpOp::Ge,
            dst: 2,
            a: 0,
            b: 1,
        },
    ];
    let prog = scalar_prog(&schema, instrs, 3, 2, vec![]);
    let (val, _) = prog.eval_row(&mb, 0);
    assert_eq!(val, 1);

    // GE: 42 >= 43 → 0
    let instrs = vec![
        LogicalInstr::LoadColInt { dst: 0, col: 1 },
        LogicalInstr::LoadConst { dst: 1, val: 43 },
        LogicalInstr::Cmp {
            op: CmpOp::Ge,
            dst: 2,
            a: 0,
            b: 1,
        },
    ];
    let prog = scalar_prog(&schema, instrs, 3, 2, vec![]);
    let (val, _) = prog.eval_row(&mb, 0);
    assert_eq!(val, 0);

    // LT: 42 < 43 → 1
    let instrs = vec![
        LogicalInstr::LoadColInt { dst: 0, col: 1 },
        LogicalInstr::LoadConst { dst: 1, val: 43 },
        LogicalInstr::Cmp {
            op: CmpOp::Lt,
            dst: 2,
            a: 0,
            b: 1,
        },
    ];
    let prog = scalar_prog(&schema, instrs, 3, 2, vec![]);
    let (val, _) = prog.eval_row(&mb, 0);
    assert_eq!(val, 1);

    // LT: 42 < 42 → 0
    let instrs = vec![
        LogicalInstr::LoadColInt { dst: 0, col: 1 },
        LogicalInstr::LoadConst { dst: 1, val: 42 },
        LogicalInstr::Cmp {
            op: CmpOp::Lt,
            dst: 2,
            a: 0,
            b: 1,
        },
    ];
    let prog = scalar_prog(&schema, instrs, 3, 2, vec![]);
    let (val, _) = prog.eval_row(&mb, 0);
    assert_eq!(val, 0);

    // LE: 42 <= 42 → 1
    let instrs = vec![
        LogicalInstr::LoadColInt { dst: 0, col: 1 },
        LogicalInstr::LoadConst { dst: 1, val: 42 },
        LogicalInstr::Cmp {
            op: CmpOp::Le,
            dst: 2,
            a: 0,
            b: 1,
        },
    ];
    let prog = scalar_prog(&schema, instrs, 3, 2, vec![]);
    let (val, _) = prog.eval_row(&mb, 0);
    assert_eq!(val, 1);

    // LE: 42 <= 41 → 0
    let instrs = vec![
        LogicalInstr::LoadColInt { dst: 0, col: 1 },
        LogicalInstr::LoadConst { dst: 1, val: 41 },
        LogicalInstr::Cmp {
            op: CmpOp::Le,
            dst: 2,
            a: 0,
            b: 1,
        },
    ];
    let prog = scalar_prog(&schema, instrs, 3, 2, vec![]);
    let (val, _) = prog.eval_row(&mb, 0);
    assert_eq!(val, 0);
}

#[test]
fn test_fcmp_eq_ne_lt_le() {
    let schema = TestSchema::with_pk_at(0, &[type_code::U64, type_code::F64, type_code::F64]);
    let a_bits = float_to_bits(3.14);
    let b_bits = float_to_bits(2.0);
    let mb = make_int_view(&schema, &[(1, 0, &[a_bits, b_bits])]);

    // FCMP_EQ: 3.14 == 3.14 → 1
    let instrs = vec![
        LogicalInstr::LoadColFloat { dst: 0, col: 1 },
        LogicalInstr::LoadColFloat { dst: 1, col: 1 },
        LogicalInstr::FCmp {
            op: CmpOp::Eq,
            dst: 2,
            a: 0,
            b: 1,
        },
    ];
    let prog = scalar_prog(&schema, instrs, 3, 2, vec![]);
    let (val, _) = prog.eval_row(&mb, 0);
    assert_eq!(val, 1);

    // FCMP_EQ: 3.14 == 2.0 → 0
    let instrs = vec![
        LogicalInstr::LoadColFloat { dst: 0, col: 1 },
        LogicalInstr::LoadColFloat { dst: 1, col: 2 },
        LogicalInstr::FCmp {
            op: CmpOp::Eq,
            dst: 2,
            a: 0,
            b: 1,
        },
    ];
    let prog = scalar_prog(&schema, instrs, 3, 2, vec![]);
    let (val, _) = prog.eval_row(&mb, 0);
    assert_eq!(val, 0);

    // FCMP_NE: 3.14 != 2.0 → 1
    let instrs = vec![
        LogicalInstr::LoadColFloat { dst: 0, col: 1 },
        LogicalInstr::LoadColFloat { dst: 1, col: 2 },
        LogicalInstr::FCmp {
            op: CmpOp::Ne,
            dst: 2,
            a: 0,
            b: 1,
        },
    ];
    let prog = scalar_prog(&schema, instrs, 3, 2, vec![]);
    let (val, _) = prog.eval_row(&mb, 0);
    assert_eq!(val, 1);

    // FCMP_LT: 2.0 < 3.14 → 1
    let instrs = vec![
        LogicalInstr::LoadColFloat { dst: 0, col: 2 },
        LogicalInstr::LoadColFloat { dst: 1, col: 1 },
        LogicalInstr::FCmp {
            op: CmpOp::Lt,
            dst: 2,
            a: 0,
            b: 1,
        },
    ];
    let prog = scalar_prog(&schema, instrs, 3, 2, vec![]);
    let (val, _) = prog.eval_row(&mb, 0);
    assert_eq!(val, 1);

    // FCMP_LE: 2.0 <= 2.0 → 1
    let instrs = vec![
        LogicalInstr::LoadColFloat { dst: 0, col: 2 },
        LogicalInstr::LoadColFloat { dst: 1, col: 2 },
        LogicalInstr::FCmp {
            op: CmpOp::Le,
            dst: 2,
            a: 0,
            b: 1,
        },
    ];
    let prog = scalar_prog(&schema, instrs, 3, 2, vec![]);
    let (val, _) = prog.eval_row(&mb, 0);
    assert_eq!(val, 1);

    // FCMP_GE: 2.0 >= 3.14 → 0
    let instrs = vec![
        LogicalInstr::LoadColFloat { dst: 0, col: 2 },
        LogicalInstr::LoadColFloat { dst: 1, col: 1 },
        LogicalInstr::FCmp {
            op: CmpOp::Ge,
            dst: 2,
            a: 0,
            b: 1,
        },
    ];
    let prog = scalar_prog(&schema, instrs, 3, 2, vec![]);
    let (val, _) = prog.eval_row(&mb, 0);
    assert_eq!(val, 0);
}

#[test]
fn test_string_lt_le_const() {
    let schema = schema_pk_strings(1, true);
    let mb = make_string_view(&schema, &[&[b"hello".as_slice()]]);

    // STR_COL_LT_CONST: "hello" < "world" → 1
    let instrs = vec![LogicalInstr::StrColConst {
        op: StrOp::Lt,
        dst: 0,
        col: 1,
        const_idx: 0,
    }];
    let prog = scalar_prog(&schema, instrs, 1, 0, vec![b"world".to_vec()]);
    let (val, _) = prog.eval_row(&mb, 0);
    assert_eq!(val, 1);

    // STR_COL_LT_CONST: "hello" < "hello" → 0
    let instrs = vec![LogicalInstr::StrColConst {
        op: StrOp::Lt,
        dst: 0,
        col: 1,
        const_idx: 0,
    }];
    let prog = scalar_prog(&schema, instrs, 1, 0, vec![b"hello".to_vec()]);
    let (val, _) = prog.eval_row(&mb, 0);
    assert_eq!(val, 0);

    // STR_COL_LE_CONST: "hello" <= "hello" → 1
    let instrs = vec![LogicalInstr::StrColConst {
        op: StrOp::Le,
        dst: 0,
        col: 1,
        const_idx: 0,
    }];
    let prog = scalar_prog(&schema, instrs, 1, 0, vec![b"hello".to_vec()]);
    let (val, _) = prog.eval_row(&mb, 0);
    assert_eq!(val, 1);

    // STR_COL_LE_CONST: "hello" <= "hella" → 0
    let instrs = vec![LogicalInstr::StrColConst {
        op: StrOp::Le,
        dst: 0,
        col: 1,
        const_idx: 0,
    }];
    let prog = scalar_prog(&schema, instrs, 1, 0, vec![b"hella".to_vec()]);
    let (val, _) = prog.eval_row(&mb, 0);
    assert_eq!(val, 0);
}

#[test]
fn test_string_col_eq_col() {
    // Schema: pk(U64), str_a(STRING), str_b(STRING)
    let schema = schema_pk_strings(2, true);
    // Row 0: str_a="abc", str_b="abc" (equal). Row 1: "abc" vs "xyz".
    let mb = make_string_view(
        &schema,
        &[
            &[b"abc".as_slice(), b"abc".as_slice()],
            &[b"abc".as_slice(), b"xyz".as_slice()],
        ],
    );

    // Row 0: col1 == col2 → 1
    let instrs = vec![LogicalInstr::StrColCol {
        op: StrOp::Eq,
        dst: 0,
        col_a: 1,
        col_b: 2,
    }];
    let prog = scalar_prog(&schema, instrs, 1, 0, vec![]);
    let (val, _) = prog.eval_row(&mb, 0);
    assert_eq!(val, 1);

    // Row 1: col1 == col2 → 0
    let (val, _) = prog.eval_row(&mb, 1);
    assert_eq!(val, 0);
}

#[test]
fn test_complex_predicate() {
    // Schema: pk(U64), a(I64), b(I64), c(I64)
    let schema = schema_pk_ints(3, true);
    // Row: a=15, b=50, c=42
    let mb = make_int_view(&schema, &[(1, 0, &[15, 50, 42])]);

    // (a > 10 AND b < 100) OR c == 42
    // r0=col1(a), r1=10, r2=(a>10), r3=col2(b), r4=100, r5=(b<100)
    // r6=(r2 AND r5), r7=col3(c), r8=42, r9=(c==42), r10=(r6 OR r9)
    let instrs = vec![
        LogicalInstr::LoadColInt { dst: 0, col: 1 }, // r0 = a = 15
        LogicalInstr::LoadConst { dst: 1, val: 10 }, // r1 = 10
        LogicalInstr::Cmp {
            op: CmpOp::Gt,
            dst: 2,
            a: 0,
            b: 1,
        }, // r2 = (15 > 10) = 1
        LogicalInstr::LoadColInt { dst: 3, col: 2 }, // r3 = b = 50
        LogicalInstr::LoadConst { dst: 4, val: 100 }, // r4 = 100
        LogicalInstr::Cmp {
            op: CmpOp::Lt,
            dst: 5,
            a: 3,
            b: 4,
        }, // r5 = (50 < 100) = 1
        LogicalInstr::BoolAnd { dst: 6, a: 2, b: 5 }, // r6 = (1 AND 1) = 1
        LogicalInstr::LoadColInt { dst: 7, col: 3 }, // r7 = c = 42
        LogicalInstr::LoadConst { dst: 8, val: 42 }, // r8 = 42
        LogicalInstr::Cmp {
            op: CmpOp::Eq,
            dst: 9,
            a: 7,
            b: 8,
        }, // r9 = (42 == 42) = 1
        LogicalInstr::BoolOr { dst: 10, a: 6, b: 9 }, // r10 = (1 OR 1) = 1
    ];
    let prog = scalar_prog(&schema, instrs, 11, 10, vec![]);
    let (val, is_null) = prog.eval_row(&mb, 0);
    assert_eq!(val, 1);
    assert!(!is_null);

    // Test with a=5 (a>10 false), b=50, c=99 (c==42 false) → false
    let batch2 = make_int_view(&schema, &[(1, 0, &[5, 50, 99])]);
    let mb2 = batch2;
    let (val, _) = prog.eval_row(&mb2, 0);
    assert_eq!(val, 0);
}

#[test]
fn test_zero_regs_program() {
    // A program with num_regs=0 (pure COPY_COL) must not crash.
    let schema = schema_pk_ints(1, true);
    let mb = make_int_view(&schema, &[(1, 0, &[100])]);

    // One COPY_COL instruction: copy col 1 → payload 0 (source type derived in resolve)
    let instrs = vec![LogicalInstr::CopyCol { src_col: 1, out: 0 }];
    let prog = scalar_prog(&schema, instrs, 0, 0, vec![]);
    let (val, is_null) = prog.eval_row(&mb, 0);
    // With num_regs=0, result should be (0, true) — sentinel
    assert_eq!(val, 0);
    assert!(is_null);
}

#[test]
fn test_resolve_column_indices_pk_at_col0() {
    // Schema: pk=col0(U64), col1=I64, col2=I64
    let schema = schema_pk_ints(2, true);
    let mb = make_int_view(&schema, &[(42, 0, &[10, 20])]);

    // LOAD_COL_INT of pk column → LoadPk
    let instrs = vec![LogicalInstr::LoadColInt { dst: 0, col: 0 }]; // col 0 = pk
    let prog = scalar_prog(&schema, instrs, 1, 0, vec![]);
    assert!(matches!(prog.prog.instrs[0], Instr::LoadPk { .. }));
    let (val, is_null) = prog.eval_row(&mb, 0);
    assert_eq!(val, 42);
    assert!(!is_null);

    // LOAD_COL_INT of col1 (logical 1) → LoadPayloadInt, physical 0
    let instrs = vec![LogicalInstr::LoadColInt { dst: 0, col: 1 }];
    let prog = scalar_prog(&schema, instrs, 1, 0, vec![]);
    assert!(matches!(prog.prog.instrs[0], Instr::LoadPayloadInt { pi: 0, .. }));
    let (val, _) = prog.eval_row(&mb, 0);
    assert_eq!(val, 10);

    // LOAD_COL_INT of col2 (logical 2) → LoadPayloadInt, physical 1
    let instrs = vec![LogicalInstr::LoadColInt { dst: 0, col: 2 }];
    let prog = scalar_prog(&schema, instrs, 1, 0, vec![]);
    assert!(matches!(prog.prog.instrs[0], Instr::LoadPayloadInt { pi: 1, .. }));
    let (val, _) = prog.eval_row(&mb, 0);
    assert_eq!(val, 20);
}

#[test]
fn test_resolve_column_indices_pk_at_middle() {
    // Schema: col0=I64, pk=col1(U64), col2=I64
    // Physical payload layout: [col0=payload0, col2=payload1]
    let schema = TestSchema::with_pk_at(1, &[type_code::I64, type_code::U64, type_code::I64]);
    let mb = make_int_view(&schema, &[(99, 0, &[5, 7])]);

    // col0 (logical 0, before pk) → LoadPayloadInt, physical 0
    let instrs = vec![LogicalInstr::LoadColInt { dst: 0, col: 0 }];
    let prog = scalar_prog(&schema, instrs, 1, 0, vec![]);
    assert!(matches!(prog.prog.instrs[0], Instr::LoadPayloadInt { pi: 0, .. }));
    let (val, _) = prog.eval_row(&mb, 0);
    assert_eq!(val, 5);

    // col1 (logical 1 = pk) → LoadPk
    let instrs = vec![LogicalInstr::LoadColInt { dst: 0, col: 1 }];
    let prog = scalar_prog(&schema, instrs, 1, 0, vec![]);
    assert!(matches!(prog.prog.instrs[0], Instr::LoadPk { .. }));
    let (val, _) = prog.eval_row(&mb, 0);
    assert_eq!(val, 99);

    // col2 (logical 2, after pk) → LoadPayloadInt, physical 1
    let instrs = vec![LogicalInstr::LoadColInt { dst: 0, col: 2 }];
    let prog = scalar_prog(&schema, instrs, 1, 0, vec![]);
    assert!(matches!(prog.prog.instrs[0], Instr::LoadPayloadInt { pi: 1, .. }));
    let (val, _) = prog.eval_row(&mb, 0);
    assert_eq!(val, 7);
}

#[test]
fn test_is_strictly_non_nullable_str_col() {
    // STR_COL_*_CONST and STR_COL_*_COL must flip no_nulls off when any
    // operand column is nullable; without that, the batch path skips null-bit
    // tracking and null rows leak through string predicates as definite results.
    let nullable_schema = TestSchema::new(
        &[
            (type_code::U64, false),
            (type_code::STRING, true),
            (type_code::STRING, true),
        ],
        &[0],
    );
    let nonnull_schema = TestSchema::new(
        &[
            (type_code::U64, false),
            (type_code::STRING, false),
            (type_code::STRING, false),
        ],
        &[0],
    );

    // STR_COL_*_CONST on col1
    for (op, _name) in &[
        (StrOp::Eq, "EQ_CONST"),
        (StrOp::Lt, "LT_CONST"),
        (StrOp::Le, "LE_CONST"),
    ] {
        let instrs = vec![LogicalInstr::StrColConst {
            op: *op,
            dst: 0,
            col: 1,
            const_idx: 0,
        }];
        let prog = scalar_prog(&nullable_schema, instrs.clone(), 1, 0, vec![b"x".to_vec()]);
        assert!(!prog.prog.no_nulls, "{_name}: nullable col1 must yield no_nulls=false");
        let prog = scalar_prog(&nonnull_schema, instrs, 1, 0, vec![b"x".to_vec()]);
        assert!(
            prog.prog.no_nulls,
            "{_name}: non-nullable col1 must yield no_nulls=true"
        );
    }

    // STR_COL_*_COL — both operands matter
    for (op, _name) in &[(StrOp::Eq, "EQ_COL"), (StrOp::Lt, "LT_COL"), (StrOp::Le, "LE_COL")] {
        let instrs = vec![LogicalInstr::StrColCol {
            op: *op,
            dst: 0,
            col_a: 1,
            col_b: 2,
        }];
        let prog = scalar_prog(&nullable_schema, instrs.clone(), 1, 0, vec![]);
        assert!(
            !prog.prog.no_nulls,
            "{_name}: nullable operands must yield no_nulls=false"
        );
        let prog = scalar_prog(&nonnull_schema, instrs, 1, 0, vec![]);
        assert!(
            prog.prog.no_nulls,
            "{_name}: non-nullable operands must yield no_nulls=true"
        );
    }
}

/// Build a 1-row view with the given schema, containing `s` as the first payload column.
fn make_single_string_batch(schema: &TestSchema, s: &[u8]) -> TestView {
    make_string_view(schema, &[&[s]])
}

#[test]
fn test_string_prefix_ordering() {
    // Schema: pk=col0(U64), col1=STRING
    let schema = schema_pk_strings(1, true);

    // (col_string, const_string, expected_lt)
    let cases: &[(&[u8], &[u8], bool)] = &[
        (b"abc", b"abd", true),      // differ at prefix byte 2
        (b"abd", b"abc", false),     // reversed
        (b"a", b"b", true),          // single-char, differ at byte 0
        (b"abcd", b"abce", true),    // differ at prefix byte 3 (boundary)
        (b"abcde", b"abcdf", true),  // differ at byte 4 (beyond prefix)
        (b"hello", b"world", true),  // differ at byte 0
        (b"he", b"hello", true),     // col shorter, same available bytes
        (b"hello", b"he", false),    // col longer
        (b"", b"a", true),           // empty < non-empty
        (b"a", b"", false),          // non-empty > empty
        (b"abcd", b"abcd", false),   // equal 4-byte strings, not lt
        (b"abcde", b"abcde", false), // equal 5-byte strings, not lt
        // LE-vs-BE regression: "ba" > "ac" because 'b'>'a' at byte 0.
        // With LE integer comparison, from_le("ba")=0x6162 < from_le("ac")=0x6361,
        // which would wrongly return true. BE comparison gives the correct false.
        (b"ba", b"ac", false),
        (b"ac", b"ba", true),
        (b"ba", b"ab", false), // byte 0 same first char but reversed positions
        (b"ab", b"ba", true),
    ];

    for &(col_s, const_s, expected_lt) in cases {
        let mb = make_single_string_batch(&schema, col_s);
        let instrs = vec![LogicalInstr::StrColConst {
            op: StrOp::Lt,
            dst: 0,
            col: 1,
            const_idx: 0,
        }];
        let prog = scalar_prog(&schema, instrs, 1, 0, vec![const_s.to_vec()]);
        let (val, _) = prog.eval_row(&mb, 0);
        assert_eq!(
            val != 0,
            expected_lt,
            "STR_COL_LT_CONST: {:?} < {:?} expected {}, got {}",
            col_s,
            const_s,
            expected_lt,
            val != 0
        );
    }
}

#[test]
fn test_bool_and_or_three_valued_logic() {
    // Schema: pk(U64), col1(I64), col2(I64) — both nullable
    let schema = schema_pk_ints(2, true);
    // null_word bits: bit 0 = col1 null, bit 1 = col2 null
    let mb = make_int_view(
        &schema,
        &[
            (1, 0, &[1, 0]), // row0: T, F
            (2, 0, &[0, 1]), // row1: F, T
            (3, 2, &[1, 0]), // row2: T, NULL
            (4, 2, &[0, 0]), // row3: F, NULL
            (5, 1, &[0, 1]), // row4: NULL, T
            (6, 1, &[0, 0]), // row5: NULL, F
            (7, 3, &[0, 0]), // row6: NULL, NULL
        ],
    );

    let and_instrs = vec![
        LogicalInstr::LoadColInt { dst: 0, col: 1 },
        LogicalInstr::LoadColInt { dst: 1, col: 2 },
        LogicalInstr::BoolAnd { dst: 2, a: 0, b: 1 },
    ];
    let and_prog = scalar_prog(&schema, and_instrs, 3, 2, vec![]);

    let or_instrs = vec![
        LogicalInstr::LoadColInt { dst: 0, col: 1 },
        LogicalInstr::LoadColInt { dst: 1, col: 2 },
        LogicalInstr::BoolOr { dst: 2, a: 0, b: 1 },
    ];
    let or_prog = scalar_prog(&schema, or_instrs, 3, 2, vec![]);

    // AND cases
    let (v, n) = and_prog.eval_row(&mb, 0);
    assert_eq!(v, 0, "T AND F = F");
    assert!(!n);
    let (v, n) = and_prog.eval_row(&mb, 1);
    assert_eq!(v, 0, "F AND T = F");
    assert!(!n);
    let (_, n) = and_prog.eval_row(&mb, 2);
    assert!(n, "T AND NULL = NULL");
    let (v, n) = and_prog.eval_row(&mb, 3);
    assert_eq!(v, 0, "F AND NULL = F");
    assert!(!n, "F AND NULL must not be null (SQL 3VL)");
    let (_, n) = and_prog.eval_row(&mb, 4);
    assert!(n, "NULL AND T = NULL");
    let (v, n) = and_prog.eval_row(&mb, 5);
    assert_eq!(v, 0, "NULL AND F = F");
    assert!(!n, "NULL AND F must not be null (SQL 3VL)");
    let (_, n) = and_prog.eval_row(&mb, 6);
    assert!(n, "NULL AND NULL = NULL");

    // OR cases
    let (v, n) = or_prog.eval_row(&mb, 0);
    assert_eq!(v, 1, "T OR F = T");
    assert!(!n);
    let (v, n) = or_prog.eval_row(&mb, 1);
    assert_eq!(v, 1, "F OR T = T");
    assert!(!n);
    let (v, n) = or_prog.eval_row(&mb, 2);
    assert_eq!(v, 1, "T OR NULL = T");
    assert!(!n, "T OR NULL must not be null (SQL 3VL)");
    let (_, n) = or_prog.eval_row(&mb, 3);
    assert!(n, "F OR NULL = NULL");
    let (v, n) = or_prog.eval_row(&mb, 4);
    assert_eq!(v, 1, "NULL OR T = T");
    assert!(!n, "NULL OR T must not be null (SQL 3VL)");
    let (_, n) = or_prog.eval_row(&mb, 5);
    assert!(n, "NULL OR F = NULL");
    let (_, n) = or_prog.eval_row(&mb, 6);
    assert!(n, "NULL OR NULL = NULL");
}

#[test]
fn test_string_prefix_le_ordering() {
    // Spot-check LE (≤) to cover the equality boundary
    let schema = schema_pk_strings(1, true);

    let cases: &[(&[u8], &[u8], bool)] = &[
        (b"abc", b"abc", true),   // equal → le
        (b"abc", b"abd", true),   // less → le
        (b"abd", b"abc", false),  // greater → not le
        (b"abcd", b"abcd", true), // equal 4-byte (prefix boundary)
        (b"he", b"he", true),     // equal short
        // LE-vs-BE regression: "ba" > "ac", so "ba" <= "ac" must be false.
        (b"ba", b"ac", false),
        (b"ac", b"ba", true),
    ];

    for &(col_s, const_s, expected_le) in cases {
        let mb = make_single_string_batch(&schema, col_s);
        let instrs = vec![LogicalInstr::StrColConst {
            op: StrOp::Le,
            dst: 0,
            col: 1,
            const_idx: 0,
        }];
        let prog = scalar_prog(&schema, instrs, 1, 0, vec![const_s.to_vec()]);
        let (val, _) = prog.eval_row(&mb, 0);
        assert_eq!(
            val != 0,
            expected_le,
            "STR_COL_LE_CONST: {:?} <= {:?} expected {}, got {}",
            col_s,
            const_s,
            expected_le,
            val != 0
        );
    }
}

// ---------------------------------------------------------------------------
// SELECT / LOAD_NULL (SQL CASE blend)
// ---------------------------------------------------------------------------

/// SELECT truth table at m=1: `cond` non-NULL and truthy → `a`; `cond` false
/// OR NULL → `b`. Mirrors SQL CASE (a NULL WHEN falls to the ELSE branch).
#[test]
fn test_select_truth_table() {
    // Schema: pk(u64), cond(i64), a(i64), b(i64).
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
    let prog = scalar_prog(&schema, instrs, 4, 3, vec![]);

    // cond=1 (truthy) → a=100
    let batch = make_int_view(&schema, &[(1, 0, &[1, 100, 200])]);
    let (v, n) = prog.eval_row(&batch, 0);
    assert!(!n);
    assert_eq!(v, 100, "truthy cond takes a");

    // cond=0 (false) → b=200
    let batch = make_int_view(&schema, &[(1, 0, &[0, 100, 200])]);
    let (v, n) = prog.eval_row(&batch, 0);
    assert!(!n);
    assert_eq!(v, 200, "false cond takes b");

    // cond=NULL → b=200 (null_word bit 0 = cond/col1); value 7 is truthy but masked.
    let batch = make_int_view(&schema, &[(1, 1, &[7, 100, 200])]);
    let (v, n) = prog.eval_row(&batch, 0);
    assert!(!n);
    assert_eq!(v, 200, "NULL cond falls to else (b), not a");
}

/// SELECT carries the *chosen* branch's null bit; the unchosen branch's null is
/// irrelevant.
#[test]
fn test_select_null_bit_blend() {
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
    let prog = scalar_prog(&schema, instrs, 4, 3, vec![]);

    // cond truthy, a NULL (payload bit 1) → result NULL.
    let batch = make_int_view(&schema, &[(1, 0b010, &[1, 0, 200])]);
    let (_, n) = prog.eval_row(&batch, 0);
    assert!(n, "truthy cond + NULL a → NULL");

    // cond truthy, b NULL (payload bit 2), a non-null → result = a; b's null ignored.
    let batch = make_int_view(&schema, &[(1, 0b100, &[1, 55, 0])]);
    let (v, n) = prog.eval_row(&batch, 0);
    assert!(!n, "truthy cond ignores b's null");
    assert_eq!(v, 55);

    // cond false, b NULL → result NULL.
    let batch = make_int_view(&schema, &[(1, 0b100, &[0, 55, 0])]);
    let (_, n) = prog.eval_row(&batch, 0);
    assert!(n, "false cond + NULL b → NULL");
}

/// `CASE WHEN cond THEN 42 END` (implicit ELSE NULL) lowers to
/// `select(cond, 42, load_null())`: truthy → 42, else → NULL.
#[test]
fn test_load_null_else_branch_eval() {
    let schema = schema_pk_ints(1, true);
    let instrs = vec![
        LogicalInstr::LoadColInt { dst: 0, col: 1 }, // cond
        LogicalInstr::LoadConst { dst: 1, val: 42 }, // a
        LogicalInstr::LoadNull { dst: 2 },           // else NULL
        LogicalInstr::Select {
            dst: 3,
            cond: 0,
            a: 1,
            b: 2,
        },
    ];
    let prog = scalar_prog(&schema, instrs, 4, 3, vec![]);

    // cond truthy → 42.
    let batch = make_int_view(&schema, &[(1, 0, &[5])]);
    let (v, n) = prog.eval_row(&batch, 0);
    assert!(!n);
    assert_eq!(v, 42);

    // cond false → else NULL.
    let batch = make_int_view(&schema, &[(1, 0, &[0])]);
    let (_, n) = prog.eval_row(&batch, 0);
    assert!(n, "false cond → else NULL");

    // cond NULL → else NULL.
    let batch = make_int_view(&schema, &[(1, 1, &[9])]);
    let (_, n) = prog.eval_row(&batch, 0);
    assert!(n, "NULL cond → else NULL");
}

/// LoadNull forces the nullable eval path even when every column is NOT NULL:
/// `is_strictly_non_nullable` must return false whenever a program manufactures
/// a NULL.
#[test]
fn test_load_null_forces_nullable_path() {
    let nonnull_schema = TestSchema::new(&[(type_code::U64, false), (type_code::I64, false)], &[0]);
    // CASE WHEN col1 THEN col1 END: r0=col1, r1=load_null, r2=select(r0, r0, r1).
    let instrs = vec![
        LogicalInstr::LoadColInt { dst: 0, col: 1 },
        LogicalInstr::LoadNull { dst: 1 },
        LogicalInstr::Select {
            dst: 2,
            cond: 0,
            a: 0,
            b: 1,
        },
    ];
    let prog = scalar_prog(&nonnull_schema, instrs, 3, 2, vec![]);
    assert!(!prog.prog.no_nulls, "LoadNull must force the nullable path");
}

/// A Select over only NOT NULL branches (no LoadNull) stays strictly-non-nullable:
/// Select copies branch values and adds no NULL of its own.
#[test]
fn test_select_non_nullable_when_branches_non_nullable() {
    let nonnull_schema = TestSchema::new(
        &[
            (type_code::U64, false),
            (type_code::I64, false),
            (type_code::I64, false),
            (type_code::I64, false),
        ],
        &[0],
    );
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
    let prog = scalar_prog(&nonnull_schema, instrs, 4, 3, vec![]);
    assert!(
        prog.prog.no_nulls,
        "Select over NOT NULL branches must stay strictly-non-nullable"
    );
}

/// U64-ness flows through Select (U64 if either branch is U64), so a downstream
/// ordered compare on the CASE result picks the unsigned variant.
#[test]
fn test_select_u64_propagation() {
    // Schema: pk(u64), u64col(U64), i64col(I64).
    let schema = TestSchema::with_pk_at(0, &[type_code::U64, type_code::U64, type_code::I64]);
    let instrs = vec![
        LogicalInstr::LoadColInt { dst: 0, col: 2 }, // cond (i64)
        LogicalInstr::LoadColInt { dst: 1, col: 1 }, // a (U64)
        LogicalInstr::LoadColInt { dst: 2, col: 2 }, // b (i64)
        LogicalInstr::Select {
            dst: 3,
            cond: 0,
            a: 1,
            b: 2,
        },
        LogicalInstr::LoadConst { dst: 4, val: 100 },
        LogicalInstr::Cmp {
            op: CmpOp::Gt,
            dst: 5,
            a: 3,
            b: 4,
        },
    ];
    let prog = scalar_prog(&schema, instrs, 6, 5, vec![]);
    assert!(
        matches!(
            prog.prog.instrs[5],
            Instr::Cmp {
                op: CmpOp::Gt,
                signed: false,
                ..
            }
        ),
        "Select carrying a U64 branch must make the downstream compare unsigned"
    );
}

/// SELECT feeding BOOL_AND: `cond` is a bool_input, the select result feeds the
/// AND as a bool_input, and the select dst (a value register) is never bit_only.
#[test]
fn test_select_classification() {
    let schema = schema_pk_ints(4, true);
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
        LogicalInstr::LoadColInt { dst: 4, col: 4 }, // other bool
        LogicalInstr::BoolAnd { dst: 5, a: 3, b: 4 },
    ];
    let prog = filter_prog(&schema, instrs, 6, 5, vec![]);
    let RegisterRoles {
        bit_only, bool_input, ..
    } = classify_registers(&prog.prog.instrs, 5, true);
    assert_ne!(bool_input & (1 << 0), 0, "cond is read as a bool_input");
    assert_ne!(bool_input & (1 << 3), 0, "select result feeds BOOL_AND as bool_input");
    assert_eq!(bit_only & (1 << 3), 0, "select dst is a value register, never bit_only");
}

/// `dst` aliasing any of `cond`/`a`/`b` violates the SELECT anti-alias rule that
/// makes `reg4`'s raw split borrows sound.
#[test]
#[should_panic(expected = "RegisterAliasing")]
fn test_select_dst_alias_panics() {
    // `LogicalProgram::new` validates aliasing before any schema is consulted.
    let instrs = vec![
        LogicalInstr::LoadColInt { dst: 0, col: 1 },
        LogicalInstr::Select {
            dst: 0,
            cond: 0,
            a: 0,
            b: 0,
        }, // dst == cond
    ];
    let _ = LogicalProgram::new(instrs, 1, 0, vec![]);
}

/// Nested SELECT — `CASE WHEN c1 THEN v1 WHEN c2 THEN v2 ELSE v3 END` lowered as
/// `select(c1, v1, select(c2, v2, v3))` — must pick the first truthy WHEN.
#[test]
fn test_select_nesting() {
    // Schema: pk, c1, c2, v1, v2, v3.
    let schema = schema_pk_ints(5, true);
    let instrs = vec![
        LogicalInstr::LoadColInt { dst: 0, col: 1 }, // c1
        LogicalInstr::LoadColInt { dst: 1, col: 2 }, // c2
        LogicalInstr::LoadColInt { dst: 2, col: 3 }, // v1
        LogicalInstr::LoadColInt { dst: 3, col: 4 }, // v2
        LogicalInstr::LoadColInt { dst: 4, col: 5 }, // v3
        LogicalInstr::Select {
            dst: 5,
            cond: 1,
            a: 3,
            b: 4,
        }, // inner = c2 ? v2 : v3
        LogicalInstr::Select {
            dst: 6,
            cond: 0,
            a: 2,
            b: 5,
        }, // outer = c1 ? v1 : inner
    ];
    let prog = scalar_prog(&schema, instrs, 7, 6, vec![]);
    // payload cols: c1, c2, v1=10, v2=20, v3=30.
    // c1 truthy → v1.
    let batch = make_int_view(&schema, &[(1, 0, &[1, 1, 10, 20, 30])]);
    let (v, _) = prog.eval_row(&batch, 0);
    assert_eq!(v, 10, "c1 truthy → v1");
    // c1 false, c2 truthy → v2.
    let batch = make_int_view(&schema, &[(1, 0, &[0, 1, 10, 20, 30])]);
    let (v, _) = prog.eval_row(&batch, 0);
    assert_eq!(v, 20, "c1 false, c2 truthy → v2");
    // both false → v3.
    let batch = make_int_view(&schema, &[(1, 0, &[0, 0, 10, 20, 30])]);
    let (v, _) = prog.eval_row(&batch, 0);
    assert_eq!(v, 30, "both false → else v3");
}

// ---------------------------------------------------------------------------
// AND-chain short-circuit detection (`compute_and_chain`)
// ---------------------------------------------------------------------------

/// `c1>1 AND c2>1 AND c3>1 AND c4>1`: a clean left-deep 4-clause chain (3 ANDs).
/// The two non-terminal ANDs (dst reg 5, reg 8) are triggers; the terminal AND
/// (dst reg 11 = result_reg) is not marked.
#[test]
fn and_chain_marks_nonterminal_ands() {
    let schema = schema_pk_ints(4, true); // PK + 4 nullable I64
    let instrs = vec![
        LogicalInstr::LoadColInt { dst: 0, col: 1 },
        LogicalInstr::LoadConst { dst: 1, val: 1 },
        LogicalInstr::Cmp {
            op: CmpOp::Gt,
            dst: 2,
            a: 0,
            b: 1,
        }, // c1>1
        LogicalInstr::LoadColInt { dst: 3, col: 2 },
        LogicalInstr::Cmp {
            op: CmpOp::Gt,
            dst: 4,
            a: 3,
            b: 1,
        }, // c2>1
        LogicalInstr::BoolAnd { dst: 5, a: 2, b: 4 }, // trigger (reg 5)
        LogicalInstr::LoadColInt { dst: 6, col: 3 },
        LogicalInstr::Cmp {
            op: CmpOp::Gt,
            dst: 7,
            a: 6,
            b: 1,
        }, // c3>1
        LogicalInstr::BoolAnd { dst: 8, a: 5, b: 7 }, // trigger (reg 8)
        LogicalInstr::LoadColInt { dst: 9, col: 4 },
        LogicalInstr::Cmp {
            op: CmpOp::Gt,
            dst: 10,
            a: 9,
            b: 1,
        }, // c4>1
        LogicalInstr::BoolAnd { dst: 11, a: 8, b: 10 }, // terminal (reg 11)
    ];
    let prog = filter_prog(&schema, instrs, 12, 11, vec![]).prog;
    assert_eq!(prog.chain_trigger_mask, (1u64 << 5) | (1u64 << 8));
    assert_eq!(prog.chain_trigger_mask.count_ones(), 2);
}

/// A single-AND filter (`c1>1 AND c2>1`) has no non-terminal AND, so the mask is
/// empty: the spine walk finds no upstream single-use AND.
#[test]
fn and_chain_single_and_empty_mask() {
    let schema = schema_pk_ints(2, true);
    let instrs = vec![
        LogicalInstr::LoadColInt { dst: 0, col: 1 },
        LogicalInstr::LoadConst { dst: 1, val: 1 },
        LogicalInstr::Cmp {
            op: CmpOp::Gt,
            dst: 2,
            a: 0,
            b: 1,
        },
        LogicalInstr::LoadColInt { dst: 3, col: 2 },
        LogicalInstr::Cmp {
            op: CmpOp::Gt,
            dst: 4,
            a: 3,
            b: 1,
        },
        LogicalInstr::BoolAnd { dst: 5, a: 2, b: 4 }, // terminal = result_reg
    ];
    let prog = filter_prog(&schema, instrs, 6, 5, vec![]).prog;
    assert_eq!(prog.chain_trigger_mask, 0);
}

/// `NOT (c1>1 AND c2>1)`: terminal is a `BoolNot`, not an AND on result_reg, so
/// nothing is detected — forcing the inner AND FALSE would be a miscompile
/// (`NOT false = true`).
#[test]
fn and_chain_not_terminal_no_mask() {
    let schema = schema_pk_ints(2, true);
    let instrs = vec![
        LogicalInstr::LoadColInt { dst: 0, col: 1 },
        LogicalInstr::LoadConst { dst: 1, val: 1 },
        LogicalInstr::Cmp {
            op: CmpOp::Gt,
            dst: 2,
            a: 0,
            b: 1,
        },
        LogicalInstr::LoadColInt { dst: 3, col: 2 },
        LogicalInstr::Cmp {
            op: CmpOp::Gt,
            dst: 4,
            a: 3,
            b: 1,
        },
        LogicalInstr::BoolAnd { dst: 5, a: 2, b: 4 },
        LogicalInstr::BoolNot { dst: 6, a: 5 }, // result_reg
    ];
    let prog = filter_prog(&schema, instrs, 7, 6, vec![]).prog;
    assert_eq!(prog.chain_trigger_mask, 0);
}

/// `(c1>1 AND c2>1) OR c3>1`: terminal is a `BoolOr`, so the inner AND is reached
/// through an OR operand, not the accumulator spine — not a trigger.
#[test]
fn and_chain_or_terminal_no_mask() {
    let schema = schema_pk_ints(3, true);
    let instrs = vec![
        LogicalInstr::LoadColInt { dst: 0, col: 1 },
        LogicalInstr::LoadConst { dst: 1, val: 1 },
        LogicalInstr::Cmp {
            op: CmpOp::Gt,
            dst: 2,
            a: 0,
            b: 1,
        },
        LogicalInstr::LoadColInt { dst: 3, col: 2 },
        LogicalInstr::Cmp {
            op: CmpOp::Gt,
            dst: 4,
            a: 3,
            b: 1,
        },
        LogicalInstr::BoolAnd { dst: 5, a: 2, b: 4 },
        LogicalInstr::LoadColInt { dst: 6, col: 3 },
        LogicalInstr::Cmp {
            op: CmpOp::Gt,
            dst: 7,
            a: 6,
            b: 1,
        },
        LogicalInstr::BoolOr { dst: 8, a: 5, b: 7 }, // result_reg
    ];
    let prog = filter_prog(&schema, instrs, 9, 8, vec![]).prog;
    assert_eq!(prog.chain_trigger_mask, 0);
}

/// The same clean chain resolved as a MAP (`is_filter = false`) gets a 0 mask:
/// detection is gated on `is_filter`, matching where `bit_only_mask` is gated.
#[test]
fn and_chain_map_program_no_mask() {
    let schema = schema_pk_ints(3, true);
    let instrs = vec![
        LogicalInstr::LoadColInt { dst: 0, col: 1 },
        LogicalInstr::LoadConst { dst: 1, val: 1 },
        LogicalInstr::Cmp {
            op: CmpOp::Gt,
            dst: 2,
            a: 0,
            b: 1,
        },
        LogicalInstr::LoadColInt { dst: 3, col: 2 },
        LogicalInstr::Cmp {
            op: CmpOp::Gt,
            dst: 4,
            a: 3,
            b: 1,
        },
        LogicalInstr::BoolAnd { dst: 5, a: 2, b: 4 },
        LogicalInstr::LoadColInt { dst: 6, col: 3 },
        LogicalInstr::Cmp {
            op: CmpOp::Gt,
            dst: 7,
            a: 6,
            b: 1,
        },
        LogicalInstr::BoolAnd { dst: 8, a: 5, b: 7 },
    ];
    let prog = scalar_prog(&schema, instrs, 9, 8, vec![]);
    assert_eq!(prog.prog.chain_trigger_mask, 0);
}

// ---------------------------------------------------------------------------
// Expr-program validation (ExprValidateErr): one crafted input per vector.
// Wire code is flat u32 quads `[opcode, w1, w2, w3]`; several opcodes read
// `w2`/`w3` as full u32 (col / const_idx / out), not the loop's truncated u16.
// ---------------------------------------------------------------------------

/// `from_wire`'s `Ok` value (`LogicalProgram`) is not `Debug`/`PartialEq`, so
/// extract the `Err` to compare the variant directly.
fn wire_err(r: Result<LogicalProgram, ExprValidateErr>) -> ExprValidateErr {
    match r {
        Ok(_) => panic!("expected Err, got a valid program"),
        Err(e) => e,
    }
}

#[test]
fn test_from_wire_rejects_unknown_opcode() {
    // 0 and u32::MAX are holes permanently. Deliberately NOT "one past the
    // current maximum": that couples the test to every opcode addition while
    // adding no coverage a new opcode's own decode test does not already give.
    assert_eq!(
        wire_err(LogicalProgram::from_wire(&[0, 0, 0, 0], 1, 0, vec![])),
        ExprValidateErr::UnknownOpcode(0)
    );
    assert_eq!(
        wire_err(LogicalProgram::from_wire(&[u32::MAX, 0, 0, 0], 1, 0, vec![])),
        ExprValidateErr::UnknownOpcode(u32::MAX)
    );
    // A valid opcode lowers (control): LOAD_COL_INT dst0 col0.
    assert!(LogicalProgram::from_wire(&[1, 0, 0, 0], 1, 0, vec![]).is_ok());
}

// The rendering the planner's `Unsupported` and the engine's `CREATE VIEW`
// rejection both print: one wording for both, and the register cap is the one
// variant a working query can newly hit, so it gets a sentence naming the limit
// rather than a struct dump.
#[test]
fn test_validate_err_display_names_the_register_limit() {
    assert_eq!(
        ExprValidateErr::TooManyRegs(66).to_string(),
        format!(
            "expression needs 66 registers; the limit is {} — split the predicate",
            crate::MAX_REGS
        )
    );
    // Everything else is an internal-shape violation with no user action:
    // rendered as its Debug form.
    assert_eq!(
        ExprValidateErr::ColOutOfRange { col: 7, num_columns: 3 }.to_string(),
        "ColOutOfRange { col: 7, num_columns: 3 }"
    );
}

#[test]
fn test_from_wire_rejects_bad_register_file() {
    // num_regs over the 64-register limit.
    assert_eq!(
        wire_err(LogicalProgram::from_wire(&[], 65, 0, vec![])),
        ExprValidateErr::TooManyRegs(65)
    );
    // result_reg out of range (>= num_regs).
    assert_eq!(
        wire_err(LogicalProgram::from_wire(&[], 3, 3, vec![])),
        ExprValidateErr::ResultRegOutOfRange {
            result_reg: 3,
            num_regs: 3,
        }
    );
}

#[test]
fn test_from_wire_rejects_out_of_range_register() {
    // IntAdd dst0 a5 b1 with num_regs=2: operand `a=5` is out of range.
    assert_eq!(
        wire_err(LogicalProgram::from_wire(&[4, 0, 5, 1], 2, 0, vec![])),
        ExprValidateErr::RegOutOfRange { reg: 5, num_regs: 2 }
    );
}

#[test]
fn test_from_wire_rejects_register_aliasing() {
    // IntAdd dst0 a0 b1: dst aliases a source — breaks reg3's split borrows.
    assert_eq!(
        wire_err(LogicalProgram::from_wire(&[4, 0, 0, 1], 2, 0, vec![])),
        ExprValidateErr::RegisterAliasing { dst: 0, a: 0, b: 1 }
    );
}

#[test]
fn test_from_wire_rejects_const_idx_out_of_range() {
    // STR_COL_EQ_CONST (40) dst0 col0 const_idx9, pool of length 1.
    assert_eq!(
        wire_err(LogicalProgram::from_wire(&[40, 0, 0, 9], 1, 0, vec![b"x".to_vec()])),
        ExprValidateErr::ConstIdxOutOfRange { const_idx: 9, n: 1 }
    );
}

#[test]
fn test_validate_rejects_out_of_range_column() {
    // LOAD_COL_INT (1) col=200 against a 3-column schema.
    let s3 = schema_pk_ints(2, true);
    let prog = LogicalProgram::from_wire(&[1, 0, 200, 0], 1, 0, vec![]).unwrap();
    assert_eq!(
        prog.validate(Some(&s3), None),
        Err(ExprValidateErr::ColOutOfRange {
            col: 200,
            num_columns: 3,
        })
    );
}

#[test]
fn test_validate_rejects_pk_column_for_payload_only_opcode() {
    // Schema with a PK at column 0 (non-nullable) plus a payload string column.
    let s_pk = TestSchema::new(&[(type_code::U64, false), (type_code::STRING, true)], &[0]);
    // Each payload-only opcode that routes a PK column to the pi=255 sentinel.
    let cases: &[(&[u32], Vec<Vec<u8>>)] = &[
        (&[2, 0, 0, 0], vec![]),               // LOAD_COL_FLOAT col0
        (&[30, 0, 0, 0], vec![]),              // IS_NULL col0
        (&[40, 0, 0, 0], vec![b"x".to_vec()]), // STR_COL_EQ_CONST col0
        (&[43, 0, 0, 1], vec![]),              // STR_COL_EQ_COL col_a=0
    ];
    for (code, consts) in cases {
        let prog = LogicalProgram::from_wire(code, 1, 0, consts.clone()).unwrap();
        assert_eq!(
            prog.validate(Some(&s_pk), None),
            Err(ExprValidateErr::ColNotPayload { col: 0 }),
            "opcode {} must reject a PK source column",
            code[0]
        );
    }
}

#[test]
fn test_validate_rejects_wide_column_register_load() {
    // LOAD_COL_INT on a 16-byte U128 column: rejected before it hits the
    // wide-register eval artefact (Part C).
    let schema = TestSchema::new(&[(type_code::U64, false), (type_code::U128, true)], &[0]);
    let prog = LogicalProgram::from_wire(&[1, 0, 1, 0], 1, 0, vec![]).unwrap();
    assert_eq!(
        prog.validate(Some(&schema), None),
        Err(ExprValidateErr::ColKindMismatch {
            col: 1,
            type_code: type_code::U128,
            want: ColKind::FixedInt,
        })
    );
}

/// `LoadColInt`'s kernel decodes little-endian integer bytes; a width test alone
/// let two shapes through that it cannot decode — a float column (whose bit
/// pattern would become an integer; both widths take different kernel arms) and
/// an unknown type code, which `wire_stride` maps to 8 by design.
#[test]
fn test_validate_load_col_int_requires_fixed_int() {
    for tc in [type_code::F64, type_code::F32, 200, type_code::STRING] {
        let schema = TestSchema::with_pk_at(0, &[type_code::U64, tc]);
        let prog = LogicalProgram::from_wire(&[1, 0, 1, 0], 1, 0, vec![]).unwrap();
        assert_eq!(
            prog.validate(Some(&schema), None),
            Err(ExprValidateErr::ColKindMismatch {
                col: 1,
                type_code: tc,
                want: ColKind::FixedInt,
            }),
            "LOAD_COL_INT must reject type code {tc}"
        );
    }
    // Every fixed-width integer is loadable, PK column included.
    for tc in [
        type_code::U8,
        type_code::I8,
        type_code::U16,
        type_code::I16,
        type_code::U32,
        type_code::I32,
        type_code::U64,
        type_code::I64,
    ] {
        let schema = TestSchema::with_pk_at(0, &[type_code::U64, tc]);
        let prog = LogicalProgram::from_wire(&[1, 0, 1, 0], 1, 0, vec![]).unwrap();
        assert_eq!(prog.validate(Some(&schema), None), Ok(()), "type code {tc}");
    }
}

/// `LoadColFloat`'s kernel branches on width alone: a 1- or 2-byte integer
/// column makes it slice an 8-byte stride out of a narrower region (a panic),
/// and every other non-float width is silent garbage.
#[test]
fn test_validate_load_col_float_requires_float() {
    let case = |tc: u8| {
        let schema = TestSchema::with_pk_at(0, &[type_code::U64, tc]);
        LogicalProgram::from_wire(&[2, 0, 1, 0], 1, 0, vec![])
            .unwrap()
            .validate(Some(&schema), None)
    };
    for tc in [type_code::U16, type_code::U64, type_code::STRING] {
        assert_eq!(
            case(tc),
            Err(ExprValidateErr::ColKindMismatch {
                col: 1,
                type_code: tc,
                want: ColKind::Float,
            }),
            "LOAD_COL_FLOAT must reject type code {tc}"
        );
    }
    assert_eq!(case(type_code::F32), Ok(()));
    assert_eq!(case(type_code::F64), Ok(()));
}

/// The string compares read 16-byte German-string cells with `col_data(pi, 16)`,
/// which over-reads (and eventually runs off) a narrower column's region.
#[test]
fn test_validate_str_opcodes_require_german_string() {
    let schema = |a: u8, b: u8| TestSchema::with_pk_at(0, &[type_code::U64, a, b]);
    // STR_COL_EQ_CONST (40) col=1.
    let vs_const = |a: u8| {
        LogicalProgram::from_wire(&[40, 0, 1, 0], 1, 0, vec![b"x".to_vec()])
            .unwrap()
            .validate(Some(&schema(a, type_code::STRING)), None)
    };
    assert_eq!(
        vs_const(type_code::U64),
        Err(ExprValidateErr::ColKindMismatch {
            col: 1,
            type_code: type_code::U64,
            want: ColKind::GermanString,
        })
    );
    assert_eq!(vs_const(type_code::STRING), Ok(()));
    assert_eq!(vs_const(type_code::BLOB), Ok(()));

    // STR_COL_EQ_COL (43) col_a=1 col_b=2 — both operands are checked.
    let vs_col = |a: u8, b: u8| {
        LogicalProgram::from_wire(&[43, 0, 1, 2], 1, 0, vec![])
            .unwrap()
            .validate(Some(&schema(a, b)), None)
    };
    assert_eq!(
        vs_col(type_code::U64, type_code::STRING),
        Err(ExprValidateErr::ColKindMismatch {
            col: 1,
            type_code: type_code::U64,
            want: ColKind::GermanString,
        })
    );
    assert_eq!(
        vs_col(type_code::STRING, type_code::U64),
        Err(ExprValidateErr::ColKindMismatch {
            col: 2,
            type_code: type_code::U64,
            want: ColKind::GermanString,
        })
    );
    assert_eq!(vs_col(type_code::STRING, type_code::BLOB), Ok(()));
}

/// `IsNull`/`IsNotNull` read the NULL bitmap and nothing else, so any payload
/// column is legitimate — splitting them out of the shared payload-only arm must
/// not have narrowed them to a type class.
#[test]
fn test_validate_null_tests_accept_any_payload_column() {
    for tc in [type_code::U128, type_code::STRING, type_code::F32] {
        let schema = TestSchema::with_pk_at(0, &[type_code::U64, tc]);
        for op in [30u32, 31] {
            let prog = LogicalProgram::from_wire(&[op, 0, 1, 0], 1, 0, vec![]).unwrap();
            assert_eq!(prog.validate(Some(&schema), None), Ok(()), "opcode {op} type {tc}");
        }
    }
}

/// `copy_column` byte-copies at equal width and otherwise widens a narrower
/// integer into a wider slot: no narrowing, no representation change. The
/// widening set is exactly the cross-width set-op coercion the client emits.
#[test]
fn test_validate_copy_col_type_compatibility() {
    // in: [U64 PK, <src>]; out: [U64 PK, <dst>] — one payload slot each.
    let pair = |src: u8, dst: u8| {
        let in_schema = TestSchema::with_pk_at(0, &[type_code::U64, src]);
        let out_schema = TestSchema::with_pk_at(0, &[type_code::U64, dst]);
        // COPY_COL (34) src_col=1 out=0.
        LogicalProgram::from_wire(&[34, 0, 1, 0], 0, 0, vec![])
            .unwrap()
            .validate(Some(&in_schema), Some(&out_schema))
    };
    for (src, dst) in [
        (type_code::I64, type_code::I64),
        (type_code::STRING, type_code::STRING),
        (type_code::BLOB, type_code::BLOB),
        (type_code::U128, type_code::U128),
        (type_code::F64, type_code::F64),
        (type_code::U32, type_code::U64),
        (type_code::U32, type_code::I64),
        (type_code::U8, type_code::I16),
    ] {
        assert_eq!(pair(src, dst), Ok(()), "{src} -> {dst} must be accepted");
    }
    for (src, dst) in [
        (type_code::STRING, type_code::U64),
        (type_code::U64, type_code::STRING),
        (type_code::U128, type_code::U64),
        (type_code::U64, type_code::F64),
        (type_code::F32, type_code::F64),
        (type_code::U64, type_code::I64),
    ] {
        assert_eq!(
            pair(src, dst),
            Err(ExprValidateErr::CopyTypeMismatch {
                col: 1,
                src_tc: src,
                out: 0,
                out_tc: dst,
            }),
            "{src} -> {dst} must be rejected"
        );
    }
    // A PK source into a payload slot of the same type is a copy, not a promotion.
    let in_pk = TestSchema::with_pk_at(0, &[type_code::U64, type_code::I64]);
    let out_pk = TestSchema::with_pk_at(0, &[type_code::I64, type_code::U64]);
    let prog = LogicalProgram::from_wire(&[34, 0, 0, 0], 0, 0, vec![]).unwrap();
    assert_eq!(prog.validate(Some(&in_pk), Some(&out_pk)), Ok(()));
    // Both output-side checks are inert for a filter (`out_schema = None`).
    assert_eq!(prog.validate(Some(&in_pk), None), Ok(()));
}

/// EMIT stores a whole 8-byte register image: a 16-byte slot panics on the first
/// row and a narrower one truncates, so the destination stride is exactly 8.
#[test]
fn test_validate_emit_slot_must_be_eight_bytes() {
    // out: [U64 PK, <slot>] — one payload slot, written by the single EMIT.
    let case = |tc: u8| {
        let schema = TestSchema::with_pk_at(0, &[type_code::U64, tc]);
        // EMIT (32) src=0 out=0.
        LogicalProgram::from_wire(&[32, 0, 0, 0], 1, 0, vec![])
            .unwrap()
            .validate(Some(&schema), Some(&schema))
    };
    for tc in [type_code::I64, type_code::U64, type_code::F64] {
        assert_eq!(case(tc), Ok(()), "type code {tc}");
    }
    for tc in [type_code::STRING, type_code::U128, type_code::F32] {
        assert_eq!(
            case(tc),
            Err(ExprValidateErr::EmitSlotNotEightBytes { out: 0, type_code: tc }),
            "type code {tc}"
        );
    }
}

#[test]
fn test_validate_output_index_map_vs_filter() {
    // in: [U64 PK, I64]; out: [U64 PK, I64] — one output payload slot.
    let in_schema = TestSchema::new(&[(type_code::U64, false), (type_code::I64, false)], &[0]);
    let out_schema = TestSchema::new(&[(type_code::U64, false), (type_code::I64, false)], &[0]);
    // COPY_COL (34) src_col=0 out=200: rejected as a MAP (out_schema Some).
    let prog = LogicalProgram::from_wire(&[34, 0, 0, 200], 0, 0, vec![]).unwrap();
    assert_eq!(
        prog.validate(Some(&in_schema), Some(&out_schema)),
        Err(ExprValidateErr::OutputIdxOutOfRange {
            out: 200,
            num_payload_cols: 1,
        })
    );
    // The same output opcode in a FILTER (out_schema None) is a harmless no-op.
    assert_eq!(prog.validate(Some(&in_schema), None), Ok(()));
}

/// Output coverage: with an `out_schema`, every declared payload slot must be
/// written. Counted by popcount, so a duplicate destination — which leaves
/// another slot unwritten while the instruction count still matches — is caught
/// by the same rule.
#[test]
fn test_validate_rejects_unwritten_output_slot() {
    // in/out: [U64 PK, I64, I64] — two output payload slots.
    let schema = TestSchema::new(
        &[
            (type_code::U64, false),
            (type_code::I64, false),
            (type_code::I64, false),
        ],
        &[0],
    );
    let map = |quads: &[u32]| LogicalProgram::from_wire(quads, 0, 0, vec![]).unwrap();

    // Both slots written — accepted.
    assert_eq!(
        map(&[34, 0, 1, 0, 34, 0, 2, 1]).validate(Some(&schema), Some(&schema)),
        Ok(())
    );

    // Only slot 0 written.
    assert_eq!(
        map(&[34, 0, 1, 0]).validate(Some(&schema), Some(&schema)),
        Err(ExprValidateErr::OutputSlotUnwritten {
            written: 0b01,
            num_payload_cols: 2,
        })
    );

    // Two CopyCols, both onto slot 0: the count matches but slot 1 is unwritten.
    assert_eq!(
        map(&[34, 0, 1, 0, 34, 0, 2, 0]).validate(Some(&schema), Some(&schema)),
        Err(ExprValidateErr::OutputSlotUnwritten {
            written: 0b01,
            num_payload_cols: 2,
        })
    );

    // A predicate over the same program is unaffected — no output plan.
    assert_eq!(map(&[34, 0, 1, 0]).validate(Some(&schema), None), Ok(()));
}

#[test]
#[should_panic(expected = "RegisterAliasing")]
fn test_new_panics_on_aliased_register() {
    // A compiler-built (trusted) aliased-register program still panics from `new`.
    let _ = LogicalProgram::new(vec![LogicalInstr::IntAdd { dst: 0, a: 0, b: 1 }], 2, 0, vec![]);
}

// ---------------------------------------------------------------------------
// INT_IN_SET — set membership as one opcode (O(1) registers, O(log N) per row)
// ---------------------------------------------------------------------------

/// Pack an i64 set into the `N × 8-byte LE` pool layout the
/// const pool carries for `INT_IN_SET`.
fn pack_i64_set(values: &[i64]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(values.len() * 8);
    for v in values {
        bytes.extend_from_slice(&v.to_le_bytes());
    }
    bytes
}

/// `r0 = col1; r1 = r0 IN set`, result_reg = 1 — the compiled shape of
/// `col1 IN (…)`. `col_tc` picks col1's type (I64 / U64 / …).
fn in_set_prog(col_tc: u8, set: &[i64]) -> (TestSchema, Evaluator) {
    let schema = TestSchema::with_pk_at(0, &[8, col_tc]); // col0 = PK(U64), col1 = col_tc
    let instrs = vec![
        LogicalInstr::LoadColInt { dst: 0, col: 1 },
        LogicalInstr::IntInSet {
            dst: 1,
            value_reg: 0,
            set_idx: 0,
        },
    ];
    let prog = scalar_prog(&schema, instrs, 2, 1, vec![pack_i64_set(set)]);
    (schema, prog)
}

#[test]
fn test_int_in_set_hit_miss_null() {
    let (schema, prog) = in_set_prog(9 /* I64 */, &[1, 3, 5, 42]);

    // Hit: 42 ∈ {1,3,5,42}.
    let mb = make_int_view(&schema, &[(1, 0, &[42])]);
    assert_eq!(prog.eval_row(&mb, 0), (1, false));

    // Miss: 7 ∉ set.
    let mb = make_int_view(&schema, &[(1, 0, &[7])]);
    assert_eq!(prog.eval_row(&mb, 0), (0, false));

    // NULL operand ⇒ NULL out (col1 is payload index 0 → null_word bit 0).
    let mb = make_int_view(&schema, &[(1, 0b1, &[0])]);
    let (_v, is_null) = prog.eval_row(&mb, 0);
    assert!(is_null, "NULL operand must produce a NULL membership result");
}

#[test]
fn test_int_in_set_u64_neg_one_matches_max() {
    // `u64col IN (-1)`: the column loads as i64 -1 (bare bit-reinterpret) and the
    // folded literal -1 is i64 -1, so u64::MAX matches — same as the OR-chain's
    // bit-equal `col = -1`.
    let (schema, prog) = in_set_prog(8 /* U64 */, &[-1]);
    let mb = make_int_view(&schema, &[(1, 0, &[u64::MAX as i64])]);
    assert_eq!(prog.eval_row(&mb, 0), (1, false));
    // A different u64 value misses.
    let mb = make_int_view(&schema, &[(1, 0, &[7])]);
    assert_eq!(prog.eval_row(&mb, 0), (0, false));
}

#[test]
fn test_int_in_set_signed_negatives_by_signed_order() {
    // Signed set with negatives, sorted ascending in signed i64 order.
    let (schema, prog) = in_set_prog(9 /* I64 */, &[-5, -1, 0, 3]);
    for (v, want) in [(-5i64, 1), (-1, 1), (0, 1), (3, 1), (2, 0), (100, 0)] {
        let mb = make_int_view(&schema, &[(1, 0, &[v])]);
        assert_eq!(prog.eval_row(&mb, 0), (want, false), "value {v} membership");
    }
}

#[test]
fn test_int_in_set_1000_elements_compiles_and_evals() {
    // The whole point: a 1000-element set is O(1) registers (the OR-chain would
    // have needed ~4000, blowing the 64-register cap with TooManyRegs).
    let set: Vec<i64> = (0..1000).collect();
    let (schema, prog) = in_set_prog(9 /* I64 */, &set);
    assert_eq!(
        prog.prog.num_regs, 2,
        "membership uses two registers regardless of set size"
    );
    let mb = make_int_view(&schema, &[(1, 0, &[777])]);
    assert_eq!(prog.eval_row(&mb, 0), (1, false));
    let mb = make_int_view(&schema, &[(1, 0, &[1000])]);
    assert_eq!(prog.eval_row(&mb, 0), (0, false));
}

#[test]
fn test_int_not_in_set_null_operand_excluded() {
    // NOT IN = bool_not(IN). A NULL operand makes IN NULL, NOT(NULL) NULL — the
    // row is excluded (3VL), matching the OR-chain's `NOT(NULL) = NULL`.
    let schema = schema_pk_ints(1, true); // col0 = PK(U64), col1 = I64
    let instrs = vec![
        LogicalInstr::LoadColInt { dst: 0, col: 1 },
        LogicalInstr::IntInSet {
            dst: 1,
            value_reg: 0,
            set_idx: 0,
        },
        LogicalInstr::BoolNot { dst: 2, a: 1 },
    ];
    let prog = scalar_prog(&schema, instrs, 3, 2, vec![pack_i64_set(&[1, 2, 3])]);
    // NULL operand → NOT IN is NULL (excluded).
    let mb = make_int_view(&schema, &[(1, 0b1, &[0])]);
    assert!(prog.eval_row(&mb, 0).1, "NOT IN NULL must be NULL");
    // A non-member is included by NOT IN.
    let mb = make_int_view(&schema, &[(1, 0, &[9])]);
    assert_eq!(prog.eval_row(&mb, 0), (1, false));
    // A member is excluded by NOT IN.
    let mb = make_int_view(&schema, &[(1, 0, &[2])]);
    assert_eq!(prog.eval_row(&mb, 0), (0, false));
}

#[test]
fn test_int_in_set_empty_pool_always_false() {
    // A zero-length pool matches nothing (binary_search on `[]` is always Err),
    // and `len % 8 == 0` so it validates.
    let (schema, prog) = in_set_prog(9 /* I64 */, &[]);
    let mb = make_int_view(&schema, &[(1, 0, &[42])]);
    assert_eq!(prog.eval_row(&mb, 0), (0, false));
}

/// The wire pool order is not trusted: `resolve` sorts it, because the kernel
/// binary-searches it and the only sort otherwise happens in the client planner.
#[test]
fn test_int_in_set_unsorted_pool_is_sorted_at_resolve() {
    let (schema, prog) = in_set_prog(9 /* I64 */, &[42, -1, 7, 3]);
    assert_eq!(prog.prog.int_sets[0], vec![-1, 3, 7, 42]);
    // A binary search over the raw descending-ish order would miss 7 (it sits
    // past the first probe's `42 > 7` left turn).
    for v in [-1i64, 3, 7, 42] {
        let mb = make_int_view(&schema, &[(1, 0, &[v])]);
        assert_eq!(prog.eval_row(&mb, 0), (1, false), "value {v} must be found");
    }
    let mb = make_int_view(&schema, &[(1, 0, &[8])]);
    assert_eq!(prog.eval_row(&mb, 0), (0, false));
}

#[test]
fn test_int_in_set_validate_rejects_misaligned_pool() {
    // A pool whose length is not a multiple of 8 is a clean IntSetNotAligned,
    // not a silent chunks_exact tail-drop at resolve. `from_wire` runs the
    // structure-only validate, so it rejects here.
    // Quad: [INT_IN_SET=46, dst=1, value_reg=0, set_idx=0], pool of 5 bytes.
    assert_eq!(
        wire_err(LogicalProgram::from_wire(&[46, 1, 0, 0], 2, 1, vec![vec![0u8; 5]])),
        ExprValidateErr::IntSetNotAligned { set_idx: 0, len: 5 }
    );
}

#[test]
fn test_int_in_set_validate_rejects_out_of_range_set_idx() {
    // set_idx = 9 against a pool of length 1 → ConstIdxOutOfRange.
    assert_eq!(
        wire_err(LogicalProgram::from_wire(&[46, 1, 0, 9], 2, 1, vec![vec![0u8; 8]])),
        ExprValidateErr::ConstIdxOutOfRange { const_idx: 9, n: 1 }
    );
}

/// Classifier: every CMP and every AND in a pure conjunction is bit_only
/// (with `is_filter=true`); result_reg stays bit_only.
#[test]
fn classifier_pure_conjunction_filter() {
    let schema = schema_pk_ints(2, true);
    let prog = filter_prog(
        &schema,
        vec![
            LogicalInstr::LoadColInt { dst: 0, col: 1 },
            LogicalInstr::LoadConst { dst: 1, val: 1 },
            LogicalInstr::Cmp {
                op: CmpOp::Gt,
                dst: 2,
                a: 0,
                b: 1,
            },
            LogicalInstr::LoadColInt { dst: 3, col: 2 },
            LogicalInstr::Cmp {
                op: CmpOp::Gt,
                dst: 4,
                a: 3,
                b: 1,
            },
            LogicalInstr::BoolAnd { dst: 5, a: 2, b: 4 },
        ],
        6,
        5,
        vec![],
    )
    .prog;
    let RegisterRoles {
        bit_only, bool_input, ..
    } = classify_registers(&prog.instrs, 5, true);
    // Bool producers: r2 (CMP_GT), r4 (CMP_GT), r5 (BOOL_AND).
    // Non-bool readers consume r0/r1/r3 (CMPs read them as i64), so those
    // never qualify for bit_only. r2/r4 are read only by BOOL_AND, and r5
    // (result_reg) stays bit_only under is_filter=true.
    let expected_bit_only = (1u64 << 2) | (1u64 << 4) | (1u64 << 5);
    assert_eq!(
        bit_only, expected_bit_only,
        "expected r2/r4/r5 bit_only; got mask {bit_only:#010b}"
    );
    // r2 and r4 are read by BOOL_AND; r5 (result_reg) is force-marked a bool
    // input so the filter's nullable word merge always finds it packed.
    assert_eq!(bool_input, (1 << 2) | (1 << 4) | (1 << 5));
}

// ---------------------------------------------------------------------------
// Numeric scalar functions and numeric CAST: validation and the resolve-time
// analyses (U64 tracking, nullability classification).
// ---------------------------------------------------------------------------

/// The cast target rides the `a2` word, so a forged blob can put anything
/// there. It must be rejected before eval, where it would index a bounds table
/// that has no arm for it.
#[test]
fn test_validate_rejects_a_forged_cast_target() {
    for op in [gnitz_wire::EXPR_INT_CAST, gnitz_wire::EXPR_FLOAT_TO_INT] {
        for tc in [
            0u32,
            type_code::STRING as u32,
            type_code::U128 as u32,
            type_code::F64 as u32,
            255,
            // A `>= 255` word must not be truncated into a valid code on the
            // way in: the full u32 has to reach `validate`, or these would
            // pass as I64.
            0x100u32 | type_code::I64 as u32,
            0x1_0000u32 | type_code::I64 as u32,
        ] {
            let code = [gnitz_wire::EXPR_LOAD_COL_INT, 0, 1, 0, op, 1, 0, tc];
            assert_eq!(
                wire_err(LogicalProgram::from_wire(&code, 2, 1, vec![])),
                ExprValidateErr::BadCastTarget { tc },
                "op {op} tc {tc}"
            );
        }
        // Control: every fixed-int target is accepted.
        for tc in [
            type_code::I8,
            type_code::U8,
            type_code::I16,
            type_code::U16,
            type_code::I32,
            type_code::U32,
            type_code::I64,
            type_code::U64,
        ] {
            let code = [gnitz_wire::EXPR_LOAD_COL_INT, 0, 1, 0, op, 1, 0, tc as u32];
            assert!(
                LogicalProgram::from_wire(&code, 2, 1, vec![]).is_ok(),
                "op {op} tc {tc} must be accepted"
            );
        }
    }
}

#[test]
fn test_validate_bounds_checks_the_new_register_operands() {
    let unary = [
        gnitz_wire::EXPR_INT_ABS,
        gnitz_wire::EXPR_FLOAT_ABS,
        gnitz_wire::EXPR_FLOAT_FLOOR,
        gnitz_wire::EXPR_FLOAT_CEIL,
        gnitz_wire::EXPR_FLOAT_ROUND,
        gnitz_wire::EXPR_FLOAT_TRUNC,
        gnitz_wire::EXPR_FLOAT_TO_F32,
    ];
    for op in unary {
        // dst out of range, then operand out of range.
        assert!(matches!(
            wire_err(LogicalProgram::from_wire(&[op, 9, 0, 0], 2, 0, vec![])),
            ExprValidateErr::RegOutOfRange { .. }
        ));
        assert!(matches!(
            wire_err(LogicalProgram::from_wire(&[op, 0, 9, 0], 2, 0, vec![])),
            ExprValidateErr::RegOutOfRange { .. }
        ));
    }
    for op in [
        gnitz_wire::EXPR_INT_MAX2,
        gnitz_wire::EXPR_INT_MIN2,
        gnitz_wire::EXPR_FLOAT_MAX2,
        gnitz_wire::EXPR_FLOAT_MIN2,
    ] {
        assert!(matches!(
            wire_err(LogicalProgram::from_wire(&[op, 0, 1, 9], 2, 0, vec![])),
            ExprValidateErr::RegOutOfRange { .. }
        ));
        // The binary ops are SSA anti-aliased: dst may not be an operand.
        assert!(
            LogicalProgram::from_wire(&[op, 0, 0, 1], 2, 0, vec![]).is_err(),
            "op {op}: dst == a must be rejected"
        );
    }
}

/// The three casts manufacture NULL out of a non-NULL input, so a program
/// carrying one can never take the `no_nulls` fast path; the pure transforms
/// only propagate and must not disturb it.
#[test]
fn test_no_nulls_classification_of_the_new_opcodes() {
    let nonnull = TestSchema::new(&[(type_code::U64, false), (type_code::I64, false)], &[0]);
    let with = |instr: LogicalInstr| {
        let instrs = vec![LogicalInstr::LoadColInt { dst: 0, col: 1 }, instr];
        scalar_prog(&nonnull, instrs, 2, 1, vec![]).prog.no_nulls
    };
    for instr in [
        LogicalInstr::IntUnary {
            op: IntUnaryOp::Abs,
            dst: 1,
            a: 0,
        },
        LogicalInstr::FloatUnary {
            op: super::FloatUnaryOp::Round,
            dst: 1,
            a: 0,
        },
        LogicalInstr::IntMinMax2 {
            dst: 1,
            a: 0,
            b: 0,
            is_max: true,
        },
    ] {
        assert!(with(instr), "a propagating transform keeps no_nulls");
    }
    for instr in [
        LogicalInstr::FloatToF32 { dst: 1, a: 0 },
        LogicalInstr::IntCast {
            dst: 1,
            a: 0,
            tc: type_code::I8 as u32,
        },
        LogicalInstr::FloatToInt {
            dst: 1,
            a: 0,
            tc: type_code::I8 as u32,
        },
    ] {
        assert!(!with(instr), "a narrowing cast manufactures NULL");
    }
}

/// `IntMinMax2` picks its compare domain from the U64 tracking of BOTH
/// operands, and re-taints its own dst — which is what makes the lowering's
/// fold-head rotation sufficient to fix the domain for a whole n-ary fold.
#[test]
fn test_min_max2_compare_domain_and_u64_propagation() {
    // pk(U64), col1 = U64, col2 = I64.
    let schema = TestSchema::with_pk_at(0, &[type_code::U64, type_code::U64, type_code::I64]);
    let signed_of = |a_col: u32, b_col: u32| {
        let instrs = vec![
            LogicalInstr::LoadColInt { dst: 0, col: a_col },
            LogicalInstr::LoadColInt { dst: 1, col: b_col },
            LogicalInstr::IntMinMax2 {
                dst: 2,
                a: 0,
                b: 1,
                is_max: true,
            },
            // A second fold against the signed column reads dst's taint.
            LogicalInstr::LoadColInt { dst: 3, col: 2 },
            LogicalInstr::IntMinMax2 {
                dst: 4,
                a: 2,
                b: 3,
                is_max: true,
            },
        ];
        let prog = scalar_prog(&schema, instrs, 5, 4, vec![]);
        prog.prog
            .instrs
            .iter()
            .filter_map(|i| match i {
                Instr::IntMinMax2 { signed, .. } => Some(*signed),
                _ => None,
            })
            .collect::<Vec<_>>()
    };
    // Both signed: signed compare throughout.
    assert_eq!(signed_of(2, 2), vec![true, true]);
    // Either operand U64-tracked makes the fold unsigned, and the taint is
    // sticky, so the follow-on fold against a signed column stays unsigned.
    assert_eq!(signed_of(1, 2), vec![false, false]);
    assert_eq!(signed_of(2, 1), vec![false, false]);
}

/// An emitted `INT_CAST` re-seeds the U64 tracking from its TARGET, which is
/// what lets `CAST(x AS BIGINT UNSIGNED)` drive a downstream unsigned compare.
#[test]
fn test_int_cast_reseeds_u64_tracking_from_its_target() {
    // pk(U64), col1 = I64 (signed-tracked), col2 = U64.
    let schema = TestSchema::with_pk_at(0, &[type_code::U64, type_code::I64, type_code::U64]);
    let cmp_signed_after = |tc: u8, src_col: u32| {
        let instrs = vec![
            LogicalInstr::LoadColInt { dst: 0, col: src_col },
            LogicalInstr::IntCast {
                dst: 1,
                a: 0,
                tc: tc as u32,
            },
            LogicalInstr::LoadConst { dst: 2, val: 0 },
            LogicalInstr::Cmp {
                op: CmpOp::Gt,
                dst: 3,
                a: 1,
                b: 2,
            },
        ];
        let prog = scalar_prog(&schema, instrs, 4, 3, vec![]);
        prog.prog
            .instrs
            .iter()
            .find_map(|i| match i {
                Instr::Cmp { signed, .. } => Some(*signed),
                _ => None,
            })
            .expect("the compare survives")
    };
    assert!(!cmp_signed_after(type_code::U64, 1), "U64 target taints the dst");
    assert!(
        cmp_signed_after(type_code::I64, 2),
        "a signed target clears a U64 source's taint"
    );
    assert!(cmp_signed_after(type_code::I32, 1));
}

/// `src_signed` is resolved from the OPERAND's tracking, not from the target —
/// it is what decides whether the range check reads the register as i64 or u64.
#[test]
fn test_int_cast_records_the_source_signedness() {
    let schema = TestSchema::with_pk_at(0, &[type_code::U64, type_code::I64, type_code::U64]);
    let src_signed_of = |src_col: u32| {
        let instrs = vec![
            LogicalInstr::LoadColInt { dst: 0, col: src_col },
            LogicalInstr::IntCast {
                dst: 1,
                a: 0,
                tc: type_code::I8 as u32,
            },
        ];
        let prog = scalar_prog(&schema, instrs, 2, 1, vec![]);
        prog.prog
            .instrs
            .iter()
            .find_map(|i| match i {
                Instr::IntCast { src_signed, .. } => Some(*src_signed),
                _ => None,
            })
            .expect("the cast survives")
    };
    assert!(src_signed_of(1), "an I64 column is a signed source");
    assert!(!src_signed_of(2), "a U64 column is an unsigned source");
}
