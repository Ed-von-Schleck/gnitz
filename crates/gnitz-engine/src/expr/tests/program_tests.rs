// 3.14 is a deliberate float-bit-pattern test fixture, not an approximation
// of PI meant to be replaced with std::f64::consts::PI.
#![allow(clippy::approx_constant)]

use super::super::program::*;
use super::{bits_to_float, eval_predicate_via_batch as eval_predicate, eval_with_emit_via_batch, float_to_bits};
use crate::schema::{SchemaColumn, SchemaDescriptor, MAX_COLUMNS};
use crate::storage::Batch;

/// Build a `LogicalProgram` from typed instructions and resolve it against the
/// schema in one step. Eval entry points take `&ResolvedProgram`, so every test
/// prog must be resolved before being passed to `eval_predicate`/`eval_with_emit`.
fn make_prog(
    schema: &SchemaDescriptor,
    instrs: Vec<LogicalInstr>,
    num_regs: u32,
    result_reg: u32,
    const_strings: Vec<Vec<u8>>,
) -> ResolvedProgram {
    LogicalProgram::new(instrs, num_regs, result_reg, const_strings).resolve(schema, /* is_filter = */ false)
}

fn make_schema(pk_index: u32, col_types: &[u8]) -> SchemaDescriptor {
    let mut columns = [SchemaColumn::new(0, 0); MAX_COLUMNS];
    for (i, &tc) in col_types.iter().enumerate() {
        let nullable = if i == pk_index as usize { 0 } else { 1 };
        columns[i] = SchemaColumn::new(tc, nullable);
    }
    SchemaDescriptor::new(&columns[..col_types.len()], &[pk_index])
}

fn make_int_batch(schema: &SchemaDescriptor, rows: &[(u64, i64, u64, &[i64])]) -> Batch {
    let mut batch = Batch::with_schema(*schema, rows.len().max(1));
    batch.count = 0;
    for &(pk, weight, null_word, cols) in rows {
        batch.extend_pk(pk as u128);
        batch.extend_weight(&weight.to_le_bytes());
        batch.extend_null_bmp(&null_word.to_le_bytes());
        for (pi, _ci, _col) in schema.payload_columns() {
            if pi < cols.len() {
                batch.extend_col(pi, &cols[pi].to_le_bytes());
            }
        }
        batch.count += 1;
    }
    batch
}

#[test]
fn test_int_comparisons() {
    // Schema: 2 columns, col0=PK(U64), col1=I64
    let schema = make_schema(0, &[8, 9]);
    let batch = make_int_batch(&schema, &[(1, 1, 0, &[42])]);
    let mb = batch.as_mem_batch();

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
    let prog = make_prog(&schema, instrs, 3, 2, vec![]);
    let (val, is_null) = eval_predicate(&prog, &mb, 0);
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
    let prog = make_prog(&schema, instrs, 3, 2, vec![]);
    let (val, _) = eval_predicate(&prog, &mb, 0);
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
    let prog = make_prog(&schema, instrs, 3, 2, vec![]);
    let (val, _) = eval_predicate(&prog, &mb, 0);
    assert_eq!(val, 1);
}

#[test]
fn test_int_arithmetic() {
    let schema = make_schema(0, &[8, 9, 9]);
    let batch = make_int_batch(&schema, &[(1, 1, 0, &[10, 3])]);
    let mb = batch.as_mem_batch();

    // ADD: 10 + 3 = 13
    let instrs = vec![
        LogicalInstr::LoadColInt { dst: 0, col: 1 },
        LogicalInstr::LoadColInt { dst: 1, col: 2 },
        LogicalInstr::IntAdd { dst: 2, a: 0, b: 1 },
    ];
    let prog = make_prog(&schema, instrs, 3, 2, vec![]);
    let (val, _) = eval_predicate(&prog, &mb, 0);
    assert_eq!(val, 13);

    // DIV by zero → NULL
    let instrs = vec![
        LogicalInstr::LoadColInt { dst: 0, col: 1 },
        LogicalInstr::LoadConst { dst: 1, val: 0 },
        LogicalInstr::IntDiv { dst: 2, a: 0, b: 1 },
    ];
    let prog = make_prog(&schema, instrs, 3, 2, vec![]);
    let (_, is_null) = eval_predicate(&prog, &mb, 0);
    assert!(is_null);

    // MOD by zero → NULL
    let instrs = vec![
        LogicalInstr::LoadColInt { dst: 0, col: 1 },
        LogicalInstr::LoadConst { dst: 1, val: 0 },
        LogicalInstr::IntMod { dst: 2, a: 0, b: 1 },
    ];
    let prog = make_prog(&schema, instrs, 3, 2, vec![]);
    let (_, is_null) = eval_predicate(&prog, &mb, 0);
    assert!(is_null);

    // NEG: -10
    let instrs = vec![
        LogicalInstr::LoadColInt { dst: 0, col: 1 },
        LogicalInstr::IntNeg { dst: 1, a: 0 },
    ];
    let prog = make_prog(&schema, instrs, 2, 1, vec![]);
    let (val, _) = eval_predicate(&prog, &mb, 0);
    assert_eq!(val, -10);
}

#[test]
fn test_float_arithmetic_and_comparison() {
    let schema = make_schema(0, &[8, 10, 10]);
    // Store floats as i64 bits
    let a_bits = float_to_bits(3.14);
    let b_bits = float_to_bits(2.0);
    let batch = make_int_batch(&schema, &[(1, 1, 0, &[a_bits, b_bits])]);
    let mb = batch.as_mem_batch();

    // FLOAT_ADD: 3.14 + 2.0
    let instrs = vec![
        LogicalInstr::LoadColFloat { dst: 0, col: 1 },
        LogicalInstr::LoadColFloat { dst: 1, col: 2 },
        LogicalInstr::FloatAdd { dst: 2, a: 0, b: 1 },
    ];
    let prog = make_prog(&schema, instrs, 3, 2, vec![]);
    let (val, _) = eval_predicate(&prog, &mb, 0);
    let result = bits_to_float(val);
    assert!((result - 5.14).abs() < 1e-10);

    // FLOAT_DIV by zero → NULL
    let instrs = vec![
        LogicalInstr::LoadColFloat { dst: 0, col: 1 },
        LogicalInstr::LoadConst { dst: 1, val: 0 }, // zero bits
        LogicalInstr::FloatDiv { dst: 2, a: 0, b: 1 },
    ];
    let prog = make_prog(&schema, instrs, 3, 2, vec![]);
    let (_, is_null) = eval_predicate(&prog, &mb, 0);
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
    let prog = make_prog(&schema, instrs, 3, 2, vec![]);
    let (val, _) = eval_predicate(&prog, &mb, 0);
    assert_eq!(val, 1);
}

#[test]
fn test_null_propagation() {
    let schema = make_schema(0, &[8, 9, 9]);
    // col1 is null (bit 0 set), col2 is not null
    let batch = make_int_batch(&schema, &[(1, 1, 1, &[0, 5])]);
    let mb = batch.as_mem_batch();

    // ADD with one null operand → null
    let instrs = vec![
        LogicalInstr::LoadColInt { dst: 0, col: 1 },
        LogicalInstr::LoadColInt { dst: 1, col: 2 },
        LogicalInstr::IntAdd { dst: 2, a: 0, b: 1 },
    ];
    let prog = make_prog(&schema, instrs, 3, 2, vec![]);
    let (_, is_null) = eval_predicate(&prog, &mb, 0);
    assert!(is_null);
}

#[test]
fn test_is_null_is_not_null() {
    let schema = make_schema(0, &[8, 9, 9]);
    // col1 is null (bit 0 set), col2 is not null
    let batch = make_int_batch(&schema, &[(1, 1, 1, &[0, 5])]);
    let mb = batch.as_mem_batch();

    // IS_NULL(col1) → 1 (always non-null result)
    let instrs = vec![LogicalInstr::IsNull { dst: 0, col: 1 }];
    let prog = make_prog(&schema, instrs, 1, 0, vec![]);
    let (val, is_null) = eval_predicate(&prog, &mb, 0);
    assert_eq!(val, 1);
    assert!(!is_null);

    // IS_NOT_NULL(col1) → 0
    let instrs = vec![LogicalInstr::IsNotNull { dst: 0, col: 1 }];
    let prog = make_prog(&schema, instrs, 1, 0, vec![]);
    let (val, is_null) = eval_predicate(&prog, &mb, 0);
    assert_eq!(val, 0);
    assert!(!is_null);

    // IS_NULL(col2) → 0
    let instrs = vec![LogicalInstr::IsNull { dst: 0, col: 2 }];
    let prog = make_prog(&schema, instrs, 1, 0, vec![]);
    let (val, _) = eval_predicate(&prog, &mb, 0);
    assert_eq!(val, 0);
}

#[test]
fn test_boolean_combinators() {
    let schema = make_schema(0, &[8, 9]);
    let batch = make_int_batch(&schema, &[(1, 1, 0, &[1])]);
    let mb = batch.as_mem_batch();

    // AND(1, 0) → 0
    let instrs = vec![
        LogicalInstr::LoadColInt { dst: 0, col: 1 },
        LogicalInstr::LoadConst { dst: 1, val: 0 },
        LogicalInstr::BoolAnd { dst: 2, a: 0, b: 1 },
    ];
    let prog = make_prog(&schema, instrs, 3, 2, vec![]);
    let (val, _) = eval_predicate(&prog, &mb, 0);
    assert_eq!(val, 0);

    // OR(1, 0) → 1
    let instrs = vec![
        LogicalInstr::LoadColInt { dst: 0, col: 1 },
        LogicalInstr::LoadConst { dst: 1, val: 0 },
        LogicalInstr::BoolOr { dst: 2, a: 0, b: 1 },
    ];
    let prog = make_prog(&schema, instrs, 3, 2, vec![]);
    let (val, _) = eval_predicate(&prog, &mb, 0);
    assert_eq!(val, 1);

    // NOT(1) → 0
    let instrs = vec![
        LogicalInstr::LoadColInt { dst: 0, col: 1 },
        LogicalInstr::BoolNot { dst: 1, a: 0 },
    ];
    let prog = make_prog(&schema, instrs, 2, 1, vec![]);
    let (val, _) = eval_predicate(&prog, &mb, 0);
    assert_eq!(val, 0);
}

#[test]
fn test_load_const_encoding() {
    let schema = make_schema(0, &[8, 9]);
    let batch = make_int_batch(&schema, &[(1, 1, 0, &[0])]);
    let mb = batch.as_mem_batch();

    // Test large constant: 0x00000001_00000002 = (1 << 32) | 2 = 4294967298
    // Wire form: lo 32 bits = 2, hi 32 bits = 1.
    let instrs = vec![LogicalInstr::LoadConst {
        dst: 0,
        val: ((1i64) << 32) | (2i64 & 0xFFFF_FFFF),
    }];
    let prog = make_prog(&schema, instrs, 1, 0, vec![]);
    let (val, is_null) = eval_predicate(&prog, &mb, 0);
    assert!(!is_null);
    assert_eq!(val, (1i64 << 32) | 2);

    // Test negative constant: -1 (the wire low/high split is reconstructed by
    // `from_wire`; the typed instruction carries the full i64 value directly).
    let instrs = vec![LogicalInstr::LoadConst { dst: 0, val: -1 }];
    let prog = make_prog(&schema, instrs, 1, 0, vec![]);
    let (val, _) = eval_predicate(&prog, &mb, 0);
    assert_eq!(val, -1);
}

#[test]
fn test_string_eq_const() {
    let schema = make_schema(0, &[8, 11]);
    // Build a batch with a short string "hello"
    let mut batch = Batch::with_schema(schema, 1);
    batch.count = 0;
    batch.extend_pk(1u128);
    batch.extend_weight(&1i64.to_le_bytes());
    batch.extend_null_bmp(&0u64.to_le_bytes());
    // German string struct for "hello" (5 bytes, inline)
    let mut gs = [0u8; 16];
    gs[0..4].copy_from_slice(&5u32.to_le_bytes()); // length = 5
    gs[4..9].copy_from_slice(b"hello"); // prefix(4) + suffix(1) inline
    batch.extend_col(0, &gs);
    batch.count = 1;

    let mb = batch.as_mem_batch();

    // STR_COL_EQ_CONST(col1, "hello") → 1
    let instrs = vec![LogicalInstr::StrColConst {
        op: StrOp::Eq,
        dst: 0,
        col: 1,
        const_idx: 0,
    }];
    let prog = make_prog(&schema, instrs, 1, 0, vec![b"hello".to_vec()]);
    let (val, is_null) = eval_predicate(&prog, &mb, 0);
    assert!(!is_null);
    assert_eq!(val, 1);

    // STR_COL_EQ_CONST(col1, "world") → 0
    let instrs = vec![LogicalInstr::StrColConst {
        op: StrOp::Eq,
        dst: 0,
        col: 1,
        const_idx: 0,
    }];
    let prog = make_prog(&schema, instrs, 1, 0, vec![b"world".to_vec()]);
    let (val, _) = eval_predicate(&prog, &mb, 0);
    assert_eq!(val, 0);
}

#[test]
fn test_int_to_float() {
    let schema = make_schema(0, &[8, 9]);
    let batch = make_int_batch(&schema, &[(1, 1, 0, &[42])]);
    let mb = batch.as_mem_batch();

    let instrs = vec![
        LogicalInstr::LoadColInt { dst: 0, col: 1 },
        LogicalInstr::IntToFloat { dst: 1, a: 0 },
    ];
    let prog = make_prog(&schema, instrs, 2, 1, vec![]);
    let (val, _) = eval_predicate(&prog, &mb, 0);
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
    let schema = make_schema(0, &[8, 8]);

    // v = u64::MAX (bit pattern 0xFFFF...FF, i.e. -1 as i64); divisor const = 2.
    // Unsigned: MAX > 100, MAX/2 = 9223372036854775807, MAX%2 = 1.
    // Signed:   -1 < 100 (false), -1/2 = 0,             -1%2 = -1.
    let v_bits = u64::MAX as i64; // == -1i64
    let batch = make_int_batch(&schema, &[(1, 1, 0, &[v_bits])]);
    let mb = batch.as_mem_batch();

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
    let prog_gt = make_prog(&schema, instrs, 3, 2, vec![]);
    // Instruction 2 is the comparison after resolution; the U64 operand must
    // select the unsigned form.
    assert!(
        matches!(
            prog_gt.instrs[2],
            Instr::Cmp {
                op: CmpOp::Gt,
                signed: false,
                ..
            }
        ),
        "U64 operand must select unsigned CMP_GT"
    );
    let (val, is_null) = eval_predicate(&prog_gt, &mb, 0);
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
    let prog_div = make_prog(&schema, instrs, 3, 2, vec![]);
    assert!(
        matches!(prog_div.instrs[2], Instr::IntDiv { signed: false, .. }),
        "U64 operand must select unsigned IntDiv"
    );
    let (val, is_null) = eval_predicate(&prog_div, &mb, 0);
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
    let prog_mod = make_prog(&schema, instrs, 3, 2, vec![]);
    assert!(
        matches!(prog_mod.instrs[2], Instr::IntMod { signed: false, .. }),
        "U64 operand must select unsigned IntMod"
    );
    let (val, is_null) = eval_predicate(&prog_mod, &mb, 0);
    assert!(!is_null);
    assert_eq!(val, 1, "u64::MAX % 2 == 1 (unsigned); signed -1 % 2 would be -1",);

    // (d) CAST col1 to float  → IntToFloat must resolve to the unsigned form.
    let instrs = vec![
        LogicalInstr::LoadColInt { dst: 0, col: 1 }, // r0 = col1 (U64)
        LogicalInstr::IntToFloat { dst: 1, a: 0 },   // r1 = (f64) r0
    ];
    let prog_cast = make_prog(&schema, instrs, 2, 1, vec![]);
    // Instruction 1 is the cast after resolution; the U64 operand must select
    // the unsigned form.
    assert!(
        matches!(prog_cast.instrs[1], Instr::IntToFloat { signed: false, .. }),
        "U64 operand must select unsigned IntToFloat"
    );
    let (val, is_null) = eval_predicate(&prog_cast, &mb, 0);
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
    let schema = make_schema(0, &[8, 9, 9]);
    let batch = make_int_batch(&schema, &[(1, 1, 0, &[10, 20])]);
    let mb = batch.as_mem_batch();

    // Compute col1 + col2, EMIT to payload col 0
    let instrs = vec![
        LogicalInstr::LoadColInt { dst: 0, col: 1 },
        LogicalInstr::LoadColInt { dst: 1, col: 2 },
        LogicalInstr::IntAdd { dst: 2, a: 0, b: 1 },
        LogicalInstr::Emit { src: 2, out: 0 }, // emit r2 to payload col 0
    ];
    let prog = make_prog(&schema, instrs, 3, 2, vec![]);

    let (val, is_null, mask, emit_vals) = eval_with_emit_via_batch(&prog, &mb, 0);
    assert_eq!(val, 30); // 10 + 20
    assert!(!is_null);
    assert_eq!(mask, 0);
    assert_eq!(emit_vals[0], 30);
}

#[test]
fn test_div_by_zero_null_semantics() {
    // Schema: pk(u64), col1(i64), col2(i64)
    let schema = make_schema(0, &[8, 9, 9]);
    // Row: pk=1, col1=10, col2=3; null_word=0 (no nulls)
    let batch = make_int_batch(&schema, &[(1, 1, 0, &[10, 3])]);
    let mb = batch.as_mem_batch();

    // 1. INT_DIV by literal 0 → NULL
    let instrs = vec![
        LogicalInstr::LoadColInt { dst: 0, col: 1 },
        LogicalInstr::LoadConst { dst: 1, val: 0 },
        LogicalInstr::IntDiv { dst: 2, a: 0, b: 1 },
    ];
    let prog = make_prog(&schema, instrs, 3, 2, vec![]);
    let (_, is_null) = eval_predicate(&prog, &mb, 0);
    assert!(is_null);

    // 2. INT_MOD by literal 0 → NULL
    let instrs = vec![
        LogicalInstr::LoadColInt { dst: 0, col: 1 },
        LogicalInstr::LoadConst { dst: 1, val: 0 },
        LogicalInstr::IntMod { dst: 2, a: 0, b: 1 },
    ];
    let prog = make_prog(&schema, instrs, 3, 2, vec![]);
    let (_, is_null) = eval_predicate(&prog, &mb, 0);
    assert!(is_null);

    // 3. FLOAT_DIV by 0.0 bits → NULL
    let instrs = vec![
        LogicalInstr::LoadColInt { dst: 0, col: 1 },
        LogicalInstr::LoadConst { dst: 1, val: 0 },
        LogicalInstr::FloatDiv { dst: 2, a: 0, b: 1 },
    ];
    let prog = make_prog(&schema, instrs, 3, 2, vec![]);
    let (_, is_null) = eval_predicate(&prog, &mb, 0);
    assert!(is_null);

    // 4. INT_DIV by non-null non-zero → correct quotient, not null
    let instrs = vec![
        LogicalInstr::LoadColInt { dst: 0, col: 1 },
        LogicalInstr::LoadColInt { dst: 1, col: 2 },
        LogicalInstr::IntDiv { dst: 2, a: 0, b: 1 },
    ];
    let prog = make_prog(&schema, instrs, 3, 2, vec![]);
    let (val, is_null) = eval_predicate(&prog, &mb, 0);
    assert!(!is_null);
    assert_eq!(val, 3); // 10 / 3 = 3

    // 5. INT_DIV where divisor column is null → NULL (null propagation)
    // null_word bit 0 = col1 null; use col2 (bit 1) as divisor with col1 null
    // Schema col indices: col1=payload_idx 0, col2=payload_idx 1
    // Build a row where col2 is null (null_word bit 1 set)
    let batch_null_div = make_int_batch(&schema, &[(1, 1, 2, &[10, 3])]);
    let mb_null_div = batch_null_div.as_mem_batch();
    let instrs = vec![
        LogicalInstr::LoadColInt { dst: 0, col: 1 },
        LogicalInstr::LoadColInt { dst: 1, col: 2 },
        LogicalInstr::IntDiv { dst: 2, a: 0, b: 1 },
    ];
    let prog = make_prog(&schema, instrs, 3, 2, vec![]);
    let (_, is_null) = eval_predicate(&prog, &mb_null_div, 0);
    assert!(is_null);

    // 6. EMIT of INT_DIV-by-zero result → emit_null_mask bit set, buffer contains 0
    let instrs = vec![
        LogicalInstr::LoadColInt { dst: 0, col: 1 },
        LogicalInstr::LoadConst { dst: 1, val: 0 },
        LogicalInstr::IntDiv { dst: 2, a: 0, b: 1 },
        LogicalInstr::Emit { src: 2, out: 0 },
    ];
    let prog = make_prog(&schema, instrs, 3, 2, vec![]);
    let (_, _, mask, emit_vals) = eval_with_emit_via_batch(&prog, &mb, 0);
    assert_eq!(mask & 1, 1); // bit 0 set → null
    assert_eq!(emit_vals[0], 0);
}

#[test]
fn test_cmp_ge_lt_le() {
    let schema = make_schema(0, &[8, 9]);
    let batch = make_int_batch(&schema, &[(1, 1, 0, &[42])]);
    let mb = batch.as_mem_batch();

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
    let prog = make_prog(&schema, instrs, 3, 2, vec![]);
    let (val, _) = eval_predicate(&prog, &mb, 0);
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
    let prog = make_prog(&schema, instrs, 3, 2, vec![]);
    let (val, _) = eval_predicate(&prog, &mb, 0);
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
    let prog = make_prog(&schema, instrs, 3, 2, vec![]);
    let (val, _) = eval_predicate(&prog, &mb, 0);
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
    let prog = make_prog(&schema, instrs, 3, 2, vec![]);
    let (val, _) = eval_predicate(&prog, &mb, 0);
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
    let prog = make_prog(&schema, instrs, 3, 2, vec![]);
    let (val, _) = eval_predicate(&prog, &mb, 0);
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
    let prog = make_prog(&schema, instrs, 3, 2, vec![]);
    let (val, _) = eval_predicate(&prog, &mb, 0);
    assert_eq!(val, 0);
}

#[test]
fn test_fcmp_eq_ne_lt_le() {
    let schema = make_schema(0, &[8, 10, 10]);
    let a_bits = float_to_bits(3.14);
    let b_bits = float_to_bits(2.0);
    let batch = make_int_batch(&schema, &[(1, 1, 0, &[a_bits, b_bits])]);
    let mb = batch.as_mem_batch();

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
    let prog = make_prog(&schema, instrs, 3, 2, vec![]);
    let (val, _) = eval_predicate(&prog, &mb, 0);
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
    let prog = make_prog(&schema, instrs, 3, 2, vec![]);
    let (val, _) = eval_predicate(&prog, &mb, 0);
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
    let prog = make_prog(&schema, instrs, 3, 2, vec![]);
    let (val, _) = eval_predicate(&prog, &mb, 0);
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
    let prog = make_prog(&schema, instrs, 3, 2, vec![]);
    let (val, _) = eval_predicate(&prog, &mb, 0);
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
    let prog = make_prog(&schema, instrs, 3, 2, vec![]);
    let (val, _) = eval_predicate(&prog, &mb, 0);
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
    let prog = make_prog(&schema, instrs, 3, 2, vec![]);
    let (val, _) = eval_predicate(&prog, &mb, 0);
    assert_eq!(val, 0);
}

#[test]
fn test_string_lt_le_const() {
    let schema = make_schema(0, &[8, 11]);
    let mut batch = Batch::with_schema(schema, 1);
    batch.count = 0;
    batch.extend_pk(1u128);
    batch.extend_weight(&1i64.to_le_bytes());
    batch.extend_null_bmp(&0u64.to_le_bytes());
    let mut gs = [0u8; 16];
    gs[0..4].copy_from_slice(&5u32.to_le_bytes());
    gs[4..9].copy_from_slice(b"hello");
    batch.extend_col(0, &gs);
    batch.count = 1;
    let mb = batch.as_mem_batch();

    // STR_COL_LT_CONST: "hello" < "world" → 1
    let instrs = vec![LogicalInstr::StrColConst {
        op: StrOp::Lt,
        dst: 0,
        col: 1,
        const_idx: 0,
    }];
    let prog = make_prog(&schema, instrs, 1, 0, vec![b"world".to_vec()]);
    let (val, _) = eval_predicate(&prog, &mb, 0);
    assert_eq!(val, 1);

    // STR_COL_LT_CONST: "hello" < "hello" → 0
    let instrs = vec![LogicalInstr::StrColConst {
        op: StrOp::Lt,
        dst: 0,
        col: 1,
        const_idx: 0,
    }];
    let prog = make_prog(&schema, instrs, 1, 0, vec![b"hello".to_vec()]);
    let (val, _) = eval_predicate(&prog, &mb, 0);
    assert_eq!(val, 0);

    // STR_COL_LE_CONST: "hello" <= "hello" → 1
    let instrs = vec![LogicalInstr::StrColConst {
        op: StrOp::Le,
        dst: 0,
        col: 1,
        const_idx: 0,
    }];
    let prog = make_prog(&schema, instrs, 1, 0, vec![b"hello".to_vec()]);
    let (val, _) = eval_predicate(&prog, &mb, 0);
    assert_eq!(val, 1);

    // STR_COL_LE_CONST: "hello" <= "hella" → 0
    let instrs = vec![LogicalInstr::StrColConst {
        op: StrOp::Le,
        dst: 0,
        col: 1,
        const_idx: 0,
    }];
    let prog = make_prog(&schema, instrs, 1, 0, vec![b"hella".to_vec()]);
    let (val, _) = eval_predicate(&prog, &mb, 0);
    assert_eq!(val, 0);
}

#[test]
fn test_string_col_eq_col() {
    // Schema: pk(U64), str_a(STRING), str_b(STRING)
    let schema = make_schema(0, &[8, 11, 11]);
    let mut batch = Batch::with_schema(schema, 2);
    batch.count = 0;

    // Row 0: str_a="abc", str_b="abc" (equal)
    batch.extend_pk(1u128);
    batch.extend_weight(&1i64.to_le_bytes());
    batch.extend_null_bmp(&0u64.to_le_bytes());
    let mut gs_a = [0u8; 16];
    gs_a[0..4].copy_from_slice(&3u32.to_le_bytes());
    gs_a[4..7].copy_from_slice(b"abc");
    batch.extend_col(0, &gs_a);
    let mut gs_b = [0u8; 16];
    gs_b[0..4].copy_from_slice(&3u32.to_le_bytes());
    gs_b[4..7].copy_from_slice(b"abc");
    batch.extend_col(1, &gs_b);
    batch.count += 1;

    // Row 1: str_a="abc", str_b="xyz" (not equal)
    batch.extend_pk(2u128);
    batch.extend_weight(&1i64.to_le_bytes());
    batch.extend_null_bmp(&0u64.to_le_bytes());
    batch.extend_col(0, &gs_a);
    let mut gs_c = [0u8; 16];
    gs_c[0..4].copy_from_slice(&3u32.to_le_bytes());
    gs_c[4..7].copy_from_slice(b"xyz");
    batch.extend_col(1, &gs_c);
    batch.count += 1;

    let mb = batch.as_mem_batch();

    // Row 0: col1 == col2 → 1
    let instrs = vec![LogicalInstr::StrColCol {
        op: StrOp::Eq,
        dst: 0,
        col_a: 1,
        col_b: 2,
    }];
    let prog = make_prog(&schema, instrs, 1, 0, vec![]);
    let (val, _) = eval_predicate(&prog, &mb, 0);
    assert_eq!(val, 1);

    // Row 1: col1 == col2 → 0
    let (val, _) = eval_predicate(&prog, &mb, 1);
    assert_eq!(val, 0);
}

#[test]
fn test_complex_predicate() {
    // Schema: pk(U64), a(I64), b(I64), c(I64)
    let schema = make_schema(0, &[8, 9, 9, 9]);
    // Row: a=15, b=50, c=42
    let batch = make_int_batch(&schema, &[(1, 1, 0, &[15, 50, 42])]);
    let mb = batch.as_mem_batch();

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
    let prog = make_prog(&schema, instrs, 11, 10, vec![]);
    let (val, is_null) = eval_predicate(&prog, &mb, 0);
    assert_eq!(val, 1);
    assert!(!is_null);

    // Test with a=5 (a>10 false), b=50, c=99 (c==42 false) → false
    let batch2 = make_int_batch(&schema, &[(1, 1, 0, &[5, 50, 99])]);
    let mb2 = batch2.as_mem_batch();
    let (val, _) = eval_predicate(&prog, &mb2, 0);
    assert_eq!(val, 0);
}

#[test]
fn test_emit_null_opcode() {
    // Schema: pk(U64), val(I64)
    let schema = make_schema(0, &[8, 9]);
    let batch = make_int_batch(&schema, &[(1, 1, 0, &[10])]);
    let mb = batch.as_mem_batch();

    // EMIT_NULL is handled at batch level, skipped by eval_batch.
    // Verify the program still evaluates and the result register holds
    // the loaded value.
    let instrs = vec![
        LogicalInstr::LoadColInt { dst: 0, col: 1 }, // r0 = col1
        LogicalInstr::EmitNull { out: 0 },           // emit null — batch-level, no-op here
    ];
    let prog = make_prog(&schema, instrs, 1, 0, vec![]);

    let (val, _, mask, _) = eval_with_emit_via_batch(&prog, &mb, 0);
    // No EMIT instructions ⇒ empty emit list, no null mask bits set.
    assert_eq!(mask, 0);
    assert_eq!(val, 10);
}

#[test]
fn test_zero_regs_program() {
    // A program with num_regs=0 (pure COPY_COL) must not crash.
    let schema = make_schema(0, &[8, 9]);
    let batch = make_int_batch(&schema, &[(1, 1, 0, &[100])]);
    let mb = batch.as_mem_batch();

    // One COPY_COL instruction: copy col 1 → payload 0, type I64=9
    let instrs = vec![LogicalInstr::CopyCol {
        src_col: 1,
        out: 0,
        tc: 9,
    }];
    let prog = make_prog(&schema, instrs, 0, 0, vec![]);
    let (val, is_null) = eval_predicate(&prog, &mb, 0);
    // With num_regs=0, result should be (0, true) — sentinel
    assert_eq!(val, 0);
    assert!(is_null);
}

#[test]
fn test_resolve_column_indices_pk_at_col0() {
    // Schema: pk=col0(U64), col1=I64, col2=I64
    let schema = make_schema(0, &[8, 9, 9]);
    let batch = make_int_batch(&schema, &[(42, 1, 0, &[10, 20])]);
    let mb = batch.as_mem_batch();

    // LOAD_COL_INT of pk column → LoadPk
    let instrs = vec![LogicalInstr::LoadColInt { dst: 0, col: 0 }]; // col 0 = pk
    let prog = LogicalProgram::new(instrs, 1, 0, vec![]).resolve(&schema, false);
    assert!(matches!(prog.instrs[0], Instr::LoadPk { .. }));
    let (val, is_null) = eval_predicate(&prog, &mb, 0);
    assert_eq!(val, 42);
    assert!(!is_null);

    // LOAD_COL_INT of col1 (logical 1) → LoadPayloadInt, physical 0
    let instrs = vec![LogicalInstr::LoadColInt { dst: 0, col: 1 }];
    let prog = LogicalProgram::new(instrs, 1, 0, vec![]).resolve(&schema, false);
    assert!(matches!(prog.instrs[0], Instr::LoadPayloadInt { pi: 0, .. }));
    let (val, _) = eval_predicate(&prog, &mb, 0);
    assert_eq!(val, 10);

    // LOAD_COL_INT of col2 (logical 2) → LoadPayloadInt, physical 1
    let instrs = vec![LogicalInstr::LoadColInt { dst: 0, col: 2 }];
    let prog = LogicalProgram::new(instrs, 1, 0, vec![]).resolve(&schema, false);
    assert!(matches!(prog.instrs[0], Instr::LoadPayloadInt { pi: 1, .. }));
    let (val, _) = eval_predicate(&prog, &mb, 0);
    assert_eq!(val, 20);
}

#[test]
fn test_resolve_column_indices_pk_at_middle() {
    // Schema: col0=I64, pk=col1(U64), col2=I64
    // Physical payload layout: [col0=payload0, col2=payload1]
    let schema = make_schema(1, &[9, 8, 9]);
    let batch = make_int_batch(&schema, &[(99, 1, 0, &[5, 7])]);
    let mb = batch.as_mem_batch();

    // col0 (logical 0, before pk) → LoadPayloadInt, physical 0
    let instrs = vec![LogicalInstr::LoadColInt { dst: 0, col: 0 }];
    let prog = LogicalProgram::new(instrs, 1, 0, vec![]).resolve(&schema, false);
    assert!(matches!(prog.instrs[0], Instr::LoadPayloadInt { pi: 0, .. }));
    let (val, _) = eval_predicate(&prog, &mb, 0);
    assert_eq!(val, 5);

    // col1 (logical 1 = pk) → LoadPk
    let instrs = vec![LogicalInstr::LoadColInt { dst: 0, col: 1 }];
    let prog = LogicalProgram::new(instrs, 1, 0, vec![]).resolve(&schema, false);
    assert!(matches!(prog.instrs[0], Instr::LoadPk { .. }));
    let (val, _) = eval_predicate(&prog, &mb, 0);
    assert_eq!(val, 99);

    // col2 (logical 2, after pk) → LoadPayloadInt, physical 1
    let instrs = vec![LogicalInstr::LoadColInt { dst: 0, col: 2 }];
    let prog = LogicalProgram::new(instrs, 1, 0, vec![]).resolve(&schema, false);
    assert!(matches!(prog.instrs[0], Instr::LoadPayloadInt { pi: 1, .. }));
    let (val, _) = eval_predicate(&prog, &mb, 0);
    assert_eq!(val, 7);
}

#[test]
fn test_is_strictly_non_nullable_str_col() {
    // STR_COL_*_CONST and STR_COL_*_COL must flip no_nulls off when any
    // operand column is nullable; without that, the batch path skips null-bit
    // tracking and null rows leak through string predicates as definite results.
    let nullable_schema = {
        let cols = [
            SchemaColumn::new(crate::schema::type_code::U64, 0),
            SchemaColumn::new(crate::schema::type_code::STRING, 1),
            SchemaColumn::new(crate::schema::type_code::STRING, 1),
        ];
        SchemaDescriptor::new(&cols, &[0])
    };
    let nonnull_schema = {
        let cols = [
            SchemaColumn::new(crate::schema::type_code::U64, 0),
            SchemaColumn::new(crate::schema::type_code::STRING, 0),
            SchemaColumn::new(crate::schema::type_code::STRING, 0),
        ];
        SchemaDescriptor::new(&cols, &[0])
    };

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
        let prog = make_prog(&nullable_schema, instrs.clone(), 1, 0, vec![b"x".to_vec()]);
        assert!(
            !prog.is_strictly_non_nullable(&nullable_schema),
            "{_name}: nullable col1 must yield no_nulls=false"
        );
        let prog = make_prog(&nonnull_schema, instrs, 1, 0, vec![b"x".to_vec()]);
        assert!(
            prog.is_strictly_non_nullable(&nonnull_schema),
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
        let prog = make_prog(&nullable_schema, instrs.clone(), 1, 0, vec![]);
        assert!(
            !prog.is_strictly_non_nullable(&nullable_schema),
            "{_name}: nullable operands must yield no_nulls=false"
        );
        let prog = make_prog(&nonnull_schema, instrs, 1, 0, vec![]);
        assert!(
            prog.is_strictly_non_nullable(&nonnull_schema),
            "{_name}: non-nullable operands must yield no_nulls=true"
        );
    }
}

/// Build a 16-byte inline German String struct for use in test batches.
fn make_german_string(s: &[u8]) -> [u8; 16] {
    assert!(s.len() <= 12, "test helper only handles inline strings (≤ 12 bytes)");
    crate::test_support::german_string(s, &mut Vec::new())
}

/// Build a 1-row batch with the given schema, containing `s` as the first payload column.
fn make_single_string_batch(schema: SchemaDescriptor, s: &[u8]) -> Batch {
    let gs = make_german_string(s);
    let mut batch = Batch::with_schema(schema, 1);
    batch.count = 0;
    batch.extend_pk(1u128);
    batch.extend_weight(&1i64.to_le_bytes());
    batch.extend_null_bmp(&0u64.to_le_bytes());
    batch.extend_col(0, &gs);
    batch.count = 1;
    batch
}

#[test]
fn test_string_prefix_ordering() {
    // Schema: pk=col0(U64), col1=STRING
    let schema = make_schema(0, &[8, 11]);

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
        let batch = make_single_string_batch(schema, col_s);
        let mb = batch.as_mem_batch();
        let instrs = vec![LogicalInstr::StrColConst {
            op: StrOp::Lt,
            dst: 0,
            col: 1,
            const_idx: 0,
        }];
        let prog = make_prog(&schema, instrs, 1, 0, vec![const_s.to_vec()]);
        let (val, _) = eval_predicate(&prog, &mb, 0);
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
    let schema = make_schema(0, &[8, 9, 9]);
    // null_word bits: bit 0 = col1 null, bit 1 = col2 null
    let batch = make_int_batch(
        &schema,
        &[
            (1, 1, 0, &[1, 0]), // row0: T, F
            (2, 1, 0, &[0, 1]), // row1: F, T
            (3, 1, 2, &[1, 0]), // row2: T, NULL
            (4, 1, 2, &[0, 0]), // row3: F, NULL
            (5, 1, 1, &[0, 1]), // row4: NULL, T
            (6, 1, 1, &[0, 0]), // row5: NULL, F
            (7, 1, 3, &[0, 0]), // row6: NULL, NULL
        ],
    );
    let mb = batch.as_mem_batch();

    let and_instrs = vec![
        LogicalInstr::LoadColInt { dst: 0, col: 1 },
        LogicalInstr::LoadColInt { dst: 1, col: 2 },
        LogicalInstr::BoolAnd { dst: 2, a: 0, b: 1 },
    ];
    let and_prog = make_prog(&schema, and_instrs, 3, 2, vec![]);

    let or_instrs = vec![
        LogicalInstr::LoadColInt { dst: 0, col: 1 },
        LogicalInstr::LoadColInt { dst: 1, col: 2 },
        LogicalInstr::BoolOr { dst: 2, a: 0, b: 1 },
    ];
    let or_prog = make_prog(&schema, or_instrs, 3, 2, vec![]);

    // AND cases
    let (v, n) = eval_predicate(&and_prog, &mb, 0);
    assert_eq!(v, 0, "T AND F = F");
    assert!(!n);
    let (v, n) = eval_predicate(&and_prog, &mb, 1);
    assert_eq!(v, 0, "F AND T = F");
    assert!(!n);
    let (_, n) = eval_predicate(&and_prog, &mb, 2);
    assert!(n, "T AND NULL = NULL");
    let (v, n) = eval_predicate(&and_prog, &mb, 3);
    assert_eq!(v, 0, "F AND NULL = F");
    assert!(!n, "F AND NULL must not be null (SQL 3VL)");
    let (_, n) = eval_predicate(&and_prog, &mb, 4);
    assert!(n, "NULL AND T = NULL");
    let (v, n) = eval_predicate(&and_prog, &mb, 5);
    assert_eq!(v, 0, "NULL AND F = F");
    assert!(!n, "NULL AND F must not be null (SQL 3VL)");
    let (_, n) = eval_predicate(&and_prog, &mb, 6);
    assert!(n, "NULL AND NULL = NULL");

    // OR cases
    let (v, n) = eval_predicate(&or_prog, &mb, 0);
    assert_eq!(v, 1, "T OR F = T");
    assert!(!n);
    let (v, n) = eval_predicate(&or_prog, &mb, 1);
    assert_eq!(v, 1, "F OR T = T");
    assert!(!n);
    let (v, n) = eval_predicate(&or_prog, &mb, 2);
    assert_eq!(v, 1, "T OR NULL = T");
    assert!(!n, "T OR NULL must not be null (SQL 3VL)");
    let (_, n) = eval_predicate(&or_prog, &mb, 3);
    assert!(n, "F OR NULL = NULL");
    let (v, n) = eval_predicate(&or_prog, &mb, 4);
    assert_eq!(v, 1, "NULL OR T = T");
    assert!(!n, "NULL OR T must not be null (SQL 3VL)");
    let (_, n) = eval_predicate(&or_prog, &mb, 5);
    assert!(n, "NULL OR F = NULL");
    let (_, n) = eval_predicate(&or_prog, &mb, 6);
    assert!(n, "NULL OR NULL = NULL");
}

#[test]
fn test_string_prefix_le_ordering() {
    // Spot-check LE (≤) to cover the equality boundary
    let schema = make_schema(0, &[8, 11]);

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
        let batch = make_single_string_batch(schema, col_s);
        let mb = batch.as_mem_batch();
        let instrs = vec![LogicalInstr::StrColConst {
            op: StrOp::Le,
            dst: 0,
            col: 1,
            const_idx: 0,
        }];
        let prog = make_prog(&schema, instrs, 1, 0, vec![const_s.to_vec()]);
        let (val, _) = eval_predicate(&prog, &mb, 0);
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
    let schema = make_schema(0, &[8, 9, 9, 9]);
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
    let prog = make_prog(&schema, instrs, 4, 3, vec![]);

    // cond=1 (truthy) → a=100
    let batch = make_int_batch(&schema, &[(1, 1, 0, &[1, 100, 200])]);
    let (v, n) = eval_predicate(&prog, &batch.as_mem_batch(), 0);
    assert!(!n);
    assert_eq!(v, 100, "truthy cond takes a");

    // cond=0 (false) → b=200
    let batch = make_int_batch(&schema, &[(1, 1, 0, &[0, 100, 200])]);
    let (v, n) = eval_predicate(&prog, &batch.as_mem_batch(), 0);
    assert!(!n);
    assert_eq!(v, 200, "false cond takes b");

    // cond=NULL → b=200 (null_word bit 0 = cond/col1); value 7 is truthy but masked.
    let batch = make_int_batch(&schema, &[(1, 1, 1, &[7, 100, 200])]);
    let (v, n) = eval_predicate(&prog, &batch.as_mem_batch(), 0);
    assert!(!n);
    assert_eq!(v, 200, "NULL cond falls to else (b), not a");
}

/// SELECT carries the *chosen* branch's null bit; the unchosen branch's null is
/// irrelevant.
#[test]
fn test_select_null_bit_blend() {
    let schema = make_schema(0, &[8, 9, 9, 9]);
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
    let prog = make_prog(&schema, instrs, 4, 3, vec![]);

    // cond truthy, a NULL (payload bit 1) → result NULL.
    let batch = make_int_batch(&schema, &[(1, 1, 0b010, &[1, 0, 200])]);
    let (_, n) = eval_predicate(&prog, &batch.as_mem_batch(), 0);
    assert!(n, "truthy cond + NULL a → NULL");

    // cond truthy, b NULL (payload bit 2), a non-null → result = a; b's null ignored.
    let batch = make_int_batch(&schema, &[(1, 1, 0b100, &[1, 55, 0])]);
    let (v, n) = eval_predicate(&prog, &batch.as_mem_batch(), 0);
    assert!(!n, "truthy cond ignores b's null");
    assert_eq!(v, 55);

    // cond false, b NULL → result NULL.
    let batch = make_int_batch(&schema, &[(1, 1, 0b100, &[0, 55, 0])]);
    let (_, n) = eval_predicate(&prog, &batch.as_mem_batch(), 0);
    assert!(n, "false cond + NULL b → NULL");
}

/// `CASE WHEN cond THEN 42 END` (implicit ELSE NULL) lowers to
/// `select(cond, 42, load_null())`: truthy → 42, else → NULL.
#[test]
fn test_load_null_else_branch_eval() {
    let schema = make_schema(0, &[8, 9]);
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
    let prog = make_prog(&schema, instrs, 4, 3, vec![]);

    // cond truthy → 42.
    let batch = make_int_batch(&schema, &[(1, 1, 0, &[5])]);
    let (v, n) = eval_predicate(&prog, &batch.as_mem_batch(), 0);
    assert!(!n);
    assert_eq!(v, 42);

    // cond false → else NULL.
    let batch = make_int_batch(&schema, &[(1, 1, 0, &[0])]);
    let (_, n) = eval_predicate(&prog, &batch.as_mem_batch(), 0);
    assert!(n, "false cond → else NULL");

    // cond NULL → else NULL.
    let batch = make_int_batch(&schema, &[(1, 1, 1, &[9])]);
    let (_, n) = eval_predicate(&prog, &batch.as_mem_batch(), 0);
    assert!(n, "NULL cond → else NULL");
}

/// LoadNull forces the nullable eval path even when every column is NOT NULL:
/// `is_strictly_non_nullable` must return false whenever a program manufactures
/// a NULL.
#[test]
fn test_load_null_forces_nullable_path() {
    use crate::schema::type_code;
    let nonnull_schema = {
        let cols = [
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 0), // NOT NULL
        ];
        SchemaDescriptor::new(&cols, &[0])
    };
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
    let prog = make_prog(&nonnull_schema, instrs, 3, 2, vec![]);
    assert!(
        !prog.is_strictly_non_nullable(&nonnull_schema),
        "LoadNull must force the nullable path"
    );
}

/// A Select over only NOT NULL branches (no LoadNull) stays strictly-non-nullable:
/// Select copies branch values and adds no NULL of its own.
#[test]
fn test_select_non_nullable_when_branches_non_nullable() {
    use crate::schema::type_code;
    let nonnull_schema = {
        let cols = [
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 0),
            SchemaColumn::new(type_code::I64, 0),
            SchemaColumn::new(type_code::I64, 0),
        ];
        SchemaDescriptor::new(&cols, &[0])
    };
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
    let prog = make_prog(&nonnull_schema, instrs, 4, 3, vec![]);
    assert!(
        prog.is_strictly_non_nullable(&nonnull_schema),
        "Select over NOT NULL branches must stay strictly-non-nullable"
    );
}

/// U64-ness flows through Select (U64 if either branch is U64), so a downstream
/// ordered compare on the CASE result picks the unsigned variant.
#[test]
fn test_select_u64_propagation() {
    // Schema: pk(u64), u64col(U64), i64col(I64).
    let schema = make_schema(0, &[8, 8, 9]);
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
    let prog = make_prog(&schema, instrs, 6, 5, vec![]);
    assert!(
        matches!(
            prog.instrs[5],
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
    let schema = make_schema(0, &[8, 9, 9, 9, 9]);
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
    let prog = LogicalProgram::new(instrs, 6, 5, vec![]).resolve(&schema, /* is_filter = */ true);
    let (bit_only, bool_input) = prog.classify_registers(true);
    assert_ne!(bool_input & (1 << 0), 0, "cond is read as a bool_input");
    assert_ne!(bool_input & (1 << 3), 0, "select result feeds BOOL_AND as bool_input");
    assert_eq!(bit_only & (1 << 3), 0, "select dst is a value register, never bit_only");
}

/// `dst` aliasing any of `cond`/`a`/`b` violates the SELECT anti-alias rule that
/// makes `reg4`'s raw split borrows sound.
#[test]
#[should_panic(expected = "SELECT register aliasing")]
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
    let schema = make_schema(0, &[8, 9, 9, 9, 9, 9]);
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
    let prog = make_prog(&schema, instrs, 7, 6, vec![]);
    // payload cols: c1, c2, v1=10, v2=20, v3=30.
    // c1 truthy → v1.
    let batch = make_int_batch(&schema, &[(1, 1, 0, &[1, 1, 10, 20, 30])]);
    let (v, _) = eval_predicate(&prog, &batch.as_mem_batch(), 0);
    assert_eq!(v, 10, "c1 truthy → v1");
    // c1 false, c2 truthy → v2.
    let batch = make_int_batch(&schema, &[(1, 1, 0, &[0, 1, 10, 20, 30])]);
    let (v, _) = eval_predicate(&prog, &batch.as_mem_batch(), 0);
    assert_eq!(v, 20, "c1 false, c2 truthy → v2");
    // both false → v3.
    let batch = make_int_batch(&schema, &[(1, 1, 0, &[0, 0, 10, 20, 30])]);
    let (v, _) = eval_predicate(&prog, &batch.as_mem_batch(), 0);
    assert_eq!(v, 30, "both false → else v3");
}

// ---------------------------------------------------------------------------
// AND-chain short-circuit detection (`compute_and_chain`)
// ---------------------------------------------------------------------------

/// Resolve `instrs` as a FILTER program (is_filter = true) so AND-chain
/// detection runs; `make_prog` resolves as a map (is_filter = false).
fn resolve_filter(
    schema: &SchemaDescriptor,
    instrs: Vec<LogicalInstr>,
    num_regs: u32,
    result_reg: u32,
) -> ResolvedProgram {
    LogicalProgram::new(instrs, num_regs, result_reg, vec![]).resolve(schema, /* is_filter = */ true)
}

/// `c1>1 AND c2>1 AND c3>1 AND c4>1`: a clean left-deep 4-clause chain (3 ANDs).
/// The two non-terminal ANDs (dst reg 5, reg 8) are triggers; the terminal AND
/// (dst reg 11 = result_reg) is not marked.
#[test]
fn and_chain_marks_nonterminal_ands() {
    let schema = make_schema(0, &[8, 9, 9, 9, 9]); // PK + 4 nullable I64
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
    let prog = resolve_filter(&schema, instrs, 12, 11);
    assert_eq!(prog.chain_trigger_mask, (1u64 << 5) | (1u64 << 8));
    assert_eq!(prog.chain_trigger_mask.count_ones(), 2);
}

/// A single-AND filter (`c1>1 AND c2>1`) has no non-terminal AND, so the mask is
/// empty: the spine walk finds no upstream single-use AND.
#[test]
fn and_chain_single_and_empty_mask() {
    let schema = make_schema(0, &[8, 9, 9]);
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
    let prog = resolve_filter(&schema, instrs, 6, 5);
    assert_eq!(prog.chain_trigger_mask, 0);
}

/// `NOT (c1>1 AND c2>1)`: terminal is a `BoolNot`, not an AND on result_reg, so
/// nothing is detected — forcing the inner AND FALSE would be a miscompile
/// (`NOT false = true`).
#[test]
fn and_chain_not_terminal_no_mask() {
    let schema = make_schema(0, &[8, 9, 9]);
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
    let prog = resolve_filter(&schema, instrs, 7, 6);
    assert_eq!(prog.chain_trigger_mask, 0);
}

/// `(c1>1 AND c2>1) OR c3>1`: terminal is a `BoolOr`, so the inner AND is reached
/// through an OR operand, not the accumulator spine — not a trigger.
#[test]
fn and_chain_or_terminal_no_mask() {
    let schema = make_schema(0, &[8, 9, 9, 9]);
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
    let prog = resolve_filter(&schema, instrs, 9, 8);
    assert_eq!(prog.chain_trigger_mask, 0);
}

/// The same clean chain resolved as a MAP (`is_filter = false`) gets a 0 mask:
/// detection is gated on `is_filter`, matching where `bit_only_mask` is gated.
#[test]
fn and_chain_map_program_no_mask() {
    let schema = make_schema(0, &[8, 9, 9, 9]);
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
    let prog = LogicalProgram::new(instrs, 9, 8, vec![]).resolve(&schema, /* is_filter = */ false);
    assert_eq!(prog.chain_trigger_mask, 0);
}
