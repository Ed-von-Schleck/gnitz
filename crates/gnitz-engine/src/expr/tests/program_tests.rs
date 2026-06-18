// 3.14 is a deliberate float-bit-pattern test fixture, not an approximation
// of PI meant to be replaced with std::f64::consts::PI.
#![allow(clippy::approx_constant)]

use super::super::program::*;
use super::{bits_to_float, eval_predicate_via_batch as eval_predicate, eval_with_emit_via_batch, float_to_bits};
use crate::schema::{SchemaColumn, SchemaDescriptor, MAX_COLUMNS};
use crate::storage::Batch;

/// Build and resolve an `ExprProgram` in one step. Eval entry points
/// `debug_assert!(prog.resolved)`, so every test prog must be resolved
/// before being passed to `eval_predicate`/`eval_with_emit`.
fn make_prog(
    schema: &SchemaDescriptor,
    code: Vec<i64>,
    num_regs: u32,
    result_reg: u32,
    const_strings: Vec<Vec<u8>>,
) -> ExprProgram {
    let mut prog = ExprProgram::new(code, num_regs, result_reg, const_strings);
    prog.resolve_column_indices(schema);
    prog
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
    let code = vec![
        EXPR_LOAD_COL_INT,
        0,
        1,
        0, // r0 = col[1]
        EXPR_LOAD_CONST,
        1,
        42,
        0, // r1 = 42
        EXPR_CMP_EQ,
        2,
        0,
        1, // r2 = (r0 == r1)
    ];
    let prog = make_prog(&schema, code, 3, 2, vec![]);
    let (val, is_null) = eval_predicate(&prog, &mb, 0);
    assert_eq!(val, 1);
    assert!(!is_null);

    // Test NE
    let code = vec![
        EXPR_LOAD_COL_INT,
        0,
        1,
        0,
        EXPR_LOAD_CONST,
        1,
        42,
        0,
        EXPR_CMP_NE,
        2,
        0,
        1,
    ];
    let prog = make_prog(&schema, code, 3, 2, vec![]);
    let (val, _) = eval_predicate(&prog, &mb, 0);
    assert_eq!(val, 0);

    // Test GT
    let code = vec![
        EXPR_LOAD_COL_INT,
        0,
        1,
        0,
        EXPR_LOAD_CONST,
        1,
        10,
        0,
        EXPR_CMP_GT,
        2,
        0,
        1,
    ];
    let prog = make_prog(&schema, code, 3, 2, vec![]);
    let (val, _) = eval_predicate(&prog, &mb, 0);
    assert_eq!(val, 1);
}

#[test]
fn test_int_arithmetic() {
    let schema = make_schema(0, &[8, 9, 9]);
    let batch = make_int_batch(&schema, &[(1, 1, 0, &[10, 3])]);
    let mb = batch.as_mem_batch();

    // ADD: 10 + 3 = 13
    let code = vec![
        EXPR_LOAD_COL_INT,
        0,
        1,
        0,
        EXPR_LOAD_COL_INT,
        1,
        2,
        0,
        EXPR_INT_ADD,
        2,
        0,
        1,
    ];
    let prog = make_prog(&schema, code, 3, 2, vec![]);
    let (val, _) = eval_predicate(&prog, &mb, 0);
    assert_eq!(val, 13);

    // DIV by zero → NULL
    let code = vec![
        EXPR_LOAD_COL_INT,
        0,
        1,
        0,
        EXPR_LOAD_CONST,
        1,
        0,
        0,
        EXPR_INT_DIV,
        2,
        0,
        1,
    ];
    let prog = make_prog(&schema, code, 3, 2, vec![]);
    let (_, is_null) = eval_predicate(&prog, &mb, 0);
    assert!(is_null);

    // MOD by zero → NULL
    let code = vec![
        EXPR_LOAD_COL_INT,
        0,
        1,
        0,
        EXPR_LOAD_CONST,
        1,
        0,
        0,
        EXPR_INT_MOD,
        2,
        0,
        1,
    ];
    let prog = make_prog(&schema, code, 3, 2, vec![]);
    let (_, is_null) = eval_predicate(&prog, &mb, 0);
    assert!(is_null);

    // NEG: -10
    let code = vec![EXPR_LOAD_COL_INT, 0, 1, 0, EXPR_INT_NEG, 1, 0, 0];
    let prog = make_prog(&schema, code, 2, 1, vec![]);
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
    let code = vec![
        EXPR_LOAD_COL_FLOAT,
        0,
        1,
        0,
        EXPR_LOAD_COL_FLOAT,
        1,
        2,
        0,
        EXPR_FLOAT_ADD,
        2,
        0,
        1,
    ];
    let prog = make_prog(&schema, code, 3, 2, vec![]);
    let (val, _) = eval_predicate(&prog, &mb, 0);
    let result = bits_to_float(val);
    assert!((result - 5.14).abs() < 1e-10);

    // FLOAT_DIV by zero → NULL
    let code = vec![
        EXPR_LOAD_COL_FLOAT,
        0,
        1,
        0,
        EXPR_LOAD_CONST,
        1,
        0,
        0, // zero bits
        EXPR_FLOAT_DIV,
        2,
        0,
        1,
    ];
    let prog = make_prog(&schema, code, 3, 2, vec![]);
    let (_, is_null) = eval_predicate(&prog, &mb, 0);
    assert!(is_null);

    // FCMP_GT: 3.14 > 2.0
    let code = vec![
        EXPR_LOAD_COL_FLOAT,
        0,
        1,
        0,
        EXPR_LOAD_COL_FLOAT,
        1,
        2,
        0,
        EXPR_FCMP_GT,
        2,
        0,
        1,
    ];
    let prog = make_prog(&schema, code, 3, 2, vec![]);
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
    let code = vec![
        EXPR_LOAD_COL_INT,
        0,
        1,
        0,
        EXPR_LOAD_COL_INT,
        1,
        2,
        0,
        EXPR_INT_ADD,
        2,
        0,
        1,
    ];
    let prog = make_prog(&schema, code, 3, 2, vec![]);
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
    let code = vec![EXPR_IS_NULL, 0, 1, 0];
    let prog = make_prog(&schema, code, 1, 0, vec![]);
    let (val, is_null) = eval_predicate(&prog, &mb, 0);
    assert_eq!(val, 1);
    assert!(!is_null);

    // IS_NOT_NULL(col1) → 0
    let code = vec![EXPR_IS_NOT_NULL, 0, 1, 0];
    let prog = make_prog(&schema, code, 1, 0, vec![]);
    let (val, is_null) = eval_predicate(&prog, &mb, 0);
    assert_eq!(val, 0);
    assert!(!is_null);

    // IS_NULL(col2) → 0
    let code = vec![EXPR_IS_NULL, 0, 2, 0];
    let prog = make_prog(&schema, code, 1, 0, vec![]);
    let (val, _) = eval_predicate(&prog, &mb, 0);
    assert_eq!(val, 0);
}

#[test]
fn test_boolean_combinators() {
    let schema = make_schema(0, &[8, 9]);
    let batch = make_int_batch(&schema, &[(1, 1, 0, &[1])]);
    let mb = batch.as_mem_batch();

    // AND(1, 0) → 0
    let code = vec![
        EXPR_LOAD_COL_INT,
        0,
        1,
        0,
        EXPR_LOAD_CONST,
        1,
        0,
        0,
        EXPR_BOOL_AND,
        2,
        0,
        1,
    ];
    let prog = make_prog(&schema, code, 3, 2, vec![]);
    let (val, _) = eval_predicate(&prog, &mb, 0);
    assert_eq!(val, 0);

    // OR(1, 0) → 1
    let code = vec![
        EXPR_LOAD_COL_INT,
        0,
        1,
        0,
        EXPR_LOAD_CONST,
        1,
        0,
        0,
        EXPR_BOOL_OR,
        2,
        0,
        1,
    ];
    let prog = make_prog(&schema, code, 3, 2, vec![]);
    let (val, _) = eval_predicate(&prog, &mb, 0);
    assert_eq!(val, 1);

    // NOT(1) → 0
    let code = vec![EXPR_LOAD_COL_INT, 0, 1, 0, EXPR_BOOL_NOT, 1, 0, 0];
    let prog = make_prog(&schema, code, 2, 1, vec![]);
    let (val, _) = eval_predicate(&prog, &mb, 0);
    assert_eq!(val, 0);
}

#[test]
fn test_load_const_encoding() {
    let schema = make_schema(0, &[8, 9]);
    let batch = make_int_batch(&schema, &[(1, 1, 0, &[0])]);
    let mb = batch.as_mem_batch();

    // Test large constant: 0x00000001_00000002 = (1 << 32) | 2 = 4294967298
    // a1 = lo 32 bits = 2, a2 = hi 32 bits = 1
    let code = vec![EXPR_LOAD_CONST, 0, 2, 1];
    let prog = make_prog(&schema, code, 1, 0, vec![]);
    let (val, is_null) = eval_predicate(&prog, &mb, 0);
    assert!(!is_null);
    assert_eq!(val, (1i64 << 32) | 2);

    // Test negative constant: -1 → lo=0xFFFFFFFF, hi=0xFFFFFFFF
    // As i64: a1 = -1 (sign-extended from 32 bits), a2 = -1
    // Reconstruction: (-1 << 32) | (-1 & 0xFFFFFFFF) = 0xFFFFFFFF_FFFFFFFF = -1
    let code = vec![EXPR_LOAD_CONST, 0, -1, -1];
    let prog = make_prog(&schema, code, 1, 0, vec![]);
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
    let code = vec![EXPR_STR_COL_EQ_CONST, 0, 1, 0];
    let prog = make_prog(&schema, code, 1, 0, vec![b"hello".to_vec()]);
    let (val, is_null) = eval_predicate(&prog, &mb, 0);
    assert!(!is_null);
    assert_eq!(val, 1);

    // STR_COL_EQ_CONST(col1, "world") → 0
    let code = vec![EXPR_STR_COL_EQ_CONST, 0, 1, 0];
    let prog = make_prog(&schema, code, 1, 0, vec![b"world".to_vec()]);
    let (val, _) = eval_predicate(&prog, &mb, 0);
    assert_eq!(val, 0);
}

#[test]
fn test_int_to_float() {
    let schema = make_schema(0, &[8, 9]);
    let batch = make_int_batch(&schema, &[(1, 1, 0, &[42])]);
    let mb = batch.as_mem_batch();

    let code = vec![EXPR_LOAD_COL_INT, 0, 1, 0, EXPR_INT_TO_FLOAT, 1, 0, 0];
    let prog = make_prog(&schema, code, 2, 1, vec![]);
    let (val, _) = eval_predicate(&prog, &mb, 0);
    assert_eq!(bits_to_float(val), 42.0);
}

#[test]
fn test_unsigned_opcode_swap_and_eval() {
    // U64-typed schema: pk=col0(U64), payload=col1(U64). resolve_column_indices
    // tracks col1's register as U64 and must swap the signed comparison /
    // division / cast opcodes to their unsigned forms (EXPR_UCMP_GT / EXPR_UDIV
    // / EXPR_UMOD / EXPR_UINT_TO_FLOAT). eval_batch ends in `_ => {}`, so a
    // mis-routed or removed U-arm is silent for values >= 2^63 — this pin makes
    // it loud by both (1) asserting the swap fired in prog.code and (2) running
    // eval on v=u64::MAX and asserting the UNSIGNED result, which diverges from
    // the signed interpretation that all other tests (BIGINT < 2^63) exercise.
    let schema = make_schema(0, &[8, 8]);

    // v = u64::MAX (bit pattern 0xFFFF...FF, i.e. -1 as i64); divisor const = 2.
    // Unsigned: MAX > 100, MAX/2 = 9223372036854775807, MAX%2 = 1.
    // Signed:   -1 < 100 (false), -1/2 = 0,             -1%2 = -1.
    let v_bits = u64::MAX as i64; // == -1i64
    let batch = make_int_batch(&schema, &[(1, 1, 0, &[v_bits])]);
    let mb = batch.as_mem_batch();

    // (a) col1 > 100  → EXPR_CMP_GT must swap to EXPR_UCMP_GT.
    let code = vec![
        EXPR_LOAD_COL_INT,
        0,
        1,
        0, // r0 = col1 (U64)
        EXPR_LOAD_CONST,
        1,
        100,
        0, // r1 = 100
        EXPR_CMP_GT,
        2,
        0,
        1, // r2 = (r0 > r1)
    ];
    let prog_gt = make_prog(&schema, code, 3, 2, vec![]);
    // Instruction 2 (offset 8) is the comparison opcode after resolution.
    assert_eq!(
        prog_gt.code[8], EXPR_UCMP_GT,
        "U64 operand must swap CMP_GT → UCMP_GT (got opcode {})",
        prog_gt.code[8],
    );
    let (val, is_null) = eval_predicate(&prog_gt, &mb, 0);
    assert!(!is_null);
    assert_eq!(
        val, 1,
        "u64::MAX > 100 is TRUE under unsigned compare; signed (-1 > 100) is false",
    );

    // (b) col1 / 2  → EXPR_INT_DIV must swap to EXPR_UDIV.
    let code = vec![
        EXPR_LOAD_COL_INT,
        0,
        1,
        0,
        EXPR_LOAD_CONST,
        1,
        2,
        0,
        EXPR_INT_DIV,
        2,
        0,
        1,
    ];
    let prog_div = make_prog(&schema, code, 3, 2, vec![]);
    assert_eq!(
        prog_div.code[8], EXPR_UDIV,
        "U64 operand must swap INT_DIV → UDIV (got opcode {})",
        prog_div.code[8],
    );
    let (val, is_null) = eval_predicate(&prog_div, &mb, 0);
    assert!(!is_null);
    assert_eq!(
        val, 9223372036854775807,
        "u64::MAX / 2 == 9223372036854775807 (unsigned); signed -1/2 would be 0",
    );

    // (c) col1 % 2  → EXPR_INT_MOD must swap to EXPR_UMOD.
    let code = vec![
        EXPR_LOAD_COL_INT,
        0,
        1,
        0,
        EXPR_LOAD_CONST,
        1,
        2,
        0,
        EXPR_INT_MOD,
        2,
        0,
        1,
    ];
    let prog_mod = make_prog(&schema, code, 3, 2, vec![]);
    assert_eq!(
        prog_mod.code[8], EXPR_UMOD,
        "U64 operand must swap INT_MOD → UMOD (got opcode {})",
        prog_mod.code[8],
    );
    let (val, is_null) = eval_predicate(&prog_mod, &mb, 0);
    assert!(!is_null);
    assert_eq!(val, 1, "u64::MAX % 2 == 1 (unsigned); signed -1 % 2 would be -1",);

    // (d) CAST col1 to float  → EXPR_INT_TO_FLOAT must swap to EXPR_UINT_TO_FLOAT.
    let code = vec![
        EXPR_LOAD_COL_INT,
        0,
        1,
        0, // r0 = col1 (U64)
        EXPR_INT_TO_FLOAT,
        1,
        0,
        0, // r1 = (f64) r0
    ];
    let prog_cast = make_prog(&schema, code, 2, 1, vec![]);
    // Instruction 1 (offset 4) is the cast opcode after resolution.
    assert_eq!(
        prog_cast.code[4], EXPR_UINT_TO_FLOAT,
        "U64 operand must swap INT_TO_FLOAT → UINT_TO_FLOAT (got opcode {})",
        prog_cast.code[4],
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
    let code = vec![
        EXPR_LOAD_COL_INT,
        0,
        1,
        0,
        EXPR_LOAD_COL_INT,
        1,
        2,
        0,
        EXPR_INT_ADD,
        2,
        0,
        1,
        EXPR_EMIT,
        0,
        2,
        0, // emit r2 to payload col 0
    ];
    let prog = make_prog(&schema, code, 3, 2, vec![]);

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
    let code = vec![
        EXPR_LOAD_COL_INT,
        0,
        1,
        0,
        EXPR_LOAD_CONST,
        1,
        0,
        0,
        EXPR_INT_DIV,
        2,
        0,
        1,
    ];
    let prog = make_prog(&schema, code, 3, 2, vec![]);
    let (_, is_null) = eval_predicate(&prog, &mb, 0);
    assert!(is_null);

    // 2. INT_MOD by literal 0 → NULL
    let code = vec![
        EXPR_LOAD_COL_INT,
        0,
        1,
        0,
        EXPR_LOAD_CONST,
        1,
        0,
        0,
        EXPR_INT_MOD,
        2,
        0,
        1,
    ];
    let prog = make_prog(&schema, code, 3, 2, vec![]);
    let (_, is_null) = eval_predicate(&prog, &mb, 0);
    assert!(is_null);

    // 3. FLOAT_DIV by 0.0 bits → NULL
    let code = vec![
        EXPR_LOAD_COL_INT,
        0,
        1,
        0,
        EXPR_LOAD_CONST,
        1,
        0,
        0,
        EXPR_FLOAT_DIV,
        2,
        0,
        1,
    ];
    let prog = make_prog(&schema, code, 3, 2, vec![]);
    let (_, is_null) = eval_predicate(&prog, &mb, 0);
    assert!(is_null);

    // 4. INT_DIV by non-null non-zero → correct quotient, not null
    let code = vec![
        EXPR_LOAD_COL_INT,
        0,
        1,
        0,
        EXPR_LOAD_COL_INT,
        1,
        2,
        0,
        EXPR_INT_DIV,
        2,
        0,
        1,
    ];
    let prog = make_prog(&schema, code, 3, 2, vec![]);
    let (val, is_null) = eval_predicate(&prog, &mb, 0);
    assert!(!is_null);
    assert_eq!(val, 3); // 10 / 3 = 3

    // 5. INT_DIV where divisor column is null → NULL (null propagation)
    // null_word bit 0 = col1 null; use col2 (bit 1) as divisor with col1 null
    // Schema col indices: col1=payload_idx 0, col2=payload_idx 1
    // Build a row where col2 is null (null_word bit 1 set)
    let batch_null_div = make_int_batch(&schema, &[(1, 1, 2, &[10, 3])]);
    let mb_null_div = batch_null_div.as_mem_batch();
    let code = vec![
        EXPR_LOAD_COL_INT,
        0,
        1,
        0,
        EXPR_LOAD_COL_INT,
        1,
        2,
        0,
        EXPR_INT_DIV,
        2,
        0,
        1,
    ];
    let prog = make_prog(&schema, code, 3, 2, vec![]);
    let (_, is_null) = eval_predicate(&prog, &mb_null_div, 0);
    assert!(is_null);

    // 6. EMIT of INT_DIV-by-zero result → emit_null_mask bit set, buffer contains 0
    let code = vec![
        EXPR_LOAD_COL_INT,
        0,
        1,
        0,
        EXPR_LOAD_CONST,
        1,
        0,
        0,
        EXPR_INT_DIV,
        2,
        0,
        1,
        EXPR_EMIT,
        0,
        2,
        0,
    ];
    let prog = make_prog(&schema, code, 3, 2, vec![]);
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
    let code = vec![
        EXPR_LOAD_COL_INT,
        0,
        1,
        0,
        EXPR_LOAD_CONST,
        1,
        42,
        0,
        EXPR_CMP_GE,
        2,
        0,
        1,
    ];
    let prog = make_prog(&schema, code, 3, 2, vec![]);
    let (val, _) = eval_predicate(&prog, &mb, 0);
    assert_eq!(val, 1);

    // GE: 42 >= 43 → 0
    let code = vec![
        EXPR_LOAD_COL_INT,
        0,
        1,
        0,
        EXPR_LOAD_CONST,
        1,
        43,
        0,
        EXPR_CMP_GE,
        2,
        0,
        1,
    ];
    let prog = make_prog(&schema, code, 3, 2, vec![]);
    let (val, _) = eval_predicate(&prog, &mb, 0);
    assert_eq!(val, 0);

    // LT: 42 < 43 → 1
    let code = vec![
        EXPR_LOAD_COL_INT,
        0,
        1,
        0,
        EXPR_LOAD_CONST,
        1,
        43,
        0,
        EXPR_CMP_LT,
        2,
        0,
        1,
    ];
    let prog = make_prog(&schema, code, 3, 2, vec![]);
    let (val, _) = eval_predicate(&prog, &mb, 0);
    assert_eq!(val, 1);

    // LT: 42 < 42 → 0
    let code = vec![
        EXPR_LOAD_COL_INT,
        0,
        1,
        0,
        EXPR_LOAD_CONST,
        1,
        42,
        0,
        EXPR_CMP_LT,
        2,
        0,
        1,
    ];
    let prog = make_prog(&schema, code, 3, 2, vec![]);
    let (val, _) = eval_predicate(&prog, &mb, 0);
    assert_eq!(val, 0);

    // LE: 42 <= 42 → 1
    let code = vec![
        EXPR_LOAD_COL_INT,
        0,
        1,
        0,
        EXPR_LOAD_CONST,
        1,
        42,
        0,
        EXPR_CMP_LE,
        2,
        0,
        1,
    ];
    let prog = make_prog(&schema, code, 3, 2, vec![]);
    let (val, _) = eval_predicate(&prog, &mb, 0);
    assert_eq!(val, 1);

    // LE: 42 <= 41 → 0
    let code = vec![
        EXPR_LOAD_COL_INT,
        0,
        1,
        0,
        EXPR_LOAD_CONST,
        1,
        41,
        0,
        EXPR_CMP_LE,
        2,
        0,
        1,
    ];
    let prog = make_prog(&schema, code, 3, 2, vec![]);
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
    let code = vec![
        EXPR_LOAD_COL_FLOAT,
        0,
        1,
        0,
        EXPR_LOAD_COL_FLOAT,
        1,
        1,
        0,
        EXPR_FCMP_EQ,
        2,
        0,
        1,
    ];
    let prog = make_prog(&schema, code, 3, 2, vec![]);
    let (val, _) = eval_predicate(&prog, &mb, 0);
    assert_eq!(val, 1);

    // FCMP_EQ: 3.14 == 2.0 → 0
    let code = vec![
        EXPR_LOAD_COL_FLOAT,
        0,
        1,
        0,
        EXPR_LOAD_COL_FLOAT,
        1,
        2,
        0,
        EXPR_FCMP_EQ,
        2,
        0,
        1,
    ];
    let prog = make_prog(&schema, code, 3, 2, vec![]);
    let (val, _) = eval_predicate(&prog, &mb, 0);
    assert_eq!(val, 0);

    // FCMP_NE: 3.14 != 2.0 → 1
    let code = vec![
        EXPR_LOAD_COL_FLOAT,
        0,
        1,
        0,
        EXPR_LOAD_COL_FLOAT,
        1,
        2,
        0,
        EXPR_FCMP_NE,
        2,
        0,
        1,
    ];
    let prog = make_prog(&schema, code, 3, 2, vec![]);
    let (val, _) = eval_predicate(&prog, &mb, 0);
    assert_eq!(val, 1);

    // FCMP_LT: 2.0 < 3.14 → 1
    let code = vec![
        EXPR_LOAD_COL_FLOAT,
        0,
        2,
        0,
        EXPR_LOAD_COL_FLOAT,
        1,
        1,
        0,
        EXPR_FCMP_LT,
        2,
        0,
        1,
    ];
    let prog = make_prog(&schema, code, 3, 2, vec![]);
    let (val, _) = eval_predicate(&prog, &mb, 0);
    assert_eq!(val, 1);

    // FCMP_LE: 2.0 <= 2.0 → 1
    let code = vec![
        EXPR_LOAD_COL_FLOAT,
        0,
        2,
        0,
        EXPR_LOAD_COL_FLOAT,
        1,
        2,
        0,
        EXPR_FCMP_LE,
        2,
        0,
        1,
    ];
    let prog = make_prog(&schema, code, 3, 2, vec![]);
    let (val, _) = eval_predicate(&prog, &mb, 0);
    assert_eq!(val, 1);

    // FCMP_GE: 2.0 >= 3.14 → 0
    let code = vec![
        EXPR_LOAD_COL_FLOAT,
        0,
        2,
        0,
        EXPR_LOAD_COL_FLOAT,
        1,
        1,
        0,
        EXPR_FCMP_GE,
        2,
        0,
        1,
    ];
    let prog = make_prog(&schema, code, 3, 2, vec![]);
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
    let code = vec![EXPR_STR_COL_LT_CONST, 0, 1, 0];
    let prog = make_prog(&schema, code, 1, 0, vec![b"world".to_vec()]);
    let (val, _) = eval_predicate(&prog, &mb, 0);
    assert_eq!(val, 1);

    // STR_COL_LT_CONST: "hello" < "hello" → 0
    let code = vec![EXPR_STR_COL_LT_CONST, 0, 1, 0];
    let prog = make_prog(&schema, code, 1, 0, vec![b"hello".to_vec()]);
    let (val, _) = eval_predicate(&prog, &mb, 0);
    assert_eq!(val, 0);

    // STR_COL_LE_CONST: "hello" <= "hello" → 1
    let code = vec![EXPR_STR_COL_LE_CONST, 0, 1, 0];
    let prog = make_prog(&schema, code, 1, 0, vec![b"hello".to_vec()]);
    let (val, _) = eval_predicate(&prog, &mb, 0);
    assert_eq!(val, 1);

    // STR_COL_LE_CONST: "hello" <= "hella" → 0
    let code = vec![EXPR_STR_COL_LE_CONST, 0, 1, 0];
    let prog = make_prog(&schema, code, 1, 0, vec![b"hella".to_vec()]);
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
    let code = vec![EXPR_STR_COL_EQ_COL, 0, 1, 2];
    let prog = make_prog(&schema, code, 1, 0, vec![]);
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
    let code = vec![
        EXPR_LOAD_COL_INT,
        0,
        1,
        0, // r0 = a = 15
        EXPR_LOAD_CONST,
        1,
        10,
        0, // r1 = 10
        EXPR_CMP_GT,
        2,
        0,
        1, // r2 = (15 > 10) = 1
        EXPR_LOAD_COL_INT,
        3,
        2,
        0, // r3 = b = 50
        EXPR_LOAD_CONST,
        4,
        100,
        0, // r4 = 100
        EXPR_CMP_LT,
        5,
        3,
        4, // r5 = (50 < 100) = 1
        EXPR_BOOL_AND,
        6,
        2,
        5, // r6 = (1 AND 1) = 1
        EXPR_LOAD_COL_INT,
        7,
        3,
        0, // r7 = c = 42
        EXPR_LOAD_CONST,
        8,
        42,
        0, // r8 = 42
        EXPR_CMP_EQ,
        9,
        7,
        8, // r9 = (42 == 42) = 1
        EXPR_BOOL_OR,
        10,
        6,
        9, // r10 = (1 OR 1) = 1
    ];
    let prog = make_prog(&schema, code, 11, 10, vec![]);
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
    let code = vec![
        EXPR_LOAD_COL_INT,
        0,
        1,
        0, // r0 = col1
        EXPR_EMIT_NULL,
        0,
        0,
        0, // emit null — batch-level, no-op here
    ];
    let prog = make_prog(&schema, code, 1, 0, vec![]);

    let (val, _, mask, _) = eval_with_emit_via_batch(&prog, &mb, 0);
    // No EMIT instructions ⇒ empty emit list, no null mask bits set.
    assert_eq!(mask, 0);
    assert_eq!(val, 10);
}

#[test]
fn test_zero_regs_program() {
    // ExprProgram with num_regs=0 (pure COPY_COL) must not crash.
    let schema = make_schema(0, &[8, 9]);
    let batch = make_int_batch(&schema, &[(1, 1, 0, &[100])]);
    let mb = batch.as_mem_batch();

    // One COPY_COL instruction: copy col 1 → payload 0, type I64=9
    let code = vec![EXPR_COPY_COL, 9, 1, 0];
    let prog = make_prog(&schema, code, 0, 0, vec![]);
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

    // LOAD_COL_INT of pk column → LOAD_PK_INT
    let code = vec![EXPR_LOAD_COL_INT, 0, 0, 0]; // col 0 = pk
    let mut prog = ExprProgram::new(code, 1, 0, vec![]);
    prog.resolve_column_indices(&schema);
    assert_eq!(prog.code[0], EXPR_LOAD_PK_UNSIGNED_INT);
    let (val, is_null) = eval_predicate(&prog, &mb, 0);
    assert_eq!(val, 42);
    assert!(!is_null);

    // LOAD_COL_INT of col1 (logical 1) → LOAD_PAYLOAD_INT, physical 0
    let code = vec![EXPR_LOAD_COL_INT, 0, 1, 0];
    let mut prog = ExprProgram::new(code, 1, 0, vec![]);
    prog.resolve_column_indices(&schema);
    assert_eq!(prog.code[0], EXPR_LOAD_PAYLOAD_INT);
    assert_eq!(prog.code[2], 0); // physical payload index
    let (val, _) = eval_predicate(&prog, &mb, 0);
    assert_eq!(val, 10);

    // LOAD_COL_INT of col2 (logical 2) → LOAD_PAYLOAD_INT, physical 1
    let code = vec![EXPR_LOAD_COL_INT, 0, 2, 0];
    let mut prog = ExprProgram::new(code, 1, 0, vec![]);
    prog.resolve_column_indices(&schema);
    assert_eq!(prog.code[2], 1);
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

    // col0 (logical 0, before pk) → physical 0
    let code = vec![EXPR_LOAD_COL_INT, 0, 0, 0];
    let mut prog = ExprProgram::new(code, 1, 0, vec![]);
    prog.resolve_column_indices(&schema);
    assert_eq!(prog.code[0], EXPR_LOAD_PAYLOAD_INT);
    assert_eq!(prog.code[2], 0);
    let (val, _) = eval_predicate(&prog, &mb, 0);
    assert_eq!(val, 5);

    // col1 (logical 1 = pk) → LOAD_PK_INT
    let code = vec![EXPR_LOAD_COL_INT, 0, 1, 0];
    let mut prog = ExprProgram::new(code, 1, 0, vec![]);
    prog.resolve_column_indices(&schema);
    assert_eq!(prog.code[0], EXPR_LOAD_PK_UNSIGNED_INT);
    let (val, _) = eval_predicate(&prog, &mb, 0);
    assert_eq!(val, 99);

    // col2 (logical 2, after pk) → physical 1
    let code = vec![EXPR_LOAD_COL_INT, 0, 2, 0];
    let mut prog = ExprProgram::new(code, 1, 0, vec![]);
    prog.resolve_column_indices(&schema);
    assert_eq!(prog.code[0], EXPR_LOAD_PAYLOAD_INT);
    assert_eq!(prog.code[2], 1);
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

    // STR_COL_EQ_CONST on col1
    for (op, _name) in &[
        (EXPR_STR_COL_EQ_CONST, "EQ_CONST"),
        (EXPR_STR_COL_LT_CONST, "LT_CONST"),
        (EXPR_STR_COL_LE_CONST, "LE_CONST"),
    ] {
        let code = vec![*op, 0, 1, 0];
        let prog = make_prog(&nullable_schema, code.clone(), 1, 0, vec![b"x".to_vec()]);
        assert!(
            !prog.is_strictly_non_nullable(&nullable_schema),
            "{_name}: nullable col1 must yield no_nulls=false"
        );
        let prog = make_prog(&nonnull_schema, code, 1, 0, vec![b"x".to_vec()]);
        assert!(
            prog.is_strictly_non_nullable(&nonnull_schema),
            "{_name}: non-nullable col1 must yield no_nulls=true"
        );
    }

    // STR_COL_EQ_COL — both operands matter
    for (op, _name) in &[
        (EXPR_STR_COL_EQ_COL, "EQ_COL"),
        (EXPR_STR_COL_LT_COL, "LT_COL"),
        (EXPR_STR_COL_LE_COL, "LE_COL"),
    ] {
        let code = vec![*op, 0, 1, 2];
        let prog = make_prog(&nullable_schema, code.clone(), 1, 0, vec![]);
        assert!(
            !prog.is_strictly_non_nullable(&nullable_schema),
            "{_name}: nullable operands must yield no_nulls=false"
        );
        let prog = make_prog(&nonnull_schema, code, 1, 0, vec![]);
        assert!(
            prog.is_strictly_non_nullable(&nonnull_schema),
            "{_name}: non-nullable operands must yield no_nulls=true"
        );
    }
}

#[test]
fn test_resolve_column_indices_is_idempotent() {
    // Schema: pk=col0(U64), col1=I64, col2=STRING
    let schema = make_schema(0, &[8, 9, 11]);
    // One opcode of each remapped class so a second pass would corrupt the
    // bytecode if the resolved flag were not respected.
    let code = vec![
        EXPR_LOAD_COL_INT,
        0,
        1,
        0,
        EXPR_IS_NULL,
        1,
        2,
        0,
        EXPR_COPY_COL,
        9,
        1,
        0,
        EXPR_STR_COL_EQ_COL,
        2,
        2,
        2,
    ];
    let mut prog = ExprProgram::new(code, 3, 0, vec![]);
    prog.resolve_column_indices(&schema);
    let first = prog.code.clone();
    prog.resolve_column_indices(&schema);
    assert_eq!(prog.code, first, "resolve_column_indices must be idempotent");
}

/// Build a 16-byte inline German String struct for use in test batches.
fn make_german_string(s: &[u8]) -> [u8; 16] {
    assert!(s.len() <= 12, "test helper only handles inline strings (≤ 12 bytes)");
    let mut gs = [0u8; 16];
    gs[0..4].copy_from_slice(&(s.len() as u32).to_le_bytes());
    let pfx = s.len().min(4);
    gs[4..4 + pfx].copy_from_slice(&s[..pfx]);
    if s.len() > 4 {
        gs[8..8 + (s.len() - 4)].copy_from_slice(&s[4..]);
    }
    gs
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
        let code = vec![EXPR_STR_COL_LT_CONST, 0, 1, 0];
        let prog = make_prog(&schema, code, 1, 0, vec![const_s.to_vec()]);
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

    let and_code = vec![
        EXPR_LOAD_COL_INT,
        0,
        1,
        0,
        EXPR_LOAD_COL_INT,
        1,
        2,
        0,
        EXPR_BOOL_AND,
        2,
        0,
        1,
    ];
    let and_prog = make_prog(&schema, and_code, 3, 2, vec![]);

    let or_code = vec![
        EXPR_LOAD_COL_INT,
        0,
        1,
        0,
        EXPR_LOAD_COL_INT,
        1,
        2,
        0,
        EXPR_BOOL_OR,
        2,
        0,
        1,
    ];
    let or_prog = make_prog(&schema, or_code, 3, 2, vec![]);

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
        let code = vec![EXPR_STR_COL_LE_CONST, 0, 1, 0];
        let prog = make_prog(&schema, code, 1, 0, vec![const_s.to_vec()]);
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
