use super::super::program::*;
use crate::schema::{SchemaColumn, SchemaDescriptor};
use crate::storage::Batch;

fn make_schema(num_cols: u32, pk_index: u32, col_types: &[(u8, u8)]) -> SchemaDescriptor {
    let mut columns = [SchemaColumn {
        type_code: 0,
        size: 0,
        nullable: 0,
        _pad: 0,
    }; 64];
    for (i, &(tc, sz)) in col_types.iter().enumerate() {
        columns[i] = SchemaColumn {
            type_code: tc,
            size: sz,
            nullable: if i == pk_index as usize { 0 } else { 1 },
            _pad: 0,
        };
    }
    SchemaDescriptor {
        num_columns: num_cols,
        pk_index,
        columns,
    }
}

fn make_int_batch(
    schema: &SchemaDescriptor,
    rows: &[(u64, i64, u64, &[i64])],
) -> Batch {
    let mut batch = Batch::with_schema(*schema, rows.len().max(1));
    batch.count = 0;
    for &(pk, weight, null_word, cols) in rows {
        batch.extend_pk_lo(&pk.to_le_bytes());
        batch.extend_pk_hi(&0u64.to_le_bytes());
        batch.extend_weight(&weight.to_le_bytes());
        batch.extend_null_bmp(&null_word.to_le_bytes());
        let mut pi = 0;
        for ci in 0..schema.num_columns as usize {
            if ci == schema.pk_index as usize {
                continue;
            }
            if pi < cols.len() {
                batch.extend_col(pi, &cols[pi].to_le_bytes());
            }
            pi += 1;
        }
        batch.count += 1;
    }
    batch
}

#[test]
fn test_int_comparisons() {
    // Schema: 2 columns, col0=PK(U64), col1=I64
    let schema = make_schema(2, 0, &[(8, 8), (9, 8)]);
    let batch = make_int_batch(&schema, &[(1, 1, 0, &[42])]);
    let mb = batch.as_mem_batch();

    // r0 = load_col_int(1), r1 = load_const(42), r2 = cmp_eq(r0, r1)
    let code = vec![
        EXPR_LOAD_COL_INT, 0, 1, 0, // r0 = col[1]
        EXPR_LOAD_CONST, 1, 42, 0, // r1 = 42
        EXPR_CMP_EQ, 2, 0, 1, // r2 = (r0 == r1)
    ];
    let prog = ExprProgram::new(code, 3, 2, vec![]);
    let (val, is_null) = eval_predicate(&prog, &mb, 0, 0);
    assert_eq!(val, 1);
    assert!(!is_null);

    // Test NE
    let code = vec![
        EXPR_LOAD_COL_INT, 0, 1, 0,
        EXPR_LOAD_CONST, 1, 42, 0,
        EXPR_CMP_NE, 2, 0, 1,
    ];
    let prog = ExprProgram::new(code, 3, 2, vec![]);
    let (val, _) = eval_predicate(&prog, &mb, 0, 0);
    assert_eq!(val, 0);

    // Test GT
    let code = vec![
        EXPR_LOAD_COL_INT, 0, 1, 0,
        EXPR_LOAD_CONST, 1, 10, 0,
        EXPR_CMP_GT, 2, 0, 1,
    ];
    let prog = ExprProgram::new(code, 3, 2, vec![]);
    let (val, _) = eval_predicate(&prog, &mb, 0, 0);
    assert_eq!(val, 1);
}

#[test]
fn test_int_arithmetic() {
    let schema = make_schema(3, 0, &[(8, 8), (9, 8), (9, 8)]);
    let batch = make_int_batch(&schema, &[(1, 1, 0, &[10, 3])]);
    let mb = batch.as_mem_batch();

    // ADD: 10 + 3 = 13
    let code = vec![
        EXPR_LOAD_COL_INT, 0, 1, 0,
        EXPR_LOAD_COL_INT, 1, 2, 0,
        EXPR_INT_ADD, 2, 0, 1,
    ];
    let prog = ExprProgram::new(code, 3, 2, vec![]);
    let (val, _) = eval_predicate(&prog, &mb, 0, 0);
    assert_eq!(val, 13);

    // DIV by zero → NULL
    let code = vec![
        EXPR_LOAD_COL_INT, 0, 1, 0,
        EXPR_LOAD_CONST, 1, 0, 0,
        EXPR_INT_DIV, 2, 0, 1,
    ];
    let prog = ExprProgram::new(code, 3, 2, vec![]);
    let (_, is_null) = eval_predicate(&prog, &mb, 0, 0);
    assert!(is_null);

    // MOD by zero → NULL
    let code = vec![
        EXPR_LOAD_COL_INT, 0, 1, 0,
        EXPR_LOAD_CONST, 1, 0, 0,
        EXPR_INT_MOD, 2, 0, 1,
    ];
    let prog = ExprProgram::new(code, 3, 2, vec![]);
    let (_, is_null) = eval_predicate(&prog, &mb, 0, 0);
    assert!(is_null);

    // NEG: -10
    let code = vec![
        EXPR_LOAD_COL_INT, 0, 1, 0,
        EXPR_INT_NEG, 1, 0, 0,
    ];
    let prog = ExprProgram::new(code, 2, 1, vec![]);
    let (val, _) = eval_predicate(&prog, &mb, 0, 0);
    assert_eq!(val, -10);
}

#[test]
fn test_float_arithmetic_and_comparison() {
    let schema = make_schema(3, 0, &[(8, 8), (10, 8), (10, 8)]);
    // Store floats as i64 bits
    let a_bits = float_to_bits(3.14);
    let b_bits = float_to_bits(2.0);
    let batch = make_int_batch(&schema, &[(1, 1, 0, &[a_bits, b_bits])]);
    let mb = batch.as_mem_batch();

    // FLOAT_ADD: 3.14 + 2.0
    let code = vec![
        EXPR_LOAD_COL_FLOAT, 0, 1, 0,
        EXPR_LOAD_COL_FLOAT, 1, 2, 0,
        EXPR_FLOAT_ADD, 2, 0, 1,
    ];
    let prog = ExprProgram::new(code, 3, 2, vec![]);
    let (val, _) = eval_predicate(&prog, &mb, 0, 0);
    let result = bits_to_float(val);
    assert!((result - 5.14).abs() < 1e-10);

    // FLOAT_DIV by zero → NULL
    let code = vec![
        EXPR_LOAD_COL_FLOAT, 0, 1, 0,
        EXPR_LOAD_CONST, 1, 0, 0, // zero bits
        EXPR_FLOAT_DIV, 2, 0, 1,
    ];
    let prog = ExprProgram::new(code, 3, 2, vec![]);
    let (_, is_null) = eval_predicate(&prog, &mb, 0, 0);
    assert!(is_null);

    // FCMP_GT: 3.14 > 2.0
    let code = vec![
        EXPR_LOAD_COL_FLOAT, 0, 1, 0,
        EXPR_LOAD_COL_FLOAT, 1, 2, 0,
        EXPR_FCMP_GT, 2, 0, 1,
    ];
    let prog = ExprProgram::new(code, 3, 2, vec![]);
    let (val, _) = eval_predicate(&prog, &mb, 0, 0);
    assert_eq!(val, 1);
}

#[test]
fn test_null_propagation() {
    let schema = make_schema(3, 0, &[(8, 8), (9, 8), (9, 8)]);
    // col1 is null (bit 0 set), col2 is not null
    let batch = make_int_batch(&schema, &[(1, 1, 1, &[0, 5])]);
    let mb = batch.as_mem_batch();

    // ADD with one null operand → null
    let code = vec![
        EXPR_LOAD_COL_INT, 0, 1, 0,
        EXPR_LOAD_COL_INT, 1, 2, 0,
        EXPR_INT_ADD, 2, 0, 1,
    ];
    let prog = ExprProgram::new(code, 3, 2, vec![]);
    let (_, is_null) = eval_predicate(&prog, &mb, 0, 0);
    assert!(is_null);
}

#[test]
fn test_is_null_is_not_null() {
    let schema = make_schema(3, 0, &[(8, 8), (9, 8), (9, 8)]);
    // col1 is null (bit 0 set), col2 is not null
    let batch = make_int_batch(&schema, &[(1, 1, 1, &[0, 5])]);
    let mb = batch.as_mem_batch();

    // IS_NULL(col1) → 1 (always non-null result)
    let code = vec![EXPR_IS_NULL, 0, 1, 0];
    let prog = ExprProgram::new(code, 1, 0, vec![]);
    let (val, is_null) = eval_predicate(&prog, &mb, 0, 0);
    assert_eq!(val, 1);
    assert!(!is_null);

    // IS_NOT_NULL(col1) → 0
    let code = vec![EXPR_IS_NOT_NULL, 0, 1, 0];
    let prog = ExprProgram::new(code, 1, 0, vec![]);
    let (val, is_null) = eval_predicate(&prog, &mb, 0, 0);
    assert_eq!(val, 0);
    assert!(!is_null);

    // IS_NULL(col2) → 0
    let code = vec![EXPR_IS_NULL, 0, 2, 0];
    let prog = ExprProgram::new(code, 1, 0, vec![]);
    let (val, _) = eval_predicate(&prog, &mb, 0, 0);
    assert_eq!(val, 0);
}

#[test]
fn test_boolean_combinators() {
    let schema = make_schema(2, 0, &[(8, 8), (9, 8)]);
    let batch = make_int_batch(&schema, &[(1, 1, 0, &[1])]);
    let mb = batch.as_mem_batch();

    // AND(1, 0) → 0
    let code = vec![
        EXPR_LOAD_COL_INT, 0, 1, 0,
        EXPR_LOAD_CONST, 1, 0, 0,
        EXPR_BOOL_AND, 2, 0, 1,
    ];
    let prog = ExprProgram::new(code, 3, 2, vec![]);
    let (val, _) = eval_predicate(&prog, &mb, 0, 0);
    assert_eq!(val, 0);

    // OR(1, 0) → 1
    let code = vec![
        EXPR_LOAD_COL_INT, 0, 1, 0,
        EXPR_LOAD_CONST, 1, 0, 0,
        EXPR_BOOL_OR, 2, 0, 1,
    ];
    let prog = ExprProgram::new(code, 3, 2, vec![]);
    let (val, _) = eval_predicate(&prog, &mb, 0, 0);
    assert_eq!(val, 1);

    // NOT(1) → 0
    let code = vec![
        EXPR_LOAD_COL_INT, 0, 1, 0,
        EXPR_BOOL_NOT, 1, 0, 0,
    ];
    let prog = ExprProgram::new(code, 2, 1, vec![]);
    let (val, _) = eval_predicate(&prog, &mb, 0, 0);
    assert_eq!(val, 0);
}

#[test]
fn test_load_const_encoding() {
    let schema = make_schema(2, 0, &[(8, 8), (9, 8)]);
    let batch = make_int_batch(&schema, &[(1, 1, 0, &[0])]);
    let mb = batch.as_mem_batch();

    // Test large constant: 0x00000001_00000002 = (1 << 32) | 2 = 4294967298
    // a1 = lo 32 bits = 2, a2 = hi 32 bits = 1
    let code = vec![EXPR_LOAD_CONST, 0, 2, 1];
    let prog = ExprProgram::new(code, 1, 0, vec![]);
    let (val, is_null) = eval_predicate(&prog, &mb, 0, 0);
    assert!(!is_null);
    assert_eq!(val, (1i64 << 32) | 2);

    // Test negative constant: -1 → lo=0xFFFFFFFF, hi=0xFFFFFFFF
    // As i64: a1 = -1 (sign-extended from 32 bits), a2 = -1
    // Reconstruction: (-1 << 32) | (-1 & 0xFFFFFFFF) = 0xFFFFFFFF_FFFFFFFF = -1
    let code = vec![EXPR_LOAD_CONST, 0, -1, -1];
    let prog = ExprProgram::new(code, 1, 0, vec![]);
    let (val, _) = eval_predicate(&prog, &mb, 0, 0);
    assert_eq!(val, -1);
}

#[test]
fn test_string_eq_const() {
    let schema = make_schema(2, 0, &[(8, 8), (11, 16)]);
    // Build a batch with a short string "hello"
    let mut batch = Batch::with_schema(schema, 1);
    batch.count = 0;
    batch.extend_pk_lo(&1u64.to_le_bytes());
    batch.extend_pk_hi(&0u64.to_le_bytes());
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
    let prog = ExprProgram::new(code, 1, 0, vec![b"hello".to_vec()]);
    let (val, is_null) = eval_predicate(&prog, &mb, 0, 0);
    assert!(!is_null);
    assert_eq!(val, 1);

    // STR_COL_EQ_CONST(col1, "world") → 0
    let code = vec![EXPR_STR_COL_EQ_CONST, 0, 1, 0];
    let prog = ExprProgram::new(code, 1, 0, vec![b"world".to_vec()]);
    let (val, _) = eval_predicate(&prog, &mb, 0, 0);
    assert_eq!(val, 0);
}

#[test]
fn test_int_to_float() {
    let schema = make_schema(2, 0, &[(8, 8), (9, 8)]);
    let batch = make_int_batch(&schema, &[(1, 1, 0, &[42])]);
    let mb = batch.as_mem_batch();

    let code = vec![
        EXPR_LOAD_COL_INT, 0, 1, 0,
        EXPR_INT_TO_FLOAT, 1, 0, 0,
    ];
    let prog = ExprProgram::new(code, 2, 1, vec![]);
    let (val, _) = eval_predicate(&prog, &mb, 0, 0);
    assert_eq!(bits_to_float(val), 42.0);
}

#[test]
fn test_emit_with_targets() {
    let schema = make_schema(3, 0, &[(8, 8), (9, 8), (9, 8)]);
    let batch = make_int_batch(&schema, &[(1, 1, 0, &[10, 20])]);
    let mb = batch.as_mem_batch();

    // Compute col1 + col2, EMIT to payload col 0
    let code = vec![
        EXPR_LOAD_COL_INT, 0, 1, 0,
        EXPR_LOAD_COL_INT, 1, 2, 0,
        EXPR_INT_ADD, 2, 0, 1,
        EXPR_EMIT, 0, 2, 0, // emit r2 to payload col 0
    ];
    let prog = ExprProgram::new(code, 3, 2, vec![]);

    let mut out_buf = [0u8; 8];
    let targets = [EmitTarget {
        base: out_buf.as_mut_ptr(),
        stride: 8,
        payload_col: 0,
    }];
    let (val, is_null, mask) =
        eval_with_emit(&prog, &mb, 0, 0, 0, &targets);
    assert_eq!(val, 30); // 10 + 20
    assert!(!is_null);
    assert_eq!(mask, 0);
    let written = i64::from_le_bytes(out_buf);
    assert_eq!(written, 30);
}

#[test]
fn test_div_by_zero_null_semantics() {
    // Schema: pk(u64), col1(i64), col2(i64)
    let schema = make_schema(3, 0, &[(8, 8), (9, 8), (9, 8)]);
    // Row: pk=1, col1=10, col2=3; null_word=0 (no nulls)
    let batch = make_int_batch(&schema, &[(1, 1, 0, &[10, 3])]);
    let mb = batch.as_mem_batch();

    // 1. INT_DIV by literal 0 → NULL
    let code = vec![
        EXPR_LOAD_COL_INT, 0, 1, 0,
        EXPR_LOAD_CONST, 1, 0, 0,
        EXPR_INT_DIV, 2, 0, 1,
    ];
    let prog = ExprProgram::new(code, 3, 2, vec![]);
    let (_, is_null) = eval_predicate(&prog, &mb, 0, 0);
    assert!(is_null);

    // 2. INT_MOD by literal 0 → NULL
    let code = vec![
        EXPR_LOAD_COL_INT, 0, 1, 0,
        EXPR_LOAD_CONST, 1, 0, 0,
        EXPR_INT_MOD, 2, 0, 1,
    ];
    let prog = ExprProgram::new(code, 3, 2, vec![]);
    let (_, is_null) = eval_predicate(&prog, &mb, 0, 0);
    assert!(is_null);

    // 3. FLOAT_DIV by 0.0 bits → NULL
    let code = vec![
        EXPR_LOAD_COL_INT, 0, 1, 0,
        EXPR_LOAD_CONST, 1, 0, 0,
        EXPR_FLOAT_DIV, 2, 0, 1,
    ];
    let prog = ExprProgram::new(code, 3, 2, vec![]);
    let (_, is_null) = eval_predicate(&prog, &mb, 0, 0);
    assert!(is_null);

    // 4. INT_DIV by non-null non-zero → correct quotient, not null
    let code = vec![
        EXPR_LOAD_COL_INT, 0, 1, 0,
        EXPR_LOAD_COL_INT, 1, 2, 0,
        EXPR_INT_DIV, 2, 0, 1,
    ];
    let prog = ExprProgram::new(code, 3, 2, vec![]);
    let (val, is_null) = eval_predicate(&prog, &mb, 0, 0);
    assert!(!is_null);
    assert_eq!(val, 3); // 10 / 3 = 3

    // 5. INT_DIV where divisor column is null → NULL (null propagation)
    // null_word bit 0 = col1 null; use col2 (bit 1) as divisor with col1 null
    // Schema col indices: col1=payload_idx 0, col2=payload_idx 1
    // Build a row where col2 is null (null_word bit 1 set)
    let batch_null_div = make_int_batch(&schema, &[(1, 1, 2, &[10, 3])]);
    let mb_null_div = batch_null_div.as_mem_batch();
    let code = vec![
        EXPR_LOAD_COL_INT, 0, 1, 0,
        EXPR_LOAD_COL_INT, 1, 2, 0,
        EXPR_INT_DIV, 2, 0, 1,
    ];
    let prog = ExprProgram::new(code, 3, 2, vec![]);
    let (_, is_null) = eval_predicate(&prog, &mb_null_div, 0, 0);
    assert!(is_null);

    // 6. EMIT of INT_DIV-by-zero result → emit_null_mask bit set, buffer contains 0
    let code = vec![
        EXPR_LOAD_COL_INT, 0, 1, 0,
        EXPR_LOAD_CONST, 1, 0, 0,
        EXPR_INT_DIV, 2, 0, 1,
        EXPR_EMIT, 0, 2, 0,
    ];
    let prog = ExprProgram::new(code, 3, 2, vec![]);
    let mut out_buf = [0xffu8; 8];
    let targets = [EmitTarget {
        base: out_buf.as_mut_ptr(),
        stride: 8,
        payload_col: 0,
    }];
    let (_, _, mask) = eval_with_emit(&prog, &mb, 0, 0, 0, &targets);
    assert_eq!(mask & 1, 1); // bit 0 set → null
    let written = i64::from_le_bytes(out_buf);
    assert_eq!(written, 0);
}

#[test]
fn test_cmp_ge_lt_le() {
    let schema = make_schema(2, 0, &[(8, 8), (9, 8)]);
    let batch = make_int_batch(&schema, &[(1, 1, 0, &[42])]);
    let mb = batch.as_mem_batch();

    // GE: 42 >= 42 → 1
    let code = vec![
        EXPR_LOAD_COL_INT, 0, 1, 0,
        EXPR_LOAD_CONST, 1, 42, 0,
        EXPR_CMP_GE, 2, 0, 1,
    ];
    let prog = ExprProgram::new(code, 3, 2, vec![]);
    let (val, _) = eval_predicate(&prog, &mb, 0, 0);
    assert_eq!(val, 1);

    // GE: 42 >= 43 → 0
    let code = vec![
        EXPR_LOAD_COL_INT, 0, 1, 0,
        EXPR_LOAD_CONST, 1, 43, 0,
        EXPR_CMP_GE, 2, 0, 1,
    ];
    let prog = ExprProgram::new(code, 3, 2, vec![]);
    let (val, _) = eval_predicate(&prog, &mb, 0, 0);
    assert_eq!(val, 0);

    // LT: 42 < 43 → 1
    let code = vec![
        EXPR_LOAD_COL_INT, 0, 1, 0,
        EXPR_LOAD_CONST, 1, 43, 0,
        EXPR_CMP_LT, 2, 0, 1,
    ];
    let prog = ExprProgram::new(code, 3, 2, vec![]);
    let (val, _) = eval_predicate(&prog, &mb, 0, 0);
    assert_eq!(val, 1);

    // LT: 42 < 42 → 0
    let code = vec![
        EXPR_LOAD_COL_INT, 0, 1, 0,
        EXPR_LOAD_CONST, 1, 42, 0,
        EXPR_CMP_LT, 2, 0, 1,
    ];
    let prog = ExprProgram::new(code, 3, 2, vec![]);
    let (val, _) = eval_predicate(&prog, &mb, 0, 0);
    assert_eq!(val, 0);

    // LE: 42 <= 42 → 1
    let code = vec![
        EXPR_LOAD_COL_INT, 0, 1, 0,
        EXPR_LOAD_CONST, 1, 42, 0,
        EXPR_CMP_LE, 2, 0, 1,
    ];
    let prog = ExprProgram::new(code, 3, 2, vec![]);
    let (val, _) = eval_predicate(&prog, &mb, 0, 0);
    assert_eq!(val, 1);

    // LE: 42 <= 41 → 0
    let code = vec![
        EXPR_LOAD_COL_INT, 0, 1, 0,
        EXPR_LOAD_CONST, 1, 41, 0,
        EXPR_CMP_LE, 2, 0, 1,
    ];
    let prog = ExprProgram::new(code, 3, 2, vec![]);
    let (val, _) = eval_predicate(&prog, &mb, 0, 0);
    assert_eq!(val, 0);
}

#[test]
fn test_fcmp_eq_ne_lt_le() {
    let schema = make_schema(3, 0, &[(8, 8), (10, 8), (10, 8)]);
    let a_bits = float_to_bits(3.14);
    let b_bits = float_to_bits(2.0);
    let batch = make_int_batch(&schema, &[(1, 1, 0, &[a_bits, b_bits])]);
    let mb = batch.as_mem_batch();

    // FCMP_EQ: 3.14 == 3.14 → 1
    let code = vec![
        EXPR_LOAD_COL_FLOAT, 0, 1, 0,
        EXPR_LOAD_COL_FLOAT, 1, 1, 0,
        EXPR_FCMP_EQ, 2, 0, 1,
    ];
    let prog = ExprProgram::new(code, 3, 2, vec![]);
    let (val, _) = eval_predicate(&prog, &mb, 0, 0);
    assert_eq!(val, 1);

    // FCMP_EQ: 3.14 == 2.0 → 0
    let code = vec![
        EXPR_LOAD_COL_FLOAT, 0, 1, 0,
        EXPR_LOAD_COL_FLOAT, 1, 2, 0,
        EXPR_FCMP_EQ, 2, 0, 1,
    ];
    let prog = ExprProgram::new(code, 3, 2, vec![]);
    let (val, _) = eval_predicate(&prog, &mb, 0, 0);
    assert_eq!(val, 0);

    // FCMP_NE: 3.14 != 2.0 → 1
    let code = vec![
        EXPR_LOAD_COL_FLOAT, 0, 1, 0,
        EXPR_LOAD_COL_FLOAT, 1, 2, 0,
        EXPR_FCMP_NE, 2, 0, 1,
    ];
    let prog = ExprProgram::new(code, 3, 2, vec![]);
    let (val, _) = eval_predicate(&prog, &mb, 0, 0);
    assert_eq!(val, 1);

    // FCMP_LT: 2.0 < 3.14 → 1
    let code = vec![
        EXPR_LOAD_COL_FLOAT, 0, 2, 0,
        EXPR_LOAD_COL_FLOAT, 1, 1, 0,
        EXPR_FCMP_LT, 2, 0, 1,
    ];
    let prog = ExprProgram::new(code, 3, 2, vec![]);
    let (val, _) = eval_predicate(&prog, &mb, 0, 0);
    assert_eq!(val, 1);

    // FCMP_LE: 2.0 <= 2.0 → 1
    let code = vec![
        EXPR_LOAD_COL_FLOAT, 0, 2, 0,
        EXPR_LOAD_COL_FLOAT, 1, 2, 0,
        EXPR_FCMP_LE, 2, 0, 1,
    ];
    let prog = ExprProgram::new(code, 3, 2, vec![]);
    let (val, _) = eval_predicate(&prog, &mb, 0, 0);
    assert_eq!(val, 1);

    // FCMP_GE: 2.0 >= 3.14 → 0
    let code = vec![
        EXPR_LOAD_COL_FLOAT, 0, 2, 0,
        EXPR_LOAD_COL_FLOAT, 1, 1, 0,
        EXPR_FCMP_GE, 2, 0, 1,
    ];
    let prog = ExprProgram::new(code, 3, 2, vec![]);
    let (val, _) = eval_predicate(&prog, &mb, 0, 0);
    assert_eq!(val, 0);
}

#[test]
fn test_string_lt_le_const() {
    let schema = make_schema(2, 0, &[(8, 8), (11, 16)]);
    let mut batch = Batch::with_schema(schema, 1);
    batch.count = 0;
    batch.extend_pk_lo(&1u64.to_le_bytes());
    batch.extend_pk_hi(&0u64.to_le_bytes());
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
    let prog = ExprProgram::new(code, 1, 0, vec![b"world".to_vec()]);
    let (val, _) = eval_predicate(&prog, &mb, 0, 0);
    assert_eq!(val, 1);

    // STR_COL_LT_CONST: "hello" < "hello" → 0
    let code = vec![EXPR_STR_COL_LT_CONST, 0, 1, 0];
    let prog = ExprProgram::new(code, 1, 0, vec![b"hello".to_vec()]);
    let (val, _) = eval_predicate(&prog, &mb, 0, 0);
    assert_eq!(val, 0);

    // STR_COL_LE_CONST: "hello" <= "hello" → 1
    let code = vec![EXPR_STR_COL_LE_CONST, 0, 1, 0];
    let prog = ExprProgram::new(code, 1, 0, vec![b"hello".to_vec()]);
    let (val, _) = eval_predicate(&prog, &mb, 0, 0);
    assert_eq!(val, 1);

    // STR_COL_LE_CONST: "hello" <= "hella" → 0
    let code = vec![EXPR_STR_COL_LE_CONST, 0, 1, 0];
    let prog = ExprProgram::new(code, 1, 0, vec![b"hella".to_vec()]);
    let (val, _) = eval_predicate(&prog, &mb, 0, 0);
    assert_eq!(val, 0);
}

#[test]
fn test_string_col_eq_col() {
    // Schema: pk(U64), str_a(STRING), str_b(STRING)
    let schema = make_schema(3, 0, &[(8, 8), (11, 16), (11, 16)]);
    let mut batch = Batch::with_schema(schema, 2);
    batch.count = 0;

    // Row 0: str_a="abc", str_b="abc" (equal)
    batch.extend_pk_lo(&1u64.to_le_bytes());
    batch.extend_pk_hi(&0u64.to_le_bytes());
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
    batch.extend_pk_lo(&2u64.to_le_bytes());
    batch.extend_pk_hi(&0u64.to_le_bytes());
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
    let prog = ExprProgram::new(code, 1, 0, vec![]);
    let (val, _) = eval_predicate(&prog, &mb, 0, 0);
    assert_eq!(val, 1);

    // Row 1: col1 == col2 → 0
    let (val, _) = eval_predicate(&prog, &mb, 1, 0);
    assert_eq!(val, 0);
}

#[test]
fn test_complex_predicate() {
    // Schema: pk(U64), a(I64), b(I64), c(I64)
    let schema = make_schema(4, 0, &[(8, 8), (9, 8), (9, 8), (9, 8)]);
    // Row: a=15, b=50, c=42
    let batch = make_int_batch(&schema, &[(1, 1, 0, &[15, 50, 42])]);
    let mb = batch.as_mem_batch();

    // (a > 10 AND b < 100) OR c == 42
    // r0=col1(a), r1=10, r2=(a>10), r3=col2(b), r4=100, r5=(b<100)
    // r6=(r2 AND r5), r7=col3(c), r8=42, r9=(c==42), r10=(r6 OR r9)
    let code = vec![
        EXPR_LOAD_COL_INT, 0, 1, 0,   // r0 = a = 15
        EXPR_LOAD_CONST, 1, 10, 0,    // r1 = 10
        EXPR_CMP_GT, 2, 0, 1,         // r2 = (15 > 10) = 1
        EXPR_LOAD_COL_INT, 3, 2, 0,   // r3 = b = 50
        EXPR_LOAD_CONST, 4, 100, 0,   // r4 = 100
        EXPR_CMP_LT, 5, 3, 4,         // r5 = (50 < 100) = 1
        EXPR_BOOL_AND, 6, 2, 5,       // r6 = (1 AND 1) = 1
        EXPR_LOAD_COL_INT, 7, 3, 0,   // r7 = c = 42
        EXPR_LOAD_CONST, 8, 42, 0,    // r8 = 42
        EXPR_CMP_EQ, 9, 7, 8,         // r9 = (42 == 42) = 1
        EXPR_BOOL_OR, 10, 6, 9,       // r10 = (1 OR 1) = 1
    ];
    let prog = ExprProgram::new(code, 11, 10, vec![]);
    let (val, is_null) = eval_predicate(&prog, &mb, 0, 0);
    assert_eq!(val, 1);
    assert!(!is_null);

    // Test with a=5 (a>10 false), b=50, c=99 (c==42 false) → false
    let batch2 = make_int_batch(&schema, &[(1, 1, 0, &[5, 50, 99])]);
    let mb2 = batch2.as_mem_batch();
    let (val, _) = eval_predicate(&prog, &mb2, 0, 0);
    assert_eq!(val, 0);
}

#[test]
fn test_emit_null_opcode() {
    // Schema: pk(U64), val(I64)
    let schema = make_schema(2, 0, &[(8, 8), (9, 8)]);
    let batch = make_int_batch(&schema, &[(1, 1, 0, &[10])]);
    let mb = batch.as_mem_batch();

    // EMIT_NULL is handled at batch level, not per-row eval.
    // Verify it doesn't crash and is treated as a no-op in eval_with_emit.
    let code = vec![
        EXPR_LOAD_COL_INT, 0, 1, 0,  // r0 = col1
        EXPR_EMIT_NULL, 0, 0, 0,     // emit null — batch-level, no-op here
    ];
    let prog = ExprProgram::new(code, 1, 0, vec![]);

    let mut out_buf = [0u8; 8];
    let targets = [EmitTarget {
        base: out_buf.as_mut_ptr(),
        stride: 8,
        payload_col: 0,
    }];
    let (val, _, mask) = eval_with_emit(&prog, &mb, 0, 0, 0, &targets);
    // EMIT_NULL is a no-op at per-row level (batch-level handles it),
    // so emit_null_mask stays 0 and result reg r0 holds the loaded value.
    assert_eq!(mask, 0);
    assert_eq!(val, 10);
}

#[test]
fn test_zero_regs_program() {
    // ExprProgram with num_regs=0 (pure COPY_COL) must not crash.
    let schema = make_schema(2, 0, &[(8, 8), (9, 8)]);
    let batch = make_int_batch(&schema, &[(1, 1, 0, &[100])]);
    let mb = batch.as_mem_batch();

    // One COPY_COL instruction: copy col 1 → payload 0, type I64=9
    let code = vec![EXPR_COPY_COL, 9, 1, 0];
    let prog = ExprProgram::new(code, 0, 0, vec![]);
    let (val, is_null) = eval_predicate(&prog, &mb, 0, 0);
    // With num_regs=0, result should be (0, true) — sentinel
    assert_eq!(val, 0);
    assert!(is_null);
}

#[test]
fn test_resolve_column_indices_pk_at_col0() {
    // Schema: pk=col0(U64), col1=I64, col2=I64
    let schema = make_schema(3, 0, &[(8, 8), (9, 8), (9, 8)]);
    let batch = make_int_batch(&schema, &[(42, 1, 0, &[10, 20])]);
    let mb = batch.as_mem_batch();

    // LOAD_COL_INT of pk column → LOAD_PK_INT
    let code = vec![EXPR_LOAD_COL_INT, 0, 0, 0]; // col 0 = pk
    let mut prog = ExprProgram::new(code, 1, 0, vec![]);
    prog.resolve_column_indices(0);
    assert_eq!(prog.code[0], EXPR_LOAD_PK_INT);
    let (val, is_null) = eval_predicate(&prog, &mb, 0, 0);
    assert_eq!(val, 42);
    assert!(!is_null);

    // LOAD_COL_INT of col1 (logical 1) → LOAD_PAYLOAD_INT, physical 0
    let code = vec![EXPR_LOAD_COL_INT, 0, 1, 0];
    let mut prog = ExprProgram::new(code, 1, 0, vec![]);
    prog.resolve_column_indices(0);
    assert_eq!(prog.code[0], EXPR_LOAD_PAYLOAD_INT);
    assert_eq!(prog.code[2], 0); // physical payload index
    let (val, _) = eval_predicate(&prog, &mb, 0, 0);
    assert_eq!(val, 10);

    // LOAD_COL_INT of col2 (logical 2) → LOAD_PAYLOAD_INT, physical 1
    let code = vec![EXPR_LOAD_COL_INT, 0, 2, 0];
    let mut prog = ExprProgram::new(code, 1, 0, vec![]);
    prog.resolve_column_indices(0);
    assert_eq!(prog.code[2], 1);
    let (val, _) = eval_predicate(&prog, &mb, 0, 0);
    assert_eq!(val, 20);
}

#[test]
fn test_resolve_column_indices_pk_at_middle() {
    // Schema: col0=I64, pk=col1(U64), col2=I64
    // Physical payload layout: [col0=payload0, col2=payload1]
    let schema = make_schema(3, 1, &[(9, 8), (8, 8), (9, 8)]);
    let batch = make_int_batch(&schema, &[(99, 1, 0, &[5, 7])]);
    let mb = batch.as_mem_batch();

    // col0 (logical 0, before pk) → physical 0
    let code = vec![EXPR_LOAD_COL_INT, 0, 0, 0];
    let mut prog = ExprProgram::new(code, 1, 0, vec![]);
    prog.resolve_column_indices(1);
    assert_eq!(prog.code[0], EXPR_LOAD_PAYLOAD_INT);
    assert_eq!(prog.code[2], 0);
    let (val, _) = eval_predicate(&prog, &mb, 0, 1);
    assert_eq!(val, 5);

    // col1 (logical 1 = pk) → LOAD_PK_INT
    let code = vec![EXPR_LOAD_COL_INT, 0, 1, 0];
    let mut prog = ExprProgram::new(code, 1, 0, vec![]);
    prog.resolve_column_indices(1);
    assert_eq!(prog.code[0], EXPR_LOAD_PK_INT);
    let (val, _) = eval_predicate(&prog, &mb, 0, 1);
    assert_eq!(val, 99);

    // col2 (logical 2, after pk) → physical 1
    let code = vec![EXPR_LOAD_COL_INT, 0, 2, 0];
    let mut prog = ExprProgram::new(code, 1, 0, vec![]);
    prog.resolve_column_indices(1);
    assert_eq!(prog.code[0], EXPR_LOAD_PAYLOAD_INT);
    assert_eq!(prog.code[2], 1);
    let (val, _) = eval_predicate(&prog, &mb, 0, 1);
    assert_eq!(val, 7);
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
    batch.extend_pk_lo(&1u64.to_le_bytes());
    batch.extend_pk_hi(&0u64.to_le_bytes());
    batch.extend_weight(&1i64.to_le_bytes());
    batch.extend_null_bmp(&0u64.to_le_bytes());
    batch.extend_col(0, &gs);
    batch.count = 1;
    batch
}

#[test]
fn test_string_prefix_ordering() {
    // Schema: pk=col0(U64), col1=STRING
    let schema = make_schema(2, 0, &[(8, 8), (11, 16)]);

    // (col_string, const_string, expected_lt)
    let cases: &[(&[u8], &[u8], bool)] = &[
        (b"abc",   b"abd",    true),  // differ at prefix byte 2
        (b"abd",   b"abc",    false), // reversed
        (b"a",     b"b",      true),  // single-char, differ at byte 0
        (b"abcd",  b"abce",   true),  // differ at prefix byte 3 (boundary)
        (b"abcde", b"abcdf",  true),  // differ at byte 4 (beyond prefix)
        (b"hello", b"world",  true),  // differ at byte 0
        (b"he",    b"hello",  true),  // col shorter, same available bytes
        (b"hello", b"he",     false), // col longer
        (b"",      b"a",      true),  // empty < non-empty
        (b"a",     b"",       false), // non-empty > empty
        (b"abcd",  b"abcd",   false), // equal 4-byte strings, not lt
        (b"abcde", b"abcde",  false), // equal 5-byte strings, not lt
        // LE-vs-BE regression: "ba" > "ac" because 'b'>'a' at byte 0.
        // With LE integer comparison, from_le("ba")=0x6162 < from_le("ac")=0x6361,
        // which would wrongly return true. BE comparison gives the correct false.
        (b"ba",    b"ac",     false),
        (b"ac",    b"ba",     true),
        (b"ba",    b"ab",     false), // byte 0 same first char but reversed positions
        (b"ab",    b"ba",     true),
    ];

    for &(col_s, const_s, expected_lt) in cases {
        let batch = make_single_string_batch(schema, col_s);
        let mb = batch.as_mem_batch();
        let code = vec![EXPR_STR_COL_LT_CONST, 0, 1, 0];
        let prog = ExprProgram::new(code, 1, 0, vec![const_s.to_vec()]);
        let (val, _) = eval_predicate(&prog, &mb, 0, 0);
        assert_eq!(
            val != 0, expected_lt,
            "STR_COL_LT_CONST: {:?} < {:?} expected {}, got {}",
            col_s, const_s, expected_lt, val != 0
        );
    }
}

#[test]
fn test_bool_and_or_three_valued_logic() {
    // Schema: pk(U64), col1(I64), col2(I64) — both nullable
    let schema = make_schema(3, 0, &[(8, 8), (9, 8), (9, 8)]);
    // null_word bits: bit 0 = col1 null, bit 1 = col2 null
    let batch = make_int_batch(&schema, &[
        (1, 1, 0, &[1, 0]), // row0: T, F
        (2, 1, 0, &[0, 1]), // row1: F, T
        (3, 1, 2, &[1, 0]), // row2: T, NULL
        (4, 1, 2, &[0, 0]), // row3: F, NULL
        (5, 1, 1, &[0, 1]), // row4: NULL, T
        (6, 1, 1, &[0, 0]), // row5: NULL, F
        (7, 1, 3, &[0, 0]), // row6: NULL, NULL
    ]);
    let mb = batch.as_mem_batch();

    let and_code = vec![
        EXPR_LOAD_COL_INT, 0, 1, 0,
        EXPR_LOAD_COL_INT, 1, 2, 0,
        EXPR_BOOL_AND, 2, 0, 1,
    ];
    let and_prog = ExprProgram::new(and_code, 3, 2, vec![]);

    let or_code = vec![
        EXPR_LOAD_COL_INT, 0, 1, 0,
        EXPR_LOAD_COL_INT, 1, 2, 0,
        EXPR_BOOL_OR, 2, 0, 1,
    ];
    let or_prog = ExprProgram::new(or_code, 3, 2, vec![]);

    // AND cases
    let (v, n) = eval_predicate(&and_prog, &mb, 0, 0);
    assert_eq!(v, 0, "T AND F = F"); assert!(!n);
    let (v, n) = eval_predicate(&and_prog, &mb, 1, 0);
    assert_eq!(v, 0, "F AND T = F"); assert!(!n);
    let (_, n) = eval_predicate(&and_prog, &mb, 2, 0);
    assert!(n, "T AND NULL = NULL");
    let (v, n) = eval_predicate(&and_prog, &mb, 3, 0);
    assert_eq!(v, 0, "F AND NULL = F"); assert!(!n, "F AND NULL must not be null (SQL 3VL)");
    let (_, n) = eval_predicate(&and_prog, &mb, 4, 0);
    assert!(n, "NULL AND T = NULL");
    let (v, n) = eval_predicate(&and_prog, &mb, 5, 0);
    assert_eq!(v, 0, "NULL AND F = F"); assert!(!n, "NULL AND F must not be null (SQL 3VL)");
    let (_, n) = eval_predicate(&and_prog, &mb, 6, 0);
    assert!(n, "NULL AND NULL = NULL");

    // OR cases
    let (v, n) = eval_predicate(&or_prog, &mb, 0, 0);
    assert_eq!(v, 1, "T OR F = T"); assert!(!n);
    let (v, n) = eval_predicate(&or_prog, &mb, 1, 0);
    assert_eq!(v, 1, "F OR T = T"); assert!(!n);
    let (v, n) = eval_predicate(&or_prog, &mb, 2, 0);
    assert_eq!(v, 1, "T OR NULL = T"); assert!(!n, "T OR NULL must not be null (SQL 3VL)");
    let (_, n) = eval_predicate(&or_prog, &mb, 3, 0);
    assert!(n, "F OR NULL = NULL");
    let (v, n) = eval_predicate(&or_prog, &mb, 4, 0);
    assert_eq!(v, 1, "NULL OR T = T"); assert!(!n, "NULL OR T must not be null (SQL 3VL)");
    let (_, n) = eval_predicate(&or_prog, &mb, 5, 0);
    assert!(n, "NULL OR F = NULL");
    let (_, n) = eval_predicate(&or_prog, &mb, 6, 0);
    assert!(n, "NULL OR NULL = NULL");
}

#[test]
fn test_string_prefix_le_ordering() {
    // Spot-check LE (≤) to cover the equality boundary
    let schema = make_schema(2, 0, &[(8, 8), (11, 16)]);

    let cases: &[(&[u8], &[u8], bool)] = &[
        (b"abc",  b"abc",  true),  // equal → le
        (b"abc",  b"abd",  true),  // less → le
        (b"abd",  b"abc",  false), // greater → not le
        (b"abcd", b"abcd", true),  // equal 4-byte (prefix boundary)
        (b"he",   b"he",   true),  // equal short
        // LE-vs-BE regression: "ba" > "ac", so "ba" <= "ac" must be false.
        (b"ba",   b"ac",   false),
        (b"ac",   b"ba",   true),
    ];

    for &(col_s, const_s, expected_le) in cases {
        let batch = make_single_string_batch(schema, col_s);
        let mb = batch.as_mem_batch();
        let code = vec![EXPR_STR_COL_LE_CONST, 0, 1, 0];
        let prog = ExprProgram::new(code, 1, 0, vec![const_s.to_vec()]);
        let (val, _) = eval_predicate(&prog, &mb, 0, 0);
        assert_eq!(
            val != 0, expected_le,
            "STR_COL_LE_CONST: {:?} <= {:?} expected {}, got {}",
            col_s, const_s, expected_le, val != 0
        );
    }
}
