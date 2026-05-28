// `0 * MORSEL`, `1 * MORSEL`, `0 * NULL_WORDS_PER_REG` etc. are deliberate
// layout-documenting expressions making the register/word index explicit at
// each access site; collapsing them obscures which register is in use.
#![allow(clippy::erasing_op, clippy::identity_op)]

use super::super::batch::{EvalScratch, MORSEL, NULL_WORDS_PER_REG, eval_batch};
use super::super::program::ExprProgram;
use super::eval_predicate_via_batch as eval_predicate;
use crate::storage::Batch;
use crate::schema::{SchemaColumn, SchemaDescriptor, type_code};

fn make_schema_2col() -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 1),
        ],
        &[0],
    )
}

fn make_batch(schema: &SchemaDescriptor, rows: &[(u64, i64, i64)]) -> Batch {
    let mut b = Batch::with_schema(*schema, rows.len().max(1));
    for &(pk, w, val) in rows {
        b.extend_pk(pk as u128);
        b.extend_weight(&w.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &val.to_le_bytes());
        b.count += 1;
    }
    b
}

#[test]
fn test_scratch_reg3() {
    let mut s = EvalScratch::new();
    s.ensure_capacity(3, true, MORSEL);
    let m = 4;
    // Fill regs 0 and 1 with known values
    for i in 0..m { s.regs[0 * MORSEL + i] = (i as i64) + 1; }
    for i in 0..m { s.regs[1 * MORSEL + i] = (i as i64) * 10; }
    // Use reg3 to add reg0 + reg1 → reg2
    {
        let (ra, rb, rd) = s.reg3(0, 1, 2, m);
        for i in 0..m { rd[i] = ra[i] + rb[i]; }
    }
    assert_eq!(&s.regs[2 * MORSEL..2 * MORSEL + m], &[1, 12, 23, 34]);
}

#[test]
fn test_scratch_null_words3() {
    let mut s = EvalScratch::new();
    s.ensure_capacity(3, false, MORSEL);
    let words = 1;
    s.null_bits[0 * NULL_WORDS_PER_REG] = 0b1010;
    s.null_bits[1 * NULL_WORDS_PER_REG] = 0b0110;
    {
        let (na, nb, nd) = s.null_words3(0, 1, 2, words);
        nd[0] = na[0] | nb[0];
    }
    assert_eq!(s.null_bits[2 * NULL_WORDS_PER_REG], 0b1110);
}

#[test]
fn test_eval_batch_add() {
    use crate::expr;
    let schema = make_schema_2col();
    let batch = make_batch(&schema, &[(1, 1, 10), (2, 1, 20), (3, 1, 30)]);
    let mb = batch.as_mem_batch();

    // r0 = pk (LOAD_COL_INT col 0 → resolves to LOAD_PK_*_INT), r1 = col[1]
    // (LOAD_PAYLOAD_INT), r2 = r0 + r1
    let code = vec![
        expr::EXPR_LOAD_COL_INT, 0, 0, 0,
        expr::EXPR_LOAD_PAYLOAD_INT, 1, 0, 0,  // pi=0 (first payload)
        expr::EXPR_INT_ADD, 2, 0, 1,
    ];
    let mut prog = ExprProgram::new(code, 3, 2, vec![]);
    prog.resolve_column_indices(&schema);
    let mut scratch = EvalScratch::new();
    scratch.ensure_capacity(3, true, 3);
    eval_batch(&prog, &mb, 0, 3, &mut scratch);

    // row 0: pk=1, val=10, sum=11
    assert_eq!(scratch.regs[2 * MORSEL + 0], 11);
    // row 1: pk=2, val=20, sum=22
    assert_eq!(scratch.regs[2 * MORSEL + 1], 22);
    // row 2: pk=3, val=30, sum=33
    assert_eq!(scratch.regs[2 * MORSEL + 2], 33);
}

#[test]
fn test_eval_batch_matches_eval_predicate() {
    use crate::expr;

    let schema = make_schema_2col();
    // Build a predicate: col[1] > 15
    let code = vec![
        expr::EXPR_LOAD_PAYLOAD_INT, 0, 0, 0,  // r0 = payload[0]
        expr::EXPR_LOAD_CONST, 1, 15, 0,        // r1 = 15
        expr::EXPR_CMP_GT, 2, 0, 1,             // r2 = r0 > r1
    ];
    let mut prog = ExprProgram::new(code, 3, 2, vec![]);
    prog.resolve_column_indices(&schema);

    let rows: &[(u64, i64, i64)] = &[(1, 1, 5), (2, 1, 15), (3, 1, 25), (4, 1, 0)];
    let batch = make_batch(&schema, rows);
    let mb = batch.as_mem_batch();

    let mut scratch = EvalScratch::new();
    scratch.ensure_capacity(3, true, 4);
    eval_batch(&prog, &mb, 0, 4, &mut scratch);

    for (i, &(_, _, val)) in rows.iter().enumerate() {
        let batch_result = scratch.regs[2 * MORSEL + i] != 0;
        let (row_val, row_null) = eval_predicate(&prog, &mb, i);
        let row_result = !row_null && row_val != 0;
        assert_eq!(
            batch_result, row_result,
            "row {i}: val={val} batch={batch_result} row={row_result}",
        );
    }
}

/// Regression: `is_strictly_non_nullable` formerly ignored STR_COL_*_CONST,
/// so a `WHERE str_col = 'foo'` against a nullable string column would set
/// `no_nulls=true` on the batch path and let null rows leak through as
/// definite-true / definite-false results. Verify the batch path matches the
/// per-row interpreter row-for-row on mixed null/non-null inputs.
#[test]
fn test_str_col_eq_const_nullable_column_matches_per_row() {
    use crate::expr::{Plan, ScalarFuncKind, EXPR_STR_COL_EQ_CONST};

    // Schema: pk(U64) + nullable STRING.
    let schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::STRING, 1),
        ],
        &[0],
    );

    // 20 rows: alternating null/non-null, with the non-null rows alternating
    // between "foo" (matches the predicate) and "bar" (does not).
    let n = 20usize;
    let mut batch = Batch::with_schema(schema, n);
    batch.count = 0;
    for row in 0..n {
        batch.extend_pk(row as u128 + 1);
        batch.extend_weight(&1i64.to_le_bytes());
        let is_null = row % 2 == 0;
        let null_word: u64 = if is_null { 1 } else { 0 };  // bit 0 = col1
        batch.extend_null_bmp(&null_word.to_le_bytes());
        let s: &[u8] = if row % 4 == 1 { b"foo" } else { b"bar" };
        let mut gs = [0u8; 16];
        gs[0..4].copy_from_slice(&(s.len() as u32).to_le_bytes());
        gs[4..4 + s.len()].copy_from_slice(s);
        batch.extend_col(0, &gs);
        batch.count += 1;
    }
    let mb = batch.as_mem_batch();

    // Predicate: col1 = 'foo'. result_reg = 0.
    let code = vec![EXPR_STR_COL_EQ_CONST, 0, 1, 0];
    let prog = ExprProgram::new(code, 1, 0, vec![b"foo".to_vec()]);
    let plan = Plan::from_predicate(prog, &schema);
    let kind = ScalarFuncKind::Plan(plan);

    // Drive the (multi-morsel) batch path through run_filter and compare to
    // the m=1 wrapper.
    let mut passing = vec![false; n];
    kind.run_filter(&mb, n, |start, end| {
        passing[start..end].fill(true);
    });
    for (row, &batch_pass) in passing.iter().enumerate() {
        let row_pass = kind.evaluate_predicate(&mb, row);
        assert_eq!(
            batch_pass, row_pass,
            "row {row}: batch={batch_pass} per-row={row_pass}",
        );
        // Stronger invariant: a null column value can never satisfy `=`.
        let null_word = mb.get_null_word(row);
        if null_word & 1 != 0 {
            assert!(!batch_pass, "row {row} is null but batch said pass");
        }
    }
}

// ---------------------------------------------------------------------------
// Edge-case golden tests at m=1
//
// These pin the points where the m=1 path through eval_batch could most
// plausibly diverge from the historical per-row interpreter. They each set up
// a one-row input and assert against a manually computed expected value via
// the single-path `Plan::evaluate_predicate` wrapper or a direct eval_batch.
// ---------------------------------------------------------------------------

/// Build a 1-row batch with one nullable I64 column. `null` toggles the
/// payload-0 null bit; `val` is the stored 8-byte payload regardless.
fn make_int_row(schema: &SchemaDescriptor, val: i64, null: bool) -> Batch {
    let mut b = Batch::with_schema(*schema, 1);
    b.extend_pk(1u128);
    b.extend_weight(&1i64.to_le_bytes());
    let null_word: u64 = if null { 1 } else { 0 };
    b.extend_null_bmp(&null_word.to_le_bytes());
    b.extend_col(0, &val.to_le_bytes());
    b.count = 1;
    b
}

fn schema_pk_int_nullable() -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 1),
        ],
        &[0],
    )
}

#[test]
fn golden_load_payload_null_row() {
    use crate::expr::{Plan, ScalarFuncKind};
    let schema = schema_pk_int_nullable();
    let batch = make_int_row(&schema, 42, true);
    let mb = batch.as_mem_batch();

    // Predicate: col1 > 0. With col1 null, result must be null → predicate fails.
    let code = vec![
        crate::expr::EXPR_LOAD_COL_INT, 0, 1, 0,
        crate::expr::EXPR_LOAD_CONST, 1, 0, 0,
        crate::expr::EXPR_CMP_GT, 2, 0, 1,
    ];
    let prog = ExprProgram::new(code, 3, 2, vec![]);
    let kind = ScalarFuncKind::Plan(Plan::from_predicate(prog, &schema));
    assert!(!kind.evaluate_predicate(&mb, 0));
}

#[test]
fn golden_int_div_zero_divisor_single_row() {
    let schema = schema_pk_int_nullable();
    let batch = make_int_row(&schema, 10, false);
    let mb = batch.as_mem_batch();

    // r0 = col1 = 10, r1 = 0, r2 = r0 / r1 → null
    let code = vec![
        crate::expr::EXPR_LOAD_COL_INT, 0, 1, 0,
        crate::expr::EXPR_LOAD_CONST, 1, 0, 0,
        crate::expr::EXPR_INT_DIV, 2, 0, 1,
    ];
    let mut prog = ExprProgram::new(code, 3, 2, vec![]);
    prog.resolve_column_indices(&schema);

    let mut scratch = EvalScratch::new();
    scratch.ensure_capacity(3, false, 1);
    eval_batch(&prog, &mb, 0, 1, &mut scratch);
    let is_null = (scratch.null_bits[2 * NULL_WORDS_PER_REG] & 1) != 0;
    assert!(is_null, "INT_DIV by zero must produce NULL at m=1");
    // The zero-mask merge into the destination null word must leave high bits zero.
    assert_eq!(
        scratch.null_bits[2 * NULL_WORDS_PER_REG] & !1u64, 0,
        "high bits of dst null word must stay zero at m=1",
    );
}

#[test]
fn golden_float_div_zero_divisor_single_row() {
    let schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::F64, 1),
        ],
        &[0],
    );
    let mut b = Batch::with_schema(schema, 1);
    b.extend_pk(1u128);
    b.extend_weight(&1i64.to_le_bytes());
    b.extend_null_bmp(&0u64.to_le_bytes());
    // arbitrary non-zero F64 bits
    let bits = f64::to_bits(2.5) as i64;
    b.extend_col(0, &bits.to_le_bytes());
    b.count = 1;
    let mb = b.as_mem_batch();

    // r0 = col1 (f64), r1 = 0.0 bits, r2 = r0 / r1 → null
    let code = vec![
        crate::expr::EXPR_LOAD_COL_FLOAT, 0, 1, 0,
        crate::expr::EXPR_LOAD_CONST, 1, 0, 0,
        crate::expr::EXPR_FLOAT_DIV, 2, 0, 1,
    ];
    let mut prog = ExprProgram::new(code, 3, 2, vec![]);
    prog.resolve_column_indices(&schema);

    let mut scratch = EvalScratch::new();
    scratch.ensure_capacity(3, false, 1);
    eval_batch(&prog, &mb, 0, 1, &mut scratch);
    let is_null = (scratch.null_bits[2 * NULL_WORDS_PER_REG] & 1) != 0;
    assert!(is_null, "FLOAT_DIV by zero must produce NULL at m=1");
    assert_eq!(
        scratch.null_bits[2 * NULL_WORDS_PER_REG] & !1u64, 0,
        "high bits of dst null word must stay zero at m=1",
    );
}

/// Build a 1-row batch with two nullable I64 columns plus a u64 PK.
fn make_two_int_row(schema: &SchemaDescriptor, v1: i64, v2: i64, nulls: u64) -> Batch {
    let mut b = Batch::with_schema(*schema, 1);
    b.extend_pk(1u128);
    b.extend_weight(&1i64.to_le_bytes());
    b.extend_null_bmp(&nulls.to_le_bytes());
    b.extend_col(0, &v1.to_le_bytes());
    b.extend_col(1, &v2.to_le_bytes());
    b.count = 1;
    b
}

fn schema_pk_two_ints_nullable() -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 1),
            SchemaColumn::new(type_code::I64, 1),
        ],
        &[0],
    )
}

fn run_bool_combinator(
    schema: &SchemaDescriptor,
    batch: &Batch,
    op: i64,
) -> (i64, bool) {
    let code = vec![
        crate::expr::EXPR_LOAD_COL_INT, 0, 1, 0,
        crate::expr::EXPR_LOAD_COL_INT, 1, 2, 0,
        op, 2, 0, 1,
    ];
    let mut prog = ExprProgram::new(code, 3, 2, vec![]);
    prog.resolve_column_indices(schema);
    let mb = batch.as_mem_batch();
    let mut scratch = EvalScratch::new();
    scratch.ensure_capacity(3, false, 1);
    eval_batch(&prog, &mb, 0, 1, &mut scratch);
    let val = scratch.regs[2 * MORSEL];
    let is_null = (scratch.null_bits[2 * NULL_WORDS_PER_REG] & 1) != 0;
    // The 3VL whole-word path writes the full u64 for word 0; bits beyond
    // bit 0 must be zero at m=1.
    assert_eq!(
        scratch.null_bits[2 * NULL_WORDS_PER_REG] & !1u64, 0,
        "3VL whole-word path must leave high bits of dst null word zero at m=1",
    );
    (val, is_null)
}

#[test]
fn golden_bool_and_3vl_single_row() {
    let schema = schema_pk_two_ints_nullable();
    // TRUE AND NULL = NULL: col1=1, col2=null (bit 1 set in null word)
    let b = make_two_int_row(&schema, 1, 0, 1u64 << 1);
    let (_, n) = run_bool_combinator(&schema, &b, crate::expr::EXPR_BOOL_AND);
    assert!(n, "TRUE AND NULL must be NULL at m=1");

    // FALSE AND NULL = FALSE: col1=0, col2=null
    let b = make_two_int_row(&schema, 0, 0, 1u64 << 1);
    let (v, n) = run_bool_combinator(&schema, &b, crate::expr::EXPR_BOOL_AND);
    assert!(!n, "FALSE AND NULL must not be NULL at m=1");
    assert_eq!(v, 0, "FALSE AND NULL must be FALSE at m=1");
}

#[test]
fn golden_bool_or_3vl_single_row() {
    let schema = schema_pk_two_ints_nullable();
    // NULL OR TRUE = TRUE: col1=null (bit 0), col2=1
    let b = make_two_int_row(&schema, 0, 1, 1u64 << 0);
    let (v, n) = run_bool_combinator(&schema, &b, crate::expr::EXPR_BOOL_OR);
    assert!(!n, "NULL OR TRUE must not be NULL at m=1");
    assert_eq!(v, 1, "NULL OR TRUE must be TRUE at m=1");

    // NULL OR FALSE = NULL: col1=null, col2=0
    let b = make_two_int_row(&schema, 0, 0, 1u64 << 0);
    let (_, n) = run_bool_combinator(&schema, &b, crate::expr::EXPR_BOOL_OR);
    assert!(n, "NULL OR FALSE must be NULL at m=1");
}

#[test]
fn golden_int_neg_null_source_single_row() {
    let schema = schema_pk_int_nullable();
    // col1 null → INT_NEG result null. null_or1 at m=1.
    let batch = make_int_row(&schema, 0, true);
    let mb = batch.as_mem_batch();
    let code = vec![
        crate::expr::EXPR_LOAD_COL_INT, 0, 1, 0,
        crate::expr::EXPR_INT_NEG, 1, 0, 0,
    ];
    let mut prog = ExprProgram::new(code, 2, 1, vec![]);
    prog.resolve_column_indices(&schema);
    let mut scratch = EvalScratch::new();
    scratch.ensure_capacity(2, false, 1);
    eval_batch(&prog, &mb, 0, 1, &mut scratch);
    assert!(
        (scratch.null_bits[1 * NULL_WORDS_PER_REG] & 1) != 0,
        "INT_NEG of NULL must be NULL at m=1",
    );
}

#[test]
fn golden_bool_not_null_source_single_row() {
    let schema = schema_pk_int_nullable();
    let batch = make_int_row(&schema, 0, true);
    let mb = batch.as_mem_batch();
    let code = vec![
        crate::expr::EXPR_LOAD_COL_INT, 0, 1, 0,
        crate::expr::EXPR_BOOL_NOT, 1, 0, 0,
    ];
    let mut prog = ExprProgram::new(code, 2, 1, vec![]);
    prog.resolve_column_indices(&schema);
    let mut scratch = EvalScratch::new();
    scratch.ensure_capacity(2, false, 1);
    eval_batch(&prog, &mb, 0, 1, &mut scratch);
    assert!(
        (scratch.null_bits[1 * NULL_WORDS_PER_REG] & 1) != 0,
        "NOT NULL must be NULL at m=1",
    );
}

#[test]
fn golden_str_col_eq_const_null_row() {
    use crate::expr::{Plan, ScalarFuncKind, EXPR_STR_COL_EQ_CONST};
    let schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::STRING, 1),
        ],
        &[0],
    );
    let mut b = Batch::with_schema(schema, 1);
    b.extend_pk(1u128);
    b.extend_weight(&1i64.to_le_bytes());
    b.extend_null_bmp(&1u64.to_le_bytes()); // payload 0 (the string col) null
    let mut gs = [0u8; 16];
    gs[0..4].copy_from_slice(&3u32.to_le_bytes());
    gs[4..7].copy_from_slice(b"foo");
    b.extend_col(0, &gs);
    b.count = 1;
    let mb = b.as_mem_batch();

    let code = vec![EXPR_STR_COL_EQ_CONST, 0, 1, 0];
    let prog = ExprProgram::new(code, 1, 0, vec![b"foo".to_vec()]);
    let kind = ScalarFuncKind::Plan(Plan::from_predicate(prog, &schema));
    assert!(
        !kind.evaluate_predicate(&mb, 0),
        "STR_COL_EQ_CONST on NULL row must not pass",
    );
}

#[test]
fn golden_is_null_and_is_not_null_single_row() {
    use crate::expr::{Plan, ScalarFuncKind};
    let schema = schema_pk_int_nullable();

    // Null row: IS NULL → true, IS NOT NULL → false.
    let batch = make_int_row(&schema, 0, true);
    let mb = batch.as_mem_batch();
    let code_is_null = vec![crate::expr::EXPR_IS_NULL, 0, 1, 0];
    let kind = ScalarFuncKind::Plan(Plan::from_predicate(
        ExprProgram::new(code_is_null, 1, 0, vec![]),
        &schema,
    ));
    assert!(kind.evaluate_predicate(&mb, 0));
    let code_is_not_null = vec![crate::expr::EXPR_IS_NOT_NULL, 0, 1, 0];
    let kind = ScalarFuncKind::Plan(Plan::from_predicate(
        ExprProgram::new(code_is_not_null, 1, 0, vec![]),
        &schema,
    ));
    assert!(!kind.evaluate_predicate(&mb, 0));

    // Non-null row: opposite.
    let batch = make_int_row(&schema, 7, false);
    let mb = batch.as_mem_batch();
    let code_is_null = vec![crate::expr::EXPR_IS_NULL, 0, 1, 0];
    let kind = ScalarFuncKind::Plan(Plan::from_predicate(
        ExprProgram::new(code_is_null, 1, 0, vec![]),
        &schema,
    ));
    assert!(!kind.evaluate_predicate(&mb, 0));
    let code_is_not_null = vec![crate::expr::EXPR_IS_NOT_NULL, 0, 1, 0];
    let kind = ScalarFuncKind::Plan(Plan::from_predicate(
        ExprProgram::new(code_is_not_null, 1, 0, vec![]),
        &schema,
    ));
    assert!(kind.evaluate_predicate(&mb, 0));
}

// ---------------------------------------------------------------------------
// Bit-only / bool_bits vectorization tests
// ---------------------------------------------------------------------------

/// Schema with 3 nullable I64 columns + U64 PK.
fn schema_pk_three_ints_nullable() -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 1),
            SchemaColumn::new(type_code::I64, 1),
            SchemaColumn::new(type_code::I64, 1),
        ],
        &[0],
    )
}

/// Build a batch with `n` rows. For each row, the value of each of the three
/// I64 payload columns is `f(row, col_idx)`. The null bit for each column is
/// set when `null_pred(row, col_idx)` returns true.
fn build_three_col_batch(
    schema: &SchemaDescriptor,
    n: usize,
    f: impl Fn(usize, usize) -> i64,
    null_pred: impl Fn(usize, usize) -> bool,
) -> Batch {
    let mut b = Batch::with_schema(*schema, n.max(1));
    for row in 0..n {
        b.extend_pk((row + 1) as u128);
        b.extend_weight(&1i64.to_le_bytes());
        let mut null_word: u64 = 0;
        for col in 0..3 {
            if null_pred(row, col) {
                null_word |= 1u64 << col;
            }
        }
        b.extend_null_bmp(&null_word.to_le_bytes());
        for col in 0..3 {
            b.extend_col(col, &f(row, col).to_le_bytes());
        }
        b.count += 1;
    }
    b
}

/// Reference 3VL: returns (truthy, is_null) for `a AND b`.
fn ref_and(a: Option<bool>, b: Option<bool>) -> (bool, bool) {
    match (a, b) {
        (Some(false), _) | (_, Some(false)) => (false, false),
        (Some(true), Some(true)) => (true, false),
        _ => (false, true), // NULL
    }
}

/// Differential test: 3-clause AND over nullable columns. For every morsel
/// boundary in 1..=MORSEL+1, verify run_filter agrees with a per-row 3VL
/// reference computed from the column values.
#[test]
fn bit_only_three_and_chain_boundary_sweep() {
    use crate::expr::{Plan, ScalarFuncKind, EXPR_LOAD_COL_INT, EXPR_LOAD_CONST,
                       EXPR_CMP_GT, EXPR_BOOL_AND};

    let schema = schema_pk_three_ints_nullable();

    // (col0 > 1) AND (col1 > 1) AND (col2 > 1)
    let code = vec![
        EXPR_LOAD_COL_INT, 0, 1, 0,        // r0 = col0
        EXPR_LOAD_CONST,   1, 1, 0,        // r1 = 1
        EXPR_CMP_GT,       2, 0, 1,        // r2 = r0 > 1
        EXPR_LOAD_COL_INT, 3, 2, 0,        // r3 = col1
        EXPR_CMP_GT,       4, 3, 1,        // r4 = r3 > 1
        EXPR_BOOL_AND,     5, 2, 4,        // r5 = r2 AND r4
        EXPR_LOAD_COL_INT, 6, 3, 0,        // r6 = col2
        EXPR_CMP_GT,       7, 6, 1,        // r7 = r6 > 1
        EXPR_BOOL_AND,     8, 5, 7,        // r8 = r5 AND r7  (result_reg)
    ];

    // Test sizes that cover: m < 64, m = 64 exactly, m crossing 64, the full
    // MORSEL=256, and the multi-morsel case (MORSEL + 1).
    for &n in &[1, 7, 63, 64, 65, 127, 128, 255, 256, 257, 300] {
        let batch = build_three_col_batch(
            &schema, n,
            // value cycles 0..4 to mix matching/non-matching rows
            |row, col| ((row + col) as i64) % 4,
            // null every 5th row in col0, every 7th in col1, every 11th in col2
            |row, col| match col {
                0 => row % 5 == 0,
                1 => row % 7 == 0,
                _ => row % 11 == 0,
            },
        );
        let mb = batch.as_mem_batch();

        let plan = Plan::from_predicate(
            ExprProgram::new(code.clone(), 9, 8, vec![]),
            &schema,
        );
        let kind = ScalarFuncKind::Plan(plan);

        let mut passed = vec![false; n];
        kind.run_filter(&mb, n, |s, e| {
            passed[s..e].fill(true);
        });

        for (row, &got) in passed.iter().enumerate() {
            let v0 = (row as i64) % 4;
            let v1 = ((row + 1) as i64) % 4;
            let v2 = ((row + 2) as i64) % 4;
            let n0 = row % 5 == 0;
            let n1 = row % 7 == 0;
            let n2 = row % 11 == 0;
            let b0 = if n0 { None } else { Some(v0 > 1) };
            let b1 = if n1 { None } else { Some(v1 > 1) };
            let b2 = if n2 { None } else { Some(v2 > 1) };
            let (v01, n01) = ref_and(b0, b1);
            let combined = if n01 { None } else { Some(v01) };
            let (v_all, n_all) = ref_and(combined, b2);
            let expected = !n_all && v_all;
            assert_eq!(
                got, expected,
                "n={n} row={row} v0={v0}(null={n0}) v1={v1}(null={n1}) v2={v2}(null={n2}) \
                 batch={got} expected={expected}",
            );
        }
    }
}

/// `NOT` over `bool(col)` against every {TRUE, FALSE, NULL} input. Exercises
/// the bit_only filter path: result_reg is the NOT output, so run_filter
/// reads `bool_bits[result_reg]` and must honor 3VL (NOT NULL = NULL = fail).
#[test]
fn bit_only_not_3vl_truth_table() {
    use crate::expr::{Plan, ScalarFuncKind, EXPR_LOAD_COL_INT, EXPR_LOAD_CONST,
                       EXPR_CMP_NE, EXPR_BOOL_NOT};

    let schema = schema_pk_int_nullable();

    // (TRUE, FALSE, NULL) — col1 value, null bit
    let cases: &[(i64, bool, Option<bool>)] = &[
        (1, false, Some(true)),
        (0, false, Some(false)),
        (0, true,  None),
    ];

    for &(val, null, src_truthy) in cases {
        let batch = make_int_row(&schema, val, null);
        let mb = batch.as_mem_batch();

        // Filter: NOT(col1 != 0). result_reg = NOT result (bit_only eligible).
        let code = vec![
            EXPR_LOAD_COL_INT, 0, 1, 0,    // r0 = col1
            EXPR_LOAD_CONST,   1, 0, 0,    // r1 = 0
            EXPR_CMP_NE,       2, 0, 1,    // r2 = bool(col1)
            EXPR_BOOL_NOT,     3, 2, 0,    // r3 = NOT r2
        ];
        let plan = Plan::from_predicate(
            ExprProgram::new(code, 4, 3, vec![]),
            &schema,
        );
        let kind = ScalarFuncKind::Plan(plan);
        let mut passed = false;
        kind.run_filter(&mb, 1, |_, _| { passed = true; });
        // NOT TRUE=FALSE, NOT FALSE=TRUE, NOT NULL=NULL (filter fails on NULL)
        let expected = matches!(src_truthy, Some(false));
        assert_eq!(passed, expected,
            "NOT 3VL: src={src_truthy:?} val={val} null={null} expected={expected} got={passed}");
    }
}

/// Boolean register consumed by an arithmetic opcode forces it OUT of
/// bit_only. The AND result must still appear correctly in `regs` for the
/// downstream add.
#[test]
fn bit_only_demotion_when_bool_feeds_arithmetic() {
    use crate::expr::{EXPR_LOAD_COL_INT, EXPR_LOAD_CONST, EXPR_CMP_GT,
                       EXPR_BOOL_AND, EXPR_INT_ADD};

    let schema = schema_pk_two_ints_nullable();
    // col1=2, col2=3, no nulls. Both > 1, so AND = 1. Add 0 → result = 1.
    let batch = make_two_int_row(&schema, 2, 3, 0);
    let mb = batch.as_mem_batch();

    let code = vec![
        EXPR_LOAD_COL_INT, 0, 1, 0,    // r0 = col1
        EXPR_LOAD_CONST,   1, 1, 0,    // r1 = 1
        EXPR_CMP_GT,       2, 0, 1,    // r2 = col1 > 1
        EXPR_LOAD_COL_INT, 3, 2, 0,    // r3 = col2
        EXPR_CMP_GT,       4, 3, 1,    // r4 = col2 > 1
        EXPR_BOOL_AND,     5, 2, 4,    // r5 = r2 AND r4    (consumed by ADD → not bit_only)
        EXPR_LOAD_CONST,   6, 0, 0,    // r6 = 0
        EXPR_INT_ADD,      7, 5, 6,    // r7 = r5 + 0 = bool-as-int
    ];
    let mut prog = ExprProgram::new(code, 8, 7, vec![]);
    prog.resolve_column_indices(&schema);
    // r5 is bool-produced but consumed by INT_ADD (non-bool). Must be demoted.
    assert!(
        (prog.bit_only_mask & (1u64 << 5)) == 0,
        "bool reg fed into arithmetic must NOT be bit_only",
    );

    let mut scratch = EvalScratch::new();
    scratch.ensure_capacity(8, false, 1);
    eval_batch(&prog, &mb, 0, 1, &mut scratch);
    // r5 lives in regs as 0/1; r7 = r5 + 0 = 1.
    assert_eq!(scratch.regs[5 * MORSEL], 1, "AND result must land in regs when !bit_only");
    assert_eq!(scratch.regs[7 * MORSEL], 1, "downstream arithmetic reads bool as i64");
}

/// Classifier: every CMP and every AND in a pure conjunction is bit_only
/// (with `is_filter=true`); result_reg stays bit_only.
#[test]
fn classifier_pure_conjunction_filter() {
    use crate::expr::{EXPR_LOAD_COL_INT, EXPR_LOAD_CONST, EXPR_CMP_GT, EXPR_BOOL_AND};

    let schema = schema_pk_two_ints_nullable();
    let mut prog = ExprProgram::new(vec![
        EXPR_LOAD_COL_INT, 0, 1, 0,
        EXPR_LOAD_CONST,   1, 1, 0,
        EXPR_CMP_GT,       2, 0, 1,
        EXPR_LOAD_COL_INT, 3, 2, 0,
        EXPR_CMP_GT,       4, 3, 1,
        EXPR_BOOL_AND,     5, 2, 4,
    ], 6, 5, vec![]);
    prog.resolve_column_indices(&schema);
    let (bit_only, bool_input) = prog.classify_registers(true);
    // Bool producers: r2 (CMP_GT), r4 (CMP_GT), r5 (BOOL_AND).
    // Non-bool readers consume r0/r1/r3 (CMPs read them as i64), so those
    // never qualify for bit_only. r2/r4 are read only by BOOL_AND, and r5
    // (result_reg) stays bit_only under is_filter=true.
    let expected_bit_only = (1u64 << 2) | (1u64 << 4) | (1u64 << 5);
    assert_eq!(bit_only, expected_bit_only,
        "expected r2/r4/r5 bit_only; got mask {bit_only:#010b}");
    // r2 and r4 are read by BOOL_AND.
    assert_eq!(bool_input, (1 << 2) | (1 << 4));
}

/// Classifier: a filter whose `result_reg` is produced by LOAD_PAYLOAD_INT
/// (not a bool producer) must NOT be in `bit_only_mask`. The fast-path
/// must therefore fall back to the per-row scan over `regs`.
#[test]
fn classifier_filter_result_reg_non_bool_falls_back() {
    use crate::expr::{Plan, ScalarFuncKind, EXPR_LOAD_COL_INT};
    let schema = schema_pk_int_nullable();
    // Predicate: WHERE col1 (treat int as truthy).
    let code = vec![EXPR_LOAD_COL_INT, 0, 1, 0];
    let plan = Plan::from_predicate(
        ExprProgram::new(code, 1, 0, vec![]),
        &schema,
    );
    let kind = ScalarFuncKind::Plan(plan);

    // Build 4 rows: non-null 1, non-null 0, null, non-null -5.
    let mut b = Batch::with_schema(schema, 4);
    let rows: &[(i64, bool)] = &[(1, false), (0, false), (0, true), (-5, false)];
    for &(v, n) in rows {
        b.extend_pk(1u128);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&(if n { 1u64 } else { 0 }).to_le_bytes());
        b.extend_col(0, &v.to_le_bytes());
        b.count += 1;
    }
    let mb = b.as_mem_batch();

    let mut passed = vec![false; 4];
    kind.run_filter(&mb, 4, |s, e| { passed[s..e].fill(true); });
    // val=1 → pass, val=0 → fail, null → fail, val=-5 → pass.
    assert_eq!(passed, vec![true, false, false, true],
        "filter on non-bool result_reg must use scalar fallback");
}

/// All-null word: 64 consecutive null rows on each side of an AND. Both
/// nullable arms (no `bit_only` and `bit_only`) must yield null in every
/// row, matching the historical 3VL behavior.
#[test]
fn bit_only_all_null_word_and() {
    use crate::expr::{Plan, ScalarFuncKind, EXPR_LOAD_COL_INT, EXPR_LOAD_CONST,
                       EXPR_CMP_NE, EXPR_BOOL_AND};
    let schema = schema_pk_two_ints_nullable();

    let n = 64;
    let mut b = Batch::with_schema(schema, n);
    for _ in 0..n {
        b.extend_pk(1u128);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&0b11u64.to_le_bytes()); // both cols null
        b.extend_col(0, &0i64.to_le_bytes());
        b.extend_col(1, &0i64.to_le_bytes());
        b.count += 1;
    }
    let mb = b.as_mem_batch();

    let code = vec![
        EXPR_LOAD_COL_INT, 0, 1, 0,
        EXPR_LOAD_CONST,   1, 0, 0,
        EXPR_CMP_NE,       2, 0, 1,    // r2 = bool(col1)
        EXPR_LOAD_COL_INT, 3, 2, 0,
        EXPR_CMP_NE,       4, 3, 1,    // r4 = bool(col2)
        EXPR_BOOL_AND,     5, 2, 4,
    ];
    let plan = Plan::from_predicate(
        ExprProgram::new(code, 6, 5, vec![]),
        &schema,
    );
    let kind = ScalarFuncKind::Plan(plan);

    let mut passed = vec![false; n];
    kind.run_filter(&mb, n, |s, e| { passed[s..e].fill(true); });
    assert!(passed.iter().all(|&p| !p), "all-null AND must reject every row");
}

/// IS_NOT_NULL feeds into BOOL_AND. Tests the tail-mask in the filter
/// fast-path: IS_NOT_NULL writes `bool_bits[dst] = !null_word`, leaving 1s
/// above bit `m % 64` of the last word. Without the tail mask those phantom
/// bits become false-positive passing rows.
#[test]
fn bit_only_is_not_null_tail_mask() {
    use crate::expr::{Plan, ScalarFuncKind, EXPR_IS_NOT_NULL, EXPR_BOOL_AND};

    let schema = schema_pk_two_ints_nullable();
    // 65 rows — straddles the 64-bit word boundary so tail handling matters.
    let n = 65;
    let mut b = Batch::with_schema(schema, n);
    for row in 0..n {
        b.extend_pk((row + 1) as u128);
        b.extend_weight(&1i64.to_le_bytes());
        // null every other row in col1, every third in col2
        let mut nw = 0u64;
        if row % 2 == 0 { nw |= 1; }
        if row % 3 == 0 { nw |= 2; }
        b.extend_null_bmp(&nw.to_le_bytes());
        b.extend_col(0, &0i64.to_le_bytes());
        b.extend_col(1, &0i64.to_le_bytes());
        b.count += 1;
    }
    let mb = b.as_mem_batch();

    // WHERE col1 IS NOT NULL AND col2 IS NOT NULL
    let code = vec![
        EXPR_IS_NOT_NULL, 0, 1, 0,
        EXPR_IS_NOT_NULL, 1, 2, 0,
        EXPR_BOOL_AND,    2, 0, 1,
    ];
    let plan = Plan::from_predicate(
        ExprProgram::new(code, 3, 2, vec![]),
        &schema,
    );
    let kind = ScalarFuncKind::Plan(plan);

    let mut passed = vec![false; n];
    kind.run_filter(&mb, n, |s, e| { passed[s..e].fill(true); });
    for (row, &got) in passed.iter().enumerate() {
        let nn1 = row % 2 != 0;
        let nn2 = row % 3 != 0;
        let expected = nn1 && nn2;
        assert_eq!(
            got, expected,
            "row {row}: nn1={nn1} nn2={nn2} expected={expected} got={got}",
        );
    }
}
