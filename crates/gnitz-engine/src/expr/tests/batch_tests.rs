// `0 * MORSEL`, `1 * MORSEL`, `0 * NULL_WORDS_PER_REG` etc. are deliberate
// layout-documenting expressions making the register/word index explicit at
// each access site; collapsing them obscures which register is in use.
#![allow(clippy::erasing_op, clippy::identity_op)]

use super::super::batch::{EvalScratch, MORSEL, NULL_WORDS_PER_REG, eval_batch};
use super::super::program::{ExprProgram, eval_predicate};
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

    // r0 = pk (LOAD_PK_INT), r1 = col[1] (LOAD_PAYLOAD_INT), r2 = r0 + r1
    let code = vec![
        expr::EXPR_LOAD_PK_INT, 0, 0, 0,
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

    // n=20 ≥ 16 ⇒ batch path engaged.
    let bits = kind.filter_batch_bits(&mb, n).expect("batch path must engage");
    for row in 0..n {
        let batch_pass = (bits[row / 64] >> (row % 64)) & 1 != 0;
        let row_pass = kind.evaluate_predicate(&mb, row);
        assert_eq!(
            batch_pass, row_pass,
            "row {row}: batch={batch_pass} per-row={row_pass}",
        );
        // Stronger invariant: a null column value can never satisfy `=`.
        let null_word = mb.get_null_word(row);
        if (null_word >> 0) & 1 != 0 {
            assert!(!batch_pass, "row {row} is null but batch said pass");
        }
    }
}
