use super::super::plan::{ScalarFuncKind, Plan};
use crate::schema::{SchemaColumn, SchemaDescriptor, type_code, MAX_COLUMNS};
use crate::storage::Batch;
use crate::expr;

fn make_schema(pk_index: u32, col_types: &[u8]) -> SchemaDescriptor {
    let mut columns = [SchemaColumn::new(0, 0); MAX_COLUMNS];
    for (i, &tc) in col_types.iter().enumerate() {
        let nullable = if i == pk_index as usize { 0 } else { 1 };
        columns[i] = SchemaColumn::new(tc, nullable);
    }
    SchemaDescriptor::new(&columns[..col_types.len()], &[pk_index])
}

fn make_int_batch(
    schema: &SchemaDescriptor,
    rows: &[(u64, i64, u64, &[i64])],
) -> Batch {
    let mut batch = Batch::with_schema(*schema, rows.len().max(1));
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
fn test_projection_batch() {
    let in_schema = make_schema(0, &[8, 9, 9]);
    let out_schema = make_schema(0, &[8, 9, 9]);
    let batch = make_int_batch(&in_schema, &[
        (1, 1, 0, &[10, 20]),
        (2, 1, 0, &[30, 40]),
    ]);

    let func = ScalarFuncKind::Plan(Plan::from_projection(
        &[2, 1],
        &[type_code::I64, type_code::I64],
        &in_schema,
        &out_schema,
    ));
    let result = func.evaluate_map_batch(&batch, &out_schema);
    assert_eq!(result.count, 2);

    let r0_col0 = i64::from_le_bytes(result.col_data(0)[0..8].try_into().unwrap());
    let r0_col1 = i64::from_le_bytes(result.col_data(1)[0..8].try_into().unwrap());
    assert_eq!(r0_col0, 20);
    assert_eq!(r0_col1, 10);
}

#[test]
fn test_map_copy_and_emit() {
    let in_schema = make_schema(0, &[8, 9, 9]);
    let out_schema = make_schema(0, &[8, 9, 9]);

    let batch = make_int_batch(&in_schema, &[
        (1, 1, 0, &[10, 20]),
    ]);

    let code = vec![
        expr::EXPR_COPY_COL, type_code::I64 as i64, 1, 0,
        expr::EXPR_LOAD_COL_INT, 0, 1, 0,
        expr::EXPR_LOAD_COL_INT, 1, 2, 0,
        expr::EXPR_INT_ADD, 2, 0, 1,
        expr::EXPR_EMIT, 0, 2, 1,
    ];
    let prog = crate::expr::ExprProgram::new(code, 3, 2, vec![]);

    let func = ScalarFuncKind::Plan(Plan::from_map(prog, &in_schema, &out_schema));
    let result = func.evaluate_map_batch(&batch, &out_schema);
    assert_eq!(result.count, 1);

    let v0 = i64::from_le_bytes(result.col_data(0)[0..8].try_into().unwrap());
    assert_eq!(v0, 10);
    let v1 = i64::from_le_bytes(result.col_data(1)[0..8].try_into().unwrap());
    assert_eq!(v1, 30);
}

#[test]
fn test_empty_batch() {
    let schema = make_schema(0, &[8, 9]);
    let batch = Batch::empty(1, 16);

    let func = ScalarFuncKind::Plan(Plan::from_projection(
        &[1], &[type_code::I64], &schema, &schema,
    ));
    let result = func.evaluate_map_batch(&batch, &schema);
    assert_eq!(result.count, 0);
}

#[test]
fn test_filter_batch_matches_per_row() {
    use crate::ops::op_filter;
    let schema = make_schema(0, &[8, 9]);

    // 20 rows so n >= THRESHOLD=16 and the batch evaluator path is taken.
    let batch = make_int_batch(&schema, &[
        (1,  1, 0, &[5]),   // fail
        (2,  1, 0, &[15]),  // pass
        (3,  1, 0, &[25]),  // pass
        (4,  1, 0, &[10]),  // fail (= not >)
        (5,  1, 0, &[20]),  // pass
        (6,  1, 0, &[3]),   // fail
        (7,  1, 0, &[30]),  // pass
        (8,  1, 0, &[10]),  // fail
        (9,  1, 0, &[11]),  // pass
        (10, 1, 0, &[0]),   // fail
        (11, 1, 0, &[50]),  // pass
        (12, 1, 0, &[9]),   // fail
        (13, 1, 0, &[12]),  // pass
        (14, 1, 0, &[8]),   // fail
        (15, 1, 0, &[100]), // pass
        (16, 1, 0, &[10]),  // fail
        (17, 1, 0, &[1]),   // fail
        (18, 1, 0, &[13]),  // pass
        (19, 1, 0, &[7]),   // fail
        (20, 1, 0, &[22]),  // pass
    ]);

    // Predicate: col[1] > 10
    let code = vec![
        expr::EXPR_LOAD_COL_INT, 0, 1, 0,
        expr::EXPR_LOAD_CONST, 1, 10, 0,
        expr::EXPR_CMP_GT, 2, 0, 1,
    ];
    let prog = crate::expr::ExprProgram::new(code, 3, 2, vec![]);
    let func = ScalarFuncKind::Plan(Plan::from_predicate(prog, &schema));

    let out = op_filter(&batch, &func, &schema);
    // pk=2(15), 3(25), 5(20), 7(30), 9(11), 11(50), 13(12), 15(100), 18(13), 20(22)
    assert_eq!(out.count, 10, "expected 10 rows with val > 10");
    assert_eq!(out.get_pk(0) as u64, 2);
    assert_eq!(out.get_pk(1) as u64, 3);
    assert_eq!(out.get_pk(2) as u64, 5);
    assert_eq!(out.get_pk(3) as u64, 7);
    assert_eq!(out.get_pk(4) as u64, 9);
    assert_eq!(out.get_pk(5) as u64, 11);
    assert_eq!(out.get_pk(6) as u64, 13);
    assert_eq!(out.get_pk(7) as u64, 15);
    assert_eq!(out.get_pk(8) as u64, 18);
    assert_eq!(out.get_pk(9) as u64, 20);
}
