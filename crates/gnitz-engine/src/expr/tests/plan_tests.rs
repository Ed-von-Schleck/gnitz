use super::super::plan::ScalarFunc;
use crate::expr;
use crate::schema::{type_code, SchemaColumn, SchemaDescriptor, MAX_COLUMNS};
use crate::storage::Batch;

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
    let batch = make_int_batch(&in_schema, &[(1, 1, 0, &[10, 20]), (2, 1, 0, &[30, 40])]);

    let func = ScalarFunc::from_projection(&[2, 1], &[type_code::I64, type_code::I64], &in_schema, &out_schema);
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

    let batch = make_int_batch(&in_schema, &[(1, 1, 0, &[10, 20])]);

    let code = vec![
        expr::EXPR_COPY_COL,
        type_code::I64 as i64,
        1,
        0,
        expr::EXPR_LOAD_COL_INT,
        0,
        1,
        0,
        expr::EXPR_LOAD_COL_INT,
        1,
        2,
        0,
        expr::EXPR_INT_ADD,
        2,
        0,
        1,
        expr::EXPR_EMIT,
        0,
        2,
        1,
    ];
    let prog = crate::expr::ExprProgram::new(code, 3, 2, vec![]);

    let func = ScalarFunc::from_map(prog, &in_schema, &out_schema);
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

    let func = ScalarFunc::from_projection(&[1], &[type_code::I64], &schema, &schema);
    let result = func.evaluate_map_batch(&batch, &schema);
    assert_eq!(result.count, 0);
}
