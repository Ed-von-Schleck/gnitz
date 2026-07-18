use super::eval::eval_pred_row;
use crate::bind::bind_single_table;
use crate::error::GnitzSqlError;
use crate::ir::BoundExpr;
use gnitz_core::{Schema, ZSetBatch};
use sqlparser::ast::Expr;

pub(crate) fn bind_residuals(residual: &[&Expr], schema: &Schema) -> Result<Vec<BoundExpr>, GnitzSqlError> {
    residual.iter().map(|&e| bind_single_table(e, schema)).collect()
}

/// True when row `i` of `batch` satisfies every residual predicate. An empty
/// slice trivially passes; short-circuits on the first failing conjunct. Binding
/// each conjunct independently and ANDing the results is equivalent to evaluating
/// one `AND`-chain — `BinOp::And` returns `None` (→ `eval_pred_row` false) the
/// moment either operand is NULL, so a NULL conjunct excludes the row either way
/// — but needs no temporary `AND`-tree to be cloned and bound.
pub(crate) fn row_passes_residuals(
    preds: &[BoundExpr],
    batch: &ZSetBatch,
    i: usize,
    schema: &Schema,
) -> Result<bool, GnitzSqlError> {
    for p in preds {
        if !eval_pred_row(p, batch, i, schema)? {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Indices of the rows of `batch` that pass every residual predicate, in order.
/// An empty `preds` slice matches every row. Shared by the SELECT residual
/// filter and the UPDATE/DELETE WHERE-row resolution, which both need the
/// passing indices.
pub(crate) fn matching_indices(
    preds: &[BoundExpr],
    batch: &ZSetBatch,
    schema: &Schema,
) -> Result<Vec<usize>, GnitzSqlError> {
    let n = batch.pks.len();
    let mut matched = Vec::with_capacity(n);
    for i in 0..n {
        if row_passes_residuals(preds, batch, i, schema)? {
            matched.push(i);
        }
    }
    Ok(matched)
}
