use super::batch::copy_batch_row;
use super::eval::eval_pred_row;
use crate::bind::Binder;
use crate::error::GnitzSqlError;
use crate::ir::BoundExpr;
use gnitz_core::{Schema, ZSetBatch};
use sqlparser::ast::Expr;
use std::sync::Arc;

pub(crate) fn bind_residuals(
    binder: &Binder<'_>,
    residual: &[&Expr],
    schema: &Schema,
) -> Result<Vec<BoundExpr>, GnitzSqlError> {
    residual.iter().map(|&e| binder.bind_expr(e, schema)).collect()
}

/// A successful seek/scan reply: `(schema, batch, lsn)` — the `Ok` shape of
/// `gnitz_core::ScanResult`.
type ScanReply = (Option<Arc<Schema>>, Option<ZSetBatch>, u64);

/// Bind a candidate's residual conjuncts and filter a successful seek result
/// through them — the shared success path of every WHERE-serving plan in
/// `execute_select` (PK seek, range scan, equality seek).
pub(crate) fn residual_filtered(
    binder: &Binder<'_>,
    schema: &Schema,
    (s, b, lsn): ScanReply,
    residual: &[&Expr],
) -> Result<ScanReply, GnitzSqlError> {
    let preds = bind_residuals(binder, residual, schema)?;
    let (s2, b2) = apply_residual_filter((s, b), &preds, schema)?;
    Ok((s2, b2, lsn))
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

fn apply_residual_filter(
    result: (Option<Arc<Schema>>, Option<ZSetBatch>),
    preds: &[BoundExpr],
    schema: &Schema,
) -> Result<(Option<Arc<Schema>>, Option<ZSetBatch>), GnitzSqlError> {
    if preds.is_empty() {
        return Ok(result);
    }
    let (schema_opt, batch_opt) = result;
    let batch = match batch_opt {
        None => return Ok((schema_opt, None)),
        Some(b) => b,
    };
    let actual_schema = schema_opt.as_deref().unwrap_or(schema);
    let mut new_batch = ZSetBatch::new(actual_schema);
    for i in 0..batch.pks.len() {
        if row_passes_residuals(preds, &batch, i, actual_schema)? {
            copy_batch_row(&batch, i, &mut new_batch, actual_schema);
        }
    }
    Ok((schema_opt, Some(new_batch)))
}
