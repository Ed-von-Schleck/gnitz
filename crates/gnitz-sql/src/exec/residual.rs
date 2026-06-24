use super::batch::copy_batch_row;
use super::eval::eval_pred_row;
use crate::bind::bind_single_table;
use crate::error::GnitzSqlError;
use crate::ir::BoundExpr;
use gnitz_core::{Schema, ZSetBatch};
use sqlparser::ast::Expr;
use std::sync::Arc;

pub(crate) fn bind_residuals(residual: &[&Expr], schema: &Schema) -> Result<Vec<BoundExpr>, GnitzSqlError> {
    residual.iter().map(|&e| bind_single_table(e, schema)).collect()
}

/// A successful seek/scan reply: `(schema, batch, lsn)` — the `Ok` shape of
/// `gnitz_core::ScanResult`.
type ScanReply = (Option<Arc<Schema>>, Option<ZSetBatch>, u64);

/// Bind a candidate's residual conjuncts and filter a successful seek result
/// through them — the shared success path of every WHERE-serving plan in
/// `execute_select` (PK seek, range scan, equality seek).
pub(crate) fn residual_filtered(
    schema: &Schema,
    (s, b, lsn): ScanReply,
    residual: &[&Expr],
) -> Result<ScanReply, GnitzSqlError> {
    let preds = bind_residuals(residual, schema)?;
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
    let n = batch.pks.len();
    let matched = matching_indices(preds, &batch, actual_schema)?;
    if matched.len() == n {
        // Every row passed — return the fetched batch unmoved, no per-row clone.
        return Ok((schema_opt, Some(batch)));
    }
    let mut new_batch = ZSetBatch::new(actual_schema);
    for &i in &matched {
        copy_batch_row(&batch, i, &mut new_batch, actual_schema);
    }
    Ok((schema_opt, Some(new_batch)))
}
