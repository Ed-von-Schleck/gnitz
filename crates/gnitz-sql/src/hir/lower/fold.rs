//! HIR → fold lowering: the ad-hoc read's reduce — or `SELECT DISTINCT`, a fold
//! with no aggregate — as layout for a stateless fold the client finishes. It
//! shares `lower::reduce`'s rules, output key included, and builds no evaluator.
//! Its input is the pieces of the flat body [`super::read`] composes: the reduce, or
//! the DISTINCT's projection, directly over the relation read.

use super::super::physical::{self, Frame};
use super::super::{AggCol, ColId, HirExpr, OrderCol, ProjEntry};
use super::{ReduceSpecs, keyed_frame, resolve_reduce_specs};
use crate::error::GnitzSqlError;
use crate::ir::{BExpr, BoundExpr};
use gnitz_core::Schema;
use gnitz_wire::{ComputeMap, GroupAggs};
use std::sync::Arc;

/// Everything an ad-hoc grouped read needs, in layout terms: what the workers
/// fold, and what the client does with the partials. The counterpart of
/// `lower::EmitPieces` for the fold sink.
pub(crate) struct FoldPieces {
    /// The reduce input: the pre-map's output schema when there is one, else the
    /// source's. The schema `agg`'s columns index, and the one that names them
    /// (a pre-map column is a hidden `_preN`).
    pub(crate) reduce_schema: Arc<Schema>,
    /// What the workers fold. It carries no cardinality COUNT: nothing stateless
    /// gates on one.
    pub(crate) agg: GroupAggs,
    /// The fold sink's pre-map over the source schema, `None` when the reduce
    /// groups and aggregates source columns directly.
    pub(crate) pre: Option<ComputeMap>,
    /// HAVING over the workers' partial reply, laid out as a view's reduce output over
    /// the same group set — `finalize`'s source; empty when the body has none or it
    /// folded to true.
    pub(crate) having: Vec<BoundExpr>,
    /// The finalize projection over the partial reply: the rows reply a read of it
    /// would answer. An aggregate's finalize composite (AVG's divide, a SUM's null
    /// gate) is *in* its item, exactly as it is in the view path's finalize map — so
    /// the client finisher has no per-aggregate shape switch to keep in step.
    pub(crate) finalize: super::read::AdhocRows,
}

/// `SELECT DISTINCT items` over `src`. Bare items group the source columns where they lie; any
/// computed item makes the projection the pre-map and groups its columns.
pub(super) fn distinct_fold(
    src: Frame,
    items: &[ProjEntry],
    order: Vec<OrderCol>,
) -> Result<FoldPieces, GnitzSqlError> {
    let (pre, group_cols) = match items.iter().map(|e| e.expr.as_col()).collect::<Option<Vec<_>>>() {
        Some(ids) => (None, ids),
        None => (Some(items), items.iter().map(|e| e.out.id).collect()),
    };
    // Every item passes its own group column through: the reduce already evaluated a computed
    // one into that column.
    let finalize: Vec<ProjEntry> = items
        .iter()
        .zip(&group_cols)
        .map(|(e, &id)| ProjEntry {
            expr: BExpr::ColRef(id),
            out: e.out.clone(),
        })
        .collect();
    fold(src, pre, &group_cols, &[], &[], finalize, order)
}

/// The fold of `group_cols` / `aggs` over `pre` (or the source itself) applied to
/// `src`, then `having` and `finalize` over the partial reply, the result ordered by
/// `order` over `finalize`'s columns.
pub(super) fn fold(
    src: Frame,
    pre: Option<&[ProjEntry]>,
    group_cols: &[ColId],
    aggs: &[AggCol],
    having: &[HirExpr],
    finalize: Vec<ProjEntry>,
    order: Vec<OrderCol>,
) -> Result<FoldPieces, GnitzSqlError> {
    // The pre-map, physicalized over the source — the same projection `lower_reduce`
    // fuses, so the reduce input's column order is identical on both paths.
    let (map, reduce_in) = match pre {
        None => (None, src),
        Some(items) => physical::projection_map(items, &src)?,
    };
    let ReduceSpecs { agg, cols } = resolve_reduce_specs(group_cols, aggs, &reduce_in)?;
    let partial = keyed_frame(&reduce_in, &agg.group_cols, agg.group_cols.iter().copied(), cols)?;
    let having = partial.resolve_preds(having)?;
    let finalize = super::read::reply_rows(partial, finalize, order)?;
    Ok(FoldPieces {
        reduce_schema: reduce_in.schema,
        agg,
        pre: map,
        having,
        finalize,
    })
}
