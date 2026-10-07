//! HIR → fold lowering: the ad-hoc read's reduce — or `SELECT DISTINCT`, a fold
//! with no aggregate — as layout for a stateless fold the client finishes. It
//! shares `lower::reduce`'s rules, output key included, and builds no evaluator.
//! Its input is the pieces of the flat body [`super::read`] composes: the reduce, or
//! the DISTINCT's projection, directly over the relation read.

use super::super::physical::{self, Frame};
use super::super::{as_col, AggCol, ColId, HirExpr, ProjEntry};
use super::{keyed_frame, resolve_reduce_specs, ReduceSpecs};
use crate::error::GnitzSqlError;
use crate::ir::{BExpr, BoundExpr};
use crate::project::{compute_map, payload_program};
use gnitz_core::Schema;
use gnitz_wire::ColumnDef;
use gnitz_wire::{AggReadSpec, ComputeMap};
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
    pub(crate) agg: AggReadSpec,
    /// The workers' partial reply, laid out as a view's reduce output over the
    /// same group set; every `BoundExpr` below is written against it.
    pub(crate) partial_schema: Arc<Schema>,
    /// The fold sink's pre-map over the source schema, `None` when the reduce
    /// groups and aggregates source columns directly.
    pub(crate) pre: Option<ComputeMap>,
    /// HAVING over the raw reduce output; empty when the body has none or it
    /// folded to true.
    pub(crate) having: Vec<BoundExpr>,
    /// The finalize projection in SELECT order: an expression over
    /// `partial_schema` and the output column it lands in. An aggregate's
    /// finalize composite (AVG's divide, a SUM's null gate) is *in* the
    /// expression, exactly as it is in the view path's finalize map — so the
    /// client finisher has no per-aggregate shape switch to keep in step.
    pub(crate) finalize: Vec<(BoundExpr, ColumnDef)>,
}

/// `SELECT DISTINCT items` over `src`. Bare items group the source columns where they lie; any
/// computed item makes the projection the pre-map and groups its columns.
pub(super) fn distinct_fold(src: Frame, items: &[ProjEntry]) -> Result<FoldPieces, GnitzSqlError> {
    let (pre, group_cols) = match items.iter().map(|e| as_col(&e.expr)).collect::<Option<Vec<_>>>() {
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
    fold(src, pre, &group_cols, &[], &[], &finalize)
}

/// The fold of `group_cols` / `aggs` over `pre` (or the source itself) applied to
/// `src`, then `having` and `finalize` over the partial reply.
pub(super) fn fold(
    src: Frame,
    pre: Option<&[ProjEntry]>,
    group_cols: &[ColId],
    aggs: &[AggCol],
    having: &[HirExpr],
    finalize: &[ProjEntry],
) -> Result<FoldPieces, GnitzSqlError> {
    // The pre-map, physicalized over the source — the same projection `lower_reduce`
    // fuses, so the reduce input's column order is identical on both paths.
    let (reduce_in, map) = match pre {
        None => (src, None),
        Some(items) => {
            let (items, out) = physical::physicalize_projection(items, &src)?;
            let map = compute_map(payload_program(&items, &out.schema, &src.schema)?, &out.schema);
            (out, Some(map))
        }
    };
    let ReduceSpecs { group, specs, cols } = resolve_reduce_specs(group_cols, aggs, &reduce_in)?;
    let partial = keyed_frame(&reduce_in, &group, group.iter().copied(), cols)?;
    let having = partial.resolve_preds(having)?;
    let finalize = finalize
        .iter()
        .map(|e| Ok((partial.resolve(&e.expr)?, e.out.def.clone())))
        .collect::<Result<Vec<_>, GnitzSqlError>>()?;
    Ok(FoldPieces {
        reduce_schema: reduce_in.schema,
        agg: AggReadSpec { group_cols: group, aggs: specs },
        partial_schema: partial.schema,
        pre: map,
        having,
        finalize,
    })
}
