//! HIR → fold lowering: the ad-hoc read's reduce — or `SELECT DISTINCT`, a fold
//! with no aggregate — as layout for a stateless fold the client finishes. It
//! shares `lower::reduce`'s rules, output key included, and builds no evaluator.
//! Its input is the flat body [`super::read`] composes: the reduce, or the
//! DISTINCT's projection, directly over the relation read.

use super::super::physical::{self, Frame};
use super::super::{as_col, split_filter, AggCol, ColId, HirExpr, ProjEntry, RelExpr};
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

/// The relation a bound fold body reads, as a frame.
fn source_frame(base: &RelExpr) -> Result<Frame, GnitzSqlError> {
    let RelExpr::Get { desc, cols } = base else {
        return Err(GnitzSqlError::Internal(
            "an ad-hoc fold body does not read a relation".into(),
        ));
    };
    Ok(Frame::scan(desc, cols))
}

/// Lower a flat ad-hoc grouped or `SELECT DISTINCT` body to its fold pieces.
pub(crate) fn lower_fold(rel: &RelExpr) -> Result<FoldPieces, GnitzSqlError> {
    match rel {
        RelExpr::Distinct { input } => {
            let RelExpr::Project { input: base, items } = input.as_ref() else {
                return Err(GnitzSqlError::Internal(
                    "ad-hoc SELECT DISTINCT body is not a projection".into(),
                ));
            };
            // Bare items group the source columns where they lie; any computed item
            // makes the projection the pre-map and groups its columns.
            let (pre, group_cols) = match items.iter().map(|e| as_col(&e.expr)).collect::<Option<Vec<_>>>() {
                Some(ids) => (None, ids),
                None => (Some(items.as_slice()), items.iter().map(|e| e.out.id).collect()),
            };
            // Every item passes its own group column through: the reduce already
            // evaluated a computed one into that column.
            let finalize: Vec<ProjEntry> = items
                .iter()
                .zip(&group_cols)
                .map(|(e, &id)| ProjEntry {
                    expr: BExpr::ColRef(id),
                    out: e.out.clone(),
                })
                .collect();
            fold(base, pre, &group_cols, &[], &[], &finalize)
        }
        RelExpr::Project { input, items } => {
            // HAVING is a Filter between the projection and the reduce.
            let (having, reduce) = split_filter(input);
            let RelExpr::Reduce { input, group_cols, aggs } = reduce.as_ref() else {
                return Err(GnitzSqlError::Internal("ad-hoc grouped body has no reduce".into()));
            };
            let (pre, base) = match input.as_ref() {
                RelExpr::Project { input, items } => (Some(items.as_slice()), input),
                _ => (None, input),
            };
            fold(base, pre, group_cols, aggs, having, items)
        }
        _ => Err(GnitzSqlError::Internal(
            "ad-hoc grouped body is not a projection".into(),
        )),
    }
}

/// The fold of `group_cols` / `aggs` over `pre` (or the source itself) applied to
/// `base`, then `having` and `finalize` over the partial reply.
fn fold(
    base: &RelExpr,
    pre: Option<&[ProjEntry]>,
    group_cols: &[ColId],
    aggs: &[AggCol],
    having: &[HirExpr],
    finalize: &[ProjEntry],
) -> Result<FoldPieces, GnitzSqlError> {
    let src = source_frame(base)?;
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
