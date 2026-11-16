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
use gnitz_wire::{ColumnDef, ComputeMap, GroupAggs};
use std::sync::Arc;

/// Everything an ad-hoc grouped read needs, in layout terms: what the workers
/// fold, and what the client does with the partials. The counterpart of
/// `lower::EmitPieces` for the fold sink.
pub(crate) struct FoldPieces {
    /// The reduce input: the source relabelled by a pre-map that only copies its
    /// columns, else the pre-map's output. The schema `agg`'s columns index, and the
    /// one that names them (a computed pre-map column is a hidden `_preN`).
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
    pub(crate) finalize: super::read::RowsReply,
}

/// `SELECT DISTINCT items` over `src`: the items are the pre-map, and the fold
/// groups every column it produces.
pub(super) fn distinct_fold(
    src: Frame,
    items: Vec<ProjEntry>,
    order: Vec<OrderCol>,
) -> Result<FoldPieces, GnitzSqlError> {
    let group_cols: Vec<ColId> = items.iter().map(|e| e.out.id).collect();
    let finalize = ProjEntry::passthroughs(items.iter().map(|e| e.out.clone()));
    fold(src, items, &group_cols, &[], &[], finalize, order)
}

/// The fold of `group_cols` / `aggs` over `pre` applied to
/// `src`, then `having` and `finalize` over the partial reply, the result ordered by
/// `order` over `finalize`'s columns.
pub(super) fn fold(
    src: Frame,
    pre: Vec<ProjEntry>,
    group_cols: &[ColId],
    aggs: &[AggCol],
    having: &[HirExpr],
    finalize: Vec<ProjEntry>,
    order: Vec<OrderCol>,
) -> Result<FoldPieces, GnitzSqlError> {
    // Pre-map entries equal in expression are one column: each later one is read
    // as the first, so a column copied twice still relabels the source. A copy
    // keeps its source column's definition; a computed column is a hidden `_preN`.
    let mut kept: Vec<ProjEntry> = Vec::with_capacity(pre.len());
    let mut same: Vec<(ColId, ColId)> = Vec::new();
    let mut computed = 0;
    for mut e in pre {
        if let Some(k) = kept.iter().find(|k| k.expr == e.expr) {
            same.push((e.out.id, k.out.id));
            continue;
        }
        e.out.def = match e.expr.as_col() {
            Some(id) => src.schema.columns()[src.slot(id)?].clone(),
            None => {
                let def = ColumnDef::new(format!("_pre{computed}"), e.out.def.ty, e.out.def.is_nullable).hidden();
                computed += 1;
                def
            }
        };
        kept.push(e);
    }
    let at = |id: ColId| same.iter().find(|(dup, _)| *dup == id).map_or(id, |&(_, k)| k);
    let read = |e: &HirExpr| e.rebuild(&mut |id| BExpr::ColRef(at(*id)));
    let mut groups: Vec<ColId> = Vec::with_capacity(group_cols.len());
    for g in group_cols.iter().map(|&g| at(g)) {
        if !groups.contains(&g) {
            groups.push(g);
        }
    }
    let aggs: Vec<AggCol> = (aggs.iter())
        .map(|c| AggCol { arg: c.arg.map(at), ..c.clone() })
        .collect();
    let having: Vec<HirExpr> = having.iter().map(read).collect();
    let finalize = (finalize.into_iter())
        .map(|it| ProjEntry { expr: read(&it.expr), out: it.out })
        .collect();
    // The pre-map over the source, under the spine's rule: one copying distinct source
    // columns relabels the source, which the reduce then reads in place; anything else
    // is a map.
    let (map, reduce_in) = match src.relabeled(&kept)? {
        Some(relabeled) => (None, relabeled),
        None => physical::projection_map(&kept, &src)?,
    };
    let ReduceSpecs { agg, cols } = resolve_reduce_specs(&groups, &aggs, &reduce_in)?;
    let partial = keyed_frame(&reduce_in, &agg.group_cols, agg.group_cols.iter().copied(), cols)?;
    let having = partial.resolve_preds(&having)?;
    let finalize = super::read::reply_rows(partial, finalize, order)?;
    Ok(FoldPieces {
        reduce_schema: reduce_in.schema,
        agg,
        pre: map,
        having,
        finalize,
    })
}
