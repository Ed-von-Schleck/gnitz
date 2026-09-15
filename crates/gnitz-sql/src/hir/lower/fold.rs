//! HIR → fold lowering: the ad-hoc read's reduce — or `SELECT DISTINCT`, a fold
//! with no aggregate — as layout for a stateless `SyntheticFold` the client
//! finishes. It shares `lower::reduce`'s rules and builds no evaluator.

use super::super::physical::{self, Frame};
use super::super::{as_col, split_filter, ColId, GetSource, HirAgg, HirExpr, ProjEntry, RelExpr};
use super::{keyed_frame, resolve_reduce_specs, ReduceSpecs};
use crate::codec::project_schema::read_reply_shape;
use crate::error::GnitzSqlError;
use crate::ir::{BExpr, BoundExpr};
use crate::validate::reject_float_keys;
use gnitz_core::{ColumnDef, ReduceOutKey, Schema};
use gnitz_wire::{AggReadSpec, ComputeMap};
use std::sync::Arc;

/// Everything an ad-hoc grouped read needs, in layout terms: what the workers
/// fold, and what the client does with the partials. The counterpart of
/// `chain::EmitPieces` for the fold sink.
pub(crate) struct FoldPieces {
    /// The reduce input: the pre-map's output schema when there is one, else the
    /// source's. The schema `agg`'s columns index, and the one that names them
    /// (a pre-map column is a hidden `_preN`).
    pub(crate) reduce_schema: Arc<Schema>,
    /// What the workers fold. It carries no cardinality COUNT: nothing stateless
    /// gates on one.
    pub(crate) agg: AggReadSpec,
    /// The SyntheticFold partial-reply layout
    /// (`[_group_pk | group cols | agg partials]`) the workers emit and every
    /// `BoundExpr` below is written against.
    pub(crate) partial_schema: Schema,
    /// The fold sink's pre-map over the source schema, `None` when the reduce
    /// groups and aggregates source columns directly.
    pub(crate) pre: Option<ComputeMap>,
    /// HAVING over the raw reduce output; empty when the body has none or it
    /// folded to true.
    pub(crate) having: Vec<BoundExpr>,
    /// The finalize projection in SELECT order: an expression over
    /// `partial_schema` and the output column it lands in. An aggregate's
    /// finalize composite (AVG's divide, a nullable SUM's null gate) is *in* the
    /// expression, exactly as it is in the view path's finalize map — so the
    /// client finisher has no per-aggregate shape switch to keep in step.
    pub(crate) finalize: Vec<(BoundExpr, ColumnDef)>,
}

/// The ad-hoc source a bound fold body reads: its schema and the `ColId` at each
/// of its slots, taken off the `Get` rather than through `cols()`, which clones a
/// `ColumnDef` per column to hand back one id each.
fn adhoc_get(base: &RelExpr) -> Result<(&Arc<Schema>, Vec<ColId>), GnitzSqlError> {
    let RelExpr::Get { source: GetSource::AdHoc, schema, cols } = base else {
        return Err(GnitzSqlError::Internal(
            "an ad-hoc fold body does not read an ad-hoc source".into(),
        ));
    };
    Ok((schema, cols.iter().map(|c| c.id).collect()))
}

/// Lower a bound ad-hoc grouped or `SELECT DISTINCT` body to its fold pieces.
pub(crate) fn lower_fold(rel: &RelExpr) -> Result<FoldPieces, GnitzSqlError> {
    match rel {
        RelExpr::Distinct { input } => {
            let RelExpr::Project { input: base, items } = input.as_ref() else {
                return Err(GnitzSqlError::Internal(
                    "ad-hoc SELECT DISTINCT body is not a projection".into(),
                ));
            };
            // A float set-identity column breaks content-hash equality (IEEE-754) —
            // the same gate a DISTINCT view is lowered under.
            reject_float_keys(items.iter().map(|e| &e.out.def), "SELECT DISTINCT")?;
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
            fold(
                base,
                pre,
                &group_cols,
                &[],
                &[],
                &finalize,
                "SELECT DISTINCT over a computed column",
            )
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
            fold(
                base,
                pre,
                group_cols,
                aggs,
                having,
                items,
                "GROUP BY over a computed key or aggregate argument",
            )
        }
        _ => Err(GnitzSqlError::Internal(
            "ad-hoc grouped body is not a projection".into(),
        )),
    }
}

/// The fold of `group_cols` / `aggs` over `pre` (or the source itself) applied to
/// `base`, then `having` and `finalize` over the partial reply. `what` names a
/// pre-map whose schema is inadmissible.
fn fold(
    base: &RelExpr,
    pre: Option<&[ProjEntry]>,
    group_cols: &[ColId],
    aggs: &[HirAgg],
    having: &[HirExpr],
    finalize: &[ProjEntry],
    what: &str,
) -> Result<FoldPieces, GnitzSqlError> {
    let (source_schema, base_layout) = adhoc_get(base)?;
    // The pre-map, physicalized over the source — the same projection `lower_reduce`
    // fuses, so the reduce input's column order is identical on both paths. Only
    // its payload slots are written: the PK region rides through verbatim.
    let (reduce_in, map) = match pre {
        None => (
            Frame {
                layout: base_layout,
                schema: Arc::clone(source_schema),
            },
            None,
        ),
        Some(items) => {
            let p = physical::physicalize_projection(items, &base_layout, source_schema)?;
            let columns = Arc::unwrap_or_clone(p.out.schema).columns;
            let (schema, map) = read_reply_shape(&p.items, columns, source_schema, what)?;
            (
                Frame {
                    layout: p.out.layout,
                    schema: Arc::new(schema),
                },
                Some(map),
            )
        }
    };
    let ReduceSpecs { group, specs, cols } = resolve_reduce_specs(group_cols, aggs, &reduce_in.layout)?;
    // The partial reply layout, which every expression below resolves against: the
    // fixed `SyntheticFold` key, then every group column in written order.
    let partial = keyed_frame(
        &reduce_in,
        ReduceOutKey::SyntheticFold,
        &group,
        group.iter().copied(),
        cols,
        "aggregate SELECT partial-reply layout",
    )?;
    let having = physical::resolve_preds(having, &partial.layout)?;
    let finalize = finalize
        .iter()
        .map(|e| Ok((physical::resolve_refs(&e.expr, &partial.layout)?, e.out.def.clone())))
        .collect::<Result<Vec<_>, GnitzSqlError>>()?;
    Ok(FoldPieces {
        reduce_schema: reduce_in.schema,
        agg: AggReadSpec { group_cols: group, aggs: specs },
        partial_schema: Arc::unwrap_or_clone(partial.schema),
        pre: map,
        having,
        finalize,
    })
}
