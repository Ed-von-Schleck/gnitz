//! HIR → fold lowering: the ad-hoc read path's half of the reduce, the sibling
//! of `lower::reduce`'s circuit half.
//!
//! Both consume the same bound `Project(Filter_having?(Reduce(Get)))` and derive
//! the same physical facts from the same shared rules — the pre-map
//! (`physicalize_pre_map` / `pre_map_blob`), the spec decomposition
//! (`push_agg_specs`), and the reduce-output layout. They differ only in the
//! sink: the view emits a circuit whose reduce strategy `ReduceShape` picks,
//! while an ad-hoc fold is **always** `SyntheticFold` (the stateless per-worker
//! hash-fold has one partial layout), carries no `should_emit` cardinality
//! COUNT, and hands its finishing to the client.
//!
//! `Distinct(Project(Get))` — an ad-hoc `SELECT DISTINCT` — folds here too, as a
//! reduce with zero aggregates whose group set is the projection.
//!
//! What leaves here is layout, not execution: positions, a wire program, and
//! `BoundExpr`s over the partial-reply schema. Compiling those to evaluators and
//! running them is `dml::select` / `exec::agg_finish`'s work, which keeps this
//! module (like every other `hir` one) free of the exec layer.

use super::super::{physical, slot_of};
use super::super::{split_filter, ColId, GetSource, RelExpr};
use super::{physicalize_pre_map, pre_map_blob, reduce_input_of, reduce_out_layout, resolve_reduce_specs, ReduceSpecs};
use crate::agg::{fold_partial_schema, AggSpec, ReduceLayout};
use crate::error::GnitzSqlError;
use crate::ir::BoundExpr;
use crate::validate::reject_float_keys;
use gnitz_core::{ColumnDef, Schema};
use gnitz_wire::ComputeMap;
use std::sync::Arc;

/// Everything an ad-hoc grouped read needs, in layout terms: what the workers
/// fold, and what the client does with the partials. The counterpart of
/// `chain::EmitPieces` for the fold sink.
pub(crate) struct FoldPieces {
    /// The reduce input: the pre-map's output schema when there is one, else the
    /// source's. The schema `group_positions` and `agg_specs[].col` index, and
    /// the one that names them (a pre-map column is a hidden `_preN`).
    pub(crate) reduce_schema: Arc<Schema>,
    /// Group columns as **reduce-input** positions. Empty = a global aggregate.
    pub(crate) group_positions: Vec<usize>,
    /// The physical reduce specs, pre-companion — no cardinality COUNT: that is
    /// a `should_emit` signal only a stateful reduce needs.
    pub(crate) agg_specs: Vec<AggSpec>,
    /// The SyntheticFold partial-reply layout
    /// (`[_group_pk | group cols | agg partials]`) the workers emit and every
    /// `BoundExpr` below is written against.
    pub(crate) partial_schema: Schema,
    /// The fold sink's pre-map over the source schema, `None` when the reduce
    /// groups and aggregates source columns directly.
    pub(crate) pre: Option<ComputeMap>,
    /// HAVING over the raw reduce output, `None` when the body has none or the
    /// binder folded it to a true constant.
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

/// Lower a bound ad-hoc grouped body to its fold pieces.
pub(crate) fn lower_fold(rel: &RelExpr) -> Result<FoldPieces, GnitzSqlError> {
    if let RelExpr::Distinct { input } = rel {
        return lower_distinct_fold(input);
    }
    let RelExpr::Project { input, items } = rel else {
        return Err(GnitzSqlError::Internal(
            "ad-hoc grouped body is not a projection".into(),
        ));
    };
    // HAVING is a Filter between the projection and the reduce; without one the
    // projection sits straight on the reduce.
    let (having_preds, reduce) = split_filter(input);
    let RelExpr::Reduce { input, pre, group_cols, aggs } = reduce.as_ref() else {
        return Err(GnitzSqlError::Internal("ad-hoc grouped body has no reduce".into()));
    };
    let (source_schema, base_layout) = adhoc_get(input)?;

    // The pre-map, physicalized over the source — the same call `lower_reduce`
    // makes, so the reduce input's column order is identical on both paths.
    let pre = physicalize_pre_map(pre, &base_layout, source_schema)?;
    let (reduce_schema, reduce_layout) = reduce_input_of(
        &pre,
        &base_layout,
        source_schema,
        "GROUP BY over a computed key or aggregate argument",
    )?;

    let ReduceSpecs {
        group_positions,
        specs: agg_specs,
        agg_starts,
    } = resolve_reduce_specs(group_cols, aggs, reduce_layout, &reduce_schema)?;

    // The partial reply layout, which every expression below resolves against.
    let ReduceLayout {
        schema: partial_schema,
        group_slots,
        agg_col_offset,
    } = fold_partial_schema(&reduce_schema, &group_positions, &agg_specs)?;

    let out_layout = reduce_out_layout(
        partial_schema.columns.len(),
        group_cols,
        &group_slots,
        aggs,
        &agg_starts,
        agg_col_offset,
    );

    let having = having_preds
        .iter()
        .map(|p| physical::resolve_refs(p, &out_layout))
        .collect::<Result<Vec<_>, _>>()?;
    let finalize = items
        .iter()
        .map(|e| Ok((physical::resolve_refs(&e.expr, &out_layout)?, e.out.def.clone())))
        .collect::<Result<Vec<_>, GnitzSqlError>>()?;

    let pre_map = pre_map_blob(&pre, source_schema)?;

    Ok(FoldPieces {
        reduce_schema,
        group_positions,
        agg_specs,
        partial_schema,
        pre: pre_map,
        having,
        finalize,
    })
}

/// Lower a bound ad-hoc `SELECT DISTINCT` body (`Distinct(Project(Get))`) to the
/// same fold pieces a grouped body produces: zero aggregates, one group column
/// per projected item in SELECT order, and an all-pass-through finalize.
fn lower_distinct_fold(input: &RelExpr) -> Result<FoldPieces, GnitzSqlError> {
    let RelExpr::Project { input: base, items } = input else {
        return Err(GnitzSqlError::Internal(
            "ad-hoc SELECT DISTINCT body is not a projection".into(),
        ));
    };
    // A float set-identity column breaks content-hash equality (IEEE-754) — the
    // same gate a DISTINCT view is lowered under.
    reject_float_keys(items.iter().map(|e| &e.out.def), "SELECT DISTINCT")?;

    let (source_schema, base_layout) = adhoc_get(base)?;
    let phys = physical::physicalize_projection(items, &base_layout, source_schema)?;
    // Each item's own slot — never a PK `place_pk_front` prepended, which the user
    // did not project and which is not part of the set identity.
    let slots: Vec<usize> = items
        .iter()
        .map(|e| slot_of(&phys.layout, e.out.id))
        .collect::<Result<_, _>>()?;
    // Identity elision: a projection of bare source columns groups them where they
    // already lie, so the common case carries no map program.
    let source_slots: Option<Vec<usize>> = slots.iter().map(|&s| phys.items[s].passthrough_src()).collect();
    let (pre, group_positions) = match source_slots {
        Some(cols) => (None, cols),
        None => (Some(phys), slots),
    };
    let (reduce_schema, _) = reduce_input_of(
        &pre,
        &base_layout,
        source_schema,
        "SELECT DISTINCT over a computed column",
    )?;

    let ReduceLayout { schema: partial_schema, group_slots, .. } =
        fold_partial_schema(&reduce_schema, &group_positions, &[])?;
    // Every item is a pass-through of its own group slot: the reduce already
    // evaluated a computed one into that slot.
    let finalize = group_slots
        .into_iter()
        .zip(items)
        .map(|(slot, e)| (BoundExpr::ColRef(slot), e.out.def.clone()))
        .collect();
    let pre_map = pre_map_blob(&pre, source_schema)?;

    Ok(FoldPieces {
        reduce_schema,
        group_positions,
        agg_specs: Vec::new(),
        partial_schema,
        pre: pre_map,
        having: Vec::new(),
        finalize,
    })
}
