//! HIR → fold lowering: the ad-hoc read path's half of the reduce, the sibling
//! of `lower::reduce`'s circuit half.
//!
//! Both consume the same bound `Project(Filter_having?(Reduce(PreMap?(Get))))`
//! and derive the same physical facts from the same `crate::agg` primitives —
//! the pre-map (`physicalize_projection`), the spec decomposition
//! (`push_agg_specs`), and the reduce-output layout. They differ only in the
//! sink: the view emits a circuit whose reduce strategy `ReduceShape` picks,
//! while an ad-hoc fold is **always** `SyntheticFold` (the stateless per-worker
//! hash-fold has one partial layout), carries no `should_emit` cardinality
//! COUNT, and hands its finishing to the client.
//!
//! What leaves here is layout, not execution: positions, a wire program, and
//! `BoundExpr`s over the partial-reply schema. Compiling those to evaluators and
//! running them is `dml::select` / `exec::agg_finish`'s work, which keeps this
//! module (like every other `hir` one) free of the exec layer.

use super::super::{chain::schema_of, physical};
use super::super::{is_pre_map, ColId, RelExpr};
use super::{reduce_out_layout, resolve_reduce_specs, ReduceSpecs};
use crate::agg::{fold_partial_schema, group_col_reduce_pos, AggSpec};
use crate::codec::project_schema::{compile_projection_map, declared_out_cols};
use crate::error::GnitzSqlError;
use crate::ir::BoundExpr;
use gnitz_core::{ColumnDef, ReduceOutKey, Schema, MAX_COLUMNS};
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
    /// The fold sink's pre-map program over the source schema, empty when the
    /// reduce groups and aggregates source columns directly.
    pub(crate) pre_map: Vec<u8>,
    /// The reduce input's payload columns, parallel to `pre_map` (both empty or
    /// both not) — what the worker rebuilds the reduce-input schema from.
    pub(crate) pre_payload: Vec<(u8, bool)>,
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

/// Lower a bound ad-hoc grouped body to its fold pieces.
///
/// `source_schema` is the scanned relation's, and the `Get` at the bottom of
/// `rel` was minted over it, so its `ColId`s are that schema's columns in order.
pub(crate) fn lower_fold(rel: &RelExpr, source_schema: &Arc<Schema>) -> Result<FoldPieces, GnitzSqlError> {
    let RelExpr::Project { input, items } = rel else {
        return Err(GnitzSqlError::Internal(
            "ad-hoc grouped body is not a projection".into(),
        ));
    };
    // HAVING is a Filter between the projection and the reduce; without one the
    // projection sits straight on the reduce.
    let (having_preds, reduce) = match input.as_ref() {
        RelExpr::Filter { input, preds } => (preds.as_slice(), input.as_ref()),
        other => (&[][..], other),
    };
    let RelExpr::Reduce {
        input,
        group_cols,
        aggs,
    } = reduce
    else {
        return Err(GnitzSqlError::Internal("ad-hoc grouped body has no reduce".into()));
    };

    // The pre-map the bind inserted so the reduce could group by, or aggregate,
    // an expression. Below it is the `Get` this body was bound over.
    let (pre_items, base) = match input.as_ref() {
        RelExpr::Project { input: below, items } if is_pre_map(below, items) => (Some(items.as_slice()), below),
        _ => (None, input),
    };
    let base_layout: Vec<ColId> = base.cols().iter().map(|c| c.id).collect();

    // The pre-map, physicalized over the source — the same call `lower_reduce`
    // makes, so the reduce input's column order is identical on both paths.
    let pre = pre_items
        .map(|its| physical::physicalize_projection(its, &base_layout, source_schema))
        .transpose()?;
    let (reduce_schema, reduce_layout) = match &pre {
        Some(p) => (schema_of(&p.out_cols, p.pk_arity), &p.layout),
        None => (Arc::clone(source_schema), &base_layout),
    };
    // A pre-map is the source's columns *plus* the computed ones, so a source
    // already near the limit can push the reduce input past it. Gated here, where
    // the shape is chosen, so it reads as the feature limit it is; the worker
    // rejects the same width too, but only as a trust boundary on a frame no
    // planner should have sent.
    if reduce_schema.columns.len() > MAX_COLUMNS {
        return Err(GnitzSqlError::Unsupported(format!(
            "GROUP BY over a computed key or aggregate argument needs {} columns, \
             which exceeds the {MAX_COLUMNS}-column limit",
            reduce_schema.columns.len()
        )));
    }

    let ReduceSpecs {
        group_positions,
        specs: agg_specs,
        agg_starts,
    } = resolve_reduce_specs(group_cols, aggs, reduce_layout, &reduce_schema)?;

    // The partial reply layout, which every expression below resolves against.
    let partial_schema = fold_partial_schema(&reduce_schema, &group_positions, &agg_specs)?;

    // Each group column's slot in that layout, through the same key-region rule
    // the circuit lowering uses — the fold's out-key is always SyntheticFold, so
    // this is the `_group_pk` at slot 0 and the group columns after it.
    let group_slots: Vec<usize> = (0..group_cols.len())
        .map(|j| {
            group_col_reduce_pos(
                group_positions[j],
                ReduceOutKey::SyntheticFold,
                &reduce_schema,
                &group_positions,
            )
        })
        .collect();
    let out_layout = reduce_out_layout(
        partial_schema.columns.len(),
        group_cols,
        &group_slots,
        aggs,
        &agg_starts,
        1 + group_positions.len(),
    );

    let having = having_preds
        .iter()
        .map(|p| physical::resolve_refs(p, &out_layout))
        .collect::<Result<Vec<_>, _>>()?;
    let finalize = items
        .iter()
        .map(|e| Ok((physical::resolve_refs(&e.expr, &out_layout)?, e.out.def.clone())))
        .collect::<Result<Vec<_>, GnitzSqlError>>()?;

    // The pre-map's wire form. Only the payload slots are written — the PK region
    // rides through verbatim, as on every other map — so the program is compiled
    // over the source schema and the worker rebuilds the reduce input as the
    // source's PK columns plus these declarations.
    let (pre_map, pre_payload) = match &pre {
        Some(p) => (
            compile_projection_map(&p.items[p.pk_arity..], source_schema)?.to_blob_bytes(),
            declared_out_cols(&p.out_cols[p.pk_arity..]),
        ),
        None => (Vec::new(), Vec::new()),
    };

    Ok(FoldPieces {
        reduce_schema,
        group_positions,
        agg_specs,
        partial_schema,
        pre_map,
        pre_payload,
        having,
        finalize,
    })
}
