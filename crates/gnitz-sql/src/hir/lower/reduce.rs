//! The GROUP BY / aggregate shell: the reduce's input through the spine, then the
//! shared `crate::agg` reduce, HAVING and the finalize projection.

use super::super::{ColId, HirExpr, ProjEntry, RelExpr};
use super::spine::{open, Top};
use super::{emit_filter, project_tail, reduce_out_layout, resolve_reduce_specs, CutMemo, ReduceSpecs};
use crate::agg::{emit_reduce, ensure_cardinality_count, reduce_output_schema, ReduceShape};
use crate::error::GnitzSqlError;
use crate::hir::chain::{EmitPieces, ViewChain};
use crate::hir::physical::Frame;
use crate::validate::reject_duplicate_column_names;
use gnitz_core::CircuitBuilder;
use std::collections::HashSet;
use std::sync::Arc;

/// Lower a `Project(Filter_having?(Reduce(...)))` body's reduce to circuit pieces.
/// `items` is the finalize projection; `having_preds` is the HAVING filter over
/// the raw reduce output.
pub(super) fn lower_reduce(
    chain: &mut ViewChain,
    memo: &mut CutMemo,
    items: &[ProjEntry],
    having_preds: &[HirExpr],
    reduce: &RelExpr,
) -> Result<EmitPieces, GnitzSqlError> {
    let RelExpr::Reduce { input, group_cols, aggs } = reduce else {
        unreachable!("lower_reduce receives a Reduce");
    };

    let mut live: HashSet<ColId> = group_cols.iter().copied().collect();
    live.extend(aggs.iter().filter_map(|a| a.arg));
    let spine = open(chain, memo, input, &live)?;
    let replicated = spine.replicated();
    let mut cb = CircuitBuilder::new();
    // The spine's output is what the reduce groups and aggregates over, so the
    // group/argument positions, the strategy and the reduce-output layout all
    // resolve against it.
    let (node, reduce_in) = spine.emit(&mut cb, Top::Slots, "GROUP BY input")?;

    let ReduceSpecs { group_positions, mut specs, agg_starts } =
        resolve_reduce_specs(group_cols, aggs, &reduce_in.layout, &reduce_in.schema)?;
    // The emission-only cardinality COUNT is the circuit's alone: it is the
    // `should_emit` signal a stateful reduce gates group existence on, which the
    // stateless fold has no use for.
    ensure_cardinality_count(&reduce_in.schema.columns, &mut specs)?;

    // Reduce strategy (two-phase global / replicated / sharded). A chain-minted
    // segment carries no descriptor, and is never replicated anyway.
    let shape = ReduceShape::new(&reduce_in.schema, &group_positions, &specs, replicated);
    let out = reduce_output_schema(&shape)?;
    let reduced = emit_reduce(&mut cb, node, &shape);

    // HAVING over the raw reduce output, then the finalize projection.
    let layout = reduce_out_layout(&out, group_cols, aggs, &agg_starts);
    let having_frame = Frame { layout, schema: Arc::new(out.schema) };
    let filtered = emit_filter(&mut cb, reduced, having_preds, &having_frame)?;
    let (node, out) = project_tail(&mut cb, filtered, items, &having_frame)?;
    reject_duplicate_column_names(out.schema.columns.iter(), "GROUP BY view")?;
    cb.sink(node);
    Ok(EmitPieces { circuit: cb.build(), out })
}
