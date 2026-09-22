//! The GROUP BY / aggregate shell: the reduce's input through the spine, then the
//! reduce, HAVING and the finalize projection.

use super::super::{ColId, HirCol, HirExpr, ProjEntry, RelExpr};
use super::spine::{open, Top};
use super::{emit_filter, keyed_frame, project_front, resolve_reduce_specs, CutMemo};
use crate::agg::agg_col_def;
use crate::error::GnitzSqlError;
use crate::hir::chain::{EmitPieces, ViewChain};
use gnitz_core::Circuit;
use gnitz_wire::{AggDescriptor, AggFunc as WireAggFunc};
use std::collections::HashSet;

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
    live.extend(aggs.iter().filter_map(|c| c.arg));
    let spine = open(chain, memo, input, &live)?;
    let mut cb = Circuit::default();
    // The spine's output is what the reduce groups and aggregates over, so the
    // group/argument positions, the strategy and the reduce-output layout all
    // resolve against it.
    let (node, reduce_in) = spine.emit(&mut cb, Top::Slots, "GROUP BY input")?;

    let mut r = resolve_reduce_specs(group_cols, aggs, &reduce_in.layout)?;
    let (out_key, group) = reduce_in.schema.reduce_key(&r.group);
    let ungrouped = group.is_empty();
    // The engine reads group existence off a COUNT(*); a hidden one if no
    // aggregate is one.
    if !r.specs.iter().any(|d| d.agg_op == WireAggFunc::Count) {
        r.push(
            AggDescriptor::COUNT_STAR,
            HirCol::new(ColId::NONE, agg_col_def(WireAggFunc::Count, None, ungrouped)),
        );
    }
    let reduced = cb.reduce_multi(node, &group, &r.specs, ungrouped);

    // HAVING over the raw reduce output, then the finalize projection.
    let having_frame = keyed_frame(
        &reduce_in,
        out_key,
        &group,
        group.iter().copied(),
        r.cols,
        "GROUP BY output",
    )?;
    let filtered = emit_filter(&mut cb, reduced, having_preds, &having_frame)?;
    let (node, out) = project_front(&mut cb, filtered, items, &having_frame)?;
    cb.sink(node);
    // One row per group key, which the reduce output is keyed on.
    Ok(EmitPieces { circuit: cb, out, pk_repeats: false })
}
