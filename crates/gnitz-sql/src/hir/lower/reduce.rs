//! The GROUP BY / aggregate shell: the reduce's input through the spine, then the
//! reduce, HAVING and the finalize projection.

use super::super::{ColId, HirExpr, ProjEntry, RelExpr};
use super::spine::{open, Top};
use super::{emit_filter, keyed_frame, project_tail, resolve_reduce_specs, CutMemo, ReduceSpecs};
use crate::agg::agg_col_def;
use crate::error::GnitzSqlError;
use crate::hir::chain::{EmitPieces, ViewChain};
use crate::validate::reject_duplicate_column_names;
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
    live.extend(aggs.iter().filter_map(|a| a.arg));
    let spine = open(chain, memo, input, &live)?;
    // A chain-minted segment carries no descriptor, so it plans as partitioned; the
    // engine elides the exchange of a view whose sources are all replicated.
    let replicated = spine.replicated();
    let mut cb = Circuit::default();
    // The spine's output is what the reduce groups and aggregates over, so the
    // group/argument positions, the strategy and the reduce-output layout all
    // resolve against it.
    let (node, reduce_in) = spine.emit(&mut cb, Top::Slots, "GROUP BY input")?;

    let mut r = resolve_reduce_specs(group_cols, aggs, &reduce_in.layout)?;
    let (out_key, group) = reduce_in.schema.reduce_key(&r.group);
    let ungrouped = group.is_empty();
    // A stateful reduce gates group existence on a NULL-blind COUNT(*): a user one
    // (or a count over a NOT NULL argument) serves, else a hidden one.
    if !r.specs.iter().any(|d| d.agg_op == WireAggFunc::Count) {
        r.push(
            AggDescriptor::COUNT_STAR,
            ColId::NONE,
            agg_col_def(WireAggFunc::Count, None, ungrouped),
        );
    }
    let ReduceSpecs { specs, mut cols, .. } = r;
    // Per-worker partials combined: exact for linear aggregates, but not a float
    // SUM, whose addition reassociates by worker count.
    let two_phase = ungrouped
        && !replicated
        && specs
            .iter()
            .zip(&cols)
            .all(|(d, (_, c))| d.agg_op.is_linear() && !(d.agg_op == WireAggFunc::Sum && c.type_code.is_float()));
    let reduced = if two_phase {
        // Each worker folds a partial; V₀'s owner merges them and seeds the ground row.
        let partials = cb.reduce_multi_local(node, &[], &specs, false);
        let mut merge: Vec<AggDescriptor> = specs
            .iter()
            .zip(1..)
            .map(|(d, col_idx)| AggDescriptor { agg_op: d.agg_op.merge_func(), col_idx })
            .collect();
        merge.push(AggDescriptor::COUNT_STAR);
        cols.push((ColId::NONE, agg_col_def(WireAggFunc::Count, None, true)));
        cb.reduce_multi(partials, &[], &merge, true)
    } else if replicated {
        // Shard-free: every worker reduces its full local copy to the same aggregate.
        cb.reduce_multi_local(node, &group, &specs, ungrouped)
    } else {
        cb.reduce_multi(node, &group, &specs, ungrouped)
    };

    // HAVING over the raw reduce output, then the finalize projection.
    let having_frame = keyed_frame(
        &reduce_in,
        out_key,
        &group,
        group.iter().copied(),
        cols,
        "GROUP BY output",
    )?;
    let filtered = emit_filter(&mut cb, reduced, having_preds, &having_frame)?;
    let (node, out) = project_tail(&mut cb, filtered, items, &having_frame)?;
    reject_duplicate_column_names(out.schema.columns.iter(), "GROUP BY view")?;
    cb.sink(node);
    Ok(EmitPieces { circuit: cb, out })
}
