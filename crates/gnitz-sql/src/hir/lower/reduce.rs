//! The GROUP BY / aggregate shell: the reduce's input through the spine, then the
//! reduce.

use super::super::{reduce_reads, AggCol, ColId, RelExpr};
use super::spine::{open, Top};
use super::{keyed_frame, resolve_reduce_specs, EmitPieces, ReduceSpecs, ViewChain};
use crate::agg::agg_col_def;
use crate::error::GnitzSqlError;
use gnitz_wire::{AggDescriptor, AggFunc as WireAggFunc, Circuit};
use std::collections::HashSet;
use std::rc::Rc;

/// Lower a `Reduce` of `input` to circuit pieces, whose frame addresses the raw
/// reduce output, which HAVING reads.
pub(super) fn lower_reduce(
    chain: &mut ViewChain,
    input: &Rc<RelExpr>,
    group_cols: &[ColId],
    aggs: &[AggCol],
) -> Result<EmitPieces, GnitzSqlError> {
    let live: HashSet<ColId> = reduce_reads(group_cols, aggs).into_iter().collect();
    let spine = open(chain, input, &live)?;
    let mut cb = Circuit::default();
    // The spine's output is what the reduce groups and aggregates over, so the
    // group/argument positions, the strategy and the reduce-output layout all
    // resolve against it.
    let (node, reduce_in) = spine.emit(&mut cb, Top::Slots)?;

    let mut r = resolve_reduce_specs(group_cols, aggs, &reduce_in)?;
    let ungrouped = r.group.is_empty();
    // The engine reads group existence off a COUNT(*); a hidden one if no
    // aggregate is one.
    if !r.specs.iter().any(|d| d.agg_op == WireAggFunc::Count) {
        r.push(
            AggDescriptor::COUNT_STAR,
            None,
            agg_col_def(WireAggFunc::Count, None, ungrouped),
        );
    }
    let ReduceSpecs { group, specs, cols } = r;
    cb.reduce(node, &group, &specs);
    let out = keyed_frame(&reduce_in, &group, group.iter().copied(), cols)?;
    // One row per group key, which the reduce output is keyed on.
    Ok(EmitPieces { circuit: cb, out, pk_repeats: false })
}
