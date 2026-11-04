//! The GROUP BY / aggregate shell: the reduce's input through the spine, then the
//! reduce.

use super::super::physical::Frame;
use super::super::{reduce_reads, AggCol, ColId, RelExpr};
use super::super::{HirCol, ProjEntry};
use super::spine::{open, Top};
use super::{keyed_frame, project_front, resolve_reduce_specs, EmitPieces, ReduceSpecs, ViewChain};
use crate::agg::agg_col_def;
use crate::error::GnitzSqlError;
use gnitz_wire::{AggDescriptor, AggFunc as WireAggFunc, Circuit, NodeId};
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
    let (node, frame) = spine.emit(&mut cb, Top::Slots)?;
    let (node, reduce_in) = prune_to_reads(&mut cb, node, frame, group_cols, &live)?;

    let mut r = resolve_reduce_specs(group_cols, aggs, &reduce_in)?;
    let ungrouped = r.agg.group_cols.is_empty();
    // The engine reads group existence off a COUNT(*); a hidden one if no
    // aggregate is one.
    if !r.agg.aggs.iter().any(|d| d.agg_op == WireAggFunc::Count) {
        r.push(
            AggDescriptor::COUNT_STAR,
            None,
            agg_col_def(WireAggFunc::Count, None, ungrouped),
        );
    }
    let ReduceSpecs { agg, cols } = r;
    let out = keyed_frame(&reduce_in, &agg.group_cols, agg.group_cols.iter().copied(), cols)?;
    cb.reduce(node, agg);
    // One row per group key, which the reduce output is keyed on.
    Ok(EmitPieces { circuit: cb, out, pk_repeats: false })
}

/// `frame`'s rows cut to its PK and the columns in `live`, so the exchange ahead
/// of the reduce carries no column the reduce does not read. A projection is
/// linear and every aggregate reads only `live`, so the reduce's output is the
/// same; `frame` comes back unchanged when it holds nothing else.
fn prune_to_reads(
    cb: &mut Circuit,
    node: NodeId,
    frame: Frame,
    group_cols: &[ColId],
    live: &HashSet<ColId>,
) -> Result<(NodeId, Frame), GnitzSqlError> {
    let pk = frame.schema.pk_cols();
    // An ungrouped reduce pre-aggregates ahead of its exchange, and one grouped on
    // the input's PK exchanges nothing: neither relays its input rows.
    let group = frame.reduce_group(group_cols)?;
    if group.is_empty() || group == pk {
        return Ok((node, frame));
    }
    let unread = (0..frame.layout.len())
        .filter(|&slot| !pk.contains(&(slot as u32)))
        .any(|slot| !frame.layout[slot].is_some_and(|id| live.contains(&id)));
    if !unread {
        return Ok((node, frame));
    }
    let items: Vec<ProjEntry> = (frame.layout.iter().zip(frame.schema.columns()))
        .filter_map(|(id, def)| id.filter(|id| live.contains(id)).map(|id| (id, def)))
        .map(|(id, def)| ProjEntry::passthrough(HirCol::new(id, def.clone())))
        .collect();
    project_front(cb, node, &items, &frame)
}
