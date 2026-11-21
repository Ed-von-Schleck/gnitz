//! The group-keyed shells: GROUP BY / aggregate, the reduce over its input; and the
//! top-N and window, the two operators over a partition kept in order. Each input
//! opens through the spine, so a projection over a relation read in place is not
//! stored a second time beside an ordered operator's index. The reduce layout they
//! and the ad-hoc fold share is `Frame::group_aggs` and `Frame::keyed_by`.

use super::super::physical::{self, Slot};
use super::super::{AggCol, ColId, ColSet, HirCol, OrderCol, OrderedOp, ProjEntry, RelExpr, reduce_reads};
use super::spine::{Top, peel};
use super::{EmitPieces, ViewChain};
use crate::agg::agg_col_def;
use crate::error::GnitzSqlError;
use gnitz_wire::{AggDescriptor, AggFunc as WireAggFunc, Circuit, ReduceKind};
use std::rc::Rc;

/// Lower a `Reduce` of `input` to circuit pieces, whose frame addresses the raw
/// reduce output, which HAVING reads. One row per group key, which the reduce
/// output is keyed on.
pub(super) fn lower_reduce(
    chain: &mut ViewChain,
    input: &Rc<RelExpr>,
    group_cols: &[ColId],
    aggs: &[AggCol],
    kind: ReduceKind,
) -> Result<EmitPieces, GnitzSqlError> {
    let live: ColSet = reduce_reads(group_cols, aggs).into_iter().collect();
    let mut cb = Circuit::default();
    // The spine's output is what the reduce groups and aggregates over, so the
    // group/argument positions, the strategy and the reduce-output layout all
    // resolve against it.
    let (mut node, mut frame) = peel(chain, input, &live).open(chain)?.emit(&mut cb, Top::Slots, None)?;
    // The exchange ahead of the reduce carries only the PK and the columns the
    // reduce reads. A projection is linear and every aggregate reads only `live`,
    // so the reduce's output is the same. An ungrouped reduce pre-aggregates ahead
    // of its exchange, and one grouped on the input's PK exchanges nothing: neither
    // relays its input rows.
    let pk = frame.schema.pk_cols();
    let group = frame.reduce_group(group_cols)?;
    let unread = (0..frame.layout.len())
        .filter(|&slot| !pk.contains(&(slot as u32)))
        .any(|slot| !frame.layout[slot].is_some_and(|id| live.contains(id)));
    if !group.is_empty() && group != pk && unread {
        let items: Vec<ProjEntry> = (frame.layout.iter().zip(frame.schema.columns()))
            .filter_map(|(id, def)| id.filter(|&id| live.contains(id)).map(|id| (id, def)))
            .map(|(id, def)| ProjEntry::passthrough(HirCol::new(id, def.clone())))
            .collect();
        let (map, pruned) = physical::projection_map(&items, &frame)?;
        node = map.map_or(node, |map| cb.map_expr(node, map));
        frame = pruned;
    }

    let mut agg = frame.group_aggs(group_cols, aggs)?;
    let mut cols: Vec<Slot> = aggs.iter().map(|c| c.col.slot()).collect();
    // The engine reads group existence off a COUNT(*); a hidden one if no
    // aggregate is one.
    if !agg.aggs.iter().any(|d| d.agg_op == WireAggFunc::Count) {
        let def = agg_col_def(WireAggFunc::Count, None, agg.group_cols.is_empty());
        agg.aggs.push(AggDescriptor::COUNT_STAR);
        cols.push((None, def));
    }
    let out = frame.keyed_by(&agg.group_cols, agg.group_cols.iter().copied(), cols)?;
    cb.reduce(node, agg, kind);
    Ok(EmitPieces { circuit: cb, out, pk_repeats: false })
}

/// Lower an `Ordered` node over `input` to circuit pieces.
pub(super) fn lower_ordered(
    chain: &mut ViewChain,
    input: &Rc<RelExpr>,
    partition: &[ColId],
    order: &[OrderCol],
    op: &OrderedOp,
) -> Result<EmitPieces, GnitzSqlError> {
    let live: ColSet = input.cols().iter().map(|c| c.id).collect();
    let mut cb = Circuit::default();
    let (node, frame) = peel(chain, input, &live)
        .open(chain)?
        .emit(&mut cb, Top::KeyFirst, None)?;
    let npk = frame.schema.pk_cols().len();
    debug_assert!(
        frame.schema.pk_cols().iter().copied().eq(0..npk as u32),
        "the input leads with its key, which the index carries as its tie-break"
    );

    let aggs: &[AggCol] = match op {
        OrderedOp::Window { aggs, .. } => aggs,
        OrderedOp::TopN(_) => &[],
    };
    let group = frame.group_aggs(partition, aggs)?;
    crate::tail::reject_order_key_count(order.len())?;
    let keys = frame.order_keys(order)?;

    // The columns the operator adds behind its input's.
    let tail = op.added_cols().map(HirCol::slot).collect();
    let out = frame.keyed_by(&group.group_cols, 0..frame.schema.columns().len() as u32, tail)?;
    // The operator, and whether a partition — the key — holds more than one row.
    let pk_repeats = match op {
        OrderedOp::TopN(slots) => {
            cb.top_n(node, &group.group_cols, &keys, *slots);
            slots.keep > 1
        }
        OrderedOp::Window { rank, dense_rank, .. } => {
            cb.window(node, group, &keys, (rank.is_some(), dense_rank.is_some()));
            true
        }
    };
    Ok(EmitPieces { circuit: cb, out, pk_repeats })
}
