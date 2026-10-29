//! The top-N and window shells: the two operators over a partition kept in
//! order. Their input opens through the spine, so a projection over a relation
//! read in place is not stored a second time beside the operator's index.

use super::super::{AggCol, ColId, HirCol, OrderCol, OrderedOp, RelExpr};
use super::spine::{open, Top};
use super::{keyed_frame, resolve_reduce_specs, EmitPieces, ViewChain};
use crate::error::GnitzSqlError;
use gnitz_wire::{Circuit, OrderKey};
use std::collections::HashSet;
use std::rc::Rc;

/// Lower an `Ordered` node over `input` to circuit pieces.
pub(super) fn lower_ordered(
    chain: &mut ViewChain,
    input: &Rc<RelExpr>,
    partition: &[ColId],
    order: &[OrderCol],
    op: &OrderedOp,
) -> Result<EmitPieces, GnitzSqlError> {
    let live: HashSet<ColId> = input.cols().iter().map(|c| c.id).collect();
    let spine = open(chain, input, &live)?;
    let mut cb = Circuit::default();
    let (node, frame) = spine.emit(&mut cb, Top::Output)?;
    debug_assert!(
        frame
            .schema
            .pk_cols()
            .iter()
            .copied()
            .eq(0..frame.schema.pk_cols().len() as u32),
        "the input leads with its key, which the index carries as its tie-break"
    );

    let aggs: &[AggCol] = match op {
        OrderedOp::Window { aggs, .. } => aggs,
        OrderedOp::TopN { .. } => &[],
    };
    let group = resolve_reduce_specs(partition, aggs, &frame)?.agg;
    crate::tail::reject_order_key_count(order.len())?;
    let keys = order
        .iter()
        .map(|k| {
            Ok(OrderKey {
                col: frame.slot(k.col)? as u16,
                desc: k.desc,
                nulls_first: k.nulls_first,
            })
        })
        .collect::<Result<Vec<_>, GnitzSqlError>>()?;

    // The columns the operator adds behind its input's.
    let tail = op.added_cols().map(HirCol::slot).collect();
    let out = keyed_frame(&frame, &group.group_cols, 0..frame.schema.columns().len() as u32, tail)?;
    // The operator, and whether a partition — the key — holds more than one row.
    let pk_repeats = match op {
        OrderedOp::TopN { limit, offset } => {
            cb.top_n(node, &group.group_cols, &keys, *limit, *offset);
            *limit > 1
        }
        OrderedOp::Window { rank, dense_rank, .. } => {
            cb.window(node, group, &keys, (rank.is_some(), dense_rank.is_some()));
            true
        }
    };
    Ok(EmitPieces { circuit: cb, out, pk_repeats })
}
