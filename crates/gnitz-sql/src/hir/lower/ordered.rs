//! The top-N and window shells: the two operators over a partition kept in
//! order. Their input opens through the spine, so a projection over a relation
//! read in place is not stored a second time beside the operator's index.

use super::super::{ColId, HirCol, RelExpr};
use super::spine::{open, Top};
use super::{emit_filter, keyed_frame, project_front, resolve_reduce_specs, Demand, EmitPieces, ViewChain};
use crate::error::GnitzSqlError;
use gnitz_wire::Circuit;
use gnitz_wire::OrderKey;
use std::collections::HashSet;

/// Lower a `TopN` or `Window` body to circuit pieces, with the
/// `Project(Filter?(…))` `above` it, if any, in the same circuit.
pub(super) fn lower_ordered(
    chain: &mut ViewChain,
    rel: &RelExpr,
    above: Option<Demand<'_>>,
) -> Result<EmitPieces, GnitzSqlError> {
    let (RelExpr::TopN { input, partition, order, .. } | RelExpr::Window { input, partition, order, .. }) = rel else {
        unreachable!("lower_ordered receives a TopN or a Window");
    };

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

    let group = frame.reduce_group(partition)?;
    if order.len() > gnitz_wire::MAX_ORDER_KEYS {
        return Err(GnitzSqlError::Rejected(format!(
            "ORDER BY: more than {} keys",
            gnitz_wire::MAX_ORDER_KEYS
        )));
    }
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

    // The operator, the columns it adds behind its input's, and whether a
    // partition — the key — holds more than one row.
    let (node, tail, pk_repeats) = match rel {
        RelExpr::TopN { limit, offset, .. } => (cb.top_n(node, &group, &keys, *limit, *offset), Vec::new(), *limit > 1),
        RelExpr::Window { aggs, rank, dense_rank, .. } => {
            let aggs = resolve_reduce_specs(partition, aggs, &frame)?;
            let ranks = [rank, dense_rank].into_iter().flatten();
            let tail = ranks.map(HirCol::slot).chain(aggs.cols).collect();
            let has = (rank.is_some(), dense_rank.is_some());
            (cb.window(node, &group, &keys, &aggs.specs, has), tail, true)
        }
        _ => unreachable!("matched above"),
    };
    let mut out = keyed_frame(&frame, &group, 0..frame.schema.columns().len() as u32, tail)?;
    if let Some(above) = above {
        let filtered = emit_filter(&mut cb, node, above.where_preds, &out)?;
        out = project_front(&mut cb, filtered, above.items, &out)?.1;
    }
    Ok(EmitPieces { circuit: cb, out, pk_repeats })
}
