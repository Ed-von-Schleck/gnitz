//! The top-N shell. Its input opens through the spine, so a projection over a
//! relation read in place is not stored a second time beside the operator's index.

use super::super::{ColId, RelExpr};
use super::spine::{open, Top};
use super::{keyed_frame, EmitPieces, ViewChain};
use crate::error::GnitzSqlError;
use gnitz_wire::Circuit;
use gnitz_wire::OrderKey;
use std::collections::HashSet;

/// Lower a `TopN` body to circuit pieces.
pub(super) fn lower_topn(chain: &mut ViewChain, rel: &RelExpr) -> Result<EmitPieces, GnitzSqlError> {
    let RelExpr::TopN { input, partition, order, limit, offset } = rel else {
        unreachable!("lower_topn receives a TopN");
    };

    let live: HashSet<ColId> = input.cols().iter().map(|c| c.id).collect();
    let spine = open(chain, input, &live)?;
    let mut cb = Circuit::default();
    let (node, frame) = spine.emit(&mut cb, Top::Output)?;
    debug_assert!(
        frame
            .schema
            .pk_cols
            .iter()
            .copied()
            .eq(0..frame.schema.pk_cols.len() as u32),
        "a top-N input leads with its key, which the index carries as its tie-break"
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

    let out = keyed_frame(&frame, &group, 0..frame.schema.columns.len() as u32, Vec::new())?;
    let node = cb.top_n(node, &group, &keys, *limit, *offset);
    // Keyed by the partition, which holds `limit` slots.
    Ok(EmitPieces {
        circuit: cb,
        top: node,
        out,
        pk_repeats: *limit > 1,
    })
}
