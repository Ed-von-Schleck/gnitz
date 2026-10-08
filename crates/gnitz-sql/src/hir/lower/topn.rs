//! The top-N shell. Its input opens through the spine, so a projection over a
//! relation read in place is not stored a second time beside the operator's index.

use super::super::physical::Rename;
use super::super::{ColId, HirCol, HirExpr, ProjEntry, RelExpr};
use super::spine::{open, Top};
use super::{emit_filter, keyed_frame, project_front, EmitPieces, ViewChain};
use crate::error::GnitzSqlError;
use gnitz_wire::Circuit;
use gnitz_wire::OrderKey;
use std::collections::HashSet;

/// What a body reads its top-N through: the names an `Alias` gives the top-N's
/// columns, if one does, then a filter and the projection over it.
pub(super) struct Above<'a> {
    pub(super) alias: Option<&'a [HirCol]>,
    pub(super) preds: &'a [HirExpr],
    pub(super) items: &'a [ProjEntry],
}

/// Lower a `TopN` body to circuit pieces, with the `Project(Filter?(Alias?(…)))`
/// `above` it, if any, in the same circuit.
pub(super) fn lower_topn(
    chain: &mut ViewChain,
    rel: &RelExpr,
    above: Option<Above<'_>>,
) -> Result<EmitPieces, GnitzSqlError> {
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
    let out = match above {
        None => out,
        Some(Above { alias, preds, items }) => {
            let out = match alias {
                Some(cols) => out.renamed(&Rename::alias(&rel.cols(), cols))?,
                None => out,
            };
            let filtered = emit_filter(&mut cb, node, preds, &out)?;
            project_front(&mut cb, filtered, items, &out)?.1
        }
    };
    // Keyed by the partition, which holds `limit` slots.
    Ok(EmitPieces { circuit: cb, out, pk_repeats: *limit > 1 })
}
