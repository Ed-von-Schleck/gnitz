//! The top-N shell. Its input opens through the spine, so a projection over a
//! relation read in place is not stored a second time beside the operator's index.

use super::super::{slot_of, slots_of, ColId, RelExpr, TopNKey};
use super::spine::{open, Top};
use super::{keyed_frame, EmitPieces, ViewChain};
use crate::error::GnitzSqlError;
use crate::validate::reject_float_keys;
use gnitz_core::Circuit;
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
    let (in_schema, in_layout) = (&frame.schema, &frame.layout);

    let written: Vec<u32> = slots_of(in_layout, partition)?.into_iter().map(|c| c as u32).collect();
    // The normalized group names the same set as the written partition, so every
    // derivation below reads this one list.
    let group = in_schema.reduce_group(&written);
    reject_float_keys(group.iter().map(|&c| &in_schema.columns[c as usize]), "PARTITION BY")?;
    // The input's key breaks ties, as `ROW_NUMBER` numbers them, so which rows are
    // selected is a function of the data alone.
    let mut key_ids: Vec<TopNKey> = order.clone();
    for &pk in &in_schema.pk_cols {
        let id = in_layout[pk as usize];
        if !partition.contains(&id) && !key_ids.iter().any(|k| k.col == id) {
            key_ids.push(TopNKey { col: id, desc: false, nulls_first: false });
        }
    }
    if key_ids.len() > gnitz_wire::MAX_ORDER_KEYS {
        return Err(GnitzSqlError::Rejected(format!(
            "ORDER BY: more than {} keys, the input's key included",
            gnitz_wire::MAX_ORDER_KEYS
        )));
    }
    let keys = key_ids
        .iter()
        .map(|k| {
            Ok(OrderKey {
                col: slot_of(in_layout, k.col)? as u16,
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
