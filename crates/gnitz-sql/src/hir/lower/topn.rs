//! The top-N shell. Its input opens through the spine, so a projection over a
//! relation read in place is not stored a second time beside the operator's index.

use super::super::{slot_of, slots_of, ColId, RelExpr, TopNKey};
use super::spine::{open, Top};
use super::{keyed_frame, CutMemo};
use crate::error::GnitzSqlError;
use crate::hir::chain::{EmitPieces, ViewChain};
use crate::validate::reject_float_keys;
use gnitz_core::Circuit;
use gnitz_wire::OrderKey;
use std::collections::HashSet;

/// Lower a `TopN` body to circuit pieces.
pub(super) fn lower_topn(
    chain: &mut ViewChain,
    memo: &mut CutMemo,
    rel: &RelExpr,
) -> Result<EmitPieces, GnitzSqlError> {
    let RelExpr::TopN { input, partition, order, limit, offset } = rel else {
        unreachable!("lower_topn receives a TopN");
    };

    let live: HashSet<ColId> = input.cols().iter().map(|c| c.id).collect();
    let spine = open(chain, memo, input, &live)?;
    let replicated = spine.replicated();
    let mut cb = Circuit::default();
    let (node, frame) = spine.emit(&mut cb, Top::Output, "ORDER BY … LIMIT input")?;
    let (in_schema, in_layout) = (&frame.schema, &frame.layout);

    let written: Vec<u32> = slots_of(in_layout, partition)?.into_iter().map(|c| c as u32).collect();
    // The normalized group names the same set as the written partition, so every
    // derivation below reads this one list.
    let (out_key, group) = in_schema.reduce_key(&written);
    reject_float_keys(group.iter().map(|&c| &in_schema.columns[c as usize]), "PARTITION BY")?;
    // The keys as identities, so each phase resolves them against the layout it
    // actually reads. The input's own key breaks ties, so two rows equal on the
    // written keys order by identity, as `ROW_NUMBER` numbers them — and so the
    // same rows are selected on every worker count.
    let mut key_ids: Vec<TopNKey> = order.clone();
    for &pk in &in_schema.pk_cols {
        let id = in_layout[pk as usize];
        if !partition.contains(&id) && !key_ids.iter().any(|k| k.col == id) {
            key_ids.push(TopNKey { col: id, desc: false, nulls_first: false });
        }
    }
    if key_ids.len() > gnitz_wire::MAX_ORDER_KEYS {
        return Err(GnitzSqlError::Unsupported(format!(
            "ORDER BY: more than {} keys, the input's key included",
            gnitz_wire::MAX_ORDER_KEYS
        )));
    }
    let wire_keys = |layout: &[ColId]| -> Result<Vec<OrderKey>, GnitzSqlError> {
        key_ids
            .iter()
            .map(|k| {
                Ok(OrderKey {
                    col: slot_of(layout, k.col)? as u16,
                    desc: k.desc,
                    nulls_first: k.nulls_first,
                })
            })
            .collect()
    };
    let keys = wire_keys(in_layout)?;

    let out = keyed_frame(
        &frame,
        out_key,
        &group,
        0..frame.schema.columns.len() as u32,
        Vec::new(),
        "ORDER BY … LIMIT output",
    )?;

    let node = if replicated {
        // Every worker holds the whole input: the local window is the window.
        cb.top_n_local(node, &group, &keys, *limit, *offset)
    } else if group.is_empty() {
        // Two-phase: each worker's local `limit + offset` slots, exchanged to
        // one worker and cut to the window there. The global slots lie inside
        // the union of the local ones, so the cut is exact.
        let local_limit = limit
            .checked_add(*offset)
            .ok_or_else(|| GnitzSqlError::Plan("ORDER BY … LIMIT: LIMIT plus OFFSET overflows".to_string()))?;
        let local = cb.top_n_local(node, &[], &keys, local_limit, 0);
        // The second phase reads the first's output, so its keys resolve against
        // that layout — which, the group set being empty here, is `out`'s.
        // Its one partition is the synthetic key the local phase led with.
        cb.top_n(local, &[0], &wire_keys(&out.layout)?, *limit, *offset)
    } else {
        cb.top_n(node, &group, &keys, *limit, *offset)
    };
    cb.sink(node);
    Ok(EmitPieces { circuit: cb, out })
}
