//! The top-N emission shell. The node's input is read in place with its WHERE
//! and projection fused into the circuit (a filter, a map) when it is a
//! projection over one relation — the index the operator keeps already holds
//! every row, so materializing the projection as its own segment would store
//! them twice — and cut to a hidden segment otherwise.
//!
//! The output is the reduce's key region for the partition set, then every
//! input column — the physical `PK-front` convention every view has.

use super::super::{slot_of, ColId, RelExpr, TopNKey};
use super::{cut_segment, resolve_in_place, CutMemo, SegInput};
use crate::agg::group_pk_def;
use crate::error::GnitzSqlError;
use crate::hir::chain::{schema_of, EmitPieces, PkArity, ViewChain};
use crate::hir::split_filter;
use crate::validate::reject_float_keys;
use gnitz_core::{CircuitBuilder, ColumnDef, NodeId, ReduceOutKey, Schema};
use gnitz_wire::OrderKey;
use std::collections::HashSet;
use std::sync::Arc;

/// The top-N output for an input of `schema` / `layout` partitioned by `group`
/// under `out_key`: its column defs, PK arity, and the `ColId` at each slot — a
/// synthetic key's is [`ColId::NONE`]. Built from the same two shared rules the
/// engine derives its schema from, so the declared and emitted layouts agree
/// rather than mirror.
fn top_n_output(
    schema: &Schema,
    layout: &[ColId],
    out_key: ReduceOutKey,
    group: &[usize],
) -> (Vec<ColumnDef>, PkArity, Vec<ColId>) {
    let group_u32: Vec<u32> = group.iter().map(|&c| c as u32).collect();
    let mut cols = Vec::new();
    let mut ids = Vec::new();
    match out_key.key_region(&schema.pk_cols, &group_u32) {
        None => {
            cols.push(group_pk_def());
            ids.push(ColId::NONE);
        }
        Some(keys) => {
            for &c in keys {
                cols.push(schema.columns[c as usize].clone());
                ids.push(layout[c as usize]);
            }
        }
    }
    let pk = cols.len();
    for c in out_key.carried_columns(&schema.pk_cols, &group_u32, schema.columns.len() as u32) {
        cols.push(schema.columns[c as usize].clone());
        ids.push(layout[c as usize]);
    }
    (cols, pk, ids)
}

/// Lower a `TopN` body to circuit pieces plus its output `ColId` layout.
pub(crate) fn lower_topn(
    chain: &mut ViewChain,
    memo: &mut CutMemo,
    rel: &RelExpr,
) -> Result<(EmitPieces, Vec<ColId>), GnitzSqlError> {
    let RelExpr::TopN { input, partition, order, limit, offset } = rel else {
        unreachable!("lower_topn receives a TopN");
    };

    let fused = match input.as_ref() {
        RelExpr::Project { input: pin, items } => {
            let (fpreds, source) = split_filter(pin);
            resolve_in_place(chain, memo, source)?.map(|seg| (seg, fpreds, items))
        }
        _ => None,
    };
    let (seg, mut cb, node, in_schema, in_layout): (SegInput, CircuitBuilder, NodeId, Arc<Schema>, Vec<ColId>) =
        match fused {
            Some((seg, fpreds, items)) => {
                let mut cb = CircuitBuilder::new(seg.tid);
                let (node, proj) = super::linear::emit_linear_into(&mut cb, &seg, fpreds, items)?;
                let schema = schema_of(&proj.out_cols, proj.pk_arity, "ORDER BY … LIMIT input")?;
                (seg, cb, node, schema, proj.layout)
            }
            None => {
                let live: HashSet<ColId> = input.cols().iter().map(|c| c.id).collect();
                let seg = cut_segment(chain, memo, input, &live)?;
                let mut cb = CircuitBuilder::new(seg.tid);
                let node = cb.input_delta();
                let (schema, layout) = (Arc::clone(&seg.schema), seg.layout.clone());
                (seg, cb, node, schema, layout)
            }
        };
    let replicated = seg.desc.as_ref().is_some_and(|d| d.replicated);

    let group: Vec<usize> = partition
        .iter()
        .map(|&id| slot_of(&in_layout, id))
        .collect::<Result<_, _>>()?;
    reject_float_keys(group.iter().map(|&c| &in_schema.columns[c]), "PARTITION BY")?;
    let out_key = in_schema.reduce_out_key(&group);
    // `shard_group_cols` names the same set as the written partition, so every
    // derivation below reads this one normalized list.
    let group = crate::agg::shard_group_cols(out_key, &in_schema, &group);
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
    let keys = wire_keys(&in_layout)?;

    let (out_cols, pk_arity, out_layout) = top_n_output(&in_schema, &in_layout, out_key, &group);

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
        // that layout — which, the group set being empty here, is `out_layout`.
        // Its one partition is the synthetic key the local phase led with.
        cb.top_n(local, &[0], &wire_keys(&out_layout)?, *limit, *offset)
    } else {
        cb.top_n(node, &group, &keys, *limit, *offset)
    };
    cb.sink(node);
    Ok((EmitPieces { circuit: cb.build(), out_cols, pk_arity }, out_layout))
}
