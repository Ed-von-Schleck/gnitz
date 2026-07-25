//! The reduce (GROUP BY / aggregate) emission shell. Sources the group columns,
//! decomposed aggregate specs, HAVING filter, and finalize projection from the
//! HIR `Reduce` node, and drives the shared `crate::agg` primitives — the
//! strategy selection (`emit_reduce`), the reduce-output layout
//! (`reduce_output_schema`, `group_col_reduce_pos`), and the spec decomposition
//! (`push_agg_specs`) — which it shares with the ad-hoc SELECT / GROUP BY
//! aggregate path (`dml::group_by`, `exec::agg_finish`).

use super::super::physical;
use super::super::{slot_of, ColId, HirExpr, HirRef, ProjEntry, RelExpr};
use super::{cut_segment, emit_filter, extract_scan_bound, seginput_of_get, split_filter, CutMemo};
use crate::agg::{
    emit_reduce, ensure_cardinality_count, group_col_reduce_pos, push_agg_specs, reduce_output_schema, AggSpec,
    ReduceShape,
};
use crate::error::GnitzSqlError;
use crate::hir::chain::{EmitPieces, ViewChain};
use crate::ir::{BExpr, BoundExpr};
use crate::lower::{compile_bound_expr, compile_filter_program};
use crate::validate::reject_duplicate_column_names;
use gnitz_core::{CircuitBuilder, ColumnDef, ExprBuilder, GnitzClient, ReduceOutKey};
use std::collections::HashSet;
use std::rc::Rc;

/// Lower a `Project(Filter_having?(Reduce(...)))` body's reduce to circuit pieces
/// for `view_id`, returning the pieces plus the output `ColId` layout. `items` is
/// the finalize projection; `having_preds` is the HAVING filter over the raw
/// reduce output.
#[allow(clippy::too_many_arguments)]
pub(crate) fn lower_reduce(
    client: &mut GnitzClient,
    chain: &mut ViewChain,
    memo: &mut CutMemo,
    items: &[ProjEntry],
    having_preds: &[HirExpr],
    reduce: &RelExpr,
    view_id: u64,
) -> Result<(EmitPieces, Vec<ColId>), GnitzSqlError> {
    let RelExpr::Reduce {
        input,
        group_cols,
        aggs,
    } = reduce
    else {
        unreachable!("lower_reduce receives a Reduce");
    };

    // Resolve the reduce's input: inline a base/segment `Get` (with WHERE + scan
    // bound), or cut a combine input (`Filter?(Join)`) to a hidden segment.
    let (inner_where, inner_source) = split_filter(input);
    let (source, source_layout, bound, where_folded) = match seginput_of_get(inner_source) {
        Some(seg) => {
            // Base/segment Get: apply the WHERE + scan bound inline.
            let folded = physical::fold_preds(inner_where, &seg.layout)?;
            let bound = extract_scan_bound(client, &folded, seg.from_catalog, seg.tid, &seg.schema)?;
            let layout = seg.layout.clone();
            (seg, layout, bound, folded)
        }
        None => {
            // Cut the whole input (WHERE included) to a hidden segment; the reduce
            // reads it delta-bounded with no bound / no re-applied WHERE.
            let mut live: HashSet<ColId> = HashSet::new();
            live.extend(group_cols.iter().copied());
            live.extend(aggs.iter().filter_map(|a| a.arg));
            let seg = cut_segment(client, chain, memo, input, &live)?;
            let layout = seg.layout.clone();
            (seg, layout, None, None)
        }
    };
    let (source_tid, source_schema) = (source.tid, Rc::clone(&source.schema));

    let group_positions: Vec<usize> = group_cols
        .iter()
        .map(|g| slot_of(&source_layout, *g))
        .collect::<Result<_, _>>()?;

    // Decompose the aggregates into physical specs; record each agg's spec start.
    let mut specs: Vec<AggSpec> = Vec::new();
    let mut agg_starts: Vec<usize> = Vec::new();
    for a in aggs {
        agg_starts.push(specs.len());
        let arg_pos = a.arg.map(|id| slot_of(&source_layout, id)).transpose()?;
        push_agg_specs(a.func, arg_pos, &source_schema.columns, &mut specs)?;
    }
    ensure_cardinality_count(&source_schema.columns, &mut specs)?;

    // Reduce strategy (two-phase global / replicated / sharded). The replication
    // probe is a full `TABLE_TAB` scan, so it is gated on catalog provenance: a
    // chain-minted segment is not registered yet and is never replicated.
    let source_replicated = source.from_catalog && client.table_replicated(source_tid).map_err(GnitzSqlError::Exec)?;
    let shape = ReduceShape::new(&source_schema, &group_positions, &specs, source_replicated);
    let out_key = shape.out_key;
    let (reduce_schema, agg_col_offset) = reduce_output_schema(&shape);
    let pk_len = reduce_schema.pk_cols.len();

    // Circuit: input delta + optional WHERE.
    let mut cb = CircuitBuilder::new(view_id, source_tid);
    let inp = cb.input_delta_bounded(bound);
    let filtered = match where_folded {
        // Already folded above (the scan bound reads the same folded predicate).
        Some(f) => match compile_filter_program(&f, &source_schema.columns)? {
            Some(p) => cb.filter(inp, Some(p)),
            None => inp,
        },
        None => inp,
    };

    let reduced = emit_reduce(&mut cb, filtered, &shape);

    // Each group column's physical reduce-output slot — computed once here and
    // reused by the finalize loop below (`group_reduce_pos[j]`), rather than
    // re-deriving the same `group_col_reduce_pos` lookup a second time per item.
    let group_reduce_pos: Vec<usize> = (0..group_cols.len())
        .map(|j| group_col_reduce_pos(group_positions[j], out_key, &source_schema, &group_positions))
        .collect();

    // Reduce output layout: `ColId` at each physical slot. Slots with no logical
    // identity — the synthetic `_group_pk` and the trailing cardinality COUNT —
    // stay `ColId::NONE`, which nothing can reference.
    let mut reduce_layout: Vec<ColId> = vec![ColId::NONE; reduce_schema.columns.len()];
    for (j, &gid) in group_cols.iter().enumerate() {
        reduce_layout[group_reduce_pos[j]] = gid;
    }
    for (i, a) in aggs.iter().enumerate() {
        let slot = agg_col_offset + agg_starts[i];
        reduce_layout[slot] = a.out.id;
        if let Some(c) = &a.companion {
            reduce_layout[slot + 1] = c.id;
        }
    }

    // HAVING filter over the raw reduce output.
    let filtered_reduced = emit_filter(&mut cb, reduced, having_preds, &reduce_layout, &reduce_schema.columns)?;

    // Finalize map + output columns + output layout. The PK region is inherited
    // by the MAP (natural group cols renamed in place; the synthetic `_group_pk`
    // carried verbatim); the payload is written by the ExprProgram.
    let mut out_cols: Vec<ColumnDef> = reduce_schema.columns[..pk_len].to_vec();
    let mut out_layout: Vec<ColId> = reduce_layout[..pk_len].to_vec();
    let mut pk_renamed = vec![false; pk_len];
    let mut eb = ExprBuilder::new();
    let mut payload_idx: u32 = 0;
    let is_natural = out_key != ReduceOutKey::SyntheticFold;
    for entry in items {
        // A bare reference to a group column (finalize a natural PK in place).
        let group_ref = match &entry.expr {
            BExpr::ColRef(HirRef::Col(id)) if group_cols.contains(id) => Some(*id),
            _ => None,
        };
        if let Some(gid) = group_ref {
            let j = group_cols.iter().position(|g| *g == gid).unwrap();
            let reduce_col = group_reduce_pos[j];
            if is_natural && !pk_renamed[reduce_col] {
                out_cols[reduce_col].name = entry.out.def.name.clone();
                out_layout[reduce_col] = entry.out.id;
                pk_renamed[reduce_col] = true;
                continue;
            }
            let tc = reduce_schema.columns[reduce_col].type_code;
            eb.copy_col(tc as u32, reduce_col as u32, payload_idx);
            out_cols.push(entry.out.def.clone());
            out_layout.push(entry.out.id);
            payload_idx += 1;
            continue;
        }
        // Aggregate composite (or a Direct ColRef to a raw reduce column).
        let resolved = physical::resolve_refs(&entry.expr, &reduce_layout)?;
        match &resolved {
            BoundExpr::ColRef(slot) => {
                let tc = reduce_schema.columns[*slot].type_code;
                eb.copy_col(tc as u32, *slot as u32, payload_idx);
            }
            _ => {
                let reg = compile_bound_expr(&resolved, &reduce_schema.columns, &mut eb)?;
                eb.emit_col(reg, payload_idx);
            }
        }
        out_cols.push(entry.out.def.clone());
        out_layout.push(entry.out.id);
        payload_idx += 1;
    }
    let mapped = cb.map_expr(filtered_reduced, eb.build(0));
    cb.sink(mapped);
    let circuit = cb.build();

    reject_duplicate_column_names(&out_cols, "GROUP BY view")?;
    let view_pk: Vec<u32> = (0..pk_len as u32).collect();
    Ok(((circuit, out_cols, view_pk), out_layout))
}
