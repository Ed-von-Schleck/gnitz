//! The reduce (GROUP BY / aggregate) emission shell. Sources the group columns,
//! decomposed aggregate specs, HAVING filter, and finalize projection from the
//! HIR `Reduce` node, and drives the shared `crate::agg` primitives — the
//! strategy selection (`emit_reduce`), the reduce-output layout
//! (`reduce_output_schema`), and the spec decomposition
//! (`push_agg_specs`) — which it shares with `lower::fold`, the same reduce
//! lowered to an ad-hoc read's fold sink instead of to a circuit.

use super::super::physical;
use super::super::split_filter;
use super::super::{ColId, HirExpr, ProjEntry, RelExpr};
use super::{
    cut_segment, emit_filter, extract_scan_bound, physicalize_pre_map, pre_map_blob, reduce_input_of,
    reduce_out_layout, resolve_in_place, resolve_reduce_specs, CutMemo, Frame, ReduceSpecs,
};
use crate::agg::{emit_reduce, ensure_cardinality_count, reduce_output_schema, ReduceLayout, ReduceShape};
use crate::codec::project_schema::{payload_map, ProjItem};
use crate::error::GnitzSqlError;
use crate::expr_lower::compile_filter_program;
use crate::hir::chain::{EmitPieces, ViewChain};
use crate::validate::reject_duplicate_column_names;
use gnitz_core::{CircuitBuilder, ColumnDef};
use std::collections::HashSet;
use std::rc::Rc;

/// Lower a `Project(Filter_having?(Reduce(...)))` body's reduce to circuit pieces
/// returning the pieces plus the output `ColId` layout. `items` is
/// the finalize projection; `having_preds` is the HAVING filter over the raw
/// reduce output.
pub(crate) fn lower_reduce(
    chain: &mut ViewChain,
    memo: &mut CutMemo,
    items: &[ProjEntry],
    having_preds: &[HirExpr],
    reduce: &RelExpr,
) -> Result<(EmitPieces, Vec<ColId>), GnitzSqlError> {
    let RelExpr::Reduce { input, pre, group_cols, aggs } = reduce else {
        unreachable!("lower_reduce receives a Reduce");
    };

    // Read the source in place (applying the WHERE and scan bound here), or cut the
    // whole input to a hidden segment. The pre-map fuses only on the in-place arm —
    // over a cut source it rides inside the segment already.
    let (inner_where, inner_source) = split_filter(input);
    let (source, bound, where_preds, pre_items) = match resolve_in_place(chain, memo, inner_source)? {
        Some(seg) => {
            let preds = physical::resolve_preds(inner_where, &seg.layout)?;
            let bound = extract_scan_bound(&preds, &seg);
            (seg, bound, preds, pre.as_slice())
        }
        None => {
            // The reduce reads the segment delta-bounded: no bound, no re-applied WHERE.
            let mut live: HashSet<ColId> = HashSet::new();
            live.extend(group_cols.iter().copied());
            live.extend(aggs.iter().filter_map(|a| a.arg));
            let cut = match pre.is_empty() {
                true => Rc::clone(input),
                false => RelExpr::project(Rc::clone(input), pre.clone()),
            };
            (cut_segment(chain, memo, &cut, &live)?, None, Vec::new(), &[][..])
        }
    };
    // The pre-map's output is what the reduce groups and aggregates over, so the
    // group/argument positions, the strategy and the reduce-output layout all
    // resolve against it rather than against the source.
    let pre = physicalize_pre_map(pre_items, &source.layout, &source.schema)?;
    let source_tid = source.tid;
    let (reduce_in, reduce_in_layout) = reduce_input_of(
        &pre,
        &source.layout,
        &source.schema,
        "GROUP BY over a computed key or aggregate argument",
    )?;

    let ReduceSpecs { group_positions, mut specs, agg_starts } =
        resolve_reduce_specs(group_cols, aggs, reduce_in_layout, &reduce_in)?;
    // The emission-only cardinality COUNT is the circuit's alone: it is the
    // `should_emit` signal a stateful reduce gates group existence on, which the
    // stateless fold has no use for.
    ensure_cardinality_count(&reduce_in.columns, &mut specs)?;

    // Reduce strategy (two-phase global / replicated / sharded). A chain-minted
    // segment carries no descriptor, and is never replicated anyway.
    let source_replicated = source.desc.as_ref().is_some_and(|d| d.replicated);
    let shape = ReduceShape::new(&reduce_in, &group_positions, &specs, source_replicated);
    let ReduceLayout {
        schema: reduce_schema,
        group_slots,
        agg_col_offset,
    } = reduce_output_schema(&shape)?;
    let pk_len = reduce_schema.pk_cols.len();

    // Circuit: input delta + optional WHERE.
    let mut cb = CircuitBuilder::new(source_tid);
    let inp = cb.input_delta_bounded(bound);
    // Resolved above against `source.layout` (the scan bound reads the same
    // conjuncts) — so it types against the source, not `reduce_in`: the pre-map
    // moves the source PK columns to the front, and a PK not already at slot 0
    // makes the two orders disagree.
    let filtered = match compile_filter_program(&where_preds, &source.schema.columns)? {
        Some(p) => cb.filter(inp, p),
        None => inp,
    };

    // The pre-map, between the WHERE and the reduce. Only the payload slots are
    // written — the PK region is carried verbatim, as it is on every other
    // `map_expr` — so `filtered`'s schema is the source's and the map's output is
    // `reduce_in`.
    let mapped = match pre_map_blob(&pre, &source.schema)? {
        Some(blob) => cb.map_expr(filtered, blob),
        None => filtered,
    };

    let reduced = emit_reduce(&mut cb, mapped, &shape);

    let reduce_layout = reduce_out_layout(
        reduce_schema.columns.len(),
        group_cols,
        &group_slots,
        aggs,
        &agg_starts,
        agg_col_offset,
    );

    // HAVING filter over the raw reduce output.
    let having_frame = Frame::of(&reduce_layout, &reduce_schema);
    let filtered_reduced = emit_filter(&mut cb, reduced, having_preds, &having_frame)?;

    // Finalize map + output columns + output layout. The PK region is inherited
    // by the MAP (natural group cols renamed in place; the synthetic `_group_pk`
    // carried verbatim); the payload is written by the expression program.
    let mut out_cols: Vec<ColumnDef> = reduce_schema.columns[..pk_len].to_vec();
    let mut out_layout: Vec<ColId> = reduce_layout[..pk_len].to_vec();
    let mut pk_renamed = vec![false; pk_len];
    let mut proj_items: Vec<ProjItem> = Vec::new();
    for entry in items {
        let item = ProjItem::from_bound(physical::resolve_refs(&entry.expr, &reduce_layout)?);
        // A group column in the inherited PK region is renamed in place rather than
        // written by the map; a second reference to it falls through to the payload.
        // Only a group column lands there — every aggregate slot is past the region.
        if let Some(slot) = item.passthrough_src().filter(|&c| c < pk_len && !pk_renamed[c]) {
            out_cols[slot].name = entry.out.def.name.clone();
            out_layout[slot] = entry.out.id;
            pk_renamed[slot] = true;
            continue;
        }
        proj_items.push(item);
        out_cols.push(entry.out.def.clone());
        out_layout.push(entry.out.id);
    }
    // The payload slots, through the shared map compiler — `proj_items` is dense in
    // payload order, since a renamed-in-place PK column never pushes one.
    let mapped = cb.map_expr(
        filtered_reduced,
        payload_map(&proj_items, &out_cols[pk_len..], &reduce_schema)?,
    );
    cb.sink(mapped);
    let circuit = cb.build();

    reject_duplicate_column_names(out_cols.iter(), "GROUP BY view")?;
    Ok(((circuit, out_cols, pk_len), out_layout))
}
