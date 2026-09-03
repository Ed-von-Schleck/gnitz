//! The reduce (GROUP BY / aggregate) emission shell. Sources the group columns,
//! decomposed aggregate specs, HAVING filter, and finalize projection from the
//! HIR `Reduce` node, and drives the shared `crate::agg` primitives — the
//! strategy selection (`emit_reduce`), the reduce-output layout
//! (`reduce_output_schema`, `group_col_reduce_pos`), and the spec decomposition
//! (`push_agg_specs`) — which it shares with `lower::fold`, the same reduce
//! lowered to an ad-hoc read's fold sink instead of to a circuit.

use super::super::physical;
use super::super::{is_pre_map, ColId, HirExpr, HirRef, ProjEntry, RelExpr};
use super::{
    cut_segment, emit_filter, extract_scan_bound, reduce_out_layout, resolve_reduce_specs, seginput_of_get,
    split_filter, CutMemo, ReduceSpecs,
};
use crate::agg::{emit_reduce, ensure_cardinality_count, group_col_reduce_pos, reduce_output_schema, ReduceShape};
use crate::codec::project_schema::{compile_projection_map, declared_out_cols, ProjItem};
use crate::error::GnitzSqlError;
use crate::expr_lower::compile_filter_program;
use crate::hir::chain::{EmitPieces, ViewChain};
use crate::ir::BExpr;
use crate::validate::reject_duplicate_column_names;
use gnitz_core::{CircuitBuilder, ColumnDef, ReduceOutKey};
use std::collections::HashSet;
use std::sync::Arc;

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
    let RelExpr::Reduce {
        input,
        group_cols,
        aggs,
    } = reduce
    else {
        unreachable!("lower_reduce receives a Reduce");
    };

    // The pre-map bind inserted so the reduce could group by, or aggregate, an
    // expression. Emitted as a map in this circuit; cutting it to its own segment
    // would materialize a second copy of the source forever.
    let (pre_map, reduce_input) = match input.as_ref() {
        RelExpr::Project { input: below, items }
            if is_pre_map(below, items) && seginput_of_get(split_filter(below).1).is_some() =>
        {
            (Some(items.as_slice()), below)
        }
        _ => (None, input),
    };

    // Resolve the reduce's input: inline a base/segment `Get` (with WHERE + scan
    // bound), or cut a combine input (`Filter?(Join)`) to a hidden segment.
    let (inner_where, inner_source) = split_filter(reduce_input);
    let (source, bound, where_preds) = match seginput_of_get(inner_source) {
        Some(seg) => {
            // Base/segment Get: apply the WHERE + scan bound inline.
            let preds = physical::resolve_preds(inner_where, &seg.layout)?;
            let bound = extract_scan_bound(&preds, &seg);
            (seg, bound, preds)
        }
        None => {
            // Cut the whole input (WHERE included) to a hidden segment; the reduce
            // reads it delta-bounded with no bound / no re-applied WHERE.
            let mut live: HashSet<ColId> = HashSet::new();
            live.extend(group_cols.iter().copied());
            live.extend(aggs.iter().filter_map(|a| a.arg));
            (cut_segment(chain, memo, input, &live)?, None, Vec::new())
        }
    };
    // The pre-map, physicalized over the source. Its output is what the reduce
    // groups and aggregates over, so the group/argument positions, the strategy
    // and the reduce-output layout are all resolved against it rather than the
    // source.
    let pre = pre_map
        .map(|items| physical::physicalize_projection(items, &source.layout, &source.schema))
        .transpose()?;
    let source_tid = source.tid;
    let (reduce_in, reduce_in_layout) = match &pre {
        Some(p) => (
            crate::hir::chain::schema_of(
                &p.out_cols,
                p.pk_arity,
                "GROUP BY over a computed key or aggregate argument",
            )?,
            &p.layout,
        ),
        None => (Arc::clone(&source.schema), &source.layout),
    };

    let ReduceSpecs {
        group_positions,
        mut specs,
        agg_starts,
    } = resolve_reduce_specs(group_cols, aggs, reduce_in_layout, &reduce_in)?;
    // The emission-only cardinality COUNT is the circuit's alone: it is the
    // `should_emit` signal a stateful reduce gates group existence on, which the
    // stateless fold has no use for.
    ensure_cardinality_count(&reduce_in.columns, &mut specs)?;

    // Reduce strategy (two-phase global / replicated / sharded). A chain-minted
    // segment carries no descriptor, and is never replicated anyway.
    let source_replicated = source.desc.as_ref().is_some_and(|d| d.replicated);
    let shape = ReduceShape::new(&reduce_in, &group_positions, &specs, source_replicated);
    let out_key = shape.out_key;
    let (reduce_schema, agg_col_offset) = reduce_output_schema(&shape)?;
    let pk_len = reduce_schema.pk_cols.len();

    // Circuit: input delta + optional WHERE.
    let mut cb = CircuitBuilder::new(source_tid);
    let inp = cb.input_delta_bounded(bound);
    // Resolved above against `source.layout` (the scan bound reads the same
    // conjuncts) — so it types against the source, not `reduce_in`: the pre-map
    // moves the source PK columns to the front, and a PK not already at slot 0
    // makes the two orders disagree.
    let filtered = match compile_filter_program(&where_preds, &source.schema.columns)? {
        Some(p) => cb.filter(inp, Some(p)),
        None => inp,
    };

    // The pre-map, between the WHERE and the reduce. Only the payload slots are
    // written — the PK region is carried verbatim, as it is on every other
    // `map_expr` — so `filtered`'s schema is the source's and the map's output is
    // `reduce_in`.
    let mapped = match &pre {
        Some(p) => cb.map_expr(
            filtered,
            compile_projection_map(&p.items[p.pk_arity..], &source.schema)?,
            &declared_out_cols(&p.out_cols[p.pk_arity..]),
        ),
        None => filtered,
    };

    let reduced = emit_reduce(&mut cb, mapped, &shape);

    // Each group column's physical reduce-output slot — computed once here and
    // reused by the finalize loop below (`group_reduce_pos[j]`), rather than
    // re-deriving the same `group_col_reduce_pos` lookup a second time per item.
    let group_reduce_pos: Vec<usize> = (0..group_cols.len())
        .map(|j| group_col_reduce_pos(group_positions[j], out_key, &reduce_in, &group_positions))
        .collect();

    let reduce_layout = reduce_out_layout(
        reduce_schema.columns.len(),
        group_cols,
        &group_reduce_pos,
        aggs,
        &agg_starts,
        agg_col_offset,
    );

    // HAVING filter over the raw reduce output.
    let filtered_reduced = emit_filter(&mut cb, reduced, having_preds, &reduce_layout, &reduce_schema.columns)?;

    // Finalize map + output columns + output layout. The PK region is inherited
    // by the MAP (natural group cols renamed in place; the synthetic `_group_pk`
    // carried verbatim); the payload is written by the expression program.
    let mut out_cols: Vec<ColumnDef> = reduce_schema.columns[..pk_len].to_vec();
    let mut out_layout: Vec<ColId> = reduce_layout[..pk_len].to_vec();
    let mut pk_renamed = vec![false; pk_len];
    let mut proj_items: Vec<ProjItem> = Vec::new();
    let is_natural = out_key != ReduceOutKey::SyntheticFold;
    for entry in items {
        // A bare reference to a group column: its reduce-output slot, resolved in
        // one scan (the position is what the emit needs, not the id).
        let group_slot = match &entry.expr {
            BExpr::ColRef(HirRef::Col(id)) => group_cols.iter().position(|g| g == id).map(|j| group_reduce_pos[j]),
            _ => None,
        };
        let item = match group_slot {
            Some(reduce_col) => {
                // A natural PK group column is finalized in place — renamed in the
                // inherited PK region, not written by the map program. A second
                // reference to the same group column falls through to the payload.
                if is_natural && !pk_renamed[reduce_col] {
                    out_cols[reduce_col].name = entry.out.def.name.clone();
                    out_layout[reduce_col] = entry.out.id;
                    pk_renamed[reduce_col] = true;
                    continue;
                }
                ProjItem::PassThrough { src_col: reduce_col }
            }
            // An aggregate composite, or a Direct `ColRef` to a raw reduce column.
            None => ProjItem::from_bound(physical::resolve_refs(&entry.expr, &reduce_layout)?),
        };
        proj_items.push(item);
        out_cols.push(entry.out.def.clone());
        out_layout.push(entry.out.id);
    }
    // The payload slots, through the shared map compiler — `proj_items` is dense in
    // payload order, since a renamed-in-place PK column never pushes one.
    let mapped = cb.map_expr(
        filtered_reduced,
        compile_projection_map(&proj_items, &reduce_schema)?,
        &declared_out_cols(&out_cols[pk_len..]),
    );
    cb.sink(mapped);
    let circuit = cb.build();

    reject_duplicate_column_names(out_cols.iter(), "GROUP BY view")?;
    Ok(((circuit, out_cols, pk_len), out_layout))
}
