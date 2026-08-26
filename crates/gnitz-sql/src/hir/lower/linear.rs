//! The linear segment emitter: an optional WHERE filter plus a projection (pure
//! column reorder/subset, or an expr-map when the projection derives or duplicates
//! a PK column). `lower::lower_linear` resolves the HIR to the physical inputs
//! (source, scan bound, folded predicate, physicalized projection) and hands them
//! here; this module owns only the `CircuitBuilder` call sequence.

use super::physical::PhysProjection;
use super::SegInput;
use crate::codec::project_schema::{compile_projection_map, declared_out_cols, ProjItem};
use crate::error::GnitzSqlError;
use crate::expr_lower::compile_filter_program;
use crate::hir::chain::EmitPieces;
use crate::ir::BoundExpr;
use gnitz_core::CircuitBuilder;
use gnitz_wire::ScanBound;

/// Emit a linear segment's circuit for `view_id` from its resolved physical
/// inputs. Returns `(circuit, output_columns, pk_cols)`; the view's physical PK is
/// the leading `k = proj.pk_arity` source-PK columns (`pk_cols == 0..k`).
///
/// The emitted circuit carries no exchange: a filter/map neither re-keys nor
/// redistributes its source, so every row stays on the worker that produced it.
pub(super) fn emit_linear(
    view_id: u64,
    src: &SegInput,
    bound: Option<ScanBound>,
    filter: Option<BoundExpr>,
    proj: &PhysProjection,
) -> Result<EmitPieces, GnitzSqlError> {
    let source_schema = &src.schema;
    let items = &proj.items;
    let k = proj.pk_arity;

    // Filter program (if any), compiled against the source schema. A predicate
    // that bound to a true constant compiles to no filter at all.
    let expr_prog = match &filter {
        Some(pred) => compile_filter_program(pred, &source_schema.columns)?,
        None => None,
    };

    // Slots 0..k are the view's physical PK (carried verbatim by commit_row). A
    // payload slot (>= k) that is a PK PassThrough is a duplicate PK value; a
    // Computed is a derived column. Either forces the expr-map, which trusts the
    // planner's declared schema and writes/derives each payload slot explicitly —
    // the pure projection path would silently drop a PK value requested as a
    // payload column.
    let needs_expr_map = items[k..].iter().any(|item| match item {
        ProjItem::Computed { .. } => true,
        ProjItem::PassThrough { src_col } => source_schema.is_pk_col(*src_col),
    });

    let mut cb = CircuitBuilder::new(view_id, src.tid);
    // The `Filter` below is emitted verbatim whether or not a bound rode in: the
    // bound narrows the backfill scan, never the predicate.
    let inp = cb.input_delta_bounded(bound);
    let filtered = match expr_prog {
        Some(p) => cb.filter(inp, Some(p)),
        None => inp,
    };

    let out_node = if needs_expr_map {
        // Emit only the payload slots (k..); the k physical PK columns are carried
        // by commit_row and must not appear in the program. payload_idx is the
        // dense output payload position, matching out_cols[k + payload_idx].
        let program = compile_projection_map(&items[k..], source_schema)?;
        cb.map_expr(filtered, program, &declared_out_cols(&proj.out_cols[k..]))
    } else if items.len() < source_schema.columns.len()
        || items.iter().enumerate().any(|(i, item)| match item {
            ProjItem::PassThrough { src_col } => *src_col != i,
            _ => false,
        })
    {
        // Pure column reorder/subset — every payload item is a non-PK
        // pass-through, so build_map_output_schema reproduces out_cols (PK region
        // in pk_indices() order, then non-PK cols in projection order). Pass only
        // the payload items (`items[k..]`): the k PK slots are inherited verbatim
        // by evaluate_map_batch's bulk PK copy / build_map_output_schema's PK prepend.
        // Including them would emit one ColMove per PK index with dst_payload set
        // to the enumeration index, shifting every payload destination out of range
        // (single-PK: payload OOB; compound-PK: SENTINEL stride past num_payload).
        let cols: Vec<usize> = items[k..]
            .iter()
            .filter_map(|i| match i {
                ProjItem::PassThrough { src_col } => Some(*src_col),
                _ => None,
            })
            .collect();
        cb.map(filtered, &cols)
    } else {
        // Identity — PK already at front, full width, no map needed.
        filtered
    };

    cb.sink(out_node);
    let circuit = cb.build();

    // The view's physical PK is the leading k columns (the source PK passed
    // through in pk_indices() order).
    let view_pk: Vec<u32> = (0..k as u32).collect();
    Ok((circuit, proj.out_cols.clone(), view_pk))
}
