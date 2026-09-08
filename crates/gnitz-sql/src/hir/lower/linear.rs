//! The linear segment emitter: an optional WHERE filter plus a projection (pure
//! column reorder/subset, or an expr-map when the projection derives or duplicates
//! a PK column). It owns the spine whole, from the HIR inputs down, so a body
//! that fuses the spine into its own operator's circuit cannot take the steps in
//! a different order.

use super::physical::{self, PhysProjection};
use super::SegInput;
use crate::error::GnitzSqlError;
use crate::expr_lower::compile_filter_program;
use crate::hir::chain::EmitPieces;
use crate::hir::{ColId, HirExpr, ProjEntry};
use gnitz_core::CircuitBuilder;

/// Emit a linear segment's own circuit, plus its output layout. The emitted
/// circuit carries no exchange: a filter/map neither re-keys nor redistributes
/// its source, so every row stays on the worker that produced it.
pub(super) fn emit_linear(
    src: &SegInput,
    filter_preds: &[HirExpr],
    proj_items: &[ProjEntry],
) -> Result<(EmitPieces, Vec<ColId>), GnitzSqlError> {
    let mut cb = CircuitBuilder::new(src.tid);
    let (out_node, proj) = emit_linear_into(&mut cb, src, filter_preds, proj_items)?;
    cb.sink(out_node);
    let pieces = EmitPieces {
        circuit: cb.build(),
        out_cols: proj.out_cols,
        pk_arity: proj.pk_arity,
    };
    Ok((pieces, proj.layout))
}

/// The linear spine emitted into `cb`, so a body whose operator sits *over* one
/// fuses it into that operator's own circuit rather than re-emitting it. The
/// physicalized projection rides back: it carries the output schema and layout
/// the caller declares its own output from.
pub(super) fn emit_linear_into(
    cb: &mut CircuitBuilder,
    src: &SegInput,
    filter_preds: &[HirExpr],
    proj_items: &[ProjEntry],
) -> Result<(gnitz_core::NodeId, PhysProjection), GnitzSqlError> {
    let source_schema = &src.schema;
    let preds = physical::resolve_preds(filter_preds, &src.layout)?;
    let bound = super::extract_scan_bound(&preds, src);
    let proj = physical::physicalize_projection(proj_items, &src.layout, source_schema)?;

    // Filter program (if any), compiled against the source schema. A WHERE with
    // nothing left to test compiles to no filter at all.
    let expr_prog = compile_filter_program(&preds, &source_schema.columns)?;

    // The `Filter` below is emitted verbatim whether or not a bound rode in: the
    // bound narrows the backfill scan, never the predicate.
    let inp = cb.input_delta_bounded(bound);
    let filtered = match expr_prog {
        Some(p) => cb.filter(inp, p),
        None => inp,
    };

    let node = super::emit_projection(cb, filtered, &proj.items, &proj.out_cols, source_schema, proj.pk_arity)?;
    Ok((node, proj))
}
