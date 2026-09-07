//! The linear segment emitter: an optional WHERE filter plus a projection (pure
//! column reorder/subset, or an expr-map when the projection derives or duplicates
//! a PK column). `lower::lower_linear` resolves the HIR to the physical inputs
//! (source, scan bound, WHERE conjuncts, physicalized projection) and hands them
//! here; this module owns only the `CircuitBuilder` call sequence.

use super::physical::PhysProjection;
use super::SegInput;
use crate::error::GnitzSqlError;
use crate::expr_lower::compile_filter_program;
use crate::hir::chain::EmitPieces;
use crate::ir::BoundExpr;
use gnitz_core::CircuitBuilder;
use gnitz_wire::IndexBound;

/// Emit a linear segment's circuit from its resolved physical
/// inputs. Returns `(circuit, output_columns, pk_cols)`; the view's physical PK is
/// the leading `k = proj.pk_arity` source-PK columns (`pk_cols == 0..k`).
///
/// The emitted circuit carries no exchange: a filter/map neither re-keys nor
/// redistributes its source, so every row stays on the worker that produced it.
pub(super) fn emit_linear(
    src: &SegInput,
    bound: Option<IndexBound>,
    filter: &[BoundExpr],
    proj: &PhysProjection,
) -> Result<EmitPieces, GnitzSqlError> {
    let source_schema = &src.schema;

    // Filter program (if any), compiled against the source schema. A WHERE with
    // nothing left to test compiles to no filter at all.
    let expr_prog = compile_filter_program(filter, &source_schema.columns)?;

    let mut cb = CircuitBuilder::new(src.tid);
    // The `Filter` below is emitted verbatim whether or not a bound rode in: the
    // bound narrows the backfill scan, never the predicate.
    let inp = cb.input_delta_bounded(bound);
    let filtered = match expr_prog {
        Some(p) => cb.filter(inp, p),
        None => inp,
    };

    let out_node = super::emit_projection(
        &mut cb,
        filtered,
        &proj.items,
        &proj.out_cols,
        source_schema,
        proj.pk_arity,
    )?;
    cb.sink(out_node);
    let circuit = cb.build();

    // The view's physical PK is the leading k columns (the source PK passed
    // through in PK-list order).
    Ok((circuit, proj.out_cols.clone(), proj.pk_arity))
}
