//! The positional pass, run at lowering. Assigns physical column positions to
//! the layout-free logical IR: it substitutes each `HirRef::Col(id)` leaf with a
//! `ColRef(position)` against a node's column layout, and physicalizes a
//! projection (the pass-through/computed split + the PK-front convention, whose
//! single home is `place_pk_front`).

use super::{slot_of, ColId, HirExpr, HirRef, ProjEntry};
use crate::codec::project_schema::{place_pk_front, ProjItem};
use crate::error::GnitzSqlError;
use crate::ir::{BExpr, BinOp, BoundExpr};
use gnitz_core::{ColumnDef, Schema};

/// Substitute every `HirRef::Col(id)` leaf with `ColRef(position of id in
/// layout)`. A `HirRef` with no layout slot is an internal compile error. (No
/// `Subquery` arm exists yet; when it lands, this is the one point that rejects
/// a survivor.)
pub(crate) fn resolve_refs(expr: &HirExpr, layout: &[ColId]) -> Result<BoundExpr, GnitzSqlError> {
    expr.try_map_refs(&|HirRef::Col(id)| slot_of(layout, *id))
}

/// Resolve each conjunct against `layout` and AND-fold left-associatively
/// (`None` for no conjuncts) — the one home of "WHERE = left-assoc AND of its
/// conjuncts", consumed by the scan bound and every filter emit.
pub(crate) fn fold_preds<'a>(
    preds: impl IntoIterator<Item = &'a HirExpr>,
    layout: &[ColId],
) -> Result<Option<BoundExpr>, GnitzSqlError> {
    let mut folded: Option<BoundExpr> = None;
    for p in preds {
        let b = resolve_refs(p, layout)?;
        folded = Some(match folded {
            None => b,
            Some(acc) => BExpr::BinOp(Box::new(acc), BinOp::And, Box::new(b)),
        });
    }
    Ok(folded)
}

/// Physicalize a projection over `input_layout` / `input_schema` — the HIR
/// realization of `build_projection`: resolve each `ProjEntry.expr`, classify a
/// bare `ColRef(i)` as `PassThrough`/else `Computed`, take the output def from
/// `ProjEntry.out`, then pin the source PK to the leading slots. Returns
/// `(items, out_cols, pk_arity)` for the lowering to drive the emission strategy.
pub(crate) fn physicalize_projection(
    items: &[ProjEntry],
    input_layout: &[ColId],
    input_schema: &Schema,
) -> Result<(Vec<ProjItem>, Vec<ColumnDef>, usize), GnitzSqlError> {
    let mut proj_items: Vec<ProjItem> = Vec::with_capacity(items.len());
    let mut out_cols: Vec<ColumnDef> = Vec::with_capacity(items.len());
    for entry in items {
        let bound = resolve_refs(&entry.expr, input_layout)?;
        proj_items.push(match bound {
            BoundExpr::ColRef(i) => ProjItem::PassThrough { src_col: i },
            other => ProjItem::Computed { bound_expr: other },
        });
        out_cols.push(entry.out.def.clone());
    }
    place_pk_front(&mut proj_items, &mut out_cols, input_schema);
    Ok((proj_items, out_cols, input_schema.pk_count()))
}
