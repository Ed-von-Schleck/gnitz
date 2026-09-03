//! The positional pass, run at lowering. Assigns physical column positions to
//! the layout-free logical IR: it substitutes each `HirRef::Col(id)` leaf with a
//! `ColRef(position)` against a node's column layout, and physicalizes a
//! projection (the pass-through/computed split + the PK-front convention, whose
//! single home is `place_pk_front`).

use super::{slot_of, ColId, HirExpr, HirRef, ProjEntry};
use crate::codec::project_schema::{place_pk_front, ProjItem};
use crate::error::GnitzSqlError;
use crate::ir::BoundExpr;
use gnitz_core::{ColumnDef, Schema};

/// Substitute every `HirRef::Col(id)` leaf with `ColRef(position of id in
/// layout)`. A `HirRef` with no layout slot is an internal compile error, and a
/// `HirRef::Subquery` leaf must have been consumed by decorrelation — its survival
/// to physicalization is the one point that rejects it.
pub(crate) fn resolve_refs(expr: &HirExpr, layout: &[ColId]) -> Result<BoundExpr, GnitzSqlError> {
    expr.try_rebuild(&|r| match r {
        HirRef::Col(id) => slot_of(layout, *id).map(BoundExpr::ColRef),
        HirRef::Subquery(_) => Err(GnitzSqlError::Internal(
            "subquery leaf survived to physicalization".into(),
        )),
    })
}

/// Resolve each conjunct against `layout`. The list is what the scan bound and
/// every filter emit consume; nothing folds it into a tree.
pub(crate) fn resolve_preds<'a>(
    preds: impl IntoIterator<Item = &'a HirExpr>,
    layout: &[ColId],
) -> Result<Vec<BoundExpr>, GnitzSqlError> {
    preds.into_iter().map(|p| resolve_refs(p, layout)).collect()
}

/// A physicalized projection: the emission items, the output column defs, the
/// leading key arity, and the output `ColId` at each physical slot.
pub(crate) struct PhysProjection {
    pub items: Vec<ProjItem>,
    pub out_cols: Vec<ColumnDef>,
    pub pk_arity: usize,
    pub layout: Vec<ColId>,
}

/// Physicalize a projection over `input_layout` / `input_schema` — the HIR
/// realization of the linear projection: resolve each `ProjEntry.expr`, classify a
/// bare `ColRef(i)` as `PassThrough`/else `Computed`, take the output def from
/// `ProjEntry.out`, then pin the source PK to the leading slots. The returned
/// `layout` is reordered through the same `place_pk_front` permutation (an
/// auto-prepended hidden PK slot is [`ColId::NONE`] — the user never named that
/// column, so nothing can reference it), so a cut linear segment exposes its layout
/// exactly like a combine one.
pub(crate) fn physicalize_projection(
    items: &[ProjEntry],
    input_layout: &[ColId],
    input_schema: &Schema,
) -> Result<PhysProjection, GnitzSqlError> {
    let mut proj_items: Vec<ProjItem> = Vec::with_capacity(items.len());
    let mut out_cols: Vec<ColumnDef> = Vec::with_capacity(items.len());
    for entry in items {
        proj_items.push(ProjItem::from_bound(resolve_refs(&entry.expr, input_layout)?));
        out_cols.push(entry.out.def.clone());
    }
    let perm = place_pk_front(&mut proj_items, &mut out_cols, input_schema);
    let layout = perm
        .into_iter()
        .map(|src| match src {
            Some(i) => items[i].out.id,
            None => ColId::NONE,
        })
        .collect();
    Ok(PhysProjection {
        items: proj_items,
        out_cols,
        pk_arity: input_schema.pk_count(),
        layout,
    })
}
