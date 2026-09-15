//! The positional pass, run at lowering. Assigns physical column positions to
//! the layout-free logical IR: it substitutes each `ColId` leaf with a
//! `ColRef(position)` against a node's column layout, and physicalizes a
//! projection (the pass-through/computed split + the PK-front convention, whose
//! single home is `place_pk_front`).

use super::{slot_of, ColId, HirExpr, ProjEntry};
use crate::codec::project_schema::{place_pk_front, ProjItem};
use crate::error::GnitzSqlError;
use crate::ir::BoundExpr;
use gnitz_core::{ColumnDef, Schema};
use std::sync::Arc;

/// A relation's physical addressing: the `ColId` at each slot and the schema
/// typing those slots. A reference resolves to a position in `layout` and is
/// typed against `schema` at that position, so the two travel as one value.
#[derive(Clone)]
pub(crate) struct Frame {
    pub(crate) layout: Vec<ColId>,
    pub(crate) schema: Arc<Schema>,
}

impl Frame {
    /// `columns` behind a leading key region of `npk` slots.
    pub(crate) fn leading(layout: Vec<ColId>, columns: Vec<ColumnDef>, npk: usize) -> Frame {
        debug_assert_eq!(layout.len(), columns.len(), "a frame's two halves are parallel");
        Frame {
            layout,
            schema: Arc::new(Schema {
                columns,
                pk_cols: (0..npk as u32).collect(),
            }),
        }
    }

    /// `pk_cols` as the leading key region — identity-free slots nothing can
    /// reference — then one iterator of `(id, def)` driving both halves.
    pub(crate) fn keyed(pk_cols: Vec<ColumnDef>, payload: impl IntoIterator<Item = (ColId, ColumnDef)>) -> Frame {
        let npk = pk_cols.len();
        let mut layout = vec![ColId::NONE; npk];
        let mut columns = pk_cols;
        for (id, def) in payload {
            layout.push(id);
            columns.push(def);
        }
        Frame::leading(layout, columns, npk)
    }

    /// The key region's width.
    pub(crate) fn npk(&self) -> usize {
        self.schema.pk_cols.len()
    }
}

/// Substitute every `ColId` leaf with `ColRef(position of id in layout)`. A
/// `ColId` with no layout slot is an internal compile error.
pub(crate) fn resolve_refs(expr: &HirExpr, layout: &[ColId]) -> Result<BoundExpr, GnitzSqlError> {
    expr.try_rebuild(&mut |id| slot_of(layout, *id).map(BoundExpr::ColRef))
}

/// Resolve each conjunct against `layout`. The list is what the scan bound and
/// every filter emit consume; nothing folds it into a tree.
pub(crate) fn resolve_preds<'a>(
    preds: impl IntoIterator<Item = &'a HirExpr>,
    layout: &[ColId],
) -> Result<Vec<BoundExpr>, GnitzSqlError> {
    preds.into_iter().map(|p| resolve_refs(p, layout)).collect()
}

/// A physicalized projection: the emission items and the output frame.
pub(crate) struct PhysProjection {
    pub items: Vec<ProjItem>,
    pub out: Frame,
}

/// Physicalize a projection over `input_layout` / `input_schema`, the source PK
/// pinned to the leading slots; an auto-prepended PK slot has no identity.
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
        out: Frame::leading(layout, out_cols, input_schema.pk_count()),
    })
}
