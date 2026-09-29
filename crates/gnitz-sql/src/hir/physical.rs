//! The positional pass, run at lowering. Assigns physical column positions to
//! the layout-free logical IR: [`Frame`] substitutes each `ColId` leaf with a
//! `ColRef(position)` against a node's column layout, and
//! [`physicalize_projection`] physicalizes a projection (the pass-through/computed
//! split + the PK-front convention, whose single home is `place_pk_front`).

use super::{as_col, ColId, HirCol, HirExpr, ProjEntry};
use crate::codec::project_schema::{leading_schema, ProjItem};
use crate::error::GnitzSqlError;
use crate::ir::BoundExpr;
use gnitz_core::{RelDescriptor, Schema};
use gnitz_wire::ColumnDef;
use std::collections::HashSet;
use std::sync::Arc;

/// A relation's physical addressing: the `ColId` at each slot and the schema
/// typing those slots. A reference resolves to a position in `layout` and is
/// typed against `schema` at that position, so the two travel as one value.
#[derive(Clone)]
pub(crate) struct Frame {
    /// `None` at a slot no reference can name.
    pub(crate) layout: Vec<Option<ColId>>,
    pub(crate) schema: Arc<Schema>,
}

impl Frame {
    /// `slots` behind a leading key region of `npk`, admitted as a schema the
    /// engine can hold.
    pub(crate) fn new(
        slots: impl IntoIterator<Item = (Option<ColId>, ColumnDef)>,
        npk: usize,
    ) -> Result<Frame, GnitzSqlError> {
        let (layout, columns): (Vec<_>, Vec<_>) = slots.into_iter().unzip();
        Ok(Frame {
            layout,
            schema: Arc::new(leading_schema(columns, npk)?),
        })
    }

    /// `pk_cols` as an identity-free leading key region, then `payload`.
    pub(crate) fn keyed(
        pk_cols: Vec<ColumnDef>,
        payload: impl IntoIterator<Item = (Option<ColId>, ColumnDef)>,
    ) -> Result<Frame, GnitzSqlError> {
        let npk = pk_cols.len();
        Frame::new(pk_cols.into_iter().map(|d| (None, d)).chain(payload), npk)
    }

    /// A catalog relation read under its `Get`'s ids.
    pub(crate) fn scan(desc: &RelDescriptor, cols: &[HirCol]) -> Frame {
        Frame {
            layout: cols.iter().map(|c| Some(c.id)).collect(),
            schema: Arc::clone(&desc.schema),
        }
    }

    /// The slot `id` names. A `ColId` with no slot is an internal compile error.
    pub(crate) fn slot(&self, id: ColId) -> Result<usize, GnitzSqlError> {
        self.layout
            .iter()
            .position(|c| *c == Some(id))
            .ok_or_else(|| GnitzSqlError::Internal("HIR column reference has no layout slot".into()))
    }

    /// [`Self::slot`] for each of `ids`, in order.
    pub(crate) fn slots(&self, ids: impl IntoIterator<Item = ColId>) -> Result<Vec<usize>, GnitzSqlError> {
        ids.into_iter().map(|id| self.slot(id)).collect()
    }

    /// The group list to send a reduce or top-N grouped by `ids`' slots: SQL's
    /// GROUP BY is unordered, so a permutation of the PK is sent as the PK list.
    pub(crate) fn reduce_group(&self, ids: &[ColId]) -> Result<Vec<u32>, GnitzSqlError> {
        let group: Vec<u32> = self.slots(ids.iter().copied())?.into_iter().map(|c| c as u32).collect();
        let pk = &self.schema.pk_cols;
        let is_pk = group.len() == pk.len() && pk.iter().all(|p| group.contains(p));
        Ok(if is_pk { pk.clone() } else { group })
    }

    /// `expr` with every `ColId` leaf substituted by `ColRef(its slot)`.
    pub(crate) fn resolve(&self, expr: &HirExpr) -> Result<BoundExpr, GnitzSqlError> {
        expr.try_rebuild(&mut |id| self.slot(*id).map(BoundExpr::ColRef))
    }

    /// [`Self::resolve`] for each conjunct.
    pub(crate) fn resolve_preds(&self, preds: &[HirExpr]) -> Result<Vec<BoundExpr>, GnitzSqlError> {
        preds.iter().map(|p| self.resolve(p)).collect()
    }

    /// This frame under `rename`'s identities. A slot it does not name loses its
    /// identity.
    pub(crate) fn renamed(&self, rename: &Rename) -> Result<Frame, GnitzSqlError> {
        let mut layout = vec![None; self.layout.len()];
        let mut columns = self.schema.columns.clone();
        for (src, out) in &rename.0 {
            let slot = self.slot(*src)?;
            layout[slot] = Some(out.id);
            columns[slot] = out.def.clone();
        }
        Ok(Frame {
            layout,
            schema: Arc::new(Schema {
                columns,
                pk_cols: self.schema.pk_cols.clone(),
            }),
        })
    }
}

/// A projection that moves no data: each output column is a distinct input
/// column under a new identity.
pub(crate) struct Rename(Vec<(ColId, HirCol)>);

impl Rename {
    /// `items` as a rename, `None` unless each is a bare column, no column twice.
    pub(crate) fn of(items: &[ProjEntry]) -> Option<Rename> {
        let mut seen = HashSet::new();
        items
            .iter()
            .map(|it| Some((as_col(&it.expr).filter(|id| seen.insert(*id))?, it.out.clone())))
            .collect::<Option<_>>()
            .map(Rename)
    }

    /// A relation's columns `inner` under `outer`, position by position.
    pub(crate) fn alias(inner: &[HirCol], outer: &[HirCol]) -> Rename {
        Rename(inner.iter().map(|c| c.id).zip(outer.iter().cloned()).collect())
    }
}

/// A projection's slots over `input`, in output order: each entry resolved and
/// classified, then the input's PK pinned to the leading slots.
pub(crate) fn project_slots(
    items: &[ProjEntry],
    input: &Frame,
) -> Result<Vec<(ProjItem, Option<ColId>, ColumnDef)>, GnitzSqlError> {
    let mut slots = items
        .iter()
        .map(|e| {
            Ok((
                ProjItem::from_bound(input.resolve(&e.expr)?),
                Some(e.out.id),
                e.out.def.clone(),
            ))
        })
        .collect::<Result<Vec<_>, GnitzSqlError>>()?;
    place_pk_front(&mut slots, &input.schema);
    Ok(slots)
}

/// [`project_slots`] as emission items and the output frame.
pub(crate) fn physicalize_projection(
    items: &[ProjEntry],
    input: &Frame,
) -> Result<(Vec<ProjItem>, Frame), GnitzSqlError> {
    let (proj, cols): (Vec<_>, Vec<_>) = project_slots(items, input)?
        .into_iter()
        .map(|(item, id, def)| (item, (id, def)))
        .unzip();
    Ok((proj, Frame::new(cols, input.schema.pk_cols.len())?))
}

/// Pin the source PK to slots `0..k` in PK-list order, as the engine's
/// `project_schema` does: each PK column's first pass-through moves there, and
/// one nothing passes through is prepended hidden.
fn place_pk_front(slots: &mut Vec<(ProjItem, Option<ColId>, ColumnDef)>, source: &Schema) {
    for (target, &pk) in source.pk_cols.iter().enumerate() {
        let pk = pk as usize;
        let slot = match slots.iter().position(|(item, ..)| item.passthrough_src() == Some(pk)) {
            Some(pos) => slots.remove(pos),
            None => (
                ProjItem::PassThrough { src_col: pk },
                None,
                source.columns[pk].clone().hidden(),
            ),
        };
        slots.insert(target, slot);
    }
}

#[cfg(test)]
#[path = "tests/physical.rs"]
mod tests;
