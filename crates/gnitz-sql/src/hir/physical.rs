//! The positional pass, run at lowering. Assigns physical column positions to
//! the layout-free logical IR: [`Frame`] substitutes each `ColId` leaf with a
//! `ColRef(position)` against a node's column layout, and
//! [`physicalize_projection`] physicalizes a projection (the pass-through/computed
//! split + the PK-front convention, whose single home is `place_pk_front`).

use super::{as_col, ColId, HirCol, HirExpr, ProjEntry};
use crate::error::GnitzSqlError;
use crate::ir::BoundExpr;
use crate::project::{leading_schema, ProjItem};
use gnitz_core::{RelDescriptor, Schema};
use gnitz_expr::SchemaFacts;
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
    /// GROUP BY is unordered, so a permutation of leading PK columns is sent in PK
    /// order.
    pub(crate) fn reduce_group(&self, ids: &[ColId]) -> Result<Vec<u32>, GnitzSqlError> {
        let group: Vec<u32> = self.slots(ids.iter().copied())?.into_iter().map(|c| c as u32).collect();
        let lead = self.schema.pk_cols.get(..group.len());
        Ok(match lead {
            Some(lead) if lead.iter().all(|p| group.contains(p)) => lead.to_vec(),
            _ => group,
        })
    }

    /// `expr` with every `ColId` leaf substituted by `ColRef(its slot)`.
    pub(crate) fn resolve(&self, expr: &HirExpr) -> Result<BoundExpr, GnitzSqlError> {
        expr.try_rebuild(&mut |id| self.slot(*id).map(BoundExpr::ColRef))
    }

    /// [`Self::resolve`] for each conjunct.
    pub(crate) fn resolve_preds(&self, preds: &[HirExpr]) -> Result<Vec<BoundExpr>, GnitzSqlError> {
        preds.iter().map(|p| self.resolve(p)).collect()
    }

    /// This frame's schema under `root`'s numbering: its unnamed key slots, then
    /// `root`'s columns in `root` order, each unnamed payload slot staying where
    /// the payload order puts it. The regions are this frame's own, so the engine
    /// relabels a batch rather than moving a column. Refused for a `root` that
    /// would move a payload column.
    pub(crate) fn schema_in_order(&self, root: impl IntoIterator<Item = ColId>) -> Result<Schema, GnitzSqlError> {
        let internal = |m: &str| GnitzSqlError::Internal(format!("output order: {m}"));
        let is_pk = |s: usize| self.schema.is_pk_col(s);
        let mut rooted = vec![false; self.layout.len()];
        let mut named = Vec::new();
        for id in root {
            let slot = self.slot(id)?;
            if std::mem::replace(&mut rooted[slot], true) {
                return Err(internal("a column is named twice"));
            }
            named.push(slot);
        }
        let rooted = &rooted;
        let unnamed = |pk: bool| (0..rooted.len()).filter(move |&s| !rooted[s] && is_pk(s) == pk);
        let mut order: Vec<usize> = unnamed(true).collect();
        let mut hidden_payload = unnamed(false).peekable();
        for slot in named {
            if !is_pk(slot) {
                order.extend(std::iter::from_fn(|| hidden_payload.next_if(|&h| h < slot)));
            }
            order.push(slot);
        }
        order.extend(hidden_payload);
        if !order.iter().filter(|&&s| !is_pk(s)).is_sorted() {
            return Err(internal("the root would move a payload column"));
        }
        let mut moved_to = vec![0u32; order.len()];
        for (to, &from) in order.iter().enumerate() {
            moved_to[from] = to as u32;
        }
        Schema::from_parts(
            order.iter().map(|&s| self.schema.columns[s].clone()).collect(),
            self.schema.pk_cols.iter().map(|&s| moved_to[s as usize]).collect(),
        )
        .map_err(|e| GnitzSqlError::Rejected(format!("output schema: {e}")))
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
            // The expression's error: the schema's would name a hidden column.
            crate::ir::check_decimal_scale(e.out.def.ty.scale)?;
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
