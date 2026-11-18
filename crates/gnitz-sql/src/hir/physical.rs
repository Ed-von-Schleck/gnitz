//! The positional pass, run at lowering. Assigns physical column positions to
//! the layout-free logical IR: [`Frame`] substitutes each `ColId` leaf with a
//! `ColRef(position)` against a node's column layout, and
//! [`physicalize_projection`] physicalizes a projection: each slot a resolved
//! expression, a bare `ColRef` being a copy, under the PK-front convention, whose
//! single home is `place_pk_front`.

use super::{ColId, HirCol, HirExpr, ProjEntry};
use crate::error::GnitzSqlError;
use crate::ir::{BExpr, BoundExpr};
use crate::project::{compute_map, payload_program};
use gnitz_core::{RelDescriptor, Schema};
use gnitz_wire::{ColumnDef, ComputeMap};
use std::sync::Arc;

/// A frame slot: the identity that names it, if any, and its column.
pub(crate) type Slot = (Option<ColId>, ColumnDef);

impl HirCol {
    /// This column as a slot its identity names.
    pub(crate) fn slot(&self) -> Slot {
        (Some(self.id), self.def.clone())
    }
}

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
    pub(crate) fn new(slots: impl IntoIterator<Item = Slot>, npk: usize) -> Result<Frame, GnitzSqlError> {
        let (layout, columns): (Vec<_>, Vec<_>) = slots.into_iter().unzip();
        let schema = Schema::from_parts(columns, gnitz_expr::leading_pk(npk))
            .map_err(|e| GnitzSqlError::rejected(format!("output schema: {e}")))?;
        Ok(Frame { layout, schema: Arc::new(schema) })
    }

    /// `pk_cols` as an identity-free leading key region, then `payload`.
    pub(crate) fn keyed(
        pk_cols: Vec<ColumnDef>,
        payload: impl IntoIterator<Item = Slot>,
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

    /// The group list to send a reduce, top-N or window grouped by `ids`' slots: SQL's
    /// GROUP BY is a set, so a slot named twice is sent once and a permutation of
    /// leading PK columns is sent in PK order.
    pub(crate) fn reduce_group(&self, ids: &[ColId]) -> Result<Vec<u32>, GnitzSqlError> {
        let mut group: Vec<u32> = Vec::with_capacity(ids.len());
        for slot in self.slots(ids.iter().copied())? {
            if !group.contains(&(slot as u32)) {
                group.push(slot as u32);
            }
        }
        let lead = self.schema.pk_cols().get(..group.len());
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
    /// would move a payload column. A `root` that keeps every slot in place returns
    /// this frame's own schema.
    pub(crate) fn schema_in_order(
        &self,
        root: impl IntoIterator<Item = ColId>,
    ) -> Result<(Arc<Schema>, Vec<u32>), GnitzSqlError> {
        let internal = |m: &str| GnitzSqlError::Internal(format!("output order: {m}"));
        let is_pk = |s: usize| self.schema.layout().is_pk_col(s);
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
        if order.iter().copied().eq(0..order.len()) {
            return Ok((Arc::clone(&self.schema), (0..order.len() as u32).collect()));
        }
        let mut moved_to = vec![0u32; order.len()];
        for (to, &from) in order.iter().enumerate() {
            moved_to[from] = to as u32;
        }
        let pk: Vec<u32> = self.schema.pk_cols().iter().map(|&s| moved_to[s as usize]).collect();
        let schema = Schema::from_parts(order.iter().map(|&s| self.schema.columns()[s].clone()).collect(), &pk)
            .map_err(|e| GnitzSqlError::rejected(format!("output schema: {e}")))?;
        Ok((Arc::new(schema), moved_to))
    }

    /// This frame under the identities of `items`, a projection that moves no data,
    /// or `None` unless each item is a bare column and none reads a column twice. A
    /// slot no item reads loses its identity.
    pub(crate) fn relabeled(&self, items: &[ProjEntry]) -> Result<Option<Frame>, GnitzSqlError> {
        let mut layout = vec![None; self.layout.len()];
        let mut columns: Option<Vec<ColumnDef>> = None;
        for it in items {
            let Some(src) = it.expr.as_col() else {
                return Ok(None);
            };
            let slot = self.slot(src)?;
            if layout[slot].is_some() {
                return Ok(None);
            }
            layout[slot] = Some(it.out.id);
            if self.schema.columns()[slot] != it.out.def {
                columns.get_or_insert_with(|| self.schema.columns().to_vec())[slot] = it.out.def.clone();
            }
        }
        let schema = match columns {
            None => Arc::clone(&self.schema),
            Some(columns) => Arc::new(
                Schema::from_parts(columns, self.schema.pk_cols())
                    .map_err(|e| GnitzSqlError::Internal(format!("relabeled schema: {e}")))?,
            ),
        };
        Ok(Some(Frame { layout, schema }))
    }

    /// [`Self::relabeled`] as stored under `items`' written order, or `None` where
    /// [`Self::schema_in_order`] would move a column: the visible items must name
    /// payload columns in payload order. Every slot no item names is hidden.
    pub(crate) fn relabeled_in_order(&self, items: &[ProjEntry]) -> Result<Option<Frame>, GnitzSqlError> {
        let Some(Frame { layout, schema }) = self.relabeled(items)? else {
            return Ok(None);
        };
        let payload = (items.iter())
            .filter(|it| !it.out.def.is_hidden)
            .filter_map(|it| it.expr.as_col())
            .map(|c| self.slot(c))
            .collect::<Result<Vec<_>, _>>()?;
        if !payload.iter().filter(|&&s| !schema.layout().is_pk_col(s)).is_sorted() {
            return Ok(None);
        }
        let unnamed = |s: &usize| layout[*s].is_none() && !schema.columns()[*s].is_hidden;
        if !(0..layout.len()).any(|s| unnamed(&s)) {
            return Ok(Some(Frame { layout, schema }));
        }
        let mut columns = schema.columns().to_vec();
        for s in (0..layout.len()).filter(unnamed) {
            columns[s].is_hidden = true;
        }
        let schema = Schema::from_parts(columns, schema.pk_cols())
            .map_err(|e| GnitzSqlError::Internal(format!("relabeled schema: {e}")))?;
        Ok(Some(Frame { layout, schema: Arc::new(schema) }))
    }
}

/// A projection's slots over `input`, in output order — each entry resolved and
/// classified, then the input's PK pinned to the leading slots — as the emission
/// items and the output frame.
pub(crate) fn physicalize_projection(
    items: &[ProjEntry],
    input: &Frame,
) -> Result<(Vec<BoundExpr>, Frame), GnitzSqlError> {
    let mut slots = items
        .iter()
        .map(|e| {
            // The expression's error: the schema's would name a hidden column.
            crate::ir::check_decimal_scale(e.out.def.ty.scale)?;
            Ok((input.resolve(&e.expr)?, Some(e.out.id), e.out.def.clone()))
        })
        .collect::<Result<Vec<_>, GnitzSqlError>>()?;
    place_pk_front(&mut slots, &input.schema);
    // Each input column copied in place under its own definition: the output schema
    // is the input's.
    let columns = input.schema.columns();
    let identity = slots.len() == columns.len()
        && slots
            .iter()
            .enumerate()
            .all(|(i, (item, _, def))| item.as_col() == Some(i) && *def == columns[i]);
    if identity {
        let (proj, layout) = slots.into_iter().map(|(item, id, _)| (item, id)).unzip();
        let schema = Arc::clone(&input.schema);
        return Ok((proj, Frame { layout, schema }));
    }
    let (proj, cols): (Vec<BoundExpr>, Vec<Slot>) = slots.into_iter().map(|(item, id, def)| (item, (id, def))).unzip();
    Ok((proj, Frame::new(cols, input.schema.pk_cols().len())?))
}

/// [`physicalize_projection`] as the map producing it: `None` when the
/// projection is `input` itself. The engine carries the key region verbatim.
pub(crate) fn projection_map(items: &[ProjEntry], input: &Frame) -> Result<(Option<ComputeMap>, Frame), GnitzSqlError> {
    let (items, out) = physicalize_projection(items, input)?;
    let input = &input.schema;
    let identity =
        items.len() == input.columns().len() && items.iter().enumerate().all(|(i, it)| it.as_col() == Some(i));
    let map = match identity {
        true => None,
        false => Some(compute_map(payload_program(&items, &out.schema, input)?, &out.schema)),
    };
    Ok((map, out))
}

/// Pin the source PK to slots `0..k` in PK-list order: each PK column's first
/// pass-through moves there, and
/// one nothing passes through is prepended hidden. A hidden key slot carries the
/// source's definition of its column.
fn place_pk_front(slots: &mut Vec<(BoundExpr, Option<ColId>, ColumnDef)>, source: &Schema) {
    for (target, &pk) in source.pk_cols().iter().enumerate() {
        let pk = pk as usize;
        let slot = match slots.iter().position(|(item, ..)| item.as_col() == Some(pk)) {
            Some(pos) => {
                let (item, id, def) = slots.remove(pos);
                let def = match def.is_hidden {
                    true => source.columns()[pk].clone().hidden(),
                    false => def,
                };
                (item, id, def)
            }
            None => (BExpr::ColRef(pk), None, source.columns()[pk].clone().hidden()),
        };
        slots.insert(target, slot);
    }
}

#[cfg(test)]
#[path = "tests/physical.rs"]
mod tests;
