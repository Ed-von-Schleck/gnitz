//! HIR → ad-hoc read lowering: a bound query that reads one relation, as the
//! bound, predicate and reply of one read of it. The filters, projections and
//! CTE references over the relation compose by substitution into the tree the
//! flat query binds to, which the rows reply and [`super::fold`] then lower.

use super::super::physical::{self, Frame, Slot};
use super::super::{as_col, hircol_of, AggCol, ColId, HirCol, HirExpr, ProjEntry, RelExpr};
use super::fold::{distinct_fold, fold, FoldPieces};
use crate::error::{derivation, GnitzSqlError};
use crate::ir::{BExpr, BoundExpr};
use crate::project::{projection_program, ProjItem};
use crate::tail::wire_keys;
use gnitz_core::RelDescriptor;
use gnitz_expr::SchemaFacts;
use gnitz_wire::{ColumnDef, OrderKey};
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;

/// What an ad-hoc SELECT reads.
pub(crate) enum AdhocRead {
    /// Nothing: the one row these constant items compute, a hidden one being an
    /// ORDER BY key.
    Constant(Vec<(BoundExpr, ColumnDef)>),
    /// One relation.
    Relation {
        desc: Arc<RelDescriptor>,
        /// The relation as the query names it.
        name: String,
        /// The WHERE over `desc`'s schema.
        conjuncts: Vec<BoundExpr>,
        shape: AdhocShape,
    },
}

pub(crate) enum AdhocShape {
    Rows(AdhocRows),
    /// The fold, and the finalize item each ORDER BY expression key sorts on.
    Fold(Box<FoldPieces>, Vec<usize>),
}

/// The ceiling on the column references one projection's composed expressions
/// hold: a CTE chain naming each predecessor's column twice is `2^n` of them from
/// `O(n)` bytes of SQL, and the planner runs in the caller's own process.
const MAX_EXPANDED_REFS: usize = 10_000;

/// The filters, projections and CTE references standing over one node, composed
/// into expressions over that node's columns.
struct Linear<'a> {
    /// The node they stand over: the `Get` of the relation read, the `Unit` of a
    /// body reading none, or a `Reduce`.
    base: &'a Rc<RelExpr>,
    /// Over `base`'s columns, innermost filter first.
    preds: Vec<HirExpr>,
    /// Each output column as an expression over `base`'s columns.
    map: HashMap<ColId, HirExpr>,
}

impl Linear<'_> {
    fn subst(&self, e: &HirExpr) -> Result<HirExpr, GnitzSqlError> {
        e.try_rebuild(&mut |id| {
            self.map
                .get(id)
                .cloned()
                .ok_or_else(|| GnitzSqlError::Internal("a read references a column its source does not expose".into()))
        })
    }

    fn items(&self, items: &[ProjEntry]) -> Result<Vec<ProjEntry>, GnitzSqlError> {
        items
            .iter()
            .map(|it| {
                Ok(ProjEntry {
                    expr: self.subst(&it.expr)?,
                    out: it.out.clone(),
                })
            })
            .collect()
    }

    /// The relation read, when `base` is one.
    fn source(&self) -> Result<(&Arc<RelDescriptor>, &[HirCol]), GnitzSqlError> {
        match self.base.as_ref() {
            RelExpr::Get { desc, cols } => Ok((desc, cols)),
            RelExpr::Reduce { .. } => Err(derivation("an aggregate over a grouped CTE")),
            _ => Err(GnitzSqlError::Rejected(
                "SELECT without FROM: aggregates and DISTINCT are not supported".into(),
            )),
        }
    }
}

/// `rel` down to the first node that is no filter, projection or CTE reference.
fn linear(rel: &Rc<RelExpr>) -> Result<Linear<'_>, GnitzSqlError> {
    match rel.as_ref() {
        RelExpr::Get { .. } | RelExpr::Unit | RelExpr::Reduce { .. } => Ok(Linear {
            base: rel,
            preds: Vec::new(),
            map: rel.cols().iter().map(|c| (c.id, BExpr::ColRef(c.id))).collect(),
        }),
        RelExpr::Filter { input, preds } => {
            let mut l = linear(input)?;
            for p in preds {
                let p = l.subst(p)?;
                l.preds.push(p);
            }
            Ok(l)
        }
        RelExpr::Project { input, items } => {
            let l = linear(input)?;
            let mut refs = 0usize;
            let mut map = HashMap::with_capacity(items.len());
            for it in items {
                let e = l.subst(&it.expr)?;
                e.for_each_ref(&mut |_| refs += 1);
                map.insert(it.out.id, e);
            }
            if refs > MAX_EXPANDED_REFS {
                return Err(GnitzSqlError::Rejected(format!(
                    "the query's CTEs expand to {refs} column references, over the limit of {MAX_EXPANDED_REFS}.\n\
                     CREATE VIEW <name> AS <the CTE body> — the engine maintains it incrementally — then SELECT from it."
                )));
            }
            Ok(Linear { map, ..l })
        }
        RelExpr::Alias { input, cols } => {
            let l = linear(input)?;
            let map = input
                .cols()
                .iter()
                .zip(cols)
                .map(|(i, o)| (o.id, l.map[&i.id].clone()))
                .collect();
            Ok(Linear { map, ..l })
        }
        RelExpr::Join { .. } => Err(derivation("JOIN")),
        RelExpr::Distinct { .. } => Err(derivation("DISTINCT CTE")),
        RelExpr::SetOp { .. } => Err(derivation("set operation")),
        RelExpr::TopN { .. } => Err(GnitzSqlError::Internal("an ad-hoc read bound a top-N".into())),
    }
}

/// Lower a bound ad-hoc body; `placed` is the item each ORDER BY expression key
/// sorts on, and `names` the catalog names the bind read.
pub(crate) fn lower_read(
    rel: &Rc<RelExpr>,
    placed: Vec<usize>,
    names: &[(u64, String)],
) -> Result<AdhocRead, GnitzSqlError> {
    let internal = || GnitzSqlError::Internal("an ad-hoc read body is not a projection".into());
    let (lin, shape) = match rel.as_ref() {
        RelExpr::Distinct { input } => {
            let RelExpr::Project { input: base, items } = input.as_ref() else {
                return Err(internal());
            };
            let lin = linear(base)?;
            let (desc, source) = lin.source()?;
            let pieces = distinct_fold(Frame::scan(desc, source), &lin.items(items)?)?;
            (lin, AdhocShape::Fold(Box::new(pieces), placed))
        }
        RelExpr::Project { input, items } => {
            let top = linear(input)?;
            match top.base.as_ref() {
                RelExpr::Unit => return constant(&top, items),
                // Everything over the reduce is its HAVING and its finalize projection.
                RelExpr::Reduce { input, group_cols, aggs } => {
                    let lin = linear(input)?;
                    let pieces = flat_reduce(&lin, input, group_cols, aggs, &top.preds, &top.items(items)?)?;
                    (lin, AdhocShape::Fold(Box::new(pieces), placed))
                }
                _ => {
                    let (desc, source) = top.source()?;
                    let rows = reply_rows(desc, source, top.items(items)?, placed)?;
                    (top, AdhocShape::Rows(rows))
                }
            }
        }
        _ => return Err(internal()),
    };
    let (desc, source) = lin.source()?;
    let conjuncts = Frame::scan(desc, source).resolve_preds(&lin.preds)?;
    let name = names.iter().find(|(tid, _)| *tid == desc.tid).map(|(_, n)| n.clone());
    Ok(AdhocRead::Relation {
        desc: Arc::clone(desc),
        name: name.ok_or_else(internal)?,
        conjuncts,
        shape,
    })
}

/// The constant row `items` compute over no relation.
fn constant(lin: &Linear<'_>, items: &[ProjEntry]) -> Result<AdhocRead, GnitzSqlError> {
    if !lin.preds.is_empty() {
        return Err(crate::error::unsupported_clause("SELECT without FROM", "WHERE"));
    }
    let mut no_column = |_: &ColId| Err(GnitzSqlError::Internal("a constant item references a column".into()));
    let items = lin
        .items(items)?
        .into_iter()
        .map(|it| Ok((it.expr.try_rebuild(&mut no_column)?, it.out.def)))
        .collect::<Result<_, GnitzSqlError>>()?;
    Ok(AdhocRead::Constant(items))
}

/// `Project(having?(Reduce))` over `lin`'s relation, as the flat query binds it:
/// a reduce input column that copies a source column is read in place, and one
/// that computes is a column of the pre-map under the reduce.
fn flat_reduce(
    lin: &Linear<'_>,
    input: &Rc<RelExpr>,
    group_cols: &[ColId],
    aggs: &[AggCol],
    having: &[HirExpr],
    items: &[ProjEntry],
) -> Result<FoldPieces, GnitzSqlError> {
    let (desc, source) = lin.source()?;
    // What the reduce reads, in the order its own pre-map lists it.
    let reads: Vec<ColId> = match input.as_ref() {
        RelExpr::Project { items, .. } => items.iter().map(|it| it.out.id).collect(),
        _ => {
            let mut reads = group_cols.to_vec();
            for id in aggs.iter().filter_map(|c| c.arg) {
                if !reads.contains(&id) {
                    reads.push(id);
                }
            }
            reads
        }
    };
    let in_cols = input.cols();
    let mut copies: HashMap<ColId, ColId> = HashMap::new();
    let mut pre: Vec<ProjEntry> = Vec::new();
    let mut computed = 0;
    for r in reads {
        let e = &lin.map[&r];
        match as_col(e) {
            Some(src) => {
                copies.insert(r, src);
                if !pre.iter().any(|p| p.out.id == src) {
                    pre.push(RelExpr::passthrough_item(hircol_of(source, src).clone()));
                }
            }
            None => {
                let def = &hircol_of(&in_cols, r).def;
                let def = ColumnDef::typed(format!("_pre{computed}"), def.ty, true).hidden();
                computed += 1;
                pre.push(ProjEntry {
                    expr: e.clone(),
                    out: HirCol::new(r, def),
                });
            }
        }
    }
    let at = |id: ColId| copies.get(&id).copied().unwrap_or(id);
    let rename = |e: &HirExpr| e.rebuild(&mut |id| BExpr::ColRef(at(*id)));
    let group: Vec<ColId> = group_cols.iter().map(|&g| at(g)).collect();
    let aggs: Vec<AggCol> = aggs
        .iter()
        .map(|c| AggCol { arg: c.arg.map(at), ..c.clone() })
        .collect();
    let having: Vec<HirExpr> = having.iter().map(rename).collect();
    let items: Vec<ProjEntry> = items
        .iter()
        .map(|it| ProjEntry {
            expr: rename(&it.expr),
            out: it.out.clone(),
        })
        .collect();
    fold(
        Frame::scan(desc, source),
        (computed > 0).then_some(&pre[..]),
        &group,
        &aggs,
        &having,
        &items,
    )
}

/// A rows read's reply items, placed: what [`AdhocRows::reply`] numbers.
pub(crate) struct AdhocRows {
    src: Frame,
    /// The items' output columns, in item order: the SELECT list, then a hidden column per
    /// ORDER BY expression no item already computes.
    cols: Vec<HirCol>,
    /// The items as the projection's slots, the source PK pinned in front, and each
    /// slot's identity and definition.
    proj: Vec<ProjItem>,
    layout: Vec<Slot>,
    /// The item each ORDER BY expression key sorts on.
    placed: Vec<usize>,
}

/// The rows reply a projection over a relation produces.
pub(crate) struct RowsReply {
    /// The reply's regions under the SELECT list's numbering: the key columns no
    /// item names, hidden, then the items — a key column at the first item that
    /// copies it, every other item a payload column.
    pub(crate) schema: Arc<gnitz_core::Schema>,
    /// The program filling the reply's payload from a source row; `None` when the
    /// reply's regions are the relation's own.
    pub(crate) program: Option<gnitz_expr::LogicalProgram>,
    /// The ORDER BY keys over `schema`.
    pub(crate) order: Vec<OrderKey>,
    /// `order` as the worker's sink numbers its input: the program's output, key
    /// region first, or the relation where there is no program.
    pub(crate) sink_order: Vec<OrderKey>,
    /// Whether `order` ascends a leading run of the relation's PK columns, so
    /// rows in store order are already in it.
    pub(crate) pk_ordered: bool,
    /// `(relation column, reply column)` for each relation column an item copies
    /// verbatim, at the first item that does.
    pub(crate) copied: Vec<(u32, u32)>,
}

/// The reply items of `written` over a read of `desc`, whose `Get` columns are `source`;
/// `placed` is the item each ORDER BY expression key sorts on.
pub(crate) fn reply_rows(
    desc: &Arc<RelDescriptor>,
    source: &[HirCol],
    written: Vec<ProjEntry>,
    placed: Vec<usize>,
) -> Result<AdhocRows, GnitzSqlError> {
    // A hidden item is an ORDER BY key; one an earlier item already computes sorts on it.
    let mut items: Vec<ProjEntry> = Vec::with_capacity(written.len());
    let mut at = Vec::with_capacity(written.len());
    for mut it in written {
        let twin = match it.out.def.is_hidden {
            true => items.iter().position(|p| p.expr == it.expr),
            false => None,
        };
        at.push(twin.unwrap_or(items.len()));
        if twin.is_none() {
            // A hidden copy of a key column is the key riding hidden, under its own definition.
            let col = source.iter().position(|c| as_col(&it.expr) == Some(c.id));
            if let Some(col) = col.filter(|&c| it.out.def.is_hidden && desc.schema.is_pk_col(c)) {
                it.out.def = source[col].def.clone().hidden();
            }
            items.push(it);
        }
    }
    let src = Frame::scan(desc, source);
    let (proj, layout) = physical::project_slots(&items, &src)?;
    Ok(AdhocRows {
        src,
        cols: items.into_iter().map(|it| it.out).collect(),
        proj,
        layout,
        placed: placed.into_iter().map(|p| at[p]).collect(),
    })
}

impl AdhocRows {
    /// The reply, ordered by `keys`.
    pub(crate) fn reply(self, keys: &[crate::tail::OrderKey<'_>]) -> Result<RowsReply, GnitzSqlError> {
        let AdhocRows { src, cols, proj, layout, placed } = self;
        let item_order = wire_keys(keys, cols.iter().map(|c| &c.def), placed)?;
        let k = src.schema.pk_cols.len();
        // The relation's payload columns, each copied in place at its own type: the reply's
        // regions are the relation's.
        let payload = proj[k..]
            .iter()
            .zip(&layout[k..])
            .map(|(item, (_, def))| (item.passthrough_src(), def.ty));
        let unmapped = payload.eq(src.schema.payload_columns().map(|(_, ci, col)| (Some(ci), col.ty)));
        let program = match unmapped {
            true => None,
            false => {
                let defs: Vec<ColumnDef> = layout[k..].iter().map(|(_, def)| def.clone()).collect();
                Some(projection_program(&proj[k..], &defs, &src.schema)?)
            }
        };
        let out = Frame::new(layout, k)?;
        let visible = cols.iter().filter(|c| !c.def.is_hidden).map(|c| c.id);
        let (schema, column_of) = out.schema_in_order(visible)?;
        // Each key with its slot of the projection's output.
        let slots = item_order
            .into_iter()
            .map(|key| Ok((out.slot(cols[key.col as usize].id)?, key)))
            .collect::<Result<Vec<_>, GnitzSqlError>>()?;
        let source_col = |slot: usize| proj[slot].passthrough_src();
        let numbered = |col: &dyn Fn(usize) -> usize| -> Vec<OrderKey> {
            slots
                .iter()
                .map(|&(slot, key)| OrderKey { col: col(slot) as u16, ..key })
                .collect()
        };
        let mut copied: Vec<(u32, u32)> = Vec::new();
        for (slot, item) in proj.iter().enumerate() {
            let src = item.passthrough_src().map(|s| s as u32);
            if let Some(src) = src.filter(|s| copied.iter().all(|(c, _)| c != s)) {
                copied.push((src, column_of[slot]));
            }
        }
        Ok(RowsReply {
            order: numbered(&|slot| column_of[slot] as usize),
            // The worker's sink numbers its input: the program's output, or the relation.
            sink_order: match unmapped {
                true => numbered(&|slot| source_col(slot).expect("an unmapped reply copies every column")),
                false => numbered(&|slot| slot),
            },
            pk_ordered: slots.len() <= k
                && slots
                    .iter()
                    .zip(&src.schema.pk_cols)
                    .all(|(&(slot, key), &pk)| !key.desc && source_col(slot) == Some(pk as usize)),
            copied,
            program,
            schema: Arc::new(schema),
        })
    }
}

#[cfg(test)]
#[path = "tests/read.rs"]
mod tests;
