//! HIR → ad-hoc read lowering: a bound query that reads one relation, as the
//! bound, predicate and reply of one read of it. The filters, projections and
//! CTE references over the relation compose by substitution into the tree the
//! flat query binds to, which the rows reply and [`super::fold`] then lower.

use super::super::physical::{self, Frame};
use super::super::{AggCol, BoundRef, ColId, HirCol, HirExpr, OrderCol, ProjEntry, RelExpr, hircol_of, reduce_reads};
use super::fold::{FoldPieces, distinct_fold, fold};
use crate::error::{GnitzSqlError, derivation};
use crate::ir::{BExpr, BoundExpr};
use crate::project::payload_program;
use gnitz_core::{RelDescriptor, Schema};
use gnitz_wire::OrderKey;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;

/// What an ad-hoc SELECT reads.
pub(crate) enum AdhocRead {
    /// Nothing: the one row these items compute over the ground row, a hidden one
    /// being an ORDER BY key.
    Constant(RowsReply),
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
    Rows(RowsReply),
    Fold {
        pieces: Box<FoldPieces>,
        /// Nothing in the fold's layout tells a DISTINCT from a zero-aggregate GROUP BY.
        is_distinct: bool,
    },
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
            // A DISTINCT over no relation is a set already, so only an aggregate meets it.
            _ => Err(GnitzSqlError::Rejected(
                "an aggregate over a SELECT without FROM is not supported".into(),
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
        RelExpr::Distinct { input } => {
            linear(input)?;
            Err(derivation("DISTINCT CTE"))
        }
        RelExpr::SetOp { .. } => Err(derivation("set operation")),
        RelExpr::Ordered { .. } => Err(GnitzSqlError::Internal(
            "an ad-hoc read bound an ordered operator".into(),
        )),
    }
}

/// Lower a bound ad-hoc body ordered by `order`, over its output columns; `refs` are
/// the catalog relations the bind read.
pub(crate) fn lower_read(
    rel: &Rc<RelExpr>,
    order: Vec<OrderCol>,
    refs: Vec<BoundRef>,
) -> Result<AdhocRead, GnitzSqlError> {
    let internal = || GnitzSqlError::Internal("an ad-hoc read body is not a projection".into());
    let (lin, shape) = match rel.as_ref() {
        RelExpr::Distinct { input } => {
            let RelExpr::Project { input: base, items } = input.as_ref() else {
                return Err(internal());
            };
            let lin = linear(base)?;
            let (desc, source) = lin.source()?;
            let pieces = distinct_fold(Frame::scan(desc, source), lin.items(items)?, order)?;
            let shape = AdhocShape::Fold {
                pieces: Box::new(pieces),
                is_distinct: true,
            };
            (lin, shape)
        }
        RelExpr::Project { input, items } => {
            let top = linear(input)?;
            match top.base.as_ref() {
                RelExpr::Unit => return constant(&top, items, order),
                // Everything over the reduce is its HAVING and its finalize projection.
                RelExpr::Reduce { input, group_cols, aggs } => {
                    let lin = linear(input)?;
                    let pieces = flat_reduce(&lin, input, group_cols, aggs, &top.preds, top.items(items)?, order)?;
                    let shape = AdhocShape::Fold {
                        pieces: Box::new(pieces),
                        is_distinct: false,
                    };
                    (lin, shape)
                }
                _ => {
                    let (desc, source) = top.source()?;
                    let rows = reply_rows(Frame::scan(desc, source), top.items(items)?, order)?;
                    (top, AdhocShape::Rows(rows))
                }
            }
        }
        _ => return Err(internal()),
    };
    let (desc, source) = lin.source()?;
    let conjuncts = Frame::scan(desc, source).resolve_preds(&lin.preds)?;
    let name = refs
        .into_iter()
        .find(|r| r.tid == desc.tid)
        .map(|r| r.name)
        .ok_or_else(|| GnitzSqlError::Internal("an ad-hoc read's relation is not among the bound references".into()))?;
    Ok(AdhocRead::Relation {
        desc: Arc::clone(desc),
        name,
        conjuncts,
        shape,
    })
}

/// The constant row `items` compute over the global ground row: one hidden
/// `_group_pk`, keyed by the global group key.
fn constant(lin: &Linear<'_>, items: &[ProjEntry], order: Vec<OrderCol>) -> Result<AdhocRead, GnitzSqlError> {
    if !lin.preds.is_empty() {
        return Err(GnitzSqlError::Rejected(
            "a WHERE over a SELECT without FROM is not supported".into(),
        ));
    }
    let ground = Frame::keyed(vec![crate::agg::group_pk_def()], [])?;
    Ok(AdhocRead::Constant(reply_rows(ground, lin.items(items)?, order)?))
}

/// `Project(having?(Reduce))` over `lin`'s relation, as the flat query binds it:
/// each column the reduce reads, as `lin` composes it over the relation.
fn flat_reduce(
    lin: &Linear<'_>,
    input: &Rc<RelExpr>,
    group_cols: &[ColId],
    aggs: &[AggCol],
    having: &[HirExpr],
    items: Vec<ProjEntry>,
    order: Vec<OrderCol>,
) -> Result<FoldPieces, GnitzSqlError> {
    let (desc, source) = lin.source()?;
    let in_cols = input.cols();
    let pre: Vec<ProjEntry> = (reduce_reads(group_cols, aggs).into_iter())
        .map(|r| ProjEntry {
            expr: lin.map[&r].clone(),
            out: hircol_of(&in_cols, r).clone(),
        })
        .collect();
    fold(Frame::scan(desc, source), pre, group_cols, aggs, having, items, order)
}

/// The rows reply a projection over a source produces.
pub(crate) struct RowsReply {
    /// The schema of the source the items read: a relation, a fold's partials, or the
    /// ground row.
    pub(crate) src: Arc<Schema>,
    /// The reply's regions under the SELECT list's numbering: the key columns no
    /// item names, hidden, then the items — a key column at the first item that
    /// copies it, every other item a payload column.
    pub(crate) schema: Arc<Schema>,
    /// The program filling the reply's payload from a source row; `None` when the
    /// reply's regions are the source's own.
    pub(crate) program: Option<gnitz_expr::LogicalProgram>,
    /// The ORDER BY keys over `schema`.
    pub(crate) order: Vec<OrderKey>,
    /// `order` as the worker's sink numbers its input: the program's output, key
    /// region first, or the relation where there is no program.
    pub(crate) sink_order: Vec<OrderKey>,
    /// Whether `order` ascends a leading run of the source's PK columns, so a walk in
    /// PK order already yields it.
    pub(crate) pk_ordered: bool,
    /// `(relation column, reply column)` for each relation column an item copies
    /// verbatim, at the first item that does.
    pub(crate) copied: Vec<(u32, u32)>,
}

/// The reply items of `written` over `src`, ordered by `order` over `written`'s columns.
pub(crate) fn reply_rows(
    src: Frame,
    written: Vec<ProjEntry>,
    mut order: Vec<OrderCol>,
) -> Result<RowsReply, GnitzSqlError> {
    // A hidden item is an ORDER BY key; one an earlier item already computes sorts on it.
    let mut items: Vec<ProjEntry> = Vec::with_capacity(written.len());
    for it in written {
        let twin = match it.out.def.is_hidden {
            true => items.iter().find(|p| p.expr == it.expr),
            false => None,
        };
        match twin {
            Some(twin) => order
                .iter_mut()
                .filter(|k| k.col == it.out.id)
                .for_each(|k| k.col = twin.out.id),
            None => items.push(it),
        }
    }
    let (proj, out) = physical::physicalize_projection(&items, &src)?;
    let src = src.schema;
    let k = src.pk_cols().len();
    // The relation's payload columns, each copied in place at its own type: the reply's
    // regions are the relation's.
    let payload = proj[k..]
        .iter()
        .zip(&out.schema.columns()[k..])
        .map(|(item, def)| (item.as_col(), def.ty));
    let unmapped = payload.eq(src.payload_columns().map(|(_, ci, col)| (Some(ci), col.ty)));
    let program = (!unmapped)
        .then(|| payload_program(&proj, &out.schema, &src))
        .transpose()?;
    let visible = items.iter().filter(|c| !c.out.def.is_hidden).map(|c| c.out.id);
    let (schema, column_of) = out.schema_in_order(visible)?;
    // Each key with its slot of the projection's output.
    let slots = order
        .into_iter()
        .map(|key| Ok((out.slot(key.col)?, key)))
        .collect::<Result<Vec<_>, GnitzSqlError>>()?;
    let source_col = |slot: usize| proj[slot].as_col();
    let numbered = |col: &dyn Fn(usize) -> usize| -> Vec<OrderKey> {
        slots
            .iter()
            .map(|&(slot, key)| OrderKey {
                col: col(slot) as u16,
                desc: key.desc,
                nulls_first: key.nulls_first,
            })
            .collect()
    };
    let mut copied: Vec<(u32, u32)> = Vec::with_capacity(proj.len());
    for (slot, item) in proj.iter().enumerate() {
        let src = item.as_col().map(|s| s as u32);
        if let Some(src) = src.filter(|s| copied.iter().all(|(c, _)| c != s)) {
            copied.push((src, column_of[slot]));
        }
    }
    // A walk in PK order yields keys ascending a leading run of the PK columns.
    let pk_ordered = slots.len() <= k
        && slots
            .iter()
            .zip(src.pk_cols())
            .all(|(&(slot, key), &pk)| !key.desc && source_col(slot) == Some(pk as usize));
    Ok(RowsReply {
        order: numbered(&|slot| column_of[slot] as usize),
        // The worker's sink numbers its input: the program's output, or the relation.
        sink_order: match unmapped {
            true => numbered(&|slot| source_col(slot).expect("an unmapped reply copies every column")),
            false => numbered(&|slot| slot),
        },
        pk_ordered,
        copied,
        program,
        schema,
        src,
    })
}

#[cfg(test)]
#[path = "tests/read.rs"]
mod tests;
