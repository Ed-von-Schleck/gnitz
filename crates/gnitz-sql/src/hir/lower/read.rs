//! HIR → ad-hoc read lowering: a bound query that reads one relation, as the
//! bound, predicate and reply of one read of it. The filters, projections and
//! CTE references over the relation compose by substitution into the tree the
//! flat query binds to, which the rows reply and [`super::fold`] then lower.

use super::super::physical::{self, Frame};
use super::super::{as_col, hircol_of, AggCol, AggCols, ColId, HirCol, HirExpr, ProjEntry, RelExpr};
use super::fold::{lower_fold, FoldPieces};
use crate::error::{derivation, GnitzSqlError};
use crate::ir::{BExpr, BoundExpr};
use crate::project::ProjItem;
use gnitz_core::RelDescriptor;
use gnitz_wire::ColumnDef;
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

/// A rows read's reply items.
pub(crate) struct AdhocRows {
    /// The source PK hidden in front, then the SELECT list, then a hidden item per
    /// ORDER BY expression no item already computes.
    pub items: Vec<ProjItem>,
    /// The output column of each item.
    pub cols: Vec<ColumnDef>,
    /// The item each ORDER BY expression key sorts on.
    pub placed: Vec<usize>,
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
    let fold = |body: RelExpr, placed| -> Result<AdhocShape, GnitzSqlError> {
        Ok(AdhocShape::Fold(Box::new(lower_fold(&body)?), placed))
    };
    let (lin, shape) = match rel.as_ref() {
        RelExpr::Distinct { input } => {
            let RelExpr::Project { input: base, items } = input.as_ref() else {
                return Err(internal());
            };
            let lin = linear(base)?;
            lin.source()?;
            let input = RelExpr::project(Rc::clone(lin.base), lin.items(items)?);
            (lin, fold(RelExpr::Distinct { input }, placed)?)
        }
        RelExpr::Project { input, items } => {
            let top = linear(input)?;
            match top.base.as_ref() {
                RelExpr::Unit => return constant(&top, items),
                // Everything over the reduce is its HAVING and its finalize projection.
                RelExpr::Reduce { input, group_cols, aggs } => {
                    let lin = linear(input)?;
                    let body = flat_reduce(&lin, input, group_cols, aggs, &top.preds, &top.items(items)?)?;
                    (lin, fold(body, placed)?)
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
) -> Result<RelExpr, GnitzSqlError> {
    let (_, source) = lin.source()?;
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
    let input = match computed {
        0 => Rc::clone(lin.base),
        _ => RelExpr::project(Rc::clone(lin.base), pre),
    };
    let aggs = AggCols(
        aggs.iter()
            .map(|c| AggCol { arg: c.arg.map(at), ..c.clone() })
            .collect(),
    );
    let mut rel = RelExpr::reduce(input, group_cols.iter().map(|&g| at(g)).collect(), aggs);
    if !having.is_empty() {
        rel = Rc::new(RelExpr::Filter {
            input: rel,
            preds: having.iter().map(rename).collect(),
        });
    }
    let items = items
        .iter()
        .map(|it| ProjEntry {
            expr: rename(&it.expr),
            out: it.out.clone(),
        })
        .collect();
    Ok(RelExpr::Project { input: rel, items })
}

/// The reply items of `written` over a read of `desc`, whose `Get` columns are
/// `source`: the source PK as hidden pass-through items in front, so no
/// SELECT-list name resolves to one and no position counts one.
pub(crate) fn reply_rows(
    desc: &Arc<RelDescriptor>,
    source: &[HirCol],
    written: Vec<ProjEntry>,
    placed: Vec<usize>,
) -> Result<AdhocRows, GnitzSqlError> {
    let mut items: Vec<ProjEntry> = desc
        .schema
        .pk_cols
        .iter()
        .map(|&pk| {
            let c = &source[pk as usize];
            RelExpr::passthrough_item(HirCol::new(c.id, c.def.clone().hidden()))
        })
        .collect();
    // A hidden item is an ORDER BY key; one an earlier item already computes sorts on it.
    let mut at = Vec::with_capacity(written.len());
    for it in written {
        let twin = items.iter().position(|p| it.out.def.is_hidden && p.expr == it.expr);
        at.push(twin.unwrap_or(items.len()));
        if twin.is_none() {
            items.push(it);
        }
    }
    let slots = physical::project_slots(&items, &Frame::scan(desc, source))?;
    debug_assert!(
        slots.iter().map(|s| s.1).eq(items.iter().map(|e| Some(e.out.id))),
        "`placed` indexes the bound items, so pinning the PK must move none of them"
    );
    let (items, cols) = slots.into_iter().map(|(item, _, def)| (item, def)).unzip();
    Ok(AdhocRows {
        items,
        cols,
        placed: placed.into_iter().map(|p| at[p]).collect(),
    })
}
