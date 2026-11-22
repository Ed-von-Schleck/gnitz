//! HIR → ad-hoc read lowering: a bound query that reads one relation, as the
//! bound, predicate and reply of one read of it. The filters and projections over
//! the relation — a CTE reference is one — compose by substitution into the tree
//! the flat query binds to, which the rows reply or the fold then lower.

use super::physical::{self, Frame, Relabel};
use super::{AggCol, BoundRef, ColId, HirCol, HirExpr, OrderCol, ProjEntry, RelExpr, hircol_of, reduce_reads};
use crate::error::{GnitzSqlError, derivation};
use crate::ir::{BExpr, BoundExpr};
use crate::project::payload_program;
use gnitz_core::{RelDescriptor, Schema};
use gnitz_wire::{ColumnDef, ComputeMap, GroupAggs, OrderKey};
use rustc_hash::FxHashMap as HashMap;
use std::borrow::Cow;
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
}

/// `rel` down to the first node that is no filter or projection.
fn linear(rel: &Rc<RelExpr>) -> Result<Linear<'_>, GnitzSqlError> {
    match rel.as_ref() {
        RelExpr::Get { .. } | RelExpr::Unit | RelExpr::Reduce { .. } | RelExpr::Distinct { .. } => Ok(Linear {
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
            let mut map = HashMap::with_capacity_and_hasher(items.len(), Default::default());
            for it in items {
                let e = l.subst(&it.expr)?;
                e.for_each_ref(&mut |_| refs += 1);
                map.insert(it.out.id, e);
            }
            if refs > MAX_EXPANDED_REFS {
                return Err(GnitzSqlError::rejected(format!(
                    "the query's CTEs expand to {refs} column references, over the limit of {MAX_EXPANDED_REFS}.\n\
                     CREATE VIEW <name> AS <the CTE body> — the engine maintains it incrementally — then SELECT from it."
                )));
            }
            Ok(Linear { map, ..l })
        }
        RelExpr::Join { .. } => Err(derivation("JOIN")),
        RelExpr::SetOp { .. } => Err(derivation("set operation")),
        RelExpr::Ordered { .. } => Err(GnitzSqlError::Internal(
            "an ad-hoc read bound an ordered operator".into(),
        )),
    }
}

/// Everything an ad-hoc grouped read needs, in layout terms: what the workers
/// fold, and what the client does with the partials — the reduce or `SELECT
/// DISTINCT`, a fold with no aggregate, as layout for a stateless fold the client
/// finishes. It shares the view reduce's rules, output key included, and builds no
/// evaluator.
pub(crate) struct FoldPieces {
    /// The reduce input: the source relabelled by a pre-map that only copies its
    /// columns, else the pre-map's output. The schema `agg`'s columns index, and the
    /// one that names them (a computed pre-map column is a hidden `_preN`).
    pub(crate) reduce_schema: Arc<Schema>,
    /// What the workers fold. It carries no cardinality COUNT: nothing stateless
    /// gates on one.
    pub(crate) agg: GroupAggs,
    /// The fold sink's pre-map over the source schema, `None` when the reduce
    /// groups and aggregates source columns directly.
    pub(crate) pre: Option<ComputeMap>,
    /// HAVING over the workers' partial reply, laid out as a view's reduce output over
    /// the same group set — `finalize`'s source; empty when the body has none or it
    /// folded to true.
    pub(crate) having: Vec<BoundExpr>,
    /// The finalize projection over the partial reply: the rows reply a read of it
    /// would answer. An aggregate's finalize composite (AVG's divide, a SUM's null
    /// gate) is *in* its item, exactly as it is in the view path's finalize map — so
    /// the client finisher has no per-aggregate shape switch to keep in step.
    pub(crate) finalize: RowsReply,
}

/// Lower a bound ad-hoc body ordered by `order`, over its output columns; `refs` are
/// the catalog relations the bind read.
pub(crate) fn lower_read(
    rel: &Rc<RelExpr>,
    order: Vec<OrderCol>,
    refs: Vec<BoundRef>,
) -> Result<AdhocRead, GnitzSqlError> {
    let (top, items) = match rel.as_ref() {
        RelExpr::Project { input, items } => {
            let top = linear(input)?;
            let items = (items.iter())
                .map(|it| {
                    Ok(ProjEntry {
                        expr: top.subst(&it.expr)?,
                        out: it.out.clone(),
                    })
                })
                .collect::<Result<_, GnitzSqlError>>()?;
            (top, items)
        }
        // A DISTINCT body is read whole, each column as itself.
        RelExpr::Distinct { .. } => (linear(rel)?, ProjEntry::passthroughs(rel.cols().into_owned())),
        _ => {
            return Err(GnitzSqlError::Internal(
                "an ad-hoc read body is not a projection".into(),
            ));
        }
    };
    // The levels over the relation read, and the reduce over them: its input, group
    // columns, aggregates and HAVING.
    let base = top.base;
    let (lin, reduce) = match base.as_ref() {
        // The one row `items` compute over the global ground row: one hidden
        // `_group_pk`, keyed by the global group key.
        RelExpr::Unit => {
            if !top.preds.is_empty() {
                return Err(GnitzSqlError::rejected(
                    "a WHERE over a SELECT without FROM is not supported",
                ));
            }
            let ground = Frame::keyed(vec![crate::agg::group_pk_def()], [])?;
            return Ok(AdhocRead::Constant(reply_rows(ground, items, order, false)?));
        }
        // Everything over the reduce is its HAVING and its finalize projection.
        RelExpr::Reduce { input, group_cols, aggs, .. } => {
            let having = top.preds;
            (
                linear(input)?,
                Some((input, Cow::Borrowed(&group_cols[..]), &aggs[..], having)),
            )
        }
        // A DISTINCT is the reduce grouping every column of its input, with no
        // aggregate, so a filter over it reads only group columns and is one under it.
        RelExpr::Distinct { input } => {
            let mut lin = linear(input)?;
            for p in &top.preds {
                let p = lin.subst(p)?;
                lin.preds.push(p);
            }
            let group_cols = input.cols().iter().map(|c| c.id).collect();
            (lin, Some((input, Cow::Owned(group_cols), &[][..], Vec::new())))
        }
        _ => (top, None),
    };
    let (desc, source) = match lin.base.as_ref() {
        RelExpr::Get { desc, cols } => (desc, cols),
        // What the CTE or derived table derives under its grouping is named first.
        RelExpr::Reduce { input, .. } => {
            linear(input)?;
            return Err(derivation("an aggregate over a grouped CTE or derived table"));
        }
        // A DISTINCT CTE or derived table, or a DISTINCT aggregate's set: the
        // fold is one stateless pass over one scan.
        RelExpr::Distinct { input } => {
            linear(input)?;
            return Err(derivation("a grouping over a DISTINCT set"));
        }
        // A DISTINCT over no relation is a set already, so only an aggregate meets it.
        _ => {
            return Err(GnitzSqlError::rejected(
                "an aggregate over a SELECT without FROM is not supported",
            ));
        }
    };
    let src = Frame::scan(desc, source);
    let shape = match reduce {
        None => AdhocShape::Rows(reply_rows(src.clone(), items, order, desc.class.is_view())?),
        Some((input, group_cols, aggs, having)) => {
            // The reduce's pre-map over the relation: each column it reads as `lin`
            // composes it. Reads equal in expression are one column: each later one
            // is read as the first, so a column copied twice still relabels the
            // source. A copy keeps its source column's definition; a computed column
            // is a hidden `_preN`.
            let in_cols = input.cols();
            let mut pre: Vec<ProjEntry> = Vec::new();
            let mut same: Vec<(ColId, ColId)> = Vec::new();
            let mut computed = 0;
            for r in reduce_reads(&group_cols, aggs) {
                let expr = &lin.map[&r];
                if let Some(k) = pre.iter().find(|k| k.expr == *expr) {
                    same.push((r, k.out.id));
                    continue;
                }
                let def = match expr.as_col() {
                    Some(id) => src.schema.columns()[src.slot(id)?].clone(),
                    None => {
                        let c = &hircol_of(&in_cols, r).def;
                        computed += 1;
                        ColumnDef::new(format!("_pre{}", computed - 1), c.ty, c.is_nullable).hidden()
                    }
                };
                pre.push(ProjEntry {
                    expr: expr.clone(),
                    out: HirCol::new(r, def),
                });
            }
            let at = |id: ColId| same.iter().find(|(dup, _)| *dup == id).map_or(id, |&(_, k)| k);
            let read = |e: &HirExpr| e.rebuild(&mut |id| BExpr::ColRef(at(*id)));
            let groups: Vec<ColId> = group_cols.iter().map(|&g| at(g)).collect();
            let aggs: Vec<AggCol> = (aggs.iter())
                .map(|c| AggCol { arg: c.arg.map(at), ..c.clone() })
                .collect();
            let having: Vec<HirExpr> = having.iter().map(read).collect();
            let finalize = (items.into_iter())
                .map(|it| ProjEntry { expr: read(&it.expr), out: it.out })
                .collect();
            // The pre-map over the source, under the spine's rule: one copying distinct
            // source columns relabels the source, which the reduce then reads in place;
            // anything else is a map.
            let (map, reduce_in) = match src.relabeled(&pre, Relabel::Slots)? {
                Some(relabeled) => (None, relabeled),
                None => physical::projection_map(&pre, &src)?,
            };
            let agg = reduce_in.group_aggs(&groups, &aggs)?;
            let cols = aggs.iter().map(|c| c.col.slot()).collect();
            let partial = reduce_in.keyed_by(&agg.group_cols, agg.group_cols.iter().copied(), cols)?;
            let having = partial.resolve_preds(&having)?;
            let finalize = reply_rows(partial, finalize, order, false)?;
            let pieces = FoldPieces {
                reduce_schema: reduce_in.schema,
                agg,
                pre: map,
                having,
                finalize,
            };
            let is_distinct = matches!(base.as_ref(), RelExpr::Distinct { .. });
            AdhocShape::Fold { pieces: Box::new(pieces), is_distinct }
        }
    };
    let conjuncts = src.resolve_preds(&lin.preds)?;
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

/// The rows reply a projection over a source produces.
pub(crate) struct RowsReply {
    /// The schema of the source the items read: a relation, a fold's partials, or the
    /// ground row.
    pub(crate) src: Arc<Schema>,
    /// The reply's regions under the SELECT list's numbering: the key columns no
    /// item names, hidden, then the items — a key column at the first item that
    /// copies it, every other item a payload column — with, where the reply ships a
    /// view's regions as they are, the view's own hidden columns beside them.
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
/// `rides_hidden` where `src` is a view, whose hidden payload columns are its own
/// operators' state rather than a table's dropped columns.
pub(crate) fn reply_rows(
    src: Frame,
    written: Vec<ProjEntry>,
    mut order: Vec<OrderCol>,
    rides_hidden: bool,
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
    // A reply moving no column ships the relation's regions as they are, a view's
    // hidden columns carried along hidden instead of the others being copied out
    // from beside them.
    let how = match rides_hidden {
        true => Relabel::WholeCarryingHidden,
        false => Relabel::Whole,
    };
    let (proj, out, program) = match src.relabeled(&items, how)? {
        Some(out) => ((0..out.layout.len()).map(BoundExpr::ColRef).collect(), out, None),
        None => {
            let (proj, out) = physical::physicalize_projection(&items, &src)?;
            let program = payload_program(&proj, &out.schema, &src.schema)?;
            (proj, out, Some(program))
        }
    };
    let src = src.schema;
    let k = src.pk_cols().len();
    let visible = items.iter().filter(|c| !c.out.def.is_hidden).map(|c| c.out.id);
    let (schema, column_of) = out.schema_in_order(visible, |_| None)?;
    // The worker's sink numbers its input: the program's output, or the relation
    // itself, which an identity projection numbers alike.
    let sink_order = out.order_keys(&order)?;
    let mut copied: Vec<(u32, u32)> = Vec::with_capacity(proj.len());
    for (slot, item) in proj.iter().enumerate() {
        let src = item.as_col().map(|s| s as u32);
        if let Some(src) = src.filter(|s| copied.iter().all(|(c, _)| c != s)) {
            copied.push((src, column_of[slot]));
        }
    }
    // A walk in PK order yields keys ascending a leading run of the PK columns.
    let pk_ordered = sink_order.len() <= k
        && (sink_order.iter().zip(src.pk_cols()))
            .all(|(key, &pk)| !key.desc && proj[key.col as usize].as_col() == Some(pk as usize));
    Ok(RowsReply {
        order: (sink_order.iter())
            .map(|&key| OrderKey { col: column_of[key.col as usize], ..key })
            .collect(),
        sink_order,
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
