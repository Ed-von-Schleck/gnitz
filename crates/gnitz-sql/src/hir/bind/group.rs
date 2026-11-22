//! GROUP BY / aggregate / HAVING binding: the reduce, the pre-map under it and
//! the leaf its HAVING and SELECT list bind through.

use super::super::{
    ColId, ColIdGen, HirAgg, HirCol, HirExpr, OrderCol, ProjEntry, RelExpr, col_by_id, hircol_of, reduce_reads,
};
use super::{BindCx, ItemLeaf, ScopeLeaf, window};
use crate::agg::{AggArg, AggFunc};
use crate::bind::{LeafBinder, bind_conjuncts, bind_structural};
use crate::error::GnitzSqlError;
use crate::ir::{BExpr, LeafTyping};
use crate::tail::{OrderKey, group_by_target};
use gnitz_wire::{ColType, ColumnDef, ReduceKind};
use sqlparser::ast::{Expr, GroupByExpr, Select};
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

/// The columns a grouped query computes below its reduce: a GROUP BY key or an
/// aggregate argument that is not already a bare column reference becomes one
/// column of a `Project` under the `Reduce`, so the `Reduce` takes plain `ColId`s.
/// `lower_reduce` emits that `Project` as a map in the reduce's own circuit, so it
/// costs no second relation.
///
/// One column per distinct expression, keyed by its **bound** form — which is what
/// makes `GROUP BY t.a + b` and `SELECT a + b` one expression, and folds `Nested`
/// away so `SUM((a * b))` and `SUM(a * b)` reach one aggregate.
///
/// Grown as a grouped body's expressions bind, with the reduce's aggregates.
struct Grouping {
    /// The reduce input's columns: the source's, then one per materialized
    /// expression.
    env: Vec<HirCol>,
    /// The materialized columns and the expressions computing them — also the
    /// memo, since `extra[i].expr` is what `extra[i].out.id` holds.
    extra: Vec<ProjEntry>,
    /// Each aggregate call once, in the order first bound.
    aggs: Vec<HirAgg>,
}

impl Grouping {
    /// The column already holding `bound`'s value: its own when it is a bare
    /// column reference, else the pre-map column computing an equal expression.
    /// One rule for minting and for looking up, so an expression cannot resolve to
    /// one column in the reduce and another above it.
    fn find(&self, bound: &HirExpr) -> Option<ColId> {
        (bound.as_col()).or_else(|| self.extra.iter().find(|x| &x.expr == bound).map(|x| x.out.id))
    }

    /// The column `e` names, materializing one on first sight.
    fn column_for(&mut self, ids: &ColIdGen, e: &Expr, leaf: &ScopeLeaf<'_>) -> Result<ColId, GnitzSqlError> {
        let bound = bind_structural(e, leaf)?;
        if let Some(id) = self.find(&bound) {
            return Ok(id);
        }
        let (ty, nullable) = bound.declared(&self.env[..]);
        // Hidden because only the expression that minted it may reach it.
        let out = HirCol::new(
            ids.next(),
            ColumnDef::new(format!("_pre{}", self.extra.len()), ty, nullable).hidden(),
        );
        let id = out.id;
        self.env.push(out.clone());
        self.extra.push(ProjEntry { expr: bound, out });
        Ok(id)
    }

    /// The projection computing exactly `ids`: a materialized column by its
    /// expression, a source column passed through.
    fn items_for(&self, ids: &[ColId]) -> Vec<ProjEntry> {
        ids.iter()
            .map(|id| match self.extra.iter().find(|x| x.out.id == *id) {
                Some(e) => e.clone(),
                None => ProjEntry::passthrough(hircol_of(&self.env, *id).clone()),
            })
            .collect()
    }
}

/// Whether dedup cannot move this aggregate's value: `MIN`/`MAX` read the same
/// extremum from a set as from the multiset it collapses.
fn dedup_is_inert(func: AggFunc) -> bool {
    matches!(func, AggFunc::Min | AggFunc::Max)
}

/// Bind the GROUP BY / aggregate / HAVING suffix over `input`, producing
/// `Project(Filter_having?(Reduce(input)))`, plus the column each of `order`
/// sorts on.
pub(super) fn bind_grouped_suffix(
    cx: &BindCx<'_>,
    select: &Select,
    input: Rc<RelExpr>,
    leaf: &ScopeLeaf<'_>,
    order: &[OrderKey<'_>],
) -> Result<(Rc<RelExpr>, Vec<OrderCol>), GnitzSqlError> {
    let ids = cx.ids;
    let mut st = Grouping {
        env: leaf.scope.combined.clone(),
        extra: Vec::new(),
        aggs: Vec::new(),
    };
    // The clause guard admits only the plain list form.
    let GroupByExpr::Expressions(exprs, _) = &select.group_by else {
        return Err(GnitzSqlError::Internal(
            "a grouped body's GROUP BY is not a list".into(),
        ));
    };
    // A 1-based position names its SELECT item, a bare reference is its own
    // column, and anything computed is materialized below the reduce.
    let mut group_cols = Vec::new();
    for ge in exprs {
        let target = group_by_target(ge, select)?;
        // The key binds through the FROM leaf, whose rejection names the column but
        // not the clause it was written in.
        group_cols.push(st.column_for(ids, target, leaf).map_err(|e| e.in_clause("GROUP BY"))?);
    }
    let grouped = GroupedLeaf {
        leaf,
        ids,
        group_cols: &group_cols,
        // `extra` holds only key columns until the first aggregate binds.
        computed_keys: !st.extra.is_empty(),
        state: RefCell::new(st),
    };
    // The reduce, under its HAVING. HAVING binds last, so its aggregates are the
    // reduce's too.
    let rel = || -> Result<Rc<RelExpr>, GnitzSqlError> {
        let having = match &select.having {
            Some(having) => bind_conjuncts(having, &grouped).map_err(|e| e.in_clause("HAVING"))?,
            None => Vec::new(),
        };
        let st = &*grouped.state.borrow();
        // A DISTINCT aggregate is the plain aggregate over `Distinct(group cols,
        // arg)`, so every aggregate of the reduce reads that one set, and must read
        // the same value from it as from the raw input.
        let distinct = st.aggs.iter().find_map(|c| match c.arg {
            AggArg::Distinct(a) => Some(a),
            _ => None,
        });
        if let Some(d) = distinct {
            if st.aggs.iter().any(|c| matches!(c.arg, AggArg::Distinct(a) if a != d)) {
                return Err(GnitzSqlError::rejected(
                    "DISTINCT aggregates: every DISTINCT aggregate of one query must read the same argument",
                ));
            }
            let survives =
                |c: &HirAgg| c.arg == AggArg::Distinct(d) || c.arg == AggArg::All(d) && dedup_is_inert(c.func);
            if !st.aggs.iter().all(survives) {
                return Err(GnitzSqlError::rejected(
                    "DISTINCT aggregates: a plain aggregate cannot be mixed with them, except MIN/MAX of the \
                     DISTINCT argument",
                ));
            }
        }
        let phys = HirAgg::physical(&st.aggs);
        // What the reduce reads, and under a DISTINCT aggregate the set it reduces over.
        let mut reads = reduce_reads(&group_cols, &phys);
        if let Some(d) = distinct.filter(|d| !reads.contains(d)) {
            reads.push(d);
        }
        let input = match distinct {
            Some(_) => RelExpr::distinct(RelExpr::project(input, st.items_for(&reads)), "DISTINCT aggregate")?,
            None if st.extra.is_empty() => input,
            None => RelExpr::project(input, st.items_for(&reads)),
        };
        RelExpr::filter(
            RelExpr::reduce(input, group_cols.clone(), phys, ReduceKind::Aggregate, "GROUP BY")?,
            having,
        )
    };
    window::bind_select_list(cx, select, rel, &grouped, "GROUP BY", order)
}

/// The leaf for an expression over the grouped relation — the SELECT list, the
/// ORDER BY keys, QUALIFY and HAVING alike. A GROUP BY key binds to the column
/// holding it and an aggregate call to its finalize composite, registered with
/// the reduce where it is first bound; nothing else is available.
struct GroupedLeaf<'a> {
    /// The body's own leaf: names, GROUP BY keys and aggregate arguments resolve
    /// through it.
    leaf: &'a ScopeLeaf<'a>,
    ids: &'a ColIdGen,
    group_cols: &'a [ColId],
    /// Whether a GROUP BY key is computed, so a written expression can name one.
    computed_keys: bool,
    state: RefCell<Grouping>,
}

impl ItemLeaf for GroupedLeaf<'_> {
    /// `None` for a pre-map or aggregate column, which the body's scope does not hold.
    fn source_name(&self, id: ColId) -> Option<Arc<str>> {
        self.leaf.source_name(id)
    }
    fn wildcard_cols(&self, _: Option<&str>) -> Result<Option<Vec<&HirCol>>, GnitzSqlError> {
        Ok(None)
    }
}

impl LeafTyping<ColId> for GroupedLeaf<'_> {
    /// A reduce input column, or an aggregate's raw value or companion.
    fn decl(&self, id: &ColId) -> (ColType, bool) {
        let st = self.state.borrow();
        let def = match col_by_id(&st.env, *id) {
            Some(c) => &c.def,
            None => {
                let mut cols = st.aggs.iter().flat_map(HirAgg::cols);
                let agg = cols.find(|c| c.col.id == *id);
                &agg.expect("a grouped ColRef is a reduce input or an aggregate column")
                    .col
                    .def
            }
        };
        (def.ty, def.is_nullable)
    }
}

impl LeafBinder<ColId> for GroupedLeaf<'_> {
    /// A written GROUP BY key, wherever it appears: `(a + b) * 2` binds over
    /// `GROUP BY a + b`. With no computed key there is nothing to match, and
    /// `bind_column` answers the bare names on its own. Binding through the
    /// **body's own leaf** is what makes one rule serve both spellings: a bare
    /// reference binds to its own column, a written key to the pre-map column
    /// holding it, and `l.v` in a join resolves the way the body resolves it.
    fn bind_node(&self, e: &Expr) -> Option<HirExpr> {
        if !self.computed_keys {
            return None;
        }
        let bound = bind_structural(e, self.leaf).ok()?;
        (self.state.borrow().find(&bound))
            .filter(|id| self.group_cols.contains(id))
            .map(BExpr::ColRef)
    }

    /// A group key binds to its column; a name the body resolves but the
    /// grouping does not cover was written outside both.
    fn bind_column(&self, qual: Option<&str>, name: &str) -> Result<HirExpr, GnitzSqlError> {
        let bound = self.leaf.bind_column(qual, name)?;
        match bound.as_col().filter(|id| self.group_cols.contains(id)) {
            Some(_) => Ok(bound),
            None => Err(GnitzSqlError::rejected(format!(
                "column '{name}' must appear in GROUP BY or an aggregate function"
            ))),
        }
    }

    /// The call's finalize composite, over the aggregate of the reduce equal to
    /// it in function and argument — registered here on first sight. The
    /// argument binds through the body's leaf, which reads no [`Grouping`].
    fn bind_aggregate(&self, func: AggFunc, arg: AggArg<&Expr>) -> Result<HirExpr, GnitzSqlError> {
        let st = &mut *self.state.borrow_mut();
        // Through the pre-map, so `SUM(a * b)` twice names one column.
        let mut arg = arg.try_map(|e| st.column_for(self.ids, e, self.leaf))?;
        if dedup_is_inert(func) {
            arg = arg.without_distinct();
        }
        let at = match st.aggs.iter().position(|a| a.func == func && a.arg == arg) {
            Some(at) => at,
            None => {
                let is_global = self.group_cols.is_empty();
                let agg = HirAgg::new(self.ids, func, arg, &st.env, is_global, &st.aggs)?;
                st.aggs.push(agg);
                st.aggs.len() - 1
            }
        };
        Ok(st.aggs[at].finalize())
    }

    fn bind_subquery(&self, e: &Expr) -> Result<HirExpr, GnitzSqlError> {
        self.leaf.bind_subquery(e)
    }
}
