//! GROUP BY / aggregate / HAVING binding: the reduce, the pre-map under it and
//! the leaf its HAVING and SELECT list bind through.

use super::super::{as_col, col_by_id, hircol_of, ColId, ColIdGen, HirAgg, HirCol, HirExpr, ProjEntry, RelExpr};
use super::{bind_select_list, ItemLeaf, ScopeLeaf, Surface};
use crate::agg::AggFunc;
use crate::ast_util::{group_by_exprs, AggArg};
use crate::bind::{bind_conjuncts, bind_structural, LeafBinder};
use crate::error::GnitzSqlError;
use crate::ir::{BExpr, LeafTyping};
use crate::rules::reject_float_keys;
use crate::tail::group_by_target;
use gnitz_wire::{ColType, ColumnDef};
use sqlparser::ast::{Expr, Select};
use std::cell::RefCell;
use std::rc::Rc;

/// The columns a grouped query computes below its reduce: a GROUP BY key or an
/// aggregate argument that is not already a bare column reference becomes one
/// column of a `Project` under the `Reduce`, so the `Reduce` takes plain `ColId`s.
/// `lower_reduce` emits that `Project` as a map in the reduce's own circuit, so it
/// costs no second relation.
///
/// One column per distinct expression, keyed by its **bound** form — which is what
/// makes `GROUP BY t.a + b` and `SELECT a + b` one expression, and folds `Nested`
/// away so `SUM((a * b))` and `SUM(a * b)` reach one aggregate.
struct PreMap<'a> {
    ids: &'a ColIdGen,
    /// The reduce input's columns: the source env, then one per materialized
    /// expression — so `env[..env.len() - extra.len()]` is the source env, which
    /// [`Self::source_cols`] hands back. Every `ColId` this mints resolves here.
    env: Vec<HirCol>,
    /// The materialized columns and the expressions computing them — also the
    /// memo, since `extra[i].expr` is what `extra[i].out.id` holds.
    extra: Vec<ProjEntry>,
}

impl<'a> PreMap<'a> {
    fn new(ids: &'a ColIdGen, env: Vec<HirCol>) -> Self {
        PreMap { ids, env, extra: Vec::new() }
    }

    /// The column `e` names, materializing one on first sight.
    fn column_for(&mut self, e: &Expr, leaf: &ScopeLeaf<'_>) -> Result<ColId, GnitzSqlError> {
        let bound = bind_structural(e, leaf)?;
        if let Some(id) = find_bound(&self.extra, &bound) {
            return Ok(id);
        }
        let ty = bound.infer_ty(&self.env[..]);
        // Declared nullable unconditionally (a computed value can be NULL: `a / 0`),
        // so an aggregate over one takes the null-skipping shape. Hidden because
        // only the expression that minted it may reach it.
        let out = HirCol::new(
            self.ids.next(),
            ColumnDef::typed(format!("_pre{}", self.extra.len()), ty, true).hidden(),
        );
        let id = out.id;
        self.env.push(out.clone());
        self.extra.push(ProjEntry { expr: bound, out });
        Ok(id)
    }

    /// The scope this pre-map was built over — the body's *resolved* columns, so
    /// a derived table's `AS d(col…)` aliases are the names, where `input.cols()`
    /// would give the subtree's own.
    fn source_cols(&self) -> &[HirCol] {
        &self.env[..self.env.len() - self.extra.len()]
    }

    /// The projection computing exactly `ids`: a materialized column by its
    /// expression, a source column passed through.
    fn items_for(&self, ids: &[ColId]) -> Vec<ProjEntry> {
        ids.iter()
            .map(|id| match self.extra.iter().find(|x| x.out.id == *id) {
                Some(e) => e.clone(),
                None => RelExpr::passthrough_item(hircol_of(self.source_cols(), *id).clone()),
            })
            .collect()
    }
}

/// The column already holding `bound`'s value: its own when it is a bare column
/// reference, else the pre-map column computing an equal expression. One rule for
/// minting and for looking up, so an expression cannot resolve to one column in
/// the reduce and another above it.
fn find_bound(extra: &[ProjEntry], bound: &HirExpr) -> Option<ColId> {
    as_col(bound).or_else(|| extra.iter().find(|x| &x.expr == bound).map(|x| x.out.id))
}

/// Resolve the GROUP BY list to the `ColId`s the reduce groups by: a 1-based
/// position names its SELECT item, a bare reference is its own column, and
/// anything computed is materialized below the reduce by `pre`.
fn resolve_group_cols(
    select: &Select,
    leaf: &ScopeLeaf<'_>,
    pre: &mut PreMap<'_>,
) -> Result<Vec<ColId>, GnitzSqlError> {
    let mut cols = Vec::new();
    for ge in group_by_exprs(select)? {
        let target = group_by_target(ge, select)?;
        // The key binds through the FROM leaf, whose rejection names the column but
        // not the clause it was written in.
        let id = pre.column_for(target, leaf).map_err(|e| e.in_clause("GROUP BY"))?;
        reject_float_keys([&hircol_of(&pre.env, id).def], "GROUP BY")?;
        cols.push(id);
    }
    Ok(cols)
}

/// Whether dedup cannot move this aggregate's value: `MIN`/`MAX` read the same
/// extremum from a set as from the multiset it collapses.
fn dedup_is_inert(func: AggFunc) -> bool {
    matches!(func, AggFunc::Min | AggFunc::Max)
}

/// Whether `call` reads the same value from `Distinct(group cols, arg)` as from
/// the raw input — which every aggregate of the reduce must, since that set is
/// its only input.
fn survives_dedup(call: &HirAgg, arg: ColId) -> bool {
    match call.arg {
        AggArg::Distinct(a) => a == arg,
        AggArg::All(a) => a == arg && dedup_is_inert(call.func),
        AggArg::Star => false,
    }
}

/// The column a body's DISTINCT aggregates all read, or `None` when it has none.
/// A DISTINCT aggregate is the plain aggregate over `Distinct(group cols, arg)`,
/// so every aggregate of the reduce reads that one set.
fn distinct_arg(calls: &[HirAgg]) -> Result<Option<ColId>, GnitzSqlError> {
    let Some(arg) = calls.iter().find_map(|c| match c.arg {
        AggArg::Distinct(a) => Some(a),
        _ => None,
    }) else {
        return Ok(None);
    };
    if calls.iter().any(|c| matches!(c.arg, AggArg::Distinct(a) if a != arg)) {
        return Err(GnitzSqlError::Rejected(
            "DISTINCT aggregates: every DISTINCT aggregate of one query must read the same argument".into(),
        ));
    }
    if calls.iter().any(|c| !survives_dedup(c, arg)) {
        return Err(GnitzSqlError::Rejected(
            "DISTINCT aggregates: a plain aggregate cannot be mixed with them, except MIN/MAX of the \
             DISTINCT argument"
                .into(),
        ));
    }
    Ok(Some(arg))
}

/// Bind the GROUP BY / aggregate / HAVING suffix over `input`, producing
/// `Project(Filter_having?(Reduce(input)))`, plus the projection item each
/// ORDER BY key of `surface` sorts on.
pub(super) fn bind_grouped_suffix(
    ids: &ColIdGen,
    select: &Select,
    input: Rc<RelExpr>,
    leaf: &ScopeLeaf<'_>,
    surface: Surface,
    order_exprs: &[&Expr],
) -> Result<(Rc<RelExpr>, Vec<usize>), GnitzSqlError> {
    let mut pre = PreMap::new(ids, leaf.scope.combined.clone());
    let group_cols = resolve_group_cols(select, leaf, &mut pre)?;
    let grouped = GroupedLeaf {
        leaf,
        ids,
        group_cols: &group_cols,
        // `extra` holds only key columns until the first aggregate binds.
        computed_keys: !pre.extra.is_empty(),
        state: RefCell::new(Grouping { pre, aggs: Vec::new() }),
    };
    // The reduce, under its HAVING. HAVING binds last, so its aggregates are the
    // reduce's too.
    let rel = || -> Result<Rc<RelExpr>, GnitzSqlError> {
        let having = match &select.having {
            Some(having) => bind_conjuncts(having, &grouped).map_err(|e| e.in_clause("HAVING"))?,
            None => Vec::new(),
        };
        let Grouping { pre, aggs } = &*grouped.state.borrow();
        // An ad-hoc read lowers to one stateless fold over one scan, which has no
        // room for the `Distinct` below the reduce.
        if surface != Surface::ViewBody && aggs.iter().any(|a| matches!(a.arg, AggArg::Distinct(_))) {
            return Err(GnitzSqlError::Rejected(
                "DISTINCT aggregates are supported in a CREATE VIEW body only".into(),
            ));
        }
        let phys = HirAgg::physical(aggs);
        // What the reduce reads, and under a DISTINCT aggregate the set it reduces over.
        let distinct = distinct_arg(aggs)?;
        let mut reads = group_cols.clone();
        for id in phys.iter().filter_map(|c| c.arg).chain(distinct) {
            if !reads.contains(&id) {
                reads.push(id);
            }
        }
        let input = match distinct {
            Some(_) => RelExpr::distinct(RelExpr::project(input, pre.items_for(&reads)), "DISTINCT aggregate")?,
            None if pre.extra.is_empty() => input,
            None => RelExpr::project(input, pre.items_for(&reads)),
        };
        RelExpr::filter(RelExpr::reduce(input, group_cols.clone(), phys), having)
    };
    bind_select_list(ids, select, rel, &grouped, "GROUP BY", order_exprs, surface)
}

/// What a grouped body's reduce computes, grown as its expressions bind.
struct Grouping<'a> {
    pre: PreMap<'a>,
    /// Each aggregate call once, in the order first bound.
    aggs: Vec<HirAgg>,
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
    state: RefCell<Grouping<'a>>,
}

impl GroupedLeaf<'_> {
    /// The group-key column an expression names, if it names one. Binding through
    /// the **body's own leaf** is what makes one rule serve both spellings: a bare
    /// reference binds to its own column, a written key to the pre-map column
    /// holding it, and `l.v` in a join resolves the way the body resolves it.
    fn group_key(&self, e: &Expr) -> Option<ColId> {
        let bound = bind_structural(e, self.leaf).ok()?;
        find_bound(&self.state.borrow().pre.extra, &bound).filter(|id| self.group_cols.contains(id))
    }
}

impl ItemLeaf for GroupedLeaf<'_> {
    /// `None` for a pre-map or aggregate column, which the body's scope does not hold.
    fn source_name(&self, id: ColId) -> Option<String> {
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
        let def = match col_by_id(&st.pre.env, *id) {
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
    /// `bind_column` answers the bare names on its own.
    fn bind_node(&self, e: &Expr) -> Option<HirExpr> {
        if !self.computed_keys {
            return None;
        }
        Some(BExpr::ColRef(self.group_key(e)?))
    }

    /// A group key binds to its column; a name the body resolves but the
    /// grouping does not cover was written outside both.
    fn bind_column(&self, qual: Option<&str>, name: &str) -> Result<HirExpr, GnitzSqlError> {
        let bound = self.leaf.bind_column(qual, name)?;
        match as_col(&bound).filter(|id| self.group_cols.contains(id)) {
            Some(_) => Ok(bound),
            None => Err(GnitzSqlError::Rejected(format!(
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
        let mut arg = arg.try_map(|e| st.pre.column_for(e, self.leaf))?;
        if dedup_is_inert(func) {
            arg = arg.without_distinct();
        }
        let at = match st.aggs.iter().position(|a| a.func == func && a.arg == arg) {
            Some(at) => at,
            None => {
                let is_global = self.group_cols.is_empty();
                let agg = HirAgg::new(self.ids, func, arg, &st.pre.env, is_global, &st.aggs)?;
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
