//! GROUP BY / aggregate / HAVING binding: the reduce, the pre-map under it and
//! the leaf its HAVING and SELECT list bind through.

use super::super::{as_col, hircol_of, ColId, ColIdGen, HirAgg, HirCol, HirExpr, ProjEntry, RelExpr};
use super::{bind_select_list, ItemLeaf, ScopeLeaf, Surface};
use crate::agg::AggFunc;
use crate::ast_util::{
    classify_agg_call, col_ref_parts, for_each_agg_call, group_by_exprs, projection_item_expr, window_spec_keys, AggArg,
};
use crate::bind::{bind_structural, LeafBinder};
use crate::error::GnitzSqlError;
use crate::ir::BExpr;
use crate::rules::reject_float_keys;
use crate::tail::group_by_target;
use gnitz_wire::{ColType, ColumnDef};
use sqlparser::ast::{Expr, Function, NamedWindowExpr, Select};
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
        let ty = bound.infer_ty_with(&|id: &ColId| hircol_of(&self.env, *id).def.ty);
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

/// One aggregate call a grouped body computes, with its argument resolved to a
/// reduce-input column.
#[derive(PartialEq)]
struct AggKey {
    func: AggFunc,
    arg: AggArg<ColId>,
}

/// Whether dedup cannot move this aggregate's value: `MIN`/`MAX` read the same
/// extremum from a set as from the multiset it collapses.
fn dedup_is_inert(func: AggFunc) -> bool {
    matches!(func, AggFunc::Min | AggFunc::Max)
}

/// Collect the aggregate calls referenced in an expression, appending each not
/// already present.
fn collect_aggs(
    expr: &Expr,
    leaf: &ScopeLeaf<'_>,
    calls: &mut Vec<AggKey>,
    pre: &mut PreMap<'_>,
) -> Result<(), GnitzSqlError> {
    for_each_agg_call(expr, &mut |f| -> Result<(), GnitzSqlError> {
        let (func, arg) = classify_agg_call(f)?;
        let mut arg = arg.try_map(|e| pre.column_for(e, leaf))?;
        if dedup_is_inert(func) {
            arg = arg.without_distinct();
        }
        let key = AggKey { func, arg };
        if !calls.contains(&key) {
            calls.push(key);
        }
        Ok(())
    })?;
    Ok(())
}

/// Whether `call` reads the same value from `Distinct(group cols, arg)` as from
/// the raw input — which every aggregate of the reduce must, since that set is
/// its only input.
fn survives_dedup(call: &AggKey, arg: ColId) -> bool {
    match call.arg {
        AggArg::Distinct(a) => a == arg,
        AggArg::All(a) => a == arg && dedup_is_inert(call.func),
        AggArg::Star => false,
    }
}

/// The column a body's DISTINCT aggregates all read, or `None` when it has none.
/// A DISTINCT aggregate is the plain aggregate over `Distinct(group cols, arg)`,
/// so every aggregate of the reduce reads that one set.
fn distinct_arg(calls: &[AggKey]) -> Result<Option<ColId>, GnitzSqlError> {
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
    let mut pre = PreMap::new(ids, leaf.env().to_vec());
    let group_cols = resolve_group_cols(select, leaf, &mut pre)?;
    let is_global = group_cols.is_empty();

    // Aggregates from the projection ∪ HAVING ∪ QUALIFY ∪ the WINDOW clause. An
    // inline window specification's keys are operands of the call, which the
    // walk reaches on its own.
    let mut calls: Vec<AggKey> = Vec::new();
    let named_keys = select.named_window.iter().flat_map(|d| match &d.1 {
        NamedWindowExpr::WindowSpec(s) => window_spec_keys(s).collect::<Vec<_>>(),
        NamedWindowExpr::NamedWindow(_) => Vec::new(),
    });
    for expr in select
        .projection
        .iter()
        .filter_map(projection_item_expr)
        .chain(select.having.iter())
        .chain(select.qualify.iter())
        .chain(named_keys)
        .chain(order_exprs.iter().copied())
    {
        collect_aggs(expr, leaf, &mut calls, &mut pre)?;
    }
    // An ad-hoc read lowers to one stateless fold over one scan, which has no room
    // for the `Distinct` below the reduce.
    if surface != Surface::ViewBody && calls.iter().any(|c| matches!(c.arg, AggArg::Distinct(_))) {
        return Err(GnitzSqlError::Rejected(
            "DISTINCT aggregates are supported in a CREATE VIEW body only".into(),
        ));
    }
    let mut aggs: Vec<HirAgg> = Vec::with_capacity(calls.len());
    for c in &calls {
        let agg = HirAgg::new(ids, c.func, c.arg.ignoring_distinct(), &pre.env, is_global, &aggs)?;
        aggs.push(agg);
    }
    let phys = HirAgg::physical(&aggs);
    // What the reduce reads, and under a DISTINCT aggregate the set it reduces over.
    let distinct = distinct_arg(&calls)?;
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

    let mut rel = RelExpr::reduce(input, group_cols.clone(), phys.clone());

    // Everything a `ColId` over the reduce output can resolve to.
    let PreMap { mut env, extra, .. } = pre;
    env.extend(phys.into_iter().map(|c| c.col));
    // One leaf per clause over the same grouped relation: only the wording of a
    // rejection differs, so only the clause does.
    let grouped = |clause| GroupedLeaf {
        leaf,
        env: &env,
        group_cols: &group_cols,
        aggs: &aggs,
        extra: &extra,
        clause,
    };

    // HAVING → a Filter over the raw reduce output.
    if let Some(having) = &select.having {
        let hexpr = bind_structural(having, &grouped("HAVING"))?;
        rel = RelExpr::filter(rel, vec![hexpr])?;
    }

    let leaves = [&grouped("GROUP BY SELECT"), &grouped("ORDER BY")];
    bind_select_list(ids, select, rel, leaves, "GROUP BY", order_exprs, surface)
}

/// The leaf for an expression over the grouped relation — HAVING and the finalize
/// SELECT list alike. A GROUP BY key binds to the column holding it and an
/// aggregate call to its finalize composite; nothing else is available.
struct GroupedLeaf<'a> {
    leaf: &'a ScopeLeaf<'a>,
    /// The typing table for a `ColId` an expression over the reduce output holds:
    /// the reduce's input columns plus each aggregate's raw value and companion.
    /// Names resolve through `leaf`, never against this list.
    env: &'a [HirCol],
    group_cols: &'a [ColId],
    aggs: &'a [HirAgg],
    /// The pre-map's materialized columns, so a written expression matches the
    /// column already computed for it — by the reduce's own pre-map, or, under a
    /// DISTINCT aggregate, by the projection below the `Distinct`.
    extra: &'a [ProjEntry],
    /// The clause being bound — this leaf serves HAVING and the SELECT list, and
    /// every rejection below names it.
    clause: &'static str,
}

impl GroupedLeaf<'_> {
    /// The group-key column an expression names, if it names one. Binding through
    /// the **body's own leaf** is what makes one rule serve both spellings: a bare
    /// reference binds to its own column, a written key to the pre-map column
    /// holding it, and `l.v` in a join resolves the way the body resolves it.
    fn group_key(&self, e: &Expr) -> Option<ColId> {
        let bound = bind_structural(e, self.leaf).ok()?;
        find_bound(self.extra, &bound).filter(|id| self.group_cols.contains(id))
    }

    fn find_agg(&self, f: &Function) -> Result<&HirAgg, GnitzSqlError> {
        // `distinct_arg` admitted one distinct set for the whole reduce, so the
        // qualifier no longer distinguishes two calls: `(func, arg)` names one.
        let (func, arg_expr) = classify_agg_call(f)?;
        // Through the pre-map, so `SUM(a * b)` here names the column the reduce
        // already aggregates rather than a second one.
        let arg = match arg_expr.ignoring_distinct() {
            Some(e) => Some(find_bound(self.extra, &bind_structural(e, self.leaf)?).ok_or_else(|| {
                GnitzSqlError::Internal(format!("{}: unsupported aggregate argument {e}", self.clause))
            })?),
            None => None,
        };
        self.aggs
            .iter()
            .find(|a| a.func == func && a.arg == arg)
            .ok_or_else(|| {
                let name = arg.map_or("*", |id| hircol_of(self.env, id).def.name.as_str());
                GnitzSqlError::Internal(format!(
                    "{}: aggregate {func:?}({name}) could not be resolved",
                    self.clause
                ))
            })
    }
}

impl ItemLeaf for GroupedLeaf<'_> {
    fn env(&self) -> &[HirCol] {
        self.env
    }
    fn wildcard_cols(&self, _: Option<&str>) -> Result<Option<Vec<&HirCol>>, GnitzSqlError> {
        Ok(None)
    }
}

impl LeafBinder<ColId> for GroupedLeaf<'_> {
    /// A written GROUP BY key, wherever it appears: `(a + b) * 2` binds over
    /// `GROUP BY a + b`. With no computed key there is nothing to match, and
    /// `bind_column` answers the bare names on its own.
    fn bind_node(&self, e: &Expr) -> Option<HirExpr> {
        // `extra` also holds aggregate arguments, which are never group keys —
        // testing it alone would bind and discard the subtree at every node.
        if !self.extra.iter().any(|x| self.group_cols.contains(&x.out.id)) {
            return None;
        }
        Some(BExpr::ColRef(self.group_key(e)?))
    }

    /// A group key binds to the column holding it; a name the body resolves but
    /// the grouping does not cover was written outside both, and anything the
    /// body itself refuses is that refusal, re-read as this clause's.
    fn bind_column(&self, e: &Expr) -> Result<HirExpr, GnitzSqlError> {
        let bound = bind_structural(e, self.leaf).map_err(|err| err.in_clause(self.clause))?;
        match find_bound(self.extra, &bound).filter(|id| self.group_cols.contains(id)) {
            Some(id) => Ok(BExpr::ColRef(id)),
            None => Err(match col_ref_parts(e).map(|(_, n)| n) {
                Some(name) => GnitzSqlError::Rejected(format!(
                    "{}: column '{name}' must appear in GROUP BY or an aggregate function",
                    self.clause
                )),
                None => GnitzSqlError::Rejected(format!("{}: expected a group key or an aggregate", self.clause)),
            }),
        }
    }

    /// An aggregate call binds to its finalize composite. There is no
    /// non-aggregate arm: `find_agg` classifies first, and `classify_agg_call` is
    /// already that name's qualifier + unknown-name rejection.
    fn bind_function(&self, f: &Function) -> Result<HirExpr, GnitzSqlError> {
        Ok(self.find_agg(f)?.finalize())
    }
    fn is_nullable(&self, id: &ColId) -> bool {
        hircol_of(self.env, *id).def.is_nullable
    }
    fn type_of(&self, id: &ColId) -> ColType {
        hircol_of(self.env, *id).def.ty
    }
}
