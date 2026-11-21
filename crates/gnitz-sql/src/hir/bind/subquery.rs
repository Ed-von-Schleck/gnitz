//! Subquery binding: EXISTS / IN, a scalar aggregate, and ANY / ALL, each bound to
//! the column its decorrelated join produces.

use super::super::{ColId, HirAgg, HirCol, HirExpr, ProjEntry, RelExpr, Side, cross_comparison, hircol_of, side};
use super::{BindCx, JoinScope, ScopeLeaf, SubPolicy, resolve_table_ref};
use crate::agg::{AggArg, AggFunc};
use crate::ast_util::{
    body_is_grouped, classify_agg_call, col_ref_parts, is_agg_call, peel_nested, single_plain_relation,
};
use crate::bind::{bind_conjuncts, bind_structural, maybe_negate};
use crate::error::GnitzSqlError;
use crate::ir::{BExpr, BinOp};
use crate::names::extract_table_ref;
use crate::validate::{
    QueryEnvelope, SelectSite, as_plain_select, reject_unhonored_query_clauses, reject_unhonored_select_clauses,
};
use gnitz_wire::{ColType, ColumnDef, ReduceKind};
use sqlparser::ast::{BinaryOperator, Expr, InTarget, Query, Select, SelectItem};
use std::cell::RefCell;
use std::rc::Rc;

/// A subquery bound where its expression was written: the column it is read
/// through, and what joins that column in when the body's projection is built.
#[derive(Clone)]
pub(super) struct SubqueryRef {
    /// The column this subquery is read through: its Mark join's BOOLEAN column,
    /// or its finalized aggregate.
    pub(super) id: ColId,
    pub(super) kind: SubqueryKind,
    /// EXISTS/IN: the inner relation (a `Get`, or a read of a CTE) under an
    /// optional `Filter` of its local WHERE. Scalar: `Project(Reduce)` passing
    /// the group columns through and computing the finalized aggregate as `id`.
    pub(super) rel: Rc<RelExpr>,
    /// The mixed-scope conjuncts that become the join's ON, an IN's
    /// `outer = inner` equality among them.
    pub(super) correlation: Vec<HirExpr>,
}

/// How a subquery joins in: `Exists` (EXISTS / IN) as a Semi/Anti/Mark join,
/// `Scalar` (an aggregate, or a range ANY/ALL's MIN/MAX) as its reduce.
#[derive(Clone, Copy)]
pub(super) enum SubqueryKind {
    /// `nullable`: an IN whose operands can be NULL, so its truth can be.
    Exists { nullable: bool },
    /// `count`: a COUNT, which a group of no rows renders as 0, never NULL.
    Scalar { ty: ColType, count: bool },
}

/// Everything a subquery bind resolves against: the body bind's own context,
/// the outer scope it correlates to, and the record of subqueries
/// bound so far.
#[derive(Clone, Copy)]
struct SubCtx<'a> {
    cx: &'a BindCx<'a>,
    outer: &'a JoinScope,
    subs: &'a RefCell<Vec<SubqueryRef>>,
}

impl SubCtx<'_> {
    /// Record `s`, and read it through its column.
    fn record(&self, s: SubqueryRef) -> HirExpr {
        let id = s.id;
        self.subs.borrow_mut().push(s);
        BExpr::ColRef(id)
    }
}

/// The resolved inner relation of a subquery: a `Get` or a read of a CTE, under an
/// optional `Filter` of its local WHERE, plus the mixed-scope correlation conjuncts
/// (over the outer ∪ inner `ColId` space) that become the decorrelated join's ON.
struct InnerResolved<'e> {
    rel: Rc<RelExpr>,
    /// The inner relation under its effective alias — what a qualified reference
    /// to an inner column must name.
    inner: JoinScope,
    inner_select: &'e Select,
    correlation: Vec<HirExpr>,
}

/// Resolve a subquery's inner relation and split its WHERE into inner-local vs
/// correlation conjuncts. The inner FROM must be one plain relation (no JOINs, no
/// derived table, no GROUP BY / HAVING / DISTINCT); a conjunct referencing only
/// the outer relation is rejected (hoisting would be wrong for NOT EXISTS).
fn resolve_inner<'e>(cx: SubCtx<'_>, subquery: &'e Query) -> Result<InnerResolved<'e>, GnitzSqlError> {
    let ctx = "subquery";
    let inner_select = as_plain_select(reject_unhonored_query_clauses(subquery, QueryEnvelope::Bare, ctx)?, ctx)?;
    reject_unhonored_select_clauses(inner_select, SelectSite::Subquery, ctx)?;
    let Some(factor) = single_plain_relation(&inner_select.from) else {
        return Err(GnitzSqlError::rejected(
            "EXISTS/IN subquery: only a single FROM table without JOINs is supported; compose via views",
        ));
    };
    let tref = extract_table_ref(factor, cx.cx.cat.schema_name(), "subquery")?;
    let (inner_get, inner_cols) = resolve_table_ref(cx.cx, &tref)?;

    let mut local_preds: Vec<HirExpr> = Vec::new();
    let mut correlation: Vec<HirExpr> = Vec::new();
    if let Some(where_expr) = &inner_select.selection {
        // The (outer, inner) scope the split resolves against — built here, and
        // read nowhere else: a subquery with no WHERE has nothing to split.
        let mut scope = cx.outer.clone();
        scope.push(&tref.alias, inner_cols.clone())?;
        let leaf = ScopeLeaf {
            scope: &scope,
            sub: SubPolicy::Reject(|_| {
                GnitzSqlError::rejected(
                    "nested subqueries inside an EXISTS/IN subquery are not supported; compose via views",
                )
            }),
        };
        for bound in bind_conjuncts(where_expr, &leaf).map_err(|e| e.in_clause("subquery"))? {
            match side(&bound, &inner_cols, &cx.outer.combined) {
                Side::One(0) | Side::Neither => local_preds.push(bound),
                Side::One(_) => {
                    return Err(GnitzSqlError::rejected(
                        "a subquery WHERE conjunct references only the outer relation; \
                         hoist it into the view's own WHERE clause",
                    ));
                }
                Side::Both => correlation.push(bound),
            }
        }
    }
    let rel = RelExpr::filter(inner_get, local_preds)?;
    Ok(InnerResolved {
        rel,
        inner: JoinScope::single(tref.alias, inner_cols),
        inner_select,
        correlation,
    })
}

/// The single projection expression of a subquery's SELECT, else `err`.
fn single_projection_expr<'e>(select: &'e Select, err: &str) -> Result<&'e Expr, GnitzSqlError> {
    match select.projection.as_slice() {
        [SelectItem::UnnamedExpr(e)] | [SelectItem::ExprWithAlias { expr: e, .. }] => Ok(e),
        _ => Err(GnitzSqlError::rejected(err)),
    }
}

/// Bind one subquery node in `cx`, correlated to the `outer` scope,
/// to the column it is read through, recording it in `subs`. ANY/ALL is normalized
/// here (`= ANY → IN`, `<> ALL → NOT IN`, range → `x OP (SELECT MIN/MAX)`).
pub(super) fn bind_one_subquery(
    cx: &BindCx<'_>,
    outer: &JoinScope,
    subs: &RefCell<Vec<SubqueryRef>>,
    e: &Expr,
) -> Result<HirExpr, GnitzSqlError> {
    let cx = SubCtx { cx, outer, subs };
    match e {
        Expr::Exists { subquery, negated } => bind_exists_sub(cx, subquery, None, *negated),
        Expr::In {
            expr,
            target: InTarget::Subquery(subquery),
            negated,
            global: false,
        } => bind_exists_sub(cx, subquery, Some(expr), *negated),
        Expr::Subquery(q) => bind_scalar_sub(cx, q),
        Expr::AnyOp { left, compare_op, right, .. } => bind_quantifier_sub(cx, left, compare_op, right, true),
        Expr::AllOp { left, compare_op, right } => bind_quantifier_sub(cx, left, compare_op, right, false),
        other => Err(GnitzSqlError::Internal(format!(
            "bind_one_subquery on a non-subquery node: {other:?}"
        ))),
    }
}

/// Bind an EXISTS / IN subquery as an `Exists`-kind `SubqueryRef`. For IN, the
/// `(outer, inner)` equality joins the correlation; an EXISTS must be correlated.
fn bind_exists_sub(
    cx: SubCtx<'_>,
    subquery: &Query,
    in_operand: Option<&Expr>,
    negated: bool,
) -> Result<HirExpr, GnitzSqlError> {
    let ir = resolve_inner(cx, subquery)?;
    let mut correlation = ir.correlation;
    let mut nullable = false;
    if in_operand.is_none() && body_is_grouped(ir.inner_select) {
        // An ungrouped aggregate yields one row over any input, so the subquery
        // always has a row; a semi-join would test for a match.
        return Err(GnitzSqlError::rejected(
            "EXISTS (SELECT <aggregate> …) is always true: an aggregate subquery returns one row",
        ));
    }
    if let Some(operand) = in_operand {
        // IN shape: reject a tuple / non-plain-column LHS before extracting the pair.
        if matches!(operand, Expr::Tuple(_)) {
            return Err(GnitzSqlError::rejected(
                "IN (SELECT …): tuple operands are not supported",
            ));
        }
        let outer = bind_plain_col(
            operand,
            cx.outer,
            "IN (SELECT …): the outer operand must be a plain column",
        )?;
        let inner_proj = single_projection_expr(
            ir.inner_select,
            "IN (SELECT …): the subquery must select exactly one plain column",
        )?;
        let inner = bind_plain_col(
            inner_proj,
            &ir.inner,
            "IN (SELECT …): the subquery must select exactly one plain column",
        )?;
        nullable = hircol_of(&cx.outer.combined, outer).def.is_nullable
            || hircol_of(&ir.inner.combined, inner).def.is_nullable;
        correlation.push(BExpr::bin(BExpr::ColRef(outer), BinOp::Eq, BExpr::ColRef(inner)));
    }
    let subref = SubqueryRef {
        id: cx.cx.ids.next(),
        kind: SubqueryKind::Exists { nullable },
        rel: ir.rel,
        correlation,
    };
    Ok(maybe_negate(cx.record(subref), negated))
}

/// Bind a scalar aggregate subquery as a `Scalar`-kind `SubqueryRef` whose `rel`
/// reduces grouped (correlated) or globally (uncorrelated).
fn bind_scalar_sub(cx: SubCtx<'_>, q: &Query) -> Result<HirExpr, GnitzSqlError> {
    let ir = resolve_inner(cx, q)?;
    let err = "a scalar subquery must be a single aggregate over its correlation group";
    let proj_expr = single_projection_expr(ir.inner_select, err)?;
    let (func, arg) = classify_scalar_agg(proj_expr, &ir.inner, err)?;
    let subref = scalar_leaf(cx, ir, func, arg)?;
    Ok(cx.record(subref))
}

/// Bind an ANY/ALL quantified comparison. `= ANY`/`<> ALL` route to IN/NOT IN;
/// range ANY/ALL becomes `x OP (SELECT MIN/MAX)` — under the null test that
/// settles an empty set.
fn bind_quantifier_sub(
    cx: SubCtx<'_>,
    left: &Expr,
    compare_op: &BinaryOperator,
    right: &Expr,
    is_any: bool,
) -> Result<HirExpr, GnitzSqlError> {
    let Expr::Subquery(q) = right else {
        return Err(GnitzSqlError::rejected(
            "an ANY/ALL operator's right operand must be a subquery",
        ));
    };
    let bop = match (is_any, compare_op) {
        (true, BinaryOperator::Eq) => return bind_exists_sub(cx, q, Some(left), false),
        (false, BinaryOperator::NotEq) => return bind_exists_sub(cx, q, Some(left), true),
        (true, BinaryOperator::NotEq) => {
            return Err(GnitzSqlError::rejected(
                "`<> ANY (SELECT …)` is not supported (use NOT (x = ALL …) semantics via a wrapping view)",
            ));
        }
        (false, BinaryOperator::Eq) => {
            return Err(GnitzSqlError::rejected("`= ALL (SELECT …)` is not supported"));
        }
        (_, BinaryOperator::Lt) => BinOp::Lt,
        (_, BinaryOperator::LtEq) => BinOp::Le,
        (_, BinaryOperator::Gt) => BinOp::Gt,
        (_, BinaryOperator::GtEq) => BinOp::Ge,
        _ => {
            return Err(GnitzSqlError::rejected(
                "only range (<, <=, >, >=), `= ANY`, and `<> ALL` quantified comparisons are supported",
            ));
        }
    };
    // x < ANY ⟺ < MAX; x > ANY ⟺ > MIN; x < ALL ⟺ < MIN; x > ALL ⟺ > MAX.
    let bounds_below = bop.as_range_rel().is_some_and(|r| r.bounds_below());
    let agg_func = if is_any != bounds_below {
        AggFunc::Max
    } else {
        AggFunc::Min
    };
    let ir = resolve_inner(cx, q)?;
    let err = "a range ANY/ALL subquery must select a single non-nullable column";
    let proj_expr = single_projection_expr(ir.inner_select, err)?;
    let inner_col = bind_plain_col(proj_expr, &ir.inner, err)?;
    if hircol_of(&ir.inner.combined, inner_col).def.is_nullable {
        return Err(GnitzSqlError::rejected(
            "a range ANY/ALL subquery's column must be NOT NULL — a NULL in the set makes ANY/ALL \
             three-valued in a way the MIN/MAX rewrite cannot reproduce",
        ));
    }
    let m = cx.record(scalar_leaf(cx, ir, agg_func, Some(inner_col))?);
    let outer_leaf = ScopeLeaf::plain(cx.outer);
    let x = bind_structural(left, &outer_leaf)?;
    let cmp = BExpr::bin(x, bop, m.clone());
    // ANY over ∅ = FALSE, ALL over ∅ = TRUE: `m` is NULL over no rows, so the
    // null test makes the edge a definite constant, exact under negation.
    let m_test = BExpr::NullTest { inner: Box::new(m), want_null: !is_any };
    Ok(BExpr::bin(m_test, if is_any { BinOp::And } else { BinOp::Or }, cmp))
}

/// Resolve `e` against `scope` as a bare column reference, returning its `ColId`.
/// Every rejection is `err`: the caller states what shape it needed, which is
/// more use here than "column 'x' not found".
fn bind_plain_col(e: &Expr, scope: &JoinScope, err: &str) -> Result<ColId, GnitzSqlError> {
    col_ref_parts(e)
        .and_then(|(qual, name)| scope.resolve(qual, name).ok())
        .ok_or_else(|| GnitzSqlError::rejected(err))
}

/// Classify a scalar subquery's single-aggregate projection into `(func, arg)`.
fn classify_scalar_agg(e: &Expr, inner: &JoinScope, non_agg: &str) -> Result<(AggFunc, Option<ColId>), GnitzSqlError> {
    let Expr::Function(f) = peel_nested(e) else {
        return Err(GnitzSqlError::rejected(non_agg));
    };
    if !is_agg_call(f) {
        return Err(GnitzSqlError::rejected(non_agg));
    }
    let (func, arg_expr) = classify_agg_call(f)?;
    let arg = match arg_expr.reject_distinct("a scalar subquery aggregate")? {
        Some(a) => Some(bind_plain_col(
            a,
            inner,
            "a scalar subquery aggregate must be over a plain column or `*`",
        )?),
        None => None,
    };
    Ok((func, arg))
}

/// The `Reduce` group columns of a scalar/quantifier subquery: the inner-side
/// `ColId` of each equality correlation conjunct. A range or non-equality
/// correlation is rejected (aggregation needs an equality GROUP BY key).
fn scalar_group_cols(correlation: &[HirExpr], outer: &[HirCol], inner: &[HirCol]) -> Result<Vec<ColId>, GnitzSqlError> {
    let mut cols = Vec::with_capacity(correlation.len());
    for conj in correlation {
        match cross_comparison(conj, outer, inner) {
            Some((_, inner, BinOp::Eq)) => cols.push(inner.id),
            Some((_, _, op)) if op.as_range_rel().is_some() => {
                return Err(GnitzSqlError::rejected(
                    "a scalar/quantifier subquery cannot use a range correlation (only equality \
                     `inner_col = outer_col` conjuncts are supported)",
                ));
            }
            _ => {
                return Err(GnitzSqlError::rejected(
                    "a scalar/quantifier subquery's correlation contains a conjunct that is not an \
                     `inner_col = outer_col` equality; filter inside the subquery or a wrapping view",
                ));
            }
        }
    }
    Ok(cols)
}

/// A scalar subquery: `func(arg)` reduced per correlation group, finalized as its
/// column.
fn scalar_leaf(
    cx: SubCtx<'_>,
    ir: InnerResolved<'_>,
    func: AggFunc,
    arg: Option<ColId>,
) -> Result<SubqueryRef, GnitzSqlError> {
    let (ids, inner_cols) = (cx.cx.ids, &ir.inner.combined);
    let group_cols = scalar_group_cols(&ir.correlation, &cx.outer.combined, inner_cols)?;
    let arg = arg.map_or(AggArg::Star, AggArg::All);
    let agg = HirAgg::new(ids, func, arg, inner_cols, group_cols.is_empty(), &[])?;
    let ty = crate::agg::agg_view_type(func, agg.out.col.def.ty);
    // A companion carries the null-ness; otherwise the raw column's own.
    let nullable = agg.companion.is_some() || agg.out.col.def.is_nullable;
    let value = HirCol::new(ids.next(), ColumnDef::new("_agg", ty, nullable).hidden());
    let mut items = ProjEntry::passthroughs(group_cols.iter().map(|&id| hircol_of(inner_cols, id).clone()));
    items.push(ProjEntry { expr: agg.finalize(), out: value.clone() });
    Ok(SubqueryRef {
        id: value.id,
        kind: SubqueryKind::Scalar { ty, count: func == AggFunc::Count },
        rel: RelExpr::project(
            RelExpr::reduce(
                ir.rel,
                group_cols,
                HirAgg::physical(std::slice::from_ref(&agg)),
                ReduceKind::Aggregate,
            ),
            items,
        ),
        correlation: ir.correlation,
    })
}
