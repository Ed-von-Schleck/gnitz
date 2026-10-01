//! Subquery binding: EXISTS / IN, a scalar aggregate, and ANY / ALL, each bound to
//! the column its decorrelated join produces.

use super::super::{
    cross_comparison, hircol_of, side, ColId, ColIdGen, HirAgg, HirCol, HirExpr, ProjEntry, RelExpr, Side,
    SubqueryKind, SubqueryRef,
};
use super::{bind_body_suffix, resolve_relation, BindCx, JoinScope, ScopeLeaf, SubPolicy};
use crate::agg::AggFunc;
use crate::ast_util::{
    classify_agg_call, classify_from, extract_table_name_and_alias, is_agg_call, peel_nested, FromShape,
};
use crate::bind::structural::maybe_negate;
use crate::bind::{bind_conjuncts, bind_structural, single_relation_col_idx};
use crate::error::GnitzSqlError;
use crate::ir::{BExpr, BinOp};
use crate::validate::{as_plain_select, reject_query_envelope_body, reject_unhonored_select_clauses, HonoredClauses};
use gnitz_wire::ColumnDef;
use sqlparser::ast::{BinaryOperator, Expr, Query, Select, SelectItem};
use std::cell::RefCell;
use std::rc::Rc;

/// Everything a subquery bind resolves against: the body bind's own context,
/// the outer scope it correlates to, and the record of subqueries bound so far.
struct SubCtx<'a, 'c> {
    cx: &'a mut BindCx<'c>,
    outer_env: &'a [HirCol],
    outer_alias: &'a str,
    subs: &'a RefCell<Vec<SubqueryRef>>,
}

impl SubCtx<'_, '_> {
    /// Record `s`, and read it through its column.
    fn record(&self, s: SubqueryRef) -> HirExpr {
        let id = s.id;
        self.subs.borrow_mut().push(s);
        BExpr::ColRef(id)
    }
}

/// Bind a single-relation linear body reading subqueries, each bound to its column
/// where `bind_structural` meets it.
pub(super) fn bind_linear_subquery_body(
    cx: &mut BindCx<'_>,
    select: &Select,
    get: Rc<RelExpr>,
    scope: &JoinScope,
    outer_alias: &str,
    order_exprs: &[&Expr],
) -> Result<(Rc<RelExpr>, Vec<usize>), GnitzSqlError> {
    let (ids, stmt, surface) = (cx.ids, cx.view.stmt, cx.surface);
    let subs = RefCell::new(Vec::new());
    let sub = RefCell::new(SubCtx {
        cx,
        outer_env: &scope.combined,
        outer_alias,
        subs: &subs,
    });
    let bind_sub = |e: &Expr| bind_one_subquery(&mut sub.borrow_mut(), e);
    let leaf = ScopeLeaf {
        scope,
        clause: stmt,
        sub: SubPolicy::Bind { bind: &bind_sub, subs: &subs },
    };
    bind_body_suffix(ids, select, get, &leaf, surface, order_exprs)
}

/// The resolved inner relation of a subquery: `Filter?(Get)` with the inner-local
/// WHERE fused, plus the mixed-scope correlation conjuncts (over the outer ∪ inner
/// `ColId` space) that become the decorrelated join's ON.
struct InnerResolved<'e> {
    rel: Rc<RelExpr>,
    inner_cols: Vec<HirCol>,
    /// The inner relation's effective alias — what a qualified reference to an
    /// inner column must name.
    inner_alias: String,
    inner_select: &'e Select,
    correlation: Vec<HirExpr>,
}

/// Resolve a subquery's inner relation and split its WHERE into inner-local vs
/// correlation conjuncts. The inner FROM must be one plain relation (no JOINs, no
/// derived table, no GROUP BY / HAVING / DISTINCT); a conjunct referencing only
/// the outer relation is rejected (hoisting would be wrong for NOT EXISTS).
fn resolve_inner<'e>(cx: &mut SubCtx<'_, '_>, subquery: &'e Query) -> Result<InnerResolved<'e>, GnitzSqlError> {
    let (outer_env, outer_alias) = (cx.outer_env, cx.outer_alias);
    let ctx = "subquery";
    let inner_select = as_plain_select(reject_query_envelope_body(subquery, ctx)?, ctx)?;
    reject_unhonored_select_clauses(inner_select, HonoredClauses::PLAIN, ctx)?;
    let FromShape::SinglePlainRelation(factor) = classify_from(&inner_select.from) else {
        return Err(GnitzSqlError::Rejected(
            "EXISTS/IN subquery: only a single FROM table without JOINs is supported; compose via views".into(),
        ));
    };
    let (inner_name, inner_alias) = extract_table_name_and_alias(factor, cx.cx.cat.schema_name(), "subquery")?;
    if outer_alias.eq_ignore_ascii_case(&inner_alias) {
        return Err(GnitzSqlError::Rejected(format!(
            "relation alias '{outer_alias}' is used by both the view FROM and its subquery; rename one"
        )));
    }
    let inner_get = resolve_relation(cx.cx, &inner_name)?;
    let inner_cols = inner_get.cols();

    let mut local_preds: Vec<HirExpr> = Vec::new();
    let mut correlation: Vec<HirExpr> = Vec::new();
    if let Some(where_expr) = &inner_select.selection {
        // The (outer, inner) scope the split resolves against — built here, and
        // read nowhere else: a subquery with no WHERE has nothing to split.
        let mut scope = JoinScope::new();
        scope.push(outer_alias, outer_env.to_vec());
        scope.push(&inner_alias, inner_cols.clone());
        let leaf = ScopeLeaf {
            scope: &scope,
            clause: "subquery",
            sub: SubPolicy::Reject(
                "nested subqueries inside an EXISTS/IN subquery are not supported; compose via views",
            ),
        };
        for bound in bind_conjuncts(where_expr, &leaf)? {
            match side(&bound, &inner_cols, outer_env) {
                Side::Left | Side::Neither => local_preds.push(bound),
                Side::Right => {
                    return Err(GnitzSqlError::Rejected(
                        "a subquery WHERE conjunct references only the outer relation; \
                         hoist it into the view's own WHERE clause"
                            .into(),
                    ))
                }
                Side::Both => correlation.push(bound),
            }
        }
    }
    let rel = RelExpr::filter(inner_get, local_preds)?;
    Ok(InnerResolved {
        rel,
        inner_cols,
        inner_alias,
        inner_select,
        correlation,
    })
}

/// The single projection expression of a subquery's SELECT, else `err`.
fn single_projection_expr<'e>(select: &'e Select, err: &str) -> Result<&'e Expr, GnitzSqlError> {
    match select.projection.as_slice() {
        [SelectItem::UnnamedExpr(e)] | [SelectItem::ExprWithAlias { expr: e, .. }] => Ok(e),
        _ => Err(GnitzSqlError::Rejected(err.into())),
    }
}

/// Bind one subquery node to the column it is read through. ANY/ALL is normalized
/// here (`= ANY → IN`, `<> ALL → NOT IN`, range → `x OP (SELECT MIN/MAX)`).
fn bind_one_subquery(cx: &mut SubCtx<'_, '_>, e: &Expr) -> Result<HirExpr, GnitzSqlError> {
    match e {
        Expr::Exists { subquery, negated } => bind_exists_sub(cx, subquery, None, *negated),
        Expr::InSubquery { expr, subquery, negated } => bind_exists_sub(cx, subquery, Some(expr), *negated),
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
    cx: &mut SubCtx<'_, '_>,
    subquery: &Query,
    in_operand: Option<&Expr>,
    negated: bool,
) -> Result<HirExpr, GnitzSqlError> {
    let (outer_env, outer_alias) = (cx.outer_env, cx.outer_alias);
    let ir = resolve_inner(cx, subquery)?;
    let mut correlation = ir.correlation;
    let mut nullable = false;
    if let Some(operand) = in_operand {
        // IN shape: reject a tuple / non-plain-column LHS before extracting the pair.
        if matches!(operand, Expr::Tuple(_)) {
            return Err(GnitzSqlError::Rejected(
                "IN (SELECT …): tuple operands are not supported".into(),
            ));
        }
        let outer = bind_plain_col(
            operand,
            outer_env,
            outer_alias,
            "IN (SELECT …): the outer operand must be a plain column",
        )?;
        let inner_proj = single_projection_expr(
            ir.inner_select,
            "IN (SELECT …): the subquery must select exactly one plain column",
        )?;
        let inner = bind_plain_col(
            inner_proj,
            &ir.inner_cols,
            &ir.inner_alias,
            "IN (SELECT …): the subquery must select exactly one plain column",
        )?;
        nullable = hircol_of(outer_env, outer).def.is_nullable || hircol_of(&ir.inner_cols, inner).def.is_nullable;
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
fn bind_scalar_sub(cx: &mut SubCtx<'_, '_>, q: &Query) -> Result<HirExpr, GnitzSqlError> {
    let (ids, outer_env) = (cx.cx.ids, cx.outer_env);
    let ir = resolve_inner(cx, q)?;
    let err = "a scalar subquery must be a single aggregate over its correlation group";
    let proj_expr = single_projection_expr(ir.inner_select, err)?;
    let (func, arg) = classify_scalar_agg(proj_expr, &ir.inner_cols, &ir.inner_alias, err)?;
    let subref = scalar_leaf(ids, outer_env, ir, func, arg)?;
    Ok(cx.record(subref))
}

/// Bind an ANY/ALL quantified comparison. `= ANY`/`<> ALL` route to IN/NOT IN;
/// range ANY/ALL becomes `x OP (SELECT MIN/MAX)` — under the null test that
/// settles an empty set.
fn bind_quantifier_sub(
    cx: &mut SubCtx<'_, '_>,
    left: &Expr,
    compare_op: &BinaryOperator,
    right: &Expr,
    is_any: bool,
) -> Result<HirExpr, GnitzSqlError> {
    let Expr::Subquery(q) = right else {
        return Err(GnitzSqlError::Rejected(
            "an ANY/ALL operator's right operand must be a subquery".into(),
        ));
    };
    let bop = match (is_any, compare_op) {
        (true, BinaryOperator::Eq) => return bind_exists_sub(cx, q, Some(left), false),
        (false, BinaryOperator::NotEq) => return bind_exists_sub(cx, q, Some(left), true),
        (true, BinaryOperator::NotEq) => {
            return Err(GnitzSqlError::Rejected(
                "`<> ANY (SELECT …)` is not supported (use NOT (x = ALL …) semantics via a wrapping view)".into(),
            ))
        }
        (false, BinaryOperator::Eq) => {
            return Err(GnitzSqlError::Rejected("`= ALL (SELECT …)` is not supported".into()))
        }
        (_, BinaryOperator::Lt) => BinOp::Lt,
        (_, BinaryOperator::LtEq) => BinOp::Le,
        (_, BinaryOperator::Gt) => BinOp::Gt,
        (_, BinaryOperator::GtEq) => BinOp::Ge,
        _ => {
            return Err(GnitzSqlError::Rejected(
                "only range (<, <=, >, >=), `= ANY`, and `<> ALL` quantified comparisons are supported".into(),
            ))
        }
    };
    // x < ANY ⟺ < MAX; x > ANY ⟺ > MIN; x < ALL ⟺ < MIN; x > ALL ⟺ > MAX.
    let bounds_below = bop.as_range_rel().is_some_and(|r| r.bounds_below());
    let agg_func = if is_any != bounds_below {
        AggFunc::Max
    } else {
        AggFunc::Min
    };
    let (ids, outer_env, outer_alias) = (cx.cx.ids, cx.outer_env, cx.outer_alias);
    let ir = resolve_inner(cx, q)?;
    let err = "a range ANY/ALL subquery must select a single non-nullable column";
    let proj_expr = single_projection_expr(ir.inner_select, err)?;
    let inner_col = bind_plain_col(proj_expr, &ir.inner_cols, &ir.inner_alias, err)?;
    if hircol_of(&ir.inner_cols, inner_col).def.is_nullable {
        return Err(GnitzSqlError::Rejected(
            "a range ANY/ALL subquery's column must be NOT NULL — a NULL in the set makes ANY/ALL \
             three-valued in a way the MIN/MAX rewrite cannot reproduce"
                .into(),
        ));
    }
    let m = cx.record(scalar_leaf(ids, outer_env, ir, agg_func, Some(inner_col))?);
    let outer_scope = JoinScope::single(outer_alias, outer_env.to_vec());
    let outer_leaf = ScopeLeaf {
        scope: &outer_scope,
        clause: cx.cx.view.stmt,
        sub: SubPolicy::PerKind,
    };
    let x = bind_structural(left, &outer_leaf)?;
    let cmp = BExpr::bin(x, bop, m.clone());
    // ANY over ∅ = FALSE, ALL over ∅ = TRUE: `m` is NULL over no rows, so the
    // null test makes the edge a definite constant, exact under negation.
    let m_test = BExpr::NullTest { inner: Box::new(m), want_null: !is_any };
    Ok(BExpr::bin(m_test, if is_any { BinOp::And } else { BinOp::Or }, cmp))
}

/// Resolve `e` against `env` as a bare column reference, returning its `ColId`.
/// Every rejection is `err`: the caller's surface states what shape it needed,
/// which is more use here than "column 'x' not found".
fn bind_plain_col(e: &Expr, env: &[HirCol], alias: &str, err: &str) -> Result<ColId, GnitzSqlError> {
    single_relation_col_idx(env.iter().map(|c| &c.def), alias, e)
        .map(|i| env[i].id)
        .map_err(|_| GnitzSqlError::Rejected(err.into()))
}

/// Classify a scalar subquery's single-aggregate projection into `(func, arg)`.
fn classify_scalar_agg(
    e: &Expr,
    inner_cols: &[HirCol],
    inner_alias: &str,
    non_agg: &str,
) -> Result<(AggFunc, Option<ColId>), GnitzSqlError> {
    let Expr::Function(f) = peel_nested(e) else {
        return Err(GnitzSqlError::Rejected(non_agg.into()));
    };
    if !is_agg_call(f) {
        return Err(GnitzSqlError::Rejected(non_agg.into()));
    }
    let (func, arg_expr) = classify_agg_call(f)?;
    let arg = match arg_expr.reject_distinct("a scalar subquery aggregate")? {
        Some(a) => Some(bind_plain_col(
            a,
            inner_cols,
            inner_alias,
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
                return Err(GnitzSqlError::Rejected(
                    "a scalar/quantifier subquery cannot use a range correlation (only equality \
                     `inner_col = outer_col` conjuncts are supported)"
                        .into(),
                ))
            }
            _ => {
                return Err(GnitzSqlError::Rejected(
                    "a scalar/quantifier subquery's correlation contains a conjunct that is not an \
                     `inner_col = outer_col` equality; filter inside the subquery or a wrapping view"
                        .into(),
                ))
            }
        }
    }
    Ok(cols)
}

/// A scalar subquery: `func(arg)` reduced per correlation group, finalized as its
/// column.
fn scalar_leaf(
    ids: &ColIdGen,
    outer_env: &[HirCol],
    ir: InnerResolved<'_>,
    func: AggFunc,
    arg: Option<ColId>,
) -> Result<SubqueryRef, GnitzSqlError> {
    let group_cols = scalar_group_cols(&ir.correlation, outer_env, &ir.inner_cols)?;
    let agg = HirAgg::new(ids, func, arg, &ir.inner_cols, group_cols.is_empty(), &[])?;
    let (expr, ty, nullable) = agg.as_value();
    let value = HirCol::new(ids.next(), ColumnDef::typed("_agg", ty, nullable).hidden());
    let mut items = RelExpr::passthrough_items(group_cols.iter().map(|&id| hircol_of(&ir.inner_cols, id).clone()));
    items.push(ProjEntry { expr, out: value.clone() });
    Ok(SubqueryRef {
        id: value.id,
        kind: SubqueryKind::Scalar { ty, count: func == AggFunc::Count },
        rel: RelExpr::project(
            RelExpr::reduce(ir.rel, group_cols, HirAgg::physical(std::slice::from_ref(&agg))),
            items,
        ),
        correlation: ir.correlation,
    })
}
