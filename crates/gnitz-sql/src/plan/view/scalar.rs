//! Scalar aggregate subquery & quantifier lowering: `(SELECT AGG(e) FROM b
//! WHERE b.k = a.k)` in a WHERE comparison or a projection, and (Step 4)
//! `x OP ANY/ALL (SELECT …)`. Pure SQL-planner lowering — every circuit shape it
//! emits (grouped/global reduce, equi/pure-range join, LEFT null-fill, linear
//! filter/map) already exists and is reached only through the existing
//! `emit_group_by_pieces` / `plan_join_chain` / `emit_linear` builders.
//!
//! Every supported view lowers to a fixed three-layer chain of existing pieces:
//! ```text
//! Gᵢ    = reduce over the inner table, one per subquery       (emit_group_by_pieces)
//! H     = outer ⟕/⋈ every Gᵢ, projecting outer cols + Gᵢ agg cols  (plan_join_chain)
//! final = linear filter/map over H: rewritten WHERE + projection   (lower_linear/emit_linear)
//! ```
//! A **correlated** aggregate materializes as a grouped `Gᵢ` LEFT-joined to the
//! outer on its correlation key; the subquery node substitutes to `Gᵢ`'s
//! aggregate column (a bare column, or `COALESCE(col, 0)` for a COUNT — the
//! COUNT-bug repair). An **uncorrelated** aggregate comparison in a top-level
//! WHERE conjunct becomes an INNER join-as-filter against the one-row global
//! aggregate (equi for `=`, pure-range for `<,<=,>,>=`), consuming the conjunct.
//!
//! Scalar cardinality is provable-by-construction: a grouped `Gᵢ` emits exactly
//! one row per correlation key, a `global_ground` reduce exactly one row total —
//! so no runtime "subquery returned more than one row" path is ever needed.

use crate::ast_util::{extract_table_name_and_alias, flatten_conjuncts, projection_item_expr};
use crate::bind::{bind_single_table, build_alias_map, Binder};
use crate::error::GnitzSqlError;
use crate::ir::{AggFunc, BoundExpr};
use crate::plan::lp::lower_linear;
use crate::plan::validate::{
    reject_unhonored_query_clauses, reject_unhonored_select_clauses, HonoredClauses, HonoredQueryClauses,
};
use crate::plan::view::exists::{and_into, count_side_refs};
use crate::plan::view::predicates::extract_join_predicates;
use crate::plan::view::{group_by, join, simple, EmitPieces, ViewChain};
use gnitz_core::{GnitzClient, Schema, TypeCode};
use sqlparser::ast::{
    BinaryOperator, Expr, GroupByExpr, Ident, Join, JoinConstraint, JoinOperator, ObjectName, ObjectNamePart, Query,
    Select, SelectItem, SetExpr, TableFactor, Value, ValueWithSpan,
};
use sqlparser::tokenizer::Span;
use std::collections::HashSet;
use std::rc::Rc;

/// Whether a single-FROM view body carries a scalar `Expr::Subquery` or an
/// `Expr::AnyOp` / `Expr::AllOp` over a subquery, anywhere in its WHERE or
/// projection — the classifier's route into this builder.
pub(crate) fn has_scalar_subquery(select: &Select) -> bool {
    let in_where = select.selection.iter().any(expr_has_scalar_subquery);
    let in_proj = select
        .projection
        .iter()
        .filter_map(projection_item_expr)
        .any(expr_has_scalar_subquery);
    in_where || in_proj
}

fn expr_has_scalar_subquery(e: &Expr) -> bool {
    matches!(e, Expr::Subquery(_) | Expr::AnyOp { .. } | Expr::AllOp { .. })
        || crate::ast_util::expr_operands(e)
            .into_iter()
            .any(expr_has_scalar_subquery)
}

/// Rewrite `x = ANY (sub)` → `x IN (sub)` and `x <> ALL (sub)` → `x NOT IN (sub)`
/// in a view body's WHERE and projection, so equality quantifiers route to the
/// existing IN / NOT IN semi/anti-join path rather than this builder. Returns a
/// modified clone only when such a node is present (so the common path never
/// clones). Range ANY/ALL and every other node are left untouched.
pub(crate) fn rewrite_eq_any_ne_all(query: &Query) -> Option<Query> {
    let SetExpr::Select(select) = query.body.as_ref() else {
        return None;
    };
    let present = select.selection.iter().any(has_eq_any_ne_all)
        || select
            .projection
            .iter()
            .filter_map(projection_item_expr)
            .any(has_eq_any_ne_all);
    if !present {
        return None;
    }
    let mut q = query.clone();
    if let SetExpr::Select(s) = q.body.as_mut() {
        if let Some(sel) = s.selection.take() {
            s.selection = Some(map_eq_any_ne_all(&sel));
        }
        for item in &mut s.projection {
            if let SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } = item {
                *e = map_eq_any_ne_all(e);
            }
        }
    }
    Some(q)
}

fn is_eq_any_or_ne_all(e: &Expr) -> bool {
    match e {
        Expr::AnyOp {
            compare_op: BinaryOperator::Eq,
            right,
            ..
        }
        | Expr::AllOp {
            compare_op: BinaryOperator::NotEq,
            right,
            ..
        } => matches!(right.as_ref(), Expr::Subquery(_)),
        _ => false,
    }
}

fn has_eq_any_ne_all(e: &Expr) -> bool {
    is_eq_any_or_ne_all(e) || crate::ast_util::expr_operands(e).into_iter().any(has_eq_any_ne_all)
}

fn map_eq_any_ne_all(e: &Expr) -> Expr {
    use sqlparser::ast::CaseWhen;
    // `= ANY` → IN ; `<> ALL` → NOT IN. The subquery's `Query` body (`SetExpr`)
    // becomes the `InSubquery` subquery; an ANY/ALL subquery carries no
    // meaningful ORDER BY / LIMIT (dropping the envelope matches the IN AST,
    // whose subquery is itself a bare `SetExpr`).
    if let Expr::AnyOp {
        left,
        compare_op: BinaryOperator::Eq,
        right,
        ..
    }
    | Expr::AllOp {
        left,
        compare_op: BinaryOperator::NotEq,
        right,
    } = e
    {
        if let Expr::Subquery(sub) = right.as_ref() {
            let negated = matches!(e, Expr::AllOp { .. });
            return Expr::InSubquery {
                expr: Box::new(map_eq_any_ne_all(left)),
                subquery: sub.body.clone(),
                negated,
            };
        }
    }
    match e {
        Expr::BinaryOp { left, op, right } => Expr::BinaryOp {
            left: Box::new(map_eq_any_ne_all(left)),
            op: op.clone(),
            right: Box::new(map_eq_any_ne_all(right)),
        },
        Expr::UnaryOp { op, expr } => Expr::UnaryOp {
            op: *op,
            expr: Box::new(map_eq_any_ne_all(expr)),
        },
        Expr::Nested(i) => Expr::Nested(Box::new(map_eq_any_ne_all(i))),
        Expr::Case {
            operand,
            conditions,
            else_result,
        } => Expr::Case {
            operand: operand.as_ref().map(|o| Box::new(map_eq_any_ne_all(o))),
            conditions: conditions
                .iter()
                .map(|cw| CaseWhen {
                    condition: map_eq_any_ne_all(&cw.condition),
                    result: map_eq_any_ne_all(&cw.result),
                })
                .collect(),
            else_result: else_result.as_ref().map(|er| Box::new(map_eq_any_ne_all(er))),
        },
        other => other.clone(),
    }
}

/// The resolved outer relation — the view's single FROM table.
struct OuterCtx {
    alias: String,
    tid: u64,
    schema: Rc<Schema>,
}

/// A single scalar subquery resolved against the (outer, inner) scope: the
/// correlation key pairs (`(outer_col, inner_col)`, empty ⇒ uncorrelated), the
/// inner relation, the aggregate, and the inner-local pre-filter conjuncts.
struct LoweredSub<'a> {
    /// `(outer_col_idx, inner_col_idx)` equality correlation pairs.
    corr: Vec<(usize, usize)>,
    inner_tid: u64,
    inner_schema: Rc<Schema>,
    /// The subquery's inner FROM item (cloned for `Gᵢ`'s FROM).
    inner_from: TableFactor,
    /// The aggregate SELECT expression (`COUNT(*)`, `SUM(e)`, or a synthesized
    /// `MAX(y)` / `MIN(y)` for a range quantifier).
    agg_expr: Expr,
    agg_func: AggFunc,
    /// The aggregate's static result type (`agg_result_type`).
    agg_tc: TypeCode,
    /// Inner-local WHERE conjuncts (reference only the inner relation).
    inner_local: Vec<&'a Expr>,
}

impl LoweredSub<'_> {
    fn is_correlated(&self) -> bool {
        !self.corr.is_empty()
    }
    /// COUNT / COUNT(x) are never NULL: their subquery node substitutes to
    /// `COALESCE(col, 0)`, and they can be constant-folded under `IS [NOT] NULL`.
    fn is_count(&self) -> bool {
        matches!(self.agg_func, AggFunc::Count | AggFunc::CountNonNull)
    }
}

/// Mutable accumulators for the H join chain being synthesized.
struct Acc {
    /// Synthesized join steps for `H`.
    joins: Vec<Join>,
    /// Extra `H` projection items — one per correlated subquery's aggregate column.
    agg_proj: Vec<SelectItem>,
    /// Running synthetic-name counter (one per subquery occurrence).
    counter: usize,
    /// Names already taken (outer columns + minted synthetics), so a fresh name
    /// never collides with a user column or another subquery's column.
    taken: HashSet<String>,
}

impl Acc {
    /// A minted name distinct from every taken name (append `_N` on collision).
    fn fresh(&mut self, base: &str) -> String {
        let mut name = base.to_string();
        let mut n = 0u32;
        while self.taken.contains(&name.to_ascii_lowercase()) {
            n += 1;
            name = format!("{base}_{n}");
        }
        self.taken.insert(name.to_ascii_lowercase());
        name
    }
}

pub(crate) fn emit_scalar_subquery_pieces(
    client: &mut GnitzClient,
    final_vid: u64,
    select: &Select,
    binder: &mut Binder<'_>,
    chain: &mut ViewChain,
) -> Result<EmitPieces, GnitzSqlError> {
    // The scalar-subquery view consumes FROM + WHERE + projection; reject GROUP BY /
    // HAVING / DISTINCT and the exotic tail so a dropped clause is a clean error.
    // (The classifier already guaranteed a single non-JOIN FROM and no outer aggregate.)
    reject_unhonored_select_clauses(
        select,
        HonoredClauses {
            where_filter: true,
            grouping: false,
            distinct: false,
        },
        "CREATE VIEW scalar subquery",
    )?;

    // ── Outer relation ───────────────────────────────────────────────────────
    let (outer_name, outer_alias) =
        extract_table_name_and_alias(&select.from[0].relation, "CREATE VIEW scalar subquery")?;
    let (outer_tid, outer_schema) = binder.resolve(client, &outer_name)?;
    let outer = OuterCtx {
        alias: outer_alias,
        tid: outer_tid,
        schema: outer_schema,
    };

    let mut taken: HashSet<String> = outer
        .schema
        .columns
        .iter()
        .map(|c| c.name.to_ascii_lowercase())
        .collect();
    // Reserve the synthetic segment aliases too, so a minted column never shadows one.
    taken.insert(outer.alias.to_ascii_lowercase());
    let mut acc = Acc {
        joins: Vec::new(),
        agg_proj: Vec::new(),
        counter: 0,
        taken,
    };

    // ── Phase 1: WHERE conjuncts ─────────────────────────────────────────────
    // An uncorrelated scalar comparison that IS a whole top-level conjunct is
    // consumed as an INNER join-as-filter (§2); every other conjunct is rewritten,
    // substituting each correlated subquery with its materialized aggregate column.
    let mut rewritten_conjuncts: Vec<Expr> = Vec::new();
    if let Some(sel) = &select.selection {
        let mut conjuncts = Vec::new();
        flatten_conjuncts(sel, &mut conjuncts);
        for &c in &conjuncts {
            if let Some((col_expr, op)) = peel_scalar_comparison(c) {
                // Resolve the subquery to decide correlated vs uncorrelated.
                let q = match right_subquery(c, &col_expr) {
                    Some(q) => q,
                    None => unreachable!("peel_scalar_comparison implies a subquery operand"),
                };
                let lowered = lower_subquery(client, binder, q, &outer)?;
                if !lowered.is_correlated() {
                    build_uncorrelated_join(client, binder, chain, &mut acc, &lowered, &col_expr, op, &outer)?;
                    continue; // conjunct consumed by the join ON
                }
            }
            // An uncorrelated range `x OP ANY (SELECT y …)` conjunct → pure-range
            // INNER join-as-filter against `G_global(MAX/MIN(y))` (the join is the
            // existence test). A correlated ANY (or ANY under OR / in CASE) falls
            // through to `rewrite_expr` below.
            if let Some((left, op, q)) = peel_range_any(c) {
                let agg = quantifier_agg(true, &op);
                let lowered = lower_quantifier(client, binder, q, &outer, agg)?;
                if !lowered.is_correlated() {
                    build_uncorrelated_join(client, binder, chain, &mut acc, &lowered, left, op, &outer)?;
                    continue;
                }
            }
            rewritten_conjuncts.push(rewrite_expr(client, binder, chain, &mut acc, c, &outer)?);
        }
    }

    // ── Phase 2: projection ──────────────────────────────────────────────────
    let mut final_proj: Vec<SelectItem> = Vec::new();
    for item in &select.projection {
        match item {
            SelectItem::Wildcard(_) => final_proj.extend(expand_outer_columns(&outer)),
            SelectItem::QualifiedWildcard(kind, _) => {
                // `a.*` — only the outer alias is a valid qualifier here.
                let name = qualified_wildcard_prefix(kind);
                if !name.eq_ignore_ascii_case(&outer.alias) {
                    return Err(GnitzSqlError::Bind(format!(
                        "qualified wildcard '{name}.*' does not name the view's FROM relation"
                    )));
                }
                final_proj.extend(expand_outer_columns(&outer));
            }
            SelectItem::UnnamedExpr(e) => final_proj.push(SelectItem::UnnamedExpr(rewrite_expr(
                client, binder, chain, &mut acc, e, &outer,
            )?)),
            SelectItem::ExprWithAlias { expr, alias } => final_proj.push(SelectItem::ExprWithAlias {
                expr: rewrite_expr(client, binder, chain, &mut acc, expr, &outer)?,
                alias: alias.clone(),
            }),
        }
    }

    // ── Build final = linear filter/map over H (or the outer directly) ───────
    // Every subquery may have been folded away (a lone COUNT under `IS [NOT]
    // NULL`), leaving no join — the view is then a plain linear filter/map over
    // the outer table with the folded constants substituted.
    let mut final_select = select.clone();
    final_select.group_by = GroupByExpr::Expressions(Vec::new(), Vec::new());
    final_select.selection = fold_conjuncts(rewritten_conjuncts);
    final_select.projection = final_proj;
    let over_h = !acc.joins.is_empty();
    if !over_h {
        // No join: the final linear runs over the outer relation as written. The
        // rewritten refs are all outer columns + folded constants.
        final_select.from[0].joins = Vec::new();
    } else {
        // H = outer ⟕/⋈ every Gᵢ, projecting every outer column plus each
        // correlated aggregate; the final linear then runs over H.
        let h_alias = fresh_alias(&mut acc.taken, "x4h");
        let mut join_select = select.clone();
        let mut h_proj = expand_outer_columns(&outer);
        h_proj.extend(std::mem::take(&mut acc.agg_proj));
        join_select.projection = h_proj;
        join_select.from[0].joins = std::mem::take(&mut acc.joins);
        join_select.selection = None;
        join_select.group_by = GroupByExpr::Expressions(Vec::new(), Vec::new());
        join_select.having = None;
        join_select.distinct = None;
        let (h_tid, h_schema) = chain.add_segment(client, |client, chain, vid| {
            join::plan_join_chain(client, binder, vid, &join_select, chain)
        })?;
        binder.cache_alias(&h_alias, (h_tid, Rc::clone(&h_schema)))?;
        final_select.from[0].relation = table_factor(&h_alias);
        final_select.from[0].joins = Vec::new();
    }
    let rel = lower_linear(client, binder, &final_select)?;
    // A final over the exchange `H` must be fan_out-backfilled after H fills, so
    // it needs an exchange node; a final over the base outer is backfilled inline.
    if over_h {
        simple::emit_linear_sharded(final_vid, rel)
    } else {
        simple::emit_linear(final_vid, rel)
    }
}

/// The shared front half of subquery lowering: reject unsupported clauses,
/// resolve the inner relation, split its WHERE into inner-local vs equality
/// correlation conjuncts, and return the single projection expression. Used by
/// both the scalar-aggregate and the range-quantifier lowerings.
struct SubSplit<'a> {
    corr: Vec<(usize, usize)>,
    inner_tid: u64,
    inner_schema: Rc<Schema>,
    inner_from: TableFactor,
    inner_local: Vec<&'a Expr>,
    proj_expr: &'a Expr,
}

fn resolve_and_split<'a>(
    client: &mut GnitzClient,
    binder: &mut Binder<'_>,
    q: &'a Query,
    outer: &OuterCtx,
) -> Result<SubSplit<'a>, GnitzSqlError> {
    reject_unhonored_query_clauses(
        q,
        HonoredQueryClauses {
            with: false,
            limit: false,
        },
        "subquery",
    )?;
    let inner_select = match q.body.as_ref() {
        SetExpr::Select(s) => s.as_ref(),
        _ => {
            return Err(GnitzSqlError::Unsupported(
                "a subquery must be a plain SELECT; compose via views".into(),
            ))
        }
    };
    reject_unhonored_select_clauses(
        inner_select,
        HonoredClauses {
            where_filter: true,
            grouping: false,
            distinct: false,
        },
        "subquery",
    )?;
    if inner_select.distinct.is_some() {
        return Err(GnitzSqlError::Unsupported(
            "a scalar/quantifier subquery cannot be DISTINCT".into(),
        ));
    }
    // Item 2: an inner GROUP BY / HAVING breaks the one-row-per-key guarantee.
    if crate::ast_util::group_by_is_present(&inner_select.group_by) || inner_select.having.is_some() {
        return Err(GnitzSqlError::Unsupported(
            "a scalar/quantifier subquery cannot carry its own GROUP BY or HAVING".into(),
        ));
    }
    // The inner FROM must be one plain base relation (no joins / derived table).
    if inner_select.from.len() != 1
        || !inner_select.from[0].joins.is_empty()
        || matches!(&inner_select.from[0].relation, TableFactor::Derived { .. })
    {
        return Err(GnitzSqlError::Unsupported(
            "a scalar/quantifier subquery's FROM must be a single base table without JOINs; compose via views".into(),
        ));
    }
    let (inner_name, inner_alias) = extract_table_name_and_alias(&inner_select.from[0].relation, "subquery")?;
    let (inner_tid, inner_schema) = binder.resolve(client, &inner_name)?;
    if outer.alias.eq_ignore_ascii_case(&inner_alias) {
        return Err(GnitzSqlError::Bind(format!(
            "relation alias '{inner_alias}' is used by both the view FROM and its subquery; rename one"
        )));
    }
    // NOTE: `outer.tid == inner_tid` (a self-correlated subquery) is *supported* —
    // `Gᵢ` is a distinct reduce segment (a distinct source id), so `H = outer ⋈ Gᵢ`
    // is the shared-source-branches shape (distinct dependency edges → separate
    // epochs), never a same-source-in-one-epoch self-join.

    // The single projection expression (an aggregate for a scalar subquery, or a
    // column for a range quantifier — the caller interprets it).
    let proj_expr = match inner_select.projection.as_slice() {
        [SelectItem::UnnamedExpr(e)] | [SelectItem::ExprWithAlias { expr: e, .. }] => e,
        _ => {
            return Err(GnitzSqlError::Unsupported(
                "a scalar/quantifier subquery must select exactly one expression".into(),
            ))
        }
    };

    // Split the inner WHERE into inner-local vs correlation conjuncts.
    let outer_n = outer.schema.columns.len();
    let alias_map = build_alias_map(&[
        (&outer.alias, outer.tid, &outer.schema),
        (&inner_alias, inner_tid, &inner_schema),
    ]);
    let mut inner_conjuncts: Vec<&Expr> = Vec::new();
    if let Some(w) = &inner_select.selection {
        flatten_conjuncts(w, &mut inner_conjuncts);
    }
    let mut inner_local: Vec<&Expr> = Vec::new();
    let mut corr_acc: Option<Expr> = None;
    for &c in &inner_conjuncts {
        let (mut o_refs, mut i_refs) = (0usize, 0usize);
        count_side_refs(c, &alias_map, outer_n, &mut o_refs, &mut i_refs)?;
        if o_refs == 0 {
            inner_local.push(c);
        } else if i_refs == 0 {
            return Err(GnitzSqlError::Unsupported(
                "a subquery WHERE conjunct references only the outer relation; \
                 move it into the view's own WHERE"
                    .into(),
            ));
        } else {
            and_into(&mut corr_acc, c.clone());
        }
    }

    // Correlation must be equality-only (a range correlation with aggregation is
    // rejected). Extract the equi key pairs via the join-ON vocabulary.
    let corr = match corr_acc {
        None => Vec::new(),
        Some(corr_expr) => {
            let (left_cols, right_cols, _tcs, range, residual) =
                extract_join_predicates(&corr_expr, &outer.schema, &inner_schema, &alias_map)?;
            if range.is_some() {
                return Err(GnitzSqlError::Unsupported(
                    "a scalar/quantifier subquery cannot use a range correlation (only equality \
                     `inner_col = outer_col` conjuncts are supported)"
                        .into(),
                ));
            }
            if !residual.is_empty() {
                return Err(GnitzSqlError::Unsupported(
                    "a scalar/quantifier subquery's correlation contains a conjunct that is not an \
                     `inner_col = outer_col` equality; filter inside the subquery or a wrapping view"
                        .into(),
                ));
            }
            left_cols.into_iter().zip(right_cols).collect()
        }
    };

    Ok(SubSplit {
        corr,
        inner_tid,
        inner_schema,
        inner_from: inner_select.from[0].relation.clone(),
        inner_local,
        proj_expr,
    })
}

/// Lower a scalar aggregate subquery: its projection is a single aggregate.
fn lower_subquery<'a>(
    client: &mut GnitzClient,
    binder: &mut Binder<'_>,
    q: &'a Query,
    outer: &OuterCtx,
) -> Result<LoweredSub<'a>, GnitzSqlError> {
    let split = resolve_and_split(client, binder, q, outer)?;
    let (agg_func, agg_arg_col) = classify_aggregate(split.proj_expr, &split.inner_schema)?;
    let agg_tc = group_by::agg_result_type(agg_func, agg_arg_col, &split.inner_schema);
    Ok(LoweredSub {
        corr: split.corr,
        inner_tid: split.inner_tid,
        inner_schema: split.inner_schema,
        inner_from: split.inner_from,
        agg_expr: split.proj_expr.clone(),
        agg_func,
        agg_tc,
        inner_local: split.inner_local,
    })
}

/// Lower a range ANY/ALL subquery: its projection is a single **non-nullable**
/// column `y`, and the caller picks `MAX`/`MIN` for the quantifier direction.
/// `Gᵢ` is built with the synthesized `MAX(y)` / `MIN(y)`. The non-nullable
/// restriction makes the MIN/MAX rewrite coincide exactly with SQL's 3VL ANY/ALL
/// (a NULL in the set would make them three-valued under negation in a way the
/// extremum cannot reproduce).
fn lower_quantifier<'a>(
    client: &mut GnitzClient,
    binder: &mut Binder<'_>,
    q: &'a Query,
    outer: &OuterCtx,
    agg_func: AggFunc,
) -> Result<LoweredSub<'a>, GnitzSqlError> {
    let split = resolve_and_split(client, binder, q, outer)?;
    let col = match bind_single_table(split.proj_expr, &split.inner_schema)? {
        BoundExpr::ColRef(c) => c,
        _ => {
            return Err(GnitzSqlError::Unsupported(
                "a range ANY/ALL subquery must select a single non-nullable column".into(),
            ))
        }
    };
    if split.inner_schema.columns[col].is_nullable {
        return Err(GnitzSqlError::Unsupported(
            "a range ANY/ALL subquery's column must be NOT NULL — a NULL in the set makes ANY/ALL \
             three-valued in a way the MIN/MAX rewrite cannot reproduce"
                .into(),
        ));
    }
    let agg_tc = group_by::agg_result_type(agg_func, Some(col), &split.inner_schema);
    Ok(LoweredSub {
        corr: split.corr,
        inner_tid: split.inner_tid,
        inner_schema: split.inner_schema,
        inner_from: split.inner_from,
        agg_expr: agg_call(agg_func, split.proj_expr.clone()),
        agg_func,
        agg_tc,
        inner_local: split.inner_local,
    })
}

/// Build a correlated subquery's `Gᵢ` (grouped reduce) + LEFT join step, and
/// return the name of `H`'s aggregate column the node substitutes to.
fn build_correlated_join(
    client: &mut GnitzClient,
    binder: &mut Binder<'_>,
    chain: &mut ViewChain,
    acc: &mut Acc,
    sub: &LoweredSub<'_>,
    outer: &OuterCtx,
) -> Result<String, GnitzSqlError> {
    let i = acc.counter;
    acc.counter += 1;
    let seg_alias = fresh_alias(&mut acc.taken, &format!("x4sq{i}"));
    let agg_name = acc.fresh(&format!("x4agg{i}"));

    // A float correlation (GROUP BY) column would surface as the reduce's
    // reject_float_key "GROUP BY" error; reject up front with a scalar message.
    for &(_, inner_col) in &sub.corr {
        if sub.inner_schema.columns[inner_col].type_code.is_float() {
            return Err(GnitzSqlError::Unsupported(format!(
                "a scalar subquery's correlation column '{}' is float; a float value cannot be a \
                 correlation/GROUP BY key",
                sub.inner_schema.columns[inner_col].name
            )));
        }
    }

    // Group-column synthetic names, one per correlation pair.
    let mut k_names: Vec<String> = Vec::with_capacity(sub.corr.len());
    for m in 0..sub.corr.len() {
        k_names.push(acc.fresh(&format!("x4k{i}_{m}")));
    }

    // Gᵢ:  SELECT inner.k AS x4k{i}_m …, <agg> AS x4agg{i}
    //      FROM inner [WHERE <inner-local>] GROUP BY inner.k …
    let mut g_proj: Vec<SelectItem> = Vec::with_capacity(sub.corr.len() + 1);
    let mut group_by: Vec<Expr> = Vec::with_capacity(sub.corr.len());
    for (m, &(_, inner_col)) in sub.corr.iter().enumerate() {
        let col_name = sub.inner_schema.columns[inner_col].name.clone();
        g_proj.push(SelectItem::ExprWithAlias {
            expr: Expr::Identifier(Ident::new(col_name.clone())),
            alias: Ident::new(k_names[m].clone()),
        });
        group_by.push(Expr::Identifier(Ident::new(col_name)));
    }
    g_proj.push(SelectItem::ExprWithAlias {
        expr: sub.agg_expr.clone(),
        alias: Ident::new(agg_name.clone()),
    });
    let g_select = Select {
        projection: g_proj,
        from: vec![sqlparser::ast::TableWithJoins {
            relation: sub.inner_from.clone(),
            joins: Vec::new(),
        }],
        selection: fold_refs(&sub.inner_local),
        group_by: GroupByExpr::Expressions(group_by, Vec::new()),
        ..blank_select()
    };
    let source = (sub.inner_tid, Rc::clone(&sub.inner_schema));
    let (g_vid, g_schema) = chain.add_segment(client, move |client, _chain, vid| {
        group_by::emit_group_by_pieces(client, vid, &g_select, source)
    })?;
    binder.cache_alias(&seg_alias, (g_vid, g_schema))?;

    // LEFT JOIN step:  outer LEFT JOIN seg ON a.k = seg.x4k{i}_m AND …
    let on = correlation_on(outer, &sub.corr, &seg_alias, &k_names);
    acc.joins.push(Join {
        relation: table_factor(&seg_alias),
        global: false,
        join_operator: JoinOperator::Left(JoinConstraint::On(on)),
    });
    acc.agg_proj.push(SelectItem::UnnamedExpr(Expr::CompoundIdentifier(vec![
        Ident::new(seg_alias),
        Ident::new(agg_name.clone()),
    ])));
    Ok(agg_name)
}

/// Build an uncorrelated scalar comparison's `G_global` (one-row reduce) + INNER
/// join-as-filter step: `outer_col OP sub.agg`. `=` → equi join, range OP →
/// pure-range INNER join. The comparison is consumed here (dropped from WHERE).
#[allow(clippy::too_many_arguments)]
fn build_uncorrelated_join(
    client: &mut GnitzClient,
    binder: &mut Binder<'_>,
    chain: &mut ViewChain,
    acc: &mut Acc,
    sub: &LoweredSub<'_>,
    col_expr: &Expr,
    op: BinaryOperator,
    outer: &OuterCtx,
) -> Result<(), GnitzSqlError> {
    let i = acc.counter;
    acc.counter += 1;
    let seg_alias = fresh_alias(&mut acc.taken, &format!("x4sq{i}"));
    let agg_name = acc.fresh(&format!("x4agg{i}"));

    // Item 4: both operands are join keys → reject a float aggregate or float outer
    // operand up front with a scalar-specific message.
    if sub.agg_tc.is_float() {
        return Err(GnitzSqlError::Unsupported(
            "an uncorrelated scalar aggregate compared to an outer column must be integer-typed \
             (AVG and a float SUM/MIN/MAX are rejected — the aggregate value is a join key)"
                .into(),
        ));
    }
    let outer_col = match bind_single_table(col_expr, &outer.schema)? {
        BoundExpr::ColRef(c) => c,
        _ => {
            return Err(GnitzSqlError::Unsupported(
                "an uncorrelated scalar aggregate comparison must compare a plain outer column \
                 with the subquery"
                    .into(),
            ))
        }
    };
    if outer.schema.columns[outer_col].type_code.is_float() {
        return Err(GnitzSqlError::Unsupported(
            "an uncorrelated scalar aggregate comparison cannot use a float outer column (it is a \
             join key)"
                .into(),
        ));
    }

    // G_global:  SELECT <agg> AS x4agg{i} FROM inner [WHERE <inner-local>]
    let g_select = Select {
        projection: vec![SelectItem::ExprWithAlias {
            expr: sub.agg_expr.clone(),
            alias: Ident::new(agg_name.clone()),
        }],
        from: vec![sqlparser::ast::TableWithJoins {
            relation: sub.inner_from.clone(),
            joins: Vec::new(),
        }],
        selection: fold_refs(&sub.inner_local),
        group_by: GroupByExpr::Expressions(Vec::new(), Vec::new()),
        ..blank_select()
    };
    let source = (sub.inner_tid, Rc::clone(&sub.inner_schema));
    let (g_vid, g_schema) = chain.add_segment(client, move |client, _chain, vid| {
        group_by::emit_group_by_pieces(client, vid, &g_select, source)
    })?;
    binder.cache_alias(&seg_alias, (g_vid, g_schema))?;

    // INNER JOIN ON  outer_col OP seg.x4agg{i}
    let on = Expr::BinaryOp {
        left: Box::new(col_expr.clone()),
        op,
        right: Box::new(Expr::CompoundIdentifier(vec![
            Ident::new(seg_alias.clone()),
            Ident::new(agg_name),
        ])),
    };
    acc.joins.push(Join {
        relation: table_factor(&seg_alias),
        global: false,
        join_operator: JoinOperator::Inner(JoinConstraint::On(on)),
    });
    Ok(())
}

/// Recursively rewrite an expression, substituting each correlated scalar
/// subquery with its materialized aggregate column and folding COUNT under
/// `IS [NOT] NULL` (Item 3) and non-last `COALESCE` (Issue D).
fn rewrite_expr(
    client: &mut GnitzClient,
    binder: &mut Binder<'_>,
    chain: &mut ViewChain,
    acc: &mut Acc,
    e: &Expr,
    outer: &OuterCtx,
) -> Result<Expr, GnitzSqlError> {
    match e {
        Expr::Subquery(q) => substitute_scalar(client, binder, chain, acc, q, outer),
        Expr::AnyOp {
            left,
            compare_op,
            right,
            ..
        } => rewrite_quantifier(client, binder, chain, acc, left, compare_op, right, true, outer),
        Expr::AllOp {
            left,
            compare_op,
            right,
        } => rewrite_quantifier(client, binder, chain, acc, left, compare_op, right, false, outer),
        Expr::IsNull(inner) => rewrite_null_test(client, binder, chain, acc, inner, true, outer),
        Expr::IsNotNull(inner) => rewrite_null_test(client, binder, chain, acc, inner, false, outer),
        Expr::BinaryOp { left, op, right } => Ok(Expr::BinaryOp {
            left: Box::new(rewrite_expr(client, binder, chain, acc, left, outer)?),
            op: op.clone(),
            right: Box::new(rewrite_expr(client, binder, chain, acc, right, outer)?),
        }),
        Expr::UnaryOp { op, expr } => Ok(Expr::UnaryOp {
            op: *op,
            expr: Box::new(rewrite_expr(client, binder, chain, acc, expr, outer)?),
        }),
        Expr::Nested(inner) => Ok(Expr::Nested(Box::new(rewrite_expr(
            client, binder, chain, acc, inner, outer,
        )?))),
        Expr::Between {
            expr,
            negated,
            low,
            high,
        } => Ok(Expr::Between {
            expr: Box::new(rewrite_expr(client, binder, chain, acc, expr, outer)?),
            negated: *negated,
            low: Box::new(rewrite_expr(client, binder, chain, acc, low, outer)?),
            high: Box::new(rewrite_expr(client, binder, chain, acc, high, outer)?),
        }),
        Expr::Case {
            operand,
            conditions,
            else_result,
        } => {
            let operand = match operand {
                Some(o) => Some(Box::new(rewrite_expr(client, binder, chain, acc, o, outer)?)),
                None => None,
            };
            let mut conds = Vec::with_capacity(conditions.len());
            for cw in conditions {
                conds.push(sqlparser::ast::CaseWhen {
                    condition: rewrite_expr(client, binder, chain, acc, &cw.condition, outer)?,
                    result: rewrite_expr(client, binder, chain, acc, &cw.result, outer)?,
                });
            }
            let else_result = match else_result {
                Some(e) => Some(Box::new(rewrite_expr(client, binder, chain, acc, e, outer)?)),
                None => None,
            };
            Ok(Expr::Case {
                operand,
                conditions: conds,
                else_result,
            })
        }
        Expr::Function(f) if fn_name_is(f, "coalesce") => rewrite_coalesce(client, binder, chain, acc, f, outer),
        Expr::Function(f) => rewrite_function(client, binder, chain, acc, f, outer),
        // A leaf (Identifier, CompoundIdentifier, Value, …) with no subquery inside.
        other => Ok(other.clone()),
    }
}

/// Substitute a scalar subquery node with its aggregate column reference (or the
/// COUNT-bug repair `COALESCE(col, 0)`); an uncorrelated scalar here (outside a
/// top-level WHERE comparison conjunct) is rejected.
fn substitute_scalar(
    client: &mut GnitzClient,
    binder: &mut Binder<'_>,
    chain: &mut ViewChain,
    acc: &mut Acc,
    q: &Query,
    outer: &OuterCtx,
) -> Result<Expr, GnitzSqlError> {
    let sub = lower_subquery(client, binder, q, outer)?;
    if !sub.is_correlated() {
        return Err(GnitzSqlError::Unsupported(
            "an uncorrelated scalar aggregate subquery is only supported as a top-level WHERE \
             comparison conjunct (`outer_col OP (SELECT AGG …)`)"
                .into(),
        ));
    }
    let is_count = sub.is_count();
    let agg_name = build_correlated_join(client, binder, chain, acc, &sub, outer)?;
    let col = Expr::Identifier(Ident::new(agg_name));
    Ok(if is_count { coalesce_zero(col) } else { col })
}

/// Rewrite a range `x OP ANY/ALL (SELECT y …)` into a predicate over the
/// materialized extremum `m` of its correlation group. `= ANY` / `<> ALL` are
/// rewritten to IN / NOT IN before this builder runs, so reaching here with `=`
/// (ALL) or `<>` (ANY) is the genuinely unsupported form. An uncorrelated range
/// ANY is only handled as a top-level WHERE conjunct (Phase 1); ALL uncorrelated
/// is unsupported (empty-set-TRUE would need a keyless preserved join).
#[allow(clippy::too_many_arguments)]
fn rewrite_quantifier(
    client: &mut GnitzClient,
    binder: &mut Binder<'_>,
    chain: &mut ViewChain,
    acc: &mut Acc,
    left: &Expr,
    compare_op: &BinaryOperator,
    right: &Expr,
    is_any: bool,
    outer: &OuterCtx,
) -> Result<Expr, GnitzSqlError> {
    let Expr::Subquery(q) = right else {
        return Err(GnitzSqlError::Unsupported(
            "an ANY/ALL operator's right operand must be a subquery".into(),
        ));
    };
    reject_unsupported_quantifier(is_any, compare_op)?;
    let agg_func = quantifier_agg(is_any, compare_op);
    let sub = lower_quantifier(client, binder, q, outer, agg_func)?;
    if !sub.is_correlated() {
        return Err(GnitzSqlError::Unsupported(
            "an uncorrelated range ANY subquery is only supported as a top-level WHERE conjunct, \
             and uncorrelated range ALL is unsupported"
                .into(),
        ));
    }
    let m_name = build_correlated_join(client, binder, chain, acc, &sub, outer)?;
    let m = Expr::Identifier(Ident::new(m_name));
    // ANY over ∅ is FALSE, ALL over ∅ is TRUE — the LEFT-join null-fill sets
    // `m = NULL` for an empty group, so the explicit null test makes the edge a
    // definite constant (exact under negation), never a dropped UNKNOWN.
    let cmp = Expr::BinaryOp {
        left: Box::new(left.clone()),
        op: compare_op.clone(),
        right: Box::new(m.clone()),
    };
    Ok(if is_any {
        // m IS NOT NULL AND left OP m
        and_expr(Expr::IsNotNull(Box::new(m)), cmp)
    } else {
        // m IS NULL OR left OP m
        or_expr(Expr::IsNull(Box::new(m)), cmp)
    })
}

/// `= ALL` and `<> ANY` are unsupported; `= ANY` / `<> ALL` are rewritten to
/// IN / NOT IN upstream so they never reach here.
fn reject_unsupported_quantifier(is_any: bool, op: &BinaryOperator) -> Result<(), GnitzSqlError> {
    match (is_any, op) {
        (true, BinaryOperator::Eq) => Err(GnitzSqlError::Plan("`= ANY` should have been rewritten to IN".into())),
        (false, BinaryOperator::NotEq) => Err(GnitzSqlError::Plan(
            "`<> ALL` should have been rewritten to NOT IN".into(),
        )),
        (true, BinaryOperator::NotEq) => Err(GnitzSqlError::Unsupported(
            "`<> ANY (SELECT …)` is not supported (use NOT (x = ALL …) semantics via a wrapping view)".into(),
        )),
        (false, BinaryOperator::Eq) => Err(GnitzSqlError::Unsupported("`= ALL (SELECT …)` is not supported".into())),
        (_, BinaryOperator::Lt | BinaryOperator::LtEq | BinaryOperator::Gt | BinaryOperator::GtEq) => Ok(()),
        _ => Err(GnitzSqlError::Unsupported(
            "only range (<, <=, >, >=), `= ANY`, and `<> ALL` quantified comparisons are supported".into(),
        )),
    }
}

/// The extremum a range quantifier reduces to: `x < ANY(S) ⟺ x < MAX(S)`,
/// `x > ANY(S) ⟺ x > MIN(S)`; `x < ALL(S) ⟺ x < MIN(S)`,
/// `x > ALL(S) ⟺ x > MAX(S)`.
fn quantifier_agg(is_any: bool, op: &BinaryOperator) -> AggFunc {
    let less = matches!(op, BinaryOperator::Lt | BinaryOperator::LtEq);
    if is_any == less {
        AggFunc::Max
    } else {
        AggFunc::Min
    }
}

/// Rewrite `inner IS [NOT] NULL`. A COUNT scalar subquery is never NULL, so the
/// whole test folds to a constant and that occurrence is not decorrelated
/// (Item 3). A SUM/MIN/MAX/AVG subquery substitutes to a bare nullable column, so
/// the null test binds ordinarily.
fn rewrite_null_test(
    client: &mut GnitzClient,
    binder: &mut Binder<'_>,
    chain: &mut ViewChain,
    acc: &mut Acc,
    inner: &Expr,
    want_null: bool,
    outer: &OuterCtx,
) -> Result<Expr, GnitzSqlError> {
    if let Expr::Subquery(q) = inner {
        // Peek at the aggregate without building a segment.
        if subquery_is_count(client, binder, q, outer)? {
            // COUNT IS NULL → FALSE (1=0); COUNT IS NOT NULL → TRUE (1=1).
            return Ok(bool_const(!want_null));
        }
    }
    let inner = rewrite_expr(client, binder, chain, acc, inner, outer)?;
    Ok(if want_null {
        Expr::IsNull(Box::new(inner))
    } else {
        Expr::IsNotNull(Box::new(inner))
    })
}

/// Rewrite a `COALESCE(…)`. A non-nullable COUNT scalar subquery makes every
/// later operand dead (COALESCE returns the first non-NULL), so truncate the
/// operand list at the first COUNT subquery — that keeps the COUNT's computed
/// `COALESCE(col, 0)` repair in the *last* position, where `bind_coalesce` binds
/// it structurally instead of rejecting it as a non-column (Issue D).
fn rewrite_coalesce(
    client: &mut GnitzClient,
    binder: &mut Binder<'_>,
    chain: &mut ViewChain,
    acc: &mut Acc,
    f: &sqlparser::ast::Function,
    outer: &OuterCtx,
) -> Result<Expr, GnitzSqlError> {
    let args = function_positional_args(f)?;
    // First operand that is a COUNT scalar subquery (never NULL).
    let mut cut = args.len();
    for (j, a) in args.iter().enumerate() {
        if let Expr::Subquery(q) = a {
            if subquery_is_count(client, binder, q, outer)? {
                cut = j + 1;
                break;
            }
        }
    }
    let kept = &args[..cut.min(args.len())];
    let mut new_args: Vec<Expr> = Vec::with_capacity(kept.len());
    for a in kept {
        new_args.push(rewrite_expr(client, binder, chain, acc, a, outer)?);
    }
    Ok(coalesce_call(new_args))
}

/// Rewrite a non-COALESCE function's positional arguments (aggregates are only
/// valid inside a subquery, so a bare function here is rejected at bind — but its
/// arguments may still hide a subquery to substitute).
fn rewrite_function(
    client: &mut GnitzClient,
    binder: &mut Binder<'_>,
    chain: &mut ViewChain,
    acc: &mut Acc,
    f: &sqlparser::ast::Function,
    outer: &OuterCtx,
) -> Result<Expr, GnitzSqlError> {
    let args = match function_positional_args(f) {
        Ok(a) => a,
        // A non-positional function (e.g. `*`, named args) can't hold a scalar
        // subquery we handle; pass it through and let the binder reject it.
        Err(_) => return Ok(Expr::Function(f.clone())),
    };
    let mut new_args = Vec::with_capacity(args.len());
    for a in &args {
        new_args.push(rewrite_expr(client, binder, chain, acc, a, outer)?);
    }
    let mut f = f.clone();
    f.args = sqlparser::ast::FunctionArguments::List(sqlparser::ast::FunctionArgumentList {
        duplicate_treatment: None,
        args: new_args
            .into_iter()
            .map(|e| sqlparser::ast::FunctionArg::Unnamed(sqlparser::ast::FunctionArgExpr::Expr(e)))
            .collect(),
        clauses: Vec::new(),
    });
    Ok(Expr::Function(f))
}

// ── AST helpers ──────────────────────────────────────────────────────────────

/// Peel a scalar comparison conjunct `col OP (subquery)` / `(subquery) OP col`
/// into `(outer_col_expr, canonical_op)` where the op is oriented `col OP sub`.
/// Returns `None` unless one side is a bare subquery and the other a column ref
/// and OP ∈ {=, <, <=, >, >=}.
fn peel_scalar_comparison(c: &Expr) -> Option<(Expr, BinaryOperator)> {
    let Expr::BinaryOp { left, op, right } = c else {
        return None;
    };
    if !is_scalar_cmp_op(op) {
        return None;
    }
    let is_col = |e: &Expr| matches!(e, Expr::Identifier(_) | Expr::CompoundIdentifier(_));
    match (left.as_ref(), right.as_ref()) {
        (l, Expr::Subquery(_)) if is_col(l) => Some((l.clone(), op.clone())),
        (Expr::Subquery(_), r) if is_col(r) => Some((r.clone(), converse_binop(op))),
        _ => None,
    }
}

/// Peel a top-level `x OP ANY (subquery)` range conjunct into
/// `(&left, op, &subquery)`. Returns `None` unless the node is an `AnyOp` with a
/// range op over a bare subquery.
fn peel_range_any(c: &Expr) -> Option<(&Expr, BinaryOperator, &Query)> {
    let Expr::AnyOp {
        left,
        compare_op,
        right,
        ..
    } = c
    else {
        return None;
    };
    if !matches!(
        compare_op,
        BinaryOperator::Lt | BinaryOperator::LtEq | BinaryOperator::Gt | BinaryOperator::GtEq
    ) {
        return None;
    }
    match right.as_ref() {
        Expr::Subquery(q) => Some((left, compare_op.clone(), q)),
        _ => None,
    }
}

/// The subquery operand of a comparison conjunct whose other operand is `col`.
fn right_subquery<'a>(c: &'a Expr, _col: &Expr) -> Option<&'a Query> {
    let Expr::BinaryOp { left, right, .. } = c else {
        return None;
    };
    match (left.as_ref(), right.as_ref()) {
        (_, Expr::Subquery(q)) => Some(q),
        (Expr::Subquery(q), _) => Some(q),
        _ => None,
    }
}

fn is_scalar_cmp_op(op: &BinaryOperator) -> bool {
    matches!(
        op,
        BinaryOperator::Eq | BinaryOperator::Lt | BinaryOperator::LtEq | BinaryOperator::Gt | BinaryOperator::GtEq
    )
}

fn converse_binop(op: &BinaryOperator) -> BinaryOperator {
    match op {
        BinaryOperator::Lt => BinaryOperator::Gt,
        BinaryOperator::LtEq => BinaryOperator::GtEq,
        BinaryOperator::Gt => BinaryOperator::Lt,
        BinaryOperator::GtEq => BinaryOperator::LtEq,
        other => other.clone(), // Eq is symmetric
    }
}

/// Classify a single-aggregate SELECT expr → `(AggFunc, arg column)`.
fn classify_aggregate(e: &Expr, inner_schema: &Schema) -> Result<(AggFunc, Option<usize>), GnitzSqlError> {
    match bind_single_table(e, inner_schema)? {
        BoundExpr::AggCall { func, arg } => {
            let arg_col = match arg.as_deref() {
                Some(BoundExpr::ColRef(c)) => Some(*c),
                None => None,
                Some(_) => {
                    return Err(GnitzSqlError::Unsupported(
                        "a scalar subquery aggregate must be over a plain column or `*`".into(),
                    ))
                }
            };
            Ok((func, arg_col))
        }
        _ => Err(GnitzSqlError::Unsupported(
            "a scalar subquery must be a single aggregate over its correlation group".into(),
        )),
    }
}

/// Whether a subquery is a valid single COUNT/COUNT(x) aggregate (for the
/// IS-NULL / COALESCE folds) — a cheap peek that resolves the inner (cached) and
/// binds the aggregate but builds no segment.
fn subquery_is_count(
    client: &mut GnitzClient,
    binder: &mut Binder<'_>,
    q: &Query,
    outer: &OuterCtx,
) -> Result<bool, GnitzSqlError> {
    // A malformed subquery here is surfaced by the real lowering; a peek only
    // needs to answer "is this a COUNT?", so a resolution error means "not COUNT".
    // A COUNT is never NULL whether correlated or not, so the fold applies to
    // both — an uncorrelated COUNT under `IS [NOT] NULL` folds to a constant with
    // no segment (it never needs a keyless join).
    let Ok(sub) = lower_subquery(client, binder, q, outer) else {
        return Ok(false);
    };
    Ok(sub.is_count())
}

fn correlation_on(outer: &OuterCtx, corr: &[(usize, usize)], seg_alias: &str, k_names: &[String]) -> Expr {
    let mut acc: Option<Expr> = None;
    for (m, &(outer_col, _)) in corr.iter().enumerate() {
        let pair = Expr::BinaryOp {
            left: Box::new(Expr::CompoundIdentifier(vec![
                Ident::new(outer.alias.clone()),
                Ident::new(outer.schema.columns[outer_col].name.clone()),
            ])),
            op: BinaryOperator::Eq,
            right: Box::new(Expr::CompoundIdentifier(vec![
                Ident::new(seg_alias.to_string()),
                Ident::new(k_names[m].clone()),
            ])),
        };
        and_into(&mut acc, pair);
    }
    acc.expect("a correlated subquery has at least one correlation pair")
}

/// Every non-hidden outer column as a qualified projection item `outer.col`.
fn expand_outer_columns(outer: &OuterCtx) -> Vec<SelectItem> {
    outer
        .schema
        .columns
        .iter()
        .filter(|c| !c.is_hidden)
        .map(|c| {
            SelectItem::UnnamedExpr(Expr::CompoundIdentifier(vec![
                Ident::new(outer.alias.clone()),
                Ident::new(c.name.clone()),
            ]))
        })
        .collect()
}

fn qualified_wildcard_prefix(kind: &sqlparser::ast::SelectItemQualifiedWildcardKind) -> String {
    use sqlparser::ast::SelectItemQualifiedWildcardKind;
    match kind {
        SelectItemQualifiedWildcardKind::ObjectName(name) => name
            .0
            .last()
            .and_then(|p| p.as_ident())
            .map(|i| i.value.clone())
            .unwrap_or_default(),
        SelectItemQualifiedWildcardKind::Expr(_) => String::new(),
    }
}

fn fold_conjuncts(conjuncts: Vec<Expr>) -> Option<Expr> {
    let mut acc: Option<Expr> = None;
    for c in conjuncts {
        and_into(&mut acc, c);
    }
    acc
}

fn fold_refs(conjuncts: &[&Expr]) -> Option<Expr> {
    let mut acc: Option<Expr> = None;
    for &c in conjuncts {
        and_into(&mut acc, c.clone());
    }
    acc
}

fn fresh_alias(taken: &mut HashSet<String>, base: &str) -> String {
    let mut name = base.to_string();
    let mut n = 0u32;
    while taken.contains(&name.to_ascii_lowercase()) {
        n += 1;
        name = format!("{base}_{n}");
    }
    taken.insert(name.to_ascii_lowercase());
    name
}

fn table_factor(relation: &str) -> TableFactor {
    TableFactor::Table {
        name: ObjectName(vec![ObjectNamePart::Identifier(Ident::new(relation))]),
        alias: None,
        args: None,
        with_hints: Vec::new(),
        version: None,
        with_ordinality: false,
        partitions: Vec::new(),
        json_path: None,
        sample: None,
        index_hints: Vec::new(),
    }
}

fn num_zero() -> Expr {
    Expr::Value(ValueWithSpan {
        value: Value::Number("0".into(), false),
        span: Span::empty(),
    })
}

fn num_one() -> Expr {
    Expr::Value(ValueWithSpan {
        value: Value::Number("1".into(), false),
        span: Span::empty(),
    })
}

/// `1 = 1` (TRUE) / `1 = 0` (FALSE) as a boolean-typed constant expression.
fn bool_const(v: bool) -> Expr {
    Expr::BinaryOp {
        left: Box::new(num_one()),
        op: BinaryOperator::Eq,
        right: Box::new(if v { num_one() } else { num_zero() }),
    }
}

/// `COALESCE(col, 0)` — the COUNT-bug repair.
fn coalesce_zero(col: Expr) -> Expr {
    coalesce_call(vec![col, num_zero()])
}

fn and_expr(a: Expr, b: Expr) -> Expr {
    Expr::BinaryOp {
        left: Box::new(a),
        op: BinaryOperator::And,
        right: Box::new(b),
    }
}

fn or_expr(a: Expr, b: Expr) -> Expr {
    Expr::BinaryOp {
        left: Box::new(a),
        op: BinaryOperator::Or,
        right: Box::new(b),
    }
}

/// `MAX(arg)` / `MIN(arg)` as an aggregate call — the synthesized aggregate for a
/// range quantifier's `Gᵢ`.
fn agg_call(func: AggFunc, arg: Expr) -> Expr {
    let name = match func {
        AggFunc::Max => "max",
        AggFunc::Min => "min",
        _ => unreachable!("only MIN/MAX are synthesized for range quantifiers"),
    };
    Expr::Function(sqlparser::ast::Function {
        name: ObjectName(vec![ObjectNamePart::Identifier(Ident::new(name))]),
        uses_odbc_syntax: false,
        parameters: sqlparser::ast::FunctionArguments::None,
        args: sqlparser::ast::FunctionArguments::List(sqlparser::ast::FunctionArgumentList {
            duplicate_treatment: None,
            args: vec![sqlparser::ast::FunctionArg::Unnamed(
                sqlparser::ast::FunctionArgExpr::Expr(arg),
            )],
            clauses: Vec::new(),
        }),
        filter: None,
        null_treatment: None,
        over: None,
        within_group: Vec::new(),
    })
}

fn coalesce_call(args: Vec<Expr>) -> Expr {
    Expr::Function(sqlparser::ast::Function {
        name: ObjectName(vec![ObjectNamePart::Identifier(Ident::new("coalesce"))]),
        uses_odbc_syntax: false,
        parameters: sqlparser::ast::FunctionArguments::None,
        args: sqlparser::ast::FunctionArguments::List(sqlparser::ast::FunctionArgumentList {
            duplicate_treatment: None,
            args: args
                .into_iter()
                .map(|e| sqlparser::ast::FunctionArg::Unnamed(sqlparser::ast::FunctionArgExpr::Expr(e)))
                .collect(),
            clauses: Vec::new(),
        }),
        filter: None,
        null_treatment: None,
        over: None,
        within_group: Vec::new(),
    })
}

fn fn_name_is(f: &sqlparser::ast::Function, name: &str) -> bool {
    matches!(f.name.0.as_slice(), [part]
        if part.as_ident().is_some_and(|i| i.value.eq_ignore_ascii_case(name)))
}

fn function_positional_args(f: &sqlparser::ast::Function) -> Result<Vec<Expr>, GnitzSqlError> {
    use sqlparser::ast::{FunctionArg, FunctionArgExpr, FunctionArguments};
    let FunctionArguments::List(list) = &f.args else {
        return Err(GnitzSqlError::Unsupported(
            "function requires a positional argument list".into(),
        ));
    };
    if list.duplicate_treatment.is_some() || !list.clauses.is_empty() {
        return Err(GnitzSqlError::Unsupported("unsupported function qualifiers".into()));
    }
    list.args
        .iter()
        .map(|a| match a {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => Ok(e.clone()),
            _ => Err(GnitzSqlError::Unsupported(
                "function expects plain positional arguments".into(),
            )),
        })
        .collect()
}

/// A `Select` with every field defaulted (empty FROM/projection, no clauses) —
/// the base for synthesizing `Gᵢ` bodies.
fn blank_select() -> Select {
    Select {
        select_token: sqlparser::ast::helpers::attached_token::AttachedToken::empty(),
        distinct: None,
        top: None,
        top_before_distinct: false,
        projection: Vec::new(),
        into: None,
        from: Vec::new(),
        lateral_views: Vec::new(),
        prewhere: None,
        selection: None,
        group_by: GroupByExpr::Expressions(Vec::new(), Vec::new()),
        cluster_by: Vec::new(),
        distribute_by: Vec::new(),
        sort_by: Vec::new(),
        having: None,
        named_window: Vec::new(),
        qualify: None,
        window_before_qualify: false,
        value_table_mode: None,
        connect_by: None,
        flavor: sqlparser::ast::SelectFlavor::Standard,
    }
}
