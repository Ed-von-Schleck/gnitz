//! CREATE VIEW front door: validate the query envelope, inline CTEs, classify
//! the view's shape, and dispatch to the matching builder. The single module in
//! `plan/view` that knows about all the others.

use crate::ast_util::{
    extract_name, extract_table_factor_name, flatten_conjuncts, group_by_is_present, is_wildcard_projection,
    projection_has_aggregate,
};
use crate::bind::Binder;
use crate::error::GnitzSqlError;
use crate::plan::validate::{
    reject_unhonored_query_clauses, reject_unhonored_select_clauses, unsupported_clause, HonoredClauses,
    HonoredQueryClauses,
};
use crate::plan::view::{exists, group_by, join, set_op, simple};
use crate::SqlResult;
use gnitz_core::GnitzClient;
use sqlparser::ast::{Expr, Query, Select, SelectItem, SetExpr, SetOperator, SetQuantifier, Statement, UnaryOperator};

pub(crate) fn execute_create_view(
    client: &mut GnitzClient,
    schema_name: &str,
    view_name_obj: &sqlparser::ast::ObjectName,
    query: &Query,
    stmt: &Statement,
    binder: &mut Binder<'_>,
) -> Result<SqlResult, GnitzSqlError> {
    let view_name = extract_name(view_name_obj, "CREATE VIEW")?;

    // The CREATE VIEW envelope honors only `WITH` (inlined just below by `inline_ctes`); every
    // other tail clause (ORDER BY, LIMIT/OFFSET, FETCH, FOR UPDATE/SHARE, FOR XML/JSON, SETTINGS,
    // FORMAT) has no incremental-view semantics and would otherwise be silently dropped.
    reject_unhonored_query_clauses(
        query,
        HonoredQueryClauses {
            with: true,
            limit: false,
        },
        "CREATE VIEW",
    )?;

    let sql_text = format!("{stmt}");

    // Process CTEs (WITH clause) — inline them into the binder cache.
    inline_ctes(client, query, binder)?;

    // Classify the view's shape once (the load-bearing precedence the old guard
    // ladder encoded), then dispatch to the matching builder.
    match ViewShape::classify(query)? {
        ViewShape::SetOp {
            op,
            set_quantifier,
            left,
            right,
        } => set_op::execute_create_set_op_view(
            client,
            schema_name,
            &view_name,
            &sql_text,
            op,
            set_quantifier,
            left,
            right,
            binder,
        ),
        ViewShape::Distinct(select) => {
            set_op::execute_create_distinct_view(client, schema_name, &view_name, &sql_text, select, binder)
        }
        ViewShape::Join(select) => {
            join::execute_create_join_view(client, schema_name, &view_name, &sql_text, select, binder)
        }
        ViewShape::Subquery {
            select,
            subq,
            outer_not,
            local,
        } => exists::execute_create_exists_view(
            client,
            schema_name,
            &view_name,
            &sql_text,
            select,
            subq,
            outer_not,
            &local,
            binder,
        ),
        ViewShape::GroupBy(select) => {
            group_by::execute_create_group_by_view(client, schema_name, &view_name, &sql_text, select, binder)
        }
        ViewShape::Simple(select) => {
            simple::execute_create_simple_view(client, schema_name, &view_name, &sql_text, select, binder)
        }
    }
}

/// The classified shape of a CREATE VIEW body. Classifying once — in this fixed
/// order — preserves the precedence the old guard ladder encoded: a set
/// operation outranks everything; `DISTINCT` is checked before the single-FROM
/// requirement; a JOIN outranks GROUP BY. Everything else is a `Simple`
/// filter/map view.
enum ViewShape<'a> {
    SetOp {
        op: SetOperator,
        set_quantifier: SetQuantifier,
        left: &'a SetExpr,
        right: &'a SetExpr,
    },
    Distinct(&'a Select),
    Join(&'a Select),
    /// A Simple-shaped view whose WHERE carries exactly one top-level
    /// `[NOT] EXISTS (…)` / `x [NOT] IN (SELECT …)` conjunct — routed to the
    /// semi/anti-join builder.
    Subquery {
        select: &'a Select,
        /// The peeled `Exists`/`InSubquery` node (`Nested` and `NOT` wrappers
        /// removed).
        subq: &'a Expr,
        /// True when a net-negating stack of `NOT` wrappers surrounded the
        /// node; the builder folds it into the node's own `negated` flag.
        outer_not: bool,
        /// The remaining WHERE conjuncts, filtering the outer relation.
        local: Vec<&'a Expr>,
    },
    GroupBy(&'a Select),
    Simple(&'a Select),
}

/// Peel `Nested` wrappers and `NOT`s off a WHERE conjunct; when the core is a
/// subquery test (`EXISTS` / `IN (SELECT …)`), return it with the net-negation
/// flag — each peeled `NOT` toggles it, so `NOT (EXISTS …)` classifies like
/// `NOT EXISTS …` and a double negation cancels.
fn as_subquery_conjunct(e: &Expr) -> Option<(&Expr, bool)> {
    let mut cur = e;
    let mut outer_not = false;
    loop {
        match cur {
            Expr::Nested(inner) => cur = inner,
            Expr::UnaryOp {
                op: UnaryOperator::Not,
                expr,
            } => {
                outer_not = !outer_not;
                cur = expr;
            }
            _ => break,
        }
    }
    matches!(cur, Expr::Exists { .. } | Expr::InSubquery { .. }).then_some((cur, outer_not))
}

impl<'a> ViewShape<'a> {
    fn classify(query: &'a Query) -> Result<ViewShape<'a>, GnitzSqlError> {
        // Set operations (UNION/INTERSECT/EXCEPT) outrank the single-SELECT shapes.
        let select = match query.body.as_ref() {
            SetExpr::SetOperation {
                op,
                set_quantifier,
                left,
                right,
            } => {
                return Ok(ViewShape::SetOp {
                    op: *op,
                    set_quantifier: *set_quantifier,
                    left,
                    right,
                })
            }
            SetExpr::Select(s) => s,
            _ => {
                return Err(GnitzSqlError::Unsupported(
                    "CREATE VIEW only supports SELECT".to_string(),
                ))
            }
        };

        // DISTINCT is checked before the single-FROM requirement. Both plain DISTINCT and DISTINCT
        // ON route here; the shared side-compiler (compile_set_op_side) honors only FROM/WHERE/
        // projection and rejects every other clause (DISTINCT ON, GROUP BY, HAVING, PREWHERE, TOP,
        // …), none of which has incremental-view semantics here.
        if select.distinct.is_some() {
            return Ok(ViewShape::Distinct(select));
        }
        if select.from.len() != 1 {
            return Err(GnitzSqlError::Unsupported(
                "CREATE VIEW: only single FROM item supported".to_string(),
            ));
        }
        // A JOIN outranks GROUP BY.
        if !select.from[0].joins.is_empty() {
            return Ok(ViewShape::Join(select));
        }
        // A single top-level `[NOT] EXISTS` / `[NOT] IN (SELECT …)` WHERE
        // conjunct routes to the semi/anti-join builder. Checked after JOIN (a
        // join-view WHERE is rejected wholesale by that builder) and before
        // GROUP BY, which cannot host the subquery in one circuit (the exists
        // builder rejects that combination with a targeted message).
        if let Some(selection) = &select.selection {
            let mut conjuncts = Vec::new();
            flatten_conjuncts(selection, &mut conjuncts);
            let mut subq: Option<(&Expr, bool)> = None;
            let mut local: Vec<&Expr> = Vec::new();
            for &c in &conjuncts {
                match as_subquery_conjunct(c) {
                    Some(peeled) => {
                        if subq.is_some() {
                            return Err(GnitzSqlError::Unsupported(
                                "at most one EXISTS/IN subquery conjunct per view WHERE; \
                                 stack views instead"
                                    .into(),
                            ));
                        }
                        subq = Some(peeled);
                    }
                    None => local.push(c),
                }
            }
            if let Some((node, outer_not)) = subq {
                return Ok(ViewShape::Subquery {
                    select,
                    subq: node,
                    outer_not,
                    local,
                });
            }
        }
        // GROUP BY *or* a no-`GROUP BY` aggregate (`SELECT MIN(x) FROM t`) routes
        // to the grouped builder: the latter is one logical group compiled as a
        // reduce with an empty group-column set. Routing it here (rather than to
        // `Simple`, which would refuse the aggregate in scalar lowering with a
        // less specific error) reuses the entire grouped pipeline — the
        // synthetic-PK branch and the strict projection validator both already
        // handle the empty group set. A `MIN(x)+1` is detected too and rejected
        // by that validator, not by `Simple`'s lowering.
        if group_by_is_present(&select.group_by) || projection_has_aggregate(select) {
            return Ok(ViewShape::GroupBy(select));
        }
        Ok(ViewShape::Simple(select))
    }
}

/// Inline a query's CTEs (the `WITH` clause) into the binder's alias cache. Each
/// CTE must be a plain column pass-through of a single base table (no WHERE,
/// projection reshape, GROUP BY, HAVING, DISTINCT, ORDER BY, or LIMIT); anything
/// else is rejected.
fn inline_ctes(client: &mut GnitzClient, query: &Query, binder: &mut Binder<'_>) -> Result<(), GnitzSqlError> {
    if let Some(with) = &query.with {
        if with.recursive {
            return Err(GnitzSqlError::Unsupported("recursive CTEs not supported".to_string()));
        }
        for cte in &with.cte_tables {
            let cte_name = cte.alias.name.value.clone();
            let ctx = format!("CTE '{cte_name}'");
            // Exhaustively destructure the `Cte` envelope so a future field can't be silently
            // dropped. `alias`/`query`/`closing_paren_token` are consumed below; `materialized`
            // parses only under PostgreSqlDialect, so under the planner's GenericDialect it is
            // always None — `_`-bound (no dead runtime check), still drift-protected by the
            // exhaustive destructure.
            let sqlparser::ast::Cte {
                alias: _,
                query: _,
                from: cte_from,
                materialized: _,
                closing_paren_token: _,
            } = cte;
            if cte_from.is_some() {
                return Err(unsupported_clause(&ctx, "a trailing FROM"));
            }
            // Reject the `Query`-envelope clauses a CTE body cannot honor (a nested `WITH`,
            // LIMIT/OFFSET, FETCH, FOR UPDATE/SHARE, SETTINGS, …) at loop top — before the body
            // match and `binder.resolve`. A nested CTE (`WITH d …`) has body=Select, so only this
            // guard catches it; an after-resolve placement would fail with a misleading "relation
            // not found" for the never-registered inner table.
            reject_unhonored_query_clauses(
                &cte.query,
                HonoredQueryClauses {
                    with: false,
                    limit: false,
                },
                &ctx,
            )?;
            // Resolve the CTE's SELECT to find its source table and schema
            let cte_select = match cte.query.body.as_ref() {
                SetExpr::Select(s) => s,
                _ => return Err(GnitzSqlError::Unsupported(format!("{ctx}: only SELECT supported"))),
            };
            // Reject every Select-body clause the pass-through cannot honor (WHERE,
            // DISTINCT, GROUP BY, HAVING, and the exotic tail — PREWHERE, TOP, …)
            // before resolving the source table, so a dropped clause is a clean
            // error, not silently inlined as a pass-through.
            reject_unhonored_select_clauses(
                cte_select,
                HonoredClauses {
                    where_filter: false,
                    grouping: false,
                    distinct: false,
                },
                &ctx,
            )?;
            if cte_select.from.len() != 1 || !cte_select.from[0].joins.is_empty() {
                return Err(GnitzSqlError::Unsupported(format!(
                    "{ctx}: only single table without JOINs"
                )));
            }
            let cte_table_name = extract_table_factor_name(&cte_select.from[0].relation, &ctx)?;
            // Resolve the CTE's source table and cache the CTE name as an alias.
            let (cte_tid, cte_schema) = binder.resolve(client, &cte_table_name)?;
            // Beyond the Select-body clauses and the Query envelope the guards above
            // rejected, a CTE is inlined only as a plain column pass-through: a
            // non-identity projection would be silently discarded by the alias
            // mechanism and return wrong rows, so reject it here too. (Compiling an
            // arbitrary CTE body as a sub-plan is a separate, unimplemented feature.)
            // Positional identity: `*`, or one identifier per source column in
            // order. The qualified form (`SELECT t.a, t.b FROM t`) parses as
            // `CompoundIdentifier` and is the same positional pass-through — match
            // `parts[1]` against each source column. This stays a positional
            // identity test (never a name→index lookup), so a dup-named source
            // simply fails the per-position compare and is rejected.
            let proj_is_identity = is_wildcard_projection(&cte_select.projection)
                || (cte_select.projection.len() == cte_schema.columns.len()
                    && cte_select.projection.iter().enumerate().all(|(i, item)| {
                        let want = &cte_schema.columns[i].name;
                        match item {
                            SelectItem::UnnamedExpr(Expr::Identifier(id)) => id.value.eq_ignore_ascii_case(want),
                            SelectItem::UnnamedExpr(Expr::CompoundIdentifier(parts)) if parts.len() == 2 => {
                                parts[1].value.eq_ignore_ascii_case(want)
                            }
                            _ => false,
                        }
                    }));
            if !proj_is_identity {
                return Err(GnitzSqlError::Unsupported(format!(
                    "{ctx}: only a plain column pass-through of a single table is \
                     supported; a subset/reordered/computed projection inside a CTE is not yet \
                     supported"
                )));
            }
            // Apply CTE column aliases (`WITH cte(a, b) AS ...`): rename the
            // resolved columns so outer references to the aliases bind.
            let cte_schema = if !cte.alias.columns.is_empty() {
                if cte.alias.columns.len() != cte_schema.columns.len() {
                    return Err(GnitzSqlError::Plan(format!(
                        "CTE '{}' defines {} column aliases but query returns {} columns",
                        cte_name,
                        cte.alias.columns.len(),
                        cte_schema.columns.len()
                    )));
                }
                let mut s = (*cte_schema).clone();
                for (col, alias) in s.columns.iter_mut().zip(&cte.alias.columns) {
                    col.name = alias.name.value.clone();
                }
                std::rc::Rc::new(s)
            } else {
                cte_schema
            };
            binder.cache_alias(cte_name, (cte_tid, cte_schema));
        }
    }

    Ok(())
}
