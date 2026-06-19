//! CREATE VIEW front door: validate the query envelope, inline CTEs, classify
//! the view's shape, and dispatch to the matching builder. The single module in
//! `plan/view` that knows about all the others.

use crate::ast_util::{extract_name, extract_table_factor_name, is_wildcard_projection};
use crate::bind::Binder;
use crate::error::GnitzSqlError;
use crate::plan::view::{group_by, join, set_op, simple};
use crate::SqlResult;
use gnitz_core::GnitzClient;
use sqlparser::ast::{Expr, GroupByExpr, Query, Select, SelectItem, SetExpr, SetOperator, SetQuantifier, Statement};

pub(crate) fn execute_create_view(
    client: &mut GnitzClient,
    schema_name: &str,
    view_name_obj: &sqlparser::ast::ObjectName,
    query: &Query,
    stmt: &Statement,
    binder: &mut Binder<'_>,
) -> Result<SqlResult, GnitzSqlError> {
    let view_name = extract_name(view_name_obj, "CREATE VIEW")?;

    if query.order_by.is_some() {
        return Err(GnitzSqlError::Unsupported("ORDER BY not supported".to_string()));
    }
    // An incremental circuit has no meaningful LIMIT/OFFSET semantics; a silently
    // ignored LIMIT would stream the full table.
    if query.limit_clause.is_some() {
        return Err(GnitzSqlError::Unsupported(
            "LIMIT and OFFSET are not supported in VIEW definitions".into(),
        ));
    }

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
    GroupBy(&'a Select),
    Simple(&'a Select),
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

        // DISTINCT is checked before the single-FROM requirement.
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
        let has_group_by = match &select.group_by {
            GroupByExpr::Expressions(exprs, _) => !exprs.is_empty(),
            _ => false,
        };
        if has_group_by {
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
            // Resolve the CTE's SELECT to find its source table and schema
            let cte_select = match cte.query.body.as_ref() {
                SetExpr::Select(s) => s,
                _ => {
                    return Err(GnitzSqlError::Unsupported(format!(
                        "CTE '{cte_name}': only SELECT supported"
                    )))
                }
            };
            if cte_select.from.len() != 1 || !cte_select.from[0].joins.is_empty() {
                return Err(GnitzSqlError::Unsupported(format!(
                    "CTE '{cte_name}': only single table without JOINs"
                )));
            }
            let cte_table_name = extract_table_factor_name(&cte_select.from[0].relation, &format!("CTE '{cte_name}'"))?;
            // Resolve the CTE's source table and cache the CTE name as an alias.
            let (cte_tid, cte_schema) = binder.resolve(client, &cte_table_name)?;
            // A CTE is inlined only as a plain column pass-through of its source
            // table. Anything that changes the row set or shape (WHERE, GROUP BY,
            // HAVING, DISTINCT, ORDER BY, LIMIT, or a non-identity projection)
            // would be silently discarded by the alias mechanism and return wrong
            // rows, so reject it explicitly. (Compiling an arbitrary CTE body as a
            // sub-plan is a separate, unimplemented feature.)
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
            let has_group_by = matches!(&cte_select.group_by,
                GroupByExpr::Expressions(e, _) if !e.is_empty());
            if cte_select.selection.is_some()
                || cte_select.having.is_some()
                || cte_select.distinct.is_some()
                || has_group_by
                || cte.query.order_by.is_some()
                || cte.query.limit_clause.is_some()
                || !proj_is_identity
            {
                return Err(GnitzSqlError::Unsupported(format!(
                    "CTE '{cte_name}': only a plain column pass-through of a single table is \
                     supported; WHERE, a subset/reordered/computed projection, GROUP BY, \
                     HAVING, DISTINCT, ORDER BY and LIMIT inside a CTE are not yet supported"
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
