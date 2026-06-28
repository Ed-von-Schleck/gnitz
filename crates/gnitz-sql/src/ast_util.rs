use crate::error::GnitzSqlError;

/// Extract the last identifier name from an ObjectName.
pub(crate) fn extract_name(name: &sqlparser::ast::ObjectName, context: &str) -> Result<String, GnitzSqlError> {
    name.0
        .last()
        .and_then(|p| p.as_ident())
        .map(|i| i.value.clone())
        .ok_or_else(|| GnitzSqlError::Bind(format!("empty name in {context}")))
}

/// True when every projection item is `*` — the shared test behind the
/// "pass every source column through unchanged" wildcard fast paths.
pub(crate) fn is_wildcard_projection(projection: &[sqlparser::ast::SelectItem]) -> bool {
    projection
        .iter()
        .all(|p| matches!(p, sqlparser::ast::SelectItem::Wildcard(_)))
}

/// True when a SELECT carries a GROUP BY — either `GROUP BY ALL` or a non-empty
/// grouping-column list. sqlparser emits `Expressions([])` for a GROUP-BY-less
/// SELECT, which is *not* present. `GROUP BY ALL` counts as present so it is
/// classified/rejected, never silently dropped. The single definition behind
/// every "is this SELECT grouped?" test.
pub(crate) fn group_by_is_present(group_by: &sqlparser::ast::GroupByExpr) -> bool {
    use sqlparser::ast::GroupByExpr;
    match group_by {
        GroupByExpr::All(_) => true,
        GroupByExpr::Expressions(exprs, _) => !exprs.is_empty(),
    }
}

/// True for the aggregate function names the binder's `bind_function` accepts
/// (`count`, `sum`, `min`, `max`, `avg`), matched case-insensitively. Kept in
/// sync with that binder match by hand: an aggregate added there must be added
/// here too, or a no-`GROUP BY` use of it would misroute to the scalar `Simple`
/// builder and report the less-specific lowering error.
pub(crate) fn is_aggregate_func_name(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "count" | "sum" | "min" | "max" | "avg"
    )
}

/// True when any SELECT projection item contains an aggregate function call —
/// at the top level (`MIN(x)`) or nested inside an arithmetic/comparison
/// expression (`MIN(x) + 1`). Dispatch uses this to route a no-`GROUP BY`
/// aggregate to the grouped builder (which compiles the ungrouped global
/// aggregate, or rejects a computed-over-aggregate via its strict validator)
/// instead of to the scalar `Simple` builder. The recursion mirrors the binder's
/// `bind_structural` node set so the two agree on where an aggregate can hide.
pub(crate) fn projection_has_aggregate(select: &sqlparser::ast::Select) -> bool {
    use sqlparser::ast::SelectItem;
    select.projection.iter().any(|item| match item {
        SelectItem::UnnamedExpr(e) => expr_has_aggregate(e),
        SelectItem::ExprWithAlias { expr, .. } => expr_has_aggregate(expr),
        // `*` / `tbl.*` cannot be an aggregate.
        _ => false,
    })
}

/// Recursively test whether an expression contains an aggregate function call.
/// Recurses through exactly the node set `bind_structural` reaches (binary/unary
/// ops, nesting, BETWEEN, IS [NOT] NULL) plus an aggregate's own arguments.
fn expr_has_aggregate(e: &sqlparser::ast::Expr) -> bool {
    use sqlparser::ast::{Expr, FunctionArg, FunctionArgExpr, FunctionArguments};
    match e {
        Expr::Function(f) => {
            if is_aggregate_func_name(&f.name.to_string()) {
                return true;
            }
            // A non-aggregate wrapper over an aggregate (`abs(SUM(x))`) is still a
            // grouped shape; recurse into the call's arguments.
            match &f.args {
                FunctionArguments::List(list) => list.args.iter().any(|a| match a {
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(inner)) => expr_has_aggregate(inner),
                    _ => false,
                }),
                _ => false,
            }
        }
        Expr::BinaryOp { left, right, .. } => expr_has_aggregate(left) || expr_has_aggregate(right),
        Expr::UnaryOp { expr, .. } => expr_has_aggregate(expr),
        Expr::Nested(inner) => expr_has_aggregate(inner),
        Expr::Between { expr, low, high, .. } => {
            expr_has_aggregate(expr) || expr_has_aggregate(low) || expr_has_aggregate(high)
        }
        Expr::IsNull(inner) | Expr::IsNotNull(inner) => expr_has_aggregate(inner),
        _ => false,
    }
}

/// Extract table name from a TableFactor::Table.
pub(crate) fn extract_table_factor_name(
    tf: &sqlparser::ast::TableFactor,
    context: &str,
) -> Result<String, GnitzSqlError> {
    match tf {
        sqlparser::ast::TableFactor::Table { name, .. } => extract_name(name, context),
        _ => Err(GnitzSqlError::Unsupported(format!(
            "{context}: only simple table references supported"
        ))),
    }
}

/// Bare column name of a single-relation reference: a plain `Identifier`, or a
/// two-part `CompoundIdentifier` whose qualifier adds no disambiguation over a
/// single grouped/base relation. `None` for any other shape, so each caller
/// raises its own context-specific error.
pub(crate) fn single_relation_col_name(e: &sqlparser::ast::Expr) -> Option<&str> {
    use sqlparser::ast::Expr;
    match e {
        Expr::Identifier(id) => Some(&id.value),
        Expr::CompoundIdentifier(p) if p.len() == 2 => Some(&p[1].value),
        _ => None,
    }
}
