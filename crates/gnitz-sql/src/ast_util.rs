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

/// Recursively test whether an expression contains an aggregate function call:
/// the call itself, or — for a non-aggregate wrapper over one (`abs(SUM(x))`,
/// still a grouped shape) — any of its operands.
fn expr_has_aggregate(e: &sqlparser::ast::Expr) -> bool {
    if let sqlparser::ast::Expr::Function(f) = e {
        if is_aggregate_func_name(&f.name.to_string()) {
            return true;
        }
    }
    expr_operands(e).into_iter().any(expr_has_aggregate)
}

/// The direct operand subexpressions of `e` — the node set the structural
/// binder recurses through (binary/unary ops, parens, BETWEEN, IS [NOT] NULL,
/// IN lists) plus function-call arguments. Subquery nodes contribute no
/// operands: no walker may silently descend into a subquery. The single
/// definition behind the crate's expression walkers (`expr_has_aggregate`,
/// HAVING aggregate collection, EXISTS correlation side-counting), so a node
/// added to the binder's vocabulary reaches them all at once.
pub(crate) fn expr_operands(e: &sqlparser::ast::Expr) -> Vec<&sqlparser::ast::Expr> {
    use sqlparser::ast::{CaseWhen, Expr, FunctionArg, FunctionArgExpr, FunctionArguments};
    match e {
        Expr::BinaryOp { left, right, .. } => vec![left, right],
        Expr::UnaryOp { expr, .. } | Expr::Nested(expr) | Expr::IsNull(expr) | Expr::IsNotNull(expr) => {
            vec![expr]
        }
        Expr::Between { expr, low, high, .. } => vec![expr, low, high],
        Expr::InList { expr, list, .. } => std::iter::once(expr.as_ref()).chain(list).collect(),
        // CASE operands: the optional operand, every WHEN condition + result, and
        // the optional ELSE — the node set `bind_structural`'s Case arm recurses
        // through, so a subquery or column ref inside a branch stays visible to
        // `expr_has_aggregate` / `collect_column_refs` / the mark rewrite.
        Expr::Case {
            operand,
            conditions,
            else_result,
        } => {
            let mut ops: Vec<&Expr> = Vec::new();
            ops.extend(operand.as_deref());
            for CaseWhen { condition, result } in conditions {
                ops.push(condition);
                ops.push(result);
            }
            ops.extend(else_result.as_deref());
            ops
        }
        Expr::Function(f) => match &f.args {
            FunctionArguments::List(list) => list
                .args
                .iter()
                .filter_map(|a| match a {
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(inner)) => Some(inner),
                    _ => None,
                })
                .collect(),
            _ => Vec::new(),
        },
        _ => Vec::new(),
    }
}

/// Count the `[NOT] EXISTS` / `[NOT] IN (SELECT …)` subquery nodes anywhere in
/// `e`. Subqueries are opaque leaves to `expr_operands`, so the walk visits each
/// one (under OR/NOT, inside CASE, in a projection) without descending into its
/// body — the mark dispatcher's "exactly one subquery per view" test.
pub(crate) fn count_subqueries(e: &sqlparser::ast::Expr) -> usize {
    use sqlparser::ast::Expr;
    let here = usize::from(matches!(e, Expr::Exists { .. } | Expr::InSubquery { .. }));
    here + expr_operands(e).into_iter().map(count_subqueries).sum::<usize>()
}

/// The first `[NOT] EXISTS` / `[NOT] IN (SELECT …)` subquery node anywhere in
/// `e` (pre-order), walking the same node set as [`count_subqueries`]; the mark
/// builder extracts its single subquery with this.
pub(crate) fn find_subquery(e: &sqlparser::ast::Expr) -> Option<&sqlparser::ast::Expr> {
    use sqlparser::ast::Expr;
    if matches!(e, Expr::Exists { .. } | Expr::InSubquery { .. }) {
        return Some(e);
    }
    expr_operands(e).into_iter().find_map(find_subquery)
}

/// The scalar expression of a projection item, or `None` for a wildcard.
pub(crate) fn projection_item_expr(item: &sqlparser::ast::SelectItem) -> Option<&sqlparser::ast::Expr> {
    use sqlparser::ast::SelectItem;
    match item {
        SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => Some(e),
        SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(..) => None,
    }
}

/// Collect every column reference in `expr` as `(qualifier, bare_name)` — a
/// two-part `CompoundIdentifier` yields `(Some(q), c)`, a bare `Identifier` yields
/// `(None, c)`. Sub-expressions recurse through `expr_operands` (function
/// arguments included, so an aggregate's argument column is captured), the same
/// node set the structural binder walks — a node it omits is one the binder
/// rejects, so under-collection can only reproduce that bind error, never a wrong
/// result. Subquery and `*`/`tbl.*` nodes contribute no references (the caller
/// handles a wildcard). The join-chain liveness pre-pass and the hidden-`H`
/// projection collector share this one walker.
pub(crate) fn collect_column_refs<'e>(expr: &'e sqlparser::ast::Expr, out: &mut Vec<(Option<&'e str>, &'e str)>) {
    use sqlparser::ast::Expr;
    match expr {
        Expr::Identifier(id) => out.push((None, id.value.as_str())),
        Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
            out.push((Some(parts[0].value.as_str()), parts[1].value.as_str()));
        }
        _ => {
            for sub in expr_operands(expr) {
                collect_column_refs(sub, out);
            }
        }
    }
}

/// Collect the column references of every projection item into `out` via
/// `collect_column_refs`. Returns `false` when any item is a wildcard
/// (`*` / `tbl.*`) — the caller's everything-is-referenced case, in which `out`
/// is meaningless and ignored. The join-chain liveness pre-pass and the
/// hidden-`H` projection collector share this one item walk.
pub(crate) fn collect_projection_column_refs<'e>(
    items: &'e [sqlparser::ast::SelectItem],
    out: &mut Vec<(Option<&'e str>, &'e str)>,
) -> bool {
    for item in items {
        match projection_item_expr(item) {
            Some(e) => collect_column_refs(e, out),
            None => return false, // wildcard
        }
    }
    true
}

/// Flattens an `AND`-tree into its leaf conjuncts, left to right. Descends
/// through `AND` nesting and unwraps parenthesised `Nested` wrappers; any other
/// node (an equality, a range, an `OR`-group, …) is a leaf kept intact. Shared
/// by the DML access-path planner and the CREATE VIEW shape classifier.
pub(crate) fn flatten_conjuncts<'e>(expr: &'e sqlparser::ast::Expr, out: &mut Vec<&'e sqlparser::ast::Expr>) {
    use sqlparser::ast::{BinaryOperator, Expr};
    match expr {
        Expr::Nested(inner) => flatten_conjuncts(inner, out),
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => {
            flatten_conjuncts(left, out);
            flatten_conjuncts(right, out);
        }
        _ => out.push(expr),
    }
}

/// Extract table name from a TableFactor::Table. Strict — a derived table
/// (subquery in FROM) is rejected; only the CREATE VIEW planner paths that
/// pre-compile derived tables may accept one (`extract_relation_name`).
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

/// Extract the resolvable relation name of a CREATE VIEW FROM factor: a table's
/// name, or a derived table's alias. Only for the view-planner paths that run
/// *after* the front door pre-compiled every top-level derived table into a
/// hidden view registered under its alias — elsewhere (DML, set-op sides, CTE
/// bodies) a derived table is NOT pre-compiled and the alias would mis-resolve,
/// so those paths use the strict `extract_table_factor_name`. An unaliased
/// derived table cannot be referenced, so it is rejected.
pub(crate) fn extract_relation_name(tf: &sqlparser::ast::TableFactor, context: &str) -> Result<String, GnitzSqlError> {
    match tf {
        sqlparser::ast::TableFactor::Derived { alias: Some(a), .. } => Ok(a.name.value.clone()),
        sqlparser::ast::TableFactor::Derived { alias: None, .. } => Err(GnitzSqlError::Unsupported(format!(
            "{context}: a derived table (subquery in FROM) needs an alias"
        ))),
        _ => extract_table_factor_name(tf, context),
    }
}

/// Extract `(relation name, effective alias)` from a CREATE VIEW FROM factor —
/// the declared alias when present, else the name itself. Derived tables resolve
/// by alias (see `extract_relation_name` for when that is sound).
pub(crate) fn extract_table_name_and_alias(
    tf: &sqlparser::ast::TableFactor,
    context: &str,
) -> Result<(String, String), GnitzSqlError> {
    let name = extract_relation_name(tf, context)?;
    let alias = match tf {
        sqlparser::ast::TableFactor::Table { alias: Some(a), .. } => a.name.value.clone(),
        _ => name.clone(),
    };
    Ok((name, alias))
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
