use crate::error::{reject_if, GnitzSqlError};
use crate::ir::AggFunc;
use gnitz_core::ColumnDef;
use sqlparser::ast::{ExcludeSelectItem, RenameSelectItem, SelectItem, WildcardAdditionalOptions};

/// The identifier of an `ObjectName`'s last part, or `None` when that part is
/// not a plain identifier. The single ObjectName→ident unwrap behind
/// [`extract_name`], the INSERT column list, and SERIAL recognition.
pub(crate) fn object_name_ident(name: &sqlparser::ast::ObjectName) -> Option<&sqlparser::ast::Ident> {
    name.0.last().and_then(|p| p.as_ident())
}

/// Extract the last identifier name from an ObjectName.
pub(crate) fn extract_name(name: &sqlparser::ast::ObjectName, context: &str) -> Result<String, GnitzSqlError> {
    object_name_ident(name)
        .map(|i| i.value.clone())
        .ok_or_else(|| GnitzSqlError::Bind(format!("empty name in {context}")))
}

/// True when the projection is one unqualified wildcard that names no output
/// column of its own: a bare `*`, or `* EXCEPT/EXCLUDE(…)`, which only drop
/// columns. The output names are then the source's own, so a duplicate among
/// them belongs to the source (a join surfacing both sides' `val`) and is
/// carried through positionally rather than rejected.
///
/// `RENAME` does name its output and can collide with a column the same wildcard
/// passes through, so it is excluded here and gets the duplicate check.
pub(crate) fn is_name_preserving_wildcard_projection(projection: &[sqlparser::ast::SelectItem]) -> bool {
    single_wildcard(projection).is_some_and(|o| o.opt_rename.is_none())
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

/// Parse `e` as a non-negative integer literal, or error. The single accept
/// rule for the count-shaped clause positions — LIMIT, OFFSET, an ORDER BY
/// position — where anything else (an expression like `1+1`, a string, an
/// out-of-range number) must be a clean error: silently degrading a LIMIT
/// returns every row. `what` names the clause for the message.
pub(crate) fn expr_usize_literal(e: &sqlparser::ast::Expr, what: &str) -> Result<usize, GnitzSqlError> {
    if let sqlparser::ast::Expr::Value(vws) = e {
        if let sqlparser::ast::Value::Number(n, _) = &vws.value {
            return n.parse::<usize>().map_err(|_| {
                GnitzSqlError::Unsupported(format!("{what} must be a non-negative integer literal, got '{n}'"))
            });
        }
    }
    Err(GnitzSqlError::Unsupported(format!(
        "{what} must be an integer literal, not an expression"
    )))
}

/// The bare name of an unqualified single-part function call, or `None` for a
/// qualified (`schema.fn`) name.
pub(crate) fn single_fn_name(f: &sqlparser::ast::Function) -> Option<&str> {
    match f.name.0.as_slice() {
        [part] => part.as_ident().map(|i| i.value.as_str()),
        _ => None,
    }
}

/// The one SQL-name ↔ aggregate map, read in both directions by
/// [`agg_func_from_name`] and [`agg_func_name`]. `CountNonNull` is absent: it is
/// not a spelling a user writes, but the `COUNT(x)` argument shape the binder
/// picks after resolving `count` (see [`agg_func_name`]).
const AGG_NAMES: [(&str, AggFunc); 5] = [
    ("count", AggFunc::Count),
    ("sum", AggFunc::Sum),
    ("min", AggFunc::Min),
    ("max", AggFunc::Max),
    ("avg", AggFunc::Avg),
];

/// The `AggFunc` a function name denotes (`count`, `sum`, `min`, `max`, `avg`),
/// matched case-insensitively without allocating; `None` for any other name.
/// The single name→aggregate map: the binder's `bind_function` dispatches the
/// argument shape from it (COUNT(*) vs COUNT(x)), and the dispatch walkers use
/// it to detect an aggregate — an aggregate added here reaches them all at once.
fn agg_func_from_name(name: &str) -> Option<AggFunc> {
    AGG_NAMES
        .into_iter()
        .find_map(|(n, f)| name.eq_ignore_ascii_case(n).then_some(f))
}

/// The canonical lowercase SQL name of an aggregate — [`agg_func_from_name`]
/// inverted over the same table, so a name can never drift between the two
/// directions. `CountNonNull` is the `COUNT(x)` argument shape of `count` and
/// shares its name: both render `count`, which is what keeps an unaliased
/// `COUNT(*)` and `COUNT(x)` on the same default output name.
pub(crate) fn agg_func_name(f: AggFunc) -> &'static str {
    let spelled = if f == AggFunc::CountNonNull { AggFunc::Count } else { f };
    AGG_NAMES
        .iter()
        .find_map(|&(n, g)| (g == spelled).then_some(n))
        .expect("every AggFunc spelling is in AGG_NAMES")
}

/// Classify an aggregate function call into `(func, arg)` — the one
/// leaf-independent aggregate-call shape dispatch, composed from the name map and
/// qualifier check above. `COUNT(*)` → `(Count, None)`, `COUNT(x)` →
/// `(CountNonNull, Some(x))`, `SUM|MIN|MAX|AVG(x)` → `(that, Some(x))`. The
/// argument expression is returned *unbound* for the caller to resolve against
/// its own leaf (a schema index for the runtime `SingleTable` binder, a `ColId`
/// for the HIR binders), so arity and argument-shape validation — and their error
/// messages — have a single home every aggregate binder shares.
pub(crate) fn classify_agg_call(
    f: &sqlparser::ast::Function,
) -> Result<(AggFunc, Option<&sqlparser::ast::Expr>), GnitzSqlError> {
    use sqlparser::ast::{FunctionArg, FunctionArgExpr, FunctionArguments};
    reject_unsupported_fn_qualifiers(f, "aggregates")?;
    let base = single_fn_name(f)
        .and_then(agg_func_from_name)
        .ok_or_else(|| unknown_function(f))?;
    match base {
        AggFunc::Count => {
            if let FunctionArguments::List(list) = &f.args {
                if list.args.len() == 1 {
                    match &list.args[0] {
                        FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => return Ok((AggFunc::Count, None)),
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(inner)) => {
                            return Ok((AggFunc::CountNonNull, Some(inner)))
                        }
                        _ => {}
                    }
                }
            }
            Err(GnitzSqlError::Unsupported("COUNT: unsupported argument form".into()))
        }
        AggFunc::Sum | AggFunc::Min | AggFunc::Max | AggFunc::Avg => {
            if let FunctionArguments::List(list) = &f.args {
                if list.args.len() == 1 {
                    if let FunctionArg::Unnamed(FunctionArgExpr::Expr(inner)) = &list.args[0] {
                        return Ok((base, Some(inner)));
                    }
                }
            }
            Err(GnitzSqlError::Unsupported(format!(
                "{}: requires exactly one column argument",
                agg_func_name(base)
            )))
        }
        AggFunc::CountNonNull => unreachable!("agg_func_from_name never yields CountNonNull"),
    }
}

/// True when a SELECT body is grouped: it carries a GROUP BY or an aggregate in
/// its projection — the one disjunction behind every "route this body to the
/// grouped builder / reject the grouped shape" test (the view-shape classifier,
/// the hidden-body router, and the EXISTS guard).
pub(crate) fn body_is_grouped(select: &sqlparser::ast::Select) -> bool {
    group_by_is_present(&select.group_by) || projection_has_aggregate(select)
}

/// True when any SELECT projection item contains an aggregate function call —
/// at the top level (`MIN(x)`) or nested inside an arithmetic/comparison
/// expression (`MIN(x) + 1`). Dispatch uses this to route a no-`GROUP BY`
/// aggregate to the grouped builder (which compiles the ungrouped global
/// aggregate, or rejects a computed-over-aggregate via its strict validator)
/// instead of to the scalar `Simple` builder. The recursion mirrors the binder's
/// `bind_structural` node set so the two agree on where an aggregate can hide.
fn projection_has_aggregate(select: &sqlparser::ast::Select) -> bool {
    // `*` / `tbl.*` (the `None` items) cannot be an aggregate.
    select
        .projection
        .iter()
        .filter_map(projection_item_expr)
        .any(expr_has_aggregate)
}

/// Whether `e` or any node beneath it satisfies `p` — the one recursive
/// existence walk over the [`expr_operands`] node set, so a walker written
/// against it inherits that set rather than re-spelling the recursion.
fn expr_any(e: &sqlparser::ast::Expr, p: &impl Fn(&sqlparser::ast::Expr) -> bool) -> bool {
    p(e) || expr_operands(e).into_iter().any(|o| expr_any(o, p))
}

/// Recursively test whether an expression contains an aggregate function call:
/// the call itself, or — for a non-aggregate wrapper over one (`abs(SUM(x))`,
/// still a grouped shape) — any of its operands.
fn expr_has_aggregate(e: &sqlparser::ast::Expr) -> bool {
    expr_any(e, &|e| matches!(e, sqlparser::ast::Expr::Function(f) if is_agg_call(f)))
}

/// The rejection for a call whose name is not one this crate implements. Shared
/// so every binder that turns a function away spells it the same way.
pub(crate) fn unknown_function(f: &sqlparser::ast::Function) -> GnitzSqlError {
    GnitzSqlError::Unsupported(format!(
        "function '{}' not supported",
        f.name.to_string().to_ascii_lowercase()
    ))
}

/// Whether a function call names one of the aggregates.
pub(crate) fn is_agg_call(f: &sqlparser::ast::Function) -> bool {
    single_fn_name(f).and_then(agg_func_from_name).is_some()
}

/// Strip redundant parentheses.
pub(crate) fn peel_nested(e: &sqlparser::ast::Expr) -> &sqlparser::ast::Expr {
    let mut cur = e;
    while let sqlparser::ast::Expr::Nested(inner) = cur {
        cur = inner;
    }
    cur
}

/// Visit every aggregate call in `e`, outermost-first: when `e` is itself one, `f`
/// runs on it and the walk stops (an aggregate's arguments cannot contain another
/// aggregate); otherwise every operand is visited. Returns whether `e`'s own top
/// level was the aggregate — the callers use it to tell "this item *is* an
/// aggregate" from "it merely contains one".
///
/// The one traversal behind the grouped bind: what it reaches is exactly what the
/// reduce materializes, because the same walk collects the aggregates and binds
/// the expressions over them. An aggregate a second walk reached and this one
/// missed would bind against a reduce-output column that does not exist.
pub(crate) fn for_each_agg_call<E>(
    e: &sqlparser::ast::Expr,
    f: &mut impl FnMut(&sqlparser::ast::Function) -> Result<(), E>,
) -> Result<bool, E> {
    let peeled = peel_nested(e);
    if let sqlparser::ast::Expr::Function(func) = peeled {
        if is_agg_call(func) {
            f(func)?;
            return Ok(true);
        }
    }
    for op in expr_operands(peeled) {
        for_each_agg_call(op, f)?;
    }
    Ok(false)
}

/// The GROUP BY clause's expression list. Only the plain list form is supported —
/// `GROUPING SETS` / `CUBE` / `ROLLUP` / `ALL` produce multiple grouping keys per
/// row, which no reduce shape models.
pub(crate) fn group_by_exprs(select: &sqlparser::ast::Select) -> Result<&[sqlparser::ast::Expr], GnitzSqlError> {
    match &select.group_by {
        sqlparser::ast::GroupByExpr::Expressions(exprs, _) => Ok(exprs),
        _ => Err(GnitzSqlError::Unsupported(
            "GROUP BY: only expression list supported".to_string(),
        )),
    }
}

/// The 1-based position a clause item names, or `None` when it is not an integer
/// literal. ORDER BY and GROUP BY share this rule; LIMIT and OFFSET call
/// [`expr_usize_literal`] directly, since a literal is the only form legal there.
pub(crate) fn clause_position(e: &sqlparser::ast::Expr, what: &str) -> Result<Option<usize>, GnitzSqlError> {
    match e {
        sqlparser::ast::Expr::Value(v) if matches!(v.value, sqlparser::ast::Value::Number(..)) => {
            Ok(Some(expr_usize_literal(e, what)?))
        }
        _ => Ok(None),
    }
}

/// Reject a 1-based clause position outside `1..=len`. ORDER BY resolves a
/// position into the visible output columns and GROUP BY into the SELECT list,
/// but out of range reads the same either way, so it is worded once.
pub(crate) fn reject_position_out_of_range(pos: usize, len: usize, what: &str) -> Result<(), GnitzSqlError> {
    if pos == 0 || pos > len {
        return Err(GnitzSqlError::Unsupported(format!(
            "{what} position {pos} is out of range (1..={len})"
        )));
    }
    Ok(())
}

/// A GROUP BY item, peeled of parens, with a 1-based SELECT-list position resolved
/// to the item it names; everything else passes through. `GROUP BY 1` is the first
/// projected expression, as in every other dialect — reading it as the constant `1`
/// would silently group everything into one group. A position must name a scalar,
/// aggregate-free item.
pub(crate) fn group_by_target<'a>(
    ge: &'a sqlparser::ast::Expr,
    select: &'a sqlparser::ast::Select,
) -> Result<&'a sqlparser::ast::Expr, GnitzSqlError> {
    let ge = peel_nested(ge);
    let Some(pos) = clause_position(ge, "GROUP BY position")? else {
        return Ok(ge);
    };
    reject_position_out_of_range(pos, select.projection.len(), "GROUP BY")?;
    let target = projection_item_expr(&select.projection[pos - 1]).ok_or_else(|| {
        GnitzSqlError::Unsupported(format!(
            "GROUP BY position {pos} names a wildcard, which is not a group key"
        ))
    })?;
    if expr_has_aggregate(target) {
        return Err(GnitzSqlError::Unsupported(format!(
            "GROUP BY position {pos} names an aggregate, which cannot be a group key"
        )));
    }
    Ok(peel_nested(target))
}

/// A column written outside both the grouping and an aggregate. `clause` names
/// where it was written (`"GROUP BY SELECT"` / `"HAVING"`), which is the only
/// thing that varies: one grouped binder raises this, so a direct SELECT and the
/// equivalent view read the identical sentence.
pub(crate) fn reject_ungrouped_column(clause: &str, name: &str) -> GnitzSqlError {
    GnitzSqlError::Plan(format!(
        "{clause}: column '{name}' must appear in GROUP BY or an aggregate function"
    ))
}

/// An expression over the grouped relation that is neither a group key nor an
/// aggregate. The shape is named, not dumped: the parser's `Debug` is a wall of
/// spans, and the reader wrote the SQL.
pub(crate) fn reject_grouped_column_ref(clause: &str) -> GnitzSqlError {
    GnitzSqlError::Unsupported(format!("{clause}: expected a group key or an aggregate"))
}

/// An aggregate a finalize leaf could not match to one the reduce computes.
/// Defensive on both paths: an aggregate reaching a finalize leaf was collected
/// into the reduce first, so no SQL body resolves to it.
pub(crate) fn reject_unresolved_aggregate(clause: &str, func: AggFunc, arg: &str) -> GnitzSqlError {
    GnitzSqlError::Bind(format!("{clause}: aggregate {func:?}({arg}) could not be resolved"))
}

/// The direct operand subexpressions of `e` — the node set the structural
/// binder recurses through (binary/unary ops, parens, BETWEEN, IS [NOT] NULL,
/// IN lists, CEIL/FLOOR/CAST) plus function-call arguments. Subquery nodes
/// contribute no operands: no walker may silently descend into a subquery. The
/// single definition behind the crate's expression walkers
/// (`expr_has_aggregate`, HAVING aggregate collection, EXISTS correlation
/// side-counting, the `EXCLUDED` guard), so a node added to the binder's
/// vocabulary reaches them all at once — and one added to the binder but *not*
/// here stays invisible to every walker, which is why the two move together.
pub(crate) fn expr_operands(e: &sqlparser::ast::Expr) -> Vec<&sqlparser::ast::Expr> {
    use sqlparser::ast::{CaseWhen, Expr, FunctionArg, FunctionArgExpr, FunctionArguments};
    match e {
        Expr::BinaryOp { left, right, .. } => vec![left, right],
        Expr::UnaryOp { expr, .. } | Expr::Nested(expr) | Expr::IsNull(expr) | Expr::IsNotNull(expr) => {
            vec![expr]
        }
        Expr::Between { expr, low, high, .. } => vec![expr, low, high],
        Expr::IsDistinctFrom(a, b) | Expr::IsNotDistinctFrom(a, b) => vec![a, b],
        Expr::Position { expr, r#in } => vec![expr, r#in],
        // CEIL/FLOOR/CAST reach the binder as their own AST nodes rather than as
        // function calls, so their operand needs naming here explicitly.
        Expr::Ceil { expr, .. } | Expr::Floor { expr, .. } | Expr::Cast { expr, .. } => vec![expr],
        // SUBSTRING and TRIM are keyword-dispatched too. TRIM's `trim_what` is a
        // literal by the time the binder accepts it, but it is a bound operand
        // position and belongs in the walk regardless.
        Expr::Substring {
            expr,
            substring_from,
            substring_for,
            ..
        } => std::iter::once(expr.as_ref())
            .chain(substring_from.as_deref())
            .chain(substring_for.as_deref())
            .collect(),
        Expr::Trim { expr, trim_what, .. } => std::iter::once(expr.as_ref()).chain(trim_what.as_deref()).collect(),
        // Same for LIKE's pattern. Its `escape_char` is a `Value`, not an `Expr`,
        // so it contributes nothing.
        Expr::Like { expr, pattern, .. } | Expr::ILike { expr, pattern, .. } => vec![expr, pattern],
        Expr::InList { expr, list, .. } => std::iter::once(expr.as_ref()).chain(list).collect(),
        // CASE operands: the optional operand, every WHEN condition + result, and
        // the optional ELSE — the node set `bind_structural`'s Case arm recurses
        // through, so a subquery or column ref inside a branch stays visible to
        // `expr_has_aggregate`, `expr_contains_excluded`, and the mark rewrite.
        Expr::Case {
            operand,
            conditions,
            else_result,
            case_token: _,
            end_token: _,
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

/// The scalar expression of a projection item, or `None` for a wildcard.
pub(crate) fn projection_item_expr(item: &sqlparser::ast::SelectItem) -> Option<&sqlparser::ast::Expr> {
    use sqlparser::ast::SelectItem;
    match item {
        SelectItem::UnnamedExpr(e)
        | SelectItem::ExprWithAlias { expr: e, .. }
        | SelectItem::ExprWithAliases { expr: e, .. } => Some(e),
        SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(..) => None,
    }
}

/// A non-wildcard SELECT item as `(expr, alias)` — the alias-carrying sibling of
/// [`projection_item_expr`]. `ctx` names the clause the reject speaks for. A
/// multi-alias item (`expr AS (a, b)`) is not a scalar projection and rejects
/// here with the wildcards.
pub(crate) fn scalar_projection_item<'a>(
    item: &'a SelectItem,
    ctx: &str,
) -> Result<(&'a sqlparser::ast::Expr, Option<String>), GnitzSqlError> {
    match item {
        SelectItem::UnnamedExpr(expr) => Ok((expr, None)),
        SelectItem::ExprWithAlias { expr, alias } => Ok((expr, Some(alias.value.clone()))),
        _ => Err(GnitzSqlError::Unsupported(format!("{ctx}: unsupported SELECT item"))),
    }
}

/// A source column re-emitted as an output column: an alias only renames it.
pub(crate) fn aliased_def(src: &ColumnDef, alias: Option<String>) -> ColumnDef {
    let mut def = src.clone();
    if let Some(name) = alias {
        def.name = name;
    }
    def
}

/// The two expression surfaces subquery detection scans — a SELECT's WHERE and its
/// projection items (a wildcard contributes none). The one definition of "which
/// surfaces decide subquery detection", shared by the EXISTS/IN and scalar/ANY/ALL
/// detectors below.
fn select_exprs(select: &sqlparser::ast::Select) -> impl Iterator<Item = &sqlparser::ast::Expr> {
    select
        .selection
        .iter()
        .chain(select.projection.iter().filter_map(projection_item_expr))
}

/// Whether `select` carries a `[NOT] EXISTS` / `[NOT] IN (SELECT …)` subquery in
/// its WHERE or projection. Subqueries are opaque leaves to `expr_operands`, so the
/// walk visits each (under OR/NOT, inside CASE, in a projection) without descending
/// into its body.
pub(crate) fn has_exists_in_subquery(select: &sqlparser::ast::Select) -> bool {
    select_exprs(select).any(|e| expr_any(e, &is_exists_in))
}

fn is_exists_in(e: &sqlparser::ast::Expr) -> bool {
    use sqlparser::ast::Expr;
    matches!(e, Expr::Exists { .. } | Expr::InSubquery { .. })
}

/// Whether `select` carries a scalar `Expr::Subquery` or an `Expr::AnyOp` /
/// `Expr::AllOp` anywhere in its WHERE or projection. (EXISTS/IN are detected
/// separately by [`has_exists_in_subquery`].)
pub(crate) fn has_scalar_subquery(select: &sqlparser::ast::Select) -> bool {
    select_exprs(select).any(|e| expr_any(e, &is_scalar_subquery))
}

fn is_scalar_subquery(e: &sqlparser::ast::Expr) -> bool {
    use sqlparser::ast::Expr;
    matches!(e, Expr::Subquery(_) | Expr::AnyOp { .. } | Expr::AllOp { .. })
}

/// The classified shape of a FROM clause — the one definition of "a single plain
/// table/view FROM", shared by the direct-SELECT derivation gate, the CTE
/// pass-through predicate, and the subquery / set-op side resolvers.
pub(crate) enum FromShape {
    /// No FROM item at all.
    Empty,
    /// Multiple comma-separated FROM items (an implicit comma join). A view body
    /// serves it as an INNER join keyed from the WHERE; distinct from `Join` only
    /// so a rejection can name the shape the user actually wrote.
    CommaJoin,
    /// One FROM item carrying an explicit JOIN chain.
    Join,
    /// One FROM item that is not a plain relation name (a derived table, a table
    /// function, …).
    DerivedTable,
    /// Exactly one plain relation name, no joins.
    SinglePlainRelation,
}

pub(crate) fn classify_from(from: &[sqlparser::ast::TableWithJoins]) -> FromShape {
    match from {
        [] => FromShape::Empty,
        [single] => {
            if !single.joins.is_empty() {
                FromShape::Join
            } else if matches!(single.relation, sqlparser::ast::TableFactor::Table { .. }) {
                FromShape::SinglePlainRelation
            } else {
                FromShape::DerivedTable
            }
        }
        _ => FromShape::CommaJoin,
    }
}

/// Flattens an `AND`-tree into its leaf conjuncts, left to right. Descends
/// through `AND` nesting and unwraps parenthesised `Nested` wrappers; any other
/// node (an equality, a range, an `OR`-group, …) is a leaf kept intact. The
/// AST form; `access::flatten_bound_conjuncts` is the `BoundExpr` analogue.
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
/// (subquery in FROM) is rejected.
///
/// The one acceptance point for a base-relation FROM factor, so every semantic
/// qualifier gnitz does not implement is rejected here (exhaustive destructure,
/// no `..`: a future `sqlparser` field stops the build until classified) —
/// otherwise `AS OF`, TABLESAMPLE, PARTITION, WITH ORDINALITY, or table-function
/// arguments would be silently dropped and the plain table scanned instead.
/// Advisory-only qualifiers (`with_hints`, `index_hints` — MySQL/T-SQL index and
/// locking hints that cannot change the result set) are accepted as no-ops.
pub(crate) fn extract_table_factor_name(
    tf: &sqlparser::ast::TableFactor,
    context: &str,
) -> Result<String, GnitzSqlError> {
    let sqlparser::ast::TableFactor::Table {
        name,
        alias: _,       // consumed by the callers that honor aliases
        with_hints: _,  // advisory locking hints (T-SQL WITH (NOLOCK)): no result impact
        index_hints: _, // advisory index hints (MySQL USE/FORCE INDEX): no result impact
        args,
        version,
        with_ordinality,
        partitions,
        json_path,
        sample,
    } = tf
    else {
        return Err(GnitzSqlError::Unsupported(format!(
            "{context}: only simple table references supported"
        )));
    };
    reject_if(args.is_some(), context, "a table function in FROM")?;
    reject_if(version.is_some(), context, "time travel (AS OF / AT)")?;
    reject_if(*with_ordinality, context, "WITH ORDINALITY")?;
    reject_if(!partitions.is_empty(), context, "PARTITION selection")?;
    reject_if(json_path.is_some(), context, "a JSON path on a table")?;
    reject_if(sample.is_some(), context, "TABLESAMPLE")?;
    extract_name(name, context)
}

/// Extract `(relation name, effective alias)` from a plain-table FROM factor —
/// the declared alias when present, else the name itself. Every caller has
/// already routed a derived table elsewhere (the HIR binds it as an inline
/// subtree), so anything but a table rejects here.
pub(crate) fn extract_table_name_and_alias(
    tf: &sqlparser::ast::TableFactor,
    context: &str,
) -> Result<(String, String), GnitzSqlError> {
    let name = extract_table_factor_name(tf, context)?;
    let alias = match tf {
        sqlparser::ast::TableFactor::Table { alias: Some(a), .. } => a.name.value.clone(),
        _ => name.clone(),
    };
    Ok((name, alias))
}

/// Reject any qualifier on a function call the binder does not implement.
/// Both aggregate-binding leaves (`SingleTable::bind_function` and the HIR
/// `GroupedLeaf`'s) and the COALESCE/NULLIF desugar read only the argument
/// list; every other `Function` field would otherwise be silently dropped,
/// computing the plain call. `on` names the rejecting context ("aggregates",
/// "COALESCE", …) in the message.
///
/// Exhaustive destructure (no `..`): a future `sqlparser` `Function` field stops
/// the build until it is classified consumed / inert / rejected.
pub(crate) fn reject_unsupported_fn_qualifiers(func: &sqlparser::ast::Function, on: &str) -> Result<(), GnitzSqlError> {
    use sqlparser::ast::{DuplicateTreatment, FunctionArguments};
    let sqlparser::ast::Function {
        // Consumed: the name dispatches the call, the argument list is bound.
        name: _,
        args,
        // Inert: ODBC's `{fn NAME(args)}` spells `NAME(args)`, round-trips
        // through `Display`, and changes no result.
        uses_odbc_syntax: _,
        // Rejected below.
        parameters,
        filter,
        null_treatment,
        over,
        within_group,
    } = func;
    // Colon form ("{what}: not supported …") sidesteps subject-verb number
    // agreement and matches the binder's existing message style
    // (e.g. "{name}: not supported on {:?} columns").
    let unsupported = |what: &str| Err(GnitzSqlError::Unsupported(format!("{what}: not supported on {on}")));
    if filter.is_some() {
        return unsupported("FILTER (WHERE …)");
    }
    if over.is_some() {
        return unsupported("window functions (OVER)");
    }
    if !within_group.is_empty() {
        return unsupported("WITHIN GROUP (ORDER BY …)");
    }
    if null_treatment.is_some() {
        return unsupported("IGNORE/RESPECT NULLS");
    }
    if !matches!(parameters, FunctionArguments::None) {
        return unsupported("parametric (ClickHouse) calls");
    }
    if let FunctionArguments::List(list) = args {
        if matches!(list.duplicate_treatment, Some(DuplicateTreatment::Distinct)) {
            return unsupported("DISTINCT");
        }
        if !list.clauses.is_empty() {
            return unsupported("in-argument clauses (ORDER BY / LIMIT / SEPARATOR)");
        }
    }
    Ok(())
}

/// Plain positional argument exprs of a function call, or a clean `Unsupported`
/// for `*`, named args, DISTINCT, or any qualifier (FILTER/OVER/…) — the shared
/// qualifier inventory (`reject_unsupported_fn_qualifiers`). Backs the scalar
/// function binder, which needs bare operand exprs.
pub(crate) fn function_positional_args<'f>(
    f: &'f sqlparser::ast::Function,
    name: &str,
) -> Result<Vec<&'f sqlparser::ast::Expr>, GnitzSqlError> {
    use sqlparser::ast::{FunctionArg, FunctionArgExpr, FunctionArguments};
    reject_unsupported_fn_qualifiers(f, name)?;
    let FunctionArguments::List(list) = &f.args else {
        return Err(GnitzSqlError::Unsupported(format!(
            "{name}: requires a parenthesized argument list"
        )));
    };
    list.args
        .iter()
        .map(|arg| match arg {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => Ok(e),
            _ => Err(GnitzSqlError::Unsupported(format!(
                "{name}: expects plain positional arguments (no `*`, named args)"
            ))),
        })
        .collect()
}

/// A strictly simple identifier expression's name, or the shared
/// "column must be a simple identifier" Bind error. Backs [`index_column_ident`]
/// and the CLUSTER BY column list (a `Vec<Expr>` in sqlparser).
pub(crate) fn simple_ident_expr<'a>(e: &'a sqlparser::ast::Expr, context: &str) -> Result<&'a str, GnitzSqlError> {
    match e {
        sqlparser::ast::Expr::Identifier(id) => Ok(&id.value),
        _ => Err(GnitzSqlError::Bind(format!(
            "{context}: column must be a simple identifier"
        ))),
    }
}

/// The bare column name of an `IndexColumn` (a PRIMARY KEY / UNIQUE constraint
/// column or a CREATE INDEX column), which gnitz requires to be a simple
/// identifier. Any expression, operator class, or ordering qualifier is rejected
/// — gnitz indexes are ordered ascending by key bytes and carry no per-column
/// ordering. Shared by the CREATE INDEX loop and the table-level PK/UNIQUE loops,
/// all of which see `Vec<IndexColumn>` since sqlparser 0.60.
pub(crate) fn index_column_ident<'a>(
    c: &'a sqlparser::ast::IndexColumn,
    context: &str,
) -> Result<&'a str, GnitzSqlError> {
    reject_if(
        c.operator_class.is_some(),
        context,
        "an operator class on an index column",
    )?;
    reject_if(
        c.column.options.asc.is_some() || c.column.options.nulls_first.is_some() || c.column.with_fill.is_some(),
        context,
        "per-column ordering (ASC/DESC, NULLS FIRST/LAST, WITH FILL)",
    )?;
    simple_ident_expr(&c.column.expr, context)
}

/// Bare column name of a single-relation reference: a plain `Identifier`, or a
/// two-part `CompoundIdentifier` whose qualifier adds no disambiguation over a
/// single grouped/base relation. Redundant parentheses are peeled — `(a)` is `a`
/// at every surface. `None` for any other shape, so each caller raises its own
/// context-specific error.
pub(crate) fn single_relation_col_name(e: &sqlparser::ast::Expr) -> Option<&str> {
    use sqlparser::ast::Expr;
    match peel_nested(e) {
        Expr::Identifier(id) => Some(&id.value),
        Expr::CompoundIdentifier(p) if p.len() == 2 => Some(&p[1].value),
        _ => None,
    }
}

/// The wildcard modifier options on a `*` / `tbl.*` item, else `None` — the one
/// place that unifies the two `SelectItem` wildcard variants so a caller never
/// has to match both.
fn wildcard_options(item: &SelectItem) -> Option<&WildcardAdditionalOptions> {
    match item {
        SelectItem::Wildcard(o) | SelectItem::QualifiedWildcard(_, o) => Some(o),
        _ => None,
    }
}

/// True when a wildcard carries any modifier (`EXCEPT`/`EXCLUDE`/`REPLACE`/
/// `RENAME`/`ILIKE`) — i.e. it is not a plain `*`. Backs
/// [`is_bare_wildcard_projection`], which must keep an options-bearing wildcard
/// out of the no-op identity fast paths.
fn wildcard_options_present(o: &WildcardAdditionalOptions) -> bool {
    o.opt_ilike.is_some()
        || o.opt_exclude.is_some()
        || o.opt_except.is_some()
        || o.opt_replace.is_some()
        || o.opt_rename.is_some()
}

/// A validated schema-space rewrite for one wildcard's `EXCEPT`/`EXCLUDE` +
/// `RENAME`. Construction ([`WildcardRewrite::for_item`]):
///   (a) rejects `REPLACE` and `ILIKE` (`Unsupported` — gnitz honors neither a
///       value substitution nor a name-pattern filter);
///   (b) unions `EXCEPT` ∪ `EXCLUDE` names into a drop-set;
///   (c) collects `RENAME` as (from → to) pairs;
///   (d) validates every drop name and every rename source names a *visible*
///       column (via the caller's `is_visible`, so a typo errors instead of
///       silently dropping/renaming nothing), that no column is both dropped
///       and renamed, and that no `RENAME` source is named twice.
/// Then per expanded column: [`excludes`](Self::excludes) (drop-all-matching)
/// and [`output_name`](Self::output_name) (renamed target, else the original).
/// Matching is case-insensitive (`eq_ignore_ascii_case`), like
/// `find_unique_column`. A plain `*` yields an empty rewrite (excludes → false,
/// output_name → None) and `is_visible` is never called.
pub(crate) struct WildcardRewrite<'a> {
    drop: Vec<&'a str>,
    rename: Vec<(&'a str, &'a str)>, // (from, to)
}

impl<'a> WildcardRewrite<'a> {
    pub(crate) fn for_item(
        item: &'a SelectItem,
        mut is_visible: impl FnMut(&str) -> bool,
        context: &str,
    ) -> Result<Self, GnitzSqlError> {
        let Some(o) = wildcard_options(item) else {
            return Ok(Self::empty());
        };
        // gnitz honors neither a computed value substitution (REPLACE) nor a
        // name-pattern filter (ILIKE); reject rather than silently expand `*`.
        reject_if(o.opt_replace.is_some(), context, "SELECT * REPLACE")?;
        reject_if(o.opt_ilike.is_some(), context, "SELECT * ILIKE")?;
        // EXCEPT and EXCLUDE are synonyms (ClickHouse/BigQuery vs Snowflake) —
        // union both into one drop-set.
        let mut drop = Vec::new();
        if let Some(e) = &o.opt_except {
            drop.push(e.first_element.value.as_str());
            drop.extend(e.additional_elements.iter().map(|i| i.value.as_str()));
        }
        match &o.opt_exclude {
            // `EXCLUDE` columns parse as `ObjectName`s (sqlparser 0.62); take each
            // one's bare identifier — a non-identifier part cannot name a column,
            // so it drops out of the exclude set.
            Some(ExcludeSelectItem::Single(i)) => drop.extend(object_name_ident(i).map(|id| id.value.as_str())),
            Some(ExcludeSelectItem::Multiple(v)) => drop.extend(
                v.iter()
                    .filter_map(|i| object_name_ident(i).map(|id| id.value.as_str())),
            ),
            None => {}
        }
        let mut rename = Vec::new();
        match &o.opt_rename {
            Some(RenameSelectItem::Single(a)) => rename.push((a.ident.value.as_str(), a.alias.value.as_str())),
            Some(RenameSelectItem::Multiple(v)) => {
                rename.extend(v.iter().map(|a| (a.ident.value.as_str(), a.alias.value.as_str())))
            }
            None => {}
        }
        let me = Self { drop, rename };
        // A drop/rename source that names no visible column is a typo: error
        // rather than silently dropping/renaming nothing.
        for &n in me.drop.iter().chain(me.rename.iter().map(|(f, _)| f)) {
            if !is_visible(n) {
                return Err(GnitzSqlError::Bind(format!(
                    "{context}: SELECT * EXCEPT/EXCLUDE/RENAME names unknown column '{n}'"
                )));
            }
        }
        // A column that is both excluded and renamed is a contradiction.
        if let Some((f, _)) = me.rename.iter().find(|(f, _)| me.excludes(f)) {
            return Err(GnitzSqlError::Bind(format!(
                "{context}: SELECT * RENAME names excluded column '{f}'"
            )));
        }
        // Contradictory rename sources (`RENAME (id AS x, id AS y)`) — `output_name`
        // would silently keep only the first. Reject rather than drop `y`.
        for (i, (f, _)) in me.rename.iter().enumerate() {
            if me.rename[i + 1..].iter().any(|(g, _)| g.eq_ignore_ascii_case(f)) {
                return Err(GnitzSqlError::Bind(format!(
                    "{context}: SELECT * RENAME names column '{f}' twice"
                )));
            }
        }
        Ok(me)
    }

    /// The no-op rewrite: what [`WildcardRewrite::for_item`] returns for a plain
    /// `*` carrying no modifier, so every expansion loop runs the same
    /// drop-then-rename transform whether or not the item had one.
    fn empty() -> Self {
        Self {
            drop: Vec::new(),
            rename: Vec::new(),
        }
    }

    /// Whether an expanded column named `name` is dropped (`EXCEPT`/`EXCLUDE`).
    fn excludes(&self, name: &str) -> bool {
        self.drop.iter().any(|d| d.eq_ignore_ascii_case(name))
    }

    /// The renamed output name for an expanded column named `name`, or `None`
    /// when it keeps its original name.
    fn output_name(&self, name: &str) -> Option<&'a str> {
        self.rename
            .iter()
            .find(|(f, _)| f.eq_ignore_ascii_case(name))
            .map(|&(_, t)| t)
    }

    /// Apply the rewrite to one expanded column: `None` if it is dropped
    /// (`EXCEPT`/`EXCLUDE`), else the column def with its `RENAME` target name
    /// applied. The single home for the drop-then-rename per-column transform
    /// every `ColumnDef`-producing wildcard-expansion loop shares.
    pub(crate) fn rewrite_column(&self, col: &ColumnDef) -> Option<ColumnDef> {
        if self.excludes(&col.name) {
            return None;
        }
        let mut out = col.clone();
        if let Some(new) = self.output_name(&out.name) {
            out.name = new.to_string();
        }
        Some(out)
    }
}

/// Expand one `*` item over `cols` into `(source index, output def)` pairs, in
/// source order. Hidden columns are skipped — a synthetic view key (`_join_pk`,
/// `_set_pk`, …) must never re-enter a payload or a row identity through a
/// wildcard — and `EXCEPT`/`EXCLUDE`/`RENAME` are applied per column while
/// `REPLACE`/`ILIKE` are rejected. The one wildcard expansion every projection
/// resolver shares; `ctx` names the surface for the messages.
pub(crate) fn expand_wildcard_item<'a, I>(
    item: &SelectItem,
    cols: I,
    ctx: &str,
) -> Result<Vec<(usize, ColumnDef)>, GnitzSqlError>
where
    I: IntoIterator<Item = &'a ColumnDef> + Clone,
{
    let rw = WildcardRewrite::for_item(item, |n| wildcard_name_is_visible(cols.clone(), n), ctx)?;
    Ok(cols
        .into_iter()
        .enumerate()
        .filter(|(_, c)| !c.is_hidden)
        .filter_map(|(i, c)| rw.rewrite_column(c).map(|out| (i, out)))
        .collect())
}

/// Whether `name` matches a *visible* (non-hidden) column of `cols`,
/// case-insensitively — the visibility test the wildcard-modifier call sites
/// hand to [`WildcardRewrite::for_item`]. Unlike [`find_unique_column`], a name
/// shared by two visible columns is not an error here: a `SELECT * EXCEPT (id)`
/// over a two-`id` join deliberately drops both.
///
/// [`find_unique_column`]: crate::bind::find_unique_column
pub(crate) fn wildcard_name_is_visible<'a>(cols: impl IntoIterator<Item = &'a ColumnDef>, name: &str) -> bool {
    cols.into_iter()
        .any(|c| !c.is_hidden && c.name.eq_ignore_ascii_case(name))
}

/// The options of the projection's single unqualified wildcard, else `None` —
/// the structural half both wildcard-projection predicates share. Requiring
/// *exactly one* item is what keeps `SELECT *, *` out of every wildcard rule: it
/// names each source column twice, which the named form (`SELECT id, a, a`) is
/// rejected for, so it must reach the duplicate-name gate rather than the
/// identity fast paths.
fn single_wildcard(projection: &[SelectItem]) -> Option<&WildcardAdditionalOptions> {
    match projection {
        [SelectItem::Wildcard(o)] => Some(o),
        _ => None,
    }
}

/// A plain `*` projection (one `Wildcard` item with NO modifiers) — a true
/// identity expansion. A wildcard carrying any option is excluded, so the
/// identity fast paths guarded by this fall through to real expansion where the
/// options are honored (`EXCEPT`/`EXCLUDE`/`RENAME`) or rejected
/// (`REPLACE`/`ILIKE`).
pub(crate) fn is_bare_wildcard_projection(projection: &[SelectItem]) -> bool {
    single_wildcard(projection).is_some_and(|o| !wildcard_options_present(o))
}

#[cfg(test)]
#[path = "tests/ast_util.rs"]
mod tests;
