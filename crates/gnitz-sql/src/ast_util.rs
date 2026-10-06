//! The planner's shared sqlparser-AST layer. An item lives here because two or
//! more surfaces must agree on it, so its doc states the rule it enforces —
//! never who calls it, which rots the moment a caller moves.

use crate::agg::{agg_func_from_name, agg_func_name, AggFunc};
use crate::bind::NameRef;
use crate::error::{reject_if, unsupported_clause, GnitzSqlError};
use crate::ir::{BExpr, NumLit};
use crate::rules::{canonical_user_name, first_duplicate};
use gnitz_core::RelName;
use gnitz_wire::decimal::decimal_of_number_text;
use gnitz_wire::ColumnDef;
use sqlparser::ast::{ExcludeSelectItem, RenameSelectItem, SelectItem, Value, WildcardAdditionalOptions};

/// The identifier of an `ObjectName`'s last part, or `None` when that part is
/// not a plain identifier.
pub(crate) fn object_name_ident(name: &sqlparser::ast::ObjectName) -> Option<&sqlparser::ast::Ident> {
    name.0.last().and_then(|p| p.as_ident())
}

/// The name an `ObjectName` of exactly one plain-identifier part spells; `None`
/// for a qualified name, so `t.a` is refused rather than truncated to `a`.
pub(crate) fn single_part_ident(name: &sqlparser::ast::ObjectName) -> Option<&str> {
    match &name.0[..] {
        [part] => part.as_ident().map(|i| i.value.as_str()),
        _ => None,
    }
}

/// An `ObjectName`'s parts as plain identifiers. `Err` when the name is empty or
/// any part is not a plain identifier — the shape every extractor below rejects
/// identically, before it classifies what a qualifier would have meant.
fn object_name_parts<'a>(name: &'a sqlparser::ast::ObjectName, context: &str) -> Result<Vec<&'a str>, GnitzSqlError> {
    let parts: Vec<&str> = name
        .0
        .iter()
        .filter_map(|p| p.as_ident())
        .map(|i| i.value.as_str())
        .collect();
    if parts.is_empty() || parts.len() != name.0.len() {
        return Err(GnitzSqlError::Rejected(format!("empty name in {context}")));
    }
    Ok(parts)
}

/// A table or view: `name` in `session_schema`, or `schema.name`.
pub(crate) fn extract_object_name(
    name: &sqlparser::ast::ObjectName,
    session_schema: &str,
    context: &str,
) -> Result<NameRef, GnitzSqlError> {
    let (schema, relation) = match object_name_parts(name, context)?.as_slice() {
        [n] => (None, *n),
        [s, n] => (Some(*s), *n),
        // `object_name_parts` rejects the empty name, so this is 3+ parts.
        _ => {
            return Err(GnitzSqlError::Rejected(format!(
                "{context}: '{name}' has too many name parts"
            )))
        }
    };
    Ok(NameRef {
        rel: RelName::new(schema.unwrap_or(session_schema), relation).map_err(GnitzSqlError::Rejected)?,
        qualified: schema.is_some(),
    })
}

/// An index name, canonical. Index names are global, so it takes no qualifier.
pub(crate) fn extract_index_name(name: &sqlparser::ast::ObjectName, context: &str) -> Result<String, GnitzSqlError> {
    match object_name_parts(name, context)?.as_slice() {
        [n] => canonical_user_name(n),
        _ => Err(GnitzSqlError::Rejected(format!(
            "{context}: an index name takes no qualifier (index names are global, not schema-scoped)"
        ))),
    }
}

/// A column reference the parser handed over as an `ObjectName`: a qualifier here
/// is `table.col`, so the column is the last part.
pub(crate) fn extract_ident_name(name: &sqlparser::ast::ObjectName, context: &str) -> Result<String, GnitzSqlError> {
    object_name_ident(name)
        .map(|i| i.value.clone())
        .ok_or_else(|| GnitzSqlError::Rejected(format!("empty name in {context}")))
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

/// SQL literal → `BExpr`. Leaf-free (no `ColRef` produced), so it is generic
/// over `R` without a `Clone` bound. An integer past `i64` binds to `LitWide`
/// up to `u128::MAX`; a fraction, an exponent or a longer integer to `LitFloat`.
pub(crate) fn bind_literal<R>(v: &Value) -> Result<BExpr<R>, GnitzSqlError> {
    match v {
        Value::Null => Ok(BExpr::LitNull),
        Value::Number(n, _) => {
            if let Ok(i) = n.parse::<i64>() {
                return Ok(BExpr::LitInt(i));
            }
            if n.bytes().all(|b| b.is_ascii_digit()) {
                if let Ok(mag) = n.parse::<u128>() {
                    return Ok(BExpr::LitWide(NumLit { mag, neg: false }));
                }
            }
            n.parse::<f64>()
                .map(|v| BExpr::LitFloat { v, dec: decimal_of_number_text(n) })
                .map_err(|_| GnitzSqlError::Rejected(format!("invalid number literal: {n}")))
        }
        Value::SingleQuotedString(s) => Ok(BExpr::LitStr(s.clone())),
        _ => Err(GnitzSqlError::Rejected(format!(
            "value type not supported in expressions: {v:?}"
        ))),
    }
}

/// The bare name of an unqualified single-part function call, or `None` for a
/// qualified (`schema.fn`) name.
pub(crate) fn single_fn_name(f: &sqlparser::ast::Function) -> Option<&str> {
    match f.name.0.as_slice() {
        [part] => part.as_ident().map(|i| i.value.as_str()),
        _ => None,
    }
}

/// An aggregate call's argument. `COUNT(*)` is the only argument-less shape and
/// `COUNT(DISTINCT *)` is not a shape at all, so a `Distinct` always carries one.
#[derive(Clone, Copy, PartialEq)]
pub(crate) enum AggArg<T> {
    Star,
    All(T),
    Distinct(T),
}

impl<T> AggArg<T> {
    /// The argument of a call whose DISTINCT the caller honours, or has refused.
    pub(crate) fn ignoring_distinct(self) -> Option<T> {
        match self {
            AggArg::Star => None,
            AggArg::All(a) | AggArg::Distinct(a) => Some(a),
        }
    }

    /// The argument of a call on a surface that does not implement DISTINCT and
    /// would otherwise compute the plain aggregate. `on` names that surface.
    pub(crate) fn reject_distinct(self, on: &str) -> Result<Option<T>, GnitzSqlError> {
        match self {
            AggArg::Distinct(_) => Err(unsupported_clause(on, "DISTINCT")),
            other => Ok(other.ignoring_distinct()),
        }
    }

    /// Resolve the argument through `f`, keeping the qualifier.
    pub(crate) fn try_map<U, E>(self, f: impl FnOnce(T) -> Result<U, E>) -> Result<AggArg<U>, E> {
        Ok(match self {
            AggArg::Star => AggArg::Star,
            AggArg::All(a) => AggArg::All(f(a)?),
            AggArg::Distinct(a) => AggArg::Distinct(f(a)?),
        })
    }

    /// Drop a DISTINCT the aggregate is indifferent to.
    pub(crate) fn without_distinct(self) -> Self {
        match self {
            AggArg::Distinct(a) => AggArg::All(a),
            other => other,
        }
    }
}

/// Classify an aggregate call into its function and argument. The argument comes
/// back unbound, for the caller to resolve against its own leaf.
pub(crate) fn classify_agg_call(
    f: &sqlparser::ast::Function,
) -> Result<(AggFunc, AggArg<&sqlparser::ast::Expr>), GnitzSqlError> {
    let call = PlainCall::check(f, CallSurface::Aggregate)?;
    let (func, arg) = classify_agg_shape(call)?;
    let arg = match (call.is_distinct(), arg) {
        (false, None) => AggArg::Star,
        (false, Some(e)) => AggArg::All(e),
        (true, Some(e)) => AggArg::Distinct(e),
        (true, None) => {
            return Err(GnitzSqlError::Rejected(
                "COUNT(DISTINCT *): DISTINCT needs a column argument".into(),
            ))
        }
    };
    Ok((func, arg))
}

/// An aggregate call's function and its one argument (`None` for `COUNT(*)`).
pub(crate) fn classify_agg_shape(
    call: PlainCall<'_>,
) -> Result<(AggFunc, Option<&sqlparser::ast::Expr>), GnitzSqlError> {
    use sqlparser::ast::{FunctionArg, FunctionArgExpr, FunctionArguments};
    let f = call.0;
    let base = single_fn_name(f)
        .and_then(agg_func_from_name)
        .ok_or_else(|| unknown_function(f))?;
    if base == AggFunc::Count {
        if let FunctionArguments::List(list) = &f.args {
            if let [FunctionArg::Unnamed(FunctionArgExpr::Wildcard)] = &list.args[..] {
                return Ok((AggFunc::Count, None));
            }
        }
    }
    Ok((base, Some(call.args(agg_func_name(base), (1, Some(1)))?[0])))
}

/// The binder a function call reaches, which decides the qualifiers it consumes:
/// an aggregate its `DISTINCT`, a window function its `OVER`.
#[derive(Clone, Copy)]
pub(crate) enum CallSurface<'a> {
    /// A scalar function, by the name it was called with.
    Scalar(&'a str),
    Aggregate,
    Window,
}

/// A function call with every qualifier its surface does not consume refused,
/// leaving its name and argument list to read.
#[derive(Clone, Copy)]
pub(crate) struct PlainCall<'f>(&'f sqlparser::ast::Function);

impl<'f> PlainCall<'f> {
    pub(crate) fn check(func: &'f sqlparser::ast::Function, surface: CallSurface<'_>) -> Result<Self, GnitzSqlError> {
        use sqlparser::ast::FunctionArguments;
        let on = match surface {
            CallSurface::Scalar(name) => name,
            CallSurface::Aggregate => "aggregates",
            CallSurface::Window => "window functions",
        };
        let sqlparser::ast::Function {
            // Consumed: the name dispatches the call, the argument list is bound.
            name: _,
            args,
            // Inert: ODBC's `{fn NAME(args)}` spells `NAME(args)`, round-trips
            // through `Display`, and changes no result.
            uses_odbc_syntax: _,
            over,
            parameters,
            filter,
            null_treatment,
            within_group,
        } = func;
        if over.is_some() && !matches!(surface, CallSurface::Window) {
            return Err(GnitzSqlError::Internal(format!(
                "{on}: a windowed call reached a non-window binder"
            )));
        }
        let call = PlainCall(func);
        reject_if(
            !matches!(surface, CallSurface::Aggregate) && call.is_distinct(),
            on,
            "DISTINCT",
        )?;
        reject_if(filter.is_some(), on, "FILTER (WHERE …)")?;
        reject_if(!within_group.is_empty(), on, "WITHIN GROUP (ORDER BY …)")?;
        reject_if(null_treatment.is_some(), on, "IGNORE/RESPECT NULLS")?;
        reject_if(
            !matches!(parameters, FunctionArguments::None),
            on,
            "parametric (ClickHouse) calls",
        )?;
        reject_if(
            matches!(args, FunctionArguments::List(list) if !list.clauses.is_empty()),
            on,
            "in-argument clauses (ORDER BY / LIMIT / SEPARATOR)",
        )?;
        Ok(call)
    }

    /// True for `f(DISTINCT x)`.
    pub(crate) fn is_distinct(self) -> bool {
        use sqlparser::ast::{DuplicateTreatment, FunctionArguments};
        matches!(&self.0.args, FunctionArguments::List(l)
            if matches!(l.duplicate_treatment, Some(DuplicateTreatment::Distinct)))
    }

    /// The plain positional argument exprs, `min..=max` of them (`None`: unbounded).
    pub(crate) fn args(
        self,
        name: &str,
        (min, max): (usize, Option<usize>),
    ) -> Result<Vec<&'f sqlparser::ast::Expr>, GnitzSqlError> {
        use sqlparser::ast::{FunctionArg, FunctionArgExpr, FunctionArguments};
        let FunctionArguments::List(list) = &self.0.args else {
            return Err(GnitzSqlError::Rejected(format!(
                "{name}: requires a parenthesized argument list"
            )));
        };
        let args = list
            .args
            .iter()
            .map(|arg| match arg {
                FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => Ok(e),
                _ => Err(GnitzSqlError::Rejected(format!(
                    "{name}: expects plain positional arguments (no `*`, named args)"
                ))),
            })
            .collect::<Result<Vec<_>, _>>()?;
        if args.len() < min || max.is_some_and(|max| args.len() > max) {
            return Err(wrong_arity(name, min, max));
        }
        Ok(args)
    }
}

fn wrong_arity(name: &str, min: usize, max: Option<usize>) -> GnitzSqlError {
    const WORDS: [&str; 4] = ["zero", "one", "two", "three"];
    let word = |n: usize| WORDS.get(n).map_or_else(|| n.to_string(), |w| (*w).to_string());
    let plural = |n: usize| if n == 1 { "argument" } else { "arguments" };
    let want = match max {
        Some(0) if min == 0 => return GnitzSqlError::Rejected(format!("{name}: takes no arguments")),
        Some(max) if max == min => format!("exactly {} {}", word(min), plural(min)),
        Some(max) => format!("{} or {} arguments", word(min), word(max)),
        None => format!("at least {} {}", word(min), plural(min)),
    };
    GnitzSqlError::Rejected(format!("{name}: requires {want}"))
}

/// True when a SELECT body is grouped — the one disjunction every "route this to
/// the grouped builder" test reads. A HAVING counts even with no aggregate
/// written: it filters the whole-relation group, which only that builder computes.
pub(crate) fn body_is_grouped(select: &sqlparser::ast::Select) -> bool {
    // `*` / `tbl.*` (the `None` items) cannot be an aggregate.
    group_by_is_present(&select.group_by)
        || select.having.is_some()
        || select
            .projection
            .iter()
            .filter_map(projection_item_expr)
            .any(expr_has_aggregate)
}

/// True when a SELECT body deduplicates its rows. `SELECT ALL` parses as a
/// quantifier too, and is the bag it spells.
pub(crate) fn select_is_distinct(select: &sqlparser::ast::Select) -> bool {
    matches!(select.distinct, Some(sqlparser::ast::Distinct::Distinct))
}

/// Whether `e` or any node beneath it satisfies `p` — the one recursive
/// existence walk over the [`expr_operands`] node set, so a walker written
/// against it inherits that set rather than re-spelling the recursion.
pub(crate) fn expr_any(e: &sqlparser::ast::Expr, p: &impl Fn(&sqlparser::ast::Expr) -> bool) -> bool {
    p(e) || expr_operands(e).into_iter().any(|o| expr_any(o, p))
}

/// Recursively test whether an expression contains an aggregate function call:
/// the call itself, or — for a non-aggregate wrapper over one (`abs(SUM(x))`,
/// still a grouped shape) — any of its operands.
pub(crate) fn expr_has_aggregate(e: &sqlparser::ast::Expr) -> bool {
    expr_any(e, &|e| matches!(e, sqlparser::ast::Expr::Function(f) if is_agg_call(f)))
}

/// The rejection for a call whose name is not one this crate implements. Shared
/// so every binder that turns a function away spells it the same way.
pub(crate) fn unknown_function(f: &sqlparser::ast::Function) -> GnitzSqlError {
    GnitzSqlError::Rejected(format!(
        "function '{}' not supported",
        f.name.to_string().to_ascii_lowercase()
    ))
}

/// Whether a function call names one of the aggregates. A windowed call is not
/// one: `SUM(x) OVER (…)` is a window function whose *argument* may hold an
/// aggregate, and it neither groups its body nor is collected into a reduce.
pub(crate) fn is_agg_call(f: &sqlparser::ast::Function) -> bool {
    f.over.is_none() && single_fn_name(f).and_then(agg_func_from_name).is_some()
}

/// The expressions a window specification keys on: its PARTITION BY, then its
/// ORDER BY. The one definition, so no two readers can disagree on what a
/// specification references.
pub(crate) fn window_spec_keys(spec: &sqlparser::ast::WindowSpec) -> impl Iterator<Item = &sqlparser::ast::Expr> {
    spec.partition_by.iter().chain(spec.order_by.iter().map(|o| &o.expr))
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
        _ => Err(GnitzSqlError::Rejected(
            "GROUP BY: only expression list supported".to_string(),
        )),
    }
}

/// The direct operand subexpressions of `e`. Subquery nodes contribute none: no
/// walker may silently descend into a subquery. Must cover every node
/// `bind_structural` recurses through — only this module's tests enforce that,
/// and a node it misses is invisible to every walker, silently.
pub(crate) fn expr_operands(e: &sqlparser::ast::Expr) -> Vec<&sqlparser::ast::Expr> {
    use sqlparser::ast::{CaseWhen, Expr, FunctionArg, FunctionArgExpr, FunctionArguments, WindowType};
    match e {
        Expr::BinaryOp { left, right, .. } => vec![left.as_ref(), right.as_ref()],
        Expr::UnaryOp { expr, .. } | Expr::Nested(expr) | Expr::IsNull(expr) | Expr::IsNotNull(expr) => {
            vec![expr.as_ref()]
        }
        Expr::Between { expr, low, high, .. } => vec![expr.as_ref(), low.as_ref(), high.as_ref()],
        Expr::IsDistinctFrom(a, b) | Expr::IsNotDistinctFrom(a, b) => vec![a.as_ref(), b.as_ref()],
        Expr::Position { expr, r#in } => vec![expr.as_ref(), r#in.as_ref()],
        // Keyword-dispatched: sqlparser gives these their own node rather than an
        // `Expr::Function`, so their operand is named here explicitly.
        Expr::Ceil { expr, .. } | Expr::Floor { expr, .. } | Expr::Cast { expr, .. } | Expr::Extract { expr, .. } => {
            vec![expr.as_ref()]
        }
        // SUBSTRING and TRIM are keyword-dispatched too. TRIM's `trim_what` is a
        // literal by the time the binder accepts it, but it is a bound operand
        // position and belongs in the walk regardless.
        Expr::Substring { expr, substring_from, substring_for, .. } => std::iter::once(expr.as_ref())
            .chain(substring_from.as_deref())
            .chain(substring_for.as_deref())
            .collect(),
        Expr::Trim { expr, trim_what, .. } => std::iter::once(expr.as_ref()).chain(trim_what.as_deref()).collect(),
        // Same for LIKE's pattern. Its `escape_char` is a `Value`, not an `Expr`,
        // so it contributes nothing.
        Expr::Like { expr, pattern, .. } | Expr::ILike { expr, pattern, .. } => {
            vec![expr.as_ref(), pattern.as_ref()]
        }
        Expr::InList { expr, list, .. } => std::iter::once(expr.as_ref()).chain(list).collect(),
        Expr::Case {
            operand,
            conditions,
            else_result,
            case_token: _,
            end_token: _,
        } => {
            let mut ops = Vec::new();
            ops.extend(operand.as_deref());
            for CaseWhen { condition, result } in conditions {
                ops.push(condition);
                ops.push(result);
            }
            ops.extend(else_result.as_deref());
            ops
        }
        // An inline window specification's keys ([`window_spec_keys`]) are
        // operands too, so the walkers see the aggregate in `ORDER BY SUM(x)`
        // and a subquery written there.
        Expr::Function(f) => {
            let mut ops: Vec<_> = match &f.args {
                FunctionArguments::List(list) => list
                    .args
                    .iter()
                    .filter_map(|a| match a {
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(inner)) => Some(inner),
                        _ => None,
                    })
                    .collect(),
                _ => Vec::new(),
            };
            if let Some(WindowType::WindowSpec(spec)) = &f.over {
                ops.extend(window_spec_keys(spec));
            }
            ops
        }
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
/// [`projection_item_expr`]. Anything that is not one scalar expression rejects,
/// naming the shape written; `ctx` names the clause.
pub(crate) fn scalar_projection_item<'a>(
    item: &'a SelectItem,
    ctx: &str,
) -> Result<(&'a sqlparser::ast::Expr, Option<String>), GnitzSqlError> {
    match item {
        SelectItem::UnnamedExpr(expr) => Ok((expr, None)),
        SelectItem::ExprWithAlias { expr, alias } => Ok((expr, Some(alias.value.clone()))),
        SelectItem::ExprWithAliases { .. } => Err(GnitzSqlError::Rejected(format!(
            "{ctx}: a multi-alias (`AS (a, b)`) SELECT item is not a supported SELECT item"
        ))),
        SelectItem::Wildcard(_) => Err(GnitzSqlError::Rejected(format!(
            "{ctx}: SELECT * is not a supported SELECT item"
        ))),
        SelectItem::QualifiedWildcard(..) => Err(GnitzSqlError::Rejected(format!(
            "{ctx}: SELECT <table>.* is not a supported SELECT item"
        ))),
    }
}

/// The expression surfaces subquery detection scans — a SELECT's
/// WHERE, its projection items (a wildcard contributes none) and its QUALIFY.
/// The one definition of "which surfaces decide detection", shared by the
/// EXISTS/IN and scalar/ANY/ALL detectors.
fn select_exprs(select: &sqlparser::ast::Select) -> impl Iterator<Item = &sqlparser::ast::Expr> {
    select
        .selection
        .iter()
        .chain(select.projection.iter().filter_map(projection_item_expr))
        .chain(select.qualify.iter())
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
/// table/view FROM".
pub(crate) enum FromShape<'a> {
    /// No FROM item at all.
    Empty,
    /// Exactly one plain relation name, no joins.
    SinglePlainRelation(&'a sqlparser::ast::TableFactor),
    /// A FROM that derives a relation rather than naming one, carrying the
    /// construct a rejection names.
    Derived(&'static str),
}

pub(crate) fn classify_from(from: &[sqlparser::ast::TableWithJoins]) -> FromShape<'_> {
    match from {
        [] => FromShape::Empty,
        [single] => {
            if !single.joins.is_empty() {
                // One FROM item carrying an explicit JOIN chain.
                FromShape::Derived("JOIN")
            } else if matches!(single.relation, sqlparser::ast::TableFactor::Table { .. }) {
                FromShape::SinglePlainRelation(&single.relation)
            } else {
                // One FROM item that is not a plain relation name (a derived
                // table, a table function, …).
                FromShape::Derived("derived table in FROM")
            }
        }
        // Multiple comma-separated FROM items (an implicit comma join). A view
        // body serves it as an INNER join keyed from the WHERE.
        _ => FromShape::Derived("comma-join FROM"),
    }
}

/// `(relation name, effective alias)` of a plain-table FROM factor: the
/// declared alias when present, else the name without its schema.
pub(crate) fn extract_table_name_and_alias(
    tf: &sqlparser::ast::TableFactor,
    session_schema: &str,
    context: &str,
) -> Result<(NameRef, String), GnitzSqlError> {
    let sqlparser::ast::TableFactor::Table {
        name,
        alias,
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
        return Err(GnitzSqlError::Rejected(format!(
            "{context}: only simple table references supported"
        )));
    };
    reject_if(args.is_some(), context, "a table function in FROM")?;
    reject_if(version.is_some(), context, "time travel (AS OF / AT)")?;
    reject_if(*with_ordinality, context, "WITH ORDINALITY")?;
    reject_if(!partitions.is_empty(), context, "PARTITION selection")?;
    reject_if(json_path.is_some(), context, "a JSON path on a table")?;
    reject_if(sample.is_some(), context, "TABLESAMPLE")?;
    let table_name = extract_object_name(name, session_schema, context)?;
    let alias = match alias {
        Some(a) => {
            let sqlparser::ast::TableAlias {
                explicit: _,
                name: alias_name,
                columns,
                at,
            } = a;
            // `FROM t AS d(x, y)` renames the columns positionally; honoring only
            // the relation alias would answer with `t`'s own column names.
            reject_if(
                !columns.is_empty(),
                context,
                "positional column aliases on a FROM table",
            )?;
            // Unreachable under GenericDialect; named so a dialect change is not silent.
            reject_if(at.is_some(), context, "AT (PartiQL index alias)")?;
            alias_name.value.clone()
        }
        None => table_name.rel.spelled_name().to_string(),
    };
    Ok((table_name, alias))
}

/// A strictly simple identifier expression's name, or the shared
/// "column must be a simple identifier" rejection. Backs [`index_column_ident`]
/// and the CLUSTER BY column list (a `Vec<Expr>` in sqlparser).
pub(crate) fn simple_ident_expr<'a>(e: &'a sqlparser::ast::Expr, context: &str) -> Result<&'a str, GnitzSqlError> {
    match e {
        sqlparser::ast::Expr::Identifier(id) => Ok(&id.value),
        _ => Err(GnitzSqlError::Rejected(format!(
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

/// A column reference as `(written qualifier, column name)`, parentheses peeled
/// — `(a)` is `a` at every surface. `None` for any other shape, so each caller
/// raises its own error.
pub(crate) fn col_ref_parts(e: &sqlparser::ast::Expr) -> Option<(Option<&str>, &str)> {
    use sqlparser::ast::Expr;
    match peel_nested(e) {
        Expr::Identifier(id) => Some((None, id.value.as_str())),
        Expr::CompoundIdentifier(p) if p.len() == 2 => Some((Some(p[0].value.as_str()), p[1].value.as_str())),
        _ => None,
    }
}

/// One wildcard's modifiers, classified. The **only** place a
/// `WildcardAdditionalOptions` is read, so a field a future sqlparser adds
/// cannot reach one reader and miss another — the destructure is exhaustive and
/// every reader goes through this struct.
struct WildcardMods<'a> {
    /// A modifier gnitz does not honor, spelled for the rejection.
    refused: Option<&'static str>,
    /// `EXCEPT` ∪ `EXCLUDE` — synonyms (ClickHouse/BigQuery vs Snowflake).
    drop: Vec<&'a str>,
    /// `RENAME` as (from, to) pairs.
    rename: Vec<(&'a str, &'a str)>,
}

fn wildcard_mods(o: &WildcardAdditionalOptions) -> WildcardMods<'_> {
    let WildcardAdditionalOptions {
        wildcard_token: _, // span only
        opt_ilike,
        opt_exclude,
        opt_except,
        opt_replace,
        opt_rename,
        // Redshift `SELECT * AS x`; `GenericDialect` leaves
        // `supports_select_wildcard_with_alias()` false, so the parser never fills it.
        opt_alias,
    } = o;
    let mut drop: Vec<&str> = Vec::new();
    if let Some(e) = opt_except {
        drop.push(e.first_element.value.as_str());
        drop.extend(e.additional_elements.iter().map(|i| i.value.as_str()));
    }
    match opt_exclude {
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
    let mut rename: Vec<(&str, &str)> = Vec::new();
    match opt_rename {
        Some(RenameSelectItem::Single(a)) => rename.push((a.ident.value.as_str(), a.alias.value.as_str())),
        Some(RenameSelectItem::Multiple(v)) => {
            rename.extend(v.iter().map(|a| (a.ident.value.as_str(), a.alias.value.as_str())))
        }
        None => {}
    }
    WildcardMods {
        // gnitz honors neither a computed value substitution nor a name-pattern
        // filter, so expanding a plain `*` in their place would answer a
        // different query than the one written.
        refused: opt_replace
            .is_some()
            .then_some("SELECT * REPLACE")
            .or(opt_ilike.is_some().then_some("SELECT * ILIKE"))
            .or(opt_alias.is_some().then_some("SELECT * AS")),
        drop,
        rename,
    }
}

/// Expand one wildcard over `cols` into `(source index, output def)` pairs, in
/// source order, dropping and renaming per its modifiers. Hidden columns are
/// skipped: a synthetic view key (`_join_pk`, `_set_pk`, …) must never re-enter
/// a payload or a row identity through a wildcard. Matching is case-insensitive,
/// like `find_unique_column`. `ctx` names the surface for the messages.
pub(crate) fn expand_wildcard_item<'a, I>(
    o: &WildcardAdditionalOptions,
    cols: I,
    ctx: &str,
) -> Result<Vec<(usize, ColumnDef)>, GnitzSqlError>
where
    I: IntoIterator<Item = &'a ColumnDef> + Clone,
{
    let WildcardMods { refused, drop, rename } = wildcard_mods(o);
    if let Some(what) = refused {
        return Err(crate::error::unsupported_clause(ctx, what));
    }
    let excludes = |name: &str| drop.iter().any(|d| d.eq_ignore_ascii_case(name));
    let output_name = |name: &str| {
        rename
            .iter()
            .find(|(f, _)| f.eq_ignore_ascii_case(name))
            .map(|&(_, t)| t)
    };
    // Each rejection below covers a rewrite that would otherwise do nothing and
    // say nothing.
    for &n in drop.iter().chain(rename.iter().map(|(f, _)| f)) {
        if !has_visible_column(cols.clone(), n) {
            return Err(GnitzSqlError::Rejected(format!(
                "{ctx}: SELECT * EXCEPT/EXCLUDE/RENAME names unknown column '{n}'"
            )));
        }
    }
    if let Some((f, _)) = rename.iter().find(|(f, _)| excludes(f)) {
        return Err(GnitzSqlError::Rejected(format!(
            "{ctx}: SELECT * RENAME names excluded column '{f}'"
        )));
    }
    if let Some(f) = first_duplicate(rename.iter().map(|&(f, _)| f)) {
        return Err(GnitzSqlError::Rejected(format!(
            "{ctx}: SELECT * RENAME names column '{f}' twice"
        )));
    }
    Ok(cols
        .into_iter()
        .enumerate()
        .filter(|(_, c)| !c.is_hidden && !excludes(&c.name))
        .map(|(i, c)| {
            let mut out = c.clone();
            if let Some(new) = output_name(&out.name) {
                out.name = new.to_string();
            }
            (i, out)
        })
        .collect())
}

/// Whether `cols` holds a *visible* (non-hidden) column named `name`, matched
/// case-insensitively. Unlike `bind::find_unique_column`, two matches are not an
/// error: `SELECT * EXCEPT (id)` over a two-`id` join deliberately drops both.
pub(crate) fn has_visible_column<'a>(cols: impl IntoIterator<Item = &'a ColumnDef>, name: &str) -> bool {
    cols.into_iter()
        .any(|c| !c.is_hidden && c.name.eq_ignore_ascii_case(name))
}

/// True when the projection is one wildcard naming no output column of its own,
/// so the output names are the source's: a duplicate among them belongs to the
/// source (a join surfacing both sides' `val`) and is carried through rather than
/// rejected. `RENAME` names its output, so it gets the duplicate check.
pub(crate) fn is_name_preserving_wildcard_projection(projection: &[SelectItem]) -> bool {
    matches!(projection, [SelectItem::Wildcard(o)] if wildcard_mods(o).rename.is_empty())
}

#[cfg(test)]
#[path = "tests/ast_util.rs"]
mod tests;
