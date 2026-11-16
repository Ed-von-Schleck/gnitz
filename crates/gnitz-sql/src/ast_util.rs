//! The planner's shared sqlparser-AST layer. An item lives here because two or
//! more surfaces must agree on it, so its doc states the rule it enforces —
//! never who calls it, which rots the moment a caller moves.

use crate::agg::{AggArg, AggFunc, agg_func_from_name, agg_func_name};
use crate::error::{GnitzSqlError, reject_if};
use sqlparser::ast::{Visit, Visitor};
use std::ops::ControlFlow;

/// The name an `ObjectName` of exactly one plain-identifier part spells; `None`
/// for a qualified name, so `t.a` is refused rather than truncated to `a`.
pub(crate) fn single_part_ident(name: &sqlparser::ast::ObjectName) -> Option<&str> {
    match &name.0[..] {
        [part] => part.as_ident().map(|i| i.value.as_str()),
        _ => None,
    }
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
            ));
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
    let base = single_part_ident(&f.name)
        .and_then(agg_func_from_name)
        .ok_or_else(|| unknown_function(f))?;
    if base == AggFunc::Count
        && let FunctionArguments::List(list) = &f.args
        && let [FunctionArg::Unnamed(FunctionArgExpr::Wildcard)] = &list.args[..]
    {
        return Ok((AggFunc::Count, None));
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

/// Whether `e` or any expression beneath it satisfies `p`. A subquery's body is
/// not searched.
pub(crate) fn expr_any(e: &sqlparser::ast::Expr, p: &dyn Fn(&sqlparser::ast::Expr) -> bool) -> bool {
    use sqlparser::ast::{Expr, Query};
    struct Outside<'p> {
        p: &'p dyn Fn(&Expr) -> bool,
        depth: usize,
    }
    impl Visitor for Outside<'_> {
        type Break = ();
        fn pre_visit_query(&mut self, _: &Query) -> ControlFlow<()> {
            self.depth += 1;
            ControlFlow::Continue(())
        }
        fn post_visit_query(&mut self, _: &Query) -> ControlFlow<()> {
            self.depth -= 1;
            ControlFlow::Continue(())
        }
        fn pre_visit_expr(&mut self, e: &Expr) -> ControlFlow<()> {
            match self.depth == 0 && (self.p)(e) {
                true => ControlFlow::Break(()),
                false => ControlFlow::Continue(()),
            }
        }
    }
    e.visit(&mut Outside { p, depth: 0 }).is_break()
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
    f.over.is_none() && single_part_ident(&f.name).and_then(agg_func_from_name).is_some()
}

/// Strip redundant parentheses.
pub(crate) fn peel_nested(e: &sqlparser::ast::Expr) -> &sqlparser::ast::Expr {
    let mut cur = e;
    while let sqlparser::ast::Expr::Nested(inner) = cur {
        cur = inner;
    }
    cur
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

#[cfg(test)]
#[path = "tests/ast_util.rs"]
mod tests;
