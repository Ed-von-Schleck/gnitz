//! The planner's shared sqlparser-AST layer. An item lives here because two or
//! more surfaces must agree on it, so its doc states the rule it enforces —
//! never who calls it, which rots the moment a caller moves.

use std::convert::Infallible;

use crate::error::{reject_if, GnitzSqlError};
use crate::ir::{AggFunc, BExpr};
use gnitz_core::{ColumnDef, TypeCode};
use sqlparser::ast::{
    ExcludeSelectItem, Expr, RenameSelectItem, SelectItem, UnaryOperator, Value, WildcardAdditionalOptions,
};

/// The identifier of an `ObjectName`'s last part, or `None` when that part is
/// not a plain identifier.
pub(crate) fn object_name_ident(name: &sqlparser::ast::ObjectName) -> Option<&sqlparser::ast::Ident> {
    name.0.last().and_then(|p| p.as_ident())
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
        return Err(GnitzSqlError::Plan(format!("empty name in {context}")));
    }
    Ok(parts)
}

/// A schema-scoped catalog object (table, view). The active schema is a session
/// parameter, never part of the statement, so a qualifier is accepted only when
/// it names `session_schema`; anything else names a relation no gnitz statement
/// can reach.
pub(crate) fn extract_object_name(
    name: &sqlparser::ast::ObjectName,
    session_schema: &str,
    context: &str,
) -> Result<String, GnitzSqlError> {
    match object_name_parts(name, context)?.as_slice() {
        [n] => Ok((*n).to_string()),
        [s, n] if s.eq_ignore_ascii_case(session_schema) => Ok((*n).to_string()),
        [s, ..] => Err(GnitzSqlError::Unsupported(format!(
            "{context}: cross-schema names are not supported \
             (qualifier '{s}' is not the session schema '{session_schema}')"
        ))),
        // `object_name_parts` rejects the empty name, so this is 3+ parts.
        _ => Err(GnitzSqlError::Unsupported(format!(
            "{context}: '{name}' has too many name parts"
        ))),
    }
}

/// An index name, which is global rather than schema-scoped — so a qualifier
/// would read as a scoping that does not exist, and is rejected.
pub(crate) fn extract_index_name(name: &sqlparser::ast::ObjectName, context: &str) -> Result<String, GnitzSqlError> {
    match object_name_parts(name, context)?.as_slice() {
        [n] => Ok((*n).to_string()),
        _ => Err(GnitzSqlError::Unsupported(format!(
            "{context}: an index name takes no qualifier (index names are global, not schema-scoped)"
        ))),
    }
}

/// A column reference the parser handed over as an `ObjectName`: a qualifier here
/// is `table.col`, so the column is the last part.
pub(crate) fn extract_ident_name(name: &sqlparser::ast::ObjectName, context: &str) -> Result<String, GnitzSqlError> {
    object_name_ident(name)
        .map(|i| i.value.clone())
        .ok_or_else(|| GnitzSqlError::Plan(format!("empty name in {context}")))
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
/// over `R` without a `Clone` bound. A magnitude past `i64` binds to `LitWide`,
/// for the reason [`BExpr::LitWide`] states.
pub(crate) fn bind_literal<R>(v: &Value) -> Result<BExpr<R>, GnitzSqlError> {
    match v {
        Value::Null => Ok(BExpr::LitNull),
        Value::Number(n, _) => {
            if let Ok(i) = n.parse::<i64>() {
                Ok(BExpr::LitInt(i))
            } else if n.contains(['.', 'e', 'E']) {
                n.parse::<f64>()
                    .map(BExpr::LitFloat)
                    .map_err(|_| GnitzSqlError::Plan(format!("invalid number literal: {n}")))
            } else {
                Ok(BExpr::LitWide(n.clone()))
            }
        }
        Value::SingleQuotedString(s) => Ok(BExpr::LitStr(s.clone())),
        _ => Err(GnitzSqlError::Unsupported(format!(
            "value type not supported in expressions: {v:?}"
        ))),
    }
}

/// `DATE '…'`, `TIMESTAMP '…'` and `CAST('…' AS DATE)`: the literal spellings of
/// a temporal value, as its type and storage integer. `None` for every other
/// expression, a typed string or cast of a non-temporal type included.
pub(crate) fn temporal_constant(e: &Expr) -> Result<Option<(TypeCode, i64)>, GnitzSqlError> {
    let (dt, v) = match peel_nested(e) {
        Expr::TypedString(ts) => (&ts.data_type, &ts.value.value),
        // The qualifiers the general CAST arm rejects are left to it: an
        // `ARRAY`/`FORMAT` cast is not a literal spelling and must not be
        // claimed here, or it would ride through unchecked.
        Expr::Cast {
            expr,
            data_type,
            array: false,
            format: None,
            ..
        } => match peel_nested(expr) {
            Expr::Value(vws) => (data_type, &vws.value),
            _ => return Ok(None),
        },
        _ => return Ok(None),
    };
    let tc = crate::types::sql_type_to_typecode(dt)?;
    match v {
        _ if !tc.is_temporal() => Ok(None),
        Value::SingleQuotedString(s) => Ok(Some((tc, crate::types::temporal_literal(tc, s)?))),
        v => Err(GnitzSqlError::Unsupported(format!(
            "{dt} literal must be a single-quoted string, got {v}"
        ))),
    }
}

/// A constant as written: the literal's magnitude, and its sign kept apart from
/// it — folding the sign in would destroy `-0`, whose sign a float column keeps.
/// A *written* sign implies a numeric literal or NULL; [`bind_constant`] is the
/// sole constructor and refuses one on anything else.
#[derive(Debug)]
pub(crate) struct Constant {
    /// `Infallible` is uninhabited, so this has no `ColRef` inhabitant: the type
    /// states that no column reference can occur.
    pub(crate) lit: BExpr<Infallible>,
    pub(crate) negated: bool,
}

impl std::fmt::Display for Constant {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let sign = if self.negated { "-" } else { "" };
        match &self.lit {
            BExpr::LitInt(v) => write!(f, "{sign}{v}"),
            BExpr::LitFloat(v) => write!(f, "{sign}{v}"),
            BExpr::LitWide(s) => write!(f, "{sign}{s}"),
            BExpr::LitStr(s) => write!(f, "'{s}'"),
            _ => write!(f, "NULL"),
        }
    }
}

/// The one decoder for a constant position, so no two of them can disagree on
/// what a written constant is: parentheses peeled, one optional sign, a literal.
pub(crate) fn bind_constant(e: &Expr) -> Result<Constant, GnitzSqlError> {
    // sqlparser lexes a literal's sign as a separate unary operator, so `+5` and
    // `-5` are both `UnaryOp`, never a bare `Value::Number`. `sign` is `None`
    // when none was written — which `negated` alone cannot say, and `+'abc'`
    // needs it said: either sign over a string used to be discarded silently.
    let (inner, sign) = match peel_nested(e) {
        Expr::UnaryOp {
            op: op @ (UnaryOperator::Minus | UnaryOperator::Plus),
            expr,
        } => (peel_nested(expr), Some(matches!(op, UnaryOperator::Minus))),
        e => (e, None),
    };
    // A temporal literal is already the integer its column stores; a sign over
    // one is refused below like a sign over a string.
    if let (None, Some((_, v))) = (sign, temporal_constant(inner)?) {
        return Ok(Constant { lit: BExpr::LitInt(v), negated: false });
    }
    let Expr::Value(vws) = inner else {
        return Err(GnitzSqlError::Unsupported(format!(
            "expected a constant, got the expression: {e}"
        )));
    };
    let lit = bind_literal(&vws.value)?;
    if let (Some(_), BExpr::LitStr(s)) = (sign, &lit) {
        return Err(GnitzSqlError::Unsupported(format!(
            "a sign does not apply to the string literal '{s}'"
        )));
    }
    Ok(Constant { lit, negated: sign == Some(true) })
}

/// Parse `e` as a non-negative integer literal, or error — silently degrading a
/// LIMIT returns every row. `what` names the clause for the message.
pub(crate) fn expr_usize_literal(e: &Expr, what: &str) -> Result<usize, GnitzSqlError> {
    let not_a_literal = || GnitzSqlError::Unsupported(format!("{what} must be an integer literal, not an expression"));
    let c = bind_constant(e).map_err(|_| not_a_literal())?;
    match &c.lit {
        // `bind_literal` yields the magnitude, so an unnegated `LitInt` is ≥ 0.
        BExpr::LitInt(n) if !c.negated => Ok(*n as usize),
        BExpr::LitInt(_) | BExpr::LitFloat(_) | BExpr::LitWide(_) => Err(GnitzSqlError::Unsupported(format!(
            "{what} must be a non-negative integer literal, got '{c}'"
        ))),
        _ => Err(not_a_literal()),
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

/// The one SQL-name ↔ aggregate map, read in both directions by
/// [`agg_func_from_name`] and [`agg_func_name`] — a bijection, so a name can
/// never drift between the two directions.
const AGG_NAMES: [(&str, AggFunc); 5] = [
    ("count", AggFunc::Count),
    ("sum", AggFunc::Sum),
    ("min", AggFunc::Min),
    ("max", AggFunc::Max),
    ("avg", AggFunc::Avg),
];

/// The `AggFunc` a function name denotes (`count`, `sum`, `min`, `max`, `avg`),
/// matched case-insensitively without allocating; `None` for any other name.
pub(crate) fn agg_func_from_name(name: &str) -> Option<AggFunc> {
    AGG_NAMES
        .into_iter()
        .find_map(|(n, f)| name.eq_ignore_ascii_case(n).then_some(f))
}

/// The canonical lowercase SQL name of an aggregate — [`agg_func_from_name`]
/// inverted over the same table.
pub(crate) fn agg_func_name(f: AggFunc) -> &'static str {
    AGG_NAMES
        .iter()
        .find_map(|&(n, g)| (g == f).then_some(n))
        .expect("every AggFunc spelling is in AGG_NAMES")
}

/// The invariant the two non-window entry points below rest on: `bind_structural`
/// routes a windowed call to the leaf, so only `classify_window_call` ever sees
/// one and `over` needs no handling here.
fn debug_assert_not_windowed(f: &sqlparser::ast::Function) {
    debug_assert!(
        f.over.is_none(),
        "a windowed call is routed by bind_structural, not classified here"
    );
}

/// Classify an aggregate call into `(func, arg)`. `COUNT(*)` is the only shape
/// yielding no argument, so `arg.is_some()` *is* the `COUNT(x)` vs `COUNT(*)`
/// distinction. The argument comes back unbound, for the caller to resolve
/// against its own leaf.
pub(crate) fn classify_agg_call(
    f: &sqlparser::ast::Function,
) -> Result<(AggFunc, Option<&sqlparser::ast::Expr>), GnitzSqlError> {
    debug_assert_not_windowed(f);
    reject_fn_qualifiers(f, "aggregates")?;
    classify_agg_shape(f)
}

/// The argument-shape half of [`classify_agg_call`], with the qualifiers
/// already checked by the caller — a windowed aggregate (`SUM(x) OVER (…)`)
/// consumes its `OVER` and classifies its name and argument through here.
pub(crate) fn classify_agg_shape(
    f: &sqlparser::ast::Function,
) -> Result<(AggFunc, Option<&sqlparser::ast::Expr>), GnitzSqlError> {
    use sqlparser::ast::{FunctionArg, FunctionArgExpr, FunctionArguments};
    let base = single_fn_name(f)
        .and_then(agg_func_from_name)
        .ok_or_else(|| unknown_function(f))?;
    let args: &[FunctionArg] = match &f.args {
        FunctionArguments::List(list) => &list.args,
        _ => &[],
    };
    match (base, args) {
        (AggFunc::Count, [FunctionArg::Unnamed(FunctionArgExpr::Wildcard)]) => Ok((AggFunc::Count, None)),
        (_, [FunctionArg::Unnamed(FunctionArgExpr::Expr(e))]) => Ok((base, Some(e))),
        (AggFunc::Count, _) => Err(GnitzSqlError::Unsupported("COUNT: unsupported argument form".into())),
        _ => Err(GnitzSqlError::Unsupported(format!(
            "{}: requires exactly one column argument",
            agg_func_name(base)
        ))),
    }
}

/// Reject any qualifier on a function call the binder does not implement — a
/// binder reads the name and the argument list, so an unrejected qualifier is
/// silently dropped and the plain call computed. `on` names the context.
pub(crate) fn reject_fn_qualifiers(func: &sqlparser::ast::Function, on: &str) -> Result<(), GnitzSqlError> {
    use sqlparser::ast::{DuplicateTreatment, FunctionArguments};
    let sqlparser::ast::Function {
        // Consumed: the name dispatches the call, the argument list is bound.
        name: _,
        args,
        // Inert: ODBC's `{fn NAME(args)}` spells `NAME(args)`, round-trips
        // through `Display`, and changes no result.
        uses_odbc_syntax: _,
        // Consumed elsewhere: `bind_structural` routes a windowed call to the
        // leaf, and `classify_window_call` consumes the specification. Nothing
        // reaches this table with an unconsumed `OVER`.
        over: _,
        // Rejected below.
        parameters,
        filter,
        null_treatment,
        within_group,
    } = func;
    let unsupported = |what: &str| Err(GnitzSqlError::Unsupported(format!("{what}: not supported on {on}")));
    if filter.is_some() {
        return unsupported("FILTER (WHERE …)");
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
/// for `*`, named args, or any qualifier [`reject_fn_qualifiers`] refuses.
pub(crate) fn function_positional_args<'f>(
    f: &'f sqlparser::ast::Function,
    name: &str,
) -> Result<Vec<&'f sqlparser::ast::Expr>, GnitzSqlError> {
    use sqlparser::ast::{FunctionArg, FunctionArgExpr, FunctionArguments};
    debug_assert_not_windowed(f);
    reject_fn_qualifiers(f, name)?;
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

/// Whether `e` or any node beneath it satisfies `p` — the one recursive
/// existence walk over the [`expr_operands`] node set, so a walker written
/// against it inherits that set rather than re-spelling the recursion.
pub(crate) fn expr_any(e: &sqlparser::ast::Expr, p: &impl Fn(&sqlparser::ast::Expr) -> bool) -> bool {
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

/// Whether a SELECT carries a window call (`… OVER (…)`) in its projection or
/// its QUALIFY — the routing test for the windowed SELECT list.
pub(crate) fn select_has_window(select: &sqlparser::ast::Select) -> bool {
    select_exprs(select).any(|e| {
        expr_any(
            e,
            &|e| matches!(e, sqlparser::ast::Expr::Function(f) if f.over.is_some()),
        )
    })
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
///
/// The gate is a *bare* `Value::Number`, deliberately narrower than
/// [`bind_constant`]: `ORDER BY (1)` and `ORDER BY +1` are expressions.
pub(crate) fn clause_position(e: &Expr, what: &str) -> Result<Option<usize>, GnitzSqlError> {
    match e {
        Expr::Value(v) if matches!(v.value, Value::Number(..)) => Ok(Some(expr_usize_literal(e, what)?)),
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

/// The direct operand subexpressions of `e`. Subquery nodes contribute none: no
/// walker may silently descend into a subquery. Must cover every node
/// `bind_structural` recurses through — only `tests/ast_util.rs` enforces that,
/// and a node it misses is invisible to every walker, silently.
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
        Expr::Substring { expr, substring_from, substring_for, .. } => std::iter::once(expr.as_ref())
            .chain(substring_from.as_deref())
            .chain(substring_for.as_deref())
            .collect(),
        Expr::Trim { expr, trim_what, .. } => std::iter::once(expr.as_ref()).chain(trim_what.as_deref()).collect(),
        // Same for LIKE's pattern. Its `escape_char` is a `Value`, not an `Expr`,
        // so it contributes nothing.
        Expr::Like { expr, pattern, .. } | Expr::ILike { expr, pattern, .. } => vec![expr, pattern],
        Expr::InList { expr, list, .. } => std::iter::once(expr.as_ref()).chain(list).collect(),
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
        // An inline window specification's keys are operands too, so the walkers
        // see the aggregate in `ORDER BY SUM(x)` and a subquery written there.
        Expr::Function(f) => {
            let mut ops: Vec<&Expr> = match &f.args {
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
            if let Some(sqlparser::ast::WindowType::WindowSpec(spec)) = &f.over {
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
        SelectItem::ExprWithAliases { .. } => Err(GnitzSqlError::Unsupported(format!(
            "{ctx}: a multi-alias (`AS (a, b)`) SELECT item is not a supported SELECT item"
        ))),
        SelectItem::Wildcard(_) => Err(GnitzSqlError::Unsupported(format!(
            "{ctx}: SELECT * is not a supported SELECT item"
        ))),
        SelectItem::QualifiedWildcard(..) => Err(GnitzSqlError::Unsupported(format!(
            "{ctx}: SELECT <table>.* is not a supported SELECT item"
        ))),
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

/// The expression surfaces subquery and window detection scan — a SELECT's
/// WHERE, its projection items (a wildcard contributes none) and its QUALIFY.
/// The one definition of "which surfaces decide detection", shared by the
/// EXISTS/IN, scalar/ANY/ALL and window detectors.
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
    SinglePlainRelation(&'a sqlparser::ast::TableFactor),
}

pub(crate) fn classify_from(from: &[sqlparser::ast::TableWithJoins]) -> FromShape<'_> {
    match from {
        [] => FromShape::Empty,
        [single] => {
            if !single.joins.is_empty() {
                FromShape::Join
            } else if matches!(single.relation, sqlparser::ast::TableFactor::Table { .. }) {
                FromShape::SinglePlainRelation(&single.relation)
            } else {
                FromShape::DerivedTable
            }
        }
        _ => FromShape::CommaJoin,
    }
}

/// Extract `(relation name, effective alias)` from a plain-table FROM factor —
/// the declared alias when present, else the name itself. The one acceptance
/// point for a base-relation FROM factor, so its name goes through
/// [`extract_object_name`] like every other.
pub(crate) fn extract_table_name_and_alias(
    tf: &sqlparser::ast::TableFactor,
    session_schema: &str,
    context: &str,
) -> Result<(String, String), GnitzSqlError> {
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
        None => table_name.clone(),
    };
    Ok((table_name, alias))
}

/// A strictly simple identifier expression's name, or the shared
/// "column must be a simple identifier" Bind error. Backs [`index_column_ident`]
/// and the CLUSTER BY column list (a `Vec<Expr>` in sqlparser).
pub(crate) fn simple_ident_expr<'a>(e: &'a sqlparser::ast::Expr, context: &str) -> Result<&'a str, GnitzSqlError> {
    match e {
        sqlparser::ast::Expr::Identifier(id) => Ok(&id.value),
        _ => Err(GnitzSqlError::Unsupported(format!(
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
    /// A modifier was written, so the item is not a plain `*`.
    present: bool,
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
        present: opt_ilike.is_some()
            || opt_exclude.is_some()
            || opt_except.is_some()
            || opt_replace.is_some()
            || opt_rename.is_some()
            || opt_alias.is_some(),
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
    let WildcardMods { refused, drop, rename, .. } = wildcard_mods(o);
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
            return Err(GnitzSqlError::Bind(format!(
                "{ctx}: SELECT * EXCEPT/EXCLUDE/RENAME names unknown column '{n}'"
            )));
        }
    }
    if let Some((f, _)) = rename.iter().find(|(f, _)| excludes(f)) {
        return Err(GnitzSqlError::Plan(format!(
            "{ctx}: SELECT * RENAME names excluded column '{f}'"
        )));
    }
    for (i, (f, _)) in rename.iter().enumerate() {
        if rename[i + 1..].iter().any(|(g, _)| g.eq_ignore_ascii_case(f)) {
            return Err(GnitzSqlError::Plan(format!(
                "{ctx}: SELECT * RENAME names column '{f}' twice"
            )));
        }
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

/// True when the projection is a plain `*` — the identity expansion the no-op
/// passthrough fast paths need; anything else falls through to
/// [`expand_wildcard_item`]. Both predicates match *one* item, so `SELECT *, *`
/// reaches the duplicate-name gate that `SELECT id, a, a` is rejected by.
pub(crate) fn is_bare_wildcard_projection(projection: &[SelectItem]) -> bool {
    matches!(projection, [SelectItem::Wildcard(o)] if !wildcard_mods(o).present)
}

#[cfg(test)]
#[path = "tests/ast_util.rs"]
mod tests;
