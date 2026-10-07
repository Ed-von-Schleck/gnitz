//! The query tail — `ORDER BY`, `LIMIT`, `OFFSET` — read off the sqlparser AST
//! into key specs and counts, and the 1-based positions ORDER BY and GROUP BY
//! share. More than one surface carries a tail and they must agree on it, so it
//! sits below all of them: no surface reaches into another for the rule, and a
//! rejection here cannot name a surface it does not know.

use crate::ast_util::{expr_has_aggregate, peel_nested, projection_item_expr};
use crate::bind::bind_constant;
use crate::error::{reject_if, unsupported_clause, GnitzSqlError};
use crate::ir::BExpr;
use gnitz_wire::ColumnDef;
use sqlparser::ast::{Expr, LimitClause, OrderBy, OrderByExpr, OrderByKind, OrderByOptions, Value};

/// Parse `e` as a non-negative integer literal, or error — silently degrading a
/// LIMIT returns every row. `what` names the clause for the message.
pub(crate) fn expr_usize_literal(e: &Expr, what: &str) -> Result<usize, GnitzSqlError> {
    let not_a_literal = || GnitzSqlError::Rejected(format!("{what} must be an integer literal, not an expression"));
    let c = bind_constant(e).map_err(|_| not_a_literal())?;
    match c {
        BExpr::LitInt(n) if n >= 0 => Ok(n as usize),
        BExpr::LitInt(_) | BExpr::LitFloat { .. } | BExpr::LitWide(_) => Err(GnitzSqlError::Rejected(format!(
            "{what} must be a non-negative integer literal, got '{}'",
            c.literal_text()
        ))),
        _ => Err(not_a_literal()),
    }
}

/// The 1-based position a clause item names, or `None` when it is not an integer
/// literal. ORDER BY and GROUP BY share this rule; LIMIT and OFFSET call
/// [`expr_usize_literal`] directly, since a literal is the only form legal there.
///
/// The gate is a *bare* `Value::Number`, deliberately narrower than
/// [`bind_constant`]: `ORDER BY (1)` and
/// `ORDER BY +1` are expressions.
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
        return Err(GnitzSqlError::Rejected(format!(
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
        GnitzSqlError::Rejected(format!(
            "GROUP BY position {pos} names a wildcard, which is not a group key"
        ))
    })?;
    if expr_has_aggregate(target) {
        return Err(GnitzSqlError::Rejected(format!(
            "GROUP BY position {pos} names an aggregate, which cannot be a group key"
        )));
    }
    Ok(peel_nested(target))
}

/// What an ORDER BY key names: a 1-based visible-output position, or an
/// expression — a bare or qualified name resolving output-first, anything else
/// bound in the SELECT list's own scope and carried as a hidden column.
pub(crate) enum OrderTarget<'a> {
    Position(usize),
    Expr(&'a Expr),
}

/// A parsed ORDER BY key: its target plus resolved direction and absolute NULL
/// placement (default NULLS LAST for ASC, FIRST for DESC).
pub(crate) struct OrderKey<'a> {
    pub(crate) target: OrderTarget<'a>,
    pub(crate) desc: bool,
    pub(crate) nulls_first: bool,
}

impl OrderKey<'_> {
    pub(crate) fn wire(&self, col: usize) -> gnitz_wire::OrderKey {
        gnitz_wire::OrderKey {
            col: col as u16,
            desc: self.desc,
            nulls_first: self.nulls_first,
        }
    }
}

/// One ORDER BY key: its expression, whether it descends, and its absolute NULL
/// placement (default NULLS LAST ascending, FIRST descending). `clause` names the
/// ORDER BY in the refusal.
pub(crate) fn parse_order_key<'a>(obe: &'a OrderByExpr, clause: &str) -> Result<(&'a Expr, bool, bool), GnitzSqlError> {
    // Exhaustive (no `..`): a dropped modifier is a wrong result.
    let OrderByExpr { expr, options, with_fill } = obe;
    reject_if(with_fill.is_some(), clause, "WITH FILL")?;
    let OrderByOptions { asc, nulls_first } = options;
    let desc = !asc.unwrap_or(true);
    Ok((expr, desc, nulls_first.unwrap_or(desc)))
}

/// The expression keys of `keys`, in key order — the list a SELECT list's binder
/// places one projection item each for, and [`key_slots`] reads back.
pub(crate) fn order_exprs<'a>(keys: &[OrderKey<'a>]) -> Vec<&'a Expr> {
    keys.iter()
        .filter_map(|k| match k.target {
            OrderTarget::Expr(e) => Some(e),
            OrderTarget::Position(_) => None,
        })
        .collect()
}

/// Classify one ORDER BY expression: an integer literal is positional, by the
/// same rule GROUP BY reads; everything else is an expression.
fn order_target(e: &Expr) -> Result<OrderTarget<'_>, GnitzSqlError> {
    Ok(match clause_position(e, "ORDER BY position")? {
        Some(pos) => OrderTarget::Position(pos),
        None => OrderTarget::Expr(e),
    })
}

/// The ORDER BY clause as key specs, or none for an absent clause.
pub(crate) fn parse_order_by(order_by: Option<&OrderBy>) -> Result<Vec<OrderKey<'_>>, GnitzSqlError> {
    let Some(ob) = order_by else {
        return Ok(Vec::new());
    };
    let keys = resolve_order_by(ob)?;
    if keys.len() > gnitz_wire::MAX_ORDER_KEYS {
        return Err(GnitzSqlError::Rejected(format!(
            "ORDER BY has more than {} keys",
            gnitz_wire::MAX_ORDER_KEYS
        )));
    }
    Ok(keys)
}

/// Parse the whole ORDER BY clause into resolved key specs, rejecting the
/// unsupported ClickHouse/DuckDB extensions.
fn resolve_order_by(ob: &OrderBy) -> Result<Vec<OrderKey<'_>>, GnitzSqlError> {
    // Exhaustive (no `..`) here and in `parse_order_key`: a dropped ORDER BY
    // modifier is a wrong result, so a future `sqlparser` field stops the build.
    let OrderBy { kind, interpolate } = ob;
    reject_if(interpolate.is_some(), "ORDER BY", "INTERPOLATE")?;
    let exprs = match kind {
        OrderByKind::Expressions(e) => e,
        OrderByKind::All(_) => return Err(unsupported_clause("ORDER BY", "ALL")),
    };
    let mut keys = Vec::with_capacity(exprs.len());
    for obe in exprs {
        let (expr, desc, nulls_first) = parse_order_key(obe, "ORDER BY")?;
        keys.push(OrderKey {
            target: order_target(expr)?,
            desc,
            nulls_first,
        });
    }
    Ok(keys)
}

/// The column each key of `keys` sorts on, over a result whose columns are `cols`. A
/// position names a *visible* column (1-based, so a hidden column never shifts it);
/// an expression key takes the slot `placed` records for it, one per
/// [`order_exprs`] entry in that order.
pub(crate) fn key_slots<'c>(
    keys: &[OrderKey<'_>],
    cols: impl IntoIterator<Item = &'c ColumnDef>,
    placed: impl IntoIterator<Item = usize>,
) -> Result<Vec<usize>, GnitzSqlError> {
    let visible: Vec<usize> = cols
        .into_iter()
        .enumerate()
        .filter(|(_, c)| !c.is_hidden)
        .map(|(i, _)| i)
        .collect();
    let mut placed = placed.into_iter();
    keys.iter()
        .map(|k| match k.target {
            OrderTarget::Position(pos) => {
                reject_position_out_of_range(pos, visible.len(), "ORDER BY")?;
                Ok(visible[pos - 1])
            }
            OrderTarget::Expr(_) => placed
                .next()
                .ok_or_else(|| GnitzSqlError::Internal("ORDER BY placements do not match the keys".into())),
        })
        .collect()
}

/// [`key_slots`] as wire keys, each carrying its key's direction.
pub(crate) fn wire_keys<'c>(
    keys: &[OrderKey<'_>],
    cols: impl IntoIterator<Item = &'c ColumnDef>,
    placed: impl IntoIterator<Item = usize>,
) -> Result<Vec<gnitz_wire::OrderKey>, GnitzSqlError> {
    Ok(key_slots(keys, cols, placed)?
        .into_iter()
        .zip(keys)
        .map(|(col, k)| k.wire(col))
        .collect())
}

/// The `LIMIT n` value, or `None` when absent. A non-integer-literal LIMIT
/// (`LIMIT 1+1`, `LIMIT 'x'`) is a clean error — silently degrading it would
/// return every row, violating the unhonored-clause contract. So is the
/// ClickHouse `LIMIT … BY` sub-form, which no surface has an operator for:
/// rejected on the way past, so every caller of this gets it.
pub(crate) fn extract_limit(query: &sqlparser::ast::Query) -> Result<Option<usize>, GnitzSqlError> {
    let limit = match &query.limit_clause {
        // Exhaustive (no `..`): a future `sqlparser` field stops the build.
        Some(LimitClause::LimitOffset { limit, offset: _, limit_by }) => {
            reject_if(!limit_by.is_empty(), "LIMIT", "BY")?;
            match limit {
                Some(e) => e,
                None => return Ok(None),
            }
        }
        Some(LimitClause::OffsetCommaLimit { limit: e, offset: _ }) => e,
        None => return Ok(None),
    };
    expr_usize_literal(limit, "LIMIT").map(Some)
}

/// The `OFFSET n` value (both `LIMIT … OFFSET n` and the MySQL `LIMIT off, lim`
/// form), or `0` when absent. Mirrors [`extract_limit`]: a non-integer-literal
/// value errors rather than silently skipping nothing.
pub(crate) fn extract_offset(query: &sqlparser::ast::Query) -> Result<usize, GnitzSqlError> {
    let offset = match &query.limit_clause {
        Some(LimitClause::LimitOffset { offset: Some(o), limit: _, limit_by: _ }) => &o.value,
        Some(LimitClause::OffsetCommaLimit { offset, limit: _ }) => offset,
        _ => return Ok(0),
    };
    expr_usize_literal(offset, "OFFSET")
}

#[cfg(test)]
#[path = "tests/tail.rs"]
mod tests;
