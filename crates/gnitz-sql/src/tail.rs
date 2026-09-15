//! The query tail — `ORDER BY`, `LIMIT`, `OFFSET` — read off the sqlparser AST
//! into key specs and counts. More than one surface carries a tail and they must
//! agree on it, so it sits below all of them: no surface reaches into another for
//! the rule, and a rejection here cannot name a surface it does not know.

use crate::ast_util::{clause_position, expr_usize_literal, reject_position_out_of_range};
use crate::error::GnitzSqlError;
use gnitz_core::ColumnDef;
use sqlparser::ast::{Expr, LimitClause, OrderBy, OrderByExpr, OrderByKind, OrderByOptions};

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
    asc: bool,
    nulls_first: bool,
}

impl OrderKey<'_> {
    /// Direction and absolute NULL placement, in the sense every consumer of a
    /// resolved key uses: `desc` rather than `asc`. The one place the sense is
    /// flipped, so an ad-hoc sink and a maintained window cannot disagree on it.
    pub(crate) fn dir(&self) -> (bool, bool) {
        (!self.asc, self.nulls_first)
    }

    pub(crate) fn wire(&self, col: usize) -> gnitz_wire::OrderKey {
        let (desc, nulls_first) = self.dir();
        gnitz_wire::OrderKey { col: col as u16, desc, nulls_first }
    }
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

fn unsupported(what: &str) -> GnitzSqlError {
    GnitzSqlError::Unsupported(format!("{what} is not supported"))
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
        return Err(GnitzSqlError::Unsupported(format!(
            "ORDER BY has more than {} keys",
            gnitz_wire::MAX_ORDER_KEYS
        )));
    }
    Ok(keys)
}

/// Parse the whole ORDER BY clause into resolved key specs, rejecting the
/// unsupported ClickHouse/DuckDB extensions as a clean `Unsupported` error.
fn resolve_order_by(ob: &OrderBy) -> Result<Vec<OrderKey<'_>>, GnitzSqlError> {
    // Exhaustive (no `..`) at each of the three levels: a dropped ORDER BY
    // modifier is a wrong result, so a future `sqlparser` field stops the build.
    let OrderBy { kind, interpolate } = ob;
    if interpolate.is_some() {
        return Err(unsupported("ORDER BY ... INTERPOLATE"));
    }
    let exprs = match kind {
        OrderByKind::Expressions(e) => e,
        OrderByKind::All(_) => return Err(unsupported("ORDER BY ALL")),
    };
    let mut keys = Vec::with_capacity(exprs.len());
    for obe in exprs {
        let OrderByExpr { expr, options, with_fill } = obe;
        if with_fill.is_some() {
            return Err(unsupported("ORDER BY ... WITH FILL"));
        }
        let OrderByOptions { asc, nulls_first } = options;
        let asc = asc.unwrap_or(true);
        keys.push(OrderKey {
            target: order_target(expr)?,
            asc,
            // Absolute default: ASC → NULLS LAST, DESC → NULLS FIRST.
            nulls_first: nulls_first.unwrap_or(!asc),
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
            if !limit_by.is_empty() {
                return Err(GnitzSqlError::Unsupported("LIMIT ... BY is not supported".to_string()));
            }
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
