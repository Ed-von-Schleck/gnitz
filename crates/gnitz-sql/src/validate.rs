//! The clause guards more than one statement shares: what a `Select` and a
//! `Query` envelope may carry on each surface, and the sub-query
//! unwraps over them. A guard for one statement sits beside that statement's
//! planner; all of them keep one contract.
//!
//! **The guard contract**: destructure the node without `..`, classifying every field
//! consumed / inert / rejected, so an upstream field addition is E0027 rather
//! than a clause silently dropped; then a table of `reject_if` calls
//! (`error::unsupported_clause`), naming the first present clause.
//!
//! **A node is destructured exhaustively where its fields are read.** A guard
//! function therefore takes the node's payload, never the enclosing
//! `Statement`; a variant sqlparser gives no payload struct is destructured in
//! the arm that already selected it. Matches that only route keep `..`.
//!
//! What a *surface* consumes is passed in — `SelectSite`, `QueryEnvelope`.

use crate::error::{GnitzSqlError, reject_if};

/// The surface a `Select` is bound on, which decides the clauses it consumes
/// beyond the universal `from` + `projection` + `WHERE` (every surface binds a
/// top-level WHERE). Passed to [`reject_unhonored_select_clauses`].
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum SelectSite {
    /// A subquery body: no DISTINCT, GROUP BY, HAVING, QUALIFY or WINDOW.
    Subquery,
    /// An ad-hoc read's body: DISTINCT, or GROUP BY with HAVING.
    Read,
    /// A view body: a read's clauses plus QUALIFY and the WINDOW clause, since
    /// its SELECT list binds window calls.
    View,
}

/// Reject any `Select` clause `site` does not consume; `context` names the
/// surface for the message.
///
/// `DISTINCT ON` is always rejected (non-deterministic without an ORDER BY
/// views forbid). GROUP BY and HAVING are rejected on a DISTINCT body: DISTINCT
/// wins the routing split and its builder does not group, so `SELECT DISTINCT …
/// GROUP BY` reports "GROUP BY is not supported".
pub(crate) fn reject_unhonored_select_clauses(
    select: &sqlparser::ast::Select,
    site: SelectSite,
    context: &str,
) -> Result<(), GnitzSqlError> {
    use sqlparser::ast::Distinct;
    let sqlparser::ast::Select {
        // Read by every surface; a top-level WHERE always binds.
        from: _,
        projection: _,
        selection: _,
        // Conditionally honored, by `site`.
        distinct,
        group_by,
        having,
        qualify,
        named_window,
        // Never honored: each carries semantics no view builder implements.
        top,
        into,
        prewhere,
        connect_by,
        lateral_views,
        cluster_by,
        distribute_by,
        sort_by,
        // `SELECT * EXCLUDE (c)` (Redshift/DuckDB) drops columns from the
        // wildcard: silently dropping it would widen the view's output schema.
        exclude,
        // Inert: parser tokens, positional flags for an already-checked clause
        // (`top`/`qualify`/`distinct`), a clause the GenericDialect never
        // produces (`value_table_mode` is BigQuery-only), or advisory-only
        // annotations (`optimizer_hints` are comment hints; `select_modifiers`
        // are MySQL perf/caching flags like STRAIGHT_JOIN / SQL_NO_CACHE). None
        // change the result set.
        select_token: _,
        top_before_distinct: _,
        window_before_qualify: _,
        value_table_mode: _,
        flavor: _,
        optimizer_hints: _,
        select_modifiers: _,
    } = select;

    let plain_distinct = crate::ast_util::select_is_distinct(select);
    let grouping = site != SelectSite::Subquery && !plain_distinct;
    reject_if(matches!(distinct, Some(Distinct::On(_))), context, "DISTINCT ON")?;
    reject_if(site == SelectSite::Subquery && plain_distinct, context, "DISTINCT")?;
    reject_if(
        !grouping && crate::ast_util::group_by_is_present(group_by),
        context,
        "GROUP BY",
    )?;
    reject_if(!grouping && having.is_some(), context, "HAVING")?;
    reject_if(top.is_some(), context, "TOP")?;
    reject_if(prewhere.is_some(), context, "PREWHERE")?;
    reject_if(into.is_some(), context, "SELECT INTO")?;
    reject_if(!connect_by.is_empty(), context, "CONNECT BY")?;
    reject_if(exclude.is_some(), context, "SELECT * EXCLUDE")?;
    reject_if(!lateral_views.is_empty(), context, "LATERAL VIEW")?;
    reject_if(!cluster_by.is_empty(), context, "CLUSTER BY")?;
    reject_if(!distribute_by.is_empty(), context, "DISTRIBUTE BY")?;
    reject_if(!sort_by.is_empty(), context, "SORT BY")?;
    if site != SelectSite::View {
        reject_if(qualify.is_some(), context, "QUALIFY")?;
        reject_if(!named_window.is_empty(), context, "WINDOW")?;
    }
    Ok(())
}

/// Which `Query`-envelope clauses a narrowing site consumes, beyond the
/// universal `body`. Every clause the variant does not name is rejected, so a
/// dropped one is a clean error rather than a silent wrong result.
#[derive(Clone, Copy)]
pub(crate) enum QueryEnvelope {
    /// No envelope clause at all: a CTE body (nested CTE), a derived table, a
    /// sub-query inner, an INSERT source, a parenthesized set-op side.
    Bare,
    /// A whole query — a view body or a direct SELECT: its `WITH` and its
    /// `ORDER BY` / `LIMIT` / `OFFSET` are left to the caller.
    WithAndTail,
}

/// Reject every `Query`-envelope clause a narrowing site does not consume, and
/// return the query's body. Every narrowing site funnels through it. `honored`
/// names what *this* site consumes; `context` names the surface for the message.
/// An EXISTS/IN or scalar subquery inner, which must be a plain SELECT, layers
/// [`as_plain_select`] on the body.
pub(crate) fn reject_unhonored_query_clauses<'a>(
    query: &'a sqlparser::ast::Query,
    honored: QueryEnvelope,
    context: &str,
) -> Result<&'a sqlparser::ast::SetExpr, GnitzSqlError> {
    let whole = matches!(honored, QueryEnvelope::WithAndTail);
    let sqlparser::ast::Query {
        // Returned for the caller to dispatch on (the SELECT / set-op / VALUES /
        // CTE body).
        body,
        // Conditionally honored (see `QueryEnvelope`).
        with,
        limit_clause,
        // Never honored by any narrowing site.
        order_by,
        fetch,
        locks,
        for_clause,
        settings,
        format_clause,
        pipe_operators,
    } = query;

    reject_if(!whole && with.is_some(), context, "WITH (CTE)")?;
    reject_if(!whole && limit_clause.is_some(), context, "LIMIT/OFFSET")?;
    reject_if(!whole && order_by.is_some(), context, "ORDER BY")?;
    reject_if(fetch.is_some(), context, "FETCH")?;
    reject_if(!locks.is_empty(), context, "FOR UPDATE/SHARE")?;
    reject_if(for_clause.is_some(), context, "FOR XML/JSON/BROWSE")?;
    reject_if(settings.is_some(), context, "SETTINGS")?;
    reject_if(format_clause.is_some(), context, "FORMAT")?;
    reject_if(!pipe_operators.is_empty(), context, "pipe operators (|>)")?;
    Ok(body.as_ref())
}

/// A relational body narrowed to the plain `Select` a surface requires — one
/// home for that rejection's message, shared by the direct SELECT and the
/// sub-query unwrap.
pub(crate) fn as_plain_select<'a>(
    body: &'a sqlparser::ast::SetExpr,
    ctx: &str,
) -> Result<&'a sqlparser::ast::Select, GnitzSqlError> {
    match body {
        sqlparser::ast::SetExpr::Select(s) => Ok(s.as_ref()),
        _ => Err(GnitzSqlError::rejected(format!(
            "{ctx}: only a plain SELECT body is supported"
        ))),
    }
}
