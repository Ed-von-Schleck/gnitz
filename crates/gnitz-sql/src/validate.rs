//! The clause guards more than one statement shares: what a `Select` and a
//! `Query` envelope may carry on each surface, and the CTE and sub-query
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
//! What a *surface* consumes is passed in — `HonoredClauses`, `QueryEnvelope`.

use crate::error::{reject_if, GnitzSqlError};

/// The `Select` clauses a shape legitimately consumes, beyond the universal
/// `from` + `projection` + `WHERE` (every surface binds a top-level WHERE).
/// Passed to [`reject_unhonored_select_clauses`]; every clause not named here
/// (and not honored unconditionally) is rejected.
#[derive(Clone, Copy)]
pub(crate) struct HonoredClauses {
    /// `GROUP BY` and its `HAVING`. Honored only by the grouped-aggregate path.
    grouping: bool,
    /// Plain `DISTINCT`. Honored only by the DISTINCT path (which dedups after
    /// this returns). `DISTINCT ON` is always rejected, independent of this flag.
    distinct: bool,
    /// `QUALIFY` and the `WINDOW` clause. Honored by a view body, whose SELECT
    /// list binds window calls; nowhere else.
    windows: bool,
}

impl HonoredClauses {
    /// A shape consuming neither GROUP BY nor DISTINCT: a subquery body, a
    /// FROM-less SELECT, a plain ungrouped SELECT.
    pub(crate) const PLAIN: HonoredClauses = HonoredClauses {
        grouping: false,
        distinct: false,
        windows: false,
    };

    /// The grouped-vs-DISTINCT split every SELECT-bodied surface makes, in one
    /// home: GROUP BY is honored only on the grouped, **non**-DISTINCT path, so a
    /// `SELECT DISTINCT … GROUP BY` body keeps the pinned "GROUP BY is not
    /// supported" rejection — DISTINCT wins the routing split and its builder does
    /// not group.
    pub(crate) fn for_body(grouped: bool, distinct: bool) -> Self {
        HonoredClauses {
            grouping: grouped && !distinct,
            distinct,
            windows: false,
        }
    }

    /// The view-body surface: the one that binds window calls, so QUALIFY and
    /// the WINDOW clause are honored — an ad-hoc read keeps rejecting both.
    pub(crate) fn with_windows(self) -> Self {
        HonoredClauses { windows: true, ..self }
    }
}

/// Reject any `Select` clause the surface behind it does not consume. Each reads a
/// hand-picked subset of the parsed `Select`; without this guard every unread
/// clause (PREWHERE, TOP, QUALIFY, …) is silently dropped, turning the query the
/// caller wrote into a different one that runs and returns rows — a silent wrong
/// result. `honored` names the clauses *this* shape consumes; `context` names the
/// surface for the message.
///
/// `DISTINCT` is gated by [`HonoredClauses::distinct`]: `DISTINCT ON` is always
/// rejected (non-deterministic without an ORDER BY views forbid), while plain
/// `DISTINCT` is rejected unless the caller honors it.
///
/// The match is an exhaustive destructure with no `..`: when a future
/// `sqlparser` bump adds a `Select` field, this stops compiling until the new
/// field is classified honored-or-rejected — converting a silent-drop-on-upgrade
/// into a build break.
pub(crate) fn reject_unhonored_select_clauses(
    select: &sqlparser::ast::Select,
    honored: HonoredClauses,
    context: &str,
) -> Result<(), GnitzSqlError> {
    use sqlparser::ast::Distinct;
    let sqlparser::ast::Select {
        // Read by every view builder.
        from: _,
        projection: _,
        // Read by every surface — a top-level WHERE always binds.
        selection: _,
        // Conditionally honored (see `HonoredClauses`); `distinct` handled below.
        distinct,
        group_by,
        having,
        // Conditionally honored (see `HonoredClauses::windows`).
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

    reject_if(matches!(distinct, Some(Distinct::On(_))), context, "DISTINCT ON")?;
    reject_if(
        !honored.distinct && matches!(distinct, Some(Distinct::Distinct)),
        context,
        "DISTINCT",
    )?;
    reject_if(
        !honored.grouping && crate::ast_util::group_by_is_present(group_by),
        context,
        "GROUP BY",
    )?;
    reject_if(!honored.grouping && having.is_some(), context, "HAVING")?;
    reject_if(top.is_some(), context, "TOP")?;
    reject_if(prewhere.is_some(), context, "PREWHERE")?;
    reject_if(into.is_some(), context, "SELECT INTO")?;
    reject_if(!connect_by.is_empty(), context, "CONNECT BY")?;
    reject_if(exclude.is_some(), context, "SELECT * EXCLUDE")?;
    reject_if(!lateral_views.is_empty(), context, "LATERAL VIEW")?;
    reject_if(!cluster_by.is_empty(), context, "CLUSTER BY")?;
    reject_if(!distribute_by.is_empty(), context, "DISTRIBUTE BY")?;
    reject_if(!sort_by.is_empty(), context, "SORT BY")?;
    if !honored.windows {
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

/// Reject every `Query`-envelope clause a narrowing site does not consume. `GenericDialect`
/// parses a tail of envelope clauses (FETCH, FOR UPDATE/XML, SETTINGS, FORMAT, …) the planner has
/// no operator for; without this guard each is silently dropped — `WITH` and `FETCH` are wrong
/// results, the rest must not be silently accepted. `honored` names what *this* site consumes;
/// `context` names the surface for the message.
///
/// This is the crate's single exhaustive `Query` destructure (no `..`): a future `sqlparser`
/// field stops the build here until it is classified, converting a silent-drop-on-upgrade into a
/// compile error. Every narrowing site funnels through it.
pub(crate) fn reject_unhonored_query_clauses(
    query: &sqlparser::ast::Query,
    honored: QueryEnvelope,
    context: &str,
) -> Result<(), GnitzSqlError> {
    let whole = matches!(honored, QueryEnvelope::WithAndTail);
    let sqlparser::ast::Query {
        // Dispatched on by the caller (the SELECT / set-op / VALUES / CTE body).
        body: _,
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
    Ok(())
}

/// Reject a sub-`Query`'s whole envelope (no honored clause) and return its body
/// `SetExpr` — the shared front step of every narrowing site that consumes a
/// nested query and accepts any relational body (a CTE body, a derived table).
/// The sites that require a plain SELECT (an EXISTS/IN or scalar subquery inner,
/// the ad-hoc read route's CTE) layer [`as_plain_select`] on top.
pub(crate) fn reject_query_envelope_body<'a>(
    q: &'a sqlparser::ast::Query,
    ctx: &str,
) -> Result<&'a sqlparser::ast::SetExpr, GnitzSqlError> {
    reject_unhonored_query_clauses(q, QueryEnvelope::Bare, ctx)?;
    Ok(q.body.as_ref())
}

/// A relational body narrowed to the plain `Select` a surface requires — one
/// home for that rejection's message, shared by the sub-query and CTE unwraps.
pub(crate) fn as_plain_select<'a>(
    body: &'a sqlparser::ast::SetExpr,
    ctx: &str,
) -> Result<&'a sqlparser::ast::Select, GnitzSqlError> {
    match body {
        sqlparser::ast::SetExpr::Select(s) => Ok(s.as_ref()),
        _ => Err(GnitzSqlError::Rejected(format!(
            "{ctx}: only a plain SELECT body is supported"
        ))),
    }
}

/// The CTE list of a query with the one universal precondition applied —
/// `WITH RECURSIVE` has no builder on any path. An absent `WITH` is the empty
/// list. The shared entry of the two CTE consumers (CREATE VIEW's `bind_ctes`
/// phase and the direct-SELECT read route).
pub(crate) fn non_recursive_ctes(query: &sqlparser::ast::Query) -> Result<&[sqlparser::ast::Cte], GnitzSqlError> {
    let Some(with) = &query.with else {
        return Ok(&[]);
    };
    reject_if(with.recursive, "WITH", "RECURSIVE")?;
    Ok(&with.cte_tables)
}

/// Reject a CTE's envelope (a trailing FROM plus every unhonored `Query` clause)
/// and return its body `SetExpr` — the front step of the CREATE VIEW CTE phase,
/// which binds any relational body (a set operation, a grouped or join body, a
/// nested derived table). The ad-hoc read route, which supports only
/// single-relation CTE bodies, layers [`as_plain_select`] on top.
/// (`materialized` parses only under PostgreSqlDialect — always `None` here.)
pub(crate) fn cte_body<'a>(
    cte: &'a sqlparser::ast::Cte,
    ctx: &str,
) -> Result<&'a sqlparser::ast::SetExpr, GnitzSqlError> {
    let sqlparser::ast::Cte {
        alias: _,
        query,
        from,
        materialized: _,
        closing_paren_token: _,
    } = cte;
    reject_if(from.is_some(), ctx, "a trailing FROM")?;
    reject_query_envelope_body(query, ctx)
}
