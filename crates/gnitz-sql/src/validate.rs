//! DDL/view validation helpers: duplicate-column-name rejection, user index-name
//! rules, float-key rejection, and the auto-generated index-name format. Shared
//! by `ddl` and every view builder.

use crate::error::GnitzSqlError;
use gnitz_core::{ColumnDef, Schema, TypeCode};

/// Reject an output column list that names the same *visible* column twice.
/// Hidden key slots are skipped — they are excluded from name resolution, so
/// they cannot bind ambiguously. `context` names the DDL surface for the error
/// message (e.g. "CREATE VIEW projection", "join view").
pub(crate) fn reject_duplicate_column_names(cols: &[ColumnDef], context: &str) -> Result<(), GnitzSqlError> {
    reject_duplicate_names(cols.iter().filter(|c| !c.is_hidden).map(|c| c.name.as_str()), context)
}

/// Raw-name form of [`reject_duplicate_column_names`], for surfaces that have
/// only parser-AST names (CREATE TABLE — a freshly created column is never
/// hidden; a base table gains a hidden slot only later, via DROP COLUMN).
pub(crate) fn reject_duplicate_names<'a>(
    names: impl Iterator<Item = &'a str>,
    context: &str,
) -> Result<(), GnitzSqlError> {
    let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
    for name in names {
        if !seen.insert(name.to_ascii_lowercase()) {
            return Err(GnitzSqlError::Plan(format!(
                "duplicate column name '{name}' in {context}"
            )));
        }
    }
    Ok(())
}

/// Validate a user-supplied table/view/schema name: reject the empty string,
/// a leading `_` (reserved for the system prefix and for synthesized hidden
/// views, `__h{vid}_{i}`), and any character outside `[A-Za-z0-9_]`. This is the
/// single production enforcement point for these names — the engine's own
/// validators are `#[cfg(test)]`-only — so CREATE TABLE/VIEW and DROP TABLE/VIEW
/// all funnel through it right after `extract_name`.
pub(crate) fn validate_user_name(name: &str) -> Result<(), GnitzSqlError> {
    gnitz_core::validate_user_identifier(name).map_err(GnitzSqlError::Plan)
}

/// Validate a user-supplied name destined to become an index name — a CREATE
/// INDEX name or a UNIQUE/constraint name that maps to a secondary index.
/// Beyond the general identifier rules, the reserved `__fk_` infix is rejected:
/// such a name would collide with internal FK-backing index names and persist as
/// undroppable (`drop_index` refuses it). The infix check is scoped here rather
/// than in `validate_user_identifier`, which also guards table/column/schema
/// names that may legitimately contain `__fk_`.
///
/// The infix is matched against the **canonical (lowercase) form**, because the
/// client canonicalizes index names at store time: a mixed-case `x__FK_y` would
/// otherwise pass this guard yet be stored as the reserved `x__fk_y`, which the
/// engine then refuses to drop.
pub(crate) fn validate_user_index_name(name: &str) -> Result<(), GnitzSqlError> {
    validate_user_name(name)?;
    if name.to_ascii_lowercase().contains(gnitz_core::FK_INDEX_INFIX) {
        return Err(GnitzSqlError::Plan(format!(
            "Index/constraint names cannot contain the reserved '{}' infix",
            gnitz_core::FK_INDEX_INFIX
        )));
    }
    Ok(())
}

/// Catalog name for an auto-generated (unnamed) secondary index:
/// `{schema}__{table}__idx_{col1}_{col2}…` (column names joined with `_`).
/// `DROP INDEX <name>` resolves this exact string, so the format is a stable
/// contract (the drop-by-name tests in `planner_create_table` pin it); this is
/// its single definition, shared by CREATE INDEX and CREATE TABLE … UNIQUE. The
/// output is lowercased so the base is canonical (matching the client's
/// store-time canonicalization), which the collision disambiguation depends on.
pub(crate) fn default_index_name(schema_name: &str, table_name: &str, col_names: &[&str]) -> String {
    format!("{schema_name}__{table_name}__idx_{}", col_names.join("_")).to_ascii_lowercase()
}

/// Return `base` if free, else the first `{base}_{n}` (n ≥ 2) not in `taken` —
/// PostgreSQL's scheme, keeping the readable base for the common non-colliding
/// case. `taken` holds canonical (lowercase) names; `base` is already canonical.
/// Only auto-generated names are routed here — an explicit collision still errors.
pub(crate) fn disambiguate_index_name(base: String, taken: &std::collections::HashSet<String>) -> String {
    if !taken.contains(&base) {
        return base;
    }
    for n in 2u32.. {
        let candidate = format!("{base}_{n}");
        if !taken.contains(&candidate) {
            return candidate;
        }
    }
    unreachable!("u32 range exhausted")
}

/// Reject a float column used as any hashed key — a GROUP BY grouping key, a
/// DISTINCT/set-op row identity, or an equijoin key. All of these hash the
/// column's raw IEEE-754 bytes, so -0.0/+0.0 and distinct-NaN bit patterns split
/// values that are numerically equal (and route them to distinct workers). `role`
/// names the offending clause for the error message.
pub(crate) fn reject_float_key(col: &ColumnDef, role: &str) -> Result<(), GnitzSqlError> {
    if col.type_code.is_float() {
        return Err(GnitzSqlError::Unsupported(format!(
            "{role}: float column '{}' cannot be a key \
             (IEEE-754 -0.0/+0.0 and NaN break key equality)",
            col.name
        )));
    }
    Ok(())
}

/// Reject any float column among `indices` as a row-identity key. See
/// [`reject_float_key`] for why floats break set/DISTINCT membership.
pub(crate) fn reject_float_keys(source_schema: &Schema, indices: &[usize]) -> Result<(), GnitzSqlError> {
    for &ci in indices {
        reject_float_key(&source_schema.columns[ci], "SELECT DISTINCT / set operation")?;
    }
    Ok(())
}

/// Reject a column type that cannot back a hashed key (PRIMARY KEY or UNIQUE
/// index). `role` names the clause for the message. `is_pk_eligible` is the
/// shared allow-list (fixed-width integer, U128, UUID).
pub(crate) fn reject_non_key_eligible(name: &str, tc: TypeCode, role: &str) -> Result<(), GnitzSqlError> {
    if !tc.is_pk_eligible() {
        return Err(non_key_eligible_error(name, tc, role));
    }
    Ok(())
}

/// The named-column rendering of "this type cannot be a key column" — the
/// planner's half of `gnitz_wire::PkRule::NotEligible`, split out so the
/// CREATE TABLE path can raise the identical message from the shared rule's
/// verdict instead of re-testing eligibility itself.
pub(crate) fn non_key_eligible_error(name: &str, tc: TypeCode, role: &str) -> GnitzSqlError {
    GnitzSqlError::Unsupported(format!(
        "{role} column '{name}' of type {tc:?} is not supported \
         ({role} must be a fixed-width integer, U128, or UUID column; \
         String, Blob, and float columns cannot be a {role} key)"
    ))
}

/// The single spelling of an "unhonored clause" rejection. `reject_unhonored_select_clauses`,
/// `reject_unhonored_query_clauses`, and the `Cte`-envelope reject all funnel through this, so the
/// `"{context}: {clause} is not supported"` grammar cannot drift per site.
fn unsupported_clause(context: &str, clause: &str) -> GnitzSqlError {
    GnitzSqlError::Unsupported(format!("{context}: {clause} is not supported"))
}

/// Reject `clause` when `present`. The clause guards below are a *table* of
/// "which clauses does this statement not honor" — written as a run of these so
/// the table reads as one, and so each entry keeps the short-circuit that makes
/// the first present clause the one named.
fn reject_if(present: bool, context: &str, clause: &str) -> Result<(), GnitzSqlError> {
    if present {
        return Err(unsupported_clause(context, clause));
    }
    Ok(())
}

/// Reject a circuit whose widest intermediate batch exceeds the engine's
/// column limit, before the server's hard schema-build assertion. `what` names
/// the view kind and stage ("JOIN view output", "EXISTS view intermediate", …).
pub(crate) fn reject_column_overflow(what: &str, cols: usize) -> Result<(), GnitzSqlError> {
    if cols > gnitz_core::MAX_COLUMNS {
        return Err(GnitzSqlError::Unsupported(format!(
            "{what} has {cols} columns, exceeding the {}-column limit",
            gnitz_core::MAX_COLUMNS
        )));
    }
    Ok(())
}

/// The `Select` clauses a shape legitimately consumes, beyond the universal
/// `from` + `projection` + `WHERE` (every surface binds a top-level WHERE).
/// Passed to [`reject_unhonored_select_clauses`]; every clause not named here
/// (and not honored unconditionally) is rejected.
#[derive(Clone, Copy)]
pub(crate) struct HonoredClauses {
    /// `GROUP BY` and its `HAVING`. Honored only by the grouped-aggregate path.
    pub(crate) grouping: bool,
    /// Plain `DISTINCT`. Honored only by the DISTINCT path (which dedups after
    /// this returns). `DISTINCT ON` is always rejected, independent of this flag.
    pub(crate) distinct: bool,
}

impl HonoredClauses {
    /// A shape consuming neither GROUP BY nor DISTINCT: a subquery body, a CTE
    /// pass-through alias, a plain ungrouped SELECT.
    pub(crate) const PLAIN: HonoredClauses = HonoredClauses {
        grouping: false,
        distinct: false,
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
        }
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
        // Never honored: each carries semantics no view builder implements.
        top,
        into,
        prewhere,
        qualify,
        connect_by,
        lateral_views,
        cluster_by,
        distribute_by,
        sort_by,
        named_window,
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
    reject_if(qualify.is_some(), context, "QUALIFY")?;
    reject_if(!connect_by.is_empty(), context, "CONNECT BY")?;
    reject_if(exclude.is_some(), context, "SELECT * EXCLUDE")?;
    reject_if(!lateral_views.is_empty(), context, "LATERAL VIEW")?;
    reject_if(!cluster_by.is_empty(), context, "CLUSTER BY")?;
    reject_if(!distribute_by.is_empty(), context, "DISTRIBUTE BY")?;
    reject_if(!sort_by.is_empty(), context, "SORT BY")?;
    reject_if(!named_window.is_empty(), context, "WINDOW")?;
    Ok(())
}

/// The `Query`-envelope clauses a narrowing site legitimately consumes, beyond the universal
/// `body`. Every clause not named here is rejected, so a dropped clause is a clean error rather
/// than a silent wrong result.
#[derive(Clone, Copy)]
pub(crate) struct HonoredQueryClauses {
    /// A `WITH` (CTE) clause. Honored by CREATE VIEW (the `bind_ctes` phase) and by
    /// direct SELECT (`cte_passthrough` aliases each pass-through CTE into the
    /// binder cache, so a FROM name a CTE shadows resolves to the CTE's source;
    /// a non-pass-through CTE rejects the whole query as a derivation). A CTE
    /// body (nested CTE) and an INSERT source reject it.
    pub(crate) with: bool,
    /// The site runs the client-side ordering sink: `ORDER BY` and `LIMIT`/`OFFSET` are applied
    /// to the fetched batch. Honored only by direct SELECT, which also rejects the `LIMIT … BY`
    /// sub-form with its own message; when `true` this guard leaves `order_by` and `limit_clause`
    /// to the caller.
    pub(crate) ordering_sink: bool,
}

impl HonoredQueryClauses {
    /// A site that honors no envelope clause at all — every narrowing site
    /// except direct SELECT.
    pub(crate) const NONE: Self = HonoredQueryClauses {
        with: false,
        ordering_sink: false,
    };
}

/// Reject every `Query`-envelope clause a narrowing site does not consume. `GenericDialect`
/// parses a tail of envelope clauses (FETCH, FOR UPDATE/XML, SETTINGS, FORMAT, …) the planner has
/// no operator for; without this guard each is silently dropped — `WITH` and `FETCH` are wrong
/// results, the rest must not be silently accepted. `honored` names what *this* site consumes;
/// `context` names the surface for the message.
///
/// This is the crate's single exhaustive `Query` destructure (no `..`): a future `sqlparser`
/// field stops the build here until it is classified, converting a silent-drop-on-upgrade into a
/// compile error. Every narrowing site funnels through it (direct SELECT and the INSERT source
/// directly, the sub-query sites via [`plain_select_body`]), so no per-site list can drift.
pub(crate) fn reject_unhonored_query_clauses(
    query: &sqlparser::ast::Query,
    honored: HonoredQueryClauses,
    context: &str,
) -> Result<(), GnitzSqlError> {
    let sqlparser::ast::Query {
        // Dispatched on by the caller (the SELECT / set-op / VALUES / CTE body).
        body: _,
        // Conditionally honored (see `HonoredQueryClauses`).
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

    reject_if(!honored.with && with.is_some(), context, "WITH (CTE)")?;
    reject_if(
        !honored.ordering_sink && limit_clause.is_some(),
        context,
        "LIMIT/OFFSET",
    )?;
    reject_if(!honored.ordering_sink && order_by.is_some(), context, "ORDER BY")?;
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
/// `plain_select_body` layers the "must be a plain SELECT" gate on top, for the
/// sites that still require it (an EXISTS/IN or scalar subquery inner).
pub(crate) fn reject_query_envelope_body<'a>(
    q: &'a sqlparser::ast::Query,
    ctx: &str,
) -> Result<&'a sqlparser::ast::SetExpr, GnitzSqlError> {
    reject_unhonored_query_clauses(q, HonoredQueryClauses::NONE, ctx)?;
    Ok(q.body.as_ref())
}

/// Reject a sub-`Query`'s whole envelope (no honored clause) and unwrap its
/// body to the plain `Select` it must be. `ctx` names the surface for both messages.
pub(crate) fn plain_select_body<'a>(
    q: &'a sqlparser::ast::Query,
    ctx: &str,
) -> Result<&'a sqlparser::ast::Select, GnitzSqlError> {
    as_plain_select(reject_query_envelope_body(q, ctx)?, ctx)
}

/// A relational body narrowed to the plain `Select` a surface requires — one
/// home for that rejection's message, shared by the sub-query and CTE unwraps.
fn as_plain_select<'a>(
    body: &'a sqlparser::ast::SetExpr,
    ctx: &str,
) -> Result<&'a sqlparser::ast::Select, GnitzSqlError> {
    match body {
        sqlparser::ast::SetExpr::Select(s) => Ok(s.as_ref()),
        _ => Err(GnitzSqlError::Unsupported(format!(
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
    if with.recursive {
        return Err(GnitzSqlError::Unsupported(
            "recursive CTEs are not supported".to_string(),
        ));
    }
    Ok(&with.cte_tables)
}

/// Unwrap one CTE to its plain `Select` body — the front step of the ad-hoc
/// direct-SELECT read route (the CREATE VIEW CTE phase uses `cte_body`, which
/// accepts any relational body).
/// Exhaustively destructures the `Cte` envelope so a future `sqlparser` field
/// cannot be silently dropped (`materialized` parses only under
/// PostgreSqlDialect — always `None` here), rejects the `Query`-envelope clauses
/// a CTE body cannot honor via [`plain_select_body`], and rejects a ClickHouse
/// trailing `FROM` (neither inliner can honor it).
pub(crate) fn cte_select_body<'a>(
    cte: &'a sqlparser::ast::Cte,
    ctx: &str,
) -> Result<&'a sqlparser::ast::Select, GnitzSqlError> {
    as_plain_select(cte_body(cte, ctx)?, ctx)
}

/// Reject a CTE's envelope (a trailing FROM plus every unhonored `Query` clause)
/// and return its body `SetExpr` — the front step of the CREATE VIEW CTE phase,
/// which binds any relational body (a set operation, a grouped or join body, a
/// nested derived table). `cte_select_body` layers the plain-SELECT gate on top
/// for the ad-hoc read route, which supports only single-relation CTE bodies.
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
    if from.is_some() {
        return Err(unsupported_clause(ctx, "a trailing FROM"));
    }
    reject_query_envelope_body(query, ctx)
}

/// Reject every `Insert`-statement clause the INSERT planner does not consume. `extract_insert_parts`
/// reads only `table`, `source`, `columns`, and `on` (ON CONFLICT, fully resolved there); every other
/// field is a conflict / overwrite / partition / RETURNING clause parsed under `GenericDialect` and
/// silently reinterpreted as a plain append. The `source` is a full `Query` whose envelope (LIMIT,
/// ORDER BY, FETCH, a `WITH`, …) an INSERT equally cannot honor, so it is routed through
/// `reject_unhonored_query_clauses` here too — every INSERT-clause rejection lives in this one guard.
///
/// Exhaustive destructure (no `..`): a future `sqlparser` `Insert` field stops the build here.
pub(crate) fn reject_unhonored_insert_clauses(
    insert: &sqlparser::ast::Insert,
    context: &str,
) -> Result<(), GnitzSqlError> {
    let sqlparser::ast::Insert {
        // Consumed by `extract_insert_parts`; `source`'s `Query` envelope is additionally checked below.
        table: _,
        source,
        columns: _,
        on: _,
        // Inert keyword markers (`INTO` / `TABLE`, the `INSERT` token) and
        // advisory-only comment hints: positional flags, no droppable semantics.
        into: _,
        has_table_keyword: _,
        insert_token: _,
        optimizer_hints: _,
        // Handled downstream in `execute_insert`: INSERT ... RETURNING is supported
        // (projected client-side from the just-built batch).
        returning: _,
        // Rejected: each is a clause the engine does not implement.
        or,
        ignore,
        overwrite,
        partitioned,
        replace_into,
        priority,
        insert_alias,
        table_alias,
        assignments,
        after_columns,
        settings,
        format_clause,
        output,
        multi_table_insert_type,
        multi_table_into_clauses,
        multi_table_when_clauses,
        multi_table_else_clause,
    } = insert;

    reject_if(or.is_some(), context, "OR (conflict clause)")?;
    reject_if(*ignore, context, "IGNORE")?;
    reject_if(*overwrite, context, "OVERWRITE")?;
    reject_if(*replace_into, context, "REPLACE INTO")?;
    reject_if(partitioned.is_some(), context, "PARTITION")?;
    reject_if(
        priority.is_some(),
        context,
        "priority (LOW_PRIORITY/HIGH_PRIORITY/DELAYED)",
    )?;
    reject_if(insert_alias.is_some(), context, "row alias (AS alias)")?;
    reject_if(table_alias.is_some(), context, "table alias")?;
    reject_if(!assignments.is_empty(), context, "SET")?;
    reject_if(!after_columns.is_empty(), context, "AFTER columns")?;
    reject_if(settings.is_some(), context, "SETTINGS")?;
    reject_if(format_clause.is_some(), context, "FORMAT")?;
    reject_if(output.is_some(), context, "OUTPUT")?;
    if multi_table_insert_type.is_some()
        || !multi_table_into_clauses.is_empty()
        || !multi_table_when_clauses.is_empty()
        || multi_table_else_clause.is_some()
    {
        return Err(unsupported_clause(context, "multi-table INSERT (ALL/FIRST)"));
    }
    // The `source` is a full `Query`; an INSERT honors no envelope clause on it (LIMIT/OFFSET,
    // ORDER BY, FETCH, FOR UPDATE/SHARE, SETTINGS, FORMAT, a `WITH`). Route it through the shared
    // `Query` guard so a dropped envelope clause is a clean error, not a silent full-table insert.
    if let Some(src) = source {
        reject_unhonored_query_clauses(src, HonoredQueryClauses::NONE, context)?;
    }
    Ok(())
}

/// Reject every `UPDATE` clause `execute_update` does not consume. It reads `table`, `assignments`,
/// `selection`; `from` (UPDATE … FROM join-update), `returning`, and `or` (SQLite conflict) all parse
/// under `GenericDialect` and were dropped — the join-update silently binds SET/WHERE against the
/// wrong relation set.
pub(crate) fn reject_unhonored_update_clauses(
    update: &sqlparser::ast::Update,
    context: &str,
) -> Result<(), GnitzSqlError> {
    let sqlparser::ast::Update {
        // Consumed by `execute_update`.
        table: _,
        assignments: _,
        selection: _,
        // Inert: the `UPDATE` token and advisory-only comment hints.
        update_token: _,
        optimizer_hints: _,
        // Rejected: each is a clause `execute_update` does not implement.
        from,
        returning,
        output,
        or,
        order_by,
        limit,
    } = update;
    reject_if(from.is_some(), context, "FROM (join-update)")?;
    reject_if(returning.is_some(), context, "RETURNING")?;
    reject_if(output.is_some(), context, "OUTPUT")?;
    reject_if(or.is_some(), context, "OR (conflict clause)")?;
    reject_if(!order_by.is_empty(), context, "ORDER BY")?;
    reject_if(limit.is_some(), context, "LIMIT")?;
    Ok(())
}

/// Reject every `DELETE` clause `execute_delete` does not consume. It reads `from` and `selection`;
/// `tables` (multi-table), `using` (join-delete), `returning`, `order_by`, and `limit` all parse
/// under `GenericDialect` and were dropped — a dropped `LIMIT` deletes every matched row (data loss),
/// a dropped `USING` binds WHERE against the wrong relation set.
pub(crate) fn reject_unhonored_delete_clauses(
    del: &sqlparser::ast::Delete,
    context: &str,
) -> Result<(), GnitzSqlError> {
    let sqlparser::ast::Delete {
        from: _,
        selection: _,
        // Inert: the `DELETE` token and advisory-only comment hints.
        delete_token: _,
        optimizer_hints: _,
        tables,
        using,
        returning,
        output,
        order_by,
        limit,
    } = del;
    reject_if(!tables.is_empty(), context, "multi-table delete")?;
    reject_if(using.is_some(), context, "USING (join-delete)")?;
    reject_if(returning.is_some(), context, "RETURNING")?;
    reject_if(output.is_some(), context, "OUTPUT")?;
    reject_if(!order_by.is_empty(), context, "ORDER BY")?;
    reject_if(limit.is_some(), context, "LIMIT")?;
    Ok(())
}

/// Reject every `DROP` clause `execute_drop` does not consume. It reads `object_type` and `names`;
/// `cascade`/`restrict` (dependent-object policy), `purge` (Hive data deletion), `temporary`
/// (MySQL DROP TEMPORARY), and `table` (MySQL `DROP INDEX i ON t` — the ON target) all parse under
/// `GenericDialect` and would otherwise be silently dropped. `if_exists` drops *loudly* (a missing
/// object still errors), so it is not rejected; implement it later.
///
/// Exhaustive destructure (no `..`): a future `sqlparser` field stops the build.
pub(crate) fn reject_unhonored_drop_clauses(
    stmt: &sqlparser::ast::Statement,
    context: &str,
) -> Result<(), GnitzSqlError> {
    let sqlparser::ast::Statement::Drop {
        object_type: _,
        names: _,
        if_exists: _, // loud on drop; implement later
        cascade,
        restrict,
        purge,
        temporary,
        table,
    } = stmt
    else {
        return Err(GnitzSqlError::Bind("not a DROP statement".to_string()));
    };
    reject_if(*cascade, context, "CASCADE")?;
    reject_if(*restrict, context, "RESTRICT")?;
    reject_if(*purge, context, "PURGE")?;
    reject_if(*temporary, context, "TEMPORARY")?;
    reject_if(table.is_some(), context, "ON <table> (MySQL DROP INDEX target)")?;
    Ok(())
}

/// Reject every `EXPLAIN` option `execute_explain` does not honor. EXPLAIN
/// describes the plan a query *would* take, so it consumes only the statement it
/// wraps: `analyze` runs the query (the one option that changes what EXPLAIN
/// does), and `verbose` / `query_plan` / `estimate` / `format` / `options` each
/// ask for a rendering this output shape does not have. `describe_alias` is inert
/// phrasing — `EXPLAIN`, `DESCRIBE` and `DESC` all introduce the same statement.
///
/// Exhaustive destructure (no `..`): a future `sqlparser` field stops the build
/// until it is classified honored-or-rejected.
pub(crate) fn reject_unhonored_explain_clauses(
    stmt: &sqlparser::ast::Statement,
    context: &str,
) -> Result<(), GnitzSqlError> {
    let sqlparser::ast::Statement::Explain {
        describe_alias: _, // EXPLAIN / DESCRIBE / DESC all introduce the same statement
        analyze,
        verbose,
        query_plan,
        estimate,
        statement: _, // the caller's own pattern supplies it
        format,
        options,
    } = stmt
    else {
        return Err(GnitzSqlError::Bind("not an EXPLAIN statement".to_string()));
    };
    reject_if(*analyze, context, "ANALYZE")?;
    reject_if(*verbose, context, "VERBOSE")?;
    reject_if(*query_plan, context, "QUERY PLAN")?;
    reject_if(*estimate, context, "ESTIMATE")?;
    reject_if(format.is_some(), context, "FORMAT")?;
    // `GenericDialect` sets `supports_explain_with_utility_options`, so the
    // parenthesized Postgres form parses into `options` rather than failing here.
    reject_if(options.is_some(), context, "the parenthesized option list")?;
    Ok(())
}

/// Reject every `START TRANSACTION` / `BEGIN` clause the SQL transaction surface
/// does not honor. `transaction` (the BEGIN/START keyword kind), `begin`, and
/// `has_end_keyword` are inert phrasing. Rejected: `modes` (`READ ONLY` /
/// `ISOLATION LEVEL ...` — gnitz has one fixed isolation: atomicity + constraint
/// consistency), `modifier` (`BEGIN DEFERRED` / `BEGIN TRY`), and `statements` /
/// `exception_statements` (a `BEGIN ... END` procedural block or its exception
/// clause — gnitz has no such block).
///
/// Exhaustive destructure (no `..`): a future `sqlparser` field stops the build
/// until it is classified honored-or-rejected — a silent-drop-on-upgrade becomes
/// a build break.
pub(crate) fn reject_unhonored_start_transaction_clauses(
    stmt: &sqlparser::ast::Statement,
    context: &str,
) -> Result<(), GnitzSqlError> {
    let sqlparser::ast::Statement::StartTransaction {
        modes,
        begin: _,
        transaction: _,
        modifier,
        statements,
        exception,
        has_end_keyword: _,
    } = stmt
    else {
        return Err(GnitzSqlError::Bind("not a START TRANSACTION statement".to_string()));
    };
    reject_if(
        !modes.is_empty(),
        context,
        "transaction modes (READ ONLY / ISOLATION LEVEL)",
    )?;
    reject_if(modifier.is_some(), context, "a BEGIN modifier (DEFERRED / TRY / CATCH)")?;
    reject_if(!statements.is_empty(), context, "a BEGIN ... END block")?;
    reject_if(exception.is_some(), context, "an EXCEPTION clause")?;
    Ok(())
}

/// Reject every `COMMIT` / `END` clause the SQL transaction surface does not
/// honor. `end` is inert (`END` is a `COMMIT` spelling). Rejected: `chain`
/// (`AND CHAIN` — gnitz opens no immediate successor transaction) and `modifier`
/// (`COMMIT TRY` / `CATCH`).
///
/// Exhaustive destructure (no `..`): a future `sqlparser` field stops the build.
pub(crate) fn reject_unhonored_commit_clauses(
    stmt: &sqlparser::ast::Statement,
    context: &str,
) -> Result<(), GnitzSqlError> {
    let sqlparser::ast::Statement::Commit {
        chain,
        end: _,
        modifier,
    } = stmt
    else {
        return Err(GnitzSqlError::Bind("not a COMMIT statement".to_string()));
    };
    reject_if(*chain, context, "AND CHAIN")?;
    reject_if(modifier.is_some(), context, "a COMMIT modifier (TRY / CATCH)")?;
    Ok(())
}

/// Reject every `ROLLBACK` clause the SQL transaction surface does not honor.
/// Rejected: `chain` (`AND CHAIN`) and `savepoint` (`ROLLBACK TO SAVEPOINT x` —
/// gnitz has no savepoints).
///
/// Exhaustive destructure (no `..`): a future `sqlparser` field stops the build.
pub(crate) fn reject_unhonored_rollback_clauses(
    stmt: &sqlparser::ast::Statement,
    context: &str,
) -> Result<(), GnitzSqlError> {
    let sqlparser::ast::Statement::Rollback { chain, savepoint } = stmt else {
        return Err(GnitzSqlError::Bind("not a ROLLBACK statement".to_string()));
    };
    reject_if(*chain, context, "AND CHAIN")?;
    reject_if(savepoint.is_some(), context, "TO SAVEPOINT")?;
    Ok(())
}

/// Reject every `CREATE TABLE` envelope clause `execute_create_table` does not consume. `name`,
/// `columns`, `constraints`, `cluster_by`, `table_options` are consumed (column/constraint contents
/// are further guarded by [`reject_unhonored_column_options`] / [`reject_unhonored_table_constraints`];
/// `table_options` carries the `WITH (replicated = …)` option, parsed by `parse_replicated_option`).
/// Rejected: `query` (CTAS), `temporary`/`global` (silent permanent table), `like`/`clone` (empty
/// table, ignoring the template), `on_commit`, `primary_key` (silently substitutes the PK), and
/// `partition_of`/`for_values` (silently creates a standalone table instead of a partition child). The
/// `_`-bound remainder parses under `GenericDialect` or not, but carries no gnitz-honorable semantics
/// — storage/engine/vendor metadata accepted as no-ops, never changing a result.
/// `or_replace`/`if_not_exists` drop *loudly* (a name collision still errors), so they are not
/// rejected; implement them later.
///
/// Exhaustive destructure (no `..`) over every field: a future `sqlparser` field stops the build.
pub(crate) fn reject_unhonored_create_table_clauses(
    create: &sqlparser::ast::CreateTable,
    context: &str,
) -> Result<(), GnitzSqlError> {
    let sqlparser::ast::CreateTable {
        // Consumed (column/constraint contents further guarded by the column-option and table-constraint guards;
        // `table_options` carries `WITH (replicated = …)`).
        name: _,
        columns: _,
        constraints: _,
        cluster_by: _,
        table_options: _,
        // Loud on drop, not silent; implement later.
        or_replace: _,
        if_not_exists: _,
        // Rejected: each silently changes the result if dropped.
        temporary,
        global,
        query,
        like,
        clone,
        on_commit,
        primary_key,
        partition_of,
        for_values,
        // No gnitz-honorable semantics — storage/engine/vendor metadata accepted as no-ops.
        external: _,
        dynamic: _,
        transient: _,
        volatile: _,
        iceberg: _,
        snapshot: _,
        hive_distribution: _,
        hive_formats: _,
        file_format: _,
        location: _,
        version: _,
        without_rowid: _,
        comment: _,
        on_cluster: _,
        order_by: _,
        partition_by: _,
        clustered_by: _,
        inherits: _,
        strict: _,
        copy_grants: _,
        enable_schema_evolution: _,
        change_tracking: _,
        data_retention_time_in_days: _,
        max_data_extension_time_in_days: _,
        default_ddl_collation: _,
        with_aggregation_policy: _,
        with_row_access_policy: _,
        with_storage_lifecycle_policy: _,
        with_tags: _,
        external_volume: _,
        base_location: _,
        catalog: _,
        catalog_sync: _,
        storage_serialization_policy: _,
        target_lag: _,
        warehouse: _,
        refresh_mode: _,
        initialize: _,
        require_user: _,
        diststyle: _,
        distkey: _,
        sortkey: _,
        backup: _,
    } = create;

    reject_if(query.is_some(), context, "AS SELECT (CTAS)")?;
    reject_if(*temporary, context, "TEMPORARY")?;
    reject_if(global.is_some(), context, "GLOBAL/LOCAL")?;
    reject_if(like.is_some(), context, "LIKE")?;
    reject_if(clone.is_some(), context, "CLONE")?;
    reject_if(on_commit.is_some(), context, "ON COMMIT")?;
    reject_if(primary_key.is_some(), context, "PRIMARY KEY expression")?;
    reject_if(partition_of.is_some() || for_values.is_some(), context, "PARTITION OF")?;
    Ok(())
}

/// The `WITH (…)` options of a `CREATE` statement — the only option form gnitz
/// reads; `None` is the common no-options case. Every other form is rejected rather
/// than accepted as a vendor no-op, because gnitz's `WITH` keys decide what the
/// relation *is*: silently ignoring `OPTIONS(stream = true)` or
/// `OPTIONS(capacity = '4 MB')` would yield an ordinary durable table or an
/// unbounded view with no error anywhere. One spelling for every statement.
pub(crate) fn with_options(
    options: &sqlparser::ast::CreateTableOptions,
) -> Result<&[sqlparser::ast::SqlOption], GnitzSqlError> {
    let form = match options {
        sqlparser::ast::CreateTableOptions::With(opts) => return Ok(opts),
        sqlparser::ast::CreateTableOptions::None => return Ok(&[]),
        sqlparser::ast::CreateTableOptions::Options(_) => "OPTIONS (…)",
        sqlparser::ast::CreateTableOptions::Plain(_) => "space-separated options",
        sqlparser::ast::CreateTableOptions::TableProperties(_) => "TBLPROPERTIES (…)",
    };
    Err(GnitzSqlError::Plan(format!(
        "{form} is not supported; options must be given as WITH (…)"
    )))
}

/// One `WITH (…)` entry as its `key = value` pair, or the shared rejection for
/// every other option form. `context` names the statement.
pub(crate) fn require_kv_option<'a>(
    opt: &'a sqlparser::ast::SqlOption,
    context: &str,
) -> Result<(&'a sqlparser::ast::Ident, &'a sqlparser::ast::Expr), GnitzSqlError> {
    match opt {
        sqlparser::ast::SqlOption::KeyValue { key, value } => Ok((key, value)),
        other => Err(GnitzSqlError::Plan(format!(
            "unsupported {context} option in WITH (…), which takes `key = value` entries: {other:?}"
        ))),
    }
}

/// Reject every `CREATE VIEW` clause `execute_create_view` does not consume
/// (`name`, `query`, and `options` — see `hir::create::decode_capacity`).
/// `materialized` is accepted — a gnitz view is already incrementally materialized. `temporary`
/// (silent permanent view), `to` (silently ignored target), and `columns` (output aliases dropped →
/// wrong view schema) are rejected. `or_alter`/`or_replace`/`if_not_exists` drop loudly; implement
/// later. `with_no_schema_binding` parses but is a no-op optimizer hint; the rest cannot populate
/// under `GenericDialect`. `options` is consumed, not rejected.
pub(crate) fn reject_unhonored_create_view_clauses(
    cv: &sqlparser::ast::CreateView,
    context: &str,
) -> Result<(), GnitzSqlError> {
    let sqlparser::ast::CreateView {
        name: _,
        query: _,
        materialized: _, // accepted: names gnitz's real behavior
        or_alter: _,
        or_replace: _,
        if_not_exists: _,          // loud on drop; implement later
        name_before_not_exists: _, // positional flag for if_not_exists
        with_no_schema_binding: _, // no-op optimizer hint
        secure: _,                 // Snowflake SECURE modifier: no result impact
        copy_grants: _,            // Snowflake COPY GRANTS: no result impact
        options: _,
        cluster_by: _,
        comment: _,
        params: _, // cannot populate under GenericDialect
        columns,
        temporary,
        to, // rejected
    } = cv;
    reject_if(!columns.is_empty(), context, "output column aliases")?;
    reject_if(*temporary, context, "TEMPORARY")?;
    reject_if(to.is_some(), context, "TO (target table)")?;
    Ok(())
}

/// Reject every `CreateIndex` field `execute_create_index` does not consume (`name`, `table_name`,
/// `columns`, `unique`). `using` is accepted only for the BTree default (gnitz's index is ordered /
/// range-scannable); any other type, plus `predicate` (partial index → full index), `concurrently`
/// (no non-blocking-build guarantee), `include`/`nulls_distinct`/`with` (silent default semantics)
/// are rejected. `if_not_exists` drops loudly; implement later.
pub(crate) fn reject_unhonored_create_index_clauses(
    ci: &sqlparser::ast::CreateIndex,
    context: &str,
) -> Result<(), GnitzSqlError> {
    use sqlparser::ast::IndexType;
    let sqlparser::ast::CreateIndex {
        name: _,
        table_name: _,
        columns: _,
        unique: _,
        if_not_exists: _, // loud on drop; implement later
        using,
        concurrently,
        include,
        nulls_distinct,
        with,
        predicate,
        index_options,
        alter_options,
    } = ci;
    reject_if(predicate.is_some(), context, "WHERE (partial index)")?;
    reject_if(!index_options.is_empty(), context, "index options")?;
    reject_if(!alter_options.is_empty(), context, "ALTER options (ALGORITHM / LOCK)")?;
    if let Some(t) = using {
        reject_if(
            !matches!(t, IndexType::BTree),
            context,
            "USING (non-default index type)",
        )?;
    }
    reject_if(*concurrently, context, "CONCURRENTLY")?;
    reject_if(!include.is_empty(), context, "INCLUDE (covering columns)")?;
    reject_if(nulls_distinct.is_some(), context, "NULLS [NOT] DISTINCT")?;
    reject_if(!with.is_empty(), context, "WITH (storage parameters)")?;
    Ok(())
}

/// Reject every FOREIGN KEY field beyond the target (`foreign_table` /
/// `referred_columns` / `columns`) and the inert names: a referential action
/// (`ON DELETE` / `ON UPDATE`), a `MATCH` kind, and constraint characteristics
/// (DEFERRABLE) are all semantics gnitz does not implement. Since sqlparser 0.60
/// the column-level and table-level FK both wrap the same `ForeignKeyConstraint`,
/// so both guards share this one check (exhaustive destructure, no `..`).
fn reject_unhonored_fk_fields(fk: &sqlparser::ast::ForeignKeyConstraint, context: &str) -> Result<(), GnitzSqlError> {
    let sqlparser::ast::ForeignKeyConstraint {
        // Consumed by `resolve_fk_target`.
        columns: _,
        foreign_table: _,
        referred_columns: _,
        // Inert metadata: gnitz has no FK constraint/index naming surface.
        name: _,
        index_name: _,
        // Rejected: unimplemented semantics.
        on_delete,
        on_update,
        match_kind,
        characteristics,
    } = fk;
    if on_delete.is_some() || on_update.is_some() {
        return Err(unsupported_clause(context, "FOREIGN KEY ON DELETE/ON UPDATE action"));
    }
    if match_kind.is_some() {
        return Err(unsupported_clause(context, "FOREIGN KEY MATCH"));
    }
    if characteristics.is_some() {
        return Err(unsupported_clause(context, "constraint characteristics (DEFERRABLE …)"));
    }
    Ok(())
}

/// Reject every PRIMARY KEY constraint field beyond the column list and the
/// inert names. `index_type` follows CREATE INDEX's rule: the BTree default is
/// accepted (it names gnitz's real ordered index), anything else is rejected.
/// Shared by the column-level and table-level guards (both wrap
/// `PrimaryKeyConstraint` since sqlparser 0.60; exhaustive destructure, no `..`).
fn reject_unhonored_pk_fields(pk: &sqlparser::ast::PrimaryKeyConstraint, context: &str) -> Result<(), GnitzSqlError> {
    let sqlparser::ast::PrimaryKeyConstraint {
        // Consumed by `execute_create_table`.
        columns: _,
        // Inert metadata: gnitz has no PK constraint/index naming surface.
        name: _,
        index_name: _,
        // Conditionally accepted / rejected below.
        index_type,
        index_options,
        characteristics,
    } = pk;
    reject_index_constraint_extras(index_type, index_options, characteristics, context)
}

/// Reject every UNIQUE constraint field beyond the column list and the
/// constraint name (which names the created index). `NULLS [NOT] DISTINCT`
/// is rejected like CREATE INDEX rejects it — gnitz defines no NULL-conflict
/// semantics to honor. Shared by the column-level and table-level guards
/// (exhaustive destructure, no `..`).
pub(crate) fn reject_unhonored_unique_fields(
    u: &sqlparser::ast::UniqueConstraint,
    context: &str,
) -> Result<(), GnitzSqlError> {
    let sqlparser::ast::UniqueConstraint {
        // Consumed by `execute_create_table` (the name becomes the index name).
        name: _,
        columns: _,
        // Inert keyword phrasing (`UNIQUE KEY` vs `UNIQUE INDEX`).
        index_type_display: _,
        // Rejected: a separate index name would be undroppable (DROP INDEX
        // resolves the constraint-derived name).
        index_name,
        // Conditionally accepted / rejected below.
        index_type,
        index_options,
        characteristics,
        nulls_distinct,
    } = u;
    if index_name.is_some() {
        return Err(unsupported_clause(
            context,
            "a UNIQUE index name (use CONSTRAINT <name>)",
        ));
    }
    if !matches!(nulls_distinct, sqlparser::ast::NullsDistinctOption::None) {
        return Err(unsupported_clause(context, "NULLS [NOT] DISTINCT"));
    }
    reject_index_constraint_extras(index_type, index_options, characteristics, context)
}

/// The PK/UNIQUE-shared tail: accept only the BTree default `USING`, no index
/// options, no constraint characteristics (DEFERRABLE …).
fn reject_index_constraint_extras(
    index_type: &Option<sqlparser::ast::IndexType>,
    index_options: &[sqlparser::ast::IndexOption],
    characteristics: &Option<sqlparser::ast::ConstraintCharacteristics>,
    context: &str,
) -> Result<(), GnitzSqlError> {
    if let Some(t) = index_type {
        if !matches!(t, sqlparser::ast::IndexType::BTree) {
            return Err(unsupported_clause(context, "USING (non-default index type)"));
        }
    }
    if !index_options.is_empty() {
        return Err(unsupported_clause(context, "index options"));
    }
    if characteristics.is_some() {
        return Err(unsupported_clause(context, "constraint characteristics (DEFERRABLE …)"));
    }
    Ok(())
}

/// Which site is walking a column definition, and so which constraint-bearing
/// options are actually acted on. CREATE TABLE consumes NOT NULL / PRIMARY KEY /
/// UNIQUE / REFERENCES; ADD COLUMN consumes none of them, so it must reject them
/// rather than silently drop them.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum ColumnOptionSite {
    CreateTable,
    AddColumn,
}

/// Reject every column option `site` does not honor. Honored by CREATE TABLE: NULL/NOT NULL
/// (nullability), PRIMARY KEY, UNIQUE, FOREIGN KEY target — the honored constraint variants are
/// descended into (`reject_unhonored_{pk,unique,fk}_fields`) so an unimplemented field inside them
/// (a referential action, DEFERRABLE, NULLS NOT DISTINCT, …) is rejected too. Every
/// constraint/semantic option gnitz lacks (DEFAULT, CHECK, GENERATED, IDENTITY, ON UPDATE, COLLATE,
/// SRID, INVISIBLE, …) is rejected; pure metadata (COMMENT/OPTIONS/POLICY/TAGS) is accepted. Exhaustive
/// over all 23 `ColumnOption` variants (no `_`): a new variant stops the build.
pub(crate) fn reject_unhonored_column_options(
    col: &sqlparser::ast::ColumnDef,
    context: &str,
    site: ColumnOptionSite,
) -> Result<(), GnitzSqlError> {
    use sqlparser::ast::ColumnOption as O;
    // Consumed at CREATE TABLE, unhonored at ADD COLUMN.
    let honored = |clause: &str| match site {
        ColumnOptionSite::CreateTable => Ok(()),
        ColumnOptionSite::AddColumn => Err(unsupported_clause(context, clause)),
    };
    for opt in &col.options {
        match &opt.option {
            // A new column over existing rows is nullable either way, so bare
            // NULL is consumed at both sites.
            O::Null => {}
            O::NotNull => honored("NOT NULL (a new column over existing rows is nullable)")?,
            // Consumed, but only the column list / constraint name — descend into
            // the wrapped constraint so an unimplemented field is rejected, not
            // silently dropped.
            O::Unique(u) => {
                honored("UNIQUE (add the column, then CREATE UNIQUE INDEX)")?;
                reject_unhonored_unique_fields(u, context)?
            }
            O::PrimaryKey(pk) => {
                honored("PRIMARY KEY (a new column cannot join the primary key)")?;
                reject_unhonored_pk_fields(pk, context)?
            }
            // Target consumed; the rest rejected.
            O::ForeignKey(fk) => {
                honored("REFERENCES (add the column, then ALTER TABLE … ADD CONSTRAINT)")?;
                reject_unhonored_fk_fields(fk, context)?
            }
            O::Comment(_) | O::Options(_) | O::Policy(_) | O::Tags(_) => {} // inert metadata
            O::Default(_) => return Err(unsupported_clause(context, "DEFAULT")),
            O::Check(_) => return Err(unsupported_clause(context, "CHECK")),
            O::Generated { .. } => return Err(unsupported_clause(context, "GENERATED")),
            O::Identity(_) => return Err(unsupported_clause(context, "IDENTITY / AUTO_INCREMENT")),
            O::OnUpdate(_) => return Err(unsupported_clause(context, "ON UPDATE")),
            O::OnConflict(_) => return Err(unsupported_clause(context, "ON CONFLICT")),
            O::Collation(_) => return Err(unsupported_clause(context, "COLLATE")),
            O::CharacterSet(_) => return Err(unsupported_clause(context, "CHARACTER SET")),
            O::Materialized(_) => return Err(unsupported_clause(context, "MATERIALIZED column")),
            O::Ephemeral(_) => return Err(unsupported_clause(context, "EPHEMERAL column")),
            O::Alias(_) => return Err(unsupported_clause(context, "ALIAS column")),
            O::Srid(_) => return Err(unsupported_clause(context, "SRID")),
            O::Invisible => return Err(unsupported_clause(context, "INVISIBLE column")),
            O::DialectSpecific(_) => return Err(unsupported_clause(context, "dialect-specific column option")),
        }
    }
    Ok(())
}

/// Reject every table constraint `execute_create_table` does not honor. Honored: PRIMARY KEY, UNIQUE,
/// FOREIGN KEY target — each honored variant is descended into
/// (`reject_unhonored_{pk,unique,fk}_fields`) so an unimplemented field inside it (a referential
/// action, DEFERRABLE, NULLS NOT DISTINCT, …) is rejected too. `CHECK`, inline `INDEX`,
/// `FULLTEXT`/`SPATIAL` indexes, and the Postgres `{PRIMARY KEY,UNIQUE} USING INDEX` promotions (no
/// pre-existing index at CREATE TABLE) are rejected. Exhaustive over all 8 `TableConstraint`
/// variants (no `_`).
pub(crate) fn reject_unhonored_table_constraints(
    constraints: &[sqlparser::ast::TableConstraint],
    context: &str,
) -> Result<(), GnitzSqlError> {
    use sqlparser::ast::TableConstraint as C;
    for c in constraints {
        match c {
            // Consumed, but only the column list / constraint name — descend so an
            // unimplemented field is rejected, not silently dropped.
            C::PrimaryKey(pk) => reject_unhonored_pk_fields(pk, context)?,
            C::Unique(u) => reject_unhonored_unique_fields(u, context)?,
            C::ForeignKey(fk) => reject_unhonored_fk_fields(fk, context)?,
            C::Check(_) => return Err(unsupported_clause(context, "CHECK constraint")),
            C::Index(_) => return Err(unsupported_clause(context, "INDEX in table definition")),
            C::FulltextOrSpatial(_) => return Err(unsupported_clause(context, "FULLTEXT/SPATIAL index")),
            C::PrimaryKeyUsingIndex(_) => return Err(unsupported_clause(context, "PRIMARY KEY USING INDEX")),
            C::UniqueUsingIndex(_) => return Err(unsupported_clause(context, "UNIQUE USING INDEX")),
        }
    }
    Ok(())
}

/// Reject every `ALTER TABLE` envelope clause gnitz does not honor: `ONLY`
/// (silently scopes out partition children), a Hive `SET LOCATION`, `ON CLUSTER`,
/// and a non-`None` `table_type` (Iceberg/Dynamic — a different storage engine).
/// The single operation (comma-count enforced by the caller) is dispatched in
/// `plan/alter.rs`. Exhaustive destructure (no `..`): a future sqlparser field
/// stops the build until it is classified.
pub(crate) fn reject_unhonored_alter_table_clauses(
    alter: &sqlparser::ast::AlterTable,
    context: &str,
) -> Result<(), GnitzSqlError> {
    let sqlparser::ast::AlterTable {
        // Consumed by the dispatcher.
        name: _,
        operations: _,
        if_exists: _,
        // Inert: the statement-terminator token.
        end_token: _,
        // Rejected.
        only,
        location,
        on_cluster,
        table_type,
    } = alter;
    reject_if(*only, context, "ONLY")?;
    reject_if(location.is_some(), context, "SET LOCATION")?;
    reject_if(on_cluster.is_some(), context, "ON CLUSTER")?;
    reject_if(
        table_type.is_some(),
        context,
        "a non-default table type (Iceberg/Dynamic/External)",
    )?;
    Ok(())
}

/// Reject an `ALTER VIEW … AS` with output column aliases (`ALTER VIEW v (a,b) AS`)
/// or `WITH` options — gnitz derives the view's output schema from the query, so
/// either would silently produce a different view. sqlparser's `AlterView` has no
/// `if_exists`, so `ALTER VIEW IF EXISTS` never parses.
pub(crate) fn reject_unhonored_alter_view_clauses(
    columns: &[sqlparser::ast::Ident],
    with_options: &[sqlparser::ast::SqlOption],
    context: &str,
) -> Result<(), GnitzSqlError> {
    reject_if(!columns.is_empty(), context, "output column aliases")?;
    reject_if(!with_options.is_empty(), context, "WITH options")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_user_name_rejects_reserved_and_malformed() {
        // Leading `_` is reserved (system prefix + synthesized `__h…` views).
        assert!(matches!(validate_user_name("_hidden"), Err(GnitzSqlError::Plan(_))));
        assert!(matches!(validate_user_name("__h5_0"), Err(GnitzSqlError::Plan(_))));
        // Empty and illegal characters.
        assert!(matches!(validate_user_name(""), Err(GnitzSqlError::Plan(_))));
        assert!(matches!(validate_user_name("bad-name"), Err(GnitzSqlError::Plan(_))));
        assert!(matches!(validate_user_name("a.b"), Err(GnitzSqlError::Plan(_))));
        // Ordinary names — including an internal `_` — are accepted.
        assert!(validate_user_name("orders").is_ok());
        assert!(validate_user_name("my_view2").is_ok());
    }

    fn first_stmt(sql: &str) -> sqlparser::ast::Statement {
        use sqlparser::dialect::GenericDialect;
        use sqlparser::parser::Parser;
        Parser::parse_sql(&GenericDialect {}, sql)
            .unwrap()
            .into_iter()
            .next()
            .unwrap()
    }

    #[test]
    fn transaction_control_rejected_clause_matrix() {
        // BEGIN / START TRANSACTION: bare forms are honored; transaction modes,
        // a BEGIN modifier, and a BEGIN..END block are rejected.
        let begin_ok = ["BEGIN", "START TRANSACTION"];
        for sql in begin_ok {
            assert!(
                reject_unhonored_start_transaction_clauses(&first_stmt(sql), "BEGIN").is_ok(),
                "{sql} should be honored"
            );
        }
        let begin_bad = [
            "BEGIN READ ONLY",
            "START TRANSACTION ISOLATION LEVEL SERIALIZABLE",
            "BEGIN DEFERRED",
        ];
        for sql in begin_bad {
            assert!(
                reject_unhonored_start_transaction_clauses(&first_stmt(sql), "BEGIN").is_err(),
                "{sql} should be rejected"
            );
        }

        // COMMIT / END: bare forms honored; AND CHAIN rejected.
        assert!(reject_unhonored_commit_clauses(&first_stmt("COMMIT"), "COMMIT").is_ok());
        assert!(reject_unhonored_commit_clauses(&first_stmt("END"), "COMMIT").is_ok());
        assert!(reject_unhonored_commit_clauses(&first_stmt("COMMIT AND CHAIN"), "COMMIT").is_err());

        // ROLLBACK: bare honored; AND CHAIN and TO SAVEPOINT rejected.
        assert!(reject_unhonored_rollback_clauses(&first_stmt("ROLLBACK"), "ROLLBACK").is_ok());
        assert!(reject_unhonored_rollback_clauses(&first_stmt("ROLLBACK AND CHAIN"), "ROLLBACK").is_err());
        assert!(reject_unhonored_rollback_clauses(&first_stmt("ROLLBACK TO SAVEPOINT sp"), "ROLLBACK").is_err());
    }
}
