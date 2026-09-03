//! The planner's message layer over `gnitz-wire`'s rules — wire decides, these
//! name the offending column — plus the shared projection-schema rules and the
//! clause guards.
//!
//! **The guard contract**, which `ast_util::reject_unsupported_fn_qualifiers`
//! also carries: destructure the node without `..`, classifying every field
//! consumed / inert / rejected, so an upstream field addition is E0027 rather
//! than a clause silently dropped; then a table of `reject_if` calls
//! (`error::unsupported_clause`), naming the first present clause.
//!
//! A guard takes a variant's payload struct where sqlparser gives it one
//! (`Statement::CreateTable(CreateTable)`) and `&Statement` otherwise. In the
//! `&Statement` case it **returns what the caller consumes** (`drop_parts`,
//! `alter_view_parts`), which is what stops a second, extracting `..` match
//! elsewhere from dropping the next upstream field — a hole the guard exists to
//! close. Matches that only route keep `..`. The exception is `Explain`, whose
//! `statement` is unwrapped in `plan_read` on a path shared with a bare
//! `Statement::Query`, so that match stays regardless.
//!
//! What a *surface* consumes is passed in — `HonoredClauses`,
//! `HonoredQueryClauses`.

use crate::error::{reject_if, unsupported_clause, GnitzSqlError};
use gnitz_core::{ColumnDef, TypeCode};

/// The column def of a *computed* projection item, from the expression's
/// nominal type. One home for the three rules every computed column obeys, so
/// the ad-hoc and CREATE VIEW binders (which each build their own projection
/// schema) cannot drift:
/// - `_expr{idx}` when the item has no alias;
/// - always nullable — an expression over a NOT NULL column can still be NULL
///   (division by zero, an unmatched CASE);
/// - typed by the register image the engine's register sink stores whole, not
///   the nominal type: `-f32col` computes in f64, so declaring the column `F32`
///   would ship the low half of the double. STRING maps to itself, which
///   `register_image` already accounts for.
pub(crate) fn computed_column(alias: Option<String>, idx: usize, nominal: TypeCode) -> ColumnDef {
    ColumnDef::new(
        alias.unwrap_or_else(|| format!("_expr{idx}")),
        nominal.register_image(),
        true,
    )
}

/// Reject an output column list that names the same *visible* column twice.
/// Hidden key slots are skipped — they are excluded from name resolution, so
/// they cannot bind ambiguously. `context` names the DDL surface for the error
/// message (e.g. "CREATE VIEW projection", "join view").
pub(crate) fn reject_duplicate_column_names<'a>(
    cols: impl Iterator<Item = &'a ColumnDef>,
    context: &str,
) -> Result<(), GnitzSqlError> {
    reject_duplicate_names(cols.filter(|c| !c.is_hidden).map(|c| c.name.as_str()), context)
}

/// [`reject_duplicate_column_names`] over what a SELECT projection produces. A
/// projection naming nothing of its own (`*`, `* EXCEPT/EXCLUDE`) is exempt: a
/// duplicate among the source's own names rides through positionally.
pub(crate) fn reject_duplicate_projection_names<'a>(
    projection: &[sqlparser::ast::SelectItem],
    cols: impl Iterator<Item = &'a ColumnDef>,
    context: &str,
) -> Result<(), GnitzSqlError> {
    if crate::ast_util::is_name_preserving_wildcard_projection(projection) {
        return Ok(());
    }
    reject_duplicate_column_names(cols, context)
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

/// Validate a user-supplied table/view/schema/index/constraint name: reject the
/// empty string, a leading `_` (reserved for the engine's own internal relation
/// and index names), and any character outside `[A-Za-z0-9_]`. CREATE
/// TABLE/VIEW and DROP TABLE/VIEW all funnel through it right after
/// `extract_name`, as do CREATE/DROP INDEX and the UNIQUE constraint names that
/// become index names.
///
/// This is *policy*: the leading-`_` reservation is what makes the engine's own
/// internal names (`gnitz_core::segment_name`, `make_fk_index_name`)
/// unspellable here, and the engine cannot enforce it — it must accept exactly
/// those rows.
pub(crate) fn validate_user_name(name: &str) -> Result<(), GnitzSqlError> {
    gnitz_core::validate_user_identifier(name).map_err(GnitzSqlError::Plan)
}

/// [`validate_user_name`] returning the canonical stored form. The one fold a
/// user-supplied name gets in this crate; the catalog gateway applies the same
/// one, so the two cannot spell canonicalization differently.
pub(crate) fn canonical_user_name(name: &str) -> Result<String, GnitzSqlError> {
    gnitz_wire::canonical_identifier(name).map_err(GnitzSqlError::Plan)
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

/// [`reject_float_key`] for a key with no column name to report — a computed one.
pub(crate) fn reject_float_key_of(what: &str, role: &str) -> GnitzSqlError {
    GnitzSqlError::Unsupported(format!(
        "{role}: {what} cannot be a key (IEEE-754 -0.0/+0.0 and NaN break key equality)"
    ))
}

/// Reject a float column used as any hashed key — a GROUP BY grouping key, a
/// DISTINCT/set-op row identity, or an equijoin key. All of these hash the
/// column's raw IEEE-754 bytes, so -0.0/+0.0 and distinct-NaN bit patterns split
/// values that are numerically equal (and route them to distinct workers). `role`
/// names the offending clause for the error message.
pub(crate) fn reject_float_key(col: &ColumnDef, role: &str) -> Result<(), GnitzSqlError> {
    if col.type_code.is_float() {
        return Err(reject_float_key_of(&format!("float column '{}'", col.name), role));
    }
    Ok(())
}

/// [`reject_float_key`] over every column of one hashed row identity — a
/// DISTINCT/set-op row, a join-key pair — so a caller assembling the key from
/// several columns has one call, not a loop.
pub(crate) fn reject_float_keys<'a>(
    cols: impl IntoIterator<Item = &'a ColumnDef>,
    role: &str,
) -> Result<(), GnitzSqlError> {
    cols.into_iter().try_for_each(|c| reject_float_key(c, role))
}

/// Reject an index key the engine could not build — the whole gate both
/// index-creating surfaces (CREATE INDEX and inline UNIQUE) run. Per-column
/// eligibility first, so an ineligible column is named rather than reaching
/// `index_key_types` (the engine's own rule, over the indexed columns *plus the
/// source PK*), whose verdict is a bare type code.
pub(crate) fn reject_unbuildable_index_key(
    names: &[&str],
    types: &[TypeCode],
    src_pk_count: usize,
    src_pk_stride: usize,
    role: &str,
) -> Result<(), GnitzSqlError> {
    for (name, &tc) in names.iter().zip(types) {
        if gnitz_wire::index_key_type(tc as u8).is_err() {
            return Err(non_key_eligible_error(name, tc, role));
        }
    }
    let raw: Vec<u8> = types.iter().map(|&tc| tc as u8).collect();
    gnitz_wire::index_key_types(&raw, src_pk_count, src_pk_stride).map_err(GnitzSqlError::Unsupported)?;
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

/// Reject a circuit whose widest intermediate batch exceeds the engine's
/// column limit, before the server's hard schema-build assertion. `what` names
/// the view kind and stage ("EXISTS view intermediate", …); a segment's output
/// width is checked by its schema build instead.
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

    /// A view body, shared by CREATE VIEW and `ALTER VIEW … AS`: `WITH` is
    /// compiled by the CTE phase, and no other tail clause has incremental-view
    /// semantics.
    pub(crate) const VIEW_BODY: Self = HonoredQueryClauses {
        with: true,
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
/// directly, the sub-query sites via [`plain_select_body`], the parenthesized set-op side in
/// `hir::bind::bind_body` via [`reject_query_envelope_body`]), so no per-site list can drift.
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
    reject_if(from.is_some(), ctx, "a trailing FROM")?;
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
    reject_if(
        multi_table_insert_type.is_some()
            || !multi_table_into_clauses.is_empty()
            || !multi_table_when_clauses.is_empty()
            || multi_table_else_clause.is_some(),
        context,
        "multi-table INSERT (ALL/FIRST)",
    )?;
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

/// The [`DropParts`] `execute_drop` acts on, once every `DROP` clause it
/// does not consume is rejected: `cascade`/`restrict` (dependent-object policy),
/// `purge` (Hive data deletion), `temporary` (MySQL DROP TEMPORARY), and `table`
/// (MySQL `DROP INDEX i ON t` — the ON target) all parse under `GenericDialect`
/// and would otherwise be silently dropped. `if_exists` is returned with them.
///
/// The crate's only `Drop` destructure (no `..`): returning the consumed fields is
/// what keeps it the only one, so a future `sqlparser` field cannot be dropped by
/// an extracting match elsewhere.
pub(crate) fn drop_parts<'a>(
    stmt: &'a sqlparser::ast::Statement,
    context: &str,
) -> Result<DropParts<'a>, GnitzSqlError> {
    let sqlparser::ast::Statement::Drop {
        object_type,
        names,
        if_exists,
        cascade,
        restrict,
        purge,
        temporary,
        table,
    } = stmt
    else {
        return Err(GnitzSqlError::Internal("not a DROP statement".to_string()));
    };
    reject_if(*cascade, context, "CASCADE")?;
    reject_if(*restrict, context, "RESTRICT")?;
    reject_if(*purge, context, "PURGE")?;
    reject_if(*temporary, context, "TEMPORARY")?;
    reject_if(table.is_some(), context, "ON <table> (MySQL DROP INDEX target)")?;
    Ok(DropParts {
        object_type,
        names,
        if_exists: *if_exists,
    })
}

/// What [`drop_parts`] hands `execute_drop`: the object kind, the names, and
/// whether a missing one is an error or a no-op.
pub(crate) struct DropParts<'a> {
    pub(crate) object_type: &'a sqlparser::ast::ObjectType,
    pub(crate) names: &'a [sqlparser::ast::ObjectName],
    pub(crate) if_exists: bool,
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
        return Err(GnitzSqlError::Internal("not an EXPLAIN statement".to_string()));
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
        return Err(GnitzSqlError::Internal("not a START TRANSACTION statement".to_string()));
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
        return Err(GnitzSqlError::Internal("not a COMMIT statement".to_string()));
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
        return Err(GnitzSqlError::Internal("not a ROLLBACK statement".to_string()));
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
/// `if_not_exists` is consumed (the dispatcher's skip route); `or_replace` is rejected — no dialect
/// gnitz targets defines `CREATE OR REPLACE TABLE`, and the plain reading of it, dropping a table and
/// its rows to install a new shape, is what `DROP TABLE` already spells out loud.
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
        // Consumed: the dispatcher's skip route.
        if_not_exists: _,
        // Rejected: each silently changes the result if dropped.
        or_replace,
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
    reject_if(*or_replace, context, "OR REPLACE (it would discard the table's rows)")?;
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
/// (`name`, `query`, `options` — see `hir::create::decode_view_options` — plus
/// `columns` as positional output aliases and `or_replace`/`if_not_exists` as the
/// dispatcher's create route). `materialized` is accepted — a gnitz view is already
/// incrementally materialized. `temporary` (silent permanent view) and `to`
/// (silently ignored target) are rejected, as is `or_alter`: T-SQL's `CREATE OR
/// ALTER` is `OR REPLACE` under another name. `with_no_schema_binding` parses but is
/// a no-op optimizer hint; the rest cannot populate under `GenericDialect`.
pub(crate) fn reject_unhonored_create_view_clauses(
    cv: &sqlparser::ast::CreateView,
    context: &str,
) -> Result<(), GnitzSqlError> {
    let sqlparser::ast::CreateView {
        name: _,
        query: _,
        materialized: _, // accepted: names gnitz's real behavior
        // Consumed here (they are mutually exclusive) and by the dispatcher's
        // replace / skip routes.
        or_replace,
        if_not_exists,
        name_before_not_exists: _, // positional flag for if_not_exists
        with_no_schema_binding: _, // no-op optimizer hint
        secure: _,                 // Snowflake SECURE modifier: no result impact
        copy_grants: _,            // Snowflake COPY GRANTS: no result impact
        options: _,
        columns, // consumed: positional output aliases, checked for decorations below
        cluster_by: _,
        comment: _,
        params: _, // cannot populate under GenericDialect
        or_alter,
        temporary,
        to, // rejected
    } = cv;
    reject_if(*or_alter, context, "OR ALTER (spell it OR REPLACE)")?;
    reject_if(*temporary, context, "TEMPORARY")?;
    reject_if(to.is_some(), context, "TO (target table)")?;
    reject_view_column_alias_decorations(columns, context)?;
    // Not `reject_if`: that template names ONE clause a statement does not honor,
    // and each of these is honored alone. `sqlparser` parses the pair; no dialect
    // defines it.
    if *or_replace && *if_not_exists {
        return Err(GnitzSqlError::Unsupported(format!(
            "{context}: OR REPLACE and IF NOT EXISTS ask for opposite outcomes — replace what \
             is there, or leave what is there alone. Write one of them."
        )));
    }
    Ok(())
}

/// A view column alias names a column and nothing else: a declared type or a
/// column option would have to be checked against the body's derived type or
/// silently ignored, and gnitz does neither.
fn reject_view_column_alias_decorations(
    columns: &[sqlparser::ast::ViewColumnDef],
    context: &str,
) -> Result<(), GnitzSqlError> {
    for col in columns {
        let sqlparser::ast::ViewColumnDef {
            name: _,
            data_type,
            options,
        } = col;
        reject_if(data_type.is_some(), context, "a type on an output column alias")?;
        reject_if(options.is_some(), context, "an option list on an output column alias")?;
    }
    Ok(())
}

/// Reject every `CreateIndex` field `execute_create_index` does not consume (`name`, `table_name`,
/// `columns`, `unique`). `using` is accepted only for the BTree default (gnitz's index is ordered /
/// range-scannable); any other type, plus `predicate` (partial index → full index), `concurrently`
/// (no non-blocking-build guarantee), `include`/`nulls_distinct`/`with` (silent default semantics)
/// are rejected. `if_not_exists` is consumed (the dispatcher's skip route).
pub(crate) fn reject_unhonored_create_index_clauses(
    ci: &sqlparser::ast::CreateIndex,
    context: &str,
) -> Result<(), GnitzSqlError> {
    let sqlparser::ast::CreateIndex {
        name: _,
        table_name: _,
        columns: _,
        unique: _,
        if_not_exists: _, // consumed: the dispatcher's skip route
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
    reject_if(!alter_options.is_empty(), context, "ALTER options (ALGORITHM / LOCK)")?;
    reject_index_type_and_options(using, index_options, context)?;
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
    reject_if(
        on_delete.is_some() || on_update.is_some(),
        context,
        "FOREIGN KEY ON DELETE/ON UPDATE action",
    )?;
    reject_if(match_kind.is_some(), context, "FOREIGN KEY MATCH")?;
    reject_constraint_characteristics(characteristics, context)
}

/// Constraint characteristics (`DEFERRABLE …`) are semantics gnitz does not
/// implement; every constraint kind that can carry them rejects them here.
fn reject_constraint_characteristics(
    characteristics: &Option<sqlparser::ast::ConstraintCharacteristics>,
    context: &str,
) -> Result<(), GnitzSqlError> {
    reject_if(
        characteristics.is_some(),
        context,
        "constraint characteristics (DEFERRABLE …)",
    )
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
    reject_if(
        index_name.is_some(),
        context,
        "a UNIQUE index name (use CONSTRAINT <name>)",
    )?;
    reject_if(
        !matches!(nulls_distinct, sqlparser::ast::NullsDistinctOption::None),
        context,
        "NULLS [NOT] DISTINCT",
    )?;
    reject_index_constraint_extras(index_type, index_options, characteristics, context)
}

/// The index-shape tail every index-creating surface shares: accept only the
/// BTree default `USING` (it names gnitz's real ordered index), and no index
/// options. CREATE INDEX, PRIMARY KEY and UNIQUE all spell the same two
/// rejections, so they read from one place and cannot drift apart.
fn reject_index_type_and_options(
    index_type: &Option<sqlparser::ast::IndexType>,
    index_options: &[sqlparser::ast::IndexOption],
    context: &str,
) -> Result<(), GnitzSqlError> {
    reject_if(
        index_type
            .as_ref()
            .is_some_and(|t| !matches!(t, sqlparser::ast::IndexType::BTree)),
        context,
        "USING (non-default index type)",
    )?;
    reject_if(!index_options.is_empty(), context, "index options")
}

/// The PK/UNIQUE-shared tail: the index shape above, plus no constraint
/// characteristics (DEFERRABLE …), which a CREATE INDEX cannot carry.
fn reject_index_constraint_extras(
    index_type: &Option<sqlparser::ast::IndexType>,
    index_options: &[sqlparser::ast::IndexOption],
    characteristics: &Option<sqlparser::ast::ConstraintCharacteristics>,
    context: &str,
) -> Result<(), GnitzSqlError> {
    reject_index_type_and_options(index_type, index_options, context)?;
    reject_constraint_characteristics(characteristics, context)
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
    let honored = |clause: &str| reject_if(site == ColumnOptionSite::AddColumn, context, clause);
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
/// The single operation — this guard enforces exactly one per statement — is
/// dispatched in `ddl::alter`. Exhaustive destructure (no `..`): a future
/// sqlparser field stops the build until it is classified.
pub(crate) fn reject_unhonored_alter_table_clauses(
    alter: &sqlparser::ast::AlterTable,
    context: &str,
) -> Result<(), GnitzSqlError> {
    let sqlparser::ast::AlterTable {
        // Consumed by the dispatcher, one per statement.
        name: _,
        operations,
        if_exists: _,
        // Inert: the statement-terminator token.
        end_token: _,
        // Rejected.
        only,
        location,
        on_cluster,
        table_type,
    } = alter;
    reject_if(operations.len() != 1, context, "more than one operation per statement")?;
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

/// The `(name, columns, query)` an `ALTER VIEW … AS` is planned from. `columns` is
/// the positional output alias list, honored exactly as `CREATE VIEW v (a, b) AS`
/// is. sqlparser's `AlterView` has no `if_exists`, so `ALTER VIEW IF EXISTS` never
/// parses.
///
/// The crate's only `AlterView` destructure (no `..`): returning the consumed
/// fields is what keeps it the only one, so a future `sqlparser` field cannot be
/// dropped by an extracting match elsewhere.
pub(crate) fn alter_view_parts<'a>(
    stmt: &'a sqlparser::ast::Statement,
    context: &str,
) -> Result<
    (
        &'a sqlparser::ast::ObjectName,
        &'a [sqlparser::ast::Ident],
        &'a sqlparser::ast::Query,
    ),
    GnitzSqlError,
> {
    let sqlparser::ast::Statement::AlterView {
        name,
        query,
        columns,
        with_options,
    } = stmt
    else {
        return Err(GnitzSqlError::Internal("not an ALTER VIEW statement".to_string()));
    };
    // `ALTER VIEW` retargets a body; letting it set a budget would make the
    // clause-less form silently drop one. `CREATE OR REPLACE VIEW` restates both.
    reject_if(!with_options.is_empty(), context, "WITH options")?;
    Ok((name, columns, query))
}

#[cfg(test)]
#[path = "tests/validate.rs"]
mod tests;
