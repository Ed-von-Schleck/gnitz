//! The planner's message layer over `gnitz-wire`'s rules — wire decides, these
//! name the offending column — plus the shared projection-schema rules and the
//! clause guards.
//!
//! **The guard contract**, which `ast_util::reject_fn_qualifiers`
//! also carries: destructure the node without `..`, classifying every field
//! consumed / inert / rejected, so an upstream field addition is E0027 rather
//! than a clause silently dropped; then a table of `reject_if` calls
//! (`error::unsupported_clause`), naming the first present clause.
//!
//! **A node is destructured exhaustively where its fields are read.** A guard
//! that stays a function here therefore takes the node's payload, never the
//! enclosing `Statement`; a variant sqlparser gives no payload struct is
//! destructured in the arm that already selected it. Matches that only route
//! keep `..`.
//!
//! What a *surface* consumes is passed in — `HonoredClauses`, `QueryEnvelope`.

use crate::error::{reject_if, unsupported_clause, GnitzSqlError};
use gnitz_core::{ColType, ColumnDef, RelClass, RelDescriptor, TypeCode};

/// The column def of a *computed* projection item, from the expression's
/// nominal type. One home for the three rules every computed column obeys, so
/// the ad-hoc and CREATE VIEW binders (which each build their own projection
/// schema) cannot drift:
/// - `_expr{idx}` when the item has no alias;
/// - always nullable — an expression over a NOT NULL column can still be NULL
///   (division by zero, an unmatched CASE);
/// - typed by the register image the engine's register sink stores whole, not
///   the nominal type: a narrowing integer cast types as its target and a window
///   placeholder as the narrow value it stands in for, yet each rides a full
///   8-byte register. STRING maps to itself, which `register_image` already
///   accounts for.
pub(crate) fn computed_column(alias: Option<String>, idx: usize, nominal: ColType) -> ColumnDef {
    ColumnDef::typed(
        alias.unwrap_or_else(|| computed_column_name(idx)),
        nominal.register_image(),
        true,
    )
}

/// The name of the unaliased computed item at SELECT position `idx`.
pub(crate) fn computed_column_name(idx: usize) -> String {
    format!("_expr{idx}")
}

/// A hidden ordering column's label, for dumps and EXPLAIN. Placement travels
/// as a column index, not by this name.
pub(crate) fn order_column_name(i: usize) -> String {
    format!("_order{i}")
}

/// Reject an output column list that names the same *visible* column twice.
/// Hidden key slots are skipped — they are excluded from name resolution, so
/// they cannot bind ambiguously. `context` names the DDL surface for the error
/// message (e.g. "CREATE VIEW projection", "GROUP BY view").
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

/// The first name `names` repeats, folded case-insensitively as SQL identifiers
/// are, and returned as the user spelled it at the repeat.
fn first_duplicate<'a>(names: impl Iterator<Item = &'a str>) -> Option<&'a str> {
    let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
    names.into_iter().find(|n| !seen.insert(n.to_ascii_lowercase()))
}

/// Raw-name form of [`reject_duplicate_column_names`], for surfaces that have
/// only parser-AST names (CREATE TABLE — a freshly created column is never
/// hidden; a base table gains a hidden slot only later, via DROP COLUMN).
pub(crate) fn reject_duplicate_names<'a>(
    names: impl Iterator<Item = &'a str>,
    context: &str,
) -> Result<(), GnitzSqlError> {
    match first_duplicate(names) {
        Some(name) => Err(GnitzSqlError::Plan(format!(
            "duplicate column name '{name}' in {context}"
        ))),
        None => Ok(()),
    }
}

/// Reject a statement naming the same catalog object twice — `DROP TABLE a, a`.
/// The two retractions would land on one catalog PK, which the engine refuses in
/// terms of the catalog rather than of the SQL the user wrote.
pub(crate) fn reject_repeated_object<'a>(
    names: impl Iterator<Item = &'a str>,
    context: &str,
) -> Result<(), GnitzSqlError> {
    match first_duplicate(names) {
        Some(name) => Err(GnitzSqlError::Plan(format!(
            "{context}: '{name}' is named more than once"
        ))),
        None => Ok(()),
    }
}

/// Validate a user-supplied table/view/schema/index/constraint name: reject the
/// empty string, a leading `_` (reserved for the engine's own internal relation
/// and index names), and any character outside `[A-Za-z0-9_]`.
///
/// The leading-`_` reservation is *policy* the engine cannot enforce for a
/// relation or index name — it must accept exactly the rows it synthesizes
/// itself. It does enforce it for a schema name, which nothing synthesizes.
pub(crate) fn validate_user_name(name: &str) -> Result<(), GnitzSqlError> {
    gnitz_wire::validate_user_identifier(name).map_err(GnitzSqlError::Plan)
}

/// [`validate_user_name`] returning the canonical stored form. The one fold a
/// user-supplied name gets in this crate; the catalog gateway applies the same
/// one, so the two cannot spell canonicalization differently.
pub(crate) fn canonical_user_name(name: &str) -> Result<String, GnitzSqlError> {
    gnitz_wire::canonical_identifier(name).map_err(GnitzSqlError::Plan)
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
///
/// A hidden slot carries no name the user wrote — it is a synthetic key a
/// pre-map minted — so it is described rather than named.
pub(crate) fn reject_float_key(col: &ColumnDef, role: &str) -> Result<(), GnitzSqlError> {
    if col.type_code.is_float() {
        let what = if col.is_hidden {
            "a float-valued expression".to_string()
        } else {
            format!("float column '{}'", col.name)
        };
        return Err(reject_float_key_of(&what, role));
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
/// index-creating surfaces (CREATE INDEX and inline UNIQUE) run. `IndexKeyRule`
/// carries the failing position so this layer can name the column wire cannot.
pub(crate) fn reject_unbuildable_index_key(
    names: &[&str],
    types: &[TypeCode],
    src_pk_count: usize,
    src_pk_stride: usize,
    role: &str,
) -> Result<(), GnitzSqlError> {
    let raw: Vec<u8> = types.iter().map(|&tc| tc as u8).collect();
    gnitz_wire::index_key_types(&raw, src_pk_count, src_pk_stride).map_err(|rule| match rule {
        gnitz_wire::IndexKeyRule::NotEligible { col, .. } => non_key_eligible_error(names[col], types[col], role),
        arity_or_stride => GnitzSqlError::Unsupported(arity_or_stride.to_string()),
    })?;
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

/// What a surface requires of the relation a name resolved to. The wording rides
/// the variant, for the reason [`ColumnOptionSite`] gives.
#[derive(Clone, Copy)]
pub(crate) enum ClassWant {
    /// A stored, writable base table.
    BaseTable,
    /// A base table or a stream — the INSERT target rule, the one surface a
    /// storeless relation is a legal write target for.
    BaseTableOrStream,
    /// Any view, bounded or fed included.
    View,
}

impl ClassWant {
    fn accepts(self, class: RelClass) -> bool {
        match self {
            ClassWant::BaseTable => class == RelClass::Table,
            ClassWant::BaseTableOrStream => !class.is_view(),
            ClassWant::View => class.is_view(),
        }
    }

    /// How the requirement names itself in a rejection.
    fn noun(self) -> &'static str {
        match self {
            ClassWant::BaseTable => "a base table",
            ClassWant::BaseTableOrStream => "a base table or a stream",
            ClassWant::View => "a view",
        }
    }
}

/// Reject a relation of the wrong class for `op`. Takes the descriptor rather
/// than resolving one: the resolve is surface-specific, the verdict on what came
/// back is not.
pub(crate) fn require_class(rel: &RelDescriptor, name: &str, want: ClassWant, op: &str) -> Result<(), GnitzSqlError> {
    if want.accepts(rel.class) {
        return Ok(());
    }
    Err(GnitzSqlError::Unsupported(format!(
        "'{name}' is a {}; {op} requires {}",
        rel.class.noun(),
        want.noun()
    )))
}

/// Reject a counted column list wider than the engine's column limit, before the
/// wire encoder's assertion. `what` names the list.
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
    if honored.windows {
        // Honored, so not reported unsupported — but a QUALIFY with no window
        // value to filter on would then be silently dropped.
        if qualify.is_some() && !crate::ast_util::select_has_window(select) {
            return Err(GnitzSqlError::Unsupported(
                "QUALIFY needs a window function in the SELECT list or in the QUALIFY predicate".into(),
            ));
        }
    } else {
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
    /// No envelope clause at all — every narrowing site but the two below: a CTE
    /// body (nested CTE), a derived table, a sub-query inner, an INSERT source,
    /// a parenthesized set-op side.
    Bare,
    /// A view body, shared by CREATE VIEW and `ALTER VIEW … AS`: a `WITH` (CTE)
    /// clause is compiled by the `bind_ctes` phase, and `ORDER BY` + `LIMIT` are
    /// maintained as a top-N. No other tail clause has incremental-view
    /// semantics. The sink pair's own rules, a leftover `OFFSET` included, are
    /// the body binder's.
    ViewBody,
    /// Direct SELECT: a `WITH` (expanded by `dml::cte`), plus the
    /// client-side ordering sink — `ORDER BY` and `LIMIT`/`OFFSET` are applied
    /// to the fetched batch, so this guard leaves them to the caller.
    DirectSelect,
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
    let with_ok = !matches!(honored, QueryEnvelope::Bare);
    let sink_ok = matches!(honored, QueryEnvelope::DirectSelect | QueryEnvelope::ViewBody);
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

    reject_if(!with_ok && with.is_some(), context, "WITH (CTE)")?;
    reject_if(!sink_ok && limit_clause.is_some(), context, "LIMIT/OFFSET")?;
    reject_if(!sink_ok && order_by.is_some(), context, "ORDER BY")?;
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

/// Reject every `Insert`-statement clause the INSERT planner does not consume. It reads only
/// `table`, `source`, `columns`, `on` (ON CONFLICT) and `returning`; every other field is a
/// conflict / overwrite / partition clause parsed under `GenericDialect` and
/// silently reinterpreted as a plain append. The `source` is a full `Query` whose envelope (LIMIT,
/// ORDER BY, FETCH, a `WITH`, …) an INSERT equally cannot honor, so it is routed through
/// `reject_unhonored_query_clauses` here too — every INSERT-clause rejection lives in this one guard.
///
/// Exhaustive destructure (no `..`): a future `sqlparser` `Insert` field stops the build here.
pub(crate) fn reject_unhonored_insert_clauses(insert: &sqlparser::ast::Insert) -> Result<(), GnitzSqlError> {
    const CTX: &str = "INSERT";
    let sqlparser::ast::Insert {
        // Consumed by `execute_insert`; `source`'s `Query` envelope is additionally checked below.
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

    reject_if(or.is_some(), CTX, "OR (conflict clause)")?;
    reject_if(*ignore, CTX, "IGNORE")?;
    reject_if(*overwrite, CTX, "OVERWRITE")?;
    reject_if(*replace_into, CTX, "REPLACE INTO")?;
    reject_if(partitioned.is_some(), CTX, "PARTITION")?;
    reject_if(priority.is_some(), CTX, "priority (LOW_PRIORITY/HIGH_PRIORITY/DELAYED)")?;
    reject_if(insert_alias.is_some(), CTX, "row alias (AS alias)")?;
    reject_if(table_alias.is_some(), CTX, "table alias")?;
    reject_if(!assignments.is_empty(), CTX, "SET")?;
    reject_if(!after_columns.is_empty(), CTX, "AFTER columns")?;
    reject_if(settings.is_some(), CTX, "SETTINGS")?;
    reject_if(format_clause.is_some(), CTX, "FORMAT")?;
    reject_if(output.is_some(), CTX, "OUTPUT")?;
    reject_if(
        multi_table_insert_type.is_some()
            || !multi_table_into_clauses.is_empty()
            || !multi_table_when_clauses.is_empty()
            || multi_table_else_clause.is_some(),
        CTX,
        "multi-table INSERT (ALL/FIRST)",
    )?;
    // The `source` is a full `Query`; an INSERT honors no envelope clause on it (LIMIT/OFFSET,
    // ORDER BY, FETCH, FOR UPDATE/SHARE, SETTINGS, FORMAT, a `WITH`). Route it through the shared
    // `Query` guard so a dropped envelope clause is a clean error, not a silent full-table insert.
    if let Some(src) = source {
        reject_unhonored_query_clauses(src, QueryEnvelope::Bare, CTX)?;
    }
    Ok(())
}

/// Reject every `UPDATE` clause `execute_update` does not consume. It reads `table`, `assignments`,
/// `selection`; `from` (UPDATE … FROM join-update), `returning`, and `or` (SQLite conflict) all parse
/// under `GenericDialect` and were dropped — the join-update silently binds SET/WHERE against the
/// wrong relation set.
pub(crate) fn reject_unhonored_update_clauses(update: &sqlparser::ast::Update) -> Result<(), GnitzSqlError> {
    const CTX: &str = "UPDATE";
    let sqlparser::ast::Update {
        // Consumed by `execute_update` — `table` whole: it classifies the FROM
        // shape, so a join written there is rejected rather than dropped.
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
    reject_if(from.is_some(), CTX, "FROM (join-update)")?;
    reject_if(returning.is_some(), CTX, "RETURNING")?;
    reject_if(output.is_some(), CTX, "OUTPUT")?;
    reject_if(or.is_some(), CTX, "OR (conflict clause)")?;
    reject_if(!order_by.is_empty(), CTX, "ORDER BY")?;
    reject_if(limit.is_some(), CTX, "LIMIT")?;
    Ok(())
}

/// Reject every `DELETE` clause `execute_delete` does not consume. It reads `from` and `selection`;
/// `tables` (multi-table), `using` (join-delete), `returning`, `order_by`, and `limit` all parse
/// under `GenericDialect` and were dropped — a dropped `LIMIT` deletes every matched row (data loss),
/// a dropped `USING` binds WHERE against the wrong relation set.
pub(crate) fn reject_unhonored_delete_clauses(del: &sqlparser::ast::Delete) -> Result<(), GnitzSqlError> {
    const CTX: &str = "DELETE";
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
    reject_if(!tables.is_empty(), CTX, "multi-table delete")?;
    reject_if(using.is_some(), CTX, "USING (join-delete)")?;
    reject_if(returning.is_some(), CTX, "RETURNING")?;
    reject_if(output.is_some(), CTX, "OUTPUT")?;
    reject_if(!order_by.is_empty(), CTX, "ORDER BY")?;
    reject_if(limit.is_some(), CTX, "LIMIT")?;
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
pub(crate) fn reject_unhonored_create_table_clauses(create: &sqlparser::ast::CreateTable) -> Result<(), GnitzSqlError> {
    const CTX: &str = "CREATE TABLE";
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

    reject_if(query.is_some(), CTX, "AS SELECT (CTAS)")?;
    reject_if(*or_replace, CTX, "OR REPLACE (it would discard the table's rows)")?;
    reject_if(*temporary, CTX, "TEMPORARY")?;
    reject_if(global.is_some(), CTX, "GLOBAL/LOCAL")?;
    reject_if(like.is_some(), CTX, "LIKE")?;
    reject_if(clone.is_some(), CTX, "CLONE")?;
    reject_if(on_commit.is_some(), CTX, "ON COMMIT")?;
    reject_if(primary_key.is_some(), CTX, "PRIMARY KEY expression")?;
    reject_if(partition_of.is_some() || for_values.is_some(), CTX, "PARTITION OF")?;
    Ok(())
}

/// The `key = value` pairs of a `CREATE` statement's `WITH (…)` clause — the only
/// option form gnitz reads; an absent clause is the empty list. Every other form,
/// and every entry that is not a pair, is rejected rather than accepted as a
/// vendor no-op: gnitz's `WITH` keys decide what the relation *is*, so a silently
/// ignored `OPTIONS(stream = true)` would yield an ordinary durable table with no
/// error anywhere. `context` names the statement.
pub(crate) fn kv_options<'a>(
    options: &'a sqlparser::ast::CreateTableOptions,
    context: &str,
) -> Result<Vec<(&'a sqlparser::ast::Ident, &'a sqlparser::ast::Expr)>, GnitzSqlError> {
    let form = match options {
        sqlparser::ast::CreateTableOptions::With(opts) => {
            return opts
                .iter()
                .map(|opt| match opt {
                    sqlparser::ast::SqlOption::KeyValue { key, value } => Ok((key, value)),
                    other => Err(GnitzSqlError::Unsupported(format!(
                        "unsupported {context} option in WITH (…), which takes `key = value` entries: {other:?}"
                    ))),
                })
                .collect()
        }
        // The common case: collecting an empty exact-size iterator allocates nothing.
        sqlparser::ast::CreateTableOptions::None => return Ok(Vec::new()),
        sqlparser::ast::CreateTableOptions::Options(_) => "OPTIONS (…)",
        sqlparser::ast::CreateTableOptions::Plain(_) => "space-separated options",
        sqlparser::ast::CreateTableOptions::TableProperties(_) => "TBLPROPERTIES (…)",
    };
    Err(GnitzSqlError::Unsupported(format!(
        "{form} is not supported; options must be given as WITH (…)"
    )))
}

/// Reject every `CREATE VIEW` clause `execute_create_view` does not consume
/// (`name`, `query`, `options` — see `hir::create::decode_view_options` — plus
/// `columns` as positional output aliases and `or_replace`/`if_not_exists` as the
/// dispatcher's create route). `materialized` is accepted — a gnitz view is already
/// incrementally materialized. `temporary` (silent permanent view) and `to`
/// (silently ignored target) are rejected, as is `or_alter`: T-SQL's `CREATE OR
/// ALTER` is `OR REPLACE` under another name. `with_no_schema_binding` parses but is
/// a no-op optimizer hint; the rest cannot populate under `GenericDialect`.
pub(crate) fn reject_unhonored_create_view_clauses(cv: &sqlparser::ast::CreateView) -> Result<(), GnitzSqlError> {
    const CTX: &str = "CREATE VIEW";
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
    reject_if(*or_alter, CTX, "OR ALTER (spell it OR REPLACE)")?;
    reject_if(*temporary, CTX, "TEMPORARY")?;
    reject_if(to.is_some(), CTX, "TO (target table)")?;
    // A view column alias names a column and nothing else: a declared type or a
    // column option would have to be checked against the body's derived type or
    // silently ignored, and gnitz does neither.
    for col in columns {
        let sqlparser::ast::ViewColumnDef { name: _, data_type, options } = col;
        reject_if(data_type.is_some(), CTX, "a type on an output column alias")?;
        reject_if(options.is_some(), CTX, "an option list on an output column alias")?;
    }
    // Not `reject_if`: that template names ONE clause a statement does not honor,
    // and each of these is honored alone. `sqlparser` parses the pair; no dialect
    // defines it.
    if *or_replace && *if_not_exists {
        return Err(GnitzSqlError::Unsupported(format!(
            "{CTX}: OR REPLACE and IF NOT EXISTS ask for opposite outcomes — replace what \
             is there, or leave what is there alone. Write one of them."
        )));
    }
    Ok(())
}

/// Reject every `CreateIndex` field `execute_create_index` does not consume (`name`, `table_name`,
/// `columns`, `unique`). `using` is accepted only for the BTree default (gnitz's index is ordered /
/// range-scannable); any other type, plus `predicate` (partial index → full index), `concurrently`
/// (no non-blocking-build guarantee), `include`/`nulls_distinct`/`with` (silent default semantics)
/// are rejected. `if_not_exists` is consumed (the dispatcher's skip route).
pub(crate) fn reject_unhonored_create_index_clauses(ci: &sqlparser::ast::CreateIndex) -> Result<(), GnitzSqlError> {
    const CTX: &str = "CREATE INDEX";
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
    reject_if(predicate.is_some(), CTX, "WHERE (partial index)")?;
    reject_if(!alter_options.is_empty(), CTX, "ALTER options (ALGORITHM / LOCK)")?;
    reject_index_type_and_options(using, index_options, CTX)?;
    reject_if(*concurrently, CTX, "CONCURRENTLY")?;
    reject_if(!include.is_empty(), CTX, "INCLUDE (covering columns)")?;
    reject_if(nulls_distinct.is_some(), CTX, "NULLS [NOT] DISTINCT")?;
    reject_if(!with.is_empty(), CTX, "WITH (storage parameters)")?;
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
        // Consumed by `execute_create_table` on both spellings (the name becomes
        // the index name); a column-level one arrives as `ColumnOptionDef.name`.
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

impl ColumnOptionSite {
    /// How the site names itself in a rejection. Derived rather than passed
    /// beside it: the two are 1:1, and a second parameter is a hole a caller can
    /// spell the wrong way round.
    fn context(self) -> &'static str {
        match self {
            ColumnOptionSite::CreateTable => "column definition",
            ColumnOptionSite::AddColumn => "ADD COLUMN",
        }
    }
}

/// Reject every column option `site` does not honor. Honored by CREATE TABLE: NULL/NOT NULL
/// (nullability), PRIMARY KEY, UNIQUE, FOREIGN KEY target — the honored constraint variants are
/// descended into (`reject_unhonored_{pk,unique,fk}_fields`) so an unimplemented field inside them
/// (a referential action, DEFERRABLE, NULLS NOT DISTINCT, …) is rejected too. Every
/// constraint/semantic option gnitz lacks (DEFAULT, CHECK, GENERATED, IDENTITY, ON UPDATE, COLLATE,
/// SRID, INVISIBLE, …) is rejected; pure metadata (COMMENT/OPTIONS/POLICY/TAGS) is accepted. Exhaustive
/// over the `ColumnDef` / `ColumnOptionDef` fields (no `..`) and over all 23 `ColumnOption` variants
/// (no `_`): a new field or variant stops the build.
pub(crate) fn reject_unhonored_column_options(
    col: &sqlparser::ast::ColumnDef,
    site: ColumnOptionSite,
) -> Result<(), GnitzSqlError> {
    use sqlparser::ast::ColumnOption as O;
    let context = site.context();
    // Consumed at CREATE TABLE, unhonored at ADD COLUMN.
    let honored = |clause: &str| reject_if(site == ColumnOptionSite::AddColumn, context, clause);
    let sqlparser::ast::ColumnDef {
        // The name and type are the column; this guard is about its options.
        name: _,
        data_type: _,
        options,
    } = col;
    for opt in options {
        // `name` is consumed for UNIQUE (it becomes the index name, as on the
        // table-level spelling) and inert elsewhere — gnitz names no other
        // constraint.
        let sqlparser::ast::ColumnOptionDef { name: _, option } = opt;
        match option {
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
) -> Result<(), GnitzSqlError> {
    const CTX: &str = "table constraint";
    use sqlparser::ast::TableConstraint as C;
    for c in constraints {
        match c {
            // Consumed, but only the column list / constraint name — descend so an
            // unimplemented field is rejected, not silently dropped.
            C::PrimaryKey(pk) => reject_unhonored_pk_fields(pk, CTX)?,
            C::Unique(u) => reject_unhonored_unique_fields(u, CTX)?,
            C::ForeignKey(fk) => reject_unhonored_fk_fields(fk, CTX)?,
            C::Check(_) => return Err(unsupported_clause(CTX, "CHECK constraint")),
            C::Index(_) => return Err(unsupported_clause(CTX, "INDEX in table definition")),
            C::FulltextOrSpatial(_) => return Err(unsupported_clause(CTX, "FULLTEXT/SPATIAL index")),
            C::PrimaryKeyUsingIndex(_) => return Err(unsupported_clause(CTX, "PRIMARY KEY USING INDEX")),
            C::UniqueUsingIndex(_) => return Err(unsupported_clause(CTX, "UNIQUE USING INDEX")),
        }
    }
    Ok(())
}

/// Reject every `ALTER TABLE` envelope clause gnitz does not honor: `ONLY`
/// (silently scopes out partition children), a Hive `SET LOCATION`, `ON CLUSTER`,
/// and a non-`None` `table_type` (Iceberg/Dynamic — a different storage engine).
/// The operation is dispatched in `ddl::alter`. Exhaustive destructure (no `..`):
/// a future sqlparser field stops the build until it is classified.
pub(crate) fn reject_unhonored_alter_table_clauses(alter: &sqlparser::ast::AlterTable) -> Result<(), GnitzSqlError> {
    const CTX: &str = "ALTER TABLE";
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
    reject_if(*only, CTX, "ONLY")?;
    reject_if(location.is_some(), CTX, "SET LOCATION")?;
    reject_if(on_cluster.is_some(), CTX, "ON CLUSTER")?;
    reject_if(
        table_type.is_some(),
        CTX,
        "a non-default table type (Iceberg/Dynamic/External)",
    )?;
    Ok(())
}

#[cfg(test)]
#[path = "tests/validate.rs"]
mod tests;
