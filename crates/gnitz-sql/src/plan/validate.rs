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
/// only parser-AST names (CREATE TABLE — base-table columns are never hidden).
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
        return Err(GnitzSqlError::Unsupported(format!(
            "{role} column '{name}' of type {tc:?} is not supported \
             ({role} must be a fixed-width integer, U128, or UUID column; \
             String, Blob, and float columns cannot be a {role} key)"
        )));
    }
    Ok(())
}

/// The single spelling of an "unhonored clause" rejection. `reject_unhonored_select_clauses`,
/// `reject_unhonored_query_clauses`, and the `Cte`-envelope reject all funnel through this, so the
/// `"{context}: {clause} is not supported"` grammar cannot drift per site.
pub(crate) fn unsupported_clause(context: &str, clause: &str) -> GnitzSqlError {
    GnitzSqlError::Unsupported(format!("{context}: {clause} is not supported"))
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

/// The `Select` clauses a view shape legitimately consumes, beyond the universal
/// `from` + `projection`. Passed to [`reject_unhonored_select_clauses`]; every
/// clause not named here (and not honored unconditionally) is rejected.
#[derive(Clone, Copy)]
pub(crate) struct HonoredClauses {
    /// A top-level `WHERE` filter. Honored by every shape except the join
    /// builder, which consumes only the `ON` predicate — a join-view `WHERE` has
    /// no builder and would be dropped.
    pub where_filter: bool,
    /// `GROUP BY` and its `HAVING`. Honored only by the grouped-aggregate builder.
    pub grouping: bool,
    /// Plain `DISTINCT`. Honored only by the DISTINCT-view builder (which dedups
    /// after this returns); every other site passes `false`. `DISTINCT ON` is
    /// always rejected, independent of this flag.
    pub distinct: bool,
}

/// Reject any `Select` clause a view builder does not consume. Each CREATE VIEW
/// builder reads a hand-picked subset of the parsed `Select`; without this guard
/// every unread clause (PREWHERE, TOP, QUALIFY, a join-view WHERE, …) is silently
/// dropped, turning the view the caller wrote into a different one that runs and
/// returns rows — a silent wrong result. `honored` names the clauses *this* shape
/// consumes; `context` names the surface for the message.
///
/// `DISTINCT` is gated by [`HonoredClauses::distinct`]: `DISTINCT ON` is always
/// rejected (non-deterministic without an ORDER BY views forbid), while plain
/// `DISTINCT` is rejected unless the caller honors it — only the DISTINCT-view
/// builder does (it dedups after this returns); every other site passes `false`.
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
        // Conditionally honored (see `HonoredClauses`); `distinct` handled below.
        selection,
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
        // Inert: parser tokens, positional flags for an already-checked clause
        // (`top`/`qualify`/`distinct`), or a clause the GenericDialect never
        // produces (`value_table_mode` is BigQuery-only). No droppable semantics.
        select_token: _,
        top_before_distinct: _,
        window_before_qualify: _,
        value_table_mode: _,
        flavor: _,
    } = select;

    let reject = |clause: &str| unsupported_clause(context, clause);

    if matches!(distinct, Some(Distinct::On(_))) {
        return Err(reject("DISTINCT ON"));
    }
    if !honored.distinct && matches!(distinct, Some(Distinct::Distinct)) {
        return Err(reject("DISTINCT"));
    }
    if !honored.where_filter && selection.is_some() {
        return Err(reject("WHERE"));
    }
    if !honored.grouping && crate::ast_util::group_by_is_present(group_by) {
        return Err(reject("GROUP BY"));
    }
    if !honored.grouping && having.is_some() {
        return Err(reject("HAVING"));
    }
    if top.is_some() {
        return Err(reject("TOP"));
    }
    if prewhere.is_some() {
        return Err(reject("PREWHERE"));
    }
    if into.is_some() {
        return Err(reject("SELECT INTO"));
    }
    if qualify.is_some() {
        return Err(reject("QUALIFY"));
    }
    if connect_by.is_some() {
        return Err(reject("CONNECT BY"));
    }
    if !lateral_views.is_empty() {
        return Err(reject("LATERAL VIEW"));
    }
    if !cluster_by.is_empty() {
        return Err(reject("CLUSTER BY"));
    }
    if !distribute_by.is_empty() {
        return Err(reject("DISTRIBUTE BY"));
    }
    if !sort_by.is_empty() {
        return Err(reject("SORT BY"));
    }
    if !named_window.is_empty() {
        return Err(reject("WINDOW"));
    }
    Ok(())
}

/// The `Query`-envelope clauses a narrowing site legitimately consumes, beyond the universal
/// `body`. Every clause not named here is rejected, so a dropped clause is a clean error rather
/// than a silent wrong result.
#[derive(Clone, Copy)]
pub(crate) struct HonoredQueryClauses {
    /// A `WITH` (CTE) clause. Honored only by CREATE VIEW, which inlines it (`inline_ctes`).
    /// A direct SELECT, a CTE body (nested CTE), and an INSERT source reject it.
    pub with: bool,
    /// A `LIMIT`/`OFFSET` clause. Honored only by direct SELECT, which applies a plain `LIMIT n`
    /// client-side and rejects `OFFSET` / `LIMIT … BY` with its own messages; when `true` this
    /// guard leaves `limit_clause` to the caller.
    pub limit: bool,
}

/// Reject every `Query`-envelope clause a narrowing site does not consume. `GenericDialect`
/// parses a tail of envelope clauses (FETCH, FOR UPDATE/XML, SETTINGS, FORMAT, …) the planner has
/// no operator for; without this guard each is silently dropped — `WITH` and `FETCH` are wrong
/// results, the rest must not be silently accepted. `honored` names what *this* site consumes;
/// `context` names the surface for the message.
///
/// This is the crate's single exhaustive `Query` destructure (no `..`): a future `sqlparser`
/// field stops the build here until it is classified, converting a silent-drop-on-upgrade into a
/// compile error. All four narrowing sites funnel through it, so no per-site list can drift.
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
    } = query;

    let reject = |clause: &str| unsupported_clause(context, clause);

    if !honored.with && with.is_some() {
        return Err(reject("WITH (CTE)"));
    }
    if !honored.limit && limit_clause.is_some() {
        return Err(reject("LIMIT/OFFSET"));
    }
    if order_by.is_some() {
        return Err(reject("ORDER BY"));
    }
    if fetch.is_some() {
        return Err(reject("FETCH"));
    }
    if !locks.is_empty() {
        return Err(reject("FOR UPDATE/SHARE"));
    }
    if for_clause.is_some() {
        return Err(reject("FOR XML/JSON/BROWSE"));
    }
    if settings.is_some() {
        return Err(reject("SETTINGS"));
    }
    if format_clause.is_some() {
        return Err(reject("FORMAT"));
    }
    Ok(())
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
        // Inert keyword markers (`INTO` / `TABLE`): positional flags, no droppable semantics.
        into: _,
        has_table_keyword: _,
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
    } = insert;

    let reject = |clause: &str| unsupported_clause(context, clause);
    if or.is_some() {
        return Err(reject("OR (conflict clause)"));
    }
    if *ignore {
        return Err(reject("IGNORE"));
    }
    if *overwrite {
        return Err(reject("OVERWRITE"));
    }
    if *replace_into {
        return Err(reject("REPLACE INTO"));
    }
    if partitioned.is_some() {
        return Err(reject("PARTITION"));
    }
    if priority.is_some() {
        return Err(reject("priority (LOW_PRIORITY/HIGH_PRIORITY/DELAYED)"));
    }
    if insert_alias.is_some() {
        return Err(reject("row alias (AS alias)"));
    }
    if table_alias.is_some() {
        return Err(reject("table alias"));
    }
    if !assignments.is_empty() {
        return Err(reject("SET"));
    }
    if !after_columns.is_empty() {
        return Err(reject("AFTER columns"));
    }
    if settings.is_some() {
        return Err(reject("SETTINGS"));
    }
    if format_clause.is_some() {
        return Err(reject("FORMAT"));
    }
    // The `source` is a full `Query`; an INSERT honors no envelope clause on it (LIMIT/OFFSET,
    // ORDER BY, FETCH, FOR UPDATE/SHARE, SETTINGS, FORMAT, a `WITH`). Route it through the shared
    // `Query` guard so a dropped envelope clause is a clean error, not a silent full-table insert.
    if let Some(src) = source {
        reject_unhonored_query_clauses(
            src,
            HonoredQueryClauses {
                with: false,
                limit: false,
            },
            context,
        )?;
    }
    Ok(())
}

/// Reject every `UPDATE` clause `execute_update` does not consume. It reads `table`, `assignments`,
/// `selection`; `from` (UPDATE … FROM join-update), `returning`, and `or` (SQLite conflict) all parse
/// under `GenericDialect` and were dropped — the join-update silently binds SET/WHERE against the
/// wrong relation set.
pub(crate) fn reject_unhonored_update_clauses(
    stmt: &sqlparser::ast::Statement,
    context: &str,
) -> Result<(), GnitzSqlError> {
    let sqlparser::ast::Statement::Update {
        table: _,
        assignments: _,
        selection: _,
        from,
        returning,
        or,
    } = stmt
    else {
        return Err(GnitzSqlError::Bind("not an UPDATE statement".to_string()));
    };
    let reject = |clause: &str| unsupported_clause(context, clause);
    if from.is_some() {
        return Err(reject("FROM (join-update)"));
    }
    if returning.is_some() {
        return Err(reject("RETURNING"));
    }
    if or.is_some() {
        return Err(reject("OR (conflict clause)"));
    }
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
        tables,
        using,
        returning,
        order_by,
        limit,
    } = del;
    let reject = |clause: &str| unsupported_clause(context, clause);
    if !tables.is_empty() {
        return Err(reject("multi-table delete"));
    }
    if using.is_some() {
        return Err(reject("USING (join-delete)"));
    }
    if returning.is_some() {
        return Err(reject("RETURNING"));
    }
    if !order_by.is_empty() {
        return Err(reject("ORDER BY"));
    }
    if limit.is_some() {
        return Err(reject("LIMIT"));
    }
    Ok(())
}

/// Reject every `CREATE TABLE` envelope clause `execute_create_table` does not consume. `name`,
/// `columns`, `constraints`, `cluster_by`, `with_options` are consumed (column/constraint contents
/// are further guarded by [`reject_unhonored_column_options`] / [`reject_unhonored_table_constraints`]).
/// Rejected: `query` (CTAS), `temporary`/`global` (silent permanent table), `like`/`clone` (empty
/// table, ignoring the template), `on_commit`, and `primary_key` (silently substitutes the PK). The
/// `_`-bound remainder parses under `GenericDialect` or not, but carries no gnitz-honorable semantics
/// — storage/engine/vendor metadata accepted as no-ops, never changing a result.
/// `or_replace`/`if_not_exists` drop *loudly* (a name collision still errors), so they are not
/// rejected; implement them later.
///
/// Exhaustive destructure (no `..`) over all 50 fields: a future `sqlparser` field stops the build.
pub(crate) fn reject_unhonored_create_table_clauses(
    create: &sqlparser::ast::CreateTable,
    context: &str,
) -> Result<(), GnitzSqlError> {
    let sqlparser::ast::CreateTable {
        // Consumed (column/constraint contents further guarded by the column-option and table-constraint guards).
        name: _,
        columns: _,
        constraints: _,
        cluster_by: _,
        with_options: _,
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
        // No gnitz-honorable semantics — storage/engine/vendor metadata accepted as no-ops.
        external: _,
        transient: _,
        volatile: _,
        iceberg: _,
        hive_distribution: _,
        hive_formats: _,
        table_properties: _,
        file_format: _,
        location: _,
        without_rowid: _,
        engine: _,
        comment: _,
        auto_increment_offset: _,
        default_charset: _,
        collation: _,
        on_cluster: _,
        order_by: _,
        partition_by: _,
        clustered_by: _,
        options: _,
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
        with_tags: _,
        external_volume: _,
        base_location: _,
        catalog: _,
        catalog_sync: _,
        storage_serialization_policy: _,
    } = create;

    let reject = |clause: &str| unsupported_clause(context, clause);
    if query.is_some() {
        return Err(reject("AS SELECT (CTAS)"));
    }
    if *temporary {
        return Err(reject("TEMPORARY"));
    }
    if global.is_some() {
        return Err(reject("GLOBAL/LOCAL"));
    }
    if like.is_some() {
        return Err(reject("LIKE"));
    }
    if clone.is_some() {
        return Err(reject("CLONE"));
    }
    if on_commit.is_some() {
        return Err(reject("ON COMMIT"));
    }
    if primary_key.is_some() {
        return Err(reject("PRIMARY KEY expression"));
    }
    Ok(())
}

/// Reject every `CREATE VIEW` clause `execute_create_view` does not consume (`name`, `query`).
/// `materialized` is accepted — a gnitz view is already incrementally materialized. `temporary`
/// (silent permanent view), `to` (silently ignored target), and `columns` (output aliases dropped →
/// wrong view schema) are rejected. `or_alter`/`or_replace`/`if_not_exists` drop loudly; implement
/// later. `with_no_schema_binding` parses but is a no-op optimizer hint; the rest cannot populate
/// under `GenericDialect`.
pub(crate) fn reject_unhonored_create_view_clauses(
    stmt: &sqlparser::ast::Statement,
    context: &str,
) -> Result<(), GnitzSqlError> {
    let sqlparser::ast::Statement::CreateView {
        name: _,
        query: _,
        materialized: _, // accepted: names gnitz's real behavior
        or_alter: _,
        or_replace: _,
        if_not_exists: _,          // loud on drop; implement later
        with_no_schema_binding: _, // no-op optimizer hint
        options: _,
        cluster_by: _,
        comment: _,
        params: _, // cannot populate under GenericDialect
        columns,
        temporary,
        to, // rejected
    } = stmt
    else {
        return Err(GnitzSqlError::Bind("not a CREATE VIEW statement".to_string()));
    };
    let reject = |clause: &str| unsupported_clause(context, clause);
    if !columns.is_empty() {
        return Err(reject("output column aliases"));
    }
    if *temporary {
        return Err(reject("TEMPORARY"));
    }
    if to.is_some() {
        return Err(reject("TO (target table)"));
    }
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
    } = ci;
    let reject = |clause: &str| unsupported_clause(context, clause);
    if predicate.is_some() {
        return Err(reject("WHERE (partial index)"));
    }
    if let Some(t) = using {
        if !matches!(t, IndexType::BTree) {
            return Err(reject("USING (non-default index type)"));
        }
    }
    if *concurrently {
        return Err(reject("CONCURRENTLY"));
    }
    if !include.is_empty() {
        return Err(reject("INCLUDE (covering columns)"));
    }
    if nulls_distinct.is_some() {
        return Err(reject("NULLS [NOT] DISTINCT"));
    }
    if !with.is_empty() {
        return Err(reject("WITH (storage parameters)"));
    }
    Ok(())
}

/// Reject every column option `execute_create_table` does not honor. Honored: NULL/NOT NULL
/// (nullability), UNIQUE (incl. PRIMARY KEY), FOREIGN KEY target. A FK referential action and every
/// constraint/semantic option gnitz lacks (DEFAULT, CHECK, GENERATED, IDENTITY, ON UPDATE, COLLATE,
/// …) is rejected; pure metadata (COMMENT/OPTIONS/POLICY/TAGS) is accepted. Exhaustive over all 20
/// `ColumnOption` variants (no `_`): a new variant stops the build.
pub(crate) fn reject_unhonored_column_options(
    col: &sqlparser::ast::ColumnDef,
    context: &str,
) -> Result<(), GnitzSqlError> {
    use sqlparser::ast::ColumnOption as O;
    let reject = |clause: &str| unsupported_clause(context, clause);
    for opt in &col.options {
        match &opt.option {
            O::Null | O::NotNull | O::Unique { .. } => {} // consumed
            O::ForeignKey {
                on_delete, on_update, ..
            } => {
                // target consumed; action rejected
                if on_delete.is_some() || on_update.is_some() {
                    return Err(reject("FOREIGN KEY ON DELETE/ON UPDATE action"));
                }
            }
            O::Comment(_) | O::Options(_) | O::Policy(_) | O::Tags(_) => {} // inert metadata
            O::Default(_) => return Err(reject("DEFAULT")),
            O::Check(_) => return Err(reject("CHECK")),
            O::Generated { .. } => return Err(reject("GENERATED")),
            O::Identity(_) => return Err(reject("IDENTITY / AUTO_INCREMENT")),
            O::OnUpdate(_) => return Err(reject("ON UPDATE")),
            O::OnConflict(_) => return Err(reject("ON CONFLICT")),
            O::Collation(_) => return Err(reject("COLLATE")),
            O::CharacterSet(_) => return Err(reject("CHARACTER SET")),
            O::Materialized(_) => return Err(reject("MATERIALIZED column")),
            O::Ephemeral(_) => return Err(reject("EPHEMERAL column")),
            O::Alias(_) => return Err(reject("ALIAS column")),
            O::DialectSpecific(_) => return Err(reject("dialect-specific column option")),
        }
    }
    Ok(())
}

/// Reject every table constraint `execute_create_table` does not honor. Honored: PRIMARY KEY, UNIQUE,
/// FOREIGN KEY target. A FK referential action, `CHECK`, inline `INDEX`, and `FULLTEXT`/`SPATIAL`
/// indexes are rejected. Exhaustive over all 6 `TableConstraint` variants (no `_`).
pub(crate) fn reject_unhonored_table_constraints(
    constraints: &[sqlparser::ast::TableConstraint],
    context: &str,
) -> Result<(), GnitzSqlError> {
    use sqlparser::ast::TableConstraint as C;
    let reject = |clause: &str| unsupported_clause(context, clause);
    for c in constraints {
        match c {
            C::PrimaryKey { .. } | C::Unique { .. } => {}
            C::ForeignKey {
                on_delete, on_update, ..
            } => {
                if on_delete.is_some() || on_update.is_some() {
                    return Err(reject("FOREIGN KEY ON DELETE/ON UPDATE action"));
                }
            }
            C::Check { .. } => return Err(reject("CHECK constraint")),
            C::Index { .. } => return Err(reject("INDEX in table definition")),
            C::FulltextOrSpatial { .. } => return Err(reject("FULLTEXT/SPATIAL index")),
        }
    }
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
}
