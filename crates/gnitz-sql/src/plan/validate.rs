//! DDL/view validation helpers: duplicate-column-name rejection, user index-name
//! rules, float-key rejection, and the auto-generated index-name format. Shared
//! by `ddl` and every view builder.

use crate::error::GnitzSqlError;
use gnitz_core::{ColumnDef, Schema, TypeCode};

/// Reject an output column list that names the same column twice. The binder's
/// name->index lookup silently binds to the first match, so a duplicate would
/// bind ambiguously for any downstream query. `context` names the DDL surface
/// for the error message (e.g. "table definition", "CREATE VIEW projection").
pub(crate) fn reject_duplicate_column_names<'a>(
    names: impl Iterator<Item = &'a str>,
    context: &str,
) -> Result<(), GnitzSqlError> {
    let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
    for name in names {
        if !seen.insert(name.to_lowercase()) {
            return Err(GnitzSqlError::Plan(format!(
                "duplicate column name '{name}' in {context}"
            )));
        }
    }
    Ok(())
}

/// Validate a user-supplied name destined to become an index name — a CREATE
/// INDEX name or a UNIQUE/constraint name that maps to a secondary index.
/// Beyond the general identifier rules, the reserved `__fk_` infix is rejected:
/// such a name would collide with internal FK-backing index names and persist as
/// undroppable (`drop_index` refuses it). The infix check is scoped here rather
/// than in `validate_user_identifier`, which also guards table/column/schema
/// names that may legitimately contain `__fk_`.
pub(crate) fn validate_user_index_name(name: &str) -> Result<(), GnitzSqlError> {
    gnitz_core::validate_user_identifier(name).map_err(GnitzSqlError::Plan)?;
    if name.contains(gnitz_core::FK_INDEX_INFIX) {
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
/// its single definition, shared by CREATE INDEX and CREATE TABLE … UNIQUE.
pub(crate) fn default_index_name(schema_name: &str, table_name: &str, col_names: &[&str]) -> String {
    format!("{schema_name}__{table_name}__idx_{}", col_names.join("_"))
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
}

/// Reject any `Select` clause a view builder does not consume. Each CREATE VIEW
/// builder reads a hand-picked subset of the parsed `Select`; without this guard
/// every unread clause (PREWHERE, TOP, QUALIFY, a join-view WHERE, …) is silently
/// dropped, turning the view the caller wrote into a different one that runs and
/// returns rows — a silent wrong result. `honored` names the clauses *this* shape
/// consumes; `context` names the surface for the message.
///
/// `distinct` is split out: `DISTINCT ON` is always rejected (non-deterministic
/// without an ORDER BY views forbid); plain `DISTINCT` is routed to the
/// DISTINCT-view shape by the classifier and rejected per-branch by the set-op
/// caller, so it falls through this guard untouched.
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

    let reject = |clause: &str| GnitzSqlError::Unsupported(format!("{context}: {clause} is not supported"));

    if matches!(distinct, Some(Distinct::On(_))) {
        return Err(reject("DISTINCT ON"));
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
