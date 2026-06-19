//! DDL/view validation helpers: duplicate-column-name rejection, user index-name
//! rules, float-key rejection, and the auto-generated index-name format. Shared
//! by `ddl` and every view builder.

use crate::error::GnitzSqlError;
use gnitz_core::{ColumnDef, Schema};

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
