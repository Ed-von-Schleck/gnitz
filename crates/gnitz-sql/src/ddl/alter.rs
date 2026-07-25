//! `ALTER TABLE` operation dispatch: rename table/view/column, ADD/DROP
//! CONSTRAINT (mapped to the CREATE/DROP INDEX paths), and clean rejections for
//! every operation not yet supported. `ALTER VIEW … AS` lives in
//! `crate::hir::create` (it recompiles a query). Every supported op is one
//! catalog-only `push_ddl` through the `gnitz-core` client.

use crate::ast_util::extract_name;
use crate::bind::{find_unique_column, Binder};
use crate::error::GnitzSqlError;
use crate::validate::{reject_unhonored_unique_fields, validate_user_index_name, validate_user_name};
use crate::SqlResult;
use gnitz_core::GnitzClient;
use sqlparser::ast::{
    AlterColumnOperation, AlterTable, AlterTableOperation, DropBehavior, Ident, ObjectName, RenameTableNameKind,
    TableConstraint,
};

pub(crate) fn execute_alter_table(
    client: &mut GnitzClient,
    schema_name: &str,
    alter: &AlterTable,
    binder: &mut Binder<'_>,
) -> Result<SqlResult, GnitzSqlError> {
    // Exactly one operation per statement (the multi-op comma form is rejected in
    // `dispatch` before we get here).
    match &alter.operations[0] {
        AlterTableOperation::RenameTable { table_name } => {
            // `RENAME TO` and (some dialects') `RENAME AS` both mean rename-to.
            let target = match table_name {
                RenameTableNameKind::To(n) | RenameTableNameKind::As(n) => n,
            };
            rename_relation(client, schema_name, &alter.name, alter.if_exists, target)
        }
        AlterTableOperation::RenameColumn {
            old_column_name,
            new_column_name,
        } => rename_column(
            client,
            schema_name,
            &alter.name,
            alter.if_exists,
            &old_column_name.value,
            &new_column_name.value,
        ),
        AlterTableOperation::AddConstraint { constraint, not_valid } => add_constraint(
            client,
            schema_name,
            &alter.name,
            alter.if_exists,
            constraint,
            *not_valid,
            binder,
        ),
        AlterTableOperation::DropConstraint {
            if_exists,
            name,
            drop_behavior: _,
        } => drop_constraint(
            client,
            schema_name,
            &alter.name,
            alter.if_exists,
            &name.value,
            *if_exists,
        ),
        AlterTableOperation::DropColumn {
            column_names,
            if_exists: col_if_exists,
            drop_behavior,
            ..
        } => drop_column(
            client,
            schema_name,
            &alter.name,
            alter.if_exists,
            *col_if_exists,
            column_names,
            *drop_behavior,
        ),
        // ADD COLUMN is plan 2's; reject here (its if_not_exists / column_position
        // FIRST/AFTER included).
        AlterTableOperation::AddColumn { .. } => Err(GnitzSqlError::Unsupported(
            "ALTER TABLE ADD COLUMN is not supported".to_string(),
        )),
        // Destructured per-variant so a newly supported operation is an additive
        // arm split, and with no plan path in any message.
        AlterTableOperation::AlterColumn { column_name, op } => {
            use AlterColumnOperation as Op;
            let msg = match op {
                Op::DropNotNull => {
                    return drop_not_null(client, schema_name, &alter.name, alter.if_exists, &column_name.value)
                }
                Op::SetNotNull => "ALTER COLUMN SET NOT NULL is not supported (needs a full-table validation scan)",
                Op::SetDataType { .. } => "ALTER COLUMN SET DATA TYPE is not supported",
                Op::SetDefault { .. } => "ALTER COLUMN SET DEFAULT is not supported (no column defaults in gnitz)",
                Op::DropDefault => "ALTER COLUMN DROP DEFAULT is not supported (no column defaults in gnitz)",
                Op::AddGenerated { .. } => {
                    "ALTER COLUMN ADD GENERATED is not supported (no generated columns in gnitz)"
                }
            };
            Err(GnitzSqlError::Unsupported(msg.to_string()))
        }
        // Every other AlterTableOperation variant (RenameConstraint, DropPrimaryKey,
        // DropForeignKey, ChangeColumn, ModifyColumn, partition/RLS/trigger ops, …):
        // the catch-all reject. A wildcard is safe (no panic) — sqlparser is pinned
        // at 0.62, so no variant is silently reinterpreted.
        _ => Err(GnitzSqlError::Unsupported(
            "this ALTER TABLE operation is not supported".to_string(),
        )),
    }
}

/// `ALTER TABLE <t> RENAME TO <n>` — `t` may be a table OR a view (Postgres
/// accepts `ALTER TABLE <view> RENAME TO`, and sqlparser parses both as
/// `RenameTable`). Views bind sources by id and columns by ordinal, so a rename
/// is always safe with dependents (no dependent-view guard).
fn rename_relation(
    client: &mut GnitzClient,
    schema_name: &str,
    source: &ObjectName,
    if_exists: bool,
    target: &ObjectName,
) -> Result<SqlResult, GnitzSqlError> {
    let source_name = extract_name(source, "ALTER TABLE")?;
    validate_user_name(&source_name)?;

    let resolved = client
        .resolve_relation_kind(schema_name, &source_name)
        .map_err(GnitzSqlError::Exec)?;
    let Some((id, is_view)) = resolved else {
        if if_exists {
            return Ok(altered("table", extract_name(target, "ALTER TABLE")?));
        }
        return Err(missing("relation", schema_name, &source_name));
    };
    reject_system_relation(id)?;

    let new_name = rename_target_name(target, schema_name)?;
    validate_user_name(&new_name)?;

    client
        .alter_rename_relation(schema_name, is_view, &source_name, &new_name)
        .map_err(GnitzSqlError::Exec)?;
    Ok(altered(relation_kind(is_view), new_name))
}

/// `ALTER TABLE <t> RENAME COLUMN <a> TO <b>` — `t` must be a base table (a view
/// is read-only and column-bound by ordinal).
fn rename_column(
    client: &mut GnitzClient,
    schema_name: &str,
    source: &ObjectName,
    if_exists: bool,
    old_col: &str,
    new_col: &str,
) -> Result<SqlResult, GnitzSqlError> {
    let source_name = extract_name(source, "ALTER TABLE")?;
    if resolve_alter_base_table(client, schema_name, &source_name, if_exists, "RENAME COLUMN")?.is_none() {
        return Ok(altered("column", new_col.to_string()));
    }
    client
        .alter_rename_column(schema_name, &source_name, old_col, new_col)
        .map_err(GnitzSqlError::Exec)?;
    Ok(altered("column", new_col.to_string()))
}

/// `ALTER TABLE <t> DROP [COLUMN] [IF EXISTS] <c> [CASCADE]` — a **logical** drop:
/// the column is flagged hidden and kept physically present (zero-filled NOT
/// NULL, disk never reclaimed — the same as Postgres). `t` must be a base table.
/// Rejects a PK / SERIAL / FK-child / secondary-index-covered column, a
/// multi-column drop, and CASCADE; the dependent-view RESTRICT is enforced
/// engine-side (like DROP TABLE), so it is not re-checked here.
fn drop_column(
    client: &mut GnitzClient,
    schema_name: &str,
    source: &ObjectName,
    tbl_if_exists: bool,
    col_if_exists: bool,
    column_names: &[Ident],
    drop_behavior: Option<DropBehavior>,
) -> Result<SqlResult, GnitzSqlError> {
    if matches!(drop_behavior, Some(DropBehavior::Cascade)) {
        return Err(GnitzSqlError::Unsupported(
            "ALTER TABLE DROP COLUMN CASCADE is not supported (gnitz has no cascadable dependent objects)".to_string(),
        ));
    }
    // sqlparser collapses `DROP a, b` into one op with a multi-name vec.
    if column_names.len() != 1 {
        return Err(GnitzSqlError::Unsupported(
            "ALTER TABLE DROP COLUMN supports exactly one column per statement".to_string(),
        ));
    }
    let col_name = &column_names[0].value;
    let source_name = extract_name(source, "ALTER TABLE")?;
    let Some((tid, schema)) =
        resolve_alter_base_table_with_schema(client, schema_name, &source_name, tbl_if_exists, "DROP COLUMN")?
    else {
        return Ok(altered("column", col_name.clone()));
    };

    let Some(col_idx) = find_unique_column(&schema.columns, col_name)? else {
        if col_if_exists {
            return Ok(altered("column", col_name.clone()));
        }
        return Err(missing("column", schema_name, col_name));
    };
    let cd = &schema.columns[col_idx];

    if schema.pk_cols.contains(&col_idx) {
        return Err(GnitzSqlError::Unsupported(format!(
            "cannot DROP COLUMN '{col_name}': it is part of the primary key"
        )));
    }
    if cd.is_serial {
        return Err(GnitzSqlError::Unsupported(format!(
            "cannot DROP COLUMN '{col_name}': it is a SERIAL column"
        )));
    }
    if cd.fk_table_id != 0 {
        return Err(GnitzSqlError::Unsupported(format!(
            "cannot DROP COLUMN '{col_name}': it carries a foreign key; drop the foreign key first"
        )));
    }
    // Reject a column covered by a secondary index (best-effort UX — a concurrent
    // CREATE INDEX could still slip one in; the engine does not re-guard, but a
    // leftover index over the still-NOT-NULL hidden column is harmless and
    // dormant). One GET_INDICES round-trip; `cols` are physical indices in the
    // same space as `col_idx`, so a composite-index member is caught.
    let indexes = client.table_indexes(tid).map_err(GnitzSqlError::Exec)?;
    if indexes.iter().any(|im| im.cols.as_slice().contains(&(col_idx as u32))) {
        return Err(GnitzSqlError::Unsupported(format!(
            "cannot DROP COLUMN '{col_name}': it is covered by a secondary index; DROP the index first"
        )));
    }

    client.alter_drop_column(tid, col_idx).map_err(GnitzSqlError::Exec)?;
    Ok(altered("column", col_name.clone()))
}

/// `ALTER TABLE <t> ALTER COLUMN <c> DROP NOT NULL` — flips the column's
/// `is_nullable 0→1`, which (if the table was all-non-null-fixed-int) swaps the
/// engine comparator `FixedIntNonnull → Generic`. `t` must be a base table; a PK
/// column is rejected (PK columns are non-nullable). The dependent-view RESTRICT
/// is enforced engine-side.
fn drop_not_null(
    client: &mut GnitzClient,
    schema_name: &str,
    source: &ObjectName,
    tbl_if_exists: bool,
    col_name: &str,
) -> Result<SqlResult, GnitzSqlError> {
    let source_name = extract_name(source, "ALTER TABLE")?;
    let Some((tid, schema)) =
        resolve_alter_base_table_with_schema(client, schema_name, &source_name, tbl_if_exists, "DROP NOT NULL")?
    else {
        return Ok(altered("column", col_name.to_string()));
    };
    let Some(col_idx) = find_unique_column(&schema.columns, col_name)? else {
        return Err(missing("column", schema_name, col_name));
    };
    if schema.pk_cols.contains(&col_idx) {
        return Err(GnitzSqlError::Unsupported(format!(
            "cannot DROP NOT NULL on '{col_name}': a primary-key column is non-nullable"
        )));
    }
    client.alter_drop_not_null(tid, col_idx).map_err(GnitzSqlError::Exec)?;
    Ok(altered("column", col_name.to_string()))
}

/// `ALTER TABLE <t> ADD CONSTRAINT [n] UNIQUE (cols)` — maps to the CREATE UNIQUE
/// INDEX path, returning `IndexCreated { index_id }` (uniform create surface).
/// Only a `UNIQUE` constraint is honored; `NOT VALID` and every other constraint
/// kind are rejected.
fn add_constraint(
    client: &mut GnitzClient,
    schema_name: &str,
    source: &ObjectName,
    if_exists: bool,
    constraint: &TableConstraint,
    not_valid: bool,
    binder: &mut Binder<'_>,
) -> Result<SqlResult, GnitzSqlError> {
    if not_valid {
        return Err(GnitzSqlError::Unsupported(
            "ADD CONSTRAINT ... NOT VALID is not supported".to_string(),
        ));
    }
    let TableConstraint::Unique(u) = constraint else {
        return Err(GnitzSqlError::Unsupported(
            "ADD CONSTRAINT: only UNIQUE constraints are supported".to_string(),
        ));
    };
    // Same field rejections as an inline CREATE TABLE … UNIQUE: a MySQL
    // `UNIQUE INDEX <idx>` name would be undroppable (DROP CONSTRAINT resolves the
    // constraint-derived name); NULLS NOT DISTINCT / DEFERRABLE are unimplemented.
    reject_unhonored_unique_fields(u, "ADD CONSTRAINT")?;

    let source_name = extract_name(source, "ALTER TABLE")?;
    if resolve_alter_base_table(client, schema_name, &source_name, if_exists, "ADD CONSTRAINT")?.is_none() {
        return Ok(altered("constraint", source_name));
    }

    // `u.name` (the CONSTRAINT name) becomes the created index's name; `None`
    // auto-generates one (`create_index_core` via default_index_name).
    let explicit_name = u.name.as_ref().map(|n| n.value.clone());
    super::table::create_index_core(
        client,
        schema_name,
        binder,
        &source_name,
        &u.columns,
        true,
        explicit_name,
        "ADD CONSTRAINT",
    )
}

/// `ALTER TABLE <t> DROP CONSTRAINT [IF EXISTS] <n>` — maps to the DROP INDEX
/// path (drop the index named `n`). Rejects a `__fk_`-infixed name up front so
/// `DROP CONSTRAINT __fk_…` fails cleanly client-side.
fn drop_constraint(
    client: &mut GnitzClient,
    schema_name: &str,
    source: &ObjectName,
    tbl_if_exists: bool,
    name: &str,
    constraint_if_exists: bool,
) -> Result<SqlResult, GnitzSqlError> {
    validate_user_index_name(name)?;
    let source_name = extract_name(source, "ALTER TABLE")?;
    if resolve_alter_base_table(client, schema_name, &source_name, tbl_if_exists, "DROP CONSTRAINT")?.is_none() {
        return Ok(altered("constraint", name.to_string()));
    }
    client
        .drop_index_by_name(name, constraint_if_exists)
        .map_err(GnitzSqlError::Exec)?;
    Ok(altered("constraint", name.to_string()))
}

// --- shared helpers ---

/// Resolve the ALTER target as a writable base table (validated name, view and
/// system relation rejected). `Ok(None)` is the `IF EXISTS` no-op — the target
/// does not exist and the caller returns its success result untouched.
fn resolve_alter_base_table(
    client: &mut GnitzClient,
    schema_name: &str,
    source_name: &str,
    if_exists: bool,
    op: &str,
) -> Result<Option<u64>, GnitzSqlError> {
    validate_user_name(source_name)?;
    match client
        .resolve_relation_kind(schema_name, source_name)
        .map_err(GnitzSqlError::Exec)?
    {
        None if if_exists => Ok(None),
        None => Err(missing("Table", schema_name, source_name)),
        Some((_, true)) => Err(require_base_table(source_name, op)),
        Some((id, false)) => {
            reject_system_relation(id)?;
            Ok(Some(id))
        }
    }
}

/// As [`resolve_alter_base_table`], but also loads the resolved base table's full
/// physical schema — columns include any hidden (dropped) slot and `pk_cols` are
/// physical indices — for the DROP COLUMN / DROP NOT NULL column-level guards.
/// `Ok(None)` is the `IF EXISTS` no-op.
fn resolve_alter_base_table_with_schema(
    client: &mut GnitzClient,
    schema_name: &str,
    source_name: &str,
    if_exists: bool,
    op: &str,
) -> Result<Option<(u64, gnitz_core::Schema)>, GnitzSqlError> {
    if resolve_alter_base_table(client, schema_name, source_name, if_exists, op)?.is_none() {
        return Ok(None);
    }
    let (tid, schema) = client
        .resolve_table_id(schema_name, source_name)
        .map_err(GnitzSqlError::Exec)?;
    Ok(Some((tid, schema)))
}

/// Reject an ALTER of a system relation (id below the user band). The engine's
/// §3.3 system-range precheck is the backstop; this is the friendly client error.
fn reject_system_relation(id: u64) -> Result<(), GnitzSqlError> {
    if id < gnitz_core::FIRST_USER_TABLE_ID {
        return Err(GnitzSqlError::Unsupported(format!(
            "cannot ALTER a system relation (id {id})"
        )));
    }
    Ok(())
}

/// The RENAME target's new name (last part). A schema qualifier that differs from
/// the source's schema is a cross-schema move — rejected (it would break qname
/// bookkeeping and hidden-segment scoping).
fn rename_target_name(target: &ObjectName, source_schema: &str) -> Result<String, GnitzSqlError> {
    let parts: Vec<&str> = target
        .0
        .iter()
        .filter_map(|p| p.as_ident())
        .map(|i| i.value.as_str())
        .collect();
    match parts.as_slice() {
        [n] => Ok((*n).to_string()),
        [s, n] => {
            if !s.eq_ignore_ascii_case(source_schema) {
                return Err(GnitzSqlError::Unsupported(format!(
                    "cross-schema RENAME is not supported (target schema '{s}' differs from '{source_schema}')"
                )));
            }
            Ok((*n).to_string())
        }
        _ => Err(GnitzSqlError::Bind("RENAME TO: unsupported qualified name".to_string())),
    }
}

fn relation_kind(is_view: bool) -> &'static str {
    if is_view {
        "view"
    } else {
        "table"
    }
}

/// The `Altered` result — also the IF-EXISTS no-op success (nothing to alter,
/// but the statement succeeds), carrying the intended name.
fn altered(object: &str, name: String) -> SqlResult {
    SqlResult::Altered {
        object: object.to_string(),
        name,
    }
}

fn missing(what: &str, schema: &str, name: &str) -> GnitzSqlError {
    GnitzSqlError::Bind(format!("{what} '{schema}.{name}' does not exist"))
}

fn require_base_table(name: &str, op: &str) -> GnitzSqlError {
    GnitzSqlError::Unsupported(format!("'{name}' is a view; ALTER TABLE {op} requires a base table"))
}
