//! `ALTER TABLE` operation dispatch: rename table/view/column, ADD/DROP COLUMN,
//! DROP NOT NULL, ADD/DROP CONSTRAINT (mapped to the CREATE/DROP INDEX paths),
//! and clean rejections for every operation not yet supported. `ALTER VIEW … AS` lives in
//! `crate::hir::create` (it recompiles a query). Every supported op is one
//! catalog-only `push_ddl` through the `gnitz-core` client.

use crate::ast_util::extract_object_name;
use crate::bind::find_unique_column;
use crate::error::{missing_relation as missing, reject_if, GnitzSqlError};
use crate::types::{serial_underlying, sql_type_to_typecode};
use crate::validate::{
    reject_unhonored_alter_table_clauses, reject_unhonored_column_options, reject_unhonored_unique_fields,
    require_class, validate_user_name, ClassWant, ColumnOptionSite,
};
use crate::SqlResult;
use gnitz_core::{GnitzClient, RelDescriptor};
use sqlparser::ast::{
    AlterColumnOperation, AlterTable, AlterTableOperation, DropBehavior, Ident, ObjectName, RenameTableNameKind,
    TableConstraint,
};
use std::sync::Arc;

pub(crate) fn execute_alter_table(
    client: &mut GnitzClient,
    schema_name: &str,
    alter: &AlterTable,
) -> Result<SqlResult, GnitzSqlError> {
    // First, so the index below is total: the guard is what rejects the multi-op
    // comma form, and a clause rejection outranks a bad name or missing relation.
    reject_unhonored_alter_table_clauses(alter)?;
    // The target name is the statement's, not the operation's. Only the name is
    // hoisted — each arm resolves behind its own clause rejections, so a clause
    // rejection still outranks a missing relation.
    let source_name = extract_object_name(&alter.name, schema_name, "ALTER TABLE")?;
    validate_user_name(&source_name)?;
    match &alter.operations[0] {
        AlterTableOperation::RenameTable { table_name } => {
            // `RENAME TO` and (some dialects') `RENAME AS` both mean rename-to.
            let target = match table_name {
                RenameTableNameKind::To(n) | RenameTableNameKind::As(n) => n,
            };
            rename_relation(client, schema_name, &source_name, alter.if_exists, target)
        }
        AlterTableOperation::RenameColumn { old_column_name, new_column_name } => rename_column(
            client,
            schema_name,
            &source_name,
            alter.if_exists,
            &old_column_name.value,
            &new_column_name.value,
        ),
        AlterTableOperation::AddConstraint { constraint, not_valid } => {
            reject_if(*not_valid, "ALTER TABLE ADD CONSTRAINT", "NOT VALID")?;
            add_constraint(client, schema_name, &source_name, alter.if_exists, constraint)
        }
        AlterTableOperation::DropConstraint { if_exists, name, drop_behavior: _ } => drop_constraint(
            client,
            schema_name,
            &source_name,
            alter.if_exists,
            &name.value,
            *if_exists,
        ),
        AlterTableOperation::DropColumn {
            // Inert: `DROP c` and `DROP COLUMN c` mean the same thing.
            has_column_keyword: _,
            column_names,
            if_exists: col_if_exists,
            drop_behavior,
        } => drop_column(
            client,
            schema_name,
            &source_name,
            alter.if_exists,
            *col_if_exists,
            column_names,
            *drop_behavior,
        ),
        AlterTableOperation::AddColumn {
            // Inert: `ADD c INT` and `ADD COLUMN c INT` mean the same thing.
            column_keyword: _,
            if_not_exists,
            column_def,
            column_position,
        } => {
            reject_if(*if_not_exists, "ALTER TABLE ADD COLUMN", "IF NOT EXISTS")?;
            reject_if(
                column_position.is_some(),
                "ALTER TABLE ADD COLUMN",
                "FIRST/AFTER (a column is always appended last)",
            )?;
            add_column(client, schema_name, &source_name, alter.if_exists, column_def)
        }
        // Destructured per-variant so a newly supported operation is an additive
        // arm split, and with no plan path in any message.
        AlterTableOperation::AlterColumn { column_name, op } => {
            use AlterColumnOperation as Op;
            let msg = match op {
                Op::DropNotNull => {
                    return drop_not_null(client, schema_name, &source_name, alter.if_exists, &column_name.value)
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
/// is always safe with dependents (no dependent-view guard). No class assertion
/// for the same reason.
fn rename_relation(
    client: &mut GnitzClient,
    schema_name: &str,
    source_name: &str,
    if_exists: bool,
    target: &ObjectName,
) -> Result<SqlResult, GnitzSqlError> {
    let new_name = extract_object_name(target, schema_name, "ALTER TABLE")?;
    let Some(rel) = resolve_alter_target(client, schema_name, source_name, if_exists)? else {
        return Ok(altered("table", new_name));
    };
    validate_user_name(&new_name)?;

    client.alter_rename_relation(schema_name, source_name, &new_name)?;
    Ok(altered(rel.class.noun(), new_name))
}

/// `ALTER TABLE <t> RENAME COLUMN <a> TO <b>` — `t` must be a base table (a view
/// is read-only and column-bound by ordinal).
fn rename_column(
    client: &mut GnitzClient,
    schema_name: &str,
    source_name: &str,
    if_exists: bool,
    old_col: &str,
    new_col: &str,
) -> Result<SqlResult, GnitzSqlError> {
    let Some(rel) = resolve_alter_base_table(client, schema_name, source_name, if_exists, "RENAME COLUMN")? else {
        return Ok(altered("column", new_col.to_string()));
    };
    let Some(col_idx) = find_unique_column(&rel.schema.columns, old_col)? else {
        return Err(missing("column", schema_name, old_col));
    };
    client.alter_rename_column(rel.tid, col_idx, new_col)?;
    Ok(altered("column", new_col.to_string()))
}

/// `ALTER TABLE <t> ADD [COLUMN] <c> <type>` — appends one **nullable** payload
/// column at the end of `t`'s physical layout (columns hidden by a previous DROP
/// COLUMN included). Existing rows read it as NULL. `t` must be a base table;
/// the dependent-view RESTRICT and the trailing-position, `MAX_COLUMNS` and
/// field-shape checks are enforced engine-side.
///
/// Everything that would need a value for the existing rows or a second catalog
/// object is rejected: NOT NULL (it would need a full-table validation scan),
/// SERIAL, the options `plan_create_table` already rejects (DEFAULT, CHECK,
/// GENERATED, IDENTITY, COLLATE, …), and inline PRIMARY KEY / UNIQUE / REFERENCES.
fn add_column(
    client: &mut GnitzClient,
    schema_name: &str,
    source_name: &str,
    tbl_if_exists: bool,
    column_def: &sqlparser::ast::ColumnDef,
) -> Result<SqlResult, GnitzSqlError> {
    reject_unhonored_column_options(column_def, ColumnOptionSite::AddColumn)?;
    // A SERIAL column's generator is seeded from the table's live rows, which an
    // append has none of; named here so it does not fall out as "unsupported type".
    if serial_underlying(&column_def.data_type).is_some() {
        return Err(GnitzSqlError::Unsupported(
            "ALTER TABLE ADD COLUMN … SERIAL is not supported".to_string(),
        ));
    }

    let col_name = &column_def.name.value;
    // No identifier validation on the column: `validate_user_name` guards
    // *relation* names only, and neither CREATE TABLE nor RENAME COLUMN
    // validates a column one.
    let Some(rel) = resolve_alter_base_table(client, schema_name, source_name, tbl_if_exists, "ADD COLUMN")? else {
        return Ok(altered("column", col_name.clone()));
    };
    let tid = rel.tid;

    let type_code = sql_type_to_typecode(&column_def.data_type)?;
    let def = gnitz_core::ColumnDef::new(col_name, type_code, /* is_nullable */ true);
    client.alter_add_column(tid, &def)?;
    Ok(altered("column", col_name.clone()))
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
    source_name: &str,
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
    // `GenericDialect` parses exactly one column here.
    let [col] = column_names else {
        return Err(GnitzSqlError::Internal(format!(
            "DROP COLUMN parsed {} column names",
            column_names.len()
        )));
    };
    let col_name = &col.value;
    let Some(rel) = resolve_alter_base_table(client, schema_name, source_name, tbl_if_exists, "DROP COLUMN")? else {
        return Ok(altered("column", col_name.clone()));
    };
    let (tid, schema) = (rel.tid, &rel.schema);

    let Some(col_idx) = find_unique_column(&schema.columns, col_name)? else {
        if col_if_exists {
            return Ok(altered("column", col_name.clone()));
        }
        return Err(missing("column", schema_name, col_name));
    };
    let cd = &schema.columns[col_idx];

    if schema.is_pk_col(col_idx) {
        return Err(GnitzSqlError::Unsupported(format!(
            "cannot DROP COLUMN '{col_name}': it is part of the primary key"
        )));
    }
    if cd.is_serial {
        return Err(GnitzSqlError::Unsupported(format!(
            "cannot DROP COLUMN '{col_name}': it is a SERIAL column"
        )));
    }
    if cd.fk.is_some() {
        return Err(GnitzSqlError::Unsupported(format!(
            "cannot DROP COLUMN '{col_name}': it carries a foreign key; drop the foreign key first"
        )));
    }
    // Reject a column covered by a secondary index (best-effort UX — a concurrent
    // CREATE INDEX could still slip one in; the engine does not re-guard, but a
    // leftover index over the still-NOT-NULL hidden column is harmless and
    // dormant). Served from the statement's resolved descriptor; `cols` are
    // physical indices in the same space as `col_idx`, so a composite-index
    // member is caught.
    if rel
        .indexes
        .iter()
        .any(|im| im.cols.as_slice().contains(&(col_idx as u32)))
    {
        return Err(GnitzSqlError::Unsupported(format!(
            "cannot DROP COLUMN '{col_name}': it is covered by a secondary index; DROP the index first"
        )));
    }

    client.alter_drop_column(tid, col_idx)?;
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
    source_name: &str,
    tbl_if_exists: bool,
    col_name: &str,
) -> Result<SqlResult, GnitzSqlError> {
    let Some(rel) = resolve_alter_base_table(client, schema_name, source_name, tbl_if_exists, "DROP NOT NULL")? else {
        return Ok(altered("column", col_name.to_string()));
    };
    let (tid, schema) = (rel.tid, &rel.schema);
    let Some(col_idx) = find_unique_column(&schema.columns, col_name)? else {
        return Err(missing("column", schema_name, col_name));
    };
    if schema.is_pk_col(col_idx) {
        return Err(GnitzSqlError::Unsupported(format!(
            "cannot DROP NOT NULL on '{col_name}': a primary-key column is non-nullable"
        )));
    }
    client.alter_drop_not_null(tid, col_idx)?;
    Ok(altered("column", col_name.to_string()))
}

/// `ALTER TABLE <t> ADD CONSTRAINT [n] UNIQUE (cols)` — maps to the CREATE UNIQUE
/// INDEX path, returning `IndexCreated { index_id }` (uniform create surface).
/// Only a `UNIQUE` constraint is honored; every other constraint kind is rejected.
fn add_constraint(
    client: &mut GnitzClient,
    schema_name: &str,
    source_name: &str,
    if_exists: bool,
    constraint: &TableConstraint,
) -> Result<SqlResult, GnitzSqlError> {
    let TableConstraint::Unique(u) = constraint else {
        return Err(GnitzSqlError::Unsupported(
            "ADD CONSTRAINT: only UNIQUE constraints are supported".to_string(),
        ));
    };
    // Same field rejections as an inline CREATE TABLE … UNIQUE: a MySQL
    // `UNIQUE INDEX <idx>` name would be undroppable (DROP CONSTRAINT resolves the
    // constraint-derived name); NULLS NOT DISTINCT / DEFERRABLE are unimplemented.
    reject_unhonored_unique_fields(u, "ADD CONSTRAINT")?;

    // `u.name` (the CONSTRAINT name) becomes the created index's name; `None`
    // auto-generates one (`create_index_core` via default_index_name).
    let explicit_name = u.name.as_ref().map(|n| n.value.clone());
    let Some(target) = resolve_alter_base_table(client, schema_name, source_name, if_exists, "ADD CONSTRAINT")? else {
        return Ok(altered("constraint", explicit_name.unwrap_or_default()));
    };

    super::table::create_index_core(
        client,
        schema_name,
        &target,
        &super::table::IndexRequest {
            table_name: source_name,
            columns: &u.columns,
            explicit_name,
            site: super::table::IndexSite::AddConstraint,
        },
    )
}

/// `ALTER TABLE <t> DROP CONSTRAINT [IF EXISTS] <n>` — maps to the DROP INDEX
/// path (drop the index named `n`).
fn drop_constraint(
    client: &mut GnitzClient,
    schema_name: &str,
    source_name: &str,
    tbl_if_exists: bool,
    name: &str,
    constraint_if_exists: bool,
) -> Result<SqlResult, GnitzSqlError> {
    validate_user_name(name)?;
    if resolve_alter_base_table(client, schema_name, source_name, tbl_if_exists, "DROP CONSTRAINT")?.is_none() {
        return Ok(altered("constraint", name.to_string()));
    }
    client.drop_indexes_by_name(&[name], constraint_if_exists)?;
    Ok(altered("constraint", name.to_string()))
}

// --- shared helpers ---

/// The ALTER target: resolved, and refused if it is a system relation. Held to no
/// class rule — `RENAME TO` accepts a view. `Ok(None)` is the `IF EXISTS` no-op.
fn resolve_alter_target(
    client: &mut GnitzClient,
    schema_name: &str,
    source_name: &str,
    if_exists: bool,
) -> Result<Option<Arc<RelDescriptor>>, GnitzSqlError> {
    let Some(rel) = client.resolve(schema_name, source_name)? else {
        return if if_exists {
            Ok(None)
        } else {
            Err(missing("Table", schema_name, source_name))
        };
    };
    reject_system_relation(rel.tid)?;
    Ok(Some(rel))
}

/// [`resolve_alter_target`] restricted to a base table: no other class has a
/// column shape to ALTER. The descriptor's schema is the physical one the column
/// guards index into — hidden (dropped) slots included, `pk_cols` physical.
fn resolve_alter_base_table(
    client: &mut GnitzClient,
    schema_name: &str,
    source_name: &str,
    if_exists: bool,
    op: &str,
) -> Result<Option<Arc<RelDescriptor>>, GnitzSqlError> {
    let Some(rel) = resolve_alter_target(client, schema_name, source_name, if_exists)? else {
        return Ok(None);
    };
    require_class(&rel, source_name, ClassWant::BaseTable, &format!("ALTER TABLE {op}"))?;
    Ok(Some(rel))
}

/// Reject an ALTER of a system relation (id below the user band). The engine's
/// own system-id precheck is the backstop; this is the friendly client error.
fn reject_system_relation(id: u64) -> Result<(), GnitzSqlError> {
    if id < gnitz_core::FIRST_USER_TABLE_ID {
        return Err(GnitzSqlError::Unsupported(format!(
            "cannot ALTER a system relation (id {id})"
        )));
    }
    Ok(())
}

/// The `Altered` result — also the IF-EXISTS no-op success (nothing to alter,
/// but the statement succeeds), carrying the intended name.
fn altered(object: &str, name: String) -> SqlResult {
    SqlResult::Altered { object: object.to_string(), name }
}
