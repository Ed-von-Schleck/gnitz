//! `ALTER TABLE`: rename table/view/column, ADD/DROP COLUMN, DROP NOT NULL,
//! ADD/DROP CONSTRAINT (mapped to the CREATE/DROP INDEX paths). `ALTER VIEW … AS`
//! lives in `ddl::view` (it recompiles a query). Every rule that reads
//! the catalog is the engine precheck's.

use super::guard::{reject_unhonored_column_options, reject_unhonored_unique_fields, ColumnOptionSite};
use crate::ast_util::extract_object_name;
use crate::bind::{find_unique_column, require_column};
use crate::error::{missing_relation, reject_if, unsupported_clause, GnitzSqlError};
use crate::rules::{canonical_user_name, require_class, validate_user_name, ClassWant};
use crate::types::column_def;
use crate::SqlResult;
use gnitz_core::GnitzClient;
use sqlparser::ast::{
    AlterColumnOperation, AlterTable, AlterTableOperation, DropBehavior, IndexColumn, RenameTableNameKind,
    TableConstraint,
};

/// One ALTER TABLE operation with every check that needs no catalog already run,
/// carrying the operands it uses.
enum Action<'a> {
    RenameRelation {
        new_name: String,
    },
    RenameColumn {
        old: &'a str,
        new: &'a str,
    },
    AddColumn(gnitz_wire::ColumnDef),
    DropColumn {
        name: &'a str,
        if_exists: bool,
    },
    DropNotNull {
        name: &'a str,
    },
    AddUnique {
        columns: &'a [IndexColumn],
        explicit_name: Option<String>,
    },
    DropConstraint {
        name: &'a str,
        if_exists: bool,
    },
}

/// Reject every `ALTER TABLE` envelope clause gnitz does not honor: `ONLY`
/// (silently scopes out partition children), a Hive `SET LOCATION`, `ON CLUSTER`,
/// and a non-`None` `table_type` (Iceberg/Dynamic — a different storage engine).
/// The operation itself is [`parse`]'s. Exhaustive destructure (no `..`):
/// a future sqlparser field stops the build until it is classified.
fn reject_unhonored_alter_table_clauses(alter: &sqlparser::ast::AlterTable) -> Result<(), GnitzSqlError> {
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

pub(crate) fn execute_alter_table(
    client: &mut GnitzClient,
    schema_name: &str,
    alter: &AlterTable,
) -> Result<SqlResult, GnitzSqlError> {
    reject_unhonored_alter_table_clauses(alter)?;
    let [operation] = alter.operations.as_slice() else {
        return Err(unsupported_clause(
            "ALTER TABLE",
            "more than one operation per statement",
        ));
    };
    let source_name = extract_object_name(&alter.name, schema_name, "ALTER TABLE")?;
    let (ctx, action) = parse(operation, schema_name)?;

    let Some(rel) = client.resolve(schema_name, &source_name)? else {
        return if alter.if_exists {
            Ok(SqlResult::Ddl)
        } else {
            Err(missing_relation(schema_name, &source_name))
        };
    };
    // `RENAME TO` accepts a view too, as Postgres does: views bind sources by id
    // and columns by ordinal, so a rename is safe under any class.
    if !matches!(action, Action::RenameRelation { .. }) {
        require_class(&rel, &source_name, ClassWant::BaseTable, ctx)?;
    }

    let cols = &rel.schema.columns;
    match action {
        Action::RenameRelation { new_name } => client.alter_rename_relation(&rel, &new_name)?,
        Action::RenameColumn { old, new } => client.alter_rename_column(rel.tid, require_column(cols, old)?, new)?,
        Action::AddColumn(def) => client.alter_add_column(&rel, &def)?,
        Action::DropColumn { name, if_exists: true } if find_unique_column(cols, name)?.is_none() => {}
        Action::DropColumn { name, .. } => client.alter_drop_column(rel.tid, require_column(cols, name)?)?,
        Action::DropNotNull { name } => client.alter_drop_not_null(rel.tid, require_column(cols, name)?)?,
        Action::AddUnique { columns, explicit_name } => {
            return super::table::create_index_core(
                client,
                schema_name,
                &rel,
                &super::table::IndexRequest {
                    table_name: &source_name,
                    columns,
                    explicit_name,
                    site: super::table::IndexSite::AddConstraint,
                },
            );
        }
        Action::DropConstraint { name, if_exists } => client.drop_unique_constraint(rel.tid, name, if_exists)?,
    }
    Ok(SqlResult::Ddl)
}

/// The operation's context and [`Action`], or the rejection the statement alone
/// earns.
fn parse<'a>(
    operation: &'a AlterTableOperation,
    schema_name: &str,
) -> Result<(&'static str, Action<'a>), GnitzSqlError> {
    match operation {
        AlterTableOperation::RenameTable { table_name } => {
            const CTX: &str = "ALTER TABLE RENAME TO";
            // `RENAME TO` and (some dialects') `RENAME AS` both mean rename-to.
            let (RenameTableNameKind::To(target) | RenameTableNameKind::As(target)) = table_name;
            let new_name = extract_object_name(target, schema_name, "ALTER TABLE")?;
            Ok((CTX, Action::RenameRelation { new_name }))
        }
        AlterTableOperation::RenameColumn { old_column_name, new_column_name } => Ok((
            "ALTER TABLE RENAME COLUMN",
            Action::RenameColumn {
                old: &old_column_name.value,
                new: &new_column_name.value,
            },
        )),
        AlterTableOperation::AddColumn {
            // Inert: `ADD c INT` and `ADD COLUMN c INT` mean the same thing.
            column_keyword: _,
            if_not_exists,
            column_def: col,
            column_position,
        } => {
            const CTX: &str = "ALTER TABLE ADD COLUMN";
            reject_if(*if_not_exists, CTX, "IF NOT EXISTS")?;
            reject_if(
                column_position.is_some(),
                CTX,
                "FIRST/AFTER (a column is always appended last)",
            )?;
            reject_unhonored_column_options(col, ColumnOptionSite::AddColumn)?;
            let (def, serial) = column_def(col)?;
            reject_if(serial, CTX, "SERIAL")?;
            Ok((CTX, Action::AddColumn(def)))
        }
        AlterTableOperation::DropColumn {
            // Inert: `DROP c` and `DROP COLUMN c` mean the same thing.
            has_column_keyword: _,
            column_names,
            if_exists,
            drop_behavior,
        } => {
            const CTX: &str = "ALTER TABLE DROP COLUMN";
            reject_if(matches!(drop_behavior, Some(DropBehavior::Cascade)), CTX, "CASCADE")?;
            // `GenericDialect` parses exactly one column here.
            let [col] = column_names.as_slice() else {
                return Err(GnitzSqlError::Internal(format!(
                    "DROP COLUMN parsed {} column names",
                    column_names.len()
                )));
            };
            Ok((CTX, Action::DropColumn { name: &col.value, if_exists: *if_exists }))
        }
        AlterTableOperation::AlterColumn { column_name, op } => {
            use AlterColumnOperation as Op;
            let msg = match op {
                Op::DropNotNull => {
                    return Ok((
                        "ALTER TABLE ALTER COLUMN DROP NOT NULL",
                        Action::DropNotNull { name: &column_name.value },
                    ))
                }
                Op::SetNotNull => "ALTER COLUMN SET NOT NULL is not supported (needs a full-table validation scan)",
                Op::SetDataType { .. } => "ALTER COLUMN SET DATA TYPE is not supported",
                Op::SetDefault { .. } => "ALTER COLUMN SET DEFAULT is not supported (no column defaults in gnitz)",
                Op::DropDefault => "ALTER COLUMN DROP DEFAULT is not supported (no column defaults in gnitz)",
                Op::AddGenerated { .. } => {
                    "ALTER COLUMN ADD GENERATED is not supported (no generated columns in gnitz)"
                }
            };
            Err(GnitzSqlError::Rejected(msg.to_string()))
        }
        AlterTableOperation::AddConstraint { constraint, not_valid } => {
            const CTX: &str = "ALTER TABLE ADD CONSTRAINT";
            reject_if(*not_valid, CTX, "NOT VALID")?;
            let TableConstraint::Unique(u) = constraint else {
                return Err(GnitzSqlError::Rejected(
                    "ADD CONSTRAINT: only UNIQUE constraints are supported".to_string(),
                ));
            };
            // Same field rejections as an inline CREATE TABLE … UNIQUE.
            reject_unhonored_unique_fields(u, "ADD CONSTRAINT")?;
            // The CONSTRAINT name becomes the index's; `None` auto-generates one.
            Ok((
                CTX,
                Action::AddUnique {
                    columns: &u.columns,
                    explicit_name: u.name.as_ref().map(|n| canonical_user_name(&n.value)).transpose()?,
                },
            ))
        }
        AlterTableOperation::DropConstraint { if_exists, name, drop_behavior } => {
            const CTX: &str = "ALTER TABLE DROP CONSTRAINT";
            reject_if(matches!(drop_behavior, Some(DropBehavior::Cascade)), CTX, "CASCADE")?;
            validate_user_name(&name.value)?;
            Ok((CTX, Action::DropConstraint { name: &name.value, if_exists: *if_exists }))
        }
        _ => Err(unsupported_clause("ALTER TABLE", &operation.to_string())),
    }
}
