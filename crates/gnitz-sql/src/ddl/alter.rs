//! `ALTER TABLE`: rename table/view/column, ADD/DROP COLUMN, DROP NOT NULL,
//! ADD/DROP CONSTRAINT (mapped to the CREATE/DROP INDEX paths). `ALTER VIEW … AS`
//! lives in `ddl::view` (it recompiles a query). The statement is read whole
//! before the catalog is; the catalog is read to resolve the relation and its
//! columns, to decide IF [NOT] EXISTS and the class gate. Whether the change is
//! admissible is the engine precheck's.

use super::guard::{ColumnConstraint, read_column, reject_unhonored_unique_fields};
use super::table::{index_cols, index_column_names};
use crate::error::{GnitzSqlError, missing_relation, reject_if, unsupported_clause};
use crate::names::extract_object_name;
use crate::rules::{ClassWant, canonical_user_name, find_unique_column, require_class, require_column};
use gnitz_core::{ColumnChange, GnitzClient, RelName};
use sqlparser::ast::{
    AlterColumnOperation, AlterTable, AlterTableOperation, DropBehavior, RenameTableNameKind, TableConstraint,
};

/// One ALTER TABLE operation with every check that needs no catalog already run,
/// carrying the operands it uses.
enum Action<'a> {
    RenameRelation {
        new_name: String,
    },
    /// A change to one existing column's catalog row. `if_exists` is set only
    /// by `DROP COLUMN IF EXISTS`.
    AlterColumn {
        name: &'a str,
        change: ColumnChange<'a>,
        if_exists: bool,
    },
    AddColumn {
        def: gnitz_wire::ColumnDef,
        if_not_exists: bool,
    },
    AddUnique {
        columns: Vec<&'a str>,
        explicit_name: Option<String>,
    },
    DropConstraint {
        name: String,
        if_exists: bool,
    },
}

/// Reject every `ALTER TABLE` envelope clause gnitz does not honor: `ONLY`
/// (silently scopes out partition children), a Hive `SET LOCATION`, `ON CLUSTER`,
/// and a non-`None` `table_type` (Iceberg/Dynamic — a different storage engine).
/// The operation itself is [`parse`]'s.
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

pub(crate) async fn execute_alter_table(
    client: &mut GnitzClient,
    schema_name: &str,
    alter: &AlterTable,
) -> Result<(), GnitzSqlError> {
    reject_unhonored_alter_table_clauses(alter)?;
    let [operation] = alter.operations.as_slice() else {
        return Err(unsupported_clause(
            "ALTER TABLE",
            "more than one operation per statement",
        ));
    };
    let source_name = extract_object_name(&alter.name, schema_name, "ALTER TABLE")?;
    let (ctx, action) = parse(operation, &source_name)?;

    let Some(rel) = client.resolve(&source_name).await? else {
        return if alter.if_exists {
            Ok(())
        } else {
            Err(missing_relation(&source_name))
        };
    };
    // `RENAME TO` accepts a view too, as Postgres does: views bind sources by id
    // and columns by ordinal, so a rename is safe under any class.
    if !matches!(action, Action::RenameRelation { .. }) {
        require_class(&rel, &source_name, ClassWant::BaseTable, ctx)?;
    }

    let cols = rel.schema.columns();
    match action {
        Action::RenameRelation { new_name } => client.alter_rename_relation(&rel, &new_name).await?,
        Action::AddColumn { def, if_not_exists: true } if find_unique_column(cols, &def.name)?.is_some() => {}
        Action::AddColumn { def, .. } => client.alter_add_column(&rel, &def).await?,
        Action::AlterColumn { name, if_exists: true, .. } if find_unique_column(cols, name)?.is_none() => {}
        Action::AlterColumn { name, change, .. } => {
            client
                .alter_column(rel.tid, require_column(cols, name)?, change)
                .await?
        }
        Action::AddUnique { columns, explicit_name } => {
            let key = index_cols(&rel.schema, &columns, "ADD CONSTRAINT")?;
            client
                .create_index(&source_name, &rel, key, explicit_name.as_deref(), true)
                .await?;
        }
        Action::DropConstraint { name, if_exists } => client.drop_unique_constraint(rel.tid, &name, if_exists).await?,
    }
    Ok(())
}

/// The operation's context and [`Action`] on `source`, or the rejection the
/// statement alone earns.
fn parse<'a>(
    operation: &'a AlterTableOperation,
    source: &RelName,
) -> Result<(&'static str, Action<'a>), GnitzSqlError> {
    match operation {
        AlterTableOperation::RenameTable { table_name } => {
            const CTX: &str = "ALTER TABLE RENAME TO";
            // `RENAME TO` and (some dialects') `RENAME AS` both mean rename-to.
            let (RenameTableNameKind::To(target) | RenameTableNameKind::As(target)) = table_name;
            // A rename keeps the relation in its schema.
            let new = extract_object_name(target, source.schema(), "ALTER TABLE")?;
            if new.schema() != source.schema() {
                return Err(GnitzSqlError::rejected(format!(
                    "{CTX}: a cross-schema rename is not supported \
                     ('{new}' is not in schema '{}')",
                    source.schema()
                )));
            }
            let new_name = new.name().to_string();
            Ok((CTX, Action::RenameRelation { new_name }))
        }
        AlterTableOperation::RenameColumn { old_column_name, new_column_name } => Ok((
            "ALTER TABLE RENAME COLUMN",
            Action::AlterColumn {
                name: &old_column_name.value,
                change: ColumnChange::Rename(&new_column_name.value),
                if_exists: false,
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
            reject_if(
                column_position.is_some(),
                CTX,
                "FIRST/AFTER (a column is always appended last)",
            )?;
            let decl = read_column(col, CTX)?;
            reject_if(decl.serial, CTX, "SERIAL")?;
            // Past SERIAL, only a written NOT NULL makes a column non-nullable.
            reject_if(
                !decl.def.is_nullable,
                CTX,
                "NOT NULL (a new column over existing rows is nullable)",
            )?;
            if let Some(constraint) = decl.constraints.first() {
                return Err(unsupported_clause(
                    CTX,
                    match constraint {
                        ColumnConstraint::PrimaryKey => "PRIMARY KEY (a new column cannot join the primary key)",
                        ColumnConstraint::Unique(_) => "UNIQUE (add the column, then CREATE UNIQUE INDEX)",
                        ColumnConstraint::ForeignKey { .. } => {
                            "REFERENCES (a foreign key is declared only at CREATE TABLE)"
                        }
                    },
                ));
            }
            Ok((
                CTX,
                Action::AddColumn {
                    def: decl.def,
                    if_not_exists: *if_not_exists,
                },
            ))
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
            Ok((
                CTX,
                Action::AlterColumn {
                    name: &col.value,
                    change: ColumnChange::Drop,
                    if_exists: *if_exists,
                },
            ))
        }
        AlterTableOperation::AlterColumn { column_name, op } => {
            use AlterColumnOperation as Op;
            let msg = match op {
                Op::DropNotNull => {
                    return Ok((
                        "ALTER TABLE ALTER COLUMN DROP NOT NULL",
                        Action::AlterColumn {
                            name: &column_name.value,
                            change: ColumnChange::DropNotNull,
                            if_exists: false,
                        },
                    ));
                }
                Op::SetNotNull => "ALTER COLUMN SET NOT NULL is not supported (needs a full-table validation scan)",
                Op::SetDataType { .. } => "ALTER COLUMN SET DATA TYPE is not supported",
                Op::SetDefault { .. } => "ALTER COLUMN SET DEFAULT is not supported (no column defaults in gnitz)",
                Op::DropDefault => "ALTER COLUMN DROP DEFAULT is not supported (no column defaults in gnitz)",
                Op::AddGenerated { .. } => {
                    "ALTER COLUMN ADD GENERATED is not supported (no generated columns in gnitz)"
                }
                Op::GenericOptions { .. } => "ALTER COLUMN OPTIONS is not supported (no foreign tables in gnitz)",
            };
            Err(GnitzSqlError::rejected(msg.to_string()))
        }
        AlterTableOperation::AddConstraint { constraint, not_valid } => {
            const CTX: &str = "ALTER TABLE ADD CONSTRAINT";
            reject_if(*not_valid, CTX, "NOT VALID")?;
            let TableConstraint::Unique(u) = constraint else {
                return Err(GnitzSqlError::rejected(
                    "ADD CONSTRAINT: only UNIQUE constraints are supported",
                ));
            };
            // Same field rejections as an inline CREATE TABLE … UNIQUE.
            reject_unhonored_unique_fields(u, "ADD CONSTRAINT")?;
            // The CONSTRAINT name becomes the index's; `None` auto-generates one.
            Ok((
                CTX,
                Action::AddUnique {
                    columns: index_column_names(&u.columns, "ADD CONSTRAINT")?,
                    explicit_name: u.name.as_ref().map(|n| canonical_user_name(&n.value)).transpose()?,
                },
            ))
        }
        AlterTableOperation::DropConstraint { if_exists, name, drop_behavior } => {
            const CTX: &str = "ALTER TABLE DROP CONSTRAINT";
            reject_if(matches!(drop_behavior, Some(DropBehavior::Cascade)), CTX, "CASCADE")?;
            Ok((
                CTX,
                Action::DropConstraint {
                    name: canonical_user_name(&name.value)?,
                    if_exists: *if_exists,
                },
            ))
        }
        _ => Err(unsupported_clause("ALTER TABLE", &operation.to_string())),
    }
}
