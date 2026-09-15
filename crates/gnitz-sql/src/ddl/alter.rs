//! `ALTER TABLE`: rename table/view/column, ADD/DROP COLUMN, DROP NOT NULL,
//! ADD/DROP CONSTRAINT (mapped to the CREATE/DROP INDEX paths). `ALTER VIEW … AS`
//! lives in `crate::hir::create` (it recompiles a query). Every rule that reads
//! the catalog is the engine precheck's.

use crate::ast_util::extract_object_name;
use crate::bind::{find_unique_column, require_column};
use crate::error::{missing_relation, reject_if, unsupported_clause, GnitzSqlError};
use crate::types::{serial_underlying, sql_col_type};
use crate::validate::{
    reject_unhonored_alter_table_clauses, reject_unhonored_column_options, reject_unhonored_unique_fields,
    require_class, validate_user_name, ClassWant, ColumnOptionSite,
};
use crate::SqlResult;
use gnitz_core::GnitzClient;
use sqlparser::ast::{
    AlterColumnOperation, AlterTable, AlterTableOperation, DropBehavior, IndexColumn, RenameTableNameKind,
    TableConstraint,
};

/// One ALTER TABLE operation with every check that needs no catalog already run.
/// `object`/`name` are what the statement reports altering.
struct Alter<'a> {
    ctx: &'static str,
    object: &'static str,
    name: String,
    action: Action<'a>,
}

enum Action<'a> {
    RenameRelation,
    RenameColumn {
        old: &'a str,
    },
    AddColumn(gnitz_core::ColumnDef),
    DropColumn {
        if_exists: bool,
    },
    DropNotNull,
    AddUnique {
        columns: &'a [IndexColumn],
        explicit_name: Option<String>,
    },
    DropConstraint {
        if_exists: bool,
    },
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
    validate_user_name(&source_name)?;
    let Alter { ctx, object, name, action } = parse(operation, schema_name)?;

    let Some(rel) = client.resolve(schema_name, &source_name)? else {
        return if alter.if_exists {
            Ok(altered(object, name))
        } else {
            Err(missing_relation("table", schema_name, &source_name))
        };
    };
    // `RENAME TO` accepts a view too, as Postgres does: views bind sources by id
    // and columns by ordinal, so a rename is safe under any class.
    if !matches!(action, Action::RenameRelation) {
        require_class(&rel, &source_name, ClassWant::BaseTable, ctx)?;
    }

    let cols = &rel.schema.columns;
    match action {
        Action::RenameRelation => {
            client.alter_rename_relation(schema_name, &source_name, &name)?;
            return Ok(altered(rel.class.noun(), name));
        }
        Action::RenameColumn { old } => client.alter_rename_column(rel.tid, require_column(cols, old)?, &name)?,
        Action::AddColumn(def) => client.alter_add_column(rel.tid, &def)?,
        Action::DropColumn { if_exists: true } if find_unique_column(cols, &name)?.is_none() => {}
        Action::DropColumn { .. } => client.alter_drop_column(rel.tid, require_column(cols, &name)?)?,
        Action::DropNotNull => client.alter_drop_not_null(rel.tid, require_column(cols, &name)?)?,
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
        Action::DropConstraint { if_exists } => {
            client.drop_unique_constraint(rel.tid, &name, if_exists)?;
        }
    }
    Ok(altered(object, name))
}

/// The operation as an [`Alter`], or the rejection the statement alone earns.
fn parse<'a>(operation: &'a AlterTableOperation, schema_name: &str) -> Result<Alter<'a>, GnitzSqlError> {
    let alter = |ctx, object, name, action| Ok(Alter { ctx, object, name, action });
    match operation {
        AlterTableOperation::RenameTable { table_name } => {
            const CTX: &str = "ALTER TABLE RENAME TO";
            // `RENAME TO` and (some dialects') `RENAME AS` both mean rename-to.
            let (RenameTableNameKind::To(target) | RenameTableNameKind::As(target)) = table_name;
            let new_name = extract_object_name(target, schema_name, "ALTER TABLE")?;
            validate_user_name(&new_name)?;
            alter(CTX, "table", new_name, Action::RenameRelation)
        }
        AlterTableOperation::RenameColumn { old_column_name, new_column_name } => alter(
            "ALTER TABLE RENAME COLUMN",
            "column",
            new_column_name.value.clone(),
            Action::RenameColumn { old: &old_column_name.value },
        ),
        AlterTableOperation::AddColumn {
            // Inert: `ADD c INT` and `ADD COLUMN c INT` mean the same thing.
            column_keyword: _,
            if_not_exists,
            column_def,
            column_position,
        } => {
            const CTX: &str = "ALTER TABLE ADD COLUMN";
            reject_if(*if_not_exists, CTX, "IF NOT EXISTS")?;
            reject_if(
                column_position.is_some(),
                CTX,
                "FIRST/AFTER (a column is always appended last)",
            )?;
            reject_unhonored_column_options(column_def, ColumnOptionSite::AddColumn)?;
            reject_if(serial_underlying(&column_def.data_type).is_some(), CTX, "SERIAL")?;
            let col_name = column_def.name.value.clone();
            let ty = sql_col_type(&column_def.data_type)?;
            let def = gnitz_core::ColumnDef::typed(&col_name, ty, /* is_nullable */ true);
            alter(CTX, "column", col_name, Action::AddColumn(def))
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
            alter(
                CTX,
                "column",
                col.value.clone(),
                Action::DropColumn { if_exists: *if_exists },
            )
        }
        AlterTableOperation::AlterColumn { column_name, op } => {
            use AlterColumnOperation as Op;
            let msg = match op {
                Op::DropNotNull => {
                    return alter(
                        "ALTER TABLE ALTER COLUMN DROP NOT NULL",
                        "column",
                        column_name.value.clone(),
                        Action::DropNotNull,
                    )
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
        AlterTableOperation::AddConstraint { constraint, not_valid } => {
            const CTX: &str = "ALTER TABLE ADD CONSTRAINT";
            reject_if(*not_valid, CTX, "NOT VALID")?;
            let TableConstraint::Unique(u) = constraint else {
                return Err(GnitzSqlError::Unsupported(
                    "ADD CONSTRAINT: only UNIQUE constraints are supported".to_string(),
                ));
            };
            // Same field rejections as an inline CREATE TABLE … UNIQUE.
            reject_unhonored_unique_fields(u, "ADD CONSTRAINT")?;
            // The CONSTRAINT name becomes the index's; `None` auto-generates one.
            let explicit_name = u.name.as_ref().map(|n| n.value.clone());
            alter(
                CTX,
                "constraint",
                explicit_name.clone().unwrap_or_default(),
                Action::AddUnique { columns: &u.columns, explicit_name },
            )
        }
        AlterTableOperation::DropConstraint { if_exists, name, drop_behavior } => {
            const CTX: &str = "ALTER TABLE DROP CONSTRAINT";
            reject_if(matches!(drop_behavior, Some(DropBehavior::Cascade)), CTX, "CASCADE")?;
            validate_user_name(&name.value)?;
            alter(
                CTX,
                "constraint",
                name.value.clone(),
                Action::DropConstraint { if_exists: *if_exists },
            )
        }
        _ => Err(GnitzSqlError::Unsupported(
            "this ALTER TABLE operation is not supported".to_string(),
        )),
    }
}

/// The `Altered` result — also the IF-EXISTS no-op success (nothing to alter,
/// but the statement succeeds), carrying the intended name.
fn altered(object: &str, name: String) -> SqlResult {
    SqlResult::Altered { object: object.to_string(), name }
}
