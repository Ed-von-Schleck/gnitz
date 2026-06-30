//! Statement dispatch — the one module that reaches both the compile side
//! (`plan`) and the execute side (`dml`). Builds the per-statement `Binder` and
//! routes a `Statement` to the matching handler.

use crate::bind::Binder;
use crate::error::GnitzSqlError;
use crate::plan::validate::{
    reject_unhonored_create_index_clauses, reject_unhonored_create_table_clauses, reject_unhonored_create_view_clauses,
    reject_unhonored_delete_clauses, reject_unhonored_insert_clauses, reject_unhonored_update_clauses,
};
use crate::SqlResult;
use crate::{dml, plan};
use gnitz_core::GnitzClient;
use sqlparser::ast::Statement;

pub(crate) fn execute_statement(
    client: &mut GnitzClient,
    schema_name: &str,
    stmt: &Statement,
) -> Result<SqlResult, GnitzSqlError> {
    let mut binder = Binder::new(schema_name);

    match stmt {
        Statement::CreateTable(create) => {
            reject_unhonored_create_table_clauses(create, "CREATE TABLE")?;
            plan::execute_create_table(client, schema_name, create)
        }
        Statement::Drop { object_type, names, .. } => plan::execute_drop(client, schema_name, object_type, names),
        Statement::CreateView { name, query, .. } => {
            reject_unhonored_create_view_clauses(stmt, "CREATE VIEW")?;
            plan::execute_create_view(client, schema_name, name, query, stmt, &mut binder)
        }
        Statement::Insert(insert) => {
            reject_unhonored_insert_clauses(insert, "INSERT")?;
            dml::execute_insert(client, schema_name, stmt, &mut binder)
        }
        Statement::Query(query) => dml::execute_select(client, schema_name, query, &mut binder),
        Statement::CreateIndex(ci) => {
            reject_unhonored_create_index_clauses(ci, "CREATE INDEX")?;
            plan::execute_create_index(client, schema_name, ci, &mut binder)
        }
        Statement::Update { .. } => {
            reject_unhonored_update_clauses(stmt, "UPDATE")?;
            dml::execute_update(client, schema_name, stmt, &mut binder)
        }
        Statement::Delete(del) => {
            reject_unhonored_delete_clauses(del, "DELETE")?;
            dml::execute_delete(client, schema_name, stmt, &mut binder)
        }
        _ => Err(GnitzSqlError::Unsupported(format!(
            "unsupported SQL statement: {stmt:?}"
        ))),
    }
}
