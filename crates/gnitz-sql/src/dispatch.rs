//! Statement dispatch — the one module that reaches both the compile side
//! (`plan`) and the execute side (`dml`). Builds the per-statement `Binder` and
//! routes a `Statement` to the matching handler.

use crate::bind::Binder;
use crate::error::GnitzSqlError;
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
        Statement::CreateTable(create) => plan::execute_create_table(client, schema_name, create, &mut binder),
        Statement::Drop { object_type, names, .. } => plan::execute_drop(client, schema_name, object_type, names),
        Statement::CreateView { name, query, .. } => {
            plan::execute_create_view(client, schema_name, name, query, stmt, &mut binder)
        }
        Statement::Insert(_) => dml::execute_insert(client, schema_name, stmt, &mut binder),
        Statement::Query(query) => dml::execute_select(client, schema_name, query, &mut binder),
        Statement::CreateIndex(ci) => plan::execute_create_index(client, schema_name, ci, &mut binder),
        Statement::Update { .. } => dml::execute_update(client, schema_name, stmt, &mut binder),
        Statement::Delete(_) => dml::execute_delete(client, schema_name, stmt, &mut binder),
        _ => Err(GnitzSqlError::Unsupported(format!(
            "unsupported SQL statement: {stmt:?}"
        ))),
    }
}
