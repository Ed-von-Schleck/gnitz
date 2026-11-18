//! CREATE SCHEMA.

use crate::error::{GnitzSqlError, unsupported_clause};
use crate::names::extract_global_name;
use gnitz_core::GnitzClient;
use sqlparser::ast::SchemaName;

/// `CREATE SCHEMA [IF NOT EXISTS] <name>`; the router has rejected the
/// statement's other clauses.
pub(crate) async fn execute_create_schema(
    client: &mut GnitzClient,
    schema_name: &SchemaName,
    if_not_exists: bool,
) -> Result<(), GnitzSqlError> {
    const CTX: &str = "CREATE SCHEMA";
    let SchemaName::Simple(name) = schema_name else {
        return Err(unsupported_clause(CTX, "AUTHORIZATION"));
    };
    let name = extract_global_name(name, "schema", CTX)?;
    client.create_schema(&name, if_not_exists).await?;
    Ok(())
}
