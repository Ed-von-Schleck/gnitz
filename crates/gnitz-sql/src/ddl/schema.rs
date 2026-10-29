//! CREATE SCHEMA.

use crate::ast_util::single_part_ident;
use crate::error::GnitzSqlError;
use crate::rules::canonical_user_name;
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
        return Err(GnitzSqlError::Rejected(format!(
            "{CTX}: AUTHORIZATION is not supported"
        )));
    };
    let name = single_part_ident(name)
        .ok_or_else(|| GnitzSqlError::Rejected(format!("{CTX}: a schema name is one identifier")))?;
    let canonical = canonical_user_name(name)?;
    client.create_schema(&canonical, if_not_exists).await?;
    Ok(())
}
