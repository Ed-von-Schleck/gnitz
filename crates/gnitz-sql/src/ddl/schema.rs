//! CREATE SCHEMA.

use crate::ast_util::single_part_ident;
use crate::error::{reject_if, GnitzSqlError};
use gnitz_core::GnitzClient;
use sqlparser::ast::{Expr, ObjectName, SchemaName, SqlOption};

/// `CREATE SCHEMA [IF NOT EXISTS] <name>`.
pub(crate) async fn execute_create_schema(
    client: &mut GnitzClient,
    schema_name: &SchemaName,
    if_not_exists: bool,
    with: Option<&[SqlOption]>,
    options: Option<&[SqlOption]>,
    default_collate_spec: Option<&Expr>,
    clone: Option<&ObjectName>,
) -> Result<(), GnitzSqlError> {
    const CTX: &str = "CREATE SCHEMA";
    reject_if(with.is_some(), CTX, "WITH options")?;
    reject_if(options.is_some(), CTX, "OPTIONS")?;
    reject_if(default_collate_spec.is_some(), CTX, "DEFAULT COLLATE")?;
    reject_if(clone.is_some(), CTX, "CLONE")?;
    let SchemaName::Simple(name) = schema_name else {
        return Err(GnitzSqlError::Rejected(format!(
            "{CTX}: AUTHORIZATION is not supported"
        )));
    };
    let name = single_part_ident(name)
        .ok_or_else(|| GnitzSqlError::Rejected(format!("{CTX}: a schema name is one identifier")))?;
    let canonical = gnitz_wire::canonical_identifier(name).map_err(GnitzSqlError::Rejected)?;
    if if_not_exists && client.catalog().await?.schemas.contains(&canonical) {
        return Ok(());
    }
    client.create_schema(&canonical).await?;
    Ok(())
}
