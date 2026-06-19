use crate::error::GnitzSqlError;

/// Extract the last identifier name from an ObjectName.
pub(crate) fn extract_name(name: &sqlparser::ast::ObjectName, context: &str) -> Result<String, GnitzSqlError> {
    name.0
        .last()
        .and_then(|p| p.as_ident())
        .map(|i| i.value.clone())
        .ok_or_else(|| GnitzSqlError::Bind(format!("empty name in {context}")))
}

/// Extract table name from a TableFactor::Table.
pub(crate) fn extract_table_factor_name(
    tf: &sqlparser::ast::TableFactor,
    context: &str,
) -> Result<String, GnitzSqlError> {
    match tf {
        sqlparser::ast::TableFactor::Table { name, .. } => extract_name(name, context),
        _ => Err(GnitzSqlError::Unsupported(format!(
            "{context}: only simple table references supported"
        ))),
    }
}
