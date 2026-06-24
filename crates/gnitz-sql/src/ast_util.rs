use crate::error::GnitzSqlError;

/// Extract the last identifier name from an ObjectName.
pub(crate) fn extract_name(name: &sqlparser::ast::ObjectName, context: &str) -> Result<String, GnitzSqlError> {
    name.0
        .last()
        .and_then(|p| p.as_ident())
        .map(|i| i.value.clone())
        .ok_or_else(|| GnitzSqlError::Bind(format!("empty name in {context}")))
}

/// True when every projection item is `*` — the shared test behind the
/// "pass every source column through unchanged" wildcard fast paths.
pub(crate) fn is_wildcard_projection(projection: &[sqlparser::ast::SelectItem]) -> bool {
    projection
        .iter()
        .all(|p| matches!(p, sqlparser::ast::SelectItem::Wildcard(_)))
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

/// Bare column name of a single-relation reference: a plain `Identifier`, or a
/// two-part `CompoundIdentifier` whose qualifier adds no disambiguation over a
/// single grouped/base relation. `None` for any other shape, so each caller
/// raises its own context-specific error.
pub(crate) fn single_relation_col_name(e: &sqlparser::ast::Expr) -> Option<&str> {
    use sqlparser::ast::Expr;
    match e {
        Expr::Identifier(id) => Some(&id.value),
        Expr::CompoundIdentifier(p) if p.len() == 2 => Some(&p[1].value),
        _ => None,
    }
}
