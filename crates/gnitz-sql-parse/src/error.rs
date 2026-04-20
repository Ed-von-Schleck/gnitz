use std::fmt;

/// Error type for the pure parser. Carries no client-layer variant;
/// `gnitz-sql` wraps this in its richer error that also carries
/// `ClientError` from gnitz-core.
#[derive(Debug)]
pub enum GnitzSqlParseError {
    Parse(sqlparser::parser::ParserError),
    Bind(String),
    Plan(String),
    Unsupported(String),
}

impl fmt::Display for GnitzSqlParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            GnitzSqlParseError::Parse(e)       => write!(f, "parse error: {}", e),
            GnitzSqlParseError::Bind(s)        => write!(f, "bind error: {}", s),
            GnitzSqlParseError::Plan(s)        => write!(f, "plan error: {}", s),
            GnitzSqlParseError::Unsupported(s) => write!(f, "unsupported: {}", s),
        }
    }
}

impl std::error::Error for GnitzSqlParseError {}

impl From<sqlparser::parser::ParserError> for GnitzSqlParseError {
    fn from(e: sqlparser::parser::ParserError) -> Self {
        GnitzSqlParseError::Parse(e)
    }
}

/// Extract the last identifier name from an ObjectName.
pub fn extract_name(
    name: &sqlparser::ast::ObjectName, context: &str,
) -> Result<String, GnitzSqlParseError> {
    name.0.last()
        .and_then(|p| p.as_ident())
        .map(|i| i.value.clone())
        .ok_or_else(|| GnitzSqlParseError::Bind(format!("empty name in {}", context)))
}

/// Extract table name from a TableFactor::Table.
pub fn extract_table_factor_name(
    tf: &sqlparser::ast::TableFactor, context: &str,
) -> Result<String, GnitzSqlParseError> {
    match tf {
        sqlparser::ast::TableFactor::Table { name, .. } => extract_name(name, context),
        _ => Err(GnitzSqlParseError::Unsupported(
            format!("{}: only simple table references supported", context)
        )),
    }
}
