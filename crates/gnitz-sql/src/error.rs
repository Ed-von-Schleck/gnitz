use std::fmt;

use gnitz_sql_parse::GnitzSqlParseError;

#[derive(Debug)]
pub enum GnitzSqlError {
    Parse(sqlparser::parser::ParserError),
    Bind(String),
    Plan(String),
    Exec(gnitz_core::ClientError),
    Unsupported(String),
}

impl fmt::Display for GnitzSqlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            GnitzSqlError::Parse(e)       => write!(f, "parse error: {}", e),
            GnitzSqlError::Bind(s)        => write!(f, "bind error: {}", s),
            GnitzSqlError::Plan(s)        => write!(f, "plan error: {}", s),
            GnitzSqlError::Exec(e)        => write!(f, "exec error: {}", e),
            GnitzSqlError::Unsupported(s) => write!(f, "unsupported: {}", s),
        }
    }
}

impl std::error::Error for GnitzSqlError {}

impl From<gnitz_core::ClientError> for GnitzSqlError {
    fn from(e: gnitz_core::ClientError) -> Self {
        GnitzSqlError::Exec(e)
    }
}

impl From<sqlparser::parser::ParserError> for GnitzSqlError {
    fn from(e: sqlparser::parser::ParserError) -> Self {
        GnitzSqlError::Parse(e)
    }
}

impl From<GnitzSqlParseError> for GnitzSqlError {
    fn from(e: GnitzSqlParseError) -> Self {
        match e {
            GnitzSqlParseError::Parse(p)       => GnitzSqlError::Parse(p),
            GnitzSqlParseError::Bind(s)        => GnitzSqlError::Bind(s),
            GnitzSqlParseError::Plan(s)        => GnitzSqlError::Plan(s),
            GnitzSqlParseError::Unsupported(s) => GnitzSqlError::Unsupported(s),
        }
    }
}

/// Extract the last identifier name from an ObjectName.
pub fn extract_name(name: &sqlparser::ast::ObjectName, context: &str) -> Result<String, GnitzSqlError> {
    name.0.last()
        .and_then(|p| p.as_ident())
        .map(|i| i.value.clone())
        .ok_or_else(|| GnitzSqlError::Bind(format!("empty name in {}", context)))
}

/// Extract table name from a TableFactor::Table.
pub fn extract_table_factor_name(tf: &sqlparser::ast::TableFactor, context: &str) -> Result<String, GnitzSqlError> {
    match tf {
        sqlparser::ast::TableFactor::Table { name, .. } => extract_name(name, context),
        _ => Err(GnitzSqlError::Unsupported(format!("{}: only simple table references supported", context))),
    }
}
