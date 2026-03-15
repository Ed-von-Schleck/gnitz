use std::fmt;

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
