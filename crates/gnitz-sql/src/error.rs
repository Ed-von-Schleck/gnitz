use std::fmt;

#[derive(Debug)]
pub enum GnitzSqlError {
    Parse(sqlparser::parser::ParserError),
    Bind(String),
    Plan(String),
    Exec(gnitz_core::ClientError),
    Unsupported(String),
    /// An OCC precondition failed (a read table was written concurrently) and the
    /// statement could not commit lose-update-free. `table` names the conflicting
    /// table for an autocommit RMW statement; `None` for a `BEGIN`/`COMMIT`
    /// transaction (the conflict spans statements). The language bindings map this
    /// to a dedicated retryable error (Python `GnitzConflictError`, C
    /// `GNITZ_ERR_TXN_CONFLICT`), distinct from a generic `Exec` failure.
    Conflict {
        table: Option<String>,
    },
}

impl fmt::Display for GnitzSqlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            GnitzSqlError::Parse(e) => write!(f, "parse error: {e}"),
            GnitzSqlError::Bind(s) => write!(f, "bind error: {s}"),
            GnitzSqlError::Plan(s) => write!(f, "plan error: {s}"),
            GnitzSqlError::Exec(e) => write!(f, "exec error: {e}"),
            GnitzSqlError::Unsupported(s) => write!(f, "unsupported: {s}"),
            GnitzSqlError::Conflict { table: Some(t) } => {
                write!(f, "transaction conflict on table '{t}'; retry the statement")
            }
            GnitzSqlError::Conflict { table: None } => {
                write!(f, "transaction conflict; retry the transaction")
            }
        }
    }
}

impl std::error::Error for GnitzSqlError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        // The two wrapped-error variants expose their cause (sqlparser's
        // `ParserError` gained a std `Error` impl in 0.61); the string/marker
        // variants have none.
        match self {
            GnitzSqlError::Parse(e) => Some(e),
            GnitzSqlError::Exec(e) => Some(e),
            GnitzSqlError::Bind(_)
            | GnitzSqlError::Plan(_)
            | GnitzSqlError::Unsupported(_)
            | GnitzSqlError::Conflict { .. } => None,
        }
    }
}

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
