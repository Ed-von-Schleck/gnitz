use std::fmt;

#[derive(Debug)]
pub enum GnitzSqlError {
    /// The statement cannot run as written.
    Rejected(String),
    Client(gnitz_core::ClientError),
    /// A planner invariant broke.
    Internal(String),
}

impl fmt::Display for GnitzSqlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            GnitzSqlError::Rejected(s) => f.write_str(s),
            GnitzSqlError::Client(e) => write!(f, "{e}"),
            GnitzSqlError::Internal(s) => write!(f, "internal error: {s}"),
        }
    }
}

impl std::error::Error for GnitzSqlError {}

impl From<gnitz_core::ClientError> for GnitzSqlError {
    fn from(e: gnitz_core::ClientError) -> Self {
        GnitzSqlError::Client(e)
    }
}

impl From<sqlparser::parser::ParserError> for GnitzSqlError {
    fn from(e: sqlparser::parser::ParserError) -> Self {
        GnitzSqlError::Rejected(e.to_string())
    }
}

/// A large predicate or computed projection runs out of registers; any other
/// failure means the planner built a bad program.
impl From<gnitz_expr::ExprValidateErr> for GnitzSqlError {
    fn from(e: gnitz_expr::ExprValidateErr) -> Self {
        match e {
            gnitz_expr::ExprValidateErr::TooManyRegs(_) => GnitzSqlError::Rejected(e.to_string()),
            _ => GnitzSqlError::Internal(e.to_string()),
        }
    }
}

/// The rejection for a clause a statement does not honor: "{context}: {clause} is not supported".
pub(crate) fn unsupported_clause(context: &str, clause: &str) -> GnitzSqlError {
    GnitzSqlError::Rejected(format!("{context}: {clause} is not supported"))
}

/// A relation miss read from the statement's catalog, as the client reports its own.
pub(crate) fn missing_relation(name: &gnitz_core::RelName) -> GnitzSqlError {
    GnitzSqlError::Client(gnitz_core::not_found("relation", name))
}

/// [`unsupported_clause`] when `present`.
pub(crate) fn reject_if(present: bool, context: &str, clause: &str) -> Result<(), GnitzSqlError> {
    if present {
        return Err(unsupported_clause(context, clause));
    }
    Ok(())
}

/// An ad-hoc SELECT reads one relation; this query derives a new one, which a view
/// maintains. `construct` names what was detected.
pub(crate) fn derivation(construct: &str) -> GnitzSqlError {
    GnitzSqlError::Rejected(format!(
        "ad-hoc SELECT reads a single relation; this query derives a new one ({construct}).\n\
         CREATE VIEW <name> AS <your query> — the engine maintains it incrementally — then SELECT from it."
    ))
}
