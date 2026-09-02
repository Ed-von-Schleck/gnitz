use gnitz_core::ConflictClass;
use std::fmt;

#[derive(Debug)]
pub enum GnitzSqlError {
    Parse(sqlparser::parser::ParserError),
    Bind(String),
    Plan(String),
    Exec(gnitz_core::ClientError),
    Unsupported(String),
    /// A planner invariant broke: a shape an earlier guard was supposed to have
    /// rejected reached a stage that cannot represent it. Never raisable by any
    /// SQL a user can write, which is what makes it worth its own variant — a
    /// `Plan` string prefix cannot be asserted on.
    Internal(String),
    /// A planning pass asked for a relation the statement's catalog snapshot does
    /// not hold. A control signal for `dispatch::plan_resolving`, which resolves
    /// the name and re-runs the pass; it never reaches a caller of
    /// `SqlPlanner::execute`.
    CatalogMiss(String),
    /// An OCC precondition failed (a read table was written concurrently) and the
    /// statement could not commit lose-update-free. `table` names the conflicting
    /// table for an autocommit RMW statement; `None` for a `BEGIN`/`COMMIT`
    /// transaction (the conflict spans statements). The Python binding maps this
    /// to a dedicated retryable `GnitzConflictError`, distinct from a generic
    /// `Exec` failure.
    Conflict {
        table: Option<String>,
    },
}

/// `Exec` delegates to the client error it wraps, so a conflict stays classified
/// as one however deep it surfaced.
impl ConflictClass for GnitzSqlError {
    fn is_conflict(&self) -> bool {
        match self {
            GnitzSqlError::Conflict { .. } => true,
            GnitzSqlError::Exec(e) => e.is_conflict(),
            _ => false,
        }
    }
}

impl fmt::Display for GnitzSqlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            GnitzSqlError::Parse(e) => write!(f, "parse error: {e}"),
            GnitzSqlError::Bind(s) => write!(f, "bind error: {s}"),
            GnitzSqlError::Plan(s) => write!(f, "plan error: {s}"),
            GnitzSqlError::Exec(e) => write!(f, "exec error: {e}"),
            GnitzSqlError::Unsupported(s) => write!(f, "unsupported: {s}"),
            GnitzSqlError::Internal(s) => write!(f, "internal error: {s}"),
            GnitzSqlError::CatalogMiss(name) => {
                write!(f, "internal error: relation '{name}' was not resolved before planning")
            }
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
            | GnitzSqlError::Internal(_)
            | GnitzSqlError::CatalogMiss(_)
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

/// The crate's one spelling of an "unhonored clause" rejection: every surface
/// that turns a parsed-but-unimplemented clause away renders it through this.
pub(crate) fn unsupported_clause(context: &str, clause: &str) -> GnitzSqlError {
    GnitzSqlError::Unsupported(format!("{context}: {clause} is not supported"))
}

/// [`unsupported_clause`] when `present`. A run of these reads as the table of
/// clauses a statement does not honor, short-circuiting on the first present one.
pub(crate) fn reject_if(present: bool, context: &str, clause: &str) -> Result<(), GnitzSqlError> {
    if present {
        return Err(unsupported_clause(context, clause));
    }
    Ok(())
}
