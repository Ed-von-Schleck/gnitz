use std::fmt;

use gnitz_core::ClientError;
use gnitz_wire::{WireFault, WireStatus};

#[derive(Debug)]
pub enum GnitzSqlError {
    /// The client or the server refused, or the connection failed. A statement
    /// that cannot run as written is refused here as the server refuses one,
    /// under [`WireStatus::Error`]: [`Self::rejected`].
    Client(ClientError),
    /// A planner invariant broke.
    Internal(String),
}

impl fmt::Display for GnitzSqlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            GnitzSqlError::Client(e) => write!(f, "{e}"),
            GnitzSqlError::Internal(s) => write!(f, "internal error: {s}"),
        }
    }
}

impl std::error::Error for GnitzSqlError {}

impl GnitzSqlError {
    /// The statement cannot run as written: a [`WireStatus::Error`] refusal.
    pub(crate) fn rejected(text: impl Into<String>) -> Self {
        GnitzSqlError::Client(ClientError::from(text.into()))
    }

    /// The text of a [`WireStatus::Error`] refusal; `None` for every other failure.
    pub(crate) fn rejection(&self) -> Option<&str> {
        match self {
            GnitzSqlError::Client(ClientError::Refused(WireFault { status: WireStatus::Error, text })) => Some(text),
            _ => None,
        }
    }

    /// This [`WireStatus::Error`] refusal, named with the clause it was raised in.
    pub(crate) fn in_clause(self, clause: &str) -> Self {
        match self {
            GnitzSqlError::Client(ClientError::Refused(f)) if f.status == WireStatus::Error => {
                GnitzSqlError::Client(ClientError::Refused(f.in_context(clause)))
            }
            other => other,
        }
    }
}

impl From<ClientError> for GnitzSqlError {
    fn from(e: ClientError) -> Self {
        GnitzSqlError::Client(e)
    }
}

impl From<sqlparser::parser::ParserError> for GnitzSqlError {
    fn from(e: sqlparser::parser::ParserError) -> Self {
        GnitzSqlError::rejected(e.to_string())
    }
}

/// A large predicate or computed projection runs out of registers; any other
/// failure means the planner built a bad program.
impl From<gnitz_expr::ExprValidateErr> for GnitzSqlError {
    fn from(e: gnitz_expr::ExprValidateErr) -> Self {
        match e {
            gnitz_expr::ExprValidateErr::TooManyRegs(_) => GnitzSqlError::rejected(e.to_string()),
            _ => GnitzSqlError::Internal(e.to_string()),
        }
    }
}

/// The rejection for a clause a statement does not honor: "{context}: {clause} is not supported".
pub(crate) fn unsupported_clause(context: &str, clause: &str) -> GnitzSqlError {
    GnitzSqlError::rejected(format!("{context}: {clause} is not supported"))
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
    GnitzSqlError::rejected(format!(
        "ad-hoc SELECT reads a single relation; this query derives a new one ({construct}).\n\
         CREATE VIEW <name> AS <your query> — the engine maintains it incrementally — then SELECT from it."
    ))
}
