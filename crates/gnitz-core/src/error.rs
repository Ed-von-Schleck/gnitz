use crate::protocol::ProtocolError;
use std::fmt;

#[derive(Debug)]
pub enum ClientError {
    Protocol(ProtocolError), // wire / IO / decode failure
    ServerError(String),     // STATUS_ERROR returned by server
    SchemaMismatch,          // STATUS_SCHEMA_MISMATCH: server rejected schema-less PUSH
    NoIndex,                 // STATUS_NO_INDEX: no secondary index on the column
    /// STATUS_TXN_CONFLICT: a user-table TXN failed its OCC precondition — a table
    /// it read was written since its basis. `fresh_basis` is the server's current
    /// watermark, which the autocommit RMW retry adopts before re-reading. The
    /// human-facing message is synthesized by the SQL/Python layer (it holds the
    /// tid→name binding).
    TxnConflict {
        fresh_basis: u64,
    },
}

impl From<ProtocolError> for ClientError {
    fn from(e: ProtocolError) -> Self {
        ClientError::Protocol(e)
    }
}

impl fmt::Display for ClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ClientError::Protocol(e) => write!(f, "protocol error: {e}"),
            ClientError::ServerError(s) => write!(f, "server error: {s}"),
            ClientError::SchemaMismatch => write!(f, "schema version mismatch"),
            ClientError::NoIndex => write!(f, "no index on requested column"),
            ClientError::TxnConflict { fresh_basis } => {
                write!(f, "transaction conflict (fresh basis {fresh_basis}); retry")
            }
        }
    }
}

impl std::error::Error for ClientError {}
