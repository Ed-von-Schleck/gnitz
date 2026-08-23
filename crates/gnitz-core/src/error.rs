use crate::protocol::ProtocolError;
use std::fmt;

#[derive(Debug)]
pub enum ClientError {
    Protocol(ProtocolError), // wire / IO / decode failure
    /// Anything that fails with a message rather than a classified outcome: a
    /// `STATUS_ERROR` the server returned, a client-side validation the request
    /// never got past, or — for a [`crate::ReadTarget`] that is not a client — a
    /// fault in whatever answered locally.
    ServerError(String),
    SchemaMismatch, // STATUS_SCHEMA_MISMATCH: server rejected schema-less PUSH
    /// STATUS_TXN_CONFLICT: a user-table TXN failed its OCC precondition — a table
    /// it read was written since its basis. `fresh_basis` is the server's current
    /// watermark, which the autocommit RMW retry adopts before re-reading. The
    /// human-facing message is synthesized by the SQL/Python layer (it holds the
    /// tid→name binding).
    TxnConflict {
        fresh_basis: u64,
    },
    /// A delta cursor that cannot be polled from. It named rounds a worker's
    /// retention sweep has already dropped (`STATUS_DELTA_EXPIRED`); or it
    /// belongs to a different boot or a different relation, which `delta_poll`
    /// detects by comparing the tag the reply carries against the one the cursor
    /// holds; or it is the zero cursor, which names no copy to continue.
    ///
    /// One variant for all three, because the recovery is one action: discard the
    /// local copy and bootstrap. Splitting it would make every caller catch
    /// several errors to take one branch.
    DeltaExpired,
}

/// Whether a failure is a retryable OCC conflict rather than a hard error. The
/// bindings surface conflicts as a dedicated, catchable type (Python
/// `GnitzConflictError`, C `GNITZ_ERR_TXN_CONFLICT`); a binding's error mapper
/// is generic over this trait, so one mapper covers every classified error type
/// and no binding re-derives the rule from a `match` of its own.
pub trait ConflictClass {
    fn is_conflict(&self) -> bool;
}

impl ConflictClass for ClientError {
    fn is_conflict(&self) -> bool {
        matches!(self, ClientError::TxnConflict { .. })
    }
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
            ClientError::TxnConflict { fresh_basis } => {
                write!(f, "transaction conflict (fresh basis {fresh_basis}); retry")
            }
            ClientError::DeltaExpired => write!(
                f,
                "delta cursor is not honourable — its rounds were dropped, or it names a \
                 different boot or relation; re-read the feed from 0"
            ),
        }
    }
}

impl std::error::Error for ClientError {}
