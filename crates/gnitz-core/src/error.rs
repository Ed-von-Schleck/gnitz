use crate::mirror::MirrorError;
use crate::protocol::ProtocolError;
use std::fmt;

#[derive(Debug)]
pub enum ClientError {
    Protocol(ProtocolError), // wire / IO / decode failure
    /// Anything that fails with a message rather than a classified outcome: a
    /// `STATUS_ERROR` the server returned, or a client-side validation the
    /// request never got past.
    ServerError(String),
    /// The session is closed and accepts no further work: a driver aborted it
    /// after a transport or protocol failure, or the request never reached the
    /// wire because the connection was already gone.
    Closed,
    /// A mirror verb on a client that never attached a store. The message names
    /// no method — each binding spells the attach differently.
    NoMirrorStore,
    /// A relation, index or schema the catalog does not hold, by whatever
    /// `name` the lookup used — a qualified name, a bare index name, or a tid.
    /// Its own variant so a binding raises a catchable class for it rather than
    /// matching the prose, which is what every consumer did before.
    NotFound {
        noun: &'static str,
        name: String,
    },
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
    /// `STATUS_SAL_FULL`: the server's shared log had no room for the group this
    /// request needed to write. Its own variant because it is the one server
    /// error that clears itself — a reclaim frees the whole log within a tick —
    /// so a caller retries it where every other error must surface.
    SalFull(String),
    /// A mirror store refused or failed. Kept as its own variant rather than
    /// flattened to a message so [`MirrorError::Poisoned`] stays a class a host
    /// can catch — a poisoned copy is recovered by
    /// [`GnitzClient::close_mirror`](crate::GnitzClient::close_mirror) and by
    /// nothing else.
    Mirror(MirrorError),
    /// The host runtime aborted a blocking call from the park hook — for the
    /// Python binding, a signal handler raised. Carries the host's own error
    /// so the binding re-raises exactly what the handler produced.
    Interrupted(Box<dyn std::error::Error + Send + Sync>),
}

impl From<ProtocolError> for ClientError {
    fn from(e: ProtocolError) -> Self {
        ClientError::Protocol(e)
    }
}

impl From<std::io::Error> for ClientError {
    fn from(e: std::io::Error) -> Self {
        ClientError::Protocol(ProtocolError::IoError(e))
    }
}

/// `gnitz-wire`'s validators report a failure as a bare `String`, so every call
/// into them would otherwise spell the same `ServerError` wrap. A site that adds
/// context still builds the variant by hand — the `From` covers only the
/// verbatim pass-through.
impl From<String> for ClientError {
    fn from(e: String) -> Self {
        ClientError::ServerError(e)
    }
}

impl fmt::Display for ClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ClientError::Protocol(e) => write!(f, "protocol error: {e}"),
            ClientError::ServerError(s) => write!(f, "server error: {s}"),
            ClientError::Closed => write!(f, "connection closed"),
            ClientError::NoMirrorStore => {
                write!(f, "this client mirrors nothing; attach a store before mirroring a view")
            }
            ClientError::NotFound { noun, name } => write!(f, "{noun} '{name}' not found"),
            ClientError::SchemaMismatch => write!(f, "schema version mismatch"),
            ClientError::TxnConflict { fresh_basis } => {
                write!(f, "transaction conflict (fresh basis {fresh_basis}); retry")
            }
            ClientError::DeltaExpired => write!(
                f,
                "delta cursor is not honourable — its rounds were dropped, or it names a \
                 different boot or relation; re-read the feed from 0"
            ),
            ClientError::SalFull(s) => write!(f, "server log full (retryable): {s}"),
            ClientError::Mirror(e) => write!(f, "{e}"),
            ClientError::Interrupted(e) => write!(f, "interrupted: {e}"),
        }
    }
}

impl std::error::Error for ClientError {}
