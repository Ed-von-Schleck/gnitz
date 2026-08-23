//! The one error type the handle reports.

use gnitz_core::ClientError;

/// Why a mirror operation did not happen.
#[derive(Debug)]
pub enum MirrorError {
    /// The upstream connection, or a refusal from the server. A
    /// [`ClientError::DeltaExpired`] reaching a caller means the recovery could
    /// not be completed, not that it was not attempted — the handle runs it
    /// itself.
    Upstream(ClientError),
    /// The local engine refused or failed: a storage fault, a registration the
    /// catalog rejected, a read the spec could not express.
    Engine(String),
    /// The handle is poisoned and refuses every further call. A delta that did
    /// not reach the store leaves a hole the cursor would step over, so
    /// continuing would answer reads off a copy that is silently missing rows.
    /// The message names what poisoned it.
    Poisoned(String),
}

impl std::fmt::Display for MirrorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MirrorError::Upstream(e) => write!(f, "upstream: {e}"),
            MirrorError::Engine(m) => write!(f, "engine: {m}"),
            MirrorError::Poisoned(m) => write!(f, "mirror handle is poisoned: {m}"),
        }
    }
}

impl std::error::Error for MirrorError {}

impl From<ClientError> for MirrorError {
    fn from(e: ClientError) -> Self {
        MirrorError::Upstream(e)
    }
}

impl From<String> for MirrorError {
    fn from(m: String) -> Self {
        MirrorError::Engine(m)
    }
}
