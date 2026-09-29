use crate::mirror::MirrorError;
use crate::ProtocolError;
use gnitz_wire::WireFault;
use std::fmt;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub enum ClientError {
    /// The transport failed, or a reply could not be decoded.
    Protocol(ProtocolError),
    /// A refusal, classified by the status a caller branches on — sent by the
    /// server, or raised by a client-side check in the same terms.
    Refused(WireFault),
    /// The session's owner closed it; it accepts no further work.
    Closed,
    /// The connection failed: every request outstanding on it, and every later
    /// one, carries this one cause.
    ConnectionLost(ProtocolError),
    /// A mirror store refused or failed. Kept as its own variant rather than
    /// flattened to a message so [`MirrorError::Poisoned`] stays a class a host
    /// can catch — a poisoned copy is recovered by
    /// [`GnitzClient::close_mirror`](crate::GnitzClient::close_mirror) and by
    /// nothing else.
    Mirror(MirrorError),
    /// The park hook aborted a blocking call — for the Python binding, a signal
    /// handler raised. Carries the host's own error so the binding re-raises
    /// exactly what the handler produced.
    Interrupted(Arc<dyn std::error::Error + Send + Sync>),
}

impl From<ProtocolError> for ClientError {
    fn from(e: ProtocolError) -> Self {
        ClientError::Protocol(e)
    }
}

impl From<std::io::Error> for ClientError {
    fn from(e: std::io::Error) -> Self {
        ClientError::Protocol(e.into())
    }
}

/// A plain message is a `WireStatus::Error` refusal.
impl From<String> for ClientError {
    fn from(e: String) -> Self {
        ClientError::Refused(WireFault::from(e))
    }
}

impl fmt::Display for ClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ClientError::Protocol(e) => write!(f, "protocol error: {e}"),
            ClientError::Refused(fault) => write!(f, "{fault}"),
            ClientError::Closed => write!(f, "connection closed"),
            ClientError::ConnectionLost(cause) => write!(f, "connection lost: {cause}"),
            ClientError::Mirror(e) => write!(f, "{e}"),
            ClientError::Interrupted(e) => write!(f, "interrupted: {e}"),
        }
    }
}

impl std::error::Error for ClientError {}
