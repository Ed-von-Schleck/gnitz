use gnitz_wire::{FrameLenError, WireFault, WireStatus};
use std::fmt;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub enum ClientError {
    /// The transport failed, or a reply could not be decoded.
    Protocol(ProtocolError),
    /// A refusal, classified by the status a caller branches on — sent by the
    /// server, or raised by a client-side check in the same terms.
    Refused(WireFault),
    /// The session that owed the reply is gone.
    Closed,
    /// The connection failed: every request outstanding on it, and every later
    /// one, carries this one cause.
    ConnectionLost(ProtocolError),
    /// The local mirror store refused or failed: a storage fault, a
    /// registration its registry rejected, a read the spec could not express.
    /// Costs the one copy it hit.
    Mirror(String),
    /// The mirror store is poisoned: a store call panicked, so it may be torn,
    /// and it refuses every further call that touches a copy.
    /// [`GnitzClient::close_mirror`](crate::GnitzClient::close_mirror) is the
    /// only recovery.
    MirrorPoisoned,
    /// The park hook aborted a blocking call — for the Python binding, a signal
    /// handler raised. Carries the host's own error so the binding re-raises
    /// exactly what the handler produced.
    Interrupted(Arc<dyn std::error::Error + Send + Sync>),
}

impl ClientError {
    /// The status of a refusal; `None` for every other failure.
    pub fn status(&self) -> Option<WireStatus> {
        match self {
            ClientError::Refused(fault) => Some(fault.status),
            _ => None,
        }
    }
}

/// A `WireStatus::NotFound` refusal raised on this side, worded by the caller.
pub(crate) fn absent(text: String) -> ClientError {
    ClientError::Refused(WireFault { status: WireStatus::NotFound, text })
}

/// The absence a catalog lookup by name reports.
pub fn not_found(noun: &'static str, name: impl std::fmt::Display) -> ClientError {
    absent(format!("{noun} '{name}' not found"))
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

impl From<&str> for ClientError {
    fn from(e: &str) -> Self {
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
            ClientError::Mirror(m) => write!(f, "mirror store: {m}"),
            ClientError::MirrorPoisoned => write!(
                f,
                "mirror store is poisoned: a store call panicked; the store may be torn"
            ),
            ClientError::Interrupted(e) => write!(f, "interrupted: {e}"),
        }
    }
}

impl std::error::Error for ClientError {}

#[derive(Debug, Clone)]
pub enum ProtocolError {
    DecodeError(String),
    IoError(Arc<std::io::Error>),
}

impl fmt::Display for ProtocolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ProtocolError::DecodeError(msg) => write!(f, "decode error: {msg}"),
            ProtocolError::IoError(e) => write!(f, "io error: {e}"),
        }
    }
}

impl std::error::Error for ProtocolError {}

impl From<std::io::Error> for ProtocolError {
    fn from(e: std::io::Error) -> Self {
        ProtocolError::IoError(Arc::new(e))
    }
}

impl From<FrameLenError> for ProtocolError {
    fn from(e: FrameLenError) -> Self {
        match e {
            FrameLenError::Zero => ProtocolError::DecodeError("zero-length frame".into()),
            FrameLenError::Oversize { len } => ProtocolError::DecodeError(format!(
                "payload length {len} exceeds maximum {} bytes",
                gnitz_wire::MAX_FRAME_PAYLOAD
            )),
            FrameLenError::Alloc { .. } => std::io::Error::from(std::io::ErrorKind::OutOfMemory).into(),
        }
    }
}
