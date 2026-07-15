use std::fmt;

#[derive(Debug)]
pub enum ProtocolError {
    UnknownTypeCode(u64),
    DecodeError(String),
    IoError(std::io::Error),
}

impl fmt::Display for ProtocolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ProtocolError::UnknownTypeCode(code) => write!(f, "unknown type code: {code}"),
            ProtocolError::DecodeError(msg) => write!(f, "decode error: {msg}"),
            ProtocolError::IoError(e) => write!(f, "io error: {e}"),
        }
    }
}

impl std::error::Error for ProtocolError {}

impl From<std::io::Error> for ProtocolError {
    fn from(e: std::io::Error) -> Self {
        ProtocolError::IoError(e)
    }
}

impl From<gnitz_wire::WalError> for ProtocolError {
    /// A malformed WAL block surfaces as a `DecodeError`, matching the crate's
    /// pervasive string-based decode errors (and the substring asserts over
    /// them). `WalError`'s `Display` authors the wording once.
    fn from(e: gnitz_wire::WalError) -> Self {
        ProtocolError::DecodeError(format!("WAL {e}"))
    }
}
