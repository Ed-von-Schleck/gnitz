use std::fmt;
use std::sync::Arc;

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
