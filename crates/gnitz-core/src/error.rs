use std::fmt;
use crate::protocol::ProtocolError;

#[derive(Debug)]
pub enum ClientError {
    Protocol(ProtocolError),  // wire / IO / decode failure
    ServerError(String),      // STATUS_ERROR returned by server
}

impl From<ProtocolError> for ClientError {
    fn from(e: ProtocolError) -> Self {
        ClientError::Protocol(e)
    }
}

impl fmt::Display for ClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ClientError::Protocol(e)   => write!(f, "protocol error: {}", e),
            ClientError::ServerError(s) => write!(f, "server error: {}", s),
        }
    }
}

impl std::error::Error for ClientError {}
