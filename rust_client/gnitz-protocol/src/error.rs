use std::fmt;

#[derive(Debug)]
pub enum ProtocolError {
    BadMagic(u64),
    UnknownTypeCode(u64),
    DecodeError(String),
    IoError(std::io::Error),
}

impl fmt::Display for ProtocolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ProtocolError::BadMagic(got) => write!(f, "bad magic: 0x{:016X}", got),
            ProtocolError::UnknownTypeCode(code) => write!(f, "unknown type code: {}", code),
            ProtocolError::DecodeError(msg) => write!(f, "decode error: {}", msg),
            ProtocolError::IoError(e) => write!(f, "io error: {}", e),
        }
    }
}

impl std::error::Error for ProtocolError {}

impl From<std::io::Error> for ProtocolError {
    fn from(e: std::io::Error) -> Self {
        ProtocolError::IoError(e)
    }
}
