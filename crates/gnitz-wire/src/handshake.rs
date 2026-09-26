//! The HELLO both ends of a connection open with, and the TLS ALPN they pin.

use std::fmt;
use std::time::Duration;

use crate::wal::WAL_FORMAT_VERSION;

/// A client's deadline over its connect, TLS handshake and HELLO exchange together.
pub const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// ALPN protocol both sides of the TLS transport pin; a mismatch fails the
/// handshake.
pub const ALPN_GNITZ: &[u8] = b"gnitz/1";

/// The first frame's payload in each direction, in the one layout no version may change.
pub const HELLO: [u8; 8] = {
    let v = WAL_FORMAT_VERSION.to_le_bytes();
    [b'G', b'N', b'T', b'Z', v[0], v[1], v[2], v[3]]
};

/// Why a peer's HELLO was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HelloError {
    /// Not 8 bytes opening with "GNTZ": the peer is not a gnitz endpoint.
    Malformed,
    /// A gnitz peer built at another wire version.
    Version { peer: u32 },
}

impl fmt::Display for HelloError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            HelloError::Malformed => f.write_str("peer is not a gnitz endpoint (malformed HELLO)"),
            HelloError::Version { peer } => {
                write!(f, "wire version mismatch: peer={peer}, local={WAL_FORMAT_VERSION}")
            }
        }
    }
}

/// Check a peer's HELLO payload against this build's.
pub fn check_hello(payload: &[u8]) -> Result<(), HelloError> {
    if payload.len() != HELLO.len() || payload[..4] != HELLO[..4] {
        return Err(HelloError::Malformed);
    }
    match crate::read_u32_le(payload, 4) {
        WAL_FORMAT_VERSION => Ok(()),
        peer => Err(HelloError::Version { peer }),
    }
}

#[cfg(test)]
#[path = "tests/handshake.rs"]
mod tests;
