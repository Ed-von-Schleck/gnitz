//! The HELLO both ends of a connection open with — the client's carrying its
//! access token, the server's its verdict on it — and the TLS ALPN they pin.

use std::fmt;
use std::time::Duration;

use crate::wal::WAL_FORMAT_VERSION;

/// A client's deadline over its connect, TLS handshake and HELLO exchange together.
pub const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// ALPN protocol both sides of the TLS transport pin; a mismatch fails the
/// handshake.
pub const ALPN_GNITZ: &[u8] = b"gnitz/1";

/// What the first frame's payload in each direction opens with, in the one
/// layout no version may change. A server refusing a client's HELLO answers it
/// bare.
pub const HELLO: [u8; 8] = {
    let v = WAL_FORMAT_VERSION.to_le_bytes();
    [b'G', b'N', b'T', b'Z', v[0], v[1], v[2], v[3]]
};

/// Why a peer's HELLO was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HelloError {
    /// Not opening with "GNTZ", or not followed by what its sender's HELLO
    /// carries: the peer is not a gnitz endpoint.
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

/// What a peer's HELLO carries behind [`HELLO`], once its prefix matches this build's.
fn check_hello(payload: &[u8]) -> Result<&[u8], HelloError> {
    match payload.split_at_checked(HELLO.len()) {
        Some((prefix, rest)) if prefix[..4] == HELLO[..4] => match crate::read_u32_le(prefix, 4) {
            WAL_FORMAT_VERSION => Ok(rest),
            peer => Err(HelloError::Version { peer }),
        },
        _ => Err(HelloError::Malformed),
    }
}

/// The longest access token a client's HELLO carries.
pub const MAX_TOKEN_BYTES: usize = 256;

/// The largest frame a server admits before the HELLO admits its client.
pub const HANDSHAKE_FRAME_MAX: usize = HELLO.len() + MAX_TOKEN_BYTES;

/// A client's HELLO payload: [`HELLO`], then its access token's bytes — none for
/// a client that holds no token.
pub fn client_hello(token: &[u8]) -> Vec<u8> {
    debug_assert!(token.len() <= MAX_TOKEN_BYTES, "a {}-byte token", token.len());
    [&HELLO[..], token].concat()
}

/// The access token a client's HELLO carries, empty when it holds none.
pub fn read_client_hello(payload: &[u8]) -> Result<&[u8], HelloError> {
    match check_hello(payload)? {
        token if token.len() <= MAX_TOKEN_BYTES => Ok(token),
        _ => Err(HelloError::Malformed),
    }
}

crate::wire_enum! {
    /// The server's verdict on a client's access token. Anything but `Accepted`
    /// is followed by the server closing the connection.
    pub enum AuthStatus: u8 {
        Accepted = 0,
        /// The listener requires a token and the client's, or its lack of
        /// one, is none the server holds.
        Refused = 1,
    }
}

/// The HELLO payload a server answers an admissible client's with: [`HELLO`],
/// then its verdict.
pub const fn server_hello(status: AuthStatus) -> [u8; HELLO.len() + 1] {
    let h = HELLO;
    [h[0], h[1], h[2], h[3], h[4], h[5], h[6], h[7], status.as_wire()]
}

/// The verdict a server's HELLO carries.
pub fn read_server_hello(payload: &[u8]) -> Result<AuthStatus, HelloError> {
    match check_hello(payload)? {
        &[v] => AuthStatus::from_wire(v).ok_or(HelloError::Malformed),
        _ => Err(HelloError::Malformed),
    }
}

#[cfg(test)]
#[path = "tests/handshake.rs"]
mod tests;
