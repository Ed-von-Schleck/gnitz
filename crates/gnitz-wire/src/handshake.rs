//! The HELLO both ends of a connection open with — the client's carrying its
//! access token, the server's its verdict on it — the TLS ALPN they pin, and
//! the wire version the HELLO carries.

use std::fmt;
use std::time::Duration;

use crate::SYS_SCHEMA_DIGEST;

/// The epoch of every client↔engine layout; the epoch-pin tests fail until it
/// moves with one.
const WIRE_EPOCH: u32 = 42;

/// The wire version: the HELLO's, a frame recording's, and part of the format
/// word on the server's log. [`SYS_SCHEMA_DIGEST`] folds in, so a system-family
/// shape change moves it too.
pub const WIRE_VERSION: u32 = WIRE_EPOCH ^ ((SYS_SCHEMA_DIGEST ^ (SYS_SCHEMA_DIGEST >> 32)) as u32);

/// A client's deadline over its connect, TLS handshake and HELLO exchange together.
pub const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// ALPN protocol both sides of the TLS transport offer: a peer offering only
/// others fails the handshake. A client offering none is admitted, and judged
/// by its HELLO.
pub const ALPN_GNITZ: &[u8] = b"gnitz/1";

/// What the first frame's payload in each direction opens with, in the one
/// layout no version may change. A server refusing a client's HELLO answers it
/// with [`refusal_hello`].
pub const HELLO: [u8; 8] = {
    let v = WIRE_VERSION.to_le_bytes();
    [b'G', b'N', b'T', b'Z', v[0], v[1], v[2], v[3]]
};

/// The release this build is: the version every crate of a release shares. Two
/// builds of one release may still speak different wire versions, as a build of
/// unreleased source does.
pub const RELEASE: &str = env!("CARGO_PKG_VERSION");

/// The longest release name a refusing server's HELLO carries.
const MAX_RELEASE_BYTES: usize = 64;

/// Why a peer's HELLO was refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HelloError {
    /// Not opening with "GNTZ", or not followed by what its sender's HELLO
    /// carries: the peer is not a gnitz endpoint.
    Malformed,
    /// A gnitz peer built at another wire version. `release` is the release a
    /// refusing server names, `None` from a client or from a server that
    /// names none.
    Version { peer: u32, release: Option<String> },
}

impl fmt::Display for HelloError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            HelloError::Malformed => f.write_str("peer is not a gnitz endpoint (malformed HELLO)"),
            HelloError::Version { peer, release: Some(theirs) } if theirs == RELEASE => write!(
                f,
                "version mismatch: the server and this client are both gnitz {RELEASE}, but built from \
                 different sources (wire versions {peer} and {WIRE_VERSION}); use a client built \
                 from the server's source"
            ),
            HelloError::Version { release: Some(theirs), .. } => write!(
                f,
                "version mismatch: the server is gnitz {theirs} and this client gnitz {RELEASE}; use a \
                 client of release {theirs}"
            ),
            HelloError::Version { peer, release: None } => write!(
                f,
                "version mismatch: the peer speaks wire version {peer}, this build (gnitz {RELEASE}) wire \
                 version {WIRE_VERSION}"
            ),
        }
    }
}

/// A HELLO split at its prefix: the wire version the prefix names, and what
/// follows it.
fn split_hello(payload: &[u8]) -> Result<(u32, &[u8]), HelloError> {
    match payload.split_at_checked(HELLO.len()) {
        Some((prefix, rest)) if prefix[..4] == HELLO[..4] => Ok((crate::read_u32_le(prefix, 4), rest)),
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
    match split_hello(payload)? {
        (WIRE_VERSION, token) if token.len() <= MAX_TOKEN_BYTES => Ok(token),
        (WIRE_VERSION, _) => Err(HelloError::Malformed),
        (peer, _) => Err(HelloError::Version { peer, release: None }),
    }
}

/// The HELLO payload a server answers a HELLO it does not admit with, another
/// version's or no gnitz endpoint's: [`HELLO`], then this build's [`RELEASE`],
/// so that a client of another version can name the release it would need.
pub fn refusal_hello() -> Vec<u8> {
    [&HELLO[..], RELEASE.as_bytes()].concat()
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
    match split_hello(payload)? {
        (WIRE_VERSION, &[v]) => AuthStatus::from_wire(v).ok_or(HelloError::Malformed),
        (WIRE_VERSION, _) => Err(HelloError::Malformed),
        (peer, rest) => Err(HelloError::Version { peer, release: release_name(rest) }),
    }
}

/// The release a refusing server's HELLO names behind its prefix: `None` where it
/// names none, as a server older than the release name does, or where what
/// follows is no release name.
fn release_name(rest: &[u8]) -> Option<String> {
    let name = std::str::from_utf8(rest).ok()?;
    let printable = name.bytes().all(|b| b.is_ascii_graphic());
    (!name.is_empty() && name.len() <= MAX_RELEASE_BYTES && printable).then(|| name.to_string())
}

#[cfg(test)]
#[path = "tests/handshake.rs"]
mod tests;
