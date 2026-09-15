//! Frame size limits and the HELLO/ACK handshake codec.

// ---------------------------------------------------------------------------
// Frame size limits
// ---------------------------------------------------------------------------

/// Maximum frame payload the server will accept from a client.
/// Tighter than the client limit: protects the master process from a
/// misbehaving or malicious peer before any allocation occurs.
pub const MAX_FRAME_PAYLOAD_SERVER: usize = 64 * 1024 * 1024; // 64 MB

/// Maximum frame payload the client library will accept from the server.
/// Larger than the server limit: a legitimate batch push can be hundreds
/// of MB; the server is trusted so the risk model is different.
pub const MAX_FRAME_PAYLOAD_CLIENT: usize = 256 * 1024 * 1024; // 256 MB

/// Payload ceiling the client applies to a frame arriving **before** the HELLO
/// ACK, when the peer has proved nothing yet: without it, four header bytes from
/// an unauthenticated peer would size a 256 MB allocation. Both frames legal
/// there — the ACK and a `WireStatus::Error` control block — fit it. The server
/// bounds its own pre-handshake frame the same way (`HELLO_PAYLOAD_LEN`).
pub const MAX_FRAME_PAYLOAD_PRE_HANDSHAKE: usize = 4 * 1024;

/// Width of the length prefix in front of every framed payload, on every path
/// that carries one: the client socket stream (`recv_framed` and its senders),
/// the reactor's header recv, and the W2M ring slot the master forwards to a
/// client verbatim. A `u32` LE count of the payload bytes that follow.
///
/// Zero is the **close sentinel**, never an empty frame: a reader that decodes
/// one treats the stream as closed, so no sender may emit one.
pub const FRAME_LEN_PREFIX_BYTES: usize = 4;

// ---------------------------------------------------------------------------
// HELLO handshake
//
// Both payloads carry the standard 4-byte LE u32 length prefix, and that prefix
// alone discriminates them from a control block, which is far larger; the magic
// stays as defence-in-depth. The ACK carries no status — it *is* the success
// reply, and a version/auth failure is a `WireStatus::Error` control block instead.
// The `*_OFF_*` constants below ARE the field layout: encoder and decoder both
// address through them, so neither can drift from the other.
// ---------------------------------------------------------------------------

/// Magic value carried in HELLO and ACK frames. ASCII "GNTZ" interpreted
/// as a little-endian u32. Defence-in-depth on top of the length-prefix
/// discriminant; a peer sending a control block first cannot collide because
/// such a block is far larger than the 8/12-byte HELLO/ACK frames.
pub const HELLO_MAGIC: u32 = u32::from_le_bytes(*b"GNTZ");

/// ALPN protocol both sides of the TLS transport pin; a mismatch fails the
/// handshake. One definition — a silent client/engine drift would brick
/// every TLS connect.
pub const ALPN_GNITZ: &[u8] = b"gnitz/1";

/// HELLO payload length in bytes (excluding the 4-byte length prefix).
pub const HELLO_PAYLOAD_LEN: u32 = 8;

/// ACK payload length in bytes (excluding the 4-byte length prefix).
pub const HELLO_ACK_PAYLOAD_LEN: u32 = 16;

/// Total wire size of an ACK frame (length prefix + payload).
pub(crate) const HELLO_ACK_FRAME_SIZE: usize = 4 + HELLO_ACK_PAYLOAD_LEN as usize;

/// HELLO payload fields; bytes `[6..8)` are reserved padding.
const HELLO_OFF_MAGIC: usize = 0;
const HELLO_OFF_VERSION: usize = 4;

/// Build a HELLO payload (the bytes after the length prefix). Every sender
/// frames it through its transport's standard framed send, which derives
/// the identical 4-byte prefix.
pub fn encode_hello_payload(version: u16) -> [u8; HELLO_PAYLOAD_LEN as usize] {
    let mut out = [0u8; HELLO_PAYLOAD_LEN as usize];
    crate::write_u32_le(&mut out, HELLO_OFF_MAGIC, HELLO_MAGIC);
    crate::write_u16_le(&mut out, HELLO_OFF_VERSION, version);
    out
}

/// Parsed HELLO payload (the 8 bytes following the length prefix).
#[derive(Debug, Clone, Copy)]
pub struct HelloHeader {
    pub magic: u32,
    pub version: u16,
}

/// Decode a HELLO payload. The caller must have already consumed the
/// 4-byte length prefix and verified that it equalled `HELLO_PAYLOAD_LEN`.
pub fn decode_hello_payload(payload: &[u8]) -> Result<HelloHeader, &'static str> {
    if payload.len() != HELLO_PAYLOAD_LEN as usize {
        return Err("hello payload wrong size");
    }
    Ok(HelloHeader {
        magic: crate::read_u32_le(payload, HELLO_OFF_MAGIC),
        version: crate::read_u16_le(payload, HELLO_OFF_VERSION),
    })
}

/// ACK payload fields, relative to the payload (past the length prefix).
const ACK_OFF_MAGIC: usize = 0;
const ACK_OFF_LIMIT: usize = 4;
const ACK_OFF_LSN: usize = 8;

/// Build an ACK frame ready to ship over the wire (length prefix + payload).
/// `published_lsn` is the server's durability watermark at connect, seeding the
/// client's OCC basis, so a connection needs no separate watermark read.
pub fn encode_hello_ack(limit_bytes: u32, published_lsn: u64) -> [u8; HELLO_ACK_FRAME_SIZE] {
    let mut out = [0u8; HELLO_ACK_FRAME_SIZE];
    crate::write_u32_le(&mut out, 0, HELLO_ACK_PAYLOAD_LEN);
    let payload = &mut out[FRAME_LEN_PREFIX_BYTES..];
    crate::write_u32_le(payload, ACK_OFF_MAGIC, HELLO_MAGIC);
    crate::write_u32_le(payload, ACK_OFF_LIMIT, limit_bytes);
    crate::write_u64_le(payload, ACK_OFF_LSN, published_lsn);
    out
}

/// Parsed ACK payload (the 16 bytes following the length prefix).
#[derive(Debug, Clone, Copy)]
pub struct HelloAck {
    pub magic: u32,
    pub limit_bytes: u32,
    /// Server durability watermark at connect — the client's initial OCC basis.
    pub published_lsn: u64,
}

/// Decode an ACK payload. The caller must have already consumed the
/// 4-byte length prefix and verified that it equalled `HELLO_ACK_PAYLOAD_LEN`.
pub fn decode_hello_ack(payload: &[u8]) -> Result<HelloAck, &'static str> {
    if payload.len() != HELLO_ACK_PAYLOAD_LEN as usize {
        return Err("hello ack payload wrong size");
    }
    Ok(HelloAck {
        magic: crate::read_u32_le(payload, ACK_OFF_MAGIC),
        limit_bytes: crate::read_u32_le(payload, ACK_OFF_LIMIT),
        published_lsn: crate::read_u64_le(payload, ACK_OFF_LSN),
    })
}

#[cfg(test)]
#[path = "tests/handshake.rs"]
mod tests;
