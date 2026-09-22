//! Frame size limits and the HELLO/ACK handshake codec.

// ---------------------------------------------------------------------------
// Frame size limits
// ---------------------------------------------------------------------------

/// The payload ceiling both ends apply once the HELLO ACK is in hand.
pub const MAX_FRAME_PAYLOAD: usize = 64 * 1024 * 1024; // 64 MB

/// Payload ceiling the client applies to a frame arriving **before** the HELLO
/// ACK, when the peer has proved nothing yet: without it, four header bytes from
/// an unauthenticated peer would size a 64 MB allocation. Both frames legal
/// there — the ACK and a `WireStatus::Error` control block — fit it. The server
/// bounds its own pre-handshake frame the same way (`HELLO_PAYLOAD_LEN`).
pub const MAX_FRAME_PAYLOAD_PRE_HANDSHAKE: usize = 4 * 1024;

/// Width of the length prefix in front of every framed payload, on every path
/// that carries one: the client socket stream (`recv_framed` and its senders),
/// the server's client ingress, and the W2M ring slot the master forwards to a
/// client verbatim. A `u32` LE count of the payload bytes that follow. Zero is
/// never a legal length.
pub const FRAME_LEN_PREFIX_BYTES: usize = 4;

// ---------------------------------------------------------------------------
// HELLO handshake
//
// A payload's length alone tells HELLO and ACK apart from a control block. The
// ACK is the success reply; a refusal is a `WireStatus::Error` control block.
// ---------------------------------------------------------------------------

/// Magic carried in HELLO and ACK payloads: ASCII "GNTZ" as a little-endian u32.
pub(crate) const HELLO_MAGIC: u32 = u32::from_le_bytes(*b"GNTZ");

/// ALPN protocol both sides of the TLS transport pin; a mismatch fails the
/// handshake.
pub const ALPN_GNITZ: &[u8] = b"gnitz/1";

/// HELLO payload length in bytes (excluding the 4-byte length prefix).
pub const HELLO_PAYLOAD_LEN: usize = 8;

/// ACK payload length in bytes (excluding the 4-byte length prefix).
pub const HELLO_ACK_PAYLOAD_LEN: usize = 12;

/// Total wire size of an ACK frame (length prefix + payload).
pub(crate) const HELLO_ACK_FRAME_SIZE: usize = FRAME_LEN_PREFIX_BYTES + HELLO_ACK_PAYLOAD_LEN;

/// HELLO payload fields.
const HELLO_OFF_MAGIC: usize = 0;
const HELLO_OFF_VERSION: usize = 4;

/// Build a HELLO payload (the bytes after the length prefix). Every sender
/// frames it through its transport's standard framed send, which derives
/// the identical 4-byte prefix.
pub fn encode_hello_payload(version: u32) -> [u8; HELLO_PAYLOAD_LEN] {
    let mut out = [0u8; HELLO_PAYLOAD_LEN];
    crate::write_u32_le(&mut out, HELLO_OFF_MAGIC, HELLO_MAGIC);
    crate::write_u32_le(&mut out, HELLO_OFF_VERSION, version);
    out
}

/// Decode a HELLO payload (the bytes following the length prefix) to the
/// client's protocol version, rejecting a wrong size or magic.
pub fn decode_hello_payload(payload: &[u8]) -> Result<u32, &'static str> {
    if payload.len() != HELLO_PAYLOAD_LEN {
        return Err("hello payload wrong size");
    }
    if crate::read_u32_le(payload, HELLO_OFF_MAGIC) != HELLO_MAGIC {
        return Err("hello magic mismatch");
    }
    Ok(crate::read_u32_le(payload, HELLO_OFF_VERSION))
}

/// ACK payload fields, relative to the payload (past the length prefix).
const ACK_OFF_MAGIC: usize = 0;
const ACK_OFF_LSN: usize = 4;

/// Build an ACK frame ready to ship over the wire (length prefix + payload).
/// `published_lsn` is the server's durability watermark at connect, seeding the
/// client's OCC basis, so a connection needs no separate watermark read.
pub fn encode_hello_ack(published_lsn: u64) -> [u8; HELLO_ACK_FRAME_SIZE] {
    let mut out = [0u8; HELLO_ACK_FRAME_SIZE];
    crate::write_u32_le(&mut out, 0, HELLO_ACK_PAYLOAD_LEN as u32);
    let payload = &mut out[FRAME_LEN_PREFIX_BYTES..];
    crate::write_u32_le(payload, ACK_OFF_MAGIC, HELLO_MAGIC);
    crate::write_u64_le(payload, ACK_OFF_LSN, published_lsn);
    out
}

/// Decode an ACK payload (the bytes following the length prefix) to the
/// server's durability watermark at connect — the client's initial OCC basis —
/// rejecting a wrong size or magic.
pub fn decode_hello_ack(payload: &[u8]) -> Result<u64, &'static str> {
    if payload.len() != HELLO_ACK_PAYLOAD_LEN {
        return Err("hello ack payload wrong size");
    }
    if crate::read_u32_le(payload, ACK_OFF_MAGIC) != HELLO_MAGIC {
        return Err("hello magic mismatch");
    }
    Ok(crate::read_u64_le(payload, ACK_OFF_LSN))
}

#[cfg(test)]
#[path = "tests/handshake.rs"]
mod tests;
