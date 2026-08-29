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
/// ACK, when the peer has proved nothing yet. Exactly two frames are legal
/// there and both are small: the ACK (`HELLO_ACK_PAYLOAD_LEN`) and a
/// `STATUS_ERROR` control block — `CTRL_BLOCK_SIZE_NO_BLOB` plus the spill of
/// the one error text that path emits, ~306 bytes at worst. 4 KiB is an order
/// of magnitude of headroom over that; without it four header bytes from an
/// unauthenticated peer would size a 256 MB allocation. The server bounds its
/// own pre-handshake frame the same way (`HELLO_PAYLOAD_LEN`).
pub const MAX_FRAME_PAYLOAD_PRE_HANDSHAKE: usize = 4 * 1024;

/// Width of the length prefix in front of every framed payload, on every path
/// that carries one: the client socket stream (`recv_framed` and its senders),
/// the reactor's header recv, and the W2M ring slot the master forwards to a
/// client verbatim. A `u32` LE count of the payload bytes that follow.
pub const FRAME_LEN_PREFIX_BYTES: usize = 4;

// ---------------------------------------------------------------------------
// HELLO handshake
// ---------------------------------------------------------------------------
//
// Layout (length-prefixed; both sides use the standard 4-byte LE u32 prefix):
//
//   HELLO  (client → server, total wire size 12 bytes)
//     [length=8 LE u32][magic: u32 LE][version: u16 LE][_pad: u16 LE]
//
//   ACK    (server → client on success, total wire size 24 bytes)
//     [length=20 LE u32][magic: u32 LE][status: u16 LE][_pad: u16 LE]
//     [limit_bytes: u32 LE][published_lsn: u64 LE]
//
// The trailing `published_lsn` seeds the client's OCC basis (the durability
// watermark at connect), so every connection starts with a basis and needs no
// separate watermark read. On version mismatch / auth failure the server
// replies with a standard length-prefixed STATUS_ERROR control block and closes
// the fd. The length prefix alone discriminates: 8 ⇒ HELLO, 20 ⇒ ACK, anything
// else ⇒ control block. Magic checks remain as defence-in-depth.

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
pub const HELLO_ACK_PAYLOAD_LEN: u32 = 20;

/// Total wire size of an ACK frame (length prefix + payload).
pub(crate) const HELLO_ACK_FRAME_SIZE: usize = 4 + HELLO_ACK_PAYLOAD_LEN as usize;

/// Status field in the ACK frame. Success is the only value the ACK carries —
/// version/auth failures use a `STATUS_ERROR` control block, not the ACK.
pub const HELLO_STATUS_OK: u16 = 0;

/// Build a HELLO payload (the bytes after the length prefix). Every sender
/// frames it through its transport's standard framed send, which derives
/// the identical 4-byte prefix.
pub const fn encode_hello_payload(version: u16) -> [u8; HELLO_PAYLOAD_LEN as usize] {
    let mag = HELLO_MAGIC.to_le_bytes();
    let ver = version.to_le_bytes();
    [mag[0], mag[1], mag[2], mag[3], ver[0], ver[1], 0, 0] // _pad
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
    let magic = crate::read_u32_le(payload, 0);
    let version = crate::read_u16_le(payload, 4);
    // bytes [6..8] are reserved padding
    Ok(HelloHeader { magic, version })
}

/// Build an ACK frame ready to ship over the wire (length prefix + payload).
/// `published_lsn` is the server's durability watermark at connect, seeding the
/// client's OCC basis. Still a `const fn` — callers pass a runtime `published_lsn`
/// and materialise the array on the stack.
pub const fn encode_hello_ack(status: u16, limit_bytes: u32, published_lsn: u64) -> [u8; HELLO_ACK_FRAME_SIZE] {
    let len = HELLO_ACK_PAYLOAD_LEN.to_le_bytes();
    let mag = HELLO_MAGIC.to_le_bytes();
    let st = status.to_le_bytes();
    let lim = limit_bytes.to_le_bytes();
    let lsn = published_lsn.to_le_bytes();
    [
        len[0], len[1], len[2], len[3], mag[0], mag[1], mag[2], mag[3], st[0], st[1], 0, 0, // _pad
        lim[0], lim[1], lim[2], lim[3], lsn[0], lsn[1], lsn[2], lsn[3], lsn[4], lsn[5], lsn[6], lsn[7],
    ]
}

/// Parsed ACK payload (the 20 bytes following the length prefix).
#[derive(Debug, Clone, Copy)]
pub struct HelloAck {
    pub magic: u32,
    pub status: u16,
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
    let magic = crate::read_u32_le(payload, 0);
    let status = crate::read_u16_le(payload, 4);
    // bytes [6..8] are reserved padding
    let limit_bytes = crate::read_u32_le(payload, 8);
    let published_lsn = crate::read_u64_le(payload, 12);
    Ok(HelloAck {
        magic,
        status,
        limit_bytes,
        published_lsn,
    })
}

#[cfg(test)]
#[path = "tests/handshake.rs"]
mod tests;
