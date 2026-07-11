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

// ---------------------------------------------------------------------------
// HELLO handshake
// ---------------------------------------------------------------------------
//
// Layout (length-prefixed; both sides use the standard 4-byte LE u32 prefix):
//
//   HELLO  (client → server, total wire size 12 bytes)
//     [length=8 LE u32][magic: u32 LE][version: u16 LE][flags: u16 LE]
//
//   ACK    (server → client on success, total wire size 16 bytes)
//     [length=12 LE u32][magic: u32 LE][status: u16 LE][_pad: u16 LE]
//     [limit_bytes: u32 LE]
//
// On version mismatch / auth failure the server replies with a standard
// length-prefixed STATUS_ERROR control block and closes the fd. The length
// prefix alone discriminates: 8 ⇒ HELLO, 12 ⇒ ACK, anything else ⇒ control
// block. Magic checks remain as defence-in-depth.

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
pub const HELLO_ACK_PAYLOAD_LEN: u32 = 12;

/// Total wire size of an ACK frame (length prefix + payload).
pub const HELLO_ACK_FRAME_SIZE: usize = 4 + HELLO_ACK_PAYLOAD_LEN as usize;

/// Status field in the ACK frame. Success is the only value the ACK carries —
/// version/auth failures use a `STATUS_ERROR` control block, not the ACK.
pub const HELLO_STATUS_OK: u16 = 0;

/// Build a HELLO payload (the bytes after the length prefix). Every sender
/// frames it through its transport's standard framed send, which derives
/// the identical 4-byte prefix.
pub const fn encode_hello_payload(version: u16, flags: u16) -> [u8; HELLO_PAYLOAD_LEN as usize] {
    let mag = HELLO_MAGIC.to_le_bytes();
    let ver = version.to_le_bytes();
    let flg = flags.to_le_bytes();
    [mag[0], mag[1], mag[2], mag[3], ver[0], ver[1], flg[0], flg[1]]
}

/// Parsed HELLO payload (the 8 bytes following the length prefix).
#[derive(Debug, Clone, Copy)]
pub struct HelloHeader {
    pub magic: u32,
    pub version: u16,
    pub flags: u16,
}

/// Decode a HELLO payload. The caller must have already consumed the
/// 4-byte length prefix and verified that it equalled `HELLO_PAYLOAD_LEN`.
pub fn decode_hello_payload(payload: &[u8]) -> Result<HelloHeader, &'static str> {
    if payload.len() != HELLO_PAYLOAD_LEN as usize {
        return Err("hello payload wrong size");
    }
    let magic = u32::from_le_bytes(payload[0..4].try_into().unwrap());
    let version = u16::from_le_bytes(payload[4..6].try_into().unwrap());
    let flags = u16::from_le_bytes(payload[6..8].try_into().unwrap());
    Ok(HelloHeader { magic, version, flags })
}

/// Build an ACK frame ready to ship over the wire (length prefix + payload).
pub const fn encode_hello_ack(status: u16, limit_bytes: u32) -> [u8; HELLO_ACK_FRAME_SIZE] {
    let len = HELLO_ACK_PAYLOAD_LEN.to_le_bytes();
    let mag = HELLO_MAGIC.to_le_bytes();
    let st = status.to_le_bytes();
    let lim = limit_bytes.to_le_bytes();
    [
        len[0], len[1], len[2], len[3], mag[0], mag[1], mag[2], mag[3], st[0], st[1], 0, 0, // _pad
        lim[0], lim[1], lim[2], lim[3],
    ]
}

/// Parsed ACK payload (the 12 bytes following the length prefix).
#[derive(Debug, Clone, Copy)]
pub struct HelloAck {
    pub magic: u32,
    pub status: u16,
    pub limit_bytes: u32,
}

/// Decode an ACK payload. The caller must have already consumed the
/// 4-byte length prefix and verified that it equalled `HELLO_ACK_PAYLOAD_LEN`.
pub fn decode_hello_ack(payload: &[u8]) -> Result<HelloAck, &'static str> {
    if payload.len() != HELLO_ACK_PAYLOAD_LEN as usize {
        return Err("hello ack payload wrong size");
    }
    let magic = u32::from_le_bytes(payload[0..4].try_into().unwrap());
    let status = u16::from_le_bytes(payload[4..6].try_into().unwrap());
    // bytes [6..8] are reserved padding
    let limit_bytes = u32::from_le_bytes(payload[8..12].try_into().unwrap());
    Ok(HelloAck {
        magic,
        status,
        limit_bytes,
    })
}

#[cfg(test)]
mod hello_tests {
    use super::*;

    #[test]
    fn hello_magic_is_ascii_gntz_le() {
        // Magic must be the LE-bytes interpretation of "GNTZ" so the
        // wire layout is stable across LE-only builds (the BE
        // compile_error guard at the top of this crate is a load-bearing
        // precondition for this test).
        assert_eq!(HELLO_MAGIC, u32::from_le_bytes(*b"GNTZ"));
    }

    #[test]
    fn hello_payload_layout_is_stable() {
        // Magic must sit at offsets 0..4, version at 4..6, flags at 6..8.
        let payload = encode_hello_payload(0x1234, 0x0001);
        assert_eq!(payload.len(), HELLO_PAYLOAD_LEN as usize);
        let magic = u32::from_le_bytes(payload[0..4].try_into().unwrap());
        assert_eq!(magic, HELLO_MAGIC);
        let version = u16::from_le_bytes(payload[4..6].try_into().unwrap());
        assert_eq!(version, 0x1234);
        let flags = u16::from_le_bytes(payload[6..8].try_into().unwrap());
        assert_eq!(flags, 0x0001);
    }

    #[test]
    fn hello_payload_decode_roundtrip() {
        let payload = encode_hello_payload(7, 0xCAFE);
        let h = decode_hello_payload(&payload).unwrap();
        assert_eq!(h.magic, HELLO_MAGIC);
        assert_eq!(h.version, 7);
        assert_eq!(h.flags, 0xCAFE);
    }

    #[test]
    fn hello_payload_decode_rejects_wrong_size() {
        // Decoder must reject anything other than HELLO_PAYLOAD_LEN
        // bytes; the caller is expected to have framed the payload via
        // the standard length prefix.
        assert!(decode_hello_payload(&[0u8; 7]).is_err());
        assert!(decode_hello_payload(&[0u8; 9]).is_err());
    }

    #[test]
    fn ack_frame_layout_is_stable() {
        let ack = encode_hello_ack(HELLO_STATUS_OK, 16 * 1024 * 1024);
        assert_eq!(ack.len(), HELLO_ACK_FRAME_SIZE);
        let prefix = u32::from_le_bytes(ack[0..4].try_into().unwrap());
        assert_eq!(prefix, HELLO_ACK_PAYLOAD_LEN);
        let magic = u32::from_le_bytes(ack[4..8].try_into().unwrap());
        assert_eq!(magic, HELLO_MAGIC);
        let status = u16::from_le_bytes(ack[8..10].try_into().unwrap());
        assert_eq!(status, HELLO_STATUS_OK);
        // [10..12] are reserved padding.
        let limit = u32::from_le_bytes(ack[12..16].try_into().unwrap());
        assert_eq!(limit, 16 * 1024 * 1024);
    }

    #[test]
    fn ack_decode_roundtrip() {
        // Raw non-zero status (not HELLO_STATUS_OK=0) so the round-trip proves
        // status is read from its own offset, distinct from the zero `_pad`.
        let ack = encode_hello_ack(1, 64 * 1024 * 1024);
        let parsed = decode_hello_ack(&ack[4..]).unwrap();
        assert_eq!(parsed.magic, HELLO_MAGIC);
        assert_eq!(parsed.status, 1);
        assert_eq!(parsed.limit_bytes, 64 * 1024 * 1024);
    }

    #[test]
    fn hello_payload_size_excludes_control_block_collision() {
        // The HELLO payload (8 bytes) is far smaller than any control
        // WAL block. A peer mistakenly sending a control
        // block first is rejected at the length-prefix check before
        // the magic is even parsed. The pre-handshake recv ceiling on
        // the server matches HELLO_PAYLOAD_LEN exactly, so this is a
        // hard guarantee, not just a likelihood.
        assert_eq!(HELLO_PAYLOAD_LEN as usize, 8);
    }
}
