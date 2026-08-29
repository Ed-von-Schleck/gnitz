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
    // Magic must sit at offsets 0..4, version at 4..6, reserved zero
    // padding at 6..8.
    let payload = encode_hello_payload(0x1234);
    assert_eq!(payload.len(), HELLO_PAYLOAD_LEN as usize);
    let magic = crate::read_u32_le(&payload, 0);
    assert_eq!(magic, HELLO_MAGIC);
    let version = u16::from_le_bytes(payload[4..6].try_into().unwrap());
    assert_eq!(version, 0x1234);
    assert_eq!(&payload[6..8], &[0, 0]);
}

#[test]
fn hello_payload_decode_roundtrip() {
    let payload = encode_hello_payload(7);
    let h = decode_hello_payload(&payload).unwrap();
    assert_eq!(h.magic, HELLO_MAGIC);
    assert_eq!(h.version, 7);
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
    let ack = encode_hello_ack(HELLO_STATUS_OK, 16 * 1024 * 1024, 0x0102_0304_0506_0708);
    assert_eq!(ack.len(), HELLO_ACK_FRAME_SIZE);
    let prefix = crate::read_u32_le(&ack, 0);
    assert_eq!(prefix, HELLO_ACK_PAYLOAD_LEN);
    let magic = crate::read_u32_le(&ack, 4);
    assert_eq!(magic, HELLO_MAGIC);
    let status = u16::from_le_bytes(ack[8..10].try_into().unwrap());
    assert_eq!(status, HELLO_STATUS_OK);
    // [10..12] are reserved padding.
    let limit = crate::read_u32_le(&ack, 12);
    assert_eq!(limit, 16 * 1024 * 1024);
    let published_lsn = crate::read_u64_le(&ack, 16);
    assert_eq!(published_lsn, 0x0102_0304_0506_0708);
}

#[test]
fn ack_decode_roundtrip() {
    // Raw non-zero status (not HELLO_STATUS_OK=0) so the round-trip proves
    // status is read from its own offset, distinct from the zero `_pad`.
    let ack = encode_hello_ack(1, 64 * 1024 * 1024, 42);
    let parsed = decode_hello_ack(&ack[4..]).unwrap();
    assert_eq!(parsed.magic, HELLO_MAGIC);
    assert_eq!(parsed.status, 1);
    assert_eq!(parsed.limit_bytes, 64 * 1024 * 1024);
    assert_eq!(parsed.published_lsn, 42);
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
