use super::*;

/// The HELLO payload's byte layout and its decode are one contract: magic at
/// 0..4, version at 4..6, reserved zero padding at 6..8, and nothing else is a
/// HELLO.
#[test]
fn hello_payload_layout_and_decode() {
    let payload = encode_hello_payload(0x1234);
    assert_eq!(payload.len(), HELLO_PAYLOAD_LEN as usize);
    assert_eq!(crate::read_u32_le(&payload, 0), HELLO_MAGIC);
    assert_eq!(crate::read_u16_le(&payload, 4), 0x1234);
    assert_eq!(&payload[6..8], &[0, 0]);

    let h = decode_hello_payload(&payload).expect("a well-formed payload");
    assert_eq!(h.magic, HELLO_MAGIC);
    assert_eq!(h.version, 0x1234);

    // The caller has already framed the payload by its length prefix, so any
    // other width is a forged frame, not a short read.
    assert!(decode_hello_payload(&[0u8; 7]).is_err());
    assert!(decode_hello_payload(&[0u8; 9]).is_err());
}

/// The ACK's byte layout and its decode, likewise. `encode_hello_ack` emits the
/// framed form (length prefix included); `decode_hello_ack` takes the payload.
#[test]
fn ack_frame_layout_and_decode() {
    let ack = encode_hello_ack(16 * 1024 * 1024, 0x0102_0304_0506_0708);
    assert_eq!(ack.len(), HELLO_ACK_FRAME_SIZE);
    assert_eq!(crate::read_u32_le(&ack, 0), HELLO_ACK_PAYLOAD_LEN);
    assert_eq!(crate::read_u32_le(&ack, 4), HELLO_MAGIC);
    assert_eq!(crate::read_u32_le(&ack, 8), 16 * 1024 * 1024);
    assert_eq!(crate::read_u64_le(&ack, 12), 0x0102_0304_0506_0708);

    let parsed = decode_hello_ack(&ack[4..]).expect("a well-formed payload");
    assert_eq!(parsed.magic, HELLO_MAGIC);
    assert_eq!(parsed.limit_bytes, 16 * 1024 * 1024);
    assert_eq!(parsed.published_lsn, 0x0102_0304_0506_0708);

    assert!(decode_hello_ack(&ack[4..ack.len() - 1]).is_err());
}

/// Before the handshake completes a reader has three frames to tell apart and
/// only the length prefix to do it with: HELLO, ACK, and the one `WireStatus::Error`
/// control block this path emits. That holds only while both handshake payloads
/// are smaller than the smallest control block, and while the error block fits
/// the pre-handshake ceiling — otherwise a rejected HELLO reads as a truncated
/// frame instead of as its error.
#[test]
fn the_length_prefix_discriminates_the_pre_handshake_frames() {
    let smallest_ctrl = crate::control::CTRL_HEADER_SIZE;
    assert_ne!(HELLO_PAYLOAD_LEN, HELLO_ACK_PAYLOAD_LEN);
    assert!((HELLO_PAYLOAD_LEN as usize) < smallest_ctrl);
    assert!((HELLO_ACK_PAYLOAD_LEN as usize) < smallest_ctrl);
    assert!(crate::control::ctrl_block_size(128) < MAX_FRAME_PAYLOAD_PRE_HANDSHAKE);
}
