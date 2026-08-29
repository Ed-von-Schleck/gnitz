use super::*;

/// A header with a distinct wide value per field, so a transposed pair or a
/// truncated width fails an assertion rather than round-tripping unnoticed.
fn probe_header() -> ControlHeader {
    ControlHeader {
        target_id: 0x1111_2222_3333_4444,
        client_id: 0x5555_6666_7777_8888,
        flags: 0x9999_AAAA_BBBB_CCCC,
        seek_pk: (0xDDDD_EEEE_FFFF_0011u128 << 64) | 0x2233_4455_6677_8899u128,
        seek_col_idx: 0xAA_BB_CC_DD_EE_FF_00_11,
        request_id: 0x1234_5678_9ABC_DEF0,
        status: 0xDEAD_BEEF,
    }
}

/// Every encode path — template fast path, inline strings, blob spill —
/// round-trips, at both trust levels and at a non-zero offset (a write
/// indexing through `out[offset + OFF_X..]` instead of the sub-slice then
/// lands outside the block).
#[test]
fn encode_roundtrip_all_paths() {
    const OFFSET: usize = 64;
    for &checksum in &[false, true] {
        let peek = |b: &[u8]| {
            if checksum {
                peek_control_block(b)
            } else {
                peek_control_block_ipc(b)
            }
        };
        let hdr = probe_header();

        // Fast path.
        let mut buf = vec![0u8; OFFSET + ctrl_block_size(0, 0)];
        let n = encode_ctrl_block(&mut buf, OFFSET, &hdr, b"", b"", checksum);
        assert_eq!(n, CTRL_BLOCK_SIZE_NO_BLOB);
        let dec = peek(&buf[OFFSET..OFFSET + n]).expect("decode empty");
        assert_eq!(dec.header(), hdr, "checksum={checksum}");
        assert!(dec.error_msg.is_empty());
        assert!(dec.seek_pk_extra.is_empty());
        assert_eq!(dec.block_size, n);

        // Inline strings (≤ 12 bytes, no blob spill).
        let short = b"abcd";
        let mut buf = vec![0u8; OFFSET + ctrl_block_size(short.len(), short.len())];
        let n = encode_ctrl_block(&mut buf, OFFSET, &hdr, short, short, checksum);
        assert_eq!(n, CTRL_BLOCK_SIZE_NO_BLOB, "inline strings must not grow the block");
        let dec = peek(&buf[OFFSET..OFFSET + n]).expect("decode short");
        assert_eq!(dec.error_msg, short);
        assert_eq!(dec.seek_pk_extra, short);

        // Both spill into the shared blob region, so the decoder has to
        // resolve one heap and hand each cell the right slice of it.
        let err = b"this error message is definitely longer than twelve bytes";
        let extra = b"and so is this wide-pk-extra blob payload past 12B";
        let mut buf = vec![0u8; OFFSET + ctrl_block_size(err.len(), extra.len())];
        let n = encode_ctrl_block(&mut buf, OFFSET, &hdr, err, extra, checksum);
        assert_eq!(n, CTRL_BLOCK_SIZE_NO_BLOB + err.len() + extra.len());
        let dec = peek(&buf[OFFSET..OFFSET + n]).expect("decode long");
        assert_eq!(dec.header(), hdr);
        assert_eq!(dec.error_msg, err);
        assert_eq!(dec.seek_pk_extra, extra);
        assert_eq!(dec.block_size, n);
    }
}

/// The directory is part of the format, not something a sender gets to
/// choose. Re-pointing a scalar field's region at the blob heap — which the
/// old per-field extent check honoured, since the extent stayed in-block —
/// must be rejected outright.
#[test]
fn peek_rejects_a_directory_that_disagrees_with_the_template() {
    let mut buf = vec![0u8; ctrl_block_size(0, 0)];
    let n = encode_ctrl_block(&mut buf, 0, &probe_header(), b"", b"", false);
    buf.truncate(n);

    // Aim `status` at `client_id`'s bytes: a well-formed, in-block extent.
    let mut forged = buf.clone();
    crate::write_u32_le(
        &mut forged,
        crate::wal::dir_entry_offset(REG_STATUS),
        OFF_CLIENT_ID as u32,
    );
    crate::wal::stamp_checksum(&mut forged, n);
    assert_eq!(
        peek_control_block(&forged).err(),
        Some("control block directory is not the canonical layout")
    );

    // The blob region's *size* is the one entry a sender legitimately varies.
    let long = b"an error message well past the twelve-byte inline threshold";
    let mut ok = vec![0u8; ctrl_block_size(long.len(), 0)];
    let n = encode_ctrl_block(&mut ok, 0, &probe_header(), long, b"", false);
    assert_eq!(peek_control_block_ipc(&ok[..n]).unwrap().error_msg, long);
}

/// A corrupted long-string blob offset must surface an error, not panic.
#[test]
fn peek_rejects_oob_error_msg_offset() {
    let long_msg = b"this error message exceeds twelve bytes so it spills into the blob";
    let mut buf = vec![0u8; ctrl_block_size(long_msg.len(), 0)];
    let n = encode_ctrl_block(
        &mut buf,
        0,
        &ControlHeader {
            status: 1,
            target_id: 1,
            client_id: 2,
            ..Default::default()
        },
        long_msg,
        b"",
        false,
    );
    buf.truncate(n);
    let (err_off, _) = crate::wal::dir_entry(&buf, REG_ERROR_MSG);
    buf[err_off + 8..err_off + 16].copy_from_slice(&u64::MAX.to_le_bytes());
    match peek_control_block_ipc(&buf) {
        Err("error_msg string offset out of bounds") => {}
        Err(other) => panic!("wrong error: {other}"),
        Ok(_) => panic!("OOB error_msg offset must be rejected"),
    }
}
