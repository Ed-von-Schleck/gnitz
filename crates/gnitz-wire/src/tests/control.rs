use super::*;

/// A header with a distinct wide value per field, so a transposed pair or a
/// truncated width fails an assertion rather than round-tripping unnoticed.
fn probe_header() -> ControlHeader {
    ControlHeader {
        target_id: 0x1111_2222_3333_4444,
        flags: WireFlags {
            verb: crate::ClientVerb::ScanSpec,
            conflict_mode: crate::WireConflictMode::Error,
            schema_version: 0xBBCC,
            has_schema: true,
            continuation: true,
            scan_last: true,
            probe_mode: crate::WireProbeMode::AllHolders,
            ..Default::default()
        },
        arg0: 0xDDDD_EEEE_FFFF_0011,
        arg1: 0xAA_BB_CC_DD_EE_FF_00_11,
        status: WireStatus::NotFound,
    }
}

fn encode(hdr: &ControlHeader, blob: &[u8]) -> Vec<u8> {
    let mut buf = vec![0u8; ctrl_block_size(blob.len())];
    let n = encode_ctrl_block(&mut buf, hdr, blob);
    assert_eq!(n, buf.len());
    buf
}

/// The header and blob round-trip with an empty and a non-empty blob, and under
/// a non-`Ok` status, whose blob is its error text.
#[test]
fn encode_roundtrips() {
    let hdr = probe_header();
    let err = ControlHeader { status: WireStatus::Error, ..hdr };
    for (hdr, blob) in [
        (hdr, &b""[..]),
        (hdr, b"a blob well past any inline threshold"),
        (err, b"table not found"),
    ] {
        let buf = encode(&hdr, blob);
        let dec = peek_control_block(&buf).expect("decode");
        assert_eq!(dec.hdr, hdr);
        assert_eq!(dec.blob, blob);
        assert_eq!(dec.block_size, CTRL_HEADER_SIZE + blob.len());
    }
}

#[test]
fn peek_rejects_a_truncated_header() {
    let buf = encode(&probe_header(), b"");
    assert_eq!(
        peek_control_block(&buf[..CTRL_HEADER_SIZE - 1]).err(),
        Some("control header truncated")
    );
}

/// A `BLOB_LEN` past the bytes present is refused, never read past.
#[test]
fn peek_rejects_a_blob_past_the_frame() {
    let mut buf = encode(&probe_header(), b"abcd");
    for len in [5u32, u32::MAX] {
        buf[OFF_BLOB_LEN..OFF_BLOB_LEN + 4].copy_from_slice(&len.to_le_bytes());
        assert_eq!(
            peek_control_block(&buf).err(),
            Some("control blob runs past the frame"),
            "BLOB_LEN {len}"
        );
    }
}

/// A status word naming no [`WireStatus`] is a decode error, not a code the
/// reader has to branch past.
#[test]
fn peek_rejects_an_unknown_status() {
    let mut buf = encode(&probe_header(), b"");
    buf[OFF_STATUS..OFF_STATUS + 4].copy_from_slice(&7u32.to_le_bytes());
    assert_eq!(peek_control_block(&buf).err(), Some("control header names no status"));
}

#[test]
fn peek_rejects_reserved_flag_bits() {
    let mut buf = encode(&probe_header(), b"");
    buf[OFF_FLAGS..OFF_FLAGS + 8].copy_from_slice(&(1u64 << 63).to_le_bytes());
    assert_eq!(peek_control_block(&buf).err(), Some("flags: reserved bits set"));
}
