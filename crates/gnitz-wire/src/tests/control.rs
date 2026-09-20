use super::*;

/// A header with a distinct wide value per field, so a transposed pair or a
/// truncated width fails an assertion rather than round-tripping unnoticed.
fn probe_header() -> ControlHeader {
    ControlHeader {
        target_id: 0x1111_2222_3333_4444,
        flags: WireFlags {
            verb: ClientVerb::ScanSpec,
            conflict_mode: crate::WireConflictMode::Error,
            schema_version: 0xBBCC,
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
    frame(hdr, blob, None, None)
}

/// A whole frame: the head, then `data` if given.
fn frame(hdr: &ControlHeader, blob: &[u8], schema_block: Option<&[u8]>, data: Option<&[u8]>) -> Vec<u8> {
    let mut buf = vec![0u8; frame_head_size(blob.len(), schema_block.map(<[u8]>::len))];
    let n = encode_frame_head(&mut buf, hdr, blob, schema_block, data.is_some());
    assert_eq!(n, buf.len());
    buf.extend_from_slice(data.unwrap_or_default());
    buf
}

/// A stand-in WAL block of `body` region bytes.
fn wal_block(tid: u32, body: &'static [u8]) -> Vec<u8> {
    let mut regions = crate::wal::Regions::new();
    regions.push(body);
    let mut out = Vec::new();
    crate::wal::WalBlock { table_id: tid, entry_count: 1, regions }.append_to(&mut out);
    out
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
        assert_eq!((dec.schema, dec.data), (None, None));
    }
}

/// The section bits are set from what the head writes and the peek locates each
/// section from them, in order: the length-prefixed schema record, then the
/// self-sizing data block.
#[test]
fn peek_locates_the_sections_the_head_announced() {
    let hdr = probe_header();
    // Arbitrary bytes: the frame sizes the record from its prefix, never parses it.
    let (sb, db) = (b"schema record bytes".to_vec(), wal_block(1, b"a data region"));
    let head = CTRL_HEADER_SIZE + 3;
    let schema_at = head + 4..head + 4 + sb.len();
    let data_after_schema = schema_at.end..schema_at.end + db.len();
    let cases = [
        (Some(&sb[..]), None, Some(schema_at.clone()), None),
        (None, Some(&db[..]), None, Some(head..head + db.len())),
        (Some(&sb[..]), Some(&db[..]), Some(schema_at), Some(data_after_schema)),
    ];
    for (schema_block, data, want_schema, want_data) in cases {
        let buf = frame(&hdr, b"abc", schema_block, data);
        let dec = peek_control_block(&buf).expect("decode");
        assert_eq!(dec.hdr, hdr, "the section bits are not a flags field");
        assert_eq!(dec.schema, want_schema);
        assert_eq!(dec.data, want_data);
    }
    // A section the bits announce but the frame does not hold is refused, at
    // every length short of the whole.
    let buf = frame(&hdr, b"abc", Some(&sb), None);
    for cut in head..buf.len() {
        assert!(peek_control_block(&buf[..cut]).is_err(), "cut at {cut}");
    }
    // An empty record is a present one: the prefix distinguishes it from absent.
    let empty = frame(&hdr, b"abc", Some(&[]), None);
    assert_eq!(peek_control_block(&empty).unwrap().schema, Some(head + 4..head + 4));
}

/// A data block is malformed on every verb but PUSH — `Scan` above all, which
/// would otherwise be answered with a streamed table dump.
#[test]
fn client_verb_rejects_data_on_a_non_push_verb() {
    let db = wal_block(1, b"row");
    let ctrl = |verb, data: Option<&[u8]>| {
        let hdr = ControlHeader {
            flags: WireFlags { verb, ..Default::default() },
            ..Default::default()
        };
        peek_control_block(&frame(&hdr, b"", None, data)).unwrap()
    };
    assert_eq!(ctrl(ClientVerb::Push, Some(&db)).client_verb(), Ok(ClientVerb::Push));
    for &verb in ClientVerb::ALL {
        assert_eq!(ctrl(verb, None).client_verb(), Ok(verb));
        if verb != ClientVerb::Push {
            assert!(
                ctrl(verb, Some(&db)).client_verb().is_err(),
                "{verb:?} must not accept a data block"
            );
        }
    }
}

/// A non-`Ok` frame's fault is its status and its blob, read lossily.
#[test]
fn fault_reads_the_status_and_blob() {
    let ok = ControlHeader::default();
    assert_eq!(peek_control_block(&encode(&ok, b"payload")).unwrap().fault(), None);
    let err = ControlHeader { status: WireStatus::SalFull, ..ok };
    let f = peek_control_block(&encode(&err, b"full \xFF")).unwrap().fault();
    assert_eq!(
        f,
        Some(WireFault {
            status: WireStatus::SalFull,
            text: "full \u{FFFD}".into()
        })
    );
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
