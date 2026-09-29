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

/// A one-row WAL block whose PK region is `pk`.
fn wal_block(pk: &'static [u8]) -> Vec<u8> {
    let mut out = Vec::new();
    crate::wal::append_block(&[pk, &1i64.to_le_bytes(), &[0; 8], &[]], 0, &mut out);
    out
}

/// The header and blob round-trip with an empty and a non-empty blob, and under
/// a non-`Ok` status, whose blob is its error text.
#[test]
fn encode_roundtrips() {
    let hdr = probe_header();
    let err = ControlHeader { status: WireStatus::Error, ..hdr };
    for (hdr, blob) in [
        (ControlHeader { status: WireStatus::Ok, ..hdr }, &b""[..]),
        (hdr, b"a blob well past any inline threshold"),
        (err, b"table not found"),
    ] {
        let buf = encode(&hdr, blob);
        let dec = peek_control_block(&buf).expect("decode");
        assert_eq!(dec.hdr, hdr);
        assert_eq!(&buf[dec.blob], blob);
        assert_eq!(dec.body, buf.len()..buf.len(), "nothing follows the blob");
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
    let (sb, db) = (b"schema record bytes".to_vec(), wal_block(b"a data region"));
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
        assert_eq!(dec.body, buf.len()..buf.len(), "the body starts past the last section");
    }
    // Bytes past the last section are the body.
    let mut buf = frame(&hdr, b"abc", Some(&sb), Some(&db));
    let end = buf.len();
    buf.extend_from_slice(b"items");
    assert_eq!(peek_control_block(&buf).unwrap().body, end..end + 5);
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
    let db = wal_block(b"row");
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

/// A multi-item frame carries its items and nothing else — no blob, no schema,
/// no `target_id` — and every other frame ends at its last section.
#[test]
fn client_verb_refuses_sections_the_verb_does_not_carry() {
    let multi = [
        ClientVerb::DdlTxn,
        ClientVerb::PushTxn,
        ClientVerb::ScanMulti,
        ClientVerb::DeltaPoll,
    ];
    let ctrl_at = |target_id, verb, blob: &[u8], schema: Option<&[u8]>, tail: &[u8]| {
        let hdr = ControlHeader {
            target_id,
            flags: WireFlags { verb, ..Default::default() },
            ..Default::default()
        };
        let mut buf = frame(&hdr, blob, schema, None);
        buf.extend_from_slice(tail);
        peek_control_block(&buf).unwrap().client_verb()
    };
    let ctrl = |verb, blob: &[u8], schema: Option<&[u8]>, tail: &[u8]| ctrl_at(0, verb, blob, schema, tail);
    for &verb in ClientVerb::ALL {
        if multi.contains(&verb) {
            assert_eq!(ctrl(verb, b"", None, b"items"), Ok(verb));
            assert!(ctrl(verb, b"blob", None, b"items").is_err(), "{verb:?} with a blob");
            assert!(
                ctrl(verb, b"", Some(b"sch"), b"items").is_err(),
                "{verb:?} with a schema"
            );
            assert!(
                ctrl_at(7, verb, b"", None, b"items").is_err(),
                "{verb:?} naming a target"
            );
        } else {
            assert_eq!(ctrl(verb, b"blob", Some(b"sch"), b""), Ok(verb));
            assert!(
                ctrl(verb, b"blob", None, b"tail").is_err(),
                "{verb:?} with trailing bytes"
            );
        }
    }
}

/// A non-`Ok` frame's fault is its status and its blob, read lossily.
#[test]
fn fault_reads_the_status_and_blob() {
    let ok = ControlHeader::default();
    let buf = encode(&ok, b"payload");
    assert_eq!(peek_control_block(&buf).unwrap().fault(&buf), None);
    let err = ControlHeader { status: WireStatus::SalFull, ..ok };
    let buf = encode(&err, b"full \xFF");
    let f = peek_control_block(&buf).unwrap().fault(&buf);
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
    let buf = encode(&probe_header(), b"gone");
    assert_eq!(
        peek_control_block(&buf[..CTRL_HEADER_SIZE - 1]).err().as_deref(),
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
            peek_control_block(&buf).err().as_deref(),
            Some("control blob runs past the frame"),
            "BLOB_LEN {len}"
        );
    }
}

/// A status word naming no [`WireStatus`] is a decode error, not a code the
/// reader has to branch past.
#[test]
fn peek_rejects_an_unknown_status() {
    let mut buf = encode(&probe_header(), b"gone");
    buf[OFF_STATUS..OFF_STATUS + 4].copy_from_slice(&8u32.to_le_bytes());
    assert_eq!(
        peek_control_block(&buf).err().as_deref(),
        Some("control header names no status")
    );
}

#[test]
fn peek_rejects_reserved_flag_bits() {
    let mut buf = encode(&probe_header(), b"gone");
    buf[OFF_FLAGS..OFF_FLAGS + 8].copy_from_slice(&(1u64 << 63).to_le_bytes());
    assert_eq!(
        peek_control_block(&buf).err().as_deref(),
        Some("flags: reserved bits set")
    );
}
