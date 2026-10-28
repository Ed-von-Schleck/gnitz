use super::*;

/// A header with a distinct wide value per field, so a transposed pair or a
/// truncated width fails an assertion rather than round-tripping unnoticed.
fn probe_header() -> ControlHeader {
    ControlHeader {
        target_id: 0x1111_2222_3333_4444,
        flags: WireFlags {
            verb: ClientVerb::ScanSpec,
            conflict_mode: WireConflictMode::Error,
            continuation: true,
            scan_last: true,
            probe_mode: WireProbeMode::Index,
            lane: WireLane::SyncAnswer,
        },
        arg0: 0xDDDD_EEEE_FFFF_0011,
        arg1: 0xAA_BB_CC_DD_EE_FF_00_11,
        status: WireStatus::NotFound,
    }
}

/// A one-row canonical region list.
const ROW: [&[u8]; 4] = [b"a data region", &[1, 0, 0, 0, 0, 0, 0, 0], &[0; 8], &[]];

/// A whole frame, sized exactly by `frame_size`.
fn frame(hdr: &ControlHeader, blob: &[u8], schema: Option<&[u8]>, data: bool) -> Vec<u8> {
    let data = data.then_some(&ROW[..]);
    let mut out = Vec::new();
    append_frame(&mut out, hdr, blob, schema, data);
    assert_eq!(out.len(), frame_size(blob, schema, data));
    out
}

/// The header and blob round-trip with an empty and a non-empty blob, and a
/// non-`Ok` frame's fault is its status and its blob, read lossily.
#[test]
fn encode_roundtrips() {
    let hdr = probe_header();
    for (hdr, blob, fault) in [
        (ControlHeader { status: WireStatus::Ok, ..hdr }, &b""[..], None),
        (hdr, b"blob", Some("blob")),
        (
            ControlHeader { status: WireStatus::Error, ..hdr },
            b"full \xFF",
            Some("full \u{FFFD}"),
        ),
    ] {
        let buf = frame(&hdr, blob, None, false);
        let dec = peek_control_block(&buf).expect("decode");
        assert_eq!(dec.hdr, hdr);
        assert_eq!(&buf[dec.blob.clone()], blob);
        assert_eq!(dec.body, buf.len()..buf.len(), "nothing follows the blob");
        assert_eq!((dec.schema.clone(), dec.data.clone()), (None, None));
        let want = fault.map(|text| WireFault { status: hdr.status, text: text.into() });
        assert_eq!(dec.fault(&buf), want);
    }
}

/// The section bits are set from what the head writes and the peek locates each
/// section from them, in order: the length-prefixed schema record, then the
/// self-sizing data block. A frame cut anywhere short of its announced sections
/// is refused.
#[test]
fn peek_locates_the_sections_the_head_announced() {
    let hdr = probe_header();
    // Arbitrary bytes: the frame sizes the record from its prefix, never parses it.
    let sb = b"schema record bytes";
    let db = crate::wal::block_size(&ROW);
    let head = CTRL_HEADER_SIZE + 3;
    let schema_at = head + 4..head + 4 + sb.len();
    let cases = [
        (Some(&sb[..]), false, Some(schema_at.clone()), None),
        (None, true, None, Some(head..head + db)),
        (
            Some(&sb[..]),
            true,
            Some(schema_at.clone()),
            Some(schema_at.end..schema_at.end + db),
        ),
    ];
    for (schema, data, want_schema, want_data) in cases {
        let buf = frame(&hdr, b"abc", schema, data);
        let dec = peek_control_block(&buf).expect("decode");
        assert_eq!(dec.hdr, hdr, "the section bits are not a flags field");
        assert_eq!(dec.schema, want_schema);
        assert_eq!(dec.data, want_data);
        assert_eq!(dec.body, buf.len()..buf.len(), "the body starts past the last section");
    }
    let mut buf = frame(&hdr, b"abc", Some(sb), true);
    for cut in 0..buf.len() {
        assert!(peek_control_block(&buf[..cut]).is_err(), "cut at {cut}");
    }
    // Bytes past the last section are the body.
    let end = buf.len();
    buf.extend_from_slice(b"items");
    assert_eq!(peek_control_block(&buf).unwrap().body, end..end + 5);
    // An empty record is a present one: the prefix distinguishes it from absent.
    let empty = frame(&hdr, b"abc", Some(&[]), false);
    assert_eq!(peek_control_block(&empty).unwrap().schema, Some(head + 4..head + 4));
}

/// A data block is malformed on every verb but PUSH — a read above all, which
/// would otherwise be answered with a streamed table dump. A multi-item frame
/// carries its items and nothing else — no blob, no schema, no `target_id` —
/// and every other frame ends at its last section.
#[test]
fn client_verb_refuses_sections_the_verb_does_not_carry() {
    let accepts = |verb, target_id, blob: &[u8], schema: Option<&[u8]>, data, tail: &[u8]| {
        let hdr = ControlHeader {
            target_id,
            flags: WireFlags { verb, ..Default::default() },
            ..Default::default()
        };
        let mut buf = frame(&hdr, blob, schema, data);
        buf.extend_from_slice(tail);
        let got = peek_control_block(&buf).unwrap().client_verb();
        assert!(got.is_err() || got == Ok(verb));
        got.is_ok()
    };
    for &verb in ClientVerb::ALL {
        let multi = crate::txn_frame::item_shape(verb).is_some();
        assert!(accepts(verb, 0, b"", None, false, b""), "{verb:?} bare");
        assert_eq!(
            accepts(verb, 0, b"", None, true, b""),
            verb == ClientVerb::Push,
            "{verb:?} data"
        );
        assert_eq!(accepts(verb, 0, b"", None, false, b"items"), multi, "{verb:?} tail");
        assert_eq!(
            accepts(verb, 7, b"blob", Some(b"sch"), false, b""),
            !multi,
            "{verb:?} sections"
        );
        if multi {
            assert!(
                !accepts(verb, 0, b"blob", None, false, b"items"),
                "{verb:?} with a blob"
            );
            assert!(
                !accepts(verb, 0, b"", Some(b"sch"), false, b"items"),
                "{verb:?} with a schema"
            );
            assert!(
                !accepts(verb, 7, b"", None, false, b"items"),
                "{verb:?} naming a target"
            );
        }
    }
}

/// A header word the codec cannot read is refused, never read past or coerced.
#[test]
fn peek_rejects_an_unreadable_header_word() {
    let clean = frame(&probe_header(), b"gone", None, false);
    let forged = |off: usize, word: &[u8]| {
        let mut buf = clean.clone();
        buf[off..off + word.len()].copy_from_slice(word);
        buf
    };
    let bad_status = crate::outside(WireStatus::from_wire);
    for (buf, want) in [
        (
            forged(OFF_BLOB_LEN, &u32::MAX.to_le_bytes()),
            "control blob runs past the frame",
        ),
        (
            forged(OFF_STATUS, &bad_status.to_le_bytes()),
            "control header names no status",
        ),
        (forged(OFF_FLAGS, &(1u64 << 63).to_le_bytes()), "reserved bits"),
    ] {
        let err = peek_control_block(&buf).unwrap_err();
        assert!(err.contains(want), "{err:?} does not name {want:?}");
    }
}

/// Every combination of field values survives `unpack(pack())`, so a field
/// bleeding into a neighbour's bits shows up as a wrong neighbour.
#[test]
fn wire_flags_roundtrip_every_combination() {
    for &verb in ClientVerb::ALL {
        for &conflict_mode in WireConflictMode::ALL {
            for &probe_mode in WireProbeMode::ALL {
                for (bits, &lane) in (0..4u8).flat_map(|bits| WireLane::ALL.iter().map(move |l| (bits, l))) {
                    let f = WireFlags {
                        verb,
                        conflict_mode,
                        probe_mode,
                        continuation: bits & 1 != 0,
                        scan_last: bits & 2 != 0,
                        lane,
                    };
                    assert_eq!(WireFlags::unpack(f.pack()), Ok(f), "{f:?}");
                }
            }
        }
    }
}

/// A word naming a verb or mode this build does not define, or setting a bit
/// outside the layout, is refused rather than coerced — coercing would turn a
/// mode the server does not implement into a silent upsert on a client's push.
#[test]
fn wire_flags_reject_unknown() {
    for bit in (16..32).chain(40..64) {
        assert!(WireFlags::unpack(1 << bit).is_err(), "bit {bit}");
    }
    // Bits 32/33 are the frame codec's, so the flags word neither refuses nor
    // carries them.
    assert_eq!(WireFlags::unpack(3 << 32), Ok(WireFlags::default()));
    // The two-bit fields leave one value unused.
    let probe = u64::from(crate::outside(WireProbeMode::from_wire));
    let lane = u64::from(crate::outside(WireLane::from_wire));
    assert!(probe < 4 && lane < 4, "a two-bit field holds no unknown value");
    for (w, want) in [
        (u64::from(crate::outside(ClientVerb::from_wire)), "unknown request verb"),
        (
            u64::from(crate::outside(WireConflictMode::from_wire)) << 8,
            "unknown conflict mode",
        ),
        (probe << 36, "unknown probe mode"),
        (lane << 38, "unknown lane"),
    ] {
        assert_eq!(WireFlags::unpack(w), Err(format!("flags: {want}").as_str()), "{w:#x}");
    }
}

/// Every probe round-trips through its wire words, and a triple no probe
/// encodes to is refused.
#[test]
fn a_probe_roundtrips_through_its_wire_words() {
    let cols = PkColList::from_slice(&[2, 5]);
    let cap = |n| std::num::NonZeroU64::new(n).unwrap();
    let probes = [
        Probe::Pk,
        Probe::PkColumn(4),
        Probe::Index(cols, cap(1)),
        Probe::Index(cols, cap(9)),
        Probe::Index(cols, std::num::NonZeroU64::MAX),
    ];
    for probe in probes {
        let (mode, arg0, arg1) = probe.wire();
        assert_eq!(Probe::from_wire(mode, arg0, arg1), Ok(probe));
    }
    let index = cols.pack();
    for (mode, arg0, arg1) in [
        (WireProbeMode::Pk, 5, 0),
        (WireProbeMode::Pk, 0, index),
        (WireProbeMode::Pk, 0, 7),
        (WireProbeMode::PkColumn, 1 << 32, 0),
        (WireProbeMode::PkColumn, 4, index),
        (WireProbeMode::Index, 0, index),
        (WireProbeMode::Index, 9, 0),
        (WireProbeMode::Index, 9, 7),
    ] {
        assert!(Probe::from_wire(mode, arg0, arg1).is_err(), "{mode:?} {arg0} {arg1}");
    }
}
