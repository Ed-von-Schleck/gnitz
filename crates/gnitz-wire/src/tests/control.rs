use super::*;

/// A header with a distinct wide value per field, so a transposed pair or a
/// truncated width fails an assertion rather than round-tripping unnoticed.
fn probe_header() -> ControlHeader {
    ControlHeader {
        target_id: 0x1111_2222_3333_4444,
        flags: WireFlags {
            verb: Some(ClientVerb::ScanMulti),
            conflict_mode: WireConflictMode::Error,
            continuation: true,
            scan_last: true,
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
fn frame(hdr: &ControlHeader, blob: &[u8], data: bool) -> Vec<u8> {
    let data = data.then_some(&ROW[..]);
    let mut out = Vec::new();
    append_frame(&mut out, hdr, blob, data);
    assert_eq!(out.len(), frame_size(blob, data));
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
        let buf = frame(&hdr, blob, false);
        let dec = peek_control_block(&buf).expect("decode");
        assert_eq!(dec.hdr, hdr);
        assert_eq!(&buf[dec.blob.clone()], blob);
        assert_eq!(dec.body, buf.len()..buf.len(), "nothing follows the blob");
        assert_eq!(dec.data, None);
        let want = fault.map(|text| WireFault { status: hdr.status, text: text.into() });
        assert_eq!(dec.fault(&buf), want);
    }
}

/// The data bit is set from what the head writes and the peek locates the
/// self-sizing data block from it. A frame cut anywhere short of its announced
/// block is refused.
#[test]
fn peek_locates_the_data_block_the_head_announced() {
    let hdr = probe_header();
    let db = crate::wal::block_size(&ROW);
    let head = CTRL_HEADER_SIZE + 3;
    for (data, want_data) in [(false, None), (true, Some(head..head + db))] {
        let buf = frame(&hdr, b"abc", data);
        let dec = peek_control_block(&buf).expect("decode");
        assert_eq!(dec.hdr, hdr, "the data bit is not a flags field");
        assert_eq!(dec.data, want_data);
        assert_eq!(dec.body, buf.len()..buf.len(), "the body starts past the last section");
    }
    let mut buf = frame(&hdr, b"abc", true);
    for cut in 0..buf.len() {
        assert!(peek_control_block(&buf[..cut]).is_err(), "cut at {cut}");
    }
    // Bytes past the last section are the body.
    let end = buf.len();
    buf.extend_from_slice(b"items");
    assert_eq!(peek_control_block(&buf).unwrap().body, end..end + 5);
}

/// A single-item frame carries the sections its verb's shape allows and
/// requires, and ends at its last one: a data block on a read above all, which
/// would otherwise be answered with a streamed table dump, is refused. A
/// multi-item frame carries its items and nothing else — no blob, no data
/// block, no `target_id`.
#[test]
fn client_verb_refuses_sections_the_verb_does_not_carry() {
    let accepts = |verb, target_id, blob: &[u8], data, tail: &[u8]| {
        let hdr = ControlHeader {
            target_id,
            flags: WireFlags { verb: Some(verb), ..Default::default() },
            ..Default::default()
        };
        let mut buf = frame(&hdr, blob, data);
        buf.extend_from_slice(tail);
        let got = peek_control_block(&buf).unwrap().client_verb();
        assert!(got.is_err() || got == Ok(verb));
        got.is_ok()
    };
    for &verb in ClientVerb::ALL {
        let VerbShape { sections, items } = VerbShape::of(verb);
        if items.is_some() {
            assert!(accepts(verb, 0, b"", false, b"items"), "{verb:?} items");
            assert!(!accepts(verb, 0, b"blob", false, b"items"), "{verb:?} with a blob");
            assert!(!accepts(verb, 0, b"", true, b"items"), "{verb:?} with a data block");
            assert!(!accepts(verb, 7, b"", false, b"items"), "{verb:?} naming a target");
            continue;
        }
        for (blob, data) in [(&b""[..], false), (b"blob", false), (b"", true), (b"blob", true)] {
            let fits = |rule: Section, present: bool| match rule {
                Section::Never => !present,
                Section::May => true,
                Section::Must => present,
            };
            assert_eq!(
                accepts(verb, 7, blob, data, b""),
                fits(sections.blob, !blob.is_empty()) && fits(sections.data, data),
                "{verb:?} blob {blob:?} data {data}"
            );
        }
        let blob: &[u8] = if sections.blob == Section::Must { b"blob" } else { b"" };
        assert!(!accepts(verb, 0, blob, false, b"items"), "{verb:?} tail");
    }
    // A request naming no verb is a reply's header.
    let buf = frame(&ControlHeader::default(), b"", false);
    assert!(peek_control_block(&buf).unwrap().client_verb().is_err());
}

/// A client frame carries no status and no reply flag, and sets a conflict mode
/// only where its verb's shape reads one, never on a multi-item prologue; a multi-item prologue sets no argument but a `DELTA_POLL`'s
/// `arg0`.
#[test]
fn client_verb_refuses_a_header_word_the_verb_does_not_carry() {
    let verdict = |hdr: ControlHeader| {
        let shape = VerbShape::of(hdr.flags.verb.unwrap());
        let tail: &[u8] = if shape.items.is_some() { b"items" } else { b"" };
        // A multi-item prologue carries no blob, whatever its items carry.
        let blob: &[u8] = match (hdr.status, shape.sections.blob) {
            (WireStatus::Ok, Section::Must) if shape.items.is_none() => b"blob",
            (WireStatus::Ok, _) => b"",
            _ => b"why",
        };
        let mut buf = frame(&hdr, blob, false);
        buf.extend_from_slice(tail);
        peek_control_block(&buf).unwrap().client_verb()
    };
    for &verb in ClientVerb::ALL {
        let multi = VerbShape::of(verb).items.is_some();
        let hdr = ControlHeader {
            flags: WireFlags { verb: Some(verb), ..Default::default() },
            ..Default::default()
        };
        assert_eq!(verdict(hdr), Ok(verb), "{verb:?}");
        let flagged = [
            WireFlags { continuation: true, ..hdr.flags },
            WireFlags { scan_last: true, ..hdr.flags },
            WireFlags { lane: WireLane::SyncAnswer, ..hdr.flags },
        ];
        for flags in flagged {
            let err = verdict(ControlHeader { flags, ..hdr }).unwrap_err();
            assert!(err.contains("flag"), "{verb:?} {flags:?}: {err}");
        }
        let err = verdict(ControlHeader { status: WireStatus::Error, ..hdr }).unwrap_err();
        assert!(err.contains("status"), "{verb:?}: {err}");
        let flags = WireFlags {
            conflict_mode: WireConflictMode::Error,
            ..hdr.flags
        };
        assert_eq!(
            verdict(ControlHeader { flags, ..hdr }).is_ok(),
            !multi && VerbShape::of(verb).sections.conflict_mode,
            "{verb:?} conflict mode"
        );
        assert_eq!(
            verdict(ControlHeader { arg1: 1, ..hdr }).is_ok(),
            !multi,
            "{verb:?} arg1"
        );
        assert_eq!(
            verdict(ControlHeader { arg0: 1, ..hdr }).is_ok(),
            !multi || verb == ClientVerb::DeltaPoll,
            "{verb:?} arg0"
        );
    }
}

/// A header word the codec cannot read is refused, never read past or coerced.
#[test]
fn peek_rejects_an_unreadable_header_word() {
    let clean = frame(&probe_header(), b"gone", false);
    let forged = |off: usize, word: &[u8]| {
        let mut buf = clean.clone();
        buf[off..off + word.len()].copy_from_slice(word);
        buf
    };
    let bad_status = crate::outside(WireStatus::from_wire);
    for (buf, want) in [
        (forged(36, &u32::MAX.to_le_bytes()), "control blob runs past the frame"),
        (forged(32, &bad_status.to_le_bytes()), "control header names no status"),
        (forged(0, &(1u64 << 63).to_le_bytes()), "reserved bits"),
    ] {
        let err = peek_control_block(&buf).unwrap_err();
        assert!(err.contains(want), "{err:?} does not name {want:?}");
    }
}

/// Every combination of field values survives `unpack(pack())`, so a field
/// bleeding into a neighbour's bits shows up as a wrong neighbour.
#[test]
fn wire_flags_roundtrip_every_combination() {
    for verb in ClientVerb::ALL.iter().copied().map(Some).chain([None]) {
        for &conflict_mode in WireConflictMode::ALL {
            for (bits, &lane) in (0..4u8).flat_map(|bits| WireLane::ALL.iter().map(move |l| (bits, l))) {
                let f = WireFlags {
                    verb,
                    conflict_mode,
                    continuation: bits & 1 != 0,
                    scan_last: bits & 2 != 0,
                    lane,
                };
                assert_eq!(WireFlags::unpack(f.pack()), Ok(f), "{f:?}");
            }
        }
    }
}

/// A word naming a verb or mode this build does not define, or setting a bit
/// outside the layout, is refused rather than coerced — coercing would turn a
/// mode the server does not implement into a silent upsert on a client's push.
#[test]
fn wire_flags_reject_unknown() {
    // Every word but the unknown verb's names a defined verb, so the field
    // under test is the one refused.
    let verb = WireFlags {
        verb: Some(ClientVerb::Push),
        ..Default::default()
    }
    .pack();
    for bit in 21..64 {
        assert_eq!(
            WireFlags::unpack(verb | 1 << bit),
            Err("flags: reserved bits set"),
            "bit {bit}"
        );
    }
    // The data block bit is the frame codec's, so the flags word neither
    // refuses nor carries it.
    assert_eq!(WireFlags::unpack(FLAG_HAS_DATA), Ok(WireFlags::default()));
    // A verb of 0 is none: a reply's.
    assert_eq!(WireFlags::unpack(0).map(|f| f.verb), Ok(None));
    // The two-bit lane leaves one value unused.
    let lane = u64::from(crate::outside(WireLane::from_wire));
    assert!(lane < 4, "a two-bit field holds no unknown value");
    for (w, want) in [
        (
            u64::from((1..=u8::MAX).find(|&v| ClientVerb::from_wire(v).is_none()).unwrap()),
            "unknown request verb",
        ),
        (
            verb | u64::from(crate::outside(WireConflictMode::from_wire)) << 8,
            "unknown conflict mode",
        ),
        (verb | lane << LANE_SHIFT, "unknown lane"),
    ] {
        assert_eq!(WireFlags::unpack(w), Err(format!("flags: {want}").as_str()), "{w:#x}");
    }
}

/// Each reply header the server builds reads back, through the client's
/// decoders, as what it was built from.
#[test]
fn every_reply_header_reads_back_as_built() {
    let peeked = |hdr: ControlHeader, blob: &[u8]| {
        let mut buf = Vec::new();
        append_frame(&mut buf, &hdr, blob, None);
        let ctrl = peek_control_block(&buf).unwrap();
        assert_eq!(ctrl.hdr, hdr);
        ctrl
    };
    let sub = SubId::new(5).unwrap();
    let at = DeltaCursor::from_pair(0xABCD, 9).unwrap();

    let ack = peeked(ControlHeader::ack(7, 42), &[]);
    assert_eq!(ack.server_lane(), Ok(ServerLane::Reply));
    assert_eq!((ack.hdr.target_id, ack.hdr.arg0), (7, 42));
    for last in [false, true] {
        let train = peeked(ControlHeader::train(7, last), &[]);
        assert_eq!(train.server_lane(), Ok(ServerLane::Reply));
        let f = train.hdr.flags;
        assert!(f.continuation);
        assert_eq!(f.scan_last, last);
    }
    let terminal = peeked(ControlHeader::delta_terminal(7, at), &[]);
    assert_eq!(terminal.server_lane(), Ok(ServerLane::Reply));
    assert!(!terminal.hdr.flags.continuation);
    assert_eq!((terminal.hdr.target_id, terminal.hdr.delta_cursor()), (7, Some(at)));
    let opener = peeked(ControlHeader::pushed_train(7, sub), &[]);
    assert_eq!(opener.server_lane(), Ok(ServerLane::PushedTrain { view: 7, sub }));
    for status in [WireStatus::Ok, WireStatus::Error] {
        let answer = peeked(ControlHeader::sync_answer(status), b"why");
        assert_eq!(answer.server_lane(), Ok(ServerLane::SyncAnswer));
        assert_eq!(answer.hdr.status, status);
    }
}

/// A pushed-train opener that is not a bare head naming a subscription, and a
/// sync answer naming a relation or carrying a data block, are refused.
#[test]
fn a_malformed_server_lane_is_refused() {
    let lane = |hdr: ControlHeader, blob: &[u8], data: bool| {
        peek_control_block(&frame(&hdr, blob, data)).unwrap().server_lane()
    };
    let opener = ControlHeader::pushed_train(7, SubId::new(5).unwrap());
    let bare = Err("a pushed train opens with a bare head");
    assert_eq!(lane(opener, b"blob", false), bare);
    assert_eq!(lane(opener, &[], true), bare);
    assert_eq!(
        lane(
            ControlHeader {
                flags: WireFlags { continuation: true, ..opener.flags },
                ..opener
            },
            &[],
            false
        ),
        bare
    );
    assert_eq!(
        lane(ControlHeader { status: WireStatus::Error, ..opener }, b"why", false),
        bare
    );
    assert_eq!(
        lane(ControlHeader { arg0: 0, ..opener }, &[], false),
        Err("a pushed train names no subscription")
    );
    let mut trailing = frame(&opener, &[], false);
    trailing.push(0);
    assert_eq!(peek_control_block(&trailing).unwrap().server_lane(), bare);

    let answer = ControlHeader::sync_answer(WireStatus::Ok);
    let not_one = Err("a sync answer is one empty frame naming 0");
    assert_eq!(lane(ControlHeader { target_id: 1, ..answer }, &[], false), not_one);
    assert_eq!(lane(answer, &[], true), not_one);
    assert_eq!(
        lane(
            ControlHeader {
                flags: WireFlags { continuation: true, ..answer.flags },
                ..answer
            },
            &[],
            false
        ),
        not_one
    );
}
