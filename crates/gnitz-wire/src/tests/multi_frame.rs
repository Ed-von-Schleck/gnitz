use super::*;
use crate::{WireFlags, peek_control_block};
use crate::{WireLane, WireStatus};

/// [`decode_items`] of a whole item list.
fn items(body: &[u8], verb: ClientVerb) -> Result<Vec<DecodedControl>, String> {
    decode_items(body, 0..body.len(), verb)
}

/// The frame `encode` appends to an empty buffer.
fn encoded(encode: impl FnOnce(&mut Vec<u8>)) -> Vec<u8> {
    let mut out = Vec::new();
    encode(&mut out);
    out
}

fn rel(tid: u64, reply_layout: u64) -> ScanItem<'static> {
    ScanItem {
        target: Target { tid, token: tid ^ 0x5A5A },
        reply_layout,
        spec: if tid.is_multiple_of(2) { b"spec" } else { b"" },
    }
}

fn item(view_id: u64, after_tick: u64, reply_layout: u64) -> DeltaPollItem<'static> {
    DeltaPollItem {
        view: Target { tid: view_id, token: view_id ^ 0xA5A5 },
        from: DeltaCursor::from_pair(!after_tick, after_tick),
        reply_layout,
        spec: b"spec",
    }
}

/// The PK region every stand-in block below carries: one 24-byte key.
const PK: &[u8] = &[7u8; 24];
const WEIGHT: &[u8] = &1i64.to_le_bytes();
const NULLS: &[u8] = &[0u8; 8];

/// A tid past `u32`: the item header carries all 64 bits.
const WIDE_TID: u64 = (1 << 40) | 16;

/// A minimal one-row canonical region list.
fn regions<'a>() -> Regions<'a> {
    Regions::from_slice(&[PK, WEIGHT, NULLS, &[]])
}

/// The same block framed — what a decoded item's data section must hold.
fn block() -> Vec<u8> {
    let mut buf = Vec::new();
    crate::wal::append_block(&regions(), 0, &mut buf);
    buf
}

fn ddl_item<'a>(tid: u64) -> FrameItem<'a> {
    FrameItem {
        target: tid.into(),
        conflict_mode: WireConflictMode::default(),
        basis: 0,
        blob: &[],
        data: regions(),
    }
}

fn family(tid: u64, conflict_mode: WireConflictMode, basis: u64, schema: &[u8]) -> FrameItem<'_> {
    FrameItem {
        target: Target { tid, token: !tid },
        conflict_mode,
        basis,
        blob: schema,
        data: regions(),
    }
}

fn sub(id: u64) -> SubId {
    SubId::new(id).unwrap()
}

/// The items of a `DELTA_POLL` body under a prologue keeping none.
fn delta_items(body: &[u8]) -> Result<Vec<DeltaPollItem<'_>>, String> {
    decode_delta_poll(body, &ControlHeader::naming(ClientVerb::DeltaPoll, 0.into(), 0)).map(|p| p.items)
}

/// Peek `frame`'s prologue — it carries only its verb, and its body starts right
/// after it — check it as a client frame, and decode its body.
fn peeked<'a, T>(frame: &'a [u8], decode: impl Fn(&'a [u8]) -> Result<T, String>) -> Result<T, String> {
    let ctrl = peek_control_block(frame)?;
    let verb = ctrl.client_verb().map_err(str::to_string)?;
    assert_eq!(
        ctrl.hdr,
        ControlHeader::naming(verb, 0.into(), 0),
        "only the verb is set"
    );
    assert!(ctrl.blob.is_empty());
    assert_eq!(ctrl.body, CTRL_HEADER_SIZE..frame.len());
    decode(&frame[ctrl.body.clone()])
}

/// Each item is its own frame: its header names the frame's verb and the
/// item's fields, its bytes end where its last section does, its body is
/// empty, and its sections are the ones it was built from.
#[test]
fn txn_frames_roundtrip_every_item_in_order() {
    let (s0, s1, d) = (b"schema 0".to_vec(), b"schema one".to_vec(), block());
    let w = 0x0102_0304_0506_0708;
    for (verb, items) in [
        (ClientVerb::DdlTxn, vec![ddl_item(WIDE_TID), ddl_item(2)]),
        (
            ClientVerb::PushTxn,
            vec![
                family(16, WireConflictMode::Error, BLIND, &s0),
                family(WIDE_TID, WireConflictMode::Update, w, &s1),
            ],
        ),
    ] {
        let frame = encoded(|out| encode_items(out, verb, &items));
        let ctrl = peek_control_block(&frame).unwrap();
        let got = decode_items(&frame, ctrl.body.clone(), verb).unwrap();
        assert_eq!(got.len(), items.len());
        let mut at = ctrl.body.start;
        for (c, it) in got.iter().zip(&items) {
            let flags = WireFlags {
                verb: Some(verb),
                conflict_mode: it.conflict_mode,
                ..Default::default()
            };
            assert_eq!(
                c.hdr,
                ControlHeader {
                    flags,
                    ..ControlHeader::naming(verb, it.target, it.basis)
                }
            );
            assert_eq!(c.blob.start, at + CTRL_HEADER_SIZE, "each item follows the one before");
            assert_eq!(&frame[c.blob.clone()], it.blob);
            assert_eq!(&frame[c.data.clone().unwrap()], &d[..]);
            assert!(c.body.is_empty());
            at = c.body.end;
        }
        assert_eq!(at, frame.len());
    }
}

#[test]
fn scan_multi_roundtrips_order_and_layouts() {
    // A repeated id is two positions, answered separately.
    let rels = [rel(7, 0), rel(8, 3), rel(9, u64::MAX), rel(7, 0)];
    assert_eq!(
        peeked(&encoded(|out| encode_scan_multi(out, &rels)), decode_scan_multi).unwrap(),
        rels
    );
}

#[test]
fn delta_poll_roundtrips_every_view_in_order() {
    let views = [
        item(1, 0, 1),
        item(2, 42, u64::MAX),
        item(3, u64::MAX, 7),
        // One view twice is two items, each under its own spec.
        DeltaPollItem { spec: b"another spec", ..item(1, 0, 1) },
        item(1, 0, 1),
    ];
    let frame = encoded(|out| encode_delta_poll(out, &views, None));
    assert_eq!(peeked(&frame, delta_items).unwrap(), views);
}

/// A frame appended behind other bytes leaves them as they were.
#[test]
fn a_multi_item_frame_appends_behind_what_its_buffer_holds() {
    let rels = [rel(7, 0), rel(8, 3)];
    let mut out = b"queued".to_vec();
    encode_scan_multi(&mut out, &rels);
    assert_eq!(
        out,
        [&b"queued"[..], &encoded(|out| encode_scan_multi(out, &rels))].concat()
    );
}

/// The first id rides the prologue's `arg0` and nothing else of it moves.
#[test]
fn a_kept_delta_poll_carries_its_first_id_in_the_prologue() {
    let views = [item(1, 5, 1), item(2, 5, 1)];
    let frame = encoded(|out| encode_delta_poll(out, &views, Some(sub(1_500))));
    let ctrl = peek_control_block(&frame).unwrap();
    assert_eq!(ctrl.client_verb(), Ok(ClientVerb::DeltaPoll));
    assert_eq!(ctrl.hdr, ControlHeader::naming(ClientVerb::DeltaPoll, 0.into(), 1_500));
    let poll = decode_delta_poll(&frame[ctrl.body.clone()], &ctrl.hdr).unwrap();
    assert_eq!(poll.items, views.to_vec());
    let kept = poll.kept().expect("a kept poll");
    assert_eq!(kept.collect::<Vec<_>>(), [sub(1_500), sub(1_501)]);

    let unkept = encoded(|out| encode_delta_poll(out, &views, None));
    let ctrl = peek_control_block(&unkept).unwrap();
    let poll = decode_delta_poll(&unkept[ctrl.body.clone()], &ctrl.hdr).unwrap();
    assert!(poll.kept().is_none());
}

/// A kept poll whose last id would run past `u64::MAX` is refused; one ending
/// on it is kept.
#[test]
fn a_kept_delta_poll_refuses_ids_past_the_last() {
    let decode = |n: u64, first: u64| {
        let views: Vec<_> = (1..=n).map(|v| item(v, 0, 1)).collect();
        let frame = encoded(|out| encode_delta_poll(out, &views, Some(sub(first))));
        let ctrl = peek_control_block(&frame).unwrap();
        let poll = decode_delta_poll(&frame[ctrl.body.clone()], &ctrl.hdr)?;
        Ok::<_, String>(poll.kept().expect("a kept poll").collect::<Vec<_>>())
    };
    assert_eq!(decode(1, u64::MAX), Ok(vec![sub(u64::MAX)]));
    let err = decode(2, u64::MAX).unwrap_err();
    assert!(err.contains("past u64::MAX"), "{err}");

    assert_eq!(decode(3, u64::MAX - 2).map(|ids| ids.len()), Ok(3));
    let err = decode(4, u64::MAX - 2).unwrap_err();
    assert!(err.contains("past u64::MAX"), "{err}");
}

/// A delta poll names no view `0`: it is the id of a fault ending the request.
#[test]
fn a_delta_poll_refuses_view_id_zero() {
    let err = peeked(
        &encoded(|out| encode_delta_poll(out, &[item(1, 0, 9), item(0, 0, 9)], None)),
        delta_items,
    )
    .expect_err("view id 0");
    assert!(err.contains("view id 0"), "{err:?}");
}

/// An item's blob opens with its cursor, so one too short to hold a cursor is
/// refused rather than read as a spec.
#[test]
fn a_delta_poll_refuses_a_blob_shorter_than_a_cursor() {
    let hdr = ControlHeader::naming(ClientVerb::DeltaPoll, Target { tid: 7, token: 3 }, 9);
    let err = delta_items(&one_item(hdr, b"", false)).expect_err("no blob");
    assert!(err.contains("lacks the blob"), "{err:?}");
    for len in [1, 15] {
        let err = delta_items(&one_item(hdr, &[0u8; 15][..len], false)).expect_err("a short blob");
        assert!(err.contains("carries no cursor"), "{len}: {err:?}");
    }
    let body = one_item(hdr, &[0u8; 16], false);
    assert_eq!(
        delta_items(&body).unwrap(),
        [DeltaPollItem {
            view: Target { tid: 7, token: 3 },
            from: None,
            reply_layout: 9,
            spec: b"",
        }]
    );
}

/// A body of one hand-built item: `hdr` (status and verb as given), `blob`,
/// and a data block where `data` says.
fn one_item(hdr: ControlHeader, blob: &[u8], data: bool) -> Vec<u8> {
    let mut out = Vec::new();
    let r = regions();
    crate::append_frame(&mut out, &hdr, blob, data.then_some(&r[..]));
    out
}

/// Each verb's items: a list at its cap decodes and one past it is refused; an
/// empty list is refused; a truncation inside an item is an error rather than a
/// short list (a cut on an item boundary is a shorter well-formed body, since
/// items run to the end); and an item is refused whose sections differ from the
/// verb's shape, that names another verb, that carries a non-`Ok` status or a
/// reply's flag, or that sets a conflict mode its verb does not read.
#[test]
fn decode_items_enforces_each_verbs_item_rules() {
    let multi: Vec<ClientVerb> = ClientVerb::ALL
        .iter()
        .copied()
        .filter(|&v| VerbShape::of(v).items.is_some())
        .collect();
    assert_eq!(multi.len(), 4);
    for verb in multi {
        let VerbShape { sections, items: shape, .. } = VerbShape::of(verb);
        let cap = shape.unwrap().cap;
        let hdr = ControlHeader::naming(verb, 7.into(), 0);
        let refused = |body: &[u8], want: &str| {
            let err = items(body, verb).expect_err(want);
            assert!(err.contains(want), "{verb:?}: {err:?} does not name {want:?}");
        };
        let blob: &[u8] = if sections.blob == Section::Must { b"blob" } else { b"" };
        let data = sections.data == Section::Must;
        let one = one_item(hdr, blob, data);

        let n = cap.min(3);
        assert_eq!(items(&one.repeat(n), verb).unwrap().len(), n, "{verb:?}");
        if cap != usize::MAX {
            refused(&one.repeat(cap + 1), "too many items");
        }
        refused(&[], "empty item list");

        let two = one.repeat(2);
        for cut in (1..two.len()).filter(|&c| c != one.len()) {
            assert!(items(&two[..cut], verb).is_err(), "{verb:?} cut at {cut}");
        }

        for (b, d) in [(&b""[..], false), (b"blob", false), (b"", true), (b"blob", true)] {
            let got = items(&one_item(hdr, b, d), verb);
            assert_eq!(
                got.is_ok(),
                sections.blob.admits(!b.is_empty()) && sections.data.admits(d),
                "{verb:?} blob {b:?} data {d}: {got:?}"
            );
            if let Err(err) = got {
                assert!(err.contains("blob") || err.contains("data block"), "{verb:?}: {err}");
            }
        }
        let other = ControlHeader::naming(ClientVerb::Push, 7.into(), 0);
        refused(&one_item(other, blob, data), "names verb");
        refused(
            &one_item(ControlHeader { status: WireStatus::Error, ..hdr }, b"boom", data),
            "status",
        );
        for flags in [
            WireFlags { continuation: true, ..hdr.flags },
            WireFlags { scan_last: true, ..hdr.flags },
            WireFlags { lane: WireLane::PushedTrain, ..hdr.flags },
        ] {
            refused(&one_item(ControlHeader { flags, ..hdr }, blob, data), "flag");
        }
        let error_mode = ControlHeader {
            flags: WireFlags {
                conflict_mode: WireConflictMode::Error,
                ..hdr.flags
            },
            ..hdr
        };
        if sections.conflict_mode {
            assert!(items(&one_item(error_mode, blob, data), verb).is_ok());
        } else {
            refused(&one_item(error_mode, blob, data), "conflict mode");
        }
    }
    let err = items(&one_item(ControlHeader::default(), b"", false), ClientVerb::Push).unwrap_err();
    assert!(err.contains("not a multi-item verb"), "{err}");
}

/// A held-ids blob names its ids in order; one that is no whole number of ids,
/// or that names `0`, is refused.
#[test]
fn held_ids_roundtrip_and_a_ragged_blob_is_refused() {
    let ids = [1, u64::MAX, 1 << 40, 1].map(sub);
    assert_eq!(decode_held(&encode_held(ids)).unwrap(), ids);
    assert_eq!(decode_held(&[]).unwrap(), []);
    let blob = encode_held(ids);
    for len in [1, 7, 9, blob.len() - 1] {
        let err = decode_held(&blob[..len]).unwrap_err();
        assert!(err.contains("no whole number"), "{len}: {err}");
    }
    let zero = [&blob[..8], &0u64.to_le_bytes()].concat();
    let err = decode_held(&zero).unwrap_err();
    assert!(err.contains("0 names no subscription"), "{err}");
}
