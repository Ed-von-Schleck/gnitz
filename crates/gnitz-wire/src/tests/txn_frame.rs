use super::*;
use crate::{WireConflictMode, CTRL_HEADER_SIZE};

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
fn regions() -> Regions<'static> {
    let mut r = Regions::new();
    for region in [PK, WEIGHT, NULLS, &[]] {
        r.push(region);
    }
    r
}

/// The same block framed — what a decoded item's data section must hold.
fn block() -> Vec<u8> {
    let mut buf = Vec::new();
    crate::wal::append_block(&regions(), 0, &mut buf);
    buf
}

fn ddl_item(tid: u64) -> FrameItem<'static> {
    FrameItem {
        hdr: ControlHeader::naming(ClientVerb::DdlTxn, tid.into(), 0),
        schema: None,
        data: regions(),
    }
}

fn family(tid: u64, conflict_mode: WireConflictMode, basis: u64, schema: &[u8]) -> FrameItem<'_> {
    FrameItem {
        hdr: ControlHeader {
            flags: WireFlags { conflict_mode, ..Default::default() },
            ..ControlHeader::naming(ClientVerb::PushTxn, Target { tid, token: !tid }, basis)
        },
        schema: Some(schema),
        data: regions(),
    }
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

/// Each item is its own frame: its header is the one it was built from under
/// the frame's verb, its bytes end where its last section does, its body is
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
        let frame = encode_items(verb, &items);
        let got = peeked(&frame, |b| decode_items(b, verb)).unwrap();
        assert_eq!(got.len(), items.len());
        for ((bytes, c), it) in got.iter().zip(&items) {
            let flags = WireFlags { verb, ..it.hdr.flags };
            assert_eq!(c.hdr, ControlHeader { flags, ..it.hdr });
            assert_eq!(c.schema.clone().map(|s| &bytes[s]), it.schema);
            assert_eq!(&bytes[c.data.clone().unwrap()], &d[..]);
            assert_eq!(c.body, bytes.len()..bytes.len());
        }
    }
}

#[test]
fn scan_multi_roundtrips_order_and_layouts() {
    // A repeated id is two positions, answered separately.
    let rels = [rel(7, 0), rel(8, 3), rel(9, u64::MAX), rel(7, 0)];
    assert_eq!(peeked(&encode_scan_multi(&rels), decode_scan_multi).unwrap(), rels);
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
    let frame = encode_delta_poll(&views, None);
    assert_eq!(peeked(&frame, decode_delta_items).unwrap(), views);
}

/// The first id rides the prologue's `arg0` and nothing else of it moves.
#[test]
fn a_kept_delta_poll_carries_its_first_id_in_the_prologue() {
    let views = [item(1, 5, 1)];
    let frame = encode_delta_poll(&views, Some(1_500));
    let ctrl = peek_control_block(&frame).unwrap();
    assert_eq!(ctrl.hdr, ControlHeader::naming(ClientVerb::DeltaPoll, 0.into(), 1_500));
    assert_eq!(decode_delta_items(&frame[ctrl.body.clone()]).unwrap(), views.to_vec());
    let kept = delta_poll_kept(&ctrl.hdr).expect("a kept poll");
    assert_eq!(kept.take(2).collect::<Vec<_>>(), [1_500, 1_501]);

    let unkept = encode_delta_poll(&views, None);
    assert!(delta_poll_kept(&peek_control_block(&unkept).unwrap().hdr).is_none());
}

/// A delta poll names no view `0`: it is the id of a fault ending the request.
#[test]
fn a_delta_poll_refuses_view_id_zero() {
    let err = peeked(
        &encode_delta_poll(&[item(1, 0, 9), item(0, 0, 9)], None),
        decode_delta_items,
    )
    .expect_err("view id 0");
    assert!(err.contains("view id 0"), "{err:?}");
}

/// An item's blob opens with its cursor, so one too short to hold a cursor is
/// refused rather than read as a spec.
#[test]
fn a_delta_poll_refuses_a_blob_shorter_than_a_cursor() {
    let hdr = ControlHeader::naming(ClientVerb::DeltaPoll, Target { tid: 7, token: 3 }, 9);
    for len in [0, 1, 15] {
        let err = decode_delta_items(&one_item(hdr, &[0u8; 15][..len], false, false)).expect_err("a short blob");
        assert!(err.contains("carries no cursor"), "{len}: {err:?}");
    }
    let body = one_item(hdr, &[0u8; 16], false, false);
    assert_eq!(
        decode_delta_items(&body).unwrap(),
        [DeltaPollItem {
            view: Target { tid: 7, token: 3 },
            from: None,
            reply_layout: 9,
            spec: b"",
        }]
    );
}

/// A body of one hand-built item: `hdr` (status and verb as given), `blob`,
/// and the sections named.
fn one_item(hdr: ControlHeader, blob: &[u8], schema: bool, data: bool) -> Vec<u8> {
    let mut out = Vec::new();
    let r = regions();
    crate::append_frame(
        &mut out,
        &hdr,
        blob,
        schema.then_some(&b"s"[..]),
        data.then_some(&r[..]),
    );
    out
}

/// Each verb's items: a list at its cap decodes and one past it is refused; an
/// empty list is refused; a truncation inside an item is an error rather than a
/// short list (a cut on an item boundary is a shorter well-formed body, since
/// items run to the end); and an item is refused whose sections differ from the
/// verb's shape, that names another verb, that carries a non-`Ok` status, or
/// that carries a blob its verb's items do not.
#[test]
fn decode_items_enforces_each_verbs_item_rules() {
    let multi: Vec<ClientVerb> = ClientVerb::ALL
        .iter()
        .copied()
        .filter(|&v| item_shape(v).is_some())
        .collect();
    assert_eq!(multi.len(), 4);
    for verb in multi {
        let ItemShape { blob, schema, data, cap } = item_shape(verb).unwrap();
        let hdr = ControlHeader::naming(verb, 7.into(), 0);
        let refused = |body: &[u8], want: &str| {
            let err = decode_items(body, verb).expect_err(want);
            assert!(err.contains(want), "{verb:?}: {err:?} does not name {want:?}");
        };
        let one = one_item(hdr, b"", schema, data);

        let n = cap.min(3);
        assert_eq!(decode_items(&one.repeat(n), verb).unwrap().len(), n, "{verb:?}");
        if cap != usize::MAX {
            refused(&one.repeat(cap + 1), "too many items");
        }
        refused(&[], "empty item list");

        let two = one.repeat(2);
        for cut in (1..two.len()).filter(|&c| c != one.len()) {
            assert!(decode_items(&two[..cut], verb).is_err(), "{verb:?} cut at {cut}");
        }

        for (s, d) in [(false, false), (true, false), (false, true), (true, true)] {
            if (s, d) != (schema, data) {
                let err = decode_items(&one_item(hdr, b"", s, d), verb).unwrap_err();
                assert!(
                    err.contains("schema record") || err.contains("data block"),
                    "{verb:?}: {err}"
                );
            }
        }
        let other = ControlHeader::naming(ClientVerb::Push, 7.into(), 0);
        refused(&one_item(other, b"", schema, data), "names verb");
        refused(
            &one_item(
                ControlHeader { status: WireStatus::Error, ..hdr },
                b"boom",
                schema,
                data,
            ),
            "status",
        );
        match blob {
            true => assert_eq!(
                decode_items(&one_item(hdr, b"blob", schema, data), verb).unwrap().len(),
                1
            ),
            false => refused(&one_item(hdr, b"blob", schema, data), "blob"),
        }
    }
    let err = decode_items(
        &one_item(ControlHeader::default(), b"", false, false),
        ClientVerb::ScanSpec,
    )
    .unwrap_err();
    assert!(err.contains("not a multi-item verb"), "{err}");
}

/// A held-ids blob names its ids in order, and one that is no whole number of
/// ids is refused.
#[test]
fn held_ids_roundtrip_and_a_ragged_blob_is_refused() {
    let ids = [1, u64::MAX, 1 << 40, 1];
    assert_eq!(decode_held(&encode_held(ids)).unwrap(), ids);
    assert_eq!(decode_held(&[]).unwrap(), []);
    let blob = encode_held(ids);
    for len in [1, 7, 9, blob.len() - 1] {
        let err = decode_held(&blob[..len]).unwrap_err();
        assert!(err.contains("no whole number"), "{len}: {err}");
    }
}
