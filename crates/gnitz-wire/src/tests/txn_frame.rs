use super::*;
use crate::control::CTRL_HEADER_SIZE;
use crate::WireConflictMode;

fn rel(tid: u64, reply_layout: u64) -> ScanMultiItem {
    ScanMultiItem { tid, reply_layout }
}

fn item(view_id: u64, after_tick: u64, reply_layout: u64) -> DeltaPollItem<'static> {
    DeltaPollItem {
        view: Target { tid: view_id, token: view_id ^ 0xA5A5 },
        tag: !after_tick,
        after_tick,
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
        hdr: ControlHeader { target_id: tid, ..Default::default() },
        blob: &[],
        schema: None,
        data: Some(regions()),
    }
}

fn family(tid: u64, conflict_mode: WireConflictMode, basis: u64, schema: &[u8]) -> FrameItem<'_> {
    FrameItem {
        hdr: ControlHeader {
            target_id: tid,
            flags: WireFlags { conflict_mode, ..Default::default() },
            arg0: basis,
            ..Default::default()
        },
        blob: &[],
        schema: Some(schema),
        data: Some(regions()),
    }
}

/// The views of a `DELTA_POLL` body sent with no wait.
fn delta_poll_views(body: &[u8]) -> Result<Vec<DeltaPollItem<'_>>, String> {
    decode_delta_poll(&ControlHeader::default(), body).map(|(_, views)| views)
}

/// Peek `frame`'s prologue — it carries only its verb, and its body starts right
/// after it — check it as a client frame, and decode its body.
fn peeked<'a, T>(frame: &'a [u8], decode: impl Fn(&'a [u8]) -> Result<T, String>) -> Result<T, String> {
    let ctrl = peek_control_block(frame)?;
    let verb = ctrl.client_verb().map_err(str::to_string)?;
    let prologue = ControlHeader {
        flags: WireFlags { verb, ..Default::default() },
        ..Default::default()
    };
    assert_eq!(ctrl.hdr, prologue, "only the verb is set");
    assert!(ctrl.blob.is_empty());
    assert_eq!(ctrl.body, CTRL_HEADER_SIZE..frame.len());
    decode(&frame[ctrl.body.clone()])
}

fn ddl(body: &[u8]) -> Result<Vec<(&[u8], DecodedControl)>, String> {
    decode_items(body, ClientVerb::DdlTxn)
}

fn push(body: &[u8]) -> Result<Vec<(&[u8], DecodedControl)>, String> {
    decode_items(body, ClientVerb::PushTxn)
}

/// Each item is its own frame: its bytes end where its last section does, its
/// body is empty, and its data section is the block it was built from.
#[test]
fn ddl_txn_roundtrips_every_family_in_order() {
    let b = block();
    let frame = encode_items(ClientVerb::DdlTxn, &[ddl_item(WIDE_TID), ddl_item(2)]);
    let got = peeked(&frame, ddl).unwrap();
    assert_eq!(got.len(), 2);
    for ((bytes, c), tid) in got.iter().zip([WIDE_TID, 2]) {
        assert_eq!(c.hdr.target_id, tid);
        assert_eq!(c.hdr.flags.verb, ClientVerb::DdlTxn);
        assert_eq!(&bytes[c.data.clone().unwrap()], &b[..]);
        assert_eq!(c.body, bytes.len()..bytes.len());
    }
}

#[test]
fn push_txn_roundtrips_families_modes_and_bases() {
    let (s0, s1, d) = (b"schema 0".to_vec(), b"schema one".to_vec(), block());
    let w = 0x0102_0304_0506_0708;
    let frame = encode_items(
        ClientVerb::PushTxn,
        &[
            family(16, WireConflictMode::Error, BLIND, &s0),
            family(WIDE_TID, WireConflictMode::Update, w, &s1),
        ],
    );
    let fams = peeked(&frame, push).unwrap();
    assert_eq!(fams.len(), 2);
    let want = [
        (16, WireConflictMode::Error, BLIND, &s0),
        (WIDE_TID, WireConflictMode::Update, w, &s1),
    ];
    for ((bytes, c), (tid, mode, basis, schema)) in fams.iter().zip(want) {
        assert_eq!(
            (c.hdr.target_id, c.hdr.flags.conflict_mode, c.hdr.arg0),
            (tid, mode, basis)
        );
        assert_eq!(&bytes[c.schema.clone().unwrap()], &schema[..]);
        assert_eq!(&bytes[c.data.clone().unwrap()], &d[..]);
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
    let frame = encode_delta_poll(&views, 0);
    assert_eq!(peeked(&frame, delta_poll_views).unwrap(), views);
}

/// The wait rides the prologue's `arg0` and nothing else of it moves.
#[test]
fn a_delta_poll_carries_its_wait_in_the_prologue() {
    let views = [item(1, 5, 1)];
    let frame = encode_delta_poll(&views, 1_500);
    let ctrl = peek_control_block(&frame).unwrap();
    let prologue = ControlHeader {
        flags: WireFlags {
            verb: ClientVerb::DeltaPoll,
            ..Default::default()
        },
        arg0: 1_500,
        ..Default::default()
    };
    assert_eq!(ctrl.hdr, prologue);
    assert_eq!(
        decode_delta_poll(&ctrl.hdr, &frame[ctrl.body.clone()]).unwrap(),
        (1_500, views.to_vec())
    );
}

/// A delta poll names no view `0`: it is the id of a fault ending the request.
#[test]
fn a_delta_poll_refuses_view_id_zero() {
    let err = peeked(&encode_delta_poll(&[item(1, 0, 9), item(0, 0, 9)], 0), delta_poll_views).expect_err("view id 0");
    assert!(err.contains("view id 0"), "{err:?}");
}

/// An item's blob opens with its cursor, so one too short to hold a cursor is
/// refused rather than read as a spec.
#[test]
fn a_delta_poll_refuses_a_blob_shorter_than_a_cursor() {
    let hdr = ControlHeader::naming(ClientVerb::DeltaPoll, Target { tid: 7, token: 3 }, 9);
    for len in [0, 1, 15] {
        let err = delta_poll_views(&one_item(hdr, &[0u8; 15][..len], false, false)).expect_err("a short blob");
        assert!(err.contains("carries no cursor"), "{len}: {err:?}");
    }
    let body = one_item(hdr, &[0u8; 16], false, false);
    assert_eq!(
        delta_poll_views(&body).unwrap(),
        [DeltaPollItem {
            view: Target { tid: 7, token: 3 },
            tag: 0,
            after_tick: 0,
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
    crate::control::append_frame(
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
        let hdr = ControlHeader {
            target_id: 7,
            flags: WireFlags { verb, ..Default::default() },
            ..Default::default()
        };
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
        let other = ControlHeader {
            flags: WireFlags {
                verb: ClientVerb::Push,
                ..Default::default()
            },
            ..hdr
        };
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
